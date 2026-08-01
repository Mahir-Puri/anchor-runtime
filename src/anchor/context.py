"""The API workflow authors actually touch.

Everything a workflow does that reaches outside itself goes through `ctx.step`,
`ctx.call_tool` or `ctx.model`. That is the one rule of the programming model,
and it buys three properties:

  * a run can be replayed from its log without repeating completed work
  * every side effect carries a token that is stable across replays
  * budgets and cancellation are checked at known boundaries instead of never

Step identity is positional: the Nth call to a step named "refund" is
"refund#N". That makes keys deterministic without asking the author to invent
them, and it is also the model's sharpest edge, documented in the class docstring
below.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import uuid
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from anchor.budget import BudgetTracker
from anchor.config import Settings
from anchor.config import settings as default_settings
from anchor.errors import (
    AmbiguousStepError,
    CancellationRequested,
    LeaseLost,
    StepFailedError,
)
from anchor.events import EventType, step_token
from anchor.models import Event, RunRecord, StepIdempotency
from anchor.providers.base import ModelProvider, ModelResponse
from anchor.registry import Registry


class RunContext:
    """Per-attempt execution context.

    Determinism requirement: a workflow must reach the same step names in the
    same order on every attempt for a given input. Branching on the *results* of
    steps is fine, because those results are replayed from the log. Branching on
    anything the log does not capture (wall-clock time, random numbers, live
    reads outside a step) is not, and will produce keys that do not line up on
    replay. Wrap that kind of thing in a step and it becomes safe.
    """

    def __init__(
        self,
        store: Any,
        run: RunRecord,
        events: Iterable[Event],
        registry: Registry,
        provider: ModelProvider,
        settings: Settings | None = None,
        lease_guard: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        self.store = store
        self.run = run
        self.run_id: uuid.UUID = run.run_id
        self.input: dict[str, Any] = run.input
        self.registry = registry
        self.provider = provider
        self.settings = settings or default_settings
        self._lease_guard = lease_guard

        self.budget = BudgetTracker(run.budget, run.usage, run.created_at)
        self._ordinals: dict[str, int] = {}
        self._completed: dict[str, Any] = {}
        self._unresolved: dict[str, str] = {}
        self._usage_lock = asyncio.Lock()

        self.replayed_steps = 0
        self.executed_steps = 0

        self._build_replay_state(events)

    # ------------------------------------------------------------------ replay

    def _build_replay_state(self, events: Iterable[Event]) -> None:
        for event in events:
            key = event.step_key
            if key is None:
                continue
            if event.type == EventType.STEP_STARTED:
                self._unresolved[key] = event.payload.get("token", "")
            elif event.type == EventType.STEP_COMPLETED:
                self._completed[key] = event.payload.get("result")
                self._unresolved.pop(key, None)
            elif event.type == EventType.STEP_FAILED:
                self._unresolved.pop(key, None)

    def _next_key(self, name: str) -> str:
        n = self._ordinals.get(name, 0) + 1
        self._ordinals[name] = n
        return f"{name}#{n}"

    # ------------------------------------------------------------ public steps

    async def step(
        self,
        name: str,
        fn: Callable[..., Awaitable[Any]] | Callable[..., Any],
        *,
        idempotency: StepIdempotency = StepIdempotency.AT_LEAST_ONCE,
        verify: Callable[..., Awaitable[Any]] | None = None,
        max_attempts: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """Run `fn` exactly once across all attempts of this run.

        `fn` may be sync or async. If it accepts a `token` parameter it receives
        the run's stable idempotency token for this step; if it accepts `ctx` it
        receives this context.
        """
        key = self._next_key(name)
        result, _ = await self._step_with_key(
            key, fn, idempotency=idempotency, verify=verify, max_attempts=max_attempts, kwargs=kwargs
        )
        return result

    async def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        """Invoke a registered tool as a durable step.

        The tool's declared idempotency class and verifier come from the
        registry, so an agent loop cannot accidentally downgrade the safety of a
        money-moving call.
        """
        spec = self.registry.tool(name)
        args = arguments or {}
        key = self._next_key(f"tool:{spec.name}")

        async def invoke(**passthrough: Any) -> Any:
            self.budget.add_tool_call()
            return await _invoke(spec.fn, self, passthrough.get("token"), args)

        async def verify(**passthrough: Any) -> Any:
            if spec.verify is None:
                return None
            return await _invoke(spec.verify, self, passthrough.get("token"), args)

        result, executed = await self._step_with_key(
            key,
            invoke,
            idempotency=spec.idempotency,
            verify=verify if spec.verify else None,
            max_attempts=None,
            kwargs={},
        )
        if executed:
            await self.append(
                EventType.TOOL_INVOKED,
                key,
                {"tool": spec.name, "arguments": args, "idempotency": spec.idempotency.value},
            )
        return result

    async def model(
        self,
        messages: list[dict[str, Any]],
        *,
        tools: list[dict[str, Any]] | None = None,
        system: str | None = None,
        max_tokens: int = 1024,
        name: str = "model",
    ) -> ModelResponse:
        """A model call as a durable step.

        On replay the recorded response is returned verbatim, which is what makes
        a resumed agent trajectory identical to the original rather than merely
        similar.
        """
        key = self._next_key(name)

        async def invoke(**_: Any) -> dict[str, Any]:
            response = await self.provider.complete(
                messages=messages, tools=tools, system=system, max_tokens=max_tokens
            )
            return response.to_dict()

        raw, executed = await self._step_with_key(
            key, invoke, idempotency=StepIdempotency.AT_LEAST_ONCE, verify=None, max_attempts=None, kwargs={}
        )
        response = ModelResponse.from_dict(raw)

        if executed:
            self.budget.add_model_usage(
                response.input_tokens, response.output_tokens, response.cost_usd
            )
            await self._persist_usage()
            await self.append(
                EventType.MODEL_CALLED,
                key,
                {
                    "model": response.model,
                    "stop_reason": response.stop_reason,
                    "input_tokens": response.input_tokens,
                    "output_tokens": response.output_tokens,
                    "tool_calls": [t.name for t in response.tool_calls],
                },
            )
        return response

    async def gather(
        self,
        name: str,
        factories: list[Callable[..., Awaitable[Any]]],
        *,
        idempotency: StepIdempotency = StepIdempotency.AT_LEAST_ONCE,
    ) -> list[Any]:
        """Fan out, with deterministic child keys.

        Keys are allocated in list order *before* anything is scheduled, so
        concurrency does not affect naming and a resumed run lines up with the
        original regardless of which branch finished first.
        """
        keys = [self._next_key(f"{name}[{i}]") for i in range(len(factories))]
        coros = [
            self._step_with_key(
                key, fn, idempotency=idempotency, verify=None, max_attempts=None, kwargs={}
            )
            for key, fn in zip(keys, factories, strict=True)
        ]
        results = await asyncio.gather(*coros)
        return [result for result, _ in results]

    # ------------------------------------------------------------------- guts

    async def _step_with_key(
        self,
        key: str,
        fn: Callable[..., Any],
        *,
        idempotency: StepIdempotency,
        verify: Callable[..., Awaitable[Any]] | None,
        max_attempts: int | None,
        kwargs: dict[str, Any],
    ) -> tuple[Any, bool]:
        """Returns (result, executed). `executed` is False on a replayed step."""
        if key in self._completed:
            self.replayed_steps += 1
            return self._completed[key], False

        await self._checkpoint_guards()

        token = step_token(self.run_id, key)
        recovering = key in self._unresolved

        if recovering:
            resolved = await self._resolve_crash_window(key, token, idempotency, verify, kwargs)
            if resolved is not _UNRESOLVED:
                return resolved, False
        else:
            await self.append(EventType.STEP_STARTED, key, {"token": str(token)})

        attempts = max_attempts or self.settings.step_max_attempts
        last_error: BaseException | None = None

        for attempt in range(1, attempts + 1):
            try:
                result = await _invoke(fn, self, token, kwargs)
            except (CancellationRequested, LeaseLost):
                raise
            except Exception as exc:  # noqa: BLE001 - deliberately broad, recorded below
                last_error = exc
                if attempt < attempts:
                    await self.append(
                        EventType.STEP_RETRIED,
                        key,
                        {"attempt": attempt, "error": repr(exc), "reason": "step_error"},
                    )
                    await asyncio.sleep(self.settings.step_backoff_base * (2 ** (attempt - 1)))
                    continue
                await self.append(
                    EventType.STEP_FAILED, key, {"attempts": attempt, "error": repr(exc)}
                )
                raise StepFailedError(key, attempt, exc) from exc
            else:
                await self.append(
                    EventType.STEP_COMPLETED,
                    key,
                    {"result": _ensure_json(result, key), "attempt": attempt},
                )
                self._completed[key] = result
                self._unresolved.pop(key, None)
                self.executed_steps += 1
                await self._persist_usage()
                return result, True

        raise StepFailedError(key, attempts, last_error or RuntimeError("unknown"))

    async def _resolve_crash_window(
        self,
        key: str,
        token: uuid.UUID,
        idempotency: StepIdempotency,
        verify: Callable[..., Awaitable[Any]] | None,
        kwargs: dict[str, Any],
    ) -> Any:
        """Decide what to do about a step that started but never finished.

        Three outcomes, in order of preference:

        1. A verifier exists and says the effect landed. Record the real outcome
           and move on: no duplicate, no lost work.
        2. The step is declared safe to repeat. Re-run it.
        3. Neither. Refuse to guess and park the run for review, because the two
           available guesses are "charge the customer twice" and "silently drop
           the refund", and picking one on the customer's behalf is not the
           runtime's call to make.
        """
        if verify is not None:
            outcome = await _invoke(verify, self, token, kwargs)
            if outcome is not None:
                await self.append(
                    EventType.STEP_COMPLETED,
                    key,
                    {"result": _ensure_json(outcome, key), "recovered_by": "verify"},
                )
                self._completed[key] = outcome
                self._unresolved.pop(key, None)
                return outcome

        if idempotency is StepIdempotency.AT_LEAST_ONCE:
            await self.append(
                EventType.STEP_RETRIED,
                key,
                {"reason": "crash_window", "token": str(token)},
            )
            return _UNRESOLVED

        raise AmbiguousStepError(key, str(token))

    async def _checkpoint_guards(self) -> None:
        """Run the three checks that only make sense at a step boundary."""
        if self._lease_guard is not None and not await self._lease_guard():
            raise LeaseLost(f"lease lost for run {self.run_id}")
        if await self.store.is_cancel_requested(self.run_id):
            raise CancellationRequested(f"run {self.run_id} was cancelled")
        self.budget.check()

    async def _persist_usage(self) -> None:
        async with self._usage_lock:
            await self.store.save_usage(self.run_id, self.budget.usage)

    async def append(
        self, type: str, step_key: str | None = None, payload: dict[str, Any] | None = None
    ) -> int:
        return await self.store.append_event(self.run_id, type, step_key, payload or {})


class _Unresolved:
    """Sentinel distinguishing "no value" from a legitimate None result."""

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<unresolved>"


_UNRESOLVED = _Unresolved()


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _invoke(
    fn: Callable[..., Any], ctx: RunContext, token: uuid.UUID | None, arguments: dict[str, Any]
) -> Any:
    """Call a step or tool function passing only what it actually declares.

    A read-only tool can be written as `async def lookup(payment_id)` while a
    money-moving one takes `(ctx, token, payment_id)`, and neither needs a
    wrapper. Functions that declare **kwargs get the token too, which is what the
    internal closures in this module rely on.
    """
    params = inspect.signature(fn).parameters
    accepts_var_kwargs = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())

    injected: dict[str, Any] = {}
    if "ctx" in params:
        injected["ctx"] = ctx
    if "token" in params or accepts_var_kwargs:
        injected["token"] = token

    if accepts_var_kwargs:
        passed = {k: v for k, v in arguments.items() if k not in injected}
    else:
        passed = {k: v for k, v in arguments.items() if k in params}

    return await _maybe_await(fn(**injected, **passed))


def _ensure_json(value: Any, key: str) -> Any:
    """Fail loudly at the boundary rather than mysteriously inside asyncpg."""
    try:
        json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(
            f"step {key!r} returned a value that is not JSON-serialisable "
            f"({type(value).__name__}); step results are persisted to the event log"
        ) from exc
    return value
