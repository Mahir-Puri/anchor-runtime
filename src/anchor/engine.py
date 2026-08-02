"""The engine runs one attempt of one run.

It knows nothing about queues or workers. Give it a run id and a way to check
that the caller still holds the lease, and it will drive the workflow to a
terminal state or die trying. Keeping it free of transport concerns is what
makes it testable without a worker process, which is how most of the test suite
is written.
"""

from __future__ import annotations

import logging
import traceback
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from anchor.config import Settings
from anchor.config import settings as default_settings
from anchor.context import RunContext
from anchor.errors import (
    AmbiguousStepError,
    BudgetExceededError,
    CancellationRequested,
    LeaseLost,
    StepFailedError,
    WorkflowNotFound,
)
from anchor.events import EventType
from anchor.models import RunStatus
from anchor.providers.base import ModelProvider
from anchor.registry import Registry
from anchor.registry import registry as default_registry

log = logging.getLogger("anchor.engine")


@dataclass
class Outcome:
    run_id: uuid.UUID
    status: RunStatus
    result: Any = None
    error: dict[str, Any] | None = None
    replayed_steps: int = 0
    executed_steps: int = 0

    @property
    def should_dequeue(self) -> bool:
        """Whether the queue row should be deleted rather than left to expire.

        LeaseLost is the one case where it should not: we no longer own the run,
        so touching the queue row would stomp on whoever does.
        """
        return self.status.terminal


class Engine:
    def __init__(
        self,
        store: Any,
        provider: ModelProvider,
        registry: Registry | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.store = store
        self.provider = provider
        self.registry = registry or default_registry
        self.settings = settings or default_settings

    async def execute(
        self,
        run_id: uuid.UUID,
        *,
        lease_guard: Callable[[], Awaitable[bool]] | None = None,
    ) -> Outcome:
        run = await self.store.get_run(run_id)
        if run is None:
            raise LookupError(f"run {run_id} does not exist")

        if run.status.terminal:
            # Duplicate delivery. Nothing to do, and importantly nothing to undo.
            log.info("run %s already terminal (%s); skipping", run_id, run.status.value)
            return Outcome(run_id=run_id, status=run.status, result=run.result, error=run.error)

        attempt = await self.store.bump_attempt(run_id)
        if attempt > 1:
            await self.store.append_event(
                run_id,
                EventType.RUN_RESUMED,
                None,
                {"attempt": attempt, "events_before_resume": run.event_seq},
            )

        await self.store.set_status(run_id, RunStatus.RUNNING)

        run = await self.store.get_run(run_id)
        events = await self.store.load_events(run_id)
        ctx = RunContext(
            store=self.store,
            run=run,
            events=events,
            registry=self.registry,
            provider=self.provider,
            settings=self.settings,
            lease_guard=lease_guard,
        )

        try:
            workflow_fn = self.registry.workflow(run.workflow)
        except WorkflowNotFound as exc:
            return await self._fail(ctx, "WorkflowNotFound", str(exc), retryable=False)

        try:
            result = await workflow_fn(ctx, run.input)

        except CancellationRequested as exc:
            await ctx.append(EventType.RUN_CANCELLED, None, {"reason": str(exc)})
            await self.store.set_status(run_id, RunStatus.CANCELLED)
            return self._outcome(ctx, RunStatus.CANCELLED)

        except BudgetExceededError as exc:
            await ctx.append(
                EventType.BUDGET_EXCEEDED,
                None,
                {"dimension": exc.dimension, "limit": exc.limit, "observed": exc.observed},
            )
            return await self._fail(
                ctx,
                "BudgetExceededError",
                str(exc),
                retryable=False,
                extra={"dimension": exc.dimension},
            )

        except AmbiguousStepError as exc:
            # The interesting terminal state. Not a failure to retry: a decision
            # to escalate. See docs/FAILURE_MATRIX.md row 4.
            await ctx.append(
                EventType.RUN_NEEDS_REVIEW,
                exc.step_key,
                {"token": exc.token, "reason": "unresolved_at_most_once_step"},
            )
            await self.store.set_status(
                run_id,
                RunStatus.NEEDS_REVIEW,
                error={
                    "type": "AmbiguousStepError",
                    "message": str(exc),
                    "step_key": exc.step_key,
                    "token": exc.token,
                    "retryable": False,
                },
            )
            return self._outcome(ctx, RunStatus.NEEDS_REVIEW)

        except LeaseLost as exc:
            # Someone else owns this run now. Leave every piece of state alone.
            log.warning("lease lost during run %s: %s", run_id, exc)
            return Outcome(
                run_id=run_id,
                status=RunStatus.RUNNING,
                error={"type": "LeaseLost", "message": str(exc)},
                replayed_steps=ctx.replayed_steps,
                executed_steps=ctx.executed_steps,
            )

        except StepFailedError as exc:
            return await self._fail(
                ctx,
                "StepFailedError",
                str(exc),
                retryable=False,
                extra={"step_key": exc.step_key, "attempts": exc.attempts},
            )

        except Exception as exc:  # noqa: BLE001 - last line of defence
            return await self._fail(
                ctx,
                type(exc).__name__,
                str(exc) or repr(exc),
                retryable=False,
                extra={"traceback": traceback.format_exc(limit=12)},
            )

        await ctx.append(
            EventType.RUN_COMPLETED,
            None,
            {
                "usage": ctx.budget.snapshot(),
                "replayed_steps": ctx.replayed_steps,
                "executed_steps": ctx.executed_steps,
            },
        )
        await self.store.save_usage(run_id, ctx.budget.usage)
        await self.store.set_status(run_id, RunStatus.COMPLETED, result=result)
        return self._outcome(ctx, RunStatus.COMPLETED, result=result)

    # ---------------------------------------------------------------- internals

    async def _fail(
        self,
        ctx: RunContext,
        type_name: str,
        message: str,
        *,
        retryable: bool,
        extra: dict[str, Any] | None = None,
    ) -> Outcome:
        error = {"type": type_name, "message": message, "retryable": retryable, **(extra or {})}
        await ctx.append(EventType.RUN_FAILED, None, error)
        await self.store.save_usage(ctx.run_id, ctx.budget.usage)
        await self.store.set_status(ctx.run_id, RunStatus.FAILED, error=error)
        return self._outcome(ctx, RunStatus.FAILED, error=error)

    @staticmethod
    def _outcome(
        ctx: RunContext,
        status: RunStatus,
        *,
        result: Any = None,
        error: dict[str, Any] | None = None,
    ) -> Outcome:
        return Outcome(
            run_id=ctx.run_id,
            status=status,
            result=result,
            error=error,
            replayed_steps=ctx.replayed_steps,
            executed_steps=ctx.executed_steps,
        )
