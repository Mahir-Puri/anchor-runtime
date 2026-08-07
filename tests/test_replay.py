"""Replay semantics.

`LeaseLost` raised from inside a workflow is how these tests simulate a worker
that vanished: the engine leaves the run non-terminal and writes no failure
event, which is exactly the state a `kill -9` leaves behind. See
scripts/chaos_kill.py for the version that actually kills a process.
"""

from __future__ import annotations

from anchor.engine import Engine
from anchor.errors import LeaseLost
from anchor.events import EventType
from anchor.models import RunStatus


async def test_completed_steps_are_not_re_executed(store, registry, echo, submit):
    calls: list[str] = []

    async def counter(ctx, payload):
        async def first():
            calls.append("first")
            return 1

        async def second():
            calls.append("second")
            if len(calls) < 3:  # blow up the first time only
                raise LeaseLost("simulated worker death")
            return 2

        a = await ctx.step("first", first)
        b = await ctx.step("second", second)
        return {"sum": a + b}

    registry.register_workflow("counter", counter)
    run = await submit("counter")
    engine = Engine(store=store, provider=echo, registry=registry)

    first_attempt = await engine.execute(run.run_id)
    assert first_attempt.status is RunStatus.RUNNING, "a lost lease is not a terminal state"
    assert calls == ["first", "second"]

    second_attempt = await engine.execute(run.run_id)
    assert second_attempt.status is RunStatus.COMPLETED
    assert second_attempt.result == {"sum": 3}

    # "first" ran once across both attempts; "second" ran again because it never
    # recorded a result and is at-least-once by default.
    assert calls == ["first", "second", "second"]
    assert second_attempt.replayed_steps == 1
    assert second_attempt.executed_steps == 1


async def test_resume_writes_a_resume_event_with_attempt_number(store, registry, echo, submit):
    async def flaky(ctx, payload):
        await ctx.step("a", lambda **_: "ok")
        if ctx.run.attempt == 1:
            raise LeaseLost("simulated worker death")
        return "done"

    registry.register_workflow("flaky", flaky)
    run = await submit("flaky")
    engine = Engine(store=store, provider=echo, registry=registry)

    await engine.execute(run.run_id)
    await engine.execute(run.run_id)

    events = await store.load_events(run.run_id)
    resumed = [e for e in events if e.type == EventType.RUN_RESUMED]
    assert len(resumed) == 1
    assert resumed[0].payload["attempt"] == 2

    fetched = await store.get_run(run.run_id)
    assert fetched.status is RunStatus.COMPLETED
    assert fetched.attempt == 2


async def test_model_response_is_replayed_verbatim(store, registry, submit):
    """A resumed agent must take the same trajectory, not a similar one."""
    from anchor.providers.echo import EchoProvider

    provider = EchoProvider(plan=["noop"], final_text="final answer")
    seen: list[str] = []

    async def agent(ctx, payload):
        response = await ctx.model([{"role": "user", "content": "hello"}])
        seen.append(response.text)
        if ctx.run.attempt == 1:
            raise LeaseLost("simulated worker death")
        return {"text": response.text}

    registry.register_workflow("agent", agent)
    run = await submit("agent")
    engine = Engine(store=store, provider=provider, registry=registry)

    await engine.execute(run.run_id)
    outcome = await engine.execute(run.run_id)

    assert outcome.status is RunStatus.COMPLETED
    assert seen[0] == seen[1], "the second attempt replayed the recorded response"
    # One model call was billed, even though the workflow observed it twice.
    fetched = await store.get_run(run.run_id)
    assert fetched.usage.model_calls == 1


async def test_terminal_runs_are_not_re_executed(store, registry, echo, submit):
    runs = 0

    async def once(ctx, payload):
        nonlocal runs
        runs += 1
        return "ok"

    registry.register_workflow("once", once)
    run = await submit("once")
    engine = Engine(store=store, provider=echo, registry=registry)

    assert (await engine.execute(run.run_id)).status is RunStatus.COMPLETED
    # Duplicate delivery of an already-finished run: a no-op, not a re-run.
    assert (await engine.execute(run.run_id)).status is RunStatus.COMPLETED
    assert runs == 1


async def test_step_retries_transient_failures_then_succeeds(store, registry, echo, submit):
    attempts = 0

    async def workflow(ctx, payload):
        async def flaky():
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise ConnectionError("transient")
            return "recovered"

        return await ctx.step("flaky", flaky)

    registry.register_workflow("retrying", workflow)
    run = await submit("retrying")
    engine = Engine(store=store, provider=echo, registry=registry)

    outcome = await engine.execute(run.run_id)
    assert outcome.status is RunStatus.COMPLETED
    assert outcome.result == "recovered"
    assert attempts == 3

    events = [e.type for e in await store.load_events(run.run_id)]
    assert events.count(EventType.STEP_RETRIED) == 2
    assert events.count(EventType.STEP_COMPLETED) == 1


async def test_step_exhausting_retries_fails_the_run(store, registry, echo, submit):
    async def workflow(ctx, payload):
        async def always_fails():
            raise ConnectionError("downstream is down")

        return await ctx.step("doomed", always_fails)

    registry.register_workflow("doomed", workflow)
    run = await submit("doomed")
    engine = Engine(store=store, provider=echo, registry=registry)

    outcome = await engine.execute(run.run_id)
    assert outcome.status is RunStatus.FAILED
    assert outcome.error["type"] == "StepFailedError"
    assert outcome.error["retryable"] is False

    fetched = await store.get_run(run.run_id)
    assert fetched.error["step_key"] == "doomed#1"


async def test_unknown_workflow_fails_cleanly(store, echo, submit, registry):
    run = await submit("not_registered")
    engine = Engine(store=store, provider=echo, registry=registry)

    outcome = await engine.execute(run.run_id)
    assert outcome.status is RunStatus.FAILED
    assert outcome.error["type"] == "WorkflowNotFound"


async def test_step_keys_are_positional_and_stable(store, registry, echo, submit):
    async def workflow(ctx, payload):
        for _ in range(3):
            await ctx.step("tick", lambda **_: "t")
        return "ok"

    registry.register_workflow("ticks", workflow)
    run = await submit("ticks")
    engine = Engine(store=store, provider=echo, registry=registry)
    await engine.execute(run.run_id)

    keys = [e.step_key for e in await store.load_events(run.run_id) if e.type == EventType.STEP_COMPLETED]
    assert keys == ["tick#1", "tick#2", "tick#3"]
