"""Cancellation.

Cooperative, observed at step boundaries. A hard interrupt mid-step would create
exactly the ambiguity the rest of this runtime exists to avoid, so cancellation
buys a bounded wait in exchange for a known state.
"""

from __future__ import annotations

from anchor.engine import Engine
from anchor.events import EventType
from anchor.models import RunStatus


async def test_cancel_stops_the_run_at_the_next_boundary(store, registry, echo, submit):
    executed: list[str] = []

    async def workflow(ctx, payload):
        await ctx.step("one", lambda **_: executed.append("one"))
        await store.request_cancel(ctx.run_id)  # arrives while the run is in flight
        await ctx.step("two", lambda **_: executed.append("two"))
        return "never"

    registry.register_workflow("cancellable", workflow)
    run = await submit("cancellable")
    engine = Engine(store=store, provider=echo, registry=registry)

    outcome = await engine.execute(run.run_id)
    assert outcome.status is RunStatus.CANCELLED
    assert executed == ["one"], "no step starts after cancellation is observed"

    types = [e.type for e in await store.load_events(run.run_id)]
    assert EventType.CANCEL_REQUESTED in types
    assert EventType.RUN_CANCELLED in types
    assert EventType.STEP_STARTED in types


async def test_cancelled_run_is_not_resumed(store, registry, echo, submit):
    entered = 0

    async def workflow(ctx, payload):
        nonlocal entered
        entered += 1
        await store.request_cancel(ctx.run_id)
        await ctx.step("boundary", lambda **_: None)
        return "never"

    registry.register_workflow("once_cancelled", workflow)
    run = await submit("once_cancelled")
    engine = Engine(store=store, provider=echo, registry=registry)

    await engine.execute(run.run_id)
    again = await engine.execute(run.run_id)

    assert again.status is RunStatus.CANCELLED
    assert entered == 1


async def test_completed_work_is_preserved_when_a_run_is_cancelled(store, registry, echo, submit):
    """Cancellation is not a rollback. What happened, happened, and the log says so."""

    async def workflow(ctx, payload):
        await ctx.step("effect", lambda **_: {"done": True})
        await store.request_cancel(ctx.run_id)
        await ctx.step("next", lambda **_: {"done": True})
        return "never"

    registry.register_workflow("partial", workflow)
    run = await submit("partial")
    engine = Engine(store=store, provider=echo, registry=registry)
    await engine.execute(run.run_id)

    completed = [
        e for e in await store.load_events(run.run_id) if e.type == EventType.STEP_COMPLETED
    ]
    assert [e.step_key for e in completed] == ["effect#1"]


async def test_cancel_on_unknown_run_is_a_no_op(store):
    import uuid

    assert await store.request_cancel(uuid.uuid4()) is False
