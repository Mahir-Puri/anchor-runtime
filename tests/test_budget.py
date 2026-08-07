"""Budget enforcement.

Ceilings are checked at step boundaries and usage is persisted as it accrues, so
the numbers survive a restart. The wall-clock case is the one worth reading: the
clock starts at run creation, not at attempt start.
"""

from __future__ import annotations

from datetime import UTC, timedelta

from anchor.budget import BudgetTracker
from anchor.engine import Engine
from anchor.errors import BudgetExceededError, LeaseLost
from anchor.events import EventType
from anchor.models import Budget, RunStatus, StepIdempotency, Usage
from anchor.registry import ToolSpec


async def test_tool_call_ceiling_fails_the_run(store, registry, echo, submit):
    """A ceiling of 3 permits exactly 3 tool executions and blocks the fourth."""
    calls = 0

    async def work():
        nonlocal calls
        calls += 1
        return "ok"

    registry.register_tool(
        ToolSpec(
            name="work",
            fn=work,
            description="does a unit of work",
            input_schema={"type": "object", "properties": {}},
            idempotency=StepIdempotency.AT_LEAST_ONCE,
        )
    )

    async def workflow(ctx, payload):
        for _ in range(10):
            await ctx.call_tool("work")
        return "never"

    registry.register_workflow("greedy", workflow)
    run = await submit("greedy", budget=Budget(max_tool_calls=3))
    engine = Engine(store=store, provider=echo, registry=registry)

    outcome = await engine.execute(run.run_id)
    assert outcome.status is RunStatus.FAILED
    assert outcome.error["type"] == "BudgetExceededError"
    assert outcome.error["dimension"] == "tool_calls"
    assert calls == 3

    types = [e.type for e in await store.load_events(run.run_id)]
    assert EventType.BUDGET_EXCEEDED in types


async def test_model_call_ceiling_stops_an_agent_loop(store, registry, submit):
    from anchor.providers.echo import EchoProvider

    provider = EchoProvider(plan=["a", "b", "c", "d", "e"])

    async def workflow(ctx, payload):
        for _ in range(5):
            await ctx.model([{"role": "user", "content": "go"}])
        return "never"

    registry.register_workflow("looping", workflow)
    run = await submit("looping", budget=Budget(max_model_calls=2))
    engine = Engine(store=store, provider=provider, registry=registry)

    outcome = await engine.execute(run.run_id)
    assert outcome.status is RunStatus.FAILED
    assert outcome.error["dimension"] == "model_calls"

    fetched = await store.get_run(run.run_id)
    assert fetched.usage.model_calls == 2


async def test_usage_is_persisted_as_it_accrues(store, registry, submit):
    from anchor.providers.echo import EchoProvider

    provider = EchoProvider(plan=["a", "b"])

    async def workflow(ctx, payload):
        await ctx.model([{"role": "user", "content": "one"}])
        snapshot = await store.get_run(ctx.run_id)
        # Mid-run read: the numbers are already durable, not buffered in memory.
        assert snapshot.usage.model_calls == 1
        await ctx.model([{"role": "user", "content": "two"}])
        return "ok"

    registry.register_workflow("accruing", workflow)
    run = await submit("accruing")
    engine = Engine(store=store, provider=provider, registry=registry)

    assert (await engine.execute(run.run_id)).status is RunStatus.COMPLETED
    fetched = await store.get_run(run.run_id)
    assert fetched.usage.model_calls == 2
    assert fetched.usage.total_tokens > 0


async def test_budget_survives_a_restart(store, registry, submit):
    """Three attempts must share one budget, not get one each."""
    from anchor.providers.echo import EchoProvider

    provider = EchoProvider(plan=["a", "b", "c", "d"])

    async def workflow(ctx, payload):
        await ctx.model([{"role": "user", "content": "hello"}])
        if ctx.run.attempt <= 2:
            raise LeaseLost("simulated death")
        await ctx.model([{"role": "user", "content": "hello again"}])
        await ctx.model([{"role": "user", "content": "and again"}])
        return "ok"

    registry.register_workflow("restarting", workflow)
    run = await submit("restarting", budget=Budget(max_model_calls=2))
    engine = Engine(store=store, provider=provider, registry=registry)

    await engine.execute(run.run_id)
    await engine.execute(run.run_id)
    outcome = await engine.execute(run.run_id)

    # The first model call was replayed on attempts 2 and 3 rather than re-billed,
    # so the ceiling bites on the genuinely new work.
    assert outcome.status is RunStatus.FAILED
    assert outcome.error["dimension"] == "model_calls"
    fetched = await store.get_run(run.run_id)
    assert fetched.usage.model_calls == 2


async def test_wall_clock_is_measured_from_run_creation(store):
    """A unit test, because faking two minutes of wall clock in an integration
    test is worse than reading the tracker directly."""
    from datetime import datetime

    created = datetime.now(UTC) - timedelta(seconds=120)
    tracker = BudgetTracker(Budget(max_wall_seconds=60), Usage(), created)

    try:
        tracker.check()
    except BudgetExceededError as exc:
        assert exc.dimension == "wall_seconds"
        assert exc.observed > 60
    else:  # pragma: no cover
        raise AssertionError("expected the wall-clock ceiling to fire")


async def test_no_budget_means_no_ceiling(store, registry, echo, submit):
    async def workflow(ctx, payload):
        for _ in range(20):
            await ctx.step("tick", lambda **_: 1)
        return "ok"

    registry.register_workflow("unbounded", workflow)
    run = await submit("unbounded")
    engine = Engine(store=store, provider=echo, registry=registry)
    assert (await engine.execute(run.run_id)).status is RunStatus.COMPLETED
