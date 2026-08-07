"""Parallel fan-out.

Keys are allocated in list order before anything is scheduled, so the naming does
not depend on which branch wins the race. That property is what lets a resumed run
line up with the original.
"""

from __future__ import annotations

import asyncio

from anchor.engine import Engine
from anchor.errors import LeaseLost
from anchor.events import EventType
from anchor.models import RunStatus


async def test_gather_runs_branches_concurrently(store, registry, echo, submit):
    async def workflow(ctx, payload):
        async def slow(i):
            async def branch(**_):
                await asyncio.sleep(0.1)
                return i * 10

            return branch

        results = await ctx.gather("fanout", [await slow(i) for i in range(5)])
        return {"results": results}

    registry.register_workflow("fanning", workflow)
    run = await submit("fanning")
    engine = Engine(store=store, provider=echo, registry=registry)

    started = asyncio.get_event_loop().time()
    outcome = await engine.execute(run.run_id)
    elapsed = asyncio.get_event_loop().time() - started

    assert outcome.status is RunStatus.COMPLETED
    assert outcome.result == {"results": [0, 10, 20, 30, 40]}
    assert elapsed < 0.4, f"5 x 100ms branches ran serially ({elapsed:.2f}s)"


async def test_gather_keys_are_deterministic_and_ordered(store, registry, echo, submit):
    async def workflow(ctx, payload):
        async def branch_factory(i):
            async def branch(**_):
                # Reverse the completion order relative to the key order.
                await asyncio.sleep(0.05 * (3 - i))
                return i

            return branch

        return await ctx.gather("job", [await branch_factory(i) for i in range(4)])

    registry.register_workflow("ordering", workflow)
    run = await submit("ordering")
    engine = Engine(store=store, provider=echo, registry=registry)
    outcome = await engine.execute(run.run_id)

    assert outcome.result == [0, 1, 2, 3], "results follow list order, not completion order"

    keys = {
        e.step_key
        for e in await store.load_events(run.run_id)
        if e.type == EventType.STEP_COMPLETED
    }
    assert keys == {"job[0]#1", "job[1]#1", "job[2]#1", "job[3]#1"}


async def test_only_unfinished_branches_re_execute_after_a_crash(store, registry, echo, submit):
    ran: list[int] = []

    async def workflow(ctx, payload):
        async def branch_factory(i):
            async def branch(**_):
                ran.append(i)
                # Branch 2 kills the worker on the first attempt, after branches
                # 0 and 1 have already recorded their results.
                if i == 2 and ctx.run.attempt == 1:
                    raise LeaseLost("died mid-fanout")
                return i

            return branch

        results = await ctx.gather("part", [await branch_factory(i) for i in range(3)])
        return {"results": results}

    registry.register_workflow("partial_fanout", workflow)
    run = await submit("partial_fanout")
    engine = Engine(store=store, provider=echo, registry=registry)

    first = await engine.execute(run.run_id)
    assert first.status is RunStatus.RUNNING

    second = await engine.execute(run.run_id)
    assert second.status is RunStatus.COMPLETED
    assert second.result == {"results": [0, 1, 2]}

    # Branches 0 and 1 ran once in total; branch 2 ran on both attempts.
    assert sorted(ran) == [0, 1, 2, 2]
    assert second.replayed_steps == 2
    assert second.executed_steps == 1
