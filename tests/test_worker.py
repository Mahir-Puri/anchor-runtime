"""Worker and lease behaviour.

The takeover test is the one that matters: it puts two workers on the same run
with a deliberately short lease and asserts that the stale one stops touching
state the moment it notices, and that the new one finishes the job without
repeating completed work.
"""

from __future__ import annotations

import asyncio

from anchor.config import Settings
from anchor.engine import Engine
from anchor.models import RunStatus
from anchor.providers.echo import EchoProvider
from anchor.registry import Registry
from anchor.worker import Worker, make_worker_id


async def test_worker_ids_are_distinct():
    assert make_worker_id() != make_worker_id()


async def test_two_workers_never_process_the_same_run_twice(store, settings, registry, submit):
    """Twelve runs, two workers, twelve side effects. Not thirteen."""

    async def workflow(ctx, payload):
        async def effect(token, **_):
            await asyncio.sleep(0.01)
            return await ctx.store.record_side_effect(
                token, ctx.run_id, "work", {"n": payload.get("n")}
            )

        return {"created": await ctx.step("effect", effect)}

    registry.register_workflow("shared", workflow)
    runs = [await submit("shared", {"n": i}) for i in range(12)]

    workers = [
        Worker(
            store=store,
            provider=EchoProvider(),
            registry=registry,
            settings=settings,
            worker_id=f"worker-{i}",
        )
        for i in range(2)
    ]

    async def drain(worker: Worker) -> None:
        while await worker.run_once() is not None:
            pass

    await asyncio.gather(*[drain(w) for w in workers])

    for run in runs:
        fetched = await store.get_run(run.run_id)
        assert fetched.status is RunStatus.COMPLETED, fetched.error
        assert await store.count_side_effects(run.run_id, "work") == 1
        assert fetched.result == {"created": True}

    assert sum(w.stats.completed for w in workers) == 12
    assert (await store.queue_depth())["total"] == 0


async def test_stale_worker_stands_down_and_the_new_owner_finishes(store, registry, submit):
    """A slow worker loses its lease, notices at the next boundary, and stops."""
    short = Settings(
        database_url=store.settings.database_url,
        lease_seconds=0.3,
        step_backoff_base=0.01,
    )
    executed: list[str] = []

    async def workflow(ctx, payload):
        async def slow(**_):
            executed.append("slow")
            await asyncio.sleep(0.5)  # longer than the lease, with nobody heartbeating
            return "slow-done"

        await ctx.step("slow", slow)
        await ctx.step("after", lambda **_: executed.append("after") or "after-done")
        return "finished"

    registry.register_workflow("slowpoke", workflow)
    run = await submit("slowpoke")
    engine = Engine(store=store, provider=EchoProvider(), registry=registry, settings=short)

    claim_a = await store.claim("worker-a", short.lease_seconds)
    assert claim_a is not None

    async def guard(worker_id: str):
        async def _guard() -> bool:
            return await store.heartbeat(run.run_id, worker_id, short.lease_seconds)

        return _guard

    async def steal_after(delay: float) -> None:
        await asyncio.sleep(delay)
        assert await store.claim("worker-b", 30) is not None

    stale, _ = await asyncio.gather(
        engine.execute(run.run_id, lease_guard=await guard("worker-a")),
        steal_after(0.4),
    )

    assert stale.status is RunStatus.RUNNING
    assert stale.error["type"] == "LeaseLost"
    assert executed == ["slow"], "the stale worker stopped before starting the next step"

    fetched = await store.get_run(run.run_id)
    assert fetched.status is RunStatus.RUNNING, "a lost lease must not mark the run terminal"

    # The new owner replays the completed step and carries on.
    fresh = await engine.execute(run.run_id, lease_guard=await guard("worker-b"))
    assert fresh.status is RunStatus.COMPLETED
    assert fresh.result == "finished"
    assert executed == ["slow", "after"], "completed work was replayed, not repeated"
    assert fresh.replayed_steps == 1


async def test_worker_drains_in_flight_runs_on_shutdown(store, settings, registry, submit):
    started = asyncio.Event()

    async def workflow(ctx, payload):
        async def slow(**_):
            started.set()
            await asyncio.sleep(0.2)
            return "ok"

        return await ctx.step("slow", slow)

    registry.register_workflow("draining", workflow)
    run = await submit("draining")

    worker = Worker(
        store=store,
        provider=EchoProvider(),
        registry=registry,
        settings=settings,
        worker_id="drainer",
    )
    task = asyncio.create_task(worker.run_forever())

    await asyncio.wait_for(started.wait(), timeout=2)
    worker.request_shutdown()
    stats = await asyncio.wait_for(task, timeout=5)

    assert stats.completed == 1
    fetched = await store.get_run(run.run_id)
    assert fetched.status is RunStatus.COMPLETED


async def test_worker_ignores_workflows_it_does_not_know(store, settings, submit):
    """A worker with the wrong code deployed fails the run loudly instead of
    spinning on it forever."""
    await submit("mystery")
    worker = Worker(
        store=store,
        provider=EchoProvider(),
        registry=Registry(),
        settings=settings,
        worker_id="empty",
    )
    outcome = await worker.run_once()

    assert outcome.status is RunStatus.FAILED
    assert outcome.error["type"] == "WorkflowNotFound"
    assert (await store.queue_depth())["total"] == 0
