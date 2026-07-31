"""Store contract tests.

Everything here is about the guarantees the engine is allowed to assume. If one
of these breaks, exactly-once breaks with it.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from anchor.events import EventType
from anchor.models import Budget, RunStatus


async def test_submit_is_idempotent_on_key(store):
    first, created_first = await store.submit_run("w", {"a": 1}, "key-1", Budget())
    second, created_second = await store.submit_run("w", {"a": 999}, "key-1", Budget())

    assert created_first is True
    assert created_second is False
    assert first.run_id == second.run_id
    # The retry must not overwrite the original input.
    assert second.input == {"a": 1}

    depth = await store.queue_depth()
    assert depth["total"] == 1


async def test_submit_writes_run_started_event(store):
    run, _ = await store.submit_run("w", {}, "key-2", Budget(max_tokens=10))
    events = await store.load_events(run.run_id)
    assert [e.type for e in events] == [EventType.RUN_STARTED]
    assert events[0].payload["budget"] == {"max_tokens": 10}


async def test_event_sequence_is_gapless_under_concurrency(store):
    run, _ = await store.submit_run("w", {}, "key-3", Budget())

    await asyncio.gather(
        *[store.append_event(run.run_id, "Probe", f"s#{i}", {"i": i}) for i in range(25)]
    )

    events = await store.load_events(run.run_id)
    seqs = [e.seq for e in events]
    assert seqs == list(range(1, 27))  # RunStarted plus 25 probes, no gaps, no dupes


async def test_claim_is_exclusive(store):
    run, _ = await store.submit_run("w", {}, "key-4", Budget())

    first = await store.claim("worker-a", lease_seconds=30)
    second = await store.claim("worker-b", lease_seconds=30)

    assert first is not None and first.run_id == run.run_id
    assert second is None, "a leased run must not be handed to a second worker"


async def test_concurrent_claims_never_overlap(store):
    for i in range(10):
        await store.submit_run("w", {}, f"key-batch-{i}", Budget())

    claims = await asyncio.gather(
        *[store.claim(f"worker-{i}", lease_seconds=30) for i in range(10)]
    )
    ids = [c.run_id for c in claims if c is not None]
    assert len(ids) == 10
    assert len(set(ids)) == 10, "SKIP LOCKED must not hand the same row to two claimants"


async def test_expired_lease_is_reclaimable(store):
    run, _ = await store.submit_run("w", {}, "key-5", Budget())

    taken = await store.claim("worker-a", lease_seconds=0.3)
    assert taken is not None
    assert await store.claim("worker-b", lease_seconds=30) is None

    await asyncio.sleep(0.4)

    recovered = await store.claim("worker-b", lease_seconds=30)
    assert recovered is not None
    assert recovered.run_id == run.run_id
    assert recovered.attempts == 2, "attempts should count deliveries, not successes"


async def test_heartbeat_rejects_a_worker_that_lost_the_lease(store):
    run, _ = await store.submit_run("w", {}, "key-6", Budget())
    await store.claim("worker-a", lease_seconds=0.3)
    await asyncio.sleep(0.4)
    await store.claim("worker-b", lease_seconds=30)

    assert await store.heartbeat(run.run_id, "worker-a", 30) is False
    assert await store.heartbeat(run.run_id, "worker-b", 30) is True


async def test_release_makes_a_run_visible_again(store):
    run, _ = await store.submit_run("w", {}, "key-7", Budget())
    await store.claim("worker-a", lease_seconds=30)
    await store.release(run.run_id, "worker-a", delay_seconds=0)

    assert (await store.claim("worker-b", lease_seconds=30)).run_id == run.run_id


async def test_side_effect_token_is_unique(store):
    run, _ = await store.submit_run("w", {}, "key-8", Budget())
    token = uuid.uuid4()

    assert await store.record_side_effect(token, run.run_id, "refund", {"n": 1}) is True
    assert await store.record_side_effect(token, run.run_id, "refund", {"n": 2}) is False
    assert await store.count_side_effects(run.run_id, "refund") == 1

    found = await store.find_side_effect(token)
    assert found["payload"] == {"n": 1}, "the first write wins; the retry is discarded"


async def test_cancel_is_rejected_once_terminal(store):
    run, _ = await store.submit_run("w", {}, "key-9", Budget())
    assert await store.request_cancel(run.run_id) is True

    await store.set_status(run.run_id, RunStatus.COMPLETED, result={"ok": True})
    assert await store.request_cancel(run.run_id) is False


async def test_status_and_result_round_trip(store):
    run, _ = await store.submit_run("w", {}, "key-10", Budget())
    await store.set_status(run.run_id, RunStatus.COMPLETED, result={"answer": 42})

    fetched = await store.get_run(run.run_id)
    assert fetched.status is RunStatus.COMPLETED
    assert fetched.result == {"answer": 42}
    assert fetched.finished_at is not None


@pytest.mark.parametrize("missing", [uuid.uuid4()])
async def test_get_run_returns_none_for_unknown(store, missing):
    assert await store.get_run(missing) is None
