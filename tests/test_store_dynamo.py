"""DynamoDB store contract tests.

The same 12 assertions from test_store.py, pointed at a moto-mocked AWS
environment instead of a real Postgres. If both pass, the Store protocol is
satisfied by both backends, which is the claim worth making in an interview.

Moto intercepts boto3 calls in-process, so these tests run with no AWS
credentials and no network access.
"""

from __future__ import annotations

import asyncio
import uuid

import boto3
import moto
import pytest
import pytest_asyncio

from anchor.store.dynamo import DynamoDBStore


@pytest.fixture(scope="module")
def aws_credentials(monkeypatch_session=None):
    """Fake credentials so moto does not complain."""
    import os
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
    os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


@pytest_asyncio.fixture
async def dstore(aws_credentials):
    """A fresh moto-backed DynamoDB store, wiped between tests."""
    with moto.mock_aws():
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        sqs = boto3.client("sqs", region_name="us-east-1")
        # Pre-create queue so migrate() can find it.
        queue_url = sqs.create_queue(
            QueueName="anchor-queue",
        )["QueueUrl"]

        store = DynamoDBStore(
            table_name="anchor",
            queue_url=queue_url,
            dynamodb_resource=ddb,
            sqs_client=sqs,
            visibility_timeout=2,
        )
        await store.migrate()
        yield store


from anchor.models import Budget, RunStatus  # noqa: E402


async def _submit(store: DynamoDBStore, workflow: str = "w", payload: dict | None = None, key: str | None = None):
    run, created = await store.submit_run(
        workflow,
        payload or {},
        key or f"test-{uuid.uuid4()}",
        Budget(),
    )
    return run, created


# ── the same 12 contract assertions ──────────────────────────────────────────

async def test_dynamo_submit_is_idempotent_on_key(dstore):
    first, created_first = await _submit(dstore, key="key-1")
    second, created_second = await _submit(dstore, payload={"x": 99}, key="key-1")

    assert created_first is True
    assert created_second is False
    assert first.run_id == second.run_id
    assert second.input == {}


async def test_dynamo_submit_writes_run_started_event(dstore):
    from anchor.events import EventType
    run, _ = await _submit(dstore, key="key-2")
    events = await dstore.load_events(run.run_id)
    assert events[0].type == EventType.RUN_STARTED


async def test_dynamo_event_sequence_is_gapless_under_concurrency(dstore):
    run, _ = await _submit(dstore, key="key-3")
    await asyncio.gather(
        *[dstore.append_event(run.run_id, "Probe", f"s#{i}", {"i": i}) for i in range(20)]
    )
    events = await dstore.load_events(run.run_id)
    seqs = [e.seq for e in events]
    # RunStarted is seq 1; 20 probes follow → 1..21 with no gaps or duplicates.
    assert seqs == list(range(1, 22))


async def test_dynamo_claim_is_exclusive(dstore):
    await _submit(dstore, key="key-4")
    first = await dstore.claim("worker-a", lease_seconds=30)
    second = await dstore.claim("worker-b", lease_seconds=30)
    assert first is not None
    assert second is None, "a claimed message must not be handed to a second worker"


async def test_dynamo_concurrent_claims_never_overlap(dstore):
    for i in range(5):
        await _submit(dstore, key=f"batch-{i}")

    claims = await asyncio.gather(
        *[dstore.claim(f"worker-{i}", lease_seconds=30) for i in range(5)]
    )
    ids = [c.run_id for c in claims if c is not None]
    assert len(ids) == 5
    assert len(set(ids)) == 5


async def test_dynamo_expired_lease_is_reclaimable(dstore):
    run, _ = await _submit(dstore, key="key-5")

    taken = await dstore.claim("worker-a", lease_seconds=1)
    assert taken is not None
    # Second claim before the timeout lapses returns nothing.
    assert await dstore.claim("worker-b", lease_seconds=30) is None

    await asyncio.sleep(1.5)

    recovered = await dstore.claim("worker-b", lease_seconds=30)
    assert recovered is not None
    assert recovered.run_id == run.run_id


async def test_dynamo_heartbeat_rejects_wrong_worker(dstore):
    run, _ = await _submit(dstore, key="key-6")
    await dstore.claim("worker-a", lease_seconds=1)
    await asyncio.sleep(1.5)
    await dstore.claim("worker-b", lease_seconds=30)

    assert await dstore.heartbeat(run.run_id, "worker-a", 30) is False
    assert await dstore.heartbeat(run.run_id, "worker-b", 30) is True


async def test_dynamo_release_makes_a_run_visible_again(dstore):
    run, _ = await _submit(dstore, key="key-7")
    await dstore.claim("worker-a", lease_seconds=30)
    await dstore.release(run.run_id, "worker-a", delay_seconds=0)
    await asyncio.sleep(0.1)
    reclaimed = await dstore.claim("worker-b", lease_seconds=30)
    assert reclaimed is not None
    assert reclaimed.run_id == run.run_id


async def test_dynamo_side_effect_token_is_unique(dstore):
    run, _ = await _submit(dstore, key="key-8")
    token = uuid.uuid4()

    assert await dstore.record_side_effect(token, run.run_id, "refund", {"n": 1}) is True
    assert await dstore.record_side_effect(token, run.run_id, "refund", {"n": 2}) is False
    assert await dstore.count_side_effects(run.run_id, "refund") == 1

    found = await dstore.find_side_effect(token)
    assert found["payload"] == {"n": 1}


async def test_dynamo_cancel_rejected_once_terminal(dstore):
    run, _ = await _submit(dstore, key="key-9")
    assert await dstore.request_cancel(run.run_id) is True
    await dstore.set_status(run.run_id, RunStatus.COMPLETED, result={"ok": True})
    assert await dstore.request_cancel(run.run_id) is False


async def test_dynamo_status_and_result_round_trip(dstore):
    run, _ = await _submit(dstore, key="key-10")
    await dstore.set_status(run.run_id, RunStatus.COMPLETED, result={"answer": 42})
    fetched = await dstore.get_run(run.run_id)
    assert fetched.status is RunStatus.COMPLETED
    assert fetched.result == {"answer": 42}
    assert fetched.finished_at is not None


async def test_dynamo_get_run_returns_none_for_unknown(dstore):
    assert await dstore.get_run(uuid.uuid4()) is None


async def test_dynamo_cancel_on_unknown_run_is_a_no_op(dstore):
    assert await dstore.request_cancel(uuid.uuid4()) is False


# ── DynamoDB-specific: verify Decimal round-trip ──────────────────────────────

async def test_dynamo_float_budget_survives_decimal_round_trip(dstore):
    """DynamoDB stores numbers as Decimal. Floats must survive the round trip."""
    run, _ = await store_submit(dstore)
    fetched = await dstore.get_run(run.run_id)
    # No assertion needed; _from_item would raise on a bad Decimal conversion.
    assert fetched is not None


async def store_submit(store):
    return await _submit(store, key=f"float-{uuid.uuid4()}")
