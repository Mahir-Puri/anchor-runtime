"""Control plane tests.

The idempotent-submit test is the important one. A client that times out and
retries must not get a second run, and the status code should tell it which
happened without making it parse prose.
"""

from __future__ import annotations

import uuid

import httpx
import pytest_asyncio

from anchor.api import create_app
from anchor.models import RunStatus
from anchor.providers.echo import EchoProvider
from anchor.registry import registry as global_registry
from anchor.worker import Worker


@pytest_asyncio.fixture
async def client(store, settings):
    app = create_app(store=store, settings=settings)
    # ASGITransport does not run the lifespan, so inject the store directly.
    app.state.store = store
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://anchor.test") as c:
        yield c


async def test_healthz_reports_registered_work(client):
    response = await client.get("/healthz")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert "refund_agent" in body["workflows"]
    assert "issue_refund" in body["tools"]


async def test_submit_returns_201_then_200_for_a_retry(client):
    payload = {"workflow": "refund_agent", "input": {"payment_id": "pay_001"}}
    headers = {"Idempotency-Key": "order-4471"}

    first = await client.post("/v1/runs", json=payload, headers=headers)
    assert first.status_code == 201
    assert first.json()["created"] is True

    retry = await client.post("/v1/runs", json=payload, headers=headers)
    assert retry.status_code == 200, "a retry is not a conflict, it is the same run"
    assert retry.json()["created"] is False
    assert retry.json()["run_id"] == first.json()["run_id"]


async def test_submit_without_a_key_creates_distinct_runs(client):
    payload = {"workflow": "refund_agent", "input": {}}
    a = await client.post("/v1/runs", json=payload)
    b = await client.post("/v1/runs", json=payload)
    assert a.json()["run_id"] != b.json()["run_id"]


async def test_unknown_workflow_is_rejected_before_enqueueing(client):
    response = await client.post("/v1/runs", json={"workflow": "nope", "input": {}})
    assert response.status_code == 422
    assert "nope" in response.json()["detail"]


async def test_budget_is_validated(client):
    response = await client.post(
        "/v1/runs",
        json={"workflow": "refund_agent", "input": {}, "budget": {"max_tokens": 0}},
    )
    assert response.status_code == 422


async def test_budget_is_echoed_back(client):
    response = await client.post(
        "/v1/runs",
        json={
            "workflow": "refund_agent",
            "input": {},
            "budget": {"max_tokens": 5000, "max_wall_seconds": 30},
        },
    )
    assert response.json()["budget"] == {"max_tokens": 5000, "max_wall_seconds": 30.0}


async def test_missing_run_is_404(client):
    assert (await client.get(f"/v1/runs/{uuid.uuid4()}")).status_code == 404
    assert (await client.get(f"/v1/runs/{uuid.uuid4()}/events")).status_code == 404
    assert (await client.post(f"/v1/runs/{uuid.uuid4()}/cancel")).status_code == 404


async def test_cancel_then_cancel_again_is_a_conflict(client, store):
    created = await client.post("/v1/runs", json={"workflow": "refund_agent", "input": {}})
    run_id = created.json()["run_id"]

    assert (await client.post(f"/v1/runs/{run_id}/cancel")).status_code == 200

    await store.set_status(uuid.UUID(run_id), RunStatus.CANCELLED)
    conflict = await client.post(f"/v1/runs/{run_id}/cancel")
    assert conflict.status_code == 409
    assert "CANCELLED" in conflict.json()["detail"]


async def test_full_lifecycle_through_the_api(client, store, settings):
    created = await client.post(
        "/v1/runs",
        json={
            "workflow": "refund_agent",
            "input": {"payment_id": "pay_002", "amount_cents": 129900},
            "budget": {"max_tool_calls": 10, "max_model_calls": 6},
        },
        headers={"Idempotency-Key": "lifecycle-1"},
    )
    run_id = created.json()["run_id"]
    assert created.json()["status"] == RunStatus.PENDING.value

    worker = Worker(
        store=store,
        provider=EchoProvider(
            plan=["lookup_payment", "issue_refund", "notify_customer"],
            arguments_from_input=True,
            final_text="refunded",
        ),
        registry=global_registry,
        settings=settings,
        worker_id="api-test",
    )
    outcome = await worker.run_once()
    assert outcome.status is RunStatus.COMPLETED, outcome.error

    final = (await client.get(f"/v1/runs/{run_id}")).json()
    assert final["status"] == RunStatus.COMPLETED.value
    assert final["result"]["summary"] == "refunded"
    assert final["usage"]["tool_calls"] == 3
    assert final["finished_at"] is not None

    events = (await client.get(f"/v1/runs/{run_id}/events")).json()
    types = [e["type"] for e in events["events"]]
    assert types[0] == "RunStarted"
    assert types[-1] == "RunCompleted"
    assert types.count("ToolInvoked") == 3


async def test_metrics_exposes_status_and_queue_gauges(client):
    await client.post("/v1/runs", json={"workflow": "refund_agent", "input": {}})
    body = (await client.get("/metrics")).text

    assert "# TYPE anchor_runs_total gauge" in body
    assert 'anchor_runs_total{status="PENDING"} 1' in body
    assert 'anchor_queue_depth{state="claimable"} 1' in body


async def test_list_runs_filters_by_status(client, store):
    for _ in range(3):
        await client.post("/v1/runs", json={"workflow": "refund_agent", "input": {}})

    all_runs = (await client.get("/v1/runs")).json()["runs"]
    assert len(all_runs) == 3

    await store.set_status(uuid.UUID(all_runs[0]["run_id"]), RunStatus.FAILED)
    failed = (await client.get("/v1/runs", params={"status": "FAILED"})).json()["runs"]
    assert len(failed) == 1
