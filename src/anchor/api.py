"""HTTP control plane.

Submission is idempotent on a client-supplied key: retrying POST /v1/runs after a
timeout returns the original run instead of starting a second one. That is the
first place a durable runtime can lose exactly-once semantics, and it happens
before any of the engine's machinery gets a chance to help.
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import AsyncIterator
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from anchor import examples  # noqa: F401  - registers the example workflow
from anchor.config import Settings
from anchor.config import settings as default_settings
from anchor.models import Budget
from anchor.registry import registry
from anchor.store.postgres import PostgresStore


class BudgetIn(BaseModel):
    max_tokens: int | None = Field(default=None, ge=1)
    max_tool_calls: int | None = Field(default=None, ge=1)
    max_model_calls: int | None = Field(default=None, ge=1)
    max_wall_seconds: float | None = Field(default=None, gt=0)
    max_cost_usd: float | None = Field(default=None, gt=0)


class SubmitRunIn(BaseModel):
    workflow: str
    input: dict[str, Any] = Field(default_factory=dict)
    budget: BudgetIn = Field(default_factory=BudgetIn)


def create_app(store: PostgresStore | None = None, settings: Settings | None = None) -> FastAPI:
    settings = settings or default_settings
    injected = store

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if injected is None:
            app.state.store = await PostgresStore(settings).connect()
            await app.state.store.migrate()
            owns_store = True
        else:
            app.state.store = injected
            owns_store = False
        try:
            yield
        finally:
            if owns_store:
                await app.state.store.close()

    app = FastAPI(
        title="Anchor",
        version="0.1.0",
        summary="Durable execution runtime for LLM agent workflows",
        lifespan=lifespan,
    )

    def get_store() -> PostgresStore:
        return app.state.store

    # ------------------------------------------------------------------- health

    @app.get("/healthz")
    async def healthz(store: PostgresStore = Depends(get_store)) -> dict[str, Any]:
        """Liveness plus a real dependency check.

        A health endpoint that does not touch the database will happily report
        green while every run fails.
        """
        try:
            depth = await store.queue_depth()
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
        return {
            "status": "ok",
            "queue": depth,
            "workflows": registry.workflow_names,
            "tools": registry.tool_names,
        }

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics(store: PostgresStore = Depends(get_store)) -> str:
        """Prometheus exposition format, hand-rolled to keep the dep list short.

        Alert on anchor_runs_total{status="NEEDS_REVIEW"} and on
        anchor_queue_depth{state="claimable"} growth; see docs/RUNBOOK.md.
        """
        counts = await store.status_counts()
        depth = await store.queue_depth()
        lines = [
            "# HELP anchor_runs_total Runs by terminal or current status.",
            "# TYPE anchor_runs_total gauge",
        ]
        for status, n in sorted(counts.items()):
            lines.append(f'anchor_runs_total{{status="{status}"}} {n}')
        lines += [
            "# HELP anchor_queue_depth Queued runs by lease state.",
            "# TYPE anchor_queue_depth gauge",
        ]
        for state, n in sorted(depth.items()):
            lines.append(f'anchor_queue_depth{{state="{state}"}} {n}')
        return "\n".join(lines) + "\n"

    # --------------------------------------------------------------------- runs

    @app.post("/v1/runs", status_code=201)
    async def submit_run(
        body: SubmitRunIn,
        response: Response,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        store: PostgresStore = Depends(get_store),
    ) -> dict[str, Any]:
        if body.workflow not in registry.workflow_names:
            raise HTTPException(
                status_code=422,
                detail=f"unknown workflow {body.workflow!r}; known: {registry.workflow_names}",
            )

        key = idempotency_key or str(uuid.uuid4())
        run, created = await store.submit_run(
            workflow=body.workflow,
            payload=body.input,
            idempotency_key=key,
            budget=Budget.from_dict(body.budget.model_dump(exclude_none=True)),
        )
        if not created:
            # 200 rather than 201, and rather than 409: the caller asked for this
            # run to exist, and it does. Retries should be boring.
            response.status_code = 200
        return {"created": created, **run.to_api()}

    @app.get("/v1/runs")
    async def list_runs(
        limit: int = 50,
        status: str | None = None,
        store: PostgresStore = Depends(get_store),
    ) -> dict[str, Any]:
        runs = await store.list_runs(limit=min(limit, 200), status=status)
        return {"runs": [r.to_api() for r in runs]}

    @app.get("/v1/runs/{run_id}")
    async def get_run(run_id: uuid.UUID, store: PostgresStore = Depends(get_store)) -> dict[str, Any]:
        run = await store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")
        return run.to_api()

    @app.get("/v1/runs/{run_id}/events")
    async def get_events(
        run_id: uuid.UUID, store: PostgresStore = Depends(get_store)
    ) -> dict[str, Any]:
        run = await store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")
        events = await store.load_events(run_id)
        return {"run_id": str(run_id), "count": len(events), "events": [e.to_api() for e in events]}

    @app.post("/v1/runs/{run_id}/cancel")
    async def cancel_run(
        run_id: uuid.UUID, store: PostgresStore = Depends(get_store)
    ) -> dict[str, Any]:
        run = await store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")
        accepted = await store.request_cancel(run_id)
        if not accepted:
            raise HTTPException(
                status_code=409, detail=f"run is already {run.status.value} and cannot be cancelled"
            )
        return {"run_id": str(run_id), "cancel_requested": True}

    return app


app = create_app()
