"""Persistence boundary.

The engine talks only to this protocol. It needs five capabilities from a
backend, and nothing else:

  1. an atomically-sequenced append-only log per run
  2. a compare-and-set on run status
  3. a queue with an atomic claim and a time-based lease
  4. a unique-key insert (for the submission idempotency index)
  5. a read of the full log for one run

Postgres gives all five directly. DynamoDB gives them with a conditional write
on a (run_id, seq) composite key plus a GSI for the queue; Cosmos DB gives them
with an ETag precondition on the run document. Keeping the surface this small is
deliberate: it is the difference between "portable" and "portable in principle".
"""

from __future__ import annotations

import uuid
from typing import Any, Protocol, runtime_checkable

from anchor.models import Budget, Claim, Event, RunRecord, RunStatus, Usage


@runtime_checkable
class Store(Protocol):
    async def migrate(self) -> None: ...

    async def submit_run(
        self,
        workflow: str,
        payload: dict[str, Any],
        idempotency_key: str,
        budget: Budget,
    ) -> tuple[RunRecord, bool]:
        """Insert a run and enqueue it. Returns (run, created).

        `created` is False when the idempotency key already existed, in which
        case the original run is returned untouched.
        """
        ...

    async def get_run(self, run_id: uuid.UUID) -> RunRecord | None: ...

    async def load_events(self, run_id: uuid.UUID) -> list[Event]: ...

    async def append_event(
        self,
        run_id: uuid.UUID,
        type: str,
        step_key: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> int: ...

    async def set_status(
        self,
        run_id: uuid.UUID,
        status: RunStatus,
        *,
        result: Any = None,
        error: dict[str, Any] | None = None,
    ) -> None: ...

    async def save_usage(self, run_id: uuid.UUID, usage: Usage) -> None: ...

    async def request_cancel(self, run_id: uuid.UUID) -> bool: ...

    async def is_cancel_requested(self, run_id: uuid.UUID) -> bool: ...

    async def claim(self, worker_id: str, lease_seconds: float) -> Claim | None: ...

    async def heartbeat(self, run_id: uuid.UUID, worker_id: str, lease_seconds: float) -> bool: ...

    async def release(self, run_id: uuid.UUID, worker_id: str, delay_seconds: float) -> None: ...

    async def dequeue(self, run_id: uuid.UUID) -> None: ...

    async def record_side_effect(
        self, token: uuid.UUID, run_id: uuid.UUID, kind: str, payload: dict[str, Any]
    ) -> bool: ...

    async def find_side_effect(self, token: uuid.UUID) -> dict[str, Any] | None: ...


def build_store(settings=None):
    """Pick a store backend from ANCHOR_BACKEND."""
    from anchor.config import settings as default_settings
    cfg = settings or default_settings

    if cfg.backend == "postgres":
        from anchor.store.postgres import PostgresStore
        return PostgresStore(cfg)
    if cfg.backend == "dynamo":
        from anchor.store.dynamo import DynamoDBStore
        return DynamoDBStore(
            table_name=cfg.dynamo_table,
            queue_url=cfg.dynamo_queue_url,
            region=cfg.dynamo_region,
        )
    raise ValueError(f"unknown ANCHOR_BACKEND: {cfg.backend!r}")
