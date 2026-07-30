"""Postgres backend.

Two things in here are load-bearing and worth reading closely:

`append_event` bumps `runs.event_seq` and inserts the event in one statement, so
sequence numbers are gapless and unique without a distributed lock. The
(run_id, seq) primary key then acts as a tripwire: if two workers ever believe
they own the same run, the second write fails instead of corrupting history.

`claim` uses FOR UPDATE SKIP LOCKED, which lets N workers poll the same table
without blocking each other and without any of them handing back a row another
worker already took.
"""

from __future__ import annotations

import json
import os
import pathlib
import uuid
from datetime import UTC, datetime
from typing import Any

import asyncpg

from anchor.config import Settings
from anchor.config import settings as default_settings
from anchor.events import EventType
from anchor.models import Budget, Claim, Event, RunRecord, RunStatus, Usage

# Repo layout by default; overridable so an installed wheel can point at a
# schema shipped elsewhere (a config map, a baked image path).
SCHEMA_DIR = pathlib.Path(
    os.getenv("ANCHOR_SCHEMA_DIR", pathlib.Path(__file__).resolve().parents[3] / "schema")
)


async def _init_connection(conn: asyncpg.Connection) -> None:
    """Teach asyncpg to hand us dicts for jsonb instead of raw strings."""
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    await conn.set_type_codec(
        "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


class PostgresStore:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or default_settings
        self.pool: asyncpg.Pool | None = None

    # ---------------------------------------------------------------- lifecycle

    async def connect(self) -> PostgresStore:
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                self.settings.database_url,
                min_size=self.settings.pool_min_size,
                max_size=self.settings.pool_max_size,
                init=_init_connection,
            )
        return self

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    async def __aenter__(self) -> PostgresStore:
        return await self.connect()

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    @property
    def _pool(self) -> asyncpg.Pool:
        if self.pool is None:
            raise RuntimeError("store is not connected; await store.connect() first")
        return self.pool

    async def migrate(self) -> None:
        sql_files = sorted(SCHEMA_DIR.glob("*.sql"))
        if not sql_files:
            raise FileNotFoundError(f"no migrations found in {SCHEMA_DIR}")
        async with self._pool.acquire() as conn:
            for path in sql_files:
                await conn.execute(path.read_text())

    async def truncate_all(self) -> None:
        """Test helper. Never called by the runtime."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                "TRUNCATE side_effects, events, run_queue, runs RESTART IDENTITY CASCADE"
            )

    # -------------------------------------------------------------------- runs

    async def submit_run(
        self,
        workflow: str,
        payload: dict[str, Any],
        idempotency_key: str,
        budget: Budget,
    ) -> tuple[RunRecord, bool]:
        run_id = uuid.uuid4()
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO runs (run_id, workflow, input, idempotency_key, status, budget)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING *
                    """,
                    run_id,
                    workflow,
                    payload,
                    idempotency_key,
                    RunStatus.PENDING.value,
                    budget.to_dict(),
                )
                if row is None:
                    existing = await conn.fetchrow(
                        "SELECT * FROM runs WHERE idempotency_key = $1", idempotency_key
                    )
                    return _to_run(existing), False
                await conn.execute("INSERT INTO run_queue (run_id) VALUES ($1)", run_id)
                await _append_event_conn(
                    conn,
                    run_id,
                    EventType.RUN_STARTED,
                    None,
                    {"workflow": workflow, "input": payload, "budget": budget.to_dict()},
                )
                fresh = await conn.fetchrow("SELECT * FROM runs WHERE run_id = $1", run_id)
        return _to_run(fresh), True

    async def get_run(self, run_id: uuid.UUID) -> RunRecord | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM runs WHERE run_id = $1", run_id)
        return _to_run(row) if row else None

    async def list_runs(self, limit: int = 50, status: str | None = None) -> list[RunRecord]:
        async with self._pool.acquire() as conn:
            if status:
                rows = await conn.fetch(
                    "SELECT * FROM runs WHERE status = $1 ORDER BY created_at DESC LIMIT $2",
                    status,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM runs ORDER BY created_at DESC LIMIT $1", limit
                )
        return [_to_run(r) for r in rows]

    async def set_status(
        self,
        run_id: uuid.UUID,
        status: RunStatus,
        *,
        result: Any = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        finished = status.terminal
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE runs
                   SET status = $2,
                       result = COALESCE($3, result),
                       error = COALESCE($4, error),
                       updated_at = now(),
                       finished_at = CASE WHEN $5 THEN now() ELSE finished_at END
                 WHERE run_id = $1
                """,
                run_id,
                status.value,
                {"value": result} if result is not None else None,
                error,
                finished,
            )

    async def bump_attempt(self, run_id: uuid.UUID) -> int:
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                "UPDATE runs SET attempt = attempt + 1, updated_at = now() "
                "WHERE run_id = $1 RETURNING attempt",
                run_id,
            )

    async def save_usage(self, run_id: uuid.UUID, usage: Usage) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE runs SET usage = $2, updated_at = now() WHERE run_id = $1",
                run_id,
                usage.to_dict(),
            )

    async def request_cancel(self, run_id: uuid.UUID) -> bool:
        """Flip the cancel flag. Cooperative by design: a hard kill mid-step
        would leave exactly the ambiguity this runtime exists to avoid."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                status = await conn.fetchval(
                    "SELECT status FROM runs WHERE run_id = $1 FOR UPDATE", run_id
                )
                if status is None or RunStatus(status).terminal:
                    return False
                await conn.execute(
                    "UPDATE runs SET cancel_requested = TRUE, updated_at = now() WHERE run_id = $1",
                    run_id,
                )
                await _append_event_conn(conn, run_id, EventType.CANCEL_REQUESTED, None, {})
        return True

    async def is_cancel_requested(self, run_id: uuid.UUID) -> bool:
        """Single-column read, called once per step boundary.

        Deliberately not cached: the point of cancellation is that it arrives
        while the run is in flight.
        """
        async with self._pool.acquire() as conn:
            return bool(
                await conn.fetchval(
                    "SELECT cancel_requested FROM runs WHERE run_id = $1", run_id
                )
            )

    # ------------------------------------------------------------------ events

    async def append_event(
        self,
        run_id: uuid.UUID,
        type: str,
        step_key: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> int:
        async with self._pool.acquire() as conn:
            return await _append_event_conn(conn, run_id, type, step_key, payload or {})

    async def load_events(self, run_id: uuid.UUID) -> list[Event]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT seq, type, step_key, payload, created_at FROM events "
                "WHERE run_id = $1 ORDER BY seq",
                run_id,
            )
        return [
            Event(
                seq=r["seq"],
                type=r["type"],
                step_key=r["step_key"],
                payload=r["payload"],
                created_at=r["created_at"],
            )
            for r in rows
        ]

    # ------------------------------------------------------------------- queue

    async def claim(self, worker_id: str, lease_seconds: float) -> Claim | None:
        """Atomically take the oldest claimable run.

        SKIP LOCKED is what makes this safe to run from many workers at once:
        a row already locked by a concurrent claim is skipped rather than waited
        on, so throughput scales with worker count instead of serialising.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE run_queue q
                   SET locked_until = now() + make_interval(secs => $2::float8),
                       worker_id = $1,
                       attempts = q.attempts + 1
                 WHERE q.run_id = (
                       SELECT run_id FROM run_queue
                        WHERE visible_at <= now()
                          AND (locked_until IS NULL OR locked_until < now())
                        ORDER BY visible_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                 )
                RETURNING q.run_id, q.attempts
                """,
                worker_id,
                lease_seconds,
            )
        return Claim(run_id=row["run_id"], attempts=row["attempts"]) if row else None

    async def heartbeat(self, run_id: uuid.UUID, worker_id: str, lease_seconds: float) -> bool:
        """Extend the lease. Returns False if we no longer own it, which is the
        signal to stop working immediately: someone else has taken over."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE run_queue
                   SET locked_until = now() + make_interval(secs => $3::float8)
                 WHERE run_id = $1 AND worker_id = $2 AND locked_until > now()
                RETURNING run_id
                """,
                run_id,
                worker_id,
                lease_seconds,
            )
        return row is not None

    async def release(self, run_id: uuid.UUID, worker_id: str, delay_seconds: float) -> None:
        """Give the run back for someone else to pick up after a delay."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE run_queue
                   SET locked_until = NULL,
                       worker_id = NULL,
                       visible_at = now() + make_interval(secs => $2::float8)
                 WHERE run_id = $1 AND worker_id = $3
                """,
                run_id,
                delay_seconds,
                worker_id,
            )

    async def dequeue(self, run_id: uuid.UUID) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("DELETE FROM run_queue WHERE run_id = $1", run_id)

    async def queue_depth(self) -> dict[str, int]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT count(*) FILTER (WHERE locked_until IS NULL OR locked_until < now())
                           AS claimable,
                       count(*) FILTER (WHERE locked_until >= now()) AS leased,
                       count(*) AS total
                  FROM run_queue
                """
            )
        return dict(row)

    async def status_counts(self) -> dict[str, int]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT status, count(*) AS n FROM runs GROUP BY status")
        return {r["status"]: r["n"] for r in rows}

    # ------------------------------------------------------------ side effects

    async def record_side_effect(
        self, token: uuid.UUID, run_id: uuid.UUID, kind: str, payload: dict[str, Any]
    ) -> bool:
        """Stand-in for a downstream system that dedupes on our token.

        Returns True if this call created the effect, False if the token had
        already been seen. A correct runtime makes False impossible in the happy
        path and harmless in the crash path.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO side_effects (token, run_id, kind, payload)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (token) DO NOTHING
                RETURNING token
                """,
                token,
                run_id,
                kind,
                payload,
            )
        return row is not None

    async def find_side_effect(self, token: uuid.UUID) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM side_effects WHERE token = $1", token)
        if row is None:
            return None
        return {
            "token": str(row["token"]),
            "run_id": str(row["run_id"]),
            "kind": row["kind"],
            "payload": row["payload"],
            "created_at": row["created_at"].isoformat(),
        }

    async def count_side_effects(self, run_id: uuid.UUID, kind: str) -> int:
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT count(*) FROM side_effects WHERE run_id = $1 AND kind = $2",
                run_id,
                kind,
            )


# ------------------------------------------------------------------- internals


async def _append_event_conn(
    conn: asyncpg.Connection,
    run_id: uuid.UUID,
    type: str,
    step_key: str | None,
    payload: dict[str, Any],
) -> int:
    """Bump the counter and write the event in one round trip.

    The data-modifying CTE keeps sequence allocation and the insert in the same
    statement, so there is no window in which a sequence number is reserved but
    unused.
    """
    return await conn.fetchval(
        """
        WITH nxt AS (
            UPDATE runs SET event_seq = event_seq + 1, updated_at = now()
             WHERE run_id = $1
            RETURNING event_seq
        )
        INSERT INTO events (run_id, seq, type, step_key, payload)
        SELECT $1, nxt.event_seq, $2, $3, $4 FROM nxt
        RETURNING seq
        """,
        run_id,
        type,
        step_key,
        payload,
    )


def _to_run(row: asyncpg.Record) -> RunRecord:
    result = row["result"]
    return RunRecord(
        run_id=row["run_id"],
        workflow=row["workflow"],
        input=row["input"],
        idempotency_key=row["idempotency_key"],
        status=RunStatus(row["status"]),
        event_seq=row["event_seq"],
        attempt=row["attempt"],
        budget=Budget.from_dict(row["budget"]),
        usage=Usage.from_dict(row["usage"]),
        result=result.get("value") if isinstance(result, dict) else result,
        error=row["error"],
        cancel_requested=row["cancel_requested"],
        created_at=_utc(row["created_at"]),
        updated_at=_utc(row["updated_at"]),
        finished_at=_utc(row["finished_at"]) if row["finished_at"] else None,
    )


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)
