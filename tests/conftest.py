"""Test fixtures.

These tests run against a real Postgres, not a fake. The behaviour under test is
FOR UPDATE SKIP LOCKED, lease expiry and unique-constraint collisions, and a
mock of those is a mock of the thing that would break in production.

`make test` starts the database via Docker Compose. Point ANCHOR_TEST_DATABASE_URL
somewhere else if you already have one running.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from anchor.config import Settings
from anchor.models import Budget, StepIdempotency
from anchor.providers.echo import EchoProvider
from anchor.registry import Registry
from anchor.store.postgres import PostgresStore
from anchor.worker import Worker

TEST_DATABASE_URL = os.getenv(
    "ANCHOR_TEST_DATABASE_URL",
    os.getenv("ANCHOR_DATABASE_URL", "postgresql://postgres:anchor@localhost:5432/anchor"),
)


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings(
        database_url=TEST_DATABASE_URL,
        lease_seconds=2.0,
        heartbeat_seconds=0.5,
        poll_interval_seconds=0.05,
        step_max_attempts=3,
        step_backoff_base=0.01,
        worker_concurrency=4,
    )


@pytest_asyncio.fixture
async def store(settings: Settings) -> AsyncIterator[PostgresStore]:
    s = await PostgresStore(settings).connect()
    await s.migrate()
    await s.truncate_all()
    try:
        yield s
    finally:
        await s.close()


@pytest.fixture
def registry() -> Registry:
    """A registry per test, so workflows registered in one test cannot leak."""
    return Registry()


@pytest.fixture
def echo() -> EchoProvider:
    return EchoProvider()


@pytest.fixture
def worker_factory(store: PostgresStore, settings: Settings, registry: Registry):
    def build(provider: EchoProvider | None = None, **overrides) -> Worker:
        return Worker(
            store=store,
            provider=provider or EchoProvider(),
            registry=registry,
            settings=settings,
            **overrides,
        )

    return build


@pytest_asyncio.fixture
async def submit(store: PostgresStore):
    async def _submit(workflow: str, payload: dict | None = None, budget: Budget | None = None):
        run, created = await store.submit_run(
            workflow=workflow,
            payload=payload or {},
            idempotency_key=f"test-{uuid.uuid4()}",
            budget=budget or Budget(),
        )
        assert created
        return run

    return _submit


@pytest.fixture
def at_most_once() -> StepIdempotency:
    return StepIdempotency.AT_MOST_ONCE


@pytest.fixture
def at_least_once() -> StepIdempotency:
    return StepIdempotency.AT_LEAST_ONCE
