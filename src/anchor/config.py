"""Configuration. Everything is env-driven so the same image runs locally,
on ECS and on Container Apps with no code change."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw else default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw else default


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv(
        "ANCHOR_DATABASE_URL", "postgresql://postgres:anchor@localhost:5432/anchor"
    )
    pool_min_size: int = _env_int("ANCHOR_POOL_MIN", 1)
    pool_max_size: int = _env_int("ANCHOR_POOL_MAX", 10)

    # Lease length. A worker must heartbeat inside this window or lose the run.
    # Short enough that recovery is fast, long enough to survive a GC pause or a
    # slow model call between heartbeats.
    lease_seconds: float = _env_float("ANCHOR_LEASE_SECONDS", 30.0)
    heartbeat_seconds: float = _env_float("ANCHOR_HEARTBEAT_SECONDS", 5.0)
    poll_interval_seconds: float = _env_float("ANCHOR_POLL_INTERVAL", 0.25)

    # Per-step retry policy for transient failures inside a single attempt.
    step_max_attempts: int = _env_int("ANCHOR_STEP_MAX_ATTEMPTS", 3)
    step_backoff_base: float = _env_float("ANCHOR_STEP_BACKOFF_BASE", 0.2)

    model_provider: str = os.getenv("ANCHOR_MODEL_PROVIDER", "echo")
    # Scripted tool sequence for the offline provider, e.g.
    # ANCHOR_ECHO_PLAN=lookup_payment,issue_refund,notify_customer
    echo_plan: str = os.getenv("ANCHOR_ECHO_PLAN", "")
    anthropic_model: str = os.getenv("ANCHOR_ANTHROPIC_MODEL", "claude-sonnet-5")
    anthropic_api_key: str | None = os.getenv("ANTHROPIC_API_KEY")

    worker_concurrency: int = _env_int("ANCHOR_WORKER_CONCURRENCY", 4)

    # Backend selection
    backend: str = os.getenv("ANCHOR_BACKEND", "postgres")  # postgres | dynamo

    # DynamoDB / SQS (only used when backend=dynamo)
    dynamo_table: str = os.getenv("ANCHOR_DYNAMO_TABLE", "anchor")
    dynamo_queue_url: str | None = os.getenv("ANCHOR_DYNAMO_QUEUE_URL")
    dynamo_region: str = os.getenv("ANCHOR_DYNAMO_REGION", "us-east-1")


settings = Settings()
