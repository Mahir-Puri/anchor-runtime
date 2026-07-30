"""Value types shared by the engine, the store and the API."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


class RunStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    # Terminal-but-not-final: the runtime hit a case it refuses to guess at.
    # Distinct from FAILED because the correct response is human review, not retry.
    NEEDS_REVIEW = "NEEDS_REVIEW"

    @property
    def terminal(self) -> bool:
        return self in {
            RunStatus.COMPLETED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
            RunStatus.NEEDS_REVIEW,
        }


class StepIdempotency(str, Enum):
    """How a step may be treated when its outcome is unknown after a crash.

    AT_LEAST_ONCE: safe to re-run. Reads, pure computation, and writes that are
    themselves keyed by the step's idempotency token.

    AT_MOST_ONCE: not safe to re-run and not safe to assume. Moving money,
    sending mail, calling a partner API with no dedupe key. If the outcome is
    unknown the runtime stops rather than choosing between a double charge and
    a silently dropped refund.
    """

    AT_LEAST_ONCE = "at_least_once"
    AT_MOST_ONCE = "at_most_once"


@dataclass(frozen=True)
class Budget:
    """Declared ceilings for a single run.

    Wall clock is measured from run creation, not from the current attempt, so
    a crash-loop cannot silently buy itself more time.
    """

    max_tokens: int | None = None
    max_tool_calls: int | None = None
    max_model_calls: int | None = None
    max_wall_seconds: float | None = None
    max_cost_usd: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> Budget:
        raw = raw or {}
        allowed = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in raw.items() if k in allowed})


@dataclass
class Usage:
    """Accumulated consumption. Persisted on every step boundary so it survives
    a crash, which is the whole point of tracking it in the runtime rather than
    in the agent code."""

    input_tokens: int = 0
    output_tokens: int = 0
    tool_calls: int = 0
    model_calls: int = 0
    cost_usd: float = 0.0

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["total_tokens"] = self.total_tokens
        return d

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> Usage:
        raw = dict(raw or {})
        raw.pop("total_tokens", None)
        allowed = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in raw.items() if k in allowed})


@dataclass
class RunRecord:
    run_id: uuid.UUID
    workflow: str
    input: dict[str, Any]
    idempotency_key: str
    status: RunStatus
    event_seq: int = 0
    attempt: int = 0
    budget: Budget = field(default_factory=Budget)
    usage: Usage = field(default_factory=Usage)
    result: Any = None
    error: dict[str, Any] | None = None
    cancel_requested: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None

    def to_api(self) -> dict[str, Any]:
        return {
            "run_id": str(self.run_id),
            "workflow": self.workflow,
            "status": self.status.value,
            "attempt": self.attempt,
            "idempotency_key": self.idempotency_key,
            "budget": self.budget.to_dict(),
            "usage": self.usage.to_dict(),
            "result": self.result,
            "error": self.error,
            "cancel_requested": self.cancel_requested,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }


@dataclass(frozen=True)
class Event:
    seq: int
    type: str
    step_key: str | None
    payload: dict[str, Any]
    created_at: datetime

    def to_api(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "type": self.type,
            "step_key": self.step_key,
            "payload": self.payload,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True)
class Claim:
    """A leased unit of work."""

    run_id: uuid.UUID
    attempts: int
