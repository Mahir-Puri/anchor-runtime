"""Event vocabulary.

The event log is the only source of truth about a run. Everything on the `runs`
row is a cache that can be rebuilt from here, which is what makes replay safe.
"""

from __future__ import annotations

import uuid

# One namespace for the whole product so tokens are stable across deploys.
TOKEN_NAMESPACE = uuid.UUID("6f1b7a4e-3c25-4a8f-9f6f-2f0d1c9b8a77")


class EventType:
    RUN_STARTED = "RunStarted"
    RUN_RESUMED = "RunResumed"
    STEP_STARTED = "StepStarted"
    STEP_COMPLETED = "StepCompleted"
    STEP_RETRIED = "StepRetried"
    STEP_FAILED = "StepFailed"
    MODEL_CALLED = "ModelCalled"
    TOOL_INVOKED = "ToolInvoked"
    BUDGET_EXCEEDED = "BudgetExceeded"
    CANCEL_REQUESTED = "CancelRequested"
    RUN_COMPLETED = "RunCompleted"
    RUN_FAILED = "RunFailed"
    RUN_CANCELLED = "RunCancelled"
    RUN_NEEDS_REVIEW = "RunNeedsReview"


def step_token(run_id: uuid.UUID, step_key: str) -> uuid.UUID:
    """Derive the idempotency token a step hands to the outside world.

    Deterministic in (run_id, step_key), so the token a downstream system sees
    on a replay is byte-identical to the one it saw on the first attempt. That
    is what lets the external system dedupe for us instead of us praying.
    """
    return uuid.uuid5(TOKEN_NAMESPACE, f"{run_id}:{step_key}")
