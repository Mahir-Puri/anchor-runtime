"""Exception hierarchy.

The distinction that matters here is between errors that should retry, errors
that should fail the run, and errors that should stop and ask a human. Most
agent frameworks collapse all three into a generic exception and lose the
ability to make that decision.
"""

from __future__ import annotations


class AnchorError(Exception):
    """Base class for everything raised by the runtime."""


class WorkflowNotFound(AnchorError):
    """A run references a workflow name that is not registered in this worker."""


class ToolNotFound(AnchorError):
    """A model asked for a tool that is not registered."""


class StepFailedError(AnchorError):
    """A step exhausted its retry budget.

    Terminal for the run. The original exception is preserved so the runbook
    can point at a real stack trace.
    """

    def __init__(self, step_key: str, attempts: int, cause: BaseException) -> None:
        super().__init__(f"step {step_key!r} failed after {attempts} attempt(s): {cause!r}")
        self.step_key = step_key
        self.attempts = attempts
        self.cause = cause


class BudgetExceededError(AnchorError):
    """A run hit one of its declared ceilings.

    Terminal, and deliberately not retryable: retrying a run that ran out of
    money costs more money.
    """

    def __init__(self, dimension: str, limit: float, observed: float) -> None:
        super().__init__(f"budget exceeded on {dimension}: {observed} > {limit}")
        self.dimension = dimension
        self.limit = limit
        self.observed = observed


class CancellationRequested(AnchorError):
    """Cooperative cancellation, observed at the next step boundary."""


class AmbiguousStepError(AnchorError):
    """The crash-window case.

    A worker died after writing StepStarted but before writing StepCompleted,
    for a step declared AT_MOST_ONCE with no verifier. The runtime cannot know
    whether the side effect landed, so it refuses to guess: the run parks in
    NEEDS_REVIEW and a human (or a verifier added later) decides.
    """

    def __init__(self, step_key: str, token: str) -> None:
        super().__init__(
            f"step {step_key!r} (token {token}) started but never completed; "
            "outcome unknown and step is declared at-most-once"
        )
        self.step_key = step_key
        self.token = token


class LeaseLost(AnchorError):
    """This worker no longer owns the run and must stop touching it."""
