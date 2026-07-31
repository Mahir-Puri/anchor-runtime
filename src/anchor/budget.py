"""Budget enforcement.

Budgets live in the runtime rather than in agent code for one reason: agent code
is exactly the thing that has just crashed. Usage is persisted at every step
boundary and wall clock is measured from run creation, so a run that restarts
five times gets one budget between all five attempts, not five budgets.
"""

from __future__ import annotations

from datetime import UTC, datetime

from anchor.errors import BudgetExceededError
from anchor.models import Budget, Usage


class BudgetTracker:
    def __init__(self, budget: Budget, usage: Usage, run_created_at: datetime) -> None:
        self.budget = budget
        self.usage = usage
        self.run_created_at = run_created_at

    @property
    def elapsed_seconds(self) -> float:
        return (datetime.now(UTC) - self.run_created_at).total_seconds()

    def check(self) -> None:
        """Raise if any ceiling is already breached.

        Called before a step executes, never before a step is replayed from the
        log: replaying costs nothing, and failing a run partway through recovery
        would strand work that had already been paid for.
        """
        b = self.budget
        if b.max_wall_seconds is not None and self.elapsed_seconds > b.max_wall_seconds:
            raise BudgetExceededError("wall_seconds", b.max_wall_seconds, round(self.elapsed_seconds, 3))
        if b.max_tokens is not None and self.usage.total_tokens > b.max_tokens:
            raise BudgetExceededError("tokens", b.max_tokens, self.usage.total_tokens)
        if b.max_tool_calls is not None and self.usage.tool_calls >= b.max_tool_calls:
            raise BudgetExceededError("tool_calls", b.max_tool_calls, self.usage.tool_calls)
        if b.max_model_calls is not None and self.usage.model_calls >= b.max_model_calls:
            raise BudgetExceededError("model_calls", b.max_model_calls, self.usage.model_calls)
        if b.max_cost_usd is not None and self.usage.cost_usd > b.max_cost_usd:
            raise BudgetExceededError("cost_usd", b.max_cost_usd, round(self.usage.cost_usd, 6))

    # Counters. Deliberately dumb; the interesting behaviour is in check().

    def add_model_usage(self, input_tokens: int, output_tokens: int, cost_usd: float) -> None:
        self.usage.input_tokens += input_tokens
        self.usage.output_tokens += output_tokens
        self.usage.cost_usd += cost_usd
        self.usage.model_calls += 1

    def add_tool_call(self) -> None:
        self.usage.tool_calls += 1

    def snapshot(self) -> dict[str, float | int]:
        return {
            **self.usage.to_dict(),
            "elapsed_seconds": round(self.elapsed_seconds, 3),
        }
