"""Model provider interface.

Kept narrow on purpose. The runtime only needs a call that takes messages and
tool schemas and returns text, requested tool calls, and usage. Anything richer
would leak provider specifics into the engine, and the engine is the part that
has to stay boring.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class ToolCall:
    id: str
    name: str
    arguments: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "name": self.name, "arguments": self.arguments}

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ToolCall:
        return cls(id=raw["id"], name=raw["name"], arguments=raw.get("arguments") or {})


@dataclass
class ModelResponse:
    text: str = ""
    tool_calls: list[ToolCall] = field(default_factory=list)
    stop_reason: str = "end_turn"
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    model: str = "unknown"

    @property
    def wants_tools(self) -> bool:
        return bool(self.tool_calls)

    def to_dict(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "tool_calls": [t.to_dict() for t in self.tool_calls],
            "stop_reason": self.stop_reason,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cost_usd": self.cost_usd,
            "model": self.model,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ModelResponse:
        return cls(
            text=raw.get("text", ""),
            tool_calls=[ToolCall.from_dict(t) for t in raw.get("tool_calls", [])],
            stop_reason=raw.get("stop_reason", "end_turn"),
            input_tokens=raw.get("input_tokens", 0),
            output_tokens=raw.get("output_tokens", 0),
            cost_usd=raw.get("cost_usd", 0.0),
            model=raw.get("model", "unknown"),
        )


class ModelProvider(Protocol):
    name: str

    async def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        system: str | None = None,
        max_tokens: int = 1024,
    ) -> ModelResponse: ...
