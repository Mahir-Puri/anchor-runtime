"""Anthropic provider.

Optional. The runtime never imports this unless ANCHOR_MODEL_PROVIDER=anthropic,
so the test suite and CI run with no API key and no network.

Pricing is read from env rather than hardcoded, because a number baked into
source is a number that will be wrong in three months. Set
ANCHOR_PRICE_IN_PER_MTOK / ANCHOR_PRICE_OUT_PER_MTOK from current pricing if you
want the cost budget to mean anything; leave them unset and cost stays 0 while
token budgets still work.
"""

from __future__ import annotations

import os
from typing import Any

from anchor.providers.base import ModelProvider, ModelResponse, ToolCall


class AnthropicProvider(ModelProvider):
    name = "anthropic"

    def __init__(
        self,
        model: str,
        api_key: str | None = None,
        price_in_per_mtok: float | None = None,
        price_out_per_mtok: float | None = None,
    ) -> None:
        try:
            from anthropic import AsyncAnthropic
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "the anthropic package is required for ANCHOR_MODEL_PROVIDER=anthropic; "
                "pip install 'anchor-runtime[anthropic]'"
            ) from exc

        key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not key:
            raise RuntimeError("ANTHROPIC_API_KEY is not set")

        self.model = model
        self.client = AsyncAnthropic(api_key=key)
        self.price_in = price_in_per_mtok if price_in_per_mtok is not None else _price("IN")
        self.price_out = price_out_per_mtok if price_out_per_mtok is not None else _price("OUT")

    async def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        system: str | None = None,
        max_tokens: int = 1024,
    ) -> ModelResponse:
        kwargs: dict[str, Any] = {
            "model": self.model,
            "max_tokens": max_tokens,
            "messages": messages,
        }
        if tools:
            kwargs["tools"] = tools
        if system:
            kwargs["system"] = system

        message = await self.client.messages.create(**kwargs)

        text_parts: list[str] = []
        calls: list[ToolCall] = []
        for block in message.content:
            if block.type == "text":
                text_parts.append(block.text)
            elif block.type == "tool_use":
                calls.append(ToolCall(id=block.id, name=block.name, arguments=dict(block.input)))

        in_tokens = message.usage.input_tokens
        out_tokens = message.usage.output_tokens
        cost = (in_tokens / 1_000_000) * self.price_in + (out_tokens / 1_000_000) * self.price_out

        return ModelResponse(
            text="".join(text_parts),
            tool_calls=calls,
            stop_reason=message.stop_reason or "end_turn",
            input_tokens=in_tokens,
            output_tokens=out_tokens,
            cost_usd=cost,
            model=self.model,
        )


def _price(direction: str) -> float:
    raw = os.getenv(f"ANCHOR_PRICE_{direction}_PER_MTOK")
    return float(raw) if raw else 0.0
