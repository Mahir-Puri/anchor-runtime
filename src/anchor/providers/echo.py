"""A deterministic stand-in for a real model.

Every test in this repo runs against this provider, and so does the chaos
script. That is deliberate: crash-recovery behaviour has to be reproducible, and
you cannot assert "the replayed run produced an identical trajectory" against a
sampler. Point the runtime at a real model with ANCHOR_MODEL_PROVIDER=anthropic
when you want to watch it work for real.

The decision rule is a function of the message list alone, so the same
conversation prefix always yields the same next action.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from anchor.providers.base import ModelProvider, ModelResponse, ToolCall


class EchoProvider(ModelProvider):
    name = "echo"

    def __init__(
        self,
        plan: list[str] | None = None,
        *,
        arguments: dict[str, dict[str, Any]] | None = None,
        final_text: str = "done",
        arguments_from_input: bool = False,
        latency_seconds: float = 0.0,
        fail_first_n: int = 0,
    ) -> None:
        self.plan = plan or []
        self.arguments = arguments or {}
        self.final_text = final_text
        self.arguments_from_input = arguments_from_input
        self.latency_seconds = latency_seconds
        self.fail_first_n = fail_first_n
        self._failures = 0

    async def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        system: str | None = None,
        max_tokens: int = 1024,
    ) -> ModelResponse:
        if self._failures < self.fail_first_n:
            self._failures += 1
            raise ConnectionError(f"simulated provider failure #{self._failures}")

        if self.latency_seconds:
            await asyncio.sleep(self.latency_seconds)

        turn = sum(1 for m in messages if m.get("role") == "assistant")
        prompt_chars = sum(len(str(m.get("content", ""))) for m in messages)

        if turn < len(self.plan):
            name = self.plan[turn]
            arguments = dict(self._input_arguments(messages)) if self.arguments_from_input else {}
            arguments.update(self.arguments.get(name, {}))
            call = ToolCall(id=f"call_{turn}", name=name, arguments=arguments)
            return ModelResponse(
                text=f"calling {name}",
                tool_calls=[call],
                stop_reason="tool_use",
                input_tokens=prompt_chars // 4,
                output_tokens=12,
                cost_usd=0.0,
                model="echo",
            )

        return ModelResponse(
            text=self.final_text,
            stop_reason="end_turn",
            input_tokens=prompt_chars // 4,
            output_tokens=len(self.final_text) // 4 + 1,
            cost_usd=0.0,
            model="echo",
        )

    @staticmethod
    def _input_arguments(messages: list[dict[str, Any]]) -> dict[str, Any]:
        """Read the first user message as a JSON object, if it is one.

        Lets a scripted run pass realistic arguments (payment ids, amounts)
        through to tools without the provider knowing anything about them. Tools
        receive only the parameters they declare, so extra keys are harmless.
        """
        for message in messages:
            content = message.get("content")
            if message.get("role") == "user" and isinstance(content, str):
                try:
                    parsed = json.loads(content)
                except (TypeError, ValueError):
                    return {}
                return parsed if isinstance(parsed, dict) else {}
        return {}
