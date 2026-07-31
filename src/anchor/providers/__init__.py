"""Model providers."""

from __future__ import annotations

from anchor.config import Settings
from anchor.config import settings as default_settings
from anchor.providers.base import ModelProvider, ModelResponse, ToolCall
from anchor.providers.echo import EchoProvider

__all__ = ["EchoProvider", "ModelProvider", "ModelResponse", "ToolCall", "build_provider"]


def build_provider(settings: Settings | None = None) -> ModelProvider:
    """Pick a provider from configuration.

    Defaults to echo so that a fresh clone runs end to end with no credentials.
    """
    settings = settings or default_settings
    kind = settings.model_provider.lower()

    if kind == "echo":
        plan = [name.strip() for name in settings.echo_plan.split(",") if name.strip()]
        return EchoProvider(plan=plan, arguments_from_input=True)
    if kind == "anthropic":
        from anchor.providers.anthropic import AnthropicProvider

        return AnthropicProvider(
            model=settings.anthropic_model,
            api_key=settings.anthropic_api_key,
        )
    raise ValueError(f"unknown ANCHOR_MODEL_PROVIDER: {settings.model_provider!r}")
