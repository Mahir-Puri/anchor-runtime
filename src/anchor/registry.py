"""Registry of workflows and tools.

A workflow is an async function of (ctx, input) -> JSON-serialisable result.
A tool is an async function invoked through ctx, declared with an idempotency
class. The declaration is mandatory rather than defaulted, because the whole
crash-safety story depends on someone having actually thought about whether the
call can be repeated.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from anchor.errors import ToolNotFound, WorkflowNotFound
from anchor.models import StepIdempotency

WorkflowFn = Callable[..., Awaitable[Any]]
ToolFn = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    fn: ToolFn
    description: str
    input_schema: dict[str, Any]
    idempotency: StepIdempotency
    verify: Callable[..., Awaitable[Any]] | None = None

    def to_model_schema(self) -> dict[str, Any]:
        """Shape a provider expects when advertising tools to a model."""
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
        }


class Registry:
    def __init__(self) -> None:
        self._workflows: dict[str, WorkflowFn] = {}
        self._tools: dict[str, ToolSpec] = {}

    # ---------------------------------------------------------------- register

    def register_workflow(self, name: str, fn: WorkflowFn) -> None:
        if name in self._workflows and self._workflows[name] is not fn:
            raise ValueError(f"workflow {name!r} is already registered")
        self._workflows[name] = fn

    def register_tool(self, spec: ToolSpec) -> None:
        if spec.name in self._tools and self._tools[spec.name] is not spec:
            raise ValueError(f"tool {spec.name!r} is already registered")
        self._tools[spec.name] = spec

    # ------------------------------------------------------------------ lookup

    def workflow(self, name: str) -> WorkflowFn:
        try:
            return self._workflows[name]
        except KeyError as exc:
            known = ", ".join(sorted(self._workflows)) or "<none>"
            raise WorkflowNotFound(f"{name!r} not registered (known: {known})") from exc

    def tool(self, name: str) -> ToolSpec:
        try:
            return self._tools[name]
        except KeyError as exc:
            known = ", ".join(sorted(self._tools)) or "<none>"
            raise ToolNotFound(f"{name!r} not registered (known: {known})") from exc

    def tool_schemas(self, names: list[str] | None = None) -> list[dict[str, Any]]:
        names = names if names is not None else sorted(self._tools)
        return [self.tool(n).to_model_schema() for n in names]

    @property
    def workflow_names(self) -> list[str]:
        return sorted(self._workflows)

    @property
    def tool_names(self) -> list[str]:
        return sorted(self._tools)

    def clear(self) -> None:
        self._workflows.clear()
        self._tools.clear()


registry = Registry()


def workflow(name: str) -> Callable[[WorkflowFn], WorkflowFn]:
    def decorate(fn: WorkflowFn) -> WorkflowFn:
        registry.register_workflow(name, fn)
        return fn

    return decorate


def tool(
    name: str,
    *,
    description: str,
    input_schema: dict[str, Any] | None = None,
    idempotency: StepIdempotency,
    verify: Callable[..., Awaitable[Any]] | None = None,
) -> Callable[[ToolFn], ToolFn]:
    """Declare a tool.

    `verify` is the escape hatch for at-most-once tools: given the step's
    idempotency token, it answers "did this already happen?" against the real
    downstream system. Supplying one turns an unrecoverable ambiguity into a
    recoverable one, which is why the refund tool in the example has it.
    """

    def decorate(fn: ToolFn) -> ToolFn:
        registry.register_tool(
            ToolSpec(
                name=name,
                fn=fn,
                description=description,
                input_schema=input_schema or {"type": "object", "properties": {}},
                idempotency=idempotency,
                verify=verify,
            )
        )
        return fn

    return decorate
