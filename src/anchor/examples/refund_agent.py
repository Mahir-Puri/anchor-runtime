"""A worked example: an agent that processes a refund request.

This exists to exercise the parts of the runtime that are hard, not to be a good
refund policy. It has one read-only tool, one money-moving tool with a verifier,
and one money-adjacent tool deliberately left without a verifier so the
NEEDS_REVIEW path is reachable and testable.

The agent loop itself is ordinary. That is the argument: durability comes from
the runtime, so the interesting code stays short.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from typing import Any

from anchor.models import StepIdempotency
from anchor.registry import registry, tool, workflow

MAX_TURNS = 8

SYSTEM = (
    "You are a payments support agent. Look up the payment before refunding it. "
    "Refund only the amount that was actually captured, then notify the customer. "
    "When you are finished, reply with a one-line summary and no tool calls."
)

# Stands in for a payments ledger. A real deployment would read the ledger inside
# the tool; nothing else about the workflow changes.
PAYMENTS: dict[str, dict[str, Any]] = {
    "pay_001": {"payment_id": "pay_001", "captured_cents": 4999, "currency": "CAD", "status": "captured"},
    "pay_002": {"payment_id": "pay_002", "captured_cents": 129900, "currency": "CAD", "status": "captured"},
    "pay_003": {"payment_id": "pay_003", "captured_cents": 0, "currency": "CAD", "status": "voided"},
}


@tool(
    "lookup_payment",
    description="Fetch a payment by id. Read-only.",
    input_schema={
        "type": "object",
        "properties": {"payment_id": {"type": "string"}},
        "required": ["payment_id"],
    },
    idempotency=StepIdempotency.AT_LEAST_ONCE,
)
async def lookup_payment(payment_id: str) -> dict[str, Any]:
    payment = PAYMENTS.get(payment_id)
    if payment is None:
        raise LookupError(f"unknown payment {payment_id!r}")
    return payment


async def _refund_landed(ctx: Any, token: uuid.UUID, **_: Any) -> dict[str, Any] | None:
    """Verifier for issue_refund.

    Asks the downstream system whether our token was already accepted. This is
    the difference between a runtime that recovers from a crash mid-refund and
    one that pages a human.
    """
    return await ctx.store.find_side_effect(token)


@tool(
    "issue_refund",
    description="Refund a captured payment. Moves money.",
    input_schema={
        "type": "object",
        "properties": {
            "payment_id": {"type": "string"},
            "amount_cents": {"type": "integer"},
        },
        "required": ["payment_id", "amount_cents"],
    },
    idempotency=StepIdempotency.AT_MOST_ONCE,
    verify=_refund_landed,
)
async def issue_refund(ctx: Any, token: uuid.UUID, payment_id: str, amount_cents: int) -> dict[str, Any]:
    payment = PAYMENTS.get(payment_id)
    if payment is None:
        raise LookupError(f"unknown payment {payment_id!r}")
    if amount_cents <= 0 or amount_cents > payment["captured_cents"]:
        raise ValueError(
            f"refund of {amount_cents} exceeds captured {payment['captured_cents']} on {payment_id}"
        )

    created = await ctx.store.record_side_effect(
        token,
        ctx.run_id,
        "refund",
        {"payment_id": payment_id, "amount_cents": amount_cents},
    )

    # Test scaffolding, not product behaviour. Widens the window between "money
    # moved" and "we recorded that money moved" so scripts/chaos_kill.py can
    # reliably land a SIGKILL inside it. Zero unless explicitly set.
    delay = float(os.getenv("ANCHOR_CHAOS_DELAY_SECONDS", "0"))
    if delay:
        await asyncio.sleep(delay)

    return {
        "payment_id": payment_id,
        "amount_cents": amount_cents,
        "token": str(token),
        # False means the downstream system deduped our retry. Surfaced rather
        # than swallowed, because "we tried twice" is a fact operators want.
        "created": created,
    }


@tool(
    "notify_customer",
    description="Email the customer about their refund. Cannot be deduplicated.",
    input_schema={
        "type": "object",
        "properties": {"payment_id": {"type": "string"}, "message": {"type": "string"}},
        "required": ["payment_id"],
    },
    idempotency=StepIdempotency.AT_MOST_ONCE,
)
async def notify_customer(ctx: Any, token: uuid.UUID, payment_id: str, message: str = "") -> dict[str, Any]:
    """No verifier, on purpose.

    Plenty of real integrations are like this: fire-and-forget, no read-back, no
    dedupe key. If a worker dies inside this window the runtime cannot know
    whether the mail went out, and it will park the run instead of guessing.
    """
    await ctx.store.record_side_effect(
        token, ctx.run_id, "email", {"payment_id": payment_id, "message": message}
    )
    return {"sent": True, "payment_id": payment_id}


@workflow("refund_agent")
async def refund_agent(ctx: Any, payload: dict[str, Any]) -> dict[str, Any]:
    tool_names = ["lookup_payment", "issue_refund", "notify_customer"]
    schemas = registry.tool_schemas(tool_names)

    messages: list[dict[str, Any]] = [{"role": "user", "content": json.dumps(payload)}]
    transcript: list[dict[str, Any]] = []

    for _ in range(MAX_TURNS):
        response = await ctx.model(messages, tools=schemas, system=SYSTEM)

        if not response.wants_tools:
            return {
                "summary": response.text,
                "steps_executed": ctx.executed_steps,
                "steps_replayed": ctx.replayed_steps,
                "transcript": transcript,
            }

        messages.append(
            {
                "role": "assistant",
                "content": (
                    ([{"type": "text", "text": response.text}] if response.text else [])
                    + [
                        {"type": "tool_use", "id": c.id, "name": c.name, "input": c.arguments}
                        for c in response.tool_calls
                    ]
                ),
            }
        )

        results = []
        for call in response.tool_calls:
            output = await ctx.call_tool(call.name, call.arguments)
            transcript.append({"tool": call.name, "output": output})
            results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": call.id,
                    "content": json.dumps(output, default=str),
                }
            )
        messages.append({"role": "user", "content": results})

    return {
        "summary": "turn limit reached without a final answer",
        "steps_executed": ctx.executed_steps,
        "steps_replayed": ctx.replayed_steps,
        "transcript": transcript,
    }
