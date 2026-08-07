"""The crash window.

A worker dies after a side effect has landed but before the runtime recorded that
it landed. Every durable execution system has this window; the only question is
what it does when it wakes up inside one. These tests pin down all three answers.
"""

from __future__ import annotations

import uuid

from anchor.engine import Engine
from anchor.errors import LeaseLost
from anchor.events import EventType, step_token
from anchor.models import RunStatus, StepIdempotency


async def test_token_is_stable_across_attempts():
    """The whole scheme rests on this: same run, same step, same token."""
    run_id = uuid.uuid4()
    assert step_token(run_id, "refund#1") == step_token(run_id, "refund#1")
    assert step_token(run_id, "refund#1") != step_token(run_id, "refund#2")
    assert step_token(run_id, "refund#1") != step_token(uuid.uuid4(), "refund#1")


async def test_at_most_once_with_verifier_recovers_without_duplicating(store, registry, echo, submit):
    """The good case: ask the downstream system what happened."""
    charges = 0

    async def workflow(ctx, payload):
        async def charge(token, **_):
            nonlocal charges
            charges += 1
            await ctx.store.record_side_effect(token, ctx.run_id, "charge", {"cents": 500})
            # Died here: money moved, nothing recorded on our side.
            raise LeaseLost("worker died after the charge landed")

        async def verify(token, **_):
            return await ctx.store.find_side_effect(token)

        effect = await ctx.step(
            "charge", charge, idempotency=StepIdempotency.AT_MOST_ONCE, verify=verify
        )
        return {"recovered": effect is not None}

    registry.register_workflow("charging", workflow)
    run = await submit("charging")
    engine = Engine(store=store, provider=echo, registry=registry)

    first = await engine.execute(run.run_id)
    assert first.status is RunStatus.RUNNING
    assert await store.count_side_effects(run.run_id, "charge") == 1

    second = await engine.execute(run.run_id)
    assert second.status is RunStatus.COMPLETED
    assert second.result == {"recovered": True}

    assert charges == 1, "the charge body must not run a second time"
    assert await store.count_side_effects(run.run_id, "charge") == 1

    events = await store.load_events(run.run_id)
    recovered = [
        e for e in events if e.type == EventType.STEP_COMPLETED and e.payload.get("recovered_by")
    ]
    assert recovered and recovered[0].payload["recovered_by"] == "verify"


async def test_at_most_once_without_verifier_parks_for_review(store, registry, echo, submit):
    """The honest case: no way to know, so refuse to guess."""
    sends = 0

    async def workflow(ctx, payload):
        async def send_email(token, **_):
            nonlocal sends
            sends += 1
            raise LeaseLost("worker died inside an unverifiable call")

        await ctx.step("send_email", send_email, idempotency=StepIdempotency.AT_MOST_ONCE)
        return "unreachable"

    registry.register_workflow("mailing", workflow)
    run = await submit("mailing")
    engine = Engine(store=store, provider=echo, registry=registry)

    await engine.execute(run.run_id)
    outcome = await engine.execute(run.run_id)

    assert outcome.status is RunStatus.NEEDS_REVIEW
    assert sends == 1, "the runtime must not re-send on an unknown outcome"

    fetched = await store.get_run(run.run_id)
    assert fetched.error["type"] == "AmbiguousStepError"
    assert fetched.error["step_key"] == "send_email#1"
    assert fetched.error["retryable"] is False
    # The token is on the record, so an operator can go look it up by hand.
    assert fetched.error["token"] == str(step_token(run.run_id, "send_email#1"))

    types = [e.type for e in await store.load_events(run.run_id)]
    assert EventType.RUN_NEEDS_REVIEW in types


async def test_at_least_once_step_is_simply_repeated(store, registry, echo, submit):
    """The cheap case: the effect is keyed by our token, so a repeat is free."""
    bodies = 0

    async def workflow(ctx, payload):
        async def idempotent_write(token, **_):
            nonlocal bodies
            bodies += 1
            created = await ctx.store.record_side_effect(
                token, ctx.run_id, "upsert", {"attempt": bodies}
            )
            if bodies == 1:
                raise LeaseLost("worker died after the write landed")
            return {"created": created}

        return await ctx.step(
            "write", idempotent_write, idempotency=StepIdempotency.AT_LEAST_ONCE
        )

    registry.register_workflow("upserting", workflow)
    run = await submit("upserting")
    engine = Engine(store=store, provider=echo, registry=registry)

    await engine.execute(run.run_id)
    outcome = await engine.execute(run.run_id)

    assert outcome.status is RunStatus.COMPLETED
    assert bodies == 2, "at-least-once means the body may run again"
    # And the downstream system deduped it, because the token did not change.
    assert outcome.result == {"created": False}
    assert await store.count_side_effects(run.run_id, "upsert") == 1

    retried = [
        e
        for e in await store.load_events(run.run_id)
        if e.type == EventType.STEP_RETRIED and e.payload.get("reason") == "crash_window"
    ]
    assert len(retried) == 1


async def test_needs_review_is_not_retried_by_a_later_delivery(store, registry, echo, submit):
    """NEEDS_REVIEW is terminal. A redelivery must not quietly resume it."""
    attempts = 0

    async def workflow(ctx, payload):
        nonlocal attempts

        async def risky(token, **_):
            raise LeaseLost("died")

        attempts += 1
        await ctx.step("risky", risky, idempotency=StepIdempotency.AT_MOST_ONCE)
        return "no"

    registry.register_workflow("risky", workflow)
    run = await submit("risky")
    engine = Engine(store=store, provider=echo, registry=registry)

    await engine.execute(run.run_id)
    await engine.execute(run.run_id)
    before = attempts

    third = await engine.execute(run.run_id)
    assert third.status is RunStatus.NEEDS_REVIEW
    assert attempts == before, "the workflow body must not be entered again"


async def test_refund_example_charges_once_across_a_crash(store, submit):
    """End to end on the example workflow, which is the version worth demoing."""
    from anchor.examples import refund_agent as example  # noqa: F401  - registers
    from anchor.providers.echo import EchoProvider
    from anchor.registry import registry as global_registry

    provider = EchoProvider(
        plan=["lookup_payment", "issue_refund", "notify_customer"],
        arguments_from_input=True,
        final_text="refund issued",
    )
    engine = Engine(store=store, provider=provider, registry=global_registry)

    run = await submit(
        "refund_agent", {"payment_id": "pay_001", "amount_cents": 4999, "reason": "test"}
    )

    outcome = await engine.execute(run.run_id)
    assert outcome.status is RunStatus.COMPLETED, outcome.error
    assert await store.count_side_effects(run.run_id, "refund") == 1
    assert await store.count_side_effects(run.run_id, "email") == 1

    # A duplicate delivery of the finished run changes nothing.
    await engine.execute(run.run_id)
    assert await store.count_side_effects(run.run_id, "refund") == 1
