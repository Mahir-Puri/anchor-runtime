"""Command line entry points.

`anchor demo` is the one to run first: it submits a run, works it to completion
with a single worker, and prints the event log so the durability story is visible
without reading any code.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any

import typer

from anchor import examples  # noqa: F401  - registers the example workflow
from anchor.config import settings
from anchor.models import Budget
from anchor.store.postgres import PostgresStore
from anchor.worker import Worker

app = typer.Typer(add_completion=False, help="Anchor durable agent runtime.")


def _setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )


async def _store() -> PostgresStore:
    return await PostgresStore(settings).connect()


@app.command()
def migrate() -> None:
    """Apply the schema. Idempotent, safe to run on every deploy."""

    async def main() -> None:
        store = await _store()
        try:
            await store.migrate()
            typer.echo("schema applied")
        finally:
            await store.close()

    _setup_logging()
    asyncio.run(main())


@app.command()
def worker(
    concurrency: int = typer.Option(None, help="Override ANCHOR_WORKER_CONCURRENCY."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Run a worker until SIGTERM."""

    async def main() -> None:
        store = await _store()
        try:
            await store.migrate()
            w = Worker(store=store)
            if concurrency:
                object.__setattr__(w.settings, "worker_concurrency", concurrency)
            w.install_signal_handlers()
            await w.run_forever()
        finally:
            await store.close()

    _setup_logging(verbose)
    asyncio.run(main())


@app.command()
def api(host: str = "0.0.0.0", port: int = 8000, reload: bool = False) -> None:
    """Serve the control plane."""
    import uvicorn

    _setup_logging()
    uvicorn.run("anchor.api:app", host=host, port=port, reload=reload)


@app.command()
def submit(
    workflow: str = typer.Argument(..., help="Registered workflow name."),
    payload: str = typer.Option("{}", "--input", help="JSON input document."),
    idempotency_key: str = typer.Option(None, help="Defaults to a fresh uuid4."),
    max_tokens: int = typer.Option(None),
    max_tool_calls: int = typer.Option(None),
    max_wall_seconds: float = typer.Option(None),
) -> None:
    """Enqueue a run without going through HTTP."""

    async def main() -> None:
        store = await _store()
        try:
            await store.migrate()
            run, created = await store.submit_run(
                workflow=workflow,
                payload=json.loads(payload),
                idempotency_key=idempotency_key or str(uuid.uuid4()),
                budget=Budget(
                    max_tokens=max_tokens,
                    max_tool_calls=max_tool_calls,
                    max_wall_seconds=max_wall_seconds,
                ),
            )
            typer.echo(json.dumps({"created": created, **run.to_api()}, indent=2, default=str))
        finally:
            await store.close()

    _setup_logging()
    asyncio.run(main())


@app.command()
def tail(run_id: str) -> None:
    """Print the full event log for a run."""

    async def main() -> None:
        store = await _store()
        try:
            rid = uuid.UUID(run_id)
            run = await store.get_run(rid)
            if run is None:
                typer.echo(f"no such run: {run_id}", err=True)
                raise typer.Exit(code=1)
            typer.echo(_format_run(run))
            for event in await store.load_events(rid):
                typer.echo(_format_event(event))
        finally:
            await store.close()

    asyncio.run(main())


@app.command()
def demo(
    payment_id: str = typer.Option("pay_002", help="One of pay_001, pay_002, pay_003."),
    amount_cents: int = typer.Option(129900),
) -> None:
    """Submit one refund run, work it to completion, print the log.

    Uses the offline provider with a scripted tool sequence, so it needs no API
    key and produces the same trajectory every time.
    """
    from anchor.providers.echo import EchoProvider

    async def main() -> None:
        store = await _store()
        try:
            await store.migrate()
            run, _ = await store.submit_run(
                workflow="refund_agent",
                payload={
                    "payment_id": payment_id,
                    "amount_cents": amount_cents,
                    "reason": "customer request",
                },
                idempotency_key=f"demo-{uuid.uuid4()}",
                budget=Budget(max_tokens=50_000, max_tool_calls=10, max_wall_seconds=120),
            )
            typer.echo(f"submitted run {run.run_id}\n")

            provider = EchoProvider(
                plan=["lookup_payment", "issue_refund", "notify_customer"],
                arguments_from_input=True,
                final_text=f"Refunded {amount_cents} cents on {payment_id}.",
            )
            w = Worker(store=store, provider=provider)
            outcome = await w.run_once()

            final = await store.get_run(run.run_id)
            typer.echo(_format_run(final))
            for event in await store.load_events(run.run_id):
                typer.echo(_format_event(event))
            refunds = await store.count_side_effects(run.run_id, "refund")
            typer.echo(
                f"\nstatus={outcome.status.value} executed={outcome.executed_steps} "
                f"replayed={outcome.replayed_steps} refunds_recorded={refunds}"
            )
        finally:
            await store.close()

    _setup_logging()
    asyncio.run(main())


def _format_run(run: Any) -> str:
    return (
        f"run {run.run_id}  workflow={run.workflow}  status={run.status.value}  "
        f"attempt={run.attempt}  usage={json.dumps(run.usage.to_dict())}\n"
        f"{'-' * 96}"
    )


def _format_event(event: Any) -> str:
    detail = json.dumps(event.payload, default=str)
    if len(detail) > 140:
        detail = detail[:137] + "..."
    key = event.step_key or "-"
    return f"  {event.seq:>3}  {event.type:<20}  {key:<28}  {detail}"


if __name__ == "__main__":
    app()
