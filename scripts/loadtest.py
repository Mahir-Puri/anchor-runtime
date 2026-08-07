#!/usr/bin/env python3
"""Throughput and latency harness.

    python scripts/loadtest.py --runs 200 --workers 4

Submits N runs, starts W worker subprocesses, waits for every run to reach a
terminal state, and reports percentiles.

Read the numbers with the caveats in mind. This measures the runtime's overhead
with a scripted offline model, so the model call costs microseconds instead of
seconds. That is deliberate: it isolates the cost of durability (four to six
event-log writes per step, one queue claim, one heartbeat per five seconds) from
the cost of inference, which dominates any real workload and would drown the
signal. Numbers from a laptop are not numbers from production, and the reported
percentiles include worker startup for the earliest runs.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from anchor.config import Settings  # noqa: E402
from anchor.models import Budget, RunStatus  # noqa: E402
from anchor.store.postgres import PostgresStore  # noqa: E402

PLAN = "lookup_payment,issue_refund,notify_customer"


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(int(round((p / 100) * (len(ordered) - 1))), len(ordered) - 1)
    return ordered[index]


def spawn_workers(count: int, concurrency: int) -> list[subprocess.Popen[bytes]]:
    env = dict(os.environ)
    env.update(
        {
            "ANCHOR_ECHO_PLAN": PLAN,
            "ANCHOR_MODEL_PROVIDER": "echo",
            "ANCHOR_WORKER_CONCURRENCY": str(concurrency),
            "ANCHOR_POLL_INTERVAL": "0.05",
            "PYTHONPATH": str(REPO_ROOT / "src"),
        }
    )
    return [
        subprocess.Popen(
            [sys.executable, "-m", "anchor.cli", "worker"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=str(REPO_ROOT),
        )
        for _ in range(count)
    ]


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=200)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=8, help="Runs in flight per worker.")
    parser.add_argument("--timeout", type=float, default=180.0)
    args = parser.parse_args()

    store = await PostgresStore(Settings()).connect()
    await store.migrate()

    print(f"submitting {args.runs} runs...")
    submit_latencies: list[float] = []
    run_ids: list[uuid.UUID] = []
    submit_started = time.monotonic()

    for _ in range(args.runs):
        t0 = time.monotonic()
        run, _ = await store.submit_run(
            workflow="refund_agent",
            payload={"payment_id": "pay_002", "amount_cents": 129900, "reason": "load"},
            idempotency_key=f"load-{uuid.uuid4()}",
            budget=Budget(max_tool_calls=10, max_model_calls=8),
        )
        submit_latencies.append((time.monotonic() - t0) * 1000)
        run_ids.append(run.run_id)

    submit_elapsed = time.monotonic() - submit_started
    print(f"submitted in {submit_elapsed:.2f}s\n")

    print(f"starting {args.workers} worker(s) at concurrency {args.concurrency}...")
    workers = spawn_workers(args.workers, args.concurrency)
    work_started = time.monotonic()

    try:
        deadline = work_started + args.timeout
        while time.monotonic() < deadline:
            counts = await store.status_counts()
            done = sum(counts.get(s, 0) for s in ("COMPLETED", "FAILED", "CANCELLED", "NEEDS_REVIEW"))
            if done >= args.runs:
                break
            await asyncio.sleep(0.2)
        work_elapsed = time.monotonic() - work_started
    finally:
        for worker in workers:
            worker.terminate()
        for worker in workers:
            worker.wait(timeout=15)

    latencies: list[float] = []
    statuses: dict[str, int] = {}
    steps = 0
    for run_id in run_ids:
        run = await store.get_run(run_id)
        statuses[run.status.value] = statuses.get(run.status.value, 0) + 1
        if run.finished_at:
            latencies.append((run.finished_at - run.created_at).total_seconds() * 1000)
        events = await store.load_events(run_id)
        steps += len(events)

    completed = statuses.get(RunStatus.COMPLETED.value, 0)
    await store.close()

    print(f"\nworked {completed}/{args.runs} runs in {work_elapsed:.2f}s\n")
    print(f"statuses:            {statuses}")
    print(f"throughput:          {completed / work_elapsed:.1f} runs/s")
    print(f"event writes:        {steps} ({steps / max(work_elapsed, 1e-9):.0f}/s)")
    print(f"submit  p50 / p99:   {percentile(submit_latencies, 50):.1f} / {percentile(submit_latencies, 99):.1f} ms")
    print(f"e2e     p50 / p99:   {percentile(latencies, 50):.0f} / {percentile(latencies, 99):.0f} ms")
    if latencies:
        print(f"e2e     mean / max:  {statistics.mean(latencies):.0f} / {max(latencies):.0f} ms")
    print("\n(e2e is measured from run creation, so it includes queue wait, not just execution)")
    return 0 if completed == args.runs else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
