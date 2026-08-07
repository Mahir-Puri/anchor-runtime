#!/usr/bin/env python3
"""SIGKILL a worker in the middle of a money-moving step and check the invariant.

This is the demo. Everything else in the repo is in service of the number this
script prints at the end: refunds recorded == 1, on a run whose worker was killed
without warning after the refund had already landed downstream.

    python scripts/chaos_kill.py --kills 3

What it does, per kill:

  1. submits a refund run and starts a real worker subprocess
  2. polls the event log until StepStarted appears for the refund step
  3. sends SIGKILL, so no shutdown hook, no final write, no cleanup
  4. starts a fresh worker, which claims the run once the lease expires
  5. asserts the refund happened exactly once and the run reached COMPLETED

The crash window is widened by ANCHOR_CHAOS_DELAY_SECONDS so step 2 can win the
race deterministically. The runtime behaviour under test is identical either way;
without the delay the window is a few milliseconds wide and the script would
mostly kill workers between steps, which proves less.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from anchor.config import Settings  # noqa: E402
from anchor.events import EventType  # noqa: E402
from anchor.models import Budget, RunStatus  # noqa: E402
from anchor.store.postgres import PostgresStore  # noqa: E402

PLAN = "lookup_payment,issue_refund,notify_customer"


def worker_env(chaos_delay: float, lease_seconds: float) -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "ANCHOR_ECHO_PLAN": PLAN,
            "ANCHOR_MODEL_PROVIDER": "echo",
            "ANCHOR_CHAOS_DELAY_SECONDS": str(chaos_delay),
            "ANCHOR_LEASE_SECONDS": str(lease_seconds),
            "ANCHOR_HEARTBEAT_SECONDS": str(max(lease_seconds / 4, 0.25)),
            "ANCHOR_POLL_INTERVAL": "0.1",
            "PYTHONPATH": str(REPO_ROOT / "src"),
        }
    )
    return env


def spawn_worker(env: dict[str, str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "anchor.cli", "worker"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=str(REPO_ROOT),
    )


async def wait_for_step_started(store: PostgresStore, run_id: uuid.UUID, needle: str, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for event in await store.load_events(run_id):
            if event.type == EventType.STEP_STARTED and needle in (event.step_key or ""):
                return True
        await asyncio.sleep(0.05)
    return False


async def wait_for_terminal(store: PostgresStore, run_id: uuid.UUID, timeout: float):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = await store.get_run(run_id)
        if run and run.status.terminal:
            return run
        await asyncio.sleep(0.1)
    return await store.get_run(run_id)


async def one_trial(store: PostgresStore, trial: int, args: argparse.Namespace) -> dict:
    run, _ = await store.submit_run(
        workflow="refund_agent",
        payload={"payment_id": "pay_002", "amount_cents": 129900, "reason": "chaos"},
        idempotency_key=f"chaos-{uuid.uuid4()}",
        budget=Budget(max_tool_calls=10, max_model_calls=8, max_wall_seconds=120),
    )
    env = worker_env(args.chaos_delay, args.lease_seconds)

    victim = spawn_worker(env)
    landed = await wait_for_step_started(store, run.run_id, "issue_refund", timeout=20)
    if not landed:
        victim.kill()
        raise RuntimeError("worker never reached the refund step; is the database reachable?")

    killed_at = time.monotonic()
    os.kill(victim.pid, signal.SIGKILL)
    victim.wait()

    refunds_at_kill = await store.count_side_effects(run.run_id, "refund")

    # Fresh worker, no shared state with the corpse. It has to recover from the
    # log alone, which is the point.
    rescuer = spawn_worker(env | {"ANCHOR_CHAOS_DELAY_SECONDS": "0"})
    try:
        final = await wait_for_terminal(store, run.run_id, timeout=args.lease_seconds + 30)
        recovery_seconds = time.monotonic() - killed_at
    finally:
        rescuer.terminate()
        rescuer.wait(timeout=10)

    refunds = await store.count_side_effects(run.run_id, "refund")
    events = await store.load_events(run.run_id)
    resumed = sum(1 for e in events if e.type == EventType.RUN_RESUMED)
    recovered_by_verify = sum(
        1 for e in events if e.payload.get("recovered_by") == "verify"
    )

    return {
        "trial": trial,
        "run_id": str(run.run_id),
        "status": final.status.value if final else "UNKNOWN",
        "refunds_at_kill": refunds_at_kill,
        "refunds_final": refunds,
        "attempts": final.attempt if final else 0,
        "resume_events": resumed,
        "recovered_by_verify": recovered_by_verify,
        "recovery_seconds": round(recovery_seconds, 2),
        "events": len(events),
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kills", type=int, default=3, help="How many crash trials to run.")
    parser.add_argument(
        "--chaos-delay",
        type=float,
        default=2.0,
        help="Seconds the refund tool stalls after moving money, widening the crash window.",
    )
    parser.add_argument("--lease-seconds", type=float, default=5.0)
    args = parser.parse_args()

    settings = Settings(lease_seconds=args.lease_seconds)
    store = await PostgresStore(settings).connect()
    await store.migrate()

    print(f"chaos: {args.kills} trial(s), lease={args.lease_seconds}s, window={args.chaos_delay}s\n")
    results = []
    try:
        for i in range(1, args.kills + 1):
            result = await one_trial(store, i, args)
            results.append(result)
            print(
                f"  trial {result['trial']}: status={result['status']:<10} "
                f"refunds_at_kill={result['refunds_at_kill']} refunds_final={result['refunds_final']} "
                f"attempts={result['attempts']} recovery={result['recovery_seconds']}s"
            )
    finally:
        await store.close()

    print()
    duplicated = [r for r in results if r["refunds_final"] != 1]
    unfinished = [r for r in results if r["status"] != RunStatus.COMPLETED.value]
    verified = sum(r["recovered_by_verify"] for r in results)
    recoveries = [r["recovery_seconds"] for r in results]

    print(f"trials:              {len(results)}")
    print(f"duplicate refunds:   {len(duplicated)}")
    print(f"unfinished runs:     {len(unfinished)}")
    print(f"steps recovered by verifier: {verified}")
    if recoveries:
        print(
            f"recovery seconds:    min={min(recoveries)} max={max(recoveries)} "
            f"mean={round(sum(recoveries) / len(recoveries), 2)}"
        )
        print("(recovery is dominated by lease expiry; shorten the lease to shorten it)")

    ok = not duplicated and not unfinished
    print("\nRESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
