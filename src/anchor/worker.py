"""Worker process.

Claims leased work, heartbeats while it runs, and hands the run back if it can no
longer make progress. There is no crash-recovery code in here on purpose: a
worker that dies stops heartbeating, its lease expires, and the next worker to
poll claims the run and replays it. Recovery is the absence of a heartbeat, not a
special code path, which is why `kill -9` and a clean SIGTERM converge on the
same outcome.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import socket
import uuid
from dataclasses import dataclass, field
from typing import Any

from anchor.config import Settings
from anchor.config import settings as default_settings
from anchor.engine import Engine, Outcome
from anchor.models import RunStatus
from anchor.providers import build_provider
from anchor.providers.base import ModelProvider
from anchor.registry import Registry
from anchor.registry import registry as default_registry

log = logging.getLogger("anchor.worker")


def make_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


@dataclass
class WorkerStats:
    claimed: int = 0
    completed: int = 0
    failed: int = 0
    cancelled: int = 0
    needs_review: int = 0
    lease_lost: int = 0
    replayed_steps: int = 0
    by_status: dict[str, int] = field(default_factory=dict)

    def record(self, outcome: Outcome) -> None:
        self.replayed_steps += outcome.replayed_steps
        self.by_status[outcome.status.value] = self.by_status.get(outcome.status.value, 0) + 1
        if outcome.status is RunStatus.COMPLETED:
            self.completed += 1
        elif outcome.status is RunStatus.FAILED:
            self.failed += 1
        elif outcome.status is RunStatus.CANCELLED:
            self.cancelled += 1
        elif outcome.status is RunStatus.NEEDS_REVIEW:
            self.needs_review += 1
        else:
            self.lease_lost += 1


class Worker:
    def __init__(
        self,
        store: Any,
        provider: ModelProvider | None = None,
        registry: Registry | None = None,
        settings: Settings | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.store = store
        self.settings = settings or default_settings
        self.registry = registry or default_registry
        self.provider = provider or build_provider(self.settings)
        self.engine = Engine(
            store=store, provider=self.provider, registry=self.registry, settings=self.settings
        )
        self.worker_id = worker_id or make_worker_id()
        self.stats = WorkerStats()
        self._shutdown = asyncio.Event()
        self._inflight: set[asyncio.Task[None]] = set()

    # -------------------------------------------------------------------- loop

    async def run_forever(self) -> WorkerStats:
        log.info(
            "worker %s up (concurrency=%d, lease=%.1fs, provider=%s)",
            self.worker_id,
            self.settings.worker_concurrency,
            self.settings.lease_seconds,
            self.provider.name,
        )
        try:
            while not self._shutdown.is_set():
                if len(self._inflight) >= self.settings.worker_concurrency:
                    await self._wait_for_slot()
                    continue

                claim = await self.store.claim(self.worker_id, self.settings.lease_seconds)
                if claim is None:
                    await self._sleep_or_shutdown(self.settings.poll_interval_seconds)
                    continue

                self.stats.claimed += 1
                task = asyncio.create_task(self._process(claim.run_id))
                self._inflight.add(task)
                task.add_done_callback(self._inflight.discard)
        finally:
            await self._drain()
        log.info("worker %s down: %s", self.worker_id, self.stats.by_status)
        return self.stats

    async def run_once(self) -> Outcome | None:
        """Claim and process a single run. The unit under test in most tests."""
        claim = await self.store.claim(self.worker_id, self.settings.lease_seconds)
        if claim is None:
            return None
        self.stats.claimed += 1
        return await self._process(claim.run_id, return_outcome=True)

    # ----------------------------------------------------------------- process

    async def _process(self, run_id: uuid.UUID, return_outcome: bool = False) -> Outcome | None:
        heartbeat = asyncio.create_task(self._heartbeat(run_id))
        try:
            outcome = await self.engine.execute(run_id, lease_guard=lambda: self._owns(run_id))
        except Exception:  # noqa: BLE001 - an engine crash must not kill the worker
            log.exception("engine raised while processing %s", run_id)
            await self.store.release(run_id, self.worker_id, self.settings.poll_interval_seconds)
            return None
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

        self.stats.record(outcome)
        if outcome.should_dequeue:
            await self.store.dequeue(run_id)
        else:
            # We lost the lease. Do not touch the queue row; its new owner has it.
            log.warning("run %s left in queue (status=%s)", run_id, outcome.status.value)
        return outcome if return_outcome else None

    async def _heartbeat(self, run_id: uuid.UUID) -> None:
        while True:
            await asyncio.sleep(self.settings.heartbeat_seconds)
            ok = await self.store.heartbeat(run_id, self.worker_id, self.settings.lease_seconds)
            if not ok:
                log.warning("heartbeat rejected for %s; lease is gone", run_id)
                return

    async def _owns(self, run_id: uuid.UUID) -> bool:
        return await self.store.heartbeat(run_id, self.worker_id, self.settings.lease_seconds)

    # ---------------------------------------------------------------- shutdown

    def request_shutdown(self) -> None:
        """SIGTERM path: stop claiming, let in-flight runs finish.

        The point of draining is not politeness. An interrupted run is recoverable
        but costs a replay, and on a rolling deploy that is every run on the box.
        """
        log.info("worker %s draining %d in-flight run(s)", self.worker_id, len(self._inflight))
        self._shutdown.set()

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.request_shutdown)

    async def _drain(self) -> None:
        if self._inflight:
            await asyncio.gather(*list(self._inflight), return_exceptions=True)

    async def _wait_for_slot(self) -> None:
        if not self._inflight:
            return
        await asyncio.wait(list(self._inflight), return_when=asyncio.FIRST_COMPLETED)

    async def _sleep_or_shutdown(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._shutdown.wait(), timeout=seconds)
