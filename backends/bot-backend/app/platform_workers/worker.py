"""The trading worker: account maintenance in its own process, separate from the API (Section H, Step 5.2).

One loop iteration (``run_once``):

1. heartbeat and ``claim`` -- renew this worker's accounts, take free or expired ones up to its fair share;
2. for every account it now owns, **recover first** when the assignment is new (``needs_recovery``): the
   ``recover`` callback reconstructs exchange state and durable work (open intents, protective orders) before
   anything new may be submitted for the account; then ``maintain`` (position maintenance, protection,
   reconciliation, risk brakes) with a ``fence`` it must check before each durable mutation;
3. release accounts above the fair share (another worker joined) at this safe point;
4. run due durable jobs.

Accounts are processed with bounded concurrency; one account's exception is isolated from the others.
``drain()`` (SIGTERM / deploy) stops claiming, lets running account work finish, then releases every account
with reason ``DRAIN`` so a successor takes it immediately instead of waiting for expiry. A worker that is
killed without draining loses its accounts when their assignment expires (one TTL) -- measured in
``tests/platform_workers/test_worker_takeover.py`` and the process-kill drill.
"""
from __future__ import annotations

import logging
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional

from shared_lib.persistence.sql import Database

from .assignments import Assignment, AssignmentLost, WorkerAssignments
from .jobs import JobProcessor, JobQueue

logger = logging.getLogger(__name__)


@dataclass
class AccountContext:
    assignment: Assignment
    assignments: WorkerAssignments

    @property
    def account_id(self) -> str:
        return self.assignment.account_id

    @property
    def tenant_id(self) -> str:
        return self.assignment.tenant_id

    @property
    def fence(self) -> tuple:
        return (self.assignment.account_id, self.assignment.worker_id, self.assignment.fencing_token)

    def check(self) -> None:
        """Raise ``AssignmentLost`` if this worker no longer owns the account. Call before each mutation."""
        self.assignments.verify(self.assignment)


Callback = Callable[[AccountContext], Any]


@dataclass
class IterationReport:
    owned: int = 0
    recovered: List[str] = field(default_factory=list)
    maintained: List[str] = field(default_factory=list)
    lost: List[str] = field(default_factory=list)
    failed: Dict[str, str] = field(default_factory=dict)
    released: List[str] = field(default_factory=list)
    jobs: List[tuple] = field(default_factory=list)
    duration_ms: float = 0.0


class TradingWorker:
    def __init__(self, db: Database, *, maintain: Callback, recover: Callback, worker_id: Optional[str] = None,
                 assignments: Optional[WorkerAssignments] = None, job_handlers: Optional[Mapping[str, Any]] = None,
                 max_concurrency: int = 8, build_version: str = "unknown") -> None:
        self.db = db
        self.assignments = assignments or WorkerAssignments(db)
        self.worker_id = self.assignments.register_worker(worker_id, build_version=build_version)
        self.maintain = maintain
        self.recover = recover
        self.max_concurrency = max(1, int(max_concurrency))
        self.jobs = JobProcessor(JobQueue(db), self.worker_id, job_handlers) if job_handlers else None
        self._draining = threading.Event()
        self._pool = ThreadPoolExecutor(max_workers=self.max_concurrency, thread_name_prefix=f"acct-{self.worker_id[-8:]}")

    def _one(self, a: Assignment, report: IterationReport) -> None:
        ctx = AccountContext(a, self.assignments)
        try:
            if a.needs_recovery:
                self.recover(ctx)
                self.assignments.mark_recovered(a)
                report.recovered.append(a.account_id)
            self.maintain(ctx)
            self.assignments.mark_success(a)
            report.maintained.append(a.account_id)
        except AssignmentLost:
            report.lost.append(a.account_id)
        except Exception as exc:
            logger.exception("account %s maintenance failed", a.account_id)
            report.failed[a.account_id] = f"{type(exc).__name__}: {exc}"[:300]

    def run_once(self) -> IterationReport:
        started = time.perf_counter()
        report = IterationReport()
        owned = self.assignments.claim(self.worker_id) if not self._draining.is_set() else []
        report.owned = len(owned)
        futures = [self._pool.submit(self._one, a, report) for a in owned]
        for f in futures:
            f.result()
        surplus = self.assignments.surplus(self.worker_id)
        if surplus:
            self.assignments.release(self.worker_id, surplus, reason="REBALANCE")
            report.released = surplus
        if self.jobs is not None and not self._draining.is_set():
            report.jobs = self.jobs.run_once()
        report.duration_ms = (time.perf_counter() - started) * 1000
        return report

    def drain(self) -> int:
        self._draining.set()
        self.assignments.set_worker_state(self.worker_id, "DRAINING")
        self._pool.shutdown(wait=True)
        released = self.assignments.release(self.worker_id, reason="DRAIN")
        self.assignments.set_worker_state(self.worker_id, "STOPPED")
        return released

    def run_forever(self, *, interval_s: float = 5.0, stop: Optional[threading.Event] = None) -> None:
        stop = stop or threading.Event()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, lambda *_: stop.set())
            except ValueError:  # not the main thread
                pass
        while not stop.is_set():
            try:
                self.run_once()
            except Exception:
                logger.exception("worker iteration failed")
            stop.wait(interval_s)
        self.drain()
