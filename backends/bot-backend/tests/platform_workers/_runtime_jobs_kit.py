"""Shared setup for the runtime job tests (``app.platform_workers.runtime_jobs``).

The trading side is REAL: a Section 18 TradePlan and its portfolio reservation, the REAL ``CATIExecutionBoundary``
over a REAL ``BinanceExecutor`` whose exchange client is a fake venue (``tests/trading_intelligence/_exec.Harness``:
no network, every order call recorded). The platform side is a disposable platform database of each dialect
(``platform_db`` fixture). Only the edges a test process cannot have are replaced (``FakeServices``): the
production profile / runtime-lease gate, credential resolution and the notification channels.
"""
from __future__ import annotations

import sys
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, List, Optional

_TI = Path(__file__).resolve().parents[1] / "trading_intelligence"
if str(_TI) not in sys.path:
    sys.path.append(str(_TI))

from _exec import Harness, _entry, _order  # noqa: E402

from app.platform_workers.assignments import WorkerAssignments  # noqa: E402
from app.platform_workers.runtime_jobs import (AccountRuntime, RuntimeJobRunner, RuntimeServices,  # noqa: E402
                                               ShadowSource, assignment_fence)
from app.trading_intelligence.execution.boundary import BoundaryStatus  # noqa: E402
from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore  # noqa: E402

ACCOUNT, TENANT = "acct1", "u1"
UNRESOLVED = ("SUBMIT_UNKNOWN", "PENDING_SUBMIT")


_TEMPLATE: dict = {}


def _migrated_copy(path):
    """``_pf.make_db`` without re-running every legacy migration for each test: the first call migrates a
    template database; later calls copy it with SQLite's online backup (consistent under WAL)."""
    import os
    import sqlite3
    import _pf
    from shared_lib.persistence.db import DB
    if "path" not in _TEMPLATE:
        folder = Path(os.environ.get("COSMICFORGE_TEST_DATABASE_PATH", str(path))).parent
        template = folder / f"runtime_jobs_template_{uuid.uuid4().hex[:6]}.db"
        _pf.make_db(template)
        _TEMPLATE["path"] = str(template)
    src, dst = sqlite3.connect(_TEMPLATE["path"]), sqlite3.connect(str(path))
    try:
        src.backup(dst)
    finally:
        src.close()
        dst.close()
    return DB(str(path))


def unknown_entry(tmp_path) -> Harness:
    """A boundary submit whose outcome is unknown: the venue accepted the order (NEW) and every read of it
    times out. The attempt is SUBMIT_UNKNOWN and the reservation RESOLUTION_PENDING."""
    import os
    import _plan
    import shared_lib.persistence.migrations as legacy_migrations
    real_migrate = legacy_migrations.migrate

    def migrate_once(db=None):                  # the session database the Harness migrates every time
        key = os.path.normcase(os.path.abspath(str(getattr(db, "path", db) or "")))
        if key not in _TEMPLATE.setdefault("migrated", set()):
            real_migrate(db)
            _TEMPLATE["migrated"].add(key)

    original, _plan.make_db = _plan.make_db, _migrated_copy
    legacy_migrations.migrate = migrate_once
    try:
        h = Harness(tmp_path, entry=_entry(), order_error=RuntimeError("timeout"))
    finally:
        _plan.make_db = original
        legacy_migrations.migrate = real_migrate
    assert (h.plan.broker_account_id, h.plan.user_id) == (ACCOUNT, TENANT)
    TradePlanEvidenceStore(h.db).append(h.plan)
    result = h.run(h.boundary())
    assert result.status == BoundaryStatus.SUBMIT_UNKNOWN
    assert h.reservation_status() == "RESOLUTION_PENDING" and creates(h) == 1
    h.reads0 = h.client.get_order.call_count
    return h


def venue_reads(h: Harness) -> int:
    """Order reads the fake venue received after the submit (the submit's own fill resolution excluded)."""
    return h.client.get_order.call_count - h.reads0


def creates(h: Harness) -> int:
    """Entry CREATE calls the fake venue received (the only order-creating call of the executor)."""
    return len(h.seen["orders"])


def venue_shows_fill(h: Harness, qty: str = "6.0", price: str = "100.0") -> None:
    """The venue now answers: the order filled and the position is open."""
    h.client.get_order.side_effect = lambda s, o: _order("FILLED", qty, price)
    h.client.get_position_info.return_value = {"positionAmt": qty, "entryPrice": price}
    h.client.user_trades.side_effect = lambda *a, **k: []


def venue_shows_cancel(h: Harness) -> None:
    """The venue now answers: the order ended without a fill; the account is flat."""
    h.client.get_order.side_effect = lambda s, o: _order("CANCELED", "0")
    h.client.get_position_info.return_value = {"positionAmt": "0.0"}
    h.client.user_trades.side_effect = lambda *a, **k: []


def attempt_history(h: Harness) -> List[str]:
    from app.trading_intelligence.contracts.execution import ExecutionAttempt
    from app.trading_intelligence.evidence.stores import ExecutionAttemptStore
    attempt_id = ExecutionAttempt.build_id(trade_plan_id=h.plan.trade_plan_id, trade_plan_hash=h.plan.trade_plan_hash,
                                           broker_account_id=h.plan.broker_account_id)
    return [r["status"] for r in ExecutionAttemptStore(h.db).history(ACCOUNT, attempt_id)]


def attempt_id_of(h: Harness) -> str:
    from app.trading_intelligence.contracts.execution import ExecutionAttempt
    return ExecutionAttempt.build_id(trade_plan_id=h.plan.trade_plan_id, trade_plan_hash=h.plan.trade_plan_hash,
                                     broker_account_id=h.plan.broker_account_id)


class FakeServices(RuntimeServices):
    """Production constructions with the process-level edges replaced. The boundary is the REAL one."""

    def __init__(self, legacy_db: Any, *, h: Optional[Harness] = None, now: Callable[[], int] = None) -> None:
        super().__init__(legacy_db)
        self.h = h
        self.now = now or (lambda: (h.now + 60_000) if h is not None else 0)
        self.lock = threading.RLock()
        self.boundaries = 0
        self.reconciliations = 0
        self.runtime_failures = 0
        self.on_runtime: Optional[Callable[[], None]] = None
        self.ops_sent: List[tuple] = []
        self.ops_failures = 0
        self.sent: List[tuple] = []
        self.send_results: List[bool] = []
        self.shadow: Optional[ShadowSource] = None
        self.gate_calls = 0

    def clock(self) -> int:
        return int(self.now())

    def broker_gate(self, account_id):
        self.gate_calls += 1

    @contextmanager
    def broker_section(self, account_id):
        with self.lock:
            yield

    def shadow_gate(self):
        return None

    def account_runtime(self, account_id):
        if self.on_runtime is not None:
            self.on_runtime()
        if self.runtime_failures:
            self.runtime_failures -= 1
            raise ConnectionError("venue connection reset")
        return AccountRuntime({"id": account_id, "user_id": TENANT, "broker_id": "binance", "environment": "DEMO"},
                              self.h.client)

    def boundary(self, runtime, bot_id):
        self.boundaries += 1
        assert bot_id == self.h.plan.bot_instance_id
        return self.h.boundary()

    def reconcile_account(self, runtime, now_ms):
        """The production account maintenance (``maintain_account``) on the real boundary."""
        from app.trading_intelligence.integration.production_execution import maintain_account
        self.reconciliations += 1
        return maintain_account(self.legacy_db, runtime.account, runtime.client, [], now_ms,
                                boundary_factory=lambda *a: self.h.boundary())

    def ops_channels(self):
        def send_text(subject, text):
            if self.ops_failures:
                self.ops_failures -= 1
                raise ConnectionError("webhook 503")
            self.ops_sent.append((subject, text))
        return SimpleNamespace(send_text=send_text)

    def notification_sender(self):
        def send(channel, recipient, rendered, event):
            ok = self.send_results.pop(0) if self.send_results else True
            if ok:
                self.sent.append((channel, recipient, event["event_id"]))
            return ok
        return send

    def shadow_source(self):
        return self.shadow


class Platform:
    """The platform database with the account registered and workers that own it."""

    def __init__(self, db, *, ttl_ms: int = 30_000) -> None:
        self.db = db
        self.assignments = WorkerAssignments(db, ttl_ms=ttl_ms)
        self.assignments.upsert_accounts([(ACCOUNT, TENANT, "binance", "DEMO")])

    def worker(self, name: Optional[str] = None) -> str:
        wid = self.assignments.register_worker(name or f"w-{uuid.uuid4().hex[:6]}")
        self.assignments.claim(wid)
        return wid

    def owns(self, wid: str) -> bool:
        owner = self.assignments.owner_of(ACCOUNT)
        return owner is not None and owner[0] == wid

    def runner(self, legacy_db, services: FakeServices, wid: str, **kw) -> RuntimeJobRunner:
        kw.setdefault("max_processing_ms", None)
        return RuntimeJobRunner(legacy_db, self.db, worker_id=wid, fence_for_account=assignment_fence(self.assignments, wid),
                                services=services, **kw)

    def due_now(self) -> None:
        """Make every waiting job due now (a test does not wait out a backoff)."""
        with self.db.transaction() as conn:
            conn.execute("UPDATE platform_jobs SET next_attempt_at=0 WHERE status='RETRY'")

    def expire_leases(self) -> None:
        with self.db.transaction() as conn:
            conn.execute("UPDATE platform_jobs SET lease_expires_at=0 WHERE status='CLAIMED'")

    def jobs(self, category: Optional[str] = None) -> List[dict]:
        from app.platform_workers.jobs import JobQueue
        q = JobQueue(self.db)
        with self.db.transaction(write=False) as conn:
            ids = [r[0] for r in conn.all("SELECT job_id FROM platform_jobs" + (" WHERE category=?" if category else "")
                                          + " ORDER BY created_at, job_id", (category,) if category else ())]
        return [q.get(i) for i in ids]


class ShadowWorld:
    """The daily shadow pipeline on synthetic frozen bars (as tests/trading_intelligence/test_step3_cycle_driver),
    on the given legacy database."""

    def __init__(self, db, tmp_path) -> None:
        import _step2
        from app.trading_intelligence.families.daily_trend import spec
        from app.trading_intelligence.families.daily_trend import targets as T
        from app.trading_intelligence.pipeline import daily_trend as DT
        from app.trading_intelligence.pipeline import family_registry as FR
        from app.trading_intelligence.pipeline.accounts import ModelAccounts
        from app.trading_intelligence.pipeline.market_data import FrozenPanelBars

        self.db = db
        self.DT = DT
        self.research = _step2.new_register(tmp_path / f"register_{uuid.uuid4().hex[:6]}.jsonl")
        dataset = _step2.SyntheticDataset()
        self.data = FrozenPanelBars(dataset._load("2020-01-01", "2021-06-30"), dataset.metadata)
        self.registry = FR.FamilyRegistry(db, research_register=self.research)
        self.registry.register_all(actor_ref="system:test")
        self.accounts = ModelAccounts(initial_equity=spec.INTERPRETATION["initial_equity_usdt"],
                                      levels=("balanced", "aggressive"), policies=(T.MANDATE,))
        self.source = ShadowSource(registry=self.registry, data={"1d": self.data}, accounts=self.accounts)
        self.start = DT.day_of("2020-09-01")

    def driver(self, *, compare_models: bool = False):
        from app.trading_intelligence.pipeline import driver as D
        return D.PlatformCycleDriver(self.db, accounts=self.accounts, data={"1d": self.data}, registry=self.registry,
                                     compare_models=compare_models)

    def run(self, day: int, *, compare_models: bool = False):
        return self.driver(compare_models=compare_models).run_cycle(
            "DAILY_TREND", self.DT.day_text(day), now_ms=self.DT.close_ms(day) + 60_000)

    def clock_for(self, day: int) -> int:
        return self.DT.close_ms(day) + 120_000

    def rows(self, day: Optional[int] = None) -> List[tuple]:
        sql = "SELECT account_id, day, reference, status FROM cati_shadow_comparisons"
        args: tuple = ()
        if day is not None:
            sql, args = sql + " WHERE day=?", (day,)
        with self.db.connect() as conn:
            return sorted(tuple(r) for r in conn.execute(sql + " ORDER BY account_id", args))


def run_in_thread(fn: Callable[[], Any], name: str) -> tuple:
    box: dict = {}

    def target():
        try:
            box["result"] = fn()
        except BaseException as exc:  # noqa: BLE001 -- surfaced to the test
            box["error"] = exc

    t = threading.Thread(target=target, name=name, daemon=True)
    t.start()
    return t, box
