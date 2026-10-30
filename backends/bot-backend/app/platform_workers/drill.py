"""A process-level failure drill for trading workers (Section H, Steps 5.2 / 5.6).

    python -m app.platform_workers.drill worker --url <db> --id <worker> [--ttl-ms 3000]

``DrillVenue`` is a database-backed stand-in for a venue that every worker process sees: an order is a row with a
unique client order id, so a duplicated order is impossible to hide (it would be a second accepted id or a
constraint error, both counted). Each maintenance pass of an account executes one protective-order intent per
epoch through the real ``OrderIntents`` ledger, with the worker's fence. ``scripts/worker_kill_drill.py`` starts
workers as separate processes, kills one with SIGKILL / ``taskkill /F`` in the middle of a cycle and measures the
takeover. This module never reaches a real venue.

The REAL production broker cycle (Step 5.2 / EXT-WRK-01):

    python -m app.platform_workers.drill broker-setup --legacy <file.db> --accounts N
    python -m app.platform_workers.drill broker --url <platform db> --legacy <file.db> --id <worker> --steps <log>

``broker`` runs ``production_runtime.sync`` (registry sync, claim, per-account fence, recovery-only first step,
``sync_account`` -> ``recover_account`` / ``process_account``) under ``BrokerCycleAssignments`` in ``assigned``
mode, over flat DEMO accounts served by ``DrillBrokerClient`` -- a fake that answers the cycle's reads and has no
order method at all; the order-submission gates stay OFF. Every account step is appended to ``--steps`` (JSON
lines, database-clock times) so the orchestrator can prove no two workers ever processed one account at once.
"""
from __future__ import annotations

import argparse
import os
import time
from decimal import Decimal
from typing import Optional

from shared_lib.persistence.sql import Database, migrate

from .assignments import WorkerAssignments
from .jobs import OrderIntents, RetryLater, UnknownSubmitOutcome
from .worker import TradingWorker

DDL = ("CREATE TABLE IF NOT EXISTS drill_venue_orders (client_order_id TEXT PRIMARY KEY, account_id TEXT NOT NULL, "
       "symbol TEXT NOT NULL, side TEXT NOT NULL, quantity TEXT NOT NULL, accepted_at BIGINT NOT NULL, worker_id TEXT)")
EPOCH_MS = 2_000


def ensure(db: Database) -> None:
    from shared_lib.persistence.sql.platform_schema import all_migrations
    migrate(db, all_migrations())
    with db.transaction() as conn:
        conn.execute(DDL)


class DrillVenue:
    def __init__(self, db: Database, worker_id: str, *, lose_ack_every: int = 7) -> None:
        self.db, self.worker_id, self.lose_ack_every, self.n = db, worker_id, lose_ack_every, 0

    def submit(self, *, account_id, symbol, side, quantity, limit_price, client_order_id) -> str:
        with self.db.transaction() as conn:
            conn.execute("INSERT INTO drill_venue_orders (client_order_id, account_id, symbol, side, quantity, accepted_at, "
                         "worker_id) VALUES (?,?,?,?,?,?,?) ON CONFLICT (client_order_id) DO NOTHING",
                         (client_order_id, account_id, symbol, side, str(quantity), conn.now_ms(), self.worker_id))
        self.n += 1
        if self.lose_ack_every and self.n % self.lose_ack_every == 0:
            raise UnknownSubmitOutcome("drill: accepted, acknowledgement lost")
        return "v-" + client_order_id

    def lookup(self, *, account_id, symbol, client_order_id) -> Optional[str]:
        with self.db.transaction(write=False) as conn:
            row = conn.one("SELECT client_order_id FROM drill_venue_orders WHERE client_order_id=?", (client_order_id,))
        return ("v-" + row[0]) if row else None


def run_worker(url: str, worker_id: str, *, ttl_ms: int, interval_s: float, slow_ms: int) -> None:
    db = Database(url, pool_size=8)
    asg = WorkerAssignments(db, ttl_ms=ttl_ms, worker_liveness_ms=ttl_ms)
    venue = DrillVenue(db, worker_id)
    intents = OrderIntents(db, asg, settle_ms=500)

    def key(account_id: str) -> str:
        return f"protect:{account_id}:{int(time.time() * 1000) // EPOCH_MS}"

    def recover(ctx) -> None:
        with db.transaction(write=False) as conn:
            open_keys = [r[0] for r in conn.all("SELECT idempotency_key FROM platform_order_intents WHERE account_id=? "
                                                "AND status='SUBMITTING'", (ctx.account_id,))]
        for k in open_keys:
            try:
                intents.execute(k, venue, fence=ctx.fence)
            except RetryLater:
                pass

    def maintain(ctx) -> None:
        if slow_ms:
            time.sleep(slow_ms / 1000)
        k = key(ctx.account_id)
        intents.prepare(idempotency_key=k, account_id=ctx.account_id, tenant_id=ctx.tenant_id, venue="drill",
                        symbol="BTCUSDT", side="SELL", quantity=Decimal("0.01"), fencing_token=ctx.assignment.fencing_token)
        try:
            intents.execute(k, venue, fence=ctx.fence)
        except RetryLater:
            pass

    worker = TradingWorker(db, maintain=maintain, recover=recover, worker_id=worker_id, assignments=asg,
                           max_concurrency=8, build_version=os.environ.get("BUILD_VERSION", "drill"))
    print(f"READY {worker_id}", flush=True)
    worker.run_forever(interval_s=interval_s)


#: ``broker_accounts`` rows of the broker-cycle drill (flat DEMO accounts without bots or execution lineage).
BROKER_DRILL_PREFIX = "bd"


def _drill_environment(legacy_path: str) -> None:
    """The process environment of a broker-cycle drill process, set BEFORE any application import: the test-mode
    profile the bootstrap of the test suite uses, the drill's own legacy database, and no operator ``.env``
    (the processes run in the drill's work directory)."""
    os.environ.update({"APP_ENV": "TEST", "COSMICFORGE_TEST_MODE": "1", "DATABASE_ROLE": "test",
                       "ENVIRONMENT_NAME": "test", "EXECUTION_MODE": "paper",
                       "DATABASE_URL": "sqlite:///" + legacy_path.replace("\\", "/"),
                       "COSMICFORGE_TEST_DATABASE_PATH": legacy_path})


def setup_broker_legacy(legacy_path: str, accounts: int) -> None:
    _drill_environment(legacy_path)
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate as migrate_legacy
    from app.execution import production_schema
    db = DB(legacy_path)
    migrate_legacy(db)
    production_schema.ensure(db)
    with db.connect() as c:
        for i in range(accounts):
            c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,environment,status,created_at,updated_at) "
                      "VALUES(?,?,'binance','crypto','demo','connected','2026-10-29','2026-10-29')",
                      (f"{BROKER_DRILL_PREFIX}{i:04d}", f"user{i % 13}"))


class DrillBrokerClient:
    """A flat Binance DEMO account as the production broker cycle reads it. It answers every read the cycle makes
    of an account without bots or execution lineage, and it has NO order, protection, cancel or transport method:
    nothing can be mutated through it. ``slow_ms`` pauses the first read of a step, so a hard kill lands inside an
    account step."""

    def __init__(self, account_id: str, user_id: str, *, slow_ms: int = 0) -> None:
        from shared_lib.broker.environment import normalize_environment
        self.broker_environment = normalize_environment("demo")
        self.base_url = "https://demo-fapi.binance.com"
        self._broker_account_id, self._broker_user_id = account_id, user_id
        self.slow_ms = slow_ms

    def account(self) -> dict:
        if self.slow_ms:
            time.sleep(self.slow_ms / 1000)
        return {"totalWalletBalance": "1000", "totalMarginBalance": "1000", "availableBalance": "1000",
                "totalInitialMargin": "0", "totalUnrealizedProfit": "0"}

    def get_balance(self) -> dict:
        return {"wallet": "1000", "equity": "1000", "available": "1000"}

    def position_risk(self, *a, **kw) -> list:
        return [{"symbol": "BTCUSDT", "positionAmt": "0", "positionSide": "BOTH"}]

    def open_orders(self, *a, **kw) -> list:
        return []

    def get_algo_orders(self, *a, **kw) -> list:
        return []


def run_broker_worker(url: str, legacy_path: str, worker_id: str, *, ttl_ms: int, interval_s: float, slow_ms: int,
                      steps_log: str, mode: str = "assigned") -> None:
    _drill_environment(legacy_path)
    import json
    import signal
    import threading
    from types import SimpleNamespace
    from app.core import config
    from app.core.config import Settings
    from app.activation import account_status
    from app.trading_intelligence.integration import production_execution, production_runtime as runtime
    from shared_lib.persistence.db import DB
    from .account_fence import current
    from .broker_assignment import BrokerCycleAssignments
    # The production profile with BOTH order-submission gates off: the drill reads and recovers; it orders nothing.
    profile = Settings(_env_file=None, APP_ENV="PRODUCTION", DATABASE_ROLE="production", ENVIRONMENT_NAME="production",
                       EXECUTION_MODE="live", DEMO_ORDER_SUBMISSION_ENABLED=False, LIVE_ORDER_SUBMISSION_ENABLED=False)
    for module in (config, runtime, production_execution):
        module.settings = profile
    platform = Database(url, pool_size=4)
    legacy = DB(legacy_path)
    runtime.resolve_broker_auth = lambda account_id, user_id, db: SimpleNamespace(
        environment="demo", account_id=account_id, user_id=user_id, broker_type="binance",
        base_url="https://demo-fapi.binance.com", credential_version=1, key_fingerprint="drill")
    account_status.refresh_if_stale = lambda *a, **kw: {"status": "SYNCED"}
    real_sync = runtime.sync_account
    log = open(steps_log, "a", encoding="utf-8")
    log_lock = threading.Lock()

    def db_now() -> int:
        with platform.transaction(write=False) as conn:
            return conn.now_ms()

    def record(**row) -> None:
        with log_lock:
            log.write(json.dumps(row) + "\n")
            log.flush()

    def sync_account(db, account, **kw):
        fence = current()
        record(worker=worker_id, account=account["id"], event="start", at=db_now(),
               token=fence.fencing_token if fence else None, recovery=bool(kw.get("recovery_only")))
        outcome = "ok"
        try:
            return real_sync(db, account, factory=lambda auth: DrillBrokerClient(account["id"], account["user_id"],
                                                                               slow_ms=slow_ms), **kw)
        except Exception as exc:
            outcome = type(exc).__name__
            raise
        finally:
            record(worker=worker_id, account=account["id"], event="end", at=db_now(), outcome=outcome)

    runtime.sync_account = sync_account
    layer = BrokerCycleAssignments(platform, mode=mode, ttl_ms=ttl_ms, margin_ms=max(1, ttl_ms // 5),
                                   worker_id=worker_id, build_version=os.environ.get("BUILD_VERSION", "drill"))
    layer.register()
    from app.ops import runtime_shutdown
    stop = threading.Event()

    def stopping(*_):
        # As the runtime's own graceful stop (``quiesce``): no new account step starts; the one in flight finishes.
        runtime_shutdown.request_stop()
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT, getattr(signal, "SIGBREAK", None)):
        if sig is not None:
            try:
                signal.signal(sig, stopping)
            except ValueError:
                pass
    print(f"READY {worker_id}", flush=True)
    while not stop.is_set():
        try:
            record(worker=worker_id, account=None, event="round", at=db_now())
            runtime.sync(legacy, layer=layer)
            layer.end_round()
        except Exception as exc:  # noqa: BLE001 -- the drill keeps going; the orchestrator judges the outcome
            print(f"ROUND_FAILED {type(exc).__name__}: {exc}", flush=True)
        stop.wait(interval_s)
    layer.drain()                       # graceful stop: every account released with reason DRAIN
    print(f"DRAINED {worker_id}", flush=True)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("worker")
    w.add_argument("--url", required=True)
    w.add_argument("--id", required=True)
    w.add_argument("--ttl-ms", type=int, default=3_000)
    w.add_argument("--interval-s", type=float, default=0.2)
    w.add_argument("--slow-ms", type=int, default=0)
    s = sub.add_parser("broker-setup")
    s.add_argument("--legacy", required=True)
    s.add_argument("--accounts", type=int, default=40)
    b = sub.add_parser("broker")
    b.add_argument("--url", required=True)
    b.add_argument("--legacy", required=True)
    b.add_argument("--id", required=True)
    b.add_argument("--steps", required=True)
    b.add_argument("--ttl-ms", type=int, default=3_000)
    b.add_argument("--interval-s", type=float, default=0.2)
    b.add_argument("--slow-ms", type=int, default=50)
    b.add_argument("--mode", default="assigned")
    a = ap.parse_args(argv)
    if a.cmd == "broker-setup":
        setup_broker_legacy(a.legacy, a.accounts)
    elif a.cmd == "broker":
        run_broker_worker(a.url, a.legacy, a.id, ttl_ms=a.ttl_ms, interval_s=a.interval_s, slow_ms=a.slow_ms,
                          steps_log=a.steps, mode=a.mode)
    else:
        run_worker(a.url, a.id, ttl_ms=a.ttl_ms, interval_s=a.interval_s, slow_ms=a.slow_ms)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
