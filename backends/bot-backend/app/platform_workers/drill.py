"""A process-level failure drill for trading workers (Section H, Steps 5.2 / 5.6).

    python -m app.platform_workers.drill worker --url <db> --id <worker> [--ttl-ms 3000]

``DrillVenue`` is a database-backed stand-in for a venue that every worker process sees: an order is a row with a
unique client order id, so a duplicated order is impossible to hide (it would be a second accepted id or a
constraint error, both counted). Each maintenance pass of an account executes one protective-order intent per
epoch through the real ``OrderIntents`` ledger, with the worker's fence. ``scripts/worker_kill_drill.py`` starts
workers as separate processes, kills one with SIGKILL / ``taskkill /F`` in the middle of a cycle and measures the
takeover. This module never reaches a real venue.
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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("worker")
    w.add_argument("--url", required=True)
    w.add_argument("--id", required=True)
    w.add_argument("--ttl-ms", type=int, default=3_000)
    w.add_argument("--interval-s", type=float, default=0.2)
    w.add_argument("--slow-ms", type=int, default=0)
    a = ap.parse_args(argv)
    run_worker(a.url, a.id, ttl_ms=a.ttl_ms, interval_s=a.interval_s, slow_ms=a.slow_ms)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
