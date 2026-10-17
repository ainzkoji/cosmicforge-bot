#!/usr/bin/env python3
"""Step 5 load test at 1, 10, 100 and 1,000 accounts (Section H, Step 5 closing measurement).

    python scripts/step5_load_test.py --url <sqlite:///...drill... | postgresql://...drill...> --out evidence/step5/load_test.json

Per account count, on one platform database (SQLite or PostgreSQL) and replayed synthetic data:

1. shared signal + per-account sizing (``scale.fanout``) on the same market fixture;
2. a maintenance cycle by 4 trading workers (threads) claiming the accounts through fenced assignments; each
   account's maintenance reads its fake venue and checks protection -- per-account stage latency and the cycle;
3. the durable queue: one ORDER job per account through the intent ledger onto fake venues, processed by 4
   processors -- throughput, failed-job rate, duplicates, backlog;
4. platform order spreading: every account requests one symbol's budget, one allocation;
5. worker failure at scale: one of the 4 workers stops; time until its accounts are owned again.

Resources: process CPU time, peak RSS (psutil), pool peak connections and statement counts per stage. Synthetic,
one workstation, fake venues: not exchange-demo throughput and not a Linux certification.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "backends" / "shared"), str(ROOT / "backends" / "bot-backend")]
os.environ.setdefault("COSMICFORGE_TEST_MODE", "1")

import psutil  # noqa: E402

from shared_lib.persistence.sql import Database, migrate  # noqa: E402
from shared_lib.persistence.sql.platform_schema import all_migrations  # noqa: E402

COUNTS = (1, 10, 100, 1000)
WORKERS = 4


def _stage(db, fn):
    proc = psutil.Process()
    s0, c0, t0 = db.counters.statements, proc.cpu_times(), time.perf_counter()
    out = fn()
    c1 = proc.cpu_times()
    return out, {"wall_ms": round((time.perf_counter() - t0) * 1000, 1),
                 "cpu_ms": round(((c1.user - c0.user) + (c1.system - c0.system)) * 1000, 1),
                 "statements": db.counters.statements - s0}


def run_count(url: str, n: int) -> dict:
    from app.exchange.platform_adapter import contract as c
    from app.exchange.platform_adapter.fake import FakeVenue
    from app.golive.synthetic import VenueExecutor, placed_orders
    from app.platform_workers.assignments import WorkerAssignments
    from app.platform_workers.jobs import ORDER, JobProcessor, JobQueue, OrderIntents, order_job_handler
    from app.platform_workers.worker import TradingWorker
    from app.trading_intelligence.pipeline import daily_trend as DT
    from app.trading_intelligence.pipeline import performance as P
    from app.trading_intelligence.scale.fanout import SharedSignal, size_accounts
    from app.trading_intelligence.scale.measure_fanout import DECISION_DAY, _books
    from app.trading_intelligence.scale.order_spreading import BUY, DepthSnapshot, PlatformOrderBudget

    db = Database(url, pool_size=24)
    migrate(db, all_migrations())
    ids = [f"L{n:04d}-{i:05d}" for i in range(n)]
    asg = WorkerAssignments(db, ttl_ms=2_000, worker_liveness_ms=2_000)
    asg.upsert_accounts([(a, f"tenant-{i % 97}", "binance", "DEMO") for i, a in enumerate(ids)])
    venues = {}
    for a in ids:
        v = FakeVenue(c.BINANCE)
        v.add_instrument("BTCUSDT", price=50_000)
        v.external_trade("BTCUSDT", c.BUY, 0.01)
        v.place_order(c.OrderRequest(symbol="BTCUSDT", side=c.SELL, order_type=c.STOP_MARKET, client_order_id=f"s-{a}",
                                     stop_price=45_000.0, close_position=True))
        venues[a] = v
    row = {"accounts": n}

    # 1. signal once, size per account
    panel, metadata = P.synthetic_market()
    signal = SharedSignal.compute(DT.FAMILY_ID, DECISION_DAY, lambda: DT.market_stage(panel, metadata,
                                                                                    DT.day_of(DECISION_DAY)))
    sized = size_accounts(signal, _books(n, signal.market), max_concurrency=1)
    row["signal"] = {"shared_signal_ms": round(signal.computed_ms, 2), "sizing_ms": round(sized.total_ms, 2),
                     "per_account_p95_ms": sized.latency()["p95"], "errors": len(sized.errors)}

    # 2. maintenance cycle by 4 workers
    latencies, lock = [], threading.Lock()

    def maintain(ctx):
        t = time.perf_counter()
        v = venues[ctx.account_id]
        pos = v.positions("BTCUSDT")
        stops = [o for o in v.open_orders("BTCUSDT") if o.order_type == c.STOP_MARKET]
        assert stops or not pos
        with lock:
            latencies.append((time.perf_counter() - t) * 1000)

    workers = [TradingWorker(db, maintain=maintain, recover=lambda ctx: None, worker_id=f"w{k}-{n}", assignments=asg,
                             max_concurrency=4) for k in range(WORKERS)]

    def cycle():
        reports = []
        ts = [threading.Thread(target=lambda w=w: reports.append(w.run_once())) for w in workers]
        [t.start() for t in ts]
        [t.join() for t in ts]
        return reports

    cycle()                                   # first pass: claims + recovery
    latencies.clear()
    reports, st = _stage(db, cycle)           # steady-state maintenance cycle
    owned = sum(r.owned for r in reports)
    lat = sorted(latencies)
    row["maintenance"] = {**st, "owned": owned, "workers": WORKERS, "accounts_maintained": len(lat),
                          "account_stage_p50_ms": round(lat[len(lat) // 2], 3) if lat else None,
                          "account_stage_p95_ms": round(lat[int(len(lat) * 0.95) - 1], 3) if lat else None,
                          "max_worker_cycle_ms": round(max(r.duration_ms for r in reports), 1)}

    # 3. durable queue: one order job per account
    intents = OrderIntents(db, settle_ms=0)
    q = JobQueue(db)
    for i, a in enumerate(ids):
        key = f"load:{n}:{a}"
        intents.prepare(idempotency_key=key, account_id=a, tenant_id=f"tenant-{i % 97}", venue="binance",
                        symbol="BTCUSDT", side="BUY", quantity="0.001")
        q.enqueue(ORDER, key, {"intent_key": key}, account_id=a, tenant_id=f"tenant-{i % 97}")
    executor = VenueExecutor(venues)
    procs = [JobProcessor(q, f"p{k}-{n}", {ORDER: order_job_handler(intents, executor)}) for k in range(WORKERS)]

    def drain_queue():
        done = []

        def loop(p):
            while True:
                r = p.run_once(limit=25)
                if not r:
                    return
                done.extend(r)
        ts = [threading.Thread(target=loop, args=(p,)) for p in procs]
        [t.start() for t in ts]
        [t.join() for t in ts]
        return done

    results, st = _stage(db, drain_queue)
    outcomes = [o for _, o in results]
    dup = sum(1 for i, a in enumerate(ids) if placed_orders(venues[a], intents.load(f"load:{n}:{a}").client_order_id) > 1)
    row["queue"] = {**st, "jobs": n, "succeeded": outcomes.count("SUCCEEDED"),
                    "failed_rate": round(1 - outcomes.count("SUCCEEDED") / max(n, 1), 4),
                    "throughput_jobs_per_s": round(n / max(st["wall_ms"] / 1000, 1e-9), 1),
                    "duplicate_orders": dup, "backlog_after": q.backlog()["by_category"].get(ORDER, {})}

    # 4. order spreading over one symbol
    with db.transaction(write=False) as conn:
        now = conn.now_ms()
    depth = DepthSnapshot.of("binance", "BTCUSDT", [(49_999 - i, 5) for i in range(20)],
                             [(50_000 + i, 5) for i in range(20)], now)
    pb = PlatformOrderBudget(db)

    def spread():
        bid = pb.open_budget(venue="binance", symbol="BTCUSDT", signal_id=f"load-{n}-{now}", side=BUY, depth=depth)
        for i, a in enumerate(ids):
            pb.request(bid, a, f"tenant-{i % 97}", "250")
        return pb.allocate(bid, tick=Decimal("0.1"))

    alloc, st = _stage(db, spread)
    row["order_spreading"] = {**st, "cap_notional": str(alloc.cap), "granted_total": str(alloc.granted_total),
                              "reserved": sum(1 for s in alloc.slots if s.status == "RESERVED"),
                              "deferred": sum(1 for s in alloc.slots if s.status == "DEFERRED"),
                              "within_cap": alloc.granted_total <= alloc.cap,
                              "depth_snapshots_used": 1, "depth_requests_per_account": round(1 / n, 6)}

    # 5. one worker stops; its accounts must be owned again within one TTL
    dead = workers.pop()
    lost = {a for a in ids if (asg.owner_of(a) or (None,))[0] == dead.worker_id}
    t0 = time.perf_counter()
    while True:
        cycle()
        orphans = [a for a in lost if (asg.owner_of(a) or (None,))[0] in (None, dead.worker_id)]
        if not orphans or time.perf_counter() - t0 > 10:
            break
        time.sleep(0.05)
    row["takeover"] = {"accounts_of_stopped_worker": len(lost), "orphans_left": len(orphans),
                       "takeover_ms": round((time.perf_counter() - t0) * 1000), "ttl_ms": 2_000}
    row["resources"] = {"rss_mb": round(psutil.Process().memory_info().rss / 2**20, 1),
                        "pool_peak_connections": db.pool.stats["peak_in_use"], "pool_size": db.pool.size,
                        "statements_total": db.counters.statements, "transactions_total": db.counters.transactions}
    for w in workers:
        w.drain()
    asg.set_worker_state(dead.worker_id, "STOPPED")   # the killed process is gone: it must not count as live
    # retire this round's accounts so the next round measures exactly its own N
    asg.upsert_accounts([(a, f"tenant-{i % 97}", "binance", "DEMO") for i, a in enumerate(ids)], state="REMOVED")
    assert row["maintenance"]["owned"] == n, f"round {n} maintained {row['maintenance']['owned']} accounts"
    db.close()
    return row


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--counts", default=",".join(map(str, COUNTS)))
    a = ap.parse_args(argv)
    if "drill" not in a.url:
        raise SystemExit("refusing: --url must name a throwaway drill database")
    from app.trading_intelligence.pipeline import performance as P
    rows = [run_count(a.url, int(n)) for n in a.counts.split(",")]
    dialect = "postgresql" if a.url.startswith("postgres") else "sqlite"
    out = Path(a.out)
    doc = json.loads(out.read_text()) if out.exists() else {"schema": "cosmicforge-step5-load-v1"}
    doc[dialect] = {"measured_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "host": P.host_description(),
                    "rows": rows, "limits": ("Synthetic replayed data and in-memory fake venues on one Windows "
                                             "workstation; threads for workers. Not exchange-demo throughput, not "
                                             "a hosted staging run, not a Linux certification.")}
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    for r in rows:
        print(json.dumps({"n": r["accounts"], "sizing_ms": r["signal"]["sizing_ms"],
                          "maint_ms": r["maintenance"]["wall_ms"], "owned": r["maintenance"]["owned"],
                          "queue_jobs_s": r["queue"]["throughput_jobs_per_s"], "dups": r["queue"]["duplicate_orders"],
                          "takeover_ms": r["takeover"]["takeover_ms"], "orphans": r["takeover"]["orphans_left"],
                          "pool_peak": r["resources"]["pool_peak_connections"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
