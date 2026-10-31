#!/usr/bin/env python3
"""Account-scale measurement of the platform layer (master prompt section 20).

    python scripts/perf/account_scale.py --counts 1,10,100,1000 --out-dir evidence/predeploy
    python scripts/perf/account_scale.py --pg-admin-url postgresql://user@127.0.0.1:55433/postgres   (optional)

``--pg-admin-url`` names a DISPOSABLE PostgreSQL server (an ``initdb`` cluster, never a shared service): for each
count a throwaway ``cf_scale_drill_*`` database is created, measured and dropped, and its row is written next to
the SQLite row of the same count.

For each account count N, on ONE throwaway platform database (a fresh SQLite file in a scratch directory unless
``--url`` names a drill database) and a throwaway legacy SQLite file (the request budget and REST
reconciliation ledgers live there), with one ``FakeVenue`` per account spread over binance / bybit / bingx:

1.  shared signal -> per-account sizing (``scale.fanout``): the family market stage is built ONCE per cycle
    (counted, and asserted) and every account is sized from it;
2.  steady-state maintenance cycle by W trading workers (``TradingWorker`` threads, fenced assignments):
    per-account cycle latency p50 / p95 / max;
3.  REST reconciliation (``RestReconciler``) of every account through ``BudgetedVenueAdapter`` on ONE egress
    address per venue: reconciliation latency and the exchange request budget used per venue (granted, refused,
    capacity, ceilings) -- the request budget is shared by every account behind one address;
4.  the durable queue: one ORDER job per account through ``OrderIntents`` onto the fake venues, 10 % of them
    with the acknowledgement LOST after the venue acted (resolved by lookup, never resent);
5.  notification throughput: one NOTIFICATION job per account (+ duplicate enqueues, which must coalesce);
6.  symbol-level order spreading (``PlatformOrderBudget.allocate``): the shared cap, the slot schedule;
7.  the entry gate at N: the production default policy refuses every MARKET entry
    ``MARKET_FALLBACK_DISABLED_BY_POLICY``; an uncalibrated market-permitting test policy lets ONE account per
    venue/symbol/side through and refuses the rest ``ORDER_SPREADING_CALIBRATION_REQUIRED``;
8.  failure cases at N: worker killed mid-cycle (lease takeover), database restart (connections dropped,
    unreachable, back), user-stream disconnect of every account, emergency flatten during load, queue consumer
    crash, interrupted deployment (the existing drain / handover: old-version workers drained while new-version
    workers run), stale market data (every entry refused ``DEPTH_STALE``);
9.  resources: process CPU time per stage, tracemalloc peak (a separate pass, so the timings are not inflated by
    tracing), RSS, database connections opened (pool ``created``) and peak in use, statements.

Every figure is SYNTHETIC: in-memory fake venues, synthetic accounts and data, worker threads in one process, on
the workstation recorded in the file (Windows). They are NOT venue evidence, NOT exchange-demo throughput, NOT a
Linux certification and NOT a hosted-staging run. The CI test (``tests/platform_workers/test_account_scale.py``)
runs this same code at <= 100 accounts and asserts only structural properties; the 1,000-account numbers come
from running this script on the workstation and are recorded in ``evidence/predeploy/scale_1000_accounts.json``.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import tracemalloc
import uuid
from collections import Counter, deque
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[2]
for _p in (ROOT / "backends" / "shared", ROOT / "backends" / "bot-backend"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))
os.environ.setdefault("COSMICFORGE_TEST_MODE", "1")

COUNTS = (1, 10, 100, 1000)
WORKERS = 4
SYMBOL = "BTCUSDT"
PRICE = 50_000.0
EGRESS = "egress-scale-drill-1"
LOST_ACK_EVERY = 10
#: queue orders are resting limit BUYs far below the market: each is a real venue order with a client order id,
#: and none fills, so the positions (and their protection) are exactly what the fleet set up
RESTING_LIMIT = "40000"
NOT_EVIDENCE = ("SYNTHETIC: in-memory FakeVenue accounts, synthetic data and worker threads in one process on one "
                "Windows workstation. Not venue evidence, not exchange-demo throughput, not a Linux certification, "
                "not a hosted-staging run.")


class Killed(BaseException):
    """The process dies here. Not an ``Exception``: nothing in the code under test catches it."""


# ── small helpers ─────────────────────────────────────────────────────────────────────────────────────────
def _pct(values: Sequence[float]) -> Dict[str, Optional[float]]:
    d = sorted(values)
    if not d:
        return {"p50": None, "p95": None, "max": None, "n": 0}
    pick = lambda q: d[min(len(d) - 1, int(round(q * (len(d) - 1))))]  # noqa: E731
    return {"p50": round(pick(0.5), 3), "p95": round(pick(0.95), 3), "max": round(d[-1], 3), "n": len(d)}


class _Stage:
    """Wall time, process CPU time, statements and connections opened for one stage."""

    def __init__(self, db) -> None:
        self.db = db

    def __enter__(self) -> "_Stage":
        self.s0, self.c0 = self.db.counters.statements, self.db.pool.stats["created"]
        self.cpu0, self.t0 = time.process_time(), time.perf_counter()
        return self

    def __exit__(self, *exc) -> None:
        self.result = {"wall_ms": round((time.perf_counter() - self.t0) * 1000, 1),
                       "cpu_ms": round((time.process_time() - self.cpu0) * 1000, 1),
                       "statements": self.db.counters.statements - self.s0,
                       "connections_opened": self.db.pool.stats["created"] - self.c0}


def _threads(fns: Sequence[Callable[[], Any]]) -> List[Any]:
    out: List[Any] = [None] * len(fns)
    errors: List[BaseException] = []

    def run(i, fn):
        try:
            out[i] = fn()
        except BaseException as exc:  # noqa: BLE001 -- recorded and re-raised in the caller's thread
            errors.append(exc)

    ts = [threading.Thread(target=run, args=(i, f)) for i, f in enumerate(fns)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    if errors:
        raise errors[0]
    return out


def commit_info() -> Dict[str, Any]:
    def git(*args):
        try:
            return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, timeout=20).stdout.strip()
        except Exception:  # noqa: BLE001
            return None
    status = git("status", "--porcelain") or ""
    return {"commit": git("rev-parse", "HEAD"), "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
            "working_tree_dirty": bool(status.strip()),
            "dirty_paths": sorted({line[3:] for line in status.splitlines() if line.strip()})[:60]}


def machine_info() -> Dict[str, Any]:
    info: Dict[str, Any] = {"os": platform.platform(), "python": sys.version.split()[0],
                            "machine": platform.machine(), "processor": platform.processor(),
                            "cpu_count": os.cpu_count()}
    try:
        import psutil
        info["memory_gb"] = round(psutil.virtual_memory().total / 2**30, 1)
    except Exception:  # noqa: BLE001
        pass
    return info


# ── test doubles that are part of the measurement ─────────────────────────────────────────────────────────
class StreamTransport:
    """A user-stream connection with no network: it answers the Bybit auth / subscribe frames with success and
    can be broken by the drill. It refuses a second simultaneous connection."""

    def __init__(self) -> None:
        self.inbox, self.is_open, self.broken, self.connects = deque(), False, False, 0

    def connect(self, spec) -> None:
        assert not self.is_open, "two simultaneous connections"
        self.is_open, self.broken, self.connects = True, False, self.connects + 1
        self.inbox.clear()

    def send(self, frame) -> None:
        from app.exchange.platform_adapter import user_stream as US
        if not self.is_open:
            raise US.StreamClosed()
        if isinstance(frame, dict) and frame.get("op") in ("auth", "subscribe"):
            self.inbox.append(json.dumps({"op": frame["op"], "success": True}))

    def receive(self):
        from app.exchange.platform_adapter import user_stream as US
        if self.broken or not self.is_open:
            self.is_open = False
            raise US.StreamClosed()
        return self.inbox.popleft() if self.inbox else None

    def ping(self) -> None:
        from app.exchange.platform_adapter import user_stream as US
        if not self.is_open:
            raise US.StreamClosed()
        self.inbox.append(US.HEARTBEAT)

    def close(self) -> None:
        self.is_open = False


class StreamVenue:
    """``FakeVenue`` with the user-stream spec each real venue uses (Binance and BingX: listen key in the URL;
    Bybit: an auth frame, then a subscribe frame). FakeVenue's own spec is an auth frame for every profile, which
    the Binance and BingX stream profiles can never authenticate (their parsers have no auth answer): that is a
    limitation of the test double, recorded in the evidence, not of the stream manager."""

    def __init__(self, fake) -> None:
        self._fake = fake

    def __getattr__(self, name):
        return getattr(self._fake, name)

    def user_stream(self):
        from app.exchange.platform_adapter import contract as c
        from app.exchange.platform_adapter import user_stream as US
        f = self._fake
        if f.venue == c.BYBIT:
            return c.UserStreamSpec(f.venue, "fake://bybit/private", US.AUTH_FRAME, 20, None, (
                {"op": "auth", "args": ["drill-key", 1, "drill-signature"]},
                {"op": "subscribe", "args": ["order", "execution", "position", "wallet"]}))
        return c.UserStreamSpec(f.venue, f"fake://{f.venue}/ws/drill-listen-key", US.LISTEN_KEY_IN_URL, 1800, 3600)


class DepthClient:
    """An order-book read and nothing else (what the spread gate asks the account's client)."""

    def __init__(self, mid: float = PRICE, *, levels: int = 20, qty: str = "5", age_ms: int = 0) -> None:
        self.mid, self.levels, self.qty, self.age_ms = mid, levels, qty, age_ms

    def get_orderbook(self, symbol, limit=50):
        return {"bids": [[f"{self.mid - 0.1 * (i + 1):.1f}", self.qty] for i in range(self.levels)],
                "asks": [[f"{self.mid + 0.1 * i:.1f}", self.qty] for i in range(self.levels)],
                "time": int(time.time() * 1000) - self.age_ms}


def entry_plan(account: str, tenant: str, venue: str, *, decision_time: int, family: str = "SCALE_DRILL_FAMILY"):
    """The fields of a TradePlan the spread gate reads, for one MARKET long entry (what every entry adapter of
    this repository sends)."""
    return SimpleNamespace(
        trade_plan_id=f"tp-{account}-{decision_time}", broker_account_id=account, user_id=tenant, side="LONG",
        venue=venue, environment="DEMO", instrument_key=SimpleNamespace(venue_symbol=SYMBOL), setup_family=family,
        setup_version="1.0.0", decision_time=decision_time, versions=(), mode="DEMO", source_candidate_id="d",
        plan_expiry_time=decision_time + 1,
        execution_preferences=SimpleNamespace(preferred_order_style="MARKET", fallback_order_styles=()))


class _FlakyFactory:
    def __init__(self, real) -> None:
        self.real, self.down = real, False

    def __call__(self):
        if self.down:
            raise ConnectionError("database unreachable (drill)")
        return self.real()


# ── the account fleet ─────────────────────────────────────────────────────────────────────────────────────
class Fleet:
    """N accounts, each with its own FakeVenue holding one protected long; durable state in the platform DB."""

    def __init__(self, url: str, legacy_path: str, n: int, *, ttl_ms: int, workers: int = WORKERS) -> None:
        from app.exchange.platform_adapter import contract as c
        from app.exchange.platform_adapter.fake import FakeVenue
        from app.platform_workers.assignments import WorkerAssignments
        from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
        from shared_lib.persistence.db import DB
        from shared_lib.persistence.sql import Database, migrate
        from shared_lib.persistence.sql.platform_schema import all_migrations

        self.c, self.url, self.n, self.ttl_ms, self.worker_count = c, url, n, ttl_ms, workers
        self.db = Database(url, pool_size=24)
        migrate(self.db, all_migrations())
        self.legacy = DB(legacy_path)
        ensure_cati_platform_schema(self.legacy)
        self.run_id = uuid.uuid4().hex[:8]
        self.ids = [f"S{n:04d}-{self.run_id}-{i:05d}" for i in range(n)]
        self.venue_of = {a: c.VENUES[i % len(c.VENUES)] for i, a in enumerate(self.ids)}
        self.tenant_of = {a: f"tenant-{i % 97:03d}" for i, a in enumerate(self.ids)}
        self.asg = WorkerAssignments(self.db, ttl_ms=ttl_ms, worker_liveness_ms=ttl_ms)
        self.asg.upsert_accounts([(a, self.tenant_of[a], self.venue_of[a], "DEMO") for a in self.ids])
        self.venues: Dict[str, Any] = {}
        for a in self.ids:
            v = FakeVenue(self.venue_of[a])
            v.add_instrument(SYMBOL, price=PRICE)
            v.place_order(c.OrderRequest(symbol=SYMBOL, side=c.BUY, order_type=c.MARKET,
                                         client_order_id=f"e-{a[-12:]}", quantity=0.01))
            v.place_order(self._stop(v, a, 0.01))
            v.mutations.clear()
            v.reads.clear()
            self.venues[a] = v
        self.maintained: List[str] = []
        self.recovered: List[str] = []
        self.latency_ms: List[float] = []
        self.recover_ms: List[float] = []
        self.lock = threading.Lock()
        self.dead: set = set()                   # worker ids whose process has "died"
        self.kill_after: Dict[str, int] = {}     # worker id -> die after this many accounts in the next round
        self.seen_after_kill: List[str] = []

    def _stop(self, v, account: str, qty: float):
        c = self.c
        if v.capabilities().close_position_stop:
            return c.OrderRequest(symbol=SYMBOL, side=c.SELL, order_type=c.STOP_MARKET,
                                  client_order_id=f"s-{account[-12:]}", stop_price=45_000.0, close_position=True)
        return c.OrderRequest(symbol=SYMBOL, side=c.SELL, order_type=c.STOP_MARKET, client_order_id=f"s-{account[-12:]}",
                              stop_price=45_000.0, quantity=qty, reduce_only=True)

    def stops(self, account: str) -> list:
        return [o for o in self.venues[account].open_orders(SYMBOL) if o.order_type == self.c.STOP_MARKET]

    def position(self, account: str) -> float:
        held = self.venues[account].positions(SYMBOL)
        return held[0].signed_quantity if held else 0.0

    # the callbacks a TradingWorker runs
    def recover(self, ctx) -> None:
        t = time.perf_counter()
        v = self.venues[ctx.account_id]
        v.positions()
        v.open_orders()
        with self.lock:
            self.recovered.append(ctx.account_id)
            self.recover_ms.append((time.perf_counter() - t) * 1000)

    def maintain(self, ctx) -> None:
        worker = ctx.assignment.worker_id
        if worker in self.dead:
            raise Killed(worker)
        with self.lock:
            left = self.kill_after.get(worker)
            if left is not None:
                if left <= 0:
                    self.dead.add(worker)
                    raise Killed(worker)
                self.kill_after[worker] = left - 1
        t = time.perf_counter()
        ctx.check()                                  # the fence, before anything that could mutate
        v = self.venues[ctx.account_id]
        pos = [p for p in v.positions(SYMBOL) if p.side != self.c.FLAT and p.quantity > 0]
        if pos and not self.stops(ctx.account_id):   # a position without a stop: protect it (never expected here)
            ctx.check()
            v.place_order(self._stop(v, ctx.account_id, pos[0].quantity))
        with self.lock:
            self.maintained.append(ctx.account_id)
            self.latency_ms.append((time.perf_counter() - t) * 1000)

    def workers(self, prefix: str, k: int, *, build: str = "scale-v1", assignments=None) -> list:
        from app.platform_workers.worker import TradingWorker
        return [TradingWorker(self.db, maintain=self.maintain, recover=self.recover, worker_id=f"{prefix}{j}-{self.run_id}",
                              assignments=assignments or self.asg, max_concurrency=4, build_version=build)
                for j in range(k)]

    def cycle(self, workers) -> list:
        """One round of every worker, concurrently. A killed worker's round ends with ``Killed``."""
        def one(w):
            if w.worker_id in self.dead:
                return None
            try:
                return w.run_once()
            except Killed:
                return None
        return _threads([lambda w=w: one(w) for w in workers])

    def owners(self) -> Counter:
        with self.db.transaction(write=False) as conn:
            now = conn.now_ms()
            rows = conn.all("SELECT account_id, worker_id FROM platform_account_assignments WHERE released_at IS NULL "
                            "AND expires_at > ?", (now,))
        mine = set(self.ids)
        return Counter(w for a, w in rows if a in mine)

    def owner_map(self) -> Dict[str, str]:
        with self.db.transaction(write=False) as conn:
            now = conn.now_ms()
            rows = conn.all("SELECT account_id, worker_id FROM platform_account_assignments WHERE released_at IS NULL "
                            "AND expires_at > ?", (now,))
        mine = set(self.ids)
        return {a: w for a, w in rows if a in mine}

    def protection_ok(self) -> Dict[str, int]:
        """Every account: exactly one resting stop while a position exists, none when flat, no open gap."""
        bad = dup = gaps = 0
        for a in self.ids:
            held, stops = self.position(a), self.stops(a)
            if held and len(stops) != 1:
                bad += 1
            if len(stops) > 1:
                dup += 1
            if any(g.open for g in self.venues[a].protection_gaps(SYMBOL)) and held:
                gaps += 1
        return {"accounts_wrongly_protected": bad, "accounts_with_duplicate_stops": dup, "open_protection_gaps": gaps}

    def retire(self, workers) -> None:
        for w in workers:
            if w.worker_id not in self.dead:
                w.drain()
            else:
                self.asg.set_worker_state(w.worker_id, "STOPPED")
        self.asg.upsert_accounts([(a, self.tenant_of[a], self.venue_of[a], "DEMO") for a in self.ids], state="REMOVED")

    def close(self) -> None:
        self.db.close()


# ── stages ────────────────────────────────────────────────────────────────────────────────────────────────
def stage_signal(n: int) -> Dict[str, Any]:
    from app.trading_intelligence.pipeline import daily_trend as DT
    from app.trading_intelligence.pipeline import performance as P
    from app.trading_intelligence.scale.fanout import SharedSignal, size_accounts
    from app.trading_intelligence.scale.measure_fanout import DECISION_DAY, _books

    panel, metadata = P.synthetic_market()
    builds = []

    def build():
        builds.append(1)
        return DT.market_stage(panel, metadata, DT.day_of(DECISION_DAY))

    c0 = time.process_time()
    signal = SharedSignal.compute(DT.FAMILY_ID, DECISION_DAY, build)
    signal_cpu = (time.process_time() - c0) * 1000
    books = _books(n, signal.market)
    c1 = time.process_time()
    sized = size_accounts(signal, books, max_concurrency=1)
    sizing_cpu = (time.process_time() - c1) * 1000
    signal.verify_unchanged()
    return {"shared_signal_computations": len(builds), "shared_signal_ms": round(signal.computed_ms, 2),
            "shared_signal_cpu_ms": round(signal_cpu, 1), "fingerprint_unchanged_after_fanout": True,
            "accounts_sized": len(sized.sizings), "sizing_errors": len(sized.errors),
            "sizing_total_ms": round(sized.total_ms, 2), "sizing_cpu_ms": round(sizing_cpu, 1),
            "sizing_per_account_ms": sized.latency(), "orders_proposed": sum(len(s.result.orders) for s in
                                                                             sized.sizings.values() if s.result)}


def stage_maintenance(f: Fleet, workers) -> Dict[str, Any]:
    with _Stage(f.db) as first:
        reports = f.cycle(workers)                     # claims + recovery-first + maintenance
    recovered_first = len({a for r in reports if r for a in r.recovered})
    first_lapsed = sum(len(r.lost) for r in reports if r)
    reconcile_ms = list(f.recover_ms)
    f.latency_ms.clear()
    maintained_before, recoveries_before = len(f.maintained), len(f.recover_ms)
    with _Stage(f.db) as st:
        reports = f.cycle(workers)                     # the steady-state cycle
    owned = len(f.owner_map())
    steady = f.maintained[maintained_before:]
    return {**st.result, "workers": len(workers), "owned": owned,
            "owned_as_reported_by_each_claim": sum(r.owned for r in reports if r),
            "accounts_maintained": len(set(steady)), "maintenance_steps": len(steady),
            # a lease that lapses mid-round is refused by the fence (fail-safe) and the account moves to another
            # worker, which recovers it first: both are counted, never hidden
            "fence_refused_lease_lapsed": sum(len(r.lost) for r in reports if r),
            "accounts_moved_and_recovered_in_round": len(f.recover_ms) - recoveries_before,
            "failed": sum(len(r.failed) for r in reports if r),
            "account_cycle_latency_ms": _pct(f.latency_ms),
            "max_worker_round_ms": round(max(r.duration_ms for r in reports if r), 1),
            "first_round": {**first.result, "accounts_recovered_first": recovered_first,
                            "recoveries_including_rebalance_moves": len(reconcile_ms),
                            "fence_refused_lease_lapsed": first_lapsed,
                            "startup_recovery_latency_ms": _pct(reconcile_ms)}}


def stage_reconciliation(f: Fleet) -> Dict[str, Any]:
    """REST reconciliation of every account through the shared request budget of its venue's egress address."""
    from app.exchange.platform_adapter import rate_budget as RB
    from app.exchange.platform_adapter.reconcile import MANUAL, RestReconciler

    budget = RB.RateBudget(f.legacy, worker=f"scale-{f.run_id}")
    clock = lambda: int(time.time() * 1000)  # noqa: E731 -- the budget window is real time
    started_at = clock()
    lat: List[float] = []
    lat_ok: List[float] = []
    lat_refused: List[float] = []
    by_venue: Dict[str, Counter] = {v: Counter() for v in f.c.VENUES}
    c0 = time.process_time()
    t0 = time.perf_counter()
    scope = f.legacy.scope()
    scope.__enter__()
    for a in f.ids:
        adapter = RB.BudgetedVenueAdapter(f.venues[a], budget, server_address=f"{EGRESS}-{f.run_id}", account_ref=a,
                                          clock=clock, priority=RB.RECONCILIATION)
        t = time.perf_counter()
        result = RestReconciler(f.legacy, adapter, account_ref=a, symbols=[SYMBOL]).run(clock(), trigger=MANUAL)
        lat.append((time.perf_counter() - t) * 1000)
        (lat_ok if not result.error_code else lat_refused).append(lat[-1])
        by_venue[f.venue_of[a]][result.status if not result.error_code else f"{result.status}:{result.error_code}"] += 1
    scope.__exit__(None, None, None)
    wall = (time.perf_counter() - t0) * 1000
    now = clock()
    venues = {}
    for v in f.c.VENUES:
        accounts = sum(1 for a in f.ids if f.venue_of[a] == v)
        if not accounts:
            continue
        snap = budget.snapshot(v, f"{EGRESS}-{f.run_id}", now)
        ok = by_venue[v].get("OK", 0)
        windows = -(-accounts // ok) if ok else None
        venues[v] = {"accounts": accounts, "outcomes": dict(by_venue[v]), "used_weight": snap["used"],
                     "accounts_reconciled_in_one_window": ok,
                     "derived_windows_for_one_full_round": windows,
                     "derived_full_round_ms_one_egress_address": None if windows is None else windows * snap["window_ms"],
                     "unit": snap["unit"], "capacity": snap["capacity"], "window_ms": snap["window_ms"],
                     "venue_limit": snap["venue_limit"], "limit_verification": snap["limit_verification"],
                     "reconciliation_ceiling": snap["ceilings"][RB.RECONCILIATION],
                     "denials": snap["denials"], "used_by_priority": snap["used_by_priority"]}
    return {"accounts": f.n, "wall_ms": round(wall, 1), "cpu_ms": round((time.process_time() - c0) * 1000, 1),
            "elapsed_window_ms": now - started_at, "reconciliation_latency_ms": _pct(lat),
            "reconciliation_latency_completed_ms": _pct(lat_ok), "refused_by_local_budget_latency_ms": _pct(lat_refused),
            "egress_addresses_per_venue": 1, "by_venue": venues}


def stage_queue(f: Fleet, *, workers: int = WORKERS) -> Dict[str, Any]:
    from app.golive.synthetic import VenueExecutor, placed_orders
    from app.platform_workers.jobs import ORDER, JobProcessor, JobQueue, OrderIntents, order_job_handler

    intents = OrderIntents(f.db, settle_ms=0)
    q = JobQueue(f.db)
    keys = {}
    lost = 0
    for i, a in enumerate(f.ids):
        key = f"scale:{f.run_id}:{a}"
        keys[a] = key
        intents.prepare(idempotency_key=key, account_id=a, tenant_id=f.tenant_of[a], venue=f.venue_of[a],
                        symbol=SYMBOL, side="BUY", quantity="0.001", limit_price=RESTING_LIMIT)
        q.enqueue(ORDER, key, {"intent_key": key}, account_id=a, tenant_id=f.tenant_of[a])
        if i % LOST_ACK_EVERY == 0:          # the venue fills it, the answer is lost on the way back
            f.venues[a].fail_next("place_order", f.c.NETWORK_UNCERTAIN, after_effect=True, symbol=SYMBOL)
            lost += 1
    executor = VenueExecutor(f.venues)
    procs = [JobProcessor(q, f"p{k}-{f.run_id}", {ORDER: order_job_handler(intents, executor)}) for k in range(workers)]

    def drain(p):
        done = []
        idle = 0
        while idle < 3:                      # a job retried "later" (delay 0) is due again at once
            r = p.run_once(limit=25)
            if r:
                done.extend(r)
                idle = 0
            else:
                idle += 1
        return done

    with _Stage(f.db) as st:
        results = [x for part in _threads([lambda p=p: drain(p) for p in procs]) for x in part]
    outcomes = Counter(o for _, o in results)
    statuses = Counter(intents.load(keys[a]).status for a in f.ids)
    dup = sum(1 for a in f.ids if placed_orders(f.venues[a], intents.load(keys[a]).client_order_id) > 1)
    recovered = 0
    with f.db.transaction(write=False) as conn:
        recovered = int(conn.scalar("SELECT COUNT(*) FROM platform_order_intents WHERE detail='RECOVERED_BY_READBACK' "
                                    "AND idempotency_key LIKE ?", (f"scale:{f.run_id}:%",)) or 0)
    return {**st.result, "jobs": f.n, "outcomes": dict(outcomes), "intent_statuses": dict(statuses),
            "throughput_jobs_per_s": round(f.n / max(st.result["wall_ms"] / 1000, 1e-9), 1),
            "venue_submits": executor.submits, "lost_acknowledgements_injected": lost,
            "lost_acknowledgements_resolved_by_lookup": recovered, "duplicate_orders": dup,
            "backlog_after": q.backlog()["by_category"].get(ORDER, {})}


def stage_notifications(f: Fleet, *, workers: int = WORKERS) -> Dict[str, Any]:
    from app.platform_workers.jobs import NOTIFICATION, JobProcessor, JobQueue

    q = JobQueue(f.db)
    delivered: Counter = Counter()
    lock = threading.Lock()

    def sink(job):
        with lock:
            delivered[job.idempotency_key] += 1
        return {"delivered": True}

    dupes = 0
    for i, a in enumerate(f.ids):
        key = f"notification:scale:{f.run_id}:{a}"
        q.enqueue(NOTIFICATION, key, {"account": a, "kind": "FILL"}, account_id=a, tenant_id=f.tenant_of[a])
        if i % 5 == 0:
            _, created = q.enqueue(NOTIFICATION, key, {"account": a, "kind": "FILL"}, account_id=a,
                                   tenant_id=f.tenant_of[a])
            dupes += 1
            assert not created
    procs = [JobProcessor(q, f"n{k}-{f.run_id}", {NOTIFICATION: sink}) for k in range(workers)]

    def drain(p):
        while p.run_once(limit=50):
            pass

    with _Stage(f.db) as st:
        _threads([lambda p=p: drain(p) for p in procs])
    return {**st.result, "enqueued": f.n, "duplicate_enqueues_coalesced": dupes, "delivered": sum(delivered.values()),
            "duplicate_deliveries": sum(v - 1 for v in delivered.values() if v > 1),
            "throughput_per_s": round(f.n / max(st.result["wall_ms"] / 1000, 1e-9), 1)}


def stage_spreading(f: Fleet) -> Dict[str, Any]:
    from app.trading_intelligence.scale.order_spreading import BUY, DepthSnapshot, PlatformOrderBudget

    with f.db.transaction(write=False) as conn:
        now = conn.now_ms()
    # its own symbol: these RESERVED slots of every account hold capacity for the budget TTL, and on the entry
    # gate's symbol they would (correctly) make every later uncalibrated entry there CALIBRATION_REQUIRED
    symbol = "SPREADUSDT"
    depth = DepthSnapshot.of("binance", symbol, [(PRICE - 1 - i, 5) for i in range(20)],
                             [(PRICE + i, 5) for i in range(20)], now)
    pb = PlatformOrderBudget(f.db)
    with _Stage(f.db) as st:
        bid = pb.open_budget(venue="binance", symbol=symbol, signal_id=f"scale-{f.run_id}", side=BUY, depth=depth)
        for a in f.ids:
            pb.request(bid, a, f.tenant_of[a], "250")
        alloc = pb.allocate(bid, tick=Decimal("0.1"))
    slots = Counter(s.scheduled_at for s in alloc.slots if s.status == "RESERVED")
    return {**st.result, "accounts_requesting": f.n, "cap_notional": str(alloc.cap),
            "granted_total": str(alloc.granted_total), "within_cap": alloc.granted_total <= alloc.cap,
            "reserved": sum(1 for s in alloc.slots if s.status == "RESERVED"),
            "deferred": sum(1 for s in alloc.slots if s.status == "DEFERRED"),
            "max_orders_in_one_slot_interval": max(slots.values()) if slots else 0,
            "policy_max_orders_per_slot": pb.policy.max_orders_per_slot,
            "slot_intervals_used": len(slots), "depth_snapshots_read": 1,
            "calibrated": pb.policy.calibrated, "policy_version": pb.policy.policy_version}


def stage_entry_gate(f: Fleet) -> Dict[str, Any]:
    from app.trading_intelligence.scale import spread_gate as G
    from app.trading_intelligence.scale.order_spreading import SpreadPolicy

    def attempt(gate, decision_time, client_factory):
        codes: Counter = Counter()
        per_venue: Dict[str, Counter] = {}
        for a in f.ids:
            p = entry_plan(a, f.tenant_of[a], f.venue_of[a], decision_time=decision_time)
            try:
                gate.reserve(p, client=client_factory(), adapter=None, quantity=Decimal("0.001"),
                             notional=Decimal("50"), attempt_id=f"att-{a}-{decision_time}",
                             client_order_id=f"cid-{a[-12:]}-{decision_time}", tick=Decimal("0.1"))
                code = "RESERVED"
            except G.SpreadRefused as exc:
                code = exc.code
            codes[code] += 1
            per_venue.setdefault(f.venue_of[a], Counter())[code] += 1
        return {"codes": dict(codes), "by_venue": {v: dict(c) for v, c in per_venue.items()}}

    def active_slots():
        with f.db.transaction(write=False) as conn:
            return int(conn.scalar("SELECT COUNT(*) FROM platform_order_slots WHERE status IN ('RESERVED','SUBMITTED',"
                                   "'FILLED')") or 0)

    before = active_slots()
    base = int(time.time() * 1000)
    out = {}
    with _Stage(f.db) as st:
        out["production_default_policy"] = attempt(G.PlatformSpreadGate(f.db), base, DepthClient)
    out["production_default_policy"].update(st.result)
    market_ok = SpreadPolicy(policy_version="scale-drill-market-entry-uncalibrated", allow_market_fallback=True)
    with _Stage(f.db) as st:
        out["uncalibrated_market_permitting_test_policy"] = attempt(G.PlatformSpreadGate(f.db, policy=market_ok),
                                                                    base + 1, DepthClient)
    out["uncalibrated_market_permitting_test_policy"].update(st.result)
    with _Stage(f.db) as st:
        out["stale_market_data"] = attempt(G.PlatformSpreadGate(f.db, policy=market_ok), base + 2,
                                           lambda: DepthClient(age_ms=60_000))
    out["stale_market_data"].update(st.result)
    out["active_slots_created"] = active_slots() - before
    return out


# ── failure cases ─────────────────────────────────────────────────────────────────────────────────────────
def _until(predicate: Callable[[], bool], step: Callable[[], Any], timeout_s: float) -> Optional[float]:
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout_s:
        step()
        if predicate():
            return (time.perf_counter() - t0) * 1000
        time.sleep(0.02)
    return None


def failure_worker_killed(f: Fleet, workers) -> Dict[str, Any]:
    # the stages before this one take longer than a TTL: re-establish full ownership first (expired claims are
    # taken over here, which the assignment history records as EXPIRED_TAKEOVER)
    _until(lambda: sum(f.owners().values()) == f.n, lambda: f.cycle(workers), 15 + 3 * f.ttl_ms / 1000)
    owners0 = Counter(f.owner_map().values())
    victim = max(workers, key=lambda w: (owners0.get(w.worker_id, 0), w.worker_id))
    held = [a for a, w in f.owner_map().items() if w == victim.worker_id]
    held_assignment = {}

    original = f.maintain

    def capture(ctx):
        if ctx.assignment.worker_id == victim.worker_id:
            held_assignment.setdefault("a", ctx.assignment)
        return original(ctx)

    f.maintain = capture
    for w in workers:
        w.maintain = capture
    f.kill_after[victim.worker_id] = len(held) // 2              # dies in the middle of its round
    f.cycle(workers)
    killed_at = time.perf_counter()
    survivors = [w for w in workers if w.worker_id != victim.worker_id]
    maintained_before = len(f.maintained)
    t_start = time.perf_counter()
    took = _until(lambda: f.victim_free(victim.worker_id, held), lambda: f.cycle(survivors), 15 + 3 * f.ttl_ms / 1000)
    stale_refused = None
    if "a" in held_assignment:
        from app.platform_workers.assignments import AssignmentLost
        try:
            f.asg.verify(held_assignment["a"])
            stale_refused = False
        except AssignmentLost as exc:
            stale_refused = exc.reason
    owners = f.owner_map()
    f.maintain = original
    for w in workers:
        w.maintain = original
    return {"worker_accounts": len(held), "died_after_accounts": len(held) // 2, "ttl_ms": f.ttl_ms,
            "lease_takeover_ms": None if took is None else round(took + (t_start - killed_at) * 1000, 1),
            "orphans_left": sum(1 for a in held if owners.get(a) in (None, victim.worker_id)),
            "accounts_owned_after": len(owners), "all_accounts_owned_after": len(owners) == f.n,
            "stale_holder_mutation_refused": stale_refused,
            "maintained_by_survivors_meanwhile": len(f.maintained) - maintained_before,
            **f.protection_ok()}


def _victim_free(self, victim: str, held: Sequence[str]) -> bool:
    owners = self.owner_map()
    return all(owners.get(a) not in (None, victim) for a in held)


Fleet.victim_free = _victim_free


def failure_database_restart(f: Fleet, workers, *, outage_s: float = 0.3) -> Dict[str, Any]:
    from shared_lib.persistence.sql import DatabaseUnavailable

    pool = f.db.pool
    flaky = _FlakyFactory(pool._factory)
    pool._factory = flaky
    before = len(f.maintained)
    pool.close()                                  # every idle connection is gone (a server restart)
    flaky.down = True
    failed_rounds = 0
    t_end = time.perf_counter() + outage_s
    while time.perf_counter() < t_end:
        for w in workers:
            try:
                w.run_once()
            except DatabaseUnavailable:
                failed_rounds += 1
            except Exception:  # noqa: BLE001 -- any other failure while the database is gone counts too
                failed_rounds += 1
        time.sleep(0.05)
    during = len(f.maintained) - before
    flaky.down = False
    back = time.perf_counter()
    created_before = pool.stats["created"]
    took = _until(lambda: sum(f.owners().values()) == f.n, lambda: f.cycle(workers), 15 + 3 * f.ttl_ms / 1000)
    pool._factory = flaky.real
    return {"outage_s": outage_s, "rounds_refused_during_outage": failed_rounds,
            "accounts_maintained_during_outage": during,
            "recovered_full_ownership_ms": None if took is None else round(took, 1),
            "connections_reopened": pool.stats["created"] - created_before, "elapsed_since_back_ms":
                round((time.perf_counter() - back) * 1000, 1), **f.protection_ok()}


def failure_user_stream(f: Fleet) -> Dict[str, Any]:
    from app.exchange.platform_adapter import user_stream as US

    clock = [int(time.time() * 1000)]
    scope = f.legacy.scope()
    scope.__enter__()
    mgrs = {}
    for a in f.ids:
        t = StreamTransport()
        mgrs[a] = (US.UserStreamManager(f.legacy, StreamVenue(f.venues[a]), t, account_ref=a,
                                        owner=f"stream-{f.run_id}", symbols=[SYMBOL]), t)
    t0 = time.perf_counter()
    live = Counter(m.step(clock[0]).health for m, _ in mgrs.values())
    connect_ms = (time.perf_counter() - t0) * 1000
    allowed = sum(US.stream_allows_entries(f.legacy, f.venues[a].venue, f.venues[a].environment, a, clock[0])[0]
                  for a in f.ids)
    for _, t in mgrs.values():
        t.broken = True
    clock[0] += 1_000
    down = Counter(m.step(clock[0]).health for m, _ in mgrs.values())
    refused = Counter(US.stream_allows_entries(f.legacy, f.venues[a].venue, f.venues[a].environment, a, clock[0])[1]
                      .split(":")[0] for a in f.ids)
    # maintenance and protection never consult the stream: positions stay protected while it is down
    protected = f.protection_ok()
    t1 = time.perf_counter()
    rounds = 0
    while rounds < 20:
        clock[0] += 5_000
        rounds += 1
        states = Counter(m.step(clock[0]).health for m, _ in mgrs.values())
        if states.get(US.LIVE, 0) == f.n:
            break
    reconnect_ms = (time.perf_counter() - t1) * 1000
    allowed_after = sum(US.stream_allows_entries(f.legacy, f.venues[a].venue, f.venues[a].environment, a, clock[0])[0]
                        for a in f.ids)
    connections = Counter(t.connects for _, t in mgrs.values())
    for m, _ in mgrs.values():
        m.close(clock[0])
    scope.__exit__(None, None, None)
    return {"streams": f.n, "connect_step_ms": round(connect_ms, 1), "live_after_connect": live.get(US.LIVE, 0),
            "entries_allowed_while_live": allowed, "health_after_disconnect": dict(down),
            "entry_refusals_while_down": dict(refused), "protection_while_down": protected,
            "reconnect_rounds": rounds, "reconnect_ms": round(reconnect_ms, 1), "live_after_reconnect": states.get(US.LIVE, 0),
            "entries_allowed_after_reconnect": allowed_after, "connections_per_stream": {str(k): v for k, v in connections.items()},
            "note": "simulated stream clock (5 s per reconnect round); a FakeVenue test double with venue-faithful "
                    "stream specs (see StreamVenue)"}


def failure_emergency_flatten(f: Fleet, workers) -> Dict[str, Any]:
    from app.platform_workers.assignments import AssignmentLost

    c = f.c
    targets = f.ids[:: max(1, 10)][: max(1, f.n // 10)]
    old = {}
    with f.db.transaction(write=False) as conn:
        for a in targets:
            row = conn.one("SELECT worker_id, fencing_token FROM platform_account_assignments WHERE account_id=?", (a,))
            old[a] = row
    flattened = []

    def flatten():
        for a in targets:
            e = f.asg.preempt(a, f"emergency-{f.run_id}", "EMERGENCY_FLATTEN_DRILL")
            v = f.venues[a]
            held = f.position(a)
            if held:
                v.place_order(c.OrderRequest(symbol=SYMBOL, side=c.SELL, order_type=c.MARKET,
                                             client_order_id=f"x-{a[-12:]}", quantity=abs(held), reduce_only=True))
            for s in f.stops(a):
                v.cancel_order(SYMBOL, client_order_id=s.client_order_id)
            f.asg.release_assignment(e, reason="EMERGENCY_FLATTEN_DONE")
            flattened.append(a)

    rec_before = Counter(f.recovered)
    t0 = time.perf_counter()
    reports: List[Any] = []
    _threads([lambda: reports.extend(f.cycle(workers)), flatten])     # the flatten runs during a maintenance cycle
    flatten_ms = (time.perf_counter() - t0) * 1000
    lost_targets = sum(1 for r in reports if r for a in r.lost if a in set(targets))
    lost_other = sum(1 for r in reports if r for a in r.lost if a not in set(targets))
    refused = 0
    for a in targets:
        worker, token = old[a]
        try:
            with f.db.transaction() as conn:
                f.asg.verify_on(conn, a, worker, int(token))
        except AssignmentLost:
            refused += 1
    f.cycle(workers)
    rec_after = Counter(f.recovered)
    recovered_next = {a for a in targets if rec_after[a] > rec_before[a]}
    return {"accounts_flattened": len(flattened), "flatten_wall_ms": round(flatten_ms, 1),
            "fence_refusals_on_flattened_accounts": lost_targets,
            "fence_refusals_on_other_accounts_lease_lapsed_under_load": lost_other,
            "previous_holder_fence_refused_after": refused,
            "flat_after": sum(1 for a in targets if f.position(a) == 0.0),
            "orphan_stops_after": sum(len(f.stops(a)) for a in targets),
            "reopened_after": sum(1 for a in targets if f.position(a) != 0.0),
            "recovered_first_by_next_owner": len(recovered_next), **{"other_accounts_" + k: v for k, v in
                                                                     _protection_except(f, targets).items()}}


def _protection_except(f: Fleet, skip) -> Dict[str, int]:
    skip = set(skip)
    bad = 0
    for a in f.ids:
        if a in skip:
            continue
        if f.position(a) and len(f.stops(a)) != 1:
            bad += 1
    return {"wrongly_protected": bad}


def failure_queue_crash(f: Fleet, *, lease_ms: int, workers: int = WORKERS) -> Dict[str, Any]:
    from app.golive.synthetic import VenueExecutor, placed_orders
    from app.platform_workers.jobs import ORDER, JobProcessor, JobQueue, OrderIntents, order_job_handler

    intents = OrderIntents(f.db, settle_ms=0)
    q = JobQueue(f.db, lease_ms=lease_ms)
    keys = {a: f"crash:{f.run_id}:{a}" for a in f.ids}
    for a in f.ids:
        intents.prepare(idempotency_key=keys[a], account_id=a, tenant_id=f.tenant_of[a], venue=f.venue_of[a],
                        symbol=SYMBOL, side="BUY", quantity="0.001", limit_price=RESTING_LIMIT)
        q.enqueue(ORDER, keys[a], {"intent_key": keys[a]}, account_id=a, tenant_id=f.tenant_of[a])
    crashed = q.claim(f"crashed-consumer-{f.run_id}", categories=[ORDER], limit=max(1, f.n // 4))   # then it dies
    crashed_at = time.perf_counter()
    executor = VenueExecutor(f.venues)
    procs = [JobProcessor(q, f"q{k}-{f.run_id}", {ORDER: order_job_handler(intents, executor)}) for k in range(workers)]

    def all_done():
        with f.db.transaction(write=False) as conn:
            left = int(conn.scalar("SELECT COUNT(*) FROM platform_jobs WHERE idempotency_key LIKE ? AND status <> "
                                   "'SUCCEEDED'", (f"crash:{f.run_id}:%",)) or 0)
        return left == 0

    def step():
        _threads([lambda p=p: p.run_once(limit=50) for p in procs])

    took = _until(all_done, step, 15 + 4 * lease_ms / 1000)
    attempts = Counter()
    with f.db.transaction(write=False) as conn:
        for job in crashed:
            attempts[int(conn.scalar("SELECT attempts FROM platform_jobs WHERE job_id=?", (job.job_id,)))] += 1
    dup = sum(1 for a in f.ids if placed_orders(f.venues[a], intents.load(keys[a]).client_order_id) > 1)
    return {"jobs": f.n, "claimed_by_crashed_consumer": len(crashed), "job_lease_ms": lease_ms,
            "all_jobs_succeeded": took is not None,
            "drained_after_crash_ms": None if took is None else round((time.perf_counter() - crashed_at) * 1000, 1),
            "attempts_of_crashed_jobs": {str(k): v for k, v in attempts.items()}, "duplicate_orders": dup}


def failure_interrupted_deployment(f: Fleet, old_workers) -> Dict[str, Any]:
    new = f.workers("v2-", len(old_workers), build="scale-v2")
    before_recovered = len(f.recovered)            # every recovery from the moment the new version starts
    f.cycle(old_workers + new)
    with f.db.transaction(write=False) as conn:
        stage_at = conn.now_ms()
    t0 = time.perf_counter()
    for w in old_workers[:-1]:
        w.drain()                                  # SIGTERM of the old version: finish, release DRAIN
    dying = old_workers[-1]
    f.kill_after[dying.worker_id] = 1              # the last old process is killed mid-round instead
    f.cycle([dying] + new)
    drained_ms = (time.perf_counter() - t0) * 1000
    new_ids = {w.worker_id for w in new}
    took = _until(lambda: sum(n for w, n in f.owners().items() if w in new_ids) == f.n, lambda: f.cycle(new),
                  15 + 3 * f.ttl_ms / 1000)
    reasons = Counter()
    mine = set(f.ids)
    with f.db.transaction(write=False) as conn:
        for a, reason in conn.all("SELECT account_id, reason FROM platform_assignment_history WHERE at >= ?",
                                  (stage_at,)):
            if a in mine:
                reasons[reason] += 1
    return {"old_workers": len(old_workers), "new_workers": len(new), "drained_cleanly": len(old_workers) - 1,
            "killed_mid_round": 1, "drain_phase_ms": round(drained_ms, 1),
            "all_accounts_on_new_version_ms": None if took is None else round(drained_ms + took, 1),
            "accounts_recovered_first_by_new_version": len(set(f.recovered[before_recovered:])),
            "assignment_history_reasons_during_stage": dict(reasons), "ttl_ms": f.ttl_ms, **f.protection_ok()}, new


# ── one account count ─────────────────────────────────────────────────────────────────────────────────────
def run_count(n: int, *, url: Optional[str] = None, workdir: Optional[str] = None, ttl_ms: int = 5_000,
              job_lease_ms: int = 1_000, memory_pass: bool = True, workers: int = WORKERS) -> Dict[str, Any]:
    import psutil

    own_dir = workdir is None
    work = Path(workdir or tempfile.mkdtemp(prefix=f"cf_scale_{n}_"))
    work.mkdir(parents=True, exist_ok=True)
    url = url or f"sqlite:///{(work / f'platform_scale_{n}.db').as_posix()}"
    proc = psutil.Process()
    cpu0, t0 = time.process_time(), time.perf_counter()
    row: Dict[str, Any] = {"accounts": n, "dialect": "postgresql" if url.startswith("postgres") else "sqlite",
                           "workers": workers, "ttl_ms": ttl_ms}
    row["shared_signal_and_sizing"] = stage_signal(n)
    f = Fleet(url, str(work / f"legacy_scale_{n}.db"), n, ttl_ms=ttl_ms, workers=workers)
    try:
        ws = f.workers("w", workers)
        row["maintenance"] = stage_maintenance(f, ws)
        row["reconciliation_and_request_budget"] = stage_reconciliation(f)
        row["queue"] = stage_queue(f, workers=workers)
        row["notifications"] = stage_notifications(f, workers=workers)
        row["order_spreading"] = stage_spreading(f)
        row["entry_gate"] = stage_entry_gate(f)
        failures: Dict[str, Any] = {}
        failures["worker_killed_mid_cycle"] = failure_worker_killed(f, ws)
        live = [w for w in ws if w.worker_id not in f.dead]
        failures["database_restart"] = failure_database_restart(f, live)
        failures["user_stream_disconnect"] = failure_user_stream(f)
        failures["emergency_flatten_during_load"] = failure_emergency_flatten(f, live)
        failures["queue_consumer_crash"] = failure_queue_crash(f, lease_ms=job_lease_ms, workers=workers)
        failures["interrupted_deployment"], new = failure_interrupted_deployment(f, live)
        failures["order_acknowledgement_lost"] = {
            "injected": row["queue"]["lost_acknowledgements_injected"],
            "resolved_by_lookup": row["queue"]["lost_acknowledgements_resolved_by_lookup"],
            "duplicate_orders": row["queue"]["duplicate_orders"]}
        failures["stale_market_data"] = {"entries_refused": row["entry_gate"]["stale_market_data"]["codes"]}
        row["failures"] = failures
        row["lease_takeover_ms"] = failures["worker_killed_mid_cycle"]["lease_takeover_ms"]
        row["resources"] = {"rss_mb": round(proc.memory_info().rss / 2**20, 1),
                            "db_connections_opened_total": f.db.pool.stats["created"],
                            "db_pool_peak_in_use": f.db.pool.stats["peak_in_use"], "db_pool_size": f.db.pool.size,
                            "statements_total": f.db.counters.statements,
                            "transactions_total": f.db.counters.transactions}
        f.retire(new + [w for w in ws if w.worker_id in f.dead])
    finally:
        f.close()
    if memory_pass:
        row["resources"]["tracemalloc_peak_bytes"] = memory_peak(n, work, ttl_ms=ttl_ms, workers=workers)
    row["resources"]["cpu_ms_total"] = round((time.process_time() - cpu0) * 1000, 1)
    row["wall_s_total"] = round(time.perf_counter() - t0, 1)
    if own_dir:
        shutil.rmtree(work, ignore_errors=True)
    return row


def memory_peak(n: int, work: Path, *, ttl_ms: int, workers: int) -> Dict[str, int]:
    """A separate pass under tracemalloc: signal + sizing, and fleet setup + one maintenance cycle."""
    out = {}
    tracemalloc.start()
    stage_signal(n)
    out["signal_and_sizing"] = tracemalloc.get_traced_memory()[1]
    tracemalloc.reset_peak()
    f = Fleet(f"sqlite:///{(work / f'platform_mem_{n}.db').as_posix()}", str(work / f"legacy_mem_{n}.db"), n,
              ttl_ms=ttl_ms, workers=workers)
    try:
        ws = f.workers("m", workers)
        f.cycle(ws)
        f.cycle(ws)
        out["fleet_and_maintenance_cycle"] = tracemalloc.get_traced_memory()[1]
        f.retire(ws)
    finally:
        f.close()
        tracemalloc.stop()
    return out


NOTES = (
    "Worker processes are threads of ONE process (4 trading workers x 4 account threads); separate-process "
    "takeover is measured by scripts/worker_kill_drill.py, not here.",
    "Lease TTL is a drill setting (5 s; production 60 s): takeover times scale with the TTL, not with N. A first "
    "run with a 1.5 s TTL showed why it must exceed the round: at 1,000 accounts a round took ~2 s on this loaded "
    "workstation, leases lapsed mid-round, the fence refused those steps and accounts moved between workers "
    "(fail-safe, but churn). fence_refused_lease_lapsed and accounts_moved_and_recovered_in_round record it.",
    "The request budget and the REST reconciliation / user-stream ledgers live in a throwaway legacy SQLite file "
    "on every dialect; the platform database is the dialect named in the row.",
    "tracemalloc peaks come from a separate pass (SQLite only) so that tracing does not inflate the timed stages.",
    "Queue orders are resting limit orders that never fill, so every position keeps exactly the protection it was "
    "given; the order-identity and lost-acknowledgement properties are the same as for a market order.",
    "Request budget: one egress address per venue and the repository's default limits and method weights "
    "(Binance VERIFIED_BY_DOC; Bybit and BingX UNVERIFIED configured envelopes). 'derived_*' fields are arithmetic "
    "on the measured window, not measurements.",
    "FakeVenue's own user-stream spec is an auth frame for every profile, which the Binance and BingX stream "
    "profiles cannot authenticate; StreamVenue supplies each venue's real spec shape. A test-double limitation.",
    "Other engineers' processes were running on the same workstation during the measurement: wall-clock figures "
    "include that noise.",
)


def evidence(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    from app.trading_intelligence.pipeline import performance as P
    return {"schema": "cosmicforge-predeploy-account-scale-v1",
            "measured_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "machine": machine_info(), "host": P.host_description(), "code": commit_info(),
            "script": "scripts/perf/account_scale.py", "rows": list(rows), "not_evidence_of": NOT_EVIDENCE,
            "notes": list(NOTES),
            "evidence_class": "SYNTHETIC_FAKEVENUE_WINDOWS_WORKSTATION"}


def _on_throwaway_postgres(admin_url: str, n: int, *, ttl_ms: int, workers: int) -> Dict[str, Any]:
    import psycopg
    name = f"cf_scale_drill_{n}_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(admin_url, autocommit=True) as conn:
        conn.execute(f"CREATE DATABASE {name}")
    try:
        return run_count(n, url=admin_url.rpartition("/")[0] + "/" + name, ttl_ms=ttl_ms, memory_pass=False,
                         workers=workers)
    finally:
        with psycopg.connect(admin_url, autocommit=True) as conn:
            conn.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--counts", default=",".join(map(str, COUNTS)))
    ap.add_argument("--out-dir", default=str(ROOT / "evidence" / "predeploy"))
    ap.add_argument("--url", default=None, help="a THROWAWAY drill database URL (must contain 'drill')")
    ap.add_argument("--ttl-ms", type=int, default=5_000)
    ap.add_argument("--workers", type=int, default=WORKERS)
    ap.add_argument("--pg-admin-url", default=None, help="a DISPOSABLE PostgreSQL server: adds a PostgreSQL row")
    ap.add_argument("--no-memory-pass", action="store_true")
    a = ap.parse_args(argv)
    if a.url and "drill" not in a.url:
        raise SystemExit("refusing: --url must name a throwaway drill database")
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for n in (int(x) for x in a.counts.split(",")):
        rows = [run_count(n, url=a.url, ttl_ms=a.ttl_ms, memory_pass=not a.no_memory_pass, workers=a.workers)]
        if a.pg_admin_url:
            rows.append(_on_throwaway_postgres(a.pg_admin_url, n, ttl_ms=a.ttl_ms, workers=a.workers))
        path = out / f"scale_{n}_accounts.json"
        path.write_text(json.dumps(evidence(rows), indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
        row = rows[0]
        m, q = row["maintenance"], row["queue"]
        print(json.dumps({"n": n, "signal_computations": row["shared_signal_and_sizing"]["shared_signal_computations"],
                          "sizing_ms": row["shared_signal_and_sizing"]["sizing_total_ms"],
                          "cycle_p95_ms": m["account_cycle_latency_ms"]["p95"], "owned": m["owned"],
                          "queue_jobs_s": q["throughput_jobs_per_s"], "dups": q["duplicate_orders"],
                          "takeover_ms": row["lease_takeover_ms"], "wall_s": row["wall_s_total"], "file": str(path)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
