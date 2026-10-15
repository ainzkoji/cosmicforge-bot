"""Measurement harness of the CATI platform cycle (Section H, Step 3.12; load evidence of Section 29).

    python -m app.trading_intelligence.pipeline.performance --out <file.json> [--baseline <earlier.json>]

It runs the REAL driver (``driver.PlatformCycleDriver.run_cycle``) on synthetic fixtures and records what a
cycle costs, so that an optimisation is justified by a number and checked against the same fixtures:

* end-to-end cycle duration and the duration of every stage;
* market-data request volume (through the daily bar feed, ``app.market_data.daily_feed``);
* database statements and connections per cycle and per account, counted by a ``sqlite3`` trace callback on
  every connection the driver opens (not estimated);
* peak memory (``tracemalloc``), in a separate pass because tracing slows the code it measures;
* account-processing throughput at several account counts, and the queue depth of the account stage;
* the time an interrupted cycle needs to recover, and that the recovered books equal an uninterrupted run;
* the symbol failure rate of a feed update and that the cycle still completes.

Nothing here is evidence about a strategy: the market is a deterministic random walk and the accounts are
invented. The figures describe the machine they were taken on (``host``); a Windows workstation measurement
is not a Linux certification and says so in the file it writes.

Shadow only. The harness holds no adapter, no client and no execution boundary, and the driver it builds has
``handoff=None``: it cannot place an order.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import tempfile
import time
import tracemalloc
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from . import daily_trend as DT
from .driver import COMPLETED, AccountContext, PlatformCycleDriver
from .family_registry import FamilyRegistry
from .market_data import FrozenPanelBars

HARNESS_VERSION = "cycle-performance-2"
SCHEMA = "cati-cycle-performance-v1"
FAMILY_ID = DT.FAMILY_ID
ACCOUNT_COUNTS: Tuple[int, ...] = (1, 10, 100, 500)
#: the fixture every account-scaling figure is taken on: 547 days x 26 contracts, cycles from 2020-09-01
FIXTURE_FIRST_DAY, FIXTURE_DAYS, FIXTURE_SYMBOLS, FIXTURE_SEED = "2020-01-01", 547, 26, 7
FIRST_CYCLE = "2020-09-01"
CYCLES = 6
#: market-stage scaling: (days, contracts). The last is the size of a production history (2020-01 to today).
MARKET_SIZES: Tuple[Tuple[int, int], ...] = ((547, 26), (1200, 150), (2470, 600))
#: the history the feed's own costs (first contact, reading a panel) are taken on
FEED_SIZES: Tuple[Tuple[int, int], ...] = ((1200, 150),)
DAY_MS = DT.DAY_MS
RISK_LEVELS: Tuple[str, ...] = ("conservative", "balanced", "aggressive")
NOT_A_CERTIFICATION = ("Measured on the host described under 'host'. A Windows workstation measurement is not a "
                       "Linux certification, and synthetic accounts are not connected customer accounts. Step 5 "
                       "scale certification is not claimed.")


# ------------------------------------------------------------------------------------------------- fixtures
def synthetic_market(*, days: int = FIXTURE_DAYS, symbols: int = FIXTURE_SYMBOLS, seed: int = FIXTURE_SEED,
                     first_day: str = FIXTURE_FIRST_DAY) -> Tuple[Any, Dict[str, Any]]:
    """A deterministic random-walk market in the research dataset's shape: one contract listed late, one that
    ends, funding known on every day. Never evidence about a strategy."""
    from app.market_data.daily_dataset import DailyPanel

    lo = DT.day_of(first_day)
    rng = np.random.default_rng(seed)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0.002, 0.03, (days, symbols)), axis=0))
    open_ = np.vstack([close[:1], close[:-1]]) * np.exp(rng.normal(0, 0.004, (days, symbols)))
    high = np.maximum(open_, close) * np.exp(np.abs(rng.normal(0, 0.008, (days, symbols))))
    low = np.minimum(open_, close) * np.exp(-np.abs(rng.normal(0, 0.008, (days, symbols))))
    volume = np.exp(rng.normal(16, 1.2, (1, symbols))) * np.exp(rng.normal(0, 0.25, (days, symbols)))
    fields = {"open": open_, "high": high, "low": low, "close": close, "quote_volume": volume}
    if symbols > 5:
        for matrix in fields.values():
            matrix[:60, 5] = np.nan                               # a late listing
            matrix[min(400, days - 20):, 3] = np.nan              # a contract that ends
    fields["funding_midnight"] = rng.normal(0.0001, 0.0001, (days, symbols))
    fields["funding_later_positive"] = np.abs(rng.normal(0.0001, 0.0001, (days, symbols)))
    fields["funding_later_negative"] = -np.abs(rng.normal(0.00003, 0.00005, (days, symbols)))
    fields["funding_hours"] = np.full((days, symbols), 24.0)
    width = max(2, len(str(symbols - 1)))
    names = [f"C{j:0{width}d}USDT" for j in range(symbols)]
    return DailyPanel(np.arange(lo, lo + days, dtype=np.int64), names, fields), {"symbols": {}}


class SyntheticAccounts:
    """``count`` invented connected accounts (a user, a venue, the engine's sizing policy). They have no
    credentials and no exchange; with ``handoff=None`` every one of them is a shadow book."""

    def __init__(self, count: int, *, initial_equity: float = 10_000.0) -> None:
        self._count, self._equity = int(count), float(initial_equity)

    def accounts(self, entry: Any) -> List[AccountContext]:
        from app.trading_intelligence.families.daily_trend import targets as T

        return [AccountContext(account_id=f"perf-{k:05d}", user_id=f"perf-user-{k:05d}", venue="binance",
                               environment="DEMO", risk_level=RISK_LEVELS[k % len(RISK_LEVELS)], policy=T.EXECUTABLE,
                               initial_equity=self._equity + 37.0 * k) for k in range(self._count)]


class SyntheticSource:
    """A feed source that answers from a panel: its bars and funding as the raw rows an archive file would
    hold, one request per call. ``fail``: symbols whose request raises. Never the network."""

    def __init__(self, panel: Any, metadata: Mapping[str, Any]) -> None:
        self._panel, self._metadata = panel, metadata
        self._lo, self._hi = int(panel.days[0]), int(panel.days[-1])
        self._column = {s: j for j, s in enumerate(panel.symbols)}
        self.fail: set = set()
        self.requests = 0

    def symbols(self) -> List[str]:
        self.requests += 1
        return list(self._panel.symbols)

    def metadata(self, fetched_at: str) -> Dict[str, Any]:
        self.requests += 1
        return {**self._metadata, "fetched_at": fetched_at}

    def _held(self, days: Sequence[int]) -> List[int]:
        return [d for d in days if self._lo <= d <= self._hi]

    def klines(self, symbol: str, days: Sequence[int]) -> Any:
        from app.market_data.daily_feed import Fetched

        self.requests += 1
        if symbol in self.fail:
            raise RuntimeError(f"the source has no answer for {symbol}")
        j, p, lo = self._column[symbol], self._panel, self._lo
        rows = [(d * DAY_MS, float(p.open[d - lo, j]), float(p.high[d - lo, j]), float(p.low[d - lo, j]),
                 float(p.close[d - lo, j]), 0.0, float(p.quote_volume[d - lo, j]), 1)
                for d in self._held(days) if not np.isnan(p.close[d - lo, j])]
        return Fetched(tuple(rows), frozenset(self._held(days)))

    def funding(self, symbol: str, days: Sequence[int]) -> Any:
        from app.market_data.daily_feed import Fetched

        self.requests += 1
        j, p, lo, rows = self._column[symbol], self._panel, self._lo, []
        for d in self._held(days):
            rows += [(d * DAY_MS, 8, float(p.funding_midnight[d - lo, j])),
                     (d * DAY_MS + 8 * 3_600_000, 8, float(p.funding_later_positive[d - lo, j])),
                     (d * DAY_MS + 16 * 3_600_000, 8, float(p.funding_later_negative[d - lo, j]))]
        return Fetched(tuple(rows), frozenset(self._held(days)))


class SteppedClock:
    def __init__(self) -> None:
        self.now = 0

    def __call__(self) -> int:
        return self.now

    def after_close(self, day: int) -> None:
        self.now = DT.close_ms(day) + 60_000


def fresh_database(directory: Path, name: str = "performance.db") -> Any:
    """A new, migrated database in ``directory``. Never the runtime database: the path is explicit."""
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    db = DB(str(Path(directory) / name))
    migrate(db)
    return db


# ------------------------------------------------------------------------------------ database statements
class StatementCounter:
    """A ``sqlite3`` trace callback: every statement the connection runs, by its first keyword. Entries a
    trigger program produces (SQLite reports them as ``-- TRIGGER ...``) are counted apart."""

    def __init__(self) -> None:
        self.total = 0
        self.by_verb: Dict[str, int] = {}

    def __call__(self, sql: str) -> None:
        head = sql.lstrip()
        verb = "TRIGGER" if head.startswith("--") else (head.split(None, 1)[0].upper().rstrip(";") if head else "EMPTY")
        if verb != "TRIGGER":
            self.total += 1
        self.by_verb[verb] = self.by_verb.get(verb, 0) + 1

    def snapshot(self) -> Tuple[int, Dict[str, int]]:
        return self.total, dict(self.by_verb)


def _delta(after: Mapping[str, int], before: Mapping[str, int]) -> Dict[str, int]:
    return {k: v - before.get(k, 0) for k, v in sorted(after.items()) if v - before.get(k, 0)}


class CountingDB:
    """The database object handed to the driver and the registry: the same database, with the trace callback
    installed on every connection it gives out. The three PRAGMAs of ``DB._open`` run before a connection is
    handed out and are reported from the connection count instead."""

    def __init__(self, db: Any, counter: StatementCounter) -> None:
        self._db, self._counter = db, counter

    @contextmanager
    def connect(self) -> Iterator[Any]:
        with self._db.connect() as conn:
            conn.set_trace_callback(self._counter)
            yield conn

    def __getattr__(self, name: str) -> Any:
        return getattr(self._db, name)


def _connections() -> Tuple[int, int]:
    from shared_lib.persistence.db import METRICS

    return METRICS.connections_opened, METRICS.connections_reused


# ------------------------------------------------------------------------------------------ account stage
class AccountProbe:
    """An ``account_runner`` that runs the accounts in the driver's order, one after the other (the driver's
    default), and records for each: when it started, how long it took, how many accounts were still queued
    and, when a counter is given, how many statements it ran."""

    def __init__(self, counter: Optional[StatementCounter] = None, *, interrupt_after: Optional[int] = None) -> None:
        self._counter, self._interrupt_after = counter, interrupt_after
        self.stages: List[Dict[str, Any]] = []

    def __call__(self, calls: Sequence[Callable[[], Any]]) -> List[Any]:
        out: List[Any] = []
        waits, durations, depths, statements = [], [], [], []
        origin = time.perf_counter()
        for k, call in enumerate(calls):
            if self._interrupt_after is not None and k >= self._interrupt_after:
                raise ProcessInterrupted(f"interrupted after {k} of {len(calls)} accounts")
            before = self._counter.total if self._counter else 0
            started = time.perf_counter()
            out.append(call())
            durations.append((time.perf_counter() - started) * 1000.0)
            waits.append((started - origin) * 1000.0)
            depths.append(len(calls) - k)
            if self._counter:
                statements.append(self._counter.total - before)
        self.stages.append({"accounts": len(calls), "durations_ms": durations, "waits_ms": waits, "depths": depths,
                            "statements": statements})
        return out


class ProcessInterrupted(BaseException):
    """Stands for a process that dies in the account stage. Not an ``Exception``: nothing in the driver may
    catch it, exactly as nothing catches a killed process."""


def _percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return float(ordered[min(len(ordered) - 1, int(round(q * (len(ordered) - 1))))])


def _queue(stage: Mapping[str, Any]) -> Dict[str, Any]:
    durations, depths = stage["durations_ms"], stage["depths"]
    total = sum(durations)
    return {"runner": "SEQUENTIAL (the driver's default)", "max_depth": max(depths, default=0),
            "time_weighted_mean_depth": round(sum(d * t for d, t in zip(depths, durations)) / total, 3) if total else 0.0,
            "longest_wait_ms": round(max(stage["waits_ms"], default=0.0), 3),
            "account_ms": {"p50": round(_percentile(durations, 0.5), 4), "p95": round(_percentile(durations, 0.95), 4),
                           "max": round(max(durations, default=0.0), 4)}}


# ------------------------------------------------------------------------------------------------ scenario
def build_driver(db: Any, *, data: Any, accounts: Any, research_register: Any = None,
                 account_runner: Optional[Callable[..., Any]] = None, owner: str = "performance") -> PlatformCycleDriver:
    registry = FamilyRegistry(db, research_register=research_register)
    registry.register_all(actor_ref="system:performance-harness")
    return PlatformCycleDriver(db, accounts=accounts, data={DT.TIMEFRAME: data}, registry=registry, owner=owner,
                               account_runner=account_runner, handoff=None)


def _run(driver: PlatformCycleDriver, day: int) -> Any:
    return driver.run_cycle(FAMILY_ID, DT.day_text(day), now_ms=DT.close_ms(day) + 60_000)


def measure_scenario(workdir: Path, *, panel: Any, metadata: Mapping[str, Any], accounts: int,
                     cycles: int = CYCLES, first_cycle: str = FIRST_CYCLE, research_register: Any = None,
                     memory: bool = True) -> Dict[str, Any]:
    """``cycles`` consecutive daily cycles for ``accounts`` accounts, three times on three fresh databases:
    timed (nothing else switched on), counted (the statement trace) and traced (``tracemalloc``)."""
    first = DT.day_of(first_cycle)
    days = [first + n for n in range(cycles)]
    source = SyntheticAccounts(accounts)

    # 1. timed
    data = FrozenPanelBars(panel, metadata)
    probe = AccountProbe()
    driver = build_driver(fresh_database(workdir, f"timed_{accounts}.db"), data=data, accounts=source,
                          research_register=research_register, account_runner=probe)
    timed: List[Dict[str, Any]] = []
    for day in days:
        t0 = time.perf_counter()
        result = _run(driver, day)
        wall = (time.perf_counter() - t0) * 1000.0
        if result.status != COMPLETED:
            raise RuntimeError(f"cycle {DT.day_text(day)} did not complete: {result.status} {result.reason}")
        stages = {k: round(v, 4) for k, v in result.timings_ms.items()}
        stages["OTHER (registry entry, cycle rows, controls, counts)"] = round(wall - sum(result.timings_ms.values()), 4)
        timed.append({"cycle_key": result.cycle_key, "end_to_end_ms": round(wall, 4), "stage_ms": stages,
                      "accounts_done": result.accounts_done, "intents": sum(r.get("intents", 0) for r in result.account_results.values()),
                      "accounts_per_second": round(accounts / (result.timings_ms["ACCOUNT_STAGE"] / 1000.0), 2)
                      if result.timings_ms.get("ACCOUNT_STAGE") else None,
                      "queue": _queue(probe.stages[-1])})
    market_data_calls = data.calls

    # 2. counted
    counter = StatementCounter()
    probe = AccountProbe(counter)
    driver = build_driver(CountingDB(fresh_database(workdir, f"counted_{accounts}.db"), counter), data=FrozenPanelBars(panel, metadata),
                          accounts=source, research_register=research_register, account_runner=probe)
    counted: List[Dict[str, Any]] = []
    for day in days:
        (total0, verbs0), (opened0, reused0) = counter.snapshot(), _connections()
        _run(driver, day)
        (total1, verbs1), (opened1, reused1) = counter.snapshot(), _connections()
        per_account = probe.stages[-1]["statements"]
        in_accounts = sum(per_account)
        counted.append({"cycle_key": DT.day_text(day), "statements": total1 - total0, "by_verb": _delta(verbs1, verbs0),
                        "account_stage_statements": in_accounts, "cycle_overhead_statements": total1 - total0 - in_accounts,
                        "statements_per_account": {"mean": round(in_accounts / accounts, 3), "max": max(per_account),
                                                   "min": min(per_account)},
                        "connections_opened": opened1 - opened0, "connections_reused": reused1 - reused0,
                        "connection_pragmas": 3 * (opened1 - opened0)})

    out: Dict[str, Any] = {"accounts": accounts, "cycles": len(days), "first_cycle": first_cycle,
                           "market_data_provider_calls": market_data_calls, "timed": timed, "counted": counted}

    # 3. traced
    if memory:
        driver = build_driver(fresh_database(workdir, f"traced_{accounts}.db"), data=FrozenPanelBars(panel, metadata),
                              accounts=source, research_register=research_register)
        peaks = []
        for day in days[:3]:
            tracemalloc.start()
            try:
                _run(driver, day)
                peaks.append(tracemalloc.get_traced_memory()[1])
            finally:
                tracemalloc.stop()
        out["memory"] = {"tracemalloc_peak_bytes_per_cycle": peaks, "peak_mib": round(max(peaks) / 2 ** 20, 3),
                         "note": "Python allocations during one cycle, the market stage included; the panel the "
                                 "provider already holds is not counted again"}
    steady = timed[1:] or timed
    out["summary"] = {
        "end_to_end_ms_median": round(statistics.median(c["end_to_end_ms"] for c in steady), 3),
        "market_stage_ms_median": round(statistics.median(c["stage_ms"]["MARKET_STAGE"] for c in steady), 3),
        "account_stage_ms_median": round(statistics.median(c["stage_ms"]["ACCOUNT_STAGE"] for c in steady), 3),
        "accounts_per_second_median": round(statistics.median(c["accounts_per_second"] for c in steady), 2),
        "ms_per_account_median": round(statistics.median(c["stage_ms"]["ACCOUNT_STAGE"] / accounts for c in steady), 4),
        "statements_per_cycle_median": int(statistics.median(c["statements"] for c in counted[1:] or counted)),
        "statements_per_account_mean": round(statistics.mean(c["statements_per_account"]["mean"] for c in counted[1:] or counted), 3),
        "cycle_overhead_statements": sorted({c["cycle_overhead_statements"] for c in counted[1:] or counted}),
        "connections_per_cycle_median": int(statistics.median(c["connections_opened"] for c in counted[1:] or counted)),
        "queue_max_depth": max(c["queue"]["max_depth"] for c in timed),
        "note": "medians over the cycles after the first (the first cycle only opens the books)"}
    return out


def measure_market_stage(sizes: Sequence[Tuple[int, int]] = MARKET_SIZES, *, repeats: int = 3, memory: bool = True) -> List[Dict[str, Any]]:
    """The market stage alone (``DailyTrendPipeline.market`` on the last day), by history length and number of
    contracts: the frozen rule recomputes every feature from the whole history, so this is where a cycle's
    cost grows with time."""
    out = []
    for days, symbols in sizes:
        panel, metadata = synthetic_market(days=days, symbols=symbols)
        data = FrozenPanelBars(panel, metadata)
        key = DT.day_text(int(panel.days[-1]))
        durations = []
        for _ in range(repeats):
            t0 = time.perf_counter()
            market = DT.PIPELINE.market(data, key)
            durations.append((time.perf_counter() - t0) * 1000.0)
        row: Dict[str, Any] = {"days": days, "symbols": symbols, "cells": days * symbols,
                               "market_stage_ms": {"min": round(min(durations), 3), "median": round(statistics.median(durations), 3)},
                               "panel_bytes": int(sum(v.nbytes for v in panel.fields.values())),
                               "universe": len(market.universe), "data_snapshot_id": market.data_snapshot_id}
        if memory:
            del market
            tracemalloc.start()
            try:
                DT.PIPELINE.market(data, key)
                row["tracemalloc_peak_bytes"] = tracemalloc.get_traced_memory()[1]
            finally:
                tracemalloc.stop()
            row["peak_over_panel"] = round(row["tracemalloc_peak_bytes"] / row["panel_bytes"], 3)
        out.append(row)
    return out


def measure_recovery(workdir: Path, *, panel: Any, metadata: Mapping[str, Any], accounts: int = 100, warm_cycles: int = 3,
                     first_cycle: str = FIRST_CYCLE, research_register: Any = None) -> Dict[str, Any]:
    """A cycle is interrupted when half of its accounts are done (the runner raises a ``BaseException``, as a
    killed process would simply stop). A new driver then runs the same cycle: how long until it is COMPLETED,
    how many accounts it had to process, and whether every book equals an uninterrupted run."""
    first = DT.day_of(first_cycle)
    source = SyntheticAccounts(accounts)
    reference_db = fresh_database(workdir, "recovery_reference.db")
    reference = build_driver(reference_db, data=FrozenPanelBars(panel, metadata), accounts=source,
                             research_register=research_register)
    for n in range(warm_cycles + 1):
        t0 = time.perf_counter()
        _run(reference, first + n)
        uninterrupted_ms = (time.perf_counter() - t0) * 1000.0
    db = fresh_database(workdir, "recovery.db")
    driver = build_driver(db, data=FrozenPanelBars(panel, metadata), accounts=source, research_register=research_register)
    for n in range(warm_cycles):
        _run(driver, first + n)
    day = first + warm_cycles
    interrupted = build_driver(db, data=FrozenPanelBars(panel, metadata), accounts=source, research_register=research_register,
                               account_runner=AccountProbe(interrupt_after=accounts // 2))
    try:
        _run(interrupted, day)
        raise RuntimeError("the interruption did not happen")
    except ProcessInterrupted:
        pass
    with db.connect() as conn:
        left = {"cycle_status": conn.execute("SELECT status FROM cati_platform_cycles WHERE cycle_key=?", (DT.day_text(day),)).fetchone()[0],
                "accounts_done": conn.execute("SELECT COUNT(*) FROM cati_platform_cycle_accounts a JOIN cati_platform_cycles c "
                                              "ON c.cycle_id=a.cycle_id WHERE c.cycle_key=? AND a.status='DONE'", (DT.day_text(day),)).fetchone()[0]}
    restarted = build_driver(db, data=FrozenPanelBars(panel, metadata), accounts=source, research_register=research_register)
    t0 = time.perf_counter()
    result = _run(restarted, day)
    recovery_ms = (time.perf_counter() - t0) * 1000.0
    replayed = sum(1 for r in result.account_results.values() if r.get("replayed"))

    def books(database: Any) -> List[Tuple[Any, ...]]:
        with database.connect() as conn:
            return [tuple(r) for r in conn.execute("SELECT account_id, equity, peak_equity, last_day, pending_orders FROM "
                                                   "cati_shadow_accounts ORDER BY account_id")]

    def decisions(database: Any) -> List[Tuple[Any, ...]]:
        with database.connect() as conn:
            return [tuple(r) for r in conn.execute("SELECT account_id, data_cutoff, decision_hash FROM cati_family_decisions "
                                                   "ORDER BY account_id, data_cutoff")]

    return {"accounts": accounts, "interrupted_after_accounts": accounts // 2, "state_left_behind": left,
            "recovered_status": result.status, "recovery_ms": round(recovery_ms, 3),
            "uninterrupted_cycle_ms": round(uninterrupted_ms, 3),
            "stage_ms": {k: round(v, 4) for k, v in result.timings_ms.items()},
            "accounts_left_alone": replayed, "accounts_processed_on_recovery": accounts - replayed,
            "books_equal_uninterrupted_run": books(db) == books(reference_db),
            "decisions_equal_uninterrupted_run": decisions(db) == decisions(reference_db),
            "duplicate_decisions": 0 if len(decisions(db)) == accounts * (warm_cycles + 1) else "CHECK"}


# ---------------------------------------------------------------------------------------------- market data
def measure_feed(workdir: Path, *, panel: Any, metadata: Mapping[str, Any], accounts: int = 10, cycles: int = CYCLES,
                 first_cycle: str = FIRST_CYCLE, research_register: Any = None, failing: int = 2) -> Dict[str, Any]:
    """The same cycles with the daily bar feed as the provider: how many requests of the source a cycle costs
    (first contact, then one closed day at a time), that a repeated cycle costs none, and a cycle in which
    ``failing`` contracts cannot be fetched (the symbol failure rate, and that the cycle still completes)."""
    from app.market_data.daily_feed import DailyBarFeed, FeedPolicy

    first, clock, source = DT.day_of(first_cycle), SteppedClock(), SyntheticSource(panel, metadata)
    db = fresh_database(workdir, "feed.db")
    feed = DailyBarFeed(db, fetcher=source, clock=clock, first_day=int(panel.days[0]), auto_update=True,
                        policy=FeedPolicy(settle_ms=0))
    driver = build_driver(db, data=feed, accounts=SyntheticAccounts(accounts), research_register=research_register)
    out_cycles: List[Dict[str, Any]] = []
    for n in range(cycles):
        day = first + n
        clock.after_close(day)
        source.fail = set(panel.symbols[-failing:]) if n == cycles - 1 and failing else set()
        before, extensions, builds = source.requests, feed.extensions, feed.builds
        t0 = time.perf_counter()
        result = driver.run_cycle(FAMILY_ID, DT.day_text(day), now_ms=clock.now)
        wall = (time.perf_counter() - t0) * 1000.0
        update = feed.status()["last_updates"][0]
        fresh = feed.freshness(day)
        out_cycles.append({
            "cycle_key": result.cycle_key, "status": result.status, "reason": result.reason, "end_to_end_ms": round(wall, 3),
            "market_stage_ms": round(result.timings_ms.get("MARKET_STAGE", 0.0), 3),
            "market_data_requests": source.requests - before, "symbol_days_requested": update["symbol_days_requested"],
            "symbols_requested": update["symbols_requested"], "symbols_failed": update["symbols_failed"],
            "symbol_failure_rate": round(update["symbols_failed"] / update["symbols_requested"], 6) if update["symbols_requested"] else 0.0,
            "bars_stored": update["bars_stored"], "bar_coverage": fresh["coverage"],
            "panel": "EXTENDED_BY_THE_NEW_DAY" if feed.extensions > extensions else "READ_WHOLE" if feed.builds > builds else "CACHED"})
    last = first + cycles - 1
    before = source.requests
    replayed = driver.run_cycle(FAMILY_ID, DT.day_text(last), now_ms=clock.now)
    feed.panel_until(last)
    symbols, history = len(panel.symbols), last - int(panel.days[0]) + 1
    return {"accounts": accounts, "cycles": out_cycles,
            "first_cycle_is_first_contact": "the whole history is fetched once, by the first cycle",
            "requests_per_daily_cycle": sorted({c["market_data_requests"] for c in out_cycles[1:-1]}),
            "requests_when_a_completed_cycle_is_run_again": source.requests - before, "replayed": bool(replayed.replayed),
            "symbol_days_per_cycle_if_the_history_were_downloaded_again": symbols * history,
            "symbol_days_per_daily_cycle": sorted({c["symbol_days_requested"] for c in out_cycles[1:-1]}),
            "failing_cycle": out_cycles[-1], "requests_total": feed.status()["http_requests_total"]}


def measure_feed_scale(workdir: Path, sizes: Sequence[Tuple[int, int]] = FEED_SIZES, *, memory: bool = True) -> List[Dict[str, Any]]:
    """What the feed itself costs by history size: the first contact, one daily update, reading a sealed day
    whole (a new process) and extending yesterday's panel by the new day (a running process)."""
    from app.market_data.daily_feed import DailyBarFeed

    out = []
    for days, symbols in sizes:
        panel, metadata = synthetic_market(days=days, symbols=symbols)
        lo, clock, source = int(panel.days[0]), SteppedClock(), SyntheticSource(panel, metadata)
        db = fresh_database(workdir, f"feed_{days}_{symbols}.db")
        feed = DailyBarFeed(db, fetcher=source, clock=clock, first_day=lo)
        clock.after_close(lo + days - 2)
        t0 = time.perf_counter()
        first = feed.update()
        first_ms = (time.perf_counter() - t0) * 1000.0
        t0 = time.perf_counter()
        feed.panel_until(lo + days - 2)
        whole_ms = (time.perf_counter() - t0) * 1000.0
        clock.after_close(lo + days - 1)
        t0 = time.perf_counter()
        daily = feed.update()
        daily_ms = (time.perf_counter() - t0) * 1000.0
        t0 = time.perf_counter()
        extended, extended_metadata = feed.panel_until(lo + days - 1)
        extended_ms = (time.perf_counter() - t0) * 1000.0
        cold = DailyBarFeed(db, clock=clock)
        t0 = time.perf_counter()
        whole, _ = cold.panel_until(lo + days - 1)
        cold_ms = (time.perf_counter() - t0) * 1000.0
        row: Dict[str, Any] = {
            "days": days, "symbols": symbols, "bars": first["bars_stored"] + daily["bars_stored"],
            "funding_records": first["funding_stored"] + daily["funding_stored"],
            "first_contact": {"ms": round(first_ms, 1), "requests": first["http_requests"], "symbol_days": first["symbol_days_requested"]},
            "daily_update": {"ms": round(daily_ms, 2), "requests": daily["http_requests"], "symbol_days": daily["symbol_days_requested"]},
            "panel_read_whole_ms": round(max(whole_ms, cold_ms), 1), "panel_read_whole_ms_second": round(cold_ms, 1),
            "panel_extended_by_one_day_ms": round(extended_ms, 2),
            "extended_equals_whole": DT.snapshot_id(extended) == DT.snapshot_id(whole),
            "prices_equal_the_source_panel": all(np.array_equal(extended.fields[k], panel.fields[k], equal_nan=True)
                                                 for k in ("open", "high", "low", "close", "quote_volume")),
            "market_stage_ms_on_the_served_panel": None, "database_bytes": Path(db.path).stat().st_size}
        t0 = time.perf_counter()
        DT.market_stage(extended, extended_metadata, lo + days - 1)
        row["market_stage_ms_on_the_served_panel"] = round((time.perf_counter() - t0) * 1000.0, 1)
        if memory:
            tracemalloc.start()
            try:
                DailyBarFeed(db, clock=clock).panel_until(lo + days - 1)
                row["panel_read_whole_peak_bytes"] = tracemalloc.get_traced_memory()[1]
            finally:
                tracemalloc.stop()
        out.append(row)
    return out


def measure_registry(workdir: Path, *, research_register: Any = None, repeats: int = 5) -> Dict[str, Any]:
    """What the governance questions of a cycle cost: the registry and the research register are read again for
    every one of them, on purpose (a family is verified now, not as of an earlier read)."""
    registry = FamilyRegistry(fresh_database(workdir, "registry.db"), research_register=research_register)
    registry.register_all(actor_ref="system:performance-harness")

    def best(call: Callable[[], Any]) -> float:
        durations = []
        for _ in range(repeats):
            t0 = time.perf_counter()
            call()
            durations.append((time.perf_counter() - t0) * 1000.0)
        return round(min(durations), 3)

    return {"families_defined": len(registry.definitions()),
            "entry_ms (once per cycle)": best(lambda: registry.entry(FAMILY_ID)),
            "verify_mandate_ms (once per cycle)": best(lambda: DT.PIPELINE.verify(registry._research())),
            "evaluable_ms (once per run_due)": best(registry.evaluable),
            "research_register": "the committed register, read-only" if research_register is None else "injected"}


# -------------------------------------------------------------------------------------------------- report
def host_description() -> Dict[str, Any]:
    import sqlite3

    return {"platform": platform.platform(), "system": platform.system(), "machine": platform.machine(),
            "processor": platform.processor(), "cpu_count": os.cpu_count(), "python": platform.python_version(),
            "numpy": np.__version__, "sqlite": sqlite3.sqlite_version,
            "sqlite_synchronous": os.environ.get("SQLITE_SYNCHRONOUS", "NORMAL (default)"),
            "statement": NOT_A_CERTIFICATION}


def measure(workdir: Path, *, account_counts: Sequence[int] = ACCOUNT_COUNTS, cycles: int = CYCLES,
            market_sizes: Sequence[Tuple[int, int]] = MARKET_SIZES, recovery_accounts: int = 100,
            feed_sizes: Sequence[Tuple[int, int]] = FEED_SIZES, research_register: Any = None, memory: bool = True,
            label: str = "current") -> Dict[str, Any]:
    panel, metadata = synthetic_market()
    workdir = Path(workdir)
    out: Dict[str, Any] = {
        "label": label, "harness_version": HARNESS_VERSION,
        "fixtures": {"market": {"kind": "deterministic random walk (synthetic_market)", "days": FIXTURE_DAYS,
                                "symbols": FIXTURE_SYMBOLS, "seed": FIXTURE_SEED, "first_day": FIXTURE_FIRST_DAY},
                     "accounts": "SyntheticAccounts: invented connected accounts, EXECUTABLE policy, risk levels in rotation",
                     "first_cycle": FIRST_CYCLE, "cycles": cycles,
                     "research_register": "the committed register, read-only" if research_register is None else "injected"},
        "scenarios": [measure_scenario(workdir, panel=panel, metadata=metadata, accounts=n, cycles=cycles,
                                       research_register=research_register, memory=memory) for n in account_counts],
        "market_stage_by_history": measure_market_stage(market_sizes, memory=memory),
        "recovery": measure_recovery(workdir, panel=panel, metadata=metadata, accounts=recovery_accounts,
                                     research_register=research_register),
        "market_data_feed": measure_feed(workdir, panel=panel, metadata=metadata, cycles=cycles,
                                         research_register=research_register),
        "market_data_feed_by_history": measure_feed_scale(workdir, feed_sizes, memory=memory),
        "registry_and_register": measure_registry(workdir, research_register=research_register)}
    return out


COMPARED: Tuple[Tuple[str, str], ...] = (
    ("end_to_end_ms_median", "end-to-end cycle, ms (median)"), ("market_stage_ms_median", "market stage, ms"),
    ("account_stage_ms_median", "account stage, ms"), ("ms_per_account_median", "account stage, ms per account"),
    ("accounts_per_second_median", "accounts per second"),
    ("statements_per_cycle_median", "database statements per cycle"),
    ("statements_per_account_mean", "database statements per account"),
    ("connections_per_cycle_median", "database connections per cycle"))


def compare(before: Mapping[str, Any], after: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Before and after on the same fixtures, metric by metric. Refuses to compare different fixtures."""
    if before["fixtures"] != after["fixtures"]:
        raise ValueError("the two measurements were not taken on the same fixtures")
    rows: List[Dict[str, Any]] = []
    earlier = {s["accounts"]: s for s in before["scenarios"]}
    for scenario in after["scenarios"]:
        base = earlier.get(scenario["accounts"])
        if base is None:
            continue
        for key, title in COMPARED:
            b, a = base["summary"][key], scenario["summary"][key]
            rows.append({"accounts": scenario["accounts"], "metric": title, "before": b, "after": a,
                         "change_percent": round(100.0 * (a - b) / b, 2) if b else None})
        if "memory" in base and "memory" in scenario:
            b, a = base["memory"]["peak_mib"], scenario["memory"]["peak_mib"]
            rows.append({"accounts": scenario["accounts"], "metric": "peak memory of a cycle, MiB", "before": b, "after": a,
                         "change_percent": round(100.0 * (a - b) / b, 2) if b else None})
    sizes = {(m["days"], m["symbols"]): m for m in before.get("market_stage_by_history", ())}
    for m in after.get("market_stage_by_history", ()):
        base = sizes.get((m["days"], m["symbols"]))
        if base is None:
            continue
        for key, title, pick in (("market_stage_ms", "market stage, ms (median)", lambda r: r["market_stage_ms"]["median"]),
                                 ("tracemalloc_peak_bytes", "market stage peak memory, MiB",
                                  lambda r: round(r.get("tracemalloc_peak_bytes", 0) / 2 ** 20, 3))):
            b, a = pick(base), pick(m)
            rows.append({"history": f"{m['days']} days x {m['symbols']} contracts", "metric": title, "before": b, "after": a,
                         "change_percent": round(100.0 * (a - b) / b, 2) if b else None,
                         "same_snapshot_id": base["data_snapshot_id"] == m["data_snapshot_id"]})
    return rows


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.trading_intelligence.pipeline.performance",
                                     description="Measure the CATI platform cycle on synthetic fixtures.")
    parser.add_argument("--out", required=True, help="the JSON file to write")
    parser.add_argument("--baseline", default=None, help="an earlier measurement (same fixtures) to compare against")
    parser.add_argument("--label", default="current")
    parser.add_argument("--accounts", default=",".join(str(n) for n in ACCOUNT_COUNTS))
    parser.add_argument("--cycles", type=int, default=CYCLES)
    parser.add_argument("--no-memory", action="store_true")
    parser.add_argument("--feed-sizes", default=";".join(f"{d}x{n}" for d, n in FEED_SIZES),
                        help="histories the feed's own costs are taken on, as DAYSxSYMBOLS;...")
    args = parser.parse_args(argv)
    # the harness only ever opens the temporary databases it creates; test mode makes any other refusal loud
    os.environ.setdefault("COSMICFORGE_TEST_MODE", "1")
    counts = tuple(int(x) for x in args.accounts.split(",") if x.strip())
    with tempfile.TemporaryDirectory(prefix="cati_cycle_performance_") as tmp:
        os.environ.setdefault("DATABASE_URL", "sqlite:///" + str(Path(tmp) / "session.db"))
        feed_sizes = tuple(tuple(int(x) for x in item.split("x")) for item in args.feed_sizes.split(";") if item.strip())
        measured = measure(Path(tmp), account_counts=counts, cycles=args.cycles, memory=not args.no_memory, label=args.label,
                           feed_sizes=feed_sizes)
    document: Dict[str, Any] = {"schema": SCHEMA, "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                                "host": host_description(), **measured}
    if args.baseline:
        before = json.loads(Path(args.baseline).read_text(encoding="utf-8"))
        document["baseline"] = {k: before[k] for k in ("label", "generated_at", "harness_version", "scenarios",
                                                        "market_stage_by_history", "recovery") if k in before}
        document["before_and_after"] = compare(before, measured)
    path = Path(args.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((json.dumps(document, indent=1, sort_keys=True) + "\n").encode("utf-8"))
    print(json.dumps({"written": str(path), "scenarios": [{"accounts": s["accounts"], **s["summary"]} for s in measured["scenarios"]]},
                     indent=1, default=str))
    return 0


__all__ = ["HARNESS_VERSION", "SCHEMA", "ACCOUNT_COUNTS", "MARKET_SIZES", "FEED_SIZES", "synthetic_market",
           "SyntheticAccounts", "SyntheticSource", "SteppedClock", "fresh_database", "StatementCounter", "CountingDB",
           "AccountProbe", "ProcessInterrupted", "build_driver", "measure_scenario", "measure_market_stage",
           "measure_recovery", "measure_feed", "measure_feed_scale", "measure_registry", "measure", "compare",
           "host_description", "main"]

if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
