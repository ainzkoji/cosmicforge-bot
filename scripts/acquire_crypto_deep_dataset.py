#!/usr/bin/env python3
"""CATI crypto DEEP dataset (Sections 10.4, 10.5, 11.6): frozen selection, 1m acquisition, 5m derivation,
supplemental features -- bounded, resumable, public endpoints only (no credentials).

Selection (``select``)
    The deep subset is 35 members of the frozen broad v1 universe
    (``docs/research/cati_crypto_universe_binance_v1.json``) ranked by HISTORICAL liquidity: the median UTC-daily
    quote volume over the 365 complete days ending at the v1 window end, computed read-only from the canonical
    ``historical_candles`` 15m rows (reproducible from the DB; no live ticker, no strategy results). Frozen into
    ``docs/research/cati_crypto_deep_universe_binance_v1.json`` with its own hash (the parent is untouched).

Acquisition (``acquire``)
    Binance USD-M public ``/fapi/v1/klines`` 1m, per member from ``requested_start_ms`` (target 1826 days before the
    window end, or the listing time -> INSUFFICIENT_HISTORY) to the window end. One period = one UTC month, logged in
    ``market_ingest_log``: FETCHED / EMPTY / NOT_LISTED / FAILED. FETCHED periods are skipped on resume; rows are
    written before the log (INSERT OR IGNORE), so an interruption only causes a re-fetch. Rate limits: pacing,
    ``X-MBX-USED-WEIGHT-1M`` back-off, Retry-After on 429, stop on 418, circuit breaker. Invalid OHLC rows are not
    stored and are counted in the period's reason (never repaired silently).

Derivation (``derive``)
    5m bars from the stored 1m per member-month (complete, aligned windows only). Quote volume / trades are summed
    only when every minute carries them -- otherwise NULL, never 0.

Features (``features``)
    funding (full history), mark and index price (1h klines), basis_bps (derived mark vs index), open interest
    (the venue serves ~30 days: AVAILABLE there, NO_HISTORICAL_ENDPOINT before), spread / book depth
    (NO_HISTORICAL_ENDPOINT), liquidations (NOT_SUPPORTED_BY_PROVIDER). Unavailable is a reasoned observation,
    never 0.

    python scripts/acquire_crypto_deep_dataset.py --db data/research/crypto_deep_binance.db select --canonical-db <cosmicforge.db>
    python scripts/acquire_crypto_deep_dataset.py --db data/research/crypto_deep_binance.db plan
    python scripts/acquire_crypto_deep_dataset.py --db data/research/crypto_deep_binance.db acquire [--max-minutes N]
    python scripts/acquire_crypto_deep_dataset.py --db data/research/crypto_deep_binance.db derive
    python scripts/acquire_crypto_deep_dataset.py --db data/research/crypto_deep_binance.db features
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(REPO, "backends", "bot-backend"), os.path.join(REPO, "backends", "shared")]

VENUE = "binance_usdm"
FAPI = "https://fapi.binance.com"
SOURCE = "binance_fapi_klines"
SOURCE_VERSION = "fapi.binance.com/fapi/v1/klines:v1"
PARENT = os.path.join(REPO, "docs", "research", "cati_crypto_universe_binance_v1.json")
DEEP = os.path.join(REPO, "docs", "research", "cati_crypto_deep_universe_binance_v1.json")
DAY_MS, MIN_MS = 86_400_000, 60_000
DONE = ("FETCHED", "EMPTY", "NOT_LISTED", "UNAVAILABLE")


class ProviderUnavailable(RuntimeError):
    pass


class BinancePublic:
    """Public fapi GETs with pacing, weight back-off, bounded retries and a circuit breaker."""

    def __init__(self, pace_s: float = 0.2, max_requests: Optional[int] = None, deadline: Optional[float] = None,
                 breaker: int = 8, session: Any = None):
        if session is None:
            import requests

            session = requests.Session()
        self.s = session
        self.pace_s, self.max_requests, self.deadline, self.breaker = pace_s, max_requests, deadline, breaker
        self.last = 0.0
        self.requests = 0
        self.failures = 0

    def budget_left(self) -> bool:
        if self.max_requests is not None and self.requests >= self.max_requests:
            return False
        return self.deadline is None or time.time() < self.deadline

    def get(self, path: str, params: Dict[str, Any]) -> Any:
        for attempt in range(5):
            wait = self.pace_s - (time.time() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()
            self.requests += 1
            try:
                r = self.s.get(FAPI + path, params=params, timeout=30)
            except Exception:
                self._fail()
                time.sleep(min(10 * (attempt + 1), 60))
                continue
            used = int(r.headers.get("X-MBX-USED-WEIGHT-1M", "0") or 0)
            if used > 1800:  # stay well under the 2400/min IP budget
                time.sleep(max(1.0, 61 - datetime.now(timezone.utc).second))
            if r.status_code == 418:
                raise ProviderUnavailable("HTTP 418: IP banned by the venue; stop and resume later")
            if r.status_code == 429:
                self._fail()
                time.sleep(float(r.headers.get("Retry-After", 30)))
                continue
            if r.status_code >= 500 or r.status_code == 408:  # server-side / request timeout: transient
                self._fail()
                time.sleep(min(10 * (attempt + 1), 60))
                continue
            if r.status_code == 400:
                self.failures = 0
                raise ValueError(f"HTTP 400 {r.text[:120]}")
            r.raise_for_status()
            self.failures = 0
            return r.json()
        raise RuntimeError("persistent failure")

    def _fail(self):
        self.failures += 1
        if self.failures >= self.breaker:
            raise ProviderUnavailable(f"{self.failures} consecutive failures")


# ── helpers ──────────────────────────────────────────────────────────────────

def _db(path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.market_data_schema import ensure_market_data_schema

    db = DB(path=path)
    ensure_market_data_schema(db)
    return db


def _log(conn, sym, dataset, tf, period, status, rows=0, reason=None):
    conn.execute("INSERT OR REPLACE INTO market_ingest_log (venue, venue_symbol, dataset, timeframe, period, status, "
                 "rows, reason, recorded_at) VALUES (?,?,?,?,?,?,?,?,?)",
                 (VENUE, sym, dataset, tf, period, status, rows, reason, int(time.time() * 1000)))


def _status(conn, sym, dataset, tf, period):
    r = conn.execute("SELECT status FROM market_ingest_log WHERE venue=? AND venue_symbol=? AND dataset=? AND "
                     "timeframe=? AND period=?", (VENUE, sym, dataset, tf, period)).fetchone()
    return r[0] if r else None


def months(start_ms: int, end_ms: int) -> Iterable[Tuple[str, int, int]]:
    """(YYYY-MM, period_start, period_end) covering [start, end) in UTC months."""
    d = datetime.fromtimestamp(start_ms / 1000, timezone.utc)
    y, m = d.year, d.month
    while True:
        a = int(datetime(y, m, 1, tzinfo=timezone.utc).timestamp() * 1000)
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        b = int(datetime(ny, nm, 1, tzinfo=timezone.utc).timestamp() * 1000)
        if a >= end_ms:
            return
        yield f"{y}-{m:02d}", max(a, start_ms), min(b, end_ms)
        y, m = ny, nm


def valid_kline(k: List[Any], step_ms: int = MIN_MS) -> bool:
    try:
        t, o, h, lo, c, v, ct = int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]), int(k[6])
    except (TypeError, ValueError, IndexError):
        return False
    return (h >= max(o, c, lo) and lo <= min(o, c, h) and v >= 0 and t % step_ms == 0 and ct == t + step_ms - 1
            and min(o, h, lo, c) > 0)


def _sid(member):
    from app.market_data.store import SeriesId

    return SeriesId(VENUE, member["venue_symbol"], member["canonical_instrument_id"], member["asset_class"],
                    member["product_type"], SOURCE, SOURCE_VERSION)


def load_deep(path=DEEP):
    from app.market_data.universe import load_deep_universe

    return load_deep_universe(path)


# ── select ───────────────────────────────────────────────────────────────────

def liquidity_scores(canonical_db: str, parent: Dict[str, Any], *, days: int = 365) -> Tuple[Dict, Dict, int, int]:
    """venue_symbol -> median UTC-daily quote volume over the complete days of [end - days, end) (read-only)."""
    end = int(parent["window_end_ms"])
    start = end - days * DAY_MS
    conn = sqlite3.connect(f"file:{canonical_db}?mode=ro", uri=True)
    scores, observed = {}, {}
    for m in parent["members"]:
        vols = [r[0] for r in conn.execute(
            "SELECT SUM(quote_volume) FROM historical_candles WHERE symbol=? AND interval='15m' AND "
            "data_source='binance' AND market_type='crypto' AND open_time>=? AND open_time<? AND quote_volume IS NOT "
            "NULL GROUP BY open_time/86400000 HAVING COUNT(*)=96", (m["venue_symbol"], start, end))]
        observed[m["venue_symbol"]] = len(vols)
        # fewer than half the days observed -> liquidity UNKNOWN (never ranked)
        scores[m["venue_symbol"]] = statistics.median(vols) if len(vols) >= days // 2 else None
    conn.close()
    return scores, observed, start, end


def _git_commit():
    try:
        import subprocess

        return subprocess.check_output(["git", "-C", REPO, "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return None


def cmd_select(args, db, api):
    from app.market_data.universe import build_deep_universe_manifest, load_frozen_universe

    parent = load_frozen_universe(PARENT)
    scores, observed, ws, we = liquidity_scores(args.canonical_db, parent, days=args.liquidity_days)
    manifest = build_deep_universe_manifest(
        parent, scores, days_observed=observed, liquidity_window_start_ms=ws, liquidity_window_end_ms=we,
        liquidity_source="canonical historical_candles 15m quote_volume (binance, data_source='binance')",
        generated_at=datetime.now(timezone.utc).isoformat(), code_commit=_git_commit(), size=args.size,
        target_history_days=args.target_days)
    if os.path.exists(args.out):
        prior = load_deep(args.out)
        if prior["universe_hash"] != manifest["universe_hash"]:
            raise SystemExit(f"{args.out} is frozen with a different identity; write a NEW versioned file")
        print(json.dumps({"unchanged": prior["universe_hash"]}))
        return
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
    print(json.dumps({"out": args.out, "members": len(manifest["members"]),
                      "insufficient_history": sum(m["history_status"] == "INSUFFICIENT_HISTORY"
                                                  for m in manifest["members"]),
                      "universe_hash": manifest["universe_hash"]}))


# ── plan / acquire ───────────────────────────────────────────────────────────

def _periods(member, window_end):
    return list(months(int(member["requested_start_ms"]), int(window_end)))


def cmd_plan(args, db, api):
    deep = load_deep(args.manifest)
    out = {"members": len(deep["members"]), "per_member": {}, "remaining_periods": 0, "done_periods": 0}
    with db.connect() as conn:
        for m in deep["members"]:
            ps = _periods(m, deep["window_end_ms"])
            done = sum(1 for p, _, _ in ps if _status(conn, m["venue_symbol"], "klines", "1m", p) in DONE)
            out["per_member"][m["venue_symbol"]] = {"periods": len(ps), "done": done,
                                                    "history_status": m["history_status"]}
            out["remaining_periods"] += len(ps) - done
            out["done_periods"] += done
    out["remaining_requests_estimate"] = out["remaining_periods"] * 45
    print(json.dumps(out, indent=2))


def acquire_member(db, api, member, window_end, *, store) -> Dict[str, Any]:
    sym = member["venue_symbol"]
    sid = _sid(member)
    listed = int(member["listed_at_ms"]) if member.get("listed_at_ms") else None
    res = {"fetched": 0, "skipped": 0, "failed": 0, "rows": 0, "stopped": None}
    for period, a, b in _periods(member, window_end):
        with db.connect() as conn:
            if _status(conn, sym, "klines", "1m", period) in DONE:
                res["skipped"] += 1
                continue
            if listed is not None and b <= listed:
                _log(conn, sym, "klines", "1m", period, "NOT_LISTED", 0, "BEFORE_LISTING")
                continue
        if not api.budget_left():
            res["stopped"] = "BUDGET_EXHAUSTED"
            return res
        cursor, n, invalid = max(a, (listed // MIN_MS) * MIN_MS if listed else a), 0, 0
        try:
            while cursor < b:
                batch = api.get("/fapi/v1/klines", {"symbol": sym, "interval": "1m", "startTime": cursor,
                                                    "endTime": b - 1, "limit": 1000}) or []
                if not batch:
                    break
                good = [k for k in batch if valid_kline(k) and a <= int(k[0]) < b]
                invalid += sum(1 for k in batch if not valid_kline(k))
                store.write_candles(sid, "1m", [[int(k[0]), k[1], k[2], k[3], k[4], k[5], int(k[6]), k[7], int(k[8])]
                                                for k in good])
                n += len(good)
                nxt = int(batch[-1][0]) + MIN_MS
                if nxt <= cursor:
                    break
                cursor = nxt
        except ProviderUnavailable:
            raise
        except Exception as exc:
            with db.connect() as conn:
                _log(conn, sym, "klines", "1m", period, "FAILED", n, f"FETCH_FAILED:{str(exc)[:120]}")
            res["failed"] += 1
            continue
        with db.connect() as conn:
            _log(conn, sym, "klines", "1m", period, "FETCHED" if n else "EMPTY", n,
                 f"INVALID_ROWS_REJECTED={invalid}" if invalid else None)
        res["fetched"] += 1
        res["rows"] += n
    return res


def cmd_acquire(args, db, api):
    from app.market_data.store import MarketDataStore

    deep = load_deep(args.manifest)
    store = MarketDataStore(db)
    only = {s.strip().upper() for s in args.symbols.split(",")} if args.symbols else None
    summary = {"members": {}, "stopped": None}
    for m in deep["members"]:
        if only and m["venue_symbol"] not in only:
            continue
        try:
            r = acquire_member(db, api, m, deep["window_end_ms"], store=store)
        except ProviderUnavailable as exc:
            summary["stopped"] = f"PROVIDER_UNAVAILABLE:{exc}"
            break
        summary["members"][m["venue_symbol"]] = r
        print(json.dumps({m["venue_symbol"]: r, "requests": api.requests}), flush=True)
        if r["stopped"]:
            summary["stopped"] = r["stopped"]
            break
    summary["requests"] = api.requests
    print(json.dumps(summary, indent=2))


# ── derive 5m ────────────────────────────────────────────────────────────────

def derive_rows(rows_1m: List[List[Any]], timeframe: str = "5m") -> List[List[Any]]:
    """Deterministic 1m -> ``timeframe`` (complete aligned windows only). Quote volume / trades are summed only
    when every minute of the window has them; otherwise None -- never a fabricated 0."""
    from app.research.dataset import DERIVABLE, derive

    step = DERIVABLE[timeframe] * MIN_MS
    extra: Dict[int, List[Any]] = {}
    for r in rows_1m:
        extra.setdefault((int(r[0]) // step) * step, []).append(r)
    out = []
    for bar in derive(rows_1m, timeframe):
        g = extra[int(bar[0])]
        qv = sum(float(r[7]) for r in g) if all(r[7] is not None for r in g) else None
        tr = sum(int(r[8]) for r in g) if all(r[8] is not None for r in g) else None
        out.append([bar[0], bar[1], bar[2], bar[3], bar[4], bar[5], bar[6], qv, tr])
    return out


def cmd_derive(args, db, api):
    from app.market_data.store import MarketDataStore

    deep = load_deep(args.manifest)
    store = MarketDataStore(db)
    for m in deep["members"]:
        sym, sid, n = m["venue_symbol"], _sid(m), 0
        for period, a, b in _periods(m, deep["window_end_ms"]):
            with db.connect() as conn:
                if _status(conn, sym, "klines", "1m", period) != "FETCHED" or \
                        _status(conn, sym, "klines", "5m", period) == "FETCHED":
                    continue
                rows = [list(r) for r in conn.execute(
                    "SELECT open_time, open, high, low, close, volume, close_time, quote_volume, trades FROM "
                    "market_candles WHERE venue=? AND venue_symbol=? AND timeframe='1m' AND source=? AND "
                    "open_time>=? AND open_time<? ORDER BY open_time", (VENUE, sym, SOURCE, a, b))]
            bars = derive_rows(rows, "5m")
            store.write_candles(sid, "5m", bars, derived_from="1m")
            with db.connect() as conn:
                _log(conn, sym, "klines", "5m", period, "FETCHED" if bars else "EMPTY", len(bars),
                     "DERIVED_FROM_1M:resample-1m-v1")
            n += len(bars)
        print(json.dumps({sym: {"5m_rows_written": n}}), flush=True)


# ── supplemental features ────────────────────────────────────────────────────

UNAVAILABLE_FEATURES = (
    ("spread_bps", "NO_HISTORICAL_ENDPOINT:binance_usdm publishes no historical bid/ask spread"),
    ("book_bid_depth", "NO_HISTORICAL_ENDPOINT:binance_usdm publishes no historical order-book snapshots"),
    ("book_ask_depth", "NO_HISTORICAL_ENDPOINT:binance_usdm publishes no historical order-book snapshots"),
    ("liquidations_long", "NOT_SUPPORTED_BY_PROVIDER:BINANCE_USDM_NO_COMPLETE_HISTORICAL_LIQUIDATION_FEED"),
    ("liquidations_short", "NOT_SUPPORTED_BY_PROVIDER:BINANCE_USDM_NO_COMPLETE_HISTORICAL_LIQUIDATION_FEED"),
)


def _feature_rows(api, path, params_for, parse, a, b, step_ms):
    out, cursor = [], a
    while cursor < b:
        batch = api.get(path, params_for(cursor, b)) or []
        if not batch:
            break
        rows = parse(batch)
        out.extend(rows)
        nxt = max(t for t, _ in rows) + step_ms if rows else b
        if nxt <= cursor:
            break
        cursor = nxt
    return out


def features_member(db, api, member, window_end, *, store, now_ms) -> Dict[str, Any]:
    sym, sid = member["venue_symbol"], _sid(member)
    start, end = int(member["requested_start_ms"]), int(window_end)
    res: Dict[str, Any] = {}
    kl = lambda b: [(int(k[0]) + 3_599_999, float(k[4])) for k in b]  # noqa: E731  hourly close observed at bar close
    specs = (
        ("funding_rate", "/fapi/v1/fundingRate",
         lambda c, b: {"symbol": sym, "startTime": c, "endTime": b - 1, "limit": 1000},
         lambda b: [(int(r["fundingTime"]), float(r["fundingRate"])) for r in b], 1),
        ("mark_price", "/fapi/v1/markPriceKlines",
         lambda c, b: {"symbol": sym, "interval": "1h", "startTime": c, "endTime": b - 1, "limit": 1000}, kl, 3_600_000),
        ("index_price", "/fapi/v1/indexPriceKlines",
         lambda c, b: {"pair": sym, "interval": "1h", "startTime": c, "endTime": b - 1, "limit": 1000}, kl, 3_600_000),
    )
    with db.connect() as conn:
        for feat, *_ in specs:
            if _status(conn, sym, feat, "native", "ALL") in DONE:
                res[feat] = "SKIPPED_DONE"
    for feat, path, params_for, parse, step in specs:
        if res.get(feat) == "SKIPPED_DONE":
            continue
        if not api.budget_left():
            res["stopped"] = "BUDGET_EXHAUSTED"
            return res
        try:
            rows = _feature_rows(api, path, params_for, parse, start, end, step)
        except ProviderUnavailable:
            raise
        except Exception as exc:
            with db.connect() as conn:
                _log(conn, sym, feat, "native", "ALL", "FAILED", 0, f"PROVIDER_FAILURE:{str(exc)[:120]}")
            res[feat] = "PROVIDER_FAILURE"
            continue
        rows = [(t, v) for t, v in rows if start <= t < end + 3_600_000]
        for t, v in rows:
            store.record_feature(sid, feat, t, value=v)
        with db.connect() as conn:
            _log(conn, sym, feat, "native", "ALL", "FETCHED" if rows else "EMPTY", len(rows))
        res[feat] = len(rows)
    # basis: derived from mark vs index at the same hourly observation (never assumed when either is missing)
    with db.connect() as conn:
        pairs = conn.execute(
            "SELECT m.observed_at, m.value, i.value FROM market_feature_observations m JOIN market_feature_observations i "
            "ON i.venue=m.venue AND i.venue_symbol=m.venue_symbol AND i.observed_at=m.observed_at AND "
            "i.feature='index_price' AND i.status='AVAILABLE' WHERE m.venue=? AND m.venue_symbol=? AND "
            "m.feature='mark_price' AND m.status='AVAILABLE'", (VENUE, sym)).fetchall()
    from app.market_data.store import SeriesId

    dsid = SeriesId(VENUE, sym, member["canonical_instrument_id"], member["asset_class"], member["product_type"],
                    "derived:mark_vs_index", "basis-bps-v1")
    for t, mark, index in pairs:
        if index:
            store.record_feature(dsid, "basis_bps", t, value=(mark - index) / index * 10_000.0)
    res["basis_bps"] = len(pairs)
    # open interest: the venue serves ~30 days of history only
    horizon = max(start, now_ms - 29 * DAY_MS)
    try:
        oi = api.get("/futures/data/openInterestHist", {"symbol": sym, "period": "1h", "startTime": horizon,
                                                         "endTime": min(end, now_ms), "limit": 500}) or []
        for r in oi:
            store.record_feature(sid, "open_interest", int(r["timestamp"]), value=float(r["sumOpenInterest"]))
        res["open_interest"] = len(oi)
    except ProviderUnavailable:
        raise
    except Exception as exc:
        res["open_interest"] = f"PROVIDER_FAILURE:{str(exc)[:80]}"
    if start < horizon:
        store.record_feature(sid, "open_interest", start, unavailable_reason=(
            f"NO_HISTORICAL_ENDPOINT:binance_usdm openInterestHist serves ~30 days; "
            f"[{start},{horizon}) unavailable"))
    for feat, reason in UNAVAILABLE_FEATURES:
        store.record_feature(sid, feat, start, unavailable_reason=reason)
    return res


def cmd_features(args, db, api):
    from app.market_data.store import MarketDataStore

    deep = load_deep(args.manifest)
    store = MarketDataStore(db)
    now = int(time.time() * 1000)
    for m in deep["members"]:
        try:
            r = features_member(db, api, m, deep["window_end_ms"], store=store, now_ms=now)
        except ProviderUnavailable as exc:
            print(json.dumps({"stopped": f"PROVIDER_UNAVAILABLE:{exc}"}))
            return
        print(json.dumps({m["venue_symbol"]: r, "requests": api.requests}), flush=True)
        if r.get("stopped"):
            return


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", required=True)
    ap.add_argument("--pace", type=float, default=0.2)
    ap.add_argument("--max-requests", type=int, default=None)
    ap.add_argument("--max-minutes", type=float, default=None)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("select")
    s.add_argument("--canonical-db", required=True)
    s.add_argument("--out", default=DEEP)
    s.add_argument("--size", type=int, default=35)
    s.add_argument("--liquidity-days", type=int, default=365)
    s.add_argument("--target-days", type=int, default=1826)
    for name in ("plan", "acquire", "derive", "features"):
        p = sub.add_parser(name)
        p.add_argument("--manifest", default=DEEP)
        if name == "acquire":
            p.add_argument("--symbols", default=None)
    args = ap.parse_args()
    db = _db(args.db)
    api = BinancePublic(pace_s=args.pace, max_requests=args.max_requests,
                        deadline=(time.time() + args.max_minutes * 60) if args.max_minutes else None)
    try:
        {"select": cmd_select, "plan": cmd_plan, "acquire": cmd_acquire, "derive": cmd_derive,
         "features": cmd_features}[args.cmd](args, db, api)
    except KeyboardInterrupt:
        print(json.dumps({"interrupted": True, "requests": api.requests}), flush=True)
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
