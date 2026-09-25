#!/usr/bin/env python3
"""Acquire the CATI FX REFERENCE dataset from Dukascopy (no broker account needed).

Reference data is NOT execution data: every row is REFERENCE_MARKET_PRICE in
``fx_reference_quotes``; execution economics come from the venue product
(e.g. Bybit EURUSDUSDT) and are mapped separately
(``app.market_data.reference_mapping``).

Tiers
-----
* ``probe``  one monthly hourly BID file per candidate pair -> which pairs the
             provider actually serves (recorded, with the reason for each
             unavailable pair). Candidates = every G10 pair in market
             convention plus USD/EUR crosses to liquid non-G10 currencies;
             nothing is assumed available.
* ``broad``  hourly BID+ASK for every available pair (one file per month and
             side).
* ``deep``   minute BID+ASK (one file per UTC day and side) for the pairs a
             connected venue lists as an FX execution instrument (discovered at
             run time from Bybit V5 + BingX swap), plus the G10 USD majors.
             15m bid/ask bars are then DERIVED deterministically
             (``fx_reference.resample_quotes``; incomplete windows dropped).
* ``validate`` coverage, gap markers, spread sanity and triangular
             consistency (EURSEK ~= EURUSD x USDSEK) as a price-scale check.

Throttled (Dukascopy rate-limits and blocks bursts): fixed pacing, backoff
on 429/timeouts, resumable through ``fx_reference_ingest_log`` (a period
already FETCHED/NO_FILE/EMPTY is never re-downloaded). Nothing is
interpolated; weekend padding bars (flat, zero volume) are dropped so a
closed market is a gap.

    python scripts/acquire_fx_reference_dataset.py --db <research.db> probe
    python scripts/acquire_fx_reference_dataset.py --db <research.db> broad --months 25
    python scripts/acquire_fx_reference_dataset.py --db <research.db> deep --days 731
    python scripts/acquire_fx_reference_dataset.py --db <research.db> validate --out report.json
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(REPO, "backends", "bot-backend"), os.path.join(REPO, "backends", "shared")]

PROVIDER = "dukascopy"
G10 = ("EUR", "GBP", "AUD", "NZD", "USD", "CAD", "CHF", "JPY")  # market-convention priority order
#: liquid non-G10 currencies quoted with the standard 1e5 point (JPY-style large quotes are excluded
#: until their price scale is validated -- see UNVALIDATED_PRICE_SCALE)
NON_G10_STANDARD_POINT = ("SEK", "NOK", "DKK", "PLN", "SGD", "HKD", "CNH", "ZAR", "MXN", "TRY", "ILS")
UNVALIDATED_SCALE = ("HUF", "CZK", "RUB", "THB", "INR", "KRW")
PACE_S = 0.7
USD_MAJORS = ("EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD")


def candidate_pairs():
    out = []
    for i, a in enumerate(G10):
        for b in G10[i + 1:]:
            out.append(a + b)
    for c in NON_G10_STANDARD_POINT:
        out += ["USD" + c, "EUR" + c]
    return sorted(set(out))


class Fetcher:
    def __init__(self):
        import requests

        self.s = requests.Session()
        self.s.headers["User-Agent"] = "Mozilla/5.0 (cosmicforge-research)"
        self.last = 0.0
        self.requests = 0

    def get(self, url):
        """bytes | None (404 = provider has no file) ; raises after persistent failure."""
        import requests

        for attempt in range(8):
            wait = PACE_S - (time.time() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()
            self.requests += 1
            try:
                r = self.s.get(url, timeout=25)
            except requests.RequestException:
                time.sleep(min(30 * (attempt + 1), 120))
                continue
            if r.status_code == 404:
                return None
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(min(30 * (attempt + 1), 120))
                continue
            r.raise_for_status()
            return r.content
        raise RuntimeError(f"persistent failure: {url}")


def _db(path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.market_data_schema import ensure_market_data_schema

    db = DB(path=path)
    ensure_market_data_schema(db)
    return db


def _logged(conn, pair, tf, period, side):
    r = conn.execute("SELECT status FROM fx_reference_ingest_log WHERE provider=? AND pair=? AND timeframe=? AND "
                     "period=? AND side=?", (PROVIDER, pair, tf, period, side)).fetchone()
    return r[0] if r else None


def _log(conn, pair, tf, period, side, status, rows=0, reason=None):
    conn.execute("INSERT OR REPLACE INTO fx_reference_ingest_log (provider, pair, timeframe, period, side, status, "
                 "rows, reason, recorded_at) VALUES (?,?,?,?,?,?,?,?,?)",
                 (PROVIDER, pair, tf, period, side, status, rows, reason, int(time.time() * 1000)))


def cmd_probe(args, db, f):
    from app.market_data import fx_reference as fx

    y, m = args.probe_year, args.probe_month
    res = {}
    for pair in candidate_pairs():
        url = fx.DUKASCOPY_HOUR_URL.format(pair=pair, y=y, m=m - 1, side="BID")
        body = f.get(url)
        if body is None:
            res[pair] = {"available": False, "reason": "PROVIDER_HAS_NO_FILE"}
        else:
            rows = fx.drop_flat_closed_bars(fx.decode_hour_file(body, year=y, month=m, point=fx.point_for(pair)))
            res[pair] = {"available": bool(rows), "reason": None if rows else "PROVIDER_FILE_EMPTY",
                         "sample_close": rows[-1]["close"] if rows else None}
        print(json.dumps({pair: res[pair]}), flush=True)
    for c in UNVALIDATED_SCALE:
        for p in ("USD" + c, "EUR" + c):
            res[p] = {"available": False, "reason": "UNVALIDATED_PRICE_SCALE"}
    with db.connect() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS fx_reference_availability (provider TEXT, pair TEXT, available INTEGER,"
                     " reason TEXT, probed_period TEXT, recorded_at INTEGER, PRIMARY KEY (provider, pair))")
        for p, v in res.items():
            conn.execute("INSERT OR REPLACE INTO fx_reference_availability VALUES (?,?,?,?,?,?)",
                         (PROVIDER, p, int(v["available"]), v["reason"], f"{y}-{m:02d}", int(time.time() * 1000)))
    print(json.dumps({"available": sorted(p for p, v in res.items() if v["available"]),
                      "unavailable": {p: v["reason"] for p, v in res.items() if not v["available"]}}))


def _available(db):
    with db.connect() as conn:
        return [r[0] for r in conn.execute("SELECT pair FROM fx_reference_availability WHERE provider=? AND "
                                           "available=1 ORDER BY pair", (PROVIDER,))]


def cmd_broad(args, db, f):
    from app.market_data import fx_reference as fx
    from app.market_data.store import MarketDataStore

    store = MarketDataStore(db)
    today = date.today()
    months = []
    y, m = today.year, today.month
    for _ in range(args.months):
        m -= 1
        if m == 0:
            y, m = y - 1, 12
        months.append((y, m))
    for pair in _available(db):
        base, quote = fx.split_pair(pair)
        n = 0
        for (y, m) in sorted(months):
            period = f"{y}-{m:02d}"
            sides = {}
            with db.connect() as conn:
                done = {s: _logged(conn, pair, "1h", period, s) for s in ("BID", "ASK")}
            if all(v in ("FETCHED", "NO_FILE", "EMPTY") for v in done.values()):
                continue
            for side in ("BID", "ASK"):
                body = f.get(fx.DUKASCOPY_HOUR_URL.format(pair=pair, y=y, m=m - 1, side=side))
                sides[side] = [] if body is None else fx.drop_flat_closed_bars(
                    fx.decode_hour_file(body, year=y, month=m, point=fx.point_for(pair)))
                with db.connect() as conn:
                    _log(conn, pair, "1h", period, side, "NO_FILE" if body is None else
                         ("FETCHED" if sides[side] else "EMPTY"), len(sides[side]))
            rows = fx.merge_sides(sides["BID"], sides["ASK"], pair=pair)
            n += store.write_fx_quotes(PROVIDER, pair, base, quote, "1h", rows, source_version="bi5-candles-hour-1:v1")
        print(json.dumps({"pair": pair, "tf": "1h", "rows_written": n, "requests": f.requests}), flush=True)


def execution_mapped_pairs():
    """FX pairs a connected venue lists as an FX execution instrument, discovered NOW."""
    import requests

    from app.exchange import instruments as I

    pairs = set()
    try:
        rows, cursor = [], ""
        while True:
            d = requests.get("https://api.bybit.com/v5/market/instruments-info",
                             params={"category": "linear", "limit": 1000, **({"cursor": cursor} if cursor else {})},
                             timeout=30).json()
            rows += d["result"]["list"]
            cursor = d["result"].get("nextPageCursor") or ""
            if not cursor:
                break
        for r in rows:
            ins = I.parse_bybit_instrument(r)
            if ins and ins.asset_class == I.FX:
                pairs.add(ins.base_currency + ins.quote_currency)
    except Exception as exc:  # discovery failure: fall back to majors only, recorded
        print(json.dumps({"bybit_discovery_error": str(exc)[:200]}))
    try:
        d = requests.get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts", timeout=30).json()
        for c in d.get("data") or []:
            ins = I.parse_bingx_contract(c)
            if ins and ins.asset_class == I.FX:
                pairs.add(ins.base_currency + ins.quote_currency)
    except Exception as exc:
        print(json.dumps({"bingx_discovery_error": str(exc)[:200]}))
    return sorted(pairs)


def cmd_deep(args, db, f):
    from app.market_data import fx_reference as fx
    from app.market_data.store import MarketDataStore

    store = MarketDataStore(db)
    avail = set(_available(db))
    pairs = sorted((set(execution_mapped_pairs()) | set(USD_MAJORS)) & avail) if not args.pairs else \
        [p.strip().upper() for p in args.pairs.split(",")]
    print(json.dumps({"deep_pairs": pairs}), flush=True)
    end = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
    start = end - timedelta(days=args.days - 1)
    for pair in pairs:
        base, quote = fx.split_pair(pair)
        d, n = start, 0
        while d <= end:
            period = d.isoformat()
            if d.weekday() == 5:  # Saturday: the FX market is closed all day -> explicit gap marker, no request
                with db.connect() as conn:
                    for side in ("BID", "ASK"):
                        if not _logged(conn, pair, "1m", period, side):
                            _log(conn, pair, "1m", period, side, "EMPTY", 0, "MARKET_CLOSED_SATURDAY")
                d += timedelta(days=1)
                continue
            with db.connect() as conn:
                done = {s: _logged(conn, pair, "1m", period, s) for s in ("BID", "ASK")}
            if all(v in ("FETCHED", "NO_FILE", "EMPTY") for v in done.values()):
                d += timedelta(days=1)
                continue
            day_start = int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)
            sides = {}
            for side in ("BID", "ASK"):
                body = f.get(fx.DUKASCOPY_URL.format(pair=pair, y=d.year, m=d.month - 1, d=d.day, side=side))
                sides[side] = [] if body is None else fx.drop_flat_closed_bars(
                    fx.decode_bi5_candles(body, day_start_ms=day_start, point=fx.point_for(pair)))
                with db.connect() as conn:
                    _log(conn, pair, "1m", period, side, "NO_FILE" if body is None else
                         ("FETCHED" if sides[side] else "EMPTY"), len(sides[side]))
            rows = fx.merge_sides(sides["BID"], sides["ASK"], pair=pair)
            n += store.write_fx_quotes(PROVIDER, pair, base, quote, "1m", rows, source_version="bi5-candles-min-1:v1")
            d += timedelta(days=1)
        print(json.dumps({"pair": pair, "tf": "1m", "rows_written": n, "requests": f.requests}), flush=True)
        derive(db, pair)


def derive(db, pair):
    """Deterministic 15m bid/ask bars from the 1m reference quotes (incomplete windows dropped)."""
    from app.market_data import fx_reference as fx
    from app.market_data.store import MarketDataStore

    store = MarketDataStore(db)
    base, quote = fx.split_pair(pair)
    with db.connect() as conn:
        conn.row_factory = sqlite3.Row
        q = [dict(r) for r in conn.execute("SELECT open_time, bid_open, bid_high, bid_low, bid_close, ask_open, "
                                           "ask_high, ask_low, ask_close, volume FROM fx_reference_quotes WHERE "
                                           "provider=? AND pair=? AND timeframe='1m' ORDER BY open_time",
                                           (PROVIDER, pair))]
    # 1h comes from the provider's own hourly files (broad tier); 15m is derived here only.
    rows = fx.resample_quotes(q, 15)
    out = {"15m": store.write_fx_quotes(PROVIDER, pair, base, quote, "15m", rows,
                                        source_version="derived-from-1m:15:v1")}
    print(json.dumps({"pair": pair, "derived": out}), flush=True)


def cmd_validate(args, db, f):
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    rep = {"provider": PROVIDER, "pairs": {}}
    for pair, tf, n, lo, hi, nulls, neg in conn.execute(
            "SELECT pair, timeframe, COUNT(*), MIN(open_time), MAX(open_time), "
            "SUM(CASE WHEN bid_close IS NULL OR ask_close IS NULL THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN spread_close < 0 THEN 1 ELSE 0 END) FROM fx_reference_quotes WHERE provider=? "
            "GROUP BY pair, timeframe", (PROVIDER,)):
        rep["pairs"].setdefault(pair, {})[tf] = {"rows": n, "first_ms": lo, "last_ms": hi,
                                                 "one_side_missing": nulls, "negative_spread": neg}
    gaps = {}
    for pair, tf, status, cnt in conn.execute("SELECT pair, timeframe, status, COUNT(*) FROM fx_reference_ingest_log "
                                              "WHERE provider=? GROUP BY 1,2,3", (PROVIDER,)):
        gaps.setdefault(pair, {}).setdefault(tf, {})[status] = cnt
    rep["ingest_log"] = gaps
    # triangular consistency on the latest common 1h bar: X/Y ~= X/USD * USD/Y (a price-scale check)
    def last_mid(p):
        r = conn.execute("SELECT open_time, mid_close FROM fx_reference_quotes WHERE provider=? AND pair=? AND "
                         "timeframe='1h' AND mid_close IS NOT NULL ORDER BY open_time DESC LIMIT 1",
                         (PROVIDER, p)).fetchone()
        return r
    usd = {}
    for p in rep["pairs"]:
        r = last_mid(p)
        if not r:
            continue
        if p.endswith("USD"):
            usd[p[:3]] = (r[0], r[1])            # value of 1 unit in USD
        elif p.startswith("USD"):
            usd[p[3:]] = (r[0], 1.0 / r[1])
    tri = {}
    for p in rep["pairs"]:
        if "USD" in p:
            continue
        a, b = p[:3], p[3:]
        r = last_mid(p)
        if r and a in usd and b in usd and usd[a][0] == usd[b][0] == r[0]:
            implied = usd[a][1] / usd[b][1]
            tri[p] = {"direct": r[1], "implied": implied, "rel_diff": abs(r[1] - implied) / implied}
    rep["triangular_consistency"] = tri
    rep["triangular_max_rel_diff"] = max((v["rel_diff"] for v in tri.values()), default=None)
    conn.close()
    text = json.dumps(rep, indent=2, sort_keys=True)
    if args.out:
        open(args.out, "w", encoding="utf-8").write(text)
    print(json.dumps({"pairs": len(rep["pairs"]), "triangular_checked": len(tri),
                      "triangular_max_rel_diff": rep["triangular_max_rel_diff"]}, indent=2))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("probe")
    p.add_argument("--probe-year", type=int, default=2026)
    p.add_argument("--probe-month", type=int, default=8)
    b = sub.add_parser("broad")
    b.add_argument("--months", type=int, default=25)
    d = sub.add_parser("deep")
    d.add_argument("--days", type=int, default=731)
    d.add_argument("--end", default=None)
    d.add_argument("--pairs", default=None)
    v = sub.add_parser("validate")
    v.add_argument("--out", default=None)
    args = ap.parse_args()
    db = _db(args.db)
    f = Fetcher()
    {"probe": cmd_probe, "broad": cmd_broad, "deep": cmd_deep, "validate": cmd_validate}[args.cmd](args, db, f)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
