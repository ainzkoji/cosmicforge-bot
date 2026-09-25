#!/usr/bin/env python3
"""Build (and optionally backfill) the frozen CATI crypto research/certification universe.

Deterministic, dynamic, reproducible:

1. discover every Binance USD-M instrument (public ``exchangeInfo``) through the
   canonical parser (``app.exchange.instruments``) -- no symbol list anywhere;
2. static filters: CRYPTO asset class, PERPETUAL, USDT-settled, TRADING,
   non-stablecoin base, listed on or before the window start;
3. liquidity = median daily quote volume over the last 30 CLOSED days ending
   at the window end (historical, so the ranking cannot drift with today's
   ticker);
4. rank (liquidity desc, symbol asc), keep ``--target`` (default 150),
   require ``--min`` (default 100);
5. write the frozen manifest (``universe_hash`` over its identity fields);
6. ``--backfill``: 15m candles for every member over the window into
   ``historical_candles`` via the EXISTING idempotent importer
   (``scripts/ml/backfill_historical_candles.py``: INSERT OR IGNORE), so the
   Section 22 certification pipeline reads them unchanged;
7. ``--validate``: per-symbol coverage/gap report from the DB (read-only).

Survivorship: discovery lists instruments trading NOW, so symbols delisted
inside the window are absent. The manifest records this bias explicitly.

    python scripts/build_crypto_research_universe.py --manifest docs/research/cati_crypto_universe_binance_v1.json
    python scripts/build_crypto_research_universe.py --manifest ... --backfill --db <cosmicforge.db>
    python scripts/build_crypto_research_universe.py --manifest ... --validate --db <cosmicforge.db>
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BOT = os.path.join(REPO, "backends", "bot-backend")
sys.path[:0] = [BOT, os.path.join(REPO, "backends", "shared")]

DAY_MS = 86_400_000
BAR_MS = {"15m": 900_000, "1h": 3_600_000, "5m": 300_000, "1m": 60_000}
#: default window: 731 UTC days ending 2026-09-24T00:00Z (exclusive)
DEFAULT_END = "2026-09-24"
DEFAULT_DAYS = 731


def _utc_ms(day: str) -> int:
    return int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)


def _getter():
    import requests

    s = requests.Session()
    s.headers["User-Agent"] = "cosmicforge-research"

    def get(url, params):
        for attempt in range(5):
            r = s.get(url, params=params, timeout=30)
            if r.status_code in (418, 429):
                time.sleep(2 ** attempt * 5)
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"rate limited: {url}")

    return get


def build_manifest(args) -> dict:
    from app.exchange.instruments import parse_binance_symbol
    from app.market_data import venue_history as vh
    from app.market_data.universe import (CERTIFICATION, SelectionCriteria, build_frozen_universe_manifest,
                                          exclude_stable_bases, historical_liquidity, select_universe)

    get = _getter()
    end_ms = _utc_ms(args.end)
    start_ms = end_ms - args.days * DAY_MS
    info = get(f"{vh.BINANCE_FAPI}/fapi/v1/exchangeInfo", {})
    instruments = [i for i in (parse_binance_symbol(s) for s in info.get("symbols", [])) if i]
    candidates, extra = exclude_stable_bases(instruments)
    crit = SelectionCriteria(asset_class="CRYPTO", product_types=("PERPETUAL",), quote_assets=("USDT",),
                             min_listing_age_days=args.days, min_quote_volume_24h=args.min_volume,
                             max_spread_bps=None, target_size=args.target, min_size=args.min)
    liquidity = {}
    for ins in candidates:
        if not (ins.asset_class == "CRYPTO" and ins.product_type == "PERPETUAL" and ins.settlement_asset == "USDT"
                and ins.api_tradable and ins.listed_at_ms is not None
                and (end_ms - ins.listed_at_ms) >= args.days * DAY_MS):
            continue  # select_universe records the precise exclusion reason
        kl = get(f"{vh.BINANCE_FAPI}/fapi/v1/klines", {"symbol": ins.venue_symbol, "interval": "1d",
                                                       "startTime": end_ms - 31 * DAY_MS, "endTime": end_ms - 1,
                                                       "limit": 40})
        liquidity[ins.venue_symbol] = historical_liquidity(kl, lookback_days=30, end_ms=end_ms - 1)
        time.sleep(0.05)
    sel = select_universe(candidates, liquidity, venue="binance_usdm", as_of_ms=end_ms, criteria=crit,
                          role=CERTIFICATION)
    manifest = build_frozen_universe_manifest(
        sel, instruments, window_start_ms=start_ms, window_end_ms=end_ms, timeframes=["15m"],
        source_provider="binance", generated_at=datetime.now(timezone.utc).isoformat(), extra_excluded=extra,
        liquidity=liquidity, role=CERTIFICATION)
    manifest["liquidity_rule"] = {"statistic": "median_daily_quote_volume", "lookback_closed_days": 30,
                                  "min_usd": args.min_volume, "source": "fapi/v1/klines interval=1d"}
    manifest["survivorship_bias"] = ("PRESENT: members are drawn from instruments TRADING at generation time; "
                                     "symbols delisted inside the window are absent")
    manifest["discovered_instruments"] = len(instruments)
    return manifest


def _load_importer():
    path = os.path.join(BOT, "scripts", "ml", "backfill_historical_candles.py")
    spec = importlib.util.spec_from_file_location("cf_backfill", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _coverage(conn, sym: str, tf: str, start: int, end: int) -> dict:
    rows = [r[0] for r in conn.execute(
        "SELECT open_time FROM historical_candles WHERE symbol=? AND interval=? AND market_type='crypto' "
        "AND data_source='binance' AND open_time>=? AND open_time<? ORDER BY open_time", (sym, tf, start, end))]
    step = BAR_MS[tf]
    expected = (end - start) // step
    gaps, prev = [], None
    for t in rows:
        if prev is not None and t - prev > step:
            gaps.append([prev + step, t, (t - prev) // step - 1])
        prev = t
    return {"rows": len(rows), "expected": expected, "completeness": round(len(rows) / expected, 6) if expected else 0,
            "first_ms": rows[0] if rows else None, "last_ms": rows[-1] if rows else None,
            "gap_count": len(gaps), "missing_bars_inside": sum(g[2] for g in gaps), "largest_gaps": sorted(
                gaps, key=lambda g: -g[2])[:3]}


def backfill(args, manifest: dict) -> dict:
    imp = _load_importer()
    conn = sqlite3.connect(args.db, timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    start, end = manifest["window_start_ms"], manifest["window_end_ms"]
    out = {}
    order = list(manifest["selected_symbols"])
    if getattr(args, "reverse", False):  # a second worker from the other end (idempotent: completeness skip + OR IGNORE)
        order.reverse()
    for i, sym in enumerate(order, 1):
        for tf in manifest["timeframes"]:
            cov = _coverage(conn, sym, tf, start, end)
            if cov["rows"] >= cov["expected"]:
                out[f"{sym}:{tf}"] = {"status": "ALREADY_COMPLETE", **cov}
                continue
            for attempt in range(3):
                try:
                    n = imp.backfill_symbol_interval(conn, sym, tf, start, end - 1, "crypto", False, False)
                    break
                except Exception as exc:  # network: retry, then record
                    n = None
                    err = str(exc)[:200]
                    time.sleep(10 * (attempt + 1))
            cov = _coverage(conn, sym, tf, start, end)
            out[f"{sym}:{tf}"] = {"status": "FETCHED" if n is not None else "FAILED", "fetched": n, **cov,
                                  **({} if n is not None else {"error": err})}
            print(json.dumps({"i": i, "symbol": sym, "tf": tf, **{k: out[f'{sym}:{tf}'][k] for k in
                                                                    ("status", "rows", "expected")}}), flush=True)
    conn.close()
    return out


def validate(args, manifest: dict) -> dict:
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    start, end = manifest["window_start_ms"], manifest["window_end_ms"]
    per = {f"{s}:{tf}": _coverage(conn, s, tf, start, end) for s in manifest["selected_symbols"]
           for tf in manifest["timeframes"]}
    conn.close()
    complete = [k for k, v in per.items() if v["completeness"] >= args.min_completeness]
    return {"universe_hash": manifest["universe_hash"], "series": len(per), "qualified": len(complete),
            "min_completeness": args.min_completeness,
            "not_qualified": {k: v["completeness"] for k, v in per.items() if k not in complete},
            "total_rows": sum(v["rows"] for v in per.values()),
            "series_with_internal_gaps": sum(1 for v in per.values() if v["gap_count"]),
            "per_series": per}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", required=True, help="frozen manifest path (written if absent)")
    ap.add_argument("--end", default=DEFAULT_END, help="window end (UTC date, exclusive)")
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS)
    ap.add_argument("--target", type=int, default=150)
    ap.add_argument("--min", type=int, default=100)
    ap.add_argument("--min-volume", type=float, default=5_000_000.0)
    ap.add_argument("--db", help="candle database (historical_candles)")
    ap.add_argument("--backfill", action="store_true")
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--reverse", action="store_true", help="backfill members in reverse order (parallel worker)")
    ap.add_argument("--min-completeness", type=float, default=0.999)
    ap.add_argument("--report", help="write backfill/validation JSON here")
    args = ap.parse_args()

    from app.market_data.universe import load_frozen_universe

    if os.path.exists(args.manifest):
        manifest = load_frozen_universe(args.manifest)  # frozen: never rebuilt in place
        print(json.dumps({"manifest": args.manifest, "status": "LOADED_FROZEN",
                          "universe_hash": manifest["universe_hash"], "members": len(manifest["selected_symbols"])}))
    else:
        manifest = build_manifest(args)
        os.makedirs(os.path.dirname(os.path.abspath(args.manifest)), exist_ok=True)
        with open(args.manifest, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2, sort_keys=True)
        print(json.dumps({"manifest": args.manifest, "status": "BUILT", "universe_hash": manifest["universe_hash"],
                          "members": len(manifest["selected_symbols"]), "shortfall": manifest["shortfall"]}))
    result = {}
    if args.backfill:
        if not args.db:
            raise SystemExit("--backfill needs --db")
        result["backfill"] = backfill(args, manifest)
    if args.validate:
        if not args.db:
            raise SystemExit("--validate needs --db")
        result["validation"] = validate(args, manifest)
        print(json.dumps({k: v for k, v in result["validation"].items() if k != "per_series"}, indent=2))
    if args.report and result:
        with open(args.report, "w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2, sort_keys=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
