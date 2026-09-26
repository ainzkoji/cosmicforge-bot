#!/usr/bin/env python3
"""Acquire the multi-asset research datasets (Phase 4B/4C/4D/4E).

Crypto (per venue, venue-aware; never merged across venues):
    1. discover the venue's instruments (public API) -> venue_instruments
    2. select a BROAD research universe dynamically (listing age, liquidity,
       status) -> immutable universe manifest (100-150 symbols, 731+ days)
    3. BROAD dataset: 15m bars for every selected symbol
    4. DEEP dataset: NOT here. The canonical deep subset is a FROZEN
       historical-liquidity selection acquired in bounded, resumable monthly
       windows by scripts/acquire_crypto_deep_dataset.py (this script's former
       top-N-of-a-live-snapshot deep path is refused).
    5. funding / open interest where the venue publishes history; otherwise
       UNAVAILABLE observations with the venue's reason
    6. an immutable dataset manifest (row counts, series hashes, quality)

FX reference (provider-independent, not an execution venue):
    Dukascopy minute bid/ask for majors + crosses -> fx_reference_quotes

    python scripts/acquire_multi_asset_dataset.py --db data/research/multi_asset.db \\
        --venue binance_usdm --days 731 --broad 150
    python scripts/acquire_multi_asset_dataset.py --db ... --fx --fx-days 731
    python scripts/acquire_multi_asset_dataset.py --db ... --plan-only     # discovery + manifest only

Requires outbound HTTPS to the venue / provider hosts. Nothing is filled in:
gaps stay gaps and are reported by the quality section of the manifest.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(REPO, "backends", "bot-backend"), os.path.join(REPO, "backends", "shared")]

from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.market_data_schema import ensure_market_data_schema  # noqa: E402

FX_UNIVERSE = ("EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD", "EURGBP", "EURJPY", "EURCHF",
               "EURAUD", "EURCAD", "EURNZD", "GBPJPY", "GBPCHF", "GBPAUD", "GBPCAD", "GBPNZD", "AUDJPY", "AUDCHF",
               "AUDCAD", "AUDNZD", "CADJPY", "CHFJPY", "NZDJPY", "NZDCAD", "NZDCHF", "CADCHF")


def _discover(venue: str, get):
    from app.exchange import instruments as I
    from app.market_data import venue_history as vh

    if venue == "binance_usdm":
        info = get(f"{vh.BINANCE_FAPI}/fapi/v1/exchangeInfo", {})
        ins = [i for i in (I.parse_binance_symbol(s) for s in info.get("symbols", [])) if i]
        tick = get(f"{vh.BINANCE_FAPI}/fapi/v1/ticker/24hr", {})
        books = {b["symbol"]: b for b in get(f"{vh.BINANCE_FAPI}/fapi/v1/ticker/bookTicker", {})}
        stats = {}
        for t in tick:
            b = books.get(t["symbol"], {})
            bid, ask = float(b.get("bidPrice") or 0), float(b.get("askPrice") or 0)
            stats[t["symbol"]] = type("S", (), {"quote_volume_24h": float(t["quoteVolume"]),
                                                "spread_bps": ((ask - bid) / ((ask + bid) / 2) * 1e4) if bid and ask else None})()
        return ins, stats
    if venue == "bybit_linear":
        out, cursor = [], None
        while True:
            d = get(f"{vh.BYBIT_API}/v5/market/instruments-info", {"category": "linear", "limit": 1000, "cursor": cursor})
            res = d.get("result") or {}
            out += [i for i in (I.parse_bybit_instrument(r) for r in res.get("list") or []) if i]
            cursor = res.get("nextPageCursor")
            if not cursor:
                break
        t = get(f"{vh.BYBIT_API}/v5/market/tickers", {"category": "linear"})
        stats = {}
        for r in (t.get("result") or {}).get("list") or []:
            bid, ask = float(r.get("bid1Price") or 0), float(r.get("ask1Price") or 0)
            stats[r["symbol"]] = type("S", (), {"quote_volume_24h": float(r.get("turnover24h") or 0) or None,
                                                "spread_bps": ((ask - bid) / ((ask + bid) / 2) * 1e4) if bid and ask else None})()
        return out, stats
    raise SystemExit(f"unsupported venue {venue}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", required=True)
    ap.add_argument("--venue", default="binance_usdm", choices=["binance_usdm", "bybit_linear"])
    ap.add_argument("--days", type=int, default=731)
    ap.add_argument("--broad", type=int, default=150)
    ap.add_argument("--deep", type=int, default=0,
                    help="refused: use scripts/acquire_crypto_deep_dataset.py (frozen historical-liquidity subset)")
    ap.add_argument("--plan-only", action="store_true")
    ap.add_argument("--fx", action="store_true")
    ap.add_argument("--fx-days", type=int, default=731)
    args = ap.parse_args()
    if args.deep:
        raise SystemExit("--deep is refused: the deep subset must be a frozen historical-liquidity selection "
                         "(scripts/acquire_crypto_deep_dataset.py), not the top-N of a live snapshot")

    from app.exchange.instruments import InstrumentCatalog
    from app.market_data import fx_reference as fx
    from app.market_data import venue_history as vh
    from app.market_data.quality import check_series
    from app.market_data.resample import series_hash
    from app.market_data.store import MarketDataStore, SeriesId
    from app.market_data.universe import (SelectionCriteria, persist_dataset_manifest, persist_universe_manifest,
                                          select_universe)

    db = DB(path=args.db)
    ensure_market_data_schema(db)
    store = MarketDataStore(db)
    get = vh.requests_getter()
    now = int(time.time() * 1000)
    end = (now // 86_400_000) * 86_400_000 - 1
    start = end + 1 - args.days * 86_400_000

    if args.fx:
        prov = fx.DukascopyProvider(fetch=lambda url: _fetch_bytes(url))
        d1 = date.today() - timedelta(days=1)
        counts = fx.ingest(prov, store, FX_UNIVERSE, d1 - timedelta(days=args.fx_days), d1)
        print(json.dumps({"fx_rows": counts}))
        return 0

    instruments, stats = _discover(args.venue, get)
    InstrumentCatalog(db).upsert(args.venue, "REAL", instruments, now)
    broad = select_universe(instruments, stats, venue=args.venue, as_of_ms=now,
                            criteria=SelectionCriteria(target_size=args.broad, min_listing_age_days=args.days))
    persist_universe_manifest(db, broad)
    print(json.dumps({"broad": len(broad.selected), "shortfall": broad.shortfall,
                      "universe_manifest": broad.manifest_hash}))
    if args.plan_only:
        return 0

    by_sym = {i.venue_symbol: i for i in instruments}
    fetch = vh.FETCHERS[args.venue]
    manifest = {"venue": args.venue, "universe_manifest": broad.manifest_hash, "start_ms": start, "end_ms": end,
                "series": {}, "features": {}}
    for sym in broad.selected:
        ins = by_sym[sym]
        sid = SeriesId(args.venue, sym, ins.canonical_symbol, ins.asset_class, ins.product_type, "public_klines")
        tfs = {"15m": fetch["klines"](get, sym, "15m", start, end)}
        for tf, rows in tfs.items():
            store.write_candles(sid, tf, rows)
            q = check_series(rows, symbol=sym, timeframe=tf, listed_at_ms=ins.listed_at_ms)
            manifest["series"][f"{sym}:{tf}"] = {"rows": len(rows), "hash": series_hash(rows), "quality": q.to_dict()}
        for feat in ("funding", "open_interest"):
            if feat not in fetch:
                continue
            got = fetch[feat](get, sym, start, end)
            if isinstance(got, vh.FeatureUnavailable):
                store.record_feature(sid, got.feature, end, unavailable_reason=got.reason)
                manifest["features"][f"{sym}:{feat}"] = {"status": "UNAVAILABLE", "reason": got.reason}
                continue
            for name, ts, val in got:
                store.record_feature(sid, name, ts, value=val)
            manifest["features"][f"{sym}:{feat}"] = {"status": "AVAILABLE", "rows": len(got)}
    h = persist_dataset_manifest(db, role="RESEARCH_UNIVERSE", asset_class="CRYPTO", venue=args.venue,
                                 payload=manifest, created_at=now)
    print(json.dumps({"dataset_manifest": h, "series": len(manifest["series"])}))
    return 0


def _fetch_bytes(url: str):
    import requests

    r = requests.get(url, timeout=30)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.content


if __name__ == "__main__":
    raise SystemExit(main())
