"""Acquire real 1m market data and build a research dataset (Phase 14).

1m is the base resolution on purpose: 5m/15m/1h/4h are *derived* from it by a
deterministic rule rather than downloaded separately, because two
independently-fetched series disagree at their edges and nobody notices until a
result depends on it.

Nothing is filled in. Gaps are reported by the quality assessment and left as
gaps, because an imputed candle is indistinguishable from a real one once it is
in the file.

    python scripts/acquire_market_data.py --days 90
    python scripts/acquire_market_data.py --symbols BTCUSDT ETHUSDT --days 730
    python scripts/acquire_market_data.py --days 30 --out data/research

Resumable: an existing shard is not re-downloaded unless --refresh is given.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND = os.path.join(REPO_ROOT, "backends", "bot-backend")
SHARED = os.path.join(REPO_ROOT, "backends", "shared")

BINANCE_KLINES = "https://api.binance.com/api/v3/klines"
MAX_LIMIT = 1000
MINUTE_MS = 60_000
#: Binance allows a lot more than this; being slower than necessary is cheaper
#: than being rate-limited half way through a multi-year pull.
REQUEST_PAUSE_S = 0.12


def day_bounds(day: datetime) -> tuple[int, int]:
    start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    return (int(start.timestamp() * 1000),
            int((start + timedelta(days=1)).timestamp() * 1000) - 1)


def fetch_window(symbol: str, start_ms: int, end_ms: int,
                 *, session: requests.Session) -> list[list]:
    """Every 1m bar in [start, end]. Paged, ascending, no gap filling."""
    out: list[list] = []
    cursor = start_ms
    while cursor <= end_ms:
        response = session.get(
            BINANCE_KLINES,
            params={"symbol": symbol, "interval": "1m",
                    "startTime": cursor, "endTime": end_ms, "limit": MAX_LIMIT},
            timeout=30,
        )
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        out.extend(batch)
        last_open = int(batch[-1][0])
        if last_open <= cursor - MINUTE_MS:
            break
        cursor = last_open + MINUTE_MS
        time.sleep(REQUEST_PAUSE_S)
        if len(batch) < MAX_LIMIT:
            break
    return out


def shard_path(root: str, symbol: str, day: datetime) -> str:
    return os.path.join(root, symbol, f"{day:%Y-%m-%d}.json")


def acquire(symbol: str, days: int, root: str, *, refresh: bool,
            session: requests.Session) -> list[list]:
    os.makedirs(os.path.join(root, symbol), exist_ok=True)
    today = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    rows: list[list] = []
    downloaded = reused = 0

    # Yesterday backwards: today is still forming and would be a partial day.
    for offset in range(days, 0, -1):
        day = today - timedelta(days=offset)
        path = shard_path(root, symbol, day)
        if os.path.exists(path) and not refresh:
            with open(path, encoding="utf-8") as handle:
                rows.extend(json.load(handle))
            reused += 1
            continue
        start, end = day_bounds(day)
        batch = fetch_window(symbol, start, end, session=session)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(batch, handle)
        rows.extend(batch)
        downloaded += 1
        print(f"  {symbol} {day:%Y-%m-%d}  {len(batch):>5} bars")

    print(f"  {symbol}: {downloaded} day(s) downloaded, {reused} reused, "
          f"{len(rows):,} bars")
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"])
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--out", default=os.path.join("data", "research"))
    parser.add_argument("--dataset-id", default=None)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument(
        "--derive", nargs="*", default=["5m", "15m", "1h", "4h"],
        help="timeframes to derive deterministically from the 1m base",
    )
    args = parser.parse_args()

    for path in (BACKEND, SHARED):
        if path not in sys.path:
            sys.path.insert(0, path)

    from app.research.dataset import assess_quality, build_manifest, derive

    root = args.out if os.path.isabs(args.out) else os.path.join(REPO_ROOT, args.out)
    raw_root = os.path.join(root, "raw")
    os.makedirs(raw_root, exist_ok=True)
    dataset_id = args.dataset_id or (
        f"binance_1m_{'_'.join(s.lower() for s in args.symbols)}_{args.days}d"
    )

    print(f"dataset : {dataset_id}")
    print(f"root    : {root}")
    print(f"symbols : {', '.join(args.symbols)}  ({args.days} days)")

    session = requests.Session()
    series: dict[str, dict[str, list]] = {}
    for symbol in args.symbols:
        print(f"\n{symbol}")
        base = acquire(symbol, args.days, raw_root, refresh=args.refresh,
                       session=session)
        if not base:
            print(f"  {symbol}: no data returned; skipping")
            continue
        series[symbol] = {"1m": base}
        for timeframe in args.derive:
            derived = derive(base, timeframe)
            series[symbol][timeframe] = derived
            print(f"  derived {timeframe:>4}: {len(derived):>6} bars")

    if not series:
        print("\nno data acquired")
        return 1

    print("\nQUALITY")
    blocking = 0
    for symbol in series:
        for timeframe, rows in series[symbol].items():
            report = assess_quality(rows, symbol=symbol, timeframe=timeframe)
            flag = "ok " if report.is_usable else "BAD"
            print(f"  [{flag}] {symbol} {timeframe:<4} rows={report.rows:>7,} "
                  f"missing={report.missing_bars:<6} dup={report.duplicate_opens} "
                  f"ooo={report.out_of_order} bad_ohlc={report.ohlc_violations} "
                  f"completeness={report.completeness:.4%}")
            if not report.is_usable:
                blocking += 1

    def revision() -> str | None:
        import subprocess

        try:
            out = subprocess.run(["git", "rev-parse", "HEAD"],
                                 capture_output=True, text=True, timeout=10,
                                 cwd=REPO_ROOT)
            return (out.stdout or "").strip() or None
        except Exception:
            return None

    manifest = build_manifest(series, dataset_id=dataset_id,
                              code_revision=revision())
    manifest_path = os.path.join(root, f"{dataset_id}.manifest.json")
    manifest.write(manifest_path)

    print("\nPARTITIONS (chronological)")
    for part in manifest.partitions:
        print(f"  {part['name']:<14} {part['start'][:19]} -> {part['end'][:19]}"
              f"  rows={part['rows']:,}")

    print(f"\ndataset_hash : {manifest.dataset_hash}")
    print(f"provenance   : {manifest.provenance}")
    print(f"manifest     : {manifest_path}")
    if blocking:
        print(f"\n{blocking} series has structural faults and is not usable as is.")
        return 1
    print("\nThe final holdout is defined and must stay untouched until final "
          "evaluation.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
