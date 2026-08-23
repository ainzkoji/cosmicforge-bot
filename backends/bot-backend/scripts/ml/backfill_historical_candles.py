#!/usr/bin/env python3
"""
Backfill Historical Candles — CosmicForge Bot

Populates the `historical_candles` table with Binance OHLCV kline data
for the specified symbols and intervals, covering `--days` of history.

This is a prerequisite for the backtest engine (app/backtest/runner.py),
which reads from `historical_candles` via HistoricalDataProvider.

Design notes
------------
- Data source: Binance Futures public API (fapi.binance.com) — no API keys needed.
- Market type: 'crypto' (matches HistoricalDataProvider convention in codebase).
- Idempotent: uses INSERT OR IGNORE; safe to re-run at any time.
- Paginated: fetches 1000 candles per request, moves startTime forward.
- Verbose: prints per-symbol/interval progress and a final validation table.

Usage
-----
    cd backends/bot-backend

    python scripts/ml/backfill_historical_candles.py \\
        --symbols BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT \\
        --intervals 15m,1h \\
        --days 180

    # With explicit DB path:
    python scripts/ml/backfill_historical_candles.py \\
        --db-path ../shared/shared_lib/persistence/cosmicforge.db \\
        --symbols BTCUSDT,ETHUSDT \\
        --intervals 15m \\
        --days 90

    # Dry-run (fetch and parse but do not insert):
    python scripts/ml/backfill_historical_candles.py \\
        --symbols BTCUSDT \\
        --intervals 15m \\
        --days 3 \\
        --dry-run
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional

# ---------------------------------------------------------------------------
# Path setup — must run from backends/bot-backend/
# ---------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve().parent          # scripts/ml/
_BOT_ROOT   = _SCRIPT_DIR.parent.parent                # backends/bot-backend/
_SHARED     = _BOT_ROOT.parent / "shared"              # backends/shared/
for _p in (str(_BOT_ROOT), str(_SHARED)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_SYMBOLS   = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
DEFAULT_INTERVALS = ["15m", "1h"]
DEFAULT_DAYS      = 180
DEFAULT_MARKET    = "crypto"
DATA_SOURCE       = "binance"
BINANCE_API_BASE  = "https://fapi.binance.com"
FETCH_LIMIT       = 1000          # candles per request (Binance max = 1500; use 1000 for safety)
RATE_LIMIT_SLEEP  = 0.12          # seconds between requests (≈ 8 req/s, well inside limits)

# Interval → milliseconds
_INTERVAL_MS: dict[str, int] = {
    "1m":  60_000,
    "3m":  3 * 60_000,
    "5m":  5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h":  60 * 60_000,
    "2h":  2 * 60 * 60_000,
    "4h":  4 * 60 * 60_000,
    "6h":  6 * 60 * 60_000,
    "8h":  8 * 60 * 60_000,
    "12h": 12 * 60 * 60_000,
    "1d":  24 * 60 * 60_000,
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill historical_candles from Binance OHLCV data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--db-path",
        default=None,
        metavar="PATH",
        help=(
            "Path to cosmicforge.db. "
            "Defaults to ../shared/shared_lib/persistence/cosmicforge.db "
            "relative to the bot-backend root."
        ),
    )
    p.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        metavar="SYM1,SYM2",
        help=f"Comma-separated symbols. Default: {','.join(DEFAULT_SYMBOLS)}",
    )
    p.add_argument(
        "--intervals",
        default=",".join(DEFAULT_INTERVALS),
        metavar="15m,1h",
        help=f"Comma-separated intervals. Default: {','.join(DEFAULT_INTERVALS)}",
    )
    p.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        metavar="N",
        help=f"Days of history to backfill. Default: {DEFAULT_DAYS}",
    )
    p.add_argument(
        "--market-type",
        default=DEFAULT_MARKET,
        metavar="TYPE",
        help=(
            "Market type stored in historical_candles.market_type. "
            f"Default: {DEFAULT_MARKET}  (matches HistoricalDataProvider convention). "
            "The Binance Futures public API is used regardless of this value."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse candles but do NOT write to the database.",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-batch progress.",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def _resolve_db_path(arg: Optional[str]) -> Path:
    if arg:
        return Path(arg).resolve()
    # Default: relative to bot-backend root
    default = _BOT_ROOT.parent / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
    return default.resolve()


def _open_db(path: Path) -> sqlite3.Connection:
    if not path.exists():
        print(f"[FAIL] Database not found: {path}", file=sys.stderr)
        sys.exit(1)
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    # Verify schema
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "historical_candles" not in tables:
        print("[FAIL] historical_candles table not found in database.", file=sys.stderr)
        print("       Run the migration (migrations.py) to create the schema first.", file=sys.stderr)
        sys.exit(1)
    return conn


def _count_existing(conn: sqlite3.Connection, symbol: str, interval: str, market_type: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM historical_candles "
        "WHERE symbol=? AND interval=? AND market_type=? AND data_source=?",
        (symbol, interval, market_type, DATA_SOURCE),
    ).fetchone()
    return int(row[0])


# ---------------------------------------------------------------------------
# Binance fetch (pure requests — no API key needed for klines)
# ---------------------------------------------------------------------------

def _fetch_batch(symbol: str, interval: str, start_ms: int, end_ms: int) -> list:
    """Fetch one batch (up to FETCH_LIMIT candles) from Binance FAPI public endpoint."""
    import urllib.request, urllib.parse, json as _json

    params = {
        "symbol":    symbol,
        "interval":  interval,
        "startTime": str(start_ms),
        "endTime":   str(end_ms),
        "limit":     str(FETCH_LIMIT),
    }
    url = f"{BINANCE_API_BASE}/fapi/v1/klines?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status} for {url}")
            return _json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Binance fetch failed for {symbol} {interval}: {exc}") from exc


def _interval_ms(interval: str) -> int:
    ms = _INTERVAL_MS.get(interval)
    if ms is None:
        raise ValueError(f"Unknown interval: {interval!r}. Supported: {list(_INTERVAL_MS)}")
    return ms


# ---------------------------------------------------------------------------
# Candle parsing
# ---------------------------------------------------------------------------

def _parse_candle(
    kline: list,
    symbol: str,
    interval: str,
    market_type: str,
    fetched_at: str,
) -> tuple:
    """
    Parse a Binance kline into a DB insert tuple.

    Binance kline format:
      [0] open_time (ms)
      [1] open
      [2] high
      [3] low
      [4] close
      [5] volume
      [6] close_time (ms)
      [7] quote asset volume
      [8] number of trades
      ...
    """
    open_time    = int(kline[0])
    open_price   = float(kline[1])
    high         = float(kline[2])
    low          = float(kline[3])
    close        = float(kline[4])
    volume       = float(kline[5])
    quote_volume = float(kline[7]) if len(kline) > 7 else 0.0
    trades       = int(kline[8])   if len(kline) > 8 else 0
    return (
        symbol, interval, open_time,
        open_price, high, low, close, volume,
        quote_volume, trades,
        market_type, "USDT",
        DATA_SOURCE, fetched_at,
    )


_INSERT_SQL = """
    INSERT OR IGNORE INTO historical_candles (
        symbol, interval, open_time,
        open, high, low, close, volume,
        quote_volume, trades,
        market_type, quote_currency,
        data_source, fetched_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


# ---------------------------------------------------------------------------
# Core backfill logic
# ---------------------------------------------------------------------------

def backfill_symbol_interval(
    conn: sqlite3.Connection,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    market_type: str,
    dry_run: bool,
    verbose: bool,
) -> int:
    """
    Fetch all candles for (symbol, interval) in [start_ms, end_ms) and insert.
    Returns the count of candles fetched (not necessarily inserted — OR IGNORE).
    """
    ivl_ms = _interval_ms(interval)
    fetched_at = datetime.now(timezone.utc).isoformat()
    current_start = start_ms
    total_fetched = 0
    batch_num = 0

    while current_start < end_ms:
        batch = _fetch_batch(symbol, interval, current_start, end_ms)
        if not batch:
            break

        batch_num += 1
        total_fetched += len(batch)

        if not dry_run:
            rows = [_parse_candle(k, symbol, interval, market_type, fetched_at) for k in batch]
            conn.executemany(_INSERT_SQL, rows)
            conn.commit()

        last_open_ms = int(batch[-1][0])
        if verbose:
            first_dt = datetime.fromtimestamp(int(batch[0][0]) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            last_dt  = datetime.fromtimestamp(last_open_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            print(f"    batch {batch_num:3d}: {len(batch):5d} candles  {first_dt} -> {last_dt}")

        # Advance: next candle starts immediately after the last open + 1 interval
        next_start = last_open_ms + ivl_ms
        if next_start <= current_start:
            # Safety: should never happen, but prevent infinite loop
            break
        current_start = next_start

        # Polite rate limiting
        if current_start < end_ms:
            time.sleep(RATE_LIMIT_SLEEP)

    return total_fetched


# ---------------------------------------------------------------------------
# Post-backfill validation queries
# ---------------------------------------------------------------------------

def _print_summary(conn: sqlite3.Connection, symbols: List[str], intervals: List[str], market_type: str) -> None:
    print(f"\n{'=' * 74}")
    print(f"  BACKFILL COMPLETE")
    print(f"{'=' * 74}")

    total = conn.execute("SELECT COUNT(*) FROM historical_candles").fetchone()[0]
    print(f"\n  Total rows in historical_candles: {total:,}\n")

    print(f"  {'Symbol':<14} {'Interval':<10} {'Market':<10} {'Rows':>8}  {'First candle':<22} {'Last candle'}")
    print(f"  {'-'*14} {'-'*10} {'-'*10} {'-'*8}  {'-'*22} {'-'*22}")

    rows = conn.execute(
        """
        SELECT symbol, interval, market_type,
               COUNT(*) AS rows,
               MIN(open_time) AS first_ms,
               MAX(open_time) AS last_ms
        FROM historical_candles
        WHERE symbol IN ({ph}) AND market_type = ? AND data_source = ?
        GROUP BY symbol, interval, market_type
        ORDER BY symbol, interval
        """.format(ph=",".join("?" * len(symbols))),
        (*symbols, market_type, DATA_SOURCE),
    ).fetchall()

    for r in rows:
        first_dt = datetime.fromtimestamp(r["first_ms"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        last_dt  = datetime.fromtimestamp(r["last_ms"]  / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        print(f"  {r['symbol']:<14} {r['interval']:<10} {r['market_type']:<10} {r['rows']:>8,}  {first_dt:<22} {last_dt}")

    # Bad OHLCV rows
    bad = conn.execute(
        """
        SELECT COUNT(*) FROM historical_candles
        WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 OR volume < 0 OR high < low
        """
    ).fetchone()[0]
    print(f"\n  Bad OHLCV rows (open/high/low/close <= 0, volume < 0, high < low): {bad}")
    if bad > 0:
        print(f"  [!!] Bad OHLCV rows detected — investigate data quality.")
    else:
        print(f"  [OK] No bad OHLCV rows.")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()

    symbols   = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    intervals = [i.strip() for i in args.intervals.split(",") if i.strip()]

    if not symbols:
        print("[FAIL] No symbols specified.", file=sys.stderr)
        sys.exit(1)
    if not intervals:
        print("[FAIL] No intervals specified.", file=sys.stderr)
        sys.exit(1)
    for ivl in intervals:
        _interval_ms(ivl)  # validate early — raises ValueError if unknown

    db_path = _resolve_db_path(args.db_path)
    conn    = _open_db(db_path)

    now_ms    = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms  = int((datetime.now(timezone.utc) - timedelta(days=args.days)).timestamp() * 1000)

    start_dt  = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    end_dt    = datetime.fromtimestamp(now_ms   / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"\n  Database:    {db_path}")
    print(f"  Symbols:     {', '.join(symbols)}")
    print(f"  Intervals:   {', '.join(intervals)}")
    print(f"  Days:        {args.days}  ({start_dt} -> {end_dt})")
    print(f"  Market type: {args.market_type}")
    print(f"  Data source: {DATA_SOURCE}  ({BINANCE_API_BASE})")
    if args.dry_run:
        print("  DRY RUN — rows will NOT be written to the database.")
    print()

    grand_total = 0

    try:
        for symbol in symbols:
            for interval in intervals:
                existing = _count_existing(conn, symbol, interval, args.market_type)
                print(f"  [{symbol} {interval}]  existing rows: {existing:,} — fetching {args.days}d …")

                fetched = backfill_symbol_interval(
                    conn     = conn,
                    symbol   = symbol,
                    interval = interval,
                    start_ms = start_ms,
                    end_ms   = now_ms,
                    market_type = args.market_type,
                    dry_run  = args.dry_run,
                    verbose  = args.verbose,
                )
                after = _count_existing(conn, symbol, interval, args.market_type) if not args.dry_run else existing
                new   = after - existing
                grand_total += fetched
                print(f"    fetched: {fetched:,}   new rows inserted: {new:,}   total in DB: {after:,}")

    finally:
        if not args.dry_run:
            _print_summary(conn, symbols, intervals, args.market_type)
        conn.close()

    if args.dry_run:
        print(f"\n  DRY RUN complete. {grand_total:,} candles fetched but NOT inserted.")
    print("  Done.\n")


if __name__ == "__main__":
    main()
