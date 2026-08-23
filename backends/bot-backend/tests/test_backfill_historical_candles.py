"""
Tests for scripts/ml/backfill_historical_candles.py

All Binance network calls are mocked — tests run offline, no API keys required.
The DB tests use an in-memory SQLite instance with the historical_candles schema.
"""
from __future__ import annotations

import importlib.util
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Import the module under test
# ---------------------------------------------------------------------------

_SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts" / "ml" / "backfill_historical_candles.py"
)

spec = importlib.util.spec_from_file_location("backfill_historical_candles", _SCRIPT)
bfh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bfh)


# ---------------------------------------------------------------------------
# In-memory DB fixture
# ---------------------------------------------------------------------------

_HISTORICAL_CANDLES_DDL = """
CREATE TABLE IF NOT EXISTS historical_candles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_time INTEGER NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    quote_volume REAL,
    trades INTEGER,
    market_type TEXT NOT NULL DEFAULT 'crypto',
    base_currency TEXT,
    quote_currency TEXT NOT NULL DEFAULT 'USDT',
    data_source TEXT NOT NULL,
    data_version TEXT,
    fetched_at TEXT,
    UNIQUE(symbol, interval, open_time, data_source, market_type)
)
"""


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(_HISTORICAL_CANDLES_DDL)
    conn.commit()
    return conn


def _count(conn: sqlite3.Connection, symbol: str = None, interval: str = None) -> int:
    if symbol and interval:
        return conn.execute(
            "SELECT COUNT(*) FROM historical_candles WHERE symbol=? AND interval=?",
            (symbol, interval),
        ).fetchone()[0]
    return conn.execute("SELECT COUNT(*) FROM historical_candles").fetchone()[0]


# ---------------------------------------------------------------------------
# Sample kline factory
# ---------------------------------------------------------------------------

def _make_klines(n: int, base_open_ms: int = 1_700_000_000_000, interval_ms: int = 60_000) -> list:
    """
    Return n synthetic Binance-format klines starting at base_open_ms.
    Prices are plausible (non-zero, high >= low).
    """
    klines = []
    for i in range(n):
        open_time = base_open_ms + i * interval_ms
        close_time = open_time + interval_ms - 1
        klines.append([
            open_time,      # [0] open_time
            "50000.0",      # [1] open
            "50100.0",      # [2] high
            "49900.0",      # [3] low
            "50050.0",      # [4] close
            "100.5",        # [5] volume
            close_time,     # [6] close_time
            "5025000.0",    # [7] quote volume
            1234,           # [8] trade count
            "50.0",         # [9] taker buy base volume
            "2512500.0",    # [10] taker buy quote volume
            "0",            # [11] unused
        ])
    return klines


# ---------------------------------------------------------------------------
# Test 1 — _interval_ms: known intervals return correct millisecond values
# ---------------------------------------------------------------------------

def test_interval_ms_known_intervals():
    """_interval_ms must return the correct millisecond duration for standard intervals."""
    cases = {
        "1m":  60_000,
        "5m":  5 * 60_000,
        "15m": 15 * 60_000,
        "1h":  60 * 60_000,
        "4h":  4 * 60 * 60_000,
        "1d":  24 * 60 * 60_000,
    }
    for interval, expected in cases.items():
        result = bfh._interval_ms(interval)
        assert result == expected, (
            f"_interval_ms({interval!r}) returned {result}, expected {expected}"
        )


# ---------------------------------------------------------------------------
# Test 2 — _interval_ms: unknown interval raises ValueError
# ---------------------------------------------------------------------------

def test_interval_ms_unknown_interval_raises():
    """An unrecognised interval string must raise ValueError, not silently return None."""
    with pytest.raises(ValueError, match="Unknown interval"):
        bfh._interval_ms("99x")


# ---------------------------------------------------------------------------
# Test 3 — _parse_candle: fields extracted from kline correctly
# ---------------------------------------------------------------------------

def test_parse_candle_returns_correct_tuple():
    """_parse_candle must map kline indices to the correct DB column positions."""
    kline = _make_klines(1)[0]
    symbol     = "BTCUSDT"
    interval   = "15m"
    market_type = "crypto"
    fetched_at  = "2026-01-01T00:00:00+00:00"

    row = bfh._parse_candle(kline, symbol, interval, market_type, fetched_at)

    # row layout: symbol, interval, open_time, open, high, low, close, volume,
    #             quote_volume, trades, market_type, quote_currency, data_source, fetched_at
    assert row[0]  == symbol
    assert row[1]  == interval
    assert row[2]  == int(kline[0])      # open_time (ms integer)
    assert row[3]  == float(kline[1])    # open
    assert row[4]  == float(kline[2])    # high
    assert row[5]  == float(kline[3])    # low
    assert row[6]  == float(kline[4])    # close
    assert row[7]  == float(kline[5])    # volume
    assert row[8]  == float(kline[7])    # quote_volume
    assert row[9]  == int(kline[8])      # trades
    assert row[10] == market_type
    assert row[11] == "USDT"
    assert row[12] == bfh.DATA_SOURCE    # 'binance'
    assert row[13] == fetched_at
    # Sanity: high >= low
    assert row[4] >= row[5]


# ---------------------------------------------------------------------------
# Test 4 — backfill_symbol_interval: inserts candles into DB via mock fetch
# ---------------------------------------------------------------------------

def test_backfill_inserts_candles_into_db():
    """
    Core integration: mocked _fetch_batch returns 50 candles on first call and
    an empty list on the second (signals end of range). After backfill,
    historical_candles must have exactly 50 rows for the target symbol/interval.
    """
    conn     = _make_db()
    symbol   = "BTCUSDT"
    interval = "15m"
    ivl_ms   = bfh._interval_ms(interval)
    base_ms  = 1_700_000_000_000
    n        = 50
    klines   = _make_klines(n, base_ms, ivl_ms)

    call_count = {"n": 0}

    def fake_fetch(sym, ivl, start, end):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return klines
        return []  # end of range

    with patch.object(bfh, "_fetch_batch", side_effect=fake_fetch):
        fetched = bfh.backfill_symbol_interval(
            conn=conn,
            symbol=symbol,
            interval=interval,
            start_ms=base_ms,
            end_ms=base_ms + (n + 10) * ivl_ms,  # room for 60 candles
            market_type="crypto",
            dry_run=False,
            verbose=False,
        )

    assert fetched == n, f"Expected {n} fetched, got {fetched}"
    assert _count(conn, symbol, interval) == n, (
        f"Expected {n} rows in DB, got {_count(conn, symbol, interval)}"
    )


# ---------------------------------------------------------------------------
# Test 5 — idempotency: re-running inserts 0 duplicate rows (INSERT OR IGNORE)
# ---------------------------------------------------------------------------

def test_backfill_is_idempotent():
    """
    Running the backfill twice for the same symbol/interval/date range must not
    create duplicate rows. INSERT OR IGNORE enforces uniqueness on
    (symbol, interval, open_time, data_source, market_type).
    """
    conn     = _make_db()
    symbol   = "ETHUSDT"
    interval = "1h"
    ivl_ms   = bfh._interval_ms(interval)
    base_ms  = 1_710_000_000_000
    n        = 30
    klines   = _make_klines(n, base_ms, ivl_ms)

    def single_batch(sym, ivl, start, end, _calls={"n": 0}):
        _calls["n"] += 1
        return klines if _calls["n"] == 1 else []

    # First run
    with patch.object(bfh, "_fetch_batch", side_effect=single_batch):
        bfh.backfill_symbol_interval(
            conn=conn, symbol=symbol, interval=interval,
            start_ms=base_ms, end_ms=base_ms + (n + 5) * ivl_ms,
            market_type="crypto", dry_run=False, verbose=False,
        )

    after_first = _count(conn, symbol, interval)

    def single_batch_again(sym, ivl, start, end, _calls={"n": 0}):
        _calls["n"] += 1
        return klines if _calls["n"] == 1 else []

    # Second run (same data)
    with patch.object(bfh, "_fetch_batch", side_effect=single_batch_again):
        bfh.backfill_symbol_interval(
            conn=conn, symbol=symbol, interval=interval,
            start_ms=base_ms, end_ms=base_ms + (n + 5) * ivl_ms,
            market_type="crypto", dry_run=False, verbose=False,
        )

    after_second = _count(conn, symbol, interval)

    assert after_first  == n,  f"First run: expected {n} rows, got {after_first}"
    assert after_second == n,  (
        f"Second run must not add rows (idempotent). "
        f"Before: {after_first}, after: {after_second}"
    )


# ---------------------------------------------------------------------------
# Test 6 — dry_run: fetches candles but writes 0 rows
# ---------------------------------------------------------------------------

def test_dry_run_does_not_insert():
    """
    With dry_run=True, _fetch_batch is called (candles are fetched and parsed)
    but nothing is written to historical_candles.
    """
    conn     = _make_db()
    symbol   = "SOLUSDT"
    interval = "1h"
    ivl_ms   = bfh._interval_ms(interval)
    base_ms  = 1_715_000_000_000
    n        = 20
    klines   = _make_klines(n, base_ms, ivl_ms)

    calls = {"n": 0}

    def once(sym, ivl, start, end):
        calls["n"] += 1
        return klines if calls["n"] == 1 else []

    with patch.object(bfh, "_fetch_batch", side_effect=once):
        fetched = bfh.backfill_symbol_interval(
            conn=conn, symbol=symbol, interval=interval,
            start_ms=base_ms, end_ms=base_ms + (n + 5) * ivl_ms,
            market_type="crypto", dry_run=True, verbose=False,
        )

    assert fetched == n,       f"Dry run should still count {n} fetched candles; got {fetched}"
    assert _count(conn) == 0,  "Dry run must write 0 rows to historical_candles"


# ---------------------------------------------------------------------------
# Test 7 — data quality: parsed candles have valid OHLCV (no bad rows)
# ---------------------------------------------------------------------------

def test_no_bad_ohlcv_rows_after_backfill():
    """
    After inserting synthetic candles, none should fail the OHLCV validity check:
    open/high/low/close > 0, volume >= 0, high >= low.
    This mirrors the post-backfill validation run by _print_summary.
    """
    conn     = _make_db()
    symbol   = "XRPUSDT"
    interval = "15m"
    ivl_ms   = bfh._interval_ms(interval)
    base_ms  = 1_720_000_000_000
    n        = 40
    klines   = _make_klines(n, base_ms, ivl_ms)

    calls = {"n": 0}

    def once(sym, ivl, start, end):
        calls["n"] += 1
        return klines if calls["n"] == 1 else []

    with patch.object(bfh, "_fetch_batch", side_effect=once):
        bfh.backfill_symbol_interval(
            conn=conn, symbol=symbol, interval=interval,
            start_ms=base_ms, end_ms=base_ms + (n + 5) * ivl_ms,
            market_type="crypto", dry_run=False, verbose=False,
        )

    bad = conn.execute(
        """
        SELECT COUNT(*) FROM historical_candles
        WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
              OR volume < 0 OR high < low
        """
    ).fetchone()[0]

    total = _count(conn)
    assert total == n,  f"Expected {n} rows, got {total}"
    assert bad   == 0,  f"Found {bad} bad OHLCV row(s) out of {total}"
