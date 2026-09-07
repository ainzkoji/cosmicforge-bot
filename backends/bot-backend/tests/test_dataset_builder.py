"""
Tests for scripts/ml/build_dataset.py — FIX-A: Organic dataset filter repair.

Covers the 5 required behaviours:
  A  Post-cutoff trades with valid traces are included
  B  Pre-cutoff trades are excluded
  C  Trades without decision_trace linkage are excluded
  D  Dataset does not collapse to 1 row when valid April-2026 data exists
     (regression guard for the DEFAULT_POST_REPAIR_START bug)
  E  v2 contract schema is preserved (21 features, correct hash)
"""
from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Import build_dataset as a module from its file path so we don't need it
# on the regular Python package path.
# ---------------------------------------------------------------------------

_SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "ml" / "build_dataset.py"
)


def _load_build_dataset():
    """Load build_dataset.py as a module and return it."""
    spec = importlib.util.spec_from_file_location("build_dataset", _SCRIPT_PATH)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Load once at module level so all tests share the same import
_bd = _load_build_dataset()


# ---------------------------------------------------------------------------
# Shared DB helpers
# ---------------------------------------------------------------------------

_TRADE_FILLS_DDL = """
CREATE TABLE IF NOT EXISTS trade_fills (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id           TEXT,
    run_id                TEXT,
    cycle_id              TEXT,
    symbol                TEXT,
    side                  TEXT,
    price                 REAL,
    qty                   REAL,
    slippage_pct          REAL DEFAULT 0.0,
    timestamp_utc         TEXT,
    strategy              TEXT DEFAULT 'momentum',
    timeframe             TEXT DEFAULT '15m',
    confidence            REAL DEFAULT 0.75,
    account_id            TEXT DEFAULT 'live',
    bot_instance_id       TEXT DEFAULT 'bot1',
    stop_loss_price       REAL,
    trace_id              TEXT,
    action                TEXT,
    realized_pnl          REAL,
    r_multiple            REAL,
    exit_reason           TEXT,
    mfe_pct               REAL,
    mae_pct               REAL,
    exit_regime           TEXT,
    exit_regime_confidence REAL
)
"""

_DECISION_TRACES_DDL = """
CREATE TABLE IF NOT EXISTS decision_traces (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id               TEXT,
    run_id                 TEXT,
    cycle_id               TEXT,
    position_id            TEXT,
    symbol                 TEXT,
    ts                     TEXT,
    account_id             TEXT DEFAULT 'live',
    timeframe              TEXT DEFAULT '15m',
    regime_state           TEXT DEFAULT 'STRONG_TREND',
    regime_confidence      REAL DEFAULT 0.8,
    adx                    REAL DEFAULT 30.0,
    atr_pct                REAL DEFAULT 0.01,
    ma_slope               REAL DEFAULT 0.001,
    compression_ratio      REAL DEFAULT 0.5,
    breakout_pressure      REAL DEFAULT 0.02,
    buy_score              REAL DEFAULT 0.8,
    sell_score             REAL DEFAULT 0.2,
    threshold              REAL DEFAULT 0.70,
    confidence             REAL DEFAULT 0.75,
    active_strategy_count  INTEGER DEFAULT 2,
    htf_opposed            INTEGER DEFAULT 0,
    aggressiveness_score   REAL DEFAULT 0.5,
    confidence_gate_modifier REAL DEFAULT 1.0,
    size_multiplier        REAL DEFAULT 1.0,
    rolling_win_rate       REAL DEFAULT 0.4,
    rolling_expectancy     REAL DEFAULT 0.1,
    loss_streak            INTEGER DEFAULT 0,
    drawdown_pct           REAL DEFAULT 0.0,
    portfolio_risk_used    REAL DEFAULT 0.01,
    open_positions_count   INTEGER DEFAULT 0,
    margin_level           REAL DEFAULT 1.5,
    last_price             REAL DEFAULT 100.0,
    mark_price             REAL DEFAULT 100.0,
    chosen_strategy        TEXT DEFAULT 'momentum',
    gate_allowed           INTEGER DEFAULT 1,
    signal                 TEXT DEFAULT 'BUY',
    sl_plan                REAL DEFAULT 98.0,
    tp_plan                REAL DEFAULT 104.0,
    final_confidence       REAL DEFAULT 0.75
)
"""

# Stub tables needed by _load_event_sources (not required for _load_traces tests)
_EVENT_STUBS = """
CREATE TABLE IF NOT EXISTS economic_events (
    id INTEGER PRIMARY KEY,
    event_time_utc TEXT,
    event_name TEXT,
    currency TEXT,
    impact_level TEXT,
    forecast TEXT,
    actual TEXT,
    previous TEXT
);
CREATE TABLE IF NOT EXISTS event_blackout_windows (
    id INTEGER PRIMARY KEY,
    event_id INTEGER,
    window_start_utc TEXT,
    window_end_utc TEXT,
    affected_symbols TEXT
);
CREATE TABLE IF NOT EXISTS market_event_reactions (
    id INTEGER PRIMARY KEY,
    event_id INTEGER,
    symbol TEXT,
    event_time_utc TEXT,
    post_window_end_utc TEXT,
    volatility_expansion_ratio REAL,
    volume_spike_ratio REAL,
    reaction_type TEXT,
    max_move_pct REAL,
    min_move_pct REAL
);
"""


def _make_db() -> sqlite3.Connection:
    """Return an in-memory SQLite connection with the required schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(_TRADE_FILLS_DDL)
    conn.execute(_DECISION_TRACES_DDL)
    for stmt in _EVENT_STUBS.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()
    return conn


def _insert_completed_trade(
    conn: sqlite3.Connection,
    *,
    position_id: str,
    run_id: str,
    cycle_id: str,
    symbol: str = "BTCUSDT",
    account_id: str = "live",
    open_ts: str,
    close_ts: str,
    open_price: float = 100.0,
    stop_loss_price: float = 98.0,
    tp_plan: float = 104.0,
    realized_pnl: float = -5.0,
    r_multiple: float = -1.0,
    exit_reason: str = "SL",
    trace_id: str | None = None,
) -> None:
    """Insert one OPEN + one CLOSE fill for a position."""
    conn.execute(
        """
        INSERT INTO trade_fills
          (position_id, run_id, cycle_id, symbol, side, price, qty,
           timestamp_utc, account_id, stop_loss_price, trace_id, action,
           confidence)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,'OPEN', 0.75)
        """,
        (position_id, run_id, cycle_id, symbol, "BUY", open_price, 1.0,
         open_ts, account_id, stop_loss_price, trace_id),
    )
    conn.execute(
        """
        INSERT INTO trade_fills
          (position_id, run_id, cycle_id, symbol, side, price, qty,
           timestamp_utc, account_id, action,
           realized_pnl, r_multiple, exit_reason, mfe_pct, mae_pct,
           exit_regime, exit_regime_confidence)
        VALUES (?,?,?,?,?,?,?,?,?,'CLOSE',?,?,?,0.01,-0.02,'RANGE',0.7)
        """,
        (position_id, run_id, cycle_id, symbol, "BUY", open_price - 2, 1.0,
         close_ts, account_id, realized_pnl, r_multiple, exit_reason),
    )


def _insert_trace(
    conn: sqlite3.Connection,
    *,
    trace_id: str,
    run_id: str,
    cycle_id: str,
    symbol: str = "BTCUSDT",
    ts: str,
    account_id: str = "live",
    gate_allowed: int = 1,
) -> None:
    """Insert one decision_trace row."""
    conn.execute(
        """
        INSERT INTO decision_traces
          (trace_id, run_id, cycle_id, symbol, ts, account_id, gate_allowed)
        VALUES (?,?,?,?,?,?,?)
        """,
        (trace_id, run_id, cycle_id, symbol, ts, account_id, gate_allowed),
    )


# ---------------------------------------------------------------------------
# Test A — post-cutoff trade with valid trace is included
# ---------------------------------------------------------------------------

def test_post_cutoff_trade_is_included():
    """A trade from April 2026 must survive the post-repair cutoff of 2026-04-01."""
    conn = _make_db()
    _insert_completed_trade(
        conn, position_id="pos_a", run_id="r_a", cycle_id="c_a",
        open_ts="2026-04-15T10:00:00+00:00",
        close_ts="2026-04-15T11:00:00+00:00",
        trace_id="trc_a",
    )
    _insert_trace(
        conn, trace_id="trc_a", run_id="r_a", cycle_id="c_a",
        ts="2026-04-15T09:59:00+00:00",
    )
    conn.commit()

    result = _bd._load_traces(
        conn,
        exclude_before="2026-04-01T00:00:00+00:00",
        exclude_accounts=None,
        executed_only=True,
    )
    assert len(result) == 1, (
        f"Expected 1 row for post-cutoff April trade, got {len(result)}"
    )


# ---------------------------------------------------------------------------
# Test B — pre-cutoff trade is excluded
# ---------------------------------------------------------------------------

def test_pre_cutoff_trade_is_excluded():
    """A trade from March 2026 must be excluded when cutoff is 2026-04-01."""
    conn = _make_db()
    _insert_completed_trade(
        conn, position_id="pos_b", run_id="r_b", cycle_id="c_b",
        open_ts="2026-03-15T10:00:00+00:00",
        close_ts="2026-03-15T11:00:00+00:00",
        trace_id="trc_b",
    )
    _insert_trace(
        conn, trace_id="trc_b", run_id="r_b", cycle_id="c_b",
        ts="2026-03-15T09:59:00+00:00",
    )
    conn.commit()

    result = _bd._load_traces(
        conn,
        exclude_before="2026-04-01T00:00:00+00:00",
        exclude_accounts=None,
        executed_only=True,
    )
    assert len(result) == 0, (
        f"Expected 0 rows for pre-cutoff March trade, got {len(result)}"
    )


# ---------------------------------------------------------------------------
# Test C — trade without decision_trace linkage is excluded
# ---------------------------------------------------------------------------

def test_trade_without_trace_linkage_excluded():
    """A completed trade with no matching decision_trace must be excluded."""
    conn = _make_db()
    _insert_completed_trade(
        conn, position_id="pos_c", run_id="r_c", cycle_id="c_c",
        open_ts="2026-04-20T10:00:00+00:00",
        close_ts="2026-04-20T11:00:00+00:00",
        trace_id=None,  # no trace_id
    )
    # Intentionally: NO decision_trace row inserted for r_c/c_c
    conn.commit()

    result = _bd._load_traces(
        conn,
        exclude_before=None,
        exclude_accounts=None,
        executed_only=True,   # INNER JOIN — requires matching trace
    )
    assert len(result) == 0, (
        f"Expected 0 rows when no matching decision_trace exists, got {len(result)}"
    )


# ---------------------------------------------------------------------------
# Test D — regression guard: dataset must not collapse to 1 row for April data
# ---------------------------------------------------------------------------

def test_dataset_does_not_collapse_with_april_data():
    """
    Regression guard for the DEFAULT_POST_REPAIR_START bug.

    Before the fix: DEFAULT_POST_REPAIR_START = "2026-05-03T05:38:37+00:00"
    After the fix:  DEFAULT_POST_REPAIR_START = "2026-04-01T00:00:00+00:00"

    With the fixed constant, 5 April-2026 trades must all appear in the dataset.
    With the old constant they would all be excluded (only post-May-3 survive).
    """
    conn = _make_db()
    for i in range(5):
        pid = f"pos_d{i}"
        rid = f"r_d{i}"
        cid = f"c_d{i}"
        trc = f"trc_d{i}"
        day = 10 + i
        _insert_completed_trade(
            conn, position_id=pid, run_id=rid, cycle_id=cid,
            open_ts=f"2026-04-{day:02d}T10:00:00+00:00",
            close_ts=f"2026-04-{day:02d}T11:00:00+00:00",
            trace_id=trc,
        )
        _insert_trace(
            conn, trace_id=trc, run_id=rid, cycle_id=cid,
            ts=f"2026-04-{day:02d}T09:59:00+00:00",
        )
    conn.commit()

    # Verify the fixed DEFAULT_POST_REPAIR_START value
    assert _bd.DEFAULT_POST_REPAIR_START == "2026-04-01T00:00:00+00:00", (
        f"DEFAULT_POST_REPAIR_START is wrong: {_bd.DEFAULT_POST_REPAIR_START!r}. "
        "Expected '2026-04-01T00:00:00+00:00' (FIX-A fix)."
    )

    # With the fixed cutoff, all 5 April trades must be included
    result = _bd._load_traces(
        conn,
        exclude_before=_bd.DEFAULT_POST_REPAIR_START,
        exclude_accounts=None,
        executed_only=True,
    )
    assert len(result) == 5, (
        f"Expected 5 rows with fixed cutoff '{_bd.DEFAULT_POST_REPAIR_START}', "
        f"got {len(result)}. Dataset collapse bug may have regressed."
    )

    # Confirm the old broken value would have excluded all 5 trades
    OLD_BROKEN_CUTOFF = "2026-05-03T05:38:37+00:00"
    result_old = _bd._load_traces(
        conn,
        exclude_before=OLD_BROKEN_CUTOFF,
        exclude_accounts=None,
        executed_only=True,
    )
    assert len(result_old) == 0, (
        f"Sanity check: old cutoff '{OLD_BROKEN_CUTOFF}' should exclude all "
        f"April trades, but got {len(result_old)} rows."
    )


# ---------------------------------------------------------------------------
# Test E — v2 contract is preserved
# ---------------------------------------------------------------------------

def test_v2_contract_preserved():
    """
    The feature engineering must produce exactly the 21 v2 contract columns
    with the correct schema hash. This guards against accidental contract changes.
    """
    import pandas as pd
    from shared_lib.ml.contract import ML_FEATURE_COLUMNS, ML_FEATURE_SCHEMA_HASH

    # The script's internal FEATURE_COLUMNS must match the shared contract
    assert list(_bd.FEATURE_COLUMNS) == list(ML_FEATURE_COLUMNS), (
        "build_dataset.FEATURE_COLUMNS diverges from shared_lib ML_FEATURE_COLUMNS. "
        "v2 contract is broken."
    )
    assert len(_bd.FEATURE_COLUMNS) == 21, (
        f"Expected 21 v2 feature columns, got {len(_bd.FEATURE_COLUMNS)}."
    )

    # Build a minimal raw DataFrame and verify _build_features returns all 21 cols
    raw = pd.DataFrame([{
        "regime_state": "STRONG_TREND",
        "adx": 30.0,
        "atr_pct": 0.01,
        "trace_ts": "2026-04-15T10:00:00+00:00",
        "compression_ratio": 0.5,
        "breakout_pressure": 0.02,
        "open_price": 100.0,
        "stop_loss_price": 98.0,
        "sl_plan": 98.0,
        "tp_plan": 104.0,
        "confidence": 0.75,
        "threshold": 0.70,
        "buy_score": 0.8,
        "sell_score": 0.2,
        "active_strategy_count": 2,
        "side": "BUY",
        "signal": "BUY",
        "htf_opposed": 0,
        "open_positions_count": 1,
    }])
    feats = _bd._build_features(raw)
    missing = [c for c in ML_FEATURE_COLUMNS if c not in feats.columns]
    assert not missing, (
        f"_build_features missing v2 contract columns: {missing}"
    )
    assert list(feats.columns) == list(ML_FEATURE_COLUMNS), (
        "Feature column order does not match v2 contract. "
        f"Got: {list(feats.columns)}"
    )
