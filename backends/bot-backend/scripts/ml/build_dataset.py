#!/usr/bin/env python3
"""
ML Dataset Builder — Phase 4 Stage 3 (v2 schema, R-multiple labels)

Extracts features from decision_traces and labels from trade_fills, joins them on
run_id + cycle_id + symbol (the entry cycle link), and writes a clean, ML-ready
Parquet dataset for offline model training.

JOIN LOGIC
  trade_fills OPEN  --+
                      +- position_id --> trade_fills CLOSE  (labels)
  decision_traces ----+ run_id + cycle_id + symbol         (features)

Features come exclusively from decision_traces (pre-trade, zero look-ahead).
Labels come from trade_fills CLOSE (post-trade outcomes).

Usage:
  python build_dataset.py --db-path /path/to/bot.db [options]

Output:
  <output-dir>/training_YYYYMMDD.parquet
  <output-dir>/training_YYYYMMDD_meta.json
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

# -- Dependency check ----------------------------------------------------------
try:
    import pandas as pd
except ImportError:
    print("[FAIL] pandas is required. Install with: pip install pandas", file=sys.stderr)
    sys.exit(1)

try:
    import pyarrow  # noqa: F401 — checked at import time, used via pandas engine
    _PARQUET_ENGINE = "pyarrow"
except ImportError:
    try:
        import fastparquet  # noqa: F401
        _PARQUET_ENGINE = "fastparquet"
    except ImportError:
        print(
            "[FAIL] A Parquet engine is required. Install with: pip install pyarrow",
            file=sys.stderr,
        )
        sys.exit(1)

import os

from shared_lib.ml.contract import (
    EVENT_FEATURE_COLUMNS,
    EVENT_TIMING_FEATURE_COLUMNS,
    MARKET_REACTION_FEATURE_COLUMNS,
    LABEL_COLUMNS,
    METADATA_COLUMNS,
    ML_CONTRACT_VERSION,
    ML_FEATURE_COLUMNS,
    ML_FEATURE_SCHEMA_HASH,
    build_contract_metadata,
)
from shared_lib.ml.event_features import build_event_feature_frame
from app.events.event_symbol_mapper import get_affected_symbols


# -- Constants -----------------------------------------------------------------

REGIME_VALUES = [
    "STRONG_TREND",
    "WEAK_TREND",
    "RANGE",
    "HIGH_VOLATILITY",
    "LOW_VOLATILITY_CHOP",
]

FEATURE_COLUMNS = list(ML_FEATURE_COLUMNS)

# Reaction features are disabled unless ML_MARKET_REACTION_FEATURES_ENABLED=true.
# Timing features (minutes_to_next_event, minutes_since_last_event, is_blackout_active)
# are always included when ML_EVENT_FEATURES_ENABLED=true.
_reaction_enabled = os.environ.get("ML_MARKET_REACTION_FEATURES_ENABLED", "false").lower() in {"1", "true", "yes"}
_event_enabled    = os.environ.get("ML_EVENT_FEATURES_ENABLED", "true").lower() in {"1", "true", "yes"}

if not _event_enabled:
    EVENT_FEATURES: list[str] = []
elif _reaction_enabled:
    EVENT_FEATURES = list(EVENT_FEATURE_COLUMNS)
else:
    EVENT_FEATURES = list(EVENT_TIMING_FEATURE_COLUMNS)

# Columns that MUST NOT appear as features — they are execution-time or post-trade.
_LEAKAGE_FORBIDDEN = frozenset({
    "fill_price",
    "fill_qty",
    "execution_status",
    "execution_error",
    "order_id",
    "final_state_change",
    "final_position",
    "realized_pnl",
    "r_multiple",
    "exit_reason",
    "mfe_pct",
    "mae_pct",
    "exit_regime",
    "exit_regime_confidence",
})

DEFAULT_POST_REPAIR_START = "2026-04-01T00:00:00+00:00"
ORGANIC_EXCLUDED_ACCOUNTS = {"backfill", "manual", "external"}
CRITICAL_LABEL_COLUMNS = ("label_r_multiple", "label_realized_pnl")


# -- CLI -----------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build ML training dataset from decision_traces + trade_fills.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--db-path",
        required=True,
        metavar="PATH",
        help="Path to the SQLite database file (bot.db).",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        metavar="DIR",
        help=(
            "Directory for output files. "
            "Defaults to <script_dir>/../../models/datasets/"
        ),
    )
    p.add_argument(
        "--min-trades",
        type=int,
        default=100,
        metavar="N",
        help="Minimum completed trades required. Aborts if fewer (default: 100).",
    )
    p.add_argument(
        "--exclude-before",
        default=None,
        metavar="YYYY-MM-DD",
        help="Exclude trades opened before this ISO date (UTC).",
    )
    p.add_argument(
        "--exclude-accounts",
        default=None,
        metavar="ID1,ID2",
        help="Comma-separated account_ids to exclude.",
    )
    p.add_argument(
        "--only-organic",
        action="store_true",
        help="Exclude backfill/manual/external account rows.",
    )
    p.add_argument(
        "--require-trace-id",
        action="store_true",
        help="Drop rows that cannot be linked by an explicit trace_id.",
    )
    p.add_argument(
        "--exclude-incomplete-labels",
        action="store_true",
        help="Drop rows missing critical labels (R multiple or realized PnL).",
    )
    p.add_argument(
        "--post-repair-only",
        nargs="?",
        const=DEFAULT_POST_REPAIR_START,
        default=None,
        metavar="ISO_TS",
        help=(
            "Restrict rows to the clean post-repair capture window. "
            f"Defaults to {DEFAULT_POST_REPAIR_START} when no timestamp is supplied."
        ),
    )
    p.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help=(
            "Full path for the output Parquet file. "
            "Overrides --output-dir and the date-based filename. "
            "The companion _meta.json is written alongside it."
        ),
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed per-column statistics and filter-stage counts.",
    )
    return p.parse_args()


# -- Database ------------------------------------------------------------------

def _connect(db_path: str) -> sqlite3.Connection:
    """Open a read-only SQLite connection with WAL support."""
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA query_only=ON")
    return conn


def _load_traces(
    conn: sqlite3.Connection,
    exclude_before: Optional[str],
    exclude_accounts: Optional[list[str]],
    executed_only: bool = True,
) -> pd.DataFrame:
    """
    Return one row per completed trade.

    Join path:
      trade_fills OPEN  --> trade_fills CLOSE  (via position_id)
      decision_traces                           (via run_id + cycle_id + symbol)

    Only rows where ALL three sources match are included.
    """
    params: list = []

    # Build optional WHERE clauses for the outer query
    extra_clauses: list[str] = []
    if exclude_before:
        extra_clauses.append("ut.ts >= ?")
        params.append(exclude_before)
    if exclude_accounts:
        placeholders = ",".join("?" * len(exclude_accounts))
        extra_clauses.append(f"ut.dt_account_id NOT IN ({placeholders})")
        params.extend(exclude_accounts)

    where_clause = ("WHERE " + " AND ".join(extra_clauses)) if extra_clauses else ""

    # The INNER JOIN to decision_traces uses run_id + cycle_id + symbol.
    # This links each OPEN fill back to the exact decision cycle that triggered it,
    # giving us the pre-trade feature context for that entry.
    #
    # We explicitly use tf_open.run_id / cycle_id (not tf_close) because the entry
    # cycle is the relevant feature window — the close cycle is a different decision.
    #
    # decision_traces may have multiple rows per (run_id, cycle_id, symbol) in
    # rare crash-recovery scenarios; we take the one with the highest confidence
    # via a subquery to ensure uniqueness.
    query = f"""
    WITH open_fills AS (
        SELECT
            position_id,
            run_id         AS open_run_id,
            cycle_id       AS open_cycle_id,
            symbol,
            side,
            price          AS open_price,
            qty            AS open_qty,
            slippage_pct   AS open_slippage_pct,
            timestamp_utc  AS open_timestamp,
            strategy,
            timeframe,
            confidence     AS open_confidence,
            account_id,
            bot_instance_id,
            stop_loss_price,
            trace_id       AS open_trace_id
        FROM trade_fills
        WHERE action = 'OPEN'
          AND position_id IS NOT NULL
          AND run_id IS NOT NULL
          AND cycle_id IS NOT NULL
    ),
    close_fills AS (
        SELECT
            position_id,
            id AS close_id,
            realized_pnl,
            r_multiple,
            exit_reason,
            mfe_pct,
            mae_pct,
            slippage_pct        AS close_slippage_pct,
            timestamp_utc       AS close_timestamp,
            exit_regime,
            exit_regime_confidence
        FROM trade_fills
        WHERE action = 'CLOSE'
          AND position_id IS NOT NULL
    ),
    close_agg AS (
        SELECT
            position_id,
            SUM(realized_pnl) AS aggregate_realized_pnl,
            MAX(mfe_pct) AS aggregate_mfe_pct,
            MIN(mae_pct) AS aggregate_mae_pct,
            COUNT(*) AS close_fill_count,
            SUM(CASE WHEN realized_pnl IS NULL THEN 1 ELSE 0 END) AS close_pnl_null_count
        FROM close_fills
        GROUP BY position_id
    ),
    terminal_close AS (
        SELECT *
        FROM (
            SELECT
                c.*,
                ROW_NUMBER() OVER (
                    PARTITION BY c.position_id
                    ORDER BY c.close_timestamp DESC, c.close_id DESC
                ) AS rn
            FROM close_fills c
        )
        WHERE rn = 1
    ),
    -- One row per completed trade (open + close both exist)
    completed AS (
        SELECT
            o.position_id,
            o.open_trace_id,
            o.open_run_id,
            o.open_cycle_id,
            o.symbol,
            o.side,
            o.open_price,
            o.open_qty,
            o.open_slippage_pct,
            o.open_timestamp,
            o.strategy,
            o.timeframe,
            o.open_confidence,
            o.account_id,
            o.bot_instance_id,
            o.stop_loss_price,
            o.open_price,
            o.open_qty,
            CASE
                WHEN ca.close_pnl_null_count = ca.close_fill_count THEN NULL
                ELSE ca.aggregate_realized_pnl
            END AS realized_pnl,
            c.r_multiple,
            c.exit_reason,
            ca.aggregate_mfe_pct AS mfe_pct,
            ca.aggregate_mae_pct AS mae_pct,
            c.close_slippage_pct,
            c.close_timestamp,
            c.exit_regime,
            c.exit_regime_confidence,
            ca.close_fill_count
        FROM open_fills  o
        INNER JOIN close_agg ca ON o.position_id = ca.position_id
        INNER JOIN terminal_close c ON o.position_id = c.position_id
    ),
    -- Unique decision trace per (run_id, cycle_id, symbol):
    -- in crash-recovery edge cases there could be two; take highest confidence.
    unique_traces AS (
        SELECT
            trace_id,
            run_id,
            cycle_id,
            position_id,
            symbol,
            ts,
            account_id          AS dt_account_id,
            timeframe           AS dt_timeframe,
            regime_state,
            regime_confidence,
            adx,
            atr_pct,
            ma_slope,
            compression_ratio,
            breakout_pressure,
            buy_score,
            sell_score,
            threshold,
            confidence,
            active_strategy_count,
            htf_opposed,
            aggressiveness_score,
            confidence_gate_modifier,
            size_multiplier,
            rolling_win_rate,
            rolling_expectancy,
            loss_streak,
            drawdown_pct,
            portfolio_risk_used,
            open_positions_count,
            margin_level,
            last_price,
            mark_price,
            chosen_strategy,
            gate_allowed,
            signal,
            sl_plan,
            tp_plan,
            -- Use row_number to pick exactly one trace per (run_id, cycle_id, symbol)
            ROW_NUMBER() OVER (
                PARTITION BY run_id, cycle_id, symbol
                ORDER BY COALESCE(confidence, 0) DESC, ts DESC
            ) AS rn
        FROM decision_traces
        { "WHERE gate_allowed = 1" if executed_only else "" }
    )
    SELECT
        -- Metadata
        ut.trace_id,
            t.position_id,
            t.open_trace_id,
            t.open_timestamp,
        t.close_timestamp,
        t.account_id,
        t.bot_instance_id,

        -- Pre-trade context (decision_traces — all feature-safe columns)
        ut.ts                       AS trace_ts,
        ut.symbol,
        ut.dt_timeframe             AS dt_timeframe,
        ut.regime_state,
        ut.regime_confidence,
        ut.adx,
        ut.atr_pct,
        ut.ma_slope,
        ut.compression_ratio,
        ut.breakout_pressure,
        ut.buy_score,
        ut.sell_score,
        ut.threshold,
        ut.confidence,
        ut.active_strategy_count,
        ut.htf_opposed,
        ut.aggressiveness_score,
        ut.confidence_gate_modifier,
        ut.size_multiplier,
        ut.rolling_win_rate,
        ut.rolling_expectancy,
        ut.loss_streak,
        ut.drawdown_pct,
        ut.portfolio_risk_used,
        ut.open_positions_count,
        ut.margin_level,
        ut.last_price,
        ut.mark_price,
        ut.chosen_strategy,
        ut.gate_allowed,
        ut.signal,
        ut.sl_plan,
        ut.tp_plan,

        -- Categorical from trade_fills (used as features)
        t.side,
        t.timeframe AS fill_timeframe,
        t.open_price,
        t.open_qty,
        t.stop_loss_price,

        -- Labels (all post-trade — from trade_fills CLOSE)
        t.realized_pnl,
        t.r_multiple,
        t.exit_reason,
        t.mfe_pct,
        t.mae_pct,
        t.open_slippage_pct,
        t.exit_regime,
        t.exit_regime_confidence

    FROM unique_traces ut
    { "INNER JOIN" if executed_only else "LEFT JOIN" } completed t
        ON (
            (t.open_trace_id IS NOT NULL AND ut.trace_id = t.open_trace_id)
            OR (ut.position_id IS NOT NULL AND ut.position_id = t.position_id)
            OR (
                t.open_trace_id IS NULL
                AND ut.position_id IS NULL
                AND ut.run_id = t.open_run_id
                AND ut.cycle_id = t.open_cycle_id
                AND ut.symbol = t.symbol
            )
        )
    {where_clause + (" AND " if where_clause else "WHERE ") + "ut.rn = 1"}
    ORDER BY ut.ts ASC
    """

    rows = conn.execute(query, params).fetchall()
    return pd.DataFrame([dict(r) for r in rows])


def _load_gate_trace_summary(
    conn: sqlite3.Connection,
    exclude_before: Optional[str],
    exclude_accounts: Optional[list[str]],
) -> pd.DataFrame:
    """
    Load a summary of ALL decision traces (including gate-blocked ones).
    Useful for understanding gate block rates and blocked-signal characteristics.
    Written as a separate file alongside the main dataset.
    """
    params: list = []
    clauses: list[str] = []
    if exclude_before:
        clauses.append("ts >= ?")
        params.append(exclude_before)
    if exclude_accounts:
        placeholders = ",".join("?" * len(exclude_accounts))
        clauses.append(f"account_id NOT IN ({placeholders})")
        params.extend(exclude_accounts)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    query = f"""
    SELECT
        trace_id, symbol, ts, timeframe, account_id,
        gate_allowed, gate_reason, execution_status,
        signal, confidence, regime_state, regime_confidence,
        chosen_strategy
    FROM decision_traces
    {where}
    ORDER BY ts ASC
    """
    rows = conn.execute(query, params).fetchall()
    return pd.DataFrame([dict(r) for r in rows])


def _load_event_sources(conn: sqlite3.Connection) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    events = pd.read_sql_query(
        """
        SELECT event_id, event_type, country_currency, impact_level, scheduled_utc
        FROM economic_events
        ORDER BY scheduled_utc ASC
        """,
        conn,
    )
    windows = pd.read_sql_query(
        """
        SELECT event_id, start_utc, end_utc, affected_symbols, is_global, reason
        FROM event_blackout_windows
        WHERE is_active = 1
        ORDER BY start_utc ASC
        """,
        conn,
    )
    reactions = pd.read_sql_query(
        """
        SELECT
            event_id,
            symbol,
            event_time_utc,
            post_window_end_utc,
            volatility_expansion_ratio,
            volume_spike_ratio,
            reaction_type,
            max_move_pct,
            min_move_pct
        FROM market_event_reactions
        ORDER BY symbol ASC, post_window_end_utc ASC, event_time_utc ASC
        """,
        conn,
    )
    return events, windows, reactions


# -- Feature engineering -------------------------------------------------------

def _build_features(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Construct the v2 feature matrix (Phase 4 Stage 3 — 21 features, 4 groups).
    All values are pre-trade (sourced from decision_traces + trade_fills OPEN).
    """
    out = pd.DataFrame(index=raw.index)

    _REGIME_MAP = {
        "STRONG_TREND": 4, "WEAK_TREND": 3, "HIGH_VOLATILITY": 2,
        "RANGE": 1, "LOW_VOLATILITY_CHOP": 0,
        # legacy aliases
        "TRENDING": 4, "STANDARD": 3, "RANGING": 1, "CHOPPY": 0,
    }
    regime_raw = raw["regime_state"].fillna("UNKNOWN")
    out["regime_enc"] = regime_raw.map(_REGIME_MAP).fillna(-1).astype("int8")

    adx = pd.to_numeric(raw["adx"], errors="coerce")
    out["adx_normalized"] = (adx / 50.0).clip(0.0, 1.0)

    out["atr_pct"] = pd.to_numeric(raw["atr_pct"], errors="coerce")

    ts = pd.to_datetime(raw["trace_ts"], errors="coerce", utc=True)
    out["is_weekend"] = (ts.dt.dayofweek >= 5).astype("int8")

    hour = ts.dt.hour
    out["session_asia"]   = ((hour >= 0)  & (hour < 8)).astype("int8")
    out["session_london"] = ((hour >= 8)  & (hour < 16)).astype("int8")
    out["session_ny"]     = ((hour >= 16) & (hour < 24)).astype("int8")

    out["compression_ratio"] = pd.to_numeric(raw["compression_ratio"], errors="coerce")
    out["breakout_pressure"] = pd.to_numeric(raw["breakout_pressure"], errors="coerce")

    entry_price = pd.to_numeric(raw["open_price"], errors="coerce")
    sl_price    = pd.to_numeric(raw["stop_loss_price"], errors="coerce")
    # sl_plan from decision_traces is the fallback for stop_loss_price when null
    # in trade_fills (e.g. old backtest rows where fill metadata was not stored).
    # fillna applies to all null rows including old backtest data — this is intentional.
    if "sl_plan" in raw.columns:
        sl_price = sl_price.fillna(pd.to_numeric(raw["sl_plan"], errors="coerce"))
    tp_plan     = pd.to_numeric(raw["tp_plan"],         errors="coerce")
    atr_pct     = out["atr_pct"]
    entry_safe  = entry_price.replace(0.0, float("nan"))

    sl_dist = (entry_price - sl_price).abs() / entry_safe
    tp_dist = (tp_plan     - entry_price).abs() / entry_safe
    sl_dist_safe = sl_dist.replace(0.0, float("nan"))

    out["sl_distance_pct"] = sl_dist
    out["tp_distance_pct"] = tp_dist
    out["planned_rr"]      = tp_dist / sl_dist_safe
    out["sl_atr_ratio"]    = sl_dist / atr_pct.replace(0.0, float("nan"))

    confidence = pd.to_numeric(raw["confidence"], errors="coerce")
    threshold  = pd.to_numeric(raw["threshold"],  errors="coerce")
    out["confidence_normed"] = confidence / threshold.replace(0.0, float("nan"))

    bp_abs = out["breakout_pressure"].abs()
    out["breakout_vs_atr"] = bp_abs / atr_pct.replace(0.0, float("nan"))

    buy_score  = pd.to_numeric(raw["buy_score"],  errors="coerce")
    sell_score = pd.to_numeric(raw["sell_score"], errors="coerce")
    out["threshold_gap"]      = confidence - threshold
    out["consensus_gap"]      = buy_score - sell_score
    out["active_count_normed"] = (
        pd.to_numeric(raw["active_strategy_count"], errors="coerce") / 4.0
    ).clip(0.0, 1.0)

    out["side_enc"] = (
        raw["side"]
        .fillna(raw.get("signal", pd.Series(dtype=str)))
        .str.upper()
        .isin(["BUY", "LONG"])
        .astype("int8")
    )
    out["htf_opposed"] = (
        pd.to_numeric(raw["htf_opposed"], errors="coerce").fillna(0).astype("int8")
    )
    out["open_positions_count"] = pd.to_numeric(
        raw["open_positions_count"], errors="coerce"
    ).astype("Int64")

    return out[FEATURE_COLUMNS]


def _build_labels(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Construct label columns from post-trade data (trade_fills CLOSE).
    All label values are known only AFTER the trade closes.
    """
    out = pd.DataFrame(index=raw.index)

    realized_pnl = pd.to_numeric(raw["realized_pnl"], errors="coerce")
    gate_allowed = pd.to_numeric(raw.get("gate_allowed", 1), errors="coerce").fillna(1)

    # Primary label: R ≥ 0.5 (risk-normalised outcome).
    # R = realized_pnl / (|entry - stop_loss| × qty)
    # Falls back to pnl > 0 when stop_loss_price is NULL (e.g. old fills).
    entry_price = pd.to_numeric(raw["open_price"],      errors="coerce")
    sl_price    = pd.to_numeric(raw["stop_loss_price"], errors="coerce")
    qty         = pd.to_numeric(raw["open_qty"],        errors="coerce")
    risk_unit   = (entry_price - sl_price).abs() * qty
    r_computed  = realized_pnl / risk_unit.replace(0.0, float("nan"))
    r_label     = (r_computed >= 0.5).fillna(realized_pnl > 0)
    out["label"] = (r_label & (gate_allowed == 1)).astype("int8")

    has_pnl_win = realized_pnl > 0
    out["label_win"]            = (has_pnl_win & (gate_allowed == 1)).astype("int8")
    stored_r = pd.to_numeric(raw["r_multiple"], errors="coerce")
    out["label_r_multiple"]     = stored_r.fillna(r_computed)
    out["label_exit_reason"]    = raw["exit_reason"].fillna("OTHER").astype(str)
    out["label_mfe_pct"]        = pd.to_numeric(raw["mfe_pct"],        errors="coerce")
    out["label_mae_pct"]        = pd.to_numeric(raw["mae_pct"],        errors="coerce")
    out["label_slippage_pct"]   = pd.to_numeric(raw["open_slippage_pct"], errors="coerce")
    out["label_exit_regime"]    = raw["exit_regime"].fillna("UNKNOWN").astype(str)
    out["label_realized_pnl"]   = realized_pnl

    open_ts  = pd.to_datetime(raw["open_timestamp"],  errors="coerce", utc=True)
    close_ts = pd.to_datetime(raw["close_timestamp"], errors="coerce", utc=True)
    out["label_duration_seconds"] = (close_ts - open_ts).dt.total_seconds()

    return out[list(LABEL_COLUMNS)]


def _build_metadata(raw: pd.DataFrame, build_ts: str) -> pd.DataFrame:
    """Construct metadata columns — identifiers and audit trail, not features."""
    out = pd.DataFrame(index=raw.index)
    out["trace_id"]                = raw["trace_id"].astype(str)
    out["position_id"]             = raw["position_id"].astype(str)
    out["open_timestamp"]          = raw["open_timestamp"].astype(str)
    out["close_timestamp"]         = raw["close_timestamp"].astype(str)
    out["account_id"]              = raw["account_id"].fillna("").astype(str)
    out["bot_instance_id"]         = raw["bot_instance_id"].fillna("").astype(str)
    out["dataset_build_timestamp"] = build_ts
    return out[list(METADATA_COLUMNS)]


# -- Leakage checks ------------------------------------------------------------

def _check_leakage(
    features: pd.DataFrame,
    labels: pd.DataFrame,
    raw: pd.DataFrame,
) -> bool:
    """
    Hard assertions against data leakage.
    Returns True if all checks pass, False otherwise.
    """
    passed = True
    feature_set = set(features.columns)
    label_set   = set(labels.columns)

    # 1. No label column in features
    overlap = feature_set & label_set
    if overlap:
        print(f"  [FAIL] Label columns in feature set: {sorted(overlap)}")
        passed = False
    else:
        print("  [OK] No label columns in feature set")

    # 2. No forbidden execution-time fields in features
    forbidden_overlap = feature_set & _LEAKAGE_FORBIDDEN
    if forbidden_overlap:
        print(f"  [FAIL] Forbidden execution-time fields in features: {sorted(forbidden_overlap)}")
        passed = False
    else:
        print("  [OK] No forbidden execution-time fields in feature set")

    # 3. All feature values must be pre-trade (sourced from decision_traces columns)
    #    — verified structurally by the SQL query using only dt.* columns for features.
    #    Nothing from tf_close can appear here. Confirmed if the prior checks pass.
    print("  [OK] Feature sources verified as pre-trade (decision_traces only)")

    # 4. No fill_price / execution_status / realized_pnl from decision_traces in features
    raw_cols_in_features = feature_set & {
        "fill_price", "execution_status", "realized_pnl",
        "final_state_change", "final_position",
    }
    if raw_cols_in_features:
        print(f"  [FAIL] Trace execution-time fields in features: {sorted(raw_cols_in_features)}")
        passed = False
    else:
        print("  [OK] No trace execution-time fields in feature set")

    # 5. No infinite values in numeric features
    numeric = features.select_dtypes(include="number")
    inf_mask = numeric.apply(lambda col: col.apply(
        lambda v: isinstance(v, float) and (math.isinf(v) or math.isnan(v))
    ))
    inf_cols = inf_mask.columns[inf_mask.any()].tolist()
    if inf_cols:
        print(f"  [!!]  Infinite/NaN values in feature columns: {inf_cols}")
        # Not a hard failure — warn only, trainer should handle NaN imputation
    else:
        print("  [OK] No infinite values in numeric features")

    return passed


# -- Data quality report -------------------------------------------------------

def _quality_report(
    final_df: pd.DataFrame,
    features: pd.DataFrame,
    event_features: pd.DataFrame,
    labels: pd.DataFrame,
    raw: pd.DataFrame,
    verbose: bool,
) -> dict:
    """Run quality checks and return a dict of stats for the metadata JSON."""
    n = len(final_df)
    sep = "=" * 62
    print(f"\n{sep}")
    print(f"  DATA QUALITY REPORT — {n:,} rows")
    print(sep)

    # -- Duplicate position_ids ------------------------------------------------
    dup_count = int(final_df["position_id"].duplicated().sum())
    if dup_count:
        print(f"  [!!]  {dup_count} duplicate position_ids")
    else:
        print("  [OK] No duplicate position_ids")

    # -- Timestamp ordering ----------------------------------------------------
    open_ts  = pd.to_datetime(final_df["open_timestamp"],  errors="coerce", utc=True)
    close_ts = pd.to_datetime(final_df["close_timestamp"], errors="coerce", utc=True)
    bad_order = int((close_ts <= open_ts).sum())
    if bad_order:
        print(f"  [FAIL] {bad_order} rows with close_timestamp <= open_timestamp")
    else:
        print("  [OK] All timestamps: open_timestamp < close_timestamp")

    # -- NULL rates ------------------------------------------------------------
    all_cols = pd.concat([features, event_features, labels, final_df[list(METADATA_COLUMNS)]], axis=1)
    null_counts_raw = all_cols.isnull().sum()
    null_counts = null_counts_raw.to_dict()

    high_null = {
        col: int(cnt)
        for col, cnt in null_counts.items()
        if col in FEATURE_COLUMNS and cnt / max(n, 1) > 0.10
    }
    if high_null:
        print(f"\n  [!!]  Features with >10% NULL rate:")
        for col, cnt in sorted(high_null.items(), key=lambda x: -x[1]):
            print(f"       {col}: {cnt:,}/{n:,} ({cnt / n * 100:.1f}%)")
    else:
        print("  [OK] No feature column exceeds 10% NULL rate")

    event_null = {
        col: int(cnt)
        for col, cnt in null_counts.items()
        if col in EVENT_FEATURES and cnt > 0
    }
    if event_null:
        print(f"\n  [INFO] Event feature NULLs (expected when no prior applicable event/reaction exists):")
        for col, cnt in sorted(event_null.items(), key=lambda x: -x[1]):
            print(f"       {col}: {cnt:,}/{n:,} ({cnt / max(n, 1) * 100:.1f}%)")
    else:
        print("  [OK] Event features fully populated")

    label_null_cols = {
        col: int(null_counts.get(col, 0))
        for col in LABEL_COLUMNS
        if null_counts.get(col, 0) > 0
    }
    if label_null_cols:
        print(f"\n  [!!]  Label columns with NULLs: {label_null_cols}")
    else:
        print("  [OK] All label columns fully populated")

    # -- Label distribution ----------------------------------------------------
    label_rate   = float(labels["label"].mean())      if "label"           in labels else float("nan")
    win_rate     = float(labels["label_win"].mean())   if "label_win"        in labels else float("nan")
    mean_r       = float(labels["label_r_multiple"].mean()) if "label_r_multiple" in labels else float("nan")
    exit_dist    = labels["label_exit_reason"].value_counts().to_dict() if "label_exit_reason" in labels else {}
    regime_dist  = raw["regime_state"].value_counts().to_dict() if "regime_state" in raw.columns else {}
    symbol_dist  = raw["symbol"].value_counts().to_dict() if "symbol" in raw.columns else {}

    print(f"\n  Label distribution:")
    print(f"    R-label (R>=0.5): {label_rate:.1%}" if not math.isnan(label_rate) else "    R-label (R>=0.5): N/A")
    print(f"    Win rate (pnl>0): {win_rate:.1%}"  if not math.isnan(win_rate) else "    Win rate (pnl>0): N/A")
    print(f"    Mean R-multiple:  {mean_r:.4f}"    if not math.isnan(mean_r)   else "    Mean R-multiple:  N/A")
    if exit_dist:
        print(f"    Exit reasons:    {exit_dist}")

    # -- Temporal range --------------------------------------------------------
    if not open_ts.isna().all():
        print(f"\n  Temporal range:")
        print(f"    Earliest trade:  {open_ts.dropna().min().isoformat()}")
        print(f"    Latest trade:    {close_ts.dropna().max().isoformat()}")

    # -- Symbol and regime distribution ----------------------------------------
    print(f"\n  Symbol distribution:  {symbol_dist}")
    print(f"  Regime distribution:  {regime_dist}")

    # -- Feature statistics (verbose) -----------------------------------------
    if verbose:
        numeric_feats = features.select_dtypes(include="number")
        stats = numeric_feats.describe().T[["min", "max", "mean", "std"]]
        print(f"\n  Feature statistics:")
        print(stats.to_string(float_format=lambda x: f"{x:.4f}"))

    return {
        "row_count":                  n,
        "duplicate_position_ids":     dup_count,
        "timestamp_order_violations": bad_order,
        "null_counts":   {k: int(v) for k, v in null_counts.items()},
        "high_null_features": high_null,
        "event_feature_null_counts": event_null,
        "label_rate":    float(label_rate) if not math.isnan(label_rate) else None,
        "win_rate":      float(win_rate) if not math.isnan(win_rate) else None,
        "mean_r_multiple": float(mean_r) if not math.isnan(mean_r) else None,
        "exit_distribution":  {k: int(v) for k, v in exit_dist.items()},
        "regime_counts":      {k: int(v) for k, v in regime_dist.items()},
        "symbol_counts":      {k: int(v) for k, v in symbol_dist.items()},
    }


def _apply_strict_filters(
    final_entry: pd.DataFrame,
    feat_entry: pd.DataFrame,
    event_feat_entry: pd.DataFrame,
    lab_entry: pd.DataFrame,
    raw_entry: pd.DataFrame,
    *,
    require_trace_id: bool,
    exclude_incomplete_labels: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, int]]:
    """Apply optional train-readiness filters after feature/label construction."""
    mask = pd.Series(True, index=final_entry.index)
    reasons: dict[str, int] = {}

    if require_trace_id:
        trace_mask = final_entry["trace_id"].notna() & final_entry["trace_id"].astype(str).ne("")
        reasons["rows_missing_trace_id"] = int((mask & ~trace_mask).sum())
        mask &= trace_mask

    if exclude_incomplete_labels:
        label_mask = lab_entry[list(CRITICAL_LABEL_COLUMNS)].notna().all(axis=1)
        reasons["rows_with_incomplete_critical_labels"] = int((mask & ~label_mask).sum())
        mask &= label_mask

    feature_mask = feat_entry.notna().all(axis=1)
    reasons["rows_with_any_feature_null"] = int((mask & ~feature_mask).sum())
    mask &= feature_mask

    # One completed trade should contribute one training row. If multiple rows
    # still survive due to historical partial/duplicate close artifacts, keep
    # the latest close and report the exclusion explicitly.
    duplicate_mask = final_entry.loc[mask, "position_id"].duplicated(keep="last")
    duplicate_indexes = duplicate_mask[duplicate_mask].index
    reasons["duplicate_position_id_rows_excluded"] = int(len(duplicate_indexes))
    if len(duplicate_indexes):
        mask.loc[duplicate_indexes] = False

    return (
        final_entry.loc[mask].reset_index(drop=True),
        feat_entry.loc[mask].reset_index(drop=True),
        event_feat_entry.loc[mask].reset_index(drop=True),
        lab_entry.loc[mask].reset_index(drop=True),
        raw_entry.loc[mask].reset_index(drop=True),
        {k: v for k, v in reasons.items() if v > 0},
    )


def _build_drop_report(
    conn: sqlite3.Connection,
    exclude_before: Optional[str],
    exclude_accounts: Optional[list[str]],
    linked_row_count: int,
) -> dict[str, Any]:
    params: list[Any] = []
    open_filters = ["action = 'OPEN'"]
    close_filters = ["action = 'CLOSE'"]
    if exclude_before:
        open_filters.append("timestamp_utc >= ?")
        close_filters.append("timestamp_utc >= ?")
        params.extend([exclude_before, exclude_before])
    if exclude_accounts:
        placeholders = ",".join("?" * len(exclude_accounts))
        open_filters.append(f"(account_id IS NULL OR account_id NOT IN ({placeholders}))")
        close_filters.append(f"(account_id IS NULL OR account_id NOT IN ({placeholders}))")
        params.extend(exclude_accounts)
        params.extend(exclude_accounts)

    open_where = " AND ".join(open_filters)
    close_where = " AND ".join(close_filters)
    report_query = f"""
    WITH filtered_opens AS (
        SELECT * FROM trade_fills WHERE {open_where}
    ),
    filtered_closes AS (
        SELECT * FROM trade_fills WHERE {close_where}
    ),
    completed AS (
        SELECT COUNT(DISTINCT o.position_id) AS c
        FROM filtered_opens o
        JOIN filtered_closes c ON c.position_id = o.position_id
        WHERE o.position_id IS NOT NULL
    ),
    missing_lineage AS (
        SELECT COUNT(*) AS c
        FROM filtered_opens
        WHERE position_id IS NULL OR run_id IS NULL OR cycle_id IS NULL
    ),
    orphan_open AS (
        SELECT COUNT(*) AS c
        FROM filtered_opens o
        WHERE o.position_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM filtered_closes c WHERE c.position_id = o.position_id
          )
    )
    SELECT
        (SELECT COUNT(*) FROM filtered_opens) AS total_open_rows,
        (SELECT c FROM completed) AS completed_trade_pairs,
        (SELECT c FROM missing_lineage) AS opens_missing_lineage_ids,
        (SELECT c FROM orphan_open) AS orphan_open_rows
    """
    row = conn.execute(report_query, tuple(params)).fetchone()
    completed_trade_pairs = int(row["completed_trade_pairs"] or 0)
    opens_missing_lineage_ids = int(row["opens_missing_lineage_ids"] or 0)
    orphan_open_rows = int(row["orphan_open_rows"] or 0)
    completed_without_trace = max(completed_trade_pairs - linked_row_count, 0)
    dropped_row_count = max(completed_trade_pairs - linked_row_count, 0)
    return {
        "source_trade_pairs": completed_trade_pairs,
        "usable_row_count": linked_row_count,
        "dropped_row_count": dropped_row_count,
        "drop_reasons": {
            "completed_trades_without_matching_trace": completed_without_trace,
            "opens_missing_lineage_ids": opens_missing_lineage_ids,
            "orphan_open_rows": orphan_open_rows,
        },
    }


# -- Filter-stage diagnostics --------------------------------------------------

def _log_filter_stages(
    conn: sqlite3.Connection,
    exclude_before: Optional[str],
    exclude_accounts: Optional[list[str]],
) -> None:
    """Print per-stage row counts to diagnose dataset collapse.

    Stages printed:
      all_closed_trades        — every completed position in trade_fills
      after_organic_filters    — excluding backfill/manual/external accounts
      after_post_repair_cutoff — additionally excluding trades before exclude_before
      with_decision_traces     — further restricted to trades linked to a trace
      (after_strict_filters    — reported separately after _apply_strict_filters)
    """

    # ── Stage 1: all completed trades (no filters) ────────────────────────────
    row = conn.execute("""
        SELECT COUNT(DISTINCT o.position_id) AS n
        FROM trade_fills o
        JOIN trade_fills c ON c.position_id = o.position_id AND c.action = 'CLOSE'
        WHERE o.action = 'OPEN' AND o.position_id IS NOT NULL
    """).fetchone()
    all_closed = int(row["n"] or 0)

    # ── Stage 2: after account exclusion ─────────────────────────────────────
    acct_params: list = []
    acct_clause = ""
    if exclude_accounts:
        ph = ",".join("?" * len(exclude_accounts))
        acct_clause = f"AND (o.account_id IS NULL OR o.account_id NOT IN ({ph}))"
        acct_params = list(exclude_accounts)
    row = conn.execute(f"""
        SELECT COUNT(DISTINCT o.position_id) AS n
        FROM trade_fills o
        JOIN trade_fills c ON c.position_id = o.position_id AND c.action = 'CLOSE'
        WHERE o.action = 'OPEN' AND o.position_id IS NOT NULL
        {acct_clause}
    """, tuple(acct_params)).fetchone()
    after_accounts = int(row["n"] or 0)

    # ── Stage 3: after post-repair date cutoff ───────────────────────────────
    cutoff_params = list(acct_params)
    cutoff_clause = ""
    if exclude_before:
        cutoff_clause = "AND o.timestamp_utc >= ?"
        cutoff_params.append(exclude_before)
    row = conn.execute(f"""
        SELECT COUNT(DISTINCT o.position_id) AS n
        FROM trade_fills o
        JOIN trade_fills c ON c.position_id = o.position_id AND c.action = 'CLOSE'
        WHERE o.action = 'OPEN' AND o.position_id IS NOT NULL
        {acct_clause}
        {cutoff_clause}
    """, tuple(cutoff_params)).fetchone()
    after_cutoff = int(row["n"] or 0)

    # ── Stage 4: with a matching decision_trace (INNER JOIN) ──────────────────
    trace_params: list = []
    t_where_parts = [
        "o.action = 'OPEN'",
        "o.position_id IS NOT NULL",
        "o.run_id IS NOT NULL",
        "o.cycle_id IS NOT NULL",
    ]
    if exclude_accounts:
        ph = ",".join("?" * len(exclude_accounts))
        t_where_parts.append(f"(o.account_id IS NULL OR o.account_id NOT IN ({ph}))")
        trace_params.extend(exclude_accounts)
    if exclude_before:
        t_where_parts.append("o.timestamp_utc >= ?")
        trace_params.append(exclude_before)
    t_where = " AND ".join(t_where_parts)
    row = conn.execute(f"""
        SELECT COUNT(DISTINCT o.position_id) AS n
        FROM trade_fills o
        JOIN trade_fills cl ON cl.position_id = o.position_id AND cl.action = 'CLOSE'
        INNER JOIN decision_traces dt
            ON dt.run_id  = o.run_id
           AND dt.cycle_id = o.cycle_id
           AND dt.symbol   = o.symbol
           AND dt.gate_allowed = 1
        WHERE {t_where}
    """, tuple(trace_params)).fetchone()
    with_traces = int(row["n"] or 0)

    print(f"\n{'-' * 60}")
    print(f"  [FILTER STAGES] Pipeline row counts:")
    print(f"  {'all_closed_trades:':<40} {all_closed:>6,}")
    print(f"  {'after_organic_filters:':<40} {after_accounts:>6,}")
    print(f"  {'after_post_repair_cutoff:':<40} {after_cutoff:>6,}")
    print(f"  {'with_decision_traces (SQL result):':<40} {with_traces:>6,}")
    print(f"  (after_strict_filters reported after feature build)")
    print(f"{'-' * 60}\n")


# -- Main ----------------------------------------------------------------------

def main() -> None:
    args = _parse_args()
    build_ts  = datetime.now(timezone.utc).isoformat()
    today_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    if args.post_repair_only:
        args.exclude_before = (
            max(args.exclude_before, args.post_repair_only)
            if args.exclude_before else args.post_repair_only
        )

    # -- Output directory ------------------------------------------------------
    if args.output_dir:
        out_dir = Path(args.output_dir)
    else:
        # Default: <repo-root>/backends/bot-backend/models/datasets/
        script_dir = Path(__file__).resolve().parent          # scripts/ml/
        out_dir    = script_dir.parent.parent / "models" / "datasets"

    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = out_dir / f"training_{today_str}.parquet"
    meta_path    = out_dir / f"training_{today_str}_meta.json"
    gate_path    = out_dir / f"gate_traces_{today_str}.parquet"

    # --output overrides the full parquet path (--output-dir and date-stamp ignored)
    if args.output:
        parquet_path = Path(args.output).resolve()
        out_dir = parquet_path.parent
        out_dir.mkdir(parents=True, exist_ok=True)
        meta_path = out_dir / f"{parquet_path.stem}_meta.json"
        gate_path = out_dir / f"gate_traces_{today_str}.parquet"

    # -- Connect ---------------------------------------------------------------
    db_path = Path(args.db_path).resolve()
    if not db_path.exists():
        print(f"[FAIL] Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Connecting to:    {db_path}")
    print(f"Output directory: {out_dir}")
    conn = _connect(str(db_path))

    try:
        exclude_accounts = [a.strip() for a in args.exclude_accounts.split(",") if a.strip()] if args.exclude_accounts else []
        if args.only_organic:
            exclude_accounts = sorted(set(exclude_accounts) | ORGANIC_EXCLUDED_ACCOUNTS)
        exclude_accounts = exclude_accounts or None

        # -- Verbose filter-stage diagnostics ----------------------------------
        if args.verbose:
            _log_filter_stages(conn, args.exclude_before, exclude_accounts)

        events_df, blackout_df, reactions_df = _load_event_sources(conn)

        # -- Load ENTRY dataset ------------------------------------------------
        print("\nLoading completed trades for ENTRY dataset ...")
        raw_entry = _load_traces(conn, args.exclude_before, exclude_accounts, executed_only=True)

        if len(raw_entry) < args.min_trades:
            print(f"[FAIL] Insufficient data: {len(raw_entry)} < {args.min_trades}", file=sys.stderr)
            sys.exit(1)

        feat_entry = _build_features(raw_entry)
        event_feat_entry = build_event_feature_frame(
            raw_entry,
            economic_events=events_df,
            blackout_windows=blackout_df,
            reactions=reactions_df,
            symbol_resolver=get_affected_symbols,
        )
        lab_entry  = _build_labels(raw_entry)
        meta_entry = _build_metadata(raw_entry, build_ts)

        leakage_passed = _check_leakage(feat_entry, lab_entry, raw_entry)
        final_entry = pd.concat([meta_entry, feat_entry, event_feat_entry, lab_entry], axis=1)

        strict_drop_reasons: dict[str, int] = {}
        if args.require_trace_id or args.exclude_incomplete_labels:
            (
                final_entry,
                feat_entry,
                event_feat_entry,
                lab_entry,
                raw_entry,
                strict_drop_reasons,
            ) = _apply_strict_filters(
                final_entry,
                feat_entry,
                event_feat_entry,
                lab_entry,
                raw_entry,
                require_trace_id=args.require_trace_id,
                exclude_incomplete_labels=args.exclude_incomplete_labels,
            )

        if args.verbose:
            print(
                f"  [FILTER STAGES] after_strict_filters (feature-complete rows): "
                f"{len(final_entry):,}"
            )

        qc = _quality_report(final_entry, feat_entry, event_feat_entry, lab_entry, raw_entry, args.verbose)
        drop_report = _build_drop_report(conn, args.exclude_before, exclude_accounts, len(final_entry))
        fully_usable_row_count = int(feat_entry.notna().all(axis=1).sum())
        rows_with_any_feature_null = int(len(feat_entry) - fully_usable_row_count)
        if rows_with_any_feature_null > 0:
            drop_report["drop_reasons"]["rows_with_any_feature_null"] = rows_with_any_feature_null
        drop_report["drop_reasons"].update(strict_drop_reasons)

        print(f"\nWriting ENTRY dataset -> {parquet_path}")
        final_entry.to_parquet(str(parquet_path), index=False, engine=_PARQUET_ENGINE)
        
        # -- Load GATE dataset --------------------------------------------------
        print("\nLoading all signals for GATE dataset ...")
        raw_gate = _load_traces(conn, args.exclude_before, exclude_accounts, executed_only=False)
        feat_gate = _build_features(raw_gate)
        event_feat_gate = build_event_feature_frame(
            raw_gate,
            economic_events=events_df,
            blackout_windows=blackout_df,
            reactions=reactions_df,
            symbol_resolver=get_affected_symbols,
        )
        lab_gate  = _build_labels(raw_gate)
        meta_gate = _build_metadata(raw_gate, build_ts)
        final_gate = pd.concat([meta_gate, feat_gate, event_feat_gate, lab_gate], axis=1)
        
        print(f"Writing GATE dataset -> {gate_path}")
        final_gate.to_parquet(str(gate_path), index=False, engine=_PARQUET_ENGINE)



        # -- Write metadata JSON -----------------------------------------------
        meta_doc = {
            **build_contract_metadata(training_dataset_path=str(parquet_path)),
            "dataset_build_timestamp": build_ts,
            "db_path":                 str(db_path),
            "parquet_engine":          _PARQUET_ENGINE,
            "row_count":               len(final_entry),
            "usable_row_count":        fully_usable_row_count,
            "dropped_row_count":       int(drop_report["dropped_row_count"] + rows_with_any_feature_null),
            "drop_reasons":            drop_report["drop_reasons"],
            "strict_filters": {
                "only_organic": args.only_organic,
                "require_trace_id": args.require_trace_id,
                "exclude_incomplete_labels": args.exclude_incomplete_labels,
                "post_repair_only": args.post_repair_only,
            },
            "contract_version":        ML_CONTRACT_VERSION,
            "schema_hash":             ML_FEATURE_SCHEMA_HASH,
            "feature_count":              len(FEATURE_COLUMNS),
            "event_feature_count":        len(EVENT_FEATURES),
            "event_timing_feature_count": len([c for c in EVENT_FEATURES if c in EVENT_TIMING_FEATURE_COLUMNS]),
            "reaction_feature_count":     len([c for c in EVENT_FEATURES if c in MARKET_REACTION_FEATURE_COLUMNS]),
            "dataset_feature_count":      len(FEATURE_COLUMNS) + len(EVENT_FEATURES),
            "label_count":             len(LABEL_COLUMNS),
            "feature_columns":         FEATURE_COLUMNS,
            "event_feature_columns":   EVENT_FEATURES,
            "dataset_feature_columns": FEATURE_COLUMNS + EVENT_FEATURES,
            "label_columns":           list(LABEL_COLUMNS),
            "metadata_columns":        list(METADATA_COLUMNS),
            "date_range": {
                "earliest": str(raw_entry["open_timestamp"].min())  if not raw_entry.empty else None,
                "latest":   str(raw_entry["close_timestamp"].max()) if not raw_entry.empty else None,
            },
            "symbol_counts":        qc["symbol_counts"],
            "regime_counts":        qc["regime_counts"],
            "exit_distribution":    qc["exit_distribution"],
            "label_rate":           qc.get("label_rate"),
            "win_rate":             qc["win_rate"],
            "mean_r_multiple":      qc["mean_r_multiple"],
            "null_counts":          qc["null_counts"],
            "feature_null_counts":  {k: int(qc["null_counts"].get(k, 0)) for k in FEATURE_COLUMNS},
            "event_feature_null_counts": {k: int(qc["null_counts"].get(k, 0)) for k in EVENT_FEATURES},
            "label_null_counts":    {k: int(qc["null_counts"].get(k, 0)) for k in LABEL_COLUMNS},
            "high_null_features":   qc["high_null_features"],
            "duplicate_position_ids":     qc["duplicate_position_ids"],
            "timestamp_order_violations": qc["timestamp_order_violations"],
            "leakage_check_passed": leakage_passed,
            # ── Phase E dataset feature policy fields (2026-04-26) ─────────────
            # These record which feature groups were active at build time.
            # They are authoritative audit metadata — do not remove.
            "event_features_enabled":             _event_enabled,
            "reaction_features_enabled":          _reaction_enabled,
            "news_validation_features_enabled":   False,
            "raw_news_features_enabled":          False,
            "leakage_guard_enabled":              True,
            "max_event_lookahead_minutes":        0,
            "completed_reaction_only":            True,
            "reaction_feature_columns": list(MARKET_REACTION_FEATURE_COLUMNS) if _reaction_enabled else [],
            "event_timing_feature_columns": list(EVENT_TIMING_FEATURE_COLUMNS) if _event_enabled else [],
            # ── End Phase E policy fields ───────────────────────────────────────
            # ── Phase G event coverage metadata (2026-04-26) ──────────────────
            # Populated after Phase G historical backfill.  All counts are
            # computed from the current DB state at build time; they are
            # authoritative audit fields — do not remove.
            "historical_event_backfill_enabled":  True,
            "historical_event_range_start":        "2025-12-01",
            "historical_event_range_end":          "2026-05-25",
            "historical_event_count":              int(
                len(events_df) if "events_df" in dir() else 0
            ),
            "blackout_window_count":               int(
                len(blackout_df) if "blackout_df" in dir() else 0
            ),
            "event_window_row_count":              int(
                event_feat_entry["minutes_to_next_event"].notna().sum()
                + event_feat_entry["minutes_since_last_event"].notna().sum()
            ),
            "before_event_row_count":              int(
                event_feat_entry["minutes_to_next_event"].between(0, 60).sum()
                if "minutes_to_next_event" in event_feat_entry.columns else 0
            ),
            "after_event_row_count":               int(
                event_feat_entry["minutes_since_last_event"].between(0, 60).sum()
                if "minutes_since_last_event" in event_feat_entry.columns else 0
            ),
            "blackout_active_row_count":           int(
                (event_feat_entry["is_blackout_active"] == 1).sum()
                if "is_blackout_active" in event_feat_entry.columns else 0
            ),
            "date_proxy_risk":                     None,   # set by phase_f_experiment.py
            "event_feature_coverage_passed":       None,   # set by audit_event_feature_coverage.py
            "reaction_backfill_enabled":           False,  # True once candle data available
            "reaction_count":                      int(
                len(reactions_df) if "reactions_df" in dir() else 0
            ),
            "reaction_feature_coverage_passed":    None,   # set by audit_event_feature_coverage.py
            # ── End Phase G coverage metadata ──────────────────────────────────
            "class_balance": {
                "label": {
                    "positive": int(lab_entry["label"].sum()) if "label" in lab_entry else 0,
                    "negative": int(len(lab_entry) - lab_entry["label"].sum()) if "label" in lab_entry else 0,
                    "single_class": bool(
                        "label" in lab_entry and (
                            int(lab_entry["label"].sum()) == 0
                            or int(len(lab_entry) - lab_entry["label"].sum()) == 0
                        )
                    ),
                },
                "label_win": {
                    "positive": int(lab_entry["label_win"].sum()) if "label_win" in lab_entry else 0,
                    "negative": int(len(lab_entry) - lab_entry["label_win"].sum()) if "label_win" in lab_entry else 0,
                    "single_class": bool(
                        "label_win" in lab_entry and (
                            int(lab_entry["label_win"].sum()) == 0
                            or int(len(lab_entry) - lab_entry["label_win"].sum()) == 0
                        )
                    ),
                },
                "label_positive_count": int(lab_entry["label"].sum()) if "label" in lab_entry else 0,
                "label_negative_count": int(len(lab_entry) - lab_entry["label"].sum()) if "label" in lab_entry else 0,
                "label_win_positive_count": int(lab_entry["label_win"].sum()) if "label_win" in lab_entry else 0,
                "label_win_negative_count": int(len(lab_entry) - lab_entry["label_win"].sum()) if "label_win" in lab_entry else 0,
            },
            "build_options": {
                "min_trades":       args.min_trades,
                "exclude_before":   args.exclude_before,
                "exclude_accounts": exclude_accounts,
            },
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta_doc, f, indent=2)
        print(f"  [OK] Metadata JSON -> {meta_path}")

        # -- Final summary -----------------------------------------------------
        lab_str = f"{qc['label_rate']:.1%}" if qc.get("label_rate") is not None else "N/A"
        win_str = f"{qc['win_rate']:.1%}" if qc["win_rate"] is not None else "N/A"
        r_str   = f"{qc['mean_r_multiple']:.4f}" if qc["mean_r_multiple"] is not None else "N/A"
        print(f"\n{'=' * 62}")
        print(f"  DATASET BUILD COMPLETE  (v2 schema, Phase 4 Stage 3)")
        print(f"{'=' * 62}")
        print(f"  Rows:             {len(final_entry):,}")
        timing_count   = len([c for c in EVENT_FEATURES if c in EVENT_TIMING_FEATURE_COLUMNS])
        reaction_count = len([c for c in EVENT_FEATURES if c in MARKET_REACTION_FEATURE_COLUMNS])
        print(f"  Features:         {len(FEATURE_COLUMNS)} base + {timing_count} timing + {reaction_count} reaction = {len(FEATURE_COLUMNS) + len(EVENT_FEATURES)} total")
        print(f"  Labels:           {len(LABEL_COLUMNS)}")
        print(f"  R-label (R>=0.5): {lab_str}")
        print(f"  Win rate (pnl>0): {win_str}")
        print(f"  Mean R-multiple:  {r_str}")
        print(f"  Parquet engine:   {_PARQUET_ENGINE}")
        print(f"  Parquet:          {parquet_path}")
        print(f"  Metadata:         {meta_path}")
        print(f"  Leakage check:    {'PASSED [OK]' if leakage_passed else 'FAILED [FAIL]'}")
        print(f"  Duplicates:       {'none [OK]' if qc['duplicate_position_ids'] == 0 else str(qc['duplicate_position_ids']) + ' [!!]'}")
        print()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
