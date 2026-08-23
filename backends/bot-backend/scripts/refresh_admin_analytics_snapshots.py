from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


BACKENDS_ROOT = Path(__file__).resolve().parents[2]
BOT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(BACKENDS_ROOT / "shared"))

from shared_lib.persistence.admin_analytics import ensure_admin_analytics_snapshot_tables

try:
    from shared_lib.ml.contract import (
        ML_CONTRACT_VERSION,
        ML_FEATURE_COLUMNS,
        ML_FEATURE_SCHEMA_HASH,
        build_contract_metadata,
        compute_feature_value,
        get_contract_status,
    )
except Exception:  # pragma: no cover - fallback for minimal script environments.
    ML_CONTRACT_VERSION = None
    ML_FEATURE_COLUMNS = ()
    ML_FEATURE_SCHEMA_HASH = None
    build_contract_metadata = None
    compute_feature_value = None
    get_contract_status = None


DEFAULT_DB_PATH = REPO_ROOT / "backends" / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
LIVE_SCOPE = "live"
RECENT_FEATURE_LIMIT = 500
REQUIRED_TRADES = 200
REQUIRED_WINS = 50
MIN_FEATURE_COVERAGE_PCT = 90.0
MIN_LINKAGE_HEALTH_PCT = 95.0
DEFAULT_ACTIVITY_DAYS = 30
DEFAULT_ACTIVITY_PAGE_SIZE = 50
DEFAULT_ACTIVITY_SNAPSHOT_ROWS = 200
DEFAULT_SHADOW_DAYS = 90
DEFAULT_DRIFT_WINDOW_DAYS = 30
ACTION_CONFIRM_PHRASES = {
    "rebuild_dataset": "REBUILD DATASET",
    "run_training": "RUN TRAINING",
    "run_validation": "RUN VALIDATION",
    "deploy_shadow": "DEPLOY TO SHADOW",
    "promote_live": "PROMOTE TO LIVE",
    "rollback_shadow": "ROLL BACK TO SHADOW",
    "disable_ml": "DISABLE ML",
}
SUPPORTED_ACTIONS = {"rebuild_dataset", "run_training", "run_validation"}


TRACE_COLUMNS = (
    "trace_id",
    "run_id",
    "cycle_id",
    "symbol",
    "ts",
    "timeframe",
    "regime_state",
    "regime_confidence",
    "signal",
    "confidence",
    "chosen_strategy",
    "ml_score",
    "ml_action",
    "adx",
    "atr_pct",
    "ma_slope",
    "compression_ratio",
    "breakout_pressure",
    "buy_score",
    "sell_score",
    "threshold",
    "sl_plan",
    "tp_plan",
    "active_strategy_count",
    "htf_opposed",
    "drawdown_pct",
    "portfolio_risk_used",
    "open_positions_count",
    "margin_level",
    "last_price",
    "mark_price",
    "ml_model_version",
    "ml_threshold",
    "position_id",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_db_path(value: str | None = None) -> Path:
    raw = value or os.environ.get("DATABASE_URL") or str(DEFAULT_DB_PATH)
    if raw.startswith("sqlite:///"):
        raw = raw[len("sqlite:///") :]
    path = Path(raw)
    if path.is_absolute():
        return path
    if value:
        return (Path.cwd() / path).resolve()
    return (BOT_ROOT / path).resolve()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh offline Admin Analytics snapshots for Profitability and ML Monitoring."
    )
    parser.add_argument("--db-path", help="SQLite DB path or sqlite:/// URL. Defaults to DATABASE_URL.")
    parser.add_argument("--all", action="store_true", help="Refresh all snapshot groups.")
    parser.add_argument("--profitability", action="store_true", help="Refresh profitability daily/symbol snapshots.")
    parser.add_argument("--sizing-events", action="store_true", help="Refresh extracted sizing event snapshots.")
    parser.add_argument("--ml", action="store_true", help="Refresh ML linked-trade/dashboard snapshots.")
    return parser.parse_args()


def safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        parsed = float(value)
        if math.isnan(parsed):
            return None
        return parsed
    except (TypeError, ValueError):
        return None


def pct(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) * 100.0 / float(denominator), 4)


def profit_factor(gross_profit: float | None, gross_loss_abs: float | None) -> float | None:
    profit = float(gross_profit or 0.0)
    loss = abs(float(gross_loss_abs or 0.0))
    if loss == 0:
        return None
    return round(profit / loss, 6)


def json_loads_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def log_result(name: str, started: float, rows: int, extra: str = "") -> None:
    elapsed_ms = (time.perf_counter() - started) * 1000
    suffix = f" {extra}" if extra else ""
    print(f"[admin-analytics-refresh] {name} rows={rows} elapsed_ms={elapsed_ms:.2f}{suffix}")


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def score_buckets() -> list[str]:
    return [
        "0.0-0.1",
        "0.1-0.2",
        "0.2-0.3",
        "0.3-0.4",
        "0.4-0.5",
        "0.5-0.6",
        "0.6-0.7",
        "0.7-0.8",
        "0.8-0.9",
        "0.9-1.0",
    ]


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def session_for_timestamp(value: str | None) -> str:
    parsed = parse_dt(value)
    if parsed is None:
        return "unknown"
    if 0 <= parsed.hour < 8:
        return "asia"
    if 8 <= parsed.hour < 16:
        return "london"
    return "ny"


def ml_mode(*, enabled: bool, shadow_mode: bool) -> str:
    if not enabled:
        return "disabled"
    return "shadow" if shadow_mode else "active"


def gate_status(
    *,
    total_linked_completed_trades: int,
    wins: int,
    linkage_healthy: bool,
    critical_feature_broken: bool,
    label_distribution_single_class: bool,
    feature_coverage_pct: float,
    training_ready: bool,
) -> str:
    if training_ready:
        return "ready_for_training"
    if total_linked_completed_trades == 0:
        return "not_ready"
    if (
        not linkage_healthy
        or critical_feature_broken
        or label_distribution_single_class
        or feature_coverage_pct < MIN_FEATURE_COVERAGE_PCT
    ):
        return "blocked"
    if total_linked_completed_trades < REQUIRED_TRADES or wins < REQUIRED_WINS:
        return "collecting_data"
    return "blocked"


def current_ml_status(gate: dict[str, Any], mode: str, current_model_version: str | None) -> str:
    if not gate["training_ready"]:
        if gate["status"] == "collecting_data":
            return "collecting_data"
        return "not_ready"
    if gate["training_ready"] and not current_model_version:
        return "ready_for_training"
    if current_model_version and mode == "disabled":
        return "ready_for_shadow_deployment"
    if current_model_version and mode in {"shadow", "active"}:
        return "ready_for_live_promotion"
    return "not_ready"


def action_counter() -> dict[str, int]:
    return {"allow_count": 0, "shadow_count": 0, "block_count": 0, "skip_count": 0}


def bump_action_counter(target: dict[str, dict[str, int]], key: str, action: str) -> None:
    entry = target.setdefault(key, action_counter())
    normalized = (action or "SKIP").upper()
    if normalized == "ALLOW":
        entry["allow_count"] += 1
    elif normalized == "SHADOW":
        entry["shadow_count"] += 1
    elif normalized == "BLOCK":
        entry["block_count"] += 1
    else:
        entry["skip_count"] += 1


def flatten_action_counters(target: dict[str, dict[str, int]]) -> list[dict[str, Any]]:
    rows = [{"key": key, **counts} for key, counts in target.items()]
    rows.sort(
        key=lambda row: row["allow_count"] + row["shadow_count"] + row["block_count"] + row["skip_count"],
        reverse=True,
    )
    return rows


def refresh_profitability_summaries(conn: sqlite3.Connection) -> int:
    started = time.perf_counter()
    now = utc_now_iso()
    daily_rows = conn.execute(
        """
        SELECT
            substr(timestamp_utc, 1, 10) AS date,
            COUNT(*) AS fills_count,
            SUM(CASE WHEN action = 'CLOSE' THEN 1 ELSE 0 END) AS closed_trades,
            SUM(CASE WHEN action = 'CLOSE' AND realized_pnl > 0 THEN 1 ELSE 0 END) AS winning_trades,
            SUM(CASE WHEN action = 'CLOSE' AND realized_pnl < 0 THEN 1 ELSE 0 END) AS losing_trades,
            SUM(CASE WHEN action = 'CLOSE' AND realized_pnl IS NOT NULL THEN realized_pnl ELSE 0 END) AS total_realized_pnl,
            AVG(CASE WHEN action = 'CLOSE' THEN realized_pnl END) AS avg_pnl,
            SUM(CASE WHEN action = 'CLOSE' AND realized_pnl > 0 THEN realized_pnl ELSE 0 END) AS gross_profit,
            ABS(SUM(CASE WHEN action = 'CLOSE' AND realized_pnl < 0 THEN realized_pnl ELSE 0 END)) AS gross_loss_abs,
            AVG(CASE WHEN action = 'CLOSE' THEN r_multiple END) AS avg_r_multiple
        FROM trade_fills
        WHERE COALESCE(account_id, '') != 'backfill'
          AND COALESCE(initiator_type, '') != 'SHADOW'
        GROUP BY substr(timestamp_utc, 1, 10)
        """
    ).fetchall()
    conn.execute("DELETE FROM admin_profitability_daily_summary WHERE account_scope = ?", (LIVE_SCOPE,))
    for row in daily_rows:
        closed = int(row["closed_trades"] or 0)
        wins = int(row["winning_trades"] or 0)
        losses = int(row["losing_trades"] or 0)
        conn.execute(
            """
            INSERT INTO admin_profitability_daily_summary (
                date, account_scope, fills_count, closed_trades, winning_trades, losing_trades,
                total_realized_pnl, avg_pnl, win_rate, profit_factor, avg_r_multiple,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["date"],
                LIVE_SCOPE,
                int(row["fills_count"] or 0),
                closed,
                wins,
                losses,
                float(row["total_realized_pnl"] or 0.0),
                safe_float(row["avg_pnl"]),
                pct(wins, wins + losses),
                profit_factor(row["gross_profit"], row["gross_loss_abs"]),
                safe_float(row["avg_r_multiple"]),
                now,
                now,
            ),
        )

    symbol_rows = conn.execute(
        """
        SELECT
            COALESCE(symbol, 'UNKNOWN') AS symbol,
            COUNT(*) AS fills_count,
            SUM(CASE WHEN action = 'CLOSE' THEN 1 ELSE 0 END) AS closed_trades,
            SUM(CASE WHEN action = 'CLOSE' AND realized_pnl > 0 THEN 1 ELSE 0 END) AS winning_trades,
            SUM(CASE WHEN action = 'CLOSE' AND realized_pnl < 0 THEN 1 ELSE 0 END) AS losing_trades,
            SUM(CASE WHEN action = 'CLOSE' AND realized_pnl IS NOT NULL THEN realized_pnl ELSE 0 END) AS total_realized_pnl,
            AVG(CASE WHEN action = 'CLOSE' THEN realized_pnl END) AS avg_pnl,
            AVG(CASE WHEN action = 'CLOSE' THEN r_multiple END) AS avg_r_multiple,
            SUM(CASE WHEN action = 'CLOSE' AND (
                UPPER(COALESCE(exit_reason, trigger_source, '')) LIKE '%SL%'
                OR UPPER(COALESCE(exit_reason, trigger_source, '')) LIKE '%STOP%'
            ) THEN 1 ELSE 0 END) AS sl_count,
            SUM(CASE WHEN action = 'CLOSE' AND (
                UPPER(COALESCE(exit_reason, trigger_source, '')) LIKE '%TP%'
                OR UPPER(COALESCE(exit_reason, trigger_source, '')) LIKE '%TAKE_PROFIT%'
            ) THEN 1 ELSE 0 END) AS tp_count,
            SUM(CASE WHEN action = 'CLOSE' AND UPPER(COALESCE(exit_reason, trigger_source, '')) LIKE '%TIME%' THEN 1 ELSE 0 END) AS time_exit_count,
            SUM(CASE WHEN action = 'CLOSE' AND NOT (
                UPPER(COALESCE(exit_reason, trigger_source, '')) LIKE '%SL%'
                OR UPPER(COALESCE(exit_reason, trigger_source, '')) LIKE '%STOP%'
                OR UPPER(COALESCE(exit_reason, trigger_source, '')) LIKE '%TP%'
                OR UPPER(COALESCE(exit_reason, trigger_source, '')) LIKE '%TAKE_PROFIT%'
                OR UPPER(COALESCE(exit_reason, trigger_source, '')) LIKE '%TIME%'
            ) THEN 1 ELSE 0 END) AS other_exit_count
        FROM trade_fills
        WHERE COALESCE(account_id, '') != 'backfill'
          AND COALESCE(initiator_type, '') != 'SHADOW'
        GROUP BY COALESCE(symbol, 'UNKNOWN')
        """
    ).fetchall()
    conn.execute("DELETE FROM admin_profitability_symbol_summary WHERE account_scope = ?", (LIVE_SCOPE,))
    for row in symbol_rows:
        wins = int(row["winning_trades"] or 0)
        losses = int(row["losing_trades"] or 0)
        conn.execute(
            """
            INSERT INTO admin_profitability_symbol_summary (
                symbol, account_scope, fills_count, closed_trades, total_realized_pnl,
                avg_pnl, win_rate, avg_r_multiple, sl_count, tp_count, time_exit_count,
                other_exit_count, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["symbol"],
                LIVE_SCOPE,
                int(row["fills_count"] or 0),
                int(row["closed_trades"] or 0),
                float(row["total_realized_pnl"] or 0.0),
                safe_float(row["avg_pnl"]),
                pct(wins, wins + losses),
                safe_float(row["avg_r_multiple"]),
                int(row["sl_count"] or 0),
                int(row["tp_count"] or 0),
                int(row["time_exit_count"] or 0),
                int(row["other_exit_count"] or 0),
                now,
                now,
            ),
        )

    total_rows = len(daily_rows) + len(symbol_rows)
    log_result("profitability_summaries", started, total_rows, f"daily={len(daily_rows)} symbols={len(symbol_rows)}")
    return total_rows


def refresh_sizing_events(conn: sqlite3.Connection) -> int:
    started = time.perf_counter()
    now = utc_now_iso()
    rows = conn.execute(
        """
        SELECT trace_id, run_id, cycle_id, symbol, ts, sizing_json
        FROM decision_traces
        WHERE sizing_json IS NOT NULL
          AND sizing_json != ''
          AND sizing_json LIKE '%"cap_applied"%'
        ORDER BY ts DESC
        """
    ).fetchall()
    conn.execute("DELETE FROM admin_profitability_sizing_events")
    inserted = 0
    for row in rows:
        sizing = json_loads_object(row["sizing_json"])
        if "cap_applied" not in sizing:
            continue
        trace_id = row["trace_id"]
        event_id = trace_id or f"{row['ts']}:{row['symbol']}:{row['run_id']}:{row['cycle_id']}"
        conn.execute(
            """
            INSERT OR REPLACE INTO admin_profitability_sizing_events (
                id, trace_id, symbol, ts, run_id, cycle_id, sizing_method, configured_margin,
                final_margin, base_notional, final_notional, leverage, cap_applied,
                risk_cap_pct, atr_stop_distance_pct, explanation, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                trace_id,
                row["symbol"],
                row["ts"],
                row["run_id"],
                row["cycle_id"],
                sizing.get("sizing_method") or sizing.get("allocation_type") or sizing.get("allocation_mode"),
                safe_float(
                    sizing.get("user_fixed_margin_usdt")
                    or sizing.get("base_margin_usdt")
                    or sizing.get("base_margin")
                ),
                safe_float(sizing.get("final_margin_usdt") or sizing.get("final_margin")),
                safe_float(sizing.get("base_notional_usdt")),
                safe_float(sizing.get("final_notional_usdt")),
                safe_float(sizing.get("leverage") or sizing.get("leverage_used_for_cap")),
                1 if bool(sizing.get("cap_applied")) else 0,
                safe_float(sizing.get("account_risk_pct") or sizing.get("theoretical_risk_pct")),
                safe_float(sizing.get("stop_distance_pct")),
                sizing.get("cap_reason") or sizing.get("risk_level_label") or sizing.get("risk_level"),
                now,
            ),
        )
        inserted += 1
    log_result("sizing_events", started, inserted, f"source_rows={len(rows)}")
    return inserted


def load_completed_open_fills(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        WITH close_agg AS (
            SELECT
                position_id,
                symbol,
                side,
                SUM(qty) AS close_qty,
                SUM(CASE WHEN realized_pnl IS NOT NULL THEN realized_pnl ELSE 0 END) AS realized_pnl_sum,
                SUM(CASE WHEN realized_pnl IS NOT NULL THEN 1 ELSE 0 END) AS realized_pnl_count,
                AVG(r_multiple) AS avg_r_multiple,
                MAX(timestamp_utc) AS close_timestamp_utc,
                MAX(trace_id) AS close_trace_id
            FROM trade_fills
            WHERE action = 'CLOSE'
              AND position_id IS NOT NULL
              AND COALESCE(account_id, '') != 'backfill'
              AND COALESCE(initiator_type, '') != 'SHADOW'
            GROUP BY position_id, symbol, side
        )
        SELECT
            of.id AS open_fill_id,
            of.run_id,
            of.cycle_id,
            of.symbol,
            of.side,
            of.qty AS open_qty,
            of.price AS open_price,
            of.timestamp_utc AS open_timestamp_utc,
            of.position_id,
            of.trace_id AS fill_trace_id,
            ca.close_qty,
            ca.realized_pnl_sum,
            ca.realized_pnl_count,
            ca.avg_r_multiple,
            ca.close_timestamp_utc,
            ca.close_trace_id
        FROM trade_fills of
        JOIN close_agg ca
          ON ca.position_id = of.position_id
         AND ca.symbol = of.symbol
         AND ca.side = of.side
        WHERE of.action = 'OPEN'
          AND of.position_id IS NOT NULL
          AND COALESCE(of.account_id, '') != 'backfill'
          AND COALESCE(of.initiator_type, '') != 'SHADOW'
          AND ca.close_qty >= of.qty * 0.99
        ORDER BY of.timestamp_utc ASC, of.id ASC
        """
    ).fetchall()


def find_trace_for_open(conn: sqlite3.Connection, row: sqlite3.Row) -> tuple[dict[str, Any] | None, str, int, str | None, str]:
    columns = ", ".join(TRACE_COLUMNS)
    trace_match_count = 0
    if row["fill_trace_id"]:
        trace = conn.execute(
            f"SELECT {columns} FROM decision_traces WHERE trace_id = ?",
            (row["fill_trace_id"],),
        ).fetchone()
        if trace:
            return dict(trace), "trace_id", 1, None, "trade_fills.trace_id"
    if row["position_id"]:
        trace_match_count = int(
            conn.execute(
                "SELECT COUNT(*) AS c FROM decision_traces WHERE position_id = ?",
                (row["position_id"],),
            ).fetchone()["c"]
            or 0
        )
        trace = conn.execute(
            f"""
            SELECT {columns}
            FROM decision_traces
            WHERE position_id = ?
            ORDER BY ts DESC
            LIMIT 1
            """,
            (row["position_id"],),
        ).fetchone()
        if trace:
            return dict(trace), "position_id", trace_match_count, None, "decision_traces.position_id"
    if row["run_id"] and row["cycle_id"] and row["symbol"]:
        trace_match_count = int(
            conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM decision_traces
                WHERE run_id = ?
                  AND cycle_id = ?
                  AND symbol = ?
                """,
                (row["run_id"], row["cycle_id"], row["symbol"]),
            ).fetchone()["c"]
            or 0
        )
        trace = conn.execute(
            f"""
            SELECT {columns}
            FROM decision_traces
            WHERE run_id = ?
              AND cycle_id = ?
              AND symbol = ?
            ORDER BY ts DESC
            LIMIT 1
            """,
            (row["run_id"], row["cycle_id"], row["symbol"]),
        ).fetchone()
        if trace:
            return dict(trace), "run_cycle_symbol", trace_match_count, None, "decision_traces.run_id_cycle_id_symbol"

    reasons: list[str] = []
    if not row["run_id"] or not row["cycle_id"]:
        reasons.append("missing_run_cycle_metadata")
    if row["position_id"] and trace_match_count == 0:
        reasons.append("no_matching_decision_trace_position_id")
    if not row["position_id"]:
        reasons.append("missing_position_id")
    if reasons == ["missing_run_cycle_metadata", "no_matching_decision_trace_position_id"]:
        reasons.append("old_tracing_gap")
    return None, "unlinked", trace_match_count, "|".join(reasons or ["no_matching_decision_trace"]), "staged_trace_lookup"


def feature_payload(data: dict[str, Any]) -> dict[str, Any]:
    if not ML_FEATURE_COLUMNS or compute_feature_value is None:
        keys = (
            "ml_score",
            "ml_action",
            "regime_state",
            "confidence",
            "adx",
            "atr_pct",
            "ma_slope",
            "compression_ratio",
            "breakout_pressure",
            "buy_score",
            "sell_score",
            "threshold",
            "portfolio_risk_used",
            "open_positions_count",
            "margin_level",
        )
        return {key: data.get(key) for key in keys}
    payload: dict[str, Any] = {}
    for feature_name in ML_FEATURE_COLUMNS:
        try:
            payload[feature_name] = compute_feature_value(data, feature_name)
        except Exception:
            payload[feature_name] = None
    return payload


def refresh_ml_linked_trade_snapshot(conn: sqlite3.Connection) -> tuple[int, Counter[str], Counter[str]]:
    started = time.perf_counter()
    now = utc_now_iso()
    open_rows = load_completed_open_fills(conn)
    conn.execute("DELETE FROM admin_ml_linked_trade_snapshot")
    branches: Counter[str] = Counter()
    unlinked_reasons: Counter[str] = Counter()
    inserted = 0
    for row in open_rows:
        trace, branch, trace_match_count, unlinked_reason, source_trace_match_basis = find_trace_for_open(conn, row)
        branches[branch] += 1
        if unlinked_reason:
            unlinked_reasons[unlinked_reason] += 1
        trace = trace or {}
        data = {
            **trace,
            "open_fill_id": row["open_fill_id"],
            "open_qty": row["open_qty"],
            "open_price": row["open_price"],
            "open_timestamp_utc": row["open_timestamp_utc"],
            "close_qty": row["close_qty"],
            "close_timestamp_utc": row["close_timestamp_utc"],
            "realized_pnl_sum": row["realized_pnl_sum"],
            "realized_pnl_count": row["realized_pnl_count"],
            "symbol": row["symbol"],
            "side": row["side"],
            "position_id": row["position_id"],
            "run_id": row["run_id"],
            "cycle_id": row["cycle_id"],
        }
        features = feature_payload(data)
        threshold = trace.get("ml_threshold")
        if threshold is None:
            threshold = trace.get("threshold")
        conn.execute(
            """
            INSERT OR REPLACE INTO admin_ml_linked_trade_snapshot (
                id, symbol, position_id, run_id, cycle_id, open_trace_id, close_trace_id,
                open_ts, close_ts, side, realized_pnl, r_multiple, ml_score, ml_action,
                ml_model_version, regime, confidence, threshold, features_json,
                linkage_branch, unlinked_reason, trace_match_count, source_trace_match_basis,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(row["open_fill_id"]),
                row["symbol"],
                row["position_id"],
                row["run_id"],
                row["cycle_id"],
                trace.get("trace_id"),
                row["close_trace_id"],
                row["open_timestamp_utc"],
                row["close_timestamp_utc"],
                row["side"],
                safe_float(row["realized_pnl_sum"]),
                safe_float(row["avg_r_multiple"]),
                safe_float(trace.get("ml_score")),
                trace.get("ml_action"),
                trace.get("ml_model_version"),
                trace.get("regime_state"),
                safe_float(trace.get("confidence")),
                safe_float(threshold),
                json.dumps(features, sort_keys=True),
                branch,
                unlinked_reason,
                int(trace_match_count or 0),
                source_trace_match_basis,
                now,
                now,
            ),
        )
        inserted += 1
    log_result("ml_linked_trade_snapshot", started, inserted, f"branches={dict(branches)} unlinked_reasons={dict(unlinked_reasons)}")
    return inserted, branches, unlinked_reasons


def refresh_ml_feature_completeness(conn: sqlite3.Connection) -> int:
    started = time.perf_counter()
    generated_at = utc_now_iso()
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT id, open_ts, features_json
            FROM admin_ml_linked_trade_snapshot
            WHERE open_trace_id IS NOT NULL
            ORDER BY open_ts DESC
            """
        ).fetchall()
    ]
    parsed = [json_loads_object(row["features_json"]) for row in rows]
    recent = parsed[:RECENT_FEATURE_LIMIT]
    feature_names = list(ML_FEATURE_COLUMNS) if ML_FEATURE_COLUMNS else sorted({k for item in parsed for k in item})

    conn.execute("DELETE FROM admin_ml_feature_completeness_snapshot")
    for feature_name in feature_names:
        non_null = sum(1 for item in parsed if item.get(feature_name) is not None)
        recent_non_null = sum(1 for item in recent if item.get(feature_name) is not None)
        total = len(parsed)
        recent_total = len(recent)
        last_seen_populated_at = None
        for row, item in zip(rows, parsed):
            if item.get(feature_name) is not None:
                last_seen_populated_at = row["open_ts"]
                break
        if total and non_null == 0:
            frontend_status = "broken"
        elif recent_total and recent_non_null == 0:
            frontend_status = "broken"
        elif non_null < total or recent_non_null < recent_total:
            frontend_status = "partially_missing"
        else:
            frontend_status = "healthy"
        conn.execute(
            """
            INSERT INTO admin_ml_feature_completeness_snapshot (
                id, scope, feature_name, total_rows, non_null_rows, null_rows,
                completeness_pct, recent_total_rows, recent_non_null_rows,
                recent_completeness_pct, last_seen_populated_at, frontend_status,
                recent_window_basis, recent_window_limit, generated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"lifetime:{feature_name}",
                "lifetime",
                feature_name,
                total,
                non_null,
                total - non_null,
                pct(non_null, total),
                recent_total,
                recent_non_null,
                pct(recent_non_null, recent_total),
                last_seen_populated_at,
                frontend_status,
                f"last_{RECENT_FEATURE_LIMIT}_linked_completed_trades",
                RECENT_FEATURE_LIMIT,
                generated_at,
            ),
        )
    log_result("ml_feature_completeness_snapshot", started, len(feature_names), f"linked_rows={len(rows)}")
    return len(feature_names)


def score_band(value: Any) -> str | None:
    score = safe_float(value)
    if score is None:
        return None
    low = max(0, min(9, int(score * 10))) / 10
    high = low + 0.1
    return f"{low:.1f}-{high:.1f}"


def drift_stats(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        value = row.get(key)
        if value is None:
            continue
        groups[str(value)].append(row)
    out: list[dict[str, Any]] = []
    for group_key, group_rows in groups.items():
        pnls = [safe_float(row.get("realized_pnl")) for row in group_rows]
        pnl_values = [value for value in pnls if value is not None]
        non_flat = [value for value in pnl_values if abs(value) > 1e-9]
        wins = sum(1 for value in non_flat if value > 0)
        r_values = [safe_float(row.get("r_multiple")) for row in group_rows]
        r_values = [value for value in r_values if value is not None]
        out.append(
            {
                "key": group_key,
                "sample_count": len(group_rows),
                "win_rate": pct(wins, len(non_flat)),
                "avg_pnl": round(sum(pnl_values) / len(pnl_values), 6) if pnl_values else None,
                "avg_r_multiple": round(sum(r_values) / len(r_values), 6) if r_values else None,
            }
        )
    out.sort(key=lambda item: item["sample_count"], reverse=True)
    return out


def refresh_ml_drift_snapshot(conn: sqlite3.Connection) -> int:
    started = time.perf_counter()
    generated_at = utc_now_iso()
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT symbol, regime, ml_score, realized_pnl, r_multiple, open_ts
            FROM admin_ml_linked_trade_snapshot
            WHERE open_trace_id IS NOT NULL
            """
        ).fetchall()
    ]
    for row in rows:
        row["score_band"] = score_band(row.get("ml_score"))

    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    scoped_rows = {
        "lifetime": rows,
        "recent_30d": [row for row in rows if str(row.get("open_ts") or "") >= cutoff],
    }
    conn.execute("DELETE FROM admin_ml_drift_snapshot")
    inserted = 0
    for scope, scope_rows in scoped_rows.items():
        for key in ("symbol", "regime", "score_band"):
            for item in drift_stats(scope_rows, key):
                conn.execute(
                    """
                    INSERT INTO admin_ml_drift_snapshot (
                        id, scope, symbol, regime, score_band, sample_count,
                        win_rate, avg_pnl, avg_r_multiple, generated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"{scope}:{key}:{item['key']}",
                        scope,
                        item["key"] if key == "symbol" else None,
                        item["key"] if key == "regime" else None,
                        item["key"] if key == "score_band" else None,
                        item["sample_count"],
                        item["win_rate"],
                        item["avg_pnl"],
                        item["avg_r_multiple"],
                        generated_at,
                    ),
                )
                inserted += 1
    log_result("ml_drift_snapshot", started, inserted, f"linked_rows={len(rows)}")
    return inserted


def load_latest_dataset_metadata() -> dict[str, Any] | None:
    dataset_dir = BOT_ROOT / "models" / "datasets"
    if not dataset_dir.exists():
        return None
    files = sorted(dataset_dir.glob("training_*_meta.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    for meta_file in files:
        metadata = json_loads_object(meta_file.read_text(encoding="utf-8"))
        if not metadata:
            continue
        dataset_path = meta_file.with_name(meta_file.name.replace("_meta.json", ".parquet"))
        if not dataset_path.exists() and metadata.get("training_dataset_path"):
            dataset_path = Path(str(metadata["training_dataset_path"])).expanduser()
        metadata.setdefault("training_dataset_path", str(dataset_path) if dataset_path else None)
        if ML_CONTRACT_VERSION:
            metadata.setdefault("contract_version", ML_CONTRACT_VERSION)
        if ML_FEATURE_SCHEMA_HASH:
            metadata.setdefault("schema_hash", ML_FEATURE_SCHEMA_HASH)
        if ML_FEATURE_COLUMNS:
            metadata.setdefault("feature_columns", list(ML_FEATURE_COLUMNS))
        return {"meta_file": str(meta_file), "meta": metadata, "dataset_path": str(dataset_path) if dataset_path else None}
    return None


def latest_runtime_status(conn: sqlite3.Connection) -> dict[str, Any] | None:
    if not table_exists(conn, "ml_runtime_status"):
        return None
    row = conn.execute(
        """
        SELECT *
        FROM ml_runtime_status
        ORDER BY last_update_timestamp DESC
        LIMIT 1
        """
    ).fetchone()
    return dict(row) if row else None


def latest_trace_config(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT ml_model_version, ml_threshold, threshold, ts
        FROM decision_traces
        WHERE ml_model_version IS NOT NULL OR ml_threshold IS NOT NULL OR threshold IS NOT NULL
        ORDER BY ts DESC
        LIMIT 1
        """
    ).fetchone()
    return dict(row) if row else {}


def latest_run_started_at(conn: sqlite3.Connection) -> str | None:
    if not table_exists(conn, "runs"):
        return None
    row = conn.execute("SELECT started_at FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()
    return row["started_at"] if row else None


def snapshot_metadata(
    *,
    generated_at: str,
    source_tables: list[str],
    source_window: str,
    row_counts: dict[str, int],
    warnings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "source_tables": source_tables,
        "source_window": source_window,
        "row_counts": row_counts,
        "stale": False,
        "warnings": warnings or [],
    }


def linked_snapshot_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT *
            FROM admin_ml_linked_trade_snapshot
            ORDER BY open_ts DESC
            """
        ).fetchall()
    ]
    for row in rows:
        row["features"] = json_loads_object(row.get("features_json"))
    return rows


def traced_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("open_trace_id")]


def pnl_value(row: dict[str, Any]) -> float | None:
    return safe_float(row.get("realized_pnl"))


def r_value(row: dict[str, Any]) -> float | None:
    return safe_float(row.get("r_multiple"))


def summarize_feature_payload(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT *
            FROM admin_ml_feature_completeness_snapshot
            ORDER BY feature_name
            """
        ).fetchall()
    ]
    features = []
    for row in rows:
        total = int(row["total_rows"] or 0)
        recent_total = int(row["recent_total_rows"] or 0)
        null_lifetime = int(row["null_rows"] or 0)
        null_recent = recent_total - int(row["recent_non_null_rows"] or 0)
        features.append(
            {
                "feature_name": row["feature_name"],
                "null_count_recent": max(null_recent, 0),
                "null_pct_recent": pct(max(null_recent, 0), recent_total) or 0.0,
                "null_count_lifetime": max(null_lifetime, 0),
                "null_pct_lifetime": pct(max(null_lifetime, 0), total) or 0.0,
                "last_seen_populated_at": row["last_seen_populated_at"],
                "status": row["frontend_status"] or "broken",
            }
        )
    recent_total_cells = sum(int(row["recent_total_rows"] or 0) for row in rows)
    recent_non_null_cells = sum(int(row["recent_non_null_rows"] or 0) for row in rows)
    lifetime_total_cells = sum(int(row["total_rows"] or 0) for row in rows)
    lifetime_non_null_cells = sum(int(row["non_null_rows"] or 0) for row in rows)
    broken_count = sum(1 for item in features if item["status"] == "broken")
    partial_count = sum(1 for item in features if item["status"] == "partially_missing")
    return {
        "recent_window_size": int(rows[0]["recent_total_rows"] or 0) if rows else 0,
        "recent_window_basis": rows[0]["recent_window_basis"] if rows else f"last_{RECENT_FEATURE_LIMIT}_linked_completed_trades",
        "recent_completeness_pct": pct(recent_non_null_cells, recent_total_cells) or 0.0,
        "lifetime_completeness_pct": pct(lifetime_non_null_cells, lifetime_total_cells) or 0.0,
        "features": features,
        "broken_feature_count": broken_count,
        "partially_missing_feature_count": partial_count,
    }


def build_linkage_health(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> dict[str, Any]:
    live_filter = "WHERE COALESCE(account_id, '') != 'backfill' AND COALESCE(initiator_type, '') != 'SHADOW'"
    total_post_fix_fills = int(conn.execute(f"SELECT COUNT(*) AS c FROM trade_fills {live_filter}").fetchone()["c"] or 0)
    fills_with_run = int(conn.execute(f"SELECT COUNT(*) AS c FROM trade_fills {live_filter} AND run_id IS NOT NULL").fetchone()["c"] or 0)
    fills_with_cycle = int(conn.execute(f"SELECT COUNT(*) AS c FROM trade_fills {live_filter} AND cycle_id IS NOT NULL").fetchone()["c"] or 0)
    fills_with_position = int(conn.execute(f"SELECT COUNT(*) AS c FROM trade_fills {live_filter} AND position_id IS NOT NULL").fetchone()["c"] or 0)
    post_fix_start = conn.execute(f"SELECT MIN(timestamp_utc) AS ts FROM trade_fills {live_filter}").fetchone()["ts"]
    open_count = int(conn.execute(f"SELECT COUNT(*) AS c FROM trade_fills {live_filter} AND action = 'OPEN'").fetchone()["c"] or 0)
    close_count = int(conn.execute(f"SELECT COUNT(*) AS c FROM trade_fills {live_filter} AND action = 'CLOSE'").fetchone()["c"] or 0)
    unmatched_close_fills = int(
        conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM trade_fills tf_close
            {live_filter}
              AND action = 'CLOSE'
              AND NOT EXISTS (
                  SELECT 1
                  FROM trade_fills tf_open
                  WHERE tf_open.action = 'OPEN'
                    AND tf_open.position_id = tf_close.position_id
              )
            """
        ).fetchone()["c"]
        or 0
    )
    linked_completed = len([row for row in rows if row.get("open_trace_id")])
    completed = len(rows)
    unlinked = completed - linked_completed
    orphan_open_fills = max(open_count - completed, 0)
    fully_linked_pct = pct(linked_completed, completed) or 0.0
    run_cov = pct(fills_with_run, total_post_fix_fills) or 0.0
    cycle_cov = pct(fills_with_cycle, total_post_fix_fills) or 0.0
    position_cov = pct(fills_with_position, total_post_fix_fills) or 0.0
    unlinked_rows = [row for row in rows if not row.get("open_trace_id")]
    reason_counts = Counter(row.get("unlinked_reason") or "unknown" for row in unlinked_rows)
    symbol_counts = Counter(row.get("symbol") or "UNKNOWN" for row in unlinked_rows)
    timestamp_values = [row.get("open_ts") for row in unlinked_rows if row.get("open_ts")]
    linkage_healthy = (
        completed > 0
        and run_cov >= MIN_LINKAGE_HEALTH_PCT
        and cycle_cov >= MIN_LINKAGE_HEALTH_PCT
        and position_cov >= MIN_LINKAGE_HEALTH_PCT
        and fully_linked_pct >= MIN_LINKAGE_HEALTH_PCT
        and unmatched_close_fills == 0
    )
    return {
        "post_fix_start": post_fix_start,
        "total_post_fix_fills": total_post_fix_fills,
        "fills_with_non_null_run_id": fills_with_run,
        "fills_with_non_null_cycle_id": fills_with_cycle,
        "fills_with_non_null_position_id": fills_with_position,
        "run_id_coverage_pct": run_cov,
        "cycle_id_coverage_pct": cycle_cov,
        "position_id_coverage_pct": position_cov,
        "fully_linked_completed_trades": linked_completed,
        "fully_linked_completed_trades_pct": fully_linked_pct,
        "completed_trade_rows": completed,
        "unlinked_completed_trades": unlinked,
        "orphan_open_fills": orphan_open_fills,
        "unmatched_close_fills": unmatched_close_fills,
        "open_fill_count": open_count,
        "close_fill_count": close_count,
        "unlinked_reason_counts": dict(reason_counts),
        "unlinked_top_symbols": [{"symbol": key, "count": value} for key, value in symbol_counts.most_common(10)],
        "unlinked_timestamp_range": {
            "min_open_ts": min(timestamp_values) if timestamp_values else None,
            "max_open_ts": max(timestamp_values) if timestamp_values else None,
        },
        "linkage_healthy": linkage_healthy,
        "scope": "live_only_snapshot_excluding_backfill_shadow",
    }


def build_training_gate(rows: list[dict[str, Any]], feature: dict[str, Any], linkage: dict[str, Any]) -> dict[str, Any]:
    linked_rows = traced_rows(rows)
    wins = sum(1 for row in linked_rows if (pnl_value(row) or 0.0) > 0)
    losses = sum(1 for row in linked_rows if (pnl_value(row) or 0.0) < 0)
    breakevens = sum(1 for row in linked_rows if abs(pnl_value(row) or 0.0) <= 1e-9)
    feature_names = list(ML_FEATURE_COLUMNS) if ML_FEATURE_COLUMNS else []
    full_feature_coverage = 0
    missing_critical = 0
    critical_feature_broken = feature.get("broken_feature_count", 0) > 0
    for row in linked_rows:
        features = row.get("features") or {}
        if feature_names and all(features.get(name) is not None for name in feature_names):
            full_feature_coverage += 1
        elif not feature_names:
            full_feature_coverage += 1
        else:
            missing_critical += 1
    total = len(linked_rows)
    feature_coverage_pct = pct(full_feature_coverage, total) or 0.0
    current_win_rate = pct(wins, wins + losses) or 0.0
    single_class = wins == 0 or losses == 0
    linkage_healthy = bool(linkage["linkage_healthy"])
    training_ready = (
        total >= REQUIRED_TRADES
        and wins >= REQUIRED_WINS
        and feature_coverage_pct >= MIN_FEATURE_COVERAGE_PCT
        and linkage_healthy
        and not critical_feature_broken
        and not single_class
    )
    status = gate_status(
        total_linked_completed_trades=total,
        wins=wins,
        linkage_healthy=linkage_healthy,
        critical_feature_broken=critical_feature_broken,
        label_distribution_single_class=single_class,
        feature_coverage_pct=feature_coverage_pct,
        training_ready=training_ready,
    )
    blocking_reasons = []
    if total < REQUIRED_TRADES:
        blocking_reasons.append("insufficient_linked_completed_trades")
    if wins < REQUIRED_WINS:
        blocking_reasons.append("insufficient_winning_trades")
    if feature_coverage_pct < MIN_FEATURE_COVERAGE_PCT:
        blocking_reasons.append("feature_coverage_below_threshold")
    if not linkage_healthy:
        blocking_reasons.append("linkage_health_below_threshold")
    if critical_feature_broken:
        blocking_reasons.append("critical_features_broken")
    if single_class:
        blocking_reasons.append("single_class_labels")
    return {
        "total_linked_completed_trades": total,
        "required_trades": REQUIRED_TRADES,
        "wins": wins,
        "required_wins": REQUIRED_WINS,
        "losses": losses,
        "breakeven_trades": breakevens,
        "excluded_open_positions": max(linkage.get("open_fill_count", 0) - linkage.get("completed_trade_rows", 0), 0),
        "trades_with_full_feature_coverage": full_feature_coverage,
        "trades_missing_critical_features": missing_critical,
        "current_win_rate": current_win_rate,
        "feature_coverage_pct": feature_coverage_pct,
        "linkage_healthy": linkage_healthy,
        "label_distribution_single_class": single_class,
        "training_ready": training_ready,
        "status": status,
        "blocking_reasons": blocking_reasons,
        "linkage_warnings": {
            "unlinked_completed_trades": linkage.get("unlinked_completed_trades", 0),
            "unlinked_reason_counts": linkage.get("unlinked_reason_counts", {}),
        },
    }


def build_overview(conn: sqlite3.Connection, gate: dict[str, Any], generated_at: str) -> dict[str, Any]:
    runtime = latest_runtime_status(conn) or {}
    trace_config = latest_trace_config(conn)
    enabled = bool(runtime.get("enabled", 0))
    shadow_mode = bool(runtime.get("shadow_mode", 0))
    mode = ml_mode(enabled=enabled, shadow_mode=shadow_mode)
    current_model = runtime.get("model_version") or trace_config.get("ml_model_version")
    current_threshold = safe_float(runtime.get("threshold") or trace_config.get("ml_threshold") or trace_config.get("threshold"))
    schema_hash = runtime.get("schema_hash") or ML_FEATURE_SCHEMA_HASH
    contract_version = runtime.get("contract_version") or ML_CONTRACT_VERSION
    schema_compatible = bool(schema_hash and ML_FEATURE_SCHEMA_HASH and schema_hash == ML_FEATURE_SCHEMA_HASH)
    return {
        "ml_enabled": enabled,
        "ml_mode": mode,
        "current_model_version": current_model,
        "current_threshold": current_threshold,
        "current_hard_block_floor": safe_float(runtime.get("hard_block_floor")),
        "model_artifact_path": runtime.get("model_path"),
        "encoder_path": runtime.get("encoders_path"),
        "metadata_path": runtime.get("metadata_path"),
        "last_model_load_time": runtime.get("last_update_timestamp"),
        "last_bot_restart_time": latest_run_started_at(conn),
        "current_ml_status": current_ml_status(gate, mode, current_model),
        "runtime_loaded": bool(runtime.get("loaded", 0)),
        "runtime_load_error": runtime.get("load_error"),
        "last_successful_score_timestamp": runtime.get("last_score_timestamp") or trace_config.get("ts"),
        "contract_version": contract_version,
        "schema_hash": schema_hash,
        "schema_compatible": schema_compatible,
        "generated_at": generated_at,
        "snapshot_warning": None if runtime else "No ml_runtime_status row found; overview uses trace/default fallbacks.",
    }


def build_activity(conn: sqlite3.Connection, overview: dict[str, Any], days: int = DEFAULT_ACTIVITY_DAYS) -> dict[str, Any]:
    window_start = f"-{days} days"
    summary_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT ts, symbol, regime_state, signal, ml_score, ml_action, ml_model_version,
                   COALESCE(ml_threshold, threshold) AS threshold
            FROM decision_traces
            WHERE ml_action IS NOT NULL
              AND ts >= datetime('now', ?)
            ORDER BY ts DESC
            LIMIT ?
            """,
            (window_start, DEFAULT_ACTIVITY_SNAPSHOT_ROWS),
        ).fetchall()
    ]
    total_recent_rows = int(
        conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM decision_traces
            WHERE ml_action IS NOT NULL
              AND ts >= datetime('now', ?)
            """,
            (window_start,),
        ).fetchone()["c"]
        or 0
    )
    score_distribution = {bucket: 0 for bucket in score_buckets()}
    per_symbol: dict[str, dict[str, int]] = {}
    per_regime: dict[str, dict[str, int]] = {}
    per_session: dict[str, dict[str, int]] = {}
    allow_count = shadow_count = block_count = skip_count = total_ml_scored = 0
    score_sum = 0.0
    recent_activity_rows: list[dict[str, Any]] = []
    for row in summary_rows:
        action = (row.get("ml_action") or "SKIP").upper()
        score = safe_float(row.get("ml_score"))
        session = session_for_timestamp(row.get("ts"))
        if action == "ALLOW":
            allow_count += 1
        elif action == "SHADOW":
            shadow_count += 1
        elif action == "BLOCK":
            block_count += 1
        else:
            skip_count += 1
        if score is not None:
            total_ml_scored += 1
            score_sum += score
            bucket = score_band(score)
            if bucket:
                score_distribution[bucket] += 1
        bump_action_counter(per_symbol, row.get("symbol") or "UNKNOWN", action)
        bump_action_counter(per_regime, row.get("regime_state") or "UNKNOWN", action)
        bump_action_counter(per_session, session, action)
        recent_activity_rows.append(
            {
                "timestamp": row.get("ts"),
                "symbol": row.get("symbol"),
                "side": "LONG" if str(row.get("signal") or "").upper() in {"BUY", "LONG"} else ("SHORT" if str(row.get("signal") or "").upper() in {"SELL", "SHORT"} else None),
                "ml_score": score,
                "ml_action": row.get("ml_action"),
                "ml_model_version": row.get("ml_model_version"),
                "threshold": safe_float(row.get("threshold")),
                "regime": row.get("regime_state"),
                "session": session,
                "linkage_status": "decision_trace_only_snapshot",
            }
        )
    return {
        "window_days": days,
        "page": 1,
        "page_size": DEFAULT_ACTIVITY_PAGE_SIZE,
        "total_recent_rows": total_recent_rows,
        "total_ml_scored_entries": total_ml_scored,
        "allow_count": allow_count,
        "shadow_count": shadow_count,
        "block_count": block_count,
        "skip_count": skip_count,
        "average_ml_score": round(score_sum / total_ml_scored, 4) if total_ml_scored else None,
        "current_threshold": overview.get("current_threshold"),
        "current_hard_floor": overview.get("current_hard_block_floor"),
        "score_distribution": [{"bucket": bucket, "count": count} for bucket, count in score_distribution.items()],
        "per_symbol_actions": flatten_action_counters(per_symbol),
        "per_regime_actions": flatten_action_counters(per_regime),
        "per_session_actions": flatten_action_counters(per_session),
        "recent_activity_rows": recent_activity_rows[:DEFAULT_ACTIVITY_PAGE_SIZE],
        "snapshot_rows_retained": len(recent_activity_rows),
        "source_window": f"last_{days}_days",
    }


def empty_group_stats() -> dict[str, Any]:
    return {"count": 0, "wins": 0, "losses": 0, "breakevens": 0, "total_pnl": 0.0, "average_pnl": 0.0}


def build_shadow_performance(rows: list[dict[str, Any]], days: int = DEFAULT_SHADOW_DAYS) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    groups = {"ALLOW": empty_group_stats(), "SHADOW": empty_group_stats(), "BLOCK": empty_group_stats()}
    total = good_allows = bad_allows = good_blocks = bad_blocks = 0
    symbol_breakdown: dict[str, dict[str, Any]] = {}
    action_regime: dict[str, dict[str, Any]] = {}
    score_band_perf: dict[str, dict[str, Any]] = {bucket: {"count": 0, "pnl_sum": 0.0, "wins": 0, "losses": 0} for bucket in score_buckets()}
    for row in traced_rows(rows):
        parsed = parse_dt(row.get("open_ts"))
        if parsed and parsed < cutoff:
            continue
        action = (row.get("ml_action") or "").upper()
        if action not in groups:
            continue
        pnl = pnl_value(row)
        total += 1
        stats = groups[action]
        stats["count"] += 1
        if pnl is None or abs(pnl) < 1e-9:
            stats["breakevens"] += 1
        elif pnl > 0:
            stats["wins"] += 1
        else:
            stats["losses"] += 1
        stats["total_pnl"] += float(pnl or 0.0)
        if action == "ALLOW":
            good_allows += 1 if pnl is not None and pnl > 0 else 0
            bad_allows += 1 if pnl is not None and pnl < 0 else 0
        else:
            good_blocks += 1 if pnl is not None and pnl < 0 else 0
            bad_blocks += 1 if pnl is not None and pnl > 0 else 0
        symbol = row.get("symbol") or "UNKNOWN"
        sym = symbol_breakdown.setdefault(symbol, {"count": 0, "total_pnl": 0.0})
        sym["count"] += 1
        sym["total_pnl"] += float(pnl or 0.0)
        regime_key = f"{action}:{row.get('regime') or 'UNKNOWN'}"
        ar = action_regime.setdefault(regime_key, {"count": 0, "total_pnl": 0.0})
        ar["count"] += 1
        ar["total_pnl"] += float(pnl or 0.0)
        bucket = score_band(row.get("ml_score"))
        if bucket:
            score_band_perf[bucket]["count"] += 1
            score_band_perf[bucket]["pnl_sum"] += float(pnl or 0.0)
            score_band_perf[bucket]["wins"] += 1 if pnl is not None and pnl > 0 else 0
            score_band_perf[bucket]["losses"] += 1 if pnl is not None and pnl < 0 else 0
    for stats in groups.values():
        stats["average_pnl"] = round(stats["total_pnl"] / stats["count"], 4) if stats["count"] else 0.0
        stats["total_pnl"] = round(stats["total_pnl"], 4)
    return {
        "window_days": days,
        "total_linked_completed_trades_with_ml_attribution": total,
        "decision_groups": groups,
        "good_allows": good_allows,
        "bad_allows": bad_allows,
        "good_blocks": good_blocks,
        "bad_blocks": bad_blocks,
        "classification_logic": "good_allows = ALLOW rows with positive realized pnl; bad_allows = ALLOW rows with negative realized pnl; good_blocks = SHADOW or BLOCK rows with negative realized pnl; bad_blocks = SHADOW or BLOCK rows with positive realized pnl.",
        "linked_trade_count": len(traced_rows(rows)),
        "win_rate": pct(sum(1 for row in traced_rows(rows) if (pnl_value(row) or 0.0) > 0), len([row for row in traced_rows(rows) if abs(pnl_value(row) or 0.0) > 1e-9])),
        "total_pnl": round(sum(pnl_value(row) or 0.0 for row in traced_rows(rows)), 6),
        "avg_r_multiple": round(sum(r_value(row) or 0.0 for row in traced_rows(rows) if r_value(row) is not None) / max(sum(1 for row in traced_rows(rows) if r_value(row) is not None), 1), 6),
        "symbol_breakdown": [{"symbol": key, **value} for key, value in sorted(symbol_breakdown.items(), key=lambda item: item[1]["count"], reverse=True)],
        "action_regime_performance": [{"key": key, **value} for key, value in sorted(action_regime.items(), key=lambda item: item[1]["count"], reverse=True)],
        "score_band_performance": [
            {
                "bucket": bucket,
                "count": int(value["count"]),
                "average_pnl": round(value["pnl_sum"] / value["count"], 4) if value["count"] else 0.0,
                "win_rate": pct(value["wins"], value["wins"] + value["losses"]),
            }
            for bucket, value in score_band_perf.items()
        ],
    }


def build_drift_monitoring(conn: sqlite3.Connection, rows: list[dict[str, Any]], days: int = DEFAULT_DRIFT_WINDOW_DAYS) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    linked_rows = traced_rows(rows)
    recent_rows = [row for row in linked_rows if not parse_dt(row.get("open_ts")) or parse_dt(row.get("open_ts")) >= cutoff]
    recent_non_flat = [row for row in recent_rows if abs(pnl_value(row) or 0.0) > 1e-9]
    historical_non_flat = [row for row in linked_rows if abs(pnl_value(row) or 0.0) > 1e-9]
    recent_wins = sum(1 for row in recent_non_flat if (pnl_value(row) or 0.0) > 0)
    historical_wins = sum(1 for row in historical_non_flat if (pnl_value(row) or 0.0) > 0)
    live_win_rate = pct(recent_wins, len(recent_non_flat)) if recent_non_flat else None
    historical_win_rate = pct(historical_wins, len(historical_non_flat)) if historical_non_flat else None
    live_score_rows = conn.execute(
        """
        SELECT ml_score
        FROM decision_traces
        WHERE ml_score IS NOT NULL
          AND ts >= datetime('now', ?)
        ORDER BY ts DESC
        LIMIT 5000
        """,
        (f"-{days} days",),
    ).fetchall()
    historical_score_rows = conn.execute(
        """
        SELECT ml_score
        FROM decision_traces
        WHERE ml_score IS NOT NULL
        ORDER BY ts DESC
        LIMIT 5000
        """
    ).fetchall()

    def score_distribution(source_rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
        counts = {bucket: 0 for bucket in score_buckets()}
        for row in source_rows:
            bucket = score_band(row["ml_score"])
            if bucket:
                counts[bucket] += 1
        return [{"bucket": bucket, "count": count} for bucket, count in counts.items()]

    def distribution_with_pnl(source_rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, float]] = {}
        for row in source_rows:
            group_key = row.get(key) or "UNKNOWN"
            stats = grouped.setdefault(str(group_key), {"count": 0, "pnl_sum": 0.0})
            stats["count"] += 1
            stats["pnl_sum"] += float(pnl_value(row) or 0.0)
        total = sum(int(value["count"]) for value in grouped.values())
        out = []
        for key_value, stats in grouped.items():
            count = int(stats["count"])
            out.append(
                {
                    "key": key_value,
                    "count": count,
                    "pct": pct(count, total) or 0.0,
                    "average_pnl": round(float(stats["pnl_sum"]) / count, 4) if count else 0.0,
                }
            )
        out.sort(key=lambda item: item["count"], reverse=True)
        return out

    session_rows = [{**row, "session": session_for_timestamp(row.get("open_ts"))} for row in recent_rows]
    score_band_rows = [{**row, "score_band": score_band(row.get("ml_score"))} for row in recent_rows]
    score_band_pnl = [
        {"bucket": item["key"], "count": item["count"], "average_pnl": item["average_pnl"]}
        for item in distribution_with_pnl(score_band_rows, "score_band")
    ]
    return {
        "window_days": days,
        "live_win_rate": live_win_rate,
        "historical_win_rate": historical_win_rate,
        "win_rate_delta": round((live_win_rate or 0.0) - (historical_win_rate or 0.0), 2) if live_win_rate is not None and historical_win_rate is not None else None,
        "live_score_distribution": score_distribution(live_score_rows),
        "training_score_distribution": score_distribution(historical_score_rows),
        "symbol_distribution": distribution_with_pnl(recent_rows, "symbol"),
        "regime_distribution": distribution_with_pnl(recent_rows, "regime"),
        "session_distribution": distribution_with_pnl(session_rows, "session"),
        "average_pnl_by_regime": [{"key": item["key"], "average_pnl": item["average_pnl"], "count": item["count"]} for item in distribution_with_pnl(recent_rows, "regime")],
        "average_pnl_by_symbol": [{"key": item["key"], "average_pnl": item["average_pnl"], "count": item["count"]} for item in distribution_with_pnl(recent_rows, "symbol")],
        "average_pnl_by_score_band": score_band_pnl,
        "source_note": "Snapshot generated from admin_ml_linked_trade_snapshot and bounded recent scored decision traces.",
    }


def build_dataset_status(feature: dict[str, Any], gate: dict[str, Any]) -> dict[str, Any]:
    latest = load_latest_dataset_metadata()
    metadata = latest["meta"] if latest else {}
    schema_status = (
        get_contract_status(feature_columns=metadata.get("feature_columns"), schema_hash=metadata.get("schema_hash"))
        if get_contract_status and metadata
        else {
            "contract_version": ML_CONTRACT_VERSION,
            "schema_hash": ML_FEATURE_SCHEMA_HASH,
            "compatible": False,
        }
    )
    linked_count = int(metadata.get("row_count") or gate["total_linked_completed_trades"] or 0)
    usable_rows = int(metadata.get("usable_row_count") or gate["trades_with_full_feature_coverage"] or 0)
    dropped_rows = int(metadata.get("dropped_row_count") or max(linked_count - usable_rows, 0))
    dropped_reasons = [
        {"reason": key, "count": int(value or 0)}
        for key, value in (metadata.get("drop_reasons") or {}).items()
    ]
    if not dropped_reasons:
        dropped_reasons = [
            {"reason": item["feature_name"], "count": item["null_count_lifetime"]}
            for item in feature["features"]
            if item["null_count_lifetime"] > 0
        ]
    class_balance = metadata.get("class_balance") or {}
    label_win = class_balance.get("label_win") if isinstance(class_balance, dict) else None
    wins = int(label_win.get("positive") or 0) if isinstance(label_win, dict) else gate["wins"]
    losses = int(label_win.get("negative") or 0) if isinstance(label_win, dict) else gate["losses"]
    single_class = bool(label_win.get("single_class")) if isinstance(label_win, dict) else gate["label_distribution_single_class"]
    feature_status = "healthy"
    if feature["broken_feature_count"] > 0:
        feature_status = "broken"
    elif feature["partially_missing_feature_count"] > 0:
        feature_status = "partially_missing"
    return {
        "dataset_source_date_range": metadata.get("date_range"),
        "linked_trade_count": linked_count,
        "fully_usable_rows": usable_rows,
        "dropped_rows": dropped_rows,
        "dropped_row_reasons": sorted(dropped_reasons, key=lambda item: item["count"], reverse=True),
        "feature_completeness_status": feature_status,
        "label_distribution": {
            "wins": wins,
            "losses": losses,
            "breakevens": gate["breakeven_trades"],
            "single_class": single_class,
        },
        "last_dataset_build_time": metadata.get("dataset_build_timestamp"),
        "last_dataset_path": str(latest["dataset_path"]) if latest else None,
        "rebuild_dataset_allowed": bool(linked_count > 0),
        "source_note": "Dataset status is snapshotted offline from latest dataset metadata when present, otherwise from linked-trade snapshots.",
        "contract_version": schema_status.get("contract_version"),
        "schema_hash": schema_status.get("schema_hash"),
        "schema_compatible": bool(schema_status.get("compatible")),
        "feature_null_counts": metadata.get("feature_null_counts"),
        "label_null_counts": metadata.get("label_null_counts"),
        "class_balance": class_balance or None,
        "warnings": [] if latest else ["No dataset metadata file was found; safe snapshot defaults are used."],
    }


def refresh_ml_validation_history_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    started = time.perf_counter()
    generated_at = utc_now_iso()
    conn.execute("DELETE FROM admin_ml_validation_history_snapshot")
    rows: list[dict[str, Any]] = []
    if table_exists(conn, "ml_validation_history"):
        rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT *
                FROM ml_validation_history
                ORDER BY training_date DESC, model_version DESC
                """
            ).fetchall()
        ]
    for row in rows:
        conn.execute(
            """
            INSERT OR REPLACE INTO admin_ml_validation_history_snapshot (
                model_version, training_date, dataset_used, train_rows, test_rows, train_auc,
                test_auc, validation_method, notes, verdict, deployed_mode, metadata_path,
                validation_path, source_payload_json, source_synced_at, generated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.get("model_version"),
                row.get("training_date"),
                row.get("dataset_used"),
                row.get("train_rows"),
                row.get("test_rows"),
                row.get("train_auc"),
                row.get("test_auc"),
                row.get("validation_method"),
                row.get("notes"),
                row.get("verdict") or "unknown",
                row.get("deployed_mode") or "not_deployed",
                row.get("metadata_path"),
                row.get("validation_path"),
                row.get("source_payload_json"),
                row.get("synced_at"),
                generated_at,
            ),
        )
    log_result("ml_validation_history_snapshot", started, len(rows))
    return {
        "items": [
            {
                "model_version": row.get("model_version"),
                "training_date": row.get("training_date"),
                "dataset_used": row.get("dataset_used"),
                "train_rows": row.get("train_rows"),
                "test_rows": row.get("test_rows"),
                "train_auc": row.get("train_auc"),
                "test_auc": row.get("test_auc"),
                "validation_method": row.get("validation_method"),
                "notes": row.get("notes"),
                "verdict": row.get("verdict") or "unknown",
                "deployed_mode": row.get("deployed_mode") or "not_deployed",
            }
            for row in rows
        ],
        "source_note": "Validation history is copied from existing ml_validation_history rows during offline snapshot refresh; no request-time sync is performed.",
    }


def build_control_panel_visibility(conn: sqlite3.Connection, gate: dict[str, Any], dataset_status: dict[str, Any], overview: dict[str, Any]) -> dict[str, Any]:
    runs: list[dict[str, Any]] = []
    if table_exists(conn, "ml_action_runs"):
        runs = [
            dict(row)
            for row in conn.execute(
                """
                SELECT id, action_key, requested_by_admin_id, requested_by_email, note, status,
                       reason, supported, dataset_path, target_model_version, log_path,
                       created_at, updated_at, started_at, finished_at, result_json
                FROM ml_action_runs
                ORDER BY created_at DESC
                LIMIT 8
                """
            ).fetchall()
        ]
    serialized_runs = [
        {
            "id": row.get("id"),
            "action_key": row.get("action_key"),
            "requested_by_admin_id": row.get("requested_by_admin_id"),
            "requested_by_email": row.get("requested_by_email"),
            "note": row.get("note"),
            "status": row.get("status"),
            "reason": row.get("reason"),
            "supported": bool(row.get("supported")),
            "dataset_path": row.get("dataset_path"),
            "target_model_version": row.get("target_model_version"),
            "log_path": row.get("log_path"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "started_at": row.get("started_at"),
            "finished_at": row.get("finished_at"),
            "result": json_loads_object(row.get("result_json")) if row.get("result_json") else None,
            "log_tail": [],
        }
        for row in runs
    ]
    actions = []
    for action_key, phrase in ACTION_CONFIRM_PHRASES.items():
        actions.append(
            {
                "action_key": action_key,
                "label": action_key.replace("_", " ").title(),
                "supported": action_key in SUPPORTED_ACTIONS,
                "allowed": False,
                "blocked_reason": "ML actions remain on user-backend; Admin Backend snapshots are visibility-only.",
                "dangerous": action_key in {"run_training", "deploy_shadow", "promote_live", "rollback_shadow", "disable_ml"},
                "requires_confirmation": True,
                "confirmation_phrase": phrase,
                "dataset_path": dataset_status.get("last_dataset_path"),
                "target_model_version": overview.get("current_model_version"),
                "log_path": None,
            }
        )
    panel = {
        "readiness_status": gate["status"],
        "training_allowed_right_now": False,
        "current_dataset_path": dataset_status.get("last_dataset_path"),
        "target_output_model_version": overview.get("current_model_version") or "entry_quality_v1.0_snapshot",
        "last_training_run_status": next((row["status"] for row in serialized_runs if row["action_key"] == "run_training"), None),
        "last_training_run_logs": [],
        "last_dataset_rebuild_status": next((row["status"] for row in serialized_runs if row["action_key"] == "rebuild_dataset"), None),
        "last_validation_run_status": next((row["status"] for row in serialized_runs if row["action_key"] == "run_validation"), None),
        "actions": actions,
        "recent_action_runs": serialized_runs,
        "source_note": "Read-only control visibility snapshot only; action execution and stale-run mutation remain on user-backend.",
    }
    now = utc_now_iso()
    conn.execute(
        """
        INSERT OR REPLACE INTO admin_ml_control_visibility_snapshot (
            snapshot_key, generated_at, control_panel_json, recent_action_runs_json,
            source_note, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "latest",
            now,
            json_dumps(panel),
            json_dumps(serialized_runs),
            panel["source_note"],
            now,
            now,
        ),
    )
    return panel


def build_alerts(
    *,
    generated_at: str,
    gate: dict[str, Any],
    feature: dict[str, Any],
    linkage: dict[str, Any],
    overview: dict[str, Any],
    dataset_status: dict[str, Any],
    drift: dict[str, Any],
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []

    def add(code: str, level: str, title: str, body: str, *, category: str, blocking: bool = False, recommended_action: str | None = None) -> None:
        items.append(
            {
                "code": code,
                "level": level,
                "title": title,
                "body": body,
                "category": category,
                "message": body,
                "affected_area": category,
                "blocking": blocking,
                "recommended_action": recommended_action,
            }
        )

    if linkage.get("unlinked_completed_trades", 0):
        add(
            "unlinked_completed_trades",
            "warning",
            "Completed trades are missing trace linkage",
            f"{linkage['unlinked_completed_trades']} completed trades are unlinked. Current reasons: {linkage.get('unlinked_reason_counts', {})}.",
            category="linkage-health",
            recommended_action="Keep these surfaced as lineage warnings; do not force-link old tracing-era rows.",
        )
    if not linkage.get("linkage_healthy"):
        add(
            "linkage_unhealthy",
            "danger",
            "Trade linkage health is below threshold",
            f"Linked completed trade coverage is {linkage.get('fully_linked_completed_trades_pct')}%.",
            category="linkage-health",
            blocking=True,
        )
    if feature.get("broken_feature_count", 0) > 0:
        add("feature_broken", "danger", "Critical ML features are broken", f"{feature['broken_feature_count']} features are fully missing.", category="feature-completeness", blocking=True)
    elif feature.get("partially_missing_feature_count", 0) > 0:
        add("feature_partial", "warning", "Some ML features are partially missing", f"{feature['partially_missing_feature_count']} features have null values.", category="feature-completeness")
    if gate["total_linked_completed_trades"] < REQUIRED_TRADES:
        add("insufficient_linked_trades", "warning", "Not enough linked completed trades", f"{gate['total_linked_completed_trades']} linked trades available; {REQUIRED_TRADES} required.", category="training-gate", blocking=True)
    if gate["wins"] < REQUIRED_WINS:
        add("insufficient_wins", "warning", "Not enough winning trades", f"{gate['wins']} winning linked trades available; {REQUIRED_WINS} required.", category="training-gate", blocking=True)
    if gate["label_distribution_single_class"]:
        add("single_class_labels", "danger", "Label distribution is single-class", "Training labels do not currently include both wins and losses.", category="training-gate", blocking=True)
    if dataset_status.get("warnings"):
        add("dataset_metadata_missing", "warning", "Dataset metadata is incomplete", "; ".join(dataset_status["warnings"]), category="dataset-builder-status")
    if not overview.get("model_artifact_path"):
        add("missing_model_artifact", "warning", "Model artifact reference is missing", "No model artifact path is available in the runtime snapshot.", category="overview")
    if drift.get("win_rate_delta") is not None and drift["win_rate_delta"] <= -5:
        add("live_underperformance", "warning", "Recent win rate trails the historical baseline", f"Recent/historical win-rate delta is {drift['win_rate_delta']}%.", category="drift-monitoring")
    if not items:
        add("snapshot_healthy", "success", "ML snapshots refreshed", "No blocking snapshot issues were detected.", category="snapshot")
    items.sort(key=lambda item: {"danger": 0, "warning": 1, "info": 2, "success": 3}.get(item["level"], 99))
    return {"generated_at": generated_at, "items": items}


def refresh_ml_dashboard_snapshot(conn: sqlite3.Connection, branches: Counter[str] | None = None) -> int:
    started = time.perf_counter()
    now = utc_now_iso()
    rows = linked_snapshot_rows(conn)
    feature = summarize_feature_payload(conn)
    linkage = build_linkage_health(conn, rows)
    training_gate = build_training_gate(rows, feature, linkage)
    overview = build_overview(conn, training_gate, now)
    activity = build_activity(conn, overview, days=DEFAULT_ACTIVITY_DAYS)
    shadow = build_shadow_performance(rows, days=DEFAULT_SHADOW_DAYS)
    validation = refresh_ml_validation_history_snapshot(conn)
    dataset_status = build_dataset_status(feature, training_gate)
    drift = build_drift_monitoring(conn, rows, days=DEFAULT_DRIFT_WINDOW_DAYS)
    alerts = build_alerts(
        generated_at=now,
        gate=training_gate,
        feature=feature,
        linkage=linkage,
        overview=overview,
        dataset_status=dataset_status,
        drift=drift,
    )
    control_panel = build_control_panel_visibility(conn, training_gate, dataset_status, overview)
    dashboard_summary = {
        "ml_mode": overview["ml_mode"],
        "current_model_version": overview["current_model_version"],
        "total_linked_completed_trades": training_gate["total_linked_completed_trades"],
        "wins": training_gate["wins"],
        "feature_coverage_pct": training_gate["feature_coverage_pct"],
        "linkage_healthy": training_gate["linkage_healthy"],
        "training_ready": training_gate["training_ready"],
        "status": training_gate["status"],
        "contract_version": overview.get("contract_version"),
        "schema_hash": overview.get("schema_hash"),
        "schema_compatible": overview.get("schema_compatible"),
    }
    dashboard = {
        "overview": overview,
        "training_gate": training_gate,
        "feature_completeness": {
            "recent_completeness_pct": feature["recent_completeness_pct"],
            "lifetime_completeness_pct": feature["lifetime_completeness_pct"],
            "broken_feature_count": feature["broken_feature_count"],
            "partially_missing_feature_count": feature["partially_missing_feature_count"],
        },
        "linkage_health": linkage,
        "activity_summary": {
            "window_days": activity["window_days"],
            "total_ml_scored_entries": activity["total_ml_scored_entries"],
            "allow_count": activity["allow_count"],
            "shadow_count": activity["shadow_count"],
            "block_count": activity["block_count"],
            "skip_count": activity["skip_count"],
            "average_ml_score": activity["average_ml_score"],
            "current_threshold": activity["current_threshold"],
            "current_hard_floor": activity["current_hard_floor"],
            "recent_activity_rows": activity["recent_activity_rows"][:10],
        },
        "shadow_performance": {
            "window_days": shadow["window_days"],
            "total_linked_completed_trades_with_ml_attribution": shadow["total_linked_completed_trades_with_ml_attribution"],
            "decision_groups": shadow["decision_groups"],
            "good_allows": shadow["good_allows"],
            "bad_allows": shadow["bad_allows"],
            "good_blocks": shadow["good_blocks"],
            "bad_blocks": shadow["bad_blocks"],
        },
        "validation_history": {
            "total_models": len(validation["items"]),
            "latest_model": validation["items"][0] if validation["items"] else None,
            "source_note": validation["source_note"],
        },
        "dataset_builder_status": dataset_status,
        "alerts": alerts,
        "control_panel": control_panel,
        "drift_monitoring": {
            "window_days": drift["window_days"],
            "live_win_rate": drift["live_win_rate"],
            "historical_win_rate": drift["historical_win_rate"],
            "win_rate_delta": drift["win_rate_delta"],
            "symbol_distribution": drift["symbol_distribution"],
            "regime_distribution": drift["regime_distribution"],
            "session_distribution": drift["session_distribution"],
            "average_pnl_by_score_band": drift["average_pnl_by_score_band"],
            "source_note": drift["source_note"],
        },
    }
    metadata = snapshot_metadata(
        generated_at=now,
        source_tables=[
            "admin_ml_linked_trade_snapshot",
            "admin_ml_feature_completeness_snapshot",
            "admin_ml_drift_snapshot",
            "decision_traces",
            "trade_fills",
            "ml_runtime_status",
            "ml_validation_history",
        ],
        source_window=f"activity={DEFAULT_ACTIVITY_DAYS}d, drift={DEFAULT_DRIFT_WINDOW_DAYS}d, shadow={DEFAULT_SHADOW_DAYS}d",
        row_counts={
            "linked_trade_rows": len(rows),
            "linked_traced_rows": len(traced_rows(rows)),
            "feature_rows": len(feature["features"]),
            "activity_rows_retained": int(activity.get("snapshot_rows_retained") or 0),
            "validation_rows": len(validation["items"]),
            "drift_rows": int(conn.execute("SELECT COUNT(*) AS c FROM admin_ml_drift_snapshot").fetchone()["c"] or 0),
        },
        warnings=alerts["items"],
    )

    conn.execute(
        """
        INSERT OR REPLACE INTO admin_ml_dashboard_snapshot (
            snapshot_key, generated_at, overview_json, training_gate_json, feature_completeness_json,
            activity_json, linkage_json, shadow_performance_json, validation_history_json,
            alerts_json, dataset_status_json, drift_json, dashboard_summary_json, dashboard_json,
            control_panel_json, metadata_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "latest",
            now,
            json_dumps(overview),
            json_dumps(training_gate),
            json_dumps(feature),
            json_dumps(activity),
            json_dumps(linkage),
            json_dumps(shadow),
            json_dumps(validation),
            json_dumps(alerts),
            json_dumps(dataset_status),
            json_dumps(drift),
            json_dumps(dashboard_summary),
            json_dumps(dashboard),
            json_dumps(control_panel),
            json_dumps(metadata),
            now,
            now,
        ),
    )
    log_result("ml_dashboard_snapshot", started, 1)
    return 1


def refresh_ml(conn: sqlite3.Connection) -> dict[str, Any]:
    linked_rows, branches, unlinked_reasons = refresh_ml_linked_trade_snapshot(conn)
    feature_rows = refresh_ml_feature_completeness(conn)
    drift_rows = refresh_ml_drift_snapshot(conn)
    dashboard_rows = refresh_ml_dashboard_snapshot(conn, branches)
    validation_rows = conn.execute("SELECT COUNT(*) AS c FROM admin_ml_validation_history_snapshot").fetchone()["c"]
    control_rows = conn.execute("SELECT COUNT(*) AS c FROM admin_ml_control_visibility_snapshot").fetchone()["c"]
    return {
        "admin_ml_linked_trade_snapshot": linked_rows,
        "admin_ml_feature_completeness_snapshot": feature_rows,
        "admin_ml_drift_snapshot": drift_rows,
        "admin_ml_dashboard_snapshot": dashboard_rows,
        "admin_ml_validation_history_snapshot": int(validation_rows or 0),
        "admin_ml_control_visibility_snapshot": int(control_rows or 0),
        "linkage_branches": dict(branches),
        "unlinked_reason_counts": dict(unlinked_reasons),
    }


def main() -> int:
    args = parse_args()
    selected_any = args.all or args.profitability or args.ml or args.sizing_events
    refresh_all = args.all or not selected_any
    db_path = resolve_db_path(args.db_path)
    print(f"[admin-analytics-refresh] db={db_path}")
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    results: dict[str, Any] = {}
    with sqlite3.connect(str(db_path), timeout=60) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=60000")
        ensure_admin_analytics_snapshot_tables(conn)

        if refresh_all or args.profitability:
            results["profitability_summaries"] = refresh_profitability_summaries(conn)
        if refresh_all or args.sizing_events:
            results["admin_profitability_sizing_events"] = refresh_sizing_events(conn)
        if refresh_all or args.ml:
            results.update(refresh_ml(conn))

    print("[admin-analytics-refresh] summary=" + json.dumps(results, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
