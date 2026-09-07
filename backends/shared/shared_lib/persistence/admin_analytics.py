from __future__ import annotations

import sqlite3
from typing import Iterable


ADMIN_ANALYTICS_INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_admin_trade_fills_timestamp_id "
    "ON trade_fills(timestamp_utc, id)",
    "CREATE INDEX IF NOT EXISTS idx_admin_trade_fills_scope_time "
    "ON trade_fills(account_id, initiator_type, timestamp_utc, id)",
    "CREATE INDEX IF NOT EXISTS idx_admin_trade_fills_scope_action_position_time "
    "ON trade_fills(account_id, action, position_id, timestamp_utc)",
    "CREATE INDEX IF NOT EXISTS idx_admin_runs_started_at_desc "
    "ON runs(started_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_admin_decision_traces_position_ts "
    "ON decision_traces(position_id, ts DESC) WHERE position_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_admin_decision_traces_run_cycle_symbol_ts "
    "ON decision_traces(run_id, cycle_id, symbol, ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_admin_decision_traces_ml_score_ts "
    "ON decision_traces(ts DESC) WHERE ml_score IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_admin_decision_traces_model_ts "
    "ON decision_traces(ts DESC) "
    "WHERE ml_model_version IS NOT NULL OR ml_threshold IS NOT NULL",
)


SNAPSHOT_INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_admin_profit_daily_scope_date "
    "ON admin_profitability_daily_summary(account_scope, date)",
    "CREATE INDEX IF NOT EXISTS idx_admin_profit_symbol_scope_pnl "
    "ON admin_profitability_symbol_summary(account_scope, total_realized_pnl DESC)",
    "CREATE INDEX IF NOT EXISTS idx_admin_profit_sizing_trace "
    "ON admin_profitability_sizing_events(trace_id)",
    "CREATE INDEX IF NOT EXISTS idx_admin_profit_sizing_ts "
    "ON admin_profitability_sizing_events(ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_admin_profit_sizing_symbol_ts "
    "ON admin_profitability_sizing_events(symbol, ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_admin_ml_linked_position "
    "ON admin_ml_linked_trade_snapshot(position_id)",
    "CREATE INDEX IF NOT EXISTS idx_admin_ml_linked_run_cycle_symbol "
    "ON admin_ml_linked_trade_snapshot(run_id, cycle_id, symbol)",
    "CREATE INDEX IF NOT EXISTS idx_admin_ml_linked_open_ts "
    "ON admin_ml_linked_trade_snapshot(open_ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_admin_ml_linked_score_action "
    "ON admin_ml_linked_trade_snapshot(ml_action, ml_score)",
    "CREATE INDEX IF NOT EXISTS idx_admin_ml_linked_branch_reason "
    "ON admin_ml_linked_trade_snapshot(linkage_branch, unlinked_reason)",
    "CREATE INDEX IF NOT EXISTS idx_admin_ml_feature_scope_feature "
    "ON admin_ml_feature_completeness_snapshot(scope, feature_name)",
    "CREATE INDEX IF NOT EXISTS idx_admin_ml_drift_scope "
    "ON admin_ml_drift_snapshot(scope, symbol, regime, score_band)",
    "CREATE INDEX IF NOT EXISTS idx_admin_ml_validation_training_date "
    "ON admin_ml_validation_history_snapshot(training_date DESC, model_version DESC)",
)


def ensure_admin_analytics_snapshot_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_profitability_daily_summary (
            date TEXT NOT NULL,
            account_scope TEXT NOT NULL,
            fills_count INTEGER NOT NULL DEFAULT 0,
            closed_trades INTEGER NOT NULL DEFAULT 0,
            winning_trades INTEGER NOT NULL DEFAULT 0,
            losing_trades INTEGER NOT NULL DEFAULT 0,
            total_realized_pnl REAL NOT NULL DEFAULT 0,
            avg_pnl REAL,
            win_rate REAL,
            profit_factor REAL,
            avg_r_multiple REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (date, account_scope)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_profitability_symbol_summary (
            symbol TEXT NOT NULL,
            account_scope TEXT NOT NULL,
            fills_count INTEGER NOT NULL DEFAULT 0,
            closed_trades INTEGER NOT NULL DEFAULT 0,
            total_realized_pnl REAL NOT NULL DEFAULT 0,
            avg_pnl REAL,
            win_rate REAL,
            avg_r_multiple REAL,
            sl_count INTEGER NOT NULL DEFAULT 0,
            tp_count INTEGER NOT NULL DEFAULT 0,
            time_exit_count INTEGER NOT NULL DEFAULT 0,
            other_exit_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (symbol, account_scope)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_profitability_sizing_events (
            id TEXT PRIMARY KEY,
            trace_id TEXT,
            symbol TEXT,
            ts TEXT,
            run_id TEXT,
            cycle_id TEXT,
            sizing_method TEXT,
            configured_margin REAL,
            final_margin REAL,
            base_notional REAL,
            final_notional REAL,
            leverage REAL,
            cap_applied INTEGER NOT NULL DEFAULT 0,
            risk_cap_pct REAL,
            atr_stop_distance_pct REAL,
            explanation TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_ml_linked_trade_snapshot (
            id TEXT PRIMARY KEY,
            symbol TEXT,
            position_id TEXT,
            run_id TEXT,
            cycle_id TEXT,
            open_trace_id TEXT,
            close_trace_id TEXT,
            open_ts TEXT,
            close_ts TEXT,
            side TEXT,
            realized_pnl REAL,
            r_multiple REAL,
            ml_score REAL,
            ml_action TEXT,
            ml_model_version TEXT,
            regime TEXT,
            confidence REAL,
            threshold REAL,
            features_json TEXT,
            linkage_branch TEXT,
            unlinked_reason TEXT,
            trace_match_count INTEGER NOT NULL DEFAULT 0,
            source_trace_match_basis TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    _ensure_column(conn, "admin_ml_linked_trade_snapshot", "linkage_branch", "TEXT")
    _ensure_column(conn, "admin_ml_linked_trade_snapshot", "unlinked_reason", "TEXT")
    _ensure_column(conn, "admin_ml_linked_trade_snapshot", "trace_match_count", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "admin_ml_linked_trade_snapshot", "source_trace_match_basis", "TEXT")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_ml_dashboard_snapshot (
            snapshot_key TEXT PRIMARY KEY,
            generated_at TEXT NOT NULL,
            overview_json TEXT NOT NULL,
            training_gate_json TEXT NOT NULL,
            feature_completeness_json TEXT,
            activity_json TEXT NOT NULL,
            linkage_json TEXT NOT NULL,
            shadow_performance_json TEXT,
            validation_history_json TEXT,
            alerts_json TEXT NOT NULL,
            dataset_status_json TEXT NOT NULL,
            drift_json TEXT NOT NULL,
            dashboard_summary_json TEXT,
            dashboard_json TEXT,
            control_panel_json TEXT,
            metadata_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    _ensure_column(conn, "admin_ml_dashboard_snapshot", "feature_completeness_json", "TEXT")
    _ensure_column(conn, "admin_ml_dashboard_snapshot", "shadow_performance_json", "TEXT")
    _ensure_column(conn, "admin_ml_dashboard_snapshot", "validation_history_json", "TEXT")
    _ensure_column(conn, "admin_ml_dashboard_snapshot", "dashboard_summary_json", "TEXT")
    _ensure_column(conn, "admin_ml_dashboard_snapshot", "dashboard_json", "TEXT")
    _ensure_column(conn, "admin_ml_dashboard_snapshot", "control_panel_json", "TEXT")
    _ensure_column(conn, "admin_ml_dashboard_snapshot", "metadata_json", "TEXT")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_ml_feature_completeness_snapshot (
            id TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            feature_name TEXT NOT NULL,
            total_rows INTEGER NOT NULL DEFAULT 0,
            non_null_rows INTEGER NOT NULL DEFAULT 0,
            null_rows INTEGER NOT NULL DEFAULT 0,
            completeness_pct REAL,
            recent_total_rows INTEGER NOT NULL DEFAULT 0,
            recent_non_null_rows INTEGER NOT NULL DEFAULT 0,
            recent_completeness_pct REAL,
            last_seen_populated_at TEXT,
            frontend_status TEXT,
            recent_window_basis TEXT,
            recent_window_limit INTEGER,
            generated_at TEXT NOT NULL
        )
        """
    )
    _ensure_column(conn, "admin_ml_feature_completeness_snapshot", "last_seen_populated_at", "TEXT")
    _ensure_column(conn, "admin_ml_feature_completeness_snapshot", "frontend_status", "TEXT")
    _ensure_column(conn, "admin_ml_feature_completeness_snapshot", "recent_window_basis", "TEXT")
    _ensure_column(conn, "admin_ml_feature_completeness_snapshot", "recent_window_limit", "INTEGER")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_ml_drift_snapshot (
            id TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            symbol TEXT,
            regime TEXT,
            score_band TEXT,
            sample_count INTEGER NOT NULL DEFAULT 0,
            win_rate REAL,
            avg_pnl REAL,
            avg_r_multiple REAL,
            generated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_ml_validation_history_snapshot (
            model_version TEXT PRIMARY KEY,
            training_date TEXT,
            dataset_used TEXT,
            train_rows INTEGER,
            test_rows INTEGER,
            train_auc REAL,
            test_auc REAL,
            validation_method TEXT,
            notes TEXT,
            verdict TEXT NOT NULL,
            deployed_mode TEXT NOT NULL DEFAULT 'not_deployed',
            metadata_path TEXT,
            validation_path TEXT,
            source_payload_json TEXT,
            source_synced_at TEXT,
            generated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_ml_control_visibility_snapshot (
            snapshot_key TEXT PRIMARY KEY,
            generated_at TEXT NOT NULL,
            control_panel_json TEXT NOT NULL,
            recent_action_runs_json TEXT NOT NULL,
            source_note TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )


def create_admin_analytics_indexes(conn: sqlite3.Connection, *, analyze: bool = True) -> list[str]:
    created_or_verified: list[str] = []
    for statement in ADMIN_ANALYTICS_INDEXES:
        conn.execute(statement)
        created_or_verified.append(_index_name(statement))
    for statement in SNAPSHOT_INDEXES:
        conn.execute(statement)
        created_or_verified.append(_index_name(statement))
    if analyze:
        conn.execute("ANALYZE")
    return created_or_verified


def ensure_admin_analytics_foundation(
    conn: sqlite3.Connection,
    *,
    create_indexes: bool = True,
    analyze: bool = True,
) -> list[str]:
    ensure_admin_analytics_snapshot_tables(conn)
    if not create_indexes:
        return []
    return create_admin_analytics_indexes(conn, analyze=analyze)


def admin_snapshot_table_names() -> tuple[str, ...]:
    return (
        "admin_profitability_daily_summary",
        "admin_profitability_symbol_summary",
        "admin_profitability_sizing_events",
        "admin_ml_linked_trade_snapshot",
        "admin_ml_dashboard_snapshot",
        "admin_ml_feature_completeness_snapshot",
        "admin_ml_drift_snapshot",
        "admin_ml_validation_history_snapshot",
        "admin_ml_control_visibility_snapshot",
    )


def _index_name(statement: str) -> str:
    parts = statement.split()
    try:
        return parts[parts.index("EXISTS") + 1]
    except (ValueError, IndexError):
        return statement


def rows_changed_for_tables(conn: sqlite3.Connection, table_names: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table_name in table_names:
        row = conn.execute(f"SELECT COUNT(*) AS c FROM {table_name}").fetchone()
        counts[table_name] = int(row["c"] if isinstance(row, sqlite3.Row) else row[0])
    return counts


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_type: str) -> None:
    existing = {str(row["name"] if isinstance(row, sqlite3.Row) else row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})")}
    if column_name in existing:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
