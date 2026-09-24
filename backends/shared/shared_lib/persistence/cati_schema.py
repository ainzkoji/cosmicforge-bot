"""CATI persistent schema (canonical, additive, idempotent migration).

Called from ``migrations.migrate()``. Trading/CATI runtime code never
creates these tables: it expects them to exist and fails closed if they do
not.

``cati_portfolio_reservations`` -- account-scoped SHADOW portfolio-selection
reservations (Section 16.23-16.28). A separate resource from production
position slots (``position_slots``) and account margin
(``AccountMarginReservations``); it replaces neither and neither reads it.

Lifecycle: RESERVED -> CONSUMED | RELEASED | EXPIRED (terminal).

``cati_trade_plans`` -- append-only SHADOW TradePlan evidence (Section 18.20).

Sections 19-21 append-only analytical evidence: ``cati_position_forecasts``,
``cati_exit_decisions``, ``cati_risk_decisions`` (evidence ABOUT the existing
hard-risk verdict), ``cati_execution_attempts`` (one row per attempt state,
linked to the canonical ``execution_attempts`` operational row where one
exists) and ``cati_component_errors``.

Idempotent: ``CREATE TABLE/INDEX IF NOT EXISTS``; a table created by the
pre-migration CATI code (lazy DDL, fewer columns) is upgraded in place with
additive ``ALTER TABLE ... ADD COLUMN`` only. Existing rows are never
rewritten or deleted.
"""
from __future__ import annotations

from typing import Any

CATI_RESERVATION_TABLE = "cati_portfolio_reservations"

_CREATE = f"""
CREATE TABLE IF NOT EXISTS {CATI_RESERVATION_TABLE} (
    reservation_id TEXT PRIMARY KEY,
    broker_account_id TEXT NOT NULL,
    bot_instance_id TEXT NOT NULL,
    cycle_id TEXT NOT NULL,
    selected_candidate_ids TEXT NOT NULL,
    selected_instruments TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('RESERVED', 'CONSUMED', 'RELEASED', 'EXPIRED')),
    mode TEXT NOT NULL DEFAULT 'SHADOW',
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    reservation_version TEXT NOT NULL,
    policy_version TEXT,
    policy_hash TEXT,
    payload_hash TEXT,
    capacity_snapshot TEXT
)
"""

#: Additive columns for a table created by the earlier lazy DDL.
_ADDITIVE_COLUMNS = (
    ("policy_version", "TEXT"),
    ("policy_hash", "TEXT"),
    ("payload_hash", "TEXT"),
    ("capacity_snapshot", "TEXT"),
)

#: Deliberately few indexes: the reserve transaction's account scan, the
#: per-bot slot count, and global expiry cleanup.
_INDEXES = (
    f"CREATE INDEX IF NOT EXISTS idx_cati_resv_account_status_expiry ON {CATI_RESERVATION_TABLE}(broker_account_id, status, expires_at)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_resv_bot_status ON {CATI_RESERVATION_TABLE}(bot_instance_id, status)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_resv_status_expiry ON {CATI_RESERVATION_TABLE}(status, expires_at)",
)


#: ``cati_trade_plans`` -- append-only SHADOW TradePlan evidence (Section 18.20).
#: One row per immutable plan; triggers refuse UPDATE and DELETE. No
#: execution-state machine lives here.
CATI_TRADE_PLAN_TABLE = "cati_trade_plans"

_CREATE_TRADE_PLANS = f"""
CREATE TABLE IF NOT EXISTS {CATI_TRADE_PLAN_TABLE} (
    trade_plan_id TEXT PRIMARY KEY,
    trade_plan_hash TEXT NOT NULL,
    user_id TEXT,
    broker_account_id TEXT NOT NULL,
    bot_instance_id TEXT NOT NULL,
    run_id TEXT,
    cycle_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    economic_opportunity_id TEXT NOT NULL,
    portfolio_decision_id TEXT NOT NULL,
    reservation_id TEXT NOT NULL,
    canonical_symbol TEXT NOT NULL,
    venue TEXT NOT NULL,
    environment TEXT NOT NULL,
    side TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'SHADOW',
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    schema_version TEXT NOT NULL,
    table_version TEXT NOT NULL,
    payload TEXT NOT NULL,
    payload_hash TEXT NOT NULL
)
"""

_TRADE_PLAN_INDEXES = (
    f"CREATE INDEX IF NOT EXISTS idx_cati_tplan_account_created ON {CATI_TRADE_PLAN_TABLE}(broker_account_id, created_at)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_tplan_bot_cycle ON {CATI_TRADE_PLAN_TABLE}(bot_instance_id, cycle_id)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_tplan_reservation ON {CATI_TRADE_PLAN_TABLE}(reservation_id)",
)

_TRADE_PLAN_TRIGGERS = (
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_tplan_no_update BEFORE UPDATE ON {CATI_TRADE_PLAN_TABLE}
        BEGIN SELECT RAISE(ABORT, 'cati_trade_plans is append-only'); END""",
    f"""CREATE TRIGGER IF NOT EXISTS trg_cati_tplan_no_delete BEFORE DELETE ON {CATI_TRADE_PLAN_TABLE}
        BEGIN SELECT RAISE(ABORT, 'cati_trade_plans is append-only'); END""",
)


# ---------------------------------------------------------------------------
# Sections 19-21 -- append-only ANALYTICAL evidence. Operational state
# (reservation status, broker/position lifecycle) stays in its own mutable
# tables; these rows are never updated or deleted (triggers refuse it). A new
# PositionForecast / ExitDecision / attempt state is a NEW row.
# ---------------------------------------------------------------------------
CATI_POSITION_FORECAST_TABLE = "cati_position_forecasts"
CATI_EXIT_DECISION_TABLE = "cati_exit_decisions"
CATI_RISK_DECISION_TABLE = "cati_risk_decisions"
CATI_EXECUTION_ATTEMPT_TABLE = "cati_execution_attempts"
CATI_COMPONENT_ERROR_TABLE = "cati_component_errors"

_TENANT_COLS = """
    user_id TEXT,
    broker_account_id TEXT NOT NULL,
    bot_instance_id TEXT NOT NULL,"""
_TAIL_COLS = """
    schema_version TEXT NOT NULL,
    table_version TEXT NOT NULL,
    payload TEXT NOT NULL,
    payload_hash TEXT NOT NULL"""

_CREATE_EVIDENCE = (
    f"""CREATE TABLE IF NOT EXISTS {CATI_POSITION_FORECAST_TABLE} (
    position_forecast_id TEXT PRIMARY KEY,
    position_id TEXT NOT NULL,
    trade_plan_id TEXT NOT NULL,
    position_path_id TEXT NOT NULL,
    market_state_id TEXT,
    regime_distribution_id TEXT,{_TENANT_COLS}
    forecast_time INTEGER NOT NULL,
    status TEXT NOT NULL,
    thesis_status TEXT NOT NULL,
    conservative_remaining_edge_r REAL NOT NULL,
    evaluation_mode TEXT NOT NULL,{_TAIL_COLS}
)""",
    f"""CREATE TABLE IF NOT EXISTS {CATI_EXIT_DECISION_TABLE} (
    exit_decision_id TEXT PRIMARY KEY,
    position_forecast_id TEXT NOT NULL,
    position_id TEXT NOT NULL,
    trade_plan_id TEXT NOT NULL,{_TENANT_COLS}
    decision_time INTEGER NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('HOLD','REDUCE','EXIT','TIGHTEN_PROTECTION','TAKE_PARTIAL','NO_CHANGE_FALLBACK')),
    requested_fraction REAL,
    suggested_protection_price REAL,
    thesis_status TEXT NOT NULL,
    evaluation_mode TEXT NOT NULL,{_TAIL_COLS}
)""",
    f"""CREATE TABLE IF NOT EXISTS {CATI_RISK_DECISION_TABLE} (
    risk_decision_id TEXT PRIMARY KEY,
    trade_plan_id TEXT NOT NULL,
    trade_plan_hash TEXT NOT NULL,{_TENANT_COLS}
    runtime_session_id TEXT,
    run_id TEXT,
    cycle_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('APPROVED','REJECTED')),
    rejection_family TEXT,
    decision_time INTEGER NOT NULL,{_TAIL_COLS}
)""",
    f"""CREATE TABLE IF NOT EXISTS {CATI_EXECUTION_ATTEMPT_TABLE} (
    record_id TEXT PRIMARY KEY,
    execution_attempt_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    trade_plan_id TEXT NOT NULL,
    risk_decision_id TEXT,{_TENANT_COLS}
    position_id TEXT,
    status TEXT NOT NULL,
    broker_order_id TEXT,
    recorded_at INTEGER NOT NULL,{_TAIL_COLS},
    UNIQUE (execution_attempt_id, sequence)
)""",
    f"""CREATE TABLE IF NOT EXISTS {CATI_COMPONENT_ERROR_TABLE} (
    error_id TEXT PRIMARY KEY,
    component TEXT NOT NULL,
    stage TEXT,
    exception_class TEXT NOT NULL,
    cycle_id TEXT,
    user_id TEXT,
    broker_account_id TEXT,
    bot_instance_id TEXT,
    observed_at INTEGER NOT NULL,{_TAIL_COLS}
)""",
)

#: Only the lookups the evidence readers actually perform.
_EVIDENCE_INDEXES = (
    f"CREATE INDEX IF NOT EXISTS idx_cati_pfc_account_time ON {CATI_POSITION_FORECAST_TABLE}(broker_account_id, forecast_time)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_pfc_position ON {CATI_POSITION_FORECAST_TABLE}(position_id)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_exd_account_time ON {CATI_EXIT_DECISION_TABLE}(broker_account_id, decision_time)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_exd_position ON {CATI_EXIT_DECISION_TABLE}(position_id)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_risk_plan ON {CATI_RISK_DECISION_TABLE}(trade_plan_id)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_risk_account_time ON {CATI_RISK_DECISION_TABLE}(broker_account_id, decision_time)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_exec_plan ON {CATI_EXECUTION_ATTEMPT_TABLE}(trade_plan_id)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_exec_account_time ON {CATI_EXECUTION_ATTEMPT_TABLE}(broker_account_id, recorded_at)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_exec_position ON {CATI_EXECUTION_ATTEMPT_TABLE}(position_id)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_err_time ON {CATI_COMPONENT_ERROR_TABLE}(observed_at)",
)


def _append_only_triggers(table: str, tag: str):
    return (
        f"""CREATE TRIGGER IF NOT EXISTS trg_{tag}_no_update BEFORE UPDATE ON {table}
        BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END""",
        f"""CREATE TRIGGER IF NOT EXISTS trg_{tag}_no_delete BEFORE DELETE ON {table}
        BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END""",
    )


_EVIDENCE_TRIGGERS = (
    _append_only_triggers(CATI_POSITION_FORECAST_TABLE, "cati_pfc")
    + _append_only_triggers(CATI_EXIT_DECISION_TABLE, "cati_exd")
    + _append_only_triggers(CATI_RISK_DECISION_TABLE, "cati_risk")
    + _append_only_triggers(CATI_EXECUTION_ATTEMPT_TABLE, "cati_exec")
    + _append_only_triggers(CATI_COMPONENT_ERROR_TABLE, "cati_err")
)

CATI_EVIDENCE_TABLES = (CATI_POSITION_FORECAST_TABLE, CATI_EXIT_DECISION_TABLE, CATI_RISK_DECISION_TABLE,
                        CATI_EXECUTION_ATTEMPT_TABLE, CATI_COMPONENT_ERROR_TABLE)


def ensure_cati_schema_on_connection(conn: Any) -> None:
    conn.execute(_CREATE)
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({CATI_RESERVATION_TABLE})").fetchall()}
    for name, col_type in _ADDITIVE_COLUMNS:
        if name not in cols:
            conn.execute(f"ALTER TABLE {CATI_RESERVATION_TABLE} ADD COLUMN {name} {col_type}")
    for ddl in _INDEXES:
        conn.execute(ddl)
    # Strictly additive: an index left by the earlier lazy DDL is kept, not dropped.
    conn.execute(_CREATE_TRADE_PLANS)
    for ddl in _TRADE_PLAN_INDEXES + _TRADE_PLAN_TRIGGERS:
        conn.execute(ddl)
    for ddl in _CREATE_EVIDENCE + _EVIDENCE_INDEXES + _EVIDENCE_TRIGGERS:
        conn.execute(ddl)


def ensure_cati_schema(db: Any) -> None:
    with db.connect() as conn:
        ensure_cati_schema_on_connection(conn)


__all__ = ["CATI_RESERVATION_TABLE", "CATI_TRADE_PLAN_TABLE", "CATI_EVIDENCE_TABLES", "CATI_POSITION_FORECAST_TABLE",
           "CATI_EXIT_DECISION_TABLE", "CATI_RISK_DECISION_TABLE", "CATI_EXECUTION_ATTEMPT_TABLE",
           "CATI_COMPONENT_ERROR_TABLE", "ensure_cati_schema", "ensure_cati_schema_on_connection"]
