"""CATI persistent schema (canonical, additive, idempotent migration).

Called from ``migrations.migrate()``. Trading/CATI runtime code never
creates these tables: it expects them to exist and fails closed if they do
not.

``cati_portfolio_reservations`` -- account-scoped SHADOW portfolio-selection
reservations (Section 16.23-16.28). A separate resource from production
position slots (``position_slots``) and account margin
(``AccountMarginReservations``); it replaces neither and neither reads it.

Lifecycle: RESERVED -> CONSUMED | RELEASED | EXPIRED (terminal), plus
RESERVED -> RESOLUTION_PENDING when a broker entry submission's outcome is
UNKNOWN. RESOLUTION_PENDING never expires; only broker-authoritative
reconciliation moves it to CONSUMED (entry exists) or RELEASED (proven none).

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
    status TEXT NOT NULL CHECK (status IN ('RESERVED', 'RESOLUTION_PENDING', 'CONSUMED', 'RELEASED', 'EXPIRED')),
    mode TEXT NOT NULL DEFAULT 'SHADOW',
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    reservation_version TEXT NOT NULL,
    policy_version TEXT,
    policy_hash TEXT,
    payload_hash TEXT,
    capacity_snapshot TEXT,
    trade_plan_id TEXT,
    execution_attempt_id TEXT,
    pending_since INTEGER,
    resolution_deadline INTEGER,
    resolution_note TEXT
)
"""

#: Additive columns for a table created by the earlier lazy DDL (and, for the
#: last five, by the pre-closure schema): unresolved submit ownership.
_ADDITIVE_COLUMNS = (
    ("policy_version", "TEXT"),
    ("policy_hash", "TEXT"),
    ("payload_hash", "TEXT"),
    ("capacity_snapshot", "TEXT"),
    ("trade_plan_id", "TEXT"),
    ("execution_attempt_id", "TEXT"),
    ("pending_since", "INTEGER"),
    ("resolution_deadline", "INTEGER"),
    ("resolution_note", "TEXT"),
)

_RESERVATION_COLUMNS = ("reservation_id", "broker_account_id", "bot_instance_id", "cycle_id", "selected_candidate_ids",
                        "selected_instruments", "status", "mode", "created_at", "expires_at", "updated_at",
                        "reservation_version", "policy_version", "policy_hash", "payload_hash", "capacity_snapshot")


def _upgrade_reservation_status_check(conn: Any) -> None:
    """``RESOLUTION_PENDING`` (unresolved broker ownership after a
    SUBMIT_UNKNOWN entry) is a new operational state. SQLite cannot alter a
    CHECK constraint, so a table created with the older four-state CHECK is
    rebuilt ONCE: same rows, same values, new constraint. Idempotent -- a
    table whose DDL already allows RESOLUTION_PENDING is left untouched.
    This is the mutable OPERATIONAL reservation table, never analytical
    evidence."""
    row = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                       (CATI_RESERVATION_TABLE,)).fetchone()
    if row is None or "RESOLUTION_PENDING" in (row[0] or ""):
        return
    old = f"{CATI_RESERVATION_TABLE}__pre_resolution_pending"
    conn.execute(f"ALTER TABLE {CATI_RESERVATION_TABLE} RENAME TO {old}")
    conn.execute(_CREATE)
    have = {r[1] for r in conn.execute(f"PRAGMA table_info({old})").fetchall()}
    cols = [c for c in _RESERVATION_COLUMNS if c in have]
    names = ", ".join(cols)
    conn.execute(f"INSERT INTO {CATI_RESERVATION_TABLE} ({names}) SELECT {names} FROM {old}")
    conn.execute(f"DROP TABLE {old}")

#: Deliberately few indexes: the reserve transaction's account scan, the
#: per-bot slot count, and global expiry cleanup.
_INDEXES = (
    f"CREATE INDEX IF NOT EXISTS idx_cati_resv_account_status_expiry ON {CATI_RESERVATION_TABLE}(broker_account_id, status, expires_at)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_resv_bot_status ON {CATI_RESERVATION_TABLE}(bot_instance_id, status)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_resv_status_expiry ON {CATI_RESERVATION_TABLE}(status, expires_at)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_resv_pending ON {CATI_RESERVATION_TABLE}(status, resolution_deadline)",
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
#: The canonical UPSTREAM decision evidence (OutcomeForecast incl. its
#: DistributionShiftAssessment, EconomicOpportunity, VetoDecision) behind a
#: plan, keyed so downstream exports reconstruct it BY ID instead of copying
#: fields into every contract or recomputing them.
CATI_DECISION_EVIDENCE_TABLE = "cati_decision_evidence"
# Section 22 -- research & certification (analytical, append-only; research
# scope, never tenant account evidence, so no broker_account_id column)
CATI_CERTIFICATION_RUN_TABLE = "cati_certification_runs"
CATI_CERTIFICATION_STAGE_TABLE = "cati_certification_stage_results"
CATI_EXPERIMENT_TABLE = "cati_experiment_registry"
CATI_HOLDOUT_TABLE = "cati_holdout_registry"
# Section 23 -- CATI ML estimator registry / status history / shadow evidence
CATI_ML_MODEL_TABLE = "cati_ml_models"
CATI_ML_MODEL_EVENT_TABLE = "cati_ml_model_events"
CATI_ML_SHADOW_TABLE = "cati_ml_shadow_predictions"
# Section 25 -- promotion governance (phase history, M7 scopes, kill switch)
CATI_PHASE_HISTORY_TABLE = "cati_promotion_phase_history"
CATI_PROMOTION_SCOPE_TABLE = "cati_promotion_scopes"
CATI_GOVERNANCE_CONTROL_TABLE = "cati_governance_controls"

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
    f"""CREATE TABLE IF NOT EXISTS {CATI_DECISION_EVIDENCE_TABLE} (
    decision_evidence_id TEXT PRIMARY KEY,
    economic_opportunity_id TEXT NOT NULL,
    forecast_id TEXT NOT NULL,
    veto_decision_id TEXT NOT NULL,
    setup_candidate_id TEXT NOT NULL,
    market_state_id TEXT NOT NULL,{_TENANT_COLS}
    decision_time INTEGER NOT NULL,{_TAIL_COLS}
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
    f"CREATE INDEX IF NOT EXISTS idx_cati_dev_opp ON {CATI_DECISION_EVIDENCE_TABLE}(broker_account_id, economic_opportunity_id)",
)


def _append_only_triggers(table: str, tag: str):
    return (
        f"""CREATE TRIGGER IF NOT EXISTS trg_{tag}_no_update BEFORE UPDATE ON {table}
        BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END""",
        f"""CREATE TRIGGER IF NOT EXISTS trg_{tag}_no_delete BEFORE DELETE ON {table}
        BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END""",
    )


_CREATE_CERTIFICATION = (
    f"""CREATE TABLE IF NOT EXISTS {CATI_CERTIFICATION_RUN_TABLE} (
    certification_run_id TEXT PRIMARY KEY,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    scope_hash TEXT NOT NULL,
    dataset_hash TEXT NOT NULL,
    policy_freeze_hash TEXT NOT NULL,
    certification_policy_hash TEXT NOT NULL,
    artifact_hash TEXT NOT NULL,
    recorded_at INTEGER NOT NULL,{_TAIL_COLS}
)""",
    f"""CREATE TABLE IF NOT EXISTS {CATI_CERTIFICATION_STAGE_TABLE} (
    stage_result_id TEXT PRIMARY KEY,
    certification_run_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    recorded_at INTEGER NOT NULL,{_TAIL_COLS}
)""",
    f"""CREATE TABLE IF NOT EXISTS {CATI_EXPERIMENT_TABLE} (
    experiment_id TEXT PRIMARY KEY,
    parent_experiment_id TEXT,
    dataset_hash TEXT NOT NULL,
    policy_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at INTEGER NOT NULL,{_TAIL_COLS}
)""",
    # one row per holdout EVENT (RESERVED / OPENED / BURNED): status is the
    # latest event, so a burned holdout can never be re-labelled untouched
    f"""CREATE TABLE IF NOT EXISTS {CATI_HOLDOUT_TABLE} (
    holdout_event_id TEXT PRIMARY KEY,
    holdout_id TEXT NOT NULL,
    dataset_hash TEXT NOT NULL,
    event TEXT NOT NULL CHECK (event IN ('RESERVED', 'OPENED', 'BURNED')),
    policy_freeze_hash TEXT,
    recorded_at INTEGER NOT NULL,{_TAIL_COLS}
)""",
)

_CREATE_ML_GOVERNANCE = (
    f"""CREATE TABLE IF NOT EXISTS {CATI_ML_MODEL_TABLE} (
    model_id TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    artifact_hash TEXT NOT NULL,
    training_dataset_hash TEXT NOT NULL,
    feature_schema_hash TEXT NOT NULL,
    label_schema_hash TEXT NOT NULL,
    code_commit TEXT,
    supersedes_model_id TEXT,
    created_at INTEGER NOT NULL,{_TAIL_COLS}
)""",
    f"""CREATE TABLE IF NOT EXISTS {CATI_ML_MODEL_EVENT_TABLE} (
    model_event_id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    recorded_at INTEGER NOT NULL,{_TAIL_COLS}
)""",
    f"""CREATE TABLE IF NOT EXISTS {CATI_ML_SHADOW_TABLE} (
    prediction_id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL,
    role TEXT NOT NULL,
    market_state_id TEXT,
    setup_candidate_id TEXT,
    decision_time INTEGER NOT NULL,
    recorded_at INTEGER NOT NULL,{_TAIL_COLS}
)""",
    f"""CREATE TABLE IF NOT EXISTS {CATI_PHASE_HISTORY_TABLE} (
    transition_id TEXT PRIMARY KEY,
    from_phase TEXT NOT NULL,
    to_phase TEXT NOT NULL,
    scope_hash TEXT NOT NULL,
    source_commit TEXT,
    certification_run_id TEXT,
    policy_freeze_hash TEXT,
    requested_at INTEGER NOT NULL,
    approved_at INTEGER,
    recorded_at INTEGER NOT NULL,{_TAIL_COLS}
)""",
    f"""CREATE TABLE IF NOT EXISTS {CATI_PROMOTION_SCOPE_TABLE} (
    scope_event_id TEXT PRIMARY KEY,
    scope_hash TEXT NOT NULL,
    event TEXT NOT NULL CHECK (event IN ('GRANTED', 'REVOKED')),
    broker_account_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    environment TEXT NOT NULL,
    recorded_at INTEGER NOT NULL,{_TAIL_COLS}
)""",
    f"""CREATE TABLE IF NOT EXISTS {CATI_GOVERNANCE_CONTROL_TABLE} (
    control_event_id TEXT PRIMARY KEY,
    control TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ON', 'OFF')),
    scope TEXT NOT NULL,
    recorded_at INTEGER NOT NULL,{_TAIL_COLS}
)""",
)

_ML_GOVERNANCE_INDEXES = (
    f"CREATE INDEX IF NOT EXISTS idx_cati_mlm_role ON {CATI_ML_MODEL_TABLE}(role, created_at)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_mle_model ON {CATI_ML_MODEL_EVENT_TABLE}(model_id, recorded_at)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_mls_model ON {CATI_ML_SHADOW_TABLE}(model_id, decision_time)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_mls_cand ON {CATI_ML_SHADOW_TABLE}(setup_candidate_id)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_phase_time ON {CATI_PHASE_HISTORY_TABLE}(recorded_at)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_scope_acct ON {CATI_PROMOTION_SCOPE_TABLE}(broker_account_id, recorded_at)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_ctrl ON {CATI_GOVERNANCE_CONTROL_TABLE}(control, scope, recorded_at)",
)

_CERTIFICATION_INDEXES = (
    f"CREATE INDEX IF NOT EXISTS idx_cati_cert_run_stage ON {CATI_CERTIFICATION_RUN_TABLE}(stage, dataset_hash)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_cert_stage_run ON {CATI_CERTIFICATION_STAGE_TABLE}(certification_run_id)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_exp_dataset ON {CATI_EXPERIMENT_TABLE}(dataset_hash, policy_hash)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_holdout_id ON {CATI_HOLDOUT_TABLE}(holdout_id, recorded_at)",
)

_EVIDENCE_TRIGGERS = (
    _append_only_triggers(CATI_POSITION_FORECAST_TABLE, "cati_pfc")
    + _append_only_triggers(CATI_EXIT_DECISION_TABLE, "cati_exd")
    + _append_only_triggers(CATI_RISK_DECISION_TABLE, "cati_risk")
    + _append_only_triggers(CATI_EXECUTION_ATTEMPT_TABLE, "cati_exec")
    + _append_only_triggers(CATI_COMPONENT_ERROR_TABLE, "cati_err")
    + _append_only_triggers(CATI_DECISION_EVIDENCE_TABLE, "cati_dev")
    + _append_only_triggers(CATI_CERTIFICATION_RUN_TABLE, "cati_crun")
    + _append_only_triggers(CATI_CERTIFICATION_STAGE_TABLE, "cati_cstg")
    + _append_only_triggers(CATI_EXPERIMENT_TABLE, "cati_exp")
    + _append_only_triggers(CATI_HOLDOUT_TABLE, "cati_hold")
    + _append_only_triggers(CATI_ML_MODEL_TABLE, "cati_mlm")
    + _append_only_triggers(CATI_ML_MODEL_EVENT_TABLE, "cati_mle")
    + _append_only_triggers(CATI_ML_SHADOW_TABLE, "cati_mls")
    + _append_only_triggers(CATI_PHASE_HISTORY_TABLE, "cati_phase")
    + _append_only_triggers(CATI_PROMOTION_SCOPE_TABLE, "cati_scope")
    + _append_only_triggers(CATI_GOVERNANCE_CONTROL_TABLE, "cati_ctrl")
)

CATI_CERTIFICATION_TABLES = (CATI_CERTIFICATION_RUN_TABLE, CATI_CERTIFICATION_STAGE_TABLE, CATI_EXPERIMENT_TABLE,
                             CATI_HOLDOUT_TABLE)

CATI_EVIDENCE_TABLES = (CATI_POSITION_FORECAST_TABLE, CATI_EXIT_DECISION_TABLE, CATI_RISK_DECISION_TABLE,
                        CATI_EXECUTION_ATTEMPT_TABLE, CATI_COMPONENT_ERROR_TABLE, CATI_DECISION_EVIDENCE_TABLE)


def ensure_cati_schema_on_connection(conn: Any) -> None:
    _upgrade_reservation_status_check(conn)
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
    for ddl in _CREATE_EVIDENCE + _CREATE_CERTIFICATION + _CREATE_ML_GOVERNANCE + _EVIDENCE_INDEXES \
            + _CERTIFICATION_INDEXES + _ML_GOVERNANCE_INDEXES + _EVIDENCE_TRIGGERS:
        conn.execute(ddl)


def ensure_cati_schema(db: Any) -> None:
    with db.connect() as conn:
        ensure_cati_schema_on_connection(conn)


__all__ = ["CATI_RESERVATION_TABLE", "CATI_TRADE_PLAN_TABLE", "CATI_EVIDENCE_TABLES", "CATI_POSITION_FORECAST_TABLE",
           "CATI_EXIT_DECISION_TABLE", "CATI_RISK_DECISION_TABLE", "CATI_EXECUTION_ATTEMPT_TABLE",
           "CATI_COMPONENT_ERROR_TABLE", "CATI_DECISION_EVIDENCE_TABLE", "CATI_CERTIFICATION_TABLES",
           "CATI_CERTIFICATION_RUN_TABLE", "CATI_CERTIFICATION_STAGE_TABLE", "CATI_EXPERIMENT_TABLE",
           "CATI_HOLDOUT_TABLE", "CATI_ML_MODEL_TABLE", "CATI_ML_MODEL_EVENT_TABLE", "CATI_ML_SHADOW_TABLE",
           "CATI_PHASE_HISTORY_TABLE", "CATI_PROMOTION_SCOPE_TABLE", "CATI_GOVERNANCE_CONTROL_TABLE",
           "ensure_cati_schema", "ensure_cati_schema_on_connection"]
