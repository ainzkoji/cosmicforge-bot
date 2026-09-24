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


def ensure_cati_schema(db: Any) -> None:
    with db.connect() as conn:
        ensure_cati_schema_on_connection(conn)


__all__ = ["CATI_RESERVATION_TABLE", "CATI_TRADE_PLAN_TABLE", "ensure_cati_schema", "ensure_cati_schema_on_connection"]
