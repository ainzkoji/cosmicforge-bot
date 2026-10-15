"""Section H Steps 3 and 4: the persistent schema of the CATI platform pipeline.

Additive and idempotent. No existing table is rewritten, renamed or dropped, and no existing row is touched, so
a database of an older build keeps every account, credential, reservation and record it had, and an older
build can still read a database this module has upgraded (it only finds tables it does not know).

Six modules contribute, each owning its own tables:

* this module: family governance, cycle driver state, immutable family decisions, the shadow book;
* ``cati_portfolio_schema``: the per-account multi-position portfolio, the reservation ledger, risk brakes;
* ``cati_execution_schema``: multi-position execution and durable stop replacement;
* ``cati_exchange_schema``: adapter certifications, the instrument rule cache, rate budgets, user streams;
* ``cati_feed_schema``: the daily bar feed;
* ``cati_operations_schema``: account leases, emergency preemption, evidence retention.

Every one exposes ``DDL`` (``CREATE ... IF NOT EXISTS`` statements) and ``upgrade(conn)`` (additive column
changes, a no-op when there are none). ``ensure_cati_platform_schema`` runs them all in one transaction.

Append-only tables are protected by triggers: an UPDATE or a DELETE aborts. Evidence is corrected by a later
record, never by changing an earlier one.
"""
from __future__ import annotations

from typing import Any, Tuple

SCHEMA_VERSION = "1.0.0"

FAMILY_EVENT_TABLE = "cati_family_events"
CYCLE_TABLE = "cati_platform_cycles"
CYCLE_ACCOUNT_TABLE = "cati_platform_cycle_accounts"
DECISION_TABLE = "cati_family_decisions"
SHADOW_ACCOUNT_TABLE = "cati_shadow_accounts"
SHADOW_POSITION_TABLE = "cati_shadow_positions"
SHADOW_FILL_TABLE = "cati_shadow_fills"
SHADOW_DAY_TABLE = "cati_shadow_days"
SHADOW_COMPARISON_TABLE = "cati_shadow_comparisons"
CYCLE_METRIC_TABLE = "cati_platform_cycle_metrics"
RETENTION_RUN_TABLE = "cati_retention_runs"


def append_only_triggers(table: str, tag: str) -> Tuple[str, str]:
    return (
        f"""CREATE TRIGGER IF NOT EXISTS trg_{tag}_no_update BEFORE UPDATE ON {table}
        BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END""",
        f"""CREATE TRIGGER IF NOT EXISTS trg_{tag}_no_delete BEFORE DELETE ON {table}
        BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END""",
    )


DDL: Tuple[str, ...] = (
    # Family governance log (3.2, 3.11). One hash chain over all families: a removed or edited record breaks
    # every later hash. ``seq`` is assigned inside the writing transaction.
    f"""CREATE TABLE IF NOT EXISTS {FAMILY_EVENT_TABLE} (
        seq INTEGER PRIMARY KEY,
        event_id TEXT NOT NULL UNIQUE,
        family_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        scope TEXT NOT NULL,
        request_id TEXT,
        from_stage TEXT,
        to_stage TEXT,
        actor_ref TEXT NOT NULL,
        recorded_at INTEGER NOT NULL,
        payload TEXT NOT NULL,
        prev_hash TEXT NOT NULL,
        event_hash TEXT NOT NULL UNIQUE)""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_fev_family ON {FAMILY_EVENT_TABLE}(family_id, seq)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_fev_request ON {FAMILY_EVENT_TABLE}(request_id)",
    *append_only_triggers(FAMILY_EVENT_TABLE, "cati_fev"),
    # Cycle driver (3.1). One row per (family, cycle key, mode): the cycle id is a function of those three, so
    # a second start of the same cycle finds the first one.
    f"""CREATE TABLE IF NOT EXISTS {CYCLE_TABLE} (
        cycle_id TEXT PRIMARY KEY,
        family_id TEXT NOT NULL,
        cycle_key TEXT NOT NULL,
        mode TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt INTEGER NOT NULL DEFAULT 1,
        owner TEXT,
        started_at INTEGER NOT NULL,
        heartbeat_at INTEGER NOT NULL,
        completed_at INTEGER,
        data_cutoff TEXT,
        market_hash TEXT,
        rule_hash TEXT,
        accounts_total INTEGER NOT NULL DEFAULT 0,
        accounts_done INTEGER NOT NULL DEFAULT 0,
        accounts_failed INTEGER NOT NULL DEFAULT 0,
        detail TEXT NOT NULL DEFAULT '{{}}',
        UNIQUE(family_id, cycle_key, mode))""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_cyc_status ON {CYCLE_TABLE}(status, started_at)",
    f"""CREATE TABLE IF NOT EXISTS {CYCLE_ACCOUNT_TABLE} (
        cycle_id TEXT NOT NULL,
        account_id TEXT NOT NULL,
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        decision_id TEXT,
        started_at INTEGER,
        completed_at INTEGER,
        duration_ms INTEGER,
        detail TEXT NOT NULL DEFAULT '{{}}',
        PRIMARY KEY(cycle_id, account_id))""",
    # Immutable family decisions (3.3). ``decision_hash`` covers the whole document; the boundary verifies it.
    f"""CREATE TABLE IF NOT EXISTS {DECISION_TABLE} (
        decision_id TEXT PRIMARY KEY,
        cycle_id TEXT NOT NULL,
        family_id TEXT NOT NULL,
        family_version TEXT NOT NULL,
        rule_hash TEXT NOT NULL,
        mandate_id TEXT,
        account_id TEXT NOT NULL,
        user_id TEXT,
        environment TEXT NOT NULL,
        mode TEXT NOT NULL,
        decision_time INTEGER NOT NULL,
        data_cutoff TEXT NOT NULL,
        expires_at INTEGER NOT NULL,
        document TEXT NOT NULL,
        decision_hash TEXT NOT NULL,
        recorded_at INTEGER NOT NULL,
        UNIQUE(cycle_id, account_id))""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_fdec_account ON {DECISION_TABLE}(account_id, family_id, decision_time)",
    *append_only_triggers(DECISION_TABLE, "cati_fdec"),
    # The shadow book (3.1, 3.6, 3.15): what a family would hold for an account. Never an exchange position.
    f"""CREATE TABLE IF NOT EXISTS {SHADOW_ACCOUNT_TABLE} (
        account_id TEXT NOT NULL,
        family_id TEXT NOT NULL,
        risk_level TEXT NOT NULL,
        policy TEXT NOT NULL,
        initial_equity REAL NOT NULL,
        equity REAL NOT NULL,
        peak_equity REAL NOT NULL,
        paused INTEGER NOT NULL DEFAULT 0,
        halved INTEGER NOT NULL DEFAULT 0,
        halted INTEGER NOT NULL DEFAULT 0,
        first_day INTEGER NOT NULL,
        last_day INTEGER,
        pending_orders TEXT NOT NULL DEFAULT '[]',
        updated_cycle TEXT,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(account_id, family_id))""",
    f"""CREATE TABLE IF NOT EXISTS {SHADOW_POSITION_TABLE} (
        account_id TEXT NOT NULL,
        family_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        quantity REAL NOT NULL,
        stop_price REAL NOT NULL,
        mark_price REAL NOT NULL,
        entry_day INTEGER NOT NULL,
        entry_price REAL NOT NULL,
        entry_stop_distance REAL NOT NULL,
        updated_cycle TEXT,
        PRIMARY KEY(account_id, family_id, symbol))""",
    f"""CREATE TABLE IF NOT EXISTS {SHADOW_FILL_TABLE} (
        fill_id TEXT PRIMARY KEY,
        cycle_id TEXT NOT NULL,
        account_id TEXT NOT NULL,
        family_id TEXT NOT NULL,
        day INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        kind TEXT NOT NULL,
        quantity REAL NOT NULL,
        price REAL NOT NULL,
        recorded_at INTEGER NOT NULL)""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_sfill_account ON {SHADOW_FILL_TABLE}(account_id, family_id, day)",
    *append_only_triggers(SHADOW_FILL_TABLE, "cati_sfill"),
    f"""CREATE TABLE IF NOT EXISTS {SHADOW_DAY_TABLE} (
        account_id TEXT NOT NULL,
        family_id TEXT NOT NULL,
        day INTEGER NOT NULL,
        cycle_id TEXT NOT NULL,
        equity REAL NOT NULL,
        daily_return REAL NOT NULL,
        drawdown REAL NOT NULL,
        positions INTEGER NOT NULL,
        gross_notional REAL NOT NULL,
        paused INTEGER NOT NULL,
        halved INTEGER NOT NULL,
        halted INTEGER NOT NULL,
        document TEXT NOT NULL,
        recorded_at INTEGER NOT NULL,
        PRIMARY KEY(account_id, family_id, day))""",
    *append_only_triggers(SHADOW_DAY_TABLE, "cati_sday"),
    f"""CREATE TABLE IF NOT EXISTS {SHADOW_COMPARISON_TABLE} (
        comparison_id TEXT PRIMARY KEY,
        family_id TEXT NOT NULL,
        account_id TEXT NOT NULL,
        day INTEGER NOT NULL,
        reference TEXT NOT NULL,
        status TEXT NOT NULL,
        largest_difference REAL,
        document TEXT NOT NULL,
        recorded_at INTEGER NOT NULL)""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_scmp_day ON {SHADOW_COMPARISON_TABLE}(family_id, account_id, day)",
    *append_only_triggers(SHADOW_COMPARISON_TABLE, "cati_scmp"),
    # Stage timings of a cycle (3.12). Operational, prunable (3.14).
    f"""CREATE TABLE IF NOT EXISTS {CYCLE_METRIC_TABLE} (
        cycle_id TEXT NOT NULL,
        stage TEXT NOT NULL,
        account_id TEXT NOT NULL DEFAULT '',
        duration_ms REAL NOT NULL,
        items INTEGER NOT NULL DEFAULT 0,
        recorded_at INTEGER NOT NULL,
        PRIMARY KEY(cycle_id, stage, account_id))""",
    # What a retention run exported and removed (3.14). The record of a removal is itself never removed.
    f"""CREATE TABLE IF NOT EXISTS {RETENTION_RUN_TABLE} (
        run_id TEXT PRIMARY KEY,
        table_name TEXT NOT NULL,
        policy TEXT NOT NULL,
        cutoff_ms INTEGER NOT NULL,
        rows_exported INTEGER NOT NULL,
        rows_removed INTEGER NOT NULL,
        archive_path TEXT,
        archive_sha256 TEXT,
        status TEXT NOT NULL,
        started_at INTEGER NOT NULL,
        completed_at INTEGER,
        detail TEXT NOT NULL DEFAULT '{{}}')""",
    *append_only_triggers(RETENTION_RUN_TABLE, "cati_retn"),
)


def upgrade(conn: Any) -> None:
    """Additive column changes of this module's tables. None yet."""


def ensure_on_connection(conn: Any) -> None:
    from shared_lib.persistence import (
        cati_exchange_schema,
        cati_execution_schema,
        cati_feed_schema,
        cati_operations_schema,
        cati_portfolio_schema,
    )

    for module in (None, cati_portfolio_schema, cati_execution_schema, cati_exchange_schema, cati_feed_schema,
                   cati_operations_schema):
        for statement in (DDL if module is None else module.DDL):
            conn.execute(statement)
        (upgrade if module is None else module.upgrade)(conn)


def ensure_cati_platform_schema(db: Any) -> None:
    with db.connect() as conn:
        ensure_on_connection(conn)


def add_column_if_missing(conn: Any, table: str, column: str, declaration: str) -> bool:
    """``ALTER TABLE ... ADD COLUMN`` when the column is absent. Returns True when it was added."""
    if column in {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}:
        return False
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")
    return True


__all__ = ["SCHEMA_VERSION", "DDL", "upgrade", "ensure_on_connection", "ensure_cati_platform_schema",
           "append_only_triggers", "add_column_if_missing", "FAMILY_EVENT_TABLE", "CYCLE_TABLE", "CYCLE_ACCOUNT_TABLE",
           "DECISION_TABLE", "SHADOW_ACCOUNT_TABLE", "SHADOW_POSITION_TABLE", "SHADOW_FILL_TABLE", "SHADOW_DAY_TABLE",
           "SHADOW_COMPARISON_TABLE", "CYCLE_METRIC_TABLE", "RETENTION_RUN_TABLE"]
