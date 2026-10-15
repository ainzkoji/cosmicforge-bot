"""Section H: tables of adapter certifications, the instrument rule cache, rate budgets and user streams (Steps 4.2 to 4.5).

Part of the CATI platform schema; see ``cati_platform_schema`` for the rules every statement here follows
(additive, idempotent, ``IF NOT EXISTS``, no existing table rewritten).

Four groups, each owned by one module of ``app.exchange.platform_adapter``:

* ``certification``: the append-only, hash-chained register of adapter certification events, the evidence of
  every check of a run, and every mutation a run sent to a venue (intent first, outcome second);
* ``rules_cache``: the current rules of every (venue, environment, symbol), their change history and the
  refresh state that carries the bounded backoff;
* ``rate_budget``: one budget per (venue, scope key) shared by every worker that opens this database, the
  sliding log of reservations, and denial counters;
* ``user_stream`` / ``reconcile``: stream health with its single-owner lease, the fill ledger that
  deduplicates across restart, the per-symbol REST cursors and the log of reconciliations.

No table stores a credential, a listen key, a stream URL or a signature. An account appears only as the
reference the caller supplied (the id of the connected account).
"""
from __future__ import annotations

from typing import Any, Tuple

from shared_lib.persistence.cati_platform_schema import append_only_triggers

CERT_EVENT_TABLE = "cati_adapter_cert_events"
CERT_CHECK_TABLE = "cati_adapter_cert_checks"
CERT_ACTION_TABLE = "cati_adapter_cert_actions"
RULES_TABLE = "cati_instrument_rules"
RULES_CHANGE_TABLE = "cati_instrument_rule_changes"
RULES_REFRESH_TABLE = "cati_instrument_rule_refreshes"
RATE_BUDGET_TABLE = "cati_rate_budgets"
RATE_RESERVATION_TABLE = "cati_rate_reservations"
RATE_DENIAL_TABLE = "cati_rate_denials"
STREAM_STATE_TABLE = "cati_user_stream_state"
STREAM_FILL_TABLE = "cati_user_stream_fills"
STREAM_CURSOR_TABLE = "cati_user_stream_cursors"
STREAM_RECONCILE_TABLE = "cati_user_stream_reconciliations"

DDL: Tuple[str, ...] = (
    # ------------------------------------------------------------------------------------ certification (4.2)
    # One hash chain over every venue and environment: a removed or edited event breaks every later hash.
    # ``status`` is set only on a decisive event (CERTIFIED / FAILED / REVOKED / BLOCKED).
    f"""CREATE TABLE IF NOT EXISTS {CERT_EVENT_TABLE} (
        seq INTEGER PRIMARY KEY,
        event_id TEXT NOT NULL UNIQUE,
        venue TEXT NOT NULL,
        environment TEXT NOT NULL,
        event_type TEXT NOT NULL,
        status TEXT,
        run_id TEXT,
        adapter_version TEXT,
        contract_version TEXT,
        operator_ref TEXT NOT NULL,
        account_ref TEXT,
        authorized_scope TEXT,
        started_at INTEGER,
        completed_at INTEGER,
        expires_at INTEGER,
        source_commit TEXT,
        recorded_at INTEGER NOT NULL,
        document TEXT NOT NULL,
        prev_hash TEXT NOT NULL,
        event_hash TEXT NOT NULL UNIQUE)""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_cert_venue ON {CERT_EVENT_TABLE}(venue, environment, seq)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_cert_run ON {CERT_EVENT_TABLE}(run_id)",
    *append_only_triggers(CERT_EVENT_TABLE, "cati_cert_ev"),
    f"""CREATE TABLE IF NOT EXISTS {CERT_CHECK_TABLE} (
        run_id TEXT NOT NULL,
        check_id TEXT NOT NULL,
        status TEXT NOT NULL,
        reason TEXT NOT NULL DEFAULT '',
        started_at INTEGER NOT NULL,
        completed_at INTEGER NOT NULL,
        evidence TEXT NOT NULL,
        evidence_hash TEXT NOT NULL,
        PRIMARY KEY(run_id, check_id))""",
    *append_only_triggers(CERT_CHECK_TABLE, "cati_cert_chk"),
    # Every mutation of a run: an INTENT row is committed before the request leaves, an OUTCOME row after.
    # An INTENT without an OUTCOME is an order whose fate must be read back from the venue (rollback).
    f"""CREATE TABLE IF NOT EXISTS {CERT_ACTION_TABLE} (
        run_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        phase TEXT NOT NULL,
        action TEXT NOT NULL,
        symbol TEXT NOT NULL,
        client_order_id TEXT,
        outcome TEXT,
        document TEXT NOT NULL,
        recorded_at INTEGER NOT NULL,
        PRIMARY KEY(run_id, seq))""",
    *append_only_triggers(CERT_ACTION_TABLE, "cati_cert_act"),
    # -------------------------------------------------------------------------------------- rule cache (4.3)
    # A rule the venue did not report is NULL and stays NULL: nothing here has a numeric default.
    f"""CREATE TABLE IF NOT EXISTS {RULES_TABLE} (
        venue TEXT NOT NULL,
        environment TEXT NOT NULL,
        symbol TEXT NOT NULL,
        venue_symbol TEXT NOT NULL,
        status TEXT NOT NULL,
        venue_status TEXT NOT NULL,
        base_asset TEXT NOT NULL,
        quote_asset TEXT NOT NULL,
        margin_asset TEXT NOT NULL,
        price_tick REAL,
        quantity_step REAL,
        min_quantity REAL,
        max_quantity REAL,
        market_max_quantity REAL,
        min_notional REAL,
        max_leverage REAL,
        contract_size REAL NOT NULL,
        complete INTEGER NOT NULL,
        rules_hash TEXT NOT NULL,
        raw_hash TEXT NOT NULL DEFAULT '',
        adapter_version TEXT NOT NULL,
        capabilities TEXT NOT NULL DEFAULT '{{}}',
        fetched_at_ms INTEGER NOT NULL,
        stored_at_ms INTEGER NOT NULL,
        expires_at_ms INTEGER NOT NULL,
        first_seen_ms INTEGER NOT NULL,
        changed_at_ms INTEGER,
        delisted_at_ms INTEGER,
        conflict_at_ms INTEGER,
        conflict_reason TEXT,
        PRIMARY KEY(venue, environment, symbol))""",
    f"""CREATE TABLE IF NOT EXISTS {RULES_CHANGE_TABLE} (
        change_id TEXT PRIMARY KEY,
        venue TEXT NOT NULL,
        environment TEXT NOT NULL,
        symbol TEXT NOT NULL,
        change_type TEXT NOT NULL,
        previous_hash TEXT,
        rules_hash TEXT,
        changed_fields TEXT NOT NULL DEFAULT '[]',
        document TEXT NOT NULL,
        adapter_version TEXT NOT NULL,
        fetched_at_ms INTEGER,
        recorded_at_ms INTEGER NOT NULL)""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_rulechg_symbol ON {RULES_CHANGE_TABLE}(venue, environment, symbol, "
    f"recorded_at_ms)",
    *append_only_triggers(RULES_CHANGE_TABLE, "cati_rulechg"),
    f"""CREATE TABLE IF NOT EXISTS {RULES_REFRESH_TABLE} (
        venue TEXT NOT NULL,
        environment TEXT NOT NULL,
        last_attempt_ms INTEGER,
        last_success_ms INTEGER,
        last_error TEXT,
        consecutive_failures INTEGER NOT NULL DEFAULT 0,
        next_attempt_ms INTEGER NOT NULL DEFAULT 0,
        claim_owner TEXT,
        claim_until_ms INTEGER NOT NULL DEFAULT 0,
        symbols_seen INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY(venue, environment))""",
    # ------------------------------------------------------------------------------------- rate budget (4.5)
    # ``scope_key`` is the server address for an IP-scoped budget and ``uid:<account reference>`` for an
    # account-scoped closure. A closure ends by time only: no statement in the application reopens one early.
    f"""CREATE TABLE IF NOT EXISTS {RATE_BUDGET_TABLE} (
        venue TEXT NOT NULL,
        scope_key TEXT NOT NULL,
        scope TEXT NOT NULL,
        capacity REAL NOT NULL,
        window_ms INTEGER NOT NULL,
        closed_until_ms INTEGER NOT NULL DEFAULT 0,
        closed_reason TEXT,
        closed_status INTEGER,
        closed_at_ms INTEGER,
        closures INTEGER NOT NULL DEFAULT 0,
        observed_used REAL,
        observed_limit REAL,
        observed_window_ms INTEGER,
        observed_at_ms INTEGER,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY(venue, scope_key))""",
    # Sliding log of granted weight. Operational and prunable: rows older than the retention are removed by
    # the reservation that follows them.
    f"""CREATE TABLE IF NOT EXISTS {RATE_RESERVATION_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        venue TEXT NOT NULL,
        scope_key TEXT NOT NULL,
        reserved_at_ms INTEGER NOT NULL,
        weight REAL NOT NULL,
        priority TEXT NOT NULL,
        category TEXT NOT NULL DEFAULT '',
        account_ref TEXT,
        worker TEXT NOT NULL DEFAULT '')""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_rate_res ON {RATE_RESERVATION_TABLE}(venue, scope_key, reserved_at_ms)",
    f"""CREATE TABLE IF NOT EXISTS {RATE_DENIAL_TABLE} (
        venue TEXT NOT NULL,
        scope_key TEXT NOT NULL,
        priority TEXT NOT NULL,
        reason TEXT NOT NULL,
        denials INTEGER NOT NULL DEFAULT 0,
        first_at_ms INTEGER NOT NULL,
        last_at_ms INTEGER NOT NULL,
        PRIMARY KEY(venue, scope_key, priority, reason))""",
    # ------------------------------------------------------------------- user streams and reconciliation (4.4)
    f"""CREATE TABLE IF NOT EXISTS {STREAM_STATE_TABLE} (
        venue TEXT NOT NULL,
        environment TEXT NOT NULL,
        account_ref TEXT NOT NULL,
        health TEXT NOT NULL,
        health_reason TEXT NOT NULL DEFAULT '',
        owner TEXT,
        lease_until_ms INTEGER NOT NULL DEFAULT 0,
        connected_at_ms INTEGER,
        last_frame_at_ms INTEGER,
        last_reconcile_at_ms INTEGER,
        last_reconcile_status TEXT,
        reconnect_attempts INTEGER NOT NULL DEFAULT 0,
        next_connect_at_ms INTEGER NOT NULL DEFAULT 0,
        connects INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY(venue, environment, account_ref))""",
    # The fill ledger. At most one row per source for one fill: the first row of either source is the fill,
    # a REST row beside a STREAM row is the venue's own record of it and is the one that is read.
    f"""CREATE TABLE IF NOT EXISTS {STREAM_FILL_TABLE} (
        venue TEXT NOT NULL,
        account_ref TEXT NOT NULL,
        symbol TEXT NOT NULL,
        trade_id TEXT NOT NULL,
        source TEXT NOT NULL,
        environment TEXT NOT NULL,
        venue_order_id TEXT,
        client_order_id TEXT,
        side TEXT NOT NULL,
        quantity REAL NOT NULL,
        price REAL NOT NULL,
        fee REAL,
        fee_asset TEXT,
        realized_pnl REAL,
        is_maker INTEGER,
        time_ms INTEGER NOT NULL,
        recorded_at_ms INTEGER NOT NULL,
        PRIMARY KEY(venue, account_ref, symbol, trade_id, source))""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_sfills_order ON {STREAM_FILL_TABLE}(venue, account_ref, symbol, "
    f"venue_order_id)",
    f"CREATE INDEX IF NOT EXISTS idx_cati_sfills_time ON {STREAM_FILL_TABLE}(venue, account_ref, time_ms)",
    *append_only_triggers(STREAM_FILL_TABLE, "cati_sfills"),
    f"""CREATE TABLE IF NOT EXISTS {STREAM_CURSOR_TABLE} (
        venue TEXT NOT NULL,
        account_ref TEXT NOT NULL,
        symbol TEXT NOT NULL,
        cursor_ms INTEGER NOT NULL,
        updated_at_ms INTEGER NOT NULL,
        PRIMARY KEY(venue, account_ref, symbol))""",
    f"""CREATE TABLE IF NOT EXISTS {STREAM_RECONCILE_TABLE} (
        reconcile_id TEXT PRIMARY KEY,
        venue TEXT NOT NULL,
        environment TEXT NOT NULL,
        account_ref TEXT NOT NULL,
        trigger_reason TEXT NOT NULL,
        status TEXT NOT NULL,
        error_code TEXT,
        started_at_ms INTEGER NOT NULL,
        completed_at_ms INTEGER NOT NULL,
        document TEXT NOT NULL)""",
    f"CREATE INDEX IF NOT EXISTS idx_cati_srecon_account ON {STREAM_RECONCILE_TABLE}(venue, account_ref, "
    f"started_at_ms)",
    *append_only_triggers(STREAM_RECONCILE_TABLE, "cati_srecon"),
)


def upgrade(conn: Any) -> None:
    """Additive column changes of this module's tables. None yet."""


__all__ = ["DDL", "upgrade", "CERT_EVENT_TABLE", "CERT_CHECK_TABLE", "CERT_ACTION_TABLE", "RULES_TABLE",
           "RULES_CHANGE_TABLE", "RULES_REFRESH_TABLE", "RATE_BUDGET_TABLE", "RATE_RESERVATION_TABLE",
           "RATE_DENIAL_TABLE", "STREAM_STATE_TABLE", "STREAM_FILL_TABLE", "STREAM_CURSOR_TABLE",
           "STREAM_RECONCILE_TABLE"]
