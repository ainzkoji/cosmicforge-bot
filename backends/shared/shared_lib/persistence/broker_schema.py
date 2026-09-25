"""Broker-account security and internal-transfer schema (Phase 2F-2J, 5F).

Additive and idempotent, called from ``migrations.migrate()``.

* ``broker_credentials_v2.permissions_json`` -- normalised API-key permission
  evidence per credential version (no secrets).
* ``broker_accounts.permission_status`` -- the activation decision derived
  from that evidence (ACCEPTED / ACCEPTED_UNVERIFIED / REJECTED_*).
* ``broker_transfer_requests`` -- one row per broker-INTERNAL transfer
  (wallet -> wallet inside ONE connected account). Idempotent per
  (user, account, idempotency_key). There is no withdrawal / external
  destination column: the schema cannot express an external move.
* ``broker_transfer_events`` -- append-only state history.
* ``broker_transfer_reconciliations`` -- one row per reconciliation run.
* ``broker_transfer_settings`` -- per-account transfer mode and user limits.
* ``broker_transfers_cache`` gains ``classification`` / ``direction`` /
  wallet columns so INTERNAL_TRANSFER is never conflated with DEPOSIT or
  WITHDRAWAL.
"""
from __future__ import annotations

from typing import Any

TRANSFER_STATUSES = (
    "REQUESTED", "VALIDATING", "BLOCKED", "SUBMITTING", "SUBMITTED", "CONFIRMATION_PENDING",
    "COMPLETED", "FAILED", "UNKNOWN", "RECONCILIATION_REQUIRED",
)

_TRANSFER_REQUESTS = f"""
CREATE TABLE IF NOT EXISTS broker_transfer_requests (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    broker_account_id TEXT NOT NULL,
    broker TEXT NOT NULL,
    environment TEXT NOT NULL,
    credential_version INTEGER,
    asset TEXT NOT NULL,
    amount TEXT NOT NULL,
    source_wallet TEXT NOT NULL,
    destination_wallet TEXT NOT NULL,
    source_venue_wallet TEXT,
    destination_venue_wallet TEXT,
    broker_transfer_id TEXT,
    idempotency_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ({", ".join(repr(s) for s in TRANSFER_STATUSES)})),
    failure_reason TEXT,
    origin TEXT NOT NULL DEFAULT 'MANUAL',
    requested_at TEXT NOT NULL,
    submitted_at TEXT,
    confirmed_at TEXT,
    last_reconciled_at TEXT,
    reconcile_attempts INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (user_id, broker_account_id, idempotency_key),
    FOREIGN KEY (broker_account_id) REFERENCES broker_accounts(id)
)
"""

_TRANSFER_EVENTS = """
CREATE TABLE IF NOT EXISTS broker_transfer_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transfer_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    broker_account_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT,
    detail_json TEXT,
    created_at TEXT NOT NULL
)
"""

_RECONCILIATIONS = """
CREATE TABLE IF NOT EXISTS broker_transfer_reconciliations (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    broker_account_id TEXT NOT NULL,
    broker TEXT NOT NULL,
    run_at TEXT NOT NULL,
    history_rows INTEGER NOT NULL DEFAULT 0,
    matched INTEGER NOT NULL DEFAULT 0,
    completed INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    still_unresolved INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    detail_json TEXT
)
"""

_SETTINGS = """
CREATE TABLE IF NOT EXISTS broker_transfer_settings (
    broker_account_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'MANUAL_TRANSFER'
        CHECK (mode IN ('MANUAL_TRANSFER', 'AUTOMATED_INTERNAL_REALLOCATION')),
    auto_rebalance_enabled INTEGER NOT NULL DEFAULT 0,
    max_transfer_amount TEXT,
    min_funding_balance TEXT,
    min_derivatives_reserve TEXT,
    min_free_margin TEXT,
    asset_allowlist_json TEXT,
    wallet_allowlist_json TEXT,
    daily_transfer_limit TEXT,
    authorized_at TEXT,
    updated_at TEXT NOT NULL
)
"""


def _cols(conn: Any, table: str) -> set:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _add(conn: Any, table: str, col: str, decl: str) -> None:
    if _cols(conn, table) and col not in _cols(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")


def ensure_broker_schema(db: Any) -> None:
    with db.connect() as conn:
        _add(conn, "broker_credentials_v2", "permissions_json", "TEXT")
        _add(conn, "broker_accounts", "permission_status", "TEXT")
        conn.execute(_TRANSFER_REQUESTS)
        conn.execute(_TRANSFER_EVENTS)
        conn.execute(_RECONCILIATIONS)
        conn.execute(_SETTINGS)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_btr_account_status ON broker_transfer_requests(broker_account_id, status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_btr_user ON broker_transfer_requests(user_id, requested_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bte_transfer ON broker_transfer_events(transfer_id, id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_btrec_account ON broker_transfer_reconciliations(broker_account_id, run_at)")
        # Transfer history cache: classification is explicit, never inferred from sign.
        for col, decl in (("classification", "TEXT"), ("direction", "TEXT"), ("source_wallet", "TEXT"),
                          ("destination_wallet", "TEXT"), ("transfer_request_id", "TEXT")):
            _add(conn, "broker_transfers_cache", col, decl)
        # Append-only: an event row can never be edited or removed.
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_bte_no_update BEFORE UPDATE ON broker_transfer_events
            BEGIN SELECT RAISE(ABORT, 'broker_transfer_events is append-only'); END
        """)
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_bte_no_delete BEFORE DELETE ON broker_transfer_events
            BEGIN SELECT RAISE(ABORT, 'broker_transfer_events is append-only'); END
        """)


__all__ = ["TRANSFER_STATUSES", "ensure_broker_schema"]
