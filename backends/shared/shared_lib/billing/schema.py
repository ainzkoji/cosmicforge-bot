"""Billing schema additions.

Additive and idempotent, called from ``migrations.migrate()`` right after the
``subscriptions`` / ``invoices`` / ``pricing_intents`` tables are created.
"""
from __future__ import annotations


def _columns(conn, table: str) -> set:
    # Index access: works whatever row factory the connection uses.
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _add_column_if_missing(conn, table: str, col: str, col_type: str) -> None:
    if col not in _columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")


def ensure_billing_schema(conn) -> None:
    # --- subscriptions: provider state driven by webhooks -------------------
    _add_column_if_missing(conn, "subscriptions", "provider", "TEXT")              # 'stripe' | NULL (legacy)
    _add_column_if_missing(conn, "subscriptions", "provider_customer_id", "TEXT")
    _add_column_if_missing(conn, "subscriptions", "provider_price_id", "TEXT")
    _add_column_if_missing(conn, "subscriptions", "billing_interval", "TEXT")      # month | year
    _add_column_if_missing(conn, "subscriptions", "grace_period_end", "TEXT")      # set while past_due
    _add_column_if_missing(conn, "subscriptions", "checkout_session_id", "TEXT")
    _add_column_if_missing(conn, "subscriptions", "previous_plan_id", "TEXT")      # plan held before a downgrade
    # ``created`` of the newest provider event applied to this row (ordering guard).
    _add_column_if_missing(conn, "subscriptions", "provider_updated_at", "INTEGER")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_subscriptions_provider_sub ON subscriptions(provider_sub_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_subscriptions_provider_customer ON subscriptions(provider_customer_id)"
    )

    # --- invoices: ``amount`` stays REAL (major units) for the admin dashboards;
    # ``amount_cents`` is the exact integer amount in the currency's minor unit.
    _add_column_if_missing(conn, "invoices", "amount_cents", "INTEGER")
    _add_column_if_missing(conn, "invoices", "provider_invoice_id", "TEXT")
    _add_column_if_missing(conn, "invoices", "provider_sub_id", "TEXT")
    _add_column_if_missing(conn, "invoices", "plan_id", "TEXT")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_pricing_intents_session ON pricing_intents(session_id)")

    # --- user <-> provider customer. One row per Stripe mode (test / live), so
    # switching keys never reuses a customer id from the other mode.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS billing_customers (
            user_id TEXT NOT NULL,
            mode TEXT NOT NULL DEFAULT 'live',
            provider TEXT NOT NULL DEFAULT 'stripe',
            provider_customer_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (user_id, mode)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_billing_customers_provider_id "
        "ON billing_customers(provider_customer_id)"
    )

    # --- billing_events: processed webhook events, idempotent by provider event id.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS billing_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT,
            event_type TEXT NOT NULL,
            provider TEXT DEFAULT 'stripe',
            payload_json TEXT,
            processed_at TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    _add_column_if_missing(conn, "billing_events", "result", "TEXT")
    # The unique index is what makes ``INSERT OR IGNORE`` an idempotency check.
    # Collapse any pre-existing duplicates first so creating it cannot fail.
    conn.execute("UPDATE billing_events SET provider = 'stripe' WHERE provider IS NULL")
    conn.execute(
        """
        DELETE FROM billing_events
        WHERE event_id IS NOT NULL
          AND id NOT IN (
              SELECT MIN(id) FROM billing_events
              WHERE event_id IS NOT NULL
              GROUP BY provider, event_id
          )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_billing_events_provider_event "
        "ON billing_events(provider, event_id) WHERE event_id IS NOT NULL"
    )
