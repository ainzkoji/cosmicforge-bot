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

    ensure_billing_launch_schema(conn)


def ensure_billing_launch_schema(conn) -> None:
    """Section H Step 8.1 additions. Additive and idempotent.

    * ``subscriptions.billing_hold``: id of the payment review (refund /
      dispute) that suspends the paid entitlement until an operator decides.
    * ``invoices.provider_charge_id`` / ``provider_payment_intent_id``: let a
      refund or dispute (which names a charge) be traced back to the invoice
      and its user.
    * ``billing_payment_reviews``: every refund and dispute Stripe reports.
    * ``billing_audit_log``: append-only record of every billing state change.
    """
    _add_column_if_missing(conn, "subscriptions", "billing_hold", "TEXT")
    _add_column_if_missing(conn, "invoices", "provider_charge_id", "TEXT")
    _add_column_if_missing(conn, "invoices", "provider_payment_intent_id", "TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_invoices_provider_charge ON invoices(provider_charge_id)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_invoices_provider_payment_intent ON invoices(provider_payment_intent_id)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS billing_payment_reviews (
            review_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL CHECK (kind IN ('REFUND', 'DISPUTE')),
            provider TEXT NOT NULL DEFAULT 'stripe',
            provider_object_id TEXT NOT NULL,
            charge_id TEXT,
            invoice_id TEXT,
            user_id TEXT,
            amount_minor INTEGER,
            amount TEXT,
            amount_refunded_minor INTEGER,
            currency TEXT,
            reason TEXT,
            provider_status TEXT,
            full_refund INTEGER NOT NULL DEFAULT 0,
            holds_entitlement INTEGER NOT NULL DEFAULT 0,
            review_status TEXT NOT NULL DEFAULT 'OPEN'
                CHECK (review_status IN ('OPEN', 'RESOLVED_KEEP_ACCESS', 'RESOLVED_REVOKE_ACCESS')),
            first_event_id TEXT,
            last_event_id TEXT,
            last_event_created INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            resolved_at TEXT,
            resolved_by TEXT,
            resolution_note TEXT,
            UNIQUE (provider, kind, provider_object_id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_billing_payment_reviews_user ON billing_payment_reviews(user_id, review_status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_billing_payment_reviews_charge ON billing_payment_reviews(charge_id)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS billing_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            occurred_at TEXT NOT NULL,
            actor_type TEXT NOT NULL,
            actor_id TEXT,
            user_id TEXT,
            action TEXT NOT NULL,
            provider_event_id TEXT,
            provider_event_type TEXT,
            before_json TEXT,
            after_json TEXT,
            details_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_billing_audit_user ON billing_audit_log(user_id, id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_billing_audit_event ON billing_audit_log(provider_event_id)")
    # Append-only: rows can be added, never changed or removed.
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_billing_audit_log_no_update
        BEFORE UPDATE ON billing_audit_log
        BEGIN SELECT RAISE(ABORT, 'billing_audit_log is append-only'); END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_billing_audit_log_no_delete
        BEFORE DELETE ON billing_audit_log
        BEGIN SELECT RAISE(ABORT, 'billing_audit_log is append-only'); END
        """
    )
