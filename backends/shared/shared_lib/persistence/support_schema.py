"""Support desk tables (Section H Step 8.5). Additive and idempotent; called at the end of
``shared_lib.persistence.migrations.migrate()``.

* ``support_tickets`` -- one row per ticket, owned by exactly one user (``user_id``); every user-facing read
  filters on it.
* ``support_ticket_messages`` -- the conversation. Append-only: a message cannot be edited or deleted.
* ``support_ticket_events`` -- who opened, assigned, replied, closed; append-only.

No attachment storage exists (out of scope), and the service layer refuses any message that looks like an API
key, password or other secret before it reaches these tables.
"""
from __future__ import annotations

from typing import Any

TICKET_CATEGORIES = ("ACCOUNT", "BILLING", "BROKER_CONNECTION", "TRADING", "BOTS", "KYC", "SECURITY", "TECHNICAL",
                     "OTHER")
TICKET_STATUSES = ("OPEN", "IN_PROGRESS", "WAITING_ON_CUSTOMER", "CLOSED")


def _in(values) -> str:
    return ", ".join(f"'{v}'" for v in values)


SUPPORT_SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id),
    category TEXT NOT NULL CHECK (category IN ({_in(TICKET_CATEGORIES)})),
    subject TEXT NOT NULL CHECK (length(trim(subject)) > 0),
    status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ({_in(TICKET_STATUSES)})),
    assigned_admin_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    closed_at TEXT,
    closed_by TEXT,
    close_reason TEXT,
    CHECK (status != 'CLOSED' OR (closed_at IS NOT NULL AND closed_by IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_support_tickets_user ON support_tickets(user_id, updated_at);
CREATE INDEX IF NOT EXISTS idx_support_tickets_status ON support_tickets(status, updated_at);

CREATE TABLE IF NOT EXISTS support_ticket_messages (
    message_id TEXT PRIMARY KEY,
    ticket_id TEXT NOT NULL REFERENCES support_tickets(ticket_id),
    author_kind TEXT NOT NULL CHECK (author_kind IN ('USER', 'ADMIN')),
    author_id TEXT NOT NULL,
    body TEXT NOT NULL CHECK (length(trim(body)) > 0),
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_support_messages_ticket ON support_ticket_messages(ticket_id, created_at);

CREATE TRIGGER IF NOT EXISTS trg_support_messages_no_update BEFORE UPDATE ON support_ticket_messages
BEGIN SELECT RAISE(ABORT, 'support messages are append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_support_messages_no_delete BEFORE DELETE ON support_ticket_messages
BEGIN SELECT RAISE(ABORT, 'support messages are append-only'); END;

CREATE TABLE IF NOT EXISTS support_ticket_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id TEXT NOT NULL REFERENCES support_tickets(ticket_id),
    event TEXT NOT NULL,
    actor_kind TEXT NOT NULL CHECK (actor_kind IN ('USER', 'ADMIN', 'SYSTEM')),
    actor_id TEXT NOT NULL,
    detail TEXT,
    at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_support_events_ticket ON support_ticket_events(ticket_id, id);

CREATE TRIGGER IF NOT EXISTS trg_support_events_no_update BEFORE UPDATE ON support_ticket_events
BEGIN SELECT RAISE(ABORT, 'support events are append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_support_events_no_delete BEFORE DELETE ON support_ticket_events
BEGIN SELECT RAISE(ABORT, 'support events are append-only'); END;
"""


def ensure_support_schema(db: Any) -> None:
    with db.connect() as conn:
        conn.executescript(SUPPORT_SCHEMA_SQL)


__all__ = ["TICKET_CATEGORIES", "TICKET_STATUSES", "SUPPORT_SCHEMA_SQL", "ensure_support_schema"]
