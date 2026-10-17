"""Platform tables for Section H Step 8.5: production operations (alerting, on-call, public status page).

Versions 30-39 are reserved for this module (``platform_schema.MIGRATION_MODULES``). Every migration is additive
(new tables, indexes and append-only triggers) and has a ``down`` that drops exactly what it created.

Append-only logs (``ops_alert_events``, ``ops_oncall_history``, ``ops_status_incident_updates``) refuse UPDATE and
DELETE in the database itself -- SQLite ``RAISE(ABORT)`` triggers, a PostgreSQL trigger function -- so a history
row cannot be rewritten even by code that bypasses the service layer.

Nothing here stores a credential. Contact addresses of on-call people exist only when an administrator entered
them; the schema seeds no person, no rotation member and no contact.

Support tickets are NOT here: they live in the user-backend's legacy schema (``support_schema``) beside the
users they belong to.
"""
from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

from .database import POSTGRESQL, SQLITE
from .migrations import Migration

ALERT_SEVERITIES = ("CRITICAL", "HIGH", "MEDIUM", "LOW")
ALERT_STATUSES = ("OPEN", "ACKNOWLEDGED", "RESOLVED")
ALERT_SCOPES = ("PLATFORM", "ACCOUNT")
DELIVERY_STATUSES = ("PENDING", "SENT", "RETRY", "ABANDONED")
CONTACT_CHANNELS = ("EMAIL", "TELEGRAM", "WEBHOOK", "PHONE", "OTHER")
INCIDENT_STATUSES = ("INVESTIGATING", "IDENTIFIED", "MONITORING", "RESOLVED")
INCIDENT_IMPACTS = ("DEGRADED", "OUTAGE", "MAINTENANCE")

_TYPES = {
    SQLITE: {"BIGINT": "INTEGER", "ID": "INTEGER PRIMARY KEY AUTOINCREMENT", "JSON": "TEXT"},
    POSTGRESQL: {"BIGINT": "BIGINT", "ID": "BIGSERIAL PRIMARY KEY", "JSON": "TEXT"},
}


def _in(*values: str) -> str:
    return "(" + ",".join(f"'{v}'" for v in values) + ")"


def _append_only(table: str) -> Dict[str, Tuple[str, ...]]:
    return {
        SQLITE: (
            f"CREATE TRIGGER trg_{table}_no_update BEFORE UPDATE ON {table} "
            f"BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY:{table}'); END",
            f"CREATE TRIGGER trg_{table}_no_delete BEFORE DELETE ON {table} "
            f"BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY:{table}'); END",
        ),
        POSTGRESQL: (
            f"CREATE TRIGGER trg_{table}_append_only BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION ops_append_only()",
        ),
    }


def _drop_append_only(table: str) -> Dict[str, Tuple[str, ...]]:
    return {SQLITE: (f"DROP TRIGGER IF EXISTS trg_{table}_no_update", f"DROP TRIGGER IF EXISTS trg_{table}_no_delete"),
            POSTGRESQL: (f"DROP TRIGGER IF EXISTS trg_{table}_append_only ON {table}",)}


def _compose(tables: Sequence[str], append_only: Sequence[str], *, first: bool = False) -> Dict[str, Tuple[str, ...]]:
    out: Dict[str, List[str]] = {}
    for dialect, types in _TYPES.items():
        stmts: List[str] = []
        if first and dialect == POSTGRESQL:
            stmts.append("CREATE OR REPLACE FUNCTION ops_append_only() RETURNS trigger AS $$ BEGIN "
                         "RAISE EXCEPTION USING MESSAGE = 'APPEND_ONLY:' || TG_TABLE_NAME; END $$ LANGUAGE plpgsql")
        stmts.extend(s.format(**types) for s in tables)
        for t in append_only:
            stmts.extend(_append_only(t)[dialect])
        out[dialect] = stmts
    return {d: tuple(s) for d, s in out.items()}


def _down(tables: Sequence[str], append_only: Sequence[str], *, last: bool = False) -> Dict[str, Tuple[str, ...]]:
    out = {}
    for dialect in (SQLITE, POSTGRESQL):
        stmts: List[str] = []
        for t in append_only:
            stmts.extend(_drop_append_only(t)[dialect])
        stmts.extend(f"DROP TABLE IF EXISTS {t}" for t in tables)
        if last and dialect == POSTGRESQL:
            stmts.append("DROP FUNCTION IF EXISTS ops_append_only()")
        out[dialect] = tuple(stmts)
    return out


# ── 30. alerting ────────────────────────────────────────────────────────────────────────────────────────────
V30 = [
    "CREATE TABLE ops_alerts (alert_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, category TEXT NOT NULL, "
    f"severity TEXT NOT NULL CHECK (severity IN {_in(*ALERT_SEVERITIES)}), "
    f"status TEXT NOT NULL CHECK (status IN {_in(*ALERT_STATUSES)}), title TEXT NOT NULL, detail {{JSON}} NOT NULL, "
    f"source TEXT NOT NULL, scope TEXT NOT NULL CHECK (scope IN {_in(*ALERT_SCOPES)}), account_ref TEXT, tenant_ref TEXT, "
    "rotation_key TEXT NOT NULL, escalation_level INTEGER NOT NULL DEFAULT 0 CHECK (escalation_level >= 0), "
    "owner_required INTEGER NOT NULL DEFAULT 0 CHECK (owner_required IN (0,1)), "
    "occurrences INTEGER NOT NULL DEFAULT 1 CHECK (occurrences >= 1), first_seen_at {BIGINT} NOT NULL, "
    "last_seen_at {BIGINT} NOT NULL, last_notified_at {BIGINT}, next_escalation_at {BIGINT}, "
    "acknowledged_at {BIGINT}, acknowledged_by TEXT, resolved_at {BIGINT}, resolved_by TEXT, resolution_note TEXT, "
    "CHECK (scope = 'PLATFORM' OR account_ref IS NOT NULL), "
    "CHECK (status <> 'ACKNOWLEDGED' OR acknowledged_by IS NOT NULL), "
    "CHECK (status <> 'RESOLVED' OR (resolved_by IS NOT NULL AND resolved_at IS NOT NULL)))",
    # One unresolved alert per fingerprint: concurrent raisers converge on one row instead of racing.
    "CREATE UNIQUE INDEX ux_ops_alerts_open_fingerprint ON ops_alerts (fingerprint) WHERE status <> 'RESOLVED'",
    "CREATE INDEX ix_ops_alerts_status ON ops_alerts (status, severity, last_seen_at)",
    "CREATE INDEX ix_ops_alerts_escalation ON ops_alerts (status, next_escalation_at)",
    "CREATE TABLE ops_alert_events (id {ID}, alert_id TEXT NOT NULL REFERENCES ops_alerts(alert_id), "
    "event TEXT NOT NULL, actor TEXT NOT NULL, escalation_level INTEGER, detail {JSON}, at {BIGINT} NOT NULL)",
    "CREATE INDEX ix_ops_alert_events_alert ON ops_alert_events (alert_id, id)",
    "CREATE TABLE ops_alert_deliveries (delivery_id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE, "
    "alert_id TEXT NOT NULL REFERENCES ops_alerts(alert_id), reason TEXT NOT NULL, escalation_level INTEGER NOT NULL, "
    f"sink TEXT NOT NULL, targets {{JSON}} NOT NULL, status TEXT NOT NULL CHECK (status IN {_in(*DELIVERY_STATUSES)}), "
    "attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL CHECK (max_attempts > 0), "
    "next_attempt_at {BIGINT} NOT NULL, last_error TEXT, sent_at {BIGINT}, created_at {BIGINT} NOT NULL, "
    "updated_at {BIGINT} NOT NULL)",
    "CREATE INDEX ix_ops_alert_deliveries_due ON ops_alert_deliveries (status, next_attempt_at)",
]

# ── 31. on-call ownership ───────────────────────────────────────────────────────────────────────────────────
V31 = [
    "CREATE TABLE ops_oncall_rotations (rotation_key TEXT PRIMARY KEY, description TEXT NOT NULL, "
    "created_at {BIGINT} NOT NULL, created_by TEXT NOT NULL)",
    "CREATE TABLE ops_oncall_members (member_id TEXT PRIMARY KEY, "
    "rotation_key TEXT NOT NULL REFERENCES ops_oncall_rotations(rotation_key), "
    "level INTEGER NOT NULL CHECK (level BETWEEN 0 AND 2), person_name TEXT NOT NULL CHECK (length(person_name) > 0), "
    f"contact_channel TEXT NOT NULL CHECK (contact_channel IN {_in(*CONTACT_CHANNELS)}), "
    "contact_address TEXT NOT NULL CHECK (length(contact_address) > 0), assigned_by TEXT NOT NULL, "
    "assigned_at {BIGINT} NOT NULL, removed_at {BIGINT}, removed_by TEXT, "
    "CHECK (removed_at IS NULL OR removed_by IS NOT NULL))",
    "CREATE INDEX ix_ops_oncall_members_active ON ops_oncall_members (rotation_key, level, removed_at)",
    "CREATE TABLE ops_oncall_history (id {ID}, rotation_key TEXT NOT NULL, member_id TEXT, action TEXT NOT NULL, "
    "level INTEGER, person_name TEXT, actor TEXT NOT NULL, at {BIGINT} NOT NULL)",
]

# ── 32. public status page ──────────────────────────────────────────────────────────────────────────────────
V32 = [
    "CREATE TABLE ops_status_incidents (incident_id TEXT PRIMARY KEY, title TEXT NOT NULL, "
    f"status TEXT NOT NULL CHECK (status IN {_in(*INCIDENT_STATUSES)}), "
    f"impact TEXT NOT NULL CHECK (impact IN {_in(*INCIDENT_IMPACTS)}), components {{JSON}} NOT NULL, "
    "created_at {BIGINT} NOT NULL, created_by TEXT NOT NULL, updated_at {BIGINT} NOT NULL, resolved_at {BIGINT}, "
    "CHECK (status <> 'RESOLVED' OR resolved_at IS NOT NULL))",
    "CREATE INDEX ix_ops_status_incidents_open ON ops_status_incidents (status, updated_at)",
    "CREATE TABLE ops_status_incident_updates (id {ID}, "
    "incident_id TEXT NOT NULL REFERENCES ops_status_incidents(incident_id), "
    f"status TEXT NOT NULL CHECK (status IN {_in(*INCIDENT_STATUSES)}), "
    f"impact TEXT NOT NULL CHECK (impact IN {_in(*INCIDENT_IMPACTS)}), message TEXT NOT NULL, actor TEXT NOT NULL, "
    "at {BIGINT} NOT NULL)",
    "CREATE INDEX ix_ops_status_updates_incident ON ops_status_incident_updates (incident_id, id)",
]

MIGRATIONS: List[Migration] = [
    Migration(30, "operations_alerting", _compose(V30, ["ops_alert_events"], first=True),
              down=_down(["ops_alert_deliveries", "ops_alert_events", "ops_alerts"], ["ops_alert_events"], last=True)),
    Migration(31, "operations_oncall_ownership", _compose(V31, ["ops_oncall_history"]),
              down=_down(["ops_oncall_history", "ops_oncall_members", "ops_oncall_rotations"], ["ops_oncall_history"])),
    Migration(32, "operations_status_page", _compose(V32, ["ops_status_incident_updates"]),
              down=_down(["ops_status_incident_updates", "ops_status_incidents"], ["ops_status_incident_updates"])),
]

OPERATIONS_TABLES = ("ops_alerts", "ops_alert_events", "ops_alert_deliveries", "ops_oncall_rotations",
                     "ops_oncall_members", "ops_oncall_history", "ops_status_incidents", "ops_status_incident_updates")
