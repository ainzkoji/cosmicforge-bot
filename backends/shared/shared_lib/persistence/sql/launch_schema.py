"""Platform tables for Step 6: go-live authorization, live-beta stages and incident records (versions 10-19).

Authorization state is never a mutable flag. Every grant, approval, consent, switch change and revocation is an
event in ``golive_events``: append-only (no UPDATE or DELETE path in code; triggers refuse them on SQLite and
PostgreSQL), hash-chained (``prev_hash`` -> ``hash``), and the current state is the fold of the events. Beta
reviews and incident records are likewise append-only documents; a correction is a new row.
"""
from __future__ import annotations

from typing import List

from .migrations import Migration
from .platform_schema import _drops, _in, render

GOLIVE_EVENT_TYPES = (
    "SWITCH_ON_REQUESTED", "SWITCH_ON_APPROVED", "SWITCH_ON_REJECTED", "SWITCH_OFF",
    "APPROVAL_REQUESTED", "APPROVAL_GRANTED", "APPROVAL_REJECTED", "APPROVAL_REVOKED",
    "CONSENT_GIVEN", "CONSENT_REVOKED",
    "DEMO_RUN_RECORDED", "DEMO_RUN_INVALIDATED",
    "BETA_INVITED", "BETA_INVITATION_REVOKED", "BETA_ENROLLED", "BETA_WITHDRAWN", "BETA_DISABLED",
    "POLICY_SET",
)
REVOKING_EVENTS = ("SWITCH_OFF", "APPROVAL_REVOKED", "CONSENT_REVOKED", "DEMO_RUN_INVALIDATED",
                   "BETA_INVITATION_REVOKED", "BETA_WITHDRAWN", "BETA_DISABLED")

_APPEND_ONLY = {
    "sqlite": lambda t: (
        f"CREATE TRIGGER {t}_no_update BEFORE UPDATE ON {t} BEGIN SELECT RAISE(ABORT, '{t.upper()}_APPEND_ONLY'); END",
        f"CREATE TRIGGER {t}_no_delete BEFORE DELETE ON {t} BEGIN SELECT RAISE(ABORT, '{t.upper()}_APPEND_ONLY'); END"),
    "postgresql": lambda t: (
        f"CREATE OR REPLACE FUNCTION {t}_append_only() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
        f"RAISE EXCEPTION '{t.upper()}_APPEND_ONLY'; END $$",
        f"CREATE TRIGGER {t}_no_mutation BEFORE UPDATE OR DELETE ON {t} FOR EACH ROW EXECUTE FUNCTION {t}_append_only()"),
}


def _with_append_only(statements, tables):
    body = render(statements)
    for dialect, maker in _APPEND_ONLY.items():
        extra = tuple(s for t in tables for s in maker(t))
        body[dialect] = body[dialect] + extra
    return body


def _drop_with_functions(*tables):
    drops = _drops(*tables)
    return {"sqlite": drops, "postgresql": drops + tuple(f"DROP FUNCTION IF EXISTS {t}_append_only()" for t in tables)}


V10 = [
    "CREATE TABLE golive_events (seq {ID}, event_id TEXT NOT NULL UNIQUE, "
    f"event_type TEXT NOT NULL CHECK (event_type IN {_in(*GOLIVE_EVENT_TYPES)}), "
    "scope_type TEXT NOT NULL, scope_id TEXT NOT NULL, subject_ref TEXT, actor_ref TEXT NOT NULL, payload {JSON} NOT NULL, "
    "recorded_at {BIGINT} NOT NULL, prev_hash TEXT NOT NULL, hash TEXT NOT NULL UNIQUE)",
    "CREATE INDEX ix_golive_events_scope ON golive_events (scope_type, scope_id, seq)",
]

BETA_REVIEW_KINDS = ("DAILY", "WEEKLY")
V11 = [
    "CREATE TABLE beta_reviews (review_id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK (kind IN ('DAILY','WEEKLY')), "
    "stage TEXT NOT NULL CHECK (stage IN ('A','B')), scope_id TEXT NOT NULL, period_start TEXT NOT NULL, "
    "period_end TEXT NOT NULL, reviewer_ref TEXT NOT NULL, document {JSON} NOT NULL, document_hash TEXT NOT NULL, "
    "verdict TEXT NOT NULL CHECK (verdict IN ('WITHIN_EXPECTATION','OUTSIDE_EXPECTATION','INCOMPLETE')), "
    "supersedes TEXT REFERENCES beta_reviews(review_id), recorded_at {BIGINT} NOT NULL)",
]

INCIDENT_SEVERITIES = ("SEV1", "SEV2", "SEV3", "SEV4")
V12 = [
    "CREATE TABLE incident_records (record_id TEXT PRIMARY KEY, incident_id TEXT NOT NULL, revision INTEGER NOT NULL, "
    "scenario TEXT NOT NULL, drill INTEGER NOT NULL CHECK (drill IN (0,1)), "
    f"severity TEXT NOT NULL CHECK (severity IN {_in(*INCIDENT_SEVERITIES)}), "
    "environment TEXT NOT NULL, started_at {BIGINT} NOT NULL, detected_at {BIGINT}, contained_at {BIGINT}, "
    "closed_at {BIGINT}, operator_ref TEXT NOT NULL, actions {JSON} NOT NULL, reconciliation_status TEXT NOT NULL, "
    "remaining_risk TEXT NOT NULL, root_cause TEXT, corrective_actions {JSON} NOT NULL, retest_evidence {JSON}, "
    "recorded_at {BIGINT} NOT NULL, UNIQUE (incident_id, revision))",
]

MIGRATIONS: List[Migration] = [
    Migration(10, "golive_authorization_ledger", _with_append_only(V10, ["golive_events"]),
              down=_drop_with_functions("golive_events")),
    Migration(11, "live_beta_reviews", _with_append_only(V11, ["beta_reviews"]),
              down=_drop_with_functions("beta_reviews")),
    Migration(12, "incident_records", _with_append_only(V12, ["incident_records"]),
              down=_drop_with_functions("incident_records")),
]
