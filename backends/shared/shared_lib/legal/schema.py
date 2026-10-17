"""Legal documents, acceptances, country eligibility and the legal audit log.

Additive and idempotent; called from ``shared_lib.persistence.migrations.migrate()``.

Integrity is enforced by the database itself, not only by the service code:

* ``legal_documents``: content (title, body, hash, version, category, locale,
  publication time, material-change flag) can never be changed or deleted
  once inserted -- a new version supersedes it. ``review_status`` is
  ``DRAFT_PENDING_COUNSEL`` unless a reviewer name AND reference are recorded,
  and a counsel approval cannot be changed afterwards.
* ``legal_acceptances``: only ``withdrawn_at`` / ``withdrawal_reason`` may be
  set, once; rows are never deleted.
* ``legal_audit_log``: append-only.
* ``legal_country_eligibility``: a determination other than ``UNDETERMINED``
  must carry a determination reference.
"""
from __future__ import annotations

from typing import Any

CATEGORIES = (
    "TERMS_OF_SERVICE",
    "PRIVACY_POLICY",
    "TRADING_RISK_DISCLOSURE",
    "ACCOUNT_CONNECTION_AUTHORIZATION",
    "AUTOMATED_EXECUTION_CONSENT",
    "DATA_PROCESSING_NOTICE",
    "SUBSCRIPTION_TERMS",
    "COUNTRY_ELIGIBILITY",
    "REGISTRATION_CONSENT",
)
REVIEW_STATUSES = ("DRAFT_PENDING_COUNSEL", "COUNSEL_APPROVED")
ELIGIBILITY_STATUSES = ("UNDETERMINED", "PERMITTED", "RESTRICTED", "PROHIBITED")


def _in(values) -> str:
    return ", ".join(f"'{v}'" for v in values)


LEGAL_SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS legal_documents (
    doc_id TEXT PRIMARY KEY,
    category TEXT NOT NULL CHECK (category IN ({_in(CATEGORIES)})),
    version INTEGER NOT NULL CHECK (version >= 1),
    locale TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    published_at TEXT NOT NULL,
    published_by TEXT,
    material_change INTEGER NOT NULL DEFAULT 1 CHECK (material_change IN (0, 1)),
    change_summary TEXT,
    supersedes_doc_id TEXT,
    review_status TEXT NOT NULL DEFAULT 'DRAFT_PENDING_COUNSEL'
        CHECK (review_status IN ({_in(REVIEW_STATUSES)})),
    reviewer_name TEXT,
    reviewer_reference TEXT,
    reviewed_at TEXT,
    review_recorded_by TEXT,
    CHECK (review_status != 'COUNSEL_APPROVED'
           OR (reviewer_name IS NOT NULL AND length(trim(reviewer_name)) > 0
               AND reviewer_reference IS NOT NULL AND length(trim(reviewer_reference)) > 0
               AND reviewed_at IS NOT NULL)),
    UNIQUE (category, locale, version)
);
CREATE INDEX IF NOT EXISTS idx_legal_documents_current ON legal_documents(category, locale, version);

CREATE TRIGGER IF NOT EXISTS trg_legal_documents_content_immutable
BEFORE UPDATE OF doc_id, category, version, locale, title, body, content_sha256, published_at, published_by,
                 material_change, change_summary, supersedes_doc_id ON legal_documents
BEGIN SELECT RAISE(ABORT, 'legal document content is immutable once published; publish a new version'); END;

CREATE TRIGGER IF NOT EXISTS trg_legal_documents_approval_final
BEFORE UPDATE ON legal_documents
WHEN OLD.review_status = 'COUNSEL_APPROVED'
BEGIN SELECT RAISE(ABORT, 'a recorded counsel approval cannot be changed'); END;

CREATE TRIGGER IF NOT EXISTS trg_legal_documents_no_delete
BEFORE DELETE ON legal_documents
BEGIN SELECT RAISE(ABORT, 'published legal documents cannot be deleted'); END;

CREATE TABLE IF NOT EXISTS legal_acceptances (
    acceptance_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    doc_id TEXT NOT NULL REFERENCES legal_documents(doc_id),
    category TEXT NOT NULL,
    version INTEGER NOT NULL,
    locale TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    review_status_at_acceptance TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    ip_hash TEXT,
    user_agent_hash TEXT,
    source TEXT NOT NULL,
    withdrawn_at TEXT,
    withdrawal_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_legal_acceptances_user ON legal_acceptances(user_id, category, accepted_at);

CREATE TRIGGER IF NOT EXISTS trg_legal_acceptances_evidence_immutable
BEFORE UPDATE OF acceptance_id, user_id, doc_id, category, version, locale, content_sha256,
                 review_status_at_acceptance, accepted_at, ip_hash, user_agent_hash, source ON legal_acceptances
BEGIN SELECT RAISE(ABORT, 'acceptance evidence is immutable'); END;

CREATE TRIGGER IF NOT EXISTS trg_legal_acceptances_withdraw_once
BEFORE UPDATE OF withdrawn_at ON legal_acceptances
WHEN OLD.withdrawn_at IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'acceptance already withdrawn'); END;

CREATE TRIGGER IF NOT EXISTS trg_legal_acceptances_no_delete
BEFORE DELETE ON legal_acceptances
BEGIN SELECT RAISE(ABORT, 'legal acceptances cannot be deleted'); END;

CREATE TABLE IF NOT EXISTS legal_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    actor_type TEXT NOT NULL,
    actor_id TEXT,
    action TEXT NOT NULL,
    user_id TEXT,
    doc_id TEXT,
    category TEXT,
    details_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_legal_audit_user ON legal_audit_log(user_id, id);

CREATE TRIGGER IF NOT EXISTS trg_legal_audit_log_no_update
BEFORE UPDATE ON legal_audit_log
BEGIN SELECT RAISE(ABORT, 'legal_audit_log is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_legal_audit_log_no_delete
BEFORE DELETE ON legal_audit_log
BEGIN SELECT RAISE(ABORT, 'legal_audit_log is append-only'); END;

CREATE TABLE IF NOT EXISTS legal_country_eligibility (
    country_code TEXT PRIMARY KEY CHECK (length(country_code) = 2 AND country_code = upper(country_code)),
    status TEXT NOT NULL DEFAULT 'UNDETERMINED' CHECK (status IN ({_in(ELIGIBILITY_STATUSES)})),
    determination_ref TEXT,
    determined_by TEXT,
    determined_at TEXT,
    notes TEXT,
    CHECK (status = 'UNDETERMINED'
           OR (determination_ref IS NOT NULL AND length(trim(determination_ref)) > 0
               AND determined_by IS NOT NULL AND determined_at IS NOT NULL))
);
"""


def ensure_legal_schema(db: Any) -> None:
    """Create the legal tables and seed the clearly-labelled DRAFT documents. Idempotent."""
    from shared_lib.legal.seed import seed_draft_documents

    with db.connect() as conn:
        conn.executescript(LEGAL_SCHEMA_SQL)
        seed_draft_documents(conn)


__all__ = ["CATEGORIES", "REVIEW_STATUSES", "ELIGIBILITY_STATUSES", "LEGAL_SCHEMA_SQL", "ensure_legal_schema"]
