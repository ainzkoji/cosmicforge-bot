"""KYC schema -- restored to the canonical migrations.

These tables were defined by ``backend/migrate_kyc.py`` in the pre-cleanup
monolith. The 2026-05-02 repository cleanup moved that script into
``repo_cleanup_backup_20260502_134654/`` without porting it, while the code that
queries the tables -- ``shared_lib.core.policy.kyc_policy`` and the user-backend
KYC API -- *was* ported. Every KYC evaluation in the canonical database therefore
failed with ``no such table: kyc_requirements_config``, and the runtime turned
that exception into "KYC not approved".

Ported verbatim, and additive: ``CREATE TABLE IF NOT EXISTS`` plus
``INSERT OR IGNORE`` of the product's own requirement policy. It seeds
*requirements* only. No KYC case, and no approval, is ever created here.
"""
from __future__ import annotations

from typing import Any

KYC_TABLES_SQL = """
-- KYC Cases: Main state machine for user verification
CREATE TABLE IF NOT EXISTS kyc_cases (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'not_started',
    -- not_started, in_progress, submitted, under_review, approved, rejected, needs_resubmission, expired
    required_steps TEXT DEFAULT '["personal_info","id_document","face_verification"]',
    completed_steps TEXT DEFAULT '[]',
    rejection_reason TEXT,
    rejection_codes TEXT,  -- JSON array of reason codes
    expires_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    submitted_at TEXT,
    approved_at TEXT,
    rejected_at TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- KYC Profiles: Encrypted personal information
CREATE TABLE IF NOT EXISTS kyc_profiles (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL UNIQUE,
    kyc_case_id TEXT NOT NULL,
    full_legal_name_encrypted TEXT,
    date_of_birth_encrypted TEXT,
    nationality TEXT,
    country_of_residence TEXT,
    address_line1_encrypted TEXT,
    address_city_encrypted TEXT,
    address_state TEXT,
    address_postal_code_encrypted TEXT,
    phone_encrypted TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (kyc_case_id) REFERENCES kyc_cases(id)
);

-- KYC Documents: Identity document metadata
CREATE TABLE IF NOT EXISTS kyc_documents (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    kyc_case_id TEXT NOT NULL,
    doc_type TEXT NOT NULL,
    issuing_country TEXT,
    doc_number_hash TEXT,
    front_file_ref TEXT,
    back_file_ref TEXT,
    file_content_type TEXT,
    file_size_bytes INTEGER,
    status TEXT NOT NULL DEFAULT 'pending_upload',
    rejection_reason TEXT,
    uploaded_at TEXT,
    reviewed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (kyc_case_id) REFERENCES kyc_cases(id)
);

-- KYC Selfie Checks: Face verification records
CREATE TABLE IF NOT EXISTS kyc_selfie_checks (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    kyc_case_id TEXT NOT NULL,
    provider TEXT DEFAULT 'internal',
    provider_session_id TEXT,
    status TEXT NOT NULL DEFAULT 'not_started',
    confidence_score REAL,
    failure_reason TEXT,
    selfie_file_ref TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (kyc_case_id) REFERENCES kyc_cases(id)
);

-- KYC Reviews: Admin/system review decisions
CREATE TABLE IF NOT EXISTS kyc_reviews (
    id TEXT PRIMARY KEY,
    kyc_case_id TEXT NOT NULL,
    reviewer_id TEXT,
    reviewer_type TEXT NOT NULL DEFAULT 'system',
    decision TEXT NOT NULL,
    reason_codes TEXT,
    notes_encrypted TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (kyc_case_id) REFERENCES kyc_cases(id)
);

-- KYC Audit Log: Full audit trail
CREATE TABLE IF NOT EXISTS kyc_audit_log (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    kyc_case_id TEXT,
    event_type TEXT NOT NULL,
    event_data TEXT,
    actor_id TEXT,
    actor_type TEXT DEFAULT 'user',
    ip_address TEXT,
    user_agent TEXT,
    created_at TEXT NOT NULL
);

-- KYC Requirements Config: Action-based requirements
CREATE TABLE IF NOT EXISTS kyc_requirements_config (
    id TEXT PRIMARY KEY,
    action_name TEXT NOT NULL UNIQUE,
    requires_kyc INTEGER NOT NULL DEFAULT 1,
    required_status TEXT DEFAULT 'approved',
    country_exceptions TEXT,
    tier_exceptions TEXT,
    description TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_kyc_cases_user_id ON kyc_cases(user_id);
CREATE INDEX IF NOT EXISTS idx_kyc_cases_status ON kyc_cases(status);
CREATE INDEX IF NOT EXISTS idx_kyc_profiles_user_id ON kyc_profiles(user_id);
CREATE INDEX IF NOT EXISTS idx_kyc_documents_user_id ON kyc_documents(user_id);
CREATE INDEX IF NOT EXISTS idx_kyc_documents_case_id ON kyc_documents(kyc_case_id);
CREATE INDEX IF NOT EXISTS idx_kyc_selfie_user_id ON kyc_selfie_checks(user_id);
CREATE INDEX IF NOT EXISTS idx_kyc_audit_user_id ON kyc_audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_kyc_audit_case_id ON kyc_audit_log(kyc_case_id);
CREATE INDEX IF NOT EXISTS idx_kyc_audit_event_type ON kyc_audit_log(event_type);

-- The product's KYC requirement policy, exactly as originally defined.
-- start_live_trading is "Required for live trading with real funds".
INSERT OR IGNORE INTO kyc_requirements_config (id, action_name, requires_kyc, required_status, description, created_at, updated_at)
VALUES
    ('req_live_trading', 'start_live_trading', 1, 'approved', 'Required for live trading with real funds', datetime('now'), datetime('now')),
    ('req_signal_provider', 'become_signal_provider', 1, 'approved', 'Required to become a copy trading signal provider', datetime('now'), datetime('now')),
    ('req_withdraw', 'withdraw_funds', 1, 'approved', 'Required to withdraw earnings', datetime('now'), datetime('now')),
    ('req_api_access', 'developer_api_access', 0, 'approved', 'Optional for API access', datetime('now'), datetime('now')),
    ('req_high_limits', 'increase_limits', 1, 'approved', 'Required for higher trading limits', datetime('now'), datetime('now'));
"""


def ensure_kyc_schema(db: Any) -> None:
    """Create the KYC tables and requirement policy if absent. Idempotent."""
    with db.connect() as conn:
        conn.executescript(KYC_TABLES_SQL)


__all__ = ["KYC_TABLES_SQL", "ensure_kyc_schema"]
