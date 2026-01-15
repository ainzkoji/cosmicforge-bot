"""
KYC Database Schema
Migration script to create all KYC-related tables
"""
import sqlite3
from pathlib import Path


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
    -- All PII fields are stored encrypted
    full_legal_name_encrypted TEXT,
    date_of_birth_encrypted TEXT,
    nationality TEXT,  -- Country code, not PII
    country_of_residence TEXT,  -- Country code
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
    doc_type TEXT NOT NULL,  -- passport, national_id, drivers_license
    issuing_country TEXT,
    doc_number_hash TEXT,  -- Hashed, never stored plain
    front_file_ref TEXT,  -- Path/key to file in secure storage
    back_file_ref TEXT,   -- Null for passport
    file_content_type TEXT,
    file_size_bytes INTEGER,
    status TEXT NOT NULL DEFAULT 'pending_upload',
    -- pending_upload, pending_review, accepted, rejected
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
    provider TEXT DEFAULT 'internal',  -- internal, onfido, jumio, etc.
    provider_session_id TEXT,
    status TEXT NOT NULL DEFAULT 'not_started',
    -- not_started, pending, passed, failed
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
    reviewer_id TEXT,  -- NULL for automated, user_id for manual
    reviewer_type TEXT NOT NULL DEFAULT 'system',  -- system, admin
    decision TEXT NOT NULL,  -- approved, rejected, needs_resubmission
    reason_codes TEXT,  -- JSON array
    notes_encrypted TEXT,  -- Encrypted notes
    created_at TEXT NOT NULL,
    FOREIGN KEY (kyc_case_id) REFERENCES kyc_cases(id)
);

-- KYC Audit Log: Full audit trail
CREATE TABLE IF NOT EXISTS kyc_audit_log (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    kyc_case_id TEXT,
    event_type TEXT NOT NULL,
    event_data TEXT,  -- JSON, no PII
    actor_id TEXT,  -- Who performed the action
    actor_type TEXT DEFAULT 'user',  -- user, system, admin
    ip_address TEXT,
    user_agent TEXT,
    created_at TEXT NOT NULL
);

-- KYC Requirements Config: Action-based requirements (could be in code, but flexible here)
CREATE TABLE IF NOT EXISTS kyc_requirements_config (
    id TEXT PRIMARY KEY,
    action_name TEXT NOT NULL UNIQUE,  -- start_live_trading, become_signal_provider, etc.
    requires_kyc INTEGER NOT NULL DEFAULT 1,
    required_status TEXT DEFAULT 'approved',  -- Minimum KYC status needed
    country_exceptions TEXT,  -- JSON array of exempt countries
    tier_exceptions TEXT,  -- JSON array of exempt tiers
    description TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_kyc_cases_user_id ON kyc_cases(user_id);
CREATE INDEX IF NOT EXISTS idx_kyc_cases_status ON kyc_cases(status);
CREATE INDEX IF NOT EXISTS idx_kyc_profiles_user_id ON kyc_profiles(user_id);
CREATE INDEX IF NOT EXISTS idx_kyc_documents_user_id ON kyc_documents(user_id);
CREATE INDEX IF NOT EXISTS idx_kyc_documents_case_id ON kyc_documents(kyc_case_id);
CREATE INDEX IF NOT EXISTS idx_kyc_selfie_user_id ON kyc_selfie_checks(user_id);
CREATE INDEX IF NOT EXISTS idx_kyc_audit_user_id ON kyc_audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_kyc_audit_case_id ON kyc_audit_log(kyc_case_id);
CREATE INDEX IF NOT EXISTS idx_kyc_audit_event_type ON kyc_audit_log(event_type);

-- Insert default KYC requirements
INSERT OR IGNORE INTO kyc_requirements_config (id, action_name, requires_kyc, required_status, description, created_at, updated_at)
VALUES 
    ('req_live_trading', 'start_live_trading', 1, 'approved', 'Required for live trading with real funds', datetime('now'), datetime('now')),
    ('req_signal_provider', 'become_signal_provider', 1, 'approved', 'Required to become a copy trading signal provider', datetime('now'), datetime('now')),
    ('req_withdraw', 'withdraw_funds', 1, 'approved', 'Required to withdraw earnings', datetime('now'), datetime('now')),
    ('req_api_access', 'developer_api_access', 0, 'approved', 'Optional for API access', datetime('now'), datetime('now')),
    ('req_high_limits', 'increase_limits', 1, 'approved', 'Required for higher trading limits', datetime('now'), datetime('now'));
"""


def migrate():
    """Run KYC database migration"""
    db_path = Path(__file__).parent / "data" / "bot.db"
    
    print(f"[KYC Migration] Connecting to {db_path}")
    conn = sqlite3.connect(str(db_path))
    
    try:
        # Execute all table creation statements
        conn.executescript(KYC_TABLES_SQL)
        conn.commit()
        print("[KYC Migration] All KYC tables created successfully!")
        
        # Verify tables
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'kyc_%'")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"[KYC Migration] Created tables: {tables}")
        
    except Exception as e:
        print(f"[KYC Migration] Error: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
