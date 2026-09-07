PRAGMA foreign_keys=off;

BEGIN TRANSACTION;

-- Create new table with updated CHECK constraint
CREATE TABLE mt_pairing_sessions_new (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    broker_id TEXT NOT NULL CHECK (broker_id IN ('mt4', 'mt5')),
    pairing_code TEXT UNIQUE NOT NULL,
    expires_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'paired', 'expired', 'completed')),
    
    account_login TEXT,
    account_server TEXT,
    
    bridge_url TEXT,
    encrypted_bridge_token TEXT,
    tls_mode TEXT DEFAULT 'strict',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    
    environment TEXT NOT NULL DEFAULT 'live',
    account_currency TEXT,
    account_type TEXT,
    account_platform TEXT,
    account_fingerprint TEXT,
    key_metadata TEXT DEFAULT 'fernet_v1',
    connector_link_token TEXT
);

-- Copy data with explicit column mapping
INSERT INTO mt_pairing_sessions_new (
    id, user_id, broker_id, pairing_code, expires_at, status, 
    account_login, account_server, bridge_url, encrypted_bridge_token, tls_mode, created_at, updated_at,
    environment, account_currency, account_type, account_platform, account_fingerprint, key_metadata,
    connector_link_token
)
SELECT 
    id, user_id, broker_id, pairing_code, expires_at, status, 
    account_login, account_server, bridge_url, encrypted_bridge_token, tls_mode, created_at, updated_at,
    environment, account_currency, account_type, account_platform, account_fingerprint, key_metadata,
    connector_link_token
FROM mt_pairing_sessions;

-- Drop old table
DROP TABLE mt_pairing_sessions;

-- Rename new table
ALTER TABLE mt_pairing_sessions_new RENAME TO mt_pairing_sessions;

-- Recreate indices
CREATE INDEX idx_mt_pairing_code ON mt_pairing_sessions(pairing_code);
CREATE INDEX idx_mt_pairing_user ON mt_pairing_sessions(user_id);
CREATE INDEX idx_mt_pairing_status ON mt_pairing_sessions(status, expires_at);
CREATE INDEX idx_mt_pairing_connector_token ON mt_pairing_sessions(connector_link_token);

COMMIT;

PRAGMA foreign_keys=on;
