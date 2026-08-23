-- MT Bridge Pairing Sessions Table
-- Migration: Add table for MT4/MT5 pairing code flow

CREATE TABLE IF NOT EXISTS mt_pairing_sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    broker_id TEXT NOT NULL CHECK (broker_id IN ('mt4', 'mt5')),
    pairing_code TEXT UNIQUE NOT NULL,
    expires_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'paired', 'expired')),
    paired_account_login TEXT,
    paired_server TEXT,
    bridge_url TEXT,
    bridge_token_encrypted TEXT,
    tls_mode TEXT DEFAULT 'strict',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mt_pairing_code ON mt_pairing_sessions(pairing_code);
CREATE INDEX IF NOT EXISTS idx_mt_pairing_user ON mt_pairing_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_mt_pairing_status ON mt_pairing_sessions(status, expires_at);

-- Note: SQLite uses TEXT for all string types and doesn't enforce foreign keys by default
-- The user_id should reference users(id) but SQLite implementation may not require explicit FK
