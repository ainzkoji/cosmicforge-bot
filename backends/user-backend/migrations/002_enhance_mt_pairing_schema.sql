-- Enhance MT Pairing Schema to match spec
-- SQLite 3.25+ supports RENAME COLUMN

ALTER TABLE mt_pairing_sessions ADD COLUMN environment TEXT NOT NULL DEFAULT 'live';
ALTER TABLE mt_pairing_sessions ADD COLUMN account_currency TEXT;
ALTER TABLE mt_pairing_sessions ADD COLUMN account_type TEXT;
ALTER TABLE mt_pairing_sessions ADD COLUMN account_platform TEXT;
ALTER TABLE mt_pairing_sessions ADD COLUMN account_fingerprint TEXT;
ALTER TABLE mt_pairing_sessions ADD COLUMN key_metadata TEXT DEFAULT 'fernet_v1';

ALTER TABLE mt_pairing_sessions RENAME COLUMN bridge_token_encrypted TO encrypted_bridge_token;
ALTER TABLE mt_pairing_sessions RENAME COLUMN paired_account_login TO account_login;
ALTER TABLE mt_pairing_sessions RENAME COLUMN paired_server TO account_server;
