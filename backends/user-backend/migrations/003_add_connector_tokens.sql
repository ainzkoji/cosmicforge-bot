-- Add connector magic link support to MT pairing sessions
-- Migration 003: Add connector_link_token and claimed_at fields

ALTER TABLE mt_pairing_sessions ADD COLUMN connector_link_token TEXT;
ALTER TABLE mt_pairing_sessions ADD COLUMN connector_claimed_at TEXT;

-- Index for fast connector token lookups
CREATE INDEX IF NOT EXISTS idx_connector_link_token ON mt_pairing_sessions(connector_link_token) WHERE connector_link_token IS NOT NULL;
