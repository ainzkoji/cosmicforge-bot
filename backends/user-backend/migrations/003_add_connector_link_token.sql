-- Migration 003: Add connector_link_token for magic link flow
-- This enables one-click connector setup via secure link tokens

-- Add connector_link_token column to mt_pairing_sessions
ALTER TABLE mt_pairing_sessions ADD COLUMN connector_link_token TEXT;

-- Create index for fast token lookups
CREATE INDEX IF NOT EXISTS idx_mt_pairing_connector_token ON mt_pairing_sessions(connector_link_token);

-- Note: Existing sessions without connector_link_token will continue to work
-- with the legacy pairing code flow for backward compatibility
