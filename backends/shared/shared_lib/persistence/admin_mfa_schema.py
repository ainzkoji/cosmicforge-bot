"""Admin MFA schema (Section H Step 8.4). Additive and idempotent.

* ``admins``: ``mfa_enabled`` (0/1), ``mfa_secret`` (encrypted TOTP secret,
  pending until the first code is verified), ``mfa_last_counter`` (time step
  of the last accepted code: replay protection), ``mfa_enrolled_at``.
* ``admin_sessions.mfa_verified``: 1 when the session was created after a
  second factor. A refresh of a password-only session is refused once the
  admin has MFA (or MFA is required).
* ``admin_mfa_recovery_codes``: salted SHA-256 hashes of one-time codes.
"""
from __future__ import annotations

import sqlite3

ADMIN_COLUMNS = (
    ("mfa_enabled", "INTEGER DEFAULT 0"),
    ("mfa_secret", "TEXT"),
    ("mfa_last_counter", "INTEGER"),
    ("mfa_enrolled_at", "TEXT"),
)


def _columns(conn, table: str) -> set:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _add_column(conn, table: str, column: str, col_type: str) -> None:
    existing = _columns(conn, table)
    if not existing or column in existing:
        return  # table absent (created elsewhere later) or already migrated
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    except sqlite3.OperationalError as exc:  # a concurrent process added it first
        if "duplicate column name" not in str(exc).lower():
            raise


def ensure_admin_mfa_schema(conn) -> None:
    for column, col_type in ADMIN_COLUMNS:
        _add_column(conn, "admins", column, col_type)
    _add_column(conn, "admin_sessions", "mfa_verified", "INTEGER DEFAULT 0")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_mfa_recovery_codes (
            id TEXT PRIMARY KEY,
            admin_id TEXT NOT NULL,
            code_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            used_at TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_mfa_recovery_admin ON admin_mfa_recovery_codes(admin_id)"
    )


__all__ = ["ensure_admin_mfa_schema"]
