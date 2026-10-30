"""Session revocation schema (Section H Step 8.4, finding F1). Additive and idempotent.

* ``users.token_version`` / ``admins.token_version`` (INTEGER, default 0):
  every access token carries the value it was minted with (claim ``tv``) and
  is accepted only while it still equals the stored one. Logout-all, password
  reset, suspension, privilege changes, refresh-token reuse and admin MFA
  enrolment / break-glass reset increase it, which ends every outstanding
  access token of that account at once.
* ``auth_sessions.family_id`` / ``admin_sessions.family_id`` (TEXT, indexed):
  the id of the first session row of a refresh-rotation chain (one device).
  Access tokens carry it (claim ``sid``) and are accepted only while the chain
  still has a live (not revoked) row, so signing one device out ends that
  device's access token too, and nothing else.

Called from ``migrations.migrate`` (step 13c) and lazily by the writers.
Readers tolerate a database that does not have the columns yet (version 0).
"""
from __future__ import annotations

import sqlite3

COLUMNS = (
    ("users", "token_version", "INTEGER DEFAULT 0"),
    ("admins", "token_version", "INTEGER DEFAULT 0"),
    ("auth_sessions", "family_id", "TEXT"),
    ("admin_sessions", "family_id", "TEXT"),
)

INDEXES = (
    ("idx_auth_sessions_family", "auth_sessions", "family_id"),
    ("idx_admin_sessions_family", "admin_sessions", "family_id"),
)


def _columns(conn, table: str) -> set:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _add_column(conn, table: str, column: str, col_type: str) -> bool:
    existing = _columns(conn, table)
    if not existing:
        return False  # table absent (created elsewhere later)
    if column in existing:
        return True
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    except sqlite3.OperationalError as exc:  # a concurrent process added it first
        if "duplicate column name" not in str(exc).lower():
            raise
    return True


def ensure_session_version_schema(conn) -> None:
    present = {}
    for table, column, col_type in COLUMNS:
        present[table] = _add_column(conn, table, column, col_type)
    for index, table, column in INDEXES:
        if present.get(table):
            conn.execute(f"CREATE INDEX IF NOT EXISTS {index} ON {table}({column})")


__all__ = ["ensure_session_version_schema"]
