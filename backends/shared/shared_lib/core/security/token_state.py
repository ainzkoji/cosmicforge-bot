"""Server-side state behind stateless access tokens (Section H Step 8.4, finding F1).

An access token is a signed JWT that lives ``ACCESS_TOKEN_EXPIRE_MINUTES``.
Its signature alone says nothing about what happened since it was minted, so
every validator of a user or admin access token (user-backend, admin-backend,
bot-backend, the SSE stream) also checks, with one indexed read:

* the account still exists and is active (``users.status == 'active'`` /
  ``admins.is_active == 1``);
* claim ``tv`` equals ``token_version`` of the account (a missing claim or
  column counts as 0). Bumped by logout-all, password reset, suspension,
  privilege changes, refresh-token reuse, admin MFA enrolment / reset;
* when the token carries ``sid`` (its refresh-rotation chain): that chain still
  has a live session row, so signing one device out ends that device's access
  token and nothing else.

Nothing is cached: a revoked token is refused on its very next request.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Mapping, Optional, Tuple

TOKEN_VERSION_CLAIM = "tv"
SESSION_CLAIM = "sid"

# Why a token was refused (never shown to a client verbatim beyond these codes).
UNKNOWN_SUBJECT = "unknown_subject"
INACTIVE = "inactive"
REVOKED = "token_revoked"
SESSION_ENDED = "session_ended"

USER_SESSIONS = ("auth_sessions", "user_id")
ADMIN_SESSIONS = ("admin_sessions", "admin_id")


def _value(row: Any, key: str, default=None):
    if row is None:
        return default
    try:
        return row[key]
    except (KeyError, IndexError):
        return default


def as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return -1  # never equal to a real version


def claimed_version(payload: Mapping[str, Any]) -> int:
    return as_int(payload.get(TOKEN_VERSION_CLAIM))


def stored_version(row: Any) -> int:
    return as_int(_value(row, "token_version"))


def session_chain_alive(conn, sessions: Tuple[str, str], subject: str, family_id: str) -> bool:
    """True while the rotation chain ``family_id`` of ``subject`` has a live row.

    Fails closed: a database without the column cannot have issued such a token."""
    table, owner = sessions
    try:
        row = conn.execute(
            f"SELECT 1 FROM {table} WHERE family_id = ? AND {owner} = ? AND revoked_at IS NULL LIMIT 1",
            (str(family_id), str(subject)),
        ).fetchone()
    except sqlite3.OperationalError:
        return False
    return row is not None


def user_token_problem(conn, payload: Mapping[str, Any]) -> Tuple[Optional[dict], Optional[str]]:
    """``(user_row, None)`` when a user access token is still current, else
    ``(row_or_None, reason)``."""
    subject = payload.get("sub")
    row = conn.execute("SELECT * FROM users WHERE id = ?", (subject,)).fetchone() if subject else None
    if row is None:
        return None, UNKNOWN_SUBJECT
    user = dict(row)
    if user.get("status") != "active":
        return user, INACTIVE
    if stored_version(user) != claimed_version(payload):
        return user, REVOKED
    sid = payload.get(SESSION_CLAIM)
    if sid and not session_chain_alive(conn, USER_SESSIONS, subject, sid):
        return user, SESSION_ENDED
    return user, None


def admin_token_problem(conn, payload: Mapping[str, Any], admin_row: Any = None
                        ) -> Tuple[Optional[dict], Optional[str]]:
    """Same for an admin access token. ``admin_row`` saves the read when the
    caller already loaded ``SELECT * FROM admins WHERE id = sub``."""
    subject = payload.get("sub")
    if admin_row is None and subject:
        admin_row = conn.execute("SELECT * FROM admins WHERE id = ?", (subject,)).fetchone()
    if admin_row is None:
        return None, UNKNOWN_SUBJECT
    admin = dict(admin_row)
    if as_int(admin.get("is_active")) != 1:
        return admin, INACTIVE
    if stored_version(admin) != claimed_version(payload):
        return admin, REVOKED
    sid = payload.get(SESSION_CLAIM)
    if sid and not session_chain_alive(conn, ADMIN_SESSIONS, subject, sid):
        return admin, SESSION_ENDED
    return admin, None


def read_token_version(conn, table: str, subject: str) -> int:
    """Current ``token_version`` of a ``users`` / ``admins`` row (0 when absent)."""
    assert table in ("users", "admins")
    try:
        row = conn.execute(f"SELECT token_version FROM {table} WHERE id = ?", (str(subject),)).fetchone()
    except sqlite3.OperationalError:
        return 0  # column not added yet: nothing was ever bumped
    return as_int(_value(row, "token_version", 0) if row is not None else 0)


def bump_token_version(conn, table: str, subject: str) -> None:
    """End every access token of the account that was minted before now."""
    assert table in ("users", "admins")
    from shared_lib.persistence.session_version_schema import ensure_session_version_schema

    ensure_session_version_schema(conn)
    conn.execute(f"UPDATE {table} SET token_version = COALESCE(token_version, 0) + 1 WHERE id = ?",
                 (str(subject),))


def bump_user_token_version(conn, user_id: str) -> None:
    bump_token_version(conn, "users", user_id)


def bump_admin_token_version(conn, admin_id: str) -> None:
    bump_token_version(conn, "admins", admin_id)


__all__ = [
    "ADMIN_SESSIONS", "INACTIVE", "REVOKED", "SESSION_CLAIM", "SESSION_ENDED", "TOKEN_VERSION_CLAIM",
    "UNKNOWN_SUBJECT", "USER_SESSIONS", "admin_token_problem", "bump_admin_token_version",
    "bump_token_version", "bump_user_token_version", "claimed_version", "read_token_version",
    "session_chain_alive", "stored_version", "user_token_problem",
]
