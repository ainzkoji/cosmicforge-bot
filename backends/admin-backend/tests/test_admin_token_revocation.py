"""Section H 8.4 closure (finding F1) for the admin-backend.

The admin-backend validates admin access tokens itself
(``app.core.auth.get_active_admin_from_token``). Besides ``is_active`` it now
re-checks ``admins.token_version`` (claim ``tv``) and, when the token names one,
the live ``admin_sessions`` rotation chain (claim ``sid``), so an MFA
enrolment / reset, a refresh-token replay, a password reset or a sign-out on
the user-backend ends the token here on its next request.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi import HTTPException
from jose import jwt

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "admin-backend"))
sys.path.insert(0, str(ROOT / "backends" / "shared"))

from app.core.auth import get_active_admin_from_token  # noqa: E402
from app.core.config import settings  # noqa: E402
from app.persistence.db import AdminDB  # noqa: E402
from shared_lib.persistence.session_version_schema import ensure_session_version_schema  # noqa: E402

ADMIN = "admin-revocation"


def _token(*, tv=None, sid=None, sub=ADMIN, token_type="admin_access") -> str:
    now = datetime.now(timezone.utc)
    claims = {"exp": now + timedelta(minutes=15), "nbf": now, "iat": now, "iss": "cosmicforge-admin-backend",
              "aud": "admin-portal", "sub": sub, "type": token_type, "role": "admin"}
    if tv is not None:
        claims["tv"] = tv
    if sid is not None:
        claims["sid"] = sid
    return jwt.encode(claims, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def _db(tmp_path: Path, *, migrated: bool = True) -> AdminDB:
    path = tmp_path / "admin_revocation.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE admins (id TEXT PRIMARY KEY, email TEXT, role TEXT, is_active INTEGER DEFAULT 1)")
    conn.execute("CREATE TABLE admin_sessions (id TEXT PRIMARY KEY, admin_id TEXT, refresh_token_hash TEXT, "
                 "created_at TEXT, expires_at TEXT, revoked_at TEXT)")
    conn.execute("INSERT INTO admins (id, email, role, is_active) VALUES (?, 'ops@example.com', 'admin', 1)",
                 (ADMIN,))
    if migrated:
        ensure_session_version_schema(conn)
    conn.commit()
    conn.close()
    return AdminDB(path=str(path))


def _execute(db: AdminDB, sql: str, params=()) -> None:
    with db.connect() as conn:
        conn.execute(sql, params)
        conn.commit()


def _refused(token: str, db: AdminDB) -> int:
    with pytest.raises(HTTPException) as caught:
        get_active_admin_from_token(token, db)
    return caught.value.status_code


def test_a_current_token_is_accepted(tmp_path):
    db = _db(tmp_path)
    assert get_active_admin_from_token(_token(tv=0), db)["id"] == ADMIN


def test_a_bumped_token_version_ends_the_token(tmp_path):
    db = _db(tmp_path)
    token = _token(tv=0)
    _execute(db, "UPDATE admins SET token_version = 1 WHERE id = ?", (ADMIN,))
    assert _refused(token, db) == 401
    assert get_active_admin_from_token(_token(tv=1), db)["id"] == ADMIN


def test_a_signed_out_session_chain_ends_the_token(tmp_path):
    db = _db(tmp_path)
    _execute(db, "INSERT INTO admin_sessions (id, admin_id, family_id, revoked_at) VALUES ('s1', ?, 's1', NULL)",
             (ADMIN,))
    token = _token(tv=0, sid="s1")
    assert get_active_admin_from_token(token, db)["id"] == ADMIN
    _execute(db, "UPDATE admin_sessions SET revoked_at = 'now' WHERE id = 's1'")
    assert _refused(token, db) == 401
    assert _refused(_token(tv=0, sid="no-such-chain"), db) == 401


def test_inactive_and_unknown_admins_and_wrong_types_are_refused(tmp_path):
    db = _db(tmp_path)
    assert _refused(_token(tv=0, sub="nobody"), db) == 401
    assert _refused(_token(tv=0, token_type="admin_refresh"), db) == 401
    _execute(db, "UPDATE admins SET is_active = 0 WHERE id = ?", (ADMIN,))
    assert _refused(_token(tv=0), db) == 403


def test_a_database_without_the_new_columns_reads_as_version_zero(tmp_path):
    """Before the user-backend migration ran: legacy tokens (no tv) still work."""
    db = _db(tmp_path, migrated=False)
    assert get_active_admin_from_token(_token(), db)["id"] == ADMIN
    assert _refused(_token(tv=1), db) == 401
    assert _refused(_token(tv=0, sid="s1"), db) == 401  # a chain the database cannot have issued: fail closed
