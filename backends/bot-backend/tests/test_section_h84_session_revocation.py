"""Section H 8.4 closure: findings F1 (bot-backend validators) and F4.

F1 -- every bot-backend validator of a user access token re-checks, against
the shared database, the account status, ``users.token_version`` (claim
``tv``) and, when present, the device session chain (claim ``sid``):
``app.core.auth`` (get_current_user_id, get_current_active_user,
require_admin, require_permission), ``app.api.deps.get_current_active_user``
and open SSE streams. A refresh token is still never an access token.

F4 -- the dead truncate/zero-pad credential helpers are gone from
``app.core.security``.
"""
from __future__ import annotations

import asyncio
import time

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

USER = "h84-revocation-user"


def _token(*, sub=USER, token_type="access", role="user", tv=None, sid=None, lifetime=600, **extra) -> str:
    import jwt  # PyJWT (F6)

    from app.core.config import settings
    from app.core.security import AUDIENCE, ISSUER

    now = int(time.time())
    claims = {"sub": sub, "type": token_type, "role": role, "iss": ISSUER, "aud": AUDIENCE,
              "iat": now, "exp": now + lifetime, "permissions": ["bot:read"], **extra}
    if tv is not None:
        claims["tv"] = tv
    if sid is not None:
        claims["sid"] = sid
    return jwt.encode(claims, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "revocation.db").as_posix())
    d = DB()
    migrate(d)
    now = "2026-10-29T00:00:00+00:00"
    with d.connect() as conn:
        conn.execute("INSERT INTO users (id, email, hashed_password, status, role, created_at, updated_at) "
                     "VALUES (?, 'rev@example.com', 'x', 'active', 'user', ?, ?)", (USER, now, now))
        conn.execute("INSERT INTO auth_sessions (id, user_id, refresh_token_hash, created_at, expires_at, family_id) "
                     "VALUES ('chain-1', ?, 'h', ?, '2099-01-01T00:00:00+00:00', 'chain-1')", (USER, now))
    return d


def _execute(db, sql, params=()):
    with db.connect() as conn:
        conn.execute(sql, params)


def _validators():
    from app.api import deps
    from app.core import auth

    def bearer(token):
        return deps.get_current_active_user(HTTPAuthorizationCredentials(scheme="Bearer", credentials=token))

    return {
        "core.get_current_user_id": auth.get_current_user_id,
        "core.get_current_active_user": auth.get_current_active_user,
        "core.require_permission": auth.require_permission("bot:read"),
        "api.deps.get_current_active_user": bearer,
    }


def _status(call, token) -> int:
    try:
        call(token)
    except HTTPException as exc:
        return exc.status_code
    return 200


def test_a_current_token_passes_every_validator(db):
    token = _token(tv=0, sid="chain-1")
    assert {name: _status(call, token) for name, call in _validators().items()} == dict.fromkeys(_validators(), 200)


def test_a_bumped_token_version_is_refused_by_every_validator(db):
    token = _token(tv=0, sid="chain-1")
    _execute(db, "UPDATE users SET token_version = 1 WHERE id = ?", (USER,))
    assert {name: _status(call, token) for name, call in _validators().items()} == dict.fromkeys(_validators(), 401)
    assert _status(_validators()["core.get_current_user_id"], _token(tv=1, sid="chain-1")) == 200


def test_a_signed_out_device_session_is_refused(db):
    token = _token(tv=0, sid="chain-1")
    _execute(db, "UPDATE auth_sessions SET revoked_at = 'now' WHERE id = 'chain-1'")
    assert {name: _status(call, token) for name, call in _validators().items()} == dict.fromkeys(_validators(), 401)
    assert _status(_validators()["core.get_current_user_id"], _token(tv=0)) == 200  # other devices unaffected


def test_a_suspended_account_is_refused_even_by_claim_only_dependencies(db):
    _execute(db, "UPDATE users SET status = 'suspended' WHERE id = ?", (USER,))
    token = _token(tv=0)
    assert {name: _status(call, token) for name, call in _validators().items()} == dict.fromkeys(_validators(), 403)


def test_a_role_admin_token_is_rechecked_by_require_admin(db):
    from app.core.auth import require_admin

    _execute(db, "UPDATE users SET role = 'admin' WHERE id = ?", (USER,))
    assert require_admin(_token(role="admin", tv=0)) == USER
    _execute(db, "UPDATE users SET token_version = 3 WHERE id = ?", (USER,))
    assert _status(require_admin, _token(role="admin", tv=0)) == 401


def test_refresh_tokens_are_still_never_access_tokens(db):
    token = _token(token_type="refresh", tv=0)
    assert {name: _status(call, token) for name, call in _validators().items()} == dict.fromkeys(_validators(), 401)


def test_unknown_subjects_keep_their_previous_per_dependency_behaviour(db):
    from app.core import auth

    ghost = _token(sub="no-such-user", tv=0)
    assert _status(auth.get_current_user_id, ghost) == 200
    assert _status(auth.get_current_active_user, ghost) == 401


def test_the_admin_emergency_service_credential_is_not_looked_up_as_a_user(db):
    from app.core import auth

    service = _token(sub="admin-1", role="admin", lifetime=60, act=auth.EMERGENCY_ACTOR_CLAIM)
    assert auth.is_service_credential(__import__("jwt").decode(service, options={"verify_signature": False}))
    assert auth.require_admin_emergency(service) == "admin-1"
    assert auth.get_current_user_id(service) == "admin-1"
    assert _status(auth.get_current_active_user, service) == 401  # never a user
    long_lived = _token(sub="admin-1", role="admin", lifetime=600, act=auth.EMERGENCY_ACTOR_CLAIM)
    assert not auth.is_service_credential(__import__("jwt").decode(long_lived, options={"verify_signature": False}))


def test_an_open_sse_stream_ends_when_its_token_is_revoked(db, monkeypatch):
    from app.api import events as events_api

    monkeypatch.setattr(events_api, "SSE_REAUTH_SECONDS", 0.0)
    token = _token(tv=0, sid="chain-1")
    check = events_api._token_still_current(token)
    assert check() is True

    async def run():
        stream = events_api._event_stream(USER, False, check)
        first = await stream.__anext__()
        assert first["event"] == "connected"
        _execute(db, "UPDATE users SET token_version = 1 WHERE id = ?", (USER,))
        second = await asyncio.wait_for(stream.__anext__(), timeout=5)
        assert second["event"] == "auth_revoked"
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()

    asyncio.run(run())


def test_the_dead_credential_helpers_are_gone_and_nothing_imports_them():
    """F4: app.core.security no longer has the truncate/zero-pad Fernet helpers."""
    from pathlib import Path

    from app.core import security

    for name in ("encrypt_credential", "decrypt_credential", "_get_fernet"):
        assert not hasattr(security, name), name
    backend = Path(__file__).resolve().parents[1]
    for path in (backend / "app").rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="replace")
        assert "encrypt_credential(" not in text and "decrypt_credential(" not in text, path
