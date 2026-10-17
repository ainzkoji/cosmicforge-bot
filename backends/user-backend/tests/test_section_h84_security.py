"""Section H Step 8.4: security hardening for the public launch.

* Admin TOTP MFA: enrolment, two-step login, replay protection, single-use
  hashed recovery codes, ADMIN_MFA_REQUIRED (enrolment-only token), disabling
  needs a current code, audit rows, password-only sessions cannot outlive MFA.
* httpOnly cookie sessions with double-submit CSRF, bearer compatibility
  window (AUTH_BEARER_COMPAT), refresh/logout through cookies, Secure flag.
* Credential encryption: HKDF-derived key, versioned ciphertexts, legacy
  ciphertexts still decrypt, re-encryption sweep, nothing logged.
* Audit regressions: token type confusion, refresh reuse (admin), reset codes
  single-use / expiring, sessions revoked on reset, tenant isolation of broker
  accounts, credentials never in broker responses, CORS never "*".

Everything runs against a throw-away database; nothing uses the network.
"""
from __future__ import annotations

import base64
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pyotp
import pytest
from cryptography.fernet import Fernet
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from passlib.context import CryptContext

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import admin_auth, auth  # noqa: E402
from app.core import deps, security  # noqa: E402
from app.core import session_cookies as sc  # noqa: E402
from app.core.config import settings  # noqa: E402
from shared_lib.core.http_surface import browser_origins  # noqa: E402
from shared_lib.core.security.session_cookies import ADMIN_REALM, USER_REALM  # noqa: E402
from shared_lib.persistence.db import DB, utc_now_iso  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

AUTH = "/api/v1/auth"
ADMIN_AUTH = "/api/v1/admin-auth"
EMAIL = "h84-trader@example.com"
PASSWORD = "h84 correct horse battery staple"
ADMIN_EMAIL = "h84-ops@example.com"
ADMIN_PASSWORD = "h84 admin pass phrase for tests"
ADMIN_ID = "admin-h84"


# --------------------------------------------------------------------------- fixture

@pytest.fixture
def ctx(tmp_path, monkeypatch):
    db_path = tmp_path / "h84.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("BROKER_SECRET_KEY", Fernet.generate_key().decode())
    migrate(str(db_path))
    db = DB(path=str(db_path))

    for module in (auth, admin_auth, deps):
        monkeypatch.setattr(module, "DB", lambda: db)
    monkeypatch.setattr(security, "get_user_subscription", lambda uid: {"entitlements": {}})
    monkeypatch.setattr(auth, "get_user_subscription", lambda uid: {"entitlements": {}})
    monkeypatch.setattr(security, "pwd_context", CryptContext(schemes=["bcrypt"], bcrypt__rounds=4))
    monkeypatch.setattr(security, "_DUMMY_PASSWORD_HASH", None)
    issued = iter(f"{n:06d}" for n in range(200001, 200999))
    monkeypatch.setattr(auth, "generate_otp", lambda: next(issued))
    sent: list = []
    monkeypatch.setattr(auth, "_deliver_code", lambda email, purpose, code: sent.append((email, purpose, code)))

    # Defaults under test unless a test says otherwise.
    monkeypatch.setattr(settings, "ADMIN_MFA_REQUIRED", False)
    monkeypatch.setattr(settings, "AUTH_BEARER_COMPAT", True)
    monkeypatch.setattr(settings, "AUTH_COOKIE_SECURE", None)

    app = FastAPI()
    app.include_router(auth.router, prefix=AUTH)
    app.include_router(admin_auth.router, prefix=ADMIN_AUTH)

    from app.api.brokers import router as brokers_router
    app.include_router(brokers_router, prefix="/api/v1/brokers")

    @app.get("/api/v1/probe")
    def probe_get(user_id: str = Depends(auth.get_current_user_id)):
        return {"user_id": user_id}

    @app.post("/api/v1/probe")
    def probe_post(user_id: str = Depends(auth.get_current_user_id)):
        return {"user_id": user_id}

    @app.post("/api/admin/probe")
    def admin_probe(admin: dict = Depends(deps.get_current_admin)):
        return {"admin_id": admin["id"]}

    sc.install_session_cookie_middleware(app)
    clock = SimpleNamespace(now=float(int(time.time()) // 30 * 30 + 10))
    monkeypatch.setattr(security, "_totp_time", lambda: clock.now)
    return SimpleNamespace(client=TestClient(app), db=db, sent=sent, clock=clock, app=app)


# --------------------------------------------------------------------------- helpers

def _rows(ctx, sql, params=()):
    with ctx.db.connect() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _execute(ctx, sql, params=()):
    with ctx.db.connect() as conn:
        conn.execute(sql, params)


def _fresh_client(ctx) -> TestClient:
    return TestClient(ctx.app)


def _activate_user(ctx, email=EMAIL, password=PASSWORD):
    assert ctx.client.post(f"{AUTH}/register", json={"email": email, "password": password}).status_code == 200
    code = ctx.sent[-1][2]
    assert ctx.client.post(f"{AUTH}/verify-email", json={"email": email, "code": code}).status_code == 200


def _user_login(client, email=EMAIL, password=PASSWORD):
    return client.post(f"{AUTH}/login", data={"username": email, "password": password})


def _user_id(ctx, email=EMAIL) -> str:
    return _rows(ctx, "SELECT id FROM users WHERE email = ?", (email,))[0]["id"]


def _make_admin(ctx, email=ADMIN_EMAIL, password=ADMIN_PASSWORD, admin_id=ADMIN_ID):
    now = utc_now_iso()
    _execute(ctx, "INSERT INTO admins (id, email, hashed_password, full_name, role, is_active, is_superuser, "
                  "created_at, updated_at) VALUES (?, ?, ?, 'Ops', 'admin', 1, 1, ?, ?)",
             (admin_id, email, security.get_password_hash(password), now, now))


def _admin_login(client, password=ADMIN_PASSWORD, email=ADMIN_EMAIL):
    return client.post(f"{ADMIN_AUTH}/login", data={"username": email, "password": password})


def _code(ctx, secret: str) -> str:
    return pyotp.TOTP(secret).generate_otp(int(ctx.clock.now) // 30)


def _next_step(ctx) -> None:
    ctx.clock.now += 30


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _set_cookie_headers(response) -> list:
    headers = response.headers
    if hasattr(headers, "multi_items"):  # httpx response
        return [v for k, v in headers.multi_items() if k.lower() == "set-cookie"]
    return headers.getlist("set-cookie")  # starlette response


def _cookie_header(response, name: str) -> str:
    return next(h for h in _set_cookie_headers(response) if h.startswith(name + "="))


def _enroll_admin(ctx, client=None) -> SimpleNamespace:
    """Password login + voluntary enrolment with the session. Returns secret + codes."""
    client = client or ctx.client
    login = _admin_login(client)
    assert login.status_code == 200, login.text
    access = login.json()["access_token"]
    setup = client.post(f"{ADMIN_AUTH}/mfa/setup", headers=_bearer(access))
    assert setup.status_code == 200, setup.text
    secret = setup.json()["secret"]
    enroll = client.post(f"{ADMIN_AUTH}/mfa/enroll", json={"code": _code(ctx, secret)}, headers=_bearer(access))
    assert enroll.status_code == 200, enroll.text
    _next_step(ctx)
    return SimpleNamespace(secret=secret, codes=enroll.json()["recovery_codes"], enroll=enroll, old_login=login)


def _audit_events(ctx, event_type: str) -> list:
    return _rows(ctx, "SELECT * FROM auth_audit_log WHERE event_type = ?", (event_type,))


# =========================================================================== admin MFA

def test_admin_without_mfa_logs_in_with_a_session_when_not_required(ctx):
    _make_admin(ctx)
    response = _admin_login(ctx.client)
    assert response.status_code == 200
    body = response.json()
    assert body["access_token"] and body["refresh_token"] and body["csrf_token"]
    assert "mfa_required" not in body and "enrollment_token" not in body


def test_setup_returns_a_provisioning_uri_and_stores_the_secret_encrypted(ctx):
    _make_admin(ctx)
    access = _admin_login(ctx.client).json()["access_token"]
    setup = ctx.client.post(f"{ADMIN_AUTH}/mfa/setup", headers=_bearer(access))
    assert setup.status_code == 200
    secret = setup.json()["secret"]
    uri = setup.json()["provisioning_uri"]
    assert uri.startswith("otpauth://totp/") and f"secret={secret}" in uri and "issuer=CosmicForge" in uri
    stored = _rows(ctx, "SELECT mfa_secret, mfa_enabled FROM admins WHERE id = ?", (ADMIN_ID,))[0]
    assert stored["mfa_secret"].startswith("enc:v2:") and secret not in stored["mfa_secret"]
    assert security.decrypt_totp_secret(stored["mfa_secret"]) == secret
    assert not stored["mfa_enabled"]  # pending until the first code


def test_enrolment_needs_a_valid_code_returns_hashed_recovery_codes_and_ends_old_sessions(ctx):
    _make_admin(ctx)
    login = _admin_login(ctx.client)
    access, old_refresh = login.json()["access_token"], login.json()["refresh_token"]
    secret = ctx.client.post(f"{ADMIN_AUTH}/mfa/setup", headers=_bearer(access)).json()["secret"]

    wrong = "000000" if _code(ctx, secret) != "000000" else "111111"
    assert ctx.client.post(f"{ADMIN_AUTH}/mfa/enroll", json={"code": wrong}, headers=_bearer(access)).status_code == 400

    enroll = ctx.client.post(f"{ADMIN_AUTH}/mfa/enroll", json={"code": _code(ctx, secret)}, headers=_bearer(access))
    assert enroll.status_code == 200, enroll.text
    codes = enroll.json()["recovery_codes"]
    assert len(codes) == security.RECOVERY_CODE_COUNT == len(set(codes))
    assert enroll.json()["access_token"]  # a fresh, MFA-verified session

    stored = _rows(ctx, "SELECT code_hash FROM admin_mfa_recovery_codes WHERE admin_id = ?", (ADMIN_ID,))
    assert len(stored) == len(codes)
    assert not any(code in row["code_hash"] or code.replace("-", "") in row["code_hash"]
                   for code in codes for row in stored)
    assert len(_audit_events(ctx, "admin_mfa_enrolled")) == 1

    # The password-only session from before enrolment is over.
    assert _fresh_client(ctx).post(f"{ADMIN_AUTH}/refresh", json={"refresh_token": old_refresh}).status_code == 401


def test_enrolled_admin_login_is_two_step_and_the_step_token_grants_nothing(ctx):
    _make_admin(ctx)
    enrolled = _enroll_admin(ctx)
    client = _fresh_client(ctx)

    first = _admin_login(client)
    assert first.status_code == 200
    body = first.json()
    assert body["mfa_required"] is True and body["mfa_token"]
    assert "access_token" not in body and "refresh_token" not in body
    assert not any(h.startswith(ADMIN_REALM.access_cookie + "=") for h in _set_cookie_headers(first))

    # The challenge token is not an admin session anywhere.
    assert client.get(f"{ADMIN_AUTH}/me", headers=_bearer(body["mfa_token"])).status_code == 401
    assert client.post("/api/admin/probe", headers=_bearer(body["mfa_token"])).status_code == 401
    assert client.post(f"{ADMIN_AUTH}/refresh", json={"refresh_token": body["mfa_token"]}).status_code == 401
    assert security.decode_admin_token(body["mfa_token"]) is None  # foreign audience

    second = client.post(f"{ADMIN_AUTH}/mfa/verify",
                         json={"mfa_token": body["mfa_token"], "code": _code(ctx, enrolled.secret)})
    assert second.status_code == 200, second.text
    assert client.get(f"{ADMIN_AUTH}/me", headers=_bearer(second.json()["access_token"])).json()["mfa_enabled"] is True


def test_a_totp_code_cannot_be_replayed(ctx):
    _make_admin(ctx)
    enrolled = _enroll_admin(ctx)
    code = _code(ctx, enrolled.secret)

    token = _admin_login(ctx.client).json()["mfa_token"]
    assert ctx.client.post(f"{ADMIN_AUTH}/mfa/verify", json={"mfa_token": token, "code": code}).status_code == 200
    token = _admin_login(ctx.client).json()["mfa_token"]
    replay = ctx.client.post(f"{ADMIN_AUTH}/mfa/verify", json={"mfa_token": token, "code": code})
    assert replay.status_code == 401
    assert replay.json()["detail"]["code"] == "MFA_INVALID"


def test_mfa_code_guessing_is_rate_limited(ctx):
    _make_admin(ctx)
    enrolled = _enroll_admin(ctx)
    token = _admin_login(ctx.client).json()["mfa_token"]
    live = {pyotp.TOTP(enrolled.secret).generate_otp(int(ctx.clock.now) // 30 + d) for d in (-1, 0, 1)}
    wrong = next(c for c in ("000000", "111111", "222222", "333333") if c not in live)
    for _ in range(auth.MAX_LOGIN_ATTEMPTS):
        assert ctx.client.post(f"{ADMIN_AUTH}/mfa/verify", json={"mfa_token": token, "code": wrong}).status_code == 401
    blocked = ctx.client.post(f"{ADMIN_AUTH}/mfa/verify", json={"mfa_token": token, "code": _code(ctx, enrolled.secret)})
    assert blocked.status_code == 429


def test_recovery_codes_work_once_and_their_use_is_audited(ctx):
    _make_admin(ctx)
    enrolled = _enroll_admin(ctx)
    code = enrolled.codes[0]

    token = _admin_login(ctx.client).json()["mfa_token"]
    used = ctx.client.post(f"{ADMIN_AUTH}/mfa/verify", json={"mfa_token": token, "recovery_code": code.upper()})
    assert used.status_code == 200, used.text
    assert used.json()["recovery_codes_remaining"] == security.RECOVERY_CODE_COUNT - 1
    assert len(_audit_events(ctx, "admin_mfa_recovery_code_used")) == 1

    token = _admin_login(ctx.client).json()["mfa_token"]
    again = ctx.client.post(f"{ADMIN_AUTH}/mfa/verify", json={"mfa_token": token, "recovery_code": code})
    assert again.status_code == 401


def test_challenge_token_dies_when_the_password_changes(ctx):
    _make_admin(ctx)
    enrolled = _enroll_admin(ctx)
    token = _admin_login(ctx.client).json()["mfa_token"]
    _execute(ctx, "UPDATE admins SET hashed_password = ? WHERE id = ?",
             (security.get_password_hash("a brand new admin password"), ADMIN_ID))
    response = ctx.client.post(f"{ADMIN_AUTH}/mfa/verify", json={"mfa_token": token, "code": _code(ctx, enrolled.secret)})
    assert response.status_code == 401


def test_required_mfa_gives_an_unenrolled_admin_only_an_enrolment_token(ctx, monkeypatch):
    _make_admin(ctx)
    monkeypatch.setattr(settings, "ADMIN_MFA_REQUIRED", True)
    client = _fresh_client(ctx)

    login = _admin_login(client)
    assert login.status_code == 200
    body = login.json()
    assert body["mfa_enrollment_required"] is True and body["enrollment_token"]
    assert "access_token" not in body and "refresh_token" not in body
    token = body["enrollment_token"]

    # The enrolment token can do nothing except enrol.
    assert client.get(f"{ADMIN_AUTH}/me", headers=_bearer(token)).status_code == 401
    assert client.post("/api/admin/probe", headers=_bearer(token)).status_code == 401
    assert client.post(f"{ADMIN_AUTH}/mfa/verify", json={"mfa_token": token, "code": "123456"}).status_code == 401
    assert client.post(f"{ADMIN_AUTH}/mfa/disable", json={"code": "123456"}, headers=_bearer(token)).status_code == 401

    secret = client.post(f"{ADMIN_AUTH}/mfa/setup", json={"enrollment_token": token}).json()["secret"]
    enroll = client.post(f"{ADMIN_AUTH}/mfa/enroll", json={"enrollment_token": token, "code": _code(ctx, secret)})
    assert enroll.status_code == 200, enroll.text
    assert enroll.json()["access_token"] and len(enroll.json()["recovery_codes"]) == security.RECOVERY_CODE_COUNT
    assert client.post("/api/admin/probe", headers=_bearer(enroll.json()["access_token"])).status_code == 200
    assert json.loads(_audit_events(ctx, "admin_mfa_enrolled")[0]["details"])["via"] == "enrollment_token"


def test_required_mfa_refuses_password_only_sessions_opened_before_the_policy(ctx, monkeypatch):
    _make_admin(ctx)
    login = _admin_login(ctx.client).json()  # policy off: a password-only session
    monkeypatch.setattr(settings, "ADMIN_MFA_REQUIRED", True)

    denied = ctx.client.post("/api/admin/probe", headers=_bearer(login["access_token"]))
    assert denied.status_code == 403 and denied.json()["detail"]["code"] == "MFA_ENROLLMENT_REQUIRED"
    refresh = _fresh_client(ctx).post(f"{ADMIN_AUTH}/refresh", json={"refresh_token": login["refresh_token"]})
    assert refresh.status_code == 401 and refresh.json()["detail"]["code"] == "MFA_REQUIRED"


def test_admin_mfa_required_defaults_to_the_production_profile(monkeypatch):
    monkeypatch.setattr(settings, "ADMIN_MFA_REQUIRED", None)
    monkeypatch.setattr(settings, "APP_ENV", "TEST")
    assert sc.admin_mfa_required() is False
    monkeypatch.setattr(settings, "APP_ENV", "PRODUCTION")
    assert sc.admin_mfa_required() is True
    monkeypatch.setattr(settings, "ADMIN_MFA_REQUIRED", False)
    assert sc.admin_mfa_required() is False  # explicit setting wins


def test_disabling_mfa_needs_a_current_totp_code_and_is_audited(ctx):
    _make_admin(ctx)
    enrolled = _enroll_admin(ctx)
    token = _admin_login(ctx.client).json()["mfa_token"]
    access = ctx.client.post(f"{ADMIN_AUTH}/mfa/verify",
                             json={"mfa_token": token, "code": _code(ctx, enrolled.secret)}).json()["access_token"]
    _next_step(ctx)

    assert ctx.client.post(f"{ADMIN_AUTH}/mfa/disable", json={"code": enrolled.codes[1]},
                           headers=_bearer(access)).status_code in (400, 422)  # recovery code: not enough
    assert ctx.client.post(f"{ADMIN_AUTH}/mfa/disable", json={"code": "000000" if _code(ctx, enrolled.secret) != "000000"
                                                             else "111111"}, headers=_bearer(access)).status_code == 400
    ok = ctx.client.post(f"{ADMIN_AUTH}/mfa/disable", json={"code": _code(ctx, enrolled.secret)}, headers=_bearer(access))
    assert ok.status_code == 200, ok.text
    row = _rows(ctx, "SELECT mfa_enabled, mfa_secret FROM admins WHERE id = ?", (ADMIN_ID,))[0]
    assert not row["mfa_enabled"] and row["mfa_secret"] is None
    assert _rows(ctx, "SELECT id FROM admin_mfa_recovery_codes WHERE admin_id = ?", (ADMIN_ID,)) == []
    assert len(_audit_events(ctx, "admin_mfa_disabled")) == 1


def test_disabling_mfa_is_refused_while_it_is_required(ctx, monkeypatch):
    _make_admin(ctx)
    enrolled = _enroll_admin(ctx)
    access = enrolled.enroll.json()["access_token"]
    monkeypatch.setattr(settings, "ADMIN_MFA_REQUIRED", True)
    response = ctx.client.post(f"{ADMIN_AUTH}/mfa/disable", json={"code": _code(ctx, enrolled.secret)},
                               headers=_bearer(access))
    assert response.status_code == 409
    assert _rows(ctx, "SELECT mfa_enabled FROM admins WHERE id = ?", (ADMIN_ID,))[0]["mfa_enabled"] == 1


def test_setup_cannot_replace_an_enrolled_factor(ctx):
    _make_admin(ctx)
    enrolled = _enroll_admin(ctx)
    access = enrolled.enroll.json()["access_token"]
    assert ctx.client.post(f"{ADMIN_AUTH}/mfa/setup", headers=_bearer(access)).status_code == 400


def test_recovery_codes_can_be_regenerated_with_a_totp_code(ctx):
    _make_admin(ctx)
    enrolled = _enroll_admin(ctx)
    access = enrolled.enroll.json()["access_token"]
    response = ctx.client.post(f"{ADMIN_AUTH}/mfa/recovery-codes", json={"code": _code(ctx, enrolled.secret)},
                               headers=_bearer(access))
    assert response.status_code == 200
    new_codes = response.json()["recovery_codes"]
    assert set(new_codes).isdisjoint(enrolled.codes)
    token = _admin_login(ctx.client).json()["mfa_token"]
    assert ctx.client.post(f"{ADMIN_AUTH}/mfa/verify",
                           json={"mfa_token": token, "recovery_code": enrolled.codes[0]}).status_code == 401
    assert len(_audit_events(ctx, "admin_mfa_recovery_codes_regenerated")) == 1


def test_admin_refresh_reuse_revokes_every_admin_session(ctx):
    _make_admin(ctx)
    first = _admin_login(ctx.client).json()
    other = _admin_login(_fresh_client(ctx)).json()
    rotated = _fresh_client(ctx).post(f"{ADMIN_AUTH}/refresh", json={"refresh_token": first["refresh_token"]})
    assert rotated.status_code == 200
    replay = _fresh_client(ctx).post(f"{ADMIN_AUTH}/refresh", json={"refresh_token": first["refresh_token"]})
    assert replay.status_code == 401
    assert _rows(ctx, "SELECT id FROM admin_sessions WHERE revoked_at IS NULL") == []
    assert _fresh_client(ctx).post(f"{ADMIN_AUTH}/refresh",
                                   json={"refresh_token": other["refresh_token"]}).status_code == 401
    assert len(_audit_events(ctx, "admin_refresh_reuse_detected")) >= 1


def test_admin_mfa_schema_is_additive_and_idempotent(ctx):
    from shared_lib.persistence.admin_mfa_schema import ensure_admin_mfa_schema

    with ctx.db.connect() as conn:
        ensure_admin_mfa_schema(conn)
        ensure_admin_mfa_schema(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(admins)").fetchall()}
    assert {"mfa_enabled", "mfa_secret", "mfa_last_counter", "mfa_enrolled_at"} <= cols


# =========================================================================== cookies + CSRF

def test_login_sets_httponly_samesite_strict_cookies_and_a_readable_csrf_cookie(ctx):
    _activate_user(ctx)
    response = _user_login(ctx.client)
    assert response.status_code == 200
    access = _cookie_header(response, USER_REALM.access_cookie).lower()
    refresh = _cookie_header(response, USER_REALM.refresh_cookie).lower()
    csrf = _cookie_header(response, USER_REALM.csrf_cookie).lower()
    for header in (access, refresh, csrf):
        assert "samesite=strict" in header
    assert "httponly" in access and "httponly" in refresh and "httponly" not in csrf
    assert "path=/api/v1/auth" in refresh
    assert response.json()["csrf_token"] == ctx.client.cookies.get(USER_REALM.csrf_cookie)
    assert response.headers["cache-control"] == "no-store"


def test_cookie_session_authenticates_safe_requests(ctx):
    _activate_user(ctx)
    assert _user_login(ctx.client).status_code == 200
    response = ctx.client.get("/api/v1/probe")  # no Authorization header at all
    assert response.status_code == 200 and response.json()["user_id"] == _user_id(ctx)
    assert ctx.client.get(f"{AUTH}/me").status_code == 200


def test_cookie_session_needs_the_csrf_header_on_state_changing_requests(ctx):
    _activate_user(ctx)
    csrf = _user_login(ctx.client).json()["csrf_token"]

    missing = ctx.client.post("/api/v1/probe")
    assert missing.status_code == 403 and missing.json()["detail"]["code"] == "CSRF_FAILED"
    forged = ctx.client.post("/api/v1/probe", headers={"X-CSRF-Token": "not-the-token"})
    assert forged.status_code == 403
    ok = ctx.client.post("/api/v1/probe", headers={"X-CSRF-Token": csrf})
    assert ok.status_code == 200


def test_placeholder_bearer_headers_fall_back_to_the_cookie(ctx):
    _activate_user(ctx)
    _user_login(ctx.client)
    for value in ("Bearer null", "Bearer undefined", "Bearer "):
        assert ctx.client.get("/api/v1/probe", headers={"Authorization": value}).status_code == 200


def test_refresh_through_the_cookie_rotates_and_needs_csrf(ctx):
    _activate_user(ctx)
    login = _user_login(ctx.client).json()
    old_refresh = ctx.client.cookies.get(USER_REALM.refresh_cookie)
    assert old_refresh == login["refresh_token"]

    assert ctx.client.post(f"{AUTH}/refresh").status_code == 403  # cookie without CSRF header
    rotated = ctx.client.post(f"{AUTH}/refresh", headers={"X-CSRF-Token": login["csrf_token"]})
    assert rotated.status_code == 200, rotated.text
    assert ctx.client.cookies.get(USER_REALM.refresh_cookie) not in (None, old_refresh)
    assert rotated.json()["csrf_token"] == login["csrf_token"]  # kept across refreshes
    assert ctx.client.get("/api/v1/probe").status_code == 200

    # The rotated cookie value replayed: reuse detection ends every session.
    attacker = _fresh_client(ctx)
    attacker.cookies.set(USER_REALM.refresh_cookie, old_refresh, path="/api/v1/auth")
    attacker.cookies.set(USER_REALM.csrf_cookie, "x")
    assert attacker.post(f"{AUTH}/refresh", headers={"X-CSRF-Token": "x"}).status_code == 401
    assert _rows(ctx, "SELECT id FROM auth_sessions WHERE revoked_at IS NULL") == []
    assert len(_audit_events(ctx, "refresh_reuse_detected")) == 1


def test_logout_through_the_cookie_revokes_the_session_and_clears_the_cookies(ctx):
    _activate_user(ctx)
    csrf = _user_login(ctx.client).json()["csrf_token"]
    response = ctx.client.post(f"{AUTH}/logout", headers={"X-CSRF-Token": csrf})
    assert response.status_code == 200
    cleared = [h.lower() for h in _set_cookie_headers(response)]
    for name in (USER_REALM.access_cookie, USER_REALM.refresh_cookie, USER_REALM.csrf_cookie):
        assert any(h.startswith(name + "=") and ("max-age=0" in h or "expires=" in h) for h in cleared), name
    assert _rows(ctx, "SELECT id FROM auth_sessions WHERE revoked_at IS NULL") == []
    assert ctx.client.get("/api/v1/probe").status_code == 401


def test_logout_still_works_with_a_body_token_and_no_access_token(ctx):
    _activate_user(ctx)
    refresh = _user_login(ctx.client).json()["refresh_token"]
    response = _fresh_client(ctx).post(f"{AUTH}/logout", json={"refresh_token": refresh})
    assert response.status_code == 200
    assert _rows(ctx, "SELECT id FROM auth_sessions WHERE revoked_at IS NULL") == []


def test_a_stale_cookie_does_not_block_signing_in_again(ctx):
    _activate_user(ctx)
    _user_login(ctx.client)
    ctx.client.cookies.set(USER_REALM.csrf_cookie, "stale")  # no matching header will be sent
    assert _user_login(ctx.client).status_code == 200


def test_bearer_headers_still_work_during_the_migration_window(ctx):
    _activate_user(ctx)
    login = _user_login(ctx.client).json()
    client = _fresh_client(ctx)  # no cookies
    assert client.post("/api/v1/probe", headers=_bearer(login["access_token"])).status_code == 200  # no CSRF needed
    refreshed = client.post(f"{AUTH}/refresh", json={"refresh_token": login["refresh_token"]})
    assert refreshed.status_code == 200 and refreshed.json()["access_token"]


def test_without_bearer_compat_tokens_leave_the_body_and_bearer_headers_are_ignored(ctx, monkeypatch):
    _activate_user(ctx)
    legacy = _user_login(_fresh_client(ctx)).json()["access_token"]
    monkeypatch.setattr(settings, "AUTH_BEARER_COMPAT", False)

    login = _user_login(ctx.client)
    body = login.json()
    assert "access_token" not in body and "refresh_token" not in body and body["csrf_token"]
    assert ctx.client.get("/api/v1/probe").status_code == 200  # the cookie session works
    assert _fresh_client(ctx).get("/api/v1/probe", headers=_bearer(legacy)).status_code == 401


def test_admin_cookies_are_separate_from_user_cookies(ctx):
    _make_admin(ctx)
    login = _admin_login(ctx.client)
    assert _cookie_header(login, ADMIN_REALM.access_cookie)
    assert "path=/api/v1/admin-auth" in _cookie_header(login, ADMIN_REALM.refresh_cookie).lower()
    csrf = login.json()["csrf_token"]
    assert ctx.client.get(f"{ADMIN_AUTH}/me").status_code == 200
    assert ctx.client.post("/api/admin/probe").status_code == 403  # CSRF
    assert ctx.client.post("/api/admin/probe", headers={"X-CSRF-Token": csrf}).status_code == 200
    # An admin cookie is never an end-user session.
    assert ctx.client.get("/api/v1/probe").status_code == 401
    refreshed = ctx.client.post(f"{ADMIN_AUTH}/refresh", headers={"X-CSRF-Token": csrf})
    assert refreshed.status_code == 200, refreshed.text
    assert ctx.client.get(f"{ADMIN_AUTH}/csrf").json()["csrf_token"] == csrf


def test_secure_flag_is_forced_in_production_and_configurable_elsewhere(monkeypatch):
    monkeypatch.setattr(settings, "APP_ENV", "TEST")
    monkeypatch.setattr(settings, "AUTH_COOKIE_SECURE", None)
    assert sc.cookie_secure() is False
    monkeypatch.setattr(settings, "AUTH_COOKIE_SECURE", True)
    assert sc.cookie_secure() is True
    monkeypatch.setattr(settings, "APP_ENV", "PRODUCTION")
    monkeypatch.setattr(settings, "AUTH_COOKIE_SECURE", False)
    assert sc.cookie_secure() is True

    from starlette.responses import Response
    response = Response()
    sc.issue_session_cookies(response, USER_REALM, "a", "r")
    assert all("secure" in h.lower() for h in _set_cookie_headers(response))


def test_samesite_none_is_never_used(monkeypatch):
    monkeypatch.setattr(settings, "AUTH_COOKIE_SAMESITE", "none")
    assert sc.cookie_policy().samesite == "strict"


def test_cors_origins_are_explicit_and_never_a_wildcard():
    env = {"CORS_ALLOWED_ORIGINS": "*,https://app.example.com,https://*.example.com"}
    production = SimpleNamespace(production=True)
    assert browser_origins(production, env) == ["https://app.example.com"]
    assert "*" not in browser_origins(SimpleNamespace(production=False), env)
    main_source = (ROOT / "backends" / "user-backend" / "app" / "main.py").read_text(encoding="utf-8")
    assert "allow_origins=browser_origins(settings)" in main_source
    assert 'allow_origins=["*"]' not in main_source


def test_cookie_middleware_is_installed_inside_cors_in_the_main_app():
    source = (ROOT / "backends" / "user-backend" / "app" / "main.py").read_text(encoding="utf-8")
    assert source.index("install_session_cookie_middleware(app)") < source.index("CORSMiddleware,\n")


# =========================================================================== credential encryption

def _legacy_ciphertext(plaintext: str) -> str:
    key = settings.CREDENTIAL_KEY.encode()[:32].ljust(32, b"\0")
    return Fernet(base64.urlsafe_b64encode(key)).encrypt(plaintext.encode()).decode()


def test_new_ciphertexts_are_versioned_and_use_the_hkdf_key():
    token = security.encrypt_credential("s3cret-value")
    assert token.startswith("v2:")
    assert security.decrypt_credential(token) == "s3cret-value"
    legacy_fernet = security._get_legacy_fernet()
    with pytest.raises(Exception):
        legacy_fernet.decrypt(token[3:].encode())  # a different key from the old derivation
    assert security._hkdf_fernet_key(settings.CREDENTIAL_KEY) != base64.urlsafe_b64encode(
        settings.CREDENTIAL_KEY.encode()[:32].ljust(32, b"\0"))


def test_legacy_ciphertexts_still_decrypt():
    legacy = _legacy_ciphertext("old-secret")
    assert security.decrypt_credential(legacy) == "old-secret"
    assert security.decrypt_credential("v1:" + legacy) == "old-secret"
    assert security.decrypt_totp_secret("enc:v1:" + _legacy_ciphertext("JBSWY3DPEHPK3PXP")) == "JBSWY3DPEHPK3PXP"
    assert security.decrypt_totp_secret("JBSWY3DPEHPK3PXP") == "JBSWY3DPEHPK3PXP"  # legacy plaintext


def test_reencryption_helpers_rewrite_legacy_values_only():
    legacy = _legacy_ciphertext("rotate-me")
    new = security.reencrypt_credential(legacy)
    assert new.startswith("v2:") and security.decrypt_credential(new) == "rotate-me"
    assert security.reencrypt_credential(new) is None
    assert security.credential_needs_reencryption(legacy) and not security.credential_needs_reencryption(new)
    assert security.reencrypt_totp_secret(security.encrypt_totp_secret("ABC")) is None


def test_any_length_credential_key_works(monkeypatch):
    for key in ("short", "x" * 100):
        monkeypatch.setattr(settings, "CREDENTIAL_KEY", key)
        assert security.decrypt_credential(security.encrypt_credential("v")) == "v"


def test_totp_sweep_reencrypts_users_and_admins_and_codes_still_verify(ctx):
    _activate_user(ctx)
    _make_admin(ctx)
    secret = pyotp.random_base32()
    _execute(ctx, "UPDATE users SET totp_secret = ? WHERE email = ?", ("enc:v1:" + _legacy_ciphertext(secret), EMAIL))
    _execute(ctx, "UPDATE admins SET mfa_secret = ? WHERE id = ?", (secret, ADMIN_ID))  # legacy plaintext

    with ctx.db.connect() as conn:
        result = security.reencrypt_stored_totp_secrets(conn)
    assert result["users"]["updated"] == 1 and result["admins"]["updated"] == 1
    for value in (_rows(ctx, "SELECT totp_secret AS v FROM users")[0]["v"],
                  _rows(ctx, "SELECT mfa_secret AS v FROM admins")[0]["v"]):
        assert value.startswith("enc:v2:") and secret not in value
        assert security.verify_totp_code(value, pyotp.TOTP(secret).generate_otp(int(ctx.clock.now) // 30))
    with ctx.db.connect() as conn:
        again = security.reencrypt_stored_totp_secrets(conn)
    assert again["users"]["updated"] == 0 and again["admins"]["updated"] == 0


def test_no_plaintext_reaches_the_logs(caplog, monkeypatch):
    caplog.set_level(logging.DEBUG)
    plaintext = "PLAINTEXT-MARKER-9f3a"
    token = security.encrypt_credential(plaintext)
    security.decrypt_credential(token)
    security.reencrypt_credential(_legacy_ciphertext(plaintext))
    tampered = token[:-6] + "AAAAAA"
    with pytest.raises(Exception) as exc:
        security.decrypt_credential(tampered)
    assert plaintext not in str(exc.value)
    assert security.verify_totp_code("enc:" + tampered, "123456") is False
    assert plaintext not in caplog.text


# =========================================================================== audit regressions

def test_a_refresh_token_is_not_an_access_token(ctx):
    _activate_user(ctx)
    login = _user_login(ctx.client).json()
    client = _fresh_client(ctx)
    assert client.get("/api/v1/probe", headers=_bearer(login["refresh_token"])).status_code == 401
    assert client.get(f"{AUTH}/me", headers=_bearer(login["refresh_token"])).status_code == 401


def test_expired_access_tokens_are_rejected(ctx):
    _activate_user(ctx)
    expired = security.create_access_token(_user_id(ctx), expires_delta=timedelta(seconds=-5))
    assert _fresh_client(ctx).get("/api/v1/probe", headers=_bearer(expired)).status_code == 401


def test_admin_and_user_tokens_are_not_interchangeable(ctx):
    _activate_user(ctx)
    _make_admin(ctx)
    user_access = _user_login(_fresh_client(ctx)).json()["access_token"]
    admin_access = _admin_login(_fresh_client(ctx)).json()["access_token"]
    client = _fresh_client(ctx)
    assert client.post("/api/admin/probe", headers=_bearer(user_access)).status_code == 401
    assert client.get("/api/v1/probe", headers=_bearer(admin_access)).status_code == 401


def test_reset_code_is_single_use_and_revokes_every_session(ctx):
    _activate_user(ctx)
    login = _user_login(_fresh_client(ctx)).json()
    assert ctx.client.post(f"{AUTH}/forgot-password", json={"email": EMAIL}).status_code == 200
    code = ctx.sent[-1][2]
    first = ctx.client.post(f"{AUTH}/reset-password",
                            json={"email": EMAIL, "code": code, "new_password": "the new password 123"})
    assert first.status_code == 200
    again = ctx.client.post(f"{AUTH}/reset-password",
                            json={"email": EMAIL, "code": code, "new_password": "another new password 456"})
    assert again.status_code == 400
    # Account-takeover protection: the session opened before the reset is gone.
    assert _fresh_client(ctx).post(f"{AUTH}/refresh",
                                   json={"refresh_token": login["refresh_token"]}).status_code == 401
    assert _user_login(_fresh_client(ctx), password="the new password 123").status_code == 200


def test_reset_code_expires(ctx):
    _activate_user(ctx)
    ctx.client.post(f"{AUTH}/forgot-password", json={"email": EMAIL})
    code = ctx.sent[-1][2]
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    _execute(ctx, "UPDATE password_resets SET expires_at = ?", (past,))
    response = ctx.client.post(f"{AUTH}/reset-password",
                               json={"email": EMAIL, "code": code, "new_password": "the new password 123"})
    assert response.status_code == 400


_SECRET_MARKERS = ("PLAINTEXT-API-KEY-alice", "PLAINTEXT-API-SECRET-alice", "PASSPHRASE-alice")


def _seed_broker_account(ctx, account_id: str, user_id: str) -> None:
    from shared_lib.core.security.broker_security import encrypt_credentials

    now = utc_now_iso()
    blob = encrypt_credentials({"api_key": _SECRET_MARKERS[0], "api_secret": _SECRET_MARKERS[1],
                                "passphrase": _SECRET_MARKERS[2]})
    _execute(ctx, "INSERT INTO broker_accounts (id, user_id, broker_id, market_type, label, status, environment, "
                  "masked_key, created_at, updated_at, active_credential_version) "
                  "VALUES (?, ?, 'binance', 'crypto', 'main', 'connected', 'live', 'PLAI...lice', ?, ?, 1)",
             (account_id, user_id, now, now))
    _execute(ctx, "INSERT INTO broker_credentials_v2 (account_id, version, status, encrypted_blob, created_at, "
                  "updated_at) VALUES (?, 1, 'active', ?, ?, ?)", (account_id, blob, now, now))


def _two_users(ctx):
    _activate_user(ctx, "h84-alice@example.com", PASSWORD)
    _activate_user(ctx, "h84-bob@example.com", PASSWORD)
    alice_id = _user_id(ctx, "h84-alice@example.com")
    _seed_broker_account(ctx, "acc-alice", alice_id)
    alice = _bearer(_user_login(_fresh_client(ctx), "h84-alice@example.com").json()["access_token"])
    bob = _bearer(_user_login(_fresh_client(ctx), "h84-bob@example.com").json()["access_token"])
    return alice, bob


def test_a_user_cannot_read_or_change_another_users_broker_account(ctx):
    alice, bob = _two_users(ctx)
    client = _fresh_client(ctx)

    assert client.get("/api/v1/brokers/acc-alice", headers=alice).status_code == 200
    assert client.get("/api/v1/brokers/acc-alice", headers=bob).status_code == 404
    assert client.get("/api/v1/brokers/acc-alice/summary", headers=bob).status_code == 404
    assert client.get("/api/v1/brokers/accounts", headers=bob).json()["accounts"] == []
    assert client.post("/api/v1/brokers/acc-alice/disconnect", headers=bob).status_code == 404
    assert client.delete("/api/v1/brokers/acc-alice", headers=bob).status_code == 404
    row = _rows(ctx, "SELECT status FROM broker_accounts WHERE id = 'acc-alice'")
    assert row and row[0]["status"] == "connected"


def test_disconnecting_someone_elses_account_touches_nothing(ctx):
    """Regression: disconnect used to delete the legacy credentials and stop
    every bot of ANY account id before checking ownership, and answered 200."""
    alice, bob = _two_users(ctx)
    now = utc_now_iso()
    alice_id = _user_id(ctx, "h84-alice@example.com")
    with ctx.db.connect() as conn:
        conn.execute("INSERT INTO broker_credentials (account_id, encrypted_blob, updated_at) "
                     "VALUES ('acc-alice', 'legacy-blob', ?)", (now,))
        conn.execute("INSERT INTO bot_instances (id, user_id, broker_account_id, market_type, strategy_id, mode, "
                     "status, created_at, updated_at) VALUES ('bot-alice', ?, 'acc-alice', 'crypto', 's1', 'live', "
                     "'running', ?, ?)", (alice_id, now, now))
    assert _fresh_client(ctx).post("/api/v1/brokers/acc-alice/disconnect", headers=bob).status_code == 404
    assert _rows(ctx, "SELECT account_id FROM broker_credentials WHERE account_id = 'acc-alice'")
    assert _rows(ctx, "SELECT status FROM bot_instances WHERE id = 'bot-alice'")[0]["status"] == "running"
    assert _fresh_client(ctx).post("/api/v1/brokers/acc-alice/disconnect", headers=alice).status_code == 200


def test_broker_responses_never_contain_exchange_credentials(ctx):
    alice, _ = _two_users(ctx)
    client = _fresh_client(ctx)
    responses = [
        client.get("/api/v1/brokers/accounts", headers=alice),
        client.get("/api/v1/brokers/acc-alice", headers=alice),
    ]
    for response in responses:
        assert response.status_code == 200, response.text
        text = response.text
        for marker in _SECRET_MARKERS:
            assert marker not in text
        assert "encrypted_blob" not in text and "api_secret" not in text
    listed = responses[0].json()["accounts"][0]
    assert listed["id"] == "acc-alice" and listed["masked_key"] == "PLAI...lice"


def _load_maintenance():
    import importlib.util

    path = ROOT / "backends" / "user-backend" / "scripts" / "security_maintenance.py"
    spec = importlib.util.spec_from_file_location("security_maintenance_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_break_glass_reset_clears_mfa_revokes_sessions_and_is_audited(ctx):
    _make_admin(ctx)
    _enroll_admin(ctx)
    maintenance = _load_maintenance()
    with ctx.db.connect() as conn:
        assert maintenance.reset_admin_mfa(conn, ADMIN_EMAIL.upper()) is True
        assert maintenance.reset_admin_mfa(conn, "nobody@example.com") is False
    row = _rows(ctx, "SELECT mfa_enabled, mfa_secret FROM admins WHERE id = ?", (ADMIN_ID,))[0]
    assert not row["mfa_enabled"] and row["mfa_secret"] is None
    assert _rows(ctx, "SELECT id FROM admin_sessions WHERE revoked_at IS NULL") == []
    assert len(_audit_events(ctx, "admin_mfa_reset_by_operator")) == 1
    assert "access_token" in _admin_login(_fresh_client(ctx)).json()  # MFA not required in this test


def test_enrolment_with_the_step_token_works_despite_a_stale_admin_cookie(ctx, monkeypatch):
    _make_admin(ctx)
    _admin_login(ctx.client)  # leaves cf_admin_* cookies in the jar
    monkeypatch.setattr(settings, "ADMIN_MFA_REQUIRED", True)
    token = _admin_login(ctx.client).json()["enrollment_token"]
    setup = ctx.client.post(f"{ADMIN_AUTH}/mfa/setup", json={"enrollment_token": token})  # no CSRF header
    assert setup.status_code == 200, setup.text
    enroll = ctx.client.post(f"{ADMIN_AUTH}/mfa/enroll",
                             json={"enrollment_token": token, "code": _code(ctx, setup.json()["secret"])})
    assert enroll.status_code == 200, enroll.text


def test_sse_stream_refuses_url_tokens_after_the_migration_window(ctx, monkeypatch):
    from app.api import events_proxy

    _activate_user(ctx)
    login = _user_login(_fresh_client(ctx)).json()
    app = FastAPI()
    app.include_router(events_proxy.router, prefix="/api/v1/events")
    sc.install_session_cookie_middleware(app)
    client = TestClient(app)

    assert client.get("/api/v1/events/stream").status_code == 401
    assert client.get("/api/v1/events/stream", params={"token": login["refresh_token"]}).status_code == 401
    monkeypatch.setattr(settings, "AUTH_BEARER_COMPAT", False)
    assert client.get("/api/v1/events/stream", params={"token": login["access_token"]}).status_code == 401
