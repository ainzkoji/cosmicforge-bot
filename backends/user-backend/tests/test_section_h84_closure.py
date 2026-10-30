"""Section H Step 8.4 closure: open findings F1, F2, F3, F5, F9.

* F1 -- access tokens carry ``tv`` (token_version) and ``sid`` (refresh-rotation
  chain); every validator re-checks the account, the version and the chain, so
  logout-all, password reset, suspension, privilege changes, refresh reuse,
  admin MFA enrolment / break-glass reset and signing one device out end the
  affected access tokens on their next request.
* F2 -- auth_sessions.expires_at follows REFRESH_TOKEN_EXPIRE_DAYS.
* F3 -- the dead GET /api/v1/auth/user/brokers route is gone.
* F5 -- production refuses the public KYC development key unless an explicit,
  audited opt-in is set.
* F9 -- while ADMIN_MFA_REQUIRED is on, enrolment needs a one-time,
  operator-issued bootstrap code.

Runs on a throw-away database (the ``ctx`` fixture of test_section_h84_security).
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
from datetime import datetime, timedelta, timezone

import pytest
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from jose import jwt

from tests.test_section_h84_security import (  # noqa: F401  (ctx is a fixture)
    ADMIN_AUTH, ADMIN_EMAIL, ADMIN_ID, AUTH, EMAIL, PASSWORD, _activate_user, _admin_login, _audit_events,
    _bearer, _bootstrap_code, _code, _enroll_admin, _execute, _fresh_client, _load_maintenance, _make_admin,
    _next_step, _rows, _user_id, _user_login, ctx,
)
from app.core import security
from app.core.config import settings

PROBE = "/api/v1/probe"
ADMIN_PROBE = "/api/admin/probe"


def _claims(token: str) -> dict:
    return jwt.get_unverified_claims(token)


def _revoked(response) -> bool:
    return response.status_code == 401 and response.json()["detail"]["code"] == "TOKEN_REVOKED"


def _two_devices(ctx):
    _activate_user(ctx)
    return _user_login(_fresh_client(ctx)).json(), _user_login(_fresh_client(ctx)).json()


def _probe(ctx, token, path=PROBE):
    return _fresh_client(ctx).get(path, headers=_bearer(token)) if path == PROBE else \
        _fresh_client(ctx).post(path, headers=_bearer(token))


# =========================================================================== F1: users

def test_access_tokens_carry_the_token_version_and_the_device_session(ctx):
    a, _b = _two_devices(ctx)
    claims = _claims(a["access_token"])
    assert claims["type"] == "access" and claims["tv"] == 0
    session = _rows(ctx, "SELECT id, family_id FROM auth_sessions WHERE refresh_token_hash = ?",
                    (security.hash_token(a["refresh_token"]),))[0]
    assert claims["sid"] == session["id"] == session["family_id"]


def test_logout_all_ends_the_access_tokens_of_every_device_at_once(ctx):
    a, b = _two_devices(ctx)
    assert _probe(ctx, a["access_token"]).status_code == 200 and _probe(ctx, b["access_token"]).status_code == 200

    done = _fresh_client(ctx).post(f"{AUTH}/logout-all", headers=_bearer(a["access_token"]))
    assert done.status_code == 200 and done.json()["count"] == 2

    assert _revoked(_probe(ctx, a["access_token"])) and _revoked(_probe(ctx, b["access_token"]))
    for tokens in (a, b):
        assert _fresh_client(ctx).post(f"{AUTH}/refresh", json={"refresh_token": tokens["refresh_token"]}
                                       ).status_code == 401
    assert len(_audit_events(ctx, "logout_all")) == 1
    # Signing in again works and is current.
    assert _probe(ctx, _user_login(_fresh_client(ctx)).json()["access_token"]).status_code == 200


def test_signing_one_device_out_ends_only_that_devices_tokens(ctx):
    a, b = _two_devices(ctx)
    assert _fresh_client(ctx).post(f"{AUTH}/logout", json={"refresh_token": a["refresh_token"]}).status_code == 200

    assert _revoked(_probe(ctx, a["access_token"]))
    assert _probe(ctx, b["access_token"]).status_code == 200
    rotated = _fresh_client(ctx).post(f"{AUTH}/refresh", json={"refresh_token": b["refresh_token"]})
    assert rotated.status_code == 200
    assert _probe(ctx, rotated.json()["access_token"]).status_code == 200


def test_revoking_a_listed_session_ends_that_devices_access_token(ctx):
    a, b = _two_devices(ctx)
    sid_b = _claims(b["access_token"])["sid"]
    assert _fresh_client(ctx).delete(f"{AUTH}/sessions/{sid_b}", headers=_bearer(a["access_token"])
                                     ).status_code == 200
    assert _revoked(_probe(ctx, b["access_token"]))
    assert _probe(ctx, a["access_token"]).status_code == 200


def test_refresh_rotation_keeps_the_device_chain_and_its_access_tokens_valid(ctx):
    a, _b = _two_devices(ctx)
    first = _fresh_client(ctx).post(f"{AUTH}/refresh", json={"refresh_token": a["refresh_token"]}).json()
    second = _fresh_client(ctx).post(f"{AUTH}/refresh", json={"refresh_token": first["refresh_token"]}).json()
    sid = _claims(a["access_token"])["sid"]
    assert _claims(first["access_token"])["sid"] == _claims(second["access_token"])["sid"] == sid
    # An access token minted before a rotation stays valid until it expires.
    for token in (a["access_token"], first["access_token"], second["access_token"]):
        assert _probe(ctx, token).status_code == 200
    chain = _rows(ctx, "SELECT revoked_at FROM auth_sessions WHERE family_id = ?", (sid,))
    assert len(chain) == 3 and sum(1 for r in chain if r["revoked_at"] is None) == 1


def test_refresh_token_reuse_also_ends_every_access_token(ctx):
    a, b = _two_devices(ctx)
    rotated = _fresh_client(ctx).post(f"{AUTH}/refresh", json={"refresh_token": a["refresh_token"]}).json()
    replay = _fresh_client(ctx).post(f"{AUTH}/refresh", json={"refresh_token": a["refresh_token"]})
    assert replay.status_code == 401
    for token in (a["access_token"], rotated["access_token"], b["access_token"]):
        assert _revoked(_probe(ctx, token))


def test_password_reset_ends_outstanding_access_tokens(ctx):
    a, _b = _two_devices(ctx)
    assert ctx.client.post(f"{AUTH}/forgot-password", json={"email": EMAIL}).status_code == 200
    code = ctx.sent[-1][2]
    reset = ctx.client.post(f"{AUTH}/reset-password",
                            json={"email": EMAIL, "code": code, "new_password": "a brand new pass phrase 2"})
    assert reset.status_code == 200
    assert _revoked(_probe(ctx, a["access_token"]))
    assert _rows(ctx, "SELECT token_version FROM users WHERE email = ?", (EMAIL,))[0]["token_version"] == 1


def test_get_current_user_id_routes_recheck_the_account_status(ctx):
    a, _b = _two_devices(ctx)
    # Status changed directly (no version bump): the per-request status check alone refuses it.
    _execute(ctx, "UPDATE users SET status = 'suspended' WHERE email = ?", (EMAIL,))
    refused = _probe(ctx, a["access_token"])
    assert refused.status_code == 403 and refused.json()["detail"] == "Account not active"
    _execute(ctx, "UPDATE users SET status = 'active' WHERE email = ?", (EMAIL,))
    assert _probe(ctx, a["access_token"]).status_code == 200


def test_admin_suspension_ends_sessions_and_access_tokens(ctx, monkeypatch):
    from app.api import admin as admin_api

    a, _b = _two_devices(ctx)
    monkeypatch.setattr(admin_api, "DB", lambda: ctx.db)
    uid = _user_id(ctx)
    asyncio.run(admin_api.suspend_user(uid, _admin={"id": ADMIN_ID, "email": ADMIN_EMAIL}))
    _execute(ctx, "UPDATE users SET status = 'active' WHERE id = ?", (uid,))  # even after re-activation
    assert _revoked(_probe(ctx, a["access_token"]))
    assert _rows(ctx, "SELECT id FROM auth_sessions WHERE user_id = ? AND revoked_at IS NULL", (uid,)) == []


def test_privilege_changes_end_tokens_minted_with_the_old_privileges(ctx, monkeypatch):
    from app.api import admin as admin_api

    a, _b = _two_devices(ctx)
    monkeypatch.setattr(admin_api, "DB", lambda: ctx.db)
    uid = _user_id(ctx)
    operator = {"id": ADMIN_ID, "email": ADMIN_EMAIL}
    asyncio.run(admin_api.grant_admin_role(uid, _admin=operator))
    assert _revoked(_probe(ctx, a["access_token"]))
    # The refresh session survives a privilege change; it mints a current token.
    fresh = _fresh_client(ctx).post(f"{AUTH}/refresh", json={"refresh_token": a["refresh_token"]}).json()
    assert _claims(fresh["access_token"])["tv"] == 1 and _probe(ctx, fresh["access_token"]).status_code == 200
    asyncio.run(admin_api.revoke_admin_role(uid, _admin=operator))
    assert _revoked(_probe(ctx, fresh["access_token"]))


def test_tokens_without_the_new_claims_are_accepted_only_until_the_first_bump(ctx):
    """Access tokens minted before this release carry neither ``tv`` nor ``sid``."""
    _activate_user(ctx)
    uid = _user_id(ctx)
    now = datetime.now(timezone.utc)
    legacy = jwt.encode({"sub": uid, "type": "access", "role": "user", "iss": security.ISSUER,
                         "aud": security.AUDIENCE, "iat": now, "nbf": now, "exp": now + timedelta(minutes=5)},
                        settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    assert _probe(ctx, legacy).status_code == 200
    with ctx.db.connect() as conn:
        from shared_lib.core.security.token_state import bump_user_token_version
        bump_user_token_version(conn, uid)
    assert _revoked(_probe(ctx, legacy))


def test_a_token_of_an_unknown_account_and_a_refresh_token_are_never_access(ctx):
    a, _b = _two_devices(ctx)
    ghost = security.create_access_token("no-such-user", token_version=0)
    assert _probe(ctx, ghost).status_code == 401
    assert _probe(ctx, a["refresh_token"]).status_code == 401


def test_cookie_sessions_are_revoked_like_bearer_sessions(ctx):
    _activate_user(ctx)
    browser = _fresh_client(ctx)
    login = _user_login(browser)
    assert browser.get(PROBE).status_code == 200  # cookie -> Authorization by the middleware
    csrf = login.cookies.get("cf_csrf") or browser.cookies.get("cf_csrf")
    assert browser.post(f"{AUTH}/logout-all", headers={"X-CSRF-Token": csrf}).status_code == 200
    assert _revoked(browser.get(PROBE))


def test_sse_proxy_refuses_a_revoked_access_token(ctx):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.api import events_proxy
    from app.core import session_cookies as sc

    a, _b = _two_devices(ctx)
    app = FastAPI()
    app.include_router(events_proxy.router, prefix="/api/v1/events")
    sc.install_session_cookie_middleware(app)
    client = TestClient(app)
    assert _fresh_client(ctx).post(f"{AUTH}/logout-all", headers=_bearer(a["access_token"])).status_code == 200
    refused = client.get("/api/v1/events/stream", headers=_bearer(a["access_token"]))
    assert refused.status_code == 401


# =========================================================================== F1: admins

def test_admin_logout_ends_that_sessions_access_token(ctx):
    _make_admin(ctx)
    one, two = _admin_login(_fresh_client(ctx)).json(), _admin_login(_fresh_client(ctx)).json()
    assert _claims(one["access_token"])["tv"] == 0 and _claims(one["access_token"])["sid"]
    assert _probe(ctx, one["access_token"], ADMIN_PROBE).status_code == 200
    assert _fresh_client(ctx).post(f"{ADMIN_AUTH}/logout", json={"refresh_token": one["refresh_token"]}
                                   ).status_code == 200
    assert _revoked(_probe(ctx, one["access_token"], ADMIN_PROBE))
    assert _probe(ctx, two["access_token"], ADMIN_PROBE).status_code == 200


def test_admin_mfa_enrolment_ends_every_earlier_admin_access_token(ctx):
    _make_admin(ctx)
    other = _admin_login(_fresh_client(ctx)).json()
    enrolled = _enroll_admin(ctx)
    assert _revoked(_probe(ctx, other["access_token"], ADMIN_PROBE))
    assert _revoked(_probe(ctx, enrolled.old_login.json()["access_token"], ADMIN_PROBE))
    assert _probe(ctx, enrolled.enroll.json()["access_token"], ADMIN_PROBE).status_code == 200


def test_admin_refresh_rotation_keeps_the_chain_and_reuse_ends_access_tokens(ctx):
    _make_admin(ctx)
    login = _admin_login(_fresh_client(ctx)).json()
    rotated = _fresh_client(ctx).post(f"{ADMIN_AUTH}/refresh", json={"refresh_token": login["refresh_token"]}).json()
    assert _claims(rotated["access_token"])["sid"] == _claims(login["access_token"])["sid"]
    assert _probe(ctx, login["access_token"], ADMIN_PROBE).status_code == 200
    replay = _fresh_client(ctx).post(f"{ADMIN_AUTH}/refresh", json={"refresh_token": login["refresh_token"]})
    assert replay.status_code == 401
    assert _revoked(_probe(ctx, rotated["access_token"], ADMIN_PROBE))


def test_break_glass_reset_ends_admin_access_tokens_and_voids_bootstrap_codes(ctx):
    _make_admin(ctx)
    enrolled = _enroll_admin(ctx)
    access = enrolled.enroll.json()["access_token"]
    unused = _bootstrap_code(ctx)
    maintenance = _load_maintenance()
    with ctx.db.connect() as conn:
        assert maintenance.reset_admin_mfa(conn, ADMIN_EMAIL) is True
    assert _revoked(_probe(ctx, access, ADMIN_PROBE))
    row = _rows(ctx, "SELECT used_at, revoked_at FROM admin_mfa_bootstrap_codes")[0]
    assert row["used_at"] is None and row["revoked_at"] is not None
    assert json.loads(_audit_events(ctx, "admin_mfa_reset_by_operator")[0]["details"])["bootstrap_codes_revoked"] == 1
    assert unused  # the revoked code is checked in the F9 tests below


def test_admin_password_reset_by_the_operator_script_ends_access_tokens(ctx):
    import importlib.util

    from tests.test_section_h84_security import ROOT

    _make_admin(ctx)
    access = _admin_login(_fresh_client(ctx)).json()["access_token"]
    spec = importlib.util.spec_from_file_location(
        "create_admin_under_test", ROOT / "backends" / "user-backend" / "scripts" / "create_admin.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with ctx.db.connect() as conn:
        assert module.upsert_admin(conn, ADMIN_EMAIL, security.get_password_hash("another admin pass phrase"),
                                   "Ops", reset_password=True) == "password_reset"
    assert _revoked(_probe(ctx, access, ADMIN_PROBE))


# =========================================================================== F2

def test_session_rows_expire_with_the_refresh_token(ctx):
    _activate_user(ctx)
    login = _user_login(_fresh_client(ctx)).json()
    rotated = _fresh_client(ctx).post(f"{AUTH}/refresh", json={"refresh_token": login["refresh_token"]}).json()
    for tokens in (login, rotated):
        row = _rows(ctx, "SELECT created_at, expires_at FROM auth_sessions WHERE refresh_token_hash = ?",
                    (security.hash_token(tokens["refresh_token"]),))[0]
        lifetime = datetime.fromisoformat(row["expires_at"]) - datetime.fromisoformat(row["created_at"])
        assert abs(lifetime - timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)) < timedelta(minutes=1)
        jwt_exp = datetime.fromtimestamp(_claims(tokens["refresh_token"])["exp"], tz=timezone.utc)
        assert abs(datetime.fromisoformat(row["expires_at"]) - jwt_exp) < timedelta(minutes=1)

    _make_admin(ctx)
    admin = _admin_login(_fresh_client(ctx)).json()
    row = _rows(ctx, "SELECT created_at, expires_at FROM admin_sessions")[0]
    lifetime = datetime.fromisoformat(row["expires_at"]) - datetime.fromisoformat(row["created_at"])
    assert abs(lifetime - timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)) < timedelta(minutes=1)
    assert admin["refresh_token"]


def test_cookie_lifetimes_follow_the_token_lifetimes(ctx):
    from app.core.session_cookies import cookie_policy

    policy = cookie_policy()
    assert policy.access_max_age == settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    assert policy.refresh_max_age == settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 3600


# =========================================================================== F3

def test_the_dead_user_brokers_route_is_gone(ctx):
    _activate_user(ctx)
    access = _user_login(_fresh_client(ctx)).json()["access_token"]
    assert _fresh_client(ctx).get(f"{AUTH}/user/brokers", headers=_bearer(access)).status_code == 404
    from app.api import auth as auth_api
    assert not [r for r in auth_api.router.routes if getattr(r, "path", "") == "/user/brokers"]


# =========================================================================== F9

def _enrolment_token(ctx, monkeypatch, client=None):
    monkeypatch.setattr(settings, "ADMIN_MFA_REQUIRED", True)
    client = client or _fresh_client(ctx)
    body = _admin_login(client).json()
    assert body["mfa_enrollment_required"] is True and body["mfa_bootstrap_required"] is True
    token = body["enrollment_token"]
    setup = client.post(f"{ADMIN_AUTH}/mfa/setup", json={"enrollment_token": token}).json()
    assert setup["bootstrap_required"] is True
    return client, token, setup["secret"]


def _enroll(ctx, client, token, secret, bootstrap=None):
    body = {"enrollment_token": token, "code": _code(ctx, secret)}
    if bootstrap is not None:
        body["bootstrap_code"] = bootstrap
    return client.post(f"{ADMIN_AUTH}/mfa/enroll", json=body)


def test_required_enrolment_without_a_bootstrap_code_is_refused_and_audited(ctx, monkeypatch):
    _make_admin(ctx)
    client, token, secret = _enrolment_token(ctx, monkeypatch)
    for bootstrap in (None, "", "aaaaa-bbbbb-ccccc"):
        refused = _enroll(ctx, client, token, secret, bootstrap)
        assert refused.status_code == 400 and refused.json()["detail"]["code"] == "MFA_BOOTSTRAP_INVALID"
    assert not _rows(ctx, "SELECT mfa_enabled FROM admins WHERE id = ?", (ADMIN_ID,))[0]["mfa_enabled"]
    reasons = [json.loads(r["details"])["reason"] for r in _audit_events(ctx, "admin_mfa_bootstrap_failed")]
    assert reasons == ["missing", "missing", "invalid_or_expired"]


def test_a_bootstrap_code_enrols_once_is_stored_hashed_and_audited(ctx, monkeypatch):
    _make_admin(ctx)
    code = _bootstrap_code(ctx)
    stored = _rows(ctx, "SELECT * FROM admin_mfa_bootstrap_codes")
    assert len(stored) == 1 and code not in json.dumps(stored) and stored[0]["admin_id"] == ADMIN_ID
    issued = json.loads(_audit_events(ctx, "admin_mfa_bootstrap_issued")[0]["details"])
    assert issued["actor"] == "operator-cli" and code not in json.dumps(issued)

    client, token, secret = _enrolment_token(ctx, monkeypatch)
    enrolled = _enroll(ctx, client, token, secret, code.upper().replace("-", ""))  # normalised like recovery codes
    assert enrolled.status_code == 200, enrolled.text
    assert _rows(ctx, "SELECT used_at FROM admin_mfa_bootstrap_codes")[0]["used_at"]
    assert len(_audit_events(ctx, "admin_mfa_bootstrap_used")) == 1
    assert json.loads(_audit_events(ctx, "admin_mfa_enrolled")[0]["details"])["bootstrap_code"] is True

    # Break-glass reset, then the SAME code cannot enrol again: a fresh one is needed.
    with ctx.db.connect() as conn:
        assert _load_maintenance().reset_admin_mfa(conn, ADMIN_EMAIL)
    _next_step(ctx)
    client, token, secret = _enrolment_token(ctx, monkeypatch)
    assert _enroll(ctx, client, token, secret, code).status_code == 400
    _next_step(ctx)
    assert _enroll(ctx, client, token, secret, _bootstrap_code(ctx)).status_code == 200


def test_a_bootstrap_code_is_bound_to_its_admin_and_expires(ctx, monkeypatch):
    _make_admin(ctx)
    _make_admin(ctx, email="other-ops@example.com", admin_id="admin-other")
    someone_elses = _bootstrap_code(ctx, email="other-ops@example.com")
    client, token, secret = _enrolment_token(ctx, monkeypatch)
    assert _enroll(ctx, client, token, secret, someone_elses).status_code == 400

    expired = _bootstrap_code(ctx)
    _execute(ctx, "UPDATE admin_mfa_bootstrap_codes SET expires_at = ? WHERE admin_id = ?",
             ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(), ADMIN_ID))
    assert _enroll(ctx, client, token, secret, expired).status_code == 400

    first = _bootstrap_code(ctx)
    second = _bootstrap_code(ctx)  # issuing a new code revokes the unused earlier one
    assert _enroll(ctx, client, token, secret, first).status_code == 400
    assert _enroll(ctx, client, token, secret, second).status_code == 200


def test_bootstrap_code_guessing_is_rate_limited(ctx, monkeypatch):
    from app.api import auth as auth_api

    _make_admin(ctx)
    client, token, secret = _enrolment_token(ctx, monkeypatch)
    for _ in range(auth_api.MAX_LOGIN_ATTEMPTS):
        assert _enroll(ctx, client, token, secret, "wrong-guess-00000").status_code == 400
    valid = _bootstrap_code(ctx)
    assert _enroll(ctx, client, token, secret, valid).status_code == 429


def test_bootstrap_codes_have_a_short_bounded_lifetime(ctx):
    from app.core import admin_mfa_bootstrap as boot

    _make_admin(ctx)
    maintenance = _load_maintenance()
    with ctx.db.connect() as conn:
        admin = conn.execute("SELECT id, email FROM admins WHERE id = ?", (ADMIN_ID,)).fetchone()
        for minutes in (0, boot.BOOTSTRAP_MAX_MINUTES + 1):
            with pytest.raises(ValueError):
                boot.issue_bootstrap_code(conn, admin, minutes=minutes)
        assert maintenance.issue_admin_mfa_bootstrap(conn, "nobody@example.com", 30) is None
    assert maintenance.main(["issue-admin-mfa-bootstrap", ADMIN_EMAIL, "--minutes", "0"]) == 2
    assert _rows(ctx, "SELECT id FROM admin_mfa_bootstrap_codes") == []  # the refused calls issued nothing
    assert boot.BOOTSTRAP_DEFAULT_MINUTES == maintenance.BOOTSTRAP_DEFAULT_MINUTES
    assert boot.BOOTSTRAP_MAX_MINUTES == maintenance.BOOTSTRAP_MAX_MINUTES


def test_voluntary_enrolment_needs_no_bootstrap_code_when_mfa_is_not_required(ctx):
    _make_admin(ctx)
    enrolled = _enroll_admin(ctx)  # ADMIN_MFA_REQUIRED off in the fixture
    assert enrolled.enroll.status_code == 200
    assert _audit_events(ctx, "admin_mfa_bootstrap_failed") == []


# =========================================================================== F5

def _legacy_ciphertext(secret: str, value: str) -> str:
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=b"cosmicforge_kyc_salt", iterations=100000)
    return base64.urlsafe_b64encode(Fernet(base64.urlsafe_b64encode(kdf.derive(secret.encode())))
                                    .encrypt(value.encode())).decode()


def _dev_default_secret() -> str:
    from app.core import kyc_encryption
    return kyc_encryption._DEV_DEFAULT_SECRET


def test_production_refuses_the_public_kyc_development_key_by_default(monkeypatch):
    from app.core import kyc_encryption

    monkeypatch.setenv("APP_ENV", "PRODUCTION")
    monkeypatch.setenv("KYC_ENCRYPTION_KEY", "k" * 48)
    monkeypatch.delenv(kyc_encryption.LEGACY_FALLBACK_ENV, raising=False)
    forged = _legacy_ciphertext(_dev_default_secret(), "written with the public key")
    assert kyc_encryption.legacy_dev_key_fallback_enabled() is False
    assert kyc_encryption.decrypt_pii(forged) is None
    blob = Fernet(kyc_encryption._derive_key(_dev_default_secret(), kyc_encryption._FILE_SALT)).encrypt(b"%PDF-1.7")
    with pytest.raises(kyc_encryption.KYCConfigError):
        kyc_encryption.decrypt_file_bytes(blob)
    # Data under the real key is unaffected.
    assert kyc_encryption.decrypt_pii(kyc_encryption.encrypt_pii("Ada")) == "Ada"


@pytest.mark.parametrize("value", ["", "false", "0", "no"])
def test_only_an_explicit_true_enables_the_production_fallback(monkeypatch, value):
    from app.core import kyc_encryption

    monkeypatch.setenv("APP_ENV", "PRODUCTION")
    monkeypatch.setenv("KYC_ENCRYPTION_KEY", "k" * 48)
    monkeypatch.setenv(kyc_encryption.LEGACY_FALLBACK_ENV, value)
    assert kyc_encryption.legacy_dev_key_fallback_enabled() is False


def test_the_explicit_production_opt_in_reads_legacy_data_and_is_audited(monkeypatch, caplog):
    from app.core import kyc_encryption

    monkeypatch.setenv("APP_ENV", "PRODUCTION")
    monkeypatch.setenv("KYC_ENCRYPTION_KEY", "k" * 48)
    monkeypatch.setenv(kyc_encryption.LEGACY_FALLBACK_ENV, "true")
    legacy = _legacy_ciphertext(_dev_default_secret(), "Ada Example")
    with caplog.at_level(logging.WARNING, logger=kyc_encryption.logger.name):
        assert kyc_encryption.decrypt_pii(legacy) == "Ada Example"
        kyc_encryption.assert_kyc_encryption_configured()
    audit = [r.getMessage() for r in caplog.records if "AUDIT" in r.getMessage()]
    assert len(audit) == 2 and all("Ada" not in line for line in audit)
    # Still never used to WRITE.
    fresh = kyc_encryption.encrypt_pii("Linus")
    monkeypatch.setenv(kyc_encryption.LEGACY_FALLBACK_ENV, "false")
    assert kyc_encryption.decrypt_pii(fresh) == "Linus"


def test_outside_production_the_development_key_stays_readable(monkeypatch):
    from app.core import kyc_encryption

    monkeypatch.setenv("APP_ENV", "TEST")
    monkeypatch.setenv("ENVIRONMENT_NAME", "test")
    monkeypatch.setenv("DATABASE_ROLE", "test")
    monkeypatch.setenv("KYC_ENCRYPTION_KEY", "k" * 48)
    monkeypatch.delenv(kyc_encryption.LEGACY_FALLBACK_ENV, raising=False)
    assert kyc_encryption.legacy_dev_key_fallback_enabled() is True
    assert kyc_encryption.decrypt_pii(_legacy_ciphertext(_dev_default_secret(), "Grace")) == "Grace"


# =========================================================================== F8

def test_legacy_sse_url_tokens_are_redacted_from_log_lines():
    from shared_lib.core.security.redaction import MASK, redact_text

    jwt_like = "eyJhbGciOiJIUzI1NiJ9" + ".eyJzdWIiOiJ4In0.c2lnbmF0dXJl"
    line = redact_text(f'127.0.0.1:5000 - "GET /api/v1/events/stream?token={jwt_like} HTTP/1.1" 200')
    assert jwt_like not in line and f"token={MASK}" in line
    assert redact_text("csrf_token_present=yes") == "csrf_token_present=yes"  # no false hit inside other words


def test_uvicorn_access_lines_are_redacted_and_still_formatted():
    from uvicorn.logging import AccessFormatter

    from shared_lib.core.security.redaction import SERVER_LOGGERS, SecretRedactionFilter, install_log_redaction

    install_log_redaction()
    for name in SERVER_LOGGERS:
        assert any(isinstance(f, SecretRedactionFilter) for f in logging.getLogger(name).filters), name
    secret = "eyJhbGciOiJIUzI1NiJ9" + ".payload.signature"
    record = logging.LogRecord("uvicorn.access", logging.INFO, __file__, 1, '%s - "%s %s HTTP/%s" %d',
                               ("127.0.0.1:5000", "GET", f"/api/v1/events/stream?token={secret}", "1.1", 200), None)
    assert SecretRedactionFilter().filter(record)
    formatted = AccessFormatter(fmt='%(client_addr)s - "%(request_line)s" %(status_code)s', use_colors=False
                                ).format(record)
    assert secret not in formatted and "/api/v1/events/stream?token=" in formatted and "200" in formatted
