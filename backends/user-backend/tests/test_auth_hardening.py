"""Regression tests for the authentication hardening (audit fixes).

Covers: durable failed-attempt counters (they used to be rolled back with the
error response), per-account / per-IP login limits, one-time-code attempt
caps and issue limits, uniform login errors, safe re-registration, password
length handling, encrypted TOTP secrets + 2FA at login, admin login limits,
code delivery (never printed/logged in production) and the production secret
check. Everything runs against a throw-away database; nothing uses the network.
"""
from __future__ import annotations

import importlib.util
import logging
import secrets
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pyotp
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from passlib.context import CryptContext
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import admin_auth, auth  # noqa: E402
from app.core import billing_service  # noqa: E402
from app.core import config as user_config  # noqa: E402
from app.core import security  # noqa: E402
from app.core.config import Settings  # noqa: E402
from app.schemas.auth import ResetPasswordRequest, UserCreate  # noqa: E402
from shared_lib.billing.plans import limits_for  # noqa: E402
from shared_lib.notifications.channels import email as email_channel  # noqa: E402
from shared_lib.notifications.channels.email import EmailChannel  # noqa: E402
from shared_lib.persistence.db import DB, utc_now_iso  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

AUTH = "/api/v1/auth"
ADMIN_AUTH = "/api/v1/admin-auth"
EMAIL = "trader@example.com"
PASSWORD = "correct horse battery staple"  # longer than the old 20-character cap
OTHER_PASSWORD = "a completely different one"
ADMIN_EMAIL = "ops@example.com"
ADMIN_PASSWORD = "admin pass phrase for tests"

# The real implementation, kept before the fixture replaces it with a recorder.
_REAL_DELIVER_CODE = auth._deliver_code


@pytest.fixture
def ctx(tmp_path, monkeypatch):
    db_path = tmp_path / "auth_hardening.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.delenv("SMTP_HOST", raising=False)
    monkeypatch.delenv("SMTP_USER", raising=False)
    migrate(str(db_path))
    db = DB(path=str(db_path))

    monkeypatch.setattr(auth, "DB", lambda: db)
    monkeypatch.setattr(admin_auth, "DB", lambda: db)
    # Token creation must not look for a subscription in another database.
    monkeypatch.setattr(security, "get_user_subscription", lambda uid: {"entitlements": {}})
    monkeypatch.setattr(auth, "get_user_subscription", lambda uid: {"entitlements": {}})
    # Same algorithm as production, cheapest cost factor: keeps the suite fast.
    monkeypatch.setattr(security, "pwd_context", CryptContext(schemes=["bcrypt"], bcrypt__rounds=4))
    monkeypatch.setattr(security, "_DUMMY_PASSWORD_HASH", None)

    # Predictable, always-distinct one-time codes; deliveries are recorded.
    issued = iter(f"{n:06d}" for n in range(100001, 100999))
    monkeypatch.setattr(auth, "generate_otp", lambda: next(issued))
    sent: list[tuple[str, str, str]] = []
    monkeypatch.setattr(auth, "_deliver_code", lambda email, purpose, code: sent.append((email, purpose, code)))

    app = FastAPI()
    app.include_router(auth.router, prefix=AUTH)
    app.include_router(admin_auth.router, prefix=ADMIN_AUTH)
    return SimpleNamespace(client=TestClient(app), db=db, sent=sent)


# --------------------------------------------------------------------------- helpers

def _rows(ctx, sql: str, params: tuple = ()) -> list[dict]:
    with ctx.db.connect() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _execute(ctx, sql: str, params: tuple = ()) -> None:
    with ctx.db.connect() as conn:
        conn.execute(sql, params)


def _register(ctx, email: str = EMAIL, password: str = PASSWORD):
    return ctx.client.post(f"{AUTH}/register", json={"email": email, "password": password})


def _last_code(ctx) -> str:
    return ctx.sent[-1][2]


def _wrong(code: str) -> str:
    return "000000" if code != "000000" else "000001"


def _verify(ctx, code: str, email: str = EMAIL):
    return ctx.client.post(f"{AUTH}/verify-email", json={"email": email, "code": code})


def _activate(ctx, email: str = EMAIL, password: str = PASSWORD) -> None:
    assert _register(ctx, email, password).status_code == 200
    assert _verify(ctx, _last_code(ctx), email).status_code == 200


def _login(ctx, email: str = EMAIL, password: str = PASSWORD, **extra):
    return ctx.client.post(f"{AUTH}/login", data={"username": email, "password": password, **extra})


def _forgot(ctx, email: str = EMAIL):
    return ctx.client.post(f"{AUTH}/forgot-password", json={"email": email})


def _reset(ctx, code: str, new_password: str, email: str = EMAIL):
    return ctx.client.post(
        f"{AUTH}/reset-password", json={"email": email, "code": code, "new_password": new_password})


def _failed_attempts(ctx, key: str) -> int:
    return len(_rows(ctx, "SELECT id FROM login_attempts WHERE email = ? AND success = 0", (key,)))


def _bearer(ctx) -> dict:
    response = _login(ctx)
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


def _invalid_totp(secret: str, at: float | None = None) -> str:
    totp = pyotp.TOTP(secret)
    step = int(time.time() if at is None else at) // 30
    live = {totp.generate_otp(step + offset) for offset in (-2, -1, 0, 1, 2)}
    return next(c for c in ("000000", "111111", "222222", "333333", "444444", "555555") if c not in live)


def _totp_clock(monkeypatch) -> SimpleNamespace:
    """A controllable clock for authenticator codes: every accepted code is
    single-use, so tests move to the next 30-second step between valid uses."""
    clock = SimpleNamespace(now=float(int(time.time()) // 30 * 30 + 10))
    monkeypatch.setattr(security, "_totp_time", lambda: clock.now)
    return clock


def _totp_code(secret: str, clock: SimpleNamespace, step_offset: int = 0) -> str:
    return pyotp.TOTP(secret).generate_otp(int(clock.now) // 30 + step_offset)


def _use_ip(monkeypatch, ip: str = "198.51.100.1") -> SimpleNamespace:
    """Let a test choose the client address each request appears to come from."""
    holder = SimpleNamespace(ip=ip)
    monkeypatch.setattr(auth, "_client_ip", lambda request: holder.ip)
    return holder


def _set_subscription(ctx, user_id: str, *, plan_id: str = "plan_pro", period_end: datetime) -> None:
    now = utc_now_iso()
    _execute(
        ctx,
        "INSERT INTO subscriptions (user_id, plan_id, status, provider, provider_sub_id, current_period_end, "
        "cancel_at_period_end, created_at, updated_at) VALUES (?, ?, 'active', 'stripe', 'sub_1', ?, 0, ?, ?)",
        (user_id, plan_id, period_end.isoformat(), now, now),
    )


def _real_entitlements(ctx, monkeypatch) -> None:
    """Token creation reads the real subscription (the fixture stubs it out)."""
    monkeypatch.setattr(billing_service, "_db", lambda: ctx.db)
    monkeypatch.setattr(security, "get_user_subscription", billing_service.get_user_subscription)


def _write_lock_is_free(ctx) -> bool:
    conn = sqlite3.connect(ctx.db.path, timeout=0.2)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.rollback()
        return True
    except sqlite3.OperationalError:
        return False
    finally:
        conn.close()


def _enable_2fa_directly(ctx, stored_secret: str) -> None:
    _execute(ctx, "UPDATE users SET totp_secret = ?, is_2fa_enabled = 1 WHERE email = ?", (stored_secret, EMAIL))


def _make_admin(ctx, email: str = ADMIN_EMAIL, password: str = ADMIN_PASSWORD) -> None:
    now = utc_now_iso()
    _execute(
        ctx,
        "INSERT INTO admins (id, email, hashed_password, full_name, role, is_active, is_superuser, "
        "created_at, updated_at) VALUES (?, ?, ?, 'Ops', 'admin', 1, 1, ?, ?)",
        ("admin-1", email, security.get_password_hash(password), now, now),
    )


def _admin_login(ctx, password: str = ADMIN_PASSWORD, email: str = ADMIN_EMAIL):
    return ctx.client.post(f"{ADMIN_AUTH}/login", data={"username": email, "password": password})


# --------------------------------------------------------------------------- login limits

def test_failed_logins_are_committed_and_lock_out_the_guessing_address(ctx):
    _activate(ctx)

    for _ in range(auth.MAX_LOGIN_ATTEMPTS):
        response = _login(ctx, password="not the password")
        assert response.status_code == 400
        assert response.json()["detail"] == auth.LOGIN_FAILED_DETAIL

    # The failures survived the error responses (they used to be rolled back).
    assert _failed_attempts(ctx, EMAIL) == auth.MAX_LOGIN_ATTEMPTS

    # Even the right password is refused from that address while it is locked out...
    locked = _login(ctx)
    assert locked.status_code == 429
    # ...and a refused request is not itself counted.
    assert _failed_attempts(ctx, EMAIL) == auth.MAX_LOGIN_ATTEMPTS


def test_successful_login_is_recorded_as_success(ctx):
    _activate(ctx)

    response = _login(ctx)

    assert response.status_code == 200
    assert response.json()["access_token"] and response.json()["refresh_token"]
    assert _failed_attempts(ctx, EMAIL) == 0
    assert len(_rows(ctx, "SELECT id FROM login_attempts WHERE email = ? AND success = 1", (EMAIL,))) == 1


def test_login_does_not_reveal_whether_the_email_exists(ctx, monkeypatch):
    _activate(ctx)
    dummy_checks = []
    real_dummy = auth.dummy_verify_password
    monkeypatch.setattr(auth, "dummy_verify_password", lambda pw: (dummy_checks.append(pw), real_dummy(pw))[1])

    wrong_password = _login(ctx, password="not the password")
    unknown_email = _login(ctx, email="nobody@example.com", password="not the password")

    assert wrong_password.status_code == unknown_email.status_code == 400
    assert wrong_password.json() == unknown_email.json() == {"detail": auth.LOGIN_FAILED_DETAIL}
    # The unknown address still paid for a password hash comparison.
    assert dummy_checks == ["not the password"]


def test_login_is_limited_per_ip_across_accounts(ctx, monkeypatch):
    monkeypatch.setattr(auth, "MAX_LOGIN_ATTEMPTS_PER_IP", 3)

    for n in range(3):
        assert _login(ctx, email=f"guess{n}@example.com", password="x").status_code == 400

    assert _login(ctx, email="guess-next@example.com", password="x").status_code == 429


def test_guesses_from_one_address_do_not_lock_the_owner_out_from_another(ctx, monkeypatch):
    _activate(ctx)
    client = _use_ip(monkeypatch, "203.0.113.66")  # the attacker

    for _ in range(auth.MAX_LOGIN_ATTEMPTS):
        assert _login(ctx, password="not the password").status_code == 400
    # The attacker's address is locked out of this account (brute force still stopped)...
    assert _login(ctx, password="not the password").status_code == 429
    assert _login(ctx).status_code == 429

    # ...but the owner, coming from somewhere else, signs in with the right password.
    client.ip = "198.51.100.7"
    assert _login(ctx).status_code == 200

    # That success did not wipe the attacker's failures: still locked out, still counted.
    client.ip = "203.0.113.66"
    assert _login(ctx).status_code == 429
    assert _failed_attempts(ctx, EMAIL) == auth.MAX_LOGIN_ATTEMPTS


def test_account_has_a_ceiling_across_all_addresses(ctx, monkeypatch):
    _activate(ctx)
    client = _use_ip(monkeypatch)
    assert auth.MAX_LOGIN_ATTEMPTS_PER_ACCOUNT > auth.MAX_LOGIN_ATTEMPTS

    made = 0
    address = 0
    while made < auth.MAX_LOGIN_ATTEMPTS_PER_ACCOUNT:
        address += 1
        client.ip = f"203.0.113.{address}"
        for _ in range(min(auth.MAX_LOGIN_ATTEMPTS, auth.MAX_LOGIN_ATTEMPTS_PER_ACCOUNT - made)):
            assert _login(ctx, password="not the password").status_code == 400
            made += 1

    # A distributed guesser is stopped: no address gets another try, right password or not.
    client.ip = "198.51.100.200"
    assert _login(ctx, password="not the password").status_code == 429
    assert _login(ctx).status_code == 429
    assert _failed_attempts(ctx, EMAIL) == auth.MAX_LOGIN_ATTEMPTS_PER_ACCOUNT
    # Other accounts are unaffected.
    assert _login(ctx, email="someone-else@example.com", password="x").status_code == 400


def test_completed_password_reset_clears_the_accounts_failed_logins(ctx, monkeypatch):
    _activate(ctx)
    monkeypatch.setattr(auth, "MAX_LOGIN_ATTEMPTS_PER_ACCOUNT", auth.MAX_LOGIN_ATTEMPTS)
    client = _use_ip(monkeypatch, "203.0.113.66")
    for _ in range(auth.MAX_LOGIN_ATTEMPTS):
        assert _login(ctx, password="not the password").status_code == 400

    # The account ceiling is reached: the owner is locked out everywhere...
    client.ip = "198.51.100.7"
    assert _login(ctx).status_code == 429

    # ...until they prove control of the mailbox by completing a reset.
    assert _forgot(ctx).status_code == 200
    assert _reset(ctx, _wrong(_last_code(ctx)), OTHER_PASSWORD).status_code == 400  # a failed reset clears nothing
    assert _login(ctx).status_code == 429
    assert _reset(ctx, _last_code(ctx), OTHER_PASSWORD).status_code == 200
    assert _failed_attempts(ctx, EMAIL) == 0
    assert _login(ctx, password=OTHER_PASSWORD).status_code == 200

    # The failures were re-keyed, not deleted: they still count against the address that made them.
    assert _failed_attempts(ctx, auth.CLEARED_ATTEMPT_PREFIX + EMAIL) == auth.MAX_LOGIN_ATTEMPTS
    rows = _rows(ctx, "SELECT DISTINCT ip FROM login_attempts WHERE email = ?", (auth.CLEARED_ATTEMPT_PREFIX + EMAIL,))
    assert [r["ip"] for r in rows] == ["203.0.113.66"]
    monkeypatch.setattr(auth, "MAX_LOGIN_ATTEMPTS_PER_IP", auth.MAX_LOGIN_ATTEMPTS)
    client.ip = "203.0.113.66"
    assert _login(ctx, email="another-target@example.com", password="x").status_code == 429


def test_loopback_and_unknown_addresses_are_not_ip_limited():
    assert auth._ip_limitable("203.0.113.9")
    assert not auth._ip_limitable("127.0.0.1")
    assert not auth._ip_limitable("::1")
    assert not auth._ip_limitable("unknown")
    assert not auth._ip_limitable(None)


def test_unverified_login_with_right_password_is_not_a_counted_failure(ctx):
    assert _register(ctx).status_code == 200

    response = _login(ctx)

    assert response.status_code == 403
    assert response.json()["detail"] == "User not verified"
    assert _failed_attempts(ctx, EMAIL) == 0


def test_suspended_account_cannot_log_in(ctx):
    _activate(ctx)
    _execute(ctx, "UPDATE users SET status = 'suspended' WHERE email = ?", (EMAIL,))

    assert _login(ctx).status_code == 403


# --------------------------------------------------------------------------- one-time codes

def test_wrong_verification_codes_are_counted_and_exhaust_the_code(ctx):
    assert _register(ctx).status_code == 200
    code = _last_code(ctx)

    for _ in range(auth.MAX_VERIFY_ATTEMPTS):
        response = _verify(ctx, _wrong(code))
        assert response.status_code == 400
        assert response.json()["detail"] == auth.CODE_FAILED_DETAIL

    attempts = _rows(ctx, "SELECT attempts FROM email_verifications WHERE used_at IS NULL")
    assert [r["attempts"] for r in attempts] == [auth.MAX_VERIFY_ATTEMPTS]

    # The code is spent: the correct value no longer verifies the account.
    assert _verify(ctx, code).status_code == 400
    assert _rows(ctx, "SELECT status FROM users WHERE email = ?", (EMAIL,))[0]["status"] == "pending_verification"


def test_wrong_reset_codes_are_counted_and_exhaust_the_code(ctx):
    _activate(ctx)
    assert _forgot(ctx).status_code == 200
    code = _last_code(ctx)

    for _ in range(auth.MAX_RESET_ATTEMPTS):
        assert _reset(ctx, _wrong(code), OTHER_PASSWORD).status_code == 400

    attempts = _rows(ctx, "SELECT attempts FROM password_resets WHERE used_at IS NULL")
    assert [r["attempts"] for r in attempts] == [auth.MAX_RESET_ATTEMPTS]

    assert _reset(ctx, code, OTHER_PASSWORD).status_code == 400
    # The password did not change.
    assert _login(ctx).status_code == 200


def test_new_reset_code_invalidates_the_older_one(ctx):
    _activate(ctx)
    assert _forgot(ctx).status_code == 200
    first = _last_code(ctx)
    assert _forgot(ctx).status_code == 200
    second = _last_code(ctx)
    assert first != second

    assert len(_rows(ctx, "SELECT id FROM password_resets WHERE used_at IS NULL")) == 1
    assert _reset(ctx, first, OTHER_PASSWORD).status_code == 400

    assert _reset(ctx, second, OTHER_PASSWORD).status_code == 200
    assert _login(ctx, password=OTHER_PASSWORD).status_code == 200
    assert _login(ctx, password=PASSWORD).status_code == 400
    # A used code cannot be replayed.
    assert _reset(ctx, second, PASSWORD).status_code == 400


def test_reset_failures_look_the_same_for_unknown_addresses(ctx):
    _activate(ctx)
    assert _forgot(ctx).status_code == 200

    known = _reset(ctx, _wrong(_last_code(ctx)), OTHER_PASSWORD)
    unknown = _reset(ctx, "123456", OTHER_PASSWORD, email="nobody@example.com")

    assert known.status_code == unknown.status_code == 400
    assert known.json() == unknown.json()


def test_forgot_password_is_rate_limited_the_same_for_unknown_addresses(ctx):
    _activate(ctx)
    sent_before = len(ctx.sent)

    for email in (EMAIL, "nobody@example.com"):
        responses = [_forgot(ctx, email) for _ in range(auth.MAX_CODES_PER_EMAIL + 1)]
        assert [r.status_code for r in responses] == [200] * auth.MAX_CODES_PER_EMAIL + [429]
        assert len({r.text for r in responses[:-1]}) == 1

    # Codes were only issued for the real account, and no more than the limit.
    reset_mails = [s for s in ctx.sent[sent_before:] if s[1] == "password reset"]
    assert [s[0] for s in reset_mails] == [EMAIL] * auth.MAX_CODES_PER_EMAIL


def test_code_requests_are_limited_per_ip(ctx, monkeypatch):
    monkeypatch.setattr(auth, "MAX_CODES_PER_IP", 2)

    assert _forgot(ctx, "a@example.com").status_code == 200
    assert _forgot(ctx, "b@example.com").status_code == 200
    assert _forgot(ctx, "c@example.com").status_code == 429


def test_resend_verification_is_rate_limited(ctx):
    assert _register(ctx).status_code == 200  # first code of the window

    statuses = [
        ctx.client.post(f"{AUTH}/resend-verification", json={"email": EMAIL}).status_code
        for _ in range(auth.MAX_CODES_PER_EMAIL)
    ]

    assert statuses == [200] * (auth.MAX_CODES_PER_EMAIL - 1) + [429]
    assert len(ctx.sent) == auth.MAX_CODES_PER_EMAIL
    # Only the newest code is still usable.
    assert len(_rows(ctx, "SELECT id FROM email_verifications WHERE used_at IS NULL")) == 1


# --------------------------------------------------------------------------- registration

def test_reregistering_with_the_same_password_only_resends_the_code(ctx):
    first = _register(ctx)
    first_code = _last_code(ctx)
    second = _register(ctx)

    assert first.status_code == second.status_code == 200
    assert first.json()["id"] == second.json()["id"]
    assert len(_rows(ctx, "SELECT id FROM users WHERE email = ?", (EMAIL,))) == 1

    assert _verify(ctx, first_code).status_code == 400  # superseded
    assert _verify(ctx, _last_code(ctx)).status_code == 200
    assert _login(ctx).status_code == 200


def test_reregistering_with_another_password_cannot_take_over_the_account(ctx):
    assert _register(ctx, password=PASSWORD).status_code == 200
    # Someone else signs the same unverified address up with their own password.
    assert _register(ctx, password=OTHER_PASSWORD).status_code == 200

    stored = _rows(ctx, "SELECT hashed_password FROM users WHERE email = ?", (EMAIL,))[0]["hashed_password"]
    assert stored == auth.UNUSABLE_PASSWORD

    # The mailbox owner verifies; neither submitted password became live.
    verified = _verify(ctx, _last_code(ctx))
    assert verified.status_code == 200
    assert verified.json()["password_reset_required"] is True
    assert _login(ctx, password=PASSWORD).status_code == 400
    assert _login(ctx, password=OTHER_PASSWORD).status_code == 400

    # Only the owner (who receives the reset code) can set a password.
    assert _forgot(ctx).status_code == 200
    assert _reset(ctx, _last_code(ctx), "the owner's new password").status_code == 200
    assert _login(ctx, password="the owner's new password").status_code == 200


def test_registering_an_active_address_is_refused(ctx):
    _activate(ctx)

    response = _register(ctx, password=OTHER_PASSWORD)

    assert response.status_code == 400
    assert _login(ctx).status_code == 200


def test_verification_cannot_reactivate_a_suspended_account(ctx):
    assert _register(ctx).status_code == 200
    _execute(ctx, "UPDATE users SET status = 'suspended' WHERE email = ?", (EMAIL,))

    assert _verify(ctx, _last_code(ctx)).status_code == 400
    assert _rows(ctx, "SELECT status FROM users WHERE email = ?", (EMAIL,))[0]["status"] == "suspended"


# --------------------------------------------------------------------------- passwords

@pytest.mark.parametrize("password", ["a" * 8, "a" * 21, "a" * 72, "é" * 36])
def test_password_lengths_that_are_accepted(password):
    assert UserCreate(email=EMAIL, password=password).password == password
    assert ResetPasswordRequest(email=EMAIL, code="123456", new_password=password).new_password == password


@pytest.mark.parametrize("password", ["a" * 7, "a" * 73, "é" * 37])
def test_password_lengths_that_are_rejected(password):
    with pytest.raises(ValidationError):
        UserCreate(email=EMAIL, password=password)
    with pytest.raises(ValidationError):
        ResetPasswordRequest(email=EMAIL, code="123456", new_password=password)


def test_long_password_round_trips_and_is_never_truncated(ctx):
    long_password = "x" * 71 + "A"
    _activate(ctx, password=long_password)

    assert _login(ctx, password=long_password).status_code == 200
    # Differs only in the last character: bcrypt truncation would accept it.
    assert _login(ctx, password="x" * 71 + "B").status_code == 400
    assert ctx.client.post(
        f"{AUTH}/register", json={"email": "other@example.com", "password": "x" * 73}).status_code == 422


def test_verify_password_never_matches_overlong_or_unusable_hashes(ctx):
    hashed = security.get_password_hash("x" * 72)

    assert security.verify_password("x" * 72, hashed) is True
    assert security.verify_password("x" * 73, hashed) is False
    assert security.verify_password("anything", auth.UNUSABLE_PASSWORD) is False
    assert security.verify_password("anything", "") is False
    with pytest.raises(ValueError):
        security.get_password_hash("x" * 73)


# --------------------------------------------------------------------------- 2FA

def test_2fa_secret_is_stored_encrypted_and_login_requires_the_code(ctx, monkeypatch):
    _activate(ctx)
    headers = _bearer(ctx)
    clock = _totp_clock(monkeypatch)

    setup = ctx.client.post(f"{AUTH}/2fa/setup", headers=headers)
    assert setup.status_code == 200, setup.text
    secret = setup.json()["items"]
    assert secret in setup.json()["uri"]

    stored = _rows(ctx, "SELECT totp_secret FROM users WHERE email = ?", (EMAIL,))[0]["totp_secret"]
    assert stored.startswith("enc:v1:") and secret not in stored
    assert security.decrypt_totp_secret(stored) == secret

    # Not enabled until a valid code proves the authenticator was set up.
    assert ctx.client.post(
        f"{AUTH}/2fa/verify", json={"code": _invalid_totp(secret, clock.now)}, headers=headers).status_code == 400
    assert _login(ctx).status_code == 200
    enabled = ctx.client.post(f"{AUTH}/2fa/verify", json={"code": _totp_code(secret, clock)}, headers=headers)
    assert enabled.status_code == 200, enabled.text
    assert ctx.client.get(f"{AUTH}/me", headers=headers).json()["is_2fa_enabled"] is True

    # Password alone is no longer enough.
    missing = _login(ctx)
    assert missing.status_code == 401
    assert missing.json()["detail"]["code"] == "TOTP_REQUIRED"

    failures_before = _failed_attempts(ctx, EMAIL)
    wrong = _login(ctx, totp_code=_invalid_totp(secret, clock.now))
    assert wrong.status_code == 401
    assert wrong.json()["detail"]["code"] == "TOTP_INVALID"
    assert _failed_attempts(ctx, EMAIL) == failures_before + 1

    # The code that enabled 2FA was spent doing so: it cannot also log in.
    replay = _login(ctx, totp_code=_totp_code(secret, clock))
    assert replay.status_code == 401
    assert replay.json()["detail"]["code"] == "TOTP_INVALID"

    clock.now += 30
    ok = _login(ctx, totp_code=_totp_code(secret, clock))
    assert ok.status_code == 200
    assert ok.json()["access_token"]

    # The secret of an account with 2FA on cannot be silently replaced.
    assert ctx.client.post(f"{AUTH}/2fa/setup", headers=headers).status_code == 400

    # Disabling needs a valid code, then the password is enough again.
    assert ctx.client.post(
        f"{AUTH}/2fa/disable", json={"code": _invalid_totp(secret, clock.now)}, headers=headers).status_code == 400
    # The code just used to log in cannot be replayed to switch 2FA off...
    assert ctx.client.post(
        f"{AUTH}/2fa/disable", json={"code": _totp_code(secret, clock)}, headers=headers).status_code == 400
    assert _rows(ctx, "SELECT is_2fa_enabled FROM users WHERE email = ?", (EMAIL,))[0]["is_2fa_enabled"] == 1
    # ...a fresh one can.
    clock.now += 30
    assert ctx.client.post(
        f"{AUTH}/2fa/disable", json={"code": _totp_code(secret, clock)}, headers=headers).status_code == 200
    assert _rows(ctx, "SELECT totp_secret, is_2fa_enabled, totp_last_counter FROM users WHERE email = ?",
                 (EMAIL,))[0] == {"totp_secret": None, "is_2fa_enabled": 0, "totp_last_counter": None}
    assert _login(ctx).status_code == 200


def test_legacy_plaintext_totp_secret_still_works(ctx):
    _activate(ctx)
    secret = pyotp.random_base32()
    _enable_2fa_directly(ctx, secret)  # stored the way older versions stored it

    assert _login(ctx).json()["detail"]["code"] == "TOTP_REQUIRED"
    assert _login(ctx, totp_code=pyotp.TOTP(secret).now()).status_code == 200


def test_totp_guessing_at_login_is_rate_limited(ctx):
    _activate(ctx)
    secret = pyotp.random_base32()
    _enable_2fa_directly(ctx, security.encrypt_totp_secret(secret))

    for _ in range(auth.MAX_LOGIN_ATTEMPTS):
        assert _login(ctx, totp_code=_invalid_totp(secret)).status_code == 401

    assert _login(ctx, totp_code=pyotp.TOTP(secret).now()).status_code == 429


def test_totp_code_cannot_be_replayed_at_login(ctx, monkeypatch):
    _activate(ctx)
    secret = pyotp.random_base32()
    _enable_2fa_directly(ctx, security.encrypt_totp_secret(secret))
    clock = _totp_clock(monkeypatch)
    current = _totp_code(secret, clock)
    previous = _totp_code(secret, clock, -1)
    following = _totp_code(secret, clock, +1)

    assert _login(ctx, totp_code=current).status_code == 200
    step = int(clock.now) // 30
    assert _rows(ctx, "SELECT totp_last_counter FROM users WHERE email = ?", (EMAIL,))[0]["totp_last_counter"] == step

    # The same code again -- still inside its validity window -- is refused...
    replay = _login(ctx, totp_code=current)
    assert replay.status_code == 401
    assert replay.json()["detail"]["code"] == "TOTP_INVALID"
    # ...as is the (also still "valid") code of the step before it...
    assert _login(ctx, totp_code=previous).status_code == 401
    # ...and both count as failed attempts.
    assert _failed_attempts(ctx, EMAIL) == 2

    # A code for a later step works, once.
    assert _login(ctx, totp_code=following).status_code == 200
    assert _login(ctx, totp_code=following).status_code == 401
    assert _rows(ctx, "SELECT totp_last_counter FROM users WHERE email = ?", (EMAIL,))[0]["totp_last_counter"] == step + 1

    # Thirty seconds on, the then-current code is that same step: still spent.
    clock.now += 30
    assert _login(ctx, totp_code=_totp_code(secret, clock)).status_code == 401


def test_totp_counter_matching_rejects_used_and_out_of_window_steps(monkeypatch):
    secret = pyotp.random_base32()
    clock = _totp_clock(monkeypatch)
    step = int(clock.now) // 30
    code = _totp_code(secret, clock)

    assert security.match_totp_counter(secret, code) == step
    assert security.match_totp_counter(secret, code, last_counter=step - 1) == step
    assert security.match_totp_counter(secret, code, last_counter=step) is None
    assert security.match_totp_counter(secret, code, last_counter=step + 5) is None
    assert security.match_totp_counter(secret, _totp_code(secret, clock, -1)) == step - 1
    assert security.match_totp_counter(secret, _totp_code(secret, clock, +1)) == step + 1
    # Outside the one-step drift window.
    two_back = _totp_code(secret, clock, -2)
    if two_back not in {_totp_code(secret, clock, o) for o in (-1, 0, 1)}:
        assert security.match_totp_counter(secret, two_back) is None
    assert security.match_totp_counter(secret, "12345") is None
    assert security.match_totp_counter(None, code) is None


def test_totp_counter_column_is_added_lazily_and_idempotently(ctx):
    with ctx.db.connect() as conn:
        assert "totp_last_counter" not in {r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()}
        auth._ensure_totp_counter_column(conn)
        auth._ensure_totp_counter_column(conn)
        assert "totp_last_counter" in {r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()}

    class Racing:
        """Another worker added the column between the check and the ALTER."""
        def execute(self, sql, *args):
            if sql.startswith("PRAGMA"):
                return SimpleNamespace(fetchall=lambda: [(0, "id")])
            raise sqlite3.OperationalError("duplicate column name: totp_last_counter")

    auth._ensure_totp_counter_column(Racing())  # tolerated

    class Broken(Racing):
        def execute(self, sql, *args):
            if sql.startswith("PRAGMA"):
                return SimpleNamespace(fetchall=lambda: [(0, "id")])
            raise sqlite3.OperationalError("disk I/O error")

    with pytest.raises(sqlite3.OperationalError):
        auth._ensure_totp_counter_column(Broken())


def test_2fa_code_guessing_keeps_the_strict_per_account_limit(ctx, monkeypatch):
    _activate(ctx)
    headers = _bearer(ctx)
    secret = ctx.client.post(f"{AUTH}/2fa/setup", headers=headers).json()["items"]
    client = _use_ip(monkeypatch)
    clock = _totp_clock(monkeypatch)

    # Spread over several addresses: the limit is still MAX_LOGIN_ATTEMPTS in total.
    for n in range(auth.MAX_LOGIN_ATTEMPTS):
        client.ip = f"203.0.113.{n + 1}"
        assert ctx.client.post(
            f"{AUTH}/2fa/verify", json={"code": _invalid_totp(secret, clock.now)}, headers=headers).status_code == 400
    client.ip = "198.51.100.9"
    assert ctx.client.post(
        f"{AUTH}/2fa/verify", json={"code": _totp_code(secret, clock)}, headers=headers).status_code == 429


def test_malformed_totp_codes_never_verify():
    secret = pyotp.random_base32()

    assert security.verify_totp_code(secret, pyotp.TOTP(secret).now()) is True
    for bad in (None, "", "12345", "1234567", "abcdef", "１２３４５６"):
        assert security.verify_totp_code(secret, bad) is False
    assert security.verify_totp_code(None, "123456") is False
    assert security.verify_totp_code("enc:v1:not-a-valid-token", "123456") is False


# --------------------------------------------------------------------------- tokens and the write lock

def test_login_with_a_lapsed_subscription_is_prompt_and_gets_free_entitlements(ctx, monkeypatch):
    """Regression: token creation used to persist the lazy downgrade on a SECOND
    connection while login still held the write lock on its own -- a 10 s stall
    ending in "database is locked", an empty-entitlement token and no downgrade."""
    _activate(ctx)
    user_id = _rows(ctx, "SELECT id FROM users WHERE email = ?", (EMAIL,))[0]["id"]
    _set_subscription(ctx, user_id, period_end=datetime.now(timezone.utc) - timedelta(days=60))
    _real_entitlements(ctx, monkeypatch)

    started = time.monotonic()
    response = _login(ctx)
    elapsed = time.monotonic() - started

    assert response.status_code == 200, response.text
    assert elapsed < 5, f"login took {elapsed:.1f}s"
    claims = security.decode_token(response.json()["access_token"])
    assert claims["sub"] == user_id
    assert claims["entitlements"] == limits_for("plan_free")
    assert claims["entitlements"]["live_trading"] is False
    # The login itself was recorded (its transaction was not disturbed)...
    assert len(_rows(ctx, "SELECT id FROM auth_sessions WHERE user_id = ?", (user_id,))) == 1
    assert _rows(ctx, "SELECT last_login_at FROM users WHERE id = ?", (user_id,))[0]["last_login_at"]
    # ...and minting a token wrote nothing to the subscription.
    assert _rows(ctx, "SELECT plan_id, status FROM subscriptions")[0] == {"plan_id": "plan_pro", "status": "active"}

    # Refresh behaves the same way.
    started = time.monotonic()
    refreshed = ctx.client.post(f"{AUTH}/refresh", json={"refresh_token": response.json()["refresh_token"]})
    assert refreshed.status_code == 200, refreshed.text
    assert time.monotonic() - started < 5
    assert security.decode_token(refreshed.json()["access_token"])["entitlements"] == limits_for("plan_free")
    assert _rows(ctx, "SELECT plan_id, status FROM subscriptions")[0] == {"plan_id": "plan_pro", "status": "active"}

    # The downgrade is stored where it is asked for (the billing status read).
    assert billing_service.get_user_subscription(user_id, persist=True)["plan"]["id"] == "plan_free"
    assert _rows(ctx, "SELECT plan_id, status FROM subscriptions")[0] == {"plan_id": "plan_free", "status": "expired"}


def test_login_with_a_live_subscription_gets_paid_entitlements(ctx, monkeypatch):
    _activate(ctx)
    user_id = _rows(ctx, "SELECT id FROM users WHERE email = ?", (EMAIL,))[0]["id"]
    _set_subscription(ctx, user_id, period_end=datetime.now(timezone.utc) + timedelta(days=20))
    _real_entitlements(ctx, monkeypatch)

    claims = security.decode_token(_login(ctx).json()["access_token"])
    assert claims["entitlements"] == limits_for("plan_pro")


def test_access_tokens_are_minted_after_the_request_transaction_committed(ctx, monkeypatch):
    """Whatever token creation does, it must not run while login / refresh still
    hold SQLite's single write lock."""
    _activate(ctx)
    lock_free = []

    def probe(user_id):
        lock_free.append(_write_lock_is_free(ctx))
        return {"entitlements": {}}

    monkeypatch.setattr(security, "get_user_subscription", probe)

    tokens = _login(ctx)
    assert tokens.status_code == 200
    refreshed = ctx.client.post(f"{AUTH}/refresh", json={"refresh_token": tokens.json()["refresh_token"]})
    assert refreshed.status_code == 200
    assert lock_free == [True, True]
    # The session row each token pairs with was already committed by then.
    assert len(_rows(ctx, "SELECT id FROM auth_sessions")) == 2


# --------------------------------------------------------------------------- sessions

def test_refresh_token_reuse_revokes_every_session_durably(ctx):
    _activate(ctx)
    tokens = _login(ctx).json()

    rotated = ctx.client.post(f"{AUTH}/refresh", json={"refresh_token": tokens["refresh_token"]})
    assert rotated.status_code == 200
    replay = ctx.client.post(f"{AUTH}/refresh", json={"refresh_token": tokens["refresh_token"]})
    assert replay.status_code == 401

    # The revocation was committed before the 401 was raised.
    assert _rows(ctx, "SELECT id FROM auth_sessions WHERE revoked_at IS NULL") == []
    assert ctx.client.post(
        f"{AUTH}/refresh", json={"refresh_token": rotated.json()["refresh_token"]}).status_code == 401


def test_legacy_broker_link_route_is_gone(ctx):
    _activate(ctx)

    response = ctx.client.post(
        f"{AUTH}/user/brokers",
        json={"name": "x", "api_key": "k", "api_secret": "s"},
        headers=_bearer(ctx),
    )

    assert response.status_code in (404, 405)


# --------------------------------------------------------------------------- admin login

def test_admin_login_failures_are_committed_and_rate_limited(ctx):
    _make_admin(ctx)

    for _ in range(auth.MAX_LOGIN_ATTEMPTS):
        response = _admin_login(ctx, password="not the password")
        assert response.status_code == 400
        assert response.json()["detail"] == "Incorrect email or password"

    assert _failed_attempts(ctx, f"admin:{ADMIN_EMAIL}") == auth.MAX_LOGIN_ATTEMPTS
    assert _admin_login(ctx).status_code == 429


def test_admin_login_succeeds_and_unknown_admin_gets_the_same_error(ctx):
    _make_admin(ctx)

    ok = _admin_login(ctx)
    assert ok.status_code == 200
    assert ok.json()["access_token"] and ok.json()["refresh_token"]
    assert _failed_attempts(ctx, f"admin:{ADMIN_EMAIL}") == 0

    unknown = _admin_login(ctx, email="nobody@example.com")
    wrong = _admin_login(ctx, password="not the password")
    assert unknown.status_code == wrong.status_code == 400
    assert unknown.json() == wrong.json()


# --------------------------------------------------------------------------- code delivery

def _capture_email(monkeypatch, result=True):
    calls = []

    def fake_send(recipient, subject, body_html, body_text=None):
        calls.append(SimpleNamespace(recipient=recipient, subject=subject, html=body_html, text=body_text))
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(EmailChannel, "send", staticmethod(fake_send))
    monkeypatch.setattr(auth, "_smtp_configured", lambda: True)
    monkeypatch.setattr(auth, "_export_smtp_settings", lambda: None)
    monkeypatch.setattr(auth, "_run_in_background", lambda fn: fn())
    return calls


def test_code_is_emailed_and_never_logged_when_smtp_is_configured(monkeypatch, caplog):
    calls = _capture_email(monkeypatch)
    caplog.set_level(logging.DEBUG)

    _REAL_DELIVER_CODE(EMAIL, "verification", "654321")

    assert len(calls) == 1
    assert calls[0].recipient == EMAIL
    assert "654321" in calls[0].text and "654321" in calls[0].html
    assert "654321" not in caplog.text


def test_production_without_smtp_logs_an_error_without_the_code(monkeypatch, caplog):
    monkeypatch.delenv("SMTP_HOST", raising=False)
    monkeypatch.delenv("SMTP_USER", raising=False)
    monkeypatch.setattr(auth, "settings", SimpleNamespace(production=True, SMTP_HOST="", SMTP_USER=""))
    caplog.set_level(logging.DEBUG)

    _REAL_DELIVER_CODE(EMAIL, "password reset", "654321")

    assert "654321" not in caplog.text
    assert any(r.levelno >= logging.ERROR and "SMTP" in r.getMessage() for r in caplog.records)


def test_non_production_without_smtp_logs_the_code_for_local_development(monkeypatch, caplog):
    monkeypatch.delenv("SMTP_HOST", raising=False)
    monkeypatch.delenv("SMTP_USER", raising=False)
    monkeypatch.setattr(auth, "settings", SimpleNamespace(production=False, SMTP_HOST="", SMTP_USER=""))
    caplog.set_level(logging.INFO)

    _REAL_DELIVER_CODE(EMAIL, "verification", "654321")

    assert "654321" in caplog.text


def test_smtp_connection_has_a_timeout(monkeypatch):
    opened = []

    class FakeSMTP:
        def __init__(self, host, port, **kwargs):
            opened.append((host, port, kwargs))

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def starttls(self):
            pass

        def login(self, user, password):
            pass

        def sendmail(self, sender, recipient, message):
            pass

    monkeypatch.setenv("SMTP_HOST", "smtp.example.test")
    monkeypatch.setenv("SMTP_PORT", "2525")
    monkeypatch.setenv("SMTP_USER", "mailer")
    monkeypatch.setenv("SMTP_PASSWORD", "not-a-real-password")
    monkeypatch.setattr(email_channel.smtplib, "SMTP", FakeSMTP)

    assert EmailChannel.send(EMAIL, "subject", "<p>body</p>", "body") is True
    assert opened == [("smtp.example.test", 2525, {"timeout": 15})]


@pytest.mark.parametrize("smtp_result", [False, RuntimeError("smtp down")])
def test_smtp_failure_does_not_break_the_request_or_leak_the_code(ctx, monkeypatch, caplog, smtp_result):
    _activate(ctx)
    monkeypatch.setattr(auth, "_deliver_code", _REAL_DELIVER_CODE)
    calls = _capture_email(monkeypatch, result=smtp_result)
    caplog.set_level(logging.DEBUG)

    response = _forgot(ctx)

    assert response.status_code == 200
    assert len(calls) == 1
    code = _rows(ctx, "SELECT id FROM password_resets WHERE used_at IS NULL")
    assert len(code) == 1
    assert calls[0].text.split(" is ")[1][:6] not in caplog.text


# --------------------------------------------------------------------------- production secrets

def _strong() -> str:
    return secrets.token_urlsafe(48)


def test_placeholder_and_short_secrets_are_recognised():
    weak = user_config.weak_secret_reason
    for value in ("", None, "changeme_in_production_secret_key",
                  "changeme_in_production_credential_key_32bytes!!!!",
                  "CHANGE_ME_BEFORE_PRODUCTION", "default-engine-key", "short", "k" * 44):
        assert weak(value), value
    assert weak(_strong()) is None
    assert weak(secrets.token_hex(32)) is None


def test_production_refuses_default_secrets():
    settings = Settings.model_construct(APP_ENV="PRODUCTION")

    errors = settings.production_secret_errors()

    assert any(e.startswith("SECRET_KEY ") for e in errors)
    assert any(e.startswith("CREDENTIAL_KEY ") for e in errors)
    with pytest.raises(ValueError, match="SECRET_KEY") as raised:
        settings.assert_production_secrets()
    assert "secrets.token_urlsafe" in str(raised.value)


def test_production_refuses_missing_or_short_secrets():
    for bad in ("", "too-short-to-be-a-key"):
        settings = Settings.model_construct(APP_ENV="PRODUCTION", SECRET_KEY=bad, CREDENTIAL_KEY=_strong())
        with pytest.raises(ValueError, match="SECRET_KEY"):
            settings.assert_production_secrets()


def test_production_accepts_strong_secrets():
    settings = Settings.model_construct(APP_ENV="PRODUCTION", SECRET_KEY=_strong(), CREDENTIAL_KEY=_strong())

    assert settings.production_secret_errors() == []
    settings.assert_production_secrets()


@pytest.mark.parametrize("app_env", ["TEST", "DEVELOPMENT"])
def test_non_production_keeps_working_with_defaults(app_env):
    settings = Settings.model_construct(APP_ENV=app_env)

    assert settings.production_secret_errors() == []
    settings.assert_production_secrets()


# --------------------------------------------------------------------------- admin script

def _load_create_admin():
    path = ROOT / "backends" / "user-backend" / "scripts" / "create_admin.py"
    spec = importlib.util.spec_from_file_location("create_admin_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_create_admin_script_enforces_password_policy(monkeypatch):
    script = _load_create_admin()

    assert script.password_problem("short-pass") is not None       # under 12 characters
    assert script.password_problem("x" * 73) is not None           # over the bcrypt limit
    assert script.password_problem("twelve chars") is None

    monkeypatch.setenv(script.PASSWORD_ENV, "too short")
    with pytest.raises(SystemExit):
        script.read_password()
    monkeypatch.setenv(script.PASSWORD_ENV, ADMIN_PASSWORD)
    assert script.read_password() == ADMIN_PASSWORD


def test_create_admin_script_creates_a_working_admin_without_printing_the_password(ctx, monkeypatch, capsys):
    script = _load_create_admin()
    monkeypatch.setenv(script.PASSWORD_ENV, ADMIN_PASSWORD)
    monkeypatch.setattr(script, "_load_env", lambda: None)
    monkeypatch.setattr("shared_lib.persistence.db.DB", lambda *a, **k: ctx.db)

    assert script.main([ADMIN_EMAIL.upper()]) == 0
    assert _admin_login(ctx).status_code == 200

    # An existing admin is left alone unless a reset is asked for explicitly.
    monkeypatch.setenv(script.PASSWORD_ENV, "another admin pass phrase")
    assert script.main([ADMIN_EMAIL]) == 1
    assert _admin_login(ctx).status_code == 200
    assert script.main([ADMIN_EMAIL, "--reset-password"]) == 0
    assert _admin_login(ctx).status_code == 400
    assert _admin_login(ctx, password="another admin pass phrase").status_code == 200

    output = capsys.readouterr()
    assert ADMIN_PASSWORD not in output.out + output.err
    assert "another admin pass phrase" not in output.out + output.err
