"""Push-token registration is authenticated and bound to the caller; the
Telegram webhook verifies its secret header.

Run: cd backends/user-backend && python -m pytest tests/test_notifications_auth.py
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from shared_lib.persistence.db import DB, utc_now_iso  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

ATTACKER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
VICTIM = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
TOKEN_1 = "fcm-token-one-" + "1" * 120
TOKEN_2 = "fcm-token-two-" + "2" * 120
WEBHOOK_SECRET = "telegram-webhook-secret-for-tests"


def _build(tmp_path, monkeypatch, *, authenticated: bool):
    monkeypatch.setenv("APP_ENV", "TEST")
    monkeypatch.setenv("ENVIRONMENT_NAME", "test")
    monkeypatch.setenv("DATABASE_ROLE", "test")
    monkeypatch.delenv("TELEGRAM_WEBHOOK_SECRET", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)

    db_path = tmp_path / "notifications.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    migrate(str(db_path))
    db = DB(path=str(db_path))

    # Imported only now: the module opens its database at import time.
    from app.api import notifications

    monkeypatch.setattr(notifications, "db", db)
    # Never talk to Telegram from a test.
    sent = []
    monkeypatch.setattr(notifications, "_send_telegram_message", lambda chat_id, text: sent.append((chat_id, text)))

    state = {"user_id": ATTACKER}
    app = FastAPI()
    app.include_router(notifications.router, prefix="/api/notifications")
    if authenticated:
        app.dependency_overrides[notifications.get_current_active_user] = lambda: {
            "id": state["user_id"], "status": "active",
        }
    return SimpleNamespace(client=TestClient(app), db=db, state=state, notifications=notifications, sent=sent)


@pytest.fixture
def env(tmp_path, monkeypatch):
    return _build(tmp_path, monkeypatch, authenticated=True)


@pytest.fixture
def anonymous(tmp_path, monkeypatch):
    return _build(tmp_path, monkeypatch, authenticated=False)


def _push_rows(db):
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT user_id, recipient, status FROM notification_endpoints WHERE channel = 'push' ORDER BY user_id"
        ).fetchall()
    return [(row["user_id"], row["recipient"], row["status"]) for row in rows]


def _seed_push(db, user_id, token):
    now = utc_now_iso()
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO notification_endpoints (user_id, channel, recipient, status, verified_at, created_at) "
            "VALUES (?, 'push', ?, 'active', ?, ?)",
            (user_id, token, now, now),
        )


# ---------------------------------------------------------------------------
# Push token registration
# ---------------------------------------------------------------------------

def test_token_registration_requires_login(anonymous):
    _seed_push(anonymous.db, VICTIM, TOKEN_1)

    # The old hijack: no credentials, victim's id in the body, attacker's device token.
    response = anonymous.client.post("/api/notifications/token", json={"userId": VICTIM, "fcmToken": TOKEN_2})
    assert response.status_code == 401
    response = anonymous.client.post("/api/notifications/push/register", json={"token": TOKEN_2})
    assert response.status_code == 401

    assert _push_rows(anonymous.db) == [(VICTIM, TOKEN_1, "active")]


def test_body_user_id_is_ignored(env):
    _seed_push(env.db, VICTIM, TOKEN_1)

    response = env.client.post("/api/notifications/token", json={"userId": VICTIM, "fcmToken": TOKEN_2})
    assert response.status_code == 200, response.text
    assert response.json()["userId"] == ATTACKER

    # The victim's endpoint is untouched; the token went to the caller's own account.
    assert _push_rows(env.db) == [(ATTACKER, TOKEN_2, "active"), (VICTIM, TOKEN_1, "active")]


def test_registration_without_user_id_works_and_is_idempotent(env):
    first = env.client.post("/api/notifications/token", json={"fcmToken": TOKEN_1, "deviceName": "Chrome on Linux"})
    assert first.status_code == 200, first.text
    assert first.json()["status"] == "registered"
    second = env.client.post("/api/notifications/token", json={"fcmToken": TOKEN_1})
    assert second.status_code == 200, second.text
    assert second.json()["status"] == "updated"
    assert _push_rows(env.db) == [(ATTACKER, TOKEN_1, "active")]

    # A new token replaces only the caller's own previous one.
    assert env.client.post("/api/notifications/token", json={"fcmToken": TOKEN_2}).status_code == 200
    assert _push_rows(env.db) == [(ATTACKER, TOKEN_2, "active")]


def test_token_registered_to_another_user_moves_to_the_authenticated_caller(env):
    # Same browser/device: the token was registered while VICTIM was logged in.
    _seed_push(env.db, VICTIM, TOKEN_1)

    env.state["user_id"] = ATTACKER  # now this user is logged in on that device
    response = env.client.post("/api/notifications/token", json={"fcmToken": TOKEN_1})
    assert response.status_code == 200, response.text

    # Exactly one owner: the authenticated caller. Never both, never a third party.
    assert _push_rows(env.db) == [(ATTACKER, TOKEN_1, "active")]


def test_legacy_register_endpoint_binds_to_caller(env):
    response = env.client.post("/api/notifications/push/register", json={"token": TOKEN_1})
    assert response.status_code == 200, response.text
    assert _push_rows(env.db) == [(ATTACKER, TOKEN_1, "active")]


def test_invalid_tokens_are_rejected(env):
    assert env.client.post("/api/notifications/token", json={"fcmToken": "short"}).status_code == 422
    assert env.client.post("/api/notifications/token", json={"userId": VICTIM}).status_code == 422
    assert _push_rows(env.db) == []


def test_test_notification_cannot_target_another_user(env):
    _seed_push(env.db, VICTIM, TOKEN_1)

    # The caller has no device; the victim's device must not be used instead.
    response = env.client.post("/api/notifications/test", json={
        "userId": VICTIM, "title": "Security alert", "body": "Log in at evil.example",
    })
    assert response.status_code == 404
    assert VICTIM not in response.text


# ---------------------------------------------------------------------------
# Telegram webhook
# ---------------------------------------------------------------------------

def _seed_link_code(db, code, user_id):
    created = datetime.now(timezone.utc)
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO telegram_link_codes (code, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (code, user_id, created.isoformat(), (created + timedelta(minutes=10)).isoformat()),
        )


def _telegram_rows(db):
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT user_id, recipient FROM notification_endpoints WHERE channel = 'telegram'"
        ).fetchall()
    return [(row["user_id"], row["recipient"]) for row in rows]


def _update(code, chat_id=4242):
    return {"message": {"chat": {"id": chat_id}, "text": f"/start {code}"}}


def test_telegram_webhook_requires_the_secret_header_when_configured(anonymous, monkeypatch):
    monkeypatch.setenv("TELEGRAM_WEBHOOK_SECRET", WEBHOOK_SECRET)
    _seed_link_code(anonymous.db, "ABC123", VICTIM)
    url = "/api/notifications/telegram/webhook"

    assert anonymous.client.post(url, json=_update("ABC123")).status_code == 403
    wrong = {"X-Telegram-Bot-Api-Secret-Token": "not-the-secret"}
    assert anonymous.client.post(url, json=_update("ABC123"), headers=wrong).status_code == 403
    assert _telegram_rows(anonymous.db) == []  # a forged update linked nothing

    right = {"X-Telegram-Bot-Api-Secret-Token": WEBHOOK_SECRET}
    response = anonymous.client.post(url, json=_update("ABC123"), headers=right)
    assert response.status_code == 200, response.text
    assert _telegram_rows(anonymous.db) == [(VICTIM, "4242")]


def test_telegram_webhook_is_rejected_in_production_without_a_secret(anonymous, monkeypatch):
    monkeypatch.setenv("APP_ENV", "PRODUCTION")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456:placeholder-not-a-real-token")
    _seed_link_code(anonymous.db, "ABC123", VICTIM)

    response = anonymous.client.post("/api/notifications/telegram/webhook", json=_update("ABC123"))
    assert response.status_code == 503
    assert _telegram_rows(anonymous.db) == []
    assert anonymous.sent == []


def test_telegram_webhook_without_secret_still_works_outside_production(anonymous):
    _seed_link_code(anonymous.db, "ABC123", VICTIM)
    response = anonymous.client.post("/api/notifications/telegram/webhook", json=_update("ABC123"))
    assert response.status_code == 200, response.text
    assert _telegram_rows(anonymous.db) == [(VICTIM, "4242")]
