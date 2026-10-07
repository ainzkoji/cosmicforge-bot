"""Admin emergency proxy: /api/admin/emergency/* -> bot-backend /api/v1/admin/emergency/*.

* admin only (an ordinary user token and no token are both rejected, and
  nothing is sent upstream);
* the engine's answer reaches the operator unchanged -- status code and body;
* flatten gets a long timeout; a timeout is reported as "outcome unknown";
* the upstream call carries a token the bot-backend accepts for an admin.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import admin_emergency, proxy_utils  # noqa: E402
from app.core.deps import require_admin  # noqa: E402
from app.core.security import create_access_token, decode_admin_token, decode_token  # noqa: E402

ADMIN = {"id": "admin-test", "role": "admin", "is_active": 1}
FLATTEN = {"scope": "all", "account_id": None, "confirm": "FLATTEN", "reason": "incident drill"}


class FakeUpstream:
    """Stands in for the pooled httpx client; records every outbound call."""

    def __init__(self, status_code=200, body=None, raises=None):
        self.status_code = status_code
        self.body = {} if body is None else body
        self.raises = raises
        self.calls = []

    async def request(self, **kwargs):
        self.calls.append(kwargs)
        if self.raises is not None:
            raise self.raises
        return SimpleNamespace(
            status_code=self.status_code,
            content=json.dumps(self.body).encode(),
            headers={"content-type": "application/json", "content-length": "1"},
        )


def _client(monkeypatch, upstream: FakeUpstream, *, admin: bool = True) -> TestClient:
    monkeypatch.setattr(proxy_utils, "_proxy_client", upstream)
    monkeypatch.setattr(proxy_utils, "BOT_BACKEND_BASE_URL", "http://bot.test")
    # Durable audit rows are captured here instead of being written to a database.
    upstream.audit = []
    monkeypatch.setattr(admin_emergency, "_write_audit",
                        lambda admin, action, details: upstream.audit.append((admin["id"], action, details)))
    app = FastAPI()
    app.include_router(admin_emergency.router, prefix="/api")
    if admin:
        app.dependency_overrides[require_admin] = lambda: dict(ADMIN)
    return TestClient(app)


@pytest.mark.parametrize("method,path,body", [
    ("get", "/status", None),
    ("post", "/kill-switch", {"enabled": True, "reason": "drill"}),
    ("post", "/flatten", FLATTEN),
])
def test_non_admin_is_rejected_and_nothing_is_forwarded(monkeypatch, method, path, body):
    upstream = FakeUpstream()
    client = _client(monkeypatch, upstream, admin=False)
    user_token = create_access_token("normal-user", role="user")
    kwargs = {} if body is None else {"json": body}

    unauthenticated = getattr(client, method)(f"/api/admin/emergency{path}", **kwargs)
    normal_user = getattr(client, method)(
        f"/api/admin/emergency{path}", headers={"Authorization": f"Bearer {user_token}"}, **kwargs)

    assert unauthenticated.status_code == 401
    assert normal_user.status_code == 401
    assert upstream.calls == []
    assert upstream.audit == []


def test_status_is_forwarded_with_a_token_the_bot_backend_accepts(monkeypatch):
    status = {"kill_switch": {"enabled": False, "reason": None, "set_at": None, "set_by": None},
              "live_order_submission_enabled": False, "demo_order_submission_enabled": True,
              "open_positions": None, "generated_at": "2026-01-01T00:00:00+00:00"}
    upstream = FakeUpstream(200, status)
    client = _client(monkeypatch, upstream)

    response = client.get("/api/admin/emergency/status", headers={"Authorization": "Bearer admin-portal-token"})

    assert response.status_code == 200
    assert response.json() == status
    call = upstream.calls[0]
    assert call["method"] == "GET"
    assert call["url"] == "http://bot.test/api/v1/admin/emergency/status"
    assert call["json"] is None
    scheme, _, token = call["headers"]["Authorization"].partition(" ")
    assert scheme == "Bearer"
    # The admin-portal token is not valid on the bot-backend; a service token
    # naming the verified admin is sent instead.
    assert token != "admin-portal-token"
    claims = decode_token(token)
    assert claims is not None
    assert claims["type"] == "access" and claims["role"] == "admin" and claims["sub"] == ADMIN["id"]
    assert decode_admin_token(token) is None
    # The marker the bot-backend emergency router requires (require_admin_emergency):
    # exactly this ``act`` claim on a token issued for one call (<= 120 s there).
    assert claims["act"] == admin_emergency.EMERGENCY_ACTOR_CLAIM == "admin-emergency"
    assert 0 < claims["exp"] - claims["iat"] <= 120
    assert admin_emergency.SERVICE_TOKEN_TTL_SECONDS <= 120
    # A successful status read is polled every few seconds: log line only, no audit row.
    assert upstream.audit == []


def test_ordinary_access_tokens_never_carry_the_emergency_marker():
    """What an end-user account gets at login -- even one with role=admin -- is not the emergency token."""
    for role in ("user", "admin"):
        claims = decode_token(create_access_token("some-user", role=role))
        assert claims is not None and "act" not in claims


def test_every_mutating_call_is_audited_with_admin_action_and_outcome(monkeypatch):
    upstream = FakeUpstream(200, {"kill_switch": {"enabled": True}})
    client = _client(monkeypatch, upstream)
    client.post("/api/admin/emergency/kill-switch", json={"enabled": True, "reason": "incident drill"})

    upstream.status_code, upstream.body = 502, {"ok": False, "detail": "1 of 1 in-scope close(s) failed", "results": []}
    client.post("/api/admin/emergency/flatten", json=FLATTEN)

    upstream.status_code, upstream.body = 409, {"detail": "engine not running", "reason": "ENGINE_NOT_RUNNING"}
    client.post("/api/admin/emergency/flatten", json={**FLATTEN, "scope": "account", "account_id": "acct-1"})

    # A transport failure is audited too, with the status the operator was given.
    upstream.raises = httpx.ReadTimeout("slow")
    client.post("/api/admin/emergency/flatten", json=FLATTEN)
    # ... and so is a status read that did not succeed.
    client.get("/api/admin/emergency/status")

    assert [(admin_id, action, details["outcome_status"], details["outcome_reason"])
            for admin_id, action, details in upstream.audit] == [
        (ADMIN["id"], "kill_switch", 200, None),
        (ADMIN["id"], "flatten", 502, None),
        (ADMIN["id"], "flatten", 409, "ENGINE_NOT_RUNNING"),
        (ADMIN["id"], "flatten", 504, "UPSTREAM_TIMEOUT"),
        (ADMIN["id"], "status", 504, "UPSTREAM_TIMEOUT"),
    ]
    first, _second, third = (details for _id, _action, details in upstream.audit[:3])
    assert first["admin_id"] == ADMIN["id"] and first["action"] == "kill_switch"
    assert first["request"] == {"enabled": True, "reason": "incident drill"}
    assert third["request"] == {"scope": "account", "account_id": "acct-1", "reason": "incident drill"}
    assert "confirm" not in third["request"]


def test_a_failing_audit_store_never_changes_the_answer(monkeypatch):
    upstream = FakeUpstream(200, {"kill_switch": {"enabled": True}})
    client = _client(monkeypatch, upstream)

    def broken(admin, action, details):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(admin_emergency, "_write_audit", broken)
    response = client.post("/api/admin/emergency/kill-switch", json={"enabled": True, "reason": "incident drill"})
    assert response.status_code == 200 and response.json() == {"kill_switch": {"enabled": True}}


def test_kill_switch_body_is_forwarded_unchanged(monkeypatch):
    upstream = FakeUpstream(200, {"kill_switch": {"enabled": True}})
    client = _client(monkeypatch, upstream)
    body = {"enabled": True, "reason": "incident drill"}

    response = client.post("/api/admin/emergency/kill-switch", json=body)

    assert response.status_code == 200
    call = upstream.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == "http://bot.test/api/v1/admin/emergency/kill-switch"
    assert call["json"] == body
    assert call["timeout"] == admin_emergency.KILL_SWITCH_TIMEOUT_SECONDS


def test_flatten_409_status_and_body_pass_through_with_long_timeout(monkeypatch):
    refusal = {"detail": "The production engine in this process cannot act on a broker (ENGINE_NOT_RUNNING); "
                         "NO position was closed. The kill switch is on (no new entries).",
               "reason": "ENGINE_NOT_RUNNING", "ok": False, "kill_switch_enabled": True, "results": []}
    upstream = FakeUpstream(409, refusal)
    client = _client(monkeypatch, upstream)

    response = client.post("/api/admin/emergency/flatten", json=FLATTEN)

    assert response.status_code == 409
    assert response.json() == refusal
    call = upstream.calls[0]
    assert call["url"] == "http://bot.test/api/v1/admin/emergency/flatten"
    assert call["json"] == FLATTEN
    assert call["timeout"] == 120.0


@pytest.mark.parametrize("status_code,body", [
    (400, {"detail": 'confirm must be exactly "FLATTEN".', "reason": "CONFIRMATION_REQUIRED"}),
    (502, {"ok": False, "kill_switch_enabled": True, "detail": "1 of 1 in-scope close(s) failed",
           "results": [{"account_id": "a1", "symbol": "BTCUSDT", "status": "failed", "detail": "x"}]}),
    (503, {"detail": "The kill switch could not be set, so NO position was closed.",
           "reason": "KILL_SWITCH_NOT_PERSISTED", "ok": False, "kill_switch_enabled": False, "results": []}),
])
def test_other_upstream_failures_reach_the_operator_unchanged(monkeypatch, status_code, body):
    upstream = FakeUpstream(status_code, body)
    client = _client(monkeypatch, upstream)

    response = client.post("/api/admin/emergency/flatten", json=FLATTEN)

    assert response.status_code == status_code
    assert response.json() == body


def test_flatten_timeout_is_reported_as_unknown_outcome_never_success(monkeypatch):
    upstream = FakeUpstream(raises=httpx.ReadTimeout("slow"))
    client = _client(monkeypatch, upstream)

    response = client.post("/api/admin/emergency/flatten", json=FLATTEN)

    assert response.status_code == 504
    payload = response.json()
    assert payload["reason"] == "UPSTREAM_TIMEOUT"
    assert "UNKNOWN" in payload["detail"]
    assert "ok" not in payload


def test_unreachable_bot_backend_is_a_502(monkeypatch):
    upstream = FakeUpstream(raises=httpx.ConnectError("refused"))
    client = _client(monkeypatch, upstream)

    response = client.get("/api/admin/emergency/status")

    assert response.status_code == 502
    assert response.json()["reason"] == "UPSTREAM_UNREACHABLE"


@pytest.mark.parametrize("error", [
    httpx.ConnectError("connection refused"),
    httpx.ConnectTimeout("connect timed out"),      # a timeout, but the request never left
    httpx.PoolTimeout("no connection available"),
])
def test_request_that_was_never_sent_is_reported_as_nothing_done(monkeypatch, error):
    upstream = FakeUpstream(raises=error)
    client = _client(monkeypatch, upstream)

    response = client.post("/api/admin/emergency/flatten", json=FLATTEN)

    assert response.status_code == 502
    payload = response.json()
    assert payload["reason"] == "UPSTREAM_UNREACHABLE"
    assert "unreachable" in payload["detail"] and "nothing was done" in payload["detail"]
    assert "UNKNOWN" not in payload["detail"] and "ok" not in payload


@pytest.mark.parametrize("error,reason", [
    (httpx.ReadTimeout("slow"), "UPSTREAM_TIMEOUT"),
    (httpx.WriteTimeout("slow write"), "UPSTREAM_TIMEOUT"),
    (httpx.ReadError("connection reset by peer"), "UPSTREAM_OUTCOME_UNKNOWN"),
    (httpx.RemoteProtocolError("server disconnected without sending a response"), "UPSTREAM_OUTCOME_UNKNOWN"),
])
def test_failure_after_sending_is_an_unknown_outcome_never_nothing_done(monkeypatch, error, reason):
    """Once the request may have reached the engine, 'not confirmed' must not read as 'not done'."""
    upstream = FakeUpstream(raises=error)
    client = _client(monkeypatch, upstream)

    response = client.post("/api/admin/emergency/flatten", json=FLATTEN)

    assert response.status_code == 504
    payload = response.json()
    assert payload["reason"] == reason
    assert "UNKNOWN" in payload["detail"] and "check positions" in payload["detail"]
    assert "nothing was done" not in payload["detail"] and "ok" not in payload


def test_upstream_401_does_not_log_the_admin_out(monkeypatch):
    upstream = FakeUpstream(401, {"detail": "Invalid token"})
    client = _client(monkeypatch, upstream)

    response = client.get("/api/admin/emergency/status")

    assert response.status_code == 502
    assert response.json()["reason"] == "UPSTREAM_AUTH_REJECTED"
