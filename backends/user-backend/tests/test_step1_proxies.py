"""Step 1.7 -- the user-backend proxies resolve to routes the bot backend serves,
forward the caller's token, deny what the engine would deny, pass upstream
failures through unmasked, and never forward secrets."""
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

from app.api import auto_pilot_proxy, cati_proxy, forex_proxy, monitoring_proxy, proxy_utils  # noqa: E402
from app.api.auth import get_current_active_user  # noqa: E402

USER = {"id": "alice", "email": "alice@example.test", "role": "user", "is_verified": True}
ADMIN = {"id": "admin-1", "email": "ops@example.test", "role": "admin", "is_verified": True}

#: Every route the bot backend actually registers for these proxies (from its routers).
BOT_BACKEND_ROUTES = {
    "/api/v1/monitoring/system/health", "/api/v1/monitoring/system/metrics", "/api/v1/monitoring/bots/overview",
    "/api/v1/monitoring/activity/events", "/api/v1/cati/runtime/status", "/api/v1/cati/bots/{id}/status",
    "/api/v1/cati/bots/{id}/positions", "/api/v1/cati/bots/{id}/trades", "/api/v1/cati/bots/{id}/summary",
    "/api/v1/cati/bots/{id}/equity", "/api/v1/auto-pilot/deploy", "/api/v1/auto-pilot/deployments",
    "/api/v1/auto-pilot/preview", "/api/v1/auto-pilot/bots", "/api/v1/forex/instruments",
}


class FakeUpstream:
    def __init__(self, status_code=200, body=None):
        self.status_code, self.body, self.calls = status_code, {} if body is None else body, []

    async def request(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(status_code=self.status_code, content=json.dumps(self.body).encode(),
                               headers={"content-type": "application/json", "content-length": "1"})


@pytest.fixture
def upstream(monkeypatch):
    fake = FakeUpstream()
    monkeypatch.setattr(proxy_utils, "_proxy_client", fake)
    monkeypatch.setattr(proxy_utils, "BOT_BACKEND_BASE_URL", "http://bot.test")
    return fake


def client(router, user=USER, prefix=""):
    app = FastAPI()
    app.include_router(router, prefix=prefix)
    app.dependency_overrides[get_current_active_user] = lambda: dict(user)
    return TestClient(app)


def upstream_path(call):
    url = call["url"]
    return url.replace("http://bot.test", "")


def normalised(path):
    parts = path.split("/")
    return "/".join("{id}" if (i > 0 and parts[i - 1] == "bots" and p not in ("overview", "executions")) else p
                    for i, p in enumerate(parts))


# ── monitoring ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("path", list(monitoring_proxy.UPSTREAM))
def test_monitoring_routes_resolve_to_served_paths_and_forward_the_token(upstream, path):
    c = client(monitoring_proxy.router, ADMIN)
    response = c.get(f"/api/v1/monitoring{path}", headers={"Authorization": "Bearer admin-token"})
    assert response.status_code == 200, response.text
    [call] = upstream.calls
    assert upstream_path(call).split("?")[0] == monitoring_proxy.UPSTREAM[path]
    assert monitoring_proxy.UPSTREAM[path] in BOT_BACKEND_ROUTES
    assert call["headers"]["Authorization"] == "Bearer admin-token"


def test_monitoring_denies_non_admins_before_forwarding(upstream):
    c = client(monitoring_proxy.router, USER)
    for path in monitoring_proxy.UPSTREAM:
        response = c.get(f"/api/v1/monitoring{path}", headers={"Authorization": "Bearer user-token"})
        assert response.status_code == 403, path
    assert upstream.calls == []


def test_an_internal_backend_failure_is_not_masked_as_success(monkeypatch):
    fake = FakeUpstream(status_code=500, body={"detail": "engine exploded"})
    monkeypatch.setattr(proxy_utils, "_proxy_client", fake)
    monkeypatch.setattr(proxy_utils, "BOT_BACKEND_BASE_URL", "http://bot.test")
    c = client(monitoring_proxy.router, ADMIN)
    response = c.get("/api/v1/monitoring/bots-overview", headers={"Authorization": "Bearer t"})
    assert response.status_code == 500 and response.json()["detail"] == "engine exploded"


def test_a_connection_failure_is_a_502(monkeypatch):
    class Down:
        async def request(self, **kwargs):
            raise httpx.ConnectError("refused")
    monkeypatch.setattr(proxy_utils, "_proxy_client", Down())
    monkeypatch.setattr(proxy_utils, "BOT_BACKEND_BASE_URL", "http://bot.test")
    c = client(monitoring_proxy.router, ADMIN)
    assert c.get("/api/v1/monitoring/system-health", headers={"Authorization": "Bearer t"}).status_code == 502


# ── cati read model ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("path", ["/runtime/status", "/bots/bot-1/status", "/bots/bot-1/positions", "/bots/bot-1/trades",
                                  "/bots/bot-1/summary", "/bots/bot-1/equity"])
def test_cati_routes_resolve_and_keep_query_and_token(upstream, path):
    c = client(cati_proxy.router)
    response = c.get(f"/api/v1/cati{path}", params={"page": 2}, headers={"Authorization": "Bearer user-token"})
    assert response.status_code == 200
    [call] = upstream.calls
    target = upstream_path(call).split("?")[0]
    assert target == f"/api/v1/cati{path}" and normalised(target) in BOT_BACKEND_ROUTES
    assert call["headers"]["Authorization"] == "Bearer user-token" and call.get("params", {}).get("page") in (2, "2")


def test_cati_routes_require_a_user():
    app = FastAPI()
    app.include_router(cati_proxy.router)
    assert TestClient(app).get("/api/v1/cati/runtime/status").status_code == 401


# ── auto-pilot: both contracts on one customer path ─────────────────────────

def test_the_step_one_body_goes_to_deployments_and_the_legacy_body_to_deploy(upstream, monkeypatch):
    monkeypatch.setattr("app.core.broker_service.get_decrypted_credentials", lambda user_id, acct: {"broker_id": "binance"})
    c = client(auto_pilot_proxy.router, prefix="/api/v1/auto-pilot")
    new = {"broker_account_id": "acct-1", "budget": {"type": "fixed_amount", "value": "1000"}, "risk_level": "balanced",
           "risk_acknowledged": True, "request_id": "req-00000001"}
    assert c.post("/api/v1/auto-pilot/deploy", json=new, headers={"Authorization": "Bearer t"}).status_code == 200
    assert upstream_path(upstream.calls[-1]).split("?")[0] == "/api/v1/auto-pilot/deployments"
    assert upstream.calls[-1]["json"]["request_id"] == "req-00000001" and "api_key" not in json.dumps(upstream.calls[-1]["json"])
    legacy = {"broker_account_ids": ["acct-1"], "risk_mode": "medium", "execution_mode": "paper",
              "allocation": {"total_capital_budget": 500, "trade_amount_per_position": 120, "allocation_type": "fixed_amount"}}
    assert c.post("/api/v1/auto-pilot/deploy", json=legacy, headers={"Authorization": "Bearer t"}).status_code == 200
    assert upstream_path(upstream.calls[-1]).split("?")[0] == "/api/v1/auto-pilot/deploy"
    assert upstream.calls[-1]["json"]["risk_mode"] == "medium"
    assert c.post("/api/v1/auto-pilot/preview", json=new, headers={"Authorization": "Bearer t"}).status_code == 200
    assert upstream_path(upstream.calls[-1]).split("?")[0] == "/api/v1/auto-pilot/preview"
    assert c.get("/api/v1/auto-pilot/bots", headers={"Authorization": "Bearer t"}).status_code == 200
    assert upstream_path(upstream.calls[-1]).split("?")[0] == "/api/v1/auto-pilot/bots"


def test_the_step_one_body_is_validated_and_ownership_checked_before_forwarding(upstream, monkeypatch):
    monkeypatch.setattr("app.core.broker_service.get_decrypted_credentials", lambda user_id, acct: None)   # not the user's account
    c = client(auto_pilot_proxy.router, prefix="/api/v1/auto-pilot")
    body = {"broker_account_id": "someone-elses", "budget": {"type": "fixed_amount", "value": "1000"}, "risk_level": "balanced",
            "risk_acknowledged": True, "request_id": "req-00000002"}
    response = c.post("/api/v1/auto-pilot/deploy", json=body, headers={"Authorization": "Bearer t"})
    assert response.status_code == 422 and response.json()["detail"]["blockers"][0]["code"] == "ACCOUNT_NOT_CONNECTED"
    bad = {**body, "environment": "live"}                               # the client cannot name an environment
    assert c.post("/api/v1/auto-pilot/preview", json=bad, headers={"Authorization": "Bearer t"}).status_code == 422
    assert upstream.calls == []


# ── forex: no secrets over the wire ─────────────────────────────────────────

def test_forex_instruments_proxy_forwards_the_account_id_not_the_credentials(upstream, monkeypatch):
    monkeypatch.setattr(forex_proxy, "get_decrypted_credentials",
                        lambda user_id, acct: {"broker_id": "oanda", "api_key": "SECRET-KEY", "account_id": "001"})
    c = client(forex_proxy.router, prefix="/api/v1/forex")
    response = c.get("/api/v1/forex/instruments", params={"broker_account_id": "acct-1"}, headers={"Authorization": "Bearer t"})
    assert response.status_code == 200
    [call] = upstream.calls
    assert upstream_path(call).split("?")[0] == "/api/v1/forex/instruments"
    assert call.get("json") is None and "SECRET-KEY" not in json.dumps(call, default=str)
    assert call["params"]["broker_account_id"] == "acct-1" and call["params"]["broker_id"] == "oanda"


def test_every_proxy_reads_the_bot_backend_url_from_one_place():
    import importlib
    for name in ("analytics_proxy", "analytics_reporting", "events_proxy", "reports_proxy", "risk_profiles_proxy", "monitoring_proxy"):
        module = importlib.import_module(f"app.api.{name}")
        assert getattr(module, "BOT_BACKEND_URL", proxy_utils.BOT_BACKEND_BASE_URL) == proxy_utils.BOT_BACKEND_BASE_URL, name
