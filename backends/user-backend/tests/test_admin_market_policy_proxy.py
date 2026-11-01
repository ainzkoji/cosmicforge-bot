"""Admin market-entry policy proxy: /api/admin/order-spreading/* -> bot-backend /api/v1/admin/order-spreading/*.

Every bot route is proxied and no other; admin only; the service token names the operator; bodies and answers
pass unchanged; writes are audited; a policy id that is not a plain identifier never reaches the engine.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import admin_market_policy, proxy_utils  # noqa: E402
from app.core.deps import require_admin  # noqa: E402
from app.core.security import decode_token  # noqa: E402

ADMIN = {"id": "admin-test", "email": "ops@example.test", "role": "admin", "is_active": 1}
BASE = "/api/admin/order-spreading"
BOT_ROUTER_SOURCE = ROOT / "backends" / "bot-backend" / "app" / "api" / "market_entry_policy.py"
RECORD = {"venue": "BINANCE_USDM", "symbol": "*", "environment": "DEMO", "account_id": "brk_demo",
          "terms": {"absolute_cap_notional": "200"}, "approval_ref": "owner brief", "reason": "experimental demo"}
ROUTES = [
    ("GET", "/market-entry-policies", None, "list"),
    ("GET", "/market-entry-policies/history", None, "history"),
    ("POST", "/market-entry-policies", RECORD, "record"),
    ("POST", "/market-entry-policies/0123456789abcdef/revoke", {"reason": "stop"}, "revoke"),
]


class FakeUpstream:
    def __init__(self, status_code=200, body=None):
        self.status_code, self.body, self.calls, self.audit = status_code, body or {}, [], []

    async def request(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(status_code=self.status_code, content=json.dumps(self.body).encode(),
                               headers={"content-type": "application/json", "content-length": "1"})


def client(monkeypatch, upstream, *, admin=True):
    monkeypatch.setattr(proxy_utils, "_proxy_client", upstream)
    monkeypatch.setattr(proxy_utils, "BOT_BACKEND_BASE_URL", "http://bot.test")
    monkeypatch.setattr(admin_market_policy, "_write_audit",
                        lambda admin, action, details: upstream.audit.append((admin["id"], action, details)))
    app = FastAPI()
    app.include_router(admin_market_policy.router, prefix="/api")
    if admin:
        app.dependency_overrides[require_admin] = lambda: dict(ADMIN)
    return TestClient(app)


def test_every_bot_route_is_proxied_and_no_other():
    bot = set(re.findall(r'@router\.(get|post)\("([^"]+)"', BOT_ROUTER_SOURCE.read_text(encoding="utf-8")))
    ours = {(sorted(r.methods - {"HEAD"})[0].lower(), r.path[len("/admin/order-spreading"):])
            for r in admin_market_policy.router.routes}
    assert ours == bot


def test_admin_only_and_nothing_is_sent_without_an_admin(monkeypatch):
    upstream = FakeUpstream()
    api = client(monkeypatch, upstream, admin=False)
    for method, path, body, _ in ROUTES:
        assert api.request(method, BASE + path, json=body).status_code == 401
    assert upstream.calls == []


def test_calls_carry_the_operator_token_and_pass_through_unchanged(monkeypatch):
    upstream = FakeUpstream(status_code=409, body={"reason": "MARKET_POLICY_ALREADY_ACTIVE", "detail": "x"})
    api = client(monkeypatch, upstream)
    for method, path, body, action in ROUTES:
        res = api.request(method, BASE + path, json=body)
        assert res.status_code == 409 and res.json()["reason"] == "MARKET_POLICY_ALREADY_ACTIVE"
        call = upstream.calls[-1]
        assert call["url"] == "http://bot.test/api/v1/admin/order-spreading" + path
        token = call["headers"]["Authorization"].split(" ", 1)[1]
        claims = decode_token(token)
        assert claims["sub"] == ADMIN["id"] and claims["act"] == "admin-emergency"
        if body is not None:
            assert call["json"] == body
    assert {a for _, a, _ in upstream.audit} == {"list", "history", "record", "revoke"}


def test_a_policy_id_that_is_not_an_identifier_is_refused_here(monkeypatch):
    upstream = FakeUpstream()
    api = client(monkeypatch, upstream)
    res = api.post(BASE + "/market-entry-policies/a%2E%2E/revoke", json={"reason": "x"})
    assert res.status_code in (400, 404) and upstream.calls == []
