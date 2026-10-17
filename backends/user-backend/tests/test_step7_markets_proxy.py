"""Step 7.3 -- the market availability proxy forwards to the route the bot backend serves, with the caller's
token, and passes the engine's answer through unchanged."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import markets_proxy, proxy_utils  # noqa: E402
from app.api.auth import get_current_active_user  # noqa: E402

USER = {"id": "alice", "email": "alice@example.test", "role": "user", "is_verified": True}
ANSWER = {"scopes": {"CRYPTO_ONLY": {"available": True, "reasons": []},
                     "CRYPTO_AND_TRADFI": {"available": False, "reasons": [{"code": "NO_TRADFI_MARKET_AVAILABLE"}]}}}


class FakeUpstream:
    def __init__(self):
        self.calls = []

    async def request(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(status_code=200, content=json.dumps(ANSWER).encode(),
                               headers={"content-type": "application/json", "content-length": "1"})


@pytest.fixture
def upstream(monkeypatch):
    fake = FakeUpstream()
    monkeypatch.setattr(proxy_utils, "_proxy_client", fake)
    monkeypatch.setattr(proxy_utils, "BOT_BACKEND_BASE_URL", "http://bot.test")
    return fake


def test_availability_is_proxied_with_the_token(upstream):
    app = FastAPI()
    app.include_router(markets_proxy.router)
    app.dependency_overrides[get_current_active_user] = lambda: dict(USER)
    response = TestClient(app).get("/api/v1/markets/availability", headers={"Authorization": "Bearer t0k"})
    assert response.status_code == 200 and response.json() == ANSWER
    call = upstream.calls[0]
    assert call["url"] == "http://bot.test/api/v1/markets/availability"
    assert call["headers"]["Authorization"] == "Bearer t0k" and call["method"] == "GET"


def test_the_proxy_requires_a_signed_in_user():
    app = FastAPI()
    app.include_router(markets_proxy.router)
    assert TestClient(app).get("/api/v1/markets/availability").status_code in (401, 403)
