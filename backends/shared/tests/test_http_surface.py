"""API documentation exposure and browser origins (Step 1 closure)."""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))

from shared_lib.core.http_surface import (DEVELOPMENT_ORIGINS, api_docs_enabled, browser_origins, docs_kwargs,  # noqa: E402
                                          normalise_origin)

PROD = SimpleNamespace(production=True)
DEV = SimpleNamespace(production=False)


def test_production_serves_no_api_documentation_unless_asked():
    assert api_docs_enabled(PROD, {}) is False
    assert api_docs_enabled(SimpleNamespace(), {}) is False              # no flag at all: treated as production
    assert api_docs_enabled(PROD, {"API_DOCS_ENABLED": "true"}) is True
    assert api_docs_enabled(SimpleNamespace(production=True, API_DOCS_ENABLED=False), {"API_DOCS_ENABLED": "true"}) is False
    assert api_docs_enabled(DEV, {}) is True
    for value in ("0", "false", "no", "", "enabled?"):
        assert api_docs_enabled(PROD, {"API_DOCS_ENABLED": value}) is False, value


@pytest.mark.parametrize("settings,expected", [(PROD, 404), (DEV, 200)])
def test_the_three_documentation_routes_follow_the_switch(settings, expected):
    app = FastAPI(**docs_kwargs(settings, {}))

    @app.get("/health")
    def health():
        return {"status": "ok"}
    client = TestClient(app)
    assert client.get("/health").status_code == 200
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert client.get(path).status_code == expected, path


@pytest.mark.parametrize("value,origin", [
    ("https://app.example.com", "https://app.example.com"), ("https://app.example.com/", "https://app.example.com"),
    ("http://localhost:5173", "http://localhost:5173"), (" https://APP.example.com:8443 ", "https://app.example.com:8443"),
    ("*", None), ("https://*.example.com", None), ("app.example.com", None), ("ftp://app.example.com", None),
    ("https://app.example.com/portal", None), ("https://user:pw@app.example.com", None), ("https://app.example.com?x=1", None),
    ("https://app.example.com:notaport", None), ("", None), (None, None),
])
def test_only_bare_http_origins_are_accepted(value, origin):
    assert normalise_origin(value) == origin


def test_production_allows_exactly_the_configured_public_addresses():
    env = {"FRONTEND_URL": "https://app.example.com", "CORS_ALLOWED_ORIGINS": "https://admin.example.com, *, https://app.example.com/"}
    assert browser_origins(PROD, env) == ["https://app.example.com", "https://admin.example.com"]
    for dev in DEVELOPMENT_ORIGINS:
        assert dev not in browser_origins(PROD, env)


def test_development_keeps_its_local_origins_and_adds_the_configured_one():
    origins = browser_origins(DEV, {"FRONTEND_URL": "http://localhost:15173"})
    assert origins[0] == "http://localhost:15173" and set(DEVELOPMENT_ORIGINS) <= set(origins)


def test_an_unconfigured_production_install_behaves_as_before():
    assert browser_origins(PROD, {}) == list(DEVELOPMENT_ORIGINS)
    assert browser_origins(PROD, {"FRONTEND_URL": "not a url"}) == list(DEVELOPMENT_ORIGINS)


def test_a_settings_attribute_wins_over_the_environment():
    settings = SimpleNamespace(production=True, PUBLIC_APP_URL="https://portal.example.com")
    assert browser_origins(settings, {"PUBLIC_APP_URL": "https://other.example.com"}) == ["https://portal.example.com"]


def test_the_browser_is_answered_only_for_an_allowed_origin():
    app = FastAPI()
    app.add_middleware(CORSMiddleware, allow_origins=browser_origins(PROD, {"FRONTEND_URL": "https://app.example.com"}),
                       allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

    @app.get("/ping")
    def ping():
        return {"ok": True}
    client = TestClient(app)
    allowed = client.get("/ping", headers={"Origin": "https://app.example.com"})
    assert allowed.headers.get("access-control-allow-origin") == "https://app.example.com"
    for origin in ("https://evil.example.com", "http://localhost:5173", "https://app.example.com.evil.test"):
        assert "access-control-allow-origin" not in client.get("/ping", headers={"Origin": origin}).headers, origin
