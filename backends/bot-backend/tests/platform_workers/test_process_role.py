"""Step 5.2: an API-only bot-backend process never runs or controls the trading runtime."""
from __future__ import annotations

import pytest

from app.ops import process_role


def test_default_role_keeps_todays_single_process_behaviour(monkeypatch):
    monkeypatch.delenv("PROCESS_ROLE", raising=False)
    assert process_role.role() == "all" and process_role.runs_trading()
    assert not process_role.refused_in_api_role("POST", "/api/v1/emergency/flatten")


def test_api_role_runs_no_trading_and_refuses_runtime_mutations(monkeypatch):
    monkeypatch.setenv("PROCESS_ROLE", "api")
    assert not process_role.runs_trading()
    assert process_role.refused_in_api_role("POST", "/api/v1/emergency/flatten")
    assert process_role.refused_in_api_role("POST", "/runner/live/start")
    assert not process_role.refused_in_api_role("GET", "/api/v1/emergency/status")
    assert not process_role.refused_in_api_role("POST", "/api/v1/emergencyx")   # prefix match is per segment


def test_trading_role_runs_trading(monkeypatch):
    monkeypatch.setenv("PROCESS_ROLE", "trading")
    assert process_role.runs_trading()


def test_unknown_role_fails_closed(monkeypatch):
    monkeypatch.setenv("PROCESS_ROLE", "everything")
    with pytest.raises(process_role.ProcessRoleInvalid):
        process_role.runs_trading()


def test_api_process_answers_409_for_emergency_routes_and_reports_its_role(monkeypatch):
    from fastapi.testclient import TestClient
    from app.main import app

    monkeypatch.setenv("PROCESS_ROLE", "api")
    client = TestClient(app)  # no context manager: startup hooks do not run
    refused = client.post("/api/v1/emergency/flatten", json={})
    assert refused.status_code == 409 and refused.json()["detail"] == "ROUTE_SERVED_BY_TRADING_PROCESS"
    health = client.get("/health", params={"ready": 1})
    assert health.status_code == 200 and health.json()["process_role"] == "api"
