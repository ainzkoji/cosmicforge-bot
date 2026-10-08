"""Step 1.10 -- the onboarding wizard and its service speak one vocabulary.

The requests below are exactly what the portal's wizard sends (step names and
payload keys from ``OnboardingWizard.tsx`` and its step components). Before
Step 1.10 every one of them after ``welcome`` was rejected. Completing the
wizard pre-fills the deployment screen and deploys nothing.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import onboarding  # noqa: E402
from app.api.auth import get_current_active_user  # noqa: E402
from app.core import onboarding_service as service  # noqa: E402
from shared_lib import risk_levels  # noqa: E402
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402

NOW = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc).isoformat()

#: step name and payload, in the order and shape the wizard submits them
WIZARD = [
    ("welcome", {}),
    ("experience_level", {"experience_level": "intermediate"}),
    ("risk_tolerance", {"risk_tolerance": "medium"}),
    ("strategy_preference", {"strategy_preference": "cati"}),
    ("capital_allocation", {"capital_allocation": 1000, "allocation_type": "fixed_amount", "allocation_value": 100}),
]
NEXT = ["experience_level", "risk_tolerance", "strategy_preference", "capital_allocation", "summary"]


@pytest.fixture
def db(tmp_path, monkeypatch):
    database = DB(str(tmp_path / "onboarding.db"))
    migrate(database)
    with database.connect() as c:
        for uid in ("alice", "bob"):
            c.execute("INSERT INTO users (id,email,hashed_password,status,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                      (uid, f"{uid}@example.test", "x", "active", NOW, NOW))
    monkeypatch.setattr(service, "DB", lambda: database)
    return database


def client(user="alice"):
    app = FastAPI()
    app.include_router(onboarding.router, prefix="/api/onboarding")
    app.dependency_overrides[get_current_active_user] = lambda: {"id": user, "email": f"{user}@example.test"}
    return TestClient(app)


def run_wizard(c, steps=WIZARD):
    out = None
    for step, data in steps:
        response = c.post("/api/onboarding/step", json={"step": step, "data": data})
        assert response.status_code == 200, (step, response.text)
        out = response.json()
    return out


def counts(db):
    with db.connect() as c:
        return {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("bot_instances", "deployment_consents")}


def test_every_wizard_step_is_accepted_and_names_the_next_step(db):
    c = client()
    assert c.get("/api/onboarding/state").json()["current_step"] == "welcome"
    for (step, data), expected in zip(WIZARD, NEXT):
        body = c.post("/api/onboarding/step", json={"step": step, "data": data}).json()
        assert body["saved"] is True and body["step"] == step and body["current_step"] == expected, body
        assert body["status"] == "in_progress"
    state = c.get("/api/onboarding/state").json()
    assert state["current_step"] == "summary"
    assert state["steps_completed"] == ["welcome", "experience_level", "risk_tolerance", "strategy_preference", "capital_allocation"]
    assert state["data"]["experience_level"] == "intermediate" and state["data"]["risk_tolerance"] == "medium"
    assert state["data"]["strategy_preference"] == "cati" and state["data"]["strategy_id"] == "cati"
    assert state["data"]["capital_allocation"] == 1000 and state["data"]["allocation_type"] == "fixed_amount"
    assert state["data"]["allocation_value"] == 100


@pytest.mark.parametrize("tolerance,level", [("low", "conservative"), ("medium", "balanced"), ("high", "aggressive")])
def test_risk_appetite_maps_to_the_shared_risk_profile(db, tolerance, level):
    c = client()
    steps = [s if s[0] != "risk_tolerance" else ("risk_tolerance", {"risk_tolerance": tolerance}) for s in WIZARD]
    state = run_wizard(c, steps)
    assert state["data"]["risk_level"] == level
    assert state["deployment_prefill"] == {"risk_level": level, "risk_profile_version": risk_levels.RISK_PROFILE_VERSION,
                                           "budget_type": "fixed_amount", "budget_value": "1000"}


def test_completing_prefills_the_deployment_and_deploys_nothing(db):
    c = client()
    before = counts(db)
    run_wizard(c)
    response = c.post("/api/onboarding/complete")
    assert response.status_code == 200, response.text
    blueprint = response.json()
    assert blueprint["strategy_id"] == "cati" and blueprint["risk_level"] == "balanced"
    assert blueprint["deployment_prefill"] == {"risk_level": "balanced", "risk_profile_version": risk_levels.RISK_PROFILE_VERSION,
                                               "budget_type": "fixed_amount", "budget_value": "1000"}
    assert blueprint["allocation_type"] == "fixed_amount" and blueprint["allocation_value"] == 100
    # never more leverage than the profile the deployment will use
    assert blueprint["risk_policy"]["max_leverage"] <= int(risk_levels.get_profile("balanced").leverage_ceiling)
    assert counts(db) == before == {"bot_instances": 0, "deployment_consents": 0}
    state = c.get("/api/onboarding/state").json()
    assert state["status"] == "completed" and state["recommended_setup"]["deployment_prefill"]["risk_level"] == "balanced"


def test_the_high_appetite_preset_never_advertises_more_leverage_than_its_profile(db):
    c = client()
    steps = [("welcome", {}), ("experience_level", {"experience_level": "advanced"}), ("risk_tolerance", {"risk_tolerance": "high"}),
             ("strategy_preference", {"strategy_preference": "cati"}),
             ("capital_allocation", {"capital_allocation": 5000, "allocation_type": "percent_balance", "allocation_value": 10})]
    state = run_wizard(c, steps)
    assert state["data"]["allocation_type"] == "percent_balance" and state["data"]["allocation_model"] == "percentage"
    blueprint = c.post("/api/onboarding/complete").json()
    assert blueprint["risk_policy"]["max_leverage"] == int(risk_levels.get_profile("aggressive").leverage_ceiling)
    assert blueprint["allocation_type"] == "percentage" and blueprint["allocation_value"] == 10


def test_the_earlier_step_names_and_payloads_are_still_accepted(db):
    c = client()
    legacy = [("welcome", {"accepted_terms": True}), ("experience", {"experience_level": "beginner"}), ("risk", {"risk_tolerance": "low"}),
              ("strategy", {"strategy_id": "cati"}), ("allocation", {"amount": 250.0, "type": "fixed_amount"})]
    state = run_wizard(c, legacy)
    assert state["current_step"] == "summary" and state["step"] == "capital_allocation"
    assert state["data"]["capital_allocation"] == 250 and state["deployment_prefill"]["budget_value"] == "250"


@pytest.mark.parametrize("step,data", [
    ("experience_level", {"experience_level": "wizard"}),
    ("risk_tolerance", {"risk_tolerance": "yolo"}),
    ("risk_tolerance", {}),
    ("strategy_preference", {"strategy_preference": "legacy_momentum"}),
    ("strategy_preference", {}),
    ("capital_allocation", {"capital_allocation": 1000, "allocation_type": "fixed_amount", "allocation_value": 0}),
    ("capital_allocation", {"capital_allocation": 1000, "allocation_type": "fixed_amount", "allocation_value": -5}),
    ("capital_allocation", {"capital_allocation": 100, "allocation_type": "fixed_amount", "allocation_value": 500}),
    ("capital_allocation", {"capital_allocation": 1000, "allocation_type": "percent_balance", "allocation_value": 150}),
    ("capital_allocation", {"capital_allocation": 1000, "allocation_type": "martingale", "allocation_value": 10}),
    ("capital_allocation", {"capital_allocation": "abc", "allocation_type": "fixed_amount", "allocation_value": 10}),
])
def test_invalid_answers_are_refused_and_nothing_is_saved(db, step, data):
    c = client()
    response = c.post("/api/onboarding/step", json={"step": step, "data": data})
    assert response.status_code == 400, response.text
    state = c.get("/api/onboarding/state").json()
    assert state["current_step"] == "welcome" and state["status"] == "not_started"


def test_non_finite_amounts_are_refused(db):
    with pytest.raises(ValueError):
        service.update_onboarding_step("alice", "capital_allocation",
                                       {"capital_allocation": float("inf"), "allocation_type": "fixed_amount", "allocation_value": 10})
    with pytest.raises(ValueError):
        service.update_onboarding_step("alice", "capital_allocation",
                                       {"capital_allocation": 100, "allocation_type": "fixed_amount", "allocation_value": float("nan")})


def test_an_unknown_step_name_is_rejected(db):
    assert client().post("/api/onboarding/step", json={"step": "leverage", "data": {}}).status_code == 422
    with pytest.raises(ValueError):
        service.canonical_step("leverage")


def test_one_users_answers_never_reach_another(db):
    run_wizard(client("alice"))
    other = client("bob").get("/api/onboarding/state").json()
    assert other["current_step"] == "welcome" and other["deployment_prefill"] is None
    assert not other["data"].get("risk_tolerance")


def test_a_row_saved_under_an_earlier_step_name_is_still_readable(db):
    with db.connect() as c:
        c.execute("INSERT INTO onboarding_profiles (user_id,status,current_step,data_json,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                  ("alice", "in_progress", "risk", '{"experience_level": "beginner", "risk_tolerance": "high"}', NOW, NOW))
        c.execute("INSERT INTO onboarding_profiles (user_id,status,current_step,data_json,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                  ("bob", "in_progress", "something_removed", "{}", NOW, NOW))
    state = client("alice").get("/api/onboarding/state").json()
    assert state["current_step"] == "risk_tolerance" and state["deployment_prefill"]["risk_level"] == "aggressive"
    assert client("bob").get("/api/onboarding/state").json()["current_step"] == "welcome"
