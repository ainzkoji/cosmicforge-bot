"""Step 3.11: promotion, revocation and the kill switches through the administrative interface."""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import family_governance as api_module
from app.core.auth import require_admin_emergency
from app.trading_intelligence.governance.promotion import PromotionGovernance
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

BASE = "/api/v1/admin/governance"
ADMIN = "admin-42"
SHADOW_STAGES = (("M1", ("v2_benchmark_tag", "section9_shadow_verified")), ("M2", ("sections_10_12_shadow_verified",)),
                 ("M3", ("sections_13_17_shadow_verified",)), ("M4", ("section19_shadow_verified",)))


@pytest.fixture
def db(tmp_path):
    database = DB(str(tmp_path / "governance.db"))
    migrate(database)
    return database


@pytest.fixture
def api(db):
    app = FastAPI()
    app.include_router(api_module.router)
    app.dependency_overrides[require_admin_emergency] = lambda: ADMIN
    app.dependency_overrides[api_module.get_db] = lambda: db
    return TestClient(app)


def post(api, path, **body):
    return api.post(f"{BASE}{path}", json=body)


def test_every_route_requires_the_operator_token(db):
    app = FastAPI()
    app.include_router(api_module.router)
    app.dependency_overrides[api_module.get_db] = lambda: db
    client = TestClient(app)
    assert len(api_module.router.routes) == 15
    for route in api_module.router.routes:
        method = sorted(route.methods - {"HEAD"})[0]
        path = route.path.replace("{family_id}", "DAILY_TREND").replace("{request_id}", "r").replace("{account_id}", "a")
        assert client.request(method, path, json={}).status_code in (401, 403), route.path
        assert require_admin_emergency in {d.call for d in route.dependant.dependencies}, route.path


def test_the_registry_is_visible_and_the_real_families_have_no_order_authority(api):
    assert api.get(f"{BASE}/families").json()["families"][0]["registered"] is False
    registered = post(api, "/families/register").json()["events"]
    assert {e["family_id"] for e in registered} >= {"DAILY_TREND", "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1"}
    assert all(e.get("unchanged") for e in post(api, "/families/register").json()["events"])
    body = api.get(f"{BASE}/families").json()
    families = {f["family_id"]: f for f in body["families"]}
    assert body["executable"] == [] and body["log"]["ok"] is True
    daily, residual = families["DAILY_TREND"], families["RESIDUAL_MOMENTUM_PORTFOLIO_TOP1"]
    assert (daily["governance_stage"], daily["certification_status"], daily["executable"]) == ("M0", "NOT_CERTIFIED", False)
    assert daily["allowed_modes"] == ["RESEARCH_SIMULATION", "SHADOW"] and daily["kill_switch_state"] == "OFF"
    assert daily["next_stage_prerequisites"]["stage"] == "M1" and daily["next_stage_prerequisites"]["missing"]
    assert residual["certification_status"] == "FAILED" and "FAMILY_FAILED_RESEARCH" in residual["blockers"]
    assert api.get(f"{BASE}/families/NOPE").status_code == 404
    one = api.get(f"{BASE}/families/DAILY_TREND").json()
    first = one["history"][0]
    assert first["event_type"] == "FAMILY_REGISTERED" and first["actor_ref"].startswith(f"operator:{ADMIN}@")


def test_a_promotion_cannot_be_obtained_by_asking_for_it(api):
    """Request, evidence and approval through the interface. The uncertified family goes no further than the
    research register allows, and every step is in the history with the operator who took it."""
    post(api, "/families/register")
    refused = post(api, "/families/DAILY_TREND/promotion-requests", to_stage="M6", reason="skip ahead")
    assert (refused.status_code, refused.json()["reason"]) == (409, "STAGE_SKIPPED")
    live = post(api, "/families/DAILY_TREND/promotion-requests", to_stage="M7", reason="live now")
    assert live.json()["reason"] == "STAGE_NOT_OPERABLE_HERE"
    request = post(api, "/families/DAILY_TREND/promotion-requests", to_stage="M1", reason="start the shadow stages").json()["event"]
    approval = post(api, f"/promotion-requests/{request['request_id']}/approve", reason="looks fine")
    assert approval.status_code == 409 and approval.json()["reason"] == "PREREQUISITES_NOT_MET"
    assert "EVIDENCE_MISSING:section9_shadow_verified" in approval.json()["detail"]
    # the body cannot carry a prerequisite: an extra field is ignored and the register is asked instead
    forged = post(api, f"/promotion-requests/{request['request_id']}/approve", reason="forced",
                  prerequisites={"certification_status": "CERTIFIED"})
    assert forged.status_code == 409
    for key in SHADOW_STAGES[0][1]:
        assert post(api, "/families/DAILY_TREND/evidence", key=key, reference="docs://shadow-week-1").status_code == 200
    approved = post(api, f"/promotion-requests/{request['request_id']}/approve", reason="shadow evidence reviewed")
    assert approved.status_code == 200 and approved.json()["event"]["payload"]["reviewer"] == ADMIN
    family = api.get(f"{BASE}/families/DAILY_TREND").json()
    assert family["family"]["governance_stage"] == "M1" and family["family"]["executable"] is False
    assert [h["event_type"] for h in family["history"]][-4:] == [
        "PROMOTION_REQUESTED", "EVIDENCE_RECORDED", "EVIDENCE_RECORDED", "PROMOTION_APPROVED"]
    assert post(api, "/families/DAILY_TREND/revoke", reason="back to the start").status_code == 200
    family = api.get(f"{BASE}/families/DAILY_TREND").json()
    assert family["family"]["governance_stage"] == "M0" and family["history"][-1]["event_type"] == "PROMOTION_REVOKED"
    assert post(api, "/promotion-requests/unknown/reject", reason="no such").status_code == 404
    assert api.get(f"{BASE}/history").json()["integrity"]["ok"] is True


def test_daily_trend_stops_at_m5_until_the_research_register_says_it_passed(api):
    post(api, "/families/register")
    family = "DAILY_TREND"
    for stage, keys in SHADOW_STAGES + (("M5", ("certification_run_id", "policy_freeze_hash", "certification_policy_frozen")),):
        for key in keys:
            post(api, f"/families/{family}/evidence", key=key, reference="recorded for the test")
        rid = post(api, f"/families/{family}/promotion-requests", to_stage=stage, reason="next stage").json()["event"]["request_id"]
        assert post(api, f"/promotion-requests/{rid}/approve", reason="reviewed").status_code == 200, stage
    post(api, f"/families/{family}/evidence", key="replay_gates_passed", reference="claimed, not true")
    rid = post(api, f"/families/{family}/promotion-requests", to_stage="M6", reason="demo trading").json()["event"]["request_id"]
    refused = post(api, f"/promotion-requests/{rid}/approve", reason="please")
    assert refused.status_code == 409
    assert "RESEARCH_CERTIFICATION_NOT_CERTIFIED" in refused.json()["detail"] and "ADMISSION_NOT_ELIGIBLE" in refused.json()["detail"]
    view = api.get(f"{BASE}/families/{family}").json()["family"]
    assert (view["governance_stage"], view["executable"]) == ("M5", False) and "DEMO" not in view["allowed_modes"]
    assert view["next_stage_prerequisites"]["stage"] == "M6"


def test_the_failed_family_cannot_pass_the_certification_stage(api):
    post(api, "/families/register")
    family = "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1"
    for stage, keys in SHADOW_STAGES:
        for key in keys:
            post(api, f"/families/{family}/evidence", key=key, reference="claimed")
        rid = post(api, f"/families/{family}/promotion-requests", to_stage=stage, reason="claim").json()["event"]["request_id"]
        assert post(api, f"/promotion-requests/{rid}/approve", reason="shadow stage").status_code == 200
    for key in ("certification_run_id", "policy_freeze_hash", "certification_policy_frozen"):
        post(api, f"/families/{family}/evidence", key=key, reference="claimed")
    rid = post(api, f"/families/{family}/promotion-requests", to_stage="M5", reason="claim").json()["event"]["request_id"]
    refused = post(api, f"/promotion-requests/{rid}/approve", reason="claim")
    assert refused.status_code == 409 and "MANDATE_NOT_VERIFIED" in refused.json()["detail"]
    view = api.get(f"{BASE}/families/{family}").json()["family"]
    assert view["governance_stage"] == "M4" and view["executable"] is False


def test_family_and_account_kill_switches(api, db):
    post(api, "/families/register")
    assert post(api, "/families/DAILY_TREND/kill-switch", enabled=True, reason="incident 7").status_code == 200
    view = api.get(f"{BASE}/families/DAILY_TREND").json()
    assert view["family"]["kill_switch_state"] == "ON" and "FAMILY_KILL_SWITCH" in view["family"]["blockers"]
    assert view["history"][-1]["payload"]["reason"] == "incident 7"
    assert post(api, "/families/DAILY_TREND/kill-switch", enabled=False, reason="resolved").status_code == 200
    assert post(api, "/families/DAILY_TREND/disable", enabled=True, reason="maintenance").status_code == 200
    assert api.get(f"{BASE}/families/DAILY_TREND").json()["family"]["allowed_modes"] == ["RESEARCH_SIMULATION"]
    assert post(api, "/families/NOPE/kill-switch", enabled=True, reason="unknown").status_code == 404
    assert post(api, "/families/DAILY_TREND/kill-switch", enabled=True, reason="x").status_code == 422   # a real reason
    assert post(api, "/accounts/acct-1/kill-switch", enabled=True, reason="customer asked").status_code == 404
    with db.connect() as conn:
        conn.execute("INSERT INTO broker_accounts (id, user_id, broker_id, market_type, label, status, environment, created_at, "
                     "updated_at) VALUES ('acct-1','u1','binance','futures','a','connected','demo',1,1)")
    on = post(api, "/accounts/acct-1/kill-switch", enabled=True, reason="customer asked")
    assert on.status_code == 200 and on.json()["account_kill_switch"]["enabled"] is True
    governance = PromotionGovernance(db)
    assert governance.kill_switch_on(scope="acct-1") and not governance.kill_switch_on(scope="acct-2")
    assert not governance.kill_switch_on()             # one account, not the platform
    assert post(api, "/accounts/GLOBAL/kill-switch", enabled=True, reason="wrong door").status_code == 400


def test_cycles_and_divergences_are_readable(api):
    assert api.get(f"{BASE}/cycles").json() == {"cycles": []}
    assert api.get(f"{BASE}/shadow/divergences").json() == {"totals": {}, "divergences": []}
