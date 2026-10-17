"""Section H Step 8.5: public status endpoint and the admin operations API (alerts, on-call, drill, incidents).

The platform database is a throwaway SQLite file with every platform migration applied; the dialect-level
behaviour (PostgreSQL included) is covered by backends/bot-backend/tests/platform_workers/test_operations_*.py.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "backends" / "shared"))
sys.path.insert(0, str(ROOT / "backends" / "user-backend"))

from app.api import operations  # noqa: E402
from app.core.deps import require_admin  # noqa: E402
from shared_lib.operations import platform as ops_platform  # noqa: E402
from shared_lib.operations.alerting import AlertEngine  # noqa: E402
from shared_lib.persistence.sql import Database, migrate  # noqa: E402
from shared_lib.persistence.sql.platform_schema import all_migrations  # noqa: E402

INTERNAL_KEYS = {"alert_id", "fingerprint", "category", "detail", "source", "scope", "account_ref", "tenant_ref",
                 "rotation_key", "created_by", "actor", "worker_id", "host", "escalation_level", "occurrences",
                 "acknowledged_by", "resolved_by", "incident_updates", "owner_required"}


class Sink:
    name = "test"

    def send(self, m):
        pass


@pytest.fixture
def api(tmp_path, monkeypatch):
    db = Database(f"sqlite:///{(tmp_path / 'platform.db').as_posix()}")
    migrate(db, all_migrations())
    ops_platform.reset_platform_db(db)
    operations.clear_public_cache()
    monkeypatch.setenv("OPS_RESTORE_DRILL_EVIDENCE", str(tmp_path / "missing.json"))
    app = FastAPI()
    app.include_router(operations.public_router)
    app.include_router(operations.admin_router, prefix="/api")
    app.dependency_overrides[require_admin] = lambda: {"id": "admin-7", "email": "a@example.test"}
    yield SimpleNamespace(client=TestClient(app), db=db, app=app, tmp=tmp_path)
    ops_platform.reset_platform_db(None)
    operations.clear_public_cache()
    db.close()


def _keys(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k
            yield from _keys(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _keys(v)


def test_public_status_contains_no_internal_fields(api):
    engine = AlertEngine(api.db, sinks=[Sink()])
    engine.raise_alert("WORKER_OUTAGE", key="w1", title="worker vps-03 at 192.168.1.20 down", source="evaluator",
                       detail={"worker_id": "vps-03:991:aa", "host": "192.168.1.20"})
    for n in range(3):
        engine.raise_alert("EXCHANGE_EXECUTION_UNCERTAINTY", key=f"o{n}", title="lost ack", source="evaluator",
                           account_ref=f"acct-secret-{n}", tenant_ref=f"tenant-{n}",
                           detail={"exchange_state": {"balance": 1234.5}})
    r = api.client.post("/api/admin/operations/status/incidents", json={
        "title": "Elevated order latency", "components": ["TRADING_ENGINE", "EXCHANGE_CONNECTIVITY"],
        "impact": "DEGRADED", "message": "Some orders are slower than usual. We are investigating."})
    assert r.status_code == 201, r.text
    r = api.client.get("/api/public/status")
    assert r.status_code == 200 and "max-age" in r.headers["cache-control"]
    payload = r.json()
    assert set(payload) == {"generated_at", "overall", "data_available", "notice", "components", "incidents"}
    assert not (set(_keys(payload)) & INTERNAL_KEYS)
    text = json.dumps(payload)
    for leak in ("vps-03", "192.168.1.20", "acct-secret", "tenant-", "1234.5", "admin-7", "WORKER_OUTAGE",
                 "EXCHANGE_EXECUTION_UNCERTAINTY", "evaluator", "lost ack"):
        assert leak not in text, leak
    assert not re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", text)
    states = {c["key"]: c["status"] for c in payload["components"]}
    assert states["TRADING_ENGINE"] == "DEGRADED" and states["EXCHANGE_CONNECTIVITY"] == "DEGRADED"
    assert [c["name"] for c in payload["components"]] == ["Portal", "Trading engine", "Market data",
                                                          "Exchange connectivity", "Notifications"]


def test_public_status_without_platform_db_is_degraded_not_fabricated(api, monkeypatch):
    def unavailable():
        raise ops_platform.OperationsUnavailable("PLATFORM_DATABASE_UNAVAILABLE")
    monkeypatch.setattr(ops_platform, "platform_db", unavailable)
    operations.clear_public_cache()
    p = api.client.get("/api/public/status").json()
    assert p["data_available"] is False and p["overall"] == "DEGRADED"
    assert api.client.get("/api/admin/operations/alerts").status_code == 503


def test_incident_text_with_internal_details_refused(api):
    r = api.client.post("/api/admin/operations/status/incidents", json={
        "title": "DB issue", "components": ["PORTAL"], "impact": "OUTAGE",
        "message": "primary db at 10.1.2.3 (db1.internal) is down"})
    assert r.status_code == 400
    assert set(r.json()["detail"]["problems"]) >= {"IP_ADDRESS", "INTERNAL_HOSTNAME"}


def test_incident_update_and_resolve_history(api):
    inc = api.client.post("/api/admin/operations/status/incidents", json={
        "title": "Notifications delayed", "components": ["NOTIFICATIONS"], "impact": "DEGRADED",
        "message": "E-mail notifications are delayed."}).json()
    iid = inc["incident_id"]
    u = api.client.post(f"/api/admin/operations/status/incidents/{iid}/updates",
                        json={"status": "IDENTIFIED", "message": "Provider issue identified."}).json()
    assert u["status"] == "IDENTIFIED"
    done = api.client.post(f"/api/admin/operations/status/incidents/{iid}/resolve",
                           json={"message": "Delivery back to normal."}).json()
    assert done["status"] == "RESOLVED" and len(done["updates"]) == 3
    assert all(x["actor"] == "admin:admin-7" for x in done["updates"])
    assert api.client.post(f"/api/admin/operations/status/incidents/{iid}/resolve",
                           json={"message": "again!"}).status_code == 409


def test_alert_ack_and_resolve_via_admin_api(api):
    engine = AlertEngine(api.db, sinks=[Sink()])
    a = engine.raise_alert("UNPROTECTED_POSITION", key="p1", title="naked position", source="evaluator")
    alerts = api.client.get("/api/admin/operations/alerts?status=OPEN").json()["alerts"]
    assert a.alert_id in {x["alert_id"] for x in alerts}
    r = api.client.post(f"/api/admin/operations/alerts/{a.alert_id}/acknowledge", json={"note": "looking"})
    assert r.status_code == 200 and r.json()["acknowledged_by"] == "admin:admin-7"
    assert api.client.post(f"/api/admin/operations/alerts/{a.alert_id}/acknowledge", json={}).status_code == 409
    assert api.client.post(f"/api/admin/operations/alerts/{a.alert_id}/resolve", json={"note": ""}).status_code == 422
    r = api.client.post(f"/api/admin/operations/alerts/{a.alert_id}/resolve", json={"note": "protection placed"})
    assert r.json()["status"] == "RESOLVED" and r.json()["resolved_by"] == "admin:admin-7"
    assert api.client.get("/api/admin/operations/alerts/nope").status_code == 404


def test_oncall_defaults_to_operations_owner_required_and_admin_assignment(api):
    view = api.client.get("/api/admin/operations/oncall").json()
    assert view["status"] == "OPERATIONS_OWNER_REQUIRED" and view["owner_assigned"] is False
    assert all(r["members"] == [] for r in view["rotations"])
    ov = api.client.get("/api/admin/operations/overview").json()
    assert ov["alerting"]["status"] == "CRITICAL" and ov["alerting"]["owner_required"] is True
    r = api.client.post("/api/admin/operations/oncall/members", json={
        "rotation_key": "operations-owner", "level": 0, "person_name": "Named Owner",
        "contact_channel": "EMAIL", "contact_address": "owner@example.test"})
    assert r.status_code == 201 and r.json()["owner_assigned"] is True
    member_id = r.json()["member_id"]
    bad = api.client.post("/api/admin/operations/oncall/members", json={
        "rotation_key": "nobody-knows", "level": 0, "person_name": "X", "contact_channel": "EMAIL",
        "contact_address": "x@example.test"})
    assert bad.status_code == 400
    api.client.post(f"/api/admin/operations/oncall/members/{member_id}/remove")
    hist = api.client.get("/api/admin/operations/oncall/history").json()["history"]
    assert [h["action"] for h in hist] == ["REMOVED", "ASSIGNED"] and hist[0]["actor"] == "admin:admin-7"


def test_backup_drill_summary(api, monkeypatch):
    s = api.client.get("/api/admin/operations/backup-drill").json()
    assert s["status"] == "NO_DRILL_RECORDED" and s["rpo_target_minutes"] == "UNSET"
    ev = api.tmp / "drill.json"
    ev.write_text(json.dumps({"ok": True, "finished_at": "2026-10-17T10:00:00Z", "checks": {"a": True, "b": True},
                              "rpo": {"rpo_ms": 0}, "rto": {"rto_ms": 812}, "encryption": {"method": "fernet-test"},
                              "source": {"kind": "synthetic"}}))
    monkeypatch.setenv("OPS_RESTORE_DRILL_EVIDENCE", str(ev))
    s = api.client.get("/api/admin/operations/backup-drill").json()
    assert s["status"] == "PASSED" and s["checks_passed"] == 2 and s["rto_ms"] == 812


def test_missing_operations_schema_is_503(tmp_path):
    db = Database(f"sqlite:///{(tmp_path / 'bare.db').as_posix()}")
    ops_platform.reset_platform_db(db)
    try:
        app = FastAPI()
        app.include_router(operations.admin_router, prefix="/api")
        app.dependency_overrides[require_admin] = lambda: {"id": "a"}
        r = TestClient(app).get("/api/admin/operations/alerts")
        assert r.status_code == 503 and r.json()["detail"]["error_code"] == "OPERATIONS_SCHEMA_MISSING"
    finally:
        ops_platform.reset_platform_db(None)
        db.close()


def test_admin_operations_require_admin(api):
    api.app.dependency_overrides.pop(require_admin)
    assert api.client.get("/api/admin/operations/overview").status_code == 401
    assert api.client.get("/api/public/status").status_code == 200
