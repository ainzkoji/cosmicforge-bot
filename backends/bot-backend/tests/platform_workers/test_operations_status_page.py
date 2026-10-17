"""Section H Step 8.5: public status page -- component derivation, admin-authored incidents with append-only
history, and the guarantee that the public payload carries no internal detail. SQLite and PostgreSQL."""
from __future__ import annotations

import json
import re

import pytest

from shared_lib.operations.alerting import AlertEngine
from shared_lib.operations.status_page import (ACCOUNT_AGGREGATE_THRESHOLD, COMPONENTS, StatusIncidents,
                                               StatusPageError, check_public_text, public_status)

T0 = 1_800_000_000_000
ALLOWED_TOP = {"generated_at", "overall", "data_available", "notice", "components", "incidents"}
ALLOWED_COMPONENT = {"key", "name", "status"}
ALLOWED_INCIDENT = {"id", "title", "status", "impact", "components", "started_at", "updated_at", "resolved_at",
                    "updates"}
ALLOWED_UPDATE = {"status", "message", "at"}


class Sink:
    name = "s"

    def send(self, m):
        pass


def assert_public_shape(payload):
    assert set(payload) == ALLOWED_TOP
    for c in payload["components"]:
        assert set(c) == ALLOWED_COMPONENT and c["status"] in ("OPERATIONAL", "DEGRADED", "OUTAGE", "MAINTENANCE")
    for i in payload["incidents"]:
        assert set(i) == ALLOWED_INCIDENT
        for u in i["updates"]:
            assert set(u) == ALLOWED_UPDATE


def states(payload):
    return {c["key"]: c["status"] for c in payload["components"]}


def test_all_operational_by_default(platform_db):
    p = public_status(platform_db)
    assert_public_shape(p)
    assert p["overall"] == "OPERATIONAL" and p["data_available"] is True
    assert [c["name"] for c in p["components"]] == list(COMPONENTS.values())


def test_platform_alert_degrades_its_component_without_leaking_internals(platform_db):
    e = AlertEngine(platform_db, sinks=[Sink()])
    e.raise_alert("WORKER_OUTAGE", key={"worker_id": "vps-01.internal:4242:abcd"}, title="worker vps-01 down",
                  source="test", detail={"worker_id": "vps-01.internal:4242:abcd", "host": "10.0.0.7"}, now_ms=T0)
    e.raise_alert("BACKUP_FAILURE", key="b", title="backup missing", source="test", now_ms=T0)   # never public
    p = public_status(platform_db)
    assert_public_shape(p)
    assert states(p)["TRADING_ENGINE"] == "DEGRADED" and p["overall"] == "DEGRADED"
    text = json.dumps(p)
    for leak in ("vps-01", "10.0.0.7", "WORKER_OUTAGE", "BACKUP_FAILURE", "worker", "alert", "fingerprint",
                 "rotation", "trading-operations", "platform-infrastructure"):
        assert leak not in text


def test_account_scoped_exchange_alerts_count_only_in_aggregate(platform_db):
    e = AlertEngine(platform_db, sinks=[Sink()])
    for n in range(ACCOUNT_AGGREGATE_THRESHOLD - 1):
        e.raise_alert("EXCHANGE_EXECUTION_UNCERTAINTY", key=f"o{n}", title="unknown", source="test",
                      account_ref=f"acct-{n}", tenant_ref=f"tenant-{n}", now_ms=T0)
    p = public_status(platform_db)
    assert states(p)["EXCHANGE_CONNECTIVITY"] == "OPERATIONAL"
    e.raise_alert("EXCHANGE_EXECUTION_UNCERTAINTY", key="o-last", title="unknown", source="test",
                  account_ref="acct-last", now_ms=T0)
    p = public_status(platform_db)
    assert states(p)["EXCHANGE_CONNECTIVITY"] == "DEGRADED"
    text = json.dumps(p)
    assert "acct-" not in text and "tenant-" not in text and str(ACCOUNT_AGGREGATE_THRESHOLD) not in re.sub(
        r"\d{4}-\d\d-\d\dT[\d:.]+Z", "", text)


def test_admin_incident_sets_impact_and_keeps_append_only_history(platform_db):
    inc = StatusIncidents(platform_db)
    created = inc.create(title="Delayed order processing", components=["TRADING_ENGINE"], impact="OUTAGE",
                         message="We are investigating delays in order processing.", actor="admin-1", now_ms=T0)
    p = public_status(platform_db, now_ms=T0)
    assert states(p)["TRADING_ENGINE"] == "OUTAGE" and p["overall"] == "OUTAGE"
    assert p["incidents"][0]["title"] == "Delayed order processing"
    inc.update(created["incident_id"], status="MONITORING", impact="DEGRADED", message="A fix is deployed.",
               actor="admin-2", now_ms=T0 + 1000)
    done = inc.resolve(created["incident_id"], message="Resolved.", actor="admin-3", now_ms=T0 + 2000)
    assert done["status"] == "RESOLVED" and [u["actor"] for u in done["updates"]] == ["admin-1", "admin-2", "admin-3"]
    p = public_status(platform_db, now_ms=T0 + 3000)
    assert_public_shape(p)
    assert states(p)["TRADING_ENGINE"] == "OPERATIONAL"
    assert p["incidents"][0]["status"] == "RESOLVED" and len(p["incidents"][0]["updates"]) == 3
    assert "admin-" not in json.dumps(p)
    with pytest.raises(StatusPageError) as err:
        inc.update(created["incident_id"], status="MONITORING", message="again", actor="a")
    assert err.value.code == "INCIDENT_ALREADY_RESOLVED"
    with pytest.raises(Exception) as err:
        with platform_db.transaction() as conn:
            conn.execute("UPDATE ops_status_incident_updates SET message='rewritten'")
    assert "APPEND_ONLY" in str(err.value)


def test_maintenance_window(platform_db):
    StatusIncidents(platform_db).create(title="Scheduled maintenance", components=["PORTAL", "NOTIFICATIONS"],
                                        impact="MAINTENANCE", message="Planned upgrade.", actor="a", now_ms=T0)
    p = public_status(platform_db, now_ms=T0)
    assert states(p)["PORTAL"] == "MAINTENANCE" and states(p)["NOTIFICATIONS"] == "MAINTENANCE"


@pytest.mark.parametrize("text, problem", [
    ("Database at 10.0.0.12 is slow", "IP_ADDRESS"),
    ("db01.internal is being restarted", "INTERNAL_HOSTNAME"),
    ("contact ops@example.test", "EMAIL_ADDRESS"),
    ("affected account_id: 42", "ACCOUNT_IDENTIFIER"),
    ("rotated api_key=AbCdEf0123456789xyz", "SECRET:API_KEY_ASSIGNMENT"),
])
def test_public_text_with_internal_details_is_refused(platform_db, text, problem):
    assert problem in check_public_text(text)
    with pytest.raises(StatusPageError) as err:
        StatusIncidents(platform_db).create(title="Incident", components=["PORTAL"], impact="DEGRADED", message=text,
                                            actor="a")
    assert err.value.code == "PUBLIC_TEXT_REFUSED" and problem in err.value.details["problems"]


def test_invalid_components_and_impacts_refused(platform_db):
    inc = StatusIncidents(platform_db)
    for kwargs in ({"components": []}, {"components": ["DATABASE"]}, {"impact": "MELTDOWN"}):
        args = dict(title="t", components=["PORTAL"], impact="DEGRADED", message="m", actor="a")
        args.update(kwargs)
        with pytest.raises(StatusPageError):
            inc.create(**args)


def test_unavailable_store_reports_degraded_never_operational():
    p = public_status(None)
    assert_public_shape(p)
    assert p["data_available"] is False and p["notice"]
    assert states(p)["PORTAL"] == "OPERATIONAL" and states(p)["TRADING_ENGINE"] == "DEGRADED"
