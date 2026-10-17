"""Step 6.5: every incident drill passes on the synthetic venue; incident records are complete and immutable."""
from __future__ import annotations

import pytest

from app.golive.drills import DRILLS, DrillEnv, run_all
from app.golive.incidents import IncidentError, IncidentLog, escalation_for


def test_all_drills_pass_and_are_recorded(database_url, tmp_path):
    env = DrillEnv(tmp_path / "drills", database_url)
    results = run_all(env)
    failed = {r["scenario"]: r["actions"] for r in results if not r["passed"]}
    assert not failed, failed
    assert {r["scenario"] for r in results} == set(DRILLS) and len(DRILLS) == 13
    for r in results:
        assert r["drill"] is True and r["closed_at"] is not None
        assert r["detected_at"] is not None and r["contained_at"] is not None
        assert r["root_cause"] and r["corrective_actions"] and r["retest_evidence"]
        assert r["reconciliation_status"] in ("RECONCILED", "NOT_AFFECTED")
    assert env.log.open_incidents() == []


def test_incident_cannot_close_without_containment_root_cause_and_retest(platform_db):
    log = IncidentLog(platform_db)
    inc = log.open(scenario="EXCHANGE_OUTAGE", operator_ref="operator:x", environment="SYNTHETIC", started_at=1, drill=True)
    with pytest.raises(IncidentError, match="CONTAINMENT"):
        log.close(inc.incident_id, root_cause="rc", corrective_actions=["fix"], retest_evidence={"ok": 1}, closed_at=5)
    log.update(inc.incident_id, detected_at=2, contained_at=3, action={"step": "failover"})
    with pytest.raises(IncidentError, match="ROOT_CAUSE"):
        log.close(inc.incident_id, root_cause="", corrective_actions=[], retest_evidence={}, closed_at=5)
    closed = log.close(inc.incident_id, root_cause="rc", corrective_actions=["fix"], retest_evidence={"ok": 1}, closed_at=5)
    assert closed.time_to_containment_ms == 2 and closed.revision == 2
    with pytest.raises(IncidentError, match="CLOSED"):
        log.update(inc.incident_id, remaining_risk="changed after closing")
    with pytest.raises(IncidentError, match="IMMUTABLE"):
        IncidentLog(platform_db).update(log.open(scenario="WORKER_CRASH", operator_ref="o", environment="S",
                                                 started_at=1, drill=True).incident_id, started_at=0)
    with pytest.raises(Exception, match="APPEND_ONLY"):
        with platform_db.transaction() as conn:
            conn.execute("DELETE FROM incident_records")


def test_unowned_escalation_is_explicit():
    assert escalation_for("SEV1")["first"] == "OPERATIONS_OWNER_REQUIRED"
    assert escalation_for("SEV1", {"PRIMARY_ON_CALL": "rotation:primary"})["first"] == "rotation:primary"
