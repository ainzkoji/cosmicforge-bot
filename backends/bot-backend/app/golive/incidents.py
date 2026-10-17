"""Incident records and escalation (Section H, Step 6.5).

An incident (or a drill -- ``drill=1``) is a series of immutable revisions in ``incident_records``: scenario,
start, detection, operator, actions, containment, reconciliation status, remaining risk, root cause, corrective
actions and retest evidence. A revision never overwrites another; the latest revision is the current state, and
an incident is closed only with a root cause, at least one corrective action and retest evidence.

Escalation (``ESCALATION``) is by severity; the named owners come from the operations on-call configuration
(Step 8.5). Where none is assigned, the escalation target is ``OPERATIONS_OWNER_REQUIRED`` -- an unowned page is
itself a finding, never silently dropped.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence

from shared_lib.persistence.sql import Database

#: severity -> (who is paged first, minutes until the next level, the next level)
ESCALATION: Mapping[str, tuple] = {
    "SEV1": ("PRIMARY_ON_CALL", 5, "ENGINEERING_LEAD_AND_OWNER"),
    "SEV2": ("PRIMARY_ON_CALL", 15, "ENGINEERING_LEAD"),
    "SEV3": ("PRIMARY_ON_CALL", 60, "NEXT_BUSINESS_DAY_REVIEW"),
    "SEV4": ("OPERATIONS_QUEUE", None, None),
}
SCENARIO_SEVERITY: Mapping[str, str] = {
    "ACCOUNT_KILL_SWITCH": "SEV3", "FAMILY_KILL_SWITCH": "SEV2", "PLATFORM_KILL_SWITCH": "SEV1",
    "EMERGENCY_FLATTEN": "SEV1", "EXCHANGE_OUTAGE": "SEV2", "DATABASE_OUTAGE": "SEV1", "WORKER_CRASH": "SEV2",
    "RATE_LIMIT_EXHAUSTION": "SEV3", "STALE_MARKET_DATA": "SEV2", "UNPROTECTED_POSITION": "SEV1",
    "LOST_SUBMISSION_ACK": "SEV2", "EXCHANGE_POSITION_MISMATCH": "SEV1", "RECONCILIATION_FAILURE": "SEV2",
}


class IncidentError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass
class Incident:
    incident_id: str
    revision: int
    scenario: str
    drill: bool
    severity: str
    environment: str
    started_at: int
    detected_at: Optional[int]
    contained_at: Optional[int]
    closed_at: Optional[int]
    operator_ref: str
    actions: List[Dict[str, Any]]
    reconciliation_status: str
    remaining_risk: str
    root_cause: Optional[str]
    corrective_actions: List[str]
    retest_evidence: Optional[Dict[str, Any]]

    @property
    def time_to_containment_ms(self) -> Optional[int]:
        return None if self.contained_at is None else self.contained_at - self.started_at

    def to_plain(self) -> Dict[str, Any]:
        d = dict(self.__dict__)
        d["time_to_containment_ms"] = self.time_to_containment_ms
        d["escalation"] = escalation_for(self.severity)
        return d


def escalation_for(severity: str, owners: Optional[Mapping[str, str]] = None) -> Dict[str, Any]:
    first, minutes, nxt = ESCALATION[severity]
    owners = owners or {}
    return {"first": owners.get(first, "OPERATIONS_OWNER_REQUIRED"), "first_role": first,
            "escalate_after_minutes": minutes, "next": owners.get(nxt, "OPERATIONS_OWNER_REQUIRED") if nxt else None,
            "next_role": nxt}


_COLS = ("incident_id, revision, scenario, drill, severity, environment, started_at, detected_at, contained_at, "
         "closed_at, operator_ref, actions, reconciliation_status, remaining_risk, root_cause, corrective_actions, "
         "retest_evidence")


class IncidentLog:
    def __init__(self, db: Database) -> None:
        self.db = db

    def open(self, *, scenario: str, operator_ref: str, environment: str, started_at: int, drill: bool,
             severity: Optional[str] = None) -> Incident:
        sev = severity or SCENARIO_SEVERITY.get(scenario, "SEV2")
        inc = Incident(uuid.uuid4().hex, 0, scenario, drill, sev, environment, int(started_at), None, None, None,
                       operator_ref, [], "PENDING", "UNASSESSED", None, [], None)
        return self._write(inc)

    def latest(self, incident_id: str) -> Incident:
        with self.db.transaction(write=False) as conn:
            r = conn.one(f"SELECT {_COLS} FROM incident_records WHERE incident_id=? ORDER BY revision DESC LIMIT 1",
                         (incident_id,))
        if r is None:
            raise IncidentError("INCIDENT_UNKNOWN")
        return Incident(r[0], int(r[1]), r[2], bool(r[3]), r[4], r[5], int(r[6]), r[7], r[8], r[9], r[10],
                        json.loads(r[11]), r[12], r[13], r[14], json.loads(r[15]), json.loads(r[16]) if r[16] else None)

    def update(self, incident_id: str, **changes: Any) -> Incident:
        inc = self.latest(incident_id)
        if inc.closed_at is not None:
            raise IncidentError("INCIDENT_CLOSED")
        action = changes.pop("action", None)
        for k, v in changes.items():
            if not hasattr(inc, k) or k in ("incident_id", "revision", "scenario", "drill", "started_at"):
                raise IncidentError(f"INCIDENT_FIELD_IMMUTABLE:{k}")
            setattr(inc, k, v)
        if action is not None:
            inc.actions = inc.actions + [dict(action)]
        inc.revision += 1
        return self._write(inc)

    def close(self, incident_id: str, *, root_cause: str, corrective_actions: Sequence[str],
              retest_evidence: Mapping[str, Any], closed_at: int) -> Incident:
        if not root_cause or not corrective_actions or not retest_evidence:
            raise IncidentError("CLOSE_REQUIRES_ROOT_CAUSE_CORRECTIVE_ACTION_AND_RETEST")
        inc = self.latest(incident_id)
        if inc.contained_at is None:
            raise IncidentError("CLOSE_REQUIRES_CONTAINMENT")
        return self.update(incident_id, root_cause=root_cause, corrective_actions=list(corrective_actions),
                           retest_evidence=dict(retest_evidence), closed_at=int(closed_at))

    def _write(self, inc: Incident) -> Incident:
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO incident_records (record_id, incident_id, revision, scenario, drill, severity, environment, "
                "started_at, detected_at, contained_at, closed_at, operator_ref, actions, reconciliation_status, "
                "remaining_risk, root_cause, corrective_actions, retest_evidence, recorded_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (uuid.uuid4().hex, inc.incident_id, inc.revision, inc.scenario, 1 if inc.drill else 0, inc.severity,
                 inc.environment, inc.started_at, inc.detected_at, inc.contained_at, inc.closed_at, inc.operator_ref,
                 json.dumps(inc.actions, default=str), inc.reconciliation_status, inc.remaining_risk, inc.root_cause,
                 json.dumps(inc.corrective_actions), json.dumps(inc.retest_evidence, default=str)
                 if inc.retest_evidence is not None else None, _now(conn)))
        return inc

    def open_incidents(self) -> List[Incident]:
        with self.db.transaction(write=False) as conn:
            ids = [r[0] for r in conn.all("SELECT DISTINCT incident_id FROM incident_records")]
        return [i for i in (self.latest(x) for x in ids) if i.closed_at is None]


def _now(conn) -> int:
    return conn.now_ms()
