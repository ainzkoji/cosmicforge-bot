"""Per-family governance: the operable M0 to M6 record of the CATI platform (Section H, Step 3.11).

One append-only, hash-chained log (``cati_family_events``) holds everything that was ever decided about a
family: its registration, the evidence recorded for it, every promotion request with its approval or
rejection and the reviewer who gave it, revocations, the family kill switch and its disabling. The current
state of a family is the fold of its events; nothing else (a flag, a deploy, a merge, a command-line switch)
can move it.

The stages are the phases of ``governance.phases`` (M0 ... M9), unchanged and not renamed, and the evidence a
stage asks for is that phase's ``entry_requirements``. Two prerequisites are added by code because no recorded
evidence may stand in for them:

* **M5 and above** need the family's mandate to verify against the research register (pinned hashes) and a
  completed, valid evaluation run of it.
* **M6** (a certified family trading on demo accounts) needs the research register to say the hypothesis
  PASSED, and, for a rule-based family, the rule-based admission route to answer ELIGIBLE.

This module operates M0 to M6 only. A request above M6 is refused: limited production is a separate decision
under the existing global promotion governance and is not part of this build.

Who may do what:

* A promotion is **requested** by anyone with a name, and changes nothing.
* It is **approved, rejected or revoked** only with an ``OperatorAuthorization``, which the authenticated
  administrative interface builds from the signed-in administrator. No command-line tool constructs one.
* Revoking, disabling and the kill switch are always possible and need no evidence: the safe direction is
  never blocked by a missing prerequisite.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple

from app.trading_intelligence.governance.phases import PHASE_BY_ID, PHASE_IDS, next_phase, phase_index
from app.trading_intelligence.hashing import short_id, stable_hash
from shared_lib.persistence.cati_platform_schema import FAMILY_EVENT_TABLE

GOVERNANCE_VERSION = "1.0.0"
GENESIS = "0" * 64
MAX_OPERABLE_STAGE = "M6"
#: from this stage a family may submit orders to demo accounts (``phases``: "DEMO CATI ACTIVE")
DEMO_STAGE = "M6"
#: from this stage, and only with the global governance scopes, to live accounts
LIVE_STAGE = "M7"

REGISTERED = "FAMILY_REGISTERED"
EVIDENCE_RECORDED = "EVIDENCE_RECORDED"
CERTIFICATION_VERIFIED = "CERTIFICATION_VERIFIED"
PROMOTION_REQUESTED = "PROMOTION_REQUESTED"
PROMOTION_APPROVED = "PROMOTION_APPROVED"
PROMOTION_REJECTED = "PROMOTION_REJECTED"
PROMOTION_REVOKED = "PROMOTION_REVOKED"
CERTIFICATION_REVOKED = "CERTIFICATION_REVOKED"
KILL_SWITCH_ON, KILL_SWITCH_OFF = "FAMILY_KILL_SWITCH_ON", "FAMILY_KILL_SWITCH_OFF"
DISABLED, ENABLED = "FAMILY_DISABLED", "FAMILY_ENABLED"
EVENT_TYPES: Tuple[str, ...] = (
    REGISTERED, EVIDENCE_RECORDED, CERTIFICATION_VERIFIED, PROMOTION_REQUESTED, PROMOTION_APPROVED,
    PROMOTION_REJECTED, PROMOTION_REVOKED, CERTIFICATION_REVOKED, KILL_SWITCH_ON, KILL_SWITCH_OFF, DISABLED, ENABLED)


class FamilyGovernanceError(RuntimeError):
    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


@dataclass(frozen=True)
class OperatorAuthorization:
    """An authenticated administrator acting through the administrative interface. ``channel`` records which
    interface authenticated them; ``permission`` the permission that was checked."""
    operator_id: str
    channel: str
    permission: str

    def __post_init__(self) -> None:
        for name in ("operator_id", "channel", "permission"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise FamilyGovernanceError("OPERATOR_AUTHORIZATION_REQUIRED", f"{name} is required")

    @property
    def actor_ref(self) -> str:
        return f"operator:{self.operator_id}@{self.channel}"


@dataclass(frozen=True)
class FamilyState:
    family_id: str
    registered: bool
    family_version: Optional[str]
    rule_hash: Optional[str]
    definition_hash: Optional[str]
    stage: str
    kill_switch: bool
    kill_switch_reason: Optional[str]
    disabled: bool
    certification_revoked: bool
    pending_request: Optional[Dict[str, Any]]
    evidence: Mapping[str, Dict[str, Any]]
    last_event_seq: int


def _require_operator(operator: Any) -> OperatorAuthorization:
    if not isinstance(operator, OperatorAuthorization):
        raise FamilyGovernanceError("OPERATOR_AUTHORIZATION_REQUIRED",
                                    "this action is taken by an authenticated administrator, not by a caller's say-so")
    return operator


class FamilyGovernance:
    def __init__(self, db: Any) -> None:
        self._db = db

    # ------------------------------------------------------------------------------------------- the log
    def _append(self, conn: Any, family_id: str, event_type: str, *, actor_ref: str, payload: Mapping[str, Any],
                request_id: Optional[str] = None, from_stage: Optional[str] = None, to_stage: Optional[str] = None,
                now_ms: Optional[int] = None) -> Dict[str, Any]:
        if event_type not in EVENT_TYPES:
            raise FamilyGovernanceError("UNKNOWN_EVENT_TYPE", event_type)
        if not actor_ref or not str(actor_ref).strip():
            raise FamilyGovernanceError("ACTOR_REQUIRED", "every governance record names who made it")
        last = conn.execute(f"SELECT seq, event_hash FROM {FAMILY_EVENT_TABLE} ORDER BY seq DESC LIMIT 1").fetchone()
        seq, prev = (int(last[0]) + 1, last[1]) if last else (1, GENESIS)
        ts = int(now_ms if now_ms is not None else time.time() * 1000)
        body = json.dumps(dict(payload), sort_keys=True, default=str)
        core = {"seq": seq, "family_id": family_id, "event_type": event_type, "scope": "FAMILY",
                "request_id": request_id, "from_stage": from_stage, "to_stage": to_stage, "actor_ref": actor_ref,
                "recorded_at": ts, "payload": body, "prev_hash": prev}
        event_hash = stable_hash(core)
        event_id = short_id("fev", {"seq": seq, "hash": event_hash})
        conn.execute(
            f"INSERT INTO {FAMILY_EVENT_TABLE} (seq, event_id, family_id, event_type, scope, request_id, from_stage, "
            "to_stage, actor_ref, recorded_at, payload, prev_hash, event_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (seq, event_id, family_id, event_type, "FAMILY", request_id, from_stage, to_stage, actor_ref, ts, body,
             prev, event_hash))
        return {**core, "event_id": event_id, "event_hash": event_hash, "payload": dict(payload)}

    def _write(self, family_id: str, event_type: str, check=None, **fields: Any) -> Dict[str, Any]:
        """One IMMEDIATE transaction: ``check(state)`` sees the state the record will be appended to."""
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            state = self._fold(family_id, self._rows(conn, family_id))
            extra = check(state) if check else {}
            return self._append(conn, family_id, event_type, **{**fields, **(extra or {})})

    @staticmethod
    def _rows(conn: Any, family_id: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = f"SELECT * FROM {FAMILY_EVENT_TABLE}" + (" WHERE family_id=?" if family_id else "") + " ORDER BY seq"
        rows = [dict(r) for r in conn.execute(sql, (family_id,) if family_id else ())]
        for r in rows:
            r["payload"] = json.loads(r["payload"])
        return rows

    def history(self, family_id: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._db.connect() as conn:
            return self._rows(conn, family_id)

    def verify_chain(self) -> Dict[str, Any]:
        """Recompute every hash. A changed, removed or reordered record is reported with its position."""
        prev, count = GENESIS, 0
        with self._db.connect() as conn:
            for r in conn.execute(f"SELECT * FROM {FAMILY_EVENT_TABLE} ORDER BY seq"):
                r = dict(r)
                count += 1
                core = {k: r[k] for k in ("seq", "family_id", "event_type", "scope", "request_id", "from_stage",
                                          "to_stage", "actor_ref", "recorded_at", "payload", "prev_hash")}
                if r["seq"] != count or r["prev_hash"] != prev or stable_hash(core) != r["event_hash"]:
                    return {"ok": False, "records": count, "broken_at_seq": r["seq"]}
                prev = r["event_hash"]
        return {"ok": True, "records": count, "head_hash": prev}

    # ------------------------------------------------------------------------------------------ the state
    @staticmethod
    def _fold(family_id: str, rows: List[Dict[str, Any]]) -> FamilyState:
        registered, version, rule_hash, definition_hash = False, None, None, None
        stage, kill, kill_reason, disabled, revoked = "M0", False, None, False, False
        pending: Optional[Dict[str, Any]] = None
        evidence: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            t, p = r["event_type"], r["payload"]
            if t == REGISTERED:
                changed = registered and (p.get("rule_hash") != rule_hash or p.get("family_version") != version)
                registered, version, rule_hash = True, p.get("family_version"), p.get("rule_hash")
                definition_hash = p.get("definition_hash")
                if changed:  # a changed rule or version is a different thing: no promotion carries over
                    stage, pending, evidence, revoked = "M0", None, {}, False
            elif t == EVIDENCE_RECORDED:
                evidence[p["key"]] = {"reference": p.get("reference"), "value": p.get("value"), "seq": r["seq"],
                                      "actor_ref": r["actor_ref"], "recorded_at": r["recorded_at"]}
            elif t == PROMOTION_REQUESTED:
                pending = {"request_id": r["request_id"], "from_stage": r["from_stage"], "to_stage": r["to_stage"],
                           "requested_by": r["actor_ref"], "requested_at": r["recorded_at"],
                           "reason": p.get("reason"), "evidence": p.get("evidence") or {}}
            elif t == PROMOTION_APPROVED:
                stage, pending = r["to_stage"], None
            elif t == PROMOTION_REJECTED:
                pending = None
            elif t == PROMOTION_REVOKED:
                stage, pending = r["to_stage"], None
            elif t == CERTIFICATION_REVOKED:
                stage, pending, revoked = "M0", None, True
            elif t == KILL_SWITCH_ON:
                kill, kill_reason = True, p.get("reason")
            elif t == KILL_SWITCH_OFF:
                kill, kill_reason = False, None
            elif t == DISABLED:
                disabled = True
            elif t == ENABLED:
                disabled = False
        return FamilyState(family_id=family_id, registered=registered, family_version=version, rule_hash=rule_hash,
                           definition_hash=definition_hash, stage=stage, kill_switch=kill, kill_switch_reason=kill_reason,
                           disabled=disabled, certification_revoked=revoked, pending_request=pending, evidence=evidence,
                           last_event_seq=rows[-1]["seq"] if rows else 0)

    def state(self, family_id: str) -> FamilyState:
        with self._db.connect() as conn:
            return self._fold(family_id, self._rows(conn, family_id))

    def families(self) -> List[str]:
        with self._db.connect() as conn:
            return [r[0] for r in conn.execute(
                f"SELECT DISTINCT family_id FROM {FAMILY_EVENT_TABLE} WHERE event_type=? ORDER BY family_id", (REGISTERED,))]

    # ------------------------------------------------------------------------------------- registration
    def register_family(self, definition: Any, *, actor_ref: str, now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Record a family definition. Registering the definition already on record is a no-op and returns
        it; a changed rule hash or version is recorded as a new registration and returns the family to M0."""
        plain = definition.to_plain()
        digest = stable_hash(plain)

        def check(state: FamilyState) -> Dict[str, Any]:
            if state.registered and state.definition_hash == digest:
                raise _Unchanged()
            return {}

        try:
            return self._write(definition.family_id, REGISTERED, check, actor_ref=actor_ref, now_ms=now_ms,
                               payload={**plain, "definition_hash": digest, "governance_version": GOVERNANCE_VERSION})
        except _Unchanged:
            return {"family_id": definition.family_id, "event_type": REGISTERED, "unchanged": True,
                    "definition_hash": digest}

    def record_evidence(self, family_id: str, key: str, *, reference: str, actor_ref: str, value: Any = True,
                        now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Attach a piece of evidence (a file, a run id, a report) to a family under a requirement key."""
        if not key or not reference:
            raise FamilyGovernanceError("EVIDENCE_INCOMPLETE", "evidence has a key and a reference to where it is")
        return self._write(family_id, EVIDENCE_RECORDED, self._must_be_registered, actor_ref=actor_ref, now_ms=now_ms,
                           payload={"key": key, "reference": reference, "value": value})

    def record_certification_check(self, family_id: str, certification: Mapping[str, Any], *, actor_ref: str,
                                   now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Keep what the research register said at a point in time. A record, never an authority: every
        decision reads the register again."""
        return self._write(family_id, CERTIFICATION_VERIFIED, self._must_be_registered, actor_ref=actor_ref,
                           now_ms=now_ms, payload=dict(certification))

    @staticmethod
    def _must_be_registered(state: FamilyState) -> Dict[str, Any]:
        if not state.registered:
            raise FamilyGovernanceError("FAMILY_NOT_REGISTERED", state.family_id)
        return {}

    # ---------------------------------------------------------------------------------------- promotion
    def request_promotion(self, family_id: str, to_stage: str, *, actor_ref: str, reason: str,
                          evidence: Optional[Mapping[str, Any]] = None, now_ms: Optional[int] = None) -> Dict[str, Any]:
        if to_stage not in PHASE_BY_ID:
            raise FamilyGovernanceError("UNKNOWN_STAGE", to_stage)
        if phase_index(to_stage) > phase_index(MAX_OPERABLE_STAGE):
            raise FamilyGovernanceError("STAGE_NOT_OPERABLE_HERE",
                                        f"{to_stage} is beyond {MAX_OPERABLE_STAGE}; live promotion is a separate decision")
        if not reason or not str(reason).strip():
            raise FamilyGovernanceError("REASON_REQUIRED", "a promotion request states its reason")
        ts = int(now_ms if now_ms is not None else time.time() * 1000)
        request_id = short_id("fpr", {"family": family_id, "to": to_stage, "t": ts, "by": actor_ref})

        def check(state: FamilyState) -> Dict[str, Any]:
            self._must_be_registered(state)
            if state.disabled:
                raise FamilyGovernanceError("FAMILY_DISABLED", family_id)
            if state.pending_request:
                raise FamilyGovernanceError("PROMOTION_ALREADY_PENDING", state.pending_request["request_id"])
            if to_stage != next_phase(state.stage):
                raise FamilyGovernanceError("STAGE_SKIPPED",
                                            f"{state.stage} -> {to_stage}: the next stage is {next_phase(state.stage)}")
            return {"from_stage": state.stage}

        return self._write(family_id, PROMOTION_REQUESTED, check, actor_ref=actor_ref, now_ms=ts,
                           request_id=request_id, to_stage=to_stage,
                           payload={"reason": reason, "evidence": dict(evidence or {})})

    def missing_prerequisites(self, state: FamilyState, to_stage: str, *,
                              prerequisites: Mapping[str, Any]) -> List[str]:
        """What stands between ``state`` and ``to_stage``. ``prerequisites`` is what the registry verified
        against the research register just now (see ``family_registry.promotion_prerequisites``)."""
        offered = dict((state.pending_request or {}).get("evidence") or {})
        missing = [f"EVIDENCE_MISSING:{key}" for key in PHASE_BY_ID[to_stage].entry_requirements
                   if not (state.evidence.get(key, {}).get("value") or offered.get(key))]
        if state.certification_revoked:
            missing.append("CERTIFICATION_REVOKED")
        if phase_index(to_stage) >= phase_index("M5"):
            if prerequisites.get("mandate_verified") is not True:
                missing.append("MANDATE_NOT_VERIFIED")
            if prerequisites.get("valid_run") is not True:
                missing.append("NO_VALID_EVALUATION_RUN")
        if phase_index(to_stage) >= phase_index(DEMO_STAGE):
            if prerequisites.get("certification_status") != "CERTIFIED":
                missing.append(f"RESEARCH_CERTIFICATION_{prerequisites.get('certification_status') or 'UNKNOWN'}")
            if prerequisites.get("admission") not in (None, "ELIGIBLE_FOR_GOVERNANCE_REVIEW"):
                missing.append(f"ADMISSION_{prerequisites.get('admission')}")
        return missing

    def approve_promotion(self, request_id: str, *, operator: OperatorAuthorization, reason: str,
                          prerequisites: Mapping[str, Any], now_ms: Optional[int] = None) -> Dict[str, Any]:
        operator = _require_operator(operator)
        family_id = self.family_of_request(request_id)

        def check(state: FamilyState) -> Dict[str, Any]:
            pending = state.pending_request
            if not pending or pending["request_id"] != request_id:
                raise FamilyGovernanceError("REQUEST_NOT_PENDING", request_id)
            if pending["from_stage"] != state.stage:
                raise FamilyGovernanceError("STAGE_CHANGED_SINCE_REQUEST", f"{pending['from_stage']} != {state.stage}")
            if state.disabled:
                raise FamilyGovernanceError("FAMILY_DISABLED", family_id)
            if prerequisites.get("rule_hash") != state.rule_hash:
                raise FamilyGovernanceError("FAMILY_RULE_HASH_MISMATCH", "the code no longer matches the registration")
            missing = self.missing_prerequisites(state, pending["to_stage"], prerequisites=prerequisites)
            if missing:
                raise FamilyGovernanceError("PREREQUISITES_NOT_MET", ", ".join(missing))
            return {"from_stage": state.stage, "to_stage": pending["to_stage"]}

        return self._write(family_id, PROMOTION_APPROVED, check, actor_ref=operator.actor_ref, now_ms=now_ms,
                           request_id=request_id,
                           payload={"reason": reason, "reviewer": operator.operator_id, "channel": operator.channel,
                                    "permission": operator.permission, "prerequisites": dict(prerequisites)})

    def reject_promotion(self, request_id: str, *, operator: OperatorAuthorization, reason: str,
                         now_ms: Optional[int] = None) -> Dict[str, Any]:
        operator = _require_operator(operator)
        family_id = self.family_of_request(request_id)

        def check(state: FamilyState) -> Dict[str, Any]:
            pending = state.pending_request
            if not pending or pending["request_id"] != request_id:
                raise FamilyGovernanceError("REQUEST_NOT_PENDING", request_id)
            return {"from_stage": state.stage, "to_stage": state.stage}

        return self._write(family_id, PROMOTION_REJECTED, check, actor_ref=operator.actor_ref, now_ms=now_ms,
                           request_id=request_id, payload={"reason": reason, "reviewer": operator.operator_id})

    def revoke_promotion(self, family_id: str, *, operator: OperatorAuthorization, reason: str, to_stage: str = "M0",
                         now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Move a family back. Any lower stage is allowed; M0 is the default and the safe one."""
        operator = _require_operator(operator)

        def check(state: FamilyState) -> Dict[str, Any]:
            self._must_be_registered(state)
            if to_stage not in PHASE_IDS or phase_index(to_stage) > phase_index(state.stage):
                raise FamilyGovernanceError("NOT_A_REVOCATION", f"{state.stage} -> {to_stage}")
            return {"from_stage": state.stage}

        return self._write(family_id, PROMOTION_REVOKED, check, actor_ref=operator.actor_ref, now_ms=now_ms,
                           to_stage=to_stage, payload={"reason": reason, "reviewer": operator.operator_id})

    def revoke_certification(self, family_id: str, *, operator: OperatorAuthorization, reason: str,
                             now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Withdraw the platform's acceptance of a research certification. The research register is not
        changed (its records are final); from here the family is REVOKED and at M0 until re-registered."""
        operator = _require_operator(operator)

        def check(state: FamilyState) -> Dict[str, Any]:
            self._must_be_registered(state)
            return {"from_stage": state.stage, "to_stage": "M0"}

        return self._write(family_id, CERTIFICATION_REVOKED, check, actor_ref=operator.actor_ref, now_ms=now_ms,
                           payload={"reason": reason, "reviewer": operator.operator_id})

    # ---------------------------------------------------------------------------------- stop and disable
    def set_kill_switch(self, family_id: str, on: bool, *, operator: OperatorAuthorization, reason: str,
                        now_ms: Optional[int] = None) -> Dict[str, Any]:
        """ON: no new entry and no increase from this family, on any account, from the next decision. What
        happens to the positions it holds is the family's registered ``kill_switch_position_policy``."""
        operator = _require_operator(operator)
        return self._write(family_id, KILL_SWITCH_ON if on else KILL_SWITCH_OFF, self._must_be_registered,
                           actor_ref=operator.actor_ref, now_ms=now_ms, payload={"reason": reason})

    def set_disabled(self, family_id: str, disabled: bool, *, operator: OperatorAuthorization, reason: str,
                     now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Disabled: the family is not evaluated at all (no shadow decisions either) and cannot be promoted."""
        operator = _require_operator(operator)
        return self._write(family_id, DISABLED if disabled else ENABLED, self._must_be_registered,
                           actor_ref=operator.actor_ref, now_ms=now_ms, payload={"reason": reason})

    def family_of_request(self, request_id: str) -> str:
        with self._db.connect() as conn:
            row = conn.execute(f"SELECT family_id FROM {FAMILY_EVENT_TABLE} WHERE request_id=? AND event_type=?",
                               (request_id, PROMOTION_REQUESTED)).fetchone()
        if row is None:
            raise FamilyGovernanceError("REQUEST_NOT_FOUND", request_id)
        return row[0]


class _Unchanged(Exception):
    pass


__all__ = ["GOVERNANCE_VERSION", "MAX_OPERABLE_STAGE", "DEMO_STAGE", "LIVE_STAGE", "EVENT_TYPES", "REGISTERED",
           "EVIDENCE_RECORDED", "CERTIFICATION_VERIFIED", "PROMOTION_REQUESTED", "PROMOTION_APPROVED",
           "PROMOTION_REJECTED", "PROMOTION_REVOKED", "CERTIFICATION_REVOKED", "KILL_SWITCH_ON", "KILL_SWITCH_OFF",
           "DISABLED", "ENABLED", "FamilyGovernanceError", "OperatorAuthorization", "FamilyState", "FamilyGovernance"]
