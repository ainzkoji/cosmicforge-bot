"""Pre-holdout readiness, the recorded authorization and open-once access (Section H, Step 2.5).

The reserved holdout is evidence, not a development tool. Three separate acts are needed to look at it, each
an append-only register record:

1. ``reserve_holdout``   -- before any run, the window is named and bound to the mandate and the dataset;
2. ``record_owner_holdout_authorization`` -- a NAMED PERSON, with a reference to where the approval was given, while readiness
                            is READY for exactly this specification, dataset and evaluation source. Nothing
                            in this codebase calls it: no test run, merge, schedule or agent may stand in for
                            the project owner;
3. ``open_holdout_once`` -- writes HOLDOUT_OPENED durably (fsync) BEFORE any holdout row is read, so a crash
                            cannot erase the fact of access. Only then does the caller receive the
                            ``HoldoutAccess`` token the data loader demands.

After the run, ``burn_holdout`` stores the result hash. A second request with the same run id returns the
stored result (idempotent). An opening that never reached BURNED stays on record as incomplete: running it
again needs its own recorded owner decision and never removes the first attempt.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from app.trading_intelligence.hashing import short_id, stable_hash

from .register import (
    HOLDOUT_AUTHORIZED, HOLDOUT_BURNED, HOLDOUT_OPENED, HOLDOUT_RESERVED, RegisterError, ResearchRegister,
)

READINESS_VERSION = "step2-pre-holdout-readiness-v1"
READY, NOT_READY = "READY", "NOT_READY"
#: (fact, reason when unmet), in the order of Section H 7.5; every unmet one is reported
REQUIREMENTS = (
    ("mandate_frozen", "MANDATE_NOT_REGISTERED_OR_HASH_DRIFT"),
    ("hypothesis_registered", "HYPOTHESIS_NOT_REGISTERED"),
    ("dataset_frozen", "DATASET_NOT_FROZEN_IN_REGISTER"),
    ("costs_registered", "COST_MODEL_NOT_REGISTERED"),
    ("statistical_gate_frozen", "STATISTICAL_GATE_POLICY_NOT_RECORDED"),
    ("simulator_tests_green", "SIMULATOR_TESTS_NOT_GREEN"),
    ("causality_audit_green", "CAUSALITY_AUDIT_NOT_GREEN"),
    ("development_run_valid", "DEVELOPMENT_RUN_NOT_VALID"),
    ("no_known_critical_defect", "KNOWN_CRITICAL_ACCOUNTING_DEFECT"),
    ("reporting_ready", "REPORT_TEMPLATES_NOT_READY"),
    ("reproduction_documented", "REPRODUCTION_PROCEDURE_NOT_DOCUMENTED"),
    ("source_committed_clean", "EVALUATION_SOURCE_NOT_COMMITTED"),
)
EXCEPTIONAL_RERUN = "HOLDOUT_EXCEPTIONAL_RERUN"


class HoldoutNotAuthorized(RegisterError):
    pass


class HoldoutAlreadyBurned(RegisterError):
    """The holdout was evaluated. ``result`` is the stored record (same run id) or None (another run)."""

    def __init__(self, message: str, result: Optional[Mapping[str, Any]] = None):
        super().__init__(message)
        self.result = result


class HoldoutAccessIncomplete(RegisterError):
    """The holdout was opened and no result was stored: access happened, the evidence is incomplete."""


@dataclass(frozen=True)
class HoldoutAccess:
    """Proof that the register recorded the opening. The data loader returns holdout rows only for one."""

    holdout_id: str
    run_id: str
    start: str
    end: str
    opened_record_hash: str


def holdout_id_for(mandate_id: str, dataset_hash: str, start: str, end: str) -> str:
    return short_id("hold", {"mandate_id": mandate_id, "dataset_hash": dataset_hash, "start": start, "end": end})


def reserve_holdout(register: ResearchRegister, *, mandate_id: str, dataset_hash: str, start: str, end: str,
                    now: Optional[str] = None) -> str:
    """Idempotent for the same mandate, dataset and window."""
    hid = holdout_id_for(mandate_id, dataset_hash, start, end)
    if register.holdout(hid)["status"] == "UNRESERVED":
        register.mandate(mandate_id)
        register.append(HOLDOUT_RESERVED, {"holdout_id": hid, "mandate_id": mandate_id, "dataset_hash": dataset_hash,
                                           "start": start, "end": end}, now=now)
    return hid


def pre_holdout_readiness(*, facts: Mapping[str, Any], mandate_id: str, specification_sha256: Optional[str],
                          dataset_hash: Optional[str], source_fingerprint: Optional[str],
                          development_run_id: Optional[str], evidence: Optional[Mapping[str, Any]] = None
                          ) -> Dict[str, Any]:
    """Deterministic over recorded facts: each requirement is True only when the caller proved it; a missing or
    non-True fact is NOT_READY. No wall clock, no holdout data."""
    checked = {key: facts.get(key) is True for key, _ in REQUIREMENTS}
    reasons = [code for key, code in REQUIREMENTS if not checked[key]]
    body = {"version": READINESS_VERSION, "status": READY if not reasons else NOT_READY, "reason_codes": reasons,
            "facts": checked, "mandate_id": mandate_id, "specification_sha256": specification_sha256,
            "dataset_hash": dataset_hash, "source_fingerprint": source_fingerprint,
            "development_run_id": development_run_id, "evidence": dict(evidence or {})}
    return {**body, "readiness_hash": stable_hash(body)}


def record_owner_holdout_authorization(register: ResearchRegister, *, holdout_id: str, readiness: Mapping[str, Any],
                      authorized_by: str, authorization_reference: str, reason: str,
                      acknowledgements: Optional[Mapping[str, Any]] = None, now: Optional[str] = None) -> Dict[str, Any]:
    """The project owner's act. Refused unless readiness is READY, the holdout is reserved and untouched, and
    the approver and the place the approval was given are both named."""
    if readiness.get("status") != READY:
        raise HoldoutNotAuthorized("holdout authorization refused: pre-holdout readiness is "
                                   f"{readiness.get('status')} ({','.join(readiness.get('reason_codes') or ())})")
    if not (str(authorized_by or "").strip() and str(authorization_reference or "").strip() and str(reason or "").strip()):
        raise HoldoutNotAuthorized("holdout authorization names the approver, the reference of the approval and a reason")
    st = register.holdout(holdout_id)
    if st["status"] != HOLDOUT_RESERVED:
        raise HoldoutNotAuthorized(f"holdout {holdout_id} is {st['status']}: only a reserved, untouched holdout "
                                   "may be authorized")
    if st["mandate_id"] != readiness.get("mandate_id") or st["dataset_hash"] != readiness.get("dataset_hash"):
        raise HoldoutNotAuthorized("readiness was computed for another mandate or dataset")
    return register.append(HOLDOUT_AUTHORIZED, {
        "holdout_id": holdout_id, "mandate_id": st["mandate_id"], "dataset_hash": st["dataset_hash"],
        "specification_sha256": readiness["specification_sha256"],
        "source_fingerprint": readiness["source_fingerprint"], "readiness_hash": readiness["readiness_hash"],
        "development_run_id": readiness.get("development_run_id"), "authorized_by": authorized_by,
        "authorization_reference": authorization_reference, "reason": reason,
        "acknowledgements": dict(acknowledgements or {})}, now=now)


def open_holdout_once(register: ResearchRegister, *, holdout_id: str, run_id: str, specification_sha256: str,
                      dataset_hash: str, source_fingerprint: str, code_commit: str,
                      now: Optional[str] = None) -> HoldoutAccess:
    st = register.holdout(holdout_id)
    status = st["status"]
    if status == HOLDOUT_BURNED:
        same = st["burned"]["run_id"] == run_id
        raise HoldoutAlreadyBurned(f"holdout {holdout_id} was already evaluated by run {st['burned']['run_id']}; "
                                   "it cannot be opened again", st["burned"] if same else None)
    if status == HOLDOUT_OPENED:
        rerun = register.decision(f"{EXCEPTIONAL_RERUN}:{holdout_id}")
        if st["opened"]["run_id"] == run_id and rerun and rerun["state"] == "APPROVED":
            opened = next(r for r in register.of_type(HOLDOUT_OPENED) if r["body"]["holdout_id"] == holdout_id)
            return HoldoutAccess(holdout_id, run_id, st["start"], st["end"], opened["record_hash"])
        raise HoldoutAccessIncomplete(f"holdout {holdout_id} was opened by run {st['opened']['run_id']} and has no "
                                      "stored result. The access is on record; completing it needs a recorded "
                                      f"owner decision '{EXCEPTIONAL_RERUN}:{holdout_id}'")
    if status != HOLDOUT_AUTHORIZED:
        raise HoldoutNotAuthorized(f"holdout {holdout_id} is {status}: a recorded owner authorization is required")
    auth = st["authorization"]
    drift = [k for k, v in (("specification_sha256", specification_sha256), ("dataset_hash", dataset_hash),
                            ("source_fingerprint", source_fingerprint)) if auth.get(k) != v]
    if drift:
        raise HoldoutNotAuthorized(f"the authorization does not cover this evaluation: {drift} changed since it "
                                   "was given (a new readiness check and a new authorization are needed)")
    # the append takes the register's exclusive lock and re-checks the state machine: of two workers, one wins
    rec = register.append(HOLDOUT_OPENED, {"holdout_id": holdout_id, "run_id": run_id,
                                           "specification_sha256": specification_sha256, "dataset_hash": dataset_hash,
                                           "source_fingerprint": source_fingerprint, "code_commit": code_commit,
                                           "authorization_readiness_hash": auth["readiness_hash"]}, now=now)
    return HoldoutAccess(holdout_id, run_id, st["start"], st["end"], rec["record_hash"])


def burn_holdout(register: ResearchRegister, access: HoldoutAccess, *, result_hash: str, verdict: str,
                 artifact_hashes: Optional[Mapping[str, str]] = None, now: Optional[str] = None) -> Dict[str, Any]:
    return register.append(HOLDOUT_BURNED, {"holdout_id": access.holdout_id, "run_id": access.run_id,
                                            "result_hash": result_hash, "verdict": verdict,
                                            "artifact_hashes": dict(artifact_hashes or {})}, now=now)


__all__ = ["HoldoutAccess", "HoldoutNotAuthorized", "HoldoutAlreadyBurned", "HoldoutAccessIncomplete",
           "READY", "NOT_READY", "REQUIREMENTS", "READINESS_VERSION", "EXCEPTIONAL_RERUN", "holdout_id_for",
           "reserve_holdout", "pre_holdout_readiness", "record_owner_holdout_authorization", "open_holdout_once", "burn_holdout"]
