"""The rule-based admission route (Section H, Step 2.1 / Section L) -- defined, NOT activated.

CATI's M5 -> M6 evidence requires seven replay gates, one of which (F_CALIBRATION) judges a calibrated
per-trade forecast. A deterministic rule has no forecast to calibrate, so on the existing route it can never
be eligible, however good its evidence. Section L of the master plan discusses admitting such a family on its
whole-portfolio evidence instead. That decision is not in the repository, so this module only DEFINES the
route:

* it is evaluated solely from recorded evidence (register, certification result, holdout record);
* it removes nothing: registration, hash pins, frozen dataset, costs, causality, the untouched holdout, the
  frozen mandate's own pass rule AND the portfolio-level statistical gate are all still required;
* without a recorded owner decision ``RULE_BASED_PORTFOLIO_ADMISSION_V1 = APPROVED`` the answer is
  BLOCKED_PENDING_APPROVAL -- for a passing family as much as for a failing one;
* its best outcome is ELIGIBLE_FOR_GOVERNANCE_REVIEW. It never moves a governance phase, enables an entry,
  selects an account or touches a flag: ``PromotionGovernance.transition`` stays the only way a phase changes,
  and its M6 evidence check is unchanged here.
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping

from .register import HOLDOUT_BURNED, RegisterError, ResearchRegister
from .statistics import PASS

ROUTE_ID = "RULE_BASED_PORTFOLIO_ADMISSION_V1"
ELIGIBLE = "ELIGIBLE_FOR_GOVERNANCE_REVIEW"
NOT_ELIGIBLE = "NOT_ELIGIBLE"
BLOCKED = "BLOCKED_PENDING_APPROVAL"
#: what the route demands of a certification result, and the reason recorded when it is absent
EVIDENCE_REQUIREMENTS = (
    ("mandate_verified", "MANDATE_HASH_NOT_VERIFIED"),
    ("dataset_frozen", "DATASET_NOT_FROZEN"),
    ("data_quality_ok", "DATA_QUALITY_NOT_ACCEPTABLE"),
    ("causality_verified", "CAUSALITY_NOT_VERIFIED"),
    ("costs_applied", "RESULT_IS_NOT_NET_OF_COSTS"),
    ("risk_limits_applied", "RISK_LIMITS_NOT_APPLIED"),
)


def route_approved(register: ResearchRegister) -> bool:
    decision = register.decision(ROUTE_ID)
    return bool(decision and decision["state"] == "APPROVED")


def evaluate_rule_based_admission(register: ResearchRegister, *, mandate_id: str,
                                  certification: Mapping[str, Any]) -> Dict[str, Any]:
    """``certification`` is the evaluator's machine-readable result. Returns a verdict and every reason."""
    reasons: List[str] = []
    try:
        mandate = register.mandate(mandate_id)
    except RegisterError:
        return {"route": ROUTE_ID, "status": NOT_ELIGIBLE, "reason_codes": ["MANDATE_NOT_REGISTERED"],
                "route_approved": route_approved(register), "promotes": False}
    integrity = dict(certification.get("integrity") or {})
    reasons += [code for key, code in EVIDENCE_REQUIREMENTS if integrity.get(key) is not True]
    if certification.get("mandate_id") != mandate_id or \
            certification.get("specification_sha256") != mandate["specification_sha256"]:
        reasons.append("CERTIFICATION_IS_FOR_ANOTHER_MANDATE_OR_SPECIFICATION")
    holdout = register.holdout(str(certification.get("holdout_id") or ""))
    if holdout["status"] != HOLDOUT_BURNED or holdout.get("mandate_id") != mandate_id:
        reasons.append("HOLDOUT_NOT_EVALUATED_UNDER_RECORDED_AUTHORIZATION")
    elif holdout["burned"]["result_hash"] != certification.get("holdout_result_hash"):
        reasons.append("CERTIFICATION_DOES_NOT_MATCH_THE_STORED_HOLDOUT_RESULT")
    if certification.get("mandate_pass_rule") != PASS:
        reasons.append("FROZEN_MANDATE_PASS_RULE_NOT_PASSED")
    if certification.get("statistical_gate") != PASS:
        reasons.append(f"STATISTICAL_GATE_{certification.get('statistical_gate') or 'NOT_EVALUATED'}")
    if certification.get("verdict") != PASS:
        reasons.append("CERTIFICATION_VERDICT_NOT_PASS")
    approved = route_approved(register)
    if reasons:
        status = NOT_ELIGIBLE
    elif not approved:
        status, reasons = BLOCKED, ["RULE_BASED_ROUTE_NOT_APPROVED_BY_RECORDED_OWNER_DECISION"]
    else:
        status = ELIGIBLE
    return {"route": ROUTE_ID, "status": status, "reason_codes": list(dict.fromkeys(reasons)),
            "route_approved": approved, "promotes": False,
            "note": "eligibility is an input to PromotionGovernance; it never changes a phase or enables trading"}


__all__ = ["ROUTE_ID", "ELIGIBLE", "NOT_ELIGIBLE", "BLOCKED", "EVIDENCE_REQUIREMENTS", "route_approved",
           "evaluate_rule_based_admission"]
