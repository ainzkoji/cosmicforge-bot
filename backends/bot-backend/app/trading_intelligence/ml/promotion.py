"""The explicit CATI model-evidence promotion gate + runtime estimator
authority (Section 23.14).

Promotion makes an estimator eligible to replace ONE deterministic
estimator behind CATI -- never a trading authority. Runtime use requires ALL
of: ``CATI_ML_ENABLED``, registry status PROMOTED, and a Section 25 phase
that grants CATI authority for the scope. Otherwise (or on any failure) the
approved deterministic CATI estimator is used -- never V2.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .contracts import ModelRole, ModelStatus
from .training import MIN_SAMPLES

BINARY_ROLES = (ModelRole.OUTCOME.value,)
MIN_SHADOW_PREDICTIONS = 200  # RESEARCH_DEFAULT: realized shadow comparisons before promotion
MIN_ML_GOVERNANCE_PHASE = "M6"


@dataclass(frozen=True)
class PromotionDecision:
    role: str
    eligible: bool
    checks: Mapping[str, Any]
    reasons: Tuple[str, ...]


def evaluate_model_promotion(card: Mapping[str, Any], *, artifact_verified: bool, reproducible: bool,
                             ood_behaviour_tested: bool, holdout_untouched: bool,
                             shadow: Optional[Mapping[str, Any]], section22_ready_for_ml: bool,
                             governance_phase: str) -> PromotionDecision:
    from app.trading_intelligence.forecast.calibration_report import CalibrationPolicy

    from ..governance.phases import phase_index

    role = card["role"]
    m = card.get("metrics") or {}
    reasons: List[str] = []
    checks: Dict[str, Any] = {
        "real_source": card.get("source_kind") == "REAL_MARKET", "artifact_verified": artifact_verified,
        "reproducible": reproducible, "ood_behaviour_tested": ood_behaviour_tested,
        "holdout_untouched": holdout_untouched, "section22_ready_for_ml": section22_ready_for_ml,
        "n_fit": m.get("n_fit", 0), "n_holdout": m.get("n_holdout", 0), "governance_phase": governance_phase,
    }
    for key, reason in (("real_source", "NON_REAL_TRAINING_DATA"), ("artifact_verified", "ARTIFACT_INTEGRITY"),
                        ("reproducible", "NOT_REPRODUCIBLE"), ("ood_behaviour_tested", "OOD_BEHAVIOUR_UNTESTED"),
                        ("holdout_untouched", "HOLDOUT_NOT_UNTOUCHED"),
                        ("section22_ready_for_ml", "DETERMINISTIC_CATI_NOT_CERTIFIED_FOR_ML")):
        if not checks[key]:
            reasons.append(reason)
    if (m.get("n_fit") or 0) < MIN_SAMPLES[role]:
        reasons.append("INSUFFICIENT_TRAINING_EVIDENCE")
    if role in BINARY_ROLES:
        cp = CalibrationPolicy()
        cal = m.get("holdout_calibration") or {}
        checks["holdout_ece"] = cal.get("ece")
        if (cal.get("n") or 0) < cp.min_calibrated_samples or cal.get("ece") is None or cal["ece"] > cp.max_ece:
            reasons.append("NOT_CALIBRATED")
    sh = shadow or {}
    checks["shadow_n"] = sh.get("n", 0)
    if sh.get("n", 0) < MIN_SHADOW_PREDICTIONS:
        reasons.append("INSUFFICIENT_SHADOW_EVIDENCE")
    elif sh.get("ml_not_worse") is False:
        reasons.append("SHADOW_WORSE_THAN_DETERMINISTIC")
    if phase_index(governance_phase) < phase_index(MIN_ML_GOVERNANCE_PHASE):
        reasons.append(f"GOVERNANCE_PHASE_BELOW_{MIN_ML_GOVERNANCE_PHASE}")
    return PromotionDecision(role, not reasons, checks, tuple(reasons))


def estimator_authority(role: str, *, registry: Any, governance_authorizes_cati: bool,
                        config: Any = None) -> Dict[str, Any]:
    """Which estimator a CATI decision may use for ``role`` right now."""
    from .config import CATIMLConfig

    cfg = config or CATIMLConfig.from_env()
    if not cfg.ml_enabled:
        return {"role": role, "estimator": "DETERMINISTIC", "reason": "CATI_ML_DISABLED"}
    if not governance_authorizes_cati:
        return {"role": role, "estimator": "DETERMINISTIC", "reason": "GOVERNANCE_NOT_AUTHORIZED"}
    promoted = registry.promoted(role)
    if not promoted:
        return {"role": role, "estimator": "DETERMINISTIC", "reason": "NO_PROMOTED_MODEL"}
    return {"role": role, "estimator": "ML", "model_id": promoted[-1]["model_id"],
            "fallback": "DETERMINISTIC_CATI_ESTIMATOR_NEVER_V2"}


def role_status_report(registry: Any = None, gates: Optional[Mapping[str, Any]] = None) -> Dict[str, Dict[str, str]]:
    """Per-role training / promotion status (no all-or-nothing)."""
    out = {}
    for role in ModelRole:
        g = (gates or {}).get(role.value)
        training = getattr(g, "status", None) or "BLOCKED_EVIDENCE"
        models = registry.models(role.value) if registry is not None else []
        status = models[-1]["status"] if models else "NOT_TRAINED"
        promotion = "PROMOTED" if status == ModelStatus.PROMOTED.value else (
            "NOT_READY" if status != ModelStatus.PROMOTION_ELIGIBLE.value else "PROMOTION_ELIGIBLE")
        out[role.value] = {"training": training, "model": status, "promotion": promotion,
                           "reasons": ",".join(getattr(g, "reasons", ()) or ("NO_REAL_EVIDENCE",))}
    return out


__all__ = ["evaluate_model_promotion", "estimator_authority", "role_status_report", "PromotionDecision",
           "MIN_SHADOW_PREDICTIONS", "MIN_ML_GOVERNANCE_PHASE"]
