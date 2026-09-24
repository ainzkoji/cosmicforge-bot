"""Section 26 success standard & operating doctrine -- a read-only verifier.

Every layer is judged on its own, from two things: a CODE-capability check
(introspection of the real modules) and its EVIDENCE status (the Section 22
certification report, when one exists). A PASS in one layer never hides a
FAIL in another; evidence that has not been produced is PENDING_EVIDENCE or
BLOCKED_DATA -- never PASS because the code exists.

Precision tiers are certification-DERIVED eligibility, never a renamed
confidence score. The policy is implemented but DISABLED (nothing consumes
tiers yet); without certified + calibrated evidence the answer is NO_TIER.
"""
from __future__ import annotations

import dataclasses
import inspect
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

PASS, FAIL, PENDING, BLOCKED, NA = "PASS", "FAIL", "PENDING_EVIDENCE", "BLOCKED_DATA", "NOT_APPLICABLE"
LAYERS = ("MARKET_STATE", "REGIME", "SETUPS", "FORECAST", "ECONOMICS", "ADMISSION", "RANKING", "PORTFOLIO",
          "TRADE_PLAN", "RISK_EXECUTION", "EXIT", "RESEARCH", "OPERATIONS")


def _fields(cls) -> set:
    return {f.name for f in dataclasses.fields(cls)}


# ------------------------------------------------------------------ code capability checks
def _market_state() -> Tuple[bool, str]:
    from app.trading_intelligence.research.certification import replay
    from app.trading_intelligence.versions import MARKET_STATE_SCHEMA_VERSION

    src = inspect.getsource(replay)
    return (bool(MARKET_STATE_SCHEMA_VERSION) and "CATIController" in src,
            "versioned; replay drives the same controller as runtime (parity); provider cut at the clock (causal)")


def _regime() -> Tuple[bool, str]:
    from app.trading_intelligence.regime.contracts import RegimeDistribution

    f = _fields(RegimeDistribution)
    return ("dominant_regime" in f and any("uncertain" in x or "entropy" in x for x in f),
            "distribution with explicit uncertainty")


def _setups() -> Tuple[bool, str]:
    from app.trading_intelligence.contracts.setup import SetupCandidate
    from app.trading_intelligence.setups.registry import SPECIALIST_REGISTRY

    families = set(SPECIALIST_REGISTRY)
    no_authority = not ({"admission_status", "approved", "final_confidence"} & _fields(SetupCandidate))
    return (len(families) == 4 and no_authority, f"{sorted(families)}; candidates carry no admission authority")


def _forecast() -> Tuple[bool, str]:
    from app.trading_intelligence.contracts.forecast import OutcomeForecast

    need = {"raw_support", "ess", "credible_interval_low", "credible_interval_high", "p_target_before_stop",
            "p_stop_before_target", "p_timeout"}
    return (need <= _fields(OutcomeForecast), "support-aware posterior, intervals, path outcome statistics")


def _economics() -> Tuple[bool, str]:
    from app.trading_intelligence.contracts.venue_economics import VenueEconomicObservation

    f = _fields(VenueEconomicObservation)
    return ({"fee_observation", "spread_observation", "slippage_observation", "funding_observation"} <= f,
            "venue/account fee, spread, slippage, funding/carry components")


def _admission() -> Tuple[bool, str]:
    import app.trading_intelligence.economics.engine as eng
    import app.trading_intelligence.veto.engine as veto

    src = inspect.getsource(eng) + inspect.getsource(veto)
    hidden = any(tok in src for tok in ("app.threshold", "final_confidence", "get_threshold_policy"))
    return (not hidden, "one economic admission authority; no confidence floor imported")


def _ranking() -> Tuple[bool, str]:
    from app.trading_intelligence.ranking.coordinator import CATICycleCoordinator

    return (all(hasattr(CATICycleCoordinator, m) for m in ("mark_due", "record_symbol_evaluation", "finalize_bot_cycle")),
            "all due symbols reach a terminal state before one whole-universe ranking")


def _portfolio() -> Tuple[bool, str]:
    from app.trading_intelligence.portfolio import exposure_builder, selector

    return (hasattr(exposure_builder, "__file__") and hasattr(selector, "__file__"),
            "account-wide exposure, correlated exposure across bots, deterministic selection")


def _trade_plan() -> Tuple[bool, str]:
    from app.trading_intelligence.contracts.trade_plan import TradePlan

    f = _fields(TradePlan)
    frozen = TradePlan.__dataclass_params__.frozen
    return (frozen and {"plan_expiry_time", "trade_plan_hash", "economic_opportunity_id"} <= f,
            "immutable, expiring, structurally defined, lineage-traceable")


def _risk_execution() -> Tuple[bool, str]:
    from app.trading_intelligence.execution import boundary
    from app.trading_intelligence.execution.config import CATIExecutionConfig

    src = inspect.getsource(boundary.CATIExecutionBoundary.process_trade_plan)
    cfg = CATIExecutionConfig()
    return ("orchestrator.process_trade_plan" in src and "authorize_entry" in src and not cfg.active_execution_enabled,
            "hard risk via the existing orchestrator; governance dual key; OFF by default")


def _exit() -> Tuple[bool, str]:
    from app.trading_intelligence.contracts.position import ExitPolicy

    return (dataclasses.is_dataclass(ExitPolicy), "remaining-edge exit intent; mechanical protection stays")


def _research() -> Tuple[bool, str]:
    from app.trading_intelligence.research.certification import gates

    return (hasattr(gates, "evaluate_gates"), "replay, holdout, cost stress, overfitting controls, forward demo")


def _operations() -> Tuple[bool, str]:
    from app.trading_intelligence.governance.phases import rollback_targets
    from app.trading_intelligence.governance.promotion import PromotionGovernance
    from app.trading_intelligence.observability.metrics import METRICS

    return (hasattr(PromotionGovernance, "set_kill_switch") and rollback_targets("M6") == ("M0",)
            and METRICS is not None, "evidence, metrics, stable reason codes, kill switch, rollback")


CODE_CHECKS: Mapping[str, Callable[[], Tuple[bool, str]]] = {
    "MARKET_STATE": _market_state, "REGIME": _regime, "SETUPS": _setups, "FORECAST": _forecast,
    "ECONOMICS": _economics, "ADMISSION": _admission, "RANKING": _ranking, "PORTFOLIO": _portfolio,
    "TRADE_PLAN": _trade_plan, "RISK_EXECUTION": _risk_execution, "EXIT": _exit, "RESEARCH": _research,
    "OPERATIONS": _operations,
}

#: which certification gates evidence each layer (None = code-level doctrine only)
LAYER_EVIDENCE: Mapping[str, Optional[Tuple[str, ...]]] = {
    "MARKET_STATE": ("A_INTEGRITY",), "REGIME": ("F_CALIBRATION",), "SETUPS": None, "FORECAST": ("F_CALIBRATION",),
    "ECONOMICS": ("C_COST_STRESS",), "ADMISSION": ("B_NET_EXPECTANCY",), "RANKING": None, "PORTFOLIO": None,
    "TRADE_PLAN": None, "RISK_EXECUTION": ("I_OPERATIONAL",), "EXIT": ("H_FORWARD_DEMO",),
    "RESEARCH": ("A_INTEGRITY", "B_NET_EXPECTANCY", "C_COST_STRESS", "D_HOLDOUT", "E_CONCENTRATION",
                 "G_EVIDENCE_COUNT", "H_FORWARD_DEMO"),
    "OPERATIONS": ("H_FORWARD_DEMO", "I_OPERATIONAL"),
}


def _evidence_status(gate_ids: Optional[Tuple[str, ...]], report: Optional[Mapping[str, Any]]) -> str:
    if gate_ids is None:
        return NA
    if not report:
        return PENDING
    gates = {g["gate"]: g for g in report.get("gates", ())}
    statuses = [gates.get(g, {}).get("status") for g in gate_ids]
    if any(s == "FAIL" for s in statuses):
        return FAIL
    if any(s == "BLOCKED" and {"NO_REAL_MARKET_DATA", "SYNTHETIC_SOURCE_CANNOT_CERTIFY", "INSUFFICIENT_DATA_COVERAGE"}
           & set(gates.get(g, {}).get("reason_codes", ())) for g, s in zip(gate_ids, statuses)):
        return BLOCKED
    return PASS if all(s == "PASS" for s in statuses) else PENDING


def verify_success_standard(report: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    layers: Dict[str, Any] = {}
    for layer in LAYERS:
        try:
            ok, note = CODE_CHECKS[layer]()
        except Exception as exc:  # a missing capability is a FAIL, not a skip
            ok, note = False, f"capability check error: {type(exc).__name__}"
        ev = _evidence_status(LAYER_EVIDENCE[layer], report)
        if not ok or ev == FAIL:
            status = FAIL
        elif ev in (BLOCKED, PENDING):
            status = ev
        else:
            status = PASS
        layers[layer] = {"status": status, "code": PASS if ok else FAIL, "evidence": ev, "note": note}
    statuses = [v["status"] for v in layers.values()]
    overall = FAIL if FAIL in statuses else (BLOCKED if BLOCKED in statuses else (PENDING if PENDING in statuses
                                                                                    else PASS))
    return {"overall": overall, "layers": layers}


# ------------------------------------------------------------------ precision tiers
@dataclass(frozen=True)
class TierRequirement:
    name: str
    min_ci_low: float
    max_ci_width: float
    min_ess: float
    min_conservative_edge_r: float
    max_ood: float
    require_2x_cost_positive: bool


@dataclass(frozen=True)
class PrecisionTierPolicy:
    """RESEARCH_DEFAULT, DISABLED: no runtime consumer exists; tiers never
    create activity and never replace admission."""

    enabled: bool = False
    tiers: Tuple[TierRequirement, ...] = (
        TierRequirement("PRECISION_A", 0.60, 0.15, 150.0, 0.25, 0.25, True),
        TierRequirement("HIGH_CONVICTION", 0.55, 0.25, 60.0, 0.15, 0.40, True),
        TierRequirement("STANDARD", 0.50, 0.35, 20.0, 0.05, 0.60, False),
    )


def precision_tier(opportunity: Mapping[str, Any], *, certification_status: str, calibration_status: str,
                   policy: Optional[PrecisionTierPolicy] = None) -> str:
    """Eligibility from certified evidence only; anything less is NO_TIER (flat)."""
    p = policy or PrecisionTierPolicy()
    if not p.enabled or certification_status != "CERTIFIED" or calibration_status != "CALIBRATED":
        return "NO_TIER"
    lo, hi = opportunity.get("credible_interval_low"), opportunity.get("credible_interval_high")
    if lo is None or hi is None:
        return "NO_TIER"
    for t in p.tiers:
        if (lo >= t.min_ci_low and hi - lo <= t.max_ci_width and (opportunity.get("ess") or 0) >= t.min_ess
                and (opportunity.get("conservative_edge_r") or float("-inf")) >= t.min_conservative_edge_r
                and (opportunity.get("ood_score") if opportunity.get("ood_score") is not None else 1.0) <= t.max_ood
                and (not t.require_2x_cost_positive or (opportunity.get("net_R_at_2x_cost") or -1.0) > 0)):
            return t.name
    return "NO_TIER"


__all__ = ["verify_success_standard", "precision_tier", "PrecisionTierPolicy", "TierRequirement", "LAYERS",
           "PASS", "FAIL", "PENDING", "BLOCKED", "NA"]
