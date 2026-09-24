"""Promotion gates A-I and the overall certification status (22.26-22.28).

No pass-by-averaging: every gate is mandatory and evaluated on its own.
Excellent expectancy never offsets a lookahead violation; a great backtest
never offsets a failed holdout; crypto evidence never certifies Forex.

Overall status, in priority order:

1. any mandatory gate FAIL                     -> NOT_CERTIFIED
2. a gate BLOCKED for DATA reasons             -> BLOCKED_BY_DATA
3. any historical gate (A-G) not PASS          -> INSUFFICIENT_EVIDENCE
   (includes CERTIFICATION_POLICY_INCOMPLETE -- never an automatic pass)
4. forward demo (H) not PASS                   -> FORWARD_DEMO_REQUIRED
5. operational (I) not PASS                    -> INSUFFICIENT_EVIDENCE
6. otherwise                                   -> CERTIFIED (for the stated scope ONLY)
"""
from __future__ import annotations

from typing import Any, List, Mapping, Optional, Sequence, Tuple

from .contracts import CertReason as R, Gate, GateResult, GateStatus as G, OverallCertificationStatus as O
from .policy import CertificationPolicy

DATA_REASONS = frozenset({R.NO_REAL_DATA.value, R.SYNTHETIC_SOURCE_REFUSED.value, R.INSUFFICIENT_COVERAGE.value})
HISTORICAL_GATES = tuple(g.value for g in (Gate.A_INTEGRITY, Gate.B_NET_EXPECTANCY, Gate.C_COST_STRESS,
                                           Gate.D_HOLDOUT, Gate.E_CONCENTRATION, Gate.F_CALIBRATION,
                                           Gate.G_EVIDENCE_COUNT))


def _incomplete(policy: CertificationPolicy, gate: str, *names: str, observed=None) -> Optional[GateResult]:
    missing = policy.missing(*names)
    if missing:
        return GateResult(gate, G.BLOCKED.value, (R.POLICY_INCOMPLETE.value, *missing), observed=observed or {})
    return None


def _approved(primary: Optional[Mapping[str, Any]]) -> Tuple[int, Optional[Mapping[str, Any]]]:
    if not primary:
        return 0, None
    pop = primary["populations"]["APPROVED"]
    return pop["n"], pop


def gate_integrity(integrity: Mapping[str, Any], policy: CertificationPolicy) -> GateResult:
    obs = {k: integrity.get(k) for k in ("lookahead_violations", "determinism_ok", "manifest_valid",
                                         "certifiable_source", "causal_feature_path_verified")}
    if (integrity.get("lookahead_violations") or 0) > policy.max_lookahead_violations.value:
        return GateResult(Gate.A_INTEGRITY.value, G.FAIL.value, (R.LOOKAHEAD_VIOLATION.value,), obs)
    if integrity.get("determinism_ok") is False:
        return GateResult(Gate.A_INTEGRITY.value, G.FAIL.value, (R.REPLAY_HASH_UNSTABLE.value,), obs)
    if integrity.get("manifest_valid") is False:
        return GateResult(Gate.A_INTEGRITY.value, G.FAIL.value, (R.MANIFEST_INVALID.value,), obs)
    if not integrity.get("real_data_present"):
        return GateResult(Gate.A_INTEGRITY.value, G.BLOCKED.value, (R.NO_REAL_DATA.value,), obs)
    if not integrity.get("certifiable_source"):
        return GateResult(Gate.A_INTEGRITY.value, G.BLOCKED.value, (R.SYNTHETIC_SOURCE_REFUSED.value,), obs)
    if integrity.get("determinism_ok") is None or not integrity.get("causal_feature_path_verified"):
        return GateResult(Gate.A_INTEGRITY.value, G.INSUFFICIENT_EVIDENCE.value, ("INTEGRITY_NOT_VERIFIED",), obs)
    return GateResult(Gate.A_INTEGRITY.value, G.PASS.value, (), obs)


def gate_net_expectancy(primary, policy: CertificationPolicy) -> GateResult:
    gate = Gate.B_NET_EXPECTANCY.value
    if primary is None:
        return GateResult(gate, G.BLOCKED.value, (R.NO_REAL_DATA.value,))
    n, pop = _approved(primary)
    if n == 0:
        return GateResult(gate, G.INSUFFICIENT_EVIDENCE.value, (R.NO_ACCEPTED_EVIDENCE.value,), {"approved": 0})
    net = pop["net"]
    obs = {"n": n, "mean_R": net.get("mean_R"), "p_expectancy_positive": net.get("p_expectancy_positive"),
           "ci": [net.get("ci_low"), net.get("ci_high")]}
    if net.get("mean_R") is None or net["mean_R"] <= policy.min_net_expectancy_R.value:
        return GateResult(gate, G.FAIL.value, (R.NEGATIVE_EXPECTANCY.value,), obs)
    blocked = _incomplete(policy, gate, "min_probability_expectancy_positive", observed=obs)
    if blocked:
        return blocked
    if (net.get("p_expectancy_positive") or 0.0) < policy.min_probability_expectancy_positive.value:
        return GateResult(gate, G.FAIL.value, (R.LOW_PROBABILITY_POSITIVE.value,), obs)
    return GateResult(gate, G.PASS.value, (), obs)


def gate_cost_stress(primary, policy: CertificationPolicy) -> GateResult:
    gate = Gate.C_COST_STRESS.value
    if primary is None:
        return GateResult(gate, G.BLOCKED.value, (R.NO_REAL_DATA.value,))
    stress = primary["cost_stress"]
    at = lambda m: stress[str(float(m))]["reselected"]["APPROVED"]  # noqa: E731
    obs = {m: {"n": at(m)["n"], "mean_R": at(m)["net"].get("mean_R")} for m in ("1.0", "1.5", "2.0")}
    if at(1.5)["n"] == 0:
        return GateResult(gate, G.INSUFFICIENT_EVIDENCE.value, (R.NO_ACCEPTED_EVIDENCE.value,), obs)
    if (at(1.5)["net"].get("mean_R") or 0.0) <= policy.min_net_expectancy_R_at_1_5x.value:
        return GateResult(gate, G.FAIL.value, (R.COST_STRESS_NOT_CREDIBLE.value,), obs)
    blocked = _incomplete(policy, gate, "catastrophic_floor_R_at_2x", observed=obs)
    if blocked:
        return blocked
    if at(2.0)["n"] and (at(2.0)["net"].get("mean_R") or 0.0) < policy.catastrophic_floor_R_at_2x.value:
        return GateResult(gate, G.FAIL.value, (R.COST_STRESS_CATASTROPHIC.value,), obs)
    return GateResult(gate, G.PASS.value, (), obs)


def gate_holdout(holdout: Optional[Mapping[str, Any]], policy: CertificationPolicy) -> GateResult:
    gate = Gate.D_HOLDOUT.value
    if holdout is None:
        return GateResult(gate, G.BLOCKED.value, (R.HOLDOUT_NOT_RUN.value,))
    if holdout.get("status") == "FAIL":
        return GateResult(gate, G.FAIL.value, tuple(holdout.get("reason_codes") or ()) or ("HOLDOUT_FAILED",))
    metrics = holdout.get("metrics") or {}
    n, pop = _approved(metrics) if metrics else (0, None)
    if n == 0:
        return GateResult(gate, G.INSUFFICIENT_EVIDENCE.value, (R.NO_ACCEPTED_EVIDENCE.value,))
    mean = pop["net"].get("mean_R")
    dd = pop["drawdown_tail"].get("max_drawdown_R")
    obs = {"n": n, "mean_R": mean, "max_drawdown_R": dd}
    if mean is None or mean <= policy.min_net_expectancy_R.value:
        return GateResult(gate, G.FAIL.value, (R.NEGATIVE_EXPECTANCY.value,), obs)
    blocked = _incomplete(policy, gate, "max_holdout_drawdown_R", observed=obs)
    if blocked:
        return blocked
    if dd is not None and dd > policy.max_holdout_drawdown_R.value:
        return GateResult(gate, G.FAIL.value, (R.HOLDOUT_DRAWDOWN.value,), obs)
    return GateResult(gate, G.PASS.value, (), obs)


def gate_concentration(primary, policy: CertificationPolicy) -> GateResult:
    gate = Gate.E_CONCENTRATION.value
    if primary is None:
        return GateResult(gate, G.BLOCKED.value, (R.NO_REAL_DATA.value,))
    n, _ = _approved(primary)
    conc = primary["concentration"]
    obs = {k: {"largest_segment": v["largest_segment"], "largest_positive_share": v["largest_positive_share"]}
           for k, v in conc["by"].items()}
    obs["population"] = conc["population"]
    if n == 0:
        return GateResult(gate, G.INSUFFICIENT_EVIDENCE.value, (R.NO_ACCEPTED_EVIDENCE.value,), obs)
    blocked = _incomplete(policy, gate, "max_single_segment_positive_share", observed=obs)
    if blocked:
        return blocked
    worst = [k for k, v in conc["by"].items()
             if (v["largest_positive_share"] or 0.0) > policy.max_single_segment_positive_share.value]
    if worst:
        return GateResult(gate, G.FAIL.value, (R.CONCENTRATION_EXCEEDED.value, *worst), obs)
    return GateResult(gate, G.PASS.value, (), obs)


def gate_calibration(calibration: Optional[Mapping[str, Any]], policy: CertificationPolicy) -> GateResult:
    """Both the library's walk-forward calibration (existing CalibrationPolicy)
    and the replay's out-of-sample forecast calibration must hold."""
    from app.trading_intelligence.forecast.calibration_report import CalibrationPolicy

    gate = Gate.F_CALIBRATION.value
    if not calibration:
        return GateResult(gate, G.BLOCKED.value, (R.NO_REAL_DATA.value,))
    cp = CalibrationPolicy()
    obs = {"library": calibration.get("library"), "replay": {k: (calibration.get("replay") or {}).get(k)
                                                              for k in ("n", "brier", "log_score", "ece", "brier_skill")}}
    reasons: List[str] = []
    insufficient = False
    for name in ("library", "replay"):
        c = calibration.get(name) or {}
        n = c.get("n") or c.get("n_evaluated") or 0
        if n < cp.min_calibrated_samples:
            insufficient = True
            reasons.append(f"{name.upper()}_{R.INSUFFICIENT_SAMPLES.value}")
            continue
        if c.get("ece") is None or c["ece"] > cp.max_ece:
            reasons.append(f"{name.upper()}_ECE_ABOVE_{cp.max_ece}")
        if c.get("brier_skill") is None or c["brier_skill"] < cp.min_brier_skill:
            reasons.append(f"{name.upper()}_BRIER_SKILL_BELOW_{cp.min_brier_skill}")
    small_families = [f for f, v in ((calibration.get("library") or {}).get("by_setup_family") or {}).items()
                      if (v or {}).get("n", 0) < cp.min_family_samples]
    if small_families:
        insufficient = True
        reasons.append("FAMILY_SAMPLES_BELOW_" + str(cp.min_family_samples))
    if any("ECE" in r or "SKILL" in r for r in reasons):
        return GateResult(gate, G.FAIL.value, (R.CALIBRATION_FAILED.value, *reasons), obs)
    if insufficient:
        return GateResult(gate, G.INSUFFICIENT_EVIDENCE.value, tuple(reasons), obs)
    return GateResult(gate, G.PASS.value, (), obs)


def gate_evidence_count(primary, policy: CertificationPolicy) -> GateResult:
    gate = Gate.G_EVIDENCE_COUNT.value
    if primary is None:
        return GateResult(gate, G.BLOCKED.value, (R.NO_REAL_DATA.value,))
    n, _ = _approved(primary)
    obs = {"approved": n, "admissible": primary["counts"]["admissible"], "candidates": primary["counts"]["candidates"]}
    if n == 0:
        return GateResult(gate, G.INSUFFICIENT_EVIDENCE.value, (R.NO_ACCEPTED_EVIDENCE.value,), obs)
    blocked = _incomplete(policy, gate, "min_accepted_evidence_count", observed=obs)
    if blocked:
        return blocked
    if n < policy.min_accepted_evidence_count.value:
        return GateResult(gate, G.INSUFFICIENT_EVIDENCE.value, (R.INSUFFICIENT_SAMPLES.value, "EXTEND_DATA_NEVER_LOWER_FLOOR"), obs)
    return GateResult(gate, G.PASS.value, (), obs)


def gate_forward_demo(tracker: Optional[Mapping[str, Any]], policy: CertificationPolicy) -> GateResult:
    gate = Gate.H_FORWARD_DEMO.value
    if not tracker:
        return GateResult(gate, G.INSUFFICIENT_EVIDENCE.value, ("NO_FORWARD_DEMO_EVIDENCE",))
    obs = {k: tracker.get(k) for k in ("elapsed_days", "executed_count", "trade_plans", "status")}
    if (tracker.get("elapsed_days") or 0.0) < policy.min_forward_demo_days.value:
        return GateResult(gate, G.INSUFFICIENT_EVIDENCE.value, (R.FORWARD_DEMO_DAYS.value,), obs)
    blocked = _incomplete(policy, gate, "min_forward_demo_executed_count", observed=obs)
    if blocked:
        return blocked
    if (tracker.get("executed_count") or 0) < policy.min_forward_demo_executed_count.value:
        return GateResult(gate, G.INSUFFICIENT_EVIDENCE.value, (R.FORWARD_DEMO_EVIDENCE.value, "EXTEND_PERIOD"), obs)
    return GateResult(gate, G.PASS.value, (), obs)


def gate_operational(defects: Optional[Mapping[str, Any]], policy: CertificationPolicy) -> GateResult:
    gate = Gate.I_OPERATIONAL.value
    if defects is None:
        return GateResult(gate, G.INSUFFICIENT_EVIDENCE.value, ("OPERATIONAL_EVIDENCE_UNAVAILABLE",))
    if (defects.get("total") or 0) > policy.max_operational_defects.value:
        return GateResult(gate, G.FAIL.value, (R.OPERATIONAL_DEFECT.value,), defects)
    return GateResult(gate, G.PASS.value, (), defects)


def evaluate_gates(*, integrity: Mapping[str, Any], primary: Optional[Mapping[str, Any]],
                   holdout: Optional[Mapping[str, Any]], calibration: Optional[Mapping[str, Any]],
                   forward_demo: Optional[Mapping[str, Any]], operational: Optional[Mapping[str, Any]],
                   policy: CertificationPolicy) -> Tuple[GateResult, ...]:
    return (gate_integrity(integrity, policy), gate_net_expectancy(primary, policy), gate_cost_stress(primary, policy),
            gate_holdout(holdout, policy), gate_concentration(primary, policy), gate_calibration(calibration, policy),
            gate_evidence_count(primary, policy), gate_forward_demo(forward_demo, policy),
            gate_operational(operational, policy))


def overall_status(gates: Sequence[GateResult]) -> Tuple[str, Tuple[str, ...]]:
    by = {g.gate: g for g in gates}
    missing = [g for g in (x.value for x in Gate) if g not in by]
    if missing:
        raise ValueError(f"every mandatory gate must be evaluated: missing {missing}")
    failed = [g.gate for g in gates if g.status == G.FAIL.value]
    if failed:
        return O.NOT_CERTIFIED.value, tuple(failed)
    data = [g.gate for g in gates if g.status == G.BLOCKED.value and DATA_REASONS & set(g.reason_codes)]
    if data:
        return O.BLOCKED_BY_DATA.value, tuple(data)
    hist = [g for g in HISTORICAL_GATES if by[g].status != G.PASS.value]
    if hist:
        return O.INSUFFICIENT_EVIDENCE.value, tuple(hist)
    if by[Gate.H_FORWARD_DEMO.value].status != G.PASS.value:
        return O.FORWARD_DEMO_REQUIRED.value, (Gate.H_FORWARD_DEMO.value,)
    if by[Gate.I_OPERATIONAL.value].status != G.PASS.value:
        return O.INSUFFICIENT_EVIDENCE.value, (Gate.I_OPERATIONAL.value,)
    return O.CERTIFIED.value, ()


def ready_for_forward_demo(gates: Sequence[GateResult]) -> bool:
    """Historical gates required BEFORE forward operation. Never enables
    CATI_ACTIVE_EXECUTION_ENABLED (Section 25 owns promotion)."""
    by = {g.gate: g for g in gates}
    return all(by[g].status == G.PASS.value for g in HISTORICAL_GATES)


__all__ = ["evaluate_gates", "overall_status", "ready_for_forward_demo", "HISTORICAL_GATES", "DATA_REASONS"]
