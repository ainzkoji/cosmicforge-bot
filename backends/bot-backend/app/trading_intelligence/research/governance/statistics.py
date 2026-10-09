"""The registered portfolio-level test (Section H, Step 2.1).

What it replaces: the Section 22 gate judges per-trade R for a forecast-driven setup, with a trial count taken
from one database and one dataset hash. A rule-based portfolio has no per-trade forecast to calibrate, and its
trades are neither independent nor the unit in which money is made or lost. The unit of evidence here is the
PORTFOLIO DAILY NET RETURN -- after fees, slippage and funding, across all instruments at once -- so correlation
between instruments and between signals is already inside the series instead of being assumed away.

Method (every number is reported, whatever the verdict):

* serial dependence    -- lag autocorrelations and an effective sample size (initial positive sequence);
* significance         -- CATI's canonical circular block bootstrap (``certification.stats``), one-sided,
                          H0: mean daily net return <= 0;
* multiple testing     -- Bonferroni over EVERY hypothesis in the authoritative register (valid under any
                          dependence between trials; needs no variance of past Sharpe ratios, which the
                          historical record cannot supply). The deflated Sharpe ratio is reported beside it;
* power                -- for a stated target annual Sharpe: the probability this sample would detect it at the
                          adjusted level, and the smallest Sharpe it could detect.

Outcomes: PASS, FAIL, INSUFFICIENT_STATISTICAL_POWER, INSUFFICIENT_DATA, INVALID_EVIDENCE,
BLOCKED_PENDING_APPROVAL. The significance level, the required power and the target effect are GOVERNANCE
thresholds: the frozen mandate does not give them and the master plan is not in the repository, so the default
policy leaves them unset and the gate answers BLOCKED_PENDING_APPROVAL. It never passes by default, and an
underpowered sample never passes at all.
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from statistics import NormalDist
from typing import Any, Dict, List, Optional, Sequence

from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.research.certification import stats as cert_stats

PASS = "PASS"
FAIL = "FAIL"
INSUFFICIENT_STATISTICAL_POWER = "INSUFFICIENT_STATISTICAL_POWER"
INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
INVALID_EVIDENCE = "INVALID_EVIDENCE"
BLOCKED_PENDING_APPROVAL = "BLOCKED_PENDING_APPROVAL"
OUTCOMES = (PASS, FAIL, INSUFFICIENT_STATISTICAL_POWER, INSUFFICIENT_DATA, INVALID_EVIDENCE,
            BLOCKED_PENDING_APPROVAL)
STATISTICAL_GATE_VERSION = "portfolio-statistical-gate-v1"
_N = NormalDist()


@dataclass(frozen=True)
class StatisticalGatePolicy:
    """Thresholds are None until an owner decision supplies them (``approved_by`` + reference)."""

    name: str = "UNAPPROVED_FAIL_CLOSED"
    #: one-sided familywise error rate across the whole register
    familywise_alpha: Optional[float] = None
    #: probability of detecting ``target_annual_sharpe`` that the sample must reach
    required_power: Optional[float] = None
    #: the effect the test must be able to see (annualized Sharpe of daily net returns)
    target_annual_sharpe: Optional[float] = None
    approved_by: Optional[str] = None
    authorization_reference: Optional[str] = None
    # -- methodology (part of the policy identity; not approval thresholds) ---------------------
    #: fewer daily observations than this is not a sample
    min_observations: int = 250
    bootstrap_repetitions: int = 20000
    bootstrap_seed: int = 22022
    periods_per_year: int = 365
    max_autocorrelation_lag: int = 30

    @property
    def approved(self) -> bool:
        return None not in (self.familywise_alpha, self.required_power, self.target_annual_sharpe) and bool(
            self.approved_by and self.authorization_reference)

    def missing(self) -> List[str]:
        out = [n for n in ("familywise_alpha", "required_power", "target_annual_sharpe") if getattr(self, n) is None]
        if not (self.approved_by and self.authorization_reference):
            out.append("recorded_owner_approval")
        return out

    @property
    def policy_hash(self) -> str:
        return stable_hash({"version": STATISTICAL_GATE_VERSION, **asdict(self)})


def unapproved_policy() -> StatisticalGatePolicy:
    return StatisticalGatePolicy()


def autocorrelations(values: Sequence[float], max_lag: int) -> List[float]:
    n = len(values)
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values)
    if var == 0:
        return []
    return [sum((values[i] - mean) * (values[i + k] - mean) for i in range(n - k)) / var
            for k in range(1, min(max_lag, n - 1) + 1)]


def effective_sample_size(values: Sequence[float], max_lag: int) -> Dict[str, Any]:
    """n / (1 + 2 * sum of leading autocorrelations), summed while consecutive PAIRS stay positive (Geyer's
    initial positive sequence). Negative dependence is never credited: the result is capped at n."""
    n = len(values)
    rho = autocorrelations(values, max_lag)
    total, used = 0.0, 0
    for k in range(0, len(rho) - 1, 2):
        pair = rho[k] + rho[k + 1]
        if pair <= 0:
            break
        total += pair
        used = k + 2
    ess = min(float(n), n / (1.0 + 2.0 * total)) if n else 0.0
    return {"n": n, "effective_n": ess, "design_effect": (n / ess) if ess else None, "lags_used": used,
            "rho_1": rho[0] if rho else None}


def power_at(target_annual_sharpe: float, *, effective_years: float, alpha: float) -> float:
    """P(reject H0 | true annual Sharpe) for a one-sided test at level ``alpha``: the Sharpe estimate over
    T years is approximately normal with standard error 1/sqrt(T)."""
    if effective_years <= 0 or not 0 < alpha < 1:
        return 0.0
    return _N.cdf(target_annual_sharpe * math.sqrt(effective_years) - _N.inv_cdf(1.0 - alpha))


def minimum_detectable_sharpe(*, effective_years: float, alpha: float, power: float) -> Optional[float]:
    if effective_years <= 0 or not 0 < alpha < 1 or not 0 < power < 1:
        return None
    return (_N.inv_cdf(1.0 - alpha) + _N.inv_cdf(power)) / math.sqrt(effective_years)


def evaluate_portfolio_gate(daily_net_returns: Sequence[float], *, hypotheses_in_register: int,
                            policy: Optional[StatisticalGatePolicy] = None,
                            expected_observations: Optional[int] = None, evidence_valid: bool = True,
                            invalid_reasons: Sequence[str] = (), trade_count: Optional[int] = None) -> Dict[str, Any]:
    """``hypotheses_in_register`` is ``ResearchRegister.hypothesis_count()`` -- the candidate included, every
    earlier attempt included. ``evidence_valid`` is the evaluator's own integrity verdict (causality audit,
    data quality, hash pins): a statistic computed on invalid evidence is not a statistic."""
    policy = policy or unapproved_policy()
    n_tests = int(hypotheses_in_register)
    out: Dict[str, Any] = {"version": STATISTICAL_GATE_VERSION, "policy": asdict(policy),
                           "policy_hash": policy.policy_hash, "unit_of_evidence": "PORTFOLIO_DAILY_NET_RETURN",
                           "null_hypothesis": "mean daily net return <= 0", "hypotheses_in_register": n_tests,
                           "multiple_testing_method": "BONFERRONI_OVER_FULL_REGISTER", "reason_codes": []}

    def done(status: str, *reasons: str) -> Dict[str, Any]:
        out["status"] = status
        out["reason_codes"] = list(dict.fromkeys([*out["reason_codes"], *reasons]))
        return out

    vals = [float(v) for v in daily_net_returns]
    out["observations"] = len(vals)
    if n_tests < 1:
        return done(INVALID_EVIDENCE, "HYPOTHESIS_NOT_IN_REGISTER")
    if not evidence_valid or any(not math.isfinite(v) for v in vals):
        return done(INVALID_EVIDENCE, *(list(invalid_reasons) or ["NON_FINITE_OR_UNVERIFIED_RETURNS"]))
    if expected_observations is not None and len(vals) != int(expected_observations):
        out["expected_observations"] = int(expected_observations)
        return done(INSUFFICIENT_DATA, "MISSING_PERIODS")
    if len(vals) < policy.min_observations:
        return done(INSUFFICIENT_DATA, "TOO_FEW_OBSERVATIONS")
    if trade_count is not None and trade_count == 0:
        return done(INSUFFICIENT_DATA, "ZERO_TRADES")
    n = len(vals)
    mean = sum(vals) / n
    sd = math.sqrt(sum((v - mean) ** 2 for v in vals) / (n - 1))
    if sd == 0:
        return done(INSUFFICIENT_DATA, "ZERO_VARIANCE")

    ppy = policy.periods_per_year
    ess = effective_sample_size(vals, policy.max_autocorrelation_lag)
    eff_years = ess["effective_n"] / ppy
    boot = cert_stats.expectancy_summary(vals, reps=policy.bootstrap_repetitions, confidence=0.95,
                                         seed=policy.bootstrap_seed)
    # one-sided p: share of resampled means at or below zero; never reported below its own resolution
    p_boot = max(1.0 - boot["p_expectancy_positive"], 1.0 / policy.bootstrap_repetitions)
    z = (mean / sd) * math.sqrt(ess["effective_n"])
    dsr = cert_stats.deflated_sharpe(vals, n_trials=n_tests, trial_sharpe_variance=1.0 / ess["effective_n"])
    out.update({
        "mean_daily_return": mean, "std_daily_return": sd, "annualized_sharpe": mean / sd * math.sqrt(ppy),
        "annualized_return_arithmetic": mean * ppy, "serial_dependence": ess,
        "years": n / ppy, "effective_years": eff_years,
        "bootstrap": {"method": boot["method"], "block_length": boot["block_length"],
                      "repetitions": policy.bootstrap_repetitions, "seed": policy.bootstrap_seed,
                      "mean_ci95": [boot["ci_low"], boot["ci_high"]],
                      "annualized_return_ci95": [boot["ci_low"] * ppy, boot["ci_high"] * ppy],
                      "p_value_one_sided": p_boot},
        "normal_approximation": {"z": z, "p_value_one_sided": 1.0 - _N.cdf(z),
                                 "note": "cross-check on the effective sample size; the bootstrap decides"},
        "multiple_testing": {"tests": n_tests, "p_value_adjusted": min(1.0, p_boot * n_tests),
                             "sensitivity_p_adjusted": {str(k): min(1.0, p_boot * k)
                                                        for k in sorted({n_tests, n_tests + 1, n_tests + 6})},
                             "deflated_sharpe_ratio": dsr.get("deflated_sharpe_ratio"),
                             "deflated_benchmark_sharpe_annualized": (dsr.get("benchmark_sharpe") or 0.0) * math.sqrt(ppy),
                             "deflated_note": "benchmark = expected best of N null Sharpe ratios of this length"},
    })
    # power is reported for the conventional 5 % / 80 % pair as INFORMATION even while no policy is approved
    ref_alpha = (policy.familywise_alpha if policy.familywise_alpha is not None else 0.05) / n_tests
    ref_power = policy.required_power if policy.required_power is not None else 0.80
    out["power"] = {
        "assumption": "Sharpe estimate ~ Normal(true Sharpe, 1/sqrt(effective years)); one-sided; Bonferroni level",
        "per_test_alpha": ref_alpha, "reference_power": ref_power,
        "thresholds_are": "APPROVED_POLICY" if policy.approved else "ILLUSTRATIVE_5PCT_80PCT_NOT_APPROVED",
        "minimum_detectable_annual_sharpe": minimum_detectable_sharpe(effective_years=eff_years, alpha=ref_alpha,
                                                                      power=ref_power),
        "power_for_annual_sharpe": {str(s): power_at(s, effective_years=eff_years, alpha=ref_alpha)
                                    for s in (0.3, 0.5, 0.75, 1.0, 1.5, 2.0)},
    }
    if not policy.approved:
        out["unresolved_governance_requirements"] = policy.missing()
        return done(BLOCKED_PENDING_APPROVAL, "STATISTICAL_GATE_THRESHOLDS_NOT_APPROVED")
    achieved = power_at(policy.target_annual_sharpe, effective_years=eff_years, alpha=ref_alpha)
    out["power"]["achieved_power_at_target"] = achieved
    if achieved < policy.required_power:
        return done(INSUFFICIENT_STATISTICAL_POWER, "POWER_BELOW_REQUIREMENT_AT_TARGET_EFFECT")
    if out["multiple_testing"]["p_value_adjusted"] <= policy.familywise_alpha:
        return done(PASS)
    return done(FAIL, "NOT_SIGNIFICANT_AFTER_MULTIPLE_TESTING" if mean > 0 else "MEAN_NET_RETURN_NOT_POSITIVE")


__all__ = ["StatisticalGatePolicy", "evaluate_portfolio_gate", "unapproved_policy", "effective_sample_size",
           "autocorrelations", "power_at", "minimum_detectable_sharpe", "OUTCOMES", "PASS", "FAIL",
           "INSUFFICIENT_STATISTICAL_POWER", "INSUFFICIENT_DATA", "INVALID_EVIDENCE", "BLOCKED_PENDING_APPROVAL",
           "STATISTICAL_GATE_VERSION"]
