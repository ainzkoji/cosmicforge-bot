"""Stage evaluation over canonical replay records (Sections 22.11, 22.14-22.23).

POPULATIONS (never mixed):

* ``APPROVED``   -- veto ``APPROVE_FOR_RANKING``: what the FROZEN CATI would
                    actually allow to trade. The ONLY certification population.
* ``ADMISSIBLE`` -- ``ECONOMICALLY_ADMISSIBLE`` regardless of veto: DIAGNOSTIC
                    (e.g. what the economics say while calibration keeps the
                    veto at WATCH). Never used to pass a gate.
* ``ALL``        -- every labeled hypothesis: market diagnostic only.

Replay has no broker, so ``executed`` is NOT_APPLICABLE_REPLAY (forward demo).
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from . import stats
from .contracts import CertificationStage, CertificationStageResult, CertificationStatus, CertReason
from .policy import CertificationPolicy
from .replay import ADMISSIBLE, APPROVE, NEIGHBOR_PLAN

S = CertificationStatus
STRATA = ("regime", "setup_family", "symbol_group", "symbol", "side", "month", "session", "liquidity_bucket",
          "volume_liquidity_proxy")
CONCENTRATION_KEYS = ("symbol", "month", "regime", "setup_family", "side")
STRESS_RULES_VERSION = "1.0.0"
STRESS_RULES = {
    "HIGH_VOLATILITY": "daily (high-low)/close above the 90th percentile of the PRIOR 60 days",
    "CRASH": "daily close-to-close return below the 5th percentile of the PRIOR 60 days",
    "SHARP_REVERSAL": "return sign flip with both |returns| above the 80th percentile of the PRIOR 60 |returns|",
}
UNAVAILABLE_STRESS = {
    "EVENT_WINDOWS": "economic calendar source unavailable/stale -- never fabricated",
    "FUNDING_EXTREMES": "no historical funding series in the dataset",
    "SPREAD_STRESS": "no historical order-book/spread series in the dataset",
    "LOW_LIQUIDITY_BOOK": "no historical depth; see volume_liquidity_proxy strata instead",
}


def population(records: Sequence[Mapping[str, Any]], name: str, *, multiplier: Optional[float] = None,
               neighbor: Optional[str] = None) -> List[Mapping[str, Any]]:
    def outcome(r, key):
        if neighbor is not None:
            return r["neighbors"][neighbor][key]
        if multiplier is not None and float(multiplier) != 1.0:
            return r["stress"][str(float(multiplier))][key]
        return r[key]

    if name == "APPROVED":
        return [r for r in records if outcome(r, "veto_outcome") == APPROVE]
    if name == "ADMISSIBLE":
        return [r for r in records if outcome(r, "admission_status") == ADMISSIBLE]
    if name == "ALL":
        return list(records)
    raise ValueError(name)


def _net(r: Mapping[str, Any], m: float = 1.0) -> float:
    return float(r["net_R"]) if float(m) == 1.0 else float(r["stress"][str(float(m))]["net_R"])


def _summary(rows: Sequence[Mapping[str, Any]], policy: CertificationPolicy, *, m: float = 1.0) -> Dict[str, Any]:
    vals = [_net(r, m) for r in rows]
    exp = stats.expectancy_summary(
        vals, reps=policy.bootstrap_repetitions.value, confidence=policy.confidence_level.value,
        seed=policy.bootstrap_seed.value, outcomes=[r["terminal_outcome"] for r in rows])
    gross = [float(r["gross_R"]) for r in rows]
    cost = [float(r["cost_R"]) * m for r in rows]
    out = {"n": len(rows), "net": exp,
           "gross_mean_R": sum(gross) / len(gross) if gross else None,
           "cost_mean_R": sum(cost) / len(cost) if cost else None,
           "cost_share_of_gross": (sum(cost) / sum(abs(g) for g in gross)) if gross and sum(abs(g) for g in gross) else None,
           "mfe_mean_R": sum(float(r["mfe_R"]) for r in rows) / len(rows) if rows else None,
           "mae_mean_R": sum(float(r["mae_R"]) for r in rows) / len(rows) if rows else None,
           "same_bar_conservative_count": sum(1 for r in rows if r.get("same_bar_conservative"))}
    out["drawdown_tail"] = stats.drawdown_and_tail(vals, [r["decision_time"] for r in rows],
                                                   es_alpha=policy.expected_shortfall_alpha.value,
                                                   min_tail_samples=policy.min_tail_samples.value)
    return out


def _stratify(rows, policy):
    kw = dict(value="net_R", min_n=policy.min_stratum_sample_size.value, reps=max(200, policy.bootstrap_repetitions.value // 4),
              confidence=policy.confidence_level.value, seed=policy.bootstrap_seed.value, cost_key="cost_R",
              gross_key="gross_R")
    out = {k: stats.stratify(rows, lambda r, k=k: r.get(k, "UNKNOWN"), **kw) for k in STRATA}
    out["asset_class"] = stats.stratify(rows, lambda r: r.get("asset_class", "CRYPTO"), **kw)
    out["venue"] = stats.stratify(rows, lambda r: r.get("venue", "BINANCE_USDM"), **kw)
    return out


def _daily(rows: Sequence[Mapping[str, Any]], days: Sequence[str]) -> List[float]:
    acc: Dict[str, float] = defaultdict(float)
    for r in rows:
        acc[_day(r["decision_time"])] += float(r["net_R"])
    return [acc.get(d, 0.0) for d in days]


def _day(ts: int) -> str:
    return datetime.fromtimestamp(int(ts) / 1000, timezone.utc).strftime("%Y-%m-%d")


def neighbor_robustness(records, policy: CertificationPolicy) -> Dict[str, Any]:
    out: Dict[str, Any] = {"plan": [list(v) for v in NEIGHBOR_PLAN], "variants": {},
                           "note": "robustness diagnostics; no neighbor replaces the frozen policy"}
    for name, *_ in NEIGHBOR_PLAN:
        for pop in ("APPROVED", "ADMISSIBLE"):
            rows = population(records, pop, neighbor=name)
            vals = [float(r["net_R"]) for r in rows]
            out["variants"].setdefault(name, {})[pop] = {"n": len(rows), "mean_net_R": sum(vals) / len(vals) if vals else None}
    for pop in ("APPROVED", "ADMISSIBLE"):
        means = [v[pop]["mean_net_R"] for v in out["variants"].values() if v[pop]["mean_net_R"] is not None]
        out[f"{pop.lower()}_positive_share"] = (sum(1 for m in means if m > 0) / len(means)) if means else None
    return out


def overfitting(records, policy: CertificationPolicy, *, n_trials: int, population_name: str) -> Dict[str, Any]:
    days = sorted({_day(r["decision_time"]) for r in records})
    perf = {"frozen": _daily(population(records, population_name), days)}
    for name, *_ in NEIGHBOR_PLAN:
        perf[name] = _daily(population(records, population_name, neighbor=name), days)
    perf = {k: v for k, v in perf.items() if any(x != 0.0 for x in v)}
    pbo = stats.pbo_cscv(perf, partitions=policy.cscv_partitions.value, min_variants=policy.cscv_min_variants.value)
    pbo["population"] = population_name
    cert = [float(r["net_R"]) for r in population(records, population_name)]
    sharpes = []
    for series_ in perf.values():
        nz = [x for x in series_ if x != 0.0]
        if len(nz) > 2:
            mu = sum(nz) / len(nz)
            sd = (sum((x - mu) ** 2 for x in nz) / (len(nz) - 1)) ** 0.5
            if sd > 0:
                sharpes.append(mu / sd)
    var = (sum((s - sum(sharpes) / len(sharpes)) ** 2 for s in sharpes) / (len(sharpes) - 1)) if len(sharpes) > 1 else 0.0
    dsr = stats.deflated_sharpe(cert, n_trials=max(n_trials, len(perf)), trial_sharpe_variance=var)
    dsr["population"] = population_name
    dsr["note"] = "Sharpe is reported only together with its deflated value"
    return {"pbo_cscv": pbo, "deflated_sharpe": dsr}


def stress_windows(series: Mapping[str, Mapping[str, Sequence[Any]]], timeframe: str, *,
                   lookback_days: int = 60) -> Dict[str, Any]:
    """Deterministic, PREDECLARED causal selection (no cherry-picking: every
    flagged day is included, whatever CATI did on it)."""
    flagged: Dict[str, Dict[str, List[str]]] = {}
    for symbol, tfs in sorted(series.items()):
        days: Dict[str, List[Any]] = defaultdict(list)
        for row in tfs.get(timeframe, ()):
            days[_day(int(row[0]))].append(row)
        ordered = sorted(days)
        closes = [float(days[d][-1][4]) for d in ordered]
        ranges = [(max(float(r[2]) for r in days[d]) - min(float(r[3]) for r in days[d])) / float(days[d][-1][4])
                  for d in ordered]
        rets = [None] + [closes[i] / closes[i - 1] - 1 for i in range(1, len(closes))]
        sym: Dict[str, List[str]] = defaultdict(list)
        for i in range(lookback_days + 1, len(ordered)):
            prior_r = ranges[i - lookback_days:i]
            prior_ret = [x for x in rets[i - lookback_days:i] if x is not None]
            q = lambda xs, p: sorted(xs)[int(p * (len(xs) - 1))]  # noqa: E731
            if ranges[i] > q(prior_r, 0.90):
                sym["HIGH_VOLATILITY"].append(ordered[i])
            if rets[i] is not None and rets[i] < q(prior_ret, 0.05):
                sym["CRASH"].append(ordered[i])
            absq = q([abs(x) for x in prior_ret], 0.80)
            if rets[i] is not None and rets[i - 1] is not None and (rets[i] > 0) != (rets[i - 1] > 0) \
                    and abs(rets[i]) > absq and abs(rets[i - 1]) > absq:
                sym["SHARP_REVERSAL"].append(ordered[i])
        flagged[symbol] = dict(sym)
    return {"version": STRESS_RULES_VERSION, "rules": STRESS_RULES, "lookback_days": lookback_days,
            "flagged_days": flagged, "unavailable": UNAVAILABLE_STRESS}


def in_stress(record: Mapping[str, Any], windows: Mapping[str, Any]) -> List[str]:
    day = _day(record["decision_time"])
    return sorted(k for k, ds in windows["flagged_days"].get(record["symbol"], {}).items() if day in ds)


def stage_metrics(records: Sequence[Mapping[str, Any]], policy: CertificationPolicy, *, n_trials: int = 1) -> Dict[str, Any]:
    pops = {p: population(records, p) for p in ("APPROVED", "ADMISSIBLE", "ALL")}
    cert = pops["APPROVED"]
    strat_pop = "APPROVED" if cert else "ADMISSIBLE"
    strat_rows = pops[strat_pop]
    metrics: Dict[str, Any] = {
        "counts": {"candidates": len(records), "admissible": len(pops["ADMISSIBLE"]), "approved": len(cert),
                   "executed": "NOT_APPLICABLE_REPLAY", "setup_families": _count(records, "setup_family"),
                   "veto_outcomes": _count(records, "veto_outcome"),
                   "veto_runtime_only_blocks": sum(1 for r in records if r.get("veto_runtime_only"))},
        "populations": {p: _summary(rows, policy) for p, rows in pops.items()},
        "stratification": {"population": strat_pop, "strata": _stratify(strat_rows, policy)},
        "concentration": {"population": strat_pop,
                          "by": {k: stats.concentration(strat_rows, lambda r, k=k: r.get(k), value="net_R")
                                 for k in CONCENTRATION_KEYS}},
        "calibration": stats.calibration_summary(
            [(float(r["forecast_p"]), int(float(r["net_R"]) > 0)) for r in records if r.get("forecast_usable")],
            bins=policy.calibration_bins.value),
        "parameter_neighbors": neighbor_robustness(records, policy),
    }
    stress_out = {}
    for m in policy.cost_stress_multipliers.value:
        m = float(m)
        entry = {"reselected": {p: _summary(population(records, p, multiplier=m), policy, m=m) for p in ("APPROVED", "ADMISSIBLE")},
                 "fixed_population": {p: _summary(pops[p], policy, m=m) for p in ("APPROVED", "ADMISSIBLE")}}
        entry["admission_change"] = {"admissible": len(population(records, "ADMISSIBLE", multiplier=m)) - len(pops["ADMISSIBLE"]),
                                     "approved": len(population(records, "APPROVED", multiplier=m)) - len(cert)}
        stress_out[str(m)] = entry
    metrics["cost_stress"] = stress_out
    metrics["overfitting"] = overfitting(records, policy, n_trials=n_trials, population_name=strat_pop)
    return metrics


def _count(rows, key):
    out: Dict[str, int] = {}
    for r in rows:
        out[str(r.get(key))] = out.get(str(r.get(key)), 0) + 1
    return dict(sorted(out.items()))


def stage_status(stage: str, metrics: Optional[Mapping[str, Any]], *, policy: CertificationPolicy,
                 integrity: Mapping[str, Any], certifiable_source: bool, coverage_days: float,
                 required_days: float, determinism_ok: Optional[bool]) -> Tuple[str, Tuple[str, ...]]:
    reasons: List[str] = []
    if integrity.get("lookahead_violations", 0) > policy.max_lookahead_violations.value:
        return S.FAIL.value, (CertReason.LOOKAHEAD_VIOLATION.value,)
    if determinism_ok is False:
        return S.FAIL.value, (CertReason.REPLAY_HASH_UNSTABLE.value,)
    if not certifiable_source:
        return S.BLOCKED_DATA.value, (CertReason.SYNTHETIC_SOURCE_REFUSED.value,)
    if coverage_days + 1e-9 < required_days:
        return S.BLOCKED_DATA.value, (CertReason.INSUFFICIENT_COVERAGE.value,)
    if metrics is None:
        return S.BLOCKED_DATA.value, (CertReason.INSUFFICIENT_COVERAGE.value,)
    if stage == CertificationStage.FAST.value:
        # FAST is logic/integrity only -- never certification
        return S.PASS.value, ("FAST_IS_NOT_CERTIFICATION",)
    n = metrics["counts"]["approved"]
    if n == 0:
        return S.INSUFFICIENT_EVIDENCE.value, (CertReason.NO_ACCEPTED_EVIDENCE.value,)
    missing = policy.missing("min_accepted_evidence_count", "min_probability_expectancy_positive")
    if missing:
        return S.INSUFFICIENT_EVIDENCE.value, (CertReason.POLICY_INCOMPLETE.value, *missing)
    if n < policy.min_accepted_evidence_count.value:
        return S.INSUFFICIENT_EVIDENCE.value, (CertReason.INSUFFICIENT_SAMPLES.value,)
    net = metrics["populations"]["APPROVED"]["net"]
    if net["mean_R"] <= policy.min_net_expectancy_R.value:
        reasons.append(CertReason.NEGATIVE_EXPECTANCY.value)
    if net.get("p_expectancy_positive", 0.0) < policy.min_probability_expectancy_positive.value:
        reasons.append(CertReason.LOW_PROBABILITY_POSITIVE.value)
    return (S.FAIL.value, tuple(reasons)) if reasons else (S.PASS.value, ())


def build_stage_result(stage: str, metrics: Optional[Mapping[str, Any]], status: str, reasons: Sequence[str], *,
                       diagnostics: Mapping[str, Any], artifact_refs: Mapping[str, str]) -> CertificationStageResult:
    return CertificationStageResult(stage=stage, status=status, metrics=metrics or {}, diagnostics=diagnostics,
                                    reason_codes=tuple(reasons), artifact_refs=artifact_refs)


__all__ = ["population", "stage_metrics", "stage_status", "build_stage_result", "stress_windows", "in_stress",
           "neighbor_robustness", "overfitting", "STRATA", "CONCENTRATION_KEYS", "STRESS_RULES", "UNAVAILABLE_STRESS"]
