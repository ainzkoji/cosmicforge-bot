"""Interpretable V1 out-of-distribution / drift detection (Section 12.13).

Every source of shift is visible individually (``continuous_feature_scores``,
``unseen_categories``, ``capability_shift_flags``, ``support_shift``) --
``ood_score`` summarizes them, it never replaces the breakdown.
"""
from __future__ import annotations

from typing import Dict, Mapping, Sequence

from app.trading_intelligence.contracts.forecast import DistributionShiftAssessment, ForecastReasonCode, ShiftSeverity
from app.trading_intelligence.contracts.market_state import clamp01

#: Robust z-score constant (Section 12.13): 0.6745 makes MAD comparable to
#: a normal standard deviation.
ROBUST_Z_CONSTANT = 0.6745
MAD_FLOOR = 1e-9

#: RESEARCH DEFAULT thresholds -- centralized, versioned via callers passing
#: policy, not scattered through engine code.
HIGH_Z_THRESHOLD = 3.5
LOW_RAW_SUPPORT = 10
LOW_ESS = 5.0
DEEP_BACKOFF_LEVEL = 5


def median(values: Sequence[float]) -> float:
    s = sorted(values)
    n = len(s)
    if n == 0:
        return 0.0
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def robust_z_score(value: float, reference: Sequence[float]) -> float:
    """0.6745 * (x - median) / MAD, MAD floored to avoid division by zero."""
    if not reference:
        return 0.0
    m = median(reference)
    abs_dev = [abs(v - m) for v in reference]
    mad = median(abs_dev)
    mad = mad if mad > MAD_FLOOR else MAD_FLOOR
    return ROBUST_Z_CONSTANT * (value - m) / mad


def assess_distribution_shift(
    *,
    continuous_features: Mapping[str, float],
    reference_distributions: Mapping[str, Sequence[float]],
    candidate_categories: Mapping[str, str],
    reference_categories: Mapping[str, Sequence[str]],
    required_capabilities_seen_in_reference: Sequence[str],
    capabilities_available_now: Sequence[str],
    raw_support: int,
    ess: float,
    backoff_level: int,
    high_z_threshold: float = HIGH_Z_THRESHOLD,
    low_raw_support: int = LOW_RAW_SUPPORT,
    low_ess: float = LOW_ESS,
    deep_backoff_level: int = DEEP_BACKOFF_LEVEL,
) -> DistributionShiftAssessment:
    continuous_scores: Dict[str, float] = {}
    for name, value in continuous_features.items():
        reference = reference_distributions.get(name, ())
        if not reference:
            continue
        continuous_scores[name] = abs(robust_z_score(value, reference))

    unseen: list = []
    for name, value in candidate_categories.items():
        seen = reference_categories.get(name, ())
        if seen and value not in seen:
            unseen.append(f"{name}={value}")

    capability_shift_flags: list = []
    missing_now = set(required_capabilities_seen_in_reference) - set(capabilities_available_now)
    for cap in sorted(missing_now):
        capability_shift_flags.append(cap)

    support_shift = raw_support < low_raw_support or ess < low_ess or backoff_level >= deep_backoff_level

    max_z = max(continuous_scores.values()) if continuous_scores else 0.0
    z_component = clamp01(max_z / (high_z_threshold * 2.0))
    category_component = clamp01(len(unseen) / 3.0)
    capability_component = clamp01(len(capability_shift_flags) / 2.0)
    support_component = 1.0 if support_shift else 0.0

    ood_score = clamp01(max(z_component, category_component, capability_component, 0.5 * support_component))

    reason_codes = []
    if max_z >= high_z_threshold:
        reason_codes.append(ForecastReasonCode.HIGH_OOD.value)
    if unseen:
        reason_codes.append(ForecastReasonCode.UNSEEN_CATEGORY.value)
    if capability_shift_flags:
        reason_codes.append(ForecastReasonCode.CAPABILITY_SHIFT.value)
    if support_shift:
        reason_codes.append(ForecastReasonCode.SUPPORT_SHIFT.value)
    if backoff_level >= deep_backoff_level:
        reason_codes.append(ForecastReasonCode.DEEP_BACKOFF.value)

    if ood_score >= 0.75 or max_z >= high_z_threshold:
        severity = ShiftSeverity.HIGH.value
    elif ood_score >= 0.45:
        severity = ShiftSeverity.MODERATE.value
    elif ood_score > 0.15:
        severity = ShiftSeverity.LOW.value
    else:
        severity = ShiftSeverity.NONE.value

    return DistributionShiftAssessment(
        ood_score=ood_score,
        continuous_feature_scores=continuous_scores,
        unseen_categories=tuple(unseen),
        capability_shift_flags=tuple(capability_shift_flags),
        support_shift=support_shift,
        severity=severity,
        reason_codes=tuple(reason_codes),
    )


def population_stability_index(reference: Sequence[float], candidate: Sequence[float], *, bins: int = 10) -> float:
    """Deterministic PSI over a shared histogram (Section 12.13.1) -- for
    offline/research batch-drift reports only. Never call this from a
    single-candidate admission path (a PSI computed from one observation is
    statistically meaningless)."""
    if not reference or not candidate:
        return 0.0
    lo = min(min(reference), min(candidate))
    hi = max(max(reference), max(candidate))
    if hi <= lo:
        return 0.0
    width = (hi - lo) / bins

    def _hist(data: Sequence[float]) -> list:
        counts = [0] * bins
        for v in data:
            idx = min(bins - 1, max(0, int((v - lo) / width)))
            counts[idx] += 1
        total = len(data)
        return [max(c / total, 1e-6) for c in counts]

    ref_hist = _hist(reference)
    cand_hist = _hist(candidate)
    psi = 0.0
    for r, c in zip(ref_hist, cand_hist):
        import math

        psi += (c - r) * math.log(c / r)
    return psi


__all__ = [
    "ROBUST_Z_CONSTANT",
    "robust_z_score",
    "assess_distribution_shift",
    "population_stability_index",
]
