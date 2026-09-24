"""AdmissionPolicy defaults and penalty functions (Section 13.3-13.4).

RESEARCH DEFAULTS: every threshold below exists to exercise the admission
pipeline, not because replay has calibrated it (Section "RESEARCH /
CALIBRATION RULES"). They are unrelated to V2's adaptive entry threshold --
never derive one from the other (Section 13.16).
"""
from __future__ import annotations

from app.trading_intelligence.contracts.economics import AdmissionPolicy
from app.trading_intelligence.contracts.market_state import clamp01


def default_admission_policy() -> AdmissionPolicy:
    return AdmissionPolicy()


def uncertainty_penalty_r(*, forecast_uncertainty: float, credible_interval_width: float, coefficient: float) -> float:
    """Section 13.3: f(forecast uncertainty, credible interval width,
    support quality) -- forecast_uncertainty already folds support quality
    in (Section 12.14), so this stays a simple, visible two-driver blend."""
    driver = clamp01(0.6 * forecast_uncertainty + 0.4 * clamp01(credible_interval_width))
    return coefficient * driver


def distribution_shift_penalty_r(*, ood_score: float, severity: str, coefficient: float) -> float:
    severity_multiplier = {"NONE": 0.0, "LOW": 0.3, "MODERATE": 0.6, "HIGH": 1.0}.get(severity, 1.0)
    driver = clamp01(ood_score) * severity_multiplier
    return coefficient * driver


def execution_uncertainty_penalty_r(*, cost_uncertainty_r: float, liquidity_quality: float, coefficient: float) -> float:
    """``liquidity_quality`` is 1.0 = fully verified, 0.0 = unverified --
    unverified liquidity amplifies the visible cost uncertainty rather than
    being ignored."""
    driver = cost_uncertainty_r * (1.0 + (1.0 - clamp01(liquidity_quality)))
    return coefficient * driver


__all__ = [
    "default_admission_policy",
    "uncertainty_penalty_r",
    "distribution_shift_penalty_r",
    "execution_uncertainty_penalty_r",
]
