"""Calibration/report capability (Section 10.8) -- structural diagnostics only.

Nothing here claims a validated probability. Measuring "regime-conditioned
future market behavior" requires outcome data (what actually happened after
a regime call) that this shadow-only Section 9/10 implementation does not
yet collect -- that belongs to a later phase once shadow evidence has
accumulated. Until then this module only reports structural properties of
the distributions themselves (dominant-regime stability, entropy, transition
frequency) -- useful for sanity-checking the engine, not for claiming the
weights are calibrated probabilities.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Sequence

from app.trading_intelligence.regime.contracts import REGIME_CLASSES, RegimeDistribution


@dataclass(frozen=True)
class CalibrationReport:
    """Structural-only diagnostics. ``is_calibrated`` is always False here --
    it exists so a caller cannot mistake this report for outcome-validated
    calibration; a future phase that adds real outcome tracking should set
    it only once reliability has actually been measured."""

    sample_count: int
    dominant_regime_stability: float  # fraction of consecutive samples with unchanged dominant regime
    average_entropy: float
    transition_frequency: float  # fraction of consecutive samples with a dominant-regime change
    dominant_regime_counts: Dict[str, int]
    is_calibrated: bool = False
    note: str = (
        "Structural diagnostics only. These are regime BELIEF WEIGHTS, not "
        "validated probabilities -- do not use this report to claim "
        "'X% chance the market is trending' until outcome-conditioned "
        "reliability has been measured against realized future behavior."
    )


def build_calibration_report(history: Sequence[RegimeDistribution]) -> CalibrationReport:
    samples = list(history)
    counts: Dict[str, int] = {r.value: 0 for r in REGIME_CLASSES}
    for d in samples:
        counts[d.dominant_regime] = counts.get(d.dominant_regime, 0) + 1

    if len(samples) < 2:
        avg_entropy = samples[0].entropy if samples else 0.0
        return CalibrationReport(
            sample_count=len(samples),
            dominant_regime_stability=1.0 if samples else 0.0,
            average_entropy=avg_entropy,
            transition_frequency=0.0,
            dominant_regime_counts=counts,
        )

    pairs: list = list(zip(samples, samples[1:]))
    unchanged = sum(1 for a, b in pairs if a.dominant_regime == b.dominant_regime)
    stability = unchanged / len(pairs)
    transition_frequency = 1.0 - stability
    average_entropy = sum(d.entropy for d in samples) / len(samples)

    return CalibrationReport(
        sample_count=len(samples),
        dominant_regime_stability=stability,
        average_entropy=average_entropy,
        transition_frequency=transition_frequency,
        dominant_regime_counts=counts,
    )


__all__ = ["CalibrationReport", "build_calibration_report"]
