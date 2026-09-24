"""Forecast uncertainty (Section 12.14) -- deterministic, component-based.
NOT "model confidence": it is a composition of explicit, named penalties,
mirroring the same discipline as ``market_state/uncertainty.py``.
"""
from __future__ import annotations

from typing import Dict, Tuple

from app.trading_intelligence.contracts.market_state import clamp01

_WEIGHTS = {
    "credible_interval_width": 0.30,
    "low_support": 0.20,
    "deep_backoff": 0.15,
    "distribution_shift": 0.20,
    "missing_capability": 0.10,
    "tail_heavy_r": 0.05,
}

#: RESEARCH DEFAULTS -- centralized, versioned via the caller's forecast
#: engine version, not scattered inline.
FULL_BACKOFF_LEVEL = 7
LOW_ESS_FLOOR = 30.0


def compute_forecast_uncertainty(
    *,
    credible_interval_width: float,
    ess: float,
    backoff_level: int,
    ood_score: float,
    missing_capability_count: int,
    r_tail_ratio: float = 0.0,
) -> Tuple[float, Dict[str, float]]:
    components: Dict[str, float] = {
        "credible_interval_width": clamp01(credible_interval_width),
        "low_support": clamp01(1.0 - min(ess, LOW_ESS_FLOOR) / LOW_ESS_FLOOR),
        "deep_backoff": clamp01(backoff_level / FULL_BACKOFF_LEVEL),
        "distribution_shift": clamp01(ood_score),
        "missing_capability": clamp01(missing_capability_count / 3.0),
        "tail_heavy_r": clamp01(r_tail_ratio / 5.0),
    }
    value = clamp01(sum(_WEIGHTS[k] * components[k] for k in _WEIGHTS))
    return value, components


__all__ = ["compute_forecast_uncertainty", "FULL_BACKOFF_LEVEL", "LOW_ESS_FLOOR"]
