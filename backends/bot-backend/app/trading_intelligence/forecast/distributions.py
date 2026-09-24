"""Robust weighted R-distribution statistics and shrinkage (Sections 12.10,
12.11). Raw and shrunk statistics are always reported separately -- nothing
here silently deletes extreme outcomes.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Sequence

from app.trading_intelligence.forecast.posterior import weighted_mean, weighted_quantile

#: RESEARCH DEFAULT shrinkage strength (Section 12.11). Centralized here,
#: not hard-coded inline at each call site.
DEFAULT_SHRINKAGE_STRENGTH = 20.0

LOWER_QUANTILE = 0.10
UPPER_QUANTILE = 0.90


@dataclass(frozen=True)
class RobustStats:
    mean: Optional[float]
    median: Optional[float]
    lower_quantile: Optional[float]
    upper_quantile: Optional[float]
    n: int


def compute_robust_stats(values: Sequence[float], weights: Sequence[float]) -> RobustStats:
    n = sum(1 for w in weights if w is not None and w > 0)
    return RobustStats(
        mean=weighted_mean(values, weights),
        median=weighted_quantile(values, weights, 0.5),
        lower_quantile=weighted_quantile(values, weights, LOWER_QUANTILE),
        upper_quantile=weighted_quantile(values, weights, UPPER_QUANTILE),
        n=n,
    )


def quantile_map(values: Sequence[float], weights: Sequence[float], quantiles: Sequence[float]) -> Dict[str, float]:
    out = {}
    for q in quantiles:
        v = weighted_quantile(values, weights, q)
        if v is not None:
            out[f"p{int(q * 100)}"] = v
    return out


def shrink_toward_broader_mean(
    *, local_mean: Optional[float], broader_mean: Optional[float], ess: float, shrinkage_strength: float = DEFAULT_SHRINKAGE_STRENGTH
) -> Optional[float]:
    """Section 12.11: lambda = ESS / (ESS + shrinkage_strength);
    shrunk = lambda * local + (1 - lambda) * broader."""
    if local_mean is None:
        return broader_mean
    if broader_mean is None:
        return local_mean
    denom = ess + shrinkage_strength
    lam = (ess / denom) if denom > 0 else 0.0
    return lam * local_mean + (1.0 - lam) * broader_mean


__all__ = [
    "DEFAULT_SHRINKAGE_STRENGTH",
    "RobustStats",
    "compute_robust_stats",
    "quantile_map",
    "shrink_toward_broader_mean",
]
