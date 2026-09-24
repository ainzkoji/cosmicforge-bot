"""Pure portfolio math (Sections 16.5-16.8): EWMA correlation, shrinkage,
side adjustment, EWMA market-factor beta. Never fetches, never reads a clock;
operates only on the histories injected via ``PortfolioMarketContext``.

Conventions
-----------
* Zero-mean EWMA (RiskMetrics style): weights decay with age from the most
  recent overlapping observation, lambda = 0.5 ** (1 / half_life_bars).
* Correlation only where enough OVERLAPPING observations exist; otherwise the
  static-group fallback is used and RECORDED (never silently).
* Beta is None (unknown) when history/variance is insufficient -- never 0.
* Causal twice over: the context builder drops rows closing after the
  decision time, and every estimator re-filters to ``t <= decision_time``.
* Alignment is on COMMON closed timestamps only; nothing is forward-filled.
"""
from __future__ import annotations

import math
from typing import Mapping, Optional, Sequence, Tuple

from app.trading_intelligence.contracts.portfolio_intel import (
    CROSS_ASSET_CLASS_PAIR, NO_STATIC_GROUP_AVAILABLE, STATIC_CORRELATION_FALLBACK_USED,
    STATISTICAL_HISTORY_INSUFFICIENT, BetaEstimate, CorrelationEstimate, PortfolioMarketContext, PortfolioPolicy,
)

Series = Sequence[Tuple[int, float]]


def side_sign(side: str) -> int:
    """LONG = +1, SHORT = -1."""
    s = str(side).upper()
    if s in ("LONG", "BUY"):
        return 1
    if s in ("SHORT", "SELL"):
        return -1
    raise ValueError(f"unknown side: {side!r}")


def log_returns_from_closes(rows: Sequence[Sequence], decision_time: int) -> Tuple[Tuple[int, float], ...]:
    """(close_time, log_return) from kline-like rows, CAUSAL: rows closing
    after ``decision_time`` are excluded before any return is formed."""
    closed = [(int(r[6]), float(r[4])) for r in rows if int(r[6]) <= decision_time]
    closed.sort()
    out = []
    for (_, prev), (t, cur) in zip(closed, closed[1:]):
        if prev > 0 and cur > 0:
            out.append((t, math.log(cur / prev)))
    return tuple(out)


def _overlap(a: Series, b: Series, max_bars: int, decision_time: Optional[int] = None) -> Tuple[list, list]:
    """Common closed timestamps only (inner join); no forward fill; never a
    value stamped after ``decision_time``."""
    bmap = {t: v for t, v in b if decision_time is None or t <= decision_time}
    common = [(t, v, bmap[t]) for t, v in a if t in bmap and (decision_time is None or t <= decision_time)]
    common.sort()
    common = common[-max_bars:]
    return [c[1] for c in common], [c[2] for c in common]


def _weights(n: int, lam: float) -> list:
    # index n-1 is the most recent observation (age 0)
    return [lam ** (n - 1 - i) for i in range(n)]


def effective_sample_size(n: int, lam: float) -> float:
    """Kish ESS of EWMA weights: (sum w)^2 / sum w^2."""
    if n <= 0:
        return 0.0
    w = _weights(n, lam)
    return sum(w) ** 2 / sum(x * x for x in w)


def ewma_correlation(a: Series, b: Series, *, ewma_lambda: float, max_bars: int,
                     decision_time: Optional[int] = None) -> Tuple[Optional[float], int]:
    x, y = _overlap(a, b, max_bars, decision_time)
    n = len(x)
    if n < 2:
        return None, n
    w = _weights(n, ewma_lambda)
    sxx = sum(wi * xi * xi for wi, xi in zip(w, x))
    syy = sum(wi * yi * yi for wi, yi in zip(w, y))
    sxy = sum(wi * xi * yi for wi, xi, yi in zip(w, x, y))
    if sxx <= 0 or syy <= 0:
        return None, n
    rho = sxy / math.sqrt(sxx * syy)
    return max(-1.0, min(1.0, rho)), n


def shrink_correlation(rho_ewma: float, *, shrinkage_lambda: float, target: float) -> float:
    """rho_shrunk = (1 - lambda) * rho_ewma + lambda * target."""
    return (1.0 - shrinkage_lambda) * rho_ewma + shrinkage_lambda * target


def ewma_beta(asset: Series, factor: Series, *, ewma_lambda: float, max_bars: int, variance_floor: float,
              min_obs: int, decision_time: Optional[int] = None) -> Tuple[Optional[float], int, str]:
    """beta = EWMA_cov(asset, factor) / EWMA_var(factor); None when unknown."""
    x, f = _overlap(asset, factor, max_bars, decision_time)
    n = len(x)
    if n < min_obs:
        return None, n, "INSUFFICIENT_HISTORY"
    w = _weights(n, ewma_lambda)
    var_f = sum(wi * fi * fi for wi, fi in zip(w, f)) / sum(w)
    if var_f < variance_floor:
        return None, n, "ZERO_VARIANCE"
    cov = sum(wi * xi * fi for wi, xi, fi in zip(w, x, f)) / sum(w)
    return cov / var_f, n, "OK"


def pair_correlation(a: str, b: str, ctx: PortfolioMarketContext, policy: PortfolioPolicy) -> CorrelationEstimate:
    """Shrunk EWMA correlation when enough overlapping history exists; else
    the versioned static-group fallback. Every fallback is RECORDED
    (``fallback_used`` + reason codes) -- a correlation is never silently 0."""
    ha, hb = ctx.return_histories.get(a), ctx.return_histories.get(b)
    lam = policy.ewma_lambda
    n_obs = 0
    if ha and hb:
        rho, n = ewma_correlation(ha, hb, ewma_lambda=lam, max_bars=policy.max_history_bars,
                                  decision_time=ctx.decision_time)
        n_obs = n
        if rho is not None and n >= policy.minimum_correlation_observations:
            shrunk = shrink_correlation(rho, shrinkage_lambda=policy.correlation_shrinkage_lambda,
                                        target=policy.correlation_target)
            return CorrelationEstimate(a, b, rho, shrunk, n, "EWMA", False, effective_sample_size(n, lam),
                                       policy.correlation_shrinkage_lambda, policy.correlation_target)
    reasons = [STATISTICAL_HISTORY_INSUFFICIENT, STATIC_CORRELATION_FALLBACK_USED]
    ga, gb = ctx.instrument_groups.get(a, "UNKNOWN"), ctx.instrument_groups.get(b, "UNKNOWN")
    ca, cb = ctx.instrument_asset_classes.get(a), ctx.instrument_asset_classes.get(b)
    if ca and cb and ca != cb:
        reasons.append(CROSS_ASSET_CLASS_PAIR)
    if ga == "UNKNOWN" or gb == "UNKNOWN":
        rho = policy.unknown_group_correlation
        reasons.append(NO_STATIC_GROUP_AVAILABLE)
    elif ga == gb:
        rho = policy.static_group_correlation
    else:
        rho = policy.static_other_correlation
    return CorrelationEstimate(a, b, None, rho, n_obs, "STATIC_GROUP_FALLBACK", True, None, 0.0, 0.0, tuple(reasons))


def effective_correlation(estimate: CorrelationEstimate, side_a: str, side_b: str) -> float:
    """effective_corr = rho_shrunk * sign_i * sign_j (direction matters)."""
    return estimate.rho_shrunk * side_sign(side_a) * side_sign(side_b)


def factor_beta(asset: str, factor_id: str, ctx: PortfolioMarketContext, policy: PortfolioPolicy,
                reference_symbol: Optional[str] = None) -> BetaEstimate:
    """EWMA beta of ``asset`` to the factor series ``factor_id`` (histories
    are keyed by namespaced factor id, e.g. ``CRYPTO:MARKET:BTC``)."""
    fh = ctx.factor_histories.get(factor_id)
    ah = ctx.return_histories.get(asset)
    if not fh:
        return BetaEstimate(asset, factor_id, None, 0, "FACTOR_MISSING")
    if reference_symbol is not None and asset.upper() == reference_symbol.upper():
        return BetaEstimate(asset, factor_id, 1.0, len(fh), "OK")
    if not ah:
        return BetaEstimate(asset, factor_id, None, 0, "INSUFFICIENT_HISTORY")
    beta, n, quality = ewma_beta(ah, fh, ewma_lambda=policy.ewma_lambda, max_bars=policy.max_history_bars,
                                 variance_floor=policy.variance_floor, min_obs=policy.minimum_beta_observations,
                                 decision_time=ctx.decision_time)
    return BetaEstimate(asset, factor_id, beta, n, quality)


__all__ = [
    "side_sign", "log_returns_from_closes", "effective_sample_size", "ewma_correlation", "shrink_correlation", "ewma_beta",
    "pair_correlation", "effective_correlation", "factor_beta",
]
