"""Deterministic statistical primitives (Sections 12.7-12.9): weighted
quantiles, effective sample size, Beta-Binomial posterior for
``net_profitable``, and a Dirichlet-Multinomial posterior for the three-way
terminal-outcome distribution. No opaque learned model anywhere here.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, Optional, Sequence, Tuple

from scipy import stats as _scipy_stats

#: Versioned credible-interval level (Section 12.8) -- a policy choice, not
#: a magic number buried in engine code.
DEFAULT_CREDIBLE_INTERVAL_LEVEL = 0.90


def effective_sample_size(weights: Sequence[float]) -> float:
    """ESS = (sum w)^2 / sum(w^2). 0 if every weight is invalid/zero."""
    finite = [w for w in weights if w is not None and w > 0]
    if not finite:
        return 0.0
    sum_w = sum(finite)
    sum_w2 = sum(w * w for w in finite)
    if sum_w2 <= 0:
        return 0.0
    return (sum_w * sum_w) / sum_w2


def weighted_mean(values: Sequence[float], weights: Sequence[float]) -> Optional[float]:
    pairs = [(v, w) for v, w in zip(values, weights) if w is not None and w > 0]
    if not pairs:
        return None
    total_w = sum(w for _, w in pairs)
    if total_w <= 0:
        return None
    return sum(v * w for v, w in pairs) / total_w


def weighted_quantile(values: Sequence[float], weights: Sequence[float], q: float) -> Optional[float]:
    """Standard weighted-quantile-by-cumulative-weight. None if there is no
    usable (finite, positively-weighted) data at all."""
    pairs = sorted(
        (v, w) for v, w in zip(values, weights) if w is not None and w > 0 and v is not None
    )
    if not pairs:
        return None
    total_w = sum(w for _, w in pairs)
    if total_w <= 0:
        return None
    target = q * total_w
    cumulative = 0.0
    for value, w in pairs:
        cumulative += w
        if cumulative >= target:
            return value
    return pairs[-1][0]


@dataclass(frozen=True)
class BetaBinomialResult:
    p_mean: float
    credible_interval_low: float
    credible_interval_high: float
    credible_interval_level: float
    prior_alpha: float
    prior_beta: float
    posterior_alpha: float
    posterior_beta: float


def beta_binomial_posterior(
    *,
    weighted_win_rate: Optional[float],
    ess: float,
    prior_alpha: float,
    prior_beta: float,
    credible_interval_level: float = DEFAULT_CREDIBLE_INTERVAL_LEVEL,
) -> BetaBinomialResult:
    """Section 12.8. ``weighted_win_rate``/``ess`` come from the *local*
    cohort; ``prior_alpha``/``prior_beta`` encode the broader setup-family
    evidence and are bounded by policy so a huge broader cohort cannot erase
    local evidence (that bounding happens in the caller, which controls how
    the prior is built -- this function only combines what it is given)."""
    p = weighted_win_rate if weighted_win_rate is not None else 0.0
    adjusted_wins = ess * p
    adjusted_losses = ess * (1.0 - p)
    posterior_alpha = prior_alpha + adjusted_wins
    posterior_beta = prior_beta + adjusted_losses

    p_mean = posterior_alpha / (posterior_alpha + posterior_beta)
    lower_q = (1.0 - credible_interval_level) / 2.0
    upper_q = 1.0 - lower_q
    ci_low = float(_scipy_stats.beta.ppf(lower_q, posterior_alpha, posterior_beta))
    ci_high = float(_scipy_stats.beta.ppf(upper_q, posterior_alpha, posterior_beta))

    return BetaBinomialResult(
        p_mean=p_mean, credible_interval_low=ci_low, credible_interval_high=ci_high,
        credible_interval_level=credible_interval_level,
        prior_alpha=prior_alpha, prior_beta=prior_beta,
        posterior_alpha=posterior_alpha, posterior_beta=posterior_beta,
    )


def dirichlet_multinomial_posterior(
    *,
    weighted_counts: Mapping[str, float],
    ess: float,
    prior: Mapping[str, float],
) -> Dict[str, float]:
    """Section 12.9 three-way posterior. ``weighted_counts`` are per-category
    weighted proportions (summing to <= 1 across categories); scaled by
    ``ess`` the same way the Beta-Binomial scales a weighted win rate, so
    the two posteriors are on a consistent evidence-strength footing.

    Returns posterior means per category, always summing to 1 -- callers
    must never derive one category as ``1 - sum(others)`` when it was not
    itself observed (Section 12.9's explicit prohibition); every category
    gets its own posterior alpha here.
    """
    categories = set(weighted_counts) | set(prior)
    posterior_alpha: Dict[str, float] = {}
    for cat in categories:
        observed_fraction = weighted_counts.get(cat, 0.0)
        posterior_alpha[cat] = prior.get(cat, 0.0) + ess * observed_fraction
    total_alpha = sum(posterior_alpha.values())
    if total_alpha <= 0:
        n = len(categories) or 1
        return {cat: 1.0 / n for cat in categories}
    return {cat: alpha / total_alpha for cat, alpha in posterior_alpha.items()}


def dirichlet_marginal_credible_interval(
    *, category_alpha: float, other_alpha_sum: float, level: float = DEFAULT_CREDIBLE_INTERVAL_LEVEL
) -> Tuple[float, float]:
    """The marginal distribution of one Dirichlet component is
    Beta(alpha_i, sum(other alphas)) -- reusing scipy's Beta ppf rather than
    a separate Dirichlet sampler."""
    lower_q = (1.0 - level) / 2.0
    upper_q = 1.0 - lower_q
    low = float(_scipy_stats.beta.ppf(lower_q, max(category_alpha, 1e-9), max(other_alpha_sum, 1e-9)))
    high = float(_scipy_stats.beta.ppf(upper_q, max(category_alpha, 1e-9), max(other_alpha_sum, 1e-9)))
    return low, high


__all__ = [
    "DEFAULT_CREDIBLE_INTERVAL_LEVEL",
    "effective_sample_size",
    "weighted_mean",
    "weighted_quantile",
    "BetaBinomialResult",
    "beta_binomial_posterior",
    "dirichlet_multinomial_posterior",
    "dirichlet_marginal_credible_interval",
]
