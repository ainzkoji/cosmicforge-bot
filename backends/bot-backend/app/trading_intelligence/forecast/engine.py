"""Section 12 forecast engine -- cohort lookup with deterministic
hierarchical backoff, Beta-Binomial + Dirichlet-Multinomial posteriors,
robust R-distribution statistics with shrinkage, and OOD assessment,
assembled into one immutable ``OutcomeForecast``.

Pure function of (candidate, market_state, regime_distribution, library) --
no clock, no network, no tenant input, no ML model.
"""
from __future__ import annotations

from typing import Any, List, Optional, Tuple

from app.trading_intelligence.contracts.forecast import (
    ForecastReasonCode,
    ForecastStatus,
    OutcomeForecast,
    TerminalOutcome,
)
from app.trading_intelligence.contracts.setup import SetupCandidate
from app.trading_intelligence.forecast.cohorts import BACKOFF_LEVELS, cohort_signature, derive_cohort_dimensions
from app.trading_intelligence.forecast.distributions import (
    DEFAULT_SHRINKAGE_STRENGTH,
    compute_robust_stats,
    quantile_map,
    shrink_toward_broader_mean,
)
from app.trading_intelligence.forecast.library import HistoricalOutcomeLibrary, LibraryRow
from app.trading_intelligence.forecast.ood import assess_distribution_shift
from app.trading_intelligence.forecast.posterior import (
    DEFAULT_CREDIBLE_INTERVAL_LEVEL,
    beta_binomial_posterior,
    dirichlet_multinomial_posterior,
    effective_sample_size,
    weighted_mean,
)
from app.trading_intelligence.forecast.uncertainty import compute_forecast_uncertainty
from app.trading_intelligence.versions import FORECAST_ENGINE_VERSION

#: RESEARCH DEFAULT: a backoff level "has enough support" once it reaches
#: this many raw rows. Centralized, versioned via FORECAST_ENGINE_VERSION.
MIN_USABLE_RAW_SUPPORT = 15

#: Total pseudo-observations the broader setup-family prior may contribute
#: -- bounded so a huge broader cohort cannot erase local evidence
#: (Section 12.8).
PRIOR_STRENGTH = 10.0

_CATEGORICAL_DIMS: Tuple[str, ...] = (
    "dominant_regime", "volatility_bucket", "liquidity_bucket", "trend_maturity",
    "htf_alignment", "funding_crowding_bucket", "instrument_group",
)


def _rows_matching(library: HistoricalOutcomeLibrary, dims, level_dims) -> List[LibraryRow]:
    sig = cohort_signature(dims, level_dims)
    return [r for r in library.rows if cohort_signature(r.cohort_dimensions, level_dims) == sig]


def _family_rows(library: HistoricalOutcomeLibrary, setup_family: str) -> List[LibraryRow]:
    return [r for r in library.rows if r.label.setup_family == setup_family]


def _unavailable_forecast(candidate: SetupCandidate, status: str, reason_codes: Tuple[str, ...]) -> OutcomeForecast:
    forecast_id = OutcomeForecast.build_id(
        setup_candidate_id=candidate.setup_candidate_id, library_hash="none",
        forecast_version=FORECAST_ENGINE_VERSION, cohort_signature="none",
    )
    return OutcomeForecast(
        forecast_id=forecast_id,
        setup_candidate_id=candidate.setup_candidate_id,
        market_state_id=candidate.market_state_id,
        forecast_version=FORECAST_ENGINE_VERSION,
        library_version="none",
        library_hash="none",
        cohort_signature="none",
        backoff_level=len(BACKOFF_LEVELS) - 1,
        raw_support=0,
        ess=0.0,
        p_net_profitable_mean=0.0,
        credible_interval_low=0.0,
        credible_interval_high=1.0,
        credible_interval_level=DEFAULT_CREDIBLE_INTERVAL_LEVEL,
        p_target_before_stop=0.0,
        p_stop_before_target=0.0,
        p_timeout=1.0,
        forecast_uncertainty=1.0,
        status=status,
        reason_codes=reason_codes,
    )


def build_outcome_forecast(
    candidate: SetupCandidate,
    market_state: Any,
    regime_distribution: Any,
    library: Optional[HistoricalOutcomeLibrary],
    *,
    instrument_group: Optional[str] = None,
    min_usable_raw_support: int = MIN_USABLE_RAW_SUPPORT,
    prior_strength: float = PRIOR_STRENGTH,
    credible_interval_level: float = DEFAULT_CREDIBLE_INTERVAL_LEVEL,
    shrinkage_strength: float = DEFAULT_SHRINKAGE_STRENGTH,
) -> OutcomeForecast:
    if library is None:
        return _unavailable_forecast(
            candidate, ForecastStatus.OUTCOME_LIBRARY_UNAVAILABLE.value,
            (ForecastReasonCode.OUTCOME_LIBRARY_UNAVAILABLE.value,),
        )
    if not market_state.is_usable:
        return _unavailable_forecast(
            candidate, ForecastStatus.INVALID_INPUT.value,
            (ForecastReasonCode.MARKET_STATE_INVALID.value,),
        )

    dims = derive_cohort_dimensions(
        setup_family=candidate.setup_family, side=candidate.side,
        market_state=market_state, regime_distribution=regime_distribution,
        instrument_group=instrument_group,
    )
    return forecast_from_dimensions(
        candidate, dims, library, min_usable_raw_support=min_usable_raw_support,
        prior_strength=prior_strength, credible_interval_level=credible_interval_level,
        shrinkage_strength=shrinkage_strength,
    )


def forecast_from_dimensions(
    candidate: Any,
    dims: Any,
    library: HistoricalOutcomeLibrary,
    *,
    min_usable_raw_support: int = MIN_USABLE_RAW_SUPPORT,
    prior_strength: float = PRIOR_STRENGTH,
    credible_interval_level: float = DEFAULT_CREDIBLE_INTERVAL_LEVEL,
    shrinkage_strength: float = DEFAULT_SHRINKAGE_STRENGTH,
) -> OutcomeForecast:
    """Core of the forecast: cohort lookup onward. ``candidate`` only needs
    setup_candidate_id, market_state_id, setup_family and room_to_target_R,
    so offline calibration can forecast a held-out library row from its own
    stored cohort dimensions without re-deriving them from a MarketState."""
    matched: List[LibraryRow] = []
    level = len(BACKOFF_LEVELS) - 1
    level_dims: Tuple[str, ...] = BACKOFF_LEVELS[-1]
    for lvl, dims_at_level in enumerate(BACKOFF_LEVELS):
        candidate_rows = _rows_matching(library, dims, dims_at_level)
        if len(candidate_rows) >= min_usable_raw_support:
            matched, level, level_dims = candidate_rows, lvl, dims_at_level
            break
        # Keep the best (broadest, non-empty) fallback seen so far in case
        # no level ever reaches the usable-support floor.
        if candidate_rows:
            matched, level, level_dims = candidate_rows, lvl, dims_at_level

    raw_support = len(matched)
    signature = cohort_signature(dims, level_dims)

    if raw_support == 0:
        return _unavailable_forecast(
            candidate, ForecastStatus.INSUFFICIENT_SUPPORT.value,
            (ForecastReasonCode.INSUFFICIENT_SUPPORT.value,),
        )

    weights = [1.0] * raw_support  # V1: uniform analog weighting (see module docstring)
    ess = effective_sample_size(weights)

    # -- Beta-Binomial (Section 12.8) -----------------------------------------
    outcomes01 = [1.0 if r.label.net_profitable else 0.0 for r in matched]
    local_win_rate = weighted_mean(outcomes01, weights) or 0.0

    family_rows = _family_rows(library, candidate.setup_family)
    if family_rows:
        broad_win_rate = sum(1 for r in family_rows if r.label.net_profitable) / len(family_rows)
    else:
        broad_win_rate = 0.5
    prior_alpha0 = broad_win_rate * prior_strength
    prior_beta0 = (1.0 - broad_win_rate) * prior_strength

    beta_result = beta_binomial_posterior(
        weighted_win_rate=local_win_rate, ess=ess,
        prior_alpha=prior_alpha0, prior_beta=prior_beta0,
        credible_interval_level=credible_interval_level,
    )

    # -- Dirichlet-Multinomial three-way (Section 12.9) -----------------------
    n = len(matched)
    counts = {
        TerminalOutcome.TARGET_BEFORE_STOP.value: sum(1 for r in matched if r.label.terminal_outcome == TerminalOutcome.TARGET_BEFORE_STOP.value) / n,
        TerminalOutcome.STOP_BEFORE_TARGET.value: sum(1 for r in matched if r.label.terminal_outcome == TerminalOutcome.STOP_BEFORE_TARGET.value) / n,
        TerminalOutcome.TIMEOUT.value: sum(1 for r in matched if r.label.terminal_outcome == TerminalOutcome.TIMEOUT.value) / n,
    }
    if family_rows:
        fam_n = len(family_rows)
        three_way_prior = {
            cat: prior_strength * (sum(1 for r in family_rows if r.label.terminal_outcome == cat) / fam_n)
            for cat in (TerminalOutcome.TARGET_BEFORE_STOP.value, TerminalOutcome.STOP_BEFORE_TARGET.value, TerminalOutcome.TIMEOUT.value)
        }
    else:
        three_way_prior = {cat: prior_strength / 3.0 for cat in counts}
    three_way = dirichlet_multinomial_posterior(weighted_counts=counts, ess=ess, prior=three_way_prior)

    # -- Robust R distributions + shrinkage (Sections 12.10, 12.11) -----------
    gross_R_values = [r.label.gross_R for r in matched]
    net_R_values = [r.label.net_R for r in matched]
    gross_stats = compute_robust_stats(gross_R_values, weights)
    net_stats = compute_robust_stats(net_R_values, weights)

    broad_gross_mean = weighted_mean([r.label.gross_R for r in family_rows], [1.0] * len(family_rows)) if family_rows else None
    shrunk_gross_mean = shrink_toward_broader_mean(
        local_mean=gross_stats.mean, broader_mean=broad_gross_mean, ess=ess, shrinkage_strength=shrinkage_strength,
    )

    def _conditional_mean(outcome: str) -> Optional[float]:
        subset = [r.label.gross_R for r in matched if r.label.terminal_outcome == outcome]
        if not subset:
            return None
        return sum(subset) / len(subset)

    mfe_values = [r.label.mfe_R for r in matched]
    mae_values = [r.label.mae_R for r in matched]
    tt_target = [r.label.time_to_target_bars for r in matched if r.label.time_to_target_bars is not None]
    tt_stop = [r.label.time_to_stop_bars for r in matched if r.label.time_to_stop_bars is not None]

    # -- Distribution shift / OOD (Section 12.13) -----------------------------
    reference_categories = {
        dim: tuple(sorted({r.cohort_dimensions.get(dim, "UNKNOWN") for r in library.rows})) for dim in _CATEGORICAL_DIMS
    }
    reference_continuous = {}
    for r in matched:
        for key, value in r.continuous_features.items():
            reference_continuous.setdefault(key, []).append(value)
    candidate_continuous = {"room_to_target_R": candidate.room_to_target_R} if candidate.room_to_target_R is not None else {}

    shift = assess_distribution_shift(
        continuous_features=candidate_continuous,
        reference_distributions=reference_continuous,
        candidate_categories={k: v for k, v in dims.items() if k in _CATEGORICAL_DIMS},
        reference_categories=reference_categories,
        required_capabilities_seen_in_reference=(),
        capabilities_available_now=(),
        raw_support=raw_support,
        ess=ess,
        backoff_level=level,
    )

    uncertainty_value, uncertainty_components = compute_forecast_uncertainty(
        credible_interval_width=beta_result.credible_interval_high - beta_result.credible_interval_low,
        ess=ess, backoff_level=level, ood_score=shift.ood_score,
        missing_capability_count=len(shift.capability_shift_flags),
    )

    reason_codes = list(shift.reason_codes)
    if level >= len(BACKOFF_LEVELS) - 3:
        reason_codes.append(ForecastReasonCode.DEEP_BACKOFF.value)

    forecast_id = OutcomeForecast.build_id(
        setup_candidate_id=candidate.setup_candidate_id, library_hash=library.library_hash,
        forecast_version=FORECAST_ENGINE_VERSION, cohort_signature=signature,
    )

    return OutcomeForecast(
        forecast_id=forecast_id,
        setup_candidate_id=candidate.setup_candidate_id,
        market_state_id=candidate.market_state_id,
        forecast_version=FORECAST_ENGINE_VERSION,
        library_version=library.library_version,
        library_hash=library.library_hash,
        cohort_signature=signature,
        backoff_level=level,
        raw_support=raw_support,
        ess=ess,
        p_net_profitable_mean=beta_result.p_mean,
        credible_interval_low=beta_result.credible_interval_low,
        credible_interval_high=beta_result.credible_interval_high,
        credible_interval_level=beta_result.credible_interval_level,
        p_target_before_stop=three_way[TerminalOutcome.TARGET_BEFORE_STOP.value],
        p_stop_before_target=three_way[TerminalOutcome.STOP_BEFORE_TARGET.value],
        p_timeout=three_way[TerminalOutcome.TIMEOUT.value],
        gross_R_mean=shrunk_gross_mean,
        gross_R_median=gross_stats.median,
        gross_R_lower_quantile=gross_stats.lower_quantile,
        gross_R_upper_quantile=gross_stats.upper_quantile,
        net_R_reference_mean=net_stats.mean,
        net_R_reference_median=net_stats.median,
        e_r_given_target=_conditional_mean(TerminalOutcome.TARGET_BEFORE_STOP.value),
        e_r_given_stop=_conditional_mean(TerminalOutcome.STOP_BEFORE_TARGET.value),
        e_r_given_timeout=_conditional_mean(TerminalOutcome.TIMEOUT.value),
        mfe_R_quantiles=quantile_map(mfe_values, weights, (0.1, 0.5, 0.9)),
        mae_R_quantiles=quantile_map(mae_values, weights, (0.1, 0.5, 0.9)),
        time_to_target_quantiles=quantile_map(tt_target, [1.0] * len(tt_target), (0.1, 0.5, 0.9)),
        time_to_stop_quantiles=quantile_map(tt_stop, [1.0] * len(tt_stop), (0.1, 0.5, 0.9)),
        distribution_shift_assessment=shift,
        forecast_uncertainty=uncertainty_value,
        forecast_uncertainty_components=uncertainty_components,
        status=ForecastStatus.VALID.value,
        reason_codes=tuple(dict.fromkeys(reason_codes)),
        calibration_status=library.calibration_status,
    )


__all__ = ["MIN_USABLE_RAW_SUPPORT", "PRIOR_STRENGTH", "build_outcome_forecast", "forecast_from_dimensions"]
