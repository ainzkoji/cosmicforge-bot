"""Section 12.16 -- historical outcome forecast tests."""
from __future__ import annotations

import inspect
import math
import random

import pytest

from app.replay.cost_model import BINANCE_FUTURES_STANDARD, CostModel
from app.trading_intelligence.contracts.forecast import ForecastStatus, LabelQuality, TerminalOutcome
from app.trading_intelligence.contracts.instrument import CRYPTO, from_symbol_fallback
from app.trading_intelligence.contracts.market_state import CandleSeries
from app.trading_intelligence.contracts.setup import SetupCandidate
from app.trading_intelligence.forecast.cohorts import BACKOFF_LEVELS, cohort_signature, derive_cohort_dimensions
from app.trading_intelligence.forecast.distributions import shrink_toward_broader_mean
from app.trading_intelligence.forecast.engine import build_outcome_forecast
from app.trading_intelligence.forecast.labels import label_candidate
from app.trading_intelligence.forecast.library import HistoricalOutcomeLibrary, LibraryRow, empty_library
from app.trading_intelligence.forecast.ood import population_stability_index, robust_z_score
from app.trading_intelligence.forecast.posterior import (
    beta_binomial_posterior,
    dirichlet_multinomial_posterior,
    effective_sample_size,
)
from app.trading_intelligence.integration.snapshot_adapter import build_data_manifest
from app.trading_intelligence.market_state.engine import build_market_state
from app.trading_intelligence.regime.engine import compute_regime_distribution
from app.trading_intelligence.regime.policy import default_policy

INSTRUMENT = from_symbol_fallback(venue="binance", venue_symbol="BTCUSDT", asset_class=CRYPTO)
POLICY = default_policy()


def _rows(closes, start=1_700_000_000_000, interval=900_000):
    rows = []
    for i, c in enumerate(closes):
        open_t = start + i * interval
        rows.append([open_t, c, c + 0.5, c - 0.5, c, 1000, open_t + interval - 1, 0, 0, 0, 0, 0])
    return rows


def _series_from_rows(rows):
    return CandleSeries(
        open=tuple(float(r[1]) for r in rows), high=tuple(float(r[2]) for r in rows),
        low=tuple(float(r[3]) for r in rows), close=tuple(float(r[4]) for r in rows),
        volume=tuple(float(r[5]) for r in rows), close_time=tuple(int(r[6]) for r in rows),
    )


def _ms_and_regime(rows):
    series = _series_from_rows(rows)
    manifest = build_data_manifest(
        instrument_key=INSTRUMENT, source="Test", timeframe="15m",
        primary_last_closed_candle_time=series.latest_close_time, primary_data_hash="h",
    )
    ms = build_market_state(
        instrument_key=INSTRUMENT, timeframe="15m", decision_time=series.latest_close_time,
        primary_series=series, snapshot_id="ms_test", data_hash="h", manifest=manifest,
    )
    return ms, compute_regime_distribution(ms, POLICY)


def _candidate(ms, side="LONG", trigger=100.0, invalidation=95.0, target=110.0, family="TREND_PULLBACK_V2"):
    return SetupCandidate.build(
        market_state_id=ms.market_state_id, snapshot_id="s", data_hash="h", instrument_key=INSTRUMENT,
        timeframe="15m", decision_time=ms.decision_time, setup_family=family, setup_version="2.0.0",
        setup_policy_hash="p1", side=side, trigger_reference=trigger, structural_invalidation=invalidation,
        target_reference=target,
    )


def _flat_history_rows(seed, n=60):
    rng = random.Random(seed)
    return _rows([100 + 0.1 * j + rng.uniform(-0.3, 0.3) for j in range(n)], start=1_600_000_000_000 + seed * 10_000_000)


def _build_library(n_rows=30, win_fraction=0.7, family="TREND_PULLBACK_V2"):
    rows = []
    n_wins = int(n_rows * win_fraction)
    for i in range(n_rows):
        hist_rows = _flat_history_rows(i)
        ms, regime = _ms_and_regime(hist_rows)
        candidate = _candidate(ms, family=family)
        future_closes = [101, 103, 106, 109, 111, 112] if i < n_wins else [99, 97, 94, 93]
        future = _rows(future_closes, start=candidate.decision_time + 900_000)
        label = label_candidate(candidate, future, cost_model=BINANCE_FUTURES_STANDARD, horizon_bars=10)
        dims = derive_cohort_dimensions(setup_family=family, side="LONG", market_state=ms, regime_distribution=regime)
        rows.append(LibraryRow(label=label, cohort_dimensions=dims, continuous_features={"room_to_target_R": candidate.room_to_target_R}))
    return HistoricalOutcomeLibrary.build(
        tuple(rows), dataset_source_hash="synthetic", candidate_generation_versions={family: "2.0.0"},
        label_policy_version="1.0.0", cost_model_version="1.0.0",
    )


# ---------------------------------------------------------------------------
# Labeling (Section 12.2)
# ---------------------------------------------------------------------------

def test_candidate_independent_of_execution_status():
    """Labeling takes only a SetupCandidate + future rows -- there is no
    parameter for whether a trade was actually taken."""
    sig = inspect.signature(label_candidate)
    for name in sig.parameters:
        assert "executed" not in name.lower() and "trade_taken" not in name.lower()


def test_structural_r_normalization():
    ms, _ = _ms_and_regime(_flat_history_rows(1))
    candidate = _candidate(ms, trigger=100.0, invalidation=95.0, target=110.0)
    assert candidate.initial_structural_risk == 5.0
    label = label_candidate(candidate, _rows([101, 103, 106, 109, 111, 112], start=candidate.decision_time + 900_000), cost_model=CostModel.zero())
    assert label.gross_R == pytest.approx(candidate.room_to_target_R)


def test_long_mfe_mae():
    ms, _ = _ms_and_regime(_flat_history_rows(2))
    candidate = _candidate(ms, side="LONG", trigger=100.0, invalidation=95.0, target=200.0)  # far target -> timeout
    future = _rows([102, 104, 98, 96, 103], start=candidate.decision_time + 900_000)
    label = label_candidate(candidate, future, cost_model=CostModel.zero(), horizon_bars=5)
    assert label.mfe_R >= 0
    assert label.mae_R >= 0


def test_short_mfe_mae():
    ms, _ = _ms_and_regime(_flat_history_rows(3))
    candidate = _candidate(ms, side="SHORT", trigger=100.0, invalidation=105.0, target=0.0)
    future = _rows([98, 96, 102, 103, 97], start=candidate.decision_time + 900_000)
    label = label_candidate(candidate, future, cost_model=CostModel.zero(), horizon_bars=5)
    assert label.mfe_R >= 0
    assert label.mae_R >= 0


def test_target_first_path():
    ms, _ = _ms_and_regime(_flat_history_rows(4))
    candidate = _candidate(ms)
    future = _rows([101, 103, 106, 109, 111, 112], start=candidate.decision_time + 900_000)
    label = label_candidate(candidate, future, cost_model=CostModel.zero())
    assert label.terminal_outcome == TerminalOutcome.TARGET_BEFORE_STOP.value
    assert label.net_profitable is True


def test_stop_first_path():
    ms, _ = _ms_and_regime(_flat_history_rows(5))
    candidate = _candidate(ms)
    future = _rows([99, 97, 94, 93], start=candidate.decision_time + 900_000)
    label = label_candidate(candidate, future, cost_model=CostModel.zero())
    assert label.terminal_outcome == TerminalOutcome.STOP_BEFORE_TARGET.value
    assert label.gross_R == -1.0
    assert label.net_profitable is False


def test_same_bar_ambiguity_is_conservative_stop_first():
    ms, _ = _ms_and_regime(_flat_history_rows(6))
    candidate = _candidate(ms, trigger=100.0, invalidation=95.0, target=110.0)
    future = [[candidate.decision_time + 900_000, 101, 112, 94, 101, 1000, candidate.decision_time + 1_799_999, 0, 0, 0, 0, 0]]
    label = label_candidate(candidate, future, cost_model=CostModel.zero())
    assert label.terminal_outcome == TerminalOutcome.STOP_BEFORE_TARGET.value
    from app.trading_intelligence.contracts.forecast import ForecastReasonCode

    assert ForecastReasonCode.SAME_BAR_CONSERVATIVE_STOP_ASSUMED.value in label.reason_codes


def test_timeout_path_is_its_own_state_not_win_or_loss():
    ms, _ = _ms_and_regime(_flat_history_rows(7))
    candidate = _candidate(ms, trigger=100.0, invalidation=95.0, target=200.0)
    future = _rows([100.5, 101, 100.8, 101.2, 100.9], start=candidate.decision_time + 900_000)
    label = label_candidate(candidate, future, cost_model=CostModel.zero(), horizon_bars=5)
    assert label.terminal_outcome == TerminalOutcome.TIMEOUT.value


def test_censored_horizon_flagged_not_silently_valid():
    ms, _ = _ms_and_regime(_flat_history_rows(8))
    candidate = _candidate(ms, trigger=100.0, invalidation=95.0, target=200.0)
    future = _rows([100.5, 101], start=candidate.decision_time + 900_000)  # far fewer than horizon_bars
    label = label_candidate(candidate, future, cost_model=CostModel.zero(), horizon_bars=48)
    assert label.label_quality == LabelQuality.CENSORED.value


def test_no_observable_future_is_invalid_not_fabricated():
    ms, _ = _ms_and_regime(_flat_history_rows(9))
    candidate = _candidate(ms)
    label = label_candidate(candidate, [], cost_model=CostModel.zero())
    assert label.label_quality == LabelQuality.INVALID.value
    assert label.gross_R == 0.0


def test_cost_components_are_separate_and_nonzero():
    ms, _ = _ms_and_regime(_flat_history_rows(10))
    candidate = _candidate(ms)
    future = _rows([101, 103, 106, 109, 111, 112], start=candidate.decision_time + 900_000)
    label = label_candidate(candidate, future, cost_model=BINANCE_FUTURES_STANDARD)
    assert label.fee_R > 0
    assert label.spread_R > 0
    assert label.slippage_R > 0
    assert label.total_cost_R == pytest.approx(label.fee_R + label.spread_R + label.slippage_R + label.funding_R + label.carry_R)


def test_no_zero_cost_fabrication_when_cost_model_nonzero():
    ms, _ = _ms_and_regime(_flat_history_rows(11))
    candidate = _candidate(ms)
    future = _rows([101, 103, 106, 109, 111, 112], start=candidate.decision_time + 900_000)
    zero_label = label_candidate(candidate, future, cost_model=CostModel.zero())
    real_label = label_candidate(candidate, future, cost_model=BINANCE_FUTURES_STANDARD)
    assert zero_label.total_cost_R == 0.0
    assert real_label.total_cost_R > 0.0
    assert real_label.net_R < real_label.gross_R


# ---------------------------------------------------------------------------
# Cohorts / backoff (Section 12.6)
# ---------------------------------------------------------------------------

def test_backoff_levels_are_explicit_and_decreasing_in_specificity():
    assert len(BACKOFF_LEVELS) == 8
    for i in range(len(BACKOFF_LEVELS) - 1):
        assert len(BACKOFF_LEVELS[i]) >= len(BACKOFF_LEVELS[i + 1])
    assert BACKOFF_LEVELS[-1] == ("setup_family",)


def test_cohort_signature_deterministic():
    dims = {"setup_family": "X", "side": "LONG", "dominant_regime": "TREND_CONTINUATION"}
    sig1 = cohort_signature(dims, ("setup_family", "side"))
    sig2 = cohort_signature(dims, ("setup_family", "side"))
    assert sig1 == sig2
    assert "dominant_regime" not in sig1


# ---------------------------------------------------------------------------
# ESS / weighted quantiles / Beta posterior (Sections 12.7, 12.8)
# ---------------------------------------------------------------------------

def test_effective_sample_size_matches_uniform_weight_count():
    assert effective_sample_size([1.0] * 10) == pytest.approx(10.0)
    assert effective_sample_size([]) == 0.0
    assert effective_sample_size([0.0, 0.0]) == 0.0


def test_beta_posterior_narrows_with_more_support_same_mean():
    small = beta_binomial_posterior(weighted_win_rate=0.8, ess=10, prior_alpha=1, prior_beta=1)
    large = beta_binomial_posterior(weighted_win_rate=0.8, ess=1000, prior_alpha=1, prior_beta=1)
    small_width = small.credible_interval_high - small.credible_interval_low
    large_width = large.credible_interval_high - large.credible_interval_low
    assert large_width < small_width


def test_8_of_10_less_certain_than_800_of_1000():
    small = beta_binomial_posterior(weighted_win_rate=0.8, ess=10, prior_alpha=1, prior_beta=1)
    large = beta_binomial_posterior(weighted_win_rate=0.8, ess=1000, prior_alpha=1, prior_beta=1)
    assert (small.credible_interval_high - small.credible_interval_low) > (large.credible_interval_high - large.credible_interval_low)
    assert small.p_mean == pytest.approx(large.p_mean, abs=0.05)


def test_dirichlet_three_way_sums_to_one():
    result = dirichlet_multinomial_posterior(
        weighted_counts={"A": 0.5, "B": 0.3, "C": 0.2}, ess=20, prior={"A": 1, "B": 1, "C": 1}
    )
    assert math.isclose(sum(result.values()), 1.0, abs_tol=1e-9)


def test_dirichlet_never_infers_one_category_from_the_others_alone():
    """Every category gets its own posterior alpha -- verified by checking a
    category with zero observed weight but nonzero prior still appears."""
    result = dirichlet_multinomial_posterior(weighted_counts={"A": 1.0}, ess=5, prior={"A": 1, "B": 1, "C": 1})
    assert result["B"] > 0
    assert result["C"] > 0


# ---------------------------------------------------------------------------
# Shrinkage (Section 12.11)
# ---------------------------------------------------------------------------

def test_shrinkage_pulls_low_support_toward_broader_mean():
    shrunk_low_support = shrink_toward_broader_mean(local_mean=3.0, broader_mean=0.5, ess=1.0, shrinkage_strength=20.0)
    shrunk_high_support = shrink_toward_broader_mean(local_mean=3.0, broader_mean=0.5, ess=1000.0, shrinkage_strength=20.0)
    assert abs(shrunk_low_support - 0.5) < abs(shrunk_high_support - 0.5)
    assert shrunk_high_support == pytest.approx(3.0, abs=0.1)


# ---------------------------------------------------------------------------
# OOD (Section 12.13)
# ---------------------------------------------------------------------------

def test_robust_z_score_flags_outlier():
    reference = [1.0, 1.1, 0.9, 1.05, 0.95, 1.02, 0.98]
    assert abs(robust_z_score(1.0, reference)) < 1.0
    assert abs(robust_z_score(50.0, reference)) > 5.0


def test_robust_z_score_handles_zero_mad_safely():
    reference = [1.0, 1.0, 1.0, 1.0]
    z = robust_z_score(5.0, reference)
    assert math.isfinite(z)


def test_population_stability_index_zero_for_identical_distributions():
    data = [1.0, 2.0, 3.0, 4.0, 5.0] * 10
    assert population_stability_index(data, data) == pytest.approx(0.0, abs=1e-9)


def test_population_stability_index_positive_for_shifted_distribution():
    ref = list(range(100))
    shifted = [x + 50 for x in range(100)]
    assert population_stability_index(ref, shifted) > 0


# ---------------------------------------------------------------------------
# Full forecast engine (Section 12.12, 12.17)
# ---------------------------------------------------------------------------

def test_forecast_library_unavailable_status():
    ms, regime = _ms_and_regime(_flat_history_rows(20))
    candidate = _candidate(ms)
    forecast = build_outcome_forecast(candidate, ms, regime, None)
    assert forecast.status == ForecastStatus.OUTCOME_LIBRARY_UNAVAILABLE.value


def test_forecast_insufficient_support_with_empty_library():
    ms, regime = _ms_and_regime(_flat_history_rows(21))
    candidate = _candidate(ms)
    lib = empty_library(label_policy_version="1.0.0", cost_model_version="1.0.0")
    forecast = build_outcome_forecast(candidate, ms, regime, lib)
    assert forecast.status == ForecastStatus.INSUFFICIENT_SUPPORT.value


def test_forecast_valid_with_populated_library():
    library = _build_library(n_rows=30, win_fraction=0.7)
    ms, regime = _ms_and_regime(_flat_history_rows(22))
    candidate = _candidate(ms)
    forecast = build_outcome_forecast(candidate, ms, regime, library)
    assert forecast.status == ForecastStatus.VALID.value
    assert forecast.raw_support > 0
    assert 0.0 <= forecast.p_net_profitable_mean <= 1.0
    total = forecast.p_target_before_stop + forecast.p_stop_before_target + forecast.p_timeout
    assert math.isclose(total, 1.0, abs_tol=1e-6)


def test_forecast_is_deterministic_and_replay_reproducible():
    library = _build_library(n_rows=30, win_fraction=0.7)
    ms, regime = _ms_and_regime(_flat_history_rows(23))
    candidate = _candidate(ms)
    f1 = build_outcome_forecast(candidate, ms, regime, library)
    f2 = build_outcome_forecast(candidate, ms, regime, library)
    assert f1.forecast_id == f2.forecast_id
    assert f1.p_net_profitable_mean == f2.p_net_profitable_mean


def test_forecast_has_no_user_or_account_state():
    sig = inspect.signature(build_outcome_forecast)
    forbidden = ("user_id", "bot_id", "bot_instance_id", "capital", "broker_account", "credential")
    for name in sig.parameters:
        lowered = name.lower()
        assert not any(f in lowered for f in forbidden)
    import dataclasses

    from app.trading_intelligence.contracts.forecast import OutcomeForecast

    for f in dataclasses.fields(OutcomeForecast):
        assert not any(term in f.name.lower() for term in ("user_id", "bot_id", "capital", "broker_account"))
