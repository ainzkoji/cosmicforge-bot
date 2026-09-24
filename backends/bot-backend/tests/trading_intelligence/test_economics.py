"""Section 13.20 -- economic edge & admission authority tests."""
from __future__ import annotations

import dataclasses
import inspect
import math
import random

import pytest

from app.replay.cost_model import BINANCE_FUTURES_STANDARD, CostModel
from app.trading_intelligence.contracts.economics import (
    AdmissionPolicy,
    AdmissionStatus,
    CostEstimate,
    CostScope,
    EconomicOpportunity,
)
from app.trading_intelligence.contracts.forecast import ForecastStatus, OutcomeForecast
from app.trading_intelligence.contracts.instrument import CRYPTO, from_symbol_fallback
from app.trading_intelligence.contracts.market_state import CandleSeries
from app.trading_intelligence.contracts.setup import SetupCandidate
from app.trading_intelligence.economics.costs import build_cost_estimate
from app.trading_intelligence.economics.engine import evaluate_economic_opportunity
from app.trading_intelligence.economics.gates import cost_share_gate, net_edge_gate
from app.trading_intelligence.economics.policy import default_admission_policy
from app.trading_intelligence.forecast.cohorts import derive_cohort_dimensions
from app.trading_intelligence.forecast.engine import build_outcome_forecast
from app.trading_intelligence.forecast.labels import label_candidate
from app.trading_intelligence.forecast.library import HistoricalOutcomeLibrary, LibraryRow
from app.trading_intelligence.integration.snapshot_adapter import build_data_manifest
from app.trading_intelligence.market_state.engine import build_market_state
from app.trading_intelligence.regime.engine import compute_regime_distribution
from app.trading_intelligence.regime.policy import default_policy

INSTRUMENT = from_symbol_fallback(venue="binance", venue_symbol="BTCUSDT", asset_class=CRYPTO)
POLICY = default_policy()


def _rows(closes, start=1_700_000_000_000, interval=900_000):
    return [[start + i * interval, c, c + 0.5, c - 0.5, c, 1000, start + i * interval + interval - 1, 0, 0, 0, 0, 0] for i, c in enumerate(closes)]


def _series(rows):
    return CandleSeries(
        open=tuple(float(r[1]) for r in rows), high=tuple(float(r[2]) for r in rows),
        low=tuple(float(r[3]) for r in rows), close=tuple(float(r[4]) for r in rows),
        volume=tuple(float(r[5]) for r in rows), close_time=tuple(int(r[6]) for r in rows),
    )


def _ms_and_regime(rows):
    series = _series(rows)
    manifest = build_data_manifest(
        instrument_key=INSTRUMENT, source="Test", timeframe="15m",
        primary_last_closed_candle_time=series.latest_close_time, primary_data_hash="h",
    )
    ms = build_market_state(
        instrument_key=INSTRUMENT, timeframe="15m", decision_time=series.latest_close_time,
        primary_series=series, snapshot_id="ms_test", data_hash="h", manifest=manifest,
    )
    return ms, compute_regime_distribution(ms, POLICY)


def _candidate(ms, family="TREND_PULLBACK_V2"):
    return SetupCandidate.build(
        market_state_id=ms.market_state_id, snapshot_id="s", data_hash="h", instrument_key=INSTRUMENT,
        timeframe="15m", decision_time=ms.decision_time, setup_family=family, setup_version="2.0.0",
        setup_policy_hash="p1", side="LONG", trigger_reference=100.0, structural_invalidation=95.0,
        target_reference=110.0,
    )


def _flat_history_rows(seed, n=60):
    rng = random.Random(seed)
    return _rows([100 + 0.1 * j + rng.uniform(-0.3, 0.3) for j in range(n)], start=1_600_000_000_000 + seed * 10_000_000)


def _build_library(n_rows=40, win_fraction=0.8, family="TREND_PULLBACK_V2"):
    rows = []
    n_wins = int(n_rows * win_fraction)
    for i in range(n_rows):
        hist_rows = _flat_history_rows(i)
        ms, regime = _ms_and_regime(hist_rows)
        candidate = _candidate(ms, family=family)
        future_closes = [101, 103, 106, 109, 111, 112] if i < n_wins else [99, 97, 94, 93]
        future = _rows(future_closes, start=candidate.decision_time + 900_000)
        label = label_candidate(candidate, future, cost_model=CostModel.zero(), horizon_bars=10)
        dims = derive_cohort_dimensions(setup_family=family, side="LONG", market_state=ms, regime_distribution=regime)
        rows.append(LibraryRow(label=label, cohort_dimensions=dims, continuous_features={"room_to_target_R": candidate.room_to_target_R}))
    return HistoricalOutcomeLibrary.build(
        tuple(rows), dataset_source_hash="synthetic", candidate_generation_versions={family: "2.0.0"},
        label_policy_version="1.0.0", cost_model_version="1.0.0",
    )


def _live_setup(seed=999):
    rows = _flat_history_rows(seed)
    ms, regime = _ms_and_regime(rows)
    candidate = _candidate(ms)
    return candidate, ms, regime


def _full_opportunity(*, win_fraction=0.8, cost_model=BINANCE_FUTURES_STANDARD, policy=None, liquidity_verified=False):
    library = _build_library(win_fraction=win_fraction)
    candidate, ms, regime = _live_setup()
    forecast = build_outcome_forecast(candidate, ms, regime, library)
    cost_estimate = build_cost_estimate(candidate, cost_model=cost_model, liquidity_verified=liquidity_verified)
    opportunity = evaluate_economic_opportunity(candidate, ms, forecast, cost_estimate, policy=policy)
    return opportunity, candidate, ms, forecast, cost_estimate


# ---------------------------------------------------------------------------
# EV equations (Section 13.2)
# ---------------------------------------------------------------------------

def test_gross_ev_equation_matches_manual_computation():
    opportunity, candidate, ms, forecast, cost_estimate = _full_opportunity()
    e_target = forecast.e_r_given_target
    e_stop = forecast.e_r_given_stop if forecast.e_r_given_stop is not None else -1.0
    e_timeout = forecast.e_r_given_timeout if forecast.e_r_given_timeout is not None else 0.0
    expected = forecast.p_target_before_stop * e_target + forecast.p_stop_before_target * e_stop + forecast.p_timeout * e_timeout
    assert opportunity.ev_gross_r == pytest.approx(expected)


def test_timeout_and_negative_stop_contribute_to_gross_ev():
    """A synthetic forecast with nonzero p_timeout and negative E[R|stop]
    must move EV_gross_R away from the target-only value."""
    from app.trading_intelligence.contracts.forecast import ForecastStatus

    candidate, ms, regime = _live_setup()
    forecast = OutcomeForecast(
        forecast_id="f1", setup_candidate_id=candidate.setup_candidate_id, market_state_id=ms.market_state_id,
        forecast_version="1.0.0", library_version="1.0.0", library_hash="h", cohort_signature="sig", backoff_level=0,
        raw_support=100, ess=100.0, p_net_profitable_mean=0.5, credible_interval_low=0.4, credible_interval_high=0.6,
        credible_interval_level=0.9, p_target_before_stop=0.4, p_stop_before_target=0.4, p_timeout=0.2,
        e_r_given_target=2.0, e_r_given_stop=-1.0, e_r_given_timeout=-0.3,
        status=ForecastStatus.VALID.value,
    )
    cost_estimate = build_cost_estimate(candidate, cost_model=CostModel.zero())
    opportunity = evaluate_economic_opportunity(candidate, ms, forecast, cost_estimate)
    expected = 0.4 * 2.0 + 0.4 * (-1.0) + 0.2 * (-0.3)
    assert opportunity.ev_gross_r == pytest.approx(expected)
    assert opportunity.ev_gross_r < 0.4 * 2.0  # timeout/stop pulled it down from target-only


def test_costs_are_summed_and_never_double_counted():
    opportunity, candidate, ms, forecast, cost_estimate = _full_opportunity(cost_model=BINANCE_FUTURES_STANDARD)
    assert opportunity.cost_r == pytest.approx(cost_estimate.total_cost_R)
    assert opportunity.ev_net_r == pytest.approx(opportunity.ev_gross_r - opportunity.cost_r)
    # Subtracted exactly once: net + cost recovers gross with no residual.
    assert (opportunity.ev_net_r + opportunity.cost_r) == pytest.approx(opportunity.ev_gross_r)


def test_conservative_edge_applies_all_three_penalties_once():
    opportunity, *_ = _full_opportunity()
    expected = opportunity.ev_net_r - opportunity.uncertainty_penalty_r - opportunity.distribution_shift_penalty_r - opportunity.execution_uncertainty_penalty_r
    assert opportunity.conservative_edge_r == pytest.approx(expected)


# ---------------------------------------------------------------------------
# Gate failures (Section 13.6-13.13)
# ---------------------------------------------------------------------------

def test_support_gate_failure_yields_insufficient_evidence():
    candidate, ms, regime = _live_setup()
    empty_lib = HistoricalOutcomeLibrary.build(
        (), dataset_source_hash="x", candidate_generation_versions={}, label_policy_version="1.0.0", cost_model_version="1.0.0",
    )
    forecast = build_outcome_forecast(candidate, ms, regime, empty_lib)
    assert forecast.status == ForecastStatus.INSUFFICIENT_SUPPORT.value
    cost_estimate = build_cost_estimate(candidate, cost_model=BINANCE_FUTURES_STANDARD)
    opportunity = evaluate_economic_opportunity(candidate, ms, forecast, cost_estimate)
    assert opportunity.admission_status == AdmissionStatus.INSUFFICIENT_EVIDENCE.value


def test_ci_width_gate_failure():
    tight_policy = dataclasses.replace(default_admission_policy(), maximum_credible_interval_width=0.001)
    opportunity, *_ = _full_opportunity(policy=tight_policy)
    ci_gate = next(g for g in opportunity.gate_results if g.gate == "PROBABILITY_UNCERTAINTY")
    assert ci_gate.passed is False
    assert opportunity.admission_status == AdmissionStatus.INSUFFICIENT_EVIDENCE.value


def test_net_edge_gate_fails_on_negative_gross_ev():
    candidate, ms, regime = _live_setup()
    forecast = OutcomeForecast(
        forecast_id="f1", setup_candidate_id=candidate.setup_candidate_id, market_state_id=ms.market_state_id,
        forecast_version="1.0.0", library_version="1.0.0", library_hash="h", cohort_signature="sig", backoff_level=0,
        raw_support=100, ess=100.0, p_net_profitable_mean=0.2, credible_interval_low=0.1, credible_interval_high=0.3,
        credible_interval_level=0.9, p_target_before_stop=0.1, p_stop_before_target=0.9, p_timeout=0.0,
        e_r_given_target=2.0, e_r_given_stop=-1.0, e_r_given_timeout=0.0,
        status=ForecastStatus.VALID.value,
    )
    gate = net_edge_gate(ev_gross_r=(0.1 * 2.0 + 0.9 * -1.0), ev_net_r=-100, policy=default_admission_policy())
    assert gate.passed is False
    assert gate.reason_code == "NEGATIVE_GROSS_EV"


def test_conservative_edge_gate_failure():
    tight_policy = dataclasses.replace(default_admission_policy(), minimum_conservative_edge_r=1000.0)
    opportunity, *_ = _full_opportunity(policy=tight_policy)
    gate = next(g for g in opportunity.gate_results if g.gate == "CONSERVATIVE_EDGE")
    assert gate.passed is False
    assert opportunity.admission_status == AdmissionStatus.ECONOMICALLY_INADMISSIBLE.value


def test_reward_geometry_gate_failure_on_malformed_geometry():
    tight_policy = dataclasses.replace(default_admission_policy(), minimum_room_to_target_r=1000.0)
    opportunity, *_ = _full_opportunity(policy=tight_policy)
    gate = next(g for g in opportunity.gate_results if g.gate == "REWARD_GEOMETRY")
    assert gate.passed is False


def test_cost_share_gate_failure_and_no_division_by_zero():
    gate, share = cost_share_gate(ev_gross_r=0.0, cost_r=1.0, policy=default_admission_policy())
    assert gate.passed is False
    assert share is None  # never divided by zero
    gate2, share2 = cost_share_gate(ev_gross_r=-1.0, cost_r=1.0, policy=default_admission_policy())
    assert gate2.passed is False
    assert share2 is None


def test_cost_share_gate_high_share_fails():
    tight_policy = dataclasses.replace(default_admission_policy(), maximum_cost_share=0.001)
    opportunity, *_ = _full_opportunity(policy=tight_policy, cost_model=BINANCE_FUTURES_STANDARD)
    gate = next(g for g in opportunity.gate_results if g.gate == "COST_SHARE")
    assert gate.passed is False


def test_ood_gate_failure():
    tight_policy = dataclasses.replace(default_admission_policy(), maximum_ood_score=-1.0)
    opportunity, *_ = _full_opportunity(policy=tight_policy)
    gate = next(g for g in opportunity.gate_results if g.gate == "DISTRIBUTION_SHIFT")
    assert gate.passed is False
    assert opportunity.admission_status == AdmissionStatus.INSUFFICIENT_EVIDENCE.value


def test_data_quality_gate_failure_on_invalid_market_state():
    candidate, ms, regime = _live_setup()
    from app.trading_intelligence.contracts.data_quality import DataQuality, DataQualityLevel

    invalid_ms = dataclasses.replace(ms, data_quality=DataQuality(level=DataQualityLevel.INVALID, reason_codes=("X",)))
    library = _build_library()
    forecast = build_outcome_forecast(candidate, ms, regime, library)
    cost_estimate = build_cost_estimate(candidate, cost_model=BINANCE_FUTURES_STANDARD)
    opportunity = evaluate_economic_opportunity(candidate, invalid_ms, forecast, cost_estimate)
    gate = next(g for g in opportunity.gate_results if g.gate == "DATA_QUALITY")
    assert gate.passed is False
    assert opportunity.admission_status == AdmissionStatus.INSUFFICIENT_EVIDENCE.value


def test_cost_quality_gate_failure_on_zero_cost_model():
    opportunity, *_ = _full_opportunity(cost_model=CostModel.zero())
    gate = next(g for g in opportunity.gate_results if g.gate == "COST_QUALITY")
    assert gate.passed is False


# ---------------------------------------------------------------------------
# Positive / insufficient-evidence cases (Section 13.20)
# ---------------------------------------------------------------------------

def test_positive_admissible_case():
    opportunity, *_ = _full_opportunity(win_fraction=0.85)
    assert opportunity.admission_status == AdmissionStatus.ECONOMICALLY_ADMISSIBLE.value
    assert all(g.passed for g in opportunity.gate_results)


def test_insufficient_evidence_case_distinct_from_negative_edge():
    """Insufficient evidence (no library) must be a DIFFERENT status from a
    well-evidenced but uneconomical candidate."""
    candidate, ms, regime = _live_setup()
    forecast_missing = build_outcome_forecast(candidate, ms, regime, None)
    cost_estimate = build_cost_estimate(candidate, cost_model=BINANCE_FUTURES_STANDARD)
    insufficient = evaluate_economic_opportunity(candidate, ms, forecast_missing, cost_estimate)
    assert insufficient.admission_status == AdmissionStatus.INSUFFICIENT_EVIDENCE.value

    bad_library = _build_library(win_fraction=0.05)
    forecast_bad = build_outcome_forecast(candidate, ms, regime, bad_library)
    inadmissible = evaluate_economic_opportunity(candidate, ms, forecast_bad, cost_estimate)
    assert inadmissible.admission_status != AdmissionStatus.INSUFFICIENT_EVIDENCE.value


# ---------------------------------------------------------------------------
# Identity / determinism (Section 13.15, 13.20)
# ---------------------------------------------------------------------------

def test_economic_opportunity_id_is_deterministic():
    opportunity1, candidate, ms, forecast, cost_estimate = _full_opportunity()
    opportunity2 = evaluate_economic_opportunity(candidate, ms, forecast, cost_estimate)
    assert opportunity1.economic_opportunity_id == opportunity2.economic_opportunity_id


def test_identical_input_reproduces_identical_economic_output():
    library = _build_library()
    candidate, ms, regime = _live_setup()
    forecast = build_outcome_forecast(candidate, ms, regime, library)
    cost_estimate = build_cost_estimate(candidate, cost_model=BINANCE_FUTURES_STANDARD)
    o1 = evaluate_economic_opportunity(candidate, ms, forecast, cost_estimate)
    o2 = evaluate_economic_opportunity(candidate, ms, forecast, cost_estimate)
    assert o1.ev_gross_r == o2.ev_gross_r
    assert o1.conservative_edge_r == o2.conservative_edge_r
    assert o1.admission_status == o2.admission_status


def test_account_scoped_cost_identity_carries_tenant_fields():
    candidate, ms, regime = _live_setup()
    cost_estimate = build_cost_estimate(
        candidate, cost_model=BINANCE_FUTURES_STANDARD, cost_scope=CostScope.ACCOUNT.value,
        user_id="user_1", broker_account_id="acct_1", bot_instance_id="bot_1",
    )
    library = _build_library()
    forecast = build_outcome_forecast(candidate, ms, regime, library)
    opportunity = evaluate_economic_opportunity(
        candidate, ms, forecast, cost_estimate, user_id="user_1", broker_account_id="acct_1", bot_instance_id="bot_1",
    )
    assert opportunity.user_id == "user_1"
    assert opportunity.broker_account_id == "acct_1"


def test_market_reference_cost_identity_never_fabricates_tenant():
    candidate, ms, regime = _live_setup()
    cost_estimate = build_cost_estimate(candidate, cost_model=BINANCE_FUTURES_STANDARD, cost_scope=CostScope.REFERENCE_RESEARCH.value)
    assert cost_estimate.user_id is None
    assert cost_estimate.broker_account_id is None
    library = _build_library()
    forecast = build_outcome_forecast(candidate, ms, regime, library)
    opportunity = evaluate_economic_opportunity(candidate, ms, forecast, cost_estimate)
    assert opportunity.user_id is None
    assert opportunity.broker_account_id is None


def test_account_cost_estimate_requires_tenant_ids():
    with pytest.raises(ValueError):
        CostEstimate(
            cost_estimate_id="c1", instrument_key=INSTRUMENT, venue="binance", cost_scope=CostScope.ACCOUNT.value,
            fee_R=0.0, spread_R=0.0, slippage_R=0.0, funding_R=0.0, carry_R=0.0, total_cost_R=0.0,
            cost_uncertainty_R=0.0, cost_model_version="1.0.0", cost_policy_hash="p", source_quality="VALID",
        )


def test_reference_cost_estimate_rejects_tenant_ids():
    with pytest.raises(ValueError):
        CostEstimate(
            cost_estimate_id="c1", instrument_key=INSTRUMENT, venue="binance", cost_scope=CostScope.REFERENCE_RESEARCH.value,
            fee_R=0.0, spread_R=0.0, slippage_R=0.0, funding_R=0.0, carry_R=0.0, total_cost_R=0.0,
            cost_uncertainty_R=0.0, cost_model_version="1.0.0", cost_policy_hash="p", source_quality="VALID",
            user_id="user_1",
        )


# ---------------------------------------------------------------------------
# No old threshold authority (Section 13.16, 13.20)
# ---------------------------------------------------------------------------

def test_economics_module_never_imports_v2_threshold_or_master_ensemble():
    import ast

    import app.trading_intelligence.economics.engine as engine_module
    import app.trading_intelligence.economics.gates as gates_module
    import app.trading_intelligence.economics.policy as policy_module

    forbidden_substrings = ("adaptiveentrythresholdengine", "master_ensemble", "masterensemblestrategy")
    for module in (engine_module, gates_module, policy_module):
        source = inspect.getsource(module).lower()
        for forbidden in forbidden_substrings:
            assert forbidden not in source, f"{module.__name__} references {forbidden}"


def test_economics_engine_has_no_client_or_execution_parameters():
    sig = inspect.signature(evaluate_economic_opportunity)
    forbidden = ("client", "execute", "order", "capital")
    for name in sig.parameters:
        lowered = name.lower()
        assert not any(f in lowered for f in forbidden)


def test_shadow_path_cannot_place_orders():
    """EconomicOpportunity has no order id, no execution method, nothing a
    caller could mistake for order confirmation."""
    fields = {f.name for f in dataclasses.fields(EconomicOpportunity)}
    forbidden = {"order_id", "client_order_id", "execution_id", "fill_id"}
    assert not (fields & forbidden)
