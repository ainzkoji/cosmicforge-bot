"""Section 10.9 -- regime distribution tests."""
from __future__ import annotations

import inspect
import math

import pytest

from app.trading_intelligence.contracts.instrument import CRYPTO, from_symbol_fallback
from app.trading_intelligence.contracts.market_state import CandleSeries
from app.trading_intelligence.integration.snapshot_adapter import build_data_manifest
from app.trading_intelligence.market_state.engine import build_market_state
from app.trading_intelligence.regime.contracts import REGIME_CLASSES, RegimeClass
from app.trading_intelligence.regime.engine import compute_regime_distribution, compute_routing_eligibility
from app.trading_intelligence.regime.policy import RegimePolicy, default_policy

from conftest import make_candle_series

INSTRUMENT = from_symbol_fallback(venue="binance", venue_symbol="BTCUSDT", asset_class=CRYPTO)


def _market_state_from(series, decision_time=None):
    decision_time = decision_time if decision_time is not None else series.latest_close_time
    manifest = build_data_manifest(
        instrument_key=INSTRUMENT, source="Test", timeframe="15m",
        primary_last_closed_candle_time=decision_time, primary_data_hash="h",
    )
    return build_market_state(
        instrument_key=INSTRUMENT, timeframe="15m", decision_time=decision_time,
        primary_series=series, snapshot_id="s", data_hash="h", manifest=manifest,
    )


def _shock_series():
    n = 100
    closes = [100.0 + 0.05 * i for i in range(n)]
    highs = [c + 0.2 for c in closes]
    lows = [c - 0.2 for c in closes]
    highs[-1] = closes[-1] + 20.0
    lows[-1] = closes[-1] - 20.0
    closes[-1] += 15.0
    opens = [closes[i - 1] if i > 0 else closes[0] for i in range(n)]
    return CandleSeries(
        open=tuple(opens), high=tuple(highs), low=tuple(lows), close=tuple(closes),
        volume=tuple(1000.0 for _ in range(n)),
        close_time=tuple(1_700_000_000_000 + i * 900_000 for i in range(n)),
    )


# -- 1, 2 -----------------------------------------------------------------
def test_weights_sum_to_one_and_are_finite_and_bounded():
    ms = _market_state_from(make_candle_series(120, trend=0.8, seed=1))
    dist = compute_regime_distribution(ms, default_policy())
    total = sum(dist.weights.values())
    assert math.isclose(total, 1.0, abs_tol=1e-6)
    for regime in REGIME_CLASSES:
        w = dist.weights[regime.value]
        assert math.isfinite(w)
        assert 0.0 <= w <= 1.0


# -- 3 ----------------------------------------------------------------------
def test_same_market_state_and_policy_gives_identical_distribution_and_hash():
    ms = _market_state_from(make_candle_series(120, trend=0.5, seed=2))
    policy = default_policy()
    d1 = compute_regime_distribution(ms, policy)
    d2 = compute_regime_distribution(ms, policy)
    assert d1.canonical_hash == d2.canonical_hash
    assert d1.regime_distribution_id == d2.regime_distribution_id


# -- 4 ----------------------------------------------------------------------
def test_ambiguous_market_keeps_high_entropy_visible():
    ms = _market_state_from(make_candle_series(120, trend=0.0, vol=2.0, seed=3))
    dist = compute_regime_distribution(ms, default_policy())
    assert dist.entropy > 0.5  # ambiguous evidence must not collapse to near-zero entropy
    # Full distribution accessible, not just the dominant regime.
    assert len(dist.weights) == len(REGIME_CLASSES)


# -- 5 ----------------------------------------------------------------------
def test_gradual_feature_change_produces_gradual_weight_change():
    policy = default_policy()
    base = make_candle_series(120, trend=0.8, vol=1.0, seed=4)
    ms_a = _market_state_from(base)
    dist_a = compute_regime_distribution(ms_a, policy)

    # Perturb only the final close slightly -- a small, gradual change.
    perturbed_close = list(base.close)
    perturbed_close[-1] = perturbed_close[-1] + (perturbed_close[-1] * 0.001)
    series_b = CandleSeries(
        open=base.open, high=base.high, low=base.low, close=tuple(perturbed_close),
        volume=base.volume, close_time=base.close_time,
    )
    ms_b = _market_state_from(series_b)
    dist_b = compute_regime_distribution(ms_b, policy)

    for regime in REGIME_CLASSES:
        delta = abs(dist_a.weights[regime.value] - dist_b.weights[regime.value])
        assert delta < 0.15, f"{regime.value} moved {delta} on a 0.1% price nudge"


# -- 6 ----------------------------------------------------------------------
def test_routing_depends_on_more_than_dominant_label_alone():
    """Two distributions with the same dominant regime but different SHOCK
    weight must not be treated identically by the router -- proving routing
    consults the full distribution, not a collapsed hard label."""
    policy = default_policy()
    weights_calm = {r.value: 0.05 for r in REGIME_CLASSES}
    weights_calm[RegimeClass.TREND_CONTINUATION.value] = 0.4
    total = sum(weights_calm.values())
    weights_calm = {k: v / total for k, v in weights_calm.items()}

    weights_shocky = dict(weights_calm)
    weights_shocky[RegimeClass.SHOCK.value] = 0.4
    weights_shocky[RegimeClass.TREND_CONTINUATION.value] = 0.41
    total2 = sum(weights_shocky.values())
    weights_shocky = {k: v / total2 for k, v in weights_shocky.items()}

    from app.trading_intelligence.regime.contracts import RegimeDistribution

    dist_calm = RegimeDistribution.build(
        market_state_id="m1", instrument_key=INSTRUMENT, timeframe="15m", decision_time=1,
        model_version="1.0.0", policy_hash=policy.policy_hash, weights=weights_calm,
        entropy=0.5, transition_uncertainty=0.2, evidence_by_regime={},
    )
    dist_shocky = RegimeDistribution.build(
        market_state_id="m1", instrument_key=INSTRUMENT, timeframe="15m", decision_time=1,
        model_version="1.0.0", policy_hash=policy.policy_hash, weights=weights_shocky,
        entropy=0.5, transition_uncertainty=0.2, evidence_by_regime={},
    )
    assert dist_calm.dominant_regime == dist_shocky.dominant_regime == RegimeClass.TREND_CONTINUATION.value
    routing_calm = compute_routing_eligibility(dist_calm)
    routing_shocky = compute_routing_eligibility(dist_shocky)
    assert routing_calm.eligibility != routing_shocky.eligibility


# -- 7 ----------------------------------------------------------------------
def test_shock_regime_reacts_to_shock_evidence():
    ms_shock = _market_state_from(_shock_series())
    assert ms_shock.volatility_state.shock_state is True
    dist = compute_regime_distribution(ms_shock, default_policy())
    assert dist.weights[RegimeClass.SHOCK.value] > 1.0 / len(REGIME_CLASSES)


# -- 8 ----------------------------------------------------------------------
def test_transition_unknown_reacts_to_insufficient_quality():
    short_series = make_candle_series(5, seed=5)
    ms = _market_state_from(short_series)
    assert not ms.is_usable
    dist = compute_regime_distribution(ms, default_policy())
    assert dist.dominant_regime == RegimeClass.TRANSITION_UNKNOWN.value
    assert dist.weights[RegimeClass.TRANSITION_UNKNOWN.value] > 0.9


# -- 9, 12 -------------------------------------------------------------------
def test_regime_functions_have_no_tenant_or_broker_parameters():
    forbidden = ("user_id", "bot_id", "bot_instance_id", "capital", "position", "credential", "client")
    for fn in (compute_regime_distribution, compute_routing_eligibility):
        sig = inspect.signature(fn)
        for name in sig.parameters:
            lowered = name.lower()
            assert not any(f in lowered for f in forbidden), f"{fn.__name__} accepts tenant/broker parameter {name}"


# -- 10 -----------------------------------------------------------------------
def test_replay_and_live_identical_market_state_give_identical_regime_output():
    series_a = make_candle_series(120, trend=0.6, vol=1.0, seed=6)
    series_b = make_candle_series(120, trend=0.6, vol=1.0, seed=6)  # same generator args == replay parity
    ms_a = _market_state_from(series_a)
    ms_b = _market_state_from(series_b)
    assert ms_a.canonical_hash == ms_b.canonical_hash
    dist_a = compute_regime_distribution(ms_a, default_policy())
    dist_b = compute_regime_distribution(ms_b, default_policy())
    assert dist_a.canonical_hash == dist_b.canonical_hash


# -- 11 -----------------------------------------------------------------------
def test_router_return_type_contains_only_eligibility_labels_never_an_order():
    ms = _market_state_from(make_candle_series(120, trend=0.5, seed=7))
    dist = compute_regime_distribution(ms, default_policy())
    routing = compute_routing_eligibility(dist)
    from app.trading_intelligence.regime.contracts import SpecialistEligibility

    valid_labels = {e.value for e in SpecialistEligibility}
    for label in routing.eligibility.values():
        assert label in valid_labels


# -- 13 ------------------------------------------------------------------------
def test_invalid_market_state_fails_closed_to_transition_unknown():
    short_series = make_candle_series(3, seed=8)
    ms = _market_state_from(short_series)
    assert ms.data_quality.level.value == "INVALID"
    dist = compute_regime_distribution(ms, default_policy())
    assert dist.dominant_regime == RegimeClass.TRANSITION_UNKNOWN.value
    assert dist.data_quality_level == "INVALID"
    total = sum(dist.weights.values())
    assert math.isclose(total, 1.0, abs_tol=1e-6)
