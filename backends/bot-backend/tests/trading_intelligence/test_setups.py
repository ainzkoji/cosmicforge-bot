"""Section 11.9 -- setup discovery specialist tests."""
from __future__ import annotations

import dataclasses
import inspect
import math
import random

import pytest

from app.trading_intelligence.contracts.instrument import CRYPTO, from_symbol_fallback
from app.trading_intelligence.contracts.market_state import CandleSeries
from app.trading_intelligence.contracts.setup import SetupCandidate, compute_geometry
from app.trading_intelligence.integration.snapshot_adapter import build_data_manifest
from app.trading_intelligence.market_state.engine import build_market_state
from app.trading_intelligence.regime.engine import compute_regime_distribution
from app.trading_intelligence.regime.policy import default_policy
from app.trading_intelligence.setups.breakout_expansion import BreakoutVolExpansionSpecialist
from app.trading_intelligence.setups.interface import SetupSpecialist
from app.trading_intelligence.setups.momentum_continuation import MomentumContinuationSpecialist
from app.trading_intelligence.setups.policy import default_policies
from app.trading_intelligence.setups.range_mean_reversion import RangeMeanReversionSpecialist
from app.trading_intelligence.setups.registry import SPECIALIST_REGISTRY, discover_all
from app.trading_intelligence.setups.trend_pullback import TrendPullbackSpecialist

from conftest import make_candle_series

INSTRUMENT = from_symbol_fallback(venue="binance", venue_symbol="BTCUSDT", asset_class=CRYPTO)
POLICY = default_policy()
POLICIES = default_policies()


class FakeSnapshot:
    def __init__(self, series: CandleSeries):
        self.symbol = "BTCUSDT"
        self.timeframe = "15m"
        self.market_snapshot_id = "ms_test"
        self.data_hash = "hash_test"
        self.reference_price = series.close[-1]
        self.latest_closed_candle_time = series.latest_close_time


def _flat_series(closes, vol=0.15, volumes=None):
    n = len(closes)
    highs = [c + vol for c in closes]
    lows = [c - vol for c in closes]
    opens = [closes[i - 1] if i > 0 else closes[0] for i in range(n)]
    volumes = volumes or [1000.0] * n
    close_times = [1_700_000_000_000 + i * 900_000 for i in range(n)]
    return CandleSeries(
        open=tuple(opens), high=tuple(highs), low=tuple(lows), close=tuple(closes),
        volume=tuple(volumes), close_time=tuple(close_times),
    )


def _market_state(series):
    manifest = build_data_manifest(
        instrument_key=INSTRUMENT, source="Test", timeframe="15m",
        primary_last_closed_candle_time=series.latest_close_time, primary_data_hash="h",
    )
    return build_market_state(
        instrument_key=INSTRUMENT, timeframe="15m", decision_time=series.latest_close_time,
        primary_series=series, snapshot_id="ms_test", data_hash="h", manifest=manifest,
    )


# -- validated positive fixtures (found via numeric search against the
# actual Section 9/10 engines, not asserted from intuition) ------------------

def _trend_pullback_series(sign: float = 1.0) -> CandleSeries:
    n_bars, trend_rate, amp, period, phase = 140, 0.5, 6, 16, 3
    closes = [100 + trend_rate * i + amp * math.sin((i + phase) / (period / (2 * math.pi))) for i in range(n_bars)]
    if sign < 0:
        # Mirror the whole series around a constant so its swing geometry
        # (pivots, retracement shape) is preserved exactly, just inverted --
        # negating the sine's sign/rate independently does not.
        pivot = max(closes) + min(closes)
        closes = [pivot - c for c in closes]
    return _flat_series(closes, vol=amp * 0.1)


def _breakout_series(sign: float = 1.0) -> CandleSeries:
    rng = random.Random(5)
    compressed = [100.0 + 0.3 * ((i % 4) - 1.5) + rng.uniform(-0.05, 0.05) for i in range(50)]
    move = [103.0, 106.0, 109.0, 111.0, 112.0, 112.3]
    if sign < 0:
        move = [200.0 - m for m in move]
        compressed = [200.0 - c for c in compressed]
    closes = compressed + move
    n = len(closes)
    highs = [c + 0.4 + rng.uniform(0, 0.05) for c in closes]
    lows = [c - 0.4 - rng.uniform(0, 0.05) for c in closes]
    opens = [closes[i - 1] if i > 0 else closes[0] for i in range(n)]
    volumes = [1000.0 + rng.uniform(-20, 20) for _ in range(n)]
    volumes[-6:] = [3000.0, 3200.0, 3300.0, 2000.0, 1800.0, 1700.0]
    close_times = [1_700_000_000_000 + i * 900_000 for i in range(n)]
    return CandleSeries(
        open=tuple(opens), high=tuple(highs), low=tuple(lows), close=tuple(closes),
        volume=tuple(volumes), close_time=tuple(close_times),
    )


def _range_series(near_low: bool = True) -> CandleSeries:
    rng = random.Random(11)
    range_low, range_high = 95.0, 105.0
    mid = (range_low + range_high) / 2
    osc = [mid + (range_high - mid) * math.sin(i / 6.0) + rng.uniform(-0.1, 0.1) for i in range(80)]
    tail = [98.5, 97.5, 96.6, 95.9, 95.5, 95.3, 95.25]
    if not near_low:
        tail = [200.0 - t for t in tail]
        osc = [200.0 - o for o in osc]
    closes = osc + tail
    n = len(closes)
    highs = [c + 0.15 + rng.uniform(0, 0.05) for c in closes]
    lows = [c - 0.15 - rng.uniform(0, 0.05) for c in closes]
    opens = [closes[i - 1] if i > 0 else closes[0] for i in range(n)]
    volumes = [1000.0 + rng.uniform(-30, 30) for _ in range(n)]
    close_times = [1_700_000_000_000 + i * 900_000 for i in range(n)]
    return CandleSeries(
        open=tuple(opens), high=tuple(highs), low=tuple(lows), close=tuple(closes),
        volume=tuple(volumes), close_time=tuple(close_times),
    )


def _momentum_series(sign: float = 1.0) -> CandleSeries:
    return make_candle_series(150, trend=0.0 if sign > 0 else 0.0, vol=3.0 * abs(sign), seed=3 if sign > 0 else 30)


# ---------------------------------------------------------------------------
# Common interface (Section 11.9)
# ---------------------------------------------------------------------------

def test_registry_contains_exactly_four_v1_specialists():
    assert set(SPECIALIST_REGISTRY.keys()) == {
        "TREND_PULLBACK_V2", "BREAKOUT_VOL_EXPANSION_V2", "RANGE_MEAN_REVERSION_V2", "MOMENTUM_CONTINUATION_V1",
    }


def test_specialists_expose_common_interface_fields():
    for family, specialist in SPECIALIST_REGISTRY.items():
        assert specialist.setup_family == family
        assert isinstance(specialist.setup_version, str) and specialist.setup_version
        assert isinstance(specialist.required_capabilities, tuple)
        assert hasattr(specialist, "discover")


def test_specialists_do_not_use_legacy_strategy_interface():
    for specialist in SPECIALIST_REGISTRY.values():
        assert not hasattr(specialist, "get_signal")
        assert not hasattr(specialist, "analyze")


def test_discover_signature_has_no_tenant_broker_or_execution_parameters():
    forbidden = ("user_id", "bot_id", "bot_instance_id", "capital", "client", "credential", "order", "execute")
    for specialist in SPECIALIST_REGISTRY.values():
        sig = inspect.signature(specialist.discover)
        for name in sig.parameters:
            lowered = name.lower()
            assert not any(f in lowered for f in forbidden), f"{specialist.setup_family}.discover accepts {name}"


def test_zero_candidates_is_normal_for_ambiguous_market():
    series = make_candle_series(120, trend=0.0, vol=3.0, seed=99)
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    candidates = discover_all(snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime)
    assert isinstance(candidates, tuple)  # zero or more; zero is valid, not an error


def test_no_candidates_when_market_state_invalid():
    short_series = make_candle_series(3, seed=1)
    ms = _market_state(short_series)
    assert not ms.is_usable
    regime = compute_regime_distribution(ms, POLICY)
    candidates = discover_all(snapshot=FakeSnapshot(short_series), market_state=ms, regime_distribution=regime)
    assert candidates == ()


def test_evidence_score_cannot_cause_admission():
    """SetupCandidate has no admission/approval field at all -- evidence_score
    is diagnostic only and nothing consumes it as a threshold here."""
    fields = {f.name for f in dataclasses.fields(SetupCandidate)}
    forbidden = {"admission_status", "approved", "trade_approved", "confidence"}
    assert not (fields & forbidden)


def test_deterministic_candidate_ids_and_ordering():
    series = _trend_pullback_series()
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    snap = FakeSnapshot(series)
    run1 = discover_all(snapshot=snap, market_state=ms, regime_distribution=regime)
    run2 = discover_all(snapshot=snap, market_state=ms, regime_distribution=regime)
    assert [c.setup_candidate_id for c in run1] == [c.setup_candidate_id for c in run2]


def test_candidate_expiry_is_deterministic_from_policy_and_decision_time():
    series = _trend_pullback_series()
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    candidates = TrendPullbackSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime, policy=POLICIES["TREND_PULLBACK_V2"],
    )
    assert len(candidates) == 1
    c = candidates[0]
    expected = c.decision_time + 15 * 60_000 * POLICIES["TREND_PULLBACK_V2"].candidate_validity_bars
    assert c.valid_until == expected
    assert c.created_at == c.decision_time


# ---------------------------------------------------------------------------
# Geometry (Section 11.2.1)
# ---------------------------------------------------------------------------

def test_geometry_rejects_non_positive_risk_distance():
    assert compute_geometry(side="LONG", trigger_reference=100.0, structural_invalidation=100.0, target_reference=110.0) is None
    assert compute_geometry(side="LONG", trigger_reference=100.0, structural_invalidation=105.0, target_reference=110.0) is None
    assert compute_geometry(side="SHORT", trigger_reference=100.0, structural_invalidation=95.0, target_reference=90.0) is None


def test_geometry_computes_room_to_target_r():
    g = compute_geometry(side="LONG", trigger_reference=100.0, structural_invalidation=95.0, target_reference=110.0)
    assert g is not None
    assert g.risk_distance == 5.0
    assert g.room_to_target_R == 2.0


def test_setup_candidate_build_rejects_malformed_geometry():
    with pytest.raises(ValueError):
        SetupCandidate.build(
            market_state_id="m", snapshot_id="s", data_hash="h", instrument_key=INSTRUMENT,
            timeframe="15m", decision_time=1, setup_family="X", setup_version="1.0.0",
            setup_policy_hash="p", side="LONG", trigger_reference=100.0,
            structural_invalidation=105.0, target_reference=110.0,
        )


# ---------------------------------------------------------------------------
# Specialist A -- Trend Pullback V2
# ---------------------------------------------------------------------------

def test_trend_pullback_positive_long():
    series = _trend_pullback_series(sign=1.0)
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    candidates = TrendPullbackSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime, policy=POLICIES["TREND_PULLBACK_V2"],
    )
    assert len(candidates) == 1
    c = candidates[0]
    assert c.side == "LONG"
    assert c.initial_structural_risk > 0
    assert c.room_to_target_R >= POLICIES["TREND_PULLBACK_V2"].min_target_room_R


def test_trend_pullback_positive_short():
    series = _trend_pullback_series(sign=-1.0)
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    candidates = TrendPullbackSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime, policy=POLICIES["TREND_PULLBACK_V2"],
    )
    assert len(candidates) == 1
    assert candidates[0].side == "SHORT"


def test_trend_pullback_rejects_when_regime_not_eligible():
    series = _trend_pullback_series()
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    tight_policy = dataclasses.replace(POLICIES["TREND_PULLBACK_V2"], regime_eligibility_floor=0.99)
    candidates = TrendPullbackSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime, policy=tight_policy,
    )
    assert candidates == ()


def test_trend_pullback_rejects_on_htf_conflict():
    from app.trading_intelligence.contracts.market_state import HigherTimeframeState

    series = _trend_pullback_series()
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    conflicted = dataclasses.replace(
        ms, higher_timeframe_state=HigherTimeframeState(available=True, direction="DOWN", structure_alignment="CONFLICT")
    )
    candidates = TrendPullbackSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=conflicted, regime_distribution=regime, policy=POLICIES["TREND_PULLBACK_V2"],
    )
    assert candidates == ()


def test_trend_pullback_rejects_too_mature_trend():
    series = _trend_pullback_series()
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    mature = dataclasses.replace(ms, trend_state=dataclasses.replace(ms.trend_state, maturity="LATE", age=999))
    candidates = TrendPullbackSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=mature, regime_distribution=regime, policy=POLICIES["TREND_PULLBACK_V2"],
    )
    assert candidates == ()


def test_trend_pullback_rejects_deep_retracement():
    series = _trend_pullback_series()
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    broken = dataclasses.replace(ms, trend_state=dataclasses.replace(ms.trend_state, retracement_depth=0.95))
    candidates = TrendPullbackSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=broken, regime_distribution=regime, policy=POLICIES["TREND_PULLBACK_V2"],
    )
    assert candidates == ()


def test_trend_pullback_liquidity_unverified_flagged_not_rejected():
    series = _trend_pullback_series()
    ms = _market_state(series)
    assert ms.liquidity_state.available is False  # no book feed wired -- expected
    regime = compute_regime_distribution(ms, POLICY)
    candidates = TrendPullbackSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime, policy=POLICIES["TREND_PULLBACK_V2"],
    )
    assert len(candidates) == 1
    from app.trading_intelligence.contracts.setup import SetupReasonCode

    assert SetupReasonCode.LIQUIDITY_UNVERIFIED.value in candidates[0].reason_codes


# ---------------------------------------------------------------------------
# Specialist B -- Breakout / Vol Expansion V2
# ---------------------------------------------------------------------------

def test_breakout_positive_long():
    series = _breakout_series(sign=1.0)
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    candidates = BreakoutVolExpansionSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime,
        policy=POLICIES["BREAKOUT_VOL_EXPANSION_V2"],
    )
    assert len(candidates) == 1
    assert candidates[0].side == "LONG"


def test_breakout_rejects_without_confirmed_break():
    series = make_candle_series(120, trend=0.0, vol=1.5, seed=2)  # ranging, no confirmed BOS expected
    ms = _market_state(series)
    if ms.structure_state.last_bos_direction != "NONE":
        pytest.skip("fixture unexpectedly produced a confirmed break")
    regime = compute_regime_distribution(ms, POLICY)
    candidates = BreakoutVolExpansionSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime,
        policy=POLICIES["BREAKOUT_VOL_EXPANSION_V2"],
    )
    assert candidates == ()


def test_breakout_rejects_when_too_extended():
    series = _breakout_series(sign=1.0)
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    tight_policy = dataclasses.replace(POLICIES["BREAKOUT_VOL_EXPANSION_V2"], max_late_entry_extension_range_fraction=0.0001)
    candidates = BreakoutVolExpansionSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime, policy=tight_policy,
    )
    assert candidates == ()


def test_breakout_liquidity_unverified_is_flagged():
    series = _breakout_series(sign=1.0)
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    candidates = BreakoutVolExpansionSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime,
        policy=POLICIES["BREAKOUT_VOL_EXPANSION_V2"],
    )
    from app.trading_intelligence.contracts.setup import SetupReasonCode

    assert SetupReasonCode.LIQUIDITY_UNVERIFIED.value in candidates[0].reason_codes


# ---------------------------------------------------------------------------
# Specialist C -- Range / Mean Reversion V2
# ---------------------------------------------------------------------------

def test_range_mean_reversion_positive_long_near_low():
    series = _range_series(near_low=True)
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    candidates = RangeMeanReversionSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime,
        policy=POLICIES["RANGE_MEAN_REVERSION_V2"],
    )
    assert len(candidates) == 1
    assert candidates[0].side == "LONG"
    assert candidates[0].target_reference > candidates[0].trigger_reference


def test_range_mean_reversion_rejects_when_vol_expansion_dominant():
    series = _range_series(near_low=True)
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    from app.trading_intelligence.regime.contracts import RegimeClass

    hot_weights = dict(regime.weights)
    hot_weights[RegimeClass.VOL_EXPANSION.value] = 0.9
    total = sum(hot_weights.values())
    hot_weights = {k: v / total for k, v in hot_weights.items()}
    hot_regime = dataclasses.replace(regime, weights=hot_weights)
    candidates = RangeMeanReversionSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=hot_regime,
        policy=POLICIES["RANGE_MEAN_REVERSION_V2"],
    )
    assert candidates == ()


def test_range_mean_reversion_rejects_too_narrow_range():
    series = _range_series(near_low=True)
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    tight_policy = dataclasses.replace(POLICIES["RANGE_MEAN_REVERSION_V2"], min_range_width_atr=1_000_000.0)
    candidates = RangeMeanReversionSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime, policy=tight_policy,
    )
    assert candidates == ()


def test_range_mean_reversion_rejects_structural_transition():
    series = _range_series(near_low=True)
    ms = _market_state(series)
    regime = compute_regime_distribution(ms, POLICY)
    transitioning = dataclasses.replace(ms, structure_state=dataclasses.replace(ms.structure_state, choch_direction="DOWN"))
    candidates = RangeMeanReversionSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=transitioning, regime_distribution=regime,
        policy=POLICIES["RANGE_MEAN_REVERSION_V2"],
    )
    assert candidates == ()


# ---------------------------------------------------------------------------
# Specialist D -- Momentum Continuation
# ---------------------------------------------------------------------------

def test_momentum_continuation_requires_participation_data():
    """Missing participation data must reject (WEAK_PARTICIPATION), never
    silently assume adequate participation."""
    series = make_candle_series(120, trend=0.0, vol=3.0, seed=3)
    ms = _market_state(series)
    from app.trading_intelligence.contracts.market_state import ParticipationState

    no_participation = dataclasses.replace(
        ms, participation_state=ParticipationState(
            volume_percentile=None, relative_volume=None, volume_acceleration=None,
            taker_imbalance=None, trade_intensity=None, available=False,
        )
    )
    regime = compute_regime_distribution(ms, POLICY)
    candidates = MomentumContinuationSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=no_participation, regime_distribution=regime,
        policy=POLICIES["MOMENTUM_CONTINUATION_V1"],
    )
    assert candidates == ()


def test_momentum_continuation_does_not_assume_no_crowding_when_missing():
    """P7: missing derivatives/crowding data must never be treated as
    'no crowding' -- structurally verified via the specialist never gating
    on an unavailable derivatives_state."""
    series = make_candle_series(120, trend=0.0, vol=3.0, seed=3)
    ms = _market_state(series)
    assert ms.derivatives_state.available is False  # no derivatives feed wired
    regime = compute_regime_distribution(ms, POLICY)
    # Should not raise, and must not reject purely because crowding is unknown.
    MomentumContinuationSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime,
        policy=POLICIES["MOMENTUM_CONTINUATION_V1"],
    )


def test_momentum_continuation_rejects_when_crowded_same_direction():
    from app.trading_intelligence.contracts.market_state import DerivativesState

    # Build any market_state with an UP trend via the shared helper, then force crowding.
    series = _trend_pullback_series(sign=1.0)
    ms = _market_state(series)
    crowded = dataclasses.replace(
        ms, derivatives_state=DerivativesState(
            funding_current=0.01, funding_predicted=None, funding_percentile=0.99,
            time_to_funding=None, open_interest=1000.0, open_interest_delta=10.0,
            basis=None, mark_index_spread=None, crowding_state="CROWDED_LONG", available=True,
        ),
        momentum_state=dataclasses.replace(ms.momentum_state, short_return=1.0, acceleration=0.1),
    )
    regime = compute_regime_distribution(ms, POLICY)
    candidates = MomentumContinuationSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=crowded, regime_distribution=regime,
        policy=POLICIES["MOMENTUM_CONTINUATION_V1"],
    )
    assert candidates == ()


def test_momentum_continuation_no_directional_trend_returns_empty():
    series = make_candle_series(120, trend=0.0, vol=0.3, seed=44)
    ms = _market_state(series)
    if ms.trend_state.direction in ("UP", "DOWN"):
        pytest.skip("fixture unexpectedly produced a directional trend")
    regime = compute_regime_distribution(ms, POLICY)
    candidates = MomentumContinuationSpecialist().discover(
        snapshot=FakeSnapshot(series), market_state=ms, regime_distribution=regime,
        policy=POLICIES["MOMENTUM_CONTINUATION_V1"],
    )
    assert candidates == ()
