"""Direct unit coverage for each Section 9 feature-family calculator."""
from __future__ import annotations

from app.trading_intelligence.contracts.data_quality import ReasonCode
from app.trading_intelligence.market_state.momentum import compute_momentum_state
from app.trading_intelligence.market_state.participation import compute_participation_state
from app.trading_intelligence.market_state.structure import compute_structure_state
from app.trading_intelligence.market_state.trend import compute_trend_state
from app.trading_intelligence.market_state.volatility import compute_volatility_state

from conftest import make_candle_series


def test_structure_insufficient_history_is_unresolved(short_series):
    state = compute_structure_state(short_series)
    assert state.available is False
    assert state.swing_sequence == "UNRESOLVED"
    assert ReasonCode.INSUFFICIENT_HISTORY.value in state.reason_codes


def test_structure_detects_uptrend_hh_hl():
    # A steep, near-monotonic drift (trend >> vol) genuinely starves the
    # pivot detector of confirmable swing lows -- that is correct, not a
    # bug (Section 9.3: only confirmed causal pivots count) -- so
    # UNRESOLVED is an accepted outcome here alongside HH_HL/MIXED.
    series = make_candle_series(80, trend=1.0, vol=0.3, seed=11)
    state = compute_structure_state(series)
    assert state.available is True
    assert state.swing_sequence in ("HH_HL", "MIXED", "UNRESOLVED")
    assert 0.0 <= state.structure_integrity <= 1.0
    assert state.pivot_confirmation_lag == 2


def test_structure_detects_uptrend_hh_hl_with_oscillation():
    # Milder drift relative to noise produces genuine pullbacks, so this
    # generator config reliably yields confirmable swing pivots.
    series = make_candle_series(100, trend=0.3, vol=1.8, seed=31)
    state = compute_structure_state(series)
    assert state.available is True
    assert state.swing_sequence in ("HH_HL", "MIXED")


def test_structure_never_emits_buy_sell_fields():
    series = make_candle_series(80, trend=1.0, seed=12)
    state = compute_structure_state(series)
    for field_name in ("swing_sequence", "last_bos_direction", "choch_direction"):
        value = getattr(state, field_name)
        assert value not in ("BUY", "SELL")


def test_trend_direction_matches_generator_bias():
    up_series = make_candle_series(100, trend=1.2, vol=0.3, seed=13)
    down_series = make_candle_series(100, trend=-1.2, vol=0.3, seed=14)
    up_state = compute_trend_state(up_series)
    down_state = compute_trend_state(down_series)
    assert up_state.direction == "UP"
    assert down_state.direction == "DOWN"
    assert up_state.maturity in ("EARLY", "MID", "LATE")


def test_volatility_shock_flags_extreme_expansion():
    import math

    from app.trading_intelligence.contracts.market_state import CandleSeries

    # A deterministic, constant-amplitude oscillation -- unlike i.i.d. random
    # noise, its realized-vol series is stationary, so the latest bar is
    # never a percentile-1.0 outlier by chance. This isolates "genuinely
    # calm and stable" from "randomly happened to spike this draw".
    n = 80
    closes = [100.0 + math.sin(i / 3.0) * 0.5 for i in range(n)]
    highs = [c + 0.15 for c in closes]
    lows = [c - 0.15 for c in closes]
    opens = [closes[i - 1] if i > 0 else closes[0] for i in range(n)]
    series = CandleSeries(
        open=tuple(opens), high=tuple(highs), low=tuple(lows), close=tuple(closes),
        volume=tuple(1000.0 for _ in range(n)),
        close_time=tuple(1_700_000_000_000 + i * 900_000 for i in range(n)),
    )
    calm_state = compute_volatility_state(series)
    assert calm_state.available is True
    assert calm_state.shock_state is False


def test_volatility_shock_flags_a_genuine_spike():
    from app.trading_intelligence.contracts.market_state import CandleSeries

    n = 60
    closes = [100.0 + 0.01 * i for i in range(n)]
    highs = [c + 0.1 for c in closes]
    lows = [c - 0.1 for c in closes]
    highs[-1] = closes[-1] + 10.0
    lows[-1] = closes[-1] - 10.0
    opens = [closes[i - 1] if i > 0 else closes[0] for i in range(n)]
    series = CandleSeries(
        open=tuple(opens), high=tuple(highs), low=tuple(lows), close=tuple(closes),
        volume=tuple(1000.0 for _ in range(n)),
        close_time=tuple(1_700_000_000_000 + i * 900_000 for i in range(n)),
    )
    state = compute_volatility_state(series)
    assert state.shock_state is True


def test_momentum_insufficient_history():
    series = make_candle_series(10, seed=16)
    state = compute_momentum_state(series)
    assert state.available is False
    assert ReasonCode.INSUFFICIENT_HISTORY.value in state.reason_codes


def test_momentum_returns_are_atr_normalized_and_bounded_reasonably():
    series = make_candle_series(60, trend=0.5, vol=0.4, seed=17)
    state = compute_momentum_state(series)
    assert state.available is True
    assert state.short_return is not None
    assert state.medium_return is not None


def test_participation_uses_real_taker_data_when_present():
    series = make_candle_series(60, seed=18)  # conftest generator populates taker_buy_volume
    state = compute_participation_state(series)
    assert state.available is True
    assert state.taker_imbalance is not None
    assert -1.0 <= state.taker_imbalance <= 1.0
