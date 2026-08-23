from __future__ import annotations

from app.strategy.iofs_components.models import (
    Candle,
    StructureResult,
    TrendResult,
    TriggerResult,
)
from app.strategy.iofs_components.scorer import passes_quality_gate, score_setup
from app.strategy.iofs_components.structure import find_structure_retest
from app.strategy.iofs_components.trend import check_4h_trend
from app.strategy.iofs_components.trigger import check_trigger_candle


def _c(
    close: float,
    *,
    open_: float | None = None,
    high: float | None = None,
    low: float | None = None,
    time: int = 0,
) -> Candle:
    open_price = close if open_ is None else open_
    return Candle(
        open_time=time,
        open=open_price,
        high=max(open_price, close) + 1.0 if high is None else high,
        low=min(open_price, close) - 1.0 if low is None else low,
        close=close,
        volume=10.0,
    )


def _trend_candles(direction: str, count: int = 220) -> list[Candle]:
    candles = []
    for index in range(count):
        close = 100.0 + index if direction == "UP" else 400.0 - index
        candles.append(_c(close, high=close + 1.5, low=close - 1.5, time=index))
    return candles


def _bullish_structure(*, break_index: int = 40, retest: bool = True) -> list[Candle]:
    candles = [_c(95.0, time=index) for index in range(50)]
    candles[30] = _c(95.0, high=100.0, low=94.0, time=30)
    candles[break_index] = _c(
        101.0, open_=99.0, high=102.0, low=98.0, time=break_index
    )
    if retest:
        candles[-1] = _c(100.1, open_=100.6, high=100.8, low=99.7, time=49)
    else:
        candles[-1] = _c(103.0, open_=102.5, high=103.5, low=102.0, time=49)
    return candles


def _bearish_structure(*, break_index: int = 40, retest: bool = True) -> list[Candle]:
    candles = [_c(95.0, time=index) for index in range(50)]
    candles[30] = _c(95.0, high=96.0, low=90.0, time=30)
    candles[break_index] = _c(
        88.8, open_=91.0, high=92.0, low=88.0, time=break_index
    )
    if retest:
        candles[-1] = _c(89.9, open_=89.4, high=90.4, low=89.2, time=49)
    else:
        candles[-1] = _c(87.0, open_=87.5, high=88.0, low=86.5, time=49)
    return candles


class TestTrend:
    def test_bullish_alignment_returns_up_and_ema_separation(self):
        result = check_4h_trend(_trend_candles("UP"))
        assert result.is_aligned is True
        assert result.direction == "UP"
        assert result.adx >= 22
        assert result.ema_sep_pct > 0

    def test_bearish_alignment_returns_down(self):
        result = check_4h_trend(_trend_candles("DOWN"))
        assert result.is_aligned is True
        assert result.direction == "DOWN"

    def test_low_adx_returns_none(self):
        flat = [_c(100.0, high=101.0, low=99.0, time=index) for index in range(220)]
        result = check_4h_trend(flat)
        assert result.is_aligned is False
        assert result.direction == "NONE"
        assert result.adx < 22

    def test_insufficient_or_bad_candles_fail_safely(self):
        assert check_4h_trend(_trend_candles("UP", 199)).reason == "INSUFFICIENT_CANDLES"
        bad = _trend_candles("UP")
        bad[-1] = Candle(None, 100.0, 90.0, 95.0, 100.0, 1.0)
        assert check_4h_trend(bad).reason == "BAD_CANDLES"
        assert check_4h_trend(_trend_candles("UP"), None).reason == "INVALID_INPUT"


class TestStructure:
    def test_detects_bullish_break_retest_and_swing_high(self):
        result = find_structure_retest(_bullish_structure(), "UP", atr=1.0)
        assert result.retest_active is True
        assert result.level == 100.0
        assert result.candles_since_break == 9
        assert result.rejection_strength > 0

    def test_detects_bearish_break_retest_and_swing_low(self):
        result = find_structure_retest(_bearish_structure(), "DOWN", atr=1.0)
        assert result.retest_active is True
        assert result.level == 90.0
        assert result.candles_since_break == 9

    def test_rejects_stale_break(self):
        candles = [_c(95.0, time=index) for index in range(50)]
        candles[5] = _c(95.0, high=100.0, low=94.0, time=5)
        candles[10] = _c(101.0, open_=99.0, high=102.0, low=98.0, time=10)
        assert find_structure_retest(candles, "UP", atr=1.0).reason == "STALE_BREAK"

    def test_rejects_no_broken_level_no_retest_and_bad_atr(self):
        no_break = [_c(95.0, time=index) for index in range(50)]
        no_break[30] = _c(95.0, high=100.0, low=94.0, time=30)
        assert find_structure_retest(no_break, "UP", atr=1.0).reason == "NO_BROKEN_LEVEL"
        assert find_structure_retest(_bullish_structure(retest=False), "UP", atr=1.0).reason == "NO_RETEST"
        assert find_structure_retest(_bullish_structure(), "UP", atr=0).reason == "INVALID_ATR"
        assert find_structure_retest(_bullish_structure(), "UP", atr=None).reason == "INVALID_ATR"

    def test_rejects_no_rejection_and_invalid_direction(self):
        candles = _bullish_structure()
        candles[-1] = _c(100.1, open_=99.9, high=100.3, low=99.9, time=49)
        assert find_structure_retest(candles, "UP", atr=1.0).reason == "NO_REJECTION"
        assert find_structure_retest(candles, "SIDEWAYS", atr=1.0).reason == "INVALID_DIRECTION"


class TestTrigger:
    def test_bullish_engulfing_confirmed_near_level(self):
        candles = [
            _c(99.8, open_=100.4, high=100.5, low=99.7),
            _c(100.6, open_=99.7, high=100.8, low=99.6),
        ]
        result = check_trigger_candle(candles, 100.5, "UP", atr=1.0)
        assert result.is_confirmed is True
        assert result.pattern == "ENGULFING"

    def test_bearish_engulfing_confirmed_near_level(self):
        candles = [
            _c(100.4, open_=99.8, high=100.5, low=99.7),
            _c(99.6, open_=100.5, high=100.6, low=99.4),
        ]
        result = check_trigger_candle(candles, 99.7, "DOWN", atr=1.0)
        assert result.is_confirmed is True
        assert result.pattern == "ENGULFING"

    def test_bullish_and_bearish_pin_bars_confirmed_near_level(self):
        bullish = [
            _c(100.0, open_=100.1, high=100.2, low=99.9),
            _c(100.2, open_=100.0, high=100.3, low=99.4),
        ]
        bearish = [
            _c(100.1, open_=100.0, high=100.2, low=99.9),
            _c(100.0, open_=100.2, high=100.8, low=99.9),
        ]
        assert check_trigger_candle(bullish, 100.1, "UP", 1.0).pattern == "PIN_BAR"
        assert check_trigger_candle(bearish, 100.1, "DOWN", 1.0).pattern == "PIN_BAR"

    def test_rejects_far_zero_body_and_invalid_direction(self):
        candles = [
            _c(99.8, open_=100.4, high=100.5, low=99.7),
            _c(100.6, open_=99.7, high=100.8, low=99.6),
        ]
        assert check_trigger_candle(candles, 105.0, "UP", 1.0).reason == "TOO_FAR_FROM_LEVEL"
        zero_body = [candles[0], _c(100.0, open_=100.0, high=100.5, low=99.5)]
        assert check_trigger_candle(zero_body, 100.0, "UP", 1.0).reason == "ZERO_BODY"
        assert check_trigger_candle(candles, 100.0, "NONE", 1.0).reason == "INVALID_DIRECTION"
        assert check_trigger_candle(candles, 100.0, "UP", None).reason == "INVALID_ATR"


class TestScorer:
    def test_strong_setup_scores_high_and_never_exceeds_100(self):
        score = score_setup(
            TrendResult(True, "UP", 35.0, 0.04),
            StructureResult(True, 100.0, 2, 1.0, 0.05),
            TriggerResult(True, "ENGULFING", 2.0, 99.0, 101.0),
        )
        assert score == 100

    def test_weak_setup_scores_low(self):
        score = score_setup(
            TrendResult(True, "UP", 20.0, 0.001),
            StructureResult(True, 100.0, 25, 0.1, 0.50),
            TriggerResult(True, "PIN_BAR", 2.0, 99.0, 101.0),
        )
        assert score < 65

    def test_profile_thresholds_and_invalid_profile_default(self):
        assert passes_quality_gate(72, "balanced") is True
        assert passes_quality_gate(79, "conservative") is False
        assert passes_quality_gate(80, "conservative") is True
        assert passes_quality_gate(65, "aggressive") is True
        assert passes_quality_gate(71, "unknown") is False

    def test_failed_components_do_not_pass(self):
        score = score_setup(
            TrendResult(False, "NONE", 35.0, 0.04, "FAILED"),
            StructureResult(True, 100.0, 2, 1.0, 0.05),
            TriggerResult(True, "ENGULFING", 2.0, 99.0, 101.0),
        )
        assert score == 0
        assert passes_quality_gate(score) is False
