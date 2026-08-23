from __future__ import annotations

from app.strategy.base import Signal
from app.strategy.hold_breakdown import component_breakdown
from app.strategy.sma_cross import SMACrossStrategy


class CandleClient:
    def __init__(self, closes: list[float]):
        self.closes = closes

    def klines(self, symbol: str, interval: str = "15m", limit: int = 120):
        rows = [
            [index * 900_000, close, close + 1, close - 1, close, 1000]
            for index, close in enumerate(self.closes)
        ]
        return rows[-limit:]


def test_clear_bullish_momentum_setup_creates_buy_component_signal():
    closes = [100.0] * 119 + [110.0]
    result = SMACrossStrategy(CandleClient(closes), interval="15m").get_signal("BTCUSDT")

    assert result.signal == Signal.BUY
    assert result.confidence > 0


def test_clear_bearish_momentum_setup_creates_sell_component_signal():
    closes = [100.0] * 119 + [90.0]
    result = SMACrossStrategy(CandleClient(closes), interval="15m").get_signal("ETHUSDT")

    assert result.signal == Signal.SELL
    assert result.confidence > 0


def test_valid_indicators_do_not_produce_zero_confidence_by_default():
    closes = [100.0] * 119 + [110.0]
    result = SMACrossStrategy(CandleClient(closes), interval="15m").get_signal("BTCUSDT")

    assert result.meta["fast_sma_current"] > result.meta["slow_sma_current"]
    assert result.confidence == 0.65


def test_missing_indicators_report_insufficient_data_not_silent_no_pattern():
    result = SMACrossStrategy(CandleClient([100.0] * 10), interval="15m").get_signal("BTCUSDT")
    diagnostic = component_breakdown(
        strategy="sma_cross",
        signal=result.signal.value,
        confidence=result.confidence,
        reason=result.reason,
        meta=result.meta,
        threshold_floor=0.55,
    )

    assert diagnostic["component_signal"] == "INSUFFICIENT_DATA"
    assert diagnostic["component_failed_conditions"] == ["required_indicator_data"]


def test_hold_breakdown_reports_exact_failed_condition():
    diagnostic = component_breakdown(
        strategy="sma_cross",
        signal="HOLD",
        confidence=0.0,
        reason="no_cross",
        meta={"fast_sma_current": 100.0, "slow_sma_current": 100.0},
        threshold_floor=0.55,
    )

    assert diagnostic["component_failed_conditions"] == ["fresh_fast_slow_sma_cross"]

