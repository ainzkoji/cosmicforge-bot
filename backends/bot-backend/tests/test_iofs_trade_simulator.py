from __future__ import annotations

import pytest

from app.strategy.iofs_components.models import Candle
from scripts.validation.iofs_trade_simulator import create_trade_plan, simulate_trade


def _candle(
    open_: float,
    high: float,
    low: float,
    close: float,
    *,
    timestamp: int = 0,
) -> Candle:
    return Candle(timestamp, open_, high, low, close, 10.0)


def _long_plan() -> dict:
    return {
        "valid": True,
        "direction": "UP",
        "entry": 100.0,
        "sl": 99.0,
        "tp1": 101.0,
        "tp2": 102.0,
        "be_stop": 100.2,
        "risk": 1.0,
        "be_buffer_r": 0.2,
    }


def test_accepted_setup_creates_valid_long_trade_levels():
    plan = create_trade_plan(
        direction="UP",
        structure_level=99.0,
        atr_15m=1.0,
        entry_candle=_candle(100.0, 101.0, 99.5, 100.5),
    )
    assert plan["valid"] is True
    assert plan["sl"] < plan["entry"] < plan["tp1"] < plan["tp2"]


def test_accepted_setup_creates_valid_short_trade_levels():
    plan = create_trade_plan(
        direction="DOWN",
        structure_level=101.0,
        atr_15m=1.0,
        entry_candle=_candle(100.0, 100.5, 99.0, 99.5),
    )
    assert plan["valid"] is True
    assert plan["sl"] > plan["entry"] > plan["tp1"] > plan["tp2"]


def test_sl_before_tp1_produces_sl_outcome():
    result = simulate_trade(_long_plan(), [_candle(100.0, 100.5, 98.8, 99.2)])
    assert result["outcome"] == "SL"
    assert result["r_multiple"] == -1.0


def test_tp1_then_tp2_produces_tp2_outcome():
    candles = [
        _candle(100.0, 101.1, 99.8, 100.9),
        _candle(100.9, 102.1, 100.5, 102.0, timestamp=1),
    ]
    result = simulate_trade(_long_plan(), candles)
    assert result["outcome"] == "TP2"
    assert result["tp1_hit"] is True
    assert result["r_multiple"] == 1.5


def test_tp1_then_be_buffer_hit_produces_break_even_buffer():
    candles = [
        _candle(100.0, 101.1, 99.8, 100.9),
        _candle(100.9, 101.2, 100.1, 100.3, timestamp=1),
    ]
    result = simulate_trade(_long_plan(), candles)
    assert result["outcome"] == "BREAK_EVEN_BUFFER"
    assert result["r_multiple"] == pytest.approx(0.6)


def test_same_candle_ambiguity_is_conservative():
    result = simulate_trade(_long_plan(), [_candle(100.0, 101.2, 98.8, 101.0)])
    assert result["outcome"] == "SL"
    assert result["ambiguous_candle"] is True
