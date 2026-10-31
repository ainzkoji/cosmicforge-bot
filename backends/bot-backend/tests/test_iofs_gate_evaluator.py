from __future__ import annotations

from datetime import datetime, timezone

import pytest

import app.strategy.iofs_gate as gate_module
from app.strategy.iofs_components.indicators import calculate_atr
from app.strategy.iofs_components.models import (
    Candle,
    IOFSGateResult,
    StructureResult,
    TrendResult,
    TriggerResult,
)
from app.strategy.iofs_gate import (
    IOFSGateEvaluator,
    gate_result_details,
    is_session_allowed,
    is_symbol_allowed,
)


def _candles(count: int, start: float = 100.0) -> list[Candle]:
    return [
        Candle(index, start, start + 1.0, start - 1.0, start + 0.25, 10.0)
        for index in range(count)
    ]


def _all_timeframes() -> dict[str, list[Candle]]:
    return {"4h": _candles(220), "1h": _candles(50), "15m": _candles(30)}


def _passing_components(monkeypatch, *, score: int = 100) -> None:
    monkeypatch.setattr(
        gate_module,
        "check_4h_trend",
        lambda candles, minimum: TrendResult(True, "UP", 35.0, 0.04),
    )
    monkeypatch.setattr(gate_module, "calculate_atr", lambda candles: 1.0)
    monkeypatch.setattr(
        gate_module,
        "find_structure_retest",
        lambda candles, direction, atr: StructureResult(True, 100.0, 2, 0.8, 0.05),
    )
    monkeypatch.setattr(
        gate_module,
        "check_trigger_candle",
        lambda candles, level, direction, atr: TriggerResult(
            True, "ENGULFING", 2.0, 99.0, 101.0
        ),
    )
    monkeypatch.setattr(gate_module, "score_setup", lambda trend, structure, trigger: score)


class TestATR:
    def test_wilder_atr_uses_true_range(self):
        candles = [
            Candle(0, 100.0, 101.0, 99.0, 100.0, 1.0),
            Candle(1, 100.0, 103.0, 99.0, 102.0, 1.0),
            Candle(2, 102.0, 104.0, 101.0, 103.0, 1.0),
            Candle(3, 103.0, 106.0, 102.0, 105.0, 1.0),
        ]
        assert calculate_atr(candles, period=3) == pytest.approx((4.0 + 3.0 + 4.0) / 3.0)

    def test_atr_unavailable_fails_safely(self):
        assert calculate_atr(_candles(14), period=14) is None
        assert calculate_atr([], period=14) is None


class TestEvaluator:
    def test_passes_when_all_components_and_score_pass(self, monkeypatch):
        _passing_components(monkeypatch)
        result = IOFSGateEvaluator().evaluate(_all_timeframes())
        assert result.passed is True
        assert result.reason == "OK"
        assert result.direction == "UP"
        assert result.score == 100
        assert result.threshold == 72

    def test_uses_separate_1h_and_15m_atr_sources(self, monkeypatch):
        _passing_components(monkeypatch)
        seen = {}
        monkeypatch.setattr(
            gate_module,
            "calculate_atr",
            lambda candles: 1.5 if len(candles) == 50 else 0.5,
        )
        monkeypatch.setattr(
            gate_module,
            "find_structure_retest",
            lambda candles, direction, atr: (
                seen.update(structure_atr=atr)
                or StructureResult(True, 100.0, 2, 0.8, 0.05)
            ),
        )
        monkeypatch.setattr(
            gate_module,
            "check_trigger_candle",
            lambda candles, level, direction, atr: (
                seen.update(trigger_atr=atr)
                or TriggerResult(True, "ENGULFING", 2.0, 99.0, 101.0)
            ),
        )
        assert IOFSGateEvaluator().evaluate(_all_timeframes()).passed is True
        assert seen == {"structure_atr": 1.5, "trigger_atr": 0.5}

    def test_fails_when_trend_not_aligned(self, monkeypatch):
        monkeypatch.setattr(
            gate_module,
            "check_4h_trend",
            lambda candles, minimum: TrendResult(False, "NONE", 10.0, 0.0, "ADX_BELOW_MINIMUM"),
        )
        result = IOFSGateEvaluator().evaluate(_all_timeframes())
        assert result.reason == "TREND_NOT_ALIGNED"
        assert result.structure is None

    def test_fails_when_structure_not_active(self, monkeypatch):
        _passing_components(monkeypatch)
        monkeypatch.setattr(
            gate_module,
            "find_structure_retest",
            lambda candles, direction, atr: StructureResult(
                False, 100.0, 3, 0.0, 0.2, "NO_REJECTION"
            ),
        )
        result = IOFSGateEvaluator().evaluate(_all_timeframes())
        assert result.reason == "STRUCTURE_NOT_ACTIVE"
        assert result.trigger is None

    def test_fails_when_trigger_not_confirmed(self, monkeypatch):
        _passing_components(monkeypatch)
        monkeypatch.setattr(
            gate_module,
            "check_trigger_candle",
            lambda candles, level, direction, atr: TriggerResult(
                False, "NONE", 0.5, 99.0, 101.0, "NO_PATTERN"
            ),
        )
        result = IOFSGateEvaluator().evaluate(_all_timeframes())
        assert result.reason == "TRIGGER_NOT_CONFIRMED"

    def test_fails_when_quality_score_is_below_profile_threshold(self, monkeypatch):
        _passing_components(monkeypatch, score=70)
        balanced = IOFSGateEvaluator().evaluate(_all_timeframes(), "balanced")
        aggressive = IOFSGateEvaluator().evaluate(_all_timeframes(), "aggressive")
        conservative = IOFSGateEvaluator().evaluate(_all_timeframes(), "conservative")
        assert balanced.reason == "QUALITY_SCORE_TOO_LOW"
        assert conservative.threshold == 80
        assert aggressive.passed is True
        assert aggressive.threshold == 65

    def test_atr_unavailable_and_missing_timeframe_fail_closed(self, monkeypatch):
        _passing_components(monkeypatch)
        monkeypatch.setattr(gate_module, "calculate_atr", lambda candles: None)
        assert IOFSGateEvaluator().evaluate(_all_timeframes()).reason == "ATR_UNAVAILABLE"

        missing = _all_timeframes()
        missing.pop("1h")
        assert IOFSGateEvaluator().evaluate(missing).reason == "MISSING_TIMEFRAME"

    def test_invalid_risk_profile_defaults_to_balanced(self, monkeypatch):
        _passing_components(monkeypatch)
        result = IOFSGateEvaluator().evaluate(_all_timeframes(), "unknown")
        assert result.risk_profile == "balanced"
        assert result.threshold == 72


class TestSessionAndSymbols:
    @pytest.mark.parametrize(
        ("hour", "minute", "expected"),
        [(7, 0, True), (9, 59, True), (10, 0, False), (13, 0, True), (16, 0, False)],
    )
    def test_session_boundaries(self, hour, minute, expected):
        now = datetime(2026, 6, 11, hour, minute, tzinfo=timezone.utc)
        assert is_session_allowed("07:00-10:00,13:00-16:00", now) is expected

    def test_allowed_symbols(self):
        allowed = "BTCUSDT,ETHUSDT"
        assert is_symbol_allowed("BTCUSDT", allowed) is True
        assert is_symbol_allowed("ethusdt", allowed) is True
        assert is_symbol_allowed("SOLUSDT", allowed) is False


def _failed_gate() -> IOFSGateResult:
    return IOFSGateResult(
        False, "NONE", 0, "TREND_NOT_ALIGNED", None, None, None, "balanced", 72
    )


def test_logger_payload_contains_required_fields():
    details = gate_result_details("BTCUSDT", "shadow", _failed_gate(), blocked_trade=False)
    required = {
        "symbol", "timestamp_utc", "mode", "passed", "direction", "score", "threshold",
        "reason", "trend_direction", "trend_adx", "trend_ema_sep_pct", "structure_level",
        "structure_retest_active", "structure_retest_distance_atr",
        "structure_candles_since_break", "trigger_confirmed", "trigger_pattern",
        "trigger_wick_ratio", "risk_profile", "blocked_trade",
    }
    assert required <= set(details)


def test_config_defaults_are_safe():
    from app.core.config import Settings

    assert Settings.model_fields["IOFS_GATE_ENABLED"].default is False
    assert Settings.model_fields["IOFS_GATE_MODE"].default == "shadow"
