from __future__ import annotations

from types import SimpleNamespace

import app.core.config as config_module
from app.strategy.base import Signal, SignalResult
from app.strategy.hold_breakdown import component_breakdown
from app.strategy.master_ensemble import MasterEnsembleStrategy
from app.strategy.regime import MarketRegime
from app.strategy.regime import RegimeClassifier, RegimeResult, RegimeThresholds, TrendDirection
from scripts.validation.run_paper_cycle_diagnostic import execution_reachability


class StaticClient:
    def __init__(self):
        self.rows = [
            [index * 900_000, 100 + index, 102 + index, 99 + index, 101 + index, 1000]
            for index in range(250)
        ]

    def klines(self, symbol: str, interval: str = "15m", limit: int = 250):
        return self.rows[-limit:]


class StaticStrategy:
    def __init__(self, signal: Signal, confidence: float):
        self.signal = signal
        self.confidence = confidence
        self.interval = "15m"

    def get_signal(self, symbol: str):
        return SignalResult(self.signal, self.confidence, "synthetic", meta={"source": "synthetic"})


def _ensemble(monkeypatch, directional: Signal) -> MasterEnsembleStrategy:
    monkeypatch.setattr(config_module.settings, "ENSEMBLE_MIN_THRESHOLD_FLOOR", 0.55)
    monkeypatch.setattr(config_module.settings, "ENSEMBLE_BLOCKED_REGIMES", "")
    monkeypatch.setattr(config_module.settings, "ENSEMBLE_SESSION_FILTER_ENABLED", False)
    monkeypatch.setattr(config_module.settings, "ENSEMBLE_SESSION_WINDOWS_UTC", "00:00-24:00")

    ensemble = MasterEnsembleStrategy(StaticClient())
    ensemble._threshold_calc = SimpleNamespace(
        get_threshold=lambda symbol: SimpleNamespace(threshold=0.40, bound_label="synthetic"),
        record=lambda symbol, confidence: None,
    )
    classifier = SimpleNamespace(
        classify_stable=lambda highs, lows, closes: SimpleNamespace(
            regime=MarketRegime.WEAK_TREND,
            regime_confidence=0.8,
            adx=30.0,
            atr_percent=1.0,
            ma_slope=0.2,
            compression_ratio=0.8,
            breakout_pressure=0.2,
        )
    )
    monkeypatch.setattr(ensemble, "_get_classifier", lambda symbol: classifier)
    ensemble._strategies = {
        "supertrend": StaticStrategy(directional, 0.90),
        "trend_pullback": StaticStrategy(directional, 0.90),
        "sma_cross": StaticStrategy(Signal.HOLD, 0.0),
        "donchian_breakout": StaticStrategy(Signal.HOLD, 0.0),
    }
    return ensemble


def test_component_buy_can_flow_into_ensemble_decision(monkeypatch):
    result = _ensemble(monkeypatch, Signal.BUY).get_signal("BTCUSDT")

    assert result.signal == Signal.BUY
    assert result.confidence > 0.55


def test_component_sell_can_flow_into_ensemble_decision(monkeypatch):
    result = _ensemble(monkeypatch, Signal.SELL).get_signal("ETHUSDT")

    assert result.signal == Signal.SELL
    assert result.confidence > 0.55


def test_ensemble_does_not_overwrite_buy_in_paper_mode(monkeypatch):
    monkeypatch.setattr(config_module.settings, "EXECUTION_MODE", "paper")
    result = _ensemble(monkeypatch, Signal.BUY).get_signal("BTCUSDT")

    assert result.signal == Signal.BUY


def test_disabled_component_reports_disabled():
    diagnostic = component_breakdown(
        strategy="bollinger_reversion",
        signal="DISABLED",
        confidence=0.0,
        reason="disabled_for_regime:WEAK_TREND",
        meta={},
        threshold_floor=0.55,
        enabled=False,
    )

    assert diagnostic["component_signal"] == "DISABLED"
    assert diagnostic["component_enabled"] is False


def test_iofs_shadow_does_not_overwrite_buy():
    result = execution_reachability(
        strategy_action="BUY",
        gate_allowed=True,
        iofs_mode="shadow",
        iofs_passed=False,
        ml_enabled=False,
    )

    assert result["executor_would_be_called"] is True
    assert result["iofs_shadow_non_blocking"] is True


def test_ml_disabled_does_not_overwrite_sell():
    result = execution_reachability(
        strategy_action="SELL",
        gate_allowed=True,
        iofs_mode="shadow",
        iofs_passed=True,
        ml_enabled=False,
        ml_blocked=True,
    )

    assert result["executor_would_be_called"] is True
    assert result["ml_disabled_non_blocking"] is True


def test_component_diagnostic_contains_required_runtime_fields():
    diagnostic = component_breakdown(
        strategy="trend_pullback",
        signal="HOLD",
        confidence=0.0,
        reason="adx_too_low",
        meta={"adx": 18.0},
        threshold_floor=0.55,
        symbol="BTCUSDT",
        timestamp="2026-06-14T10:00:00+00:00",
        timeframe="15m",
        market_regime="WEAK_TREND",
        session_allowed=True,
    )

    required = {
        "symbol",
        "timestamp",
        "timeframe",
        "market_regime",
        "session_allowed",
        "component_name",
        "component_enabled",
        "component_signal",
        "component_confidence",
        "component_reason",
        "component_required_conditions",
        "component_failed_conditions",
        "indicator_snapshot",
    }
    assert required <= diagnostic.keys()


def test_regime_hysteresis_promotes_repeated_candidate(monkeypatch):
    classifier = RegimeClassifier(RegimeThresholds(stability_candles=2))

    def result(regime: MarketRegime) -> RegimeResult:
        return RegimeResult(
            regime=regime,
            trend_dir=TrendDirection.NONE,
            regime_confidence=0.8,
            adx=20.0,
            atr_percent=1.0,
            ma_slope=0.0,
            compression_ratio=0.5,
            breakout_pressure=0.0,
            details={},
        )

    raw = iter(
        [
            result(MarketRegime.WEAK_TREND),
            result(MarketRegime.RANGE),
            result(MarketRegime.RANGE),
        ]
    )
    monkeypatch.setattr(classifier, "classify", lambda highs, lows, closes: next(raw))

    first = classifier.classify_stable([], [], [])
    stabilizing = classifier.classify_stable([], [], [])
    promoted = classifier.classify_stable([], [], [])

    assert first.regime == MarketRegime.WEAK_TREND
    assert stabilizing.regime == MarketRegime.WEAK_TREND
    assert stabilizing.details["candidate_regime"] == "RANGE"
    assert promoted.regime == MarketRegime.RANGE
