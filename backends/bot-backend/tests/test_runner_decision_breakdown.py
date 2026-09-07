from __future__ import annotations

import inspect

from app.runner.runner import PaperRunner
from app.strategy.base import Signal
from app.strategy.hold_breakdown import build_hold_breakdown, component_breakdown
from app.strategy.master_ensemble import MasterEnsembleStrategy


def test_hold_decision_includes_canonical_hold_reason():
    result = MasterEnsembleStrategy._hold(
        "BTCUSDT",
        "SESSION_BLOCKED",
        meta={
            "regime": "WEAK_TREND",
            "threshold": 0.55,
            "session_gate_result": "blocked",
        },
    )

    assert result.signal == Signal.HOLD
    assert result.meta["hold_reason"] == "SESSION_BLOCKED"
    assert result.meta["failed_conditions"] == ["SESSION_BLOCKED"]


def test_component_hold_breakdown_preserves_reason_and_indicators():
    component = component_breakdown(
        strategy="trend_pullback",
        signal="HOLD",
        confidence=0.0,
        reason="adx_too_low",
        meta={"adx": 18.0, "threshold": 25.0},
        threshold_floor=0.55,
    )

    assert component["reason"] == "adx_too_low"
    assert component["indicator_values"]["adx"] == 18.0
    assert component["failed_conditions"] == ["TREND_FILTER_FAILED"]


def test_structured_hold_breakdown_contains_required_fields():
    payload = build_hold_breakdown(
        symbol="ETHUSDT",
        raw_strategy_signal="hold",
        raw_confidence=0.52,
        final_action="hold",
        reason="master_ensemble_v2",
        meta={
            "regime": "WEAK_TREND",
            "session_gate_result": "allowed",
            "ensemble_threshold_floor": 0.55,
            "regime_gate_blocked_regimes": ["STRONG_TREND"],
            "adx": 30.0,
            "buy_score": 0.52,
            "sell_score": 0.0,
        },
        timestamp="2026-06-14T12:00:00+00:00",
    )

    required = {
        "symbol",
        "timestamp",
        "regime",
        "session_allowed",
        "raw_strategy_signal",
        "raw_confidence",
        "final_action",
        "hold_reason",
        "indicator_values",
        "failed_conditions",
        "threshold_floor",
        "blocked_regime",
    }
    assert required <= set(payload)
    assert payload["hold_reason"] == "CONFIDENCE_BELOW_FLOOR"


def test_runner_emits_structured_hold_breakdown_log():
    source = inspect.getsource(PaperRunner._step_symbol_orchestrated)

    assert "[HOLD_BREAKDOWN]" in source
    assert "build_hold_breakdown" in source
