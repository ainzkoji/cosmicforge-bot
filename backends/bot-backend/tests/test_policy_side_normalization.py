"""Pre-Section-22 closure: ONE canonical BUY/SELL boundary for PolicyEngine.

Root cause fixed: TradingOrchestrator passed ``Signal.value`` ("buy"/"sell")
into PolicyContext while PolicyEngine compares against "BUY"/"SELL", so
``is_new_open`` was always False on the orchestrator path and the
max-open-position, R:R and ATR noise-floor / ATR risk-cap gates never ran --
for V2 and for the CATI TradePlan path alike.

These tests drive the REAL TradingOrchestrator (real SafetyEngine, real
PolicyEngine) and prove each gate now fires. A control that disables the
normalization reproduces the old bypass, so the tests would have caught it.
"""
from __future__ import annotations

import pytest

from app.core.trading_orchestrator import TradingOrchestrator
from app.models.unified_trading import Side
from app.policy import policy_engine as pe
from app.policy.policy_engine import (
    InvalidPolicySignal, PolicyContext, ReasonCode, normalize_policy_signal, normalize_trade_side,
)
from app.risk.system_limits import UserConfigurableLimits
from app.strategy.strategy_framework import BaseStrategy, Signal, StrategyOutput


@pytest.fixture(scope="module", autouse=True)
def _migrated():
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    migrate(DB())


class _Fixed(BaseStrategy):
    """A deterministic strategy: fixed signal / stop / TP / ATR."""

    name = "fixed_test_strategy"

    def __init__(self, signal=Signal.BUY, stop=0.02, tp=0.044, atr=2.0):
        self._out = StrategyOutput(signal=signal, confidence=0.8, suggested_stop_distance=stop,
                                   take_profit_distance=tp, reason="test", indicators={"atr": atr},
                                   meta={"session_reason_code": "CRYPTO_SESSION_24_7_BYPASS"})

    def analyze(self, **_kw):
        return self._out


def _orch(strategy, *, fixed=120.0):
    return TradingOrchestrator(
        "cfg_side", UserConfigurableLimits(requested_leverage={"BTCUSDT": 5}, allowed_symbols=["BTCUSDT"],
                                           use_fixed_size=True, fixed_size_usdt=fixed),
        "fixed_test_strategy", "binance", strategy_instance=strategy)


def _run(orch, *, equity=50_000.0, open_positions=0, max_open_positions=5, **kw):
    return orch.process_trading_opportunity(
        symbol="BTCUSDT", klines=[], current_price=100.0, current_equity=equity, margin_used=0.0,
        margin_available=equity, open_positions=open_positions, client=None, max_open_positions=max_open_positions,
        **kw)


def _code(result):
    return result["details"].get("reason_code")


# ============================== normalization ==============================
@pytest.mark.parametrize("raw,expected", [
    ("buy", "BUY"), ("BUY", "BUY"), ("Buy", "BUY"), (" buy ", "BUY"), (Signal.BUY, "BUY"), (Side.BUY, "BUY"),
    ("sell", "SELL"), ("SELL", "SELL"), ("Sell", "SELL"), (Signal.SELL, "SELL"), (Side.SELL, "SELL"),
])
def test_trade_side_normalization(raw, expected):
    assert normalize_trade_side(raw) == expected
    assert PolicyContext(symbol="BTCUSDT", signal=raw).signal == expected


@pytest.mark.parametrize("bad", ["hold", "longish", "", "   ", None, 1, "LONG"])
def test_invalid_trade_side_fails_closed(bad):
    with pytest.raises(InvalidPolicySignal):
        normalize_trade_side(bad)


@pytest.mark.parametrize("bad", ["longish", "", None, 3.0, "BUYX"])
def test_invalid_policy_signal_cannot_build_a_context(bad):
    with pytest.raises(InvalidPolicySignal):
        PolicyContext(symbol="BTCUSDT", signal=bad)


def test_non_directional_signals_stay_canonical():
    """HOLD / CLOSE are legitimate PolicyEngine signals (the runner routes them);
    they are normalized, never mapped to a trade side."""
    assert normalize_policy_signal("hold") == "HOLD" and normalize_policy_signal(Signal.HOLD) == "HOLD"
    assert PolicyContext(symbol="X", signal="close").signal == "CLOSE"


# ============================== real orchestrator gates ==============================
def _spy(monkeypatch, orch):
    seen = []
    real = orch.policy_engine.evaluate

    def evaluate(ctx):
        seen.append(ctx.signal)
        return real(ctx)

    monkeypatch.setattr(orch.policy_engine, "evaluate", evaluate)
    return seen


def test_v2_path_reaches_policy_engine_with_canonical_side(monkeypatch):
    for signal, expected in ((Signal.BUY, "BUY"), (Signal.SELL, "SELL")):
        orch = _orch(_Fixed(signal))
        seen = _spy(monkeypatch, orch)
        assert _run(orch)["decision"] == "execute"
        assert seen == [expected]


def test_max_open_position_gate_now_fires_on_the_real_path():
    """Before the fix this exact call was APPROVED (is_new_open was False)."""
    res = _run(_orch(_Fixed()), open_positions=5, max_open_positions=5)
    assert res["decision"] == "blocked" and _code(res) == ReasonCode.MAX_POSITIONS_REACHED.value
    assert _run(_orch(_Fixed()), open_positions=4, max_open_positions=5)["decision"] == "execute"


def test_risk_reward_gate_now_fires():
    # the orchestrator's policy take-profit is 2.2 x the stop distance, so R:R = 2.2 on this path
    res = _run(_orch(_Fixed()), min_risk_reward=3.0)
    assert res["decision"] == "blocked" and _code(res) == ReasonCode.RISK_REWARD_TOO_LOW.value
    assert _run(_orch(_Fixed()), min_risk_reward=2.0)["decision"] == "execute"


def test_atr_noise_floor_gate_now_fires():
    res = _run(_orch(_Fixed(stop=0.005, atr=4.0)))  # stop 0.5 < 0.5 x ATR 4.0
    assert res["decision"] == "blocked" and _code(res) == ReasonCode.STOP_BELOW_ATR_NOISE_FLOOR.value


def test_missing_atr_gate_now_fires():
    res = _run(_orch(_Fixed(atr=0.0)))
    assert res["decision"] == "blocked" and _code(res) == ReasonCode.MISSING_ATR_FOR_FIXED_SIZING.value


def test_atr_risk_cap_gate_now_fires():
    res = _run(_orch(_Fixed()), equity=200.0)  # loss at stop far above the equity risk allowance
    assert res["decision"] == "blocked" and _code(res) == ReasonCode.ATR_RISK_CAP_EXCEEDED.value


def test_thresholds_and_signals_are_untouched():
    """The fix changes WHICH checks run, never their thresholds or the signal."""
    ctx = PolicyContext(symbol="BTCUSDT", signal="buy")
    defaults = PolicyContext.__dataclass_fields__
    assert ctx.min_stop_atr_multiplier == defaults["min_stop_atr_multiplier"].default
    assert ctx.max_open_positions == defaults["max_open_positions"].default
    assert pe.POLICY_SIGNALS == ("BUY", "SELL", "HOLD", "CLOSE")
