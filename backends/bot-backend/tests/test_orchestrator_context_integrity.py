from __future__ import annotations

import json
from types import SimpleNamespace

from app.core.trading_orchestrator import TradingOrchestrator
from app.risk.system_limits import RiskLevel
from app.strategy.strategy_framework import Signal, StrategyOutput


class _Strategy:
    def __init__(self, signal: Signal):
        self.signal = signal

    def analyze(self, **kwargs):
        return StrategyOutput(
            signal=self.signal,
            confidence=0.81,
            suggested_stop_distance=0.02,
            take_profit_distance=0.044,
            reason="synthetic_reason",
            indicators={"atr": 10.0},
            meta={"session_reason_code": "CRYPTO_SESSION_24_7_BYPASS", "effective_threshold": 0.7},
        )

    @staticmethod
    def validate_output(output):
        return True


def _orchestrator(signal: Signal):
    orchestrator = TradingOrchestrator.__new__(TradingOrchestrator)
    orchestrator.config_id = "cfg-test"
    orchestrator.strategy_id = "master_ensemble"
    orchestrator.broker_id = "binance"
    orchestrator.strategy = _Strategy(signal)
    orchestrator._stop_loss_cooldowns = {}
    orchestrator._current_regimes = {}
    orchestrator.market_analyzer = SimpleNamespace()
    orchestrator.broker_monitor = SimpleNamespace()
    orchestrator.validated_config = SimpleNamespace(
        allowed_symbols=["BTCUSDT"],
        requested_leverage={"BTCUSDT": 3.0},
        paper_mode=True,
        capital_allocation_pct=1.0,
        risk_level=RiskLevel.LOW,
        use_fixed_size=False,
        fixed_size_usdt=None,
        max_trades_per_day=20,
        max_open_positions=5,
    )
    orchestrator.config_validator = SimpleNamespace(get_risk_per_trade_limit=lambda risk: 0.01)
    orchestrator.safety = SimpleNamespace(
        config=SimpleNamespace( fallback_max_leverage=2.0),
        check_daily_activity_fallback=lambda config_id: {"should_activate": False},
    )
    orchestrator.record_decision = lambda **kwargs: None
    return orchestrator


def test_policy_context_receives_actual_per_bot_risk_state():
    orchestrator = _orchestrator(Signal.BUY)
    captured = {}

    def check_pre_trade(**kwargs):
        captured["pretrade"] = kwargs
        return SimpleNamespace(allowed=True, details={}, message="allowed", block_reason=None)

    def evaluate(context):
        captured["policy"] = context
        return SimpleNamespace(
            allowed=False,
            reason="stop after capture",
            reason_code=SimpleNamespace(value="TEST_CAPTURE"),
        )

    orchestrator.safety.check_pre_trade = check_pre_trade
    orchestrator.policy_engine = SimpleNamespace(evaluate=evaluate)

    result = orchestrator.process_trading_opportunity(
        symbol="BTCUSDT",
        klines=[],
        current_price=50_000.0,
        current_equity=1_234.0,
        margin_used=12.0,
        margin_available=1_222.0,
        open_positions=2,
        client=None,
        user_id="user-1",
        bot_instance_id="bot-1",
        broker_account_id="broker-1",
        market_type="CRYPTO",
        execution_mode="paper",
        user_kyc_approved=True,
        live_readiness_approved=True,
        daily_realized_pnl=-17.5,
        daily_trade_count=4,
        kill_switch=True,
        max_daily_loss=55.0,
        max_daily_trades=8,
        max_open_positions=3,
        weekly_drawdown_pct=2.5,
        monthly_drawdown_pct=4.5,
        max_weekly_drawdown_pct=8.0,
        max_monthly_drawdown_pct=12.0,
        min_risk_reward=2.0,
    )

    ctx = captured["policy"]
    assert result["decision"] == "blocked"
    assert ctx.user_id == "user-1"
    assert ctx.equity == 1_234.0
    assert ctx.open_positions_count == 2
    assert ctx.daily_realized_pnl == -17.5
    assert ctx.daily_trade_count == 4
    assert ctx.kill_switch is True
    assert ctx.max_daily_loss == 55.0
    assert ctx.max_daily_trades == 8
    assert ctx.max_open_positions == 3
    assert ctx.weekly_drawdown_pct == 2.5
    assert ctx.monthly_drawdown_pct == 4.5
    assert ctx.max_weekly_drawdown_pct == 8.0
    assert ctx.max_monthly_drawdown_pct == 12.0
    assert ctx.min_risk_reward == 2.0
    assert captured["pretrade"]["user_kyc_approved"] is True
    assert captured["pretrade"]["live_readiness_approved"] is True


def test_kyc_and_live_readiness_default_fail_closed_for_live_config():
    orchestrator = _orchestrator(Signal.BUY)
    orchestrator.validated_config.paper_mode = False
    captured = {}

    def check_pre_trade(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            allowed=False,
            details={},
            message="blocked",
            block_reason=SimpleNamespace(value="live_readiness_requirements_not_met"),
        )

    orchestrator.safety.check_pre_trade = check_pre_trade
    orchestrator.process_trading_opportunity(
        symbol="BTCUSDT",
        klines=[],
        current_price=50_000.0,
        current_equity=1_000.0,
        margin_used=0.0,
        margin_available=1_000.0,
        open_positions=0,
        client=None,
    )

    assert captured["user_kyc_approved"] is False
    assert captured["live_readiness_approved"] is False


def test_strategy_hold_records_risk_as_not_evaluated():
    orchestrator = _orchestrator(Signal.HOLD)
    recorded = {}
    orchestrator.record_decision = lambda **kwargs: recorded.update(kwargs)

    result = orchestrator.process_trading_opportunity(
        symbol="BTCUSDT",
        klines=[],
        current_price=50_000.0,
        current_equity=1_000.0,
        margin_used=0.0,
        margin_available=1_000.0,
        open_positions=0,
        client=None,
        run_id="run-1",
        cycle_id="cycle-1",
        trace_id="trace-1",
        timeframe="15m",
        user_id="user-1",
        bot_instance_id="bot-1",
        broker_account_id="broker-1",
        market_type="CRYPTO",
    )

    risk = json.loads(recorded["risk_gate_decision_json"])
    assert result["decision"] == "hold"
    assert risk["risk_evaluated"] is False
    assert risk["risk_allowed"] is None
    assert risk["session_status"] == "CRYPTO_SESSION_24_7_BYPASS"
    assert risk["bot_instance_id"] == "bot-1"
    assert risk["user_id"] == "user-1"
    assert risk["broker_account_id"] == "broker-1"
    assert risk["run_id"] == "run-1"
    assert risk["cycle_id"] == "cycle-1"
    assert risk["trace_id"] == "trace-1"

