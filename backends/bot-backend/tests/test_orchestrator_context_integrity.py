"""The orchestrator hands hard risk the ACTUAL per-bot risk state, and its
KYC / live-readiness inputs fail closed.

The scalar strategy entry (``process_trading_opportunity``) is retired: the
one entry into hard risk is ``process_trade_plan`` (a real Section 18
TradePlan -> ``_evaluate_hard_risk``). These tests drive that path with the
real orchestrator and capture what Layer A (SafetyEngine.check_pre_trade) and
PolicyEngine receive.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core.trading_orchestrator import TradingOrchestrator

_TI = Path(__file__).resolve().parent / "trading_intelligence"
if str(_TI) not in sys.path:
    sys.path.append(str(_TI))


@pytest.fixture
def h(tmp_path):
    from _exec import Harness

    return Harness(tmp_path)


def _run(h, *, equity=1_000.0, margin_used=0.0, open_positions=0, **kw):
    from app.trading_intelligence.contracts.system_health import BrokerHealthContext
    from app.trading_intelligence.trade_plan.validation import MarketReference

    plan = h.plan
    return h.orch.process_trade_plan(
        plan, now_ms=h.now, market_reference=MarketReference(100.0, h.now - 100, 1.0),
        broker_health=BrokerHealthContext(plan.broker_account_id, plan.venue, plan.environment, "HEALTHY", h.now, "t"),
        reservation_state=h.reservations.get(plan.portfolio_reservation_id),
        venue_capabilities=h.kw["evaluated"].venue_observation.execution_capabilities, klines=[],
        current_equity=equity, margin_used=margin_used, margin_available=equity - margin_used,
        open_positions=open_positions, atr=2.0, **kw)


def test_policy_context_receives_actual_per_bot_risk_state(h, monkeypatch):
    captured = {}

    def check_pre_trade(**kwargs):
        captured["pretrade"] = kwargs
        return SimpleNamespace(allowed=True, details={}, message="allowed", block_reason=None)

    def evaluate(context):
        captured["policy"] = context
        return SimpleNamespace(allowed=False, reason="stop after capture",
                               reason_code=SimpleNamespace(value="TEST_CAPTURE"))

    monkeypatch.setattr(h.orch.safety, "check_pre_trade", check_pre_trade)
    monkeypatch.setattr(h.orch.policy_engine, "evaluate", evaluate)

    result = _run(
        h, equity=1_234.0, margin_used=12.0, open_positions=2,
        market_type="CRYPTO", execution_mode="paper",
        user_kyc_approved=True, live_readiness_approved=True,
        daily_realized_pnl=-17.5, daily_trade_count=4, kill_switch=True,
        max_daily_loss=55.0, max_daily_trades=8, max_open_positions=3,
        weekly_drawdown_pct=2.5, monthly_drawdown_pct=4.5,
        max_weekly_drawdown_pct=8.0, max_monthly_drawdown_pct=12.0, min_risk_reward=2.0,
    )

    ctx = captured["policy"]
    assert result["decision"] == "blocked"
    assert result["risk_decision"].approved is False and "TEST_CAPTURE" in result["risk_decision"].reason_codes
    assert h.client.place_order.call_count == 0
    # the tenant comes from the TradePlan itself, never from caller kwargs
    assert ctx.user_id == h.plan.user_id
    assert ctx.equity == 1_234.0
    assert ctx.margin_used == 12.0
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
    assert captured["pretrade"]["open_positions"] == 2
    assert captured["pretrade"]["current_equity"] == 1_234.0
    assert captured["pretrade"]["user_kyc_approved"] is True
    assert captured["pretrade"]["live_readiness_approved"] is True


def test_kyc_and_live_readiness_default_fail_closed_for_live_config(h, monkeypatch):
    h.orch.validated_config.paper_mode = False
    captured = {}

    def check_pre_trade(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(allowed=False, details={}, message="blocked",
                               block_reason=SimpleNamespace(value="live_readiness_requirements_not_met"))

    monkeypatch.setattr(h.orch.safety, "check_pre_trade", check_pre_trade)
    result = _run(h)  # no KYC / readiness inputs supplied at all

    assert captured["is_live_mode"] is True
    assert captured["user_kyc_approved"] is False
    assert captured["live_readiness_approved"] is False
    assert result["decision"] == "blocked"
    assert result["details"]["reason_code"] == "live_readiness_requirements_not_met"
    assert result["risk_decision"].approved is False
    assert h.client.place_order.call_count == 0


def test_strategy_hold_records_risk_as_not_evaluated():
    """The retired scalar entry is a fail-closed stub: it never calls the
    strategy, never evaluates risk, never records an executable decision and
    never attempts execution -- whatever the strategy would have said."""

    class _Strategy:
        def analyze(self, **_kw):
            raise AssertionError("the retired scalar entry must not call strategy.analyze")

    def _never(**_kw):
        raise AssertionError("the retired scalar entry must not reach hard risk")

    recorded = []
    orchestrator = TradingOrchestrator.__new__(TradingOrchestrator)
    orchestrator.strategy = _Strategy()
    orchestrator.safety = SimpleNamespace(check_pre_trade=_never, check_daily_activity_fallback=_never)
    orchestrator.policy_engine = SimpleNamespace(evaluate=lambda _ctx: _never())
    orchestrator.record_decision = lambda **kwargs: recorded.append(kwargs)

    result = orchestrator.process_trading_opportunity(
        symbol="BTCUSDT", klines=[], current_price=50_000.0, current_equity=1_000.0, margin_used=0.0,
        margin_available=1_000.0, open_positions=0, client=None, run_id="run-1", cycle_id="cycle-1",
        trace_id="trace-1", timeframe="15m", user_id="user-1", bot_instance_id="bot-1",
        broker_account_id="broker-1", market_type="CRYPTO", user_kyc_approved=True, live_readiness_approved=True,
    )

    assert result["decision"] == "blocked"
    assert result["reason"] == "LEGACY_SCALAR_ENTRY_PATH_REMOVED"
    assert result["details"]["execution_attempted"] is False
    assert result["details"]["authority_owner"] == "NONE"
    assert "trade_params" not in result
    assert recorded == []
