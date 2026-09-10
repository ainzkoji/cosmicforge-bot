from __future__ import annotations

from dataclasses import replace
from datetime import date
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.core.bot_instance_service import BotInstanceService
from app.execution.paper_executor import PaperExecutor
from app.models.bot_instance_models import BotInstance
from app.runner.bot_context import BotRunContext
from app.runner.effective_policy import EffectivePolicyError, resolve_effective_bot_policy
from app.runner.market_snapshot import MarketSnapshot, SnapshotMarketClient, claim_candle
from app.product_safety.approvals import (
    active_readiness_approval,
    approve_readiness,
    invalidate_readiness_approval,
)
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate


class PriceClient:
    def __init__(self, price: float = 100.0):
        self.price = price

    def get_prices(self, symbols):
        return {symbols[0]: self.price}


def _instance(**changes):
    values = dict(
        id="bot-1", user_id="user-1", broker_account_id="broker-1",
        market_type="CRYPTO", strategy_id="master_ensemble", strategy_version="2",
        risk_level="balanced", symbols=["BTCUSDT", "ETHUSDT"], timeframes=["15m"],
        allocation_type="fixed_amount", allocation_value=50.0,
        capital_allocation=500.0, capital_allocation_type="fixed_amount", mode="paper",
    )
    values.update(changes)
    return BotInstance(**values)


def test_effective_policy_preserves_capital_and_context_agrees():
    policy = resolve_effective_bot_policy(
        instance=_instance(),
        risk_params=BotInstanceService.get_risk_profile_preset("balanced"),
        broker_environment="demo",
    )
    context = BotRunContext.from_effective_policy(policy, broker_credentials={})
    assert policy.capital_budget == context.capital_budget == 500.0
    assert policy.position_allocation_value == context.trade_usdt_per_order == 50.0
    assert policy.execution_mode == context.execution_mode == "paper"
    assert policy.timeframe == context.interval == "15m"
    assert policy.risk_per_trade <= policy.risk_per_trade_ceiling
    assert policy.policy_hash == context.effective_policy_hash


def test_effective_policy_never_manufactures_legacy_capital():
    with pytest.raises(EffectivePolicyError, match="capital budget is missing"):
        resolve_effective_bot_policy(
            instance=_instance(capital_allocation=None),
            risk_params=BotInstanceService.get_risk_profile_preset("balanced"),
            broker_environment="demo",
        )


def test_policy_hash_changes_for_material_runtime_fields():
    base = resolve_effective_bot_policy(
        instance=_instance(), risk_params=BotInstanceService.get_risk_profile_preset("balanced"),
        broker_environment="demo",
    )
    changed = resolve_effective_bot_policy(
        instance=_instance(mode="live", allocation_value=40.0),
        risk_params=BotInstanceService.get_risk_profile_preset("balanced"),
        broker_environment="testnet",
    )
    assert base.policy_hash != changed.policy_hash
    assert changed.execution_mode == "broker"
    assert changed.broker_environment == "testnet"


def test_paper_open_tp1_final_close_uses_remaining_quantity():
    paper = PaperExecutor(PriceClient())
    opened = paper.open_position(symbol="BTCUSDT", side="LONG", notional_usdt=100.0, quantity=1.0)
    assert opened.success and opened.filled_qty == 1.0
    partial = paper.partial_close(symbol="BTCUSDT", position_side="LONG", quantity=0.5)
    assert partial.success and partial.details["remaining_qty"] == pytest.approx(0.5)
    closed = paper.close_position(symbol="BTCUSDT", position_side="LONG", quantity=0.5)
    assert closed.success and closed.filled_qty == pytest.approx(0.5)
    assert closed.details["remaining_qty"] == 0.0
    assert "BTCUSDT" not in paper._positions


def test_paper_restart_after_tp1_restores_only_remaining_quantity():
    restarted = PaperExecutor(PriceClient())
    restarted.seed_position("BTCUSDT", "LONG", 0.5, 100.0)
    closed = restarted.close_position(symbol="BTCUSDT", position_side="LONG", quantity=0.5)
    assert closed.success and closed.filled_qty == pytest.approx(0.5)
    second = restarted.close_position(symbol="BTCUSDT", position_side="LONG", quantity=0.5)
    assert not second.success


def test_closed_candle_claim_is_persistent_and_snapshot_client_is_pinned(tmp_path):
    path = str(tmp_path / "runtime.db")
    db = DB(path)
    migrate(path)
    rows = [
        [1, "99", "101", "98", "100", "1", 2],
        [3, "100", "102", "99", "101", "1", 4],
    ]
    snapshot = MarketSnapshot.build(
        symbol="BTCUSDT", timeframe="15m", candles=rows, source="test",
        higher_timeframe="4h", higher_timeframe_candles=rows,
    )
    assert claim_candle(db, bot_instance_id="bot-1", symbol="BTCUSDT", timeframe="15m", close_time=4)
    for _ in range(99):
        assert not claim_candle(
            db, bot_instance_id="bot-1", symbol="BTCUSDT", timeframe="15m", close_time=4
        )
    assert claim_candle(db, bot_instance_id="bot-1", symbol="BTCUSDT", timeframe="15m", close_time=5)
    client = SnapshotMarketClient(PriceClient(), snapshot)
    assert client.klines(symbol="BTCUSDT", interval="15m", limit=1) == [rows[-1]]
    with pytest.raises(ValueError, match="snapshot_timeframe_unavailable"):
        client.klines(symbol="BTCUSDT", interval="1m", limit=1)


def test_downstream_risk_engines_do_not_repeat_approved_confidence(tmp_path):
    from app.policy.policy_engine import PolicyContext, PolicyEngine, ReasonCode
    from app.risk.safety_engine import SafetyConfig, SafetyEngine, BlockReason

    db = DB(str(tmp_path / "confidence.db"))
    safety = SafetyEngine(db, risk_budget=None, protection=None, config=SafetyConfig(
    ))
    safety_result = safety.check_pre_trade(
        "bot-1", "BTCUSDT", 0.58, 1.0, 1_000.0, 0,
        confidence_already_approved=True,
    )
    assert safety_result.block_reason != BlockReason.LOW_CONFIDENCE

    policy = PolicyEngine()
    policy_result = policy.evaluate(PolicyContext(
        symbol="BTCUSDT", signal="BUY", confidence=0.58,
        confidence_already_approved=True, entry_price=100.0, atr=1.0,
        equity=1_000.0, margin_available=1_000.0,
        stop_loss_price=98.0, take_profit_price=104.0,
        trade_amount_mode="fixed", trade_amount_value=10.0,
        now_ms=2_000_000_000_000,
    ))
    assert policy_result.reason_code != ReasonCode.LOW_CONFIDENCE


def test_readiness_approval_is_explicit_and_invalidated_by_policy_change(tmp_path):
    path = str(tmp_path / "readiness.db")
    db = DB(path)
    migrate(path)
    created = approve_readiness(
        db=db,
        bot_instance_id="bot-1",
        reviewer_admin_id="admin-1",
        policy_hash="policy-hash-a",
        evidence_snapshot={"readiness_status": "READY_FOR_CONTROLLED_BETA_REVIEW"},
        source_commit_sha="abc123",
        notes="reviewed evidence",
    )
    assert created["policy_hash"] == "policy-hash-a"
    assert active_readiness_approval(db=db, bot_instance_id="bot-1") is not None
    assert invalidate_readiness_approval(
        db=db,
        bot_instance_id="bot-1",
        reason="POLICY_CHANGED",
        current_policy_hash="policy-hash-b",
    ) == 1
    assert active_readiness_approval(db=db, bot_instance_id="bot-1") is None


def test_run_cycle_owns_kill_switch_handling():
    from app.runner.runner import PaperRunner

    runner = PaperRunner.__new__(PaperRunner)
    runner._cycle_lock = threading.Lock()
    runner._closed_symbols_this_cycle = set()
    runner._reconciliation_done = True
    runner.live_trades_this_cycle = 0
    runner.daily = SimpleNamespace(day=date.today(), kill=True)
    runner.activate_kill_switch = MagicMock()

    result = runner.run_cycle()

    runner.activate_kill_switch.assert_called_once_with()
    assert result["status"] == "paused"
    assert result["reason"] == "KILL_SWITCH_ACTIVE"


def test_run_cycle_owns_daily_close_handling(tmp_path):
    from app.runner.runner import PaperRunner

    path = str(tmp_path / "cycle.db")
    db = DB(path)
    migrate(path)
    runner = PaperRunner.__new__(PaperRunner)
    runner._cycle_lock = threading.Lock()
    runner._closed_symbols_this_cycle = set()
    runner._reconciliation_done = True
    runner.live_trades_this_cycle = 0
    runner.daily = SimpleNamespace(day=date.today(), kill=False)
    runner._run_daily_close_from_cycle = MagicMock(return_value=1)
    runner.trade_symbols = []
    runner._run_dynamic_universe_shadow_diagnostics = MagicMock(return_value={"status": "disabled"})
    runner.db = db
    runner.run_id = "run-1"
    runner.context = SimpleNamespace(bot_instance_id="bot-1")

    result = runner.run_cycle()

    runner._run_daily_close_from_cycle.assert_called_once_with()
    assert result["results"]["_daily_close"]["reason"] == "DAILY_CLOSE"
