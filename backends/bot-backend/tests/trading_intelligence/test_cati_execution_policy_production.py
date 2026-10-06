"""Corrected CATI execution policy through the REAL production path:
TradingOrchestrator hard risk, account risk, deployment and the boundary."""
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from _exec import cati_bot
from test_production_execution import live, profile
from app.core import config
from app.trading_intelligence.contracts.trade_plan import TradePlan, TargetZone
from app.trading_intelligence.execution.boundary import BoundaryStatus
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.trade_plan.validation import MarketReference
from app.trading_intelligence.contracts.system_health import BrokerHealthContext


def geometry(live, stop, target):
    fields = {k: getattr(live.plan, k) for k in TradePlan.__dataclass_fields__ if k not in TradePlan._NON_ANALYTICAL}
    fields.update(structural_invalidation_price=stop, initial_risk_distance=100. - stop,
                  target_zones=(TargetZone(live.plan.trade_plan_id, target, target, 2.5, "PRIMARY"),))
    return TradePlan.build(**fields)


def hard_risk(live, plan, price=100.):
    return live.orch.process_trade_plan(plan, now_ms=live.now, market_reference=MarketReference(price, live.now),
        broker_health=BrokerHealthContext(plan.broker_account_id, plan.venue, "LIVE", "HEALTHY", live.now, "t"),
        reservation_state=live.reservations.get(plan.portfolio_reservation_id),
        venue_capabilities=live.kw["evaluated"].venue_observation.execution_capabilities, klines=[],
        current_equity=5000, margin_used=0, margin_available=5000, open_positions=0, atr=1.,
        entry_cost_rates=production.residual_cost_rates(live.db, live.plan))


# 4/5/6/3 -- a 7% structural stop: leverage lowered under the maximum, stop and 120 margin kept.
def test_seven_percent_stop_executes_at_resolved_leverage_with_user_margin(live):
    out = hard_risk(live, geometry(live, 93., 117.5))
    risk = out["risk_decision"]
    assert risk.approved, risk.reason_codes
    resolution = out["details"]["leverage_resolution"]
    assert resolution["leverage"] == 3 and resolution["binding"] == "COMPOUND_RISK_LIMIT"
    assert risk.resolved_leverage == 3 <= live.orch.validated_config.requested_leverage["ADAUSDT"]
    assert risk.resolved_stop_price == pytest.approx(93.) and not risk.stop_tightened_by_hard_risk
    assert risk.resolved_notional / risk.resolved_leverage == pytest.approx(120.)
    trace = out["trade_params"]["sizing_trace"]
    assert trace["user_fixed_margin_usdt"] == 120. and trace["final_margin_usdt"] == 120.


# 7 -- above the true 15% system maximum.
def test_structural_stop_above_system_maximum_is_rejected(live):
    out = hard_risk(live, geometry(live, 82., 145.))
    assert out["risk_decision"].reason_codes == ("CATI_STRUCTURAL_STOP_EXCEEDS_SYSTEM_MAX",)


# 19/20 -- live price, not the next-open band, decides validity.
def test_live_drift_that_keeps_economics_passes_and_a_chase_is_blocked(live):
    assert live.plan.allowed_entry_zone.maximum_price < 100.2          # outside the old modeled-slippage band
    assert hard_risk(live, live.plan, price=100.2)["risk_decision"].approved   # 0.1R drift: net R:R 1.92
    chased = hard_risk(live, live.plan, price=100.6)["risk_decision"]  # 0.3R adverse chase
    assert chased.reason_codes == ("CATI_ENTRY_ECONOMICS_DEGRADED",)


# 21 -- the order book cannot fill the size inside the trade's economics.
@pytest.mark.parametrize("book,code", [
    ({"bids": [["99.9", "1"]], "asks": [["100.01", "0.5"]]}, "CATI_ORDER_BOOK_DEPTH_INSUFFICIENT"),
    ({"bids": [["99.9", "1"]], "asks": [["100.01", "0.5"], ["100.8", "100"]]}, "CATI_EXECUTION_ECONOMICS_DEPTH"),
    ({"unavailable": True}, "CATI_ORDER_BOOK_UNAVAILABLE"),
])
def test_order_book_economics_block_before_any_create(live, monkeypatch, book, code):
    monkeypatch.setattr(config, "settings", profile(True))
    out = live.run(live.live_boundary(), atr=1., order_book=book)
    assert out.status == BoundaryStatus.RISK_REJECTED and code in out.reason_codes
    assert out.reservation_status == "RELEASED"
    live.client.place_order.assert_not_called()


# 22 -- LIVE gate false: an approved plan never mutates a LIVE broker.
def test_live_gate_false_never_mutates_an_approved_plan(live):
    out = live.run(live.live_boundary(), atr=1.)
    assert out.status == "LIVE_ORDER_SUBMISSION_DISABLED" and out.risk_decision.approved
    live.client.place_order.assert_not_called()


# 13 -- bots sharing one account: the most restrictive resolved policy applies.
def test_shared_account_uses_most_restrictive_resolved_daily_policy(live):
    cati_bot(live.db, "s1", "shared", daily_loss_limit_pct=0.04)
    cati_bot(live.db, "s2", "shared", daily_loss_limit_pct=0.02)
    cati_bot(live.db, "other", "elsewhere", daily_loss_limit_pct=0.01)
    policy = production.account_daily_loss_policy(live.db, {"id": "shared"}, [{"id": "s1"}, {"id": "s2"}, {"id": "other"}])
    assert policy == {"pct": 0.02, "source": "USER_CONFIGURED", "bot_instance_id": "s2"}
    assert production.account_daily_loss_policy(live.db, {"id": "shared"}, []) is None


def account_client(wallet, income):
    client = Mock()
    client.account.return_value = dict(totalMarginBalance=wallet, totalWalletBalance=wallet, availableBalance=wallet,
                                       totalInitialMargin=0, totalUnrealizedProfit=0)
    client.income_history.return_value = income
    return client


# 15/16/17/18 -- certification is account truth, never CATI strategy performance.
def test_certification_loss_is_excluded_from_strategy_accounting_but_not_from_equity(live):
    production.initialize(live.db)
    cati_bot(live.db, "certbot", "certacct", daily_loss_limit_pct=0.025)
    start = live.now - 60_000
    with live.db.connect() as c:
        for fid, oid, side, pnl, fee in (("e1", "1", "BUY", "0", "0.34"), ("x1", "2", "SELL", "-7.09", "0.33")):
            c.execute("INSERT INTO cati_production_fills VALUES(?,?,?,?,?)", ("certacct", fid, oid, "ADAUSDT", json.dumps(
                {"id": fid, "orderId": oid, "side": side, "realizedPnl": pnl, "commission": fee, "time": start,
                 "_execution": {"purpose": "DEMO_CERTIFICATION", "trade_plan_id": "tplan_cert", "leg": "ENTRY"}})))
    bots = [{"id": "certbot"}]
    production.account_risk(live.db, {"id": "certacct"}, account_client(426.38, []), [], [], bots, live.now)
    cert_income = [{"incomeType": "REALIZED_PNL", "income": "-7.09", "time": start, "tradeId": "x1"},
                   {"incomeType": "COMMISSION", "income": "-0.33", "time": start, "tradeId": "x1"},
                   {"incomeType": "COMMISSION", "income": "-0.34", "time": start, "tradeId": "e1"}]
    for income in ([], cert_income):          # ledger lagging, then ledger complete
        risk = production.account_risk(live.db, {"id": "certacct"}, account_client(418.62, income), [], [], bots, live.now)
        assert risk["equity"] == 418.62 and risk["certification_pnl"] == pytest.approx(-7.76)
        assert risk["realized_pnl"] == pytest.approx(0.) and risk["daily_loss_usage"] == pytest.approx(0.)
        assert risk["consecutive_losses"] == 0 and not risk["loss_latched"] and risk["reason"] is None
        assert risk["adaptive_daily_risk"]["risk_budget_consumed_usdt"] == pytest.approx(0.)
        assert risk["adaptive_daily_risk"]["daily_risk_state"] != "HARD_STOP"
    strategy_loss = cert_income + [{"incomeType": "REALIZED_PNL", "income": "-3", "time": start, "tradeId": "n1"}]
    risk = production.account_risk(live.db, {"id": "certacct"}, account_client(415.62, strategy_loss), [], [], bots, live.now)
    assert risk["realized_pnl"] == pytest.approx(-3.) and risk["consecutive_losses"] == 1


# 1/2 -- deployment persists the user's fixed 120 margin and daily limit unchanged.
def test_deployment_persists_fixed_margin_and_daily_limit(live, monkeypatch):
    from app.core.bot_instance_service import BotInstanceService
    from app.core import broker_capability_gate
    from app.runner.effective_policy import resolve_effective_bot_policy
    monkeypatch.setattr(broker_capability_gate, "assert_broker_execution_capability", lambda *a, **kw: None)
    with live.db.connect() as c:
        c.execute("INSERT OR IGNORE INTO broker_accounts (id, user_id, broker_id, market_type, status, created_at, updated_at)"
                  " VALUES ('dep','udep','binance','crypto','active','2026-01-01','2026-01-01')")
    service = BotInstanceService(db=live.db)
    [bot] = service.deploy_auto_pilot(user_id="udep", risk_level="balanced", allocation_type="fixed_amount",
        allocation_value=120., broker_account_ids=["dep"], mode="paper", capital_allocation=500.,
        daily_loss_limit_pct=0.04)
    stored = service.get_bot_instance(bot.id)
    assert (stored.allocation_type, stored.allocation_value, stored.daily_loss_limit_pct) == ("fixed_amount", 120., 0.04)
    policy = resolve_effective_bot_policy(instance=stored, broker_environment="demo",
                                          risk_params=service.get_risk_profile_preset(stored.risk_level))
    assert (policy.position_allocation_type, policy.position_allocation_value) == ("fixed_amount", 120.)
    assert (policy.max_daily_loss_pct, policy.daily_loss_source) == (0.04, "USER_CONFIGURED")
    reset = service.update_bot_instance(bot.id, {"daily_loss_limit_pct": None})
    assert reset.daily_loss_limit_pct is None
    with pytest.raises(ValueError, match="INVALID_DAILY_LOSS_LIMIT"):
        service.update_bot_instance(bot.id, {"daily_loss_limit_pct": 0.5})
