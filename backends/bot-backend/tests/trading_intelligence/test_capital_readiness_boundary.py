"""Section 9.13-9.15: capital readiness at the CATI execution boundary.

Local transfer intent is not money: an entry that needs an internal transfer
reaches hard risk / the broker only once that transfer is broker-COMPLETED;
shared collateral needs a valid logical reservation. Ready capital never
bypasses hard risk (the 2.5% daily hard cap stays superior) and an internal
transfer never changes the daily risk budget.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from _exec import Harness, _hard_cap_context

from app.risk.adaptive_daily_budget import AdaptiveDailyRiskBudgetEngine, AdaptiveDailyRiskInputs
from app.trading_intelligence.capital.planner import (
    CapitalReadiness, AccountCapitalState, CapitalSettings, capital_readiness, plan_capital,
)
from app.trading_intelligence.contracts.execution import RiskRejectionFamily as F
from app.trading_intelligence.execution.boundary import BoundaryStatus as B, CATIExecutionBoundary
from shared_lib.broker.wallets import topology_for

D = Decimal
AUTO = CapitalSettings(mode="AUTOMATED_INTERNAL_REALLOCATION", auto_rebalance_enabled=True, authorized=True)


@pytest.fixture
def h(tmp_path):
    return Harness(tmp_path)


def _physical_plan():
    state = AccountCapitalState("acc", "USDT", topology_for("binance"),
                                {"UMFUTURE": D("10"), "FUNDING": D("5000"), "MAIN": None}, transfer_capability_usable=True)
    plan = plan_capital(state=state, product="CRYPTO_PERPETUAL", required=D("300"), settings=AUTO, plan_key="p")
    assert plan.needs_transfer
    return plan


def test_missing_capital_readiness_never_reaches_risk_or_the_broker(h):
    orch = MagicMock()
    ref = h.boundary()  # test governance authority + enabled config
    b = CATIExecutionBoundary(orchestrator=orch, adapter=h.adapter, db=h.db, config=ref.config, authority=ref.authority)
    res = h.run(b, capital=None)
    assert res.status == B.CAPITAL_NOT_READY and res.reason_codes == ("CAPITAL_READINESS_NOT_ESTABLISHED",)
    orch.process_trade_plan.assert_not_called()
    h.client.place_order.assert_not_called()
    assert h.reservation_status() == "RESERVED"


@pytest.mark.parametrize("status", ["REQUESTED", "SUBMITTED", "CONFIRMATION_PENDING", "UNKNOWN",
                                    "RECONCILIATION_REQUIRED"])
def test_unconfirmed_transfer_cannot_fund_execution(h, status):
    ready = capital_readiness(_physical_plan(), transfer_status=status)
    assert not ready.ready and ready.pending and ready.path == "PHYSICAL"
    res = h.run(capital=ready)
    assert res.status == B.CAPITAL_NOT_READY and res.reason_codes == (f"CAPITAL_NOT_READY:INTERNAL_TRANSFER_{status}",)
    h.client.place_order.assert_not_called()
    assert h.reservation_status() == "RESERVED"  # may still complete: capacity stays owned, nothing submitted


def test_failed_transfer_is_definitive_and_releases_the_reservation(h):
    ready = capital_readiness(_physical_plan(), transfer_status="FAILED")
    assert not ready.ready and not ready.pending
    res = h.run(capital=ready)
    assert res.status == B.CAPITAL_NOT_READY and h.reservation_status() == "RELEASED"
    h.client.place_order.assert_not_called()


def test_confirmed_transfer_and_valid_logical_reservation_satisfy_readiness():
    plan = _physical_plan()
    assert capital_readiness(plan, transfer_status="COMPLETED") == CapitalReadiness(True, "PHYSICAL")
    unified = plan_capital(state=AccountCapitalState("acc", "USDT", topology_for("bybit", "UNIFIED"),
                                                     {"UNIFIED": D("1000"), "FUND": D("0")}),
                           product="FX_PERPETUAL", required=D("100"), settings=AUTO, plan_key="u")
    assert unified.outcome == "NO_ACTION_SHARED_COLLATERAL" and unified.transfer is None  # no physical call
    assert capital_readiness(unified, reservation_status="RESERVED").ready
    assert capital_readiness(unified, reservation_status="RELEASED").reason == "LOGICAL_RESERVATION_NOT_VALID"
    assert capital_readiness(unified).reason == "LOGICAL_RESERVATION_UNKNOWN"
    # a transfer status never stands in for a logical reservation, and vice versa
    assert not capital_readiness(unified, transfer_status="COMPLETED").ready
    assert not capital_readiness(plan, reservation_status="RESERVED").ready


def test_ready_capital_never_bypasses_the_hard_daily_cap(h):
    res = h.run(capital=CapitalReadiness(True, "PHYSICAL"), adaptive_daily_risk=_hard_cap_context(125.0))
    assert res.status == B.RISK_REJECTED and res.risk_decision.rejection_family == F.DAILY_LOSS.value
    h.client.place_order.assert_not_called()


def test_internal_transfer_does_not_change_the_daily_risk_budget():
    eng = AdaptiveDailyRiskBudgetEngine()
    base = dict(bot_instance_id="b", risk_date=date(2026, 9, 26), day_open_equity=5000.0, realized_pnl_today=-40.0)
    before = eng.evaluate(AdaptiveDailyRiskInputs(current_equity=4960.0, **base))
    # 2,000 USDT moved INTO the trading wallet intraday: equity rises, the frozen day-open cap does not
    after = eng.evaluate(AdaptiveDailyRiskInputs(current_equity=6960.0, **base))
    for field in ("hard_daily_cap_usdt", "effective_daily_budget_usdt", "remaining_daily_risk_usdt",
                  "risk_budget_consumed_usdt", "daily_risk_state"):
        assert getattr(before, field) == getattr(after, field), field
    assert before.hard_daily_cap_usdt == pytest.approx(125.0)  # 2.5% of day-open equity
