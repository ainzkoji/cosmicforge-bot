"""Section 20/21 test harness: a REAL TradingOrchestrator + REAL
BinanceExecutor over a MagicMock exchange client (no network, no real
order), and a REAL Section 18 TradePlan with its portfolio reservation."""
from __future__ import annotations

import time
from datetime import date
import uuid
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from _position import plan_setup

from app.core.trading_orchestrator import TradingOrchestrator
from app.risk.adaptive_daily_budget import AdaptiveDailyRiskBudgetEngine, AdaptiveDailyRiskInputs
from app.execution.executor import BinanceExecutor
from app.risk.system_limits import UserConfigurableLimits
from app.strategy.strategy_framework import BaseStrategy
from app.trading_intelligence.contracts.system_health import BrokerHealthContext
from app.trading_intelligence.execution.binance_adapter import BinanceExecutionAdapter
from app.trading_intelligence.execution.boundary import AccountState, CATIExecutionBoundary
from app.trading_intelligence.execution.config import CATIExecutionConfig
from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore
from app.trading_intelligence.trade_plan.validation import MarketReference

ON = CATIExecutionConfig(active_execution_enabled=True)


class NeverAnalyze(BaseStrategy):
    """Proves the CATI path never calls strategy.analyze / the Master Ensemble."""

    name = "never_analyze"

    def __init__(self):
        self.calls = 0

    def analyze(self, *a, **k):
        self.calls += 1
        raise AssertionError("strategy.analyze must not be called on the CATI TradePlan path")


def _order(status, qty="0", avg="0.00", oid=555):
    return {"orderId": oid, "clientOrderId": "cid-1", "status": status, "executedQty": qty, "avgPrice": avg,
            "origQty": "100"}


def _entry(qty="0", avg="0.00", status="NEW", oid="555"):
    def build(req):
        dump = {"orderId": oid, "clientOrderId": req.client_order_id, "status": status, "executedQty": qty,
                "avgPrice": avg, "updateTime": 1_700_000_000_000}
        return SimpleNamespace(broker_order_id=oid, client_order_id=req.client_order_id, qty_filled=Decimal(qty),
                               avg_fill_price=Decimal(avg), status=status, model_dump=lambda: dict(dump))
    return build


class Harness:
    def __init__(self, tmp_path, *, entry=None, orders=(), order_error=None, max_slots=3, balance="5000.0",
                 fixed=120.0, place_error=None):
        from shared_lib.persistence.db import DB
        from shared_lib.persistence.migrations import migrate

        migrate(DB())  # the orchestrator's own audit DB (the per-session TEST database)
        self.db, self.pipe, self.kw, self.plan = plan_setup(tmp_path, f"s20_{uuid.uuid4().hex[:6]}.db")
        self.now = self.pipe["now_ms"]
        self.strategy = NeverAnalyze()
        self.orch = TradingOrchestrator(
            "cfg_s20", UserConfigurableLimits(requested_leverage={"BTCUSDT": 5, "ETHUSDT": 5},
                                              allowed_symbols=["BTCUSDT", "ETHUSDT"], use_fixed_size=True,
                                              fixed_size_usdt=fixed),
            "cati", "binance", strategy_instance=self.strategy)
        client = MagicMock()
        client.get_position_info.return_value = {"positionAmt": "0.0"}
        client.server_time.side_effect = lambda: int(time.time() * 1000)
        client.ping.return_value = {}
        client.account.return_value = {"availableBalance": balance, "totalWalletBalance": balance,
                                       "totalMaintMargin": "0.0", "totalInitialMargin": "0.0"}
        client.get_prices.return_value = {"BTCUSDT": 100.0, "ETHUSDT": 100.0}
        client.get_klines.side_effect = lambda **_k: [[0, 0, 0, 0, 0, 0, int(time.time() * 1000) - 1_000]]
        self.seen = {"orders": [], "protection": []}
        orders = list(orders)

        def place_order(req):
            self.seen["orders"].append(req)
            if place_error is not None:
                raise place_error
            return (entry or _entry("6.0", "100.00", "FILLED"))(req)

        def get_order(symbol, order_id):
            if order_error:
                raise order_error
            if not orders:
                raise RuntimeError("order not found")
            return orders.pop(0) if len(orders) > 1 else orders[0]

        def protect(req):
            self.seen["protection"].append(req)
            return SimpleNamespace(status="success", sl_order_id="SL-1", tp_order_id="TP-1",
                                   model_dump=lambda: {"status": "success", "sl_order_id": "SL-1",
                                                       "tp_order_id": "TP-1"})

        client.place_order.side_effect = place_order
        client.get_order.side_effect = get_order
        client.user_trades.side_effect = (lambda *a, **k: (_ for _ in ()).throw(order_error)) if order_error else (
            lambda *a, **k: [])
        client.place_protection.side_effect = protect
        self.client = client
        ex = BinanceExecutor(client=client, execution_mode="live", live_symbols=["BTCUSDT", "ETHUSDT"],
                             bot_instance_id=self.plan.bot_instance_id, db=self.db)
        ex.run_id = "run-s20"
        ex._capital_budget = fixed
        ex._allocation_type = "fixed_amount"
        ex._allocation_value = fixed
        ex._max_notional_per_symbol = fixed * 5 * 1.2
        ex._broker_account_id = f"acct_{uuid.uuid4().hex}"
        ex._max_open_positions = max_slots
        ex._fill_resolution_sleep = lambda _s: None
        ex._size_qty = lambda symbol, budget, lev_mult, sl_price=0.0, leverage_override=None: (
            budget / 100.0, {"price": 100.0, "leverage": 5})
        self.executor = ex
        self.adapter = BinanceExecutionAdapter(ex, venue=self.plan.venue)
        self.reservations = CATIReservationStore(self.db)

    def boundary(self, config=ON, adapter=None, **kw):
        return CATIExecutionBoundary(orchestrator=self.orch, adapter=adapter or self.adapter, db=self.db, config=config,
                                     **kw)

    def run(self, boundary=None, plan=None, *, price=None, ref_age=100, health="HEALTHY", now=None, atr=2.0, **kw):
        plan = plan or self.plan
        now = self.now if now is None else now
        caps = self.kw["evaluated"].venue_observation.execution_capabilities
        return (boundary or self.boundary()).process_trade_plan(
            plan, market_reference=MarketReference(price or plan.entry_reference, now - ref_age, 1.0),
            broker_health=BrokerHealthContext(plan.broker_account_id, plan.venue, plan.environment, health, now, "t"),
            venue_capabilities=caps, account=kw.pop("account", AccountState(5000.0, 0.0, 5000.0, 0)), atr=atr,
            now_ms=now, runtime_session_id="rts_test", **kw)

    def reservation_status(self, plan=None):
        return self.reservations.get((plan or self.plan).portfolio_reservation_id).status


def _hard_cap_context(loss):
    dec = AdaptiveDailyRiskBudgetEngine().evaluate(AdaptiveDailyRiskInputs(
        bot_instance_id="botA", risk_date=date(2026, 9, 24), day_open_equity=5000.0, current_equity=5000.0 - loss,
        realized_pnl_today=-loss))
    return dec.as_policy_context()
