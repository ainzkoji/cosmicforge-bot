"""Section 20 -- the CATI hard-risk / execution boundary: the 20.21 matrix
plus the 20.20 Binance broker-contract suite.

REAL components throughout: a real Section 18 TradePlan, the real
TradingOrchestrator hard-risk stack (Layers A/B/C + PolicyEngine), the real
BinanceExecutor (capital gate, atomic slots, account margin reservations,
entry-protection idempotency, submit-unknown, fill resolution, protection)
over a MagicMock exchange client -- no network, no real order."""
from __future__ import annotations

import ast
import dataclasses
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from _exec import Harness, NeverAnalyze, _entry, _hard_cap_context, _order

from app.core.trading_orchestrator import TradingOrchestrator
from app.execution.position_slots import occupied_slots
from app.risk.capital_ledger import ACCOUNT_RESERVATIONS
from app.risk.system_limits import UserConfigurableLimits
from app.trading_intelligence.contracts.execution import ExecutionAttemptStatus as X, RiskRejectionFamily as F
from app.trading_intelligence.contracts.position import ExitAction, ExitDecision
from app.trading_intelligence.contracts.system_health import BrokerHealthContext
from app.trading_intelligence.evidence.stores import ExecutionAttemptStore, RiskDecisionStore
from app.trading_intelligence.execution import config as exec_config
from app.trading_intelligence.execution.adapter import (
    ExecutionAdapter, ExecutionAdapterRegistry, ExecutionNotSupported, ProtectionWideningRefused,
    UnvalidatedExecutionAdapter,
)
from app.trading_intelligence.execution.boundary import AccountState, BoundaryStatus as B, CATIExecutionBoundary
from app.trading_intelligence.execution.config import CATIExecutionConfig
from app.trading_intelligence.trade_plan.validation import MarketReference

BACKEND = Path(__file__).resolve().parents[2]


@pytest.fixture
def h(tmp_path):
    return Harness(tmp_path)


# ============================== FLAG / NO PROMOTION ==============================
def test_active_execution_is_off_by_default(h, monkeypatch):
    monkeypatch.delenv(exec_config.ENV_ACTIVE_EXECUTION, raising=False)
    monkeypatch.setenv("CATI_CYCLE_SHADOW_ENABLED", "1")  # shadow evidence never implies active trading
    assert CATIExecutionConfig.from_env().active_execution_enabled is False
    orch = MagicMock()
    b = CATIExecutionBoundary(orchestrator=orch, adapter=h.adapter, db=h.db)
    res = h.run(b)
    assert res.status == B.DISABLED
    orch.process_trade_plan.assert_not_called()
    h.client.place_order.assert_not_called()
    assert h.reservation_status() == "RESERVED"


def test_runtime_never_calls_the_boundary():
    for path in list((BACKEND / "app" / "runner").glob("*.py")) + [BACKEND / "app" / "main.py"]:
        text = path.read_text(errors="ignore")
        assert "CATIExecutionBoundary" not in text and "process_trade_plan" not in text, path.name
        assert "CATI_ACTIVE_EXECUTION_ENABLED" not in text


def test_live_environment_refused_while_not_promoted(h):
    res = h.run(plan=dataclasses.replace(h.plan, environment="LIVE"))
    assert res.status == B.ENVIRONMENT_NOT_ALLOWED
    h.client.place_order.assert_not_called()


# ============================== ENTRYPOINT ==============================
def test_process_trade_plan_skips_strategy_and_reuses_hard_risk(h, monkeypatch):
    seen = {}
    real_check = h.orch.safety.check_pre_trade

    def spy(**kw):
        seen.update(kw)
        return real_check(**kw)

    monkeypatch.setattr(h.orch.safety, "check_pre_trade", spy)
    res = h.run()
    assert res.status == B.EXECUTED, res
    assert h.strategy.calls == 0
    assert seen["confidence_already_approved"] is True  # the V2 adaptive threshold is never consulted
    assert seen["symbol"] == h.plan.instrument_key.venue_symbol


def test_orchestrator_result_carries_risk_evidence(h):
    reservation = h.reservations.get(h.plan.portfolio_reservation_id)
    out = h.orch.process_trade_plan(
        h.plan, now_ms=h.now, market_reference=MarketReference(100.0, h.now - 100, 1.0),
        broker_health=BrokerHealthContext(h.plan.broker_account_id, h.plan.venue, "DEMO", "HEALTHY", h.now, "t"),
        reservation_state=reservation, venue_capabilities=h.kw["evaluated"].venue_observation.execution_capabilities,
        klines=[], current_equity=5000.0, margin_used=0.0, margin_available=5000.0, open_positions=0, atr=2.0)
    r = out["risk_decision"]
    assert out["decision"] == "execute" and r.approved and r.stage == "PRE_EXECUTION"
    assert out["details"]["strategy_output"]["meta"]["trade_plan_id"] == h.plan.trade_plan_id
    # hard risk may TIGHTEN the structural stop, never widen it
    assert r.resolved_stop_price >= h.plan.structural_invalidation_price
    assert dict(r.allocation_basis)["semantics"] == "PER_APPROVED_TRADE"


# ============================== PREVALIDATION ==============================
@pytest.mark.parametrize("case,expected", [
    ("expired", "PREVALIDATION_EXPIRED"), ("stale", "PREVALIDATION_STALE"),
    ("entry_zone", "PREVALIDATION_ENTRY_ZONE_VIOLATION"), ("broker", "PREVALIDATION_BROKER_DEGRADED"),
])
def test_prevalidation_rejects_and_releases(h, case, expected):
    kw = {}
    if case == "expired":
        kw["now"] = h.plan.plan_expiry_time + 1
    elif case == "stale":
        kw["ref_age"] = 60_000
    elif case == "entry_zone":
        kw["price"] = h.plan.allowed_entry_zone.maximum_price * 1.05
    else:
        kw["health"] = "DEGRADED"
    res = h.run(**kw)
    assert res.status == B.RISK_REJECTED and res.risk_decision.rejection_family == F.PREVALIDATION.value
    assert expected in res.reason_codes
    assert h.reservation_status() in ("RELEASED", "EXPIRED")
    h.client.place_order.assert_not_called()


def test_bad_hash_rejected(h):
    forged = dataclasses.replace(h.plan, entry_reference=h.plan.entry_reference + 1)
    res = h.run(plan=forged)
    assert res.status == B.RISK_REJECTED and "TRADE_PLAN_HASH_MISMATCH" in res.reason_codes
    h.client.place_order.assert_not_called()


def test_reservation_lost_rejected(h):
    h.reservations.release(h.plan.portfolio_reservation_id, h.now)
    res = h.run()
    assert res.status == B.RISK_REJECTED and "PORTFOLIO_RESERVATION_LOST" in res.reason_codes
    h.client.place_order.assert_not_called()


# ============================== RISK ==============================
def test_hard_daily_cap_rejection_at_2_5_percent(h):
    ctx = _hard_cap_context(125.0)  # 2.5% of 5000
    assert ctx["hard_daily_cap_usdt"] == pytest.approx(125.0) and ctx["daily_risk_state"] == "HARD_STOP"
    res = h.run(adaptive_daily_risk=ctx)
    assert res.status == B.RISK_REJECTED and res.risk_decision.rejection_family == F.DAILY_LOSS.value
    assert "DAILY_HARD_EQUITY_CAP_REACHED" in res.reason_codes and h.reservation_status() == "RELEASED"


def test_adaptive_daily_risk_rejection(h):
    res = h.run(adaptive_daily_risk={"daily_risk_state": "HARD_STOP", "decision_reason": "DAILY_RISK_BUDGET_EXHAUSTED"})
    assert res.risk_decision.rejection_family == F.ADAPTIVE_RISK.value and h.reservation_status() == "RELEASED"


def test_drawdown_rejection(h):
    res = h.run(weekly_drawdown_pct=9.0, max_weekly_drawdown_pct=8.0)
    assert res.risk_decision.rejection_family == F.DRAWDOWN.value


def test_leverage_limits(h):
    h.orch.safety.config.max_leverage = 3.0  # a stricter operator ceiling than the requested 5x
    res = h.run()
    assert res.status == B.RISK_REJECTED and res.risk_decision.rejection_family == F.LEVERAGE.value


def test_leverage_is_clamped_by_system_limits(tmp_path):
    h = Harness(tmp_path)
    h.orch = TradingOrchestrator("cfg", UserConfigurableLimits(requested_leverage={"BTCUSDT": 125},
                                                                allowed_symbols=["BTCUSDT"], use_fixed_size=True,
                                                                fixed_size_usdt=120.0),
                                 "cati", "binance", strategy_instance=NeverAnalyze())
    res = h.run(h.boundary(config=CATIExecutionConfig(active_execution_enabled=False)))
    assert res.status == B.DISABLED  # flag still gates
    out = h.orch.process_trade_plan(
        h.plan, now_ms=h.now, market_reference=MarketReference(100.0, h.now - 100, 1.0),
        broker_health=BrokerHealthContext(h.plan.broker_account_id, h.plan.venue, "DEMO", "HEALTHY", h.now, "t"),
        reservation_state=h.reservations.get(h.plan.portfolio_reservation_id),
        venue_capabilities=h.kw["evaluated"].venue_observation.execution_capabilities, klines=[],
        current_equity=5000.0, margin_used=0.0, margin_available=5000.0, open_positions=0, atr=2.0)
    assert out["risk_decision"].resolved_leverage <= 10.0  # major-crypto ceiling, whatever CATI "prefers"


def test_layer_b_margin_rejection_in_hard_risk(h):
    res = h.run(account=AccountState(5000.0, 4999.0, 1.0, 0))
    assert res.status == B.RISK_REJECTED and res.risk_decision.rejection_family == F.MARGIN.value
    assert res.risk_decision.stage == "PRE_EXECUTION" and h.reservation_status() == "RELEASED"


def test_executor_slot_rejection_releases(tmp_path):
    from _pf import add_position

    h = Harness(tmp_path, max_slots=1)
    add_position(h.db, h.plan.bot_instance_id, "SOLUSDT")
    res = h.run()
    assert res.status == B.EXECUTION_REJECTED and res.attempt.status == X.REJECTED.value
    assert res.risk_decision.stage == "EXECUTOR_GATES" and res.risk_decision.rejection_family == F.SLOT.value
    assert h.reservation_status() == "RELEASED"
    h.client.place_order.assert_not_called()


def test_executor_margin_rejection_releases(tmp_path):
    h = Harness(tmp_path, balance="1.0")
    res = h.run()
    assert res.risk_decision.rejection_family == F.MARGIN.value and h.reservation_status() == "RELEASED"
    h.client.place_order.assert_not_called()


def test_executor_sizing_rejection(tmp_path):
    h = Harness(tmp_path, fixed=0.5)
    res = h.run()
    assert res.status == B.EXECUTION_REJECTED and res.risk_decision.rejection_family == F.SIZING.value


# ============================== CAPITAL ==============================
def test_fixed_amount_is_per_trade_not_aggregate(h):
    first = h.run()
    assert first.status == B.EXECUTED
    assert first.attempt.filled_quantity == pytest.approx(6.0)
    rows = ExecutionAttemptStore(h.db).for_plan(h.plan.broker_account_id, h.plan.trade_plan_id)
    assert rows[-1]["payload"]["status"] == X.FILLED.value
    # the executor's capital authority approved THIS trade's own 120 -- an open position never shrinks the next one
    auth = h.executor._authorize_capital("ETHUSDT", 600.0, 5, 5.0)
    assert auth.approved and auth.approved_margin == pytest.approx(120.0)


# ============================== RESERVATIONS ==============================
def test_success_consumes(h):
    assert h.run().status == B.EXECUTED and h.reservation_status() == "CONSUMED"


def test_submit_unknown_does_not_release_and_never_resubmits(tmp_path):
    h = Harness(tmp_path, entry=_entry(), order_error=RuntimeError("timeout"))
    res = h.run()
    assert res.status == B.SUBMIT_UNKNOWN and res.attempt.status == X.SUBMIT_UNKNOWN.value
    assert h.reservation_status() == "RESERVED"
    assert len(h.seen["orders"]) == 1
    again = h.run()
    assert again.status == B.DUPLICATE_PLAN and len(h.seen["orders"]) == 1


def test_submit_unknown_reconciled_from_broker_truth(tmp_path):
    h = Harness(tmp_path, entry=_entry(), order_error=RuntimeError("timeout"))
    b = h.boundary()
    h.run(b)
    # still unknown: broker cannot answer -> nothing changes
    assert b.reconcile_submit_unknown(h.plan, now_ms=h.now + 1).status == B.STILL_UNKNOWN
    assert h.reservation_status() == "RESERVED"
    h.client.get_order.side_effect = lambda s, o: _order("FILLED", "6.0", "100.0")
    h.client.get_position_info.return_value = {"positionAmt": "6.0", "entryPrice": "100.0"}
    res = b.reconcile_submit_unknown(h.plan, now_ms=h.now + 2)
    assert res.status == B.RECONCILED and res.attempt.status == X.RECONCILED_POSITION_EXISTS.value
    assert h.reservation_status() == "CONSUMED" and len(h.seen["orders"]) == 1


def test_submit_unknown_reconciled_no_position_releases(tmp_path):
    h = Harness(tmp_path, entry=_entry(), order_error=RuntimeError("timeout"))
    b = h.boundary()
    h.run(b)
    h.client.get_order.side_effect = lambda s, o: _order("CANCELED", "0")
    res = b.reconcile_submit_unknown(h.plan, now_ms=h.now + 2)
    assert res.attempt.status == X.RECONCILED_NO_POSITION.value and h.reservation_status() == "RELEASED"


# ============================== EXECUTION ==============================
def test_idempotent_repeated_plan(h):
    b = h.boundary()
    assert h.run(b).status == B.EXECUTED
    assert h.run(b).status == B.DUPLICATE_PLAN
    assert len(h.seen["orders"]) == 1


def test_plan_identity_is_the_executor_idempotency_key(h):
    """Even bypassing CATI's own evidence check, the SAME plan identity maps to the
    same entry intent in the executor, so it can never open a second position."""
    from app.trading_intelligence.execution.adapter import EntryRequest

    req = EntryRequest(trade_plan_id=h.plan.trade_plan_id, trade_plan_hash=h.plan.trade_plan_hash, risk_decision_id="r",
                       venue_symbol="BTCUSDT", side="LONG", notional=600.0, stop_price=97.75, target_price=110.0,
                       leverage=5, requested_order_type="MARKET", requested_price=100.0, max_slippage_bps=10.0,
                       current_open_count=0, current_equity=5000.0, cycle_id="c1",
                       intent_identity=f"{h.plan.trade_plan_id}|{h.plan.trade_plan_hash}")
    first = h.adapter.submit_entry(req)
    second = h.adapter.submit_entry(req)
    assert first.status == X.FILLED.value and second.status == X.DUPLICATE_SUPPRESSED.value
    assert len(h.seen["orders"]) == 1
    k1 = h.executor._build_entry_idempotency("BTCUSDT", "LONG", 600.0, 97.75, 110.0, intent_identity="p|h")
    k2 = h.executor._build_entry_idempotency("BTCUSDT", "LONG", 600.0, 97.75, 110.0, intent_identity="p|h")
    k3 = h.executor._build_entry_idempotency("BTCUSDT", "LONG", 600.0, 97.75, 110.0, intent_identity="p|h2")
    assert k1 == k2 and k1 != k3


def test_partial_fill_uses_broker_quantity_and_protects_it(tmp_path):
    h = Harness(tmp_path, entry=_entry(), orders=[_order("PARTIALLY_FILLED", "3.5", "100.0"),
                                                   _order("CANCELED", "3.5", "100.0")])
    res = h.run()
    a = res.attempt
    assert res.status == B.EXECUTED and a.status == X.PARTIALLY_FILLED.value
    assert a.filled_quantity == pytest.approx(3.5) and a.requested_quantity > a.filled_quantity
    assert float(h.seen["protection"][-1].qty) == pytest.approx(3.5)
    assert h.reservation_status() == "CONSUMED"


def test_rejected_order_releases(tmp_path):
    h = Harness(tmp_path, entry=_entry(), orders=[_order("CANCELED", "0")])
    res = h.run()
    assert res.attempt.status == X.NOT_FILLED.value and h.reservation_status() == "RELEASED"
    h.client.place_protection.assert_not_called()


def test_exchange_rejection_pre_submit_releases(tmp_path):
    h = Harness(tmp_path, place_error=RuntimeError('{"code":-1111,"msg":"invalid precision"}'))
    res = h.run()
    assert res.attempt.status == X.REJECTED.value and h.reservation_status() == "RELEASED"


def test_protection_after_fill_uses_risk_resolved_stop(h):
    res = h.run()
    assert res.status == B.EXECUTED
    prot = h.seen["protection"][-1]
    assert float(prot.qty) == pytest.approx(6.0)
    risk = RiskDecisionStore(h.db).for_plan(h.plan.broker_account_id, h.plan.trade_plan_id)[0]["payload"]
    assert float(prot.sl_price) == pytest.approx(risk["resolved_stop_price"], rel=1e-3)
    assert res.attempt.protection.get("sl_order_id") == "SL-1"


def test_position_lifecycle_registered_with_filled_quantity(tmp_path):
    from app.execution.position_manager import PositionManager

    h = Harness(tmp_path, entry=_entry(), orders=[_order("PARTIALLY_FILLED", "3.5", "100.0"),
                                                   _order("CANCELED", "3.5", "100.0")])
    pm = PositionManager()
    res = h.run(h.boundary(position_manager=pm))
    pos = pm.get_position("BTCUSDT")
    assert pos is not None and pos.current_qty == pytest.approx(3.5) and pos.position_id == res.attempt.position_id


# ============================== BROKER NEUTRALITY ==============================
def test_binance_adapter_implements_protocol(h):
    assert isinstance(h.adapter, ExecutionAdapter)
    assert h.adapter.execution_support_status == "CONTRACT_VALIDATED"


def test_unvalidated_adapter_fails_closed(h):
    reg = ExecutionAdapterRegistry()
    reg.register(h.adapter)
    other = reg.resolve("OANDA")
    assert isinstance(other, UnvalidatedExecutionAdapter)
    with pytest.raises(ExecutionNotSupported):
        other.submit_entry(None)
    res = h.run(h.boundary(adapter=UnvalidatedExecutionAdapter(h.plan.venue)))
    assert res.status == B.ADAPTER_UNVALIDATED and h.reservation_status() == "RELEASED"
    h.client.place_order.assert_not_called()


def test_no_binance_semantics_in_cati_contracts():
    for rel in ("contracts/execution.py", "contracts/position.py", "execution/adapter.py", "execution/boundary.py"):
        src = (BACKEND / "app" / "trading_intelligence" / rel).read_text()
        tree = ast.parse(src)
        strings = {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)}
        assert not {"BUY", "SELL", "clientOrderId", "reduceOnly", "USDT"} & strings, rel
        assert not any("binance" in s.lower() for s in strings if len(s) < 60), rel


# -- 20.20 Binance broker-contract parity -------------------------------------------------------
def test_contract_symbol_mapping_and_market_order(h):
    h.run()
    req = h.seen["orders"][0]
    assert req.symbol == h.plan.instrument_key.venue_symbol and str(req.type).endswith("MARKET")
    assert req.reduce_only is False and req.client_order_id


def test_contract_quantity_precision(tmp_path):
    h = Harness(tmp_path)
    h.executor._size_qty = lambda symbol, budget, lev_mult, sl_price=0.0, leverage_override=None: (
        budget / 100.0 + 0.123456, {"price": 100.0, "leverage": 5, "step_size": "0.001", "min_qty": "0.001",
                                    "min_notional": "5", "contract_size": "1"})
    h.run()
    qty = h.seen["orders"][0].qty
    assert qty == qty.quantize(Decimal("0.001"))


def test_contract_query_cancel_reconcile(h):
    h.client.get_order.side_effect = lambda s, o: _order("FILLED", "2.0", "101.0", oid=777)
    st = h.adapter.query_order("BTCUSDT", broker_order_id="777")
    assert st.answered and st.executed_qty == 2.0 and st.avg_price == 101.0
    h.client.cancel_order.return_value = True
    assert h.adapter.cancel_order("BTCUSDT", "777") is True
    h.client.cancel_order.assert_called_with("BTCUSDT", "777")
    h.client.get_position_info.return_value = {"positionAmt": "-3.0", "entryPrice": "99.0"}
    pos = h.adapter.reconcile_position("BTCUSDT")
    assert (pos.side, pos.quantity, pos.entry_price) == ("SHORT", 3.0, 99.0)


def test_contract_protection_modify_is_reduce_only_and_never_widens(h):
    h.client.update_protection.return_value = {"status": "success"}
    h.adapter.modify_protection("BTCUSDT", side="LONG", quantity=6.0, existing_stop=95.0, new_stop=98.0)
    req = h.client.update_protection.call_args[0][0]
    assert req.reduce_only is True and req.new_sl_price == 98.0
    h.client.update_protection.reset_mock()
    with pytest.raises(ProtectionWideningRefused):
        h.adapter.modify_protection("BTCUSDT", side="LONG", quantity=6.0, existing_stop=95.0, new_stop=94.0)
    with pytest.raises(ProtectionWideningRefused):
        h.adapter.modify_protection("BTCUSDT", side="SHORT", quantity=6.0, existing_stop=105.0, new_stop=106.0)
    h.client.update_protection.assert_not_called()


def test_contract_reduce_and_exit_delegate_to_existing_paths(h):
    h.executor.execute_tp1_partial_close = MagicMock(return_value={"status": "ok"})
    h.adapter.submit_reduce("BTCUSDT", side="LONG", fraction=0.5, live_qty=6.0, sl_price=95.0, tp_price=110.0)
    assert h.executor.execute_tp1_partial_close.call_args.kwargs["tp1_fraction"] == 0.5
    with pytest.raises(ValueError):
        h.adapter.submit_reduce("BTCUSDT", side="LONG", fraction=1.0, live_qty=6.0)
    h.executor.execute_signal = MagicMock(return_value=SimpleNamespace(status="CLOSED_POSITION", success=True,
                                                                      avg_price=101.0))
    out = h.adapter.submit_exit("BTCUSDT", side="LONG", quantity=6.0)
    assert out["status"] == "CLOSED_POSITION" and h.executor.execute_signal.call_args.args[1] == "CLOSE"


# ============================== EXIT INTENT HANDOFF ==============================
def _decision(plan, action, **kw):
    base = dict(position_forecast_id="pfc_x", position_id="pos_1", trade_plan_id=plan.trade_plan_id,
                position_path_id="ppath_x", user_id=plan.user_id, broker_account_id=plan.broker_account_id,
                bot_instance_id=plan.bot_instance_id, action=action, requested_fraction=None,
                suggested_protection_price=None, existing_protection_price=95.0, side=plan.side, decision_time=1,
                conservative_remaining_edge_R=0.0, thesis_status="VALID", reason_codes=(), policy_version="1",
                policy_hash="h", engine_version="1")
    base.update(kw)
    return ExitDecision.build(**base)


def test_exit_routing_disabled_by_default(h):
    b = h.boundary(config=CATIExecutionConfig(active_execution_enabled=True))
    h.adapter.submit_exit = MagicMock()
    res = b.process_exit_decision(_decision(h.plan, "EXIT"), h.plan, live_qty=6.0)
    assert res.status == B.EXIT_ROUTING_DISABLED
    h.adapter.submit_exit.assert_not_called()
    hold = b.process_exit_decision(_decision(h.plan, "HOLD"), h.plan, live_qty=6.0)
    assert hold.status == B.NO_BROKER_ACTION


def test_exit_routing_when_explicitly_enabled(h):
    b = h.boundary(config=CATIExecutionConfig(active_execution_enabled=True, exit_intent_routing_enabled=True))
    h.adapter.submit_reduce = MagicMock(return_value={"status": "ok"})
    res = b.process_exit_decision(_decision(h.plan, "REDUCE", requested_fraction=0.5), h.plan, live_qty=6.0)
    assert res.status == B.EXIT_ROUTED and h.adapter.submit_reduce.call_args.kwargs["fraction"] == 0.5
    h.client.update_protection.return_value = {"status": "success"}
    ok = b.process_exit_decision(_decision(h.plan, "TIGHTEN_PROTECTION", suggested_protection_price=98.0), h.plan,
                                 live_qty=6.0)
    assert ok.status == B.EXIT_ROUTED
    nf = b.process_exit_decision(_decision(h.plan, ExitAction.NO_CHANGE_FALLBACK.value), h.plan, live_qty=6.0)
    assert nf.status == B.NO_BROKER_ACTION


# ============================== SAFETY ==============================
def test_no_secret_leaks_into_execution_evidence(tmp_path):
    secret = "apiKey=AKIAxxSECRETxxKEY9 signature=0123456789abcdef0123456789abcdef"
    h = Harness(tmp_path, entry=_entry(), order_error=RuntimeError(secret))
    h.run()
    with h.db.connect() as conn:
        blob = json.dumps([dict(r) for r in conn.execute("SELECT * FROM cati_execution_attempts").fetchall()]
                          + [dict(r) for r in conn.execute("SELECT * FROM cati_risk_decisions").fetchall()])
    assert "AKIAxxSECRETxxKEY9" not in blob and "0123456789abcdef0123456789abcdef" not in blob


def test_account_margin_reservation_is_settled_not_leaked(h):
    h.run()
    # the executor's own reservation authority settled to the broker-executed margin
    assert ACCOUNT_RESERVATIONS.pending_margin(h.executor._broker_account_id) >= 0.0
    assert "BTCUSDT" in occupied_slots(h.db, h.plan.bot_instance_id)["pending"]
