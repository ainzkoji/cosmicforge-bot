"""Production DEMO uses real persistence/risk/lifecycle and mocked transports."""
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock
import json

import pytest
from test_production_execution import live
from app.core import config
from app.core.config import Settings
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.execution.config import CATIExecutionConfig
from app.trading_intelligence.execution.boundary import BoundaryStatus
from app.trading_intelligence.execution.adapter import OrderState, BrokerPositionState, adapter_supports
from app.trading_intelligence.execution.binance_adapter import executor_adapter_for
from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore
from shared_lib.broker.environment import normalize_environment, resolve_base_url
from shared_lib.core.production import (require_broker_mutation_permission, order_submission_gate,
    LiveOrderSubmissionDisabled, DemoOrderSubmissionDisabled)


def demo_profile(demo=True, live=False):
    return Settings(_env_file=None, APP_ENV="PRODUCTION", DATABASE_ROLE="production",
        ENVIRONMENT_NAME="production", EXECUTION_MODE="live",
        DEMO_ORDER_SUBMISSION_ENABLED=demo, LIVE_ORDER_SUBMISSION_ENABLED=live)


@pytest.fixture
def demo(live, monkeypatch):
    state = demo_profile()
    monkeypatch.setattr(config, "settings", state)
    monkeypatch.setattr(production, "settings", state)
    live.client.broker_environment = normalize_environment("testnet")
    with live.db.connect() as c:
        row = dict(c.execute("SELECT * FROM cati_residual_decisions WHERE decision_id=?",
                             (live.plan.source_candidate_id,)).fetchone())
    live.account = {"id":live.plan.broker_account_id, "user_id":live.plan.user_id,
                    "broker_id":"binance", "environment":"testnet"}
    live.plan = production.build_plan(row, live.account, live.plan.bot_instance_id,
        live.instrument("ADAUSDT"), live.plan.portfolio_reservation_id)
    TradePlanEvidenceStore(live.db).append(live.plan)
    live.adapter = executor_adapter_for("binance", live.executor)
    live.demo_boundary = lambda **kw: live.boundary(config=CATIExecutionConfig(True, False, ("DEMO","LIVE")),
        preflight=live.preflight(seed=False), **kw)
    return live


def test_demo_gate_creates_once_and_never_promotes_live(demo):
    b = demo.demo_boundary()
    assert demo.plan.environment == "DEMO"
    assert adapter_supports(demo.adapter, "DEMO", production_scope=True)
    assert not adapter_supports(demo.adapter, "LIVE", production_scope=True)
    out = demo.run(b, atr=1.)
    assert out.status == BoundaryStatus.EXECUTED, out
    assert demo.run(demo.demo_boundary(), atr=1.).status == BoundaryStatus.DUPLICATE_PLAN
    assert demo.client.place_order.call_count == 1
    assert out.attempt.environment == "DEMO"
    assert out.attempt.client_order_id


def test_disabled_demo_gate_runs_risk_and_records_exact_reason(demo, monkeypatch):
    monkeypatch.setattr(config, "settings", demo_profile(demo=False, live=True))
    out = demo.run(demo.demo_boundary(), atr=1.)
    assert out.status == "DEMO_ORDER_SUBMISSION_DISABLED"
    assert out.risk_decision.approved
    demo.client.place_order.assert_not_called()


@pytest.mark.parametrize("case", ["cap", "kill", "legacy", "geometry", "environment", "owner"])
def test_demo_preserves_fail_closed_checks(demo, case):
    plan, kw, options = demo.plan, {}, {}
    if case == "cap":
        kw["adaptive_daily_risk"] = {"daily_risk_state":"HARD_STOP", "decision_reason":"DAILY_HARD_LOSS_CAP_REACHED"}
    elif case == "kill":
        from app.trading_intelligence.governance.promotion import StaticAuthority
        options["authority"] = StaticAuthority(False, "CATI_NEW_ENTRY_KILL_SWITCH")
    elif case == "legacy":
        plan = replace(plan, setup_family="TRADINGVIEW")
    elif case == "geometry":
        plan = replace(plan, structural_invalidation_price=81.6)
    elif case == "environment":
        plan = replace(plan, environment="LIVE")
    else:
        options["account_scope"] = ("another-user", plan.broker_account_id)
    out = demo.run(demo.demo_boundary(**options), plan=plan, atr=1., **kw)
    assert out.status != BoundaryStatus.EXECUTED
    if case == "environment":
        assert out.reason_codes == ("BROKER_ENVIRONMENT_MISMATCH",)
    demo.client.place_order.assert_not_called()


def test_demo_crash_after_intent_reads_before_recreate(demo):
    b = demo.demo_boundary()
    submit = demo.adapter.submit_entry
    demo.adapter.submit_entry = Mock(side_effect=KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        demo.run(b, atr=1.)
    demo.adapter.submit_entry = submit
    demo.adapter.query_order = Mock(return_value=OrderState(None,None,None,0,0,False))
    demo.adapter.reconcile_position = Mock(return_value=BrokerPositionState("ADAUSDT","FLAT",0,None,True))
    fresh = demo.demo_boundary()
    assert fresh.recover_pending(now_ms=demo.now)[0].status == BoundaryStatus.STILL_UNKNOWN
    demo.adapter.query_order.assert_called_once()
    assert demo.run(fresh, atr=1.).status == BoundaryStatus.DUPLICATE_PLAN
    demo.client.place_order.assert_not_called()


def test_unknown_demo_create_reads_back_without_retry(demo):
    demo.client.place_order.side_effect = TimeoutError()
    b = demo.demo_boundary()
    out = demo.run(b, atr=1.)
    assert out.status == BoundaryStatus.SUBMIT_UNKNOWN
    demo.adapter.query_order = Mock(return_value=OrderState(None,out.attempt.client_order_id,None,0,0,False))
    demo.adapter.reconcile_position = Mock(return_value=BrokerPositionState("ADAUSDT","FLAT",0,None,True))
    b.recover_pending(now_ms=demo.now)
    demo.adapter.query_order.assert_called_once()
    demo.run(b, atr=1.)
    assert demo.client.place_order.call_count == 1


def test_demo_native_protection_is_account_scoped_close_only(demo):
    from app.execution.production_protection import place_native_protection
    from app.models.unified_trading import ProtectionRequest, Side
    demo.client._production_intent_identity = demo.plan.trade_plan_id + "|" + demo.plan.trade_plan_hash
    demo.client._signed_post.side_effect = [{"algoId":"sl"},{"algoId":"tp"}]
    req = ProtectionRequest(symbol="ADAUSDT", position_side=Side.BUY, qty=1, sl_price="98", tp_price="105")
    assert place_native_protection(demo.client, req).status == "success"
    for call in demo.client._signed_post.call_args_list:
        params = call.kwargs["params"]
        assert params["closePosition"] == "true" and params["side"] == "SELL"
        assert "quantity" not in params and params["clientAlgoId"]
    with pytest.raises(ValueError, match="OUTCOME_UNKNOWN"):
        place_native_protection(demo.client, req)
    assert demo.client._signed_post.call_count == 2


@pytest.mark.parametrize("broker", ["bybit","bingx","oanda","ibkr","mt5"])
def test_unsupported_demo_adapter_truthful_and_never_uses_binance(demo, broker):
    account = {**demo.account, "broker_id":broker}
    if broker in {"bybit", "bingx"}:
        # The implemented contracts now require broker-wide risk rather than
        # returning capability-unavailable. An incomplete account read fails.
        with pytest.raises(KeyError):
            production.process_account(demo.db, account, demo.client, {"positions":[],"orders":[]}, now_ms=demo.now)
        demo.client.account.assert_called_once()
        demo.client.place_order.assert_not_called()
        return
    out = production.process_account(demo.db, account, demo.client, {"positions":[],"orders":[]}, now_ms=demo.now)
    assert out["reason"] == "DEMO_CAPABILITY_UNAVAILABLE"
    assert out["execution_permission"] == "BLOCKED_ACCOUNT"
    assert out["missing_capabilities"]
    demo.client.place_order.assert_not_called()
    demo.client.income_history.assert_not_called()


@pytest.mark.parametrize("alias", ["demo","testnet","sandbox","paper","test"])
def test_canonical_aliases_use_only_demo_gate(monkeypatch, alias):
    monkeypatch.setattr(config, "settings", demo_profile())
    env = normalize_environment(alias)
    require_broker_mutation_permission("POST", environment=env, broker="binance", base_url=resolve_base_url("binance",env))
    assert order_submission_gate(env)["name"] == "DEMO_ORDER_SUBMISSION_ENABLED"
    with pytest.raises(LiveOrderSubmissionDisabled):
        require_broker_mutation_permission("POST", environment="live", broker="binance", base_url=resolve_base_url("binance",normalize_environment("live")))


@pytest.mark.parametrize("broker", ["binance","bybit","bingx"])
def test_endpoint_mismatch_is_rejected_even_with_both_gates_enabled(monkeypatch, broker):
    monkeypatch.setattr(config, "settings", demo_profile(live=True))
    with pytest.raises(ValueError, match="BROKER_ENVIRONMENT_MISMATCH"):
        require_broker_mutation_permission("POST", environment="DEMO", broker=broker,
                                          base_url=resolve_base_url(broker,normalize_environment("live")))
    with pytest.raises(ValueError, match="BROKER_ENVIRONMENT_MISMATCH"):
        require_broker_mutation_permission("DELETE", environment="LIVE", broker=broker,
                                          base_url=resolve_base_url(broker,normalize_environment("demo")))


def test_demo_partial_fill_uses_only_confirmed_quantity(demo):
    from _exec import _entry, _order
    demo.client.place_order.side_effect = _entry("0.3", "100", "PARTIALLY_FILLED")
    demo.client.get_order.return_value = _order("PARTIALLY_FILLED", "0.3", "100")
    demo.client.get_order.side_effect = None
    out = demo.run(demo.demo_boundary(), atr=1.)
    assert out.attempt.status == "PARTIALLY_FILLED"
    assert out.attempt.filled_quantity == pytest.approx(.3)
    assert float(demo.seen["protection"][0].qty) == pytest.approx(.3)


def test_demo_daily_loss_latch_and_shared_account_exposure(demo):
    import test_production_execution as existing
    existing.test_daily_hard_cap_latches_across_restart(demo)
    existing.test_account_risk_includes_other_bots_and_manual_exposure(demo)


def test_demo_factory_binds_current_account_environment(demo, monkeypatch):
    from app.models.bot_instance_models import BotInstance
    from app.core.bot_instance_service import BotInstanceService
    from app.core import broker_capability_gate
    instance = BotInstance(id=demo.plan.bot_instance_id, user_id=demo.plan.user_id,
        broker_account_id=demo.plan.broker_account_id, market_type="CRYPTO", strategy_id="cati", strategy_version="1.0.0",
        risk_level="balanced", symbols=["ADAUSDT"], timeframes=["15m"], allocation_type="fixed_amount",
        allocation_value=120., mode="testnet", capital_allocation=5000., capital_allocation_type="fixed_amount")
    monkeypatch.setattr(BotInstanceService, "get_bot_instance", lambda self, bot_id: instance)
    monkeypatch.setattr(broker_capability_gate, "assert_broker_execution_capability", lambda *a, **kw: None)
    boundary = production.boundary_for(demo.db, demo.account, {"id":instance.id}, demo.client)
    assert boundary.orchestrator.validated_config.paper_mode is False
    assert boundary.adapter.executor.execution_mode == "live"
    assert boundary.preflight.catalog_environment == "DEMO"
    assert boundary.preflight.account_environment == "demo"
    assert adapter_supports(boundary.adapter,"DEMO",production_scope=True)
    assert not adapter_supports(boundary.adapter,"LIVE",production_scope=True)


def test_demo_transport_requires_current_risk_approved_account_intent(demo):
    from _exec import _entry
    demo.client._broker_account_id = demo.plan.broker_account_id
    demo.client._broker_user_id = demo.plan.user_id
    from shared_lib.core.production import require_broker_mutation_permission
    seen=[]
    def create(request):
        require_broker_mutation_permission("POST", "/fapi/v1/order", environment="DEMO", broker="binance",
            base_url=resolve_base_url("binance",normalize_environment("demo")), client=demo.client,
            payload={"symbol":"ADAUSDT","side":"BUY"})
        seen.append("TRANSPORT_AUTHORIZED_AFTER_INTENT")
        return _entry("1.2","100","FILLED")(request)
    demo.client.place_order.side_effect=create
    out=demo.run(demo.demo_boundary(),atr=1.)
    assert out.status==BoundaryStatus.EXECUTED, out
    assert seen==["TRANSPORT_AUTHORIZED_AFTER_INTENT"]
    with pytest.raises(ValueError,match="CATI_ENTRY_AUTHORITY_REQUIRED"):
        require_broker_mutation_permission("POST", "/fapi/v1/order", environment="DEMO", broker="binance",
            base_url=resolve_base_url("binance",normalize_environment("demo")), client=demo.client,
            payload={"symbol":"ADAUSDT","side":"BUY"})


def test_demo_missing_native_target_capability_blocks_before_create(demo):
    evaluated=demo.kw["evaluated"]
    caps=replace(evaluated.venue_observation.execution_capabilities, supported_order_types=("MARKET","STOP_MARKET"))
    demo.kw["evaluated"]=replace(evaluated,venue_observation=replace(evaluated.venue_observation,execution_capabilities=caps))
    out=demo.run(demo.demo_boundary(),atr=1.)
    assert out.status==BoundaryStatus.PREFLIGHT_BLOCKED
    assert out.reason_codes==("DEMO_CAPABILITY_UNAVAILABLE",)
    demo.client.place_order.assert_not_called()
