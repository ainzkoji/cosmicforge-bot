"""Production execution acceptance: real persistence/risk/executor, mock brokers only."""
from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from _exec import Harness, _entry, _order
from app.core import config
from app.core.config import Settings
from app.trading_intelligence.integration import production_execution as production
from app.trading_intelligence.integration.residual_prospective import Tracker, H, Q, FAMILY, REGISTRY_HASH, SOURCE
from app.trading_intelligence.execution.config import CATIExecutionConfig
from app.trading_intelligence.execution.boundary import CATIExecutionBoundary, BoundaryStatus
from app.trading_intelligence.contracts.trade_plan import TradePlan
from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore
from app.trading_intelligence.contracts.execution import ExecutionAttempt, ExecutionAttemptStatus as X
from app.trading_intelligence.execution.adapter import OrderState, BrokerPositionState


def profile(enabled=False):
    return Settings(_env_file=None, APP_ENV="PRODUCTION", DATABASE_ROLE="production", ENVIRONMENT_NAME="production",
                    EXECUTION_MODE="live", LIVE_ORDER_SUBMISSION_ENABLED=enabled)


@pytest.fixture
def live(tmp_path, monkeypatch):
    h = Harness(tmp_path)
    tracker = Tracker(h.db, now_ms=h.now-H)
    decision = (h.now//H)*H-1
    h.now = decision+1000
    candidate = dict(symbol="ADAUSDT", side="LONG", score=3., entry_reference=100., stop=98., target=105., risk=2., atr14=1.)
    snapshot = {"candidate": candidate, "eligible_universe": production.frozen_definition()[0]["universe"][:30],
                "reason": "SELECTED_TOP1"}
    did, _ = tracker.commit_decision(decision, snapshot, h.now)
    tracker.record_entry(did, decision+1, 100., h.now)
    with h.db.connect() as c:
        row = dict(c.execute("SELECT * FROM cati_residual_decisions WHERE decision_id=?", (did,)).fetchone())
    # Use real catalog, reservation, immutable plan and existing executor.
    ins = h.instrument("ADAUSDT")
    h.seed_catalog(now=h.now-2000, instruments=[ins])
    h.orch.update_allowed_symbols(["ADAUSDT"], leverage=5)
    h.executor._live_symbols_override = {"ADAUSDT"}
    reservation = h.reservations.reserve(broker_account_id=h.plan.broker_account_id, bot_instance_id=h.plan.bot_instance_id,
        cycle_id=did, selected=[(did, ins.canonical_symbol, "BINANCE_USDM", "ADAUSDT", "LONG")], now_ms=h.now, ttl_seconds=900)
    h.plan = production.build_plan(row, {"id":h.plan.broker_account_id,"user_id":h.plan.user_id,"environment":"LIVE"}, h.plan.bot_instance_id,
                                   ins, reservation.reservation.reservation_id)
    h.adapter.execution_support_status = "PRODUCTION_VALIDATED"  # explicit test certification, never runtime promotion
    h.executor.client._production_db = h.db
    h.executor.client._production_account_id = h.plan.broker_account_id
    h.client.broker_environment = "LIVE"
    h.client.get_instrument.return_value = ins
    h.client.get_algo_orders.return_value = []
    h.kw["evaluated"] = replace(h.kw["evaluated"], venue_observation=replace(h.kw["evaluated"].venue_observation,
        execution_capabilities=replace(h.kw["evaluated"].venue_observation.execution_capabilities, venue_symbol="ADAUSDT",
            supported_order_types=("MARKET", "LIMIT", "STOP_MARKET", "TAKE_PROFIT_MARKET"))))
    settings = profile()
    monkeypatch.setattr(config, "settings", settings)
    monkeypatch.setattr(production, "settings", settings)
    monkeypatch.setattr("app.trading_intelligence.integration.residual_prospective.owner_current", lambda db: True)
    monkeypatch.setattr(production, "owner_current", lambda db: True)
    def live_boundary(**kw):
        preflight = h.preflight(seed=False)
        preflight.account_environment = "live"
        return h.boundary(config=CATIExecutionConfig(True, False, ("LIVE",)), preflight=preflight, **kw)
    h.live_boundary = live_boundary
    TradePlanEvidenceStore(h.db).append(h.plan)
    return h


def test_gate_false_runs_risk_and_never_mutates(live):
    out = live.run(live.live_boundary(), atr=1.)
    assert out.status == "LIVE_ORDER_SUBMISSION_DISABLED"
    assert out.risk_decision.approved
    live.client.place_order.assert_not_called()
    assert live.live_boundary().risk_store.for_plan(live.plan.broker_account_id, live.plan.trade_plan_id)


def test_gate_true_creates_exactly_once_and_replay_is_suppressed(live, monkeypatch):
    monkeypatch.setattr(config, "settings", profile(True))
    b = live.live_boundary()
    first = live.run(b, atr=1.)
    assert first.status == BoundaryStatus.EXECUTED, first
    assert first.attempt.client_order_id and first.attempt.broker_order_id
    assert live.run(b, atr=1.).status == BoundaryStatus.DUPLICATE_PLAN
    assert live.client.place_order.call_count == 1
    assert len(b.attempts.history(live.plan.broker_account_id, first.attempt.execution_attempt_id)) == 2


@pytest.mark.parametrize("case", ["hard_risk", "geometry", "wrong_environment", "wrong_owner", "legacy", "kill"])
def test_production_rejections_never_mutate(live, monkeypatch, case):
    monkeypatch.setattr(config, "settings", profile(True))
    kw, plan, boundary_kw = {}, live.plan, {}
    if case == "hard_risk":
        kw["adaptive_daily_risk"] = {"daily_risk_state":"HARD_STOP", "decision_reason":"DAILY_RISK_BUDGET_EXHAUSTED"}
    elif case == "geometry":
        plan = replace(plan, structural_invalidation_price=81.6)
    elif case == "wrong_environment":
        plan = replace(plan, environment="DEMO")
    elif case == "wrong_owner":
        boundary_kw["account_scope"] = ("somebody_else", plan.broker_account_id)
    elif case == "legacy":
        plan = replace(plan, setup_family="TRADINGVIEW")
    else:
        from app.trading_intelligence.governance.promotion import StaticAuthority
        boundary_kw["authority"] = StaticAuthority(False, "CATI_NEW_ENTRY_KILL_SWITCH")
    out = live.run(live.live_boundary(**boundary_kw), plan=plan, atr=1., **kw)
    assert out.status != BoundaryStatus.EXECUTED
    live.client.place_order.assert_not_called()


def test_restart_after_persisted_intent_reads_before_any_create(live, monkeypatch):
    monkeypatch.setattr(config, "settings", profile(True))
    b = live.live_boundary()
    original = b.adapter.submit_entry
    def crash(request):
        raise KeyboardInterrupt("simulated process interruption")
    b.adapter.submit_entry = crash
    with pytest.raises(KeyboardInterrupt):
        live.run(b, atr=1.)
    b.adapter.submit_entry = original
    fresh = live.live_boundary()
    seen = []
    fresh.adapter.query_order = lambda *a, **kw: (seen.append("READ_BACK") or OrderState(None, kw["client_order_id"], None, 0., 0., False))
    fresh.adapter.reconcile_position = lambda *a: BrokerPositionState("ADAUSDT", "FLAT", 0., None, True)
    recovery = fresh.recover_pending(now_ms=live.now)
    assert seen == ["READ_BACK"] and recovery[0].status == BoundaryStatus.STILL_UNKNOWN
    assert live.run(fresh, atr=1.).status == BoundaryStatus.DUPLICATE_PLAN
    live.client.place_order.assert_not_called()


def test_ambiguous_create_reconciles_and_does_not_retry(live, monkeypatch):
    monkeypatch.setattr(config, "settings", profile(True))
    live.client.place_order.side_effect = TimeoutError("timeout")
    b = live.live_boundary()
    out = live.run(b, atr=1.)
    assert out.status == BoundaryStatus.SUBMIT_UNKNOWN
    assert out.attempt.client_order_id
    b.adapter.query_order = Mock(return_value=OrderState(None, out.attempt.client_order_id, None, 0., 0., False))
    b.recover_pending(now_ms=live.now)
    b.adapter.query_order.assert_called_once()
    live.run(b, atr=1.)
    assert live.client.place_order.call_count == 1


def test_partial_fill_uses_broker_quantity(live, monkeypatch):
    monkeypatch.setattr(config, "settings", profile(True))
    live.client.place_order.side_effect = _entry("0.3", "100", "PARTIALLY_FILLED")
    live.client.get_order.return_value = _order("PARTIALLY_FILLED", "0.3", "100")
    live.client.get_order.side_effect = None
    out = live.run(live.live_boundary(), atr=1.)
    assert out.attempt.status == X.PARTIALLY_FILLED.value
    assert out.attempt.filled_quantity == pytest.approx(.3)
    assert float(live.seen["protection"][0].qty) == pytest.approx(.3)


def test_account_risk_includes_other_bots_and_manual_exposure(live):
    production.initialize(live.db)
    client = Mock()
    client.account.return_value = dict(totalMarginBalance=5000, totalWalletBalance=5000, availableBalance=4900,
                                      totalInitialMargin=100, totalUnrealizedProfit=0)
    client.income_history.return_value = []
    out = production.account_risk(live.db, {"id":"shared"}, client,
        [{"symbol":"ETHUSDT", "positionAmt":"1"}], [], [{"id":"a"}, {"id":"b"}], live.now)
    assert out["open_positions"] == 1 and out["bot_instance_ids"] == ["a", "b"]
    assert out["reason"] == "ACCOUNT_WIDE_POSITION_OR_ORDER_ACTIVE"
    assert out["adaptive_daily_risk"]["hard_daily_cap_usdt"] == 125


def test_daily_hard_cap_latches_across_restart(live):
    production.initialize(live.db)
    client = Mock()
    client.account.return_value = dict(totalMarginBalance=4875, totalWalletBalance=4875, availableBalance=4875,
                                      totalInitialMargin=0, totalUnrealizedProfit=0)
    client.income_history.return_value = [{"incomeType":"REALIZED_PNL", "income":"-125", "time":live.now}]
    risk = production.account_risk(live.db, {"id":"loss"}, client, [], [], [{"id":"a"}], live.now)
    assert risk["reason"] == "DAILY_HARD_LOSS_CAP_REACHED" and risk["remaining_daily_risk"] == 0
    client.income_history.return_value = []
    assert production.account_risk(live.db, {"id":"loss"}, client, [], [], [{"id":"a"}], live.now)["loss_latched"]


def test_live_certification_cannot_be_assumed(live, monkeypatch):
    monkeypatch.setattr(config, "settings", profile(True))
    live.adapter.execution_support_status = "CONTRACT_VALIDATED"
    out = live.run(live.live_boundary(), atr=1.)
    assert out.status == BoundaryStatus.ADAPTER_UNVALIDATED and out.risk_decision is None
    live.client.place_order.assert_not_called()


def test_native_protection_unknown_reads_back_before_recreate(live, monkeypatch):
    from app.execution.production_protection import place_native_protection
    from app.models.unified_trading import ProtectionRequest, Side
    monkeypatch.setattr(config, "settings", profile(True))
    client = Mock(broker_environment="LIVE", _production_db=live.db, _production_account_id=live.plan.broker_account_id,
                  _production_intent_identity="natural-test-intent")
    client.get_instrument.return_value = SimpleNamespace(tick_size=.01)
    client.get_algo_orders.return_value = []
    client._signed_post.side_effect = TimeoutError()
    req = ProtectionRequest(symbol="ADAUSDT", position_side=Side.BUY, qty=1, sl_price="98", tp_price="105")
    with pytest.raises(TimeoutError):
        place_native_protection(client, req)
    with pytest.raises(ValueError, match="OUTCOME_UNKNOWN"):
        place_native_protection(client, req)
    assert client.get_algo_orders.call_count == 2 and client._signed_post.call_count == 1


def test_runtime_persists_disabled_reason_without_mutation(live, monkeypatch):
    # process_account owns reservation acquisition. Start with a free execution
    # portfolio rather than the Harness's pre-reserved boundary-only plan.
    live.reservations.release(live.plan.portfolio_reservation_id, live.now)
    from shared_lib.broker.auto_trading import set_authorization
    with live.db.connect() as c:
        set_authorization(c, account_id=live.plan.broker_account_id, user_id=live.plan.user_id,
            bot_instance_id=live.plan.bot_instance_id, enabled=True, now="fixture")
    from app.trading_intelligence.execution.preflight import SubmissionPreflight
    from app.exchange.instruments import InstrumentCatalog
    from app.product_safety import execution_safety
    account = {"id": live.plan.broker_account_id, "user_id": live.plan.user_id,
               "broker_id": "binance", "environment": "live"}
    with live.db.connect() as c:
        c.execute("UPDATE bot_instances SET status='stopped' WHERE broker_account_id=? AND id<>?",
                  (account["id"], live.plan.bot_instance_id))
    cat = InstrumentCatalog(live.db)
    cat.upsert("binance_usdm", "LIVE", [live.instrument("ADAUSDT")], live.now-2000)
    b = live.live_boundary()
    b.preflight = SubmissionPreflight(catalog=cat, broker="binance", venue_key="binance_usdm",
        catalog_environment="LIVE", account_environment="live")
    b.authority.gov = SimpleNamespace(kill_switch_on=lambda **kw: False)
    live.client.account.return_value = dict(totalWalletBalance=5000, totalMarginBalance=5000,
        availableBalance=5000, totalInitialMargin=0, totalUnrealizedProfit=0)
    live.client.income_history.return_value = []
    live.client.last_price.return_value = 100.
    live.client.klines.return_value = []
    live.client.exchange_info_cached.return_value = {"symbols": [{"symbol":"ADAUSDT", "baseAsset":"ADA",
        "quoteAsset":"USDT", "marginAsset":"USDT", "contractType":"PERPETUAL", "orderTypes":["MARKET","STOP_MARKET","TAKE_PROFIT_MARKET"],
        "timeInForce":["GTC"], "filters":[{"filterType":"PRICE_FILTER","tickSize":".01"},
        {"filterType":"LOT_SIZE","stepSize":".001","minQty":".001"}]}]}
    monkeypatch.setattr(production, "persisted_risk_controls", lambda *a: {})
    monkeypatch.setattr(execution_safety, "evaluate_execution_kyc", lambda **kw: SimpleNamespace(allowed=True, state="APPROVED"))
    monkeypatch.setattr(execution_safety, "evaluate_execution_readiness", lambda **kw: SimpleNamespace(allowed=True, state="APPROVED"))
    result = production.process_account(live.db, account, live.client, {"positions":[], "orders":[]},
        now_ms=live.now, boundary_factory=lambda *a: b)
    assert result["reason"] == "LIVE_ORDER_SUBMISSION_DISABLED"
    assert result["boundary"]["risk_decision"]["status"] == "APPROVED", result
    live.client.place_order.assert_not_called()
    with live.db.connect() as c:
        stored = json.loads(c.execute("SELECT document FROM cati_production_decisions WHERE account_id=? AND decision_id=?",
            (account["id"], live.plan.source_candidate_id)).fetchone()[0])
    assert stored["reason"] == "LIVE_ORDER_SUBMISSION_DISABLED"


def test_broker_fill_reconciles_actual_position(live):
    from app.execution.position_reconciliation import parse_broker_positions, reconcile_position_rows
    from decimal import Decimal
    raw = [{"symbol":"ADAUSDT", "positionAmt":".3", "entryPrice":"100", "leverage":"5", "positionSide":"BOTH"}]
    reconcile_position_rows(live.db, bot_instance_id=live.plan.bot_instance_id, broker_account_id=live.plan.broker_account_id,
        broker_positions=parse_broker_positions(raw, hedge_mode=False), position_mode="ONE_WAY",
        spec_resolver=lambda s: SimpleNamespace(step_size=Decimal(".001"), min_qty=Decimal(".001")),
        execution_mode="broker", broker_environment="live")
    with live.db.connect() as c:
        row = c.execute("SELECT remaining_qty,broker_account_id,broker_environment FROM positions WHERE status='OPEN' AND symbol='ADAUSDT'").fetchone()
    assert row[0] == pytest.approx(.3) and row[1] == live.plan.broker_account_id and row[2] == "live"


def test_residual_stop_is_rejected_instead_of_clamped(live):
    from app.trading_intelligence.trade_plan.validation import MarketReference
    from app.trading_intelligence.contracts.system_health import BrokerHealthContext
    fields = {k:getattr(live.plan,k) for k in TradePlan.__dataclass_fields__ if k not in TradePlan._NON_ANALYTICAL}
    fields["structural_invalidation_price"] = 81.6
    fields["initial_risk_distance"] = 18.4
    plan = TradePlan.build(**fields)
    out = live.orch.process_trade_plan(plan, now_ms=live.now, market_reference=MarketReference(100, live.now),
        broker_health=BrokerHealthContext(plan.broker_account_id, plan.venue, "LIVE", "HEALTHY", live.now, "test"),
        reservation_state=live.reservations.get(plan.portfolio_reservation_id),
        venue_capabilities=live.kw["evaluated"].venue_observation.execution_capabilities,
        klines=[], current_equity=5000, margin_used=0, margin_available=5000, open_positions=0, atr=1)
    assert not out["risk_decision"].approved
    assert "STOP_DISTANCE_EXCEEDS_HARD_MAXIMUM" in out["risk_decision"].reason_codes
    live.client.place_order.assert_not_called()


def test_account_factory_constructs_live_components_without_legacy_runner(live, monkeypatch):
    from app.models.bot_instance_models import BotInstance
    from app.core.bot_instance_service import BotInstanceService
    from app.core import broker_capability_gate
    instance = BotInstance(id=live.plan.bot_instance_id, user_id=live.plan.user_id,
        broker_account_id=live.plan.broker_account_id, market_type="CRYPTO", strategy_id="cati", strategy_version="1.0.0",
        risk_level="balanced", symbols=["ADAUSDT"], timeframes=["15m"], allocation_type="fixed_amount",
        allocation_value=120., mode="live", capital_allocation=5000., capital_allocation_type="fixed_amount")
    monkeypatch.setattr(BotInstanceService, "get_bot_instance", lambda self, bot_id: instance)
    monkeypatch.setattr(broker_capability_gate, "assert_broker_execution_capability", lambda *a, **kw: None)
    account = {"id":live.plan.broker_account_id, "user_id":live.plan.user_id, "broker_id":"binance", "environment":"live"}
    boundary = production.boundary_for(live.db, account, {"id":live.plan.bot_instance_id}, live.client)
    assert boundary.account_scope == (instance.user_id, instance.broker_account_id)
    assert boundary.orchestrator.validated_config.paper_mode is False
    assert boundary.adapter.executor.execution_mode == "live"
    assert boundary.adapter.executor._broker_account_id == instance.broker_account_id
    assert boundary.config.allowed_environments == ("DEMO", "LIVE")


def test_production_quantity_uses_connected_account_metadata(live, monkeypatch):
    from app.execution import executor
    monkeypatch.setattr(executor, "settings", profile(True))
    live.client.get_prices.return_value = {"ADAUSDT":100.}
    qty, detail = executor.BinanceExecutor._size_qty(live.executor, "ADAUSDT", 120., sl_price=98., leverage_override=5)
    assert qty > 0, detail
    assert detail["step_size"] == .001
    live.client.get_instrument.assert_called_with("ADAUSDT")
    live.client.place_order.assert_not_called()


def test_orphan_protection_blocks_next_entry(live):
    production.initialize(live.db)
    client = Mock()
    client.account.return_value = dict(totalMarginBalance=5000, totalWalletBalance=5000, availableBalance=5000,
                                      totalInitialMargin=0, totalUnrealizedProfit=0)
    client.income_history.return_value = []
    out = production.account_risk(live.db, {"id":"orphan"}, client, [],
        [{"symbol":"ADAUSDT", "closePosition":True, "type":"STOP_MARKET"}], [{"id":"a"}], live.now)
    assert out["reason"] == "ACCOUNT_WIDE_ORPHAN_ORDER_ACTIVE"
