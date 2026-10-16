"""Canonical handoff with synthetic certified authority and a real fake venue.

No real family is promoted. The boundary, provenance, journals and reservations
are real; the synthetic family stage is explicit and lives only in this test.
"""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import _step3_execution as E
from app.trading_intelligence.integration.portfolio_runtime import ConnectedPortfolioRuntime, close_brakes
from app.trading_intelligence.pipeline import brakes, contracts as C, execution as X
from app.trading_intelligence.pipeline.family_registry import FamilyRegistry


@pytest.fixture
def world(tmp_path_factory, tmp_path):
    return E.make_world(tmp_path_factory, tmp_path)


def setup(world, monkeypatch):
    account = {"id": world.account.broker_account_id, "user_id": world.account.user_id,
               "broker_id": world.account.venue, "environment": world.account.environment}
    with world.db.connect() as conn:
        conn.execute("INSERT INTO bot_instances (id,user_id,broker_account_id,market_type,strategy_id,mode,status,"
                     "created_at,updated_at,risk_level) VALUES ('b',?,?,'futures','cati','demo','active',1,1,'conservative')",
                     (account["user_id"], account["id"]))
    stage = Mock(return_value=SimpleNamespace(
        intents=(E.entry(E.BTC), E.entry(E.ETH)), instruments=(E.BTC, E.ETH), document={}))
    pipeline = SimpleNamespace(broker_account=stage, decision_time_ms=lambda key: E.T0 - 100)
    market = SimpleNamespace(cycle_key="2026-05-28", data_snapshot_id="synthetic-broker-market")
    runtime = ConnectedPortfolioRuntime(
        world.db, registry=world.registry, make_boundary=lambda *args: world.boundary(),
        brake_source=lambda *args: brakes.NO_BRAKE, clock=world.clock)
    monkeypatch.setattr(runtime, "_market", lambda entry, now: (pipeline, market))
    return runtime, account, stage


def test_broker_handoff_executes_multiple_positions_and_restart_reuses_decision(world, monkeypatch):
    runtime, account, stage = setup(world, monkeypatch)
    maintenance = Mock()
    first = runtime.process(account, None, legacy_maintenance=maintenance)
    assert first["reason"] == "PORTFOLIO_DECISION_PROCESSED"
    assert first["families"][0]["execution"]["status"] == X.BATCH_PROCESSED
    assert world.signed(E.BTC) > 0 and world.signed(E.ETH) > 0
    before = len(world.orders_sent(protective=False))
    snapshot = stage.call_args.args[1]
    assert snapshot.account_id == account["id"] and snapshot.positions == ()
    runtime.markets.clear()  # Process-local cache isn't needed for recovery.
    second = runtime.process(account, None, legacy_maintenance=maintenance)
    assert first["families"][0]["decision_id"] == second["families"][0]["decision_id"]
    assert stage.call_count == 1 and len(world.orders_sent(protective=False)) == before
    assert maintenance.call_count == 2
    with world.db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM cati_shadow_fills").fetchone()[0] == 0
        assert conn.execute("SELECT mode FROM cati_family_decisions").fetchone()[0] == C.DEMO


def test_current_real_registry_cannot_reach_broker_decision_or_boundary(world):
    factory = Mock(side_effect=AssertionError("uncertified family reached execution"))
    runtime = ConnectedPortfolioRuntime(world.db, registry=FamilyRegistry(world.db), make_boundary=factory)
    account = {"id": world.account.broker_account_id, "user_id": world.account.user_id,
               "broker_id": world.account.venue, "environment": world.account.environment}
    assert runtime.process(account, None) is None
    factory.assert_not_called()
    assert world.fake.mutations == world.fake.reads == []


@pytest.mark.parametrize("status,permission", [(X.BATCH_REFUSED, "BLOCKED_ACCOUNT"),
                                               (X.BATCH_DEFERRED, "PORTFOLIO_DEFERRED"),
                                               (X.BATCH_HALTED, "BLOCKED_ACCOUNT")])
def test_batch_refusal_or_wait_is_not_reported_as_success(world, monkeypatch, status, permission):
    from app.trading_intelligence.integration.reason_registry import describe
    runtime, account, _ = setup(world, monkeypatch)
    boundary = world.boundary()
    boundary.execute = Mock(return_value=X.BatchResult(status, ("synthetic outcome",), ()))
    runtime.make_boundary = lambda *args: boundary
    result = runtime.process(account, None)
    assert result["execution_permission"] == permission
    assert describe(result["reason"])["severity"] in ("blocked", "waiting", "attention")
    assert world.fake.mutations == []


def test_missing_broker_close_evidence_refuses_before_a_family_decision(world, monkeypatch):
    runtime, account, stage = setup(world, monkeypatch)
    def missing(*args):
        raise ValueError("ACCOUNT_BROKER_CLOSE_EVIDENCE_REQUIRED")
    runtime.brake_source = missing
    with pytest.raises(ValueError, match="ACCOUNT_BROKER_CLOSE_EVIDENCE_REQUIRED"):
        runtime.process(account, None)
    stage.assert_not_called()
    assert world.orders_sent(protective=False) == []
    with world.db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM cati_family_decisions").fetchone()[0] == 0


def test_manual_positions_are_not_adopted_into_a_family_book(world, monkeypatch):
    runtime, account, _ = setup(world, monkeypatch)
    world.fake.external_trade(E.BTC, "BUY", 0.001)
    full = world.boundary().account_snapshot()
    assert len(full.positions) == 1
    assert runtime._owned_positions(full, world.definition.family_id, account).positions == ()


def test_native_stop_then_reentry_starts_a_new_owned_position_lifecycle(world, monkeypatch):
    runtime, account, _ = setup(world, monkeypatch)
    assert world.run(E.entry(E.BTC)).status == X.BATCH_PROCESSED
    stop = world.stop_prices(E.BTC)[0]
    world.fake.set_price(E.BTC, stop - 1)
    assert world.signed(E.BTC) == 0
    world.boundary().recover()
    world.fake.set_price(E.BTC, E.PRICES[E.BTC])
    assert world.run(E.entry(E.BTC)).status == X.BATCH_PROCESSED
    full = world.boundary().account_snapshot()
    owned = runtime._owned_positions(full, world.definition.family_id, account)
    assert len(owned.positions) == 1
    assert owned.positions[0].quantity == pytest.approx(world.signed(E.BTC))


def test_production_rejects_injected_portfolio_dependencies(world, monkeypatch):
    from app.core.config import settings
    monkeypatch.setattr(settings, "APP_ENV", "PRODUCTION")
    monkeypatch.setattr(settings, "RUNTIME_ENV", "PRODUCTION")
    with pytest.raises(ValueError, match="AUTHORITATIVE"):
        ConnectedPortfolioRuntime(world.db, registry=world.registry)


def test_close_brakes_require_consecutive_observed_closes_and_preserve_high_water(world, monkeypatch):
    from app.observability import account_recorder
    from app.trading_intelligence.integration import income_ledger
    account = {"id": world.account.broker_account_id, "user_id": world.account.user_id}
    close = 1_780_012_800_000
    def sample(at, equity):
        account_recorder._insert(world.db, user_id=account["user_id"], account_id=account["id"],
                                  bot_instance_id=None, observed_at=at + 10_000, figures={"equity": equity},
                                  source="ENGINE_CYCLE", freshness_ms=0, reason="HOURLY", dedupe_key=str(at))
    sample(close - 86_400_000, 10_000)
    sample(close, 9_000)
    monkeypatch.setattr(income_ledger, "ensure_covered", Mock())
    monkeypatch.setattr(income_ledger, "rows", lambda *args: [])
    market = SimpleNamespace(cycle_key="2026-05-28", freshness={"decision_time_ms": close})
    flags = close_brakes(world.db, account, None, market, "conservative", close + 20_000)
    assert flags.halve
    state = brakes.current(world.db, account["id"], brakes.ACCOUNT_SCOPE)
    assert state.highest_closing_equity == 10_000
    assert close_brakes(world.db, account, None, market, "conservative", close + 30_000) == flags
    market.cycle_key = "2026-05-30"
    with pytest.raises(ValueError, match="ACCOUNT_BRAKE_CLOSE_GAP"):
        close_brakes(world.db, account, None, market, "conservative", close + 2 * 86_400_000)


def test_portfolio_transport_permit_binds_exact_account_order_and_quantity_and_is_reset(world, monkeypatch):
    from app.trading_intelligence.execution import entry_permit
    from app.trading_intelligence.integration import residual_prospective
    client = SimpleNamespace(_production_db=world.db, _production_account_id=world.account.broker_account_id,
                             _broker_account_id=world.account.broker_account_id, _broker_user_id=world.account.user_id,
                             broker_environment="demo")
    monkeypatch.setattr(residual_prospective, "owner_current", lambda db: True)
    observed = []
    def before_send():
        _, _, request = entry_permit._PORTFOLIO_PERMIT.get()
        payload = {"symbol": request.symbol, "side": request.side, "quantity": str(request.quantity),
                   "newClientOrderId": request.client_order_id, "type": request.order_type}
        observed.append(entry_permit.transport_entry_permitted(client, request.symbol, request.side, payload=payload))
        assert not entry_permit.transport_entry_permitted(client, request.symbol, request.side,
                                                          payload={**payload, "quantity": str(request.quantity * 2)})
        assert not entry_permit.transport_entry_permitted(client, request.symbol, request.side,
                                                          payload={**payload, "newClientOrderId": "another-order"})
        client._broker_user_id = "other-user"
        assert not entry_permit.transport_entry_permitted(client, request.symbol, request.side, payload=payload)
        client._broker_user_id = world.account.user_id
    world.adapter.before["place_order"] = [before_send]
    assert world.run(E.entry(E.BTC)).status == X.BATCH_PROCESSED
    assert observed == [True]
    assert entry_permit._PORTFOLIO_PERMIT.get() is None
    assert not entry_permit.transport_entry_permitted(client, E.BTC, "BUY", payload={})


@pytest.mark.parametrize("control,expected", [
    ({"kill_switch": True}, "CATI_NEW_ENTRY_KILL_SWITCH"),
    ({"consec_loss_day_paused": True}, "CONSECUTIVE_LOSS_DAY_PAUSED"),
    ({"consec_loss_cooldown_until_ms": E.T0 + 100_000}, "CONSECUTIVE_LOSS_COOLDOWN"),
    ({"weekly_drawdown_pct": 20}, "WEEKLY_DRAWDOWN_LIMIT_REACHED"),
    ({"monthly_drawdown_pct": 30}, "MONTHLY_DRAWDOWN_LIMIT_REACHED"),
])
def test_portfolio_handoff_keeps_existing_operating_controls(world, monkeypatch, control, expected):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from app.core.config import settings
    from app.trading_intelligence.integration import production_execution as old
    from app.trading_intelligence.integration.portfolio_runtime import operating_controls
    from app.risk.capital_ledger import ACCOUNT_RESERVATIONS
    risk = {"risk_date": datetime.fromtimestamp(E.T0 / 1000, ZoneInfo(settings.ADAPTIVE_DAILY_RISK_TIMEZONE)).date().isoformat(),
            "reason": None, "remaining_daily_risk": 100}
    controls = {"kill_switch": False, "daily_trade_count": 0, "consec_loss_day_paused": False,
                "consec_loss_cooldown_until_ms": 0, "max_weekly_drawdown_pct": 10,
                "max_monthly_drawdown_pct": 20, "weekly_drawdown_pct": 0, "monthly_drawdown_pct": 0, **control}
    monkeypatch.setattr(old, "account_risk", lambda *args, **kwargs: risk)
    monkeypatch.setattr(old, "persisted_risk_controls", lambda *args: controls)
    monkeypatch.setattr(ACCOUNT_RESERVATIONS, "pending_margin", lambda *args: 0)
    boundary = SimpleNamespace(effective_policy=SimpleNamespace(daily_trade_cap_enabled=True, max_daily_trades=5,
                                                                hard_runaway_daily_entry_limit=10))
    account = {"id": world.account.broker_account_id}
    reason, _, _ = operating_controls(world.db, account, None, [{"id": "b"}], {"positions": [], "orders": []}, boundary, E.T0)
    assert reason == expected
