"""Step 1 closing test -- SYNTHETIC rehearsal of the engine-side journey.

This is NOT the Binance demo certification. The exchange is a controlled fake
and no order reaches any venue. It proves, on one migrated database and through
the real code paths, the part of the closing test that needs no credentials:

    deploy (shared contract, consent, environment from the account, 409 on a
    second bot) -> the production runtime discovers and evaluates the bot ->
    the read model and the events show it to its owner only -> pause, resume
    and stop behave and survive a process restart.

Entry points: ``deployment_service.preview / deploy``, ``production_runtime.sync``
(the 30-second broker cycle with the real ``process_account``), the
``cati_account`` and ``bot_instances`` route handlers, ``BotInstanceService``.
"""
from decimal import Decimal

import pytest
from fastapi import HTTPException
from test_production_execution import live, profile
from test_production_demo_execution import demo, demo_profile
from test_production_loop import accounts_db, state_of
from app.api import bot_instances as bot_routes
from app.api import cati_account
from app.core import broker_capability_gate, deployment_service as svc
from app.core import cati_read_model as model
from app.core.bot_instance_service import BotInstanceService
from app.execution import production_schema
from app.observability import user_events
from app.trading_intelligence.integration import production_runtime as runtime
from shared_lib import risk_levels
from shared_lib.core.production import order_submission_gate
from shared_lib.deployment import contract as C
from shared_lib.deployment.contract import DeploymentRequest
from shared_lib.persistence.db import DB

__all__ = ["live", "profile", "demo", "demo_profile", "accounts_db"]

ALICE, BOB = {"id": "alice"}, {"id": "bob"}


def request(**over):
    base = dict(broker_account_id="acct", budget={"type": "fixed_amount", "value": "1000"}, risk_level="balanced",
                advanced={"max_position_usdt": None, "daily_loss_limit_pct": None, "symbols": []},
                risk_acknowledged=True, request_id="closing-0001")
    base.update(over)
    return DeploymentRequest.model_validate(base)


def balance(db, account, now):
    return {"equity": Decimal("1000"), "available": Decimal("1000"), "wallet": Decimal("1000"), "source": "SYNTHETIC",
            "observed_at": now}


@pytest.fixture
def world(accounts_db, monkeypatch):
    db = accounts_db.db
    with db.connect() as c:
        for uid in ("alice", "bob"):
            c.execute("INSERT INTO users (id,email,hashed_password,status,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                      (uid, f"{uid}@example.test", "x", "active", "2026-10-08", "2026-10-08"))
        c.execute("UPDATE broker_accounts SET label='Demo one' WHERE id='acct'")
    monkeypatch.setattr(broker_capability_gate, "assert_broker_execution_capability", lambda *a, **kw: None)
    accounts_db.client.income_history.return_value = []
    accounts_db.client.get_algo_orders.return_value = []
    accounts_db.client.broker_environment = "demo"                 # the synthetic exchange is a DEMO venue
    accounts_db.client.base_url = "https://demo-fapi.binance.com"
    accounts_db.service = BotInstanceService(db=db)
    return accounts_db


def deploy(world, **over):
    return svc.deploy(world.db, "alice", request(**over), service=world.service, account_state_reader=balance)


# ── Scenario C: preview ─────────────────────────────────────────────────────

def test_c_the_preview_matches_the_shared_risk_profile_library(world):
    out = svc.preview(world.db, "alice", request(risk_acknowledged=False), account_state_reader=balance)
    money = risk_levels.as_json(risk_levels.money_view("balanced", "1000"))
    assert out["money"] == money and out["environment"] == "DEMO" and out["exchange"] == "BINANCE"
    assert out["money"]["risk_per_trade"] == "4.00" and out["money"]["ceiling_applied"] is True     # 0.4 % ceiling, not 0.5 %
    assert out["requirements"] == [C.RISK_NOT_ACKNOWLEDGED] and out["consent"]["version"] == C.CONSENT_VERSION
    with world.db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM bot_instances").fetchone()[0] == 0                   # a preview creates nothing


# ── Scenario D: deployment ──────────────────────────────────────────────────

def test_d_deploy_persists_and_a_second_bot_on_the_account_is_a_409(world):
    result = deploy(world)
    bot = result["bot"]
    assert bot["environment"] == "DEMO" and bot["risk_level"] == "balanced" and bot["status"] == "deploying"
    stored = world.service.get_bot_instance(bot["id"])
    assert stored.user_id == "alice" and stored.broker_account_id == "acct" and stored.allocation_type == "risk_based"
    with world.db.connect() as c:
        consent = c.execute("SELECT user_id, consent_version FROM deployment_consents WHERE bot_instance_id=?", (bot["id"],)).fetchone()
    assert tuple(consent) == ("alice", C.CONSENT_VERSION)
    assert deploy(world)["idempotent_replay"] is True                                             # the same request: the same bot
    with pytest.raises(svc.DeploymentRefused) as refused:
        deploy(world, request_id="closing-0002")
    assert refused.value.status == 409 and refused.value.body["blockers"][0]["code"] == C.ACCOUNT_ALREADY_HAS_BOT
    with pytest.raises(svc.DeploymentRefused) as foreign:                                         # another user's account
        svc.deploy(world.db, "bob", request(request_id="closing-0003"), service=world.service, account_state_reader=balance)
    assert foreign.value.body["blockers"][0]["code"] == C.ACCOUNT_NOT_CONNECTED
    with pytest.raises(svc.DeploymentRefused) as unacknowledged:
        svc.deploy(world.db, "alice", request(request_id="closing-0004", risk_acknowledged=False), service=world.service,
                   account_state_reader=balance)
    assert C.RISK_NOT_ACKNOWLEDGED in [b["code"] for b in unacknowledged.value.body["blockers"]]
    with world.db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM bot_instances").fetchone()[0] == 1


# ── Scenario E (mechanics only) and F: the engine discovers the bot ─────────

def test_e_f_the_runtime_discovers_the_bot_and_the_owner_sees_the_engine_truth(world):
    runtime.sync(world.db)                                                                        # before the deployment
    assert state_of(world.db)["execution"]["evaluation_scope"] == "IDLE_NO_TRADABLE_BOT"
    bot_id = deploy(world)["bot"]["id"]
    runtime.sync(world.db)                                                                        # the next 30-second cycle
    state = state_of(world.db)
    assert state["status"] == "SYNCED" and state["execution"]["evaluation_scope"] == "FULL"
    assert state["execution"]["risk"]["bot_instance_ids"] == [bot_id]
    instance = world.service.get_bot_instance(bot_id)
    status = model.bot_status(world.db, instance, now_ms=state["observed_at"] + 1000)
    assert status["status"] == "running" and status["environment"] == "DEMO" and status["exchange"] == "BINANCE"
    assert status["engine"]["running"] is True and status["engine"]["stale"] is False
    # No strategy signal is not a fault, and it is said in words.
    assert status["eligibility"]["reason_code"] == "AWAITING_NATURAL_CATI_DECISION" and status["eligibility"]["severity"] == "waiting"
    assert status["eligibility"]["execution_permission"] == "WAITING_SIGNAL" and status["eligibility"]["eligible_to_enter"] is False
    assert status["eligibility"]["reason"] == "No qualifying CATI decision is open right now."
    assert model.positions(world.db, instance) == [] and model.trades(world.db, instance)["total"] == 0
    assert model.summary(world.db, instance)["equity"]["current"] == 1000.0
    world.client.place_order.assert_not_called()                                                  # nothing was sent anywhere
    assert order_submission_gate("live")["enabled"] is False


def test_f_another_customer_sees_none_of_it(world):
    bot_id = deploy(world)["bot"]["id"]
    runtime.sync(world.db)
    service = world.service
    owner = cati_account.bot_status(bot_id, user=ALICE, service=service, _perm="bot:read")
    assert owner["status"] == "running"
    calls = [
        lambda: cati_account.bot_status(bot_id, user=BOB, service=service, _perm="bot:read"),
        lambda: cati_account.bot_positions(bot_id, user=BOB, service=service, _perm="bot:read"),
        lambda: cati_account.bot_trades(bot_id, page=1, page_size=50, include_open=True, user=BOB, service=service, _perm="bot:read"),
        lambda: cati_account.bot_summary(bot_id, user=BOB, service=service, _perm="bot:read"),
        lambda: cati_account.bot_equity(bot_id, since=None, until=None, limit=100, user=BOB, service=service, _perm="bot:read"),
        lambda: cati_account.bot_events(bot_id, limit=50, user=BOB, service=service, _perm="bot:read"),
    ]
    for call in calls:
        with pytest.raises(HTTPException) as denied:
            call()
        assert denied.value.status_code == 404
    for control in (bot_routes.pause_bot_instance, bot_routes.stop_bot_instance, bot_routes.start_bot_instance):
        with pytest.raises(HTTPException) as denied:
            control(bot_id, user=BOB, service=service, _perm="bot:control")
        assert denied.value.status_code == 403
    assert world.service.get_bot_instance(bot_id).status == "active"                              # untouched


# ── Scenario H: pause, resume, stop, restart ────────────────────────────────

def test_h_pause_resume_and_stop_are_persisted_and_a_stopped_bot_stays_stopped_after_a_restart(world, tmp_path):
    bot_id = deploy(world)["bot"]["id"]
    service = world.service
    runtime.sync(world.db)
    assert state_of(world.db)["execution"]["evaluation_scope"] == "FULL"

    bot_routes.pause_bot_instance(bot_id, user=ALICE, service=service, _perm="bot:control")
    runtime.sync(world.db)
    paused = state_of(world.db)
    assert paused["execution"]["evaluation_scope"] == "IDLE_NO_TRADABLE_BOT" and paused["orders_read"] == "SKIPPED_IDLE_ACCOUNT"
    assert model.bot_status(world.db, service.get_bot_instance(bot_id))["status"] == "paused"
    # An open position of a paused bot is still maintained (account-wide read, no idle skip).
    world.client.position_risk.return_value = [{"symbol": "ADAUSDT", "positionAmt": "3", "positionSide": "BOTH", "entryPrice": "1"}]
    runtime.sync(world.db)
    assert state_of(world.db)["orders_read"] == "ACCOUNT_WIDE"
    world.client.position_risk.return_value = [{"symbol": "ADAUSDT", "positionAmt": "0", "positionSide": "BOTH"}]

    bot_routes.start_bot_instance(bot_id, user=ALICE, service=service, _perm="bot:control")
    runtime.sync(world.db)
    assert state_of(world.db)["execution"]["evaluation_scope"] == "FULL"

    bot_routes.stop_bot_instance(bot_id, user=ALICE, service=service, _perm="bot:control")
    runtime.sync(world.db)
    assert state_of(world.db)["execution"]["evaluation_scope"] == "IDLE_NO_TRADABLE_BOT"

    # Restart: a new process has no memo and a new connection to the same file.
    production_schema.forget()
    runtime._clients.clear()
    restarted = DB(world.db.path)
    runtime.initialize(restarted)
    runtime.sync(restarted)
    after = state_of(restarted)
    assert after["execution"]["evaluation_scope"] == "IDLE_NO_TRADABLE_BOT"
    stopped = BotInstanceService(db=restarted).get_bot_instance(bot_id)
    assert stopped.status == "stopped" and stopped.stopped_reason == "USER_STOP"
    assert model.bot_status(restarted, stopped)["status"] == "stopped"
    world.client.place_order.assert_not_called()

    kinds = [e["event_type"] for e in reversed(user_events.recent(restarted, user_id="alice", bot_id=bot_id))]
    assert sorted(kinds) == ["BOT_PAUSED", "BOT_RESUMED", "BOT_STOPPED"]
    assert user_events.recent(restarted, user_id="bob") == []
    # The account is free again: a new deployment is possible without touching the database.
    again = svc.deploy(restarted, "alice", request(request_id="closing-0009"), service=BotInstanceService(db=restarted),
                       account_state_reader=balance)
    assert again["bot"]["id"] != bot_id and again["idempotent_replay"] is False


def test_a_demo_bot_can_be_resumed_without_a_paid_plan_even_when_billing_is_enforced(world, monkeypatch):
    monkeypatch.setenv("BILLING_ENFORCED", "true")
    bot_id = deploy(world)["bot"]["id"]
    bot_routes.pause_bot_instance(bot_id, user=ALICE, service=world.service, _perm="bot:control")
    resumed = bot_routes.start_bot_instance(bot_id, user=ALICE, service=world.service, _perm="bot:control")
    assert resumed.status == "active"
    # The same bot on a LIVE account (or an unreadable one) still needs the entitlement.
    bot_routes.pause_bot_instance(bot_id, user=ALICE, service=world.service, _perm="bot:control")
    with world.db.connect() as c:
        c.execute("UPDATE broker_accounts SET environment='live' WHERE id='acct'")
    with pytest.raises(HTTPException) as denied:
        bot_routes.start_bot_instance(bot_id, user=ALICE, service=world.service, _perm="bot:control")
    assert denied.value.status_code == 403 and denied.value.detail["error_code"] == "LIVE_TRADING_NOT_IN_PLAN"



