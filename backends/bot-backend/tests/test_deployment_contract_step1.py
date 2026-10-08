"""Step 1.2 / 1.3 -- deployment preview and deploy on the shared contract, the
broker-derived environment, one bot per account, durable consent and
idempotency, through the real handlers on a migrated database."""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from app.api import auto_pilot
from app.core import broker_capability_gate, deployment_service as svc
from app.core.bot_instance_service import BotInstanceService
from shared_lib.deployment import contract as C
from shared_lib.deployment.contract import DeploymentRequest
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

NOW = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc).isoformat()
ALICE, BOB = "alice", "bob"


@pytest.fixture
def db(tmp_path, monkeypatch):
    database = DB(str(tmp_path / "deploy.db"))
    migrate(database)
    with database.connect() as c:
        for uid in (ALICE, BOB):
            c.execute("INSERT INTO users (id,email,hashed_password,status,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                      (uid, f"{uid}@example.test", "x", "active", NOW, NOW))
        for acct, uid, env, status in (("demo-a", ALICE, "demo", "connected"), ("live-a", ALICE, "live", "connected"),
                                       ("demo-b", BOB, "demo", "connected"), ("draft-a", ALICE, "demo", "draft")):
            c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,label,status,environment,created_at,updated_at) "
                      "VALUES (?,?,?,?,?,?,?,?,?)", (acct, uid, "binance", "crypto", f"{acct} label", status, env, NOW, NOW))
    # The capability gate needs permission evidence from a validated credential; it is exercised by its own suite.
    monkeypatch.setattr(broker_capability_gate, "assert_broker_execution_capability", lambda *a, **kw: None)
    return database


def balance(equity="5000", available="4800"):
    return lambda db, account, now: {"equity": Decimal(equity), "available": Decimal(available), "wallet": Decimal(equity),
                                     "source": "TEST", "observed_at": now}


def request(**over):
    base = dict(broker_account_id="demo-a", budget={"type": "fixed_amount", "value": "1000.00"}, risk_level="balanced",
                advanced={"max_position_usdt": "200.00", "daily_loss_limit_pct": "2", "symbols": []},
                risk_acknowledged=True, request_id="req-0000-0001")
    base.update(over)
    return DeploymentRequest.model_validate(base)


def service(db):
    return BotInstanceService(db=db)


# ── the contract itself ─────────────────────────────────────────────────────

def test_the_contract_is_versioned_decimal_and_strict():
    r = request()
    assert r.schema_version == C.DEPLOYMENT_SCHEMA_VERSION and r.budget.value == "1000.00"
    with pytest.raises(Exception):
        request(schema_version="1999.v0")
    with pytest.raises(Exception):
        request(environment="demo")                           # the client cannot choose the environment
    with pytest.raises(Exception):
        request(budget={"type": "fixed_amount", "value": "-5"})
    with pytest.raises(Exception):
        request(budget={"type": "fixed_amount", "value": "1e999"})
    with pytest.raises(Exception):
        request(advanced={"daily_loss_limit_pct": "90"})
    with pytest.raises(Exception):
        request(request_id="short")
    assert C.is_new_contract({"broker_account_id": "a", "budget": {}, "risk_level": "balanced"})
    assert not C.is_new_contract({"broker_account_ids": ["a"], "risk_mode": "medium"})
    assert request().fingerprint() == request(risk_acknowledged=False).fingerprint()   # acknowledgement is not part of the identity
    assert request().fingerprint() != request(risk_level="aggressive").fingerprint()


# ── preview ─────────────────────────────────────────────────────────────────

def test_preview_shows_the_money_view_from_the_shared_library_and_can_deploy(db):
    out = svc.preview(db, ALICE, request(), account_state_reader=balance())
    assert out["can_deploy"] is True and out["blockers"] == [] and out["requirements"] == []
    assert out["exchange"] == "BINANCE" and out["environment"] == "DEMO"
    assert out["account"] == {"equity": "5000", "available_balance": "4800", "wallet": "5000", "source": "TEST",
                              "observed_at": out["evaluated_at"]}
    money = out["money"]
    assert money["risk_level"] == "balanced" and money["risk_profile_version"] == "2026-10-08.v1"
    assert money["per_trade_risk_pct"] == "0.50" and money["effective_per_trade_risk_pct"] == "0.40"
    assert money["risk_per_trade"] == "4.00" and money["approved_profile_risk_per_trade"] == "5.00"   # the ceiling conflict, visible
    assert money["max_open_risk"] == "20.00" and money["daily_loss_pause"] == "20.00"
    assert money["drawdown_reduce_threshold"] == "80.00" and money["drawdown_stop_threshold"] == "150.00"
    assert money["leverage_ceiling"] == 2 and money["max_positions"] == 6
    assert out["ceiling_conflict"]["ceiling_applied"] is True
    assert out["minimum_deployable_budget"] == "187.50"        # 5 USDT minimum at the 15 % system-maximum stop and 0.4 %
    assert out["typical_position"] is None and out["typical_position_note"] == "STOP_DISTANCE_ASSUMPTION_REQUIRED"
    assert "exceed" in out["disclaimer"] and out["consent"]["version"] == C.CONSENT_VERSION


def test_preview_blockers_are_stable_codes_with_explanations(db):
    small = svc.preview(db, ALICE, request(budget={"type": "fixed_amount", "value": "50"}), account_state_reader=balance())
    assert [b["code"] for b in small["blockers"]] == [C.BUDGET_TOO_SMALL_FOR_LEVEL] and not small["can_deploy"]
    assert small["blockers"][0]["message"] and small["blockers"][0]["action"]
    rich = svc.preview(db, ALICE, request(budget={"type": "fixed_amount", "value": "9000"}), account_state_reader=balance())
    assert [b["code"] for b in rich["blockers"]] == [C.BUDGET_EXCEEDS_BALANCE]
    live = svc.preview(db, ALICE, request(broker_account_id="live-a"), account_state_reader=balance())
    assert C.LIVE_NOT_AVAILABLE in [b["code"] for b in live["blockers"]] and live["environment"] == "LIVE"
    draft = svc.preview(db, ALICE, request(broker_account_id="draft-a"), account_state_reader=balance())
    assert [b["code"] for b in draft["blockers"]] == [C.ACCOUNT_NOT_CONNECTED]
    foreign = svc.preview(db, ALICE, request(broker_account_id="demo-b"), account_state_reader=balance())
    assert [b["code"] for b in foreign["blockers"]] == [C.ACCOUNT_NOT_CONNECTED]   # another user's account: not revealed
    unread = svc.preview(db, ALICE, request(), account_state_reader=Mock(side_effect=TimeoutError("exchange")))
    assert C.ACCOUNT_BALANCE_UNAVAILABLE in [b["code"] for b in unread["blockers"]]
    unacknowledged = svc.preview(db, ALICE, request(risk_acknowledged=False), account_state_reader=balance())
    assert unacknowledged["requirements"] == [C.RISK_NOT_ACKNOWLEDGED] and unacknowledged["can_deploy"] is True
    loose = svc.preview(db, ALICE, request(advanced={"daily_loss_limit_pct": "5"}), account_state_reader=balance())
    assert [b["code"] for b in loose["blockers"]] == [C.INVALID_ADVANCED_SETTINGS]   # may only tighten the profile pause


def test_percentage_budget_uses_the_server_side_equity(db):
    out = svc.preview(db, ALICE, request(budget={"type": "percent_balance", "value": "20"}), account_state_reader=balance())
    assert out["effective_budget"] == "1000" and out["money"]["risk_per_trade"] == "4.00"


def test_preview_prefers_the_engine_snapshot_and_never_the_client(db):
    with db.connect() as c:
        c.execute("CREATE TABLE IF NOT EXISTS cati_production_state (account_id TEXT PRIMARY KEY, user_id TEXT, observed_at INTEGER NOT NULL, document TEXT NOT NULL)")
        c.execute("INSERT INTO cati_production_state VALUES (?,?,?,?)", ("demo-a", ALICE, 1_000_000,
                  json.dumps({"account": {"totalMarginBalance": "2500", "totalWalletBalance": "2500", "availableBalance": "2400"}})))
    state = svc.persisted_account_state(db, "demo-a", 1_000_000 + 60_000)
    assert state["equity"] == Decimal("2500") and state["source"] == "ENGINE_SNAPSHOT"
    assert svc.persisted_account_state(db, "demo-a", 1_000_000 + 600_000) is None   # stale: not used


# ── deploy ──────────────────────────────────────────────────────────────────

def test_deploy_persists_ownership_configuration_environment_and_consent(db):
    out = svc.deploy(db, ALICE, request(), account_state_reader=balance(), now_ms=1_000_000)
    bot = out["bot"]
    assert out["idempotent_replay"] is False and out["consent"]["version"] == C.CONSENT_VERSION
    assert bot["exchange"] == "BINANCE" and bot["environment"] == "DEMO" and bot["broker_account_id"] == "demo-a"
    assert bot["risk_level"] == "balanced" and bot["risk_profile_version"] == "2026-10-08.v1"
    assert bot["budget"] == {"type": "fixed_amount", "value": "1000.0"} and bot["money"]["risk_per_trade"] == "4.00"
    assert bot["status"] == "deploying" and bot["stopped_reason"] is None and bot["max_position_usdt"] == 200.0
    assert set(bot) >= {"id", "name", "exchange", "environment", "broker_account_id", "risk_level", "budget", "money",
                        "status", "stopped_reason", "created_at"}
    stored = service(db).get_bot_instance(bot["id"])
    assert stored.user_id == ALICE and stored.allocation_type == "risk_based" and stored.allocation_value == 0.5
    assert stored.capital_allocation == 1000.0 and stored.capital_allocation_type == "fixed_amount"
    assert not hasattr(stored, "environment")                 # always read from the broker account (bot["environment"] above)
    assert stored.mode == "live" and stored.status == "active"
    assert stored.daily_loss_limit_pct == 0.02 and stored.max_position_usdt == 200.0
    assert stored.deploy_request_id == "req-0000-0001" and stored.risk_acknowledged_at
    with db.connect() as c:
        consent = dict(c.execute("SELECT * FROM deployment_consents WHERE bot_instance_id=?", (bot["id"],)).fetchone())
    assert (consent["user_id"], consent["risk_level"], consent["consent_version"], consent["request_id"]) == \
        (ALICE, "balanced", C.CONSENT_VERSION, "req-0000-0001")
    assert json.loads(consent["preview_json"])["money"]["risk_per_trade"] == "4.00"


def test_deploy_revalidates_the_current_account_state_not_the_preview(db):
    assert svc.preview(db, ALICE, request(), account_state_reader=balance())["can_deploy"]
    with pytest.raises(svc.DeploymentRefused) as refused:          # the balance dropped after the preview
        svc.deploy(db, ALICE, request(), account_state_reader=balance(equity="500", available="500"))
    assert refused.value.status == 422 and [b["code"] for b in refused.value.body["blockers"]] == [C.BUDGET_EXCEEDS_BALANCE]
    assert service(db).get_user_bot_instances(ALICE) == []


def test_deploy_requires_the_acknowledgement_and_a_demo_account(db):
    with pytest.raises(svc.DeploymentRefused) as refused:
        svc.deploy(db, ALICE, request(risk_acknowledged=False), account_state_reader=balance())
    assert [b["code"] for b in refused.value.body["blockers"]] == [C.RISK_NOT_ACKNOWLEDGED]
    with pytest.raises(svc.DeploymentRefused) as live:
        svc.deploy(db, ALICE, request(broker_account_id="live-a"), account_state_reader=balance())
    assert C.LIVE_NOT_AVAILABLE in [b["code"] for b in live.value.body["blockers"]]
    with pytest.raises(svc.DeploymentRefused) as foreign:
        svc.deploy(db, ALICE, request(broker_account_id="demo-b"), account_state_reader=balance())
    assert [b["code"] for b in foreign.value.body["blockers"]] == [C.ACCOUNT_NOT_CONNECTED]
    assert service(db).get_user_bot_instances(ALICE) == [] and service(db).get_user_bot_instances(BOB) == []


def test_a_second_deployment_on_an_occupied_account_is_a_409(db):
    first = svc.deploy(db, ALICE, request(), account_state_reader=balance())
    with pytest.raises(svc.DeploymentRefused) as refused:
        svc.deploy(db, ALICE, request(request_id="req-0000-0002"), account_state_reader=balance())
    assert refused.value.status == 409 and refused.value.body["blockers"][0]["code"] == C.ACCOUNT_ALREADY_HAS_BOT
    assert refused.value.body["blockers"][0]["bot_id"] == first["bot"]["id"]
    service(db).pause_bot_instance(first["bot"]["id"])            # paused still occupies the account
    with pytest.raises(svc.DeploymentRefused):
        svc.deploy(db, ALICE, request(request_id="req-0000-0003"), account_state_reader=balance())
    service(db).stop_bot_instance(first["bot"]["id"])             # stopped frees it
    second = svc.deploy(db, ALICE, request(request_id="req-0000-0004"), account_state_reader=balance())
    assert second["bot"]["id"] != first["bot"]["id"]


def test_the_same_request_id_resolves_to_the_same_deployment(db):
    first = svc.deploy(db, ALICE, request(), account_state_reader=balance())
    again = svc.deploy(db, ALICE, request(), account_state_reader=balance())
    assert again["idempotent_replay"] is True and again["bot"]["id"] == first["bot"]["id"]
    assert len(service(db).get_user_bot_instances(ALICE)) == 1
    with pytest.raises(svc.DeploymentRefused) as reused:          # same identity, different content
        svc.deploy(db, ALICE, request(risk_level="aggressive"), account_state_reader=balance())
    assert reused.value.status == 409 and reused.value.body["blockers"][0]["code"] == C.REQUEST_ID_REUSED
    # Request identities are per user: Bob reusing the same string gets HIS bot on HIS account, never Alice's.
    bob = svc.deploy(db, BOB, request(broker_account_id="demo-b"), account_state_reader=balance())
    assert bob["idempotent_replay"] is False and bob["bot"]["broker_account_id"] == "demo-b"
    assert [b.user_id for b in service(db).get_user_bot_instances(BOB)] == [BOB]
    assert [b.id for b in service(db).get_user_bot_instances(ALICE)] == [first["bot"]["id"]]


def test_concurrent_deployments_create_exactly_one_bot(db):
    results, errors = [], []

    def attempt(n):
        try:
            results.append(svc.deploy(db, ALICE, request(request_id=f"req-concurrent-{n}"), account_state_reader=balance()))
        except svc.DeploymentRefused as exc:
            errors.append(exc)
    threads = [threading.Thread(target=attempt, args=(n,)) for n in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(results) == 1 and len(errors) == 5
    assert all(e.status == 409 and e.body["blockers"][0]["code"] == C.ACCOUNT_ALREADY_HAS_BOT for e in errors)
    bots = service(db).get_user_bot_instances(ALICE)
    assert [b.status for b in bots] == ["active"]


def test_the_one_bot_per_account_invariant_is_the_deploy_transaction_not_an_index(db):
    """Accounts with several active bots exist in the engine's model (they are
    blocked as ACCOUNT_EXECUTION_OWNER_AMBIGUOUS); the deployment path is what
    refuses to CREATE a second occupying bot, under the write lock."""
    with db.connect() as c:
        assert not c.execute("SELECT 1 FROM sqlite_master WHERE name='ux_bot_instances_one_occupying_per_account'").fetchone()
    first = svc.deploy(db, ALICE, request(), account_state_reader=balance())
    with pytest.raises(svc.DeploymentRefused) as refused:
        svc.deploy(db, ALICE, request(request_id="req-0000-0077"), account_state_reader=balance())
    assert refused.value.status == 409 and refused.value.body["blockers"][0]["code"] == C.ACCOUNT_ALREADY_HAS_BOT
    assert [b.id for b in service(db).get_user_bot_instances(ALICE)] == [first["bot"]["id"]]


def test_the_handlers_translate_refusals_and_replays(db):
    svc_obj = service(db)
    user = {"id": ALICE}
    import app.core.deployment_service as module
    reader = balance()
    original_evaluate = module.evaluate
    module_default = module.default_account_state_reader
    module.default_account_state_reader = reader
    try:
        created = auto_pilot.create_deployment(request(), user=user, service=svc_obj, _perm=ALICE)
        assert created["bot"]["status"] == "deploying"
        replay = auto_pilot.create_deployment(request(), user=user, service=svc_obj, _perm=ALICE)
        assert replay.status_code == 200 and json.loads(replay.body)["idempotent_replay"] is True
        with pytest.raises(HTTPException) as conflict:
            auto_pilot.create_deployment(request(request_id="req-0000-0009"), user=user, service=svc_obj, _perm=ALICE)
        assert conflict.value.status_code == 409 and conflict.value.detail["blockers"][0]["code"] == C.ACCOUNT_ALREADY_HAS_BOT
        preview = auto_pilot.preview_deployment(request(request_id="req-0000-0010"), user=user, service=svc_obj, _perm=ALICE)
        assert preview["blockers"][0]["code"] == C.ACCOUNT_ALREADY_HAS_BOT
        bots = auto_pilot.list_bots(user=user, service=svc_obj, _perm=ALICE)
        assert [b["environment"] for b in bots] == ["DEMO"] and bots[0]["status"] == "deploying"
        assert auto_pilot.list_bots(user={"id": BOB}, service=svc_obj, _perm=BOB) == []
    finally:
        module.default_account_state_reader = module_default
        module.evaluate = original_evaluate


# ── the environment comes from the broker account (Step 1.3) ────────────────

def test_the_live_readiness_gate_does_not_block_a_demo_account_but_still_guards_live(db, monkeypatch):
    from app.models.bot_instance_models import CreateBotInstanceRequest
    import app.core.bot_instance_service as bis
    calls = []
    import app.product_safety.readiness_gate as gate
    monkeypatch.setattr(gate, "assert_user_capital_activation_allowed",
                        lambda **kw: calls.append(kw) or (_ for _ in ()).throw(gate.UserCapitalReadinessError({"status": "REJECTED", "reason": "USER_CAPITAL_READINESS_NOT_MET"})))

    def req(account):
        return CreateBotInstanceRequest(user_id=ALICE, broker_account_id=account, market_type="CRYPTO", strategy_id="cati",
                                        strategy_version="1", risk_level="balanced", symbols=[], timeframes=["15m"],
                                        allocation_type="fixed_amount", allocation_value=100.0, mode="live",
                                        capital_allocation=1000.0, universe_mode="BROKER")
    demo = service(db).create_bot_instance(req("demo-a"))          # a DEMO account: broker execution, no user capital
    assert demo.status == "active" and calls == []
    with pytest.raises(ValueError, match="USER_CAPITAL_READINESS_NOT_MET"):
        service(db).create_bot_instance(req("live-a"))              # a LIVE account keeps the readiness gate
    assert len(calls) == 1


def test_bot_status_maps_the_engine_state_machine(db):
    out = svc.deploy(db, ALICE, request(), account_state_reader=balance())
    bot_id = out["bot"]["id"]
    with db.connect() as c:                                          # the engine saw the account after creation
        c.execute("CREATE TABLE IF NOT EXISTS cati_production_state (account_id TEXT PRIMARY KEY, user_id TEXT, observed_at INTEGER NOT NULL, document TEXT NOT NULL)")
        c.execute("INSERT OR REPLACE INTO cati_production_state VALUES ('demo-a','alice',?, '{}')", (int(datetime.now(timezone.utc).timestamp() * 1000) + 60_000,))
    assert svc.bot_payload(db, service(db).get_bot_instance(bot_id))["status"] == "running"
    service(db).pause_bot_instance(bot_id)
    assert svc.bot_payload(db, service(db).get_bot_instance(bot_id))["status"] == "paused"
    service(db).stop_bot_instance(bot_id)
    assert svc.bot_payload(db, service(db).get_bot_instance(bot_id))["status"] == "stopped"


def test_legacy_deploy_contract_is_unchanged(db, monkeypatch):
    from app.api.auto_pilot import DeployAutoPilotRequest
    legacy = DeployAutoPilotRequest(broker_account_ids=["demo-a"], risk_mode="medium",
                                    allocation={"total_capital_budget": 500, "trade_amount_per_position": 120,
                                                "allocation_type": "fixed_amount"}, execution_mode="paper")
    assert legacy.execution_mode == "paper" and legacy.allocation.trade_amount_per_position == 120
    monkeypatch.setenv("BILLING_ENFORCED", "false")
    out = auto_pilot.deploy_auto_pilot(legacy, background_tasks=SimpleNamespace(), user={"id": ALICE}, service=service(db), _perm=ALICE)
    [bot] = out.instances
    assert bot.allocation_type == "fixed_amount" and bot.allocation_value == 120.0 and bot.risk_level == "balanced"
    assert bot.mode == "paper" and bot.risk_profile_version is None       # legacy bots are not migrated into the new model
