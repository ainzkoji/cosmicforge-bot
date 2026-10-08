"""Plan (subscription) gates on the bot API.

The plan is read from the ``subscriptions`` table at request time. A user with
no subscription row is on the free plan: one bot, no live trading.

* bot-count limit on the only path that creates bots (auto-pilot deploy);
* live_trading entitlement on: deploy in live mode, PATCH that asks for
  ``mode=live``, start/resume of a live bot, and auto-pilot resume_all;
* paper/demo bots are never blocked by the live gate;
* refusals are 403 with a machine-readable ``error_code``.

The route handlers are called directly (as the neighbouring auto-pilot tests
do) against a migrated temporary database. No network.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.fixture(autouse=True)
def _billing_enforced(monkeypatch):
    """These tests assert the entitlement rules WITH billing enforced (Step 1.5:
    BILLING_ENFORCED defaults to false and then the gates refuse nothing)."""
    monkeypatch.setenv("BILLING_ENFORCED", "true")
from fastapi import BackgroundTasks, HTTPException

from app.api import auto_pilot, bot_instances
from app.api.auto_pilot import AutoPilotAllocation, DeployAutoPilotRequest
from app.core import plan_gate
from app.models.bot_instance_models import BotInstance
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

FREE_USER = "plan-free-user"
PRO_USER = "plan-pro-user"


# ── helpers ─────────────────────────────────────────────────────────────────

@pytest.fixture
def db(tmp_path):
    database = DB(str(tmp_path / "plan_gates.db"))
    migrate(database)
    return database


def give_plan(db: DB, user_id: str, plan_id: str = "plan_pro", *, status: str = "active",
              period_end: datetime | None = None) -> None:
    """Give ``user_id`` a subscription row (tests that need more than the free plan)."""
    now = datetime.now(timezone.utc)
    period_end = period_end or now + timedelta(days=30)
    with db.connect() as conn:
        conn.execute("DELETE FROM subscriptions WHERE user_id = ?", (user_id,))
        conn.execute(
            """
            INSERT INTO subscriptions (user_id, plan_id, status, provider_sub_id, current_period_end,
                                       cancel_at_period_end, created_at, updated_at, provider)
            VALUES (?, ?, ?, ?, ?, 0, ?, ?, 'stripe')
            """,
            (user_id, plan_id, status, f"sub_{user_id}", period_end.isoformat(), now.isoformat(), now.isoformat()),
        )


def add_bot_row(db: DB, bot_id: str, user_id: str, *, mode: str = "paper", status: str = "active") -> None:
    now = datetime.now(timezone.utc).isoformat()
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO bot_instances (id, user_id, broker_account_id, market_type, strategy_id, mode, status,
                                       created_at, updated_at)
            VALUES (?, ?, 'acc-1', 'CRYPTO', 'cati', ?, ?, ?, ?)
            """,
            (bot_id, user_id, mode, status, now, now),
        )


def make_bot(bot_id: str, user_id: str, *, mode: str = "paper", status: str = "paused") -> BotInstance:
    return BotInstance(id=bot_id, user_id=user_id, broker_account_id="acc-1", market_type="CRYPTO",
                       strategy_id="cati", strategy_version="1", risk_level="balanced", mode=mode, status=status)


def bot_service(db: DB, *bots: BotInstance) -> SimpleNamespace:
    """The slice of BotInstanceService the handlers use, on a real database."""
    by_id = {bot.id: bot for bot in bots}
    return SimpleNamespace(
        db=db,
        get_bot_instance=lambda instance_id: by_id.get(instance_id),
        get_user_bot_instances=lambda user_id, **_: [b for b in bots if b.user_id == user_id],
        start_bot_instance=Mock(side_effect=lambda instance_id: by_id[instance_id]),
        update_bot_instance=Mock(side_effect=lambda instance_id, payload: by_id[instance_id]),
        deploy_auto_pilot=Mock(return_value=[]),
        get_risk_profile_preset=lambda level: {},
    )


def deploy_request(mode: str = "paper", accounts: int = 1) -> DeployAutoPilotRequest:
    return DeployAutoPilotRequest(
        broker_account_ids=[f"acc-{i}" for i in range(accounts)], risk_mode="medium", execution_mode=mode,
        allocation=AutoPilotAllocation(total_capital_budget=500, trade_amount_per_position=100),
    )


def deploy(service, user_id: str, request: DeployAutoPilotRequest):
    return auto_pilot.deploy_auto_pilot(request=request, background_tasks=BackgroundTasks(),
                                        user={"id": user_id}, service=service, _perm=user_id)


def start(service, user_id: str, bot_id: str):
    return bot_instances.start_bot_instance(bot_id, user={"id": user_id}, service=service, _perm=user_id)


def assert_refused(exc_info, error_code: str) -> dict:
    assert exc_info.value.status_code == 403
    detail = exc_info.value.detail
    assert isinstance(detail, dict), detail
    assert detail["error_code"] == error_code
    assert isinstance(detail["message"], str) and detail["message"]
    return detail


# ── bot-count limit ─────────────────────────────────────────────────────────

def test_free_user_second_bot_is_refused(db):
    service = bot_service(db)

    deploy(service, FREE_USER, deploy_request())               # first bot fits the free plan
    assert service.deploy_auto_pilot.call_count == 1

    add_bot_row(db, "bot-free-1", FREE_USER)                   # ...and now exists
    with pytest.raises(HTTPException) as exc_info:
        deploy(service, FREE_USER, deploy_request())

    detail = assert_refused(exc_info, plan_gate.BOT_LIMIT_REACHED)
    assert (detail["plan_id"], detail["used"], detail["limit"]) == ("plan_free", 1, 1)
    assert service.deploy_auto_pilot.call_count == 1           # nothing was created


def test_free_user_cannot_deploy_two_bots_at_once(db):
    service = bot_service(db)
    with pytest.raises(HTTPException) as exc_info:
        deploy(service, FREE_USER, deploy_request(accounts=2))
    assert_refused(exc_info, plan_gate.BOT_LIMIT_REACHED)
    service.deploy_auto_pilot.assert_not_called()


def test_deleted_and_archived_bots_free_their_slot(db):
    service = bot_service(db)
    add_bot_row(db, "bot-gone-1", FREE_USER, status="deleted")
    add_bot_row(db, "bot-gone-2", FREE_USER, status="archived")
    deploy(service, FREE_USER, deploy_request())
    service.deploy_auto_pilot.assert_called_once()


def test_pro_user_can_run_several_bots_up_to_the_plan_limit(db):
    give_plan(db, PRO_USER, "plan_pro")
    service = bot_service(db)
    for index in range(4):
        add_bot_row(db, f"bot-pro-{index}", PRO_USER)

    deploy(service, PRO_USER, deploy_request())                # the fifth
    service.deploy_auto_pilot.assert_called_once()

    add_bot_row(db, "bot-pro-4", PRO_USER)
    with pytest.raises(HTTPException) as exc_info:
        deploy(service, PRO_USER, deploy_request())            # the sixth
    detail = assert_refused(exc_info, plan_gate.BOT_LIMIT_REACHED)
    assert (detail["plan_id"], detail["used"], detail["limit"]) == ("plan_pro", 5, 5)


# ── live trading: deploy ────────────────────────────────────────────────────

def test_free_user_live_deploy_is_refused(db):
    service = bot_service(db)
    with pytest.raises(HTTPException) as exc_info:
        deploy(service, FREE_USER, deploy_request(mode="live"))
    detail = assert_refused(exc_info, plan_gate.LIVE_TRADING_NOT_IN_PLAN)
    assert detail["plan_id"] == "plan_free"
    service.deploy_auto_pilot.assert_not_called()


def test_pro_user_live_deploy_is_allowed(db):
    give_plan(db, PRO_USER, "plan_pro")
    service = bot_service(db)
    deploy(service, PRO_USER, deploy_request(mode="live"))
    assert service.deploy_auto_pilot.call_args.kwargs["mode"] == "live"


# ── live trading: start / resume ────────────────────────────────────────────

def test_free_user_cannot_start_a_live_bot(db):
    service = bot_service(db, make_bot("bot-live", FREE_USER, mode="live"))
    with pytest.raises(HTTPException) as exc_info:
        start(service, FREE_USER, "bot-live")
    assert_refused(exc_info, plan_gate.LIVE_TRADING_NOT_IN_PLAN)
    service.start_bot_instance.assert_not_called()


def test_pro_user_can_start_a_live_bot(db):
    give_plan(db, PRO_USER, "plan_pro")
    service = bot_service(db, make_bot("bot-live", PRO_USER, mode="live"))
    assert start(service, PRO_USER, "bot-live").id == "bot-live"
    service.start_bot_instance.assert_called_once_with("bot-live")


@pytest.mark.parametrize("mode", ["paper", "demo", "PAPER"])
def test_paper_and_demo_bots_are_never_blocked_by_the_live_gate(db, mode):
    service = bot_service(db, make_bot("bot-sim", FREE_USER, mode=mode))
    assert start(service, FREE_USER, "bot-sim").id == "bot-sim"
    service.start_bot_instance.assert_called_once_with("bot-sim")


def test_paper_start_does_not_even_read_the_plan():
    service = bot_service(None, make_bot("bot-sim", FREE_USER, mode="paper"))   # no database at all
    assert start(service, FREE_USER, "bot-sim").id == "bot-sim"


def test_downgraded_user_loses_live_start(db):
    """An expired paid subscription is the free plan, whatever the token says."""
    give_plan(db, PRO_USER, "plan_pro", period_end=datetime.now(timezone.utc) - timedelta(days=60))
    service = bot_service(db, make_bot("bot-live", PRO_USER, mode="live"))
    with pytest.raises(HTTPException) as exc_info:
        bot_instances.start_bot_instance(
            "bot-live", user={"id": PRO_USER, "entitlements": {"live_trading": True}}, service=service, _perm="x")
    assert_refused(exc_info, plan_gate.LIVE_TRADING_NOT_IN_PLAN)
    service.start_bot_instance.assert_not_called()


def test_live_gate_does_not_replace_the_ownership_check(db):
    give_plan(db, PRO_USER, "plan_pro")
    service = bot_service(db, make_bot("bot-live", FREE_USER, mode="live"))
    with pytest.raises(HTTPException) as exc_info:
        start(service, PRO_USER, "bot-live")                   # someone else's bot
    assert exc_info.value.status_code == 403 and exc_info.value.detail == "Not authorized"
    service.start_bot_instance.assert_not_called()


# ── live trading: PATCH mode=live ───────────────────────────────────────────

def test_free_user_patch_to_live_is_refused_by_the_plan(db):
    service = bot_service(db, make_bot("bot-paper", FREE_USER, mode="paper"))
    with pytest.raises(HTTPException) as exc_info:
        bot_instances.update_bot_instance("bot-paper", {"mode": "live"}, user={"id": FREE_USER},
                                          service=service, _perm="x")
    assert_refused(exc_info, plan_gate.LIVE_TRADING_NOT_IN_PLAN)
    service.update_bot_instance.assert_not_called()


def test_pro_user_patch_to_live_is_not_refused_by_the_plan(db):
    """Not a plan refusal for an entitled user. (Execution mode is still not
    editable through this endpoint, which answers 422 for it.)"""
    give_plan(db, PRO_USER, "plan_pro")
    service = bot_service(db, make_bot("bot-paper", PRO_USER, mode="paper"))
    with pytest.raises(HTTPException) as exc_info:
        bot_instances.update_bot_instance("bot-paper", {"mode": "live"}, user={"id": PRO_USER},
                                          service=service, _perm="x")
    assert exc_info.value.status_code == 422
    service.update_bot_instance.assert_not_called()


# ── live trading: auto-pilot resume_all ─────────────────────────────────────

def test_resume_all_refuses_live_bots_for_a_free_user(db):
    service = bot_service(db, make_bot("bot-live", FREE_USER, mode="live"))
    with pytest.raises(HTTPException) as exc_info:
        auto_pilot.resume_auto_pilot(user={"id": FREE_USER}, service=service)
    detail = assert_refused(exc_info, plan_gate.LIVE_TRADING_NOT_IN_PLAN)
    assert detail["blocked_ids"] == ["bot-live"]
    service.start_bot_instance.assert_not_called()


def test_resume_all_still_resumes_paper_bots_and_reports_blocked_live_ones(db):
    service = bot_service(db, make_bot("bot-paper", FREE_USER, mode="paper"),
                          make_bot("bot-live", FREE_USER, mode="live"))
    result = auto_pilot.resume_auto_pilot(user={"id": FREE_USER}, service=service)

    assert result["ids"] == ["bot-paper"]
    assert result["blocked_ids"] == ["bot-live"]
    assert result["blocked"]["error_code"] == plan_gate.LIVE_TRADING_NOT_IN_PLAN
    service.start_bot_instance.assert_called_once_with("bot-paper")


def test_resume_all_resumes_live_bots_for_a_pro_user(db):
    give_plan(db, PRO_USER, "plan_pro")
    service = bot_service(db, make_bot("bot-paper", PRO_USER, mode="paper"),
                          make_bot("bot-live", PRO_USER, mode="live"))
    result = auto_pilot.resume_auto_pilot(user={"id": PRO_USER}, service=service)

    assert result == {"message": "Resumed 2 instances", "ids": ["bot-paper", "bot-live"]}
    assert service.start_bot_instance.call_count == 2
