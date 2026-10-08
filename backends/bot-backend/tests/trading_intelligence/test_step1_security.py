"""Step 1, Section 20 -- the new surface keeps the existing security rules.

Authentication and admin authority are verified on the routes Step 1 added;
secrets never appear in what the portal receives; the deployment request
cannot carry anything the server must decide (environment, owner, consent
time, leverage). Tenant isolation of the same routes is exercised in
``test_step1_closing_synthetic.py``; SSE ownership in ``test_user_events.py``;
operator grants and the emergency proxy in the user-backend suites;
withdrawal permission, mandatory keys and 2FA in their existing suites.
"""
import json

import pytest
from pydantic import ValidationError
from test_production_execution import live, profile
from test_production_demo_execution import demo, demo_profile
from test_production_loop import accounts_db
from test_step1_closing_synthetic import ALICE, balance, deploy, request, world
from app.api import auto_pilot, cati_account
from app.core import cati_read_model as model
from app.core import deployment_service as svc
from app.core.auth import get_current_active_user, require_admin
from app.observability import user_events
from app.trading_intelligence.integration import production_runtime as runtime
from shared_lib.deployment.contract import DeploymentRequest

__all__ = ["live", "profile", "demo", "demo_profile", "accounts_db", "world"]

SENTINEL = "SENTINEL-CIPHERTEXT-9f3a"


def dependency_calls(route):
    seen, stack = set(), [route.dependant]
    while stack:
        node = stack.pop()
        if node.call is not None:
            seen.add(node.call)
        stack.extend(node.dependencies)
    return seen


def test_every_customer_route_added_by_step_one_requires_an_authenticated_user():
    routes = [r for r in cati_account.router.routes] + [
        r for r in auto_pilot.router.routes if r.path.rstrip("/").split("/")[-1] in ("preview", "deployments", "bots")]
    assert len(routes) >= 9
    for route in routes:
        assert get_current_active_user in dependency_calls(route), route.path


def test_every_admin_route_added_by_step_one_requires_admin_authority():
    assert len(cati_account.admin_router.routes) >= 5
    for route in cati_account.admin_router.routes:
        calls = dependency_calls(route)
        assert require_admin in calls, route.path
        assert get_current_active_user not in calls, route.path      # never reachable with a customer token alone


@pytest.mark.parametrize("extra", [
    {"environment": "live"}, {"user_id": "bob"}, {"risk_acknowledged_at": "2020-01-01T00:00:00Z"}, {"leverage": 20},
    {"mode": "live"}, {"execution_mode": "live"}, {"risk_profile_version": "1999.v0"}, {"per_trade_risk_pct": "5"},
    {"api_key": "x"}, {"broker_environment": "mainnet"},
])
def test_the_request_cannot_carry_what_the_server_decides(extra):
    base = dict(broker_account_id="acct", budget={"type": "fixed_amount", "value": "1000"}, risk_level="balanced",
                risk_acknowledged=True, request_id="sec-0001")
    DeploymentRequest.model_validate(base)
    with pytest.raises(ValidationError):
        DeploymentRequest.model_validate({**base, **extra})
    with pytest.raises(ValidationError):
        DeploymentRequest.model_validate({**base, "advanced": {"symbols": [], **extra}})
    with pytest.raises(ValidationError):
        DeploymentRequest.model_validate({**base, "budget": {"type": "fixed_amount", "value": "1000", **extra}})


def test_consent_is_recorded_by_the_server_only_for_an_acknowledged_deploy(world):
    with pytest.raises(svc.DeploymentRefused):
        svc.deploy(world.db, "alice", request(risk_acknowledged=False), service=world.service, account_state_reader=balance)
    with world.db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM deployment_consents").fetchone()[0] == 0
        assert c.execute("SELECT COUNT(*) FROM bot_instances").fetchone()[0] == 0
    bot = deploy(world)["bot"]
    with world.db.connect() as c:
        consent = dict(c.execute("SELECT * FROM deployment_consents WHERE bot_instance_id=?", (bot["id"],)).fetchone())
    stored = world.service.get_bot_instance(bot["id"])
    assert consent["user_id"] == "alice" and consent["request_id"] == "closing-0001"
    assert consent["acknowledged_at"] == stored.risk_acknowledged_at and stored.risk_acknowledged_at[:4] >= "2026"


def test_no_credential_reaches_any_portal_response(world):
    with world.db.connect() as c:
        columns = {r[1] for r in c.execute("PRAGMA table_info(broker_credentials)")}
        row = {"account_id": "acct", "encrypted_blob": SENTINEL, "encrypted_data": SENTINEL, "ciphertext": SENTINEL,
               "key_metadata": json.dumps({"fingerprint": SENTINEL}), "created_at": "2026-10-08", "updated_at": "2026-10-08",
               "nonce": SENTINEL, "iv": SENTINEL}
        row = {k: v for k, v in row.items() if k in columns}
        assert len(row) >= 2, columns                                # the fixture really stored something secret
        c.execute(f"INSERT OR REPLACE INTO broker_credentials ({','.join(row)}) VALUES ({','.join('?' for _ in row)})", tuple(row.values()))
        c.execute("UPDATE broker_accounts SET masked_key=? WHERE id='acct'", (SENTINEL,))
    preview = svc.preview(world.db, "alice", request(risk_acknowledged=False), account_state_reader=balance)
    deployed = deploy(world)
    runtime.sync(world.db)
    instance = world.service.get_bot_instance(deployed["bot"]["id"])
    service = world.service
    responses = {
        "preview": preview, "deploy": deployed, "bots": [svc.bot_payload(world.db, instance)],
        "status": cati_account.bot_status(instance.id, user=ALICE, service=service, _perm="bot:read"),
        "positions": cati_account.bot_positions(instance.id, user=ALICE, service=service, _perm="bot:read"),
        "trades": cati_account.bot_trades(instance.id, page=1, page_size=50, include_open=True, user=ALICE, service=service, _perm="bot:read"),
        "summary": cati_account.bot_summary(instance.id, user=ALICE, service=service, _perm="bot:read"),
        "equity": cati_account.bot_equity(instance.id, since=None, until=None, limit=100, user=ALICE, service=service, _perm="bot:read"),
        "events": cati_account.bot_events(instance.id, limit=50, user=ALICE, service=service, _perm="bot:read"),
        "model_status": model.bot_status(world.db, instance),
        "user_events": user_events.recent(world.db, user_id="alice"),
    }
    for name, body in responses.items():
        text = json.dumps(body, default=str)
        assert SENTINEL not in text, name
        lowered = text.lower()
        for forbidden in ('"api_key"', '"api_secret"', '"secret"', '"password"', '"encrypted_data"', '"key_fingerprint"',
                          '"credential_version"', '"masked_key"', '"encrypted_blob"'):
            assert forbidden not in lowered, (name, forbidden)
