"""Step 7.3 -- the optional deployment market scope, the TRADFI_NOT_AVAILABLE blocker, and the market
availability service. Crypto deployments are unaffected; a traditional-market scope is never enabled."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from app.core import broker_capability_gate, deployment_service as svc
from app.core.bot_instance_service import BotInstanceService
from app.markets import availability as AV
from shared_lib.deployment import contract as C
from shared_lib.deployment.contract import DeploymentRequest
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

NOW = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc).isoformat()
ALICE, CAROL = "alice", "carol"
REPO = Path(__file__).resolve().parents[4]


@pytest.fixture
def db(tmp_path, monkeypatch):
    database = DB(str(tmp_path / "scope.db"))
    migrate(database)
    with database.connect() as c:
        for uid in (ALICE, CAROL):
            c.execute("INSERT INTO users (id,email,hashed_password,status,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                      (uid, f"{uid}@example.test", "x", "active", NOW, NOW))
        c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,label,status,environment,created_at,"
                  "updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                  ("demo-a", ALICE, "binance", "crypto", "demo", "connected", "demo", NOW, NOW))
    monkeypatch.setattr(broker_capability_gate, "assert_broker_execution_capability", lambda *a, **kw: None)
    return database


def balance(db, account, now):
    return {"equity": Decimal("5000"), "available": Decimal("4800"), "wallet": Decimal("5000"), "source": "TEST",
            "observed_at": now}


def body(**over):
    base = dict(broker_account_id="demo-a", budget={"type": "fixed_amount", "value": "1000.00"}, risk_level="balanced",
                advanced={"max_position_usdt": "200.00", "daily_loss_limit_pct": "2", "symbols": []},
                risk_acknowledged=True, request_id="req-7777-0001")
    base.update(over)
    return base


# ── the contract ───────────────────────────────────────────────────────────

def test_an_old_payload_is_still_valid_and_crypto_only():
    r = DeploymentRequest.model_validate(body())
    assert r.market_scope == C.CRYPTO_ONLY and r.schema_version == "2026-10-08.v1" == C.DEPLOYMENT_SCHEMA_VERSION
    assert DeploymentRequest.model_validate(body(market_scope="CRYPTO_AND_TRADFI")).market_scope == C.CRYPTO_AND_TRADFI
    with pytest.raises(Exception):
        DeploymentRequest.model_validate(body(market_scope="EVERYTHING"))


def test_the_fingerprint_of_an_old_payload_is_unchanged():
    r = DeploymentRequest.model_validate(body())
    legacy = r.model_dump(mode="json")
    legacy.pop("risk_acknowledged")
    legacy.pop("market_scope")
    expected = hashlib.sha256(json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert r.fingerprint() == expected == DeploymentRequest.model_validate(body(market_scope="CRYPTO_ONLY")).fingerprint()
    assert DeploymentRequest.model_validate(body(market_scope="CRYPTO_AND_TRADFI")).fingerprint() != expected


# ── preview and deploy ─────────────────────────────────────────────────────

def test_a_crypto_preview_is_unchanged_and_names_its_scope(db):
    out = svc.preview(db, ALICE, DeploymentRequest.model_validate(body()), account_state_reader=balance)
    assert out["can_deploy"] is True and out["blockers"] == [] and out["market_scope"] == C.CRYPTO_ONLY


def test_a_tradfi_scope_is_refused_with_its_reasons(db):
    out = svc.preview(db, ALICE, DeploymentRequest.model_validate(body(market_scope="CRYPTO_AND_TRADFI")),
                      account_state_reader=balance)
    assert out["can_deploy"] is False
    blocker = next(b for b in out["blockers"] if b["code"] == C.TRADFI_NOT_AVAILABLE)
    codes = {r["code"] for r in blocker["reasons"]}
    assert {"NO_TRADFI_MARKET_AVAILABLE", "NO_EXECUTION_ADAPTER", "NO_CERTIFIED_FAMILY", "CALENDAR_UNVERIFIED",
            "ACTIVATION_STAGE_NONE"} <= codes
    assert blocker["message"] and blocker["action"] and blocker["market_scope"] == C.CRYPTO_AND_TRADFI


def test_a_tradfi_deploy_creates_nothing(db):
    with pytest.raises(svc.DeploymentRefused) as refused:
        svc.deploy(db, ALICE, DeploymentRequest.model_validate(body(market_scope="CRYPTO_AND_TRADFI")),
                   account_state_reader=balance)
    assert refused.value.status == 422
    assert C.TRADFI_NOT_AVAILABLE in {b["code"] for b in refused.value.body["blockers"]}
    assert BotInstanceService(db=db).get_user_bot_instances(ALICE) == []


def test_a_crypto_bot_reports_a_crypto_only_scope(db):
    out = svc.deploy(db, ALICE, DeploymentRequest.model_validate(body()), account_state_reader=balance)
    assert out["bot"]["market_scope"] == C.CRYPTO_ONLY and out["bot"]["market_type"] == "CRYPTO"


# ── the availability service ───────────────────────────────────────────────

def test_crypto_markets_are_available_and_tradfi_markets_are_not(db):
    view = AV.market_availability(db, ALICE)
    by_id = {m["market_id"]: m for m in view["markets"]}
    binance = by_id["CRYPTO_BINANCE_USDT_PERPETUAL"]
    assert binance["available"] is True and binance["reasons"] == [] and binance["connected_accounts"] == 1
    assert "DAILY_TREND" in binance["families"]
    tradfi = [m for m in view["markets"] if m["is_tradfi"]]
    assert tradfi and all(m["available"] is False for m in tradfi)
    for m in tradfi:
        codes = {r["code"] for r in m["reasons"]}
        assert {"NO_EXECUTION_ADAPTER", "NO_FAMILY_DECLARES_MARKET", "NO_CERTIFIED_FAMILY", "CALENDAR_UNVERIFIED",
                "ACTIVATION_STAGE_NONE", "NO_CONNECTED_ACCOUNT"} <= codes
        assert all(r["message"] for r in m["reasons"])
    assert view["scopes"]["CRYPTO_ONLY"]["available"] is True
    assert view["scopes"]["CRYPTO_AND_TRADFI"]["available"] is False
    assert view["scopes"]["CRYPTO_AND_TRADFI"]["reasons"][0]["code"] == "NO_TRADFI_MARKET_AVAILABLE"
    assert view["default_scope"] == C.CRYPTO_ONLY and view["live_trading"] == "DISABLED"


def test_a_user_without_accounts_is_told_so_without_crypto_being_marked_unavailable(db):
    view = AV.market_availability(db, CAROL)
    binance = next(m for m in view["markets"] if m["market_id"] == "CRYPTO_BINANCE_USDT_PERPETUAL")
    assert binance["available"] is True and [r["code"] for r in binance["reasons"]] == ["NO_CONNECTED_ACCOUNT"]


def test_even_a_certified_activated_tradfi_market_without_an_adapter_stays_unavailable(db):
    class Ledger:
        def highest_stage(self, market_id):
            return "DEMO"
    view = AV.market_availability(db, ALICE, certification_lookup=lambda f, m: {"status": "CERTIFIED"},
                                  ledger=Ledger())
    assert view["scopes"]["CRYPTO_AND_TRADFI"]["available"] is False


# ── the API surface ────────────────────────────────────────────────────────

def test_the_endpoint_is_registered_on_both_backends():
    from app.api.markets import router

    assert any(getattr(r, "path", "") == "/api/v1/markets/availability" and "GET" in r.methods for r in router.routes)
    bot_main = (REPO / "backends" / "bot-backend" / "app" / "main.py").read_text(encoding="utf-8")
    user_main = (REPO / "backends" / "user-backend" / "app" / "main.py").read_text(encoding="utf-8")
    proxy = (REPO / "backends" / "user-backend" / "app" / "api" / "markets_proxy.py").read_text(encoding="utf-8")
    assert "app.include_router(markets_router)" in bot_main
    assert "app.include_router(markets_proxy_router)" in user_main
    assert '"/api/v1/markets/availability"' in proxy and "get_current_active_user" in proxy
