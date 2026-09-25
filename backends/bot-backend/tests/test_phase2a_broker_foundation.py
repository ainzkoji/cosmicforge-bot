"""Phase 2A: broker construction, capability gate, legacy-route lockdown,
credential encryption, redaction and the resolver-backed snapshot path."""
from __future__ import annotations

import json
import logging
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from shared_lib.broker import BrokerResolverError, build_client_from_auth
from shared_lib.broker.capabilities import (
    REASON_EXECUTION_CAPABILITY_INCOMPLETE,
    WITHDRAWALS_SUPPORTED_BY_PLATFORM,
    Capability,
    CapabilityState,
    declared_profile,
    execution_readiness,
)
from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url
from shared_lib.broker.resolver import BrokerAuth
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate


def _auth(broker: str, env: BrokerEnvironment, **extra) -> BrokerAuth:
    return BrokerAuth(account_id="acc", user_id="u", broker_type=broker, environment=env,
                      base_url=resolve_base_url(broker, env), api_key="key-abcdef", api_secret="sec-123456",
                      credential_version=1, key_fingerprint="...cdef", extra=extra)


# ── 2A: client construction ────────────────────────────────────────────────

@pytest.mark.parametrize("broker,cls", [("binance", "BinanceFuturesClient"), ("bybit", "BybitClient"),
                                        ("bingx", "BingXClient")])
@pytest.mark.parametrize("env", [BrokerEnvironment.LIVE, BrokerEnvironment.DEMO])
def test_canonical_factory_builds_every_crypto_broker(broker, cls, env):
    client = build_client_from_auth(_auth(broker, env))
    assert type(client).__name__ == cls
    assert client.base_url == resolve_base_url(broker, env)


def test_environment_url_table():
    assert resolve_base_url("binance", BrokerEnvironment.DEMO) == "https://demo-fapi.binance.com"
    assert resolve_base_url("bybit", BrokerEnvironment.LIVE) == "https://api.bybit.com"
    assert resolve_base_url("bingx", BrokerEnvironment.DEMO) == "https://open-api-vst.bingx.com"


def test_bingx_testnet_flag_is_honoured():
    from app.exchange.bingx.client import BingXClient

    assert BingXClient("k", "s", testnet=True).base_url == "https://open-api-vst.bingx.com"
    assert BingXClient("k", "s").base_url == "https://open-api.bingx.com"


def test_unknown_broker_fails_closed():
    with pytest.raises(BrokerResolverError) as exc:
        build_client_from_auth(_auth("binance", BrokerEnvironment.LIVE).__class__(
            account_id="a", user_id="u", broker_type="kraken", environment=BrokerEnvironment.LIVE,
            base_url="https://x", api_key="k", api_secret="s", credential_version=1, key_fingerprint="****"))
    assert exc.value.reason_code == BrokerResolverError.REASON_AUTH_FAILED


# ── 2A: capability profiles ────────────────────────────────────────────────

def test_withdrawals_are_never_a_platform_capability():
    assert WITHDRAWALS_SUPPORTED_BY_PLATFORM is False
    for broker in ("binance", "bybit", "bingx", "kraken"):
        prof = declared_profile(broker)
        assert prof.withdrawals_supported_by_platform is False
        assert prof.to_dict()["withdrawals_supported_by_platform"] is False
    assert not any("withdraw" in c.value for c in Capability)


def test_binance_executes_live_and_demo():
    assert execution_readiness("binance", "live").permitted
    assert execution_readiness("binance", "demo").permitted


def test_unknown_broker_has_no_capabilities():
    prof = declared_profile("kraken")
    assert all(prof.state(c) == CapabilityState.UNSUPPORTED for c in Capability)
    assert not execution_readiness("kraken", "demo").permitted


def test_account_permissions_narrow_but_never_widen():
    prof = declared_profile("binance")
    narrowed = prof.for_account({"TRADE": False, "READ_ACCOUNT": True})
    assert narrowed.state(Capability.ORDERS) == CapabilityState.ACCOUNT_RESTRICTED
    assert not narrowed.execution_readiness("live").permitted
    # INTERNAL_TRANSFER needs POSITIVE evidence; unknown restricts it.
    assert prof.for_account({}).state(Capability.INTERNAL_TRANSFER) == CapabilityState.ACCOUNT_RESTRICTED
    assert prof.for_account({"INTERNAL_TRANSFER": True}).state(Capability.INTERNAL_TRANSFER) == CapabilityState.UNVALIDATED
    # an UNSUPPORTED capability stays unsupported whatever the key says
    assert prof.for_account({"TRADE": True}).state(Capability.SPOT_TRADING) == CapabilityState.UNSUPPORTED


# ── 2A: bot-start gate (DB-backed) ─────────────────────────────────────────

@pytest.fixture
def migrated_db(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "phase2.db").as_posix())
    db = DB()
    migrate(db)
    return db


def _account(db, acc_id, user_id, broker, env="demo", status="connected"):
    now = "2026-09-25T00:00:00Z"
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO broker_accounts (id, user_id, broker_id, market_type, label, status, environment, "
            "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (acc_id, user_id, broker, "crypto", "t", status, env, now, now))


def test_gate_refuses_unknown_brokers_and_foreign_accounts(migrated_db):
    from app.core.broker_capability_gate import (
        REASON_ACCOUNT_NOT_OWNED, BrokerCapabilityGateError, assert_broker_execution_capability)

    _account(migrated_db, "acc_kraken", "alice", "kraken")
    _account(migrated_db, "acc_bin", "alice", "binance")
    with pytest.raises(BrokerCapabilityGateError) as exc:
        assert_broker_execution_capability(migrated_db, user_id="alice", broker_account_id="acc_kraken")
    assert exc.value.reason_code == REASON_EXECUTION_CAPABILITY_INCOMPLETE
    with pytest.raises(BrokerCapabilityGateError) as exc:
        assert_broker_execution_capability(migrated_db, user_id="mallory", broker_account_id="acc_bin")
    assert exc.value.reason_code == REASON_ACCOUNT_NOT_OWNED
    assert assert_broker_execution_capability(migrated_db, user_id="alice", broker_account_id="acc_bin").permitted


def test_start_bot_instance_refuses_incapable_broker(migrated_db):
    from app.core.bot_instance_service import BotInstanceService
    from app.core.broker_capability_gate import BrokerCapabilityGateError

    _account(migrated_db, "acc_kraken", "alice", "kraken")
    svc = BotInstanceService(db=migrated_db)
    inst = SimpleNamespace(id="bot1", status="paused", user_id="alice", broker_account_id="acc_kraken")
    with patch.object(svc, "get_bot_instance", return_value=inst), pytest.raises(BrokerCapabilityGateError) as exc:
        svc.start_bot_instance("bot1")
    assert json.loads(str(exc.value))["reason_code"] == REASON_EXECUTION_CAPABILITY_INCOMPLETE


# ── 2C: legacy platform-key routes require admin ───────────────────────────

def _token(role: str) -> str:
    from jose import jwt

    from app.core.config import settings
    from app.core.security import AUDIENCE, ISSUER

    now = int(time.time())
    return jwt.encode({"sub": f"{role}-1", "type": "access", "role": role, "iss": ISSUER, "aud": AUDIENCE,
                       "iat": now, "exp": now + 600}, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


LEGACY_ROUTES = [
    ("post", "/runner/live/start"), ("post", "/runner/live/stop"), ("post", "/binance/leverage"),
    ("post", "/binance/cancel-all"), ("get", "/binance/balance"), ("get", "/binance/order"),
    ("post", "/emergency/flatten"), ("post", "/risk/kill"), ("post", "/risk/unkill"),
    ("get", "/logs/events/tail"), ("post", "/debug/crash-next-cycle"), ("get", "/runner/status"),
]


@pytest.fixture(scope="module")
def api_client():
    from fastapi.testclient import TestClient

    from app.core.config import settings

    # app.main's startup guard compares settings.DATABASE_URL with DB().path;
    # settings may have been built while another test's temp DB was active.
    with patch.object(settings, "DATABASE_URL", "sqlite:///" + DB().path):
        from app.main import app
    return TestClient(app)  # no context manager: startup tasks do not run


@pytest.mark.parametrize("method,path", LEGACY_ROUTES)
def test_legacy_routes_reject_anonymous_and_normal_users(api_client, method, path):
    anon = getattr(api_client, method)(path)
    assert anon.status_code == 401, (path, anon.status_code)
    user = getattr(api_client, method)(path, headers={"Authorization": f"Bearer {_token('user')}"})
    assert user.status_code == 403, (path, user.status_code)


def test_every_legacy_global_route_is_admin_only(api_client):
    from app.core.auth import require_admin

    app = api_client.app

    public = {"/", "/health"}
    for route in app.routes:
        path = getattr(route, "path", "")
        if not path or path in public or path.startswith("/api/") or not getattr(route, "endpoint", None):
            continue
        if route.endpoint.__module__ != "app.main":
            continue
        deps = [d.call for d in route.dependant.dependencies]
        assert require_admin in deps, path


# ── 2D: encryption key policy ──────────────────────────────────────────────

def test_production_without_broker_key_fails_closed(monkeypatch):
    from shared_lib.core.security import broker_security as bs

    monkeypatch.delenv("BROKER_SECRET_KEY", raising=False)
    monkeypatch.setenv("APP_ENV", "production")
    with pytest.raises(bs.BrokerEncryptionConfigError):
        bs.assert_broker_encryption_configured()
    with pytest.raises(bs.BrokerEncryptionConfigError):
        bs.encrypt_credentials({"api_key": "k"})


def test_dedicated_key_encrypts_and_legacy_blobs_stay_readable(monkeypatch):
    from cryptography.fernet import Fernet

    from shared_lib.core.security import broker_security as bs

    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("BROKER_SECRET_KEY", raising=False)
    legacy_blob = bs.encrypt_credentials({"api_key": "old"})  # dev derivation
    monkeypatch.setenv("BROKER_SECRET_KEY", Fernet.generate_key().decode())
    new_blob = bs.encrypt_credentials({"api_key": "new"})
    assert bs.decrypt_credentials(new_blob) == {"api_key": "new"}
    assert bs.decrypt_credentials(legacy_blob) == {"api_key": "old"}  # decrypt-only legacy key
    assert bs.needs_reencryption(legacy_blob) and not bs.needs_reencryption(new_blob)
    # production: legacy keys only inside the explicit migration window
    monkeypatch.setenv("APP_ENV", "production")
    assert bs.decrypt_credentials(legacy_blob) == {}
    monkeypatch.setenv("BROKER_LEGACY_KEY_DECRYPT", "1")
    assert bs.decrypt_credentials(legacy_blob) == {"api_key": "old"}


def test_zero_key_never_used_to_encrypt_in_production(monkeypatch):
    from cryptography.fernet import Fernet, InvalidToken

    from shared_lib.core.security import broker_security as bs

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("BROKER_SECRET_KEY", "a-dedicated-non-fernet-secret")
    blob = bs.encrypt_credentials({"api_key": "k"})
    with pytest.raises(InvalidToken):
        Fernet(bs._ZERO_KEY).decrypt(blob.encode())


# ── 2D: redaction ──────────────────────────────────────────────────────────

def test_redaction_scrubs_signed_urls_headers_and_mappings():
    from shared_lib.core.security.redaction import MASK, redact_mapping, redact_text

    url = "400 Client Error for url: https://fapi.binance.com/fapi/v1/order?symbol=BTCUSDT&timestamp=1&signature=abc123def"
    assert "abc123def" not in redact_text(url) and "symbol=BTCUSDT" in redact_text(url)
    assert "SECRETVALUE1" not in redact_text('{"api_secret": "SECRETVALUE1"}')
    assert "KEYVALUE12345" not in redact_text("X-MBX-APIKEY: KEYVALUE12345")
    red = redact_mapping({"api_key": "abc", "nested": {"passphrase": "p"}, "symbol": "BTCUSDT"})
    assert red == {"api_key": MASK, "nested": {"passphrase": MASK}, "symbol": "BTCUSDT"}


def test_log_filter_redacts_records(caplog):
    from shared_lib.core.security.redaction import SecretRedactionFilter

    log = logging.getLogger("phase2.redaction.test")
    log.addFilter(SecretRedactionFilter())
    with caplog.at_level(logging.INFO, logger="phase2.redaction.test"):
        log.info("calling %s", "https://x/api?api_key=LEAKME1234&signature=SIGLEAK")
    assert "LEAKME1234" not in caplog.text and "SIGLEAK" not in caplog.text


def test_binance_error_text_is_redacted():
    from app.exchange.binance.client import BinanceFuturesClient

    client = BinanceFuturesClient(api_key="k", api_secret="s", base_url="https://example.invalid")
    err = RuntimeError("400 Client Error: for url: https://x/fapi/v1/order?a=1&signature=TOPSECRETSIG")
    client.session = MagicMock()
    client.session.request.side_effect = err
    with pytest.raises(RuntimeError) as exc:
        client._request("GET", "/fapi/v1/order", max_retries=0)
    assert "TOPSECRETSIG" not in str(exc.value)


# ── 2E: resolver-backed daily snapshots ────────────────────────────────────

def _store_credentials(db, acc_id, blob_dict, version=1):
    from shared_lib.core.security.broker_security import encrypt_credentials

    now = "2026-09-25T00:00:00Z"
    with db.connect() as conn:
        conn.execute("INSERT INTO broker_credentials_v2 (account_id, version, status, encrypted_blob, created_at, "
                     "updated_at) VALUES (?,?,?,?,?,?)", (acc_id, version, "active", encrypt_credentials(blob_dict), now, now))
        conn.execute("UPDATE broker_accounts SET active_credential_version=? WHERE id=?", (version, acc_id))


def test_daily_snapshot_uses_canonical_resolver(migrated_db):
    from app.analytics.daily_snapshot_scheduler import DailySnapshotScheduler

    _account(migrated_db, "acc_ok", "alice", "bybit", env="demo")
    _store_credentials(migrated_db, "acc_ok", {"api_key": "alicekey1", "api_secret": "alicesecret"})
    _account(migrated_db, "acc_nocred", "bob", "binance", env="live")
    _account(migrated_db, "acc_revoked", "bob", "binance", status="revoked")

    sched = DailySnapshotScheduler(db=migrated_db)
    sched.snapshot_service = MagicMock()
    summary = sched._record_daily_snapshots()
    assert summary["accounts"] == 2 and summary["recorded"] == 1 and summary["failed"] == 1
    assert summary["errors"]["acc_nocred"] == BrokerResolverError.REASON_NO_CREDENTIALS
    call = sched.snapshot_service.record_snapshot.call_args.kwargs
    assert call["broker_account_id"] == "acc_ok" and call["user_id"] == "alice"
    assert call["client"].base_url == "https://api-testnet.bybit.com"  # demo URL, never mainnet
    assert call["client"].api_key == "alicekey1"


def test_resolver_validation_mode_reads_pending_but_never_revoked(migrated_db):
    from shared_lib.broker import resolve_broker_auth

    _account(migrated_db, "acc_pending", "alice", "mt5", status="pending")
    _store_credentials(migrated_db, "acc_pending", {"bridge_url": "https://vps:8443", "bridge_token": "tok"})
    with pytest.raises(BrokerResolverError):
        resolve_broker_auth("acc_pending", "alice", migrated_db)
    auth = resolve_broker_auth("acc_pending", "alice", migrated_db, allow_unvalidated=True)
    assert auth.extra["bridge_token"] == "tok" and auth.base_url == "https://vps:8443"
    with pytest.raises(BrokerResolverError) as exc:
        resolve_broker_auth("acc_pending", "mallory", migrated_db, allow_unvalidated=True)
    assert exc.value.reason_code == BrokerResolverError.REASON_ACCESS_DENIED
    _account(migrated_db, "acc_rev", "alice", "binance", status="revoked")
    with pytest.raises(BrokerResolverError):
        resolve_broker_auth("acc_rev", "alice", migrated_db, allow_unvalidated=True)
