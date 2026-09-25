"""Phase 2 user-backend: no fake validation, withdraw-capable keys rejected,
resolver-backed credential access, INTERNAL_TRANSFER classification, and no
plaintext secrets forwarded to bot-backend.

Run: cd backends/user-backend && python -m pytest tests/test_phase2_broker_security.py
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

NOW = "2026-09-25T00:00:00Z"


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "ub.db"
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + path.as_posix())
    d = DB(path=str(path))
    migrate(d)
    return d


def _account(db, acc, user, broker="binance", status="pending", env="live"):
    from shared_lib.core.security.broker_security import encrypt_credentials

    with db.connect() as c:
        c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,label,status,environment,created_at,"
                  "updated_at,active_credential_version) VALUES (?,?,?,?,?,?,?,?,?,1)",
                  (acc, user, broker, "crypto", "t", status, env, NOW, NOW))
        c.execute("INSERT INTO broker_credentials_v2 (account_id,version,status,encrypted_blob,created_at,updated_at) "
                  "VALUES (?,?,?,?,?,?)", (acc, 1, "active",
                                           encrypt_credentials({"api_key": "key-abcd1234", "api_secret": "sec"}), NOW, NOW))


@pytest.mark.parametrize("broker", ["coinbase", "kraken", "alpaca"])
def test_unimplemented_brokers_never_report_connected(broker):
    from app.core.broker_service import _test_broker_connection

    res = _test_broker_connection(broker, {"api_key": "k", "api_secret": "s"}, "live")
    assert res["success"] is False and res["reason_code"] == "UNSUPPORTED_BROKER"


def test_withdraw_capable_binance_key_is_restricted_not_connected(db):
    from app.core import broker_service as bs

    _account(db, "acc1", "alice")
    ok = {"success": True, "capabilities": ["read", "trade"]}
    restrictions = {"enableReading": True, "enableFutures": True, "enableWithdrawals": True,
                    "permitsUniversalTransfer": True}
    wallet = MagicMock()
    wallet._signed_get.return_value = restrictions
    with patch.object(bs, "_test_broker_connection", return_value=ok), \
            patch("shared_lib.broker.client_factory.build_client_from_auth", return_value=wallet):
        res = bs.validate_broker_account("alice", "acc1")
    assert res["success"] is False and res["status"] == "restricted"
    assert res["permission_decision"] == "REJECTED_WITHDRAW_PERMISSION"
    with db.connect() as c:
        row = c.execute("SELECT status, permission_status FROM broker_accounts WHERE id='acc1'").fetchone()
        ev = json.loads(c.execute("SELECT permissions_json FROM broker_credentials_v2 WHERE account_id='acc1'").fetchone()[0])
    assert tuple(row) == ("restricted", "REJECTED_WITHDRAW_PERMISSION")
    assert ev["permissions"]["WITHDRAW"] is True and "key-abcd1234" not in json.dumps(ev)


def test_trade_only_key_connects_with_evidence(db):
    from app.core import broker_service as bs

    _account(db, "acc2", "alice")
    wallet = MagicMock()
    wallet._signed_get.return_value = {"enableReading": True, "enableFutures": True, "enableWithdrawals": False,
                                       "permitsUniversalTransfer": False}
    with patch.object(bs, "_test_broker_connection", return_value={"success": True, "capabilities": ["read"]}), \
            patch("shared_lib.broker.client_factory.build_client_from_auth", return_value=wallet):
        res = bs.validate_broker_account("alice", "acc2")
    assert res["success"] and res["status"] == "connected" and res["permission_decision"] == "ACCEPTED"


def test_get_decrypted_credentials_is_resolver_backed_and_owned(db):
    from app.core.broker_service import get_decrypted_credentials

    _account(db, "acc3", "alice", status="connected")
    creds = get_decrypted_credentials("alice", "acc3")
    assert creds["api_key"] == "key-abcd1234" and creds["base_url"] == "https://fapi.binance.com"
    assert get_decrypted_credentials("mallory", "acc3") is None
    _account(db, "acc4", "alice", status="pending")
    assert get_decrypted_credentials("alice", "acc4") is None
    assert get_decrypted_credentials("alice", "acc4", allow_unvalidated=True)["api_key"] == "key-abcd1234"


def test_internal_transfers_are_not_deposits_or_withdrawals():
    from app.core.transaction_service import classify_binance_income, classify_bybit_log

    b = classify_binance_income({"incomeType": "TRANSFER", "income": "-25", "asset": "USDT", "time": 1, "tranId": 9})
    assert b["type"] == "INTERNAL_TRANSFER" and b["direction"] == "OUT" and b["amount"] == 25.0
    assert classify_binance_income({"incomeType": "REALIZED_PNL", "income": "5"}) is None
    y = classify_bybit_log({"type": "TRANSFER_IN", "change": "10", "currency": "USDT", "transactionTime": "2"})
    assert y["type"] == "INTERNAL_TRANSFER" and y["direction"] == "IN"
    assert classify_bybit_log({"type": "TRADE", "change": "1"}) is None


def test_proxies_do_not_forward_plaintext_credentials():
    import inspect

    import app.api.auto_pilot_proxy as ap
    import app.api.bot_instances_proxy as bp

    assert "broker_credentials_map" not in inspect.getsource(ap)
    assert 'body["broker_credentials"]' not in inspect.getsource(bp)
