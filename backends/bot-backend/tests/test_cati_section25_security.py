"""CATI Section 25: credentials, redaction, withdrawal independence, least privilege, signature windows, retry
safety, rate limits, permission separation and action lineage."""
from __future__ import annotations

import json
import logging
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from shared_lib.broker.capabilities import execution_readiness
from shared_lib.broker.permissions import is_internal_transfer_permitted, normalize_bybit_query_api

from test_phase2b_permissions_transfers import (  # noqa: F401  (db / api are fixtures)
    SAFE_EVIDENCE, FakeAdapter, _account, _intent, _service, _token, api, db,
)

UNIQUE_SECRET = "zz-UNIQUE-SECRET-5b1d9"


# ---------------------------------------------------------------- 25.1 / 25.2 storage + keys
def test_credentials_are_encrypted_at_rest_and_the_key_is_not_in_the_database(db, monkeypatch):
    from shared_lib.core.security.broker_security import decrypt_credentials, encrypt_credentials

    monkeypatch.setenv("BROKER_SECRET_KEY", "k" * 44)
    blob = encrypt_credentials({"api_key": "AK-visible", "api_secret": UNIQUE_SECRET, "passphrase": UNIQUE_SECRET})
    assert UNIQUE_SECRET not in blob and "AK-visible" not in blob
    assert decrypt_credentials(blob)["api_secret"] == UNIQUE_SECRET
    with db.connect() as c:
        c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,label,status,environment,created_at,"
                  "updated_at,active_credential_version) VALUES ('k1','u','bybit','crypto','t','connected','demo','x','x',1)")
        c.execute("INSERT INTO broker_credentials_v2 (account_id,version,status,encrypted_blob,created_at,updated_at) "
                  "VALUES ('k1',1,'active',?,'x','x')", (blob,))
    raw = open(db.path, "rb").read()
    assert UNIQUE_SECRET.encode() not in raw and b"k" * 44 not in raw  # neither the secret nor the key is persisted


# ---------------------------------------------------------------- 25.3 redaction
@pytest.mark.parametrize("text", [
    "GET /v5/order?signature=deadbeefcafebabe1234&timestamp=1", "Authorization: Bearer eyJhbGciOi.secret.token",
    "api_key=AKIAABCDEFGH12345 api_secret=verysecretvalue", "X-BAPI-SIGN: 0123456789abcdef0123456789abcdef",
    "passphrase=hunter2hunter2",
])
def test_log_lines_and_exceptions_are_redacted(text):
    from shared_lib.core.security.redaction import SecretRedactionFilter, redact_exception, redact_text

    out = redact_text(text)
    for leaked in ("deadbeefcafebabe1234", "eyJhbGciOi.secret.token", "verysecretvalue", "0123456789abcdef0123456789abcdef",
                   "hunter2hunter2"):
        assert leaked not in out
    assert "deadbeefcafebabe1234" not in redact_exception(RuntimeError(text))
    rec = logging.LogRecord("x", logging.INFO, __file__, 1, text, (), None)
    SecretRedactionFilter().filter(rec)
    assert "verysecretvalue" not in rec.getMessage() and "hunter2hunter2" not in rec.getMessage()


def test_structured_events_are_sanitized():
    from app.trading_intelligence.observability.logging import stage_payload

    p = stage_payload(component="multi_asset.transfer", status="UNKNOWN", user_id="u", broker_account_id="a",
                      extra={"detail": "failed ?signature=deadbeefcafebabe1234 apiKey=AKIASECRETVALUE123"})
    assert "deadbeefcafebabe1234" not in json.dumps(p) and "AKIASECRETVALUE123" not in json.dumps(p)


# ---------------------------------------------------------------- 25.4 / 25.8 withdrawal + permission separation
def test_withdrawal_permission_is_never_required_and_its_absence_never_blocks():
    trade_only = {"TRADE": True, "READ_ACCOUNT": True, "WITHDRAW": False, "INTERNAL_TRANSFER": None}
    for broker in ("binance",):
        assert execution_readiness(broker, "live", permissions=trade_only).permitted  # no withdraw -> still trades
    from shared_lib.broker.wallets import topology_for

    from app.trading_intelligence.capital.planner import AccountCapitalState, CapitalSettings, plan_capital

    state = AccountCapitalState("a", "USDT", topology_for("bybit", "UNIFIED"), {"UNIFIED": Decimal("100")})
    plan = plan_capital(state=state, product="CRYPTO_PERPETUAL", required=Decimal("10"), settings=CapitalSettings(),
                        plan_key="k")
    assert plan.outcome == "NO_ACTION_SHARED_COLLATERAL"  # logical routing needs neither transfer nor withdraw permission
    assert not execution_readiness("binance", "live", permissions={**trade_only, "WITHDRAW": True}).permitted


def test_no_code_path_requests_or_depends_on_withdrawal():
    import re
    from pathlib import Path

    from app.exchange.contract import CANONICAL, EXECUTOR_REQUIRED, INTERNAL_TRANSFER_METHODS

    assert not any("withdraw" in m.lower() for m in (*CANONICAL, *EXECUTOR_REQUIRED, *INTERNAL_TRANSFER_METHODS))
    from app.exchange.binance.client import BinanceFuturesClient
    from app.exchange.bingx.client import BingXClient
    from app.exchange.bybit.client import BybitClient
    from app.exchange.contract import FORBIDDEN_SUBSTRINGS

    for cls in (BinanceFuturesClient, BybitClient, BingXClient):  # no client exposes any withdraw-named method
        assert not [n for n in dir(cls) if any(f in n.lower() for f in FORBIDDEN_SUBSTRINGS)], cls.__name__
    app = Path(__file__).resolve().parents[1] / "app"
    code = "\n".join(p.read_text(encoding="utf-8", errors="ignore") for d in ("exchange", "transfers")
                     for p in (app / d).rglob("*.py"))
    # no MUTATING request to any withdraw endpoint (read-only history mentioned in comments is not an action)
    assert not re.search(r"(POST|DELETE|PUT)[\"'][^\n]{0,80}withdraw", code, re.I)


def test_trade_and_internal_transfer_permissions_are_distinct():
    trade_no_transfer = normalize_bybit_query_api({"readOnly": 0, "permissions": {"ContractTrade": ["Order", "Position"]}})
    assert is_internal_transfer_permitted(trade_no_transfer) == (False, "INTERNAL_TRANSFER_PERMISSION_MISSING")
    assert execution_readiness("bybit", "demo", permissions=dict(trade_no_transfer.permissions)).permitted
    transfer_no_trade = normalize_bybit_query_api({"readOnly": 1, "permissions": {"Wallet": ["AccountTransfer"]}})
    assert not execution_readiness("bybit", "live", permissions=dict(transfer_no_trade.permissions)).permitted


def test_least_privilege_and_ip_allowlist_guidance(db, api, monkeypatch):
    import app.activation.account_status as ast

    monkeypatch.setattr(ast, "refresh_if_stale", lambda *a, **k: None)
    client, _ad = api
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    body = client.get("/api/v1/brokers/acc_a/market-status", headers={"Authorization": f"Bearer {_token('alice')}"}).json()
    lp = body["least_privilege"]
    assert lp["withdrawal"] == "NEVER_REQUIRED" and "ip_allowlist" in lp and lp["ip_allowlist"]["guidance"]
    assert body["permission_health"]["INTERNAL_TRANSFER"]["use"] == "REQUIRED_ONLY_FOR_PHYSICAL_TRANSFER"


# ---------------------------------------------------------------- 25.6 signature windows / no blind replay
def test_bybit_signs_with_recv_window_and_never_retries_a_mutation():
    import requests

    from app.exchange.bybit.client import BybitClient

    c = BybitClient.__new__(BybitClient)
    c.api_key, c.api_secret, c.base_url, c.recv_window = "k", "s", "https://api-demo.bybit.com", 5000
    with patch.object(requests, "post", side_effect=requests.Timeout("read timed out")) as post:
        with pytest.raises(Exception):
            c._request_v5("POST", "/v5/order/create", {"symbol": "BTCUSDT"})
    assert post.call_count == 1  # dispatched once; the executor reconciles an unknown outcome, never re-sends
    headers = post.call_args.kwargs["headers"]
    assert headers["X-BAPI-RECV-WINDOW"] == "5000" and headers["X-BAPI-TIMESTAMP"] and headers["X-BAPI-SIGN"]


def test_bingx_signs_every_request_with_a_fresh_timestamp():
    import requests

    from app.exchange.bingx.client import BingXClient

    c = BingXClient.__new__(BingXClient)
    c.api_key, c.api_secret, c.base_url = "k", "s", "https://open-api-vst.bingx.com"
    with patch.object(requests, "post", side_effect=requests.Timeout("t")) as post:
        try:
            c._request("POST", "/openApi/swap/v2/trade/order", {"symbol": "BTC-USDT"})
        except Exception:
            pass
    assert post.call_count == 1
    sent = post.call_args.kwargs.get("data") or post.call_args.args[1] if len(post.call_args.args) > 1 else \
        post.call_args.kwargs.get("data", "")
    assert "timestamp=" in str(sent) and "signature=" in str(sent)


# ---------------------------------------------------------------- 25.7 rate limits / no storms
def test_transfer_storm_is_blocked_while_one_is_in_flight(db):
    from app.transfers.models import TransferStatus as S

    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)

    def timeout(**k):
        raise TimeoutError("t")
    ad = FakeAdapter(submit=timeout)
    svc = _service(db, ad)
    first = svc.request_transfer(_intent(key="storm-0001"))
    second = svc.request_transfer(_intent(key="storm-0002"))
    assert first["status"] == S.UNKNOWN.value and second["status"] == S.BLOCKED.value
    assert second["failure_reason"] == "TRANSFER_IN_FLIGHT" and len(ad.submits) == 1


# ---------------------------------------------------------------- 25.9 mutation lineage
def test_account_mutations_carry_tenant_account_and_intent_lineage(db):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    row = _service(db, FakeAdapter()).request_transfer(_intent(key="lineage-0001"))
    assert (row["user_id"], row["broker_account_id"], row["idempotency_key"]) == ("alice", "acc_a", "lineage-0001")
    with db.connect() as c:
        ev = c.execute("SELECT user_id, broker_account_id FROM broker_transfer_events WHERE transfer_id=?",
                       (row["id"],)).fetchall()
    assert ev and all(tuple(e) == ("alice", "acc_a") for e in ev)
    from app.trading_intelligence.contracts.execution import ExecutionAttempt

    fields = set(ExecutionAttempt.__dataclass_fields__)
    assert {"user_id", "broker_account_id", "trade_plan_id", "execution_attempt_id", "client_order_id"} <= fields
