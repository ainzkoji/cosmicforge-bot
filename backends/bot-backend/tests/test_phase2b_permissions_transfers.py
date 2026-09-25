"""Phase 2F-2J: API-key permission evidence, withdraw rejection, broker-internal
transfers (validation, idempotency, submit-unknown, reconciliation), wallet
topology, the transfer API and tenant isolation."""
from __future__ import annotations

import inspect
import json
import sqlite3
import time
from decimal import Decimal
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from shared_lib.broker.capabilities import REASON_WITHDRAW_PERMISSION_PRESENT, execution_readiness
from shared_lib.broker.environment import BrokerEnvironment
from shared_lib.broker.permissions import (
    PermissionDecision, evaluate_trading_permissions, is_internal_transfer_permitted,
    normalize_binance_api_restrictions, normalize_bybit_query_api, unverified,
)
from shared_lib.broker.wallets import (
    BrokerTopology, BrokerWallet, TopologyMode, WalletPurpose, topology_for,
)
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

from app.transfers.adapters import (
    BinanceTransferAdapter, BrokerRejected, BybitTransferAdapter, TransferAdapter,
)
from app.transfers.models import (
    IdempotencyConflict, LookupOutcome, SubmitOutcome, TransferIntent, TransferOrigin, TransferStatus as S,
)
from app.transfers.reconciliation import TransferReconciler
from app.transfers.service import InternalTransferService, TransferAccessError

NOW = "2026-09-25T00:00:00Z"
SAFE_BINANCE = {"enableReading": True, "enableFutures": True, "permitsUniversalTransfer": True,
                "enableWithdrawals": False, "ipRestrict": True}


# ── 2F: permission normalisation + decisions ───────────────────────────────

def test_binance_withdraw_key_rejected_trade_only_accepted():
    ev = normalize_binance_api_restrictions({**SAFE_BINANCE, "enableWithdrawals": True})
    assert evaluate_trading_permissions(ev)[0] == PermissionDecision.REJECTED_WITHDRAW_PERMISSION
    ok = normalize_binance_api_restrictions(SAFE_BINANCE)
    assert evaluate_trading_permissions(ok)[0] == PermissionDecision.ACCEPTED
    assert is_internal_transfer_permitted(ok) == (True, "ok")
    no_futures = normalize_binance_api_restrictions({**SAFE_BINANCE, "enableFutures": False})
    assert evaluate_trading_permissions(no_futures)[0] == PermissionDecision.REJECTED_MISSING_TRADING_PERMISSION


def test_bybit_permissions_mapping():
    base = {"readOnly": 0, "permissions": {"ContractTrade": ["Order", "Position"], "Wallet": ["AccountTransfer"]},
            "ips": ["1.2.3.4"]}
    ev = normalize_bybit_query_api(base)
    assert evaluate_trading_permissions(ev)[0] == PermissionDecision.ACCEPTED
    assert is_internal_transfer_permitted(ev)[0] and ev.ip_restricted is True
    wd = normalize_bybit_query_api({**base, "permissions": {**base["permissions"], "Wallet": ["AccountTransfer", "Withdraw"]}})
    assert evaluate_trading_permissions(wd)[0] == PermissionDecision.REJECTED_WITHDRAW_PERMISSION
    assert is_internal_transfer_permitted(wd) == (False, "WITHDRAW_PERMISSION_PRESENT")
    ro = normalize_bybit_query_api({**base, "readOnly": 1})
    assert evaluate_trading_permissions(ro)[0] == PermissionDecision.REJECTED_MISSING_TRADING_PERMISSION


def test_uninspectable_key_trades_but_never_moves_money():
    ev = unverified("bingx", "bingx:not-inspectable", read_account=True)
    assert evaluate_trading_permissions(ev)[0] == PermissionDecision.ACCEPTED_UNVERIFIED
    assert is_internal_transfer_permitted(ev) == (False, "PERMISSION_EVIDENCE_REQUIRED")
    assert is_internal_transfer_permitted(None) == (False, "PERMISSION_EVIDENCE_REQUIRED")


def test_withdraw_capable_key_cannot_execute():
    r = execution_readiness("binance", "live", permissions={"WITHDRAW": True, "TRADE": True})
    assert not r.permitted and r.reason_code == REASON_WITHDRAW_PERMISSION_PRESENT


def test_permission_probe_uses_wallet_host_and_fails_closed_without_one():
    from shared_lib.broker.permission_probe import probe_permissions
    from shared_lib.broker.resolver import BrokerAuth

    seen = {}

    def build(auth):
        seen["url"] = auth.base_url
        c = MagicMock()
        c._signed_get.return_value = SAFE_BINANCE
        return c

    live = BrokerAuth("a", "u", "binance", BrokerEnvironment.LIVE, "https://fapi.binance.com", "k", "s", 1, "...")
    ev = probe_permissions(live, build=build)
    assert seen["url"] == "https://api.binance.com" and ev.inspected
    demo = BrokerAuth("a", "u", "binance", BrokerEnvironment.DEMO, "https://demo-fapi.binance.com", "k", "s", 1, "...")
    assert probe_permissions(demo, build=build).inspected is False

    def boom(auth):
        raise RuntimeError("timeout ?signature=SECRETSIG")

    failed = probe_permissions(live, build=boom)
    assert failed.inspected is False and "SECRETSIG" not in json.dumps(failed.to_dict())


# ── 2H: topology ───────────────────────────────────────────────────────────

def test_topologies_are_broker_specific():
    b, y, x = topology_for("binance"), topology_for("bybit", "UNIFIED"), topology_for("bingx")
    assert b.wallet("FUNDING").native_type == "FUNDING" and b.wallet("MAIN").purpose == WalletPurpose.SPOT
    assert y.wallet("MAIN") is None  # Bybit has no MAIN wallet
    assert x.wallet("FUND").purpose == WalletPurpose.FUNDING
    assert b.route(b.wallet("FUNDING"), b.wallet("UMFUTURE")) == "FUNDING_UMFUTURE"
    assert y.mode_between(WalletPurpose.UNIFIED, WalletPurpose.FX) == TopologyMode.SHARED_COLLATERAL
    assert y.mode_between(WalletPurpose.FUNDING, WalletPurpose.UNIFIED) == TopologyMode.PHYSICAL_TRANSFER_REQUIRED
    assert b.mode_between(WalletPurpose.SPOT, WalletPurpose.TRADFI) == TopologyMode.UNSUPPORTED
    assert topology_for("bybit", "CLASSIC").wallet("CONTRACT").purpose == WalletPurpose.DERIVATIVES


# ── Transfer service fixtures ──────────────────────────────────────────────

@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "transfers.db").as_posix())
    d = DB()
    migrate(d)
    return d


def _account(db, acc, user, broker="bybit", env="demo", perms: Optional[dict] = None):
    from shared_lib.core.security.broker_security import encrypt_credentials

    with db.connect() as c:
        c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,label,status,environment,created_at,"
                  "updated_at,active_credential_version) VALUES (?,?,?,?,?,?,?,?,?,1)",
                  (acc, user, broker, "crypto", "t", "connected", env, NOW, NOW))
        c.execute("INSERT INTO broker_credentials_v2 (account_id,version,status,encrypted_blob,created_at,updated_at,"
                  "permissions_json) VALUES (?,?,?,?,?,?,?)",
                  (acc, 1, "active", encrypt_credentials({"api_key": f"{user}-key-1234", "api_secret": "sec"}), NOW, NOW,
                   json.dumps(perms) if perms is not None else None))


SAFE_EVIDENCE = normalize_bybit_query_api({"readOnly": 0, "permissions": {
    "ContractTrade": ["Order", "Position"], "Wallet": ["AccountTransfer"]}}).to_dict()
WITHDRAW_EVIDENCE = normalize_bybit_query_api({"readOnly": 0, "permissions": {
    "ContractTrade": ["Order"], "Wallet": ["AccountTransfer", "Withdraw"]}}).to_dict()


class FakeAdapter(TransferAdapter):
    broker = "bybit"
    client_supplied_id = True

    def __init__(self, topo=None, balances=None, submit=None, lookup=None):
        self._topo = topo or topology_for("bybit", "CLASSIC")
        self.balances = balances if balances is not None else {("FUND", "USDT"): Decimal("1000"),
                                                               ("CONTRACT", "USDT"): Decimal("500")}
        self.submit_behavior = submit or (lambda **k: SubmitOutcome(S.COMPLETED, k["request_id"], "SUCCESS"))
        self.lookup_behavior = lookup or (lambda **k: LookupOutcome(False))
        self.submits = []
        self.history_rows = []

    def topology(self):
        return self._topo

    def transferable(self, wallet, asset):
        return self.balances.get((wallet.native_type, asset.upper()))

    def submit(self, **kw):
        self.submits.append(kw)
        return self.submit_behavior(**kw)

    def lookup(self, **kw):
        return self.lookup_behavior(**kw)

    def history(self, start_ms, end_ms):
        return self.history_rows


def _service(db, adapter):
    return InternalTransferService(db, adapter_factory=lambda auth: adapter)


def _intent(user="alice", acc="acc_a", amount="100", src="FUND", dst="CONTRACT", key="key-00000001",
            origin=TransferOrigin.MANUAL):
    return TransferIntent(user, acc, "USDT", Decimal(amount), src, dst, key, origin)


# ── 2G: service ────────────────────────────────────────────────────────────

def test_transfer_completes_and_is_idempotent(db):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    ad = FakeAdapter()
    svc = _service(db, ad)
    row = svc.request_transfer(_intent())
    assert row["status"] == "COMPLETED" and row["confirmed_at"] and row["source_venue_wallet"] == "FUND"
    again = svc.request_transfer(_intent())
    assert again["id"] == row["id"] and len(ad.submits) == 1
    with pytest.raises(IdempotencyConflict):
        svc.request_transfer(_intent(amount="101"))
    events = [e["event_type"] for e in svc.store.events(user_id="alice", broker_account_id="acc_a", transfer_id=row["id"])]
    assert events == ["CREATED", "VALIDATION_STARTED", "SUBMITTING", "SUBMITTED"]


@pytest.mark.parametrize("perms,reason", [
    (WITHDRAW_EVIDENCE, "WITHDRAW_PERMISSION_PRESENT"),
    (None, "PERMISSION_EVIDENCE_REQUIRED"),
    (unverified("bybit", "x").to_dict(), "PERMISSION_EVIDENCE_REQUIRED"),
])
def test_permission_gates_block_before_any_submission(db, perms, reason):
    _account(db, "acc_a", "alice", perms=perms)
    ad = FakeAdapter()
    row = _service(db, ad).request_transfer(_intent())
    assert row["status"] == "BLOCKED" and row["failure_reason"] == reason and ad.submits == []


@pytest.mark.parametrize("kwargs,reason", [
    ({"amount": "5000"}, "INSUFFICIENT_TRANSFERABLE_BALANCE"),
    ({"src": "SPOT", "dst": "FUND"}, "SOURCE_BALANCE_UNAVAILABLE"),
    ({"src": "MAIN"}, "UNKNOWN_SOURCE_WALLET"),
    ({"dst": "FUND"}, "SAME_WALLET"),
    ({"amount": "0"}, "INVALID_AMOUNT"),
    ({"origin": TransferOrigin.AUTOMATED}, "AUTOMATION_NOT_AUTHORIZED"),
])
def test_validation_blocks(db, kwargs, reason):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    ad = FakeAdapter()
    row = _service(db, ad).request_transfer(_intent(**kwargs))
    assert row["status"] == "BLOCKED" and row["failure_reason"] == reason and ad.submits == []


def test_unvalidated_transfer_adapter_is_refused_on_live(db):
    _account(db, "acc_live", "alice", env="live", perms=SAFE_EVIDENCE)
    ad = FakeAdapter()
    row = _service(db, ad).request_transfer(_intent(acc="acc_live"))
    assert row["failure_reason"] == "INTERNAL_TRANSFER_UNSUPPORTED" and ad.submits == []


def test_shared_collateral_needs_no_physical_transfer(db):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    topo = BrokerTopology("bybit", "UNIFIED", (
        BrokerWallet("bybit", "UNIFIED", WalletPurpose.UNIFIED), BrokerWallet("bybit", "FXW", WalletPurpose.FX)),
        routes={("UNIFIED", "FXW"): "X"}, shared_purposes=((WalletPurpose.UNIFIED, WalletPurpose.FX),))
    ad = FakeAdapter(topo=topo, balances={("UNIFIED", "USDT"): Decimal("1000")})
    row = _service(db, ad).request_transfer(_intent(src="UNIFIED", dst="FXW"))
    assert row["failure_reason"] == "SHARED_COLLATERAL_NO_TRANSFER_NEEDED" and ad.submits == []


def test_user_limits_and_reserves(db):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    ad = FakeAdapter()
    svc = _service(db, ad)
    svc.store.save_settings(user_id="alice", broker_account_id="acc_a",
                            values={"max_transfer_amount": "50", "asset_allowlist": ["USDT"]})
    assert svc.request_transfer(_intent(key="k-limit-0001"))["failure_reason"] == "MAX_TRANSFER_AMOUNT_EXCEEDED"
    svc.store.save_settings(user_id="alice", broker_account_id="acc_a",
                            values={"max_transfer_amount": None, "min_funding_balance": "950"})
    assert svc.request_transfer(_intent(key="k-limit-0002"))["failure_reason"] == "MIN_FUNDING_BALANCE"
    svc.store.save_settings(user_id="alice", broker_account_id="acc_a",
                            values={"min_funding_balance": None, "daily_transfer_limit": "150"})
    assert svc.request_transfer(_intent(key="k-limit-0003"))["status"] == "COMPLETED"
    assert svc.request_transfer(_intent(key="k-limit-0004"))["failure_reason"] == "DAILY_TRANSFER_LIMIT_EXCEEDED"


def test_moving_collateral_out_respects_reservations_and_bot_allocations(db):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    ad = FakeAdapter()
    svc = _service(db, ad)
    with db.connect() as c:
        c.execute("INSERT INTO cati_portfolio_reservations (reservation_id, broker_account_id, bot_instance_id, cycle_id,"
                  " selected_candidate_ids, selected_instruments, status, created_at, expires_at, updated_at, "
                  "reservation_version) VALUES ('r1','acc_a','b1','c1','[]','[]','RESERVED',0,9999999999999,0,'v')")
    row = svc.request_transfer(_intent(src="CONTRACT", dst="FUND", key="k-out-000001"))
    assert row["failure_reason"] == "PENDING_RISK_RESERVATIONS"
    with db.connect() as c:
        c.execute("UPDATE cati_portfolio_reservations SET status='RELEASED'")
        cols = {r[1] for r in c.execute("PRAGMA table_info(bot_instances)")}
        vals = {"id": "bot1", "user_id": "alice", "broker_account_id": "acc_a", "status": "active",
                "capital_allocation": 450, "capital_allocation_type": "fixed_amount", "config_id": "c",
                "risk_profile_id": "r", "created_at": NOW, "updated_at": NOW, "market_type": "crypto",
                "strategy_id": "s", "strategy_version": "1", "risk_level": "medium", "mode": "paper"}
        vals = {k: v for k, v in vals.items() if k in cols}
        c.execute(f"INSERT INTO bot_instances ({','.join(vals)}) VALUES ({','.join('?' * len(vals))})", tuple(vals.values()))
    row = svc.request_transfer(_intent(src="CONTRACT", dst="FUND", key="k-out-000002"))
    assert row["failure_reason"] == "BOT_ALLOCATION_CONSTRAINT"  # 500 - 100 = 400 < 450 allocated
    assert svc.request_transfer(_intent(src="CONTRACT", dst="FUND", amount="40", key="k-out-000003"))["status"] == "COMPLETED"


def test_submit_unknown_is_never_resubmitted_and_blocks_the_account(db):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)

    def timeout(**kw):
        raise TimeoutError("read timed out")

    ad = FakeAdapter(submit=timeout)
    svc = _service(db, ad)
    row = svc.request_transfer(_intent())
    assert row["status"] == "UNKNOWN" and row["failure_reason"] == "SUBMIT_OUTCOME_UNKNOWN"
    assert svc.request_transfer(_intent())["status"] == "UNKNOWN" and len(ad.submits) == 1  # replay: no resubmit
    blocked = svc.request_transfer(_intent(key="key-00000002", amount="10"))
    assert blocked["failure_reason"] == "TRANSFER_IN_FLIGHT"
    # Broker history shows it completed -> reconciliation settles it.
    ad.lookup_behavior = lambda **k: LookupOutcome(True, S.COMPLETED, k["request_id"], "SUCCESS")
    rec = TransferReconciler(db, adapter_factory=lambda a: ad)
    summary = rec.reconcile_account(user_id="alice", broker_account_id="acc_a")
    assert summary["completed"] == 1
    assert svc.store.get(user_id="alice", broker_account_id="acc_a", transfer_id=row["id"])["status"] == "COMPLETED"
    ad.submit_behavior = lambda **k: SubmitOutcome(S.COMPLETED, k["request_id"], "SUCCESS")
    assert svc.request_transfer(_intent(key="key-00000003", amount="10"))["status"] == "COMPLETED"


def test_broker_rejection_is_failed_not_unknown(db):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)

    def reject(**kw):
        raise BrokerRejected("retCode=131212 insufficient balance")

    row = _service(db, FakeAdapter(submit=reject)).request_transfer(_intent())
    assert row["status"] == "FAILED" and row["failure_reason"] == "BROKER_REJECTED"


def test_reconciliation_not_found_policy(db):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    _account(db, "acc_b", "bob", broker="binance", perms=normalize_binance_api_restrictions(SAFE_BINANCE).to_dict())

    def timeout(**kw):
        raise TimeoutError()

    bybit_like = FakeAdapter(submit=timeout)
    _service(db, bybit_like).request_transfer(_intent())
    binance_like = FakeAdapter(submit=timeout, topo=topology_for("binance"),
                               balances={("FUNDING", "USDT"): Decimal("1000")})
    binance_like.client_supplied_id = False
    _service(db, binance_like).request_transfer(_intent("bob", "acc_b", src="FUNDING", dst="UMFUTURE"))

    clock = [int(time.time() * 1000)]
    later = lambda: clock[0]  # noqa: E731
    for i in range(3):
        clock[0] += 20 * 60_000
        TransferReconciler(db, adapter_factory=lambda a: bybit_like, now_ms=later).reconcile_account(
            user_id="alice", broker_account_id="acc_a")
        TransferReconciler(db, adapter_factory=lambda a: binance_like, now_ms=later).reconcile_account(
            user_id="bob", broker_account_id="acc_b")
    store = InternalTransferService(db).store
    a = store.list(user_id="alice", broker_account_id="acc_a")[0]
    b = store.list(user_id="bob", broker_account_id="acc_b")[0]
    assert a["status"] == "FAILED" and a["failure_reason"] == "NOT_FOUND_AT_BROKER"  # our id, provably absent
    assert b["status"] == "RECONCILIATION_REQUIRED"  # no client id: never guessed as failed


def test_reconciliation_caches_history_as_internal_transfer(db):
    from app.transfers.models import HistoryRow

    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    ad = FakeAdapter()
    ad.history_rows = [HistoryRow("t-1", "USDT", Decimal("25"), "FUND", "CONTRACT", S.COMPLETED, 1_700_000_000_000)]
    TransferReconciler(db, adapter_factory=lambda a: ad).reconcile_account(user_id="alice", broker_account_id="acc_a")
    with db.connect() as c:
        r = c.execute("SELECT type, classification, direction FROM broker_transfers_cache WHERE raw_id='t-1'").fetchone()
    assert tuple(r) == ("INTERNAL_TRANSFER", "INTERNAL_TRANSFER", "FUND->CONTRACT")


def test_tenant_isolation_in_service_and_store(db):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    ad = FakeAdapter()
    svc = _service(db, ad)
    row = svc.request_transfer(_intent())
    with pytest.raises(TransferAccessError):
        svc.request_transfer(_intent(user="mallory", key="key-mallory-01"))
    assert svc.store.get(user_id="mallory", broker_account_id="acc_a", transfer_id=row["id"]) is None
    assert svc.store.list(user_id="mallory", broker_account_id="acc_a") == []
    with pytest.raises(TransferAccessError):
        svc.capabilities(user_id="mallory", account_id="acc_a")
    assert len(ad.submits) == 1


def test_transfer_events_are_append_only(db):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    _service(db, FakeAdapter()).request_transfer(_intent())
    with db.connect() as c, pytest.raises(sqlite3.IntegrityError):
        c.execute("UPDATE broker_transfer_events SET event_type='X'")
    with db.connect() as c, pytest.raises(sqlite3.IntegrityError):
        c.execute("DELETE FROM broker_transfer_events")


# ── adapters over recorded payload shapes ──────────────────────────────────

def _auth(broker, env=BrokerEnvironment.LIVE):
    from shared_lib.broker.resolver import BrokerAuth

    return BrokerAuth("acc", "u", broker, env, "https://x", "k", "s", 1, "...")


def test_binance_adapter_submit_lookup_and_ambiguity():
    wallet, trading = MagicMock(), MagicMock()
    adapter = BinanceTransferAdapter(_auth("binance"), build=lambda a: wallet if "api.binance.com" in a.base_url else trading)
    wallet.universal_transfer.return_value = {"tranId": 13526853623}
    topo = adapter.topology()
    out = adapter.submit(request_id="r", route_code="FUNDING_UMFUTURE", source=topo.wallet("FUNDING"),
                         destination=topo.wallet("UMFUTURE"), asset="USDT", amount=Decimal("10"))
    assert out.status == S.CONFIRMATION_PENDING and out.broker_transfer_id == "13526853623"
    wallet.universal_transfer.assert_called_once_with("FUNDING_UMFUTURE", "USDT", "10")
    t0 = 1_700_000_000_000
    wallet.universal_transfer_history.return_value = {"rows": [
        {"asset": "USDT", "amount": "10", "type": "FUNDING_UMFUTURE", "status": "CONFIRMED", "tranId": 1, "timestamp": t0}]}
    hit = adapter.lookup(request_id="r", broker_transfer_id=None, route_code="FUNDING_UMFUTURE", asset="USDT",
                         amount=Decimal("10"), submitted_at_ms=t0, claimed_ids=set())
    assert hit.found and hit.status == S.COMPLETED and hit.broker_transfer_id == "1"
    wallet.universal_transfer_history.return_value["rows"].append(
        {"asset": "USDT", "amount": "10", "type": "FUNDING_UMFUTURE", "status": "CONFIRMED", "tranId": 2, "timestamp": t0 + 5})
    amb = adapter.lookup(request_id="r", broker_transfer_id=None, route_code="FUNDING_UMFUTURE", asset="USDT",
                         amount=Decimal("10"), submitted_at_ms=t0, claimed_ids=set())
    assert amb.found is False  # two candidates: never guess
    trading.account_balance.return_value = [{"asset": "USDT", "maxWithdrawAmount": "123.4"}]
    assert adapter.transferable(topo.wallet("UMFUTURE"), "USDT") == Decimal("123.4")


def test_binance_demo_has_no_wallet_api():
    from app.transfers.adapters import VenueApiUnavailable

    adapter = BinanceTransferAdapter(_auth("binance", BrokerEnvironment.DEMO), build=lambda a: MagicMock())
    with pytest.raises(VenueApiUnavailable):
        adapter.transferable(adapter.topology().wallet("FUNDING"), "USDT")


def test_bybit_adapter_mode_balance_and_rejection():
    client = MagicMock()
    adapter = BybitTransferAdapter(_auth("bybit"), build=lambda a: client)
    client.account_info.return_value = {"retCode": 0, "result": {"unifiedMarginStatus": 4}}
    assert adapter.topology().account_mode == "UNIFIED"
    client.account_coin_balance.return_value = {"retCode": 0, "result": {"balance": {"transferBalance": "77.5"}}}
    assert adapter.transferable(adapter.topology().wallet("FUND"), "USDT") == Decimal("77.5")
    client.inter_transfer.return_value = {"retCode": 131212, "retMsg": "insufficient"}
    topo = adapter.topology()
    with pytest.raises(BrokerRejected):
        adapter.submit(request_id="0e7d1b8e-0000-4000-8000-000000000001", route_code="FUND->UNIFIED",
                       source=topo.wallet("FUND"), destination=topo.wallet("UNIFIED"), asset="USDT", amount=Decimal("1"))
    client.account_info.return_value = {"retCode": 10001}
    assert BybitTransferAdapter(_auth("bybit"), build=lambda a: client).topology() is None  # unknown mode: no guess


# ── no withdrawal anywhere ─────────────────────────────────────────────────

def test_no_withdrawal_primitive_exists():
    from app.exchange.binance.client import BinanceFuturesClient
    from app.exchange.bingx.client import BingXClient
    from app.exchange.bybit.client import BybitClient
    import app.transfers.adapters as adapters_mod

    for cls in (BinanceFuturesClient, BybitClient, BingXClient, *adapters_mod.ADAPTERS.values()):
        for name, _ in inspect.getmembers(cls, inspect.isfunction):
            assert "withdraw" not in name.lower(), f"{cls.__name__}.{name}"
    src = inspect.getsource(adapters_mod)
    assert "/withdraw" not in src and "withdraw/apply" not in src


# ── 2J: API + isolation ────────────────────────────────────────────────────

def _token(user):
    from jose import jwt

    from app.core.config import settings
    from app.core.security import AUDIENCE, ISSUER

    now = int(time.time())
    return jwt.encode({"sub": user, "type": "access", "role": "user", "iss": ISSUER, "aud": AUDIENCE, "iat": now,
                       "exp": now + 600}, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


@pytest.fixture
def api(db):
    from fastapi.testclient import TestClient

    from app.api.broker_transfers import get_transfer_reconciler, get_transfer_service
    from app.core.config import settings

    with patch.object(settings, "DATABASE_URL", "sqlite:///" + DB().path):
        from app.main import app
    ad = FakeAdapter()
    app.dependency_overrides[get_transfer_service] = lambda: _service(db, ad)
    app.dependency_overrides[get_transfer_reconciler] = lambda: TransferReconciler(db, adapter_factory=lambda a: ad)
    try:
        yield TestClient(app), ad
    finally:
        app.dependency_overrides.pop(get_transfer_service, None)
        app.dependency_overrides.pop(get_transfer_reconciler, None)


def test_transfer_api_flow_and_isolation(db, api):
    client, ad = api
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    alice = {"Authorization": f"Bearer {_token('alice')}"}
    mallory = {"Authorization": f"Bearer {_token('mallory')}"}
    body = {"asset": "usdt", "amount": "12.5", "source_wallet": "fund", "destination_wallet": "contract"}

    assert client.post("/api/v1/brokers/acc_a/internal-transfers", json=body).status_code == 401
    assert client.post("/api/v1/brokers/acc_a/internal-transfers", json=body, headers=alice).status_code == 422
    r = client.post("/api/v1/brokers/acc_a/internal-transfers", json=body,
                    headers={**alice, "Idempotency-Key": "idem-key-0001"})
    assert r.status_code == 201 and r.json()["status"] == "COMPLETED" and r.json()["amount"] == "12.5"
    tid = r.json()["id"]
    assert client.get(f"/api/v1/brokers/acc_a/internal-transfers/{tid}", headers=alice).json()["events"]

    for method, path, kw in (("post", "/api/v1/brokers/acc_a/internal-transfers",
                              {"json": {**body, "idempotency_key": "idem-mallory-1"}}),
                             ("get", "/api/v1/brokers/acc_a/internal-transfers", {}),
                             ("get", f"/api/v1/brokers/acc_a/internal-transfers/{tid}", {}),
                             ("get", "/api/v1/brokers/acc_a/wallets", {}),
                             ("get", "/api/v1/brokers/acc_a/transfer-capabilities", {}),
                             ("put", "/api/v1/brokers/acc_a/transfer-settings", {"json": {"mode": "MANUAL_TRANSFER"}}),
                             ("post", "/api/v1/brokers/acc_a/internal-transfers/reconcile", {})):
        resp = getattr(client, method)(path, headers=mallory, **kw)
        assert resp.status_code == 404, (path, resp.status_code)
    assert len(ad.submits) == 1

    bad = client.put("/api/v1/brokers/acc_a/transfer-settings", headers=alice,
                     json={"mode": "AUTOMATED_INTERNAL_REALLOCATION"})
    assert bad.status_code == 422
    ok = client.put("/api/v1/brokers/acc_a/transfer-settings", headers=alice,
                    json={"mode": "AUTOMATED_INTERNAL_REALLOCATION", "authorize_automated_reallocation": True,
                          "max_transfer_amount": "100"})
    assert ok.status_code == 200 and ok.json()["authorized_at"]
    caps = client.get("/api/v1/brokers/acc_a/transfer-capabilities", headers=alice).json()
    assert caps["withdrawals_supported_by_platform"] is False


def test_no_route_can_withdraw():
    from app.core.config import settings

    with patch.object(settings, "DATABASE_URL", "sqlite:///" + DB().path):
        from app.main import app
    assert not [r.path for r in app.routes if "withdraw" in getattr(r, "path", "").lower()]
