"""CATI multi-asset Sections 7-9 closure.

7  dynamic venue capability discovery: complete pagination, empty / partial responses, idempotent
   registry updates, delisting history, metadata changes, new-listing lifecycle, independent
   capability dimensions, DEMO vs LIVE catalogs, throttled refresh, deterministic reasons.
8  connected broker accounts: one account -> several market families, per-family "why blocked",
   abstract permission health, tenant/account isolation (404), secret redaction.
9  internal capital routing: topology classes, unknown topology / route blocks, Auto Capital Routing
   policy (enable, emergency stop, kill switch, route allowlist, % / destination / manual-approval
   caps), admitted-opportunity preconditions, idempotency, ambiguous submission, reconciliation,
   no withdrawal path anywhere reachable.

Venue payloads are the recorded public shapes (see test_multi_asset_closure.py). No network.
"""
from __future__ import annotations

import ast
import json
import sqlite3
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from shared_lib.broker.permissions import normalize_bybit_query_api, unverified
from shared_lib.broker.wallets import (BrokerTopology, BrokerWallet, TopologyClass, WalletPurpose, topology_class,
                                       topology_for, topology_for_account)
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

from app.exchange.instruments import (FX, OTHER, STOCK, InstrumentCatalog, parse_bingx_contract,
                                      parse_bybit_instrument, sync_instruments)
from app.transfers.adapters import TransferAdapter
from app.transfers.models import (TRANSFER_PRECONDITIONS, LookupOutcome, SubmitOutcome, TransferIntent,
                                  TransferOrigin, TransferStatus as S)
from app.transfers.reconciliation import TransferReconciler
from app.transfers.service import InternalTransferService, TransferAccessError

BACKEND = Path(__file__).resolve().parents[1]
NOW = "2026-09-26T00:00:00Z"
D = Decimal


def _bybit(symbol, base, symbol_type, *, status="Trading", tick="0.00001"):
    return {"symbol": symbol, "baseCoin": base, "quoteCoin": "USDT", "settleCoin": "USDT", "symbolType": symbol_type,
            "contractType": "LinearPerpetual", "status": status, "launchTime": "1788858596000", "fundingInterval": 480,
            "lotSizeFilter": {"maxOrderQty": "356000.0", "minOrderQty": "0.1", "qtyStep": "0.1",
                              "minNotionalValue": "5"},
            "priceFilter": {"minPrice": "0.00001", "maxPrice": "199.99998", "tickSize": tick},
            "leverageFilter": {"maxLeverage": "100"}}


def _bingx(symbol, asset, *, status=1, api_open="true"):
    return {"contractId": "1", "symbol": symbol, "size": "0.0001", "quantityPrecision": 4, "pricePrecision": 5,
            "currency": "USDT", "asset": asset, "status": status, "apiStateOpen": api_open, "apiStateClose": "true",
            "tradeMinQuantity": 0.0001, "tradeMinUSDT": 2, "launchTime": 1586275200000}


BTC = parse_bybit_instrument(_bybit("BTCUSDT", "BTC", ""))
EUR = parse_bybit_instrument(_bybit("EURUSDUSDT", "EURUSD", "forex"))
GBP = parse_bybit_instrument(_bybit("GBPUSDUSDT", "GBPUSD", "forex"))
JPY = parse_bybit_instrument(_bybit("USDJPYUSDT", "USDJPY", "forex"))
SPY = parse_bybit_instrument(_bybit("SPYUSDT", "SPY", "ETF"))
PERMS = {"READ_ACCOUNT": True, "READ_POSITIONS": True, "READ_ORDERS": True, "TRADE": True,
         "INTERNAL_TRANSFER": True, "WITHDRAW": False}


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "s79.db").as_posix())
    d = DB()
    migrate(d)
    return d


def _instruments(n, prefix="C"):
    return [parse_bybit_instrument(_bybit(f"{prefix}{i:03d}USDT", f"{prefix}{i:03d}", "")) for i in range(n)]


# ═══════════════════════════════ SECTION 7 ═══════════════════════════════

def test_fx_identity_regressions_cannot_return():
    assert [(i.canonical_symbol, i.base_currency, i.quote_currency, i.settlement_asset) for i in (EUR, GBP, JPY)] == [
        ("EUR/USD:FX_PERPETUAL", "EUR", "USD", "USDT"), ("GBP/USD:FX_PERPETUAL", "GBP", "USD", "USDT"),
        ("USD/JPY:FX_PERPETUAL", "USD", "JPY", "USDT")]
    assert "EURUSD/USD" not in {i.canonical_symbol.split(":")[0] for i in (EUR, GBP, JPY)}
    assert (SPY.asset_class, SPY.product_type) == (STOCK, "TRADFI_PERPETUAL")  # ETF never CRYPTO
    assert EUR.product_type == "FX_PERPETUAL" and BTC.product_type == "PERPETUAL"  # product type != asset class


def test_bybit_pagination_never_returns_a_truncated_universe():
    from app.exchange.bybit.client import BybitClient

    c = BybitClient("k", "s", base_url="https://x")
    c._request_v5 = lambda m, p, payload=None: {"retCode": 0, "result": {"list": [_bybit("BTCUSDT", "BTC", "")],
                                                                        "nextPageCursor": "same"}}
    with pytest.raises(RuntimeError, match="repeated pagination cursor"):
        c.discover_instruments()
    counter = iter(range(10_000))
    c._request_v5 = lambda m, p, payload=None: {"retCode": 0, "result": {"list": [],
                                                                        "nextPageCursor": f"c{next(counter)}"}}
    with pytest.raises(RuntimeError, match="did not terminate"):
        c.discover_instruments()


def test_catalog_update_is_idempotent_and_keeps_history(tmp_path):
    from shared_lib.persistence.market_data_schema import ensure_market_data_schema

    d = DB(path=str(tmp_path / "cat.db"))
    ensure_market_data_schema(d)
    ensure_market_data_schema(d)  # migration re-run is a no-op
    cat = InstrumentCatalog(d)
    first = cat.upsert("bybit_linear", "DEMO", [BTC, EUR, EUR], 1)  # a symbol repeated in one response: once
    assert (first["discovered"], first["new"]) == (2, 2)
    again = cat.upsert("bybit_linear", "DEMO", [BTC, EUR], 2)
    assert (again["new"], again["delisted"], again["metadata_changed"], again["relisted"]) == (0, 0, 0, 0)
    with d.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM venue_instruments").fetchone()[0] == 2
    rec = cat.record("bybit_linear", "DEMO", "EURUSDUSDT")
    assert (rec["first_seen_ms"], rec["last_seen_ms"], rec["state"]) == (1, 2, "LISTED")
    # metadata change (tick size) is detected and stamped
    eur_tick = parse_bybit_instrument(_bybit("EURUSDUSDT", "EURUSD", "forex", tick="0.0001"))
    assert cat.upsert("bybit_linear", "DEMO", [BTC, eur_tick], 3)["metadata_changed"] == 1
    assert cat.record("bybit_linear", "DEMO", "EURUSDUSDT")["metadata_changed_ms"] == 3
    # delisting keeps the identity queryable; relisting clears it; first_seen never moves
    assert cat.upsert("bybit_linear", "DEMO", [BTC], 4)["delisted"] == 1
    gone = cat.record("bybit_linear", "DEMO", "EURUSDUSDT")
    assert gone["state"] == "DELISTED" and gone["delisted_at_ms"] == 4 and gone["instrument"].canonical_symbol == \
        "EUR/USD:FX_PERPETUAL"
    assert [i.venue_symbol for i in cat.list("bybit_linear", "DEMO", tradable_only=False)] == ["BTCUSDT"]
    assert {i.venue_symbol for i in cat.list("bybit_linear", "DEMO", tradable_only=False, include_delisted=True)} == {
        "BTCUSDT", "EURUSDUSDT"}
    back = cat.upsert("bybit_linear", "DEMO", [BTC, eur_tick], 5)
    assert back["relisted"] == 1 and cat.record("bybit_linear", "DEMO", "EURUSDUSDT")["first_seen_ms"] == 1
    # environments are separate catalogs: DEMO discovery says nothing about LIVE
    assert cat.list("bybit_linear", "LIVE", tradable_only=False) == [] and cat.last_synced_ms("bybit_linear", "LIVE") is None
    # an empty response never delists
    with pytest.raises(ValueError):
        cat.upsert("bybit_linear", "DEMO", [], 6)
    assert cat.active_count("bybit_linear", "DEMO") == 2


def test_partial_discovery_is_refused_not_recorded_as_mass_delisting(tmp_path):
    from shared_lib.persistence.market_data_schema import ensure_market_data_schema

    d = DB(path=str(tmp_path / "cat.db"))
    ensure_market_data_schema(d)
    cat = InstrumentCatalog(d)
    full = _instruments(40)
    sync_instruments(SimpleNamespace(discover_instruments=lambda: full), catalog=cat, venue="bybit_linear",
                     environment="LIVE", now_ms=1)
    with pytest.raises(RuntimeError, match="DISCOVERY_SUSPECT_PARTIAL"):
        sync_instruments(SimpleNamespace(discover_instruments=lambda: full[:10]), catalog=cat, venue="bybit_linear",
                         environment="LIVE", now_ms=2)
    assert cat.active_count("bybit_linear", "LIVE") == 40
    # an ordinary delisting of a few symbols goes through
    assert sync_instruments(SimpleNamespace(discover_instruments=lambda: full[:37]), catalog=cat,
                            venue="bybit_linear", environment="LIVE", now_ms=3)["delisted"] == 3


def test_capability_dimensions_are_independent_with_deterministic_reasons():
    from app.activation.market import instrument_capabilities

    # BingX lists the FX contract with public data, but its API does not accept orders for it
    closed = parse_bingx_contract(_bingx("NCFXEUR2USD-USDT", "NCFXEUR2USD", api_open="false"))
    r = instrument_capabilities(closed, broker="bingx", environment="DEMO", permissions=PERMS)
    dims = {k: v["state"] for k, v in r["dimensions"].items()}
    assert dims["MARKET_EXISTS"] == dims["MARKET_DATA_SUPPORTED"] == "YES"
    assert r["dimensions"]["API_EXECUTION_SUPPORTED"]["reason"] == "VENUE_API_NOT_SUPPORTED"
    assert r["lifecycle_stage"] == "RESEARCH_ONLY" and r["next_blocker"]["reason_class"] == "VENUE_API_NOT_SUPPORTED"
    # venue + API support the product, but this account is not eligible on LIVE (adapter unvalidated)
    live = instrument_capabilities(EUR, broker="bybit", environment="LIVE", permissions=PERMS)
    assert live["dimensions"]["API_EXECUTION_SUPPORTED"]["state"] == "YES"
    assert live["dimensions"]["ACCOUNT_ELIGIBLE"]["reason"] == "BROKER_EXECUTION_UNVALIDATED_FOR_LIVE"
    demo = instrument_capabilities(EUR, broker="bybit", environment="DEMO", permissions=PERMS)
    assert demo["dimensions"]["ACCOUNT_ELIGIBLE"]["state"] == "YES"  # DEMO eligibility never implies LIVE
    # a key without TRADE: venue supports it, the account cannot
    nt = instrument_capabilities(EUR, broker="bybit", environment="DEMO", permissions={**PERMS, "TRADE": False})
    assert nt["dimensions"]["ACCOUNT_ELIGIBLE"]["state"] == "NO"
    assert nt["dimensions"]["ORDER_SUBMISSION_SUPPORTED"]["reason"] == "API_KEY_PERMISSION_MISSING"
    # economics exist only for the validated venue adapter; unknown governance is never authority
    assert demo["dimensions"]["ECONOMICS_AVAILABLE"]["reason"] == "CATI_ECONOMIC_ADAPTER_NOT_VALIDATED_FOR_VENUE"
    assert demo["dimensions"]["GOVERNANCE_AUTHORISED"]["state"] == "UNKNOWN"
    # unknown venue product type: OTHER, never executable
    other = parse_bybit_instrument(_bybit("ZZZUSDT", "ZZZ", "someNewType"))
    o = instrument_capabilities(other, broker="bybit", environment="DEMO", permissions=PERMS)
    assert other.asset_class == OTHER and o["dimensions"]["ACCOUNT_ELIGIBLE"]["reason"] == "ASSET_CLASS_NOT_SUPPORTED"
    # deterministic: same facts -> same output
    assert json.dumps(r, sort_keys=True) == json.dumps(
        instrument_capabilities(closed, broker="bingx", environment="DEMO", permissions=PERMS), sort_keys=True)
    # delisted: stays identifiable, nothing executable
    gone = instrument_capabilities(EUR, broker="bybit", environment="DEMO", permissions=PERMS, delisted=True)
    assert gone["lifecycle_stage"] == "DELISTED" and gone["dimensions"]["ACCOUNT_ELIGIBLE"]["state"] == "NO"


def test_a_new_listing_is_never_immediately_cati_executable():
    from app.activation.market import instrument_capabilities
    from app.activation.model import Prerequisite, decide

    new = parse_bybit_instrument(_bybit("NEWCOINUSDT", "NEWCOIN", "innovation"))
    r = instrument_capabilities(new, broker="bybit", environment="DEMO", permissions=PERMS)
    assert r["lifecycle_stage"] == "DATA_READY" and r["next_blocker"]["dimension"] == "CERTIFICATION_READY"
    gov_m0 = decide("CATI_CRYPTO_EXECUTION", [Prerequisite("governance_phase", False,
                                                           "GOVERNANCE_PHASE_M0_NO_CATI_AUTHORITY")])
    certified = instrument_capabilities(new, broker="bybit", environment="DEMO", permissions=PERMS,
                                        certified_instruments={"NEWCOIN/USDT:PERP"}, cati_decision=gov_m0)
    assert certified["lifecycle_stage"] == "ACCOUNT_ELIGIBLE"
    assert certified["next_blocker"]["reason_class"] == "GOVERNANCE_NOT_READY"


def test_bybit_mt5_cfd_is_not_an_api_product():
    from shared_lib.broker.capabilities import Capability, declared_profile

    assert "MT5/CFD" in declared_profile("bybit").entry(Capability.TRADFI).detail  # separate platform, no V5 route


def test_discovery_refresh_is_stale_driven_and_throttled(tmp_path, monkeypatch):
    from shared_lib.persistence.market_data_schema import ensure_market_data_schema

    from app.activation import account_status as st

    d = DB(path=str(tmp_path / "cat.db"))
    ensure_market_data_schema(d)
    monkeypatch.setattr(st, "_refresh_attempts", {})
    monkeypatch.setattr("app.exchange.catalog_refresh._pending", {})  # no event requests from other tests
    monkeypatch.setattr("app.ops.multi_asset_metrics.instrument_sync", lambda *a, **k: None)
    calls = []
    client = SimpleNamespace(discover_instruments=lambda: calls.append(1) or [BTC, EUR])
    auth = SimpleNamespace(broker_type="bybit", environment=SimpleNamespace(value="DEMO"))
    now = 10_000_000_000
    assert st.discovery_freshness(d, "bybit", "DEMO", now_ms=now)["status"] == "DATA_NOT_READY"
    assert st.refresh_if_stale(d, auth, client_factory=lambda a: client, now_ms=now)["status"] == "SYNCED"
    assert st.refresh_if_stale(d, auth, client_factory=lambda a: client, now_ms=now + 1000) is None  # fresh
    later = now + st.DISCOVERY_MAX_AGE_MS + 1
    assert st.discovery_freshness(d, "bybit", "DEMO", now_ms=later)["status"] == "STALE"
    assert st.refresh_if_stale(d, auth, client_factory=lambda a: client, now_ms=later)["status"] == "SYNCED"
    # a failing venue is retried at most once per interval (no call storm)
    boom = SimpleNamespace(discover_instruments=lambda: calls.append(1) or [])
    much_later = later + st.DISCOVERY_MAX_AGE_MS + 1
    assert st.refresh_if_stale(d, auth, client_factory=lambda a: boom, now_ms=much_later)["status"] == "FAILED"
    assert st.refresh_if_stale(d, auth, client_factory=lambda a: boom, now_ms=much_later + 1)["status"] == "THROTTLED"
    assert len(calls) == 3


# ═══════════════════════════════ SECTION 8 ═══════════════════════════════

def test_one_account_answers_which_families_it_can_automate_and_why_not():
    from app.activation.market import REASON_CLASSES, account_market_status

    ins = [BTC, EUR, SPY]
    demo = account_market_status(broker="bybit", environment="DEMO", permissions=PERMS, instruments=ins,
                                 transfers_in_flight=0, account_mode="UNIFIED")
    ex = {f: (v["execution"]["status"], v["execution"]["reason_class"]) for f, v in demo["markets"].items()}
    assert ex["CRYPTO"] == ex["FX"] == ex["STOCK"] == ("ACTIVE", "ACTIVE")  # no separate FX key needed
    assert ex["INDEX"][1] == "NOT_LISTED" and ex["COMMODITIES"][1] == "NOT_LISTED"
    assert demo["markets"]["FX"]["cati"]["reason_class"] == "CERTIFICATION_NOT_READY"
    assert demo["markets"]["FX"]["markets_cati_eligible"] == 0 and demo["topology_class"] == "UNIFIED"
    live = account_market_status(broker="bybit", environment="LIVE", permissions=PERMS, instruments=ins,
                                 transfers_in_flight=0)
    assert live["markets"]["FX"]["execution"]["reason_class"] == "ADAPTER_NOT_VALIDATED"  # ACTIVE on DEMO only
    assert live["markets"]["FX"]["blocked_reasons"] == {"BROKER_EXECUTION_UNVALIDATED_FOR_LIVE": 1}
    assert live["topology_class"] == "UNKNOWN"  # account mode not read -> not assumed unified
    # venue lists the product but its API does not accept orders
    bx = [parse_bingx_contract(_bingx("NCFXEUR2USD-USDT", "NCFXEUR2USD", api_open="false"))]
    b = account_market_status(broker="bingx", environment="DEMO", permissions=None, instruments=bx)
    assert b["markets"]["FX"]["execution"]["reason_class"] == "VENUE_API_NOT_SUPPORTED"
    assert b["permission_health"]["INTERNAL_TRANSFER"]["state"] == "UNVERIFIED"
    assert b["capabilities"]["INTERNAL_TRANSFER"]["state"] != "ACTIVE"
    uninspectable = account_market_status(broker="bybit", environment="DEMO", permissions={"TRADE": True},
                                          instruments=ins, transfers_in_flight=0, account_mode="UNIFIED")
    xfer = uninspectable["capabilities"]["INTERNAL_TRANSFER"]
    assert (xfer["reason"], xfer["reason_class"]) == ("PERMISSION_EVIDENCE_REQUIRED", "PERMISSION_EVIDENCE_REQUIRED")
    denied = account_market_status(broker="bybit", environment="DEMO", permissions={**PERMS, "INTERNAL_TRANSFER": False},
                                   instruments=ins, transfers_in_flight=0, account_mode="UNIFIED")
    assert denied["capabilities"]["INTERNAL_TRANSFER"]["reason_class"] == "PERMISSION_MISSING"
    assert uninspectable["markets"]["CRYPTO"]["execution"]["status"] == "ACTIVE"  # trading needs no transfer grant
    # never synced / stale catalog
    none = account_market_status(broker="bybit", environment="DEMO", permissions=PERMS, instruments=None)
    assert none["markets"]["FX"]["execution"]["reason"] == "DISCOVERY_NOT_SYNCED"
    stale = account_market_status(broker="bybit", environment="DEMO", permissions=PERMS, instruments=ins,
                                  discovery_fresh=False)
    assert stale["markets"]["CRYPTO"]["execution"]["reason"] == "DISCOVERY_STALE"
    assert stale["capabilities"]["CRYPTO_EXECUTION"]["status"] == "DATA_NOT_READY"
    # a withdrawal-capable key: refused, never "missing a permission"
    wd = account_market_status(broker="bybit", environment="DEMO", permissions={**PERMS, "WITHDRAW": True},
                               instruments=ins)
    assert wd["markets"]["CRYPTO"]["execution"]["reason_class"] == "ACCOUNT_NOT_ELIGIBLE"
    assert wd["permission_health"]["WITHDRAW"] == {"state": "PRESENT_KEY_REFUSED", "use": "NEVER_REQUIRED"}
    for st_ in (demo, live, b, none, stale, wd):
        for fam in st_["markets"].values():
            assert fam["execution"]["reason_class"] in REASON_CLASSES


def test_permission_health_is_abstract():
    from app.activation.market import permission_health

    ph = permission_health({"TRADE": True, "INTERNAL_TRANSFER": False})
    assert ph["TRADE"]["state"] == "VERIFIED" and ph["INTERNAL_TRANSFER"]["state"] == "MISSING"
    assert ph["READ_ACCOUNT"]["state"] == "UNVERIFIED" and ph["WITHDRAW"]["use"] == "NEVER_REQUIRED"
    assert set(ph) == {"READ_ACCOUNT", "READ_POSITIONS", "READ_ORDERS", "TRADE", "INTERNAL_TRANSFER", "WITHDRAW"}


def _account(db, acc, user, broker="bybit", env="demo", perms=None):
    from shared_lib.core.security.broker_security import encrypt_credentials

    with db.connect() as c:
        c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,label,status,environment,created_at,"
                  "updated_at,active_credential_version) VALUES (?,?,?,?,?,?,?,?,?,1)",
                  (acc, user, broker, "crypto", "t", "connected", env, NOW, NOW))
        c.execute("INSERT INTO broker_credentials_v2 (account_id,version,status,encrypted_blob,created_at,updated_at,"
                  "permissions_json) VALUES (?,?,?,?,?,?,?)",
                  (acc, 1, "active", encrypt_credentials({"api_key": f"{user}-{acc}-APIKEY", "api_secret":
                                                          f"{user}-SECRET-VALUE"}), NOW, NOW,
                   json.dumps(perms) if perms is not None else None))


SAFE_EVIDENCE = normalize_bybit_query_api({"readOnly": 0, "permissions": {
    "ContractTrade": ["Order", "Position"], "Wallet": ["AccountTransfer"]}}).to_dict()


class FakeAdapter(TransferAdapter):
    broker = "bybit"
    client_supplied_id = True

    def __init__(self, topo="CLASSIC", balances=None, submit=None, lookup=None):
        self._topo = topology_for("bybit", topo) if isinstance(topo, str) else topo
        self.balances = balances if balances is not None else {("FUND", "USDT"): D("1000"),
                                                               ("CONTRACT", "USDT"): D("500")}
        self.submit_behavior = submit or (lambda **k: SubmitOutcome(S.COMPLETED, k["request_id"], "SUCCESS"))
        self.lookup_behavior = lookup or (lambda **k: LookupOutcome(False))
        self.submits = []

    def account_mode(self):
        return getattr(self._topo, "account_mode", None)

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
        return []


def _svc(db, ad):
    return InternalTransferService(db, adapter_factory=lambda auth: ad)


def test_account_status_is_per_account_isolated_and_secret_free(db):
    from app.activation.account_status import account_status

    _account(db, "acc_a1", "alice", perms=SAFE_EVIDENCE)
    _account(db, "acc_a2", "alice", perms=SAFE_EVIDENCE)
    _account(db, "acc_b", "bob", perms=SAFE_EVIDENCE)
    ad = FakeAdapter(submit=lambda **k: (_ for _ in ()).throw(TimeoutError("read timed out")))
    svc = _svc(db, ad)
    unknown = svc.request_transfer(TransferIntent("alice", "acc_a1", "USDT", D("10"), "FUND", "CONTRACT",
                                                  "k-iso-000001"))
    assert unknown["status"] == "UNKNOWN"
    a1 = account_status(db, user_id="alice", account_id="acc_a1", service=svc, include_balances=True)
    a2 = account_status(db, user_id="alice", account_id="acc_a2", service=svc)
    assert [t["id"] for t in a1["current_transfers"]] == [unknown["id"]] and a2["current_transfers"] == []
    assert a1["capabilities"]["INTERNAL_TRANSFER"]["status"] == "TRANSFER_PENDING"
    # the other account is independent: no pending transfer, but its mode was not read -> no route assumed
    assert a2["capabilities"]["INTERNAL_TRANSFER"]["reasons"] == ["ACCOUNT_TOPOLOGY_UNKNOWN"]
    assert account_status(db, user_id="alice", account_id="acc_a2", service=svc,
                          include_balances=True)["capabilities"]["INTERNAL_TRANSFER"]["state"] == "ACTIVE"
    assert a1["capital_buckets"]["account_mode"] == "CLASSIC" and a1["topology_class"] == "SEGMENTED"
    assert a2["capital_buckets"]["reason"] == "ACCOUNT_TOPOLOGY_UNKNOWN"  # mode not read without a live read
    for other in (("bob", "acc_a1"), ("alice", "acc_b"), ("mallory", "acc_a1")):
        with pytest.raises(TransferAccessError):
            account_status(db, user_id=other[0], account_id=other[1], service=svc)
    blob = json.dumps([a1, a2], default=str)
    for secret in ("APIKEY", "SECRET-VALUE", "encrypted_blob", "signature"):
        assert secret not in blob


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
    from app.api.multi_asset_status import get_db
    from app.core.config import settings

    with patch.object(settings, "DATABASE_URL", "sqlite:///" + DB().path):
        from app.main import app
    ad = FakeAdapter()
    app.dependency_overrides[get_transfer_service] = lambda: _svc(db, ad)
    app.dependency_overrides[get_transfer_reconciler] = lambda: TransferReconciler(db, adapter_factory=lambda a: ad)
    app.dependency_overrides[get_db] = lambda: db
    try:
        yield TestClient(app), ad
    finally:
        for dep in (get_transfer_service, get_transfer_reconciler, get_db):
            app.dependency_overrides.pop(dep, None)


def test_market_status_and_transfer_routes_are_404_for_other_users(db, api, monkeypatch):
    client, ad = api
    monkeypatch.setattr("app.activation.account_status.refresh_if_stale", lambda *a, **k: None)  # no network
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    alice = {"Authorization": f"Bearer {_token('alice')}"}
    bob = {"Authorization": f"Bearer {_token('bob')}"}
    ok = client.get("/api/v1/brokers/acc_a/market-status?instruments_family=FX", headers=alice)
    assert ok.status_code == 200 and "permission_health" in ok.json() and ok.json()["instruments"] == []
    assert client.get("/api/v1/brokers/acc_a/market-status?instruments_family=NOPE", headers=alice).status_code == 422
    for method, path in (("get", "/api/v1/brokers/acc_a/market-status"),
                         ("post", "/api/v1/brokers/acc_a/market-discovery/sync"),
                         ("get", "/api/v1/brokers/acc_a/transfer-settings"),
                         ("get", "/api/v1/brokers/acc_a/internal-transfers"),
                         ("get", "/api/v1/brokers/nonexistent/market-status")):
        assert getattr(client, method)(path, headers=bob).status_code == 404, path
    r = client.post("/api/v1/brokers/acc_a/internal-transfers", headers={**bob, "Idempotency-Key": "bob-key-0001"},
                    json={"asset": "USDT", "amount": "1", "source_wallet": "FUND", "destination_wallet": "CONTRACT"})
    assert r.status_code == 404 and ad.submits == []
    # policy fields validated
    bad = client.put("/api/v1/brokers/acc_a/transfer-settings", headers=alice, json={"max_transfer_pct": "1.5"})
    assert bad.status_code == 422
    assert client.put("/api/v1/brokers/acc_a/transfer-settings", headers=alice,
                      json={"allowed_routes": ["FUND"]}).status_code == 422
    good = client.put("/api/v1/brokers/acc_a/transfer-settings", headers=alice,
                      json={"allowed_routes": ["fund -> contract"], "max_transfer_pct": "0.5",
                            "emergency_disabled": True})
    assert good.status_code == 200 and good.json()["allowed_routes"] == ["FUND->CONTRACT"]
    assert good.json()["emergency_disabled"] is True


# ═══════════════════════════════ SECTION 9 ═══════════════════════════════

def test_topology_classes_and_unknown_modes():
    assert topology_class("bybit", "UNIFIED") == TopologyClass.UNIFIED
    assert topology_class("bybit", "CLASSIC") == TopologyClass.SEGMENTED
    assert topology_class("bybit", None) == TopologyClass.UNKNOWN and topology_for_account("bybit", None) is None
    assert topology_class("kraken", "UNIFIED") == TopologyClass.UNSUPPORTED
    assert topology_class("bingx", None) == TopologyClass.SEGMENTED  # FUND + perpetual account
    assert topology_class("binance", None) == TopologyClass.SEGMENTED  # classic: MAIN / FUNDING / UMFUTURE
    # a segmented account whose one derivatives wallet holds several families needs NO transfer between them
    from app.trading_intelligence.capital.planner import AccountCapitalState, CapitalSettings, plan_capital

    fx = plan_capital(state=AccountCapitalState("a", "USDT", topology_for("bingx"), {"PFUTURES": D("500")}),
                      product="FX_PERPETUAL", required=D("100"), settings=CapitalSettings(), plan_key="k")
    assert fx.outcome == "LOGICAL_REALLOCATION" and fx.transfer is None
    routes = topology_for("binance").to_dict()["routes"]
    assert routes and all(r["fee"] is None and r["min_amount"] is None and
                          r["facts_status"] == "UNAVAILABLE_FROM_VENUE_API" for r in routes)  # never 0


def test_planner_blocks_unknown_topology_and_unavailable_routes():
    from app.trading_intelligence.capital.planner import (AccountCapitalState, CapitalSettings, capital_readiness,
                                                          plan_capital)

    auto = CapitalSettings(mode="AUTOMATED_INTERNAL_REALLOCATION", auto_rebalance_enabled=True, authorized=True)
    unknown = plan_capital(state=AccountCapitalState("acc", "USDT", None, {}), product="FX_PERPETUAL",
                           required=D("100"), settings=auto, plan_key="k")
    assert unknown.outcome == "TRANSFER_UNSUPPORTED" and unknown.reason_codes == ("ACCOUNT_TOPOLOGY_UNKNOWN",)
    assert not capital_readiness(unknown).ready
    state = AccountCapitalState("acc", "USDT", topology_for("binance"),
                                {"UMFUTURE": D("10"), "FUNDING": D("5000"), "MAIN": D("5000")},
                                transfer_capability_usable=True)
    closed = plan_capital(state=state, product="CRYPTO_PERPETUAL", required=D("300"),
                          settings=CapitalSettings(**{**auto.__dict__, "allowed_routes": ("SPOT->FUNDING",)}),
                          plan_key="k")
    assert closed.outcome == "TRANSFER_UNSUPPORTED" and "INTERNAL_TRANSFER_ROUTE_UNAVAILABLE" in closed.reason_codes
    allowed = plan_capital(state=state, product="CRYPTO_PERPETUAL", required=D("300"),
                           settings=CapitalSettings(**{**auto.__dict__, "allowed_routes": ("MAIN->UMFUTURE",)}),
                           plan_key="k")
    assert allowed.transfer.source_wallet == "MAIN"
    capped = plan_capital(state=state, product="CRYPTO_PERPETUAL", required=D("300"),
                          settings=CapitalSettings(**{**auto.__dict__, "manual_approval_threshold": D("100")}),
                          plan_key="k")
    assert capped.transfer.auto_submit is False and "MANUAL_APPROVAL_REQUIRED" in capped.reason_codes
    halted = plan_capital(state=state, product="CRYPTO_PERPETUAL", required=D("300"),
                          settings=CapitalSettings(**{**auto.__dict__, "emergency_disabled": True}), plan_key="k")
    assert halted.transfer.auto_submit is False and "AUTOMATION_EMERGENCY_DISABLED" in halted.reason_codes


def _auto(db, **extra):
    InternalTransferService(db).store.save_settings(
        user_id="alice", broker_account_id="acc_a",
        values={"mode": "AUTOMATED_INTERNAL_REALLOCATION", "auto_rebalance_enabled": True, **extra})


def _planned(key="cap-000000001", amount="100", pre=None, src="FUND", dst="CONTRACT"):
    facts = {k: True for k in TRANSFER_PRECONDITIONS} if pre is None else pre
    return TransferIntent("alice", "acc_a", "USDT", D(amount), src, dst, key, TransferOrigin.CAPITAL_PLANNER,
                          {"preconditions": facts})


@pytest.mark.parametrize("settings,intent_kw,reason", [
    ({}, {"pre": {}}, "TRANSFER_PRECONDITIONS_NOT_ESTABLISHED"),                 # no admitted opportunity
    ({}, {"pre": {**{k: True for k in TRANSFER_PRECONDITIONS}, "hard_risk_accepts": False}},
     "TRANSFER_PRECONDITIONS_NOT_ESTABLISHED"),                                  # hard risk failed -> no transfer
    ({"auto_rebalance_enabled": False}, {}, "AUTOMATION_DISABLED"),
    ({"emergency_disabled": True}, {}, "AUTOMATION_EMERGENCY_DISABLED"),
    ({"manual_approval_threshold": "50"}, {}, "MANUAL_APPROVAL_REQUIRED"),
    ({"allowed_routes": ["CONTRACT->FUND"]}, {}, "ROUTE_NOT_ALLOWED"),
    ({"max_transfer_pct": "0.05"}, {}, "MAX_TRANSFER_PCT_EXCEEDED"),              # 100 > 5% of 1000
    ({"max_destination_balance": "550"}, {}, "DESTINATION_CAP_EXCEEDED"),         # 500 + 100 > 550
    ({"max_transfer_amount": "50"}, {}, "MAX_TRANSFER_AMOUNT_EXCEEDED"),
    ({"daily_transfer_limit": "10"}, {}, "DAILY_TRANSFER_LIMIT_EXCEEDED"),
    ({"min_funding_balance": "950"}, {}, "MIN_FUNDING_BALANCE"),                  # minimum source reserve
    ({}, {"amount": "5000"}, "INSUFFICIENT_TRANSFERABLE_BALANCE"),
])
def test_auto_capital_routing_policy_blocks_before_any_submission(db, settings, intent_kw, reason):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    _auto(db, **settings)
    ad = FakeAdapter()
    row = _svc(db, ad).request_transfer(_planned(**intent_kw))
    assert (row["status"], row["failure_reason"]) == ("BLOCKED", reason) and ad.submits == []


def test_kill_switch_and_unknown_destination_balance_block(db):
    from app.trading_intelligence.governance.promotion import PromotionGovernance

    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    _auto(db, max_destination_balance="10000")
    ad = FakeAdapter(balances={("FUND", "USDT"): D("1000")})  # CONTRACT balance unreadable
    assert _svc(db, ad).request_transfer(_planned(key="cap-dest-00001"))["failure_reason"] == \
        "DESTINATION_BALANCE_UNAVAILABLE"
    PromotionGovernance(db).set_kill_switch(True, reason="test", actor_ref="op_test", scope="acc_a")
    ad2 = FakeAdapter()
    assert _svc(db, ad2).request_transfer(_planned(key="cap-kill-00001"))["failure_reason"] == "CATI_KILL_SWITCH_ACTIVE"
    assert ad2.submits == []


def test_authorised_planner_transfer_is_submitted_once_and_funds_only_when_confirmed(db):
    from app.trading_intelligence.capital.planner import (AccountCapitalState, CapitalSettings, capital_readiness,
                                                          plan_capital, submit_if_authorised)

    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    _auto(db)
    ad = FakeAdapter(submit=lambda **k: SubmitOutcome(S.CONFIRMATION_PENDING, k["request_id"], "PENDING"))
    svc = _svc(db, ad)
    settings = CapitalSettings.from_store(svc.store.settings(user_id="alice", broker_account_id="acc_a"))
    state = AccountCapitalState("acc_a", "USDT", topology_for("bybit", "CLASSIC"),
                                {"FUND": D("1000"), "CONTRACT": D("10"), "SPOT": None}, transfer_capability_usable=True)
    plan = plan_capital(state=state, product="CRYPTO_PERPETUAL", required=D("100"), settings=settings, plan_key="opp1")
    assert plan.transfer.auto_submit
    facts = {k: True for k in TRANSFER_PRECONDITIONS}
    row = submit_if_authorised(plan, service=svc, user_id="alice", preconditions=facts)
    again = submit_if_authorised(plan, service=svc, user_id="alice", preconditions=facts)
    assert row["id"] == again["id"] and len(ad.submits) == 1  # same logical transfer -> one broker transfer
    assert not capital_readiness(plan, transfer_status=row["status"]).ready  # accepted is not confirmed
    ad.lookup_behavior = lambda **k: LookupOutcome(True, S.COMPLETED, k["broker_transfer_id"], "SUCCESS")
    TransferReconciler(db, adapter_factory=lambda a: ad).reconcile_account(user_id="alice", broker_account_id="acc_a")
    done = svc.store.get(user_id="alice", broker_account_id="acc_a", transfer_id=row["id"])
    assert done["status"] == "COMPLETED" and capital_readiness(plan, transfer_status=done["status"]).ready


def test_ambiguous_submission_is_reconciled_never_retried_and_rejections_resolve(db):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    ad = FakeAdapter(submit=lambda **k: (_ for _ in ()).throw(ConnectionResetError("reset after send")))
    svc = _svc(db, ad)
    intent = TransferIntent("alice", "acc_a", "USDT", D("20"), "FUND", "CONTRACT", "k-amb-000001")
    row = svc.request_transfer(intent)
    assert row["status"] == "UNKNOWN" and len(ad.submits) == 1
    for _ in range(3):  # replays and a worker pass never resubmit
        assert svc.request_transfer(intent)["status"] in ("UNKNOWN", "RECONCILIATION_REQUIRED")
    TransferReconciler(db, adapter_factory=lambda a: ad).reconcile_account(user_id="alice", broker_account_id="acc_a")
    assert len(ad.submits) == 1
    ad.lookup_behavior = lambda **k: LookupOutcome(True, S.FAILED, k["request_id"], "FAILED")
    TransferReconciler(db, adapter_factory=lambda a: ad).reconcile_account(user_id="alice", broker_account_id="acc_a")
    final = svc.store.get(user_id="alice", broker_account_id="acc_a", transfer_id=row["id"])
    assert final["status"] == "FAILED" and final["failure_reason"] == "BROKER_STATUS_FAILED"
    events = [e["event_type"] for e in svc.store.events(user_id="alice", broker_account_id="acc_a",
                                                        transfer_id=row["id"])]
    assert events[-1] == "RECONCILED" and "SUBMIT_OUTCOME_UNKNOWN" in events  # appended, history kept
    assert len(ad.submits) == 1


def test_unknown_topology_blocks_and_no_cross_account_destination(db):
    _account(db, "acc_a", "alice", perms=SAFE_EVIDENCE)
    _account(db, "acc_b", "bob", perms=SAFE_EVIDENCE)
    ad = FakeAdapter(topo=None)
    row = _svc(db, ad).request_transfer(TransferIntent("alice", "acc_a", "USDT", D("5"), "FUND", "CONTRACT",
                                                       "k-topo-00001"))
    assert row["failure_reason"] == "ACCOUNT_MODE_UNKNOWN" and ad.submits == []  # never probed with a transfer
    ad2 = FakeAdapter()
    other = _svc(db, ad2).request_transfer(TransferIntent("alice", "acc_a", "USDT", D("5"), "FUND", "acc_b",
                                                          "k-xacc-00001"))
    assert other["failure_reason"] == "UNKNOWN_DESTINATION_WALLET" and ad2.submits == []
    with pytest.raises(TransferAccessError):
        _svc(db, ad2).request_transfer(TransferIntent("alice", "acc_b", "USDT", D("5"), "FUND", "CONTRACT",
                                                      "k-xacc-00002"))


def test_settings_migration_is_additive_and_old_rows_read_safely(tmp_path):
    from shared_lib.persistence.broker_schema import ensure_broker_schema

    d = DB(path=str(tmp_path / "old.db"))
    with d.connect() as c:  # a pre-Section-9.8 settings table with an existing row
        c.execute("CREATE TABLE broker_transfer_settings (broker_account_id TEXT PRIMARY KEY, user_id TEXT NOT NULL,"
                  " mode TEXT NOT NULL DEFAULT 'MANUAL_TRANSFER', auto_rebalance_enabled INTEGER NOT NULL DEFAULT 0,"
                  " max_transfer_amount TEXT, min_funding_balance TEXT, min_derivatives_reserve TEXT,"
                  " min_free_margin TEXT, asset_allowlist_json TEXT, wallet_allowlist_json TEXT,"
                  " daily_transfer_limit TEXT, authorized_at TEXT, updated_at TEXT NOT NULL)")
        c.execute("INSERT INTO broker_transfer_settings (broker_account_id, user_id, updated_at) VALUES "
                  "('acc','u','x')")
        for t in ("broker_credentials_v2", "broker_accounts", "broker_transfers_cache"):
            c.execute(f"CREATE TABLE IF NOT EXISTS {t} (id TEXT)")
    ensure_broker_schema(d)
    ensure_broker_schema(d)  # idempotent
    with d.connect() as c:
        cols = {r[1] for r in c.execute("PRAGMA table_info(broker_transfer_settings)").fetchall()}
        assert c.execute("SELECT emergency_disabled FROM broker_transfer_settings").fetchone()[0] == 0
    assert {"allowed_routes_json", "max_transfer_pct", "max_destination_balance", "manual_approval_threshold",
            "emergency_disabled"} <= cols
    from app.transfers.store import TransferStore

    s = TransferStore(d).settings(user_id="u", broker_account_id="acc")
    assert s["emergency_disabled"] is False and s["allowed_routes"] is None and s["max_transfer_pct"] is None


# ── withdrawal / cross-broker prohibition: audit the reachable code, not only the behaviour ──

_ROUTING_MODULES = ("app/transfers", "app/trading_intelligence/capital", "app/activation",
                    "app/api/broker_transfers.py", "app/api/multi_asset_status.py",
                    "app/trading_intelligence/execution/boundary.py")


def _sources():
    for rel in _ROUTING_MODULES:
        p = BACKEND / rel
        for f in ([p] if p.is_file() else sorted(p.rglob("*.py"))):
            yield f, f.read_text(encoding="utf-8")


def test_no_withdrawal_or_on_chain_call_is_reachable_from_capital_routing():
    banned_calls = ("withdraw", "deposit_address", "onchain", "on_chain", "blockchain")
    banned_paths = ("/withdraw", "withdraw/apply", "/capital/deposit", "/deposit/address", "sub-member-transfer",
                    "universal-transfer?toEmail")
    for f, src in _sources():
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                fn = node.func
                name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", "")
                assert not any(b in str(name).lower() for b in banned_calls), f"{f}: call {name}"
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                assert not any(b in node.name.lower() for b in banned_calls), f"{f}: def {node.name}"
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert not any(b in node.value for b in banned_paths), f"{f}: literal {node.value[:60]}"
    # the transfer intent / schema cannot even name an external destination
    fields = set(TransferIntent.__dataclass_fields__)
    assert not fields & {"address", "destination_address", "network", "chain", "to_broker", "to_account",
                         "destination_account_id"}


def test_no_code_path_triggers_a_transfer_from_pnl_or_losses():
    """Capital routing funds admitted plans; nothing submits a transfer on a loss / drawdown signal."""
    for f, src in _sources():
        for node in ast.walk(ast.parse(src)):
            if isinstance(node, ast.Call):
                fn = node.func
                if (fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", "")) in (
                        "request_transfer", "submit_if_authorised"):
                    rel = f.relative_to(BACKEND).as_posix()
                    assert rel in ("app/api/broker_transfers.py", "app/trading_intelligence/capital/planner.py"), rel
