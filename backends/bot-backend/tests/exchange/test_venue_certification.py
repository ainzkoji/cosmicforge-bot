"""Step 4.2 -- the adapter certification framework and its register.

Every run here is against a simulated venue held in this file: a stub adapter over an in-memory account. No
real venue is contacted and nothing here certifies one. What is proven is the framework: who may run, what
is recorded, what a run may touch, how it cleans up, and when the register answers CERTIFIED and when it
does not (90 days, version change, failure, revocation, tampering).
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional

import pytest

from app.exchange.platform_adapter import certification as CF
from app.exchange.platform_adapter import contract as C
from shared_lib.persistence import cati_exchange_schema as S
from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

NOW = 1_800_000_000_000
DAY = 86_400_000
SYMBOL = "BTCUSDT"
ACCOUNT = "acct-demo-1"
COMMIT = "0123456789abcdef0123456789abcdef01234567"
STREAM_KEY = "STREAMKEYSECRET0001"
OPEN = (C.NEW, C.PARTIALLY_FILLED)


# ------------------------------------------------------------------------------------ the simulated venue
class SimAccount:
    """The venue's truth about one account. It outlives the adapter object, as a venue outlives a process."""

    def __init__(self, *, mark: float = 60_000.0) -> None:
        self.now, self.mark = NOW, mark
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.by_client: Dict[str, List[str]] = {}
        self.fills: List[C.FillInfo] = []
        self.position = 0.0
        self.entry_price: Optional[float] = None
        self.wallet = 1_000.0
        self.leverage: Dict[str, int] = {}
        self.mutations: List[str] = []
        self.peak_position = 0.0
        self._ids = 0

    def tick(self) -> int:
        self.now += 5
        return self.now

    def next_id(self, prefix: str) -> str:
        self._ids += 1
        return f"{prefix}{self._ids}"


class StubAdapter:
    """A ``VenueAdapter`` over a ``SimAccount``, and the fault injector of its own transport.

    ``behaviour`` switches on the ways a real adapter or venue can differ or be wrong."""
    adapter_version = "stub-adapter-1.0"

    def __init__(self, account: SimAccount, *, venue: str = C.BINANCE, environment: str = C.DEMO, **behaviour: Any):
        self.account, self.venue, self.environment = account, venue, environment
        self.b: Dict[str, Any] = dict(
            amend_in_place=False, multiple_stops=True, user_stream=True, one_way=True, native_stop=True,
            partial_fill=None, duplicate="RETURN", stop_shift=0.0, venue_removes_stop=False, cancel_final_raises=False,
            swallow_rate_limit=False, clock_offset_ms=0, flat_record=False, fail_on={}, min_notional=100.0,
            position_mode="ONE_WAY")
        unknown = set(behaviour) - set(self.b)
        assert not unknown, unknown
        self.b.update(behaviour)
        self._lose_ack = self._interrupt = False
        self._rate_limit: Optional[float] = None
        self._skew = 0

    # -- fault injector ------------------------------------------------------------------------------------
    def lose_next_acknowledgment(self) -> None:
        self._lose_ack = True

    def interrupt_next_request(self) -> None:
        self._interrupt = True

    def rate_limit_next_request(self, retry_after_s: float) -> None:
        self._rate_limit = retry_after_s

    def skew_clock(self, offset_ms: int) -> None:
        self._skew = offset_ms

    def reduce_position_externally(self, symbol: str, quantity: float) -> None:
        a = self.account
        a.position = round(a.position - quantity, 9)
        a.fills.append(C.FillInfo(self.venue, symbol, a.next_id("t"), "external-order", None, C.SELL, quantity, a.mark,
                                  0.0, "USDT", 0.0, False, a.tick()))

    # -- the transport every call passes through -----------------------------------------------------------
    def _gate(self, name: str, *, signed: bool = True) -> None:
        failure = self.b["fail_on"].get(name)
        if failure is not None:
            raise failure
        if self._rate_limit is not None:
            wait, self._rate_limit = self._rate_limit, None
            if not self.b["swallow_rate_limit"]:
                raise C.AdapterError(C.RATE_LIMITED, venue=self.venue, retry_after_s=wait, http_status=429)
        if self._interrupt:
            self._interrupt = False
            raise C.AdapterError(C.VENUE_UNAVAILABLE, venue=self.venue)
        if self._skew and signed:
            raise C.AdapterError(C.TIMESTAMP_OUT_OF_SYNC, venue=self.venue, venue_code="-1021", http_status=400)

    def _answered(self, value: Any) -> Any:
        if self._lose_ack:
            self._lose_ack = False
            raise C.AdapterError(C.NETWORK_UNCERTAIN, venue=self.venue, uncertain=True)
        return value

    # -- reads ---------------------------------------------------------------------------------------------
    def capabilities(self) -> C.AdapterCapabilities:
        b = self.b
        return C.AdapterCapabilities(
            venue=self.venue, environment=self.environment, native_stop_market=b["native_stop"],
            native_take_profit=True, reduce_only=True, close_position_stop=True, stop_amend_in_place=b["amend_in_place"],
            multiple_stops_per_position=b["multiple_stops"], one_way_mode=b["one_way"], hedge_mode=False,
            user_stream=b["user_stream"], demo_environment=True, client_order_id_max_length=36)

    def server_time(self) -> C.ServerTime:
        self._gate("server_time", signed=False)
        return C.ServerTime(self.account.now + self.b["clock_offset_ms"], self.account.now)

    def instrument_rules(self, symbols=None):
        self._gate("instrument_rules", signed=False)
        return {SYMBOL: C.InstrumentRules(
            venue=self.venue, environment=self.environment, symbol=SYMBOL, venue_symbol=SYMBOL, status=C.TRADING,
            venue_status="TRADING", base_asset="BTC", quote_asset="USDT", margin_asset="USDT", price_tick=0.1,
            quantity_step=0.001, min_quantity=0.001, max_quantity=1000.0, min_notional=self.b["min_notional"],
            max_leverage=125.0, contract_size=1.0, fetched_at_ms=self.account.now)}

    def balance(self) -> C.AccountBalance:
        self._gate("balance")
        wallet = self.account.wallet
        return C.AccountBalance(self.venue, "USDT", wallet, 0.0, wallet, wallet, self.account.now)

    def positions(self, symbol=None):
        self._gate("positions")
        a = self.account
        if a.position == 0 and not self.b["flat_record"]:
            return ()
        side = C.LONG if a.position > 0 else C.SHORT if a.position < 0 else C.FLAT
        return (C.PositionInfo(self.venue, SYMBOL, side, abs(a.position), a.entry_price, a.mark, 0.0,
                               float(a.leverage.get(SYMBOL, 20)), "CROSSED", self.b["position_mode"], a.now),)

    def _info(self, order_id: str) -> C.OrderInfo:
        o = self.account.orders[order_id]
        return C.OrderInfo(self.venue, o["symbol"], o["client_id"], order_id, o["status"], o["status"], o["side"],
                           o["type"], o["quantity"], o["filled"], o["average"], o["stop_price"], o["reduce_only"],
                           False, o["updated"])

    def open_orders(self, symbol=None):
        self._gate("open_orders")
        return tuple(self._info(i) for i, o in self.account.orders.items() if o["status"] in OPEN)

    def order(self, symbol, *, client_order_id=None, venue_order_id=None):
        self._gate("order")
        ids = self.account.by_client.get(str(client_order_id).lower(), []) if client_order_id else [venue_order_id]
        if not ids or ids[0] not in self.account.orders:
            raise C.AdapterError(C.ORDER_NOT_FOUND, venue=self.venue)
        return self._info(ids[0])

    def fills(self, symbol, *, since_ms=None, venue_order_id=None):
        self._gate("fills")
        return tuple(f for f in self.account.fills if venue_order_id in (None, f.venue_order_id))

    def fee_rates(self, symbol):
        return C.FeeRates(self.venue, symbol, 0.0002, 0.0005, "SCHEDULE")

    def funding(self, symbol):
        self._gate("funding", signed=False)
        return C.FundingInfo(self.venue, symbol, 0.0001, None, 480, self.account.mark, self.account.now)

    def rate_limit(self) -> C.RateLimitObservation:
        return C.RateLimitObservation(self.venue, 12.0, 2400.0, 60, None, None, self.account.now)

    def user_stream(self) -> C.UserStreamSpec:
        self._gate("user_stream")
        return C.UserStreamSpec(self.venue, f"wss://stream.invalid/ws/{STREAM_KEY}", "LISTEN_KEY_IN_URL", 1800, 3600)

    # -- mutations -----------------------------------------------------------------------------------------
    def set_leverage(self, symbol, leverage):
        self._gate("set_leverage")
        self.account.mutations.append("set_leverage")
        self.account.leverage[symbol] = int(leverage)
        return self._answered(int(leverage))

    def _new_order(self, request: C.OrderRequest, status: str, filled: float = 0.0, stop: Optional[float] = None) -> str:
        a = self.account
        order_id = a.next_id("o")
        a.orders[order_id] = dict(
            symbol=request.symbol, client_id=request.client_order_id.lower(), status=status, side=request.side,
            type=request.order_type, quantity=request.quantity, filled=filled, average=a.mark if filled else None,
            stop_price=stop, reduce_only=request.reduce_only, updated=a.tick())
        a.by_client.setdefault(request.client_order_id.lower(), []).append(order_id)
        return order_id

    def _trade(self, order_id: str, side: str, quantity: float) -> None:
        a = self.account
        a.position = round(a.position + (quantity if side == C.BUY else -quantity), 9)
        a.peak_position = max(a.peak_position, a.position)
        a.entry_price = a.mark if a.position else None
        a.wallet -= 0.01
        a.fills.append(C.FillInfo(self.venue, SYMBOL, a.next_id("t"), order_id, a.orders[order_id]["client_id"], side,
                                  quantity, a.mark, 0.01, "USDT", 0.0, False, a.tick()))
        if a.position == 0 and self.b["venue_removes_stop"]:
            for o in a.orders.values():
                if o["type"] == C.STOP_MARKET and o["status"] in OPEN:
                    o["status"] = C.EXPIRED

    def place_order(self, request: C.OrderRequest) -> C.OrderInfo:
        self._gate("place_order")
        a = self.account
        a.mutations.append(f"place:{request.client_order_id}")
        existing = a.by_client.get(request.client_order_id.lower())
        if existing and self.b["duplicate"] == "RAISE":
            raise C.AdapterError(C.DUPLICATE_CLIENT_ORDER_ID, venue=self.venue)
        if existing and self.b["duplicate"] == "RETURN":
            return self._answered(self._info(existing[0]))
        if request.order_type == C.MARKET and request.reduce_only:
            if a.position <= 0:
                raise C.AdapterError(C.REDUCE_ONLY_REJECTED, venue=self.venue, venue_code="-2022", http_status=400)
            quantity = min(float(request.quantity or 0), a.position)
            order_id = self._new_order(request, C.FILLED, quantity)
            self._trade(order_id, C.SELL, quantity)
        elif request.order_type == C.MARKET:
            fraction = self.b["partial_fill"]
            quantity = float(request.quantity or 0) if fraction is None else round(float(request.quantity or 0) * fraction, 6)
            order_id = self._new_order(request, C.FILLED if fraction is None else C.PARTIALLY_FILLED, quantity)
            self._trade(order_id, C.BUY, quantity)
        elif request.order_type == C.LIMIT:
            order_id = self._new_order(request, C.NEW)
        else:
            stops = [o for o in a.orders.values() if o["type"] == C.STOP_MARKET and o["status"] in OPEN]
            if stops and not self.b["multiple_stops"]:
                raise C.AdapterError(C.UNSUPPORTED, venue=self.venue)
            order_id = self._new_order(request, C.NEW, stop=float(request.stop_price or 0) + self.b["stop_shift"])
        return self._answered(self._info(order_id))

    def cancel_order(self, symbol, *, client_order_id=None, venue_order_id=None):
        self._gate("cancel_order")
        self.account.mutations.append(f"cancel:{client_order_id}")
        ids = self.account.by_client.get(str(client_order_id).lower(), [])
        if not ids:
            raise C.AdapterError(C.ORDER_NOT_FOUND, venue=self.venue)
        order = self.account.orders[ids[0]]
        if order["status"] in OPEN:
            order["status"], order["updated"] = C.CANCELED, self.account.tick()
        elif self.b["cancel_final_raises"]:
            raise C.AdapterError(C.ORDER_NOT_FOUND, venue=self.venue)
        return self._answered(self._info(ids[0]))

    def amend_stop(self, symbol, *, client_order_id, new_stop_price):
        self._gate("amend_stop")
        self.account.mutations.append(f"amend:{client_order_id}")
        ids = self.account.by_client.get(str(client_order_id).lower(), [])
        if not ids or self.account.orders[ids[0]]["status"] not in OPEN:
            raise C.AdapterError(C.ORDER_NOT_FOUND, venue=self.venue)
        self.account.orders[ids[0]]["stop_price"] = float(new_stop_price) + self.b["stop_shift"]
        return self._answered(self._info(ids[0]))


# ----------------------------------------------------------------------------------------------- helpers
@pytest.fixture
def db(tmp_path):
    database = DB(str(tmp_path / "cert.db"))
    ensure_cati_platform_schema(database)
    return database


def permit(**over) -> CF.OperatorPermit:
    base = dict(operator_ref="operator:alice", owner_authorization_ref="owner-authorisation-2026-10-09-001",
                venue=C.BINANCE, environment=C.DEMO, account_ref=ACCOUNT,
                authorized_scope="DEMO adapter certification, BTCUSDT, minimum size", symbol=SYMBOL, max_notional=1_000.0,
                issued_at_ms=NOW - 1_000, expires_at_ms=NOW + 3_600_000, allow_orders=True)
    return CF.OperatorPermit(**{**base, **over})


def later(at: int) -> SimAccount:
    """A flat account whose clock stands at ``at``: a run is identified by who ran it, on what, and when."""
    account = SimAccount()
    account.now = at
    return account


def run(db, account: Optional[SimAccount] = None, *, faults: bool = True, factory: bool = True,
        account_ref: str = ACCOUNT, the_permit: Optional[CF.OperatorPermit] = None, venue: str = C.BINANCE,
        environment: str = C.DEMO, **behaviour):
    account = account or SimAccount()
    adapter = StubAdapter(account, venue=venue, environment=environment, **behaviour)
    runner = CF.CertificationRunner(
        db, adapter, account_ref=account_ref, permit=the_permit or permit(venue=venue, environment=environment),
        source_commit=COMMIT, clock=account.tick, sleep=lambda seconds: None, faults=adapter if faults else None,
        adapter_factory=(lambda: StubAdapter(account, venue=venue, environment=environment, **behaviour))
        if factory else None)
    return runner.run(), account, adapter


def statuses(result: CF.CertificationResult) -> Dict[str, str]:
    return {check_id: entry["status"] for check_id, entry in result.checks.items()}


def reason(result: CF.CertificationResult, check_id: str) -> str:
    return result.checks[check_id]["reason"]


def assert_account_restored(account: SimAccount) -> None:
    assert account.position == 0
    assert [o for o in account.orders.values() if o["status"] in OPEN] == []


# ------------------------------------------------------------------------------------------- the register
def test_every_venue_and_environment_starts_not_certified(db):
    matrix = CF.certification_matrix(db, NOW)
    assert set(matrix) == {f"{v}/{e}" for v in C.VENUES for e in C.ENVIRONMENTS} and len(matrix) == 6
    assert {s.status for s in matrix.values()} == {CF.NOT_CERTIFIED}
    assert {s.reason for s in matrix.values()} == {"NO_CERTIFICATION_EVIDENCE"}
    for venue in C.VENUES:
        for environment in C.ENVIRONMENTS:
            assert CF.adapter_certified(db, venue, environment, NOW) == (
                False, "ADAPTER_NOT_CERTIFIED:NO_CERTIFICATION_EVIDENCE")
    with pytest.raises(ValueError):
        CF.certification_status(db, "kraken", C.DEMO, NOW)


def test_the_catalogue_covers_every_required_test_and_is_the_same_for_every_venue():
    covered = {number for spec in CF.CATALOGUE for number in spec.assignment_tests}
    assert covered == set(range(1, 21))
    assert len(set(CF.CHECK_IDS)) == len(CF.CHECK_IDS) == 30
    assert all(CF.catalogue(v, e) is CF.CATALOGUE for v in C.VENUES for e in C.ENVIRONMENTS)
    needed = {check for _, checks in CF.OPERATIONS.values() for check in checks}
    assert needed <= set(CF.CHECK_IDS)
    assert {s.check_id for s in CF.CATALOGUE if s.scope != CF.CORE} == {"USER_STREAM_SPEC"}
    with pytest.raises(ValueError):
        CF.catalogue("kraken", C.DEMO)


def test_a_complete_green_run_is_certified_for_ninety_days_and_leaves_the_account_as_it_found_it(db):
    result, account, _ = run(db)
    assert statuses(result) == {check_id: CF.PASS for check_id in CF.CHECK_IDS}
    assert (result.outcome, result.recorded_status, result.failure_reasons) == (CF.PASSED, CF.CERTIFIED, ())
    assert result.cleanup_complete and set(result.operations) == set(CF.OPERATIONS)
    assert_account_restored(account)
    status = CF.certification_status(db, C.BINANCE, C.DEMO, account.now)
    assert status.certified and status.run_id == result.run_id and status.adapter_version == "stub-adapter-1.0"
    assert status.contract_version == C.CONTRACT_VERSION
    assert status.expires_at_ms == result.expires_at_ms == status.completed_at_ms + 90 * DAY
    assert CF.adapter_certified(db, C.BINANCE, C.DEMO, account.now) == (True, "")
    # a certificate is for one venue and one environment
    assert CF.certification_status(db, C.BINANCE, C.LIVE, account.now).status == CF.NOT_CERTIFIED
    assert CF.certification_status(db, C.BYBIT, C.DEMO, account.now).status == CF.NOT_CERTIFIED


def test_the_evidence_of_a_run_is_complete_and_holds_no_secret(db):
    result, account, _ = run(db)
    evidence = CF.CertificationRegister(db).run_evidence(result.run_id)
    started, completed = evidence["events"]
    assert (started["event_type"], completed["event_type"]) == (CF.RUN_STARTED, CF.RUN_COMPLETED)
    document = json.loads(completed["document"])
    stored = {  # the fields the owner listed
        "exchange": completed["venue"], "adapter_version": completed["adapter_version"],
        "environment": completed["environment"], "certification_run_id": completed["run_id"],
        "authorized_scope": completed["authorized_scope"], "test_account_reference": completed["account_ref"],
        "started_at": completed["started_at"], "completed_at": completed["completed_at"],
        "test_cases": document["test_cases"], "pass_fail_status": document["outcome"],
        "failure_reasons": document["failure_reasons"], "exchange_capabilities": document["exchange_capabilities"],
        "evidence_hashes": document["evidence_hashes"], "source_commit": completed["source_commit"],
        "expiration_date": completed["expires_at"]}
    assert all(value is not None for value in stored.values())
    assert (stored["exchange"], stored["environment"], stored["test_account_reference"]) == (C.BINANCE, C.DEMO, ACCOUNT)
    assert stored["source_commit"] == COMMIT and completed["operator_ref"] == "operator:alice"
    assert json.loads(started["document"])["owner_authorization_ref"] == "owner-authorisation-2026-10-09-001"
    assert set(stored["evidence_hashes"]) == set(CF.CHECK_IDS) == {c["check_id"] for c in evidence["checks"]}
    assert stored["exchange_capabilities"]["native_stop_market"] is True
    assert CF.CertificationRegister(db).verify_run(result.run_id)
    with db.connect() as conn:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        everything = "".join(repr(tuple(row)) for t in tables for row in conn.execute(f"SELECT * FROM {t}"))
    assert STREAM_KEY not in everything and "wss://" not in everything


def test_every_mutation_is_recorded_before_it_is_sent_and_is_marked_as_a_certification_order(db):
    result, account, _ = run(db)
    actions = CF.CertificationRegister(db).run_evidence(result.run_id)["actions"]
    assert [a["seq"] for a in actions] == list(range(1, len(actions) + 1))
    assert [a["phase"] for a in actions] == ["INTENT", "OUTCOME"] * (len(actions) // 2)
    for intent, outcome in zip(actions[0::2], actions[1::2]):
        assert (intent["action"], intent["client_order_id"]) == (outcome["action"], outcome["client_order_id"])
        assert intent["recorded_at"] < outcome["recorded_at"] and outcome["outcome"] in ("ANSWERED", "REFUSED", "UNCERTAIN")
    # nothing reached the venue unrecorded; the one request recorded and never received is the interrupted one
    assert len(actions) // 2 == len(account.mutations) + 1
    assert [a["outcome"] for a in actions].count("UNCERTAIN") == 1     # the lost acknowledgment
    order_ids = {a["client_order_id"] for a in actions if a["client_order_id"]}
    assert order_ids and all(CF.is_certification_order(i) and len(i) == 16 for i in order_ids)
    assert set(account.by_client) <= {i.lower() for i in order_ids}
    assert CF.is_certification_order("CERT1234") and not CF.is_certification_order("cf-trend-001")
    assert not CF.is_certification_order(None)
    # the exposure of the whole run stayed inside the permit
    assert account.peak_position * account.mark <= permit().max_notional


def test_evidence_older_than_ninety_days_is_expired_and_expired_is_never_certified(db):
    result, account, _ = run(db)
    completed = CF.certification_status(db, C.BINANCE, C.DEMO, account.now).completed_at_ms
    last_valid = completed + 90 * DAY - 1
    assert CF.adapter_certified(db, C.BINANCE, C.DEMO, last_valid) == (True, "")
    expired = CF.certification_status(db, C.BINANCE, C.DEMO, last_valid + 1)
    assert (expired.status, expired.certified, expired.reason) == (CF.EXPIRED, False, "CERTIFICATION_OLDER_THAN_90_DAYS")
    assert expired.operations == () and expired.run_id == result.run_id
    assert CF.adapter_certified(db, C.BINANCE, C.DEMO, last_valid + 1) == (
        False, "ADAPTER_EXPIRED:CERTIFICATION_OLDER_THAN_90_DAYS")
    assert CF.adapter_certified(db, C.BINANCE, C.DEMO, last_valid + 400 * DAY, operation="ENTRY")[0] is False
    # a certificate from the future is a clock fault, not a certificate
    early = CF.certification_status(db, C.BINANCE, C.DEMO, completed - CF.CLOCK_TOLERANCE_MS - 1)
    assert (early.status, early.reason) == (CF.NOT_CERTIFIED, "CERTIFICATION_DATED_IN_THE_FUTURE")
    assert CF.CERTIFICATION_VALIDITY_DAYS == 90


def test_a_changed_adapter_or_contract_version_requires_revalidation(db, monkeypatch):
    _, account, _ = run(db)
    register, at = CF.CertificationRegister(db), account.now
    # the caller's deployed adapter is not the certified one
    assert CF.adapter_certified(db, C.BINANCE, C.DEMO, at, adapter_version="stub-adapter-1.0") == (True, "")
    assert CF.adapter_certified(db, C.BINANCE, C.DEMO, at, adapter_version="stub-adapter-1.1") == (
        False, "ADAPTER_EXPIRED:ADAPTER_VERSION_MISMATCH")
    # a recorded change expires the certificate for everyone, and going back does not bring it back
    assert register.record_adapter_version(C.BINANCE, C.DEMO, adapter_version="stub-adapter-1.0", now_ms=at) is False
    assert CF.adapter_certified(db, C.BINANCE, C.DEMO, at)[0] is True
    assert register.record_adapter_version(C.BINANCE, C.DEMO, adapter_version="stub-adapter-1.1", now_ms=at) is True
    changed = CF.certification_status(db, C.BINANCE, C.DEMO, at)
    assert (changed.status, changed.reason) == (CF.EXPIRED, "ADAPTER_OR_CONTRACT_VERSION_CHANGED")
    register.record_adapter_version(C.BINANCE, C.DEMO, adapter_version="stub-adapter-1.0", now_ms=at + 1)
    assert CF.certification_status(db, C.BINANCE, C.DEMO, at + 1).status == CF.EXPIRED
    assert register.record_adapter_version(C.BYBIT, C.DEMO, adapter_version="x", now_ms=at) is False   # nothing to expire
    # only a new complete green run certifies again
    result, account, _ = run(db, later(at + 10_000))
    assert result.recorded_status == CF.CERTIFIED
    assert CF.adapter_certified(db, C.BINANCE, C.DEMO, account.now) == (True, "")
    # the contract the adapter was certified against is no longer the contract of the code
    monkeypatch.setattr(C, "CONTRACT_VERSION", "2.0.0")
    stale = CF.certification_status(db, C.BINANCE, C.DEMO, account.now)
    assert (stale.status, stale.reason) == (CF.EXPIRED, "CONTRACT_VERSION_CHANGED")


def test_revocation_and_blocking_are_decisive_until_a_new_green_run(db):
    _, account, _ = run(db)
    register = CF.CertificationRegister(db)
    register.revoke(C.BINANCE, C.DEMO, operator_ref="operator:bob", reason="incident 42", now_ms=account.now + 1)
    revoked = CF.certification_status(db, C.BINANCE, C.DEMO, account.now + 2)
    assert (revoked.status, revoked.reason, revoked.certified) == (CF.REVOKED, "incident 42", False)
    assert CF.adapter_certified(db, C.BINANCE, C.DEMO, account.now + 2) == (False, "ADAPTER_REVOKED:incident 42")
    register.block(C.BINGX, C.LIVE, operator_ref="operator:bob", reason="NO_SAFE_ENVIRONMENT", now_ms=NOW)
    blocked = CF.certification_status(db, C.BINGX, C.LIVE, NOW)
    assert (blocked.status, blocked.reason, blocked.certified) == (CF.BLOCKED, "NO_SAFE_ENVIRONMENT", False)
    with pytest.raises(CF.CertificationRefused, match="OPERATOR_REQUIRED"):
        register.revoke(C.BINANCE, C.DEMO, operator_ref="  ", reason="x", now_ms=NOW)
    fresh = later(account.now + 10)
    run(db, fresh)
    assert CF.certification_status(db, C.BINANCE, C.DEMO, fresh.now).certified


def test_the_register_is_append_only_and_detects_tampering(db):
    result, account, _ = run(db)
    for table in (S.CERT_EVENT_TABLE, S.CERT_CHECK_TABLE, S.CERT_ACTION_TABLE):
        for statement in (f"UPDATE {table} SET run_id='x'", f"DELETE FROM {table}"):
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                with db.connect() as conn:
                    conn.execute(statement)
    assert CF.CertificationRegister(db).verify_chain() == (True, "")
    # someone removes the protection and improves a check: the run no longer matches what was sealed
    with db.connect() as conn:
        conn.execute("DROP TRIGGER trg_cati_cert_chk_no_update")
        conn.execute(f"UPDATE {S.CERT_CHECK_TABLE} SET evidence='{{}}' WHERE check_id='ENTRY_FILL'")
    tampered = CF.certification_status(db, C.BINANCE, C.DEMO, account.now)
    assert (tampered.status, tampered.reason) == (CF.NOT_CERTIFIED, "RUN_EVIDENCE_INTEGRITY_FAILED")
    # someone rewrites the register itself: the chain breaks at the event that was touched
    with db.connect() as conn:
        conn.execute("DROP TRIGGER trg_cati_cert_ev_no_update")
        conn.execute(f"UPDATE {S.CERT_EVENT_TABLE} SET expires_at=expires_at+1 WHERE status='CERTIFIED'")
    broken = CF.certification_status(db, C.BINANCE, C.DEMO, account.now)
    assert (broken.status, broken.reason) == (CF.NOT_CERTIFIED, "REGISTER_INTEGRITY_FAILED:EVENT_2")
    assert CF.certification_status(db, C.BYBIT, C.DEMO, account.now).reason == "REGISTER_INTEGRITY_FAILED:EVENT_2"
    assert result.recorded_status == CF.CERTIFIED


def test_the_register_decides_the_outcome_itself_and_never_certifies_live(db):
    register = CF.CertificationRegister(db)
    live = permit(environment=C.LIVE, account_ref="acct-live-1")
    run_id = register.start_run(live, adapter_version="a", source_commit=COMMIT, started_at=NOW)
    for check_id in CF.CHECK_IDS:
        register.record_check(run_id, check_id, CF.PASS, "", {}, NOW, NOW)
    with pytest.raises(CF.CertificationRefused, match="LIVE_CERTIFICATION_NOT_PERMITTED_IN_THIS_BUILD"):
        register.complete_run(run_id, completed_at=NOW + 1, capabilities={})
    assert CF.certification_status(db, C.BINANCE, C.LIVE, NOW + 2).status == CF.NOT_CERTIFIED
    # all but one core check recorded: the caller cannot call that a pass
    demo = register.start_run(permit(), adapter_version="a", source_commit=COMMIT, started_at=NOW)
    for check_id in CF.CHECK_IDS[:-1]:
        register.record_check(demo, check_id, CF.PASS, "", {}, NOW, NOW)
    undecided = register.complete_run(demo, completed_at=NOW + 1, capabilities={"native_stop_market": True})
    assert (undecided.outcome, undecided.recorded_status, undecided.expires_at_ms) == (CF.INCOMPLETE, None, None)
    assert undecided.failure_reasons == ("FINAL_ACCOUNT_STATE:NOT_RUN:NOT_RECORDED",)
    assert CF.certification_status(db, C.BINANCE, C.DEMO, NOW + 2).reason == "ONLY_INCOMPLETE_RUNS"
    with pytest.raises(CF.CertificationRefused, match="RUN_ALREADY_RECORDED"):
        register.complete_run(demo, completed_at=NOW + 2, capabilities={})
    with pytest.raises(CF.CertificationRefused, match="RUN_ALREADY_RECORDED"):
        register.start_run(permit(), adapter_version="a", source_commit=COMMIT, started_at=NOW)
    with pytest.raises(CF.CertificationRefused, match="SOURCE_COMMIT_REQUIRED"):
        register.start_run(permit(), adapter_version="a", source_commit="", started_at=NOW + 5)
    with pytest.raises(ValueError):
        register.record_check(demo, "MADE_UP_CHECK", CF.PASS, "", {}, NOW, NOW)


# --------------------------------------------------------------------------------------- who may run what
@pytest.mark.parametrize("change,refusal", [
    (dict(the_permit=permit(venue=C.BYBIT)), "PERMIT_VENUE_MISMATCH"),
    (dict(account_ref="acct-other"), "PERMIT_ACCOUNT_MISMATCH"),
    (dict(account_ref=""), "PERMIT_ACCOUNT_MISMATCH"),
    (dict(the_permit=permit(expires_at_ms=NOW)), "PERMIT_EXPIRED"),
    (dict(the_permit=permit(issued_at_ms=NOW + 60_000, expires_at_ms=NOW + 120_000)), "PERMIT_NOT_YET_VALID"),
    (dict(the_permit=permit(environment=C.LIVE)), "PERMIT_ENVIRONMENT_MISMATCH"),
    (dict(the_permit=permit(), environment=C.LIVE), "PERMIT_ENVIRONMENT_MISMATCH"),
    # an operator, an authorisation, the right account, a live adapter and a live permit: still refused
    (dict(the_permit=permit(environment=C.LIVE), environment=C.LIVE), "LIVE_CERTIFICATION_NOT_PERMITTED_IN_THIS_BUILD"),
    (dict(the_permit="operator says it is fine"), "OPERATOR_PERMIT_REQUIRED"),
])
def test_a_run_without_an_exactly_matching_permit_is_refused_before_the_venue_is_touched(db, change, refusal):
    account = SimAccount()
    touched: List[str] = []
    original = StubAdapter._gate
    StubAdapter._gate = lambda self, name, **kw: touched.append(name) or original(self, name, **kw)   # type: ignore
    try:
        with pytest.raises(CF.CertificationRefused, match=refusal):
            run(db, account, **change)
    finally:
        StubAdapter._gate = original   # type: ignore
    assert touched == [] and account.mutations == []
    with db.connect() as conn:
        assert conn.execute(f"SELECT COUNT(*) FROM {S.CERT_EVENT_TABLE}").fetchone()[0] == 0


def test_a_permit_names_an_operator_an_authorisation_one_account_and_a_bound():
    for bad in (dict(operator_ref=""), dict(owner_authorization_ref=" "), dict(account_ref=""), dict(authorized_scope=""),
                dict(venue="kraken"), dict(environment="PAPER"), dict(symbol="BTC-USDT"), dict(max_notional=0),
                dict(max_notional=float("inf")), dict(expires_at_ms=NOW - 2_000), dict(leverage=0)):
        with pytest.raises(C.ContractViolation):
            permit(**bad)
    assert permit().allow_orders is True and CF.OperatorPermit.__dataclass_fields__["allow_orders"].default is False
    assert CF.permit_refusal(permit(), object(), ACCOUNT, NOW) == "NOT_A_VENUE_ADAPTER"
    assert CF.permit_refusal(permit(), StubAdapter(SimAccount()), ACCOUNT, NOW) is None
    assert isinstance(StubAdapter(SimAccount()), C.VenueAdapter) and isinstance(StubAdapter(SimAccount()), CF.FaultInjector)


def test_without_permission_for_orders_only_reads_happen_and_nothing_is_certified(db):
    result, account, _ = run(db, the_permit=permit(allow_orders=False))
    assert account.mutations == [] and account.orders == {}
    got = statuses(result)
    assert all(got[s.check_id] == CF.PASS for s in CF.CATALOGUE if s.phase == CF.READ)
    assert all(got[s.check_id] == CF.NOT_RUN for s in CF.CATALOGUE if s.phase != CF.READ)
    assert reason(result, "ENTRY_SUBMIT") == "NOT_REACHED:ORDERS_NOT_ALLOWED_BY_PERMIT"
    assert reason(result, "FLATTEN") == "NO_ORDER_WAS_SENT"
    assert (result.outcome, result.recorded_status, result.operations) == (CF.INCOMPLETE, None, ())
    assert result.cleanup_complete
    status = CF.certification_status(db, C.BINANCE, C.DEMO, account.now)
    assert (status.status, status.reason) == (CF.NOT_CERTIFIED, "ONLY_INCOMPLETE_RUNS")


def test_an_incomplete_run_neither_certifies_nor_withdraws_a_standing_certificate(db):
    first, account, _ = run(db)
    second, probe, _ = run(db, later(account.now + 1_000), the_permit=permit(allow_orders=False))
    assert second.outcome == CF.INCOMPLETE
    status = CF.certification_status(db, C.BINANCE, C.DEMO, probe.now)
    assert status.certified and status.run_id == first.run_id


def test_an_account_with_a_position_or_an_order_on_the_test_symbol_is_never_used(db):
    held = SimAccount()
    held.position, held.entry_price = 0.5, 58_000.0             # somebody's position
    result, account, _ = run(db, held)
    assert account.mutations == [] and account.position == 0.5 and account.orders == {}
    assert (statuses(result)["TEST_SCOPE"], reason(result, "TEST_SCOPE")) == (CF.NOT_RUN, "ACCOUNT_NOT_FLAT_ON_TEST_SYMBOL")
    assert reason(result, "ENTRY_SUBMIT") == "NOT_REACHED:TEST_SCOPE" and result.outcome == CF.INCOMPLETE
    resting = later(NOW + 60_000)
    resting.orders["o-foreign"] = dict(symbol=SYMBOL, client_id="someone", status=C.NEW, side=C.BUY, type=C.LIMIT,
                                       quantity=1.0, filled=0.0, average=None, stop_price=None, reduce_only=False,
                                       updated=NOW)
    result, account, _ = run(db, resting)
    assert account.mutations == [] and account.orders["o-foreign"]["status"] == C.NEW
    assert reason(result, "TEST_SCOPE") == "ACCOUNT_NOT_FLAT_ON_TEST_SYMBOL"


def test_a_test_that_does_not_fit_the_permit_sends_nothing(db):
    result, account, _ = run(db, the_permit=permit(max_notional=400.0))     # two entries of 240 do not fit
    assert account.mutations == [] and reason(result, "TEST_SCOPE") == "MINIMUM_TEST_SIZE_EXCEEDS_PERMIT"
    result, account, _ = run(db, later(NOW + 60_000), the_permit=permit(leverage=200))
    assert account.mutations == [] and reason(result, "TEST_SCOPE") == "LEVERAGE_ABOVE_VENUE_MAXIMUM"


def test_a_failed_read_check_stops_the_run_before_any_order(db):
    result, account, _ = run(db, clock_offset_ms=5_000)
    assert (statuses(result)["TIME_SYNC"], reason(result, "TIME_SYNC")) == (CF.FAIL, "CLOCK_OFFSET_EXCEEDS_LIMIT")
    assert account.mutations == [] and reason(result, "TEST_SCOPE") == "NOT_REACHED:READ_CHECKS_NOT_PASSED"
    assert (result.outcome, result.recorded_status) == (CF.FAILED, CF.FAILED)
    status = CF.certification_status(db, C.BINANCE, C.DEMO, account.now)
    assert status.status == CF.FAILED and "TIME_SYNC:CLOCK_OFFSET_EXCEEDS_LIMIT" in status.reason


# ------------------------------------------------------------------------- what a harness must supply
def test_without_a_fault_injector_and_a_fresh_adapter_the_run_is_incomplete_but_still_cleans_up(db):
    result, account, _ = run(db, faults=False, factory=False)
    got = statuses(result)
    needs = {s.check_id: s.needs for s in CF.CATALOGUE if s.needs}
    assert {c: reason(result, c) for c in needs} == {
        "LOST_ACKNOWLEDGMENT": "FAULT_INJECTOR_REQUIRED", "CONNECTION_INTERRUPTION": "FAULT_INJECTOR_REQUIRED",
        "RATE_LIMIT": "FAULT_INJECTOR_REQUIRED", "CLOCK_DRIFT": "FAULT_INJECTOR_REQUIRED",
        "MANUAL_POSITION_CHANGE": "FAULT_INJECTOR_REQUIRED", "RESTART_WITH_PROTECTED_POSITION": "ADAPTER_FACTORY_REQUIRED"}
    assert all(got[c] == CF.NOT_RUN for c in needs) and all(got[c] == CF.PASS for c in CF.CHECK_IDS if c not in needs)
    assert (result.outcome, result.recorded_status, result.cleanup_complete) == (CF.INCOMPLETE, None, True)
    assert_account_restored(account)
    assert account.peak_position == 0.002                          # one unit: nothing needs a second one
    assert CF.adapter_certified(db, C.BINANCE, C.DEMO, account.now)[0] is False


# --------------------------------------------------------------------------------- venue differences
@pytest.mark.parametrize("behaviour,method,unprotected", [
    (dict(amend_in_place=True), "AMEND_IN_PLACE", False),
    (dict(multiple_stops=True), "CREATE_THEN_CANCEL", False),
    (dict(multiple_stops=False), "CANCEL_THEN_CREATE", True),
])
def test_the_stop_is_replaced_the_way_the_venue_allows_and_the_way_is_recorded(db, behaviour, method, unprotected):
    result, account, _ = run(db, **behaviour)
    assert result.outcome == CF.PASSED
    checks = {c["check_id"]: json.loads(c["evidence"]) for c in CF.CertificationRegister(db).run_evidence(
        result.run_id)["checks"]}
    replaced = checks["PROTECTION_REPLACE"]
    assert (replaced["method"], replaced["unprotected_between"], replaced["protective_stops_open"]) == (
        method, unprotected, 1)
    # the first stop is on the tick grid at 5% under the entry, the second is tighter and still under the mark
    assert checks["PROTECTION_GEOMETRY"]["stop_price"] == 57_000.0 and replaced["stop_price"] == 58_500.0
    assert checks["PROTECTION_GEOMETRY"]["quantity"] == checks["PROTECTION_GEOMETRY"]["position_quantity"] == 0.004
    assert_account_restored(account)


@pytest.mark.parametrize("behaviour", [
    dict(duplicate="RAISE"), dict(venue_removes_stop=True), dict(cancel_final_raises=True), dict(flat_record=True)])
def test_equally_valid_venue_behaviours_all_pass(db, behaviour):
    result, account, _ = run(db, **behaviour)
    assert (result.outcome, result.failure_reasons) == (CF.PASSED, ())
    assert_account_restored(account)


def test_a_partial_fill_is_protected_for_exactly_what_was_filled(db):
    result, account, _ = run(db, partial_fill=0.75)                 # 0.003 of 0.004 fills; the rest is withdrawn
    assert result.outcome == CF.PASSED
    checks = {c["check_id"]: json.loads(c["evidence"]) for c in CF.CertificationRegister(db).run_evidence(
        result.run_id)["checks"]}
    assert checks["ENTRY_FILL"]["partial"] is True and checks["ENTRY_FILL"]["filled_quantity"] == 0.003
    assert checks["ENTRY_FILL"]["status"] == C.CANCELED
    assert checks["PROTECTION_GEOMETRY"]["quantity"] == 0.003
    assert_account_restored(account)


def test_a_venue_without_a_user_stream_is_certified_but_not_for_that_operation(db):
    result, account, _ = run(db, user_stream=False)
    assert result.outcome == CF.PASSED and statuses(result)["USER_STREAM_SPEC"] == CF.NOT_SUPPORTED
    assert "USER_STREAM" not in result.operations
    at = account.now
    assert CF.adapter_certified(db, C.BINANCE, C.DEMO, at) == (True, "")
    assert CF.adapter_certified(db, C.BINANCE, C.DEMO, at, operation="PROTECTIVE_STOP") == (True, "")
    assert CF.adapter_certified(db, C.BINANCE, C.DEMO, at, operation="USER_STREAM") == (
        False, "OPERATION_NOT_CERTIFIED:USER_STREAM")
    # a capability no check exercises is never selectable on the strength of a certificate
    for unexercised in ("TAKE_PROFIT", "HEDGE_MODE", "anything"):
        assert CF.adapter_certified(db, C.BINANCE, C.DEMO, at, operation=unexercised) == (
            False, f"OPERATION_NOT_EXERCISED_BY_CERTIFICATION:{unexercised}")


# ----------------------------------------------------------------------------- adapters that are wrong
def certified(db, account) -> bool:
    return CF.adapter_certified(db, C.BINANCE, C.DEMO, account.now)[0]


def test_an_adapter_that_creates_a_second_order_for_a_repeated_id_fails_and_both_are_flattened(db):
    result, account, _ = run(db, duplicate="BROKEN")
    assert (statuses(result)["IDEMPOTENT_REPEAT"], reason(result, "IDEMPOTENT_REPEAT")) == (
        CF.FAIL, "REPEAT_CREATED_ANOTHER_ORDER")
    assert statuses(result)["PROTECTION_CREATE"] == CF.NOT_RUN
    assert reason(result, "PROTECTION_CREATE") == "NOT_REACHED:IDEMPOTENT_REPEAT"
    assert (result.outcome, result.recorded_status, result.cleanup_complete) == (CF.FAILED, CF.FAILED, True)
    assert account.peak_position == 0.008 and account.peak_position * account.mark <= permit().max_notional
    assert_account_restored(account)                                # the second entry was closed as well
    assert statuses(result)["FLATTEN"] == statuses(result)["FINAL_ACCOUNT_STATE"] == CF.PASS
    assert not certified(db, account)


def test_a_failed_run_supersedes_an_earlier_certificate(db):
    _, first, _ = run(db)
    assert certified(db, first)
    _, second, _ = run(db, later(first.now + 1_000), stop_shift=25.0)
    status = CF.certification_status(db, C.BINANCE, C.DEMO, second.now)
    assert status.status == CF.FAILED and "PROTECTION_GEOMETRY:STOP_GEOMETRY:TRIGGER" in status.reason
    assert_account_restored(second)


@pytest.mark.parametrize("behaviour,check,why", [
    (dict(stop_shift=25.0), "PROTECTION_GEOMETRY", "STOP_GEOMETRY:TRIGGER"),
    (dict(native_stop=False), "PROTECTION_CREATE", "NO_VENUE_HELD_PROTECTIVE_STOP"),
    (dict(one_way=False), "LEVERAGE_AND_MODE", "ONE_WAY_MODE_NOT_AVAILABLE"),
    (dict(position_mode="HEDGE", flat_record=True), "LEVERAGE_AND_MODE", "ACCOUNT_IN_HEDGE_MODE"),
    (dict(swallow_rate_limit=True), "RATE_LIMIT", "RATE_REFUSAL_NOT_SURFACED"),
    (dict(fail_on={"amend_stop": KeyError("venue said something new")}, amend_in_place=True), "PROTECTION_REPLACE",
     "UNEXPECTED_ERROR:KeyError"),
    (dict(fail_on={"fills": C.AdapterError(C.MALFORMED_RESPONSE, venue=C.BINANCE)}), "ENTRY_FILL",
     "ADAPTER_ERROR:MALFORMED_RESPONSE"),
])
def test_a_wrong_or_broken_adapter_fails_its_check_and_is_not_certified(db, behaviour, check, why):
    result, account, _ = run(db, **behaviour)
    assert (statuses(result)[check], reason(result, check)) == (CF.FAIL, why)
    assert (result.outcome, result.recorded_status) == (CF.FAILED, CF.FAILED) and not certified(db, account)
    assert f"{check}:{why}" in result.failure_reasons
    assert result.cleanup_complete                                 # flattening needs only what an order lookup needs
    assert_account_restored(account)


def test_when_cleanup_cannot_finish_the_result_says_so_loudly(db):
    class Outage(StubAdapter):
        """The venue stops answering the moment the entry has traded."""

        def _gate(self, name, **kw):
            if self.account.fills:
                raise C.AdapterError(C.VENUE_UNAVAILABLE, venue=self.venue)
            super()._gate(name, **kw)

    account = SimAccount()
    result = CF.CertificationRunner(db, Outage(account), account_ref=ACCOUNT, permit=permit(), source_commit=COMMIT,
                                    clock=account.tick, sleep=lambda s: None).run()
    assert reason(result, "ORDER_LOOKUP") == "ADAPTER_ERROR:VENUE_UNAVAILABLE"
    assert statuses(result)["FLATTEN"] == CF.FAIL and reason(result, "FLATTEN") == "ADAPTER_ERROR:VENUE_UNAVAILABLE"
    assert result.cleanup_complete is False and account.position > 0          # the test position is still open
    assert "CLEANUP_INCOMPLETE_OPERATOR_ACTION_REQUIRED" in result.failure_reasons
    status = CF.certification_status(db, C.BINANCE, C.DEMO, account.now)
    assert status.status == CF.FAILED and "CLEANUP_INCOMPLETE_OPERATOR_ACTION_REQUIRED" in status.reason
    # the intent of the order that opened it is on record: an operator can see exactly what to close
    actions = CF.CertificationRegister(db).run_evidence(result.run_id)["actions"]
    assert [a["action"] for a in actions if a["phase"] == "INTENT"][-1] == "PLACE_ORDER"


def test_a_refusal_the_adapter_cannot_name_is_not_a_definite_rejection(db):
    unnamed = C.AdapterError(C.UNMAPPED_VENUE_ERROR, venue=C.BINANCE, venue_code="-9999")
    account = SimAccount()
    adapter = StubAdapter(account)
    original = adapter.place_order

    def place(request):
        if request.reduce_only and request.order_type == C.MARKET and account.position == 0:
            account.mutations.append(f"place:{request.client_order_id}")
            raise unnamed
        return original(request)

    adapter.place_order = place   # type: ignore
    result = CF.CertificationRunner(db, adapter, account_ref=ACCOUNT, permit=permit(), source_commit=COMMIT,
                                    clock=account.tick, sleep=lambda s: None, faults=adapter,
                                    adapter_factory=lambda: StubAdapter(account)).run()
    assert reason(result, "VENUE_REJECTION") == "REJECTION_NOT_DEFINITE:UNMAPPED_VENUE_ERROR"
    assert result.outcome == CF.FAILED and account.position == 0


# ------------------------------------------------------------------------- rollback and transactions
class Killed(BaseException):
    """The process is gone: not an error any handler of the run may catch."""


def test_a_run_whose_process_died_is_rolled_back_from_its_recorded_intents(db):
    class Dying(StubAdapter):
        def _gate(self, name, **kw):
            if self.account.fills:
                raise Killed()
            super()._gate(name, **kw)

    account = SimAccount()
    with pytest.raises(Killed):
        CF.CertificationRunner(db, Dying(account), account_ref=ACCOUNT, permit=permit(), source_commit=COMMIT,
                               clock=account.tick, sleep=lambda s: None).run()
    assert account.position == 0.002                               # the entry is open at the venue, nobody is watching
    register = CF.CertificationRegister(db)
    (left_open,) = register.open_runs()
    assert (left_open["venue"], left_open["account_ref"], left_open["document"]["symbol"]) == (C.BINANCE, ACCOUNT, SYMBOL)
    assert CF.certification_status(db, C.BINANCE, C.DEMO, account.now).status == CF.NOT_CERTIFIED
    recover = dict(run_id=left_open["run_id"], clock=account.tick, sleep=lambda s: None)
    # the same rules as a run: an exactly matching permit, and never LIVE
    for wrong, refusal in ((dict(account_ref="acct-other", permit=permit()), "PERMIT_ACCOUNT_MISMATCH"),
                           (dict(account_ref=ACCOUNT, permit=permit(symbol="ETHUSDT")), "PERMIT_DOES_NOT_COVER_THIS_RUN"),
                           (dict(account_ref=ACCOUNT, permit=permit(expires_at_ms=NOW)), "PERMIT_EXPIRED")):
        with pytest.raises(CF.CertificationRefused, match=refusal):
            CF.CertificationRunner.recover(db, StubAdapter(account), **recover, **wrong)
    assert account.position == 0.002
    result = CF.CertificationRunner.recover(db, StubAdapter(account), account_ref=ACCOUNT, permit=permit(), **recover)
    assert_account_restored(account)
    assert statuses(result)["FLATTEN"] == statuses(result)["FINAL_ACCOUNT_STATE"] == CF.PASS
    assert (result.outcome, result.recorded_status, result.cleanup_complete) == (CF.INCOMPLETE, None, True)
    assert register.open_runs() == () and not certified(db, account)
    closing = [a for a in register.run_evidence(result.run_id)["actions"] if a["phase"] == "INTENT"][-1]
    assert CF.is_certification_order(closing["client_order_id"]) and json.loads(closing["document"])["reduce_only"]
    with pytest.raises(CF.CertificationRefused, match="RUN_ALREADY_RECORDED"):
        CF.CertificationRunner.recover(db, StubAdapter(account), account_ref=ACCOUNT, permit=permit(), **recover)
    with pytest.raises(CF.CertificationRefused, match="RUN_NOT_STARTED"):
        CF.CertificationRunner.recover(db, StubAdapter(account), account_ref=ACCOUNT, permit=permit(),
                                       **{**recover, "run_id": "cert_unknown"})


def test_no_database_transaction_is_open_while_the_venue_is_called(db):
    calls: List[str] = []

    class Probing(StubAdapter):
        """Takes the database's write lock from another connection inside every venue call. If the runner
        held a transaction across the call, this could not get the lock and the check would fail."""

        def _gate(self, name, **kw):
            other = sqlite3.connect(db.path, timeout=0.2)
            try:
                other.execute("BEGIN IMMEDIATE")
                other.rollback()
            finally:
                other.close()
            calls.append(name)
            super()._gate(name, **kw)

    account = SimAccount()
    adapter = Probing(account)
    result = CF.CertificationRunner(db, adapter, account_ref=ACCOUNT, permit=permit(), source_commit=COMMIT,
                                    clock=account.tick, sleep=lambda s: None, faults=adapter,
                                    adapter_factory=lambda: Probing(account)).run()
    assert result.outcome == CF.PASSED and len(calls) > 100 and {"place_order", "cancel_order", "fills"} <= set(calls)


# ------------------------------------------------------------------------------------------- the schema
def snapshot(conn, only_own: bool = False) -> List[tuple]:
    rows = [tuple(r) for r in conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name").fetchall()]
    return [r for r in rows if r[2] in OWN_TABLES] if only_own else rows


OWN_TABLES = [getattr(S, name) for name in S.__all__ if name.endswith("_TABLE")]


def database_of_an_older_build(tmp_path) -> str:
    """A database file with every table, index and trigger of the full migration chain except the ones this
    schema module adds: what a build from before Step 4.2 left on disk."""
    source = DB(str(tmp_path / "source.db"))
    migrate(source)
    with source.connect() as conn:
        objects = [tuple(r) for r in conn.execute(
            "SELECT name, tbl_name, sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' "
            "ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 ELSE 2 END, name").fetchall()]
    path = str(tmp_path / "older.db")
    older = sqlite3.connect(path)
    for _name, table, sql in objects:
        if table not in OWN_TABLES:
            older.execute(sql)
    older.execute("INSERT INTO users (id, email, hashed_password, created_at, updated_at) VALUES "
                  "('u1', 'u1@example.test', 'x', 'then', 'then')")
    older.execute("INSERT INTO broker_accounts (id, user_id, broker_id, market_type, status, created_at, updated_at) "
                  "VALUES ('acct-demo-1', 'u1', 'binance', 'crypto', 'connected', 'then', 'then')")
    older.execute("INSERT INTO broker_credentials (account_id, encrypted_blob, updated_at) VALUES "
                  "('acct-demo-1', 'ciphertext', 'then')")
    older.commit()
    older.close()
    return path


def test_the_schema_is_additive_and_idempotent_on_a_database_that_already_has_tables(tmp_path):
    from shared_lib.persistence.cati_platform_schema import ensure_on_connection

    path = database_of_an_older_build(tmp_path)
    raw = sqlite3.connect(path)
    before = snapshot(raw)
    assert len(before) > 100 and snapshot(raw, only_own=True) == [] and len(OWN_TABLES) == 13
    ensure_on_connection(raw)
    raw.commit()
    first = snapshot(raw)
    assert set(before) <= set(first)                                # nothing that existed was changed or dropped
    assert {r[1] for r in first if r[0] == "table"} >= set(OWN_TABLES)
    own = snapshot(raw, only_own=True)
    raw.close()
    database = DB(path)
    result, account, _ = run(database)                             # put rows into the new tables
    assert result.outcome == CF.PASSED
    ensure_cati_platform_schema(database)
    ensure_cati_platform_schema(database)
    with database.connect() as conn:
        assert snapshot(conn, only_own=True) == own                 # applying it again changes nothing
        assert conn.execute("SELECT email FROM users WHERE id='u1'").fetchone()[0] == "u1@example.test"
        assert conn.execute("SELECT status FROM broker_accounts WHERE id='acct-demo-1'").fetchone()[0] == "connected"
        assert conn.execute("SELECT encrypted_blob FROM broker_credentials").fetchone()[0] == "ciphertext"
        triggers = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'").fetchall()}
    assert CF.adapter_certified(database, C.BINANCE, C.DEMO, account.now) == (True, "")
    for tag in ("cati_cert_ev", "cati_cert_chk", "cati_cert_act", "cati_rulechg", "cati_sfills", "cati_srecon"):
        assert {f"trg_{tag}_no_update", f"trg_{tag}_no_delete"} <= triggers
    assert all("IF NOT EXISTS" in statement for statement in S.DDL)
    assert not any(statement.lstrip().upper().startswith(("DROP", "ALTER", "DELETE", "UPDATE")) for statement in S.DDL)


def test_an_interrupted_schema_change_is_completed_by_applying_it_again(tmp_path):
    from shared_lib.persistence.cati_platform_schema import ensure_on_connection

    complete = sqlite3.connect(str(tmp_path / "complete.db"))
    ensure_on_connection(complete)
    for stopped_after in (1, len(S.DDL) // 2, len(S.DDL) - 1):
        interrupted = sqlite3.connect(str(tmp_path / f"interrupted_{stopped_after}.db"))
        for statement in S.DDL[:stopped_after]:                     # the process stopped here
            interrupted.execute(statement)
        interrupted.commit()
        assert snapshot(interrupted, only_own=True) != snapshot(complete, only_own=True)
        ensure_on_connection(interrupted)
        assert snapshot(interrupted) == snapshot(complete)
        interrupted.close()
    complete.close()


def test_the_schema_applies_on_a_fully_migrated_database_and_again(tmp_path):
    database = DB(str(tmp_path / "migrated.db"))
    migrate(database)
    ensure_cati_platform_schema(database)
    with database.connect() as conn:
        first = snapshot(conn)
    migrate(database)
    ensure_cati_platform_schema(database)
    with database.connect() as conn:
        assert snapshot(conn) == first
        names = {r[1] for r in first if r[0] == "table"}
    assert set(OWN_TABLES) <= names and "venue_instruments" in names and "broker_credentials" in names
    assert {s.status for s in CF.certification_matrix(database, NOW).values()} == {CF.NOT_CERTIFIED}
