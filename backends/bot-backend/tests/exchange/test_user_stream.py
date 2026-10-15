"""Step 4.4 -- user-data streams and REST reconciliation: the stream is a hint and REST is the truth.

Everything here runs against scripted frames, a scripted transport and a stub venue on a real SQLite file.
That proves the parsers for the frame shapes written below, and the manager's state machine: connect,
acknowledgement, heartbeat, key renewal, reconnect with bounded backoff, duplicate suppression, gap
detection, reconciliation, the lease and the health other code reads.

It does not prove that a venue sends these frames, and no websocket was opened.
"""
from __future__ import annotations

import gzip
import json
import sqlite3
from collections import deque
from typing import Any, Deque, Dict, List, Optional

import pytest

from app.exchange.platform_adapter import contract as C
from app.exchange.platform_adapter import reconcile as R
from app.exchange.platform_adapter import user_stream as US
from shared_lib.persistence.cati_exchange_schema import STREAM_CURSOR_TABLE, STREAM_RECONCILE_TABLE
from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
from shared_lib.persistence.db import DB

NOW = 1_800_000_000_000
ACCOUNT = "account-1"
SYMBOL = "BTCUSDT"
LISTEN_KEY = "LISTENKEYSECRET0001"
SIGNATURE = "SIGNATURESECRET0001"


# ---------------------------------------------------------------------------------------------- doubles
class ScriptedTransport:
    """Frames are pushed by the test. It refuses a second simultaneous connection, so any code path that
    would open two fails the test that reaches it."""

    def __init__(self) -> None:
        self.inbox: Deque[Any] = deque()
        self.sent: List[Any] = []
        self.is_open = False
        self.connects = 0
        self.refuse_connect = False
        self.answer_pings = True
        self.pings = 0
        self._broken = False

    def connect(self, spec: C.UserStreamSpec) -> None:
        assert not self.is_open, "a second connection was opened while the first was still open"
        if self.refuse_connect:
            raise US.StreamClosed()
        self.is_open, self._broken = True, False
        self.connects += 1
        self.inbox.clear()

    def send(self, frame: Any) -> None:
        if not self.is_open:
            raise US.StreamClosed()
        self.sent.append(frame)

    def receive(self) -> Any:
        if self._broken or not self.is_open:
            self.is_open = False
            raise US.StreamClosed()
        return self.inbox.popleft() if self.inbox else None

    def ping(self) -> None:
        if not self.is_open:
            raise US.StreamClosed()
        self.pings += 1
        if self.answer_pings:
            self.inbox.append(US.HEARTBEAT)

    def close(self) -> None:
        self.is_open = False

    def push(self, *frames: Any) -> None:
        self.inbox.extend(frames)

    def break_connection(self) -> None:
        self._broken = True


class StubVenue:
    """The venue's own truth, read through the four methods the stream code uses."""
    adapter_version = "stub-1"

    def __init__(self, venue: str = C.BINANCE, auth_mode: str = US.LISTEN_KEY_IN_URL) -> None:
        self.venue, self.environment, self.auth_mode = venue, C.DEMO, auth_mode
        self.orders: Dict[str, C.OrderInfo] = {}
        self.position: Dict[str, C.PositionInfo] = {}
        self.trades: List[C.FillInfo] = []
        self.calls: List[tuple] = []
        self.fail: Optional[C.AdapterError] = None
        self.stream_fail: Optional[C.AdapterError] = None
        self.key = LISTEN_KEY
        self.probe = lambda: None       # run inside every venue call

    def user_stream(self) -> C.UserStreamSpec:
        self.calls.append(("user_stream",))
        self.probe()
        if self.stream_fail is not None:
            raise self.stream_fail
        if self.auth_mode == US.AUTH_FRAME:
            frames = ({"op": "auth", "args": ["key-id", NOW, SIGNATURE]},
                      {"op": "subscribe", "args": ["order", "execution", "position", "wallet"]})
            return C.UserStreamSpec(self.venue, "wss://stream.invalid/v5/private", US.AUTH_FRAME, 20, None, frames)
        return C.UserStreamSpec(self.venue, f"wss://stream.invalid/ws/{self.key}", US.LISTEN_KEY_IN_URL, 1800, 3600)

    def _read(self, *call: Any) -> None:
        self.calls.append(call)
        self.probe()
        if self.fail is not None:
            raise self.fail

    def open_orders(self, symbol=None):
        self._read("open_orders")
        return tuple(o for o in self.orders.values() if o.open and symbol in (None, o.symbol))

    def positions(self, symbol=None):
        self._read("positions")
        return tuple(p for p in self.position.values() if symbol in (None, p.symbol))

    def fills(self, symbol, *, since_ms=None, venue_order_id=None):
        self._read("fills", symbol, since_ms)
        return tuple(f for f in self.trades if f.symbol == symbol and (since_ms is None or f.time_ms >= since_ms))

    # what happens at the venue
    def set_order(self, order_id, client_id, status, filled=0.0, *, symbol=SYMBOL, side=C.BUY, at=NOW):
        self.orders[str(order_id)] = C.OrderInfo(
            self.venue, symbol, client_id, str(order_id), status, status, side, C.LIMIT, 0.01, filled, None, None,
            False, False, at)

    def set_position(self, quantity, *, symbol=SYMBOL, side=C.LONG, entry=60_000.0):
        self.position[symbol] = C.PositionInfo(self.venue, symbol, side if quantity else C.FLAT, quantity, entry,
                                               None, None, None, None, "ONE_WAY", NOW)

    def trade(self, trade_id, order_id, quantity, price=60_000.0, *, symbol=SYMBOL, side=C.BUY, at=NOW, client_id=None):
        fill = C.FillInfo(self.venue, symbol, str(trade_id), str(order_id), client_id, side, quantity, price, 0.01,
                          "USDT", 0.0, False, at)
        self.trades.append(fill)
        return fill

    def reads(self, name: str) -> List[tuple]:
        return [c for c in self.calls if c[0] == name]


def binance_order(*, order_id=1001, client_id="c1", symbol=SYMBOL, side="BUY", execution="NEW", status="NEW",
                  last="0", cumulative="0", price="0", trade_id=0, at=NOW, **extra) -> str:
    body = {"s": symbol, "c": client_id, "S": side, "o": "LIMIT", "f": "GTC", "q": "0.010", "p": "60000", "ap": price,
            "sp": "0", "x": execution, "X": status, "i": order_id, "l": last, "z": cumulative, "L": price, "T": at,
            "t": trade_id, "m": False, "R": False, "wt": "CONTRACT_PRICE", "ot": "LIMIT", "ps": "BOTH", "cp": False,
            "rp": "0", **extra}
    return json.dumps({"e": "ORDER_TRADE_UPDATE", "E": at, "T": at, "o": body})


def binance_account(*, amount="0.010", symbol=SYMBOL, at=NOW, position_side="BOTH", wallet="1000.5") -> str:
    return json.dumps({"e": "ACCOUNT_UPDATE", "E": at, "T": at, "a": {
        "m": "ORDER", "B": [{"a": "USDT", "wb": wallet, "cw": wallet, "bc": "0"}],
        "P": [{"s": symbol, "pa": amount, "ep": "60000.0", "bep": "0", "cr": "0", "up": "1.5", "mt": "cross", "iw": "0",
               "ps": position_side}]}})


@pytest.fixture
def db(tmp_path):
    database = DB(str(tmp_path / "stream.db"))
    ensure_cati_platform_schema(database)
    return database


class Harness:
    def __init__(self, db, venue: Optional[StubVenue] = None, *, account: str = ACCOUNT, owner: str = "worker-1",
                 transport: Optional[ScriptedTransport] = None, **config) -> None:
        self.db, self.venue = db, venue or StubVenue()
        self.transport = transport or ScriptedTransport()
        self.fills: List[C.FillInfo] = []
        self.manager = US.UserStreamManager(
            db, self.venue, self.transport, account_ref=account, owner=owner, symbols=[SYMBOL],
            config=US.StreamConfig(**config), on_fill=self.fills.append)
        self.account = account
        self.passes: List[tuple] = []
        run = self.manager.reconciler.run

        def counted(now_ms, **kw):
            result = run(now_ms, **kw)
            self.passes.append((result.trigger, result.status))
            return result

        self.manager.reconciler.run = counted   # type: ignore

    def step(self, at: int) -> US.StreamStatus:
        return self.manager.step(at)

    def reconciliations(self) -> List[tuple]:
        """Every REST pass this manager made, in order: (trigger, status)."""
        return list(self.passes)

    def logged(self) -> List[tuple]:
        """The passes written to the append-only log."""
        with self.db.connect() as conn:
            return [tuple(r) for r in conn.execute(
                f"SELECT trigger_reason, status FROM {STREAM_RECONCILE_TABLE} WHERE account_ref=? ORDER BY rowid",
                (self.account,)).fetchall()]

    def published(self, at: int) -> US.StreamStatus:
        return US.stream_status(self.db, self.venue.venue, self.venue.environment, self.account, at)


# ---------------------------------------------------------------------------------------------- parsers
def test_binance_order_trade_update_becomes_a_fill_and_an_order_update():
    frame = json.loads(binance_order(execution="TRADE", status="PARTIALLY_FILLED", last="0.004", cumulative="0.004",
                                     price="60000.5", trade_id=77, n="0.012", N="USDT", rp="1.25", m=True))
    fill_event, order_event = US.parse_binance_frame(frame)
    assert fill_event.kind == US.FILL and fill_event.fill == C.FillInfo(
        venue=C.BINANCE, symbol=SYMBOL, trade_id="77", venue_order_id="1001", client_order_id="c1", side=C.BUY,
        quantity=0.004, price=60000.5, fee=0.012, fee_asset="USDT", realized_pnl=1.25, is_maker=True, time_ms=NOW)
    assert order_event.order == US.OrderUpdate(C.BINANCE, SYMBOL, "c1", "1001", C.PARTIALLY_FILLED, "PARTIALLY_FILLED",
                                               C.BUY, 0.004, 60000.5, NOW)
    new_only = US.parse_binance_frame(json.loads(binance_order()))
    assert [e.kind for e in new_only] == [US.ORDER] and new_only[0].order.status == C.NEW
    assert new_only[0].order.average_price is None             # "0" is the venue saying nothing was filled
    odd = US.parse_binance_frame(json.loads(binance_order(status="SOMETHING_NEW")))
    assert odd[0].order.status == C.UNKNOWN and odd[0].order.venue_status == "SOMETHING_NEW"


def test_binance_account_update_becomes_balances_and_one_way_positions():
    events = US.parse_binance_frame(json.loads(binance_account(amount="-0.25")))
    assert events[0].balance == US.BalanceUpdate(C.BINANCE, "USDT", 1000.5, NOW)
    assert events[1].position == US.PositionUpdate(C.BINANCE, SYMBOL, C.SHORT, 0.25, 60000.0, 1.5, NOW)
    flat = US.parse_binance_frame(json.loads(binance_account(amount="0")))[1].position
    assert (flat.side, flat.quantity) == (C.FLAT, 0.0)
    # two positions on one symbol cannot be one position of the contract: it is asked about, not parsed
    hedge = US.parse_binance_frame(json.loads(binance_account(position_side="LONG")))
    assert [(e.kind, e.control, e.detail) for e in hedge[1:]] == [(US.CONTROL, US.RECONCILE_HINT, "HEDGE_MODE_POSITION")]


def test_binance_frames_that_are_not_understood_are_hints_and_never_guessed():
    parse = US.parse_binance_frame
    assert parse({"e": "listenKeyExpired", "E": NOW})[0].control == US.LISTEN_KEY_EXPIRED
    assert parse({"e": "ALGO_UPDATE", "E": NOW, "o": {"anything": 1}})[0].detail == "UNPARSED:ALGO_UPDATE"
    assert parse({"e": "TRADE_LITE", "E": NOW}) == [] and parse({"result": None, "id": 1}) == []
    for broken in ({"e": "ORDER_TRADE_UPDATE", "E": NOW}, json.loads(binance_order(S="HOLD")),
                   json.loads(binance_order(execution="TRADE", last="0.1", cumulative="0.1", price="0")),
                   {"e": "ACCOUNT_UPDATE", "E": NOW, "a": {"B": "not a list"}},
                   json.loads(binance_order(z="not a number"))):
        events = parse(broken)
        assert [(e.kind, e.control) for e in events] == [(US.CONTROL, US.RECONCILE_HINT)]
        assert events[0].detail.startswith("MALFORMED:")


def bybit(topic: str, *items: Dict[str, Any], at: int = NOW) -> Dict[str, Any]:
    return {"id": "x", "topic": topic, "creationTime": at, "data": list(items)}


def bybit_execution(**over) -> Dict[str, Any]:
    return {"category": "linear", "symbol": SYMBOL, "execId": "e-1", "execPrice": "60000.5", "execQty": "0.004",
            "execType": "Trade", "execFee": "0.012", "isMaker": False, "orderId": "o-1", "orderLinkId": "c1",
            "side": "Buy", "execTime": str(NOW), "seq": 9001, **over}


def bybit_order(**over) -> Dict[str, Any]:
    return {"category": "linear", "symbol": SYMBOL, "orderId": "o-1", "orderLinkId": "c1", "side": "Buy",
            "orderType": "Limit", "orderStatus": "PartiallyFilled", "qty": "0.010", "cumExecQty": "0.004",
            "avgPrice": "60000.5", "triggerPrice": "", "reduceOnly": False, "closeOnTrigger": False,
            "updatedTime": str(NOW), **over}


def bybit_position(**over) -> Dict[str, Any]:
    return {"category": "linear", "symbol": SYMBOL, "side": "Buy", "size": "0.004", "entryPrice": "60000.5",
            "leverage": "1", "markPrice": "60010", "unrealisedPnl": "0.04", "positionIdx": 0, "seq": 9001,
            "updatedTime": str(NOW), **over}


def test_bybit_answers_and_topics_are_parsed_for_the_linear_category():
    parse = US.parse_bybit_frame
    assert parse({"success": True, "ret_msg": "", "op": "auth", "conn_id": "c"})[0].control == US.AUTH_OK
    assert parse({"success": False, "ret_msg": "bad", "op": "auth"})[0].control == US.AUTH_FAILED
    assert parse({"success": True, "op": "subscribe"})[0].control == US.SUBSCRIBED
    assert parse({"success": False, "op": "subscribe"})[0].control == US.SUBSCRIBE_FAILED
    assert parse({"op": "pong", "args": ["1"], "conn_id": "c"})[0].control == US.PONG
    fill = parse(bybit("execution", bybit_execution()))[0].fill
    assert fill == C.FillInfo(venue=C.BYBIT, symbol=SYMBOL, trade_id="e-1", venue_order_id="o-1", client_order_id="c1",
                              side=C.BUY, quantity=0.004, price=60000.5, fee=0.012, fee_asset=None, realized_pnl=None,
                              is_maker=False, time_ms=NOW)
    order = parse(bybit("order.linear", bybit_order()))[0].order
    assert order == US.OrderUpdate(C.BYBIT, SYMBOL, "c1", "o-1", C.PARTIALLY_FILLED, "PartiallyFilled", C.BUY, 0.004,
                                   60000.5, NOW)
    position = parse(bybit("position", bybit_position()))[0].position
    assert position == US.PositionUpdate(C.BYBIT, SYMBOL, C.LONG, 0.004, 60000.5, 0.04, NOW, 9001)
    assert parse(bybit("position", bybit_position(side="", size="0")))[0].position.side == C.FLAT
    wallet = parse(bybit("wallet", {"accountType": "UNIFIED", "coin": [{"coin": "USDT", "walletBalance": "950.25"}]}))
    assert wallet[0].balance == US.BalanceUpdate(C.BYBIT, "USDT", 950.25, NOW)


def test_bybit_statuses_and_execution_types_that_are_not_certain_are_not_guessed():
    parse = US.parse_bybit_frame
    statuses = {s: parse(bybit("order", bybit_order(orderStatus=s)))[0].order.status
                for s in ("New", "Filled", "Cancelled", "Rejected", "Untriggered", "Deactivated", "Triggered")}
    assert statuses == {"New": C.NEW, "Filled": C.FILLED, "Cancelled": C.CANCELED, "Rejected": C.REJECTED,
                        "Untriggered": C.NEW, "Deactivated": C.CANCELED, "Triggered": C.UNKNOWN}
    assert parse(bybit("execution", bybit_execution(execType="Funding"))) == []         # not a fill
    assert parse(bybit("execution", bybit_execution(execType="Settle")))[0].control == US.RECONCILE_HINT
    assert parse(bybit("execution", bybit_execution(category="spot"))) == []            # another product
    assert parse(bybit("position", bybit_position(positionIdx=1)))[0].detail == "HEDGE_MODE_POSITION"
    assert parse(bybit("execution.fast", {"execId": "e"}))[0].detail == "UNPARSED:execution.fast"
    for broken in (bybit("execution", bybit_execution(execQty="")), bybit("order", bybit_order(side="Hold")),
                   {"topic": "order", "data": {"not": "a list"}}, bybit("position", bybit_position(updatedTime=None))):
        assert [e.control for e in parse(broken)] == [US.RECONCILE_HINT]


def test_no_field_of_a_bingx_frame_is_read():
    lookalike = json.loads(binance_order(execution="TRADE", last="5", cumulative="5", price="7.8", trade_id=3))
    events = US.parse_bingx_frame(lookalike)
    assert [(e.kind, e.control, e.detail) for e in events] == [(US.CONTROL, US.RECONCILE_HINT, "UNVERIFIED_FRAME_SHAPE")]
    assert all(e.fill is None and e.order is None and e.position is None and e.balance is None for e in events)
    assert US.parse_bingx_frame({"e": "listenKeyExpired"})[0].control == US.LISTEN_KEY_EXPIRED
    assert {k for k, v in US.VERIFICATION.items() if k.startswith("bingx.")} == {
        k for k, v in US.VERIFICATION.items() if k.startswith("bingx.") and v == US.UNVERIFIED}


def test_frames_are_decoded_defensively():
    payload = {"e": "ACCOUNT_UPDATE", "E": NOW}
    assert US.decode_frame(json.dumps(payload)) == ("JSON", payload)
    assert US.decode_frame(gzip.compress(json.dumps(payload).encode())) == ("JSON", payload)
    assert US.decode_frame(payload) == ("JSON", payload)
    assert US.decode_frame(gzip.compress(b"Ping")) == ("TEXT", "Ping") and US.decode_frame(" pong ") == ("TEXT", "pong")
    assert US.decode_frame(US.HEARTBEAT) == ("HEARTBEAT", None)
    for bad in ("not json", "[1, 2]", b"\xff\xfe", b"\x1f\x8bnot gzip", 42, None,
                gzip.compress(b"x" * (US.MAX_FRAME_BYTES + 1))):
        assert US.decode_frame(bad) == ("MALFORMED", None)


# ---------------------------------------------------------------------------------- connect and reconcile
def test_connect_subscribes_reconciles_through_rest_and_only_then_is_live(db):
    venue = StubVenue()
    venue.set_order(1001, "c1", C.NEW)
    venue.set_position(0.02)
    venue.trade(1, 900, 0.02)
    h = Harness(db, venue)
    assert h.published(NOW).health == US.DISCONNECTED and not h.published(NOW).entries_allowed
    status = h.step(NOW)
    assert (status.health, status.entries_allowed, h.transport.connects) == (US.LIVE, True, 1)
    assert h.reconciliations() == [(R.CONNECT, R.OK)]
    state = h.manager.state
    assert set(state.open_orders) == {"1001"} and state.open_orders["1001"].source == R.REST
    assert (state.positions[SYMBOL].side, state.positions[SYMBOL].quantity) == (C.LONG, 0.02)
    assert [f.trade_id for f in h.fills] == ["1"]
    assert h.published(NOW).health == US.LIVE and h.published(NOW).owner == "worker-1"
    assert US.stream_allows_entries(db, C.BINANCE, C.DEMO, ACCOUNT, NOW) == (True, "")
    assert US.stream_allows_entries(db, C.BINANCE, C.DEMO, "another-account", NOW) == (
        False, "USER_STREAM_DISCONNECTED:NO_STREAM_STATE")


def test_a_fill_is_delivered_once_whether_it_is_replayed_or_read_again_from_the_venue(db):
    h = Harness(db, heartbeat_interval_ms=3_600_000, stale_after_ms=36_000_000)
    h.step(NOW)
    h.manager.expect_order("c1")
    frame = binance_order(execution="TRADE", status="FILLED", last="0.010", cumulative="0.010", price="60000",
                          trade_id=501, at=NOW + 100)
    h.transport.push(frame, frame, frame)                         # the same frame three times
    h.step(NOW + 200)
    assert [f.trade_id for f in h.fills] == ["501"] and h.manager.counters["duplicate_fills"] == 2
    assert h.manager.counters["stale_frames"] == 2 and len(h.reconciliations()) == 1     # a replay asks nothing
    h.venue.trade(501, 1001, 0.010, at=NOW + 100)                 # the venue's own record of the same fill
    h.step(NOW + 400_000)                                         # the periodic pass
    assert [f.trade_id for f in h.fills] == ["501"] and h.reconciliations()[-1] == (R.PERIODIC, R.OK)
    ledger = h.manager.ledger.fills()
    assert len(ledger) == 1 and ledger[0].fee_asset == "USDT" and ledger[0].fee == 0.01    # the venue's record


def test_the_venues_record_of_a_fill_replaces_the_streams(db):
    h = Harness(db)
    h.step(NOW)
    h.manager.expect_order("c1")
    h.transport.push(binance_order(execution="TRADE", status="FILLED", last="0.010", cumulative="0.010",
                                   price="60000", trade_id=501, at=NOW + 100))
    h.step(NOW + 200)
    h.venue.trade(501, 1001, 0.009, price=60_001.0, at=NOW + 100)     # the venue says otherwise
    result = h.manager.reconciler.run(NOW + 300, trigger=R.MANUAL, previous=h.manager.state)
    assert result.differences == (f"FILL_CORRECTED_BY_VENUE:{SYMBOL}:501",) and result.new_fills == ()
    assert [(f.quantity, f.price) for f in h.manager.ledger.fills()] == [(0.009, 60_001.0)]
    assert h.manager.ledger.filled_quantity(SYMBOL, "1001") == 0.009


def test_the_reconciled_state_wins_over_a_stream_hint(db):
    venue = StubVenue()
    venue.set_position(0.01)
    h = Harness(db, venue, heartbeat_interval_ms=3_600_000, stale_after_ms=36_000_000)
    h.step(NOW)
    h.transport.push(binance_account(amount="5.000", at=NOW + 1_000))     # a hint the venue does not confirm
    h.step(NOW + 1_100)
    hinted = h.manager.state.positions[SYMBOL]
    assert (hinted.quantity, hinted.source) == (5.0, R.STREAM)
    assert h.manager.balances["USDT"].wallet_balance == 1000.5
    h.step(NOW + 301_000)
    settled = h.manager.state.positions[SYMBOL]
    assert (settled.quantity, settled.source) == (0.01, R.REST) and h.reconciliations()[-1] == (R.PERIODIC, R.OK)
    with db.connect() as conn:
        document = json.loads(conn.execute(
            f"SELECT document FROM {STREAM_RECONCILE_TABLE} ORDER BY rowid DESC LIMIT 1").fetchone()[0])
    assert document["differences"] == [f"POSITION_DIFFERS:{SYMBOL}"] and h.logged()[-1][1] == R.OK


# ------------------------------------------------------------------- disconnect, event loss, reconnect
def test_after_a_disconnect_and_lost_events_the_state_is_the_venues_and_no_fill_is_lost_or_doubled(db):
    venue = StubVenue()
    venue.set_order(1001, "c1", C.NEW)
    h = Harness(db, venue)
    h.step(NOW)
    h.transport.break_connection()
    h.transport.refuse_connect = True
    # while the stream is down the order fills at the venue: no frame of it ever arrives
    venue.set_order(1001, "c1", C.FILLED, 0.01, at=NOW + 500)
    venue.set_position(0.01)
    venue.trade(601, 1001, 0.006, at=NOW + 400)
    venue.trade(602, 1001, 0.004, at=NOW + 500)
    down = h.step(NOW + 1_000)
    assert (down.health, down.entries_allowed, down.reason) == (US.DISCONNECTED, False, "CONNECTION_LOST:StreamClosed")
    assert h.published(NOW + 1_000).health == US.DISCONNECTED
    # REST alone keeps the state while the stream is away, and entries stay blocked
    still_down = h.step(NOW + 2_100)
    assert (still_down.health, still_down.entries_allowed) == (US.DISCONNECTED, False)
    assert [f.trade_id for f in h.fills] == ["601", "602"]
    assert h.manager.state.open_orders == {} and h.manager.state.positions[SYMBOL].quantity == 0.01
    h.transport.refuse_connect = False
    assert h.step(still_down.next_connect_at_ms).health == US.LIVE and h.transport.connects == 2
    assert h.reconciliations() == [(R.CONNECT, R.OK), (R.STREAM_DOWN, R.OK), (R.RECONNECT, R.OK)]
    # late frames of what was already read from the venue are suppressed
    h.transport.push(binance_order(execution="TRADE", status="FILLED", last="0.004", cumulative="0.010",
                                   price="60000", trade_id=602, at=NOW + 500))
    after = h.step(still_down.next_connect_at_ms + 100)
    assert [f.trade_id for f in h.fills] == ["601", "602"] and h.manager.counters["duplicate_fills"] == 1
    assert after.health == US.LIVE and h.manager.state.open_orders == {} and len(h.reconciliations()) == 3


def test_reconnect_backs_off_within_bounds_and_never_holds_two_connections(db):
    h = Harness(db)
    h.step(NOW)
    h.transport.break_connection()
    h.transport.refuse_connect = True
    at, waits = NOW + 10, []
    h.step(at)
    for _ in range(9):
        next_at = h.manager.status(at).next_connect_at_ms
        assert h.step(next_at - 1).health == US.DISCONNECTED       # not a millisecond before the backoff
        waits.append(next_at - at)
        at = next_at
        h.step(at)
    assert waits[:4] == [1_000, 2_000, 4_000, 8_000] and max(waits) == 60_000 and waits == sorted(waits)
    assert h.transport.connects == 1 and h.manager.status(at).reconnect_attempts == 10
    h.transport.refuse_connect = False
    assert h.step(h.manager.status(at).next_connect_at_ms).health == US.LIVE
    assert h.transport.connects == 2 and h.manager.status(at).reconnect_attempts == 0


def test_a_missing_fill_is_detected_from_the_orders_cumulative_quantity(db):
    venue = StubVenue()
    venue.set_order(1001, "c1", C.NEW)
    h = Harness(db, venue)
    h.step(NOW)
    # the venue filled 0.006 in two trades; only the second frame arrived
    venue.set_order(1001, "c1", C.PARTIALLY_FILLED, 0.006, at=NOW + 300)
    venue.trade(701, 1001, 0.002, at=NOW + 200)
    venue.trade(702, 1001, 0.004, at=NOW + 300)
    h.transport.push(binance_order(execution="TRADE", status="PARTIALLY_FILLED", last="0.004", cumulative="0.006",
                                   price="60000", trade_id=702, at=NOW + 300))
    assert h.step(NOW + 400).health == US.LIVE                     # an order update may precede its own fill
    gap = h.step(NOW + 400 + 2_000)
    assert h.reconciliations()[-1] == (R.GAP, R.OK) and gap.health == US.LIVE
    assert sorted(f.trade_id for f in h.fills) == ["701", "702"]
    assert h.manager.ledger.filled_quantity(SYMBOL, "1001") == pytest.approx(0.006)
    # nothing is missing any more: no further pass is asked for
    h.transport.push(binance_order(execution="TRADE", status="PARTIALLY_FILLED", last="0.004", cumulative="0.006",
                                   price="60000", trade_id=702, at=NOW + 300))
    h.step(NOW + 10_000)
    assert len(h.reconciliations()) == 2


def test_a_gap_takes_the_stream_out_of_live_until_the_venue_has_answered(db):
    venue = StubVenue()
    venue.set_order(1001, "c1", C.NEW)
    h = Harness(db, venue)
    h.step(NOW)
    h.transport.push(binance_order(status="PARTIALLY_FILLED", cumulative="0.006", at=NOW + 300))
    h.step(NOW + 400)
    venue.fail = C.AdapterError(C.VENUE_UNAVAILABLE, venue=C.BINANCE)
    blocked = h.step(NOW + 2_400)
    assert (blocked.health, blocked.entries_allowed) == (US.RECONCILE_REQUIRED, False)
    assert h.reconciliations()[-1] == (R.GAP, R.FAILED)
    venue.fail = None
    assert h.step(NOW + 2_400 + 5_000).health == US.LIVE


def test_failed_passes_back_off_within_bounds_and_routine_passes_that_find_nothing_are_not_logged(db):
    venue = StubVenue()
    h = Harness(db, venue, heartbeat_interval_ms=3_600_000, stale_after_ms=360_000_000)
    h.step(NOW)
    for minute in range(1, 6):                                     # five routine passes that find nothing
        h.step(NOW + minute * 300_000)
    assert h.reconciliations() == [(R.CONNECT, R.OK)] + [(R.PERIODIC, R.OK)] * 5
    assert h.logged() == [(R.CONNECT, R.OK)]                       # the log does not grow with the clock
    assert h.published(NOW + 1_500_000).last_reconcile_at_ms == NOW + 1_500_000
    venue.fail = C.AdapterError(C.VENUE_UNAVAILABLE, venue=C.BINANCE)
    at, waits = NOW + 1_800_000, []
    h.step(at)
    for _ in range(8):
        before = len(h.passes)
        step_at = at
        while len(h.passes) == before:                             # find the instant of the next attempt
            step_at += 1_000
            h.step(step_at)
        waits.append(step_at - at)
        at = step_at
    assert waits[:4] == [5_000, 10_000, 20_000, 40_000] and max(waits) == 60_000 and waits == sorted(waits)
    assert h.logged() == [(R.CONNECT, R.OK)] + [(R.PERIODIC, R.FAILED)] * 9     # every failure is on record
    venue.fail = None
    venue.set_position(0.01)
    assert h.step(at + 60_000).health == US.LIVE
    assert h.logged()[-1] == (R.PERIODIC, R.OK)                    # it found a difference: logged


def test_an_order_nobody_announced_asks_the_venue_and_an_announced_one_does_not(db):
    venue = StubVenue()
    h = Harness(db, venue)
    h.step(NOW)
    h.manager.expect_order("mine-1")
    h.transport.push(binance_order(order_id=2001, client_id="mine-1", at=NOW + 100))
    assert h.step(NOW + 200).health == US.LIVE and len(h.reconciliations()) == 1
    assert h.manager.state.open_orders["2001"].source == R.STREAM
    venue.set_order(2001, "mine-1", C.NEW, at=NOW + 100)
    venue.set_order(3001, "someone-else", C.NEW, at=NOW + 300)
    h.transport.push(binance_order(order_id=3001, client_id="someone-else", at=NOW + 300))
    # from the frame until the venue has answered, the hints are not believed and entries are blocked
    doubted = h.step(NOW + 400)
    assert (doubted.health, doubted.reason, doubted.entries_allowed) == (US.RECONCILE_REQUIRED, R.UNKNOWN_ORDER, False)
    assert h.published(NOW + 400).health == US.RECONCILE_REQUIRED and len(h.reconciliations()) == 1
    assert h.step(NOW + 2_500).health == US.LIVE
    assert h.reconciliations()[-1] == (R.UNKNOWN_ORDER, R.OK)
    assert {k: v.source for k, v in h.manager.state.open_orders.items()} == {"2001": R.REST, "3001": R.REST}
    # a state the venue names and this module cannot map is asked about too, never treated as open or closed
    h.transport.push(binance_order(order_id=2001, client_id="mine-1", status="SOMETHING_NEW", at=NOW + 5_000))
    h.step(NOW + 6_000)
    assert h.reconciliations()[-1] == (R.UNKNOWN_ORDER, R.OK) and "2001" in h.manager.state.open_orders


def test_an_older_frame_never_overwrites_newer_state(db):
    venue = StubVenue()
    venue.set_order(1001, "c1", C.PARTIALLY_FILLED, 0.004, at=NOW + 1_000)
    venue.trade(1, 1001, 0.004, at=NOW + 900)
    venue.set_position(0.004)
    h = Harness(db, venue)
    h.step(NOW + 5_000)
    h.transport.push(binance_order(status="NEW", at=NOW + 500), binance_account(amount="0", at=NOW))
    h.step(NOW + 5_100)
    assert h.manager.state.open_orders["1001"].status == C.PARTIALLY_FILLED
    assert h.manager.state.positions[SYMBOL].quantity == 0.004 and h.manager.counters["stale_frames"] == 2


# ----------------------------------------------------------------------------------- liveness and keys
def test_a_silent_stream_becomes_stale_blocks_entries_and_is_replaced(db):
    h = Harness(db)
    h.step(NOW)
    h.step(NOW + 15_000)
    assert h.transport.pings == 1 and h.step(NOW + 15_100).health == US.LIVE     # the pong counts as life
    h.transport.answer_pings = False
    assert h.step(NOW + 45_000).health == US.LIVE
    silent = h.step(NOW + 15_100 + 45_000)
    assert (silent.health, silent.reason, silent.entries_allowed) == (US.STALE, "STREAM_SILENT", False)
    assert not h.transport.is_open and h.published(NOW + 61_000).health == US.STALE
    assert US.stream_allows_entries(db, C.BINANCE, C.DEMO, ACCOUNT, NOW + 61_000) == (
        False, "USER_STREAM_STALE:STREAM_SILENT")
    assert h.reconciliations()[-1] == (R.STREAM_DOWN, R.OK)       # the venue was asked the moment silence was found
    h.transport.answer_pings = True
    assert h.step(NOW + 15_100 + 46_000).health == US.LIVE and h.transport.connects == 2


def test_the_listen_key_is_renewed_and_a_replaced_or_expired_key_moves_the_connection(db):
    venue = StubVenue()
    h = Harness(db, venue, heartbeat_interval_ms=3_600_000, stale_after_ms=36_000_000, reconcile_interval_ms=36_000_000)
    h.step(NOW)
    assert len(venue.reads("user_stream")) == 1
    h.step(NOW + 1_799_999)
    assert len(venue.reads("user_stream")) == 1
    h.step(NOW + 1_800_000)                                       # keepalive: the same key comes back
    assert len(venue.reads("user_stream")) == 2 and h.transport.connects == 1
    venue.key = "LISTENKEYSECRET0002"                             # the venue issued another key
    h.step(NOW + 3_600_000)
    assert h.manager.status(NOW + 3_600_000).reason == "LISTEN_KEY_REPLACED"
    assert h.step(NOW + 3_600_001).health == US.LIVE and h.transport.connects == 2
    h.transport.push(json.dumps({"e": "listenKeyExpired", "E": NOW + 3_700_000}))
    assert h.step(NOW + 3_700_000).reason == "LISTEN_KEY_EXPIRED"
    assert h.step(NOW + 3_700_001).health == US.LIVE and h.transport.connects == 3
    assert h.reconciliations()[-1] == (R.RECONNECT, R.OK)


def test_a_key_that_cannot_be_renewed_is_given_up_when_it_expires(db):
    venue = StubVenue()
    h = Harness(db, venue, heartbeat_interval_ms=3_600_000, stale_after_ms=36_000_000, reconcile_interval_ms=36_000_000)
    h.step(NOW)
    venue.stream_fail = C.AdapterError(C.VENUE_UNAVAILABLE, venue=C.BINANCE)
    assert h.step(NOW + 1_800_000).health == US.LIVE              # renewal failed; the key still lives
    assert h.step(NOW + 1_804_999).health == US.LIVE and len(venue.reads("user_stream")) == 2
    h.step(NOW + 1_805_000)
    assert len(venue.reads("user_stream")) == 3                   # retried, not hammered
    expired = h.step(NOW + 3_600_000)
    assert expired.health == US.DISCONNECTED and expired.reason in ("LISTEN_KEY_EXPIRED_UNRENEWED",
                                                                    "STREAM_SPEC_REFUSED:VENUE_UNAVAILABLE")
    assert not h.transport.is_open


def test_an_auth_frame_stream_is_live_only_after_the_venue_acknowledged_it(db):
    venue = StubVenue(C.BYBIT, US.AUTH_FRAME)
    venue.set_position(0.004)
    h = Harness(db, venue)
    waiting = h.step(NOW)
    assert (waiting.health, waiting.entries_allowed) == (US.RECONCILE_REQUIRED, False)
    assert [f["op"] for f in h.transport.sent] == ["auth"]
    h.transport.push({"success": True, "ret_msg": "", "op": "auth", "conn_id": "c"})
    assert h.step(NOW + 100).health == US.RECONCILE_REQUIRED       # authenticated, not yet subscribed
    assert [f["op"] for f in h.transport.sent] == ["auth", "subscribe"]
    h.transport.push({"success": True, "ret_msg": "", "op": "subscribe", "conn_id": "c"})
    assert h.step(NOW + 200).health == US.LIVE and h.reconciliations() == [(R.CONNECT, R.OK)]
    h.transport.push({"op": "pong", "args": [str(NOW)], "conn_id": "c"})
    h.step(NOW + 15_300)
    assert h.transport.sent[-1] == {"op": "ping"} and h.transport.pings == 0      # the venue's own ping frame
    h.manager.expect_order("c1")
    stamp = dict(execTime=str(NOW + 16_000), updatedTime=str(NOW + 16_000))
    h.transport.push(bybit("execution", bybit_execution(**stamp)), bybit("order", bybit_order(**stamp)),
                     bybit("position", bybit_position(size="0.008", seq=9005, updatedTime=str(NOW + 16_000))),
                     bybit("position", bybit_position(size="0.004", seq=9001, updatedTime=str(NOW + 16_000))))
    assert h.step(NOW + 16_100).health == US.LIVE and len(h.reconciliations()) == 1
    assert [f.trade_id for f in h.fills] == ["e-1"]
    assert h.manager.state.open_orders["o-1"].filled_quantity == 0.004
    held = h.manager.state.positions[SYMBOL]                       # the lower sequence arrived late and is dropped
    assert (held.quantity, held.source) == (0.008, R.STREAM) and h.manager.counters["stale_frames"] == 1


def test_a_refused_or_unacknowledged_authentication_is_not_live_and_not_retried_quickly(db):
    refused = Harness(db, StubVenue(C.BYBIT, US.AUTH_FRAME))
    refused.step(NOW)
    refused.transport.push({"success": False, "ret_msg": "invalid", "op": "auth"})
    status = refused.step(NOW + 100)
    assert (status.health, status.reason, refused.transport.is_open) == (US.DISCONNECTED, US.AUTH_FAILED, False)
    assert status.next_connect_at_ms == NOW + 100 + 60_000
    silent = Harness(db, StubVenue(C.BYBIT, US.AUTH_FRAME), account="account-2")
    silent.step(NOW)
    assert silent.step(NOW + 9_999).health == US.RECONCILE_REQUIRED
    assert silent.step(NOW + 10_000).reason == "STREAM_NOT_ACKNOWLEDGED"
    # a subscription answer without the authentication answer is not an acknowledged stream
    half = Harness(db, StubVenue(C.BYBIT, US.AUTH_FRAME), account="account-3")
    half.step(NOW)
    half.transport.push({"success": True, "ret_msg": "", "op": "subscribe", "conn_id": "c"})
    assert half.step(NOW + 100).health == US.RECONCILE_REQUIRED and half.reconciliations() == []
    assert half.step(NOW + 10_000).reason == "STREAM_NOT_ACKNOWLEDGED" and not half.transport.is_open


def test_a_stream_the_venue_does_not_offer_stays_down_while_rest_keeps_the_state(db):
    venue = StubVenue()
    venue.stream_fail = C.AdapterError(C.UNSUPPORTED, venue=C.BINANCE)
    venue.set_position(0.01)
    h = Harness(db, venue)
    status = h.step(NOW)
    assert (status.health, status.reason) == (US.DISCONNECTED, "STREAM_SPEC_REFUSED:UNSUPPORTED")
    assert h.transport.connects == 0 and h.manager.state.positions[SYMBOL].quantity == 0.01
    h.step(NOW + 15_000)
    assert [r[0] for r in h.reconciliations()] == [R.STREAM_DOWN, R.STREAM_DOWN]


def test_a_bingx_frame_only_wakes_the_reconciler(db):
    venue = StubVenue(C.BINGX)
    venue.set_position(5.0, symbol="LINKUSDT", entry=7.8)
    h = Harness(db, venue)
    h.step(NOW)
    lookalike = json.loads(binance_order(execution="TRADE", last="5", cumulative="5", price="7.8", trade_id=3,
                                         symbol="LINK-USDT"))
    h.transport.push(gzip.compress(b"Ping"), *[gzip.compress(json.dumps(lookalike).encode())] * 5)
    venue.trade(3, 9, 5.0, price=7.8, symbol="LINKUSDT", at=NOW + 50)
    h.step(NOW + 100)
    assert h.transport.sent == ["Pong"] and h.fills == []          # nothing was taken from the frames
    h.step(NOW + 2_000)                                            # five hints, one pass, and the fill is the venue's
    assert h.reconciliations() == [(R.CONNECT, R.OK), (R.HINT, R.OK)] and h.manager.counters["hints"] == 5
    assert [(f.symbol, f.trade_id, f.quantity) for f in h.manager.ledger.fills()] == [("LINKUSDT", "3", 5.0)]


# -------------------------------------------------------------------------------- reconciliation rules
def test_a_venue_instruction_to_wait_is_honoured_before_the_next_pass(db):
    venue = StubVenue()
    h = Harness(db, venue)
    h.step(NOW)
    venue.fail = C.AdapterError(C.RATE_LIMITED, venue=C.BINANCE, retry_after_s=30)
    h.transport.push(json.dumps({"e": "ALGO_UPDATE", "E": NOW}))
    failed = h.step(NOW + 2_000)
    assert (failed.health, failed.reason) == (US.RECONCILE_REQUIRED, "RECONCILIATION_FAILED:RATE_LIMITED")
    venue.fail = None
    reads = len(venue.reads("open_orders"))
    for at in (NOW + 5_000, NOW + 20_000, NOW + 32_000):
        h.transport.push(json.dumps({"e": "ALGO_UPDATE", "E": at}))    # more hints do not bring the pass forward
        h.step(at)
    assert len(venue.reads("open_orders")) == reads
    assert h.step(NOW + 32_001).health == US.LIVE and len(venue.reads("open_orders")) == reads + 1


def test_a_failed_pass_moves_no_cursor_and_returns_no_state(db):
    venue = StubVenue()
    venue.trade(1, 10, 0.01, at=NOW - 5_000)
    reconciler = R.RestReconciler(db, venue, account_ref=ACCOUNT, symbols=[SYMBOL, "ETHUSDT"])

    def fail_on_second_symbol(symbol, *, since_ms=None, venue_order_id=None):
        if symbol == SYMBOL:
            raise C.AdapterError(C.VENUE_UNAVAILABLE, venue=C.BINANCE)
        return ()

    original, venue.fills = venue.fills, fail_on_second_symbol
    failed = reconciler.run(NOW, trigger=R.MANUAL)
    assert (failed.status, failed.error_code, failed.state, failed.new_fills) == (R.FAILED, C.VENUE_UNAVAILABLE, None, ())
    assert reconciler.ledger.cursors() == {} and reconciler.ledger.fills() == ()
    venue.fills = original
    done = reconciler.run(NOW + 1, trigger=R.MANUAL)
    assert done.ok and [f.trade_id for f in done.new_fills] == ["1"]
    # a cursor is the newest fill read, or the start of the pass less the overlap when there was none
    assert reconciler.ledger.cursors() == {"BTCUSDT": NOW - 5_000, "ETHUSDT": NOW + 1 - 60_000}
    with pytest.raises(ValueError):
        reconciler.run(NOW + 2, trigger="BECAUSE")


def test_cursors_are_per_symbol_and_each_pass_reads_again_from_before_its_cursor(db):
    venue = StubVenue()
    venue.trade(1, 10, 0.01, at=NOW - 10_000)
    reconciler = R.RestReconciler(db, venue, account_ref=ACCOUNT, symbols=[SYMBOL])
    reconciler.run(NOW, trigger=R.MANUAL)
    assert venue.reads("fills")[-1] == ("fills", SYMBOL, None)        # first pass: the venue's own default window
    venue.trade(2, 11, 0.02, at=NOW - 30_000)                          # published late, older than the cursor
    second = reconciler.run(NOW + 1_000, trigger=R.MANUAL, extra_symbols=["eth-usdt"])
    assert venue.reads("fills")[-2:] == [("fills", SYMBOL, NOW - 70_000), ("fills", "ETHUSDT", None)]
    assert [f.trade_id for f in second.new_fills] == ["2"] and second.duplicate_fills == 1
    with db.connect() as conn:
        assert conn.execute(f"SELECT COUNT(*) FROM {STREAM_CURSOR_TABLE}").fetchone()[0] == 2


def test_the_same_trade_id_on_two_symbols_or_two_accounts_is_not_one_fill(db):
    venue = StubVenue()
    venue.trade(7, 10, 0.01, symbol="BTCUSDT")
    venue.trade(7, 20, 0.5, symbol="ETHUSDT")                         # Binance numbers trades per symbol
    first = R.RestReconciler(db, venue, account_ref="account-1", symbols=["BTCUSDT", "ETHUSDT"])
    second = R.RestReconciler(db, venue, account_ref="account-2", symbols=["BTCUSDT", "ETHUSDT"])
    assert len(first.run(NOW, trigger=R.MANUAL).new_fills) == 2
    assert len(second.run(NOW, trigger=R.MANUAL).new_fills) == 2      # another account's ledger is its own
    assert first.run(NOW + 1, trigger=R.MANUAL).new_fills == ()
    assert len(first.ledger.fills()) == 2 and len(first.ledger.fills(symbol="eth-usdt")) == 1
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        with db.connect() as conn:
            conn.execute("DELETE FROM cati_user_stream_fills")


def test_no_database_transaction_is_open_while_the_venue_is_asked(db):
    venue = StubVenue()
    venue.set_order(1001, "c1", C.NEW)
    venue.trade(1, 1001, 0.001)
    probes: List[int] = []

    def take_the_write_lock() -> None:
        other = sqlite3.connect(db.path, timeout=0.2)
        try:
            other.execute("BEGIN IMMEDIATE")        # impossible if the caller held a transaction across this call
            other.rollback()
        finally:
            other.close()
        probes.append(1)

    venue.probe = take_the_write_lock
    h = Harness(db, venue)
    assert h.step(NOW).health == US.LIVE
    h.transport.push(binance_order(order_id=3001, client_id="unknown", at=NOW + 100))
    assert h.step(NOW + 2_100).health == US.LIVE and h.reconciliations()[-1] == (R.UNKNOWN_ORDER, R.OK)
    assert len(probes) == 1 + 2 * 3                                 # the stream spec, then two passes of three reads


def test_malformed_venue_answers_fail_the_pass(db):
    venue = StubVenue()
    venue.open_orders = lambda symbol=None: ("not an order",)
    result = R.RestReconciler(db, venue, account_ref=ACCOUNT).run(NOW, trigger=R.MANUAL)
    assert (result.status, result.error_code) == (R.FAILED, C.MALFORMED_RESPONSE)


# ------------------------------------------------------------------------------------ restart and lease
def test_after_a_process_restart_nothing_is_delivered_twice_and_the_cursor_is_resumed(db):
    venue = StubVenue()
    venue.set_order(1001, "c1", C.NEW)
    first = Harness(db, venue)
    first.step(NOW)
    first.manager.expect_order("c1")
    frame = binance_order(execution="TRADE", status="PARTIALLY_FILLED", last="0.004", cumulative="0.004",
                          price="60000", trade_id=801, at=NOW + 100)
    first.transport.push(frame)
    first.step(NOW + 200)
    assert [f.trade_id for f in first.fills] == ["801"]
    first.manager.close(NOW + 300)                                     # the process stops
    assert first.published(NOW + 300).health == US.DISCONNECTED and first.published(NOW + 300).owner is None
    venue.set_order(1001, "c1", C.FILLED, 0.01, at=NOW + 5_000)        # and misses the rest of the order
    venue.trade(801, 1001, 0.004, at=NOW + 100)
    venue.trade(802, 1001, 0.006, at=NOW + 5_000)
    venue.set_position(0.01)
    second = Harness(db, venue, owner="worker-1-restarted")
    assert second.step(NOW + 60_000).health == US.LIVE
    assert second.reconciliations()[-1] == (R.RESTART, R.OK)
    assert [f.trade_id for f in second.fills] == ["802"]               # 801 was delivered before the restart
    assert venue.reads("fills")[-1] == ("fills", SYMBOL, NOW - 120_000)     # resumed from the durable cursor
    second.transport.push(frame)                                       # the venue replays the old frame
    second.step(NOW + 60_100)
    assert [f.trade_id for f in second.fills] == ["802"]
    assert second.manager.state.open_orders == {} and second.manager.state.positions[SYMBOL].quantity == 0.01
    assert [f.trade_id for f in second.manager.ledger.fills()] == ["801", "802"]


def test_only_one_worker_holds_an_accounts_stream(db):
    venue = StubVenue()
    first = Harness(db, venue, owner="worker-1")
    first.step(NOW)
    second = Harness(db, venue, owner="worker-2")
    status = second.step(NOW + 1_000)
    assert (status.health, status.reason) == (US.DISCONNECTED, "STREAM_HELD_BY_ANOTHER_WORKER")
    assert second.transport.connects == 0 and len(venue.reads("user_stream")) == 1
    assert second.published(NOW + 1_000).owner == "worker-1"           # and it wrote nothing over the holder's state
    first.manager.close(NOW + 2_000)
    assert second.step(NOW + 2_001).health == US.LIVE and second.transport.connects == 1


def test_a_dead_workers_live_state_is_reported_stale_and_its_lease_can_be_taken(db):
    venue = StubVenue()
    dead = Harness(db, venue, owner="worker-1")
    dead.step(NOW)
    assert dead.published(NOW + 59_999).health == US.LIVE
    abandoned = dead.published(NOW + 60_000)
    assert (abandoned.health, abandoned.reason, abandoned.entries_allowed) == (
        US.STALE, "STREAM_OWNER_NOT_REPORTING", False)
    heir = Harness(db, venue, owner="worker-2")
    assert heir.step(NOW + 60_000).health == US.LIVE and heir.published(NOW + 60_000).owner == "worker-2"
    # the first worker wakes up late: it finds the lease gone, drops its connection and writes nothing
    late = dead.step(NOW + 61_000)
    assert late.reason == "STREAM_HELD_BY_ANOTHER_WORKER" and not dead.transport.is_open
    assert heir.published(NOW + 61_000).health == US.LIVE


def test_two_accounts_have_separate_streams_states_and_ledgers(db):
    one, two = Harness(db, StubVenue(), account="account-1"), Harness(db, StubVenue(), account="account-2")
    one.step(NOW)
    two.step(NOW)
    for h in (one, two):
        h.manager.expect_order("c1")
        h.transport.push(binance_order(execution="TRADE", status="FILLED", last="0.010", cumulative="0.010",
                                       price="60000", trade_id=901, at=NOW + 100))
        h.step(NOW + 200)
    assert [f.trade_id for f in one.fills] == ["901"] and [f.trade_id for f in two.fills] == ["901"]
    one.transport.break_connection()
    assert one.step(NOW + 300).health == US.DISCONNECTED and two.step(NOW + 300).health == US.LIVE
    with pytest.raises(ValueError):
        US.UserStreamManager(db, StubVenue(), ScriptedTransport(), account_ref="", owner="w")


# ------------------------------------------------------------------------------------------- secrets
def test_no_listen_key_and_no_signature_is_stored_logged_or_shown(db, tmp_path, caplog):
    caplog.set_level("DEBUG")
    key_stream = Harness(db, StubVenue())
    key_stream.step(NOW)
    key_stream.transport.push(json.dumps({"e": "listenKeyExpired", "E": NOW + 1, "listenKey": LISTEN_KEY}))
    key_stream.step(NOW + 1)
    key_stream.transport.refuse_connect = True
    key_stream.step(NOW + 2)
    signed = Harness(db, StubVenue(C.BYBIT, US.AUTH_FRAME), account="account-2")
    signed.step(NOW)
    signed.transport.push({"success": False, "ret_msg": SIGNATURE, "op": "auth"})
    signed.step(NOW + 1)
    shown = repr(key_stream.venue.user_stream()) + repr(key_stream.manager.status(NOW + 2)) + repr(
        signed.manager.status(NOW + 2)) + caplog.text
    with db.connect() as conn:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        stored = "".join(repr(tuple(row)) for table in tables for row in conn.execute(f"SELECT * FROM {table}"))
    for secret in (LISTEN_KEY, SIGNATURE, "wss://stream.invalid"):
        assert secret not in shown and secret not in stored
    assert "cati_user_stream_state" in tables and "account-2" in stored
