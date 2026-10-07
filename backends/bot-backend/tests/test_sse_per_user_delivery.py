"""Per-user real-time delivery, emitter to subscriber.

The SSE stream filters every event by owner (fail closed), so the emitter must
say whose trade an event belongs to. These tests drive the real chain

    TradeTracker -> emit_info -> EventStore.emit -> EventBroadcaster -> _event_stream

and prove that

* user A receives the events of A's own trades,
* user A never receives user B's,
* an admin receives both,
* a trade opened without any owner identifier reaches admins only,
* the pre-existing payload keys are unchanged (owner keys are additive).

No network. The trade/event tables live in a throwaway SQLite file.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate

ALICE = "sse-alice"
BOB = "sse-bob"
ADMIN = "sse-admin"


def _insert_row(db: DB, table: str, **values) -> None:
    """Insert a row, filling every other NOT NULL column with a neutral value."""
    with db.connect() as conn:
        for col in conn.execute(f"PRAGMA table_info({table})").fetchall():
            name, col_type, notnull, default = col[1], str(col[2] or "").upper(), col[3], col[4]
            if name in values or not notnull or default is not None:
                continue
            values[name] = 0 if ("INT" in col_type or "REAL" in col_type) else "x"
        columns = ", ".join(values)
        marks = ", ".join("?" for _ in values)
        conn.execute(f"INSERT INTO {table} ({columns}) VALUES ({marks})", tuple(values.values()))


@pytest.fixture
def events_api(tmp_path, monkeypatch):
    """The events router with real owner lookups against a migrated temp DB."""
    pytest.importorskip("sse_starlette")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "sse_owner.db").as_posix())
    db = DB()
    migrate(db)
    _insert_row(db, "bot_instances", id="bot-alice", user_id=ALICE)
    _insert_row(db, "bot_instances", id="bot-bob", user_id=BOB)

    from app.api import events

    events._OWNER_CACHE.clear()
    yield events
    events._OWNER_CACHE.clear()


@pytest.fixture
def tracker(tmp_path, monkeypatch):
    """A TradeTracker whose events go through the real store and broadcaster."""
    from shared_lib.persistence import events as event_store_module
    from shared_lib.persistence.trade_tracker import TradeTracker

    path = str(tmp_path / "sse_trades.db")
    # The legacy audit store writes both ``ts`` and ``timestamp_utc``; give it a
    # table that has every column it inserts.
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE events (
                event_id TEXT PRIMARY KEY, run_id TEXT, trade_id TEXT, cycle_id INTEGER,
                ts TEXT NOT NULL, timestamp_utc TEXT, event_type TEXT NOT NULL, level TEXT NOT NULL,
                symbol TEXT, timeframe TEXT, strategy TEXT, mode TEXT, payload_json TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()
    monkeypatch.setattr(event_store_module.EventStore, "_instance", None)
    monkeypatch.setattr(event_store_module, "_store", None)
    event_store_module.get_event_store(path)
    return TradeTracker(path)


def _lifecycle(tracker, symbol, **owner):
    """One complete trade: open, TP1, add, close (4 analytics events)."""
    from shared_lib.persistence.trade_tracker import ExitReason

    tracker.open_trade(symbol=symbol, side="LONG", strategy="cati", mode="paper", timeframe="1h",
                       entry_price=100.0, entry_qty=1.0, entry_confidence=0.7, initial_stop=95.0, **owner)
    tracker.record_tp1(symbol, 105.0)
    tracker.record_add(symbol, 106.0, 0.5)
    tracker.close_trade(symbol, 110.0, ExitReason.TP2, realized_pnl=10.0)


def _collect(events_api, tracker, user_id, is_admin, expected_count):
    async def scenario():
        stream = events_api._event_stream(user_id, is_admin)
        received = [await stream.__anext__()]                      # subscribes; "connected"
        _lifecycle(tracker, "BTCUSDT", bot_instance_id="bot-alice")            # owner via the bot
        _lifecycle(tracker, "ETHUSDT", user_id=BOB, bot_instance_id="bot-bob")  # owner stated
        _lifecycle(tracker, "SOLUSDT")                                          # no owner at all
        # A marker addressed to this subscriber: it must be the very next thing
        # delivered after the expected events, i.e. nothing else was let through.
        get_event_broadcaster().broadcast(Event(
            event_type=EventType.TP1_HIT, level=EventLevel.INFO, symbol="MARKER",
            payload={"user_id": user_id, "marker": True}))
        try:
            for _ in range(expected_count + 1):
                received.append(await asyncio.wait_for(stream.__anext__(), timeout=5))
        finally:
            await stream.aclose()
        return received

    from shared_lib.persistence.event_broadcaster import get_event_broadcaster
    from shared_lib.persistence.events import Event, EventLevel, EventType

    listeners_before = get_event_broadcaster().get_listener_count()
    messages = asyncio.run(scenario())
    assert get_event_broadcaster().get_listener_count() == listeners_before   # listener cleaned up
    assert messages[0]["event"] == "connected"
    delivered = [(m["event"], json.loads(m["data"])) for m in messages[1:]]
    assert delivered[-1][1]["symbol"] == "MARKER", "an event that was not expected reached this subscriber"
    return delivered[:-1]


LIFECYCLE = ["POSITION_OPENED", "TP1_HIT", "ADD_FILLED", "POSITION_CLOSED"]


def test_user_receives_own_trade_events_and_not_another_users(events_api, tracker):
    alice = _collect(events_api, tracker, ALICE, False, 4)

    assert [name for name, _ in alice] == LIFECYCLE
    assert {data["symbol"] for _, data in alice} == {"BTCUSDT"}
    assert all(data["payload"]["bot_instance_id"] == "bot-alice" for _, data in alice)
    serialized = json.dumps(alice)
    assert "ETHUSDT" not in serialized and "bot-bob" not in serialized and BOB not in serialized
    assert "SOLUSDT" not in serialized                              # ownerless: admins only


def test_other_user_receives_only_their_own(events_api, tracker):
    bob = _collect(events_api, tracker, BOB, False, 4)

    assert [name for name, _ in bob] == LIFECYCLE
    assert {data["symbol"] for _, data in bob} == {"ETHUSDT"}
    assert all(data["payload"]["user_id"] == BOB for _, data in bob)
    assert "BTCUSDT" not in json.dumps(bob) and "bot-alice" not in json.dumps(bob)


def test_admin_receives_every_users_events(events_api, tracker):
    admin = _collect(events_api, tracker, ADMIN, True, 12)

    assert [name for name, _ in admin] == LIFECYCLE * 3
    assert [data["symbol"] for _, data in admin] == ["BTCUSDT"] * 4 + ["ETHUSDT"] * 4 + ["SOLUSDT"] * 4


def test_owner_keys_are_additive_and_existing_payload_keys_unchanged(events_api, tracker):
    owned = dict(_collect(events_api, tracker, ALICE, False, 4))
    owner_keys = {"bot_instance_id"}

    assert set(owned["POSITION_OPENED"]["payload"]) - owner_keys == {
        "trade_id", "symbol", "side", "strategy", "entry_price", "qty"}
    assert set(owned["TP1_HIT"]["payload"]) - owner_keys == {"trade_id", "fill_price"}
    assert set(owned["ADD_FILLED"]["payload"]) - owner_keys == {"trade_id", "add_price", "add_qty", "add_count"}
    assert set(owned["POSITION_CLOSED"]["payload"]) - owner_keys == {
        "trade_id", "exit_price", "exit_reason", "realized_pnl", "r_multiple", "duration_minutes"}
    # The SSE envelope is unchanged too.
    assert set(owned["POSITION_CLOSED"]) == {"event_id", "trade_id", "symbol", "ts", "payload"}


def test_trade_without_owner_emits_no_owner_keys(tracker):
    from shared_lib.persistence.event_broadcaster import get_event_broadcaster
    from shared_lib.persistence.events import EventType

    listener_id, queue = get_event_broadcaster().subscribe(event_filter={EventType.POSITION_OPENED})
    try:
        tracker.open_trade(symbol="XRPUSDT", side="LONG", strategy="cati", mode="paper", timeframe="1h",
                           entry_price=1.0, entry_qty=1.0, entry_confidence=0.5)
        event = queue.get_nowait()
    finally:
        get_event_broadcaster().unsubscribe(listener_id)

    assert set(event.payload) == {"trade_id", "symbol", "side", "strategy", "entry_price", "qty"}
