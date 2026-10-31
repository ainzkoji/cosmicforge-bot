"""Section K3 -- the customer event stream survives reconnects and process boundaries.

* every customer event frame carries ``id`` (its event id) and the top-level identity fields;
* a reconnect with ``Last-Event-ID`` replays the owner's events recorded after that id, never anyone else's;
* an event recorded by another process (only in ``user_events``, never on this process's broadcaster) is
  still delivered by the durable tail, and an event seen on both paths is sent once.
"""
import asyncio
import json
import time

import pytest

from app.api import events as sse
from app.observability import user_events as ue
from shared_lib.persistence import event_broadcaster
from shared_lib.persistence.db import DB
from shared_lib.persistence.event_broadcaster import EventBroadcaster


@pytest.fixture
def world(tmp_path, monkeypatch):
    database = DB(str(tmp_path / "sse.db"))
    monkeypatch.setattr(sse, "DB", lambda: database)
    broadcaster = EventBroadcaster()
    monkeypatch.setattr(sse, "get_event_broadcaster", lambda: broadcaster)
    # Recorded "by another process": nothing reaches this process's broadcaster.
    monkeypatch.setattr(event_broadcaster, "get_event_broadcaster", lambda: _Silent())
    return database


class _Silent:
    def broadcast(self, event):
        pass


def _emit(db, kind, user, bot, at, **payload):
    eid = ue.event_id_for(kind, user, bot, at)
    assert ue.emit(db, kind, event_id=eid, user_id=user, bot_id=bot, broker_account_id=f"acct-{user}",
                   environment="DEMO", payload={"symbol": "BTCUSDT", **payload}, now_ms=at) is not None
    return eid


def _run(coro):
    """Run on a private loop: ``asyncio.run`` would clear the current loop, and a later first import of
    ``app.main`` (ib_insync/eventkit) needs one."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _collect(stream, count, timeout=5.0):
    async def run():
        frames = []
        try:
            while len(frames) < count:
                frame = await asyncio.wait_for(stream.__anext__(), timeout=timeout)
                if "comment" not in frame:
                    frames.append(frame)
        finally:
            await stream.aclose()
        return frames
    return _run(run())


def test_a_reconnect_replays_what_the_owner_missed_and_nothing_of_anyone_else(world):
    now = int(time.time() * 1000)
    first = _emit(world, ue.ENTRY_FILLED, "alice", "bot-a", now - 3000)
    _emit(world, ue.ENTRY_FILLED, "bob", "bot-b", now - 2500)
    second = _emit(world, ue.BOT_PAUSED, "alice", "bot-a", now - 2000)
    third = _emit(world, ue.EXIT_FILLED, "alice", "bot-a", now - 1000)
    frames = _collect(sse._event_stream("alice", False, last_event_id=first, poll_seconds=60), 3)
    assert frames[0]["event"] == "connected"
    assert [f["id"] for f in frames[1:]] == [second, third]
    data = json.loads(frames[1]["data"])
    assert data["event_type"] == "BOT_PAUSED" and data["user_id"] == "alice" and data["bot_id"] == "bot-a"
    assert data["broker_account_id"] == "acct-alice" and data["environment"] == "DEMO"
    assert data["timestamp"] == now - 2000 and data["event_id"] == second


def _marker(user):
    from shared_lib.persistence.events import Event, EventLevel, EventType
    sse.get_event_broadcaster().broadcast(Event(event_type=EventType.TP1_HIT, level=EventLevel.INFO, symbol="MARKER",
                                                payload={"user_id": user, "marker": True}))


def _until_marker(stream, after):
    """Frames after ``connected`` up to and including a marker addressed to the subscriber; ``after`` runs
    once the stream is subscribed. Nothing that should not have been delivered can hide behind the marker."""
    async def run():
        frames = [await stream.__anext__()]
        try:
            await after()
            while True:
                frame = await asyncio.wait_for(stream.__anext__(), timeout=5)
                if "comment" in frame:
                    continue
                frames.append(frame)
                if json.loads(frame["data"]).get("symbol") == "MARKER":
                    return frames
        finally:
            await stream.aclose()
    return _run(run())


def test_another_users_event_id_replays_nothing(world):
    now = int(time.time() * 1000)
    alice_event = _emit(world, ue.ENTRY_FILLED, "alice", "bot-a", now - 5000)
    _emit(world, ue.EXIT_FILLED, "alice", "bot-a", now - 4000)

    async def after():
        await asyncio.sleep(0.3)      # several durable polls: the tail is owner-scoped too
        _marker("bob")
    frames = _until_marker(sse._event_stream("bob", False, last_event_id=alice_event, poll_seconds=0.05), after)
    assert frames[0]["event"] == "connected"
    assert [json.loads(f["data"])["symbol"] for f in frames[1:]] == ["MARKER"]


def test_an_event_recorded_by_another_process_reaches_the_subscriber(world):
    box = {}

    async def after():
        box["eid"] = _emit(world, ue.POSITION_UNPROTECTED, "alice", "bot-a", int(time.time() * 1000))
        _emit(world, ue.ENTRY_FILLED, "bob", "bot-b", int(time.time() * 1000))
        await asyncio.sleep(0.3)
        _marker("alice")
    frames = _until_marker(sse._event_stream("alice", False, poll_seconds=0.05), after)
    assert [f["event"] for f in frames] == ["connected", "POSITION_UNPROTECTED", "TP1_HIT"]
    assert frames[1]["id"] == box["eid"] and json.loads(frames[1]["data"])["user_id"] == "alice"


def test_an_event_seen_live_and_in_the_durable_record_is_sent_once(world, monkeypatch):
    broadcaster = sse.get_event_broadcaster()
    monkeypatch.setattr(event_broadcaster, "get_event_broadcaster", lambda: broadcaster)  # same process now
    box = {}

    async def after():
        box["eid"] = _emit(world, ue.BOT_STOPPED, "alice", "bot-a", int(time.time() * 1000))
        await asyncio.sleep(0.3)      # the durable tail sees it too
        _marker("alice")
    frames = _until_marker(sse._event_stream("alice", False, poll_seconds=0.05), after)
    assert [f["event"] for f in frames] == ["connected", "BOT_STOPPED", "TP1_HIT"]
    assert frames[1]["id"] == box["eid"]


def test_a_fresh_connection_does_not_replay_history(world):
    _emit(world, ue.ENTRY_FILLED, "alice", "bot-a", int(time.time() * 1000) - 10_000)   # before the connection

    async def after():
        await asyncio.sleep(0.3)
        _marker("alice")
    frames = _until_marker(sse._event_stream("alice", False, poll_seconds=0.05), after)
    assert [f["event"] for f in frames] == ["connected", "TP1_HIT"]
