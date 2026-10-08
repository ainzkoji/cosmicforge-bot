"""Step 1.8 -- customer events and their durable delivery.

Unit level: ``app.observability.user_events`` on a migrated database (idempotent
emission, restart replay, the outbox with bounded retry, channel selection,
ENTRY_BLOCKED rate limiting, outage isolation, secret stripping, SSE ownership).
Integration level: the real production path (``process_account`` and the
runtime cycle on the certification broker fake) and the real bot lifecycle
service emit each event once, after the state change is committed.
"""
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from test_demo_boundary_certification import broker
from test_production_execution import live, profile
from test_production_demo_execution import demo, demo_profile
from test_production_research_separation import fresh, process
from test_runtime_cost_budget import measured, waiting_for_signal
from app.api import cati_account
from app.api import events as sse
from app.core import broker_capability_gate
from app.core.bot_instance_service import BotInstanceService
from app.execution import demo_boundary_certification as cert
from app.notifications.worker import NotificationWorker
from app.observability import user_events as ue
from shared_lib.persistence import event_broadcaster
from shared_lib.persistence.db import DB
from shared_lib.persistence.events import Event, EventLevel, EventStore, EventType
from shared_lib.persistence.migrations import migrate

__all__ = ["broker", "live", "profile", "demo", "demo_profile", "fresh", "measured"]

NOW_ISO = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc).isoformat()
T0 = 1_791_000_000_000


class Bus:
    def __init__(self):
        self.events = []

    def broadcast(self, event):
        self.events.append(event)


@pytest.fixture
def bus(monkeypatch):
    fake = Bus()
    monkeypatch.setattr(event_broadcaster, "get_event_broadcaster", lambda: fake)
    return fake


@pytest.fixture
def db(tmp_path, bus):
    database = DB(str(tmp_path / "events.db"))
    migrate(database)
    with database.connect() as c:
        for uid in ("alice", "bob"):
            c.execute("INSERT INTO users (id,email,hashed_password,status,created_at,updated_at) VALUES (?,?,?,?,?,?)",
                      (uid, f"{uid}@example.test", "x", "active", NOW_ISO, NOW_ISO))
            c.execute("INSERT INTO broker_accounts (id,user_id,broker_id,market_type,label,status,environment,created_at,updated_at) "
                      "VALUES (?,?,?,?,?,?,?,?,?)", (f"acct-{uid}", uid, "binance", "crypto", uid, "connected", "demo", NOW_ISO, NOW_ISO))
    return database


def enable(db, user, channel, category, recipient="dest"):
    with db.connect() as c:
        c.execute("INSERT OR REPLACE INTO notification_preferences (user_id,channel,category,is_enabled,updated_at) VALUES (?,?,?,1,?)",
                  (user, channel, category, NOW_ISO))
        if recipient:
            c.execute("INSERT OR REPLACE INTO notification_endpoints (user_id,channel,recipient,status,created_at) VALUES (?,?,?,'active',?)",
                      (user, channel, recipient, NOW_ISO))


def emit(db, kind=ue.ENTRY_FILLED, key="plan-1", user="alice", now=T0, **payload):
    return ue.emit(db, kind, event_id=ue.event_id_for(kind, key), user_id=user, bot_id=f"bot-{user}",
                   broker_account_id=f"acct-{user}", environment="DEMO",
                   payload={"symbol": "ADAUSDT", "message": "hello", **payload}, now_ms=now)


def alerts(db, kind=None):
    with db.connect() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM alerts")]
    return [r for r in rows if kind is None or r["alert_type"] == kind]


# ── emission ────────────────────────────────────────────────────────────────

def test_an_event_is_recorded_once_with_an_in_app_alert_and_a_broadcast(db, bus):
    event = emit(db)
    assert event["event_type"] == "ENTRY_FILLED" and event["user_id"] == "alice" and event["environment"] == "DEMO"
    [stored] = ue.recent(db, user_id="alice")
    assert stored["event_id"] == event["event_id"] and stored["payload"]["symbol"] == "ADAUSDT" and stored["at"] == T0
    [alert] = alerts(db, "ENTRY_FILLED")
    assert alert["user_id"] == "alice" and alert["message"] == "hello" and alert["severity"] == "INFO"
    [row] = ue.outbox(db, event_ids=[event["event_id"]])
    assert row["channel"] == "in_app" and row["status"] == "sent"
    [sent] = bus.events
    assert sent.event_type is EventType.ENTRY_FILLED and sent.payload["user_id"] == "alice" and sent.payload["bot_id"] == "bot-alice"


def test_a_duplicate_emission_writes_and_broadcasts_nothing(db, bus):
    assert emit(db) is not None
    assert emit(db) is None and emit(db, now=T0 + 5000) is None
    assert len(ue.recent(db, user_id="alice")) == 1 and len(alerts(db)) == 1 and len(ue.outbox(db)) == 1 and len(bus.events) == 1


def test_a_replay_after_a_restart_is_still_one_event(db, bus, tmp_path):
    enable(db, "alice", "email", "trade")
    emit(db)
    reopened = DB(str(tmp_path / "events.db"))                # a new process sees the same file
    assert emit(reopened) is None
    assert len(ue.recent(reopened, user_id="alice")) == 1
    assert sorted(r["channel"] for r in ue.outbox(reopened)) == ["email", "in_app"]


def test_the_tables_are_created_on_first_use_without_a_migration(tmp_path, bus):
    bare = DB(str(tmp_path / "bare.db"))
    with bare.connect() as c:
        c.execute("CREATE TABLE marker (x)")
    assert emit(bare) is not None and len(ue.recent(bare, user_id="alice")) == 1
    assert ue.recent(DB(str(tmp_path / "empty.db")), user_id="alice") == [] and ue.outbox(DB(str(tmp_path / "empty.db"))) == []


def test_secrets_never_reach_the_event(db):
    emit(db, api_key="K", api_secret="S", access_token="T", password="P", note="kept")
    [stored] = ue.recent(db, user_id="alice")
    assert set(stored["payload"]) == {"symbol", "message", "note"}
    assert "K" not in alerts(db)[0]["details_json"].replace("kept", "")


def test_an_unknown_type_is_a_programming_error_and_a_missing_user_is_skipped(db):
    with pytest.raises(ValueError):
        ue.emit(db, "SOMETHING_ELSE", event_id="x", user_id="alice", bot_id=None, broker_account_id=None, environment=None, payload={})
    assert ue.emit(db, ue.BOT_PAUSED, event_id="y", user_id=None, bot_id="b", broker_account_id=None, environment=None, payload={}) is None
    assert ue.recent(db, user_id="alice") == []


def test_a_database_outage_never_reaches_the_caller(bus):
    class Down:
        def connect(self):
            raise sqlite3.OperationalError("database is locked")
    assert emit(Down()) is None and bus.events == []
    assert ue.deliver_pending(Down()) == {"sent": 0, "retried": 0, "failed": 0, "skipped": 0}


def test_a_broadcast_failure_does_not_lose_the_event(db, monkeypatch):
    monkeypatch.setattr(event_broadcaster, "get_event_broadcaster", lambda: (_ for _ in ()).throw(RuntimeError("bus down")))
    assert emit(db) is not None and len(ue.recent(db, user_id="alice")) == 1


# ── channels and the outbox ─────────────────────────────────────────────────

def test_external_channels_need_the_preference_and_an_endpoint(db):
    enable(db, "alice", "email", "trade")
    enable(db, "alice", "telegram", "trade", recipient=None)        # enabled, but nowhere to send
    enable(db, "alice", "push", "risk")                             # another category
    event = emit(db)
    assert sorted(r["channel"] for r in ue.outbox(db, event_ids=[event["event_id"]])) == ["email", "in_app"]
    other = emit(db, user="bob", key="plan-b")
    assert [r["channel"] for r in ue.outbox(db, event_ids=[other["event_id"]])] == ["in_app"]


def test_in_app_can_be_switched_off(db):
    with db.connect() as c:
        c.execute("INSERT INTO notification_preferences (user_id,channel,category,is_enabled,updated_at) VALUES ('alice','in_app','trade',0,?)", (NOW_ISO,))
    event = emit(db)
    assert ue.outbox(db, event_ids=[event["event_id"]]) == [] and alerts(db) == [] and len(ue.recent(db, user_id="alice")) == 1


def test_delivery_sends_once_through_each_channel(db):
    for channel in ue.EXTERNAL_CHANNELS:
        enable(db, "alice", channel, "trade", recipient=f"{channel}-dest")
    event = emit(db)
    sent = []
    counts = ue.deliver_pending(db, now_ms=T0 + 1, sender=lambda ch, to, r, e: sent.append((ch, to, r["subject"], e["event_id"])) or True)
    assert counts == {"sent": 3, "retried": 0, "failed": 0, "skipped": 0}
    assert sorted(s[:2] for s in sent) == [("email", "email-dest"), ("push", "push-dest"), ("telegram", "telegram-dest")]
    assert all(s[2] == "CosmicForge: Entry filled ADAUSDT [DEMO]" and s[3] == event["event_id"] for s in sent)
    assert ue.deliver_pending(db, now_ms=T0 + 2, sender=lambda *a: pytest.fail("sent twice")) == {"sent": 0, "retried": 0, "failed": 0, "skipped": 0}
    assert all(r["status"] == "sent" and r["delivered_at"] for r in ue.outbox(db, event_ids=[event["event_id"]]))


def test_a_failing_channel_is_retried_with_backoff_then_fails_and_is_never_called_delivered(db):
    enable(db, "alice", "email", "trade")
    event = emit(db)

    def boom(*a):
        raise ConnectionError("smtp down")
    now = T0 + 1
    for attempt in range(1, ue.MAX_ATTEMPTS):
        assert ue.deliver_pending(db, now_ms=now, sender=boom)["retried"] == 1
        [row] = ue.outbox(db, event_ids=[event["event_id"]], status="pending")
        assert row["attempts"] == attempt and "ConnectionError" in row["last_error"] and row["delivered_at"] is None
        assert row["next_attempt_at"] == now + ue.BACKOFF_SECONDS[attempt - 1] * 1000
        assert ue.deliver_pending(db, now_ms=row["next_attempt_at"] - 1, sender=boom) == {"sent": 0, "retried": 0, "failed": 0, "skipped": 0}
        now = row["next_attempt_at"]
    assert ue.deliver_pending(db, now_ms=now, sender=boom)["failed"] == 1
    [row] = [r for r in ue.outbox(db, event_ids=[event["event_id"]]) if r["channel"] == "email"]
    assert row["status"] == "failed" and row["attempts"] == ue.MAX_ATTEMPTS and row["delivered_at"] is None
    assert ue.deliver_pending(db, now_ms=now + 10**9, sender=boom) == {"sent": 0, "retried": 0, "failed": 0, "skipped": 0}


def test_a_channel_that_is_not_configured_is_not_reported_delivered_and_recovers(db):
    enable(db, "alice", "telegram", "trade")
    event = emit(db)
    assert ue.deliver_pending(db, now_ms=T0 + 1, sender=lambda *a: False)["retried"] == 1
    [row] = ue.outbox(db, event_ids=[event["event_id"]], status="pending")
    assert row["last_error"] == "CHANNEL_NOT_CONFIGURED_OR_REFUSED"
    assert ue.deliver_pending(db, now_ms=row["next_attempt_at"], sender=lambda *a: True)["sent"] == 1


def test_an_endpoint_removed_before_delivery_fails_the_row(db):
    enable(db, "alice", "email", "trade")
    event = emit(db)
    with db.connect() as c:
        c.execute("DELETE FROM notification_endpoints")
    assert ue.deliver_pending(db, now_ms=T0 + 1, sender=lambda *a: pytest.fail("no recipient"))["failed"] == 1
    assert [r["last_error"] for r in ue.outbox(db, event_ids=[event["event_id"]]) if r["channel"] == "email"] == ["NO_RECIPIENT"]


def test_entry_blocked_is_rate_limited_per_account_on_external_channels_only(db):
    enable(db, "alice", "email", "system")
    enable(db, "bob", "email", "system")
    first = emit(db, ue.ENTRY_BLOCKED, key="d1")
    second = emit(db, ue.ENTRY_BLOCKED, key="d2", now=T0 + 60_000)
    other = emit(db, ue.ENTRY_BLOCKED, key="d3", user="bob", now=T0 + 60_000)
    later = emit(db, ue.ENTRY_BLOCKED, key="d4", now=T0 + ue.ENTRY_BLOCKED_WINDOW_MS + 1)
    status = lambda e: {r["channel"]: r["status"] for r in ue.outbox(db, event_ids=[e["event_id"]])}
    assert status(first) == {"email": "pending", "in_app": "sent"}
    assert status(second) == {"email": "suppressed", "in_app": "sent"}       # still visible in the portal
    assert status(other) == {"email": "pending", "in_app": "sent"}           # another account is not affected
    assert status(later) == {"email": "pending", "in_app": "sent"}
    assert ue.deliver_pending(db, now_ms=T0 + ue.ENTRY_BLOCKED_WINDOW_MS + 2, sender=lambda *a: True)["sent"] == 3


def test_the_notification_worker_delivers_the_outbox_and_survives_its_failure(db, monkeypatch):
    enable(db, "alice", "email", "trade")
    emit(db)
    calls = []
    monkeypatch.setattr(ue, "deliver_pending", lambda database: calls.append(database) or {"sent": 1, "retried": 0, "failed": 0})
    worker = NotificationWorker(db)
    monkeypatch.setattr(worker, "_process_legacy_jobs", lambda: 2)
    assert worker._process_batch_sync() == 3 and calls == [db]
    monkeypatch.setattr(ue, "deliver_pending", lambda database: (_ for _ in ()).throw(RuntimeError("outbox broken")))
    assert worker._process_batch_sync() == 2                               # the legacy jobs still ran


# ── SSE ─────────────────────────────────────────────────────────────────────

def test_every_customer_event_is_streamed_and_only_to_its_owner(db, bus):
    assert {EventType(t) for t in ue.EVENT_TYPES} <= sse.ANALYTICS_EVENTS
    emit(db)
    [event] = bus.events
    assert sse.event_visible_to(event, "alice", False) and not sse.event_visible_to(event, "bob", False)
    assert sse.event_visible_to(event, "ops", True)


def test_the_event_store_writes_to_either_table_shape(tmp_path, bus):
    def store_for(path):
        store = object.__new__(EventStore)
        store._db_path = path
        return store
    canonical = str(tmp_path / "canonical.db")
    migrate(DB(canonical))
    store_for(canonical).emit(Event(event_type=EventType.BOT_PAUSED, level=EventLevel.INFO, payload={"a": 1}, symbol="X"))
    with sqlite3.connect(canonical) as c:
        assert c.execute("SELECT event_type, symbol FROM events").fetchall() == [("BOT_PAUSED", "X")]
    own = str(tmp_path / "own.db")
    with sqlite3.connect(own) as c:
        c.execute("CREATE TABLE events (event_id TEXT PRIMARY KEY, run_id TEXT, trade_id TEXT, cycle_id INTEGER, ts TEXT NOT NULL, "
                  "event_type TEXT NOT NULL, level TEXT NOT NULL, symbol TEXT, timeframe TEXT, strategy TEXT, mode TEXT, payload_json TEXT)")
    store_for(own).emit(Event(event_type=EventType.ENTRY_FILLED, level=EventLevel.INFO, payload={"a": 1}))
    with sqlite3.connect(own) as c:
        assert c.execute("SELECT event_type, payload_json FROM events").fetchall() == [("ENTRY_FILLED", '{"a": 1}')]
    assert len(bus.events) == 2


# ── the bot lifecycle ───────────────────────────────────────────────────────

def test_pause_resume_and_stop_each_emit_once_after_the_status_is_committed(db, bus, monkeypatch):
    monkeypatch.setattr(broker_capability_gate, "assert_broker_execution_capability", lambda *a, **kw: None)
    service = BotInstanceService(db=db)
    with db.connect() as c:
        columns = {r[1] for r in c.execute("PRAGMA table_info(bot_instances)")}
        row = {"id": "bot-1", "user_id": "alice", "broker_account_id": "acct-alice", "name": "b", "status": "active", "mode": "paper",
               "environment": "demo", "created_at": NOW_ISO, "updated_at": NOW_ISO, "config_json": "{}", "symbols_json": "[]",
               "market_type": "crypto", "broker_id": "binance", "strategy_id": "cati", "strategy_version": "1", "config_id": "c",
               "risk_profile_id": "r", "timeframes_json": "[]", "allocation_type": "fixed_amount", "allocation_value": 100.0}
        row = {k: v for k, v in row.items() if k in columns}
        c.execute(f"INSERT INTO bot_instances ({','.join(row)}) VALUES ({','.join('?' for _ in row)})", tuple(row.values()))
    seen = []
    original = ue.emit

    def checked(database, kind, **kw):
        with database.connect() as c:                                     # the status is already committed
            seen.append((kind, c.execute("SELECT status FROM bot_instances WHERE id='bot-1'").fetchone()[0]))
        return original(database, kind, **kw)
    monkeypatch.setattr(ue, "emit", checked)
    service.pause_bot_instance("bot-1")
    service.start_bot_instance("bot-1")
    service.stop_bot_instance("bot-1")
    assert seen == [("BOT_PAUSED", "paused"), ("BOT_RESUMED", "active"), ("BOT_STOPPED", "stopped")]
    kinds = [e["event_type"] for e in reversed(ue.recent(db, user_id="alice", bot_id="bot-1"))]
    assert sorted(kinds) == ["BOT_PAUSED", "BOT_RESUMED", "BOT_STOPPED"] and ue.recent(db, user_id="bob") == []
    with db.connect() as c:
        assert c.execute("SELECT stopped_reason FROM bot_instances WHERE id='bot-1'").fetchone()[0] == "USER_STOP"
    monkeypatch.setattr(ue, "emit", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("events down")))
    assert service.pause_bot_instance("bot-1").status == "paused"          # an event failure never fails the action


# ── the production path ─────────────────────────────────────────────────────

def events_of(h, kind=None):
    rows = ue.recent(h.db, user_id=h.account["user_id"], limit=200)
    return [r for r in rows if kind is None or r["event_type"] == kind]


def test_a_blocked_qualifying_entry_is_reported_once_per_decision_and_reason(fresh, bus):
    boundary = fresh.boundary_for()
    boundary.authority.gov = SimpleNamespace(kill_switch_on=lambda **kw: True)
    for _ in range(3):
        assert process(fresh)["reason"] == "CATI_NEW_ENTRY_KILL_SWITCH"
    [event] = events_of(fresh, "ENTRY_BLOCKED")
    assert event["payload"]["reason"] == "CATI_NEW_ENTRY_KILL_SWITCH" and event["payload"]["decision_id"] == fresh.row["decision_id"]
    assert event["broker_account_id"] == fresh.account["id"] and event["environment"] == "DEMO" and event["bot_id"]
    fresh.client.place_order.assert_not_called()


def test_no_event_when_nothing_qualifies_or_the_entry_goes_through(fresh, bus):
    process(fresh)
    assert fresh.client.place_order.call_count == 1 and events_of(fresh, "ENTRY_BLOCKED") == []


def test_an_event_failure_never_costs_the_entry(fresh, monkeypatch):
    monkeypatch.setattr(ue, "emit", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("events down")))
    boundary = fresh.boundary_for()
    boundary.authority.gov = SimpleNamespace(kill_switch_on=lambda **kw: True)
    assert process(fresh)["reason"] == "CATI_NEW_ENTRY_KILL_SWITCH"


def test_fills_are_reported_once_across_cycles_and_restarts(measured, monkeypatch, bus):
    m = measured
    waiting_for_signal(monkeypatch)
    # The certification fake confirms its close only before a runtime cycle has
    # run, so the order is hold -> close -> cycles (as in the read-model suite).
    cert.run(m.h.db, m.h.account["id"], "events", action="hold")
    cert.run(m.h.db, m.h.account["id"], "events", action="close")
    m.cycle("closed")
    m.cycle("again")
    [entry] = events_of(m.h, "ENTRY_FILLED")
    assert entry["payload"]["symbol"] == "ADAUSDT" and entry["payload"]["quantity"] > 0
    assert entry["payload"]["entry_price"] == pytest.approx(100.0) and entry["environment"] == "DEMO"
    assert events_of(m.h, "POSITION_UNPROTECTED") == []
    m.cycle("closed_again", db=DB(m.h.db.path))                             # a restarted process replays the same history
    assert len(events_of(m.h, "ENTRY_FILLED")) == 1
    [exit_] = events_of(m.h, "EXIT_FILLED")
    assert exit_["payload"]["trade_plan_id"] == entry["payload"]["trade_plan_id"] and exit_["payload"]["exit_price"] == pytest.approx(100.0)
    assert exit_["at"] >= entry["at"]


def test_an_unreadable_stop_alerts_only_after_the_bound_and_a_proven_absence_at_once(measured, monkeypatch, bus):
    from app.execution import protection_state
    m = measured
    waiting_for_signal(monkeypatch)
    cert.run(m.h.db, m.h.account["id"], "events", action="hold")
    m.cycle("protected")
    m.cycle("protected_again")
    [entry] = events_of(m.h, "ENTRY_FILLED")                                # an open, protected trade: one entry, no exit
    assert entry["payload"]["protection"] == "CONFIRMED" and events_of(m.h, "EXIT_FILLED") == []
    m.h.client.get_algo_orders = lambda *a, **k: (_ for _ in ()).throw(TimeoutError("read timed out"))
    for i in range(protection_state.UNKNOWN_ALERT_CYCLES - 1):
        m.cycle(f"unknown-{i}")
    assert events_of(m.h, "POSITION_UNPROTECTED") == []                    # a failed read is not an alarm yet
    for i in range(3):
        m.cycle(f"bound-{i}")
    [event] = events_of(m.h, "POSITION_UNPROTECTED")
    assert event["payload"]["state"] == "UNKNOWN" and event["payload"]["reason"] == "PROTECTION_READ_UNAVAILABLE"
    assert "fail_safe_close" not in event["payload"]                        # the position was kept


# ── the API ─────────────────────────────────────────────────────────────────

def test_the_bot_events_route_is_owner_scoped(db):
    from fastapi import HTTPException
    service = BotInstanceService(db=db)
    emit(db)
    emit(db, user="bob", key="plan-2")
    bot = SimpleNamespace(id="bot-alice", user_id="alice")
    service.get_bot_instance = lambda bot_id: bot if bot_id == "bot-alice" else None
    out = cati_account.bot_events("bot-alice", limit=50, user={"id": "alice"}, service=service, _perm="bot:read")
    assert [e["user_id"] for e in out["events"]] == ["alice"] and out["bot_id"] == "bot-alice"
    with pytest.raises(HTTPException) as denied:
        cati_account.bot_events("bot-alice", limit=50, user={"id": "bob"}, service=service, _perm="bot:read")
    assert denied.value.status_code == 404


# ── the daily loss latch ────────────────────────────────────────────────────

def test_the_daily_loss_pause_is_reported_once_on_the_latch_transition(live, bus):
    from unittest.mock import Mock
    from _exec import cati_bot
    from app.trading_intelligence.integration import production_execution as production
    production.initialize(live.db)
    client = Mock()
    client.account.return_value = dict(totalMarginBalance=4875, totalWalletBalance=4875, availableBalance=4875,
                                       totalInitialMargin=0, totalUnrealizedProfit=0)
    client.income_history.return_value = [{"incomeType": "REALIZED_PNL", "income": "-125", "time": live.now}]
    cati_bot(live.db, "a", "loss")
    account, bots = {"id": "loss", "user_id": "u1", "environment": "demo"}, [{"id": "a", "user_id": "u1"}]
    for _ in range(3):                                                    # the latch persists; the event does not repeat
        assert production.account_risk(live.db, account, client, [], [], bots, live.now)["loss_latched"]
    [event] = [e for e in ue.recent(live.db, user_id="u1") if e["event_type"] == "DAILY_LOSS_PAUSE"]
    assert event["bot_id"] == "a" and event["broker_account_id"] == "loss" and event["environment"] == "DEMO"
    assert event["payload"]["loss_usdt"] == pytest.approx(125.0) and event["payload"]["limit_usdt"] == pytest.approx(125.0)
    [alert] = alerts(live.db, "DAILY_LOSS_PAUSE")
    assert alert["severity"] == "WARNING" and "no new entries" in alert["message"]
