"""Customer-facing production events and their durable delivery (Step 1.8).

Events (the master plan's names):

    ENTRY_FILLED  EXIT_FILLED  POSITION_UNPROTECTED  BOT_PAUSED  BOT_RESUMED
    BOT_STOPPED   DAILY_LOSS_PAUSE  ENTRY_BLOCKED

Every event represents a COMMITTED state change: the emitter is called after
the block that recorded the fill / status / latch has committed, never before.
``emit`` is idempotent on a deterministic ``event_id`` (the same state change
replayed after a restart writes nothing twice) and never raises: a failure here
is logged and the trade path continues (protection, fills, closes and the
emergency close do not depend on it).

Delivery is an outbox: ``emit`` writes the event to the ``user_events`` table
and one ``notification_outbox`` row per channel the user has enabled (in-app
always, email / telegram / push when enabled in ``notification_preferences``
and an endpoint exists), plus a real-time SSE broadcast. ``deliver_pending``
(run by the existing notification worker) sends each outbox row through the
existing channel senders with bounded exponential retry; a channel that is not
configured is recorded as ``CHANNEL_NOT_CONFIGURED`` and never reported as
delivered. ``ENTRY_BLOCKED`` is aggregated: one event per account, decision and
reason, and at most one notification per account per ``ENTRY_BLOCKED_WINDOW_MS``.
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

ENTRY_FILLED = "ENTRY_FILLED"
EXIT_FILLED = "EXIT_FILLED"
POSITION_UNPROTECTED = "POSITION_UNPROTECTED"
BOT_PAUSED = "BOT_PAUSED"
BOT_RESUMED = "BOT_RESUMED"
BOT_STOPPED = "BOT_STOPPED"
DAILY_LOSS_PAUSE = "DAILY_LOSS_PAUSE"
ENTRY_BLOCKED = "ENTRY_BLOCKED"
EVENT_TYPES = (ENTRY_FILLED, EXIT_FILLED, POSITION_UNPROTECTED, BOT_PAUSED, BOT_RESUMED, BOT_STOPPED,
               DAILY_LOSS_PAUSE, ENTRY_BLOCKED)
#: category / severity per event (drives channel preferences and templates)
CLASSIFICATION = {
    ENTRY_FILLED: ("trade", "INFO"), EXIT_FILLED: ("trade", "INFO"), POSITION_UNPROTECTED: ("risk", "CRITICAL"),
    BOT_PAUSED: ("system", "INFO"), BOT_RESUMED: ("system", "INFO"), BOT_STOPPED: ("system", "INFO"),
    DAILY_LOSS_PAUSE: ("risk", "WARNING"), ENTRY_BLOCKED: ("system", "INFO"),
}
#: Channels delivered by the worker (in_app is written at emit time; sse is broadcast at emit time).
EXTERNAL_CHANNELS = ("email", "telegram", "push")
ENTRY_BLOCKED_WINDOW_MS = 3_600_000
MAX_ATTEMPTS = 8
BACKOFF_SECONDS = (30, 60, 120, 300, 600, 1800, 3600, 7200)

EVENTS_TABLE = "user_events"
OUTBOX_TABLE = "notification_outbox"
DDL = (
    f"""CREATE TABLE IF NOT EXISTS {EVENTS_TABLE} (
        event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, at INTEGER NOT NULL, user_id TEXT NOT NULL,
        bot_id TEXT, broker_account_id TEXT, environment TEXT, payload_json TEXT NOT NULL, created_at TEXT NOT NULL)""",
    f"CREATE INDEX IF NOT EXISTS idx_{EVENTS_TABLE}_user_at ON {EVENTS_TABLE}(user_id, at)",
    f"CREATE INDEX IF NOT EXISTS idx_{EVENTS_TABLE}_bot_at ON {EVENTS_TABLE}(bot_id, at)",
    f"""CREATE TABLE IF NOT EXISTS {OUTBOX_TABLE} (
        outbox_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, user_id TEXT NOT NULL, channel TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at INTEGER NOT NULL,
        last_error TEXT, created_at INTEGER NOT NULL, delivered_at INTEGER, recipient TEXT,
        dedupe_key TEXT NOT NULL UNIQUE)""",
    f"CREATE INDEX IF NOT EXISTS idx_{OUTBOX_TABLE}_due ON {OUTBOX_TABLE}(status, next_attempt_at)",
)


def initialize(conn) -> None:
    for statement in DDL:
        conn.execute(statement)


def _retry_on_missing_table(db, work):
    """Run ``work(conn)`` once; create the tables and retry on the first miss only."""
    with db.connect() as c:
        try:
            return work(c)
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc):
                raise
            initialize(c)
            return work(c)


def event_id_for(event_type: str, *parts: Any) -> str:
    """Deterministic identity of one state change (idempotent replay)."""
    raw = ":".join([event_type, *[str(p) for p in parts]])
    return "evt_" + hashlib.sha256(raw.encode()).hexdigest()[:24]


def _user_channels(conn, user_id: str, category: str) -> List[str]:
    """in_app always; email / telegram / push when the preference says so AND an endpoint exists."""
    channels = ["in_app", "sse"]
    try:
        rows = conn.execute("SELECT channel, is_enabled FROM notification_preferences WHERE user_id=? AND category=?",
                            (user_id, category)).fetchall()
        enabled = {r[0]: bool(r[1]) for r in rows}
        if "in_app" in enabled and not enabled["in_app"]:
            channels.remove("in_app")
        endpoints = {r[0] for r in conn.execute("SELECT channel FROM notification_endpoints WHERE user_id=? AND status='active'",
                                                 (user_id,)).fetchall()}
        for channel in EXTERNAL_CHANNELS:
            if enabled.get(channel) and channel in endpoints:
                channels.append(channel)
    except sqlite3.OperationalError:
        pass  # a database without the preference tables keeps the in-app default
    return channels


def _write_in_app(conn, event: Dict[str, Any]) -> None:
    category, severity = CLASSIFICATION[event["event_type"]]
    payload = event["payload"]
    message = payload.get("message") or event["event_type"].replace("_", " ").capitalize()
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()}
    except sqlite3.OperationalError:
        return
    if not cols:
        return
    row = {"ts": event["created_at"], "alert_type": event["event_type"], "severity": severity,
           "trace_id": event["event_id"], "symbol": payload.get("symbol"), "message": message,
           "details_json": json.dumps({**payload, "bot_id": event["bot_id"], "broker_account_id": event["broker_account_id"],
                                       "environment": event["environment"], "category": category}, default=str)}
    if "user_id" in cols:
        row["user_id"] = event["user_id"]
    row = {k: v for k, v in row.items() if k in cols}
    conn.execute(f"INSERT INTO alerts ({', '.join(row)}) VALUES ({', '.join('?' for _ in row)})", tuple(row.values()))


def _broadcast(event: Dict[str, Any]) -> None:
    """Real-time fan-out to SSE subscribers (owner-filtered by the stream). Never raises."""
    try:
        from shared_lib.persistence.event_broadcaster import get_event_broadcaster
        from shared_lib.persistence.events import Event, EventLevel, EventType
        kind = EventType(event["event_type"])
        level = {"INFO": EventLevel.INFO, "WARNING": EventLevel.WARN, "CRITICAL": EventLevel.ERROR}.get(
            CLASSIFICATION[event["event_type"]][1], EventLevel.INFO)
        payload = {**event["payload"], "user_id": event["user_id"], "bot_id": event["bot_id"], "bot_instance_id": event["bot_id"],
                   "broker_account_id": event["broker_account_id"], "environment": event["environment"], "at": event["at"]}
        get_event_broadcaster().broadcast(Event(event_type=kind, level=level, payload=payload,
                                                symbol=event["payload"].get("symbol"), event_id=event["event_id"],
                                                trade_id=event["payload"].get("trade_plan_id")))
    except Exception:
        logger.debug("[USER_EVENTS] broadcast skipped", exc_info=True)


def emit(db, event_type: str, *, event_id: str, user_id: Optional[str], bot_id: Optional[str], broker_account_id: Optional[str],
         environment: Optional[str], payload: Dict[str, Any], now_ms: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """Record one committed state change for its user. Returns the event when it
    was NEW, None when it was already recorded or could not be recorded. Never raises."""
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unknown user event type {event_type!r}")
    if not user_id:
        logger.warning("[USER_EVENTS] %s without a user (bot=%s account=%s): not emitted", event_type, bot_id, broker_account_id)
        return None
    now = int(time.time() * 1000) if now_ms is None else int(now_ms)
    created_at = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now / 1000)) + f".{now % 1000:03d}+00:00"
    safe_payload = {k: v for k, v in (payload or {}).items() if not any(s in str(k).lower() for s in ("secret", "api_key", "token", "password"))}
    event = {"event_id": event_id, "event_type": event_type, "at": now, "user_id": str(user_id), "bot_id": bot_id,
             "broker_account_id": broker_account_id, "environment": environment, "payload": safe_payload, "created_at": created_at}
    try:
        def work(c):
            inserted = c.execute(f"INSERT OR IGNORE INTO {EVENTS_TABLE} VALUES(?,?,?,?,?,?,?,?,?)",
                                 (event_id, event_type, now, str(user_id), bot_id, broker_account_id, environment,
                                  json.dumps(safe_payload, default=str), created_at)).rowcount
            if not inserted:
                return False
            category, _ = CLASSIFICATION[event_type]
            for channel in _user_channels(c, str(user_id), category):
                if channel == "sse":
                    continue
                suppressed = event_type == ENTRY_BLOCKED and channel != "in_app" and _entry_blocked_recently(c, broker_account_id, channel, now)
                if channel == "in_app":
                    _write_in_app(c, event)
                    status, delivered = "sent", now
                else:
                    status, delivered = ("suppressed", None) if suppressed else ("pending", None)
                c.execute(f"INSERT OR IGNORE INTO {OUTBOX_TABLE} (outbox_id, event_id, user_id, channel, status, attempts, "
                          "next_attempt_at, last_error, created_at, delivered_at, recipient, dedupe_key) VALUES(?,?,?,?,?,0,?,?,?,?,NULL,?)",
                          (f"obx_{uuid.uuid4().hex[:16]}", event_id, str(user_id), channel, status, now,
                           "ENTRY_BLOCKED_RATE_LIMITED" if suppressed else None, now, delivered, f"{event_id}:{channel}"))
            return True
        new = _retry_on_missing_table(db, work)
    except Exception:
        logger.exception("[USER_EVENTS] %s could not be recorded (event_id=%s)", event_type, event_id)
        return None
    if not new:
        return None
    _broadcast(event)
    return event


def _entry_blocked_recently(conn, broker_account_id: Optional[str], channel: str, now: int) -> bool:
    if not broker_account_id:
        return False
    row = conn.execute(f"SELECT 1 FROM {OUTBOX_TABLE} o JOIN {EVENTS_TABLE} e ON e.event_id=o.event_id WHERE e.event_type=? "
                       "AND e.broker_account_id=? AND o.channel=? AND o.status IN ('pending','sent') AND e.at > ? LIMIT 1",
                       (ENTRY_BLOCKED, broker_account_id, channel, now - ENTRY_BLOCKED_WINDOW_MS)).fetchone()
    return row is not None


# ── delivery (outbox) ───────────────────────────────────────────────────────

def _recipient(conn, user_id: str, channel: str) -> Optional[str]:
    try:
        row = conn.execute("SELECT recipient FROM notification_endpoints WHERE user_id=? AND channel=? AND status='active'",
                           (user_id, channel)).fetchone()
    except sqlite3.OperationalError:
        return None
    return row[0] if row else None


def _render(event: Dict[str, Any]) -> Dict[str, str]:
    p = event["payload"]
    kind = event["event_type"]
    symbol = p.get("symbol") or ""
    env = f" [{event.get('environment')}]" if event.get("environment") else ""
    titles = {
        ENTRY_FILLED: f"Entry filled {symbol}{env}", EXIT_FILLED: f"Exit filled {symbol}{env}",
        POSITION_UNPROTECTED: f"Position protection needs attention {symbol}{env}", BOT_PAUSED: "Bot paused",
        BOT_RESUMED: "Bot resumed", BOT_STOPPED: "Bot stopped", DAILY_LOSS_PAUSE: "Daily loss limit reached",
        ENTRY_BLOCKED: f"Entry not taken {symbol}{env}",
    }
    body = p.get("message") or json.dumps({k: v for k, v in p.items() if k != "message"}, default=str)
    return {"subject": f"CosmicForge: {titles.get(kind, kind)}", "text": body}


def _send(channel: str, recipient: str, rendered: Dict[str, str], event: Dict[str, Any]) -> bool:
    if channel == "email":
        from shared_lib.notifications.channels.email import EmailChannel
        return bool(EmailChannel.send(recipient, rendered["subject"], rendered["text"], rendered["text"]))
    if channel == "telegram":
        from shared_lib.notifications.channels.telegram import TelegramChannel
        return bool(TelegramChannel.send(recipient, f"{rendered['subject']}\n{rendered['text']}"))
    if channel == "push":
        from shared_lib.notifications.channels.push import PushChannel
        return bool(PushChannel.send(recipient, rendered["subject"], rendered["text"], {"event_id": event["event_id"],
                                                                                        "event_type": event["event_type"]}))
    raise ValueError(f"unknown channel {channel}")


def deliver_pending(db, *, now_ms: Optional[int] = None, limit: int = 50, sender=_send) -> Dict[str, int]:
    """Deliver due outbox rows through the configured channels with bounded
    retry. Never raises; returns counts. A channel the sender reports as not
    configured (returns False without raising) is retried like a transient
    failure until the attempts are exhausted, and never marked delivered."""
    now = int(time.time() * 1000) if now_ms is None else int(now_ms)
    out = {"sent": 0, "retried": 0, "failed": 0, "skipped": 0}
    try:
        def due(c):
            return [dict(r) for r in c.execute(
                f"SELECT o.*, e.event_type, e.payload_json, e.environment FROM {OUTBOX_TABLE} o JOIN {EVENTS_TABLE} e "
                "ON e.event_id=o.event_id WHERE o.status='pending' AND o.next_attempt_at<=? AND o.channel IN ('email','telegram','push') "
                "ORDER BY o.next_attempt_at LIMIT ?", (now, int(limit)))]
        rows = _retry_on_missing_table(db, due)
    except Exception:
        logger.exception("[USER_EVENTS] outbox could not be read")
        return out
    for row in rows:
        event = {"event_id": row["event_id"], "event_type": row["event_type"], "environment": row["environment"],
                 "payload": json.loads(row["payload_json"] or "{}")}
        with db.connect() as c:
            recipient = row.get("recipient") or _recipient(c, row["user_id"], row["channel"])
        attempts = int(row["attempts"]) + 1
        if not recipient:
            with db.connect() as c:
                c.execute(f"UPDATE {OUTBOX_TABLE} SET status='failed', attempts=?, last_error='NO_RECIPIENT' WHERE outbox_id=?",
                          (attempts, row["outbox_id"]))
            out["failed"] += 1
            continue
        try:
            ok = sender(row["channel"], recipient, _render(event), event)
            error = None if ok else "CHANNEL_NOT_CONFIGURED_OR_REFUSED"
        except Exception as exc:
            ok, error = False, f"{type(exc).__name__}: {str(exc)[:160]}"
        with db.connect() as c:
            if ok:
                c.execute(f"UPDATE {OUTBOX_TABLE} SET status='sent', attempts=?, delivered_at=?, recipient=?, last_error=NULL WHERE outbox_id=?",
                          (attempts, now, recipient, row["outbox_id"]))
                out["sent"] += 1
            elif attempts >= MAX_ATTEMPTS:
                c.execute(f"UPDATE {OUTBOX_TABLE} SET status='failed', attempts=?, last_error=?, recipient=? WHERE outbox_id=?",
                          (attempts, error, recipient, row["outbox_id"]))
                out["failed"] += 1
            else:
                delay = BACKOFF_SECONDS[min(attempts - 1, len(BACKOFF_SECONDS) - 1)] * 1000
                c.execute(f"UPDATE {OUTBOX_TABLE} SET attempts=?, next_attempt_at=?, last_error=?, recipient=? WHERE outbox_id=?",
                          (attempts, now + delay, error, recipient, row["outbox_id"]))
                out["retried"] += 1
    return out


# ── reads ───────────────────────────────────────────────────────────────────

def recent(db, *, user_id: str, bot_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    """The user's recent events, newest first (owner-scoped)."""
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name=?", (EVENTS_TABLE,)).fetchone():
            return []
        sql, args = f"SELECT * FROM {EVENTS_TABLE} WHERE user_id=?", [str(user_id)]
        if bot_id:
            sql += " AND bot_id=?"
            args.append(bot_id)
        rows = [dict(r) for r in c.execute(sql + " ORDER BY at DESC LIMIT ?", (*args, int(limit)))]
    for r in rows:
        r["payload"] = json.loads(r.pop("payload_json") or "{}")
    return rows


def outbox(db, *, event_ids: Optional[Iterable[str]] = None, status: Optional[str] = None) -> List[Dict[str, Any]]:
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name=?", (OUTBOX_TABLE,)).fetchone():
            return []
        sql, args = f"SELECT * FROM {OUTBOX_TABLE} WHERE 1=1", []
        if event_ids is not None:
            ids = list(event_ids)
            if not ids:
                return []
            sql += f" AND event_id IN ({','.join('?' for _ in ids)})"
            args += ids
        if status:
            sql += " AND status=?"
            args.append(status)
        return [dict(r) for r in c.execute(sql + " ORDER BY created_at, channel", args)]


__all__ = ["EVENT_TYPES", "ENTRY_FILLED", "EXIT_FILLED", "POSITION_UNPROTECTED", "BOT_PAUSED", "BOT_RESUMED", "BOT_STOPPED",
           "DAILY_LOSS_PAUSE", "ENTRY_BLOCKED", "ENTRY_BLOCKED_WINDOW_MS", "MAX_ATTEMPTS", "EVENTS_TABLE", "OUTBOX_TABLE",
           "initialize", "event_id_for", "emit", "deliver_pending", "recent", "outbox"]
