"""Production alerting engine (Section H Step 8.5).

Stored in the platform database (``operations_schema`` migrations 30-31), so every API process and worker on
SQLite or PostgreSQL sees one set of alerts, one dedup state and one escalation clock -- the **database server's**
clock (``conn.now_ms()``) unless a caller injects ``now_ms`` (tests, replays).

Lifecycle of an alert::

    raise_alert -> OPEN --acknowledge(actor)--> ACKNOWLEDGED --resolve(actor)--> RESOLVED
                   OPEN --resolve(actor)--> RESOLVED

* **Categories** (``CATEGORIES``) fix a default severity, the on-call rotation an alert routes to and the public
  status-page component it may affect (``None``: never shown publicly).
* **Deduplication**: the fingerprint is ``sha256(category, key)``. At most one unresolved alert exists per
  fingerprint (a partial unique index enforces it). A repeat signal increments ``occurrences``; inside
  ``dedup_window_ms`` of the last notification it is silent, after the window an OPEN alert is re-notified
  (``REMINDER``) because the condition is still there and nobody acknowledged it.
* **Escalation**: an alert left OPEN (unacknowledged) for ``ESCALATION_MINUTES[severity][n]`` minutes after it
  was first seen moves to level ``n+1`` and is delivered to that level. Level 2 with nobody assigned in the
  rotation goes to the ``operations-owner`` rotation. Acknowledging stops escalation.
* **Routing to nobody**: a rotation without an assigned primary resolves to ``OPERATIONS_OWNER_REQUIRED``. The
  alert is still delivered to the configured channels, marked ``owner_required``, and a CRITICAL
  ``OPERATIONS_OWNER_REQUIRED`` configuration alert is raised for that rotation.
* **Delivery** goes through pluggable ``AlertSink`` objects (the notify_failure channels, a NOTIFICATION job on
  the platform JobQueue, ...). Each (alert, reason, level, sink) is one ``ops_alert_deliveries`` row with an
  idempotency key; a failing send is retried with backoff and, after ``max_attempts``, ABANDONED -- which itself
  raises ``ALERT_DELIVERY_FAILURE``.
* **Event log**: every transition, dedup, delivery outcome and escalation is appended to ``ops_alert_events``
  (append-only in the database). Actors are recorded for acknowledge/resolve.

Alert details pass through secret redaction before they are stored; an alert never carries a credential.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple

from shared_lib.core.security.redaction import redact_mapping, redact_text
from shared_lib.persistence.sql import POSTGRESQL, Connection, Database

from .oncall import OPERATIONS_OWNER_REQUIRED, OncallDirectory, Resolution
from .secrets_scan import find_secrets

logger = logging.getLogger(__name__)

CRITICAL, HIGH, MEDIUM, LOW = "CRITICAL", "HIGH", "MEDIUM", "LOW"
SEVERITIES = (CRITICAL, HIGH, MEDIUM, LOW)
OPEN, ACKNOWLEDGED, RESOLVED = "OPEN", "ACKNOWLEDGED", "RESOLVED"

# Public status-page components (see status_page.COMPONENTS).
PORTAL, TRADING_ENGINE, MARKET_DATA, EXCHANGE_CONNECTIVITY, NOTIFICATIONS = (
    "PORTAL", "TRADING_ENGINE", "MARKET_DATA", "EXCHANGE_CONNECTIVITY", "NOTIFICATIONS")


@dataclass(frozen=True)
class CategoryPolicy:
    category: str
    severity: str
    rotation_key: str
    component: Optional[str]
    description: str


def _p(category, severity, rotation, component, description):
    return category, CategoryPolicy(category, severity, rotation, component, description)


CATEGORIES: Dict[str, CategoryPolicy] = dict([
    _p("EXCHANGE_EXECUTION_UNCERTAINTY", CRITICAL, "trading-operations", EXCHANGE_CONNECTIVITY,
       "An order's outcome at the venue is unknown (lost acknowledgement, timeout after send)."),
    _p("UNPROTECTED_POSITION", CRITICAL, "trading-operations", TRADING_ENGINE,
       "A position is open without a confirmed protective order."),
    _p("RECONCILIATION_MISMATCH", HIGH, "trading-operations", TRADING_ENGINE,
       "Venue state and the ledger disagree."),
    _p("KILL_SWITCH_ACTIVATED", HIGH, "trading-operations", TRADING_ENGINE,
       "A global, family or account kill switch was engaged."),
    _p("WORKER_OUTAGE", CRITICAL, "platform-infrastructure", TRADING_ENGINE,
       "A platform worker stopped heartbeating."),
    _p("LEASE_TAKEOVER_FAILURE", CRITICAL, "platform-infrastructure", TRADING_ENGINE,
       "Account assignments expired and no worker took them over."),
    _p("DATABASE_UNAVAILABLE", CRITICAL, "platform-infrastructure", TRADING_ENGINE,
       "The platform database does not answer its readiness probe."),
    _p("QUEUE_BACKLOG", HIGH, "platform-infrastructure", TRADING_ENGINE,
       "Durable jobs are overdue or dead-lettered."),
    _p("API_OUTAGE", CRITICAL, "platform-infrastructure", PORTAL, "A public API does not answer its health check."),
    _p("STALE_INSTRUMENT_RULES", HIGH, "market-data", MARKET_DATA,
       "Venue instrument rules were not refreshed within their freshness bound."),
    _p("STALE_MARKET_DATA", HIGH, "market-data", MARKET_DATA, "Market data is older than its freshness bound."),
    _p("AUTHENTICATION_ANOMALY", HIGH, "security", None,
       "Unusual authentication activity (failure bursts, lockouts, token misuse)."),
    _p("BILLING_WEBHOOK_FAILURE", HIGH, "billing", None, "Billing provider webhooks fail or are rejected."),
    _p("BACKUP_FAILURE", HIGH, "platform-infrastructure", None,
       "The newest verified backup is missing, failed or too old."),
    _p("DISK_GROWTH", MEDIUM, "platform-infrastructure", None,
       "A data volume is filling up or growing abnormally fast."),
    _p("EXCESSIVE_LATENCY", MEDIUM, "platform-infrastructure", PORTAL,
       "Request or cycle latency exceeds its bound."),
    _p("EXPIRED_EXCHANGE_CERTIFICATION", HIGH, "research-governance", None,
       "An exchange adapter certification has expired."),
    _p("GOVERNANCE_VIOLATION_ATTEMPT", HIGH, "security", None,
       "An action was refused because it would bypass research or execution governance."),
    _p(OPERATIONS_OWNER_REQUIRED, CRITICAL, "operations-owner", None,
       "An alert was routed to a rotation that has no assigned person."),
    _p("ALERT_DELIVERY_FAILURE", HIGH, "operations-owner", NOTIFICATIONS,
       "An alert could not be delivered through a channel after every retry."),
])

#: Minutes after first seen (unacknowledged) at which an alert escalates to level 1, then level 2.
ESCALATION_MINUTES: Dict[str, Tuple[int, ...]] = {CRITICAL: (5, 15), HIGH: (15, 60), MEDIUM: (60,), LOW: ()}
DEFAULT_DEDUP_WINDOW_MS = 10 * 60_000
DEFAULT_DELIVERY_ATTEMPTS = 6
DELIVERY_BACKOFF_MS = (30_000, 60_000, 300_000, 900_000, 1_800_000)
DELIVERY_CLAIM_MS = 120_000
_MAX_DETAIL_CHARS = 8_000


class AlertError(ValueError):
    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code


@dataclass(frozen=True)
class AlertMessage:
    """What a sink receives. Contains no credential; ``detail`` was redacted on the way in."""
    alert_id: str
    category: str
    severity: str
    title: str
    detail: Dict[str, Any]
    reason: str
    escalation_level: int
    rotation_key: str
    owner_required: bool
    targets: List[Dict[str, Any]]
    delivery_id: str
    occurrences: int

    def subject(self) -> str:
        owner = " [OPERATIONS OWNER REQUIRED]" if self.owner_required else ""
        return f"[{self.severity}] {self.category}: {self.title}{owner}"[:300]

    def text(self) -> str:
        who = ", ".join(t.get("person_name", "?") for t in self.targets) or OPERATIONS_OWNER_REQUIRED
        lines = [self.subject(), f"reason={self.reason} level={self.escalation_level} rotation={self.rotation_key}",
                 f"on-call: {who}", f"occurrences={self.occurrences} alert_id={self.alert_id}"]
        if self.detail:
            lines.append(redact_text(json.dumps(self.detail, sort_keys=True, default=str))[:1500])
        return "\n".join(lines)


class AlertSink(Protocol):
    name: str

    def send(self, message: AlertMessage) -> None:
        """Deliver or raise. A raise is retried with backoff."""


class CallableSink:
    def __init__(self, name: str, fn: Callable[[AlertMessage], None]) -> None:
        self.name = name
        self._fn = fn

    def send(self, message: AlertMessage) -> None:
        self._fn(message)


@dataclass
class RaiseResult:
    alert_id: str
    created: bool
    deduplicated: bool
    reminded: bool
    owner_required: bool
    deliveries: List[str] = field(default_factory=list)


def fingerprint(category: str, key: Any) -> str:
    body = json.dumps(key, sort_keys=True, default=str) if not isinstance(key, str) else key
    return hashlib.sha256(f"{category}\x00{body}".encode()).hexdigest()[:40]


def _scrub(value: Any) -> Any:
    """Second layer after ``redact_mapping``: a string that still looks like a credential is replaced whole."""
    if isinstance(value, dict):
        return {k: _scrub(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_scrub(v) for v in value]
    if isinstance(value, str):
        kinds = find_secrets(value, include_env_lines=True)
        return f"***REDACTED:{','.join(kinds)}***" if kinds else value
    return value


def _clean_text(text: str) -> str:
    text = redact_text(text)
    kinds = find_secrets(text)
    return f"***REDACTED:{','.join(kinds)}***" if kinds else text


def _clean_detail(detail: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    cleaned = _scrub(redact_mapping(dict(detail or {})))
    text = json.dumps(cleaned, sort_keys=True, default=str)
    if len(text) > _MAX_DETAIL_CHARS:
        return {"truncated": True, "excerpt": redact_text(text[:_MAX_DETAIL_CHARS])}
    return json.loads(text)


_ALERT_COLS = ("alert_id", "fingerprint", "category", "severity", "status", "title", "detail", "source", "scope",
               "account_ref", "tenant_ref", "rotation_key", "escalation_level", "owner_required", "occurrences",
               "first_seen_at", "last_seen_at", "last_notified_at", "next_escalation_at", "acknowledged_at",
               "acknowledged_by", "resolved_at", "resolved_by", "resolution_note")


def _alert_row(row) -> Dict[str, Any]:
    out = dict(zip(_ALERT_COLS, row))
    out["detail"] = json.loads(out["detail"]) if out["detail"] else {}
    out["owner_required"] = bool(out["owner_required"])
    return out


class AlertEngine:
    def __init__(self, db: Database, *, sinks: Sequence[AlertSink] = (), oncall: Optional[OncallDirectory] = None,
                 dedup_window_ms: int = DEFAULT_DEDUP_WINDOW_MS, max_delivery_attempts: int = DEFAULT_DELIVERY_ATTEMPTS,
                 escalation_minutes: Optional[Mapping[str, Tuple[int, ...]]] = None) -> None:
        self.db = db
        self.sinks = list(sinks)
        self.oncall = oncall or OncallDirectory(db)
        self.dedup_window_ms = int(dedup_window_ms)
        self.max_delivery_attempts = int(max_delivery_attempts)
        self.escalation_minutes = dict(escalation_minutes or ESCALATION_MINUTES)

    # ── helpers ───────────────────────────────────────────────────────────────────────────────────────
    @staticmethod
    def _now(conn: Connection, now_ms: Optional[int]) -> int:
        return conn.now_ms() if now_ms is None else int(now_ms)

    @staticmethod
    def _event(conn: Connection, alert_id: str, event: str, actor: str, at: int, *, level: Optional[int] = None,
               detail: Optional[Mapping[str, Any]] = None) -> None:
        conn.execute("INSERT INTO ops_alert_events (alert_id, event, actor, escalation_level, detail, at) "
                     "VALUES (?,?,?,?,?,?)", (alert_id, event, actor, level,
                                              json.dumps(dict(detail or {}), sort_keys=True, default=str), at))

    def _next_escalation(self, severity: str, level: int, first_seen: int) -> Optional[int]:
        steps = self.escalation_minutes.get(severity, ())
        return first_seen + steps[level] * 60_000 if level < len(steps) else None

    def _route(self, rotation_key: str, level: int) -> Resolution:
        res = self.oncall.resolve(rotation_key, level)
        if level >= 2 and rotation_key != "operations-owner" and res.resolved_level != 2:
            owner = self.oncall.resolve("operations-owner", 0)
            if not owner.owner_required:
                return Resolution(rotation_key, level, owner.targets, 2, False, "ESCALATED_TO_OPERATIONS_OWNER")
        return res

    def _queue(self, conn: Connection, alert_id: str, reason: str, level: int, seq: int, res: Resolution,
               now: int) -> List[str]:
        targets = [{**t.public(), "contact_address": t.contact_address} for t in res.targets]
        ids = []
        for sink in self.sinks:
            delivery_id = uuid.uuid4().hex
            key = f"{alert_id}:{reason}:{level}:{seq}:{sink.name}"
            cur = conn.execute(
                "INSERT INTO ops_alert_deliveries (delivery_id, idempotency_key, alert_id, reason, escalation_level, "
                "sink, targets, status, attempts, max_attempts, next_attempt_at, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?,'PENDING',0,?,?,?,?) ON CONFLICT (idempotency_key) DO NOTHING",
                (delivery_id, key, alert_id, reason, level, sink.name, json.dumps(targets, sort_keys=True),
                 self.max_delivery_attempts, now, now, now))
            if cur.rowcount == 1:
                ids.append(delivery_id)
                self._event(conn, alert_id, "DELIVERY_QUEUED", "system", now, level=level,
                            detail={"sink": sink.name, "reason": reason, "delivery_id": delivery_id})
        if not self.sinks:
            self._event(conn, alert_id, "NO_DELIVERY_CHANNEL", "system", now, level=level, detail={"reason": reason})
        conn.execute("UPDATE ops_alerts SET last_notified_at=? WHERE alert_id=?", (now, alert_id))
        return ids

    # ── raising ───────────────────────────────────────────────────────────────────────────────────────
    def raise_alert(self, category: str, *, key: Any, title: str, source: str,
                    detail: Optional[Mapping[str, Any]] = None, severity: Optional[str] = None,
                    account_ref: Optional[str] = None, tenant_ref: Optional[str] = None,
                    now_ms: Optional[int] = None) -> RaiseResult:
        policy = CATEGORIES.get(category)
        if policy is None:
            raise AlertError("UNKNOWN_ALERT_CATEGORY", category)
        severity = severity or policy.severity
        if severity not in SEVERITIES:
            raise AlertError("INVALID_SEVERITY", severity)
        if not source:
            raise AlertError("SOURCE_REQUIRED")
        fp = fingerprint(category, key)
        clean = _clean_detail(detail)
        title = _clean_text(title)[:300]
        res = self._route(policy.rotation_key, 0)
        with self.db.transaction() as conn:
            if conn.dialect == POSTGRESQL:
                conn.execute("SELECT pg_advisory_xact_lock(?)", (int(fp[:15], 16),))
            now = self._now(conn, now_ms)
            row = conn.one(f"SELECT {', '.join(_ALERT_COLS)} FROM ops_alerts WHERE fingerprint=? AND status<>'RESOLVED'"
                           + conn.lock_rows_suffix(), (fp,))
            if row is not None:
                alert = _alert_row(row)
                occurrences = alert["occurrences"] + 1
                conn.execute("UPDATE ops_alerts SET occurrences=?, last_seen_at=?, detail=? WHERE alert_id=?",
                             (occurrences, now, json.dumps(clean, sort_keys=True), alert["alert_id"]))
                last = alert["last_notified_at"] or alert["first_seen_at"]
                remind = alert["status"] == OPEN and now - last >= self.dedup_window_ms
                deliveries: List[str] = []
                if remind:
                    lvl = alert["escalation_level"]
                    self._event(conn, alert["alert_id"], "REMINDED", "system", now, level=lvl,
                                detail={"occurrences": occurrences})
                    deliveries = self._queue(conn, alert["alert_id"], "REMINDER", lvl, occurrences,
                                             self._route(alert["rotation_key"], lvl), now)
                else:
                    self._event(conn, alert["alert_id"], "DEDUPLICATED", "system", now,
                                detail={"occurrences": occurrences})
                return RaiseResult(alert["alert_id"], False, True, remind, alert["owner_required"], deliveries)
            alert_id = uuid.uuid4().hex
            scope = "ACCOUNT" if account_ref else "PLATFORM"
            conn.execute(
                "INSERT INTO ops_alerts (alert_id, fingerprint, category, severity, status, title, detail, source, scope, "
                "account_ref, tenant_ref, rotation_key, escalation_level, owner_required, occurrences, first_seen_at, "
                "last_seen_at, next_escalation_at) VALUES (?,?,?,?,'OPEN',?,?,?,?,?,?,?,0,?,1,?,?,?)",
                (alert_id, fp, category, severity, title, json.dumps(clean, sort_keys=True), source, scope, account_ref,
                 tenant_ref, policy.rotation_key, 1 if res.owner_required else 0, now, now,
                 self._next_escalation(severity, 0, now)))
            self._event(conn, alert_id, "RAISED", source, now, level=0,
                        detail={"severity": severity, "rotation_key": policy.rotation_key,
                                "targets": [t.person_name for t in res.targets]})
            if res.owner_required:
                self._event(conn, alert_id, OPERATIONS_OWNER_REQUIRED, "system", now, level=0,
                            detail={"rotation_key": policy.rotation_key})
            deliveries = self._queue(conn, alert_id, "RAISED", 0, 1, res, now)
        result = RaiseResult(alert_id, True, False, False, res.owner_required, deliveries)
        if res.owner_required and category != OPERATIONS_OWNER_REQUIRED:
            self.raise_alert(OPERATIONS_OWNER_REQUIRED, key={"rotation_key": policy.rotation_key},
                             title=f"OPERATIONS OWNER REQUIRED: '{policy.rotation_key}' has no assigned primary",
                             source="alerting.routing",
                             detail={"rotation_key": policy.rotation_key, "triggering_category": category,
                                     "triggering_alert_id": alert_id}, now_ms=now_ms)
        return result

    # ── human actions ─────────────────────────────────────────────────────────────────────────────────
    def acknowledge(self, alert_id: str, *, actor: str, note: Optional[str] = None,
                    now_ms: Optional[int] = None) -> Dict[str, Any]:
        if not (actor or "").strip():
            raise AlertError("ACTOR_REQUIRED")
        with self.db.transaction() as conn:
            now = self._now(conn, now_ms)
            row = conn.one("SELECT status FROM ops_alerts WHERE alert_id=?" + conn.lock_rows_suffix(), (alert_id,))
            if row is None:
                raise AlertError("ALERT_NOT_FOUND")
            if row[0] != OPEN:
                raise AlertError("ALERT_NOT_OPEN", f"alert is {row[0]}")
            conn.execute("UPDATE ops_alerts SET status='ACKNOWLEDGED', acknowledged_at=?, acknowledged_by=?, "
                         "next_escalation_at=NULL WHERE alert_id=?", (now, actor, alert_id))
            self._event(conn, alert_id, "ACKNOWLEDGED", actor, now, detail={"note": _clean_text(note or "")[:1000]})
        return self.get(alert_id)

    def resolve(self, alert_id: str, *, actor: str, note: str, now_ms: Optional[int] = None) -> Dict[str, Any]:
        if not (actor or "").strip():
            raise AlertError("ACTOR_REQUIRED")
        if not (note or "").strip():
            raise AlertError("RESOLUTION_NOTE_REQUIRED")
        with self.db.transaction() as conn:
            now = self._now(conn, now_ms)
            row = conn.one("SELECT status FROM ops_alerts WHERE alert_id=?" + conn.lock_rows_suffix(), (alert_id,))
            if row is None:
                raise AlertError("ALERT_NOT_FOUND")
            if row[0] == RESOLVED:
                raise AlertError("ALERT_ALREADY_RESOLVED")
            conn.execute("UPDATE ops_alerts SET status='RESOLVED', resolved_at=?, resolved_by=?, resolution_note=?, "
                         "next_escalation_at=NULL WHERE alert_id=?", (now, actor, _clean_text(note)[:2000], alert_id))
            self._event(conn, alert_id, "RESOLVED", actor, now, detail={"note": _clean_text(note)[:1000]})
        return self.get(alert_id)

    def resolve_condition(self, category: str, *, key: Any, actor: str = "system", note: str = "condition cleared",
                          now_ms: Optional[int] = None) -> bool:
        """Resolve the open alert for a fingerprint when an evaluator sees the condition cleared."""
        fp = fingerprint(category, key)
        with self.db.transaction(write=False) as conn:
            alert_id = conn.scalar("SELECT alert_id FROM ops_alerts WHERE fingerprint=? AND status<>'RESOLVED'", (fp,))
        if alert_id is None:
            return False
        self.resolve(alert_id, actor=actor, note=note, now_ms=now_ms)
        return True

    # ── escalation ────────────────────────────────────────────────────────────────────────────────────
    def escalate_due(self, *, now_ms: Optional[int] = None) -> List[Tuple[str, int]]:
        escalated: List[Tuple[str, int]] = []
        with self.db.transaction(write=False) as conn:
            now = self._now(conn, now_ms)
            due = conn.all("SELECT alert_id FROM ops_alerts WHERE status='OPEN' AND next_escalation_at IS NOT NULL "
                           "AND next_escalation_at <= ? ORDER BY next_escalation_at", (now,))
        for (alert_id,) in due:
            with self.db.transaction() as conn:
                row = conn.one("SELECT status, severity, escalation_level, first_seen_at, rotation_key, occurrences, "
                               "next_escalation_at FROM ops_alerts WHERE alert_id=?" + conn.lock_rows_suffix(), (alert_id,))
                if row is None or row[0] != OPEN or row[6] is None or int(row[6]) > now:
                    continue          # acknowledged or escalated by someone else meanwhile
                severity, level, first_seen, rotation = row[1], int(row[2]) + 1, int(row[3]), row[4]
                res = self._route(rotation, level)
                conn.execute("UPDATE ops_alerts SET escalation_level=?, next_escalation_at=?, owner_required=? "
                             "WHERE alert_id=?", (level, self._next_escalation(severity, level, first_seen),
                                                  1 if res.owner_required else 0, alert_id))
                self._event(conn, alert_id, "ESCALATED", "system", now, level=level,
                            detail={"targets": [t.person_name for t in res.targets], "gap": res.gap,
                                    "unacknowledged_for_ms": now - first_seen})
                self._queue(conn, alert_id, "ESCALATED", level, level, res, now)
            escalated.append((alert_id, level))
        return escalated

    # ── delivery ──────────────────────────────────────────────────────────────────────────────────────
    def deliver_pending(self, *, now_ms: Optional[int] = None, limit: int = 50) -> Dict[str, int]:
        by_name = {s.name: s for s in self.sinks}
        stats = {"sent": 0, "retry": 0, "abandoned": 0, "skipped": 0}
        with self.db.transaction(write=False) as conn:
            now = self._now(conn, now_ms)
            due = conn.all("SELECT delivery_id FROM ops_alert_deliveries WHERE status IN ('PENDING','RETRY') "
                           "AND next_attempt_at <= ? ORDER BY next_attempt_at LIMIT ?", (now, int(limit)))
        abandoned_alerts: List[Tuple[str, str, str]] = []
        for (delivery_id,) in due:
            with self.db.transaction() as conn:   # claim: push next_attempt_at forward so nobody else sends it
                cur = conn.execute("UPDATE ops_alert_deliveries SET next_attempt_at=?, updated_at=? WHERE delivery_id=? "
                                   "AND status IN ('PENDING','RETRY') AND next_attempt_at <= ?",
                                   (now + DELIVERY_CLAIM_MS, now, delivery_id, now))
                if cur.rowcount != 1:
                    stats["skipped"] += 1
                    continue
                d = conn.one("SELECT d.alert_id, d.reason, d.escalation_level, d.sink, d.targets, d.attempts, "
                             "d.max_attempts, a.category, a.severity, a.title, a.detail, a.rotation_key, "
                             "a.owner_required, a.occurrences FROM ops_alert_deliveries d JOIN ops_alerts a "
                             "ON a.alert_id = d.alert_id WHERE d.delivery_id=?", (delivery_id,))
            sink = by_name.get(d[3])
            message = AlertMessage(alert_id=d[0], category=d[7], severity=d[8], title=d[9],
                                   detail=json.loads(d[10]) if d[10] else {}, reason=d[1], escalation_level=int(d[2]),
                                   rotation_key=d[11], owner_required=bool(d[12]), targets=json.loads(d[4]),
                                   delivery_id=delivery_id, occurrences=int(d[13]))
            error: Optional[str] = None
            try:
                if sink is None:
                    raise RuntimeError(f"SINK_NOT_CONFIGURED:{d[3]}")
                sink.send(message)       # no transaction open while a channel is called
            except Exception as exc:  # noqa: BLE001 -- every failure is retried or abandoned, never lost
                error = redact_text(f"{type(exc).__name__}: {exc}")[:500]
            attempts = int(d[5]) + 1
            with self.db.transaction() as conn:
                if error is None:
                    conn.execute("UPDATE ops_alert_deliveries SET status='SENT', attempts=?, sent_at=?, updated_at=?, "
                                 "last_error=NULL WHERE delivery_id=?", (attempts, now, now, delivery_id))
                    self._event(conn, d[0], "DELIVERY_SUCCEEDED", "system", now, level=int(d[2]),
                                detail={"sink": d[3], "delivery_id": delivery_id, "attempts": attempts})
                    stats["sent"] += 1
                elif attempts >= int(d[6]):
                    conn.execute("UPDATE ops_alert_deliveries SET status='ABANDONED', attempts=?, last_error=?, "
                                 "updated_at=? WHERE delivery_id=?", (attempts, error, now, delivery_id))
                    self._event(conn, d[0], "DELIVERY_ABANDONED", "system", now, level=int(d[2]),
                                detail={"sink": d[3], "delivery_id": delivery_id, "error": error})
                    stats["abandoned"] += 1
                    abandoned_alerts.append((d[0], d[3], d[7]))
                else:
                    wait = DELIVERY_BACKOFF_MS[min(attempts - 1, len(DELIVERY_BACKOFF_MS) - 1)]
                    conn.execute("UPDATE ops_alert_deliveries SET status='RETRY', attempts=?, last_error=?, "
                                 "next_attempt_at=?, updated_at=? WHERE delivery_id=?",
                                 (attempts, error, now + wait, now, delivery_id))
                    self._event(conn, d[0], "DELIVERY_FAILED", "system", now, level=int(d[2]),
                                detail={"sink": d[3], "delivery_id": delivery_id, "error": error,
                                        "retry_in_ms": wait})
                    stats["retry"] += 1
        for alert_id, sink_name, category in abandoned_alerts:
            if category != "ALERT_DELIVERY_FAILURE":
                self.raise_alert("ALERT_DELIVERY_FAILURE", key={"sink": sink_name},
                                 title=f"Alert delivery through '{sink_name}' failed after every retry",
                                 source="alerting.delivery", detail={"sink": sink_name, "alert_id": alert_id},
                                 now_ms=now_ms)
        return stats

    # ── reads ─────────────────────────────────────────────────────────────────────────────────────────
    def get(self, alert_id: str) -> Dict[str, Any]:
        with self.db.transaction(write=False) as conn:
            row = conn.one(f"SELECT {', '.join(_ALERT_COLS)} FROM ops_alerts WHERE alert_id=?", (alert_id,))
            if row is None:
                raise AlertError("ALERT_NOT_FOUND")
            events = conn.all("SELECT id, event, actor, escalation_level, detail, at FROM ops_alert_events "
                              "WHERE alert_id=? ORDER BY id", (alert_id,))
            deliveries = conn.all("SELECT delivery_id, reason, escalation_level, sink, status, attempts, max_attempts, "
                                  "last_error, sent_at, created_at FROM ops_alert_deliveries WHERE alert_id=? "
                                  "ORDER BY created_at, delivery_id", (alert_id,))
        out = _alert_row(row)
        out["events"] = [{"id": e[0], "event": e[1], "actor": e[2], "escalation_level": e[3],
                          "detail": json.loads(e[4]) if e[4] else {}, "at": e[5]} for e in events]
        out["deliveries"] = [dict(zip(("delivery_id", "reason", "escalation_level", "sink", "status", "attempts",
                                       "max_attempts", "last_error", "sent_at", "created_at"), d)) for d in deliveries]
        return out

    def list_alerts(self, *, status: Optional[Iterable[str]] = None, limit: int = 200) -> List[Dict[str, Any]]:
        statuses = tuple(status or ())
        where = (" WHERE status IN (" + ",".join("?" * len(statuses)) + ")") if statuses else ""
        with self.db.transaction(write=False) as conn:
            rows = conn.all(f"SELECT {', '.join(_ALERT_COLS)} FROM ops_alerts{where} "
                            "ORDER BY CASE status WHEN 'OPEN' THEN 0 WHEN 'ACKNOWLEDGED' THEN 1 ELSE 2 END, "
                            "CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END, "
                            "last_seen_at DESC LIMIT ?", (*statuses, int(limit)))
        return [_alert_row(r) for r in rows]

    def open_alerts(self) -> List[Dict[str, Any]]:
        return self.list_alerts(status=(OPEN, ACKNOWLEDGED), limit=1000)

    def health(self, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Health of the alerting system itself (not of the platform): can an alert reach a named person?"""
        findings = list(self.oncall.findings())
        if not self.sinks:
            findings.append({"code": "NO_DELIVERY_CHANNEL", "severity": CRITICAL,
                             "message": "No alert delivery channel is configured; alerts are recorded but not sent."})
        with self.db.transaction(write=False) as conn:
            now = self._now(conn, now_ms)
            abandoned = int(conn.scalar("SELECT COUNT(*) FROM ops_alert_deliveries WHERE status='ABANDONED'") or 0)
            overdue = int(conn.scalar("SELECT COUNT(*) FROM ops_alert_deliveries WHERE status IN ('PENDING','RETRY') "
                                      "AND next_attempt_at < ?", (now - 15 * 60_000,)) or 0)
            counts = conn.all("SELECT status, severity, COUNT(*) FROM ops_alerts WHERE status<>'RESOLVED' "
                              "GROUP BY status, severity")
        if abandoned:
            findings.append({"code": "DELIVERIES_ABANDONED", "severity": HIGH,
                             "message": f"{abandoned} alert deliveries were abandoned after every retry."})
        if overdue:
            findings.append({"code": "DELIVERIES_OVERDUE", "severity": HIGH,
                             "message": f"{overdue} alert deliveries are more than 15 minutes overdue."})
        worst = CRITICAL if any(f["severity"] == CRITICAL for f in findings) else (HIGH if findings else None)
        return {"status": "OK" if worst is None else ("CRITICAL" if worst == CRITICAL else "DEGRADED"),
                "owner_required": any(f["code"] == OPERATIONS_OWNER_REQUIRED for f in findings),
                "findings": findings, "sinks": [s.name for s in self.sinks],
                "open_alerts": [{"status": s, "severity": sev, "count": int(n)} for s, sev, n in counts]}
