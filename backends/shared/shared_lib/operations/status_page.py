"""Public status page (Section H Step 8.5).

``public_status(db)`` builds the ONLY payload served at ``GET /api/public/status``. It is assembled from an
allow-list of fields -- component name and state, and admin-authored incident notices -- so nothing internal can
leak through it by accident: no alert id, fingerprint, category, detail, actor, account, tenant, host, IP,
worker id, queue name or exchange state of any customer.

Component state is the worst of:

* the impact of every unresolved admin-authored incident that names the component (``DEGRADED`` / ``OUTAGE`` /
  ``MAINTENANCE``) -- only a human declares an outage or maintenance;
* ``DEGRADED`` when an unresolved CRITICAL/HIGH **platform-scoped** alert maps to the component. Account-scoped
  alerts (one customer's exchange problem) count only in aggregate: ``EXCHANGE_CONNECTIVITY`` turns DEGRADED when
  at least ``ACCOUNT_AGGREGATE_THRESHOLD`` distinct accounts have one open, and never reveals how many or whose.

Incident text is written by administrators and is checked before it is stored (``check_public_text``): no
credential-like strings, IP addresses, internal host names, e-mail addresses or URLs with credentials.
"""
from __future__ import annotations

import json
import re
import uuid
from typing import Any, Dict, List, Optional, Sequence

from shared_lib.persistence.sql import Database

from .alerting import CATEGORIES
from .secrets_scan import find_secrets

OPERATIONAL, DEGRADED, OUTAGE, MAINTENANCE = "OPERATIONAL", "DEGRADED", "OUTAGE", "MAINTENANCE"
COMPONENT_STATES = (OPERATIONAL, DEGRADED, OUTAGE, MAINTENANCE)
_RANK = {OPERATIONAL: 0, MAINTENANCE: 1, DEGRADED: 2, OUTAGE: 3}
COMPONENTS: Dict[str, str] = {
    "PORTAL": "Portal",
    "TRADING_ENGINE": "Trading engine",
    "MARKET_DATA": "Market data",
    "EXCHANGE_CONNECTIVITY": "Exchange connectivity",
    "NOTIFICATIONS": "Notifications",
}
INCIDENT_STATUSES = ("INVESTIGATING", "IDENTIFIED", "MONITORING", "RESOLVED")
INCIDENT_IMPACTS = (DEGRADED, OUTAGE, MAINTENANCE)
ACCOUNT_AGGREGATE_THRESHOLD = 3
RECENT_RESOLVED_MS = 7 * 24 * 3600 * 1000
PUBLIC_UNAVAILABLE_NOTE = "Live status information is temporarily unavailable."

_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_IPV6 = re.compile(r"\b(?:[0-9a-f]{1,4}:){3,7}[0-9a-f]{1,4}\b", re.I)
_INTERNAL_HOST = re.compile(r"(?i)\b(?:localhost|[a-z0-9\-]+\.(?:internal|local|lan|intra|corp|localdomain))\b")
_EMAIL = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
_ACCOUNTISH = re.compile(r"(?i)\b(?:account|tenant|user|worker)[\s_\-]?(?:id)?\s*[:=#]\s*\S+")


class StatusPageError(ValueError):
    def __init__(self, code: str, message: str = "", **details: Any) -> None:
        super().__init__(message or code)
        self.code = code
        self.details = details


def check_public_text(text: str) -> List[str]:
    """Reasons a text must not be published (empty list: acceptable)."""
    problems = [f"SECRET:{k}" for k in find_secrets(text)]
    for code, pattern in (("IP_ADDRESS", _IPV4), ("IP_ADDRESS", _IPV6), ("INTERNAL_HOSTNAME", _INTERNAL_HOST),
                          ("EMAIL_ADDRESS", _EMAIL), ("ACCOUNT_IDENTIFIER", _ACCOUNTISH)):
        if pattern.search(text or "") and code not in problems:
            problems.append(code)
    return problems


def _require_public(*texts: str) -> None:
    problems = sorted({p for t in texts for p in check_public_text(t)})
    if problems:
        raise StatusPageError("PUBLIC_TEXT_REFUSED", "the notice contains internal or sensitive details",
                              problems=problems)


def _components(values: Sequence[str]) -> List[str]:
    comps = sorted(set(values or ()))
    if not comps or any(c not in COMPONENTS for c in comps):
        raise StatusPageError("INVALID_COMPONENTS", f"components must be a non-empty subset of {list(COMPONENTS)}")
    return comps


class StatusIncidents:
    """Admin-authored incident notices; every change is an append-only update row."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def create(self, *, title: str, components: Sequence[str], impact: str, message: str, actor: str,
               status: str = "INVESTIGATING", now_ms: Optional[int] = None) -> Dict[str, Any]:
        title, message = (title or "").strip(), (message or "").strip()
        if not title or not message:
            raise StatusPageError("TITLE_AND_MESSAGE_REQUIRED")
        if impact not in INCIDENT_IMPACTS:
            raise StatusPageError("INVALID_IMPACT")
        if status not in INCIDENT_STATUSES or status == "RESOLVED":
            raise StatusPageError("INVALID_STATUS")
        if not actor:
            raise StatusPageError("ACTOR_REQUIRED")
        comps = _components(components)
        _require_public(title, message)
        incident_id = uuid.uuid4().hex
        with self.db.transaction() as conn:
            now = conn.now_ms() if now_ms is None else now_ms
            conn.execute("INSERT INTO ops_status_incidents (incident_id, title, status, impact, components, created_at, "
                         "created_by, updated_at) VALUES (?,?,?,?,?,?,?,?)",
                         (incident_id, title[:200], status, impact, json.dumps(comps), now, actor, now))
            conn.execute("INSERT INTO ops_status_incident_updates (incident_id, status, impact, message, actor, at) "
                         "VALUES (?,?,?,?,?,?)", (incident_id, status, impact, message[:4000], actor, now))
        return self.get(incident_id)

    def update(self, incident_id: str, *, status: str, message: str, actor: str, impact: Optional[str] = None,
               components: Optional[Sequence[str]] = None, now_ms: Optional[int] = None) -> Dict[str, Any]:
        message = (message or "").strip()
        if not message:
            raise StatusPageError("MESSAGE_REQUIRED")
        if status not in INCIDENT_STATUSES:
            raise StatusPageError("INVALID_STATUS")
        if impact is not None and impact not in INCIDENT_IMPACTS:
            raise StatusPageError("INVALID_IMPACT")
        if not actor:
            raise StatusPageError("ACTOR_REQUIRED")
        _require_public(message)
        with self.db.transaction() as conn:
            now = conn.now_ms() if now_ms is None else now_ms
            row = conn.one("SELECT status, impact, components FROM ops_status_incidents WHERE incident_id=?"
                           + conn.lock_rows_suffix(), (incident_id,))
            if row is None:
                raise StatusPageError("INCIDENT_NOT_FOUND")
            if row[0] == "RESOLVED":
                raise StatusPageError("INCIDENT_ALREADY_RESOLVED")
            new_impact = impact or row[1]
            comps = _components(components) if components is not None else json.loads(row[2])
            conn.execute("UPDATE ops_status_incidents SET status=?, impact=?, components=?, updated_at=?, resolved_at=? "
                         "WHERE incident_id=?", (status, new_impact, json.dumps(comps), now,
                                                 now if status == "RESOLVED" else None, incident_id))
            conn.execute("INSERT INTO ops_status_incident_updates (incident_id, status, impact, message, actor, at) "
                         "VALUES (?,?,?,?,?,?)", (incident_id, status, new_impact, message[:4000], actor, now))
        return self.get(incident_id)

    def resolve(self, incident_id: str, *, message: str, actor: str, now_ms: Optional[int] = None) -> Dict[str, Any]:
        return self.update(incident_id, status="RESOLVED", message=message, actor=actor, now_ms=now_ms)

    def get(self, incident_id: str) -> Dict[str, Any]:
        with self.db.transaction(write=False) as conn:
            row = conn.one("SELECT incident_id, title, status, impact, components, created_at, created_by, updated_at, "
                           "resolved_at FROM ops_status_incidents WHERE incident_id=?", (incident_id,))
            if row is None:
                raise StatusPageError("INCIDENT_NOT_FOUND")
            updates = conn.all("SELECT id, status, impact, message, actor, at FROM ops_status_incident_updates "
                               "WHERE incident_id=? ORDER BY id", (incident_id,))
        out = dict(zip(("incident_id", "title", "status", "impact", "components", "created_at", "created_by",
                        "updated_at", "resolved_at"), row))
        out["components"] = json.loads(out["components"])
        out["updates"] = [dict(zip(("id", "status", "impact", "message", "actor", "at"), u)) for u in updates]
        return out

    def list(self, *, include_resolved: bool = True, limit: int = 100) -> List[Dict[str, Any]]:
        where = "" if include_resolved else " WHERE status<>'RESOLVED'"
        with self.db.transaction(write=False) as conn:
            ids = [r[0] for r in conn.all(f"SELECT incident_id FROM ops_status_incidents{where} "
                                          "ORDER BY updated_at DESC LIMIT ?", (int(limit),))]
        return [self.get(i) for i in ids]


def _iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    from datetime import datetime, timezone
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _worst(a: str, b: str) -> str:
    return a if _RANK[a] >= _RANK[b] else b


def public_status(db: Optional[Database], *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    """The public payload. Every key is chosen here; nothing is copied from a row wholesale."""
    states = {k: OPERATIONAL for k in COMPONENTS}
    incidents_out: List[Dict[str, Any]] = []
    available = db is not None
    if db is not None:
        try:
            with db.transaction(write=False) as conn:
                now = conn.now_ms() if now_ms is None else now_ms
                alerts = conn.all("SELECT category, severity, scope, account_ref FROM ops_alerts "
                                  "WHERE status<>'RESOLVED' AND severity IN ('CRITICAL','HIGH')")
                rows = conn.all("SELECT incident_id, title, status, impact, components, created_at, updated_at, "
                                "resolved_at FROM ops_status_incidents WHERE status<>'RESOLVED' OR resolved_at >= ? "
                                "ORDER BY updated_at DESC LIMIT 20", (now - RECENT_RESOLVED_MS,))
                updates = {r[0]: conn.all("SELECT status, message, at FROM ops_status_incident_updates "
                                          "WHERE incident_id=? ORDER BY id DESC LIMIT 20", (r[0],)) for r in rows}
        except Exception:
            available = False
        else:
            per_component_accounts: Dict[str, set] = {}
            for category, severity, scope, account_ref in alerts:
                policy = CATEGORIES.get(category)
                if policy is None or policy.component is None:
                    continue
                if scope == "PLATFORM":
                    states[policy.component] = _worst(states[policy.component], DEGRADED)
                elif account_ref:
                    per_component_accounts.setdefault(policy.component, set()).add(account_ref)
            for component, accounts in per_component_accounts.items():
                if len(accounts) >= ACCOUNT_AGGREGATE_THRESHOLD:
                    states[component] = _worst(states[component], DEGRADED)
            for incident_id, title, status, impact, comps, created_at, updated_at, resolved_at in rows:
                comps = [c for c in json.loads(comps) if c in COMPONENTS]
                if status != "RESOLVED":
                    for c in comps:
                        states[c] = _worst(states[c], impact)
                incidents_out.append({
                    "id": incident_id, "title": title, "status": status, "impact": impact,
                    "components": [COMPONENTS[c] for c in comps], "started_at": _iso(created_at),
                    "updated_at": _iso(updated_at), "resolved_at": _iso(resolved_at),
                    "updates": [{"status": s, "message": m, "at": _iso(a)} for s, m, a in updates[incident_id]],
                })
    if not available:
        for k in states:
            if k != "PORTAL":
                states[k] = DEGRADED
    overall = OPERATIONAL
    for s in states.values():
        overall = _worst(overall, s)
    from datetime import datetime, timezone
    return {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "overall": overall,
        "data_available": available,
        "notice": None if available else PUBLIC_UNAVAILABLE_NOTE,
        "components": [{"key": k, "name": COMPONENTS[k], "status": states[k]} for k in COMPONENTS],
        "incidents": incidents_out,
    }
