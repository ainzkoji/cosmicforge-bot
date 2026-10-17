"""Production operations API (Section H Step 8.5).

Public (no authentication)::

    GET /api/public/status      component states + admin-authored incident notices (allow-listed payload)

Admin (``require_admin``), under ``/api/admin/operations``::

    GET  /overview                                alerting health, on-call, open alerts, drill, open incidents
    GET  /alerts?status=OPEN,ACKNOWLEDGED         GET /alerts/{id}
    POST /alerts/{id}/acknowledge  {note?}        POST /alerts/{id}/resolve {note}
    GET  /oncall                                  GET /oncall/history
    POST /oncall/members  {rotation_key, level, person_name, contact_channel, contact_address}
    POST /oncall/members/{member_id}/remove
    GET  /backup-drill                            newest restore-drill evidence (file written by restore_drill.py)
    GET  /status/incidents                        POST /status/incidents
    POST /status/incidents/{id}/updates {status, message, impact?, components?}
    POST /status/incidents/{id}/resolve {message}

Alerts, on-call and incidents live in the platform database (``PLATFORM_DATABASE_URL``, falling back to
``DATABASE_URL``; operations migrations 30-32 applied by the deploy). Without it the admin endpoints answer 503
``PLATFORM_DATABASE_UNAVAILABLE`` / ``OPERATIONS_SCHEMA_MISSING`` and the public page reports degraded data
availability -- never a fabricated "all operational".

No person is ever invented: an empty on-call configuration is reported as ``OPERATIONS_OWNER_REQUIRED``.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field

from app.core.deps import require_admin
from shared_lib.operations import platform as ops_platform
from shared_lib.operations.alerting import AlertEngine, AlertError
from shared_lib.operations.oncall import CONTACT_CHANNELS, OPERATIONS_OWNER_REQUIRED, ROTATIONS, OncallDirectory, OncallError
from shared_lib.operations.status_page import (COMPONENTS, INCIDENT_IMPACTS, INCIDENT_STATUSES, StatusIncidents,
                                               StatusPageError, public_status)

public_router = APIRouter(tags=["Public Status"])
admin_router = APIRouter(prefix="/admin/operations", tags=["Admin Operations"])

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_DRILL_EVIDENCE = REPO_ROOT / "evidence" / "step8" / "restore_drill.json"
PUBLIC_CACHE_SECONDS = 10.0
_cache_lock = threading.Lock()
_cache: Dict[str, Any] = {"at": 0.0, "payload": None}


def _platform_db():
    """The platform database, or a 503 explaining why it is not usable."""
    try:
        db = ops_platform.platform_db()
    except ops_platform.OperationsUnavailable as exc:
        raise HTTPException(503, detail={"error_code": exc.code, "message": "platform database not configured"})
    if not ops_platform.ops_schema_ready(db):
        raise HTTPException(503, detail={"error_code": "OPERATIONS_SCHEMA_MISSING",
                                         "message": "operations migrations (30-32) are not applied; run "
                                                    "`python -m shared_lib.persistence.sql migrate`"})
    return db


def _actor(admin: dict) -> str:
    return f"admin:{admin.get('id')}"


def _err(exc: Exception) -> HTTPException:
    code = getattr(exc, "code", "OPERATION_REFUSED")
    status = 404 if code.endswith("NOT_FOUND") else 409 if code in (
        "ALERT_NOT_OPEN", "ALERT_ALREADY_RESOLVED", "INCIDENT_ALREADY_RESOLVED") else 400
    return HTTPException(status, detail={"error_code": code, "message": str(exc), **getattr(exc, "details", {})})


# ── public ──────────────────────────────────────────────────────────────────────────────────────────────
def build_public_status() -> Dict[str, Any]:
    db = None
    try:
        candidate = ops_platform.platform_db()
        if ops_platform.ops_schema_ready(candidate):
            db = candidate
    except ops_platform.OperationsUnavailable:
        db = None
    return public_status(db)


@public_router.get("/api/public/status")
def get_public_status(response: Response) -> Dict[str, Any]:
    now = time.monotonic()
    with _cache_lock:
        if _cache["payload"] is not None and now - _cache["at"] < PUBLIC_CACHE_SECONDS:
            payload = _cache["payload"]
        else:
            payload = build_public_status()
            _cache.update(at=now, payload=payload)
    response.headers["Cache-Control"] = f"public, max-age={int(PUBLIC_CACHE_SECONDS)}"
    return payload


def clear_public_cache() -> None:
    with _cache_lock:
        _cache.update(at=0.0, payload=None)


# ── admin: models ───────────────────────────────────────────────────────────────────────────────────────
class AckRequest(BaseModel):
    note: Optional[str] = Field(None, max_length=1000)


class ResolveRequest(BaseModel):
    note: str = Field(min_length=1, max_length=2000)


class MemberRequest(BaseModel):
    rotation_key: str = Field(min_length=1, max_length=64)
    level: int = Field(ge=0, le=2)
    person_name: str = Field(min_length=1, max_length=200)
    contact_channel: str = Field(min_length=1, max_length=16)
    contact_address: str = Field(min_length=1, max_length=300)


class IncidentCreate(BaseModel):
    title: str = Field(min_length=3, max_length=200)
    components: List[str] = Field(min_length=1)
    impact: str
    message: str = Field(min_length=3, max_length=4000)
    status: str = "INVESTIGATING"


class IncidentUpdate(BaseModel):
    status: str
    message: str = Field(min_length=3, max_length=4000)
    impact: Optional[str] = None
    components: Optional[List[str]] = None


class IncidentResolve(BaseModel):
    message: str = Field(min_length=3, max_length=4000)


# ── admin: alerts ───────────────────────────────────────────────────────────────────────────────────────
def _engine(db) -> AlertEngine:
    return AlertEngine(db, sinks=[])


def alerting_health(db) -> Dict[str, Any]:
    """Health of the alerting system as the admin API can see it. Delivery sinks run in the operations worker
    (bot-backend), so instead of this process's sinks, the last 24h of recorded deliveries are reported."""
    h = _engine(db).health()
    h["findings"] = [f for f in h["findings"] if f["code"] != "NO_DELIVERY_CHANNEL"]
    with db.transaction(write=False) as conn:
        since = conn.now_ms() - 24 * 3600 * 1000
        rows = conn.all("SELECT sink, status, COUNT(*) FROM ops_alert_deliveries WHERE created_at >= ? "
                        "GROUP BY sink, status", (since,))
    h["recent_deliveries"] = [{"sink": s, "status": st, "count": int(n)} for s, st, n in rows]
    h["sinks"] = sorted({r["sink"] for r in h["recent_deliveries"]})
    critical = any(f["severity"] == "CRITICAL" for f in h["findings"])
    h["status"] = "CRITICAL" if critical else ("DEGRADED" if h["findings"] else "OK")
    h["owner_required"] = any(f["code"] == OPERATIONS_OWNER_REQUIRED for f in h["findings"])
    return h


@admin_router.get("/alerts")
def list_alerts(status: Optional[str] = Query(None, max_length=64), limit: int = Query(200, ge=1, le=1000),
                admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    statuses = [s for s in (status or "").split(",") if s]
    if any(s not in ("OPEN", "ACKNOWLEDGED", "RESOLVED") for s in statuses):
        raise HTTPException(400, detail={"error_code": "INVALID_STATUS"})
    return {"alerts": _engine(_platform_db()).list_alerts(status=statuses, limit=limit)}


@admin_router.get("/alerts/{alert_id}")
def get_alert(alert_id: str, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    try:
        return _engine(_platform_db()).get(alert_id)
    except AlertError as exc:
        raise _err(exc)


@admin_router.post("/alerts/{alert_id}/acknowledge")
def acknowledge(alert_id: str, body: AckRequest, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    try:
        return _engine(_platform_db()).acknowledge(alert_id, actor=_actor(admin), note=body.note)
    except AlertError as exc:
        raise _err(exc)


@admin_router.post("/alerts/{alert_id}/resolve")
def resolve(alert_id: str, body: ResolveRequest, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    try:
        return _engine(_platform_db()).resolve(alert_id, actor=_actor(admin), note=body.note)
    except AlertError as exc:
        raise _err(exc)


# ── admin: on-call ──────────────────────────────────────────────────────────────────────────────────────
@admin_router.get("/oncall")
def oncall(admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    view = OncallDirectory(_platform_db()).overview()
    view["contact_channels"] = list(CONTACT_CHANNELS)
    view["levels"] = [{"level": 0, "name": "PRIMARY"}, {"level": 1, "name": "SECONDARY"},
                      {"level": 2, "name": "ESCALATION_OWNER"}]
    return view


@admin_router.get("/oncall/history")
def oncall_history(admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    return {"history": OncallDirectory(_platform_db()).history()}


@admin_router.post("/oncall/members", status_code=201)
def add_member(body: MemberRequest, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    d = OncallDirectory(_platform_db())
    try:
        member_id = d.assign(rotation_key=body.rotation_key, level=body.level, person_name=body.person_name,
                             contact_channel=body.contact_channel, contact_address=body.contact_address,
                             actor=_actor(admin))
    except OncallError as exc:
        raise _err(exc)
    return {"member_id": member_id, **d.overview()}


@admin_router.post("/oncall/members/{member_id}/remove")
def remove_member(member_id: str, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    d = OncallDirectory(_platform_db())
    try:
        d.remove(member_id, actor=_actor(admin))
    except OncallError as exc:
        raise _err(exc)
    return d.overview()


# ── admin: backups / restore drill ──────────────────────────────────────────────────────────────────────
def drill_summary(path: Optional[Path] = None) -> Dict[str, Any]:
    path = Path(path or os.environ.get("OPS_RESTORE_DRILL_EVIDENCE") or DEFAULT_DRILL_EVIDENCE)
    targets = {"rpo_target_minutes": os.environ.get("OPS_RPO_TARGET_MINUTES") or "UNSET",
               "rto_target_minutes": os.environ.get("OPS_RTO_TARGET_MINUTES") or "UNSET",
               "targets_note": "RPO/RTO targets are UNSET until the operations owner decides them."}
    try:
        evidence = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"status": "NO_DRILL_RECORDED", **targets}
    except (OSError, ValueError) as exc:
        return {"status": "DRILL_EVIDENCE_UNREADABLE", "error": type(exc).__name__, **targets}
    finished = evidence.get("finished_at")
    age_h = None
    if finished:
        try:
            age_h = round((datetime.now(timezone.utc) - datetime.fromisoformat(finished.replace("Z", "+00:00")))
                          .total_seconds() / 3600, 1)
        except ValueError:
            age_h = None
    checks = evidence.get("checks", {})
    return {"status": "PASSED" if evidence.get("ok") else "FAILED", "finished_at": finished, "age_hours": age_h,
            "source_kind": evidence.get("source", {}).get("kind"), "encryption": evidence.get("encryption", {}).get("method"),
            "rpo_ms": evidence.get("rpo", {}).get("rpo_ms"), "rto_ms": evidence.get("rto", {}).get("rto_ms"),
            "checks_passed": sum(1 for v in checks.values() if v is True),
            "checks_failed": sorted(k for k, v in checks.items() if v is False),
            "postgres_leg": (evidence.get("postgres") or {}).get("status", "NOT_RUN"), **targets}


@admin_router.get("/backup-drill")
def backup_drill(admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    return drill_summary()


# ── admin: status-page incidents ────────────────────────────────────────────────────────────────────────
@admin_router.get("/status/incidents")
def list_incidents(admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    return {"incidents": StatusIncidents(_platform_db()).list(), "components": COMPONENTS,
            "statuses": list(INCIDENT_STATUSES), "impacts": list(INCIDENT_IMPACTS)}


@admin_router.post("/status/incidents", status_code=201)
def create_incident(body: IncidentCreate, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    try:
        out = StatusIncidents(_platform_db()).create(title=body.title, components=body.components, impact=body.impact,
                                                     message=body.message, status=body.status, actor=_actor(admin))
    except StatusPageError as exc:
        raise _err(exc)
    clear_public_cache()
    return out


@admin_router.post("/status/incidents/{incident_id}/updates")
def update_incident(incident_id: str, body: IncidentUpdate, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    try:
        out = StatusIncidents(_platform_db()).update(incident_id, status=body.status, message=body.message,
                                                     impact=body.impact, components=body.components,
                                                     actor=_actor(admin))
    except StatusPageError as exc:
        raise _err(exc)
    clear_public_cache()
    return out


@admin_router.post("/status/incidents/{incident_id}/resolve")
def resolve_incident(incident_id: str, body: IncidentResolve, admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    try:
        out = StatusIncidents(_platform_db()).resolve(incident_id, message=body.message, actor=_actor(admin))
    except StatusPageError as exc:
        raise _err(exc)
    clear_public_cache()
    return out


# ── admin: overview ─────────────────────────────────────────────────────────────────────────────────────
@admin_router.get("/overview")
def overview(admin: dict = Depends(require_admin)) -> Dict[str, Any]:
    db = _platform_db()
    engine = _engine(db)
    return {"alerting": alerting_health(db), "oncall": OncallDirectory(db).overview(),
            "open_alerts": engine.open_alerts()[:50], "backup_drill": drill_summary(),
            "status_incidents": StatusIncidents(db).list(include_resolved=False),
            "rotations": ROTATIONS}
