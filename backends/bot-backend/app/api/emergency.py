"""Operator emergency controls for the production engine (admin only).

Authorisation: ``app.core.auth.require_admin_emergency`` -- the one-call
service token minted by the user-backend admin emergency proxy
(``act=admin-emergency``, lifetime <= 120 s). A plain ``role=admin`` access
token is refused with 403.

    GET  /api/v1/admin/emergency/status
    POST /api/v1/admin/emergency/kill-switch   {"enabled": bool, "reason": str}
    POST /api/v1/admin/emergency/flatten       {"scope": "all"|"account", "account_id": str|null,
                                                "confirm": "FLATTEN", "reason": str}

Why this exists: the old ``/emergency/flatten`` walked the legacy runners,
which do not exist in production, and answered ``ok`` for zero bots; the kill
switch had no API at all.

What these controls are and are not:

* The kill switch is the persisted governance control the execution boundary
  already consults before every ENTRY (``GovernanceAuthority.authorize_entry``,
  twice: before hard risk and again immediately before the durable submit
  claim). On means no new entries. Protection, reconciliation and closes are
  unaffected, by design.
* Flatten sets the kill switch FIRST, then closes through the same durable
  reduce-only close the runtime's fail-safe uses, under the runtime's own
  broker-cycle lock. It bypasses no gate: the order-submission flags, the
  endpoint/environment check and the runtime lease all still apply. A
  reduce-only close can never open or increase a position.
* It never reports success for work it did not do: 409 when this process
  cannot act at all, 502 when any in-scope close failed. ``ok`` is true only
  when every in-scope position is confirmed ``closed`` / ``no_position``, or
  THIS request sent a close order that the venue acknowledged (``submitted``,
  whose detail carries that evidence). A close that an earlier request or
  cycle sent and that is still unconfirmed is ``failed``: this request did
  nothing about it.
* Rate limits. The close order itself (a POST) is never paced, delayed or
  retried by the Binance client. The reads before it (open positions, the
  previous close's read-back, the position size) are signed GETs: while the
  venue has told this IP to back off (HTTP 429 / 418 Retry-After) they are not
  sent and fail at once, so the affected account is reported ``failed`` with
  ``BinanceRateLimited`` -- quickly, and without lengthening the ban. Outside
  such a venue-mandated backoff they can be slowed by a few seconds in total
  at most (see ``app.exchange.binance.client``), never refused.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Not the generic ``require_admin``: any access token with role=admin passes
# that, including an end-user account whose users.role is 'admin'. These
# routes accept only the dedicated short-lived service token the user-backend
# admin emergency proxy mints for a verified ``admins``-table operator.
from app.core.auth import require_admin_emergency
from shared_lib.persistence.db import DB

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/admin/emergency", tags=["Emergency Controls"])

CONFIRMATION = "FLATTEN"
#: ``detail`` prefix the engine puts on a ``submitted`` row: the evidence that
#: THIS request sent a close order which the venue acknowledged (the same
#: value as ``production_execution.SUBMITTED_DETAIL_PREFIX``).
SUBMITTED_EVIDENCE = "CLOSE_ORDER_ACKNOWLEDGED"
#: A local broker snapshot older than this says nothing about "now".
SNAPSHOT_MAX_AGE_MS = 300_000


def get_db() -> DB:
    return DB()


class KillSwitchRequest(BaseModel):
    enabled: bool
    reason: str = ""


class FlattenRequest(BaseModel):
    scope: str = ""
    account_id: Optional[str] = None
    confirm: str = ""
    reason: str = ""


def _error(status: int, reason: str, message: str, **extra: Any) -> JSONResponse:
    """One error shape for every refusal: ``detail`` is always a readable
    message (the framework's convention), ``reason`` a stable code."""
    return JSONResponse(status_code=status, content={"detail": message, "reason": reason, **extra})


def _iso(ms: Optional[int]) -> Optional[str]:
    return None if ms is None else datetime.fromtimestamp(int(ms) / 1000, timezone.utc).isoformat()


def _audit(db: Any, action: str, admin_id: str, details: Dict[str, Any]) -> None:
    """Every call is audit-logged: the canonical event log, plus the process
    log in case the database itself is the problem. Never raises."""
    record = {"actor": f"admin:{admin_id}", **details}
    logger.warning("[EMERGENCY] %s %s", action, json.dumps(record, default=str, sort_keys=True))
    try:
        from shared_lib.persistence.audit import Audit

        Audit(db).event(event_type="EMERGENCY", action=action, details=record)
    except Exception:
        logger.exception("[EMERGENCY] audit event could not be persisted: %s", action)


def _open_positions(db: Any, now_ms: int) -> Optional[List[Dict[str, Any]]]:
    """Open positions from the runtime's last broker snapshot of each execution
    account -- LOCAL durable state only, no broker call. ``None`` whenever that
    is not a complete, recent picture (an empty list must mean "flat")."""
    try:
        from app.trading_intelligence.integration.production_runtime import execution_accounts

        accounts = execution_accounts(db)
        positions: List[Dict[str, Any]] = []
        with db.connect() as conn:
            if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='cati_production_state'").fetchone():
                return None
            for account in accounts:
                row = conn.execute("SELECT observed_at, document FROM cati_production_state WHERE account_id=?",
                                   (account["id"],)).fetchone()
                if row is None or now_ms - int(row[0]) > SNAPSHOT_MAX_AGE_MS:
                    return None
                held = json.loads(row[1]).get("positions")
                if not isinstance(held, list):
                    return None
                for p in held:
                    amount = float(p.get("positionAmt") or 0)
                    if amount:
                        positions.append({"account_id": account["id"], "user_id": account.get("user_id"),
                                          "symbol": str(p.get("symbol")), "side": "LONG" if amount > 0 else "SHORT",
                                          "qty": abs(amount)})
        return positions
    except Exception:
        return None


def _status(db: Any) -> Dict[str, Any]:
    from app.trading_intelligence.governance.promotion import kill_switch_status
    from shared_lib.core.production import order_submission_gate

    now_ms = int(time.time() * 1000)
    try:
        switch = kill_switch_status(db)
    except Exception as exc:
        # Unreadable governance is never shown as "off".
        raise HTTPException(status_code=503, detail=f"Kill switch state is unavailable ({type(exc).__name__}).")
    return {
        "kill_switch": {"enabled": bool(switch["enabled"]), "reason": switch["reason"],
                        "set_at": _iso(switch["set_at_ms"]), "set_by": switch["set_by"]},
        "live_order_submission_enabled": bool(order_submission_gate("live")["enabled"]),
        "demo_order_submission_enabled": bool(order_submission_gate("demo")["enabled"]),
        "open_positions": _open_positions(db, now_ms),
        "generated_at": _iso(now_ms),
    }


def _set_kill_switch(db: Any, enabled: bool, reason: str, admin_id: str) -> None:
    """Persist the GLOBAL new-entry kill switch and prove it reads back.

    Only the governance module's two kill-switch helpers are used here: this
    API cannot move a promotion phase or grant a scope."""
    from app.trading_intelligence.governance.promotion import kill_switch_status, set_new_entry_kill_switch

    try:
        set_new_entry_kill_switch(db, bool(enabled), reason=reason, actor_ref=f"admin:{admin_id}")
    except Exception:
        # e.g. an identical record in the same millisecond: only the durable
        # state decides whether the request was honoured.
        logger.exception("[EMERGENCY] kill switch record could not be written")
    if kill_switch_status(db)["enabled"] != bool(enabled):
        raise RuntimeError("KILL_SWITCH_NOT_PERSISTED")


@router.get("/status")
def emergency_status(admin_id: str = Depends(require_admin_emergency), db: DB = Depends(get_db)):
    return _status(db)


@router.post("/kill-switch")
def set_kill_switch(body: KillSwitchRequest, admin_id: str = Depends(require_admin_emergency), db: DB = Depends(get_db)):
    reason = body.reason.strip()
    if len(reason) < 3:
        return _error(400, "REASON_REQUIRED", "A reason of at least 3 characters is required.")
    _audit(db, "KILL_SWITCH_REQUESTED", admin_id, {"enabled": body.enabled, "reason": reason})
    try:
        _set_kill_switch(db, body.enabled, reason, admin_id)
    except Exception as exc:
        _audit(db, "KILL_SWITCH_FAILED", admin_id, {"enabled": body.enabled, "error": type(exc).__name__})
        return _error(503, "KILL_SWITCH_NOT_PERSISTED", "The kill switch change could not be persisted.")
    _audit(db, "KILL_SWITCH_SET", admin_id, {"enabled": body.enabled, "reason": reason})
    return _status(db)


@router.post("/flatten")
def emergency_flatten(body: FlattenRequest, admin_id: str = Depends(require_admin_emergency), db: DB = Depends(get_db)):
    if body.confirm != CONFIRMATION:
        return _error(400, "CONFIRMATION_REQUIRED", 'confirm must be exactly "FLATTEN".')
    if body.scope not in ("all", "account") or (body.scope == "account" and not body.account_id):
        return _error(400, "SCOPE_INVALID", 'scope must be "all", or "account" with an account_id.')
    reason = body.reason.strip()
    if len(reason) < 3:
        return _error(400, "REASON_REQUIRED", "A reason of at least 3 characters is required.")
    account_id = body.account_id if body.scope == "account" else None
    request_id = uuid.uuid4().hex
    context = {"request_id": request_id, "scope": body.scope, "account_id": account_id, "reason": reason}
    _audit(db, "FLATTEN_REQUESTED", admin_id, context)

    # 1. Nothing may re-enter while (or after) positions are being closed.
    try:
        _set_kill_switch(db, True, f"EMERGENCY_FLATTEN {request_id}: {reason}", admin_id)
    except Exception as exc:
        _audit(db, "FLATTEN_ABORTED", admin_id, {**context, "error": type(exc).__name__})
        return _error(503, "KILL_SWITCH_NOT_PERSISTED", "The kill switch could not be set, so NO position was closed.",
                      ok=False, kill_switch_enabled=False, results=[])

    # 2. The same durable reduce-only close as the fail-safe, under the cycle lock.
    from app.trading_intelligence.integration import production_runtime as runtime

    try:
        results = runtime.flatten(db, account_id=account_id, request_id=request_id)
    except runtime.EngineUnavailable as exc:
        _audit(db, "FLATTEN_ENGINE_UNAVAILABLE", admin_id, {**context, "engine": str(exc)})
        return _error(409, str(exc), "The production engine in this process cannot act on a broker "
                      f"({exc}); NO position was closed. The kill switch is on (no new entries).",
                      ok=False, kill_switch_enabled=True, results=[])
    except Exception as exc:
        _audit(db, "FLATTEN_FAILED", admin_id, {**context, "error": type(exc).__name__})
        return JSONResponse(status_code=502, content={
            "ok": False, "kill_switch_enabled": True,
            "results": [{"account_id": account_id or "*", "symbol": "*", "status": "failed",
                         "detail": type(exc).__name__}],
            "detail": f"The flatten failed inside the engine ({type(exc).__name__}); positions may still be open. "
                      "The kill switch is on (no new entries)."})

    # Every field is a string ("*" = the whole account), whatever the engine returned.
    results = [{"account_id": str(r.get("account_id") or "*"), "symbol": str(r.get("symbol") or "*"),
                "status": str(r.get("status") or "failed"), "detail": str(r.get("detail") or "")} for r in results]
    for row in results:
        # "submitted" must mean: this request sent a close order and the venue
        # acknowledged it. Without that evidence the row is a failure.
        if row["status"] == "submitted" and not row["detail"].startswith(SUBMITTED_EVIDENCE):
            row["status"], row["detail"] = "failed", f"SUBMITTED_WITHOUT_VENUE_ACKNOWLEDGEMENT:{row['detail']}"
    # Success needs evidence: at least one account answered and none failed.
    ok = bool(results) and all(r["status"] in ("closed", "submitted", "no_position") for r in results)
    payload = {"ok": ok, "kill_switch_enabled": True, "results": results}
    _audit(db, "FLATTEN_COMPLETED" if ok else "FLATTEN_INCOMPLETE", admin_id, {**context, "results": results})
    if ok:
        return payload
    failed = sum(r["status"] not in ("closed", "submitted", "no_position") for r in results)
    message = (f"{failed} of {len(results)} in-scope close(s) failed or could not be confirmed; positions may still "
               "be open. The kill switch is on (no new entries)." if results else
               "The engine reported nothing for this scope; NO position was closed. The kill switch is on.")
    return JSONResponse(status_code=502, content={**payload, "detail": message})


__all__ = ["router", "get_db", "CONFIRMATION"]
