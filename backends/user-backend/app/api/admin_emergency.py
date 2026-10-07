"""Operator (admin) emergency controls -- proxied to the bot-backend admin family
``/api/v1/admin/emergency`` (kill switch, flatten).

    GET  /api/admin/emergency/status        -> GET  /api/v1/admin/emergency/status
    POST /api/admin/emergency/kill-switch   -> POST /api/v1/admin/emergency/kill-switch
    POST /api/admin/emergency/flatten       -> POST /api/v1/admin/emergency/flatten

Two independent authorizations: this service's ``require_admin`` (admin-portal
token, ``admins`` table) AND the bot-backend's ``require_admin_emergency``.

The admin-portal token itself cannot be forwarded: it is issued for a different
issuer/audience (``cosmicforge-admin-backend`` / ``admin-portal``) and the
bot-backend only accepts service tokens (``cosmicforge-user-backend`` /
``cosmicforge-services``, ``type=access``). So once the caller has been
verified as an active admin here, a short-lived (60 s) service token carrying
``role=admin``, ``act=admin-emergency`` and the admin's id as ``sub`` is minted
for the single upstream call. The bot-backend emergency router accepts ONLY a
token with that ``act`` claim and a lifetime of at most 120 s, so an end-user
account whose ``users.role`` is ``admin`` cannot call it directly. The
bot-backend audit log still names the real operator.

Every call writes one audit line to the service log (admin id, action, outcome
status). Every kill-switch / flatten call -- and every status call that did not
succeed -- also writes an ``auth_audit_log`` row; a successful status read does
not, because the admin app polls it every few seconds.

The upstream answer is returned unchanged -- status code AND body -- because
the operator must see exactly what the engine said (400 refused, 409 engine
unavailable, 502 a close failed). This proxy never turns a failure into a
success, and never invents one. Transport failures are told apart:

* the request was never sent (connection refused, connect timeout, no pooled
  connection): 502 ``UPSTREAM_UNREACHABLE`` -- nothing was done;
* the request was sent and then the read timed out or the connection broke:
  504 ``UPSTREAM_TIMEOUT`` / ``UPSTREAM_OUTCOME_UNKNOWN`` -- the outcome is
  UNKNOWN and the operator must check positions.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, Body, Depends
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response
from jose import jwt

from app.api import proxy_utils
from app.core import security
from app.core.config import settings
from app.core.deps import require_admin

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/emergency", tags=["Admin Emergency"])

UPSTREAM_PREFIX = "/api/v1/admin/emergency"

STATUS_TIMEOUT_SECONDS = 15.0
KILL_SWITCH_TIMEOUT_SECONDS = 30.0
#: Flatten closes positions on the exchange account by account; the default
#: 10 s proxy timeout would report a failure while the engine is still closing.
FLATTEN_TIMEOUT_SECONDS = 120.0

#: Only long enough to start one upstream call (the token is checked once, when
#: the bot-backend accepts the request).
SERVICE_TOKEN_TTL_SECONDS = 60


#: ``act`` claim the bot-backend emergency router requires
#: (``app.core.auth.EMERGENCY_ACTOR_CLAIM`` there). Only this module mints it,
#: and only after ``require_admin`` verified the caller against the ``admins``
#: table -- an end-user access token with ``role=admin`` does not carry it.
EMERGENCY_ACTOR_CLAIM = "admin-emergency"

#: Failures that happen BEFORE the request leaves this service: the connection
#: was never established (refused, DNS, TLS, connect timeout) or no pooled
#: connection became available. The engine received nothing.
_NOT_SENT_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout, httpx.UnsupportedProtocol)


def _service_authorization(admin: Dict[str, Any]) -> str:
    """``Authorization`` value the bot-backend accepts for this verified admin."""
    now = datetime.now(timezone.utc)
    claims = {
        "exp": now + timedelta(seconds=SERVICE_TOKEN_TTL_SECONDS),
        "nbf": now,
        "iat": now,
        "iss": security.ISSUER,
        "aud": security.AUDIENCE,
        "sub": str(admin["id"]),
        "jti": str(uuid.uuid4()),
        "type": "access",
        "role": "admin",
        "permissions": [],
        "entitlements": {},
        "act": EMERGENCY_ACTOR_CLAIM,
    }
    return "Bearer " + jwt.encode(claims, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def _gateway_error(status: int, reason: str, message: str) -> JSONResponse:
    """Same error shape as the bot-backend emergency API (``detail`` + ``reason``)."""
    return JSONResponse(status_code=status, content={"detail": message, "reason": reason})


def _write_audit(admin: Dict[str, Any], action: str, details: Dict[str, Any]) -> None:
    """One ``auth_audit_log`` row for an emergency call (blocking: run in the threadpool)."""
    from app.api.auth import audit_event
    from shared_lib.persistence.db import DB

    with DB().connect() as conn:
        audit_event(
            conn,
            f"admin_emergency_{action}",
            user_id=str(admin.get("id")),
            email=admin.get("email"),
            details=details,
        )


def _request_summary(json_body: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """The few request fields worth keeping in the audit trail (never the whole body)."""
    if not isinstance(json_body, dict):
        return {}
    summary = {key: json_body[key] for key in ("enabled", "scope", "account_id") if key in json_body}
    if json_body.get("reason") is not None:
        summary["reason"] = str(json_body["reason"])[:200]
    return summary


def _outcome_reason(response: Response) -> Optional[str]:
    """``reason`` of a JSON error body, when there is one (for the audit line only)."""
    try:
        body = json.loads(bytes(response.body or b"").decode("utf-8"))
    except Exception:
        return None
    reason = body.get("reason") if isinstance(body, dict) else None
    return str(reason)[:100] if reason else None


async def _audit(admin: Dict[str, Any], action: str, response: Response,
                 json_body: Optional[Dict[str, Any]]) -> None:
    """Record who did what and how it ended. Never fails the request it describes."""
    details = {
        "admin_id": str(admin.get("id")),
        "action": action,
        "outcome_status": response.status_code,
        "outcome_reason": _outcome_reason(response),
        "request": _request_summary(json_body),
    }
    # The admin app polls /status every few seconds: a successful read gets the
    # log line only. Every kill-switch / flatten call, and every status call
    # that did not succeed, also gets a durable auth_audit_log row.
    routine_read = action == "status" and response.status_code == 200
    logger.log(
        logging.INFO if routine_read else logging.WARNING,
        "[EMERGENCY-AUDIT] admin=%s action=%s outcome_status=%s outcome_reason=%s request=%s",
        details["admin_id"], action, details["outcome_status"], details["outcome_reason"], details["request"],
    )
    if routine_read:
        return
    try:
        await run_in_threadpool(_write_audit, admin, action, details)
    except Exception as exc:
        # The log line above is the fallback record.
        logger.error("[EMERGENCY-AUDIT] audit row not persisted for admin=%s action=%s (%s)",
                     details["admin_id"], action, type(exc).__name__)


async def _forward(method: str, path: str, admin: Dict[str, Any], *, timeout: float,
                   json_body: Optional[Dict[str, Any]] = None) -> Response:
    """Forward one emergency call and write its audit line, whatever the outcome."""
    action = path.strip("/").replace("-", "_")
    if method != "GET":
        # Intent first: if this request is cancelled while the engine is still
        # working (flatten can take minutes), the log still shows who asked.
        logger.warning("[EMERGENCY-AUDIT] admin=%s action=%s requested request=%s",
                       admin.get("id"), action, _request_summary(json_body))
    response = await _call_upstream(method, path, admin, timeout=timeout, json_body=json_body)
    await _audit(admin, action, response, json_body)
    return response


async def _call_upstream(method: str, path: str, admin: Dict[str, Any], *, timeout: float,
                         json_body: Optional[Dict[str, Any]] = None) -> Response:
    url = f"{proxy_utils.BOT_BACKEND_BASE_URL}{UPSTREAM_PREFIX}{path}"
    try:
        upstream = await proxy_utils._proxy_client.request(
            method=method,
            url=url,
            json=json_body,
            headers={"Authorization": _service_authorization(admin)},
            timeout=timeout,
        )
    except _NOT_SENT_ERRORS as exc:
        # Checked BEFORE the timeout branch: a connect timeout is a timeout,
        # but the request never reached the engine.
        logger.error("[EMERGENCY-PROXY] %s %s not sent: %s", method, path, type(exc).__name__)
        return _gateway_error(
            502, "UPSTREAM_UNREACHABLE",
            f"The engine is unreachable ({type(exc).__name__}): the request was NOT sent and nothing was done. "
            "Nothing was changed by this call.")
    except httpx.TimeoutException as exc:
        logger.error("[EMERGENCY-PROXY] %s %s timed out after %.0fs (%s)", method, path, timeout,
                     type(exc).__name__)
        return _gateway_error(
            504, "UPSTREAM_TIMEOUT",
            f"The bot service did not answer within {timeout:.0f}s. The outcome is UNKNOWN: "
            "the request may still be running. Reload the status and check positions on the exchange.")
    except httpx.RequestError as exc:
        # The connection was established, so the request (or part of it) may
        # have reached the engine before the transport failed.
        logger.error("[EMERGENCY-PROXY] %s %s failed after sending: %s", method, path, type(exc).__name__)
        return _gateway_error(
            504, "UPSTREAM_OUTCOME_UNKNOWN",
            f"The connection to the bot service failed after the request was sent ({type(exc).__name__}). "
            "The outcome is UNKNOWN: the engine may have acted. Reload the status and check positions on "
            "the exchange.")

    if upstream.status_code == 401:
        # The caller was authenticated here; a 401 upstream means the two
        # services disagree about the signing key. Passing a 401 on would make
        # the admin app discard a perfectly valid session.
        logger.error("[EMERGENCY-PROXY] bot-backend rejected the service token for %s %s", method, path)
        return _gateway_error(
            502, "UPSTREAM_AUTH_REJECTED",
            "The bot service rejected this backend's service credential (check that both services share "
            "the same SECRET_KEY). Nothing was changed.")

    # Status code and body exactly as the engine answered.
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        media_type=upstream.headers.get("content-type") or "application/json",
    )


@router.get("/status")
async def emergency_status(admin: dict = Depends(require_admin)):
    return await _forward("GET", "/status", admin, timeout=STATUS_TIMEOUT_SECONDS)


@router.post("/kill-switch")
async def emergency_kill_switch(body: Dict[str, Any] = Body(...), admin: dict = Depends(require_admin)):
    return await _forward("POST", "/kill-switch", admin, json_body=body, timeout=KILL_SWITCH_TIMEOUT_SECONDS)


@router.post("/flatten")
async def emergency_flatten(body: Dict[str, Any] = Body(...), admin: dict = Depends(require_admin)):
    return await _forward("POST", "/flatten", admin, json_body=body, timeout=FLATTEN_TIMEOUT_SECONDS)


__all__ = ["router", "FLATTEN_TIMEOUT_SECONDS"]
