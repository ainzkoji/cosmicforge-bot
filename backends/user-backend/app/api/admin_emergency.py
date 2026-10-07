"""Operator (admin) emergency controls -- proxied to the bot-backend admin family
``/api/v1/admin/emergency`` (kill switch, flatten).

    GET  /api/admin/emergency/status        -> GET  /api/v1/admin/emergency/status
    POST /api/admin/emergency/kill-switch   -> POST /api/v1/admin/emergency/kill-switch
    POST /api/admin/emergency/flatten       -> POST /api/v1/admin/emergency/flatten

Two independent authorizations: this service's ``require_admin`` (admin-portal
token, ``admins`` table) AND the bot-backend's own ``require_admin``.

The admin-portal token itself cannot be forwarded: it is issued for a different
issuer/audience (``cosmicforge-admin-backend`` / ``admin-portal``) and the
bot-backend only accepts service tokens (``cosmicforge-user-backend`` /
``cosmicforge-services``, ``type=access``). So once the caller has been
verified as an active admin here, a short-lived service token carrying
``role=admin`` and the admin's id as ``sub`` is minted for the single upstream
call. The bot-backend audit log therefore still names the real operator.

The upstream answer is returned unchanged -- status code AND body -- because
the operator must see exactly what the engine said (400 refused, 409 engine
unavailable, 502 a close failed). This proxy never turns a failure into a
success, and never invents one: when the upstream cannot be reached or does not
answer in time the outcome is reported as unknown.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, Body, Depends
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
        "act": "admin-portal",
    }
    return "Bearer " + jwt.encode(claims, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def _gateway_error(status: int, reason: str, message: str) -> JSONResponse:
    """Same error shape as the bot-backend emergency API (``detail`` + ``reason``)."""
    return JSONResponse(status_code=status, content={"detail": message, "reason": reason})


async def _forward(method: str, path: str, admin: Dict[str, Any], *, timeout: float,
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
    except httpx.TimeoutException:
        logger.error("[EMERGENCY-PROXY] %s %s timed out after %.0fs", method, path, timeout)
        return _gateway_error(
            504, "UPSTREAM_TIMEOUT",
            f"The bot service did not answer within {timeout:.0f}s. The outcome is UNKNOWN: "
            "the request may still be running. Reload the status and verify on the exchange.")
    except httpx.RequestError as exc:
        logger.error("[EMERGENCY-PROXY] %s %s failed: %s", method, path, type(exc).__name__)
        return _gateway_error(
            502, "UPSTREAM_UNREACHABLE",
            f"The bot service could not be reached ({type(exc).__name__}); the request was NOT confirmed.")

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
