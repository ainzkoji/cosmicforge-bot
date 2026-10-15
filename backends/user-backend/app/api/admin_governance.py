"""Operator (admin) family governance -- proxied to the bot-backend admin family
``/api/v1/admin/governance`` (Step 3.11: the family registry, promotion and
revocation, the family and account kill switches).

    GET  /api/admin/governance/families                                   the registry, as it stands now
    GET  /api/admin/governance/families/{family_id}                       one entry and its whole history
    GET  /api/admin/governance/history                                    the governance log and its integrity
    GET  /api/admin/governance/cycles                                     the platform cycle driver's cycles
    GET  /api/admin/governance/shadow/divergences                         shadow-comparison alerts
    POST /api/admin/governance/families/register
    POST /api/admin/governance/families/{family_id}/evidence
    POST /api/admin/governance/families/{family_id}/promotion-requests
    POST /api/admin/governance/promotion-requests/{request_id}/approve
    POST /api/admin/governance/promotion-requests/{request_id}/reject
    POST /api/admin/governance/families/{family_id}/revoke
    POST /api/admin/governance/families/{family_id}/revoke-certification
    POST /api/admin/governance/families/{family_id}/kill-switch
    POST /api/admin/governance/families/{family_id}/disable
    POST /api/admin/governance/accounts/{account_id}/kill-switch

Every route forwards to the same path under ``/api/v1/admin/governance``; there
is no route here that the bot-backend router does not have, and none of its
routes is missing (a test compares the two).

It is the emergency proxy's mechanism, reused and not copied (see
``admin_emergency``): this service's ``require_admin`` (admin-portal token,
``admins`` table), then a service token minted for the single upstream call
(``act=admin-emergency``, 60 s, the admin's id as ``sub``), which is the only
credential the bot-backend governance router accepts. The operator id in that
token is the reviewer the bot-backend records on every decision, so nothing a
caller puts in a request body can name someone else.

The upstream answer is returned unchanged -- status code AND body -- because a
governance refusal is an answer the operator must read as it was given (409
with its ``reason``, 404 for an unknown family or request, 422 for a body the
engine does not accept). A 401 from upstream is a 502 here, and transport
failures are told apart exactly as on the emergency routes.

Identifiers are validated before anything is sent. A family id, a promotion
request id and an account id are plain identifiers; anything else (a ``/``, a
``.``, a ``%``, a space, an empty value) is refused here with 400
``IDENTIFIER_INVALID``, so a path parameter can never select another upstream
route. The refusal is audited like any other outcome.

Audit: one line in the service log for every call (admin id, action,
identifiers, outcome status and reason). Every write -- and every read that did
not succeed -- also writes an ``auth_audit_log`` row
(``admin_governance_<action>``). A successful read does not.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Depends
from fastapi.responses import Response

from app.api import admin_emergency as emergency
from app.core.deps import require_admin

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/governance", tags=["Admin Governance"])

UPSTREAM_PREFIX = "/api/v1/admin/governance"

#: The registry read verifies the research register for every family; the
#: writes append one record. Neither waits on an exchange.
READ_TIMEOUT_SECONDS = 30.0
WRITE_TIMEOUT_SECONDS = 30.0

#: What a family id (``DAILY_TREND``), a promotion request id (``fpr_<hex>``) and
#: an account id (``brk_<hex>``) all are: one path segment of letters, digits,
#: ``_`` and ``-`` that starts with a letter or a digit.
IDENTIFIER_PATTERN = r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}"
_IDENTIFIER = re.compile(IDENTIFIER_PATTERN)

#: Request fields kept in the audit trail (plus ``reason``); never the whole body.
_SUMMARY_KEYS = ("enabled", "to_stage", "key", "reference")

_RECHECK = "Reload the family registry and its history before doing anything else."


def is_identifier(value: Any) -> bool:
    """True for a plain identifier, the only thing a path parameter may be."""
    return isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None


def _bounded(value: Any) -> Any:
    """A request value as it is kept in the audit trail: short, and never a nested structure."""
    if isinstance(value, str):
        return value[:200]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return f"<{type(value).__name__}>"


def _write_audit(admin: Dict[str, Any], action: str, details: Dict[str, Any]) -> None:
    """One ``auth_audit_log`` row for a governance call (blocking: run in the threadpool)."""
    emergency._write_audit_row(f"admin_governance_{action}", admin, details)


async def _forward(method: str, template: str, admin: Dict[str, Any], *, action: str, timeout: float,
                   ids: Optional[Dict[str, str]] = None, json_body: Optional[Dict[str, Any]] = None,
                   params: Optional[Dict[str, Any]] = None) -> Response:
    """Forward one governance call and write its audit line, whatever the outcome.

    ``template`` is the upstream path with ``{name}`` placeholders; ``ids`` are the path parameters that
    fill it, each of which must be a plain identifier."""
    ids = ids or {}
    request = {key: _bounded(value)
               for key, value in {**ids, **emergency._request_summary(json_body, _SUMMARY_KEYS)}.items()}
    invalid = sorted(name for name, value in ids.items() if not is_identifier(value))
    if invalid:
        response: Response = emergency._gateway_error(
            400, "IDENTIFIER_INVALID",
            f"{', '.join(invalid)} is not a plain identifier (letters, digits, '_' and '-'; at most 128 "
            "characters). The request was NOT sent and nothing was changed.")
    else:
        if method != "GET":
            # Intent first, as on the emergency routes: the log shows who asked
            # even when this request is cancelled before the engine answers.
            logger.warning("[GOVERNANCE-AUDIT] admin=%s action=%s requested request=%s",
                           admin.get("id"), action, request)
        response = await emergency._call_upstream(
            method, template.format(**ids), admin, timeout=timeout, json_body=json_body, params=params,
            upstream_prefix=UPSTREAM_PREFIX, tag="GOVERNANCE-PROXY", recheck=_RECHECK)
    # A successful read is routine (the page reloads the registry after every
    # action): log line only. Everything else also gets a durable row.
    await emergency._record_outcome(
        admin, action, response, request, routine_read=method == "GET" and response.status_code == 200,
        write=_write_audit, tag="GOVERNANCE-AUDIT")
    return response


def _limit(limit: Optional[int]) -> Optional[Dict[str, Any]]:
    return None if limit is None else {"limit": limit}


# ------------------------------------------------------------------------------------------------- reads
@router.get("/families")
async def list_families(admin: dict = Depends(require_admin)):
    return await _forward("GET", "/families", admin, action="families", timeout=READ_TIMEOUT_SECONDS)


@router.get("/families/{family_id}")
async def get_family(family_id: str, admin: dict = Depends(require_admin)):
    return await _forward("GET", "/families/{family_id}", admin, action="family", ids={"family_id": family_id},
                          timeout=READ_TIMEOUT_SECONDS)


@router.get("/history")
async def governance_history(admin: dict = Depends(require_admin)):
    return await _forward("GET", "/history", admin, action="history", timeout=READ_TIMEOUT_SECONDS)


@router.get("/cycles")
async def platform_cycles(limit: Optional[int] = None, admin: dict = Depends(require_admin)):
    return await _forward("GET", "/cycles", admin, action="cycles", params=_limit(limit),
                          timeout=READ_TIMEOUT_SECONDS)


@router.get("/shadow/divergences")
async def shadow_divergences(limit: Optional[int] = None, admin: dict = Depends(require_admin)):
    return await _forward("GET", "/shadow/divergences", admin, action="shadow_divergences", params=_limit(limit),
                          timeout=READ_TIMEOUT_SECONDS)


# ------------------------------------------------------------------------------------------------ writes
@router.post("/families/register")
async def register_families(body: Optional[Dict[str, Any]] = Body(None), admin: dict = Depends(require_admin)):
    """The engine takes no body here. One sent by the admin app (its typed reason) is forwarded as it came
    and its ``reason`` is kept in this service's audit row."""
    return await _forward("POST", "/families/register", admin, action="families_register", json_body=body,
                          timeout=WRITE_TIMEOUT_SECONDS)


@router.post("/families/{family_id}/evidence")
async def record_evidence(family_id: str, body: Dict[str, Any] = Body(...), admin: dict = Depends(require_admin)):
    return await _forward("POST", "/families/{family_id}/evidence", admin, action="evidence",
                          ids={"family_id": family_id}, json_body=body, timeout=WRITE_TIMEOUT_SECONDS)


@router.post("/families/{family_id}/promotion-requests")
async def request_promotion(family_id: str, body: Dict[str, Any] = Body(...),
                            admin: dict = Depends(require_admin)):
    return await _forward("POST", "/families/{family_id}/promotion-requests", admin, action="promotion_request",
                          ids={"family_id": family_id}, json_body=body, timeout=WRITE_TIMEOUT_SECONDS)


@router.post("/promotion-requests/{request_id}/approve")
async def approve_promotion(request_id: str, body: Dict[str, Any] = Body(...),
                            admin: dict = Depends(require_admin)):
    return await _forward("POST", "/promotion-requests/{request_id}/approve", admin, action="promotion_approve",
                          ids={"request_id": request_id}, json_body=body, timeout=WRITE_TIMEOUT_SECONDS)


@router.post("/promotion-requests/{request_id}/reject")
async def reject_promotion(request_id: str, body: Dict[str, Any] = Body(...),
                           admin: dict = Depends(require_admin)):
    return await _forward("POST", "/promotion-requests/{request_id}/reject", admin, action="promotion_reject",
                          ids={"request_id": request_id}, json_body=body, timeout=WRITE_TIMEOUT_SECONDS)


@router.post("/families/{family_id}/revoke")
async def revoke_promotion(family_id: str, body: Dict[str, Any] = Body(...), admin: dict = Depends(require_admin)):
    return await _forward("POST", "/families/{family_id}/revoke", admin, action="promotion_revoke",
                          ids={"family_id": family_id}, json_body=body, timeout=WRITE_TIMEOUT_SECONDS)


@router.post("/families/{family_id}/revoke-certification")
async def revoke_certification(family_id: str, body: Dict[str, Any] = Body(...),
                               admin: dict = Depends(require_admin)):
    return await _forward("POST", "/families/{family_id}/revoke-certification", admin,
                          action="certification_revoke", ids={"family_id": family_id}, json_body=body,
                          timeout=WRITE_TIMEOUT_SECONDS)


@router.post("/families/{family_id}/kill-switch")
async def family_kill_switch(family_id: str, body: Dict[str, Any] = Body(...),
                             admin: dict = Depends(require_admin)):
    return await _forward("POST", "/families/{family_id}/kill-switch", admin, action="family_kill_switch",
                          ids={"family_id": family_id}, json_body=body, timeout=WRITE_TIMEOUT_SECONDS)


@router.post("/families/{family_id}/disable")
async def family_disable(family_id: str, body: Dict[str, Any] = Body(...), admin: dict = Depends(require_admin)):
    return await _forward("POST", "/families/{family_id}/disable", admin, action="family_disable",
                          ids={"family_id": family_id}, json_body=body, timeout=WRITE_TIMEOUT_SECONDS)


@router.post("/accounts/{account_id}/kill-switch")
async def account_kill_switch(account_id: str, body: Dict[str, Any] = Body(...),
                              admin: dict = Depends(require_admin)):
    return await _forward("POST", "/accounts/{account_id}/kill-switch", admin, action="account_kill_switch",
                          ids={"account_id": account_id}, json_body=body, timeout=WRITE_TIMEOUT_SECONDS)


__all__ = ["router", "UPSTREAM_PREFIX", "IDENTIFIER_PATTERN", "is_identifier"]
