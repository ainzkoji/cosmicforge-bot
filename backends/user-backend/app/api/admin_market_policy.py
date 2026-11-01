"""Operator (admin) market-entry policy (master plan L-08) -- proxied to the bot-backend
``/api/v1/admin/order-spreading``.

    GET  /api/admin/order-spreading/market-entry-policies                     every record, its state, integrity
    GET  /api/admin/order-spreading/market-entry-policies/history             every event of the ledger
    POST /api/admin/order-spreading/market-entry-policies                     record an owner-approved policy
    POST /api/admin/order-spreading/market-entry-policies/{policy_id}/revoke  revoke one

The governance proxy's mechanism, reused (``admin_governance._forward``): this service's ``require_admin``
(admin-portal token, ``admins`` table), then a 60-second service token minted for the one upstream call; the
operator id in it is the actor the bot-backend records. The upstream answer is returned unchanged; the policy
id is validated as a plain identifier before anything is sent; every write is audited.

What a record can and cannot do is the engine's rule, not this proxy's: single account, never calibrated, terms
equal to or tighter than the conservative default, mandatory expiry and absolute notional cap, LIVE refused in
this build, the all-symbols scope for DEMO only.
"""
from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Body, Depends

from app.api import admin_emergency as emergency
from app.api import admin_governance as governance
from app.core.deps import require_admin

router = APIRouter(prefix="/admin/order-spreading", tags=["Admin Market-Entry Policy"])
UPSTREAM_PREFIX = "/api/v1/admin/order-spreading"
_SUMMARY_KEYS = ("venue", "symbol", "environment", "account_id", "approval_ref")
_RECHECK = "Reload the market-entry policies before doing anything else."


def _write_audit(admin: Dict[str, Any], action: str, details: Dict[str, Any]) -> None:
    emergency._write_audit_row(f"admin_market_policy_{action}", admin, details)


async def _forward(method: str, template: str, admin: Dict[str, Any], *, action: str, ids=None, json_body=None):
    ids = ids or {}
    request = {key: governance._bounded(value)
               for key, value in {**ids, **emergency._request_summary(json_body, _SUMMARY_KEYS)}.items()}
    invalid = sorted(name for name, value in ids.items() if not governance.is_identifier(value))
    if invalid:
        response = emergency._gateway_error(
            400, "IDENTIFIER_INVALID", f"{', '.join(invalid)} is not a plain identifier. The request was NOT sent.")
    else:
        response = await emergency._call_upstream(
            method, template.format(**ids), admin, timeout=30.0, json_body=json_body, params=None,
            upstream_prefix=UPSTREAM_PREFIX, tag="MARKET-POLICY-PROXY", recheck=_RECHECK)
    await emergency._record_outcome(admin, action, response, request,
                                    routine_read=method == "GET" and response.status_code == 200,
                                    write=_write_audit, tag="MARKET-POLICY-AUDIT")
    return response


@router.get("/market-entry-policies")
async def list_policies(admin: dict = Depends(require_admin)):
    return await _forward("GET", "/market-entry-policies", admin, action="list")


@router.get("/market-entry-policies/history")
async def policy_history(admin: dict = Depends(require_admin)):
    return await _forward("GET", "/market-entry-policies/history", admin, action="history")


@router.post("/market-entry-policies")
async def record_policy(body: Dict[str, Any] = Body(...), admin: dict = Depends(require_admin)):
    return await _forward("POST", "/market-entry-policies", admin, action="record", json_body=body)


@router.post("/market-entry-policies/{policy_id}/revoke")
async def revoke_policy(policy_id: str, body: Dict[str, Any] = Body(...), admin: dict = Depends(require_admin)):
    return await _forward("POST", "/market-entry-policies/{policy_id}/revoke", admin, action="revoke",
                          ids={"policy_id": policy_id}, json_body=body)


__all__ = ["router", "UPSTREAM_PREFIX"]
