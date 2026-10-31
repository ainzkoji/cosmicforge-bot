"""Administrative interface of the governed single-account market-entry policy (master plan L-08, EXT-LIQ-01).

    GET  /api/v1/admin/order-spreading/market-entry-policies                     every record, its state, the
                                                                                 ledger's integrity
    GET  /api/v1/admin/order-spreading/market-entry-policies/history             every event of the ledger
    POST /api/v1/admin/order-spreading/market-entry-policies                     record an owner-approved policy
    POST /api/v1/admin/order-spreading/market-entry-policies/{policy_id}/revoke  revoke one

Authorisation is the emergency router's: ``require_admin_emergency``, the short-lived service token the
user-backend mints for a verified operator of the ``admins`` table (401 without a valid token, 403 for any other
token, a plain ``role=admin`` access token included). The operator id from that token is the actor recorded on
every event, as an ``OperatorAuthorization``; there is no other way to produce one in the service.

What this interface cannot do, by construction:

* record a policy without an owner ``approval_ref`` and a reason (``MARKET_POLICY_APPROVAL_REF_REQUIRED`` /
  ``MARKET_POLICY_REASON_REQUIRED``; the schema refuses it too);
* calibrate anything: a ``calibrated`` field is refused (422 at the top level, ``MARKET_POLICY_CALIBRATION_NOT_PERMITTED``
  inside the terms), and the record is single-account only -- a second account on the same venue/symbol/side is
  still refused ``ORDER_SPREADING_CALIBRATION_REQUIRED``;
* loosen the conservative default: participation, slippage band, depth age and depth levels can only be equal or
  tighter (``MARKET_POLICY_TERMS_INVALID``);
* permit LIVE (``MARKET_POLICY_LIVE_NOT_PERMITTED_IN_THIS_BUILD``), or skip any other gate: the family, adapter,
  go-live, risk, fence and submission gates of the execution boundary are not affected by a record;
* submit, cancel or resize an order. There is no order route here.
"""
from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from app.core.auth import require_admin_emergency

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/admin/order-spreading", tags=["Admin Order Spreading"])
CHANNEL, PERMISSION = "admin_api:/api/v1/admin/order-spreading", "admin-emergency"


class RecordRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    venue: str = Field(min_length=1, max_length=64)
    symbol: str = Field(min_length=1, max_length=64)
    environment: str = Field(min_length=1, max_length=16)
    account_id: Optional[str] = Field(default=None, min_length=1, max_length=200)
    terms: Dict[str, Any]
    approval_ref: Optional[str] = None      # mandatory: refused by the ledger with its own code when absent
    reason: Optional[str] = None


class RevokeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: Optional[str] = None


@lru_cache(maxsize=4)
def _database(url: str):
    from shared_lib.persistence.sql import Database
    return Database(url, pool_size=4)


def get_platform_db():
    """The platform database named by ``PLATFORM_DATABASE_URL`` (as the go-live console); absent -> 503."""
    url = os.environ.get("PLATFORM_DATABASE_URL")
    if not url:
        raise HTTPException(status_code=503, detail="PLATFORM_DATABASE_URL_NOT_CONFIGURED")
    return _database(url)


def _operator(admin_id: str):
    from app.trading_intelligence.pipeline.family_governance import OperatorAuthorization
    return OperatorAuthorization(operator_id=str(admin_id), channel=CHANNEL, permission=PERMISSION)


def _ledger(db: Any):
    from app.trading_intelligence.scale.market_entry_policy import MarketEntryPolicyLedger
    ledger = MarketEntryPolicyLedger(db)
    try:
        with db.transaction(write=False) as conn:
            installed = ledger.installed(conn)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"PLATFORM_DATABASE_UNAVAILABLE:{type(exc).__name__}")
    if not installed:
        raise HTTPException(status_code=503, detail="MARKET_POLICY_LEDGER_NOT_INSTALLED")
    return ledger


def _refused(exc: Any) -> JSONResponse:
    status = 404 if exc.code == "MARKET_POLICY_NOT_FOUND" else 409
    return JSONResponse(status_code=status, content={"detail": str(exc), "reason": exc.code})


def _now(db: Any) -> int:
    with db.transaction(write=False) as conn:
        return conn.now_ms()


# ── reads ──────────────────────────────────────────────────────────────────────────────────────────────────
@router.get("/market-entry-policies")
def list_policies(admin_id: str = Depends(require_admin_emergency), db: Any = Depends(get_platform_db)):
    ledger = _ledger(db)
    ok, why = ledger.verify()
    now = _now(db)
    return {"integrity": {"ok": ok, "reason": why}, "now_ms": now,
            "policies": [r.to_plain(now) for r in ledger.records()]}


@router.get("/market-entry-policies/history")
def policy_history(admin_id: str = Depends(require_admin_emergency), db: Any = Depends(get_platform_db)):
    ledger = _ledger(db)
    ok, why = ledger.verify()
    return {"integrity": {"ok": ok, "reason": why}, "events": [e.to_plain() for e in ledger.events()]}


# ── writes ─────────────────────────────────────────────────────────────────────────────────────────────────
@router.post("/market-entry-policies")
def record_policy(body: RecordRequest, admin_id: str = Depends(require_admin_emergency),
                  db: Any = Depends(get_platform_db)):
    from app.trading_intelligence.scale.market_entry_policy import MarketPolicyError
    ledger = _ledger(db)
    details = {"venue": body.venue, "symbol": body.symbol, "environment": body.environment,
               "account_id": body.account_id, "approval_ref": body.approval_ref}
    logger.warning("[ORDER_SPREADING] MARKET_ENTRY_POLICY_RECORD_REQUESTED by admin:%s %s", admin_id, details)
    try:
        record = ledger.record(_operator(admin_id), venue=body.venue, symbol=body.symbol,
                               environment=body.environment, terms=body.terms, approval_ref=body.approval_ref,
                               reason=body.reason, account_id=body.account_id)
    except MarketPolicyError as exc:
        logger.warning("[ORDER_SPREADING] MARKET_ENTRY_POLICY_RECORD_REFUSED by admin:%s %s", admin_id, exc.code)
        return _refused(exc)
    logger.warning("[ORDER_SPREADING] MARKET_ENTRY_POLICY_RECORDED by admin:%s policy_id=%s", admin_id,
                   record.policy_id)
    return {"policy": record.to_plain(_now(db))}


@router.post("/market-entry-policies/{policy_id}/revoke")
def revoke_policy(policy_id: str, body: RevokeRequest, admin_id: str = Depends(require_admin_emergency),
                  db: Any = Depends(get_platform_db)):
    from app.trading_intelligence.scale.market_entry_policy import MarketPolicyError
    ledger = _ledger(db)
    try:
        event = ledger.revoke(_operator(admin_id), policy_id, reason=body.reason)
    except MarketPolicyError as exc:
        logger.warning("[ORDER_SPREADING] MARKET_ENTRY_POLICY_REVOKE_REFUSED by admin:%s %s", admin_id, exc.code)
        return _refused(exc)
    logger.warning("[ORDER_SPREADING] MARKET_ENTRY_POLICY_REVOKED by admin:%s policy_id=%s", admin_id, policy_id)
    return {"event": event.to_plain()}
