"""Multi-asset capability / activation status API.

``/api/v1/brokers/{account_id}/market-status``   (user-scoped) -- markets available / API-tradable /
    CATI-eligible per family, capability states with UI status + reason, capital buckets, current
    transfers, logical allocation, reservations, risk state. 404 for an account the user does not own.
``/api/v1/brokers/{account_id}/market-discovery/sync`` (user-scoped, POST) -- refresh the venue's
    instrument catalog from its official discovery API.
``/api/v1/capabilities/cati``                     (admin) -- AUTO_ACTIVE_IF_ELIGIBLE state of every CATI
    capability (shadow evidence, execution per environment, ML authority per role) with reasons.
``/api/v1/capabilities/cati/multi-asset``         (admin) -- per market family, separately: market availability,
    data readiness, manifest/freeze, pre-holdout certification, holdout, governance, execution authority,
    external venue validation (Section 20.11).
``/api/v1/capabilities/cati/datasets``            (admin) -- safe research dataset manifests + acquisition state.
``/api/v1/capabilities/cati/datasets/backfill-plan`` (admin, POST) -- a bounded research backfill PLAN inside a
    frozen universe (job identity, progress, supervised command). It downloads nothing and opens no holdout.

No route exposes credentials, and no route can enable anything: states are derived.
"""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from shared_lib.broker import BrokerResolverError
from shared_lib.persistence.db import DB

from app.core.auth import get_current_user_id, require_admin
from app.transfers.service import InternalTransferService, TransferAccessError

router = APIRouter()
admin_router = APIRouter()


def get_db() -> DB:
    return DB()


def _not_found() -> HTTPException:
    return HTTPException(status_code=404, detail="Broker account not found")


@router.get("/{account_id}/market-status")
def market_status(account_id: str, include_balances: bool = False, instruments_family: Optional[str] = None,
                  product_type: Optional[str] = None, lifecycle_stage: Optional[str] = None,
                  api_tradable: Optional[bool] = None, research_state: Optional[str] = None,
                  execution_authorized: Optional[bool] = None,
                  user_id: str = Depends(get_current_user_id), db: DB = Depends(get_db)):
    """``include_balances`` = broker-authoritative reads (balances + account mode / wallet topology).
    ``instruments_family`` (CRYPTO|FX|COMMODITIES|STOCK|INDEX) adds per-instrument capability dimensions.
    A missing or stale venue catalog is refreshed from the venue's public discovery API (throttled)."""
    from app.activation.account_status import account_status

    if instruments_family is not None and instruments_family.upper() not in (
            "CRYPTO", "FX", "COMMODITIES", "STOCK", "INDEX"):
        raise HTTPException(status_code=422, detail={"reason_code": "INVALID_INSTRUMENTS_FAMILY"})
    try:
        return account_status(db, user_id=user_id, account_id=account_id, include_balances=include_balances,
                              refresh_stale=True, instruments_family=instruments_family,
                              instrument_filters={"product_type": product_type, "lifecycle_stage": lifecycle_stage,
                                                  "api_tradable": api_tradable, "research_state": research_state,
                                                  "execution_authorized": execution_authorized})
    except TransferAccessError:
        raise _not_found()
    except BrokerResolverError as exc:
        raise HTTPException(status_code=409, detail={"reason_code": exc.reason_code,
                                                     "message": "broker account cannot be used right now"})


@router.post("/{account_id}/market-discovery/sync")
def market_discovery_sync(account_id: str, user_id: str = Depends(get_current_user_id), db: DB = Depends(get_db)):
    from app.activation.account_status import request_sync

    svc = InternalTransferService(db)
    try:
        auth = svc._auth(user_id, account_id)
    except TransferAccessError:
        raise _not_found()
    except BrokerResolverError as exc:
        raise HTTPException(status_code=409, detail={"reason_code": exc.reason_code,
                                                     "message": "broker account cannot be used right now"})
    return request_sync(db, auth, user_id=user_id)  # throttled per venue: repeats never storm the venue


@admin_router.get("/cati")
def cati_capabilities(_admin: str = Depends(require_admin), db: DB = Depends(get_db)):
    from app.activation.cati import cati_status

    registry = None
    try:
        from app.trading_intelligence.ml.registry import ModelRegistry

        registry = ModelRegistry(db)
    except Exception:
        registry = None
    return {"capabilities": cati_status(db, registry=registry)}


@admin_router.get("/cati/multi-asset")
def cati_multi_asset_status(_admin: str = Depends(require_admin), db: DB = Depends(get_db)):
    from app.market_data.research_status import multi_asset_status

    return multi_asset_status(db)


@admin_router.get("/cati/datasets")
def cati_dataset_manifests(_admin: str = Depends(require_admin), db: DB = Depends(get_db)):
    from app.market_data.research_status import dataset_manifests

    return dataset_manifests(db)


class BackfillPlanRequest(BaseModel):
    dataset: str = Field(..., max_length=32)           # FX_REFERENCE | CRYPTO_DEEP
    provider: str = Field(..., max_length=32)          # allowlisted per dataset
    instruments: List[str] = Field(..., min_length=1, max_length=200)
    timeframe: str = Field(..., max_length=8)
    start: str = Field(..., max_length=10)             # YYYY-MM-DD
    end: str = Field(..., max_length=10)


@admin_router.post("/cati/datasets/backfill-plan")
def cati_backfill_plan(body: BackfillPlanRequest, admin_id: str = Depends(require_admin)):
    from app.market_data.research_status import BackfillRequestError, backfill_plan

    try:
        return backfill_plan(dataset=body.dataset, provider=body.provider, instruments=body.instruments,
                             timeframe=body.timeframe, start=body.start, end=body.end, actor_ref=str(admin_id))
    except BackfillRequestError as exc:
        raise HTTPException(status_code=422, detail={"reason_code": exc.reason_code, "detail": exc.detail})


__all__ = ["admin_router", "router"]
