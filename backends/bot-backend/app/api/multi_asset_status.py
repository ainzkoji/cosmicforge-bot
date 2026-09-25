"""Multi-asset capability / activation status API.

``/api/v1/brokers/{account_id}/market-status``   (user-scoped) -- markets available / API-tradable /
    CATI-eligible per family, capability states with UI status + reason, capital buckets, current
    transfers, logical allocation, reservations, risk state. 404 for an account the user does not own.
``/api/v1/brokers/{account_id}/market-discovery/sync`` (user-scoped, POST) -- refresh the venue's
    instrument catalog from its official discovery API.
``/api/v1/capabilities/cati``                     (admin) -- AUTO_ACTIVE_IF_ELIGIBLE state of every CATI
    capability (shadow evidence, execution per environment, ML authority per role) with reasons.

No route exposes credentials, and no route can enable anything: states are derived.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

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
def market_status(account_id: str, include_balances: bool = False, user_id: str = Depends(get_current_user_id),
                  db: DB = Depends(get_db)):
    from app.activation.account_status import account_status

    try:
        return account_status(db, user_id=user_id, account_id=account_id, include_balances=include_balances)
    except TransferAccessError:
        raise _not_found()
    except BrokerResolverError as exc:
        raise HTTPException(status_code=409, detail={"reason_code": exc.reason_code,
                                                     "message": "broker account cannot be used right now"})


@router.post("/{account_id}/market-discovery/sync")
def market_discovery_sync(account_id: str, user_id: str = Depends(get_current_user_id), db: DB = Depends(get_db)):
    from app.activation.account_status import sync_discovery

    svc = InternalTransferService(db)
    try:
        auth = svc._auth(user_id, account_id)
    except TransferAccessError:
        raise _not_found()
    except BrokerResolverError as exc:
        raise HTTPException(status_code=409, detail={"reason_code": exc.reason_code,
                                                     "message": "broker account cannot be used right now"})
    return sync_discovery(db, auth)


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


__all__ = ["admin_router", "router"]
