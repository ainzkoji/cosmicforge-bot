"""Broker-INTERNAL transfer API (Phase 2J). Mounted at /api/v1/brokers.

Every route is scoped to the authenticated user: the account id in the path
is resolved through the canonical broker resolver with the token's user id,
so a user can never address another user's account (404, not 403, so the
existence of other accounts is not revealed).

There is no withdrawal route and no route that names an external
destination: the destination is always another wallet of the same account.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import List, Optional, Union

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

from shared_lib.broker import BrokerResolverError
from shared_lib.persistence.db import DB

from app.core.auth import get_current_user_id
from app.transfers.models import IdempotencyConflict, TransferIntent, TransferOrigin
from app.transfers.reconciliation import TransferReconciler
from app.transfers.service import InternalTransferService, TransferAccessError

router = APIRouter()


def get_transfer_service() -> InternalTransferService:
    return InternalTransferService(DB())


def get_transfer_reconciler() -> TransferReconciler:
    return TransferReconciler(DB())


class InternalTransferCreate(BaseModel):
    asset: str = Field(..., min_length=1, max_length=20)
    amount: Union[str, float, int]
    source_wallet: str = Field(..., min_length=1, max_length=40)
    destination_wallet: str = Field(..., min_length=1, max_length=40)
    idempotency_key: Optional[str] = Field(None, min_length=8, max_length=128)


class TransferSettingsUpdate(BaseModel):
    mode: Optional[str] = None  # MANUAL_TRANSFER | AUTOMATED_INTERNAL_REALLOCATION
    authorize_automated_reallocation: bool = False
    auto_rebalance_enabled: Optional[bool] = None
    max_transfer_amount: Optional[str] = None
    min_funding_balance: Optional[str] = None
    min_derivatives_reserve: Optional[str] = None
    min_free_margin: Optional[str] = None
    daily_transfer_limit: Optional[str] = None
    asset_allowlist: Optional[List[str]] = None
    wallet_allowlist: Optional[List[str]] = None
    # Auto Capital Routing policy (Section 9.8)
    allowed_routes: Optional[List[str]] = Field(None, max_length=32)  # "FUND->CONTRACT" (native or purpose)
    max_transfer_pct: Optional[str] = None          # (0, 1]
    max_destination_balance: Optional[str] = None
    manual_approval_threshold: Optional[str] = None
    emergency_disabled: Optional[bool] = None       # stops automated routing immediately


def _not_found() -> HTTPException:
    return HTTPException(status_code=404, detail="Broker account not found")


def _resolver_error(exc: BrokerResolverError) -> HTTPException:
    return HTTPException(status_code=409, detail={"reason_code": exc.reason_code,
                                                  "message": "broker account cannot be used right now"})


@router.get("/{account_id}/transfer-capabilities")
def transfer_capabilities(account_id: str, user_id: str = Depends(get_current_user_id),
                          service: InternalTransferService = Depends(get_transfer_service)):
    try:
        return service.capabilities(user_id=user_id, account_id=account_id)
    except TransferAccessError:
        raise _not_found()
    except BrokerResolverError as exc:
        raise _resolver_error(exc)


@router.get("/{account_id}/wallets")
def list_wallets(account_id: str, assets: str = "USDT", user_id: str = Depends(get_current_user_id),
                 service: InternalTransferService = Depends(get_transfer_service)):
    wanted = [a.strip().upper() for a in assets.split(",") if a.strip()][:10]
    try:
        return service.wallets(user_id=user_id, account_id=account_id, assets=wanted)
    except TransferAccessError:
        raise _not_found()
    except BrokerResolverError as exc:
        raise _resolver_error(exc)


@router.get("/{account_id}/transfer-settings")
def get_transfer_settings(account_id: str, user_id: str = Depends(get_current_user_id),
                          service: InternalTransferService = Depends(get_transfer_service)):
    try:
        service._auth(user_id, account_id)
    except TransferAccessError:
        raise _not_found()
    except BrokerResolverError as exc:
        raise _resolver_error(exc)
    return service.store.settings(user_id=user_id, broker_account_id=account_id)


@router.put("/{account_id}/transfer-settings")
def update_transfer_settings(account_id: str, body: TransferSettingsUpdate,
                             user_id: str = Depends(get_current_user_id),
                             service: InternalTransferService = Depends(get_transfer_service)):
    try:
        service._auth(user_id, account_id)
    except TransferAccessError:
        raise _not_found()
    except BrokerResolverError as exc:
        raise _resolver_error(exc)
    values = body.model_dump(exclude_none=True)
    mode = values.get("mode")
    if mode not in (None, "MANUAL_TRANSFER", "AUTOMATED_INTERNAL_REALLOCATION"):
        raise HTTPException(status_code=422, detail="invalid mode")
    if mode == "AUTOMATED_INTERNAL_REALLOCATION" and not body.authorize_automated_reallocation:
        # Automation moves money without a click: it needs an explicit grant.
        raise HTTPException(status_code=422, detail={"reason_code": "AUTOMATION_AUTHORIZATION_REQUIRED"})
    for key in ("max_transfer_amount", "min_funding_balance", "min_derivatives_reserve", "min_free_margin",
                "daily_transfer_limit", "max_destination_balance", "manual_approval_threshold", "max_transfer_pct"):
        if key in values:
            try:
                d = Decimal(str(values[key]))
                if not d.is_finite() or d < 0 or (key == "max_transfer_pct" and not (0 < d <= 1)):
                    raise InvalidOperation
            except (InvalidOperation, ValueError):
                raise HTTPException(status_code=422, detail=f"invalid {key}")
    if "allowed_routes" in values:
        routes = [str(r).strip().upper().replace(" ", "") for r in values["allowed_routes"]]
        if any(r.count("->") != 1 or r.startswith("->") or r.endswith("->") or len(r) > 64 for r in routes):
            raise HTTPException(status_code=422, detail={"reason_code": "INVALID_ROUTE"})
        values["allowed_routes"] = routes
    values.pop("authorize_automated_reallocation", None)
    if mode == "MANUAL_TRANSFER":
        values["auto_rebalance_enabled"] = False
    return service.store.save_settings(user_id=user_id, broker_account_id=account_id, values=values)


@router.post("/{account_id}/internal-transfers", status_code=201)
def create_internal_transfer(account_id: str, body: InternalTransferCreate,
                             idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
                             user_id: str = Depends(get_current_user_id),
                             service: InternalTransferService = Depends(get_transfer_service)):
    key = body.idempotency_key or idempotency_key
    if not key or len(key) < 8:
        raise HTTPException(status_code=422, detail={"reason_code": "IDEMPOTENCY_KEY_REQUIRED"})
    try:
        amount = Decimal(str(body.amount))
    except (InvalidOperation, ValueError):
        raise HTTPException(status_code=422, detail={"reason_code": "INVALID_AMOUNT"})
    if not amount.is_finite() or amount <= 0:
        raise HTTPException(status_code=422, detail={"reason_code": "INVALID_AMOUNT"})
    intent = TransferIntent(user_id=user_id, broker_account_id=account_id, asset=body.asset.strip().upper(),
                            amount=amount, source_wallet=body.source_wallet.strip().upper(),
                            destination_wallet=body.destination_wallet.strip().upper(), idempotency_key=key,
                            origin=TransferOrigin.MANUAL)
    try:
        return service.request_transfer(intent)
    except TransferAccessError:
        raise _not_found()
    except IdempotencyConflict:
        raise HTTPException(status_code=409, detail={"reason_code": "IDEMPOTENCY_KEY_REUSED"})
    except BrokerResolverError as exc:
        raise _resolver_error(exc)


@router.get("/{account_id}/internal-transfers")
def list_internal_transfers(account_id: str, limit: int = 50, user_id: str = Depends(get_current_user_id),
                            service: InternalTransferService = Depends(get_transfer_service)):
    try:
        service._auth(user_id, account_id)
    except TransferAccessError:
        raise _not_found()
    except BrokerResolverError as exc:
        raise _resolver_error(exc)
    return {"transfers": service.store.list(user_id=user_id, broker_account_id=account_id,
                                            limit=max(1, min(int(limit), 200)))}


@router.get("/{account_id}/internal-transfers/{transfer_id}")
def get_internal_transfer(account_id: str, transfer_id: str, user_id: str = Depends(get_current_user_id),
                          service: InternalTransferService = Depends(get_transfer_service)):
    row = service.store.get(user_id=user_id, broker_account_id=account_id, transfer_id=transfer_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Transfer not found")
    row["events"] = service.store.events(user_id=user_id, broker_account_id=account_id, transfer_id=transfer_id)
    return row


@router.post("/{account_id}/internal-transfers/reconcile")
def reconcile_internal_transfers(account_id: str, user_id: str = Depends(get_current_user_id),
                                 service: InternalTransferService = Depends(get_transfer_service),
                                 reconciler: TransferReconciler = Depends(get_transfer_reconciler)):
    try:
        service._auth(user_id, account_id)
    except TransferAccessError:
        raise _not_found()
    except BrokerResolverError as exc:
        raise _resolver_error(exc)
    return reconciler.reconcile_account(user_id=user_id, broker_account_id=account_id)
