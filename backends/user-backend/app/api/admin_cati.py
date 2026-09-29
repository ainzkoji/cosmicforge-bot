"""Operator (admin) CATI research / certification status -- proxied to the bot-backend admin family
``/api/v1/capabilities/cati`` (Sections 20.9-20.11, 21.9).

Two independent authorizations: this service's ``require_admin`` AND the bot-backend's own ``require_admin`` on
the forwarded token. Read-only views plus a bounded backfill PLAN; nothing here downloads data, opens a holdout,
changes certification or governance state, or enables execution.
"""
from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Body, Depends, Request

from app.api.proxy_utils import proxy_request as _proxy
from app.core.deps import require_admin

router = APIRouter(prefix="/admin/cati", tags=["Admin CATI"])


@router.get("/capabilities")
async def cati_capabilities(request: Request, _admin: dict = Depends(require_admin)):
    return await _proxy(request, "/api/v1/capabilities/cati")


@router.get("/multi-asset")
async def cati_multi_asset_status(request: Request, _admin: dict = Depends(require_admin)):
    return await _proxy(request, "/api/v1/capabilities/cati/multi-asset", timeout=30.0)


@router.get("/datasets")
async def cati_datasets(request: Request, _admin: dict = Depends(require_admin)):
    return await _proxy(request, "/api/v1/capabilities/cati/datasets", timeout=30.0)


@router.post("/datasets/backfill-plan")
async def cati_backfill_plan(request: Request, body: Dict[str, Any] = Body(...),
                             _admin: dict = Depends(require_admin)):
    return await _proxy(request, "/api/v1/capabilities/cati/datasets/backfill-plan", json_body=body, timeout=30.0)
