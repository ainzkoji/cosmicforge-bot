"""CATI read-model proxy (Step 1.6 / 1.7): the customer portal reaches the
engine only through the user backend.

    /api/v1/cati/runtime/status
    /api/v1/cati/bots/{id}/status | positions | trades | summary | equity

The access token is forwarded; the bot-backend checks ownership and admin
authority itself. Query parameters pass through unchanged.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from app.api.auth import get_current_active_user
from app.api.proxy_utils import proxy_request

router = APIRouter(prefix="/api/v1/cati", tags=["CATI Proxy"])


@router.get("/runtime/status")
async def runtime_status(request: Request, user: dict = Depends(get_current_active_user)):
    return await proxy_request(request, "/api/v1/cati/runtime/status")


@router.get("/bots/{bot_id}/status")
async def bot_status(request: Request, bot_id: str, user: dict = Depends(get_current_active_user)):
    return await proxy_request(request, f"/api/v1/cati/bots/{bot_id}/status")


@router.get("/bots/{bot_id}/positions")
async def bot_positions(request: Request, bot_id: str, user: dict = Depends(get_current_active_user)):
    return await proxy_request(request, f"/api/v1/cati/bots/{bot_id}/positions")


@router.get("/bots/{bot_id}/trades")
async def bot_trades(request: Request, bot_id: str, user: dict = Depends(get_current_active_user)):
    return await proxy_request(request, f"/api/v1/cati/bots/{bot_id}/trades")


@router.get("/bots/{bot_id}/summary")
async def bot_summary(request: Request, bot_id: str, user: dict = Depends(get_current_active_user)):
    return await proxy_request(request, f"/api/v1/cati/bots/{bot_id}/summary")


@router.get("/bots/{bot_id}/equity")
async def bot_equity(request: Request, bot_id: str, user: dict = Depends(get_current_active_user)):
    return await proxy_request(request, f"/api/v1/cati/bots/{bot_id}/equity")


@router.get("/bots/{bot_id}/events")
async def bot_events(request: Request, bot_id: str, user: dict = Depends(get_current_active_user)):
    return await proxy_request(request, f"/api/v1/cati/bots/{bot_id}/events")


__all__ = ["router"]
