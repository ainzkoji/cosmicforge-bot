"""Market availability proxy (Section H, Step 7.3): the portal reaches the engine only through the user backend.

    GET /api/v1/markets/availability  ->  bot-backend /api/v1/markets/availability

The access token is forwarded; the bot-backend resolves the user and their connected accounts itself.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from app.api.auth import get_current_active_user
from app.api.proxy_utils import proxy_request

router = APIRouter(prefix="/api/v1/markets", tags=["Markets Proxy"])


@router.get("/availability")
async def market_availability(request: Request, user: dict = Depends(get_current_active_user)):
    return await proxy_request(request, "/api/v1/markets/availability", params={"user_id": user["id"]})
