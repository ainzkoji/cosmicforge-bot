"""Market availability API (Section H, Step 7.3).

    GET /api/v1/markets/availability

Per market of the market registry: whether it is available to the signed-in user and why not (adapter
support, research certification, activation stage, connected accounts), plus the two deployment scopes
(``CRYPTO_ONLY``, ``CRYPTO_AND_TRADFI``). Read-only; it enables nothing.
"""
from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends

from app.core.auth import get_current_active_user, require_permission
from app.core.bot_instance_service import BotInstanceService, get_bot_instance_service

router = APIRouter(prefix="/api/v1/markets", tags=["Markets"])


@router.get("/availability")
def market_availability(user: dict = Depends(get_current_active_user),
                        service: BotInstanceService = Depends(get_bot_instance_service),
                        _perm: str = Depends(require_permission("bot:read"))) -> Dict[str, Any]:
    from app.markets.availability import market_availability as compute

    return compute(service.db, user["id"])
