"""The CATI account read model API (Step 1.6).

    GET /api/v1/cati/bots/{id}/status
    GET /api/v1/cati/bots/{id}/positions
    GET /api/v1/cati/bots/{id}/trades
    GET /api/v1/cati/bots/{id}/summary
    GET /api/v1/cati/bots/{id}/equity
    GET /api/v1/cati/runtime/status          (app.api.cati_runtime)

Every handler reads locally persisted, validated engine state only (no exchange
call); timestamps let the reader tell fresh from stale. A bot is readable by its
owner; an administrator reads any bot through the explicit admin routes under
/api/v1/admin/cati/bots/{id}/... (require_admin), never through the customer
routes.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core import cati_read_model as model
from app.core.auth import get_current_active_user, require_admin, require_permission
from app.core.bot_instance_service import BotInstanceService, get_bot_instance_service

router = APIRouter(prefix="/api/v1/cati/bots", tags=["CATI account read model"])
admin_router = APIRouter(prefix="/api/v1/admin/cati/bots", tags=["CATI account read model (admin)"])


def _owned_bot(service: BotInstanceService, bot_id: str, user: Dict[str, Any]):
    instance = service.get_bot_instance(bot_id)
    if instance is None:
        raise HTTPException(status_code=404, detail="Bot not found")
    if instance.user_id != user["id"]:
        # The same answer as "missing": another customer's bot is not revealed.
        raise HTTPException(status_code=404, detail="Bot not found")
    return instance


def _any_bot(service: BotInstanceService, bot_id: str):
    instance = service.get_bot_instance(bot_id)
    if instance is None:
        raise HTTPException(status_code=404, detail="Bot not found")
    return instance


def _progress() -> Optional[Dict[str, Any]]:
    try:
        from app.trading_intelligence.integration.production_runtime import progress
        return progress()
    except Exception:
        return None


def _status(service, instance):
    return model.bot_status(service.db, instance, runtime_progress=_progress())


def _trades(service, instance, page, page_size, include_open):
    return model.trades(service.db, instance, page=page, page_size=page_size, include_open=include_open)


def _equity(service, instance, since, until, limit):
    return model.equity(service.db, instance, since_ms=since, until_ms=until, limit=limit)


# ── customer routes (owner only) ────────────────────────────────────────────

@router.get("/{bot_id}/status")
def bot_status(bot_id: str, user: dict = Depends(get_current_active_user),
               service: BotInstanceService = Depends(get_bot_instance_service), _perm: str = Depends(require_permission("bot:read"))):
    return _status(service, _owned_bot(service, bot_id, user))


@router.get("/{bot_id}/positions")
def bot_positions(bot_id: str, user: dict = Depends(get_current_active_user),
                  service: BotInstanceService = Depends(get_bot_instance_service), _perm: str = Depends(require_permission("bot:read"))):
    instance = _owned_bot(service, bot_id, user)
    return {"bot_id": instance.id, "positions": model.positions(service.db, instance)}


@router.get("/{bot_id}/trades")
def bot_trades(bot_id: str, page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=200), include_open: bool = Query(True),
               user: dict = Depends(get_current_active_user), service: BotInstanceService = Depends(get_bot_instance_service),
               _perm: str = Depends(require_permission("bot:read"))):
    return {"bot_id": bot_id, **_trades(service, _owned_bot(service, bot_id, user), page, page_size, include_open)}


@router.get("/{bot_id}/summary")
def bot_summary(bot_id: str, user: dict = Depends(get_current_active_user),
                service: BotInstanceService = Depends(get_bot_instance_service), _perm: str = Depends(require_permission("bot:read"))):
    return {"bot_id": bot_id, **model.summary(service.db, _owned_bot(service, bot_id, user))}


@router.get("/{bot_id}/equity")
def bot_equity(bot_id: str, since: Optional[int] = Query(None), until: Optional[int] = Query(None), limit: int = Query(2000, ge=1, le=5000),
               user: dict = Depends(get_current_active_user), service: BotInstanceService = Depends(get_bot_instance_service),
               _perm: str = Depends(require_permission("bot:read"))):
    return {"bot_id": bot_id, **_equity(service, _owned_bot(service, bot_id, user), since, until, limit)}


@router.get("/{bot_id}/events")
def bot_events(bot_id: str, limit: int = Query(50, ge=1, le=200), user: dict = Depends(get_current_active_user),
               service: BotInstanceService = Depends(get_bot_instance_service), _perm: str = Depends(require_permission("bot:read"))):
    """The bot's recent customer events (Step 1.8), owner-scoped."""
    from app.observability import user_events
    instance = _owned_bot(service, bot_id, user)
    return {"bot_id": instance.id, "events": user_events.recent(service.db, user_id=user["id"], bot_id=instance.id, limit=limit)}


# ── administrator routes (explicit admin authority) ─────────────────────────

@admin_router.get("/{bot_id}/status")
def admin_bot_status(bot_id: str, _admin: str = Depends(require_admin), service: BotInstanceService = Depends(get_bot_instance_service)):
    return _status(service, _any_bot(service, bot_id))


@admin_router.get("/{bot_id}/positions")
def admin_bot_positions(bot_id: str, _admin: str = Depends(require_admin), service: BotInstanceService = Depends(get_bot_instance_service)):
    instance = _any_bot(service, bot_id)
    return {"bot_id": instance.id, "positions": model.positions(service.db, instance)}


@admin_router.get("/{bot_id}/trades")
def admin_bot_trades(bot_id: str, page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=200), include_open: bool = Query(True),
                     _admin: str = Depends(require_admin), service: BotInstanceService = Depends(get_bot_instance_service)):
    return {"bot_id": bot_id, **_trades(service, _any_bot(service, bot_id), page, page_size, include_open)}


@admin_router.get("/{bot_id}/summary")
def admin_bot_summary(bot_id: str, _admin: str = Depends(require_admin), service: BotInstanceService = Depends(get_bot_instance_service)):
    return {"bot_id": bot_id, **model.summary(service.db, _any_bot(service, bot_id))}


@admin_router.get("/{bot_id}/equity")
def admin_bot_equity(bot_id: str, since: Optional[int] = Query(None), until: Optional[int] = Query(None), limit: int = Query(2000, ge=1, le=5000),
                     _admin: str = Depends(require_admin), service: BotInstanceService = Depends(get_bot_instance_service)):
    return {"bot_id": bot_id, **_equity(service, _any_bot(service, bot_id), since, until, limit)}


__all__ = ["router", "admin_router"]
