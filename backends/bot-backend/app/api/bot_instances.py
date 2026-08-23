"""
Bot Instances API

Manages bot instance lifecycle (list, get, start, stop, pause, delete).
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from typing import List, Optional
import logging

from app.core.auth import get_current_active_user, require_permission
from app.core.bot_instance_service import get_bot_instance_service, BotInstanceService
from app.models.bot_instance_models import BotInstance

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/bot-instances", response_model=List[BotInstance])
def get_user_bot_instances(
    status: Optional[str] = None,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:read"))
):
    """Get all bot instances for the current user (excludes deleted/archived by default unless requested)."""
    return service.get_user_bot_instances(user["id"], status_filter=status)


@router.get("/bot-instances/inventory", response_model=List[BotInstance])
def get_bot_inventory(
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:read"))
):
    """Get all bot instances in the system including archived and deleted (admin diagnostic)."""
    return service.get_all_bot_instances()


@router.get("/bot-instances/{instance_id}", response_model=BotInstance)
def get_bot_instance(
    instance_id: str,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:read"))
):
    """Get a specific bot instance."""
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    return instance


@router.post("/bot-instances/{instance_id}/start", response_model=BotInstance)
def start_bot_instance(
    instance_id: str,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:control"))
):
    """Start a bot instance."""
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    try:
        return service.start_bot_instance(instance_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/bot-instances/{instance_id}/pause", response_model=BotInstance)
def pause_bot_instance(
    instance_id: str,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:control"))
):
    """Pause a bot instance."""
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
        
    try:
        return service.pause_bot_instance(instance_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/bot-instances/{instance_id}/stop", response_model=BotInstance)
def stop_bot_instance(
    instance_id: str,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:control"))
):
    """Stop a bot instance."""
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
        
    try:
        return service.stop_bot_instance(instance_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/bot-instances/{instance_id}")
def delete_bot_instance(
    instance_id: str,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:write"))
):
    """Delete a bot instance."""
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
        
    try:
        service.delete_bot_instance(instance_id)
        return {"status": "deleted", "id": instance_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/bot-instances/{instance_id}/archive")
def archive_bot_instance(
    instance_id: str,
    user: dict = Depends(get_current_active_user),
    service: BotInstanceService = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("bot:write"))
):
    """Archive a bot instance (preserve record but remove from active flows)."""
    instance = service.get_bot_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Bot instance not found")
    if instance.user_id != user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
        
    try:
        service.archive_bot_instance(instance_id)
        return {"status": "archived", "id": instance_id}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
