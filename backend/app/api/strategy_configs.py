"""
Strategy Configuration API Endpoints

Provides REST API for managing user strategy configurations.
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from app.api.auth import get_current_active_user
from app.persistence.db import DB
from app.core.user_strategy_config_service import UserStrategyConfigService
from app.risk.account_protection import AccountProtection

router = APIRouter()


#  =========================
# Request/Response Models
# =========================
class CreateConfigRequest(BaseModel):
    broker_account_id: str
    strategy_id: str
    name: str
    risk_parameters: Dict[str,  Any]
    strategy_parameters: Optional[Dict[str, Any]] = None


class UpdateConfigRequest(BaseModel):
    name: Optional[str] = None
    risk_parameters: Optional[Dict[str, Any]] = None
    strategy_parameters: Optional[Dict[str, Any]] = None


class ConfigResponse(BaseModel):
    id: str
    user_id: str
    broker_account_id: str
    strategy_id: str
    name: str
    status: str
    created_at: str
    updated_at: str
    activated_at: Optional[str]
    risk_parameters: Optional[Dict[str, Any]] = None
    strategy_parameters: Optional[Dict[str, Any]] = None
    protection_state: Optional[Dict[str, Any]] = None


# =========================
# Dependency Injection
# =========================
def get_config_service() -> UserStrategyConfigService:
    return UserStrategyConfigService(DB())


def get_protection_service() -> AccountProtection:
    return AccountProtection(DB())


# =========================
# Endpoints
# =========================
@router.post("", response_model=ConfigResponse)
async def create_configuration(
    request: CreateConfigRequest,
    user: dict = Depends(get_current_active_user),
    service: UserStrategyConfigService = Depends(get_config_service)
):
    """Create a new strategy configuration."""
    try:
        config_id = service.create_config(
            user_id=user["id"],
            broker_account_id=request.broker_account_id,
            strategy_id=request.strategy_id,
            name=request.name,
            risk_params=request.risk_parameters,
            strategy_params=request.strategy_parameters
        )
        
        config = service.get_config(config_id)
        return config
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[Dict[str, Any]])
async def list_configurations(
    status: Optional[str] = None,
    user: dict = Depends(get_current_active_user),
    service: UserStrategyConfigService = Depends(get_config_service)
):
    """List all configurations for the current user."""
    configs = service.list_configs(user["id"], status=status)
    return configs


@router.get("/{config_id}", response_model=ConfigResponse)
async def get_configuration(
    config_id: str,
    user: dict = Depends(get_current_active_user),
    service: UserStrategyConfigService = Depends(get_config_service)
):
    """Get a specific configuration."""
    config = service.get_config(config_id, user_id=user["id"])
    if not config:
        raise HTTPException(status_code=404, detail="Configuration not found")
    return config


@router.put("/{config_id}", response_model=ConfigResponse)
async def update_configuration(
    config_id: str,
    request: UpdateConfigRequest,
    user: dict = Depends(get_current_active_user),
    service: UserStrategyConfigService = Depends(get_config_service)
):
    """Update a configuration."""
    updates = {}
    if request.name:
        updates["name"] = request.name
    if request.risk_parameters:
        updates["risk_parameters"] = request.risk_parameters
    if request.strategy_parameters:
        updates["strategy_parameters"] = request.strategy_parameters
    
    success = service.update_config(config_id, user["id"], updates)    
    if not success:
        raise HTTPException(status_code=404, detail="Configuration not found or access denied")
    
    config = service.get_config(config_id)
    return config


@router.delete("/{config_id}")
async def delete_configuration(
    config_id: str,
    user: dict = Depends(get_current_active_user),
    service: UserStrategyConfigService = Depends(get_config_service)
):
    """Soft delete a configuration (archive it)."""
    success = service.update_config(
        config_id, 
        user["id"], 
        {"status": "archived"}
    )
    if not success:
        raise HTTPException(status_code=404, detail="Configuration not found or access denied")
    return {"message": "Configuration archived successfully"}


@router.post("/{config_id}/activate")
async def activate_configuration(
    config_id: str,
    user: dict = Depends(get_current_active_user),
    service: UserStrategyConfigService = Depends(get_config_service)
):
    """Activate a configuration for trading."""
    success = service.activate_config(config_id, user["id"])
    if not success:
        raise HTTPException(status_code=404, detail="Configuration not found or access denied")
    return {"message": "Configuration activated successfully"}


@router.post("/{config_id}/deactivate")
async def deactivate_configuration(
    config_id: str,
    user: dict = Depends(get_current_active_user),
    service: UserStrategyConfigService = Depends(get_config_service)
):
    """Deactivate a configuration (pause trading)."""
    success = service.deactivate_config(config_id, user["id"])
    if not success:
        raise HTTPException(status_code=404, detail="Configuration not found or access denied")
    return {"message": "Configuration deactivated successfully"}


@router.get("/{config_id}/protection-status")
async def get_protection_status(
    config_id: str,
    equity: float,
    user: dict = Depends(get_current_active_user),
    service: UserStrategyConfigService = Depends(get_config_service),
    protection: AccountProtection = Depends(get_protection_service)
):
    """Get current protection status for a configuration."""
    config = service.get_config(config_id, user_id=user["id"])
    if not config:
        raise HTTPException(status_code=404, detail="Configuration not found")
    
    risk_params = config.get("risk_parameters", {})
    
    status = protection.check_protection(
        config_id=config_id,
        daily_loss_limit_pct=risk_params.get("daily_loss_limit_pct", 0.05),
        max_drawdown_pct=risk_params.get("max_drawdown_pct", 0.15),
        current_equity=equity
    )
    
    return {
        "is_protected": status.is_protected,
        "protection_reason": status.protection_reason,
        "daily_loss_today": status.daily_loss_today,
        "current_drawdown_pct": status.current_drawdown_pct,
        "consecutive_losses": status.consecutive_losses,
        "cool_down_until": status.cool_down_until,
        "details": status.details
    }


@router.post("/{config_id}/reset-protection")
async def reset_protection(
    config_id: str,
    user: dict = Depends(get_current_active_user),
    service: UserStrategyConfigService = Depends(get_config_service),
    protection: AccountProtection = Depends(get_protection_service)
):
    """Manually reset protection (admin/manual override)."""
    config = service.get_config(config_id, user_id=user["id"])
    if not config:
        raise HTTPException(status_code=404, detail="Configuration not found")
    
    protection.reset_protection(config_id)
    return {"message": "Protection reset successfully"}
