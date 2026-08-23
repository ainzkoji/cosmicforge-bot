"""
Auto Pilot API

Handles deployment requests for the Auto Pilot (Master Ensemble) strategy.
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field, validator, root_validator
from typing import List, Optional, Dict, Any
from typing_extensions import Literal
import logging

# Configure logger
logger = logging.getLogger(__name__)

from app.core.auth import get_current_active_user, require_permission
from app.core.bot_instance_service import get_bot_instance_service
from app.models.bot_instance_models import BotInstance

router = APIRouter()

class AutoPilotAllocation(BaseModel):
    total_capital_budget: float = Field(gt=0, description="Total capital cap")
    trade_amount_per_position: float = Field(gt=0, description="Allocation per bot/position")

class DeployAutoPilotRequest(BaseModel):
    """Request to deploy the Auto Pilot (Master Ensemble) strategy."""
    broker_account_ids: List[str] = Field(min_items=1)
    
    # Primary Fields (New Frontend)
    risk_mode: Optional[Literal["conservative", "medium", "aggressive"]] = None
    allocation: Optional[AutoPilotAllocation] = None
    execution_mode: Optional[Literal["paper", "live"]] = Field(default=None)
    symbol_universe_mode: Literal["auto"] = Field(default="auto")
    market_type: Optional[Literal["crypto", "forex"]] = Field(default="crypto")
    forex_config: Optional[Dict[str, Any]] = Field(default=None)

    # Legacy Fields (Stale Frontend Support)
    risk_level: Optional[Literal["conservative", "balanced", "aggressive"]] = None
    allocation_value: Optional[float] = None
    capital_allocation: Optional[float] = None
    mode: Optional[Literal["paper", "live"]] = None

    @root_validator(pre=True)
    def normalize_payload(cls, values):
        """Map legacy frontend fields to new schema if needed."""
        # 1. Map mode -> execution_mode
        if values.get("mode") and not values.get("execution_mode"):
            values["execution_mode"] = values["mode"]
        elif not values.get("execution_mode"):
             values["execution_mode"] = "paper" # Default

        # 2. Map risk_level -> risk_mode
        if values.get("risk_level") and not values.get("risk_mode"):
            rl = values["risk_level"]
            mapping = {"balanced": "medium"}
            values["risk_mode"] = mapping.get(rl, rl)
        
        # 3. Map flat allocation -> allocation object
        if not values.get("allocation"):
            if values.get("allocation_value"):
                values["allocation"] = {
                    "total_capital_budget": values.get("capital_allocation", values["allocation_value"]),
                    "trade_amount_per_position": values["allocation_value"]
                }
        
        return values

    @validator("forex_config")
    def validate_forex_config(cls, v, values):
        """Validate forex_config when market_type is forex."""
        market_type = values.get("market_type")
        if market_type == "forex":
            if not v or not isinstance(v, dict):
                raise ValueError("forex_config is required when market_type is forex")
            allowlist = v.get("allowlist")
            if not allowlist or not isinstance(allowlist, list) or len(allowlist) == 0:
                raise ValueError("forex_config.allowlist must be a non-empty list when market_type is forex")
        return v

class AutoPilotDeploymentResponse(BaseModel):
    """Response for Auto Pilot deployment."""
    instances: List[BotInstance]
    risk_warning: str
    message: str

class AutoPilotStatus(BaseModel):
    """Aggregated status of Auto Pilot instances."""
    active_count: int
    paused_count: int
    total_equity: float
    unrealized_pnl: float
    instances: List[BotInstance]

# A-8: Minimum trade amount per position enforced at deployment time.
_MIN_TRADE_AMOUNT = 50.0


@router.post("/deploy", response_model=AutoPilotDeploymentResponse)
def deploy_auto_pilot(
    request: DeployAutoPilotRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_active_user), 
    service = Depends(get_bot_instance_service),
    _perm: str = Depends(require_permission("strategy:execute"))
):
    """
    Deploy the Auto Pilot strategy.
    
    WARNING: High Risk Strategy. 
    Leverage is capped at 10x. Stop Loss is dynamic but capped at 30%.
    """
    
    # 1. Check Allocation Integrity
    if not request.allocation:
        raise HTTPException(status_code=422, detail="Allocation settings (capital/trade amount) are required.")

    # A-8: Enforce minimum trade amount per position (50 USDT).
    if request.allocation.trade_amount_per_position < _MIN_TRADE_AMOUNT:
        logger.warning(
            "TRADE_AMOUNT_TOO_SMALL_MINIMUM_50_USDT: user=%s requested %.2f USDT per position",
            user.get("id"), request.allocation.trade_amount_per_position,
        )
        raise HTTPException(
            status_code=422,
            detail={
                "error_code": "TRADE_AMOUNT_TOO_SMALL_MINIMUM_50_USDT",
                "message": (
                    f"Trade amount per position is too small. "
                    f"Minimum allowed amount is {_MIN_TRADE_AMOUNT:.0f} USDT to avoid "
                    f"exchange-level minimum notional failures. "
                    f"Requested: {request.allocation.trade_amount_per_position:.2f} USDT."
                ),
            },
        )

    # 2. Check Entitlements (Max Bots)
    entitlements = user.get("entitlements", {})
    max_bots = entitlements.get("max_bots")
    
    if max_bots and str(max_bots).lower() != "unlimited":
        try:
            limit = int(max_bots)
            # Count active/paused bots
            current_bots = service.get_user_bot_instances(user["id"])
            count = len(current_bots)
            requested = len(request.broker_account_ids)
            
            if count + requested > limit:
                raise HTTPException(
                    status_code=403, 
                    detail=f"Plan limit reached. You have {count} bots, limit is {limit}. Upgrade your plan."
                )
        except ValueError:
             pass

    try:
        # Map Frontend Request to Internal Service Parameters
        
        # Risk map: "medium" -> "balanced"
        risk_map = {
            "conservative": "conservative",
            "medium": "balanced",
            "aggressive": "aggressive"
        }
        internal_risk = risk_map.get(request.risk_mode, "balanced")
        
        # Market Type Map: "crypto" -> "CRYPTO"
        internal_market = request.market_type.upper() if request.market_type else "CRYPTO"
        
        # Allocation (Safely accessed now)
        # service.deploy_auto_pilot is synchronous
        instances = service.deploy_auto_pilot(
            user_id=user["id"],
            risk_level=internal_risk,
            allocation_type="fixed_amount", # Defaulting as frontend doesn't send type
            allocation_value=request.allocation.trade_amount_per_position,
            broker_account_ids=request.broker_account_ids,
            mode=request.execution_mode,
            capital_allocation=request.allocation.total_capital_budget,
            capital_allocation_type="fixed_amount",
            market_type=internal_market,
            forex_config=request.forex_config
        )
        
        # Generate Risk Warning based on selection
        warning = "Standard Risk Parameters Applied."
        if request.risk_mode == "aggressive":
            warning = "⚠️ AGGRESSIVE RISK: High leverage and wide stops. Significant capital loss is possible."
        elif request.risk_mode == "medium": # balanced
             warning = "ℹ️ BALANCED RISK: Moderate leverage. Monitor performance regularly."
             
        message = f"Successfully deployed {len(instances)} Auto Pilot instances."
        
        return AutoPilotDeploymentResponse(
            instances=instances,
            risk_warning=warning,
            message=message
        )
            
    except Exception as e:
        logger.error(f"Failed to deploy Auto Pilot: {str(e)}")
        # raise HTTPException(status_code=500, detail=f"Failed to deploy Auto Pilot: {str(e)}")
        # Raise 422 if it's a value error (like missing fields) or 500 otherwise
        if isinstance(e, ValueError):
             # Section F: propagate structured readiness gate rejections if present.
             try:
                 import json as _json
                 payload = _json.loads(str(e))
                 if isinstance(payload, dict) and payload.get("reason") and payload.get("status") == "REJECTED":
                     raise HTTPException(status_code=403, detail=payload)
             except Exception:
                 pass
             raise HTTPException(status_code=422, detail=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to deploy Auto Pilot: {str(e)}")


@router.get("/status", response_model=AutoPilotStatus)
def get_auto_pilot_status(
    user: dict = Depends(get_current_active_user),
    service = Depends(get_bot_instance_service)
):
    """Get aggregated status of all Auto Pilot instances."""
    from shared_lib.persistence.db import DB
    
    # Filter for master_ensemble strategy
    all_bots = service.get_user_bot_instances(user["id"])
    ap_bots = [b for b in all_bots if b.strategy_id == "master_ensemble"]
    
    active = sum(1 for b in ap_bots if b.status == "active")
    paused = sum(1 for b in ap_bots if b.status == "paused")
    
    total_eq = 0.0
    total_unrealized = 0.0
    
    # Aggregate Metrics if we have running instances
    if ap_bots:
        db = DB()
        with db.connect() as conn:
            for bot in ap_bots:
                # 1. Get Equity
                if bot.last_run_id:
                     # Check decision_traces first for latest equity
                     trace = conn.execute(
                         "SELECT equity FROM decision_traces WHERE run_id = ? ORDER BY ts DESC LIMIT 1",
                         (bot.last_run_id,)
                     ).fetchone()
                     
                     if trace and trace["equity"]:
                         total_eq += trace["equity"]
                         # approximate unrealized using allocation if available
                         if bot.capital_allocation:
                             total_unrealized += (trace["equity"] - bot.capital_allocation)
                     elif bot.capital_allocation:
                         total_eq += bot.capital_allocation
                elif bot.capital_allocation:
                     total_eq += bot.capital_allocation

    return AutoPilotStatus(
        active_count=active,
        paused_count=paused,
        total_equity=total_eq,
        unrealized_pnl=total_unrealized,
        instances=ap_bots
    )


@router.post("/pause_all")
def pause_auto_pilot(
    user: dict = Depends(get_current_active_user),
    service = Depends(get_bot_instance_service)
):
    """Pause all active Auto Pilot instances."""
    all_bots = service.get_user_bot_instances(user["id"])
    ap_bots = [b for b in all_bots if b.strategy_id == "master_ensemble" and b.status == "active"]
    
    paused = []
    for bot in ap_bots:
        try:
            service.pause_bot_instance(bot.id)
            paused.append(bot.id)
        except Exception as e:
            logger.error(f"Failed to pause {bot.id}: {e}")
            
    return {"message": f"Paused {len(paused)} instances", "ids": paused}


@router.post("/resume_all")
def resume_auto_pilot(
    user: dict = Depends(get_current_active_user),
    service = Depends(get_bot_instance_service)
):
    """Resume all paused Auto Pilot instances."""
    all_bots = service.get_user_bot_instances(user["id"])
    ap_bots = [b for b in all_bots if b.strategy_id == "master_ensemble" and b.status == "paused"]
    
    resumed = []
    for bot in ap_bots:
        try:
            service.start_bot_instance(bot.id)
            resumed.append(bot.id)
        except Exception as e:
            logger.error(f"Failed to resume {bot.id}: {e}")
            
    return {"message": f"Resumed {len(resumed)} instances", "ids": resumed}

