"""
Risk Profile API Endpoints

Provides preset risk profiles and position sizing calculations.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional
from app.core.user_strategy_config_service import UserStrategyConfigService
from app.risk.sizing import PositionSizer, calculate_atr

router = APIRouter()


# =========================
# Request/Response Models
# =========================
class CalculatePositionSizeRequest(BaseModel):
    account_balance: float
    risk_percentage: float  # 1.0 = 1%
    entry_price: float
    stop_loss_price: Optional[float] = None
    atr: Optional[float] = None
    atr_multiplier: Optional[float] = 2.0


class ValidateRiskParametersRequest(BaseModel):
    portfolio_risk_pct: float
    per_trade_risk_pct: float
    max_margin_usage_pct: float
    max_drawdown_pct: float
    daily_loss_limit_pct: float


# =========================
# Endpoints
# =========================
@router.get("/templates")
async def get_risk_profile_templates():
    """Get preset risk profile templates."""
    return {
        "conservative": UserStrategyConfigService.get_risk_profile_preset("conservative"),
        "balanced": UserStrategyConfigService.get_risk_profile_preset("balanced"),
        "aggressive": UserStrategyConfigService.get_risk_profile_preset("aggressive")
    }


@router.post("/calculate")
async def calculate_position_size(request: CalculatePositionSizeRequest):
    """
    Calculate position size for given parameters.
    
    Either stop_loss_price OR (atr + atr_multiplier) must be provided.
    """
    if not request.stop_loss_price and not request.atr:
        raise HTTPException(
            status_code=400,
            detail="Either stop_loss_price or atr must be provided"
        )
    
    # Calculate risk amount
    risk_amount = request.account_balance * (request.risk_percentage / 100)
    
    # Calculate stop distance
    if request.stop_loss_price:
        stop_distance = abs(request.entry_price - request.stop_loss_price)
    else:
        stop_distance = request.atr * request.atr_multiplier
    
    if stop_distance <= 0:
        raise HTTPException(status_code=400, detail="Invalid stop distance")
    
    # Calculate position size
    position_size = risk_amount / stop_distance
    position_value = position_size * request.entry_price
    
    return {
        "position_size": position_size,
        "position_value_usdt": position_value,
        "risk_amount_usdt": risk_amount,
        "stop_distance": stop_distance,
        "risk_percentage": request.risk_percentage,
        "entry_price": request.entry_price,
        "stop_loss_price": request.stop_loss_price if request.stop_loss_price else request.entry_price - stop_distance,
        "potential_loss": risk_amount
    }


@router.post("/validate")
async def validate_risk_parameters(request: ValidateRiskParametersRequest):
    """Validate custom risk parameters."""
    errors = []
    warnings = []
    
    # Validate ranges
    if request.portfolio_risk_pct < 0.01 or request.portfolio_risk_pct > 0.20:
        errors.append("Portfolio risk must be between 1% and 20%")
    
    if request.per_trade_risk_pct < 0.001 or request.per_trade_risk_pct > 0.05:
        errors.append("Per-trade risk must be between 0.1% and 5%")
    
    if request.per_trade_risk_pct > request.portfolio_risk_pct:
        errors.append("Per-trade risk cannot exceed portfolio risk")
    
    if request.max_margin_usage_pct < 0.10 or request.max_margin_usage_pct > 0.90:
        errors.append("Max margin usage must be between 10% and 90%")
    
    if request.max_drawdown_pct < 0.05 or request.max_drawdown_pct > 0.50:
        errors.append("Max drawdown must be between 5% and 50%")
    
    if request.daily_loss_limit_pct < 0.01 or request.daily_loss_limit_pct > 0.20:
        errors.append("Daily loss limit must be between 1% and 20%")
    
    # Warnings for aggressive settings
    if request.portfolio_risk_pct > 0.10:
        warnings.append("Portfolio risk above 10% is very aggressive")
    
    if request.max_margin_usage_pct > 0.70:
        warnings.append("Margin usage above 70% increases liquidation risk")
    
    if request.per_trade_risk_pct > 0.02:
        warnings.append("Per-trade risk above 2% is aggressive")
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }
