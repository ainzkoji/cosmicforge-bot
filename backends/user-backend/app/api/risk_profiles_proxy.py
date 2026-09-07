"""
Risk Profiles Proxy API

Proxies risk profile requests from frontend to bot-backend service.
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Optional
import httpx
import logging

from app.api.auth import get_current_active_user
from app.core.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

# Bot-backend service URL (internal service-to-service)
BOT_BACKEND_URL = getattr(settings, 'BOT_BACKEND_URL', 'http://127.0.0.1:9000')


class CalculatePositionSizeRequest(BaseModel):
    account_balance: float
    risk_percentage: float
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


@router.get("/templates")
async def get_risk_profile_templates(
    request: Request,
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Get preset risk profile templates."""
    try:
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{BOT_BACKEND_URL}/api/v1/risk-profiles/templates",
                headers=headers
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=response.text
                )
            
            return response.json()
            
    except httpx.RequestError as e:
        logger.error(f"Failed to connect to bot-backend: {e}")
        raise HTTPException(status_code=503, detail=f"Bot service unavailable: {str(e)}")


@router.post("/calculate")
async def calculate_position_size(
    request: Request,
    body: CalculatePositionSizeRequest,
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Calculate position size for given parameters."""
    try:
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{BOT_BACKEND_URL}/api/v1/risk-profiles/calculate",
                json=body.dict(),
                headers=headers
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=response.text
                )
            
            return response.json()
            
    except httpx.RequestError as e:
        logger.error(f"Failed to connect to bot-backend: {e}")
        raise HTTPException(status_code=503, detail=f"Bot service unavailable: {str(e)}")


@router.post("/validate")
async def validate_risk_parameters(
    request: Request,
    body: ValidateRiskParametersRequest,
    user: dict = Depends(get_current_active_user)
):
    """Proxy: Validate custom risk parameters."""
    try:
        auth_header = request.headers.get("authorization")
        headers = {"Authorization": auth_header} if auth_header else {}

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{BOT_BACKEND_URL}/api/v1/risk-profiles/validate",
                json=body.dict(),
                headers=headers
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=response.text
                )
            
            return response.json()
            
    except httpx.RequestError as e:
        logger.error(f"Failed to connect to bot-backend: {e}")
        raise HTTPException(status_code=503, detail=f"Bot service unavailable: {str(e)}")
