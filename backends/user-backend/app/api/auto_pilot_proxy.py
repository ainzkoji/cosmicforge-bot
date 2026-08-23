"""
Auto Pilot Proxy API

Proxies Auto Pilot deployment requests from frontend to bot-backend service.
Enforces strict "Auto Pilot Only" contract.
"""
from fastapi import APIRouter, Depends, Request, HTTPException
from pydantic import BaseModel, Field, validator
from typing import List, Optional
from typing_extensions import Literal

from app.api.auth import get_current_active_user
from app.api.proxy_utils import proxy_request

router = APIRouter()

class AllocationParams(BaseModel):
    """Allocation parameters for Auto Pilot."""
    total_capital_budget: float = Field(gt=0, description="Total USDT budget for this deployment")
    trade_amount_per_position: float = Field(gt=0, description="USDT amount per trade")

    class Config:
        extra = "forbid"

class DeployAutoPilotRequest(BaseModel):
    """
    Request to deploy the Auto Pilot (Master Ensemble) strategy.
    Strictly enforces Auto Pilot parameters.
    """
    broker_account_ids: List[str] = Field(min_items=1)
    risk_mode: Literal["conservative", "medium", "aggressive"]
    allocation: AllocationParams
    execution_mode: Literal["paper", "live"] = Field(default="paper")
    symbol_universe_mode: Literal["auto"] = Field(default="auto")
    market_type: Literal["crypto", "forex"] = Field(default="crypto")
    forex_config: Optional[dict] = None # Or use specific schema if shared

    class Config:
        extra = "forbid"

@router.post("/deploy")
async def deploy_auto_pilot(
    request: Request,
    body: DeployAutoPilotRequest,
    user: dict = Depends(get_current_active_user)
):
    """
    Proxy endpoint: Forward Auto Pilot deployment request to bot-backend.
    
    Performs parameter mapping:
    - risk_mode: medium -> balanced
    - allocation -> flat params for backend
    - Injects decrypted credentials for all broker accounts.
    """
    
    # Map risk_mode to backend risk_level
    risk_map = {
        "conservative": "conservative",
        "medium": "balanced",
        "aggressive": "aggressive"
    }

    # KYC Gating for Live Forex
    # OANDA (and other regulated brokers) require strict KYC.
    # We use verified email as a proxy for "User Identity Verified" in this iteration.
    if getattr(body, "market_type", "crypto") == "forex" and body.execution_mode == "live":
        if not user.get("is_verified"):
             raise HTTPException(403, "Live Forex trading requires account verification. Please verify your email.")

    # Inject credentials
    from app.core.broker_service import get_decrypted_credentials
    credentials_map = {}
    
    for acc_id in body.broker_account_ids:
        creds = get_decrypted_credentials(user["id"], acc_id)
        if not creds:
             raise HTTPException(400, f"Broker account {acc_id} not found or invalid.")
        credentials_map[acc_id] = creds

    # Transform to backend contract
    backend_payload = {
        "risk_level": risk_map[body.risk_mode],
        "allocation_type": "fixed_amount", # Enforced backend type
        "allocation_value": body.allocation.trade_amount_per_position,
        "capital_allocation": body.allocation.total_capital_budget,
        "capital_allocation_type": "fixed_amount",
        "broker_account_ids": body.broker_account_ids,
        "broker_credentials_map": credentials_map, # New injection
        "mode": body.execution_mode,
        "market_type": getattr(body, "market_type", "crypto"),
        "forex_config": getattr(body, "forex_config", None)
    }
    
    return await proxy_request(
        request,
        "/api/v1/auto-pilot/deploy",
        params={"user_id": user["id"]},
        json_body=backend_payload,
        timeout=60.0
    )
