"""
Auto Pilot Proxy API

Proxies Auto Pilot deployment requests from frontend to bot-backend service.
Enforces strict "Auto Pilot Only" contract.
"""
from fastapi import APIRouter, Depends, Request, HTTPException
from pydantic import BaseModel, Field, validator, root_validator
from typing import List, Optional
from typing_extensions import Literal

from app.api.auth import get_current_active_user
from app.api.proxy_utils import proxy_request

router = APIRouter()

class AllocationParams(BaseModel):
    """Allocation parameters for Auto Pilot."""
    total_capital_budget: float = Field(gt=0, description="Total USDT budget for this deployment")
    trade_amount_per_position: float = Field(gt=0, description="Amount or percentage per trade")
    allocation_type: Literal["fixed_amount", "percent_balance"] = "fixed_amount"

    @root_validator(skip_on_failure=True)
    def validate_budget(cls, values):
        amount = values.get("trade_amount_per_position")
        total = values.get("total_capital_budget")
        if values.get("allocation_type") == "percent_balance":
            if amount is not None and amount > 100:
                raise ValueError("Position allocation percentage cannot exceed 100")
        elif amount is not None and total is not None and amount > total:
            raise ValueError("Position amount cannot exceed total capital budget")
        return values

    class Config:
        extra = "forbid"

class DeployAutoPilotRequest(BaseModel):
    """
    Request to deploy the CATI Auto Trading strategy.
    Strictly enforces Auto Pilot parameters.
    """
    broker_account_ids: List[str] = Field(min_items=1)
    risk_mode: Literal["conservative", "medium", "aggressive"]
    allocation: AllocationParams
    execution_mode: Literal["paper", "live"] = Field(default="paper")
    # auto: the connected broker's market universe. custom: ``symbols`` only.
    symbol_universe_mode: Literal["auto", "custom"] = Field(default="auto")
    symbols: Optional[List[str]] = None
    market_type: Literal["crypto", "forex"] = Field(default="crypto")
    forex_config: Optional[dict] = None # Or use specific schema if shared
    # Daily loss limit as a FRACTION of day-opening account equity (0.03 = 3%).
    # None/omitted: the selected risk profile's default applies. Only a coarse
    # sanity bound here; bot-backend enforces the exact system limits
    # (validate_daily_loss_limit_pct) and its refusal is relayed to the client.
    daily_loss_limit_pct: Optional[float] = Field(default=None, gt=0, lt=1)

    class Config:
        extra = "forbid"

from shared_lib.deployment.contract import DeploymentRequest as StepOneDeploymentRequest, is_new_contract


async def _step_one_body(request: Request) -> Optional[StepOneDeploymentRequest]:
    """The Step 1 contract body when the request carries one (validated here,
    with the same schema the bot-backend validates again), else None."""
    try:
        raw = await request.json()
    except Exception:
        return None
    if not is_new_contract(raw):
        return None
    try:
        return StepOneDeploymentRequest.model_validate(raw)
    except Exception as exc:  # pydantic.ValidationError
        detail = exc.errors() if hasattr(exc, "errors") else str(exc)
        raise HTTPException(status_code=422, detail=detail)


def _assert_account_owned(user: dict, broker_account_id: str) -> None:
    """Ownership only (secrets stay here; the bot-backend resolves credentials itself)."""
    from app.core.broker_service import get_decrypted_credentials
    if not get_decrypted_credentials(user["id"], broker_account_id):
        raise HTTPException(status_code=422, detail={"blockers": [{"code": "ACCOUNT_NOT_CONNECTED",
            "message": "The selected exchange account is not connected to your profile.",
            "action": "Connect the exchange account and complete its validation first."}], "can_deploy": False})


@router.post("/preview")
async def preview_deployment(request: Request, user: dict = Depends(get_current_active_user)):
    """Step 1.2: the money view and blockers of a deployment, computed by the engine side."""
    body = await _step_one_body(request)
    if body is None:
        raise HTTPException(status_code=422, detail="a deployment preview requires broker_account_id, budget and risk_level")
    _assert_account_owned(user, body.broker_account_id)
    return await proxy_request(request, "/api/v1/auto-pilot/preview", params={"user_id": user["id"]},
                               json_body=body.model_dump(mode="json"), timeout=30.0)


@router.get("/bots")
async def list_bots(request: Request, user: dict = Depends(get_current_active_user)):
    """Step 1.3: every bot of the user in the Step 1 payload."""
    return await proxy_request(request, "/api/v1/auto-pilot/bots", params={"user_id": user["id"]})


@router.post("/deploy")
async def deploy_auto_pilot(
    request: Request,
    user: dict = Depends(get_current_active_user)
):
    """
    Proxy endpoint: Forward Auto Pilot deployment request to bot-backend.

    Two contracts share this customer-facing path:

    * the Step 1 contract (``broker_account_id``, ``budget``, ``risk_level``,
      ``advanced``, ``risk_acknowledged``, ``request_id``) is validated against the
      shared versioned schema and forwarded to ``/api/v1/auto-pilot/deployments``;
    * the legacy body (``broker_account_ids``, ``risk_mode``, ``allocation`` ...)
      keeps its existing validation and target.
    
    Performs parameter mapping:
    - risk_mode: medium -> balanced
    - allocation -> flat params for backend
    - Injects decrypted credentials for all broker accounts.
    """
    step_one = await _step_one_body(request)
    if step_one is not None:
        _assert_account_owned(user, step_one.broker_account_id)
        return await proxy_request(request, "/api/v1/auto-pilot/deployments", method="POST",
                                   params={"user_id": user["id"]}, json_body=step_one.model_dump(mode="json"), timeout=60.0)
    try:
        body = DeployAutoPilotRequest.model_validate(await request.json())
    except Exception as exc:  # pydantic.ValidationError
        detail = exc.errors() if hasattr(exc, "errors") else str(exc)
        raise HTTPException(status_code=422, detail=detail)

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

    # Ownership check only. Secrets are NOT forwarded: bot-backend resolves
    # credentials itself through the canonical resolver.
    from app.core.broker_service import get_decrypted_credentials

    for acc_id in body.broker_account_ids:
        creds = get_decrypted_credentials(user["id"], acc_id)
        if not creds:
             raise HTTPException(400, f"Broker account {acc_id} not found or invalid.")

    # Transform to backend contract
    backend_payload = {
        "risk_mode": body.risk_mode,
        "allocation": body.allocation.dict(),
        "broker_account_ids": body.broker_account_ids,
        "execution_mode": body.execution_mode,
        "market_type": body.market_type,
        "forex_config": body.forex_config,
        "symbol_universe_mode": body.symbol_universe_mode,
        "symbols": body.symbols,
        "daily_loss_limit_pct": body.daily_loss_limit_pct,
    }
    
    return await proxy_request(
        request,
        "/api/v1/auto-pilot/deploy",
        params={"user_id": user["id"]},
        json_body=backend_payload,
        timeout=60.0
    )
