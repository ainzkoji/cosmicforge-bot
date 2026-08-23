from fastapi import APIRouter, Depends, HTTPException, Body, Request
from typing import List, Dict, Any

from app.api.auth import get_current_user_id
from app.core.broker_service import (
    get_broker_catalog,
    create_broker_account_draft,
    submit_broker_credentials,
    validate_broker_account,
    list_user_broker_accounts,
    get_broker_account,
    disconnect_broker_account,
    delete_broker_account_permanently,
    get_broker_summary,
    _test_broker_connection
)

router = APIRouter(tags=["Brokers"])

# -----------------------------------------------
# Schemas (Inline for simplicity, can move to app.schemas)
# -----------------------------------------------
from pydantic import BaseModel
from typing import Optional

class CreateDraftRequest(BaseModel):
    broker_id: str
    market_type: str

class CredentialsRequest(BaseModel):
    credentials: Dict[str, Any]

class UpdateLabelRequest(BaseModel):
    label: str

class TestConnectionRequest(BaseModel):
    broker_id: str
    credentials: Dict[str, Any]
    environment: Optional[str] = "live"

# -----------------------------------------------
# Endpoints
# -----------------------------------------------

@router.get("/catalog")
def get_catalog(user_id: str = Depends(get_current_user_id)):
    """List available brokers and their connection requirements."""
    return {"brokers": get_broker_catalog(user_id)}

@router.get("/accounts")
def get_accounts(user_id: str = Depends(get_current_user_id)):
    """List all broker accounts for the user."""
    return {"accounts": list_user_broker_accounts(user_id)}

@router.post("/connect")
def start_connection(
    req: CreateDraftRequest,
    user_id: str = Depends(get_current_user_id)
):
    """
    Step 1: Create a Draft Broker Account.
    Returns account_id to be used in subsequent steps.
    """
    try:
        account_id = create_broker_account_draft(user_id, req.broker_id, req.market_type)
        return {"success": True, "account_id": account_id}
    except ValueError as e:
        error_msg = str(e)
        status_code = 403 if "limit reached" in error_msg.lower() else 400
        raise HTTPException(status_code=status_code, detail=error_msg)

@router.post("/{account_id}/credentials")
def submit_credentials(
    account_id: str,
    req: CredentialsRequest,
    user_id: str = Depends(get_current_user_id)
):
    """
    Step 2: Submit API keys/credentials securely.
    """
    # Verify ownership handled in core service
    success = submit_broker_credentials(user_id, account_id, req.credentials)
    if not success:
        raise HTTPException(404, "Broker account not found")
        
    return {"success": True, "message": "Credentials stored securely"}


@router.post("/{account_id}/test")
async def test_broker_connection(
    account_id: str,
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    """
    Test broker connection without saving.
    For MT4/MT5: proxies to bot-backend test endpoint.
    For IBKR bridge mode: validates host/port/client_id connectivity.
    For others: runs existing validation logic.
    """
    from app.core.broker_service import get_broker_account, get_decrypted_credentials
    from app.api.proxy_utils import proxy_request
    
    # Get account details
    account = get_broker_account(user_id, account_id)
    if not account:
        raise HTTPException(404, "Broker account not found")
    
    broker_id = account.get("broker_id")
    
    # For MT4/MT5, proxy to bot-backend with credentials
    if broker_id in ("mt4", "mt5"):
        # Get decrypted credentials
        creds = get_decrypted_credentials(user_id, account_id)
        if not creds:
            raise HTTPException(404, "Bridge configuration not found")
        
        # Proxy to bot-backend test endpoint
        return await proxy_request(
            request,
            "/api/v1/brokers/test-connection",
            json_body={
                "broker_id": broker_id,
                "environment": account.get("environment", "live"),
                "credentials": {
                    "bridge_url": creds.get("bridge_url"),
                    "bridge_token": creds.get("bridge_token")
                }
            },
            method="POST"
        )
    
    # For IBKR, proxy to bot-backend with bridge params
    elif broker_id == "ibkr":
        # Get credentials (bridge config)
        creds = get_decrypted_credentials(user_id, account_id)
        if not creds:
            raise HTTPException(404, "Bridge configuration not found")
        
        # Proxy to bot-backend test endpoint
        return await proxy_request(
            request,
            "/api/v1/brokers/test-connection",
            json_body={
                "broker_id": "ibkr",
                "credentials": creds,
                "environment": account.get("environment", "paper")
            },
            method="POST"
        )
    
    # For other brokers, use existing validation
    result = validate_broker_account(user_id, account_id)
    if "error" in result and result["error"] == "Account not found":
        raise HTTPException(404, result["error"])
        
    return result

@router.post("/{account_id}/validate")
def validate_connection(
    account_id: str,
    user_id: str = Depends(get_current_user_id)
):
    """
    Step 3: Trigger validation pipeline (connectivity, permissions check).
    """
    result = validate_broker_account(user_id, account_id)
    if "error" in result and result["error"]  == "Account not found":
        raise HTTPException(404, "Broker account not found")
        
    return result

@router.post("/{account_id}/disconnect")
def disconnect_broker(
    account_id: str,
    user_id: str = Depends(get_current_user_id)
):
    """
    Disconnect a broker account.
    """
    success = disconnect_broker_account(user_id, account_id)
    if not success:
         raise HTTPException(404, "Broker account not found")
    return {"success": True, "message": "Broker disconnected"}

@router.get("/{account_id}")
def get_account_detail(
    account_id: str,
    user_id: str = Depends(get_current_user_id)
):
    """Get details for a single broker account."""
    account = get_broker_account(user_id, account_id)
    if not account:
        raise HTTPException(404, "Broker account not found")
    return {"account": account}

@router.get("/{account_id}/summary")
def get_summary(
    account_id: str,
    user_id: str = Depends(get_current_user_id)
):
    """
    Get live capital summary (Wallet, Equity, PnL) from the exchange.
    Real-time call.
    """
    try:
        summary = get_broker_summary(user_id, account_id)
        if summary is None:
            raise HTTPException(404, "Broker account not found")
        if "error" in summary and "Account not found" in summary["error"]:
            raise HTTPException(404, summary["error"])
        return summary
    except ValueError as e:
        raise HTTPException(404, str(e))

@router.delete("/{account_id}")
def delete_broker(
    account_id: str,
    user_id: str = Depends(get_current_user_id)
):
    """
    Permanently delete a broker account and its keys.
    """
    success = delete_broker_account_permanently(user_id, account_id)
    if not success:
         raise HTTPException(404, "Broker account not found")
    return {"success": True, "message": "Broker account deleted permanently"}

@router.post("/test-connection")
async def test_connection_endpoint(
    request: Request,
    req: TestConnectionRequest,
    user_id: str = Depends(get_current_user_id)
):
    """
    Test credentials against a broker without saving them.
    Useful for 'Test Connection' button in UI before submitting.
    """
    # IBKR uses the new Link Flow, so 'Test Connection' with credentials is not supported.
    if req.broker_id == "ibkr":
        return {"success": False, "error": "Please use the 'Connect IBKR' button to link your account securely."}

    # For others, use internal simple validation
    # Note: sensitive error messages might be returned, frontend should handle display gracefully
    result = _test_broker_connection(req.broker_id, req.credentials, req.environment)
    return result

# -----------------------------------------------
# IBKR Link Flow
# -----------------------------------------------

class IBKRLinkCallbackRequest(BaseModel):
    ibkr_connection_id: str
    accounts: List[str]
    environment: str

@router.post("/ibkr/link/start")
async def start_ibkr_link(
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    """
    Initiate IBKR linking process.
    Proxies to bot-backend to generate the IBeam/Gateway link URL.
    """
    import httpx
    from app.api.proxy_utils import BOT_BACKEND_BASE_URL
    from app.core.broker_service import link_ibkr_account
    from fastapi import Response

    target_url = f"{BOT_BACKEND_BASE_URL}/api/v1/ibkr/connect/start"
    
    # Forward headers
    headers = {}
    if request.headers.get("authorization"):
        headers["Authorization"] = request.headers.get("authorization")

    # Read body if present (e.g. for gateway_url override)
    try:
        req_body = await request.json()
    except:
        req_body = {}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(target_url, headers=headers, json=req_body)
        except Exception as e:
             raise HTTPException(502, f"Failed to connect to bot backend: {str(e)}")

        if resp.status_code != 200:
            return Response(content=resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"))
        
        data = resp.json()
        
        # Check for immediate success (Auto-Discovery)
        if data.get("status") == "connected" and data.get("accounts"):
            # Persist accounts immediately
            link_ibkr_account(user_id, {
                "accounts": data["accounts"],
                "environment": "live" if any(not a.startswith("D") for a in data["accounts"]) else "paper" # Heuristic fallback
            })
            
        return data

@router.post("/ibkr/link/callback")
def ibkr_link_callback(
    req: IBKRLinkCallbackRequest,
    user_id: str = Depends(get_current_user_id)
):
    """
    Finalize IBKR linking.
    Receives connection metadata and accounts list.
    Stores them as broker_accounts.
    """
    from app.core.broker_service import link_ibkr_account
    
    try:
        data = req.dict()
        link_ibkr_account(user_id, data)
        return {"success": True}
    except ValueError as e:
        raise HTTPException(400, str(e))
