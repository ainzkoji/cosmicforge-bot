from fastapi import APIRouter, Depends, HTTPException, Body
from typing import List, Dict, Any

from app.api.auth import get_current_user_id
from app.core.broker_service import (
    get_broker_catalog,
    create_broker_account_draft,
    submit_broker_credentials,
    validate_broker_account,
    list_user_broker_accounts,
    get_broker_account,
    disconnect_broker_account
)

router = APIRouter(tags=["Brokers"])

# -----------------------------------------------
# Schemas (Inline for simplicity, can move to app.schemas)
# -----------------------------------------------
from pydantic import BaseModel

class CreateDraftRequest(BaseModel):
    broker_id: str
    market_type: str

class CredentialsRequest(BaseModel):
    credentials: Dict[str, Any]

class UpdateLabelRequest(BaseModel):
    label: str

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
    account_id = create_broker_account_draft(user_id, req.broker_id, req.market_type)
    return {"success": True, "account_id": account_id}

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
