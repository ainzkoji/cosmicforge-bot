"""
Connector Authentication API
Handles magic link token exchange for MT connector setup.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.core import mt_pairing_service

router = APIRouter(prefix="/api/v1/mt/connector")

class ClaimTokenRequest(BaseModel):
    token: str

class ClaimTokenResponse(BaseModel):
    pairing_code: str
    platform: str  # "mt4" or "mt5"
    environment: str  # "live" or "demo"
    expires_at: str

@router.post("/claim", response_model=ClaimTokenResponse)
def claim_connector_token(req: ClaimTokenRequest):
    """
    Exchange a connector_link_token for the pairing code.
    This allows the connector to authenticate and get its pairing code
    without requiring the user to manually type it.
    """
    session = mt_pairing_service.get_session_by_connector_token(req.token)
    
    if not session:
        raise HTTPException(status_code=404, detail="Invalid or expired setup token")
    
    if session["status"] != "pending":
        raise HTTPException(status_code=400, detail="This setup link has already been used")
    
    return ClaimTokenResponse(
        pairing_code=session["pairing_code"],
        platform=session["broker_id"],
        environment=session["environment"],
        expires_at=session["expires_at"]
    )
