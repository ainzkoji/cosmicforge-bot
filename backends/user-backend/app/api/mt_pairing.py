"""
MT Bridge Pairing API Endpoints
Handles pairing code generation and completion flow.
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
import logging

from app.api.auth import get_current_user_id
from app.core import mt_pairing_service

router = APIRouter(prefix="/api/v1/mt", tags=["MT Bridge Pairing"])
logger = logging.getLogger(__name__)

# ============================================================================
# Request/Response Models
# ============================================================================

# ============================================================================
# Request/Response Models
# ============================================================================

class PairingInstructions(BaseModel):
    download_url: str
    steps: list[str]

class CreatePairingSessionRequest(BaseModel):
    broker_id: str  # "mt4" or "mt5"
    environment: str = "live" # "live" or "demo"

class CreatePairingSessionResponse(BaseModel):
    session_id: str
    expires_at: str
    download_url: str = "/api/v1/mt/connector/download"
    status: str

class ClaimPairingCodeRequest(BaseModel):
    session_id: str
    device_secret: str

class ClaimPairingCodeResponse(BaseModel):
    pairing_code: str
    broker_id: str
    environment: str

class FinishPairingRequest(BaseModel):
    session_id: str

class FinishPairingResponse(BaseModel):
    account_id: str

class AccountDetails(BaseModel):
    login: Optional[str]
    server: Optional[str]
    currency: Optional[str]
    platform: Optional[str]

class PairingSessionStatusResponse(BaseModel):
    status: str  # "pending", "paired", "expired"
    broker_id: str
    environment: str
    pairing_code: Optional[str] = None
    expires_at: str
    account: Optional[AccountDetails] = None

class AccountInfo(BaseModel):
    login: str
    server: str
    currency: str = "USD"
    type: str = "Demo"
    platform: str

class CompletePairingRequest(BaseModel):
    pairing_code: str
    bridge_url: str
    bridge_token: str
    tls_mode: str  # "strict" or "insecure"
    account: AccountInfo

class CompletePairingResponse(BaseModel):
    ok: bool
    user_visible_message: str
    # Keeping account_id for internal tracking even if not in minimal spec
    account_id: str

# ============================================================================
# Endpoints
# ============================================================================

@router.post("/pairing-sessions", response_model=CreatePairingSessionResponse)
def create_pairing_session(
    req: CreatePairingSessionRequest,
    user_id: str = Depends(get_current_user_id)
):
    """
    Create a new MT pairing session.
    
    Frontend uses this to generate a pairing code for the user.
    User enters this code in the Windows Connector.
    """
    if req.broker_id not in ("mt4", "mt5"):
        raise HTTPException(400, "broker_id must be 'mt4' or 'mt5'")
    
    if req.environment not in ("live", "demo"):
        # We can default to live or error. Strict spec says CHECK(environment IN ('live','demo'))
        # But for API, let's be strict.
        raise HTTPException(400, "environment must be 'live' or 'demo'")
    
    try:
        session = mt_pairing_service.create_pairing_session(user_id, req.broker_id, req.environment)
        # Filter out sensitive fields like pairing_code
        return CreatePairingSessionResponse(
            session_id=session["session_id"],
            expires_at=session["expires_at"],
            status=session["status"]
        )
    except ValueError as e:
        # Rate limit exceeded
        raise HTTPException(429, str(e))

@router.post("/connector/claim", response_model=ClaimPairingCodeResponse)
def claim_pairing_code(req: ClaimPairingCodeRequest):
    """
    Claim valid pairing code using a session ID.
    Called by the Windows Connector.
    """
    try:
        result = mt_pairing_service.claim_pairing_session(req.session_id, req.device_secret)
        return ClaimPairingCodeResponse(**result)
    except ValueError as e:
        raise HTTPException(400, str(e))

@router.post("/connect/finish", response_model=FinishPairingResponse)
def finish_pairing(
    req: FinishPairingRequest,
    user_id: str = Depends(get_current_user_id)
):
    """
    Finalize pairing and create broker account.
    Called by Frontend after 'Verify'.
    """
    try:
        account_id = mt_pairing_service.finish_pairing(req.session_id, user_id)
        return FinishPairingResponse(account_id=account_id)
    except ValueError as e:
        raise HTTPException(400, str(e))

@router.get("/pairing-sessions/{pairing_code}", response_model=PairingSessionStatusResponse)
def get_pairing_status(
    pairing_code: str,
    user_id: str = Depends(get_current_user_id)
):
    """
    Poll pairing session status.
    
    Frontend calls this every 2 seconds to check if Windows Connector has paired.
    """
    session = mt_pairing_service.get_pairing_session(pairing_code, user_id=user_id)
    
    if not session:
        raise HTTPException(404, "Pairing session not found")
    
    # Service returns dictionary matching the model structure we want now
    return PairingSessionStatusResponse(**session)

@router.get("/connect/status", response_model=PairingSessionStatusResponse)
def get_pairing_status_by_id(
    session_id: str,
    user_id: str = Depends(get_current_user_id)
):
    """
    Poll pairing session status by Session ID.
    Preferred method for new UI.
    """
    session = mt_pairing_service.get_session_by_id(session_id, user_id)
    
    if not session:
        raise HTTPException(404, "Pairing session not found")
    
    return PairingSessionStatusResponse(**session)

@router.post("/pair", response_model=CompletePairingResponse)
def complete_pairing(req: CompletePairingRequest):
    """
    Complete pairing (called by Windows Connector).
    
    NO AUTHENTICATION REQUIRED - pairing_code is the credential.
    
    This endpoint:
    1. Validates pairing code
    2. Creates broker account for the user
    3. Stores bridge credentials securely
    4. Marks session as paired
    """
    # Validate inputs
    if req.account.platform not in ("mt4", "mt5"):
        raise HTTPException(400, "account.platform must be 'mt4' or 'mt5'")
    
    if req.tls_mode not in ("strict", "insecure"):
        raise HTTPException(400, "tls_mode must be 'strict' or 'insecure'")
    
    if not req.bridge_url.startswith("https://"):
        raise HTTPException(400, "bridge_url must use HTTPS")
    
    if len(req.bridge_token) < 24:
        raise HTTPException(400, "bridge_token must be at least 24 characters")
    
    try:
        account_id = mt_pairing_service.complete_pairing(
            pairing_code=req.pairing_code,
            bridge_url=req.bridge_url,
            bridge_token=req.bridge_token,
            tls_mode=req.tls_mode,
            mt_platform=req.account.platform,
            account_login=req.account.login,
            server=req.account.server,
            account_currency=req.account.currency,
            account_type=req.account.type
        )
        
        logger.info(f"Pairing completed: code={req.pairing_code}, account={account_id}, platform={req.account.platform}")
        
        return CompletePairingResponse(
            ok=True,
            user_visible_message=f"{req.account.platform.upper()} account connected successfully",
            account_id=account_id
        )
    
    except ValueError as e:
        # Invalid/expired/used pairing code
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.exception("Pairing failed")
        raise HTTPException(500, f"Pairing failed: {str(e)}")
