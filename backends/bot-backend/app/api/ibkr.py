from fastapi import APIRouter, Depends, HTTPException, Body
from typing import Dict, Any, List, Optional
import uuid
import time
import logging
import requests
from pydantic import BaseModel

from app.core.auth import get_current_user_id
from app.core.config import settings
from app.exchange.ibkr.client import IBKRClient
from app.exchange.ibkr.session import IBKRSession, IBKRSessionManager
from shared_lib.core.security.url_guard import (
    OutboundPolicy,
    UnsafeDestinationError,
    validate_outbound_host_port,
    validate_outbound_url,
)

# Every route here requires an authenticated user (declared on the router so
# a route added later cannot be public by omission).
router = APIRouter(
    prefix="/api/v1/ibkr",
    tags=["IBKR Connect"],
    dependencies=[Depends(get_current_user_id)],
)
logger = logging.getLogger(__name__)

# --- Models ---

class ConnectStartRequest(BaseModel):
    # Optional gateway_url for legacy HTTP mode, though we prioritize TWS now
    gateway_url: Optional[str] = None 
    host: str = "127.0.0.1"
    port: int = 7496
    client_id: int = 1
    bridge_mode: str = "tws" # 'tws' or 'gateway' (informational mostly, affects default ports)
    verify_ssl: bool = False

class ConnectCallbackRequest(BaseModel):
    connection_id: str

class ConnectResponse(BaseModel):
    connect_url: str
    connection_id: str
    accounts: Optional[List[str]] = None
    status: Optional[str] = "pending"
    message: Optional[str] = None
    environment: Optional[str] = None

class CallbackResponse(BaseModel):
    connection_id: str
    connected: bool
    accounts: List[str]
    environment: str
    message: Optional[str] = None

# --- Connection Manager ---

class ConnectionManager:
    """
    Simple in-memory store for IBKR connection flows.
    """
    def __init__(self):
        self._connections: Dict[str, Dict[str, Any]] = {}
        
    def get_connection(self, connection_id: str, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Return a connection record; with ``user_id`` only the owner's own record."""
        data = self._connections.get(connection_id)
        if data is None:
            return None
        if user_id is not None and str(data.get("user_id")) != str(user_id):
            return None
        return data

    # We get session directly via IBKRSessionManager now    
    # But we still store metadata about the "Frontend Connection Attempt" here
    def record_connection(self, connection_id: str, data: Dict[str, Any]):
        self._connections[connection_id] = data

# Note: The global manager for connections
manager = ConnectionManager()

# --- Endpoints ---

@router.post("/connect/start", response_model=ConnectResponse)
async def connect_start(req: ConnectStartRequest, user_id: str = Depends(get_current_user_id)):
    """
    Start IBKR connection flow (TWS/Gateway Mode).
    Connects to the specified Host/Port using IB Insync.
    """
    if settings.production and req.port in {7497, 4002}:
        raise HTTPException(400, "Production requires a LIVE IBKR endpoint")

    # host/port (and the legacy gateway_url) are caller-supplied: refuse any
    # destination the outbound policy does not allow BEFORE connecting, then
    # connect to the IP that was validated (no second DNS lookup).
    policy = OutboundPolicy.from_settings(settings)
    try:
        destination = validate_outbound_host_port(req.host, req.port, policy=policy)
        if req.gateway_url:
            validate_outbound_url(req.gateway_url, policy=policy)
    except UnsafeDestinationError as exc:
        logger.warning(f"IBKR connection refused: destination not allowed ({exc.reason})")
        raise HTTPException(status_code=400, detail=f"Destination not allowed ({exc.reason})")

    connection_id = str(uuid.uuid4())
    logger.info(f"Starting IBKR connection to {req.host}:{req.port} (Client ID: {req.client_id})")

    # Use the shared IBKRSessionManager to get/create the TWS session
    session_manager = IBKRSessionManager()
    
    try:
        # Attempt connection
        session = await session_manager.get_session(
            connection_id=connection_id,
            host=destination.connect_host,
            port=destination.port,
            # We need to update get_session signature in session.py if we want client_id support
            # For now, it hardcodes client_id=1, but we should fix that.
        )
        # Note: We need to update session.py to accept client_id!
        # Assuming we will fix that next.
        
        if session.is_connected():
            # Auto-discover accounts
            client = IBKRClient(session)
            accounts = client.get_portfolio_accounts()
            
            # Determine environment based on port heuristic
            # Live: 7496, 4001
            # Paper: 7497, 4002
            is_paper = req.port in [7497, 4002] or (accounts and any(a.startswith("D") for a in accounts))
            if settings.production and is_paper:
                raise ValueError("PRODUCTION_REQUIRES_LIVE_IBKR_ACCOUNT")
            environment = "paper" if is_paper else "live"
            
            manager.record_connection(connection_id, {
                "user_id": str(user_id),
                "host": req.host,
                "port": req.port,
                "status": "connected",
                "accounts": accounts,
                "environment": environment
            })

            return ConnectResponse(
                connect_url="",
                connection_id=connection_id,
                accounts=accounts,
                status="connected",
                environment=environment,
                message=f"Connected to {req.bridge_mode.upper()} at {req.host}:{req.port}"
            )
    except Exception as e:
        logger.error(f"IBKR Connection Failed: {e}")
        return ConnectResponse(
            connect_url="",
            connection_id=connection_id,
            status="unreachable",
            message=f"Connection failed: {str(e)}. Ensure TWS is running and API is enabled."
        )

    return ConnectResponse(
        connect_url="",
        connection_id=connection_id,
        status="unreachable",
        message="Unknown connection error"
    )

@router.post("/connect/callback", response_model=CallbackResponse)
def connect_callback(req: ConnectCallbackRequest, user_id: str = Depends(get_current_user_id)):
    """
    Callback not strictly needed for TWS as connection is direct,
    but kept for compatibility if frontend calls it.
    """
    # Owner-scoped: another user's connection id is indistinguishable from an
    # unknown one.
    conn_data = manager.get_connection(req.connection_id, user_id=user_id)
    if not conn_data:
        raise HTTPException(status_code=404, detail="Connection ID not found")
        
    return CallbackResponse(
        connection_id=req.connection_id,
        connected=(conn_data.get("status") == "connected"),
        accounts=conn_data.get("accounts", []),
        environment=conn_data.get("environment", "live"),
        message=conn_data.get("message", "Status check complete")
    )
