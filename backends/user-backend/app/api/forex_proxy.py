"""
Forex Proxy API

Proxies forex-related requests to bot-backend with credentials injection.
"""
from fastapi import APIRouter, Request, HTTPException, Depends, Query
from typing import Optional
import logging

from app.api.auth import get_current_active_user
from app.core.broker_service import get_decrypted_credentials
from app.api.proxy_utils import proxy_request

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/instruments")
async def get_forex_instruments(
    request: Request,
    broker_id: str = Query(default=None), # Optional, can be derived
    broker_account_id: Optional[str] = Query(default=None),
    environment: str = Query(default="practice"),
    user: dict = Depends(get_current_active_user)
):
    """
    Get forex instruments list from bot-backend.
    
    If broker_account_id is provided, fetches live instruments from broker API.
    Otherwise returns fallback list from FOREX_SYMBOLS.
    """
    # Build query params for bot-backend
    query_params = {
        "environment": environment
    }
    
    # If broker_account_id provided, validate ownership and inject credentials
    credentials_map = None
    target_broker_id = broker_id or "oanda" # Fallback default
    
    if broker_account_id:
        # Verify user owns this broker account
        try:
            credentials = await get_decrypted_credentials(user["id"], broker_account_id)
            if not credentials:
                logger.warning(f"No credentials found for user {user['id']} broker_account_id {broker_account_id}")
                # Continue without credentials
            else:
                credentials_map = {broker_account_id: credentials}
                query_params["broker_account_id"] = broker_account_id
                
                # Derive broker_id from credentials metadata if not strictly provided
                if credentials.get("broker_id"):
                    target_broker_id = credentials["broker_id"]
                
                logger.info(f"Injecting instruments credentials for {target_broker_id} account {broker_account_id}")
        except Exception as e:
            logger.error(f"Failed to get credentials: {e}")
            # Continue without credentials
            
    query_params["broker_id"] = target_broker_id
    
    # Proxy to bot-backend
    return await proxy_request(
        request,
        "/api/v1/forex/instruments",
        params=query_params,
        json_body={"broker_credentials_map": credentials_map} if credentials_map else None,
        method="GET"
    )
