"""
MetaTrader Bridge - FastAPI Server
Provides REST API interface to MT4/MT5 Expert Advisor via ZeroMQ

⚠️ SINGLE-USER ARCHITECTURE:
This bridge serves EXACTLY ONE MT account per instance.
Deploy one bridge per user with unique ports and tokens.
"""

from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import zmq
import zmq.asyncio  # P0.1: Async ZMQ support
import json
import logging
import asyncio  # P0.1: For async lock
import os  # P0.2: For SSL and env var checks
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(
    title="MetaTrader Bridge API",
    description="REST API for MT4/MT5 trading via Expert Advisor",
    version="1.0.0"
)

# P0.3: CORS middleware - restricted to bot backend only
ALLOWED_ORIGIN = os.getenv("ALLOWED_ORIGIN", "http://localhost:8000")  # Bot backend URL
app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN] if ALLOWED_ORIGIN != "*" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBearer()

# Configuration (loaded from config.json)
CONFIG = {
    "zmq_host": "localhost",
    "zmq_port": "5555",
    "api_tokens": [],  # Will be loaded from config file
    "request_timeout": 5000  # milliseconds
}

# ZeroMQ context and socket (global)
zmq_context = None
zmq_socket = None

# P0.1: Global asyncio lock for ZMQ request serialization
# CRITICAL: ZMQ REQ sockets are NOT thread-safe. This lock ensures
# only one request is sent at a time, preventing response mis-pairing.
zmq_lock = asyncio.Lock()

#============================================================================
# Pydantic Models
#============================================================================

class OrderRequest(BaseModel):
    symbol: str
    side: str  # "buy" or "sell"
    type: str  # "market", "limit", "stop"
    quantity: float
    price: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    client_order_id: Optional[str] = None

class CancelOrderRequest(BaseModel):
    order_id: str

class PricesRequest(BaseModel):
    symbols: List[str]

class KlinesRequest(BaseModel):
    symbol: str
    interval: str  # "M1", "M5", "H1", "D1", etc.
    limit: Optional[int] = 100

#============================================================================
# ZeroMQ Communication
#============================================================================

def init_zmq():
    """Initialize ZeroMQ connection to EA"""
    global zmq_context, zmq_socket
    
    # P0.1: Use async-aware ZMQ context
    zmq_context = zmq.asyncio.Context()
    zmq_socket = zmq_context.socket(zmq.REQ)
    
    endpoint = f"tcp://{CONFIG['zmq_host']}:{CONFIG['zmq_port']}"
    zmq_socket.connect(endpoint)
    zmq_socket.setsockopt(zmq.RCVTIMEO, CONFIG['request_timeout'])
    zmq_socket.setsockopt(zmq.SNDTIMEO, CONFIG['request_timeout'])
    
    logger.info(f"Connected to MT EA at {endpoint}")

async def send_zmq_request(action: str, params: dict = None) -> dict:
    """
    Send request to EA via ZeroMQ and get response.
    
    P0.1 CRITICAL FIX:
    Uses asyncio.Lock to serialize all ZMQ requests. This prevents concurrent
    HTTP requests from interleaving send/recv on the ZMQ REQ socket, which
    would cause response mis-pairing and data corruption.
    
    Args:
        action: EA handler action (e.g., "health", "order")
        params: Optional parameters dictionary
    
    Returns:
        dict: EA response as parsed JSON
    
    Raises:
        HTTPException: On ZMQ timeout, invalid JSON, or EA errors
    """
    global zmq_socket
    
    if zmq_socket is None:
        init_zmq()
    
    # Build request
    request = {"action": action}
    if params:
        request.update(params)
    
    # P0.1: Acquire lock to serialize ZMQ requests
    async with zmq_lock:
        try:
            # Send request
            message = json.dumps(request)
            logger.debug(f"Sending ZMQ request: {message[:200]}")
            await zmq_socket.send_string(message)
            
            # Receive response
            response_str = await zmq_socket.recv_string()
            logger.debug(f"Received ZMQ response: {response_str[:200]}")
            
            response = json.loads(response_str)
            
            # Check for error in response
            if "error" in response:
                raise HTTPException(status_code=400, detail=response["error"])
            
            return response
        
        except zmq.error.Again:
            logger.error("ZMQ timeout - EA not responding")
            raise HTTPException(status_code=504, detail="MT terminal not responding")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from EA: {e}")
            raise HTTPException(status_code=500, detail="Invalid response from MT terminal")
        except Exception as e:
            logger.error(f"ZMQ communication error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

#============================================================================
# Authentication
#============================================================================

def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)) -> str:
    """Verify API token"""
    token = credentials.credentials
    
    if token not in CONFIG["api_tokens"]:
        raise HTTPException(status_code=401, detail="Invalid API token")
    
    return token

#============================================================================
# API Endpoints
#============================================================================

@app.get("/")
async def root(token: str = Depends(verify_token)):  # P0.5: Auth required
    """Root endpoint - requires authentication"""
    return {
        "name": "MetaTrader Bridge API",
        "version": "1.0.0",
        "status": "running",
        "mode": "single-user"
    }

@app.get("/v1/health")
async def health(token: str = Depends(verify_token)):
    """Health check - returns platform and account info"""
    response = await send_zmq_request("health")  # P0.1: await async call
    return response

@app.get("/v1/instruments")
async def get_instruments(token: str = Depends(verify_token)):
    """Get available trading instruments"""
    response = await send_zmq_request("instruments")  # P0.1: await async call
    return response

@app.post("/v1/prices")
async def get_prices(request: PricesRequest, token: str = Depends(verify_token)):
    """Get current prices for symbols"""
    response = await send_zmq_request("prices", {"symbols": json.dumps(request.symbols)})  # P0.1
    return response

@app.post("/v1/klines")
async def get_klines(request: KlinesRequest, token: str = Depends(verify_token)):
    """Get historical candles"""
    params = {
        "symbol": request.symbol,
        "interval": request.interval,
        "limit": str(request.limit)
    }
    response = await send_zmq_request("klines", params)  # P0.1: await async call
    return response

@app.post("/v1/order")
async def place_order(order: OrderRequest, token: str = Depends(verify_token)):
    """Place a new order"""
    params = {
        "symbol": order.symbol,
        "side": order.side,
        "type": order.type,
        "quantity": str(order.quantity),
        "price": str(order.price) if order.price else "0",
        "sl": str(order.sl) if order.sl else "0",
        "tp": str(order.tp) if order.tp else "0",
        "client_order_id": order.client_order_id or ""
    }
    response = await send_zmq_request("order", params)  # P0.1: await async call
    return response

@app.post("/v1/order/cancel")
async def cancel_order(request: CancelOrderRequest, token: str = Depends(verify_token)):
    """Cancel a pending order"""
    params = {"order_id": request.order_id}
    response = await send_zmq_request("cancel_order", params)  # P0.1: await async call
    return response

@app.get("/v1/order/{order_id}")
async def get_order(order_id: str, token: str = Depends(verify_token)):
    """Get order status"""
    params = {"order_id": order_id}
    response = await send_zmq_request("get_order", params)  # P0.1: await async call
    return response

@app.get("/v1/positions")
async def get_positions(token: str = Depends(verify_token)):
    """Get all open positions"""
    response = await send_zmq_request("positions")  # P0.1: await async call
    return response

@app.get("/v1/balance")
async def get_balance(token: str = Depends(verify_token)):
    """Get account balance and equity"""
    response = await send_zmq_request("balance")  # P0.1: await async call
    return response

#============================================================================
# Startup/Shutdown
#============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize on startup with security validations"""
    logger.info("Starting MetaTrader Bridge Server...")
    
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            loaded_config = json.load(f)
            CONFIG.update(loaded_config)
        logger.info(f"Configuration loaded from {config_path}")
    else:
        logger.warning(f"Config file not found: {config_path}")
        logger.warning("Using default configuration")
    
    # P0.4: CRITICAL - Reject default/placeholder tokens
    if not CONFIG['api_tokens']:
        logger.error("FATAL: No API tokens configured")
        raise ValueError("At least one API token required in config.json")
    
    for token in CONFIG['api_tokens']:
        if "CHANGE_ME" in token or "PLACEHOLDER" in token or len(token) < 24:
            logger.error(f"FATAL: Insecure or default token detected: {token[:10]}...")
            raise ValueError(
                "Default or weak token detected. Token must be at least 24 characters. "
                "python scripts/generate_token.py"
            )
    
    # P0.2: CRITICAL - Enforce SSL/TLS for production
    ssl_key = os.path.join(os.path.dirname(__file__), "key.pem")
    ssl_cert = os.path.join(os.path.dirname(__file__), "cert.pem")
    require_ssl = os.getenv("REQUIRE_SSL", "true").lower() == "true"
    
    if require_ssl and (not os.path.exists(ssl_key) or not os.path.exists(ssl_cert)):
        logger.error("FATAL: SSL certificates (key.pem, cert.pem) not found")
        logger.error("Generate with: openssl req -new -x509 -keyout key.pem -out cert.pem -days 365 -nodes")
        logger.error("Or set REQUIRE_SSL=false (NOT RECOMMENDED for production)")
        raise ValueError("SSL certificates required for production deployment")
    
    # WARN if multiple tokens (all access same account)
    if len(CONFIG['api_tokens']) > 1:
        logger.warning("="*60)
        logger.warning("WARNING: Multiple API tokens configured")
        logger.warning("This bridge serves a SINGLE MT account.")
        logger.warning("ALL tokens access the SAME account (no isolation).")
        logger.warning("For multi-user: deploy one bridge per user.")
        logger.warning("="*60)
    
    # Initialize ZMQ
    init_zmq()
    
    logger.info("Bridge server started successfully")
    logger.info(f"API Tokens: {len(CONFIG['api_tokens'])} configured")
    logger.info("Bridge Mode: SINGLE-USER (one account per instance)")
    logger.info(f"SSL Enforcement: {'ENABLED' if require_ssl else 'DISABLED (UNSAFE)'}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global zmq_socket, zmq_context
    
    logger.info("Shutting down MetaTrader Bridge Server...")
    
    if zmq_socket:
        zmq_socket.close()
    if zmq_context:
        zmq_context.term()
    
    logger.info("Bridge server stopped")

#============================================================================
# Main Entry Point
#============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # P0.2: SSL configuration (required unless explicitly disabled)
    ssl_key = "key.pem"
    ssl_cert = "cert.pem"
    require_ssl = os.getenv("REQUIRE_SSL", "true").lower() == "true"
    
    if require_ssl:
        if not os.path.exists(ssl_key) or not os.path.exists(ssl_cert):
            print("ERROR: SSL certificates required but not found")
            print("Generate with: openssl req -new -x509 -keyout key.pem -out cert.pem -days 365 -nodes")
            exit(1)
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("HTTP_PORT", "8443")),
        ssl_keyfile=ssl_key if require_ssl and os.path.exists(ssl_key) else None,
        ssl_certfile=ssl_cert if require_ssl and os.path.exists(ssl_cert) else None,
        log_level="info",
        reload=False
    )
