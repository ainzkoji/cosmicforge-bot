from fastapi import APIRouter, HTTPException, Body, Depends, Request
from pydantic import BaseModel
from typing import Dict, Any, Optional, List, Literal
import logging
import uuid
from datetime import datetime
from app.exchange.ibkr.adapter import IBKRAdapter
from app.exchange.ibkr.errors import IBKRConnectionError, IBKRAuthError

router = APIRouter()
logger = logging.getLogger(__name__)

# --- Models ---

class BrokerAuthField(BaseModel):
    name: str
    label: str
    type: str # text, password, select
    required: bool = True
    options: Optional[List[Any]] = None
    default: Optional[Any] = None

class Broker(BaseModel):
    id: str
    name: str
    market_types: List[str] # crypto, forex
    logo: Optional[str] = None
    auth_fields: List[BrokerAuthField]
    features: List[str] = []
    required_permissions: List[str] = []
    is_available: bool = True
    signup_url: Optional[str] = None

class BrokerCatalogResponse(BaseModel):
    brokers: List[Broker]

class BrokerAccount(BaseModel):
    id: str
    broker_id: str
    market_type: str
    status: Literal['draft', 'validating', 'connected', 'disconnected', 'disabled', 'restricted', 'error']
    label: str
    masked_key: Optional[str] = None
    environment: Literal['live', 'paper', 'testnet', 'demo'] = 'paper'
    capabilities: List[str] = []
    created_at: str
    last_validated_at: Optional[str] = None
    last_error_message: Optional[str] = None

class BrokerAccountsResponse(BaseModel):
    accounts: List[BrokerAccount]

class ConnectRequest(BaseModel):
    broker_id: str
    market_type: str
    label: Optional[str] = None

class ConnectResponse(BaseModel):
    account_id: str
    status: str

class CredentialsRequest(BaseModel):
    credentials: Dict[str, Any]

class ValidateResponse(BaseModel):
    success: bool
    error: Optional[str] = None

class TestConnectionRequest(BaseModel):
    broker_id: str
    environment: Literal["paper", "live"]
    credentials: Dict[str, Any]

class TestConnectionResponse(BaseModel):
    ok: bool
    error: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

# --- In-Memory Store (Mock for MVP) ---
# In a real app, this would be a DB table
_ACCOUNTS_DB: Dict[str, BrokerAccount] = {}
_CREDENTIALS_DB: Dict[str, Dict[str, Any]] = {}

# --- Catalog Definition ---

def _get_catalog_data() -> List[Broker]:
    return [
        Broker(
            id="binance",
            name="Binance Futures",
            market_types=["crypto"],
            logo="https://public.bnbstatic.com/image/cms/content/body/202010/dcb407137f61b04533b66472481d6830.png",
            auth_fields=[
                BrokerAuthField(name="api_key", label="API Key", type="text"),
                BrokerAuthField(name="api_secret", label="API Secret", type="password")
            ],
            features=["perpetual", "hedge_mode"],
            required_permissions=["Enable Futures"],
            signup_url="https://www.binance.com/en/futures"
        ),
        Broker(
            id="bybit",
            name="Bybit",
            market_types=["crypto"],
            logo="https://s3.coinmarketcap.com/static-gravity/image/5cc0b99a825647f69842f1f3e994966d.png",
            auth_fields=[
                BrokerAuthField(name="api_key", label="API Key", type="text"),
                BrokerAuthField(name="api_secret", label="API Secret", type="password")
            ],
            features=["perpetual", "unified_account"],
            required_permissions=["Orders", "Positions"],
            signup_url="https://www.bybit.com/"
        ),
        Broker(
            id="oanda",
            name="OANDA",
            market_types=["forex"],
            logo="https://upload.wikimedia.org/wikipedia/commons/f/fd/Oanda_logo.png",
            auth_fields=[
                BrokerAuthField(name="account_id", label="Account ID", type="text"),
                BrokerAuthField(name="api_token", label="API Token", type="password")
            ],
            features=["spot", "cfd"],
            required_permissions=["Read", "Trade"],
            signup_url="https://www.oanda.com/"
        ),
        Broker(
            id="ibkr",
            name="Interactive Brokers",
            market_types=["forex"], # Can add 'stock' later
            logo="https://upload.wikimedia.org/wikipedia/commons/thumb/8/83/Interactive_Brokers_logo.svg/1200px-Interactive_Brokers_logo.svg.png",
            auth_fields=[
                # No manual input required - uses local gateway or auto-discovery
            ],
            features=["spot", "cfd", "futures", "stocks"],
            required_permissions=["Trading Access"],
            is_available=True,
            signup_url="https://www.interactivebrokers.com/"
        ),
        Broker(
            id="mt4",
            name="MetaTrader 4 (Bridge)",
            market_types=["forex"],
            logo="https://upload.wikimedia.org/wikipedia/commons/thumb/0/09/MetaTrader_4_Logo.svg/240px-MetaTrader_4_Logo.svg.png",
            auth_fields=[
                BrokerAuthField(
                    name="bridge_url", 
                    label="Bridge URL", 
                    type="text",
                    required=True
                ),
                BrokerAuthField(
                    name="api_token", 
                    label="API Token", 
                    type="password",
                    required=True
                )
            ],
            features=["spot", "cfd", "ticket_mode"],
            required_permissions=["MT4 Bridge Running"],
            is_available=True,
            signup_url="https://www.metatrader4.com/"
        ),
        Broker(
            id="mt5",
            name="MetaTrader 5 (Bridge)",
            market_types=["forex"],
            logo="https://upload.wikimedia.org/wikipedia/commons/thumb/9/96/MetaTrader_5_Logo.svg/240px-MetaTrader_5_Logo.svg.png",
            auth_fields=[
                BrokerAuthField(
                    name="bridge_url", 
                    label="Bridge URL", 
                    type="text",
                    required=True
                ),
                BrokerAuthField(
                    name="api_token", 
                    label="API Token", 
                    type="password",
                    required=True
                )
            ],
            features=["spot", "cfd", "ticket_mode", "hedging"],
            required_permissions=["MT5 Bridge Running"],
            is_available=True,
            signup_url="https://www.metatrader5.com/"
        )
    ]

# --- Endpoints ---

@router.get("/catalog", response_model=BrokerCatalogResponse)
async def get_broker_catalog():
    return BrokerCatalogResponse(brokers=_get_catalog_data())

@router.get("/accounts", response_model=BrokerAccountsResponse)
async def get_broker_accounts():
    return BrokerAccountsResponse(accounts=list(_ACCOUNTS_DB.values()))

@router.post("/connect", response_model=ConnectResponse)
async def start_connection(request: ConnectRequest):
    account_id = str(uuid.uuid4())
    account = BrokerAccount(
        id=account_id,
        broker_id=request.broker_id,
        market_type=request.market_type,
        status="draft",
        label=request.label or f"{request.broker_id.upper()} Account",
        created_at=datetime.utcnow().isoformat()
    )
    _ACCOUNTS_DB[account_id] = account
    return ConnectResponse(account_id=account_id, status="draft")

@router.post("/{account_id}/credentials")
async def submit_credentials(account_id: str, request: CredentialsRequest):
    if account_id not in _ACCOUNTS_DB:
        raise HTTPException(status_code=404, detail="Account not found")
    
    # In a real app, encrypt this!
    credentials = request.credentials.copy()
    _CREDENTIALS_DB[account_id] = credentials
    
    # Update account status
    _ACCOUNTS_DB[account_id].status = "validating"
    _ACCOUNTS_DB[account_id].environment = credentials.get("environment", "paper")
    
    # Mask key for display
    if "api_key" in credentials:
        _ACCOUNTS_DB[account_id].masked_key = f"***{credentials['api_key'][-4:]}"
    elif "account_id" in credentials:
        _ACCOUNTS_DB[account_id].masked_key = f"{credentials['account_id']}"
    
    return {"success": True, "status": "validating"}

@router.post("/{account_id}/validate", response_model=ValidateResponse)
async def validate_connection(account_id: str):
    if account_id not in _ACCOUNTS_DB:
        raise HTTPException(status_code=404, detail="Account not found")
    
    account = _ACCOUNTS_DB[account_id]
    credentials = _CREDENTIALS_DB.get(account_id)
    
    if not credentials:
        return ValidateResponse(success=False, error="No credentials provided")
    
    try:
        if account.broker_id == "ibkr":
            # Reuse test logic
            gateway_url = credentials.get("gateway_url", "https://localhost:5000/v1/api")
            verify_ssl = False
            
            # Extract IBKR specific credentials from the flattened map if needed
            # For now adapter only needs gateway_url and account_id
            target_account_id = credentials.get("account_id")
            
            adapter = IBKRAdapter(base_url=gateway_url, account_id=target_account_id, verify_ssl=verify_ssl)
            if not target_account_id:
                adapter._discover_account_id()
            
            # If we get here without error, update account ID if discovered
            if adapter._account_id:
                account.masked_key = adapter._account_id
                # Update credentials with discovered ID so it persists for future usage
                if _CREDENTIALS_DB.get(account_id) is not None:
                    _CREDENTIALS_DB[account_id]["account_id"] = adapter._account_id

        elif account.broker_id == "oanda":
             # TODO: Implement OANDA validation
             pass
        elif account.broker_id in ["binance", "bybit"]:
             # TODO: Implement Crypto validation
             pass
             
        # Success
        account.status = "connected"
        account.last_validated_at = datetime.utcnow().isoformat()
        return ValidateResponse(success=True)
        
    except Exception as e:
        account.status = "error"
        account.last_error_message = str(e)
        logger.error(f"Validation failed for {account_id}: {e}")
        return ValidateResponse(success=False, error=str(e))

@router.post("/{account_id}/disconnect")
async def disconnect_account(account_id: str):
    if account_id in _ACCOUNTS_DB:
        _ACCOUNTS_DB[account_id].status = "disconnected"
    return {"success": True}

@router.delete("/{account_id}")
async def delete_account(account_id: str):
    if account_id in _ACCOUNTS_DB:
        del _ACCOUNTS_DB[account_id]
    if account_id in _CREDENTIALS_DB:
        del _CREDENTIALS_DB[account_id]
    return {"success": True}

@router.post("/test-connection", response_model=TestConnectionResponse)
async def test_broker_connection(request: TestConnectionRequest = Body(...)):
    """
    Test connection to a broker without saving credentials.
    Supports: IBKR (bridge mode), MT4, MT5.
    
    For IBKR Bridge Mode:
    - Attempts TCP connection to TWS/IB Gateway
    - Fetches account summary to validate authentication
    
    For MT4/MT5 Bridge Mode:
    - Attempts HTTPS connection to user-hosted bridge
    - Validates bearer token and fetches account info
    
    Expected credentials for IBKR:
    {
      "bridge_type": "tws"|"ib_gateway",
      "host": "127.0.0.1",
      "port": 7497,
      "client_id": 1,
      "environment": "paper"|"live"
    }
    
    Expected credentials for MT4/MT5:
    {
      "bridge_url": "https://vps.example.com:8443",
      "bridge_token": "your-api-token"
    }
    """
    # Route based on broker_id
    if request.broker_id == "ibkr":
        return await _test_ibkr_connection(request)
    elif request.broker_id in ("mt4", "mt5"):
        return await _test_mt_bridge_connection(request)
    else:
        # Mock success for other brokers (not yet implemented)
        return TestConnectionResponse(
            ok=True, 
            details={"message": f"Mock success - {request.broker_id} validation not yet implemented"}
        )


async def _test_ibkr_connection(request: TestConnectionRequest) -> TestConnectionResponse:
    """Test IBKR bridge connection (TWS/IB Gateway)"""
    try:
        # Extract bridge configuration
        bridge_type = request.credentials.get("bridge_type", "ib_gateway")
        host = request.credentials.get("host", "127.0.0.1")
        port = int(request.credentials.get("port", 4001))
        client_id = int(request.credentials.get("client_id", 1))
        
        logger.info(f"Testing IBKR connection to {bridge_type} at {host}:{port} (client_id={client_id})")
        
        # Import IBKR components
        from app.exchange.ibkr.session import IBKRSessionManager, IBKRSession
        from app.exchange.ibkr.client import IBKRClient
        
        # Create session and attempt connection
        session_manager = IBKRSessionManager()
        connection_id = f"test_{uuid.uuid4().hex[:8]}"
        
        # Get or create session (this will attempt connect)
        session = await session_manager.get_session(connection_id, host=host, port=port)
        
        # Create client
        client = IBKRClient(session)
        
        # Fetch accounts to validate connection
        accounts = client.get_portfolio_accounts()
        
        if not accounts:
            return TestConnectionResponse(
                ok=False,
                error="No accounts found. Ensure you're logged into TWS/Gateway."
            )
        
        # Fetch account summary for first account
        first_account = accounts[0]
        summary = client.get_account_summary(first_account)
        
        return TestConnectionResponse(
            ok=True,
            details={
                "message": "Connection successful",
                "bridge_type": bridge_type,
                "host": host,
                "port": port,
                "accounts": accounts,
                "account_summary": {
                    "account_id": first_account,
                    "wallet": float(summary.get("wallet", 0)),
                    "equity": float(summary.get("equity", 0)),
                    "available": float(summary.get("available", 0))
                }
            }
        )
    
    except ImportError as e:
        logger.error(f"ib_insync not installed: {e}")
        return TestConnectionResponse(
            ok=False,
            error="ib_insync library not installed. Run: pip install ib_insync"
        )
    except ConnectionError as e:
        logger.error(f"Connection failed: {e}")
        return TestConnectionResponse(
            ok=False,
            error=f"Could not connect to TWS/Gateway at {host}:{port}. Ensure it's running and API is enabled."
        )
    except Exception as e:
        logger.exception("Test connection failed")
        return TestConnectionResponse(ok=False, error=str(e))


async def _test_mt_bridge_connection(request: TestConnectionRequest) -> TestConnectionResponse:
    """Test MT4/MT5 bridge connection"""
    try:
        # Extract bridge credentials
        bridge_url = request.credentials.get("bridge_url")
        bridge_token = request.credentials.get("bridge_token")
        tls_mode = request.credentials.get("tls_mode", "strict")
        
        if not bridge_url or not bridge_token:
            return TestConnectionResponse(
                ok=False,
                error="Missing required credentials: bridge_url and bridge_token"
            )
        
        logger.info(f"Testing {request.broker_id.upper()} bridge connection to {bridge_url} (tls_mode={tls_mode})")
        
        from app.exchange.mt_bridge.client import MTBridgeClient
        
        # Create bridge client
        client = MTBridgeClient(
            base_url=bridge_url,
            api_token=bridge_token,
            timeout=10,
            verify_ssl=(tls_mode != "insecure")
        )
        
        # Test health endpoint
        health = client.get_health()
        
        # Test balance endpoint
        balance = client.get_balance()
        
        return TestConnectionResponse(
            ok=True,
            details={
                "message": "Connection successful",
                "platform": health.get("platform"),
                "account": health.get("account"),
                "server": health.get("server"),
                "balance": balance.get("balance"),
                "equity": balance.get("equity"),
                "currency": balance.get("currency"),
                "free_margin": balance.get("free_margin"),
                "server_time": health.get("time")
            }
        )
    
    except Exception as e:
        logger.exception(f"{request.broker_id.upper()} Bridge test connection failed")
        return TestConnectionResponse(
            ok=False,
            error=f"Bridge connection failed: {str(e)}"
        )

# -----------------------------------------------
# IBKR Link Flow
# -----------------------------------------------

@router.post("/ibkr/connect/start")
async def start_ibkr_link_flow(request: Request = None):
    """
    Called by User-Backend to initiate IBKR connection.
    In this MVP, assuming local gateway, we immediately attempt discovery 
    and return the connected account info if successful.
    """
    try:
        # Default local gateway params
        bridge_type = "ib_gateway"
        host = "127.0.0.1"
        port = 4001
        client_id = 1
        
        # Accept overrides
        if request:
            try:
                body = await request.json()
                host = body.get("host", host)
                port = int(body.get("port", port))
                client_id = int(body.get("client_id", client_id))
            except:
                pass

        logger.info(f"Starting IBKR Link Flow: {host}:{port}")

        # Attempt discovery using direct session manager (since Adapter is being refactored)
        from app.exchange.ibkr.session import IBKRSessionManager
        from app.exchange.ibkr.client import IBKRClient
        
        session_manager = IBKRSessionManager()
        connection_id = f"link_{uuid.uuid4().hex[:8]}"
        
        # Connect
        session = await session_manager.get_session(connection_id, host=host, port=port, client_id=client_id)
        
        # Get Accounts
        client = IBKRClient(session)
        accounts = client.get_portfolio_accounts()
        
        if not accounts:
             return {"status": "unreachable", "message": "Connected but no accounts found. Validate TWS login."}
             
        # Success
        return {
            "status": "connected",
            "accounts": accounts,
            "connect_url": None 
        }
        
    except ImportError:
        return {"status": "error", "message": "ib_insync not installed"}
    except Exception as e:
         logger.exception("IBKR Link Start Failed")
         return {"status": "error", "message": str(e)}
