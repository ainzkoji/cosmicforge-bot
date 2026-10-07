from fastapi import APIRouter, HTTPException, Body, Depends, Request
from pydantic import BaseModel
from typing import Dict, Any, Optional, List, Literal
import logging
import uuid
from datetime import datetime
from app.core.auth import get_current_user_id
from app.core.config import settings
from app.exchange.ibkr.adapter import IBKRAdapter
from app.exchange.ibkr.errors import IBKRConnectionError, IBKRAuthError
from shared_lib.core.security.url_guard import (
    OutboundPolicy,
    UnsafeDestinationError,
    ValidatedDestination,
    validate_outbound_host_port,
    validate_outbound_url,
)

# Every route in this router requires an authenticated user. The dependency is
# declared on the router so a route added later cannot be public by omission.
router = APIRouter(dependencies=[Depends(get_current_user_id)])
logger = logging.getLogger(__name__)


# --- Outbound destination policy (SSRF guard) ---
# These routes connect to addresses the caller supplies (IBKR gateway URL,
# TWS/IB Gateway host:port, MT4/MT5 bridge URL). Every such address goes
# through shared_lib.core.security.url_guard first.

def guard_gateway_url(url: Any) -> ValidatedDestination:
    """Validate a caller-supplied gateway / bridge URL. Raises UnsafeDestinationError."""
    return validate_outbound_url(url, policy=OutboundPolicy.from_settings(settings))


def guard_gateway_host_port(host: Any, port: Any) -> ValidatedDestination:
    """Validate a caller-supplied TWS / IB Gateway host:port. Raises UnsafeDestinationError."""
    return validate_outbound_host_port(host, port, policy=OutboundPolicy.from_settings(settings))


def gateway_verify_tls(legacy_default: bool) -> bool:
    """Whether to verify TLS for a caller-supplied gateway / bridge URL.

    BROKER_GATEWAY_VERIFY_TLS: "true" always verifies; "false" keeps the legacy
    behaviour (``legacy_default``); "auto" verifies in production and keeps the
    legacy behaviour elsewhere (local self-signed gateways in development).
    """
    mode = str(getattr(settings, "BROKER_GATEWAY_VERIFY_TLS", "auto") or "auto").strip().lower()
    if mode in ("true", "1", "yes", "on"):
        return True
    if mode in ("false", "0", "no", "off"):
        return legacy_default
    return True if settings.production else legacy_default


def _destination_error(exc: UnsafeDestinationError) -> str:
    return f"Destination not allowed ({exc.reason})"

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
    environment: Literal['live', 'paper', 'testnet', 'demo'] = 'live'
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
# In a real app, this would be a DB table.
# Scoped by owner: user_id -> account_id -> record. A caller can only ever
# reach the inner dict for their own user id, so one user can never list,
# validate, modify or delete another user's draft account or credentials.
_ACCOUNTS_DB: Dict[str, Dict[str, BrokerAccount]] = {}
_CREDENTIALS_DB: Dict[str, Dict[str, Dict[str, Any]]] = {}
# Bound the per-user draft store so an authenticated caller cannot grow it forever.
_MAX_ACCOUNTS_PER_USER = 50


def _user_accounts(user_id: str) -> Dict[str, BrokerAccount]:
    return _ACCOUNTS_DB.setdefault(str(user_id), {})


def _user_credentials(user_id: str) -> Dict[str, Dict[str, Any]]:
    return _CREDENTIALS_DB.setdefault(str(user_id), {})


def _owned_account(user_id: str, account_id: str) -> BrokerAccount:
    """The caller's own draft account, or 404 (never another user's)."""
    account = _ACCOUNTS_DB.get(str(user_id), {}).get(account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    return account

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
async def get_broker_accounts(user_id: str = Depends(get_current_user_id)):
    return BrokerAccountsResponse(accounts=list(_ACCOUNTS_DB.get(str(user_id), {}).values()))

@router.post("/connect", response_model=ConnectResponse)
async def start_connection(request: ConnectRequest, user_id: str = Depends(get_current_user_id)):
    accounts = _user_accounts(user_id)
    if len(accounts) >= _MAX_ACCOUNTS_PER_USER:
        raise HTTPException(status_code=409, detail="Too many pending broker connections; delete one first")
    account_id = str(uuid.uuid4())
    account = BrokerAccount(
        id=account_id,
        broker_id=request.broker_id,
        market_type=request.market_type,
        status="draft",
        label=request.label or f"{request.broker_id.upper()} Account",
        created_at=datetime.utcnow().isoformat()
    )
    accounts[account_id] = account
    return ConnectResponse(account_id=account_id, status="draft")

@router.post("/{account_id}/credentials")
async def submit_credentials(
    account_id: str,
    request: CredentialsRequest,
    user_id: str = Depends(get_current_user_id),
):
    account = _owned_account(user_id, account_id)
    
    # In a real app, encrypt this!
    credentials = request.credentials.copy()
    _user_credentials(user_id)[account_id] = credentials
    
    # Update account status
    account.status = "validating"
    account.environment = credentials.get("environment", "paper")
    
    # Mask key for display
    if "api_key" in credentials:
        account.masked_key = f"***{credentials['api_key'][-4:]}"
    elif "account_id" in credentials:
        account.masked_key = f"{credentials['account_id']}"
    
    return {"success": True, "status": "validating"}

@router.post("/{account_id}/validate", response_model=ValidateResponse)
async def validate_connection(account_id: str, user_id: str = Depends(get_current_user_id)):
    account = _owned_account(user_id, account_id)
    credentials = _user_credentials(user_id).get(account_id)
    
    if not credentials:
        return ValidateResponse(success=False, error="No credentials provided")
    
    try:
        if account.broker_id == "ibkr":
            # Reuse test logic
            gateway_url = credentials.get("gateway_url", "https://localhost:5000/v1/api")
            # Caller-supplied destination: refuse anything the outbound policy
            # does not allow BEFORE the server connects to it.
            guard_gateway_url(gateway_url)
            # A local IBKR Client Portal gateway serves a self-signed
            # certificate, hence the legacy default of no verification.
            # Production verifies unless BROKER_GATEWAY_VERIFY_TLS=false.
            verify_ssl = gateway_verify_tls(False)
            
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
                credentials["account_id"] = adapter._account_id

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
        
    except UnsafeDestinationError as e:
        account.status = "error"
        account.last_error_message = _destination_error(e)
        logger.warning(f"Validation refused for {account_id}: gateway destination not allowed ({e.reason})")
        return ValidateResponse(success=False, error=_destination_error(e))
    except Exception as e:
        account.status = "error"
        account.last_error_message = str(e)
        logger.error(f"Validation failed for {account_id}: {e}")
        return ValidateResponse(success=False, error=str(e))

@router.post("/{account_id}/disconnect")
async def disconnect_account(account_id: str, user_id: str = Depends(get_current_user_id)):
    account = _ACCOUNTS_DB.get(str(user_id), {}).get(account_id)
    if account is not None:
        account.status = "disconnected"
    return {"success": True}

@router.delete("/{account_id}")
async def delete_account(account_id: str, user_id: str = Depends(get_current_user_id)):
    _ACCOUNTS_DB.get(str(user_id), {}).pop(account_id, None)
    _CREDENTIALS_DB.get(str(user_id), {}).pop(account_id, None)
    return {"success": True}

@router.post("/test-connection", response_model=TestConnectionResponse)
async def test_broker_connection(
    request: TestConnectionRequest = Body(...),
    user_id: str = Depends(get_current_user_id),
):
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

        # Caller-supplied host:port. Validate it, then connect to the IP that
        # was validated so the hostname is not resolved a second time.
        try:
            destination = guard_gateway_host_port(host, port)
        except UnsafeDestinationError as e:
            logger.warning(f"IBKR test connection refused: destination not allowed ({e.reason})")
            return TestConnectionResponse(ok=False, error=_destination_error(e))
        
        logger.info(f"Testing IBKR connection to {bridge_type} at {host}:{port} (client_id={client_id})")
        
        # Import IBKR components
        from app.exchange.ibkr.session import IBKRSessionManager, IBKRSession
        from app.exchange.ibkr.client import IBKRClient
        
        # Create session and attempt connection
        session_manager = IBKRSessionManager()
        connection_id = f"test_{uuid.uuid4().hex[:8]}"
        
        # Get or create session (this will attempt connect)
        session = await session_manager.get_session(
            connection_id, host=destination.connect_host, port=destination.port
        )
        
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
        
        # Caller-supplied URL: refuse it before the server sends the bearer
        # token (or anything else) to a destination the policy does not allow.
        try:
            destination = guard_gateway_url(bridge_url)
        except UnsafeDestinationError as e:
            logger.warning(
                f"{request.broker_id.upper()} bridge test refused: destination not allowed ({e.reason})"
            )
            return TestConnectionResponse(ok=False, error=_destination_error(e))

        # Host only: the full URL is caller input and is not written to logs.
        logger.info(
            f"Testing {request.broker_id.upper()} bridge connection to "
            f"{destination.scheme}://{destination.host}:{destination.port} (tls_mode={tls_mode})"
        )
        
        from app.exchange.mt_bridge.client import MTBridgeClient
        
        # Create bridge client. "insecure" (caller-selected, for self-signed
        # bridges) is honoured outside production only, unless
        # BROKER_GATEWAY_VERIFY_TLS=false.
        client = MTBridgeClient(
            base_url=bridge_url,
            api_token=bridge_token,
            timeout=10,
            verify_ssl=gateway_verify_tls(tls_mode != "insecure")
        )
        # Only bridge_url was validated: never follow a redirect to an address
        # the guard did not see (requests raises TooManyRedirects instead).
        _http_session = getattr(client, "_session", None)
        if _http_session is not None:
            _http_session.max_redirects = 0
        
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
async def start_ibkr_link_flow(request: Request = None, user_id: str = Depends(get_current_user_id)):
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

        # host/port may be caller-supplied: validate, then connect to the
        # validated IP (no second DNS lookup).
        try:
            destination = guard_gateway_host_port(host, port)
        except UnsafeDestinationError as e:
            logger.warning(f"IBKR Link Flow refused: destination not allowed ({e.reason})")
            return {"status": "error", "message": _destination_error(e)}

        logger.info(f"Starting IBKR Link Flow: {host}:{port}")

        # Attempt discovery using direct session manager (since Adapter is being refactored)
        from app.exchange.ibkr.session import IBKRSessionManager
        from app.exchange.ibkr.client import IBKRClient
        
        session_manager = IBKRSessionManager()
        connection_id = f"link_{uuid.uuid4().hex[:8]}"
        
        # Connect
        session = await session_manager.get_session(
            connection_id, host=destination.connect_host, port=destination.port, client_id=client_id
        )
        
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
