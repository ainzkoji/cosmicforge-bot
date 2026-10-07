from fastapi import APIRouter, HTTPException, Body, Depends, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel
from typing import Dict, Any, Optional, List, Literal
import json
import logging
import time
import uuid
from datetime import datetime
from app.core.auth import caller_is_admin, get_current_user_id
from app.core.config import settings
from app.exchange.ibkr.adapter import IBKRAdapter
from app.exchange.ibkr.errors import IBKRConnectionError, IBKRAuthError
from shared_lib.core.security.url_guard import (
    OutboundPolicy,
    UnsafeDestinationError,
    ValidatedDestination,
    revalidate_destination,
    validate_gateway_url,
    validate_outbound_host_port,
)

# Every route in this router requires an authenticated user. The dependency is
# declared on the router so a route added later cannot be public by omission.
router = APIRouter(dependencies=[Depends(get_current_user_id)])
logger = logging.getLogger(__name__)


# --- Outbound destination policy (SSRF guard) ---
# These routes connect to addresses the caller supplies (IBKR gateway URL,
# TWS/IB Gateway host:port, MT4/MT5 bridge URL). Every such address goes
# through shared_lib.core.security.url_guard first.
#
# The allow-list (BROKER_GATEWAY_ALLOWED_HOSTS) names loopback / private
# addresses, i.e. the platform's OWN gateways. In production only an admin may
# target them: an ordinary user is validated without the allow-list and can
# therefore only reach public addresses. Otherwise every user could run a test
# connection against the platform's local gateway and read its accounts.
#
# The guards resolve DNS (blocking getaddrinfo). Async handlers must call them
# through ``run_in_threadpool``; this process runs the trading loop too.

def outbound_policy(is_admin: bool = False) -> OutboundPolicy:
    """The outbound policy for this caller: the allow-list applies to admins only."""
    policy = OutboundPolicy.from_settings(settings)
    return policy if is_admin else policy.public_only()


def guard_gateway_url(url: Any, is_admin: bool = False) -> ValidatedDestination:
    """Validate a caller-supplied gateway / bridge base URL. Raises UnsafeDestinationError.

    Production: https only (http only for an allow-listed host, admins only).
    Every environment: no query string, no fragment.
    """
    return validate_gateway_url(url, policy=outbound_policy(is_admin))


def guard_gateway_host_port(host: Any, port: Any, is_admin: bool = False) -> ValidatedDestination:
    """Validate a caller-supplied TWS / IB Gateway host:port. Raises UnsafeDestinationError."""
    return validate_outbound_host_port(host, port, policy=outbound_policy(is_admin))


def _close_ibkr_session(session_manager: Any, connection_id: str) -> None:
    """Disconnect a one-off (test / discovery) IBKR session and forget it.

    Called on the event loop on purpose: ib_insync is bound to it, and closing
    the socket does not block.
    """
    try:
        session_manager.close_session(connection_id)
    except Exception as exc:  # never turn a finished test into an error
        logger.warning(f"Could not close IBKR session {connection_id}: {type(exc).__name__}")


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
# A draft (and the plaintext credentials submitted for it) lives this long
# after it was created or last given credentials / validated, then it is
# dropped: this store must not become a long-lived credential cache.
_DRAFT_TTL_SECONDS = 30 * 60
# Upper bound on one submitted credential set (serialized JSON). Real ones are
# a few hundred bytes.
_MAX_CREDENTIALS_BYTES = 16 * 1024
# account_id -> time.monotonic() deadline.
_DRAFT_EXPIRES_AT: Dict[str, float] = {}


def _touch_draft(account_id: str) -> None:
    _DRAFT_EXPIRES_AT[account_id] = time.monotonic() + _DRAFT_TTL_SECONDS


def _purge_expired_drafts(now: Optional[float] = None) -> int:
    """Drop every draft account (and its credentials) whose TTL has passed."""
    now = time.monotonic() if now is None else now
    removed = 0
    live = set()
    for user_id in list(_ACCOUNTS_DB):
        accounts = _ACCOUNTS_DB.get(user_id) or {}
        for account_id in list(accounts):
            # A record without a deadline (created before this bookkeeping)
            # starts its TTL now rather than living forever.
            deadline = _DRAFT_EXPIRES_AT.setdefault(account_id, now + _DRAFT_TTL_SECONDS)
            if deadline <= now:
                accounts.pop(account_id, None)
                _CREDENTIALS_DB.get(user_id, {}).pop(account_id, None)
                removed += 1
            else:
                live.add(account_id)
        if not accounts:
            _ACCOUNTS_DB.pop(user_id, None)
            _CREDENTIALS_DB.pop(user_id, None)
    for account_id in list(_DRAFT_EXPIRES_AT):
        if account_id not in live:
            _DRAFT_EXPIRES_AT.pop(account_id, None)
    return removed


def _credentials_size(credentials: Dict[str, Any]) -> int:
    try:
        return len(json.dumps(credentials, default=str).encode("utf-8"))
    except (TypeError, ValueError):
        return _MAX_CREDENTIALS_BYTES + 1


def _user_accounts(user_id: str) -> Dict[str, BrokerAccount]:
    _purge_expired_drafts()
    return _ACCOUNTS_DB.setdefault(str(user_id), {})


def _user_credentials(user_id: str) -> Dict[str, Dict[str, Any]]:
    return _CREDENTIALS_DB.setdefault(str(user_id), {})


def _owned_account(user_id: str, account_id: str) -> BrokerAccount:
    """The caller's own draft account, or 404 (never another user's)."""
    _purge_expired_drafts()
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
    _purge_expired_drafts()
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
    _touch_draft(account_id)
    return ConnectResponse(account_id=account_id, status="draft")

@router.post("/{account_id}/credentials")
async def submit_credentials(
    account_id: str,
    request: CredentialsRequest,
    user_id: str = Depends(get_current_user_id),
):
    account = _owned_account(user_id, account_id)

    if _credentials_size(request.credentials) > _MAX_CREDENTIALS_BYTES:
        raise HTTPException(status_code=413, detail="Credentials payload too large")

    # In a real app, encrypt this!
    credentials = request.credentials.copy()
    _user_credentials(user_id)[account_id] = credentials
    _touch_draft(account_id)

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
def validate_connection(
    account_id: str,
    user_id: str = Depends(get_current_user_id),
    is_admin: bool = Depends(caller_is_admin),
):
    # A plain ``def`` on purpose: the destination guard resolves DNS and the
    # gateway call is synchronous HTTP, so FastAPI runs this handler in its
    # threadpool instead of on the event loop the trading runtime shares.
    account = _owned_account(user_id, account_id)
    credentials = _user_credentials(user_id).get(account_id)
    
    if not credentials:
        return ValidateResponse(success=False, error="No credentials provided")
    
    try:
        if account.broker_id == "ibkr":
            # Reuse test logic
            gateway_url = credentials.get("gateway_url", "https://localhost:5000/v1/api")
            # Caller-supplied destination: refuse anything the outbound policy
            # does not allow BEFORE the server connects to it. An allow-listed
            # (loopback / private) gateway is for admins only.
            guard_gateway_url(gateway_url, is_admin)
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
        _touch_draft(account_id)
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
    _purge_expired_drafts()
    account = _ACCOUNTS_DB.get(str(user_id), {}).get(account_id)
    if account is not None:
        account.status = "disconnected"
    return {"success": True}

@router.delete("/{account_id}")
async def delete_account(account_id: str, user_id: str = Depends(get_current_user_id)):
    removed = _ACCOUNTS_DB.get(str(user_id), {}).pop(account_id, None)
    _CREDENTIALS_DB.get(str(user_id), {}).pop(account_id, None)
    if removed is not None:
        _DRAFT_EXPIRES_AT.pop(account_id, None)
    _purge_expired_drafts()
    return {"success": True}

@router.post("/test-connection", response_model=TestConnectionResponse)
async def test_broker_connection(
    request: TestConnectionRequest = Body(...),
    user_id: str = Depends(get_current_user_id),
    is_admin: bool = Depends(caller_is_admin),
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
        return await _test_ibkr_connection(request, is_admin)
    elif request.broker_id in ("mt4", "mt5"):
        # Synchronous HTTP (requests, 10 s timeouts) and DNS: off the event loop.
        return await run_in_threadpool(_test_mt_bridge_connection, request, is_admin)
    else:
        # Mock success for other brokers (not yet implemented)
        return TestConnectionResponse(
            ok=True, 
            details={"message": f"Mock success - {request.broker_id} validation not yet implemented"}
        )


async def _test_ibkr_connection(request: TestConnectionRequest, is_admin: bool = False) -> TestConnectionResponse:
    """Test IBKR bridge connection (TWS/IB Gateway)"""
    session_manager = None
    connection_id = None
    try:
        # Extract bridge configuration
        bridge_type = request.credentials.get("bridge_type", "ib_gateway")
        host = request.credentials.get("host", "127.0.0.1")
        port = int(request.credentials.get("port", 4001))
        client_id = int(request.credentials.get("client_id", 1))

        # Caller-supplied host:port. Validate it, then connect to the IP that
        # was validated so the hostname is not resolved a second time. The
        # guard resolves DNS, so it runs in the threadpool. An allow-listed
        # (loopback / private) gateway is the platform's own: admins only.
        try:
            destination = await run_in_threadpool(guard_gateway_host_port, host, port, is_admin)
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
    finally:
        # A test connection is one-off: do not leave the TWS session (and its
        # client id) open, and do not let the session cache grow per call.
        if session_manager is not None and connection_id is not None:
            _close_ibkr_session(session_manager, connection_id)


_BRIDGE_FAILURE_TEXT = {
    "TIMEOUT": "the bridge did not answer in time",
    "UNREACHABLE": "the bridge could not be reached",
    "REQUEST_FAILED": "the request to the bridge failed",
    "INVALID_RESPONSE": "the bridge sent an unexpected response",
    "HTTP_ERROR": "the bridge rejected the request",
}


def _bridge_failure_message(exc: Exception) -> str:
    """What a caller is told when a bridge test fails: a fixed text and the HTTP status.

    Never the exception message or the response body: both can carry text the
    remote host chose, and the remote host is whatever URL the caller named.
    """
    reason = _BRIDGE_FAILURE_TEXT.get(getattr(exc, "failure_kind", None), "unexpected error")
    status = getattr(exc, "status_code", None)
    suffix = f" (HTTP {status})" if isinstance(status, int) and not isinstance(status, bool) else ""
    return f"Bridge connection failed: {reason}{suffix}"


def _redact(text: Any, *secrets: Any, limit: int = 500) -> str:
    out = str(text)
    for secret in secrets:
        if secret:
            out = out.replace(str(secret), "[REDACTED]")
    return out[:limit]


def _test_mt_bridge_connection(request: TestConnectionRequest, is_admin: bool = False) -> TestConnectionResponse:
    """Test MT4/MT5 bridge connection.

    Synchronous (requests + DNS): the route runs it in the threadpool.
    """
    bridge_token = None
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
        # Production: https only, public addresses only unless the caller is
        # an admin and the host is allow-listed; never a query or fragment.
        policy = outbound_policy(is_admin)
        try:
            destination = validate_gateway_url(bridge_url, policy=policy)
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
        
        from app.exchange.mt_bridge.client import MTBridgeClient, harden_bridge_client

        # Create bridge client. "insecure" (caller-selected, for self-signed
        # bridges) is honoured outside production only, unless
        # BROKER_GATEWAY_VERIFY_TLS=false.
        client = MTBridgeClient(
            base_url=destination.url,
            api_token=bridge_token,
            timeout=10,
            verify_ssl=gateway_verify_tls(tls_mode != "insecure")
        )
        # Only bridge_url was validated: never follow a redirect to an address
        # the guard did not see (requests raises TooManyRedirects instead).
        # And because requests resolves the hostname again for every request,
        # the destination is re-validated (fresh lookup, same policy)
        # immediately before each one. Residual: the gap between that lookup
        # and the one inside requests -- pinning the connection to the
        # validated IP would break TLS SNI / certificate checks with requests,
        # so close it at the network layer (egress firewall).
        harden_bridge_client(client, lambda: revalidate_destination(destination, policy=policy))

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
        # Detail (which may quote the remote host) stays in the server log,
        # with the bearer token removed; the caller gets a fixed text.
        logger.error(
            f"{request.broker_id.upper()} Bridge test connection failed "
            f"({type(e).__name__}): {_redact(e, bridge_token)}"
        )
        if getattr(e, "failure_kind", None) == "DESTINATION_NOT_ALLOWED":
            return TestConnectionResponse(ok=False, error="Destination not allowed (DESTINATION_CHANGED)")
        return TestConnectionResponse(ok=False, error=_bridge_failure_message(e))

# -----------------------------------------------
# IBKR Link Flow
# -----------------------------------------------

@router.post("/ibkr/connect/start")
async def start_ibkr_link_flow(
    request: Request = None,
    user_id: str = Depends(get_current_user_id),
    is_admin: bool = Depends(caller_is_admin),
):
    """
    Called by User-Backend to initiate IBKR connection.
    In this MVP, assuming local gateway, we immediately attempt discovery
    and return the connected account info if successful.

    Production: the default local gateway (and any other loopback / private
    address) is reachable only when it is allow-listed AND the caller is an
    admin; an ordinary user may only name a public address.
    """
    session_manager = None
    connection_id = None
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

        # host/port may be caller-supplied: validate (in the threadpool: the
        # guard resolves DNS), then connect to the validated IP (no second
        # DNS lookup).
        try:
            destination = await run_in_threadpool(guard_gateway_host_port, host, port, is_admin)
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
    finally:
        # Discovery only: the session id is never returned to the caller, so
        # nothing can use this session again. Close it rather than keep it.
        if session_manager is not None and connection_id is not None:
            _close_ibkr_session(session_manager, connection_id)
