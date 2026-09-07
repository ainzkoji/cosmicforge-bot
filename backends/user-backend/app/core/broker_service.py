import uuid
import json
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from shared_lib.persistence.db import DB
from shared_lib.core.security.broker_security import encrypt_credentials, mask_credentials

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_db() -> DB:
    import os
    # We load standard env first, then determine path
    db_url = os.environ.get("DATABASE_URL")
    if db_url and db_url.startswith("sqlite:///"):
        path = db_url.replace("sqlite:///", "")
        return DB(path=path)
    return DB()

# ============================================================================
# Broker Catalog
# ============================================================================

BROKER_CATALOG = [
    {
        "id": "binance",
        "name": "Binance",
        "market_types": ["crypto"],
        "logo": "https://upload.wikimedia.org/wikipedia/commons/e/e8/Binance_Logo.svg",
        "signup_url": "https://accounts.binance.com/register",
        "auth_fields": [
            {"name": "api_key", "label": "API Key", "type": "text", "required": True},
            {"name": "api_secret", "label": "API Secret", "type": "password", "required": True},
        ],
        "features": ["futures"],
        "required_permissions": ["Read General", "Futures Trading"]
    },
    {
        "id": "bybit",
        "name": "ByBit",
        "market_types": ["crypto"],
        "logo": "/assets/brokers/bybit.png",
        "signup_url": "#",
        "auth_fields": [
            {"name": "api_key", "label": "API Key", "type": "text", "required": True},
            {"name": "api_secret", "label": "API Secret", "type": "password", "required": True},
        ],
        "features": ["spot", "futures", "derivatives"],
        "required_permissions": ["Read", "Contract Trade", "Spot Trade"]
    },
    {
        "id": "bingx",
        "name": "BingX",
        "market_types": ["crypto"],
        "logo": "/assets/brokers/bingx.png",
        "signup_url": "#",
        "auth_fields": [
            {"name": "api_key", "label": "API Key", "type": "text", "required": True},
            {"name": "api_secret", "label": "API Secret", "type": "password", "required": True},
        ],
        "features": ["futures"],
        "required_permissions": ["Perpetual Futures Trading"]
    },
    {
        "id": "coinbase",
        "name": "Coinbase",
        "market_types": ["crypto"],
        "logo": "/assets/brokers/coinbase.png",
        "signup_url": "#",
        "auth_fields": [
            {"name": "api_key", "label": "API Key", "type": "text", "required": True},
            {"name": "api_secret", "label": "API Secret", "type": "password", "required": True},
            {"name": "passphrase", "label": "Passphrase", "type": "password", "required": True},
        ],
        "features": ["spot"],
        "required_permissions": ["View", "Trade"]
    },
    {
        "id": "kraken",
        "name": "Kraken",
        "market_types": ["crypto"],
        "logo": "/assets/brokers/kraken.png",
        "signup_url": "#",
        "auth_fields": [
            {"name": "api_key", "label": "API Key", "type": "text", "required": True},
            {"name": "private_key", "label": "Private Key", "type": "password", "required": True},
        ],
        "features": ["spot", "futures", "margin"],
        "required_permissions": ["Query Funds", "Create & Modify Orders"]
    },
    {
        "id": "alpaca",
        "name": "Alpaca",
        "market_types": ["stocks", "crypto"],
        "logo": "/assets/brokers/alpaca.png",
        "signup_url": "#",
        "auth_fields": [
            {"name": "api_key", "label": "API Key", "type": "text", "required": True},
            {"name": "api_secret", "label": "API Secret", "type": "password", "required": True},
        ],
        "features": ["stocks", "crypto"],
        "required_permissions": ["Read", "Trade"]
    },
    {
        "id": "oanda",
        "name": "OANDA",
        "market_types": ["forex"],
        "logo": "/assets/brokers/oanda.png",
        "signup_url": "https://www.oanda.com/register",
        "auth_fields": [
            {"name": "api_token", "label": "Personal Access Token", "type": "password", "required": True},
            {"name": "account_id", "label": "Account ID", "type": "text", "required": True},
        ],
        "features": ["forex", "cfd"],
        "required_permissions": ["Trading", "Read Account"]
    },
    {
        "id": "ibkr",
        "name": "Interactive Brokers",
        "market_types": ["forex", "stocks"],
        "logo": "/assets/brokers/ibkr.png",
        "signup_url": "https://www.interactivebrokers.com/en/index.php?f=1338",
        "connect_type": "bridge",
        "bridge_types": ["tws", "ib_gateway"],
        "auth_fields": [
            {"name": "bridge_type", "label": "Bridge Type", "type": "select", "options": [
                {"value": "tws", "label": "TWS (Trader Workstation)"},
                {"value": "ib_gateway", "label": "IB Gateway"}
            ], "required": True, "default": "ib_gateway"},
            {"name": "host", "label": "Host", "type": "text", "required": True, "default": "127.0.0.1", "help": "Bridge host address"},
            {"name": "port", "label": "Port", "type": "number", "required": True, "default": 4001, "help": "TWS: 7496 (live) / 7497 (paper), Gateway: 4001 (live) / 4002 (paper)"},
            {"name": "client_id", "label": "Client ID", "type": "number", "required": True, "default": 1, "help": "Unique client identifier (1-32)"},
            {"name": "environment", "label": "Environment", "type": "select", "options": [
                {"value": "paper", "label": "Paper Trading"},
                {"value": "live", "label": "Live Trading"}
            ], "required": True, "default": "paper"}
        ],
        "features": ["forex", "stocks"],
        "required_permissions": ["Trading", "Account Info"],
        "setup_instructions": "Install and run TWS or IB Gateway on your machine. Log in with your IBKR credentials before connecting."
    },
    {
        "id": "mt4",
        "name": "MetaTrader 4",
        "market_types": ["forex"],
        "logo": "/assets/brokers/mt4.png",
        "signup_url": "https://www.metatrader4.com/",
        "connect_type": "bridge",
        "auth_fields": [
            {"name": "bridge_url", "label": "Bridge URL", "type": "text", "required": True, "help": "Your Windows VPS bridge URL (e.g., https://your-vps.com:8443)"},
            {"name": "bridge_token", "label": "API Token", "type": "password", "required": True, "help": "Bridge API authentication token"},
            {"name": "tls_mode", "label": "TLS Verification", "type": "select", "options": [
                {"value": "strict", "label": "Strict (Verified Cert)"},
                {"value": "insecure", "label": "Insecure (Self-Signed)"}
            ], "required": True, "default": "strict", "help": "Use 'Strict' for public domains with valid SSL. Use 'Insecure' only for self-signed certificates (not recommended)."},
            {"name": "account_label", "label": "Account Label (Optional)", "type": "text", "required": False}
        ],
        "features": ["forex", "cfd"],
        "required_permissions": ["Trading", "Account Access"],
        "setup_instructions": "Install MT4 Bridge on your Windows VPS. Run MT4 terminal and the bridge service, then provide the bridge URL and token."
    },
    {
        "id": "mt5",
        "name": "MetaTrader 5",
        "market_types": ["forex"],
        "logo": "/assets/brokers/mt5.png",
        "signup_url": "https://www.metatrader5.com/",
        "connect_type": "bridge",
        "auth_fields": [
            {"name": "bridge_url", "label": "Bridge URL", "type": "text", "required": True, "help": "Your Windows VPS bridge URL (e.g., https://your-vps.com:8443)"},
            {"name": "bridge_token", "label": "API Token", "type": "password", "required": True, "help": "Bridge API authentication token"},
            {"name": "tls_mode", "label": "TLS Verification", "type": "select", "options": [
                {"value": "strict", "label": "Strict (Verified Cert)"},
                {"value": "insecure", "label": "Insecure (Self-Signed)"}
            ], "required": True, "default": "strict", "help": "Use 'Strict' for public domains with valid SSL. Use 'Insecure' only for self-signed certificates (not recommended)."},
            {"name": "account_label", "label": "Account Label (Optional)", "type": "text", "required": False}
        ],
        "features": ["forex", "cfd", "hedging"],
        "required_permissions": ["Trading", "Account Access"],
        "setup_instructions": "Install MT5 Bridge on your Windows VPS. Run MT5 terminal and the bridge service, then provide the bridge URL and token."
    }
]

def get_broker_catalog(user_id: str) -> List[Dict[str, Any]]:
    """
    Returns list of available brokers for the user.
    Can implement logic here to filter based on user region, plan, etc.
    """
    # Transform catalog items to match response schema
    transformed = []
    for broker in BROKER_CATALOG:
        # Determine auth_types based on auth_fields
        auth_types = []
        for field in broker.get("auth_fields", []):
            if field["name"] in ["api_key", "api_secret"]:
                if "api_key" not in auth_types:
                    auth_types.append("api_key")
            elif field["name"] in ["login", "password", "server"]:
                if "mt5" not in auth_types:
                    auth_types.append("mt5")
        
        if not auth_types:  # Default to api_key if unclear
            auth_types = ["api_key"]
        
        is_available = broker["id"] in ["binance", "bybit", "bingx", "oanda", "ibkr", "mt4", "mt5"]
        
        transformed.append({
            "id": broker["id"],
            "name": broker["name"],
            "market_types": broker["market_types"],
            "logo": broker["logo"],
            "auth_fields": broker["auth_fields"],
            "features": broker["features"],
            "required_permissions": broker["required_permissions"],
            "is_available": is_available,
            "unavailable_reason": None if is_available else "Coming Soon",
            "signup_url": broker.get("signup_url"),
            "affiliate_info": None
        })
    
    return transformed

def get_broker_details(broker_id: str) -> Optional[Dict[str, Any]]:
    for b in BROKER_CATALOG:
        if b["id"] == broker_id:
            return b
    return None

# ============================================================================
# Account Management
# ============================================================================

def create_broker_account_draft(user_id: str, broker_id: str, market_type: str, label: Optional[str] = None) -> str:
    """
    Creates a new broker account in DRAFT state.
    Returns account_id.
    """
    db = get_db()
    account_id = f"brk_{uuid.uuid4().hex[:12]}"
    now = utc_now_iso()
    
    if not label:
        broker_info = get_broker_details(broker_id)
        label = f"{broker_info['name'] if broker_info else broker_id} Account"

    # Enforce Entitlements
    from app.core import billing_service
    sub = billing_service.get_user_subscription(user_id)
    max_brokers = sub["entitlements"].get("max_brokers", 1)
    
    # Check for existing DRAFT for this broker (resume flow of existing draft)
    with db.connect() as conn:
        existing_draft = conn.execute(
            "SELECT id FROM broker_accounts WHERE user_id = ? AND broker_id = ? AND status = 'draft'",
            (user_id, broker_id)
        ).fetchone()
        
        if existing_draft:
             return existing_draft[0]
             
        # Count existing (active/draft) accounts for limit check
        count = conn.execute(
            "SELECT COUNT(*) FROM broker_accounts WHERE user_id = ? AND status != 'disconnected'", 
            (user_id,)
        ).fetchone()[0]
        
        if count >= max_brokers:
           raise ValueError(f"Plan limit reached: Max {max_brokers} connected brokers.")

        conn.execute(
            """
            INSERT INTO broker_accounts 
            (id, user_id, broker_id, market_type, label, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'draft', ?, ?)
            """,
            (account_id, user_id, broker_id, market_type, label, now, now)
        )
        
    return account_id

def submit_broker_credentials(user_id: str, account_id: str, credentials: Dict[str, Any]) -> bool:
    """
    Securely saves broker credentials and moves state to 'validating'.
    """
    db = get_db()
    now = utc_now_iso()
    
    # Extract environment if present (metadata), remove from blob if desired, or keep it.
    # Usually we want environment in the table for easy querying.
    environment = credentials.get("environment", "live")
    
    # Check broker_id from account
    with db.connect() as conn:
        acc_row = conn.execute("SELECT broker_id FROM broker_accounts WHERE id = ? AND user_id = ?", (account_id, user_id)).fetchone()
        if not acc_row:
            return False
        broker_id = acc_row["broker_id"]
    
    # KYC Check for IBKR Live Trading
    if broker_id == "ibkr" and environment == "live":
        from app.core.kyc_service import check_kyc_status
        if not check_kyc_status(user_id):
            # Return False or raise error - credentials will not be saved
            raise ValueError("KYC verification required for IBKR live trading. Please complete verification first.")
    
    # Encrypt everything (including env is fine, but env is also metadata)
    encrypted_blob = encrypt_credentials(credentials)
    masked = mask_credentials(credentials)
    
    with db.connect() as conn:
        # Verify ownership
        row = conn.execute("SELECT id FROM broker_accounts WHERE id = ? AND user_id = ?", (account_id, user_id)).fetchone()
        if not row:
            return False
            
        # Insert or Replace into Credentials table
        conn.execute(
            """
            INSERT INTO broker_credentials (account_id, encrypted_blob, key_metadata, updated_at)
            VALUES (?, ?, 'fernet_v1', ?)
            ON CONFLICT(account_id) DO UPDATE SET
            encrypted_blob = excluded.encrypted_blob,
            updated_at = excluded.updated_at
            """,
            (account_id, encrypted_blob, now)
        )
            
        # Update Account Status and Environment
        conn.execute(
            """
            UPDATE broker_accounts 
            SET masked_key = ?, status = 'validating', environment = ?, updated_at = ?
            WHERE id = ?
            """,
            (masked, environment, now, account_id)
        )
        
        # Log event
        _log_audit_event(conn, account_id, user_id, "credentials_submitted", {"environment": environment})
        
    return True

def validate_broker_account(user_id: str, account_id: str) -> Dict[str, Any]:
    """
    Runs the validation pipeline by testing the API keys with the real broker.
    Uses resolve_broker_auth() so credentials are read from broker_credentials_v2
    (with v1 fallback) — same path as get_broker_summary and the bot-backend.
    """
    from shared_lib.broker.resolver import resolve_broker_auth
    from shared_lib.broker.errors import BrokerResolverError

    db = get_db()
    now = utc_now_iso()

    # 1. Verify account belongs to this user
    with db.connect() as conn:
        row = conn.execute(
            "SELECT broker_id FROM broker_accounts WHERE id = ? AND user_id = ?",
            (account_id, user_id),
        ).fetchone()
        if not row:
            return {"success": False, "error": "Account not found"}
        broker_id = row["broker_id"]

    # 2. Resolve credentials via canonical v2 resolver (v1 fallback included)
    try:
        auth = resolve_broker_auth(account_id, user_id, db)
    except BrokerResolverError as exc:
        return {"success": False, "error": str(exc)}

    # 3. Test live connection
    credentials = {"api_key": auth.api_key, "api_secret": auth.api_secret}
    environment = auth.environment.value
    validation_result = _test_broker_connection(broker_id, credentials, environment)

    # 4. Persist result
    if validation_result["success"]:
        new_status = "connected"
        capabilities = validation_result.get("capabilities", ["read", "trade"])
        error_msg = None
    else:
        new_status = "restricted"
        capabilities = []
        error_msg = validation_result.get("error", "Unknown validation error")

    with db.connect() as conn:
        conn.execute(
            """
            UPDATE broker_accounts
            SET status = ?,
                capabilities = ?,
                last_validated_at = ?,
                last_error_message = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (new_status, json.dumps(capabilities) if capabilities else None, now, error_msg, now, account_id),
        )
        _log_audit_event(conn, account_id, user_id, "validation_completed",
                         {"success": validation_result["success"], "status": new_status})

    return {
        "success": validation_result["success"],
        "status": new_status,
        "capabilities": capabilities,
        "error": error_msg,
    }

def get_broker_summary(user_id: str, account_id: str) -> Dict[str, Any]:
    """
    Fetches live capital summary from the exchange.

    Uses resolve_broker_auth() so credentials are read from broker_credentials_v2
    (with legacy fallback) via the same DB as the rest of the user-backend.
    """
    from shared_lib.broker.resolver import resolve_broker_auth
    from shared_lib.broker.errors import BrokerResolverError

    db = get_db()

    # ── 1. Resolve credentials via canonical resolver ──────────────────────
    try:
        auth = resolve_broker_auth(account_id, user_id, db)
    except BrokerResolverError as exc:
        return {
            "account_id": account_id,
            "status": "error",
            "error_code": exc.reason_code,
            "error": str(exc),
        }

    broker_id = auth.broker_type

    # ── 2. Instantiate client using auth.base_url (canonical URL) ─────────
    client = None
    if broker_id == "binance":
        from app.exchange.binance.client import BinanceFuturesClient
        client_raw = BinanceFuturesClient(
            api_key=auth.api_key,
            api_secret=auth.api_secret,
            base_url=auth.base_url,
        )
        # Wrap in a namespace so downstream code using client.client.account()
        # also works, and direct .account() / .position_risk_all() work too.
        class _BinanceWrap:
            def __init__(self, c): self.client = c
            def account(self): return self.client.account()
        client = _BinanceWrap(client_raw)
    elif broker_id == "bybit":
        from app.exchange.bybit.client import BybitClient
        testnet = (auth.environment.value in ("demo",))
        client = BybitClient(auth.api_key, auth.api_secret, testnet=testnet)
    elif broker_id == "bingx":
        from app.exchange.bingx.client import BingXClient
        client = BingXClient(auth.api_key, auth.api_secret)
    elif broker_id == "oanda":
        from app.exchange.oanda_client import OandaClient
        practice = (auth.environment.value == "demo")
        client = OandaClient(
            api_token=auth.api_key,
            account_id=auth.extra.get("account_id", ""),
            practice=practice,
        )
    else:
        return {"error": f"Unsupported broker {broker_id}"}

    # ── 4. Fetch Account Data ──────────────────────────────────────────────
    # Both clients implement .account() returning {totalWalletBalance, totalMarginBalance, availableBalance, totalUnrealizedProfit}
    try:
        data = client.account()

        # Try to get position count (if client supports it)
        position_count = 0
        initial_margin = 0.0

        # For Binance, try to get positions
        if broker_id == "binance" and hasattr(client.client, 'positions'):
            try:
                positions = client.client.positions()
                if positions and isinstance(positions, list):
                    # Count non-zero positions
                    position_count = len([p for p in positions if float(p.get('positionAmt', 0)) != 0])
            except:
                pass

        # For Bybit, utilize positions() method
        if broker_id == "bybit" and hasattr(client, 'positions'):
            try:
                positions = client.positions()
                if positions and isinstance(positions, list):
                    # Count non-zero positions (Bybit V5: size != 0)
                    position_count = len([p for p in positions if float(p.get('size', 0)) != 0])
            except:
                pass

        # OANDA specific summary approach
        if broker_id == "oanda":
            data = client.get_account_summary()
            balance = float(data.get("balance", 0.0))
            unrealized = float(data.get("unrealizedPL", 0.0))
            margin_used = float(data.get("marginUsed", 0.0))

            return {
                "account_id": account_id,
                "broker_id": broker_id,
                "status": "connected",
                "updated_at": utc_now_iso(),
                "capital": {
                    "total_wallet_balance": balance,
                    "total_equity": float(data.get("NAV", balance)),
                    "available_balance": float(data.get("marginAvailable", 0.0)),
                    "unrealized_pnl": unrealized,
                    "currency": data.get("currency", "USD"),
                    "position_count": int(data.get("openTradeCount", 0)),
                    "initial_margin": margin_used
                }
            }

        if data.get("totalInitialMargin") is not None:
            initial_margin = float(data.get("totalInitialMargin"))

        return {
            "account_id": account_id,
            "broker_id": broker_id,
            "status": "connected",
            "updated_at": utc_now_iso(),
            "capital": {
                "total_wallet_balance": float(data.get("totalWalletBalance", 0.0)),
                "total_equity": float(data.get("totalMarginBalance", 0.0)),
                "available_balance": float(data.get("availableBalance", 0.0)),
                "unrealized_pnl": float(data.get("totalUnrealizedProfit", 0.0)),
                "currency": "USDT",
                "position_count": position_count,
                "initial_margin": initial_margin
            }
        }
    except Exception as e:
        err_msg = str(e)
        if "null apikey" in err_msg.lower():
            err_msg = "Authentication failed: Invalid API Key or signature."

        return {
            "account_id": account_id,
            "broker_id": broker_id,
            "status": "error",
            "error": err_msg
        }


def _test_broker_connection(broker_id: str, credentials: Dict[str, Any], environment: str) -> Dict[str, Any]:
    """Test connection to a specific broker using their API"""
    try:
        if broker_id == "binance":
            from app.exchange.binance_client import BinanceClient
            testnet = (environment == "demo" or environment == "testnet")
            client = BinanceClient(
                api_key=credentials.get("api_key"),
                api_secret=credentials.get("api_secret"),
                testnet=testnet
            )
            return client.test_connection()
            
        elif broker_id == "bybit":
            from app.exchange.bybit.client import BybitClient as ByBitClient
            testnet = (environment == "demo" or environment == "testnet")
            client = ByBitClient(
                api_key=credentials.get("api_key"),
                api_secret=credentials.get("api_secret"),
                testnet=testnet
            )
            return client.test_connection()
            
        elif broker_id == "bingx":
            from app.exchange.bingx.client import BingXClient
            testnet = (environment == "demo" or environment == "testnet")
            client = BingXClient(
                api_key=credentials.get("api_key"),
                api_secret=credentials.get("api_secret"),
                testnet=testnet
            )
            return client.test_connection()
        
        elif broker_id == "oanda":
            # OANDA v20 connection test
            from app.exchange.oanda_client import OandaClient
            practice = (environment in ("demo", "practice", "testnet"))
            client = OandaClient(
                api_token=credentials.get("api_token"),
                account_id=credentials.get("account_id"),
                practice=practice
            )
            
            # Test by fetching account summary
            return client.test_connection()
            
        elif broker_id == "coinbase":
            # Coinbase validation would go here
            # For now, return mock success
            return {
                "success": True,
                "message": "Coinbase validation not yet implemented",
                "capabilities": ["read", "trade", "spot"]
            }
            
        elif broker_id == "kraken":
            # Kraken validation would go here
            return {
                "success": True,
                "message": "Kraken validation not yet implemented",
                "capabilities": ["read", "trade", "spot"]
            }
            
        elif broker_id == "alpaca":
            # Alpaca validation would go here
            return {
                "success": True,
                "message": "Alpaca validation not yet implemented",
                "capabilities": ["read", "trade", "stocks"]
            }
        
        elif broker_id == "ibkr":
            # IBKR validation
            from app.exchange.ibkr_client import IBKRClientValidator
            gateway_url = credentials.get("gateway_url", "https://localhost:5000")
            validator = IBKRClientValidator(
                gateway_url=gateway_url,
                username=credentials.get("username"),
                password=credentials.get("password"),
                account_id=credentials.get("account_id"),
                environment=environment
            )
            return validator.test_connection()
        
        elif broker_id in ("mt4", "mt5"):
            # MT Bridge validation - proxy to bot-backend
            import httpx
            from os import getenv
            
            bot_backend_url = getenv("BOT_BACKEND_URL", "http://localhost:8000")
            
            try:
                with httpx.Client(timeout=15.0) as client:
                    response = client.post(
                        f"{bot_backend_url}/api/v1/brokers/test-connection",
                        json={
                            "broker_id": broker_id,
                            "bridge_url": credentials.get("bridge_url"),
                            "bridge_token": credentials.get("bridge_token")
                        }
                    )
                    
                    if response.status_code == 200:
                        return response.json()
                    else:
                        return {
                            "success": False,
                            "error": f"Bridge connection failed: {response.text[:200]}"
                        }
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Failed to connect to MT bridge: {str(e)}"
                }
        
        else:
            return {"success": False, "error": f"Unknown broker: {broker_id}"}
            
    except ImportError as e:
        return {"success": False, "error": f"Broker client not available: {str(e)}"}
    except Exception as e:
        return {"success": False, "error": f"Validation error: {str(e)}"}

def list_user_broker_accounts(user_id: str) -> List[Dict[str, Any]]:
    db = get_db()
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT id, broker_id, market_type, label, status, masked_key, last_validated_at, capabilities, environment, created_at, last_error_message
            FROM broker_accounts
            WHERE user_id = ?
            ORDER BY created_at DESC
            """,
            (user_id,)
        ).fetchall()
        
        def _clean_error(msg):
            if not msg: return None
            # Common raw technical errors to map
            m_lower = msg.lower()
            if "null apikey" in m_lower or "api key in http header" in m_lower:
                return "Authentication failed: Invalid API Key or signature."
            if "ip" in m_lower and "whitelist" in m_lower:
                return "Access Denied: IP Address not whitelisted."
            return msg

        def parse_row(r):
            d = dict(r)
            if d.get("capabilities") and isinstance(d["capabilities"], str):
                try: d["capabilities"] = json.loads(d["capabilities"])
                except: pass
            
            # Sanitize displayed error
            if d.get("last_error_message"):
                d["last_error_message"] = _clean_error(d["last_error_message"])
                
            return d
            
        return [parse_row(r) for r in rows]

def get_broker_account(user_id: str, account_id: str) -> Optional[Dict[str, Any]]:
    db = get_db()
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM broker_accounts WHERE id = ? AND user_id = ?",
            (account_id, user_id)
        ).fetchone()
        
        if row:
            d = dict(row)
            if d.get("capabilities") and isinstance(d["capabilities"], str):
                try: d["capabilities"] = json.loads(d["capabilities"])
                except: pass
                
             # Sanitize displayed error inline (duplicating logic for safety or refactor later)
            err = d.get("last_error_message")
            if err:
                 err_lower = err.lower()
                 if "null apikey" in err_lower or "api key in http header" in err_lower:
                     d["last_error_message"] = "Authentication failed: Invalid API Key or signature."
                 elif "ip" in err_lower and "whitelist" in err_lower:
                     d["last_error_message"] = "Access Denied: IP Address not whitelisted."

            return d
        return None

def disconnect_broker_account(user_id: str, account_id: str) -> bool:
    db = get_db()
    with db.connect() as conn:
        # We might want to keep history?
        # For now, HARD DELETE credentials, SOFT DELETE account (or set status 'disconnected')
        
        # 1. Delete credentials (security first)
        conn.execute("DELETE FROM broker_credentials WHERE account_id = ?", (account_id,))
        
        # 2. Update status
        now = utc_now_iso()
        conn.execute("UPDATE broker_accounts SET status = 'disconnected', updated_at = ? WHERE id = ? AND user_id = ?", 
                     (now, account_id, user_id))
                     
        # 3. Soft-delete ALL bot instances that reference this broken broker account so they don't block
        conn.execute(
            """
            UPDATE bot_instances 
            SET status = 'deleted', 
                last_error = 'Linked broker account was disconnected',
                broker_health_status = 'broker_blocked',
                block_category = 'broker_auth_failure',
                block_reason_code = 'broker_not_found',
                updated_at = ?
            WHERE broker_account_id = ? AND status != 'deleted'
            """,
            (now, account_id)
        )
        
        return True

def delete_broker_account_permanently(user_id: str, account_id: str) -> bool:
    """
    Permanently deletes a broker account and its credentials from the database.
    """
    db = get_db()
    with db.connect() as conn:
        # Check ownership
        row = conn.execute("SELECT id FROM broker_accounts WHERE id = ? AND user_id = ?", (account_id, user_id)).fetchone()
        if not row:
            return False
            
        # Hard DELETE credentials
        conn.execute("DELETE FROM broker_credentials WHERE account_id = ?", (account_id,))
        
        # Hard DELETE account
        conn.execute("DELETE FROM broker_accounts WHERE id = ?", (account_id,))
        
        # Soft-delete all active bots executing under this account instead of tearing out history
        now = utc_now_iso()
        conn.execute(
            """
            UPDATE bot_instances 
            SET status = 'deleted', 
                last_error = 'Linked broker account was permanently deleted',
                broker_health_status = 'broker_blocked',
                block_category = 'broker_auth_failure',
                block_reason_code = 'broker_not_found',
                updated_at = ?
            WHERE broker_account_id = ? AND status != 'deleted'
            """,
            (now, account_id)
        )
        
        # Log audit
        _log_audit_event(conn, account_id, user_id, "account_deleted", {})
        
        return True

def get_decrypted_credentials(user_id: str, account_id: str) -> Optional[Dict[str, Any]]:
    """
    Internal helper to retrieve and decrypt credentials.
    Used by proxies to inject credentials into bot-backend requests.
    """
    db = get_db()
    with db.connect() as conn:
        # Check ownership
        row = conn.execute("SELECT broker_id, market_type, environment FROM broker_accounts WHERE id = ? AND user_id = ?", (account_id, user_id)).fetchone()
        if not row:
            return None
        
        market_type = row["market_type"]
        broker_id = row["broker_id"]
        
        cred_row = conn.execute("SELECT encrypted_blob FROM broker_credentials WHERE account_id = ?", (account_id,)).fetchone()
        if not cred_row:
             return None
             
        from shared_lib.core.security.broker_security import decrypt_credentials
        creds = decrypt_credentials(cred_row["encrypted_blob"])
        
        # Add metadata that might be useful for the bot
        creds["broker_id"] = broker_id
        creds["market_type"] = market_type
        creds["account_id"] = account_id # The internal ID
        
        return creds

# ============================================================================
# Private Helpers
# ============================================================================

def _log_audit_event(conn, account_id: str, user_id: str, event_type: str, details: Any):
    details_json = json.dumps(details) if details else None
    conn.execute(
        """
        INSERT INTO broker_audit_log (id, broker_account_id, user_id, event_type, details_json, timestamp_utc)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (f"evt_{uuid.uuid4().hex[:12]}", account_id, user_id, event_type, details_json, utc_now_iso())
    )

def link_ibkr_account(user_id: str, data: Dict[str, Any]) -> List[str]:
    """
    Creates/Updates IBKR accounts discovered from the link flow.
    """
    db = DB()
    now = utc_now_iso()
    accounts = data.get("accounts", [])
    environment = data.get("environment", "live")
    
    if not accounts:
        return []

    created_ids = []
    
    with db.connect() as conn:
        for acc_label in accounts:
            # acc_label is like "DU123456"
            # Check if exists
            # We use the label as the 'masked_key' equivalent identifier for IBKR lookup
            # But wait, masked_key is usually "API...Key".
            # For IBKR, we can store the Account ID in masked_key for display.
            
            # Check existence by broker_id + masked_key (Account ID)
            # Actually weak check, better to check if we have an entry with this label?
            
            row = conn.execute(
                "SELECT id FROM broker_accounts WHERE user_id = ? AND broker_id = 'ibkr' AND label = ?", 
                (user_id, acc_label)
            ).fetchone()
            
            if row:
                # Update existing
                account_id = row[0]
                conn.execute(
                    """
                    UPDATE broker_accounts 
                    SET status = 'connected', environment = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (environment, now, account_id)
                )
                created_ids.append(account_id)
            else:
                # Create new
                account_id = f"brk_{uuid.uuid4().hex[:12]}"
                # Default to forex for now as per requirement context, or generic
                market_type = "forex" 
                
                conn.execute(
                    """
                    INSERT INTO broker_accounts 
                    (id, user_id, broker_id, market_type, label, status, masked_key, environment, created_at, updated_at)
                    VALUES (?, ?, 'ibkr', ?, ?, 'connected', ?, ?, ?, ?)
                    """,
                    (account_id, user_id, market_type, acc_label, acc_label, environment, now, now)
                )
                
                # Also create a dummy credential entry so validations don't fail on missing JOIN
                # We store minimal metadata
                enc_blob = encrypt_credentials({"account_id": acc_label, "environment": environment})
                conn.execute(
                    """
                    INSERT INTO broker_credentials (account_id, encrypted_blob, key_metadata, updated_at)
                    VALUES (?, ?, 'managed_link', ?)
                    ON CONFLICT(account_id) DO NOTHING
                    """,
                    (account_id, enc_blob, now)
                )
                
                created_ids.append(account_id)
                
                _log_audit_event(conn, account_id, user_id, "ibkr_linked", {"ibkr_account": acc_label})

    return created_ids


# ============================================================================
# SEV-1 FIX: Versioned credential reconnect flow (Option A)
# ============================================================================
#
# Design contract:
#   - broker_accounts.id is stable forever; bots always reference it as FK
#   - broker_credentials_v2 grows a new row on every reconnect (version N+1)
#   - broker_accounts.active_credential_version is atomically updated in the
#     same transaction that supersedes the old version
#   - The canonical resolver (shared_lib.broker.resolver) only reads
#     broker_credentials_v2 through the active_credential_version pointer
#   - The health cache is invalidated for all versions on reconnect so that
#     running bots re-resolve within one TTL cycle (≤120 s)
#
# IMPORTANT: validate_broker_credentials_v2() must be called BEFORE
# activate_credential_version() — we never promote an unvalidated credential.
# ============================================================================


def submit_broker_credentials_v2(
    user_id: str,
    account_id: str,
    credentials: Dict[str, Any],
) -> int:
    """
    Store a NEW credential version in broker_credentials_v2 without activating
    it yet.  The account status moves to 'validating' and the caller must
    subsequently call validate_and_activate_credential_v2().

    Returns the new version number.
    """
    db = get_db()
    now = utc_now_iso()

    # Environment MUST come from broker_accounts (authoritative) — NOT from
    # the credential blob.  The blob may contain an `environment` key for
    # cross-check purposes only.  We strip it out to avoid re-introducing the
    # env-mismatch bug that caused this incident.
    with db.connect() as conn:
        acc_row = conn.execute(
            "SELECT broker_id, environment, user_id FROM broker_accounts WHERE id = ? AND user_id = ?",
            (account_id, user_id),
        ).fetchone()
        if not acc_row:
            raise ValueError(f"Broker account {account_id!r} not found or does not belong to user")

        broker_id  = acc_row["broker_id"]
        # Authoritative environment from account row (never from blob)
        account_env = acc_row["environment"] or "live"

        # If the caller included environment in credentials, it MUST match the
        # account row or we refuse early (loud failure > silent mismatch).
        blob_env = credentials.get("environment")
        if blob_env:
            from shared_lib.broker.environment import normalize_environment, BrokerEnvironment
            try:
                norm_blob = normalize_environment(blob_env)
                norm_acct = normalize_environment(account_env)
                if norm_blob != norm_acct:
                    raise ValueError(
                        f"Environment mismatch: account row has environment={account_env!r} "
                        f"but submitted credentials contain environment={blob_env!r}. "
                        f"To change environment you must disconnect and reconnect the account."
                    )
            except ValueError:
                raise

        # KYC gate for IBKR live
        if broker_id == "ibkr" and account_env == "live":
            from app.core.kyc_service import check_kyc_status
            if not check_kyc_status(user_id):
                raise ValueError("KYC verification required for IBKR live trading.")

        # Encrypt
        from shared_lib.core.security.broker_security import encrypt_credentials, mask_credentials
        encrypted_blob = encrypt_credentials(credentials)
        masked         = mask_credentials(credentials)

        api_key = credentials.get("api_key") or credentials.get("key") or ""
        key_fingerprint = f"...{api_key[-4:]}" if len(api_key) >= 4 else "****"

        # Get next version number (MAX + 1, or 1 if no prior rows)
        max_ver_row = conn.execute(
            "SELECT COALESCE(MAX(version), 0) AS max_ver FROM broker_credentials_v2 WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        next_version = (max_ver_row["max_ver"] or 0) + 1

        # Insert new version as 'validating' — NOT active yet
        conn.execute(
            """
            INSERT INTO broker_credentials_v2
                (account_id, version, status, encrypted_blob, key_metadata,
                 key_fingerprint, created_at, updated_at)
            VALUES (?, ?, 'validating', ?, 'fernet_v1', ?, ?, ?)
            """,
            (account_id, next_version, encrypted_blob, key_fingerprint, now, now),
        )

        # Move account to 'validating' and update masked key for display
        conn.execute(
            """
            UPDATE broker_accounts
            SET status = 'validating', masked_key = ?, updated_at = ?
            WHERE id = ?
            """,
            (masked, now, account_id),
        )

        _log_audit_event(conn, account_id, user_id, "credentials_submitted_v2", {
            "version": next_version, "key_fingerprint": key_fingerprint,
        })

    return next_version


def validate_and_activate_credential_v2(
    user_id: str,
    account_id: str,
    version: int,
) -> Dict[str, Any]:
    """
    Validate a pending credential version against the live exchange and, on
    success, atomically:
      1. Mark the new version as 'active'
      2. Mark all prior 'active' versions as 'superseded'
      3. Update broker_accounts.active_credential_version = version
      4. Update broker_accounts.status = 'connected'
      5. Invalidate the broker health cache for this account

    On failure:
      - New version is marked 'invalid'
      - Account status moves to 'invalid'
      - broker_accounts.validation_error is populated
      - Health cache is NOT invalidated (existing healthy bots keep running)

    Returns:
        {"success": bool, "status": str, "error": Optional[str], "version": int}
    """
    db  = get_db()
    now = utc_now_iso()

    with db.connect() as conn:
        # Ownership check
        acc_row = conn.execute(
            "SELECT broker_id, environment, user_id FROM broker_accounts WHERE id = ? AND user_id = ?",
            (account_id, user_id),
        ).fetchone()
        if not acc_row:
            return {"success": False, "error": "Account not found", "version": version}

        broker_id   = acc_row["broker_id"]
        environment = acc_row["environment"] or "live"

        # Fetch the credential row we're promoting
        cred_row = conn.execute(
            """
            SELECT id, encrypted_blob, status
            FROM broker_credentials_v2
            WHERE account_id = ? AND version = ?
            """,
            (account_id, version),
        ).fetchone()
        if not cred_row:
            return {"success": False, "error": f"Credential version {version} not found", "version": version}

        if cred_row["status"] not in ("validating", "active"):
            return {
                "success": False,
                "error": f"Cannot validate credential in status={cred_row['status']!r}",
                "version": version,
            }

        # Decrypt
        from shared_lib.core.security.broker_security import decrypt_credentials
        decrypted = decrypt_credentials(cred_row["encrypted_blob"])
        if not decrypted:
            # Mark invalid immediately
            conn.execute(
                "UPDATE broker_credentials_v2 SET status='invalid', validation_error=?, updated_at=? "
                "WHERE account_id=? AND version=?",
                ("Decryption failed — key mismatch or data corruption", now, account_id, version),
            )
            conn.execute(
                "UPDATE broker_accounts SET status='invalid', validation_error=?, updated_at=? WHERE id=?",
                ("Credential decryption failed. Re-submit credentials.", now, account_id),
            )
            return {"success": False, "error": "Decryption failed", "version": version}

        # Live exchange validation
        validation = _test_broker_connection(broker_id, decrypted, environment)

        if not validation["success"]:
            err_msg = validation.get("error", "Unknown validation error")
            conn.execute(
                "UPDATE broker_credentials_v2 SET status='invalid', validation_error=?, "
                "last_validated_at=?, updated_at=? WHERE account_id=? AND version=?",
                (err_msg, now, now, account_id, version),
            )
            conn.execute(
                "UPDATE broker_accounts SET status='invalid', last_error_message=?, "
                "validation_error=?, updated_at=? WHERE id=?",
                (err_msg, err_msg, now, account_id),
            )
            _log_audit_event(conn, account_id, user_id, "credentials_validation_failed_v2", {
                "version": version, "error": err_msg,
            })
            return {"success": False, "status": "invalid", "error": err_msg, "version": version}

        # ── SUCCESS PATH — atomic promotion ──────────────────────────────────

        # 1. Supersede all currently-active versions for this account
        conn.execute(
            """
            UPDATE broker_credentials_v2
            SET status = 'superseded', superseded_at = ?, updated_at = ?
            WHERE account_id = ? AND version != ? AND status = 'active'
            """,
            (now, now, account_id, version),
        )

        # 2. Activate the new version
        capabilities = validation.get("capabilities", ["read", "trade"])
        conn.execute(
            """
            UPDATE broker_credentials_v2
            SET status = 'active', last_validated_at = ?, validation_error = NULL, updated_at = ?
            WHERE account_id = ? AND version = ?
            """,
            (now, now, account_id, version),
        )

        # 3. Point broker_accounts at the new version, mark connected
        conn.execute(
            """
            UPDATE broker_accounts
            SET active_credential_version = ?,
                status                    = 'connected',
                capabilities              = ?,
                last_validated_at         = ?,
                last_error_message        = NULL,
                validation_error          = NULL,
                updated_at                = ?
            WHERE id = ?
            """,
            (version, json.dumps(capabilities), now, now, account_id),
        )

        _log_audit_event(conn, account_id, user_id, "credentials_activated_v2", {
            "version": version, "capabilities": capabilities,
        })

    # 4. Invalidate health cache so running bots re-resolve within one TTL
    try:
        from shared_lib.broker import get_broker_health_cache
        get_broker_health_cache().invalidate(account_id)
    except Exception:
        pass  # Health cache is best-effort; activation already committed

    return {
        "success": True,
        "status":  "connected",
        "version": version,
        "capabilities": capabilities,
    }


def reconnect_broker_account(
    user_id: str,
    account_id: str,
    credentials: Dict[str, Any],
) -> Dict[str, Any]:
    """
    High-level reconnect: submit new credentials → validate → activate atomically.

    This is the primary entry point for the "reconnect" flow in the API.
    It is equivalent to submit_broker_credentials_v2() + validate_and_activate_credential_v2()
    but wrapped as a single call for callers that don't need the two-step flow.

    Returns:
        {"success": bool, "status": str, "version": int, "error": Optional[str]}
    """
    version = submit_broker_credentials_v2(user_id, account_id, credentials)
    return validate_and_activate_credential_v2(user_id, account_id, version)
