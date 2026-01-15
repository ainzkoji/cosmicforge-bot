import uuid
import json
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from app.persistence.db import DB
from app.core.broker_security import encrypt_credentials, mask_credentials

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

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
        
        is_available = broker["id"] == "binance"
        
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
    db = DB()
    account_id = f"brk_{uuid.uuid4().hex[:12]}"
    now = utc_now_iso()
    
    if not label:
        broker_info = get_broker_details(broker_id)
        label = f"{broker_info['name'] if broker_info else broker_id} Account"

    # Enforce Entitlements
    from app.core import billing_service
    sub = billing_service.get_user_subscription(user_id)
    max_brokers = sub["entitlements"].get("max_brokers", 1)
    
    # Count existing (active) accounts
    with db.connect() as conn:
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
    db = DB()
    now = utc_now_iso()
    
    # Extract environment if present (metadata), remove from blob if desired, or keep it.
    # Usually we want environment in the table for easy querying.
    environment = credentials.get("environment", "live")
    
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
    """
    db = DB()
    now = utc_now_iso()
    
    with db.connect() as conn:
        # Fetch account details
        row = conn.execute("SELECT * FROM broker_accounts WHERE id = ? AND user_id = ?", (account_id, user_id)).fetchone()
        if not row:
            return {"success": False, "error": "Account not found"}
        
        account = {key: row[key] for key in row.keys()}
        broker_id = account["broker_id"]
        
        # Fetch encrypted credentials
        cred_row = conn.execute("SELECT encrypted_blob FROM broker_credentials WHERE account_id = ?", (account_id,)).fetchone()
        if not cred_row:
            return {"success": False, "error": "No credentials found"}
        
        # Decrypt credentials
        from app.core.broker_security import decrypt_credentials
        credentials = decrypt_credentials(cred_row["encrypted_blob"])
        
        # Test connection based on broker type
        validation_result = _test_broker_connection(broker_id, credentials, account.get("environment", "live"))
        
        if validation_result["success"]:
            new_status = "connected"
            capabilities = validation_result.get("capabilities", ["read", "trade"])
            error_msg = None
        else:
            new_status = "restricted"
            capabilities = []
            error_msg = validation_result.get("error", "Unknown validation error")
            
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
            (new_status, json.dumps(capabilities) if capabilities else None, now, error_msg, now, account_id)
        )
        
        _log_audit_event(conn, account_id, user_id, "validation_completed", {"success": validation_result["success"], "status": new_status})
        
        return {
            "success": validation_result["success"],
            "status": new_status,
            "capabilities": capabilities,
            "error": error_msg
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
            from app.exchange.bybit_client import ByBitClient
            testnet = (environment == "demo" or environment == "testnet")
            client = ByBitClient(
                api_key=credentials.get("api_key"),
                api_secret=credentials.get("api_secret"),
                testnet=testnet
            )
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
        
        else:
            return {"success": False, "error": f"Unknown broker: {broker_id}"}
            
    except ImportError as e:
        return {"success": False, "error": f"Broker client not available: {str(e)}"}
    except Exception as e:
        return {"success": False, "error": f"Validation error: {str(e)}"}

def list_user_broker_accounts(user_id: str) -> List[Dict[str, Any]]:
    db = DB()
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT id, broker_id, market_type, label, status, masked_key, last_validated_at, capabilities, environment, created_at
            FROM broker_accounts
            WHERE user_id = ?
            ORDER BY created_at DESC
            """,
            (user_id,)
        ).fetchall()
        
        def parse_row(r):
            d = dict(r)
            if d.get("capabilities") and isinstance(d["capabilities"], str):
                try: d["capabilities"] = json.loads(d["capabilities"])
                except: pass
            return d
            
        return [parse_row(r) for r in rows]

def get_broker_account(user_id: str, account_id: str) -> Optional[Dict[str, Any]]:
    db = DB()
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
            return d
        return None

def disconnect_broker_account(user_id: str, account_id: str) -> bool:
    db = DB()
    with db.connect() as conn:
        # We might want to keep history?
        # For now, HARD DELETE credentials, SOFT DELETE account (or set status 'disconnected')
        
        # 1. Delete credentials (security first)
        conn.execute("DELETE FROM broker_credentials WHERE account_id = ?", (account_id,))
        
        # 2. Update status
        conn.execute("UPDATE broker_accounts SET status = 'disconnected', updated_at = ? WHERE id = ? AND user_id = ?", 
                     (utc_now_iso(), account_id, user_id))
        
        return True

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
