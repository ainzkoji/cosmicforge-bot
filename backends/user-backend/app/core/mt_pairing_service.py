"""
MT Bridge Pairing Service
Handles pairing code generation, session management, and bridge credential storage.
"""
import uuid
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

from shared_lib.persistence.db import DB
from shared_lib.core.security.broker_security import encrypt_credentials, decrypt_credentials

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def utc_now_iso() -> str:
    return utc_now().isoformat()

# ============================================================================
# Pairing Code Generation
# ============================================================================

def generate_pairing_code() -> str:
    """
    Generate a unique 8-character alphanumeric pairing code.
    Format: XXXX-YYYY (easier to read)
    """
    chars = string.ascii_uppercase + string.digits
    # Exclude confusing characters
    chars = chars.replace('O', '').replace('0', '').replace('I', '').replace('1', '')
    
    code = ''.join(random.choice(chars) for _ in range(8))
    # Format as XXXX-YYYY
    return f"{code[:4]}-{code[4:]}"

def generate_connector_link_token() -> str:
    """
    Generate a secure one-time connector link token.
    32 characters (128 bits of entropy) for security.
    """
    import secrets
    return secrets.token_urlsafe(32)

# ============================================================================
# Session Management
# ============================================================================

import hashlib

def create_pairing_session(user_id: str, broker_id: str, environment: str = "live") -> Dict[str, Any]:
    """
    Create a new MT pairing session.
    
    Returns:
        {
            "pairing_code": "ABCD-EFGH",
            "expires_at": "2024-12-25T12:00:00Z",
            "session_id": "uuid",
            "status": "pending",
            "instructions": ...
        }
    
    Raises:
        ValueError: If rate limit exceeded
    """
    db = DB()
    
    # Rate limit: 5 pending sessions per user
    with db.connect() as conn:
        pending_count = conn.execute(
            """
            SELECT COUNT(*) FROM mt_pairing_sessions 
            WHERE user_id = ? AND status = 'pending' AND expires_at > ?
            """,
            (user_id, utc_now_iso())
        ).fetchone()[0]
        
        if pending_count >= 5:
            raise ValueError("Rate limit exceeded: Maximum 5 pending pairing sessions allowed")
    
    # Generate unique pairing code and connector link token
    pairing_code = generate_pairing_code()
    connector_link_token = generate_connector_link_token()
    session_id = str(uuid.uuid4())
    expires_at = utc_now() + timedelta(minutes=10)
    
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO mt_pairing_sessions 
            (id, user_id, broker_id, environment, pairing_code, connector_link_token, expires_at, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            """,
            (session_id, user_id, broker_id, environment, pairing_code, connector_link_token, expires_at.isoformat(), utc_now_iso(), utc_now_iso())
        )
    
    return {
        "pairing_code": pairing_code,
        "connector_link_token": connector_link_token,
        "expires_at": expires_at.isoformat(),
        "session_id": session_id,
        "status": "pending",
        "setup_link": f"cosmicforge://mt-connect?token={connector_link_token}"
    }

def get_pairing_session(pairing_code: str, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Get pairing session by code.
    
    Args:
        pairing_code: The pairing code
        user_id: Optional user_id filter (for polling endpoint)
    
    Returns:
        Session dict or None if not found
    """
    db = DB()
    
    with db.connect() as conn:
        if user_id:
            row = conn.execute(
                "SELECT * FROM mt_pairing_sessions WHERE pairing_code = ? AND user_id = ?",
                (pairing_code, user_id)
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM mt_pairing_sessions WHERE pairing_code = ?",
                (pairing_code,)
            ).fetchone()
        
        if not row:
            return None
        
        session = dict(row)
        
        # Check expiration
        expires_at = datetime.fromisoformat(session["expires_at"])
        if utc_now() > expires_at and session["status"] == "pending":
            # Mark as expired
            conn.execute(
                "UPDATE mt_pairing_sessions SET status = 'expired', updated_at = ? WHERE id = ?",
                (utc_now_iso(), session["id"])
            )
            session["status"] = "expired"
        
        # Format response for polling
        result = {
            "status": session["status"],
            "broker_id": session["broker_id"],
            "environment": session.get("environment", "live"),
            "expires_at": session["expires_at"],
            "account": None
        }
        
        if session["status"] == "paired":
            result["account"] = {
                "login": session.get("account_login"), # New column name
                "server": session.get("account_server"), # New column name
                "platform": session.get("account_platform"),
                "currency": session.get("account_currency")
            }
            # Fallback for old schema if columns missing (should be handled by migration but safety first)
            if not result["account"]["login"]:
                 result["account"]["login"] = session.get("paired_account_login")
            if not result["account"]["server"]:
                 result["account"]["server"] = session.get("paired_server")
        
        return result

def get_session_by_connector_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Get pairing session by connector_link_token.
    Used for magic link authentication flow.
    
    Args:
        token: The connector_link_token from the setup link
    
    Returns:
        Session dict or None if not found
    """
    db = DB()
    
    with db.connect() as conn:
        row = conn.execute(
            "SELECT * FROM mt_pairing_sessions WHERE connector_link_token = ?",
            (token,)
        ).fetchone()
        
        if not row:
            return None
        
        session = dict(row)
        
        # Check expiration
        expires_at = datetime.fromisoformat(session["expires_at"])
        if utc_now() > expires_at and session["status"] == "pending":
            # Mark as expired
            conn.execute(
                "UPDATE mt_pairing_sessions SET status = 'expired', updated_at = ? WHERE id = ?",
                (utc_now_iso(), session["id"])
            )
            session["status"] = "expired"
        
        return session
    
def claim_pairing_session(session_id: str, device_secret: str) -> Dict[str, Any]:
    """
    Claim pairing code for a session.
    """
    db = DB()
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM mt_pairing_sessions WHERE id = ?", (session_id,)).fetchone()
        if not row:
            raise ValueError("Invalid session ID")
        
        session = dict(row)
        if session["status"] != "pending":
            raise ValueError(f"Session is {session['status']}")
            
        expires_at = datetime.fromisoformat(session["expires_at"])
        if utc_now() > expires_at:
            conn.execute("UPDATE mt_pairing_sessions SET status = 'expired' WHERE id = ?", (session_id,))
            raise ValueError("Session expired")
            
        return {
            "pairing_code": session["pairing_code"],
            "broker_id": session["broker_id"],
            "environment": session.get("environment", "live")
        }

def finish_pairing(session_id: str, user_id: str) -> str:
    """
    Finalize pairing: Create broker account from verified session.
    """
    db = DB()
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM mt_pairing_sessions WHERE id = ? AND user_id = ?", (session_id, user_id)).fetchone()
        if not row:
            raise ValueError("Session not found")
        
        session = dict(row)
        if session["status"] != "paired":
             # If already completed, maybe return the existing account_id if we tracked it?
             # For now, strict check.
             raise ValueError("Connector not yet linked. Please complete the installation step." if session["status"] == "pending" else f"Session is {session['status']}")

        # Retrieve stored details
        mt_platform = session["account_platform"]
        account_login = session["account_login"]
        server = session["account_server"]
        environment = session.get("environment", "live")
        
        # Create broker account
        from app.core.broker_service import create_broker_account_draft, submit_broker_credentials
        
        account_id = create_broker_account_draft(
            user_id=user_id,
            broker_id=mt_platform,
            market_type="forex",
            label=f"{mt_platform.upper()} - {account_login}"
        )
        
        # Decrypt token to re-submit or use internal method if we had one.
        # But submit_broker_credentials likely encrypts it again.
        # We need the raw token.
        encrypted_token = session["encrypted_bridge_token"]
        # Assuming decrypt_credentials returns dict
        decrypted = decrypt_credentials(encrypted_token)
        bridge_token = decrypted.get("bridge_token")
        
        credentials = {
            "bridge_url": session["bridge_url"],
            "bridge_token": bridge_token,
            "tls_mode": session.get("tls_mode", "insecure"),
            "account_label": f"{account_login} @ {server}",
            "environment": environment,
            "account_fingerprint": session.get("account_fingerprint")
        }
        
        submit_broker_credentials(user_id, account_id, credentials)
        
        # Mark as completed
        conn.execute("UPDATE mt_pairing_sessions SET status = 'completed', updated_at = ? WHERE id = ?", (utc_now_iso(), session_id))
        
        return account_id

def complete_pairing(
    pairing_code: str,
    bridge_url: str,
    bridge_token: str,
    tls_mode: str,
    mt_platform: str,
    account_login: str,
    server: str,
    account_currency: str = "USD",
    account_type: str = "Demo"
) -> str:
    """
    Complete the pairing process.
    
    Args:
        pairing_code: The pairing code from user
        bridge_url: Bridge URL (https://vps:8443)
        bridge_token: Bridge API token
        tls_mode: "strict" or "insecure"
        mt_platform: "mt4" or "mt5"
        account_login: MT account number
        server: MT server name
        account_currency: Account currency
        account_type: Account type (Demo/Real)
    
    Returns:
        account_id: The created broker account ID
    
    Raises:
        ValueError: If pairing code invalid, expired, or already used
    """
    db = DB()
    
    with db.connect() as conn:
        # Get session
        session_row = conn.execute(
            "SELECT * FROM mt_pairing_sessions WHERE pairing_code = ?",
            (pairing_code,)
        ).fetchone()
        
        if not session_row:
            raise ValueError("Invalid pairing code")
        
        session = dict(session_row)
        
        # Validate session
        if session["status"] != "pending":
            raise ValueError(f"Pairing code already {session['status']}")
        
        expires_at = datetime.fromisoformat(session["expires_at"])
        if utc_now() > expires_at:
            conn.execute(
                "UPDATE mt_pairing_sessions SET status = 'expired', updated_at = ? WHERE id = ?",
                (utc_now_iso(), session["id"])
            )
            raise ValueError("Pairing code has expired")
        
        # Validate broker_id matches
        if session["broker_id"] != mt_platform:
            raise ValueError(f"Platform mismatch: expected {session['broker_id']}, got {mt_platform}")
        
        user_id = session["user_id"]
        environment = session.get("environment", "live")
        
        # Create fingerprint
        account_fingerprint = hashlib.sha256(f"{account_login}:{server}:{mt_platform}".encode()).hexdigest()
        
        # Encrypt token for pairing session record
        encrypted_token = encrypt_credentials({"bridge_token": bridge_token})
        
        # Update pairing session to PAIRED (connected)
        # We do NOT create the broker account yet. That happens in finish_pairing.
        conn.execute(
            """
            UPDATE mt_pairing_sessions 
            SET status = 'paired',
                account_login = ?,
                account_server = ?,
                account_currency = ?,
                account_type = ?,
                account_platform = ?,
                account_fingerprint = ?,
                bridge_url = ?,
                encrypted_bridge_token = ?,
                tls_mode = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                account_login, server, account_currency, account_type, mt_platform, 
                account_fingerprint, bridge_url, encrypted_token, tls_mode, 
                utc_now_iso(), session["id"]
            )
        )
        
        return session["id"] # Return session ID or strict None, but caller (mt_pairing.py) might not use it anymore since we changed endpoint return type? 
        # Wait, complete_pairing endpoint in mt_pairing check return.
        # mt_pairing.py: complete_pairing calls this and returns CompletePairingResponse with account_id.
        # We should change mt_pairing_py's complete_pairing to NOT return account_id or return None.
        # But wait, the Connector calls complete_pairing. It expects "ok".
        # So we should return something indicating success. The session_id is fine.

def get_session_by_id(session_id: str, user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get pairing session by ID.
    """
    db = DB()
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM mt_pairing_sessions WHERE id = ? AND user_id = ?", (session_id, user_id)).fetchone()
        if not row:
            return None
        
        session = dict(row)
        
        # Check expiration
        expires_at = datetime.fromisoformat(session["expires_at"])
        if utc_now() > expires_at and session["status"] == "pending":
            conn.execute("UPDATE mt_pairing_sessions SET status = 'expired', updated_at = ? WHERE id = ?", (utc_now_iso(), session["id"]))
            session["status"] = "expired"
        
        # Consistent return format for status endpoint
        result = {
            "status": session["status"],
            "broker_id": session["broker_id"],
            "environment": session.get("environment", "live"),
            "expires_at": session["expires_at"],
            "account": None
        }
        
        if session["status"] == "paired":
            result["account"] = {
                "login": session.get("account_login"),
                "server": session.get("account_server"),
                "platform": session.get("account_platform"),
                "currency": session.get("account_currency")
            }
            # Fallback for old schema if columns missing
            if not result["account"]["login"]:
                 result["account"]["login"] = session.get("paired_account_login")
            if not result["account"]["server"]:
                 result["account"]["server"] = session.get("paired_server")
                 
        return result

def cleanup_expired_sessions():
    """
    Background task to mark expired sessions.
    Should be called periodically (e.g., every 5 minutes).
    """
    db = DB()
    
    with db.connect() as conn:
        conn.execute(
            """
            UPDATE mt_pairing_sessions 
            SET status = 'expired', updated_at = ?
            WHERE status = 'pending' AND expires_at < ?
            """,
            (utc_now_iso(), utc_now_iso())
        )
        
        rows_updated = conn.total_changes
        return rows_updated
