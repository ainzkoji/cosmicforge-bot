"""
MT Bridge Pairing Service
Handles pairing code generation, session management, and bridge credential storage.
"""
import hashlib
import hmac
import ipaddress
import secrets
import socket
import sqlite3
import uuid
import string
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, Optional, Dict, Any
from urllib.parse import urlsplit

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
    
    # The pairing code is a credential: it must come from the OS CSPRNG, never
    # from the predictable ``random`` module.
    code = ''.join(secrets.choice(chars) for _ in range(8))
    # Format as XXXX-YYYY
    return f"{code[:4]}-{code[4:]}"

def generate_connector_link_token() -> str:
    """
    Generate a secure one-time connector link token.
    32 characters (128 bits of entropy) for security.
    """
    return secrets.token_urlsafe(32)

# ============================================================================
# Connector device binding
# ============================================================================

DEVICE_SECRET_MIN_LENGTH = 16
DEVICE_SECRET_MAX_LENGTH = 256


def _hash_device_secret(device_secret: str) -> str:
    return hashlib.sha256(("mt-connector-device:" + device_secret).encode("utf-8")).hexdigest()


def _ensure_claim_columns(conn) -> None:
    """Add the device-binding columns to ``mt_pairing_sessions`` if absent (idempotent)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(mt_pairing_sessions)").fetchall()}
    if not cols:
        return  # table does not exist; the caller's query will report that
    for column in ("device_secret_hash", "connector_claimed_at"):
        if column in cols:
            continue
        try:
            conn.execute(f"ALTER TABLE mt_pairing_sessions ADD COLUMN {column} TEXT")
        except sqlite3.OperationalError as exc:
            # Another request added it between the check above and the ALTER.
            if "duplicate column name" not in str(exc).lower():
                raise


# ============================================================================
# Bridge URL validation
# ============================================================================

# Cloud metadata endpoints that are not inside the link-local range.
_METADATA_ADDRESSES = frozenset({
    ipaddress.ip_address("100.100.100.200"),   # Alibaba Cloud
    ipaddress.ip_address("192.0.0.192"),       # Oracle Cloud
    ipaddress.ip_address("fd00:ec2::254"),     # AWS IMDS over IPv6
})
_METADATA_HOSTNAMES = frozenset({
    "metadata", "metadata.google.internal", "metadata.goog", "instance-data",
    "instance-data.ec2.internal",
})
_INTERNAL_HOST_SUFFIXES = (".localhost", ".local", ".internal", ".intranet", ".lan", ".home.arpa")


def _is_production() -> bool:
    try:
        from shared_lib.core.security.broker_security import is_production
        return bool(is_production())
    except Exception:
        # If the environment cannot be determined, behave as production.
        return True


def _unmap(ip):
    mapped = getattr(ip, "ipv4_mapped", None)
    return mapped if mapped is not None else ip


def _is_link_local_or_metadata(ip) -> bool:
    ip = _unmap(ip)
    return ip.is_link_local or ip in _METADATA_ADDRESSES


def _is_non_public(ip) -> bool:
    ip = _unmap(ip)
    return (
        ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast
        or ip.is_reserved or ip.is_unspecified or ip in _METADATA_ADDRESSES
        or not ip.is_global
    )


def _default_resolver(host: str, port: int) -> Iterable[str]:
    infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    return [info[4][0] for info in infos]


def validate_bridge_url(
    bridge_url: str,
    *,
    production: Optional[bool] = None,
    resolver: Optional[Callable[[str, int], Iterable[str]]] = None,
) -> str:
    """Validate a connector-supplied bridge URL before it is attached to an account.

    The platform later makes authenticated requests to this URL, so it must
    never point at the platform's own network.

    * All environments: well-formed http(s) URL, no embedded credentials, no
      link-local / cloud-metadata address or hostname.
    * Production: HTTPS only, and no loopback / private / reserved address --
      neither as a literal nor as what the hostname currently resolves to.

    Returns the normalized URL. Raises ``ValueError`` with a user-safe message.
    """
    if production is None:
        production = _is_production()

    if not isinstance(bridge_url, str):
        raise ValueError("bridge_url is invalid")
    url = bridge_url.strip()
    if not url or len(url) > 2048 or any(ch.isspace() or ord(ch) < 32 for ch in url):
        raise ValueError("bridge_url is invalid")

    try:
        parts = urlsplit(url)
        host = parts.hostname
        port = parts.port
    except ValueError:
        raise ValueError("bridge_url is invalid")

    scheme = (parts.scheme or "").lower()
    if scheme not in ("https", "http"):
        raise ValueError("bridge_url must use HTTPS")
    if production and scheme != "https":
        raise ValueError("bridge_url must use HTTPS")
    if not host:
        raise ValueError("bridge_url is invalid")
    if parts.username is not None or parts.password is not None:
        raise ValueError("bridge_url must not contain credentials")
    if parts.fragment:
        raise ValueError("bridge_url must not contain a fragment")

    host = host.rstrip(".").lower()
    if not host:
        raise ValueError("bridge_url is invalid")

    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None

    if literal is not None:
        if _is_link_local_or_metadata(literal):
            raise ValueError("bridge_url must not point to a link-local or metadata address")
        if production and _is_non_public(literal):
            raise ValueError("bridge_url must be a public address")
    else:
        if host in _METADATA_HOSTNAMES:
            raise ValueError("bridge_url must not point to a link-local or metadata address")
        if production:
            if host == "localhost" or host.endswith(_INTERNAL_HOST_SUFFIXES) or "." not in host:
                raise ValueError("bridge_url must be a public address")
            try:
                addresses = list((resolver or _default_resolver)(host, port or 443))
            except Exception:
                raise ValueError("bridge_url host could not be resolved")
            if not addresses:
                raise ValueError("bridge_url host could not be resolved")
            for address in addresses:
                try:
                    resolved = ipaddress.ip_address(str(address).split("%", 1)[0])
                except ValueError:
                    raise ValueError("bridge_url host could not be resolved")
                if _is_non_public(resolved):
                    raise ValueError("bridge_url must be a public address")

    return url


# ============================================================================
# Session Management
# ============================================================================

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

    The connector generates ``device_secret`` itself and presents it here.
    The first valid claim binds the session to that secret (only its hash is
    stored); every later claim must present the same secret, compared in
    constant time. The pairing code is therefore released to exactly one
    connector instance, and ``complete_pairing`` refuses codes that were never
    released through a claim.
    """
    if not isinstance(session_id, str) or not session_id or len(session_id) > 128:
        raise ValueError("Invalid session ID")
    if (
        not isinstance(device_secret, str)
        or not (DEVICE_SECRET_MIN_LENGTH <= len(device_secret) <= DEVICE_SECRET_MAX_LENGTH)
    ):
        raise ValueError("Invalid device secret")

    presented_hash = _hash_device_secret(device_secret)
    expired = False

    db = DB()
    with db.connect() as conn:
        _ensure_claim_columns(conn)
        row = conn.execute("SELECT * FROM mt_pairing_sessions WHERE id = ?", (session_id,)).fetchone()
        if not row:
            raise ValueError("Invalid session ID")
        
        session = dict(row)
        if session["status"] != "pending":
            raise ValueError(f"Session is {session['status']}")
            
        expires_at = datetime.fromisoformat(session["expires_at"])
        if utc_now() > expires_at:
            conn.execute(
                "UPDATE mt_pairing_sessions SET status = 'expired', updated_at = ? WHERE id = ?",
                (utc_now_iso(), session_id)
            )
            expired = True
        else:
            stored_hash = session.get("device_secret_hash")
            if stored_hash:
                if not hmac.compare_digest(str(stored_hash), presented_hash):
                    raise ValueError("Session already claimed by another connector")
            else:
                # First claim: bind atomically (a concurrent claim cannot also win).
                cur = conn.execute(
                    """
                    UPDATE mt_pairing_sessions
                    SET device_secret_hash = ?, connector_claimed_at = ?, updated_at = ?
                    WHERE id = ? AND status = 'pending' AND device_secret_hash IS NULL
                    """,
                    (presented_hash, utc_now_iso(), utc_now_iso(), session_id)
                )
                if cur.rowcount != 1:
                    raise ValueError("Session already claimed by another connector")

    # Raised after the transaction so the 'expired' status is committed.
    if expired:
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

        # Defence in depth: the stored bridge URL is validated again before it
        # becomes a broker account the platform will connect to.
        validate_bridge_url(session.get("bridge_url") or "")

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
    if not isinstance(pairing_code, str) or not pairing_code or len(pairing_code) > 32:
        raise ValueError("Invalid pairing code")

    # Never attach an unvalidated, caller-supplied URL to an account.
    bridge_url = validate_bridge_url(bridge_url)

    db = DB()
    expired = False
    
    with db.connect() as conn:
        _ensure_claim_columns(conn)
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
            expired = True

    # Raised after the transaction so the 'expired' status is committed.
    if expired:
        raise ValueError("Pairing code has expired")

    with db.connect() as conn:
        # Proof of possession: the code must have been released to a connector
        # through /connector/claim (which binds the session to that device).
        if not session.get("device_secret_hash"):
            raise ValueError("Invalid pairing code")

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
        # Single use: the transition pending -> paired happens at most once,
        # even if two requests race with the same code.
        cur = conn.execute(
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
            WHERE id = ? AND status = 'pending'
            """,
            (
                account_login, server, account_currency, account_type, mt_platform, 
                account_fingerprint, bridge_url, encrypted_token, tls_mode, 
                utc_now_iso(), session["id"]
            )
        )
        if cur.rowcount != 1:
            raise ValueError("Pairing code already used")
        
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
