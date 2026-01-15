import base64
import json
import os
from typing import Dict, Any, Optional
from cryptography.fernet import Fernet

# Generally, this key should come from environment variables.
# If not present, we will generate one (in memory) which means restarts invalidate keys 
# if not persisted. For this environment, we'll try to use a deterministic key 
# based on a Secret if available, or fallback to a hardcoded specific key for dev persistence.
#
# IMPORTANT: IN PRODUCTION, SET 'BROKER_SECRET_KEY' ENV VAR.

_SECRET_KEY_ENV = os.getenv("BROKER_SECRET_KEY")

# Fallback dev key (32 url-safe base64-encoded bytes)
# This ensures that between restarts in this dev env, we can still decrypt.
# Only use this if env var is missing.
_DEV_KEY = b'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY=' 

def _get_fernet_key() -> bytes:
    """
    Get or generate a Fernet-compatible encryption key.
    Fernet requires a 32-byte key that is base64url-encoded.
    """
    if _SECRET_KEY_ENV:
        # If user provided a key, try to use it
        key_bytes = _SECRET_KEY_ENV.encode() if isinstance(_SECRET_KEY_ENV, str) else _SECRET_KEY_ENV
        # Check if it's already a valid Fernet key
        try:
            from cryptography.fernet import Fernet
            Fernet(key_bytes)
            return key_bytes
        except:
            # Not a valid Fernet key, derive one
            pass
    
    # Try to use settings.SECRET_KEY
    try:
        from app.core.config import settings
        if hasattr(settings, "SECRET_KEY") and settings.SECRET_KEY:
            # Derive a Fernet key from SECRET_KEY
            import hashlib
            m = hashlib.sha256()
            m.update(settings.SECRET_KEY.encode())
            # Take the 32-byte hash directly and base64url encode it
            return base64.urlsafe_b64encode(m.digest())
    except ImportError:
        pass
        
    # Fallback to insecure but deterministic key for development
    # This allows credentials to persist across restarts in dev
    return base64.urlsafe_b64encode(b"0" * 32)

def encrypt_credentials(credentials: Dict[str, Any]) -> str:
    """
    Encrypts dictionary of credentials into a string blob using Fernet.
    """
    key = _get_fernet_key()
    f = Fernet(key)
    json_str = json.dumps(credentials)
    encrypted_bytes = f.encrypt(json_str.encode("utf-8"))
    return encrypted_bytes.decode("utf-8")

def decrypt_credentials(encrypted_blob: str) -> Dict[str, Any]:
    """
    Decrypts the blob back into a dictionary using Fernet.
    """
    try:
        key = _get_fernet_key()
        f = Fernet(key)
        decrypted_bytes = f.decrypt(encrypted_blob.encode("utf-8"))
        return json.loads(decrypted_bytes.decode("utf-8"))
    except Exception:
        # Decryption failed (key rotation? data corruption?)
        return {}

def mask_credentials(credentials: Dict[str, Any]) -> str:
    """
    Returns a masked version of the primary key for display (e.g., '...a1b2')
    """
    # Try different common key names
    # MT5 might have 'login'
    login = credentials.get("login")
    if login:
        return str(login) # For MT5 login is public usually
        
    key = credentials.get("api_key") or credentials.get("public_key") or credentials.get("key") or credentials.get("ClientId")
    
    if key and isinstance(key, str) and len(key) > 8:
        return f"...{key[-4:]}"
    elif key:
        return "***"
    
    return "..."
