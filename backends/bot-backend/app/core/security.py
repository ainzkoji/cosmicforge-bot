"""
Security Module (Bot-Backend Subset)
- JWT validation (decode_token)
- Credential encryption helper (retained if needed, or could move to shared)

User management, password hashing, and OTP logic have been removed as they belong in user-backend.
"""
from typing import Optional
from jose import jwt, JWTError
from cryptography.fernet import Fernet
import base64

from app.core.config import settings


# --- Constants ---
ISSUER = "cosmicforge-user-backend"
AUDIENCE = "cosmicforge-services"


# --- JWT Tokens ---

import logging
from jose import jwt, JWTError, ExpiredSignatureError
from jose.exceptions import JWTClaimsError

logger = logging.getLogger(__name__)

def decode_token(token: str) -> Optional[dict]:
    """Decode and validate a JWT token. Returns payload or None if invalid."""
    try:
        # Debugging: Use logger to ensure output is seen
        logger.warning(f"[AUTH DEBUG] Validating token: {token[:10]}... key_prefix={settings.SECRET_KEY[:5]}")
        logger.warning(f"[AUTH DEBUG] Expected Audience: {AUDIENCE}, Issuer: {ISSUER}")
        
        # Enforce issuer and audience
        payload = jwt.decode(
            token, 
            settings.SECRET_KEY, 
            algorithms=[settings.ALGORITHM],
            issuer=ISSUER,
            audience=AUDIENCE,
            options={
                "require": ["iss", "aud", "exp", "sub"]
            }
        )
        logger.warning(f"[AUTH DEBUG] Token valid! User: {payload.get('sub')}")
        return payload
    except ExpiredSignatureError:
        logger.warning("[AUTH] Token validation failed: Signature has expired.")
        return None
    except JWTClaimsError as e:
        logger.warning(f"[AUTH] Token validation failed: Invalid claims. {str(e)}")
        return None
    except JWTError as e:
        logger.warning(f"[AUTH] Token validation failed: Invalid token. {str(e)}")
        return None


# --- Credential Encryption (Fernet) ---
# Retaining this if bot-backend has other encryption needs distinct from 'broker_security'
# If unused, this can also be removed.

def _get_fernet() -> Fernet:
    key_bytes = settings.CREDENTIAL_KEY.encode()[:32].ljust(32, b'\0')
    key_b64 = base64.urlsafe_b64encode(key_bytes)
    return Fernet(key_b64)


def encrypt_credential(text: str) -> str:
    if not text:
        return ""
    f = _get_fernet()
    return f.encrypt(text.encode()).decode()


def decrypt_credential(encrypted: str) -> str:
    if not encrypted:
        return ""
    f = _get_fernet()
    return f.decrypt(encrypted.encode()).decode()
