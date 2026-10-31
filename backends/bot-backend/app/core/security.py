"""
Security Module (Bot-Backend Subset)
- JWT validation (decode_token)

User management, password hashing, OTP logic and the legacy credential
encryption helpers have been removed: they belong in user-backend /
shared_lib.core.security.broker_security.
"""
from typing import Optional
from shared_lib.core.security.jwt_tokens import jwt, JWTError  # PyJWT (F6)

from app.core.config import settings


# --- Constants ---
ISSUER = "cosmicforge-user-backend"
AUDIENCE = "cosmicforge-services"


# --- JWT Tokens ---

import logging
from shared_lib.core.security.jwt_tokens import jwt, JWTError, ExpiredSignatureError
from shared_lib.core.security.jwt_tokens import JWTClaimsError

logger = logging.getLogger(__name__)

def decode_token(token: str) -> Optional[dict]:
    """Decode and validate a JWT token. Returns payload or None if invalid."""
    try:
        # Never log the token or any part of the signing secret.
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


# The truncate / zero-pad ``encrypt_credential`` / ``decrypt_credential`` helpers
# that used to live here were removed (Section H 8.4, finding F4): nothing
# called them. Exchange credentials are encrypted by
# ``shared_lib.core.security.broker_security`` (BROKER_SECRET_KEY).
