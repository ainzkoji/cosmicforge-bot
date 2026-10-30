"""
Authentication Dependencies for Bot Backend

Provides authentication utilities for bot-backend endpoints.
"""
from fastapi import Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Dict

from app.core.auth import authenticate_access_token

security = HTTPBearer()


def get_current_active_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict:
    """
    Validate JWT token and return user info.
    This is a simplified version for bot-backend.

    Section H 8.4: only an ACCESS token authenticates a request (a refresh
    token carries the same issuer/audience and used to be accepted here), and
    since F1 the token must also still be current: active account, matching
    ``token_version``, live device session (``app.core.auth``).
    """
    payload, _user = authenticate_access_token(credentials.credentials,
                                               unknown_detail="Could not validate credentials")
    return {
        "id": payload.get("sub"),
        "email": payload.get("email"),
    }
