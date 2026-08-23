"""
Authentication Dependencies for Bot Backend

Provides authentication utilities for bot-backend endpoints.
"""
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from typing import Optional, Dict

# JWT Configuration
from app.core.config import settings
from app.core.security import ISSUER, AUDIENCE

security = HTTPBearer()


def get_current_active_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict:
    """
    Validate JWT token and return user info.
    This is a simplified version for bot-backend.
    """
    token = credentials.credentials
    
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        # Debugging: Print key prefix to ensure it matches
        print(f"[DEBUG-DEPS] Validating token with Secret Key Prefix: {settings.SECRET_KEY[:5]}...")
        
        payload = jwt.decode(
            token, 
            settings.SECRET_KEY, 
            algorithms=[settings.ALGORITHM],
            audience=AUDIENCE,
            issuer=ISSUER,
            options={
                "require": ["iss", "aud", "exp", "sub"]
            }
        )
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
        
        # Return user dict
        return {
            "id": user_id,
            "email": payload.get("email"),
        }
    except JWTError as e:
        print(f"[DEBUG-DEPS] Token JWTError: {e}")
        raise credentials_exception
