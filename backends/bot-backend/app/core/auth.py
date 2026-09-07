"""
Shared Authentication Utilities for Bot-Backend

This module provides authentication and authorization functions used by 
trading-engine modules. This replaces dependencies on the user-management
API (`app.api.auth`) which is being removed to enforce service boundaries.
"""
from fastapi import HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer
from app.core.security import decode_token
from shared_lib.persistence.db import DB

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


def get_current_user_id(token: str = Depends(oauth2_scheme)) -> str:
    """
    Validate access token and return user ID.
    
    Args:
        token: JWT access token
        
    Returns:
        User ID from token payload
        
    Raises:
        HTTPException: 401 if token is invalid
    """
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload.get("sub")


def get_current_active_user(token: str = Depends(oauth2_scheme)) -> dict:
    """
    Get current user from token and validate they are active.
    
    Args:
        token: JWT access token
        
    Returns:
        User dict with id, email, status, role, etc.
        
    Raises:
        HTTPException: 401 if invalid token, 403 if account not active
    """
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid token")
    
    user_id = payload.get("sub")
    db = DB()
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="User not found")
        if row["status"] != "active":
            raise HTTPException(status_code=403, detail="Account not active")
        
        user_data = dict(row)
        # Enrich with token claims (permissions, entitlements)
        user_data["permissions"] = payload.get("permissions", [])
        user_data["entitlements"] = payload.get("entitlements", {})
        
        return user_data


def require_admin(token: str = Depends(oauth2_scheme)) -> str:
    """
    Dependency that requires admin role.
    
    Args:
        token: JWT access token
        
    Returns:
        User ID if admin
        
    Raises:
        HTTPException: 401 if invalid token, 403 if not admin
    """
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid token")
    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return payload.get("sub")


def require_permission(required_perm: str):
    """
    Factory for dependency that requires a specific permission.
    
    Usage:
        @router.post("/", dependencies=[Depends(require_permission("bot:write"))])
    """
    def permission_checker(token: str = Depends(oauth2_scheme)) -> str:
        payload = decode_token(token)
        if not payload or payload.get("type") != "access":
            raise HTTPException(status_code=401, detail="Invalid token")
        
        # Check permissions list
        perms = payload.get("permissions", [])
        
        # Admin override (optional, but good for safety if we miss a permission mapping)
        if payload.get("role") == "admin":
            return payload.get("sub")
            
        if required_perm not in perms:
            raise HTTPException(
                status_code=403, 
                detail=f"Missing permission: {required_perm}"
            )
        return payload.get("sub")
        
    return permission_checker
