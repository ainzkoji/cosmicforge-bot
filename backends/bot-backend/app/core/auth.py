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


def caller_is_admin(
    token: str = Depends(oauth2_scheme),
    _user_id: str = Depends(get_current_user_id),
) -> bool:
    """True when the caller's access token carries the admin role.

    Same rule as ``require_admin``, but for routes that serve ordinary users
    too and only widen what an admin may do. It never rejects a valid
    non-admin token; an invalid one is a 401 (via ``get_current_user_id``).
    """
    payload = decode_token(token) or {}
    return payload.get("type") == "access" and payload.get("role") == "admin"


#: ``act`` claim of the one-call service token the user-backend admin emergency
#: proxy mints after it has verified the caller against its ``admins`` table
#: (``backends/user-backend/app/api/admin_emergency.py``). No login flow issues
#: a token with this claim.
EMERGENCY_ACTOR_CLAIM = "admin-emergency"
#: The proxy mints the token for one upstream call (60 s). Anything issued for
#: longer than this is not that token.
EMERGENCY_TOKEN_MAX_LIFETIME_SECONDS = 120


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def require_admin_emergency(token: str = Depends(oauth2_scheme)) -> str:
    """Dependency for the emergency router (kill switch, flatten) ONLY.

    ``require_admin`` accepts any access token with ``role=admin``, which an
    end-user account whose ``users.role`` is ``admin`` also holds. Emergency
    controls additionally require the dedicated, short-lived service token the
    user-backend admin emergency proxy mints for a verified ``admins``-table
    operator:

    * ``act`` is exactly ``EMERGENCY_ACTOR_CLAIM``;
    * the token was issued for at most
      ``EMERGENCY_TOKEN_MAX_LIFETIME_SECONDS`` (``exp - iat``).

    Returns the admin id (``sub``).

    Raises:
        HTTPException: 401 if invalid token, 403 if not such a token
    """
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid token")
    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    issued_at, expires_at = payload.get("iat"), payload.get("exp")
    lifetime_ok = (
        _is_number(issued_at)
        and _is_number(expires_at)
        and 0 < expires_at - issued_at <= EMERGENCY_TOKEN_MAX_LIFETIME_SECONDS
    )
    if payload.get("act") != EMERGENCY_ACTOR_CLAIM or not lifetime_ok or not payload.get("sub"):
        raise HTTPException(
            status_code=403,
            detail="Emergency controls require the admin emergency service credential",
        )
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
