"""
Shared Authentication Utilities for Bot-Backend

This module provides authentication and authorization functions used by
trading-engine modules. This replaces dependencies on the user-management
API (`app.api.auth`) which is being removed to enforce service boundaries.

Section H 8.4 (finding F1): a signature alone does not make an access token
current. Every validator here also checks, with one indexed read of the shared
database, that the user's account is active, that the token's ``tv``
claim equals ``users.token_version`` (bumped by logout-all, password reset,
suspension, privilege changes, refresh reuse) and, when the token names a
device session (``sid``), that the session chain is still live. Nothing is
cached, so a revoked token is refused on its next request.
"""
from typing import Optional, Tuple

from fastapi import HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer
from app.core.security import decode_token
from shared_lib.core.security import token_state
from shared_lib.persistence.db import DB

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

_REVOKED_DETAIL = {"code": "TOKEN_REVOKED", "message": "Session ended. Sign in again."}


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


def _short_lived(payload: dict) -> bool:
    issued_at, expires_at = payload.get("iat"), payload.get("exp")
    return (
        _is_number(issued_at)
        and _is_number(expires_at)
        and 0 < expires_at - issued_at <= EMERGENCY_TOKEN_MAX_LIFETIME_SECONDS
    )


def is_service_credential(payload: dict) -> bool:
    """The admin emergency/governance service token: its subject is an
    ``admins`` id that the minting user-backend verified (active, token
    version, MFA policy) at most ``EMERGENCY_TOKEN_MAX_LIFETIME_SECONDS`` ago
    for this single call, so there is no ``users`` row to check."""
    return (payload.get("act") == EMERGENCY_ACTOR_CLAIM and payload.get("role") == "admin"
            and bool(payload.get("sub")) and _short_lived(payload))


def _unauthorized(detail) -> HTTPException:
    return HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail,
                         headers={"WWW-Authenticate": "Bearer"})


def authenticate_access_token(
    token: Optional[str],
    *,
    invalid_detail="Could not validate credentials",
    unknown_detail="User not found",
    allow_service: bool = True,
    require_known_subject: bool = False,
) -> Tuple[dict, Optional[dict]]:
    """``(payload, users_row)`` for a valid, current access token; else HTTPException.

    Every token whose subject HAS a ``users`` row is fully re-checked (status,
    token_version, device session). A subject without a row keeps the
    behaviour each dependency had before 8.4: ``get_current_active_user``
    refuses it (``require_known_subject``), the claim-only dependencies accept
    it -- only the holder of SECRET_KEY can mint such a token (the user-backend
    mints them for service calls), and the user-backend, the only public path
    to this service, refuses unknown subjects itself. ``users_row`` is None
    for a service credential or an unknown subject."""
    payload = decode_token(token) if token else None
    if not payload or payload.get("type") != "access" or not payload.get("sub"):
        raise _unauthorized(invalid_detail)
    if allow_service and is_service_credential(payload):
        return payload, None
    with DB().connect() as conn:
        user, problem = token_state.user_token_problem(conn, payload)
    if problem == token_state.UNKNOWN_SUBJECT:
        if require_known_subject:
            raise _unauthorized(unknown_detail)
        return payload, None
    if problem == token_state.INACTIVE:
        raise HTTPException(status_code=403, detail="Account not active")
    if problem is not None:
        raise _unauthorized(_REVOKED_DETAIL)
    return payload, user


def get_current_user_id(token: str = Depends(oauth2_scheme)) -> str:
    """
    Validate access token and return user ID.

    Args:
        token: JWT access token

    Returns:
        User ID from token payload

    Raises:
        HTTPException: 401 if token is invalid or revoked, 403 if the account is not active
    """
    payload, _user = authenticate_access_token(token, unknown_detail="Could not validate credentials")
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
    payload, user = authenticate_access_token(token, invalid_detail="Invalid token", allow_service=False,
                                              require_known_subject=True)
    user_data = dict(user)
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
    payload, _user = authenticate_access_token(token, invalid_detail="Invalid token")
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
    if payload.get("act") != EMERGENCY_ACTOR_CLAIM or not _short_lived(payload) or not payload.get("sub"):
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
        payload, _user = authenticate_access_token(token, invalid_detail="Invalid token")

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
