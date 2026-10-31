from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from shared_lib.core.security.jwt_tokens import jwt, JWTError  # PyJWT (F6)
from app.core.security import decode_token, decode_admin_token
from shared_lib.core.security import token_state
from shared_lib.persistence.db import DB

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")
admin_oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/admin-auth/login")

# Section H 8.4 (F1): a token whose account was signed out everywhere, reset,
# suspended, re-privileged, or whose device session ended. 401 so a client
# refreshes (which fails when the refresh session is gone too) or signs in.
_REVOKED_DETAIL = {"code": "TOKEN_REVOKED", "message": "Session ended. Sign in again."}


def _unauthorized(detail) -> HTTPException:
    return HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail,
                         headers={"WWW-Authenticate": "Bearer"})


def require_current_user(conn, payload: dict, *, unknown_detail="User not found") -> dict:
    """The users row behind a decoded ``access`` token, or HTTPException.

    One indexed read: the account must exist, be active, still have the token's
    ``token_version`` and (when the token names one) a live device session."""
    user, problem = token_state.user_token_problem(conn, payload)
    if problem == token_state.UNKNOWN_SUBJECT:
        raise _unauthorized(unknown_detail)
    if problem == token_state.INACTIVE:
        raise HTTPException(status_code=403, detail="Account not active")
    if problem is not None:
        raise _unauthorized(_REVOKED_DETAIL)
    return user


def require_current_admin_row(conn, payload: dict, admin_row=None) -> dict:
    """Same for an ``admin_access`` token and the admins table."""
    admin, problem = token_state.admin_token_problem(conn, payload, admin_row)
    if problem == token_state.UNKNOWN_SUBJECT:
        raise _unauthorized("Admin not found")
    if problem == token_state.INACTIVE:
        raise HTTPException(status_code=403, detail="Admin account is inactive")
    if problem is not None:
        raise _unauthorized(_REVOKED_DETAIL)
    return admin


def get_current_user_id(token: str = Depends(oauth2_scheme)) -> str:
    """
    Validate access token and return user ID.
    """
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        raise _unauthorized("Could not validate credentials")
    with DB().connect() as conn:
        user = require_current_user(conn, payload, unknown_detail="Could not validate credentials")
    return user["id"]

def get_current_active_user(token: str = Depends(oauth2_scheme)) -> dict:
    """
    Get current user and validate they are active.
    """
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        raise _unauthorized("Invalid token")
    with DB().connect() as conn:
        return require_current_user(conn, payload)

def get_current_admin(token: str = Depends(admin_oauth2_scheme)) -> dict:
    """
    Validate admin access token and return admin identity from admins table.
    """
    payload = decode_admin_token(token)
    if not payload or payload.get("type") != "admin_access":
        raise _unauthorized("Could not validate admin credentials")

    db = DB()
    with db.connect() as conn:
        admin = require_current_admin_row(conn, payload)
    # Section H 8.4: while admin MFA is mandatory, a session of an admin who has
    # not enrolled (e.g. one opened before the policy was switched on) is not
    # accepted anywhere; the console sends the admin through enrolment.
    from app.core.session_cookies import admin_mfa_required
    if admin_mfa_required() and not (admin.get("mfa_enabled") and admin.get("mfa_secret")):
        raise HTTPException(
            status_code=403,
            detail={"code": "MFA_ENROLLMENT_REQUIRED", "message": "Admin MFA enrolment is required"},
        )
    return admin

def require_admin(current_admin: dict = Depends(get_current_admin)) -> dict:
    """
    Alias for get_current_admin to maintain backward compatibility with existing routes.
    """
    return current_admin
