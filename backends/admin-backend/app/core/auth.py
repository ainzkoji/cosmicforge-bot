from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, status
from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError, JWTClaimsError

from app.core.config import settings
from app.persistence.db import AdminDB
from shared_lib.core.security import token_state


logger = logging.getLogger(__name__)

ADMIN_ISSUER = "cosmicforge-admin-backend"
ADMIN_AUDIENCE = "admin-portal"
ADMIN_ACCESS_TYPE = "admin_access"


def decode_admin_token(token: str) -> dict[str, Any] | None:
    try:
        return jwt.decode(
            token,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM],
            issuer=ADMIN_ISSUER,
            audience=ADMIN_AUDIENCE,
            options={"require": ["iss", "aud", "exp", "sub"]},
        )
    except ExpiredSignatureError:
        logger.warning("Admin token validation failed: token expired")
    except JWTClaimsError as exc:
        logger.warning("Admin token validation failed: invalid claims: %s", exc)
    except JWTError as exc:
        logger.warning("Admin token validation failed: invalid token: %s", exc)
    return None


def get_active_admin_from_token(token: str, db: AdminDB) -> dict[str, Any]:
    payload = decode_admin_token(token)
    if not payload or payload.get("type") != ADMIN_ACCESS_TYPE:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate admin credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Section H 8.4 (F1): besides is_active, the token's ``tv`` must equal
    # admins.token_version and its ``sid`` rotation chain must still have a live
    # admin_sessions row -- so MFA enrolment/reset, refresh reuse, a password
    # reset or a sign-out end the token here at once. One indexed read each.
    admin_id = payload.get("sub")
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM admins WHERE id = ?", (admin_id,)).fetchone()
        admin, problem = token_state.admin_token_problem(conn, payload, row) if row else (None, None)

    if not row:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Admin not found")
    if problem == token_state.INACTIVE:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin account is inactive")
    if problem is not None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "TOKEN_REVOKED", "message": "Session ended. Sign in again."},
            headers={"WWW-Authenticate": "Bearer"},
        )
    # Section J: the user backend's admin MFA policy, applied here too. A session of an admin who has not
    # enrolled (e.g. one opened before the policy was on) is refused, as by the user backend's admin routes.
    if admin_mfa_required() and not (admin.get("mfa_enabled") and admin.get("mfa_secret")):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"code": "MFA_ENROLLMENT_REQUIRED", "message": "Admin MFA enrolment is required"},
        )
    return admin


def admin_mfa_required() -> bool:
    """Always in production (an explicit ``ADMIN_MFA_REQUIRED=false`` is ignored there); elsewhere the setting,
    off when unset."""
    explicit = getattr(settings, "ADMIN_MFA_REQUIRED", None)
    if bool(getattr(settings, "production", True)):
        if explicit is False or str(explicit).strip().lower() in ("false", "0", "no", "off"):
            logger.warning("ADMIN_MFA_REQUIRED=false is ignored in production; admin MFA stays mandatory")
        return True
    if explicit is None or explicit == "":
        return False
    return explicit is True or str(explicit).strip().lower() in ("true", "1", "yes", "on")


def sanitize_admin_identity(admin: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": admin.get("id"),
        "email": admin.get("email"),
        "full_name": admin.get("full_name") or admin.get("name"),
        "role": admin.get("role"),
        "is_superuser": int(admin.get("is_superuser") or 0),
        "is_active": int(admin.get("is_active") or 0),
    }
