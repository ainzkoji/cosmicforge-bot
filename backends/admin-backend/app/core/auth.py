from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, status
from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError, JWTClaimsError

from app.core.config import settings
from app.persistence.db import AdminDB


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

    admin_id = payload.get("sub")
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM admins WHERE id = ?", (admin_id,)).fetchone()

    if not row:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Admin not found")
    admin = dict(row)
    if int(admin.get("is_active") or 0) != 1:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin account is inactive")
    return admin


def sanitize_admin_identity(admin: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": admin.get("id"),
        "email": admin.get("email"),
        "full_name": admin.get("full_name") or admin.get("name"),
        "role": admin.get("role"),
        "is_superuser": int(admin.get("is_superuser") or 0),
        "is_active": int(admin.get("is_active") or 0),
    }
