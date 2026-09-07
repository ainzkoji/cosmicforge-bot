from __future__ import annotations

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from app.core.auth import get_active_admin_from_token
from app.persistence.db import AdminDB, get_db


admin_oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/admin-auth/login")


def require_admin(
    token: str = Depends(admin_oauth2_scheme),
    db: AdminDB = Depends(get_db),
) -> dict:
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing admin credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return get_active_admin_from_token(token, db)
