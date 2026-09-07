from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from app.core.security import decode_token, decode_admin_token
from shared_lib.persistence.db import DB

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")
admin_oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/admin-auth/login")

def get_current_user_id(token: str = Depends(oauth2_scheme)) -> str:
    """
    Validate access token and return user ID.
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
    Get current user and validate they are active.
    """
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user_id = payload.get("sub")
    db = DB()
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="User not found")
        if row["status"] != "active":
            raise HTTPException(status_code=403, detail="Account not active")
        return dict(row)

def get_current_admin(token: str = Depends(admin_oauth2_scheme)) -> dict:
    """
    Validate admin access token and return admin identity from admins table.
    """
    payload = decode_admin_token(token)
    if not payload or payload.get("type") != "admin_access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate admin credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    admin_id = payload.get("sub")
    db = DB()
    with db.connect() as conn:
        row = conn.execute("SELECT * FROM admins WHERE id = ?", (admin_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Admin not found")
        if row["is_active"] != 1:
            raise HTTPException(status_code=403, detail="Admin account is inactive")
        return dict(row)

def require_admin(current_admin: dict = Depends(get_current_admin)) -> dict:
    """
    Alias for get_current_admin to maintain backward compatibility with existing routes.
    """
    return current_admin
