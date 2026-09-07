from fastapi import APIRouter, Depends, HTTPException, status, Form, Request
from fastapi.security import OAuth2PasswordRequestForm
from datetime import datetime, timezone
import uuid

from app.core.security import (
    verify_password,
    create_admin_access_token,
    create_admin_refresh_token,
    decode_admin_token,
    hash_token
)
from shared_lib.persistence.db import DB
from pydantic import BaseModel

router = APIRouter()

# Schema for responses
class AdminAuthResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"

class AdminMeResponse(BaseModel):
    id: str
    email: str
    full_name: str | None
    role: str
    is_superuser: int

from app.core.deps import get_current_admin

@router.post("/login", response_model=AdminAuthResponse)
def admin_login(request: Request, form_data: OAuth2PasswordRequestForm = Depends()):
    db = DB()
    with db.connect() as conn:
        admin_row = conn.execute("SELECT * FROM admins WHERE email = ?", (form_data.username,)).fetchone()
        
        if not admin_row or not verify_password(form_data.password, admin_row["hashed_password"]):
            raise HTTPException(status_code=400, detail="Incorrect email or password")
            
        if admin_row["is_active"] != 1:
            raise HTTPException(status_code=403, detail="Admin account is inactive")

        # Update last login
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("UPDATE admins SET last_login_at = ? WHERE id = ?", (now, admin_row["id"]))

        # Generate tokens
        access_token = create_admin_access_token(admin_row["id"], role=admin_row["role"])
        refresh_token = create_admin_refresh_token(admin_row["id"])
        
        # Store session
        session_id = str(uuid.uuid4())
        device = request.headers.get("User-Agent", "Unknown Device")
        ip = request.client.host if request.client else "Unknown IP"
        
        # 30 days refresh expiry
        from datetime import timedelta
        from app.core.config import settings
        expires_at = (datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)).isoformat()
        
        conn.execute(
            """
            INSERT INTO admin_sessions 
            (id, admin_id, refresh_token_hash, device, ip, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (session_id, admin_row["id"], hash_token(refresh_token), device, ip, now, expires_at)
        )

        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer"
        }

class RefreshRequest(BaseModel):
    refresh_token: str

@router.post("/refresh", response_model=AdminAuthResponse)
def admin_refresh(request: RefreshRequest, req: Request):
    payload = decode_admin_token(request.refresh_token)
    if not payload or payload.get("type") != "admin_refresh":
        raise HTTPException(status_code=401, detail="Invalid refresh token")
        
    admin_id = payload.get("sub")
    hashed_rt = hash_token(request.refresh_token)
    
    db = DB()
    with db.connect() as conn:
        session = conn.execute(
            "SELECT * FROM admin_sessions WHERE refresh_token_hash = ? AND admin_id = ? AND revoked_at IS NULL", 
            (hashed_rt, admin_id)
        ).fetchone()
        
        if not session:
            raise HTTPException(status_code=401, detail="Session invalid or revoked")
            
        if datetime.fromisoformat(session["expires_at"]) < datetime.now(timezone.utc):
            raise HTTPException(status_code=401, detail="Session expired")
            
        admin_row = conn.execute("SELECT * FROM admins WHERE id = ?", (admin_id,)).fetchone()
        if not admin_row or admin_row["is_active"] != 1:
            raise HTTPException(status_code=403, detail="Admin inactive")

        # Rotate token
        now = datetime.now(timezone.utc).isoformat()
        new_access = create_admin_access_token(admin_id, role=admin_row["role"])
        new_refresh = create_admin_refresh_token(admin_id)
        new_hashed_rt = hash_token(new_refresh)
        
        conn.execute("UPDATE admin_sessions SET revoked_at = ?, rotated_from = ? WHERE id = ?", 
                     (now, "refresh_rotation", session["id"]))
        
        new_session_id = str(uuid.uuid4())
        from datetime import timedelta
        from app.core.config import settings
        expires_at = (datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)).isoformat()
        
        device = req.headers.get("User-Agent", session["device"])
        ip = req.client.host if req.client else session["ip"]
        
        conn.execute(
            """
            INSERT INTO admin_sessions 
            (id, admin_id, refresh_token_hash, device, ip, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (new_session_id, admin_id, new_hashed_rt, device, ip, now, expires_at)
        )
        
        return {
            "access_token": new_access,
            "refresh_token": new_refresh,
            "token_type": "bearer"
        }

@router.post("/logout")
def admin_logout(request: RefreshRequest):
    hashed_rt = hash_token(request.refresh_token)
    db = DB()
    now = datetime.now(timezone.utc).isoformat()
    with db.connect() as conn:
        conn.execute(
            "UPDATE admin_sessions SET revoked_at = ? WHERE refresh_token_hash = ?",
            (now, hashed_rt)
        )
    return {"status": "logged_out"}

@router.get("/me", response_model=AdminMeResponse)
def get_admin_me(current_admin: dict = Depends(get_current_admin)):
    return AdminMeResponse(**current_admin)
