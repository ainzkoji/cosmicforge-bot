"""
Enhanced Authentication API
- Registration with email verification
- Login with rate limiting and status checks
- Session management (create, refresh, revoke)
- Password reset flow
"""
from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import pyotp
from app.schemas.auth import (
    UserCreate, UserResponse, Token, RefreshTokenReq, 
    BrokerLinkReq, BrokerResponse, UserStatus,
    VerifyEmailRequest, ResendVerificationRequest,
    ForgotPasswordRequest, ResetPasswordRequest,
    SessionResponse, SessionListResponse
)
from app.schemas.security import TwoFASetupResponse, TwoFAVerifyRequest, SessionRevokeRequest
from app.core.security import (
    get_password_hash, verify_password, 
    create_access_token, create_refresh_token, 
    encrypt_credential, decode_token,
    generate_otp, hash_otp, verify_otp, hash_token
)
from app.persistence.db import DB, utc_now_iso
import uuid
from typing import List, Optional
from datetime import datetime, timedelta, timezone

router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


# --- Constants ---
MAX_LOGIN_ATTEMPTS = 5
LOGIN_WINDOW_MINUTES = 15
MAX_VERIFY_ATTEMPTS = 5
OTP_EXPIRE_MINUTES = 15
RESET_EXPIRE_MINUTES = 60
RESEND_COOLDOWN_SECONDS = 90  # Cooldown between resend requests
FORGOT_COOLDOWN_SECONDS = 90   # Cooldown between forgot password requests


def normalize_email(email: str) -> str:
    """Normalize email: lowercase and strip whitespace."""
    return email.lower().strip()


def audit_event(conn, event_type: str, user_id: str = None, email: str = None, ip: str = None, details: dict = None):
    """
    Log security/audit events for auth actions.
    Events: user_registered, verification_sent, email_verified, login_success, login_failed,
    refresh_success, refresh_failed, refresh_reuse_detected, logout, logout_all,
    password_reset_requested, password_reset_completed, user_suspended, user_unsuspended
    """
    import json
    conn.execute(
        """INSERT INTO auth_audit_log (id, event_type, user_id, email, ip, details, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (str(uuid.uuid4()), event_type, user_id, email, ip, json.dumps(details) if details else None, utc_now_iso())
    )


# --- Helpers ---
def get_current_user_id(token: str = Depends(oauth2_scheme)) -> str:
    """Validate access token and return user ID"""
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload.get("sub")


def get_current_active_user(token: str = Depends(oauth2_scheme)) -> dict:
    """Get current user and validate they are active"""
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
        return dict(row)


def _check_rate_limit(conn, email: str) -> None:
    """Check login rate limiting. Raises if exceeded."""
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=LOGIN_WINDOW_MINUTES)).isoformat()
    rows = conn.execute(
        "SELECT COUNT(*) as cnt FROM login_attempts WHERE email = ? AND attempted_at > ? AND success = 0",
        (email, cutoff)
    ).fetchone()
    if rows and rows["cnt"] >= MAX_LOGIN_ATTEMPTS:
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")


def _record_login_attempt(conn, email: str, ip: str, success: bool) -> None:
    """Record a login attempt for rate limiting."""
    conn.execute(
        "INSERT INTO login_attempts (id, email, ip, success, attempted_at) VALUES (?, ?, ?, ?, ?)",
        (str(uuid.uuid4()), email, ip, 1 if success else 0, utc_now_iso())
    )


# --- Registration ---
@router.post("/register", response_model=UserResponse)
def register(user: UserCreate, request: Request):
    email = normalize_email(user.email)
    ip = request.client.host if request else "unknown"
    db = DB()
    with db.connect() as conn:
        # Check exists (return same message to prevent email enumeration)
        existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
        if existing:
            raise HTTPException(status_code=400, detail="Registration failed")
        
        # Create user with pending status
        uid = str(uuid.uuid4())
        hashed = get_password_hash(user.password)
        now = utc_now_iso()
        
        conn.execute("""
            INSERT INTO users (id, email, password_hash, status, role, is_verified, created_at, 
            locale, country, timezone, terms_accepted_at, risk_disclaimer_accepted_at, marketing_session_id, selected_plan_id) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                uid, email, hashed, "pending_verification", "user", False, now,
                user.locale, user.country, user.timezone, 
                user.terms_accepted_at, user.risk_disclaimer_accepted_at,
                user.marketing_session_id, user.selected_plan_id
            )
        )

        # Update marketing session with converted user ID
        if user.marketing_session_id:
            conn.execute(
                "UPDATE marketing_sessions SET converted_user_id = ? WHERE id = ?",
                (uid, user.marketing_session_id)
            )

        # Create pricing intent if plan selected but no session (direct signup)
        if user.selected_plan_id and not user.marketing_session_id:
            # We treat this as a direct conversion
            pass # simplified for now
        
        # Generate verification OTP
        otp = generate_otp()
        otp_hash = hash_otp(otp)
        expires = (datetime.now(timezone.utc) + timedelta(minutes=OTP_EXPIRE_MINUTES)).isoformat()
        
        conn.execute("""
            INSERT INTO email_verifications (id, user_id, code_hash, expires_at, attempts, created_at)
            VALUES (?, ?, ?, ?, 0, ?)""",
            (str(uuid.uuid4()), uid, otp_hash, expires, now)
        )
        
        # Audit events
        audit_event(conn, "user_registered", user_id=uid, email=email, ip=ip)
        audit_event(conn, "verification_sent", user_id=uid, email=email, ip=ip)
        
        # In production: send email. For now, log to console.
        print(f"[AUTH] Verification code for {email}: {otp}")
        
        return {
            "id": uid, 
            "email": email, 
            "status": "pending_verification",
            "role": "user",
            "is_verified": False, 
            "created_at": now
        }


# --- Email Verification ---
@router.post("/verify-email")
def verify_email(req: VerifyEmailRequest):
    db = DB()
    with db.connect() as conn:
        user = conn.execute("SELECT id, status FROM users WHERE email = ?", (req.email,)).fetchone()
        if not user:
            raise HTTPException(status_code=400, detail="Verification failed")
        
        if user["status"] == "active":
            return {"message": "Email already verified"}
        
        # Get latest verification code
        verification = conn.execute("""
            SELECT * FROM email_verifications 
            WHERE user_id = ? AND used_at IS NULL 
            ORDER BY created_at DESC LIMIT 1""", 
            (user["id"],)
        ).fetchone()
        
        if not verification:
            raise HTTPException(status_code=400, detail="No pending verification")
        
        # Check attempts
        if verification["attempts"] >= MAX_VERIFY_ATTEMPTS:
            raise HTTPException(status_code=400, detail="Too many attempts. Request new code.")
        
        # Check expiry
        expires = datetime.fromisoformat(verification["expires_at"].replace('Z', '+00:00'))
        if datetime.now(timezone.utc) > expires:
            raise HTTPException(status_code=400, detail="Code expired. Request new code.")
        
        # Increment attempts
        conn.execute(
            "UPDATE email_verifications SET attempts = attempts + 1 WHERE id = ?",
            (verification["id"],)
        )
        
        # Verify code
        if not verify_otp(req.code, verification["code_hash"]):
            raise HTTPException(status_code=400, detail="Invalid code")
        
        # Mark as used and activate user
        now = utc_now_iso()
        conn.execute("UPDATE email_verifications SET used_at = ? WHERE id = ?", (now, verification["id"]))
        conn.execute("UPDATE users SET status = 'active', is_verified = 1 WHERE id = ?", (user["id"],))
        
        return {"message": "Email verified successfully"}


@router.post("/resend-verification")
def resend_verification(req: ResendVerificationRequest):
    db = DB()
    with db.connect() as conn:
        user = conn.execute("SELECT id, status FROM users WHERE email = ?", (req.email,)).fetchone()
        
        # Same message regardless to prevent enumeration
        if not user or user["status"] != "pending_verification":
            return {"message": "If your email is registered and pending, a new code will be sent."}
        
        # Generate new OTP
        otp = generate_otp()
        otp_hash = hash_otp(otp)
        now = utc_now_iso()
        expires = (datetime.now(timezone.utc) + timedelta(minutes=OTP_EXPIRE_MINUTES)).isoformat()
        
        conn.execute("""
            INSERT INTO email_verifications (id, user_id, code_hash, expires_at, attempts, created_at)
            VALUES (?, ?, ?, ?, 0, ?)""",
            (str(uuid.uuid4()), user["id"], otp_hash, expires, now)
        )
        
        print(f"[AUTH] New verification code for {req.email}: {otp}")
        
        return {"message": "If your email is registered and pending, a new code will be sent."}


# --- Login ---
@router.post("/login", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends(), request: Request = None):
    db = DB()
    ip = request.client.host if request else "unknown"
    
    with db.connect() as conn:
        # Rate limiting
        _check_rate_limit(conn, form_data.username)
        
        row = conn.execute("SELECT * FROM users WHERE email = ?", (form_data.username,)).fetchone()
        
        # Invalid credentials (same message for security)
        if not row or not verify_password(form_data.password, row["password_hash"]):
            _record_login_attempt(conn, form_data.username, ip, False)
            raise HTTPException(status_code=400, detail="Invalid credentials")
        
        # Check user status
        if row["status"] == "pending_verification":
            raise HTTPException(status_code=403, detail="Please verify your email first")
        if row["status"] == "suspended":
            raise HTTPException(status_code=403, detail="Account suspended")
        if row["status"] == "deleted":
            raise HTTPException(status_code=400, detail="Invalid credentials")
        
        uid = row["id"]
        role = row["role"] or "user"
        
        # Record successful login
        _record_login_attempt(conn, form_data.username, ip, True)
        conn.execute("UPDATE users SET last_login_at = ? WHERE id = ?", (utc_now_iso(), uid))
        
        # Create tokens
        access_token = create_access_token(uid, role=role)
        refresh_token = create_refresh_token(uid)
        
        # Store session
        rt_hash = hash_token(refresh_token)
        now = utc_now_iso()
        session_id = str(uuid.uuid4())
        expires = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        device = request.headers.get("User-Agent", "unknown") if request else "unknown"
        
        conn.execute("""
            INSERT INTO auth_sessions (id, user_id, refresh_token_hash, device, ip, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (session_id, uid, rt_hash, device[:255], ip, now, expires)
        )
        
        return {"access_token": access_token, "refresh_token": refresh_token, "token_type": "bearer"}


# --- Token Refresh with Rotation ---
@router.post("/refresh", response_model=Token)
def refresh(req: RefreshTokenReq, request: Request = None):
    payload = decode_token(req.refresh_token)
    if not payload or payload.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Invalid refresh token")
    
    uid = payload.get("sub")
    old_hash = hash_token(req.refresh_token)
    
    db = DB()
    with db.connect() as conn:
        # Find session
        session = conn.execute(
            "SELECT * FROM auth_sessions WHERE refresh_token_hash = ? AND user_id = ?",
            (old_hash, uid)
        ).fetchone()
        
        if not session:
            raise HTTPException(status_code=401, detail="Session not found")
        
        # SECURITY: Detect refresh token reuse (token used after it was already rotated)
        if session["revoked_at"] is not None:
            # Token reuse detected! This is a security incident.
            # Revoke ALL sessions for this user as a precaution.
            print(f"[SECURITY] Refresh token reuse detected for user {uid}! Revoking all sessions.")
            conn.execute(
                "UPDATE auth_sessions SET revoked_at = ? WHERE user_id = ?",
                (utc_now_iso(), uid)
            )
            raise HTTPException(status_code=401, detail="Security alert: session invalidated")
        
        # Check expiry
        expires = datetime.fromisoformat(session["expires_at"].replace('Z', '+00:00'))
        if datetime.now(timezone.utc) > expires:
            raise HTTPException(status_code=401, detail="Session expired")
        
        # Get user role
        user = conn.execute("SELECT role, status FROM users WHERE id = ?", (uid,)).fetchone()
        if not user or user["status"] != "active":
            raise HTTPException(status_code=403, detail="Account not active")
        
        role = user["role"] or "user"
        
        # Create new tokens
        new_access = create_access_token(uid, role=role)
        new_refresh = create_refresh_token(uid)
        new_hash = hash_token(new_refresh)
        now = utc_now_iso()
        new_expires = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        
        # Revoke old session
        conn.execute("UPDATE auth_sessions SET revoked_at = ? WHERE id = ?", (now, session["id"]))
        
        # Create new session (rotation)
        ip = request.client.host if request else "unknown"
        device = request.headers.get("User-Agent", "unknown") if request else "unknown"
        
        conn.execute("""
            INSERT INTO auth_sessions (id, user_id, refresh_token_hash, device, ip, created_at, expires_at, rotated_from)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (str(uuid.uuid4()), uid, new_hash, device[:255], ip, now, new_expires, session["id"])
        )
        
        return {"access_token": new_access, "refresh_token": new_refresh, "token_type": "bearer"}


# --- Logout ---
@router.post("/logout")
def logout(req: RefreshTokenReq, user_id: str = Depends(get_current_user_id)):
    """Revoke current session"""
    db = DB()
    token_hash = hash_token(req.refresh_token)
    
    with db.connect() as conn:
        conn.execute(
            "UPDATE auth_sessions SET revoked_at = ? WHERE refresh_token_hash = ? AND user_id = ?",
            (utc_now_iso(), token_hash, user_id)
        )
    
    return {"message": "Logged out successfully"}


# --- Session Management ---
@router.get("/sessions", response_model=SessionListResponse)
def list_sessions(user_id: str = Depends(get_current_user_id)):
    db = DB()
    with db.connect() as conn:
        rows = conn.execute("""
            SELECT id, device, ip, created_at FROM auth_sessions 
            WHERE user_id = ? AND revoked_at IS NULL AND expires_at > ?
            ORDER BY created_at DESC""",
            (user_id, utc_now_iso())
        ).fetchall()
        
        sessions = [
            {"id": r["id"], "device": r["device"], "ip": r["ip"], "created_at": r["created_at"], "is_current": False}
            for r in rows
        ]
        return {"sessions": sessions}


@router.delete("/sessions/{session_id}")
def revoke_session(session_id: str, user_id: str = Depends(get_current_user_id)):
    db = DB()
    with db.connect() as conn:
        result = conn.execute(
            "UPDATE auth_sessions SET revoked_at = ? WHERE id = ? AND user_id = ?",
            (utc_now_iso(), session_id, user_id)
        )
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Session not found")
    
    return {"message": "Session revoked"}


# --- Password Reset ---
@router.post("/forgot-password")
def forgot_password(req: ForgotPasswordRequest):
    """Request password reset code"""
    db = DB()
    with db.connect() as conn:
        user = conn.execute("SELECT id FROM users WHERE email = ? AND status = 'active'", (req.email,)).fetchone()
        
        # Same message regardless to prevent enumeration
        if not user:
            return {"message": "If your email is registered, a reset code will be sent."}
        
        otp = generate_otp()
        otp_hash = hash_otp(otp)
        now = utc_now_iso()
        expires = (datetime.now(timezone.utc) + timedelta(minutes=RESET_EXPIRE_MINUTES)).isoformat()
        
        conn.execute("""
            INSERT INTO password_resets (id, user_id, code_hash, expires_at, attempts, created_at)
            VALUES (?, ?, ?, ?, 0, ?)""",
            (str(uuid.uuid4()), user["id"], otp_hash, expires, now)
        )
        
        print(f"[AUTH] Password reset code for {req.email}: {otp}")
        
        return {"message": "If your email is registered, a reset code will be sent."}


@router.post("/reset-password")
def reset_password(req: ResetPasswordRequest):
    """Reset password with code"""
    db = DB()
    with db.connect() as conn:
        user = conn.execute("SELECT id FROM users WHERE email = ?", (req.email,)).fetchone()
        if not user:
            raise HTTPException(status_code=400, detail="Reset failed")
        
        # Get latest reset code
        reset = conn.execute("""
            SELECT * FROM password_resets 
            WHERE user_id = ? AND used_at IS NULL 
            ORDER BY created_at DESC LIMIT 1""",
            (user["id"],)
        ).fetchone()
        
        if not reset:
            raise HTTPException(status_code=400, detail="No pending reset")
        
        if reset["attempts"] >= 3:
            raise HTTPException(status_code=400, detail="Too many attempts. Request new code.")
        
        expires = datetime.fromisoformat(reset["expires_at"].replace('Z', '+00:00'))
        if datetime.now(timezone.utc) > expires:
            raise HTTPException(status_code=400, detail="Code expired")
        
        conn.execute("UPDATE password_resets SET attempts = attempts + 1 WHERE id = ?", (reset["id"],))
        
        if not verify_otp(req.code, reset["code_hash"]):
            raise HTTPException(status_code=400, detail="Invalid code")
        
        # Update password and mark reset as used
        new_hash = get_password_hash(req.new_password)
        now = utc_now_iso()
        
        conn.execute("UPDATE password_resets SET used_at = ? WHERE id = ?", (now, reset["id"]))
        conn.execute("UPDATE users SET password_hash = ? WHERE id = ?", (new_hash, user["id"]))
        
        # Revoke all sessions
        conn.execute("UPDATE auth_sessions SET revoked_at = ? WHERE user_id = ?", (now, user["id"]))
        
        return {"message": "Password reset successfully. Please login."}


# --- Broker Management (requires active user) ---
@router.get("/user/brokers", response_model=List[BrokerResponse])
def list_brokers(user: dict = Depends(get_current_active_user)):
    db = DB()
    with db.connect() as conn:
        rows = conn.execute("SELECT * FROM broker_accounts WHERE user_id = ?", (user["id"],)).fetchall()
        return [dict(r) for r in rows]


@router.post("/user/brokers", response_model=BrokerResponse)
def link_broker(req: BrokerLinkReq, user: dict = Depends(get_current_active_user)):
    db = DB()
    with db.connect() as conn:
        account_id = str(uuid.uuid4())
        now = utc_now_iso()
        
        conn.execute("""
            INSERT INTO broker_accounts (id, user_id, exchange, name, is_active, created_at) 
            VALUES (?, ?, ?, ?, ?, ?)""",
            (account_id, user["id"], req.exchange, req.name, True, now)
        )
        
        key_enc = encrypt_credential(req.api_key)
        secret_enc = encrypt_credential(req.api_secret)
        pass_enc = encrypt_credential(req.passphrase) if req.passphrase else None
        
        conn.execute("""
            INSERT INTO broker_credentials (account_id, api_key_enc, api_secret_enc, passphrase_enc, updated_at) 
            VALUES (?, ?, ?, ?, ?)""",
            (account_id, key_enc, secret_enc, pass_enc, now)
        )
        
        return {"id": account_id, "exchange": req.exchange, "name": req.name, "is_active": True, "created_at": now}


# --- User Profile ---
@router.get("/me")
def get_me(user: dict = Depends(get_current_active_user)):
    """Get current user profile"""
    return {
        "id": user["id"],
        "email": user["email"],
        "name": user.get("name"),
        "status": user["status"],
        "role": user["role"],
        "is_verified": user.get("is_verified", False),
        "created_at": user["created_at"],
        "last_login_at": user.get("last_login_at")
    }


@router.patch("/me")
def update_me(
    name: Optional[str] = None,
    user: dict = Depends(get_current_active_user)
):
    """Update current user profile"""
    db = DB()
    with db.connect() as conn:
        if name is not None:
            conn.execute("UPDATE users SET name = ? WHERE id = ?", (name.strip(), user["id"]))
        
        # Return updated user
        updated = conn.execute("SELECT * FROM users WHERE id = ?", (user["id"],)).fetchone()
        return {
            "id": updated["id"],
            "email": updated["email"],
            "name": updated.get("name"),
            "status": updated["status"],
            "role": updated["role"],
            "is_verified": updated.get("is_verified", False),
        }


# --- Logout All Sessions ---
@router.post("/logout-all")
def logout_all(user_id: str = Depends(get_current_user_id)):
    """Revoke ALL sessions for current user"""
    db = DB()
    with db.connect() as conn:
        result = conn.execute(
            "UPDATE auth_sessions SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL",
            (utc_now_iso(), user_id)
        )
    return {"message": "All sessions revoked", "count": result.rowcount}


# --- Admin Endpoints ---
def require_admin(token: str = Depends(oauth2_scheme)) -> str:
    """Dependency that requires admin role."""
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid token")
    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return payload.get("sub")


@router.post("/admin/users/{user_id}/suspend")
def admin_suspend_user(user_id: str, _admin: str = Depends(require_admin)):
    """Suspend a user account (admin only)"""
    db = DB()
    with db.connect() as conn:
        user = conn.execute("SELECT status FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        if user["status"] == "suspended":
            return {"message": "User already suspended"}
        
        now = utc_now_iso()
        conn.execute("UPDATE users SET status = 'suspended' WHERE id = ?", (user_id,))
        # Revoke all sessions
        conn.execute("UPDATE auth_sessions SET revoked_at = ? WHERE user_id = ?", (now, user_id))
    
    return {"message": "User suspended", "user_id": user_id}


@router.post("/admin/users/{user_id}/unsuspend")
def admin_unsuspend_user(user_id: str, _admin: str = Depends(require_admin)):
    """Unsuspend a user account (admin only)"""
    db = DB()
    with db.connect() as conn:
        user = conn.execute("SELECT status FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        if user["status"] != "suspended":
            return {"message": "User is not suspended"}
        
        conn.execute("UPDATE users SET status = 'active' WHERE id = ?", (user_id,))
    
    return {"message": "User unsuspended", "user_id": user_id}


@router.get("/admin/users")
def admin_list_users(_admin: str = Depends(require_admin), status: Optional[str] = None, limit: int = 50):
    """List all users (admin only)"""
    db = DB()
    with db.connect() as conn:
        if status:
            rows = conn.execute(
                "SELECT id, email, status, role, created_at, last_login_at FROM users WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, email, status, role, created_at, last_login_at FROM users ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
    return {"users": [dict(r) for r in rows], "count": len(rows)}


@router.post("/2fa/setup", response_model=TwoFASetupResponse)
def setup_2fa(current_user: UserResponse = Depends(get_current_active_user)):
    # Generate secret
    secret = pyotp.random_base32()
    
    # Save secret to user (but don't enable yet until verified)
    db = DB()
    with db.connect() as conn:
        conn.execute(
            "UPDATE users SET totp_secret = ? WHERE id = ?",
            (secret, current_user.id)
        )
        
    # Generate URI for QR code
    uri = pyotp.totp.TOTP(secret).provisioning_uri(
        name=current_user.email,
        issuer_name="CosmicForge Stratos"
    )
    
    return {"items": secret, "uri": uri}

@router.post("/2fa/verify")
def verify_2fa_setup(req: TwoFAVerifyRequest, current_user: UserResponse = Depends(get_current_active_user)):
    db = DB()
    with db.connect() as conn:
        user = conn.execute("SELECT totp_secret FROM users WHERE id = ?", (current_user.id,)).fetchone()
        if not user or not user["totp_secret"]:
            raise HTTPException(status_code=400, detail="2FA setup not initiated")
            
        totp = pyotp.TOTP(user["totp_secret"])
        if not totp.verify(req.code):
            raise HTTPException(status_code=400, detail="Invalid code")
            
        # Enable 2FA
        conn.execute("UPDATE users SET is_2fa_enabled = 1 WHERE id = ?", (current_user.id,))
        audit_event(conn, "2fa_enabled", user_id=current_user.id, email=current_user.email)
        
    return {"message": "2FA enabled successfully"}

@router.post("/2fa/disable")
def disable_2fa(req: TwoFAVerifyRequest, current_user: UserResponse = Depends(get_current_active_user)):
    db = DB()
    with db.connect() as conn:
        user = conn.execute("SELECT totp_secret, is_2fa_enabled FROM users WHERE id = ?", (current_user.id,)).fetchone()
        if not user or not user["is_2fa_enabled"]:
            raise HTTPException(status_code=400, detail="2FA not enabled")
            
        totp = pyotp.TOTP(user["totp_secret"])
        if not totp.verify(req.code):
            raise HTTPException(status_code=400, detail="Invalid code")
            
        # Disable 2FA and clear secret
        conn.execute("UPDATE users SET is_2fa_enabled = 0, totp_secret = NULL WHERE id = ?", (current_user.id,))
        audit_event(conn, "2fa_disabled", user_id=current_user.id, email=current_user.email)
        
    return {"message": "2FA disabled successfully"}

# --- Session Management ---

@router.get("/sessions", response_model=SessionListResponse)
def get_sessions(current_user: UserResponse = Depends(get_current_active_user)):
    db = DB()
    with db.connect() as conn:
        # Get active sessions (not expired, not revoked)
        # Note: auth_sessions table usage
        sessions = conn.execute("""
            SELECT id, device, ip, created_at, expires_at, 
            CASE WHEN revoked_at IS NOT NULL THEN 1 ELSE 0 END as is_revoked 
            FROM auth_sessions 
            WHERE user_id = ? AND expires_at > ? AND revoked_at IS NULL
            ORDER BY created_at DESC
        """, (current_user.id, utc_now_iso())).fetchall()
        
        # Map to response schema
        return {
            "sessions": [dict(s) for s in sessions]
        }

@router.post("/sessions/revoke")
def revoke_session(req: SessionRevokeRequest, current_user: UserResponse = Depends(get_current_active_user)):
    db = DB()
    with db.connect() as conn:
        conn.execute(
            "UPDATE auth_sessions SET revoked_at = ? WHERE id = ? AND user_id = ?",
            (utc_now_iso(), req.session_id, current_user.id)
        )
        audit_event(conn, "session_revoked", user_id=current_user.id, detail={"session_id": req.session_id})
        
    return {"message": "Session revoked"}
