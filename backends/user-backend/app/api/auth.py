"""
Enhanced Authentication API
- Registration with email verification
- Login with rate limiting and status checks
- Session management (create, refresh, revoke)
- Password reset flow
"""
from fastapi import APIRouter, Depends, Form, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import pyotp
from app.schemas.auth import (
    UserCreate, UserResponse, Token, RefreshTokenReq, 
    BrokerResponse, UserStatus,
    VerifyEmailRequest, ResendVerificationRequest,
    ForgotPasswordRequest, ResetPasswordRequest,
    SessionResponse, SessionListResponse
)
from app.schemas.security import TwoFASetupResponse, TwoFAVerifyRequest, SessionRevokeRequest
from app.core.security import (
    get_password_hash, verify_password, dummy_verify_password,
    create_access_token, create_refresh_token, 
    decode_token,
    generate_otp, hash_otp, verify_otp, hash_token,
    encrypt_totp_secret, match_totp_counter,
)
from app.core.config import settings
from shared_lib.persistence.db import DB, utc_now_iso
import logging
import os
import sqlite3
import threading
import uuid
from typing import List, Optional
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")


# --- Constants ---
MAX_LOGIN_ATTEMPTS = 5            # failed attempts per (account, client IP) pair per window
# Ceiling per account across ALL client IPs. Higher than the per-pair limit so
# that one address guessing at an account cannot lock its owner out from
# another address, while a distributed guesser is still stopped.
MAX_LOGIN_ATTEMPTS_PER_ACCOUNT = 20
MAX_LOGIN_ATTEMPTS_PER_IP = 20    # failed attempts per client IP per window, across accounts
# login_attempts.email prefix of failures that a completed password reset has
# cleared for the account (they keep counting against the client IP).
CLEARED_ATTEMPT_PREFIX = "cleared:"
LOGIN_WINDOW_MINUTES = 15
MAX_VERIFY_ATTEMPTS = 5           # wrong guesses allowed per email-verification code
MAX_RESET_ATTEMPTS = 3            # wrong guesses allowed per password-reset code
CODE_WINDOW_MINUTES = 15
MAX_CODES_PER_EMAIL = 3           # codes issued per address per window
MAX_CODES_PER_EMAIL_PER_DAY = 10  # ...and per 24h, which bounds total guesses at a 6-digit code
MAX_CODES_PER_IP = 10             # codes requested per client IP per window
OTP_EXPIRE_MINUTES = 15
RESET_EXPIRE_MINUTES = 60
RESEND_COOLDOWN_SECONDS = 90  # Cooldown between resend requests
FORGOT_COOLDOWN_SECONDS = 90   # Cooldown between forgot password requests

LOGIN_FAILED_DETAIL = "Incorrect email or password"
CODE_FAILED_DETAIL = "Invalid or expired code. Request a new code."
TOO_MANY_CODES_DETAIL = "Too many codes requested. Try again later."
VERIFICATION_CODE_EVENT = "verification_code_requested"
RESET_CODE_EVENT = "password_reset_requested"
# Stored in users.hashed_password when two sign-ups for the same unverified
# address disagree on the password. It is not a valid hash, so nothing matches
# it; the owner sets a password through the reset flow after verifying.
UNUSABLE_PASSWORD = "!registration-conflict"


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


_LOOPBACK_IPS = {"127.0.0.1", "::1", "localhost"}


def _client_ip(request: Optional[Request]) -> str:
    client = getattr(request, "client", None)
    return (getattr(client, "host", None) or "unknown") if client else "unknown"


def _ip_limitable(ip: Optional[str]) -> bool:
    # A loopback/unknown address means the reverse proxy did not pass the
    # client address on; limiting on it would lock every user out together.
    return bool(ip) and ip != "unknown" and ip not in _LOOPBACK_IPS


def _row_value(row, key: str, default=None):
    """Column value from a sqlite3.Row/dict, tolerating a column that an older database lacks."""
    try:
        return row[key]
    except (KeyError, IndexError):
        return default


def _window_cutoff(minutes: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()


def _check_rate_limit(conn, email: str, ip: Optional[str] = None, account_limit: Optional[int] = None) -> None:
    """Check login rate limiting. Raises 429 if exceeded.

    Three counters of recent FAILED attempts:

    * per (account, client IP) pair -- ``MAX_LOGIN_ATTEMPTS``. Someone guessing
      at an account locks only their own address out of it, not the owner.
      (When the proxy passes no client address every caller shares one
      "address", and this is the old per-account limit.)
    * per account across all addresses -- ``account_limit`` (default
      ``MAX_LOGIN_ATTEMPTS_PER_ACCOUNT``), bounding a distributed guesser.
    * per client IP across all accounts -- ``MAX_LOGIN_ATTEMPTS_PER_IP``.
    """
    cutoff = _window_cutoff(LOGIN_WINDOW_MINUTES)
    if account_limit is None:
        account_limit = MAX_LOGIN_ATTEMPTS_PER_ACCOUNT
    rows = conn.execute(
        "SELECT COUNT(*) as cnt FROM login_attempts "
        "WHERE email = ? AND ip IS ? AND attempted_at > ? AND success = 0",
        (email, ip, cutoff)
    ).fetchone()
    if rows and rows["cnt"] >= MAX_LOGIN_ATTEMPTS:
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")
    rows = conn.execute(
        "SELECT COUNT(*) as cnt FROM login_attempts WHERE email = ? AND attempted_at > ? AND success = 0",
        (email, cutoff)
    ).fetchone()
    if rows and rows["cnt"] >= account_limit:
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")
    if _ip_limitable(ip):
        rows = conn.execute(
            "SELECT COUNT(*) as cnt FROM login_attempts WHERE ip = ? AND attempted_at > ? AND success = 0",
            (ip, cutoff)
        ).fetchone()
        if rows and rows["cnt"] >= MAX_LOGIN_ATTEMPTS_PER_IP:
            raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")


def begin_login_attempt(conn, key: str, ip: Optional[str], account_limit: Optional[int] = None) -> str:
    """Rate-limit check plus a durable record of this attempt, as one atomic step.

    DB.connect() commits only when its block exits cleanly, so an attempt that
    is written and then followed by ``raise HTTPException`` is rolled back and
    never counted. The attempt is therefore stored as a FAILURE and committed
    here, before any credential is looked at; a caller that goes on to
    authenticate flips it with mark_login_attempt_success(). The immediate
    transaction also stops parallel requests from all passing the check.

    ``key`` is the normalised email (other limiters namespace it, e.g. "admin:").
    ``account_limit`` overrides the per-account ceiling across all client IPs.
    Must be called before anything else is written on ``conn``.
    """
    attempt_id = str(uuid.uuid4())
    conn.execute("BEGIN IMMEDIATE")
    try:
        _check_rate_limit(conn, key, ip, account_limit)
        conn.execute(
            "INSERT INTO login_attempts (id, email, ip, success, attempted_at) VALUES (?, ?, ?, 0, ?)",
            (attempt_id, key, ip, utc_now_iso())
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return attempt_id


def mark_login_attempt_success(conn, attempt_id: str) -> None:
    # Only this attempt: failures recorded from other addresses are NOT cleared
    # by a successful login (the per-account ceiling keeps counting them).
    conn.execute("UPDATE login_attempts SET success = 1 WHERE id = ?", (attempt_id,))


def clear_login_failures(conn, email: str) -> None:
    """Forget an account's failed logins once its owner proved control of the
    mailbox (completed password reset), so the owner is not kept locked out by
    someone else's guesses. The rows stay, re-keyed, and keep counting against
    the client IP that produced them."""
    conn.execute(
        "UPDATE login_attempts SET email = ? WHERE email = ? AND success = 0",
        (CLEARED_ATTEMPT_PREFIX + email, email)
    )


def _ensure_totp_counter_column(conn) -> None:
    """Add ``users.totp_last_counter`` if absent (idempotent, safe under concurrency)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    if not cols or "totp_last_counter" in cols:
        return
    try:
        conn.execute("ALTER TABLE users ADD COLUMN totp_last_counter INTEGER")
    except sqlite3.OperationalError as exc:
        if "duplicate column name" not in str(exc).lower():
            raise


def consume_totp_code(conn, user_id: str, stored_secret: Optional[str], code: Optional[str]) -> bool:
    """Accept an authenticator code at most once.

    The time step of the last accepted code is stored per user; a code for
    that step or an earlier one is refused, so a captured code cannot be
    replayed during the ~90 s it would otherwise stay valid. The guarded
    UPDATE makes two simultaneous uses of one code resolve to a single winner,
    and it is committed at once so the code is spent whatever happens next.
    Call it only while nothing else is pending on ``conn``.
    """
    _ensure_totp_counter_column(conn)
    row = conn.execute("SELECT totp_last_counter FROM users WHERE id = ?", (user_id,)).fetchone()
    if not row:
        return False
    counter = match_totp_counter(stored_secret, code, row["totp_last_counter"])
    if counter is None:
        return False
    spent = conn.execute(
        "UPDATE users SET totp_last_counter = ? "
        "WHERE id = ? AND (totp_last_counter IS NULL OR totp_last_counter < ?)",
        (counter, user_id, counter)
    )
    conn.commit()
    return spent.rowcount == 1


def _count_audit_events(conn, where: str, params: tuple) -> int:
    row = conn.execute(f"SELECT COUNT(*) as cnt FROM auth_audit_log WHERE {where}", params).fetchone()
    return int(row["cnt"]) if row else 0


def _reserve_code_request(conn, event_type: str, email: str, ip: Optional[str]) -> bool:
    """Count a request for an emailed code against the per-address and per-IP limits.

    Returns False when a limit is reached. The request is counted (and
    committed) whether or not the address belongs to an account, so the limit
    behaves identically for registered and unregistered emails.
    Must be called before anything else is written on ``conn``.
    """
    cutoff = _window_cutoff(CODE_WINDOW_MINUTES)
    conn.execute("BEGIN IMMEDIATE")
    try:
        allowed = (
            _count_audit_events(conn, "event_type = ? AND email = ? AND created_at > ?",
                                (event_type, email, cutoff)) < MAX_CODES_PER_EMAIL
            and _count_audit_events(conn, "event_type = ? AND email = ? AND created_at > ?",
                                    (event_type, email, _window_cutoff(24 * 60))) < MAX_CODES_PER_EMAIL_PER_DAY
        )
        if allowed and _ip_limitable(ip):
            allowed = _count_audit_events(
                conn, "event_type IN (?, ?) AND ip = ? AND created_at > ?",
                (VERIFICATION_CODE_EVENT, RESET_CODE_EVENT, ip, cutoff)) < MAX_CODES_PER_IP
        if allowed:
            audit_event(conn, event_type, email=email, ip=ip)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return allowed


def _issue_code(conn, table: str, user_id: str, expire_minutes: int) -> str:
    """Create a fresh one-time code; every older unused code stops working."""
    assert table in ("email_verifications", "password_resets")
    now = utc_now_iso()
    conn.execute(f"UPDATE {table} SET used_at = ? WHERE user_id = ? AND used_at IS NULL", (now, user_id))
    otp = generate_otp()
    expires = (datetime.now(timezone.utc) + timedelta(minutes=expire_minutes)).isoformat()
    conn.execute(
        f"INSERT INTO {table} (id, user_id, code_hash, expires_at, attempts, created_at) VALUES (?, ?, ?, ?, 0, ?)",
        (str(uuid.uuid4()), user_id, hash_otp(otp), expires, now)
    )
    return otp


def _consume_code_attempt(conn, table: str, user_id: Optional[str], max_attempts: int):
    """Spend one guess on the user's current code and return its row, or None.

    The guess is counted and committed BEFORE the code is compared, so a wrong
    answer followed by ``raise`` can no longer be rolled back, and the guarded
    UPDATE cannot be raced past ``max_attempts``.
    """
    assert table in ("email_verifications", "password_resets")
    if not user_id:
        return None
    row = conn.execute(
        f"SELECT * FROM {table} WHERE user_id = ? AND used_at IS NULL ORDER BY created_at DESC LIMIT 1",
        (user_id,)
    ).fetchone()
    if not row:
        return None
    expires = datetime.fromisoformat(row["expires_at"].replace('Z', '+00:00'))
    if datetime.now(timezone.utc) > expires:
        return None
    counted = conn.execute(
        f"UPDATE {table} SET attempts = attempts + 1 WHERE id = ? AND used_at IS NULL AND attempts < ?",
        (row["id"], max_attempts)
    )
    conn.commit()
    return row if counted.rowcount == 1 else None


# --- One-time code delivery ---
_CODE_EMAILS = {
    "verification": ("Your CosmicForge verification code", "verify your email address", OTP_EXPIRE_MINUTES),
    "password reset": ("Your CosmicForge password reset code", "reset your password", RESET_EXPIRE_MINUTES),
}


def _smtp_configured() -> bool:
    return bool((settings.SMTP_HOST or os.getenv("SMTP_HOST")) and (settings.SMTP_USER or os.getenv("SMTP_USER")))


def _export_smtp_settings() -> None:
    # EmailChannel reads SMTP_* from the process environment. Pass on values
    # that only reached settings (.env), including this service's
    # SMTP_PASSWORD / SMTP_FROM_EMAIL spellings.
    for env_name, value in (
        ("SMTP_HOST", settings.SMTP_HOST), ("SMTP_PORT", str(settings.SMTP_PORT or "")),
        ("SMTP_USER", settings.SMTP_USER), ("SMTP_PASS", settings.SMTP_PASSWORD),
        ("SMTP_FROM", settings.SMTP_FROM_EMAIL),
    ):
        if value and not os.environ.get(env_name):
            os.environ[env_name] = value


def _run_in_background(fn) -> None:
    threading.Thread(target=fn, name="auth-email", daemon=True).start()


def _deliver_code(email: str, purpose: str, code: str) -> None:
    """Email a one-time code. Never raises, never blocks the request, and never
    logs the code outside local development."""
    subject, action, minutes = _CODE_EMAILS[purpose]
    if not _smtp_configured():
        if settings.production:
            logger.error(
                "[AUTH] SMTP is not configured: a %s code was NOT delivered. "
                "Set SMTP_HOST, SMTP_USER and SMTP_PASSWORD.", purpose)
        else:
            # Local development convenience only.
            logger.info("[AUTH] SMTP not configured (non-production): %s code for %s is %s", purpose, email, code)
        return

    text = (f"Your code to {action} is {code}. It expires in {minutes} minutes. "
            "If you did not request it, you can ignore this email.")
    html = (f"<p>Your code to {action} is:</p><p style=\"font-size:24px;letter-spacing:4px\"><b>{code}</b></p>"
            f"<p>It expires in {minutes} minutes. If you did not request it, you can ignore this email.</p>")

    def _send() -> None:
        try:
            _export_smtp_settings()
            from shared_lib.notifications.channels.email import EmailChannel
            if not EmailChannel.send(email, subject, html, text):
                logger.error("[AUTH] %s email could not be sent", purpose)
        except Exception as exc:
            logger.error("[AUTH] %s email failed: %s", purpose, type(exc).__name__)

    try:
        _run_in_background(_send)
    except Exception as exc:
        logger.error("[AUTH] %s email could not be queued: %s", purpose, type(exc).__name__)


# --- Registration ---
@router.post("/register", response_model=UserResponse)
def register(user: UserCreate, request: Request):
    email = normalize_email(user.email)
    ip = _client_ip(request)
    hashed = get_password_hash(user.password)
    db = DB()
    with db.connect() as conn:
        # Every registration emails a code, so it shares the code limits.
        if not _reserve_code_request(conn, VERIFICATION_CODE_EVENT, email, ip):
            raise HTTPException(status_code=429, detail=TOO_MANY_CODES_DETAIL)

        existing = conn.execute(
            "SELECT id, status, hashed_password, created_at FROM users WHERE email = ?", (email,)
        ).fetchone()

        uid = str(uuid.uuid4())
        now = utc_now_iso()
        created_at = now

        if existing:
            if existing["status"] != "pending_verification":
                # Active/Verified (or suspended/deleted) user
                raise HTTPException(status_code=400, detail="Registration failed")

            # Unverified account: a repeat sign-up only re-sends the code. It
            # never replaces the stored credentials -- anyone can submit a
            # sign-up for someone else's address, and whichever password is
            # stored becomes live the moment the real owner enters the code.
            uid = existing["id"]
            created_at = existing["created_at"]
            if not verify_password(user.password, existing["hashed_password"]):
                # Two sign-ups disagree on the password and neither has proven
                # ownership of the mailbox: trust neither. The owner verifies
                # the address and then sets a password via "forgot password".
                if existing["hashed_password"] != UNUSABLE_PASSWORD:
                    conn.execute(
                        "UPDATE users SET hashed_password = ?, updated_at = ? WHERE id = ?",
                        (UNUSABLE_PASSWORD, now, uid)
                    )
                audit_event(conn, "registration_conflict", user_id=uid, email=email, ip=ip)
        else:
            # Create new user
            conn.execute("""
                INSERT INTO users (id, email, hashed_password, status, role, is_verified, created_at, updated_at,
                locale, country, timezone, terms_accepted_at, risk_disclaimer_accepted_at, marketing_session_id, selected_plan_id) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    uid, email, hashed, "pending_verification", "user", False, now, now,
                    user.locale, user.country, user.timezone, 
                    user.terms_accepted_at, user.risk_disclaimer_accepted_at,
                    user.marketing_session_id, user.selected_plan_id
                )
            )

            # Update marketing session with converted user ID
            if user.marketing_session_id:
                try:
                    conn.execute(
                        "UPDATE marketing_sessions SET converted_user_id = ? WHERE id = ?",
                        (uid, user.marketing_session_id)
                    )
                except Exception as exc:
                    # Attribution is best-effort; it must not fail a sign-up.
                    logger.warning("[AUTH] marketing session attribution skipped: %s", type(exc).__name__)

            audit_event(conn, "user_registered", user_id=uid, email=email, ip=ip)

        otp = _issue_code(conn, "email_verifications", uid, OTP_EXPIRE_MINUTES)
        audit_event(conn, "verification_sent", user_id=uid, email=email, ip=ip)
        _deliver_code(email, "verification", otp)

        return {
            "id": uid, 
            "email": email, 
            "status": "pending_verification",
            "role": "user",
            "is_verified": False, 
            "created_at": created_at
        }


# --- Email Verification ---
@router.post("/verify-email")
def verify_email(req: VerifyEmailRequest):
    email = normalize_email(req.email)
    db = DB()
    with db.connect() as conn:
        user = conn.execute("SELECT id, status, hashed_password FROM users WHERE email = ?", (email,)).fetchone()

        if user and user["status"] == "active":
            return {"message": "Email already verified"}

        # Only an account that is waiting for verification can be activated
        # here (never a suspended or deleted one).
        pending_id = user["id"] if user and user["status"] == "pending_verification" else None
        verification = _consume_code_attempt(conn, "email_verifications", pending_id, MAX_VERIFY_ATTEMPTS)

        if not verification or not verify_otp(req.code, verification["code_hash"]):
            raise HTTPException(status_code=400, detail=CODE_FAILED_DETAIL)

        # Mark as used and activate user
        now = utc_now_iso()
        conn.execute("UPDATE email_verifications SET used_at = ? WHERE id = ?", (now, verification["id"]))
        conn.execute("UPDATE users SET status = 'active', is_verified = 1 WHERE id = ?", (user["id"],))
        audit_event(conn, "email_verified", user_id=user["id"], email=email)

        if user["hashed_password"] == UNUSABLE_PASSWORD:
            return {
                "message": "Email verified successfully. Set your password with 'Forgot password' before signing in.",
                "password_reset_required": True,
            }
        return {"message": "Email verified successfully"}


@router.post("/resend-verification")
def resend_verification(req: ResendVerificationRequest, request: Request = None):
    email = normalize_email(req.email)
    ip = _client_ip(request)
    db = DB()
    with db.connect() as conn:
        if not _reserve_code_request(conn, VERIFICATION_CODE_EVENT, email, ip):
            raise HTTPException(status_code=429, detail=TOO_MANY_CODES_DETAIL)

        user = conn.execute("SELECT id, status FROM users WHERE email = ?", (email,)).fetchone()

        # Same message regardless to prevent enumeration
        if not user or user["status"] != "pending_verification":
            return {"message": "If your email is registered and pending, a new code will be sent."}

        otp = _issue_code(conn, "email_verifications", user["id"], OTP_EXPIRE_MINUTES)
        audit_event(conn, "verification_sent", user_id=user["id"], email=email, ip=ip)
        _deliver_code(email, "verification", otp)

        return {"message": "If your email is registered and pending, a new code will be sent."}


# --- Login ---
@router.post("/login", response_model=Token)
def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    request: Request = None,
    totp_code: Optional[str] = Form(None),
):
    db = DB()
    ip = _client_ip(request)
    
    with db.connect() as conn:
        # Normalize email
        email = normalize_email(form_data.username)
        
        # Rate limiting: raises 429, otherwise records this attempt as a
        # (committed) failure until it is proven otherwise below.
        attempt_id = begin_login_attempt(conn, email, ip)
        
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        
        # One answer for "no such account" and "wrong password", and the same
        # hashing work either way, so neither wording nor timing reveals
        # whether an email is registered.
        if not row:
            dummy_verify_password(form_data.password)
            password_ok = False
        else:
            password_ok = verify_password(form_data.password, row["hashed_password"])

        if not password_ok:
            audit_event(conn, "login_failed", user_id=row["id"] if row else None, email=email, ip=ip)
            conn.commit()
            raise HTTPException(status_code=400, detail=LOGIN_FAILED_DETAIL)
        
        if row["status"] == "pending_verification":
            # The password was right, so this is not a guessing attempt.
            mark_login_attempt_success(conn, attempt_id)
            conn.commit()
            # Send a fresh code unless one went out in the last minute or the
            # address has reached its code limit.
            recent = conn.execute(
                "SELECT created_at FROM email_verifications WHERE user_id = ? ORDER BY created_at DESC LIMIT 1", 
                (row["id"],)
            ).fetchone()
            
            should_send = True
            if recent:
                last_time = datetime.fromisoformat(recent["created_at"].replace('Z', '+00:00'))
                if datetime.now(timezone.utc) - last_time < timedelta(minutes=1):
                    should_send = False # Rate limit OTP generation
            
            if should_send and _reserve_code_request(conn, VERIFICATION_CODE_EVENT, email, ip):
                otp = _issue_code(conn, "email_verifications", row["id"], OTP_EXPIRE_MINUTES)
                audit_event(conn, "verification_sent_login", user_id=row["id"], email=email, ip=ip)
                # Commit before raising: the 403 below would otherwise roll the new code back.
                conn.commit()
                _deliver_code(email, "verification", otp)
            
            raise HTTPException(status_code=403, detail="User not verified")
            
        elif row["status"] != "active":
            mark_login_attempt_success(conn, attempt_id)
            conn.commit()
            raise HTTPException(status_code=403, detail="Account suspended or deleted")
        
        uid = row["id"]
        role = row["role"] or "user"

        # Second factor: required whenever the account has 2FA switched on.
        if _row_value(row, "is_2fa_enabled"):
            if not (totp_code or "").strip():
                # No second factor was guessed, so this does not count as a failure.
                mark_login_attempt_success(conn, attempt_id)
                conn.commit()
                raise HTTPException(
                    status_code=401,
                    detail={"code": "TOTP_REQUIRED", "message": "Two-factor authentication code required"},
                )
            if not consume_totp_code(conn, uid, _row_value(row, "totp_secret"), totp_code):
                # Wrong, or already used (replay). Stays recorded as a failed
                # attempt (rate-limited like a wrong password).
                audit_event(conn, "login_failed_2fa", user_id=uid, email=email, ip=ip)
                conn.commit()
                raise HTTPException(
                    status_code=401,
                    detail={"code": "TOTP_INVALID", "message": "Invalid two-factor authentication code"},
                )
        
        # Record successful login
        mark_login_attempt_success(conn, attempt_id)
        conn.execute("UPDATE users SET last_login_at = ? WHERE id = ?", (utc_now_iso(), uid))
        
        # Refresh token (no database access) and its session row.
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

    # The access token is minted only AFTER the transaction above has been
    # committed and its connection closed: building it reads the user's
    # entitlements on a connection of its own, and SQLite has a single writer.
    access_token = create_access_token(uid, role=role)
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
            logger.warning("[SECURITY] Refresh token reuse detected for user %s. Revoking all sessions.", uid)
            conn.execute(
                "UPDATE auth_sessions SET revoked_at = ? WHERE user_id = ?",
                (utc_now_iso(), uid)
            )
            # Commit before raising, or the revocation is rolled back with the 401.
            conn.commit()
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
        
        # New refresh token (no database access); the access token is minted
        # after this transaction has committed (see login).
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

    new_access = create_access_token(uid, role=role)
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
def forgot_password(req: ForgotPasswordRequest, request: Request = None):
    """Request password reset code"""
    email = normalize_email(req.email)
    ip = _client_ip(request)
    db = DB()
    with db.connect() as conn:
        if not _reserve_code_request(conn, RESET_CODE_EVENT, email, ip):
            raise HTTPException(status_code=429, detail=TOO_MANY_CODES_DETAIL)

        user = conn.execute("SELECT id FROM users WHERE email = ? AND status = 'active'", (email,)).fetchone()
        
        # Same message regardless to prevent enumeration
        if not user:
            return {"message": "If your email is registered, a reset code will be sent."}
        
        # Issuing a code invalidates any earlier unused one.
        otp = _issue_code(conn, "password_resets", user["id"], RESET_EXPIRE_MINUTES)
        _deliver_code(email, "password reset", otp)
        
        return {"message": "If your email is registered, a reset code will be sent."}


@router.post("/reset-password")
def reset_password(req: ResetPasswordRequest):
    """Reset password with code"""
    email = normalize_email(req.email)
    db = DB()
    with db.connect() as conn:
        user = conn.execute("SELECT id FROM users WHERE email = ? AND status = 'active'", (email,)).fetchone()

        # Unknown address, no pending code, expired, out of attempts or wrong
        # code all get the same answer.
        reset = _consume_code_attempt(conn, "password_resets", user["id"] if user else None, MAX_RESET_ATTEMPTS)
        if not reset or not verify_otp(req.code, reset["code_hash"]):
            raise HTTPException(status_code=400, detail=CODE_FAILED_DETAIL)
        
        # Update password and mark reset as used
        new_hash = get_password_hash(req.new_password)
        now = utc_now_iso()
        
        conn.execute("UPDATE password_resets SET used_at = ? WHERE id = ?", (now, reset["id"]))
        conn.execute("UPDATE users SET hashed_password = ? WHERE id = ?", (new_hash, user["id"]))
        
        # Revoke all sessions
        conn.execute("UPDATE auth_sessions SET revoked_at = ? WHERE user_id = ?", (now, user["id"]))
        # The owner proved control of the mailbox: failed logins recorded
        # against the account (possibly someone else's guesses) no longer lock it.
        clear_login_failures(conn, email)
        audit_event(conn, "password_reset_completed", user_id=user["id"], email=email)
        
        return {"message": "Password reset successfully. Please login."}


# --- Broker Management (requires active user) ---
@router.get("/user/brokers", response_model=List[BrokerResponse])
def list_brokers(user: dict = Depends(get_current_active_user)):
    db = DB()
    with db.connect() as conn:
        rows = conn.execute("SELECT * FROM broker_accounts WHERE user_id = ?", (user["id"],)).fetchall()
        return [dict(r) for r in rows]


# The legacy POST /user/brokers (per-field encryption with CREDENTIAL_KEY into
# columns that no longer exist) was removed: no client calls it. Broker
# accounts are linked through app.api.brokers / broker_service.


# --- User Profile ---
from app.core.rbac import resolve_permissions
try:
    from app.core.billing_service import get_user_subscription
except ImportError:
    def get_user_subscription(uid): return {"entitlements": {}}

@router.get("/me")
def get_me(user: dict = Depends(get_current_active_user)):
    """Get current user profile"""
    # 1. Resolve Permissions
    permissions = resolve_permissions(user["role"])
    
    # 2. Fetch Entitlements
    entitlements = {}
    try:
        sub = get_user_subscription(user["id"])
        entitlements = sub.get("entitlements", {})
    except Exception:
        pass

    return {
        "id": user["id"],
        "email": user["email"],
        "name": user.get("name"),
        "status": user["status"],
        "role": user["role"],
        "permissions": permissions,
        "entitlements": entitlements,
        "is_verified": user.get("is_verified", False),
        "is_2fa_enabled": bool(user.get("is_2fa_enabled")),
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
        updated = dict(conn.execute("SELECT * FROM users WHERE id = ?", (user["id"],)).fetchone())
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

# --- Admin Endpoints ---
# Moved to app.api.admin



TWO_FA_INVALID_DETAIL = "Invalid code"


def _two_fa_attempt_key(user_id: str) -> str:
    # Authenticator-code guesses share the login_attempts table, under their own key.
    return f"2fa:{user_id}"


@router.post("/2fa/setup", response_model=TwoFASetupResponse)
def setup_2fa(current_user: dict = Depends(get_current_active_user)):
    # Replacing the secret of an account that already has 2FA on would let a
    # stolen access token swap the second factor without knowing a code.
    if current_user.get("is_2fa_enabled"):
        raise HTTPException(status_code=400, detail="2FA is already enabled. Disable it first.")

    # Generate secret
    secret = pyotp.random_base32()
    
    # Save secret (encrypted) to user (but don't enable yet until verified)
    db = DB()
    with db.connect() as conn:
        # A new secret starts a new code sequence: forget the last accepted step.
        _ensure_totp_counter_column(conn)
        conn.execute(
            "UPDATE users SET totp_secret = ?, totp_last_counter = NULL WHERE id = ?",
            (encrypt_totp_secret(secret), current_user["id"])
        )
        
    # Generate URI for QR code
    uri = pyotp.totp.TOTP(secret).provisioning_uri(
        name=current_user["email"],
        issuer_name="CosmicForge Stratos"
    )
    
    return {"items": secret, "uri": uri}

@router.post("/2fa/verify")
def verify_2fa_setup(req: TwoFAVerifyRequest, request: Request = None,
                     current_user: dict = Depends(get_current_active_user)):
    user_id = current_user["id"]
    db = DB()
    with db.connect() as conn:
        # Only the authenticated account holder can spend these attempts, so the
        # strict limit applies across all addresses (no lockout-by-stranger here).
        attempt_id = begin_login_attempt(
            conn, _two_fa_attempt_key(user_id), _client_ip(request), account_limit=MAX_LOGIN_ATTEMPTS)

        user = conn.execute("SELECT totp_secret, is_2fa_enabled FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user or not user["totp_secret"]:
            mark_login_attempt_success(conn, attempt_id)
            conn.commit()
            raise HTTPException(status_code=400, detail="2FA setup not initiated")
            
        if not consume_totp_code(conn, user_id, user["totp_secret"], req.code):
            raise HTTPException(status_code=400, detail=TWO_FA_INVALID_DETAIL)
            
        # Enable 2FA
        mark_login_attempt_success(conn, attempt_id)
        conn.execute("UPDATE users SET is_2fa_enabled = 1 WHERE id = ?", (user_id,))
        audit_event(conn, "2fa_enabled", user_id=user_id, email=current_user["email"])
        
    return {"message": "2FA enabled successfully"}

@router.post("/2fa/disable")
def disable_2fa(req: TwoFAVerifyRequest, request: Request = None,
                current_user: dict = Depends(get_current_active_user)):
    user_id = current_user["id"]
    db = DB()
    with db.connect() as conn:
        # Only the authenticated account holder can spend these attempts, so the
        # strict limit applies across all addresses (no lockout-by-stranger here).
        attempt_id = begin_login_attempt(
            conn, _two_fa_attempt_key(user_id), _client_ip(request), account_limit=MAX_LOGIN_ATTEMPTS)

        user = conn.execute("SELECT totp_secret, is_2fa_enabled FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user or not user["is_2fa_enabled"]:
            mark_login_attempt_success(conn, attempt_id)
            conn.commit()
            raise HTTPException(status_code=400, detail="2FA not enabled")
            
        if not consume_totp_code(conn, user_id, user["totp_secret"], req.code):
            raise HTTPException(status_code=400, detail=TWO_FA_INVALID_DETAIL)
            
        # Disable 2FA and clear secret
        mark_login_attempt_success(conn, attempt_id)
        conn.execute(
            "UPDATE users SET is_2fa_enabled = 0, totp_secret = NULL, totp_last_counter = NULL WHERE id = ?",
            (user_id,))
        audit_event(conn, "2fa_disabled", user_id=user_id, email=current_user["email"])
        
    return {"message": "2FA disabled successfully"}

# --- Session Management ---
# GET /sessions is served by list_sessions above.

@router.post("/sessions/revoke")
def revoke_session_by_id(req: SessionRevokeRequest, current_user: dict = Depends(get_current_active_user)):
    db = DB()
    with db.connect() as conn:
        conn.execute(
            "UPDATE auth_sessions SET revoked_at = ? WHERE id = ? AND user_id = ?",
            (utc_now_iso(), req.session_id, current_user["id"])
        )
        audit_event(conn, "session_revoked", user_id=current_user["id"], details={"session_id": req.session_id})
        
    return {"message": "Session revoked"}
