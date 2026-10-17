"""Admin console authentication.

Password login, optional/mandatory TOTP second factor (Section H 8.4), rotating
refresh sessions with reuse detection, and httpOnly cookie sessions.

Login flow:

1. ``POST /login`` (form: username, password). The answer is one of
   * a full session (admin without MFA while ``ADMIN_MFA_REQUIRED`` is off);
   * ``{"mfa_required": true, "mfa_token": ...}`` -- the admin has MFA: post
     the token with a TOTP code (or a recovery code) to ``/mfa/verify``;
   * ``{"mfa_enrollment_required": true, "enrollment_token": ...}`` -- MFA is
     required but not set up. That token can do nothing except
     ``/mfa/setup`` + ``/mfa/enroll``; every admin route rejects it.
2. A full session sets the ``cf_admin_*`` cookies (and, during the
   AUTH_BEARER_COMPAT window, also returns the token strings).

Step tokens carry their own audience (``admin-mfa``), live 5 / 15 minutes and
die when the admin's password changes. TOTP codes are single-use (the last
accepted time step is stored); recovery codes are single-use salted hashes.
Enrolment, disabling, recovery-code use and regeneration are written to
``auth_audit_log``.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import pyotp
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel

from app.core.config import settings
from app.core.security import (
    ADMIN_MFA_CHALLENGE,
    ADMIN_MFA_ENROLL,
    create_admin_access_token,
    create_admin_mfa_token,
    create_admin_refresh_token,
    decode_admin_mfa_token,
    decode_admin_token,
    dummy_verify_password,
    encrypt_totp_secret,
    generate_recovery_codes,
    hash_recovery_code,
    hash_token,
    match_totp_counter,
    password_fingerprint,
    verify_password,
    verify_recovery_code,
)
from app.core.session_cookies import (
    ADMIN_REALM,
    admin_mfa_required,
    clear_cookies,
    issue_session_cookies,
    refresh_token_from,
    token_body,
)
from shared_lib.persistence.admin_mfa_schema import ensure_admin_mfa_schema
from shared_lib.persistence.db import DB

router = APIRouter()

MFA_ISSUER_NAME = "CosmicForge Admin"


# --------------------------------------------------------------------------- schemas

class AdminAuthResponse(BaseModel):
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    token_type: str = "bearer"
    csrf_token: Optional[str] = None
    mfa_required: Optional[bool] = None
    mfa_token: Optional[str] = None
    mfa_enrollment_required: Optional[bool] = None
    enrollment_token: Optional[str] = None
    recovery_codes: Optional[List[str]] = None
    recovery_codes_remaining: Optional[int] = None


class AdminMeResponse(BaseModel):
    id: str
    email: str
    full_name: str | None
    role: str
    is_superuser: int
    mfa_enabled: bool = False


class RefreshRequest(BaseModel):
    refresh_token: Optional[str] = None


class MfaVerifyRequest(BaseModel):
    mfa_token: str
    code: Optional[str] = None
    recovery_code: Optional[str] = None


class MfaSetupRequest(BaseModel):
    enrollment_token: Optional[str] = None


class MfaEnrollRequest(BaseModel):
    code: str
    enrollment_token: Optional[str] = None


class MfaCodeRequest(BaseModel):
    code: str


from app.core.deps import get_current_admin  # noqa: E402
# Same durable, atomic attempt tracking as the user login (shared login_attempts table).
from app.api.auth import (  # noqa: E402
    MAX_LOGIN_ATTEMPTS,
    audit_event,
    begin_login_attempt,
    mark_login_attempt_success,
)


# --------------------------------------------------------------------------- helpers

def _admin_attempt_key(email: str) -> str:
    # Namespaced so admin and user counters for the same address stay separate.
    return f"admin:{(email or '').lower().strip()}"


def _mfa_attempt_key(admin_id: str) -> str:
    return f"admin-mfa:{admin_id}"


def _client_ip(request: Optional[Request]) -> str:
    client = getattr(request, "client", None)
    return (getattr(client, "host", None) or "unknown") if client else "unknown"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _value(row, key: str, default=None):
    try:
        return row[key]
    except (KeyError, IndexError):
        return default


def _mfa_enabled(row) -> bool:
    return bool(_value(row, "mfa_enabled")) and bool(_value(row, "mfa_secret"))


def _audit(conn, event_type: str, admin, request: Optional[Request], details: Optional[dict] = None) -> None:
    audit_event(conn, event_type, user_id=str(_value(admin, "id")), email=_value(admin, "email"),
                ip=_client_ip(request), details={"actor": "admin", **(details or {})})


def _issue_admin_session(conn, admin, request: Request, *, mfa_verified: bool) -> Tuple[str, str]:
    """Insert a refresh session row; returns (access, refresh)."""
    now = _now()
    conn.execute("UPDATE admins SET last_login_at = ? WHERE id = ?", (now, admin["id"]))
    access_token = create_admin_access_token(admin["id"], role=admin["role"])
    refresh_token = create_admin_refresh_token(admin["id"])
    expires_at = (datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)).isoformat()
    conn.execute(
        """
        INSERT INTO admin_sessions
        (id, admin_id, refresh_token_hash, device, ip, created_at, expires_at, mfa_verified)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (str(uuid.uuid4()), admin["id"], hash_token(refresh_token),
         (request.headers.get("User-Agent", "Unknown Device") or "")[:255], _client_ip(request),
         now, expires_at, 1 if mfa_verified else 0),
    )
    return access_token, refresh_token


def _session_body(response: Response, request: Request, access: str, refresh: str, **extra) -> dict:
    csrf = issue_session_cookies(response, ADMIN_REALM, access, refresh, request)
    return token_body(access, refresh, csrf, **extra)


def consume_admin_totp_code(conn, admin_id: str, stored_secret: Optional[str], code: Optional[str]) -> bool:
    """Accept an authenticator code at most once (stored last time step), like
    the user 2FA. Commits at once so the code is spent whatever happens next."""
    row = conn.execute("SELECT mfa_last_counter FROM admins WHERE id = ?", (admin_id,)).fetchone()
    if not row:
        return False
    counter = match_totp_counter(stored_secret, code, row["mfa_last_counter"])
    if counter is None:
        return False
    spent = conn.execute(
        "UPDATE admins SET mfa_last_counter = ? "
        "WHERE id = ? AND (mfa_last_counter IS NULL OR mfa_last_counter < ?)",
        (counter, admin_id, counter),
    )
    conn.commit()
    return spent.rowcount == 1


def consume_recovery_code(conn, admin_id: str, code: Optional[str]) -> bool:
    rows = conn.execute(
        "SELECT id, code_hash FROM admin_mfa_recovery_codes WHERE admin_id = ? AND used_at IS NULL",
        (admin_id,),
    ).fetchall()
    for row in rows:
        if verify_recovery_code(code, row["code_hash"]):
            spent = conn.execute(
                "UPDATE admin_mfa_recovery_codes SET used_at = ? WHERE id = ? AND used_at IS NULL",
                (_now(), row["id"]),
            )
            conn.commit()
            return spent.rowcount == 1
    return False


def _remaining_recovery_codes(conn, admin_id: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM admin_mfa_recovery_codes WHERE admin_id = ? AND used_at IS NULL",
        (admin_id,),
    ).fetchone()
    return int(row["cnt"]) if row else 0


def _replace_recovery_codes(conn, admin_id: str) -> List[str]:
    codes = generate_recovery_codes()
    now = _now()
    conn.execute("DELETE FROM admin_mfa_recovery_codes WHERE admin_id = ?", (admin_id,))
    for code in codes:
        conn.execute(
            "INSERT INTO admin_mfa_recovery_codes (id, admin_id, code_hash, created_at) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), admin_id, hash_recovery_code(code), now),
        )
    return codes


def _bearer_from(request: Request) -> Optional[str]:
    header = request.headers.get("Authorization") or ""
    scheme, _, token = header.partition(" ")
    return token.strip() if scheme.lower() == "bearer" and token.strip() else None


def _enrolling_admin_id(request: Request, enrollment_token: Optional[str]) -> Tuple[str, Optional[dict]]:
    """Who is enrolling, from the enrollment step token or a full admin session.

    No database access, so it can run before begin_login_attempt()."""
    if enrollment_token:
        payload = decode_admin_mfa_token(enrollment_token, ADMIN_MFA_ENROLL)
        if not payload:
            raise HTTPException(status_code=401, detail="Enrollment token invalid or expired. Sign in again.")
        return payload["sub"], payload
    payload = decode_admin_token(_bearer_from(request) or "")
    if not payload or payload.get("type") != "admin_access":
        raise HTTPException(status_code=401, detail="Could not validate admin credentials",
                            headers={"WWW-Authenticate": "Bearer"})
    return payload["sub"], None


def _load_enrolling_admin(conn, admin_id: str, step_payload: Optional[dict]):
    admin = conn.execute("SELECT * FROM admins WHERE id = ?", (admin_id,)).fetchone()
    if not admin or admin["is_active"] != 1:
        raise HTTPException(status_code=403, detail="Admin account is inactive")
    if step_payload is not None and step_payload.get("pwf") != password_fingerprint(admin["hashed_password"]):
        raise HTTPException(status_code=401, detail="Enrollment token invalid or expired. Sign in again.")
    return admin


def _mfa_error(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(status_code=status, detail={"code": code, "message": message})


# --------------------------------------------------------------------------- login

@router.post("/login", response_model=AdminAuthResponse, response_model_exclude_none=True)
def admin_login(request: Request, response: Response, form_data: OAuth2PasswordRequestForm = Depends()):
    db = DB()
    ip = _client_ip(request)
    with db.connect() as conn:
        # Raises 429 when this account or client IP has too many recent
        # failures; otherwise the attempt is committed as a failure up front,
        # so the 400 below cannot roll it back.
        attempt_id = begin_login_attempt(conn, _admin_attempt_key(form_data.username), ip)
        ensure_admin_mfa_schema(conn)

        admin_row = conn.execute("SELECT * FROM admins WHERE email = ?", (form_data.username,)).fetchone()
        if not admin_row:
            # Equal work whether or not the admin exists.
            dummy_verify_password(form_data.password)

        if not admin_row or not verify_password(form_data.password, admin_row["hashed_password"]):
            raise HTTPException(status_code=400, detail="Incorrect email or password")

        mark_login_attempt_success(conn, attempt_id)

        if admin_row["is_active"] != 1:
            # Commit first: the password was right, so this is not a counted failure.
            conn.commit()
            raise HTTPException(status_code=403, detail="Admin account is inactive")

        if _mfa_enabled(admin_row):
            response.headers["Cache-Control"] = "no-store"
            return {
                "mfa_required": True,
                "mfa_token": create_admin_mfa_token(admin_row["id"], ADMIN_MFA_CHALLENGE,
                                                    admin_row["hashed_password"]),
                "token_type": "mfa_challenge",
            }
        if admin_mfa_required():
            response.headers["Cache-Control"] = "no-store"
            return {
                "mfa_enrollment_required": True,
                "enrollment_token": create_admin_mfa_token(admin_row["id"], ADMIN_MFA_ENROLL,
                                                           admin_row["hashed_password"]),
                "token_type": "mfa_enrollment",
            }

        access_token, refresh_token = _issue_admin_session(conn, admin_row, request, mfa_verified=False)

    return _session_body(response, request, access_token, refresh_token)


@router.post("/mfa/verify", response_model=AdminAuthResponse, response_model_exclude_none=True)
def admin_mfa_verify(body: MfaVerifyRequest, request: Request, response: Response):
    """Second login step: the challenge token plus a TOTP code or a recovery code."""
    payload = decode_admin_mfa_token(body.mfa_token, ADMIN_MFA_CHALLENGE)
    if not payload:
        raise _mfa_error(401, "MFA_CHALLENGE_INVALID", "Sign-in step expired. Enter your password again.")
    admin_id = payload["sub"]
    used_recovery = False
    db = DB()
    with db.connect() as conn:
        attempt_id = begin_login_attempt(conn, _mfa_attempt_key(admin_id), _client_ip(request),
                                         account_limit=MAX_LOGIN_ATTEMPTS)
        ensure_admin_mfa_schema(conn)
        admin = conn.execute("SELECT * FROM admins WHERE id = ?", (admin_id,)).fetchone()
        if (not admin or admin["is_active"] != 1 or not _mfa_enabled(admin)
                or payload.get("pwf") != password_fingerprint(admin["hashed_password"])):
            raise _mfa_error(401, "MFA_CHALLENGE_INVALID", "Sign-in step expired. Enter your password again.")

        ok = False
        if (body.code or "").strip():
            ok = consume_admin_totp_code(conn, admin_id, admin["mfa_secret"], body.code)
        elif (body.recovery_code or "").strip():
            ok = consume_recovery_code(conn, admin_id, body.recovery_code)
            used_recovery = ok
        if not ok:
            _audit(conn, "admin_login_failed_mfa", admin, request)
            conn.commit()
            raise _mfa_error(401, "MFA_INVALID", "Invalid or already used code")

        mark_login_attempt_success(conn, attempt_id)
        extra = {}
        if used_recovery:
            remaining = _remaining_recovery_codes(conn, admin_id)
            _audit(conn, "admin_mfa_recovery_code_used", admin, request, {"remaining": remaining})
            extra["recovery_codes_remaining"] = remaining
        access_token, refresh_token = _issue_admin_session(conn, admin, request, mfa_verified=True)

    return _session_body(response, request, access_token, refresh_token, **extra)


# --------------------------------------------------------------------------- enrolment

@router.post("/mfa/setup")
def admin_mfa_setup(request: Request, body: Optional[MfaSetupRequest] = None):
    """Start (or restart) enrolment: a new secret, pending until /mfa/enroll."""
    admin_id, step = _enrolling_admin_id(request, body.enrollment_token if body else None)
    secret = pyotp.random_base32()
    db = DB()
    with db.connect() as conn:
        ensure_admin_mfa_schema(conn)
        admin = _load_enrolling_admin(conn, admin_id, step)
        if _mfa_enabled(admin):
            # Swapping the factor of an enrolled admin needs the old one first.
            raise HTTPException(status_code=400, detail="MFA is already enabled. Disable it first.")
        conn.execute(
            "UPDATE admins SET mfa_secret = ?, mfa_last_counter = NULL WHERE id = ? AND COALESCE(mfa_enabled, 0) = 0",
            (encrypt_totp_secret(secret), admin_id),
        )
        email = admin["email"]
    uri = pyotp.totp.TOTP(secret).provisioning_uri(name=email, issuer_name=MFA_ISSUER_NAME)
    return {"secret": secret, "provisioning_uri": uri}


@router.post("/mfa/enroll", response_model=AdminAuthResponse, response_model_exclude_none=True)
def admin_mfa_enroll(body: MfaEnrollRequest, request: Request, response: Response):
    """Confirm the authenticator with a first code. Enables MFA, returns ten
    one-time recovery codes (shown once), revokes every earlier admin session
    and opens a fresh, MFA-verified one."""
    admin_id, step = _enrolling_admin_id(request, body.enrollment_token)
    db = DB()
    with db.connect() as conn:
        attempt_id = begin_login_attempt(conn, _mfa_attempt_key(admin_id), _client_ip(request),
                                         account_limit=MAX_LOGIN_ATTEMPTS)
        ensure_admin_mfa_schema(conn)
        admin = _load_enrolling_admin(conn, admin_id, step)
        if _mfa_enabled(admin):
            mark_login_attempt_success(conn, attempt_id)
            conn.commit()
            raise HTTPException(status_code=400, detail="MFA is already enabled")
        if not _value(admin, "mfa_secret"):
            mark_login_attempt_success(conn, attempt_id)
            conn.commit()
            raise HTTPException(status_code=400, detail="MFA setup not initiated")
        if not consume_admin_totp_code(conn, admin_id, admin["mfa_secret"], body.code):
            raise _mfa_error(400, "MFA_INVALID", "Invalid code")

        mark_login_attempt_success(conn, attempt_id)
        now = _now()
        conn.execute("UPDATE admins SET mfa_enabled = 1, mfa_enrolled_at = ? WHERE id = ?", (now, admin_id))
        codes = _replace_recovery_codes(conn, admin_id)
        # Sessions opened with the password alone end here.
        conn.execute("UPDATE admin_sessions SET revoked_at = ? WHERE admin_id = ? AND revoked_at IS NULL",
                     (now, admin_id))
        _audit(conn, "admin_mfa_enrolled", admin, request,
               {"via": "enrollment_token" if step is not None else "session"})
        access_token, refresh_token = _issue_admin_session(conn, admin, request, mfa_verified=True)

    return _session_body(response, request, access_token, refresh_token, recovery_codes=codes)


@router.get("/mfa/status")
def admin_mfa_status(current_admin: dict = Depends(get_current_admin)):
    db = DB()
    with db.connect() as conn:
        ensure_admin_mfa_schema(conn)
        remaining = _remaining_recovery_codes(conn, current_admin["id"])
    return {
        "enabled": _mfa_enabled(current_admin),
        "required": admin_mfa_required(),
        "recovery_codes_remaining": remaining,
    }


@router.post("/mfa/disable")
def admin_mfa_disable(body: MfaCodeRequest, request: Request, current_admin: dict = Depends(get_current_admin)):
    """Turn MFA off. Needs a current authenticator code (a recovery code is not
    enough) and is refused while ADMIN_MFA_REQUIRED is in force."""
    if admin_mfa_required():
        raise _mfa_error(409, "MFA_REQUIRED_BY_POLICY",
                         "Admin MFA is required (ADMIN_MFA_REQUIRED) and cannot be disabled")
    admin_id = current_admin["id"]
    db = DB()
    with db.connect() as conn:
        attempt_id = begin_login_attempt(conn, _mfa_attempt_key(admin_id), _client_ip(request),
                                         account_limit=MAX_LOGIN_ATTEMPTS)
        ensure_admin_mfa_schema(conn)
        admin = conn.execute("SELECT * FROM admins WHERE id = ?", (admin_id,)).fetchone()
        if not admin or not _mfa_enabled(admin):
            mark_login_attempt_success(conn, attempt_id)
            conn.commit()
            raise HTTPException(status_code=400, detail="MFA is not enabled")
        if not consume_admin_totp_code(conn, admin_id, admin["mfa_secret"], body.code):
            raise _mfa_error(400, "MFA_INVALID", "Invalid code")
        mark_login_attempt_success(conn, attempt_id)
        conn.execute(
            "UPDATE admins SET mfa_enabled = 0, mfa_secret = NULL, mfa_last_counter = NULL, "
            "mfa_enrolled_at = NULL WHERE id = ?", (admin_id,))
        conn.execute("DELETE FROM admin_mfa_recovery_codes WHERE admin_id = ?", (admin_id,))
        _audit(conn, "admin_mfa_disabled", admin, request)
    return {"status": "mfa_disabled"}


@router.post("/mfa/recovery-codes")
def admin_mfa_regenerate_recovery_codes(body: MfaCodeRequest, request: Request,
                                        current_admin: dict = Depends(get_current_admin)):
    """Replace every recovery code (old ones stop working). Needs a current TOTP code."""
    admin_id = current_admin["id"]
    db = DB()
    with db.connect() as conn:
        attempt_id = begin_login_attempt(conn, _mfa_attempt_key(admin_id), _client_ip(request),
                                         account_limit=MAX_LOGIN_ATTEMPTS)
        ensure_admin_mfa_schema(conn)
        admin = conn.execute("SELECT * FROM admins WHERE id = ?", (admin_id,)).fetchone()
        if not admin or not _mfa_enabled(admin):
            mark_login_attempt_success(conn, attempt_id)
            conn.commit()
            raise HTTPException(status_code=400, detail="MFA is not enabled")
        if not consume_admin_totp_code(conn, admin_id, admin["mfa_secret"], body.code):
            raise _mfa_error(400, "MFA_INVALID", "Invalid code")
        mark_login_attempt_success(conn, attempt_id)
        codes = _replace_recovery_codes(conn, admin_id)
        _audit(conn, "admin_mfa_recovery_codes_regenerated", admin, request)
    return {"recovery_codes": codes}


# --------------------------------------------------------------------------- sessions

@router.get("/csrf")
def admin_csrf(request: Request):
    """The CSRF token of the current cookie session (for a console served from
    another host, where the csrf cookie is not readable by page JavaScript).
    CORS keeps other origins from reading this answer."""
    return {"csrf_token": request.cookies.get(ADMIN_REALM.csrf_cookie)}


@router.post("/refresh", response_model=AdminAuthResponse, response_model_exclude_none=True)
def admin_refresh(req: Request, response: Response, request: Optional[RefreshRequest] = None):
    token = refresh_token_from(req, ADMIN_REALM, request.refresh_token if request else None)
    payload = decode_admin_token(token or "")
    if not payload or payload.get("type") != "admin_refresh":
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    admin_id = payload.get("sub")
    hashed_rt = hash_token(token)

    db = DB()
    with db.connect() as conn:
        ensure_admin_mfa_schema(conn)
        session = conn.execute(
            "SELECT * FROM admin_sessions WHERE refresh_token_hash = ? AND admin_id = ?",
            (hashed_rt, admin_id)
        ).fetchone()

        if not session:
            raise HTTPException(status_code=401, detail="Session invalid or revoked")

        if session["revoked_at"] is not None:
            # A rotated (or signed-out) refresh token came back: it was copied.
            # End every session of this admin, like the user refresh does.
            now = _now()
            conn.execute("UPDATE admin_sessions SET revoked_at = ? WHERE admin_id = ? AND revoked_at IS NULL",
                         (now, admin_id))
            admin_row = conn.execute("SELECT id, email FROM admins WHERE id = ?", (admin_id,)).fetchone()
            if admin_row:
                _audit(conn, "admin_refresh_reuse_detected", admin_row, req)
            conn.commit()  # before raising, or the revocation is rolled back
            raise HTTPException(status_code=401, detail="Security alert: session invalidated")

        if datetime.fromisoformat(session["expires_at"]) < datetime.now(timezone.utc):
            raise HTTPException(status_code=401, detail="Session expired")

        admin_row = conn.execute("SELECT * FROM admins WHERE id = ?", (admin_id,)).fetchone()
        if not admin_row or admin_row["is_active"] != 1:
            raise HTTPException(status_code=403, detail="Admin inactive")

        mfa_verified = bool(_value(session, "mfa_verified"))
        if (_mfa_enabled(admin_row) or admin_mfa_required()) and not mfa_verified:
            # A password-only session cannot outlive the MFA requirement.
            conn.execute("UPDATE admin_sessions SET revoked_at = ? WHERE id = ?", (_now(), session["id"]))
            conn.commit()
            raise _mfa_error(401, "MFA_REQUIRED", "Sign in again with your second factor")

        # Rotate token
        now = _now()
        new_access = create_admin_access_token(admin_id, role=admin_row["role"])
        new_refresh = create_admin_refresh_token(admin_id)

        conn.execute("UPDATE admin_sessions SET revoked_at = ?, rotated_from = ? WHERE id = ?",
                     (now, "refresh_rotation", session["id"]))

        expires_at = (datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)).isoformat()
        device = req.headers.get("User-Agent", session["device"])
        ip = req.client.host if req.client else session["ip"]

        conn.execute(
            """
            INSERT INTO admin_sessions
            (id, admin_id, refresh_token_hash, device, ip, created_at, expires_at, mfa_verified)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), admin_id, hash_token(new_refresh), (device or "")[:255], ip, now, expires_at,
             1 if mfa_verified else 0)
        )

    return _session_body(response, req, new_access, new_refresh)


@router.post("/logout")
def admin_logout(req: Request, response: Response, request: Optional[RefreshRequest] = None):
    token = refresh_token_from(req, ADMIN_REALM, request.refresh_token if request else None)
    if token:
        db = DB()
        with db.connect() as conn:
            conn.execute(
                "UPDATE admin_sessions SET revoked_at = ? WHERE refresh_token_hash = ? AND revoked_at IS NULL",
                (_now(), hash_token(token))
            )
    clear_cookies(response, ADMIN_REALM)
    return {"status": "logged_out"}


@router.get("/me", response_model=AdminMeResponse)
def get_admin_me(current_admin: dict = Depends(get_current_admin)):
    return AdminMeResponse(**{**current_admin, "mfa_enabled": _mfa_enabled(current_admin)})
