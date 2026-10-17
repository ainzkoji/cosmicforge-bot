"""User-backend binding of the shared cookie-session layer (Section H 8.4).

Settings (app.core.config):

* ``AUTH_BEARER_COMPAT`` (default true): migration window. Bearer headers are
  still accepted and login/refresh bodies still carry the token strings.
* ``AUTH_COOKIE_SECURE``: unset = on in production, off elsewhere. A
  production process always sets ``Secure``, whatever the variable says.
* ``AUTH_COOKIE_SAMESITE`` (strict | lax), ``AUTH_COOKIE_DOMAIN`` (empty =
  host-only).
* ``ADMIN_MFA_REQUIRED``: unset = true in production, false otherwise.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import HTTPException, Request

from app.core.config import settings
from shared_lib.core.security.session_cookies import (
    ADMIN_REALM,
    CSRF_FAILED_CODE,
    CSRF_HEADER,
    USER_REALM,
    CookiePolicy,
    CookieRealm,
    SessionCookieMiddleware,
    clear_session_cookies,
    new_csrf_token,
    normalise_samesite,
    request_csrf_ok,
    set_session_cookies,
)

logger = logging.getLogger(__name__)

#: Paths whose cookie realm is the admin console; every other path is the user realm.
ADMIN_PATH_PREFIXES = ("/api/admin", "/api/v1/admin-auth")

#: Unauthenticated endpoints: a stale cookie without a CSRF header is ignored
#: there instead of refusing the request (otherwise a user with an expired
#: session could not sign in again).
CSRF_EXEMPT_PATHS = frozenset({
    "/api/v1/auth/login",
    "/api/v1/auth/register",
    "/api/v1/auth/verify-email",
    "/api/v1/auth/resend-verification",
    "/api/v1/auth/forgot-password",
    "/api/v1/auth/reset-password",
    "/api/v1/admin-auth/login",
    "/api/v1/admin-auth/mfa/verify",
    # Enrolment with the step token in the body (no cookie involved); a
    # cookie-session enrolment without the CSRF header just gets no session.
    "/api/v1/admin-auth/mfa/setup",
    "/api/v1/admin-auth/mfa/enroll",
})
CSRF_EXEMPT_PREFIXES = ("/public/",)


def _production() -> bool:
    return bool(getattr(settings, "production", False))


def bearer_compat() -> bool:
    return bool(getattr(settings, "AUTH_BEARER_COMPAT", True))


def admin_mfa_required() -> bool:
    explicit = getattr(settings, "ADMIN_MFA_REQUIRED", None)
    if explicit is None:
        return _production()
    return bool(explicit)


def cookie_secure() -> bool:
    if _production():
        explicit = getattr(settings, "AUTH_COOKIE_SECURE", None)
        if explicit is False:
            logger.warning("[AUTH] AUTH_COOKIE_SECURE=false is ignored in production")
        return True
    explicit = getattr(settings, "AUTH_COOKIE_SECURE", None)
    return False if explicit is None else bool(explicit)


def cookie_policy() -> CookiePolicy:
    return CookiePolicy(
        secure=cookie_secure(),
        samesite=normalise_samesite(getattr(settings, "AUTH_COOKIE_SAMESITE", "strict")),
        domain=(getattr(settings, "AUTH_COOKIE_DOMAIN", "") or None),
        access_max_age=int(settings.ACCESS_TOKEN_EXPIRE_MINUTES) * 60,
        refresh_max_age=int(settings.REFRESH_TOKEN_EXPIRE_DAYS) * 24 * 3600,
    )


def realm_for_path(path: str) -> CookieRealm:
    return ADMIN_REALM if (path or "").startswith(ADMIN_PATH_PREFIXES) else USER_REALM


def csrf_exempt(path: str) -> bool:
    return path in CSRF_EXEMPT_PATHS or (path or "").startswith(CSRF_EXEMPT_PREFIXES)


def install_session_cookie_middleware(app) -> None:
    """Add the cookie -> Authorization middleware. Add it BEFORE CORSMiddleware
    so CORS stays the outer layer and a 403 CSRF answer still carries CORS headers."""
    app.add_middleware(SessionCookieMiddleware, realm_for_path=realm_for_path,
                       csrf_exempt=csrf_exempt, bearer_compat=bearer_compat)


def issue_session_cookies(response, realm: CookieRealm, access_token: str, refresh_token: str,
                          request: Optional[Request] = None) -> str:
    """Set the session cookies; returns the CSRF token (kept across refreshes)."""
    csrf = None
    if request is not None:
        csrf = request.cookies.get(realm.csrf_cookie)
    csrf = csrf or new_csrf_token()
    set_session_cookies(response, realm, cookie_policy(), access_token=access_token,
                        refresh_token=refresh_token, csrf_token=csrf)
    return csrf


def clear_cookies(response, realm: CookieRealm) -> None:
    clear_session_cookies(response, realm, cookie_policy())


def token_body(access_token: str, refresh_token: str, csrf_token: str, **extra) -> dict:
    """Login/refresh JSON body. Token strings only during the bearer migration window."""
    body = {"token_type": "bearer", "csrf_token": csrf_token, **extra}
    if bearer_compat():
        body["access_token"] = access_token
        body["refresh_token"] = refresh_token
    return body


def refresh_token_from(request: Request, realm: CookieRealm, body_token: Optional[str]) -> Optional[str]:
    """The refresh token of this request: the JSON body (legacy clients) or the
    refresh cookie, which is only honoured together with a valid CSRF header.
    After the migration window (AUTH_BEARER_COMPAT=false) a body token is ignored."""
    if body_token and bearer_compat():
        return body_token
    cookie_token = request.cookies.get(realm.refresh_cookie)
    if not cookie_token:
        return None
    if not request_csrf_ok(request.cookies, request.headers, realm):
        raise HTTPException(status_code=403, detail={
            "code": CSRF_FAILED_CODE, "message": f"Missing or invalid {CSRF_HEADER} header"})
    return cookie_token


__all__ = [
    "ADMIN_PATH_PREFIXES", "ADMIN_REALM", "CSRF_EXEMPT_PATHS", "USER_REALM", "admin_mfa_required",
    "bearer_compat", "clear_cookies", "cookie_policy", "cookie_secure", "csrf_exempt",
    "install_session_cookie_middleware", "issue_session_cookies", "realm_for_path",
    "refresh_token_from", "token_body",
]
