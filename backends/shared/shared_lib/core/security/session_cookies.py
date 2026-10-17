"""Browser sessions in httpOnly cookies, with double-submit CSRF protection.

Section H Step 8.4. The access and refresh tokens are set as ``HttpOnly``,
``Secure`` (configurable only outside production), ``SameSite=Strict``
cookies, so page JavaScript -- and therefore an XSS payload -- can no longer
read or exfiltrate them. Page JavaScript only ever sees the CSRF token: a
random value stored in a readable cookie and echoed back in the
``X-CSRF-Token`` header on every state-changing request.

How a request is authenticated (``SessionCookieMiddleware``):

* An ``Authorization: Bearer <token>`` header is used as before (migration
  window, ``bearer_compat``). Placeholder values a front end produces when it
  has no token (``Bearer null`` / ``Bearer undefined`` / ``Bearer``) are
  removed. A browser never attaches an Authorization header to a cross-site
  request on its own, so bearer requests need no CSRF check.
* Otherwise, when the realm's access cookie is present, its value is injected
  as ``Authorization: Bearer <token>``. Every existing dependency
  (``OAuth2PasswordBearer``) and every proxy that forwards the caller's
  Authorization header keeps working unchanged.
* On an unsafe method (anything but GET/HEAD/OPTIONS/TRACE) a cookie is only
  used when the ``X-CSRF-Token`` header equals the realm's CSRF cookie;
  otherwise the request is refused with 403 ``CSRF_FAILED`` -- except on the
  explicitly exempt, unauthenticated endpoints (login, password reset...),
  where the cookie is simply not used.

Nothing here logs a token value.
"""
from __future__ import annotations

import hmac
import json
import secrets
from dataclasses import dataclass
from typing import Callable, Iterable, List, Mapping, Optional, Tuple

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})
CSRF_HEADER = "X-CSRF-Token"
_CSRF_HEADER_LOWER = CSRF_HEADER.lower().encode("latin-1")
_PLACEHOLDER_BEARERS = {"", "null", "undefined", "none", "false"}
CSRF_FAILED_CODE = "CSRF_FAILED"


@dataclass(frozen=True)
class CookieRealm:
    """One browser session family (end user or admin), with its own cookies."""

    name: str
    access_cookie: str
    refresh_cookie: str
    csrf_cookie: str
    #: The refresh cookie is only sent to the endpoints that need it.
    refresh_path: str


USER_REALM = CookieRealm("user", "cf_access", "cf_refresh", "cf_csrf", "/api/v1/auth")
ADMIN_REALM = CookieRealm("admin", "cf_admin_access", "cf_admin_refresh", "cf_admin_csrf", "/api/v1/admin-auth")


@dataclass(frozen=True)
class CookiePolicy:
    secure: bool
    samesite: str = "strict"
    domain: Optional[str] = None
    access_max_age: int = 15 * 60
    refresh_max_age: int = 14 * 24 * 3600


def normalise_samesite(value: object) -> str:
    """``strict`` or ``lax``. ``none`` (cookies on cross-site requests) is never used."""
    text = str(value or "").strip().lower()
    return text if text in ("strict", "lax") else "strict"


def new_csrf_token() -> str:
    return secrets.token_urlsafe(32)


def csrf_matches(cookie_value: Optional[str], header_value: Optional[str]) -> bool:
    """Constant-time double-submit comparison; both values must be present."""
    if not cookie_value or not header_value:
        return False
    return hmac.compare_digest(str(cookie_value).encode("utf-8"), str(header_value).encode("utf-8"))


def parse_cookie_header(raw: str) -> dict:
    """Cookie header -> {name: value}. First occurrence wins (most specific path)."""
    cookies: dict = {}
    for chunk in (raw or "").split(";"):
        if "=" not in chunk:
            continue
        name, value = chunk.split("=", 1)
        name, value = name.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] == '"':
            value = value[1:-1]
        if name and name not in cookies:
            cookies[name] = value
    return cookies


def is_placeholder_bearer(value: Optional[str]) -> bool:
    """True for an Authorization header that carries no usable token."""
    text = (value or "").strip()
    if not text:
        return True
    scheme, _, token = text.partition(" ")
    return scheme.lower() == "bearer" and token.strip().lower() in _PLACEHOLDER_BEARERS


def _cookie_kwargs(policy: CookiePolicy) -> dict:
    kwargs = {"secure": bool(policy.secure), "samesite": normalise_samesite(policy.samesite)}
    if policy.domain:
        kwargs["domain"] = policy.domain
    return kwargs


def set_session_cookies(response, realm: CookieRealm, policy: CookiePolicy, *,
                        access_token: str, refresh_token: str, csrf_token: str) -> None:
    """Set the three cookies of a session on a Starlette/FastAPI response."""
    common = _cookie_kwargs(policy)
    response.set_cookie(realm.access_cookie, access_token, max_age=int(policy.access_max_age),
                        path="/", httponly=True, **common)
    response.set_cookie(realm.refresh_cookie, refresh_token, max_age=int(policy.refresh_max_age),
                        path=realm.refresh_path, httponly=True, **common)
    # Readable by page JavaScript on purpose (double submit); it grants nothing alone.
    response.set_cookie(realm.csrf_cookie, csrf_token, max_age=int(policy.refresh_max_age),
                        path="/", httponly=False, **common)
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"


def clear_session_cookies(response, realm: CookieRealm, policy: CookiePolicy) -> None:
    common = _cookie_kwargs(policy)
    response.delete_cookie(realm.access_cookie, path="/", httponly=True, **common)
    response.delete_cookie(realm.refresh_cookie, path=realm.refresh_path, httponly=True, **common)
    response.delete_cookie(realm.csrf_cookie, path="/", httponly=False, **common)
    response.headers["Cache-Control"] = "no-store"


def request_csrf_ok(cookies: Mapping[str, str], headers: Mapping[str, str], realm: CookieRealm) -> bool:
    """Double-submit check for an endpoint that reads a session cookie itself."""
    return csrf_matches(cookies.get(realm.csrf_cookie), headers.get(CSRF_HEADER) or headers.get(CSRF_HEADER.lower()))


def _header(headers: Iterable[Tuple[bytes, bytes]], name: bytes) -> Optional[str]:
    for key, value in headers:
        if key.lower() == name:
            return value.decode("latin-1")
    return None


class SessionCookieMiddleware:
    """Pure ASGI middleware: cookie session -> ``Authorization`` header, with CSRF.

    ``realm_for_path(path)`` names the cookie realm a path belongs to (None: no
    cookie authentication on that path). ``csrf_exempt(path)`` lists the
    unauthenticated endpoints where a failed CSRF check just means "ignore the
    cookie". ``bearer_compat()`` is read per request: when it returns False an
    incoming Authorization header is dropped and only cookies authenticate.
    """

    def __init__(self, app, *, realm_for_path: Callable[[str], Optional[CookieRealm]],
                 csrf_exempt: Callable[[str], bool], bearer_compat: Callable[[], bool]):
        self.app = app
        self.realm_for_path = realm_for_path
        self.csrf_exempt = csrf_exempt
        self.bearer_compat = bearer_compat

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        headers: List[Tuple[bytes, bytes]] = list(scope.get("headers") or [])
        authorization = _header(headers, b"authorization")
        drop_auth = authorization is not None and (
            is_placeholder_bearer(authorization) or not self.bearer_compat())
        if drop_auth:
            headers = [(k, v) for k, v in headers if k.lower() != b"authorization"]
            authorization = None

        if authorization is not None:
            # A real bearer token: used as before, no cookie involved.
            await self.app(scope, receive, send)
            return

        path = scope.get("path") or ""
        realm = self.realm_for_path(path)
        cookies = parse_cookie_header(_header(headers, b"cookie") or "")
        access = cookies.get(realm.access_cookie) if realm else None

        if access:
            method = str(scope.get("method") or "GET").upper()
            csrf_ok = method in SAFE_METHODS or csrf_matches(
                cookies.get(realm.csrf_cookie), _header(headers, _CSRF_HEADER_LOWER))
            if csrf_ok:
                headers.append((b"authorization", b"Bearer " + access.encode("latin-1")))
            elif not self.csrf_exempt(path):
                await _send_json(send, 403, {"detail": {
                    "code": CSRF_FAILED_CODE,
                    "message": f"Missing or invalid {CSRF_HEADER} header for a cookie-authenticated request",
                }})
                return

        await self.app(dict(scope, headers=headers), receive, send)


async def _send_json(send, status: int, body: dict) -> None:
    payload = json.dumps(body).encode("utf-8")
    await send({"type": "http.response.start", "status": status, "headers": [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(payload)).encode("latin-1")),
        (b"cache-control", b"no-store"),
    ]})
    await send({"type": "http.response.body", "body": payload})


__all__ = [
    "ADMIN_REALM", "CSRF_FAILED_CODE", "CSRF_HEADER", "CookiePolicy", "CookieRealm", "SAFE_METHODS",
    "SessionCookieMiddleware", "USER_REALM", "clear_session_cookies", "csrf_matches", "is_placeholder_bearer",
    "new_csrf_token", "normalise_samesite", "parse_cookie_header", "request_csrf_ok", "set_session_cookies",
]
