from __future__ import annotations

import logging
import sqlite3
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api.bot_monitor import router as bot_monitor_router
from app.api.dashboard import router as dashboard_router
from app.api.events import router as events_router
from app.api.health import router as health_router
from app.api.ml_monitoring import router as ml_monitoring_router
from app.api.news import router as news_router
from app.api.profitability import router as profitability_router
from app.api.revenue import router as revenue_router
from app.api.signals import router as signals_router
from app.api.tradingview import router as tradingview_router
from app.api.users import router as users_router
from app.core.cors import configure_cors


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cosmicforge.admin_backend")


from app.core.config import settings
from shared_lib.core.http_surface import docs_kwargs  # no /docs in production unless API_DOCS_ENABLED=true
app = FastAPI(title="CosmicForge Admin Backend", version="0.1.0", **docs_kwargs(settings))

# Section H 8.4: the admin console's httpOnly session cookie (cf_admin_access,
# issued by the user-backend) is accepted like a bearer header; state-changing
# requests need the X-CSRF-Token double-submit header. Added before CORS so
# CORS stays the outer layer.
import os  # noqa: E402
from shared_lib.core.security.session_cookies import ADMIN_REALM, SessionCookieMiddleware  # noqa: E402


def _bearer_compat() -> bool:
    raw = getattr(settings, "AUTH_BEARER_COMPAT", None)
    if raw is None:
        raw = os.environ.get("AUTH_BEARER_COMPAT", "true")
    return str(raw).strip().lower() not in ("0", "false", "no", "off")


app.add_middleware(SessionCookieMiddleware, realm_for_path=lambda _path: ADMIN_REALM,
                   csrf_exempt=lambda _path: False, bearer_compat=_bearer_compat)

configure_cors(app)


@app.middleware("http")
async def request_timing_middleware(request: Request, call_next):
    started_at = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        logger.info(
            "admin_backend_request method=%s path=%s status_code=%s duration_ms=%s",
            request.method,
            request.url.path,
            status_code,
            duration_ms,
        )


@app.exception_handler(sqlite3.OperationalError)
async def sqlite_operational_error_handler(_request: Request, exc: sqlite3.OperationalError):
    message = str(exc).lower()
    if "database is locked" in message or "database is busy" in message or "database table is locked" in message:
        return JSONResponse(
            status_code=503,
            content={"detail": "Database is busy. Please retry shortly."},
        )
    logger.exception("Unhandled SQLite operational error")
    return JSONResponse(status_code=500, content={"detail": "Database operation failed."})


@app.exception_handler(Exception)
async def unhandled_exception_handler(_request: Request, exc: Exception):
    logger.exception("Unhandled admin backend error: %s", type(exc).__name__)
    return JSONResponse(status_code=500, content={"detail": "Internal server error."})


app.include_router(health_router)
app.include_router(dashboard_router)
app.include_router(users_router)
app.include_router(revenue_router)
app.include_router(profitability_router)
app.include_router(tradingview_router)
app.include_router(bot_monitor_router)
app.include_router(signals_router)
app.include_router(events_router)
app.include_router(news_router)
app.include_router(ml_monitoring_router)
