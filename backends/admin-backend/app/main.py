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


app = FastAPI(title="CosmicForge Admin Backend", version="0.1.0")

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
