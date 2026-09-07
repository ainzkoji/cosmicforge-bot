# Pre-load environment variables for shared_lib
import os
import logging
import sqlite3
import time
from dotenv import load_dotenv
load_dotenv()

import sys
from pathlib import Path

# Add shared package to path
shared_path = Path(__file__).parent.parent.parent / "shared"
sys.path.insert(0, str(shared_path))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Local imports
from app.core.config import settings

# Shared imports
from shared_lib.persistence.migrations import migrate

# Run migrations at startup
db_url = settings.DATABASE_URL
db_path = None
if db_url and db_url.startswith("sqlite:///"):
    db_path = db_url.replace("sqlite:///", "")

migrate(db_path)

from app.core.bootstrap import bootstrap_admin
bootstrap_admin()

# Configure logging to suppress noisy shutdown errors
from app.core.logging_config import configure_shutdown_logging
configure_shutdown_logging()

# Initialize Firebase Admin SDK
try:
    from shared_lib.notifications.firebase_client import initialize_firebase
    initialize_firebase()
except Exception as e:
    print(f"[FIREBASE] Warning: Could not initialize Firebase Admin SDK: {e}")
    print(f"[FIREBASE] Push notifications will not be available.")

# Create FastAPI app
app = FastAPI(title="CosmicForge API", version="1.0.0")
runtime_logger = logging.getLogger("cosmicforge.runtime")


@app.exception_handler(sqlite3.OperationalError)
async def sqlite_operational_error_handler(request, exc: sqlite3.OperationalError):
    message = str(exc).lower()
    if "locked" in message or "busy" in message:
        runtime_logger.warning(
            "sqlite_busy_response %s",
            {"method": request.method, "path": request.url.path, "error": type(exc).__name__},
        )
        return JSONResponse(
            status_code=503,
            content={"detail": "Database is busy. Please try again in a moment."},
        )
    return JSONResponse(status_code=500, content={"detail": "Database error"})


@app.middleware("http")
async def log_admin_auth_request_timing(request, call_next):
    started_at = time.perf_counter()
    path = request.url.path
    should_log = (
        path == "/api/v1/auth/login"
        or path.startswith("/api/admin")
        or path.startswith("/api/v1/admin-auth")
    )
    status_code = None
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    except Exception as exc:
        status_code = "failed"
        if should_log:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            runtime_logger.warning(
                "api_endpoint_timing %s",
                {
                    "method": request.method,
                    "path": path,
                    "duration_ms": duration_ms,
                    "status": status_code,
                    "error": type(exc).__name__,
                },
            )
        raise
    finally:
        if should_log and status_code != "failed":
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            log_fn = runtime_logger.warning if duration_ms >= 1000 or (status_code and int(status_code) >= 500) else runtime_logger.info
            log_fn(
                "api_endpoint_timing %s",
                {
                    "method": request.method,
                    "path": path,
                    "duration_ms": duration_ms,
                    "status": status_code,
                },
            )

# Security Headers Middleware
@app.middleware("http")
async def add_security_headers(request, call_next):
    response = await call_next(request)
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    # Basic CSP - failing open for images/scripts to avoid breaking potential CDNs, but restricting logic
    response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; font-src 'self' data: https:;"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return response

# CORS Middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:4173",
        "http://127.0.0.1:4173",
        getattr(settings, 'FRONTEND_URL', "http://localhost:5173")
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API routers
from app.api.monitoring_proxy import router as monitoring_proxy_router
app.include_router(monitoring_proxy_router)  # Prefix is defined in router (/api/v1/monitoring)

from app.api import auth, analytics_proxy, analytics_reporting, brokers
from app.api import reports_proxy  # Reports proxy

app.include_router(auth.router, prefix="/api/v1/auth", tags=["Authentication"])

from app.api import admin_auth
app.include_router(admin_auth.router, prefix="/api/v1/admin-auth", tags=["Admin Auth"])
# app.include_router(users.router, prefix="/api/v1/users", tags=["Users"])
# app.include_router(portfolio_proxy.router, prefix="/api/v1/portfolio", tags=["Portfolio"])
app.include_router(analytics_proxy.router, prefix="/api/v1/analytics", tags=["Analytics"])
app.include_router(analytics_reporting.router)  # Already has prefix="/api/analytics"
app.include_router(brokers.router, prefix="/api/v1/brokers", tags=["Brokers"])
app.include_router(reports_proxy.router, prefix="/api/v1/reports", tags=["Reports"])

from app.api.public import router as public_router
app.include_router(public_router, tags=["Public"])

from app.api.kyc import router as kyc_router
app.include_router(kyc_router, tags=["KYC"])

from app.api.billing import router as billing_router
app.include_router(billing_router, prefix="/api/billing", tags=["Billing"])

from app.api.onboarding import router as onboarding_router
app.include_router(onboarding_router, prefix="/api/onboarding", tags=["Onboarding"])

# Auto Pilot Proxy (forwards to bot-backend /api/v1/auto-pilot)
from app.api.auto_pilot_proxy import router as auto_pilot_proxy_router
app.include_router(auto_pilot_proxy_router, prefix="/api/v1/auto-pilot", tags=["Auto Pilot Proxy"])


# Events Proxy (SSE streaming for real-time updates)
from app.api.events_proxy import router as events_proxy_router
app.include_router(events_proxy_router, prefix="/api/v1/events", tags=["Events Proxy"])

from app.api.admin import router as admin_router
app.include_router(admin_router, prefix="/api", tags=["Admin"])

from app.api.admin_ml import router as admin_ml_router
app.include_router(admin_ml_router, prefix="/api", tags=["Admin ML"])

from app.api.admin_events import router as admin_events_router
app.include_router(admin_events_router, prefix="/api", tags=["Admin Events"])

from app.api.admin_news import router as admin_news_router
app.include_router(admin_news_router, prefix="/api", tags=["Admin News"])

from app.api.admin_tradingview import router as admin_tradingview_router
app.include_router(admin_tradingview_router, prefix="/api", tags=["Admin TradingView"])

from app.api.admin_signals import router as admin_signals_router
app.include_router(admin_signals_router, prefix="/api", tags=["Admin Signals"])

from app.api.admin_signal_pairs import router as admin_signal_pairs_router
app.include_router(admin_signal_pairs_router, prefix="/api", tags=["Admin Signal Pairs"])

from app.api.risk_profiles_proxy import router as risk_profiles_proxy_router
app.include_router(risk_profiles_proxy_router, prefix="/api/v1/risk-profiles", tags=["Risk Profiles Proxy"])

from app.api.portfolio import router as portfolio_router
app.include_router(portfolio_router, prefix="/api/portfolio", tags=["Portfolio"])

from app.api.notifications import router as notifications_router
app.include_router(notifications_router, prefix="/api/notifications", tags=["Notifications"])

from app.api.signals import router as signals_router
app.include_router(signals_router, prefix="/api/signals", tags=["Signals"])



# Register Backtest Proxy API (Phase 6)
from app.api.backtesting_proxy import router as backtesting_proxy_router
app.include_router(backtesting_proxy_router, prefix="/api/v1/backtests", tags=["Backtest Proxy"])

# Bot Instances Proxy (forwards to bot-backend service)
from app.api.bot_instances_proxy import router as bot_instances_router
app.include_router(bot_instances_router, prefix="/api/v1", tags=["Bot Instances"])

# Forex Proxy
from app.api.forex_proxy import router as forex_proxy_router
app.include_router(forex_proxy_router, prefix="/api/v1/forex", tags=["Forex Proxy"])

# MT Bridge Pairing
from app.api.mt_pairing import router as mt_pairing_router
app.include_router(mt_pairing_router, tags=["MT Pairing"])

# MT Connector Authentication (magic link)
from app.api.connector_auth import router as connector_auth_router
app.include_router(connector_auth_router, tags=["MT Connector"])





@app.get("/")
def root():
    return {
        "service": "CosmicForge API",
        "status": "ok",
        "version": "1.0.0"
    }


@app.get("/health")
def health():
    from datetime import datetime, timezone
    from shared_lib.persistence.db import DB

    database_reachable = False
    try:
        with DB().connect() as conn:
            conn.execute("SELECT 1").fetchone()
        database_reachable = True
    except Exception:
        database_reachable = False

    return {
        "status": "healthy" if database_reachable else "degraded",
        "service": "user-backend",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database_reachable": database_reachable,
    }


if __name__ == "__main__":
    import uvicorn
    print("[USER-API] Starting CosmicForge User API Service on port 8000...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
