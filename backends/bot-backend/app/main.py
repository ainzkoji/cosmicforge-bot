import time
import uuid
import json
import traceback
import logging  # ✅ Add logging import

import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

PROCESS_STARTED_AT = datetime.now(timezone.utc).isoformat()
PHASE6_GATE_CODE_VERSION = "phase6_limited_gate_v1_2026-05-21"

# ========== INSTALL GLOBAL EXCEPTION HANDLERS ==========
import app.core.global_exception_handlers  # Auto-installs on import
# ========== END GLOBAL EXCEPTION HANDLERS ==========

# Add shared package to path
shared_path = Path(__file__).parent.parent.parent / "shared"
sys.path.insert(0, str(shared_path))

# Load .env into os.environ so all components that read os.environ.get() see the correct
# values. pydantic-settings (BaseSettings) reads .env into model fields only and does NOT
# populate os.environ in v2. This call ensures ShadowConfig, and any future module using
# os.environ.get(), sees the same values as the pydantic Settings model.
from dotenv import load_dotenv as _load_dotenv
_load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=True)
CONFIG_LOADED_AT = datetime.now(timezone.utc).isoformat()
# Phase 5F-2 reload trigger v2: 2026-04-04 — shadow side-normalization fix

"""
Bot Backend Main Application

Provides API for bot management and runs the MultiBotRunner in the background.

**DEVELOPMENT NOTE**: 
When testing the bot loop cycle locally, make sure to run uvicorn without the `--reload` 
flag, otherwise changes to SQLite files or logs might trigger a reload that kills the loop:

    uvicorn app.main:app --host 0.0.0.0 --port 8001 --no-reload
"""

import asyncio
from anyio import to_thread
from pathlib import Path
from fastapi import Query
from fastapi import Body
from typing import Any, Dict, List
import app.strategy
from fastapi import Depends
from uuid import uuid4
from app.risk.system_limits import SystemLimits

# removed debug prints


from fastapi import FastAPI
from app.core.config import settings
from app.exchange.binance.client import BinanceFuturesClient
from app.execution.executor import BinanceExecutor
from app.symbols.universe import parse_symbols, build_universe
from app.core.strong_trend_guard import evaluate_strong_trend_guard
from app.runner.runner import PaperRunner
from app.symbols.leverage import parse_leverage_map, leverage_for
from app.exchange.binance.filters import extract_filters, round_qty
from app.ops.run_manager import RunManager
from app.ops.context import set_run_id, clear_run_id
from app.ops.run_tracker import RunTracker
from shared_lib.persistence.db import DB
from app.ops.context import set_cycle_id, clear_cycle_id
from app.strategy.registry import list_strategies, get_strategy_spec

from shared_lib.notifications.dispatcher import NotificationDispatcher
from app.notifications.worker import NotificationWorker
from app.backtest.worker import BacktestWorker
from app.events.event_calendar_service import EventCalendarService
from app.events.calendar_sync_worker import build_calendar_sync_worker
from app.events.event_ingestion_worker import build_event_ingestion_worker
from app.events.market_reaction_tracker import MarketReactionTracker
from app.services.market_data_snapshot_service import MarketDataSnapshotService
from app.workers.event_reaction_worker import build_event_reaction_worker
from app.workers.news_ingestion_worker import build_news_ingestion_worker
from app.workers.real_time_news_worker import build_real_time_news_worker


from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone
from app.risk.realized_pnl import realized_pnl_from_user_trades
from datetime import date
from app.execution.confirm import wait_until_flat
from shared_lib.persistence.migrations import migrate
from shared_lib.core.policy.kyc_policy import check_kyc_gate, KYCAction
from app.core.auth import get_current_active_user
from fastapi import HTTPException



# Helper to extract path from DATABASE_URL
def get_db_path() -> str | None:
    url = settings.DATABASE_URL
    if url and url.startswith("sqlite:///"):
        return url.replace("sqlite:///", "")
    return None

db_path = get_db_path()
print(f"[STARTUP] Using database path: {db_path or 'DEFAULT (data/bot.db)'}")

# --- STARTUP GUARD: Prevent DB Split ---
if db_path:
    import os
    abs_db_path = os.path.abspath(db_path)
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.trace_recorder import get_trace_recorder
    
    resolved_db = os.path.abspath(DB().path)
    if resolved_db != abs_db_path:
        raise RuntimeError(f"FATAL: DB Split detected! DATABASE_URL={abs_db_path} but DB() resolved to {resolved_db}")
        
    resolved_trace = os.path.abspath(get_trace_recorder()._db_path)
    if resolved_trace != abs_db_path:
        raise RuntimeError(f"FATAL: Trace DB Split! Expected {abs_db_path} but got {resolved_trace}")

# Run migrations on the CORRECT database
migrate(db_path=db_path)

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


app = FastAPI(title="CosmicForge Bot MVP")

# --- CORS Middleware ---
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register monitoring API router
from app.api.monitoring import router as monitoring_router
app.include_router(monitoring_router, prefix="/api/v1/monitoring")

# Register Bot Instances
from app.api.bot_instances import router as bot_instances_router
app.include_router(bot_instances_router, prefix="/api/v1")

# Register Auto Pilot
from app.api.auto_pilot import router as auto_pilot_router
app.include_router(auto_pilot_router, prefix="/api/v1/auto-pilot")

# Register Shadow Trading API router
from app.api.shadow_routes import router as shadow_router
app.include_router(shadow_router)  # Prefix is defined in router

# Register Analytics API router
from app.api.analytics import router as analytics_router
app.include_router(analytics_router, prefix="/api/v1/analytics")

# Register Equity API router
from app.api.equity import router as equity_router
app.include_router(equity_router, prefix="/api/v1/analytics", tags=["Equity"])

# Register Reporting API router (Phase 8)
from app.api.analytics_reporting import router as analytics_reporting_router
app.include_router(analytics_reporting_router)  # Prefix already defined in router

# Register Reports API router
from app.api.reports import router as reports_router
app.include_router(reports_router, prefix="/api/v1/reports", tags=["Reports"])

# Register Backtest API (Phase 6)
from app.api.backtesting import router as backtesting_router
app.include_router(backtesting_router, prefix="/api/v1/backtests", tags=["Backtest"])

# Register Events API router (SSE streaming) - OPTIONAL
try:
    from app.api.events import router as events_router
    app.include_router(events_router, prefix="/api/v1/events")
    print("[SSE] Server-Sent Events enabled for real-time analytics updates")
except ImportError as e:
    print(f"[SSE] Server-Sent Events disabled (missing dependency: {e}). Analytics will use polling only.")

# Register Risk Profiles API router
from app.api.risk_profiles import router as risk_profiles_router
app.include_router(risk_profiles_router, prefix="/api/v1/risk-profiles")

# Register Forex Instruments API router
from app.api.forex_instruments import router as forex_instruments_router
app.include_router(forex_instruments_router, prefix="/api/v1/forex")

# Register Brokers API router
from app.api.brokers import router as brokers_router
app.include_router(brokers_router, prefix="/api/v1/brokers")

# Register IBKR Connect API router
from app.api.ibkr import router as ibkr_router
app.include_router(ibkr_router)  # Prefix already defined in router

# Register News Intelligence API router (Phase 3)
from app.api.news_intelligence import router as news_intelligence_router
app.include_router(news_intelligence_router, prefix="/api/v1/news-intelligence", tags=["News Intelligence"])

# Register TradingView advisory-only webhook router (Stage 1)
from app.api.tradingview import router as tradingview_router
app.include_router(tradingview_router, prefix="/api/v1/tradingview", tags=["TradingView"])

paper_runner_instance: PaperRunner | None = None
run_tracker = RunTracker(DB(path=get_db_path()))
CURRENT_RUN_ID: str | None = None


run_manager = RunManager()


SENSITIVE_KEYS = {
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.on_event("startup")
async def _startup_validate_config():
    """Fail-fast config validation at startup."""
    try:
        # Debug Auth Config
        print(f"[STARTUP] SECRET_KEY prefix: {settings.SECRET_KEY[:5]}...")
        print(f"[STARTUP] Execution Mode: {settings.EXECUTION_MODE}")

        warnings = settings.validate_runtime()
        for w in warnings:
            print(f"[CONFIG WARNING] {w}")

        # Safety startup check — logs report; blocks live mode on failures
        from app.core.safety_startup import run_startup_safety_check
        safety_report = run_startup_safety_check(settings, mode=settings.EXECUTION_MODE)
        if not safety_report.all_pass and settings.EXECUTION_MODE == "live":
            raise RuntimeError(
                "Safety startup check FAILED in live mode. "
                "Fix the configuration failures listed above before enabling live trading."
            )
        print(f"[STARTUP] Safety check: {'PASS' if safety_report.all_pass else 'WARNINGS (paper mode continues)'}")
    except RuntimeError:
        raise
    except Exception as e:
        # Fail-closed: crash the service rather than running with a dangerous config
        print(str(e))
        raise


@app.on_event("startup")
async def _startup_run_manager():
    # create a run record in DB
    info = run_manager.start()
    print(f"[RUN] started run_id={info.run_id} mode={info.mode}")

    # ✅ Auto-start the MultiBotRunner on every startup (restored post-unification audit)
    runner_service.running = True
    runner_service.task = asyncio.create_task(runner_loop())
    print("[RUNNER] Loop started automatically.")


@app.on_event("shutdown")
async def _shutdown_run_manager():
    """Fast shutdown - target ~1 second."""
    import signal
    import os
    
    # ✅ Signal loop to stop IMMEDIATELY
    runner_service.running = False
    
    # ✅ Signal synchronous runner to break loop
    if paper_runner_instance:
        paper_runner_instance._stop_requested = True
    
    # ✅ Cancel task IMMEDIATELY without waiting
    if runner_service.task and not runner_service.task.done():
        runner_service.task.cancel()
        # Don't await - just force cancel
        try:
            await asyncio.wait_for(runner_service.task, timeout=0.5)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
            
    # stop the most recent running run (safe even after reload)
    current = run_manager.get_current()
    if current and current.get("run"):
        run_id = current["run"]["run_id"]
        run_manager.stop(run_id, status="STOPPED")
        print(f"[RUN] stopped run_id={run_id}")
    
    # ✅ Force exit ONLY if threads are genuinely blocking shutdown.
    # IMPORTANT: Do NOT call os._exit() unconditionally — during a uvicorn --reload,
    # the old worker shuts down while the new worker child process is spawning.
    # os._exit() in the old worker kills the entire process group on Windows,
    # interrupting the child mid-import (producing the garbled enum.py crash).
    # Guard: only exit if the asyncio loop is still alive after the timeout,
    # meaning background threads are truly blocking graceful shutdown.
    def force_exit():
        import time
        import asyncio as _aio
        time.sleep(1.5)
        try:
            loop = _aio.get_event_loop()
            if loop.is_closed():
                # Process is already winding down cleanly — skip os._exit()
                return
        except Exception:
            pass
        # Only reach here if the loop is somehow still open after 1.5s → truly stuck
        print("[SHUTDOWN] Force exit after 1.5s timeout (stuck threads detected)")
        os._exit(0)

    import threading
    exit_thread = threading.Thread(target=force_exit, daemon=True)
    exit_thread.start()


def get_runner() -> PaperRunner:
    global paper_runner_instance

    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )

    # ✅ TIME SYNC AT STARTUP (prevents -1021 on first request)
    try:
        client.sync_time()
    except Exception:
        pass

    if paper_runner_instance is None:
        paper_runner_instance = PaperRunner(client)

    # ✅ keep client fresh after reload / env changes
    paper_runner_instance.client = client
    paper_runner_instance.executor.client = client

    return paper_runner_instance


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RunnerServiceState:
    running: bool = False
    mode: str = settings.EXECUTION_MODE.lower()
    interval_seconds: int = 10
    max_symbols: int = 10
    started_at: Optional[str] = None
    last_cycle_at: Optional[str] = None
    cycle_count: int = 0
    last_error: Optional[str] = None
    task: Optional[asyncio.Task] = None
    crash_next_cycle: bool = False
    multi_runner: Optional[Any] = None # Avoid circular import if needed, or use Type Hint


runner_service = RunnerServiceState()

# ✅ NOTIFICATION SERVICES
# Initialize Dispatcher (Registers EventHooks)
notification_dispatcher = NotificationDispatcher(DB(path=get_db_path()))

# Initialize Worker
_worker_db = DB(path=get_db_path())
notification_worker = NotificationWorker(DB(path=get_db_path()))
backtest_worker = BacktestWorker(DB(path=get_db_path()))

# Event Awareness — calendar sync worker (keeps event_blackout_windows fresh)
_event_calendar_svc = EventCalendarService(_worker_db)
calendar_sync_worker = build_calendar_sync_worker(_worker_db, calendar_service=_event_calendar_svc)
event_ingestion_worker = build_event_ingestion_worker(
    _worker_db,
    on_after_ingest=calendar_sync_worker.sync_once_now,
)

# Market Reaction Layer (Phase 2) — observer only, shadow mode by default
_reaction_exchange_client = None  # resolved lazily on first tick if enabled
if settings.MARKET_REACTION_LAYER_ENABLED:
    try:
        _reaction_exchange_client = BinanceFuturesClient(
            api_key=settings.BINANCE_API_KEY,
            api_secret=settings.BINANCE_API_SECRET,
            base_url=settings.BINANCE_FAPI_BASE_URL,
        )
    except Exception:
        pass

_reaction_snap_svc = MarketDataSnapshotService(
    db=_worker_db,
    exchange_client=_reaction_exchange_client,
    exchange_name="binance",
)
_reaction_tracker = MarketReactionTracker(
    db=_worker_db,
    snapshot_interval_minutes=settings.REACTION_SNAPSHOT_INTERVAL_SECONDS / 60.0,
)
_active_symbols = [s.strip() for s in settings.TRADE_SYMBOLS.split(",") if s.strip()]
event_reaction_worker = build_event_reaction_worker(
    db=_worker_db,
    snapshot_service=_reaction_snap_svc,
    tracker=_reaction_tracker,
    active_symbols=_active_symbols,
)

# News Intelligence Layer (Phase 3) - Shadow mode ingestion
news_ingestion_worker = build_news_ingestion_worker(db=_worker_db)
# Real-Time News Layer (Phase 3 hardening) - Shadow mode, API providers
real_time_news_worker = build_real_time_news_worker(db=_worker_db)

@app.on_event("startup")
async def _startup_notifications():
    # Start the worker loop in background
    asyncio.create_task(notification_worker.start())
    asyncio.create_task(backtest_worker.start())
    asyncio.create_task(event_ingestion_worker.start())
    asyncio.create_task(calendar_sync_worker.start())
    asyncio.create_task(event_reaction_worker.start())
    asyncio.create_task(news_ingestion_worker.start())
    asyncio.create_task(real_time_news_worker.start())

@app.on_event("shutdown")
async def _shutdown_notifications():
    # Don't wait - just signal workers to stop
    notification_worker.stop()
    backtest_worker.stop()
    event_ingestion_worker.stop()
    calendar_sync_worker.stop()
    event_reaction_worker.stop()
    await news_ingestion_worker.stop()
    await real_time_news_worker.stop()
    # Workers are daemon tasks, they'll die with the process


# ─── Signal Scheduler (C-1) ──────────────────────────────────────────────────
_signal_scheduler = None


@app.on_event("startup")
async def _startup_signal_scheduler():
    global _signal_scheduler
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from app.signals.crypto_signal_engine import generate_crypto_signals
        from app.signals.signal_expiry import expire_stale_signals
        from app.signals.signal_scheduler_config import SIGNAL_GENERATION_TIMES_UTC
    except ImportError as exc:
        print(f"[SIGNAL_SCHEDULER] WARNING: Could not import scheduler dependencies: {exc}")
        return

    if _signal_scheduler is not None:
        print("[SIGNAL_SCHEDULER] Already started — skipping duplicate startup")
        return

    scheduler = AsyncIOScheduler(timezone="UTC")

    for time_str in SIGNAL_GENERATION_TIMES_UTC:
        hour, minute = map(int, time_str.split(":"))
        job_id = f"signal_gen_{time_str.replace(':', '')}"
        scheduler.add_job(
            generate_crypto_signals,
            trigger="cron",
            hour=hour,
            minute=minute,
            id=job_id,
            replace_existing=True,
            misfire_grace_time=300,
            kwargs={"scheduled_time_utc": time_str, "scheduler_source": "apscheduler"},
        )
        print(f"[SIGNAL_SCHEDULER] SIGNAL_SCHEDULER_JOB_REGISTERED job_id={job_id} time={time_str} UTC")

    scheduler.add_job(
        expire_stale_signals,
        trigger="interval",
        minutes=5,
        id="signal_expiry",
        replace_existing=True,
    )
    print("[SIGNAL_SCHEDULER] SIGNAL_EXPIRY_JOB_REGISTERED job_id=signal_expiry interval=5min")

    if bool(getattr(settings, "ORGANIC_DATASET_NIGHTLY_ENABLED", False)):
        from app.jobs.nightly_dataset_builder import run_nightly_organic_dataset_build

        time_str = str(getattr(settings, "ORGANIC_DATASET_NIGHTLY_TIME_UTC", "02:00"))
        try:
            hour, minute = map(int, time_str.split(":"))
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError("time out of range")
        except (TypeError, ValueError):
            print(
                f"[ORGANIC_DATASET_NIGHTLY] WARNING: Invalid time={time_str}; "
                "using 02:00 UTC"
            )
            hour, minute = 2, 0
            time_str = "02:00"

        scheduler.add_job(
            run_nightly_organic_dataset_build,
            trigger="cron",
            hour=hour,
            minute=minute,
            id="organic_dataset_nightly",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=3600,
        )
        print(
            "[ORGANIC_DATASET_NIGHTLY] ORGANIC_DATASET_JOB_REGISTERED "
            f"job_id=organic_dataset_nightly time={time_str} UTC"
        )

    try:
        from scripts.ml.retrain_pipeline import retrain_entry_model_if_ready

        scheduler.add_job(
            retrain_entry_model_if_ready,
            trigger="cron",
            day=1,
            hour=3,
            minute=0,
            id="ml_monthly_retrain",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=3600,
        )
        print(
            "[ML_MONTHLY_RETRAIN] ML_MONTHLY_RETRAIN_JOB_REGISTERED "
            "job_id=ml_monthly_retrain time=day-1 03:00 UTC"
        )
    except Exception as exc:
        print(
            "[ML_MONTHLY_RETRAIN] WARNING: Could not register monthly retrain job; "
            f"trading runner continues: {exc}"
        )

    try:
        from scripts.validation.daily_paper_validation_monitor import (
            run_daily_paper_validation_monitor,
        )

        scheduler.add_job(
            run_daily_paper_validation_monitor,
            trigger="cron",
            hour=23,
            minute=30,
            id="daily_paper_validation_monitor",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=3600,
        )
        print(
            "[DAILY_PAPER_VALIDATION_MONITOR] JOB_REGISTERED "
            "job_id=daily_paper_validation_monitor time=23:30 UTC"
        )
    except Exception as exc:
        print(
            "[DAILY_PAPER_VALIDATION_MONITOR] WARNING: Could not register daily monitor; "
            f"trading runner continues: {exc}"
        )

    scheduler.start()
    _signal_scheduler = scheduler
    print(f"[SIGNAL_SCHEDULER] SIGNAL_SCHEDULER_STARTED generation_times_utc={SIGNAL_GENERATION_TIMES_UTC}")


@app.on_event("shutdown")
async def _shutdown_signal_scheduler():
    global _signal_scheduler
    if _signal_scheduler:
        _signal_scheduler.shutdown(wait=False)
        _signal_scheduler = None
        print("[SIGNAL_SCHEDULER] SIGNAL_SCHEDULER_STOPPED")
# ─── End Signal Scheduler ─────────────────────────────────────────────────────


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


from app.runner.multi_runner import MultiBotRunner

# ✅ Initialize logger for debugging
logger = logging.getLogger(__name__)

async def runner_loop():
    """
    Continuous loop that executes MultiBotRunner.
    """
    # ✅ USE PRINT instead of logger to bypass logging config
    print("[DEBUG] runner_loop() STARTING - Initializing MultiBotRunner")
    
    try:
        # Initialize Multi-Bot Runner
        runner = MultiBotRunner()
        runner_service.multi_runner = runner
        print("[DEBUG] runner_loop() - MultiBotRunner initialized successfully")
        
        # We use the internal loop of MultiBotRunner which is robust
        print("[DEBUG] runner_loop() - Calling runner.run_loop(interval_seconds=10)")
        await runner.run_loop(interval_seconds=10)
        
        print("[DEBUG] runner_loop() - run_loop() exited normally")
            
    except Exception as e:
        # Catch errors outside the loop
        err = traceback.format_exc()
        runner_service.last_error = f"CRITICAL RUNNER CRASH: {err}"
        runner_service.running = False
        print(f"[DEBUG] CRITICAL RUNNER CRASH: {err}")
        logger.error(f"[DEBUG] CRITICAL RUNNER CRASH: {err}")
        print(f"[{datetime.now()}] CRITICAL RUNNER CRASH: {err}")


@app.get("/")
def root():
    return {
        "status": "ok",
        "exchange": "binance-futures-testnet",
        "api_key_loaded": bool(settings.BINANCE_API_KEY),
        "api_secret_loaded": bool(settings.BINANCE_API_SECRET),
    }


@app.get("/binance/ping", dependencies=[Depends(get_current_active_user)])
def binance_ping():
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.ping()


@app.get("/binance/balance", dependencies=[Depends(get_current_active_user)])
def binance_balance():
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.account_balance()


@app.get("/binance/price", dependencies=[Depends(get_current_active_user)])
def binance_price(symbol: str = "BTCUSDT"):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.mark_price(symbol)


@app.get("/binance/klines", dependencies=[Depends(get_current_active_user)])
def binance_klines(symbol: str = "BTCUSDT", interval: str = "1m", limit: int = 50):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.klines(symbol, interval, limit)


@app.get("/config/symbols", dependencies=[Depends(get_current_active_user)])
def config_symbols():
    symbols = [
        s.strip().upper() for s in settings.TRADE_SYMBOLS.split(",") if s.strip()
    ]
    return {"symbols": symbols, "interval": settings.DEFAULT_INTERVAL}


@app.get("/symbols/universe", dependencies=[Depends(get_current_active_user)])
def symbols_universe():
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )

    requested = parse_symbols(settings.TRADE_SYMBOLS, settings.MAX_SYMBOLS)
    exch = client.exchange_info_cached()
    uni = build_universe(requested, exch)

    return {
        "requested_count": len(uni.requested),
        "valid_count": len(uni.valid),
        "invalid_count": len(uni.invalid),
        "invalid": uni.invalid[:20],  # show only first 20
        "valid_sample": uni.valid[:20],
    }


@app.get("/binance/prices", dependencies=[Depends(get_current_active_user)])
def binance_prices():
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )

    requested = set(parse_symbols(settings.TRADE_SYMBOLS, settings.MAX_SYMBOLS))
    prices = client.all_prices()

    # filter to only our configured symbols
    filtered = [p for p in prices if p.get("symbol") in requested]
    return {
        "count": len(filtered),
        "prices": filtered[:50],
    }  # return first 50 to avoid huge payload


@app.get("/runner/paper/once", dependencies=[Depends(get_current_active_user)])
def paper_run_once(max_symbols: int = 10):
    raise HTTPException(status_code=410, detail="Legacy runner endpoint disabled. Use Auto Pilot.")


@app.get("/runner/paper/state", dependencies=[Depends(get_current_active_user)])
def paper_state():
    runner = get_runner()
    items = list(runner.state.items())[:50]
    return {
        "status": "started",
        "symbols_loaded": len(runner.symbols),
        "state_sample": {k: v.__dict__ for k, v in items},
        "daily": {
            "day": str(runner.daily.day),
            "realized_pnl": runner.daily.realized_pnl,
            "kill": runner.daily.kill,
        },
    }


@app.post("/binance/leverage", dependencies=[Depends(get_current_active_user)])
def binance_set_leverage(symbol: str = "BTCUSDT"):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )

    lev_map = parse_leverage_map(settings.SYMBOL_LEVERAGE_MAP)
    lev = leverage_for(
        symbol, lev_map, settings.DEFAULT_LEVERAGE, settings.MIN_LEVERAGE
    )

    return {
        "symbol": symbol,
        "leverage": lev,
        "result": client.set_leverage(symbol, lev),
    }


@app.get("/binance/qty", dependencies=[Depends(get_current_active_user)])
def binance_qty(symbol: str = "BTCUSDT", usdt: float = 10.0):
    """
    Calculates a valid order quantity for a given USDT amount using stepSize/minQty.
    Uses last price to convert USDT -> qty, then rounds down to stepSize.
    """
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )

    price = client.last_price(symbol)
    exch = client.exchange_info_cached()
    flt = extract_filters(exch, symbol)

    raw_qty = usdt / price
    qty = round_qty(raw_qty, flt.step_size)

    min_usdt_required = float(flt.min_qty) * price
    ok = bool(qty >= flt.min_qty and qty > 0)

    return {
        "symbol": symbol,
        "price": price,
        "usdt": usdt,
        "raw_qty": raw_qty,
        "qty_rounded": str(qty),
        "min_qty": str(flt.min_qty),
        "step_size": str(flt.step_size),
        "min_usdt_required": min_usdt_required,
        "is_valid": ok,
    }



async def require_kyc_trading(user: dict = Depends(get_current_active_user)):
    """Dependency to block trading actions if KYC is not approved"""
    allowed, message = check_kyc_gate(user["id"], KYCAction.START_LIVE_TRADING)
    if not allowed:
        # Check if it's just a demo/paper user? 
        # For now, we enforce strict KYC for the "START_LIVE_TRADING" action.
        # But if the user is in PAPER mode, maybe we allow?
        # The prompt implies "sensitive actions" should be gated.
        # kyc_policy.check_kyc_gate should handle the logic (e.g. allows if requirements not met?)
        # actually check_kyc_gate checks the DB.
        
        # NOTE: If we want to allow PAPER trading without KYC, we should check execution mode here
        # or in the policy. The policy right now just checks case status.
        # Assuming we want to block 'live' actions only.
        
        # If the endpoint is strictly for LIVE trading, we block.
        raise HTTPException(status_code=403, detail=f"KYC Required: {message}")
    return user


@app.post("/trade/market", dependencies=[Depends(require_kyc_trading)])
def trade_market(symbol: str = "XRPUSDT", side: str = "BUY", usdt: float = 10.0):
    """
    Places a MARKET order using the centralized BinanceExecutor.
    """
    raise HTTPException(status_code=410, detail="Manual trading disabled. Use Auto Pilot.")


@app.post("/runner/live/once", dependencies=[Depends(require_kyc_trading)])
def runner_live_once(max_symbols: int = 10):
    raise HTTPException(status_code=410, detail="Legacy runner endpoint disabled. Use Auto Pilot.")


@app.get("/binance/order", dependencies=[Depends(get_current_active_user)])
def binance_order(symbol: str, order_id: int):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.get_order(symbol, order_id)


@app.get("/binance/open-orders", dependencies=[Depends(get_current_active_user)])
def binance_open_orders(symbol: str = "XRPUSDT"):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.open_orders(symbol)


@app.get("/binance/position", dependencies=[Depends(get_current_active_user)])
def binance_position(symbol: str = "XRPUSDT"):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.position_risk(symbol)


@app.post("/trade/close", dependencies=[Depends(get_current_active_user)])
def trade_close(symbol: str = "XRPUSDT"):
    runner = get_runner()
    symbol = symbol.upper()
    
    with runner.symbol_guard(symbol, timeout_s=2.0) as ok:
        if not ok:
            return {"status": "rejected", "reason": "symbol_lock_busy"}

        try:
            res = runner.executor.execute_signal(symbol, "CLOSE", 0.0)
            return {
                "symbol": symbol,
                "action": res.action,
                "details": res.details,
                "error": res.error
            }
        except Exception as e:
            return {"error": "CLOSE_FAILED", "detail": str(e)}


from app.core.config import settings


@app.get("/debug/settings", dependencies=[Depends(get_current_active_user)])
def debug_settings():
    """
    Show loaded settings so we can confirm FORCE_SIGNAL, symbols, sizing, etc.
    """
    return {
        "EXECUTION_MODE": settings.EXECUTION_MODE,
        "TRADE_SYMBOLS": settings.TRADE_SYMBOLS,
        "LIVE_SYMBOLS": settings.LIVE_SYMBOLS,
        "TRADE_USDT_PER_ORDER": settings.TRADE_USDT_PER_ORDER,
        "MIN_NOTIONAL_USDT": settings.MIN_NOTIONAL_USDT,
        "DEFAULT_LEVERAGE": settings.DEFAULT_LEVERAGE,
        "STOP_LOSS_PCT": settings.STOP_LOSS_PCT,
        "TAKE_PROFIT_PCT": settings.TAKE_PROFIT_PCT,
        # "FORCE_SIGNAL": settings.FORCE_SIGNAL,
        "TRADE_MODE": settings.TRADE_MODE,
    }


@app.post("/binance/cancel-all", dependencies=[Depends(get_current_active_user)])
def binance_cancel_all(symbol: str = "XRPUSDT"):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.cancel_all_orders(symbol)


@app.get("/trade/protection", dependencies=[Depends(get_current_active_user)])
def trade_protection(symbol: str = "XRPUSDT"):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    ex = BinanceExecutor(client)
    return ex.ensure_protection(symbol)


@app.get("/risk/daily", dependencies=[Depends(get_current_active_user)])
def risk_daily():
    runner = get_runner()
    today = date.today()

    # ✅ ALWAYS load from DB (source of truth)
    saved = runner.store.load_daily(today)
    if saved:
        runner.daily.day = today
        runner.daily.realized_pnl = float(saved.get("realized_pnl", 0.0))
        runner.daily.kill = bool(saved.get("kill", False))
    else:
        # no record yet → ensure clean state
        runner.daily.day = today
        runner.daily.realized_pnl = 0.0
        runner.daily.kill = False

    return {
        "day": str(today),
        "realized_pnl": runner.daily.realized_pnl,
        "kill": runner.daily.kill,
        "max_loss": settings.DAILY_MAX_LOSS_USDT,
    }


@app.post("/risk/reset", dependencies=[Depends(get_current_active_user)])
def risk_reset():
    runner = get_runner()

    today = date.today()

    # 1) Reset in-memory (runner)
    runner.daily.day = today
    runner.daily.realized_pnl = 0.0
    runner.daily.kill = False

    # 2) Reset DB (source of truth)
    # StateStore already exists on runner as runner.store
    runner.store.save_daily(today, realized_pnl=0.0, kill=False)

    return {
        "status": "reset",
        "day": str(today),
        "kill": False,
        "realized_pnl": 0.0,
    }


@app.post("/risk/circuit/reset", dependencies=[Depends(get_current_active_user)])
def risk_circuit_reset(broker_id: str = None):
    """
    Reset circuit breaker for a specific broker.
    
    Args:
        broker_id: Broker to reset (e.g., BINANCE). If None, resets all.
    """
    from app.risk.circuit import get_circuit_registry
    
    registry = get_circuit_registry()
    
    if broker_id:
        # Reset specific broker
        old_state = registry.get_state(broker_id).value
        registry.reset(broker_id)
        new_state = registry.get_state(broker_id).value
        return {
            "status": "reset",
            "broker_id": broker_id,
            "old_state": old_state,
            "new_state": new_state,
            "message": f"Circuit breaker reset for {broker_id}",
        }
    else:
        # Reset all brokers
        old_states = registry.get_all_states()
        registry.reset_all()
        new_states = registry.get_all_states()
        return {
            "status": "reset_all",
            "old_states": old_states,
            "new_states": new_states,
            "message": "All circuit breakers reset",
        }


@app.get("/risk/circuit/status", dependencies=[Depends(get_current_active_user)])
def risk_circuit_status(broker_id: str = None):
    """
    Get circuit breaker status for all brokers or a specific one.
    """
    from app.risk.circuit import get_circuit_registry
    
    registry = get_circuit_registry()
    
    if broker_id:
        return {
            "broker_id": broker_id,
            "state": registry.get_state(broker_id).value,
            "is_tripped": registry.is_tripped(broker_id),
        }
    else:
        return {
            "brokers": registry.get_all_states(),
            "registered": registry.list_brokers(),
        }


@app.get("/risk/status", dependencies=[Depends(get_current_active_user)])
def risk_status():
    """
    Comprehensive risk status endpoint exposing:
    - Daily state (PnL, kill switch, trade count)
    - Weekly/Monthly snapshots (for drawdown)
    - Circuit Breaker state
    - Gate check result
    """
    runner = get_runner()
    today = date.today()
    
    # Load full risk state
    risk_state = runner.store.load_risk_state(today)
    risk_state.current_equity = runner.get_account_balance()
    risk_state.open_positions = sum(1 for s in runner.state.values() if s.position in ("LONG", "SHORT"))
    
    # Circuit breaker
    circuit_state = "UNKNOWN"
    if hasattr(runner, "circuit_breaker") and runner.circuit_breaker:
        circuit_state = runner.circuit_breaker.get_state().value
    
    # Gate check (would we allow a trade right now?)
    gate_decision = runner.risk_gate.can_open(risk_state, "TEST_SYMBOL")
    
    # Weekly snapshot
    weekly_info = None
    if risk_state.weekly:
        weekly_info = {
            "start_date": str(risk_state.weekly.start_date),
            "start_equity": risk_state.weekly.start_equity,
            "peak_equity": risk_state.weekly.peak_equity,
            "low_equity": risk_state.weekly.low_equity,
        }
    
    # Monthly snapshot
    monthly_info = None
    if risk_state.monthly:
        monthly_info = {
            "start_date": str(risk_state.monthly.start_date),
            "start_equity": risk_state.monthly.start_equity,
            "peak_equity": risk_state.monthly.peak_equity,
            "low_equity": risk_state.monthly.low_equity,
        }
    
    return {
        "timestamp": _utc_now_iso(),
        "daily": {
            "day": str(risk_state.daily.day),
            "realized_pnl": risk_state.daily.realized_pnl,
            "kill": risk_state.daily.kill,
            "trade_count": risk_state.daily.trade_count,
        },
        "weekly_snapshot": weekly_info,
        "monthly_snapshot": monthly_info,
        "current_equity": risk_state.current_equity,
        "open_positions": risk_state.open_positions,
        "circuit_breaker": circuit_state,
        "gate_check": {
            "allowed": gate_decision.allowed,
            "reason_code": gate_decision.reason_code,
            "reason": gate_decision.reason,
            "severity": gate_decision.severity,
        },
        "settings": {
            "max_loss_usdt": settings.DAILY_MAX_LOSS_USDT,
            "max_trades_daily": getattr(settings, "MAX_TRADES_DAILY", 20),
            "max_open_positions": getattr(settings, "MAX_OPEN_POSITIONS", 3),
            "max_weekly_drawdown_pct": getattr(settings, "MAX_WEEKLY_DRAWDOWN_PCT", 0.0),
            "max_monthly_drawdown_pct": getattr(settings, "MAX_MONTHLY_DRAWDOWN_PCT", 0.0),
        }
    }


@app.post("/trade/close-record", dependencies=[Depends(get_current_active_user)])
def trade_close_record(symbol: str = "ETHUSDT"):
    global paper_runner_instance

    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )

    # Ensure runner exists (risk tracker lives there)
    if paper_runner_instance is None:
        paper_runner_instance = PaperRunner(client)

    # Read current position BEFORE closing
    pos = client.get_position_info(symbol)
    if not pos:
        return {"status": "no_position_info"}

    pos_amt = float(pos.get("positionAmt", "0"))
    entry_price = float(pos.get("entryPrice", "0"))

    if pos_amt == 0:
        return {"status": "flat"}

    # Close
    close = client.close_position_market(symbol)

    # Use mark/last price as exit for MVP
    exit_price = client.last_price(symbol)
    qty = abs(pos_amt)

    pnl = (
        (exit_price - entry_price) * qty
        if pos_amt > 0
        else (entry_price - exit_price) * qty
    )

    paper_runner_instance.daily.realized_pnl += pnl
    if paper_runner_instance.daily.realized_pnl <= -settings.DAILY_MAX_LOSS_USDT:
        paper_runner_instance.daily.kill = True

    return {
        "status": "closed",
        "symbol": symbol,
        "qty": qty,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "pnl_added": pnl,
        "daily_realized_pnl": paper_runner_instance.daily.realized_pnl,
        "kill": paper_runner_instance.daily.kill,
        "close_order": close,
    }


@app.post("/runner/live/start")
async def runner_live_start(
    interval_seconds: int | None = None,
    max_symbols: int | None = None,
):
    # already running?
    if (
        runner_service.running
        and runner_service.task
        and not runner_service.task.done()
    ):
        return {
            "status": "already_running",
            **runner_status(),
        }

    runner = get_runner()  # ensure runner exists

    # ✅ Use CURRENT_RUN_ID as the single source of truth
    global CURRENT_RUN_ID

    if not CURRENT_RUN_ID:
        CURRENT_RUN_ID = str(uuid.uuid4())

    runner.run_id = CURRENT_RUN_ID

    # ✅ set context run_id so Audit can attach automatically
    set_run_id(CURRENT_RUN_ID)

    runner.audit.start_run(
        run_id=runner.run_id,
        mode=settings.EXECUTION_MODE.lower(),
        interval_seconds=runner_service.interval_seconds,
        max_symbols=runner_service.max_symbols,
    )

    runner_service.running = True
    runner_service.mode = settings.EXECUTION_MODE.lower()  # "paper" or "live"
    runner_service.interval_seconds = interval_seconds or settings.RUN_INTERVAL_SECONDS
    runner_service.max_symbols = max_symbols or settings.RUN_MAX_SYMBOLS
    runner_service.started_at = _utc_now_iso()
    runner_service.last_cycle_at = None
    runner_service.cycle_count = 0
    runner_service.last_error = None

    runner_service.task = asyncio.create_task(runner_loop())

    return {
        "status": "started",
        **runner_status(),
    }


@app.post("/runner/live/stop")
async def runner_live_stop():
    if not runner_service.running:
        return {
            "status": "not_running",
            **runner_status(),
        }

    runner_service.running = False

    # cancel task if needed
    if runner_service.task and not runner_service.task.done():
        runner_service.task.cancel()
        try:
            await runner_service.task
        except asyncio.CancelledError:
            # expected when we cancel the background loop
            pass
        except Exception:
            pass

    runner_service.task = None

    # ✅ ADD: stop audit run (DO NOT REMOVE ANYTHING ELSE)
    runner = get_runner()
    if runner.run_id:
        runner.audit.stop_run(runner.run_id)

    return {
        "status": "stopped",
        **runner_status(),
    }



def runner_status() -> dict:
    runner = get_runner()

    return {
        "running": runner_service.running,
        "mode": runner_service.mode,
        "interval_seconds": runner_service.interval_seconds,
        "max_symbols": runner_service.max_symbols,
        "started_at": runner_service.started_at,
        "last_cycle_at": runner_service.last_cycle_at,
        "cycle_count": runner_service.cycle_count,
        "last_error": runner_service.last_error,
        "daily": {
            "day": str(runner.daily.day),
            "realized_pnl": runner.daily.realized_pnl,
            "kill": runner.daily.kill,
            "max_loss": settings.DAILY_MAX_LOSS_USDT,
        },
    }


@app.get("/runner/status")
def runner_status_endpoint():
    return runner_status()


@app.on_event("shutdown")
async def on_shutdown():
    if runner_service.running:
        runner_service.running = False
        if runner_service.task and not runner_service.task.done():
            runner_service.task.cancel()
            try:
                await runner_service.task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass


@app.get("/logs/events/tail")
def logs_events_tail(limit: int = 50):
    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500

    runner = get_runner()

    with runner.db.connect() as conn:
        rows = conn.execute(
            "SELECT id, timestamp_utc, run_id, cycle_id, symbol, event_type, action, details_json FROM events ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()

    data = []
    for r in rows[::-1]:
        data.append(
            {
                "id": r["id"],
                "timestamp_utc": r["timestamp_utc"],
                "run_id": r["run_id"],
                "cycle_id": r["cycle_id"],
                "symbol": r["symbol"],
                "event_type": r["event_type"],
                "action": r["action"],
                "details": json.loads(r["details_json"] or "{}"),
            }
        )

    return {"count": len(data), "events": data}


@app.get("/debug/db/daily")
def debug_db_daily():
    runner = get_runner()
    with runner.db.connect() as conn:
        rows = conn.execute(
            "SELECT * FROM daily_state ORDER BY day DESC LIMIT 30"
        ).fetchall()
    return {"rows": [dict(r) for r in rows]}


@app.get("/debug/db/symbols")
def debug_db_symbols():
    runner = get_runner()
    with runner.db.connect() as conn:
        rows = conn.execute(
            "SELECT symbol, position, last_signal, last_action, last_checked_ms, last_trade_ms, pending_open, entry_qty, updated_at FROM symbol_state ORDER BY updated_at DESC LIMIT 200"
        ).fetchall()
    return {"rows": [dict(r) for r in rows]}


@app.post("/trade/close-record-usertrades")
def trade_close_record_usertrades(symbol: str = "ETHUSDT", window_minutes: int = 10):
    """
    Robust close endpoint:
    - closes position
    - waits for FILLED / flat confirmation
    - calculates realized pnl from userTrades (dedup via saved symbol_state.last_user_trade_id)
    - updates daily_state
    - ✅ records CLOSE fill with attribution inherited from last OPEN
    - ✅ writes signal_outcomes + strategy_performance rows
    - ✅ stamps run_id/cycle_id for fills written by this endpoint
    - ✅ avoids polluting confidence calibration when confidence is None
    """
    runner = get_runner()
    client = runner.client

    # ✅ normalize early (avoid whitespace / casing issues)
    symbol = (symbol or "").upper().strip()
    if not symbol:
        return {"status": "error", "error": "symbol_required"}

    # ✅ OPTIONAL (strongly recommended): reject symbols not in configured universe
    try:
        live_set = {
            s.upper().strip() for s in getattr(settings, "LIVE_SYMBOLS", []) or []
        }
        trade_set = {
            s.upper().strip() for s in getattr(settings, "TRADE_SYMBOLS", []) or []
        }
        universe = live_set | trade_set
        if universe and symbol not in universe:
            return {
                "status": "error",
                "error": "symbol_not_in_configured_universe",
                "symbol": symbol,
                "hint": "Add it to LIVE_SYMBOLS/TRADE_SYMBOLS or call the correct symbol.",
            }
    except Exception:
        # don't block endpoint if settings parsing fails
        pass

    # ✅ ADD: prevent collision with runner loop for same symbol
    with runner.symbol_guard(symbol, timeout_s=2.0) as ok:
        if not ok:
            return {
                "status": "rejected",
                "reason": "symbol_lock_busy",
                "symbol": symbol,
            }

        # 1) Read current position (✅ FIX: never crash on invalid symbol)
        try:
            pos = client.get_position_info(symbol)
        except RuntimeError as e:
            msg = str(e)
            if "Invalid symbol" in msg or '"code":-1121' in msg or 'code":-1121' in msg:
                return {
                    "status": "error",
                    "error": "BINANCE_INVALID_SYMBOL",
                    "symbol": symbol,
                    "detail": msg,
                }
            return {
                "status": "error",
                "error": "BINANCE_RUNTIME_ERROR",
                "symbol": symbol,
                "detail": msg,
            }
        except Exception as e:
            return {
                "status": "error",
                "error": "POSITION_INFO_FAILED",
                "symbol": symbol,
                "detail": f"{type(e).__name__}: {e}",
            }

        if not pos:
            return {"status": "no_position_info", "symbol": symbol}

        try:
            pos_amt = float(pos.get("positionAmt", "0") or 0.0)
        except Exception:
            pos_amt = 0.0

        if pos_amt == 0.0:
            return {"status": "flat", "symbol": symbol}

        # 2) Close (✅ FIX: catch Binance errors)
        try:
            close_order = client.close_position_market(symbol)
        except RuntimeError as e:
            msg = str(e)
            if "Invalid symbol" in msg or '"code":-1121' in msg or 'code":-1121' in msg:
                return {
                    "status": "error",
                    "error": "BINANCE_INVALID_SYMBOL",
                    "symbol": symbol,
                    "detail": msg,
                }
            return {
                "status": "error",
                "error": "CLOSE_FAILED_RUNTIME",
                "symbol": symbol,
                "detail": msg,
            }
        except Exception as e:
            return {
                "status": "error",
                "error": "CLOSE_FAILED",
                "symbol": symbol,
                "detail": f"{type(e).__name__}: {e}",
            }

        order_id = None
        try:
            order_id = close_order.get("orderId")
        except Exception:
            order_id = None

        # 3) Wait for order filled + position flat (robust)
        import time

        filled = False
        flat = False

        for _ in range(20):  # ~6 seconds max (20 * 0.3)
            try:
                if order_id is not None:
                    try:
                        od = client.get_order(symbol, int(order_id))
                        if (od or {}).get("status") == "FILLED":
                            filled = True
                    except Exception:
                        pass

                # also check flat (✅ FIX: protect against invalid symbol / temporary errors)
                try:
                    p2 = client.get_position_info(symbol)
                    amt2 = float(p2.get("positionAmt", "0") or 0.0) if p2 else 0.0
                    if amt2 == 0.0:
                        flat = True
                except Exception:
                    pass

                if filled and flat:
                    break
            except Exception:
                pass

            time.sleep(0.3)

        # 4) Compute realized pnl from userTrades (dedup)
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (max(1, int(window_minutes)) * 60 * 1000)

        st = runner.state.get(symbol)
        if st is None:
            # if symbol not in runner list, create minimal state entry
            try:
                from app.runner.models import SymbolState  # ✅ corrected import
            except Exception:
                # fallback (in case structure differs)
                from app.runner.runner import SymbolState  # type: ignore
            st = SymbolState()
            runner.state[symbol] = st

        # ✅ FIX: user_trades call can also raise if symbol invalid / API issue
        try:
            trades = (
                client.user_trades(
                    symbol, start_time_ms=start_ms, end_time_ms=end_ms, limit=1000
                )
                or []
            )
        except RuntimeError as e:
            msg = str(e)
            if "Invalid symbol" in msg or '"code":-1121' in msg or 'code":-1121' in msg:
                return {
                    "status": "error",
                    "error": "BINANCE_INVALID_SYMBOL",
                    "symbol": symbol,
                    "detail": msg,
                    "filled": filled,
                    "flat": flat,
                    "order_id": order_id,
                    "close_order": close_order,
                }
            return {
                "status": "error",
                "error": "USERTRADES_FAILED_RUNTIME",
                "symbol": symbol,
                "detail": msg,
                "filled": filled,
                "flat": flat,
                "order_id": order_id,
                "close_order": close_order,
            }
        except Exception as e:
            return {
                "status": "error",
                "error": "USERTRADES_FAILED",
                "symbol": symbol,
                "detail": f"{type(e).__name__}: {e}",
                "filled": filled,
                "flat": flat,
                "order_id": order_id,
                "close_order": close_order,
            }

        new_trades = []
        try:
            last_id = int(getattr(st, "last_user_trade_id", 0) or 0)
        except Exception:
            last_id = 0

        max_id = last_id
        for t in trades:
            tid = t.get("id")
            if tid is None:
                continue
            try:
                tid_i = int(tid)
            except Exception:
                continue
            if tid_i > last_id:
                new_trades.append(t)
                if tid_i > max_id:
                    max_id = tid_i

        # ✅ FIX 1: always stamp ids for fills written by this endpoint
        set_run_id(str(uuid.uuid4()))
        set_cycle_id(str(uuid.uuid4()))

        pnl_added = 0.0
        try:
            pnl_added = float(realized_pnl_from_user_trades(new_trades) or 0.0)
        except Exception:
            pnl_added = 0.0

        # ============================
        # ✅ NEW ADJUSTMENT BLOCK START
        # ============================

        # ✅ Compute close fill stats from new_trades (qty/avg price/fees)
        total_qty = 0.0
        notional = 0.0
        total_fee = 0.0

        for t in new_trades:
            try:
                q = float(t.get("qty") or 0.0)
                p = float(t.get("price") or 0.0)
                total_qty += q
                notional += q * p
                if t.get("commission") is not None:
                    total_fee += float(t.get("commission") or 0.0)
            except Exception:
                pass

        avg_price = (notional / total_qty) if total_qty > 0 else 0.0

        # ✅ Look up most recent "unmatched" OPEN to inherit attribution
        db = getattr(runner, "db", None)
        if db is None:
            from shared_lib.persistence.db import DB

            db = DB()

        open_meta = None
        _bot_id = None
        try:
            _bot_id = getattr(getattr(runner, "context", None), "bot_instance_id", None)
        except Exception:
            _bot_id = None
        with db.connect() as conn:
            if _bot_id:
                last_close = conn.execute(
                    """
                    SELECT id
                    FROM trade_fills
                    WHERE symbol=? AND action='CLOSE'
                      AND (bot_instance_id = ? OR (bot_instance_id IS NULL AND ? = 'default'))
                    ORDER BY id DESC LIMIT 1
                    """,
                    (symbol, _bot_id, _bot_id),
                ).fetchone()
            else:
                last_close = conn.execute(
                    "SELECT id FROM trade_fills WHERE symbol=? AND action='CLOSE' ORDER BY id DESC LIMIT 1",
                    (symbol,),
                ).fetchone()
            last_close_id = int(last_close["id"]) if last_close else 0

            if _bot_id:
                open_meta = conn.execute(
                    """
                    SELECT * FROM trade_fills
                    WHERE symbol=? AND action='OPEN' AND id > ?
                      AND (bot_instance_id = ? OR (bot_instance_id IS NULL AND ? = 'default'))
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (symbol, last_close_id, _bot_id, _bot_id),
                ).fetchone()
            else:
                open_meta = conn.execute(
                    """
                    SELECT * FROM trade_fills
                    WHERE symbol=? AND action='OPEN' AND id > ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (symbol, last_close_id),
                ).fetchone()

            if open_meta is None:
                if _bot_id:
                    open_meta = conn.execute(
                        """
                        SELECT * FROM trade_fills
                        WHERE symbol=? AND action='OPEN'
                          AND (bot_instance_id = ? OR (bot_instance_id IS NULL AND ? = 'default'))
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (symbol, _bot_id, _bot_id),
                    ).fetchone()
                else:
                    open_meta = conn.execute(
                        """
                        SELECT * FROM trade_fills
                        WHERE symbol=? AND action='OPEN'
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (symbol,),
                    ).fetchone()

        def _row_get(row, key, default=None):
            if row is None:
                return default
            try:
                return row[key]
            except Exception:
                return default

        # Defaults if we can't find a matching open row
        strategy = getattr(getattr(runner, "strategy", None), "name", "unknown")
        strategy_version = getattr(getattr(runner, "strategy", None), "version", "0")
        broker_id = getattr(settings, "BROKER_ID", "binance_futures")
        account_id = getattr(settings, "ACCOUNT_ID", "default")
        asset_class = getattr(settings, "ASSET_CLASS", "CRYPTO")
        timeframe = getattr(settings, "DEFAULT_INTERVAL", "1m")
        confidence = None
        side = "UNKNOWN"

        if open_meta is not None:
            try:
                strategy = _row_get(open_meta, "strategy", strategy) or strategy
                user_id = _row_get(open_meta, "user_id", None)
                bot_instance_id = _row_get(open_meta, "bot_instance_id", None)
                broker_account_id = _row_get(open_meta, "broker_account_id", None)
            except Exception:
                pass
            try:
                strategy_version = (
                    _row_get(open_meta, "strategy_version", strategy_version)
                    or strategy_version
                )
            except Exception:
                pass
            try:
                broker_id = _row_get(open_meta, "broker_id", broker_id) or broker_id
            except Exception:
                pass
            try:
                account_id = _row_get(open_meta, "account_id", account_id) or account_id
            except Exception:
                pass
            try:
                asset_class = (
                    _row_get(open_meta, "asset_class", asset_class) or asset_class
                )
            except Exception:
                pass
            try:
                timeframe = _row_get(open_meta, "timeframe", timeframe) or timeframe
            except Exception:
                pass
            try:
                confidence = _row_get(open_meta, "confidence", None)
                confidence = float(confidence) if confidence is not None else None
            except Exception:
                confidence = None
            try:
                side = _row_get(open_meta, "side", side) or side
            except Exception:
                pass

        # ✅ Record CLOSE fill inheriting open attribution
        from shared_lib.persistence.trade_fills import record_fill, ExitReason
        from shared_lib.persistence.db import utc_now_iso

        _close_position_id = getattr(st, "position_id", _row_get(open_meta, "position_id", None))
        _st_entry = float(getattr(st, "entry_price", 0.0) or 0.0)
        _st_sl = float(getattr(st, "current_stop_loss", 0.0) or 0.0)
        
        _r_multiple = None
        if _st_entry > 0 and _st_sl > 0 and _st_entry != _st_sl:
            _risk = abs(_st_entry - _st_sl)
            if side == "LONG":
                _pnl_ps = float(avg_price) - _st_entry
            else:
                _pnl_ps = _st_entry - float(avg_price)
            _r_multiple = float(_pnl_ps / _risk)

        record_fill(
            db,
            symbol=symbol,
            side=side,
            action="CLOSE",
            qty=float(total_qty) if total_qty > 0 else 0.0,
            price=float(avg_price) if avg_price else 0.0,
            fee=float(total_fee) if total_fee else None,
            realized_pnl=float(pnl_added),
            strategy=strategy,
            strategy_version=strategy_version,
            broker_id=broker_id,
            account_id=account_id,
            asset_class=asset_class,
            timeframe=str(timeframe),
            confidence=confidence,
            position_id=_close_position_id,
            stop_loss_price=_st_sl if _st_sl > 0 else None,
            entry_price_expected=None,  # no reference price for API manual close
            slippage_pct=None,          # no reference price → cannot compute slippage
            r_multiple=_r_multiple,
            exit_reason=ExitReason.MANUAL,
            user_id=user_id if 'user_id' in locals() else None,
            bot_instance_id=bot_instance_id if 'bot_instance_id' in locals() else None,
            broker_account_id=broker_account_id if 'broker_account_id' in locals() else None,
        )
        
        # Clear out state properties
        st.position_id = None
        st.current_stop_loss = None

        # ✅ FIX 2: confidence calibration (don’t pollute with NULL confidence)
        outcome = 1 if float(pnl_added) > 0 else 0
        conf_for_row = None if confidence is None else float(confidence)

        if conf_for_row is not None:
            with db.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO signal_outcomes (
                        strategy, strategy_version, symbol, asset_class, broker_id, account_id, timeframe,
                        confidence, outcome, pnl, created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        strategy,
                        strategy_version,
                        symbol,
                        asset_class,
                        broker_id,
                        account_id,
                        str(timeframe),
                        float(conf_for_row),
                        int(outcome),
                        float(pnl_added),
                        utc_now_iso(),
                    ),
                )

        # ✅ Update strategy_performance (simple upsert)
        with db.connect() as conn:
            row = conn.execute(
                """
                SELECT id, trades, wins, losses, net_pnl, fees
                FROM strategy_performance
                WHERE strategy=? AND strategy_version=? AND symbol=? AND asset_class=? AND broker_id=? AND account_id=? AND timeframe=?
                LIMIT 1
                """,
                (
                    strategy,
                    strategy_version,
                    symbol,
                    asset_class,
                    broker_id,
                    account_id,
                    str(timeframe),
                ),
            ).fetchone()

            if row:
                trades_n = int(row["trades"]) + 1
                wins_n = int(row["wins"]) + (1 if outcome == 1 else 0)
                losses_n = int(row["losses"]) + (1 if outcome == 0 else 0)
                net_pnl_n = float(row["net_pnl"] or 0.0) + float(pnl_added or 0.0)

                # ✅ OPTIONAL: safer NULL handling
                fees_n = float(row["fees"] or 0.0) + float(total_fee or 0.0)

                conn.execute(
                    """
                    UPDATE strategy_performance
                    SET trades=?, wins=?, losses=?, net_pnl=?, gross_pnl=?, fees=?, updated_at=?
                    WHERE id=?
                    """,
                    (
                        trades_n,
                        wins_n,
                        losses_n,
                        net_pnl_n,
                        net_pnl_n,
                        fees_n,
                        utc_now_iso(),
                        int(row["id"]),
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO strategy_performance (
                        strategy, strategy_version, symbol, asset_class, broker_id, account_id, timeframe,
                        trades, wins, losses, net_pnl, gross_pnl, fees, avg_slippage, avg_r, max_drawdown, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        strategy,
                        strategy_version,
                        symbol,
                        asset_class,
                        broker_id,
                        account_id,
                        str(timeframe),
                        1,
                        1 if outcome == 1 else 0,
                        1 if outcome == 0 else 0,
                        float(pnl_added or 0.0),
                        float(pnl_added or 0.0),
                        float(total_fee or 0.0),
                        0.0,
                        0.0,
                        0.0,
                        utc_now_iso(),
                    ),
                )

        # ==========================
        # ✅ NEW ADJUSTMENT BLOCK END
        # ==========================

        st.last_user_trade_id = max_id

        # persist symbol state (so dedup survives restart)
        try:
            runner.store.save_symbol(symbol, st)
        except Exception:
            pass

        # 5) Update daily
        runner.daily.reset_if_new_day()
        try:
            runner.daily.realized_pnl = float(runner.daily.realized_pnl or 0.0) + float(
                pnl_added or 0.0
            )
        except Exception:
            runner.daily.realized_pnl = float(pnl_added or 0.0)

        if runner.daily.realized_pnl <= -float(settings.DAILY_MAX_LOSS_USDT):
            runner.daily.kill = True

        try:
            runner.store.save_daily(
                runner.daily.day, runner.daily.realized_pnl, runner.daily.kill
            )
        except Exception:
            pass

        return {
            "status": "closed_recorded",
            "symbol": symbol,
            "order_id": order_id,
            "filled": filled,
            "flat": flat,
            "userTrades_window_minutes": int(window_minutes),
            "userTrades_total": len(trades),
            "userTrades_new_count": len(new_trades),
            "pnl_added": pnl_added,
            "daily_realized_pnl": runner.daily.realized_pnl,
            "kill": runner.daily.kill,
            "close_order": close_order,
        }


@app.post("/risk/kill")
def risk_kill(reason: str = "manual_kill"):
    runner = get_runner()

    # set in memory
    today = date.today()
    runner.daily.day = today
    runner.daily.kill = True

    # (optional) if your DailyLossState supports this field, keep it; otherwise remove
    if hasattr(runner.daily, "kill_reason"):
        runner.daily.kill_reason = reason

    # save to DB (persist immediately)
    try:
        runner.store.save_daily(
            runner.daily.day,
            runner.daily.realized_pnl,
            runner.daily.kill,
        )
    except Exception:
        pass

    # log event
    try:
        runner.audit.log_event(
            event_type="RISK_KILL",
            symbol=None,
            action="KILL_SWITCH_ON",
            details={"reason": reason, "day": str(today)},
        )
    except Exception:
        pass

    return {"status": "killed", "reason": reason, "day": str(today)}


@app.post("/risk/unkill")
def risk_unkill():
    runner = get_runner()
    today = date.today()

    runner.daily.day = today
    runner.daily.kill = False

    # ✅ persist correctly
    runner.store.save_daily(
        today,
        runner.daily.realized_pnl,
        False,
    )

    runner.audit.event(
        event_type="RISK",
        run_id=getattr(runner, "run_id", None),
        symbol=None,
        action="KILL_SWITCH_RESET",
        details={"day": str(today), "reset_pnl": False},
    )
    return {
        "ok": True,
        "day": str(today),
        "kill": runner.daily.kill,
        "realized_pnl": runner.daily.realized_pnl,
    }


@app.get("/strategy/signal")
def strategy_signal(symbol: str = "ETHUSDT"):
    runner = get_runner()
    res = runner.strategy.get_signal(symbol)
    return {
        "symbol": symbol.upper(),
        "strategy": runner.strategy.name,
        "signal": res.signal.value,
        "confidence": res.confidence,
        "reason": res.reason,
        "meta": res.meta,
    }


@app.post("/debug/crash-next-cycle")
def debug_crash_next_cycle():
    runner_service.crash_next_cycle = True
    return {"status": "ok", "crash_next_cycle": runner_service.crash_next_cycle}


@app.get("/runner/audit/tail")
def audit_tail(limit: int = Query(50, ge=1, le=500)):
    """
    Tail the live audit log so we can see DECISION / EXECUTION_RESULT without opening files.
    """
    path = Path("logs/live_audit.jsonl")
    if not path.exists():
        return {"ok": False, "error": "logs/live_audit.jsonl not found"}

    # read last N lines safely
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    tail = lines[-limit:]
    return {"ok": True, "limit": limit, "lines": tail}


@app.post("/emergency/flatten")
async def emergency_flatten():
    """
    Cancel all open orders and close all positions for EVERY active bot managed by MultiBotRunner.
    This replaces the legacy symbol-only flatten logic.
    """
    if not runner_service.multi_runner:
        # Fallback to legacy flatten if MultiBotRunner isn't active (unlikely)
        logger.warning("[EMERGENCY] MultiBotRunner not found. Falling back to legacy flatten logic.")
        # ... (Legacy logic below if we want to keep it, but better to fail or fix)
        return {"ok": False, "error": "MultiBotRunner not initialized."}

    try:
        results = await runner_service.multi_runner.flatten_all()
        # Audit
        try:
            # Audit on the first available runner or generally
            for bot_res in results:
                bot_id = bot_res['bot_id']
                logger.info(f"[EMERGENCY] Bot {bot_id} flatted: {len(bot_res['symbols'])} symbols processed.")
        except Exception:
            pass

        return {
            "ok": True, 
            "message": f"Flatten triggered for {len(results)} active bots.",
            "results": results
        }
    except Exception as e:
        logger.error(f"Critical error during Global Emergency Flatten: {e}")
        return {"ok": False, "error": str(e)}


@app.get("/debug/position_amt/{symbol}")
def debug_position_amt(symbol: str):
    runner = get_runner()
    amt = runner.client.get_position_amt(symbol.upper())
    return {"symbol": symbol.upper(), "position_amt": amt}


@app.post("/risk/reset_kill")
def reset_kill(reset_pnl: bool = False):
    runner = get_runner()

    # reset the actual flag used everywhere
    runner.daily.kill = False

    if reset_pnl:
        runner.daily.realized_pnl = 0.0

    # persist so restarts don’t re-load kill=true
    try:
        runner.store.save_daily(
            runner.daily.day, runner.daily.realized_pnl, runner.daily.kill
        )
    except Exception:
        pass

    runner.audit.event(
        event_type="RISK",
        run_id=getattr(runner, "run_id", None),
        symbol=None,
        action="KILL_SWITCH_RESET",
        details={
            "at": runner.db.now_utc().isoformat() if hasattr(runner, "db") else None,
            "reset_pnl": reset_pnl,
            "day": str(runner.daily.day),
        },
    )

    return {
        "ok": True,
        "kill": runner.daily.kill,
        "realized_pnl": runner.daily.realized_pnl,
    }


@app.post("/debug/set_last_stop")
def debug_set_last_stop(payload: dict = Body(...)):
    """
    Set last_stop_ms for a symbol to simulate a recent stop-loss.
    This does NOT place any trades.
    """
    symbol = (payload.get("symbol") or "").upper()
    minutes_ago = int(payload.get("minutes_ago") or 0)

    if not symbol:
        return {"ok": False, "error": "symbol is required"}

    runner = get_runner()  # ✅ YOUR runner source

    st = runner.state.get(symbol)
    if st is None:
        return {"ok": False, "error": f"symbol {symbol} not in runner.state"}

    now_ms = int(time.time() * 1000)
    st.last_stop_ms = now_ms - (minutes_ago * 60 * 1000)

    # reset confirmation state (if these fields exist)
    if hasattr(st, "reentry_confirm_signal"):
        st.reentry_confirm_signal = "NONE"
    if hasattr(st, "reentry_confirm_count"):
        st.reentry_confirm_count = 0

    # persist if you have a store
    if hasattr(runner, "store") and runner.store is not None:
        try:
            runner.store.save_symbol(symbol, st)
        except Exception:
            pass

    return {
        "ok": True,
        "symbol": symbol,
        "last_stop_ms": st.last_stop_ms,
        "minutes_ago": minutes_ago,
    }


def _csv_runtime(value: str) -> list[str]:
    return [part.strip().upper() for part in str(value or "").split(",") if part.strip()]


def _phase6_runtime_fingerprint() -> Dict[str, Any]:
    try:
        from app.queue.external_signal_processor import ExternalSignalProcessor
        phase6_gate_available = hasattr(ExternalSignalProcessor, "_phase6_gate_for_row")
    except Exception:
        phase6_gate_available = False

    lockout_status: dict[str, Any] | None = None
    try:
        from shared_lib.persistence.tradingview import get_tradingview_safety_lockout

        with DB(path=get_db_path()).connect() as conn:
            row = conn.execute(
                "SELECT id FROM bot_instances WHERE status='active' ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
        if row:
            lockout_status = get_tradingview_safety_lockout(DB(path=get_db_path()), row["id"])
    except Exception as exc:
        lockout_status = {"error": str(exc)}

    git_commit = None
    try:
        git_commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(Path(__file__).resolve().parents[2]),
            text=True,
            timeout=2,
        ).strip()
    except Exception:
        git_commit = None

    return {
        "code_version": git_commit,
        "process_started_at": PROCESS_STARTED_AT,
        "config_loaded_at": CONFIG_LOADED_AT,
        "pid": os.getpid(),
        "working_directory": os.getcwd(),
        "python_executable": sys.executable,
        "phase6_gate_available": phase6_gate_available,
        "phase6_gate_code_version": PHASE6_GATE_CODE_VERSION,
        "TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED": settings.TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED,
        "TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED": settings.TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED,
        "TRADINGVIEW_ALLOWED_SYMBOLS": _csv_runtime(settings.TRADINGVIEW_ALLOWED_SYMBOLS),
        "TRADINGVIEW_ALLOWED_ACTIONS": _csv_runtime(settings.TRADINGVIEW_ALLOWED_ACTIONS),
        "TRADINGVIEW_MAX_QUEUE_PER_CYCLE": settings.TRADINGVIEW_MAX_QUEUE_PER_CYCLE,
        "TRADINGVIEW_MAX_EXECUTIONS_PER_DAY": settings.TRADINGVIEW_MAX_EXECUTIONS_PER_DAY,
        "TRADINGVIEW_MAX_SIGNALS_PER_HOUR": settings.TRADINGVIEW_MAX_SIGNALS_PER_HOUR,
        "TRADINGVIEW_MAX_SIGNALS_PER_DAY": settings.TRADINGVIEW_MAX_SIGNALS_PER_DAY,
        "TRADINGVIEW_MAX_TRADE_USDT_CAP": settings.TRADINGVIEW_MAX_TRADE_USDT_CAP,
        "TRADINGVIEW_ALLOW_CLOSE": settings.TRADINGVIEW_ALLOW_CLOSE,
        "TRADINGVIEW_ALLOW_REVERSE": settings.TRADINGVIEW_ALLOW_REVERSE,
        "TRADINGVIEW_ALLOW_REDUCE": settings.TRADINGVIEW_ALLOW_REDUCE,
        "TRADINGVIEW_ALLOW_CANCEL": settings.TRADINGVIEW_ALLOW_CANCEL,
        "TRADINGVIEW_ALLOW_EXTERNAL_SLTP": settings.TRADINGVIEW_ALLOW_EXTERNAL_SLTP,
        "TRADINGVIEW_ALLOW_EXTERNAL_SIZE": settings.TRADINGVIEW_ALLOW_EXTERNAL_SIZE,
        "TRADINGVIEW_ALLOW_RISK_OVERRIDE": settings.TRADINGVIEW_ALLOW_RISK_OVERRIDE,
        "TRADINGVIEW_REQUIRE_SLTP_PROTECTION": settings.TRADINGVIEW_REQUIRE_SLTP_PROTECTION,
        "TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL": settings.TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL,
        "active_safety_lockout": bool(lockout_status and int(lockout_status.get("is_locked") or 0)),
        "active_safety_lockout_reason": (lockout_status or {}).get("reason"),
    }


@app.get("/api/admin/tradingview/runtime-fingerprint")
async def tradingview_runtime_fingerprint():
    return _phase6_runtime_fingerprint()


def _active_bot_id_for_tradingview() -> str | None:
    try:
        with DB(path=get_db_path()).connect() as conn:
            row = conn.execute(
                "SELECT id FROM bot_instances WHERE status='active' ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
        return str(row["id"]) if row else None
    except Exception:
        return None


def _phase6_status_counts(bot_id: str | None) -> Dict[str, Any]:
    if not bot_id:
        return {
            "signals_used_today": 0,
            "executions_used_today": 0,
            "rejected_due_to_limits": 0,
            "unprotected_positions_count": 0,
            "stuck_claimed_rows_count": 0,
            "last_processor_result": None,
            "last_tradingview_candidate": None,
        }
    day_ago = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    with DB(path=get_db_path()).connect() as conn:
        signal_count = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM external_signal_queue
            WHERE bot_id = ? AND source = 'TRADINGVIEW' AND created_at >= ?
            """,
            (bot_id, day_ago),
        ).fetchone()["c"]
        exec_count = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM tradingview_signal_decisions
            WHERE bot_id = ?
              AND created_at >= ?
              AND final_status IN ('PROCESSED_EXECUTED','PROCESSED_EXECUTED_PROTECTED')
            """,
            (bot_id, day_ago),
        ).fetchone()["c"]
        limit_rejects = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM tradingview_signal_decisions
            WHERE bot_id = ?
              AND created_at >= ?
              AND final_status IN (
                'REJECTED_TV_RATE_LIMIT',
                'REJECTED_TV_DAILY_EXECUTION_CAP',
                'REJECTED_TV_TRADE_CAP_EXCEEDED'
              )
            """,
            (bot_id, day_ago),
        ).fetchone()["c"]
        unprotected = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM position_lifecycle_state
            WHERE bot_instance_id = ?
              AND COALESCE(exchange_position_active, 0) = 1
              AND (
                sl_order_id IS NULL OR tp_order_id IS NULL
                OR sl_order_id LIKE 'DUPLICATE_%'
                OR tp_order_id LIKE 'DUPLICATE_%'
              )
            """,
            (bot_id,),
        ).fetchone()["c"]
        stuck = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM external_signal_queue
            WHERE bot_id = ? AND status = 'CLAIMED'
            """,
            (bot_id,),
        ).fetchone()["c"]
        heartbeat = conn.execute(
            """
            SELECT *
            FROM tradingview_processor_heartbeat
            WHERE bot_instance_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (bot_id,),
        ).fetchone()
        last_candidate = conn.execute(
            """
            SELECT queue_id, symbol, action, final_status, final_reason,
                   execution_result, created_at
            FROM tradingview_signal_decisions
            WHERE bot_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (bot_id,),
        ).fetchone()
    return {
        "signals_used_today": int(signal_count or 0),
        "executions_used_today": int(exec_count or 0),
        "rejected_due_to_limits": int(limit_rejects or 0),
        "unprotected_positions_count": int(unprotected or 0),
        "stuck_claimed_rows_count": int(stuck or 0),
        "last_processor_result": dict(heartbeat) if heartbeat else None,
        "last_tradingview_candidate": dict(last_candidate) if last_candidate else None,
    }


@app.get("/api/admin/tradingview/limited-status")
async def tradingview_limited_status():
    bot_id = _active_bot_id_for_tradingview()
    fp = _phase6_runtime_fingerprint()
    counts = _phase6_status_counts(bot_id)
    return {
        "bot_id": bot_id,
        "live_limited_mode_enabled": fp["TRADINGVIEW_LIVE_MODE_LIMITED_ENABLED"],
        "external_signals_enabled": fp["TRADINGVIEW_EXTERNAL_SIGNALS_ENABLED"],
        "allowed_symbols": fp["TRADINGVIEW_ALLOWED_SYMBOLS"],
        "allowed_actions": fp["TRADINGVIEW_ALLOWED_ACTIONS"],
        "max_trade_cap": fp["TRADINGVIEW_MAX_TRADE_USDT_CAP"],
        "hourly_signal_limit": fp["TRADINGVIEW_MAX_SIGNALS_PER_HOUR"],
        "daily_signal_limit": fp["TRADINGVIEW_MAX_SIGNALS_PER_DAY"],
        "daily_execution_cap": fp["TRADINGVIEW_MAX_EXECUTIONS_PER_DAY"],
        "max_queue_per_cycle": fp["TRADINGVIEW_MAX_QUEUE_PER_CYCLE"],
        "require_sltp_protection": fp["TRADINGVIEW_REQUIRE_SLTP_PROTECTION"],
        "auto_disable_on_invariant_fail": fp["TRADINGVIEW_AUTO_DISABLE_ON_INVARIANT_FAIL"],
        "forbidden_capabilities": {
            "close": fp["TRADINGVIEW_ALLOW_CLOSE"],
            "reverse": fp["TRADINGVIEW_ALLOW_REVERSE"],
            "reduce": fp["TRADINGVIEW_ALLOW_REDUCE"],
            "cancel": fp["TRADINGVIEW_ALLOW_CANCEL"],
            "external_sltp": fp["TRADINGVIEW_ALLOW_EXTERNAL_SLTP"],
            "external_size": fp["TRADINGVIEW_ALLOW_EXTERNAL_SIZE"],
            "risk_override": fp["TRADINGVIEW_ALLOW_RISK_OVERRIDE"],
        },
        "safety_lockout": {
            "active": fp["active_safety_lockout"],
            "reason": fp["active_safety_lockout_reason"],
        },
        **counts,
    }


@app.get("/api/admin/tradingview/processor-status")
async def tradingview_processor_status():
    bot_id = _active_bot_id_for_tradingview()
    return {
        "bot_id": bot_id,
        "runtime_fingerprint": _phase6_runtime_fingerprint(),
        **_phase6_status_counts(bot_id),
    }


@app.get("/health")
async def health():
    trade_symbols = parse_symbols(settings.TRADE_SYMBOLS, settings.MAX_SYMBOLS)
    live_symbols = parse_symbols(settings.LIVE_SYMBOLS, settings.MAX_SYMBOLS)
    strong_trend_guard = evaluate_strong_trend_guard(settings)
    return {
        "status": "ok",
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "execution_mode": settings.EXECUTION_MODE,
        "binance_env": settings.BINANCE_ENV,
        "binance_base_url": settings.BINANCE_FAPI_BASE_URL,
        "default_interval": settings.DEFAULT_INTERVAL,
        "trade_symbols_count": len(trade_symbols),
        "trade_symbols": ",".join(trade_symbols[:20]),
        "live_symbols_count": len(live_symbols),
        "ml_enabled": bool(settings.ML_ENABLED),
        "iofs_gate_mode": settings.IOFS_GATE_MODE,
        "strong_trend_allowed_only_in_paper": strong_trend_guard.allowed_only_in_paper,
        "strong_trend_configured_unblocked": strong_trend_guard.configured_unblocked,
        "strong_trend_effective_unblocked": strong_trend_guard.effective_unblocked,
        "strong_trend_guard_reason": strong_trend_guard.reason,
        "max_live_trades_per_cycle": settings.MAX_LIVE_TRADES_PER_CYCLE,
        "risk": {
            "daily_max_loss_usdt": settings.DAILY_MAX_LOSS_USDT,
            "kill_switch_close_positions": settings.KILL_SWITCH_CLOSE_POSITIONS,
            "stop_loss_pct": settings.STOP_LOSS_PCT,
            "take_profit_pct": settings.TAKE_PROFIT_PCT,
        },
        "tradingview_runtime_fingerprint": _phase6_runtime_fingerprint(),
    }


def _settings_public_dict() -> Dict[str, Any]:
    data = settings.model_dump()
    # remove/mask secrets
    for k in list(data.keys()):
        if k in SENSITIVE_KEYS:
            data[k] = "***"
    return data


@app.get("/debug/config")
async def debug_config():
    return {"config": _settings_public_dict()}


def _sanity_checks() -> Dict[str, Any]:
    warnings: List[str] = []
    errors: List[str] = []

    # ---------- SYMBOL CHECKS ----------
    if len(settings.TRADE_SYMBOLS) > settings.MAX_SYMBOLS:
        warnings.append(
            f"TRADE_SYMBOLS count ({len(settings.TRADE_SYMBOLS)}) exceeds MAX_SYMBOLS ({settings.MAX_SYMBOLS})."
        )

    missing_live = set(settings.LIVE_SYMBOLS) - set(settings.TRADE_SYMBOLS)
    if missing_live:
        errors.append(f"LIVE_SYMBOLS not in TRADE_SYMBOLS: {sorted(missing_live)}")

    # ---------- SIZING CHECKS ----------
    if settings.TRADE_USDT_PER_ORDER < settings.MIN_NOTIONAL_USDT:
        warnings.append(
            f"TRADE_USDT_PER_ORDER ({settings.TRADE_USDT_PER_ORDER}) "
            f"is below MIN_NOTIONAL_USDT ({settings.MIN_NOTIONAL_USDT})."
        )

    # ---------- LEVERAGE CHECKS ----------
    for sym, lev in settings.SYMBOL_LEVERAGE_MAP.items():
        if sym not in settings.TRADE_SYMBOLS:
            warnings.append(f"Leverage defined for unused symbol: {sym}")
        if lev < settings.MIN_LEVERAGE:
            warnings.append(
                f"Leverage for {sym} ({lev}) is below MIN_LEVERAGE ({settings.MIN_LEVERAGE})"
            )

    # ---------- RISK CHECKS ----------
    if settings.DAILY_MAX_LOSS_USDT <= 0:
        warnings.append(
            "DAILY_MAX_LOSS_USDT is <= 0 (kill switch will trigger immediately)."
        )

    if settings.STOP_LOSS_PCT > 5:
        warnings.append(
            f"STOP_LOSS_PCT ({settings.STOP_LOSS_PCT}%) is high for leveraged trading."
        )

    # ---------- EXECUTION SAFETY ----------
    if settings.EXECUTION_MODE == "live" and settings.BINANCE_ENV == "mainnet":
        warnings.append("LIVE + MAINNET = REAL MONEY TRADING")

    # ---------- DB CHECK ----------
    try:
        from shared_lib.persistence.db import DB

        db = DB()  # defaults to data/bot.db
        with db.connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS cnt FROM symbol_state").fetchone()
            symbol_rows = int(row["cnt"]) if row else 0

    except Exception as e:
        errors.append(f"Database error: {str(e)}")
        symbol_rows = None

    return {
        "status": "ok" if not errors else "error",
        "errors": errors,
        "warnings": warnings,
        "db": {"symbol_state_rows": symbol_rows},
    }


@app.get("/debug/sanity")
async def debug_sanity():
    return _sanity_checks()



@app.get("/debug/run/current")
def debug_run_current():
    return {
        "run_id": CURRENT_RUN_ID,
        "running": runner_service.running,
        "mode": runner_service.mode,
    }


@app.post("/debug/run/cycle")
def debug_run_cycle(symbol: str = "ETHUSDT"):
    """
    Manually force a strategy cycle for ONE symbol.
    Useful for testing 'SMA Cross' logic without waiting for runner loop.
    """
    runner = get_runner()
    
    # ensure state exists
    if symbol not in runner.state:
        from app.runner.runner import SymbolState
        runner.state[symbol] = SymbolState()

    # 1. Update market data (klines)
    # The runner usually does this in step_symbol -> update_market_data.
    # We can call granular methods or just `step_symbol`.
    # `step_symbol` does EVERYTHING: candles, strategy, execution.
    
    result = runner.step_symbol(symbol)
    
    return {
        "symbol": symbol,
        "step_result": result,
        "state": runner.state[symbol].__dict__,
    }


@app.post("/debug/run/force")
def debug_run_force():
    """
    Force the runner to run ONE full cycle immediately (over all max_symbols).
    """
    runner = get_runner()
    return runner.run_once(max_symbols=runner_service.max_symbols)


@app.get("/debug/check-symbol")
def debug_check_symbol(symbol: str = "ETHUSDT"):
    """
    Inspects internal state for a symbol, including last signal, position, and recent klines info.
    """
    runner = get_runner()
    
    st = runner.state.get(symbol.upper())
    state_dict = st.__dict__ if st else None
    
    # fetch raw klines to see if data is flowing
    klines = []
    try:
        klines = runner.client.klines(symbol.upper(), interval=settings.DEFAULT_INTERVAL, limit=5)
    except Exception as e:
        klines = [str(e)]

    return {
        "symbol": symbol.upper(),
        "in_memory_state": state_dict,
        "klines_sample_5": klines,
        "strategy_config": runner.strategy.params_schema, 
    }

@app.get("/debug/strategy/check")
def debug_strategy_check(symbol: str = "ETHUSDT"):
    """
    Directly calls the strategy's get_signal method to see what it returns right now.
    """
    runner = get_runner()
    
    # Ensure client is synced (in case strategy needs it)
    if hasattr(runner.strategy, "client") and runner.strategy.client is None:
        runner.strategy.client = runner.client

    try:
        sig = runner.strategy.get_signal(symbol.upper())
        return {
            "symbol": symbol.upper(),
            "strategy": runner.strategy.name,
            "signal": sig.signal.value,
            "confidence": sig.confidence,
            "reason": sig.reason,
            "meta": sig.meta,
        }
    except Exception as e:
        return {
            "error": f"Strategy execution failed: {str(e)}",
            "traceback": traceback.format_exc()
        }

