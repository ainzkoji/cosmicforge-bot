import time
import uuid
import json
import traceback

import asyncio
from anyio import to_thread
from pathlib import Path
from fastapi import Query
from fastapi import Body
from typing import Any, Dict, List
import app.strategy
from fastapi import Depends
from uuid import uuid4


from fastapi import FastAPI
from app.core.config import settings
from app.exchange.binance.client import BinanceFuturesClient
from app.execution.executor import BinanceExecutor
from app.symbols.universe import parse_symbols, build_universe
from app.runner.runner import PaperRunner
from app.symbols.leverage import parse_leverage_map, leverage_for
from app.exchange.binance.filters import extract_filters, round_qty
from app.ops.run_manager import RunManager
from app.ops.context import set_run_id, clear_run_id
from app.ops.run_tracker import RunTracker
from app.persistence.db import DB
from app.ops.context import set_cycle_id, clear_cycle_id
from app.strategy.registry import list_strategies, get_strategy_spec


from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone
from app.risk.realized_pnl import realized_pnl_from_user_trades
from datetime import date
from app.execution.confirm import wait_until_flat
from app.persistence.migrations import migrate
from app.core.kyc_policy import check_kyc_gate, KYCAction
from app.api.auth import get_current_active_user
from fastapi import HTTPException


migrate()


app = FastAPI(title="CosmicForge Bot MVP")

# --- CORS Middleware ---
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register monitoring API router
from app.api.monitoring import router as monitoring_router
app.include_router(monitoring_router)  # Router already has prefix="/monitoring"

# Register Auth API router
from app.api.auth import router as auth_router
app.include_router(auth_router, prefix="/auth", tags=["Authentication"])

# Register Public API router
from app.api.public import router as public_router
app.include_router(public_router, tags=["Public"])

# Register KYC API router
from app.api.kyc import router as kyc_router
app.include_router(kyc_router, tags=["KYC"])

# Register Broker API router
from app.api.brokers import router as broker_router
app.include_router(broker_router, prefix="/api/brokers", tags=["Brokers"])

# Register Billing API router
from app.api.billing import router as billing_router
app.include_router(billing_router, prefix="/api/billing", tags=["Billing"])

# Register Onboarding API router
from app.api.onboarding import router as onboarding_router
app.include_router(onboarding_router, prefix="/api/onboarding", tags=["Onboarding"])

# Register Strategy API router
from app.api.strategies import router as strategies_router
app.include_router(strategies_router, prefix="/api/strategies", tags=["Strategies"])

# Register Analytics API router
from app.api.analytics import router as analytics_router
app.include_router(analytics_router, prefix="/api/analytics", tags=["Analytics"])

# Register Admin API router
from app.api.admin import router as admin_router
app.include_router(admin_router, prefix="/api", tags=["Admin"])

# Register Strategy Configs API router
from app.api.strategy_configs import router as strategy_configs_router
app.include_router(strategy_configs_router, prefix="/api/strategy-configs", tags=["Strategy Configs"])

# Register Risk Profiles API router
from app.api.risk_profiles import router as risk_profiles_router
app.include_router(risk_profiles_router, prefix="/api/risk-profiles", tags=["Risk Profiles"])

paper_runner_instance: PaperRunner | None = None
run_tracker = RunTracker(DB())
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
        warnings = settings.validate_runtime()
        for w in warnings:
            print(f"[CONFIG WARNING] {w}")
    except Exception as e:
        # Fail-closed: crash the service rather than running with a dangerous config
        print(str(e))
        raise


@app.on_event("startup")
async def _startup_run_manager():
    # create a run record in DB
    info = run_manager.start()
    print(f"[RUN] started run_id={info.run_id} mode={info.mode}")

    # ✅ AUTO-START RUNNER LOOP
    runner_service.running = True
    runner_service.task = asyncio.create_task(runner_loop())
    print("[RUNNER] Loop started automatically.")


@app.on_event("shutdown")
async def _shutdown_run_manager():
    # ✅ Signal loop to stop
    runner_service.running = False
    
    # ✅ Signal synchronous runner to break loop
    if paper_runner_instance:
        paper_runner_instance._stop_requested = True
    
    # ✅ Cancel task immediately (don't wait for sleep to finish)
    if runner_service.task:
        runner_service.task.cancel()
        try:
            await runner_service.task
        except asyncio.CancelledError:
            pass
            
    # stop the most recent running run (safe even after reload)
    current = run_manager.get_current()
    if current and current.get("run"):
        run_id = current["run"]["run_id"]
        run_manager.stop(run_id, status="STOPPED")
        print(f"[RUN] stopped run_id={run_id}")


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


runner_service = RunnerServiceState()


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


async def runner_loop():
    """
    Continuous loop that calls runner.run_once() every interval_seconds.
    FAIL-CLOSED: on unhandled exception -> set kill=True, persist, log FATAL, stop runner.
    """
    try:
        # ✅ Initialize runner in thread to keep startup non-blocking
        runner = await asyncio.to_thread(get_runner)
    
        while runner_service.running:
            # ✅ generate cycle id ONCE per loop
            cycle_id = str(uuid.uuid4())
            set_cycle_id(cycle_id)
    
            try:
                runner_service.last_cycle_at = _utc_now_iso()
    
                # 🔴 CYCLE START EVENT (context provides run_id + cycle_id)
                runner.audit.event(
                    event_type="CYCLE",
                    action="CYCLE_START",
                    details={},
                )
    
                # ---- EXISTING LOGIC (DO NOT MOVE) ----
                # ✅ Run in thread to keep API responsive during cycle
                await asyncio.to_thread(runner.run_once, runner_service.max_symbols)
                runner_service.cycle_count += 1
                runner_service.last_error = None
    
                # 🔴 CYCLE END EVENT
                runner.audit.event(
                    event_type="CYCLE",
                    action="CYCLE_END",
                    details={},
                )
    
            except Exception:
                err = traceback.format_exc()
                runner_service.last_error = err
    
                # ✅ FAIL-CLOSED (existing behavior)
                try:
                    runner.daily.kill = True
                    runner.store.save_daily(
                        runner.daily.day,
                        runner.daily.realized_pnl,
                        runner.daily.kill,
                    )
                except Exception:
                    pass
    
                try:
                    runner.audit.event(
                        event_type="FATAL",
                        action="RUNNER_HALTED",
                        details={"error": err},
                    )
                except Exception:
                    pass
    
                runner_service.running = False
                break
    
            finally:
                # ✅ ALWAYS clear cycle context
                clear_cycle_id()
    
            # ✅ REPLACED: Chunked sleep for instant shutdown response
            # Instead of sleeping for full interval, check every 0.5s if we should stop
            sleep_ms = runner_service.interval_seconds * 1000
            step = 500  # 0.5s check
            while sleep_ms > 0 and runner_service.running:
                await asyncio.sleep(step / 1000.0)
                sleep_ms -= step
            
    except Exception as e:
        # Catch errors outside the loop (like get_runner failure)
        err = traceback.format_exc()
        runner_service.last_error = f"CRITICAL LOOP CRASH: {err}"
        runner_service.running = False
        print(f"[{datetime.now()}] CRITICAL RUNNER CRASH: {err}")


@app.get("/")
def root():
    return {
        "status": "ok",
        "exchange": "binance-futures-testnet",
        "api_key_loaded": bool(settings.BINANCE_API_KEY),
        "api_secret_loaded": bool(settings.BINANCE_API_SECRET),
    }


@app.get("/binance/ping")
def binance_ping():
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.ping()


@app.get("/binance/balance")
def binance_balance():
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.account_balance()


@app.get("/binance/price")
def binance_price(symbol: str = "BTCUSDT"):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.mark_price(symbol)


@app.get("/binance/klines")
def binance_klines(symbol: str = "BTCUSDT", interval: str = "1m", limit: int = 50):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.klines(symbol, interval, limit)


@app.get("/config/symbols")
def config_symbols():
    symbols = [
        s.strip().upper() for s in settings.TRADE_SYMBOLS.split(",") if s.strip()
    ]
    return {"symbols": symbols, "interval": settings.DEFAULT_INTERVAL}


@app.get("/symbols/universe")
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


@app.get("/binance/prices")
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


@app.get("/runner/paper/once")
def paper_run_once(max_symbols: int = 10):
    runner = get_runner()
    return runner.run_once(max_symbols=max_symbols)


@app.get("/runner/paper/state")
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


@app.post("/binance/leverage")
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


@app.get("/binance/qty")
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
    Places a MARKET order on Binance Futures TESTNET using USDT sizing.
    - Enforces leverage minimum and per-symbol leverage settings
    - Rounds quantity using stepSize
    """
    side = side.upper()
    if side not in {"BUY", "SELL"}:
        return {"error": "side must be BUY or SELL"}

    # Respect kill switch (global safety)
    runner = get_runner()

    symbol = symbol.upper()

    # ✅ ADD: prevent collision with runner loop for same symbol
    with runner.symbol_guard(symbol, timeout_s=2.0) as ok:
        if not ok:
            return {
                "status": "rejected",
                "reason": "symbol_lock_busy",
                "symbol": symbol,
            }

        # ✅ ADD: use the same RiskGate policy as runner/executor
        # Build risk state for gate check
        from datetime import date as dt_date
        risk_state = runner.store.load_risk_state(dt_date.today())
        risk_state.current_equity = runner.get_account_balance()
        risk_state.open_positions = sum(1 for s in runner.state.values() if s.position in ("LONG", "SHORT"))
        
        decision = runner.risk_gate.can_open(risk_state, symbol)
        if not decision.allowed:
            # ✅ audit log for manual blocked attempts
            try:
                runner.audit.event(
                    event_type="RISK_BLOCK",
                    run_id=getattr(runner, "run_id", None),
                    symbol=symbol.upper(),
                    action="MANUAL_OPEN_BLOCKED",
                    details={
                        "reason": decision.reason,
                        "code": decision.reason_code,
                        "severity": decision.severity,
                        "endpoint": "/trade/market",
                        "side": side.upper(),
                        "usdt": usdt,
                    },
                )
            except Exception:
                pass

            return {
                "error": "RISK_BLOCK",
                "reason_code": decision.reason_code,
                "reason": decision.reason,
                "severity": decision.severity,
            }

        # (Your existing kill-switch check stays — now basically redundant but harmless)
        if runner.daily.kill:
            return {
                "error": "kill_switch_active",
                "message": "Daily max loss reached. Open trades blocked.",
            }

        client = BinanceFuturesClient(
            api_key=settings.BINANCE_API_KEY,
            api_secret=settings.BINANCE_API_SECRET,
            base_url=settings.BINANCE_FAPI_BASE_URL,
            recv_window=settings.BINANCE_RECV_WINDOW,
        )

        # --- leverage ---
        lev_map = parse_leverage_map(settings.SYMBOL_LEVERAGE_MAP)
        lev = leverage_for(
            symbol, lev_map, settings.DEFAULT_LEVERAGE, settings.MIN_LEVERAGE
        )
        lev_result = client.set_leverage(symbol, lev)

        # --- qty calc ---
        price = client.last_price(symbol)
        exch = client.exchange_info_cached()
        flt = extract_filters(exch, symbol)

        raw_qty = usdt / price
        qty = round_qty(raw_qty, flt.step_size)

        if not (qty >= flt.min_qty and qty > 0):
            return {
                "error": "USDT amount too small for symbol filters",
                "symbol": symbol,
                "price": price,
                "usdt": usdt,
                "raw_qty": raw_qty,
                "qty_rounded": str(qty),
                "min_qty": str(flt.min_qty),
                "step_size": str(flt.step_size),
                "min_usdt_required": float(flt.min_qty) * price,
            }

        # --- place order ---
        order = client.place_market_order(symbol=symbol, side=side, quantity=float(qty))

        return {
            "symbol": symbol,
            "side": side,
            "usdt": usdt,
            "price_used": price,
            "qty": str(qty),
            "leverage": lev,
            "leverage_result": lev_result,
            "order": order,
        }


@app.post("/runner/live/once", dependencies=[Depends(require_kyc_trading)])
def runner_live_once(max_symbols: int = 10):
    runner = get_runner()
    return runner.run_once(max_symbols=max_symbols)


@app.get("/binance/order")
def binance_order(symbol: str, order_id: int):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.get_order(symbol, order_id)


@app.get("/binance/open-orders")
def binance_open_orders(symbol: str = "XRPUSDT"):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.open_orders(symbol)


@app.get("/binance/position")
def binance_position(symbol: str = "XRPUSDT"):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.position_risk(symbol)


@app.post("/trade/close")
def trade_close(symbol: str = "XRPUSDT"):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )

    try:
        return client.close_position_market(symbol)
    except Exception as e:
        return {
            "error": "CLOSE_FAILED",
            "symbol": symbol,
            "detail": str(e),
        }


from app.core.config import settings


@app.get("/debug/settings")
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


@app.post("/binance/cancel-all")
def binance_cancel_all(symbol: str = "XRPUSDT"):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    return client.cancel_all_orders(symbol)


@app.get("/trade/protection")
def trade_protection(symbol: str = "XRPUSDT"):
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL,
        recv_window=settings.BINANCE_RECV_WINDOW,
    )
    ex = BinanceExecutor(client)
    return ex.ensure_protection(symbol)


@app.get("/risk/daily")
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


@app.post("/risk/reset")
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


@app.post("/risk/circuit/reset")
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


@app.get("/risk/circuit/status")
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


@app.get("/risk/status")
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


@app.post("/trade/close-record")
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
            from app.persistence.db import DB

            db = DB()

        open_meta = None
        with db.connect() as conn:
            last_close = conn.execute(
                "SELECT id FROM trade_fills WHERE symbol=? AND action='CLOSE' ORDER BY id DESC LIMIT 1",
                (symbol,),
            ).fetchone()
            last_close_id = int(last_close["id"]) if last_close else 0

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
        from app.persistence.trade_fills import record_fill
        from app.persistence.db import utc_now_iso

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
        )

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
def emergency_flatten():
    """
    Cancel all open orders and close all positions for LIVE_SYMBOLS.
    """
    runner = get_runner()
    client = runner.client

    symbols = list(settings.LIVE_SYMBOLS)
    results = []

    for sym in symbols:
        sym = sym.upper()
        with runner.symbol_guard(sym, timeout_s=1.0) as ok:
            if not ok:
                results.append({"symbol": sym, "status": "skipped_lock_busy"})
                continue

            r = {"symbol": sym}
            try:
                client.cancel_all_orders(sym)
                r["cancel_all"] = "ok"
            except Exception as e:
                r["cancel_all"] = f"error: {type(e).__name__}: {e}"

            try:
                close = client.close_position_market(sym)
                r["close"] = close
            except Exception as e:
                r["close"] = f"error: {type(e).__name__}: {e}"

            results.append(r)

    try:
        runner.audit.event(
            event_type="EMERGENCY",
            run_id=getattr(runner, "run_id", None),
            symbol=None,
            action="FLATTEN_ALL",
            details={"count": len(results)},
        )
    except Exception:
        pass

    return {"ok": True, "results": results}


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


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "execution_mode": settings.EXECUTION_MODE,
        "binance_env": settings.BINANCE_ENV,
        "binance_base_url": settings.BINANCE_FAPI_BASE_URL,
        "default_interval": settings.DEFAULT_INTERVAL,
        "trade_symbols_count": len(settings.TRADE_SYMBOLS),
        "trade_symbols": settings.TRADE_SYMBOLS[:20],  # avoid huge output
        "live_symbols_count": len(settings.LIVE_SYMBOLS),
        "max_live_trades_per_cycle": settings.MAX_LIVE_TRADES_PER_CYCLE,
        "risk": {
            "daily_max_loss_usdt": settings.DAILY_MAX_LOSS_USDT,
            "kill_switch_close_positions": settings.KILL_SWITCH_CLOSE_POSITIONS,
            "stop_loss_pct": settings.STOP_LOSS_PCT,
            "take_profit_pct": settings.TAKE_PROFIT_PCT,
        },
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
        from app.persistence.db import DB

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
async def debug_run_current():
    data = run_manager.get_current()
    if not data:
        return {"status": "no_running_run"}
    return {"status": "ok", **data}


@app.get("/debug/run/last")
async def debug_run_last():
    data = run_manager.get_last()
    if not data:
        return {"status": "no_runs"}
    return {"status": "ok", **data}


@app.on_event("startup")
async def _startup_run_manager():
    info = run_manager.start()
    set_run_id(info.run_id)
    print(f"[RUN] started run_id={info.run_id}")


@app.on_event("shutdown")
async def _shutdown_run_manager():
    current = run_manager.get_current()
    if current and current.get("run"):
        run_id = current["run"]["run_id"]
        run_manager.stop(run_id, status="STOPPED")
    clear_run_id()


@app.get("/debug/db/events/latest")
async def debug_db_events_latest(limit: int = 20):
    from app.persistence.db import DB

    db = DB()
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT timestamp_utc, run_id, cycle_id, symbol, event_type, action
            FROM events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return {"rows": [dict(r) for r in rows]}


def _interval_to_seconds(s: str) -> int:
    s = (s or "1m").lower().strip()
    if s.endswith("m"):
        return int(s[:-1]) * 60
    if s.endswith("h"):
        return int(s[:-1]) * 60
    if s.endswith("s"):
        return int(s[:-1])
    return 60


@app.on_event("startup")
async def _startup_run():
    global CURRENT_RUN_ID
    CURRENT_RUN_ID = run_tracker.start_run(
        mode=settings.EXECUTION_MODE,
        interval_seconds=_interval_to_seconds(settings.DEFAULT_INTERVAL),
        max_symbols=settings.MAX_SYMBOLS,
    )
    set_run_id(CURRENT_RUN_ID)


@app.on_event("shutdown")
async def _shutdown_run():
    if CURRENT_RUN_ID:
        run_tracker.stop_run(CURRENT_RUN_ID)
    clear_run_id()


@app.get("/debug/run/summary/current")
async def run_summary_current():
    if not CURRENT_RUN_ID:
        return {"status": "no_run"}
    summary = run_tracker.refresh_summary(CURRENT_RUN_ID)
    wins = summary.get("win_trades") or 0
    losses = summary.get("loss_trades") or 0
    total_closed = wins + losses
    win_rate = (wins / total_closed) if total_closed else None
    return {
        "status": "ok",
        "run_id": CURRENT_RUN_ID,
        "summary": summary,
        "win_rate": win_rate,
    }


@app.get("/debug/run/summary/last")
async def run_summary_last():
    from app.persistence.db import DB

    db = DB()
    with db.connect() as conn:
        run = conn.execute(
            "SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
    if not run:
        return {"status": "no_runs"}
    run_id = run["run_id"]
    summary = run_tracker.refresh_summary(run_id)
    wins = summary.get("win_trades") or 0
    losses = summary.get("loss_trades") or 0
    total_closed = wins + losses
    win_rate = (wins / total_closed) if total_closed else None
    return {"status": "ok", "run_id": run_id, "summary": summary, "win_rate": win_rate}


@app.get("/debug/db/trade_fills")
async def debug_trade_fills(limit: int = 50):
    from app.persistence.db import DB

    db = DB()
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT * FROM trade_fills ORDER BY id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    return {"rows": [dict(r) for r in rows]}


@app.post("/risk/daily/reset")
async def risk_daily_reset():
    runner = get_runner()
    runner.daily.kill = False
    runner.daily.realized_pnl = 0.0
    runner.store.save_daily(
        runner.daily.day, runner.daily.realized_pnl, runner.daily.kill
    )

    runner.audit.event(
        event_type="RISK",
        symbol=None,
        action="DAILY_RESET",
        details={
            "day": runner.daily.day,
            "realized_pnl": runner.daily.realized_pnl,
            "kill": runner.daily.kill,
        },
    )
    return {
        "status": "ok",
        "day": runner.daily.day,
        "kill": runner.daily.kill,
        "realized_pnl": runner.daily.realized_pnl,
    }


@app.get("/debug/routes")
def debug_routes():
    routes = []
    for r in app.routes:
        methods = sorted(list(getattr(r, "methods", []) or []))
        path = getattr(r, "path", None)
        name = getattr(r, "name", None)
        if path:
            routes.append({"methods": methods, "path": path, "name": name})
    # Sort for readability
    routes.sort(key=lambda x: x["path"])
    return {"count": len(routes), "routes": routes}


@app.get("/metrics/strategy")
def metrics_strategy(strategy: str | None = None, symbol: str | None = None):
    db = DB()
    q = "SELECT * FROM strategy_performance WHERE 1=1"
    params = []
    if strategy:
        q += " AND strategy=?"
        params.append(strategy)
    if symbol:
        q += " AND symbol=?"
        params.append(symbol)
    q += " ORDER BY updated_at DESC LIMIT 200"
    with db.connect() as conn:
        rows = [dict(r) for r in conn.execute(q, params).fetchall()]
    return {"rows": rows}


@app.get("/metrics/confidence")
def metrics_confidence(strategy: str | None = None, symbol: str | None = None):
    db = DB()
    q = "SELECT confidence, outcome FROM signal_outcomes WHERE 1=1"
    params = []
    if strategy:
        q += " AND strategy=?"
        params.append(strategy)
    if symbol:
        q += " AND symbol=?"
        params.append(symbol)

    with db.connect() as conn:
        rows = conn.execute(q, params).fetchall()

    buckets = {}
    for r in rows:
        c = float(r["confidence"])
        o = int(r["outcome"])
        b = min(9, max(0, int(c * 10)))
        key = f"{b/10:.1f}-{(b+1)/10:.1f}"
        if key not in buckets:
            buckets[key] = {"trades": 0, "wins": 0}
        buckets[key]["trades"] += 1
        buckets[key]["wins"] += o

    out = []
    for k in sorted(buckets.keys()):
        t = buckets[k]["trades"]
        w = buckets[k]["wins"]
        out.append({"bucket": k, "trades": t, "win_rate": (w / t) if t else 0.0})

    return {"buckets": out, "samples": len(rows)}


@app.get("/strategies")
def strategies_list():
    return {
        "strategies": [
            {
                "name": s.name,
                "version": s.version,
                "supports_asset_classes": s.supports_asset_classes,
                "description": s.description,
                "params_schema": s.params_schema,
            }
            for s in list_strategies()
        ]
    }


@app.get("/strategies/{name}")
def strategy_detail(name: str):
    spec = get_strategy_spec(name)
    if not spec:
        return {"error": "strategy_not_found", "name": name}
    return {
        "name": spec.name,
        "version": spec.version,
        "supports_asset_classes": spec.supports_asset_classes,
        "description": spec.description,
        "params_schema": spec.params_schema,
    }


@app.get("/debug/db_counts")
def debug_db_counts():
    db = DB()
    with db.connect() as conn:

        def count(table: str) -> int:
            try:
                return int(
                    conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
                )
            except Exception:
                return -1

        return {
            "trade_fills": count("trade_fills"),
            "strategy_performance": count("strategy_performance"),
            "signal_outcomes": count("signal_outcomes"),
        }


@app.get("/debug/recent_fills")
def debug_recent_fills(limit: int = 20):
    db = DB()
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT * FROM trade_fills ORDER BY timestamp_utc DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
        return {"rows": [dict(r) for r in rows]}


@app.get("/debug/table_info/trade_fills")
def debug_table_info_trade_fills():
    db = DB()
    with db.connect() as conn:
        rows = conn.execute("PRAGMA table_info(trade_fills)").fetchall()
        return {"columns": [dict(r) for r in rows]}


@app.get("/debug/recent_closes")
def debug_recent_closes(limit: int = 50):
    db = DB()
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT * FROM trade_fills WHERE action='CLOSE' ORDER BY timestamp_utc DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
        return {"rows": [dict(r) for r in rows]}


# ============================================================================
# STRATEGY ROUTER ENDPOINTS (B+ Strategy System)
# ============================================================================

@app.get("/strategy/market-context")
def strategy_market_context(
    symbol: str = "BTCUSDT",
    htf: str = "4h",
    mtf: str = "1h",
):
    """
    Get current market context including regime, mode, and allowed strategies.
    """
    from app.strategy.regime import RegimeClassifier
    from app.strategy.mode import ModeRouter
    from app.strategy.timeframe import TimeframeAnalyzer
    from app.strategy.router import StrategyRouter
    
    runner = get_runner()
    
    # Build minimal strategy dict for router
    strategies = {}
    try:
        from app.strategy.supertrend import SuperTrendStrategy
        strategies["supertrend"] = SuperTrendStrategy(runner.client, interval="15m")
    except Exception:
        pass
    try:
        from app.strategy.bollinger_reversion import BollingerReversionStrategy
        strategies["bollinger_reversion"] = BollingerReversionStrategy(runner.client, interval="15m")
    except Exception:
        pass
    try:
        from app.strategy.squeeze_breakout import SqueezeBreakoutStrategy
        strategies["squeeze_breakout"] = SqueezeBreakoutStrategy(runner.client, interval="15m")
    except Exception:
        pass
    try:
        from app.strategy.robust_ensemble import RobustEnsembleStrategy
        strategies["robust_ensemble"] = RobustEnsembleStrategy(runner.client, interval="15m")
    except Exception:
        pass
    
    router = StrategyRouter(
        client=runner.client,
        strategies=strategies,
    )
    
    return router.get_market_context(symbol, htf, mtf)


@app.get("/strategy/routed-signal")
def strategy_routed_signal(
    symbol: str = "BTCUSDT",
    strategy: str | None = None,
    htf: str = "4h",
    mtf: str = "1h",
):
    """
    Get a routed signal for a symbol using the strategy router.
    Uses multi-timeframe analysis and regime-based strategy selection.
    """
    from app.strategy.router import StrategyRouter
    
    runner = get_runner()
    
    # Build strategy dict with all available strategies
    strategies = {}
    
    # Precision Mode strategies
    try:
        from app.strategy.supertrend import SuperTrendStrategy
        strategies["supertrend"] = SuperTrendStrategy(runner.client, interval="15m")
    except Exception:
        pass
    try:
        from app.strategy.trend_pullback import TrendPullbackStrategy
        strategies["trend_pullback"] = TrendPullbackStrategy(runner.client, interval="15m")
    except Exception:
        pass
    try:
        from app.strategy.donchian_breakout import DonchianBreakoutStrategy
        strategies["donchian_breakout"] = DonchianBreakoutStrategy(runner.client, interval="15m")
    except Exception:
        pass
    
    # Flow Mode strategies
    try:
        from app.strategy.bollinger_reversion import BollingerReversionStrategy
        strategies["bollinger_reversion"] = BollingerReversionStrategy(runner.client, interval="5m")
    except Exception:
        pass
    try:
        from app.strategy.vwap_reversion import VWAPReversionStrategy
        strategies["vwap_reversion"] = VWAPReversionStrategy(runner.client, interval="5m")
    except Exception:
        pass
    try:
        from app.strategy.squeeze_breakout import SqueezeBreakoutStrategy
        strategies["squeeze_breakout"] = SqueezeBreakoutStrategy(runner.client, interval="15m")
    except Exception:
        pass
    
    # Legacy
    try:
        from app.strategy.robust_ensemble import RobustEnsembleStrategy
        strategies["robust_ensemble"] = RobustEnsembleStrategy(runner.client, interval="15m")
    except Exception:
        pass
    
    router = StrategyRouter(
        client=runner.client,
        strategies=strategies,
    )
    
    result = router.route_signal(symbol, strategy_name=strategy, htf=htf, mtf=mtf)
    
    return {
        "symbol": symbol,
        "signal": result.signal.value,
        "raw_confidence": result.raw_confidence,
        "final_confidence": result.final_confidence,
        "strategy_name": result.strategy_name,
        "strategy_type": result.strategy_type,
        "regime": result.regime.value,
        "trend_direction": result.trend_dir.value,
        "mode": result.mode.value,
        "htf_bias": result.htf_bias,
        "htf_aligned": result.htf_aligned,
        "mtf_aligned": result.mtf_aligned,
        "calibration_passed": result.calibration_passed,
        "calibration_reason": result.calibration_reason,
        "is_actionable": result.is_actionable,
        "reason": result.reason,
        "conflicts": result.conflicts,
        "meta": result.meta,
    }


@app.get("/strategy/regime")
def strategy_regime(symbol: str = "BTCUSDT", timeframe: str = "4h"):
    """
    Get market regime classification for a symbol.
    Now includes: compression_ratio, breakout_pressure, chop detection.
    """
    from app.strategy.regime import RegimeClassifier
    
    runner = get_runner()
    
    klines = runner.client.klines(symbol=symbol, interval=timeframe, limit=200)
    
    if not klines or len(klines) < 100:
        return {"error": "insufficient_data", "need": 100, "have": len(klines) if klines else 0}
    
    highs = [float(k[2]) for k in klines]
    lows = [float(k[3]) for k in klines]
    closes = [float(k[4]) for k in klines]
    
    classifier = RegimeClassifier()
    result = classifier.classify(highs, lows, closes)
    
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "regime": result.regime.value,
        "trend_direction": result.trend_dir.value,
        "regime_confidence": result.regime_confidence,
        "adx": result.adx,
        "atr_percent": result.atr_percent,
        "ma_slope": result.ma_slope,
        "compression_ratio": result.compression_ratio,
        "breakout_pressure": result.breakout_pressure,
        "details": result.details,
    }


@app.get("/strategy/calibration")
def strategy_calibration(
    strategy: str = "supertrend",
    symbol: str = "BTCUSDT",
    timeframe: str = "15m",
):
    """
    Get confidence calibration report for a strategy/symbol/timeframe.
    Shows win rate, profit factor per confidence bucket.
    """
    from app.metrics.calibration import ConfidenceCalibrator
    
    calibrator = ConfidenceCalibrator()
    report = calibrator.get_calibration_report(strategy, symbol, timeframe)
    
    return {
        "strategy": report.strategy,
        "symbol": report.symbol,
        "timeframe": report.timeframe,
        "total_trades": report.total_trades,
        "overall_win_rate": report.overall_win_rate,
        "overall_profit_factor": report.overall_profit_factor,
        "is_calibrated": report.is_calibrated,
        "min_tradeable_confidence": report.min_tradeable_confidence,
        "buckets": [
            {
                "range": b.bucket_label,
                "trades": b.trade_count,
                "win_rate": b.win_rate,
                "profit_factor": b.profit_factor,
                "avg_pnl": b.avg_pnl,
                "avg_r": b.avg_r,
                "expectancy": b.expectancy,
            }
            for b in report.buckets
        ],
    }


@app.get("/strategy/all-signals")
def strategy_all_signals(symbol: str = "BTCUSDT"):
    """
    Test all strategies against a symbol and return their signals.
    Useful for debugging which strategies would fire.
    """
    runner = get_runner()
    results = []
    
    # Import all strategies
    strategy_classes = [
        ("supertrend", "app.strategy.supertrend", "SuperTrendStrategy"),
        ("trend_pullback", "app.strategy.trend_pullback", "TrendPullbackStrategy"),
        ("donchian_breakout", "app.strategy.donchian_breakout", "DonchianBreakoutStrategy"),
        ("bollinger_reversion", "app.strategy.bollinger_reversion", "BollingerReversionStrategy"),
        ("vwap_reversion", "app.strategy.vwap_reversion", "VWAPReversionStrategy"),
        ("squeeze_breakout", "app.strategy.squeeze_breakout", "SqueezeBreakoutStrategy"),
        ("robust_ensemble", "app.strategy.robust_ensemble", "RobustEnsembleStrategy"),
    ]
    
    for name, module_path, class_name in strategy_classes:
        try:
            import importlib
            module = importlib.import_module(module_path)
            cls = getattr(module, class_name)
            strategy = cls(runner.client, interval="15m")
            result = strategy.get_signal(symbol)
            results.append({
                "strategy": name,
                "signal": result.signal.value,
                "confidence": result.confidence,
                "reason": result.reason,
                "meta": result.meta,
            })
        except Exception as e:
            results.append({
                "strategy": name,
                "error": str(e),
            })
    
    return {"symbol": symbol, "signals": results}


# ============================================================================
# A+ EXECUTION ENDPOINTS
# ============================================================================

@app.get("/execution/tp-sl")
def execution_tp_sl(
    symbol: str = "BTCUSDT",
    side: str = "LONG",
    entry_price: float | None = None,
    mode: str = "PRECISION",
):
    """
    Calculate TP/SL levels using 4-layer stop-loss system.
    Layer 1: ATR + Structure hybrid
    Layer 2: Validation (reject bad setups)
    """
    from app.execution.tp_sl import StopLossCalculator, calculate_atr
    
    runner = get_runner()
    
    # Get current price if not provided
    if entry_price is None:
        entry_price = runner.client.last_price(symbol)
    
    # Get OHLC data
    klines = runner.client.klines(symbol=symbol, interval="15m", limit=50)
    if not klines or len(klines) < 25:
        return {"error": "insufficient_data", "need": 25, "have": len(klines) if klines else 0}
    
    highs = [float(k[2]) for k in klines]
    lows = [float(k[3]) for k in klines]
    closes = [float(k[4]) for k in klines]
    
    atr = calculate_atr(highs, lows, closes, 14)
    
    # Get mode-specific calculator
    calc = StopLossCalculator()
    if mode == "FLOW":
        calc.config = calc.get_flow_config()
    else:
        calc.config = calc.get_precision_config()
    
    # Calculate with full 4-layer validation
    result = calc.calculate(
        side=side,
        entry_price=entry_price,
        highs=highs,
        lows=lows,
        closes=closes,
        is_mean_reversion=(mode == "FLOW"),
    )
    
    return {
        "symbol": symbol,
        "side": side,
        "entry_price": entry_price,
        "mode": mode,
        "atr": atr,
        "atr_percent": (atr / entry_price) * 100 if entry_price > 0 else 0,
        # Validation
        "valid": result.valid,
        "reject_reason": result.reject_reason.value if result.reject_reason else "NONE",
        "reject_detail": result.reject_detail,
        # Stop loss
        "stop_price": result.stop_price,
        "stop_distance": result.stop_distance,
        "stop_type": result.stop_type.value if result.stop_type else "NONE",
        "atr_stop_candidate": result.atr_stop_candidate,
        "structure_stop_candidate": result.structure_stop_candidate,
        "swing_price": result.swing_price,
        # Take profits
        "tp1_price": result.tp1_price,
        "tp1_r": result.risk_reward_tp1,
        "tp2_price": result.tp2_price,
        "tp2_r": result.risk_reward_tp2,
        "r_value": result.r_value,
        # Fee analysis
        "estimated_fees": result.estimated_fees,
        "fee_adjusted_tp1": result.fee_adjusted_tp1,
        "min_edge_met": result.min_edge_met,
    }


@app.get("/execution/position-state")
def execution_position_state(symbol: str = "BTCUSDT"):
    """
    Get current position state from position manager (if any).
    Shows phase, TP/SL levels, break-even status, trailing.
    """
    from app.execution.position_manager import PositionManager
    
    # Note: In production, this would use the runner's position manager
    # For now, return the expected state structure
    pm = PositionManager()
    
    return pm.get_state_summary(symbol)


@app.get("/execution/flip-state")
def execution_flip_state(symbol: str = "BTCUSDT"):
    """
    Get current flip control state for a symbol.
    Shows hold time, reset status, confirmation progress.
    """
    from app.execution.anti_flip import FlipController
    
    fc = FlipController()
    return fc.get_state(symbol)


@app.post("/execution/test-flip")
def execution_test_flip(
    symbol: str = "BTCUSDT",
    signal: str = "BUY",
    confidence: float = 0.75,
    in_position: bool = False,
    position_side: str = "LONG",
):
    """
    Test flip controller logic with given parameters.
    """
    from app.execution.anti_flip import FlipController, FlipAction
    from datetime import datetime, timedelta
    
    fc = FlipController()
    
    # Setup state if in position
    if in_position:
        fc.on_position_opened(symbol, position_side)
        # Simulate some time passed
        state = fc._get_state(symbol)
        state.position_opened_at = datetime.utcnow() - timedelta(seconds=700)  # Past min hold
    
    # Evaluate signal
    decision = fc.evaluate_signal(symbol, signal, confidence)
    
    return {
        "symbol": symbol,
        "signal": signal,
        "confidence": confidence,
        "in_position": in_position,
        "position_side": position_side if in_position else None,
        "action": decision.action.value,
        "reason": decision.reason,
        "can_close": decision.can_close,
        "can_open": decision.can_open,
        "wait_seconds": decision.wait_seconds,
        "confirm_progress": decision.confirm_progress,
    }


@app.get("/execution/add-state")
def execution_add_state(symbol: str = "BTCUSDT"):
    """
    Get current add state for a symbol.
    Shows eligibility, add count, phase.
    """
    from app.execution.add_manager import AddManager
    
    am = AddManager()
    return am.get_state(symbol)


@app.post("/execution/test-add")
def execution_test_add(
    symbol: str = "BTCUSDT",
    signal_confidence: float = 0.85,
    current_price: float = 91500,
    entry_price: float = 91000,
    current_stop: float = 91050,  # BE stop
    atr: float = 275,
    mode: str = "PRECISION",
    regime: str = "STRONG_TREND",
    unrealized_pnl: float = 100,
    is_be_active: bool = True,
):
    """
    Test add eligibility with given parameters.
    Simulates TP1 already hit.
    """
    from app.execution.add_manager import AddManager
    
    am = AddManager()
    
    # Simulate position opened and TP1 hit
    am.on_position_opened(symbol, entry_confidence=0.78, size=0.1)
    am.on_tp1_hit(symbol)
    
    decision = am.can_add(
        symbol=symbol,
        signal_confidence=signal_confidence,
        current_price=current_price,
        entry_price=entry_price,
        current_stop=current_stop,
        atr=atr,
        mode=mode,
        regime=regime,
        unrealized_pnl=unrealized_pnl,
        is_be_active=is_be_active,
    )
    
    return {
        "symbol": symbol,
        "allowed": decision.allowed,
        "rejection": decision.rejection.value if decision.rejection else "NONE",
        "reason": decision.reason,
        "recommended_size": decision.recommended_size,
        "recommended_stop": decision.recommended_stop,
        "recommended_tp": decision.recommended_tp,
        "config": {
            "min_confidence": am.config.min_confidence_precision if mode == "PRECISION" else am.config.min_confidence_flow,
            "max_adds": am.config.max_adds_per_position,
            "add_size_fraction": am.config.add_size_fraction if mode == "PRECISION" else am.config.add_size_fraction_flow,
        }
    }


# ============================================================================
# D) PERSISTENCE + AUDIT ENDPOINTS
# ============================================================================

@app.get("/persistence/runs")
def persistence_list_runs(limit: int = 20):
    """List recent runs with status and summary."""
    from app.persistence.run_manager import get_run_manager
    return {"runs": get_run_manager().list_runs(limit)}


@app.get("/persistence/run/{run_id}")
def persistence_get_run(run_id: str):
    """Get full run report with summary and breakdowns."""
    from app.persistence.exports import get_run_report
    return get_run_report(run_id)


@app.get("/persistence/trades/{run_id}")
def persistence_get_trades(run_id: str):
    """Get all trades for a run."""
    from app.persistence.trade_tracker import get_trade_tracker
    return {"trades": get_trade_tracker().get_trades_by_run(run_id)}


@app.get("/persistence/events/{run_id}")
def persistence_get_events(run_id: str, limit: int = 100):
    """Get events for a run."""
    from app.persistence.events import get_event_store
    return {"events": get_event_store().get_by_run(run_id, limit)}


@app.get("/persistence/export/trades/{run_id}")
def persistence_export_trades(run_id: str):
    """Export trades as CSV."""
    from fastapi.responses import PlainTextResponse
    from app.persistence.exports import export_trades_csv
    csv_content = export_trades_csv(run_id)
    return PlainTextResponse(csv_content, media_type="text/csv")


@app.get("/persistence/export/strategy/{run_id}")
def persistence_export_strategy(run_id: str):
    """Export strategy performance as CSV."""
    from fastapi.responses import PlainTextResponse
    from app.persistence.exports import export_strategy_performance_csv
    csv_content = export_strategy_performance_csv(run_id)
    return PlainTextResponse(csv_content, media_type="text/csv")


@app.get("/dashboard/summary")
def dashboard_summary():
    """
    Dashboard summary with key metrics.
    Uses most recent run if available.
    """
    from app.persistence.run_manager import get_run_manager
    from app.persistence.trade_tracker import get_trade_tracker
    
    rm = get_run_manager()
    runs = rm.list_runs(limit=1)
    
    if not runs:
        return {"message": "no_runs_found"}
    
    run_id = runs[0]["run_id"]
    summary = get_trade_tracker().get_trade_summary(run_id)
    
    return {
        "run_id": run_id,
        "run_status": runs[0]["status"],
        "summary": summary,
    }


@app.get("/analytics/strategy-leaderboard")
def analytics_strategy_leaderboard(
    environment: str = "PAPER",
    exchange: str = "BINANCE_FUTURES",
    account_id: str = "default",
    limit: int = 20,
):
    """
    Get global strategy performance leaderboard.
    Aggregated per environment/exchange/account.
    """
    from app.persistence.global_analytics import get_global_analytics
    return {"leaderboard": get_global_analytics().get_strategy_leaderboard(environment, exchange, account_id, limit)}


@app.get("/analytics/confidence-calibration")
def analytics_confidence_calibration(
    strategy: str = None,
    environment: str = "PAPER",
    exchange: str = "BINANCE_FUTURES",
    account_id: str = "default",
):
    """
    Get confidence calibration data (win rate per bucket).
    Powers gating decisions. Filtered by environment/exchange/account.
    """
    from app.persistence.global_analytics import get_global_analytics
    return {"buckets": get_global_analytics().get_confidence_calibration(strategy, environment, exchange, account_id)}


@app.post("/analytics/process-run/{run_id}")
def analytics_process_run(
    run_id: str,
    environment: str = "PAPER",
    exchange: str = "BINANCE_FUTURES",
    account_id: str = "default",
):
    """
    Process a run into global analytics (idempotent).
    """
    from app.persistence.global_analytics import get_global_analytics
    processed = get_global_analytics().process_run(run_id, environment, exchange, account_id)
    return {"run_id": run_id, "processed": processed}


@app.get("/analytics/unprocessed-runs")
def analytics_unprocessed_runs():
    """Get list of runs that haven't been processed into global analytics."""
    from app.persistence.global_analytics import get_global_analytics
    return {"unprocessed": get_global_analytics().get_unprocessed_runs()}


# ============================================================================
# LEGACY EXPORTS - Works with existing trade_fills table
# ============================================================================

@app.get("/legacy/fills")
def legacy_fills_export(run_id: str = None):
    """Export fills from legacy trade_fills table as CSV."""
    from fastapi.responses import PlainTextResponse
    from app.persistence.exports import export_legacy_fills_csv
    csv_content = export_legacy_fills_csv(run_id)
    return PlainTextResponse(csv_content, media_type="text/csv")


@app.get("/legacy/performance")
def legacy_performance_export():
    """Export performance summary from legacy trade_fills table as CSV."""
    from fastapi.responses import PlainTextResponse
    from app.persistence.exports import export_legacy_performance_csv
    csv_content = export_legacy_performance_csv()
    return PlainTextResponse(csv_content, media_type="text/csv")


@app.get("/legacy/summary")
def legacy_summary():
    """Get summary stats from legacy trade_fills table."""
    from app.persistence.exports import get_legacy_summary
    return get_legacy_summary()


# ============================================================================
# RISK BUDGET ENGINE ENDPOINTS
# ============================================================================

@app.get("/risk/budget")
def risk_budget_status():
    """
    Get current risk budget state showing all limits and usage.
    Returns portfolio risk, margin usage, concentration, and remaining capacity.
    
    Uses cached data for instant response (~1ms vs ~500ms).
    """
    from app.risk.risk_budget import get_risk_budget_engine, PositionRisk
    from app.exchange.cache import get_exchange_cache
    
    engine = get_risk_budget_engine()
    cache = get_exchange_cache()
    
    # Get cached account data (instant)
    cached_account = cache.get_account()
    if cached_account:
        equity = cached_account.equity
        margin_used = cached_account.margin_used
        margin_available = cached_account.available_balance
    else:
        # Fallback to runner's cached balance
        runner = get_runner()
        equity = runner.get_account_balance()
        margin_used = 0.0
        margin_available = equity
    
    # Get cached positions (instant)
    positions = []
    for symbol, p in cache.get_positions().items():
        positions.append(PositionRisk(
            symbol=p.symbol,
            side=p.side,
            qty=p.qty,
            entry_price=p.entry_price,
            stop_price=None,
            notional=p.notional,
            strategy=None,
        ))
    
    # Update engine state
    engine.update_account_state(equity, margin_used, margin_available)
    engine.update_positions(positions)
    
    # Get budget state
    state = engine.get_budget_state()
    remaining = engine.get_remaining_capacity()
    
    return {
        "equity": equity,
        "portfolio_risk": {
            "total_risk_usdt": state.total_risk_usdt,
            "budget": state.portfolio_risk_budget,
            "usage_pct": state.portfolio_risk_usage_pct,
            "remaining": remaining["portfolio_risk_remaining"],
        },
        "margin": {
            "used": state.margin_used,
            "available": state.margin_available,
            "usage_pct": state.margin_usage_pct,
            "level": state.margin_level,
            "remaining": remaining["margin_remaining"],
        },
        "concentration": {
            "by_symbol": state.exposure_by_symbol,
            "by_strategy": state.exposure_by_strategy,
            "long_exposure": state.exposure_long,
            "short_exposure": state.exposure_short,
            "gross_exposure": state.gross_exposure,
            "net_exposure": state.net_exposure,
        },
        "slots": {
            "current": state.position_count,
            "allowed": state.allowed_slots,
            "remaining": remaining["slots_remaining"],
        },
        "limits": state.limits,
    }


@app.get("/risk/concentration")
def risk_concentration():
    """
    Get detailed concentration breakdown per symbol and strategy.
    
    Uses cached data for instant response (~1ms vs ~500ms).
    """
    from app.risk.risk_budget import get_risk_budget_engine, PositionRisk
    from app.exchange.cache import get_exchange_cache
    
    engine = get_risk_budget_engine()
    cache = get_exchange_cache()
    
    # Get cached equity
    cached_account = cache.get_account()
    equity = cached_account.equity if cached_account else get_runner().get_account_balance()
    
    # Get cached positions (instant)
    positions = []
    for symbol, p in cache.get_positions().items():
        positions.append(PositionRisk(
            symbol=p.symbol,
            side=p.side,
            qty=p.qty,
            entry_price=p.entry_price,
            stop_price=None,
            notional=p.notional,
            strategy=None,
        ))
    engine.update_positions(positions)
    
    engine.update_account_state(equity, 0, equity)
    state = engine.get_budget_state()
    
    # Calculate percentages
    symbol_pcts = {
        s: (v / equity * 100) if equity > 0 else 0
        for s, v in state.exposure_by_symbol.items()
    }
    
    return {
        "equity": equity,
        "by_symbol": {
            "notional": state.exposure_by_symbol,
            "pct_of_equity": symbol_pcts,
        },
        "by_side": {
            "long": state.exposure_long,
            "short": state.exposure_short,
            "long_pct": (state.exposure_long / state.gross_exposure * 100) if state.gross_exposure > 0 else 0,
            "short_pct": (state.exposure_short / state.gross_exposure * 100) if state.gross_exposure > 0 else 0,
        },
        "totals": {
            "gross_exposure": state.gross_exposure,
            "net_exposure": state.net_exposure,
            "gross_vs_equity": (state.gross_exposure / equity) if equity > 0 else 0,
        },
        "position_count": state.position_count,
    }
