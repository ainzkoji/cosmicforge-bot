import time
import uuid
import json
import traceback

import asyncio
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

migrate()


app = FastAPI(title="CosmicForge Bot MVP")
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


@app.on_event("shutdown")
async def _shutdown_run_manager():
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
    runner = get_runner()

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
            runner.run_once(max_symbols=runner_service.max_symbols)
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

            runner.audit.event(
                event_type="FATAL",
                action="RUNNER_HALTED",
                details={"error": err},
            )

            runner_service.running = False
            break

        finally:
            # ✅ ALWAYS clear cycle context
            clear_cycle_id()

        await asyncio.sleep(runner_service.interval_seconds)


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


@app.post("/trade/market")
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
        decision = runner.risk_gate.can_open()
        if not decision.allowed:
            # ✅ audit log for manual blocked attempts
            try:
                # ✅ FIX: Audit exposes event(...), not log_event(...)
                runner.audit.event(
                    event_type="RISK_BLOCK",
                    run_id=getattr(runner, "run_id", None),
                    symbol=symbol.upper(),
                    action="MANUAL_OPEN_BLOCKED",
                    details={
                        "reason": decision.reason,
                        "kill": decision.kill,
                        "realized_pnl": decision.realized_pnl,
                        "max_loss": decision.max_loss,
                        "endpoint": "/trade/market",
                        "side": side.upper(),
                        "usdt": usdt,
                    },
                )
            except Exception:
                pass

            return {
                "error": "RISK_BLOCK",
                "reason": decision.reason,
                "kill": decision.kill,
                "realized_pnl": decision.realized_pnl,
                "max_loss": decision.max_loss,
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


@app.post("/runner/live/once")
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
