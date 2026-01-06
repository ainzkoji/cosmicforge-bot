import time
import uuid
import json
import traceback

import asyncio
from pathlib import Path
from fastapi import Query
from fastapi import Body

from fastapi import FastAPI
from app.core.config import settings
from app.exchange.binance.client import BinanceFuturesClient
from app.execution.executor import BinanceExecutor
from app.symbols.universe import parse_symbols, build_universe
from app.runner.runner import PaperRunner
from app.symbols.leverage import parse_leverage_map, leverage_for
from app.exchange.binance.filters import extract_filters, round_qty

from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone
from app.risk.realized_pnl import realized_pnl_from_user_trades
from datetime import date
from app.execution.confirm import wait_until_flat


app = FastAPI(title="CosmicForge Bot MVP")
paper_runner_instance: PaperRunner | None = None


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


# app/main.py


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


async def runner_loop():
    """
    Continuous loop that calls runner.run_once() every interval_seconds.
    FAIL-CLOSED: on unhandled exception -> set kill=True, persist, log FATAL, stop runner.
    """
    runner = get_runner()

    while runner_service.running:
        try:
            runner_service.last_cycle_at = _utc_now_iso()

            # OPTIONAL: refresh daily state from DB each cycle (source of truth)
            try:
                saved = runner.store.load_daily(date.today())
                if saved:
                    runner.daily.day = date.today()
                    runner.daily.realized_pnl = float(saved.get("realized_pnl", 0.0))
                    runner.daily.kill = bool(saved.get("kill", False))
            except Exception:
                pass

            # ✅ DEBUG crash hook (must be before run_once)
            if getattr(runner_service, "crash_next_cycle", False):
                runner_service.crash_next_cycle = False
                raise RuntimeError("Intentional crash (debug/crash-next-cycle)")

            runner.run_once(max_symbols=runner_service.max_symbols)
            runner_service.cycle_count += 1
            runner_service.last_error = None

        except Exception:
            # capture full traceback
            err = traceback.format_exc()
            runner_service.last_error = err

            # ✅ FAIL CLOSED: kill switch ON + persist
            try:
                runner.daily.day = date.today()
                runner.daily.kill = True
                runner.store.save_daily(
                    runner.daily.day, runner.daily.realized_pnl, runner.daily.kill
                )
            except Exception:
                pass

            # ✅ log fatal event
            try:
                runner.audit.event(
                    event_type="FATAL",
                    run_id=getattr(runner, "run_id", None),
                    cycle_id=None,
                    symbol=None,
                    action="RUNNER_HALTED",
                    details={"error": err},
                )
            except Exception:
                pass

            # ✅ stop loop -> manual restart required
            runner_service.running = False
            break

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

    # ✅ ADD: start audit run (DO NOT REMOVE ANYTHING ELSE)
    runner.run_id = str(uuid.uuid4())
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
    """
    runner = get_runner()
    client = runner.client

    symbol = symbol.upper()

    # ✅ ADD: prevent collision with runner loop for same symbol
    with runner.symbol_guard(symbol, timeout_s=2.0) as ok:
        if not ok:
            return {
                "status": "rejected",
                "reason": "symbol_lock_busy",
                "symbol": symbol,
            }

        # 1) Read current position
        pos = client.get_position_info(symbol)
        if not pos:
            return {"status": "no_position_info"}

        pos_amt = float(pos.get("positionAmt", "0") or 0.0)
        if pos_amt == 0:
            return {"status": "flat"}

        # 2) Close
        close_order = client.close_position_market(symbol)
        order_id = close_order.get("orderId")

        # 3) Wait for order filled + position flat (robust)
        import time

        filled = False
        flat = False

        for _ in range(20):  # ~6 seconds max (20 * 0.3)
            try:
                if order_id is not None:
                    od = client.get_order(symbol, int(order_id))
                    if od.get("status") == "FILLED":
                        filled = True
                # also check flat
                p2 = client.get_position_info(symbol)
                amt2 = float(p2.get("positionAmt", "0") or 0.0) if p2 else 0.0
                if amt2 == 0.0:
                    flat = True
                if filled and flat:
                    break
            except Exception:
                pass

            time.sleep(0.3)

        # 4) Compute realized pnl from userTrades (dedup)
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (max(1, window_minutes) * 60 * 1000)

        st = runner.state.get(symbol)
        if st is None:
            # if symbol not in runner list, create minimal state entry
            from app.runner.runner import SymbolState

            st = SymbolState()
            runner.state[symbol] = st

        trades = client.user_trades(
            symbol, start_time_ms=start_ms, end_time_ms=end_ms, limit=1000
        )

        new_trades = []
        max_id = st.last_user_trade_id
        for t in trades:
            tid = t.get("id")
            if tid is None:
                continue
            try:
                tid_i = int(tid)
            except Exception:
                continue
            if tid_i > st.last_user_trade_id:
                new_trades.append(t)
                if tid_i > max_id:
                    max_id = tid_i

        pnl_added = realized_pnl_from_user_trades(new_trades)
        st.last_user_trade_id = max_id

        # persist symbol state (so dedup survives restart)
        try:
            runner.store.save_symbol(symbol, st)
        except Exception:
            pass

        # 5) Update daily
        runner.daily.reset_if_new_day()
        runner.daily.realized_pnl += pnl_added
        if runner.daily.realized_pnl <= -settings.DAILY_MAX_LOSS_USDT:
            runner.daily.kill = True

        runner.store.save_daily(
            runner.daily.day, runner.daily.realized_pnl, runner.daily.kill
        )

        return {
            "status": "closed_recorded",
            "symbol": symbol,
            "order_id": order_id,
            "filled": filled,
            "flat": flat,
            "userTrades_window_minutes": window_minutes,
            "userTrades_total": len(trades),
            "userTrades_new_count": len(new_trades),
            "pnl_added": pnl_added,
            "daily_realized_pnl": runner.daily.realized_pnl,
            "kill": runner.daily.kill,
            "close_order": close_order,
        }


@app.post("/trade/close-record-usertrades")
def trade_close_record_usertrades(symbol: str = "ETHUSDT", window_minutes: int = 10):
    """
    Robust close endpoint:
    - closes position
    - waits until FLAT confirmation (via wait_until_flat)
    - calculates realized pnl from userTrades (dedup via saved symbol_state.last_user_trade_id)
    - updates daily_state
    """
    runner = get_runner()
    client = runner.client

    symbol = symbol.upper()

    # 1) Read current position
    pos = client.get_position_info(symbol)
    if not pos:
        return {"status": "no_position_info"}

    pos_amt = float(pos.get("positionAmt", "0") or 0.0)
    if pos_amt == 0:
        return {"status": "flat"}

    # 2) Close (send market close)
    close_order = client.close_position_market(symbol)
    order_id = close_order.get("orderId")

    # 3) ✅ Wait until position is truly flat BEFORE recording PnL
    flat_confirmed = wait_until_flat(client, symbol, timeout_s=12, poll_s=0.5)
    if not flat_confirmed:
        return {
            "status": "close_sent_but_not_flat_confirmed",
            "symbol": symbol.upper(),
            "order_id": order_id,
        }

    # 4) Compute realized pnl from userTrades (dedup)
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - (max(1, window_minutes) * 60 * 1000)

    st = runner.state.get(symbol)
    if st is None:
        # if symbol not in runner list, create minimal state entry
        from app.runner.runner import SymbolState

        st = SymbolState()
        runner.state[symbol] = st

    trades = client.user_trades(
        symbol, start_time_ms=start_ms, end_time_ms=end_ms, limit=1000
    )

    new_trades = []
    max_id = st.last_user_trade_id
    for t in trades:
        tid = t.get("id")
        if tid is None:
            continue
        try:
            tid_i = int(tid)
        except Exception:
            continue

        if tid_i > st.last_user_trade_id:
            new_trades.append(t)
            if tid_i > max_id:
                max_id = tid_i

    pnl_added = realized_pnl_from_user_trades(new_trades)
    st.last_user_trade_id = max_id

    # persist symbol state (so dedup survives restart)
    try:
        runner.store.save_symbol(symbol, st)
    except Exception:
        pass

    # 5) Update daily
    runner.daily.reset_if_new_day()
    runner.daily.realized_pnl += pnl_added
    if runner.daily.realized_pnl <= -settings.DAILY_MAX_LOSS_USDT:
        runner.daily.kill = True

    runner.store.save_daily(
        runner.daily.day, runner.daily.realized_pnl, runner.daily.kill
    )

    return {
        "status": "closed_recorded",
        "symbol": symbol,
        "order_id": order_id,
        "flat_confirmed": True,
        "userTrades_window_minutes": window_minutes,
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
