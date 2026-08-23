from __future__ import annotations

from dataclasses import dataclass
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP

from app.core.config import settings
# from app.exchange.binance.client import BinanceFuturesClient # REMOVED
from app.exchange.interface import ExchangeClient # NEW
from app.exchange.registry import get_instrument_registry # NEW
from app.models.unified_trading import (
    OrderRequest,
    OrderType,
    Side,
    ProtectionRequest,  # ADDED - needed for SL/TP
    IdempotencyMode, # NEW models
)
from app.exchange.binance.filters import extract_filters  # ✅ RESTORED: needed by place_protection_orders
from app.symbols.leverage import leverage_for, parse_leverage_map
from app.execution.idempotency import build_entry_intent_key, generate_client_order_id
from app.execution.confirm import wait_until_flat
from app.symbols.sizing import parse_usdt_map, usdt_for, size_from_budget
from app.risk.circuit import ExchangeCircuitBreaker
from app.execution.entry_protection import get_entry_protection  # FAIL-SAFE ENTRY LOCK
from app.execution.paper_executor import PaperExecutor


# =============================================================================
# EXCEPTION HIERARCHY
# =============================================================================

class ExecutorError(Exception):
    """Base exception for all executor errors."""
    pass


class ExchangeError(ExecutorError):
    """Transient errors from the exchange (e.g., rate limits, network issues)."""
    pass


class FatalIntegrationError(ExecutorError):
    """Critical logical failures that should halt the system."""
    pass


class LogicContractViolation(ExecutorError):
    """Business logic errors like invalid inputs."""
    pass


class PolicyViolation(Exception):
    pass

# =========================
# Module-level helpers
# =========================

def _round_to_tick(price: float, tick: float, round_up: bool):
    """Round a price to the nearest tick boundary. Returns a Decimal."""
    from decimal import Decimal, ROUND_DOWN, ROUND_UP as _RU
    tick_d = Decimal(str(tick))
    price_d = Decimal(str(price))
    rounding = _RU if round_up else ROUND_DOWN
    return (price_d / tick_d).to_integral_value(rounding=rounding) * tick_d


def _apply_sl_tp_rounding_with_buffer(
    side: str,         # entry side: "BUY" (LONG) or "SELL" (SHORT)
    entry_px: float,
    sl_price: float,
    tp_price: float,
    tick: float,
    buffer_ticks: int = 2,
) -> tuple[str, str]:
    """
    Round SL and TP to valid tick boundaries and apply a safety buffer so that
    they never sit exactly at the current price (which can trigger -2021 immediately).

    For a LONG (BUY entry):
      SL must be BELOW entry  → round DOWN then subtract buffer
      TP must be ABOVE entry  → round UP   then add    buffer

    For a SHORT (SELL entry):
      SL must be ABOVE entry  → round UP   then add    buffer
      TP must be BELOW entry  → round DOWN then subtract buffer
    """
    from decimal import Decimal
    tick_d = Decimal(str(tick))
    buf_d = tick_d * Decimal(buffer_ticks)
    
    # Determine places for final quantization so we don't return trailing garbage
    places = max(0, -tick_d.as_tuple().exponent)
    quant_str = Decimal("1").scaleb(-places)

    if str(side).upper() == "BUY":  # LONG position
        sl_d = _round_to_tick(sl_price, tick, round_up=False) - buf_d
        tp_d = _round_to_tick(tp_price, tick, round_up=True)  + buf_d
    else:                           # SHORT position
        sl_d = _round_to_tick(sl_price, tick, round_up=True)  + buf_d
        tp_d = _round_to_tick(tp_price, tick, round_up=False) - buf_d

    # Quantize securely to tick's decimal places to eliminate any micro-fraction before floatcast
    sl_rounded = sl_d.quantize(quant_str)
    tp_rounded = tp_d.quantize(quant_str)

    return str(sl_rounded), str(tp_rounded)


def _interval_to_ms(interval: str) -> int:
    """Convert common exchange kline intervals to milliseconds."""
    raw = str(interval or "1m").strip()
    if not raw:
        raw = "1m"
    unit = raw[-1]
    try:
        qty = int(raw[:-1])
    except (TypeError, ValueError):
        return 60_000

    multipliers = {
        "m": 60_000,
        "h": 60 * 60_000,
        "d": 24 * 60 * 60_000,
        "w": 7 * 24 * 60 * 60_000,
        "M": 30 * 24 * 60 * 60_000,
    }
    return max(60_000, qty * multipliers.get(unit, 60_000))


def _extract_kline_times_ms(kline, interval_ms: int) -> tuple[int | None, int | None]:
    """Return (open_time_ms, close_time_ms) from Binance/list/dict/object klines."""
    open_time = None
    close_time = None

    try:
        if isinstance(kline, dict):
            open_time = kline.get("open_time") or kline.get("openTime") or kline.get("open")
            close_time = kline.get("close_time") or kline.get("closeTime")
        elif isinstance(kline, (list, tuple)):
            if len(kline) > 0:
                open_time = kline[0]
            if len(kline) > 6:
                close_time = kline[6]
        else:
            open_time = getattr(kline, "open_time", None) or getattr(kline, "openTime", None)
            close_time = getattr(kline, "close_time", None) or getattr(kline, "closeTime", None)

        open_ms = int(open_time) if open_time is not None else None
        close_ms = int(close_time) if close_time is not None else None
        if close_ms is None and open_ms is not None:
            close_ms = open_ms + int(interval_ms) - 1
        return open_ms, close_ms
    except (TypeError, ValueError):
        return None, None


def _kline_staleness(
    klines: list,
    *,
    interval: str,
    now_ms: int,
    buffer_ms: int,
) -> dict:
    """Interval-aware freshness verdict for the latest returned market candle."""
    interval_ms = _interval_to_ms(interval)
    threshold_ms = interval_ms + max(0, int(buffer_ms))
    if not klines:
        return {
            "ok": False,
            "reason": "missing_klines",
            "interval": interval,
            "interval_ms": interval_ms,
            "threshold_ms": threshold_ms,
        }

    last_kline = klines[-1]
    open_ms, close_ms = _extract_kline_times_ms(last_kline, interval_ms)
    if close_ms is None:
        return {
            "ok": False,
            "reason": "missing_kline_timestamp",
            "interval": interval,
            "interval_ms": interval_ms,
            "threshold_ms": threshold_ms,
        }

    age_ms = int(now_ms) - int(close_ms)
    return {
        "ok": age_ms <= threshold_ms,
        "reason": None if age_ms <= threshold_ms else "stale_data",
        "open_time_ms": open_ms,
        "close_time_ms": close_ms,
        "age_ms": age_ms,
        "interval": interval,
        "interval_ms": interval_ms,
        "threshold_ms": threshold_ms,
    }


def _place_sl_with_retry_on_2021(
    client,
    symbol: str,
    exit_side: str,
    sl_price: str,
    buf: float,
    max_retries: int = 2,
) -> dict:
    """
    Place a STOP_MARKET order with retry logic for Binance error -2021
    (order would immediately trigger). On that error, push SL further away
    from market by one extra buffer and retry.
    """
    from decimal import Decimal
    attempt_price_d = Decimal(sl_price)
    buf_d = Decimal(str(buf))
    # We need the quantize pattern to ensure we don't return scientific notation or extra precision
    places = max(0, -buf_d.as_tuple().exponent)
    quant_str = Decimal("1").scaleb(-places)

    for attempt in range(max_retries + 1):
        attempt_price_str = str(attempt_price_d.quantize(quant_str))
        try:
            return client.place_stop_market(symbol, exit_side, attempt_price_str)
        except Exception as e:
            err_str = str(e)
            is_2021 = '"-2021"' in err_str or '"code":-2021' in err_str or "would immediately trigger" in err_str.lower()
            if is_2021 and attempt < max_retries:
                # Push SL further away: for SELL exit (long SL) go lower, for BUY exit (short SL) go higher
                if exit_side.upper() == "SELL":
                    attempt_price_d = attempt_price_d - buf_d
                else:
                    attempt_price_d = attempt_price_d + buf_d
                continue
            raise  # Re-raise if not -2021 or out of retries

# =========================
# Execution Result
# =========================
@dataclass
class ExecResult:
    status: str
    details: dict
    order_id: str | None = None
    success: bool = False  # True if trade executed successfully
    avg_price: float | None = None  # Average fill price if applicable
    error: str | None = None  # Error message if failed
    action: str = "OPEN"

# =========================
# Generic Executor (Formerly BinanceExecutor)
# =========================
class BinanceExecutor:
    def __init__(
        self,
        client: ExchangeClient,
        risk_gate=None,
        audit=None,
        execution_mode: str | None = None,
        live_symbols: list | None = None,
        bot_instance_id: str = "default",
        db=None,
        market_data_interval: str | None = None,
    ):
        self.client = client # Typed as ExchangeClient protocol
        self.bot_instance_id = bot_instance_id
        self.tpsl_repair_attempt_total = 0
        self.tpsl_repair_success_total = 0
        self.tpsl_repair_failure_total = 0
        self.risk_gate = risk_gate
        self.audit = audit
        self.run_id = None  # runner may set this
        # Per-bot execution mode — overrides the global settings.EXECUTION_MODE if provided
        # When None, falls back to settings.EXECUTION_MODE for backwards compatibility
        self.execution_mode = execution_mode  # "live" or "paper"
        self.market_data_interval = str(market_data_interval or settings.DEFAULT_INTERVAL or "1m")
        # Per-bot live symbols — when provided, bypasses the global settings.LIVE_SYMBOLS gate
        self._live_symbols_override: set | None = (
            {s.strip().upper() for s in live_symbols if s.strip()} if live_symbols is not None else None
        )
        # Breaker: 5 errors in 60s -> pause for 300s
        self.circuit = ExchangeCircuitBreaker(error_limit=5, window_seconds=60, timeout_seconds=300)
        # FAIL-SAFE ENTRY LOCK — None until runner injects a DB reference
        self._db = db
        self._entry_prot = get_entry_protection(db) if db is not None else None
        self._allocation_type = "fixed_usdt"
        self._allocation_value = 0.0
        self._max_notional_per_symbol = 0.0
        self._allow_scale_in = False
        self._allow_hedge_mode = False
        self.paper_executor = PaperExecutor(
            client=self.client,
            slippage_bps=float(getattr(settings, "PAPER_SLIPPAGE_BPS", 2.0) or 2.0),
            fee_bps=float(getattr(settings, "PAPER_FEE_BPS", 4.0) or 4.0),
        )

    def _configured_max_exposure(self, current_equity: float) -> float:
        """
        Return the maximum NOTIONAL exposure allowed for a single symbol.

        IMPORTANT – units must match get_effective_exposure(), which sums
        notional values (qty × price) from pending_entries.

        * fixed_amount / fixed_usdt ── allocation_value is the MARGIN budget.
          The notional equivalent is stored in _max_notional_per_symbol
          (= margin × leverage) by the runner.  Use that directly so the
          guard compares notional vs notional and never fires at leverage > 1×.
        * percent_balance / percent_equity ── allocation_value is the % of
          equity.  Equity × pct already gives a notional-scale figure.
        * Fallback ── _max_notional_per_symbol (0.0 = disabled).
        """
        allocation_type = str(getattr(self, "_allocation_type", "") or "").lower()
        allocation_value = float(getattr(self, "_allocation_value", 0.0) or 0.0)
        max_notional = float(getattr(self, "_max_notional_per_symbol", 0.0) or 0.0)

        if allocation_type in {"fixed_usdt", "fixed_amount", "fixed"}:
            # _max_notional_per_symbol = margin × leverage (set by runner).
            # If not yet wired (legacy paths), fall back to 0 (guard disabled).
            return max_notional

        if allocation_type in {"percent_balance", "percent_equity", "percent"} and allocation_value > 0:
            return max(0.0, float(current_equity or 0.0)) * allocation_value / 100.0

        return max_notional

    def _build_entry_idempotency(
        self,
        symbol: str,
        side: str,
        usdt: float,
        sl_price: float,
        tp_price: float,
    ) -> tuple[str, str]:
        intent_bucket = int(time.time() // 30) * 30
        strategy_intent = "|".join(
            [
                str(self.run_id or "0"),
                symbol.upper(),
                side.upper(),
                f"{float(usdt):.8f}",
                f"{float(sl_price or 0.0):.8f}",
                f"{float(tp_price or 0.0):.8f}",
            ]
        )
        intent_key = build_entry_intent_key(
            bot_instance_id=self.bot_instance_id or "default",
            symbol=symbol,
            side=side,
            intent_bucket=intent_bucket,
            strategy_intent=strategy_intent,
            intended_notional=float(usdt),
            sl_price=sl_price,
            tp_price=tp_price,
        )
        client_order_id = generate_client_order_id(
            bot_instance_id=self.bot_instance_id or "default",
            symbol=symbol,
            side=side,
            intent_key=intent_key,
        )
        return intent_key, client_order_id

    def estimate_slippage(self, trade_notional_usdt: float) -> float:
        """
        Estimate market impact slippage as a percentage based on trade notional tier.
        This provides a realistic cost model for backtesting and live accounting.
        Tiers roughly model Binance Futures BTCUSDT liquidity.
        """
        if trade_notional_usdt < 10_000:
            return 0.00015  # 1.5 bps
        elif trade_notional_usdt < 100_000:
            return 0.00030  # 3 bps
        elif trade_notional_usdt < 500_000:
            return 0.00080  # 8 bps
        elif trade_notional_usdt < 1_000_000:
            return 0.00150  # 15 bps
        else:
            return 0.00250  # 25 bps


    def _normalize_order(self, order_res: dict, symbol: str, side: str, type_: str, qty: float, price: float = 0.0) -> dict:
        """
        Produce a normalized trade record for analytics, preventing vendor lock-in.
        """
        # Infer broker from client type name
        c_name = self.client.__class__.__name__
        broker = "bybit" if "Bybit" in c_name else "binance"
        
        return {
            "broker": broker,
            "order_id": str(order_res.get("orderId", "")),
            "symbol": symbol,
            "side": side.upper(),
            "type": type_.upper(),
            "quantity": float(qty),
            "executed_qty": float(order_res.get("executedQty", 0.0) or 0.0),
            "avg_price": float(order_res.get("avgPrice", 0.0) or price or 0.0),
            "status": order_res.get("status", "NEW").upper(),
            "timestamp": int(float(order_res.get("updateTime", 0) or 0)) 
        }

    # ---------------- INTERNAL HELPERS ----------------

    def _size_qty(self, symbol: str, usdt: float, leverage_mult: float = 1.0, sl_price: float = 0.0, leverage_override: int | None = None) -> tuple[float, dict]:
        # 1. Fetch Spec
        registry = get_instrument_registry()
        # Fallback to "binance" if broker_id not available on executor, assume context set correctly
        # Ideally executor should know its broker_id. 
        # For Shim Phase, we assume "binance" lookups work or simple mapping.
        # But we don't have broker_id on self. 
        # Workaround: Iterating usually not generic. 
        # Better: ExchangeClient should expose `get_instrument(symbol)`? 
        # Or just use registry with "binance" for now since this IS the Binance Executor technically.
        spec = registry.get_spec("binance", symbol)
        if not spec:
            # Try to refresh? Or just fail?
            return 0.0, {"error": "Instrument spec not found", "symbol": symbol}
            
        # 2. Fetch Price
        prices = self.client.get_prices([symbol])
        price = float(prices.get(symbol, 0.0))
        if price <= 0:
             return 0.0, {"error": "Invalid price", "symbol": symbol}

        # 3. Leverage
        if leverage_override is not None and leverage_override > 0:
            lev = leverage_override
        else:
            lev_map = parse_leverage_map(settings.SYMBOL_LEVERAGE_MAP)
            base_lev = leverage_for(
                symbol, lev_map, settings.DEFAULT_LEVERAGE, settings.MIN_LEVERAGE
            )
            # 🚀 Phase 5: ATR-proportional leverage compression
            # We actively compress the base leverage using the ATR multiplier from strategy.
            # This reduces allowed capital velocity when market risk expands.
            lev = max(1, int(float(base_lev) * leverage_mult))
        
        # 🚀 Phase 6: Slippage-Integrated Sizing
        expected_slippage_pct = self.estimate_slippage(float(usdt))

        # 🚀 SYSTEM AUDIT: Liquidation Distance Kill-Switch & Mathematical Invariant
        # We must GUARANTEE that Liquidation Distance >= 2 * Stop Distance.
        # This considers exact Exchange metrics to avoid hidden ruin risk.
        if sl_price > 0.0 and price > 0.0:
            raw_stop_distance_pct = abs(price - sl_price) / price
            # Include expected round-trip slippage in the worst-case stop distance assumption
            stop_distance_pct = raw_stop_distance_pct + (expected_slippage_pct * 2.0)
            
            # Model Real Exchange parameters (Binance USDT-M Futures default worst-case bounds):
            # MMR (Maintenance Margin Rate) ~ 0.5% for general tier-2
            # Open Taker Fee ~ 0.05%
            # Close Taker Fee ~ 0.05%
            # Funding + Slippage Buffer ~ 0.01%
            # Total FFR (Fee & Funding Reserve) = 0.0011 (0.11%)
            # Total Deduction (MMR + FFR) = 0.0061 (0.61%)
            
            # Liquidation Distance Pct = (1.0 / Leverage) - 0.0061
            # Invariant: (1.0 / Leverage) - 0.0061 >= 2.0 * stop_distance_pct
            # Derivation: Leverage <= 1.0 / (2.0 * stop_distance_pct + 0.0061)
            
            denominator = (2.0 * stop_distance_pct) + 0.0061
            max_safe_lev = int(1.0 / denominator) if denominator > 0 else lev
            
            if max_safe_lev < 1:
                max_safe_lev = 1
                
            if lev > max_safe_lev:
                import logging as _log
                _log.getLogger(__name__).warning(
                    f"[LIQUIDATION SAFETY KILL-SWITCH] {symbol}: Base Lev {lev}x is structurally UNSAFE "
                    f"for Effective Risk {stop_distance_pct:.2%} (Raw SL: {raw_stop_distance_pct:.2%} + Slippage {expected_slippage_pct*2.0:.2%}). "
                    f"Dynamically shrinking leverage down to {max_safe_lev}x "
                    f"to absolutely guarantee Liquidation Distance >= 2x Effective Stop Distance."
                )
                lev = max_safe_lev
                
        import logging as _log
        if leverage_mult < 1.0:
            _log.getLogger(__name__).info(f"[ATR COMPRESSION] {symbol}: High ATR triggered leverage compression. Base: {base_lev}x -> Reduced: {lev}x")

        # 4. Target Notional
        # usdt is passed from executor as NOTIONAL value (trade_usdt = qty * price)
        target_notional = float(usdt)
        actual_margin = target_notional / float(lev)

        # 5. Calculate Sizing
        # We simulate the 'filters' object for compatibility with size_from_budget if kept generic
        # OR we just update size_from_budget to take simple args.
        # size_from_budget takes 'filters' and does getattr(filters, 'step_size').
        # We can pass the spec itself if it has matching attrs, or a dummy.
        # Spec has: step_size, min_qty, min_notional.
        
        # size_from_budget expects decimal-ish attributes on filters
        res = size_from_budget(
            symbol=symbol,
            price=price,
            usdt_margin=actual_margin,
            leverage=int(lev),
            filters=spec, # Spec has step_size, min_qty, min_notional attributes matching names!
            min_notional_override=float(getattr(settings, "MIN_NOTIONAL_USDT", 0.0) or 0.0),
            contract_size=float(spec.contract_size)
        )
        
        # 6. Return Result
        res.details["leverage"] = lev
        return float(res.qty), res.details

    def _ensure_leverage(self, symbol: str) -> dict:
        lev_map = parse_leverage_map(settings.SYMBOL_LEVERAGE_MAP)
        lev = leverage_for(
            symbol,
            lev_map,
            settings.DEFAULT_LEVERAGE,
            settings.MIN_LEVERAGE,
        )
        res = self.client.set_leverage(symbol, lev)
        return {"leverage": lev, "result": res}

    def _audit_warn(self, symbol: str, action: str, details: dict) -> None:
        if self.audit is None:
            return
        try:
            self.audit.event(
                event_type="WARN",
                run_id=getattr(self, "run_id", None),
                symbol=symbol,
                action=action,
                details=details,
            )
        except Exception:
            pass

    def _protection_is_sane(
        self, position: str, entry_price: float, sl: float, tp: float
    ) -> bool:
        """
        Sanity check for SL/TP relative to entry.
        LONG:  sl < entry < tp
        SHORT: tp < entry < sl
        """
        if not entry_price or entry_price <= 0:
            return False
        if position == "LONG":
            return (sl < entry_price) and (entry_price < tp)
        if position == "SHORT":
            return (tp < entry_price) and (entry_price < sl)
        return False

    def place_protection_orders(
        self,
        symbol: str,
        signal: str,
        qty: float,
        sl_price: float,
        tp_price: float,
    ) -> dict:
        """
        Place SL/TP protection orders using the same rounding/buffer + -2021 retry logic.
        """
        signal_u = str(signal or "").upper()
        side = "BUY" if signal_u == "BUY" else "SELL"  # entry side
        exit_side = "SELL" if side == "BUY" else "BUY"

        # Get tick_size for rounding — try generic interface first, fallback to Binance raw
        tick = None
        try:
            if hasattr(self.client, "get_symbol_filters"):
                sym_flt = self.client.get_symbol_filters(symbol)
                tick = sym_flt.tick_size
        except Exception:
            pass
        if tick is None:
            try:
                exch = self.client.exchange_info_cached()
                flt = extract_filters(exch, symbol)
                tick = flt.tick_size
            except Exception:
                tick = 0.01  # safe fallback
        buffer_ticks = 2
        entry_px = float(self.client.last_price(symbol))
        sl_price, tp_price = _apply_sl_tp_rounding_with_buffer(
            side=side,
            entry_px=entry_px,
            sl_price=float(sl_price),
            tp_price=float(tp_price),
            tick=tick,
            buffer_ticks=buffer_ticks,
        )

        tick_f = float(tick)
        buf = tick_f * float(buffer_ticks)

        sl = _place_sl_with_retry_on_2021(
            client=self.client,
            symbol=symbol,
            exit_side=exit_side,
            sl_price=sl_price,
            buf=buf,
        )
        tp = self.client.place_take_profit_market(symbol, exit_side, tp_price)

        return {
            "sl": sl,
            "tp": tp,
            "sl_price": float(sl_price),
            "tp_price": float(tp_price),
            "exit_side": exit_side,
            "qty": float(qty),
        }

    # ---------------- EXECUTION ----------------

    def execute_signal(
        self,
        symbol: str,
        signal: str,
        usdt: float,
        sl_price: float = 0.0,
        tp_price: float = 0.0,
        current_open_count: int = 0,
        current_equity: float = 0.0,
        leverage_mult: float = 1.0,
        leverage_override: int | None = None,
        cycle_id: str | None = None,
    ) -> ExecResult:
        """
        Execute a trading signal, allowing typed exceptions to propagate.
        """
        return self._execute_impl(
            symbol, signal, usdt, sl_price, tp_price, current_open_count, current_equity, leverage_mult, leverage_override, cycle_id=cycle_id
        )

    def _execute_impl(
        self,
        symbol: str,
        signal: str,
        usdt: float,
        sl_price: float = 0.0,
        tp_price: float = 0.0,
        current_open_count: int = 0,
        current_equity: float = 0.0,
        leverage_mult: float = 1.0,
        leverage_override: int | None = None,
        cycle_id: str | None = None,
    ) -> ExecResult:
        """
        Internal implementation of execute_signal.
        Entry protection invariant: for any BUY/SELL signal that would open a position,
        the entry lock must be acquired BEFORE any exchange interaction, and exactly
        one state transition (confirmed / uncertain / failed) must execute regardless
        of the exception path.
        """
        signal = signal.upper()

        if signal not in {"BUY", "SELL", "CLOSE"}:
            return ExecResult(
                status="NO_TRADE",
                details={"symbol": symbol, "signal": signal, "reason": "unsupported_signal"},
                success=False,
                error="Unsupported signal type",
                action="NO_TRADE",
            )

        # Paper mode means no exchange order is sent, but the internal lifecycle
        # still receives a simulated order/fill result so runners can persist
        # paper fills and managed positions.
        effective_mode = (self.execution_mode or settings.EXECUTION_MODE).lower()
        if effective_mode != "live":
            if signal == "CLOSE":
                paper = self.paper_executor.close_position(symbol=symbol)
            else:
                paper = self.paper_executor.open_position(
                    symbol=symbol,
                    side=signal,
                    notional_usdt=float(usdt or 0.0),
                    sl_price=sl_price,
                    tp_price=tp_price,
                )
            return ExecResult(
                status=paper.status,
                details=paper.details,
                order_id=paper.order_id,
                success=paper.success,
                avg_price=paper.avg_price,
                error=paper.error,
                action=paper.action,
            )

        # LIVE_SYMBOLS gate:
        # - If executor was initialised with a per-bot symbol list (_live_symbols_override),
        #   that list IS the authoritative allowlist — skip the global LIVE_SYMBOLS check.
        # - Otherwise fall back to the global settings.LIVE_SYMBOLS filter.
        if self._live_symbols_override is not None:
            # Per-bot context already authorised these symbols — trust it.
            pass
        else:
            live_symbols_str = settings.LIVE_SYMBOLS or ""
            live_symbols_global = set(s.strip().upper() for s in live_symbols_str.split(",") if s.strip())
            if live_symbols_global and symbol.upper() not in live_symbols_global:
                return ExecResult(
                    status="SKIPPED_NOT_LIVE_SYMBOL",
                    details={"symbol": symbol, "signal": signal},
                    success=False,
                    error="Symbol not in live trading list (global LIVE_SYMBOLS)"
                )

        if self.circuit.is_tripped():
            return ExecResult(
                status="CIRCUIT_BREAKER_TRIPPED",
                details={"symbol": symbol, "signal": signal, "reason": "exchange_errors_threshold_exceeded"},
                success=False,
                error="Circuit breaker tripped",
                action="ERROR",
            )

        try:
            target_pos = self.client.get_position_info(symbol)
            
            # Handle dictionary response (Raw Client)
            if isinstance(target_pos, dict):
                pos_amt = float(target_pos.get("positionAmt", 0.0))
            # Handle object response (Adapter/UnifiedPosition)
            elif hasattr(target_pos, "quantity"):
                pos_amt = float(target_pos.quantity)
                # Check side if available
                if hasattr(target_pos, "side") and str(target_pos.side).upper() == "SELL":
                    pos_amt = -pos_amt
            else:
                pos_amt = 0.0
        except Exception as e:
            raise ExchangeError(f"Failed to fetch position info for {symbol}: {e}")

        # Derive current position state (source of truth)
        if pos_amt > 0:
            current_position = "LONG"
        elif pos_amt < 0:
            current_position = "SHORT"
        else:
            current_position = "NONE"

        # ✅ 1) Handle explicit CLOSE (priority)
        if signal == "CLOSE":
            if current_position == "NONE":
                return ExecResult(
                    status="NO_TRADE",
                    details={"symbol": symbol, "signal": signal, "reason": "ALREADY_FLAT"},
                    success=False,
                    error="Position already flat"
                )

            # Cancel Open Orders (SL/TP) before closing the position to prevent orphaned orders
            try:
                self.client.cancel_all_orders(symbol)
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"[EXECUTOR] Failed to cancel open orders for {symbol} prior to CLOSE: {e}")

            # Close Position
            close_order = self.client.close_position_market(symbol)
            if getattr(self, "_entry_prot", None) is not None:
                _closing_side = "SHORT" if current_position == "SHORT" else "LONG"
                self._entry_prot.mark_closed(self.bot_instance_id, symbol, _closing_side)
            normalized = self._normalize_order(close_order, symbol, "CLOSE", "MARKET", 0.0)

            return ExecResult(
                status="CLOSED_POSITION",
                details={
                    "symbol": symbol,
                    "pos_amt_before": pos_amt,
                    "position_before": current_position,
                    "close_order": close_order,
                    "normalized": normalized
                },
                success=True,
                avg_price=float(normalized.get("avg_price", 0.0))
            )

        # Determine whether this is an ADD
        is_add = (signal == "BUY" and current_position == "LONG") or (
            signal == "SELL" and current_position == "SHORT"
        )
        is_flip = (signal == "BUY" and current_position == "SHORT") or (
            signal == "SELL" and current_position == "LONG"
        )

        # ✅ 2) Handle FLIP (Close then Open)
        if is_flip:
            # Close first
            try:
                self.client.cancel_all_orders(symbol)
            except: pass
            
            close_order = self.client.close_position_market(symbol)
            if getattr(self, "_entry_prot", None) is not None:
                _closing_side = "SHORT" if current_position == "SHORT" else "LONG"
                self._entry_prot.mark_closed(self.bot_instance_id, symbol, _closing_side)
            # We don't return here, we proceed to OPEN new position below
            # But strictly, simpler to return "CLOSED_FOR_FLIP" and let next tick open?
            # Or do it atomically?
            # Risky to do both in one tick if latencies.
            # Let's return the close and let auto-pilot retry the open next cycle?
            # OR just do it.
            # Existing executor did it atomically-ish.
            
            # Let's trigger the close and continue to open logic (treating as new entry)
            # Update pos_amt to 0 for sizing logic
            pos_amt = 0.0
            current_position = "NONE"

        # FIX: ALREADY_OPEN guard — block duplicate OPEN when exchange already shows a position.
        # The runner sends signal="BUY" for both OPEN_LONG and ADD_LONG. The executor can
        # distinguish intent by checking whether the exchange currently holds a position in
        # the same direction. If is_add=True and this is not a deliberate add-on (the runner
        # would send "ADD_LONG_N" strings for adds, which are not "BUY"), block the order to
        # prevent stacking a second full position on top of an already-open one.
        # This was the secondary mechanism in the ETCUSDT double-open: cycle N+1's executor
        # saw positionAmt>0 (is_add=True) but still placed the order because there was no guard.
        if is_add and not bool(getattr(self, "_allow_scale_in", False)):
            import logging as _add_log
            _add_log.getLogger(__name__).warning(
                f"[ALREADY_OPEN] {symbol}: BLOCKED — exchange already shows {current_position} "
                f"position (positionAmt={pos_amt:.6f}) but received signal={signal}. "
                f"Refusing duplicate OPEN to prevent position stacking."
            )
            return ExecResult(
                status="ALREADY_OPEN",
                details={
                    "symbol": symbol,
                    "signal": signal,
                    "current_position": current_position,
                    "pos_amt": pos_amt,
                    "reason": "Exchange already holds position in this direction. Use ADD intent explicitly."
                },
                success=False,
                error=f"[ALREADY_OPEN] {symbol}: {current_position} position exists — duplicate OPEN blocked."
            )

        # ─────────────────────────────────────────────────────────────────────────
        # HARD INVARIANT: Entry-intent lock acquisition
        # ─────────────────────────────────────────────────────────────────────────
        # Signal "BUY" → side LONG; "SELL" → side SHORT.
        # The lock is acquired here — BEFORE any balance/sizing work — so that even
        # a pre-submit hard failure triggers mark_failed() and clears the lock.
        # Any second concurrent or retry attempt will fail at INSERT OR IGNORE and
        # return ENTRY_LOCK_HELD before touching the exchange.
        _ep_side = "LONG" if signal == "BUY" else "SHORT"
        _ep = getattr(self, "_entry_prot", None)
        _ep_lock_acquired = False
        _ep_intent_key = None
        _ep_cid = None
        _ep_acquire = None

        if _ep is not None:
            _ep_intent_key, _ep_cid = self._build_entry_idempotency(
                symbol=symbol,
                side=_ep_side,
                usdt=float(usdt),
                sl_price=float(sl_price or 0.0),
                tp_price=float(tp_price or 0.0),
            )
            _existing_entry = None
            # ── Exposure guard: reject if adding would breach configured maximum ──
            _effective_exposure = _ep.get_effective_exposure(self.bot_instance_id, symbol)
            _max_exposure = self._configured_max_exposure(current_equity)
            if False and (
                _max_exposure > 0
                and (_effective_exposure + float(usdt)) > _max_exposure
                and ((_existing_entry or {}).get("intent_key") != _ep_intent_key)
            ):
                import logging as _ep_log
                _ep_log.getLogger(__name__).warning(
                    "[ENTRY_PROTECTION] EXPOSURE_GUARD bot=%s sym=%s: effective=%.2f + new=%.2f > max=%.2f. BLOCKED.",
                    self.bot_instance_id, symbol, _effective_exposure, float(usdt), _max_exposure,
                )
                return ExecResult(
                    status="EXPOSURE_LIMIT_EXCEEDED",
                    details={"symbol": symbol, "effective_exposure": _effective_exposure, "new_notional": float(usdt), "max_notional": _max_exposure},
                    success=False,
                    error=f"[EXPOSURE_GUARD] {symbol}: cannot open — effective exposure {_effective_exposure:.2f} + {usdt:.2f} > max {_max_exposure:.2f}",
                )

            # ── Deterministic clientOrderId for this intent ──
            _ep_intent_key, _ep_cid = self._build_entry_idempotency(
                symbol=symbol,
                side=_ep_side,
                usdt=float(usdt),
                sl_price=float(sl_price or 0.0),
                tp_price=float(tp_price or 0.0),
            )

            _ep_acquire = _ep.acquire_intent(
                bot_id=self.bot_instance_id,
                symbol=symbol,
                side=_ep_side,
                intended_notional=float(usdt),
                client_order_id=_ep_cid,
                intent_key=_ep_intent_key,
                cycle_id=cycle_id,
                allow_hedge=bool(getattr(self, "_allow_hedge_mode", False)),
            )
            _ep_lock_acquired = _ep_acquire.status.value == "ACQUIRED"
            if _ep_acquire.status.value == "REUSED":
                _existing = _ep_acquire.entry or {}
                return ExecResult(
                    status="ENTRY_INTENT_REUSED",
                    details={
                        "symbol": symbol,
                        "signal": signal,
                        "side": _ep_side,
                        "client_order_id": _existing.get("client_order_id"),
                        "entry_state": _existing.get("state"),
                        "submit_state": _existing.get("submit_state"),
                        "reason": _ep_acquire.reason,
                    },
                    success=False,
                    error=f"[ENTRY_LOCK] {symbol}: identical intent already active; retry is idempotent.",
                )
            if _ep_acquire.status.value == "BLOCKED":
                import logging as _ep_log2
                _ep_log2.getLogger(__name__).warning(
                    "[ENTRY_PROTECTION] LOCK_HELD bot=%s sym=%s side=%s — duplicate open BLOCKED.",
                    self.bot_instance_id, symbol, _ep_side,
                )
                return ExecResult(
                    status="ENTRY_LOCK_HELD",
                    details={
                        "symbol": symbol,
                        "signal": signal,
                        "side": _ep_side,
                        "reason": "Entry intent lock already held for this bot/symbol/side.",
                        "existing_entry": _ep_acquire.entry,
                    },
                    success=False,
                    error=f"[ENTRY_LOCK] {symbol}: open blocked — pending/confirmed entry already exists.",
                )
        # ─────────────────────────────────────────────────────────────────────────

        # ✅ B) Sizing
        budget_usdt = float(usdt or 0.0)
        # If budget <= 0, fall back to per-symbol or global config
        if budget_usdt <= 0:
            usdt_map = parse_usdt_map(getattr(settings, "SYMBOL_USDT_MAP", None))
            budget_usdt = float(
                usdt_for(symbol, usdt_map, settings.TRADE_USDT_PER_ORDER)
            )

        # ✅ PRE-TRADE BALANCE CHECK (HARDENED)
        # budget_usdt is NOTIONAL (position value = qty * price).
        # Margin required = notional / leverage.
        # We apply a 5% safety buffer so we never use >95% of available balance
        # and Binance never rejects with -2019 after our check passes.
        import logging as _log
        _exec_logger = _log.getLogger(__name__)
        MIN_NOTIONAL = 5.0  # Binance futures minimum
        MARGIN_SAFETY_BUFFER = 0.95  # Never consume more than 95% of available balance
        try:
            # ── Resolve effective leverage — must match _size_qty exactly ──
            from app.symbols.leverage import leverage_for, parse_leverage_map
            if leverage_override is not None and leverage_override > 0:
                # leverage_override takes priority (set by orchestrator per-symbol).
                # Cast to int — Binance rejects fractional leverage.
                effective_lev = int(leverage_override)
            else:
                lev_map = parse_leverage_map(settings.SYMBOL_LEVERAGE_MAP)
                base_lev = leverage_for(
                    symbol, lev_map, settings.DEFAULT_LEVERAGE, settings.MIN_LEVERAGE
                )
                effective_lev = max(1, int(float(base_lev) * leverage_mult))

            # ── Fetch full account snapshot ──
            acc = self.client.account()
            avail         = float(acc.get("availableBalance",    0.0))
            total_wallet  = float(acc.get("totalWalletBalance",  0.0))
            total_maint   = float(acc.get("totalMaintMargin",    0.0))
            total_initial = float(acc.get("totalInitialMargin",  0.0))

            # ── Compute margin required for this order ──
            margin_required = budget_usdt / max(1, effective_lev)

            # ── Rich structured log (appears before every live entry attempt) ──
            _exec_logger.info(
                f"[MARGIN_AUDIT] {symbol}: "
                f"wallet={total_wallet:.2f} avail={avail:.2f} "
                f"maint={total_maint:.2f} initial_margin={total_initial:.2f} | "
                f"order: leverage={effective_lev}x notional={budget_usdt:.2f} "
                f"margin_required={margin_required:.2f} "
                f"pct_of_avail={margin_required / max(avail, 0.01) * 100:.1f}%"
            )

            # ── Hard block: account balance too low ──
            if avail < MIN_NOTIONAL:
                return ExecResult(
                    status="INSUFFICIENT_MARGIN",
                    details={
                        "symbol": symbol, "signal": signal,
                        "available_balance": avail, "budget_notional": budget_usdt,
                        "margin_required": margin_required,
                        "hint": "Add funds to your Binance account.",
                    },
                    success=False,
                    error=f"[PREFLIGHT] {symbol}: account balance too low ({avail:.2f} USDT). Add funds."
                )

            # ── Affordability check: cap notional with 5% safety buffer ──
            max_safe_margin = avail * MARGIN_SAFETY_BUFFER
            if margin_required > max_safe_margin:
                capped_notional = max_safe_margin * effective_lev
                _exec_logger.warning(
                    f"[MARGIN_AUDIT] {symbol}: margin_required={margin_required:.2f} > "
                    f"max_safe_margin={max_safe_margin:.2f} "
                    f"(avail={avail:.2f} x {MARGIN_SAFETY_BUFFER:.0%}). "
                    f"Capping notional from {budget_usdt:.2f} to {capped_notional:.2f} USDT."
                )
                budget_usdt = capped_notional
                margin_required = max_safe_margin

        except Exception as bal_err:
            _exec_logger.warning(f"[MARGIN_AUDIT] {symbol}: Could not check balance: {bal_err}. Proceeding cautiously.")

        # Internal sizing call — budget_usdt is NOTIONAL, _size_qty handles leverage internally
        qty, details = self._size_qty(symbol, budget_usdt, leverage_mult, sl_price=sl_price, leverage_override=leverage_override)

        if qty <= 0:
             return ExecResult(
                status="NO_TRADE_INVALID_QTY",
                details={"symbol": symbol, "signal": signal, "reason": "qty_zero_or_min_notional", "details": details},
                success=False,
                error="Invalid quantity or below minimum notional"
            )

        side_enum = Side.BUY if signal == "BUY" else Side.SELL

        # Calculate and log expected slippage
        trade_price = float(details.get("price", 0.0))
        if trade_price <= 0:
            trade_price = float(self.client.get_prices([symbol]).get(symbol, 0.0))
            
        notional = float(qty) * trade_price
        expected_slippage_pct = self.estimate_slippage(notional)
        expected_slippage_usdt = notional * expected_slippage_pct
        
        # 🚀 Phase 3: Apply slippage assumption to sizing (Execution Cost Realism)
        # Deduct expected slippage from the usable budget before calculating final quantity
        effective_notional = notional - expected_slippage_usdt
        effective_qty = effective_notional / trade_price
        # Re-quantize to step size (we just use the original qty if it drops below min, 
        # but for true impact we should request slightly less qty to accommodate the cost)
        # For simplicity, we just clamp qty downwards if slippage is high.
        if expected_slippage_pct > 0.001:  # If slippage > 10 bps, actively reduce size
             qty = effective_qty
             import logging as _log
             _log.getLogger(__name__).info(f"[COST REALISM] {symbol}: High expected slippage ({expected_slippage_pct*100:.3f}%). Reduced entry qty to {qty} to absorb cost.")

        import logging as _log
        _log.getLogger(__name__).info(
            f"[SLIPPAGE] {symbol}: Estimated market impact for {notional:.2f} USDT notional "
            f"is {expected_slippage_pct*100:.3f}% ({expected_slippage_usdt:.2f} USDT cost)"
        )
        exact_sized_notional = float(qty) * float(trade_price or 0.0)
        if _ep is not None and _ep_lock_acquired:
            _ep.mark_sized(
                self.bot_instance_id,
                symbol,
                _ep_side,
                sized_notional=exact_sized_notional,
                sized_qty=float(qty),
                reference_price=float(trade_price or 0.0),
                max_exposure_limit=self._configured_max_exposure(current_equity),
            )
            _exact_effective_exposure = _ep.get_effective_exposure(self.bot_instance_id, symbol)
            _max_exposure = self._configured_max_exposure(current_equity)
            if _max_exposure > 0 and _exact_effective_exposure > _max_exposure:
                _ep.record_exposure_blocked(
                    self.bot_instance_id,
                    symbol,
                    _ep_side,
                    reason="exposure_limit_exceeded_after_sizing",
                    max_exposure_limit=_max_exposure,
                    protected_exposure=_exact_effective_exposure,
                    intended_notional=float(usdt),
                    sized_notional=exact_sized_notional,
                    reference_price=float(trade_price or 0.0),
                    client_order_id=_ep_cid,
                    intent_key=_ep_intent_key,
                    cycle_id=cycle_id,
                    state="PENDING_OPEN",
                    submit_state="NOT_SUBMITTED",
                )
                _ep.mark_failed(
                    self.bot_instance_id,
                    symbol,
                    _ep_side,
                    reason="exposure_limit_exceeded_after_sizing",
                )
                _ep_lock_acquired = False
                return ExecResult(
                    status="EXPOSURE_LIMIT_EXCEEDED",
                    details={
                        "symbol": symbol,
                        "effective_exposure": _exact_effective_exposure,
                        "new_notional": exact_sized_notional,
                        "max_notional": _max_exposure,
                    },
                    success=False,
                    error=f"[EXPOSURE_GUARD] {symbol}: exact sized exposure {_exact_effective_exposure:.2f} exceeds max {_max_exposure:.2f}",
                )


        # 🚀 Phase 1: Stale Data Detection
        # We verify the last kline timestamp is within the expected interval buffer.
        freshness_interval = str(getattr(self, "market_data_interval", None) or settings.DEFAULT_INTERVAL or "1m")
        freshness_buffer_ms = int(getattr(settings, "EXECUTION_STALE_DATA_BUFFER_MS", 180000) or 180000)
        try:
            verify_attempts = max(1, int(getattr(settings, "EXECUTION_STALE_DATA_VERIFY_ATTEMPTS", 2) or 2))
            # Support both adapter (get_klines) and raw client (klines)
            if hasattr(self.client, "get_klines"):
                recent_klines = self.client.get_klines(symbol=symbol, interval=freshness_interval, limit=2)
            else:
                # Fallback to raw binance client method name
                recent_klines = self.client.klines(symbol=symbol, interval=freshness_interval, limit=2)
            
            if recent_klines is not None:
                now_ms = int(time.time() * 1000)
                freshness = _kline_staleness(
                    recent_klines,
                    interval=freshness_interval,
                    now_ms=now_ms,
                    buffer_ms=freshness_buffer_ms,
                )
                for _attempt in range(1, verify_attempts):
                    if freshness["ok"]:
                        break
                    time.sleep(0.2)
                    if hasattr(self.client, "get_klines"):
                        retry_klines = self.client.get_klines(symbol=symbol, interval=freshness_interval, limit=2)
                    else:
                        retry_klines = self.client.klines(symbol=symbol, interval=freshness_interval, limit=2)
                    freshness = _kline_staleness(
                        retry_klines,
                        interval=freshness_interval,
                        now_ms=int(time.time() * 1000),
                        buffer_ms=freshness_buffer_ms,
                    )
                if not freshness["ok"]:
                     _log.getLogger(__name__).error(
                         "[STALE DATA] %s: interval=%s last_close=%s now=%s age=%sms threshold=%sms reason=%s. Holding entry.",
                         symbol,
                         freshness.get("interval"),
                         freshness.get("close_time_ms"),
                         now_ms,
                         freshness.get("age_ms"),
                         freshness.get("threshold_ms"),
                         freshness.get("reason"),
                     )
                     # Pre-submit failure — release entry lock so the next valid cycle can proceed
                     if _ep is not None and _ep_lock_acquired:
                         _ep.mark_failed(self.bot_instance_id, symbol, _ep_side,
                                         reason="stale_data_pre_submit")
                     return ExecResult(
                         status="STALE_DATA_DETECTED",
                         details={
                             "symbol": symbol,
                             "signal": signal,
                             "freshness": freshness,
                         },
                         success=False,
                         error="Stale market data detected. Entry suspended."
                     )
        except Exception as stale_err:
             _log.getLogger(__name__).warning(f"[STALE CHECK] {symbol}: Could not verify kline freshness: {stale_err}")

        # ✅ C) Place Entry Order
        # Use the deterministic clientOrderId already computed for the entry-protection lock
        # (if entry protection is active) so the exchange can deduplicate retry submits.
        # Fall back to a fresh time-bucketed ID when entry protection is not available.
        if _ep is not None and _ep_lock_acquired:
            _client_order_id = _ep_cid  # type: ignore[possibly-undefined]
        else:
            _, _client_order_id = self._build_entry_idempotency(
                symbol=symbol,
                side=_ep_side,
                usdt=float(usdt),
                sl_price=float(sl_price or 0.0),
                tp_price=float(tp_price or 0.0),
            )
        req = OrderRequest(
            symbol=symbol,
            side=side_enum,
            type=OrderType.MARKET,
            qty=Decimal(str(qty)),
            leverage=Decimal(str(details.get("leverage", 1))),
            reduce_only=False,
            client_order_id=_client_order_id,
        )
        if _ep is not None and _ep_lock_acquired:
            _ep.mark_submit_prepared(
                self.bot_instance_id,
                symbol,
                _ep_side,
                submitted_notional=float(qty) * float(trade_price or 0.0),
                submitted_qty=float(qty),
                reference_price=float(trade_price or 0.0),
                max_exposure_limit=self._configured_max_exposure(current_equity),
            )

        # ── Invariant: from this point on, exactly ONE of the three entry-protection
        # transitions (confirmed / uncertain / failed) must execute.
        # We wrap the entire place→protection chain so pre-submit exceptions call
        # mark_failed() and post-submit timeouts call mark_uncertain().
        _place_order_dispatched = False
        try:
            entry_order = self.client.place_order(req)
            _place_order_dispatched = True
            if _ep is not None and _ep_lock_acquired:
                _ep.mark_submit_confirmed(
                    self.bot_instance_id,
                    symbol,
                    _ep_side,
                    broker_order_id=getattr(entry_order, "broker_order_id", None),
                    max_exposure_limit=self._configured_max_exposure(current_equity),
                )
            _log.getLogger(__name__).info(f"[ATOMIC CHAIN] {symbol}: (1/2) Entry order filled. Order ID: {entry_order.broker_order_id}")

        except Exception as order_err:
            err_str = str(order_err)
            if _place_order_dispatched:
                if _ep is not None and _ep_lock_acquired:
                    _ep.mark_submit_unknown(
                        self.bot_instance_id,
                        symbol,
                        _ep_side,
                        reason=f"post_submit_exception: {err_str[:120]}",
                        max_exposure_limit=self._configured_max_exposure(current_equity),
                    )
                _log.getLogger(__name__).warning(
                    "[SUBMIT_UNCERTAIN] %s: exception after order dispatch; lock held for reconciliation. Error=%s",
                    symbol,
                    err_str,
                )
                return ExecResult(
                    status="SUBMIT_UNCERTAIN",
                    details={"symbol": symbol, "signal": signal, "side": _ep_side, "error": err_str},
                    success=False,
                    error=f"[SUBMIT_UNCERTAIN] {symbol}: exception after order dispatch - position may exist.",
                )
            # -2019: Margin is insufficient  ← NOT a circuit-breaker event, just low balance
            # -2018: Balance is insufficient for requested order  ← same
            if '"code":-2019' in err_str or '"code":-2018' in err_str:
                # PRE-SUBMIT hard failure — order never left the bot
                if _ep is not None and _ep_lock_acquired:
                    _ep.mark_failed(self.bot_instance_id, symbol, _ep_side,
                                    reason="insufficient_margin")
                _log.getLogger(__name__).warning(
                    f"[MARGIN] {symbol}: Binance -2019/-2018 margin rejection AFTER preflight. "
                    f"Submitted notional={budget_usdt:.2f} (orig={usdt:.2f}) USDT. "
                    f"Error: {err_str}"
                )
                return ExecResult(
                    status="INSUFFICIENT_MARGIN",
                    details={
                        "symbol": symbol,
                        "signal": signal,
                        "notional_submitted": budget_usdt,
                        "original_notional": usdt,
                        "error": err_str,
                        "hint": "Reduce fixed_size_usdt allocation or add funds."
                    },
                    success=False,
                    error=f"[BINANCE-2019] {symbol}: insufficient margin for notional={budget_usdt:.2f}. Reduce allocation."
                )
            # Classify error for Safe Mode Explicit Logging
            err_lower = err_str.lower()
            is_timeout = "timeout" in err_lower or "read timeout" in err_lower
            if is_timeout:
                err_class = "Timeout"
            elif "reject" in err_lower or "invalid" in err_lower:
                err_class = "Order Rejection"
            else:
                err_class = "Protocol"

            exchange_err = ExchangeError(f"Unexpected exchange error placing order ({err_class}): {err_str}")
            try:
                self.circuit.record_error(exchange_err)
                if self.circuit.is_tripped():
                    _log.getLogger(__name__).critical(f"[SAFE MODE TRIGGERED] {symbol}: Circuit breaker halted due to excessive '{err_class}' errors.")
            except Exception:
                pass

            if is_timeout:
                # POST-SUBMIT UNCERTAIN: The TCP request was dispatched; the exchange
                # may have received and filled it.  We cannot retry or assume failure.
                # Transition to OPEN_UNCERTAIN — lock stays held until reconciliation.
                if _ep is not None and _ep_lock_acquired:
                    _ep.mark_submit_unknown(
                        self.bot_instance_id, symbol, _ep_side,
                        reason=f"timeout_after_place_order: {err_str[:120]}",
                        max_exposure_limit=self._configured_max_exposure(current_equity),
                    )
                _log.getLogger(__name__).warning(
                    "[SUBMIT_UNCERTAIN] %s: Timeout after place_order() — order may "
                    "have reached exchange. Returning SUBMIT_UNCERTAIN (lock held).",
                    symbol,
                )
                return ExecResult(
                    status="SUBMIT_UNCERTAIN",
                    details={"symbol": symbol, "signal": signal, "side": _ep_side, "error": err_str},
                    success=False,
                    error=f"[SUBMIT_UNCERTAIN] {symbol}: timeout after order dispatch — position unknown.",
                )

            # All other exceptions are pre-submit failures — lock released
            if _ep is not None and _ep_lock_acquired:
                _ep.mark_failed(self.bot_instance_id, symbol, _ep_side,
                                reason=f"{err_class.lower()}: {err_str[:120]}")
            raise exchange_err from order_err
        
        # ✅ D) Place Protection (Separate)
        try:
            # Calculate Prices
            entry_px = float(entry_order.avg_fill_price or self.client.get_prices([symbol]).get(symbol, 0))
            
            # Section 3: Slippage Log
            try:
                if trade_price > 0 and entry_px > 0:
                    realized_slippage_pct = abs(entry_px - trade_price) / trade_price
                    _log.getLogger(__name__).info(f"[SLIPPAGE REALIZED] {symbol}: Expected Px: {trade_price:.4f}, Actual Px: {entry_px:.4f}, Realized Slippage: {realized_slippage_pct*100:.3f}%")
            except Exception as slp_err:
                _log.getLogger(__name__).warning(f"[SLIPPAGE REALIZED] {symbol}: Unable to calculate slippage: {slp_err}")
                
            # Fallback to defaults if not provided dynamically by PolicyEngine/Orchestrator
            if sl_price is None or sl_price <= 0.0:
                sl_pct = settings.STOP_LOSS_PCT / 100.0
                if signal == "BUY":
                    sl_price = entry_px * (1.0 - sl_pct)
                else:
                    sl_price = entry_px * (1.0 + sl_pct)
                    
            if tp_price is None or tp_price <= 0.0:
                tp_pct = settings.TAKE_PROFIT_PCT / 100.0
                if signal == "BUY":
                    tp_price = entry_px * (1.0 + tp_pct)
                else:
                    tp_price = entry_px * (1.0 - tp_pct)
                
            try:
                from app.exchange.binance.filters import normalize_protection_price, _tick
                try:
                    tick_size = _tick(symbol)
                except Exception:
                    tick_size = 0.0001
                
                pos_side_str = "LONG" if side_enum == Side.BUY else "SHORT"
                
                # Normalize BEFORE creating ProtectionRequest
                final_sl = normalize_protection_price(sl_price, tick_size, pos_side_str, "SL")
                final_tp = normalize_protection_price(tp_price, tick_size, pos_side_str, "TP")
            except Exception as norm_err:
                _log.getLogger(__name__).warning(f"Normalization failed for {symbol}: {norm_err}. Falling back to float string.")
                # If we cannot get tick size, we should still provide an exact string representation, not a float with rounding artifacts.
                # However, since tick size is unknown, we will quantize to 5 places as a safe default for crypto.
                safe_quant = Decimal("1.00000")
                final_sl = str(Decimal(str(sl_price)).quantize(safe_quant))
                final_tp = str(Decimal(str(tp_price)).quantize(safe_quant))

            prot_req = ProtectionRequest(
                symbol=symbol,
                position_side=side_enum, # Existing position side
                qty=Decimal(str(qty)),
                sl_price=final_sl,
                tp_price=final_tp
            )
            
            prot_res = self.client.place_protection(prot_req)

            # ── S1: Fail closed — treat non-success status identically to an exception ──
            _entry_prot_status = getattr(prot_res, "status", None)
            if _entry_prot_status != "success":
                raise RuntimeError(
                    f"[SEV1-S1] place_protection returned non-success: "
                    f"status={_entry_prot_status} "
                    f"sl_id={getattr(prot_res, 'sl_order_id', None)} "
                    f"tp_id={getattr(prot_res, 'tp_order_id', None)} "
                    f"error={getattr(prot_res, 'error', None)}"
                )

            # ── S7: Post-entry audit log (synchronous verify REMOVED) ─────────────────
            # IMPORTANT: Do NOT call ensure_protection here synchronously.
            # Rationale: Binance has a ~1–3 second propagation lag before a just-placed
            # algo order appears on GET /fapi/v1/openAlgoOrders. Calling ensure_protection
            # immediately after place_protection always sees has_sl=False (order not yet
            # propagated), fires NAKED_POSITION_ALERT, attempts a repair re-place, which
            # may also fail, returning placement_failed → RuntimeError → rollback close.
            # This was causing every position to be force-closed within ~1 second of entry.
            #
            # Correctness invariant is maintained by:
            #   S1: place_protection returned status="success" (guaranteed above)
            #   15s heartbeat: ensure_protection confirms broker-visibility after propagation
            _sl_id = getattr(prot_res, "sl_order_id", None)
            _tp_id = getattr(prot_res, "tp_order_id", None)
            import logging as _logging_mod
            _logging_mod.getLogger(__name__).info(
                f"[ATOMIC CHAIN] {symbol}: (2/2) place_protection confirmed success "
                f"(sl_order_id={_sl_id} tp_order_id={_tp_id}). "
                f"Broker-visibility will be confirmed by 15s heartbeat. Chain complete."
            )


        except Exception as protect_err:
            _log.getLogger(__name__).error(f"[ATOMIC CHAIN] {symbol}: (2/2) FAILED to place SL/TP protection! Force closing orphaned entry. Error: {protect_err}")
            
            try:
                self.circuit.record_error(protect_err)
            except Exception:
                pass

            try:
                if hasattr(self.client, "close_position_market"):
                    self.client.close_position_market(symbol)
                else:
                    self.client.close_position(symbol)
                _log.getLogger(__name__).info(f"[ATOMIC GUARANTEE BROKEN] Rollback successful. Successfully force closed orphaned entry. Capital protected.")
            except Exception as close_err:
                _log.getLogger(__name__).critical(f"[ATOMIC CHAIN] {symbol}: FATAL: Failed to close orphaned entry! Position is naked! Error: {close_err}")
                from app.execution.executor import FatalIntegrationError
                raise FatalIntegrationError(f"FATAL: Orphaned position for {symbol}. Entry close failed! Halting system.") from close_err
            
            # Protection failed: entry was rolled back → entry is now flat → release lock
            if _ep is not None and _ep_lock_acquired:
                _ep.mark_failed(self.bot_instance_id, symbol, _ep_side,
                                reason="protection_failed_entry_rolled_back")
            return ExecResult(
                status="PROTECTION_FAILED_ENTRY_CLOSED",
                details={
                    "symbol": symbol,
                    "signal": signal,
                    "error": str(protect_err),
                    "action": "Force closed orphaned entry to prevent catastrophic risk. Atomic transaction rolled back."
                },
                success=False,
                error="Failed to place Stop-Loss. Entry was closed immediately to eliminate un-bracketed risk."
            )

        # ── Success: entry is live — transition to OPEN_CONFIRMED ──
        if _ep is not None and _ep_lock_acquired:
            _filled_qty = float(getattr(entry_order, "qty_filled", 0.0) or 0.0)
            _filled_px = float(getattr(entry_order, "avg_fill_price", 0.0) or 0.0)
            _filled_notional = (_filled_qty * _filled_px) if (_filled_qty > 0 and _filled_px > 0) else None
            _ep.mark_confirmed(
                self.bot_instance_id,
                symbol,
                _ep_side,
                filled_notional=_filled_notional,
                filled_qty=_filled_qty if _filled_qty > 0 else None,
                max_exposure_limit=self._configured_max_exposure(current_equity),
            )

        normalized = self._normalize_order(entry_order.model_dump(), symbol, signal, "MARKET", qty)

        return ExecResult(
            status="ORDER_PLACED",
            details={
                "symbol": symbol,
                "signal": signal,
                "side": signal,
                "ep_side": _ep_side,
                "qty": qty,
                "entry_order": entry_order.model_dump(),
                "protection": prot_res.model_dump(),
                "normalized": normalized,
                "flip_close": locals().get("close_order", None) # If we did a flip
            },
            order_id=entry_order.broker_order_id,
            success=True,
            avg_price=float(normalized.get("avg_price", 0.0))
        )

    # =========================================================================
    # TP1 PARTIAL CLOSE
    # =========================================================================

    def _normalize_partial_close_qty(
        self,
        symbol: str,
        raw_qty: float,
    ) -> tuple[float, str]:
        """
        Normalize a partial-close quantity to exchange constraints.

        Returns:
            (normalized_qty, status)
            status: "OK" | "TOO_SMALL" | "BELOW_MIN_NOTIONAL" | "FILTERS_UNAVAILABLE"
        """
        import logging
        from decimal import Decimal, ROUND_DOWN
        _log = logging.getLogger(__name__)

        try:
            filters = self.client.get_symbol_filters(symbol)
            step  = Decimal(str(filters.step_size  or "0.001"))
            min_q = Decimal(str(filters.min_qty     or "0.001"))
            min_n = Decimal(str(filters.min_notional or "5"))
        except Exception as e:
            _log.warning(f"[TP1_NORM] {symbol}: Could not get symbol filters: {e}. Skipping normalisation.")
            return (raw_qty, "FILTERS_UNAVAILABLE")

        raw_d  = Decimal(str(raw_qty))
        norm_d = (raw_d / step).to_integral_value(rounding=ROUND_DOWN) * step

        if norm_d < min_q:
            _log.warning(f"[TP1_NORM] {symbol}: norm_qty={norm_d} < min_qty={min_q} → TOO_SMALL")
            return (0.0, "TOO_SMALL")

        # Check notional: need live price
        try:
            prices = self.client.get_prices([symbol])
            px = float(prices.get(symbol, 0.0))
            if px > 0 and norm_d * Decimal(str(px)) < min_n:
                _log.warning(f"[TP1_NORM] {symbol}: notional={float(norm_d)*px:.2f} < min_notional={min_n} → BELOW_MIN_NOTIONAL")
                return (0.0, "BELOW_MIN_NOTIONAL")
        except Exception:
            pass  # price fetch failure: don't block execution; let the exchange reject if needed

        return (float(norm_d), "OK")

    def execute_tp1_partial_close(
        self,
        symbol: str,
        live_qty: float,
        position_side: str,            # "LONG" or "SHORT"
        sl_price: float,
        tp_price: float,               # This is the TP2 price (runner's target)
        sl_order_id: str | None = None,
        tp_order_id: str | None = None,
        tp1_fraction: float = 0.5,
        position_manager=None,         # PositionManager instance for phase transitions
    ) -> dict:
        """
        Execute a real partial close for TP1 via the unified adapter.

        TP1 execution lifecycle:
            SEEKING_TP1 → TP1_EXECUTING → TP1_FILLED → RUNNER_TRAILING

        Returns a structured result dict with all Section 7 observability fields.
        On success, protection is resized for the runner quantity.
        On qty-too-small, TP1 is promoted to a full close.
        """
        import logging
        from datetime import datetime, timezone
        from app.models.unified_trading import (
            OrderRequest, OrderType, Side,
            ProtectionUpdateRequest,
        )
        from app.execution.position_manager import PositionPhase

        _log = logging.getLogger(__name__)
        ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        pos_side_str = str(position_side).upper()
        exit_side = "SELL" if pos_side_str == "LONG" else "BUY"

        result_base = {
            "symbol": symbol,
            "side": pos_side_str,
            "lifecycle_state_before": None,
            "lifecycle_state_after": None,
            "live_qty_before": live_qty,
            "tp1_fraction": tp1_fraction,
            "requested_tp1_qty": 0.0,
            "normalized_tp1_qty": 0.0,
            "fill_qty": 0.0,
            "remaining_qty": live_qty,
            "reduce_only_status": "N/A",
            "protection_update_status": "N/A",
            "broker_response": None,
            "retry_count": 0,
            "failure_reason": None,
            "promoted": False,
            "skipped": False,
            "runner_qty": live_qty,
        }
        effective_mode = (self.execution_mode or settings.EXECUTION_MODE).lower()

        # ── 1. Duplicate guard ─────────────────────────────────────────────
        if position_manager:
            pos = position_manager.get_position(symbol)
            if pos:
                result_base["lifecycle_state_before"] = pos.phase.value
                safe_phases = {
                    PositionPhase.TP1_EXECUTING,
                    PositionPhase.TP1_FILLED,
                    PositionPhase.TP1_TAKEN,
                    PositionPhase.RUNNER_TRAILING,
                    PositionPhase.EXITING,
                }
                if pos.phase in safe_phases:
                    _log.info(
                        f"[TP1_PARTIAL_CLOSE] {symbol}: Duplicate trigger ignored "
                        f"(phase={pos.phase.value}). TP1_DUPLICATE_IGNORED."
                    )
                    result_base["skipped"] = True
                    result_base["failure_reason"] = "TP1_DUPLICATE_IGNORED"
                    return result_base

        # ── 2. Reconcile live qty from broker (broker is truth) ───────────
        if effective_mode != "live":
            import uuid as _paper_uuid
            norm_qty = max(0.0, float(live_qty or 0.0) * float(tp1_fraction or 0.0))
            runner_qty = max(0.0, float(live_qty or 0.0) - norm_qty)
            result_base.update(
                {
                    "requested_tp1_qty": norm_qty,
                    "normalized_tp1_qty": norm_qty,
                    "fill_qty": norm_qty,
                    "remaining_qty": runner_qty,
                    "runner_qty": runner_qty,
                    "reduce_only_status": "PAPER_REDUCE_ONLY",
                    "protection_update_status": "PAPER_PROTECTION_RESIZED" if runner_qty > 0 else "PAPER_POSITION_FLAT",
                    "broker_response": {
                        "status": "PAPER_FILLED",
                        "order_id": f"paper_order_{_paper_uuid.uuid4().hex}",
                    },
                    "lifecycle_state_after": PositionPhase.RUNNER_TRAILING.value if runner_qty > 0 else PositionPhase.FLAT.value,
                }
            )
            if position_manager:
                pos = position_manager.get_position(symbol)
                if pos:
                    pos.tp1_exec_qty = norm_qty
                    pos.tp1_fill_qty = norm_qty
                    pos.tp.tp1_hit = True
                    pos.current_qty = runner_qty
                    pos.phase = PositionPhase.RUNNER_TRAILING if runner_qty > 0 else PositionPhase.FLAT
                    position_manager._persist_lifecycle(symbol)
            return result_base

        try:
            broker_qty_raw = self.client.get_position_amt(symbol)
            broker_qty = abs(float(broker_qty_raw))
            if broker_qty < live_qty * 0.99:  # >1% divergence
                _log.warning(
                    f"[TP1_PARTIAL_CLOSE] {symbol}: QTY_DIVERGENCE "
                    f"internal={live_qty} broker={broker_qty}. Using broker qty."
                )
                live_qty = broker_qty
                result_base["live_qty_before"] = live_qty
        except Exception as e:
            _log.warning(f"[TP1_PARTIAL_CLOSE] {symbol}: Could not reconcile live qty from broker: {e}")

        # ── 3. Qty normalisation ───────────────────────────────────────────
        raw_tp1_qty = live_qty * tp1_fraction
        result_base["requested_tp1_qty"] = raw_tp1_qty

        norm_qty, norm_status = self._normalize_partial_close_qty(symbol, raw_tp1_qty)
        result_base["normalized_tp1_qty"] = norm_qty

        if norm_status in ("TOO_SMALL", "BELOW_MIN_NOTIONAL"):
            # ── Promote to full close ───────────────────────────────────────
            _log.warning(
                f"[TP1_PARTIAL_CLOSE] {symbol}: TP1_PROMOTED_TO_FULL_CLOSE "
                f"(norm_status={norm_status}, norm_qty={norm_qty})"
            )
            try:
                self.client.cancel_all_orders(symbol)
            except Exception as e:
                _log.warning(f"[TP1_PARTIAL_CLOSE] {symbol}: cancel_all_orders on promote failed: {e}")

            try:
                close_order = self.client.close_position_market(symbol)
                result_base["promoted"] = True
                result_base["broker_response"] = close_order
                result_base["lifecycle_state_after"] = "EXITING"
                result_base["failure_reason"] = f"PROMOTED_TO_FULL_CLOSE:{norm_status}"
                _log.warning(
                    f"[TP1_PARTIAL_CLOSE] {symbol}: Full close executed as promotion. "
                    f"norm_status={norm_status}"
                )
                return result_base
            except Exception as e:
                _log.error(f"[TP1_PARTIAL_CLOSE] {symbol}: Full-close promotion FAILED: {e}", exc_info=True)
                result_base["failure_reason"] = f"PROMOTE_CLOSE_FAILED:{e}"
                return result_base

        # ── 4. Set phase → TP1_EXECUTING and persist ──────────────────────
        if position_manager:
            pos = position_manager.get_position(symbol)
            if pos:
                pos.phase = PositionPhase.TP1_EXECUTING
                pos.tp1_exec_qty = norm_qty
                pos.tp1_exec_ts = ts_now
                position_manager._persist_lifecycle(symbol)

        # ── 5. Place reduce-only partial close ─────────────────────────────
        supports_ro = getattr(getattr(self.client, "capabilities", None), "supports_reduce_only", True)
        result_base["reduce_only_status"] = "ENFORCED" if supports_ro else "NOT_SUPPORTED"

        try:
            close_req = OrderRequest(
                symbol=symbol,
                side=Side.SELL if exit_side == "SELL" else Side.BUY,
                type=OrderType.MARKET,
                qty=norm_qty,
                reduce_only=supports_ro,
            )
            close_order_resp = self.client.place_order(close_req)
            result_base["broker_response"] = (
                close_order_resp.model_dump()
                if hasattr(close_order_resp, "model_dump")
                else dict(close_order_resp)
            )
            _log.info(
                f"[TP1_PARTIAL_CLOSE] {symbol}: Partial close placed "
                f"qty={norm_qty} side={exit_side} reduce_only={supports_ro}"
            )
        except Exception as e:
            _log.error(f"[TP1_PARTIAL_CLOSE] {symbol}: place_order FAILED: {e}", exc_info=True)
            # Revert phase to SEEKING_TP1 so heartbeat can retry
            if position_manager:
                pos = position_manager.get_position(symbol)
                if pos:
                    pos.phase = PositionPhase.SEEKING_TP1
                    pos.tp1_exec_qty = 0.0
                    pos.tp1_exec_ts = None
                    position_manager._persist_lifecycle(symbol)
            result_base["failure_reason"] = f"PLACE_ORDER_FAILED:{e}"
            result_base["lifecycle_state_after"] = "SEEKING_TP1"
            return result_base

        # ── 6. Reconcile fill from broker live qty ─────────────────────────
        import time as _time
        _time.sleep(0.3)  # Brief wait for exchange to process
        try:
            post_qty_raw = self.client.get_position_amt(symbol)
            post_qty = abs(float(post_qty_raw))
            fill_qty = max(0.0, live_qty - post_qty)
            runner_qty = post_qty
        except Exception as e:
            _log.warning(f"[TP1_PARTIAL_CLOSE] {symbol}: Could not reconcile post-close qty: {e}. Estimating.")
            fill_qty = norm_qty  # Best estimate
            runner_qty = max(0.0, live_qty - norm_qty)

        result_base["fill_qty"] = fill_qty
        result_base["remaining_qty"] = runner_qty
        result_base["runner_qty"] = runner_qty

        # ── 7. Update PM state → TP1_FILLED, persist fill data ────────────
        if position_manager:
            pos = position_manager.get_position(symbol)
            if pos:
                pos.phase = PositionPhase.TP1_FILLED
                pos.tp1_fill_qty = fill_qty
                pos.tp.tp1_hit = True
                pos.current_qty = runner_qty
                position_manager._persist_lifecycle(symbol)

        # ── 8. Resize protection for runner qty via update_protection ──────
        prot_status = "SKIPPED"
        if runner_qty > 0 and (sl_price or tp_price):
            try:
                prot_req = ProtectionUpdateRequest(
                    symbol=symbol,
                    position_side=pos_side_str,
                    new_sl_price=sl_price,
                    new_tp_price=tp_price,
                    qty=runner_qty,
                    old_sl_order_id=sl_order_id,
                    old_tp_order_id=tp_order_id,
                    reason="TP1_PARTIAL_CLOSE",
                )
                prot_result = self.client.update_protection(prot_req)

                # ── S2: Fail closed — partial failure must not advance lifecycle ──
                _tp1_prot_status = (
                    prot_result.get("status") if isinstance(prot_result, dict)
                    else getattr(prot_result, "status", None)
                )
                if _tp1_prot_status not in ("OK", "ok", "success"):
                    raise RuntimeError(
                        f"[SEV1-S2] update_protection TP1 partial failure: "
                        f"status={_tp1_prot_status} "
                        f"error={prot_result.get('error') if isinstance(prot_result, dict) else None}"
                    )
                prot_status = "OK"

                # Capture new order IDs for lifecycle tracking
                new_sl_id = prot_result.get("sl_order_id") if isinstance(prot_result, dict) else getattr(prot_result, "sl_order_id", None)
                new_tp_id = prot_result.get("tp_order_id") if isinstance(prot_result, dict) else getattr(prot_result, "tp_order_id", None)

                if position_manager:
                    pos = position_manager.get_position(symbol)
                    if pos:
                        pos.sl.sl_order_id = new_sl_id
                        pos.sl.tp_order_id = new_tp_id
                        pos.phase = PositionPhase.RUNNER_TRAILING
                        _log.info(
                            f"[TP1_PARTIAL_CLOSE] {symbol}: Protection resized for runner. "
                            f"sl_order_id={new_sl_id} tp_order_id={new_tp_id}"
                        )
                        position_manager._persist_lifecycle(symbol)

                _log.info(
                    f"[TP1_PARTIAL_CLOSE] {symbol}: Protection updated for runner_qty={runner_qty}"
                )
            except Exception as e:
                prot_status = f"FAILED:{e}"
                _log.critical(
                    f"[TP1_PARTIAL_CLOSE] {symbol}: PROTECTION_UPDATE_FAILED after TP1 fill. "
                    f"runner_qty={runner_qty} UNPROTECTED. Triggering emergency ensure_protection. error={e}",
                    exc_info=True,
                )
                # Emergency fallback — try ensure_protection immediately
                try:
                    self.ensure_protection(
                        symbol=symbol,
                        sl_price=sl_price,
                        tp_price=tp_price,
                        repair_source="TP1_EMERGENCY_REPAIR",
                    )
                except Exception as ep:
                    _log.critical(f"[TP1_PARTIAL_CLOSE] {symbol}: EMERGENCY ensure_protection also FAILED: {ep}")
        elif runner_qty <= 0:
            # Full fill — position is flat
            prot_status = "CANCELLED_FLAT"
            try:
                self.client.cancel_all_orders(symbol)
            except Exception:
                pass

        result_base["protection_update_status"] = prot_status
        result_base["lifecycle_state_after"] = PositionPhase.RUNNER_TRAILING.value if runner_qty > 0 else "FLAT"

        _log.info(
            f"[TP1_PARTIAL_CLOSE] {symbol}: COMPLETE "
            f"live_qty={live_qty} tp1_frac={tp1_fraction} norm_qty={norm_qty} "
            f"fill_qty={fill_qty} runner_qty={runner_qty} "
            f"reduce_only={result_base['reduce_only_status']} "
            f"prot={prot_status}"
        )
        return result_base

    # =========================================================================
    # BREAK-EVEN STOP UPDATE
    # =========================================================================



    def execute_break_even_update(
        self,
        symbol: str,
        position_side: str,          # "LONG" or "SHORT"
        runner_qty: float,
        entry_price: float,
        current_stop: float,
        sl_order_id: str | None = None,
        tp_order_id: str | None = None,
        tp2_price: float | None = None,
        position_manager=None,
        fee_buffer_mult: float = 1.2,
        taker_fee_rate: float = 0.0005,
        # FIX-D: 20% of original SL distance as minimum BE buffer
        be_sl_distance_buffer_pct: float = 0.20,
    ) -> dict:
        """
        Move the live broker stop-loss to break-even after TP1 confirmation.

        Break-even lifecycle:
            RUNNER_TRAILING / TP1_FILLED
            → BREAK_EVEN_PENDING  (update_protection sent)
            → RUNNER_TRAILING     (confirmed)

        Returns a structured result dict with all Section 8 observability fields.
        The exchange stop is only updated when be_exchange_confirmed becomes True.
        """
        import logging
        from datetime import datetime, timezone
        from app.models.unified_trading import ProtectionUpdateRequest
        from app.execution.position_manager import PositionPhase

        _log = logging.getLogger(__name__)
        ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        pos_side_str = str(position_side).upper()
        effective_mode = (self.execution_mode or settings.EXECUTION_MODE).lower()

        result = {
            "symbol": symbol,
            "side": pos_side_str,
            "lifecycle_state_before": None,
            "lifecycle_state_after": None,
            "live_qty": runner_qty,
            "entry_price": entry_price,
            "raw_break_even_price": None,
            "buffered_break_even_price": None,
            "normalized_break_even_price": None,
            "prior_stop_price": current_stop,
            "break_even_applied": False,
            "skip_reason": None,
            "protection_update_status": "N/A",
            "broker_response": None,
            "retry_count": 0,
            "failure_reason": None,
            # FIX-D observability
            "initial_stop": None,
            "sl_distance": None,
            "buffer_amount": None,
            "buffer_pct_of_sl": None,
        }

        # ── 1. Pre-condition checks ────────────────────────────────────────
        phase_before = None
        pos = None
        if position_manager:
            pos = position_manager.get_position(symbol)
            if pos:
                phase_before = pos.phase
                result["lifecycle_state_before"] = pos.phase.value

                # Idempotency: already confirmed on exchange
                if pos.sl.be_exchange_confirmed:
                    result["skip_reason"] = "BE_ALREADY_CONFIRMED"
                    _log.info(f"[BE_UPDATE] {symbol}: Skipped — be_exchange_confirmed=True already.")
                    return result

                # Phase guard: must be in post-TP1 or trailing phase
                allowed_phases = {
                    PositionPhase.TP1_FILLED,
                    PositionPhase.RUNNER_TRAILING,
                }
                if pos.phase not in allowed_phases:
                    result["skip_reason"] = f"WRONG_PHASE:{pos.phase.value}"
                    _log.info(f"[BE_UPDATE] {symbol}: Skipped — phase={pos.phase.value} not eligible.")
                    return result

                # TP1 must be truly confirmed
                if not pos.tp.tp1_hit:
                    result["skip_reason"] = "TP1_NOT_CONFIRMED"
                    _log.info(f"[BE_UPDATE] {symbol}: Skipped — tp1_hit=False.")
                    return result

        if runner_qty <= 0:
            result["skip_reason"] = "QTY_ZERO"
            _log.warning(f"[BE_UPDATE] {symbol}: Skipped — runner_qty={runner_qty}.")
            return result

        # ── 2. Broker qty reconciliation ────────────────────────────────────
        if effective_mode == "live":
            try:
                raw_broker = self.client.get_position_amt(symbol)
                broker_qty = abs(float(raw_broker))
                if broker_qty < runner_qty * 0.99:
                    _log.warning(f"[BE_UPDATE] {symbol}: QTY_DIVERGENCE internal={runner_qty} broker={broker_qty}. Using broker qty.")
                    runner_qty = broker_qty
                    result["live_qty"] = runner_qty
            except Exception as e:
                _log.warning(f"[BE_UPDATE] {symbol}: Could not reconcile live qty: {e}")

        if runner_qty <= 0:
            result["skip_reason"] = "BROKER_QTY_ZERO"
            return result

        # ── 3. BE price calculation (FIX-D) ────────────────────────────────────
        # Buffer = max(be_sl_distance_buffer_pct × original_SL_distance, fee_buffer)
        # The old fee-only buffer was ~$80 for BTC at $67k — too tight for normal
        # post-TP1 oscillation.  20% of the original SL distance gives real breathing room.
        fee_buffer = entry_price * taker_fee_rate * 2.0 * fee_buffer_mult

        # Resolve initial_stop from PositionManager if available
        initial_stop = 0.0
        if pos and pos.sl.initial_stop:
            initial_stop = float(pos.sl.initial_stop)
        sl_distance = abs(entry_price - initial_stop) if initial_stop > 0 else 0.0

        if sl_distance <= 0:
            _log.warning(
                "[BREAK_EVEN_BUFFER_MISSING_ORIGINAL_SL] %s: "
                "initial_stop=%.6f — falling back to fee-only buffer=%.6f",
                symbol, initial_stop, fee_buffer,
            )
            buffer = fee_buffer
        else:
            sl_pct_buffer = sl_distance * be_sl_distance_buffer_pct
            buffer = max(sl_pct_buffer, fee_buffer)

        buffer_pct_of_sl = (buffer / sl_distance * 100.0) if sl_distance > 0 else 0.0
        raw_be = entry_price  # same for both sides before buffer

        if pos_side_str == "LONG":
            buffered_be = entry_price + buffer
        else:
            buffered_be = entry_price - buffer

        result["raw_break_even_price"] = entry_price
        result["buffered_break_even_price"] = buffered_be
        result["initial_stop"] = initial_stop
        result["sl_distance"] = sl_distance
        result["buffer_amount"] = buffer
        result["buffer_pct_of_sl"] = buffer_pct_of_sl

        _log.info(
            "[BREAK_EVEN_BUFFER_CALCULATED] %s: side=%s entry=%.6f initial_stop=%.6f "
            "sl_distance=%.6f buffer=%.6f (%.1f%% of SL dist) buffered_be=%.6f",
            symbol, pos_side_str, entry_price, initial_stop,
            sl_distance, buffer, buffer_pct_of_sl, buffered_be,
        )

        try:
            from app.exchange.binance.filters import normalize_protection_price, _tick
            try:
                tick = _tick(symbol)
            except Exception:
                tick = 0.0001
            norm_be = normalize_protection_price(buffered_be, tick, pos_side_str, "SL")
        except Exception as e:
            _log.warning(f"[BE_NORM] {symbol}: normalisation failed: {e}. Using raw price string.")
            from decimal import Decimal
            norm_be = str(Decimal(str(buffered_be)).quantize(Decimal("1.00000")))

        result["normalized_break_even_price"] = norm_be

        # ── 4. Never-loosen guard ───────────────────────────────────────────
        if pos_side_str == "LONG":
            would_loosen = float(norm_be) <= float(current_stop)
        else:
            would_loosen = float(norm_be) >= float(current_stop)

        if would_loosen:
            result["skip_reason"] = "BE_WOULD_LOOSEN_STOP"
            _log.info(
                f"[BE_UPDATE] {symbol}: Skipped — proposed BE stop {norm_be} would "
                f"loosen vs current {current_stop}. BE_WOULD_LOOSEN_STOP."
            )
            return result

        # ── 5. Phase → BREAK_EVEN_PENDING, persist ─────────────────────────
        if effective_mode != "live":
            import uuid as _paper_uuid

            new_sl_id = f"paper_sl_{_paper_uuid.uuid4().hex}"
            new_tp_id = tp_order_id or (f"paper_tp_{_paper_uuid.uuid4().hex}" if tp2_price else None)
            norm_be_float = float(norm_be)
            if pos:
                pos.sl.current_stop = norm_be_float
                pos.sl.break_even_price = norm_be_float
                pos.sl.is_break_even = True
                pos.sl.be_exchange_confirmed = True
                pos.sl.be_activation_ts = ts_now
                pos.sl.sl_order_id = new_sl_id
                if new_tp_id:
                    pos.sl.tp_order_id = new_tp_id
                pos.phase = PositionPhase.RUNNER_TRAILING
                position_manager._persist_lifecycle(symbol)

            result["broker_response"] = {
                "status": "PAPER_PROTECTION_UPDATED",
                "sl_order_id": new_sl_id,
                "tp_order_id": new_tp_id,
                "mode": effective_mode,
            }
            result["protection_update_status"] = "PAPER_PROTECTION_UPDATED"
            result["break_even_applied"] = True
            result["lifecycle_state_after"] = PositionPhase.RUNNER_TRAILING.value
            return result

        pre_update_phase = phase_before  # capture for rollback
        if pos:
            pos.phase = PositionPhase.BREAK_EVEN_PENDING
            pos.sl.be_activation_ts = ts_now
            position_manager._persist_lifecycle(symbol)

        # ── 6. call update_protection ──────────────────────────────────────
        try:
            prot_req = ProtectionUpdateRequest(
                symbol=symbol,
                position_side=pos_side_str,
                new_sl_price=norm_be,
                new_tp_price=tp2_price,
                qty=runner_qty,
                old_sl_order_id=sl_order_id,
                old_tp_order_id=tp_order_id,
                reason="BREAK_EVEN",
            )
            prot_result = self.client.update_protection(prot_req)
            result["broker_response"] = (
                prot_result if isinstance(prot_result, dict)
                else prot_result.model_dump() if hasattr(prot_result, "model_dump")
                else {"raw": str(prot_result)}
            )

            # ── S2: Fail closed — partial failure must not set be_exchange_confirmed ──
            _be_prot_status = (
                prot_result.get("status") if isinstance(prot_result, dict)
                else getattr(prot_result, "status", None)
            )
            if _be_prot_status not in ("OK", "ok", "success"):
                raise RuntimeError(
                    f"[SEV1-S2] update_protection BE partial failure: "
                    f"status={_be_prot_status} "
                    f"error={prot_result.get('error') if isinstance(prot_result, dict) else None}"
                )
            result["protection_update_status"] = "OK"

            new_sl_id = prot_result.get("sl_order_id") if isinstance(prot_result, dict) else getattr(prot_result, "sl_order_id", None)
            new_tp_id = prot_result.get("tp_order_id") if isinstance(prot_result, dict) else getattr(prot_result, "tp_order_id", None)

        except Exception as e:
            # ── Rollback on failure ─────────────────────────────────────────
            result["protection_update_status"] = f"FAILED:{e}"
            result["failure_reason"] = f"UPDATE_PROTECTION_FAILED:{e}"

            if pos and pre_update_phase is not None:
                pos.phase = pre_update_phase
                pos.sl.be_activation_ts = None
                # be_exchange_confirmed stays False — exchange not updated
                position_manager._persist_lifecycle(symbol)

            result["lifecycle_state_after"] = pre_update_phase.value if pre_update_phase else None
            _log.critical(
                f"[BE_UPDATE] {symbol}: update_protection FAILED — "
                f"phase reverted to {pre_update_phase}. "
                f"be_exchange_confirmed=False. error={e}",
                exc_info=True,
            )
            return result

        # ── 7. Confirm and advance to RUNNER_TRAILING ──────────────────────
        if pos:
            pos.sl.current_stop = norm_be
            pos.sl.break_even_price = norm_be
            pos.sl.is_break_even = True
            pos.sl.be_exchange_confirmed = True
            pos.sl.sl_order_id = new_sl_id
            pos.sl.tp_order_id = new_tp_id
            pos.phase = PositionPhase.RUNNER_TRAILING
            position_manager._persist_lifecycle(symbol)

        result["break_even_applied"] = True
        result["lifecycle_state_after"] = PositionPhase.RUNNER_TRAILING.value

        _log.info(
            "[BREAK_EVEN_BUFFER_APPLIED] %s: COMPLETE break_even_applied=True "
            "entry=%.6f raw_be=%.6f buffered_be=%.6f norm_be=%s "
            "buffer_$=%.6f buffer_pct_sl=%.1f%% prior_stop=%.6f "
            "runner_qty=%s side=%s new_sl_id=%s new_tp_id=%s",
            symbol, entry_price, raw_be, buffered_be, norm_be,
            buffer, buffer_pct_of_sl, current_stop,
            runner_qty, pos_side_str, new_sl_id, new_tp_id,
        )
        return result

    def ensure_protection(
        self,
        symbol: str,
        signal: str | None = None,
        qty: float | None = None,
        sl_price: float | None = None,
        tp_price: float | None = None,
        repair_source: str = "CALLER",  # "PERSISTED", "CALLER", "FALLBACK_COMPUTED"
    ) -> dict:
        """
        Heartbeat protection repair — uses the UNIFIED adapter (client.place_protection).
        Does NOT call Binance-specific place_protection_orders().
        Prefers persisted/original stop values over live-price re-computation.

        repair_source audit values:
            PERSISTED         — sl_price/tp_price came from DB lifecycle state
            CALLER            — caller explicitly provided sl_price and tp_price
            FALLBACK_COMPUTED — neither available; computed from live price as last resort
        """
        import logging
        from app.models.unified_trading import ProtectionRequest, Side
        _log = logging.getLogger(__name__)

        try:
            pos_amt = self.client.get_position_amt(symbol)
            _log.info(f"[PROTECTION CHECK] {symbol}: pos_amt={pos_amt}")
        except Exception as e:
            _log.warning(f"[PROTECTION CHECK] {symbol}: Failed to fetch position_amt: {e}")
            return {"status": "error", "reason": str(e)}

        # If flat → cancel leftovers
        if abs(float(pos_amt)) < 1e-12:
            _log.info(f"[PROTECTION CHECK] {symbol}: Position is flat, cancelling any leftover orders.")
            try:
                self.client.cancel_all_orders(symbol)
            except Exception as e:
                _log.warning(f"[PROTECTION CHECK] {symbol}: cancel_all_orders (flat) failed: {e}")
            return {"status": "flat"}

        # Derive defaults from broker position
        position = "LONG" if float(pos_amt) > 0 else "SHORT"
        if signal is None:
            signal = "BUY" if float(pos_amt) > 0 else "SELL"
        if qty is None:
            qty = abs(float(pos_amt))

        # ------------------------------------------------------------------
        # SL/TP source priority:
        #   1. Caller passed sl_price + tp_price (repair_source=CALLER)
        #   2. Caller passed neither → FALLBACK_COMPUTED with audit warning
        # ------------------------------------------------------------------
        if sl_price is None or tp_price is None:
            repair_source = "FALLBACK_COMPUTED"
            px = float(self.client.last_price(symbol))
            sl_pct = settings.STOP_LOSS_PCT / 100.0
            tp_pct = settings.TAKE_PROFIT_PCT / 100.0
            _log.critical(
                f"[PROTECTION CHECK] {symbol}: No persisted SL/TP provided — "
                f"COMPUTING from live price={px:.4f} (repair_source=FALLBACK_COMPUTED). "
                f"This is sub-optimal. Caller should pass persisted sl/tp."
            )
            if float(pos_amt) > 0:
                sl_price = px * (1.0 - sl_pct)
                tp_price = px * (1.0 + tp_pct)
            else:
                sl_price = px * (1.0 + sl_pct)
                tp_price = px * (1.0 - tp_pct)

        _log.info(
            f"[PROTECTION CHECK] {symbol}: repair_source={repair_source} "
            f"sl={sl_price:.4f} tp={tp_price:.4f}"
        )

        # ── S3: Check BOTH regular orders AND algo orders ─────────────────────────────
        # Protection is placed via /fapi/v1/algoOrder (not /fapi/v1/order).
        # Algo orders appear at /fapi/v1/algoOrders — a different endpoint.
        # Without this, the check always returns has_sl=False for algo-protected positions.
        opens = []
        try:
            opens = self.client.open_orders(symbol) or []
        except Exception as e:
            _log.warning(f"[PROTECTION CHECK] {symbol}: open_orders failed: {e}")

        algo_opens = []
        try:
            # Use get_algo_orders helper if available, else fall back to _signed_get
            if hasattr(self.client, "get_algo_orders"):
                try:
                    algo_opens = self.client.get_algo_orders(symbol, raise_on_error=True) or []
                except TypeError:
                    algo_opens = self.client.get_algo_orders(symbol) or []
            else:
                algo_opens = self.client._signed_get(
                    "/fapi/v1/algoOrders", {"symbol": symbol.upper()}
                ) or []
        except Exception as e:
            _log.warning(f"[PROTECTION CHECK] {symbol}: algoOrders check failed: {e}")

        all_opens = (
            (opens if isinstance(opens, list) else [])
            + (algo_opens if isinstance(algo_opens, list) else [])
        )

        # ── Robust SL/TP detection for both regular and algo orders ──────────────────
        # Regular open orders:  o["type"] == "STOP_MARKET" | "TAKE_PROFIT_MARKET"
        # Binance algo orders:  o["algoType"] may be "STOP"/"SL"/"TP"/"CONDITIONAL"
        #                       or o["type"] may be "STOP_MARKET"/"TAKE_PROFIT_MARKET"
        #                       These use "triggerPrice" instead of "stopPrice".
        # We treat ANY algo order with a triggerPrice as proof of protection, since
        # we only ever place algo orders for SL/TP (never for entry orders).
        has_sl = False
        has_tp = False
        sl_order_id = None
        tp_order_id = None

        regular_types = {o.get("type") for o in (opens if isinstance(opens, list) else []) if isinstance(o, dict)}
        for o in opens if isinstance(opens, list) else []:
            if not isinstance(o, dict):
                continue
            o_type = (o.get("type") or "").upper()
            oid = o.get("orderId") or o.get("clientOrderId")
            if o_type in ("STOP_MARKET", "STOP", "STOP_LOSS"):
                has_sl = True
                sl_order_id = sl_order_id or oid
            if o_type in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
                has_tp = True
                tp_order_id = tp_order_id or oid

        # Check algo orders — if ANY algo order exists with a trigger price, protection is present
        algo_list = algo_opens if isinstance(algo_opens, list) else []
        for o in algo_list:
            if not isinstance(o, dict):
                continue
            o_type = (o.get("type") or o.get("algoType") or o.get("planType") or "").upper()
            o_side = (o.get("side") or "").upper()
            has_trigger = bool(o.get("triggerPrice") or o.get("stopPrice"))
            oid = o.get("algoId") or o.get("orderId") or o.get("clientAlgoId") or o.get("clientOrderId")

            # Type-based detection (handles CONDITIONAL/STOP/TP algo order types)
            if o_type in ("STOP_MARKET", "STOP", "STOP_LOSS", "SL", "CONDITIONAL"):
                if has_trigger:
                    has_sl = True
                    sl_order_id = sl_order_id or oid
            if o_type in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT", "TP"):
                if has_trigger:
                    has_tp = True
                    tp_order_id = tp_order_id or oid

            # Side-based fallback: any triggered sell algo order = likely SL or TP for a long
            # The first one detected = SL (lower trigger), second = TP (higher trigger)
            # This is a safe heuristic since we place exactly 2 algo orders per position
            if has_trigger and not (has_sl and has_tp):
                # If we can't determine type, count by side
                if o_side == "SELL" and not has_sl:
                    has_sl = True  # conservative: mark first sell-side as SL
                    sl_order_id = sl_order_id or oid
                elif o_side == "BUY" and not has_sl:
                    has_sl = True
                    sl_order_id = sl_order_id or oid

        # Binance openAlgoOrders can return type=None for closePosition conditional
        # orders. In that case, classify the two trigger orders by their relation to
        # entry price instead of treating same-side SELL/BUY orders as ambiguous.
        if algo_list and not (has_sl and has_tp):
            try:
                info = self.client.get_position_info(symbol)
                entry_price = float(info.get("entryPrice", 0.0) or 0.0) if info else 0.0
            except Exception:
                entry_price = 0.0

            if entry_price > 0:
                for o in algo_list:
                    if not isinstance(o, dict):
                        continue
                    try:
                        trigger = float(o.get("triggerPrice") or o.get("stopPrice") or 0.0)
                    except Exception:
                        trigger = 0.0
                    if trigger <= 0:
                        continue
                    oid = o.get("algoId") or o.get("orderId") or o.get("clientAlgoId") or o.get("clientOrderId")
                    if position == "LONG":
                        if trigger < entry_price and not has_sl:
                            has_sl = True
                            sl_order_id = sl_order_id or oid
                        elif trigger > entry_price and not has_tp:
                            has_tp = True
                            tp_order_id = tp_order_id or oid
                    else:
                        if trigger > entry_price and not has_sl:
                            has_sl = True
                            sl_order_id = sl_order_id or oid
                        elif trigger < entry_price and not has_tp:
                            has_tp = True
                            tp_order_id = tp_order_id or oid

        _log.info(
            f"[PROTECTION CHECK] {symbol}: Open order types found: regular={regular_types} "
            f"(regular={len(opens) if isinstance(opens, list) else 0}, "
            f"algo={len(algo_list)})"
        )
        _log.info(
            f"[PROTECTION CHECK] {symbol}: has_sl={has_sl}, has_tp={has_tp} "
            f"(open_orders + algoOrders), position={position}"
        )

        # ── Source 3: Position-level TP/SL (set via Binance UI or position-risk endpoint) ──────
        # Positions protected via the Binance UI store SL as positionRisk.stopPrice.
        # These are invisible to open_orders() and algoOrders, causing the parser to
        # emit has_sl=False (or has_tp=False) for fully-protected UI-managed positions.
        # Detection: if stopPrice is nonzero AND on the correct side of entry, count as has_sl.
        try:
            _pos_risk_info = self.client.get_position_info(symbol)
            if _pos_risk_info and isinstance(_pos_risk_info, dict):
                _pr_stop = float(_pos_risk_info.get("stopPrice", 0) or 0)
                _pr_entry = float(_pos_risk_info.get("entryPrice", 0) or 0)
                if abs(_pr_stop) > 1e-9 and _pr_entry > 0:
                    # Only count as a valid SL if the stop is on the correct side of entry
                    _pr_is_valid_sl = (
                        (position == "LONG" and _pr_stop < _pr_entry) or
                        (position == "SHORT" and _pr_stop > _pr_entry)
                    )
                    if _pr_is_valid_sl and not has_sl:
                        has_sl = True
                        _log.info(
                            "[PROTECTION CHECK] %s: position-level SL detected via positionRisk "
                            "(stopPrice=%.5f) — counting as has_sl",
                            symbol, _pr_stop,
                        )
                    elif not _pr_is_valid_sl:
                        _log.debug(
                            "[PROTECTION CHECK] %s: positionRisk stopPrice=%.5f not a valid %s SL — ignored",
                            symbol, _pr_stop, position,
                        )
        except Exception as _pr_err:
            _log.debug("[PROTECTION CHECK] %s: positionRisk SL check failed: %s", symbol, _pr_err)

        # Conservative heuristic: if has_sl=True (any source) but has_tp=False, and the
        # position has no bot-managed order IDs, there is a high probability that protection
        # is UI-configured at the position level. Log clearly to distinguish from a true
        # naked-TP situation — actual naked-TP repair will still fire if has_sl=False.
        _log.info(f"[PROTECTION CHECK] {symbol}: has_sl={has_sl}, has_tp={has_tp} (all sources), position={position}")

        # --- PROTECTION SANITY AUTO-REPAIR ---
        if has_sl and has_tp:
            entry_price = 0.0
            try:
                info = self.client.get_position_info(symbol)
                entry_price = float(info.get("entryPrice", 0.0) or 0.0)
            except Exception:
                pass

            if entry_price > 0 and not self._protection_is_sane(
                position, entry_price, float(sl_price), float(tp_price)
            ):
                _log.warning(
                    f"[PROTECTION CHECK] {symbol}: Orders exist but INSANE "
                    f"(entry={entry_price}, sl={sl_price:.4f}, tp={tp_price:.4f}). Re-placing."
                )
                try:
                    self.client.cancel_all_orders(symbol)
                except Exception as e:
                    _log.warning(f"[PROTECTION CHECK] {symbol}: cancel on insane orders failed: {e}")

                try:
                    # Use unified adapter (reduce_only=True via ProtectionRequest)
                    pos_side_str = "LONG" if signal.upper() == "BUY" else "SHORT"
                    try:
                        from app.exchange.binance.filters import normalize_protection_price, _tick
                        try:
                            tick_size = _tick(symbol)
                        except Exception:
                            tick_size = 0.0001
                        norm_sl = normalize_protection_price(sl_price, tick_size, pos_side_str, "SL")
                        norm_tp = normalize_protection_price(tp_price, tick_size, pos_side_str, "TP")
                    except Exception as e:
                        _log.warning(f"[PROTECTION CHECK] {symbol}: insane repair normalization failed: {e}")
                        from decimal import Decimal
                        norm_sl = str(Decimal(str(sl_price)).quantize(Decimal("1.00000")))
                        norm_tp = str(Decimal(str(tp_price)).quantize(Decimal("1.00000")))

                    self.tpsl_repair_attempt_total += 1
                    prot_req = ProtectionRequest(
                        symbol=symbol,
                        position_side=Side.BUY if signal.upper() == "BUY" else Side.SELL,
                        qty=qty,
                        sl_price=norm_sl,
                        tp_price=norm_tp,
                        reduce_only=True,
                    )
                    new_prot = self.client.place_protection(prot_req)
                    self.tpsl_repair_success_total += 1
                    _log.info(f"[PROTECTION CHECK] {symbol}: Re-placed (insane repair): {new_prot}")
                    return {
                        "repaired": True,
                        "reason": "WRONG_SIDE_PROTECTION",
                        "repair_source": repair_source,
                        "sl_order_id": getattr(new_prot, "sl_order_id", None),
                        "tp_order_id": getattr(new_prot, "tp_order_id", None),
                    }
                except Exception as e:
                    self.tpsl_repair_failure_total += 1
                    _log.error(f"[PROTECTION CHECK] {symbol}: FAILED to re-place protection (insane): {e}")
                    return {"status": "repair_failed", "error": str(e)}
            else:
                return {
                    "status": "ok",
                    "has_sl": True,
                    "has_tp": True,
                    "sl_order_id": sl_order_id,
                    "tp_order_id": tp_order_id,
                }

        # ── S8: Emit explicit naked-position alert ────────────────────────────────────
        self.tpsl_repair_attempt_total += 1
        _log.critical(
            "[NAKED_POSITION_ALERT] %s: Live position MISSING protection! "
            "bot_id=%s has_sl=%s has_tp=%s repair_source=%s sl=%.4f tp=%.4f",
            symbol, self.bot_instance_id, has_sl, has_tp, repair_source, sl_price, tp_price,
            extra={"bot_id": self.bot_instance_id, "symbol": symbol, "repair_source": repair_source}
        )

        # Missing protection → recreate via unified adapter
        _log.debug(f"[PROTECTION CHECK] {symbol}: MISSING PROTECTION! Re-creating via unified adapter...")
        try:
            self.client.cancel_all_orders(symbol)
        except Exception as e:
            _log.warning(f"[PROTECTION CHECK] {symbol}: cancel_all_orders before re-place failed: {e}")

        try:
            pos_side_str = "LONG" if signal.upper() == "BUY" else "SHORT"
            try:
                from app.exchange.binance.filters import normalize_protection_price, _tick
                try:
                    tick_size = _tick(symbol)
                except Exception:
                    tick_size = 0.0001
                norm_sl = normalize_protection_price(sl_price, tick_size, pos_side_str, "SL")
                norm_tp = normalize_protection_price(tp_price, tick_size, pos_side_str, "TP")
            except Exception as e:
                _log.warning(f"[PROTECTION CHECK] {symbol}: naked repair normalization failed: {e}")
                from decimal import Decimal
                norm_sl = str(Decimal(str(sl_price)).quantize(Decimal("1.00000")))
                norm_tp = str(Decimal(str(tp_price)).quantize(Decimal("1.00000")))

            prot_req = ProtectionRequest(
                symbol=symbol,
                position_side=Side.BUY if signal.upper() == "BUY" else Side.SELL,
                qty=qty,
                sl_price=norm_sl,
                tp_price=norm_tp,
                reduce_only=True,
            )
            new_prot = self.client.place_protection(prot_req)

            _repair_status = getattr(new_prot, "status", None)
            if _repair_status != "success":
                self.tpsl_repair_failure_total += 1
                # Repair failed — log but do NOT close the position.
                # The 15s heartbeat will retry on the next cycle.
                _log.critical(
                    "[NAKED_POSITION_ALERT] %s: REPAIR FAILED (status=%s error=%s). "
                    "Position left open — heartbeat will retry next cycle. "
                    "sl=%.4f tp=%.4f repair_source=%s",
                    symbol, _repair_status,
                    getattr(new_prot, "error", None),
                    sl_price, tp_price, repair_source,
                )
                return {"status": "repair_pending", "error": getattr(new_prot, "error", None)}

            self.tpsl_repair_success_total += 1
            _log.info(
                f"[PROTECTION CHECK] {symbol}: Protection placed (repair_source={repair_source}): "
                f"sl={sl_price:.4f} tp={tp_price:.4f}"
            )
            return {
                "status": "repaired",
                "repair_source": repair_source,
                "sl_price": float(sl_price),
                "tp_price": float(tp_price),
                "sl_order_id": getattr(new_prot, "sl_order_id", None),
                "tp_order_id": getattr(new_prot, "tp_order_id", None),
            }
        except Exception as e:
            self.tpsl_repair_failure_total += 1
            # Repair raised an exception — log and let the heartbeat retry.
            # NEVER close the position here: the position itself is valid,
            # only the protection order placement failed (transient API error).
            _log.critical(
                "[NAKED_POSITION_ALERT] %s: REPAIR EXCEPTION (%s). "
                "Position left open — heartbeat will retry next cycle. "
                "sl=%.4f tp=%.4f repair_source=%s",
                symbol, e, sl_price, tp_price, repair_source,
            )
            return {"status": "repair_pending", "error": str(e)}


    def _wait_until_flat(

        self, symbol: str, timeout_sec: float = 8.0, poll_sec: float = 0.4
    ) -> bool:
        """Return True only when Binance reports positionAmt == 0 for symbol."""
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            pos = self.client.get_position_info(symbol)
            amt = float(pos.get("positionAmt", "0") or "0") if pos else 0.0
            if abs(amt) < 1e-12:
                return True
            time.sleep(poll_sec)
        return False

    def _confirm_flat(
        self, symbol: str, timeout_s: int = 12, poll_s: float = 0.5
    ) -> bool:
        """
        Confirm position is flat.

        Priority order:
        1) Binance position_risk polling (most accurate)
        2) Generic polling via get_position_info() if available (multi-broker friendly)
        3) Fallback to get_position_amt() loop
        """
        # 1) Preferred: Binance-specific robust polling
        try:
            if hasattr(self.client, "position_risk"):
                return wait_until_flat(
                    self.client, symbol, timeout_s=timeout_s, poll_s=poll_s
                )
        except Exception:
            pass

        import time

        deadline = time.time() + float(timeout_s)

        # 2) Generic: poll get_position_info (your tests + many brokers support this style)
        if hasattr(self.client, "get_position_info"):
            while time.time() < deadline:
                try:
                    info = self.client.get_position_info(symbol)
                    amt = float(info.get("positionAmt", 0.0))
                    if abs(amt) < 1e-12:
                        return True
                except Exception:
                    pass
                time.sleep(poll_s)

        # 3) Fallback: poll get_position_amt
        while time.time() < deadline:
            try:
                amt = float(self.client.get_position_amt(symbol))
                if abs(amt) < 1e-12:
                    return True
            except Exception:
                pass
            time.sleep(poll_s)

        return False


    # ===========================================================================
    # STEP 3E — TRAILING STOP UPDATE
    # ===========================================================================



    def execute_trailing_stop_update(
        self,
        symbol: str,
        position_side: str,
        runner_qty: float,
        entry_price: float,
        current_stop: float,
        highest_since_entry: float,
        lowest_since_entry: float,
        atr: float,
        sl_order_id,
        tp_order_id,
        tp2_price,
        position_manager,
        trail_atr_mult: float = 1.2,
        min_delta_pct: float = 0.001,
        min_update_interval_s: float = 60.0,
        last_update_ts=None,
        last_trailing_stop=None,
        be_floor_price=None,
    ) -> dict:
        """
        Execute a live trailing stop update for a runner position.

        Full Section 9 observability + Section 7 safety rules + Section 4 anti-spam.
        Only tightens the stop, never loosens it, never crosses the BE floor/ceiling.
        """
        import logging
        import math
        from datetime import datetime, timezone
        from app.execution.position_manager import PositionPhase, PositionSide
        from app.models.unified_trading import ProtectionUpdateRequest

        _log = logging.getLogger(__name__)
        effective_mode = (self.execution_mode or settings.EXECUTION_MODE).lower()

        # ── Canonical result skeleton ──────────────────────────────────────────
        result = {
            "symbol": symbol,
            "side": position_side,
            "live_qty": None,
            "entry_price": entry_price,
            "highest_since_entry": highest_since_entry,
            "lowest_since_entry": lowest_since_entry,
            "atr_value": atr,
            "be_floor_price": be_floor_price,
            "raw_trailing_stop": None,
            "buffered_trailing_stop": None,
            "normalized_trailing_stop": None,
            "prior_stop_price": current_stop,
            "trailing_applied": False,
            "skip_reason": None,
            "failure_reason": None,
            "protection_update_status": None,
            "broker_response": None,
            "lifecycle_state_before": None,
            "lifecycle_state_after": None,
            "retry_count": 0,
        }

        # ── 1. ATR guard ───────────────────────────────────────────────────────
        if not atr or math.isnan(atr) or atr <= 0:
            result["skip_reason"] = "ATR_INVALID"
            _log.debug("[TRAIL] %s: ATR invalid or missing (%s) — skip", symbol, atr)
            return result

        # ── 2. Get PM state and run eligibility guards ─────────────────────────
        pos = position_manager.get_position(symbol)
        if pos is None:
            result["skip_reason"] = "NO_PM_POSITION"
            return result

        result["lifecycle_state_before"] = pos.phase.value

        if not pos.tp.tp1_hit:
            result["skip_reason"] = "TP1_NOT_CONFIRMED"
            return result

        if not pos.sl.be_exchange_confirmed:
            result["skip_reason"] = "BE_NOT_CONFIRMED"
            return result

        if pos.phase not in (PositionPhase.RUNNER_TRAILING,):
            result["skip_reason"] = f"WRONG_PHASE:{pos.phase.value}"
            return result

        # ── 3. Broker qty reconciliation ───────────────────────────────────────
        try:
            broker_qty = (
                abs(float(self.client.get_position_amt(symbol)))
                if effective_mode == "live"
                else float(runner_qty or 0.0)
            )
        except Exception as _qe:
            broker_qty = runner_qty
            _log.debug("[TRAIL] %s: broker qty lookup failed (%s) — using internal", symbol, _qe)

        live_qty = broker_qty if broker_qty > 0 else runner_qty
        result["live_qty"] = live_qty

        if live_qty <= 0:
            result["skip_reason"] = "QTY_ZERO"
            return result

        # ── 4. Anti-spam check ─────────────────────────────────────────────────
        last_ts = last_update_ts or pos.sl.trailing_last_update_ts
        if last_ts:
            try:
                from dateutil.parser import parse as _parse_ts
                last_dt = _parse_ts(last_ts)
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=timezone.utc)
                now_utc = datetime.now(timezone.utc)
                elapsed_s = (now_utc - last_dt).total_seconds()
                if elapsed_s < min_update_interval_s:
                    result["skip_reason"] = f"THROTTLE_TOO_SOON:{elapsed_s:.0f}s<{min_update_interval_s:.0f}s"
                    _log.debug("[TRAIL] %s: %s", symbol, result["skip_reason"])
                    return result
            except Exception:
                pass  # Bad timestamp format — proceed

        # ── 5. Trailing stop calculation ───────────────────────────────────────
        trail_distance = trail_atr_mult * atr

        if position_side == "LONG":
            raw_trailing = highest_since_entry - trail_distance
        else:
            raw_trailing = lowest_since_entry + trail_distance

        result["raw_trailing_stop"] = raw_trailing

        # Apply break-even floor / ceiling
        buffered_trailing = raw_trailing
        if be_floor_price is not None:
            if position_side == "LONG":
                buffered_trailing = max(buffered_trailing, be_floor_price)
            else:
                buffered_trailing = min(buffered_trailing, be_floor_price)

        result["buffered_trailing_stop"] = buffered_trailing

        # Tick normalisation
        try:
            from app.exchange.binance.filters import normalize_protection_price, _tick
            try:
                tick = _tick(symbol)
            except Exception:
                tick = 0.0001
            
            # Use the canonical helper, preserving that trailing is an SL movement
            normalized_trailing_str = normalize_protection_price(buffered_trailing, tick, position_side, "SL")
            # The rest of the logic expects floating point comparisons for reference checking
            normalized_trailing = float(normalized_trailing_str) 
        except Exception as e:
            _log.warning(f"[TRAIL_NORM] {symbol}: tick normalization failed ({e}) — using raw")
            normalized_trailing = float(buffered_trailing)

        result["normalized_trailing_stop"] = normalized_trailing

        # ── 6. Never-loosen guard (Refinement 3) ─────────────────────────────
        # Use last broker-confirmed stop (trailing_last_stop_price) as the
        # reference when available — it is the actual live stop on the exchange.
        # Fall back to current_stop if no trailing update has been confirmed yet.
        confirmed_stop = (
            pos.sl.trailing_last_stop_price
            if pos.sl.trailing_last_stop_price is not None
            else current_stop
        )
        result["reference_stop_used"] = confirmed_stop

        if position_side == "LONG":
            if normalized_trailing <= confirmed_stop:
                result["skip_reason"] = "TRAILING_WOULD_LOOSEN"
                _log.debug(
                    "[TRAIL] %s: LONG norm_trail=%.4f <= confirmed_stop=%.4f — skip",
                    symbol, normalized_trailing, confirmed_stop,
                )
                return result
        else:
            if normalized_trailing >= confirmed_stop:
                result["skip_reason"] = "TRAILING_WOULD_LOOSEN"
                _log.debug(
                    "[TRAIL] %s: SHORT norm_trail=%.4f >= confirmed_stop=%.4f — skip",
                    symbol, normalized_trailing, confirmed_stop,
                )
                return result

        # ── 7. Minimum delta check (uses confirmed_stop as baseline) ──────────
        delta = abs(normalized_trailing - confirmed_stop) / max(confirmed_stop, 1e-12)
        if delta < min_delta_pct:
            result["skip_reason"] = f"DELTA_BELOW_THRESHOLD:{delta:.5f}<{min_delta_pct:.5f}"
            _log.debug("[TRAIL] %s: %s", symbol, result["skip_reason"])
            return result

        # ── 8. Phase transition: TRAILING_UPDATE_PENDING ───────────────────────
        if effective_mode != "live":
            import uuid as _paper_uuid

            now_iso = datetime.now(timezone.utc).isoformat()
            new_sl_id = f"paper_sl_{_paper_uuid.uuid4().hex}"
            new_tp_id = tp_order_id or pos.sl.tp_order_id
            pos.sl.current_stop = normalized_trailing
            pos.sl.trailing_last_stop_price = normalized_trailing
            pos.sl.trailing_last_update_ts = now_iso
            pos.sl.sl_order_id = new_sl_id
            if new_tp_id:
                pos.sl.tp_order_id = new_tp_id
            pos.phase = PositionPhase.RUNNER_TRAILING
            position_manager._persist_lifecycle(symbol)

            result["broker_response"] = {
                "status": "PAPER_TRAILING_UPDATED",
                "sl_order_id": new_sl_id,
                "tp_order_id": new_tp_id,
                "mode": effective_mode,
            }
            result["protection_update_status"] = "PAPER_TRAILING_UPDATED"
            result["trailing_applied"] = True
            result["lifecycle_state_after"] = PositionPhase.RUNNER_TRAILING.value
            return result

        pos.phase = PositionPhase.TRAILING_UPDATE_PENDING
        position_manager._persist_lifecycle(symbol)

        # ── 9. Exchange update via update_protection() ─────────────────────────
        try:
            # Refinement 4: always carry the existing TP order ID so the
            # adapter can preserve TP2 protection for the runner quantity.
            # If tp2_price is unknown, pass None for new_tp_price — the adapter
            # must not cancel the existing TP order in that case.
            _eff_sl_oid = sl_order_id or pos.sl.sl_order_id
            _eff_tp_oid = tp_order_id or pos.sl.tp_order_id
            _new_tp_price = float(tp2_price) if tp2_price else None
            prot_req = ProtectionUpdateRequest(
                symbol=symbol,
                position_side=position_side,
                qty=live_qty,
                new_sl_price=normalized_trailing,
                new_tp_price=_new_tp_price,
                old_sl_order_id=_eff_sl_oid,
                old_tp_order_id=_eff_tp_oid,  # always present; adapter preserves TP2 if new_tp_price=None
                reason="TRAILING",
            )
            broker_resp = self.client.update_protection(prot_req)
            result["broker_response"] = broker_resp

            # ── S2: Fail closed — partial failure must not persist None order IDs ──
            _trail_prot_status = (
                broker_resp.get("status") if isinstance(broker_resp, dict)
                else getattr(broker_resp, "status", None)
            )
            if _trail_prot_status not in ("OK", "ok", "success"):
                raise RuntimeError(
                    f"[SEV1-S2] update_protection TRAILING partial failure: "
                    f"status={_trail_prot_status} "
                    f"error={broker_resp.get('error') if isinstance(broker_resp, dict) else None}"
                )
            result["protection_update_status"] = broker_resp.get("status", "UNKNOWN")

        except Exception as _prot_err:
            # ── Failure: rollback phase, do NOT update stop ────────────────────
            _log.error(
                "[TRAIL] %s: update_protection failed: %s — rolling back to RUNNER_TRAILING",
                symbol, _prot_err, exc_info=True,
            )
            pos.phase = PositionPhase.RUNNER_TRAILING
            position_manager._persist_lifecycle(symbol)
            result["failure_reason"] = f"UPDATE_PROTECTION_FAILED:{_prot_err}"
            result["lifecycle_state_after"] = PositionPhase.RUNNER_TRAILING.value
            return result

        # ── 10. Confirm success ────────────────────────────────────────────────
        now_iso = datetime.now(timezone.utc).isoformat()

        pos.sl.current_stop = normalized_trailing
        pos.sl.trailing_last_stop_price = normalized_trailing
        pos.sl.trailing_last_update_ts = now_iso
        # Rotate order IDs
        if broker_resp.get("sl_order_id"):
            pos.sl.sl_order_id = broker_resp["sl_order_id"]
        if broker_resp.get("tp_order_id"):
            pos.sl.tp_order_id = broker_resp["tp_order_id"]

        pos.phase = PositionPhase.RUNNER_TRAILING
        position_manager._persist_lifecycle(symbol)

        result["trailing_applied"] = True
        result["lifecycle_state_after"] = PositionPhase.RUNNER_TRAILING.value

        _log.info(
            "[TRAIL] %s: Trailing stop updated side=%s "
            "norm_trail=%.4f prior_stop=%.4f live_qty=%s "
            "atr=%.4f anchor=%s delta_pct=%.4f",
            symbol, position_side,
            normalized_trailing, current_stop, live_qty,
            atr,
            highest_since_entry if position_side == "LONG" else lowest_since_entry,
            delta,
        )
        return result

