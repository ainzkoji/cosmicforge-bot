from __future__ import annotations

from dataclasses import dataclass
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP

from app.core.config import settings
from app.exchange.binance.client import BinanceFuturesClient
from app.exchange.binance.filters import (
    extract_filters,
    round_qty,
)  # round_price no longer used here
from app.symbols.leverage import leverage_for
from app.execution.confirm import wait_until_flat
from app.symbols.sizing import parse_usdt_map, usdt_for, size_from_budget


class PolicyViolation(Exception):
    pass


# =========================
# Price helpers (tick rounding + buffer)
# =========================
def _round_to_tick(px: float, tick: Decimal, *, up: bool) -> float:
    """
    Round price to the exchange tick.
    up=False -> round DOWN
    up=True  -> round UP
    """
    p = Decimal(str(px))
    mode = ROUND_UP if up else ROUND_DOWN
    return float((p / tick).to_integral_value(rounding=mode) * tick)


def _apply_sl_tp_rounding_with_buffer(
    *,
    side: str,
    entry_px: float,
    sl_price: float,
    tp_price: float,
    tick: Decimal,
    buffer_ticks: int = 2,
) -> tuple[float, float]:
    """
    side: "BUY" (long entry) or "SELL" (short entry)

    Ensures direction-correct tick rounding:
      - LONG:  SL below -> DOWN, TP above -> UP
      - SHORT: SL above -> UP,   TP below -> DOWN

    Adds 1–2 tick safety buffer away from entry to avoid Binance -2021.
    """
    tick_f = float(tick)
    buf = tick_f * float(buffer_ticks)

    if side == "BUY":
        sl_price = _round_to_tick(sl_price, tick, up=False)  # below
        tp_price = _round_to_tick(tp_price, tick, up=True)  # above
        sl_price = min(sl_price, entry_px - buf)
        tp_price = max(tp_price, entry_px + buf)
    else:
        sl_price = _round_to_tick(sl_price, tick, up=True)  # above
        tp_price = _round_to_tick(tp_price, tick, up=False)  # below
        sl_price = max(sl_price, entry_px + buf)
        tp_price = min(tp_price, entry_px - buf)

    return sl_price, tp_price


def _place_sl_with_retry_on_2021(
    *,
    client: BinanceFuturesClient,
    symbol: str,
    exit_side: str,
    sl_price: float,
    buf: float,
) -> dict:
    """
    Retry once on Binance -2021 by nudging stop further away.
    exit_side:
      - "SELL" closes LONG (SL is below)
      - "BUY"  closes SHORT (SL is above)
    """
    try:
        return client.place_stop_market(symbol, exit_side, sl_price)
    except RuntimeError as e:
        s = str(e)
        if '"code":-2021' in s or "would immediately trigger" in s:
            # push 2 more ticks away and retry once
            if exit_side == "SELL":
                # closing LONG -> SL must be BELOW, push further down
                sl_price = sl_price - buf
            else:
                # closing SHORT -> SL must be ABOVE, push further up
                sl_price = sl_price + buf
            return client.place_stop_market(symbol, exit_side, sl_price)
        raise


# =========================
# Execution Result
# =========================
@dataclass
class ExecResult:
    action: str
    details: dict


# =========================
# Binance Executor
# =========================
class BinanceExecutor:
    def __init__(self, client: BinanceFuturesClient, *, risk_gate=None, audit=None):
        self.client = client
        self.risk_gate = risk_gate
        self.audit = audit
        self.run_id = None  # runner may set this

    # ---------------- INTERNAL HELPERS ----------------

    def _size_qty(self, symbol: str, usdt: float) -> tuple[float, dict]:
        # usdt here is MARGIN budget per order (what you want to spend)
        price = self.client.last_price(symbol)
        exch = self.client.exchange_info_cached()
        flt = extract_filters(exch, symbol)

        # leverage for this symbol (same policy used everywhere else)
        lev_map = settings.SYMBOL_LEVERAGE_MAP or {}
        lev = leverage_for(
            symbol, lev_map, settings.DEFAULT_LEVERAGE, settings.MIN_LEVERAGE
        )

        # Convert margin -> notional target
        target_notional = float(usdt) * float(lev)

        raw_qty = target_notional / float(price)
        qty_dec = round_qty(raw_qty, flt.step_size)

        notional = float(qty_dec) * float(price)

        # Binance minimum notional (your config should represent NOTIONAL minimum)
        min_notional = float(getattr(settings, "MIN_NOTIONAL_USDT", 0.0) or 0.0)

        # Basic qty filter
        if not (qty_dec >= flt.min_qty and qty_dec > 0):
            return 0.0, {
                "error": "USDT too small (qty < min_qty after rounding)",
                "symbol": symbol,
                "price": price,
                "usdt_margin": usdt,
                "leverage": lev,
                "target_notional": target_notional,
                "raw_qty": raw_qty,
                "qty_rounded": str(qty_dec),
                "min_qty": str(flt.min_qty),
                "step_size": str(flt.step_size),
                "notional": notional,
                "min_notional_required": min_notional,
            }

        # Notional filter (this is what caused your crash)
        if min_notional > 0 and notional < min_notional:
            return 0.0, {
                "error": "USDT too small (notional < MIN_NOTIONAL_USDT)",
                "symbol": symbol,
                "price": price,
                "usdt_margin": usdt,
                "leverage": lev,
                "target_notional": target_notional,
                "raw_qty": raw_qty,
                "qty_rounded": str(qty_dec),
                "min_qty": str(flt.min_qty),
                "step_size": str(flt.step_size),
                "notional": notional,
                "min_notional_required": min_notional,
                "min_margin_required_est": (min_notional / float(lev)) if lev else None,
            }

        return float(qty_dec), {
            "symbol": symbol,
            "price": price,
            "usdt_margin": usdt,
            "leverage": lev,
            "target_notional": target_notional,
            "raw_qty": raw_qty,
            "qty": str(qty_dec),
            "min_qty": str(flt.min_qty),
            "step_size": str(flt.step_size),
            "notional": notional,
            "min_notional_required": min_notional,
        }

    def _ensure_leverage(self, symbol: str) -> dict:
        lev_map = settings.SYMBOL_LEVERAGE_MAP or {}
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

    # ---------------- EXECUTION ----------------

    def execute_signal(self, symbol: str, signal: str, usdt: float) -> ExecResult:
        signal = signal.upper()

        # Paper / dry-run mode
        if settings.EXECUTION_MODE.lower() != "live":
            return ExecResult(
                "PAPER_ONLY",
                {"symbol": symbol, "signal": signal},
            )

        live_symbols = set(settings.LIVE_SYMBOLS)
        if symbol.upper() not in live_symbols:
            return ExecResult(
                "SKIPPED_NOT_LIVE_SYMBOL",
                {"symbol": symbol, "signal": signal},
            )

        if signal not in {"BUY", "SELL", "CLOSE"}:
            return ExecResult(
                "NO_TRADE",
                {"symbol": symbol, "signal": signal, "reason": "unsupported_signal"},
            )

        # Sync actual position from exchange
        pos_amt = self.client.get_position_amt(symbol)

        # Derive current position state (source of truth)
        if pos_amt > 0:
            current_position = "LONG"
        elif pos_amt < 0:
            current_position = "SHORT"
        else:
            current_position = "NONE"

        # ✅ 1) Handle explicit CLOSE (priority, safe, no accidental opens)
        if signal == "CLOSE":
            # already flat
            if current_position == "NONE":
                return ExecResult(
                    "NO_TRADE",
                    {"symbol": symbol, "signal": signal, "reason": "ALREADY_FLAT"},
                )

            # cancel TP/SL + any open orders first (do not block close if cancel fails)
            try:
                self.client.cancel_all_orders(symbol)
            except Exception as e:
                self._audit_warn(
                    symbol,
                    "CANCEL_ORDERS_FAILED",
                    {"error": f"{type(e).__name__}: {e}"},
                )

            # close position at market
            close = self.client.close_position_market(symbol)

            # confirm flat (avoid partial-close / latency issues)
            ok = self._wait_until_flat(symbol, timeout_sec=8.0, poll_sec=0.5)

            return ExecResult(
                "CLOSED_POSITION",
                {
                    "symbol": symbol,
                    "pos_amt_before": pos_amt,
                    "position_before": current_position,
                    "close_order": close,
                    "confirmed_flat": ok,
                },
            )

        # Determine whether this is an ADD (same-direction action while already in position)
        is_add = (signal == "BUY" and current_position == "LONG") or (
            signal == "SELL" and current_position == "SHORT"
        )

        # ✅ 2) Global Risk Gate / Kill Switch (blocks opens/adds only; closes are allowed above)
        is_open_action = (signal in {"BUY", "SELL"}) and (
            current_position == "NONE" or is_add
        )

        if is_open_action and self.risk_gate is not None:
            decision = self.risk_gate.can_open()
            if not decision.allowed:
                # log once here (central)
                try:
                    if self.audit is not None:
                        self.audit.event(
                            event_type="RISK_BLOCK",
                            run_id=getattr(self, "run_id", None),
                            symbol=symbol,
                            action="OPEN_BLOCKED",
                            details={
                                "reason": decision.reason,
                                "kill": decision.kill,
                                "realized_pnl": decision.realized_pnl,
                                "max_loss": decision.max_loss,
                            },
                        )
                except Exception:
                    pass

                return ExecResult(
                    action="BLOCKED_RISK", details={"reason": decision.reason}
                )

        # Ensure leverage (for live order placement)
        lev_info = self._ensure_leverage(symbol)

        # SELL signal while LONG → close first (flip-close)
        if signal == "SELL" and current_position == "LONG":
            close = self.client.close_position_market(symbol)
            return ExecResult(
                "CLOSED_LONG",
                {
                    "symbol": symbol,
                    "pos_amt": pos_amt,
                    **lev_info,
                    "close_order": close,
                },
            )

        # BUY signal while SHORT → close first (flip-close)
        if signal == "BUY" and current_position == "SHORT":
            close = self.client.close_position_market(symbol)
            return ExecResult(
                "CLOSED_SHORT",
                {
                    "symbol": symbol,
                    "pos_amt": pos_amt,
                    **lev_info,
                    "close_order": close,
                },
            )

        # ✅ B) compute budget here (MARGIN budget per order)
        budget_usdt = float(usdt or 0.0)
        if budget_usdt <= 0:
            usdt_map = parse_usdt_map(getattr(settings, "SYMBOL_USDT_MAP", None))
            budget_usdt = float(
                usdt_for(symbol, usdt_map, settings.TRADE_USDT_PER_ORDER)
            )

        # ✅ C) Size order using sizing engine (centralized, broker-agnostic)
        price = self.client.last_price(symbol)
        exch = self.client.exchange_info_cached()
        flt = extract_filters(exch, symbol)

        lev_map = settings.SYMBOL_LEVERAGE_MAP or {}
        lev = leverage_for(
            symbol, lev_map, settings.DEFAULT_LEVERAGE, settings.MIN_LEVERAGE
        )

        # IMPORTANT:
        # size_from_budget() signature is:
        #   (symbol, price, usdt_margin, leverage, filters, min_notional_override)
        res = size_from_budget(
            symbol=symbol,
            price=float(price),
            usdt_margin=float(budget_usdt),
            leverage=int(lev),
            filters=flt,
            min_notional_override=float(
                getattr(settings, "MIN_NOTIONAL_USDT", 0.0) or 0.0
            ),
        )

        # size_from_budget returns SizeResult with:
        # qty, notional, min_notional_required, min_margin_required, reason, details
        qty = float(getattr(res, "qty", 0.0) or 0.0)
        reason = str(getattr(res, "reason", "") or "")
        details = getattr(res, "details", {}) or {}

        ok = (reason == "ok") and (qty > 0.0)

        if not ok or qty <= 0:
            # Map sizing reasons to executor actions
            if reason in {"below_min_notional", "qty_below_min_qty"}:
                return ExecResult(
                    "SKIPPED_MIN_NOTIONAL",
                    {
                        "symbol": symbol,
                        "signal": signal,
                        "trade_usdt": budget_usdt,
                        "sizing_reason": reason,
                        "min_notional_required": getattr(
                            res, "min_notional_required", None
                        ),
                        "min_margin_required": getattr(
                            res, "min_margin_required", None
                        ),
                        **details,
                    },
                )

            return ExecResult(
                "NO_TRADE_INVALID_QTY",
                {
                    "symbol": symbol,
                    "signal": signal,
                    "trade_usdt": budget_usdt,
                    "sizing_reason": reason,
                    "min_notional_required": getattr(
                        res, "min_notional_required", None
                    ),
                    "min_margin_required": getattr(res, "min_margin_required", None),
                    **lev_info,
                    **details,
                },
            )

        # Keep the existing shape executor uses downstream
        sizing = {
            "qty": qty,
            "notional": float(getattr(res, "notional", 0.0) or 0.0),
            "reason": reason,
            "min_notional_required": float(
                getattr(res, "min_notional_required", 0.0) or 0.0
            ),
            "min_margin_required": float(
                getattr(res, "min_margin_required", 0.0) or 0.0
            ),
            "details": details,
        }

        # ✅ enforce notional after rounding
        notional = float(sizing.get("notional", 0.0))
        if float(
            getattr(settings, "MIN_NOTIONAL_USDT", 0.0) or 0.0
        ) > 0 and notional < float(getattr(settings, "MIN_NOTIONAL_USDT", 0.0) or 0.0):
            return ExecResult(
                "SKIPPED_MIN_NOTIONAL_ROUNDED",
                {
                    "symbol": symbol,
                    "signal": signal,
                    "trade_usdt": budget_usdt,
                    "usdt": budget_usdt,
                    "qty": qty,
                    "price": float(price),
                    "notional": notional,
                    "min_required": float(
                        getattr(settings, "MIN_NOTIONAL_USDT", 0.0) or 0.0
                    ),
                    "sizing": sizing,
                },
            )

        side = "BUY" if signal == "BUY" else "SELL"

        # --- entry order ---
        order = self.client.place_market_order(
            symbol=symbol,
            side=side,
            quantity=qty,
        )

        # --- protection orders (SL / TP) ---
        exch = self.client.exchange_info_cached()
        flt = extract_filters(exch, symbol)

        entry_px = float(self.client.last_price(symbol))

        sl_pct = settings.STOP_LOSS_PCT / 100.0
        tp_pct = settings.TAKE_PROFIT_PCT / 100.0

        if side == "BUY":
            sl_price = entry_px * (1.0 - sl_pct)
            tp_price = entry_px * (1.0 + tp_pct)
            exit_side = "SELL"
        else:
            sl_price = entry_px * (1.0 + sl_pct)
            tp_price = entry_px * (1.0 - tp_pct)
            exit_side = "BUY"

        # Directional tick rounding + buffer (prevents -2021)
        tick = flt.tick_size
        buffer_ticks = 2  # 1 or 2 is fine
        sl_price, tp_price = _apply_sl_tp_rounding_with_buffer(
            side=side,
            entry_px=entry_px,
            sl_price=sl_price,
            tp_price=tp_price,
            tick=tick,
            buffer_ticks=buffer_ticks,
        )

        # Retry once on -2021 for SL only
        tick_f = float(tick)
        buf = tick_f * float(buffer_ticks)

        sl = _place_sl_with_retry_on_2021(
            client=self.client,
            symbol=symbol,
            exit_side=exit_side,
            sl_price=sl_price,
            buf=buf,
        )

        tp = self.client.place_take_profit_market(
            symbol,
            exit_side,
            tp_price,
        )

        return ExecResult(
            "ORDER_PLACED",
            {
                "symbol": symbol,
                "signal": signal,
                "side": side,
                "qty": qty,
                "trade_usdt": budget_usdt,
                "usdt": budget_usdt,
                "pos_amt_before": pos_amt,
                "leverage": lev_info["leverage"],
                "leverage_result": lev_info["result"],
                "sizing": sizing,
                "order": order,
                "protection": {
                    "sl": sl,
                    "tp": tp,
                    "sl_price": sl_price,
                    "tp_price": tp_price,
                },
            },
        )

    # ---------------- PROTECTION REPAIR ----------------

    def ensure_protection(self, symbol: str) -> dict:
        pos_amt = self.client.get_position_amt(symbol)

        # If flat → cancel leftovers
        if pos_amt == 0:
            return {
                "status": "flat",
                "cancel": self.client.cancel_all_orders(symbol),
            }

        opens = self.client.open_orders(symbol)
        types = {o.get("type") for o in opens} if isinstance(opens, list) else set()

        has_sl = "STOP_MARKET" in types
        has_tp = "TAKE_PROFIT_MARKET" in types

        if has_sl and has_tp:
            return {
                "status": "ok",
                "has_sl": True,
                "has_tp": True,
                "open_orders": len(opens) if isinstance(opens, list) else None,
            }

        # Missing protection → recreate
        self.client.cancel_all_orders(symbol)

        exch = self.client.exchange_info_cached()
        flt = extract_filters(exch, symbol)
        px = float(self.client.last_price(symbol))

        sl_pct = settings.STOP_LOSS_PCT / 100.0
        tp_pct = settings.TAKE_PROFIT_PCT / 100.0

        if pos_amt > 0:
            # LONG
            exit_side = "SELL"
            sl_price = px * (1.0 - sl_pct)
            tp_price = px * (1.0 + tp_pct)
            entry_side = "BUY"
        else:
            # SHORT
            exit_side = "BUY"
            sl_price = px * (1.0 + sl_pct)
            tp_price = px * (1.0 - tp_pct)
            entry_side = "SELL"

        tick = flt.tick_size
        buffer_ticks = 2
        sl_price, tp_price = _apply_sl_tp_rounding_with_buffer(
            side=entry_side,
            entry_px=px,
            sl_price=sl_price,
            tp_price=tp_price,
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
            "status": "repaired",
            "sl_price": sl_price,
            "tp_price": tp_price,
            "sl": sl,
            "tp": tp,
        }

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
