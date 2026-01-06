from __future__ import annotations

import threading
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict

from app.core.config import settings
from app.exchange.binance.client import BinanceFuturesClient, kline_closes
from app.execution.executor import BinanceExecutor, ExecResult
from app.persistence.audit import Audit
from app.persistence.db import DB
from app.persistence.state_store import StateStore
from app.risk.daily_loss import DailyLossState
from app.risk.realized_pnl import realized_pnl_from_user_trades
from app.runner.models import SymbolState
from app.strategy.robust_ensemble import RobustEnsembleStrategy
from app.strategy.sma_cross import signal_from_closes
from app.symbols.sizing import parse_usdt_map, usdt_for
from app.symbols.universe import parse_symbols
from app.execution.confirm import wait_until_flat
from app.execution.position_manager import should_exit
from app.execution.exit_rules import should_close_position

# ✅ ADD: wire RiskGate into runner (dependency injection)
from app.risk.gate import RiskGate


class PaperRunner:
    def __init__(self, client: BinanceFuturesClient):
        self.client = client

        # ---- Basic config / strategy ----

        self.symbols = parse_symbols(
            ",".join(settings.TRADE_SYMBOLS), settings.MAX_SYMBOLS
        )
        self.interval = settings.DEFAULT_INTERVAL
        self.strategy = RobustEnsembleStrategy(self.client, interval="1m")

        # --- Execution locks (robust anti-overlap) ---
        self._cycle_lock = threading.Lock()
        self._symbol_locks = defaultdict(threading.Lock)  # symbol -> Lock

        # ---- Persistence + audit MUST exist before calling self.store.* ----
        self.db = DB()
        self.audit = Audit(self.db)
        self.run_id: str | None = None
        self.store = StateStore(self.db)

        # ---- Universes (trade vs live) ----
        self.trade_symbols = list(settings.TRADE_SYMBOLS)
        self.live_symbols = list(settings.LIVE_SYMBOLS)

        # ✅ Universe used for state + reconciliation (union of trade + live symbols)
        seen = set()
        self.universe_symbols = []
        for s in list(self.trade_symbols) + list(self.live_symbols):
            ss = (s or "").upper()
            if ss and ss not in seen:
                seen.add(ss)
                self.universe_symbols.append(ss)

        # ✅ Create state from the union universe (trade + live)
        self.state: Dict[str, SymbolState] = {
            s: SymbolState() for s in self.universe_symbols
        }

        # ✅ KEEP YOUR BLOCK: restore symbol state early (NOW store exists)
        saved = self.store.load_symbols()
        for sym, row in saved.items():
            if sym not in self.state:
                continue

            st = self.state[sym]
            if isinstance(row, dict):
                st.position = row.get("position", "NONE")
                st.entry_price = row.get("entry_price")
                st.last_signal = row.get("last_signal", "HOLD")
                st.last_action = row.get("last_action", "NOOP")
                st.last_checked_ms = int(row.get("last_checked_ms", 0) or 0)
                st.adds = int(row.get("adds", 0) or 0)
                st.last_trade_ms = int(row.get("last_trade_ms", 0) or 0)
                st.pending_open = row.get("pending_open", "NONE")
                st.entry_qty = float(row.get("entry_qty", 0.0) or 0.0)
                st.last_user_trade_id = int(row.get("last_user_trade_id", 0) or 0)

        # Per-symbol USDT sizing map
        self.usdt_map = parse_usdt_map(settings.SYMBOL_USDT_MAP)

        # Track how many live trades were placed in the current run_once() cycle
        self.live_trades_this_cycle = 0

        # Daily loss kill-switch state
        self.daily = DailyLossState(day=date.today())

        # ✅ ADD (as requested): create RiskGate after self.daily exists
        self.risk_gate = RiskGate(
            get_daily_state=lambda: self.daily,
            max_loss_usdt=settings.DAILY_MAX_LOSS_USDT,
        )

        # ✅ CHANGE: pass risk_gate + audit into executor
        self.executor = BinanceExecutor(
            client,
            risk_gate=self.risk_gate,
            audit=self.audit,
        )

        # ✅ KEEP your second restore too (even though it's duplicate, per your request)
        saved_daily = self.store.load_daily(self.daily.day)
        if saved_daily:
            self.daily.realized_pnl = float(saved_daily.get("realized_pnl", 0.0))
            self.daily.kill = bool(saved_daily.get("kill", False))

        # Restore symbol states (typed SymbolState objects)
        saved_symbols = self.store.load_symbols()
        for sym, row in saved_symbols.items():
            if sym not in self.state:
                continue

            st = self.state[sym]

            # row can be dict OR SymbolState (robust)
            if isinstance(row, dict):
                st.position = row.get("position", "NONE")
                st.entry_price = row.get("entry_price")
                st.last_signal = row.get("last_signal", "HOLD")
                st.last_action = row.get("last_action", "NOOP")
                st.last_checked_ms = int(row.get("last_checked_ms", 0) or 0)
                st.adds = int(row.get("adds", 0) or 0)
                st.last_trade_ms = int(row.get("last_trade_ms", 0) or 0)
                st.pending_open = row.get("pending_open", "NONE")
                st.entry_qty = float(row.get("entry_qty", 0.0) or 0.0)
                st.last_user_trade_id = int(row.get("last_user_trade_id", 0) or 0)

            else:
                # assume it's already a SymbolState-like object
                st.position = getattr(row, "position", "NONE")
                st.entry_price = getattr(row, "entry_price", None)
                st.last_signal = getattr(row, "last_signal", "HOLD")
                st.last_action = getattr(row, "last_action", "NOOP")
                st.last_checked_ms = int(getattr(row, "last_checked_ms", 0) or 0)
                st.adds = int(getattr(row, "adds", 0) or 0)
                st.last_trade_ms = int(getattr(row, "last_trade_ms", 0) or 0)
                st.pending_open = getattr(row, "pending_open", "NONE")
                st.entry_qty = float(getattr(row, "entry_qty", 0.0) or 0.0)
                st.last_user_trade_id = int(getattr(row, "last_user_trade_id", 0) or 0)

        self.reconcile_positions_from_exchange()

        # ✅ FIX: override DB state with exchange truth on startup
        self.reconcile_positions_on_startup()

    @contextmanager
    def cycle_guard(self, timeout_s: float = 0.0):
        """
        Prevent overlapping run_once cycles.
        If another cycle is running, we skip cleanly.
        """
        acquired = self._cycle_lock.acquire(timeout=timeout_s)
        try:
            yield acquired
        finally:
            if acquired:
                self._cycle_lock.release()

    @contextmanager
    def symbol_guard(self, symbol: str, timeout_s: float = 0.0):
        """
        Prevent overlapping work per symbol across:
        - runner loop
        - manual trade endpoints
        """
        sym = (symbol or "").upper()
        lock = self._symbol_locks[sym]
        acquired = lock.acquire(timeout=timeout_s)
        try:
            yield acquired
        finally:
            if acquired:
                lock.release()

    # ✅ NEW: reconcile positions on startup (exchange truth overrides DB)
    def reconcile_positions_on_startup(self) -> None:
        try:
            # prefer the new helper if it exists
            if hasattr(self.client, "position_risk_all"):
                risks = self.client.position_risk_all()
            else:
                risks = self.client.position_risk(None)

            if not isinstance(risks, list):
                return

            updated = 0
            for row in risks:
                sym = (row.get("symbol") or "").upper()
                if not sym:
                    continue
                if sym not in self.state:
                    continue

                st = self.state[sym]

                try:
                    amt = float(row.get("positionAmt", "0") or 0.0)
                except Exception:
                    amt = 0.0

                try:
                    entry_px = float(row.get("entryPrice", "0") or 0.0)
                except Exception:
                    entry_px = 0.0

                if amt > 0:
                    st.position = "LONG"
                    st.entry_price = entry_px if entry_px > 0 else st.entry_price
                    st.entry_qty = abs(amt)
                elif amt < 0:
                    st.position = "SHORT"
                    st.entry_price = entry_px if entry_px > 0 else st.entry_price
                    st.entry_qty = abs(amt)
                else:
                    st.position = "NONE"
                    st.entry_price = None
                    st.entry_qty = 0.0
                    st.adds = 0

                # persist reconciled symbol state immediately
                try:
                    self.store.save_symbol(sym, st)
                except Exception:
                    pass

                updated += 1

            # audit
            try:
                self.audit.event(
                    event_type="INFO",
                    run_id=self.run_id,
                    symbol=None,
                    action="RECONCILE_POSITIONS_STARTUP",
                    details={"updated": updated},
                )
            except Exception:
                pass

        except Exception as e:
            try:
                self.audit.event(
                    event_type="ERROR",
                    run_id=self.run_id,
                    symbol=None,
                    action="RECONCILE_POSITIONS_FAILED",
                    details={"error": f"{type(e).__name__}: {e}"},
                )
            except Exception:
                pass

    # Persist symbol state every time (robust, restart-safe)
    def _finalize(
        self, symbol: str, st: SymbolState, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            self.store.save_symbol(symbol, st)
        except Exception as e:
            # Don't crash the bot because persistence failed — log it
            try:
                self.audit.event(
                    event_type="ERROR",
                    run_id=self.run_id,
                    symbol=symbol,
                    action="SAVE_SYMBOL_FAILED",
                    details={"error": f"{type(e).__name__}: {e}"},
                )
            except Exception:
                pass
        return payload

    # F) If kill-switch triggers: cancel orders + optionally close positions
    def activate_kill_switch(self) -> None:
        for sym in {
            s.strip().upper()
            for s in ",".join(settings.LIVE_SYMBOLS).split(",")
            if s.strip()
        }:
            try:
                self.client.cancel_all_orders(sym)
                if settings.KILL_SWITCH_CLOSE_POSITIONS:
                    self.client.close_position_market(sym)
            except Exception:
                pass

    # ✅ ADD: helper to confirm position is flat
    def _is_flat(self, symbol: str) -> bool:
        pos_info = self.executor.client.get_position_info(symbol)
        if not pos_info:
            return True
        try:
            amt = float(pos_info.get("positionAmt", "0") or 0.0)
        except Exception:
            amt = 0.0
        return amt == 0.0

    def step_symbol(self, symbol: str) -> Dict[str, Any]:
        symbol = symbol.upper()

        # ✅ Robust fix: Wrap step_symbol() with a non-blocking lock (fail-fast)
        lock = self._symbol_locks[symbol]
        if not lock.acquire(timeout=10):
            try:
                self.audit.event(
                    event_type="EXEC_LOCK",
                    run_id=self.run_id,
                    symbol=symbol,
                    action="SKIP_LOCK_TIMEOUT",
                    details={"reason": "SYMBOL_LOCK_TIMEOUT"},
                )
            except Exception:
                pass

            # Keep the older event too (do not remove)
            try:
                self.audit.event(
                    event_type="SYMBOL_LOCK_BUSY",
                    run_id=self.run_id,
                    symbol=symbol,
                    action="SKIP",
                    details={"note": "Symbol execution lock timeout"},
                )
            except Exception:
                pass

            return {"symbol": symbol, "skipped": True, "reason": "SYMBOL_LOCK_TIMEOUT"}

        try:
            # 1) Market data
            kl = self.client.klines(symbol=symbol, interval=self.interval, limit=120)

            # ✅ STRATEGY SAFETY WRAP (never crash runner)
            try:
                res = self.strategy.get_signal(symbol)
            except Exception as e:
                try:
                    self.audit.event(
                        event_type="STRATEGY_ERROR",
                        run_id=getattr(self, "run_id", None),
                        symbol=symbol,
                        action="SIGNAL_FAILED",
                        details={"error": repr(e)},
                    )
                except Exception:
                    pass

                # Fallback: HOLD (safe, no trade)
                class _Tmp:
                    pass

                class _Sig:
                    value = "HOLD"

                res = _Tmp()
                res.signal = _Sig()
                res.confidence = 0.0
                res.reason = "strategy_exception"
                res.meta = {"reason": "strategy_exception"}
            base_signal = (getattr(res.signal, "value", None) or "HOLD").upper()
            sig = base_signal

            self.audit.event(
                event_type="STRATEGY_SIGNAL",
                symbol=symbol,
                # ✅ IMPORTANT: log the final signal (forced override included)
                action=sig,
                details={
                    "strategy": getattr(self.strategy, "name", "unknown"),
                    "confidence": res.confidence,
                    "reason": res.reason,
                    "meta": res.meta,
                },
            )

            price = self.client.last_price(symbol)

            st = self.state[symbol]
            now_ms = int(time.time() * 1000)
            st.last_checked_ms = now_ms
            st.last_signal = sig

            cooldown_ok = (now_ms - st.last_trade_ms) >= (
                settings.COOLDOWN_SECONDS * 1000
            )

            # ✅ SL cooldown (after stop-loss)
            sl_cooldown_ok = (now_ms - int(getattr(st, "last_stop_ms", 0) or 0)) >= (
                int(getattr(settings, "SL_COOLDOWN_SECONDS", 600) or 600) * 1000
            )

            def mark_trade(action: str) -> None:
                st.last_action = action
                st.last_trade_ms = now_ms

            # 2) Sync from exchange (source of truth) + sync entry price & qty
            pos_info = self.executor.client.get_position_info(symbol)
            pos_amt = float(pos_info.get("positionAmt", "0")) if pos_info else 0.0

            if pos_amt > 0:
                st.position = "LONG"
                st.entry_price = (
                    float(pos_info.get("entryPrice", "0"))
                    if pos_info
                    else st.entry_price
                )
                st.entry_qty = abs(pos_amt)

            elif pos_amt < 0:
                st.position = "SHORT"
                st.entry_price = (
                    float(pos_info.get("entryPrice", "0"))
                    if pos_info
                    else st.entry_price
                )
                st.entry_qty = abs(pos_amt)

            else:
                st.position = "NONE"
                st.entry_price = None
                st.entry_qty = 0.0
                st.adds = 0

            # Per-symbol USDT sizing
            trade_usdt = usdt_for(symbol, self.usdt_map, settings.TRADE_USDT_PER_ORDER)

            # --- AUTO EXIT MANAGEMENT (priority) ---
            exit_now, exit_reason = should_exit(
                position=st.position,
                entry_price=st.entry_price,
                price=price,
                last_trade_ms=st.last_trade_ms,
                signal=sig,
            )

            if exit_now and st.position in {"LONG", "SHORT"}:
                decision = f"CLOSE_{st.position}_{exit_reason}"
                exec_signal = "CLOSE"

                self.audit.event(
                    event_type="DECISION",
                    run_id=self.run_id,
                    symbol=symbol,
                    action=decision,
                    details={
                        "position": st.position,
                        "entry_price": st.entry_price,
                        "price": price,
                        "signal": sig,
                        "reason": exit_reason,
                    },
                )

                exec_result = self.executor.execute_signal(
                    symbol, exec_signal, trade_usdt
                )

                if exec_result.action == "CLOSED_POSITION":
                    st.position = "NONE"
                    st.entry_price = None
                    st.entry_qty = 0.0
                    st.adds = 0
                    st.pending_open = "NONE"

                # ✅ A) Record stop-out time for SL cooldown (persisted via StateStore.save_symbol)
                if str(exit_reason).startswith("STOP_LOSS"):
                    st.last_stop_ms = int(now_ms)
                    st.reentry_confirm_signal = "NONE"
                    st.reentry_confirm_count = 0

                if exec_result.action in {
                    "CLOSED_LONG",
                    "CLOSED_SHORT",
                    "ORDER_PLACED",
                }:
                    mark_trade(decision)

                self.audit.event(
                    event_type="EXECUTION_RESULT",
                    run_id=self.run_id,
                    symbol=symbol,
                    action=exec_result.action,
                    details={
                        "decision": decision,
                        "execution": {
                            "action": exec_result.action,
                            "details": exec_result.details,
                        },
                    },
                )

                return {
                    "symbol": symbol,
                    "decision": decision,
                    "signal": sig,
                    "position": st.position,
                    "price": price,
                    "exit_reason": exit_reason,
                }

            # ✅ B) STOP-LOSS RE-ENTRY CONTROL: cooldown + confirmation (no logic removed)
            reentry_confirm_ok = True
            # ✅ Log when SL cooldown blocks a fresh entry (not just pending_open)
            if (
                st.position == "NONE"
                and sig in ("BUY", "SELL")
                and cooldown_ok
                and not sl_cooldown_ok
            ):
                self.audit.event(
                    event_type="DECISION",
                    run_id=self.run_id,
                    symbol=symbol,
                    action="NOOP_SL_COOLDOWN",
                    details={
                        "signal": sig,
                        "last_stop_ms": int(getattr(st, "last_stop_ms", 0) or 0),
                        "sl_cooldown_seconds": int(
                            getattr(settings, "SL_COOLDOWN_SECONDS", 600) or 600
                        ),
                    },
                )

            last_stop_ms = int(getattr(st, "last_stop_ms", 0) or 0)
            needed_conf = int(getattr(settings, "REENTRY_CONFIRMATION_COUNT", 2) or 2)

            # Only apply confirmation after we have a stop record AND cooldown has passed AND we're flat
            if st.position == "NONE" and last_stop_ms > 0 and sig in ("BUY", "SELL"):
                if sl_cooldown_ok:
                    if getattr(st, "reentry_confirm_signal", "NONE") == sig:
                        st.reentry_confirm_count = (
                            int(getattr(st, "reentry_confirm_count", 0) or 0) + 1
                        )
                    else:
                        st.reentry_confirm_signal = sig
                        st.reentry_confirm_count = 1

                    if st.reentry_confirm_count < needed_conf:
                        reentry_confirm_ok = False
                        self.audit.event(
                            event_type="DECISION",
                            run_id=self.run_id,
                            symbol=symbol,
                            action="NOOP_REENTRY_CONFIRMATION",
                            details={
                                "signal": sig,
                                "confirm_count": st.reentry_confirm_count,
                                "needed": needed_conf,
                            },
                        )
                else:
                    # cooldown not ok => block re-entry (reentry_confirm_ok stays True but open conditions still require sl_cooldown_ok)
                    pass

            # OPTIONAL: log why pending open was blocked by SL cooldown
            if (
                st.position == "NONE"
                and st.pending_open in {"BUY", "SELL"}
                and cooldown_ok
                and not sl_cooldown_ok
            ):
                self.audit.event(
                    event_type="DECISION",
                    run_id=self.run_id,
                    symbol=symbol,
                    action="NOOP_SL_COOLDOWN",
                    details={
                        "last_stop_ms": int(getattr(st, "last_stop_ms", 0) or 0),
                        "sl_cooldown_seconds": int(
                            getattr(settings, "SL_COOLDOWN_SECONDS", 600) or 600
                        ),
                    },
                )

            # 3) If we have a pending open and we're flat, open it now
            if (
                st.position == "NONE"
                and st.pending_open in {"BUY", "SELL"}
                and cooldown_ok
                and sl_cooldown_ok
                and reentry_confirm_ok
            ):
                decision = f"OPEN_PENDING_{st.pending_open}"
                exec_signal = st.pending_open

                # Kill-switch
                if self.daily.kill:
                    exec_signal = "HOLD"
                    decision = "NOOP_KILL_SWITCH"

                # Enforce max live trades per cycle
                if (
                    exec_signal in {"BUY", "SELL"}
                    and settings.EXECUTION_MODE.lower() == "live"
                ):
                    if (
                        self.live_trades_this_cycle
                        >= settings.MAX_LIVE_TRADES_PER_CYCLE
                    ):
                        exec_signal = "HOLD"
                        decision = "NOOP_MAX_TRADES_PER_CYCLE"

                exec_result = self.executor.execute_signal(
                    symbol, exec_signal, trade_usdt
                )

                # D) Audit decision right after you compute it and before executing
                self.audit.event(
                    event_type="DECISION",
                    run_id=self.run_id,
                    symbol=symbol,
                    action=decision,
                    details={
                        "signal": sig,
                        "position": st.position,
                        "pending_open": st.pending_open,
                        "cooldown_ok": cooldown_ok,
                        "kill_switch": self.daily.kill,
                    },
                )

                # ✅ Global kill-switch enforcement: block ALL opens/adds when kill is active (closes still allowed)
                is_open_or_add = decision.startswith(
                    ("OPEN_", "ADD_")
                ) or exec_signal in ("OPEN_LONG", "OPEN_SHORT", "ADD_LONG", "ADD_SHORT")

                if self.daily.kill and is_open_or_add:
                    self.audit.event(
                        event_type="EXECUTION_RESULT",
                        run_id=self.run_id,
                        symbol=symbol,
                        action="KILL_SWITCH_BLOCKED",
                        details={
                            "decision": decision,
                            "signal": exec_signal,
                            "reason": "daily_kill_switch_true",
                        },
                    )
                    return self._finalize(
                        symbol,
                        st,
                        {
                            "symbol": symbol,
                            "signal": exec_signal,
                            "decision": decision,
                            "execution": {"action": "KILL_SWITCH_BLOCKED"},
                        },
                    )

                # D) Audit execution result right after we get it
                self.audit.event(
                    event_type="EXECUTION_RESULT",
                    run_id=self.run_id,
                    symbol=symbol,
                    action=exec_result.action,
                    details={
                        "decision": decision,
                        "signal": sig,
                        "trade_usdt": trade_usdt,
                    },
                )

                if exec_result.action == "ORDER_PLACED":
                    st.pending_open = "NONE"
                    # keep for UI until next sync refreshes entryPrice
                    st.entry_price = price
                    st.adds = 0

                    # ✅ C) Once we re-enter successfully, clear stop tracking
                    st.last_stop_ms = 0
                    st.reentry_confirm_signal = "NONE"
                    st.reentry_confirm_count = 0

                    mark_trade(decision)

                if exec_result.action in {
                    "ORDER_PLACED",
                    "CLOSED_LONG",
                    "CLOSED_SHORT",
                }:
                    if settings.EXECUTION_MODE.lower() == "live":
                        self.live_trades_this_cycle += 1

                return self._finalize(
                    symbol,
                    st,
                    {
                        "symbol": symbol,
                        "price": price,
                        "signal": sig,
                        "position": st.position,
                        "pending_open": st.pending_open,
                        "trade_usdt": trade_usdt,
                        "decision": decision,
                        "cooldown_ok": cooldown_ok,
                        "daily_realized_pnl": self.daily.realized_pnl,
                        "kill_switch": self.daily.kill,
                        "execution": {
                            "action": exec_result.action,
                            "details": exec_result.details,
                        },
                    },
                )

            # 4) Decide what to do
            decision = "NOOP"
            exec_signal = "HOLD"

            if sig == "BUY":
                if (
                    st.position == "NONE"
                    and cooldown_ok
                    and sl_cooldown_ok
                    and reentry_confirm_ok
                ):
                    decision = "OPEN_LONG"
                    exec_signal = "BUY"

                elif st.position == "SHORT" and cooldown_ok:
                    # close short first; open long next cycle
                    decision = "CLOSE_SHORT_FOR_FLIP"
                    exec_signal = "BUY"
                    st.pending_open = "BUY"

                elif st.position == "LONG":
                    if (
                        settings.TRADE_MODE == "repeat"
                        and cooldown_ok
                        and sl_cooldown_ok
                        and reentry_confirm_ok
                        and st.adds < settings.MAX_ADDS_PER_POSITION
                    ):
                        st.adds += 1
                        decision = f"ADD_LONG_{st.adds}"
                        exec_signal = "BUY"

            elif sig == "SELL":
                if (
                    st.position == "NONE"
                    and cooldown_ok
                    and sl_cooldown_ok
                    and reentry_confirm_ok
                ):
                    decision = "OPEN_SHORT"
                    exec_signal = "SELL"

                elif st.position == "LONG" and cooldown_ok:
                    # close long first; open short next cycle
                    decision = "CLOSE_LONG_FOR_FLIP"
                    exec_signal = "SELL"
                    st.pending_open = "SELL"

                elif st.position == "SHORT":
                    if (
                        settings.TRADE_MODE == "repeat"
                        and cooldown_ok
                        and sl_cooldown_ok
                        and reentry_confirm_ok
                        and st.adds < settings.MAX_ADDS_PER_POSITION
                    ):
                        st.adds += 1
                        decision = f"ADD_SHORT_{st.adds}"
                        exec_signal = "SELL"

            # Kill-switch (before executing any trade signal)
            if self.daily.kill:
                exec_signal = "HOLD"
                decision = "NOOP_KILL_SWITCH"

            # Enforce max live trades per cycle (right before calling executor)
            if (
                exec_signal in {"BUY", "SELL"}
                and settings.EXECUTION_MODE.lower() == "live"
            ):
                if self.live_trades_this_cycle >= settings.MAX_LIVE_TRADES_PER_CYCLE:
                    exec_signal = "HOLD"
                    decision = "NOOP_MAX_TRADES_PER_CYCLE"

            # D) Audit decision right after you compute it and before executing
            self.audit.event(
                event_type="DECISION",
                run_id=self.run_id,
                symbol=symbol,
                action=decision,
                details={
                    "signal": sig,
                    "position": st.position,
                    "pending_open": st.pending_open,
                    "cooldown_ok": cooldown_ok,
                    "kill_switch": self.daily.kill,
                },
            )

            # 5) Execute
            if exec_signal not in {"BUY", "SELL", "CLOSE"}:
                exec_result = ExecResult(
                    "NO_TRADE", {"reason": "noop", "signal": exec_signal}
                )
            else:
                exec_result = self.executor.execute_signal(
                    symbol, exec_signal, trade_usdt
                )

            # D) Audit execution result right after we get it
            self.audit.event(
                event_type="EXECUTION_RESULT",
                run_id=self.run_id,
                symbol=symbol,
                action=exec_result.action,
                details={
                    "decision": decision,
                    "signal": sig,
                    "trade_usdt": trade_usdt,
                },
            )

            # Count trade actions this cycle (safer: count closes too)
            if exec_result.action in {"ORDER_PLACED", "CLOSED_LONG", "CLOSED_SHORT"}:
                if settings.EXECUTION_MODE.lower() == "live":
                    self.live_trades_this_cycle += 1

            # If we executed an order, update local meta (exchange remains source of truth)
            if exec_result.action == "ORDER_PLACED":
                mark_trade(decision)
                if decision.startswith(("OPEN_", "ADD_", "OPEN_PENDING_")):
                    # keep for UI until next sync refreshes entryPrice
                    st.entry_price = price

                # ✅ C) Once we re-enter successfully, clear stop tracking (OPEN / OPEN_PENDING only)
                if decision.startswith(("OPEN_", "OPEN_PENDING_")):
                    st.last_stop_ms = 0
                    st.reentry_confirm_signal = "NONE"
                    st.reentry_confirm_count = 0

            # When a close happens: update realized PnL and trigger kill-switch if needed
            if exec_result.action in {"CLOSED_LONG", "CLOSED_SHORT", "CLOSED_POSITION"}:
                # ✅ Confirm position is actually flat before counting realized pnl
                flat = False
                try:
                    # small wait/retry because close fill can be slightly delayed
                    for _ in range(5):
                        if self._is_flat(symbol):
                            flat = True
                            break
                        time.sleep(0.3)
                except Exception:
                    pass

                pnl = 0.0
                if flat:
                    try:
                        end_ms = int(time.time() * 1000)
                        start_ms = end_ms - (10 * 60 * 1000)  # last 10 minutes

                        trades = self.executor.client.user_trades(
                            symbol=symbol,
                            start_time_ms=start_ms,
                            end_time_ms=end_ms,
                            limit=1000,
                        )

                        # Dedup: only count fills not processed yet
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

                        pnl = realized_pnl_from_user_trades(new_trades)
                        st.last_user_trade_id = max_id

                    except Exception as e:
                        # ------------------------------------------------------------------
                        # Fallback (keep your old estimate logic — NOT REMOVED)
                        # ------------------------------------------------------------------
                        exit_px = price
                        qty = st.entry_qty
                        entry_px = st.entry_price

                        # Fallback: if for any reason entry/qty not available, try exchange risk endpoint
                        if (entry_px is None) or (qty <= 0):
                            try:
                                pos_risk = self.executor.client.position_risk(symbol)
                                if isinstance(pos_risk, list) and pos_risk:
                                    if entry_px is None:
                                        entry_px = float(
                                            pos_risk[0].get("entryPrice", "0") or 0.0
                                        )
                                    if qty <= 0:
                                        qty = abs(pos_amt)
                            except Exception:
                                pass

                        if entry_px is not None and qty > 0:
                            if exec_result.action == "CLOSED_LONG":
                                pnl = (exit_px - entry_px) * qty
                            else:  # CLOSED_SHORT
                                pnl = (entry_px - exit_px) * qty

                        # log the fallback
                        try:
                            self.audit.event(
                                event_type="ERROR",
                                run_id=self.run_id,
                                symbol=symbol,
                                action="REALIZED_PNL_USERTRADES_FAILED",
                                details={
                                    "error": f"{type(e).__name__}: {e}",
                                    "fallback_pnl": pnl,
                                },
                            )
                        except Exception:
                            pass

                    # ✅ Keep your existing daily pnl/kill logic (NOT REMOVED)
                    self.daily.realized_pnl += pnl
                    if self.daily.realized_pnl < -settings.DAILY_MAX_LOSS_USDT:
                        self.daily.kill = True

                    # ✅ Persist daily state immediately after any realized pnl update
                    self.store.save_daily(
                        self.daily.day, self.daily.realized_pnl, self.daily.kill
                    )

                    # if kill just triggered, activate it
                    if self.daily.kill:
                        self.activate_kill_switch()

                    mark_trade(decision)
                    return self._finalize(
                        symbol,
                        st,
                        {
                            "symbol": symbol,
                            "price": price,
                            "signal": sig,
                            "position": st.position,
                            "pending_open": st.pending_open,
                            "trade_usdt": trade_usdt,
                            "entry_price": st.entry_price,
                            "entry_qty": st.entry_qty,
                            "adds": st.adds,
                            "decision": decision,
                            "cooldown_ok": cooldown_ok,
                            "realized_pnl_added": pnl,
                            "daily_realized_pnl": self.daily.realized_pnl,
                            "kill_switch": self.daily.kill,
                            "execution": {
                                "action": exec_result.action,
                                "details": exec_result.details,
                            },
                            "note": "Closed position confirmed flat; pnl counted. Will open pending direction on next cycle after sync shows NONE.",
                        },
                    )

                else:
                    # Not flat yet → do NOT count pnl (prevents double counting / wrong pnl)
                    try:
                        self.audit.event(
                            event_type="INFO",
                            run_id=self.run_id,
                            symbol=symbol,
                            action="CLOSE_NOT_CONFIRMED_FLAT",
                            details={
                                "note": "Position not flat after close attempt; pnl not counted yet."
                            },
                        )
                    except Exception:
                        pass

                    # Persist daily state (even if unchanged)
                    self.store.save_daily(
                        self.daily.day, self.daily.realized_pnl, self.daily.kill
                    )

                    mark_trade(decision)
                    return self._finalize(
                        symbol,
                        st,
                        {
                            "symbol": symbol,
                            "price": price,
                            "signal": sig,
                            "position": st.position,
                            "pending_open": st.pending_open,
                            "trade_usdt": trade_usdt,
                            "entry_price": st.entry_price,
                            "entry_qty": st.entry_qty,
                            "adds": st.adds,
                            "decision": decision,
                            "cooldown_ok": cooldown_ok,
                            "realized_pnl_added": 0.0,
                            "daily_realized_pnl": self.daily.realized_pnl,
                            "kill_switch": self.daily.kill,
                            "execution": {
                                "action": exec_result.action,
                                "details": exec_result.details,
                            },
                            "note": "Close action received but position not confirmed flat yet; pnl not counted to prevent duplication.",
                        },
                    )

            return self._finalize(
                symbol,
                st,
                {
                    "symbol": symbol,
                    "price": price,
                    "signal": sig,
                    "position": st.position,
                    "pending_open": st.pending_open,
                    "trade_usdt": trade_usdt,
                    "entry_price": st.entry_price,
                    "entry_qty": st.entry_qty,
                    "adds": st.adds,
                    "decision": decision,
                    "cooldown_ok": cooldown_ok,
                    "daily_realized_pnl": self.daily.realized_pnl,
                    "kill_switch": self.daily.kill,
                    "execution": {
                        "action": exec_result.action,
                        "details": exec_result.details,
                    },
                },
            )

        finally:
            # ✅ ALWAYS unlock
            try:
                lock.release()
            except Exception:
                pass

    def run_once(self, max_symbols: int = 10) -> Dict[str, Any]:
        # ✅ Robust solution: one cycle lock (prevents overlapping cycles)
        if not self._cycle_lock.acquire(blocking=False):
            # Another cycle is still running → skip
            try:
                self.audit.event(
                    event_type="CYCLE_SKIPPED",
                    run_id=self.run_id,
                    action="CYCLE_ALREADY_RUNNING",
                    details={"note": "Previous cycle still running"},
                )
            except Exception:
                pass

            return {"skipped": True, "reason": "CYCLE_ALREADY_RUNNING"}

        try:
            # ✅ keep your existing run_once logic below
            self.live_trades_this_cycle = 0

            # Daily reset (new day)
            self.daily.reset_if_new_day()

            # Persist daily state (even if unchanged)
            self.store.save_daily(
                self.daily.day, self.daily.realized_pnl, self.daily.kill
            )

            # --- CYCLE AUDIT START ---
            cycle_id = str(uuid.uuid4())
            self.audit.event(
                event_type="CYCLE_START",
                run_id=self.run_id,
                cycle_id=cycle_id,
                details={
                    "interval": self.interval,
                    "max_symbols": max_symbols,
                    "execution_mode": settings.EXECUTION_MODE.lower(),
                    "kill_switch": self.daily.kill,
                    "daily_realized_pnl": self.daily.realized_pnl,
                },
            )
            # --- END CYCLE AUDIT START ---

            if settings.EXECUTION_MODE.lower() == "live":
                symbols = list(settings.LIVE_SYMBOLS)
            else:
                symbols = list(settings.TRADE_SYMBOLS)

            symbols = symbols[:max_symbols]

            results = []
            for s in symbols:
                results.append(self.step_symbol(s))

            # --- CYCLE AUDIT END ---
            self.audit.event(
                event_type="CYCLE_END",
                run_id=self.run_id,
                cycle_id=cycle_id,
                details={
                    "ran": len(results),
                    "live_trades_this_cycle": self.live_trades_this_cycle,
                    "kill_switch": self.daily.kill,
                    "daily_realized_pnl": self.daily.realized_pnl,
                },
            )
            # --- END CYCLE AUDIT END ---

            return {
                "interval": self.interval,
                "ran": len(results),
                "live_trades_this_cycle": self.live_trades_this_cycle,
                "daily_realized_pnl": self.daily.realized_pnl,
                "kill_switch": self.daily.kill,
                "results": results,
            }

        finally:
            self._cycle_lock.release()

    def record_realized_pnl_from_usertrades(
        self, symbol: str, window_minutes: int = 30
    ) -> float:
        """
        Pull userTrades, dedup by SymbolState.last_user_trade_id, add realized pnl to daily.
        Assumes position is already FLAT.
        """
        import time

        symbol = symbol.upper()
        st = self.state.get(symbol)
        if st is None:
            st = SymbolState()
            self.state[symbol] = st

        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (max(1, window_minutes) * 60 * 1000)

        trades = self.executor.client.user_trades(
            symbol=symbol,
            start_time_ms=start_ms,
            end_time_ms=end_ms,
            limit=1000,
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

        # persist symbol state (dedup survives restart)
        try:
            self.store.save_symbol(symbol, st)
        except Exception:
            pass

        # daily accounting
        self.daily.reset_if_new_day()
        self.daily.realized_pnl += pnl_added
        if self.daily.realized_pnl <= -settings.DAILY_MAX_LOSS_USDT:
            self.daily.kill = True

        self.store.save_daily(self.daily.day, self.daily.realized_pnl, self.daily.kill)

        # audit
        self.audit.event(
            event_type="REALIZED_PNL",
            run_id=self.run_id,
            symbol=symbol,
            action="PNL_RECORDED_USERTRADES",
            details={
                "window_minutes": window_minutes,
                "new_trades": len(new_trades),
                "pnl_added": pnl_added,
                "daily_realized_pnl": self.daily.realized_pnl,
                "kill": self.daily.kill,
            },
        )

        return pnl_added

    # app/runner/runner.py (inside class PaperRunner)
    def reconcile_positions(self) -> None:
        """
        Startup reconciliation:
        - Reads exchange positionRisk (truth)
        - Rebuilds state for symbols we care about
        - Clears pending_open to avoid accidental "ghost trades" after restart
        """
        try:
            positions = self.client.position_risk(None)  # returns list for all symbols
            if not isinstance(positions, list):
                return

            pos_map = {p.get("symbol"): p for p in positions if p.get("symbol")}

            for sym in self.symbols:
                st = self.state[sym]
                p = pos_map.get(sym)

                # Clear any pending open after restart
                st.pending_open = "NONE"
                st.adds = 0

                if not p:
                    st.position = "NONE"
                    st.entry_price = None
                    st.entry_qty = 0.0
                    continue

                amt = float(p.get("positionAmt", "0") or "0")
                entry = float(p.get("entryPrice", "0") or "0")

                if amt > 0:
                    st.position = "LONG"
                    st.entry_price = entry if entry > 0 else None
                    st.entry_qty = abs(amt)

                elif amt < 0:
                    st.position = "SHORT"
                    st.entry_price = entry if entry > 0 else None
                    st.entry_qty = abs(amt)

                else:
                    st.position = "NONE"
                    st.entry_price = None
                    st.entry_qty = 0.0

                # Persist reconciled state immediately
                self.store.save_symbol(sym, st)

        except Exception:
            # Never crash startup due to reconciliation
            return

    def reconcile_positions_from_exchange(self) -> None:
        """
        Startup reconciliation:
        Exchange is source of truth. If we have a live position, override DB state.
        """
        for sym in self.symbols:
            try:
                pos = self.client.get_position_info(sym)
                if not pos:
                    continue

                amt = float(pos.get("positionAmt", "0") or 0)
                entry = float(pos.get("entryPrice", "0") or 0)

                st = self.state.get(sym)
                if st is None:
                    continue

                if amt > 0:
                    st.position = "LONG"
                    st.entry_price = entry if entry > 0 else st.entry_price
                    st.entry_qty = abs(amt)

                elif amt < 0:
                    st.position = "SHORT"
                    st.entry_price = entry if entry > 0 else st.entry_price
                    st.entry_qty = abs(amt)

                else:
                    # flat on exchange → reset local state
                    st.position = "NONE"
                    st.entry_price = None
                    st.entry_qty = 0.0
                    st.adds = 0
                    st.pending_open = "NONE"

                # persist reconciled state
                self.store.save_symbol(sym, st)

            except Exception:
                # don't crash startup because one symbol failed
                continue
