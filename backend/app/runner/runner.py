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
from app.strategy.loader import build_strategy
from app.strategy.sma_cross import signal_from_closes
from app.symbols.sizing import parse_usdt_map, usdt_for
from app.symbols.universe import parse_symbols
from app.execution.confirm import wait_until_flat
from app.execution.position_manager import should_exit
from app.execution.exit_rules import should_close_position
from app.persistence.trade_fills import record_fill
from app.policy.trade_policy import PolicyInputs, decide, Action
from app.risk.realized_pnl import record_realized_pnl_for_symbol
from app.metrics.hooks import on_trade_close_update_metrics


# ✅ ADD: wire RiskGate into runner (dependency injection)
# ✅ ADD: wire RiskGate into runner (dependency injection)
from app.risk.gate import RiskGate, GateSettings
from app.risk.sizing import PositionSizer, calculate_atr
from app.risk.drawdown import DrawdownMonitor
from app.risk.circuit import get_circuit_registry
from app.risk.risk_budget import get_risk_budget_engine
from app.risk.invariant_checker import get_invariant_checker
from app.metrics.health import StrategyHealthMonitor

# ✅ ADD: Monitoring infrastructure
from app.persistence.trace_recorder import get_trace_recorder, StrategySignal

# ✅ ADD: cycle context helpers
from app.ops.context import set_cycle_id, clear_cycle_id, set_run_id, clear_run_id

# ✅ ADD: Trading Orchestrator & Governance
from app.core.trading_orchestrator import TradingOrchestrator
from app.core.user_strategy_config_service import UserStrategyConfigService
from app.risk.system_limits import UserConfigurableLimits, RiskLevel


def _norm_pos(p: str | None) -> str:
    if not p:
        return "flat"
    p = str(p).upper()
    if p in ("NONE", "FLAT"):
        return "flat"
    if p in ("LONG", "BUY"):
        return "long"
    if p in ("SHORT", "SELL"):
        return "short"
    # safe default
    return "flat"


def _norm_pending(x: str | None):
    if not x:
        return None
    x = str(x).upper()
    if x in ("NONE", "NULL", "0", ""):
        return None
    if x in ("BUY", "SELL"):
        return x
    return None


class PaperRunner:

    def __init__(self, client: BinanceFuturesClient):
        self.client = client
        self.settings = settings
        # ✅ Store last signal confidence per symbol (used on CLOSE)
        self.last_signal_confidence: dict[str, float] = {}

        # ---- Basic config / strategy ----

        self.symbols = parse_symbols(
            ",".join(settings.TRADE_SYMBOLS), settings.MAX_SYMBOLS
        )
        self.interval = settings.DEFAULT_INTERVAL

        self.strategy = build_strategy(
            name=settings.STRATEGY_NAME,
            client=self.client,
            interval=self.interval,
            params_json=settings.STRATEGY_PARAMS_JSON or None,
        )

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
        # Track symbols that were CLOSED this cycle (for post-cycle realized pnl sync)
        self._closed_symbols_this_cycle: set[str] = set()

        # Daily loss kill-switch state
        self.daily = DailyLossState(day=date.today())

        # Drawdown monitor
        self.drawdown_monitor = DrawdownMonitor(self.store)

        # Health monitor
        self.health_monitor = StrategyHealthMonitor(self.db)

        # Circuit Breaker (Universal Registry)
        self.circuit_registry = get_circuit_registry()

        # Risk Budget Engine
        self.budget_engine = get_risk_budget_engine()

        # Gate Settings
        gate_settings = GateSettings(
            max_loss_usdt=settings.DAILY_MAX_LOSS_USDT,
            max_trades_daily=getattr(settings, "MAX_TRADES_DAILY", 20),
            max_open_positions=getattr(settings, "MAX_OPEN_POSITIONS", 3),
            max_weekly_drawdown_pct=getattr(settings, "MAX_WEEKLY_DRAWDOWN_PCT", 0.0),
            max_monthly_drawdown_pct=getattr(settings, "MAX_MONTHLY_DRAWDOWN_PCT", 0.0),
        )

        # ✅ ADD (as requested): create RiskGate
        self.risk_gate = RiskGate(
            settings=gate_settings,
            drawdown_monitor=self.drawdown_monitor,
            budget_engine=self.budget_engine,
        )

        # ✅ CHANGE: pass risk_gate + audit into executor
        self.executor = BinanceExecutor(
            client=self.client,
            risk_gate=self.risk_gate,
            audit=self.audit
        )
        self.sizer = PositionSizer(
            account_risk_pct=getattr(settings, "ACCOUNT_RISK_PCT", 1.0),
            default_usdt=settings.TRADE_USDT_PER_ORDER,
            max_notional=settings.TRADE_USDT_PER_ORDER,  # ✅ FIX: Cap trades at configured size
        )
        self.cached_balance = 0.0
        self.last_balance_time = 0.0

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
        
        # ✅ LOAD ORCHESTRATOR
        self.orchestrator: TradingOrchestrator | None = None
        self._load_orchestrator()

    def _load_orchestrator(self):
        """Attempts to load active user configuration and initialize orchestrator."""
        try:
            config_service = UserStrategyConfigService(self.db)
            broker_account_id = getattr(self.settings, "ACCOUNT_ID", "default")
            
            # Helper to get active config (using the service)
            active_config = config_service.get_active_config_for_account(broker_account_id)
            
            if active_config:
                risk_params = active_config.risk_parameters
                
                # Map to UserConfigurableLimits
                try:
                    risk_level = RiskLevel(risk_params.risk_profile)
                except ValueError:
                    risk_level = RiskLevel.MEDIUM
                
                # Parse additional params from JSON
                extra_params = {}
                if hasattr(risk_params, "parameters_json"):
                     try:
                         extra_params = json.loads(risk_params.parameters_json)
                     except:
                         pass

                user_config = UserConfigurableLimits(
                    risk_level=risk_level,
                    max_daily_loss_pct=risk_params.daily_loss_limit_pct,
                    max_open_positions=risk_params.max_position_slots,
                    # Use current settings for symbols/paper mode as defaults
                    allowed_symbols=self.settings.TRADE_SYMBOLS,
                    paper_mode=self.settings.EXECUTION_MODE == "paper",
                    min_strategy_confidence=float(extra_params.get("min_confidence_score", 0.5)),
                    volatility_filter_enabled=bool(extra_params.get("volatility_filter_enabled", True)),
                    strict_circuit_breakers=bool(extra_params.get("strict_circuit_breakers", False))
                )
                
                self.orchestrator = TradingOrchestrator(
                    config_id=active_config.id,
                    user_config=user_config,
                    strategy_id=active_config.strategy_id,
                    broker_id=getattr(self.settings, "BROKER_ID", "binance_futures")
                )
                print(f"✅ Loaded TradingOrchestrator for config {active_config.id}")
            else:
                # If no config in DB, we rely on legacy settings-based operation
                # or we could auto-create a default config?
                print(f"ℹ️ No active configuration found for account {broker_account_id}, using legacy mode")
                
        except Exception as e:
            print(f"⚠️ Failed to load orchestrator: {e}")
            # Do not crash, fall back to legacy


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
            
            # ✅ Finalize trace if active
            recorder = get_trace_recorder()
            recorder.finalize(
                trace_id=getattr(recorder, "_active_trace_id", None) or payload.get("trace_id", ""),  # fallback
                state_change=payload.get("decision", "NONE"),
                final_position=st.position,
            )
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
        """True if ALL position sides for symbol are effectively flat.

        Binance futures can return multiple rows for the same symbol (hedge mode LONG/SHORT).
        Also, after a market close there can be tiny residual 'dust' amounts, so we use an epsilon.
        """
        symbol_u = symbol.upper()

        try:
            data = self.executor.client.position_risk(
                symbol_u
            )  # returns list in most cases
        except Exception:
            # fallback to older helper
            pos_info = self.executor.client.get_position_info(symbol_u)
            if not pos_info:
                return True
            try:
                amt = float(pos_info.get("positionAmt", "0") or 0.0)
            except Exception:
                amt = 0.0
            return abs(amt) < 1e-8

        if not data:
            return True

        total_abs = 0.0
        rows = data if isinstance(data, list) else [data]
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("symbol", "").upper() != symbol_u:
                continue
            try:
                amt = float(row.get("positionAmt", "0") or 0.0)
            except Exception:
                amt = 0.0
            total_abs += abs(amt)

        return total_abs < 1e-8

        return total_abs < 1e-8

    def get_account_balance(self) -> float:
        now = time.time()
        if now - self.last_balance_time < 60:
            return self.cached_balance
        try:
            # Need available balance for future trades
            # For futures: 'availableBalance' or 'totalWalletBalance' depending on risk view
            # We'll use totalWalletBalance for sizing base (equity)
            acc = self.client.account()
            self.cached_balance = float(acc.get("totalWalletBalance", 0.0))
            self.last_balance_time = now
            
            # Update exchange cache for fast API responses
            try:
                from app.exchange.cache import get_exchange_cache
                pos_data = self.client.position_risk()
                get_exchange_cache().update_from_exchange(acc, pos_data)
            except Exception:
                pass  # Don't fail balance fetch if cache update fails
                
        except Exception:
            # ✅ Record circuit breaker error (Universal)
            self.circuit_registry.record_error("BINANCE")
            pass
        return self.cached_balance

    def _step_symbol_orchestrated(self, symbol: str, klines: Any, trace_id: str) -> Dict[str, Any]:
        """
        Orchestrator-driven processing. Replacement for legacy logic.
        """
        st = self.state[symbol]
        
        # 1. Sync State (Simplified)
        try:
            pos_info = self.executor.client.get_position_info(symbol)
            if pos_info:
                pos_amt = float(pos_info.get("positionAmt", "0"))
                st.entry_price = float(pos_info.get("entryPrice", "0"))
                if abs(pos_amt) > 1e-12:
                    st.position = "LONG" if pos_amt > 0 else "SHORT"
                    st.entry_qty = abs(pos_amt)
                else:
                    st.position = "NONE"
                    st.entry_qty = 0.0
        except Exception:
            pass # Use cached state if sync fails
            
        price = float(self.client.last_price(symbol))
        
        # 2. Check Exits (Legacy logic)
        if st.position in ("LONG", "SHORT"):
             exit_now, exit_reason = should_exit(
                position=st.position,
                entry_price=st.entry_price,
                price=price,
                last_trade_ms=st.last_trade_ms,
                signal="HOLD" 
             )
             if exit_now:
                 res = self.executor.execute_signal(symbol, "CLOSE", 0.0)
                 return {"symbol": symbol, "decision": f"CLOSE_{exit_reason}", "details": res.details}

        # 3. Process Entry via Orchestrator
        try:
            acc_data = self.client.account()
            equity = float(acc_data.get("totalWalletBalance", 0.0))
            margin_used = float(acc_data.get("totalMaintMargin", 0.0) or 0.0)
            margin_avail = float(acc_data.get("availableBalance", 0.0))
        except:
            equity = self.get_account_balance()
            margin_used = 0.0
            margin_avail = equity

        open_pos_count = 0
        total_exposure = 0.0
        for s in self.state.values():
            if s.position in ("LONG", "SHORT"):
                open_pos_count += 1
                total_exposure += (s.entry_price or 0.0) * (s.entry_qty or 0.0)
        
        orch_res = self.orchestrator.process_trading_opportunity(
            symbol=symbol,
            klines=klines,
            current_price=price,
            current_equity=equity,
            margin_used=margin_used,
            margin_available=margin_avail,
            open_positions=open_pos_count,
            total_exposure=total_exposure,
            client=self.client,
            run_id=trace_id
        )
        
        decision = orch_res["decision"]
        st.last_signal = orch_res["details"].get("strategy_output", {}).get("signal", "HOLD")
        
        if decision == "execute" and st.position == "NONE":
            p = orch_res["trade_params"]
            trade_usdt = p["quantity"] * p["entry_price"]
            
            res = self.executor.execute_signal(
                symbol, 
                p["side"], 
                trade_usdt,
                current_open_count=open_pos_count,
                current_equity=equity
            )
            
            # Feedback to orchestrator for Layer D monitoring
            self.orchestrator.record_trade_execution(
                symbol=symbol,
                success=res.success,
                expected_price=p["entry_price"],
                executed_price=res.avg_price if res.success else None,
                error_message=res.error
            )
            
            return {"symbol": symbol, "decision": "EXECUTE", "details": res.details}
            
        return {"symbol": symbol, "decision": decision, "details": orch_res.get("reason")}


    def step_symbol(self, symbol: str) -> Dict[str, Any]:
        # ✅ START TRACE
        recorder = get_trace_recorder()
        trace_id = recorder.start_trace(
            run_id=self.run_id,
            cycle_id=self.cycle_id,
            symbol=symbol,
            account_id=getattr(settings, "ACCOUNT_ID", "default"),
            environment=getattr(settings, "EXECUTION_MODE", "paper"),
            timeframe=self.interval,
        )
        # Store for internal use if needed (hacky, but simple)
        recorder._active_trace_id = trace_id

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
            
            # ✅ FAST EXIT CHECK
            if getattr(self, "_stop_requested", False):
                return {"symbol": symbol, "skipped": True, "reason": "STOP_REQUESTED"}

            # ✅ ORCHESTRATOR COMPATIBILITY LAYER
            if self.orchestrator:
                return self._step_symbol_orchestrated(symbol, kl, trace_id)

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
            # ✅ STORE CONFIDENCE FOR THIS SYMBOL (used later on CLOSE)
            try:
                self.last_signal_confidence[symbol] = float(res.confidence or 0.0)
            except Exception:
                self.last_signal_confidence[symbol] = 0.0

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
                    "policy_reason": res.reason,
                },
                trace_id=trace_id,  # ✅ Link audit to trace
            )
            
            # ✅ Record Strategy Signals
            recorder.record_strategies(
                trace_id,
                signals=[
                    StrategySignal(
                        strategy_name=getattr(self.strategy, "name", "unknown"),
                        signal=sig,
                        confidence=float(res.confidence or 0.0),
                        reason=str(res.reason),
                        meta=res.meta or {},
                    )
                ],
                chosen_strategy=getattr(self.strategy, "name", "unknown"),
                final_signal=sig,
                final_confidence=float(res.confidence or 0.0),
            )

            price = self.client.last_price(symbol)
            if price is None:
                raise ValueError("price_unavailable")

            # ✅ Record Market Snapshot
            acc_balance_map = self.get_account_balance()
            equity_val = float(acc_balance_map if isinstance(acc_balance_map, (int, float)) else acc_balance_map.get("equity", 0.0))
            
            # Calculate Margin Level (Best effort)
            margin_level = 999.0
            margin_used = 0.0
            try:
                acc_data = self.client.account()
                if isinstance(acc_data, dict):
                    maint = float(acc_data.get("totalMaintMargin", 0.0) or 0.0)
                    bal = float(acc_data.get("totalMarginBalance", 0.0) or 0.0)
                    margin_used = maint
                    if maint > 0:
                        margin_level = (bal / maint) * 100
            except Exception:
                pass

            # ✅ Extract Always-On Risk Metrics
            kill_switch = "NORMAL"
            exposure_freeze = False
            portfolio_risk_budget = 0.0
            portfolio_risk_used = 0.0
            
            try:
                # Access risk state if available
                if hasattr(self, "risk_state") and self.risk_state:
                    if self.risk_state.daily.kill:
                        kill_switch = "HARD_KILL"
                elif hasattr(self, "daily") and self.daily.kill:
                     kill_switch = "HARD_KILL"
                
                # Access budget engine if available
                # Try to access via gate
                if hasattr(self, "risk_gate") and hasattr(self.risk_gate, "budget_engine") and self.risk_gate.budget_engine:
                    bg = self.risk_gate.budget_engine
                    state = bg.get_budget_state()
                    portfolio_risk_budget = state.portfolio_risk_budget
                    portfolio_risk_used = state.total_risk_usdt
                    
                    # Exposure freeze logic (heuristic based on budget saturation)
                    if state.allowed_slots <= state.position_count:
                         exposure_freeze = True
            except Exception as e:
                print(f"Error extracting risk metrics: {e}")

            recorder.record_market(
                trace_id,
                last_price=float(price),
                equity=equity_val,
                margin_used=margin_used,
                margin_level=margin_level,
                open_positions_count=len(self.positions) if hasattr(self, 'positions') else 0,
                # Always-On Fields
                regime_state="STANDARD", # Placeholder
                kill_switch_state=kill_switch,
                exposure_freeze=exposure_freeze,
                portfolio_risk_budget=portfolio_risk_budget,
                portfolio_risk_used=portfolio_risk_used,
            )
            
            # ✅ FAST EXIT CHECK
            if getattr(self, "_stop_requested", False):
                return {"symbol": symbol, "skipped": True, "reason": "STOP_REQUESTED"}

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
            entry_price = float(pos_info.get("entryPrice", "0")) if pos_info else 0.0
            
            # ✅ FAST EXIT CHECK
            if getattr(self, "_stop_requested", False):
                return {"symbol": symbol, "skipped": True, "reason": "STOP_REQUESTED"}

            # --- STATE SYNC HARDENING (broker truth wins) ---
            # Normalize broker position
            broker_pos = "NONE"
            if abs(pos_amt) > 1e-12:
                broker_pos = "LONG" if pos_amt > 0 else "SHORT"

            # If broker says flat, hard reset local state
            if broker_pos == "NONE":
                if st.position != "NONE" or st.entry_qty or st.entry_price or st.adds:
                    self.audit.event(
                        event_type="STATE_SYNC",
                        run_id=self.run_id,
                        symbol=symbol,
                        action="RESET_TO_FLAT",
                        details={
                            "prev_position": st.position,
                            "prev_entry_price": st.entry_price,
                            "prev_entry_qty": st.entry_qty,
                            "prev_adds": st.adds,
                        },
                    )
                st.position = "NONE"
                st.entry_price = None
                st.entry_qty = 0.0
                st.adds = 0
                st.pending_open = "NONE"

            # If broker says in-position, enforce local to match broker
            else:
                if st.position != broker_pos:
                    self.audit.event(
                        event_type="STATE_SYNC",
                        run_id=self.run_id,
                        symbol=symbol,
                        action="BROKER_OVERRIDES_LOCAL",
                        details={
                            "local_position": st.position,
                            "broker_position": broker_pos,
                            "pos_amt": float(pos_amt),
                        },
                    )
                    st.position = broker_pos

                # Keep entry consistent if broker provides it
                if entry_price and float(entry_price) > 0:
                    st.entry_price = float(entry_price)
                st.entry_qty = abs(float(pos_amt))

            """""

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

            """ ""
            # Per-symbol USDT sizing
            trade_usdt = usdt_for(symbol, self.usdt_map, settings.TRADE_USDT_PER_ORDER)

            # ✅ RISK SIZING (if enabled and applicable)
            # Only calculate if we might open/add (signal is BUY/SELL) or just always for logging?
            # Let's do it if signal != HOLD to save perf.
            if sig in ("BUY", "SELL"):
                try:
                    # 4) Calculate Position Size (Dynamic Risk)
                    atr_val = calculate_atr(kl, period=14) # Assuming 'kl' is klines from market data

                    # TODO: If strategy provides confidence, use it here. Default 1.0.
                    confidence = self.last_signal_confidence.get(symbol, 1.0)
                    
                    sizer_res = self.sizer.calculate_atr_size(
                        account_balance=self.get_account_balance(),
                        entry_price=price, # Assuming 'price' is current_price
                        atr=atr_val,
                        confidence=confidence,
                    )
                    
                    if sizer_res.qty > 0:
                        trade_usdt = sizer_res.size_usdt
                        # Audit sizing justification (optional, or rely on execution audit)
                    else:
                        # Sizer returned 0 (e.g. risk too small for min notional)
                        # or it needs to be handled differently if this is the only place
                        # where trade_usdt is set to 0.0 due to sizing.
                        # For now, just set trade_usdt to 0.0
                        trade_usdt = 0.0
                except Exception:
                    # If sizing fails, proceed with default trade_usdt or 0.0
                    pass

            # --- AUTO EXIT MANAGEMENT (priority) ---
            exit_now, exit_reason = should_exit(
                position=st.position,
                entry_price=st.entry_price,
                price=price,
                last_trade_ms=st.last_trade_ms,
                signal=sig,
            )

            # Calculate current open positions for risk gate
            current_open_count = sum(
                1 for s in self.state.values() if s.position in ("LONG", "SHORT")
            )

            # ✅ FIXED FULL EXIT BLOCK (no placeholders, no logic removed — only adjustments)
            if exit_now and st.position in {"LONG", "SHORT"}:
                decision = f"CLOSE_{st.position}_{exit_reason}"
                exec_signal = "CLOSE"

                # ✅ ADJUSTMENT: capture side BEFORE we potentially set st.position = "NONE"
                pos_before_close = st.position

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
                    trace_id=trace_id,  # ✅ Link
                )

                # ✅ Record Intent (Exit)
                recorder.record_intent(
                    trace_id,
                    action=decision,
                    sizing={"trade_usdt": trade_usdt},
                )

                exec_result = self.executor.execute_signal(
                    symbol, exec_signal, trade_usdt, current_open_count=current_open_count
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
                    trace_id=trace_id,  # ✅ Link
                )

                # ✅ Record Execution (Exit)
                recorder.record_execution(
                    trace_id,
                    status=exec_result.action,
                    order_id=exec_result.order_id,
                    fill_price=float(exec_result.details.get("avg_price", 0.0) or 0.0),
                    fill_qty=float(exec_result.details.get("executed_qty", 0.0) or 0.0),
                    error=exec_result.error,
                )

                # ✅ After closing: record realized pnl from fills (dedup-safe, works even if re-entry happens fast)
                if exec_signal == "CLOSE" and exec_result.action in {
                    "CLOSED_LONG",
                    "CLOSED_SHORT",
                    "CLOSED_POSITION",
                }:
                    self._closed_symbols_this_cycle.add(symbol)

                    try:
                        # userTrades can lag slightly; retry a few times
                        pnl_added = 0.0
                        for _ in range(6):
                            pnl_added = float(
                                record_realized_pnl_for_symbol(
                                    runner=self,
                                    symbol=symbol,
                                    window_minutes=30,
                                )
                                or 0.0
                            )
                            if abs(pnl_added) > 1e-12:
                                break
                            time.sleep(0.5)

                        # If kill just triggered, activate it immediately
                        if self.daily.kill:
                            self.activate_kill_switch()

                    except Exception as e:
                        # don't crash the runner
                        try:
                            self.audit.event(
                                event_type="REALIZED_PNL",
                                run_id=self.run_id,
                                symbol=symbol,
                                action="PNL_RECORD_FAILED",
                                details={"error": f"{type(e).__name__}: {e}"},
                            )
                        except Exception:
                            pass

                return {
                    "symbol": symbol,
                    "decision": decision,
                    "signal": sig,
                    "position": st.position,
                    "price": price,
                    "exit_reason": exit_reason,
                    "execution": {
                        "action": exec_result.action,
                        "details": exec_result.details,
                    },
                    "daily_realized_pnl": self.daily.realized_pnl,
                    "kill_switch": self.daily.kill,
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
                        decision = "NOOP_MAX_TRADES_PER_CYCLE"
                        # Make sure we don't proceed to execute if blocked by cycle limit
                        exec_signal = "HOLD"

                # ✅ RISK GATE CHECK
                if exec_signal in {"BUY", "SELL"}:
                    self.store.save_daily(self.daily.day, self.daily.realized_pnl, self.daily.kill, self.daily.trade_count or 0)
                    
                    risk_state = self.store.load_risk_state(self.daily.day)
                    risk_state.current_equity = self.get_account_balance()
                    risk_state.open_positions = current_open_count
                    
                    # Populate Strategy Health (Layer E)
                    try:
                        risk_state.health = self.health_monitor.get_rolling_health(symbol, limit=20)
                    except Exception:
                        pass # Don't block if health check fails, just skip Layer E
                    
                    # Calculate potential position params for budget check
                    # Rough estimate if sizer failed or wasn't called (e.g. fixed size)
                    budget_qty = 0.0
                    budget_entry_price = price
                    if trade_usdt > 0 and price > 0:
                        budget_qty = trade_usdt / price
                    
                    gate_decision = self.risk_gate.can_open(
                        state=risk_state, 
                        signal_symbol=symbol,
                        qty=budget_qty,
                        entry_price=budget_entry_price,
                        broker_id="BINANCE"
                    )
                    
                    if not gate_decision.allowed:
                        exec_signal = "HOLD"
                        decision = f"BLOCKED_{gate_decision.reason_code}"
                        self.audit.event(
                            event_type="RISK_BLOCK",
                            run_id=self.run_id,
                            symbol=symbol,
                            action="BLOCKED",
                            details={
                                "reason": gate_decision.reason,
                                "code": gate_decision.reason_code,
                                "severity": gate_decision.severity
                            },
                        )
                    
                    # ✅ Record Gate Decision
                    recorder.record_gate(
                        trace_id,
                        allowed=gate_decision.allowed,
                        reason_code=gate_decision.reason_code,
                        reason=gate_decision.reason,
                        details={"severity": gate_decision.severity},
                    )

                # Ensure we have fresh balance for drawdown check (redundant now but safe)
                current_equity = self.get_account_balance()
                
                # ✅ Record Intent (Entry)
                recorder.record_intent(
                    trace_id,
                    action=decision,
                    sizing={"trade_usdt": trade_usdt, "budget_qty": budget_qty},
                    sl_plan=None, # Inferred by strategy/executor
                    tp_plan=None,
                )

                # ✅ ALWAYS-ON INVARIANT CHECK (The Guarantee)
                # Ensure no action is taken if it violates critical invariants
                is_gate_blocked = False
                if 'gate_decision' in locals() and not gate_decision.allowed:
                    is_gate_blocked = True
                
                checker = get_invariant_checker()
                checker.check_all(
                    symbol=symbol,
                    trace_id=trace_id,
                    run_id=self.run_id,
                    action=exec_signal,
                    kill_switch_active=self.daily.kill,
                    exposure_freeze=exposure_freeze,
                    gate_blocked=is_gate_blocked,
                    has_intent=True,
                )

                exec_result = self.executor.execute_signal(
                    symbol,
                    exec_signal,
                    trade_usdt,
                    current_open_count=current_open_count,
                    current_equity=current_equity,
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
                        "trade_usdt": trade_usdt,
                    },
                    trace_id=trace_id,  # ✅ Link
                )

                # ✅ Record Execution (Entry)
                recorder.record_execution(
                    trace_id,
                    status=exec_result.action,
                    order_id=exec_result.order_id,
                    fill_price=float(exec_result.details.get("avg_price", 0.0) or 0.0),
                    fill_qty=float(exec_result.details.get("executed_qty", 0.0) or 0.0),
                    error=exec_result.error,
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

                    # ✅ Now the REAL piece: record trade_fills so PnL + win rate work (OPEN)
                    try:
                        _d = exec_result.details or {}
                        filled_qty = (
                            _d.get("filled_qty")
                            or _d.get("executed_qty")
                            or _d.get("qty")
                            or _d.get("quantity")
                            or st.entry_qty
                            or 0.0
                        )
                        avg_price = (
                            _d.get("avg_price")
                            or _d.get("avgPrice")
                            or _d.get("price")
                            or price
                        )
                        fee = _d.get("fee")

                        record_fill(
                            self.db,
                            symbol=symbol,
                            side="LONG" if exec_signal == "BUY" else "SHORT",
                            action="OPEN",
                            qty=float(filled_qty),
                            price=float(avg_price),
                            fee=float(fee) if fee is not None else None,
                            realized_pnl=None,
                            # ✅ ADD: attribution (future-proof)
                            strategy=getattr(self.strategy, "name", "unknown"),
                            strategy_version=getattr(self.strategy, "version", "0"),
                            broker_id=getattr(settings, "BROKER_ID", "binance_futures"),
                            account_id=getattr(settings, "ACCOUNT_ID", "default"),
                            asset_class=getattr(settings, "ASSET_CLASS", "CRYPTO"),
                            timeframe=str(getattr(self, "interval", "")),
                            # ✅ ADD: confidence at entry (calibration)
                            confidence=float(getattr(res, "confidence", 0.0) or 0.0),
                        )
                    except Exception:
                        pass

                if exec_result.action in {
                    "ORDER_PLACED",
                    "CLOSED_LONG",
                    "CLOSED_SHORT",
                }:
                    # Increment daily trade count on OPEN
                    if exec_result.action == "ORDER_PLACED":
                        try:
                            self.daily.increment_trade()
                        except Exception:
                            pass
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

            now_ms = int(time.time() * 1000)

            # Always initialize so later code can't crash
            decision = "HOLD"
            exec_signal = "HOLD"
            reason = "default"

            res = decide(
                PolicyInputs(
                    position=_norm_pos(st.position),
                    adds=st.adds,
                    pending_open=_norm_pending(st.pending_open),
                    reentry_confirm_signal=st.reentry_confirm_signal,
                    reentry_confirm_count=st.reentry_confirm_count,
                    last_trade_ms=st.last_trade_ms,
                    last_stop_ms=st.last_stop_ms,
                    signal=sig,
                    cooldown_seconds=self.settings.COOLDOWN_SECONDS,
                    sl_cooldown_seconds=self.settings.SL_COOLDOWN_SECONDS,
                    max_adds=self.settings.MAX_ADDS_PER_POSITION,
                    trade_mode=self.settings.TRADE_MODE,
                    reentry_confirmations=int(
                        getattr(self.settings, "REENTRY_CONFIRMATION_COUNT", 1) or 1
                    ),
                    now_ms=now_ms,
                    kill_switch=self.daily.kill,
                )
            )

            # Apply state updates from policy
            st.pending_open = (
                res.pending_open or "NONE"
            )  # ✅ ADJUSTMENT: never persist NULL
            st.reentry_confirm_signal = res.reentry_confirm_signal
            st.reentry_confirm_count = res.reentry_confirm_count

            # Map policy action -> runner variables used later
            reason = res.reason

            if res.action == Action.OPEN_LONG:
                decision = "OPEN"
                exec_signal = "BUY"
            elif res.action == Action.OPEN_SHORT:
                decision = "OPEN"
                exec_signal = "SELL"
            elif res.action == Action.ADD_LONG:
                decision = "ADD"
                exec_signal = f"ADD_LONG_{st.adds + 1}"
            elif res.action == Action.ADD_SHORT:
                decision = "ADD"
                exec_signal = f"ADD_SHORT_{st.adds + 1}"
            elif res.action == Action.CLOSE:
                decision = "CLOSE"
                exec_signal = "CLOSE"
            elif res.action == Action.FLIP_TO_LONG:
                decision = "FLIP"
                exec_signal = "BUY"
            elif res.action == Action.FLIP_TO_SHORT:
                decision = "FLIP"
                exec_signal = "SELL"
            else:
                decision = "HOLD"
                exec_signal = "HOLD"

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

                # ✅ Now the REAL piece: record trade_fills so PnL + win rate work (OPEN)
                try:
                    _d = exec_result.details or {}
                    filled_qty = (
                        _d.get("filled_qty")
                        or _d.get("executed_qty")
                        or _d.get("qty")
                        or _d.get("quantity")
                        or st.entry_qty
                        or 0.0
                    )
                    avg_price = (
                        _d.get("avg_price")
                        or _d.get("avgPrice")
                        or _d.get("price")
                        or price
                    )
                    fee = _d.get("fee")

                    if "LONG" in decision:
                        side = "LONG"
                    elif "SHORT" in decision:
                        side = "SHORT"
                    else:
                        side = "LONG" if exec_signal == "BUY" else "SHORT"

                    record_fill(
                        self.db,
                        symbol=symbol,
                        side=side,
                        action="OPEN",
                        qty=float(filled_qty),
                        price=float(avg_price),
                        fee=float(fee) if fee is not None else None,
                        realized_pnl=None,
                        # ✅ ADD: attribution (future-proof, multi-broker ready)
                        strategy=getattr(self.strategy, "name", "unknown"),
                        strategy_version=getattr(self.strategy, "version", "0"),
                        broker_id=getattr(settings, "BROKER_ID", "binance_futures"),
                        account_id=getattr(settings, "ACCOUNT_ID", "default"),
                        asset_class=getattr(settings, "ASSET_CLASS", "CRYPTO"),
                        timeframe=str(getattr(self, "interval", "") or self.interval),
                        # ✅ IMPORTANT: use stored strategy confidence (NOT policy res)
                        confidence=float(self.last_signal_confidence.get(symbol, 0.0)),
                    )
                except Exception:
                    pass

                # ✅ C) Once we re-enter successfully, clear stop tracking (OPEN / OPEN_PENDING only)
                if decision.startswith(("OPEN_", "OPEN_PENDING_")):
                    st.last_stop_ms = 0
                    st.reentry_confirm_signal = "NONE"
                    st.reentry_confirm_count = 0

            # When a close happens: record realized PnL from broker fills (works even if re-entry happens fast)
            if exec_result.action in {"CLOSED_LONG", "CLOSED_SHORT", "CLOSED_POSITION"}:
                self._closed_symbols_this_cycle.add(symbol)
                pnl = 0.0

                # 1) Prefer broker fill-based realized pnl (dedup-safe)
                try:
                    # Small retry loop because userTrades can lag slightly after a close
                    for _ in range(6):
                        pnl = float(
                            record_realized_pnl_for_symbol(
                                runner=self,
                                symbol=symbol,
                                window_minutes=30,
                            )
                            or 0.0
                        )
                        if abs(pnl) > 1e-12:
                            # ✅ UPDATE STRATEGY PERFORMANCE + CONFIDENCE METRICS (EXACT PLACE)
                            try:
                                on_trade_close_update_metrics(
                                    strategy=getattr(self.strategy, "name", "unknown"),
                                    strategy_version=getattr(
                                        self.strategy, "version", "0"
                                    ),
                                    symbol=symbol,
                                    timeframe=self.interval,
                                    confidence=self.last_signal_confidence.get(symbol),
                                    realized_pnl=float(pnl),
                                    fees=0.0,  # already netted in record_realized_pnl_for_symbol
                                )
                            except Exception as e:
                                self.audit.event(
                                    event_type="METRICS_ERROR",
                                    run_id=self.run_id,
                                    symbol=symbol,
                                    action="METRICS_UPDATE_FAILED",
                                    details={"error": repr(e)},
                                )

                            break
                        time.sleep(0.5)
                except Exception as e:
                    self.audit.event(
                        event_type="REALIZED_PNL",
                        run_id=self.run_id,
                        symbol=symbol,
                        action="PNL_RECORD_FAILED",
                        details={"error": f"{type(e).__name__}: {e}"},
                    )

                # 2) If pnl still 0, keep your old fallback estimate logic (optional safety)
                # IMPORTANT: fallback should NOT add to daily again; only use it for logging.
                if abs(pnl) < 1e-12:
                    try:
                        exit_px = price
                        qty = float(st.entry_qty or 0.0)
                        entry_px = float(st.entry_price or 0.0)

                        if qty > 0 and entry_px > 0:
                            if exec_result.action == "CLOSED_LONG":
                                est = (exit_px - entry_px) * qty
                            elif exec_result.action == "CLOSED_SHORT":
                                est = (entry_px - exit_px) * qty
                            else:
                                # Best guess: use decision hint
                                if "SHORT" in decision:
                                    est = (entry_px - exit_px) * qty
                                else:
                                    est = (exit_px - entry_px) * qty

                            self.audit.event(
                                event_type="REALIZED_PNL",
                                run_id=self.run_id,
                                symbol=symbol,
                                action="PNL_FALLBACK_ESTIMATE_ONLY",
                                details={
                                    "estimate": float(est),
                                    "note": "fill-based pnl was 0 (likely userTrades lag); estimate not added to daily.",
                                },
                            )
                    except Exception:
                        pass

                # ✅ keep: record close fill (for win-rate/reports)
                try:
                    _d = exec_result.details or {}
                    filled_qty = (
                        _d.get("filled_qty")
                        or _d.get("executed_qty")
                        or _d.get("qty")
                        or _d.get("quantity")
                        or st.entry_qty
                        or 0.0
                    )
                    avg_price = (
                        _d.get("avg_price")
                        or _d.get("avgPrice")
                        or _d.get("price")
                        or price
                    )
                    fee = _d.get("fee")

                    if exec_result.action == "CLOSED_LONG":
                        side = "LONG"
                    elif exec_result.action == "CLOSED_SHORT":
                        side = "SHORT"
                    else:
                        # best guess
                        side = "LONG" if "LONG" in decision else "SHORT"

                    record_fill(
                        self.db,
                        symbol=symbol,
                        side=side,
                        action="CLOSE",
                        qty=float(filled_qty or 0.0),
                        price=float(avg_price or 0.0),
                        fee=float(fee or 0.0) if fee is not None else None,
                        pnl=float(pnl or 0.0),
                        notes={
                            "exec_action": exec_result.action,
                            "decision": decision,
                            "signal": sig,
                        },
                        # ✅ Attribution
                        strategy=getattr(self.strategy, "name", "unknown"),
                        strategy_version=getattr(self.strategy, "version", "0"),
                        broker_id=getattr(settings, "BROKER_ID", "binance_futures"),
                        account_id=getattr(settings, "ACCOUNT_ID", "default"),
                        asset_class=getattr(settings, "ASSET_CLASS", "CRYPTO"),
                        timeframe=str(getattr(self, "interval", "") or self.interval),
                        # best available at close time
                        confidence=float(
                            self.last_signal_confidence.get(symbol, 0.0) or 0.0
                        ),
                    )
                except Exception:
                    pass

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
        # ✅ FAST EXIT: If shutdown requested, don't even start the cycle setup
        if getattr(self, "_stop_requested", False):
            return {
                 "status": "stopped",
                 "symbols_checked": 0,
                 "duration_ms": 0,
            }

        if not self.symbols:
            return {}

        # 1. Acquire cycle context via Context Manager
        with self.cycle_guard(timeout_s=5.0) as acquired:
            if not acquired:
                return {"skipped": True, "reason": "CYCLE_ALREADY_RUNNING"}

            # 2. Acquire cycle context
            cycle_id = str(uuid.uuid4())
            set_cycle_id(cycle_id)

            try:
                # ✅ keep your existing run_once logic below
                self.live_trades_this_cycle = 0
                self._closed_symbols_this_cycle.clear()

                # Daily reset (new day)
                self.daily.reset_if_new_day()

                # Persist daily state (even if unchanged)
                self.store.save_daily(
                    self.daily.day, self.daily.realized_pnl, self.daily.kill
                )

                # Update drawdown snapshots (peaks)
                bal = self.get_account_balance()
                self.drawdown_monitor.update_snapshots(self.daily.day, bal)

                # --- CYCLE AUDIT START ---
                self.run_id = self.run_id or str(uuid.uuid4())
                set_run_id(self.run_id)
                # cycle_id already set above

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
                syms = symbols

                results = []

                for s in syms:
                    # ✅ Break early if shutdown requested
                    if getattr(self, "_stop_requested", False):
                        print("[RUNNER] Stop requested, breaking loop.")
                        break

                    try:
                        results.append(self.step_symbol(s))
                    except Exception as e:
                        # ✅ Record circuit breaker error (Universal)
                        self.circuit_registry.record_error("BINANCE")

                        # never kill whole runner for one symbol
                        self.audit.event(
                            event_type="ERROR",
                            symbol=s,
                            action="STEP_SYMBOL_FAILED",
                            details={"error": repr(e)},
                        )
                        results.append({"symbol": s, "ok": False, "error": repr(e)})
                        continue

                        # --- POST-CYCLE REALIZED PNL SYNC ---
                # Binance userTrades can lag inside step_symbol (especially with fast churn / flips).
                # Sync again after the cycle so daily pnl + kill-switch see the closes.
                for sym in list(self._closed_symbols_this_cycle):
                    try:
                        pnl_added = float(
                            record_realized_pnl_for_symbol(
                                runner=self,
                                symbol=sym,
                                window_minutes=30,
                            )
                            or 0.0
                        )

                        if abs(pnl_added) > 1e-12:
                            self.audit.event(
                                event_type="REALIZED_PNL",
                                run_id=self.run_id,
                                symbol=sym,
                                action="PNL_RECORDED_POST_CYCLE",
                                details={"pnl_added": pnl_added},
                            )

                        # Persist daily state after syncing
                        self.store.save_daily(
                            self.daily.day, self.daily.realized_pnl, self.daily.kill
                        )

                        # Activate kill-switch immediately if needed
                        if self.daily.kill:
                            self.activate_kill_switch()

                    except Exception as e:
                        self.audit.event(
                            event_type="REALIZED_PNL",
                            run_id=self.run_id,
                            symbol=sym,
                            action="PNL_POST_CYCLE_FAILED",
                            details={"error": f"{type(e).__name__}: {e}"},
                        )

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
                clear_cycle_id()
                clear_run_id()
                # Lock is released automatically by context manager

    def record_realized_pnl_from_usertrades(
        self, symbol: str, window_minutes: int = 30
    ) -> float:
        """
        Compatibility wrapper. Uses the centralized recorder.
        Safe to call after CLOSE/flip-close (even if re-entry is fast).
        """
        return float(
            record_realized_pnl_for_symbol(
                runner=self,
                symbol=symbol,
                window_minutes=window_minutes,
            )
            or 0.0
        )

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
