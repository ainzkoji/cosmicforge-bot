from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional
import concurrent.futures
import traceback

from app.core.config import settings
from app.exchange.binance.client import BinanceFuturesClient, kline_closes
from app.adaptive import get_adaptive_engine
from app.execution.executor import BinanceExecutor, ExecResult, ExchangeError, FatalIntegrationError
from app.execution.entry_protection import get_entry_protection  # FAIL-SAFE ENTRY LOCK
from shared_lib.persistence.audit import Audit
from shared_lib.persistence.db import DB
from shared_lib.persistence.state_store import StateStore
from app.risk.daily_loss import DailyLossState
from app.risk.realized_pnl import realized_pnl_from_user_trades
from app.runner.models import SymbolState
from app.data.multi_timeframe_fetcher import MultiTimeframeFetcher, MultiTimeframeFetchError
from app.strategy.iofs_gate import (
    IOFSGateEvaluator,
    gate_result_details,
    is_session_allowed,
    is_symbol_allowed,
    make_gate_failure,
)
from app.strategy.loader import build_strategy
from app.strategy.hold_breakdown import build_hold_breakdown
from app.strategy.sma_cross import signal_from_closes
from app.symbols.sizing import parse_usdt_map, usdt_for
from app.symbols.leverage import leverage_for, parse_leverage_map
from app.symbols.universe import parse_symbols
from app.symbols.dynamic_universe import (
    DynamicUniverseService,
    DynamicUniverseShadowRecorder,
)
from app.symbols.symbol_selector import DynamicSymbolSelector
from app.symbols.symbol_promotion import SymbolPromotionEvaluator
from app.events.event_news_influence_engine import EventNewsInfluenceEngine
from app.events.event_news_mode_controller import EventNewsModeController
from app.execution.confirm import wait_until_flat
from app.execution.position_manager import PositionManager, PositionManagerConfig, PositionPhase, PositionSide, PositionState
from shared_lib.persistence.trade_fills import record_fill, ExitReason
from app.risk.realized_pnl import record_realized_pnl_for_symbol
from app.metrics.hooks import on_trade_close_update_metrics
# D-1: Per-bot consecutive-loss guard
from app.risk.guard import on_trade_closed as _guard_on_trade_closed, should_pause as _guard_should_pause, reset_bot as _guard_reset_bot


# âœ… ADD: Unified Policy Engine
from app.policy.policy_engine import (
    get_policy_engine,
    reset_policy_engine,
    PolicyContext,
    Action as PolicyAction,
    TradeAmountMode,
    calculate_atr
)
from app.risk.drawdown import DrawdownMonitor
from app.risk.circuit import get_circuit_registry
from app.risk.risk_budget import get_risk_budget_engine
from app.risk.invariant_checker import get_invariant_checker
from app.metrics.health import StrategyHealthMonitor

# âœ… ADD: Monitoring infrastructure
from shared_lib.persistence.trace_recorder import get_trace_recorder, StrategySignal

# ML inference (Step 5D-2) â€” additive scoring layer; disabled by default via settings
from app.ml.scorer import get_ml_scorer, ACTION_SKIP

# âœ… ADD: cycle context helpers
from app.ops.context import set_cycle_id, clear_cycle_id, get_cycle_id, set_run_id, get_run_id, clear_run_id

# Event Awareness (Phase 1)
from app.events.event_calendar_service import EventCalendarService
from app.events.event_blackout_filter import build_event_blackout_filter
# Market Reaction Layer (Phase 2) â€” risk gate, shadow mode by default
from app.risk.event_reaction_risk_gate import build_event_reaction_risk_gate

# âœ… ADD: Trading Orchestrator & Governance
# âœ… ADD: Trading Orchestrator & Governance
from app.core.trading_orchestrator import TradingOrchestrator
from app.runner.errors import RunnerInitializationError
from app.core.bot_health import resolve_last_error
from app.risk.system_limits import UserConfigurableLimits, RiskLevel

# Logger instance
logger = logging.getLogger(__name__)

PAPER_OPEN_STATUSES = {
    "PAPER_ORDER_CREATED",
    "PAPER_FILLED",
    "PAPER_POSITION_OPENED",
}
ORDER_OPEN_STATUSES = {"ORDER_PLACED", *PAPER_OPEN_STATUSES}
ORDER_CLOSE_STATUSES = {"CLOSED_LONG", "CLOSED_SHORT", "CLOSED_POSITION", "PAPER_POSITION_CLOSED"}


# â”€â”€ Standardized Reason Mapping (Section 7) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Normalizes internal orchestrator/gate reasons to compact, grep-friendly labels.
_REASON_MAP = {
    "master_ensemble_v2":             "no_signal",
    "strategy says hold":             "no_signal",
    "regime_low_vol_chop_suspended":  "low_vol_chop",
    "volatility_spike_detected":      "volatility_spike",
    "protective_orders_validated":     "passed",
    "already_open":                   "already_open",
    "stop_loss_cooldown":             "cooldown",
    "circuit_breaker":                "circuit_breaker",
    "ml_floor_block":                 "ml_block",
    "ml_threshold_block":             "ml_block",
    "policy":                         "policy_block",
    "layer a":                        "policy_block",
    "layer b":                        "policy_block",
    "layer c":                        "policy_block",
    "correlation":                    "exec_filter_block",
    "max_positions":                  "max_positions",
    "exposure_limit":                 "max_positions",
}

def _normalize_reason(raw_reason: str) -> str:
    """Map internal reason strings to standardized log labels."""
    if not raw_reason:
        return "unknown"
    low = raw_reason.lower().strip().replace(" ", "_").replace("-", "_")
    for key, label in _REASON_MAP.items():
        if key in low:
            return label
    # Fallback: if confidence/threshold mentioned, it's below_threshold
    if "confidence" in low or "threshold" in low or "below" in low:
        return "below_threshold"
    return raw_reason[:40]  # Truncate long reasons


class _CycleStats:
    """Lightweight per-cycle aggregator for the [CYCLE_SUMMARY] log."""
    __slots__ = (
        "evaluated", "hold", "passed", "blocked", "execute_attempts",
        "orders_placed", "errors", "ml_blocks", "policy_blocks",
        "exec_filter_blocks", "hold_reasons", "best_hold_symbol",
        "best_hold_conf",
    )
    def __init__(self):
        self.evaluated = 0
        self.hold = 0
        self.passed = 0
        self.blocked = 0
        self.execute_attempts = 0
        self.orders_placed = 0
        self.errors = 0
        self.ml_blocks = 0
        self.policy_blocks = 0
        self.exec_filter_blocks = 0
        self.hold_reasons: dict[str, int] = {}
        self.best_hold_symbol: str | None = None
        self.best_hold_conf: float = 0.0

    def record_hold(self, symbol: str, confidence: float, reason: str):
        self.hold += 1
        nr = _normalize_reason(reason)
        self.hold_reasons[nr] = self.hold_reasons.get(nr, 0) + 1
        if confidence > self.best_hold_conf:
            self.best_hold_conf = confidence
            self.best_hold_symbol = symbol

    def record_pass(self):
        self.passed += 1

    def record_block(self, reason: str):
        self.blocked += 1
        nr = _normalize_reason(reason)
        if nr == "ml_block":
            self.ml_blocks += 1
        elif nr in ("policy_block", "below_threshold"):
            self.policy_blocks += 1
        elif nr == "exec_filter_block":
            self.exec_filter_blocks += 1

    def summary_line(self) -> str:
        """Build the [CYCLE_SUMMARY] log string."""
        parts = [
            f"evaluated={self.evaluated}",
            f"hold={self.hold}",
            f"pass={self.passed}",
            f"blocked={self.blocked}",
            f"exec_attempts={self.execute_attempts}",
            f"orders_placed={self.orders_placed}",
        ]
        # Optional detail counters
        detail_parts = []
        if self.ml_blocks:
            detail_parts.append(f"ml_block={self.ml_blocks}")
        if self.policy_blocks:
            detail_parts.append(f"policy_block={self.policy_blocks}")
        if self.exec_filter_blocks:
            detail_parts.append(f"exec_filter={self.exec_filter_blocks}")
        if self.errors:
            detail_parts.append(f"errors={self.errors}")
        if detail_parts:
            parts.append("| " + " ".join(detail_parts))
        # Top HOLD reasons (up to 3)
        if self.hold_reasons:
            sorted_reasons = sorted(self.hold_reasons.items(), key=lambda x: -x[1])[:3]
            top_str = " ".join(f"{r}={c}" for r, c in sorted_reasons)
            parts.append(f"| top_hold: {top_str}")
        # Best HOLD confidence
        if self.best_hold_symbol and self.best_hold_conf > 0:
            parts.append(f"| best_hold: {self.best_hold_symbol}@{self.best_hold_conf:.3f}")
        return " | ".join(parts[:6]) + " " + " ".join(parts[6:])

    def as_dict(self) -> Dict[str, Any]:
        reasons = dict(sorted(self.hold_reasons.items()))
        count = lambda token: sum(v for k, v in reasons.items() if token in k.lower())
        return {
            "evaluated_symbols": self.evaluated + self.hold,
            "new_candle_evaluations": self.evaluated,
            "skipped_same_candle": count("new_candle"),
            "no_opportunity": count("opportunity"),
            "regime_blocked": count("regime"),
            "session_blocked": count("session"),
            "volatility_blocked": count("volatility"),
            "consensus_blocked": count("consensus"),
            "confidence_blocked": count("threshold") + count("confidence"),
            "risk_blocked": self.policy_blocks,
            "correlation_blocked": count("correlation"),
            "execution_blocked": self.exec_filter_blocks,
            "approved": self.passed,
            "execution_attempts": self.execute_attempts,
            "fills": self.orders_placed,
            "partial_fills": count("partial"),
            "closes": 0,
            "execution_errors": self.errors,
            "reason_counts": reasons,
        }


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


# âœ… ADD: Bot Run Context
from app.runner.bot_context import BotRunContext

class PaperRunner:

    def __init__(
        self,
        client: BinanceFuturesClient,
        context: BotRunContext | None = None,
        effective_policy: object | None = None,
    ):
        # The resolved policy must be available BEFORE any collaborator is built
        # (the orchestrator reads it during construction), so accept it here and
        # attach it to the context rather than patching the runner afterwards.
        if effective_policy is not None:
            if context is None:
                raise RunnerInitializationError(
                    RunnerInitializationError.RUNTIME_CONTEXT_INVALID,
                    "effective_policy supplied without a BotRunContext",
                )
            context.effective_policy = effective_policy
            context.effective_policy_hash = getattr(effective_policy, "policy_hash", "") or ""
        if context is not None and getattr(context, "effective_policy", None) is None:
            raise RunnerInitializationError(
                RunnerInitializationError.EFFECTIVE_POLICY_INVALID,
                "Auto Pilot runner requires a resolved EffectiveBotPolicy",
                bot_instance_id=getattr(context, "bot_instance_id", None),
            )
        self.client = client
        self.settings = settings
        self.context: BotRunContext | None = context  # âœ… Store context
        
        # âœ… Store last signal confidence per symbol (used on CLOSE)
        self.last_signal_confidence: dict[str, float] = {}
        # âœ… Runtime counters
        self._last_protection_checks: dict[str, float] = {}
        
        # âœ… Determine effective configuration (Global vs Context)
        if self.context:
            self.run_id = self.context.run_id
            effective_symbols = self.context.symbols
            effective_interval = self.context.interval
            effective_strategy = self.context.strategy_id
            effective_params = self.context.strategy_params
            effective_mode = self.context.execution_mode
            
            # Risk & Size settings from context
            self.daily_max_loss = self.context.daily_max_loss_usdt
            self.max_trades_daily = self.context.max_trades_daily
            self.max_open_positions = self.context.max_open_positions
            self.trade_usdt = self.context.trade_usdt_per_order
            
        else:
            self.run_id = None
            effective_symbols = settings.TRADE_SYMBOLS
            effective_interval = settings.DEFAULT_INTERVAL
            effective_strategy = settings.STRATEGY_NAME
            effective_params = settings.STRATEGY_PARAMS_JSON
            effective_mode = settings.EXECUTION_MODE
            
            # Default Risk & Size
            self.daily_max_loss = settings.DAILY_MAX_LOSS_USDT
            self.max_trades_daily = getattr(settings, "MAX_TRADES_DAILY", 20)
            self.max_open_positions = getattr(settings, "MAX_OPEN_POSITIONS", 3)
            self.trade_usdt = settings.TRADE_USDT_PER_ORDER

        # B-7 Fix: Hard-block sma_cross for user-context bots.
        # sma_cross is a simple MA crossover with no regime/volatility/spread filters.
        # It must never run for a real user bot â€” force master_ensemble instead.
        _UNSAFE_STRATEGY_ALIASES = {"sma_cross", "sma-cross", "sma cross"}
        if self.context and effective_strategy.strip().lower() in _UNSAFE_STRATEGY_ALIASES:
            logger.warning(
                "STRATEGY_REJECTED_SMA_CROSS_UNSAFE_FOR_USER_BOT â€” "
                "user bot %s requested '%s'; enforcing master_ensemble. "
                "STRATEGY_FALLBACK_MASTER_ENSEMBLE",
                self.context.bot_instance_id,
                effective_strategy,
            )
            effective_strategy = "master_ensemble"

        # ---- Basic config / strategy ----

        # B-2 Fix: If effective_symbols is already a str, do NOT join â€” joining a str
        # character-by-character produces single-letter symbols like T, E, D, S, ,.
        if isinstance(effective_symbols, str):
            _symbols_str = effective_symbols
        else:
            _symbols_str = ",".join(effective_symbols)
        self.symbols = parse_symbols(_symbols_str, settings.MAX_SYMBOLS)
        # Validate no single-character symbols leaked through
        _bad = [s for s in self.symbols if len(s) <= 2]
        if _bad:
            logger.error("INVALID_SYMBOL_AFTER_PARSE â€” suspicious short symbols: %s", _bad)
        else:
            logger.debug("SYMBOL_PARSE_SUCCESS â€” parsed %d symbols: %s", len(self.symbols), self.symbols)
        self.interval = effective_interval

        self._strategy_name = effective_strategy
        self._strategy_params = effective_params
        self.strategy = build_strategy(
            name=effective_strategy,
            client=self.client,
            interval=self.interval,
            params_json=effective_params if isinstance(effective_params, str) else json.dumps(effective_params) if effective_params else None,
        )
        self._dynamic_shadow_strategy = None
        self._dynamic_shadow_recorder = None
        if bool(getattr(settings, "DYNAMIC_UNIVERSE_SHADOW_ENABLED", False)):
            try:
                self._dynamic_shadow_strategy = build_strategy(
                    name=effective_strategy,
                    client=self.client,
                    interval=self.interval,
                    params_json=effective_params if isinstance(effective_params, str) else json.dumps(effective_params) if effective_params else None,
                )
                self._dynamic_shadow_recorder = DynamicUniverseShadowRecorder(self.db if hasattr(self, "db") else None)
            except Exception as _dyn_shadow_init_err:
                logger.warning("[DYNAMIC_UNIVERSE_SHADOW] init failed: %s", _dyn_shadow_init_err)
                self._dynamic_shadow_strategy = None
                self._dynamic_shadow_recorder = None

        # --- Execution locks (robust anti-overlap) ---
        self._cycle_lock = threading.Lock()
        self._symbol_locks = defaultdict(threading.Lock)  # symbol -> Lock

        # ---- Persistence + audit MUST exist before calling self.store.* ----
        self.db = DB()
        self.audit = Audit(self.db)
        self.cycle_id: str | None = None
        
        # âœ… SCOPED STATE STORE
        bot_id = self.context.bot_instance_id if self.context else "default"
        self.store = StateStore(self.db, bot_instance_id=bot_id)
        
        # âœ… SYNC ROBUSTNESS: Flush confidence buffer (require N zero-reads before declaring NONE)
        self._position_flush_counts: dict[str, int] = defaultdict(int)
        
        # âœ… EXECUTOR

        # Pass per-bot execution_mode so the executor knows whether to trade live or paper
        # This fixes the root cause: executor was always reading global settings.EXECUTION_MODE = 'paper'
        _policy_mode = self.context.execution_mode if self.context else settings.EXECUTION_MODE
        # Executor retains the historical "live" spelling internally.  The
        # canonical policy dimension is paper|broker.
        _exec_mode = "live" if str(_policy_mode).lower() in {"broker", "live"} else "paper"
        _exec_symbols = list(self.context.symbols) if self.context else None
        self.executor = BinanceExecutor(
            client=self.client,
            risk_gate=None,
            audit=self.audit,
            execution_mode=_exec_mode,
            live_symbols=_exec_symbols,
            bot_instance_id=bot_id,
            market_data_interval=self.interval,
            db=self.db,  # â† CRITICAL: wires _entry_prot; without this the entire
                         #   fail-safe system is silently disabled (always None).
        )
        self.effective_policy = getattr(self.context, "effective_policy", None) if self.context else None
        self.effective_policy_hash = getattr(self.context, "effective_policy_hash", "") if self.context else ""
        # â”€â”€ Exposure ceiling for _entry_prot guard â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # Teaches the executor the maximum notional for this bot instance so the
        # hard-max exposure guard actually has a ceiling to enforce.
        _max_notional_per_symbol = 0.0
        _allocation_type = "fixed_usdt"
        _allocation_value = 0.0
        if self.context:
            try:
                _allocation_type = str(getattr(self.context, "allocation_type", "fixed_usdt") or "fixed_usdt")
                _allocation_value = float(getattr(self.context, "allocation_value", 0.0) or 0.0)
                if _allocation_type == 'fixed_amount':
                    # allocation_value is the MARGIN budget (e.g. 120 USDT).
                    # The executor interprets trade_usdt as NOTIONAL, so:
                    #   sized_notional = allocation_value Ã— leverage
                    # The sizing function allows Â±15% step-rounding tolerance, so
                    # the real max notional for a single trade is:
                    #   allocation_value Ã— max_leverage Ã— 1.15
                    # We add a 5% safety buffer on top (Ã—1.20 total) so this guard
                    # never false-fires on a correctly-sized single trade, while
                    # still blocking any true duplicate entry on the same symbol.
                    _ctx_max_lev = float(getattr(self.context, "max_leverage", 0) or 0)
                    _cfg_lev = float(getattr(settings, "DEFAULT_LEVERAGE", 5) or 5)
                    _eff_max_lev = _ctx_max_lev if _ctx_max_lev > 0 else _cfg_lev
                    _max_notional_per_symbol = _allocation_value * _eff_max_lev * 1.20
                else:
                    _max_notional_per_symbol = float(getattr(self.context, 'trade_usdt_per_order', 0.0) or 0.0)
            except Exception:
                _max_notional_per_symbol = 0.0
                _allocation_type = "fixed_usdt"
                _allocation_value = 0.0
        self.executor._allocation_type = _allocation_type
        self.executor._allocation_value = _allocation_value
        self.executor._max_notional_per_symbol = _max_notional_per_symbol
        self.executor._allow_scale_in = False
        self.executor._allow_hedge_mode = False
        # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # âœ… Session Monitor (Daily Close)
        from app.runner.session_monitor import SessionMonitor
        self.session_monitor = SessionMonitor(
            self.executor,
            self.audit,
            bot_instance_id=self.context.bot_instance_id if self.context else None,
        )

        # Section F â€” product safety: bot health status (user-facing).
        self._last_bot_health_status: str | None = None

        # âœ… CIRCUIT BREAKER
        # Key: "{bot_instance_id}:{broker_account_id}" â€” per-bot per-broker scope.
        # Bot A's trip never blocks Bot B even on the same exchange account.
        self.circuit_registry = get_circuit_registry()
        _broker_acct = self.context.broker_account_id if self.context else "default"
        self._circuit_id = f"{bot_id}:{_broker_acct}"
        self.circuit = self.circuit_registry.get_breaker(broker_id=self._circuit_id)

        # ---- Universes (trade vs live) ----
        # If context is used, trade_symbols are the context symbols
        if self.context:
            self.trade_symbols = list(self.context.symbols)
            # Live symbols treated same as trade symbols for context-based run
            self.live_symbols = list(self.context.symbols) 
        else:
            self.trade_symbols = parse_symbols(settings.TRADE_SYMBOLS, settings.MAX_SYMBOLS)
            self.live_symbols = parse_symbols(settings.LIVE_SYMBOLS, settings.MAX_SYMBOLS)

        # ---- Validate symbols against exchange (drops unlisted/delisted symbols) ----
        # This prevents Binance -1121 "Invalid symbol" errors (e.g. MATICUSDT â†’ POL on demo-fapi)
        try:
            exch_info = self.client.exchange_info()
            valid_symbols = {
                s["symbol"] for s in exch_info.get("symbols", [])
                if s.get("status") == "TRADING"
            }
            def _filter(syms):
                filtered, dropped = [], []
                for s in syms:
                    if s.upper() in valid_symbols:
                        filtered.append(s)
                    else:
                        dropped.append(s)
                if dropped:
                    logger.warning(
                        f"[SYMBOL FILTER] Dropped {len(dropped)} symbol(s) not available "
                        f"on this exchange endpoint: {dropped}"
                    )
                return filtered
            self.trade_symbols = _filter(self.trade_symbols)
            self.live_symbols  = _filter(self.live_symbols)
            self.symbols       = _filter(self.symbols)
        except Exception as e:
            logger.warning(f"[SYMBOL FILTER] Could not validate symbols against exchange: {e}")
            # Proceed with unfiltered list â€” errors will surface per-symbol at runtime

        # âœ… Universe used for state + reconciliation (union of trade + live symbols)
        seen = set()
        self.universe_symbols = []
        for s in list(self.trade_symbols) + list(self.live_symbols):
            ss = (s or "").upper()
            if ss and ss not in seen:
                seen.add(ss)
                self.universe_symbols.append(ss)

        # âœ… POSITION MANAGER (Layer C Exit Management)
        # Fix C-3: pass store + bot_instance_id so _persist_lifecycle() writes to
        # position_lifecycle_state on every phase mutation instead of silently no-oping.
        # self.store and bot_id are both defined above (lines ~162-163).
        # FIX-D: wire BREAK_EVEN_BUFFER_FRACTION from settings so the BE buffer
        # fraction is configurable via .env without changing source code.
        _pm_config = PositionManagerConfig(
            be_sl_distance_buffer_pct=float(settings.BREAK_EVEN_BUFFER_FRACTION),
        )
        self.position_manager = PositionManager(store=self.store, bot_instance_id=bot_id, config=_pm_config)

        # âœ… Create state from the union universe (trade + live)
        self.state: Dict[str, SymbolState] = {
            s: SymbolState() for s in self.universe_symbols
        }

        # âœ… KEEP YOUR BLOCK: restore symbol state early (NOW store exists)
        # Note: self.store.load_symbols() is now scoped by bot_instance_id
        saved = self.store.load_symbols()
        for sym, row in saved.items():
            if sym not in self.state:
                # If we have state for a symbol not in current config, ignore or load?
                # For safety, if it's in DB for this bot, we should probably track it to close it if needed.
                # But for now, stick to configured universe.
                continue

            st = self.state[sym]
            # (Typed SymbolState copy)
            st.position = row.position
            st.entry_price = row.entry_price
            st.last_signal = row.last_signal
            st.last_action = row.last_action
            st.last_checked_ms = row.last_checked_ms
            st.adds = row.adds
            st.last_trade_ms = row.last_trade_ms
            st.pending_open = row.pending_open
            st.entry_qty = row.entry_qty
            st.last_user_trade_id = row.last_user_trade_id
            st.reentry_confirm_signal = row.reentry_confirm_signal
            st.reentry_confirm_count = row.reentry_confirm_count
            st.position_id = row.position_id  # restore linkage key across restarts
            if self._effective_execution_mode() == "paper" and st.position in ("LONG", "SHORT"):
                self.executor.paper_executor.seed_position(
                    sym,
                    st.position,
                    st.entry_qty,
                    st.entry_price or 0.0,
                    position_id=st.position_id,
                )

        # Per-symbol USDT sizing map
        # TODO: Support context-based specific sizing updates if needed
        self.usdt_map = parse_usdt_map(settings.SYMBOL_USDT_MAP)

        # Track how many live trades were placed in the current run_once() cycle
        self.live_trades_this_cycle = 0
        self._dynamic_shadow_last_run_ts = 0.0
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

        # Risk Budget Engine â€” per-bot so Bot A's positions never exhaust Bot B's budget
        self.budget_engine = get_risk_budget_engine(bot_id=bot_id)

        # Policy Engine â€” per-bot so each bot uses its own budget engine.
        # Reset cache first so the engine is always created with the current config's
        # min_confidence (not the stale 0.10 default from a previous instantiation).
        reset_policy_engine(bot_id)
        self.policy_engine = get_policy_engine(
            bot_id=bot_id,
            budget_engine=self.budget_engine,
            circuit_registry=self.circuit_registry,
            min_confidence=settings.MIN_CONFIDENCE_THRESHOLD,
        )

        # Adaptive Engine â€” per-bot so loss streak / drawdown / rolling stats
        # are always scoped to this bot's own trade history.
        self.adaptive_engine = get_adaptive_engine(bot_id=bot_id, db=self.db)

        # ML Entry Quality Scorer (Step 5D-2) â€” additive gate, loaded once at startup.
        # Disabled by default (ML_ENABLED=False in settings).  All code paths degrade
        # gracefully if the model is unavailable or settings.ML_ENABLED is False.
        self.ml_scorer = get_ml_scorer()
        if self.ml_scorer.enabled:
            logger.info(
                "[MLScorer] Loaded: version=%s shadow=%s threshold=%.2f",
                self.ml_scorer.model_version,
                self.ml_scorer.shadow_mode,
                self.ml_scorer.threshold,
            )
        else:
            logger.debug("[MLScorer] Disabled (ML_ENABLED=False)")

        # â”€â”€ Event Awareness (Phase 1) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # Disabled by default (EVENT_FILTER_ENABLED=False in settings).  When disabled,
        # is_blocked() returns False immediately with no DB access.
        self._event_calendar_svc = EventCalendarService(self.db)
        self.event_blackout_filter = build_event_blackout_filter(self._event_calendar_svc)
        logger.debug(
            "[EventFilter] Initialized (enabled=%s failsafe=%s)",
            settings.EVENT_FILTER_ENABLED,
            settings.EVENT_FILTER_FAILSAFE_ENABLED,
        )

        # â”€â”€ Market Reaction Risk Gate (Phase 2) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # REACTION_ALLOW_RISK_INFLUENCE=False by default â€” returns (False) immediately.
        # Only activates after shadow-mode data quality is validated and flag is set.
        self.reaction_risk_gate = build_event_reaction_risk_gate(self.db)

        # IOFS Gate 0 performs no exchange calls while disabled. Enforce mode is
        # downgraded to shadow for live execution.
        self.iofs_fetcher = MultiTimeframeFetcher(self.client)
        self.iofs_evaluator = IOFSGateEvaluator()
        self.last_iofs_result: dict[str, dict[str, Any]] = {}

        # NOTE: PositionSizer is now handled by PolicyEngine
        # NOTE: Executor and PositionManager already initialized above (lines ~161-172 and ~231)
        # Do NOT re-initialize here as it would strip execution_mode and live_symbols.

        self.cached_balance = 0.0
        self.last_balance_time = 0.0

        # âœ… KEEP your second restore too (even though it's duplicate, per your request)
        saved_daily = self.store.load_daily(self.daily.day)
        if saved_daily:
            self.daily.realized_pnl = float(saved_daily.get("realized_pnl", 0.0))
            self.daily.kill = bool(saved_daily.get("kill", False))
            # F-9: restore consecutive loss counter and cooldown from DB
            self.daily.consecutive_losses = int(saved_daily.get("consecutive_losses", 0))
            self.daily.consec_loss_cooldown_until_ms = int(saved_daily.get("consec_loss_cooldown_until_ms", 0))

        # Restore symbol states (typed SymbolState objects)
        saved_symbols = self.store.load_symbols()
        for sym, row in saved_symbols.items():
            if sym not in self.state:
                continue

            st = self.state[sym]
            st.position = row.position
            st.entry_price = row.entry_price
            st.last_signal = row.last_signal
            st.last_action = row.last_action
            st.last_checked_ms = row.last_checked_ms
            st.adds = row.adds
            st.last_trade_ms = row.last_trade_ms
            st.pending_open = row.pending_open
            st.entry_qty = row.entry_qty
            st.last_user_trade_id = row.last_user_trade_id
            st.reentry_confirm_signal = row.reentry_confirm_signal
            st.reentry_confirm_count = row.reentry_confirm_count

        # â”€â”€ S4: Restore lifecycle state from DB at startup â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        # On restart, PositionManager was a blank object. We must restore each live
        # position from the persisted lifecycle_state row so the PM uses the original
        # SL/TP (not ATR-computed defaults). This prevents ensure_protection from
        # placing protection at the wrong price levels on startup.
        for sym_r, row_r in saved_symbols.items():
            if sym_r not in self.state:
                continue
            st_r = self.state[sym_r]
            if st_r.position not in ("LONG", "SHORT"):
                continue
            try:
                lifecycle = self.store.load_lifecycle_state(sym_r)
                if lifecycle and hasattr(self.position_manager, "restore_from_persisted"):
                    self.position_manager.restore_from_persisted(
                        symbol=sym_r,
                        lifecycle=lifecycle,
                        entry_price=float(st_r.entry_price or 0.0),
                        entry_qty=float(st_r.entry_qty or 0.0),
                        side_str=st_r.position,
                    )
                    logger.info(
                        f"[PM_STARTUP_RESTORE] {sym_r}: Lifecycle restored from DB "
                        f"(phase={getattr(lifecycle, 'phase', '?')} "
                        f"sl={getattr(lifecycle, 'current_stop', '?')} "
                        f"tp={getattr(lifecycle, 'tp2_price', '?')})"
                    )
            except Exception as _re:
                logger.warning(
                    f"[PM_STARTUP_RESTORE] {sym_r}: restore_from_persisted failed: {_re}. "
                    f"PM will use defaults â€” ensure_protection heartbeat will repair."
                )

        # F-8: Reconstruct dynamic threshold rolling-window history from DB so
        # the first cycle after restart uses the real historical distribution instead
        # of cold-starting at the fallback threshold (0.45) for 30+ cycles.
        try:
            from app.risk.dynamic_threshold import get_dynamic_threshold_calculator
            _thresh_bot_id = self.context.bot_instance_id if self.context else bot_id
            _thresh_calc = get_dynamic_threshold_calculator(bot_id=_thresh_bot_id)
            for _sym in self.symbols:
                try:
                    _thresh_calc.reconstruct_memory_from_db(
                        config_id=_thresh_bot_id,
                        symbol=_sym,
                    )
                    logger.info(
                        "[THRESHOLD] Reconstructed memory for %s (%d samples)",
                        _sym, _thresh_calc.sample_count(_sym),
                    )
                except Exception as _thresh_sym_err:
                    logger.warning("[THRESHOLD] Failed to reconstruct memory for %s: %s", _sym, _thresh_sym_err)
        except Exception as _thresh_err:
            logger.warning("[THRESHOLD] reconstruct_memory_from_db startup hook failed: %s", _thresh_err)

        # Only reconcile if running (avoid doing this purely on init if not about to run)
        # But PaperRunner is usually instantied to run.
        # self.reconcile_positions_from_exchange()
        # Skip legacy reconcile, use new one

        # âœ… FIX: DEFER position reconciliation to first run_cycle()
        # DON'T call exchange APIs in __init__() - this causes startup failures!
        # self.reconcile_positions_on_startup()
        self._reconciliation_done = False  # Track if we've done startup reconciliation
        
        # âœ… LOAD ORCHESTRATOR
        self.orchestrator: TradingOrchestrator | None = None
        self.initialization_status = "INITIALIZING"
        self.initialization_error: RunnerInitializationError | None = None
        self._load_orchestrator()
        self._assert_initialized()

    # â”€â”€ Atomic initialization contract â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    #: Collaborators that must exist on a context-bound (Auto Pilot) runner.
    REQUIRED_COLLABORATORS = (
        ("effective_policy", RunnerInitializationError.EFFECTIVE_POLICY_INVALID),
        ("strategy", RunnerInitializationError.STRATEGY_INITIALIZATION_FAILED),
        ("orchestrator", RunnerInitializationError.ORCHESTRATOR_INITIALIZATION_FAILED),
        ("executor", RunnerInitializationError.BROKER_INITIALIZATION_FAILED),
        ("position_manager", RunnerInitializationError.POSITION_REHYDRATION_FAILED),
    )

    def initialization_report(self) -> dict:
        """Machine-readable snapshot of which collaborators exist."""
        return {
            "context": self.context is not None,
            "status": getattr(self, "initialization_status", "UNKNOWN"),
            "effective_policy_hash": getattr(self, "effective_policy_hash", "") or None,
            "components": {
                name: getattr(self, name, None) is not None
                for name, _ in self.REQUIRED_COLLABORATORS
            },
        }

    def _assert_initialized(self) -> None:
        """Reject a half-built Auto Pilot runner.

        A contextless (legacy/global) runner is exempt: it never trades and only
        serves diagnostic endpoints.
        """
        if self.context is None:
            self.initialization_status = "LEGACY_NO_CONTEXT"
            return
        for attr, reason_code in self.REQUIRED_COLLABORATORS:
            if getattr(self, attr, None) is None:
                err = RunnerInitializationError(
                    reason_code,
                    f"Auto Pilot runner is missing required component '{attr}'",
                    bot_instance_id=getattr(self.context, "bot_instance_id", None),
                )
                self.initialization_status = "FAILED_INITIALIZATION"
                self.initialization_error = err
                raise err
        self.initialization_status = "READY"

    def _set_bot_health(
        self,
        *,
        status: str,
        message: str | None = None,
        reason_code: str | None = None,
        recommended_action: str | None = None,
        last_error: str | None = None,
        last_warning: str | None = None,
    ) -> None:
        if not self.context:
            return
        bot_id = self.context.bot_instance_id
        if not bot_id:
            return
        # Reduce DB churn: only write when the OBSERVABLE state changes.
        # Keying on status alone let the reason_code drift out of sync
        # (same status, different cause -> no write, stale evidence).
        _health_key = (status, reason_code)
        if self._last_bot_health_status == _health_key:
            return
        self._last_bot_health_status = _health_key
        # last_error is current-state, not history: clear it when the bot is no
        # longer in error.  The append-only history lives in bot_system_events.
        _resolved_last_error = resolve_last_error(
            status=status,
            message=message,
            reason_code=reason_code,
            explicit_last_error=last_error,
        )
        try:
            from shared_lib.persistence.db import utc_now_iso
            now = utc_now_iso()
            with self.db.connect() as conn:
                conn.execute(
                    """
                    UPDATE bot_instances
                    SET bot_health_status=?,
                        bot_health_message=?,
                        bot_health_reason_code=?,
                        bot_health_recommended_action=?,
                        bot_health_updated_at=?,
                        last_error=?,
                        last_warning=COALESCE(?, last_warning),
                        updated_at=?
                    WHERE id=?
                    """,
                    (
                        status,
                        message,
                        reason_code,
                        recommended_action,
                        now,
                        _resolved_last_error,
                        last_warning,
                        now,
                        bot_id,
                    ),
                )
        except Exception:
            pass

    def _update_bot_health_from_reason_code(self, *, reason_code: str | None, reason: str | None) -> None:
        if not reason_code:
            return
        # Map policy reason codes to user-facing health states.
        if reason_code in {"MIN_NOTIONAL_NOT_MET", "SIZE_ZERO", "PRICE_INVALID"}:
            self._set_bot_health(
                status="ERROR_SIZING_FAILURE",
                reason_code="TRADE_AMOUNT_TOO_SMALL_MINIMUM_50_USDT",
                message=(
                    "The bot cannot place trades because your trade amount is below the exchange minimum. "
                    "Increase trade amount to at least 50 USDT per position."
                ),
                recommended_action="Increase trade amount per position or reduce selected symbols.",
            )
            return
        if reason_code == "CIRCUIT_BREAKER_TRIPPED":
            self._set_bot_health(
                status="PAUSED_CIRCUIT_BREAKER",
                reason_code="CIRCUIT_BREAKER_TRIPPED",
                message="Trading is paused because repeated execution or exchange errors were detected.",
                recommended_action="Check exchange connection and API credentials.",
            )
            return
        if reason_code == "KILL_SWITCH_ACTIVE":
            self._set_bot_health(
                status="PAUSED_KILL_SWITCH",
                reason_code="KILL_SWITCH_TRIGGERED",
                message="Trading is paused because the loss protection limit was reached.",
                recommended_action="Review performance before restarting the bot.",
            )
            return
        if reason_code in {"DAILY_LOSS_LIMIT", "WEEKLY_DRAWDOWN_LIMIT", "MONTHLY_DRAWDOWN_LIMIT", "PORTFOLIO_RISK_BUDGET", "MARGIN_USAGE_LIMIT"}:
            self._set_bot_health(
                status="PAUSED_RISK_LIMIT",
                reason_code=reason_code,
                message="Trading is paused because a risk protection rule was triggered.",
                recommended_action="Review risk settings and performance before restarting.",
            )
            return
        if reason_code == "EVENT_BLACKOUT":
            self._set_bot_health(
                status="PAUSED_EVENT_BLACKOUT",
                reason_code="EVENT_BLACKOUT",
                message="Trading is paused due to an event blackout window.",
                recommended_action="No action needed. Trading resumes after the blackout window ends.",
            )
            return
        if reason_code in {"CONSECUTIVE_LOSS_COOLDOWN", "CONSECUTIVE_LOSS_DAY_PAUSE", "COOLDOWN_ACTIVE", "SL_COOLDOWN_ACTIVE"}:
            self._set_bot_health(
                status="PAUSED_CONSECUTIVE_LOSS_COOLDOWN",
                reason_code=reason_code,
                message="Trading is paused due to a cooldown after losses or a stop-loss event.",
                recommended_action="No action needed. Trading resumes after the cooldown ends.",
            )
            return
        if reason_code == "DAILY_TRADE_LIMIT":
            self._set_bot_health(
                status="PAUSED_MAX_DAILY_TRADES",
                reason_code="MAX_DAILY_TRADES_REACHED",
                message="Trading is paused because the daily trade limit was reached.",
                recommended_action="No action needed. Trading resumes after the daily reset.",
            )
            return
        if reason_code == "MAX_POSITIONS_REACHED":
            self._set_bot_health(
                status="PAUSED_MAX_OPEN_POSITIONS",
                reason_code="MAX_OPEN_POSITIONS_REACHED",
                message="Trading is paused because the maximum number of open positions was reached.",
                recommended_action="No action needed. Close positions or increase limits if appropriate.",
            )
            return
        if reason_code in {"SIGNAL_HOLD", "LOW_CONFIDENCE"}:
            self._set_bot_health(
                status="WAITING_FOR_SETUP",
                reason_code="NO_HIGH_QUALITY_SETUP",
                message="Bot is running, but no high-quality setup is available right now. No action is needed.",
                recommended_action="No action needed.",
            )
            return

    def _load_orchestrator(self):
        """Build the TradingOrchestrator for this bot.

        Contract (Auto Pilot):
          * ``self.context is None``  -> legacy/global runner, orchestrator stays
            inactive.  This runner is diagnostic-only and never trades.
          * ``self.context is not None`` -> the orchestrator is MANDATORY.  Any
            failure raises ``RunnerInitializationError`` so the runner is never
            registered in a half-initialised state.  Previously the exception was
            printed and swallowed, leaving ``self.orchestrator = None`` and
            emitting ERROR_STRATEGY_UNAVAILABLE on every tick indefinitely.
        """
        try:
            if not self.context:
                # No context means we are in legacy mode with no bot instance
                # For safety, we skip orchestrator or use system defaults if really needed
                # But requirement says "Only MultiBotRunner -> PaperRunner...".
                # We should assume context is present.
                print("âš ï¸ No BotContext provided, TradingOrchestrator inactive.")
                return

            # Construct UserConfigurableLimits from Context
            try:
                risk_level = RiskLevel(self.context.risk_level)
            except ValueError:
                risk_level = RiskLevel.MEDIUM

            # Resolve allocation settings
            use_fixed = False
            fixed_val = None
            cap_alloc = 1.0
            
            if hasattr(self.context, 'allocation_type'):
                if self.context.allocation_type == "fixed_amount":
                     use_fixed = True
                     fixed_val = float(self.context.allocation_value)
                elif self.context.allocation_type == "percent":
                     # allocation_value is 0-100, convert to 0.0-1.0
                     cap_alloc = float(self.context.allocation_value) / 100.0

            # âœ… DEBUG: Log the values we found
            print(f"[RUNNER CONFIG] Allocation settings from context: type='{getattr(self.context, 'allocation_type', 'MISSING')}', value='{getattr(self.context, 'allocation_value', 'MISSING')}'")
            print(f"[RUNNER CONFIG] Mapped to: use_fixed={use_fixed}, fixed_val={fixed_val}, cap_alloc={cap_alloc}")

            # Daily loss ceiling comes from the resolved policy when available and
            # otherwise from the context field that actually exists
            # (BotRunContext.daily_max_loss_usdt).  The previous code read a
            # non-existent ``context.max_daily_loss``; the resulting AttributeError
            # was swallowed and left the orchestrator permanently None.
            _daily_loss_usdt = getattr(self.effective_policy, "max_daily_loss", None)
            if _daily_loss_usdt is None:
                _daily_loss_usdt = getattr(self.context, "daily_max_loss_usdt", 0.0)
            _daily_loss_usdt = float(_daily_loss_usdt or 0.0)

            # Map context params (which came from preset) to Orchestrator config
            user_config = UserConfigurableLimits(
                risk_level=risk_level,
                max_daily_loss_pct=(
                    float(_daily_loss_usdt) / float(self.context.capital_budget)
                    if float(getattr(self.context, "capital_budget", 0.0) or 0.0) > 0
                    else 0.0
                ),
                max_trades_per_day=self.context.max_trades_daily,
                max_open_positions=self.context.max_open_positions,
                default_stop_loss_pct=self.context.stop_loss_pct,
                requested_leverage={s: int(self.context.max_leverage) for s in self.context.symbols},
                allowed_symbols=self.context.symbols,
                paper_mode=self.context.execution_mode == "paper",
                # Pass min confidence from context if available, else use SafetyConfig default (0.40)
                # NOTE: Do NOT hardcode 0.5 here â€” it overrides safety_engine's min_confidence_hard
                min_strategy_confidence=getattr(self.context, 'min_confidence', 0.40), 
                strict_circuit_breakers=False,
                
                # âœ… CORRECTION: Pass allocation settings
                use_fixed_size=use_fixed,
                fixed_size_usdt=fixed_val,
                capital_allocation_pct=cap_alloc
            )
            
            # REMOVED: Overwrite of capital_allocation_pct
            # user_config.capital_allocation_pct = 1.0 
            
            print(f"[RUNNER CONFIG] Final UserConfig: use_fixed_size={user_config.use_fixed_size}, fixed_size_usdt={user_config.fixed_size_usdt}, capital_allocation_pct={user_config.capital_allocation_pct}")
            # Actually Orchestrator uses user_config.capital_allocation_pct to scale equity.
            # BotInstance has capital_allocation (value). 
            # If we want to support fixed allocation, we need logic.
            # But prompt says "Auto Pilot uses internal strategy... single execution path".
            # We'll stick to a simple mapping for now.
            
            self.orchestrator = TradingOrchestrator(
                config_id=self.context.bot_instance_id,
                user_config=user_config,
                strategy_id=self.context.strategy_id,
                broker_id=self.context.broker_account_id,
                strategy_instance=self.strategy,
                effective_policy=self.effective_policy,
            )
            if self.orchestrator is None:
                raise RunnerInitializationError(
                    RunnerInitializationError.ORCHESTRATOR_INITIALIZATION_FAILED,
                    "TradingOrchestrator construction returned no instance",
                    bot_instance_id=self.context.bot_instance_id,
                )
            print(f"Loaded TradingOrchestrator for Bot {self.context.bot_instance_id}")

        except RunnerInitializationError:
            self.orchestrator = None
            raise
        except Exception as e:
            # FAIL CLOSED.  An Auto Pilot runner with a context but no orchestrator
            # has no strategy path and must not be scheduled for trading cycles.
            self.orchestrator = None
            import traceback
            traceback.print_exc()
            raise RunnerInitializationError(
                RunnerInitializationError.ORCHESTRATOR_INITIALIZATION_FAILED,
                "Failed to initialise TradingOrchestrator for Auto Pilot bot",
                cause=e,
                bot_instance_id=getattr(self.context, "bot_instance_id", None),
            ) from e

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

    def _persist_protection_result(self, symbol: str, result: dict | None, source: str) -> None:
        """
        Keep position_lifecycle_state aligned with exchange protection repairs.

        ensure_protection() is the broker-facing operation; this helper is the
        persistence bridge so restored/repaired SL/TP IDs do not vanish from the
        durable lifecycle row.
        """
        if not isinstance(result, dict):
            return

        status = str(result.get("status") or ("repaired" if result.get("repaired") else "")).lower()
        reason = str(result.get("reason") or result.get("error") or source)

        if status == "flat":
            try:
                if hasattr(self.position_manager, "mark_position_flat"):
                    self.position_manager.mark_position_flat(symbol, reason=f"{source}:exchange_flat")
                elif self.store:
                    self.store.mark_lifecycle_flat(symbol, f"{source}:exchange_flat")
            except Exception as exc:
                logger.warning("[LIFECYCLE_TRUTH] %s: failed to mark FLAT after %s: %s", symbol, source, exc)
            return

        sl_order_id = result.get("sl_order_id")
        tp_order_id = result.get("tp_order_id")
        if "DUPLICATE_4130" in {str(sl_order_id or ""), str(tp_order_id or "")}:
            logger.critical(
                "[LIFECYCLE_TRUTH] %s: refusing to persist placeholder "
                "DUPLICATE_4130 as protection evidence (source=%s status=%s reason=%s)",
                symbol,
                source,
                status,
                reason,
            )
            return
        if not sl_order_id and not tp_order_id:
            if status in {"repair_failed", "repair_pending"}:
                logger.critical(
                    "[LIFECYCLE_TRUTH] %s: protection repair did not produce IDs "
                    "(source=%s status=%s reason=%s)",
                    symbol, source, status, reason,
                )
            return

        persisted = False
        try:
            if hasattr(self.position_manager, "update_protection_order_ids"):
                persisted = bool(
                    self.position_manager.update_protection_order_ids(
                        symbol,
                        sl_order_id=sl_order_id,
                        tp_order_id=tp_order_id,
                        status="PROTECTED",
                        reason=f"{source}:{status or 'ok'}",
                    )
                )
        except Exception as exc:
            logger.warning("[LIFECYCLE_TRUTH] %s: PM protection ID update failed: %s", symbol, exc)

        if not persisted and self.store:
            try:
                self.store.update_lifecycle_protection_ids(
                    symbol,
                    sl_order_id=sl_order_id,
                    tp_order_id=tp_order_id,
                    status="PROTECTED",
                    reason=f"{source}:{status or 'ok'}",
                )
                persisted = True
            except Exception as exc:
                logger.warning("[LIFECYCLE_TRUTH] %s: DB protection ID update failed: %s", symbol, exc)

        if persisted:
            logger.info(
                "[LIFECYCLE_TRUTH] %s: persisted protection IDs from %s "
                "(sl_order_id=%s tp_order_id=%s)",
                symbol, source, sl_order_id, tp_order_id,
            )

    # âœ… NEW: reconcile positions on startup (exchange truth overrides DB)
    def reconcile_positions_on_startup(self) -> None:
        """
        24/7 Position Manager: Fast sync of all exchange positions.
        If a position was opened manually or is missing TP/SL, we discover it and track it.
        """
        import time
        now = time.time()
        # Throttle to avoid rate limits (runs every ~30 seconds)
        if hasattr(self, "_last_reconcile_time") and now - getattr(self, "_last_reconcile_time") < 30:
            return
        self._last_reconcile_time = now

        try:
            try:
                if self.store and hasattr(self.store, "reconcile_lifecycle_from_fills"):
                    _closed_rows = self.store.reconcile_lifecycle_from_fills()
                    if _closed_rows:
                        logger.warning(
                            "[LIFECYCLE_TRUTH] marked %d lifecycle rows FLAT from persisted CLOSE/ALREADY_FLAT fills: %s",
                            len(_closed_rows),
                            [r.get("symbol") for r in _closed_rows],
                        )
            except Exception as _fill_reconcile_err:
                logger.warning(
                    "[LIFECYCLE_TRUTH] DB fill lifecycle reconciliation failed: %s",
                    _fill_reconcile_err,
                )

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

                try:
                    amt = float(row.get("positionAmt", "0") or 0.0)
                except Exception:
                    amt = 0.0

                # âœ… 24/7 ACCOUNT PROTECTION: Dynamically track manual positions!
                if amt != 0 and sym not in self.state:
                    logger.info(f"[24/7 PROTECTION] Discovered untracked external position on {sym} ({amt}). Taking over management to ensure SL/TP bounds.")
                    self.state[sym] = SymbolState()
                    if sym not in self.trade_symbols:
                        self.trade_symbols.append(sym)
                    if sym not in self.symbols:
                        self.symbols.append(sym)

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

                if st.position not in ("LONG", "SHORT"):
                    try:
                        lifecycle = self.store.load_lifecycle_state(sym)
                        if lifecycle and str(lifecycle.get("phase") or "").upper() not in {
                            "FLAT", "CLOSED", "DONE", "CANCELLED", "CANCELED"
                        }:
                            if hasattr(self.position_manager, "mark_position_flat"):
                                self.position_manager.mark_position_flat(sym, reason="STARTUP_RECONCILE:exchange_flat")
                            else:
                                self.store.mark_lifecycle_flat(sym, "STARTUP_RECONCILE:exchange_flat")
                            logger.warning("[LIFECYCLE_TRUTH] %s: exchange flat; lifecycle marked FLAT", sym)
                    except Exception as _flat_lifecycle_err:
                        logger.warning(
                            "[LIFECYCLE_TRUTH] %s: failed to mark exchange-flat lifecycle: %s",
                            sym, _flat_lifecycle_err,
                        )

                # âœ… HARDENING: Ensure protection exists for any found position
                if st.position in ("LONG", "SHORT"):
                    try:
                        _pm_pos = self.position_manager.get_position(sym) if hasattr(self, "position_manager") else None
                        _pm_sl = float(_pm_pos.sl.current_stop) if _pm_pos and _pm_pos.sl.current_stop else None
                        _pm_tp = float(_pm_pos.tp.tp2_price) if _pm_pos and _pm_pos.tp.tp2_price else None
                        _repair_src = "STARTUP_RECONCILE" if (_pm_sl and _pm_tp) else "FALLBACK_COMPUTED"

                        _startup_protection = self.executor.ensure_protection(
                            symbol=sym,
                            sl_price=_pm_sl,
                            tp_price=_pm_tp,
                            repair_source=_repair_src
                        )
                        self._persist_protection_result(sym, _startup_protection, _repair_src)
                        self.audit.event(
                            event_type="INFO",
                            run_id=self.run_id,
                            symbol=sym,
                            action="STARTUP_PROTECTION_RESTORED",
                            details={"position": st.position, "repair_source": _repair_src},
                        )
                    except Exception as e:
                        logger.warning(f"Failed to restore protection for {sym} on startup: {e}")

                # âœ… DISCOVERY TRACE: Record to decision_traces so audit joins work for closing
                if st.position in ("LONG", "SHORT"):
                    try:
                        from shared_lib.persistence.trace_recorder import get_trace_recorder
                        recorder = get_trace_recorder()
                        # Use a stable trace ID prefixed with 'rec' to identify reconciled entries
                        rec_trace_id = f"rec_{self.run_id}_{sym}"
                        recorder.start_trace(
                            run_id=self.run_id,
                            cycle_id=getattr(self, "cycle_id", "STU"), # STU = STartUp
                            symbol=sym,
                            account_id=getattr(settings, "ACCOUNT_ID", "default"),
                            environment=getattr(settings, "EXECUTION_MODE", "paper"),
                            timeframe=self.interval,
                        )
                        recorder.record_market(rec_trace_id, last_price=st.entry_price or 0.0, equity=0.0)
                        recorder.record_ml_score(
                            trace_id=rec_trace_id,
                            score=0.0, # Neutral score for reconciled positions
                            action="RECONCILED",
                            model_version="manual_reconcile",
                            threshold=0.30
                        )
                        recorder.finalize(rec_trace_id, state_change=f"DISCOVERED_{st.position}", final_position=st.position)
                    except Exception as _tr_err:
                        logger.warning(f"Failed to record discovery trace for {sym}: {_tr_err}")

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
            
            # âœ… Finalize trace if active
            recorder = get_trace_recorder()
            recorder.finalize(
                trace_id=getattr(recorder, "_active_trace_id", None) or payload.get("trace_id", ""),  # fallback
                state_change=payload.get("decision", "NONE"),
                final_position=st.position,
            )
        except Exception as e:
            # Don't crash the bot because persistence failed â€” log it
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
        logger.warning("KILL_SWITCH_TRIGGERED â€” daily loss limit reached. Blocking all new entries.")
        self.audit.event(
            event_type="KILL_SWITCH_TRIGGERED",
            run_id=self.run_id,
            cycle_id=getattr(self, "cycle_id", None),
            details={
                "reason": "daily_loss_limit_reached",
                "realized_pnl": getattr(self.daily, "realized_pnl", None),
                "close_positions": settings.KILL_SWITCH_CLOSE_POSITIONS,
            },
        )

        # Collect all symbols with open positions.
        all_symbols = set()
        for sym, st in self.state.items():
            if getattr(st, "position", "NONE") not in ("NONE", "FLAT", None):
                all_symbols.add(sym)

        for sym in all_symbols:
            if settings.KILL_SWITCH_CLOSE_POSITIONS:
                logger.warning("KILL_SWITCH_CLOSING_OPEN_POSITIONS â€” attempting close for %s", sym)
                try:
                    result = self._close_managed_position(sym, "KILL_SWITCH_ACTIVE")
                    if not result.get("success"):
                        raise RuntimeError(result.get("error") or "kill_switch_close_failed")
                    logger.warning(
                        "KILL_SWITCH_POSITION_CLOSE_SUCCESS â€” %s closed. result=%s", sym, result
                    )
                    self.audit.event(
                        event_type="KILL_SWITCH_POSITION_CLOSE_SUCCESS",
                        run_id=self.run_id,
                        cycle_id=getattr(self, "cycle_id", None),
                        symbol=sym,
                        details={"result": str(result)},
                    )
                except Exception as e:
                    logger.error(
                        "KILL_SWITCH_POSITION_CLOSE_FAILED â€” %s could not be closed: %s", sym, e
                    )
                    self.audit.event(
                        event_type="KILL_SWITCH_POSITION_CLOSE_FAILED",
                        run_id=self.run_id,
                        cycle_id=getattr(self, "cycle_id", None),
                        symbol=sym,
                        details={"error": str(e)},
                    )

        logger.warning("KILL_SWITCH_NEW_ENTRIES_BLOCKED â€” no new trades will open this session.")

    def _close_managed_position(self, symbol: str, reason: str) -> Dict[str, Any]:
        """Close exactly the managed remainder and persist one canonical close fill."""
        st = self.state.get(symbol)
        pos = self.position_manager.get_position(symbol)
        if st is None or st.position not in ("LONG", "SHORT") or pos is None:
            return {"success": False, "error": "managed_position_state_required"}
        side = st.position
        qty = float(pos.current_qty or st.entry_qty or 0.0)
        if qty <= 0:
            return {"success": False, "error": "remaining_quantity_required"}
        try:
            price = float(self.client.last_price(symbol))
        except Exception:
            price = float(st.entry_price or pos.entry_price or 0.0)
        try:
            self.executor.cancel_open_orders(symbol)
        except Exception:
            pass
        result = self.executor.execute_signal(
            symbol, "CLOSE", 0.0,
            position_side=side,
            remaining_quantity=qty,
            fallback_price=price,
            cycle_id=self.cycle_id,
        )
        if not getattr(result, "success", False):
            return {"success": False, "error": result.error, "details": result.details}
        close_price = float(result.avg_price or price)
        entry_price = float(pos.entry_price or st.entry_price or 0.0)
        pnl = (close_price - entry_price) * qty if side == "LONG" else (entry_price - close_price) * qty
        from shared_lib.persistence.trade_fills import record_fill
        record_fill(
            self.db,
            symbol=symbol, side=side, action="CLOSE", qty=qty, price=close_price,
            fee=float((result.details or {}).get("fee") or 0.0), realized_pnl=pnl,
            strategy="orchestrated", bot_instance_id=self.context.bot_instance_id if self.context else None,
            user_id=self.context.user_id if self.context else None,
            broker_account_id=self.context.broker_account_id if self.context else None,
            execution_mode=self._effective_execution_mode(), timeframe=self.interval,
            position_id=st.position_id or pos.position_id, order_id=result.order_id,
            exit_reason=reason, initiator_type="BOT", trigger_source=reason,
            position_phase="EXITING", run_id=self.run_id, cycle_id=self.cycle_id,
            fill_type="FINAL_CLOSE", remaining_qty=0.0,
        )
        self.position_manager.close_position(symbol, reason)
        st.position = "NONE"
        st.entry_qty = 0.0
        st.position_id = None
        self.store.save_symbol(symbol, st)
        self._closed_symbols_this_cycle.add(symbol)
        logger.info(
            "[PAPER_LIFECYCLE] FINAL_CLOSE symbol=%s qty=%s position_id=%s reason=%s",
            symbol, qty, pos.position_id, reason,
        )
        return {"success": True, "qty": qty, "price": close_price, "pnl": pnl, "order_id": result.order_id}

    def _run_daily_close_from_cycle(self) -> int:
        """Evaluate and execute daily close from the active management heartbeat."""
        if not settings.DAILY_CLOSE_ENABLED:
            return 0
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo
        try:
            local = datetime.now(timezone.utc).astimezone(ZoneInfo(settings.DAILY_CLOSE_TIMEZONE))
            start_h, start_m = map(int, settings.DAILY_CLOSE_WINDOW_START.split(":"))
            end_h, end_m = map(int, settings.DAILY_CLOSE_WINDOW_END.split(":"))
        except Exception as exc:
            logger.error("DAILY_CLOSE_CONFIGURATION_ERROR: %s", exc)
            return 0
        minute = local.hour * 60 + local.minute
        start, end = start_h * 60 + start_m, end_h * 60 + end_m
        in_window = start <= minute < end if start <= end else minute >= start or minute < end
        if not in_window:
            return 0
        closed = 0
        for symbol, st in list(self.state.items()):
            pos = self.position_manager.get_position(symbol)
            if st.position not in ("LONG", "SHORT") or pos is None:
                continue
            try:
                price = float(self.client.last_price(symbol))
            except Exception:
                continue
            qty = float(pos.current_qty or 0.0)
            pnl = (price - pos.entry_price) * qty if st.position == "LONG" else (pos.entry_price - price) * qty
            notional = max(abs(pos.entry_price * qty), 1e-12)
            pnl_pct = pnl / notional * 100.0
            if pnl <= 0 or pnl < float(settings.DAILY_CLOSE_MIN_PROFIT_USDT or 0.0):
                continue
            if pnl_pct < float(settings.DAILY_CLOSE_MIN_PROFIT_PCT or 0.0):
                continue
            outcome = self._close_managed_position(symbol, "DAILY_CLOSE")
            if outcome.get("success"):
                closed += 1
                self.audit.event(
                    event_type="DAILY_CLOSE_POSITION_CLOSE_SUCCESS", run_id=self.run_id,
                    cycle_id=self.cycle_id, symbol=symbol,
                    details={"bot_instance_id": self.context.bot_instance_id if self.context else None, "pnl": pnl},
                )
        return closed

    # âœ… ADD: helper to confirm position is flat

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

    def _lookup_entry_order_evidence(self, symbol: str, client_order_id: str | None) -> tuple[str | None, dict | None]:
        if not client_order_id:
            return (None, None)
        client = getattr(self.executor, "client", None)
        if client is None or not hasattr(client, "get_order_by_client_order_id"):
            return (None, None)
        try:
            order = client.get_order_by_client_order_id(symbol, client_order_id)
        except Exception:
            return (None, None)
        if not isinstance(order, dict):
            return ("FOUND", None)
        status = str(order.get("status", "") or "").upper()
        if status:
            return (status, order)
        if order.get("orderId") or order.get("clientOrderId"):
            return ("FOUND", order)
        return (None, order)

    def _reconcile_entry_protection(self, symbol: str, exchange_pos_amt: float, st: SymbolState | None = None) -> None:
        if getattr(self.executor, "_entry_prot", None) is None:
            return
        ep = self.executor._entry_prot
        bot_id = self.context.bot_instance_id if self.context else "default"
        rows = ep.list_entries(bot_id, symbol=symbol)
        pending_side = "NONE"
        for row in rows:
            side = str(row.get("side") or "").upper()
            evidence, order_snapshot = self._lookup_entry_order_evidence(symbol, row.get("client_order_id"))
            result = ep.reconcile_entry(
                bot_id=bot_id,
                symbol=symbol,
                side=side,
                exchange_pos_amt=exchange_pos_amt,
                order_evidence=evidence,
                order_snapshot=order_snapshot,
            )
            if result in {"CONFIRMED", "ORDER_EVIDENCE_HELD", "WAITING_STRONGER_FLAT_PROOF"}:
                pending_side = "BUY" if side == "LONG" else "SELL"
        if st is not None:
            st.pending_open = pending_side

    def _run_dynamic_universe_shadow_diagnostics(self) -> dict[str, Any]:
        """
        Shadow-only dynamic universe diagnostics.

        This method records what dynamic symbols would have produced as strategy
        outputs without changing runner symbols, executor allowlists, allocation,
        leverage, risk controls, entry protection, or strategy execution flow.
        """
        active_mode = str(getattr(settings, "SYMBOL_UNIVERSE_MODE", "static") or "static")
        shadow_enabled = bool(getattr(settings, "DYNAMIC_UNIVERSE_SHADOW_ENABLED", False))
        auto_enabled = bool(getattr(settings, "AUTO_SYMBOL_SELECTION_ENABLED", False))
        print(
            "[DYNAMIC_SHADOW_DEBUG] hook entry "
            f"mode={active_mode} shadow_enabled={shadow_enabled} auto_enabled={auto_enabled} "
            f"cycle_id={getattr(self, 'cycle_id', None)}"
        )
        if not shadow_enabled and active_mode != "dynamic_shadow":
            print("[DYNAMIC_SHADOW_DEBUG] hook disabled by config")
            return {"status": "disabled"}

        try:
            interval_s = int(getattr(settings, "DYNAMIC_UNIVERSE_SHADOW_INTERVAL_SECONDS", 300))
        except Exception:
            interval_s = 300
        now = time.time()
        if interval_s > 0 and (now - float(getattr(self, "_dynamic_shadow_last_run_ts", 0.0) or 0.0)) < interval_s:
            print(
                "[DYNAMIC_SHADOW_DEBUG] hook skipped interval "
                f"elapsed={now - float(getattr(self, '_dynamic_shadow_last_run_ts', 0.0) or 0.0):.2f}s "
                f"interval={interval_s}s"
            )
            return {"status": "skipped", "reason": "interval_not_elapsed"}
        self._dynamic_shadow_last_run_ts = now

        shadow_strategy = getattr(self, "_dynamic_shadow_strategy", None)
        if shadow_strategy is None:
            return {"status": "skipped", "reason": "shadow_strategy_unavailable"}

        recorder = getattr(self, "_dynamic_shadow_recorder", None)
        if recorder is None:
            try:
                recorder = DynamicUniverseShadowRecorder(self.db)
                self._dynamic_shadow_recorder = recorder
            except Exception as err:
                logger.warning("[DYNAMIC_UNIVERSE_SHADOW] recorder unavailable: %s", err)
                return {"status": "error", "reason": "recorder_unavailable", "error": str(err)}

        try:
            print("[DYNAMIC_SHADOW_DEBUG] discovery start")
            universe = DynamicUniverseService().discover()
        except Exception as err:
            logger.warning("[DYNAMIC_UNIVERSE_SHADOW] discovery failed: %s", err)
            print(f"[DYNAMIC_SHADOW_DEBUG] discovery failed error={err}")
            return {"status": "error", "reason": "discovery_failed", "error": str(err)}

        live_set = {str(s).upper() for s in list(self.trade_symbols or []) + list(self.live_symbols or []) if s}
        print(
            "[DYNAMIC_SHADOW_DEBUG] discovery complete "
            f"ranked={len(universe.get('ranked_candidates', []) or [])} "
            f"structural={len(universe.get('structural_candidates', []) or [])} "
            f"live_symbols={len(live_set)}"
        )
        ranking_rows: list[dict[str, Any]] = []
        try:
            selector = getattr(self, "_dynamic_symbol_selector", None)
            if selector is None:
                selector = DynamicSymbolSelector(self.db)
                self._dynamic_symbol_selector = selector
            print("[DYNAMIC_SHADOW_DEBUG] selector call start")
            ranking_rows = selector.rank_shadow_universe(
                universe,
                live_symbols=live_set,
                bot_instance_id=self.context.bot_instance_id if self.context else "default",
                persist=True,
            )
            print(f"[DYNAMIC_SHADOW_DEBUG] selector call complete rankings={len(ranking_rows)}")
            try:
                promotion_evaluator = getattr(self, "_symbol_promotion_evaluator", None)
                if promotion_evaluator is None:
                    promotion_evaluator = SymbolPromotionEvaluator(self.db)
                    self._symbol_promotion_evaluator = promotion_evaluator
                promotion_decision = promotion_evaluator.evaluate_and_record(
                    bot_instance_id=self.context.bot_instance_id if self.context else "default"
                )
                print(
                    "[DYNAMIC_SHADOW_DEBUG] promotion evaluation "
                    f"decision={promotion_decision.get('decision_type')} "
                    f"status={promotion_decision.get('status')} "
                    f"executed={promotion_decision.get('executed')} "
                    f"failures={promotion_decision.get('failure_reasons')}"
                )
            except Exception as _promotion_err:
                logger.warning("[SYMBOL_PROMOTION] evaluation failed: %s", _promotion_err)
            try:
                if bool(getattr(settings, "EVENT_NEWS_MODE_CONTROLLER_ENABLED", True)):
                    mode_controller = getattr(self, "_event_news_mode_controller", None)
                    if mode_controller is None:
                        mode_controller = EventNewsModeController(self.db)
                        self._event_news_mode_controller = mode_controller
                    mode_decision = mode_controller.evaluate_and_record()
                    print(
                        "[EVENT_NEWS_MODE] evaluation "
                        f"decision={mode_decision.get('decision_type')} "
                        f"mode={mode_decision.get('current_mode')} "
                        f"max_action={mode_decision.get('max_allowed_action')} "
                        f"safety={mode_decision.get('safety_status')} "
                        f"failures={mode_decision.get('failed_criteria')}"
                    )
            except Exception as _event_news_mode_err:
                logger.warning("[EVENT_NEWS_MODE] evaluation failed: %s", _event_news_mode_err)
        except Exception as err:
            logger.warning("[DYNAMIC_UNIVERSE_SHADOW] ranking persist failed: %s", err)
            print(f"[DYNAMIC_SHADOW_DEBUG] ranking persist failed error={err}")

        eval_top_n = int(getattr(settings, "DYNAMIC_UNIVERSE_SHADOW_EVAL_TOP_N", 30) or 30)
        ranked = universe.get("ranked_candidates", []) or []

        rows: list[dict[str, Any]] = []
        evaluated = 0
        would_pass = 0

        for candidate in ranked:
            symbol = str(candidate.get("symbol") or "").upper()
            in_live = symbol in live_set
            row: dict[str, Any] = {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "run_id": self.run_id,
                "cycle_id": self.cycle_id,
                "bot_instance_id": self.context.bot_instance_id if self.context else "default",
                "symbol": symbol,
                "rank": candidate.get("rank"),
                "in_live_config": in_live,
                "was_evaluated": False,
                "would_pass_strategy": False,
                "quote_volume_24h": candidate.get("quote_volume_24h"),
                "spread_bps": candidate.get("spread_bps"),
                "exclusion_reasons": candidate.get("exclusion_reasons") or [],
                "diagnostics": {
                    "shadow_only": True,
                    "already_live_configured": in_live,
                    "total_ranked_candidates": universe.get("total_ranked_candidates"),
                    "discovery_generated_at": universe.get("generated_at"),
                },
            }

            if in_live:
                row["reason"] = "already_live_configured"
                rows.append(row)
                continue
            if evaluated >= eval_top_n:
                row["reason"] = "not_evaluated_shadow_top_n_limit"
                rows.append(row)
                continue

            evaluated += 1
            try:
                result = shadow_strategy.get_signal(symbol)
                signal = getattr(getattr(result, "signal", None), "value", getattr(result, "signal", None))
                confidence = float(getattr(result, "confidence", 0.0) or 0.0)
                meta = getattr(result, "meta", None) or {}
                threshold = meta.get("threshold")
                try:
                    threshold = float(threshold) if threshold is not None else None
                except Exception:
                    threshold = None
                passed = str(signal).upper() in {"BUY", "SELL"} and confidence > 0
                if passed:
                    would_pass += 1
                row.update(
                    {
                        "was_evaluated": True,
                        "would_pass_strategy": passed,
                        "signal": str(signal).upper() if signal is not None else None,
                        "confidence": confidence,
                        "threshold": threshold,
                        "reason": getattr(result, "reason", None),
                    }
                )
                row["diagnostics"].update(
                    {
                        "strategy": getattr(shadow_strategy, "name", "unknown"),
                        "strategy_version": getattr(shadow_strategy, "version", "0"),
                        "meta": meta,
                    }
                )
            except Exception as err:
                row.update(
                    {
                        "was_evaluated": True,
                        "would_pass_strategy": False,
                        "reason": "shadow_strategy_error",
                    }
                )
                row["diagnostics"].update({"error": str(err)})
                logger.debug("[DYNAMIC_UNIVERSE_SHADOW] strategy error %s: %s", symbol, err)
            rows.append(row)

        try:
            recorder.record_many(rows)
        except Exception as err:
            logger.warning("[DYNAMIC_UNIVERSE_SHADOW] persist failed: %s", err)
            return {"status": "error", "reason": "persist_failed", "error": str(err)}

        missed = [
            row["symbol"]
            for row in rows
            if row.get("would_pass_strategy") and not row.get("in_live_config")
        ]
        logger.info(
            "[DYNAMIC_UNIVERSE_SHADOW] ranked=%s scored=%s evaluated=%s would_pass=%s missed=%s",
            len(ranked),
            len(ranking_rows),
            evaluated,
            would_pass,
            missed[:10],
        )
        return {
            "status": "completed",
            "ranked": len(ranked),
            "scored": len(ranking_rows),
            "evaluated": evaluated,
            "would_pass": would_pass,
            "missed_opportunities": missed,
            "top_recommendations": [
                {
                    "symbol": row.get("symbol"),
                    "rank": row.get("rank"),
                    "score": row.get("score"),
                    "recommended_action": row.get("recommended_action"),
                    "inclusion_reason": row.get("inclusion_reason"),
                    "exclusion_reason": row.get("exclusion_reason"),
                }
                for row in ranking_rows[:10]
            ],
        }

    # âœ… ADD: Encapsulated cycle execution (for MultiBotRunner)
    def run_cycle(self) -> Dict[str, Any]:
        """
        Execute one full cycle of trading for all symbols.
        Returns a summary of actions taken.
        """
        results = {}
        
        # Guard against overlapping cycles for this runner instance
        with self.cycle_guard(timeout_s=5.0) as acquired:
            if not acquired:
                return {"status": "skipped", "reason": "cycle_lock_busy"}
            
            # Reset per-cycle trackers
            self.live_trades_this_cycle = 0
            self._closed_symbols_this_cycle.clear()
            self.cycle_id = str(uuid.uuid4())
            self._cycle_stats = _CycleStats()  # â”€â”€ Visibility: per-cycle aggregator
            
            # âœ… RECONCILE ON FIRST RUN (Exchange truth wins over DB)
            if not getattr(self, "_reconciliation_done", False):
                logger.info(f"[STARTUP] Bot {self.run_id}: Performing initial position reconciliation from exchange...")
                self.reconcile_positions_on_startup()
                self._reconciliation_done = True

                # F-10: Create initial weekly/monthly drawdown snapshots if none exist.
                # DrawdownMonitor silently disables its gates when no snapshot is present,
                # leaving the bot unprotected for the entire first week/month of operation.
                try:
                    from datetime import timedelta
                    from app.risk.state import PeriodSnapshot, get_week_start, get_month_start
                    _startup_equity = self.get_account_balance()
                    if _startup_equity > 0:
                        _today = date.today()
                        _week_start = get_week_start(_today)
                        _month_start = get_month_start(_today)

                        if self.store.load_weekly_snapshot(_week_start) is None:
                            self.store.save_weekly_snapshot(PeriodSnapshot(
                                start_date=_week_start,
                                start_equity=_startup_equity,
                                peak_equity=_startup_equity,
                                low_equity=_startup_equity,
                            ))
                            logger.info("[DRAWDOWN] Created initial weekly snapshot: equity=%.2f", _startup_equity)

                        if self.store.load_monthly_snapshot(_month_start) is None:
                            self.store.save_monthly_snapshot(PeriodSnapshot(
                                start_date=_month_start,
                                start_equity=_startup_equity,
                                peak_equity=_startup_equity,
                                low_equity=_startup_equity,
                            ))
                            logger.info("[DRAWDOWN] Created initial monthly snapshot: equity=%.2f", _startup_equity)
                except Exception as _snap_err:
                    logger.warning("[DRAWDOWN] Failed to create initial snapshots: %s", _snap_err)
            
            # 1. Update Risk State (daily check)
            # If we passed midnight, day logic handles itself in DailyLossState usually, 
            # but we should ensure DB sync.
            if self.daily.day != date.today():
                self.daily = DailyLossState(day=date.today())
                # Re-load from DB just in case
                saved_daily = self.store.load_daily(self.daily.day)
                if saved_daily:
                    self.daily.realized_pnl = float(saved_daily.get("realized_pnl", 0.0))
                    self.daily.kill = bool(saved_daily.get("kill", False))
                    # F-9: restore consecutive loss state from DB (new day means counter was reset
                    # at midnight but we still restore from DB in case it wasn't 0)
                    self.daily.consecutive_losses = int(saved_daily.get("consecutive_losses", 0))
                    self.daily.consec_loss_cooldown_until_ms = int(saved_daily.get("consec_loss_cooldown_until_ms", 0))
                # D-1: Reset per-bot consecutive-loss guard at midnight so each day
                # starts fresh.  reset_bot() only clears this bot's state.
                try:
                    _daily_bot_id = self.context.bot_instance_id if self.context else "default"
                    _guard_reset_bot(_daily_bot_id)
                    logger.debug("[GUARD] Daily reset â€” cleared consecutive-loss state for bot %s", _daily_bot_id)
                except Exception as _grb_err:
                    logger.warning("[GUARD] reset_bot failed on daily reset: %s", _grb_err)

            # Critical account management belongs to run_cycle, the only active
            # MultiBotRunner heartbeat path.
            if self.daily.kill:
                self.activate_kill_switch()
                return {
                    "status": "paused",
                    "reason": "KILL_SWITCH_ACTIVE",
                    "health_status": "PAUSED_KILL_SWITCH",
                    "results": results,
                    "trades_count": 0,
                }
            _daily_closes = self._run_daily_close_from_cycle()
            if _daily_closes:
                results["_daily_close"] = {"decision": "CLOSE", "reason": "DAILY_CLOSE", "fills": _daily_closes}

            # 2. Iterate Symbols
            for symbol in self.trade_symbols:
                try:
                    res = self.step_symbol(symbol)
                    results[symbol] = res
                except ExchangeError as e:
                    results[symbol] = {"error": str(e)}
                    self._cycle_stats.errors += 1
                    self.circuit_registry.record_error(self._circuit_id)
                    try:
                        self.audit.event(
                            event_type="WARNING",
                            run_id=self.run_id,
                            symbol=symbol,
                            action="EXCHANGE_ERROR",
                            details={"error": str(e)}
                        )
                    except:
                        pass
                except FatalIntegrationError as e:
                    import sys
                    import logging
                    logging.getLogger(__name__).critical(f"FATAL INTEGRATION ERROR on {symbol}: {e}. Halting system to prevent ghost positions.")
                    sys.exit(1)
                except Exception as e:
                    results[symbol] = {"error": str(e)}
                    self._cycle_stats.errors += 1
                    try:
                        self.audit.event(
                            event_type="ERROR",
                            run_id=self.run_id,
                            symbol=symbol,
                            action="CYCLE_STEP_ERROR",
                            details={"error": str(e)}
                        )
                    except:
                        pass
            
            # 3. Post-cycle cleanup (e.g. realized PnL sync if needed)
            # (Logic handled inside step_symbol usually for PnL recording)
            try:
                shadow_diag = self._run_dynamic_universe_shadow_diagnostics()
                if shadow_diag.get("status") == "completed":
                    results["_dynamic_universe_shadow"] = shadow_diag
            except Exception as _dyn_shadow_err:
                logger.warning("[DYNAMIC_UNIVERSE_SHADOW] cycle hook failed: %s", _dyn_shadow_err)

            # â”€â”€ [CYCLE_SUMMARY] â€” compact INFO line emitted every cycle â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # Ensures the terminal always shows activity, even during 100%-HOLD markets.
            cs = self._cycle_stats
            cs.orders_placed = self.live_trades_this_cycle
            logger.info("[CYCLE_SUMMARY] %s", cs.summary_line())
            _summary = cs.as_dict()
            try:
                with self.db.connect() as _summary_conn:
                    _summary_conn.execute(
                        """INSERT OR REPLACE INTO cycle_decision_summaries
                           (cycle_id,run_id,bot_instance_id,created_at,summary_json)
                           VALUES (?,?,?,?,?)""",
                        (
                            self.cycle_id, self.run_id,
                            self.context.bot_instance_id if self.context else None,
                            datetime.now(timezone.utc).isoformat(),
                            json.dumps(_summary, sort_keys=True),
                        ),
                    )
                logger.info("[CYCLE_DECISION_SUMMARY] %s", json.dumps(_summary, sort_keys=True))
            except Exception as _summary_exc:
                logger.error("[CYCLE_DECISION_SUMMARY] persistence failed: %s", _summary_exc)

            return {
                "status": "completed", 
                "cycle_id": self.cycle_id, 
                "results": results,
                "trades_count": self.live_trades_this_cycle,
                "summary": {
                    "symbols_processed": cs.evaluated,
                    "decisions_buy": sum(1 for r in results.values() if isinstance(r, dict) and str(r.get("signal", "")).upper() == "BUY"),
                    "decisions_sell": sum(1 for r in results.values() if isinstance(r, dict) and str(r.get("signal", "")).upper() == "SELL"),
                    "holds": cs.hold,
                    "skips": sum(1 for r in results.values() if isinstance(r, dict) and r.get("skipped")),
                    "risk_blocks": cs.policy_blocks,
                    "execution_attempts": cs.execute_attempts,
                    "fills_recorded": self.live_trades_this_cycle,
                    "rejected_orders": max(0, cs.execute_attempts - self.live_trades_this_cycle),
                    "errors": cs.errors,
                    **_summary,
                },
            }


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
            # âœ… Record circuit breaker error (Universal)
            self.circuit_registry.record_error(self._circuit_id)
            pass
        return self.cached_balance

    def _get_drawdown_context(self) -> dict:
        """Compute current weekly/monthly drawdown percentages and consecutive losses.

        Returns a dict suitable for spreading into PolicyContext keyword args.
        Falls back to 0.0 if snapshots are unavailable.
        """
        today = date.today()
        equity = self.get_account_balance()
        result = {
            "weekly_drawdown_pct": 0.0,
            "monthly_drawdown_pct": 0.0,
            "max_weekly_drawdown_pct": getattr(settings, "MAX_WEEKLY_DRAWDOWN_PCT", 0.0),
            "max_monthly_drawdown_pct": getattr(settings, "MAX_MONTHLY_DRAWDOWN_PCT", 0.0),
            "consecutive_losses": getattr(self.daily, "consecutive_losses", 0),
            "max_consecutive_losses": getattr(settings, "MAX_CONSECUTIVE_LOSSES", 0),
            # D-1: pass soft/hard pause state from DailyLossState
            "consec_loss_cooldown_until_ms": getattr(self.daily, "consec_loss_cooldown_until_ms", 0),
            "consec_loss_day_paused": getattr(self.daily, "consec_loss_day_paused", False),
            # D-2: pass actual SL/TP and min R:R from settings
            "min_risk_reward": (
                float(self.context.min_risk_reward)
                if self.context and self.context.min_risk_reward > 0
                else float(getattr(settings, "MIN_RISK_REWARD", 0.0))
            ),
            # D-3: pass ATR sizing config
            "min_stop_atr_multiplier": getattr(settings, "MIN_STOP_ATR_MULTIPLIER", 0.5),
            "max_risk_per_trade_pct": getattr(settings, "MAX_RISK_PER_TRADE_PCT", 1.0),
        }
        try:
            from app.risk.state import get_week_start, get_month_start
            ws = self.drawdown_monitor.store.load_weekly_snapshot(get_week_start(today))
            ms = self.drawdown_monitor.store.load_monthly_snapshot(get_month_start(today))
            if ws and ws.peak_equity > 0 and equity > 0:
                dd_w = (ws.peak_equity - equity) / ws.peak_equity * 100.0
                result["weekly_drawdown_pct"] = max(0.0, dd_w)
            if ms and ms.peak_equity > 0 and equity > 0:
                dd_m = (ms.peak_equity - equity) / ms.peak_equity * 100.0
                result["monthly_drawdown_pct"] = max(0.0, dd_m)
        except Exception as _dd_err:
            logger.error(
                "[DRAWDOWN] Failed to read drawdown context for bot %s: %s. "
                "Drawdown gates will pass with 0%% â€” this is a safety risk.",
                getattr(self.context, "bot_instance_id", "default") if self.context else "default",
                _dd_err,
            )
        return result

    def _runtime_kyc_allowed(self) -> bool:
        """Resolve the real live-trading KYC gate; paper execution is explicitly exempt."""
        if self._effective_execution_mode() != "broker":
            return True
        if not self.context or not self.context.user_id:
            return False
        try:
            from shared_lib.core.policy.kyc_policy import check_kyc_gate, KYCAction
            allowed, _reason = check_kyc_gate(
                self.context.user_id,
                KYCAction.START_LIVE_TRADING,
            )
            return bool(allowed)
        except Exception as exc:
            logger.error("[KYC GATE] Runtime KYC check failed closed: %s", exc)
            return False

    def _runtime_live_readiness_allowed(self) -> bool:
        """Re-evaluate the controlled-beta gate for every live trade attempt."""
        if self._effective_execution_mode() != "broker":
            return True
        if not self.context or not self.context.bot_instance_id:
            return False
        try:
            from app.product_safety.readiness_gate import assert_user_capital_activation_allowed
            assert_user_capital_activation_allowed(
                db=self.db,
                bot_instance_id=self.context.bot_instance_id,
            )
            return True
        except Exception as exc:
            logger.error("[LIVE READINESS] Runtime readiness check failed closed: %s", exc)
            return False

    def process_external_signal_candidate(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        """Evaluate a queued external BUY/SELL candidate through runner safety.

        The webhook and queue only provide intent metadata. This method rechecks
        event safety, calls the policy engine for risk/sizing, runs the existing
        execution filter, and only then calls the normal executor. External
        candidates cannot choose size, leverage, SL/TP, close, reverse, reduce,
        cancel, or update protection.
        """
        source = str(candidate.get("source") or "EXTERNAL").upper()
        queue_id = str(candidate.get("queue_id") or "")
        symbol = str(candidate.get("symbol") or "").upper()
        action = str(candidate.get("action") or "").upper()

        # B-5 Fix: missing/null/invalid confidence MUST NOT default to 1.0.
        # A missing confidence means an unscored, unvalidated signal â€” treat as 0.0
        # and reject immediately so it cannot reach execution.
        _raw_conf = candidate.get("confidence")
        if _raw_conf is None:
            logger.warning(
                "[ExtSigRunner] EXTERNAL_SIGNAL_REJECTED_MISSING_CONFIDENCE "
                "source=%s symbol=%s queue_id=%s â€” confidence field absent",
                source, symbol, queue_id,
            )
            # _result() is defined later in this method; return dict directly for early exit.
            return {
                "symbol": symbol,
                "queue_status": "REJECTED",
                "final_status": "REJECTED_MISSING_CONFIDENCE",
                "final_reason": "EXTERNAL_SIGNAL_REJECTED_MISSING_CONFIDENCE",
                "event_filter_result": "NOT_CHECKED",
                "policy_result": "NOT_CHECKED",
                "sizing_result": None,
                "execution_result": "NOT_CALLED",
                "source": source,
                "queue_id": queue_id,
            }
        try:
            confidence = float(_raw_conf)
        except (TypeError, ValueError):
            confidence = -1.0  # forces the invalid check below
        # F-14: cap external confidence so callers cannot claim 0.99 and bypass
        # the dynamic threshold that internal signals must pass.
        _MAX_EXTERNAL_CONFIDENCE = 0.75
        if 0.0 <= confidence <= 1.0 and confidence > _MAX_EXTERNAL_CONFIDENCE:
            logger.info(
                "[ExtSigRunner] Capped confidence %.2f â†’ %.2f for external signal on %s",
                confidence, _MAX_EXTERNAL_CONFIDENCE, symbol,
            )
            confidence = _MAX_EXTERNAL_CONFIDENCE
        if confidence < 0 or confidence > 1:
            logger.warning(
                "[ExtSigRunner] EXTERNAL_SIGNAL_REJECTED_INVALID_CONFIDENCE "
                "source=%s symbol=%s queue_id=%s raw=%r",
                source, symbol, queue_id, _raw_conf,
            )
            return {
                "symbol": symbol,
                "queue_status": "REJECTED",
                "final_status": "REJECTED_INVALID_CONFIDENCE",
                "final_reason": "EXTERNAL_SIGNAL_REJECTED_INVALID_CONFIDENCE",
                "event_filter_result": "NOT_CHECKED",
                "policy_result": "NOT_CHECKED",
                "sizing_result": None,
                "execution_result": "NOT_CALLED",
                "source": source,
                "queue_id": queue_id,
            }
        proof_context = candidate.get("proof_context") if isinstance(candidate.get("proof_context"), dict) else {}
        trace_id: str | None = None

        def _result(
            *,
            queue_status: str,
            final_status: str,
            final_reason: str,
            event_filter_result: str | None = None,
            policy_result: str | None = None,
            sizing_result: str | None = None,
            execution_result: str | None = None,
            decision_trace_id: str | None = None,
        ) -> Dict[str, Any]:
            return {
                "queue_status": queue_status,
                "final_status": final_status,
                "final_reason": final_reason,
                "event_filter_result": event_filter_result,
                "policy_result": policy_result,
                "sizing_result": sizing_result,
                "execution_result": execution_result,
                "decision_trace_id": decision_trace_id or trace_id,
            }

        if action not in {"BUY", "SELL"}:
            return _result(
                queue_status="REJECTED",
                final_status="REJECTED_UNSUPPORTED_ACTION",
                final_reason=f"{source} action {action!r} is not allowed",
                execution_result="NOT_CALLED:UNSUPPORTED_ACTION",
            )
        if not symbol:
            return _result(
                queue_status="REJECTED",
                final_status="REJECTED_INVALID_SYMBOL",
                final_reason="External signal missing symbol",
                execution_result="NOT_CALLED:INVALID_SYMBOL",
            )

        recorder = get_trace_recorder()
        try:
            trace_id = recorder.start_trace(
                run_id=self.run_id,
                cycle_id=getattr(self, "cycle_id", None),
                symbol=symbol,
                account_id=getattr(settings, "ACCOUNT_ID", "default"),
                environment=getattr(settings, "EXECUTION_MODE", "paper"),
                timeframe=self.interval,
                bot_instance_id=self.context.bot_instance_id if self.context else None,
                user_id=self.context.user_id if self.context else None,
                effective_policy_hash=self.effective_policy_hash,
                signal_source="TRADINGVIEW_EXTERNAL",
            )
        except Exception:
            trace_id = None

        if symbol not in self.state:
            self.state[symbol] = SymbolState()
        st = self.state[symbol]

        try:
            evt = self.event_blackout_filter.check(symbol=symbol)
            if evt.is_blocked:
                if trace_id:
                    try:
                        recorder.record_event_block(
                            trace_id,
                            reason=evt.reason or "EVENT_BLACKOUT",
                            details=evt.details,
                        )
                        recorder.finalize(
                            trace_id,
                            state_change="EXTERNAL_SIGNAL_EVENT_BLOCKED",
                            final_position=st.position if st.position in ("LONG", "SHORT") else "NONE",
                        )
                    except Exception:
                        pass
                return _result(
                    queue_status="REJECTED",
                    final_status="REJECTED_EVENT_BLACKOUT",
                    final_reason=evt.reason or "EVENT_BLACKOUT",
                    event_filter_result=f"BLOCKED:{evt.reason or 'EVENT_BLACKOUT'}",
                    execution_result="NOT_CALLED:EVENT_BLACKOUT",
                )
        except Exception as exc:
            logger.warning("[ExtSigRunner] event filter failed safely for %s: %s", symbol, exc)

        clean_proof_enabled = (
            bool(proof_context.get("phase5b_clean_candle_proof"))
            and str(proof_context.get("proof_type") or "") == "CONTROLLED_CLEAN_CANDLE_PROOF"
            and source == "TRADINGVIEW"
        )
        try:
            if clean_proof_enabled:
                kl = proof_context.get("clean_candles") or []
                if not isinstance(kl, list):
                    kl = []
                logger.warning(
                    "[ExtSigRunner] %s queue=%s using CONTROLLED CLEAN-CANDLE PROOF market data",
                    symbol,
                    queue_id[:8],
                )
            else:
                kl = self.client.klines(symbol=symbol, interval=self.interval, limit=120)
        except Exception as exc:
            return _result(
                queue_status="REJECTED",
                final_status="REJECTED_STALE_MARKET_DATA",
                final_reason=f"Kline fetch failed before external signal evaluation: {exc}",
                event_filter_result="PASS",
                execution_result=f"NOT_CALLED:MARKET_DATA:{type(exc).__name__}",
            )
        if not kl:
            return _result(
                queue_status="REJECTED",
                final_status="REJECTED_STALE_MARKET_DATA",
                final_reason="No klines available for external signal evaluation",
                event_filter_result="PASS",
                execution_result="NOT_CALLED:NO_KLINES",
            )

        price = float(self.client.last_price(symbol) or 0.0)
        if clean_proof_enabled and proof_context.get("clean_reference_price"):
            try:
                price = float(proof_context.get("clean_reference_price") or price)
            except Exception:
                pass
        atr = float(calculate_atr(kl, period=14))
        short_atr = float(calculate_atr(kl[-4:], period=3)) if len(kl) >= 4 else atr
        trade_usdt_reference = usdt_for(symbol, self.usdt_map, settings.TRADE_USDT_PER_ORDER)
        t_mode = "atr_risk"
        t_val = 0.0
        if self.context and hasattr(self.context, "get_trade_amount_settings"):
            try:
                t_mode, t_val = self.context.get_trade_amount_settings()
            except Exception:
                t_mode, t_val = "atr_risk", 0.0

        lev_map = parse_leverage_map(settings.SYMBOL_LEVERAGE_MAP)
        symbol_leverage = float(
            leverage_for(symbol, lev_map, settings.DEFAULT_LEVERAGE, settings.MIN_LEVERAGE)
        )
        risk_level_str = (
            self.context.risk_level.lower()
            if self.context and getattr(self.context, "risk_level", None)
            else "low"
        )
        account_risk_pct = {"low": 1.0, "medium": 2.0, "high": 3.0}.get(
            risk_level_str,
            1.0,
        )

        _dd_ctx = self._get_drawdown_context()
        ctx = PolicyContext(
            symbol=symbol,
            signal=action,
            confidence=confidence,
            position=st.position,
            adds=st.adds,
            last_trade_ms=st.last_trade_ms,
            last_stop_ms=int(getattr(st, "last_stop_ms", 0) or 0),
            pending_open=_norm_pending(st.pending_open),
            reentry_confirm_signal=getattr(st, "reentry_confirm_signal", None),
            reentry_confirm_count=getattr(st, "reentry_confirm_count", 0),
            entry_price=price,
            atr=atr,
            baseline_atr=atr,
            short_term_atr=short_atr,
            expected_slippage_pct=self.executor.estimate_slippage(trade_usdt_reference)
            if hasattr(self.executor, "estimate_slippage")
            else 0.0,
            equity=self.get_account_balance(),
            daily_realized_pnl=self.daily.realized_pnl,
            daily_trade_count=self.daily.trade_count,
            open_positions_count=sum(
                1 for state in self.state.values() if state.position in ("LONG", "SHORT")
            ),
            leverage=symbol_leverage,
            stop_loss_pct=float(getattr(settings, "STOP_LOSS_PCT", 0.02)),
            take_profit_pct=float(getattr(settings, "TAKE_PROFIT_PCT", 0.03)),
            cooldown_seconds=getattr(settings, "COOLDOWN_SECONDS", 120),
            sl_cooldown_seconds=getattr(settings, "SL_COOLDOWN_SECONDS", 1800),
            max_adds=getattr(settings, "MAX_ADDS_PER_POSITION", 0),
            trade_mode=getattr(settings, "TRADE_MODE", "normal"),
            min_hold_time_seconds=int(getattr(settings, "MIN_HOLD_TIME_SECONDS", 0)),
            reentry_confirmations=getattr(settings, "REENTRY_CONFIRMATION_COUNT", 1),
            account_risk_pct=account_risk_pct,
            max_daily_loss=self.daily_max_loss,
            max_daily_trades=self.max_trades_daily,
            max_open_positions=self.max_open_positions,
            kill_switch=self.daily.kill,
            execution_mode=self.context.execution_mode if self.context else settings.EXECUTION_MODE,
            trade_amount_mode=t_mode,
            trade_amount_value=t_val,
            now_ms=int(time.time() * 1000),
            broker_id=self.context.broker_account_id if self.context else "BINANCE",
            # Scoped circuit key â€” format "{bot_id}:{broker_account_id}" so the
            # policy engine checks the per-bot circuit, not a shared broker key.
            circuit_key=self._circuit_id if hasattr(self, "_circuit_id") else None,
            # D-2/D-3: pass actual SL/TP prices from symbol state for R:R and ATR checks
            stop_loss_price=float(st.current_stop_loss or 0.0),
            take_profit_price=float(getattr(st, "tp_price", 0.0) or 0.0),
            **_dd_ctx,
        )

        policy = self.policy_engine.evaluate(ctx)
        policy_result = (
            "PASS"
            if policy.allowed
            else f"BLOCKED:{policy.reason_code.name if policy.reason_code else 'UNKNOWN'}:{policy.reason}"
        )
        sizing_result = json.dumps(policy.details or {}, sort_keys=True) if policy.details else None

        if trace_id:
            try:
                recorder.record_gate(
                    trace_id,
                    allowed=bool(policy.allowed),
                    reason_code=policy.reason_code.name if policy.reason_code else "UNKNOWN",
                    reason=policy.reason or "",
                    details={"source": source, "queue_id": queue_id, "confidence": confidence},
                )
            except Exception:
                pass

        if not policy.allowed:
            sizing_codes = {"MIN_NOTIONAL_NOT_MET", "PRICE_INVALID", "SIZE_ZERO", "ATR_INVALID"}
            code_name = policy.reason_code.name if policy.reason_code else "UNKNOWN"
            return _result(
                queue_status="REJECTED",
                final_status="REJECTED_SIZING" if code_name in sizing_codes else "REJECTED_POLICY_RISK",
                final_reason=policy.reason,
                event_filter_result="PASS",
                policy_result=policy_result,
                sizing_result=sizing_result,
                execution_result="NOT_CALLED:POLICY_RISK",
            )

        allowed_action = PolicyAction.OPEN_LONG if action == "BUY" else PolicyAction.OPEN_SHORT
        if policy.action != allowed_action:
            return _result(
                queue_status="REJECTED",
                final_status="REJECTED_DUPLICATE_POSITION",
                final_reason=f"Policy action {policy.action.value} is not allowed for external entry candidates",
                event_filter_result="PASS",
                policy_result=f"BLOCKED:{policy.action.value}",
                sizing_result=sizing_result,
                execution_result="NOT_CALLED:POLICY_ACTION_NOT_ALLOWED",
            )

        trade_usdt = float(policy.risk_usdt or 0.0)
        if trade_usdt <= 0:
            return _result(
                queue_status="REJECTED",
                final_status="REJECTED_SIZING",
                final_reason="Policy sizing produced zero/negative risk_usdt",
                event_filter_result="PASS",
                policy_result=policy_result,
                sizing_result=sizing_result or "risk_usdt<=0",
                execution_result="NOT_CALLED:SIZING",
            )
        tv_trade_cap = float(getattr(settings, "TRADINGVIEW_MAX_TRADE_USDT_CAP", 150.0) or 150.0)
        if source == "TRADINGVIEW" and tv_trade_cap > 0 and trade_usdt > tv_trade_cap:
            cap_details: Dict[str, Any] = {}
            try:
                cap_details = json.loads(sizing_result or "{}") if sizing_result else {}
            except Exception:
                cap_details = {}
            cap_details.update(
                {
                    "tradingview_phase6_trade_cap_usdt": tv_trade_cap,
                    "tradingview_calculated_trade_usdt": trade_usdt,
                    "tradingview_cap_enforced": True,
                }
            )
            return _result(
                queue_status="REJECTED",
                final_status="REJECTED_TV_TRADE_CAP_EXCEEDED",
                final_reason=(
                    f"TradingView limited-mode trade cap exceeded: "
                    f"{trade_usdt:.2f} > {tv_trade_cap:.2f} USDT"
                ),
                event_filter_result="PASS",
                policy_result=policy_result,
                sizing_result=json.dumps(cap_details, sort_keys=True, default=str),
                execution_result="NOT_CALLED:TRADINGVIEW_TRADE_CAP",
            )

        try:
            from app.execution.execution_filter import build_atr_history_from_klines, check_execution

            ticker = self.executor.client.get_ticker(symbol)
            exec_filter = check_execution(
                symbol=symbol,
                current_price=price,
                bid=float(ticker.get("bidPrice", 0) or 0),
                ask=float(ticker.get("askPrice", 0) or 0),
                volume_usdt_15m=float(ticker.get("quoteVolume", 1_000_000) or 1_000_000),
                atr_history=build_atr_history_from_klines(kl) if kl else [],
                data_timestamp_ms=int(ticker.get("closeTime", 0) or 0),
                spread_history=st.spread_history if st.spread_history is not None else [],
            )
            st.spread_history = exec_filter.updated_spread_history
            if not exec_filter.allowed:
                block_reason = exec_filter.block_reason or "Execution filter blocked external candidate"
                return _result(
                    queue_status="REJECTED",
                    final_status="REJECTED_STALE_MARKET_DATA"
                    if "stale" in str(block_reason).lower()
                    else "REJECTED_POLICY_RISK",
                    final_reason=block_reason,
                    event_filter_result="PASS",
                    policy_result=policy_result,
                    sizing_result=sizing_result,
                    execution_result=f"NOT_CALLED:EXEC_FILTER:{block_reason}",
                )
        except Exception as exc:
            logger.warning("[ExtSigRunner] execution filter failed safely for %s: %s", symbol, exc)

        try:
            st.last_action = f"EXTERNAL_{source}_{action}"
            st.last_trade_ms = int(time.time() * 1000)
            exec_result = self.executor.execute_signal(symbol, action, trade_usdt, leverage_mult=1.0)
        except Exception as exc:
            logger.exception("[ExtSigRunner] executor raised for %s queue=%s", symbol, queue_id)
            return _result(
                queue_status="FAILED",
                final_status="FAILED_EXECUTION",
                final_reason=f"Executor raised {type(exc).__name__}: {exc}",
                event_filter_result="PASS",
                policy_result=policy_result,
                sizing_result=sizing_result,
                execution_result=f"EXCEPTION:{type(exc).__name__}:{exc}",
            )

        exec_status = str(getattr(exec_result, "status", "") or "")
        exec_details = getattr(exec_result, "details", {}) or {}
        if exec_status == "STALE_DATA_DETECTED":
            final_status = "REJECTED_STALE_MARKET_DATA"
            queue_status = "REJECTED"
        elif exec_status in {"ALREADY_OPEN", "ENTRY_LOCK_HELD", "ENTRY_INTENT_REUSED"}:
            final_status = "REJECTED_DUPLICATE_POSITION"
            queue_status = "REJECTED"
        elif exec_status in ORDER_OPEN_STATUSES or exec_status == "PAPER_ONLY":
            final_status = "PROCESSED_EXECUTED"
            queue_status = "PROCESSED"
        elif exec_status in {"INSUFFICIENT_MARGIN", "NO_TRADE_INVALID_QTY", "SKIPPED_NOT_LIVE_SYMBOL", "EXPOSURE_LIMIT_EXCEEDED"}:
            final_status = "REJECTED_SIZING" if "QTY" in exec_status else "REJECTED_POLICY_RISK"
            queue_status = "REJECTED"
        else:
            final_status = "FAILED_EXECUTION"
            queue_status = "FAILED"

        if exec_status in ORDER_OPEN_STATUSES:
            try:
                side = "LONG" if action == "BUY" else "SHORT"
                position_id = str(uuid.uuid4())
                normalized = exec_details.get("normalized") or {}
                protection = exec_details.get("protection") or {}
                entry_order = exec_details.get("entry_order") or {}
                fill_price = float(
                    normalized.get("avg_price")
                    or entry_order.get("avgFillPrice")
                    or entry_order.get("avg_fill_price")
                    or getattr(exec_result, "avg_price", 0.0)
                    or price
                )
                fill_qty = float(
                    normalized.get("qty")
                    or entry_order.get("qty_filled")
                    or entry_order.get("executedQty")
                    or entry_order.get("origQty")
                    or policy.quantity
                    or 0.0
                )
                sl_pct = float(getattr(policy, "adjusted_stop_loss_pct", 0.0) or ctx.stop_loss_pct)
                tp_pct = float(getattr(policy, "adjusted_take_profit_pct", 0.0) or ctx.take_profit_pct)
                if side == "LONG":
                    stop_price = fill_price * (1.0 - sl_pct)
                    tp2_price = fill_price * (1.0 + tp_pct)
                    tp1_price = fill_price + ((tp2_price - fill_price) * 0.5)
                    pm_side = PositionSide.LONG
                else:
                    stop_price = fill_price * (1.0 + sl_pct)
                    tp2_price = fill_price * (1.0 - tp_pct)
                    tp1_price = fill_price - ((fill_price - tp2_price) * 0.5)
                    pm_side = PositionSide.SHORT

                sl_order_id = protection.get("sl_order_id")
                tp_order_id = protection.get("tp_order_id")
                if "DUPLICATE_4130" in {str(sl_order_id or ""), str(tp_order_id or "")}:
                    logger.critical(
                        "[ExtSigRunner] %s: executor returned placeholder protection IDs; "
                        "lifecycle will not be marked protected",
                        symbol,
                    )
                else:
                    st.position = side
                    st.entry_price = fill_price
                    st.entry_qty = fill_qty
                    st.position_id = position_id
                    st.current_stop_loss = stop_price
                    # F-3: persist TP2 price for D-2 R:R gate; F-12: persist originals for PM restore
                    st.tp_price = float(tp2_price) if tp2_price else 0.0
                    st.original_sl_price = float(stop_price) if stop_price else 0.0
                    st.original_tp1_price = float(tp1_price) if tp1_price else 0.0
                    st.original_tp2_price = float(tp2_price) if tp2_price else 0.0
                    self.position_manager.open_position(
                        symbol=symbol,
                        side=pm_side,
                        position_id=position_id,
                        entry_price=fill_price,
                        qty=fill_qty,
                        stop_price=stop_price,
                        tp1_price=tp1_price,
                        tp2_price=tp2_price,
                        mode="PRECISION",
                        strategy_name=f"external_{source.lower()}",
                        sl_order_id=str(sl_order_id) if sl_order_id else None,
                        tp_order_id=str(tp_order_id) if tp_order_id else None,
                    )
                    try:
                        record_fill(
                            self.db,
                            symbol=symbol,
                            side=side,
                            action="OPEN",
                            qty=fill_qty,
                            price=fill_price,
                            fee=0.0,
                            realized_pnl=0.0,
                            order_id=str(getattr(exec_result, "order_id", None) or ""),
                            strategy=f"external_{source.lower()}",
                            market_type=self.context.market_type if self.context else "UNKNOWN",
                            execution_mode=self._effective_execution_mode(),
                            timeframe=str(getattr(self, "interval", "") or self.interval),
                            confidence=confidence,
                            bot_instance_id=self.context.bot_instance_id if self.context else None,
                            user_id=self.context.user_id if self.context else None,
                            broker_account_id=self.context.broker_account_id if self.context else None,
                            position_id=position_id,
                            stop_loss_price=stop_price,
                            trace_id=trace_id,
                            initiator_type="BOT",
                            trigger_source=f"{source}_EXTERNAL_SIGNAL",
                            position_phase="SEEKING_TP1",
                            run_id=self.run_id,
                            cycle_id=self.cycle_id or str(uuid.uuid4()),
                        )
                    except Exception as fill_exc:
                        logger.warning("[ExtSigRunner] %s: OPEN fill record failed: %s", symbol, fill_exc)
                    try:
                        self.store.save_symbol(symbol, st)
                    except Exception:
                        pass
            except Exception as lifecycle_exc:
                logger.exception(
                    "[ExtSigRunner] %s: post-execution lifecycle persistence failed",
                    symbol,
                )
                final_status = "FAILED_EXECUTION"
                queue_status = "FAILED"
                exec_details = {
                    **exec_details,
                    "lifecycle_error": str(lifecycle_exc),
                }

        if trace_id:
            try:
                recorder.record_intent(
                    trace_id,
                    action=f"EXTERNAL_{source}_{action}",
                    sizing={
                        "trade_usdt": trade_usdt,
                        "source": source,
                        "queue_id": queue_id,
                        "policy_details": policy.details or {},
                    },
                    sl_plan=float(getattr(policy, "sl_plan", None))
                    if getattr(policy, "sl_plan", None) is not None
                    else None,
                    tp_plan=float(getattr(policy, "tp_plan", None))
                    if getattr(policy, "tp_plan", None) is not None
                    else None,
                )
                recorder.finalize(
                    trace_id,
                    state_change=f"EXTERNAL_SIGNAL_{final_status}",
                    final_position=st.position if st.position in ("LONG", "SHORT") else "NONE",
                )
            except Exception:
                pass

        return _result(
            queue_status=queue_status,
            final_status=final_status,
            final_reason=str(getattr(exec_result, "error", None) or exec_status),
            event_filter_result="PASS",
            policy_result=policy_result,
            sizing_result=sizing_result,
            execution_result=f"{exec_status}:{json.dumps(exec_details, sort_keys=True, default=str)}",
            decision_trace_id=trace_id,
        )

    def _step_symbol_orchestrated(
        self,
        symbol: str,
        klines: Any,
        trace_id: str,
        *,
        market_snapshot: Any = None,
        evaluate_entry: bool = True,
    ) -> Dict[str, Any]:
        """
        Orchestrator-driven processing. Replacement for legacy logic.
        """
        # [EVAL] Logging Variables
        eval_strat = getattr(self.orchestrator, "strategy_id", "unknown")
        eval_sig = "None"
        eval_conf_raw = "None"
        eval_conf_norm = "None"
        eval_thr = "None"
        eval_decision = "SKIP"
        eval_reason = "initializing"
        
        try:
            st = self.state[symbol]
            
            # 1. Sync State (Simplified)
            # â”€â”€ Fix 1+2: Capture pre-sync state for exchange-driven close detection â”€â”€
            _pre_sync_position   = st.position
            _pre_sync_entry_price = st.entry_price
            _pre_sync_entry_qty   = st.entry_qty
            _pre_sync_position_id = st.position_id

            try:
                # Simulated positions are internal truth. A demo exchange is expected
                # to be flat while a paper position exists, so never reconcile paper
                # state from the exchange position endpoint.
                pos_info = None
                if self._effective_execution_mode() == "broker":
                    pos_info = self.executor.client.get_position_info(symbol)
                if pos_info:
                    pos_amt = float(pos_info.get("positionAmt", "0"))
                    if abs(pos_amt) > 1e-12:
                        st.position = "LONG" if pos_amt > 0 else "SHORT"
                        st.entry_qty = abs(pos_amt)
                        st.entry_price = float(pos_info.get("entryPrice", "0"))
                        self._position_flush_counts[symbol] = 0 # âœ… Reset on valid read
                    else:
                        # âœ… Only set NONE if we see 3 consecutive zero reads (State-Sync Hardening).
                        # Reducing to 1 risks teardown on transient API zeros; 3 is intentional.
                        self._position_flush_counts[symbol] += 1
                        _flush_n = self._position_flush_counts[symbol]
                        if _flush_n < 3:
                            logger.debug(
                                "[SYNC] %s: exchange reports flat (%d/3) â€” holding %s until confirmed",
                                symbol, _flush_n, st.position,
                            )
                        if _flush_n >= 3:
                            # F-15: verify flatness once more before declaring the position closed.
                            # Three consecutive empty reads could be transient API failures, not a
                            # real close. If the re-check shows a position still exists, abort flush.
                            _flush_confirmed = True
                            try:
                                _verify_info = self.executor.client.get_position_info(symbol)
                                if _verify_info and abs(float(_verify_info.get("positionAmt", "0"))) > 1e-12:
                                    logger.warning(
                                        "[FLUSH] %s: false flatten prevented â€” exchange still shows "
                                        "position after 3 empty reads. Resetting flush counter.",
                                        symbol,
                                    )
                                    self._position_flush_counts[symbol] = 0
                                    _flush_confirmed = False
                            except Exception as _flush_verify_err:
                                logger.warning(
                                    "[FLUSH] %s: verification check failed (%s). Proceeding with flush "
                                    "as exchange is unreachable.", symbol, _flush_verify_err,
                                )
                            if _flush_confirmed:
                                st.position = "NONE"
                                st.entry_qty = 0.0
                                self._position_flush_counts[symbol] = 0 # âœ… Reset after flush

            except Exception:
                pass # Use cached state if sync fails

            if self._effective_execution_mode() == "broker":
                try:
                    self._reconcile_entry_protection(symbol, float(pos_amt if 'pos_amt' in locals() else 0.0), st)
                except Exception:
                    pass

            # â”€â”€ Fix 1+2: Exchange-driven close accounting â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # If pre-sync tracked a position but exchange now reports NONE, the
            # position was closed externally (TP/SL order hit).  Record the CLOSE
            # fill now so realized_pnl and position_id linkage are preserved.
            #
            # Guard: skip when _pre_sync_position_id is None.  That means the normal
            # close path already ran this lifecycle (it clears position_id via
            # st.position_id = None and saves state).  Without this guard, the next
            # cycle's exchange sync detects position LONG/SHORTâ†’NONE and records a
            # duplicate CLOSE fill (no position_id, double-counted PnL).
            if _pre_sync_position in ("LONG", "SHORT") and st.position == "NONE" and _pre_sync_position_id is not None:
                try:
                    from shared_lib.persistence.trade_fills import record_fill as _rf_ec, ExitReason as _ER_ec
                    from app.core.config import settings as _s_ec

                    # â”€â”€ P2: Dedup guard â€” skip fill recording if already recorded â”€â”€â”€â”€â”€â”€
                    _ec_already_recorded = False
                    try:
                        _bot_id = self.context.bot_instance_id if self.context else "default"
                        with self.db.connect() as _ec_conn:
                            _ec_dup = _ec_conn.execute(
                                """
                                SELECT 1
                                FROM trade_fills
                                WHERE position_id=?
                                  AND action='CLOSE'
                                  AND (bot_instance_id = ? OR (bot_instance_id IS NULL AND ? = 'default'))
                                LIMIT 1
                                """,
                                (_pre_sync_position_id, _bot_id, _bot_id)
                            ).fetchone()
                            _ec_already_recorded = _ec_dup is not None
                    except Exception:
                        pass  # dedup check failure â†’ proceed with recording (safer than silently skipping)

                    _ec_last_price  = float(self.client.last_price(symbol))  # always fetched as baseline
                    _ec_price       = _ec_last_price                          # may be overridden below
                    _ec_ep          = float(_pre_sync_entry_price or 0.0)
                    _ec_qty         = float(_pre_sync_entry_qty or 0.0)

                    # Belt-and-suspenders: recover entry price from OPEN fill when
                    # pre_sync captured 0 (e.g. after a restart between flush cycles).
                    if _ec_ep == 0.0 and _pre_sync_position_id:
                        try:
                            with self.db.connect() as _ep_conn:
                                _ep_row = _ep_conn.execute(
                                    """
                                    SELECT price, qty
                                    FROM trade_fills
                                    WHERE position_id=?
                                      AND action='OPEN'
                                      AND (bot_instance_id = ? OR (bot_instance_id IS NULL AND ? = 'default'))
                                    LIMIT 1
                                    """,
                                    (_pre_sync_position_id, _bot_id, _bot_id)
                                ).fetchone()
                                if _ep_row:
                                    _db_ep = float(_ep_row[0] or 0)
                                    if _db_ep > 0:
                                        _ec_ep = _db_ep
                                    if _ec_qty == 0.0:
                                        _db_qty = float(_ep_row[1] or 0)
                                        if _db_qty > 0:
                                            _ec_qty = _db_qty
                        except Exception:
                            pass

                    _ec_pnl         = None
                    _ec_exit_reason = _ER_ec.OTHER
                    _ec_sl_at_exit  = None
                    _ec_tp_at_exit  = None
                    _ec_price_source = "last"

                    if not _ec_already_recorded:
                        # â”€â”€ P1a: Capture PM state BEFORE close_position() clears it â”€â”€
                        _ec_pm_pos = self.position_manager.get_position(symbol)
                        _ec_pm_sl_price    = None
                        _ec_pm_initial_stop = None
                        _ec_pm_tp1_price   = None
                        _ec_pm_tp2_price   = None
                        if _ec_pm_pos:
                            try:
                                _ec_pm_sl_price     = float(_ec_pm_pos.sl.current_stop)
                                _ec_pm_initial_stop = float(_ec_pm_pos.sl.initial_stop)
                                _ec_sl_at_exit      = _ec_pm_sl_price
                            except Exception:
                                pass
                            try:
                                _ec_pm_tp1_price = float(_ec_pm_pos.tp.tp1_price)
                                _ec_pm_tp2_price = float(_ec_pm_pos.tp.tp2_price)
                                _ec_tp_at_exit   = _ec_pm_tp2_price
                            except Exception:
                                pass

                        # Fallback to position_lifecycle_state when PM has no position
                        # (restart between flush cycles, or PM already cleared this symbol).
                        if _ec_pm_sl_price is None:
                            try:
                                with self.db.connect() as _lvl_conn:
                                    _pls = _lvl_conn.execute(
                                        "SELECT original_stop, original_tp1, original_tp2"
                                        " FROM position_lifecycle_state"
                                        " WHERE symbol=? ORDER BY updated_at DESC LIMIT 1",
                                        (symbol,)
                                    ).fetchone()
                                    if _pls:
                                        if _pls[0] and float(_pls[0] or 0) > 0:
                                            _ec_pm_sl_price     = float(_pls[0])
                                            _ec_pm_initial_stop = float(_pls[0])
                                            _ec_sl_at_exit      = _ec_pm_sl_price
                                        if _pls[1] and float(_pls[1] or 0) > 0:
                                            _ec_pm_tp1_price = float(_pls[1])
                                        if _pls[2] and float(_pls[2] or 0) > 0:
                                            _ec_pm_tp2_price = float(_pls[2])
                                            _ec_tp_at_exit   = _ec_pm_tp2_price
                            except Exception:
                                pass

                        # â”€â”€ P1b: Fetch actual broker fill price (last 5 min) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                        try:
                            _now_ms = int(time.time() * 1000)
                            _ec_trades = self.client.user_trades(
                                symbol, start_time_ms=_now_ms - 300_000
                            )
                            if _ec_trades:
                                _close_side_filter = "SELL" if _pre_sync_position == "LONG" else "BUY"
                                _candidates = [
                                    t for t in _ec_trades
                                    if str(t.get("side", "")).upper() == _close_side_filter
                                ]
                                if _candidates:
                                    _fill_p = float(_candidates[-1].get("price", 0) or 0)
                                    if _fill_p > 0:
                                        _ec_price = _fill_p
                                        _ec_price_source = "broker_user_trades"
                        except Exception:
                            pass  # keep last_price fallback

                        # â”€â”€ P1c: Infer exit_reason from price vs PM levels â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                        if _ec_pm_sl_price is not None and _ec_pm_tp1_price is not None and _ec_ep > 0:
                            _tol = abs(_ec_ep - _ec_pm_sl_price) * 0.15  # 15% of initial risk distance
                            if _pre_sync_position == "LONG":
                                if _ec_price <= _ec_pm_sl_price + _tol:
                                    _ec_exit_reason = _ER_ec.SL
                                elif _ec_pm_tp2_price and _ec_price >= _ec_pm_tp2_price - _tol:
                                    _ec_exit_reason = _ER_ec.TP2
                                elif _ec_price >= _ec_pm_tp1_price - _tol:
                                    _ec_exit_reason = _ER_ec.TP1
                            else:  # SHORT
                                if _ec_price >= _ec_pm_sl_price - _tol:
                                    _ec_exit_reason = _ER_ec.SL
                                elif _ec_pm_tp2_price and _ec_price <= _ec_pm_tp2_price + _tol:
                                    _ec_exit_reason = _ER_ec.TP2
                                elif _ec_price <= _ec_pm_tp1_price + _tol:
                                    _ec_exit_reason = _ER_ec.TP1

                        if _ec_ep > 0 and _ec_qty > 0:
                            _ec_pnl = (
                                (_ec_price - _ec_ep) * _ec_qty
                                if _pre_sync_position == "LONG"
                                else (_ec_ep - _ec_price) * _ec_qty
                            )

                        # â”€â”€ P3: Analytics integrity fields â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                        _ec_risk_dist = abs(_ec_ep - _ec_pm_initial_stop) if _ec_pm_initial_stop and _ec_ep > 0 else 0.0
                        _ec_r_multiple = None
                        if _ec_risk_dist > 0:
                            _ec_r_multiple = (
                                (_ec_price - _ec_ep) / _ec_risk_dist
                                if _pre_sync_position == "LONG"
                                else (_ec_ep - _ec_price) / _ec_risk_dist
                            )

                        _ec_run_id = self.run_id
                        _ec_cycle_id = self.cycle_id
                        if not _ec_run_id or not _ec_cycle_id:
                            logger.error(
                                "[FILL LINKAGE ERROR] Missing run_id/cycle_id for symbol=%s path=exchange_close",
                                symbol,
                            )
                        _rf_ec(
                            self.db,
                            symbol=symbol,
                            side=_pre_sync_position,
                            action="CLOSE",
                            qty=_ec_qty,
                            price=_ec_price,
                            realized_pnl=_ec_pnl,
                            position_id=_pre_sync_position_id,
                            exit_reason=_ec_exit_reason,
                            strategy="orchestrated",
                            broker_id=getattr(_s_ec, "BROKER_ID", "binance_futures"),
                            account_id=getattr(_s_ec, "ACCOUNT_ID", "default"),
                            bot_instance_id=getattr(self, "run_id", None),
                            timeframe=getattr(_s_ec, "DEFAULT_INTERVAL", "15m"),
                            initiator_type="EXCHANGE",
                            trigger_source="EXCHANGE_SL_TP_ORDER",
                            sl_at_exit=_ec_sl_at_exit,
                            tp_at_exit=_ec_tp_at_exit,
                            stop_loss_price=_ec_pm_initial_stop,
                            r_multiple=_ec_r_multiple,
                            exit_regime=st.last_regime,
                            exit_regime_confidence=float(st.last_regime_confidence) if st.last_regime_confidence is not None else None,
                            market_price_used=_ec_last_price,
                            price_source=_ec_price_source,
                            sync_state_before=_pre_sync_position,
                            sync_state_after="NONE",
                            run_id=_ec_run_id,
                            cycle_id=_ec_cycle_id,
                        )

                        # === F-1: DAILY STATE UPDATE (exchange-driven close) ===
                        # This block was missing â€” the kill switch, D-1 consecutive loss gate,
                        # and adaptive engine all depend on realized_pnl being updated here.
                        try:
                            _ec_close_pnl = float(_ec_pnl or 0.0)
                            _ec_bot_id = self.context.bot_instance_id if self.context else "default"

                            self.daily.record_trade_result(
                                is_win=_ec_close_pnl > 0,
                                soft_limit=getattr(settings, "MAX_CONSECUTIVE_LOSSES_SOFT", 3),
                                cooldown_minutes=getattr(settings, "CONSECUTIVE_LOSS_COOLDOWN_MINUTES", 120),
                                hard_limit=getattr(settings, "MAX_CONSECUTIVE_LOSSES_HARD", 5),
                            )
                            self.daily.add_pnl(
                                _ec_close_pnl,
                                max_loss=getattr(settings, "DAILY_MAX_LOSS_USDT", 50.0),
                            )
                            _guard_on_trade_closed(_ec_close_pnl, _ec_bot_id)
                            self.store.save_daily(
                                self.daily.day,
                                self.daily.realized_pnl,
                                self.daily.kill,
                                trade_count=self.daily.trade_count,
                                consecutive_losses=self.daily.consecutive_losses,
                                consec_loss_cooldown_until_ms=self.daily.consec_loss_cooldown_until_ms,
                            )
                            logger.info(
                                "[DAILY_STATE] Bot %s | symbol=%s | pnl=%.4f | "
                                "daily_pnl=%.4f | consecutive_losses=%d | kill=%s",
                                _ec_bot_id, symbol, _ec_close_pnl,
                                self.daily.realized_pnl, self.daily.consecutive_losses,
                                self.daily.kill,
                            )
                        except Exception as _ec_daily_err:
                            logger.error(
                                "[DAILY_STATE] Failed to update daily state for exchange close "
                                "%s: %s", symbol, _ec_daily_err,
                            )
                        # === END F-1 ===

                    st.position_id = None
                    # â”€â”€ Audit: Record the sync event explicitly â”€â”€
                    try:
                        self.audit.event(
                            event_type="EXCHANGE_CLOSE",
                            run_id=getattr(self, "run_id", None),
                            symbol=symbol,
                            action="RESET_TO_FLAT",
                            details={
                                "pre_sync_pos": _pre_sync_position,
                                "exit_reason": _ec_exit_reason,
                                "price": _ec_price,
                                "pnl": _ec_pnl,
                                "pos_id": _pre_sync_position_id,
                                "dedup_skipped": _ec_already_recorded,
                            }
                        )
                    except Exception:
                        pass
                    # â”€â”€ Persist flat state immediately so bot_symbol_state
                    #    reflects NONE on next restart (no stale open) â”€â”€
                    try:
                        self.store.save_symbol(symbol, st)
                    except Exception as _save_ec_err:
                        logger.warning(
                            "[EXCHANGE_CLOSE] %s: failed to persist flat state: %s",
                            symbol, _save_ec_err,
                        )
                    # Clean up PM lifecycle if it still tracks this position
                    if self.position_manager.get_position(symbol):
                        self.position_manager.close_position(symbol, "EXCHANGE_DRIVEN_CLOSE")
                    if not _ec_already_recorded:
                        logger.info(
                            "[EXCHANGE_CLOSE] %s: %s closed by exchange â€” exit_reason=%s "
                            "entry=%.6f close=%.6f pnl~=%.4f pos_id=%s (price_source=%s)",
                            symbol, _pre_sync_position, _ec_exit_reason,
                            _ec_ep, _ec_price, _ec_pnl or 0.0, _pre_sync_position_id,
                            _ec_price_source,
                        )
                    else:
                        logger.info(
                            "[EXCHANGE_CLOSE] %s: %s â€” dedup: fill already recorded for pos_id=%s, skipped",
                            symbol, _pre_sync_position, _pre_sync_position_id,
                        )

                except Exception as _ec_err:
                    logger.warning(
                        "[EXCHANGE_CLOSE] %s: failed to record exchange-driven close: %s",
                        symbol, _ec_err,
                    )
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

            price = float(self.client.last_price(symbol))
            
            # 2. Check Exits (PositionManager lifecycle)
            if st.position in ("LONG", "SHORT"):
                 action = None  # default; set by update_price if pos exists
                 pos = self.position_manager.get_position(symbol)
                 if pos:
                     action = self.position_manager.update_price(
                         symbol=symbol,
                         current_price=price,
                         current_atr=float(calculate_atr(klines, period=14)) if klines and len(klines) >= 14 else price * 0.02
                     )
                     reason = str(action) if action else ""
                     
                     if action in ("HIT_STOP", "HIT_TP2", "TIME_EXIT"):
                         eval_decision = "CLOSE"
                         eval_reason = reason
                         # Snapshot position state before close clears it
                         _pm_close_side    = st.position
                         _pm_close_ep      = float(pos.entry_price) if pos and pos.entry_price else float(st.entry_price or 0.0)
                         _pm_close_qty     = float(pos.current_qty)  if pos and pos.current_qty  else float(st.entry_qty or 0.0)
                         _pm_close_pos_id  = st.position_id
                         res = self.executor.execute_signal(
                             symbol, "CLOSE", 0.0,
                             position_side=_pm_close_side,
                             remaining_quantity=_pm_close_qty,
                             fallback_price=price,
                         )
                         if not getattr(res, "success", False):
                             raise RuntimeError(getattr(res, "error", None) or "CLOSE_EXECUTION_FAILED")
                         self.position_manager.close_position(symbol, reason)
                         # â”€â”€ Fix 1+2: Record CLOSE fill with position_id linkage â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                         try:
                             from shared_lib.persistence.trade_fills import record_fill as _rf_pm, ExitReason as _ER
                             from app.core.config import settings as _s_pm
                             _pm_close_price = float(res.avg_price or price)
                             _pm_pnl = None
                             if _pm_close_ep > 0 and _pm_close_qty > 0:
                                 _pm_pnl = (
                                     (_pm_close_price - _pm_close_ep) * _pm_close_qty
                                     if _pm_close_side == "LONG"
                                     else (_pm_close_ep - _pm_close_price) * _pm_close_qty
                                 )
                             _exit_reason_map = {
                                 # FIX-D: HIT_STOP no longer maps blindly to SL.
                                 # Actual reason is resolved below using pos state.
                                 "HIT_TP2":  _ER.TP2,
                                 "TIME_EXIT": _ER.TIME_EXIT,
                             }
                             # FIX-D: distinguish post-TP1 buffered stop from plain SL
                             if action == "HIT_STOP":
                                 if pos and pos.sl.trailing_last_stop_price is not None:
                                     _exit_reason_map["HIT_STOP"] = _ER.TRAILING_SL
                                 elif pos and pos.sl.is_break_even and pos.sl.be_buffer_amount:
                                     _exit_reason_map["HIT_STOP"] = _ER.BREAK_EVEN_BUFFER
                                 elif pos and pos.sl.is_break_even:
                                     _exit_reason_map["HIT_STOP"] = _ER.BREAK_EVEN
                                 else:
                                     _exit_reason_map["HIT_STOP"] = _ER.SL
                             # â”€â”€ P3: Analytics integrity fields â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                             _pm_initial_stop = float(pos.sl.initial_stop) if pos and hasattr(pos, "sl") else None
                             _pm_risk_dist = abs(_pm_close_ep - _pm_initial_stop) if _pm_initial_stop and _pm_close_ep > 0 else 0.0
                             _pm_r_multiple = None
                             if _pm_risk_dist > 0:
                                 _pm_r_multiple = (
                                     (_pm_close_price - _pm_close_ep) / _pm_risk_dist
                                     if _pm_close_side == "LONG"
                                     else (_pm_close_ep - _pm_close_price) / _pm_risk_dist
                                 )
                             _pm_run_id = self.run_id
                             _pm_cycle_id = self.cycle_id
                             if not _pm_run_id or not _pm_cycle_id:
                                 logger.error(
                                     "[FILL LINKAGE ERROR] Missing run_id/cycle_id for symbol=%s path=pm_close",
                                     symbol,
                                 )
                             _rf_pm(
                                 self.db,
                                 symbol=symbol,
                                 side=_pm_close_side,
                                 action="CLOSE",
                                 qty=_pm_close_qty,
                                 price=_pm_close_price,
                                 realized_pnl=_pm_pnl,
                                 position_id=_pm_close_pos_id,
                                 exit_reason=_exit_reason_map.get(action, _ER.OTHER),
                                 order_id=getattr(res, "order_id", None),
                                 strategy="orchestrated",
                                 broker_id=getattr(_s_pm, "BROKER_ID", "binance_futures"),
                                 account_id=getattr(_s_pm, "ACCOUNT_ID", "default"),
                                 bot_instance_id=self.context.bot_instance_id if self.context else None,
                                 user_id=self.context.user_id if self.context else None,
                                 broker_account_id=self.context.broker_account_id if self.context else None,
                                 execution_mode=self._effective_execution_mode(),
                                 timeframe=getattr(_s_pm, "DEFAULT_INTERVAL", "15m"),
                                 initiator_type="BOT",
                                 trigger_source=f"LIFECYCLE_{action}",
                                 position_phase=str(getattr(pos, "phase", "EXITING")),
                                 sl_at_exit=float(pos.sl.current_stop) if pos and hasattr(pos, "sl") else None,
                                 tp_at_exit=float(pos.tp.tp2_price) if pos and hasattr(pos, "tp") else None,
                                 stop_loss_price=_pm_initial_stop,
                                 r_multiple=_pm_r_multiple,
                                 exit_regime=st.last_regime,
                                 exit_regime_confidence=float(st.last_regime_confidence) if st.last_regime_confidence is not None else None,
                                 market_price_used=price,
                                 price_source="last",
                                 broker_response=json.dumps(res.details) if hasattr(res, "details") else None,
                                 run_id=_pm_run_id,
                                 cycle_id=_pm_cycle_id,
                             )
                             st.position_id = None

                             # === F-1: DAILY STATE UPDATE (PM-driven close) ===
                             try:
                                 _pm_close_pnl = float(_pm_pnl or 0.0)
                                 _pm_bot_id = self.context.bot_instance_id if self.context else "default"

                                 self.daily.record_trade_result(
                                     is_win=_pm_close_pnl > 0,
                                     soft_limit=getattr(_s_pm, "MAX_CONSECUTIVE_LOSSES_SOFT", 3),
                                     cooldown_minutes=getattr(_s_pm, "CONSECUTIVE_LOSS_COOLDOWN_MINUTES", 120),
                                     hard_limit=getattr(_s_pm, "MAX_CONSECUTIVE_LOSSES_HARD", 5),
                                 )
                                 self.daily.add_pnl(
                                     _pm_close_pnl,
                                     max_loss=getattr(_s_pm, "DAILY_MAX_LOSS_USDT", 50.0),
                                 )
                                 _guard_on_trade_closed(_pm_close_pnl, _pm_bot_id)
                                 self.store.save_daily(
                                     self.daily.day,
                                     self.daily.realized_pnl,
                                     self.daily.kill,
                                     trade_count=self.daily.trade_count,
                                     consecutive_losses=self.daily.consecutive_losses,
                                     consec_loss_cooldown_until_ms=self.daily.consec_loss_cooldown_until_ms,
                                 )
                                 logger.info(
                                     "[DAILY_STATE] Bot %s | symbol=%s | pnl=%.4f | "
                                     "daily_pnl=%.4f | consecutive_losses=%d | kill=%s",
                                     _pm_bot_id, symbol, _pm_close_pnl,
                                     self.daily.realized_pnl, self.daily.consecutive_losses,
                                     self.daily.kill,
                                 )
                             except Exception as _pm_daily_err:
                                 logger.error(
                                     "[DAILY_STATE] Failed to update daily state for PM close "
                                     "%s: %s", symbol, _pm_daily_err,
                                 )
                             # === END F-1 ===

                             # â”€â”€ Audit: Record the bot exit event explicitly â”€â”€
                             try:
                                 self.audit.event(
                                     event_type="LIFECYCLE_EXIT",
                                     run_id=getattr(self, "run_id", None),
                                     symbol=symbol,
                                     action=action,
                                     details={
                                         "price": _pm_close_price,
                                         "pnl": _pm_pnl,
                                         "pos_id": _pm_close_pos_id
                                     }
                                 )
                             except Exception:
                                 pass
                             logger.info(
                                 "[PM_CLOSE] %s: %s %s â€” entry=%.6f close=%.6f pnl=%.4f pos_id=%s",
                                 symbol, action, _pm_close_side, _pm_close_ep,
                                 _pm_close_price, _pm_pnl or 0.0, _pm_close_pos_id,
                             )

                         except Exception as _pm_fill_err:
                             logger.warning("[PM_CLOSE] %s: failed to record close fill: %s", symbol, _pm_fill_err)
                         # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                         return {"symbol": symbol, "decision": f"CLOSE_{reason}", "details": res.details}
                     elif action == "HIT_TP1":
                          eval_decision = "PARTIAL_CLOSE"
                          eval_reason = "HIT_TP1"
                          import logging as _tp1_log
                          _tp1_log = _tp1_log.getLogger(__name__)
                          pos_state = self.position_manager.get_position(symbol)
                          if pos_state is not None:
                              try:
                                  tp1_result = self.executor.execute_tp1_partial_close(
                                      symbol=symbol,
                                      live_qty=abs(float(st.entry_qty or 0.0)),
                                      position_side=st.position,
                                      sl_price=float(pos_state.sl.current_stop),
                                      tp_price=float(pos_state.tp.tp2_price),
                                      sl_order_id=pos_state.sl.sl_order_id,
                                      tp_order_id=pos_state.sl.tp_order_id,
                                      tp1_fraction=float(pos_state.tp.tp1_close_fraction),
                                      position_manager=self.position_manager,
                                  )
                                  if tp1_result.get("promoted"):
                                      # Full close was promoted â€” clear lifecycle state
                                      self.position_manager.close_position(symbol, "TP1_PROMOTED_FULL_CLOSE")
                                      return {"symbol": symbol, "decision": "CLOSE_TP1_PROMOTED", "details": tp1_result}
                                  if tp1_result.get("skipped"):
                                      _tp1_log.info(f"{symbol} HIT_TP1 skipped (duplicate): {tp1_result.get('failure_reason')}")
                                  else:
                                      self._persist_tp1_outcome(
                                          symbol=symbol,
                                          st=st,
                                          tp1_result=tp1_result,
                                          fallback_price=price,
                                          trace_id=trace_id,
                                      )
                              except Exception as _tp1_err:
                                  _tp1_log.error(f"{symbol} HIT_TP1 execute_tp1_partial_close error: {_tp1_err}", exc_info=True)
                          else:
                              _tp1_log.warning(f"{symbol} HIT_TP1 fired but no PM position state found â€” skipping partial close")

                 # â”€â”€ Break-even trigger (Step 3D) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                 # Fires on the same cycle as TP1 is confirmed and again every heartbeat
                 # until be_exchange_confirmed=True.  execute_break_even_update() is
                 # idempotent: it skips if already confirmed, wrong phase, or if the
                 # proposed BE price would loosen the existing stop.
                 _be_pos = self.position_manager.get_position(symbol)
                 if (
                     _be_pos is not None
                     and _be_pos.tp.tp1_hit
                     and not _be_pos.sl.be_exchange_confirmed
                     and _be_pos.phase in (
                         PositionPhase.TP1_FILLED,
                         PositionPhase.RUNNER_TRAILING,
                     )
                 ):
                     try:
                         _be_result = self.executor.execute_break_even_update(
                             symbol=symbol,
                             position_side=st.position,
                             runner_qty=float(_be_pos.current_qty),
                             entry_price=float(_be_pos.entry_price),
                             current_stop=float(_be_pos.sl.current_stop),
                             sl_order_id=_be_pos.sl.sl_order_id,
                             tp_order_id=_be_pos.sl.tp_order_id,
                             tp2_price=float(_be_pos.tp.tp2_price) if _be_pos.tp.tp2_price else None,
                             position_manager=self.position_manager,
                         )
                         if _be_result.get('break_even_applied'):
                             logger.info(
                                 '[BE_TRIGGER] %s: Break-even applied norm_be=%s prior_stop=%s runner_qty=%s',
                                 symbol,
                                 _be_result.get('normalized_break_even_price'),
                                 _be_result.get('prior_stop_price'),
                                 _be_result.get('live_qty'),
                             )
                         elif _be_result.get('skip_reason'):
                             logger.debug(
                                 '[BE_TRIGGER] %s: BE skipped: %s', symbol, _be_result['skip_reason']
                             )
                         elif _be_result.get('failure_reason'):
                             logger.error(
                                 '[BE_TRIGGER] %s: BE FAILED: %s prot_status=%s',
                                 symbol,
                                 _be_result['failure_reason'],
                                 _be_result.get('protection_update_status'),
                             )
                     except Exception as _be_err:
                         logger.error(
                             '[BE_TRIGGER] %s: execute_break_even_update raised: %s',
                             symbol, _be_err, exc_info=True,
                         )

                 # â”€â”€ Trailing stop trigger (Step 3E, Refinement 1) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                 # Primary gate: TRAIL_UPDATED means the internal anchor (highest/
                 # lowest_since_entry) moved this cycle, so the proposed trailing stop
                 # may now be tighter than the live exchange stop.
                 # Secondary gate: if RUNNER_TRAILING + be_confirmed but
                 # trailing_last_stop_price diverges from current_stop (e.g. bot
                 # restarted after a successful trailing update that was never
                 # reflected back into PM), also fire to reconcile.
                 _trail_pos = self.position_manager.get_position(symbol)
                 _trail_anchor_moved = (action == "TRAIL_UPDATED")
                 _trail_desync = (
                     _trail_pos is not None
                     and _trail_pos.sl.trailing_last_stop_price is not None
                     and abs(
                         _trail_pos.sl.trailing_last_stop_price
                         - _trail_pos.sl.current_stop
                     ) > 0.001
                 )
                 if (
                     _trail_pos is not None
                     and _trail_pos.sl.be_exchange_confirmed
                     and _trail_pos.phase == PositionPhase.RUNNER_TRAILING
                     and (_trail_anchor_moved or _trail_desync)
                 ):
                     # Use the current cycle's ATR (already computed above for update_price)
                     try:
                         _trail_atr = float(calculate_atr(klines, period=14)) \
                             if klines and len(klines) >= 14 else None
                     except Exception:
                         _trail_atr = None

                     if _trail_atr and _trail_atr > 0:
                         try:
                             _trail_result = self.executor.execute_trailing_stop_update(
                                 symbol=symbol,
                                 position_side=st.position,
                                 runner_qty=float(_trail_pos.current_qty),
                                 entry_price=float(_trail_pos.entry_price),
                                 current_stop=float(_trail_pos.sl.current_stop),
                                 highest_since_entry=float(_trail_pos.highest_since_entry),
                                 lowest_since_entry=float(_trail_pos.lowest_since_entry),
                                 atr=_trail_atr,
                                 sl_order_id=_trail_pos.sl.sl_order_id,
                                 tp_order_id=_trail_pos.sl.tp_order_id,
                                 tp2_price=float(_trail_pos.tp.tp2_price) if _trail_pos.tp.tp2_price else None,
                                 position_manager=self.position_manager,
                                 be_floor_price=_trail_pos.sl.break_even_price,
                                 last_update_ts=_trail_pos.sl.trailing_last_update_ts,
                                 last_trailing_stop=_trail_pos.sl.trailing_last_stop_price,
                             )
                             if _trail_result.get('trailing_applied'):
                                 logger.info(
                                     '[TRAIL_TRIGGER] %s: Trailing stop applied '
                                     'norm=%s prior=%s atr=%.4f qty=%s',
                                     symbol,
                                     _trail_result.get('normalized_trailing_stop'),
                                     _trail_result.get('prior_stop_price'),
                                     _trail_atr,
                                     _trail_result.get('live_qty'),
                                 )
                             elif _trail_result.get('skip_reason'):
                                 logger.debug(
                                     '[TRAIL_TRIGGER] %s: skip: %s',
                                     symbol, _trail_result['skip_reason'],
                                 )
                             elif _trail_result.get('failure_reason'):
                                 logger.error(
                                     '[TRAIL_TRIGGER] %s: FAILED: %s prot_status=%s',
                                     symbol,
                                     _trail_result['failure_reason'],
                                     _trail_result.get('protection_update_status'),
                                 )
                         except Exception as _trail_err:
                             logger.error(
                                 '[TRAIL_TRIGGER] %s: execute_trailing_stop_update raised: %s',
                                 symbol, _trail_err, exc_info=True,
                             )
                     else:
                         logger.debug(
                             '[TRAIL_TRIGGER] %s: ATR unavailable this cycle â€” skip', symbol
                         )

                 else:
                      # âœ… FIX: PositionManager is out of sync with exchange (e.g., after restart).
                      # Restore the position into active management.
                      # F-12: prefer persisted original SL/TP prices over ATR approximation.
                      try:
                          _entry = st.entry_price or price
                          _side_pm = PositionSide.LONG if st.position == "LONG" else PositionSide.SHORT

                          # Use original prices if persisted at open time
                          _has_original = (
                              getattr(st, "original_sl_price", 0.0) > 0
                              and getattr(st, "original_tp2_price", 0.0) > 0
                          )
                          if _has_original:
                              _stop_p = float(st.original_sl_price)
                              _tp1_p  = float(getattr(st, "original_tp1_price", 0.0) or 0.0)
                              _tp2_p  = float(st.original_tp2_price)
                              logger.info("[PM_RESTORE] %s: Using persisted SL/TP (sl=%.4f tp1=%.4f tp2=%.4f)", symbol, _stop_p, _tp1_p, _tp2_p)
                          else:
                              # Fall back to ATR approximation when original prices are unavailable
                              _atr_est = float(calculate_atr(klines, period=14)) if klines and len(klines) >= 14 else (price * 0.02)
                              _stop_dist = max(_atr_est * 1.5 / price, 0.01)
                              _tp1_dist  = _stop_dist * 1.0
                              _tp2_dist  = _stop_dist * 2.2
                              if _side_pm == PositionSide.LONG:
                                  _stop_p = _entry * (1.0 - _stop_dist)
                                  _tp1_p  = _entry * (1.0 + _tp1_dist)
                                  _tp2_p  = _entry * (1.0 + _tp2_dist)
                              else:
                                  _stop_p = _entry * (1.0 + _stop_dist)
                                  _tp1_p  = _entry * (1.0 - _tp1_dist)
                                  _tp2_p  = _entry * (1.0 - _tp2_dist)
                              logger.warning("[PM_RESTORE] %s: No persisted SL/TP â€” using ATR approximation. Original stop may differ.", symbol)
                          
                          self.position_manager.open_position(
                              symbol=symbol,
                              side=_side_pm,
                              position_id=st.position_id,
                              entry_price=_entry,
                              qty=st.entry_qty or 0.0,
                              stop_price=_stop_p,
                              tp1_price=_tp1_p,
                              tp2_price=_tp2_p,
                          )
                          logger.info(
                              f"[PM_RESTORE] {symbol}: Restored {st.position} pos into PositionManager "
                              f"(entry={_entry:.4f}, stop={_stop_p:.4f}, tp1={_tp1_p:.4f}, tp2={_tp2_p:.4f}). "
                              f"Active exit management now enabled."
                          )
                          # -- [GAP B FIX] Place protection on exchange after PM restore --
                          # The PM now has the right SL/TP. Push them to the exchange
                          # so the position is never unprotected after a restart.
                          try:
                              _pm_restore_protection = self.executor.ensure_protection(
                                  symbol=symbol,
                                  sl_price=_stop_p,
                                  tp_price=_tp2_p,
                                  repair_source="PM_RESTORE",
                              )
                              self._persist_protection_result(symbol, _pm_restore_protection, "PM_RESTORE")
                               # Seed heartbeat timestamp so the 15s check doesn't fire
                               # again within the same cycle (avoids double NAKED_POSITION_ALERT)
                              import time as _time
                              self._last_protection_checks[symbol] = _time.time()
                              logger.info(
                                  "[PM_RESTORE] %s: ensure_protection placed after restore"
                                  " (sl=%.4f, tp=%.4f)", symbol, _stop_p, _tp2_p
                              )
                          except Exception as _ep_err:
                              logger.error(
                                  "[PM_RESTORE] %s: ensure_protection FAILED: %s",
                                  symbol, _ep_err, exc_info=True,
                              )
                      except Exception as _pm_err:
                          logger.warning(f"[PM_RESTORE] {symbol}: Failed to restore into PositionManager: {_pm_err}")

            # Position management runs on every heartbeat. Entry analysis runs once
            # per newly closed strategy candle.
            if not evaluate_entry:
                try:
                    get_trace_recorder().record_gate(
                        trace_id,
                        allowed=False,
                        reason_code="NO_NEW_CANDLE",
                        reason="NO_NEW_CANDLE",
                        details={"timeframe": self.interval},
                    )
                    get_trace_recorder().finalize(
                        trace_id,
                        state_change="NO_NEW_CANDLE",
                        final_position=st.position,
                    )
                except Exception:
                    pass
                _cs = getattr(self, "_cycle_stats", None)
                if _cs:
                    _cs.record_hold(symbol, 0.0, "NO_NEW_CANDLE")
                return {"symbol": symbol, "decision": "HOLD", "reason": "NO_NEW_CANDLE"}

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
            
            # --- FETCH ADAPTIVE STATE ---
            _hint_dd = abs(getattr(self.daily, "realized_pnl", 0.0)) / max(equity or 1.0, 1.0)
            _atr_hint = float(calculate_atr(klines, period=14)) if klines and len(klines) >= 14 else (price * 0.02)
            _atr_pct_hint = (_atr_hint / price * 100) if price > 0 else 1.5
            
            from app.risk.dynamic_threshold import get_dynamic_threshold_calculator
            dyntc = get_dynamic_threshold_calculator()
            dyn_res = dyntc.get_threshold(symbol)

            a_state = self.adaptive_engine.get_adaptive_state(
                config_id=getattr(self, "run_id", "default") or "default",
                symbol=symbol,
                drawdown_pct_hint=_hint_dd,
                current_atr_pct=_atr_pct_hint,
                active_regime="UNKNOWN", # Will be recorded internally by Strategy later
                base_threshold=dyn_res.threshold,
            )

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
                run_id=self.run_id,
                market_snapshot=market_snapshot,
                cycle_id=self.cycle_id,
                trace_id=trace_id,
                timeframe=self.context.interval if self.context else getattr(settings, "DEFAULT_INTERVAL", "15m"),
                user_id=self.context.user_id if self.context else "",
                bot_instance_id=self.context.bot_instance_id if self.context else None,
                broker_account_id=self.context.broker_account_id if self.context else None,
                market_type=self.context.market_type if self.context else "UNKNOWN",
                enforce_session=(self.context.strategy_params.get("enforce_session") if self.context else None),
                crypto_session_enabled=(self.context.strategy_params.get("crypto_session_enabled", False) if self.context else False),
                execution_mode=self._effective_execution_mode(),
                user_kyc_approved=self._runtime_kyc_allowed(),
                live_readiness_approved=self._runtime_live_readiness_allowed(),
                daily_realized_pnl=float(getattr(self.daily, "realized_pnl", 0.0)),
                daily_trade_count=int(getattr(self.daily, "trade_count", 0)),
                kill_switch=bool(getattr(self.daily, "kill", False)),
                max_daily_loss=float(self.daily_max_loss),
                max_daily_trades=int(self.max_trades_daily),
                max_open_positions=int(self.max_open_positions),
                **self._get_drawdown_context(),
                # --- Adaptive Parameters Passed Down ---
                min_confidence_gate=max(
                    float(a_state.min_confidence_gate),
                    float(getattr(self.context, "min_confidence", 0.0) if self.context else 0.0),
                ),
                strategy_weight_adjustments=a_state.strategy_weight_adjustments,
                adaptive_size_multiplier=a_state.size_multiplier,
                adaptive_leverage_multiplier=a_state.leverage_multiplier,
            )
            
            # Update logging variables from orchestrator result
            eval_decision = orch_res["decision"].upper()
            eval_reason = orch_res.get("reason", "unknown")
            
            # Extract strategy details if available
            strat_out = orch_res.get("details", {}).get("strategy_output", {})
            if strat_out:
                eval_sig = strat_out.get("signal", "None")
                eval_conf_raw = f"{strat_out.get('confidence', 0.0):.4f}"
                eval_conf_norm = eval_conf_raw # We assume normalized if orchestrator did its job
                
                # Extract threshold returning from strategy
                strat_meta = strat_out.get("meta") or {}
                if "threshold" in strat_meta:
                    eval_thr = f"{strat_meta['threshold']:.4f}"

            st.last_signal = strat_out.get("signal", "HOLD")

            # â”€â”€ Stage 1A: Propagate strategy meta â†’ SymbolState + trace â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # strat_meta is the MasterEnsemble meta dict (regime, adx, buy_scoreâ€¦).
            # Without this block every trace has signal=HOLD, confidence=0.0,
            # regime=UNKNOWN, and all indicator columns NULL â€” making ML blind.
            _strat_meta_s1 = (strat_out.get("meta") or {}) if isinstance(strat_out, dict) else {}
            _hold_breakdown_s1 = None
            if isinstance(strat_out, dict) and str(strat_out.get("signal", "")).upper() == "HOLD":
                _hold_breakdown_s1 = build_hold_breakdown(
                    symbol=symbol,
                    raw_strategy_signal=str(strat_out.get("signal", "HOLD")),
                    raw_confidence=float(strat_out.get("confidence", 0.0) or 0.0),
                    final_action=str(orch_res.get("decision", "hold")),
                    reason=str(strat_out.get("reason", orch_res.get("reason", "")) or ""),
                    meta=_strat_meta_s1,
                )
                logger.info("[HOLD_BREAKDOWN] %s", json.dumps(_hold_breakdown_s1, sort_keys=True, default=str))
            if _strat_meta_s1:
                # Persist regime into SymbolState so the ML feature builder
                # (line ~1215) and the adaptive engine see the real regime.
                st.last_regime            = _strat_meta_s1.get("regime", st.last_regime)
                st.last_regime_confidence = float(_strat_meta_s1.get("regime_confidence", 0.0))

                # Also update regime_state in the in-flight trace.
                # record_strategies() writes indicator columns but not regime_state/
                # regime_confidence which are separate columns in decision_traces.
                try:
                    from shared_lib.persistence.trace_recorder import get_trace_recorder as _gtr_regime_s1
                    _gtr_regime_s1().record_regime(
                        trace_id=trace_id,
                        regime_state=st.last_regime,
                        regime_confidence=st.last_regime_confidence,
                    )
                except Exception:
                    pass

            # Write signal truth + indicator snapshot to in-memory trace so
            # finalize() persists them to decision_traces columns.
            try:
                from shared_lib.persistence.trace_recorder import (
                    get_trace_recorder as _gtr_s1,
                    StrategySignal as _SS_s1,
                )
                _sig_s1  = strat_out.get("signal", "HOLD") if isinstance(strat_out, dict) else "HOLD"
                _conf_s1 = float(strat_out.get("confidence", 0.0)) if isinstance(strat_out, dict) else 0.0
                _active_strats_s1 = _strat_meta_s1.get("active_strategies", [])
                _gtr_s1().record_strategies(
                    trace_id=trace_id,
                    signals=[_SS_s1(
                        strategy_name=eval_strat,
                        signal=_sig_s1,
                        confidence=_conf_s1,
                        reason=strat_out.get("reason", "") if isinstance(strat_out, dict) else "",
                        meta=_strat_meta_s1,
                    )],
                    chosen_strategy=eval_strat if _sig_s1 != "HOLD" else None,
                    final_signal=_sig_s1,
                    final_confidence=_conf_s1,
                    reason_codes=str(orch_res.get("reason", "") or ""),
                    # Regime indicators
                    adx=_strat_meta_s1.get("adx"),
                    atr_pct=_strat_meta_s1.get("atr_pct"),
                    ma_slope=_strat_meta_s1.get("ma_slope"),
                    compression_ratio=_strat_meta_s1.get("compression_ratio"),
                    breakout_pressure=_strat_meta_s1.get("breakout_pressure"),
                    # Ensemble scores
                    buy_score=_strat_meta_s1.get("buy_score"),
                    sell_score=_strat_meta_s1.get("sell_score"),
                    threshold=_strat_meta_s1.get("threshold"),
                    active_strategy_count=len(_active_strats_s1) if _active_strats_s1 else None,
                    htf_opposed=bool(_strat_meta_s1.get("htf_opposed")) if "htf_opposed" in _strat_meta_s1 else None,
                )
            except Exception as _s1_err:
                import logging as _s1_log
                _s1_log.getLogger(__name__).debug("[Stage1A] record_strategies failed: %s", _s1_err)
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

            decision = orch_res["decision"]

            # â”€â”€ [PASS] / [ORCH_DECISION] visibility (Sections 2+3) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # Tracks cycle stats and emits INFO logs for actionable signals.
            _cs = getattr(self, "_cycle_stats", None)
            _vis_sig = str(eval_sig).upper() if eval_sig else "NONE"
            _vis_conf = float(strat_out.get("confidence", 0.0)) if isinstance(strat_out, dict) else 0.0
            _vis_thr_f = float(dyn_res.threshold) if dyn_res is not None else 0.0
            if _cs:
                _cs.evaluated += 1
            if _vis_sig in ("HOLD", "NONE", "SIGNAL.HOLD"):
                # Strategy returned HOLD â€” aggregate for cycle summary, DEBUG only
                if _cs:
                    _cs.record_hold(symbol, _vis_conf, eval_reason)
                logger.debug(
                    "[HOLD] %s | conf=%.3f thr=%.3f | reason=%s",
                    symbol, _vis_conf, _vis_thr_f, _normalize_reason(eval_reason),
                )
            elif decision == "execute":
                # Non-HOLD signal approved by orchestrator â†’ [PASS]
                _pass_icon = "ðŸŸ¢" if _vis_sig == "BUY" else "ðŸ”´"
                logger.info(
                    "%s [PASS] %s | side=%s | conf=%.4f | threshold=%.4f | reason=%s",
                    _pass_icon, symbol, _vis_sig, _vis_conf, _vis_thr_f,
                    _normalize_reason(eval_reason),
                )
                if _cs:
                    _cs.record_pass()
            else:
                # Non-HOLD signal BLOCKED by orchestrator
                logger.info(
                    "ðŸš« [ORCH_DECISION] %s | decision=BLOCKED | side=%s | conf=%.4f | threshold=%.4f | reason=%s",
                    symbol, _vis_sig, _vis_conf, _vis_thr_f,
                    _normalize_reason(eval_reason),
                )
                if _cs:
                    _cs.record_block(eval_reason)
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

            # â”€â”€ Observability Fix 1+2: record_gate() â€” persist orchestrator gate decision â”€â”€
            # Before this fix, gate_reason='' and gate_details_json='{}' for all BLOCKED rows.
            # Now stores confidence, dynamic threshold, gap, and the reason text so future
            # audits can reconstruct exactly why each signal was blocked.
            try:
                from shared_lib.persistence.trace_recorder import get_trace_recorder as _gtr_gate_orch
                _gate_conf_orch = float(strat_out.get("confidence", 0.0)) if isinstance(strat_out, dict) else 0.0
                _gate_thr_orch  = float(dyn_res.threshold) if dyn_res is not None else 0.0
                _gate_reason_orch = str(orch_res.get("reason", "") or "")
                _gate_details_orch = {
                    "confidence": _gate_conf_orch,
                    "dynamic_threshold": _gate_thr_orch,
                    "confidence_gap": round(_gate_conf_orch - _gate_thr_orch, 4),
                    "orchestrator_reason": _gate_reason_orch,
                    "orchestrator_decision": decision,
                    "hold_breakdown": _hold_breakdown_s1,
                    "bot_instance_id": self.context.bot_instance_id if self.context else None,
                    "user_id": self.context.user_id if self.context else None,
                    "broker_account_id": self.context.broker_account_id if self.context else None,
                    "market_type": self.context.market_type if self.context else "UNKNOWN",
                    "mode": self._effective_execution_mode(),
                    "timeframe": self.interval,
                    "strategy_id": eval_strat,
                    "signal_source": "internal_strategy",
                    "session_status": _strat_meta_s1.get("session_reason_code"),
                    "risk_evaluated": _vis_sig not in ("HOLD", "NONE", "SIGNAL.HOLD"),
                    "risk_allowed": (decision == "execute") if _vis_sig not in ("HOLD", "NONE", "SIGNAL.HOLD") else None,
                    "sizing_evaluated": decision == "execute",
                    "execution_attempted": False,
                    "fill_recorded": False,
                }
                # Normalise reason to a compact reason_code (max 64 chars, no spaces)
                _rc_orch = _gate_reason_orch.upper().replace(" ", "_").replace("-", "_")[:64] or "ORCHESTRATOR_BLOCKED"
                _gtr_gate_orch().record_gate(
                    trace_id=trace_id,
                    allowed=((decision == "execute") if _vis_sig not in ("HOLD", "NONE", "SIGNAL.HOLD") else None),
                    reason_code=_rc_orch,
                    reason=_gate_reason_orch,
                    details=_gate_details_orch,
                )
                
                # STAGE 2 AUDIT: Initial rejection reason from orchestrator
                if decision != "execute" and _vis_sig not in ("HOLD", "NONE", "SIGNAL.HOLD"):
                   _gtr_gate_orch().record_sizing(
                       trace_id=trace_id,
                       allocation_mode="N/A",
                       base_size=0.0,
                       final_size=0.0,
                       final_qty=0.0,
                       rejection_reason=_gate_reason_orch
                   )
            except Exception:
                pass
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

            # â”€â”€ Fix E-1: record_market() â€” inject real market/risk snapshot into trace â”€â”€
            # The orchestrated path never called record_market(), leaving equity=0.0,
            # last_price=0.0, margin_level=0.0, open_positions_count=0 in every trace.
            # All values are already resolved above; this call costs no extra API round-trips.
            try:
                from shared_lib.persistence.trace_recorder import get_trace_recorder as _gtr_mkt
                _mkt_margin_level = (
                    (equity / max(margin_used, 0.01) * 100.0) if margin_used > 0 else 0.0
                )
                _gtr_mkt().record_market(
                    trace_id=trace_id,
                    last_price=float(price),
                    equity=float(equity),
                    margin_used=float(margin_used),
                    margin_level=float(_mkt_margin_level),
                    open_positions_count=int(open_pos_count),
                    drawdown_pct=float(_hint_dd),
                    regime_state=st.last_regime or "UNKNOWN",
                    regime_confidence=float(st.last_regime_confidence or 0.0),
                    aggressiveness_score=float(a_state.aggressiveness_score) if a_state is not None else None,
                    confidence_gate_modifier=float(a_state.confidence_gate_modifier) if a_state is not None else None,
                    size_multiplier=float(a_state.size_multiplier) if a_state is not None else None,
                    rolling_win_rate=float(a_state.rolling_win_rate) if a_state is not None else None,
                    rolling_expectancy=float(a_state.rolling_expectancy) if a_state is not None else None,
                    loss_streak=int(a_state.loss_streak) if a_state is not None else None,
                )
            except Exception as _mkt_err:
                import logging as _mkt_log
                _mkt_log.getLogger(__name__).debug(
                    "[Fix E-1] record_market failed for %s: %s", symbol, _mkt_err
                )
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

            # --- START ML GATING ---
            _ml_score = None
            _ml_action = None
            if decision == "execute" and st.position == "NONE" and getattr(self, "ml_scorer", None) and self.ml_scorer.enabled:
                try:
                    _strat_meta = strat_out.get("meta", {}) if isinstance(strat_out, dict) else {}
                    from datetime import datetime, timezone
                    import logging as _s_log

                    if not _strat_meta:
                        _s_log.getLogger(__name__).debug(
                            "[MLScorer] %s: strat_meta is empty â€” ensemble features (buy_score, "
                            "sell_score, threshold, htf_opposed, active_strategy_count, adx, "
                            "atr_pct, ma_slope, compression_ratio, breakout_pressure) will be NaN",
                            symbol,
                        )

                    # Real margin_level: already computed just above for record_market()
                    _ml_margin_level = _mkt_margin_level

                    # Real portfolio_risk_used from budget engine (best-effort; 0.0 if unavailable)
                    _ml_portfolio_risk_used = 0.0
                    try:
                        if hasattr(self, "risk_gate") and hasattr(self.risk_gate, "budget_engine") and self.risk_gate.budget_engine:
                            _ml_portfolio_risk_used = float(self.risk_gate.budget_engine.get_budget_state().total_risk_usdt)
                    except Exception:
                        pass

                    _ml_fv = self.ml_scorer.build_feature_vector(
                        symbol=symbol,
                        timeframe=getattr(settings, "DEFAULT_INTERVAL", "15m"),
                        ts=datetime.now(timezone.utc).isoformat(),
                        regime_state=st.last_regime,
                        regime_confidence=st.last_regime_confidence,
                        last_price=float(price),   # neutralised to NaN inside build_feature_vector
                        mark_price=float(price),   # neutralised to NaN inside build_feature_vector
                        margin_level=_ml_margin_level,
                        drawdown_pct=_hint_dd,
                        open_positions_count=int(open_pos_count),
                        portfolio_risk_used=_ml_portfolio_risk_used,
                        final_confidence=float(strat_out.get("confidence", 0.0)) if isinstance(strat_out, dict) else 0.0,
                        chosen_strategy=strat_out.get("strategy_name", "orchestrated") if isinstance(strat_out, dict) else "orchestrated",
                        side=str(orch_res.get("trade_params", {}).get("side", "LONG")),
                        adx=_strat_meta.get("adx"),
                        atr_pct=_strat_meta.get("atr_pct"),
                        ma_slope=_strat_meta.get("ma_slope"),
                        compression_ratio=_strat_meta.get("compression_ratio"),
                        breakout_pressure=_strat_meta.get("breakout_pressure"),
                        buy_score=_strat_meta.get("buy_score"),
                        sell_score=_strat_meta.get("sell_score"),
                        threshold=_strat_meta.get("threshold"),
                        active_strategy_count=len(_strat_meta.get("active_strategies", [])) if "active_strategies" in _strat_meta else None,
                        htf_opposed=bool(_strat_meta.get("htf_opposed")) if "htf_opposed" in _strat_meta else None,
                        open_price=float(orch_res.get("trade_params", {}).get("entry_price", price)),
                        stop_loss_price=float(orch_res.get("trade_params", {}).get("stop_loss")) if orch_res.get("trade_params", {}).get("stop_loss") is not None else None,
                        tp_plan=float(orch_res.get("trade_params", {}).get("take_profit")) if orch_res.get("trade_params", {}).get("take_profit") is not None else None,
                        aggressiveness_score=float(a_state.aggressiveness_score) if a_state is not None else None,
                        confidence_gate_modifier=float(a_state.confidence_gate_modifier) if a_state is not None else None,
                        size_multiplier=float(a_state.size_multiplier) if a_state is not None else None,
                        rolling_win_rate=float(a_state.rolling_win_rate) if a_state is not None else None,
                        rolling_expectancy=float(a_state.rolling_expectancy) if a_state is not None else None,
                        loss_streak=int(a_state.loss_streak) if a_state is not None else None,
                    )
                    _ml_score = self.ml_scorer.score(_ml_fv)
                    _ml_action = self.ml_scorer.get_action(_ml_score)

                    self.ml_scorer.log_prediction(
                        trace_id=trace_id,
                        symbol=symbol,
                        score=_ml_score,
                        action=_ml_action,
                        model_version=self.ml_scorer.model_version,
                        threshold=self.ml_scorer.threshold,
                        feature_vector=_ml_fv,
                    )

                    if _ml_action == "BLOCK":
                        _block_kind = (
                            "FLOOR_BLOCK"
                            if (
                                _ml_score is not None
                                and getattr(self.ml_scorer, "_hard_block_floor", 0.0) > 0.0
                                and _ml_score < self.ml_scorer._hard_block_floor
                            )
                            else "THRESHOLD_BLOCK"
                        )
                        _s_log.getLogger(__name__).info(
                            "ðŸš« [ORCH_DECISION] %s | decision=ML_BLOCK | score=%.3f "
                            "(floor=%.2f threshold=%.2f model=%s)",
                            symbol, _ml_score,
                            getattr(self.ml_scorer, "_hard_block_floor", 0.0),
                            self.ml_scorer.threshold, self.ml_scorer.model_version,
                        )
                        decision = "hold"
                        eval_decision = "HOLD"
                        eval_reason = f"ML_{_block_kind}(score={_ml_score:.3f})"
                        st.last_signal = "HOLD"
                        # Track ML block in cycle stats
                        _cs_ml = getattr(self, "_cycle_stats", None)
                        if _cs_ml:
                            _cs_ml.record_block(f"ml_{_block_kind}")
                            # Un-count the earlier "pass" since ML overrode it
                            if _cs_ml.passed > 0:
                                _cs_ml.passed -= 1

                    elif _ml_action == "SHADOW" and _ml_score is not None:
                        _s_log.getLogger(__name__).debug(
                            "[MLScorer] %s: orchestrated SHADOW score=%.3f < threshold=%.2f",
                            symbol, _ml_score, self.ml_scorer.threshold,
                        )
                    elif _ml_action == "ALLOW" and _ml_score is not None:
                        _s_log.getLogger(__name__).debug(
                            "[MLScorer] %s: orchestrated ALLOW score=%.3f >= threshold=%.2f",
                            symbol, _ml_score, self.ml_scorer.threshold,
                        )
                except Exception as _ml_err:
                    import logging as _s_log
                    _s_log.getLogger(__name__).warning("[MLScorer] %s: orchestrated scoring failed: %s", symbol, _ml_err)

            if trace_id and getattr(self, "ml_scorer", None) and self.ml_scorer.enabled:
                from app.ml.scorer import ACTION_SKIP
                from shared_lib.persistence.trace_recorder import get_trace_recorder
                get_trace_recorder().record_ml_score(
                    trace_id,
                    score=_ml_score,
                    action=_ml_action if _ml_action else ACTION_SKIP,
                    model_version=self.ml_scorer.model_version if getattr(self.ml_scorer, "model_version", None) else None,
                    threshold=self.ml_scorer.threshold if getattr(self.ml_scorer, "threshold", None) else None,
                )
            # --- END ML GATING ---

            # â”€â”€ Stage 2D: Cross-symbol correlation filter â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # Block entry if we already hold a same-direction position in a
            # highly-correlated symbol (prevents double-exposure to the same
            # macro factor when two correlated alts fire simultaneously).
            if decision == "execute" and st.position == "NONE":
                try:
                    from app.risk.correlation_filter import get_correlation_filter
                    _new_dir_s2d = "LONG" if orch_res.get("trade_params", {}).get("side", "BUY") == "BUY" else "SHORT"
                    _open_positions_s2d = {
                        sym: (s.position if s.position in ("LONG", "SHORT") else "NONE")
                        for sym, s in self.state.items()
                        if s.position in ("LONG", "SHORT")
                    }
                    _corr_blocked, _corr_reason = get_correlation_filter().should_block(
                        new_symbol=symbol,
                        new_direction=_new_dir_s2d,
                        open_positions=_open_positions_s2d,
                    )
                    if _corr_blocked:
                        decision = "skip"
                        logger.info(
                            "ðŸš« [ORCH_DECISION] %s | decision=BLOCKED | reason=exec_filter_block (correlation: %s)",
                            symbol, _corr_reason,
                        )
                        _cs_corr = getattr(self, "_cycle_stats", None)
                        if _cs_corr:
                            _cs_corr.record_block("correlation")
                            if _cs_corr.passed > 0:
                                _cs_corr.passed -= 1
                except Exception as _s2d_err:
                    logger.debug("[Stage2D] correlation filter error: %s", _s2d_err)
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

            # â”€â”€ Fix E-2: record_intent() â€” capture pre-execution intended action â”€â”€â”€â”€â”€â”€â”€â”€
            # Without this, every orchestrated trace shows intended_action="HOLD" (default).
            # Called AFTER ML gating + correlation filter so it reflects the final decision
            # before execution (e.g. "hold" if ML blocked, "execute" if allowed).
            try:
                from shared_lib.persistence.trace_recorder import get_trace_recorder as _gtr_int
                _int_tp = orch_res.get("trade_params") if decision == "execute" else {}
                _gtr_int().record_intent(
                    trace_id=trace_id,
                    action=decision.upper(),
                    sizing=_int_tp.get("sizing_trace", {}) if _int_tp else {},
                    sl_plan=float(_int_tp.get("stop_loss") or 0.0) if _int_tp else None,
                    tp_plan=float(_int_tp.get("take_profit") or 0.0) if _int_tp else None,
                )
            except Exception:
                pass
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

            if decision == "execute" and st.position == "NONE":
                p = orch_res["trade_params"]
                trade_usdt = p["quantity"] * p["entry_price"]

                # â”€â”€ [GAP C FIX] Compute real runner TP2 (2.2R) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                # The orchestrator returns a single 'take_profit' at ~1R (TP1 target).
                # TP2 is the runner's final exit â€” it must be 2.2Ã— the SL distance so
                # the exchange TP order is not hit at the same time as TP1.
                _oc_sl   = p.get("stop_loss", 0.0) or 0.0
                _oc_tp1  = p.get("take_profit", 0.0) or 0.0
                _oc_entry = p["entry_price"]
                _oc_side  = PositionSide.LONG if p["side"] == "BUY" else PositionSide.SHORT
                _sl_dist  = abs(_oc_entry - _oc_sl) if _oc_sl > 0 else abs(_oc_entry * 0.02)
                if _oc_side == PositionSide.LONG:
                    _oc_tp2 = _oc_entry + 2.2 * _sl_dist
                else:
                    _oc_tp2 = _oc_entry - 2.2 * _sl_dist

                # STAGE 2 AUDIT: Record detailed sizing before submission
                try:
                    _trace_sz = _int_tp.get("sizing_trace", {}) if _int_tp else {}
                    _gtr_int().record_sizing(
                        trace_id=trace_id,
                        allocation_mode=str(_trace_sz.get("allocation_mode", "unknown")),
                        base_size=float(_trace_sz.get("base_size", 0.0)),
                        final_size=float(_trace_sz.get("final_size", 0.0)),
                        final_qty=float(p["quantity"]),
                        min_qty=float(_trace_sz.get("min_qty", 0.0)),
                        min_notional=float(_trace_sz.get("min_notional", 0.0)),
                    )
                except Exception:
                    pass

                # â”€â”€ [EXECUTE_ATTEMPT] â€” pre-order visibility (Section 4) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                logger.info(
                    "âš¡ [EXECUTE_ATTEMPT] %s | side=%s | notional=%.2f | entry=%.4f | "
                    "sl=%.4f | tp=%.4f | leverage=%s",
                    symbol, p["side"], trade_usdt, _oc_entry,
                    _oc_sl, _oc_tp2, p.get("leverage", "default"),
                )
                _cs_exec = getattr(self, "_cycle_stats", None)
                if _cs_exec:
                    _cs_exec.execute_attempts += 1
                # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

                res = self.executor.execute_signal(
                    symbol,
                    p["side"],
                    trade_usdt,
                    sl_price=_oc_sl,
                    tp_price=_oc_tp2,   # â† runner's 2.2R target sent to exchange
                    current_open_count=open_pos_count,
                    current_equity=equity,
                    leverage_override=p.get("leverage")
                )
                _executed_qty = float(
                    ((res.details or {}).get("filled_qty") if isinstance(res.details, dict) else 0.0)
                    or p.get("quantity", 0.0)
                )

                # â”€â”€ Step 5F-1 execution linkage fix â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                # Bind execution result (order_id, fill_price) into the decision trace
                # so decision_traces.order_id is populated and joins trade_fills correctly.
                try:
                    from shared_lib.persistence.trace_recorder import get_trace_recorder as _gtr
                    _gtr().record_execution(
                        trace_id,
                        status=res.status,
                        order_id=res.order_id,
                        fill_price=(float(res.avg_price) if (res.avg_price and float(res.avg_price) > 0) else float(_oc_entry)) if res.success else None,
                        fill_qty=_executed_qty if res.success else None,
                        error=res.error if not res.success else None,
                    )
                except Exception:
                    pass
                # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

                if res.success:
                    self.live_trades_this_cycle += 1
                    # â”€â”€ Fix 2: Assign position_id at OPEN so OPENâ†’CLOSE fills link â”€â”€â”€â”€â”€â”€â”€â”€
                    import uuid as _uuid_open
                    st.position_id = str(_uuid_open.uuid4())
                    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

                    # â”€â”€ [ORDER_PLACED] â€” trade confirmation visibility â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                    logger.info(
                        "âœ… [ORDER_PLACED] %s | side=%s | notional=%.2f | "
                        "entry=%.4f | sl=%.4f | tp=%.4f | order_id=%s",
                        symbol, p["side"], trade_usdt, _oc_entry,
                        _oc_sl, _oc_tp2, res.order_id or "N/A",
                    )
                    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

                    self.position_manager.open_position(
                        symbol=symbol,
                        side=_oc_side,
                        position_id=st.position_id,
                        entry_price=_oc_entry,
                        qty=_executed_qty,
                        stop_price=_oc_sl,
                        tp1_price=_oc_tp1,  # TP1 for partial-close trigger
                        tp2_price=_oc_tp2,  # TP2 (2.2R) for runner exit
                        strategy_name=getattr(p, 'strategy_name', ''),
                        sl_order_id=((res.details or {}).get("protection") or {}).get("sl_order_id") if isinstance(res.details, dict) else None,
                        tp_order_id=((res.details or {}).get("protection") or {}).get("tp_order_id") if isinstance(res.details, dict) else None,
                    )

                    # STAGE 2 AUDIT: Position opened successfully
                    try:
                        _gtr().record_execution(trace_id, status=res.status) # Update flags
                        with _gtr()._lock:
                            _tr = _gtr()._traces.get(trace_id)
                            if _tr:
                                _tr.position_opened = True
                    except Exception:
                        pass

                    # â”€â”€ Fix 3: Update in-memory SymbolState immediately after open â”€â”€â”€â”€â”€â”€â”€â”€â”€
                    # Without this, st.position stays "NONE" for the rest of this cycle.
                    # Subsequent symbols then see open_pos_count=0 and can all open at once,
                    # bypassing max_open_positions.  Set the broker-truth fields now so the
                    # next symbol's capacity check sees the correct count.
                    _open_side_str = "LONG" if p["side"] == "BUY" else "SHORT"
                    st.position   = _open_side_str
                    st.entry_price = float(res.avg_price or _oc_entry)
                    st.entry_qty   = _executed_qty
                    # F-3: persist TP2 price so D-2 R:R gate can evaluate it
                    # F-12: persist original SL/TP so PM restore after restart uses exact levels
                    st.current_stop_loss = float(_oc_sl) if _oc_sl else None
                    st.tp_price = float(_oc_tp2) if _oc_tp2 else 0.0
                    st.original_sl_price = float(_oc_sl) if _oc_sl else 0.0
                    st.original_tp1_price = float(_oc_tp1) if _oc_tp1 else 0.0
                    st.original_tp2_price = float(_oc_tp2) if _oc_tp2 else 0.0
                    # Persist immediately so restarts and other instances pick this up.
                    try:
                        self.store.save_symbol(symbol, st)
                    except Exception as _save_err:
                        import logging as _save_log
                        _save_log.getLogger(__name__).warning(
                            "[Fix 3] %s: failed to persist SymbolState after open: %s",
                            symbol, _save_err,
                        )
                    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

                    # â”€â”€ Step 5F-1 fill recording fix (updated with position_id) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                    # Record the OPEN fill into trade_fills. position_id links to CLOSE.
                    try:
                        from shared_lib.persistence.trade_fills import record_fill as _rf
                        from app.core.config import settings as _s
                        _fill_side = "LONG" if p.get("side", "BUY") == "BUY" else "SHORT"
                        _fill_actual = float(res.avg_price or _oc_entry)
                        _slip_pct = (
                            ((_fill_actual - _oc_entry) / _oc_entry * 100)
                            if _oc_entry > 0 else None
                        )
                        _fill_conf = (
                            float(strat_out.get("confidence", 0.0))
                            if isinstance(strat_out, dict) else None
                        )
                        _fill_strat = (
                            strat_out.get("strategy_name", "orchestrated")
                            if isinstance(strat_out, dict) else "orchestrated"
                        )
                        _oc_run_id = self.run_id
                        _oc_cycle_id = self.cycle_id
                        if not _oc_run_id or not _oc_cycle_id:
                            logger.error(
                                "[FILL LINKAGE ERROR] Missing run_id/cycle_id for symbol=%s path=orchestrated_open",
                                symbol,
                            )
                        _rf(
                            self.db,
                            symbol=symbol,
                            side=_fill_side,
                            action="OPEN",
                            qty=_executed_qty,
                            price=_fill_actual,
                            fee=None,
                            realized_pnl=None,
                            order_id=res.order_id,
                            strategy=_fill_strat,
                            strategy_version="0",
                            broker_id=getattr(_s, "BROKER_ID", "binance_futures"),
                            account_id=getattr(_s, "ACCOUNT_ID", "default"),
                            asset_class=getattr(_s, "ASSET_CLASS", "CRYPTO"),
                            market_type=self.context.market_type if self.context else getattr(_s, "ASSET_CLASS", "CRYPTO"),
                            execution_mode=self._effective_execution_mode(),
                            timeframe=getattr(_s, "DEFAULT_INTERVAL", "15m"),
                            confidence=_fill_conf,
                            slippage_pct=_slip_pct,
                            entry_price_expected=float(_oc_entry),
                            stop_loss_price=float(_oc_sl) if _oc_sl > 0 else None,
                            position_id=st.position_id,
                            bot_instance_id=self.context.bot_instance_id if self.context else None,
                            user_id=self.context.user_id if self.context else None,
                            broker_account_id=self.context.broker_account_id if self.context else None,
                            run_id=_oc_run_id,
                            cycle_id=_oc_cycle_id,
                            trace_id=trace_id,
                        )
                        _gtr().link_position(trace_id, st.position_id)
                        # STAGE 2 AUDIT: Fill recorded
                        try:
                            with _gtr()._lock:
                                _tr = _gtr()._traces.get(trace_id)
                                if _tr:
                                    _tr.fill_recorded = True
                        except Exception:
                            pass
                    except Exception as _fill_err:
                        logger.warning(f"Fill recording failed for {symbol}: {_fill_err}")
                    # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

                # ========== EXPOSE EXECUTION FAILURES (CIRCUIT BREAKER ROOT CAUSE) ==========
                # Only show urgent banner for unexpected failures â€” not business-logic rejections
                _non_fatal_statuses = {"INSUFFICIENT_MARGIN", "NO_TRADE_INVALID_QTY", 
                                        "SKIPPED_NOT_LIVE_SYMBOL", "PAPER_ONLY", "NO_TRADE"}
                if not res.success and res.status not in _non_fatal_statuses:
                    import sys
                    print("\n" + "="*80, file=sys.stderr)
                    print("ðŸš¨ TRADE EXECUTION FAILED - CIRCUIT BREAKER TRIGGER", file=sys.stderr)
                    print("="*80, file=sys.stderr)
                    print(f"Symbol: {symbol}", file=sys.stderr)
                    print(f"Side: {p['side']}", file=sys.stderr)
                    print(f"Trade USDT: {trade_usdt}", file=sys.stderr)
                    print(f"Execution Result:", file=sys.stderr)
                    print(f"  - Status: {res.status}", file=sys.stderr)
                    print(f"  - Success: {res.success}", file=sys.stderr)
                    print(f"  - Error: {res.error}", file=sys.stderr)
                    print(f"  - Details: {res.details}", file=sys.stderr)
                    print("="*80 + "\n", file=sys.stderr)
                elif not res.success:
                    # Non-fatal: just log a warning, no panic banner
                    import logging as _rl
                    _rl.getLogger(__name__).warning(
                        f"[EXEC SKIP] {symbol}: {res.status} â€” {res.error}"
                    )
                
                # Feedback to orchestrator for Layer D monitoring
                # CRITICAL: Do NOT record paper/non-fatal results as circuit breaker failures.
                # PAPER_ONLY, NO_TRADE, etc. have success=False but are not real exchange errors.
                # Recording them caused the circuit breaker to trip after ~5 cycles, silently
                # blocking all future trades for that symbol in paper mode.
                _is_real_failure = not res.success and res.status not in _non_fatal_statuses
                self.orchestrator.record_trade_execution(
                    symbol=symbol,
                    success=True if not _is_real_failure else False,  # treat paper skips as "success" for CB
                    expected_price=p["entry_price"],
                    executed_price=res.avg_price if res.success else None,
                    error_message=res.error if _is_real_failure else None,
                    action=getattr(res, "action", "OPEN")
                )
                
                if not res.success:
                    eval_decision = "EXEC_FAIL"
                    eval_reason = res.error
                else:
                    eval_decision = "EXECUTED"
                
                return {
                    "symbol": symbol,
                    "signal": p["side"],
                    "decision": "execute" if res.success else "error",
                    "reason": None if res.success else (res.error or res.status),
                    "execution_attempted": True,
                    "fill_recorded": bool(res.success),
                    "details": res.details,
                }
                
            # Log skipped symbol reason (duplicate of [EVAL] but keeps legacy info format)
            if decision == "execute" and st.position != "NONE":
                 logger.info(
                     "ðŸš« [ORCH_DECISION] %s | decision=BLOCKED | reason=already_open (%s position)",
                     symbol, st.position,
                 )
                 eval_decision = "ALREADY_OPEN"
                 eval_reason = f"Existing {st.position} position blocks new entry"
                 _cs_ao = getattr(self, "_cycle_stats", None)
                 if _cs_ao:
                     _cs_ao.record_block("already_open")
                     if _cs_ao.passed > 0:
                         _cs_ao.passed -= 1

                 # â”€â”€ Trace labeling fix: write execution_status='ALREADY_OPEN' so the trace
                 # is self-consistent.  Without this, intended_action='EXECUTE' + execution_status=
                 # 'None' looks like a silent execution failure when it is intentional suppression.
                 try:
                     from shared_lib.persistence.trace_recorder import get_trace_recorder as _gtr_ao
                     _gtr_ao().record_execution(
                         trace_id=trace_id,
                         status="ALREADY_OPEN",
                         order_id=None,
                         fill_price=None,
                         fill_qty=None,
                         error=eval_reason,
                     )
                 except Exception:
                     pass

            if decision != "execute":
                pass # Suppress duplicate log, rely on [EVAL]

            return {"symbol": symbol, "decision": decision, "reason": orch_res.get("reason"), "details": orch_res.get("details")}
            
        except Exception as e:
            eval_decision = "ERROR"
            eval_reason = f"{type(e).__name__}: {e}"
            logger.exception(f"CRITICAL: Runner exception for {symbol}: {e}")
            return {
                "symbol": symbol, 
                "decision": "error", 
                "reason": eval_reason, 
                "details": {"traceback": traceback.format_exc()}
            }
        finally:
            # â”€â”€ Step 5F-1 persistence fix â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # _step_symbol_orchestrated() is an early-return path that bypasses the
            # _finalize() call in step_symbol().  Without this block, the trace that
            # was started in step_symbol() (and had record_ml_score() called on it)
            # is never written to the DB â€” ML decisions stay JSONL-only.
            # This finally-block guarantees finalize() fires on every code path.
            try:
                from shared_lib.persistence.trace_recorder import get_trace_recorder as _gtr
                _orch_final_pos = "NONE"
                try:
                    _orch_final_pos = st.position
                except Exception:
                    pass
                _gtr().finalize(
                    trace_id=trace_id,
                    state_change=eval_decision,
                    final_position=_orch_final_pos,
                )
            except Exception:
                pass
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # â”€â”€ Shadow Trading System Capture â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # Passive observer to capture rejected/non-executed trade opportunities
            # for research analytics. Never mutates live state.
            try:
                from app.shadow.capture import get_shadow_capture
                _shadow = get_shadow_capture()
                _shadow.maybe_capture(
                    bot_instance_id=getattr(self.context, "bot_instance_id", "default"),
                    trace_id=trace_id,
                    symbol=symbol,
                    interval=self.config.interval,
                    eval_decision=locals().get("eval_decision", "UNKNOWN"),
                    orchestrator_decision=locals().get("decision", "hold"),
                    strat_out=locals().get("strat_out"),
                    orch_res=locals().get("orch_res"),
                    ml_score=locals().get("_ml_score"),
                    ml_action=locals().get("_ml_action"),
                    dyn_threshold=locals().get("dyn_res").threshold if locals().get("dyn_res") else None,
                    entry_price=locals().get("price"),
                    klines=klines,
                    client=self.client,
                )
            except Exception as exc:
                logger.debug("[ShadowCapture] unexpected failure: %s", exc)
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # LEGACY EVAL LOG (safety net â€” primary visibility is now [PASS]/[ORCH_DECISION]/[EXECUTE_ATTEMPT])
            # HOLDs are at DEBUG via the Section 2 block above; non-HOLDs already have dedicated INFO logs.
            # This block only fires as a catch-all for edge cases.
            safe_sig = str(eval_sig).upper() if eval_sig else "NONE"
            if safe_sig not in ("HOLD", "NONE", "SIGNAL.HOLD"):
                logger.debug(
                    "[EVAL] %s | sig=%s | conf=%s thr=%s | decision=%s | reason=%s",
                    symbol, safe_sig, eval_conf_raw, eval_thr, eval_decision, eval_reason,
                )


    def _persist_tp1_outcome(
        self,
        *,
        symbol: str,
        st,
        tp1_result: dict,
        fallback_price: float,
        trace_id: str | None = None,
    ) -> None:
        """Make a TP1 partial close durable: remaining quantity + a real fill row.

        Both TP1 call sites must go through here.  Updating PositionManager alone
        is not evidence: without the persisted SymbolState the remaining quantity
        reverts to the pre-TP1 size on restart, and without the fill row the
        accounting invariant (open - partials - final == 0) cannot be verified.
        """
        import logging as _logging

        _log = _logging.getLogger(__name__)

        runner_qty = tp1_result.get("runner_qty")
        if runner_qty is None or float(runner_qty) < 0:
            return

        runner_qty = float(runner_qty)
        st.entry_qty = runner_qty

        # 1. Authoritative remaining quantity must survive a restart.
        if getattr(self, "store", None):
            try:
                self.store.save_symbol(symbol, st)
            except Exception as exc:
                _log.warning("%s TP1: failed to persist SymbolState: %s", symbol, exc)

        fill_qty = float(tp1_result.get("fill_qty") or 0.0)
        if fill_qty <= 0:
            return

        # 2. Persist the partial-close fill with full, distinct attribution.
        #    bot_instance_id and run_id are different identifiers and must never
        #    be substituted for one another.
        try:
            from shared_lib.persistence.trade_fills import record_fill

            record_fill(
                self.db,
                symbol=symbol,
                side=st.position,
                action="PARTIAL_CLOSE",
                qty=fill_qty,
                price=float(tp1_result.get("fill_price") or fallback_price),
                fee=float(tp1_result.get("fee") or 0.0),
                realized_pnl=float(tp1_result.get("realized_pnl") or 0.0),
                order_id=str((tp1_result.get("broker_response") or {}).get("order_id") or ""),
                strategy="orchestrated",
                bot_instance_id=self.context.bot_instance_id if self.context else None,
                user_id=self.context.user_id if self.context else None,
                broker_account_id=self.context.broker_account_id if self.context else None,
                execution_mode=self._effective_execution_mode(),
                timeframe=self.interval,
                position_id=st.position_id,
                initiator_type="BOT",
                trigger_source="LIFECYCLE_TP1",
                position_phase="RUNNER_TRAILING",
                run_id=self.run_id,
                cycle_id=self.cycle_id,
                trace_id=trace_id,
                fill_type="TP1",
                remaining_qty=runner_qty,
            )
        except Exception as exc:
            _log.error("%s TP1: failed to persist partial-close fill: %s", symbol, exc, exc_info=True)
            return

        _log.info(
            "[PAPER_LIFECYCLE] event=TP1 bot=%s run_id=%s position_id=%s symbol=%s side=%s "
            "executed_qty=%s remaining_qty=%s fill_id=%s status=%s",
            self.context.bot_instance_id if self.context else None,
            self.run_id,
            st.position_id,
            symbol,
            st.position,
            fill_qty,
            runner_qty,
            tp1_result.get("fill_id"),
            tp1_result.get("lifecycle_state_after"),
        )

    def _reconcile_tp1_executing(self, symbol: str, pm_state) -> None:
        """
        Restart recovery for TP1_EXECUTING phase.

        When the bot restarts with a persisted TP1_EXECUTING phase the partial
        close order may or may not have filled.  Compare the live broker position
        quantity to pm_state.tp1_exec_qty to determine which path we were on and
        advance the lifecycle accordingly.

        Outcomes:
            qty_reduced (>= exec_qty * 0.5) â†’ TP1 likely filled â†’ RUNNER_TRAILING
            qty_unchanged                   â†’ TP1 order failed  â†’ SEEKING_TP1
        """
        import logging
        from app.execution.position_manager import PositionPhase
        _log = logging.getLogger(__name__)

        try:
            raw = self.client.get_position_amt(symbol)
            live_qty = abs(float(raw))
        except Exception as e:
            _log.warning(
                f"[TP1_RECONCILE] {symbol}: Could not fetch broker qty on restart: {e}. "
                f"Leaving phase=TP1_EXECUTING â€” will retry on next heartbeat."
            )
            return

        exec_qty  = pm_state.tp1_exec_qty or 0.0
        entry_qty = pm_state.entry_qty or pm_state.current_qty or 0.0

        qty_reduced = (entry_qty - live_qty) >= (exec_qty * 0.5)

        if qty_reduced and live_qty > 0:
            # TP1 close most likely filled â€” advance to RUNNER_TRAILING
            pm_state.phase = PositionPhase.RUNNER_TRAILING
            pm_state.tp1_fill_qty = entry_qty - live_qty
            pm_state.tp.tp1_hit = True
            pm_state.current_qty = live_qty
            self.position_manager._persist_lifecycle(symbol)
            _log.info(
                f"[TP1_RECONCILE] {symbol}: HEALED â†’ RUNNER_TRAILING "
                f"(live_qty={live_qty} entry_qty={entry_qty} exec_qty={exec_qty})"
            )
            # Ensure protection is anchored for runner
            try:
                _tp1_reconcile_protection = self.executor.ensure_protection(
                    symbol=symbol,
                    sl_price=float(pm_state.sl.current_stop) if pm_state.sl.current_stop else None,
                    tp_price=float(pm_state.tp.tp2_price) if pm_state.tp.tp2_price else None,
                    repair_source="TP1_RECONCILE_RESTART",
                )
                self._persist_protection_result(symbol, _tp1_reconcile_protection, "TP1_RECONCILE_RESTART")
            except Exception as ep:
                _log.error(f"[TP1_RECONCILE] {symbol}: ensure_protection after healing failed: {ep}")
        elif live_qty <= 0:
            # Position is flat â€” close must have over-filled (rare)
            self.position_manager.close_position(symbol, "TP1_RECONCILE_FLAT")
            _log.warning(f"[TP1_RECONCILE] {symbol}: Position is FLAT on restart â†’ closed lifecycle.")
        else:
            # Qty unchanged â€” close likely failed, revert so heartbeat can retry
            pm_state.phase = PositionPhase.SEEKING_TP1
            pm_state.tp1_exec_qty = 0.0
            pm_state.tp1_exec_ts = None
            self.position_manager._persist_lifecycle(symbol)
            _log.warning(
                f"[TP1_RECONCILE] {symbol}: TP1 close appears to have NOT filled â€” "
                f"reverted to SEEKING_TP1 (live_qty={live_qty} entry_qty={entry_qty})"
            )

    def _reconcile_break_even_pending(self, symbol: str, pm_state) -> None:
        """
        Restart recovery for BREAK_EVEN_PENDING phase (Step 3D).

        When the bot restarts mid-BREAK_EVEN_PENDING the exchange stop may or
        may not have been updated.  We use be_exchange_confirmed as the source
        of truth:

            be_exchange_confirmed=True  -> exchange already has the BE stop;
                                          advance PM to RUNNER_TRAILING and persist.
            be_exchange_confirmed=False -> update_protection never completed;
                                          retry immediately via execute_break_even_update().

        Both paths are idempotent â€” the executor's own guards prevent double-updates.
        """
        import logging
        from app.execution.position_manager import PositionPhase, PositionSide
        _log = logging.getLogger(__name__)

        if pm_state.sl.be_exchange_confirmed:
            # Exchange already has the BE stop â€” PM phase just needs advancing.
            _log.info(
                f"[BE_RECONCILE] {symbol}: be_exchange_confirmed=True on restart; "
                f"advancing phase to RUNNER_TRAILING without re-sending update."
            )
            pm_state.phase = PositionPhase.RUNNER_TRAILING
            self.position_manager._persist_lifecycle(symbol)
        else:
            # Exchange update never completed â€” retry it now.
            _log.warning(
                f"[BE_RECONCILE] {symbol}: BREAK_EVEN_PENDING with be_exchange_confirmed=False; "
                f"retrying execute_break_even_update on restart."
            )
            pos_side_str = (
                "LONG" if pm_state.side == PositionSide.LONG else "SHORT"
            )
            try:
                # Reset phase to RUNNER_TRAILING so the executor's phase guard
                # allows the retry.  BREAK_EVEN_PENDING + be_exchange_confirmed=False
                # means the update_protection call never completed; we treat this
                # identically to a fresh heartbeat in RUNNER_TRAILING.
                pm_state.phase = PositionPhase.RUNNER_TRAILING
                be_result = self.executor.execute_break_even_update(
                    symbol=symbol,
                    position_side=pos_side_str,
                    runner_qty=float(pm_state.current_qty),
                    entry_price=float(pm_state.entry_price),
                    current_stop=float(pm_state.sl.current_stop),
                    sl_order_id=pm_state.sl.sl_order_id,
                    tp_order_id=pm_state.sl.tp_order_id,
                    tp2_price=float(pm_state.tp.tp2_price) if pm_state.tp.tp2_price else None,
                    position_manager=self.position_manager,
                )
                if be_result.get("break_even_applied"):
                    _log.info(
                        f"[BE_RECONCILE] {symbol}: BE retry succeeded on restart. "
                        f"norm_be={be_result.get('normalized_break_even_price')}"
                    )
                else:
                    _log.warning(
                        f"[BE_RECONCILE] {symbol}: BE retry on restart did not apply. "
                        f"skip_reason={be_result.get('skip_reason')} "
                        f"failure_reason={be_result.get('failure_reason')}"
                    )
            except Exception as e:
                _log.error(
                    f"[BE_RECONCILE] {symbol}: execute_break_even_update retry on restart FAILED: {e}",
                    exc_info=True,
                )

    def _reconcile_trailing_update_pending(self, symbol: str, pm_state) -> None:
        """
        Restart recovery for TRAILING_UPDATE_PENDING phase (Step 3E, Refinement 2).

        Reconciles broker truth before deciding whether the trailing update landed:

            broker_stop ~= intended_stop (trailing_last_stop_price)
                -> update succeeded while bot was down;
                   confirm: update current_stop + advance to RUNNER_TRAILING.

            broker_stop != intended_stop (or lookup fails)
                -> update did not land; revert to RUNNER_TRAILING so the
                   normal heartbeat trigger fires on next cycle.

        In both cases the next TRAIL_UPDATED heartbeat re-evaluates cleanly.
        """
        import logging
        from app.execution.position_manager import PositionPhase
        _log = logging.getLogger(__name__)

        intended_stop = pm_state.sl.trailing_last_stop_price
        confirmed = False

        if intended_stop is not None:
            try:
                broker_stop = float(self.executor.client.get_position_stop(symbol))
                tol = max(abs(intended_stop) * 0.0005, 0.01)  # 0.05% or 1 cent
                if abs(broker_stop - intended_stop) <= tol:
                    confirmed = True
                    _log.info(
                        "[TRAIL_RECONCILE] %s: broker_stop=%.4f matches intended=%.4f "
                        "â€” confirming trailing update that landed while bot was down.",
                        symbol, broker_stop, intended_stop,
                    )
                    pm_state.sl.current_stop = intended_stop
                else:
                    _log.warning(
                        "[TRAIL_RECONCILE] %s: broker_stop=%.4f != intended=%.4f "
                        "â€” trailing update did NOT land; reverting to RUNNER_TRAILING.",
                        symbol, broker_stop, intended_stop,
                    )
            except Exception as _e:
                _log.warning(
                    "[TRAIL_RECONCILE] %s: broker stop lookup failed (%s) "
                    "â€” reverting to RUNNER_TRAILING conservatively.",
                    symbol, _e,
                )
        else:
            _log.warning(
                "[TRAIL_RECONCILE] %s: TRAILING_UPDATE_PENDING but no intended stop recorded "
                "â€” reverting to RUNNER_TRAILING.",
                symbol,
            )

        pm_state.phase = PositionPhase.RUNNER_TRAILING
        self.position_manager._persist_lifecycle(symbol)
        _log.info(
            "[TRAIL_RECONCILE] %s: phase reset to RUNNER_TRAILING (confirmed=%s).",
            symbol, confirmed,
        )

    def _effective_execution_mode(self) -> str:
        mode = self.context.execution_mode if self.context else getattr(settings, "EXECUTION_MODE", "paper")
        normalized = str(mode or "paper").strip().lower()
        return "broker" if normalized in {"live", "broker", "testnet", "demo"} else "paper"

    def _effective_iofs_mode(self) -> str:
        if not bool(getattr(settings, "IOFS_GATE_ENABLED", False)):
            return "disabled"

        mode = str(getattr(settings, "IOFS_GATE_MODE", "shadow") or "shadow").strip().lower()
        if mode not in {"disabled", "shadow", "enforce"}:
            logger.warning("[IOFS_GATE] Invalid mode=%s; defaulting to shadow", mode)
            mode = "shadow"
        if mode == "enforce" and self._effective_execution_mode() == "broker":
            logger.warning(
                "[IOFS_GATE] Live enforce requested; downgrading to shadow. "
                "IOFS enforcement is paper/testnet-only."
            )
            return "shadow"
        return mode

    def _run_iofs_pre_ensemble(
        self,
        symbol: str,
        *,
        trace_id: str | None,
        current_position: str = "NONE",
    ) -> dict[str, Any]:
        mode = self._effective_iofs_mode()
        if mode == "disabled":
            return {"evaluated": False, "blocked": False, "mode": "disabled"}

        profile = str(getattr(settings, "IOFS_RISK_PROFILE", "balanced") or "balanced")
        allowed_symbols = str(getattr(settings, "IOFS_ALLOWED_SYMBOLS", "BTCUSDT,ETHUSDT") or "")
        if not is_symbol_allowed(symbol, allowed_symbols):
            result = make_gate_failure("SYMBOL_NOT_ALLOWED", profile)
        elif bool(getattr(settings, "IOFS_SESSION_FILTER_ENABLED", True)) and not is_session_allowed(
            str(getattr(settings, "IOFS_SESSION_WINDOWS_UTC", "07:00-10:00,13:00-16:00"))
        ):
            result = make_gate_failure("OUTSIDE_SESSION", profile)
        else:
            try:
                fetcher = getattr(self, "iofs_fetcher", None) or MultiTimeframeFetcher(self.client)
                evaluator = getattr(self, "iofs_evaluator", None) or IOFSGateEvaluator()
                self.iofs_fetcher = fetcher
                self.iofs_evaluator = evaluator
                candles_by_tf = asyncio.run(fetcher.fetch_all(symbol))
                result = evaluator.evaluate(candles_by_tf, profile)
            except MultiTimeframeFetchError as exc:
                reason = "INVALID_CANDLES" if "invalid_candles" in str(exc) else "MISSING_TIMEFRAME"
                result = make_gate_failure(reason, profile)
            except Exception as exc:
                logger.warning("[IOFS_GATE] %s evaluation failed closed: %s", symbol, exc)
                result = make_gate_failure("INVALID_CANDLES", profile)

        position_is_flat = str(current_position or "NONE").upper() not in {"LONG", "SHORT"}
        blocked = mode == "enforce" and not result.passed and position_is_flat
        details = gate_result_details(symbol, mode, result, blocked_trade=blocked)
        if not hasattr(self, "last_iofs_result"):
            self.last_iofs_result = {}
        self.last_iofs_result[str(symbol).upper()] = details

        logger.info("[IOFS_GATE] %s", json.dumps(details, sort_keys=True))
        try:
            self.audit.event(
                event_type="IOFS_GATE",
                run_id=getattr(self, "run_id", None),
                cycle_id=getattr(self, "cycle_id", None),
                symbol=symbol,
                action="BLOCKED" if blocked else "EVALUATED",
                details=details,
                trace_id=trace_id,
            )
        except Exception:
            pass

        return {
            "evaluated": True,
            "blocked": blocked,
            "mode": mode,
            "result": result,
            "details": details,
        }

    def step_symbol(self, symbol: str) -> Dict[str, Any]:
        # âœ… START TRACE
        recorder = get_trace_recorder()
        trace_id = recorder.start_trace(
            run_id=self.run_id,
            cycle_id=getattr(self, "cycle_id", None),
            symbol=symbol,
            account_id=self.context.broker_account_id if self.context else getattr(settings, "ACCOUNT_ID", "default"),
            environment=self.context.execution_mode if self.context else getattr(settings, "EXECUTION_MODE", "paper"),
            timeframe=self.interval,
            bot_instance_id=self.context.bot_instance_id if self.context else None,
            user_id=self.context.user_id if self.context else None,
            effective_policy_hash=self.effective_policy_hash,
            signal_source="INTERNAL_MASTER_ENSEMBLE",
        )
        # Store for internal use if needed (hacky, but simple)
        recorder._active_trace_id = trace_id

        # âœ… LAZY INIT STATE (Robustness)
        if symbol not in self.state:
            self.state[symbol] = SymbolState()
            
        st = self.state[symbol]

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

            logger.info(f"â­ï¸ SKIPPING {symbol}: Execution lock timeout (busy)")
            recorder.record_gate(trace_id, allowed=False, reason_code="SYMBOL_LOCK_BUSY", reason="SYMBOL_LOCK_BUSY", details={})
            recorder.finalize(trace_id, state_change="SKIP", final_position=st.position)
            return {"symbol": symbol, "skipped": True, "reason": "SYMBOL_LOCK_BUSY"}

        try:
            # âœ… HARDENING: Periodic protection check (Runtime)
            # Verify SL/TP exists if position is open. Throttled (e.g. 60s).
            if st and st.position in ("LONG", "SHORT"):
                now = time.time()
                last_chk = self._last_protection_checks.get(symbol, 0.0)
                if now - last_chk > 15:  # âœ… SEV-1 S8: Tightened from 60s to 15s for containment
                    try:
                        # â”€â”€ [GAP A FIX] Use PositionManager as source of truth â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
                        # Always prefer the PM's tracked stop and runner TP over the
                        # FALLBACK_COMPUTED path (live price Ã— config%).  The PM owns
                        # current_stop (which may be a trailed/break-even value) and
                        # tp2_price (the runner's final target).  This avoids misplaced
                        # repair orders whenever the exchange protection is missing.
                        _pm_hb = self.position_manager.get_position(symbol)
                        _hb_sl = float(_pm_hb.sl.current_stop) if _pm_hb and _pm_hb.sl.current_stop else None
                        _hb_tp = float(_pm_hb.tp.tp2_price)    if _pm_hb and _pm_hb.tp.tp2_price else None
                        _heartbeat_protection = self.executor.ensure_protection(
                            symbol=symbol,
                            sl_price=_hb_sl,
                            tp_price=_hb_tp,
                            repair_source="PERSISTED" if (_hb_sl and _hb_tp) else "FALLBACK_COMPUTED",
                        )
                        self._persist_protection_result(
                            symbol,
                            _heartbeat_protection,
                            "PERSISTED" if (_hb_sl and _hb_tp) else "FALLBACK_COMPUTED",
                        )
                        self._last_protection_checks[symbol] = now
                    except Exception as e:
                        logger.error(f"[PROTECTION CHECK] Runtime protection check FAILED for {symbol}: {e}", exc_info=True)

            # D-1: Per-bot consecutive-loss guard â€” checked before any strategy work.
            # Only blocks NEW entries; existing position management (SL/TP) continues.
            _bot_id_guard = self.context.bot_instance_id if self.context else "default"
            if st.position not in ("LONG", "SHORT"):
                try:
                    if _guard_should_pause(
                        bot_id=_bot_id_guard,
                        max_losses=int(getattr(settings, "MAX_CONSECUTIVE_LOSSES", 3)),
                        cooldown_seconds=int(getattr(settings, "CONSECUTIVE_LOSS_COOLDOWN_MINUTES", 120)) * 60,
                    ):
                        logger.info(
                            "[GUARD] Bot %s paused â€” consecutive loss threshold reached. "
                            "Skipping new entry for %s.",
                            _bot_id_guard, symbol,
                        )
                        recorder.record_gate(trace_id, allowed=False, reason_code="CONSECUTIVE_LOSS_COOLDOWN", reason="CONSECUTIVE_LOSS_COOLDOWN", details={})
                        recorder.finalize(trace_id, state_change="HOLD", final_position=st.position)
                        return {"symbol": symbol, "skipped": True, "reason": "CONSECUTIVE_LOSS_COOLDOWN"}
                except Exception as _guard_err:
                    logger.warning("[GUARD] should_pause check failed: %s", _guard_err)

            # 1) Market data
            kl = self.client.klines(symbol=symbol, interval=self.interval, limit=250)

            from app.runner.market_snapshot import MarketSnapshot, claim_candle
            _primary_snapshot = MarketSnapshot.build(
                symbol=symbol,
                timeframe=self.interval,
                candles=kl,
                source=type(self.client).__name__,
            )
            _bot_candle_id = self.context.bot_instance_id if self.context else str(self.run_id or "legacy")
            _evaluate_entry = claim_candle(
                self.db,
                bot_instance_id=_bot_candle_id,
                symbol=symbol,
                timeframe=self.interval,
                close_time=_primary_snapshot.latest_closed_candle_time,
            )
            _snapshot = _primary_snapshot
            if _evaluate_entry:
                _htf = self.context.higher_timeframe if self.context else "4h"
                _htf_rows = self.client.klines(symbol=symbol, interval=_htf, limit=250)
                _snapshot = MarketSnapshot.build(
                    symbol=symbol,
                    timeframe=self.interval,
                    candles=kl,
                    source=type(self.client).__name__,
                    higher_timeframe=_htf,
                    higher_timeframe_candles=_htf_rows,
                )
            kl = list(_snapshot.candles)
            try:
                _last_row = _snapshot.candles[-1]
                _open_ms = int(_last_row.get("openTime") or _last_row.get("open_time")) if isinstance(_last_row, dict) else int(_last_row[0])
                recorder.record_candle(
                    trace_id,
                    open_time=_open_ms,
                    close_time=_snapshot.latest_closed_candle_time,
                )
            except Exception:
                pass

            # âœ… FAST EXIT CHECK
            if getattr(self, "_stop_requested", False):
                logger.info(f"â­ï¸ SKIPPING {symbol}: Stop requested")
                recorder.record_gate(trace_id, allowed=False, reason_code="STOP_REQUESTED", reason="STOP_REQUESTED", details={})
                recorder.finalize(trace_id, state_change="SKIP", final_position=st.position)
                return {"symbol": symbol, "skipped": True, "reason": "STOP_REQUESTED"}

            # â”€â”€ EVENT BLACKOUT GUARD (Point 1 â€” pre-strategy) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # Checked before signal generation so we skip all computation when blocked.
            # Only blocks NEW entries â€” never closes or modifies existing positions.
            try:
                _evt_decision = self.event_blackout_filter.check(symbol=symbol)
                if _evaluate_entry and st.position not in ("LONG", "SHORT") and _evt_decision.is_blocked:
                    try:
                        self.audit.event(
                            event_type="EVENT_BLOCK",
                            run_id=self.run_id,
                            cycle_id=getattr(self, "cycle_id", None),
                            symbol=symbol,
                            action="PRE_STRATEGY_BLACKOUT",
                            details={
                                "reason": _evt_decision.reason,
                                **(_evt_decision.details or {}),
                            },
                            trace_id=trace_id,
                        )
                    except Exception:
                        pass
                    if trace_id:
                        try:
                            recorder = get_trace_recorder()
                            recorder.record_event_block(
                                trace_id,
                                reason=_evt_decision.reason or "EVENT_BLACKOUT",
                                details=_evt_decision.details,
                            )
                            recorder.finalize(trace_id, state_change="EVENT_BLOCKED", final_position="NONE")
                        except Exception:
                            pass
                    return {"symbol": symbol, "skipped": True, "reason": _evt_decision.reason or "EVENT_BLACKOUT"}
            except Exception as _evt_err:
                logger.warning("[EventFilter] check() raised unexpectedly: %s", _evt_err)

            # â”€â”€ POST-EVENT REACTION GATE (Phase 2) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # Only active when REACTION_ALLOW_RISK_INFLUENCE=True (default: False).
            # Extends blackout if market volatility remains elevated after an event.
            try:
                _react_decision = self.reaction_risk_gate.check(symbol=symbol)
                if _evaluate_entry and st.position not in ("LONG", "SHORT") and _react_decision.is_blocked:
                    if trace_id:
                        try:
                            recorder = get_trace_recorder()
                            recorder.record_event_block(
                                trace_id,
                                reason=_react_decision.reason or "POST_EVENT_VOLATILITY",
                                details=_react_decision.details,
                            )
                            recorder.finalize(trace_id, state_change="EVENT_BLOCKED", final_position="NONE")
                        except Exception:
                            pass
                    return {"symbol": symbol, "skipped": True, "reason": _react_decision.reason or "POST_EVENT_VOLATILITY"}
            except Exception as _react_err:
                logger.warning("[ReactionGate] check() raised unexpectedly: %s", _react_err)

            # IOFS Gate 0: shadow logs without blocking; enforce can block only
            # flat-position entries in paper/testnet execution.
            _iofs = self._run_iofs_pre_ensemble(
                symbol,
                trace_id=trace_id,
                current_position=st.position,
            )
            if _evaluate_entry and _iofs.get("blocked"):
                _iofs_result = _iofs["result"]
                try:
                    recorder.record_gate(
                        trace_id,
                        allowed=False,
                        reason_code=f"IOFS_{_iofs_result.reason}",
                        reason=_iofs_result.reason,
                        details=_iofs["details"],
                    )
                    recorder.finalize(trace_id, state_change="IOFS_BLOCKED", final_position="NONE")
                except Exception:
                    pass
                _cs = getattr(self, "_cycle_stats", None)
                if _cs:
                    _cs.record_hold(symbol, 0.0, f"IOFS_{_iofs_result.reason}")
                return {
                    "symbol": symbol,
                    "decision": "HOLD",
                    "skipped": True,
                    "reason": f"IOFS_{_iofs_result.reason}",
                    "iofs": _iofs["details"],
                }

            # âœ… ORCHESTRATOR COMPATIBILITY LAYER
            if self.orchestrator:
                return self._step_symbol_orchestrated(
                    symbol,
                    kl,
                    trace_id,
                    market_snapshot=_snapshot,
                    evaluate_entry=_evaluate_entry,
                )

            if self.context is not None:
                recorder.record_gate(
                    trace_id,
                    allowed=False,
                    reason_code="ERROR_STRATEGY_UNAVAILABLE",
                    reason="Auto Pilot requires the orchestrated strategy path",
                    details={},
                )
                recorder.finalize(trace_id, state_change="ERROR_CONFIGURATION", final_position=st.position)
                return {"symbol": symbol, "decision": "ERROR", "reason": "ERROR_STRATEGY_UNAVAILABLE"}

            # âœ… STRATEGY SAFETY WRAP (never crash runner)
            a_state = None  # Pre-initialize so it's in scope even if get_adaptive_state fails
            try:
                # Phase 3 Section 4-7: Resolve adaptive state from trusted DB sources
                _hint_dd = abs(getattr(self.daily, "realized_pnl", 0.0)) / max(
                    getattr(self, "cached_balance", 0.0) or 1.0, 1.0
                )
                _atr_hint = 1.5  # HEURISTIC: no DB source yet
                try:
                    _atr_val = float(calculate_atr(kl, period=14))
                    _price = float(kl[-1][4]) if kl else 1.0
                    _atr_hint = (_atr_val / _price * 100) if _price > 0 else 1.5
                except Exception:
                    pass

                from app.risk.dynamic_threshold import get_dynamic_threshold_calculator
                try:
                    # Pass bot_id so each bot owns its own rolling confidence window
                    _dyntc = get_dynamic_threshold_calculator(bot_id=_bot_id_guard)
                    _dyn_res = _dyntc.get_threshold(symbol)
                    _base_thr = _dyn_res.threshold
                except Exception:
                    _base_thr = 0.5

                a_state = self.adaptive_engine.get_adaptive_state(
                    config_id=getattr(self, "run_id", "default") or "default",
                    symbol=symbol,
                    drawdown_pct_hint=_hint_dd,
                    current_atr_pct=_atr_hint,
                    active_regime="UNKNOWN",  # Runner doesn't know regime directly
                    base_threshold=_base_thr,
                )

                # Feed strictly separated adaptive params to strategy
                try:
                    res = self.strategy.get_signal(
                        symbol,
                        min_confidence_gate=a_state.min_confidence_gate,
                        drawdown_pct=0.0,  # Strategy no longer receives drawdown
                        strategy_weight_adjustments=a_state.strategy_weight_adjustments,
                        execution_mode=self._effective_execution_mode(),
                    )
                except TypeError:
                    res = self.strategy.get_signal(symbol)
            except Exception as e:
                logger.error(f"[STRATEGY FATAL] {symbol} get_signal crashed: {e}", exc_info=True)
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
            
            # âœ… ADDED: [EVAL] Log for legacy path (mirroring orchestrator logging)
            eval_decision = "EXECUTE" if sig != "HOLD" else "SKIP"
            eval_reason = getattr(res, "reason", "strategy_signal")
            conf_raw = getattr(res, "confidence", 0.0)
            _meta = getattr(res, "meta", {}) or {}

            if sig == "HOLD":
                logger.debug(
                    f"[EVAL] bot={getattr(self, 'run_id', 'unknown')} "
                    f"sym={symbol} sig=HOLD "
                    f"conf_raw={conf_raw:.4f} reason={eval_reason}"
                )
            else:
                _regime   = _meta.get("regime", "?")
                _buy_sc   = _meta.get("buy_score", 0.0)
                _sell_sc  = _meta.get("sell_score", 0.0)
                _thr      = _meta.get("threshold", 0.0)
                _icon     = "ðŸŸ¢" if sig == "BUY" else "ðŸ”´"
                logger.info(
                    f"{_icon} [{sig} SIGNAL] {symbol} | "
                    f"conf={conf_raw:.3f} thr={_thr:.3f} | "
                    f"buy={_buy_sc:.3f} sell={_sell_sc:.3f} | "
                    f"regime={_regime} | reason={eval_reason}"
                )

            # âœ… STORE CONFIDENCE FOR THIS SYMBOL (used later on CLOSE)
            try:
                self.last_signal_confidence[symbol] = float(res.confidence or 0.0)
            except Exception:
                self.last_signal_confidence[symbol] = 0.0

            # ---- Capture active strategies for PerformanceTracker feedback ----
            # Reads active_strategies from ensemble meta (set by MasterEnsembleStrategy v2)
            try:
                _signal_meta = getattr(res, "meta", None) or {}
                
                # ---- Regime Change Detection & Audit Logging ----
                current_regime = _signal_meta.get("regime")
                if current_regime and current_regime != "UNKNOWN":
                    if st.last_regime != "UNKNOWN" and st.last_regime != current_regime:
                        self.audit.event(
                            event_type="REGIME_CHANGE",
                            symbol=symbol,
                            action=current_regime,
                            details={
                                "previous": st.last_regime,
                                "new": current_regime,
                                "confidence": _signal_meta.get("regime_confidence", 0.0),
                                "atr_pct": _signal_meta.get("atr_pct", 0.0),
                            },
                            trace_id=trace_id
                        )
                    st.last_regime = current_regime
                    st.last_regime_confidence = float(_signal_meta.get("regime_confidence", 0.0))

                _active_strats = _signal_meta.get("active_strategies")
                if _active_strats:
                    st.last_active_strategies = list(_active_strats)
            except Exception as e:
                logger.error(f"[REGIME] Error checking regime change: {e}", exc_info=True)


            self.audit.event(
                event_type="STRATEGY_SIGNAL",
                symbol=symbol,
                # âœ… IMPORTANT: log the final signal (forced override included)
                action=sig,
                details={
                    "strategy": getattr(self.strategy, "name", "unknown"),
                    "confidence": res.confidence,
                    "reason": res.reason,
                    "meta": res.meta,
                    "policy_reason": res.reason,
                },
                trace_id=trace_id,  # âœ… Link audit to trace
            )
            
            # âœ… Record Strategy Signals
            recorder.record_strategies(
                trace_id,
                signals=[
                    StrategySignal(
                        strategy_name=getattr(self.strategy, "name", "unknown"),
                        signal=sig,
                        confidence=float(res.confidence or 0.0),
                        reason=str(res.reason),
                        meta=_meta,
                    )
                ],
                chosen_strategy=getattr(self.strategy, "name", "unknown"),
                final_signal=sig,
                final_confidence=float(res.confidence or 0.0),
                # Frozen indicator snapshot â€” extracted from ensemble meta (pre-execution)
                adx=float(_meta["adx"]) if "adx" in _meta else None,
                atr_pct=float(_meta["atr_pct"]) if "atr_pct" in _meta else None,
                ma_slope=float(_meta["ma_slope"]) if "ma_slope" in _meta else None,
                compression_ratio=float(_meta["compression_ratio"]) if "compression_ratio" in _meta else None,
                breakout_pressure=float(_meta["breakout_pressure"]) if "breakout_pressure" in _meta else None,
                buy_score=float(_meta["buy_score"]) if "buy_score" in _meta else None,
                sell_score=float(_meta["sell_score"]) if "sell_score" in _meta else None,
                threshold=float(_meta["threshold"]) if "threshold" in _meta else None,
                active_strategy_count=len(_meta["active_strategies"]) if "active_strategies" in _meta else None,
                htf_opposed=bool(_meta["htf_opposed"]) if "htf_opposed" in _meta else None,
            )

            price = self.client.last_price(symbol)
            if price is None:
                raise ValueError("price_unavailable")

            # âœ… Record Market Snapshot
            acc_balance_map = self.get_account_balance()
            equity_val = float(acc_balance_map if isinstance(acc_balance_map, (int, float)) else acc_balance_map.get("equity", 0.0))
            
            # Calculate Margin Level (Best effort)
            margin_level = 999.0
            margin_used = 0.0
            _margin_fetch_ok = True
            try:
                acc_data = self.client.account()
                if isinstance(acc_data, dict):
                    maint = float(acc_data.get("totalMaintMargin", 0.0) or 0.0)
                    bal = float(acc_data.get("totalMarginBalance", 0.0) or 0.0)
                    margin_used = maint
                    if maint > 0:
                        margin_level = (bal / maint) * 100
            except Exception:
                _margin_fetch_ok = False

            # âœ… Extract Always-On Risk Metrics
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
                regime_state=_meta.get("regime", "STANDARD"),
                regime_confidence=float(_meta.get("regime_confidence", 0.0)),
                kill_switch_state=kill_switch,
                exposure_freeze=exposure_freeze,
                portfolio_risk_budget=portfolio_risk_budget,
                portfolio_risk_used=portfolio_risk_used,
                # Frozen adaptive engine state snapshot (pre-execution)
                aggressiveness_score=float(a_state.aggressiveness_score) if a_state is not None else None,
                confidence_gate_modifier=float(a_state.confidence_gate_modifier) if a_state is not None else None,
                size_multiplier=float(a_state.size_multiplier) if a_state is not None else None,
                rolling_win_rate=float(a_state.rolling_win_rate) if a_state is not None else None,
                rolling_expectancy=float(a_state.rolling_expectancy) if a_state is not None else None,
                loss_streak=int(a_state.loss_streak) if a_state is not None else None,
            )
            
            # âœ… FAST EXIT CHECK
            if getattr(self, "_stop_requested", False):
                return {"symbol": symbol, "skipped": True, "reason": "STOP_REQUESTED"}

            st = self.state[symbol]
            now_ms = int(time.time() * 1000)
            st.last_checked_ms = now_ms
            st.last_signal = sig

            cooldown_ok = (now_ms - st.last_trade_ms) >= (
                settings.COOLDOWN_SECONDS * 1000
            )

            # âœ… SL cooldown (after stop-loss)
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
            
            # âœ… FAST EXIT CHECK
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

            self._reconcile_entry_protection(symbol, pos_amt, st)

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

            # -------------------------------------------------------------------------
            # âœ… UNIFIED POLICY ENGINE EVALUATION
            # -------------------------------------------------------------------------

            # Get current timestamp and price (needed for Policy evaluations and later logic)
            now_ms = int(time.time() * 1000)
            price = self.client.last_price(symbol)
            if price is None:
                price = 0.0  # fallback
            
            # 1. Determine Effective Signal â€” PositionManager price ticks override strategy
            effective_signal = sig
            exit_reason = ""

            if st.position in {"LONG", "SHORT"}:
                # Route current price through PositionManager lifecycle
                current_atr = float(calculate_atr(kl, period=14))
                pm_action = self.position_manager.update_price(symbol, price, current_atr)

                if pm_action in {"HIT_STOP", "HIT_TP2", "TIME_EXIT"}:
                    # Deterministic forced close â€” no strategy signal needed
                    effective_signal = "CLOSE"
                    if pm_action == "HIT_STOP":
                        st.last_stop_ms = now_ms
                        _stop_pos = self.position_manager.get_position(symbol)
                        if _stop_pos is not None and _stop_pos.sl.trailing_last_stop_price is not None:
                            exit_reason = ExitReason.TRAILING_SL
                        elif _stop_pos is not None and _stop_pos.sl.is_break_even and _stop_pos.sl.be_buffer_amount:
                            # FIX-D: post-TP1 buffered stop (20% of SL distance) â€” not a raw SL
                            exit_reason = ExitReason.BREAK_EVEN_BUFFER
                        elif _stop_pos is not None and _stop_pos.sl.is_break_even:
                            exit_reason = ExitReason.BREAK_EVEN
                        else:
                            exit_reason = ExitReason.SL
                    elif pm_action == "HIT_TP2":
                        exit_reason = ExitReason.TP2
                    else:  # TIME_EXIT
                        exit_reason = ExitReason.TIME_EXIT
                elif pm_action == "HIT_TP1":
                    exit_reason = "HIT_TP1"
                    pm_pos = self.position_manager.get_position(symbol)
                    if pm_pos is not None:
                        import logging as _s2_log
                        _s2_log = _s2_log.getLogger(__name__)
                        try:
                            tp1_result = self.executor.execute_tp1_partial_close(
                                symbol=symbol,
                                live_qty=abs(float(st.entry_qty or 0.0)),
                                position_side=st.position,
                                sl_price=float(pm_pos.sl.current_stop),
                                tp_price=float(pm_pos.tp.tp2_price),
                                sl_order_id=pm_pos.sl.sl_order_id,
                                tp_order_id=pm_pos.sl.tp_order_id,
                                tp1_fraction=float(pm_pos.tp.tp1_close_fraction),
                                position_manager=self.position_manager,
                            )
                            if tp1_result.get("promoted"):
                                self.position_manager.close_position(symbol, "TP1_PROMOTED_FULL_CLOSE")
                                effective_signal = "CLOSE"
                                exit_reason = ExitReason.TP1
                            elif not tp1_result.get("skipped"):
                                # Previously this site only mutated the in-memory qty:
                                # the remaining quantity was lost on restart and no
                                # TP1 fill evidence was written.
                                self._persist_tp1_outcome(
                                    symbol=symbol,
                                    st=st,
                                    tp1_result=tp1_result,
                                    fallback_price=price,
                                )
                        except Exception as _s2_err:
                            _s2_log.error(f"{symbol} HIT_TP1 (site2) execute_tp1_partial_close error: {_s2_err}", exc_info=True)

            # 2. Build Policy Context
            # Resolve trade amount settings
            t_mode = "atr_risk"
            t_val = 0.0
            if self.context and hasattr(self.context, 'get_trade_amount_settings'):
                t_mode, t_val = self.context.get_trade_amount_settings()
                # ðŸ” DEBUG: Log what values context provides
                import sys
                print(f"[RUNNER DEBUG] {symbol}: Context provided trade_amount_mode={t_mode}, value={t_val}", file=sys.stderr)
                print(f"  Context allocation_type={self.context.allocation_type}, allocation_value={self.context.allocation_value}", file=sys.stderr)

            # FIX 1: Resolve actual per-symbol execution leverage (mirrors executor._size_qty).
            # DEFAULT_LEVERAGE is a fallback; SYMBOL_LEVERAGE_MAP takes precedence per symbol.
            # The ATR cap must use the same leverage the exchange will actually execute at.
            _lev_map = parse_leverage_map(settings.SYMBOL_LEVERAGE_MAP)
            _symbol_leverage = float(leverage_for(
                symbol, _lev_map, settings.DEFAULT_LEVERAGE, settings.MIN_LEVERAGE
            ))

            # FIX 2: Derive account_risk_pct from the bot's configured risk_level instead of
            # relying on the PolicyContext dataclass default (1.0% which caps like LOW for all users).
            # LOW=1.0% / MEDIUM=2.0% / HIGH=3.0%
            _RISK_PCT_MAP = {"low": 1.0, "medium": 2.0, "high": 3.0}
            _ctx_risk_str = (
                self.context.risk_level.lower()
                if self.context and getattr(self.context, "risk_level", None)
                else "low"
            )
            _account_risk_pct = _RISK_PCT_MAP.get(_ctx_risk_str, 1.0)

            # Resolve risk settings
            _dd_ctx2 = self._get_drawdown_context()
            ctx = PolicyContext(
                # Symbol & Signal
                symbol=symbol,
                signal=effective_signal,
                # B-5 Fix: Never default strategy confidence to 1.0 â€” use 0.0 if unavailable.
                confidence=float(res.confidence or 0.0) if 'res' in locals() and hasattr(res, 'confidence') else 0.0,

                # State
                position=st.position,
                adds=st.adds,
                last_trade_ms=st.last_trade_ms,
                last_stop_ms=int(getattr(st, "last_stop_ms", 0) or 0),
                pending_open=_norm_pending(st.pending_open),
                reentry_confirm_signal=getattr(st, "reentry_confirm_signal", None),
                reentry_confirm_count=getattr(st, "reentry_confirm_count", 0),

                # Market
                entry_price=float(st.entry_price or price) if st.position != "NONE" else price,
                atr=float(calculate_atr(kl, period=14)),
                baseline_atr=float(calculate_atr(kl, period=14)),
                short_term_atr=float(calculate_atr(kl[-4:], period=3)) if len(kl) >= 4 else float(calculate_atr(kl, period=14)),
                expected_slippage_pct=self.executor.estimate_slippage(trade_usdt) if hasattr(self.executor, "estimate_slippage") else 0.0,

                # Account
                equity=self.get_account_balance(),
                daily_realized_pnl=self.daily.realized_pnl,
                daily_trade_count=self.daily.trade_count,
                open_positions_count=sum(1 for s in self.state.values() if s.position in ("LONG", "SHORT")),

                # D-2/D-3: pass actual SL/TP prices from symbol state
                stop_loss_price=float(st.current_stop_loss or 0.0),
                take_profit_price=float(getattr(st, "tp_price", 0.0) or 0.0),

                # Config â€” use actual per-symbol leverage (FIX 1)
                leverage=_symbol_leverage,
                stop_loss_pct=float(getattr(settings, "STOP_LOSS_PCT", 0.02)),
                take_profit_pct=float(getattr(settings, "TAKE_PROFIT_PCT", 0.03)),
                cooldown_seconds=getattr(settings, "COOLDOWN_SECONDS", 120),
                sl_cooldown_seconds=getattr(settings, "SL_COOLDOWN_SECONDS", 1800),
                max_adds=getattr(settings, "MAX_ADDS_PER_POSITION", 0),
                trade_mode=getattr(settings, "TRADE_MODE", "normal"),
                min_hold_time_seconds=int(getattr(settings, "MIN_HOLD_TIME_SECONDS", 0)),
                reentry_confirmations=getattr(settings, "REENTRY_CONFIRMATION_COUNT", 1),

                # Risk Limits (from runner settings)
                account_risk_pct=_account_risk_pct,  # FIX 2: explicit, derived from risk_level
                max_daily_loss=self.daily_max_loss,
                max_daily_trades=self.max_trades_daily,
                max_open_positions=self.max_open_positions,
                kill_switch=self.daily.kill,
                execution_mode=getattr(settings, "EXECUTION_MODE", "paper"),

                # Trade Amount
                trade_amount_mode=t_mode,
                trade_amount_value=t_val,

                # Context
                now_ms=int(time.time() * 1000),
                broker_id=self.context.broker_account_id if self.context else "BINANCE",
                circuit_key=self._circuit_id if hasattr(self, "_circuit_id") else None,

                # Drawdown & consecutive loss safety
                **_dd_ctx2,
            )
            
            # â”€â”€ EVENT BLACKOUT GUARD (Point 2 â€” pre-execution safety net) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # Catches edge cases where a blackout activated after Point 1 was checked.
            try:
                _evt2 = self.event_blackout_filter.check(symbol=symbol)
                if _evt2.is_blocked:
                    logger.warning("[EventFilter] Execution-time block symbol=%s reason=%s", symbol, _evt2.reason)
                    try:
                        self.audit.event(
                            event_type="EVENT_BLOCK",
                            run_id=self.run_id,
                            cycle_id=getattr(self, "cycle_id", None),
                            symbol=symbol,
                            action="PRE_EXECUTION_BLACKOUT",
                            details={
                                "reason": _evt2.reason,
                                **(_evt2.details or {}),
                            },
                            trace_id=trace_id,
                        )
                    except Exception:
                        pass
                    if trace_id:
                        try:
                            recorder.record_event_block(
                                trace_id,
                                reason=_evt2.reason or "EVENT_BLACKOUT_EXEC",
                                details=_evt2.details,
                            )
                            recorder.finalize(
                                trace_id,
                                state_change="EVENT_BLOCKED_EXECUTION",
                                final_position=st.position if st.position in ("LONG", "SHORT") else "NONE",
                            )
                        except Exception:
                            pass
                    return {"symbol": symbol, "skipped": True, "reason": _evt2.reason or "EVENT_BLACKOUT_EXEC"}
            except Exception as _evt2_err:
                logger.warning("[EventFilter] execution-time check() raised: %s", _evt2_err)

            # 3. Evaluate
            policy = self.policy_engine.evaluate(ctx)

            # â”€â”€ Observability Fix 1+2: record_gate() â€” persist PolicyEngine gate decision â”€â”€
            # Before this fix, gate_reason='' and gate_details_json='{}' for all rows.
            # Now captures: confidence, policy floor, dynamic threshold, gap, and reason code.
            if trace_id:
                try:
                    _pe_floor = float(getattr(self.policy_engine, "min_confidence", 0.40))
                    _pe_gate_details: dict = {
                        "confidence": float(ctx.confidence),
                        "policy_floor": _pe_floor,
                        "confidence_gap_floor": round(float(ctx.confidence) - _pe_floor, 4),
                        "policy_reason": policy.reason or "",
                    }
                    # Include dynamic threshold if it was computed this cycle
                    _pe_dyn_thr = locals().get("_base_thr")
                    if _pe_dyn_thr is not None:
                        _pe_gate_details["dynamic_threshold"] = float(_pe_dyn_thr)
                        _pe_gate_details["confidence_gap_dyn"] = round(float(ctx.confidence) - float(_pe_dyn_thr), 4)
                    recorder.record_gate(
                        trace_id,
                        allowed=bool(policy.allowed),
                        reason_code=policy.reason_code.name if policy.reason_code else "UNKNOWN",
                        reason=policy.reason or "",
                        details=_pe_gate_details,
                    )
                except Exception:
                    pass
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

            # â”€â”€ 3.5: ML Entry Quality Scorer (Step 5D-2) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # Additive gate between AdaptiveEngine and risk gate.
            # ML can ADD a block; it cannot override policy.allowed=True.
            # Graceful degradation: scorer returns None if disabled/unavailable.
            _ml_score: Optional[float] = None
            _ml_action: Optional[str] = None
            _ml_fv: Optional[dict] = None

            # Only score if the policy engine would allow an entry (BUY/SELL intent)
            _policy_wants_entry = (
                policy.allowed
                and hasattr(policy, "action")
                and str(getattr(policy.action, "name", "")) in {
                    "OPEN_LONG", "OPEN_SHORT", "ADD_LONG", "ADD_SHORT",
                    "FLIP_TO_LONG", "FLIP_TO_SHORT",
                }
            )

            if self.ml_scorer.enabled and _policy_wants_entry:
                try:
                    _sig_meta = _signal_meta if "_signal_meta" in locals() and _signal_meta else {}
                    _ml_res = res if "res" in locals() else None
                    _ml_price = float(price) if "price" in dir() and price else 0.0

                    # Real margin_level â€” same formula as Path 1 (_step_symbol_orchestrated).
                    # margin_used and equity_val are already resolved above for record_market().
                    # Fallback 0.0 when margin_used=0 (no open debt) or account() call failed.
                    _ml_p2_margin_level = (
                        (equity_val / max(margin_used, 0.01) * 100.0)
                        if margin_used > 0
                        else 0.0
                    )
                    if not _margin_fetch_ok:
                        import logging as _ml2_log
                        _ml2_log.getLogger(__name__).debug(
                            "[MLScorer] %s: margin_level fallback 0.0 â€” account() call failed, "
                            "real margin level unavailable for ML feature vector",
                            symbol,
                        )

                    _ml_fv = self.ml_scorer.build_feature_vector(
                        symbol=symbol,
                        timeframe=getattr(settings, "DEFAULT_INTERVAL", "15m"),
                        ts=datetime.now(timezone.utc).isoformat(),
                        regime_state=st.last_regime,
                        regime_confidence=st.last_regime_confidence,
                        last_price=_ml_price,
                        mark_price=_ml_price,
                        margin_level=_ml_p2_margin_level,
                        drawdown_pct=abs(self.daily.realized_pnl) / max(float(self.cached_balance), 1.0) if self.cached_balance else 0.0,
                        open_positions_count=int(ctx.open_positions_count),
                        portfolio_risk_used=float(getattr(ctx, "portfolio_risk_used", 0.0)),
                        final_confidence=float(_ml_res.confidence or 0.0) if _ml_res is not None and hasattr(_ml_res, "confidence") else 0.0,
                        chosen_strategy=getattr(_ml_res, "strategy_name", None) if _ml_res is not None else None,
                        side="LONG" if str(getattr(policy.action, "name", "")).endswith("LONG") else "SHORT",
                        adx=_sig_meta.get("adx"),
                        atr_pct=_sig_meta.get("atr_pct"),
                        ma_slope=_sig_meta.get("ma_slope"),
                        compression_ratio=_sig_meta.get("compression_ratio"),
                        breakout_pressure=_sig_meta.get("breakout_pressure"),
                        buy_score=_sig_meta.get("buy_score"),
                        sell_score=_sig_meta.get("sell_score"),
                        threshold=_sig_meta.get("threshold"),
                        active_strategy_count=len(_sig_meta.get("active_strategies", [])) or None,
                        htf_opposed=bool(_sig_meta.get("htf_opposed")) if "htf_opposed" in _sig_meta else None,
                        open_price=float(_ml_price),
                        stop_loss_price=float(getattr(policy, "sl_plan", None)) if getattr(policy, "sl_plan", None) is not None else None,
                        tp_plan=float(getattr(policy, "tp_plan", None)) if getattr(policy, "tp_plan", None) is not None else None,
                        aggressiveness_score=float(a_state.aggressiveness_score) if a_state is not None else None,
                        confidence_gate_modifier=float(a_state.confidence_gate_modifier) if a_state is not None else None,
                        size_multiplier=float(a_state.size_multiplier) if a_state is not None else None,
                        rolling_win_rate=float(a_state.rolling_win_rate) if a_state is not None else None,
                        rolling_expectancy=float(a_state.rolling_expectancy) if a_state is not None else None,
                        loss_streak=int(a_state.loss_streak) if a_state is not None else None,
                    )
                    _ml_score = self.ml_scorer.score(_ml_fv)
                    _ml_action = self.ml_scorer.get_action(_ml_score)

                    # Log prediction to JSONL
                    self.ml_scorer.log_prediction(
                        trace_id=trace_id,
                        symbol=symbol,
                        score=_ml_score,
                        action=_ml_action,
                        model_version=self.ml_scorer.model_version,
                        threshold=self.ml_scorer.threshold,
                        feature_vector=_ml_fv,
                    )

                    if _ml_action == "BLOCK":
                        _block_kind = (
                            "FLOOR_BLOCK"
                            if (
                                _ml_score is not None
                                and getattr(self.ml_scorer, "_hard_block_floor", 0.0) > 0.0
                                and _ml_score < self.ml_scorer._hard_block_floor
                            )
                            else "THRESHOLD_BLOCK"
                        )
                        logger.info(
                            "[MLScorer] %s: %s â€” score=%.3f "
                            "(floor=%.2f threshold=%.2f model=%s)",
                            symbol, _block_kind, _ml_score,
                            getattr(self.ml_scorer, "_hard_block_floor", 0.0),
                            self.ml_scorer.threshold, self.ml_scorer.model_version,
                        )
                        # Override policy to block â€” mark as not allowed
                        policy.allowed = False  # type: ignore[assignment]
                        policy.reason = f"ML_{_block_kind}(score={_ml_score:.3f})"  # type: ignore[assignment]
                    elif _ml_action == "SHADOW" and _ml_score is not None:
                        logger.debug(
                            "[MLScorer] %s: SHADOW score=%.3f < threshold=%.2f â€” logging only",
                            symbol, _ml_score, self.ml_scorer.threshold,
                        )
                    elif _ml_action == "ALLOW" and _ml_score is not None:
                        logger.debug(
                            "[MLScorer] %s: ALLOW score=%.3f >= threshold=%.2f",
                            symbol, _ml_score, self.ml_scorer.threshold,
                        )

                except Exception as _ml_err:
                    logger.warning("[MLScorer] %s: scoring failed, skipping: %s", symbol, _ml_err)
                    _ml_score = None
                    _ml_action = None

            # Record ML score in trace regardless of shadow/active mode
            if trace_id:
                recorder.record_ml_score(
                    trace_id,
                    score=_ml_score,
                    action=_ml_action if _ml_action else ACTION_SKIP,
                    model_version=self.ml_scorer.model_version if self.ml_scorer.enabled else None,
                    threshold=self.ml_scorer.threshold if self.ml_scorer.enabled else None,
                )
            # â”€â”€ End ML scoring â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

            # 4. Apply State Updates (regardless of execution)
            st.pending_open = policy.pending_open or "NONE"
            st.reentry_confirm_signal = policy.reentry_confirm_signal
            st.reentry_confirm_count = policy.reentry_confirm_count
            
            # 5. Handle Decision
            decision = policy.reason_code.name if policy.reason_code else "HOLD"
            # Map policy action to executor signal
            exec_signal = "HOLD"
            trade_usdt = 0.0

            # B-8 Fix: Sizing failures must be surfaced to audit and user dashboard.
            # When policy is blocked due to min-notional failure, emit a structured
            # audit event so the user sees why the bot is not trading.
            if not policy.allowed and policy.reason_code is not None:
                from app.policy.policy_engine import ReasonCode as _RC
                _sizing_reason_codes = {_RC.MIN_NOTIONAL_NOT_MET, _RC.SIZE_ZERO, _RC.PRICE_INVALID}
                if policy.reason_code in _sizing_reason_codes:
                    _sz_msg = (
                        "Trade amount too small for exchange minimum. "
                        "Increase trade amount to at least 50 USDT per position."
                    )
                    logger.error(
                        "SIZING_FAILURE_TRADE_SKIPPED: symbol=%s reason_code=%s reason=%s â€” %s",
                        symbol, policy.reason_code.name, policy.reason, _sz_msg,
                    )
                    try:
                        self.audit.event(
                            event_type="SIZING_FAILURE",
                            run_id=self.run_id,
                            cycle_id=getattr(self, "cycle_id", None),
                            symbol=symbol,
                            details={
                                "error_code": "SIZING_FAILURE_MIN_NOTIONAL",
                                "reason_code": policy.reason_code.name,
                                "reason": policy.reason or "",
                                "requested_budget": float(getattr(self, "trade_usdt", 0)),
                                "bot_instance_id": self.context.bot_instance_id if self.context else None,
                                "user_id": self.context.user_id if self.context else None,
                                "user_message": _sz_msg,
                                "sizing_details": policy.details or {},
                            },
                        )
                        logger.debug("SIZING_FAILURE_DASHBOARD_ALERT_SET symbol=%s", symbol)
                    except Exception as _sz_audit_err:
                        logger.warning(
                            "SIZING_FAILURE audit event failed: %s", _sz_audit_err
                        )
                    # Update user-facing health status (Section F-2)
                    self._update_bot_health_from_reason_code(
                        reason_code=policy.reason_code.name if policy.reason_code else None,
                        reason=policy.reason,
                    )
                else:
                    # Other policy blocks still need to surface as PAUSED_* statuses.
                    self._update_bot_health_from_reason_code(
                        reason_code=policy.reason_code.name if policy.reason_code else None,
                        reason=policy.reason,
                    )

            if policy.allowed:
                # If we are allowed to trade, mark as TRADING unless another safety pause is active.
                self._set_bot_health(
                    status="TRADING",
                    message="Bot is active and monitoring the market.",
                    reason_code=None,
                    recommended_action=None,
                )
                trade_usdt = policy.risk_usdt  # This is the computed budget/risk size

                # ---- LAYER 5.5: HTF Bias Sizing ----------------------------------
                # If HTF trend opposes the signal, cut size by 50%
                if 'res' in locals() and getattr(res, "meta", {}).get("htf_opposed"):
                    logger.info(f"[HTF BIAS] {symbol}: trend opposes signal, halving size ({trade_usdt:.2f} â†’ {trade_usdt * 0.5:.2f} USDT)")
                    trade_usdt *= 0.5
                # ------------------------------------------------------------------

                # ---- LAYER 6: Risk Compression -----------------------------------
                # Compress trade size and leverage based on drawdown, loss streak,
                # and current volatility. This is a final multiplier AFTER policy sizing.
                try:
                    from app.risk.risk_compression import compute_compression, apply_compression
                    _dd_pct = abs(self.daily.realized_pnl) / max(self.cached_balance, 1.0) if self.cached_balance > 0 else 0.0
                    _loss_streak = getattr(self, "_loss_streak", 0)
                    _atr_pct = float(kl[-1][4]) if kl else 1.5  # fallback ATR% estimate
                    # Use ATR% if we can compute it properly
                    try:
                        _atr_val = float(calculate_atr(kl, period=14))
                        _price = float(kl[-1][4]) if kl else 1.0
                        _atr_pct = (_atr_val / _price * 100) if _price > 0 else 1.5
                    except Exception:
                        _atr_pct = 1.5
                    _compression = compute_compression(
                        adaptive_size_multiplier=a_state.size_multiplier,
                        adaptive_leverage_multiplier=a_state.leverage_multiplier,
                        atr_pct=_atr_pct,
                    )
                    if _compression.is_hard_blocked:
                        logger.info(
                            f"[COMPRESSION] {symbol}: BLOCKED â€” {_compression.block_reason} "
                            f"(ATR%={_atr_pct:.2f})"
                        )
                        trade_usdt = 0.0
                        exec_signal = "HOLD"
                    elif _compression.risk_multiplier < 1.0:
                        trade_usdt, _ = apply_compression(trade_usdt, 1.0, _compression)
                        logger.info(
                            f"[COMPRESSION] {symbol}: sizeÃ—{_compression.risk_multiplier:.2f} "
                            f"(DD={_dd_pct:.1%} ATR%={_atr_pct:.2f}) "
                            f"â†’ {trade_usdt:.2f} USDT"
                        )
                except Exception as _ce:
                    logger.warning(f"[COMPRESSION] {symbol}: failed, using uncompressed size: {_ce}")
                # ------------------------------------------------------------------

                if policy.action == PolicyAction.OPEN_LONG:
                    exec_signal = "BUY"
                    decision = "OPEN_LONG"
                elif policy.action == PolicyAction.OPEN_SHORT:
                    exec_signal = "SELL"
                    decision = "OPEN_SHORT"
                elif policy.action == PolicyAction.ADD_LONG:
                    exec_signal = f"ADD_LONG_{st.adds + 1}"
                    decision = "ADD_LONG"
                elif policy.action == PolicyAction.ADD_SHORT:
                    exec_signal = f"ADD_SHORT_{st.adds + 1}"
                    decision = "ADD_SHORT"
                elif policy.action == PolicyAction.CLOSE:
                    exec_signal = "CLOSE"
                    decision = f"CLOSE_{exit_reason if exit_reason else 'STRATEGY'}"
                elif policy.action == PolicyAction.FLIP_TO_LONG:
                    exec_signal = "BUY"
                    decision = "FLIP_LONG"
                elif policy.action == PolicyAction.FLIP_TO_SHORT:
                    exec_signal = "SELL"
                    decision = "FLIP_SHORT"
            else:
                if sig != "HOLD":
                    logger.info(f"[POLICY] {symbol}: BLOCKED â€” {policy.reason_text}")
                decision = f"BLOCKED_{policy.reason_code.name if policy.reason_code else 'UNKNOWN'}"
                exec_signal = "HOLD"



            event_news_influence = None
            if (
                exec_signal in {"BUY", "SELL"}
                and policy.allowed
                and policy.action in {PolicyAction.OPEN_LONG, PolicyAction.OPEN_SHORT}
            ):
                try:
                    influence_engine = getattr(self, "_event_news_influence_engine", None)
                    if influence_engine is None:
                        influence_engine = EventNewsInfluenceEngine(self.db)
                        self._event_news_influence_engine = influence_engine
                    event_news_influence = influence_engine.evaluate(
                        symbol=symbol,
                        trace_id=trace_id,
                        side=exec_signal,
                        trade_usdt=trade_usdt,
                        confidence=float(confidence) if "confidence" in locals() else None,
                    )
                    if event_news_influence.applied_action == "SIZE_REDUCTION" and event_news_influence.execution_impact_allowed:
                        before_usdt = trade_usdt
                        multiplier = max(0.75, min(1.0, float(event_news_influence.size_multiplier or 1.0)))
                        trade_usdt = min(trade_usdt, trade_usdt * multiplier)
                        logger.info(
                            "[EVENT_NEWS_INFLUENCE] %s: RISK_LITE size %.2f -> %.2f (x%.2f) reason=%s",
                            symbol,
                            before_usdt,
                            trade_usdt,
                            multiplier,
                            event_news_influence.reason,
                        )
                    elif event_news_influence.applied_action == "DELAY_ENTRY" and event_news_influence.execution_impact_allowed:
                        delay_seconds = max(0, min(300, int(event_news_influence.delay_seconds or 0)))
                        logger.info(
                            "[EVENT_NEWS_INFLUENCE] %s: RISK_LITE delayed entry for %ss reason=%s",
                            symbol,
                            delay_seconds,
                            event_news_influence.reason,
                        )
                        decision = "DELAYED_EVENT_NEWS"
                        exec_signal = "HOLD"
                        trade_usdt = 0.0
                    elif event_news_influence.applied_action == "CONFIDENCE_PENALTY":
                        logger.info(
                            "[EVENT_NEWS_INFLUENCE] %s: confidence penalty recorded only (no runner mutation) reason=%s",
                            symbol,
                            event_news_influence.reason,
                        )
                except Exception as _eni_err:
                    logger.warning("[EVENT_NEWS_INFLUENCE] %s: evaluation failed safely: %s", symbol, _eni_err)

            # Audit Decision
            self.audit.event(
                event_type="DECISION",
                run_id=self.run_id,
                symbol=symbol,
                action=decision,
                details={
                    "signal": sig,
                    "effective_signal": effective_signal,
                    "policy_action": policy.action.name,
                    "reason": policy.reason,
                    "sub_reason": policy.reason_code.name if policy.reason_code else None,
                    "sizing": {
                        "qty": policy.quantity,
                        "notional": policy.notional, 
                        "usdt_budget": policy.risk_usdt
                    },
                    "risk_metrics": {
                        "daily_pnl": self.daily.realized_pnl,
                        "open_positions": ctx.open_positions_count
                    }
                },
                trace_id=trace_id
            )

            # Record Intent â€” include full sizing/cap details from PolicyDecision so
            # decision_traces.sizing_json persists cap_applied, account_risk_pct,
            # leverage_used_for_cap, base_margin, final_margin, and cap_reason.
            _sizing_payload = {"trade_usdt": trade_usdt}
            if policy.details:
                _sizing_payload.update(policy.details)
            if event_news_influence is not None:
                _sizing_payload["event_news_influence"] = {
                    "mode": event_news_influence.mode,
                    "applied_action": event_news_influence.applied_action,
                    "size_multiplier": event_news_influence.size_multiplier,
                    "confidence_penalty": event_news_influence.confidence_penalty,
                    "delay_seconds": event_news_influence.delay_seconds,
                    "reason": event_news_influence.reason,
                    "ledger_id": event_news_influence.ledger_id,
                }
            recorder.record_intent(
                trace_id,
                action=decision,
                sizing=_sizing_payload,
                sl_plan=float(policy.sl_plan) if getattr(policy, "sl_plan", None) is not None else None,
                tp_plan=float(policy.tp_plan) if getattr(policy, "tp_plan", None) is not None else None,
            )

            # Check Invariants (Always-on safety)
            checker = get_invariant_checker()
            checker.check_all(
                symbol=symbol,
                trace_id=trace_id,
                run_id=self.run_id,
                bot_instance_id=self.context.bot_instance_id if self.context else None,
                action=exec_signal,
                kill_switch_active=self.daily.kill,
                exposure_freeze=False, # Handled by PolicyEngine allowed check basically
                gate_blocked=not policy.allowed,
                has_intent=policy.allowed,
            )

            # 5) Execute
            if exec_signal not in {"BUY", "SELL", "CLOSE"}:
                exec_result = ExecResult(
                    "NO_TRADE", {"reason": "noop", "signal": exec_signal}
                )
            else:
                # ---- LAYER 7: Execution Filter -----------------------------------
                # Hard gate: spread, liquidity, volatility spike.
                # Runs ONLY for new orders (BUY/SELL), not for CLOSE.
                _exec_blocked = False
                if exec_signal in {"BUY", "SELL"}:
                    try:
                        from app.execution.execution_filter import (
                            check_execution, build_atr_history_from_klines
                        )
                        _ticker = self.executor.client.get_ticker(symbol)
                        _bid = float(_ticker.get("bidPrice", 0) or 0)
                        _ask = float(_ticker.get("askPrice", 0) or 0)
                        _vol_15m = float(_ticker.get("quoteVolume", 1_000_000) or 1_000_000)
                        _ts_ms = int(_ticker.get("closeTime", 0) or 0)
                        _atr_hist = build_atr_history_from_klines(kl) if kl else []
                        _filter = check_execution(
                            symbol=symbol,
                            current_price=price,
                            bid=_bid,
                            ask=_ask,
                            volume_usdt_15m=_vol_15m,
                            atr_history=_atr_hist,
                            data_timestamp_ms=_ts_ms,
                            spread_history=st.spread_history if st.spread_history is not None else [],
                        )
                        # Save updated spread history back to SymbolState
                        st.spread_history = _filter.updated_spread_history

                        if not _filter.allowed:
                            logger.info(
                                f"[EXEC FILTER] {symbol}: BLOCKED â€” {_filter.block_reason}"
                            )
                            _exec_blocked = True
                            exec_result = ExecResult(
                                "EXEC_FILTER_BLOCKED",
                                {"reason": _filter.block_reason, "checks": _filter.checks}
                            )
                    except Exception as _fe:
                        logger.warning(f"[EXEC FILTER] {symbol}: filter check failed, allowing: {_fe}")
                # ------------------------------------------------------------------

                if not _exec_blocked:
                    _lev_mult = _compression.leverage_multiplier if '_compression' in locals() and _compression else 1.0

                    # FIX: Optimistic state lock â€” mark trade BEFORE calling execute_signal.
                    # This ensures st.last_trade_ms is set even if execute_signal raises an
                    # exception (e.g. exchange timeout), which arms the cooldown and prevents
                    # an immediate second OPEN on the next cycle. Previously mark_trade() was
                    # only called inside "if ORDER_PLACED:", leaving a TOCTOU window that
                    # caused the ETCUSDT double-open (two fills 2 seconds apart, +40% oversize).
                    mark_trade(decision)

                    exec_result = self.executor.execute_signal(
                        symbol, exec_signal, trade_usdt, leverage_mult=_lev_mult
                    )

            # D) Audit execution result right after we get it
            # âœ… PHASE 3: Entry spread logging
            _spread = getattr(_filter, "spread_pct", 0.0) if "_filter" in locals() and _filter else 0.0
            self.audit.event(
                event_type="EXECUTION_RESULT",
                run_id=self.run_id,
                symbol=symbol,
                action=exec_result.status,
                details={
                    "decision": decision,
                    "signal": sig,
                    "trade_usdt": trade_usdt,
                    "spread_pct_logged": _spread,
                },
            )

            # Count trade actions this cycle (safer: count closes too)
            if exec_result.status in ORDER_OPEN_STATUSES | ORDER_CLOSE_STATUSES:
                if settings.EXECUTION_MODE.lower() == "live":
                    self.live_trades_this_cycle += 1

            # â”€â”€ EntryProtection: surface lock results immediately â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            # SUBMIT_UNCERTAIN: order dispatch timed out; lock is held by the executor
            # until reconciliation confirms flat or open. Update pending_open so the
            # UI reflects the uncertain state without waiting for the next reconcile.
            if exec_result.status == "SUBMIT_UNCERTAIN":
                _ep_pend = "BUY" if exec_signal == "BUY" else "SELL"
                st.pending_open = _ep_pend
                logger.warning(
                    "[SUBMIT_UNCERTAIN] %s: Order dispatch timed out â€” position unknown. "
                    "Entry lock held; reconciliation will resolve on next cycle. "
                    "Verify on exchange.",
                    symbol,
                )
            elif exec_result.status == "ENTRY_LOCK_HELD":
                # A distinct intent is already PENDING_OPEN/OPEN_CONFIRMED for this
                # bot/symbol/side. The EP lock blocked the duplicate.
                logger.warning(
                    "[ENTRY_PROTECTION] %s: ENTRY_LOCK_HELD â€” duplicate open blocked. "
                    "Existing entry: %s",
                    symbol,
                    (exec_result.details or {}).get("existing_entry"),
                )
            elif exec_result.status == "ENTRY_INTENT_REUSED":
                # Idempotent retry: same intent_key already active; no new order sent.
                logger.info(
                    "[ENTRY_PROTECTION] %s: ENTRY_INTENT_REUSED â€” identical intent "
                    "already active (state=%s). Skipping duplicate submission.",
                    symbol,
                    (exec_result.details or {}).get("entry_state"),
                )
            # â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

            # If we executed an order, update local meta (exchange remains source of truth)
            if exec_result.status in ORDER_OPEN_STATUSES:
                if decision.startswith(("OPEN_", "ADD_", "OPEN_PENDING_")):
                    # keep for UI until next sync refreshes entryPrice
                    st.entry_price = price
                    if decision.startswith("OPEN_"):
                        import uuid
                        st.position_id = str(uuid.uuid4())
                        st.current_stop_loss = policy.sl_plan
                        # â”€â”€ Fix: set position direction immediately so position management
                        # survives a bot restart before the next exchange sync cycle.
                        # Without this, a crash between ORDER_PLACED and the next exchange
                        # sync leaves st.position=NONE while the exchange holds an open
                        # position â€” causing the bot to lose track of it and miss SL/TP.
                        st.position = "LONG" if exec_signal == "BUY" else "SHORT"

                # âœ… Now the REAL piece: record trade_fills so PnL + win rate work (OPEN)
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

                    # âœ… PHASE 3: Slippage & Adjusted Expectancy Modeling (Paper only)
                    # If not in live mode, severely penalize simulated execution to prevent illusionary alpha
                    if settings.EXECUTION_MODE.lower() != "live":
                        _sim_slippage_bps = 5.0
                        _total_friction_pct = (_spread / 2.0) + (_sim_slippage_bps / 10000.0)
                        
                        original_avg = float(avg_price)
                        if side == "LONG":
                            avg_price = original_avg * (1.0 + _total_friction_pct)
                        else:
                            avg_price = original_avg * (1.0 - _total_friction_pct)
                        
                        logger.info(f"[EXEC COST REALISM] {symbol}: Adjusted paper {side} fill by {_total_friction_pct*100:.3f}% ({original_avg:.4f} â†’ {avg_price:.4f})")

                    _price_expected = float(price)
                    _slippage_pct = ((float(avg_price) - _price_expected) / _price_expected) * 100 if _price_expected > 0 else 0.0

                    _leg_open_run_id = self.run_id
                    _leg_open_cycle_id = self.cycle_id
                    if not _leg_open_run_id or not _leg_open_cycle_id:
                        logger.error(
                            "[FILL LINKAGE ERROR] Missing run_id/cycle_id for symbol=%s path=legacy_open",
                            symbol,
                        )
                    # B-3 Fix: wrap record_fill in explicit error handling â€” never swallow silently.
                    logger.debug("TRADE_FILL_PERSISTENCE_ATTEMPTED action=OPEN symbol=%s", symbol)
                    try:
                        record_fill(
                            self.db,
                            symbol=symbol,
                            side=side,
                            action="OPEN",
                            qty=float(filled_qty),
                            price=float(avg_price),
                            fee=float(fee) if fee is not None else None,
                            realized_pnl=None,
                            strategy=getattr(self.strategy, "name", "unknown"),
                            strategy_version=getattr(self.strategy, "version", "0"),
                            broker_id=getattr(settings, "BROKER_ID", "binance_futures"),
                            account_id=getattr(settings, "ACCOUNT_ID", "default"),
                            asset_class=getattr(settings, "ASSET_CLASS", "CRYPTO"),
                            market_type=self.context.market_type if self.context else getattr(settings, "ASSET_CLASS", "CRYPTO"),
                            execution_mode=self._effective_execution_mode(),
                            timeframe=str(getattr(self, "interval", "") or self.interval),
                            confidence=float(self.last_signal_confidence.get(symbol, 0.0)),
                            slippage_pct=_slippage_pct,
                            entry_price_expected=_price_expected,
                            stop_loss_price=st.current_stop_loss,
                            position_id=st.position_id,
                            r_multiple=None,
                            bot_instance_id=self.context.bot_instance_id if self.context else None,
                            user_id=self.context.user_id if self.context else None,
                            broker_account_id=self.context.broker_account_id if self.context else None,
                            order_id=str(exec_result.order_id) if exec_result.order_id else (
                                str(_d.get("order_id") or _d.get("orderId") or "")
                            ) or None,
                            run_id=_leg_open_run_id,
                            cycle_id=_leg_open_cycle_id,
                            trace_id=trace_id,
                        )
                        logger.debug("TRADE_FILL_PERSISTED action=OPEN symbol=%s", symbol)
                    except Exception as _rf_err:
                        logger.exception(
                            "TRADE_FILL_PERSISTENCE_FAILED action=OPEN symbol=%s error=%s",
                            symbol, _rf_err,
                        )
                    recorder.link_position(trace_id, st.position_id)

                    # E-1: Write full entry decision context to audit after successful open
                    try:
                        from app.core.trade_context_logger import log_entry_decision_context as _log_ctx
                        _rr_e1 = None
                        _avg_price_e1 = float(avg_price) if avg_price else 0.0
                        if st.current_stop_loss and _avg_price_e1 > 0 and st.current_stop_loss > 0:
                            _risk_e1 = abs(_avg_price_e1 - float(st.current_stop_loss))
                            _tp_pct = float(getattr(settings, "TAKE_PROFIT_PCT", 0.03))
                            _rwd_e1 = _avg_price_e1 * _tp_pct
                            if _risk_e1 > 0:
                                _rr_e1 = round(_rwd_e1 / _risk_e1, 4)
                        _atr_e1 = float(calculate_atr(kl, period=14)) if kl and len(kl) >= 14 else None
                        _dd_ctx_e1 = getattr(self, "_last_dd_ctx", {})
                        _log_ctx(
                            audit=self.audit,
                            run_id=self.run_id,
                            cycle_id=getattr(self, "cycle_id", None),
                            symbol=symbol,
                            side=side,
                            action="OPEN",
                            bot_instance_id=self.context.bot_instance_id if self.context else None,
                            user_id=self.context.user_id if self.context else None,
                            strategy_name=getattr(self.strategy, "name", "unknown"),
                            strategy_version=getattr(self.strategy, "version", "0"),
                            confidence_score=float(self.last_signal_confidence.get(symbol, 0.0)),
                            min_required_confidence=getattr(settings, "MIN_CONFIDENCE_THRESHOLD", 0.70),
                            market_regime=getattr(st, "last_regime", None),
                            market_regime_confidence=getattr(st, "last_regime_confidence", None),
                            atr_at_entry=_atr_e1,
                            signal_interval=getattr(settings, "SIGNAL_INTERVAL", settings.DEFAULT_INTERVAL),
                            intended_entry_price=float(price),
                            actual_entry_price=_avg_price_e1,
                            stop_loss=float(st.current_stop_loss) if st.current_stop_loss else None,
                            leverage=float(getattr(settings, "DEFAULT_LEVERAGE", 3)),
                            trade_amount_mode=getattr(settings, "TRADE_MODE", "normal"),
                            trade_amount_per_position=float(self.trade_usdt),
                            quantity=float(filled_qty) if filled_qty else None,
                            gross_risk_reward=_rr_e1,
                            min_required_risk_reward=getattr(settings, "MIN_RISK_REWARD", 1.8),
                            weekly_drawdown_pct=_dd_ctx_e1.get("weekly_drawdown_pct"),
                            monthly_drawdown_pct=_dd_ctx_e1.get("monthly_drawdown_pct"),
                            consecutive_losses=_dd_ctx_e1.get("consecutive_losses"),
                            broker_id=getattr(settings, "BROKER_ID", "binance_futures"),
                            environment=getattr(settings, "EXECUTION_MODE", "paper"),
                            trace_id=trace_id,
                            position_id=st.position_id,
                            order_id=str(exec_result.order_id) if exec_result.order_id else None,
                            estimated_fee=float(fee) if fee is not None else None,
                            policy_decision="APPROVED",
                            approval_reason="POLICY_PASSED",
                        )
                    except Exception as _e1_err:
                        logger.warning("E-1: log_entry_decision_context failed: %s", _e1_err)

                    try:
                        from shared_lib.persistence.trace_recorder import get_trace_recorder as _gtr
                        _gtr().record_execution(
                            trace_id=getattr(_gtr(), "_active_trace_id", None) or "",
                            status=exec_result.status,
                            order_id=str(exec_result.order_id) if exec_result.order_id else (str(_d.get("order_id") or _d.get("orderId") or "") or None),
                            fill_price=float(avg_price) if avg_price else None,
                            fill_qty=float(filled_qty) if filled_qty else None,
                            error=exec_result.error if not exec_result.success else None,
                        )
                    except Exception as e:
                        import logging
                        logging.getLogger(__name__).warning(f"Failed to record OPEN execution trace: {e}")
                except Exception:
                    pass

                # âœ… C) Once we re-enter successfully, clear stop tracking (OPEN / OPEN_PENDING only)
                if decision.startswith(("OPEN_", "OPEN_PENDING_")):
                    st.last_stop_ms = 0
                    st.reentry_confirm_signal = "NONE"
                    st.reentry_confirm_count = 0
                    # âœ… Notify PositionManager of the new position so price-tick exits are armed
                    try:
                        pm_side = PositionSide.LONG if exec_signal == "BUY" else PositionSide.SHORT
                        entry_px = float(avg_price) if avg_price else price  # type: ignore[possibly-undefined]
                        # Derive stop/tp from PolicyDecision if available, else fall back to percentage
                        pm_stop = float(policy.stop_loss_price or (entry_px * (1 - ctx.stop_loss_pct) if pm_side == PositionSide.LONG else entry_px * (1 + ctx.stop_loss_pct)))
                        pm_tp1  = float(policy.take_profit_price or (entry_px * (1 + ctx.take_profit_pct) if pm_side == PositionSide.LONG else entry_px * (1 - ctx.take_profit_pct)))
                        pm_tp2  = pm_tp1  # simplified; PositionManager will trail from here
                        self.position_manager.open_position(
                            symbol=symbol,
                            side=pm_side,
                            position_id=st.position_id,
                            entry_price=entry_px,
                            qty=float(filled_qty),  # type: ignore[possibly-undefined]
                            stop_price=pm_stop,
                            tp1_price=pm_tp1,
                            tp2_price=pm_tp2,
                            mode="PRECISION",
                            strategy_name=getattr(self.strategy, "name", "unknown"),
                            sl_order_id=((exec_result.details or {}).get("protection") or {}).get("sl_order_id") if isinstance(exec_result.details, dict) else None,
                            tp_order_id=((exec_result.details or {}).get("protection") or {}).get("tp_order_id") if isinstance(exec_result.details, dict) else None,
                        )
                    except Exception as pm_err:
                        logger.warning(f"[PM] {symbol}: open_position hook failed: {pm_err}")

            # When a close happens: record realized PnL from broker fills (works even if re-entry happens fast)
            if exec_result.status in ORDER_CLOSE_STATUSES:
                self._closed_symbols_this_cycle.add(symbol)
                # Capture MFE/MAE before close_position() removes the position from tracker
                _mfe_pct, _mae_pct = self.position_manager.compute_mfe_mae(symbol)
                # Capture trigger-level expected price for close slippage computation.
                # Must be done before close_position() removes the PositionState.
                _expected_close_price: Optional[float] = None
                try:
                    _pm_close_pos = self.position_manager.get_position(symbol)
                    if _pm_close_pos is not None:
                        if exit_reason == ExitReason.TP2:
                            if _pm_close_pos.tp.tp2_price and _pm_close_pos.tp.tp2_price > 0:
                                _expected_close_price = float(_pm_close_pos.tp.tp2_price)
                        elif exit_reason == ExitReason.TP1:
                            if _pm_close_pos.tp.tp1_price and _pm_close_pos.tp.tp1_price > 0:
                                _expected_close_price = float(_pm_close_pos.tp.tp1_price)
                        elif exit_reason in {ExitReason.SL, ExitReason.TRAILING_SL,
                                             ExitReason.BREAK_EVEN, ExitReason.BREAK_EVEN_BUFFER}:
                            if _pm_close_pos.sl.current_stop and _pm_close_pos.sl.current_stop > 0:
                                _expected_close_price = float(_pm_close_pos.sl.current_stop)
                        # TIME_EXIT / SIGNAL_REVERSAL / OTHER â†’ fall back to market price below
                except Exception:
                    pass  # keep None â†’ will fall back to current market price
                # Notify PositionManager so state machine resets cleanly
                try:
                    self.position_manager.close_position(symbol, reason=exit_reason or "signal")
                except Exception:
                    pass

                # ----------------------------------------------------------------

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
                            # âœ… UPDATE STRATEGY PERFORMANCE + CONFIDENCE METRICS (EXACT PLACE)
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

                # D-1: Update consecutive-loss guard with this trade's outcome.
                # Called regardless of whether pnl is exact or still 0 â€” the guard
                # tracks the sign of pnl (win / loss); a 0 pnl is treated as a win
                # (break-even) and resets the streak. This call is fire-and-forget.
                try:
                    _guard_on_trade_closed(pnl=float(pnl), bot_id=_bot_id_guard)
                except Exception as _guard_close_err:
                    logger.warning("[GUARD] on_trade_closed failed: %s", _guard_close_err)

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

                # D-1: Record trade result for consecutive loss tracking
                try:
                    _is_win = float(pnl) > 0
                    self.daily.record_trade_result(
                        is_win=_is_win,
                        soft_limit=getattr(settings, "MAX_CONSECUTIVE_LOSSES_SOFT", 3),
                        cooldown_minutes=getattr(settings, "CONSECUTIVE_LOSS_COOLDOWN_MINUTES", 120),
                        hard_limit=getattr(settings, "MAX_CONSECUTIVE_LOSSES_HARD", 5),
                        now_ms=int(time.time() * 1000),
                    )
                except Exception as _d1_err:
                    logger.warning("D-1: record_trade_result failed: %s", _d1_err)

                # âœ… keep: record close fill (for win-rate/reports)
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

                    # Use trigger-level price (SL/TP level) if captured; fall back to market price
                    _price_expected = _expected_close_price if (_expected_close_price and _expected_close_price > 0) else float(price)
                    _slippage_pct = ((float(avg_price) - _price_expected) / _price_expected) * 100 if _price_expected > 0 else None
                    
                    _r_multiple = None
                    if st.entry_price and st.current_stop_loss and st.entry_price != st.current_stop_loss:
                        risk_per_share = abs(st.entry_price - st.current_stop_loss)
                        if side == "LONG":
                            pnl_per_share = float(avg_price) - st.entry_price
                        else:
                            pnl_per_share = st.entry_price - float(avg_price)
                        _r_multiple = pnl_per_share / risk_per_share if risk_per_share > 0 else 0.0
                        
                    _close_position_id = st.position_id
                    _exit_reason_for_fill = exit_reason if exit_reason else ExitReason.SIGNAL_REVERSAL

                    _leg_close_run_id = self.run_id
                    _leg_close_cycle_id = self.cycle_id
                    if not _leg_close_run_id or not _leg_close_cycle_id:
                        logger.error(
                            "[FILL LINKAGE ERROR] Missing run_id/cycle_id for symbol=%s path=legacy_close",
                            symbol,
                        )
                    # B-3 Fix: wrap record_fill in explicit error handling â€” never swallow silently.
                    logger.debug("TRADE_FILL_PERSISTENCE_ATTEMPTED action=CLOSE symbol=%s", symbol)
                    try:
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
                            strategy=getattr(self.strategy, "name", "unknown"),
                            strategy_version=getattr(self.strategy, "version", "0"),
                            broker_id=getattr(settings, "BROKER_ID", "binance_futures"),
                            account_id=getattr(settings, "ACCOUNT_ID", "default"),
                            asset_class=getattr(settings, "ASSET_CLASS", "CRYPTO"),
                            market_type=self.context.market_type if self.context else getattr(settings, "ASSET_CLASS", "CRYPTO"),
                            execution_mode=self._effective_execution_mode(),
                            timeframe=str(getattr(self, "interval", "") or self.interval),
                            confidence=float(self.last_signal_confidence.get(symbol, 0.0)),
                            slippage_pct=_slippage_pct,
                            entry_price_expected=_price_expected,
                            stop_loss_price=st.current_stop_loss,
                            position_id=_close_position_id,
                            r_multiple=_r_multiple,
                            exit_reason=_exit_reason_for_fill,
                            mfe_pct=_mfe_pct,
                            mae_pct=_mae_pct,
                            exit_regime=st.last_regime if st.last_regime != "UNKNOWN" else None,
                            exit_regime_confidence=st.last_regime_confidence if st.last_regime != "UNKNOWN" else None,
                            bot_instance_id=self.context.bot_instance_id if self.context else None,
                            user_id=self.context.user_id if self.context else None,
                            broker_account_id=self.context.broker_account_id if self.context else None,
                            order_id=str(exec_result.order_id) if exec_result.order_id else (
                                str(_d.get("order_id") or _d.get("normalized", {}).get("order_id") or "")
                            ) or None,
                            run_id=_leg_close_run_id,
                            cycle_id=_leg_close_cycle_id,
                        )
                        logger.debug("TRADE_FILL_PERSISTED action=CLOSE symbol=%s", symbol)
                    except Exception as _rf_err:
                        logger.exception(
                            "TRADE_FILL_PERSISTENCE_FAILED action=CLOSE symbol=%s error=%s",
                            symbol, _rf_err,
                        )
                    
                    try:
                        from shared_lib.persistence.trace_recorder import get_trace_recorder as _gtr
                        _gtr().record_execution(
                            trace_id=getattr(_gtr(), "_active_trace_id", None) or "",
                            status=exec_result.status,
                            order_id=str(exec_result.order_id) if exec_result.order_id else (str(_d.get("order_id") or _d.get("normalized", {}).get("order_id") or "") or None),
                            fill_price=float(avg_price) if avg_price else None,
                            fill_qty=float(filled_qty) if filled_qty else None,
                            error=exec_result.error if not exec_result.success else None,
                        )
                    except Exception as e:
                        import logging
                        logging.getLogger(__name__).warning(f"Failed to record CLOSE execution trace: {e}")
                    
                    if "CLOSE" in decision:
                        st.position_id = None
                        st.current_stop_loss = None
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
                    # Not flat yet â†’ do NOT count pnl (prevents double counting / wrong pnl)
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
                        "action": exec_result.status,
                        "details": exec_result.details,
                    },
                },
            )

        finally:
            # âœ… ALWAYS unlock
            try:
                lock.release()
            except Exception:
                pass

    def run_once(self, max_symbols: int = 10) -> Dict[str, Any]:
        """Deprecated compatibility wrapper; all business logic lives in run_cycle."""
        logger.warning("[DEPRECATED] PaperRunner.run_once delegates to run_cycle")
        return self.run_cycle()
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
                self._reconcile_entry_protection(sym, amt, st)
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
                    # flat on exchange â†’ reset local state
                    st.position = "NONE"
                    st.entry_price = None
                    st.entry_qty = 0.0
                    st.adds = 0
                # persist reconciled state
                self._reconcile_entry_protection(sym, amt, st)
                self.store.save_symbol(sym, st)

            except Exception:
                # don't crash startup because one symbol failed
                continue


