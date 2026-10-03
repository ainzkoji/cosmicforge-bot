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
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo
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
from app.symbols.sizing import parse_usdt_map, usdt_for
from app.symbols.leverage import leverage_for, parse_leverage_map
from app.symbols.universe import parse_symbols
from app.symbols.dynamic_universe import (
    DynamicUniverseService,
    DynamicUniverseShadowRecorder,
)
from app.symbols.symbol_selector import DynamicSymbolSelector
from app.universe.contracts import UniverseMode

#: Strategy-candle length per interval, for the no-new-candle pre-gate.
_INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "1d": 86_400_000,
}
#: A closed candle is requested this long after its close, so the venue has it.
_CANDLE_DUE_GRACE_MS = 2_000
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
from app.risk.adaptive_daily_budget import (
    AdaptiveDailyRiskBudgetEngine,
    AdaptiveDailyRiskInputs,
    AdaptiveDailyRiskPolicy,
    planned_initial_risk_usdt,
)
from app.risk.invariant_checker import get_invariant_checker
from app.metrics.health import StrategyHealthMonitor

# âœ… ADD: Monitoring infrastructure
from shared_lib.persistence.trace_recorder import get_trace_recorder, StrategySignal

# ML inference (Step 5D-2) â€” additive scoring layer; disabled by default via settings

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
from app.decision.reasons import CycleReason, QualityReason
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

def _governing_threshold(strategy: Any) -> str:
    """Format the threshold that actually governed the last evaluation.

    The runner does not resolve a threshold and must not print one as if it
    had. This reads the value off the AdaptiveThresholdDecision the strategy
    produced, and prints ``not-evaluated`` when no threshold was decided --
    rather than a zero, which would read as "the bar was zero".
    """
    decision = getattr(strategy, "last_threshold_decision", None)
    final = getattr(decision, "final_threshold", None)
    return "not-evaluated" if final is None else f"{float(final):.4f}"


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
        # Production defaults to wall-clock UTC. Historical replay may inject
        # a millisecond clock so calendar decisions use the replay timestamp.
        self._clock_source = None

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
            self.daily_trade_cap_enabled = bool(getattr(self.context, "daily_trade_cap_enabled", False))
            self.hard_runaway_daily_entry_limit = int(getattr(self.context, "hard_runaway_daily_entry_limit", 200))
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
            _raw_max_trades = getattr(settings, "MAX_TRADES_DAILY", None)
            self.max_trades_daily = None if _raw_max_trades in (None, "", 0, "0") else int(_raw_max_trades)
            self.daily_trade_cap_enabled = self.max_trades_daily is not None
            self.hard_runaway_daily_entry_limit = 200
            self.max_open_positions = getattr(settings, "MAX_OPEN_POSITIONS", 3)
            self.trade_usdt = settings.TRADE_USDT_PER_ORDER

        # B-7 Fix: Hard-block sma_cross for user-context bots.
        # sma_cross is a simple MA crossover with no regime/volatility/spread filters.
        # It must never run for a real user bot â€” bind CATI instead.
        _UNSAFE_STRATEGY_ALIASES = {"sma_cross", "sma-cross", "sma cross"}
        # Historical configured strategy IDs never select runtime intelligence.
        effective_strategy = "cati"

        # ---- Basic config / strategy ----

        # B-2 Fix: If effective_symbols is already a str, do NOT join â€” joining a str
        # character-by-character produces single-letter symbols like T, E, D, S, ,.
        if isinstance(effective_symbols, str):
            _symbols_str = effective_symbols
        else:
            _symbols_str = ",".join(effective_symbols)
        # The environment's MAX_SYMBOLS caps only the legacy contextless
        # runner (development). A bot's markets come from its own policy --
        # an explicit allowlist or its connected broker's universe.
        self.symbols = parse_symbols(
            _symbols_str, settings.MAX_SYMBOLS if not self.context else 100_000
        )
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
        # BotInstance.capital_allocation: the base for percent allocations and a
        # required configuration. NOT an aggregate cap -- the executor sizes
        # each trade against its own per-trade allocation, and open positions
        # never shrink the next trade's allocation.
        self.executor._capital_budget = float(
            getattr(self.context, "capital_budget", 0.0) or 0.0
        ) if self.context else 0.0
        # Account margin reservations are keyed by the broker account, so two
        # bots on one account cannot both spend the same free margin.
        self.executor._broker_account_id = (
            getattr(self.context, "broker_account_id", None) if self.context else None
        )
        # The executor re-checks and reserves the position slot atomically with
        # the entry intent, right before submission.
        self.executor._max_open_positions = int(getattr(self, "max_open_positions", 0) or 0)
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
        # A broker-universe bot starts from the symbols it already holds; the
        # first cycle's universe refresh adds ranked new-entry candidates. An
        # allowlist bot keeps its explicit list.
        self._universe_runtime = None
        self._universe_open_symbols: set[str] = set()
        self._next_candle_due_ms: dict[str, int] = {}
        self._last_closed_candle_ms: dict[str, int] = {}
        self._universe_deferred = 0
        self._last_quiet_feed_check = 0.0
        self.universe_mode = self._resolve_universe_mode()
        if self.context and self.universe_mode == UniverseMode.BROKER:
            self._universe_runtime = self._build_universe_runtime()
            _held = self._held_symbols_from_store(bot_id)
            self.context.symbols = list(_held)
            self.symbols = list(_held)
            self._universe_open_symbols = set(_held)
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
        self.daily = DailyLossState(day=self._today())

        # Drawdown monitor
        self.drawdown_monitor = DrawdownMonitor(self.store)

        # Health monitor
        self.health_monitor = StrategyHealthMonitor(self.db)

        # Circuit Breaker (Universal Registry)
        self.circuit_registry = get_circuit_registry()

        # Risk Budget Engine â€” per-bot so Bot A's positions never exhaust Bot B's budget
        self.budget_engine = get_risk_budget_engine(bot_id=bot_id)
        self.daily_budget_engine = AdaptiveDailyRiskBudgetEngine(
            AdaptiveDailyRiskPolicy(
                max_daily_loss_pct=min(0.025, float(getattr(settings, "ADAPTIVE_DAILY_RISK_MAX_DAILY_LOSS_PCT", 0.025))),
                daily_r_budget=float(getattr(settings, "ADAPTIVE_DAILY_RISK_R_BUDGET", 1.5)),
                minimum_history_trades=int(getattr(settings, "ADAPTIVE_DAILY_RISK_MIN_HISTORY_TRADES", 30)),
                risk_lookback_trades=int(getattr(settings, "ADAPTIVE_DAILY_RISK_LOOKBACK_TRADES", 100)),
                risk_lookback_days=int(getattr(settings, "ADAPTIVE_DAILY_RISK_LOOKBACK_DAYS", 45)),
                minimum_budget_usdt=float(getattr(settings, "ADAPTIVE_DAILY_RISK_MIN_BUDGET_USDT", 6.0)),
                maximum_budget_usdt=float(getattr(settings, "ADAPTIVE_DAILY_RISK_MAX_BUDGET_USDT", 24.0)),
                caution_consumption_pct=float(getattr(settings, "ADAPTIVE_DAILY_RISK_CAUTION_PCT", 0.50)),
                defensive_consumption_pct=float(getattr(settings, "ADAPTIVE_DAILY_RISK_DEFENSIVE_PCT", 0.80)),
                performance_factor_min=float(getattr(settings, "ADAPTIVE_DAILY_RISK_PERFORMANCE_FACTOR_MIN", 0.50)),
                performance_factor_max=float(getattr(settings, "ADAPTIVE_DAILY_RISK_PERFORMANCE_FACTOR_MAX", 1.0)),
                volatility_factor_min=float(getattr(settings, "ADAPTIVE_DAILY_RISK_VOLATILITY_FACTOR_MIN", 0.60)),
                drawdown_factor_min=float(getattr(settings, "ADAPTIVE_DAILY_RISK_DRAWDOWN_FACTOR_MIN", 0.40)),
                timezone_name=str(getattr(settings, "ADAPTIVE_DAILY_RISK_TIMEZONE", "Europe/Rome")),
            ),
            db=self.db,
        )

        # Policy Engine â€” per-bot so each bot uses its own budget engine.
        # Reset cache first so the engine is always created with the current config's
        # min_confidence (not the stale 0.10 default from a previous instantiation).
        reset_policy_engine(bot_id)
        # No confidence floor is passed: PolicyEngine's confidence gate was
        # deleted. Entry quality has one authority.
        self.policy_engine = get_policy_engine(
            bot_id=bot_id,
            budget_engine=self.budget_engine,
            circuit_registry=self.circuit_registry,
        )

        # Adaptive Engine â€” per-bot so loss streak / drawdown / rolling stats
        # are always scoped to this bot's own trade history.
        self.adaptive_engine = get_adaptive_engine(bot_id=bot_id, db=self.db)

        # ML Entry Quality Scorer (Step 5D-2) â€” additive gate, loaded once at startup.
        # Disabled by default (ML_ENABLED=False in settings).  All code paths degrade
        # gracefully if the model is unavailable or settings.ML_ENABLED is False.
        from types import SimpleNamespace
        self.ml_scorer = SimpleNamespace(enabled=False)  # historical V2 scorer is runtime-inert
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
            self.daily.trade_count = int(saved_daily.get("trade_count", 0))
            # F-9: restore consecutive loss counter and cooldown from DB
            self.daily.consecutive_losses = int(saved_daily.get("consecutive_losses", 0))
            self.daily.consec_loss_cooldown_until_ms = int(saved_daily.get("consec_loss_cooldown_until_ms", 0))
        self._reconcile_daily_economic_trade_count("startup")

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

        # The dynamic threshold rolling window is GONE along with its
        # calculator. Nothing reconstructs a confidence history at startup any
        # more; AdaptiveEntryThresholdEngine persists its own state, keyed per
        # bot/symbol/timeframe, and reloads it on restart.
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
                strategy_id="cati",
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

            # Canonical broker execution truth.  This runs before the in-memory
            # SymbolState projection below, so one-way accounts cannot retain a
            # stale opposite-side local row or reserve capital for venue dust.
            try:
                from app.execution.position_reconciliation import reconcile_runner_positions

                _broker_reconcile = reconcile_runner_positions(self, risks)
                if _broker_reconcile.get("changed"):
                    logger.warning(
                        "[BROKER_POSITION_RECONCILIATION] bot=%s mode=%s changes=%s",
                        self.context.bot_instance_id if self.context else self.run_id,
                        _broker_reconcile.get("position_mode"),
                        _broker_reconcile.get("changes"),
                    )
            except Exception as _canonical_reconcile_err:
                logger.exception(
                    "[BROKER_POSITION_RECONCILIATION] canonical projection failed: %s",
                    _canonical_reconcile_err,
                )

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
        result = self._execute_signal_with_evidence(
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
        self._record_fill(
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

    def _daily_close_already_marked(self, symbol: str, close_window: str) -> bool:
        """True when this symbol was already daily-closed in this window.

        Fails CLOSED on a storage error: if we cannot prove the position has not
        already been closed, we do not close it again.
        """
        bot_id = self.context.bot_instance_id if self.context else None
        if not bot_id:
            return False
        try:
            with self.db.connect() as conn:
                row = conn.execute(
                    """SELECT 1 FROM bot_daily_close_marks
                       WHERE bot_instance_id=? AND symbol=? AND close_window=?""",
                    (bot_id, symbol.upper(), close_window),
                ).fetchone()
            return row is not None
        except Exception as exc:
            logger.error(
                "[DAILY_CLOSE] %s: idempotency lookup failed (%s); skipping to avoid a repeat close",
                symbol, exc,
            )
            return True

    def _mark_daily_close(self, symbol: str, close_window: str, *, position_id: str | None) -> None:
        """Record that this symbol has been daily-closed for this window."""
        bot_id = self.context.bot_instance_id if self.context else None
        if not bot_id:
            return
        try:
            with self.db.connect() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO bot_daily_close_marks
                       (bot_instance_id,symbol,close_window,position_id,closed_at,reason)
                       VALUES (?,?,?,?,?,?)""",
                    (
                        bot_id, symbol.upper(), close_window, position_id,
                        datetime.now(timezone.utc).isoformat(), "DAILY_CLOSE",
                    ),
                )
        except Exception as exc:
            logger.error("[DAILY_CLOSE] %s: failed to persist close marker: %s", symbol, exc)

    def _run_daily_close_from_cycle(self) -> int:
        """Evaluate and execute daily close from the active management heartbeat."""
        if not settings.DAILY_CLOSE_ENABLED:
            return 0
        from datetime import datetime, timedelta, timezone
        from zoneinfo import ZoneInfo
        try:
            local = self._now_utc().astimezone(ZoneInfo(settings.DAILY_CLOSE_TIMEZONE))
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

        # One deterministic identity per close window. A window that wraps past
        # midnight keeps the date it STARTED on, so a single overnight window is
        # one window and not two.
        window_date = local.date() if minute >= start or start <= end else (
            local.date() - timedelta(days=1)
        )
        close_window = f"{window_date.isoformat()}:{settings.DAILY_CLOSE_WINDOW_START}"

        closed = 0
        for symbol, st in list(self.state.items()):
            pos = self.position_manager.get_position(symbol)
            if st.position not in ("LONG", "SHORT") or pos is None:
                continue
            # Restart-safe idempotency. The in-memory position going flat is not
            # enough: a restart inside the window, or a re-entry on the same
            # symbol, would otherwise re-issue a close on every 10-second tick.
            if self._daily_close_already_marked(symbol, close_window):
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
                self._mark_daily_close(symbol, close_window, position_id=st.position_id)
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
        return {"status": "disabled", "reason": "LEGACY_STRATEGY_DIAGNOSTICS_REMOVED"}


    # âœ… ADD: Encapsulated cycle execution (for MultiBotRunner)
    def _record_canonical_cycle(self, started_at: str) -> None:
        """Persist one canonical trading_cycles row for this cycle.

        Counts come from the canonical decisions written during the cycle, not
        from in-memory tallies, so the summary and the detail can never
        disagree.
        """
        if self.context is None:
            return

        from app.evidence.runner_bridge import resolve_provenance
        from app.evidence.writers import record_trading_cycle

        with self.db.connect() as conn:
            rows = conn.execute(
                """SELECT primary_reason, final_action, quality_result, risk_result,
                          execution_feasibility_result
                   FROM trading_decisions WHERE cycle_id=? AND bot_instance_id=?""",
                (self.cycle_id, self.context.bot_instance_id),
            ).fetchall()
            attempts = conn.execute(
                "SELECT COUNT(*) FROM execution_attempts WHERE cycle_id=?", (self.cycle_id,)
            ).fetchone()[0]

        reason_counts: dict[str, int] = {}
        for row in rows:
            reason = row["primary_reason"] or "UNKNOWN"
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

        # Heartbeats are no longer persisted as decisions (§11); they are
        # tallied per cycle so the summary still proves the loop ran without
        # writing ~17k rows a day.
        heartbeats = getattr(self, "_heartbeat_counts", {}) or {}
        no_new_candle = sum(heartbeats.values()) or reason_counts.get("NO_NEW_CANDLE", 0)
        if heartbeats:
            reason_counts.setdefault("NO_NEW_CANDLE", no_new_candle)
        self._heartbeat_counts = {}
        record_trading_cycle(self.db, {
            "cycle_id": self.cycle_id,
            "run_id": self.run_id,
            "runtime_session_id": getattr(self, "runtime_session_id", None),
            "bot_instance_id": self.context.bot_instance_id,
            "policy_hash": getattr(self.context, "effective_policy_hash", None) or None,
            "provenance": resolve_provenance(
                self._effective_execution_mode(),
                getattr(self.context, "broker_environment", None),
            ),
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "symbols_seen": len(rows) + no_new_candle,
            "symbols_managed": len(
                [s for s in self.state.values() if s.position in ("LONG", "SHORT")]
            ),
            "new_candle_evaluations": len(rows),
            "no_new_candle_count": no_new_candle,
            "no_opportunity_count": reason_counts.get("NO_OPPORTUNITY", 0),
            "quality_rejection_count": sum(1 for r in rows if r["quality_result"] == "FAIL"),
            "risk_rejection_count": sum(1 for r in rows if r["risk_result"] == "FAIL"),
            "execution_rejection_count": sum(
                1 for r in rows if r["execution_feasibility_result"] == "FAIL"
            ),
            "approved_count": sum(1 for r in rows if r["final_action"] == "APPROVED"),
            "execution_attempt_count": int(attempts or 0),
            "error_count": sum(1 for r in rows if r["final_action"] == "ERROR"),
            "reason_counts": reason_counts,
        })

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
            _cycle_started_at = datetime.now(timezone.utc).isoformat()
            self._cycle_stats = _CycleStats()  # â”€â”€ Visibility: per-cycle aggregator

            # Broker mode is reconciled periodically; the helper has a 30s
            # throttle, so management heartbeats stay cheap.  The first call
            # also performs the startup restore below.
            if getattr(self, "_reconciliation_done", False):
                self.reconcile_positions_on_startup()

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
                        _today = self._today()
                        _week_start = get_week_start(_today)
                        _month_start = get_month_start(_today)

                        _existing_weekly = self.store.load_weekly_snapshot(_week_start)
                        _reconstructed_weekly = self.store.reconstruct_weekly_snapshot(
                            _week_start,
                            broker_account_id=self.context.broker_account_id,
                        ) if self.context and self.context.broker_account_id else None
                        if _reconstructed_weekly is not None:
                            if _existing_weekly is not None:
                                _reconstructed_weekly.peak_equity = max(
                                    _reconstructed_weekly.peak_equity,
                                    _existing_weekly.peak_equity,
                                )
                                _reconstructed_weekly.low_equity = min(
                                    _reconstructed_weekly.low_equity,
                                    _existing_weekly.low_equity,
                                )
                            _reconstructed_weekly.peak_equity = max(
                                _reconstructed_weekly.peak_equity, _startup_equity
                            )
                            _reconstructed_weekly.low_equity = min(
                                _reconstructed_weekly.low_equity, _startup_equity
                            )
                            self.store.save_weekly_snapshot(_reconstructed_weekly)
                            logger.info(
                                "[DRAWDOWN] Reconciled weekly snapshot from broker equity evidence: "
                                "start=%.2f peak=%.2f low=%.2f current=%.2f",
                                _reconstructed_weekly.start_equity,
                                _reconstructed_weekly.peak_equity,
                                _reconstructed_weekly.low_equity,
                                _startup_equity,
                            )
                        elif _existing_weekly is None:
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

            # Keep peak/low protection current and create the next local week/month
            # snapshot even when the process runs continuously across a boundary.
            try:
                _cycle_equity = self.get_account_balance()
                self.drawdown_monitor.update_snapshots(self._today(), _cycle_equity)
            except Exception as _snap_err:
                logger.error("[DRAWDOWN] Failed to update period snapshots: %s", _snap_err)

            # 1. Update Risk State (daily check)
            # If we passed midnight, day logic handles itself in DailyLossState usually,
            # but we should ensure DB sync.
            if self.daily.day != self._today():
                self.daily = DailyLossState(day=self._today())
                # Re-load from DB just in case
                saved_daily = self.store.load_daily(self.daily.day)
                if saved_daily:
                    self.daily.realized_pnl = float(saved_daily.get("realized_pnl", 0.0))
                    self.daily.kill = bool(saved_daily.get("kill", False))
                    self.daily.trade_count = int(saved_daily.get("trade_count", 0))
                    # F-9: restore consecutive loss state from DB (new day means counter was reset
                    # at midnight but we still restore from DB in case it wasn't 0)
                    self.daily.consecutive_losses = int(saved_daily.get("consecutive_losses", 0))
                    self.daily.consec_loss_cooldown_until_ms = int(saved_daily.get("consec_loss_cooldown_until_ms", 0))
                self._reconcile_daily_economic_trade_count("daily_rollover")
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

            # 2. Iterate Symbols -- held positions first, then new-entry
            # candidates. For a broker-universe bot the candidate part is
            # bounded by a time and request budget; a deferred candidate keeps
            # its unclaimed candle and is evaluated on a following cycle.
            self._apply_universe()
            from app.trading_intelligence.integration import cycle_shadow as _cati_cycle

            _cati_cycle.on_cycle_start(self)  # flag-gated shadow batch; never raises
            # Held-position management has priority and does not consume the
            # new-entry scan budget. Start that clock at the first candidate.
            _candidate_started: float | None = None
            _budget_s = float(getattr(settings, "UNIVERSE_CYCLE_EVAL_BUDGET_SECONDS", 20.0) or 20.0)
            _deferred: list[str] = []
            for symbol in list(self.trade_symbols):
                _is_candidate = (
                    getattr(self, "_universe_runtime", None) is not None
                    and str(symbol).upper() not in getattr(self, "_universe_open_symbols", set())
                )
                if _is_candidate:
                    if _candidate_started is None:
                        _candidate_started = time.monotonic()
                    if self._candidate_budget_exhausted(_candidate_started, _budget_s):
                        _deferred.append(symbol)
                        continue
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

            if getattr(self, "_universe_runtime", None) is not None:
                self._after_universe_cycle(_deferred)
            _cati_cycle.on_cycle_end(self, results, tuple(_deferred))

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

            # Canonical cycle summary (Phase 9 §9). Derived from the decisions
            # this cycle actually wrote, so cycle health is queryable instead of
            # reconstructed by parsing console output.
            try:
                self._record_canonical_cycle(_cycle_started_at)
            except Exception as _canon_exc:
                logger.error("[TRADING_CYCLE] canonical summary failed: %s", _canon_exc)

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


    @property
    def client(self):
        """The exchange client, paper-book-backed when running in paper mode.

        The runner reconciles positions directly against this client in several
        places. In paper mode those reads must come from the paper book, not
        the broker, or an open paper position looks flat on the next cycle.
        """
        raw = self.__dict__.get("_client_raw")
        executor = self.__dict__.get("executor")
        paper = getattr(executor, "paper_executor", None)
        if raw is None or paper is None:
            return raw
        try:
            mode = str(self._effective_execution_mode() or "").lower()
        except Exception:
            return raw
        if mode != "paper":
            return raw
        cached = self.__dict__.get("_client_paper_view")
        if cached is None or getattr(cached, "inner", None) is not raw:
            from app.execution.paper_book_client import wrap_for_paper

            cached = wrap_for_paper(raw, paper, "paper")
            self.__dict__["_client_paper_view"] = cached
        return cached

    @client.setter
    def client(self, value):
        self.__dict__["_client_raw"] = value
        self.__dict__.pop("_client_paper_view", None)

    class _PositionAlreadyArmedError(Exception):
        """Not an error: the PositionManager already manages this position."""

    def _v2_order_authority_block(self, symbol, action):
        """Section 25 runtime switch: a V2 NEW entry reaches the executor only while the runtime order-authority
        router gives V2 this account scope. When CATI owns the scope (or nobody does) V2 is refused here -- the
        executor is never called -- so the two engines can never both open positions on one account. Exits,
        protection and reconciliation are not routed and always proceed."""
        from app.execution.executor import ExecResult
        return ExecResult(status="BLOCKED", success=False, error="ORDER_AUTHORITY_NOT_V2:LEGACY_ENTRY_PATH_REMOVED",
                          details={"symbol": symbol, "signal": action, "reason": "LEGACY_ENTRY_PATH_REMOVED"},
                          action="NO_TRADE")

    def _execute_signal_with_evidence(self, symbol, action, *args, **kwargs):
        """Execute a signal inside a canonical execution-attempt row.

        The attempt is opened BEFORE the executor is called and completed with
        whatever came back -- including nothing. That is the point: an executor
        that returns no order id, or raises, still leaves evidence that an
        order was attempted and why it did not become a fill. Without it the
        only trace of a failed execution is a log line.
        """
        from app.evidence.fill_bridge import execution_attempt

        if str(action).upper() in ("BUY", "SELL"):
            blocked = self._v2_order_authority_block(symbol, action)
            if blocked is not None:
                return blocked
        with execution_attempt(self, symbol, action) as attempt:
            result = self.executor.execute_signal(symbol, action, *args, **kwargs)
            details = result.details if isinstance(getattr(result, "details", None), dict) else {}
            normalized = details.get("normalized") if isinstance(details.get("normalized"), dict) else {}
            entry_order = details.get("entry_order") if isinstance(details.get("entry_order"), dict) else {}
            client_order_id = (
                details.get("client_order_id") or normalized.get("client_order_id")
                or entry_order.get("client_order_id") or entry_order.get("clientOrderId")
            )
            requested_qty = details.get("requested_qty")
            if requested_qty is None:
                requested_qty = details.get("qty") or normalized.get("quantity")
            executed_qty = details.get("filled_qty")
            if executed_qty is None:
                executed_qty = normalized.get("executed_qty") or entry_order.get("qty_filled")
            avg_fill_price = details.get("avg_price")
            if avg_fill_price is None:
                avg_fill_price = normalized.get("avg_price") or entry_order.get("avg_fill_price")
            resolution = (
                details.get("fill_resolution")
                if isinstance(details.get("fill_resolution"), dict) else {}
            )
            attempt.completed(
                str(getattr(result, "status", "") or "UNKNOWN"),
                broker_order_id=getattr(result, "order_id", None) or resolution.get("broker_order_id"),
                client_order_id=client_order_id or resolution.get("client_order_id"),
                requested_qty=requested_qty,
                executed_qty=executed_qty,
                avg_fill_price=avg_fill_price,
                primary_reason=getattr(result, "error", None),
                fill_resolution={
                    "initial_response_executed_qty": resolution.get("initial_executed_qty"),
                    "resolved_executed_qty": resolution.get("executed_qty"),
                    "fill_resolution_source": resolution.get("source"),
                    "fill_resolution_status": resolution.get("status"),
                    "fees": resolution.get("fees"),
                    "fee_asset": resolution.get("fee_asset"),
                } if resolution else None,
            )
            self._last_execution_attempt_by_symbol = getattr(
                self, "_last_execution_attempt_by_symbol", {}
            )
            self._last_execution_attempt_by_symbol[str(symbol).upper()] = attempt.attempt_id
            self._symbol_evidence = getattr(self, "_symbol_evidence", {})
            self._symbol_evidence.setdefault(str(symbol).upper(), {})[
                "execution_attempt_id"
            ] = attempt.attempt_id
            return result

    def _record_fill(self, db, *args, **kwargs):
        """Record a fill AND its canonical position evidence.

        Every fill in this runner goes through here. Before this existed the
        runtime wrote trade_fills and trading_decisions but never
        execution_attempts, positions or position_events -- the canonical
        lineage stopped at the decision, so no trade could be reconstructed
        from decision to realized result.
        """
        from app.evidence.fill_bridge import record_fill_with_evidence

        result = record_fill_with_evidence(self, db, *args, **kwargs)
        if str(kwargs.get("action") or "").upper() == "OPEN" and float(kwargs.get("qty") or 0.0) > 0:
            self._reconcile_daily_economic_trade_count("authoritative_open_fill")
        return result

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

    def _initial_risk_history(self) -> tuple[float, ...]:
        bot = self.context.bot_instance_id if self.context else "default"
        try:
            with self.db.connect() as conn:
                rows = conn.execute(
                    """
                    SELECT risk_amount
                    FROM trading_decisions
                    WHERE bot_instance_id = ?
                      AND risk_amount IS NOT NULL
                      AND risk_amount > 0
                    ORDER BY evaluated_at DESC
                    LIMIT ?
                    """,
                    (bot, int(getattr(settings, "ADAPTIVE_DAILY_RISK_LOOKBACK_TRADES", 100))),
                ).fetchall()
            return tuple(float(r["risk_amount"]) for r in reversed(rows))
        except Exception:
            return ()

    def _recent_r_history(self) -> tuple[float, ...]:
        bot = self.context.bot_instance_id if self.context else "default"
        values: list[float] = []
        try:
            with self.db.connect() as conn:
                rows = conn.execute(
                    """
                    SELECT r_multiple
                    FROM trade_fills
                    WHERE bot_instance_id = ?
                      AND r_multiple IS NOT NULL
                    ORDER BY timestamp_utc DESC
                    LIMIT 100
                    """,
                    (bot,),
                ).fetchall()
            values = [float(r["r_multiple"]) for r in reversed(rows)]
        except Exception:
            pass
        return tuple(values)

    def _realized_r_today(self) -> float:
        bot = self.context.bot_instance_id if self.context else "default"
        risk_day = self.daily_budget_engine.risk_date_for(self._now_utc())
        start_local = datetime.combine(
            risk_day,
            datetime.min.time(),
            tzinfo=self.daily_budget_engine.timezone,
        )
        start_utc = start_local.astimezone(timezone.utc)
        end_utc = (start_local + timedelta(days=1)).astimezone(timezone.utc)
        try:
            with self.db.connect() as conn:
                row = conn.execute(
                    """
                    SELECT COALESCE(SUM(r_multiple), 0.0) AS realized_r
                    FROM trade_fills
                    WHERE bot_instance_id = ?
                      AND action IN ('CLOSE', 'PARTIAL_CLOSE')
                      AND r_multiple IS NOT NULL
                      AND timestamp_utc >= ?
                      AND timestamp_utc < ?
                    """,
                    (bot, start_utc.isoformat(), end_utc.isoformat()),
                ).fetchone()
            return float(row["realized_r"] or 0.0) if row else 0.0
        except Exception:
            return 0.0

    def _day_open_equity(self) -> float:
        bot = self.context.bot_instance_id if self.context else "default"
        risk_date = self.daily_budget_engine.risk_date_for(self._now_utc())
        try:
            with self.db.connect() as conn:
                rows = conn.execute(
                    """
                    SELECT equity, timestamp_utc
                    FROM equity_snapshots
                    WHERE bot_instance_id = ?
                    ORDER BY timestamp_utc DESC
                    LIMIT 500
                    """,
                    (bot,),
                ).fetchall()
            same_day: list[tuple[datetime, float]] = []
            for row in rows:
                ts = datetime.fromisoformat(str(row["timestamp_utc"]).replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts.astimezone(self.daily_budget_engine.timezone).date() == risk_date:
                    same_day.append((ts, float(row["equity"] or 0.0)))
            if same_day:
                same_day.sort(key=lambda item: item[0])
                if same_day[0][1] > 0:
                    return same_day[0][1]
        except Exception:
            pass
        return float(self.get_account_balance() or 0.0)

    def _adaptive_daily_risk_context(
        self,
        *,
        symbol: str | None = None,
        quantity: float = 0.0,
        entry_price: float = 0.0,
        stop_price: float = 0.0,
        side: str = "LONG",
        regime: str | None = None,
    ) -> dict:
        if not bool(getattr(settings, "ADAPTIVE_DAILY_RISK_ENABLED", True)):
            return {}
        legacy_breached = (
            float(getattr(self, "daily_max_loss", 0.0) or 0.0) > 0
            and float(getattr(self.daily, "realized_pnl", 0.0) or 0.0) <= -abs(float(self.daily_max_loss))
        )
        dd = self._get_drawdown_context()
        decision = self.daily_budget_engine.evaluate(
            AdaptiveDailyRiskInputs(
                bot_instance_id=self.context.bot_instance_id if self.context else "default",
                risk_date=self.daily_budget_engine.risk_date_for(self._now_utc()),
                day_open_equity=self._day_open_equity(),
                current_equity=float(self.get_account_balance() or 0.0),
                realized_pnl_today=float(getattr(self.daily, "realized_pnl", 0.0) or 0.0),
                realized_r_today=self._realized_r_today(),
                initial_risk_history_usdt=self._initial_risk_history(),
                recent_r_history=self._recent_r_history(),
                account_drawdown_pct=float(dd.get("monthly_drawdown_pct", 0.0) or 0.0),
                market_regime=regime,
                policy_effective=not legacy_breached,
            )
        )
        payload = decision.as_policy_context()
        payload["symbol"] = symbol
        payload["planned_initial_risk_usdt"] = planned_initial_risk_usdt(
            quantity=quantity,
            entry_price=entry_price,
            stop_price=stop_price,
            side=side,
            fee_slippage_buffer_pct=self.daily_budget_engine.policy.fee_slippage_buffer_pct,
        )
        return payload

    def _get_drawdown_context(self) -> dict:
        """Compute current weekly/monthly drawdown percentages and consecutive losses.

        Returns a dict suitable for spreading into PolicyContext keyword args.
        Falls back to 0.0 if snapshots are unavailable.
        """
        today = self._today()
        equity = self.get_account_balance()

        # Weekly drawdown remains measured in all environments, but during
        # paper/demo/testnet development it is advisory rather than blocking.
        # Unknown/live broker environments fail closed and keep enforcement.
        broker_env = str(
            getattr(self.context, "broker_environment", "") if self.context else ""
        ).strip().lower()
        execution_mode = str(self._effective_execution_mode()).strip().lower()
        weekly_limit = float(getattr(settings, "MAX_WEEKLY_DRAWDOWN_PCT", 0.0) or 0.0)
        monthly_limit = float(getattr(settings, "MAX_MONTHLY_DRAWDOWN_PCT", 0.0) or 0.0)

        drawdown_limits_enforced = (
            execution_mode == "broker"
            and broker_env not in {"demo", "testnet", "paper", "sandbox", "practice"}
        )
        weekly_drawdown_enforced = drawdown_limits_enforced
        monthly_drawdown_enforced = drawdown_limits_enforced

        result = {
            "weekly_drawdown_pct": 0.0,
            "monthly_drawdown_pct": 0.0,
            "max_weekly_drawdown_pct": weekly_limit if weekly_drawdown_enforced else 0.0,
            "max_monthly_drawdown_pct": monthly_limit if monthly_drawdown_enforced else 0.0,
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
        if (
            not weekly_drawdown_enforced
            and weekly_limit > 0
            and result["weekly_drawdown_pct"] >= weekly_limit
        ):
            logger.warning(
                "[WEEKLY_DRAWDOWN_SHADOW] bot=%s account_environment=%s "
                "weekly_drawdown=%.2f%% reference_limit=%.2f%% blocking=False",
                getattr(self.context, "bot_instance_id", "default") if self.context else "default",
                broker_env or "paper",
                result["weekly_drawdown_pct"],
                weekly_limit,
            )

        if (
            not monthly_drawdown_enforced
            and monthly_limit > 0
            and result["monthly_drawdown_pct"] >= monthly_limit
        ):
            logger.warning(
                "[MONTHLY_DRAWDOWN_SHADOW] bot=%s account_environment=%s "
                "monthly_drawdown=%.2f%% reference_limit=%.2f%% blocking=False",
                getattr(self.context, "bot_instance_id", "default") if self.context else "default",
                broker_env or "paper",
                result["monthly_drawdown_pct"],
                monthly_limit,
            )

        return result

    def _execution_safety(self) -> Dict[str, Any]:
        """KYC and real-capital readiness, decided from the CONNECTED ACCOUNT.

        ``execution_mode=broker`` means "execute through the connected broker
        account"; whether real money is at stake is a property of that account
        (``broker_accounts.environment``, resolved into
        ``context.broker_environment``), never of the bot and never of a global
        setting. A live account keeps both gates, fail closed; a demo/test
        account records NOT_REQUIRED for both. Re-evaluated on every call, so
        each trade attempt is judged on the current account context.
        """
        if self._effective_execution_mode() != "broker":
            snapshot = {
                "scope": "PAPER",
                "kyc": {"gate": "KYC", "state": "NOT_REQUIRED", "allowed": True,
                        "reason": "paper execution: no broker order is placed"},
                "readiness": {"gate": "REAL_CAPITAL_READINESS",
                              "state": "NOT_REQUIRED_FOR_PAPER_EXECUTION", "allowed": True,
                              "reason": "paper execution: no broker order is placed"},
            }
            self._execution_safety_snapshot = snapshot
            return snapshot

        from app.product_safety.execution_safety import (
            evaluate_execution_kyc,
            evaluate_execution_readiness,
        )

        environment = getattr(self.context, "broker_environment", None) if self.context else None
        kyc = evaluate_execution_kyc(
            user_id=self.context.user_id if self.context else None,
            broker_environment=environment,
        )
        readiness = evaluate_execution_readiness(
            db=getattr(self, "db", None),
            bot_instance_id=self.context.bot_instance_id if self.context else None,
            broker_environment=environment,
        )
        snapshot = {
            "scope": "BROKER",
            "broker_account_id": self.context.broker_account_id if self.context else None,
            "account_environment": kyc.account_environment,
            "real_capital": kyc.real_capital,
            "kyc": kyc.to_dict(),
            "readiness": readiness.to_dict(),
        }
        self._execution_safety_snapshot = snapshot

        # Logged when the verdict changes, not on every heartbeat.
        key = (kyc.state, readiness.state, kyc.account_environment)
        if key != getattr(self, "_execution_safety_logged", None):
            self._execution_safety_logged = key
            bot = self.context.bot_instance_id if self.context else "default"
            if not kyc.allowed:
                logger.error("[KYC GATE] bot=%s blocked: state=%s account_environment=%s reason=%s",
                             bot, kyc.state, kyc.account_environment, kyc.reason)
            if not readiness.allowed:
                logger.error("[LIVE READINESS] bot=%s blocked: state=%s account_environment=%s reason=%s",
                             bot, readiness.state, readiness.account_environment, readiness.reason)
            logger.warning(
                "[EXECUTION_SAFETY] bot=%s broker_account=%s account_environment=%s "
                "real_capital=%s kyc=%s readiness=%s",
                bot, snapshot["broker_account_id"], kyc.account_environment,
                kyc.real_capital, kyc.state, readiness.state,
            )
        return snapshot

    def _runtime_kyc_allowed(self) -> bool:
        """The KYC gate for this trade attempt; see _execution_safety."""
        return bool(self._execution_safety()["kyc"]["allowed"])

    def _runtime_live_readiness_allowed(self) -> bool:
        """The real-capital readiness gate for this trade attempt; see _execution_safety."""
        return bool(self._execution_safety()["readiness"]["allowed"])

    def process_external_signal_candidate(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        return {"status": "BLOCKED", "decision": "BLOCKED", "executed": False,
                "reason": "EXTERNAL_SIGNAL_ADVISORY_ONLY_CATI_DISPATCH_REQUIRED"}


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
                        self._record_fill(
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
                            bot_instance_id=self.context.bot_instance_id if self.context else None,
                            user_id=self.context.user_id if self.context else None,
                            broker_account_id=self.context.broker_account_id if self.context else None,
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
                         res = self._execute_signal_with_evidence(
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
                             self._record_fill(
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

                 elif (
                     self.position_manager.get_position(symbol) is None
                     or getattr(
                         self.position_manager.get_position(symbol), "phase", None
                     ) == PositionPhase.FLAT
                 ):
                      # The PositionManager has no live position for a symbol the
                      # runner believes is open -- a genuine desync, normally
                      # after a restart. Restore it into active management.
                      #
                      # This used to be the `else` of the trailing-stop-update
                      # condition above, which meant it fired whenever a trailing
                      # update simply was not due: on almost every cycle. Re-arming
                      # a live position resets its phase to SEEKING_TP1, clears
                      # tp1_hit and restores the original stop, so TP1, break-even
                      # and trailing state were destroyed as fast as they were
                      # created, and TP1 could fire again on the next tick.
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
                try:
                    _bucket = getattr(self, "_symbol_evidence", None)
                    if _bucket is not None:
                        _bucket.setdefault(symbol, {})["snapshot"] = market_snapshot
                except Exception:
                    pass
                _cs = getattr(self, "_cycle_stats", None)
                if _cs:
                    _cs.record_hold(symbol, 0.0, CycleReason.NO_NEW_CANDLE)
                # Phase 6 §6.9: this is NOT a strategy HOLD. The strategy did not
                # run at all -- there was no new closed candle to evaluate. A
                # HOLD would claim the strategy looked and found nothing, which
                # is a different (and later, downstream) fact: NO_OPPORTUNITY.
                return {
                    "symbol": symbol,
                    "decision": CycleReason.NO_NEW_CANDLE,
                    "evaluated": False,
                    "reason": CycleReason.NO_NEW_CANDLE,
                    "reason_code": CycleReason.NO_NEW_CANDLE,
                    "timeframe": self.interval,
                }

            # CATI controller has already observed this closed snapshot. Only
            # the whole-universe CATI dispatcher may create governed entries.
            from app.trading_intelligence.governance.runtime_authority import resolve_order_authority
            authority = resolve_order_authority(
                self.db, broker_account_id=getattr(self.context, "broker_account_id", None),
                venue=getattr(self.context, "broker_type", None),
                environment=getattr(self.context, "broker_environment", None))
            get_trace_recorder().record_gate(trace_id, allowed=False,
                reason_code="CATI_CONTROLLER_DISPATCH_ONLY", reason=authority.reason,
                details=authority.to_dict())
            get_trace_recorder().finalize(trace_id, state_change="CATI_OBSERVE", final_position=st.position)
            return {"symbol": symbol, "decision": "CATI_OBSERVE", "evaluated": True,
                    "reason_code": QualityReason.CATI_OBSERVE,
                    "reason": authority.reason, "authority": authority.to_dict(),
                    "signal_source": "CATI", "executed": False}

            # 3. Process Entry via Orchestrator

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

            self._record_fill(
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

    def _now_ms(self) -> int:
        source = getattr(self, "_clock_source", None)
        if source is not None:
            value = source()
            return int(value.timestamp() * 1000) if hasattr(value, "timestamp") else int(value)
        return int(datetime.now(timezone.utc).timestamp() * 1000)

    def _now_utc(self) -> datetime:
        return datetime.fromtimestamp(self._now_ms() / 1000.0, timezone.utc)

    def _today(self) -> date:
        tz_name = str(getattr(settings, "ADAPTIVE_DAILY_RISK_TIMEZONE", "Europe/Rome"))
        try:
            return self._now_utc().astimezone(ZoneInfo(tz_name)).date()
        except Exception:
            return self._now_utc().date()

    def _reconcile_daily_economic_trade_count(self, reason: str) -> None:
        if not getattr(self, "store", None) or not getattr(self, "daily", None):
            return
        tz_name = str(getattr(settings, "ADAPTIVE_DAILY_RISK_TIMEZONE", "Europe/Rome"))
        old_count = int(getattr(self.daily, "trade_count", 0) or 0)
        try:
            new_count, evidence_ids = self.store.reconcile_daily_trade_count(
                self.daily.day,
                realized_pnl=float(getattr(self.daily, "realized_pnl", 0.0) or 0.0),
                kill=bool(getattr(self.daily, "kill", False)),
                consecutive_losses=int(getattr(self.daily, "consecutive_losses", 0) or 0),
                consec_loss_cooldown_until_ms=int(getattr(self.daily, "consec_loss_cooldown_until_ms", 0) or 0),
                timezone_name=tz_name,
            )
            self.daily.trade_count = int(new_count)
            if old_count != new_count:
                logger.warning(
                    "[DAILY_TRADE_COUNT_RECONCILED] bot=%s day=%s old_count=%s new_count=%s "
                    "reason=%s evidence_ids=%s",
                    getattr(self.context, "bot_instance_id", "default") if self.context else "default",
                    self.daily.day,
                    old_count,
                    new_count,
                    reason,
                    evidence_ids,
                )
        except Exception as exc:
            logger.warning("[DAILY_TRADE_COUNT_RECONCILE_FAILED] reason=%s error=%s", reason, exc)

    def _effective_execution_mode(self) -> str:
        mode = self.context.execution_mode if self.context else getattr(settings, "EXECUTION_MODE", "paper")
        normalized = str(mode or "paper").strip().lower()
        return "broker" if normalized in {"live", "broker", "testnet", "demo"} else "paper"

    def _evidence_provenance(self) -> str:
        """Provenance for trace evidence: the same authority canonical decisions use.

        The run's own classification wins (REPLAY, PAPER_FORWARD_VALIDATION, ...),
        falling back to the execution-mode derivation. Cached per run because
        the heartbeat starts a trace every few seconds.
        """
        from app.evidence.runner_bridge import resolve_provenance, run_provenance

        run_id = getattr(self, "run_id", None)
        cached = getattr(self, "_evidence_provenance_cache", None)
        if cached is not None and cached[0] == run_id:
            return cached[1]
        value = run_provenance(
            getattr(self, "db", None),
            run_id,
            resolve_provenance(
                self._effective_execution_mode(),
                getattr(self.context, "broker_environment", None) if self.context else None,
            ),
        )
        self._evidence_provenance_cache = (run_id, value)
        return value

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

    # ── Broker-derived market universe ─────────────────────────────────────

    def _resolve_universe_mode(self) -> str:
        if not self.context:
            return UniverseMode.ALLOWLIST
        raw = getattr(self.context, "universe_mode", None)
        try:
            return UniverseMode.normalize(raw) or UniverseMode.ALLOWLIST
        except ValueError:
            logger.error("[UNIVERSE] unknown universe_mode %r; treating as ALLOWLIST", raw)
            return UniverseMode.ALLOWLIST

    def _build_universe_runtime(self):
        """The connected account's universe: its own client, never an env list."""
        from app.universe.adapters import UniverseAdapterUnavailable, adapter_for
        from app.universe.engine import UniverseConfig, UniverseEngine
        from app.universe.runtime import UniverseRuntime

        try:
            adapter = adapter_for(getattr(self.context, "broker_type", ""), self.client)
        except UniverseAdapterUnavailable as exc:
            logger.error("[UNIVERSE] %s -- no new-entry candidates; held positions stay managed", exc)
            return None
        capital = float(getattr(self.context, "capital_budget", 0.0) or 0.0)
        leverage = float(getattr(self.context, "max_leverage", 0.0) or 0.0)
        cap = capital * leverage if capital > 0 and leverage > 0 else None
        config = UniverseConfig.from_settings(settings, max_position_notional=cap)
        allowed = tuple(getattr(self.context, "allowed_asset_classes", ()) or ())
        if allowed and allowed != ("CRYPTO",):
            # Multi-asset bot: widen the underlying types to the allowed classes
            # (a crypto-only bot keeps the configured default, unchanged).
            from dataclasses import replace as _replace
            from app.universe.adapters import underlying_types_for
            config = _replace(config, allowed_underlying_types=underlying_types_for(allowed))
        engine = UniverseEngine(adapter, config)
        return UniverseRuntime(
            engine=engine,
            broker_account_id=self.context.broker_account_id,
            bot_instance_id=self.context.bot_instance_id,
            mode=UniverseMode.BROKER,
            refresh_seconds=float(getattr(settings, "UNIVERSE_REFRESH_SECONDS", 900) or 900),
            db=self.db,
        )

    @staticmethod
    def _ordered_unique(symbols) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for s in symbols:
            u = str(s or "").strip().upper()
            if u and u not in seen:
                seen.add(u)
                out.append(u)
        return out

    def _economic_open_symbols(self) -> set[str]:
        """Symbols this bot holds a position in, or has an entry in flight for.

        The max_open_positions gate must count what exists at the broker, not
        what this process remembers. In-memory SymbolState alone misses an entry
        whose fill quantity was absent from the order response (the runner
        raises before recording it) and every position reconciliation adopted
        from the broker -- on 2026-09-13 that let one cycle open five 120-USDT
        entries against two slots. So: in-memory state, OPEN ledger rows and
        active entry-protection intents, one slot per symbol.
        """
        held = {
            str(sym).upper() for sym, st in self.state.items()
            if st.position in ("LONG", "SHORT")
        }
        bot_id = self.context.bot_instance_id if self.context else "default"
        held.update(str(s).upper() for s in self._ledger_held_symbols(bot_id) if s)
        return held

    def _ledger_held_symbols(self, bot_id: str) -> list[str]:
        """Symbols with an OPEN ledger position or an in-flight entry for this bot."""
        out: list[str] = []
        try:
            with self.db.connect() as conn:
                out += [
                    r[0] for r in conn.execute(
                        "SELECT DISTINCT symbol FROM positions WHERE bot_instance_id=? AND status='OPEN'",
                        (bot_id,),
                    ).fetchall()
                ]
        except Exception as exc:
            logger.warning("[UNIVERSE] open-position lookup failed: %s", exc)
        ep = getattr(getattr(self, "executor", None), "_entry_prot", None)
        if ep is not None:
            try:
                out += [str(r.get("symbol") or "") for r in ep.list_entries(bot_id)]
            except Exception as exc:
                logger.warning("[UNIVERSE] entry-protection lookup failed: %s", exc)
        return out

    def _held_symbols_from_store(self, bot_id: str) -> list[str]:
        held: list[str] = []
        try:
            for sym, row in (self.store.load_symbols() or {}).items():
                if getattr(row, "position", "NONE") in ("LONG", "SHORT") or str(
                    getattr(row, "pending_open", "NONE") or "NONE"
                ) != "NONE":
                    held.append(sym)
        except Exception as exc:
            logger.warning("[UNIVERSE] symbol-state lookup failed: %s", exc)
        return self._ordered_unique(held + self._ledger_held_symbols(bot_id))

    def _held_symbols(self) -> list[str]:
        """Every symbol that must be managed this cycle, whatever the ranking says."""
        bot_id = self.context.bot_instance_id if self.context else "default"
        held = [
            s for s, st in self.state.items()
            if st.position in ("LONG", "SHORT") or str(st.pending_open or "NONE") != "NONE"
        ]
        return self._ordered_unique(held + self._ledger_held_symbols(bot_id))

    def _restore_symbol_states(self, symbols) -> None:
        try:
            saved = self.store.load_symbols() or {}
        except Exception:
            saved = {}
        for sym in symbols:
            st = SymbolState()
            row = saved.get(sym)
            if row is not None:
                for name in (
                    "position", "entry_price", "last_signal", "last_action", "last_checked_ms",
                    "adds", "last_trade_ms", "pending_open", "entry_qty", "last_user_trade_id",
                    "reentry_confirm_signal", "reentry_confirm_count", "position_id",
                ):
                    if hasattr(row, name):
                        setattr(st, name, getattr(row, name))
            self.state[sym] = st

    def _apply_universe(self) -> None:
        """Refresh (on its cadence) and install this cycle's managed symbols."""
        if not getattr(self, "context", None) or getattr(self, "universe_mode", None) != UniverseMode.BROKER:
            return
        held = self._held_symbols()
        managed, open_set = list(held), set(held)
        if getattr(self, "_universe_runtime", None) is not None:
            try:
                res = self._universe_runtime.resolve(
                    open_symbols=held,
                    run_id=self.run_id,
                    runtime_session_id=getattr(self, "runtime_session_id", None),
                )
                managed, open_set = list(res.managed), set(res.open_symbols)
            except Exception as exc:
                logger.error("[UNIVERSE] resolve failed; managing held positions only: %s", exc)
        self._universe_open_symbols = open_set
        new = [s for s in managed if s not in self.state]
        if new:
            self._restore_symbol_states(new)
        self.trade_symbols = list(managed)
        self.live_symbols = list(managed)
        self.symbols = list(managed)
        self.universe_symbols = list(managed)
        orchestrator = getattr(self, "orchestrator", None)
        if orchestrator is not None and hasattr(orchestrator, "update_allowed_symbols"):
            try:
                orchestrator.update_allowed_symbols(
                    managed, leverage=float(getattr(self.context, "max_leverage", 10.0) or 10.0),
                )
            except Exception as exc:
                logger.error("[UNIVERSE] orchestrator symbol refresh failed: %s", exc)
        try:
            from app.ops.runtime_watchdog import get_watchdog

            get_watchdog().retain_symbols(self.context.bot_instance_id, managed)
        except Exception:
            pass

    def _candidate_budget_exhausted(self, started: float, budget_s: float) -> bool:
        if time.monotonic() - started > budget_s:
            return True
        runtime = getattr(self, "_universe_runtime", None)
        try:
            used = runtime.engine.adapter.request_budget().fraction_used if runtime else None
        except Exception:
            used = None
        limit = float(getattr(settings, "UNIVERSE_REQUEST_WEIGHT_BUDGET_FRACTION", 0.5) or 0.5)
        return used is not None and used > limit

    def _candle_pregate(self, symbol: str):
        """NO_NEW_CANDLE without a fetch or a trace, for a flat candidate between closes.

        Only for broker-universe bots, only for symbols with no position, no
        pending entry and a known next close still in the future. Anything
        held -- or unknown -- takes the full path, so position management is
        never skipped.
        """
        if getattr(self, "_universe_runtime", None) is None:
            return None
        sym = str(symbol).upper()
        if sym in getattr(self, "_universe_open_symbols", set()):
            return None
        st = self.state.get(symbol)
        if st is None or st.position in ("LONG", "SHORT") or str(st.pending_open or "NONE") != "NONE":
            return None
        due = getattr(self, "_next_candle_due_ms", {}).get(sym)
        if due is None or int(time.time() * 1000) >= due:
            return None
        return {
            "symbol": symbol,
            "decision": CycleReason.NO_NEW_CANDLE,
            "evaluated": False,
            "reason": CycleReason.NO_NEW_CANDLE,
            "reason_code": CycleReason.NO_NEW_CANDLE,
            "timeframe": self.interval,
            "pregate": "CANDLE_NOT_DUE",
        }

    def _note_candle_close(self, symbol: str, close_ms) -> None:
        interval_ms = _INTERVAL_MS.get(str(self.interval))
        try:
            close_ms = int(close_ms)
        except (TypeError, ValueError):
            return
        if not interval_ms:
            return
        sym = str(symbol).upper()
        if not hasattr(self, "_last_closed_candle_ms"):
            self._last_closed_candle_ms = {}
        if not hasattr(self, "_next_candle_due_ms"):
            self._next_candle_due_ms = {}
        self._last_closed_candle_ms[sym] = close_ms
        self._next_candle_due_ms[sym] = close_ms + interval_ms + _CANDLE_DUE_GRACE_MS

    def _after_universe_cycle(self, deferred) -> None:
        """Record deferrals; prove the feed alive for symbols not fetched this cycle.

        One batched price read (not a candle fetch per symbol) keeps the
        watchdog's market-data clocks honest for pre-gated candidates. Their
        latest closed candle is the one already evaluated -- the next one has
        not closed yet -- so no clock is made to look behind or ahead.
        """
        self._universe_deferred = len(deferred)
        if deferred:
            logger.info(
                "[UNIVERSE] %d candidate evaluation(s) deferred to a later cycle (budget)", len(deferred)
            )
        now = time.time()
        if now - self._last_quiet_feed_check < 60:
            return
        quiet = [
            s for s in self.trade_symbols
            if str(s).upper() not in self._universe_open_symbols and str(s).upper() in self._last_closed_candle_ms
        ]
        if not quiet:
            return
        self._last_quiet_feed_check = now
        from app.ops.runtime_watchdog import get_watchdog

        bot = self.context.bot_instance_id
        try:
            prices = self.client.get_prices(quiet) or {}
        except Exception as exc:
            for s in quiet:
                get_watchdog().market_data(bot, s, self.interval, error=f"{type(exc).__name__}: {exc}")
            return
        for s in quiet:
            if prices.get(s):
                get_watchdog().market_data(
                    bot, s, self.interval, latest_closed_candle=self._last_closed_candle_ms.get(str(s).upper()),
                )

    def step_symbol(self, symbol: str) -> Dict[str, Any]:
        """Evaluate one symbol and finalize exactly one canonical decision.

        Every early return inside ``_step_symbol_evaluate`` — and every
        exception it raises — passes through here, so the Phase 9 invariant
        (one bot + one symbol + one evaluation = one finalized decision) holds
        without each of the ~30 branches having to remember to record itself.
        """
        from app.evidence.runner_bridge import record_symbol_evaluation

        return record_symbol_evaluation(
            self, symbol, evaluate=self._step_symbol_evaluate,
        )

    def _step_symbol_evaluate(self, symbol: str) -> Dict[str, Any]:
        # A flat broker-universe candidate with no new closed candle costs
        # nothing: no fetch, no trace. See _candle_pregate.
        _pregated = self._candle_pregate(symbol)
        if _pregated is not None:
            return _pregated
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
            signal_source="CATI",
            provenance=self._evidence_provenance(),
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

            # 1) Market data.
            # A fetch failure is NOT "no new candle" (§9). DNS/network errors
            # against the broker previously degraded into NO_NEW_CANDLE, which
            # made a broken data feed look like a quiet market.
            try:
                kl = self.client.klines(symbol=symbol, interval=self.interval, limit=250)
            except Exception as _md_exc:
                from app.ops.runtime_watchdog import MARKET_DATA_UNAVAILABLE, get_watchdog

                _md_bot = self.context.bot_instance_id if self.context else str(self.run_id or "legacy")
                try:
                    get_watchdog().market_data(
                        _md_bot, symbol, self.interval,
                        error=f"{type(_md_exc).__name__}: {_md_exc}",
                    )
                except Exception:
                    pass
                logger.error("[MARKET_DATA] %s: fetch failed: %s", symbol, _md_exc)
                return {
                    "symbol": symbol,
                    "decision": "ERROR",
                    "evaluated": False,
                    "reason": MARKET_DATA_UNAVAILABLE,
                    "reason_code": MARKET_DATA_UNAVAILABLE,
                    "error": f"{type(_md_exc).__name__}: {_md_exc}",
                }

            from app.runner.market_snapshot import MarketSnapshot, claim_candle
            _primary_snapshot = MarketSnapshot.build(
                symbol=symbol,
                timeframe=self.interval,
                candles=kl,
                source=type(self.client).__name__,
            )
            _bot_candle_id = self.context.bot_instance_id if self.context else str(self.run_id or "legacy")
            # Watchdog: record that market data was fetched and what the latest
            # closed candle is. A fetch failure is handled below and must never
            # be reported as NO_NEW_CANDLE.
            try:
                from app.ops.runtime_watchdog import get_watchdog

                get_watchdog().market_data(
                    _bot_candle_id, symbol, self.interval,
                    latest_closed_candle=_primary_snapshot.latest_closed_candle_time,
                )
            except Exception:
                pass

            _evaluate_entry = claim_candle(
                self.db,
                bot_instance_id=_bot_candle_id,
                symbol=symbol,
                timeframe=self.interval,
                close_time=_primary_snapshot.latest_closed_candle_time,
            )
            self._note_candle_close(symbol, _primary_snapshot.latest_closed_candle_time)
            _snapshot = _primary_snapshot
            if _evaluate_entry:
                _htf = self.context.higher_timeframe if self.context else "4h"
                _htf_rows = self.client.klines(symbol=symbol, interval=_htf, limit=250)
                # Experts that read their own timeframe (vwap_reversion: 5m) must
                # get it from the snapshot as well: SnapshotMarketClient serves
                # only what the snapshot pins, and without these series such an
                # expert raised snapshot_timeframe_unavailable on every candle.
                _aux_rows: Dict[str, Any] = {}
                _aux_timeframes = getattr(getattr(self, "strategy", None), "snapshot_timeframes", ()) or ()
                for _aux_tf in _aux_timeframes:
                    if _aux_tf in {self.interval, _htf}:
                        continue
                    try:
                        _aux_rows[_aux_tf] = self.client.klines(symbol=symbol, interval=_aux_tf, limit=250)
                    except Exception as _aux_exc:
                        # Not fatal for the candle: an expert that needs this
                        # series will report ERROR, and the threshold engine
                        # fails that candle closed.
                        logger.error(
                            "[MARKET_DATA] %s: %s candles unavailable: %s",
                            symbol, _aux_tf, _aux_exc,
                        )
                _snapshot = MarketSnapshot.build(
                    symbol=symbol,
                    timeframe=self.interval,
                    candles=kl,
                    source=type(self.client).__name__,
                    higher_timeframe=_htf,
                    higher_timeframe_candles=_htf_rows,
                    auxiliary_candles=_aux_rows,
                )
            kl = list(_snapshot.candles)

            # Watchdog: a claimed candle means the strategy clock advanced.
            try:
                from app.ops.runtime_watchdog import get_watchdog

                _wd = get_watchdog()
                if _evaluate_entry:
                    _wd.candle_evaluated(
                        _bot_candle_id, symbol, self.interval,
                        closed_candle=_snapshot.latest_closed_candle_time,
                    )
                else:
                    # The candle was already claimed -- possibly by a previous
                    # process. Seed the marker so a restarted runtime is not
                    # mistaken for a stalled one, then age the stall counter.
                    from app.runner.market_snapshot import last_evaluated_candle

                    _wd.seed_evaluated_candle(
                        _bot_candle_id, symbol, self.interval,
                        closed_candle=last_evaluated_candle(
                            self.db, bot_instance_id=_bot_candle_id,
                            symbol=symbol, timeframe=self.interval,
                        ),
                    )
                    _wd.observe_clock(_bot_candle_id, symbol, self.interval)
            except Exception:
                pass

            # Persist snapshot lineage only for a claimed candle. Building the
            # snapshot happens every heartbeat (that is how the candle gate
            # works), but writing a row every 10 seconds would be a write storm
            # for data that is identical between evaluations.
            if _evaluate_entry and self.context is not None:
                try:
                    from app.evidence.runner_bridge import resolve_provenance
                    from app.evidence.writers import record_market_snapshot

                    record_market_snapshot(
                        self.db, _snapshot,
                        bot_instance_id=self.context.bot_instance_id,
                        provenance=resolve_provenance(
                            self._effective_execution_mode(),
                            getattr(self.context, "broker_environment", None),
                        ),
                    )
                except Exception as _ms_exc:
                    logger.error("[EVIDENCE] %s: snapshot persist failed: %s", symbol, _ms_exc)

            # CATI shadow evaluation -- disabled by default (CATI_SHADOW_ENABLED),
            # observes the same causally-pinned snapshot, never returns a value
            # this loop reads, and cannot raise (see shadow_hook module docstring).
            # Gated the same way snapshot persistence is: once per claimed candle,
            # not every 10s heartbeat.
            if _evaluate_entry:
                from app.trading_intelligence.integration.shadow_hook import run_full_shadow_pipeline, run_shadow_evaluation

                run_shadow_evaluation(
                    _snapshot,
                    venue=type(self.client).__name__,
                    source=type(self.client).__name__,
                    bot_instance_id=self.context.bot_instance_id if self.context else None,
                    symbol=symbol,
                    run_id=str(self.run_id) if getattr(self, "run_id", None) else None,
                    cycle_id=getattr(self, "_current_cycle_id", None),
                )
                # Section 11-13 pipeline -- separate flag (CATI_FULL_PIPELINE_
                # SHADOW_ENABLED), disabled by default, same never-raises
                # contract. See shadow_hook.run_full_shadow_pipeline docstring
                # for why it legitimately returns INSUFFICIENT_EVIDENCE for
                # every candidate until a real outcome library exists.
                run_full_shadow_pipeline(
                    _snapshot,
                    venue=type(self.client).__name__,
                    source=type(self.client).__name__,
                    bot_instance_id=self.context.bot_instance_id if self.context else None,
                    symbol=symbol,
                    run_id=str(self.run_id) if getattr(self, "run_id", None) else None,
                    cycle_id=getattr(self, "_current_cycle_id", None),
                )
                from app.trading_intelligence.integration import cycle_shadow as _cycle_shadow

                _cycle_shadow.record_symbol(
                    self, _snapshot, symbol,
                    venue=type(self.client).__name__, source=type(self.client).__name__,
                )

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

            if self.orchestrator:
                return self._step_symbol_orchestrated(symbol, kl, trace_id,
                    market_snapshot=_snapshot, evaluate_entry=_evaluate_entry)
            recorder.finalize(trace_id, state_change="CATI_OBSERVE", final_position=st.position)
            return {"symbol": symbol, "decision": "CATI_OBSERVE", "evaluated": bool(_evaluate_entry),
                    "reason": "CATI_CONTEXT_UNAVAILABLE_NO_ENTRY", "executed": False}

            # âœ… FAST EXIT CHECK

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
