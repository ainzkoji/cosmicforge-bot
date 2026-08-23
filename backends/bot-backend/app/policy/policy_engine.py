# app/policy/policy_engine.py
"""
Unified Policy Engine - Single Source of Truth for Trade Decisions

Consolidates:
- Action decisions (from trade_policy.py)
- Risk profiles (from risk_policy.py)
- Position sizing (from sizing.py)
- Pre-trade gating (from gate.py + safety_engine.py)
- Compound risk validation (from safety_engine.py)

Flow: Runner -> PolicyEngine.evaluate() -> Executor
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from app.risk.risk_budget import RiskBudgetEngine
    from app.risk.circuit import CircuitBreakerRegistry

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS
# =============================================================================

class Action(str, Enum):
    """Trade actions the runner should execute."""
    HOLD = "HOLD"
    OPEN_LONG = "OPEN_LONG"
    OPEN_SHORT = "OPEN_SHORT"
    ADD_LONG = "ADD_LONG"
    ADD_SHORT = "ADD_SHORT"
    CLOSE = "CLOSE"
    FLIP_TO_LONG = "FLIP_TO_LONG"
    FLIP_TO_SHORT = "FLIP_TO_SHORT"


class TradeAmountMode(str, Enum):
    """Trade amount sizing modes."""
    FIXED = "fixed"           # Fixed USDT amount per trade
    PERCENT = "percent"       # Percent of account balance per trade  
    ATR_RISK = "atr_risk"     # Default: ATR-based risk sizing


    # ...
    # Existing Enums
    
from app.exchange.registry import get_instrument_registry
from app.symbols.market_hours import ForexSessionGuard
class ReasonCode(str, Enum):
    # Allowed
    OK = "OK"
    
    # System safety (hard blocks)
    KILL_SWITCH_ACTIVE = "KILL_SWITCH_ACTIVE"
    CIRCUIT_BREAKER_TRIPPED = "CIRCUIT_BREAKER_TRIPPED"
    MARKET_CLOSED = "MARKET_CLOSED"
    KYC_REQUIRED = "KYC_REQUIRED"  # KYC verification required for live Forex
    
    # Capital safety (hard blocks) 
    DAILY_LOSS_LIMIT = "DAILY_LOSS_LIMIT"
    WEEKLY_DRAWDOWN_LIMIT = "WEEKLY_DRAWDOWN_LIMIT"
    MONTHLY_DRAWDOWN_LIMIT = "MONTHLY_DRAWDOWN_LIMIT"
    
    # Overtrading controls
    DAILY_TRADE_LIMIT = "DAILY_TRADE_LIMIT"
    MAX_POSITIONS_REACHED = "MAX_POSITIONS_REACHED"
    MAX_ADDS_REACHED = "MAX_ADDS_REACHED"
    
    # Cooldowns
    COOLDOWN_ACTIVE = "COOLDOWN_ACTIVE"
    SL_COOLDOWN_ACTIVE = "SL_COOLDOWN_ACTIVE"
    WAITING_REENTRY_CONFIRMATION = "WAITING_REENTRY_CONFIRMATION"
    
    # Confidence/signal
    SIGNAL_HOLD = "SIGNAL_HOLD"
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    
    # Execution mode
    PAPER_MODE = "PAPER_MODE"
    NOT_LIVE_SYMBOL = "NOT_LIVE_SYMBOL"
    
    # Sizing blocks
    MARGIN_INSUFFICIENT = "MARGIN_INSUFFICIENT"
    EXPOSURE_LIMIT = "EXPOSURE_LIMIT"
    MIN_NOTIONAL_NOT_MET = "MIN_NOTIONAL_NOT_MET"
    ATR_INVALID = "ATR_INVALID"
    VOLATILITY_SPIKE = "VOLATILITY_SPIKE"
    PRICE_INVALID = "PRICE_INVALID"
    SIZE_ZERO = "SIZE_ZERO"
    
    # Safety blocks
    COMPOUND_RISK_EXCEEDED = "COMPOUND_RISK_EXCEEDED"
    STOP_TOO_WIDE = "STOP_TOO_WIDE"
    LEVERAGE_TOO_HIGH = "LEVERAGE_TOO_HIGH"
    
    # Budget engine blocks
    PORTFOLIO_RISK_BUDGET = "PORTFOLIO_RISK_BUDGET"
    MARGIN_USAGE_LIMIT = "MARGIN_USAGE_LIMIT"
    SYMBOL_CONCENTRATION = "SYMBOL_CONCENTRATION"

    # D-1: Consecutive loss pauses
    CONSECUTIVE_LOSS_COOLDOWN = "CONSECUTIVE_LOSS_COOLDOWN"
    CONSECUTIVE_LOSS_DAY_PAUSE = "CONSECUTIVE_LOSS_DAY_PAUSE"

    # D-2: Risk/Reward enforcement
    RISK_REWARD_TOO_LOW = "RISK_REWARD_TOO_LOW"
    INVALID_RISK = "INVALID_RISK"
    INVALID_REWARD = "INVALID_REWARD"

    # D-3: ATR fixed sizing protection
    STOP_BELOW_ATR_NOISE_FLOOR = "STOP_BELOW_ATR_NOISE_FLOOR"
    MISSING_ATR_FOR_FIXED_SIZING = "MISSING_ATR_FOR_FIXED_SIZING"
    ATR_RISK_CAP_EXCEEDED = "ATR_RISK_CAP_EXCEEDED"



class RiskLevel(str, Enum):
    """User risk tolerance levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class RiskProfile:
    """Configuration for a risk level."""
    max_compound_risk_pct: float
    label: str
    description: str
    
    def max_stop_loss_pct(self, leverage: float) -> float:
        """Derived max stop loss at given leverage."""
        if leverage <= 0:
            return self.max_compound_risk_pct
        return self.max_compound_risk_pct / leverage


# Central risk profiles
RISK_PROFILES: Dict[RiskLevel, RiskProfile] = {
    RiskLevel.LOW: RiskProfile(
        max_compound_risk_pct=0.15,
        label="Conservative",
        description="Low drawdowns, smaller gains. Max risk per trade: 15%."
    ),
    RiskLevel.MEDIUM: RiskProfile(
        max_compound_risk_pct=0.225,
        label="Balanced",
        description="Moderate risk. Max risk per trade: 22.5%."
    ),
    RiskLevel.HIGH: RiskProfile(
        max_compound_risk_pct=0.30,
        label="Aggressive",
        description="High volatility. Max risk per trade: 30%."
    ),
}


@dataclass
class PolicyContext:
    """All inputs needed for policy decisions."""
    # Symbol info
    symbol: str
    signal: str  # "BUY" | "SELL" | "HOLD"
    confidence: float = 1.0
    user_id: str = ""  # User ID for KYC checks
    
    # Current state
    position: str = "NONE"  # "NONE" | "LONG" | "SHORT"
    adds: int = 0
    last_trade_ms: int = 0
    last_stop_ms: int = 0
    pending_open: Optional[str] = None
    reentry_confirm_signal: Optional[str] = None
    reentry_confirm_count: int = 0
    
    # Market data
    entry_price: float = 0.0
    atr: float = 0.0
    baseline_atr: float = 0.0
    short_term_atr: float = 0.0
    expected_slippage_pct: float = 0.0
    
    # Account state
    equity: float = 0.0
    margin_used: float = 0.0
    margin_available: float = 0.0
    open_positions_count: int = 0
    total_exposure: float = 0.0
    daily_realized_pnl: float = 0.0
    daily_trade_count: int = 0
    
    # Config (resolved from user/strategy settings)
    leverage: float = 1.0
    stop_loss_pct: float = 0.02  # 2%
    take_profit_pct: float = 0.03  # 3%
    cooldown_seconds: int = 120
    sl_cooldown_seconds: int = 600
    max_adds: int = 0
    trade_mode: str = "normal"  # "normal" | "flip"
    min_hold_time_seconds: int = 0  # Minimum seconds a position must be held before any signal-based exit is allowed
    reentry_confirmations: int = 1
    
    # Risk settings
    risk_level: RiskLevel = RiskLevel.LOW
    account_risk_pct: float = 1.0  # 1% risk per trade
    max_daily_loss: float = 50.0
    max_daily_trades: int = 20
    max_open_positions: int = 3
    max_leverage: float = 20.0
    min_notional: float = 5.0
    max_notional: float = 10000.0
    
    # Drawdown state (passed by runner from DrawdownMonitor)
    weekly_drawdown_pct: float = 0.0
    monthly_drawdown_pct: float = 0.0
    max_weekly_drawdown_pct: float = 0.0
    max_monthly_drawdown_pct: float = 0.0

    # Consecutive loss tracking (D-1)
    consecutive_losses: int = 0
    max_consecutive_losses: int = 0
    consec_loss_cooldown_until_ms: int = 0  # epoch ms; 0 = not in cooldown
    consec_loss_day_paused: bool = False    # True = no new entries until daily reset

    # D-2: Risk/Reward enforcement
    stop_loss_price: float = 0.0     # Actual stop-loss price (0 = unknown)
    take_profit_price: float = 0.0   # Actual take-profit price (0 = unknown)
    min_risk_reward: float = 0.0     # 0 = check disabled

    # D-3: ATR fixed sizing config
    min_stop_atr_multiplier: float = 0.5   # Stop must be >= this × ATR
    max_risk_per_trade_pct: float = 1.0    # Max % of equity risked per trade

    # Flags
    kill_switch: bool = False
    execution_mode: str = "paper"  # "paper" | "live"
    live_symbols: List[str] = field(default_factory=list)
    
    # Trade amount settings (strategy-level sizing)
    trade_amount_mode: str = "atr_risk"  # "fixed" | "percent" | "atr_risk"
    trade_amount_value: float = 0.0  # USDT if fixed; % if percent
    
    # Context
    now_ms: int = 0
    broker_id: str = "BINANCE"
    # Scoped circuit breaker key — format: "{bot_instance_id}:{broker_account_id}".
    # When set, the policy engine uses this key for circuit checks instead of broker_id
    # so that Bot A's trip never blocks Bot B on the same broker.
    circuit_key: Optional[str] = None


@dataclass
class PolicyDecision:
    """Complete policy decision with sizing."""
    # Core decision
    allowed: bool
    action: Action
    reason_code: ReasonCode
    reason: str  # Human-readable message
    
    # Sizing output (if allowed)
    quantity: float = 0.0
    notional: float = 0.0
    margin_required: float = 0.0
    
    # Adjusted values
    adjusted_stop_loss_pct: float = 0.0
    adjusted_take_profit_pct: float = 0.0
    
    # Risk metrics
    risk_usdt: float = 0.0
    compound_risk_pct: float = 0.0
    
    # State updates for runner
    pending_open: Optional[str] = None
    reentry_confirm_signal: Optional[str] = None
    reentry_confirm_count: int = 0
    
    # Audit trail
    adjustments: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def blocked(
        reason_code: ReasonCode,
        reason: str,
        action: Action = Action.HOLD,
        **kwargs
    ) -> "PolicyDecision":
        """Factory for blocked decisions."""
        return PolicyDecision(
            allowed=False,
            action=action,
            reason_code=reason_code,
            reason=reason,
            **kwargs
        )


# =============================================================================
# ATR SIZING (migrated from sizing.py)
# =============================================================================

def calculate_atr(klines: List, period: int = 14) -> float:
    """
    Calculate Average True Range from klines.
    
    Accepts list of dicts (with 'high', 'low', 'close') or 
    list of lists (Binance format: [open_time, open, high, low, close, ...])
    """
    if len(klines) < period + 1:
        return 0.0

    parsed = []
    for k in klines:
        if isinstance(k, dict):
            h = float(k.get('high', 0))
            l = float(k.get('low', 0))
            c = float(k.get('close', 0))
        else:
            h = float(k[2]) if len(k) > 2 else 0.0
            l = float(k[3]) if len(k) > 3 else 0.0
            c = float(k[4]) if len(k) > 4 else 0.0
        parsed.append((h, l, c))

    if not parsed:
        return 0.0

    trs = []
    prev_c = parsed[0][2]
    for i in range(1, len(parsed)):
        h, l, c = parsed[i]
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        trs.append(tr)
        prev_c = c

    if not trs:
        return 0.0

    if len(trs) < period:
        return sum(trs) / len(trs)
        
    # EMA-style ATR
    atr = sum(trs[i] for i in range(period)) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    
    return atr





# =============================================================================
# COOLDOWN HELPERS (migrated from trade_policy.py)
# =============================================================================

def _ms(seconds: int) -> int:
    return int(seconds) * 1000


def cooldown_ok(last_trade_ms: int, now_ms: int, cooldown_seconds: int) -> bool:
    if cooldown_seconds <= 0:
        return True
    if last_trade_ms <= 0:
        return True
    return (now_ms - last_trade_ms) >= _ms(cooldown_seconds)


def sl_cooldown_ok(last_stop_ms: int, now_ms: int, sl_cooldown_seconds: int) -> bool:
    if sl_cooldown_seconds <= 0:
        return True
    if last_stop_ms <= 0:
        return True
    return (now_ms - last_stop_ms) >= _ms(sl_cooldown_seconds)


# =============================================================================
# POLICY ENGINE
# =============================================================================

class PolicyEngine:
    """
    Single source of truth for trade decisions.
    
    Consolidates:
    - Action decisions (trade_policy.py)
    - Risk profiles (risk_policy.py)
    - Position sizing (sizing.py)
    - Pre-trade gating (gate.py + safety_engine.py)
    - Compound risk validation (safety_engine.py)
    """
    
    def __init__(
        self,
        budget_engine: Optional["RiskBudgetEngine"] = None,
        circuit_registry: Optional["CircuitBreakerRegistry"] = None,
        min_confidence: float = 0.10,
        min_margin_buffer_pct: float = 0.30,
        max_total_exposure_mult: float = 2.0,
        max_stop_distance_pct: float = 0.10,
        sl_multiplier: float = 2.0,
    ):
        self.budget_engine = budget_engine
        self.circuit_registry = circuit_registry
        self.min_confidence = min_confidence
        self.min_margin_buffer_pct = min_margin_buffer_pct
        self.max_total_exposure_mult = max_total_exposure_mult
        self.max_stop_distance_pct = max_stop_distance_pct
        self.sl_multiplier = sl_multiplier
    
    def evaluate(self, ctx: PolicyContext) -> PolicyDecision:
        """
        Main entry point. Evaluates all policy rules and returns decision.
        
        Check order:
        1. Signal check (HOLD → skip)
        2. System safety (kill switch, circuit breaker)
        3. Capital safety (daily loss)
        4. Overtrading (daily trades, max positions)
        5. Cooldowns
        6. Confidence check
        7. Action determination + state updates
        8. Sizing calculation (if opening/adding)
        9. Compound risk validation
        """
        # State updates (may be modified by checks)
        pending_open = ctx.pending_open
        reentry_sig = ctx.reentry_confirm_signal
        reentry_cnt = ctx.reentry_confirm_count
        
        # -----------------------------------------------------------------
        # 1. Signal check
        # -----------------------------------------------------------------
        if ctx.signal == "HOLD":
            return PolicyDecision(
                allowed=True,
                action=Action.HOLD,
                reason_code=ReasonCode.SIGNAL_HOLD,
                reason="Signal is HOLD, no action",
                pending_open=pending_open,
                reentry_confirm_signal=reentry_sig,
                reentry_confirm_count=reentry_cnt,
            )

        # -----------------------------------------------------------------
        # 1.5 Market Hours Check (Forex Guard)
        # -----------------------------------------------------------------
        from app.exchange.registry import get_instrument_registry
        from app.symbols.market_hours import ForexSessionGuard
        
        registry = get_instrument_registry()
        spec = registry.get_spec(ctx.broker_id, ctx.symbol)
        
        if spec:
            if not ForexSessionGuard.is_market_open(spec.asset_class, ctx.now_ms):
                reason_msg = ForexSessionGuard.get_status_reason(spec.asset_class, ctx.now_ms)
                return PolicyDecision.blocked(
                    ReasonCode.MARKET_CLOSED,
                    f"Market Closed: {reason_msg}",
                    pending_open=pending_open,
                    reentry_confirm_signal=reentry_sig,
                    reentry_confirm_count=reentry_cnt,
                )
        
        # 3.2: KYC Gating for Live Forex (Phase 4)
        if ctx.execution_mode == "live" and spec and spec.asset_class.name.startswith("FOREX"):
            # Check if KYC is required for live Forex trading
            try:
                from shared_lib.core.policy.kyc_policy import check_kyc_gate, KYCAction
                # ReasonCode is imported/defined globally
                
                # Gate live Forex execution on KYC approval
                allowed, reason = check_kyc_gate(ctx.user_id, KYCAction.START_LIVE_TRADING.value)
                
                if not allowed:
                    return PolicyDecision.blocked(
                        ReasonCode.KYC_REQUIRED,
                        f"[KYC GATE] {reason}. Live Forex trading requires KYC approval.",
                        pending_open=pending_open,
                        reentry_confirm_signal=reentry_sig,
                        reentry_confirm_count=reentry_cnt,
                    )
            except Exception as e:
                # Log error but don't block (fail open for backwards compatibility)
                logger.warning(f"[KYC GATE] Failed to check KYC status: {e}")
        
        # -----------------------------------------------------------------
        # 2. System safety (hard blocks)
        # -----------------------------------------------------------------
        if ctx.kill_switch:
            return PolicyDecision.blocked(
                ReasonCode.KILL_SWITCH_ACTIVE,
                "Kill switch is active",
                pending_open=pending_open,
                reentry_confirm_signal=reentry_sig,
                reentry_confirm_count=reentry_cnt,
            )
        
        if self.circuit_registry:
            _circuit_check_key = ctx.circuit_key or ctx.broker_id
            if self.circuit_registry.is_tripped(_circuit_check_key):
                return PolicyDecision.blocked(
                    ReasonCode.CIRCUIT_BREAKER_TRIPPED,
                    f"Circuit breaker tripped for {ctx.broker_id}",
                    pending_open=pending_open,
                    reentry_confirm_signal=reentry_sig,
                    reentry_confirm_count=reentry_cnt,
                )
        
        # -----------------------------------------------------------------
        # 2.5 Volatility Spike Gate
        # -----------------------------------------------------------------
        if ctx.baseline_atr <= 0:
            logger.warning(
                "[POLICY] baseline_atr=0 for %s — volatility spike gate disabled this cycle.",
                ctx.symbol,
            )
        elif ctx.short_term_atr > (3.0 * ctx.baseline_atr):
            return PolicyDecision.blocked(
                ReasonCode.VOLATILITY_SPIKE,
                f"Volatility spike detected: Short-term ATR ({ctx.short_term_atr:.4f}) > 3x Baseline ATR ({ctx.baseline_atr:.4f})",
                pending_open=pending_open,
                reentry_confirm_signal=reentry_sig,
                reentry_confirm_count=reentry_cnt,
            )
        
        # -----------------------------------------------------------------
        # 3. Capital safety
        # -----------------------------------------------------------------
        if ctx.max_daily_loss > 0 and ctx.daily_realized_pnl <= -abs(ctx.max_daily_loss):
            return PolicyDecision.blocked(
                ReasonCode.DAILY_LOSS_LIMIT,
                f"Daily loss limit reached: {ctx.daily_realized_pnl:.2f} <= -{ctx.max_daily_loss:.2f}",
                pending_open=pending_open,
                reentry_confirm_signal=reentry_sig,
                reentry_confirm_count=reentry_cnt,
            )
        
        # -----------------------------------------------------------------
        # 3.5 Weekly / monthly drawdown limits
        # -----------------------------------------------------------------
        if ctx.max_weekly_drawdown_pct > 0 and ctx.weekly_drawdown_pct >= ctx.max_weekly_drawdown_pct:
            return PolicyDecision.blocked(
                ReasonCode.WEEKLY_DRAWDOWN_LIMIT,
                f"TRADE_REJECTED_WEEKLY_DRAWDOWN_LIMIT: weekly drawdown "
                f"{ctx.weekly_drawdown_pct:.2f}% >= limit {ctx.max_weekly_drawdown_pct:.2f}%",
                pending_open=pending_open,
                reentry_confirm_signal=reentry_sig,
                reentry_confirm_count=reentry_cnt,
            )

        if ctx.max_monthly_drawdown_pct > 0 and ctx.monthly_drawdown_pct >= ctx.max_monthly_drawdown_pct:
            return PolicyDecision.blocked(
                ReasonCode.MONTHLY_DRAWDOWN_LIMIT,
                f"TRADE_REJECTED_MONTHLY_DRAWDOWN_LIMIT: monthly drawdown "
                f"{ctx.monthly_drawdown_pct:.2f}% >= limit {ctx.max_monthly_drawdown_pct:.2f}%",
                pending_open=pending_open,
                reentry_confirm_signal=reentry_sig,
                reentry_confirm_count=reentry_cnt,
            )

        # -----------------------------------------------------------------
        # 3.6 Consecutive loss pause (D-1 — soft cooldown + hard day pause)
        # Hard day pause: blocked for the rest of the trading day.
        # Soft cooldown: blocked for a timed window (e.g. 2 hours).
        # Only blocks new entries — existing positions continue to be managed.
        # -----------------------------------------------------------------
        if ctx.consec_loss_day_paused:
            return PolicyDecision.blocked(
                ReasonCode.CONSECUTIVE_LOSS_DAY_PAUSE,
                f"TRADE_REJECTED_CONSECUTIVE_LOSS_DAY_PAUSE: {ctx.consecutive_losses} consecutive losses. "
                f"New entries blocked for the rest of the trading day.",
                pending_open=pending_open,
                reentry_confirm_signal=reentry_sig,
                reentry_confirm_count=reentry_cnt,
            )

        _now_ms_consec = ctx.now_ms or int(1e12)
        if ctx.consec_loss_cooldown_until_ms > 0 and _now_ms_consec < ctx.consec_loss_cooldown_until_ms:
            _remaining_s = (ctx.consec_loss_cooldown_until_ms - _now_ms_consec) // 1000
            return PolicyDecision.blocked(
                ReasonCode.CONSECUTIVE_LOSS_COOLDOWN,
                f"TRADE_REJECTED_CONSECUTIVE_LOSS_COOLDOWN: {ctx.consecutive_losses} consecutive losses. "
                f"Cooldown active, {_remaining_s}s remaining.",
                pending_open=pending_open,
                reentry_confirm_signal=reentry_sig,
                reentry_confirm_count=reentry_cnt,
            )

        # -----------------------------------------------------------------
        # 4. Overtrading controls
        # -----------------------------------------------------------------
        if ctx.daily_trade_count >= ctx.max_daily_trades:
            return PolicyDecision.blocked(
                ReasonCode.DAILY_TRADE_LIMIT,
                f"Daily trade limit: {ctx.daily_trade_count} >= {ctx.max_daily_trades}",
                pending_open=pending_open,
                reentry_confirm_signal=reentry_sig,
                reentry_confirm_count=reentry_cnt,
            )
        
        # Max positions (only for new opens, not adds or closes)
        is_new_open = ctx.position == "NONE" and ctx.signal in ("BUY", "SELL")
        if is_new_open and ctx.open_positions_count >= ctx.max_open_positions:
            # Check budget engine if available
            if self.budget_engine:
                budget_state = self.budget_engine.get_budget_state()
                if budget_state.allowed_slots <= budget_state.position_count:
                    return PolicyDecision.blocked(
                        ReasonCode.MAX_POSITIONS_REACHED,
                        f"Budget slots exhausted: {budget_state.position_count}/{budget_state.allowed_slots}",
                        pending_open=pending_open,
                        reentry_confirm_signal=reentry_sig,
                        reentry_confirm_count=reentry_cnt,
                    )
            else:
                return PolicyDecision.blocked(
                    ReasonCode.MAX_POSITIONS_REACHED,
                    f"Max positions: {ctx.open_positions_count} >= {ctx.max_open_positions}",
                    pending_open=pending_open,
                    reentry_confirm_signal=reentry_sig,
                    reentry_confirm_count=reentry_cnt,
                )
        
        # -----------------------------------------------------------------
        # 5. Cooldowns
        # -----------------------------------------------------------------
        if not cooldown_ok(ctx.last_trade_ms, ctx.now_ms, ctx.cooldown_seconds):
            return PolicyDecision.blocked(
                ReasonCode.COOLDOWN_ACTIVE,
                f"Trade cooldown active ({ctx.cooldown_seconds}s)",
                pending_open=pending_open,
                reentry_confirm_signal=reentry_sig,
                reentry_confirm_count=reentry_cnt,
            )
        
        if not sl_cooldown_ok(ctx.last_stop_ms, ctx.now_ms, ctx.sl_cooldown_seconds):
            return PolicyDecision.blocked(
                ReasonCode.SL_COOLDOWN_ACTIVE,
                f"Stop-loss cooldown active ({ctx.sl_cooldown_seconds}s)",
                pending_open=pending_open,
                reentry_confirm_signal=reentry_sig,
                reentry_confirm_count=reentry_cnt,
            )
        
        # -----------------------------------------------------------------
        # 6. Confidence check
        # -----------------------------------------------------------------
        if ctx.confidence < self.min_confidence:
            return PolicyDecision.blocked(
                ReasonCode.LOW_CONFIDENCE,
                f"Confidence {ctx.confidence:.1%} < {self.min_confidence:.1%}",
                pending_open=pending_open,
                reentry_confirm_signal=reentry_sig,
                reentry_confirm_count=reentry_cnt,
            )
        
        # -----------------------------------------------------------------
        # 6.5 Risk/Reward enforcement (D-2) — only for new opens
        # -----------------------------------------------------------------
        if is_new_open and ctx.min_risk_reward > 0:
            _sl = ctx.stop_loss_price
            _tp = ctx.take_profit_price
            _ep = ctx.entry_price
            _sig = ctx.signal.upper()

            if _ep > 0 and _sl > 0 and _tp > 0:
                if _sig == "BUY":
                    _risk = _ep - _sl
                    _reward = _tp - _ep
                else:  # SELL / SHORT
                    _risk = _sl - _ep
                    _reward = _ep - _tp

                if _risk <= 0:
                    return PolicyDecision.blocked(
                        ReasonCode.INVALID_RISK,
                        f"TRADE_REJECTED_INVALID_RISK: risk={_risk:.6f} <= 0 "
                        f"(entry={_ep} sl={_sl} side={_sig})",
                        pending_open=pending_open,
                        reentry_confirm_signal=reentry_sig,
                        reentry_confirm_count=reentry_cnt,
                    )
                if _reward <= 0:
                    return PolicyDecision.blocked(
                        ReasonCode.INVALID_REWARD,
                        f"TRADE_REJECTED_INVALID_REWARD: reward={_reward:.6f} <= 0 "
                        f"(entry={_ep} tp={_tp} side={_sig})",
                        pending_open=pending_open,
                        reentry_confirm_signal=reentry_sig,
                        reentry_confirm_count=reentry_cnt,
                    )
                _rr = _reward / _risk
                if _rr < ctx.min_risk_reward:
                    return PolicyDecision.blocked(
                        ReasonCode.RISK_REWARD_TOO_LOW,
                        f"TRADE_REJECTED_RISK_REWARD_TOO_LOW: R:R={_rr:.2f} < min={ctx.min_risk_reward:.2f} "
                        f"(risk={_risk:.4f} reward={_reward:.4f})",
                        pending_open=pending_open,
                        reentry_confirm_signal=reentry_sig,
                        reentry_confirm_count=reentry_cnt,
                        details={
                            "entry_price": _ep, "stop_loss": _sl, "take_profit": _tp,
                            "risk_amount": _risk, "reward_amount": _reward,
                            "gross_risk_reward": round(_rr, 4),
                            "min_required_risk_reward": ctx.min_risk_reward,
                            "risk_reward_status": "REJECTED",
                            "risk_reward_reject_reason": "TRADE_REJECTED_RISK_REWARD_TOO_LOW",
                        },
                    )

        # -----------------------------------------------------------------
        # 6.6 ATR noise floor check (D-3) — only for new opens, all sizing modes
        # -----------------------------------------------------------------
        if is_new_open:
            _atr_d3 = ctx.atr
            _ep_d3 = ctx.entry_price
            _sl_d3 = ctx.stop_loss_price
            _sig_d3 = ctx.signal.upper()
            _mult = ctx.min_stop_atr_multiplier  # default 0.5

            if _atr_d3 <= 0:
                # Missing ATR in fixed-sizing mode is unsafe — reject
                return PolicyDecision.blocked(
                    ReasonCode.MISSING_ATR_FOR_FIXED_SIZING,
                    "TRADE_REJECTED_MISSING_ATR_FOR_FIXED_SIZING: ATR is zero/missing; "
                    "cannot validate stop distance for fixed-sizing mode.",
                    pending_open=pending_open,
                    reentry_confirm_signal=reentry_sig,
                    reentry_confirm_count=reentry_cnt,
                )

            if _ep_d3 > 0 and _sl_d3 > 0:
                _stop_dist_d3 = abs(_ep_d3 - _sl_d3)
                _atr_floor = _mult * _atr_d3
                if _stop_dist_d3 < _atr_floor:
                    return PolicyDecision.blocked(
                        ReasonCode.STOP_BELOW_ATR_NOISE_FLOOR,
                        f"TRADE_REJECTED_STOP_DISTANCE_BELOW_ATR_NOISE_FLOOR: "
                        f"stop_distance={_stop_dist_d3:.4f} < {_mult}×ATR={_atr_floor:.4f}. "
                        f"Stop is inside market noise — trade rejected.",
                        pending_open=pending_open,
                        reentry_confirm_signal=reentry_sig,
                        reentry_confirm_count=reentry_cnt,
                        details={
                            "atr_value": _atr_d3,
                            "stop_distance": _stop_dist_d3,
                            "min_stop_distance_required": _atr_floor,
                            "atr_sizing_status": "REJECTED",
                            "atr_sizing_reject_reason": "TRADE_REJECTED_STOP_DISTANCE_BELOW_ATR_NOISE_FLOOR",
                        },
                    )

            # ATR account-risk cap: estimated loss at stop vs max equity risk
            if _ep_d3 > 0 and _sl_d3 > 0 and ctx.equity > 0 and ctx.trade_amount_value > 0:
                _qty_d3 = (ctx.trade_amount_value * ctx.leverage) / _ep_d3
                _loss_at_stop = _qty_d3 * abs(_ep_d3 - _sl_d3)
                _max_loss_allowed = ctx.equity * (ctx.max_risk_per_trade_pct / 100.0)
                if _loss_at_stop > _max_loss_allowed:
                    return PolicyDecision.blocked(
                        ReasonCode.ATR_RISK_CAP_EXCEEDED,
                        f"TRADE_REJECTED_ATR_RISK_CAP_EXCEEDED: estimated loss at stop "
                        f"{_loss_at_stop:.2f} > max allowed {_max_loss_allowed:.2f} "
                        f"({ctx.max_risk_per_trade_pct:.1f}% of equity).",
                        pending_open=pending_open,
                        reentry_confirm_signal=reentry_sig,
                        reentry_confirm_count=reentry_cnt,
                        details={
                            "estimated_loss_at_stop": round(_loss_at_stop, 4),
                            "max_allowed_loss": round(_max_loss_allowed, 4),
                            "max_risk_per_trade_pct": ctx.max_risk_per_trade_pct,
                            "atr_sizing_status": "REJECTED",
                            "atr_sizing_reject_reason": "TRADE_REJECTED_ATR_RISK_CAP_EXCEEDED",
                        },
                    )

        # -----------------------------------------------------------------
        # 7. Action determination
        # -----------------------------------------------------------------
        action, action_reason, pending_open, reentry_sig, reentry_cnt = self._determine_action(ctx)
        
        # If action is HOLD (max adds reached, waiting confirmation, etc.), return early
        if action == Action.HOLD:
            return PolicyDecision(
                allowed=True,
                action=Action.HOLD,
                reason_code=ReasonCode.OK,
                reason=action_reason,
                pending_open=pending_open,
                reentry_confirm_signal=reentry_sig,
                reentry_confirm_count=reentry_cnt,
            )
        
        # -----------------------------------------------------------------
        # 8. Sizing (for opens and adds only)
        # -----------------------------------------------------------------
        qty = 0.0
        notional = 0.0
        risk_usdt = 0.0
        margin_required = 0.0
        stop_dist = 0.0
        adjustments = []
        sizing_details = {}
        
        if action in (Action.OPEN_LONG, Action.OPEN_SHORT, Action.ADD_LONG, Action.ADD_SHORT,
                      Action.FLIP_TO_LONG, Action.FLIP_TO_SHORT):
            
            # Validate inputs
            if ctx.entry_price <= 0:
                return PolicyDecision.blocked(
                    ReasonCode.PRICE_INVALID,
                    "Entry price is invalid",
                    action=action,
                    pending_open=pending_open,
                    reentry_confirm_signal=reentry_sig,
                    reentry_confirm_count=reentry_cnt,
                )
            
            from app.risk.sizing_engine import get_sizing_engine, SizingInputs
            
            # Calculate stop distance as percentage
            stop_dist = ctx.stop_loss_pct
            if ctx.atr > 0 and ctx.entry_price > 0:
                # ATR-derived stop distance
                stop_dist = (ctx.atr * self.sl_multiplier) / ctx.entry_price
            
            # Incorporate expected slippage for realistic risk sizing
            if ctx.expected_slippage_pct > 0:
                stop_dist += (ctx.expected_slippage_pct * 2.0)
            
            sizing_inputs = SizingInputs(
                symbol=ctx.symbol,
                equity=ctx.equity,
                entry_price=ctx.entry_price,
                risk_per_trade_pct=ctx.account_risk_pct / 100.0,
                stop_distance_pct=stop_dist,
                volatility_atr=ctx.atr,
                is_fallback_mode=False, 
                capital_allocation_pct=1.0, 
                max_risk_ceiling_pct=0.05,
                min_notional_usdt=ctx.min_notional,
                max_notional_usdt=ctx.max_notional,
                custom_trade_amount_mode=ctx.trade_amount_mode,
                custom_trade_amount_value=ctx.trade_amount_value
            )
            
            sizing_result = get_sizing_engine().calculate(sizing_inputs)
            
            if not sizing_result.allowed:
                return PolicyDecision.blocked(
                    ReasonCode.MIN_NOTIONAL_NOT_MET,
                    f"Sizing failed: {sizing_result.reason}",
                    action=action,
                    details=sizing_result.details,
                    pending_open=pending_open,
                    reentry_confirm_signal=reentry_sig,
                    reentry_confirm_count=reentry_cnt,
                )
                
            qty = sizing_result.quantity
            notional = sizing_result.notional
            sizing_details = sizing_result.details
            
            # For strict fixed dollar allocations, ensure notional = budget * leverage
            # and EXEMPT IT from leverage penalties, so the margin precisely hits the
            # user's explicit allocation (e.g. $10 USDT margin exactly).
            #
            # ATR safety cap (Stage 3B):
            # The user's chosen allocation amount is AUTHORITATIVE as the base size.
            # The ATR cap is a SAFETY CEILING only — it may reduce but never replaces
            # the sizing mode.  It fires only when the notional risk at the computed
            # stop distance would exceed the account's max-risk-per-trade limit.
            #
            # Formula (correct units throughout):
            #   notional_cap  = max_risk_capital / stop_distance_pct   ($ notional)
            #   margin_cap    = notional_cap / leverage                 ($ margin)
            #   cap fires if  margin_cap < user_fixed_margin
            #
            # The sizing_method label is always "fixed_amount_strict" for these
            # modes. ATR cap values remain visible as diagnostics/warnings only.
            if ctx.trade_amount_mode in ("fixed", "fixed_usdt", "fixed_amount") and ctx.trade_amount_value > 0:
                import os as _os
                _atr_cap_enabled = _os.getenv("ATR_SAFETY_CAP_ENABLED", "true").strip().lower() not in ("false", "0", "no")

                _base_margin    = ctx.trade_amount_value   # user's stated allocation (MARGIN $)
                _fixed_margin   = _base_margin
                _cap_reason = "fixed_amount_strict: user fixed margin respected"
                _leverage_factor = max(1.0, ctx.leverage)  # hoisted: used in cap calc and sizing_details
                _atr_stop_dist: float | None = None
                _max_risk_capital: float | None = None
                _notional_cap: float | None = None
                _margin_cap: float | None = None
                _theoretical_risk_usdt: float | None = None
                _theoretical_risk_pct: float | None = None
                _risk_warning = False

                if _atr_cap_enabled and ctx.atr > 0 and ctx.entry_price > 0 and ctx.equity > 0:
                    _atr_stop_dist = (ctx.atr * self.sl_multiplier) / ctx.entry_price
                    if _atr_stop_dist > 0:
                        # Notional cap: max position where dollar-risk-at-stop = max_risk_capital
                        _max_risk_capital  = ctx.equity * (ctx.account_risk_pct / 100.0)
                        _notional_cap      = _max_risk_capital / _atr_stop_dist
                        # Convert to margin basis for a like-for-like comparison
                        _margin_cap        = _notional_cap / _leverage_factor

                        _theoretical_risk_usdt = (_base_margin * _leverage_factor) * _atr_stop_dist
                        _theoretical_risk_pct = (_theoretical_risk_usdt / ctx.equity) * 100.0
                        _risk_warning = _theoretical_risk_pct > ctx.account_risk_pct

                        if _risk_warning:
                            adjustments.append(
                                f"fixed_amount_strict_risk_warning: estimated_risk_pct="
                                f"{_theoretical_risk_pct:.4f}% exceeds selected "
                                f"risk_pct={ctx.account_risk_pct:.4f}%"
                            )
                            logger.info(
                                "[SIZE WARNING] %s: fixed_amount_strict uses %.2f USDT margin. "
                                "Estimated risk is %.4f%% of equity, above selected risk level %.4f%%. "
                                "Risk cap was NOT applied because user selected fixed amount.",
                                ctx.symbol,
                                _base_margin,
                                _theoretical_risk_pct,
                                ctx.account_risk_pct,
                            )

                notional        = _fixed_margin * _leverage_factor
                qty             = notional / max(ctx.entry_price, 1e-8)
                risk_usdt       = notional
                margin_required = _fixed_margin
                _base_notional  = _base_margin * _leverage_factor
                _base_qty       = _base_notional / max(ctx.entry_price, 1e-8)

                _sizing_method = "fixed_amount_strict"
                adjustments.append(
                    f"Fixed Allocation Strict: user_margin={_base_margin:.4f} final={_fixed_margin:.4f} USDT margin"
                )
                sizing_details["sizing_method"]      = _sizing_method
                sizing_details["allocation_type"]    = "fixed_amount"
                sizing_details["allocation_mode"]    = "fixed"
                sizing_details["user_fixed_margin_usdt"] = round(_base_margin, 8)
                sizing_details["base_margin"]        = round(_base_margin, 8)
                sizing_details["final_margin"]       = round(_fixed_margin, 8)
                sizing_details["base_margin_usdt"]   = round(_base_margin, 8)
                sizing_details["final_margin_usdt"]  = round(_fixed_margin, 8)
                sizing_details["base_notional_usdt"] = round(_base_notional, 8)
                sizing_details["final_notional_usdt"] = round(notional, 8)
                sizing_details["base_qty"]           = round(_base_qty, 8)
                sizing_details["calculated_qty"]     = round(qty, 8)
                sizing_details["rounded_qty"]        = round(qty, 8)
                sizing_details["final_qty"]          = round(qty, 8)
                sizing_details["entry_price"]        = round(ctx.entry_price, 12)
                sizing_details["leverage"]           = _leverage_factor
                sizing_details["cap_reason"]         = _cap_reason
                sizing_details["cap_applied"]        = False
                # FIX 3: Include cap inputs in sizing trace for audit/debug
                sizing_details["account_risk_pct"]   = ctx.account_risk_pct
                sizing_details["leverage_used_for_cap"] = _leverage_factor
                sizing_details["stop_distance_pct"]  = round(_atr_stop_dist * 100.0, 8) if _atr_stop_dist else None
                sizing_details["stop_distance_fraction"] = round(_atr_stop_dist, 10) if _atr_stop_dist else None
                sizing_details["max_risk_capital"]   = round(_max_risk_capital, 8) if _max_risk_capital is not None else None
                sizing_details["notional_cap_usdt"]  = round(_notional_cap, 8) if _notional_cap is not None else None
                sizing_details["margin_cap_usdt"]    = round(_margin_cap, 8) if _margin_cap is not None else None
                sizing_details["atr_cap_margin_usdt"] = round(_margin_cap, 8) if _margin_cap is not None else None
                sizing_details["theoretical_risk_usdt"] = round(_theoretical_risk_usdt, 8) if _theoretical_risk_usdt is not None else None
                sizing_details["theoretical_risk_pct"] = round(_theoretical_risk_pct, 8) if _theoretical_risk_pct is not None else None
                sizing_details["risk_warning"]       = _risk_warning
                _risk_level_raw = ctx.risk_level.value if hasattr(ctx.risk_level, "value") else str(ctx.risk_level)
                _risk_level_label = {
                    "low": "Conservative",
                    "medium": "Balanced",
                    "high": "Aggressive",
                }.get(str(_risk_level_raw).lower(), str(_risk_level_raw))
                sizing_details["risk_level"]         = _risk_level_raw
                sizing_details["risk_level_label"]   = _risk_level_label
                if _risk_warning:
                    sizing_details["admin_message"] = (
                        f"Your {_base_margin:.2f} USDT fixed margin was respected. "
                        f"Estimated risk is {(_theoretical_risk_pct or 0.0):.2f}% of equity, "
                        f"above selected {_risk_level_label} risk limit {ctx.account_risk_pct:.2f}%. "
                        f"Risk cap was NOT applied because fixed_amount strict mode is active."
                    )
                else:
                    sizing_details["admin_message"] = (
                        f"Your {_base_margin:.2f} USDT fixed margin was respected."
                    )
            else:
                risk_usdt = notional
                margin_required = notional / max(1.0, ctx.leverage)
    
                # Apply leverage penalty (compensating control) ONLY to dynamic risk modes!
                if ctx.leverage > 10.0:
                    penalty = 0.8 if ctx.leverage <= 20 else 0.6
                    qty *= penalty
                    notional *= penalty
                    risk_usdt *= penalty
                    margin_required *= penalty
                    adjustments.append(f"Leverage penalty ({ctx.leverage}x): {(1-penalty)*100:.0f}% reduction")
            
            
            # Drop the inline margin buffer & exposure cap duplicate checks here because 
            # Layer B (calculate_safe_size) validates bounds strictly after Policy execution.
        
        # -----------------------------------------------------------------
        # 9. Compound risk validation
        # -----------------------------------------------------------------
        adjusted_sl_pct = ctx.stop_loss_pct
        adjusted_tp_pct = ctx.take_profit_pct
        compound_risk = 0.0
        
        if action in (Action.OPEN_LONG, Action.OPEN_SHORT, Action.ADD_LONG, Action.ADD_SHORT,
                      Action.FLIP_TO_LONG, Action.FLIP_TO_SHORT):
            
            # Get max compound risk from profile
            profile = RISK_PROFILES.get(ctx.risk_level, RISK_PROFILES[RiskLevel.LOW])
            max_compound = profile.max_compound_risk_pct
            
            # Calculate stop distance as percentage
            stop_pct = ctx.stop_loss_pct
            if ctx.atr > 0 and ctx.entry_price > 0:
                # ATR-derived stop distance
                stop_pct = (ctx.atr * self.sl_multiplier) / ctx.entry_price
            
            # Incorporate expected slippage for realistic risk sizing
            if ctx.expected_slippage_pct > 0:
                stop_pct += (ctx.expected_slippage_pct * 2.0)
            
            compound_risk = stop_pct * ctx.leverage
            
            # Check against profile limit
            if compound_risk > max_compound:
                # Clamp stop loss
                max_sl_pct = max_compound / ctx.leverage
                adjusted_sl_pct = min(ctx.stop_loss_pct, max_sl_pct)
                compound_risk = adjusted_sl_pct * ctx.leverage
                adjustments.append(
                    f"SL clamped: {ctx.stop_loss_pct*100:.2f}% -> {adjusted_sl_pct*100:.2f}% "
                    f"(compound risk {max_compound*100:.0f}% limit)"
                )
            
            # Absolute safety cap (30%)
            ABSOLUTE_MAX = 0.30
            if compound_risk > ABSOLUTE_MAX:
                return PolicyDecision.blocked(
                    ReasonCode.COMPOUND_RISK_EXCEEDED,
                    f"Compound risk {compound_risk*100:.1f}% exceeds absolute cap {ABSOLUTE_MAX*100:.0f}%",
                    action=action,
                    compound_risk_pct=compound_risk,
                    pending_open=pending_open,
                    reentry_confirm_signal=reentry_sig,
                    reentry_confirm_count=reentry_cnt,
                )
            
            # Stop too wide check
            if adjusted_sl_pct > self.max_stop_distance_pct:
                return PolicyDecision.blocked(
                    ReasonCode.STOP_TOO_WIDE,
                    f"Stop distance {adjusted_sl_pct*100:.2f}% exceeds max {self.max_stop_distance_pct*100:.0f}%",
                    action=action,
                    pending_open=pending_open,
                    reentry_confirm_signal=reentry_sig,
                    reentry_confirm_count=reentry_cnt,
                )
        
        # -----------------------------------------------------------------
        # Decision: ALLOWED
        # -----------------------------------------------------------------
        return PolicyDecision(
            allowed=True,
            action=action,
            reason_code=ReasonCode.OK,
            reason=action_reason,
            quantity=qty,
            notional=notional,
            margin_required=margin_required,
            adjusted_stop_loss_pct=adjusted_sl_pct,
            adjusted_take_profit_pct=adjusted_tp_pct,
            risk_usdt=risk_usdt,
            compound_risk_pct=compound_risk,
            pending_open=pending_open,
            reentry_confirm_signal=reentry_sig,
            reentry_confirm_count=reentry_cnt,
            adjustments=adjustments,
            details=sizing_details,
        )
    
    def _determine_action(
        self, ctx: PolicyContext
    ) -> Tuple[Action, str, Optional[str], Optional[str], int]:
        """
        Determine action based on position and signal.
        
        Returns: (action, reason, pending_open, reentry_sig, reentry_cnt)
        """
        pending_open = ctx.pending_open
        reentry_sig = ctx.reentry_confirm_signal
        reentry_cnt = ctx.reentry_confirm_count
        
        pos = ctx.position.upper() if ctx.position else "NONE"
        sig = ctx.signal.upper() if ctx.signal else "HOLD"
        
        # ---------------------------
        # Position = FLAT/NONE
        # ---------------------------
        if pos in ("NONE", "FLAT"):
            # Reentry confirmation logic
            if ctx.reentry_confirmations > 1:
                if reentry_sig == sig:
                    reentry_cnt += 1
                else:
                    reentry_sig = sig
                    reentry_cnt = 1
                
                if reentry_cnt < ctx.reentry_confirmations:
                    return (
                        Action.HOLD,
                        f"Waiting for reentry confirmation ({reentry_cnt}/{ctx.reentry_confirmations})",
                        pending_open,
                        reentry_sig,
                        reentry_cnt,
                    )
                
                # Confirmed - reset
                reentry_sig = None
                reentry_cnt = 0
            
            if sig == "BUY":
                return Action.OPEN_LONG, "open_long", None, reentry_sig, reentry_cnt
            if sig == "SELL":
                return Action.OPEN_SHORT, "open_short", None, reentry_sig, reentry_cnt
        
        # ---------------------------
        # Position = LONG
        # ---------------------------
        if pos == "LONG":
            if sig == "BUY":
                if ctx.max_adds == 0:
                    return Action.HOLD, "POSITION_ADD_REJECTED_MAX_ADDS_DISABLED", pending_open, reentry_sig, reentry_cnt
                if ctx.adds < ctx.max_adds:
                    return Action.ADD_LONG, "add_long", pending_open, reentry_sig, reentry_cnt
                return Action.HOLD, "max_adds_reached", pending_open, reentry_sig, reentry_cnt
            
            if sig == "SELL":
                if ctx.trade_mode == "flip":
                    return Action.FLIP_TO_SHORT, "flip_long_to_short", "SELL", reentry_sig, reentry_cnt
                # Normal mode: opposing signal does NOT close the position.
                # Positions exit only via SL, TP, or explicit exit logic.
                return Action.HOLD, "opposing_signal_hold", pending_open, reentry_sig, reentry_cnt

        # ---------------------------
        # Position = SHORT
        # ---------------------------
        if pos == "SHORT":
            if sig == "SELL":
                if ctx.max_adds == 0:
                    return Action.HOLD, "POSITION_ADD_REJECTED_MAX_ADDS_DISABLED", pending_open, reentry_sig, reentry_cnt
                if ctx.adds < ctx.max_adds:
                    return Action.ADD_SHORT, "add_short", pending_open, reentry_sig, reentry_cnt
                return Action.HOLD, "max_adds_reached", pending_open, reentry_sig, reentry_cnt

            if sig == "BUY":
                if ctx.trade_mode == "flip":
                    return Action.FLIP_TO_LONG, "flip_short_to_long", "BUY", reentry_sig, reentry_cnt
                # Normal mode: opposing signal does NOT close the position.
                # Positions exit only via SL, TP, or explicit exit logic.
                return Action.HOLD, "opposing_signal_hold", pending_open, reentry_sig, reentry_cnt
        
        return Action.HOLD, "no_action_matched", pending_open, reentry_sig, reentry_cnt


# =============================================================================
# PER-BOT FACTORY  (replaces shared singleton)
# =============================================================================
# Each bot instance owns its own PolicyEngine so that Bot A's budget engine
# (and its open positions / consumed risk) is never reused to evaluate Bot B.

import threading as _pe_threading

_policy_engines: dict[str, "PolicyEngine"] = {}
_pe_lock = _pe_threading.Lock()


def get_policy_engine(
    bot_id: str = "default",
    budget_engine: Optional["RiskBudgetEngine"] = None,
    circuit_registry: Optional["CircuitBreakerRegistry"] = None,
    min_confidence: float = 0.10,
    **kwargs,
) -> "PolicyEngine":
    """Return the PolicyEngine for *bot_id*, creating it on first call."""
    with _pe_lock:
        if bot_id not in _policy_engines:
            _policy_engines[bot_id] = PolicyEngine(
                budget_engine=budget_engine,
                circuit_registry=circuit_registry,
                min_confidence=min_confidence,
                **kwargs,
            )
        return _policy_engines[bot_id]


def reset_policy_engine(bot_id: str = "default") -> None:
    """Evict a single bot's policy engine (for testing or bot teardown)."""
    with _pe_lock:
        _policy_engines.pop(bot_id, None)


def reset_all_policy_engines() -> None:
    """Clear all per-bot engines. Use in tests only."""
    with _pe_lock:
        _policy_engines.clear()
