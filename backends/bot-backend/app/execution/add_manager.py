# STATUS: STUB — Not yet wired into runner or executor.
# AddManager defines add eligibility rules for future partial scaling-in.
# Will be wired in Phase 3 Step 3C (partial take profits / position scaling).
# DO NOT instantiate from runner until that phase is complete.

"""
Position Add Manager - A+ Add Rules

Core principle: Adds are a PRIVILEGE, not a right.
The bot earns the right to add only after the market proves it right.

Golden rule: NEVER add to a losing or unproven position.
"""
from __future__ import annotations
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict
from datetime import datetime, timedelta


class AddPhase(Enum):
    """Add state machine phases."""
    NO_ADD_ALLOWED = "NO_ADD_ALLOWED"      # TP1 not hit, position unproven
    ADD_ELIGIBLE = "ADD_ELIGIBLE"          # TP1 hit + BE active, can add
    ADD_PENDING = "ADD_PENDING"            # Add order inflight
    ADD_ACTIVE = "ADD_ACTIVE"              # Add filled, managing
    ADD_EXITED = "ADD_EXITED"              # Add closed, back to runner
    RUNNER_ONLY = "RUNNER_ONLY"            # No more adds allowed


class AddRejection(Enum):
    """Reasons why an add is rejected."""
    NONE = "NONE"
    POSITION_NEGATIVE = "POSITION_NEGATIVE"
    TP1_NOT_HIT = "TP1_NOT_HIT"
    BE_NOT_SET = "BE_NOT_SET"
    REGIME_INVALID = "REGIME_INVALID"
    CONFIDENCE_TOO_LOW = "CONFIDENCE_TOO_LOW"
    IN_RESET_OR_COOLDOWN = "IN_RESET_OR_COOLDOWN"
    MIN_HOLD_NOT_MET = "MIN_HOLD_NOT_MET"
    MAX_ADDS_REACHED = "MAX_ADDS_REACHED"
    ADD_ALREADY_PENDING = "ADD_ALREADY_PENDING"
    ADD_ALREADY_ACTIVE = "ADD_ALREADY_ACTIVE"
    DRAWDOWN_LOCKOUT = "DRAWDOWN_LOCKOUT"
    WOULD_WIDEN_SL = "WOULD_WIDEN_SL"
    STRUCTURE_NOT_CONFIRMED = "STRUCTURE_NOT_CONFIRMED"


@dataclass
class AddConfig:
    """Configuration for add rules - locked per A+ spec."""
    # Eligibility
    min_confidence_precision: float = 0.85
    min_confidence_flow: float = 0.75
    confidence_margin_over_entry: float = 0.05  # Must be entry + this
    
    # Limits
    max_adds_per_position: int = 1
    
    # Sizing
    add_size_fraction: float = 0.40  # 40% of original size
    add_size_fraction_flow: float = 0.30  # 30% for flow mode
    
    # TP/SL for adds
    add_tp_r: float = 1.0  # Quick TP for adds
    add_sl_atr_mult: float = 0.8  # Tighter SL for adds
    
    # Time stops (in candles)
    add_max_candles_precision: int = 6  # ~1.5h on 15m
    add_max_candles_flow: int = 10  # ~50min on 5m
    
    # Valid regimes for adds
    precision_add_regimes: list = None
    flow_add_regimes: list = None
    
    def __post_init__(self):
        if self.precision_add_regimes is None:
            self.precision_add_regimes = ["STRONG_TREND"]
        if self.flow_add_regimes is None:
            self.flow_add_regimes = ["WEAK_TREND"]


@dataclass
class AddState:
    """State for add management on a position."""
    symbol: str
    phase: AddPhase
    add_count: int = 0
    
    # Original entry context
    original_entry_confidence: float = 0.0
    original_size: float = 0.0
    
    # Add details (when active)
    add_entry_price: Optional[float] = None
    add_entry_time: Optional[datetime] = None
    add_size: Optional[float] = None
    add_stop: Optional[float] = None
    add_tp: Optional[float] = None
    add_candle_count: int = 0


@dataclass
class AddDecision:
    """Result of add eligibility check."""
    allowed: bool
    rejection: AddRejection
    reason: str
    
    # If allowed
    recommended_size: float = 0.0
    recommended_stop: float = 0.0
    recommended_tp: float = 0.0


class AddManager:
    """
    Manages position adds with strict eligibility rules.
    
    Core rules:
    - Adds only after TP1 hit (position proven)
    - Adds never increase downside risk
    - Adds are smaller than original
    - Max 1 add per position
    """
    
    def __init__(self, config: Optional[AddConfig] = None):
        self.config = config or AddConfig()
        self._states: Dict[str, AddState] = {}
    
    def _get_state(self, symbol: str) -> AddState:
        if symbol not in self._states:
            self._states[symbol] = AddState(
                symbol=symbol,
                phase=AddPhase.NO_ADD_ALLOWED,
            )
        return self._states[symbol]
    
    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================
    
    def on_position_opened(
        self,
        symbol: str,
        entry_confidence: float,
        size: float,
    ):
        """Called when a new position is opened."""
        self._states[symbol] = AddState(
            symbol=symbol,
            phase=AddPhase.NO_ADD_ALLOWED,
            add_count=0,
            original_entry_confidence=entry_confidence,
            original_size=size,
        )
    
    def on_tp1_hit(self, symbol: str):
        """Called when TP1 is hit and BE is set. Enables add eligibility."""
        state = self._get_state(symbol)
        if state.phase == AddPhase.NO_ADD_ALLOWED:
            state.phase = AddPhase.ADD_ELIGIBLE
    
    def on_position_closed(self, symbol: str):
        """Called when position is fully closed."""
        if symbol in self._states:
            del self._states[symbol]
    
    def on_add_filled(
        self,
        symbol: str,
        entry_price: float,
        size: float,
        stop: float,
        tp: float,
    ):
        """Called when add order fills."""
        state = self._get_state(symbol)
        state.phase = AddPhase.ADD_ACTIVE
        state.add_count += 1
        state.add_entry_price = entry_price
        state.add_entry_time = datetime.utcnow()
        state.add_size = size
        state.add_stop = stop
        state.add_tp = tp
        state.add_candle_count = 0
    
    def on_add_exited(self, symbol: str, reason: str = "tp"):
        """Called when add portion exits."""
        state = self._get_state(symbol)
        state.phase = AddPhase.RUNNER_ONLY  # No more adds after first exits
        state.add_entry_price = None
        state.add_entry_time = None
        state.add_size = None
    
    def on_candle_close(self, symbol: str):
        """Called on each candle close to track add duration."""
        state = self._get_state(symbol)
        if state.phase == AddPhase.ADD_ACTIVE:
            state.add_candle_count += 1
    
    # =========================================================================
    # ELIGIBILITY CHECK
    # =========================================================================
    
    def can_add(
        self,
        symbol: str,
        signal_confidence: float,
        current_price: float,
        entry_price: float,
        current_stop: float,
        atr: float,
        mode: str,
        regime: str,
        unrealized_pnl: float,
        is_be_active: bool,
        is_in_drawdown: bool = False,
        has_pending_order: bool = False,
    ) -> AddDecision:
        """
        Check if an add is allowed. ALL conditions must pass.
        """
        state = self._get_state(symbol)
        
        # =====================================================================
        # ABSOLUTE BLOCKS (any one = no add)
        # =====================================================================
        
        # Block 1: Position is negative
        if unrealized_pnl < 0:
            return AddDecision(False, AddRejection.POSITION_NEGATIVE, 
                               f"PnL={unrealized_pnl:.2f} < 0")
        
        # Block 2: TP1 not hit (phase check)
        if state.phase == AddPhase.NO_ADD_ALLOWED:
            return AddDecision(False, AddRejection.TP1_NOT_HIT,
                               "TP1 not hit yet")
        
        # Block 3: BE not active
        if not is_be_active:
            return AddDecision(False, AddRejection.BE_NOT_SET,
                               "Break-even not set")
        
        # Block 4: Max adds reached
        if state.add_count >= self.config.max_adds_per_position:
            return AddDecision(False, AddRejection.MAX_ADDS_REACHED,
                               f"Add count {state.add_count} >= max {self.config.max_adds_per_position}")
        
        # Block 5: Add already pending or active
        if state.phase == AddPhase.ADD_PENDING:
            return AddDecision(False, AddRejection.ADD_ALREADY_PENDING, "")
        if state.phase == AddPhase.ADD_ACTIVE:
            return AddDecision(False, AddRejection.ADD_ALREADY_ACTIVE, "")
        if state.phase == AddPhase.RUNNER_ONLY:
            return AddDecision(False, AddRejection.MAX_ADDS_REACHED, "Runner only mode")
        
        # Block 6: Drawdown lockout
        if is_in_drawdown:
            return AddDecision(False, AddRejection.DRAWDOWN_LOCKOUT,
                               "Account in drawdown recovery")
        
        # =====================================================================
        # QUALITY CONDITIONS
        # =====================================================================
        
        # Condition 1: Regime must be valid for mode
        valid_regimes = (self.config.precision_add_regimes if mode == "PRECISION" 
                         else self.config.flow_add_regimes)
        if regime not in valid_regimes:
            return AddDecision(False, AddRejection.REGIME_INVALID,
                               f"Regime {regime} not in {valid_regimes}")
        
        # Condition 2: Confidence must be high enough
        min_conf = (self.config.min_confidence_precision if mode == "PRECISION"
                    else self.config.min_confidence_flow)
        if signal_confidence < min_conf:
            return AddDecision(False, AddRejection.CONFIDENCE_TOO_LOW,
                               f"Confidence {signal_confidence:.2f} < {min_conf}")
        
        # Condition 3: Confidence must be higher than entry
        required_conf = state.original_entry_confidence + self.config.confidence_margin_over_entry
        if signal_confidence < required_conf:
            return AddDecision(False, AddRejection.CONFIDENCE_TOO_LOW,
                               f"Confidence {signal_confidence:.2f} < entry+margin {required_conf:.2f}")
        
        # =====================================================================
        # CALCULATE ADD PARAMETERS
        # =====================================================================
        
        add_size_frac = (self.config.add_size_fraction if mode == "PRECISION"
                         else self.config.add_size_fraction_flow)
        recommended_size = state.original_size * add_size_frac
        
        # Add stop: tighter than original (0.8 ATR from current price)
        add_stop_distance = atr * self.config.add_sl_atr_mult
        
        # Determine side from entry vs current
        is_long = current_price > entry_price
        
        if is_long:
            recommended_stop = current_price - add_stop_distance
            # Add stop must not be below BE stop
            if recommended_stop < current_stop:
                return AddDecision(False, AddRejection.WOULD_WIDEN_SL,
                                   f"Add stop {recommended_stop:.2f} < BE stop {current_stop:.2f}")
            recommended_tp = current_price + (atr * self.config.add_tp_r)
        else:
            recommended_stop = current_price + add_stop_distance
            if recommended_stop > current_stop:
                return AddDecision(False, AddRejection.WOULD_WIDEN_SL,
                                   f"Add stop {recommended_stop:.2f} > BE stop {current_stop:.2f}")
            recommended_tp = current_price - (atr * self.config.add_tp_r)
        
        return AddDecision(
            allowed=True,
            rejection=AddRejection.NONE,
            reason="all_conditions_met",
            recommended_size=recommended_size,
            recommended_stop=recommended_stop,
            recommended_tp=recommended_tp,
        )
    
    def should_exit_add(
        self,
        symbol: str,
        current_price: float,
        mode: str,
    ) -> tuple[bool, str]:
        """
        Check if add portion should exit (time stop or price).
        """
        state = self._get_state(symbol)
        
        if state.phase != AddPhase.ADD_ACTIVE:
            return False, "no_active_add"
        
        # Check time stop
        max_candles = (self.config.add_max_candles_precision if mode == "PRECISION"
                       else self.config.add_max_candles_flow)
        if state.add_candle_count >= max_candles:
            return True, "add_time_stop"
        
        # Check add TP hit
        if state.add_entry_price and state.add_tp:
            is_long = state.add_tp > state.add_entry_price
            if is_long and current_price >= state.add_tp:
                return True, "add_tp_hit"
            if not is_long and current_price <= state.add_tp:
                return True, "add_tp_hit"
        
        # Check add SL hit
        if state.add_stop:
            is_long = state.add_entry_price and state.add_tp and state.add_tp > state.add_entry_price
            if is_long and current_price <= state.add_stop:
                return True, "add_sl_hit"
            if not is_long and current_price >= state.add_stop:
                return True, "add_sl_hit"
        
        return False, ""
    
    # =========================================================================
    # STATE QUERIES
    # =========================================================================
    
    def get_state(self, symbol: str) -> dict:
        """Get current add state for a symbol."""
        state = self._get_state(symbol)
        
        result = {
            "symbol": symbol,
            "phase": state.phase.value,
            "add_count": state.add_count,
            "max_adds": self.config.max_adds_per_position,
            "can_add": state.phase == AddPhase.ADD_ELIGIBLE,
        }
        
        if state.phase == AddPhase.ADD_ACTIVE:
            result["add"] = {
                "entry_price": state.add_entry_price,
                "size": state.add_size,
                "stop": state.add_stop,
                "tp": state.add_tp,
                "candle_count": state.add_candle_count,
            }
        
        return result
