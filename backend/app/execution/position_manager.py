"""
Position Manager - A+ Execution Phase State Machine

Implements position phases:
- ENTERED → SEEKING_TP1 → TP1_TAKEN → RUNNER_TRAILING → EXITING
- RESET_PENDING (flat, waiting for confirmation)

Handles:
- Phase transitions
- Break-even moves after TP1
- Trailing stops
- Time-based exits
"""
from __future__ import annotations
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, List
from datetime import datetime, timedelta


class PositionPhase(Enum):
    """Position lifecycle phases."""
    FLAT = "FLAT"                    # No position
    ENTERED = "ENTERED"              # Just opened
    SEEKING_TP1 = "SEEKING_TP1"      # Working toward TP1
    TP1_TAKEN = "TP1_TAKEN"          # TP1 hit, break-even set
    RUNNER_TRAILING = "RUNNER_TRAILING"  # Trailing the runner
    EXITING = "EXITING"              # Exit in progress
    RESET_PENDING = "RESET_PENDING"  # Closed due to opposite signal, waiting


class PositionSide(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


@dataclass
class TPLevels:
    """Take profit levels for a position."""
    tp1_price: float
    tp1_r: float
    tp1_close_fraction: float
    tp2_price: float
    tp2_r: float
    tp1_hit: bool = False
    tp1_fill_price: Optional[float] = None


@dataclass
class StopLevels:
    """Stop loss tracking."""
    initial_stop: float
    current_stop: float
    break_even_price: Optional[float] = None
    is_break_even: bool = False
    trail_atr_mult: float = 1.2


@dataclass
class PositionState:
    """Complete state for a position."""
    symbol: str
    side: PositionSide
    phase: PositionPhase
    
    # Entry info
    entry_price: float
    entry_time: datetime
    entry_qty: float
    current_qty: float
    
    # R calculation
    r_value: float  # Entry - Initial Stop (absolute)
    
    # TP/SL
    tp: TPLevels
    sl: StopLevels
    
    # Trailing
    highest_since_entry: float = 0.0  # For longs
    lowest_since_entry: float = 999999.0  # For shorts
    
    # Time tracking
    max_duration_seconds: int = 10800  # 3 hours default
    
    # Metadata
    strategy_name: str = ""
    mode: str = "PRECISION"  # PRECISION or FLOW


@dataclass 
class ResetState:
    """State for reset-pending symbols."""
    symbol: str
    closed_side: PositionSide  # Side we just closed
    target_side: PositionSide  # Side we want to enter
    reset_started_at: datetime
    confirm_count: int = 0  # Consecutive confirmations
    cooldown_until: Optional[datetime] = None
    close_reason: str = "signal"  # "signal", "stop_loss", "time_stop"


class StrategyType(Enum):
    """Strategy types with different SL/TP profiles."""
    TREND = "TREND"
    MEAN_REVERSION = "MEAN_REVERSION"
    BREAKOUT = "BREAKOUT"


@dataclass
class StrategyTypeConfig:
    """TP/SL config per strategy type (locked spec)."""
    k_sl: float
    tp1_r: float
    tp2_r: float
    tp1_close_fraction: float
    k_trail: float
    max_sl_atr: float  # Reject if SL exceeds this


# Locked strategy-type configs per A+ spec
STRATEGY_TYPE_CONFIGS = {
    StrategyType.TREND: StrategyTypeConfig(
        k_sl=1.2,       # Slightly wider, breathing room
        tp1_r=1.0,
        tp2_r=2.2,
        tp1_close_fraction=0.5,
        k_trail=1.2,
        max_sl_atr=2.0,
    ),
    StrategyType.MEAN_REVERSION: StrategyTypeConfig(
        k_sl=0.8,       # Tight - fail fast
        tp1_r=0.8,      # Win-rate biased
        tp2_r=1.6,
        tp1_close_fraction=0.6,
        k_trail=0.9,
        max_sl_atr=1.2,  # Reject wide stops
    ),
    StrategyType.BREAKOUT: StrategyTypeConfig(
        k_sl=1.0,       # Structure-first
        tp1_r=1.0,
        tp2_r=2.5,
        tp1_close_fraction=0.5,
        k_trail=1.2,
        max_sl_atr=2.0,
    ),
}


@dataclass
class PositionManagerConfig:
    """Configuration for position management - A+ Final Spec."""
    # Anti-flip cooldowns
    min_hold_seconds: int = 600        # 10 min - no signal exits before this
    reset_cooldown_seconds: int = 900  # 15 min - after opposite signal close
    sl_cooldown_seconds: int = 1800    # 30 min - after stop loss (longer!)
    confirm_ticks_required: int = 3
    reentry_min_confidence: float = 0.80
    
    # TP/SL (Precision defaults)
    precision_k_sl: float = 1.2
    precision_tp1_r: float = 1.0
    precision_tp2_r: float = 2.2
    precision_tp1_close_fraction: float = 0.5
    precision_k_trail: float = 1.2
    precision_max_duration_seconds: int = 10800  # 3h
    
    # TP/SL (Flow defaults)
    flow_k_sl: float = 0.8
    flow_tp1_r: float = 0.8
    flow_tp2_r: float = 1.6
    flow_tp1_close_fraction: float = 0.6
    flow_k_trail: float = 0.9
    flow_max_duration_seconds: int = 7200  # 2h
    
    # Break-even
    be_fee_buffer_mult: float = 1.2  # 1.2x fees as buffer
    taker_fee_rate: float = 0.0005  # 0.05%
    
    # Fee-aware gating
    min_edge_multiplier: float = 2.0  # TP1 must be 2x fees


class PositionManager:
    """
    Manages position phases and state transitions.
    
    Core principles:
    - No flips: close → reset → confirm → re-enter
    - Break-even after TP1
    - Trail after TP1
    - Time-based exit if stagnant
    """
    
    def __init__(self, config: Optional[PositionManagerConfig] = None):
        self.config = config or PositionManagerConfig()
        self._positions: Dict[str, PositionState] = {}
        self._resets: Dict[str, ResetState] = {}
    
    def get_position(self, symbol: str) -> Optional[PositionState]:
        """Get current position state for a symbol."""
        return self._positions.get(symbol)
    
    def get_reset(self, symbol: str) -> Optional[ResetState]:
        """Get reset state for a symbol."""
        return self._resets.get(symbol)
    
    def is_in_position(self, symbol: str) -> bool:
        """Check if we have an open position."""
        pos = self._positions.get(symbol)
        return pos is not None and pos.phase not in [PositionPhase.FLAT, PositionPhase.EXITING]
    
    def is_in_reset(self, symbol: str) -> bool:
        """Check if symbol is in reset-pending state."""
        return symbol in self._resets
    
    def get_mode_config(self, mode: str) -> dict:
        """Get TP/SL config for a trading mode."""
        if mode == "FLOW":
            return {
                "k_sl": self.config.flow_k_sl,
                "tp1_r": self.config.flow_tp1_r,
                "tp2_r": self.config.flow_tp2_r,
                "tp1_close_fraction": self.config.flow_tp1_close_fraction,
                "k_trail": self.config.flow_k_trail,
                "max_duration": self.config.flow_max_duration_seconds,
            }
        return {
            "k_sl": self.config.precision_k_sl,
            "tp1_r": self.config.precision_tp1_r,
            "tp2_r": self.config.precision_tp2_r,
            "tp1_close_fraction": self.config.precision_tp1_close_fraction,
            "k_trail": self.config.precision_k_trail,
            "max_duration": self.config.precision_max_duration_seconds,
        }
    
    # =========================================================================
    # POSITION LIFECYCLE
    # =========================================================================
    
    def open_position(
        self,
        symbol: str,
        side: PositionSide,
        entry_price: float,
        qty: float,
        stop_price: float,
        tp1_price: float,
        tp2_price: float,
        mode: str = "PRECISION",
        strategy_name: str = "",
    ) -> PositionState:
        """
        Open a new position and initialize state.
        """
        mode_cfg = self.get_mode_config(mode)
        
        # Calculate R value
        if side == PositionSide.LONG:
            r_value = abs(entry_price - stop_price)
        else:
            r_value = abs(stop_price - entry_price)
        
        state = PositionState(
            symbol=symbol,
            side=side,
            phase=PositionPhase.SEEKING_TP1,  # Skip ENTERED, go straight to seeking
            entry_price=entry_price,
            entry_time=datetime.utcnow(),
            entry_qty=qty,
            current_qty=qty,
            r_value=r_value,
            tp=TPLevels(
                tp1_price=tp1_price,
                tp1_r=mode_cfg["tp1_r"],
                tp1_close_fraction=mode_cfg["tp1_close_fraction"],
                tp2_price=tp2_price,
                tp2_r=mode_cfg["tp2_r"],
            ),
            sl=StopLevels(
                initial_stop=stop_price,
                current_stop=stop_price,
                trail_atr_mult=mode_cfg["k_trail"],
            ),
            highest_since_entry=entry_price,
            lowest_since_entry=entry_price,
            max_duration_seconds=mode_cfg["max_duration"],
            strategy_name=strategy_name,
            mode=mode,
        )
        
        self._positions[symbol] = state
        
        # Clear any reset state
        if symbol in self._resets:
            del self._resets[symbol]
        
        return state
    
    def update_price(self, symbol: str, current_price: float, current_atr: float) -> Optional[str]:
        """
        Update position with current price. Returns action if needed.
        
        Actions: "HIT_TP1", "HIT_TP2", "HIT_STOP", "TRAIL_UPDATED", "TIME_EXIT", None
        """
        pos = self._positions.get(symbol)
        if not pos or pos.phase == PositionPhase.FLAT:
            return None
        
        # Update high/low tracking
        if pos.side == PositionSide.LONG:
            pos.highest_since_entry = max(pos.highest_since_entry, current_price)
        else:
            pos.lowest_since_entry = min(pos.lowest_since_entry, current_price)
        
        # Check time exit
        if self._check_time_exit(pos):
            pos.phase = PositionPhase.EXITING
            return "TIME_EXIT"
        
        # Check stop hit
        if self._check_stop_hit(pos, current_price):
            pos.phase = PositionPhase.EXITING
            return "HIT_STOP"
        
        # Phase-specific updates
        if pos.phase == PositionPhase.SEEKING_TP1:
            if self._check_tp1_hit(pos, current_price):
                pos.tp.tp1_hit = True
                pos.tp.tp1_fill_price = current_price
                pos.phase = PositionPhase.TP1_TAKEN
                self._move_to_break_even(pos)
                return "HIT_TP1"
        
        elif pos.phase == PositionPhase.TP1_TAKEN:
            # Transition to trailing
            pos.phase = PositionPhase.RUNNER_TRAILING
        
        elif pos.phase == PositionPhase.RUNNER_TRAILING:
            # Update trailing stop
            if self._update_trailing_stop(pos, current_price, current_atr):
                return "TRAIL_UPDATED"
            
            # Check TP2
            if self._check_tp2_hit(pos, current_price):
                pos.phase = PositionPhase.EXITING
                return "HIT_TP2"
        
        return None
    
    def close_position(self, symbol: str, reason: str = "manual") -> Optional[PositionState]:
        """
        Close a position and return the final state.
        """
        pos = self._positions.get(symbol)
        if not pos:
            return None
        
        pos.phase = PositionPhase.FLAT
        del self._positions[symbol]
        return pos
    
    def close_for_opposite_signal(self, symbol: str, target_side: PositionSide) -> Optional[PositionState]:
        """
        Close position due to opposite signal and enter reset state.
        This is the anti-flip mechanism.
        """
        pos = self._positions.get(symbol)
        if not pos:
            return None
        
        closed_side = pos.side
        final_state = self.close_position(symbol, reason="opposite_signal")
        
        # Enter reset-pending state
        self._resets[symbol] = ResetState(
            symbol=symbol,
            closed_side=closed_side,
            target_side=target_side,
            reset_started_at=datetime.utcnow(),
            confirm_count=0,
            cooldown_until=datetime.utcnow() + timedelta(seconds=self.config.reset_cooldown_seconds),
            close_reason="signal",
        )
        
        return final_state
    
    def close_for_stop_hit(self, symbol: str) -> Optional[PositionState]:
        """
        Close position due to stop loss hit.
        Uses LONGER cooldown than signal-based close (prevents revenge trading).
        """
        pos = self._positions.get(symbol)
        if not pos:
            return None
        
        closed_side = pos.side
        final_state = self.close_position(symbol, reason="stop_loss")
        
        # Enter reset with LONGER cooldown (SL cooldown > reset cooldown)
        self._resets[symbol] = ResetState(
            symbol=symbol,
            closed_side=closed_side,
            target_side=PositionSide.FLAT,  # No target yet
            reset_started_at=datetime.utcnow(),
            confirm_count=0,
            cooldown_until=datetime.utcnow() + timedelta(seconds=self.config.sl_cooldown_seconds),
            close_reason="stop_loss",
        )
        
        return final_state
    
    def close_for_time_stop(self, symbol: str) -> Optional[PositionState]:
        """
        Close position due to time stop.
        Uses standard reset cooldown.
        """
        pos = self._positions.get(symbol)
        if not pos:
            return None
        
        closed_side = pos.side
        final_state = self.close_position(symbol, reason="time_stop")
        
        # Enter reset state
        self._resets[symbol] = ResetState(
            symbol=symbol,
            closed_side=closed_side,
            target_side=PositionSide.FLAT,
            reset_started_at=datetime.utcnow(),
            confirm_count=0,
            cooldown_until=datetime.utcnow() + timedelta(seconds=self.config.reset_cooldown_seconds),
            close_reason="time_stop",
        )
        
        return final_state
    
    # =========================================================================
    # ANTI-FLIP LOGIC
    # =========================================================================
    
    def can_open_after_reset(
        self,
        symbol: str,
        signal_side: PositionSide,
        confidence: float,
    ) -> tuple[bool, str]:
        """
        Check if we can open a new position after reset.
        
        Returns: (allowed, reason)
        """
        reset = self._resets.get(symbol)
        if not reset:
            return True, "no_reset_pending"
        
        now = datetime.utcnow()
        
        # Check cooldown
        if reset.cooldown_until and now < reset.cooldown_until:
            remaining = (reset.cooldown_until - now).seconds
            return False, f"cooldown_active:{remaining}s"
        
        # Check if signal matches expected target
        if signal_side != reset.target_side:
            # Signal changed direction - reset the confirmation counter
            reset.target_side = signal_side
            reset.confirm_count = 0
            return False, "signal_direction_changed"
        
        # Check confidence
        if confidence < self.config.reentry_min_confidence:
            return False, f"confidence_too_low:{confidence:.2f}"
        
        # Increment confirmation
        reset.confirm_count += 1
        
        # Check confirmation count
        if reset.confirm_count < self.config.confirm_ticks_required:
            return False, f"confirming:{reset.confirm_count}/{self.config.confirm_ticks_required}"
        
        # All checks passed - clear reset and allow
        del self._resets[symbol]
        return True, "confirmed"
    
    def can_exit_position(self, symbol: str, reason: str = "signal") -> tuple[bool, str]:
        """
        Check if we can exit a position (respects min hold time).
        
        TP/SL exits always allowed. Signal-based exits respect min hold.
        """
        if reason in ["stop", "tp1", "tp2", "time"]:
            return True, "always_allowed"
        
        pos = self._positions.get(symbol)
        if not pos:
            return True, "no_position"
        
        elapsed = (datetime.utcnow() - pos.entry_time).total_seconds()
        if elapsed < self.config.min_hold_seconds:
            remaining = self.config.min_hold_seconds - elapsed
            return False, f"min_hold:{int(remaining)}s"
        
        return True, "hold_time_passed"
    
    # =========================================================================
    # INTERNAL HELPERS
    # =========================================================================
    
    def _check_stop_hit(self, pos: PositionState, price: float) -> bool:
        """Check if stop loss is hit."""
        if pos.side == PositionSide.LONG:
            return price <= pos.sl.current_stop
        return price >= pos.sl.current_stop
    
    def _check_tp1_hit(self, pos: PositionState, price: float) -> bool:
        """Check if TP1 is hit."""
        if pos.side == PositionSide.LONG:
            return price >= pos.tp.tp1_price
        return price <= pos.tp.tp1_price
    
    def _check_tp2_hit(self, pos: PositionState, price: float) -> bool:
        """Check if TP2 is hit."""
        if pos.side == PositionSide.LONG:
            return price >= pos.tp.tp2_price
        return price <= pos.tp.tp2_price
    
    def _check_time_exit(self, pos: PositionState) -> bool:
        """Check if max duration exceeded (only before TP1)."""
        if pos.tp.tp1_hit:
            return False  # Allow more time for runner
        
        elapsed = (datetime.utcnow() - pos.entry_time).total_seconds()
        return elapsed > pos.max_duration_seconds
    
    def _move_to_break_even(self, pos: PositionState):
        """Move stop to break-even after TP1."""
        fee_buffer = pos.entry_price * self.config.taker_fee_rate * 2 * self.config.be_fee_buffer_mult
        
        if pos.side == PositionSide.LONG:
            be_price = pos.entry_price + fee_buffer
        else:
            be_price = pos.entry_price - fee_buffer
        
        pos.sl.break_even_price = be_price
        pos.sl.current_stop = be_price
        pos.sl.is_break_even = True
    
    def _update_trailing_stop(self, pos: PositionState, price: float, atr: float) -> bool:
        """Update trailing stop. Returns True if stop was moved."""
        trail_distance = atr * pos.sl.trail_atr_mult
        
        if pos.side == PositionSide.LONG:
            new_stop = pos.highest_since_entry - trail_distance
            if new_stop > pos.sl.current_stop:
                pos.sl.current_stop = new_stop
                return True
        else:
            new_stop = pos.lowest_since_entry + trail_distance
            if new_stop < pos.sl.current_stop:
                pos.sl.current_stop = new_stop
                return True
        
        return False
    
    # =========================================================================
    # STATE SERIALIZATION
    # =========================================================================
    
    def get_state_summary(self, symbol: str) -> dict:
        """Get summary of position/reset state for a symbol."""
        pos = self._positions.get(symbol)
        reset = self._resets.get(symbol)
        
        result = {
            "symbol": symbol,
            "has_position": pos is not None,
            "has_reset": reset is not None,
        }
        
        if pos:
            result["position"] = {
                "side": pos.side.value,
                "phase": pos.phase.value,
                "entry_price": pos.entry_price,
                "current_stop": pos.sl.current_stop,
                "tp1_price": pos.tp.tp1_price,
                "tp1_hit": pos.tp.tp1_hit,
                "is_break_even": pos.sl.is_break_even,
                "mode": pos.mode,
            }
        
        if reset:
            result["reset"] = {
                "closed_side": reset.closed_side.value,
                "target_side": reset.target_side.value,
                "confirm_count": reset.confirm_count,
                "cooldown_until": reset.cooldown_until.isoformat() if reset.cooldown_until else None,
            }
        
        return result


# =============================================================================
# LEGACY COMPATIBILITY - functions expected by runner.py
# =============================================================================

def should_exit(
    current_price: float = 0.0,
    entry_price: float = 0.0,
    stop_loss: float = 0.0,
    take_profit: float = 0.0,
    side: str = "NONE",
    **kwargs,  # ✅ ADD: ignore extra args (like 'position' object passed by runner)
) -> tuple[bool, str]:
    """
    Legacy function for backward compatibility with runner.py.
    Checks if position should exit based on price hitting SL or TP.
    
    Returns: (should_exit, reason)
    """
    # ✅ FIX: Caller might pass 'position' object instead of unpacked args
    if "position" in kwargs:
         position = kwargs["position"]
         # Try to extract from dict or object
         if isinstance(position, dict):
             side = side if side != "NONE" else position.get("side", "NONE")
             stop_loss = stop_loss if stop_loss != 0.0 else float(position.get("stop_loss", 0.0))
             take_profit = take_profit if take_profit != 0.0 else float(position.get("take_profit", 0.0))
         else:
             # Assume object
             side = side if side != "NONE" else getattr(position, "side", "NONE")
             stop_loss = stop_loss if stop_loss != 0.0 else getattr(position, "stop_loss", 0.0)
             take_profit = take_profit if take_profit != 0.0 else getattr(position, "take_profit", 0.0)

    if side == "LONG":
        if current_price <= stop_loss and stop_loss > 0:
            return True, "stop_loss"
        if current_price >= take_profit and take_profit > 0:
            return True, "take_profit"
    elif side == "SHORT":
        if current_price >= stop_loss and stop_loss > 0:
            return True, "stop_loss"
        if current_price <= take_profit and take_profit > 0:
            return True, "take_profit"
    
    return False, ""
