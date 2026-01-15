"""
Anti-Flip Controller - A+ Execution

Prevents flip churn by implementing:
- Minimum hold time
- Cooldown after close
- Confirmation requirement for re-entry
- "Close → Reset → Confirm → Re-enter" flow
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional, List
from datetime import datetime, timedelta
from enum import Enum


class FlipAction(Enum):
    """Possible actions when opposite signal appears."""
    HOLD = "HOLD"              # Ignore signal, keep position
    CLOSE_AND_RESET = "CLOSE_AND_RESET"  # Close, enter reset state
    ALLOW_ENTRY = "ALLOW_ENTRY"          # Reset complete, can enter


@dataclass
class FlipState:
    """State for a symbol's flip control."""
    symbol: str
    
    # Current position tracking
    in_position: bool = False
    position_side: Optional[str] = None  # "LONG" or "SHORT"
    position_opened_at: Optional[datetime] = None
    
    # Reset tracking
    in_reset: bool = False
    reset_started_at: Optional[datetime] = None
    reset_target_side: Optional[str] = None
    reset_confirm_count: int = 0
    
    # Cooldown
    cooldown_until: Optional[datetime] = None
    
    # Opposite signal tracking
    opposite_signal_count: int = 0
    last_signal: Optional[str] = None


@dataclass
class FlipConfig:
    """Configuration for flip control."""
    # Minimum hold time before any signal-based exit
    min_hold_seconds: int = 600  # 10 minutes
    
    # Cooldown after closing due to opposite signal
    reset_cooldown_seconds: int = 900  # 15 minutes
    
    # Confirmations required for re-entry after reset
    confirm_ticks_required: int = 3
    
    # Minimum confidence for re-entry
    reentry_min_confidence: float = 0.80
    
    # Minimum confidence to trigger close (higher than entry)
    close_trigger_confidence: float = 0.60


@dataclass
class FlipDecision:
    """Result of flip evaluation."""
    action: FlipAction
    reason: str
    can_close: bool = False
    can_open: bool = False
    wait_seconds: int = 0
    confirm_progress: str = ""


class FlipController:
    """
    Controls position flips to prevent churn.
    
    Core rule: No immediate flips.
    Instead: Close → Reset → Confirm → Re-enter
    """
    
    def __init__(self, config: Optional[FlipConfig] = None):
        self.config = config or FlipConfig()
        self._states: Dict[str, FlipState] = {}
    
    def _get_state(self, symbol: str) -> FlipState:
        if symbol not in self._states:
            self._states[symbol] = FlipState(symbol=symbol)
        return self._states[symbol]
    
    # =========================================================================
    # POSITION LIFECYCLE HOOKS
    # =========================================================================
    
    def on_position_opened(self, symbol: str, side: str):
        """Called when a position is opened."""
        state = self._get_state(symbol)
        state.in_position = True
        state.position_side = side
        state.position_opened_at = datetime.utcnow()
        
        # Clear reset state
        state.in_reset = False
        state.reset_started_at = None
        state.reset_target_side = None
        state.reset_confirm_count = 0
        state.cooldown_until = None
    
    def on_position_closed(self, symbol: str, reason: str = "manual"):
        """Called when a position is closed."""
        state = self._get_state(symbol)
        
        if reason == "opposite_signal":
            # Enter reset state with cooldown
            state.in_reset = True
            state.reset_started_at = datetime.utcnow()
            state.reset_confirm_count = 0
            state.cooldown_until = datetime.utcnow() + timedelta(
                seconds=self.config.reset_cooldown_seconds
            )
        else:
            # Normal close - no reset state
            state.in_reset = False
            state.reset_started_at = None
            state.reset_target_side = None
            state.reset_confirm_count = 0
            state.cooldown_until = None
        
        state.in_position = False
        state.position_side = None
        state.position_opened_at = None
    
    # =========================================================================
    # SIGNAL EVALUATION
    # =========================================================================
    
    def evaluate_signal(
        self,
        symbol: str,
        signal: str,  # "BUY", "SELL", "HOLD"
        confidence: float,
    ) -> FlipDecision:
        """
        Evaluate a signal and determine allowed action.
        
        Returns FlipDecision with action and reasoning.
        """
        state = self._get_state(symbol)
        now = datetime.utcnow()
        
        # Track signal
        state.last_signal = signal
        
        # ------------------------------------------
        # Case 1: No position, not in reset
        # ------------------------------------------
        if not state.in_position and not state.in_reset:
            if signal in ["BUY", "SELL"]:
                return FlipDecision(
                    action=FlipAction.ALLOW_ENTRY,
                    reason="no_position_no_reset",
                    can_open=True,
                )
            return FlipDecision(
                action=FlipAction.HOLD,
                reason="signal_is_hold",
            )
        
        # ------------------------------------------
        # Case 2: No position, in reset (waiting)
        # ------------------------------------------
        if not state.in_position and state.in_reset:
            return self._evaluate_reset_reentry(state, signal, confidence, now)
        
        # ------------------------------------------
        # Case 3: In position
        # ------------------------------------------
        if state.in_position:
            return self._evaluate_in_position(state, signal, confidence, now)
        
        # Fallback
        return FlipDecision(action=FlipAction.HOLD, reason="fallback")
    
    def _evaluate_in_position(
        self,
        state: FlipState,
        signal: str,
        confidence: float,
        now: datetime,
    ) -> FlipDecision:
        """Evaluate signal when in a position."""
        
        # Same direction signal - just hold
        is_same_direction = (
            (state.position_side == "LONG" and signal == "BUY") or
            (state.position_side == "SHORT" and signal == "SELL")
        )
        
        if is_same_direction or signal == "HOLD":
            return FlipDecision(
                action=FlipAction.HOLD,
                reason="same_direction_or_hold",
            )
        
        # Opposite signal - check if we can close
        
        # Check minimum hold time
        if state.position_opened_at:
            elapsed = (now - state.position_opened_at).total_seconds()
            if elapsed < self.config.min_hold_seconds:
                remaining = self.config.min_hold_seconds - elapsed
                return FlipDecision(
                    action=FlipAction.HOLD,
                    reason=f"min_hold_not_met",
                    wait_seconds=int(remaining),
                )
        
        # Check confidence threshold for close
        if confidence < self.config.close_trigger_confidence:
            return FlipDecision(
                action=FlipAction.HOLD,
                reason=f"close_confidence_too_low:{confidence:.2f}",
            )
        
        # Track opposite signal (for confirmation before closing)
        if signal != state.last_signal:
            state.opposite_signal_count = 1
        else:
            state.opposite_signal_count += 1
        
        # Require at least 2 consecutive opposite signals before closing
        if state.opposite_signal_count < 2:
            return FlipDecision(
                action=FlipAction.HOLD,
                reason=f"confirming_close:{state.opposite_signal_count}/2",
            )
        
        # Set target side for reset
        state.reset_target_side = signal  # "BUY" -> want LONG, "SELL" -> want SHORT
        
        # Allow close and enter reset
        return FlipDecision(
            action=FlipAction.CLOSE_AND_RESET,
            reason="opposite_confirmed",
            can_close=True,
        )
    
    def _evaluate_reset_reentry(
        self,
        state: FlipState,
        signal: str,
        confidence: float,
        now: datetime,
    ) -> FlipDecision:
        """Evaluate if we can re-enter after reset."""
        
        # Check cooldown
        if state.cooldown_until and now < state.cooldown_until:
            remaining = (state.cooldown_until - now).seconds
            return FlipDecision(
                action=FlipAction.HOLD,
                reason="cooldown_active",
                wait_seconds=remaining,
            )
        
        # Check if signal matches expected target
        expected = state.reset_target_side
        if signal != expected:
            # Signal changed - reset confirmation counter
            state.reset_target_side = signal
            state.reset_confirm_count = 0
            return FlipDecision(
                action=FlipAction.HOLD,
                reason="signal_direction_changed",
            )
        
        # Check confidence
        if confidence < self.config.reentry_min_confidence:
            return FlipDecision(
                action=FlipAction.HOLD,
                reason=f"reentry_confidence_low:{confidence:.2f}",
            )
        
        # Increment confirmation
        state.reset_confirm_count += 1
        
        # Check if enough confirmations
        if state.reset_confirm_count < self.config.confirm_ticks_required:
            return FlipDecision(
                action=FlipAction.HOLD,
                reason="confirming_reentry",
                confirm_progress=f"{state.reset_confirm_count}/{self.config.confirm_ticks_required}",
            )
        
        # All checks passed - allow entry
        return FlipDecision(
            action=FlipAction.ALLOW_ENTRY,
            reason="reset_complete",
            can_open=True,
        )
    
    # =========================================================================
    # STATE QUERIES
    # =========================================================================
    
    def get_state(self, symbol: str) -> dict:
        """Get current flip state for a symbol."""
        state = self._get_state(symbol)
        now = datetime.utcnow()
        
        result = {
            "symbol": symbol,
            "in_position": state.in_position,
            "position_side": state.position_side,
            "in_reset": state.in_reset,
        }
        
        if state.in_position and state.position_opened_at:
            elapsed = (now - state.position_opened_at).total_seconds()
            result["hold_elapsed_seconds"] = int(elapsed)
            result["min_hold_remaining"] = max(0, self.config.min_hold_seconds - int(elapsed))
        
        if state.in_reset:
            result["reset_target"] = state.reset_target_side
            result["confirm_count"] = state.reset_confirm_count
            result["confirm_required"] = self.config.confirm_ticks_required
            
            if state.cooldown_until and now < state.cooldown_until:
                result["cooldown_remaining"] = (state.cooldown_until - now).seconds
        
        return result
    
    def clear_state(self, symbol: str):
        """Clear all state for a symbol."""
        if symbol in self._states:
            del self._states[symbol]
