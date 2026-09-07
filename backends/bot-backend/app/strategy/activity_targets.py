# app/strategy/activity_targets.py
"""
Activity Targets Module — DAILY ACTIVITY FALLBACK DISABLED (B-6)

B-6 Fix: Inactivity-based confidence softening is permanently disabled.
The correct behavior when the bot has not traded for 24 hours (or longer)
is to WAIT — not to lower standards.

This module is retained for its trade-counter and ATR-tracking utilities,
but the threshold-reduction logic is hard-blocked. No matter what `enabled`
is set to in an ActivityTargets instance, `get_current_reduction()` will
always return 0.0 (DAILY_ACTIVITY_FALLBACK_DISABLED).

Required behavior:
- MIN_CONFIDENCE_THRESHOLD remains static and must not be softened by inactivity.
- CONFIDENCE_THRESHOLD_STATIC: the minimum confidence is what it is.
- NO_TRADE_HIGH_QUALITY_SETUP_REQUIRED: prefer no trade over a forced trade.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Dict, Any

logger = logging.getLogger(__name__)


@dataclass
class ActivityCheckpoint:
    """Time checkpoint for threshold relaxation."""
    hour_utc: int  # Hour of day (0-23)
    threshold_reduction_pct: float  # How much to reduce min_confidence (0.0-0.20)


@dataclass
class ActivityTargets:
    """Configuration for activity targeting / minimum trades per day."""
    
    # Enable/disable (OFF by default - opt-in only)
    enabled: bool = False
    
    # Target range (not hard minimum - allows flexibility)
    min_signals_per_day: int = 1  # Soft target for daily trades
    max_signals_per_day: int = 20  # Existing cap
    
    # Time checkpoints for threshold relaxation
    # At each hour, if trades < min_signals, reduce confidence threshold
    checkpoints: List[ActivityCheckpoint] = field(default_factory=lambda: [
        ActivityCheckpoint(hour_utc=14, threshold_reduction_pct=0.05),  # 5% relaxation at 14:00
        ActivityCheckpoint(hour_utc=18, threshold_reduction_pct=0.10),  # 10% at 18:00
        ActivityCheckpoint(hour_utc=21, threshold_reduction_pct=0.15),  # 15% at 21:00
    ])
    
    # Guardrails (prevent garbage entries)
    min_confidence_floor: float = 0.20  # NEVER go below 20% confidence threshold
    require_trend_alignment: bool = True  # Must align with higher TF trend
    max_atr_multiplier: float = 1.5  # Skip if volatility > 1.5x normal
    skip_high_volatility: bool = True  # Don't nudge during vol spikes
    
    # Reset time (UTC hour when daily counter resets)
    reset_hour_utc: int = 0
    
    def get_current_reduction(
        self,
        trades_today: int,
        current_hour_utc: int,
    ) -> Tuple[float, str]:
        """
        B-6 Fix: DAILY_ACTIVITY_FALLBACK_DISABLED.

        This method previously reduced the confidence threshold when the bot
        had been inactive. That behavior is now permanently disabled.

        Lowering the confidence threshold because of inactivity forces trades
        where none should be taken. Low activity means the market is not
        offering good setups — the correct response is to wait.

        Returns: (0.0, "DAILY_ACTIVITY_FALLBACK_DISABLED") always.
        """
        logger.debug(
            "DAILY_ACTIVITY_FALLBACK_DISABLED — confidence threshold remains static. "
            "CONFIDENCE_THRESHOLD_STATIC: NO_TRADE_HIGH_QUALITY_SETUP_REQUIRED."
        )
        return 0.0, "DAILY_ACTIVITY_FALLBACK_DISABLED"


@dataclass
class NudgeResult:
    """Result of activity nudge calculation."""
    adjusted_min_confidence: float
    original_min_confidence: float
    reduction_applied: float
    reason: str
    should_apply: bool
    guardrail_triggered: Optional[str] = None


class ActivityTargetsMixin:
    """
    Mixin for strategies that want activity targeting.
    
    Provides methods to:
    1. Track daily trade count
    2. Get adjusted confidence threshold based on time/activity
    3. Apply guardrails before nudging
    """
    
    # These should be initialized by the strategy
    activity_targets: ActivityTargets = None
    _trades_today: int = 0
    _last_reset_date: Optional[str] = None
    _avg_atr: float = 0.0  # Rolling average ATR for volatility comparison
    
    def reset_activity_counter_if_needed(self) -> None:
        """Reset daily trade counter at reset hour."""
        now = datetime.now(timezone.utc)
        today_str = now.strftime("%Y-%m-%d")
        
        if self._last_reset_date != today_str:
            self._trades_today = 0
            self._last_reset_date = today_str
            logger.debug(f"Activity counter reset for {today_str}")
    
    def record_trade(self) -> None:
        """Record a trade for activity tracking."""
        self.reset_activity_counter_if_needed()
        self._trades_today += 1
    
    def get_trades_today(self) -> int:
        """Get number of trades today."""
        self.reset_activity_counter_if_needed()
        return self._trades_today
    
    def calculate_nudge(
        self,
        base_min_confidence: float,
        current_atr: float = 0.0,
        trend_aligned: bool = True,
    ) -> NudgeResult:
        """
        Calculate adjusted confidence threshold based on activity targets.
        
        Args:
            base_min_confidence: The strategy's normal minimum confidence
            current_atr: Current ATR for volatility check
            trend_aligned: Whether signal aligns with higher TF trend
        
        Returns:
            NudgeResult with adjusted threshold and details
        """
        if not self.activity_targets or not self.activity_targets.enabled:
            return NudgeResult(
                adjusted_min_confidence=base_min_confidence,
                original_min_confidence=base_min_confidence,
                reduction_applied=0.0,
                reason="activity_targets_disabled",
                should_apply=False,
            )
        
        targets = self.activity_targets
        now = datetime.now(timezone.utc)
        trades_today = self.get_trades_today()
        
        # Get reduction from checkpoints
        reduction, reason = targets.get_current_reduction(
            trades_today=trades_today,
            current_hour_utc=now.hour,
        )
        
        if reduction <= 0:
            return NudgeResult(
                adjusted_min_confidence=base_min_confidence,
                original_min_confidence=base_min_confidence,
                reduction_applied=0.0,
                reason=reason,
                should_apply=False,
            )
        
        # ====================
        # GUARDRAILS
        # ====================
        
        # Guardrail 1: Trend alignment
        if targets.require_trend_alignment and not trend_aligned:
            return NudgeResult(
                adjusted_min_confidence=base_min_confidence,
                original_min_confidence=base_min_confidence,
                reduction_applied=0.0,
                reason="guardrail_trend_misaligned",
                should_apply=False,
                guardrail_triggered="trend_alignment",
            )
        
        # Guardrail 2: High volatility
        if targets.skip_high_volatility and current_atr > 0 and self._avg_atr > 0:
            atr_ratio = current_atr / self._avg_atr
            if atr_ratio > targets.max_atr_multiplier:
                return NudgeResult(
                    adjusted_min_confidence=base_min_confidence,
                    original_min_confidence=base_min_confidence,
                    reduction_applied=0.0,
                    reason=f"guardrail_high_volatility_{atr_ratio:.2f}x",
                    should_apply=False,
                    guardrail_triggered="high_volatility",
                )
        
        # ====================
        # APPLY REDUCTION
        # ====================
        
        adjusted = base_min_confidence - reduction
        
        # Enforce floor
        if adjusted < targets.min_confidence_floor:
            adjusted = targets.min_confidence_floor
            reason = f"{reason}_floored"
        
        return NudgeResult(
            adjusted_min_confidence=adjusted,
            original_min_confidence=base_min_confidence,
            reduction_applied=reduction,
            reason=reason,
            should_apply=True,
        )
    
    def apply_activity_nudge_to_confidence(
        self,
        raw_confidence: float,
        raw_signal: str,
        base_min_confidence: float,
        current_atr: float = 0.0,
        trend_aligned: bool = True,
    ) -> Tuple[bool, str, float]:
        """
        Determine if signal should pass based on activity-adjusted threshold.
        
        Args:
            raw_confidence: The signal's actual confidence
            raw_signal: The signal type ("BUY", "SELL", "HOLD")
            base_min_confidence: Normal minimum confidence threshold
            current_atr: Current ATR for volatility check
            trend_aligned: Whether signal aligns with higher TF trend
        
        Returns:
            (passes_threshold, reason, adjusted_min_confidence)
        """
        if raw_signal == "HOLD":
            return False, "signal_is_hold", base_min_confidence
        
        nudge = self.calculate_nudge(
            base_min_confidence=base_min_confidence,
            current_atr=current_atr,
            trend_aligned=trend_aligned,
        )
        
        passes = raw_confidence >= nudge.adjusted_min_confidence
        
        if passes and nudge.should_apply:
            reason = f"passed_with_{nudge.reason}"
        elif passes:
            reason = "passed_normal_threshold"
        else:
            reason = f"below_threshold_{nudge.adjusted_min_confidence:.2f}"
        
        return passes, reason, nudge.adjusted_min_confidence
    
    def update_avg_atr(self, current_atr: float, alpha: float = 0.1) -> None:
        """Update rolling average ATR for volatility comparison."""
        if self._avg_atr <= 0:
            self._avg_atr = current_atr
        else:
            # Exponential moving average
            self._avg_atr = (1 - alpha) * self._avg_atr + alpha * current_atr


# =============================================================================
# FACTORY / DEFAULTS
# =============================================================================

def default_activity_targets() -> ActivityTargets:
    """Get default activity targets configuration."""
    return ActivityTargets(
        enabled=False,  # Opt-in only
        min_signals_per_day=1,
        checkpoints=[
            ActivityCheckpoint(hour_utc=14, threshold_reduction_pct=0.05),
            ActivityCheckpoint(hour_utc=18, threshold_reduction_pct=0.10),
            ActivityCheckpoint(hour_utc=21, threshold_reduction_pct=0.15),
        ],
        min_confidence_floor=0.20,
        require_trend_alignment=True,
        max_atr_multiplier=1.5,
    )


def conservative_activity_targets() -> ActivityTargets:
    """Very conservative activity targets (minimal nudging)."""
    return ActivityTargets(
        enabled=True,
        min_signals_per_day=1,
        checkpoints=[
            ActivityCheckpoint(hour_utc=20, threshold_reduction_pct=0.05),  # Only at 20:00, 5% only
        ],
        min_confidence_floor=0.30,  # Higher floor
        require_trend_alignment=True,
        max_atr_multiplier=1.2,  # Stricter volatility check
    )


def moderate_activity_targets() -> ActivityTargets:
    """Moderate activity targets."""
    return ActivityTargets(
        enabled=True,
        min_signals_per_day=2,
        checkpoints=[
            ActivityCheckpoint(hour_utc=12, threshold_reduction_pct=0.03),
            ActivityCheckpoint(hour_utc=16, threshold_reduction_pct=0.07),
            ActivityCheckpoint(hour_utc=20, threshold_reduction_pct=0.12),
        ],
        min_confidence_floor=0.20,
        require_trend_alignment=True,
        max_atr_multiplier=1.5,
    )
