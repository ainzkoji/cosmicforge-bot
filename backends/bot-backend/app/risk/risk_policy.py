"""
DEPRECATED: This module is deprecated.

Use app.policy.policy_engine instead:
    from app.policy.policy_engine import RiskLevel, RiskProfile, RISK_PROFILES, PolicyEngine

The risk profile and enforcement logic has been consolidated into PolicyEngine.
This module will be removed in a future version.

---
Original docstring:

Centralized Risk Policy Module.

This module is the single source of truth for:
1. Risk Profiles (Conservative, Balanced, Aggressive)
2. Dynamic Policy Resolution (based on User Config)
3. Risk Enforcement Logic (Clamping/Blocking)

It ensures that all trading activities (Manual, Auto-Pilot, Strategies) 
adhere to the same dynamic risk limits.
"""
from __future__ import annotations
import warnings
warnings.warn(
    "app.risk.risk_policy is deprecated. Use app.policy.policy_engine instead.",
    DeprecationWarning,
    stacklevel=2
)
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, Any
from enum import Enum
import logging

from app.risk.system_limits import RiskLevel

logger = logging.getLogger(__name__)

# Fixed System Constants
FIXED_LEVERAGE = 10.0
# Absolute Safety Cap (System Wide Hard Limit) - Safety Net
ABSOLUTE_MAX_COMPOUND_RISK = 0.30 

@dataclass
class RiskProfileConfig:
    """Configuration for a specific Risk Profile."""
    max_compound_risk_pct: float
    label: str
    color: str
    description: str

    @property
    def max_stop_loss_pct(self) -> float:
        """Derived max stop loss at fixed leverage."""
        return self.max_compound_risk_pct / FIXED_LEVERAGE

# Central Definition of Policies
RISK_PROFILES: Dict[RiskLevel, RiskProfileConfig] = {
    RiskLevel.LOW: RiskProfileConfig(
        max_compound_risk_pct=0.15,
        label="Conservative",
        color="green",
        description="Low drawdowns, smaller gains. Max risk per trade: 15%."
    ),
    RiskLevel.MEDIUM: RiskProfileConfig(
        max_compound_risk_pct=0.225,
        label="Balanced", 
        color="yellow",
        description="Moderate risk. Max risk per trade: 22.5%."
    ),
    RiskLevel.HIGH: RiskProfileConfig(
        max_compound_risk_pct=0.30,
        label="Aggressive",
        color="red",
        description="High volatility. Max risk per trade: 30%."
    ),
}

@dataclass
class EnforcementResult:
    """Result of a risk enforcement check."""
    allowed: bool
    adjusted_stop_loss_pct: float
    is_clamped: bool
    reason: str
    details: Dict[str, Any]

class RiskPolicy:
    """
    Represents the active risk policy for a specific user/bot execution context.
    
    Acts as a facade to resolve and enforce risk limits.
    """
    
    def __init__(self, profile_config: RiskProfileConfig):
        self.config = profile_config

    @classmethod
    def resolve(cls, risk_level: RiskLevel) -> 'RiskPolicy':
        """
        Resolve the policy based on the user's selected RiskLevel.
        """
        config = RISK_PROFILES.get(risk_level, RISK_PROFILES[RiskLevel.LOW])
        return cls(config)

    def enforce(
        self, 
        requested_stop_loss_pct: float, 
        is_autopilot: bool = True
    ) -> EnforcementResult:
        """
        Enforce the Maximum Stop Loss policy.
        
        Args:
            requested_stop_loss_pct: The stop loss requested by strategy/user (e.g. 0.05 for 5%)
            is_autopilot: If True, prefer CLAMPING. If False (Manual), prefer WARNING (but we clamp for safety anyway).
            
        Returns:
            EnforcementResult with final allowed values and modification details.
        """
        final_sl = requested_stop_loss_pct
        max_sl = self.config.max_stop_loss_pct
        
        details = {
            "profile": self.config.label,
            "max_compound_risk": self.config.max_compound_risk_pct,
            "fixed_leverage": FIXED_LEVERAGE,
            "limit_sl": max_sl,
            "requested_sl": requested_stop_loss_pct
        }

        # 1. Check if Clamp Needed
        if requested_stop_loss_pct > max_sl:
            # CLAMP logic
            final_sl = max_sl
            compound_risk = final_sl * FIXED_LEVERAGE
            
            msg = (
                f"RISK_ADJUSTMENT: Stop loss clamped from {requested_stop_loss_pct:.2%} "
                f"to {final_sl:.2%} to respect {self.config.label} limit "
                f"(Max Risk {self.config.max_compound_risk_pct:.1%})"
            )
            logger.debug(msg)
            
            return EnforcementResult(
                allowed=True,
                adjusted_stop_loss_pct=final_sl,
                is_clamped=True,
                reason=msg,
                details={**details, "final_compound_risk": compound_risk}
            )

        # 2. Check Absolute System Hard Cap (Redundant Code, Safety Net)
        # Just in case the profile config itself is somehow corrupted or overridden dangerously
        compound_risk = final_sl * FIXED_LEVERAGE
        if compound_risk > ABSOLUTE_MAX_COMPOUND_RISK + 0.0001: # Epsilon
             return EnforcementResult(
                allowed=False,
                adjusted_stop_loss_pct=final_sl,
                is_clamped=False,
                reason=f"CRITICAL: System Hard Cap Violated. {compound_risk:.1%} > {ABSOLUTE_MAX_COMPOUND_RISK:.1%}",
                details=details
            )

        return EnforcementResult(
            allowed=True,
            adjusted_stop_loss_pct=final_sl,
            is_clamped=False,
            reason="Within limits",
            details={**details, "final_compound_risk": compound_risk}
        )

