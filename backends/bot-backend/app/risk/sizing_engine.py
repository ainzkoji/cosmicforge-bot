"""
Universal Sizing Engine
=======================

Phase 3 Consolidation: ONE universal sizing function.
Replaces legacy percentage-based fixed notional sizing and static 5% equity caps.

Inputs:
- Equity
- Risk per trade (%)
- Stop distance (%)
- Volatility (ATR)
- Mode (fallback / normal)
- User capital allocation (%)
- Hard risk ceilings

Output:
- Final quantity
- Final notional
- Required leverage
- Effective risk %
- Margin utilization %
"""
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Tuple

logger = logging.getLogger(__name__)

@dataclass
class SizingInputs:
    """All necessary inputs for the Universal Sizing Engine."""
    symbol: str
    equity: float
    entry_price: float
    risk_per_trade_pct: float
    stop_distance_pct: float
    
    volatility_atr: Optional[float] = None
    is_fallback_mode: bool = False
    
    # User / System Configuration
    capital_allocation_pct: float = 1.0  # Usually 1.0, but user might restrict bot to 50% of account
    max_risk_ceiling_pct: float = 0.05   # e.g. Absolute ceiling of 5% risk per trade
    min_notional_usdt: float = 5.0       # Exchange minimum
    max_notional_usdt: float = 100000.0  # Exchange/System maximum
    fallback_size_multiplier: float = 0.25 # Only apply to fallback trades
    
    # Advanced logic inputs
    custom_trade_amount_mode: str = "atr_risk" # "fixed", "percent", "atr_risk"
    custom_trade_amount_value: float = 0.0     # USDT if fixed, % if percent


@dataclass
class SizingResult:
    """Consolidated output from the Sizing Engine."""
    allowed: bool
    reason: str
    
    quantity: float
    notional: float
    required_leverage: float
    effective_risk_pct: float
    margin_utilization_pct: float
    
    # Detailed tracking for logs/audit
    base_notional: float = 0.0
    adjusted_notional: float = 0.0
    risk_usdt: float = 0.0
    
    details: Dict[str, Any] = field(default_factory=dict)


class UniversalSizingEngine:
    """
    Core sizing mathematical logic.
    Computes precise position sizing based on risk/stop distance,
    or overrides based on specific user configurations.
    """
    
    def calculate(self, inputs: SizingInputs) -> SizingResult:
        """
        Calculates the definitive position size (quantity and notional)
        for a given trade.
        """
        # 1. Effective Equity Check
        effective_equity = inputs.equity * inputs.capital_allocation_pct
        if effective_equity <= 0:
             return SizingResult(False, "Zero or negative effective equity", 0.0, 0.0, 1.0, 0.0, 0.0)
             
        if inputs.entry_price <= 0:
             return SizingResult(False, "Invalid entry price", 0.0, 0.0, 1.0, 0.0, 0.0)

        # 2. Risk Amount Definition
        # Default strategy focuses on Risking a specific % on the stop-loss distance
        target_risk_usdt = effective_equity * inputs.risk_per_trade_pct
        
        # Hard cap the *intended* risk amount
        max_risk_allowed_usdt = effective_equity * inputs.max_risk_ceiling_pct
        if target_risk_usdt > max_risk_allowed_usdt:
             target_risk_usdt = max_risk_allowed_usdt
             logger.debug(f"[{inputs.symbol}] Risk capped by ceiling: {max_risk_allowed_usdt:.2f} USDT")

        # 3. Base Notional Calculation (Core Math)
        # Risk = Notional * Stop Distance  =>  Notional = Risk / Stop Distance
        # Prevent division by zero if stop distance is tiny/missing
        safe_stop_distance_pct = max(inputs.stop_distance_pct, 0.001) # Minimum 0.1% stop
        
        base_notional = target_risk_usdt / safe_stop_distance_pct
        sizing_method = "risk_based"

        # 4. Custom Trade Amount Override (e.g. forced by Policy Engine/Strategy)
        if inputs.custom_trade_amount_mode == "fixed" and inputs.custom_trade_amount_value > 0:
            base_notional = inputs.custom_trade_amount_value
            sizing_method = "fixed_usdt"
        elif inputs.custom_trade_amount_mode == "percent" and inputs.custom_trade_amount_value > 0:
            pct_to_use = min(inputs.custom_trade_amount_value, 100.0) / 100.0
            base_notional = effective_equity * pct_to_use
            sizing_method = "percent_equity"

        # 5. Fallback Mode Adjustment
        # Replaces duplicated fallback multiplier math across runner/orchestrator/safety
        if inputs.is_fallback_mode:
            original_notional = base_notional
            base_notional *= inputs.fallback_size_multiplier
            logger.info(f"[{inputs.symbol}] Fallback multi applied: {original_notional:.2f} -> {base_notional:.2f}")
            sizing_method += "_fallback"

        # 6. Apply Exchange / System Limits
        adjusted_notional = base_notional
        
        if adjusted_notional < inputs.min_notional_usdt:
            # We bump up to min notional IF we have enough equity, otherwise block
            if effective_equity >= inputs.min_notional_usdt:
                 adjusted_notional = inputs.min_notional_usdt
                 sizing_method += "_bumped_to_min"
            else:
                 return SizingResult(False, f"Insufficient equity for min notional ${inputs.min_notional_usdt}", 0.0, 0.0, 1.0, 0.0, 0.0)
                 
        if adjusted_notional > inputs.max_notional_usdt:
            adjusted_notional = inputs.max_notional_usdt
            sizing_method += "_capped_at_max"

        # Absolute hard cap: no single trade can ever be larger than 100% of effective equity * max leverage allowed
        # (Though Leverage validation happens explicitly in Safety/Policy, we cap raw notional here just in case)
        # Note: Actually, we shouldn't cap it here based on leverage, that's Layer B/C's job.
        # But we DO cap it to prevent massive overflows.
        
        # 7. Final Outputs
        final_quantity = adjusted_notional / inputs.entry_price
        
        # Recalculate actual risk and margin given the *final* adjusted notional
        actual_risk_usdt = adjusted_notional * safe_stop_distance_pct
        effective_risk_pct = actual_risk_usdt / effective_equity
        
        # Assumes 1x leverage for margin util output (Safety/LeverageEngine will dictate actual margin)
        # If the user wants $100 notional on $100 account, it's 100% util at 1x leverage.
        margin_utilization_pct = adjusted_notional / effective_equity
        
        details = {
            "sizing_method": sizing_method,
            "effective_equity": effective_equity,
            "target_risk_usdt": target_risk_usdt,
            "safe_stop_distance_pct": safe_stop_distance_pct,
            "is_fallback_mode": inputs.is_fallback_mode
        }
        
        return SizingResult(
            allowed=True,
            reason="sizing_ok",
            quantity=final_quantity,
            notional=adjusted_notional,
            required_leverage=1.0, # Placeholder, caller / SafetyEngine adjusts
            effective_risk_pct=effective_risk_pct,
            margin_utilization_pct=margin_utilization_pct,
            base_notional=base_notional,
            adjusted_notional=adjusted_notional,
            risk_usdt=actual_risk_usdt,
            details=details
        )


# Singleton Pattern
_sizing_engine_instance: Optional[UniversalSizingEngine] = None

def get_sizing_engine() -> UniversalSizingEngine:
    global _sizing_engine_instance
    if _sizing_engine_instance is None:
        _sizing_engine_instance = UniversalSizingEngine()
    return _sizing_engine_instance
