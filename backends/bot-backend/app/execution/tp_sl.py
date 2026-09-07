"""
4-Layer Stop-Loss System - Production Grade

Layer 1: Entry Stop (ATR + Structure hybrid)
Layer 2: Validation Stop (reject bad trades)
Layer 3: Dynamic Stop (break-even, trailing)
Layer 4: Time-based Stop

This is the anchor of the trading system.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List, Tuple
from enum import Enum


class StopType(Enum):
    ATR = "ATR"
    STRUCTURE = "STRUCTURE"
    HYBRID = "HYBRID"
    ATR_CAPPED = "ATR_CAPPED"
    INVALID = "INVALID"


class RejectReason(Enum):
    NONE = "NONE"
    ATR_ZERO = "ATR_ZERO"
    STOP_TOO_TIGHT = "STOP_TOO_TIGHT"  # < 0.5 ATR (noise)
    STOP_TOO_WIDE = "STOP_TOO_WIDE"    # > 2.0 ATR (poor R:R)
    INSUFFICIENT_EDGE = "INSUFFICIENT_EDGE"  # TP1 < 2x fees
    CHOP_DETECTED = "CHOP_DETECTED"    # ATR% too low
    R_TOO_LOW = "R_TOO_LOW"            # Can't achieve min R


@dataclass
class TPSLResult:
    """Result of TP/SL calculation with full validation."""
    # Validity
    valid: bool
    reject_reason: RejectReason
    reject_detail: str
    
    # Stop loss
    stop_price: float
    stop_distance: float
    stop_type: StopType
    
    # Take profits
    tp1_price: float
    tp1_distance: float
    tp2_price: float
    tp2_distance: float
    
    # R metrics
    r_value: float
    risk_reward_tp1: float
    risk_reward_tp2: float
    
    # Fee analysis
    estimated_fees: float
    fee_adjusted_tp1: float  # TP1 minus fees
    min_edge_met: bool
    
    # Structure info
    swing_price: Optional[float] = None
    atr_stop_candidate: float = 0.0
    structure_stop_candidate: float = 0.0


@dataclass
class StopConfig:
    """Mode-aware stop-loss configuration."""
    # ATR multipliers
    k_sl: float = 1.2  # SL = k_sl × ATR
    k_sl_mean_reversion: float = 0.8
    
    # Validity bounds (in ATR units)
    min_sl_atr_mult: float = 0.5  # Noise floor
    max_sl_atr_mult: float = 2.0  # Width ceiling
    max_sl_mean_reversion: float = 1.2  # Tighter for mean reversion
    
    # TP in R multiples
    tp1_r: float = 1.0
    tp2_r: float = 2.2
    tp1_close_fraction: float = 0.5
    
    # Fee gating
    taker_fee_rate: float = 0.0005  # 0.05%
    min_edge_multiplier: float = 2.0  # TP1 must be ≥ 2× fees
    
    # Structure
    swing_lookback: int = 20  # Candles to look back for swing
    structure_buffer_atr: float = 0.1  # Buffer below swing in ATR
    
    # Chop detection
    min_atr_percent: float = 0.3  # Reject if ATR% below this


# =============================================================================
# INDICATOR CALCULATIONS
# =============================================================================

def calculate_atr(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int = 14
) -> float:
    """Calculate Average True Range."""
    if len(closes) < period + 1:
        return 0.0
    
    trs = []
    for i in range(1, len(highs)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        )
        trs.append(tr)
    
    if len(trs) < period:
        return sum(trs) / len(trs) if trs else 0.0
    
    return sum(trs[-period:]) / period


def find_swing_low(lows: List[float], lookback: int = 20) -> Tuple[float, int]:
    """Find lowest low and its index in lookback period."""
    if len(lows) < lookback:
        return min(lows) if lows else 0.0, len(lows) - 1
    
    window = lows[-lookback:]
    min_val = min(window)
    min_idx = len(lows) - lookback + window.index(min_val)
    return min_val, min_idx


def find_swing_high(highs: List[float], lookback: int = 20) -> Tuple[float, int]:
    """Find highest high and its index in lookback period."""
    if len(highs) < lookback:
        return max(highs) if highs else 0.0, len(highs) - 1
    
    window = highs[-lookback:]
    max_val = max(window)
    max_idx = len(highs) - lookback + window.index(max_val)
    return max_val, max_idx


def calculate_atr_percent(closes: List[float], atr: float) -> float:
    """Calculate ATR as percentage of price."""
    if not closes or closes[-1] <= 0:
        return 0.0
    return (atr / closes[-1]) * 100


# =============================================================================
# 4-LAYER STOP-LOSS CALCULATOR
# =============================================================================

class StopLossCalculator:
    """
    Production-grade stop-loss calculator.
    
    4 Layers:
    1. Entry Stop - ATR + Structure hybrid
    2. Validation - Reject bad setups
    3. Dynamic - Break-even, trailing (handled by PositionManager)
    4. Time-based - Exit stagnant trades (handled by PositionManager)
    """
    
    def __init__(self, config: Optional[StopConfig] = None):
        self.config = config or StopConfig()
    
    def get_precision_config(self) -> StopConfig:
        """Config for Precision mode (trend following)."""
        return StopConfig(
            k_sl=1.2,
            k_sl_mean_reversion=0.8,
            min_sl_atr_mult=0.5,
            max_sl_atr_mult=2.0,
            tp1_r=1.0,
            tp2_r=2.2,
        )
    
    def get_flow_config(self) -> StopConfig:
        """Config for Flow mode (mean reversion)."""
        return StopConfig(
            k_sl=0.8,
            k_sl_mean_reversion=0.6,
            min_sl_atr_mult=0.4,
            max_sl_atr_mult=1.2,
            max_sl_mean_reversion=1.0,
            tp1_r=0.8,
            tp2_r=1.8,
        )
    
    def calculate(
        self,
        side: str,  # "LONG" or "SHORT"
        entry_price: float,
        highs: List[float],
        lows: List[float],
        closes: List[float],
        is_mean_reversion: bool = False,
    ) -> TPSLResult:
        """
        Calculate TP/SL with full 4-layer validation.
        """
        # Calculate ATR
        atr = calculate_atr(highs, lows, closes, 14)
        
        # Layer 1A: Reject if ATR is zero (can't calculate stops)
        if atr <= 0:
            return self._invalid_result(RejectReason.ATR_ZERO, "ATR is zero or negative")
        
        # Layer 1B: Chop detection
        atr_pct = calculate_atr_percent(closes, atr)
        if atr_pct < self.config.min_atr_percent:
            return self._invalid_result(
                RejectReason.CHOP_DETECTED, 
                f"ATR%={atr_pct:.2f} < {self.config.min_atr_percent}"
            )
        
        # Get mode-specific k values
        k_sl = self.config.k_sl_mean_reversion if is_mean_reversion else self.config.k_sl
        max_sl = self.config.max_sl_mean_reversion if is_mean_reversion else self.config.max_sl_atr_mult
        
        # =====================================================================
        # LAYER 1: ENTRY STOP (ATR + Structure)
        # =====================================================================
        
        # Candidate A: ATR-based stop
        atr_stop_distance = k_sl * atr
        
        # Candidate B: Structure-based stop
        structure_stop_distance = None
        swing_price = None
        
        if side == "LONG" and lows:
            swing_low, _ = find_swing_low(lows, self.config.swing_lookback)
            # Add buffer below swing
            swing_price = swing_low - (self.config.structure_buffer_atr * atr)
            structure_stop_distance = entry_price - swing_price
            
        elif side == "SHORT" and highs:
            swing_high, _ = find_swing_high(highs, self.config.swing_lookback)
            # Add buffer above swing
            swing_price = swing_high + (self.config.structure_buffer_atr * atr)
            structure_stop_distance = swing_price - entry_price
        
        # Choose stop: tighter of ATR/structure if both valid
        stop_distance = atr_stop_distance
        stop_type = StopType.ATR
        
        if structure_stop_distance and structure_stop_distance > 0:
            min_valid = self.config.min_sl_atr_mult * atr
            
            if structure_stop_distance >= min_valid:
                if structure_stop_distance < atr_stop_distance:
                    stop_distance = structure_stop_distance
                    stop_type = StopType.STRUCTURE
                else:
                    stop_type = StopType.HYBRID
        
        # =====================================================================
        # LAYER 2: VALIDATION STOP
        # =====================================================================
        
        # 2A: Too tight (noise)
        min_stop = self.config.min_sl_atr_mult * atr
        if stop_distance < min_stop:
            return self._invalid_result(
                RejectReason.STOP_TOO_TIGHT,
                f"Stop {stop_distance:.2f} < min {min_stop:.2f} (noise protection)"
            )
        
        # 2B: Too wide (poor R:R)
        max_stop = max_sl * atr
        if stop_distance > max_stop:
            # Cap it, but flag as potentially poor
            stop_distance = max_stop
            stop_type = StopType.ATR_CAPPED
        
        # Calculate stop price
        if side == "LONG":
            stop_price = entry_price - stop_distance
        else:
            stop_price = entry_price + stop_distance
        
        # R value = stop distance
        r_value = stop_distance
        
        # Calculate TP levels
        tp1_distance = self.config.tp1_r * r_value
        tp2_distance = self.config.tp2_r * r_value
        
        if side == "LONG":
            tp1_price = entry_price + tp1_distance
            tp2_price = entry_price + tp2_distance
        else:
            tp1_price = entry_price - tp1_distance
            tp2_price = entry_price - tp2_distance
        
        # 2C: Fee validation
        estimated_fees = entry_price * self.config.taker_fee_rate * 2  # Entry + exit
        fee_adjusted_tp1 = tp1_distance - estimated_fees
        min_edge = estimated_fees * self.config.min_edge_multiplier
        min_edge_met = tp1_distance >= min_edge
        
        if not min_edge_met:
            return self._invalid_result(
                RejectReason.INSUFFICIENT_EDGE,
                f"TP1={tp1_distance:.2f} < min_edge={min_edge:.2f}"
            )
        
        # All validations passed
        return TPSLResult(
            valid=True,
            reject_reason=RejectReason.NONE,
            reject_detail="",
            stop_price=stop_price,
            stop_distance=stop_distance,
            stop_type=stop_type,
            tp1_price=tp1_price,
            tp1_distance=tp1_distance,
            tp2_price=tp2_price,
            tp2_distance=tp2_distance,
            r_value=r_value,
            risk_reward_tp1=self.config.tp1_r,
            risk_reward_tp2=self.config.tp2_r,
            estimated_fees=estimated_fees,
            fee_adjusted_tp1=fee_adjusted_tp1,
            min_edge_met=True,
            swing_price=swing_price,
            atr_stop_candidate=atr_stop_distance,
            structure_stop_candidate=structure_stop_distance or 0.0,
        )
    
    def _invalid_result(self, reason: RejectReason, detail: str) -> TPSLResult:
        """Create an invalid result."""
        return TPSLResult(
            valid=False,
            reject_reason=reason,
            reject_detail=detail,
            stop_price=0,
            stop_distance=0,
            stop_type=StopType.INVALID,
            tp1_price=0,
            tp1_distance=0,
            tp2_price=0,
            tp2_distance=0,
            r_value=0,
            risk_reward_tp1=0,
            risk_reward_tp2=0,
            estimated_fees=0,
            fee_adjusted_tp1=0,
            min_edge_met=False,
        )
    
    # =========================================================================
    # LAYER 3: DYNAMIC STOP METHODS (called by PositionManager)
    # =========================================================================
    
    def calculate_break_even_stop(
        self,
        entry_price: float,
        side: str,
        fee_buffer_mult: float = 1.2,
    ) -> float:
        """
        Calculate break-even stop after TP1 hit.
        Covers entry + exit fees with buffer.
        """
        total_fees = entry_price * self.config.taker_fee_rate * 2
        buffer = total_fees * fee_buffer_mult
        
        if side == "LONG":
            return entry_price + buffer
        return entry_price - buffer
    
    def calculate_trailing_stop(
        self,
        side: str,
        current_stop: float,
        extreme_price: float,  # Highest for long, lowest for short
        atr: float,
        k_trail: float = 1.2,
    ) -> float:
        """
        Calculate trailing stop level.
        Only moves in profit direction, never loosens.
        """
        trail_distance = k_trail * atr
        
        if side == "LONG":
            new_stop = extreme_price - trail_distance
            return max(current_stop, new_stop)  # Never loosen
        else:
            new_stop = extreme_price + trail_distance
            return min(current_stop, new_stop)  # Never loosen


# =============================================================================
# LEGACY COMPATIBILITY
# =============================================================================

# Keep old names for backward compatibility
TPSLCalculator = StopLossCalculator
TPSLConfig = StopConfig
