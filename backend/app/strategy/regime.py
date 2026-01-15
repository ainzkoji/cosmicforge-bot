"""
Market Regime Classifier - Full Spec (A1-A4)

Classifies market conditions using:
- ADX(14) — trend strength
- ATR% — volatility as percentage
- MA slope — trend direction strength
- Range compression ratio — chop detector
- Breakout pressure — distance to Donchian bands

Outputs: regime_state, trend_dir, regime_confidence
"""
from __future__ import annotations
from enum import Enum
from dataclasses import dataclass
from typing import List, Optional, Tuple
import math


class MarketRegime(Enum):
    STRONG_TREND = "STRONG_TREND"           # ADX >= 25, clear MA slope
    WEAK_TREND = "WEAK_TREND"               # ADX 18-25
    RANGE = "RANGE"                         # ADX < 18, not choppy
    HIGH_VOLATILITY = "HIGH_VOLATILITY"     # ATR% >= high threshold
    LOW_VOLATILITY_CHOP = "LOW_VOLATILITY_CHOP"  # Low quality, avoid trading


class TrendDirection(Enum):
    UP = "UP"
    DOWN = "DOWN"
    NONE = "NONE"


@dataclass
class RegimeResult:
    regime: MarketRegime
    trend_dir: TrendDirection
    regime_confidence: float  # 0-1, how far from thresholds
    adx: float
    atr_percent: float
    ma_slope: float
    compression_ratio: float
    breakout_pressure: float
    details: dict


# =============================================================================
# INDICATOR CALCULATIONS
# =============================================================================

def calculate_ema(closes: List[float], period: int) -> List[float]:
    """Calculate Exponential Moving Average."""
    if len(closes) < period:
        return []
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    emas = [ema]
    for close in closes[period:]:
        ema = (close - ema) * multiplier + ema
        emas.append(ema)
    return emas


def calculate_sma(data: List[float], period: int) -> List[float]:
    """Calculate Simple Moving Average."""
    if len(data) < period:
        return []
    smas = []
    for i in range(period - 1, len(data)):
        smas.append(sum(data[i - period + 1:i + 1]) / period)
    return smas


def calculate_adx(
    highs: List[float], 
    lows: List[float], 
    closes: List[float], 
    period: int = 14
) -> Tuple[float, float, float]:
    """
    Calculate ADX, +DI, -DI using Wilder's smoothing.
    Returns: (adx, plus_di, minus_di)
    """
    if len(highs) < period + 1:
        return 0.0, 0.0, 0.0
    
    tr_list = []
    plus_dm_list = []
    minus_dm_list = []
    
    for i in range(1, len(highs)):
        high = highs[i]
        low = lows[i]
        prev_high = highs[i - 1]
        prev_low = lows[i - 1]
        prev_close = closes[i - 1]
        
        # True Range
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_list.append(tr)
        
        # Directional Movement
        up_move = high - prev_high
        down_move = prev_low - low
        
        plus_dm = up_move if (up_move > down_move and up_move > 0) else 0
        minus_dm = down_move if (down_move > up_move and down_move > 0) else 0
        
        plus_dm_list.append(plus_dm)
        minus_dm_list.append(minus_dm)
    
    if len(tr_list) < period:
        return 0.0, 0.0, 0.0
    
    # Wilder's smoothing: EMA with alpha = 1/period
    # Formula: new = prev * (1 - 1/period) + current * (1/period)
    # Or equivalently: new = (prev * (period - 1) + current) / period
    def wilder_smooth(data: List[float], period: int) -> List[float]:
        alpha = 1.0 / period
        first_val = sum(data[:period]) / period  # SMA for first value
        smoothed = [first_val]
        for i in range(period, len(data)):
            new_val = smoothed[-1] * (1 - alpha) + data[i] * alpha
            smoothed.append(new_val)
        return smoothed
    
    atr = wilder_smooth(tr_list, period)
    smoothed_plus_dm = wilder_smooth(plus_dm_list, period)
    smoothed_minus_dm = wilder_smooth(minus_dm_list, period)
    
    plus_di_list = []
    minus_di_list = []
    dx_list = []
    
    for i in range(len(atr)):
        if atr[i] == 0:
            plus_di_list.append(0)
            minus_di_list.append(0)
            continue
            
        plus_di = (smoothed_plus_dm[i] / atr[i]) * 100
        minus_di = (smoothed_minus_dm[i] / atr[i]) * 100
        plus_di_list.append(plus_di)
        minus_di_list.append(minus_di)
        
        di_sum = plus_di + minus_di
        if di_sum > 0:
            dx = abs(plus_di - minus_di) / di_sum * 100
            dx_list.append(dx)
    
    if len(dx_list) < period:
        return 0.0, plus_di_list[-1] if plus_di_list else 0.0, minus_di_list[-1] if minus_di_list else 0.0
    
    adx_smoothed = wilder_smooth(dx_list, period)
    
    return adx_smoothed[-1], plus_di_list[-1], minus_di_list[-1]


def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    """Calculate ATR."""
    if len(closes) < period + 1:
        return 0.0
    
    trs = []
    for i in range(1, len(highs)):
        high = highs[i]
        low = lows[i]
        prev_close = closes[i - 1]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    
    if len(trs) < period:
        return 0.0
    
    return sum(trs[-period:]) / period


def calculate_atr_percent(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    """Calculate ATR as percentage of current price."""
    atr = calculate_atr(highs, lows, closes, period)
    if closes[-1] <= 0:
        return 0.0
    return (atr / closes[-1]) * 100


def calculate_ma_slope(closes: List[float], period: int = 50, lookback: int = 5) -> float:
    """
    Calculate EMA slope as percentage change over lookback periods.
    """
    emas = calculate_ema(closes, period)
    if len(emas) < lookback + 1:
        return 0.0
    
    current = emas[-1]
    past = emas[-lookback - 1]
    if past <= 0:
        return 0.0
    
    return ((current - past) / past) * 100


def calculate_compression_ratio(
    highs: List[float], 
    lows: List[float], 
    closes: List[float],
    ema_period: int = 50,
    lookback: int = 20
) -> float:
    """
    Range compression ratio (chop detector).
    
    Measures: abs(close - EMA50) / ATR averaged over lookback.
    Low values = price hugging EMA = consolidation/chop
    High values = price extended from EMA = trending
    
    Returns normalized 0-1 where:
    - < 0.5 = compressed/choppy
    - > 0.5 = extended/trending
    """
    emas = calculate_ema(closes, ema_period)
    if len(emas) < lookback:
        return 0.5
    
    atr = calculate_atr(highs, lows, closes, 14)
    if atr <= 0:
        return 0.5
    
    # Average distance from EMA in ATR units
    distances = []
    offset = len(closes) - len(emas)
    for i in range(lookback):
        idx = len(emas) - lookback + i
        close_idx = idx + offset
        if idx >= 0 and close_idx < len(closes):
            dist = abs(closes[close_idx] - emas[idx]) / atr
            distances.append(dist)
    
    if not distances:
        return 0.5
    
    avg_distance = sum(distances) / len(distances)
    
    # Normalize: 0-2 ATR distance maps to 0-1
    return min(1.0, avg_distance / 2.0)


def calculate_donchian(highs: List[float], lows: List[float], period: int = 20) -> Tuple[float, float]:
    """Calculate Donchian Channel high and low."""
    if len(highs) < period:
        return 0.0, 0.0
    return max(highs[-period:]), min(lows[-period:])


def calculate_breakout_pressure(
    highs: List[float], 
    lows: List[float], 
    closes: List[float],
    donchian_period: int = 20
) -> float:
    """
    Breakout pressure: distance to Donchian bands in ATR units.
    
    Positive = price near upper band (bullish pressure)
    Negative = price near lower band (bearish pressure)
    Near 0 = price in middle (no pressure)
    
    Returns -1 to +1
    """
    dc_high, dc_low = calculate_donchian(highs, lows, donchian_period)
    if dc_high == dc_low:
        return 0.0
    
    atr = calculate_atr(highs, lows, closes, 14)
    if atr <= 0:
        return 0.0
    
    current_close = closes[-1]
    channel_mid = (dc_high + dc_low) / 2
    channel_range = dc_high - dc_low
    
    # Distance from mid in ATR units, normalized to -1 to +1
    distance_from_mid = (current_close - channel_mid) / atr
    
    return max(-1.0, min(1.0, distance_from_mid / 2.0))


# =============================================================================
# REGIME CLASSIFIER
# =============================================================================

@dataclass
class RegimeThresholds:
    """Tunable thresholds for regime classification."""
    # Volatility
    high_vol_atr_pct: float = 3.0       # ATR% >= this = HIGH_VOLATILITY
    low_vol_atr_pct: float = 0.5        # ATR% <= this = candidate LOW_VOL
    
    # Trend strength (ADX)
    strong_trend_adx: float = 25.0      # ADX >= this = STRONG_TREND candidate
    weak_trend_adx_low: float = 18.0    # ADX >= this = WEAK_TREND
    
    # MA slope (percentage)
    slope_threshold: float = 0.15       # abs(slope) >= this confirms trend
    
    # Compression (chop detection)
    chop_compression: float = 0.3       # compression < this = choppy
    
    # Stability (anti-whipsaw)
    stability_candles: int = 2          # Regime must hold for N candles


class RegimeClassifier:
    """
    Full Spec Market Regime Classifier (A1-A4).
    
    Decision flow:
    1. Check volatility buckets (HIGH_VOL / LOW_VOL candidate)
    2. Check trend strength (ADX + slope)
    3. Apply chop override
    4. Determine trend direction
    """
    
    def __init__(self, thresholds: Optional[RegimeThresholds] = None):
        self.thresholds = thresholds or RegimeThresholds()
        self._last_regime: Optional[MarketRegime] = None
        self._regime_count: int = 0
    
    def classify(
        self,
        highs: List[float],
        lows: List[float],
        closes: List[float],
    ) -> RegimeResult:
        """
        Classify market regime from OHLC data.
        Requires at least 200 candles for reliable indicators.
        """
        if len(closes) < 100:
            return RegimeResult(
                regime=MarketRegime.LOW_VOLATILITY_CHOP,
                trend_dir=TrendDirection.NONE,
                regime_confidence=0.0,
                adx=0.0,
                atr_percent=0.0,
                ma_slope=0.0,
                compression_ratio=0.5,
                breakout_pressure=0.0,
                details={"error": "insufficient_data", "candles": len(closes)}
            )
        
        # Calculate all features
        adx, plus_di, minus_di = calculate_adx(highs, lows, closes)
        atr_pct = calculate_atr_percent(highs, lows, closes)
        ma_slope = calculate_ma_slope(closes, period=50, lookback=5)
        compression = calculate_compression_ratio(highs, lows, closes)
        breakout_pressure = calculate_breakout_pressure(highs, lows, closes)
        
        # Calculate EMAs for trend direction
        ema50 = calculate_ema(closes, 50)
        ema200 = calculate_ema(closes, 200)
        
        # Step 1: Volatility buckets
        if atr_pct >= self.thresholds.high_vol_atr_pct:
            regime = MarketRegime.HIGH_VOLATILITY
            confidence = min(1.0, atr_pct / (self.thresholds.high_vol_atr_pct * 1.5))
        
        # Step 2: Trend strength (if not high vol)
        elif adx >= self.thresholds.strong_trend_adx and abs(ma_slope) >= self.thresholds.slope_threshold:
            regime = MarketRegime.STRONG_TREND
            confidence = min(1.0, adx / 40.0)
        
        elif adx >= self.thresholds.weak_trend_adx_low:
            regime = MarketRegime.WEAK_TREND
            confidence = (adx - self.thresholds.weak_trend_adx_low) / (self.thresholds.strong_trend_adx - self.thresholds.weak_trend_adx_low)
        
        # Step 3: Range or Chop
        else:
            # Check for chop override
            if compression < self.thresholds.chop_compression or atr_pct <= self.thresholds.low_vol_atr_pct:
                regime = MarketRegime.LOW_VOLATILITY_CHOP
                confidence = 1.0 - compression  # Lower compression = more confident it's chop
            else:
                regime = MarketRegime.RANGE
                confidence = 1.0 - (adx / self.thresholds.weak_trend_adx_low)
        
        # Determine trend direction (for STRONG/WEAK trends only)
        trend_dir = TrendDirection.NONE
        if regime in [MarketRegime.STRONG_TREND, MarketRegime.WEAK_TREND]:
            if len(ema50) > 0 and len(ema200) > 0:
                ema50_val = ema50[-1]
                ema200_val = ema200[-1] if len(ema200) > 0 else ema50[-1]
                
                if ema50_val > ema200_val and ma_slope > 0:
                    trend_dir = TrendDirection.UP
                elif ema50_val < ema200_val and ma_slope < 0:
                    trend_dir = TrendDirection.DOWN
                else:
                    # Mixed signals - downgrade to WEAK_TREND
                    if regime == MarketRegime.STRONG_TREND:
                        regime = MarketRegime.WEAK_TREND
                        confidence *= 0.7
        
        return RegimeResult(
            regime=regime,
            trend_dir=trend_dir,
            regime_confidence=max(0.0, min(1.0, confidence)),
            adx=adx,
            atr_percent=atr_pct,
            ma_slope=ma_slope,
            compression_ratio=compression,
            breakout_pressure=breakout_pressure,
            details={
                "plus_di": plus_di,
                "minus_di": minus_di,
                "ema50": ema50[-1] if ema50 else 0,
                "ema200": ema200[-1] if ema200 else 0,
            }
        )
    
    def classify_stable(
        self,
        highs: List[float],
        lows: List[float],
        closes: List[float],
    ) -> RegimeResult:
        """
        Classify with stability requirement (anti-whipsaw).
        Regime must hold for N consecutive calls before changing.
        """
        result = self.classify(highs, lows, closes)
        
        if result.regime == self._last_regime:
            self._regime_count += 1
        else:
            self._regime_count = 1
        
        # If new regime hasn't stabilized, return previous
        if self._regime_count < self.thresholds.stability_candles and self._last_regime is not None:
            result = RegimeResult(
                regime=self._last_regime,
                trend_dir=result.trend_dir,
                regime_confidence=result.regime_confidence * 0.8,  # Reduce confidence during transition
                adx=result.adx,
                atr_percent=result.atr_percent,
                ma_slope=result.ma_slope,
                compression_ratio=result.compression_ratio,
                breakout_pressure=result.breakout_pressure,
                details={**result.details, "stabilizing": True, "count": self._regime_count}
            )
        else:
            self._last_regime = result.regime
        
        return result
