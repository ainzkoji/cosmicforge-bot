"""
SuperTrend + EMA Trend Strategy

A trend-following strategy that uses SuperTrend indicator with EMA slope confirmation.
Best used in PRECISION mode during STRONG_TREND regimes.
"""
from __future__ import annotations
from typing import List, Optional
import math

from app.strategy.base import Strategy, Signal, SignalResult
from app.strategy.registry import register_strategy


def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int = 10) -> List[float]:
    """Calculate Average True Range."""
    if len(closes) < period + 1:
        return []
    
    trs = []
    for i in range(1, len(highs)):
        high = highs[i]
        low = lows[i]
        prev_close = closes[i - 1]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    
    # Simple moving average of TR
    atrs = []
    for i in range(period - 1, len(trs)):
        atr = sum(trs[i - period + 1:i + 1]) / period
        atrs.append(atr)
    
    return atrs


def calculate_supertrend(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int = 10,
    multiplier: float = 3.0,
) -> tuple[List[float], List[int]]:
    """
    Calculate SuperTrend indicator.
    
    Returns:
        supertrend: List of SuperTrend values
        direction: List of directions (1 = bullish, -1 = bearish)
    """
    if len(closes) < period + 1:
        return [], []
    
    atrs = calculate_atr(highs, lows, closes, period)
    
    if not atrs:
        return [], []
    
    # Align indices - ATR starts at index (period)
    offset = len(closes) - len(atrs)
    
    supertrend = []
    direction = []
    
    upper_bands = []
    lower_bands = []
    
    for i in range(len(atrs)):
        idx = i + offset
        
        # HL2 (midpoint)
        hl2 = (highs[idx] + lows[idx]) / 2
        
        # Basic bands
        basic_upper = hl2 + (multiplier * atrs[i])
        basic_lower = hl2 - (multiplier * atrs[i])
        
        # Final bands with trend logic
        if i == 0:
            final_upper = basic_upper
            final_lower = basic_lower
            st_dir = 1 if closes[idx] > basic_lower else -1
        else:
            prev_upper = upper_bands[-1]
            prev_lower = lower_bands[-1]
            prev_close = closes[idx - 1]
            
            # Upper band
            if basic_upper < prev_upper or prev_close > prev_upper:
                final_upper = basic_upper
            else:
                final_upper = prev_upper
            
            # Lower band
            if basic_lower > prev_lower or prev_close < prev_lower:
                final_lower = basic_lower
            else:
                final_lower = prev_lower
            
            # Direction
            prev_dir = direction[-1]
            if prev_dir == 1:
                if closes[idx] < final_lower:
                    st_dir = -1
                else:
                    st_dir = 1
            else:
                if closes[idx] > final_upper:
                    st_dir = 1
                else:
                    st_dir = -1
        
        upper_bands.append(final_upper)
        lower_bands.append(final_lower)
        direction.append(st_dir)
        
        # SuperTrend value
        if st_dir == 1:
            supertrend.append(final_lower)
        else:
            supertrend.append(final_upper)
    
    return supertrend, direction


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


@register_strategy(
    name="supertrend",
    version="1.0.0",
    supports_asset_classes=["CRYPTO", "FOREX"],
    description="SuperTrend crossover with EMA slope confirmation. Best for trending markets.",
    params_schema={
        "type": "object",
        "properties": {
            "supertrend_period": {"type": "integer", "default": 10},
            "supertrend_multiplier": {"type": "number", "default": 3.0},
            "ema_period": {"type": "integer", "default": 20},
            "min_ema_slope": {"type": "number", "default": 0.1},
            "min_confidence": {"type": "number", "default": 0.50},
        },
    },
)
class SuperTrendStrategy(Strategy):
    name = "supertrend"
    version = "1.0.0"
    strategy_type = "TREND"
    
    def __init__(
        self,
        client,
        interval: str = "15m",
        supertrend_period: int = 10,
        supertrend_multiplier: float = 3.0,
        ema_period: int = 20,
        min_ema_slope: float = 0.1,
        min_confidence: float = 0.50,
    ):
        self.client = client
        self.interval = interval
        self.supertrend_period = supertrend_period
        self.supertrend_multiplier = supertrend_multiplier
        self.ema_period = ema_period
        self.min_ema_slope = min_ema_slope
        self.min_confidence = min_confidence
    
    def get_signal(self, symbol: str) -> SignalResult:
        try:
            klines = self.client.klines(symbol=symbol, interval=self.interval, limit=200)
            
            if not klines or len(klines) < 100:
                return SignalResult(Signal.HOLD, 0.0, "insufficient_data", meta={})
            
            highs = [float(k[2]) for k in klines]
            lows = [float(k[3]) for k in klines]
            closes = [float(k[4]) for k in klines]
            
            # Calculate SuperTrend
            supertrend, direction = calculate_supertrend(
                highs, lows, closes,
                period=self.supertrend_period,
                multiplier=self.supertrend_multiplier,
            )
            
            if len(direction) < 2:
                return SignalResult(Signal.HOLD, 0.0, "supertrend_calc_failed", meta={})
            
            # Calculate EMA for slope confirmation
            emas = calculate_ema(closes, self.ema_period)
            
            if len(emas) < 5:
                return SignalResult(Signal.HOLD, 0.0, "ema_calc_failed", meta={})
            
            # Current state
            current_dir = direction[-1]
            prev_dir = direction[-2]
            current_close = closes[-1]
            current_st = supertrend[-1]
            current_ema = emas[-1]
            
            # EMA slope (percentage change over 5 periods)
            ema_slope = ((emas[-1] - emas[-5]) / emas[-5]) * 100 if len(emas) >= 5 and emas[-5] > 0 else 0.0
            
            # Signal logic
            signal = Signal.HOLD
            confidence = 0.0
            reasons = []
            
            # BUY: SuperTrend flips bullish + EMA slope positive
            if current_dir == 1 and prev_dir == -1:
                if ema_slope > self.min_ema_slope:
                    signal = Signal.BUY
                    confidence = 0.7 + min(0.3, abs(ema_slope) / 1.0)
                    reasons.append("supertrend_bullish_cross")
                    reasons.append("ema_slope_confirms")
                else:
                    signal = Signal.HOLD
                    reasons.append("supertrend_bullish_cross")
                    reasons.append("ema_slope_weak")
            
            # SELL: SuperTrend flips bearish + EMA slope negative
            elif current_dir == -1 and prev_dir == 1:
                if ema_slope < -self.min_ema_slope:
                    signal = Signal.SELL
                    confidence = 0.7 + min(0.3, abs(ema_slope) / 1.0)
                    reasons.append("supertrend_bearish_cross")
                    reasons.append("ema_slope_confirms")
                else:
                    signal = Signal.HOLD
                    reasons.append("supertrend_bearish_cross")
                    reasons.append("ema_slope_weak")
            
            # Continuation signals (already in trend)
            elif current_dir == 1 and current_close > current_ema and ema_slope > self.min_ema_slope * 2:
                # Strong bullish continuation
                signal = Signal.BUY
                confidence = 0.5 + min(0.3, abs(ema_slope) / 2.0)
                reasons.append("supertrend_bullish")
                reasons.append("strong_ema_slope")
            
            elif current_dir == -1 and current_close < current_ema and ema_slope < -self.min_ema_slope * 2:
                # Strong bearish continuation
                signal = Signal.SELL
                confidence = 0.5 + min(0.3, abs(ema_slope) / 2.0)
                reasons.append("supertrend_bearish")
                reasons.append("strong_ema_slope")
            
            # Apply confidence gate
            if confidence < self.min_confidence:
                return SignalResult(
                    Signal.HOLD,
                    confidence,
                    "gated_low_confidence",
                    meta={
                        "reasons": reasons,
                        "ema_slope": ema_slope,
                        "supertrend_dir": current_dir,
                    }
                )
            
            return SignalResult(
                signal,
                confidence,
                "supertrend_ema",
                meta={
                    "reasons": reasons,
                    "ema_slope": ema_slope,
                    "supertrend": current_st,
                    "supertrend_dir": current_dir,
                    "strategy_type": self.strategy_type,
                }
            )
            
        except Exception as e:
            return SignalResult(Signal.HOLD, 0.0, f"error:{e}", meta={})
