"""
Bollinger Band Reversion Strategy

A mean reversion strategy that trades extreme Bollinger Band touches with RSI confirmation.
Best used in FLOW mode during RANGE regimes.
"""
from __future__ import annotations
from typing import List, Optional
import math

from app.strategy.base import Strategy, Signal, SignalResult
from app.strategy.registry import register_strategy


def calculate_sma(closes: List[float], period: int) -> List[float]:
    """Calculate Simple Moving Average."""
    if len(closes) < period:
        return []
    
    smas = []
    for i in range(period - 1, len(closes)):
        sma = sum(closes[i - period + 1:i + 1]) / period
        smas.append(sma)
    
    return smas


def calculate_bollinger_bands(
    closes: List[float],
    period: int = 20,
    std_dev: float = 2.0,
) -> tuple[List[float], List[float], List[float]]:
    """
    Calculate Bollinger Bands.
    
    Returns:
        upper: Upper band
        middle: Middle band (SMA)
        lower: Lower band
    """
    if len(closes) < period:
        return [], [], []
    
    upper = []
    middle = []
    lower = []
    
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1:i + 1]
        sma = sum(window) / period
        
        # Standard deviation
        variance = sum((x - sma) ** 2 for x in window) / period
        std = math.sqrt(variance)
        
        middle.append(sma)
        upper.append(sma + std_dev * std)
        lower.append(sma - std_dev * std)
    
    return upper, middle, lower


def calculate_rsi(closes: List[float], period: int = 14) -> List[float]:
    """Calculate Relative Strength Index."""
    if len(closes) < period + 1:
        return []
    
    gains = []
    losses = []
    
    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))
    
    if len(gains) < period:
        return []
    
    rsi_values = []
    
    # First RSI
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    if avg_loss == 0:
        rsi_values.append(100.0)
    else:
        rs = avg_gain / avg_loss
        rsi_values.append(100 - (100 / (1 + rs)))
    
    # Subsequent RSI
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        
        if avg_loss == 0:
            rsi_values.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsi_values.append(100 - (100 / (1 + rs)))
    
    return rsi_values


def calculate_bb_percent_b(
    close: float,
    upper: float,
    lower: float,
) -> float:
    """
    Calculate %B (Bollinger Band position indicator).
    0 = at lower band, 1 = at upper band
    """
    if upper == lower:
        return 0.5
    return (close - lower) / (upper - lower)


@register_strategy(
    name="bollinger_reversion",
    version="1.0.0",
    supports_asset_classes=["CRYPTO", "FOREX"],
    description="Mean reversion on Bollinger Band extremes with RSI confirmation. Best for ranging markets.",
    params_schema={
        "type": "object",
        "properties": {
            "bb_period": {"type": "integer", "default": 20},
            "bb_std": {"type": "number", "default": 2.0},
            "rsi_period": {"type": "integer", "default": 14},
            "rsi_oversold": {"type": "number", "default": 30},
            "rsi_overbought": {"type": "number", "default": 70},
            "percent_b_extreme": {"type": "number", "default": 0.05},
            "min_confidence": {"type": "number", "default": 0.55},
        },
    },
)
class BollingerReversionStrategy(Strategy):
    name = "bollinger_reversion"
    version = "1.0.0"
    strategy_type = "MEAN_REVERSION"
    
    def __init__(
        self,
        client,
        interval: str = "15m",
        bb_period: int = 20,
        bb_std: float = 2.0,
        rsi_period: int = 14,
        rsi_oversold: float = 30,
        rsi_overbought: float = 70,
        percent_b_extreme: float = 0.05,
        min_confidence: float = 0.55,
    ):
        self.client = client
        self.interval = interval
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.rsi_period = rsi_period
        self.rsi_oversold = rsi_oversold
        self.rsi_overbought = rsi_overbought
        self.percent_b_extreme = percent_b_extreme
        self.min_confidence = min_confidence
    
    def get_signal(self, symbol: str) -> SignalResult:
        try:
            klines = self.client.klines(symbol=symbol, interval=self.interval, limit=100)
            
            if not klines or len(klines) < 50:
                return SignalResult(Signal.HOLD, 0.0, "insufficient_data", meta={})
            
            opens = [float(k[1]) for k in klines]
            highs = [float(k[2]) for k in klines]
            lows = [float(k[3]) for k in klines]
            closes = [float(k[4]) for k in klines]
            
            # Calculate indicators
            upper, middle, lower = calculate_bollinger_bands(closes, self.bb_period, self.bb_std)
            rsi_values = calculate_rsi(closes, self.rsi_period)
            
            if not upper or not rsi_values:
                return SignalResult(Signal.HOLD, 0.0, "indicator_calc_failed", meta={})
            
            # Current values
            current_close = closes[-1]
            current_upper = upper[-1]
            current_lower = lower[-1]
            current_middle = middle[-1]
            current_rsi = rsi_values[-1]
            
            # Calculate %B
            percent_b = calculate_bb_percent_b(current_close, current_upper, current_lower)
            
            # Previous candle for reversal confirmation
            prev_close = closes[-2]
            prev_open = opens[-2]
            current_open = opens[-1]
            
            signal = Signal.HOLD
            confidence = 0.0
            reasons = []
            
            # Bullish reversal: price at/below lower band + oversold RSI + bullish candle
            if percent_b <= self.percent_b_extreme:
                reasons.append("at_lower_band")
                
                if current_rsi <= self.rsi_oversold:
                    reasons.append("rsi_oversold")
                    
                    # Check for reversal candle (bullish engulfing or hammer-like)
                    is_bullish_candle = current_close > current_open
                    is_reversal = current_close > prev_close
                    
                    if is_bullish_candle and is_reversal:
                        reasons.append("bullish_reversal_candle")
                        signal = Signal.BUY
                        
                        # Confidence based on extremity
                        extremity_score = (self.percent_b_extreme - percent_b) / self.percent_b_extreme
                        rsi_score = (self.rsi_oversold - current_rsi) / self.rsi_oversold
                        
                        confidence = 0.6 + min(0.2, extremity_score * 0.2) + min(0.2, rsi_score * 0.2)
            
            # Bearish reversal: price at/above upper band + overbought RSI + bearish candle
            elif percent_b >= (1 - self.percent_b_extreme):
                reasons.append("at_upper_band")
                
                if current_rsi >= self.rsi_overbought:
                    reasons.append("rsi_overbought")
                    
                    # Check for reversal candle (bearish engulfing or shooting star-like)
                    is_bearish_candle = current_close < current_open
                    is_reversal = current_close < prev_close
                    
                    if is_bearish_candle and is_reversal:
                        reasons.append("bearish_reversal_candle")
                        signal = Signal.SELL
                        
                        # Confidence based on extremity
                        extremity_score = (percent_b - (1 - self.percent_b_extreme)) / self.percent_b_extreme
                        rsi_score = (current_rsi - self.rsi_overbought) / (100 - self.rsi_overbought)
                        
                        confidence = 0.6 + min(0.2, extremity_score * 0.2) + min(0.2, rsi_score * 0.2)
            
            # Apply confidence gate
            if confidence < self.min_confidence:
                return SignalResult(
                    Signal.HOLD,
                    confidence,
                    "gated_low_confidence" if confidence > 0 else "no_signal",
                    meta={
                        "reasons": reasons,
                        "percent_b": percent_b,
                        "rsi": current_rsi,
                    }
                )
            
            return SignalResult(
                signal,
                confidence,
                "bollinger_reversion",
                meta={
                    "reasons": reasons,
                    "percent_b": percent_b,
                    "rsi": current_rsi,
                    "bb_upper": current_upper,
                    "bb_lower": current_lower,
                    "bb_middle": current_middle,
                    "strategy_type": self.strategy_type,
                }
            )
            
        except Exception as e:
            return SignalResult(Signal.HOLD, 0.0, f"error:{e}", meta={})
