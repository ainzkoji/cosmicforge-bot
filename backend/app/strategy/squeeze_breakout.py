"""
Bollinger-Keltner Squeeze Breakout Strategy

A volatility expansion strategy that trades breakouts after periods of low volatility
(when Bollinger Bands contract inside Keltner Channels).

Best used in PRECISION mode for high-quality breakout entries.
"""
from __future__ import annotations
from typing import List, Optional
import math

from app.strategy.base import Strategy, Signal, SignalResult
from app.strategy.registry import register_strategy


def calculate_sma(closes: List[float], period: int) -> float:
    """Calculate Simple Moving Average."""
    if len(closes) < period:
        return 0.0
    return sum(closes[-period:]) / period


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


def calculate_atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> List[float]:
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
    
    # EMA of TR for ATR
    emas = calculate_ema(trs, period)
    return emas


def calculate_bollinger_bands(
    closes: List[float],
    period: int = 20,
    std_dev: float = 2.0,
) -> tuple[float, float, float]:
    """Calculate current Bollinger Bands values."""
    if len(closes) < period:
        return 0.0, 0.0, 0.0
    
    window = closes[-period:]
    sma = sum(window) / period
    
    variance = sum((x - sma) ** 2 for x in window) / period
    std = math.sqrt(variance)
    
    return sma + std_dev * std, sma, sma - std_dev * std  # upper, middle, lower


def calculate_keltner_channels(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    ema_period: int = 20,
    atr_period: int = 10,
    atr_multiplier: float = 1.5,
) -> tuple[float, float, float]:
    """Calculate current Keltner Channels values."""
    emas = calculate_ema(closes, ema_period)
    atrs = calculate_atr(highs, lows, closes, atr_period)
    
    if not emas or not atrs:
        return 0.0, 0.0, 0.0
    
    middle = emas[-1]
    atr = atrs[-1]
    
    return middle + atr_multiplier * atr, middle, middle - atr_multiplier * atr


def calculate_momentum(closes: List[float], period: int = 12) -> List[float]:
    """Calculate momentum (current - period ago)."""
    if len(closes) < period + 1:
        return []
    
    momentum = []
    for i in range(period, len(closes)):
        mom = closes[i] - closes[i - period]
        momentum.append(mom)
    
    return momentum


@register_strategy(
    name="squeeze_breakout",
    version="1.0.0",
    supports_asset_classes=["CRYPTO", "FOREX"],
    description="Bollinger-Keltner squeeze breakout. Trades volatility expansion after compression.",
    params_schema={
        "type": "object",
        "properties": {
            "bb_period": {"type": "integer", "default": 20},
            "bb_std": {"type": "number", "default": 2.0},
            "kc_ema_period": {"type": "integer", "default": 20},
            "kc_atr_period": {"type": "integer", "default": 10},
            "kc_atr_mult": {"type": "number", "default": 1.5},
            "momentum_period": {"type": "integer", "default": 12},
            "squeeze_lookback": {"type": "integer", "default": 6},
            "min_confidence": {"type": "number", "default": 0.60},
        },
    },
)
class SqueezeBreakoutStrategy(Strategy):
    name = "squeeze_breakout"
    version = "1.0.0"
    strategy_type = "BREAKOUT"
    
    def __init__(
        self,
        client,
        interval: str = "15m",
        bb_period: int = 20,
        bb_std: float = 2.0,
        kc_ema_period: int = 20,
        kc_atr_period: int = 10,
        kc_atr_mult: float = 1.5,
        momentum_period: int = 12,
        squeeze_lookback: int = 6,
        min_confidence: float = 0.60,
    ):
        self.client = client
        self.interval = interval
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.kc_ema_period = kc_ema_period
        self.kc_atr_period = kc_atr_period
        self.kc_atr_mult = kc_atr_mult
        self.momentum_period = momentum_period
        self.squeeze_lookback = squeeze_lookback
        self.min_confidence = min_confidence
    
    def _is_in_squeeze(
        self,
        bb_upper: float,
        bb_lower: float,
        kc_upper: float,
        kc_lower: float,
    ) -> bool:
        """Squeeze = BB inside KC."""
        return bb_lower > kc_lower and bb_upper < kc_upper
    
    def _calculate_squeeze_history(
        self,
        highs: List[float],
        lows: List[float],
        closes: List[float],
        lookback: int,
    ) -> List[bool]:
        """Calculate squeeze status for last N candles."""
        squeeze_history = []
        
        for i in range(lookback, 0, -1):
            end_idx = len(closes) - i + 1
            
            if end_idx < max(self.bb_period, self.kc_ema_period, self.kc_atr_period) + 1:
                squeeze_history.append(False)
                continue
            
            # Calculate bands at this point
            bb_upper, bb_mid, bb_lower = calculate_bollinger_bands(
                closes[:end_idx], self.bb_period, self.bb_std
            )
            kc_upper, kc_mid, kc_lower = calculate_keltner_channels(
                highs[:end_idx], lows[:end_idx], closes[:end_idx],
                self.kc_ema_period, self.kc_atr_period, self.kc_atr_mult
            )
            
            squeeze_history.append(self._is_in_squeeze(bb_upper, bb_lower, kc_upper, kc_lower))
        
        return squeeze_history
    
    def get_signal(self, symbol: str) -> SignalResult:
        try:
            klines = self.client.klines(symbol=symbol, interval=self.interval, limit=100)
            
            if not klines or len(klines) < 50:
                return SignalResult(Signal.HOLD, 0.0, "insufficient_data", meta={})
            
            highs = [float(k[2]) for k in klines]
            lows = [float(k[3]) for k in klines]
            closes = [float(k[4]) for k in klines]
            
            # Current bands
            bb_upper, bb_mid, bb_lower = calculate_bollinger_bands(closes, self.bb_period, self.bb_std)
            kc_upper, kc_mid, kc_lower = calculate_keltner_channels(
                highs, lows, closes,
                self.kc_ema_period, self.kc_atr_period, self.kc_atr_mult
            )
            
            if bb_upper == 0 or kc_upper == 0:
                return SignalResult(Signal.HOLD, 0.0, "indicator_calc_failed", meta={})
            
            # Current squeeze status
            is_squeeze_now = self._is_in_squeeze(bb_upper, bb_lower, kc_upper, kc_lower)
            
            # Squeeze history
            squeeze_history = self._calculate_squeeze_history(highs, lows, closes, self.squeeze_lookback)
            
            # Was in squeeze recently?
            was_in_squeeze = any(squeeze_history[:-1]) if len(squeeze_history) > 1 else False
            
            # Momentum
            momentum_values = calculate_momentum(closes, self.momentum_period)
            
            if not momentum_values:
                return SignalResult(Signal.HOLD, 0.0, "momentum_calc_failed", meta={})
            
            current_momentum = momentum_values[-1]
            prev_momentum = momentum_values[-2] if len(momentum_values) > 1 else 0
            
            signal = Signal.HOLD
            confidence = 0.0
            reasons = []
            
            # SQUEEZE RELEASE: Was in squeeze, now released
            if was_in_squeeze and not is_squeeze_now:
                reasons.append("squeeze_released")
                
                # Momentum direction determines breakout direction
                if current_momentum > 0 and current_momentum > prev_momentum:
                    reasons.append("bullish_momentum")
                    signal = Signal.BUY
                    
                    # Confidence based on momentum strength
                    momentum_strength = min(1.0, abs(current_momentum) / (closes[-1] * 0.02))  # 2% move as baseline
                    confidence = 0.65 + min(0.25, momentum_strength * 0.25)
                
                elif current_momentum < 0 and current_momentum < prev_momentum:
                    reasons.append("bearish_momentum")
                    signal = Signal.SELL
                    
                    momentum_strength = min(1.0, abs(current_momentum) / (closes[-1] * 0.02))
                    confidence = 0.65 + min(0.25, momentum_strength * 0.25)
                
                else:
                    reasons.append("weak_momentum")
            
            # Currently in squeeze - no signal (waiting)
            elif is_squeeze_now:
                reasons.append("in_squeeze_waiting")
            
            # Apply confidence gate
            if confidence < self.min_confidence:
                return SignalResult(
                    Signal.HOLD,
                    confidence,
                    "gated_low_confidence" if confidence > 0 else "no_signal",
                    meta={
                        "reasons": reasons,
                        "in_squeeze": is_squeeze_now,
                        "was_in_squeeze": was_in_squeeze,
                        "momentum": current_momentum,
                    }
                )
            
            return SignalResult(
                signal,
                confidence,
                "squeeze_breakout",
                meta={
                    "reasons": reasons,
                    "in_squeeze": is_squeeze_now,
                    "was_in_squeeze": was_in_squeeze,
                    "momentum": current_momentum,
                    "bb_upper": bb_upper,
                    "bb_lower": bb_lower,
                    "kc_upper": kc_upper,
                    "kc_lower": kc_lower,
                    "strategy_type": self.strategy_type,
                }
            )
            
        except Exception as e:
            return SignalResult(Signal.HOLD, 0.0, f"error:{e}", meta={})
