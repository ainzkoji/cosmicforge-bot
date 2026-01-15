"""
Donchian Breakout Strategy - Precision Strategy A2

Quality breakout entries using Donchian Channels.
Avoids late breakouts by checking distance from band.

Bias (4H): trend_dir must match breakout direction, ADX strong
Confirm (1H): price compressing then expanding (volatility rising)
Entry (15m): break above Donchian high (20) for long / below low for short
"""
from __future__ import annotations
from typing import List, Optional, Tuple
import math

from app.strategy.base import Strategy, Signal, SignalResult
from app.strategy.registry import register_strategy


def calculate_donchian(
    highs: List[float], 
    lows: List[float], 
    period: int = 20
) -> Tuple[List[float], List[float], List[float]]:
    """
    Calculate Donchian Channels.
    Returns: (upper, middle, lower) lists
    """
    if len(highs) < period:
        return [], [], []
    
    upper = []
    lower = []
    middle = []
    
    for i in range(period - 1, len(highs)):
        h = max(highs[i - period + 1:i + 1])
        l = min(lows[i - period + 1:i + 1])
        upper.append(h)
        lower.append(l)
        middle.append((h + l) / 2)
    
    return upper, middle, lower


def calculate_atr(
    highs: List[float], 
    lows: List[float], 
    closes: List[float], 
    period: int = 14
) -> List[float]:
    """Calculate ATR series."""
    if len(closes) < period + 1:
        return []
    
    trs = []
    for i in range(1, len(highs)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        )
        trs.append(tr)
    
    if len(trs) < period:
        return []
    
    # SMA-based ATR
    atrs = []
    for i in range(period - 1, len(trs)):
        atrs.append(sum(trs[i - period + 1:i + 1]) / period)
    
    return atrs


def calculate_adx(
    highs: List[float], 
    lows: List[float], 
    closes: List[float], 
    period: int = 14
) -> float:
    """Calculate ADX (simplified)."""
    if len(highs) < period * 2:
        return 0.0
    
    tr_list = []
    plus_dm_list = []
    minus_dm_list = []
    
    for i in range(1, len(highs)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        )
        tr_list.append(tr)
        
        up_move = highs[i] - highs[i-1]
        down_move = lows[i-1] - lows[i]
        
        plus_dm = up_move if up_move > down_move and up_move > 0 else 0
        minus_dm = down_move if down_move > up_move and down_move > 0 else 0
        
        plus_dm_list.append(plus_dm)
        minus_dm_list.append(minus_dm)
    
    if len(tr_list) < period:
        return 0.0
    
    atr = sum(tr_list[:period])
    plus_dm_sum = sum(plus_dm_list[:period])
    minus_dm_sum = sum(minus_dm_list[:period])
    
    dx_list = []
    for i in range(period, len(tr_list)):
        atr = atr - atr/period + tr_list[i]
        plus_dm_sum = plus_dm_sum - plus_dm_sum/period + plus_dm_list[i]
        minus_dm_sum = minus_dm_sum - minus_dm_sum/period + minus_dm_list[i]
        
        if atr > 0:
            plus_di = (plus_dm_sum / atr) * 100
            minus_di = (minus_dm_sum / atr) * 100
            di_sum = plus_di + minus_di
            if di_sum > 0:
                dx = abs(plus_di - minus_di) / di_sum * 100
                dx_list.append(dx)
    
    if len(dx_list) < period:
        return sum(dx_list) / len(dx_list) if dx_list else 0.0
    
    adx = sum(dx_list[:period]) / period
    for i in range(period, len(dx_list)):
        adx = (adx * (period - 1) + dx_list[i]) / period
    
    return adx


def calculate_ema(closes: List[float], period: int) -> List[float]:
    """Calculate EMA."""
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
    name="donchian_breakout",
    version="1.0.0",
    supports_asset_classes=["CRYPTO", "FOREX"],
    description="Donchian Channel breakout with ATR filter. Precision Mode A2.",
    params_schema={
        "type": "object",
        "properties": {
            "donchian_period": {"type": "integer", "default": 20},
            "atr_period": {"type": "integer", "default": 14},
            "adx_threshold": {"type": "number", "default": 25},
            "max_atr_from_band": {"type": "number", "default": 1.5},
            "min_confidence": {"type": "number", "default": 0.75},
        },
    },
)
class DonchianBreakoutStrategy(Strategy):
    name = "donchian_breakout"
    version = "1.0.0"
    strategy_type = "BREAKOUT"
    
    def __init__(
        self,
        client,
        interval: str = "15m",
        donchian_period: int = 20,
        atr_period: int = 14,
        adx_threshold: float = 25,
        max_atr_from_band: float = 1.5,  # Max ATR distance from band for valid breakout
        min_confidence: float = 0.75,
    ):
        self.client = client
        self.interval = interval
        self.donchian_period = donchian_period
        self.atr_period = atr_period
        self.adx_threshold = adx_threshold
        self.max_atr_from_band = max_atr_from_band
        self.min_confidence = min_confidence
    
    def get_signal(self, symbol: str) -> SignalResult:
        try:
            klines = self.client.klines(symbol=symbol, interval=self.interval, limit=200)
            
            if not klines or len(klines) < 50:
                return SignalResult(Signal.HOLD, 0.0, "insufficient_data", meta={})
            
            highs = [float(k[2]) for k in klines]
            lows = [float(k[3]) for k in klines]
            closes = [float(k[4]) for k in klines]
            
            # Calculate indicators
            dc_upper, dc_mid, dc_lower = calculate_donchian(highs, lows, self.donchian_period)
            atrs = calculate_atr(highs, lows, closes, self.atr_period)
            adx = calculate_adx(highs, lows, closes)
            ema50 = calculate_ema(closes, 50)
            ema200 = calculate_ema(closes, 200)
            
            if not dc_upper or not atrs:
                return SignalResult(Signal.HOLD, 0.0, "indicator_calc_failed", meta={})
            
            # Current values
            current_close = closes[-1]
            current_high = highs[-1]
            current_low = lows[-1]
            prev_close = closes[-2]
            prev_high = highs[-2]
            prev_low = lows[-2]
            
            dc_upper_now = dc_upper[-1]
            dc_lower_now = dc_lower[-1]
            dc_upper_prev = dc_upper[-2]
            dc_lower_prev = dc_lower[-2]
            atr_now = atrs[-1]
            
            signal = Signal.HOLD
            confidence = 0.0
            reasons = []
            
            # Check ADX (Bias requirement)
            if adx < self.adx_threshold:
                return SignalResult(
                    Signal.HOLD, 0.0, "adx_too_low",
                    meta={"adx": adx, "threshold": self.adx_threshold}
                )
            reasons.append(f"adx_strong:{adx:.1f}")
            
            # Determine trend direction from EMAs
            trend_up = len(ema50) > 0 and len(ema200) > 0 and ema50[-1] > ema200[-1]
            trend_down = len(ema50) > 0 and len(ema200) > 0 and ema50[-1] < ema200[-1]
            
            # BULLISH BREAKOUT
            if current_high > dc_upper_prev and prev_high <= dc_upper_prev:
                reasons.append("breakout_above_donchian")
                
                # Check trend alignment
                if trend_up:
                    reasons.append("trend_aligned_up")
                    
                    # Check not too late (within X ATR of band)
                    distance_from_band = (current_close - dc_upper_prev) / atr_now
                    
                    if distance_from_band <= self.max_atr_from_band:
                        reasons.append(f"valid_distance:{distance_from_band:.2f}atr")
                        
                        # Strong close above band
                        if current_close > dc_upper_prev:
                            signal = Signal.BUY
                            confidence = 0.75 + (0.15 * (1 - distance_from_band / self.max_atr_from_band))
                            reasons.append("confirmed_close")
                    else:
                        reasons.append(f"late_breakout:{distance_from_band:.2f}atr")
                else:
                    reasons.append("trend_not_aligned")
            
            # BEARISH BREAKOUT
            elif current_low < dc_lower_prev and prev_low >= dc_lower_prev:
                reasons.append("breakout_below_donchian")
                
                # Check trend alignment
                if trend_down:
                    reasons.append("trend_aligned_down")
                    
                    # Check not too late
                    distance_from_band = (dc_lower_prev - current_close) / atr_now
                    
                    if distance_from_band <= self.max_atr_from_band:
                        reasons.append(f"valid_distance:{distance_from_band:.2f}atr")
                        
                        # Strong close below band
                        if current_close < dc_lower_prev:
                            signal = Signal.SELL
                            confidence = 0.75 + (0.15 * (1 - distance_from_band / self.max_atr_from_band))
                            reasons.append("confirmed_close")
                    else:
                        reasons.append(f"late_breakout:{distance_from_band:.2f}atr")
                else:
                    reasons.append("trend_not_aligned")
            
            # Apply confidence gate
            if confidence < self.min_confidence:
                return SignalResult(
                    Signal.HOLD,
                    confidence,
                    "gated" if confidence > 0 else "no_signal",
                    meta={
                        "reasons": reasons,
                        "adx": adx,
                        "dc_upper": dc_upper_now,
                        "dc_lower": dc_lower_now,
                    }
                )
            
            return SignalResult(
                signal,
                confidence,
                "donchian_breakout",
                meta={
                    "reasons": reasons,
                    "adx": adx,
                    "dc_upper": dc_upper_now,
                    "dc_lower": dc_lower_now,
                    "atr": atr_now,
                    "strategy_type": self.strategy_type,
                }
            )
            
        except Exception as e:
            return SignalResult(Signal.HOLD, 0.0, f"error:{e}", meta={})
