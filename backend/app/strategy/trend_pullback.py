"""
Trend Pullback Strategy - Precision Strategy A3

Entry on pullbacks to key EMAs during strong trends.
Uses RSI reset as confirmation.

Bias (4H): Strong trend UP/DOWN
Confirm (1H): RSI pulls back toward 40-50 zone (uptrend) or 50-60 (downtrend)
Entry (15m): Cross back above EMA20 after pullback
"""
from __future__ import annotations
from typing import List, Optional
import math

from app.strategy.base import Strategy, Signal, SignalResult
from app.strategy.registry import register_strategy


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


def calculate_rsi(closes: List[float], period: int = 14) -> List[float]:
    """Calculate RSI."""
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
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    if avg_loss == 0:
        rsi_values.append(100.0)
    else:
        rs = avg_gain / avg_loss
        rsi_values.append(100 - (100 / (1 + rs)))
    
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        
        if avg_loss == 0:
            rsi_values.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsi_values.append(100 - (100 / (1 + rs)))
    
    return rsi_values


def calculate_adx(
    highs: List[float], 
    lows: List[float], 
    closes: List[float], 
    period: int = 14
) -> float:
    """Calculate ADX."""
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
    
    # Wilder smoothing
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


@register_strategy(
    name="trend_pullback",
    version="1.0.0",
    supports_asset_classes=["CRYPTO", "FOREX"],
    description="Trend pullback entry on EMA + RSI reset. Precision Mode A3.",
    params_schema={
        "type": "object",
        "properties": {
            "ema_fast": {"type": "integer", "default": 20},
            "ema_slow": {"type": "integer", "default": 50},
            "rsi_period": {"type": "integer", "default": 14},
            "adx_threshold": {"type": "number", "default": 25},
            "min_confidence": {"type": "number", "default": 0.75},
        },
    },
)
class TrendPullbackStrategy(Strategy):
    name = "trend_pullback"
    version = "1.0.0"
    strategy_type = "TREND"
    
    def __init__(
        self,
        client,
        interval: str = "15m",
        ema_fast: int = 20,
        ema_slow: int = 50,
        rsi_period: int = 14,
        adx_threshold: float = 25,
        min_confidence: float = 0.75,
    ):
        self.client = client
        self.interval = interval
        self.ema_fast = ema_fast
        self.ema_slow = ema_slow
        self.rsi_period = rsi_period
        self.adx_threshold = adx_threshold
        self.min_confidence = min_confidence
    
    def get_signal(self, symbol: str) -> SignalResult:
        try:
            klines = self.client.klines(symbol=symbol, interval=self.interval, limit=200)
            
            if not klines or len(klines) < 100:
                return SignalResult(Signal.HOLD, 0.0, "insufficient_data", meta={})
            
            highs = [float(k[2]) for k in klines]
            lows = [float(k[3]) for k in klines]
            closes = [float(k[4]) for k in klines]
            
            # Calculate indicators
            ema_fast_vals = calculate_ema(closes, self.ema_fast)
            ema_slow_vals = calculate_ema(closes, self.ema_slow)
            rsi_vals = calculate_rsi(closes, self.rsi_period)
            adx = calculate_adx(highs, lows, closes)
            
            if not ema_fast_vals or not ema_slow_vals or not rsi_vals:
                return SignalResult(Signal.HOLD, 0.0, "indicator_calc_failed", meta={})
            
            # Current values
            current_close = closes[-1]
            prev_close = closes[-2]
            ema_fast_now = ema_fast_vals[-1]
            ema_fast_prev = ema_fast_vals[-2]
            ema_slow_now = ema_slow_vals[-1]
            rsi_now = rsi_vals[-1]
            rsi_prev = rsi_vals[-2] if len(rsi_vals) > 1 else rsi_now
            
            signal = Signal.HOLD
            confidence = 0.0
            reasons = []
            
            # Check ADX for trend strength (Bias requirement)
            if adx < self.adx_threshold:
                return SignalResult(
                    Signal.HOLD, 0.0, "adx_too_low",
                    meta={"adx": adx, "threshold": self.adx_threshold}
                )
            
            reasons.append(f"adx_strong:{adx:.1f}")
            
            # UPTREND PULLBACK
            if ema_fast_now > ema_slow_now:
                reasons.append("uptrend")
                
                # RSI reset zone (40-50 in uptrend)
                rsi_in_reset = 40 <= rsi_now <= 55
                rsi_was_lower = rsi_prev < rsi_now  # RSI turning up
                
                # Price crossed back above EMA20
                price_crossed_ema = prev_close < ema_fast_prev and current_close > ema_fast_now
                
                # OR price bounced off EMA20 zone
                price_at_ema = abs(current_close - ema_fast_now) / ema_fast_now < 0.005  # Within 0.5%
                
                if rsi_in_reset and rsi_was_lower:
                    reasons.append("rsi_reset_40_55")
                    
                    if price_crossed_ema:
                        reasons.append("price_crossed_ema20")
                        signal = Signal.BUY
                        confidence = 0.80
                    elif price_at_ema and current_close > prev_close:
                        reasons.append("price_bounce_ema20")
                        signal = Signal.BUY
                        confidence = 0.75
            
            # DOWNTREND PULLBACK
            elif ema_fast_now < ema_slow_now:
                reasons.append("downtrend")
                
                # RSI reset zone (50-60 in downtrend)
                rsi_in_reset = 45 <= rsi_now <= 60
                rsi_was_higher = rsi_prev > rsi_now  # RSI turning down
                
                # Price crossed back below EMA20
                price_crossed_ema = prev_close > ema_fast_prev and current_close < ema_fast_now
                
                # OR price rejected from EMA20 zone
                price_at_ema = abs(current_close - ema_fast_now) / ema_fast_now < 0.005
                
                if rsi_in_reset and rsi_was_higher:
                    reasons.append("rsi_reset_45_60")
                    
                    if price_crossed_ema:
                        reasons.append("price_crossed_ema20")
                        signal = Signal.SELL
                        confidence = 0.80
                    elif price_at_ema and current_close < prev_close:
                        reasons.append("price_reject_ema20")
                        signal = Signal.SELL
                        confidence = 0.75
            
            # Apply confidence gate
            if confidence < self.min_confidence:
                return SignalResult(
                    Signal.HOLD,
                    confidence,
                    "gated" if confidence > 0 else "no_signal",
                    meta={"reasons": reasons, "adx": adx, "rsi": rsi_now}
                )
            
            return SignalResult(
                signal,
                confidence,
                "trend_pullback",
                meta={
                    "reasons": reasons,
                    "adx": adx,
                    "rsi": rsi_now,
                    "ema_fast": ema_fast_now,
                    "ema_slow": ema_slow_now,
                    "strategy_type": self.strategy_type,
                }
            )
            
        except Exception as e:
            return SignalResult(Signal.HOLD, 0.0, f"error:{e}", meta={})
