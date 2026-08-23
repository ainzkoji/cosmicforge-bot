"""
VWAP Reversion Strategy - Flow Strategy B2

Intraday mean reversion toward VWAP.
Works best in RANGE or WEAK_TREND regimes.

Bias (1H): price deviation from VWAP in ATR units
Entry (5m): enter when price mean-reverts toward VWAP with trigger
"""
from __future__ import annotations
from typing import List, Optional
from datetime import datetime
import math

from app.strategy.base import Strategy, Signal, SignalResult
from app.strategy.registry import register_strategy


def calculate_vwap(
    highs: List[float], 
    lows: List[float], 
    closes: List[float], 
    volumes: List[float]
) -> List[float]:
    """
    Calculate VWAP (Volume Weighted Average Price).
    VWAP = cumsum(typical_price * volume) / cumsum(volume)
    """
    if not volumes or len(volumes) != len(closes):
        return []
    
    vwaps = []
    cum_tp_vol = 0.0
    cum_vol = 0.0
    
    for i in range(len(closes)):
        typical_price = (highs[i] + lows[i] + closes[i]) / 3
        cum_tp_vol += typical_price * volumes[i]
        cum_vol += volumes[i]
        
        if cum_vol > 0:
            vwaps.append(cum_tp_vol / cum_vol)
        else:
            vwaps.append(closes[i])
    
    return vwaps


def calculate_atr(
    highs: List[float], 
    lows: List[float], 
    closes: List[float], 
    period: int = 14
) -> float:
    """Calculate ATR."""
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


def calculate_rsi(closes: List[float], period: int = 14) -> float:
    """Calculate current RSI."""
    if len(closes) < period + 1:
        return 50.0
    
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
        return 50.0
    
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    
    if avg_loss == 0:
        return 100.0
    
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


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
    
    if not dx_list:
        return 0.0
    
    return sum(dx_list[-period:]) / min(len(dx_list), period)


@register_strategy(
    name="vwap_reversion",
    version="1.0.0",
    supports_asset_classes=["CRYPTO", "FOREX"],
    description="VWAP mean reversion strategy. Flow Mode B2.",
    params_schema={
        "type": "object",
        "properties": {
            "atr_period": {"type": "integer", "default": 14},
            "min_deviation_atr": {"type": "number", "default": 1.5},
            "max_adx": {"type": "number", "default": 25},
            "rsi_oversold": {"type": "number", "default": 35},
            "rsi_overbought": {"type": "number", "default": 65},
            "min_confidence": {"type": "number", "default": 0.60},
        },
    },
)
class VWAPReversionStrategy(Strategy):
    name = "vwap_reversion"
    version = "1.0.0"
    strategy_type = "MEAN_REVERSION"
    
    def __init__(
        self,
        client,
        interval: str = "5m",
        atr_period: int = 14,
        min_deviation_atr: float = 1.5,  # Minimum deviation in ATR units
        max_adx: float = 25,  # Avoid strong trends
        rsi_oversold: float = 35,
        rsi_overbought: float = 65,
        min_confidence: float = 0.60,
    ):
        self.client = client
        self.interval = interval
        self.atr_period = atr_period
        self.min_deviation_atr = min_deviation_atr
        self.max_adx = max_adx
        self.rsi_oversold = rsi_oversold
        self.rsi_overbought = rsi_overbought
        self.min_confidence = min_confidence
    
    def get_signal(self, symbol: str) -> SignalResult:
        try:
            klines = self.client.klines(symbol=symbol, interval=self.interval, limit=200)
            
            if not klines or len(klines) < 50:
                return SignalResult(Signal.HOLD, 0.0, "insufficient_data", meta={})
            
            opens = [float(k[1]) for k in klines]
            highs = [float(k[2]) for k in klines]
            lows = [float(k[3]) for k in klines]
            closes = [float(k[4]) for k in klines]
            volumes = [float(k[5]) for k in klines]
            
            # Calculate indicators
            vwaps = calculate_vwap(highs, lows, closes, volumes)
            atr = calculate_atr(highs, lows, closes, self.atr_period)
            adx = calculate_adx(highs, lows, closes)
            rsi = calculate_rsi(closes)
            
            if not vwaps or atr <= 0:
                return SignalResult(Signal.HOLD, 0.0, "indicator_calc_failed", meta={})
            
            # Current values
            current_close = closes[-1]
            current_open = opens[-1]
            prev_close = closes[-2]
            vwap_now = vwaps[-1]
            
            # Deviation from VWAP in ATR units
            deviation = (current_close - vwap_now) / atr
            
            signal = Signal.HOLD
            confidence = 0.0
            reasons = []
            
            # Check ADX - avoid strong trends
            if adx > self.max_adx:
                return SignalResult(
                    Signal.HOLD, 0.0, "adx_too_high_for_reversion",
                    meta={"adx": adx, "max": self.max_adx}
                )
            reasons.append(f"adx_ok:{adx:.1f}")
            
            # LONG: Price below VWAP by X ATR + oversold RSI + reverting
            if deviation <= -self.min_deviation_atr:
                reasons.append(f"below_vwap:{abs(deviation):.2f}atr")
                
                if rsi <= self.rsi_oversold:
                    reasons.append(f"rsi_oversold:{rsi:.1f}")
                    
                    # Reversion trigger: bullish candle or close moving toward VWAP
                    is_bullish = current_close > current_open
                    moving_toward_vwap = current_close > prev_close
                    
                    if is_bullish and moving_toward_vwap:
                        reasons.append("bullish_reversion")
                        signal = Signal.BUY
                        
                        # Confidence based on deviation and RSI
                        dev_score = min(1.0, abs(deviation) / 3.0)
                        rsi_score = (self.rsi_oversold - rsi) / self.rsi_oversold
                        confidence = 0.60 + (0.20 * dev_score) + (0.15 * rsi_score)
            
            # SHORT: Price above VWAP by X ATR + overbought RSI + reverting
            elif deviation >= self.min_deviation_atr:
                reasons.append(f"above_vwap:{deviation:.2f}atr")
                
                if rsi >= self.rsi_overbought:
                    reasons.append(f"rsi_overbought:{rsi:.1f}")
                    
                    # Reversion trigger: bearish candle or close moving toward VWAP
                    is_bearish = current_close < current_open
                    moving_toward_vwap = current_close < prev_close
                    
                    if is_bearish and moving_toward_vwap:
                        reasons.append("bearish_reversion")
                        signal = Signal.SELL
                        
                        dev_score = min(1.0, deviation / 3.0)
                        rsi_score = (rsi - self.rsi_overbought) / (100 - self.rsi_overbought)
                        confidence = 0.60 + (0.20 * dev_score) + (0.15 * rsi_score)
            
            # Apply confidence gate
            if confidence < self.min_confidence:
                return SignalResult(
                    Signal.HOLD,
                    confidence,
                    "gated" if confidence > 0 else "no_signal",
                    meta={
                        "reasons": reasons,
                        "vwap": vwap_now,
                        "deviation_atr": deviation,
                        "rsi": rsi,
                    }
                )
            
            return SignalResult(
                signal,
                confidence,
                "vwap_reversion",
                meta={
                    "reasons": reasons,
                    "vwap": vwap_now,
                    "deviation_atr": deviation,
                    "rsi": rsi,
                    "adx": adx,
                    "strategy_type": self.strategy_type,
                }
            )
            
        except Exception as e:
            return SignalResult(Signal.HOLD, 0.0, f"error:{e}", meta={})
