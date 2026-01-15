from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict
import math

@dataclass
class SizingResult:
    qty: float
    size_usdt: float
    risk_usdt: float
    stop_distance: float
    reason: str
    details: dict

def calculate_atr(klines: List[Dict | List], period: int = 14) -> float:
    """
    Simple ATR calculation from binance klines (list of dicts or list of lists).
    Binance kline: [open_time, open, high, low, close, volume, ...]
    """
    if len(klines) < period + 1:
        return 0.0

    trs = []
    # parse klines to high/low/close floats
    
    parsed = []
    for k in klines:
        if isinstance(k, dict):
            h = float(k['high'])
            l = float(k['low'])
            c = float(k['close'])
        else:
            h = float(k[2])
            l = float(k[3])
            c = float(k[4])
        parsed.append((h, l, c))

    prev_c = parsed[0][2]
    for i in range(1, len(parsed)):
        h, l, c = parsed[i]
        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        trs.append(tr)
        prev_c = c

    if not trs:
        return 0.0

    # Exponential Moving Average for ATR
    # First value is SMA
    if len(trs) < period:
        return sum(trs) / len(trs)
        
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    
    return atr

class PositionSizer:
    def __init__(
        self, 
        account_risk_pct: float, 
        default_usdt: float,
        max_leverage: float = 20.0,
        min_notional: float = 5.0,
        max_notional: float = 10000.0,
    ):
        self.account_risk_pct = float(account_risk_pct)
        self.default_usdt = float(default_usdt)
        self.max_leverage = float(max_leverage)
        self.min_notional = float(min_notional)
        self.max_notional = float(max_notional)

    def calculate_atr_size(
        self,
        account_balance: float,
        entry_price: float,
        atr: float,
        confidence: float = 1.0,
        sl_multiplier: float = 2.0
    ) -> SizingResult:
        if atr <= 0 or entry_price <= 0:
            return SizingResult(0.0, self.default_usdt, 0.0, 0.0, "fallback_invalid_atr_or_price", {})
        
        # 1. Base Risk Amount (Equity * Risk %)
        risk_pct = self.account_risk_pct / 100.0
        
        # 2. Confidence Scaling
        # Simple Linear: 0.5 conf -> 0.5 risk.
        # But usually we have a floor. Let's assume confidence is 0.0-1.0.
        # We can implement a curve or buckets.
        # For now: risk_pct * confidence.
        risk_pct_scaled = risk_pct * max(0.1, min(1.0, confidence))
        
        risk_amt = account_balance * risk_pct_scaled
        
        # 3. Stop Distance
        stop_dist = atr * sl_multiplier
        if stop_dist <= 0:
             return SizingResult(0.0, self.default_usdt, 0.0, 0.0, "fallback_zero_stop", {})

        # 4. Quantity derived from Risk
        # Loss = qty * stop_dist
        # qty = risk_amt / stop_dist
        qty = risk_amt / stop_dist
        
        # 5. Notional Check
        notional = qty * entry_price
        
        # Constrain Max Notional
        if notional > self.max_notional:
            notional = self.max_notional
            qty = notional / entry_price
            reason = "capped_by_max_notional"
        else:
            reason = "atr_sizing"

        # Constrain Max Leverage
        max_allowed_notional = account_balance * self.max_leverage
        if notional > max_allowed_notional:
             notional = max_allowed_notional
             qty = notional / entry_price
             reason = "capped_by_max_leverage"

        # Constrain Min Notional
        if notional < self.min_notional:
            # If we can't meet min notional with this risk, we usually skip specific sizing and fallback 
            # OR we clamp up to min_notional if it's within hard risk limits?
            # Safe production: return 0.0 (Block) or skip sizing?
            # Let's return 0 qty to indicate "Risk too tight for min notional"
            return SizingResult(0.0, 0.0, risk_amt, stop_dist, "blocked_min_notional_risk_too_small", {})

        return SizingResult(
            qty=qty,
            size_usdt=notional,
            risk_usdt=risk_amt,
            stop_distance=stop_dist,
            reason=reason,
            details={
                "base_risk_pct": self.account_risk_pct,
                "confidence": confidence,
                "atr": atr,
                "sl_mult": sl_multiplier,
            }
        )
