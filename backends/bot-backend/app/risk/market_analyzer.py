"""
Market Conditions Analyzer

Checks market safety before allowing trades:
- Spread analysis
- Volatility monitoring
- Volume checks  
- Price stability
"""
from __future__ import annotations

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MarketConditions:
    """Market condition metrics for a symbol."""
    symbol: str
    spread_pct: float  # Bid-ask spread as % of mid price
    volatility: float  # ATR or similar
    volume_24h: float
    price_change_24h_pct: float
    is_safe: bool
    reason: Optional[str] = None


class MarketAnalyzer:
    """
    Analyzes market conditions to determine if trading is safe.
    
    Unsafe conditions:
    - Wide spreads (low liquidity)
    - Extreme volatility (flash crash risk)
    - Low volume (manipulation risk)
    - Rapid price changes (market disruption)
    """
    
    def __init__(
        self,
        max_spread_pct: float = 0.005,  # 0.5%
        max_volatility_multiplier: float = 3.0,
        min_volume_24h: float = 1000000,  # $1M minimum
        max_price_change_24h_pct: float = 0.20  # 20%
    ):
        self.max_spread_pct = max_spread_pct
        self.max_volatility_multiplier = max_volatility_multiplier
        self.min_volume_24h = min_volume_24h
        self.max_price_change_24h_pct = max_price_change_24h_pct
        self._baseline_volatility: Dict[str, float] = {}
    
    def analyze(
        self,
        symbol: str,
        bid: float,
        ask: float,
        last_price: float,
        volume_24h: float,
        atr: float,
        price_24h_ago: Optional[float] = None
    ) -> MarketConditions:
        """
        Analyze market conditions for a symbol.
        
        Args:
            symbol: Trading symbol
            bid: Best bid price
            ask: Best ask price
            last_price: Last traded price
            volume_24h: 24-hour volume in USDT
            atr: Current ATR (Average True Range)
            price_24h_ago: Price 24 hours ago (for change calculation)
            
        Returns:
            MarketConditions with is_safe flag
        """
        # Calculate spread
        mid_price = (bid + ask) / 2
        spread = ask - bid
        spread_pct = spread / mid_price if mid_price > 0 else 0
        
        # Calculate price change
        price_change_24h_pct = 0.0
        if price_24h_ago and price_24h_ago > 0:
            price_change_24h_pct = abs(last_price - price_24h_ago) / price_24h_ago
        
        # Check baseline volatility
        if symbol not in self._baseline_volatility:
            # First time seeing this symbol, set baseline
            self._baseline_volatility[symbol] = atr
            baseline_atr = atr
        else:
            baseline_atr = self._baseline_volatility[symbol]
            # Update baseline with exponential moving average (slow adaptation)
            self._baseline_volatility[symbol] = baseline_atr * 0.95 + atr * 0.05
        
        volatility_ratio = atr / baseline_atr if baseline_atr > 0 else 1.0
        
        # Safety checks
        unsafe_reasons = []
        
        if spread_pct > self.max_spread_pct:
            unsafe_reasons.append(f"Wide spread {spread_pct:.3%}")
        
        if volatility_ratio > self.max_volatility_multiplier:
            unsafe_reasons.append(f"High volatility {volatility_ratio:.2f}x normal")
        
        if volume_24h < self.min_volume_24h:
            unsafe_reasons.append(f"Low volume ${volume_24h:,.0f}")
        
        if price_change_24h_pct > self.max_price_change_24h_pct:
            unsafe_reasons.append(f"Rapid price change {price_change_24h_pct:.1%}")
        
        is_safe = len(unsafe_reasons) == 0
        reason = "; ".join(unsafe_reasons) if unsafe_reasons else None
        
        return MarketConditions(
            symbol=symbol,
            spread_pct=spread_pct,
            volatility=atr,
            volume_24h=volume_24h,
            price_change_24h_pct=price_change_24h_pct,
            is_safe=is_safe,
            reason=reason
        )
    
    def analyze_from_exchange_data(
        self,
        symbol: str,
        ticker: Dict[str, Any],
        klines: List[Any],
        atr: float
    ) -> MarketConditions:
        """
        Convenience method to analyze from exchange API data.
        
        Args:
            symbol: Trading symbol
            ticker: 24hr ticker data (bid, ask, volume, etc.)
            klines: Recent klines for price history
            atr: Calculated ATR
            
        Returns:
            MarketConditions
        """
        bid = float(ticker.get("bidPrice", 0))
        ask = float(ticker.get("askPrice", 0))
        last_price = float(ticker.get("lastPrice", 0))
        volume_24h = float(ticker.get("quoteVolume", 0))  # Volume in USDT
        
        # Get price from 24h ago if available
        price_24h_ago = None
        if klines and len(klines) >= 24:  # Assuming hourly klines
            price_24h_ago = float(klines[-24][4])  # Close price 24h ago
        
        return self.analyze(
            symbol=symbol,
            bid=bid,
            ask=ask,
            last_price=last_price,
            volume_24h=volume_24h,
            atr=atr,
            price_24h_ago=price_24h_ago
        )
