"""
Multi-Timeframe Analysis System

Provides context from higher timeframes to guide lower timeframe entries.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum

from app.strategy.regime import MarketRegime, RegimeClassifier, calculate_ema_slope


class Bias(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"


@dataclass
class TimeframeAnalysis:
    """Analysis result for a single timeframe."""
    timeframe: str
    bias: Bias
    strength: float  # 0.0 - 1.0
    ema_slope: float
    momentum: float  # Positive = bullish momentum
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TimeframeContext:
    """
    Multi-timeframe context passed to strategies.
    Contains analysis from HTF, MTF, and current regime.
    """
    # Regime
    regime: MarketRegime
    regime_confidence: float
    
    # Higher Timeframe (Bias)
    htf: TimeframeAnalysis
    
    # Medium Timeframe (Confirmation)
    mtf: TimeframeAnalysis
    
    # Derived properties
    @property
    def htf_bias(self) -> Bias:
        return self.htf.bias
    
    @property
    def htf_strength(self) -> float:
        return self.htf.strength
    
    @property
    def mtf_aligned(self) -> bool:
        """Check if MTF confirms HTF direction."""
        if self.htf.bias == Bias.NEUTRAL:
            return True  # Neutral allows any direction
        return self.htf.bias == self.mtf.bias
    
    @property
    def mtf_momentum(self) -> float:
        return self.mtf.momentum
    
    def allows_long(self) -> bool:
        """Check if context allows long entries."""
        if self.regime == MarketRegime.LOW_VOLATILITY:
            return False
        if self.htf.bias == Bias.SHORT:
            return False
        return True
    
    def allows_short(self) -> bool:
        """Check if context allows short entries."""
        if self.regime == MarketRegime.LOW_VOLATILITY:
            return False
        if self.htf.bias == Bias.LONG:
            return False
        return True
    
    def compute_alignment_score(self, signal_direction: str) -> float:
        """
        Compute how well a signal aligns with the multi-TF context.
        Returns 0.0 - 1.0
        """
        score = 0.0
        
        # HTF alignment (40% weight)
        if signal_direction == "BUY" and self.htf.bias == Bias.LONG:
            score += 0.4 * self.htf.strength
        elif signal_direction == "SELL" and self.htf.bias == Bias.SHORT:
            score += 0.4 * self.htf.strength
        elif self.htf.bias == Bias.NEUTRAL:
            score += 0.2  # Partial credit for neutral
        
        # MTF confirmation (30% weight)
        if signal_direction == "BUY" and self.mtf.bias == Bias.LONG:
            score += 0.3 * self.mtf.strength
        elif signal_direction == "SELL" and self.mtf.bias == Bias.SHORT:
            score += 0.3 * self.mtf.strength
        elif self.mtf.bias == Bias.NEUTRAL:
            score += 0.15
        
        # Regime alignment (30% weight)
        if self.regime in [MarketRegime.STRONG_TREND, MarketRegime.WEAK_TREND]:
            score += 0.3 * self.regime_confidence
        elif self.regime == MarketRegime.RANGE:
            # Range is okay for mean reversion signals
            score += 0.15
        
        return min(1.0, score)


class TimeframeAnalyzer:
    """
    Analyzes multiple timeframes to build context for strategy decisions.
    """
    
    def __init__(
        self,
        client,  # BinanceFuturesClient
        regime_classifier: Optional[RegimeClassifier] = None,
    ):
        self.client = client
        self.regime_classifier = regime_classifier or RegimeClassifier()
    
    def analyze_timeframe(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 100,
    ) -> TimeframeAnalysis:
        """Analyze a single timeframe."""
        try:
            klines = self.client.klines(symbol=symbol, interval=timeframe, limit=limit)
            
            if not klines or len(klines) < 50:
                return TimeframeAnalysis(
                    timeframe=timeframe,
                    bias=Bias.NEUTRAL,
                    strength=0.0,
                    ema_slope=0.0,
                    momentum=0.0,
                    details={"error": "insufficient_data"}
                )
            
            closes = [float(k[4]) for k in klines]
            highs = [float(k[2]) for k in klines]
            lows = [float(k[3]) for k in klines]
            
            # EMA slope
            ema_slope = calculate_ema_slope(closes, period=20, lookback=5)
            
            # Simple momentum: (close - close_n_periods_ago) / close_n_periods_ago
            lookback = min(10, len(closes) - 1)
            momentum = (closes[-1] - closes[-lookback - 1]) / closes[-lookback - 1] * 100 if closes[-lookback - 1] > 0 else 0.0
            
            # Determine bias
            if ema_slope > 0.2 and momentum > 0:
                bias = Bias.LONG
                strength = min(1.0, abs(ema_slope) / 1.0)  # Normalize
            elif ema_slope < -0.2 and momentum < 0:
                bias = Bias.SHORT
                strength = min(1.0, abs(ema_slope) / 1.0)
            else:
                bias = Bias.NEUTRAL
                strength = 0.3
            
            return TimeframeAnalysis(
                timeframe=timeframe,
                bias=bias,
                strength=strength,
                ema_slope=ema_slope,
                momentum=momentum,
                details={
                    "last_close": closes[-1],
                    "candles": len(closes),
                }
            )
            
        except Exception as e:
            return TimeframeAnalysis(
                timeframe=timeframe,
                bias=Bias.NEUTRAL,
                strength=0.0,
                ema_slope=0.0,
                momentum=0.0,
                details={"error": str(e)}
            )
    
    def build_context(
        self,
        symbol: str,
        htf: str = "4h",
        mtf: str = "1h",
    ) -> TimeframeContext:
        """
        Build complete multi-timeframe context for a symbol.
        """
        # Analyze HTF for bias
        htf_analysis = self.analyze_timeframe(symbol, htf, limit=100)
        
        # Analyze MTF for confirmation
        mtf_analysis = self.analyze_timeframe(symbol, mtf, limit=100)
        
        # Get regime from HTF data
        try:
            htf_klines = self.client.klines(symbol=symbol, interval=htf, limit=100)
            highs = [float(k[2]) for k in htf_klines]
            lows = [float(k[3]) for k in htf_klines]
            closes = [float(k[4]) for k in htf_klines]
            
            regime_result = self.regime_classifier.classify(highs, lows, closes)
            regime = regime_result.regime
            regime_confidence = regime_result.confidence
        except Exception:
            regime = MarketRegime.LOW_VOLATILITY
            regime_confidence = 0.0
        
        return TimeframeContext(
            regime=regime,
            regime_confidence=regime_confidence,
            htf=htf_analysis,
            mtf=mtf_analysis,
        )


def compute_final_confidence(
    ltf_signal_confidence: float,
    tf_context: TimeframeContext,
    signal_direction: str,
) -> float:
    """
    Compute final confidence using multi-timeframe weighting.
    
    Formula:
        final = 0.4 * htf_alignment + 0.3 * mtf_confirmation + 0.3 * ltf_signal
    """
    # HTF alignment (40%)
    htf_score = 0.0
    if signal_direction == "BUY" and tf_context.htf.bias == Bias.LONG:
        htf_score = tf_context.htf.strength
    elif signal_direction == "SELL" and tf_context.htf.bias == Bias.SHORT:
        htf_score = tf_context.htf.strength
    elif tf_context.htf.bias == Bias.NEUTRAL:
        htf_score = 0.5  # Partial
    
    # MTF confirmation (30%)
    mtf_score = 0.0
    if signal_direction == "BUY" and tf_context.mtf.bias == Bias.LONG:
        mtf_score = tf_context.mtf.strength
    elif signal_direction == "SELL" and tf_context.mtf.bias == Bias.SHORT:
        mtf_score = tf_context.mtf.strength
    elif tf_context.mtf.bias == Bias.NEUTRAL:
        mtf_score = 0.5
    
    # LTF signal (30%)
    ltf_score = ltf_signal_confidence
    
    final = 0.4 * htf_score + 0.3 * mtf_score + 0.3 * ltf_score
    
    return min(1.0, max(0.0, final))
