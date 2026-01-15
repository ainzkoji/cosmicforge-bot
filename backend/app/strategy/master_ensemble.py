"""
MasterEnsembleStrategy - Combines ALL 7 strategies with weighted voting.

This ensemble runs every strategy, collects their signals and confidences,
then uses weighted voting to produce a final high-confidence signal.
"""
from __future__ import annotations

from typing import Dict, List, Tuple
from app.strategy.base import Strategy, Signal, SignalResult
from app.strategy.registry import register_strategy

# Import all strategy classes
from app.strategy.supertrend import SuperTrendStrategy
from app.strategy.vwap_reversion import VWAPReversionStrategy
from app.strategy.trend_pullback import TrendPullbackStrategy
from app.strategy.squeeze_breakout import SqueezeBreakoutStrategy
from app.strategy.sma_cross import SMACrossStrategy
from app.strategy.donchian_breakout import DonchianBreakoutStrategy
from app.strategy.bollinger_reversion import BollingerReversionStrategy


# Strategy weights - higher = more influence on final signal
STRATEGY_WEIGHTS = {
    "supertrend": 1.5,           # Strong trend detection
    "trend_pullback": 1.3,       # Reliable pullback entries
    "vwap_reversion": 1.2,       # Good mean reversion
    "squeeze_breakout": 1.1,     # Volatility breakouts
    "bollinger_reversion": 1.0,  # Overbought/oversold reversals
    "donchian_breakout": 1.0,    # Channel breakouts
    "sma_cross": 0.9,            # Slower, lagging signals
}


@register_strategy(
    name="master_ensemble",
    version="1.0.0",
    supports_asset_classes=["CRYPTO", "FOREX"],
    description="Master ensemble combining ALL 7 strategies with weighted voting.",
    params_schema={
        "type": "object",
        "properties": {
            "min_confidence": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0,
                "default": 0.55,
            },
            "consensus_threshold": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0,
                "default": 0.60,  # 60% weighted votes needed
            },
            "interval": {"type": "string", "default": "15m"},
        },
    },
)
class MasterEnsembleStrategy(Strategy):
    """
    Master Ensemble Strategy
    
    Combines all 7 strategies using weighted voting:
    1. Runs all strategies on each symbol
    2. Collects BUY/SELL/HOLD signals with confidence
    3. Calculates weighted vote scores
    4. Requires consensus threshold to signal
    5. Returns HOLD if insufficient consensus
    """
    name = "master_ensemble"
    version = "1.0.0"

    def __init__(
        self, 
        client, 
        interval: str = "15m", 
        min_confidence: float = 0.55,
        consensus_threshold: float = 0.60,
    ):
        self.client = client
        self.interval = interval
        self.min_confidence = float(min_confidence)
        self.consensus_threshold = float(consensus_threshold)
        
        # Initialize all sub-strategies
        self.strategies: Dict[str, Strategy] = {}
        self._init_strategies()
    
    def _init_strategies(self):
        """Initialize all sub-strategies with proper error handling."""
        strategy_configs = [
            ("supertrend", SuperTrendStrategy),
            ("vwap_reversion", VWAPReversionStrategy),
            ("trend_pullback", TrendPullbackStrategy),
            ("squeeze_breakout", SqueezeBreakoutStrategy),
            ("sma_cross", SMACrossStrategy),
            ("donchian_breakout", DonchianBreakoutStrategy),
            ("bollinger_reversion", BollingerReversionStrategy),
        ]
        
        for name, strategy_class in strategy_configs:
            try:
                self.strategies[name] = strategy_class(
                    client=self.client, 
                    interval=self.interval
                )
            except Exception as e:
                # Log but don't fail - we can work with fewer strategies
                print(f"[MasterEnsemble] Failed to init {name}: {e}")
    
    def get_signal(self, symbol: str) -> SignalResult:
        """
        Get signal by running all strategies and voting.
        """
        votes: List[Tuple[str, Signal, float]] = []  # (strategy_name, signal, confidence)
        errors = []
        
        # Collect signals from all strategies
        for name, strategy in self.strategies.items():
            try:
                result = strategy.get_signal(symbol)
                signal = result.signal if hasattr(result, 'signal') else Signal.HOLD
                confidence = float(result.confidence) if hasattr(result, 'confidence') else 0.0
                
                # Normalize signal to enum
                if isinstance(signal, str):
                    signal = Signal[signal.upper()] if signal.upper() in Signal.__members__ else Signal.HOLD
                
                votes.append((name, signal, confidence))
            except Exception as e:
                errors.append(f"{name}:{type(e).__name__}")
        
        if not votes:
            return SignalResult(
                Signal.HOLD, 0.0, "no_valid_strategies",
                meta={"errors": errors}
            )
        
        # Calculate weighted votes
        buy_score = 0.0
        sell_score = 0.0
        total_weight = 0.0
        
        vote_details = []
        
        for name, signal, confidence in votes:
            weight = STRATEGY_WEIGHTS.get(name, 1.0)
            weighted_vote = weight * confidence
            total_weight += weight
            
            if signal == Signal.BUY:
                buy_score += weighted_vote
                vote_details.append(f"{name}:BUY({confidence:.2f})")
            elif signal == Signal.SELL:
                sell_score += weighted_vote
                vote_details.append(f"{name}:SELL({confidence:.2f})")
            else:
                vote_details.append(f"{name}:HOLD({confidence:.2f})")
        
        # Normalize scores
        if total_weight > 0:
            buy_pct = buy_score / total_weight
            sell_pct = sell_score / total_weight
        else:
            buy_pct = sell_pct = 0.0
        
        # Determine final signal with consensus check
        final_signal = Signal.HOLD
        final_confidence = 0.0
        
        if buy_pct > sell_pct and buy_pct >= self.consensus_threshold:
            final_signal = Signal.BUY
            final_confidence = buy_pct
        elif sell_pct > buy_pct and sell_pct >= self.consensus_threshold:
            final_signal = Signal.SELL
            final_confidence = sell_pct
        else:
            # No consensus - check if there's a strong lean
            if buy_pct > 0.4 and buy_pct > sell_pct * 1.5:
                # Weak buy signal
                final_signal = Signal.BUY
                final_confidence = buy_pct * 0.7  # Reduce confidence for weak consensus
            elif sell_pct > 0.4 and sell_pct > buy_pct * 1.5:
                # Weak sell signal
                final_signal = Signal.SELL
                final_confidence = sell_pct * 0.7
        
        # Final confidence gating
        if final_confidence < self.min_confidence:
            return SignalResult(
                Signal.HOLD, 
                final_confidence, 
                "gated_low_confidence",
                meta={
                    "buy_score": round(buy_pct, 3),
                    "sell_score": round(sell_pct, 3),
                    "votes": vote_details,
                    "strategies_used": len(votes),
                }
            )
        
        return SignalResult(
            final_signal,
            float(final_confidence),
            "master_ensemble",
            meta={
                "buy_score": round(buy_pct, 3),
                "sell_score": round(sell_pct, 3),
                "votes": vote_details,
                "strategies_used": len(votes),
                "consensus_met": True,
            }
        )
