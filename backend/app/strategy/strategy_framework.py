"""
Strategy Family Framework

Base classes and protocol for strategy families.
Every strategy MUST output: signal, confidence, suggested_stop, riskiness.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class Signal(Enum):
    """Trading signal types."""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


class StrategyFamily(Enum):
    """Strategy family categories."""
    TREND_FOLLOWING = "trend_following"
    MEAN_REVERSION = "mean_reversion"
    MOMENTUM = "momentum"
    VOLATILITY_BREAKOUT = "volatility_breakout"
    GRID_DCA = "grid_dca"  # Dangerous - requires strict caps
    SCALPING = "scalping"  # Dangerous - requires low latency
    PORTFOLIO_REBALANCE = "portfolio_rebalance"  # Future


@dataclass
class StrategyOutput:
    """
    Standard output from every strategy.
    
    All strategies MUST return this structure.
    """
    # Core outputs (REQUIRED)
    signal: Signal  # buy, sell, or hold
    confidence: float  # 0.0 to 1.0 (0 = no confidence, 1 = maximum confidence)
    suggested_stop_distance: float  # Stop distance as % (e.g., 0.02 = 2%)
    
    # Optional outputs
    riskiness: Optional[float] = None  # 0.0 to 1.0 (how risky is this signal)
    take_profit_distance: Optional[float] = None  # TP distance as %
    
    # Metadata
    reason: Optional[str] = None  # Why this signal was generated
    indicators: Optional[Dict[str, float]] = None  # Indicator values used
    
    def __post_init__(self):
        # Validate confidence is 0-1
        if not 0 <= self.confidence <= 1:
            raise ValueError(f"Confidence must be 0-1, got {self.confidence}")
        
        # Validate riskiness if provided
        if self.riskiness is not None and not 0 <= self.riskiness <= 1:
            raise ValueError(f"Riskiness must be 0-1, got {self.riskiness}")
        
        # Validate stop distance is positive
        if self.suggested_stop_distance <= 0:
            raise ValueError(f"Stop distance must be positive, got {self.suggested_stop_distance}")


@dataclass
class StrategyParameters:
    """Base parameters that all strategies can use."""
    # Common parameters
    lookback_period: int = 20
    confidence_threshold: float = 0.3  # Minimum confidence to trade
    
    # Can be extended by specific strategy families
    custom_params: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.custom_params is None:
            self.custom_params = {}


class BaseStrategy(ABC):
    """
    Base class for all strategy families.
    
    Every strategy implementation MUST inherit from this and implement analyze().
    """
    
    def __init__(
        self,
        strategy_id: str,
        family: StrategyFamily,
        name: str,
        description: str,
        parameters: StrategyParameters = None
    ):
        self.strategy_id = strategy_id
        self.family = family
        self.name = name
        self.description = description
        self.parameters = parameters or StrategyParameters()
    
    @abstractmethod
    def analyze(
        self,
        symbol: str,
        klines: List[Dict[str, Any]],
        current_price: float,
        **kwargs
    ) -> StrategyOutput:
        """
        Analyze market data and generate trading signal.
        """
        pass
    
    def get_parameter_schema(self) -> Dict[str, Dict[str, Any]]:
        """
        Return schema for strategy parameters.
        
        Format:
        {
            "param_name": {
                "type": "int|float|bool|str",
                "default": value,
                "min": min_val,
                "max": max_val,
                "description": "text",
                "locked": bool 
            }
        }
        """
        return {
            "lookback_period": {
                "type": "int",
                "default": 20,
                "min": 5,
                "max": 200,
                "description": "Base lookback period for analysis",
                "locked": False
            },
            "confidence_threshold": {
                "type": "float",
                "default": 0.3,
                "min": 0.1,
                "max": 0.9,
                "description": "Minimum confidence required to trade",
                "locked": False
            }
        }

    def validate_output(self, output: StrategyOutput) -> bool:
        """
        Validate strategy output meets requirements.
        """
        try:
            # Check required fields exist
            if output.signal not in Signal:
                logger.error(f"Invalid signal: {output.signal}")
                return False
            
            if not 0 <= output.confidence <= 1:
                logger.error(f"Invalid confidence: {output.confidence}")
                return False
            
            if output.suggested_stop_distance <= 0:
                logger.error(f"Invalid stop distance: {output.suggested_stop_distance}")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Output validation failed: {e}")
            return False


# =============================================================================
# STRATEGY FAMILY IMPLEMENTATIONS
# =============================================================================

# -----------------------------------------------------------------------------
# 1. TREND FOLLOWING
# -----------------------------------------------------------------------------

@dataclass
class TrendFollowingParams(StrategyParameters):
    """Parameters for trend-following strategies."""
    fast_ma_period: int = 10
    slow_ma_period: int = 50
    atr_period: int = 14
    atr_stop_multiplier: float = 2.0


class MovingAverageCross(BaseStrategy):
    """
    MA Cross / EMA Trend strategy.
    
    Generates BUY when fast MA crosses above slow MA.
    Generates SELL when fast MA crosses below slow MA.
    """
    
    def __init__(self, parameters: TrendFollowingParams = None):
        super().__init__(
            strategy_id="ma_cross_v1",
            family=StrategyFamily.TREND_FOLLOWING,
            name="Moving Average Crossover",
            description="Classic MA cross with ATR-based stops",
            parameters=parameters or TrendFollowingParams()
        )
        
    def get_parameter_schema(self) -> Dict[str, Dict[str, Any]]:
        """Return schema for MA Cross."""
        schema = super().get_parameter_schema()
        schema.update({
            "fast_ma_period": {
                "type": "int",
                "default": 10,
                "min": 2,
                "max": 50,
                "description": "Fast moving average period",
                "locked": False
            },
            "slow_ma_period": {
                "type": "int",
                "default": 50,
                "min": 10,
                "max": 200,
                "description": "Slow moving average period",
                "locked": False
            },
            "atr_period": {
                "type": "int",
                "default": 14,
                "min": 5,
                "max": 30,
                "description": "ATR period for volatility calculation",
                "locked": True  # User shouldn't tamper with risk base
            },
            "atr_stop_multiplier": {
                "type": "float",
                "default": 2.0,
                "min": 1.0,
                "max": 5.0,
                "description": "Multiplier for ATR-based stop loss",
                "locked": False
            }
        })
        return schema
    
    def analyze(
        self,
        symbol: str,
        klines: List[Dict[str, Any]],
        current_price: float,
        **kwargs
    ) -> StrategyOutput:
        """Analyze using MA crossover logic."""
        params: TrendFollowingParams = self.parameters
        
        # Calculate moving averages
        closes = [float(k['close']) for k in klines]
        fast_ma = self._calculate_ma(closes, params.fast_ma_period)
        slow_ma = self._calculate_ma(closes, params.slow_ma_period)
        
        # Calculate ATR for stop distance
        atr = self._calculate_atr(klines, params.atr_period)
        suggested_stop = (atr * params.atr_stop_multiplier) / current_price
        
        # Detect crossover
        prev_fast = self._calculate_ma(closes[:-1], params.fast_ma_period)
        prev_slow = self._calculate_ma(closes[:-1], params.slow_ma_period)
        
        # Determine signal
        signal = Signal.HOLD
        confidence = 0.0
        reason = "No clear trend"
        
        if fast_ma > slow_ma and prev_fast <= prev_slow:
            # Bullish crossover
            signal = Signal.BUY
            # Confidence based on distance between MAs
            ma_distance = (fast_ma - slow_ma) / slow_ma
            confidence = min(0.9, 0.5 + ma_distance * 10)
            reason = f"Fast MA crossed above slow MA (distance: {ma_distance:.2%})"
        
        elif fast_ma < slow_ma and prev_fast >= prev_slow:
            # Bearish crossover
            signal = Signal.SELL
            ma_distance = (slow_ma - fast_ma) / fast_ma
            confidence = min(0.9, 0.5 + ma_distance * 10)
            reason = f"Fast MA crossed below slow MA (distance: {ma_distance:.2%})"
        
        # Calculate riskiness (higher when trend is weak)
        trend_strength = abs(fast_ma - slow_ma) / slow_ma
        riskiness = max(0.1, 1.0 - trend_strength * 5)  # Weak trend = higher risk
        
        return StrategyOutput(
            signal=signal,
            confidence=confidence,
            suggested_stop_distance=suggested_stop,
            riskiness=riskiness,
            take_profit_distance=suggested_stop * 2,  # 2:1 TP:SL ratio
            reason=reason,
            indicators={
                "fast_ma": fast_ma,
                "slow_ma": slow_ma,
                "atr": atr,
                "trend_strength": trend_strength
            }
        )
    
    def _calculate_ma(self, prices: List[float], period: int) -> float:
        """Calculate simple moving average."""
        if len(prices) < period:
            return prices[-1] if prices else 0.0
        return sum(prices[-period:]) / period
    
    def _calculate_atr(self, klines: List[Dict[str, Any]], period: int) -> float:
        """Calculate Average True Range."""
        if len(klines) < period + 1:
            return 0.0
        
        true_ranges = []
        for i in range(1, len(klines)):
            high = float(klines[i]['high'])
            low = float(klines[i]['low'])
            prev_close = float(klines[i-1]['close'])
            
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            true_ranges.append(tr)
        
        return sum(true_ranges[-period:]) / period


# -----------------------------------------------------------------------------
# 2. MEAN REVERSION
# -----------------------------------------------------------------------------

@dataclass
class MeanReversionParams(StrategyParameters):
    """Parameters for mean reversion strategies."""
    bb_period: int = 20
    bb_std_multiplier: float = 2.0
    rsi_period: int = 14
    rsi_oversold: float = 30
    rsi_overbought: float = 70


class BollingerReversion(BaseStrategy):
    """
    Bollinger Band Mean Reversion strategy.
    
    BUY when price touches lower band and RSI oversold.
    SELL when price touches upper band and RSI overbought.
    """
    
    def __init__(self, parameters: MeanReversionParams = None):
        super().__init__(
            strategy_id="bb_reversion_v1",
            family=StrategyFamily.MEAN_REVERSION,
            name="Bollinger Reversion",
            description="Mean reversion using Bollinger Bands + RSI",
            parameters=parameters or MeanReversionParams()
        )

    def get_parameter_schema(self) -> Dict[str, Dict[str, Any]]:
        """Return schema for Bollinger Reversion."""
        schema = super().get_parameter_schema()
        schema.update({
            "bb_period": {
                "type": "int",
                "default": 20,
                "min": 10,
                "max": 50,
                "description": "Bollinger Band period",
                "locked": False
            },
            "bb_std_multiplier": {
                "type": "float",
                "default": 2.0,
                "min": 1.0,
                "max": 3.0,
                "description": "Standard deviation multiplier",
                "locked": False
            },
            "rsi_period": {
                "type": "int",
                "default": 14,
                "min": 5,
                "max": 25,
                "description": "RSI period",
                "locked": False
            },
            "rsi_oversold": {
                "type": "float",
                "default": 30.0,
                "min": 10.0,
                "max": 45.0,
                "description": "RSI oversold threshold",
                "locked": False
            },
            "rsi_overbought": {
                "type": "float",
                "default": 70.0,
                "min": 55.0,
                "max": 90.0,
                "description": "RSI overbought threshold",
                "locked": False
            }
        })
        return schema

    
    def analyze(
        self,
        symbol: str,
        klines: List[Dict[str, Any]],
        current_price: float,
        **kwargs
    ) -> StrategyOutput:
        """Analyze using Bollinger Band reversion logic."""
        params: MeanReversionParams = self.parameters
        
        # Calculate Bollinger Bands
        closes = [float(k['close']) for k in klines]
        bb_middle, bb_upper, bb_lower = self._calculate_bollinger_bands(
            closes, params.bb_period, params.bb_std_multiplier
        )
        
        # Calculate RSI
        rsi = self._calculate_rsi(closes, params.rsi_period)
        
        # Calculate stop distance (distance to middle band)
        suggested_stop = abs(current_price - bb_middle) / current_price
        
        # Determine signal
        signal = Signal.HOLD
        confidence = 0.0
        reason = "Waiting for reversion setup"
        
        # BUY signal: Price near lower band + RSI oversold
        if current_price <= bb_lower * 1.01 and rsi < params.rsi_oversold:
            signal = Signal.BUY
            # Confidence based on how oversold
            confidence = min(0.9, 0.5 + (params.rsi_oversold - rsi) / 100)
            reason = f"Price at lower BB ({current_price:.2f} vs {bb_lower:.2f}), RSI oversold ({rsi:.0f})"
        
        # SELL signal: Price near upper band + RSI overbought
        elif current_price >= bb_upper * 0.99 and rsi > params.rsi_overbought:
            signal = Signal.SELL
            confidence = min(0.9, 0.5 + (rsi - params.rsi_overbought) / 100)
            reason = f"Price at upper BB ({current_price:.2f} vs {bb_upper:.2f}), RSI overbought ({rsi:.0f})"
        
        # Calculate riskiness (higher in trending markets)
        bb_width = (bb_upper - bb_lower) / bb_middle
        riskiness = min(0.9, bb_width * 5)  # Wide bands = trending = riskier for reversion
        
        return StrategyOutput(
            signal=signal,
            confidence=confidence,
            suggested_stop_distance=max(suggested_stop, 0.01),  # Min 1% stop
            riskiness=riskiness,
            take_profit_distance=suggested_stop * 1.5,  # 1.5:1 TP:SL for reversion
            reason=reason,
            indicators={
                "bb_upper": bb_upper,
                "bb_middle": bb_middle,
                "bb_lower": bb_lower,
                "rsi": rsi,
                "bb_width": bb_width
            }
        )
    
    def _calculate_bollinger_bands(
        self, prices: List[float], period: int, std_mult: float
    ) -> tuple[float, float, float]:
        """Calculate Bollinger Bands."""
        if len(prices) < period:
            return prices[-1], prices[-1], prices[-1]
        
        recent = prices[-period:]
        middle = sum(recent) / period
        
        variance = sum((p - middle) ** 2 for p in recent) / period
        std_dev = variance ** 0.5
        
        upper = middle + (std_dev * std_mult)
        lower = middle - (std_dev * std_mult)
        
        return middle, upper, lower
    
    def _calculate_rsi(self, prices: List[float], period: int) -> float:
        """Calculate Relative Strength Index."""
        if len(prices) < period + 1:
            return 50.0  # Neutral
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi


# =============================================================================
# STRATEGY REGISTRY
# =============================================================================

class StrategyRegistry:
    """Registry of available strategies."""
    
    _strategies: Dict[str, BaseStrategy] = {}
    
    @classmethod
    def register(cls, strategy: BaseStrategy):
        """Register a strategy."""
        cls._strategies[strategy.strategy_id] = strategy
    
    @classmethod
    def get(cls, strategy_id: str) -> Optional[BaseStrategy]:
        """Get a strategy by ID."""
        return cls._strategies.get(strategy_id)
    
    @classmethod
    def list_by_family(cls, family: StrategyFamily) -> List[BaseStrategy]:
        """List all strategies in a family."""
        return [s for s in cls._strategies.values() if s.family == family]
    
    @classmethod
    def list_all(cls) -> List[BaseStrategy]:
        """List all registered strategies."""
        return list(cls._strategies.values())


# Auto-register available strategies
StrategyRegistry.register(MovingAverageCross())
StrategyRegistry.register(BollingerReversion())
