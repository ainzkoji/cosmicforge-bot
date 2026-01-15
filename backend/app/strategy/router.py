"""
Strategy Router - Full Spec Integration

Orchestrates:
- Regime classification (A)
- Mode selection with conflict resolution (D)
- Strategy filtering by type (B)
- Multi-TF confidence weighting (C2)
- Calibration gating (C4)
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Type

from app.strategy.base import Strategy, Signal, SignalResult
from app.strategy.regime import (
    MarketRegime, 
    TrendDirection,
    RegimeClassifier, 
    RegimeResult,
    RegimeThresholds,
)
from app.strategy.mode import (
    TradingMode, 
    ModeConfig, 
    ModeRouter, 
    ModeDecision,
    TimeframeStack,
    PRECISION_MODE_CONFIG,
    FLOW_MODE_CONFIG,
)


@dataclass
class RoutedSignal:
    """Signal result with full routing context."""
    # Signal
    signal: Signal
    raw_confidence: float
    final_confidence: float
    
    # Context
    strategy_name: str
    strategy_type: str
    regime: MarketRegime
    trend_dir: TrendDirection
    mode: TradingMode
    
    # Multi-TF info
    htf_bias: str
    htf_aligned: bool
    mtf_aligned: bool
    
    # Calibration
    calibration_passed: bool
    calibration_reason: str
    
    # Metadata
    reason: str
    meta: Dict[str, Any] = field(default_factory=dict)
    conflicts: List[str] = field(default_factory=list)
    
    @property
    def is_actionable(self) -> bool:
        """Check if this signal should be acted upon."""
        return (
            self.signal != Signal.HOLD and 
            self.mode != TradingMode.DISABLED and
            self.htf_aligned and
            self.calibration_passed and
            len(self.conflicts) == 0
        )


# Strategy type mapping (B1/B2)
STRATEGY_TYPES: Dict[str, str] = {
    # Precision Mode (B1)
    "supertrend": "TREND",
    "trend_pullback": "TREND",
    "donchian_breakout": "BREAKOUT",
    
    # Flow Mode (B2)
    "bollinger_reversion": "MEAN_REVERSION",
    "vwap_reversion": "MEAN_REVERSION",
    "squeeze_breakout": "BREAKOUT",
    
    # Legacy
    "robust_ensemble": "TREND",
    "sma_cross": "TREND",
}


@dataclass
class TimeframeAnalysis:
    """Analysis for a single timeframe."""
    timeframe: str
    bias: str  # "LONG", "SHORT", "NEUTRAL"
    strength: float
    adx: float
    ma_slope: float
    rsi: float
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass 
class MultiTFContext:
    """Full multi-timeframe context."""
    # Regime
    regime: MarketRegime
    trend_dir: TrendDirection
    regime_confidence: float
    
    # Timeframe analysis
    htf: TimeframeAnalysis
    mtf: TimeframeAnalysis
    
    # Derived
    @property
    def htf_bias(self) -> str:
        return self.htf.bias
    
    @property
    def mtf_aligned(self) -> bool:
        if self.htf.bias == "NEUTRAL":
            return True
        return self.htf.bias == self.mtf.bias


class StrategyRouter:
    """
    Full strategy router implementing A-D spec.
    """
    
    def __init__(
        self,
        client,
        strategies: Dict[str, Strategy],
        regime_classifier: Optional[RegimeClassifier] = None,
        mode_router: Optional[ModeRouter] = None,
        calibrator = None,  # Optional[ConfidenceCalibrator]
    ):
        self.client = client
        self.strategies = strategies
        self.regime_classifier = regime_classifier or RegimeClassifier()
        self.mode_router = mode_router or ModeRouter()
        self.calibrator = calibrator
    
    def get_strategy_type(self, strategy_name: str) -> str:
        """Get the type of a strategy."""
        return STRATEGY_TYPES.get(strategy_name, "UNKNOWN")
    
    def get_allowed_strategies(
        self,
        mode: TradingMode,
    ) -> List[str]:
        """Get list of strategy names allowed for current mode."""
        config = self.mode_router.get_config(mode)
        allowed_types = set(config.allowed_strategy_types)
        
        return [
            name for name in self.strategies.keys()
            if self.get_strategy_type(name) in allowed_types
        ]
    
    def _analyze_timeframe(
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
                    bias="NEUTRAL",
                    strength=0.0,
                    adx=0.0,
                    ma_slope=0.0,
                    rsi=50.0,
                    details={"error": "insufficient_data"}
                )
            
            highs = [float(k[2]) for k in klines]
            lows = [float(k[3]) for k in klines]
            closes = [float(k[4]) for k in klines]
            
            # Import calculations
            from app.strategy.regime import (
                calculate_adx,
                calculate_ma_slope,
                calculate_ema,
            )
            
            adx, plus_di, minus_di = calculate_adx(highs, lows, closes)
            ma_slope = calculate_ma_slope(closes, period=50, lookback=5)
            ema50 = calculate_ema(closes, 50)
            ema200 = calculate_ema(closes, 200)
            
            # Simple RSI
            from app.strategy.trend_pullback import calculate_rsi
            rsi_vals = calculate_rsi(closes, 14)
            rsi = rsi_vals[-1] if rsi_vals else 50.0
            
            # Determine bias
            bias = "NEUTRAL"
            strength = 0.3
            
            if len(ema50) > 0 and len(ema200) > 0:
                ema50_val = ema50[-1]
                ema200_val = ema200[-1]
                
                if ema50_val > ema200_val and ma_slope > 0.1 and plus_di > minus_di:
                    bias = "LONG"
                    strength = min(1.0, adx / 40.0)
                elif ema50_val < ema200_val and ma_slope < -0.1 and minus_di > plus_di:
                    bias = "SHORT"
                    strength = min(1.0, adx / 40.0)
            
            return TimeframeAnalysis(
                timeframe=timeframe,
                bias=bias,
                strength=strength,
                adx=adx,
                ma_slope=ma_slope,
                rsi=rsi,
                details={
                    "plus_di": plus_di,
                    "minus_di": minus_di,
                    "ema50": ema50[-1] if ema50 else 0,
                    "ema200": ema200[-1] if ema200 else 0,
                }
            )
            
        except Exception as e:
            return TimeframeAnalysis(
                timeframe=timeframe,
                bias="NEUTRAL",
                strength=0.0,
                adx=0.0,
                ma_slope=0.0,
                rsi=50.0,
                details={"error": str(e)}
            )
    
    def _build_context(
        self,
        symbol: str,
        htf: str = "4h",
        mtf: str = "1h",
    ) -> MultiTFContext:
        """Build complete multi-TF context."""
        # Analyze timeframes
        htf_analysis = self._analyze_timeframe(symbol, htf)
        mtf_analysis = self._analyze_timeframe(symbol, mtf)
        
        # Get regime from HTF data
        try:
            htf_klines = self.client.klines(symbol=symbol, interval=htf, limit=200)
            highs = [float(k[2]) for k in htf_klines]
            lows = [float(k[3]) for k in htf_klines]
            closes = [float(k[4]) for k in htf_klines]
            
            regime_result = self.regime_classifier.classify(highs, lows, closes)
        except Exception:
            regime_result = RegimeResult(
                regime=MarketRegime.LOW_VOLATILITY_CHOP,
                trend_dir=TrendDirection.NONE,
                regime_confidence=0.0,
                adx=0.0,
                atr_percent=0.0,
                ma_slope=0.0,
                compression_ratio=0.5,
                breakout_pressure=0.0,
                details={"error": "classification_failed"}
            )
        
        return MultiTFContext(
            regime=regime_result.regime,
            trend_dir=regime_result.trend_dir,
            regime_confidence=regime_result.regime_confidence,
            htf=htf_analysis,
            mtf=mtf_analysis,
        )
    
    def _compute_confidence(
        self,
        ltf_confidence: float,
        context: MultiTFContext,
        signal_direction: str,
        mode: TradingMode,
    ) -> float:
        """C2: Compute weighted confidence using mode-specific weights."""
        config = self.mode_router.get_config(mode)
        weights = config.confidence_weights
        
        # HTF alignment score
        htf_score = 0.0
        if signal_direction == "BUY" and context.htf.bias == "LONG":
            htf_score = context.htf.strength
        elif signal_direction == "SELL" and context.htf.bias == "SHORT":
            htf_score = context.htf.strength
        elif context.htf.bias == "NEUTRAL":
            htf_score = 0.5
        
        # MTF confirmation score
        mtf_score = 0.0
        if signal_direction == "BUY" and context.mtf.bias == "LONG":
            mtf_score = context.mtf.strength
        elif signal_direction == "SELL" and context.mtf.bias == "SHORT":
            mtf_score = context.mtf.strength
        elif context.mtf.bias == "NEUTRAL":
            mtf_score = 0.5
        
        # LTF signal strength
        ltf_score = ltf_confidence
        
        return weights.compute(htf_score, mtf_score, ltf_score)
    
    def _check_htf_alignment(self, signal_direction: str, context: MultiTFContext) -> bool:
        """Check if signal aligns with HTF bias."""
        if context.htf.bias == "NEUTRAL":
            return True
        if signal_direction == "BUY" and context.htf.bias == "LONG":
            return True
        if signal_direction == "SELL" and context.htf.bias == "SHORT":
            return True
        return False
    
    def route_signal(
        self,
        symbol: str,
        strategy_name: Optional[str] = None,
        htf: str = "4h",
        mtf: str = "1h",
        has_open_position: bool = False,
    ) -> RoutedSignal:
        """
        Get a routed signal for a symbol.
        """
        # 1. Build multi-TF context
        context = self._build_context(symbol, htf, mtf)
        
        # 2. Get mode decision
        mode_decision = self.mode_router.route(
            symbol=symbol,
            regime=context.regime,
            trend_dir=context.trend_dir,
            has_open_position=has_open_position,
        )
        
        mode = mode_decision.mode
        config = mode_decision.config
        
        # 3. Handle disabled mode
        if mode == TradingMode.DISABLED:
            return RoutedSignal(
                signal=Signal.HOLD,
                raw_confidence=0.0,
                final_confidence=0.0,
                strategy_name="none",
                strategy_type="NONE",
                regime=context.regime,
                trend_dir=context.trend_dir,
                mode=mode,
                htf_bias=context.htf_bias,
                htf_aligned=False,
                mtf_aligned=False,
                calibration_passed=False,
                calibration_reason="mode_disabled",
                reason="mode_disabled",
                conflicts=mode_decision.conflicts,
            )
        
        # 4. Get allowed strategies
        allowed = self.get_allowed_strategies(mode)
        
        if strategy_name:
            if strategy_name in allowed and strategy_name in self.strategies:
                strategies_to_run = [strategy_name]
            else:
                return RoutedSignal(
                    signal=Signal.HOLD,
                    raw_confidence=0.0,
                    final_confidence=0.0,
                    strategy_name=strategy_name,
                    strategy_type=self.get_strategy_type(strategy_name),
                    regime=context.regime,
                    trend_dir=context.trend_dir,
                    mode=mode,
                    htf_bias=context.htf_bias,
                    htf_aligned=False,
                    mtf_aligned=False,
                    calibration_passed=False,
                    calibration_reason="strategy_not_allowed",
                    reason="strategy_not_allowed_for_mode",
                    meta={"allowed": allowed},
                )
        else:
            strategies_to_run = allowed
        
        if not strategies_to_run:
            return RoutedSignal(
                signal=Signal.HOLD,
                raw_confidence=0.0,
                final_confidence=0.0,
                strategy_name="none",
                strategy_type="NONE",
                regime=context.regime,
                trend_dir=context.trend_dir,
                mode=mode,
                htf_bias=context.htf_bias,
                htf_aligned=False,
                mtf_aligned=False,
                calibration_passed=False,
                calibration_reason="no_strategies",
                reason="no_strategies_available",
            )
        
        # 5. Run strategies and find best signal
        best_signal: Optional[RoutedSignal] = None
        
        for strat_name in strategies_to_run:
            strategy = self.strategies.get(strat_name)
            if not strategy:
                continue
            
            try:
                result = strategy.get_signal(symbol)
            except Exception as e:
                continue
            
            if result.signal == Signal.HOLD:
                continue
            
            signal_direction = result.signal.value
            
            # Check HTF alignment
            htf_aligned = self._check_htf_alignment(signal_direction, context)
            
            # Skip if HTF not aligned and required
            if config.require_htf_alignment and not htf_aligned:
                continue
            
            # Compute final confidence (C2)
            final_conf = self._compute_confidence(
                result.confidence,
                context,
                signal_direction,
                mode,
            )
            
            # Apply mode confidence gate
            if final_conf < config.min_confidence:
                continue
            
            # C4: Calibration gating (if calibrator available)
            cal_passed = True
            cal_reason = "calibrator_not_configured"
            
            if self.calibrator:
                try:
                    cal_passed, cal_reason = self.calibrator.should_allow_trade(
                        strategy=strat_name,
                        symbol=symbol,
                        timeframe=config.timeframe_stack.ltf,
                        confidence=final_conf,
                        is_precision_mode=(mode == TradingMode.PRECISION),
                    )
                except Exception:
                    cal_passed = True
                    cal_reason = "calibration_error"
            
            if not cal_passed:
                continue
            
            routed = RoutedSignal(
                signal=result.signal,
                raw_confidence=result.confidence,
                final_confidence=final_conf,
                strategy_name=strat_name,
                strategy_type=self.get_strategy_type(strat_name),
                regime=context.regime,
                trend_dir=context.trend_dir,
                mode=mode,
                htf_bias=context.htf_bias,
                htf_aligned=htf_aligned,
                mtf_aligned=context.mtf_aligned,
                calibration_passed=cal_passed,
                calibration_reason=cal_reason,
                reason=result.reason,
                meta=result.meta or {},
                conflicts=mode_decision.conflicts,
            )
            
            # Keep best (highest final confidence)
            if best_signal is None or routed.final_confidence > best_signal.final_confidence:
                best_signal = routed
        
        # 6. Return best signal or HOLD
        if best_signal:
            return best_signal
        
        return RoutedSignal(
            signal=Signal.HOLD,
            raw_confidence=0.0,
            final_confidence=0.0,
            strategy_name="none",
            strategy_type="NONE",
            regime=context.regime,
            trend_dir=context.trend_dir,
            mode=mode,
            htf_bias=context.htf_bias,
            htf_aligned=False,
            mtf_aligned=context.mtf_aligned,
            calibration_passed=True,
            calibration_reason="no_signal",
            reason="no_actionable_signal",
            meta={"tried": strategies_to_run},
            conflicts=mode_decision.conflicts,
        )
    
    def get_market_context(
        self,
        symbol: str,
        htf: str = "4h",
        mtf: str = "1h",
    ) -> Dict[str, Any]:
        """Get current market context for a symbol."""
        context = self._build_context(symbol, htf, mtf)
        mode_decision = self.mode_router.route(
            symbol=symbol,
            regime=context.regime,
            trend_dir=context.trend_dir,
        )
        allowed = self.get_allowed_strategies(mode_decision.mode)
        
        return {
            "symbol": symbol,
            "regime": context.regime.value,
            "trend_direction": context.trend_dir.value,
            "regime_confidence": context.regime_confidence,
            "mode": mode_decision.mode.value,
            "mode_stable": mode_decision.is_stable,
            "conflicts": mode_decision.conflicts,
            "htf": {
                "timeframe": htf,
                "bias": context.htf.bias,
                "strength": context.htf.strength,
                "adx": context.htf.adx,
                "ma_slope": context.htf.ma_slope,
                "rsi": context.htf.rsi,
            },
            "mtf": {
                "timeframe": mtf,
                "bias": context.mtf.bias,
                "strength": context.mtf.strength,
                "adx": context.mtf.adx,
                "ma_slope": context.mtf.ma_slope,
                "rsi": context.mtf.rsi,
            },
            "allowed_strategies": allowed,
            "min_confidence": mode_decision.config.min_confidence,
            "max_trades_per_day": mode_decision.config.max_trades_per_day,
        }
