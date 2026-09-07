"""
Trading Mode System - Full Spec (B-D)

Implements:
- Mode A: Precision (high win rate, fewer trades)
- Mode B: Flow (balanced activity)
- Mode switching with stability requirements
- Conflict resolution rules
- Symbol-level exclusivity
"""
from __future__ import annotations
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set
from datetime import datetime, timedelta

from app.strategy.regime import MarketRegime, TrendDirection


class TradingMode(Enum):
    PRECISION = "PRECISION"  # High win rate, few trades (7-9/10)
    FLOW = "FLOW"            # Balanced expectancy, more trades
    DISABLED = "DISABLED"    # No trading allowed


@dataclass
class TimeframeStack:
    """Defines the timeframe hierarchy for a trading mode."""
    htf: str   # Higher TimeFrame - Bias (e.g., "4h")
    mtf: str   # Medium TimeFrame - Confirmation (e.g., "1h")
    ltf: str   # Lower TimeFrame - Execution (e.g., "15m")


@dataclass
class ConfidenceWeights:
    """Multi-TF confidence composition weights per mode."""
    htf_weight: float
    mtf_weight: float
    ltf_weight: float
    
    def compute(self, htf_score: float, mtf_score: float, ltf_score: float) -> float:
        """Compute weighted confidence."""
        return (
            self.htf_weight * htf_score +
            self.mtf_weight * mtf_score +
            self.ltf_weight * ltf_score
        )


@dataclass
class ModeConfig:
    """Full configuration for a trading mode."""
    mode: TradingMode
    
    # Timeframes
    timeframe_stack: TimeframeStack
    
    # Confidence thresholds (C3)
    min_confidence: float
    add_confidence: float  # Higher threshold for pyramid adds
    soft_loss_confidence: float  # Raised threshold after soft loss
    
    # Confidence composition (C2)
    confidence_weights: ConfidenceWeights
    
    # Trade limits
    max_trades_per_day: int
    
    # Allowed strategy types (B1/B2)
    allowed_strategy_types: List[str]
    
    # Risk adjustments
    risk_multiplier: float = 1.0
    
    # Requirements
    require_htf_alignment: bool = True
    require_mtf_confirmation: bool = True
    
    # Health thresholds
    min_strategy_win_rate: float = 0.50
    min_profit_factor: float = 1.0
    
    # Stability (D4) - candles needed before mode switch
    stability_candles: int = 2


# =============================================================================
# DEFAULT MODE CONFIGURATIONS (C3 Thresholds)
# =============================================================================

PRECISION_MODE_CONFIG = ModeConfig(
    mode=TradingMode.PRECISION,
    timeframe_stack=TimeframeStack(htf="4h", mtf="1h", ltf="15m"),
    
    # C3: Precision thresholds
    min_confidence=0.75,
    add_confidence=0.85,
    soft_loss_confidence=0.85,
    
    # C2: Precision confidence composition
    confidence_weights=ConfidenceWeights(
        htf_weight=0.45,
        mtf_weight=0.35,
        ltf_weight=0.20,
    ),
    
    max_trades_per_day=5,
    allowed_strategy_types=["TREND", "BREAKOUT"],  # B1
    
    risk_multiplier=0.8,  # Lower risk per trade
    require_htf_alignment=True,
    require_mtf_confirmation=True,
    min_strategy_win_rate=0.60,
    min_profit_factor=1.5,
    stability_candles=2,  # D4: 2x 4H candles
)

FLOW_MODE_CONFIG = ModeConfig(
    mode=TradingMode.FLOW,
    timeframe_stack=TimeframeStack(htf="1h", mtf="15m", ltf="5m"),
    
    # C3: Flow thresholds
    min_confidence=0.60,
    add_confidence=0.70,
    soft_loss_confidence=0.70,
    
    # C2: Flow confidence composition
    confidence_weights=ConfidenceWeights(
        htf_weight=0.25,
        mtf_weight=0.20,
        ltf_weight=0.55,
    ),
    
    max_trades_per_day=15,
    allowed_strategy_types=["TREND", "MEAN_REVERSION", "BREAKOUT"],  # B2
    
    risk_multiplier=1.0,
    require_htf_alignment=True,
    require_mtf_confirmation=False,  # Less strict
    min_strategy_win_rate=0.45,
    min_profit_factor=1.0,
    stability_candles=3,  # D4: 3x 1H candles
)

DISABLED_MODE_CONFIG = ModeConfig(
    mode=TradingMode.DISABLED,
    timeframe_stack=TimeframeStack(htf="4h", mtf="1h", ltf="15m"),
    min_confidence=1.0,
    add_confidence=1.0,
    soft_loss_confidence=1.0,
    confidence_weights=ConfidenceWeights(0.33, 0.33, 0.34),
    max_trades_per_day=0,
    allowed_strategy_types=[],
    risk_multiplier=0.0,
    require_htf_alignment=True,
    require_mtf_confirmation=True,
    min_strategy_win_rate=1.0,
    min_profit_factor=999.0,
    stability_candles=1,
)


# =============================================================================
# REGIME TO MODE ROUTING (D1)
# =============================================================================

# D1: Mode priority
# Precision Mode wins when regime is STRONG_TREND or HIGH_VOLATILITY
# Flow Mode is allowed in RANGE or WEAK_TREND

REGIME_MODE_MAP: Dict[MarketRegime, TradingMode] = {
    MarketRegime.STRONG_TREND: TradingMode.PRECISION,
    MarketRegime.WEAK_TREND: TradingMode.FLOW,
    MarketRegime.RANGE: TradingMode.FLOW,
    MarketRegime.HIGH_VOLATILITY: TradingMode.PRECISION,  # Precision but very strict
    MarketRegime.LOW_VOLATILITY_CHOP: TradingMode.DISABLED,
}


# =============================================================================
# MODE ROUTER WITH CONFLICT RESOLUTION (D1-D6)
# =============================================================================

@dataclass
class SymbolModeState:
    """Tracks mode state per symbol (D2, D3)."""
    current_mode: Optional[TradingMode] = None
    has_open_position: bool = False
    position_mode: Optional[TradingMode] = None  # Mode that opened the position
    last_regime: Optional[MarketRegime] = None
    regime_stable_count: int = 0
    precision_disabled_until: Optional[datetime] = None


@dataclass
class ModeDecision:
    """Result of mode routing decision."""
    mode: TradingMode
    config: ModeConfig
    regime: MarketRegime
    trend_dir: TrendDirection
    reason: str
    is_stable: bool
    conflicts: List[str] = field(default_factory=list)
    
    @property
    def is_trading_allowed(self) -> bool:
        return self.mode != TradingMode.DISABLED and len(self.conflicts) == 0


class ModeRouter:
    """
    Routes regimes to trading modes with full conflict resolution.
    
    Implements:
    - D1: Mode priority (Precision > Flow)
    - D2: Symbol-level exclusivity
    - D3: Switching rules (no flip mid-trade)
    - D4: Regime stability requirement
    - D5: Conflict resolution
    - D6: Strategy health impacts
    """
    
    def __init__(
        self,
        precision_config: Optional[ModeConfig] = None,
        flow_config: Optional[ModeConfig] = None,
        regime_overrides: Optional[Dict[MarketRegime, TradingMode]] = None,
    ):
        self.precision_config = precision_config or PRECISION_MODE_CONFIG
        self.flow_config = flow_config or FLOW_MODE_CONFIG
        self.disabled_config = DISABLED_MODE_CONFIG
        
        self.regime_map = dict(REGIME_MODE_MAP)
        if regime_overrides:
            self.regime_map.update(regime_overrides)
        
        # D2: Symbol state tracking
        self._symbol_states: Dict[str, SymbolModeState] = {}
    
    def _get_symbol_state(self, symbol: str) -> SymbolModeState:
        if symbol not in self._symbol_states:
            self._symbol_states[symbol] = SymbolModeState()
        return self._symbol_states[symbol]
    
    def get_config(self, mode: TradingMode) -> ModeConfig:
        """Get configuration for a trading mode."""
        if mode == TradingMode.PRECISION:
            return self.precision_config
        elif mode == TradingMode.FLOW:
            return self.flow_config
        return self.disabled_config
    
    def route(
        self,
        symbol: str,
        regime: MarketRegime,
        trend_dir: TrendDirection,
        has_open_position: bool = False,
        precision_health_ok: bool = True,
        flow_health_ok: bool = True,
    ) -> ModeDecision:
        """
        Full mode routing with conflict resolution.
        """
        state = self._get_symbol_state(symbol)
        conflicts: List[str] = []
        
        # Update position state
        state.has_open_position = has_open_position
        
        # D4: Check regime stability
        is_stable = self._check_stability(state, regime)
        
        # Base mode from regime
        base_mode = self.regime_map.get(regime, TradingMode.DISABLED)
        
        # D6: Strategy health impacts
        if base_mode == TradingMode.PRECISION and not precision_health_ok:
            conflicts.append("PRECISION_HEALTH_FAILED")
            if flow_health_ok and regime not in [MarketRegime.LOW_VOLATILITY_CHOP]:
                base_mode = TradingMode.FLOW
            else:
                base_mode = TradingMode.DISABLED
        
        if base_mode == TradingMode.FLOW and not flow_health_ok:
            conflicts.append("FLOW_HEALTH_FAILED")
            base_mode = TradingMode.DISABLED
        
        # D3: Don't switch modes mid-trade
        if state.has_open_position and state.position_mode is not None:
            if base_mode != state.position_mode:
                conflicts.append("MODE_SWITCH_BLOCKED_POSITION_OPEN")
            # Keep using position mode for management
            active_mode = state.position_mode
        else:
            active_mode = base_mode
        
        # D2: Symbol exclusivity
        if state.has_open_position and state.position_mode == TradingMode.PRECISION:
            if base_mode == TradingMode.FLOW:
                conflicts.append("FLOW_BLOCKED_PRECISION_ACTIVE")
                active_mode = TradingMode.PRECISION
        
        # Update state
        state.current_mode = active_mode
        if state.has_open_position and state.position_mode is None:
            state.position_mode = active_mode
        elif not state.has_open_position:
            state.position_mode = None
        
        config = self.get_config(active_mode)
        
        reason = f"regime_{regime.value.lower()}"
        if not is_stable:
            reason += "_stabilizing"
        if conflicts:
            reason += f"_conflicts:{len(conflicts)}"
        
        return ModeDecision(
            mode=active_mode,
            config=config,
            regime=regime,
            trend_dir=trend_dir,
            reason=reason,
            is_stable=is_stable,
            conflicts=conflicts,
        )
    
    def _check_stability(self, state: SymbolModeState, regime: MarketRegime) -> bool:
        """D4: Check if regime is stable enough for mode switch."""
        if state.last_regime == regime:
            state.regime_stable_count += 1
        else:
            state.regime_stable_count = 1
            state.last_regime = regime
        
        # Get required stability from target mode config
        target_mode = self.regime_map.get(regime, TradingMode.DISABLED)
        config = self.get_config(target_mode)
        
        return state.regime_stable_count >= config.stability_candles
    
    def resolve_conflict(
        self,
        symbol: str,
        precision_signal: Optional[str],  # "BUY", "SELL", None
        precision_confidence: float,
        flow_signal: Optional[str],
        flow_confidence: float,
    ) -> tuple[Optional[str], TradingMode, float]:
        """
        D5: Conflict resolution when both modes want to trade.
        
        Returns: (signal, winning_mode, confidence)
        """
        # If Precision eligible -> Precision takes trade
        if precision_signal and precision_confidence >= self.precision_config.min_confidence:
            return precision_signal, TradingMode.PRECISION, precision_confidence
        
        # Else if Flow eligible -> Flow takes trade
        if flow_signal and flow_confidence >= self.flow_config.min_confidence:
            # Check if Flow opposes Precision bias
            if precision_signal and precision_signal != flow_signal:
                # Block Flow if it opposes Precision direction
                return None, TradingMode.DISABLED, 0.0
            
            return flow_signal, TradingMode.FLOW, flow_confidence
        
        # If both eligible same direction (rare)
        if precision_signal == flow_signal and precision_signal is not None:
            # Prefer higher confidence
            if precision_confidence >= flow_confidence:
                return precision_signal, TradingMode.PRECISION, precision_confidence
            return flow_signal, TradingMode.FLOW, flow_confidence
        
        return None, TradingMode.DISABLED, 0.0
    
    def mark_position_opened(self, symbol: str, mode: TradingMode):
        """Mark that a position was opened by a mode."""
        state = self._get_symbol_state(symbol)
        state.has_open_position = True
        state.position_mode = mode
    
    def mark_position_closed(self, symbol: str):
        """Mark that position was closed."""
        state = self._get_symbol_state(symbol)
        state.has_open_position = False
        state.position_mode = None
    
    def disable_precision_for_symbol(self, symbol: str, hours: int = 24):
        """D6: Disable Precision mode for a symbol temporarily."""
        state = self._get_symbol_state(symbol)
        state.precision_disabled_until = datetime.utcnow() + timedelta(hours=hours)
    
    def is_precision_disabled(self, symbol: str) -> bool:
        """Check if Precision is temporarily disabled for symbol."""
        state = self._get_symbol_state(symbol)
        if state.precision_disabled_until is None:
            return False
        return datetime.utcnow() < state.precision_disabled_until
