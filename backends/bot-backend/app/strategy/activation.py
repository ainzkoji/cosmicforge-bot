"""
Strategy Activation Engine — Layer 3
=====================================
Single authority for: regime → active strategy set.

Rules:
- LOW_VOLATILITY_CHOP → empty set → caller MUST return HOLD, no execution.
- HIGH_VOLATILITY     → only breakout strategies (lower false-positive rate in spikes).
- STRONG_TREND        → trend followers at full weight; reversion strategies excluded.
- WEAK_TREND          → blended set; reversion allowed with reduced weighting.
- RANGE               → reversion strategies only; trend followers excluded.

No strategy fires outside its registered regime. This is enforced here, not in
the strategy itself — strategies remain regime-agnostic internally.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Set

from app.strategy.regime import MarketRegime, TrendDirection


# ---------------------------------------------------------------------------
# Regime → strategy name mapping (immutable)
# ---------------------------------------------------------------------------

REGIME_STRATEGY_MAP: Dict[MarketRegime, FrozenSet[str]] = {
    MarketRegime.STRONG_TREND: frozenset([
        "supertrend",
        "trend_pullback",
        "donchian_breakout",
        "sma_cross",
    ]),
    MarketRegime.WEAK_TREND: frozenset([
        "supertrend",
        "trend_pullback",
        "vwap_reversion",
        "squeeze_breakout",
    ]),
    MarketRegime.RANGE: frozenset([
        "vwap_reversion",
        "bollinger_reversion",
        "donchian_breakout",
    ]),
    MarketRegime.HIGH_VOLATILITY: frozenset([
        "squeeze_breakout",
        "donchian_breakout",
    ]),
    # Capital preservation mode — zero strategies, HOLD is mandatory
    MarketRegime.LOW_VOLATILITY_CHOP: frozenset(),
}

# Strategies that change behaviour based on trend direction
# (e.g., trend_pullback should only go long in UP, short in DOWN)
DIRECTION_AWARE_STRATEGIES: FrozenSet[str] = frozenset([
    "trend_pullback",
    "supertrend",
    "vwap_reversion",
])


@dataclass(frozen=True)
class ActivationResult:
    """Output of ActivationEngine.get_active()"""
    regime: MarketRegime
    trend_dir: TrendDirection
    active_strategies: FrozenSet[str]
    blocked_reason: Optional[str]  # non-None only when active_strategies is empty
    signal_bias: Optional[str]     # "BUY_ONLY" | "SELL_ONLY" | None

    @property
    def is_blocked(self) -> bool:
        return len(self.active_strategies) == 0

    def allows(self, strategy_name: str) -> bool:
        return strategy_name in self.active_strategies


class ActivationEngine:
    """
    Stateless engine: each call returns an ActivationResult.
    Instances may be shared across threads — no mutable state.
    """

    def __init__(
        self,
        regime_map: Optional[Dict[MarketRegime, FrozenSet[str]]] = None,
        override_enabled: bool = False,
        forced_regime: Optional[MarketRegime] = None,
    ):
        """
        Args:
            regime_map:       Override the default REGIME_STRATEGY_MAP (for testing).
            override_enabled: If True, use forced_regime instead of the live regime.
            forced_regime:    Only used when override_enabled=True.
        """
        self._map = regime_map or REGIME_STRATEGY_MAP
        self._override_enabled = override_enabled
        self._forced_regime = forced_regime

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_active(
        self,
        regime: MarketRegime,
        trend_dir: TrendDirection = TrendDirection.NONE,
    ) -> ActivationResult:
        """
        Return the set of strategies cleared to run for this regime.

        If regime is LOW_VOLATILITY_CHOP:
            → active_strategies = frozenset()
            → is_blocked = True
            → caller MUST short-circuit to HOLD signal

        For directional regimes with clear trend_dir:
            → signal_bias is set to constrain strategy vote direction
        """
        effective_regime = self._forced_regime if self._override_enabled else regime
        active = self._map.get(effective_regime, frozenset())

        blocked_reason: Optional[str] = None
        if len(active) == 0:
            blocked_reason = f"regime={effective_regime.value} — capital preservation active"

        signal_bias = self._derive_signal_bias(effective_regime, trend_dir)

        return ActivationResult(
            regime=effective_regime,
            trend_dir=trend_dir,
            active_strategies=active,
            blocked_reason=blocked_reason,
            signal_bias=signal_bias,
        )

    def filter_votes(
        self,
        votes: List[tuple],  # [(strategy_name, signal_str, confidence), ...]
        activation: ActivationResult,
    ) -> List[tuple]:
        """
        Strip any votes from strategies not in the active set,
        and optionally filter by signal_bias.

        Args:
            votes: Raw signal list from strategy pool.
            activation: Result from get_active().

        Returns:
            Filtered list — may be empty if no strategies cleared.
        """
        filtered = [
            (name, sig, conf)
            for name, sig, conf in votes
            if activation.allows(name)
        ]

        # Apply directional bias: if trend is clear, only accept aligned votes
        if activation.signal_bias == "BUY_ONLY":
            filtered = [(n, s, c) for n, s, c in filtered if s.upper() != "SELL"]
        elif activation.signal_bias == "SELL_ONLY":
            filtered = [(n, s, c) for n, s, c in filtered if s.upper() != "BUY"]

        return filtered

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _derive_signal_bias(
        self,
        regime: MarketRegime,
        trend_dir: TrendDirection,
    ) -> Optional[str]:
        """
        In STRONG_TREND with clear direction, constrain votes to the trend.
        Prevents counter-trend signals from entering the ensemble vote.
        """
        if regime != MarketRegime.STRONG_TREND:
            return None
        if trend_dir == TrendDirection.UP:
            return "BUY_ONLY"
        if trend_dir == TrendDirection.DOWN:
            return "SELL_ONLY"
        return None


# ---------------------------------------------------------------------------
# Module-level singleton (shared across all ensemble instances)
# ---------------------------------------------------------------------------

_activation_engine: Optional[ActivationEngine] = None


def get_activation_engine() -> ActivationEngine:
    """Return the module-level singleton ActivationEngine."""
    global _activation_engine
    if _activation_engine is None:
        _activation_engine = ActivationEngine()
    return _activation_engine


def reset_activation_engine() -> None:
    """Reset the singleton (for testing)."""
    global _activation_engine
    _activation_engine = None
