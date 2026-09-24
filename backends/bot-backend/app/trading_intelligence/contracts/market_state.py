"""The MarketState contract (Section 9.2) and its feature-family sub-states.

Every dataclass here is frozen (immutable) and carries only market facts --
no user_id, bot_instance_id, account capital, positions, risk profile or
allocation (P3 tenant isolation). Optional numeric fields are ``None`` when
the underlying feature is unavailable/unsupported/invalid -- never a
fabricated zero (P7 fail closed).
"""
from __future__ import annotations

import dataclasses
import math
import uuid
from dataclasses import dataclass, field
from typing import Mapping, Optional, Sequence, Tuple

from app.trading_intelligence.contracts.data_quality import DataQuality, FeatureAvailability
from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.hashing import stable_hash


def safe_float(value: Optional[float]) -> Optional[float]:
    """None for anything that is not a finite float -- never a fabricated 0.0."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


# ---------------------------------------------------------------------------
# Causal candle series -- the one shared input shape every feature family reads.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CandleSeries:
    """A causally-cut OHLCV series. Index 0 is oldest, index -1 is the latest
    closed candle. Never contains a candle later than the decision boundary
    that produced it -- enforced by the adapter that builds it, not here.
    """

    open: Tuple[float, ...]
    high: Tuple[float, ...]
    low: Tuple[float, ...]
    close: Tuple[float, ...]
    volume: Tuple[float, ...]
    close_time: Tuple[int, ...]
    #: Optional aggressor/trade-count series, present only when the source
    #: candle format actually carries them (e.g. Binance klines). Absent
    #: rather than zero-filled when the source does not provide them.
    taker_buy_volume: Optional[Tuple[float, ...]] = None
    trade_count: Optional[Tuple[float, ...]] = None

    def __len__(self) -> int:
        return len(self.close)

    @property
    def latest_close_time(self) -> Optional[int]:
        return self.close_time[-1] if self.close_time else None


# ---------------------------------------------------------------------------
# Feature-family states
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class StructureState:
    swing_sequence: str  # HH_HL | LH_LL | MIXED | UNRESOLVED
    last_bos_direction: str  # UP | DOWN | NONE
    choch_direction: str  # UP | DOWN | NONE
    range_high: Optional[float]
    range_low: Optional[float]
    distance_to_support_atr: Optional[float]
    distance_to_resistance_atr: Optional[float]
    failed_break_count: int
    structure_integrity: float  # 0..1 deterministic evidence score, not probability
    invalidation_reference: Optional[float]
    pivot_confirmation_lag: int
    available: bool
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class TrendState:
    direction: str  # UP | DOWN | FLAT | UNCERTAIN
    strength: float  # 0..1 deterministic evidence strength
    age: int
    maturity: str  # EARLY | MID | LATE | UNKNOWN
    acceleration: Optional[float]
    extension_atr: Optional[float]
    retracement_depth: Optional[float]
    efficiency: Optional[float]
    diagnostics: Mapping[str, float] = field(default_factory=dict)
    available: bool = True
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class VolatilityState:
    realized_vol: Optional[float]
    percentile: Optional[float]
    atr_percentile: Optional[float]
    vol_acceleration: Optional[float]
    vol_of_vol: Optional[float]
    compression: Optional[float]
    expansion: Optional[float]
    shock_state: bool
    available: bool
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class MomentumState:
    short_return: Optional[float]
    medium_return: Optional[float]
    acceleration: Optional[float]
    efficiency: Optional[float]
    participation_divergence: Optional[float]
    exhaustion_proxy: Optional[float]
    available: bool
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class ParticipationState:
    volume_percentile: Optional[float]
    relative_volume: Optional[float]
    volume_acceleration: Optional[float]
    taker_imbalance: Optional[float]
    trade_intensity: Optional[float]
    available: bool
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class LiquidityState:
    spread_bps: Optional[float]
    spread_percentile: Optional[float]
    top_book_depth: Optional[float]
    depth_imbalance: Optional[float]
    estimated_slippage: Optional[float]
    stale_book: bool
    available: bool
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class DerivativesState:
    funding_current: Optional[float]
    funding_predicted: Optional[float]
    funding_percentile: Optional[float]
    time_to_funding: Optional[int]
    open_interest: Optional[float]
    open_interest_delta: Optional[float]
    basis: Optional[float]
    mark_index_spread: Optional[float]
    crowding_state: Optional[str]
    available: bool
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class HigherTimeframeState:
    available: bool
    direction: Optional[str] = None  # UP | DOWN | FLAT | UNCERTAIN
    structure_alignment: Optional[str] = None  # ALIGNED | CONFLICT | NEUTRAL
    volatility_state: Optional[str] = None
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class StateUncertainty:
    """Deterministic evidence uncertainty -- NOT a probability (Section 9.11)."""

    value: float  # 0..1, higher = less trustworthy
    components: Mapping[str, float] = field(default_factory=dict)
    reason_codes: Tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# MarketState
# ---------------------------------------------------------------------------
_stable_hash = stable_hash  # local alias, kept for readability in this module


def _canonical_payload(
    *,
    instrument_key: InstrumentKey,
    timeframe: str,
    decision_time: int,
    latest_closed_candle_time: int,
    snapshot_id: str,
    data_hash: str,
    data_manifest_hash: str,
    schema_version: str,
    engine_version: str,
    structure_state: StructureState,
    trend_state: TrendState,
    volatility_state: VolatilityState,
    momentum_state: MomentumState,
    participation_state: ParticipationState,
    liquidity_state: LiquidityState,
    derivatives_state: DerivativesState,
    higher_timeframe_state: HigherTimeframeState,
    data_quality: DataQuality,
    feature_availability: FeatureAvailability,
    state_uncertainty: StateUncertainty,
    reason_codes: Sequence[str],
) -> dict:
    return {
        "instrument_key": dataclasses.asdict(instrument_key),
        "timeframe": timeframe,
        "decision_time": decision_time,
        "latest_closed_candle_time": latest_closed_candle_time,
        "snapshot_id": snapshot_id,
        "data_hash": data_hash,
        "data_manifest_hash": data_manifest_hash,
        "schema_version": schema_version,
        "engine_version": engine_version,
        "structure_state": dataclasses.asdict(structure_state),
        "trend_state": dataclasses.asdict(trend_state),
        "volatility_state": dataclasses.asdict(volatility_state),
        "momentum_state": dataclasses.asdict(momentum_state),
        "participation_state": dataclasses.asdict(participation_state),
        "liquidity_state": dataclasses.asdict(liquidity_state),
        "derivatives_state": dataclasses.asdict(derivatives_state),
        "higher_timeframe_state": dataclasses.asdict(higher_timeframe_state),
        "data_quality": dataclasses.asdict(data_quality),
        "feature_availability": dataclasses.asdict(feature_availability),
        "state_uncertainty": dataclasses.asdict(state_uncertainty),
        "reason_codes": list(reason_codes),
    }


@dataclass(frozen=True)
class MarketState:
    """Immutable, versioned, deterministic market analysis for one instrument
    at one causal decision boundary. Contains no tenant identity of any kind.
    """

    market_state_id: str
    instrument_key: InstrumentKey
    timeframe: str

    decision_time: int
    latest_closed_candle_time: int

    snapshot_id: str
    data_hash: str
    data_manifest_hash: str

    schema_version: str
    engine_version: str

    structure_state: StructureState
    trend_state: TrendState
    volatility_state: VolatilityState
    momentum_state: MomentumState
    participation_state: ParticipationState
    liquidity_state: LiquidityState
    derivatives_state: DerivativesState
    higher_timeframe_state: HigherTimeframeState

    data_quality: DataQuality
    feature_availability: FeatureAvailability
    state_uncertainty: StateUncertainty
    reason_codes: Tuple[str, ...] = ()

    def canonical_payload(self) -> dict:
        return _canonical_payload(
            instrument_key=self.instrument_key,
            timeframe=self.timeframe,
            decision_time=self.decision_time,
            latest_closed_candle_time=self.latest_closed_candle_time,
            snapshot_id=self.snapshot_id,
            data_hash=self.data_hash,
            data_manifest_hash=self.data_manifest_hash,
            schema_version=self.schema_version,
            engine_version=self.engine_version,
            structure_state=self.structure_state,
            trend_state=self.trend_state,
            volatility_state=self.volatility_state,
            momentum_state=self.momentum_state,
            participation_state=self.participation_state,
            liquidity_state=self.liquidity_state,
            derivatives_state=self.derivatives_state,
            higher_timeframe_state=self.higher_timeframe_state,
            data_quality=self.data_quality,
            feature_availability=self.feature_availability,
            state_uncertainty=self.state_uncertainty,
            reason_codes=self.reason_codes,
        )

    @property
    def canonical_hash(self) -> str:
        """Deterministic hash of every analytical field (excludes market_state_id
        itself, which is derived from this same hash -- see ``build()``)."""
        return _stable_hash(self.canonical_payload())

    @property
    def is_usable(self) -> bool:
        return self.data_quality.is_usable

    @classmethod
    def build(
        cls,
        *,
        instrument_key: InstrumentKey,
        timeframe: str,
        decision_time: int,
        latest_closed_candle_time: int,
        snapshot_id: str,
        data_hash: str,
        data_manifest_hash: str,
        schema_version: str,
        engine_version: str,
        structure_state: StructureState,
        trend_state: TrendState,
        volatility_state: VolatilityState,
        momentum_state: MomentumState,
        participation_state: ParticipationState,
        liquidity_state: LiquidityState,
        derivatives_state: DerivativesState,
        higher_timeframe_state: HigherTimeframeState,
        data_quality: DataQuality,
        feature_availability: FeatureAvailability,
        state_uncertainty: StateUncertainty,
        reason_codes: Sequence[str] = (),
    ) -> "MarketState":
        payload = _canonical_payload(
            instrument_key=instrument_key,
            timeframe=timeframe,
            decision_time=decision_time,
            latest_closed_candle_time=latest_closed_candle_time,
            snapshot_id=snapshot_id,
            data_hash=data_hash,
            data_manifest_hash=data_manifest_hash,
            schema_version=schema_version,
            engine_version=engine_version,
            structure_state=structure_state,
            trend_state=trend_state,
            volatility_state=volatility_state,
            momentum_state=momentum_state,
            participation_state=participation_state,
            liquidity_state=liquidity_state,
            derivatives_state=derivatives_state,
            higher_timeframe_state=higher_timeframe_state,
            data_quality=data_quality,
            feature_availability=feature_availability,
            state_uncertainty=state_uncertainty,
            reason_codes=reason_codes,
        )
        content_hash = _stable_hash(payload)
        market_state_id = f"mkst_{content_hash[:24]}"
        return cls(
            market_state_id=market_state_id,
            instrument_key=instrument_key,
            timeframe=timeframe,
            decision_time=decision_time,
            latest_closed_candle_time=latest_closed_candle_time,
            snapshot_id=snapshot_id,
            data_hash=data_hash,
            data_manifest_hash=data_manifest_hash,
            schema_version=schema_version,
            engine_version=engine_version,
            structure_state=structure_state,
            trend_state=trend_state,
            volatility_state=volatility_state,
            momentum_state=momentum_state,
            participation_state=participation_state,
            liquidity_state=liquidity_state,
            derivatives_state=derivatives_state,
            higher_timeframe_state=higher_timeframe_state,
            data_quality=data_quality,
            feature_availability=feature_availability,
            state_uncertainty=state_uncertainty,
            reason_codes=tuple(reason_codes),
        )


def new_correlation_id(prefix: str) -> str:
    """Random correlation id for evidence/logging only -- never part of a
    canonical hash (mirrors ``MarketSnapshot.market_snapshot_id``)."""
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


__all__ = [
    "safe_float",
    "clamp01",
    "CandleSeries",
    "StructureState",
    "TrendState",
    "VolatilityState",
    "MomentumState",
    "ParticipationState",
    "LiquidityState",
    "DerivativesState",
    "HigherTimeframeState",
    "StateUncertainty",
    "MarketState",
    "new_correlation_id",
]
