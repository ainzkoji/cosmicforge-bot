"""SetupCandidate contract (Section 11.2) -- a causal, market-only trading
hypothesis. Shareable market intelligence so long as no account data is
introduced (P3).

A SetupCandidate never approves a trade, sizes a position, or submits an
order. ``evidence_score`` is diagnostic only -- explicitly NOT a probability
of winning, a calibrated confidence, an economic admission verdict, or a
final trade score (Section 11.2). Section 12/13 are what turn evidence into
an economic verdict.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Optional, Tuple

from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.hashing import short_id
from app.trading_intelligence.versions import SETUP_CANDIDATE_SCHEMA_VERSION


class SetupSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


#: Stable CATI setup reason-code vocabulary (Section 11.2.2). One vocabulary,
#: extended here only -- never scattered as ad-hoc strings through specialist
#: files.
class SetupReasonCode(str, Enum):
    SETUP_FOUND = "SETUP_FOUND"
    REGIME_NOT_ELIGIBLE = "REGIME_NOT_ELIGIBLE"
    STRUCTURE_NOT_ALIGNED = "STRUCTURE_NOT_ALIGNED"
    HTF_CONFLICT = "HTF_CONFLICT"
    TREND_TOO_MATURE = "TREND_TOO_MATURE"
    TREND_TOO_EXTENDED = "TREND_TOO_EXTENDED"
    INVALID_STRUCTURAL_RISK = "INVALID_STRUCTURAL_RISK"
    INSUFFICIENT_TARGET_ROOM = "INSUFFICIENT_TARGET_ROOM"
    PULLBACK_STRUCTURALLY_BROKEN = "PULLBACK_STRUCTURALLY_BROKEN"
    WEAK_PARTICIPATION = "WEAK_PARTICIPATION"
    BREAKOUT_TOO_EXTENDED = "BREAKOUT_TOO_EXTENDED"
    NEAR_HTF_BARRIER = "NEAR_HTF_BARRIER"
    RANGE_TRANSITION_RISK = "RANGE_TRANSITION_RISK"
    RANGE_TOO_NARROW = "RANGE_TOO_NARROW"
    BOUNDARY_INTEGRITY_WEAK = "BOUNDARY_INTEGRITY_WEAK"
    MOMENTUM_EXHAUSTION = "MOMENTUM_EXHAUSTION"
    CROWDING_EXTREME = "CROWDING_EXTREME"
    LIQUIDITY_UNVERIFIED = "LIQUIDITY_UNVERIFIED"
    REQUIRED_CAPABILITY_MISSING = "REQUIRED_CAPABILITY_MISSING"
    #: Additional codes needed by specialist implementations, kept in this
    #: same single vocabulary rather than invented ad hoc per file.
    SETUP_INSUFFICIENT_HISTORY = "SETUP_INSUFFICIENT_HISTORY"
    NO_DIRECTIONAL_TREND = "NO_DIRECTIONAL_TREND"
    REGIME_DISTRIBUTION_INVALID = "REGIME_DISTRIBUTION_INVALID"
    MARKET_STATE_INVALID = "MARKET_STATE_INVALID"
    NO_VALIDATED_RANGE = "NO_VALIDATED_RANGE"
    VOL_EXPANSION_ACTIVE = "VOL_EXPANSION_ACTIVE"
    STRUCTURAL_TRANSITION_ACTIVE = "STRUCTURAL_TRANSITION_ACTIVE"
    NO_CONFIRMED_BREAK = "NO_CONFIRMED_BREAK"
    COMPRESSION_NOT_ESTABLISHED = "COMPRESSION_NOT_ESTABLISHED"


_VALID_SIDES = frozenset(s.value for s in SetupSide)

#: Reused from app.research.dataset.DERIVABLE (timeframe -> minutes) so CATI
#: does not maintain a second timeframe-duration table.
_MINUTE_MS = 60_000


def timeframe_to_ms(timeframe: str) -> Optional[int]:
    """Bar duration in ms for a known timeframe string, else None (never a
    fabricated default -- callers must handle the None case explicitly)."""
    from app.research.dataset import DERIVABLE

    if timeframe == "1m":
        return _MINUTE_MS
    minutes = DERIVABLE.get(timeframe)
    return minutes * _MINUTE_MS if minutes else None


@dataclass(frozen=True)
class SetupGeometry:
    """Pure geometry (Section 11.2.1) -- no admission judgement."""

    risk_distance: float
    room_to_target_R: Optional[float]


def compute_geometry(
    *, side: str, trigger_reference: float, structural_invalidation: float, target_reference: Optional[float]
) -> Optional[SetupGeometry]:
    """None when geometry is malformed (risk_distance <= 0) -- the specialist
    must treat that as INVALID_STRUCTURAL_RISK and produce no candidate,
    never a candidate with negative/zero risk."""
    if side == SetupSide.LONG.value:
        risk_distance = trigger_reference - structural_invalidation
    elif side == SetupSide.SHORT.value:
        risk_distance = structural_invalidation - trigger_reference
    else:
        raise ValueError(f"unknown side: {side!r}")
    if risk_distance <= 0:
        return None
    room_to_target_R = None
    if target_reference is not None:
        room_to_target_R = abs(target_reference - trigger_reference) / risk_distance
    return SetupGeometry(risk_distance=risk_distance, room_to_target_R=room_to_target_R)


@dataclass(frozen=True)
class SetupCandidate:
    setup_candidate_id: str

    market_state_id: str
    snapshot_id: str
    data_hash: str

    instrument_key: InstrumentKey
    timeframe: str
    decision_time: int

    setup_family: str
    setup_version: str
    setup_policy_hash: str

    side: str

    trigger_reference: float
    structural_invalidation: float
    target_reference: Optional[float]

    initial_structural_risk: float
    room_to_target_R: Optional[float]

    geometry_features: Mapping[str, float] = field(default_factory=dict)
    required_capabilities: Tuple[str, ...] = ()
    evidence_components: Mapping[str, float] = field(default_factory=dict)
    #: Diagnostic only -- see module docstring. Never a probability/threshold.
    evidence_score: float = 0.0
    reason_codes: Tuple[str, ...] = ()

    created_at: int = 0
    valid_until: Optional[int] = None

    schema_version: str = SETUP_CANDIDATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.side not in _VALID_SIDES:
            raise ValueError(f"unknown side: {self.side!r}")
        if self.initial_structural_risk <= 0:
            raise ValueError("initial_structural_risk must be > 0 (invalid geometry must never reach this contract)")

    def canonical_payload(self) -> dict:
        return {
            "market_state_id": self.market_state_id,
            "snapshot_id": self.snapshot_id,
            "data_hash": self.data_hash,
            "instrument_key": {
                "canonical_symbol": self.instrument_key.canonical_symbol,
                "venue": self.instrument_key.venue,
                "venue_symbol": self.instrument_key.venue_symbol,
            },
            "timeframe": self.timeframe,
            "decision_time": self.decision_time,
            "setup_family": self.setup_family,
            "setup_version": self.setup_version,
            "setup_policy_hash": self.setup_policy_hash,
            "side": self.side,
            "trigger_reference": self.trigger_reference,
            "structural_invalidation": self.structural_invalidation,
            "target_reference": self.target_reference,
            "schema_version": self.schema_version,
        }

    @classmethod
    def build(
        cls,
        *,
        market_state_id: str,
        snapshot_id: str,
        data_hash: str,
        instrument_key: InstrumentKey,
        timeframe: str,
        decision_time: int,
        setup_family: str,
        setup_version: str,
        setup_policy_hash: str,
        side: str,
        trigger_reference: float,
        structural_invalidation: float,
        target_reference: Optional[float],
        geometry_features: Optional[Mapping[str, float]] = None,
        required_capabilities: Tuple[str, ...] = (),
        evidence_components: Optional[Mapping[str, float]] = None,
        evidence_score: float = 0.0,
        reason_codes: Tuple[str, ...] = (SetupReasonCode.SETUP_FOUND.value,),
        validity_bars: int = 3,
    ) -> "SetupCandidate":
        geometry = compute_geometry(
            side=side, trigger_reference=trigger_reference,
            structural_invalidation=structural_invalidation, target_reference=target_reference,
        )
        if geometry is None:
            raise ValueError("INVALID_STRUCTURAL_RISK: risk_distance <= 0 -- caller must reject before build()")

        bar_ms = timeframe_to_ms(timeframe)
        valid_until = decision_time + (bar_ms * validity_bars) if bar_ms else None

        identity_payload = dict(
            market_state_id=market_state_id, snapshot_id=snapshot_id, data_hash=data_hash,
            canonical_symbol=instrument_key.canonical_symbol, venue=instrument_key.venue,
            venue_symbol=instrument_key.venue_symbol, timeframe=timeframe, decision_time=decision_time,
            setup_family=setup_family, setup_version=setup_version, setup_policy_hash=setup_policy_hash,
            side=side, trigger_reference=trigger_reference, structural_invalidation=structural_invalidation,
            target_reference=target_reference,
        )
        content_id = short_id("setc", identity_payload)

        return cls(
            setup_candidate_id=content_id,
            market_state_id=market_state_id,
            snapshot_id=snapshot_id,
            data_hash=data_hash,
            instrument_key=instrument_key,
            timeframe=timeframe,
            decision_time=decision_time,
            setup_family=setup_family,
            setup_version=setup_version,
            setup_policy_hash=setup_policy_hash,
            side=side,
            trigger_reference=trigger_reference,
            structural_invalidation=structural_invalidation,
            target_reference=target_reference,
            initial_structural_risk=geometry.risk_distance,
            room_to_target_R=geometry.room_to_target_R,
            geometry_features=dict(geometry_features or {}),
            required_capabilities=tuple(required_capabilities),
            evidence_components=dict(evidence_components or {}),
            evidence_score=evidence_score,
            reason_codes=tuple(reason_codes),
            created_at=decision_time,
            valid_until=valid_until,
        )


__all__ = [
    "SetupSide",
    "SetupReasonCode",
    "SetupGeometry",
    "compute_geometry",
    "timeframe_to_ms",
    "SetupCandidate",
]
