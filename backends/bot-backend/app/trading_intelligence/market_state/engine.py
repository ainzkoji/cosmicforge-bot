"""The Market Reality / MarketState engine (Section 9) and the shared,
tenant-free caching service (Section 9.13, Section 8.5).

``build_market_state`` is a pure function: identical arguments always
produce a byte-identical ``MarketState.canonical_hash`` (P2 determinism). It
takes no implicit input (no wall clock, no live fetch, no user/bot context)
-- everything it needs is passed in by the integration adapter, which is the
only layer allowed to touch the existing, live ``MarketSnapshot``.
"""
from __future__ import annotations

from collections import OrderedDict
from threading import Lock
from typing import Callable, Mapping, Optional

from app.trading_intelligence.contracts.data_quality import (
    Capability,
    DataManifest,
    FeatureAvailability,
    ReasonCode,
    SharedStateCacheKey,
    derive_data_quality,
)
from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.contracts.market_state import CandleSeries, MarketState
from app.trading_intelligence.market_state.derivatives import DerivativesSnapshotInput, compute_derivatives_state
from app.trading_intelligence.market_state.liquidity import BookSnapshotInput, compute_liquidity_state
from app.trading_intelligence.market_state.momentum import compute_momentum_state
from app.trading_intelligence.market_state.multi_timeframe import compute_higher_timeframe_state
from app.trading_intelligence.market_state.participation import compute_participation_state
from app.trading_intelligence.market_state.structure import MIN_HISTORY as STRUCTURE_MIN_HISTORY, compute_structure_state
from app.trading_intelligence.market_state.trend import compute_trend_state
from app.trading_intelligence.market_state.uncertainty import compute_state_uncertainty
from app.trading_intelligence.market_state.volatility import compute_volatility_state
from app.trading_intelligence.versions import MARKET_STATE_ENGINE_VERSION, MARKET_STATE_SCHEMA_VERSION


def build_market_state(
    *,
    instrument_key: InstrumentKey,
    timeframe: str,
    decision_time: int,
    primary_series: CandleSeries,
    snapshot_id: str,
    data_hash: str,
    manifest: DataManifest,
    htf_series: Optional[CandleSeries] = None,
    htf_causally_aligned: bool = True,
    auxiliary_series: Optional[Mapping[str, CandleSeries]] = None,
    book: Optional[BookSnapshotInput] = None,
    derivatives: Optional[DerivativesSnapshotInput] = None,
) -> MarketState:
    reason_codes: list = []

    if primary_series is None or len(primary_series) < STRUCTURE_MIN_HISTORY:
        reason_codes.append(ReasonCode.INSUFFICIENT_HISTORY.value)
    latest_primary_close = primary_series.latest_close_time if primary_series else None
    if latest_primary_close is None:
        reason_codes.append(ReasonCode.PRIMARY_CANDLES_MISSING.value)
    elif latest_primary_close > decision_time:
        reason_codes.append(ReasonCode.OUT_OF_CAUSAL_BOUNDARY.value)

    structure_state = compute_structure_state(primary_series)
    trend_state = compute_trend_state(primary_series)
    volatility_state = compute_volatility_state(primary_series)
    momentum_state = compute_momentum_state(primary_series)
    participation_state = compute_participation_state(primary_series)
    liquidity_state = compute_liquidity_state(book, decision_time=decision_time)
    derivatives_state = compute_derivatives_state(derivatives, decision_time=decision_time)
    higher_timeframe_state = compute_higher_timeframe_state(
        htf_series,
        decision_time=decision_time,
        htf_causally_aligned=htf_causally_aligned,
        local_direction=trend_state.direction,
    )

    for family in (structure_state, trend_state, volatility_state, momentum_state, participation_state, liquidity_state, derivatives_state, higher_timeframe_state):
        reason_codes.extend(family.reason_codes or ())

    feature_availability = FeatureAvailability.build(
        available=tuple(
            cap
            for cap, ok in (
                (Capability.OHLCV, bool(primary_series) and len(primary_series) > 0),
                (Capability.TOP_OF_BOOK, liquidity_state.available),
                (Capability.DEPTH, liquidity_state.available and liquidity_state.top_book_depth is not None),
                (Capability.TRADES_AGGRESSOR, participation_state.taker_imbalance is not None),
                (Capability.OPEN_INTEREST, derivatives_state.available and derivatives_state.open_interest is not None),
                (Capability.FUNDING, derivatives_state.available and derivatives_state.funding_current is not None),
                (Capability.BASIS, derivatives_state.available and derivatives_state.basis is not None),
            )
            if ok
        ),
        unavailable={
            **({Capability.TOP_OF_BOOK: ReasonCode.TOP_BOOK_UNAVAILABLE} if not liquidity_state.available else {}),
            **({Capability.FUNDING: ReasonCode.FUNDING_UNAVAILABLE} if not (derivatives_state.available and derivatives_state.funding_current is not None) else {}),
            **({Capability.OPEN_INTEREST: ReasonCode.OPEN_INTEREST_UNAVAILABLE} if not (derivatives_state.available and derivatives_state.open_interest is not None) else {}),
        },
        unsupported=(Capability.LIQUIDATIONS,),
    )

    data_quality = derive_data_quality(tuple(reason_codes))

    state_uncertainty = compute_state_uncertainty(
        structure_state=structure_state,
        trend_state=trend_state,
        volatility_state=volatility_state,
        momentum_state=momentum_state,
        participation_state=participation_state,
        higher_timeframe_state=higher_timeframe_state,
        data_quality=data_quality,
        feature_availability=feature_availability,
    )

    return MarketState.build(
        instrument_key=instrument_key,
        timeframe=timeframe,
        decision_time=decision_time,
        latest_closed_candle_time=latest_primary_close or 0,
        snapshot_id=snapshot_id,
        data_hash=data_hash,
        data_manifest_hash=manifest.manifest_hash,
        schema_version=MARKET_STATE_SCHEMA_VERSION,
        engine_version=MARKET_STATE_ENGINE_VERSION,
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
        reason_codes=tuple(dict.fromkeys(reason_codes)),
    )


class SharedMarketIntelligenceService:
    """Builds/retrieves MarketState, cached strictly by SharedStateCacheKey.

    MUST NOT and does not accept, read or store: user_id, bot_instance_id,
    capital, positions, credentials or any other tenant-scoped value -- its
    entire public surface takes only a cache key and a zero-argument builder
    callback. This is what makes one computation safely reusable by many
    bots/users evaluating the same instrument at the same causal boundary
    (P3 tenant isolation, Section 9.13).

    Bounded, in-memory, process-local for V1 (Section 8.5) -- correctness
    does not depend on a distributed cache; a cache miss just recomputes.
    """

    def __init__(self, max_entries: int = 512) -> None:
        self._max_entries = max_entries
        self._cache: "OrderedDict[SharedStateCacheKey, MarketState]" = OrderedDict()
        self._lock = Lock()
        self.hits = 0
        self.misses = 0

    def get_or_build(self, cache_key: SharedStateCacheKey, builder: Callable[[], MarketState]) -> MarketState:
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                self._cache.move_to_end(cache_key)
                self.hits += 1
                return cached
        state = builder()
        with self._lock:
            self._cache[cache_key] = state
            self._cache.move_to_end(cache_key)
            while len(self._cache) > self._max_entries:
                self._cache.popitem(last=False)
            self.misses += 1
        return state

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self.hits = 0
            self.misses = 0

    def size(self) -> int:
        with self._lock:
            return len(self._cache)


_shared_service_lock = Lock()
_shared_service: Optional[SharedMarketIntelligenceService] = None


def get_shared_market_intelligence_service() -> SharedMarketIntelligenceService:
    global _shared_service
    with _shared_service_lock:
        if _shared_service is None:
            _shared_service = SharedMarketIntelligenceService()
        return _shared_service


def reset_shared_market_intelligence_service() -> None:
    """Test-only reset, mirroring app.threshold.runtime.reset_for_tests()."""
    global _shared_service
    with _shared_service_lock:
        _shared_service = None
