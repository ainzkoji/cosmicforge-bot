"""Broker-neutral venue economic capability (Section 17.1).

``VenueEconomicAdapter`` is CATI's core contract -- never a Binance-shaped
interface. An adapter turns ONE credential-free ``VenueRawSnapshot`` (native
venue payloads captured by an existing safe runtime client) plus a
normalized ``VenueEconomicRequest`` into ONE immutable
``VenueEconomicObservation``. It never computes EV, never admits anything,
never submits orders, and never receives API keys, secrets or headers.

Side, entry reference, holding time and economic size are applied afterwards
by the single venue-neutral cost model (``venue/cost_model.py``): the
observation is side-independent so LONG and SHORT candidates of the same
instrument share one piece of evidence.

``BaseVenueEconomicAdapter`` holds the venue-NEUTRAL resolution logic every
adapter shares -- causality/freshness checks, the spread and slippage
fallback hierarchies, session state (reusing the runtime's canonical
``ForexSessionGuard``), adapter status and assembly/hashing. Venue adapters
only parse their native payloads.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Mapping, Optional, Protocol, Sequence, Tuple

from app.trading_intelligence.contracts.instrument import CRYPTO, FX, InstrumentKey
from app.trading_intelligence.contracts.system_health import BrokerHealthContext
from app.trading_intelligence.contracts.venue_economics import (
    AdapterValidationStatus, CarryObservation, ExecutionCapabilities, FeeObservation, FeeSource, FinancingObservation,
    FundingObservation, FundingSource, InstrumentMetadata, SessionState, SessionStatus, SlippageObservation,
    SlippageSource, SpreadObservation, SpreadSource, VenueEconomicObservation, VenueEnvironment, VenueReasonCode,
)
from app.trading_intelligence.venue.policy import VenueCostPolicy, default_venue_cost_policy

R = VenueReasonCode


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class VenueRawSnapshot:
    """Native venue payloads for one instrument, captured once. Market data
    and public instrument metadata only -- a collector must never place a
    credential, signature or header in here."""

    venue_symbol: str
    payloads: Mapping[str, Any]
    #: local time after the last payload arrived
    captured_at: int
    reason_codes: Tuple[str, ...] = ()


@dataclass(frozen=True)
class VenueEconomicRequest:
    """Normalized, credential-free inputs. ``environment`` comes from the
    canonical broker account (``broker_accounts.environment``), never from
    global .env configuration. ``decision_time`` is the cost-decision
    instant: every observation used must be stamped at or before it."""

    instrument_key: InstrumentKey
    environment: str  # VenueEnvironment
    decision_time: int
    user_id: Optional[str] = None
    broker_account_id: Optional[str] = None
    bot_instance_id: Optional[str] = None
    run_id: Optional[str] = None
    cycle_id: Optional[str] = None
    broker_health: Optional[BrokerHealthContext] = None


@dataclass(frozen=True)
class BookQuote:
    best_bid: float
    best_ask: float
    bid_quantity: Optional[float]
    ask_quantity: Optional[float]
    as_of: int


@dataclass(frozen=True)
class DepthBook:
    bids: Tuple[Tuple[float, float], ...]
    asks: Tuple[Tuple[float, float], ...]
    as_of: int


_ENVIRONMENT_ALIASES = {
    "demo": VenueEnvironment.DEMO, "paper": VenueEnvironment.DEMO, "practice": VenueEnvironment.DEMO,
    "testnet": VenueEnvironment.TESTNET, "sandbox": VenueEnvironment.TESTNET,
    "live": VenueEnvironment.REAL, "real": VenueEnvironment.REAL, "production": VenueEnvironment.REAL,
    "prod": VenueEnvironment.REAL, "mainnet": VenueEnvironment.REAL,
}


def normalize_environment(value: Optional[str]) -> str:
    """Canonical environment identity. Unknown is UNKNOWN -- never guessed."""
    if value is None:
        return VenueEnvironment.UNKNOWN.value
    v = str(value).strip()
    if v.upper() in VenueEnvironment.__members__:
        return v.upper()
    return _ENVIRONMENT_ALIASES.get(v.lower(), VenueEnvironment.UNKNOWN).value


# ---------------------------------------------------------------------------
# The contract
# ---------------------------------------------------------------------------
class VenueEconomicAdapter(Protocol):
    adapter_id: str
    broker: str
    venue_id: str
    adapter_version: str
    supported_asset_classes: Tuple[str, ...]

    def resolve_instrument_metadata(self, request: VenueEconomicRequest, raw: VenueRawSnapshot) -> Optional[InstrumentMetadata]: ...
    def estimate_fees(self, request: VenueEconomicRequest, raw: VenueRawSnapshot) -> FeeObservation: ...
    def observe_spread(self, request: VenueEconomicRequest, raw: VenueRawSnapshot) -> SpreadObservation: ...
    def estimate_slippage(self, request: VenueEconomicRequest, raw: VenueRawSnapshot) -> SlippageObservation: ...
    def estimate_funding_or_financing(self, request: VenueEconomicRequest, raw: VenueRawSnapshot) -> Tuple[FundingObservation, FinancingObservation]: ...
    def estimate_carry_basis(self, request: VenueEconomicRequest, raw: VenueRawSnapshot) -> CarryObservation: ...
    def describe_execution_capabilities(self, request: VenueEconomicRequest, raw: VenueRawSnapshot) -> Optional[ExecutionCapabilities]: ...
    def status_for(self, environment: str) -> str: ...
    def observe(self, request: VenueEconomicRequest, raw: VenueRawSnapshot) -> VenueEconomicObservation: ...


def _percentile(values: Sequence[float], q: float) -> float:
    ordered = sorted(float(v) for v in values)
    idx = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
    return ordered[idx]


@dataclass
class _Notes:
    reasons: List[str] = field(default_factory=list)

    def add(self, *codes: Any) -> None:
        for c in codes:
            value = c.value if hasattr(c, "value") else str(c)
            if value not in self.reasons:
                self.reasons.append(value)


class BaseVenueEconomicAdapter:
    adapter_id: str = ""
    broker: str = ""
    venue_id: str = ""
    adapter_version: str = "1.0.0"
    supported_asset_classes: Tuple[str, ...] = ()

    def __init__(self, policy: Optional[VenueCostPolicy] = None,
                 status_registry: Optional[Mapping[Tuple[str, str], str]] = None) -> None:
        self.policy = policy or default_venue_cost_policy()
        if status_registry is None:
            from app.trading_intelligence.venue.registry import ADAPTER_STATUS_REGISTRY

            status_registry = ADAPTER_STATUS_REGISTRY
        self._status_registry = status_registry

    # -- venue-specific native parsing (subclasses) -----------------------------------
    def resolve_instrument_metadata(self, request, raw) -> Optional[InstrumentMetadata]:
        return None

    def estimate_fees(self, request, raw) -> FeeObservation:
        return FeeObservation(fee_model="PERCENT_NOTIONAL", source=FeeSource.UNAVAILABLE.value,
                              reason_codes=(R.FEE_UNAVAILABLE.value,))

    def estimate_funding_or_financing(self, request, raw) -> Tuple[FundingObservation, FinancingObservation]:
        from app.trading_intelligence.contracts.venue_economics import FinancingSource

        return (FundingObservation(applicable=False, source=FundingSource.NOT_APPLICABLE.value),
                FinancingObservation(applicable=False, source=FinancingSource.NOT_APPLICABLE.value))

    def estimate_carry_basis(self, request, raw) -> CarryObservation:
        from app.trading_intelligence.contracts.venue_economics import CarrySource

        return CarryObservation(applicable=False, source=CarrySource.NOT_APPLICABLE.value)

    def describe_execution_capabilities(self, request, raw) -> Optional[ExecutionCapabilities]:
        return None

    def book_quote(self, raw: VenueRawSnapshot) -> Optional[BookQuote]:
        return None

    def depth_book(self, raw: VenueRawSnapshot) -> Optional[DepthBook]:
        return None

    def spread_history_bps(self, raw: VenueRawSnapshot) -> Tuple[float, ...]:
        return tuple(float(x) for x in (raw.payloads.get("spread_history_bps") or ()))

    def slippage_history_bps(self, raw: VenueRawSnapshot) -> Optional[float]:
        v = raw.payloads.get("slippage_history_bps")
        return float(v) if v is not None else None

    def liquidity_bucket(self, raw: VenueRawSnapshot) -> Optional[str]:
        v = raw.payloads.get("liquidity_bucket")
        return str(v).upper() if v else None

    def session_hint(self, raw: VenueRawSnapshot) -> Optional[str]:
        """Venue-published session state for non-24/7, non-FX instruments."""
        v = raw.payloads.get("session_status")
        return str(v).upper() if v else None

    def payload_timestamps(self, raw: VenueRawSnapshot) -> Tuple[int, ...]:
        """Venue timestamps carried by the payloads (adapters add their own)."""
        out = []
        for item in (self.book_quote(raw), self.depth_book(raw)):
            if item is not None and item.as_of is not None:
                out.append(int(item.as_of))
        return tuple(out)

    def causal_decision_time(self, raw: VenueRawSnapshot, floor: int) -> int:
        """The cost-decision instant: after the snapshot's candle close and
        after every piece of venue evidence was received (venue and local
        clocks may disagree slightly -- the later one wins, so evidence is
        never stamped after the decision it informs)."""
        return max([int(floor), int(raw.captured_at), *self.payload_timestamps(raw)])

    # -- status ------------------------------------------------------------------------
    def status_for(self, environment: str) -> str:
        return self._status_registry.get((self.adapter_id, environment),
                                         AdapterValidationStatus.UNVALIDATED.value)

    # -- shared causal helpers -----------------------------------------------------------
    @staticmethod
    def _causal(ts: Optional[int], decision_time: int, notes: _Notes) -> bool:
        if ts is not None and ts > decision_time:
            notes.add(R.NON_CAUSAL_OBSERVATION)
            return False
        return True

    def _fresh_book(self, request, raw, notes: _Notes) -> Optional[BookQuote]:
        book = self.book_quote(raw)
        if book is None:
            return None
        if not self._causal(book.as_of, request.decision_time, notes):
            return None
        if request.decision_time - book.as_of > self.policy.max_book_age_ms:
            notes.add(R.STALE_BOOK)
            return None
        if book.best_bid <= 0 or book.best_ask <= 0 or book.best_ask < book.best_bid:
            return None
        return book

    # -- shared spread hierarchy (17.4) ----------------------------------------------------
    def observe_spread(self, request, raw) -> SpreadObservation:
        notes = _Notes()
        return self._spread(request, raw, notes)

    def _spread(self, request, raw, notes: _Notes, tick_size: Optional[float] = None) -> SpreadObservation:
        p = self.policy
        asset = request.instrument_key.asset_class
        raw_book = self.book_quote(raw)
        book = self._fresh_book(request, raw, notes)
        if book is not None:
            absolute = book.best_ask - book.best_bid
            if tick_size and absolute < tick_size:
                absolute = tick_size  # a locked/one-tick book still costs one tick to cross
            mid = (book.best_ask + book.best_bid) / 2.0
            return SpreadObservation(
                source=SpreadSource.LIVE_TOP_OF_BOOK.value, spread_absolute=absolute, spread_bps=absolute / mid * 1e4,
                best_bid=book.best_bid, best_ask=book.best_ask, book_as_of=book.as_of, stale=False, fallback_level=0,
                reason_codes=tuple(notes.reasons))
        stale = R.STALE_BOOK.value in notes.reasons
        common = dict(best_bid=raw_book.best_bid if raw_book else None, best_ask=raw_book.best_ask if raw_book else None,
                      book_as_of=raw_book.as_of if raw_book else None, stale=stale)
        history = self.spread_history_bps(raw)
        if history:
            notes.add(R.SPREAD_FALLBACK_USED)
            return SpreadObservation(source=SpreadSource.RECENT_VENUE_DISTRIBUTION.value, spread_absolute=None,
                                     spread_bps=_percentile(history, p.spread_distribution_percentile), fallback_level=1,
                                     reason_codes=tuple(notes.reasons), **common)
        bucket = self.liquidity_bucket(raw)
        if bucket and bucket in p.liquidity_bucket_spread_bps:
            notes.add(R.SPREAD_FALLBACK_USED)
            return SpreadObservation(source=SpreadSource.LIQUIDITY_BUCKET_HISTORICAL.value, spread_absolute=None,
                                     spread_bps=float(p.liquidity_bucket_spread_bps[bucket]), fallback_level=2,
                                     reason_codes=tuple(notes.reasons), **common)
        if asset in p.fallback_spread_bps:
            notes.add(R.SPREAD_FALLBACK_USED)
            return SpreadObservation(source=SpreadSource.CONSERVATIVE_VENUE_FALLBACK.value, spread_absolute=None,
                                     spread_bps=float(p.fallback_spread_bps[asset]), fallback_level=3,
                                     reason_codes=tuple(notes.reasons), **common)
        notes.add(R.SPREAD_UNAVAILABLE)
        return SpreadObservation(source=SpreadSource.UNAVAILABLE.value, spread_absolute=None, spread_bps=None,
                                 fallback_level=4, reason_codes=tuple(notes.reasons), **common)

    # -- shared slippage hierarchy (17.5) --------------------------------------------------
    def estimate_slippage(self, request, raw) -> SlippageObservation:
        return self._slippage(request, raw, _Notes())

    def _slippage(self, request, raw, notes: _Notes) -> SlippageObservation:
        p = self.policy
        asset = request.instrument_key.asset_class
        book = self._fresh_book(request, raw, _Notes())  # staleness already recorded by the spread step
        tops = dict(top_bid_quantity=book.bid_quantity if book else None,
                    top_ask_quantity=book.ask_quantity if book else None)
        depth = self.depth_book(raw)
        bucket = self.liquidity_bucket(raw)
        if depth is None:
            notes.add(R.DEPTH_UNAVAILABLE)
        elif not self._causal(depth.as_of, request.decision_time, notes):
            depth = None
        elif request.decision_time - depth.as_of > p.max_depth_age_ms:
            notes.add(R.DEPTH_STALE)
            depth = None
        elif min(len(depth.bids), len(depth.asks)) < p.min_reliable_depth_levels:
            notes.add(R.DEPTH_UNAVAILABLE)
            depth = None
        if depth is not None:
            return SlippageObservation(source=SlippageSource.DEPTH_WALK.value, depth_bids=depth.bids, depth_asks=depth.asks,
                                       depth_as_of=depth.as_of, liquidity_bucket=bucket, fallback_level=0,
                                       reason_codes=tuple(notes.reasons), **tops)
        notes.add(R.SLIPPAGE_FALLBACK_USED)
        hist = self.slippage_history_bps(raw)
        if hist is not None and hist > 0:
            src, bps, level = SlippageSource.HISTORICAL_INSTRUMENT, hist, 1
        elif bucket and bucket in p.liquidity_bucket_slippage_bps:
            src, bps, level = SlippageSource.HISTORICAL_LIQUIDITY_BUCKET, p.liquidity_bucket_slippage_bps[bucket], 2
        elif asset in p.venue_class_slippage_bps:
            src, bps, level = SlippageSource.VENUE_ASSET_CLASS_DEFAULT, p.venue_class_slippage_bps[asset], 3
        else:
            src, bps, level = SlippageSource.CONSERVATIVE_CONFIGURED_FALLBACK, p.conservative_slippage_bps, 4
        return SlippageObservation(source=src.value, per_side_bps=float(bps), liquidity_bucket=bucket,
                                   fallback_level=level, reason_codes=tuple(notes.reasons), **tops)

    # -- shared session (17.10): reuse the runtime's canonical session truth -----------------
    def resolve_session(self, request, raw) -> SessionState:
        asset = request.instrument_key.asset_class
        t = request.decision_time
        if asset == CRYPTO:
            return SessionState(SessionStatus.ALWAYS_OPEN.value, "CRYPTO_24_7", t)
        if asset == FX:
            try:
                from app.models.unified_trading import AssetClass
                from app.symbols.market_hours import ForexSessionGuard

                if ForexSessionGuard.is_market_open(AssetClass.FOREX_SPOT, t):
                    return SessionState(SessionStatus.OPEN.value, "ForexSessionGuard", t)
                if "Rollover" in ForexSessionGuard.get_status_reason(AssetClass.FOREX_SPOT, t):
                    return SessionState(SessionStatus.ROLLOVER.value, "ForexSessionGuard", t,
                                        (R.ROLLOVER_WINDOW.value, R.MARKET_CLOSED.value))
                return SessionState(SessionStatus.CLOSED.value, "ForexSessionGuard", t, (R.MARKET_CLOSED.value,))
            except Exception:
                return SessionState(SessionStatus.UNKNOWN.value, "ForexSessionGuard", t, (R.SESSION_UNKNOWN.value,))
        hint = self.session_hint(raw)
        if hint in (SessionStatus.OPEN.value, SessionStatus.THIN.value):
            codes = (R.THIN_SESSION.value,) if hint == SessionStatus.THIN.value else ()
            return SessionState(hint, f"{self.adapter_id}:venue_session", t, codes)
        if hint == SessionStatus.CLOSED.value:
            return SessionState(SessionStatus.CLOSED.value, f"{self.adapter_id}:venue_session", t, (R.MARKET_CLOSED.value,))
        codes = (R.SESSION_UNKNOWN.value,) if self.policy.unknown_session_fails_closed else ()
        return SessionState(SessionStatus.UNKNOWN.value, "none", t, codes)

    # -- assembly --------------------------------------------------------------------------
    def observe(self, request: VenueEconomicRequest, raw: VenueRawSnapshot) -> VenueEconomicObservation:
        notes = _Notes()
        notes.add(*raw.reason_codes)
        environment = normalize_environment(request.environment)
        if environment == VenueEnvironment.UNKNOWN.value:
            notes.add(R.ENVIRONMENT_UNKNOWN)
        if request.instrument_key.asset_class not in self.supported_asset_classes:
            notes.add(R.UNSUPPORTED_ASSET_CLASS)
        status = self.status_for(environment)
        if status not in self.policy.trusted_adapter_statuses:
            notes.add(R.ADAPTER_UNVALIDATED)
        health = request.broker_health
        if health is not None:
            if health.status == "UNAVAILABLE":
                notes.add(R.BROKER_UNAVAILABLE)
            elif health.status in ("DEGRADED", "UNKNOWN"):
                notes.add(R.BROKER_DEGRADED)

        metadata = self.resolve_instrument_metadata(request, raw)
        if metadata is None:
            notes.add(R.INSTRUMENT_METADATA_UNAVAILABLE)
        elif metadata.canonical_symbol != request.instrument_key.canonical_symbol \
                or metadata.venue_symbol.upper() != request.instrument_key.venue_symbol.upper():
            notes.add(R.INSTRUMENT_MAPPING_MISMATCH)
        elif metadata.as_of is not None:
            self._causal(metadata.as_of, request.decision_time, notes)

        fee = self.estimate_fees(request, raw)
        spread = self._spread(request, raw, notes, tick_size=metadata.tick_size if metadata else None)
        slippage = self._slippage(request, raw, notes)
        funding, financing = self.estimate_funding_or_financing(request, raw)
        carry = self.estimate_carry_basis(request, raw)
        for obs in (fee, funding, financing, carry):
            notes.add(*obs.reason_codes)
            self._causal(getattr(obs, "observed_at", None), request.decision_time, notes)
        capabilities = self.describe_execution_capabilities(request, raw)
        session = self.resolve_session(request, raw)
        notes.add(*session.reason_codes)

        stamps = [t for t in (spread.book_as_of if spread.source == SpreadSource.LIVE_TOP_OF_BOOK.value else None,
                              slippage.depth_as_of, fee.observed_at, funding.observed_at, financing.observed_at,
                              carry.observed_at) if t is not None and t <= request.decision_time]
        observed_at = max(stamps) if stamps else request.decision_time

        from app.trading_intelligence.contracts.venue_economics import FATAL_VENUE_REASONS

        fallback = (spread.fallback_level > 0 or slippage.fallback_level > 0
                    or fee.source not in (FeeSource.OBSERVED_ACCOUNT_TIER.value, FeeSource.BROKER_METADATA.value)
                    or funding.source == FundingSource.CONSERVATIVE_CONFIGURED_FALLBACK.value)
        if any(r in FATAL_VENUE_REASONS for r in notes.reasons):
            quality = "INVALID"
        else:
            quality = "DEGRADED" if fallback else "VALID"
        availability = (
            ("account_fee_tier", fee.source == FeeSource.OBSERVED_ACCOUNT_TIER.value),
            ("carry_basis", carry.source == "OBSERVED_BASIS"),
            ("depth", slippage.source == SlippageSource.DEPTH_WALK.value),
            ("financing", financing.source == "BROKER_SWAP_RATES"),
            ("funding", funding.source in (FundingSource.PREDICTED_RATE.value, FundingSource.CURRENT_RATE_SCHEDULE.value)),
            ("instrument_metadata", metadata is not None),
            ("mark_index", funding.mark_price is not None and funding.index_price is not None),
            ("top_of_book", spread.source == SpreadSource.LIVE_TOP_OF_BOOK.value),
        )
        return VenueEconomicObservation.build(
            user_id=request.user_id, broker_account_id=request.broker_account_id,
            bot_instance_id=request.bot_instance_id, run_id=request.run_id, cycle_id=request.cycle_id,
            broker=self.broker, venue=self.venue_id, environment=environment, instrument_key=request.instrument_key,
            decision_time=request.decision_time, observed_at=observed_at,
            valid_until=observed_at + self.policy.observation_validity_ms,
            fee_observation=fee, spread_observation=spread, slippage_observation=slippage,
            funding_observation=funding, financing_observation=financing, carry_observation=carry,
            instrument_metadata=metadata, execution_capabilities=capabilities, session_state=session,
            adapter_id=self.adapter_id, adapter_version=self.adapter_version, adapter_status=status,
            cost_policy_hash=self.policy.policy_hash, source_quality=quality, feature_availability=availability,
            reason_codes=tuple(notes.reasons),
        )


class UnsupportedVenueAdapter(BaseVenueEconomicAdapter):
    """What a venue without a CATI economic adapter resolves to: an explicit,
    fail-closed observation (UNSUPPORTED_VENUE) -- never a zero-cost default."""

    adapter_id = "unsupported"
    adapter_version = "1.0.0"

    def __init__(self, broker: str, policy: Optional[VenueCostPolicy] = None) -> None:
        super().__init__(policy, status_registry={})
        self.broker = str(broker or "UNKNOWN").upper()
        self.venue_id = self.broker

    def observe(self, request, raw) -> VenueEconomicObservation:
        obs = super().observe(request, raw)
        import dataclasses

        codes = tuple(dict.fromkeys(obs.reason_codes + (R.UNSUPPORTED_VENUE.value,)))
        fields = {f.name: getattr(obs, f.name) for f in dataclasses.fields(obs)
                  if f.name not in ("observation_id", "observation_hash")}
        fields.update(reason_codes=codes, source_quality="INVALID")
        return VenueEconomicObservation.build(**fields)


__all__ = [
    "VenueRawSnapshot", "VenueEconomicRequest", "BookQuote", "DepthBook", "normalize_environment",
    "VenueEconomicAdapter", "BaseVenueEconomicAdapter", "UnsupportedVenueAdapter",
]
