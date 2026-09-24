"""Canonical instrument identity for CATI (Section 8.1).

The repo already has a canonical-instrument abstraction for the live trading
path: ``app.universe.identity.CanonicalInstrument`` (underlying/quote/kind/
multiplier) plus ``VenueSymbol``. That abstraction is deliberately narrow --
it exists to answer "is this the same exposure", not to describe the full
economic shape of an instrument (asset class, settlement asset, expiry,
option strike/right, contract multiplier as a first-class field, etc).

``InstrumentKey`` is CATI's identity contract. It does not replace
``CanonicalInstrument`` and does not change the exchange symbol API: it is
built from a venue symbol plus (when available) the existing canonical
mapping, via ``from_canonical()``. A symbol string alone is never sufficient
identity for CATI's shared cache key (Section 8.4) -- two venues can spell
the same economic instrument differently, and one venue can list the same
canonical underlying under several contract types (spot vs. perpetual vs. a
dated future).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.trading_intelligence.versions import INSTRUMENT_SCHEMA_VERSION

# -- asset_class -------------------------------------------------------------
CRYPTO = "CRYPTO"
FUTURES = "FUTURES"
FX = "FX"
EQUITY = "EQUITY"
OPTION = "OPTION"

# -- contract_type -------------------------------------------------------------
PERPETUAL = "PERPETUAL"
FUTURE = "FUTURE"
SPOT = "SPOT"
CFD = "CFD"

_VALID_ASSET_CLASSES = frozenset({CRYPTO, FUTURES, FX, EQUITY, OPTION})
_VALID_CONTRACT_TYPES = frozenset({PERPETUAL, FUTURE, SPOT, "OPTION", CFD})


@dataclass(frozen=True)
class InstrumentKey:
    """What is being analyzed -- venue-independent economic identity.

    ``canonical_symbol`` is the venue-independent identity used as part of
    CATI's shared cache key (Section 8.4); ``venue`` + ``venue_symbol``
    record which venue's data actually fed the computation, so two venues
    mapping the same canonical economic instrument never collide in the
    shared cache, and are never silently treated as the identical instrument
    when their venue-level mechanics (funding, contract multiplier) differ.
    """

    asset_class: str
    base_asset: str
    quote_asset: str
    canonical_symbol: str
    venue: str
    venue_symbol: str
    settlement_asset: Optional[str] = None
    contract_type: str = PERPETUAL
    expiry: Optional[str] = None
    strike: Optional[float] = None
    option_right: Optional[str] = None
    contract_multiplier: float = 1.0
    schema_version: str = INSTRUMENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.asset_class not in _VALID_ASSET_CLASSES:
            raise ValueError(f"unknown asset_class: {self.asset_class!r}")
        if self.contract_type not in _VALID_CONTRACT_TYPES:
            raise ValueError(f"unknown contract_type: {self.contract_type!r}")
        if not self.canonical_symbol:
            raise ValueError("canonical_symbol is required")
        if not self.venue or not self.venue_symbol:
            raise ValueError("venue and venue_symbol are required")

    @property
    def cache_identity(self) -> tuple:
        """The identity tuple used by SharedStateCacheKey (Section 8.4)."""
        return (self.venue, self.canonical_symbol, self.contract_type, self.schema_version)


def from_canonical(
    *,
    canonical,  # app.universe.identity.CanonicalInstrument
    venue: str,
    venue_symbol: str,
    asset_class: str = CRYPTO,
    settlement_asset: Optional[str] = None,
) -> InstrumentKey:
    """Build an InstrumentKey from the existing live-path CanonicalInstrument.

    Reuses ``app.universe.identity.CanonicalInstrument`` rather than
    re-deriving underlying/multiplier splitting logic. A crypto perpetual's
    settlement asset defaults to its quote asset (USDT-margined contracts
    are the current production reality); pass ``settlement_asset`` explicitly
    for coin-margined or non-USDT-settled contracts.
    """
    contract_type = PERPETUAL if canonical.kind == "PERP" else FUTURE
    return InstrumentKey(
        asset_class=asset_class,
        base_asset=canonical.underlying,
        quote_asset=canonical.quote,
        settlement_asset=settlement_asset or canonical.quote,
        contract_type=contract_type,
        contract_multiplier=float(canonical.multiplier),
        canonical_symbol=canonical.canonical_id,
        venue=venue,
        venue_symbol=venue_symbol,
    )


def from_symbol_fallback(*, venue: str, venue_symbol: str, asset_class: str = CRYPTO) -> InstrumentKey:
    """Best-effort InstrumentKey when no canonical mapping metadata is at hand.

    Uses ``app.universe.identity.underlying_from_symbol`` -- documented there
    as "for risk classification only" -- so this fallback must never be used
    for order routing. It exists so CATI can still build a MarketState (with
    an honest, coarse identity) for a symbol the live universe layer has not
    (yet) resolved metadata for, rather than refusing to run at all.
    """
    from app.universe.identity import underlying_from_symbol, split_multiplier
    from app.universe.identity import PERP as _UNIVERSE_PERP

    s = str(venue_symbol or "").strip().upper()
    underlying = underlying_from_symbol(s)
    _, multiplier = split_multiplier(underlying)
    # Best-effort quote extraction mirrors underlying_from_symbol's own scan.
    quote = "USDT"
    for known_quote in ("FDUSD", "USDT", "USDC", "BUSD", "USD1", "USD"):
        if s.endswith(known_quote) and len(s) > len(known_quote):
            quote = known_quote
            break
    # Must match app.universe.identity.CanonicalInstrument.canonical_id's own
    # "PERP" suffix exactly -- from_canonical() builds canonical_symbol from
    # that same property, and the two paths must never disagree on the
    # identity of the same economic instrument (Section 8.4 cache identity).
    canonical_symbol = f"{underlying}/{quote}:{_UNIVERSE_PERP}"
    return InstrumentKey(
        asset_class=asset_class,
        base_asset=underlying,
        quote_asset=quote,
        settlement_asset=quote,
        contract_type=PERPETUAL,
        contract_multiplier=float(multiplier),
        canonical_symbol=canonical_symbol,
        venue=venue,
        venue_symbol=s,
    )


def from_fx_pair(*, venue: str, venue_symbol: str, base: str, quote: str, contract_type: str = SPOT) -> InstrumentKey:
    """An FX pair from KNOWN base/quote currencies (preferred path: broker
    instrument metadata). Settlement is the quote currency; no funding,
    liquidation or 24/7-session assumption is implied by this identity."""
    b, q = str(base).strip().upper(), str(quote).strip().upper()
    if not b or not q:
        raise ValueError("FX pair needs base and quote currencies")
    return InstrumentKey(
        asset_class=FX, base_asset=b, quote_asset=q, settlement_asset=q, contract_type=contract_type,
        canonical_symbol=f"{b}/{q}:{contract_type}", venue=venue, venue_symbol=str(venue_symbol).strip().upper(),
    )


def instrument_key_for(*, venue: str, venue_symbol: str, asset_class: str = CRYPTO) -> InstrumentKey:
    """Best-effort key from a persisted (venue, symbol, asset class) when no
    richer broker metadata is at hand (e.g. rows read back from the DB).

    * CRYPTO  -> ``from_symbol_fallback`` (existing canonical mapping)
    * FX      -> the 6-letter currency pair after stripping separators
                 (``EUR_USD``, ``EUR/USD``, ``EURUSD`` are one identity)
    * other   -> an opaque identity (base = venue symbol); never guessed
                 into a quote/settlement currency it may not have.
    """
    if asset_class == CRYPTO:
        return from_symbol_fallback(venue=venue, venue_symbol=venue_symbol, asset_class=asset_class)
    sym = str(venue_symbol or "").strip().upper()
    if asset_class == FX:
        letters = "".join(ch for ch in sym if ch.isalpha())
        if len(letters) >= 6:
            return from_fx_pair(venue=venue, venue_symbol=sym, base=letters[:3], quote=letters[3:6])
    contract = FUTURE if asset_class == FUTURES else SPOT
    return InstrumentKey(asset_class=asset_class, base_asset=sym, quote_asset="UNSPECIFIED", settlement_asset=None,
                         contract_type=contract, canonical_symbol=f"{sym}:{contract}", venue=venue, venue_symbol=sym)


__all__ = [
    "InstrumentKey",
    "from_canonical",
    "from_symbol_fallback",
    "from_fx_pair",
    "instrument_key_for",
    "CRYPTO",
    "FUTURES",
    "FX",
    "EQUITY",
    "OPTION",
    "PERPETUAL",
    "FUTURE",
    "SPOT",
    "CFD",
]
