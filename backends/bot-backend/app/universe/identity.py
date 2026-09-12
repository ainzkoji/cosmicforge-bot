"""Canonical instrument identity -- one name per exposure, whatever a venue calls it.

Strategy, risk and evidence code must not depend on the fact that Binance says
``BTCUSDT``, OKX ``BTC-USDT-SWAP`` and Bybit ``BTCUSDT``: all three are
``BTC/USDT:PERP``. The venue symbol is what an order is sent with; the canonical
instrument is what the exposure *is*.

Two exposures are the same when their underlying is the same. That is why the
multiplier contracts venues list for low-priced coins (``1000PEPEUSDT``,
``1MBABYDOGEUSDT``) map back to their underlying (``PEPE``, ``BABYDOGE``).
"""
from __future__ import annotations

import re
from dataclasses import dataclass

PERP = "PERP"
FUTURE = "FUTURE"

#: Multiplier prefixes venues put in front of low-priced coins. Longest first.
_MULTIPLIERS = (
    ("1000000", 1_000_000),
    ("100000", 100_000),
    ("10000", 10_000),
    ("1000", 1_000),
    ("1M", 1_000_000),
)
_REMAINDER = re.compile(r"^[A-Z][A-Z0-9]+$")

#: Quote assets tried, longest first, when a concatenated symbol has no metadata.
_KNOWN_QUOTES = ("FDUSD", "USDT", "USDC", "BUSD", "USD1", "USD")


def split_multiplier(base_asset: str) -> tuple[str, int]:
    """``1000PEPE`` -> ``("PEPE", 1000)``; ``BTC`` -> ``("BTC", 1)``."""
    base = str(base_asset or "").strip().upper()
    for prefix, value in _MULTIPLIERS:
        if base.startswith(prefix):
            rest = base[len(prefix):]
            if len(rest) >= 2 and _REMAINDER.match(rest):
                return rest, value
    return base, 1


@dataclass(frozen=True)
class CanonicalInstrument:
    """What an exposure is, independent of the venue that lists it."""

    underlying: str
    quote: str
    kind: str = PERP
    multiplier: int = 1

    @property
    def canonical_id(self) -> str:
        return f"{self.underlying}/{self.quote}:{self.kind}"

    def same_exposure(self, other: "CanonicalInstrument") -> bool:
        """Same underlying risk, whatever the quote, venue or multiplier."""
        return self.underlying == other.underlying


def canonical_instrument(base_asset: str, quote_asset: str, kind: str = PERP) -> CanonicalInstrument:
    underlying, multiplier = split_multiplier(base_asset)
    return CanonicalInstrument(
        underlying=underlying,
        quote=str(quote_asset or "").strip().upper(),
        kind=kind,
        multiplier=multiplier,
    )


@dataclass(frozen=True)
class VenueSymbol:
    """A venue's name for an instrument, bound to its canonical identity."""

    venue: str
    symbol: str
    instrument: CanonicalInstrument


def parse_venue_symbol(
    venue: str,
    symbol: str,
    *,
    base_asset: str | None = None,
    quote_asset: str | None = None,
    kind: str = PERP,
) -> VenueSymbol:
    """Map a venue symbol to its canonical instrument.

    Metadata wins when given. Venues that spell the pair out in the symbol
    (OKX ``BTC-USDT-SWAP``) can be parsed without it. Venues whose symbols are
    bare concatenations (Binance and Bybit ``BTCUSDT``) are not guessed at: an
    adapter must pass the base and quote from the venue's instrument metadata.
    """
    v = str(venue or "").strip().lower()
    s = str(symbol or "").strip().upper()
    if base_asset and quote_asset:
        return VenueSymbol(v, s, canonical_instrument(base_asset, quote_asset, kind))
    if v.startswith("okx"):
        parts = s.split("-")
        if len(parts) >= 3:
            return VenueSymbol(
                v, s, canonical_instrument(parts[0], parts[1], PERP if parts[2] == "SWAP" else FUTURE)
            )
    raise ValueError(f"cannot map {venue}:{symbol} to a canonical instrument without base/quote metadata")


def underlying_from_symbol(symbol: str) -> str:
    """Best-effort underlying of a concatenated symbol when no metadata is at hand.

    For risk classification only (leverage ceilings, exposure grouping). Order
    routing always uses the venue symbol and never this.
    """
    s = str(symbol or "").strip().upper()
    for quote in _KNOWN_QUOTES:
        if s.endswith(quote) and len(s) > len(quote):
            return split_multiplier(s[: -len(quote)])[0]
    return split_multiplier(s)[0]


__all__ = [
    "CanonicalInstrument",
    "FUTURE",
    "PERP",
    "VenueSymbol",
    "canonical_instrument",
    "parse_venue_symbol",
    "split_multiplier",
    "underlying_from_symbol",
]
