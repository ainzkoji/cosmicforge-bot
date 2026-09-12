"""Broker-derived market universe.

The connected broker account -- not an environment variable -- is the source of
the markets a bot may trade::

    broker account -> UniverseAdapter -> UniverseEngine (eligibility, ranking)
                   -> UniverseRuntime (open positions first, candidates after)
                   -> runner -> Master Ensemble -> threshold -> risk -> capital -> execution

Nothing in this package is Binance-specific except the one adapter that speaks
Binance USD-M; everything above the adapter is broker-neutral.
"""
from app.universe.contracts import (
    Exclusion,
    InstrumentMeta,
    MarketStats,
    Product,
    UniverseMember,
    UniverseMode,
    UniverseSnapshot,
)
from app.universe.identity import CanonicalInstrument, canonical_instrument, parse_venue_symbol

__all__ = [
    "CanonicalInstrument",
    "Exclusion",
    "InstrumentMeta",
    "MarketStats",
    "Product",
    "UniverseMember",
    "UniverseMode",
    "UniverseSnapshot",
    "canonical_instrument",
    "parse_venue_symbol",
]
