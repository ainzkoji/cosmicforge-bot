"""FX reference <-> execution mapping (Section 19.14), exposed from existing persistence -- no new table.

One canonical FX pair (``EUR/USD``) maps to SEPARATE rows per source:

* ``REFERENCE`` rows -- the frozen FX universe manifest (e.g. Dukascopy ``EURUSD``): price kind
  ``REFERENCE_MARKET_PRICE``. Research / context evidence only.
* ``EXECUTION`` rows -- the Section 7 venue instrument catalog (``venue_instruments``: e.g. Bybit
  ``EURUSDT`` FX perpetual, BingX ``NCFXEUR2USD-USDT``): price kind ``EXECUTABLE_VENUE_PRICE``, with
  availability from the venue's own listing / API state / venue-evidenced classification.

A reference row can never satisfy an execution lookup (different ``role`` and ``price_kind``): reference
price is never executable price. Deterministic (sorted) and content-hashed per mapping version.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Tuple

from app.trading_intelligence.hashing import stable_hash

FX_MAPPING_VERSION = "fx-reference-execution-mapping-v1"
REFERENCE, EXECUTION = "REFERENCE", "EXECUTION"
REFERENCE_MARKET_PRICE, EXECUTABLE_VENUE_PRICE = "REFERENCE_MARKET_PRICE", "EXECUTABLE_VENUE_PRICE"


@dataclass(frozen=True)
class FXMapping:
    canonical_pair: str
    role: str                 # REFERENCE | EXECUTION
    source: str               # reference provider or execution venue
    symbol: str               # provider / venue symbol
    product_type: str
    price_kind: str
    availability: str         # AVAILABLE | UNAVAILABLE
    reason: Optional[str] = None
    environment: Optional[str] = None
    mapping_version: str = FX_MAPPING_VERSION

    @property
    def identity(self) -> Tuple[str, str, str, str, Optional[str]]:
        return (self.canonical_pair, self.role, self.source, self.symbol, self.environment)


def reference_mappings(fx_universe: Mapping[str, Any]) -> List[FXMapping]:
    provider = str(fx_universe.get("provider") or "").lower()
    kind = str(fx_universe.get("price_kind") or REFERENCE_MARKET_PRICE)
    out = [FXMapping(f"{m['base']}/{m['quote']}", REFERENCE, provider, m["pair"], "REFERENCE_QUOTE", kind, "AVAILABLE")
           for m in fx_universe.get("members") or ()]
    from app.market_data.fx_reference import split_pair

    for pair, why in sorted((fx_universe.get("excluded") or {}).items()):
        try:
            b, q = split_pair(pair)
        except ValueError:
            continue
        out.append(FXMapping(f"{b}/{q}", REFERENCE, provider, pair, "REFERENCE_QUOTE", kind, "UNAVAILABLE",
                             str(why)))
    return out


def execution_mappings(catalog: Any, venues: Sequence[Tuple[str, str]]) -> List[FXMapping]:
    from app.exchange.instruments import VENUE_EVIDENCED_SOURCES, fx_legs

    out = []
    for venue, env in venues:
        for ins in catalog.list(venue, env, asset_class="FX", tradable_only=False, include_delisted=True):
            b, q = fx_legs(ins.base_currency, ins.quote_currency)
            rec = catalog.record(venue, env, ins.venue_symbol) or {}
            reason = ("DELISTED" if rec.get("delisted_at_ms") is not None
                      else "NOT_API_TRADABLE" if not ins.api_tradable
                      else "CLASSIFICATION_NOT_VENUE_EVIDENCED" if ins.classification_source not in VENUE_EVIDENCED_SOURCES
                      else None)
            out.append(FXMapping(f"{b}/{q}", EXECUTION, venue, ins.venue_symbol, f"{ins.product_type}:{ins.contract_type}",
                                 EXECUTABLE_VENUE_PRICE, "UNAVAILABLE" if reason else "AVAILABLE", reason, env))
    return out


def fx_reference_execution_mappings(*, fx_universe: Mapping[str, Any], catalog: Any,
                                    venues: Sequence[Tuple[str, str]]) -> Tuple[Tuple[FXMapping, ...], str]:
    """(mappings sorted by identity, mapping hash). Raises on a duplicate identity."""
    rows = sorted(reference_mappings(fx_universe) + execution_mappings(catalog, venues), key=lambda m: m.identity)
    ids = [m.identity for m in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("DUPLICATE_FX_MAPPING_IDENTITY")
    return tuple(rows), stable_hash({"version": FX_MAPPING_VERSION, "rows": [asdict(m) for m in rows]})


def for_pair(mappings: Iterable[FXMapping], canonical_pair: str, *, role: str) -> List[FXMapping]:
    return [m for m in mappings if m.canonical_pair == canonical_pair and m.role == role]


__all__ = ["EXECUTABLE_VENUE_PRICE", "EXECUTION", "FXMapping", "FX_MAPPING_VERSION", "REFERENCE",
           "REFERENCE_MARKET_PRICE", "execution_mappings", "for_pair", "fx_reference_execution_mappings",
           "reference_mappings"]
