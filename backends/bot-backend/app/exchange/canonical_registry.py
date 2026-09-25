"""Canonical instrument registry across venues (CanonicalInstrument <- VenueInstrument).

Built from what discovery returned (``DiscoveredInstrument``), never from a
symbol list. The canonical identity is venue-independent
(``EUR/USD:FX_PERPETUAL``, ``BTC/USDT:PERP``); every venue listing of it is a
``VenueInstrument`` with that venue's own symbol, filters, status and
API-execution flag -- so Bybit ``EURUSDUSDT`` and BingX ``NCFXEUR2USD-USDT``
are two venue instruments of ONE canonical FX perpetual, and both map to the
SAME reference market ``FX:EUR/USD`` (``reference_mapping``).

Duplicates are reported, never silently merged:

* the same (venue, venue_symbol) discovered twice with different
  classification -> ``CONFLICTING_CLASSIFICATION`` (both kept out of the
  registry until resolved);
* two venue symbols of one venue mapping to one canonical id -> kept, flagged
  ``MULTIPLE_VENUE_SYMBOLS`` (e.g. a relisted contract).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

REGISTRY_VERSION = "canonical-instrument-registry-v1"


@dataclass(frozen=True)
class VenueInstrument:
    venue: str
    venue_symbol: str
    canonical_instrument_id: str
    asset_class: str
    product_type: str
    status: str
    api_market_data_supported: bool
    api_order_supported: bool
    tick_size: Optional[float]
    qty_step: Optional[float]
    min_qty: Optional[float]
    min_notional: Optional[float]
    max_qty: Optional[float]
    max_leverage: Optional[float]
    settlement_asset: str
    funding_interval_minutes: Optional[int]
    listed_at_ms: Optional[int]
    session_restricted: Optional[bool]
    classification_source: str
    metadata_version: str

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class CanonicalInstrument:
    canonical_instrument_id: str
    asset_class: str
    base_asset: str
    quote_asset: str
    product_type: str
    settlement_assets: Tuple[str, ...]
    reference_market_id: Optional[str]
    venues: Tuple[VenueInstrument, ...]
    flags: Tuple[str, ...] = ()

    @property
    def status(self) -> str:
        return "TRADING" if any(v.api_order_supported for v in self.venues) else "NOT_API_TRADABLE"

    def to_dict(self) -> Dict[str, Any]:
        return {"canonical_instrument_id": self.canonical_instrument_id, "asset_class": self.asset_class,
                "base_asset": self.base_asset, "quote_asset": self.quote_asset, "product_type": self.product_type,
                "settlement_assets": list(self.settlement_assets), "reference_market_id": self.reference_market_id,
                "status": self.status, "flags": list(self.flags), "venues": [v.to_dict() for v in self.venues]}


def _metadata_version(ins: Any) -> str:
    raw = ins.venue_metadata.get("raw_filters") if isinstance(ins.venue_metadata, Mapping) else None
    body = {"tick": ins.tick_size, "step": ins.qty_step, "min_qty": ins.min_qty, "min_notional": ins.min_notional,
            "max_qty": ins.max_qty, "lev": ins.max_leverage, "status": ins.status, "raw": raw}
    return hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()[:16]


def venue_instrument(ins: Any) -> VenueInstrument:
    return VenueInstrument(
        venue=ins.venue, venue_symbol=ins.venue_symbol, canonical_instrument_id=ins.canonical_symbol,
        asset_class=ins.asset_class, product_type=ins.product_type, status=ins.status,
        api_market_data_supported=True,  # it was discovered through the venue's public market API
        api_order_supported=bool(ins.api_tradable), tick_size=ins.tick_size, qty_step=ins.qty_step,
        min_qty=ins.min_qty, min_notional=ins.min_notional, max_qty=ins.max_qty, max_leverage=ins.max_leverage,
        settlement_asset=ins.settlement_asset, funding_interval_minutes=ins.funding_interval_minutes,
        listed_at_ms=ins.listed_at_ms, session_restricted=ins.session_restricted,
        classification_source=ins.classification_source, metadata_version=_metadata_version(ins))


@dataclass(frozen=True)
class RegistryBuild:
    instruments: Mapping[str, CanonicalInstrument]
    conflicts: Mapping[str, Tuple[str, ...]]      # "venue:symbol" -> the conflicting canonical ids
    version: str = REGISTRY_VERSION

    @property
    def registry_hash(self) -> str:
        body = {k: v.to_dict() for k, v in sorted(self.instruments.items())}
        return hashlib.sha256(json.dumps({"i": body, "c": {k: list(v) for k, v in sorted(self.conflicts.items())},
                                          "v": self.version}, sort_keys=True, default=str).encode()).hexdigest()

    def by_venue_symbol(self, venue: str, venue_symbol: str) -> Optional[CanonicalInstrument]:
        for c in self.instruments.values():
            if any(v.venue == venue and v.venue_symbol == venue_symbol for v in c.venues):
                return c
        return None

    def summary(self) -> Dict[str, Any]:
        per_class: Dict[str, int] = {}
        for c in self.instruments.values():
            per_class[c.asset_class] = per_class.get(c.asset_class, 0) + 1
        multi = sum(1 for c in self.instruments.values() if len({v.venue for v in c.venues}) > 1)
        return {"canonical_instruments": len(self.instruments), "by_asset_class": dict(sorted(per_class.items())),
                "listed_on_multiple_venues": multi, "conflicts": len(self.conflicts),
                "registry_hash": self.registry_hash}


def build_registry(discovered: Iterable[Any]) -> RegistryBuild:
    from app.market_data.reference_mapping import reference_market_for

    seen: Dict[Tuple[str, str], Any] = {}
    conflicts: Dict[str, set] = {}
    for ins in discovered:
        key = (ins.venue, ins.venue_symbol)
        prev = seen.get(key)
        if prev is not None and (prev.canonical_symbol, prev.asset_class) != (ins.canonical_symbol, ins.asset_class):
            conflicts.setdefault(f"{ins.venue}:{ins.venue_symbol}", set()).update(
                {prev.canonical_symbol, ins.canonical_symbol})
        seen[key] = ins
    groups: Dict[str, List[Any]] = {}
    for key, ins in seen.items():
        if f"{key[0]}:{key[1]}" in conflicts:
            continue
        groups.setdefault(ins.canonical_symbol, []).append(ins)
    out: Dict[str, CanonicalInstrument] = {}
    for cid, members in sorted(groups.items()):
        members.sort(key=lambda i: (i.venue, i.venue_symbol))
        first = members[0]
        per_venue: Dict[str, int] = {}
        for m in members:
            per_venue[m.venue] = per_venue.get(m.venue, 0) + 1
        flags = tuple(sorted(f"MULTIPLE_VENUE_SYMBOLS:{v}" for v, n in per_venue.items() if n > 1))
        ref = reference_market_for(first)
        out[cid] = CanonicalInstrument(
            canonical_instrument_id=cid, asset_class=first.asset_class, base_asset=first.base_currency,
            quote_asset=first.quote_currency, product_type=first.product_type,
            settlement_assets=tuple(sorted({m.settlement_asset for m in members})),
            reference_market_id=ref.reference_market_id if ref else None,
            venues=tuple(venue_instrument(m) for m in members), flags=flags)
    return RegistryBuild(out, {k: tuple(sorted(v)) for k, v in sorted(conflicts.items())})


__all__ = ["CanonicalInstrument", "REGISTRY_VERSION", "RegistryBuild", "VenueInstrument", "build_registry",
           "venue_instrument"]
