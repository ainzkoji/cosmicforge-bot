"""Reference market != execution instrument (mandatory distinction).

A long-history reference series (e.g. Dukascopy EUR/USD bid/ask) is where
CATI may LEARN market behaviour; the venue product (e.g. Bybit
``EURUSDUSDT``, a USDT-settled linear perpetual with funding) is what is
TRADED, with its own price, spread, funding and settlement. They are linked,
never equated:

* ``reference_market_for(ins)``  -> the ReferenceMarket a venue instrument tracks
  (FX pair, precious metal) or None (a crypto perpetual is its own market; a
  cross-venue crypto reference is not assumed);
* ``ExecutionReferenceLink``     -> venue, contract type, settlement, funding and
  the tracking basis between the two;
* ``tracking_statistics(...)``   -> historical venue-vs-reference divergence over
  CLOSED, time-aligned bars (median / p95 |bps|, coverage). Too little overlap
  -> UNAVAILABLE with a reason, never 0.

Execution economics always come from the venue product; a reference price is
never used as a fill or cost assumption.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from statistics import median
from typing import Any, Dict, Mapping, Optional, Sequence

MAPPING_VERSION = "reference-mapping-v1"
_PROVIDER_PAIRS_METALS = {"XAU": "XAUUSD", "XAG": "XAGUSD"}


@dataclass(frozen=True)
class ReferenceMarket:
    reference_market_id: str        # FX:EUR/USD | COMMODITY:XAU/USD
    asset_class: str
    base: str
    quote: str
    provider_pair: str              # the reference provider's own key (Dukascopy EURUSD)
    kind: str                       # SPOT_OTC_REFERENCE

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionReferenceLink:
    reference_market_id: str
    execution_instrument_id: str    # venue:venue_symbol
    canonical_instrument_id: str
    venue: str
    venue_symbol: str
    contract_type: str
    settlement_asset: str
    funding_interval_minutes: Optional[int]
    tracking_basis: str
    stablecoin_settlement_proxy: bool
    version: str = MAPPING_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def reference_market_for(ins: Any) -> Optional[ReferenceMarket]:
    ac = getattr(ins, "asset_class", None)
    base, quote = str(ins.base_currency).upper(), str(ins.quote_currency).upper()
    if ac == "FX" and len(base) == 3 and len(quote) == 3:
        return ReferenceMarket(f"FX:{base}/{quote}", "FX", base, quote, f"{base}{quote}", "SPOT_OTC_REFERENCE")
    if ac == "COMMODITIES" and base in _PROVIDER_PAIRS_METALS:
        return ReferenceMarket(f"COMMODITY:{base}/USD", "COMMODITIES", base, "USD", _PROVIDER_PAIRS_METALS[base],
                               "SPOT_OTC_REFERENCE")
    return None


def link_for(ins: Any) -> Optional[ExecutionReferenceLink]:
    ref = reference_market_for(ins)
    if ref is None:
        return None
    perp = str(ins.contract_type).upper() == "PERPETUAL"
    settle = str(ins.settlement_asset).upper()
    return ExecutionReferenceLink(
        reference_market_id=ref.reference_market_id, execution_instrument_id=f"{ins.venue}:{ins.venue_symbol}",
        canonical_instrument_id=ins.canonical_symbol, venue=ins.venue, venue_symbol=ins.venue_symbol,
        contract_type=str(ins.contract_type), settlement_asset=settle,
        funding_interval_minutes=ins.funding_interval_minutes,
        tracking_basis=("PERPETUAL_WITH_FUNDING_VS_OTC_SPOT" if perp else "DELIVERY_VS_OTC_SPOT"),
        stablecoin_settlement_proxy=settle in ("USDT", "USDC"))


@dataclass(frozen=True)
class TrackingStatistics:
    status: str                     # AVAILABLE | UNAVAILABLE
    reason: Optional[str]
    aligned_bars: int
    venue_bars: int
    reference_bars: int
    median_abs_bps: Optional[float]
    p95_abs_bps: Optional[float]
    mean_bps: Optional[float]
    max_abs_bps: Optional[float]
    timeframe: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def tracking_statistics(venue_bars: Sequence[Mapping[str, Any]], reference_bars: Sequence[Mapping[str, Any]], *,
                        timeframe: str, min_aligned: int = 30) -> TrackingStatistics:
    """``venue_bars``: {open_time, close}; ``reference_bars``: {open_time, mid_close} (both CLOSED bars).

    Only bars present in BOTH series at the same open_time are compared (no
    interpolation, no forward fill): a closed FX market is a gap, not a price."""
    ref = {int(r["open_time"]): r.get("mid_close") for r in reference_bars if r.get("mid_close") is not None}
    diffs = []
    for v in venue_bars:
        t = int(v["open_time"])
        m, c = ref.get(t), v.get("close")
        if m is None or c is None or m <= 0:
            continue
        diffs.append((float(c) - float(m)) / float(m) * 10_000.0)
    if len(diffs) < min_aligned:
        return TrackingStatistics("UNAVAILABLE", f"INSUFFICIENT_ALIGNED_BARS_{len(diffs)}_LT_{min_aligned}",
                                  len(diffs), len(venue_bars), len(reference_bars), None, None, None, None, timeframe)
    absd = sorted(abs(d) for d in diffs)
    p95 = absd[min(len(absd) - 1, int(round(0.95 * (len(absd) - 1))))]
    return TrackingStatistics("AVAILABLE", None, len(diffs), len(venue_bars), len(reference_bars),
                              round(float(median(absd)), 4), round(float(p95), 4),
                              round(sum(diffs) / len(diffs), 4), round(absd[-1], 4), timeframe)


__all__ = ["ExecutionReferenceLink", "MAPPING_VERSION", "ReferenceMarket", "TrackingStatistics", "link_for",
           "reference_market_for", "tracking_statistics"]
