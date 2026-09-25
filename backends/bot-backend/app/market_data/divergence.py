"""Venue / reference divergence for exchange-listed FX perpetuals (Phase 4G).

A Bybit (or any venue) FX perpetual is NOT the OTC FX market. Divergence
compares the venue's own prices (mark, last, bid/ask: EXECUTION_VENUE_PRICE)
with the provider reference mid (REFERENCE_MARKET_PRICE) at the same
decision time. Any missing side is UNAVAILABLE with a reason -- never 0.
A USDT-quoted perpetual is compared against the USD pair as a proxy and
says so (``stablecoin_proxy``).
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

STALE_REFERENCE_MS = 5 * 60_000


@dataclass(frozen=True)
class Divergence:
    status: str                       # AVAILABLE | UNAVAILABLE
    reason: Optional[str]
    reference_mid: Optional[float]
    venue_mark: Optional[float]
    venue_last: Optional[float]
    venue_mid: Optional[float]
    mark_vs_reference_bps: Optional[float]
    mid_vs_reference_bps: Optional[float]
    venue_spread_bps: Optional[float]
    reference_spread_bps: Optional[float]
    reference_age_ms: Optional[int]
    stablecoin_proxy: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _bps(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b <= 0:
        return None
    return (a - b) / b * 10_000.0


def compute_divergence(*, as_of_ms: int, reference: Optional[Dict[str, Any]], venue_mark: Optional[float] = None,
                       venue_last: Optional[float] = None, venue_bid: Optional[float] = None,
                       venue_ask: Optional[float] = None, settlement_asset: str = "USDT") -> Divergence:
    stable = settlement_asset.upper() in ("USDT", "USDC")
    venue_mid = (venue_bid + venue_ask) / 2.0 if venue_bid and venue_ask else None
    venue_spread = _bps(venue_ask, venue_mid) * 2 if venue_mid and venue_ask else None
    if not reference:
        return Divergence("UNAVAILABLE", "REFERENCE_UNAVAILABLE", None, venue_mark, venue_last, venue_mid, None,
                          None, venue_spread, None, None, stable)
    ref_close_ms = int(reference["open_time"]) + 60_000 - 1 if reference.get("timeframe", "1m") == "1m" else None
    age = as_of_ms - (ref_close_ms or int(reference["open_time"]))
    ref_mid = reference.get("mid_close")
    bid, ask = reference.get("bid_close"), reference.get("ask_close")
    ref_spread = _bps(ask, ref_mid) * 2 if ref_mid and ask else None
    if ref_mid is None:
        return Divergence("UNAVAILABLE", "REFERENCE_MID_UNAVAILABLE", None, venue_mark, venue_last, venue_mid, None,
                          None, venue_spread, ref_spread, age, stable)
    if age > STALE_REFERENCE_MS:
        return Divergence("UNAVAILABLE", "REFERENCE_STALE", ref_mid, venue_mark, venue_last, venue_mid, None, None,
                          venue_spread, ref_spread, age, stable)
    mark_bps, mid_bps = _bps(venue_mark, ref_mid), _bps(venue_mid, ref_mid)
    if mark_bps is None and mid_bps is None:
        return Divergence("UNAVAILABLE", "VENUE_PRICE_UNAVAILABLE", ref_mid, venue_mark, venue_last, venue_mid, None,
                          None, venue_spread, ref_spread, age, stable)
    return Divergence("AVAILABLE", None, ref_mid, venue_mark, venue_last, venue_mid, mark_bps, mid_bps, venue_spread,
                      ref_spread, age, stable)


__all__ = ["Divergence", "STALE_REFERENCE_MS", "compute_divergence"]
