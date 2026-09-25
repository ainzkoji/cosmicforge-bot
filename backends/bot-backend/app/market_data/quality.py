"""Dataset quality checks (Phase 4I) on top of ``research.dataset.assess_quality``.

``assess_quality`` already covers chronology, duplicates, gaps, bad OHLC,
non-positive prices and negative volume. This adds what multi-venue / FX
data needs:

* timestamp alignment        every open_time is a multiple of the timeframe
* listing / delisting bounds no bar before the venue listing or after delisting
* source continuity          one source per series (a silent source switch is
                             a different series)
* venue continuity           one venue per series
* impossible spreads         FX: ask < bid, or spread wider than a bound
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence

from app.research.dataset import DERIVABLE, MINUTE_MS, assess_quality, open_time


@dataclass(frozen=True)
class ExtendedQualityReport:
    base: Mapping[str, Any]
    misaligned_timestamps: int
    before_listing: int
    after_delisting: int
    source_switches: int
    venue_switches: int
    impossible_spreads: int
    notes: tuple = ()

    @property
    def is_usable(self) -> bool:
        return bool(self.base.get("is_usable")) and not (
            self.misaligned_timestamps or self.before_listing or self.after_delisting
            or self.source_switches or self.venue_switches or self.impossible_spreads)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["base"] = dict(self.base)
        d["notes"] = list(self.notes)
        d["is_usable"] = self.is_usable
        return d


def check_series(rows: Sequence[Any], *, symbol: str, timeframe: str, sources: Optional[Sequence[str]] = None,
                 venues: Optional[Sequence[str]] = None, listed_at_ms: Optional[int] = None,
                 delisted_at_ms: Optional[int] = None) -> ExtendedQualityReport:
    base = assess_quality(rows, symbol=symbol, timeframe=timeframe).to_dict()
    step = MINUTE_MS * (DERIVABLE.get(timeframe, 1) if timeframe != "1m" else 1)
    misaligned = sum(1 for r in rows if open_time(r) % step)
    before = sum(1 for r in rows if listed_at_ms is not None and open_time(r) < listed_at_ms)
    after = sum(1 for r in rows if delisted_at_ms is not None and open_time(r) >= delisted_at_ms)
    return ExtendedQualityReport(base=base, misaligned_timestamps=misaligned, before_listing=before,
                                 after_delisting=after, source_switches=_switches(sources),
                                 venue_switches=_switches(venues), impossible_spreads=0)


def check_fx_quotes(quotes: Sequence[Mapping[str, Any]], *, pair: str, timeframe: str,
                    max_spread_bps: float = 200.0) -> ExtendedQualityReport:
    rows = [[q["open_time"], q.get("mid_open") or q.get("bid_open"), q.get("mid_high") or q.get("bid_high"),
             q.get("mid_low") or q.get("bid_low"), q.get("mid_close") or q.get("bid_close"), q.get("volume") or 0,
             q["open_time"] + MINUTE_MS * (DERIVABLE.get(timeframe, 1) if timeframe != "1m" else 1) - 1]
            for q in quotes]
    base = check_series(rows, symbol=pair, timeframe=timeframe, sources=[q.get("provider") for q in quotes])
    bad = 0
    for q in quotes:
        bid, ask = q.get("bid_close"), q.get("ask_close")
        if bid is None or ask is None:
            continue
        mid = (bid + ask) / 2.0
        if ask < bid or mid <= 0 or (ask - bid) / mid * 10_000 > max_spread_bps:
            bad += 1
    return ExtendedQualityReport(**{**{k: getattr(base, k) for k in ("base", "misaligned_timestamps", "before_listing",
                                                                     "after_delisting", "source_switches", "venue_switches")},
                                    "impossible_spreads": bad})


def _switches(values: Optional[Sequence[Optional[str]]]) -> int:
    if not values:
        return 0
    return sum(1 for a, b in zip(values, values[1:]) if a != b)


__all__ = ["ExtendedQualityReport", "check_fx_quotes", "check_series"]
