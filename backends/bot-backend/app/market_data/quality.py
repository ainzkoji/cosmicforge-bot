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

import math
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
    #: FX rows whose BID or ASK side is partial, non-positive or violates its own OHLC envelope
    invalid_quote_sides: int = 0

    @property
    def is_usable(self) -> bool:
        return bool(self.base.get("is_usable")) and not (
            self.misaligned_timestamps or self.before_listing or self.after_delisting
            or self.source_switches or self.venue_switches or self.impossible_spreads
            or self.invalid_quote_sides)

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


#: ``max_spread_bps`` for ingestion/derivation gates: only crossed quotes count, never a merely wide spread
STRUCTURAL_ONLY = float("inf")


def check_fx_quotes(quotes: Sequence[Mapping[str, Any]], *, pair: str, timeframe: str,
                    max_spread_bps: float = 200.0) -> ExtendedQualityReport:
    rows = []
    for q in quotes:
        # Use ONE complete OHLC side, never midpoint close with bid high/low.
        side = next((side for side in ("bid", "ask", "mid")
                     if all(q.get(f"{side}_{f}") is not None for f in ("open", "high", "low", "close"))), None)
        values = [q[f"{side}_{f}"] for f in ("open", "high", "low", "close")] if side else [float("nan")] * 4
        rows.append([q["open_time"], *values, q.get("volume") or 0,
                     q["open_time"] + MINUTE_MS * (DERIVABLE.get(timeframe, 1) if timeframe != "1m" else 1) - 1])
    base = check_series(rows, symbol=pair, timeframe=timeframe, sources=[q.get("provider") for q in quotes])
    crossed = invalid_sides = 0
    for q in quotes:
        # each side is validated independently; a side that is entirely absent is "unavailable", not invalid
        broken = False
        for side in ("bid", "ask"):
            values = [q.get(f"{side}_{f}") for f in ("open", "high", "low", "close")]
            if all(v is None for v in values):
                continue
            if any(v is None or not math.isfinite(float(v)) or float(v) <= 0 for v in values):
                broken = True
                continue
            o, h, lo, c = map(float, values)
            broken |= h < max(o, c, lo) or lo > min(o, c, h)
        invalid_sides += int(broken)
        # ask/bid consistency: every field present on both sides must satisfy ask >= bid
        if any(q.get(f"bid_{f}") is not None and q.get(f"ask_{f}") is not None
               and float(q[f"ask_{f}"]) < float(q[f"bid_{f}"]) for f in ("open", "high", "low", "close")):
            crossed += 1
            continue
        bid, ask = q.get("bid_close"), q.get("ask_close")
        if bid is None or ask is None:
            continue
        mid = (bid + ask) / 2.0
        if mid <= 0 or (ask - bid) / mid * 10_000 > max_spread_bps:
            crossed += 1
    return ExtendedQualityReport(**{**{k: getattr(base, k) for k in ("base", "misaligned_timestamps", "before_listing",
                                                                     "after_delisting", "source_switches", "venue_switches")},
                                    "impossible_spreads": crossed, "invalid_quote_sides": invalid_sides})


def _switches(values: Optional[Sequence[Optional[str]]]) -> int:
    if not values:
        return 0
    return sum(1 for a, b in zip(values, values[1:]) if a != b)


__all__ = ["ExtendedQualityReport", "STRUCTURAL_ONLY", "check_fx_quotes", "check_series"]


QUALITY_POLICY_VERSION = "multi-asset-quality-v2"


def audit_partition(rows, *, symbol, timeframe, start_ms, end_ms, source, venue=None,
                    acquiring=False, failed=False, gap_classifier=None, source_version=None):
    """Bounded-memory QA of one ordered partition, without silently sorting.

    Caller selects an exact provider/venue/product series (SQL identity); rows
    are consumed once. Content hash excludes operational acquisition status.
    Missing ranges are compressed runs. No whole-universe materialization.
    """
    import hashlib
    import json
    from app.market_data.gaps import UNKNOWN_GAP
    step = MINUTE_MS * (DERIVABLE.get(timeframe, 1) if timeframe != "1m" else 1)
    if not source or end_ms <= start_ms or start_ms % step or end_ms % step:
        raise ValueError("PARTITION_IDENTITY_INVALID")
    digest = hashlib.sha256()
    digest.update(json.dumps([symbol, timeframe, source, source_version, venue, start_ms, end_ms, QUALITY_POLICY_VERSION]).encode())
    count = invalid = duplicates = out_of_order = 0
    previous = actual_start = actual_end = None
    cursor = start_ms
    missing = []
    def gap(a, b):
        if b > a:
            missing.append({"start_ms": a, "end_ms": b, "missing_bars": (b-a)//step,
                            "reason": gap_classifier(a, b-step) if gap_classifier else UNKNOWN_GAP})
    for row in rows:
        t = open_time(row)
        if previous is not None and t <= previous:
            duplicates += int(t == previous)
            out_of_order += int(t < previous)
            continue
        previous = t
        if not start_ms <= t < end_ms:
            invalid += 1
            continue
        check = check_series([row], symbol=symbol, timeframe=timeframe)
        if not check.is_usable:
            invalid += 1
            continue
        gap(cursor, t)
        cursor = t + step
        actual_start = t if actual_start is None else actual_start
        actual_end = t + step
        count += 1
        digest.update(json.dumps(list(row)[:7], separators=(",", ":"), allow_nan=False).encode())
    gap(cursor, end_ms)
    expected = (end_ms-start_ms)//step - sum(g["missing_bars"] for g in missing
        if g["reason"] in ("WEEKEND_CLOSED", "HOLIDAY_CLOSED", "SESSION_CLOSED", "LISTING_AGE"))
    status = "FAILED" if failed else "ACQUIRING" if acquiring else (
        "COMPLETE" if count == expected and not (invalid or duplicates or out_of_order) else "PARTIAL")
    return {"symbol": symbol, "timeframe": timeframe, "source": source, "venue": venue,
            "requested_start_ms": start_ms, "requested_end_ms": end_ms,
            "actual_start_ms": actual_start, "actual_end_ms": actual_end,
            "rows": count, "expected_rows": expected, "coverage_pct": 100*count/expected if expected else None,
            "invalid_rows": invalid, "duplicate_rows": duplicates, "out_of_order": out_of_order,
            "missing_ranges": missing, "status": status, "partition_hash": digest.hexdigest(),
            "quality_policy_version": QUALITY_POLICY_VERSION, "source_version": source_version}
