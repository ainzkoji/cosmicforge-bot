"""Deterministic gap detection and classification (Section 11.10, 12.7/12.8).

A gap is a run of expected bar timestamps with no stored bar. It is never
filled; it is CLASSIFIED from evidence, and without evidence it stays
``UNKNOWN_GAP`` (fail-closed: an unexplained gap is a data-quality finding).

FX (reference market, UTC timestamps; DST handled through New York time)
  WEEKEND_CLOSED           inside the weekly close: Friday 17:00 -> Sunday 17:00
                           America/New_York (21:00/22:00 UTC depending on US DST)
  HOLIDAY_CLOSED           a UTC date in the governed holiday list (Dec 25, Jan 1)
  SESSION_CLOSED           the daily rollover break 16:55-17:15 America/New_York
  PROVIDER_NO_FILE         the provider period is NO_FILE in the ingest log
  PROVIDER_OUTAGE          the provider period is FAILED / quarantined, or EMPTY on a
                           market-open day
  UNKNOWN_GAP              none of the above

Crypto (24/7 venue market)
  LISTING_AGE              before the instrument's listing/onboard time
  PROVIDER_FAILURE         the acquisition period is FAILED in the ingest log
  VENUE_OUTAGE / MARKET_HALT  only when supplied as explicit evidence windows
  UNKNOWN_GAP              none of the above

All timestamps are UTC epoch ms; local time is used only to evaluate the New
York session rule, so DST can neither create nor remove an hour.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

GAP_POLICY_VERSION = "gap-classification-v2"
MINUTE_MS = 60_000
NY = ZoneInfo("America/New_York")

WEEKEND_CLOSED = "WEEKEND_CLOSED"
HOLIDAY_CLOSED = "HOLIDAY_CLOSED"
SESSION_CLOSED = "SESSION_CLOSED"
PROVIDER_NO_FILE = "PROVIDER_NO_FILE"
PROVIDER_OUTAGE = "PROVIDER_OUTAGE"
LISTING_AGE = "LISTING_AGE"
PROVIDER_FAILURE = "PROVIDER_FAILURE"
INGEST_FAILURE = "INGEST_FAILURE"
VENUE_OUTAGE = "VENUE_OUTAGE"
MARKET_HALT = "MARKET_HALT"
UNKNOWN_GAP = "UNKNOWN_GAP"
EXPECTED_CLOSURES = frozenset({WEEKEND_CLOSED, HOLIDAY_CLOSED, SESSION_CLOSED})

#: governed FX holiday calendar: (month, day) the interbank market is closed. Versioned with the policy.
FX_HOLIDAYS = ((12, 25), (1, 1))


def _ny(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, timezone.utc).astimezone(NY)


def fx_weekend_closed(ts_ms: int) -> bool:
    """Inside the weekly FX close (Fri 17:00 -> Sun 17:00 New York time)."""
    t = _ny(ts_ms)
    wd, minutes = t.weekday(), t.hour * 60 + t.minute
    return (wd == 4 and minutes >= 17 * 60) or wd == 5 or (wd == 6 and minutes < 17 * 60)


def fx_rollover_break(ts_ms: int) -> bool:
    t = _ny(ts_ms)
    minutes = t.hour * 60 + t.minute
    return 16 * 60 + 55 <= minutes < 17 * 60 + 15


def fx_holiday(ts_ms: int) -> bool:
    d = datetime.fromtimestamp(ts_ms / 1000, timezone.utc)
    return (d.month, d.day) in FX_HOLIDAYS


def expected_fx_bar(ts_ms: int) -> bool:
    """A market-open bar is expected at ``ts_ms`` (no weekend, holiday or rollover closure)."""
    return not (fx_weekend_closed(ts_ms) or fx_holiday(ts_ms) or fx_rollover_break(ts_ms))


@dataclass(frozen=True)
class Gap:
    start_ms: int           # first missing bar open time
    end_ms: int             # last missing bar open time
    missing: int
    classification: str

    def to_dict(self) -> Dict[str, Any]:
        return {"start_ms": self.start_ms, "end_ms": self.end_ms, "missing_bars": self.missing,
                "classification": self.classification}


def find_gaps(open_times: Iterable[int], step_ms: int, *, start_ms: Optional[int] = None,
              end_ms: Optional[int] = None) -> List[Tuple[int, int, int]]:
    """(first_missing, last_missing, count) runs inside [start, end) given sorted open times (streamed)."""
    out: List[Tuple[int, int, int]] = []
    prev = None if start_ms is None else start_ms - step_ms
    for t in open_times:
        if prev is not None and t - prev > step_ms:
            out.append((prev + step_ms, t - step_ms, (t - prev) // step_ms - 1))
        prev = t
    if end_ms is not None:
        last = end_ms - step_ms
        if prev is None:
            if start_ms is not None and end_ms > start_ms:
                out.append((start_ms, last, (end_ms - start_ms) // step_ms))
        elif last > prev:
            out.append((prev + step_ms, last, (last - prev) // step_ms))
    return out


def _period_of(ts_ms: int, period_kind: str) -> str:
    d = datetime.fromtimestamp(ts_ms / 1000, timezone.utc)
    return d.strftime("%Y-%m") if period_kind == "month" else d.date().isoformat()


def classify_fx_gap(first_ms: int, last_ms: int, step_ms: int, *,
                    ingest_status: Optional[Mapping[str, str]] = None, period_kind: str = "day") -> str:
    """Classify one missing run. ``ingest_status``: period -> worst side status in the ingest log."""
    status = ingest_status or {}
    ts = range(first_ms, last_ms + 1, step_ms)
    counts = {WEEKEND_CLOSED: 0, HOLIDAY_CLOSED: 0, SESSION_CLOSED: 0}
    states = set()
    for t in ts:
        if fx_weekend_closed(t):
            counts[WEEKEND_CLOSED] += 1
        elif fx_holiday(t):
            counts[HOLIDAY_CLOSED] += 1
        elif fx_rollover_break(t):
            counts[SESSION_CLOSED] += 1
        else:
            states.add(status.get(_period_of(t, period_kind)))
    if not states:
        return max(counts, key=counts.get)
    if states == {"NO_FILE"}:
        return PROVIDER_NO_FILE
    if states == {"PROVIDER_OUTAGE"}:
        return PROVIDER_OUTAGE
    if states <= {"FAILED", "QUARANTINED"}:
        return INGEST_FAILURE
    # An empty file or mixed/unknown period statuses do not establish an outage.
    return UNKNOWN_GAP


def classify_crypto_gap(first_ms: int, last_ms: int, *, listed_at_ms: Optional[int] = None,
                        failed_periods: Iterable[Tuple[int, int]] = (),
                        evidence: Mapping[str, Sequence[Tuple[int, int]]] = ()) -> str:
    """``evidence``: {VENUE_OUTAGE|MARKET_HALT: [(start, end), ...]} documented windows. No evidence -> UNKNOWN."""
    if listed_at_ms is not None and last_ms < listed_at_ms:
        return LISTING_AGE
    for a, b in failed_periods:
        if first_ms >= a and last_ms <= b:
            return PROVIDER_FAILURE
    for kind in (MARKET_HALT, VENUE_OUTAGE):
        for a, b in (dict(evidence) if evidence else {}).get(kind, ()):
            if first_ms >= a and last_ms <= b:
                return kind
    return UNKNOWN_GAP


def summarize(gaps: Sequence[Gap]) -> Dict[str, Any]:
    by: Dict[str, Dict[str, int]] = {}
    for g in gaps:
        s = by.setdefault(g.classification, {"gaps": 0, "missing_bars": 0})
        s["gaps"] += 1
        s["missing_bars"] += g.missing
    unexplained = sum(v["gaps"] for k, v in by.items() if k not in EXPECTED_CLOSURES)
    return {"by_classification": dict(sorted(by.items())), "unexpected_gaps": unexplained,
            "policy_version": GAP_POLICY_VERSION}


def fx_expected_bars(start_ms: int, end_ms: int, step_ms: int) -> int:
    """Market-open bars expected in [start, end) under the session policy (computable, deterministic)."""
    return sum(1 for t in range(start_ms, end_ms, step_ms) if expected_fx_bar(t))


__all__ = ["EXPECTED_CLOSURES", "FX_HOLIDAYS", "GAP_POLICY_VERSION", "Gap", "HOLIDAY_CLOSED", "LISTING_AGE",
           "MARKET_HALT", "PROVIDER_FAILURE", "PROVIDER_NO_FILE", "PROVIDER_OUTAGE", "SESSION_CLOSED", "UNKNOWN_GAP",
           "VENUE_OUTAGE", "WEEKEND_CLOSED", "classify_crypto_gap", "classify_fx_gap", "expected_fx_bar",
           "find_gaps", "fx_expected_bars", "fx_holiday", "fx_rollover_break", "fx_weekend_closed", "summarize"]
