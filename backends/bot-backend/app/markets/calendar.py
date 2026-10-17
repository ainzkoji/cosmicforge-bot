"""Venue-local trading calendars (Step 7.2).

A calendar is data, never code: weekly session templates in the venue's own time zone, holidays, early closes
and maintenance windows, loaded from a versioned definition (``registry``). This module only evaluates it.

Conventions (stated once, applied everywhere):

* Times are epoch milliseconds (UTC). Session times in a definition are venue-local wall-clock times and are
  converted through ``zoneinfo``, so a session that opens at 17:00 Chicago time opens at a different UTC time
  in summer and in winter. A local session time that does not exist on a date (inside a daylight-saving gap)
  is a definition error, never silently shifted.
* A session may span midnight (and a weekend): it opens on one local weekday and closes on a later one. Its
  **trading date** is the local date on which it closes.
* A **holiday** is a trading date with no session. An **early close** replaces the close time of the session
  with that trading date. A **maintenance window** is a local daily interval during which the market is closed
  inside a session.
* Intervals are half-open: open at ``open_ms``, closed at ``close_ms``.
* A calendar with ``coverage`` only answers for dates inside it: holidays outside the period the data covers
  are unknown, and an unknown is an error (``CalendarCoverageError``), never "no holiday".
* ``CONTINUOUS`` calendars (crypto perpetuals) are always open. Their daily bar is the UTC day.

``expected_bars`` never fabricates a bar for a closed period: an intraday bar is expected only when its
interval overlaps an open interval, and a daily bar exists only for a trading date that has a session.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from zoneinfo import ZoneInfo

CONTINUOUS, SESSIONS = "CONTINUOUS", "SESSIONS"
KINDS = (CONTINUOUS, SESSIONS)
VERIFIED, UNVERIFIED, PLATFORM_CONVENTION = "VERIFIED", "UNVERIFIED", "PLATFORM_CONVENTION"
VERIFICATION_STATUSES = (VERIFIED, UNVERIFIED, PLATFORM_CONVENTION)

MINUTE_MS = 60_000
DAY_MS = 86_400_000
TIMEFRAME_MS: Mapping[str, int] = {"1m": MINUTE_MS, "5m": 5 * MINUTE_MS, "15m": 15 * MINUTE_MS,
                                   "30m": 30 * MINUTE_MS, "1h": 60 * MINUTE_MS, "4h": 240 * MINUTE_MS, "1d": DAY_MS}
#: how far ``next_open`` / ``next_close`` look before giving up (a calendar with no session in this window is
#: either wrong or closed for longer than any real market)
SEARCH_HORIZON_DAYS = 21


class CalendarDefinitionError(ValueError):
    """The definition is not a valid calendar."""


class CalendarCoverageError(LookupError):
    """The question concerns a date the calendar's holiday data does not cover."""


def _hhmm(text: str, name: str) -> dtime:
    try:
        hours, minutes = str(text).split(":")
        out = dtime(int(hours), int(minutes))
    except Exception as exc:  # noqa: BLE001
        raise CalendarDefinitionError(f"{name}: expected HH:MM, got {text!r}") from exc
    return out


def _date(text: str, name: str) -> date:
    try:
        return date.fromisoformat(str(text))
    except Exception as exc:  # noqa: BLE001
        raise CalendarDefinitionError(f"{name}: expected YYYY-MM-DD, got {text!r}") from exc


def _weekday(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= 6:
        raise CalendarDefinitionError(f"{name}: weekday is 0 (Monday) to 6 (Sunday), got {value!r}")
    return value


@dataclass(frozen=True)
class WeeklySession:
    open_weekday: int
    open_time: dtime
    close_weekday: int
    close_time: dtime

    @property
    def day_span(self) -> int:
        span = (self.close_weekday - self.open_weekday) % 7
        if span == 0 and self.close_time <= self.open_time:
            raise CalendarDefinitionError("a session that closes before it opens on the same weekday is not allowed; "
                                          "a week-long session is not a session")
        return span


@dataclass(frozen=True)
class MaintenanceWindow:
    weekdays: Tuple[int, ...]
    start: dtime
    end: dtime


@dataclass(frozen=True)
class Session:
    trading_date: date
    open_ms: int
    close_ms: int
    early_close: bool = False

    def contains(self, ts_ms: int) -> bool:
        return self.open_ms <= ts_ms < self.close_ms

    def to_plain(self) -> Dict[str, Any]:
        return {"trading_date": self.trading_date.isoformat(), "open_ms": self.open_ms, "close_ms": self.close_ms,
                "early_close": self.early_close}


@dataclass(frozen=True)
class MarketCalendar:
    calendar_id: str
    version: str
    kind: str
    timezone: str
    source: str
    verification_status: str
    weekly_sessions: Tuple[WeeklySession, ...] = ()
    holidays: frozenset = field(default_factory=frozenset)
    early_closes: Mapping[date, dtime] = field(default_factory=dict)
    maintenance: Tuple[MaintenanceWindow, ...] = ()
    coverage: Optional[Tuple[date, date]] = None
    description: str = ""

    def __post_init__(self) -> None:
        if self.kind not in KINDS:
            raise CalendarDefinitionError(f"{self.calendar_id}: kind must be one of {KINDS}")
        if self.verification_status not in VERIFICATION_STATUSES:
            raise CalendarDefinitionError(f"{self.calendar_id}: verification_status must be one of {VERIFICATION_STATUSES}")
        if not str(self.source or "").strip():
            raise CalendarDefinitionError(f"{self.calendar_id}: every calendar names its source")
        try:
            ZoneInfo(self.timezone)
        except Exception as exc:  # noqa: BLE001
            raise CalendarDefinitionError(f"{self.calendar_id}: unknown time zone {self.timezone!r}") from exc
        if self.kind == SESSIONS and not self.weekly_sessions:
            raise CalendarDefinitionError(f"{self.calendar_id}: a SESSIONS calendar needs at least one weekly session")
        if self.kind == CONTINUOUS and (self.weekly_sessions or self.holidays or self.early_closes or self.maintenance):
            raise CalendarDefinitionError(f"{self.calendar_id}: a CONTINUOUS calendar has no sessions, holidays or breaks")
        for s in self.weekly_sessions:
            s.day_span  # noqa: B018 -- validates
        for m in self.maintenance:
            if m.end <= m.start:
                raise CalendarDefinitionError(f"{self.calendar_id}: a maintenance window ends after it starts, on one day")
        if self.coverage and self.coverage[1] < self.coverage[0]:
            raise CalendarDefinitionError(f"{self.calendar_id}: coverage ends before it starts")

    # -------------------------------------------------------------------------------------- construction
    @classmethod
    def from_plain(cls, d: Mapping[str, Any]) -> "MarketCalendar":
        cid = str(d.get("calendar_id") or "")
        if not cid:
            raise CalendarDefinitionError("calendar_id is required")
        sessions = tuple(WeeklySession(_weekday(s.get("open_weekday"), f"{cid}.open_weekday"),
                                       _hhmm(s.get("open"), f"{cid}.open"),
                                       _weekday(s.get("close_weekday"), f"{cid}.close_weekday"),
                                       _hhmm(s.get("close"), f"{cid}.close"))
                         for s in d.get("weekly_sessions") or ())
        maintenance = tuple(MaintenanceWindow(tuple(_weekday(w, f"{cid}.maintenance") for w in m.get("weekdays") or ()),
                                              _hhmm(m.get("start"), f"{cid}.maintenance.start"),
                                              _hhmm(m.get("end"), f"{cid}.maintenance.end"))
                            for m in d.get("maintenance_windows") or ())
        coverage = d.get("coverage")
        return cls(calendar_id=cid, version=str(d.get("version") or ""), kind=str(d.get("kind") or ""),
                   timezone=str(d.get("timezone") or "UTC"), source=str(d.get("source") or ""),
                   verification_status=str(d.get("verification_status") or ""), weekly_sessions=sessions,
                   holidays=frozenset(_date(h, f"{cid}.holidays") for h in d.get("holidays") or ()),
                   early_closes={_date(k, f"{cid}.early_closes"): _hhmm(v, f"{cid}.early_closes")
                                 for k, v in (d.get("early_closes") or {}).items()},
                   maintenance=maintenance,
                   coverage=(_date(coverage["from"], f"{cid}.coverage"), _date(coverage["to"], f"{cid}.coverage"))
                   if coverage else None,
                   description=str(d.get("description") or ""))

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)

    @property
    def is_continuous(self) -> bool:
        return self.kind == CONTINUOUS

    # ------------------------------------------------------------------------------------------ helpers
    def _local_ms(self, day: date, at: dtime) -> int:
        local = datetime.combine(day, at, tzinfo=self.tz)
        utc = local.astimezone(timezone.utc)
        if utc.astimezone(self.tz).replace(tzinfo=None) != local.replace(tzinfo=None):
            raise CalendarDefinitionError(
                f"{self.calendar_id}: {day.isoformat()} {at.isoformat('minutes')} does not exist in {self.timezone} "
                "(daylight-saving gap)")
        return int(utc.timestamp() * 1000)

    def _local_date(self, ts_ms: int) -> date:
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(self.tz).date()

    def _check_coverage(self, first: date, last: date) -> None:
        if self.coverage is None:
            return
        if first < self.coverage[0] or last > self.coverage[1]:
            raise CalendarCoverageError(
                f"{self.calendar_id}: holiday data covers {self.coverage[0]} to {self.coverage[1]}, "
                f"asked about {first} to {last}")

    def sessions_between(self, start_ms: int, end_ms: int) -> List[Session]:
        """Every session that overlaps [start_ms, end_ms), ordered by open. Maintenance is not cut out here."""
        if end_ms <= start_ms:
            return []
        if self.is_continuous:
            first = start_ms - start_ms % DAY_MS
            return [Session(datetime.fromtimestamp(t / 1000, tz=timezone.utc).date(), t, t + DAY_MS)
                    for t in range(first, end_ms, DAY_MS)]
        self._check_coverage(self._local_date(start_ms), self._local_date(end_ms - 1))
        span = max(s.day_span for s in self.weekly_sessions)
        first_day = self._local_date(start_ms) - timedelta(days=span + 1)
        last_day = self._local_date(end_ms) + timedelta(days=1)
        out: List[Session] = []
        day = first_day
        while day <= last_day:
            for template in self.weekly_sessions:
                if template.open_weekday != day.weekday():
                    continue
                trading_date = day + timedelta(days=template.day_span)
                open_ms = self._local_ms(day, template.open_time)
                early = self.early_closes.get(trading_date)
                close_ms = self._local_ms(trading_date, early or template.close_time)
                if close_ms <= start_ms or open_ms >= end_ms:
                    continue
                self._check_coverage(trading_date, trading_date)
                if trading_date in self.holidays:
                    continue
                if early is not None and close_ms <= open_ms:
                    raise CalendarDefinitionError(f"{self.calendar_id}: early close on {trading_date} is before the open")
                out.append(Session(trading_date, open_ms, close_ms, early_close=early is not None))
            day += timedelta(days=1)
        return sorted(out, key=lambda s: s.open_ms)

    def _maintenance_between(self, start_ms: int, end_ms: int) -> List[Tuple[int, int]]:
        if not self.maintenance:
            return []
        out = []
        day = self._local_date(start_ms) - timedelta(days=1)
        last = self._local_date(end_ms) + timedelta(days=1)
        while day <= last:
            for m in self.maintenance:
                if day.weekday() in m.weekdays:
                    s, e = self._local_ms(day, m.start), self._local_ms(day, m.end)
                    if e > start_ms and s < end_ms:
                        out.append((s, e))
            day += timedelta(days=1)
        return sorted(out)

    def open_intervals(self, start_ms: int, end_ms: int) -> List[Tuple[int, int]]:
        """The periods in [start_ms, end_ms) during which the market is open: sessions minus maintenance,
        merged and clipped."""
        if end_ms <= start_ms:
            return []
        if self.is_continuous:
            return [(start_ms, end_ms)]
        intervals = [(max(s.open_ms, start_ms), min(s.close_ms, end_ms)) for s in self.sessions_between(start_ms, end_ms)]
        for ms, me in self._maintenance_between(start_ms, end_ms):
            cut: List[Tuple[int, int]] = []
            for s, e in intervals:
                if me <= s or ms >= e:
                    cut.append((s, e))
                    continue
                if s < ms:
                    cut.append((s, ms))
                if me < e:
                    cut.append((me, e))
            intervals = cut
        merged: List[Tuple[int, int]] = []
        for s, e in sorted(i for i in intervals if i[1] > i[0]):
            if merged and s <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], e))
            else:
                merged.append((s, e))
        return merged

    # ------------------------------------------------------------------------------------- the questions
    def is_open(self, ts_ms: int) -> bool:
        ts_ms = int(ts_ms)
        return bool(self.open_intervals(ts_ms, ts_ms + 1))

    def session_for(self, ts_ms: int) -> Optional[Session]:
        """The session ``ts_ms`` falls in (also during a maintenance break inside it), or None when the market
        is between sessions."""
        ts_ms = int(ts_ms)
        for s in self.sessions_between(ts_ms, ts_ms + 1):
            if s.contains(ts_ms):
                return s
        return None

    def next_open(self, ts_ms: int, *, horizon_days: int = SEARCH_HORIZON_DAYS) -> int:
        """The earliest instant at or after ``ts_ms`` at which the market is open."""
        ts_ms = int(ts_ms)
        intervals = self.open_intervals(ts_ms, ts_ms + horizon_days * DAY_MS)
        if not intervals:
            raise CalendarCoverageError(f"{self.calendar_id}: no open period within {horizon_days} days of {ts_ms}")
        return intervals[0][0]

    def next_close(self, ts_ms: int, *, horizon_days: int = SEARCH_HORIZON_DAYS) -> Optional[int]:
        """When the current open period ends (None for a continuous market or when the market is closed)."""
        if self.is_continuous or not self.is_open(ts_ms):
            return None
        intervals = self.open_intervals(int(ts_ms), int(ts_ms) + horizon_days * DAY_MS)
        end = intervals[0][1]
        return end if end < int(ts_ms) + horizon_days * DAY_MS else None

    def expected_bars(self, start_ms: int, end_ms: int, timeframe: str) -> List[int]:
        """Open times of the bars a feed is expected to deliver in [start_ms, end_ms).

        * ``1d``: one bar per session, keyed by the session open (a continuous market: per UTC day, at 00:00).
          A holiday has no bar.
        * intraday: epoch-aligned bars whose interval overlaps an open interval. A bar wholly inside a closed
          period (weekend, holiday, maintenance, between sessions) is never expected.
        """
        if timeframe not in TIMEFRAME_MS:
            raise ValueError(f"unknown timeframe {timeframe!r}; known: {sorted(TIMEFRAME_MS)}")
        start_ms, end_ms = int(start_ms), int(end_ms)
        if end_ms <= start_ms:
            return []
        if timeframe == "1d":
            return [s.open_ms for s in self.sessions_between(start_ms, end_ms) if start_ms <= s.open_ms < end_ms]
        step = TIMEFRAME_MS[timeframe]
        first = start_ms + (-start_ms) % step
        intervals = self.open_intervals(first, end_ms + step)
        out: List[int] = []
        i = 0
        for bar in range(first, end_ms, step):
            while i < len(intervals) and intervals[i][1] <= bar:
                i += 1
            if i < len(intervals) and intervals[i][0] < bar + step:
                out.append(bar)
        return out

    def classify_bars(self, open_times: Iterable[int], start_ms: int, end_ms: int, timeframe: str) -> Dict[str, List[int]]:
        """Observed bars against the calendar: ``missing`` (expected, not delivered) and ``unexpected``
        (delivered for a closed period -- e.g. a forward-filled weekend or holiday bar, which must be dropped,
        never used)."""
        expected = set(self.expected_bars(start_ms, end_ms, timeframe))
        observed = {int(t) for t in open_times if start_ms <= int(t) < end_ms}
        return {"missing": sorted(expected - observed), "unexpected": sorted(observed - expected)}

    def to_summary(self) -> Dict[str, Any]:
        return {"calendar_id": self.calendar_id, "version": self.version, "kind": self.kind, "timezone": self.timezone,
                "source": self.source, "verification_status": self.verification_status,
                "coverage": [self.coverage[0].isoformat(), self.coverage[1].isoformat()] if self.coverage else None}


def utc_ms(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> int:
    """Epoch milliseconds of a UTC wall-clock time (tests and tooling)."""
    return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


def local_ms(tz_name: str, year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> int:
    """Epoch milliseconds of a wall-clock time in ``tz_name`` (tests and tooling)."""
    return int(datetime(year, month, day, hour, minute, tzinfo=ZoneInfo(tz_name)).timestamp() * 1000)


__all__ = ["CONTINUOUS", "SESSIONS", "VERIFIED", "UNVERIFIED", "PLATFORM_CONVENTION", "VERIFICATION_STATUSES",
           "TIMEFRAME_MS", "CalendarDefinitionError", "CalendarCoverageError", "WeeklySession", "MaintenanceWindow",
           "Session", "MarketCalendar", "utc_ms", "local_ms"]
