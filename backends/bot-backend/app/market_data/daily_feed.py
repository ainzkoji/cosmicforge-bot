"""The daily bar feed: the market-data provider that follows the exchange day by day (Section H, Steps 3.1, 3.12).

    python -m app.market_data.daily_feed update [--max-requests N]     fetch what is missing (public data only)
    python -m app.market_data.daily_feed status                        what the feed holds and how fresh it is

``DailyBarFeed`` is the ``DailyBars`` provider of the platform cycle driver (``pipeline.market_data``). It keeps
daily bars, funding records and exchange-metadata snapshots in the platform database, brings them up to date
from an injected fetcher, and serves ``panel_until(day)`` as a ``DailyPanel`` with the shape and the cleaning
rules of the research dataset (``daily_dataset``), so that the evaluator's ``prepare`` computes the same
features from the same bars. The cleaning is not re-implemented: ``clean_klines`` and ``clean_funding`` of the
dataset decide what is kept.

What the feed guarantees:

* **Only closed, valid, traded bars are stored.** A row is a bar when its day has closed and it passes the
  dataset's rule (finite, positive, ``high >= max(open, close)``, ``low <= min(open, close)``, non-negative
  volume) and has at least one trade. Anything else the source answers is recorded as a rejection with a
  reason and is never used as a price. Nothing is filled forward, interpolated or repaired.
* **Stored data never changes.** Bars, funding records and metadata snapshots are append-only. A later
  download that disagrees with a stored bar or funding record is written to the revision table and is NOT
  applied: decisions were taken on the earlier value.
* **A served day stays the same.** The first time a decision day is served it is sealed; from then on it is
  served from the rows that existed at that moment, so a replayed cycle reads what the first run read.
* **One symbol cannot stop the others.** Every symbol is fetched and stored on its own; a failed request or a
  corrupt file costs that symbol's day and is reported, the other symbols are unaffected.
* **Only what is missing is requested.** Per symbol the feed knows up to which day its days are resolved; a
  repeated update asks for nothing it already has. The number of requests is counted per update and bounded.
* **Unavailable stays unavailable.** A missing funding event is a missing row (``funding_hours`` is zero for
  that day, which the evaluator's registered rule treats as unknown, never as a zero rate). A symbol without
  exchange metadata has no entry in the snapshot: no tick, step or minimum notional is invented here.
* **Freshness is explicit.** ``freshness(day)`` says what share of yesterday's symbols has a bar, how old the
  newest bar is, which symbols failed, and whether the day may be used. A day that may not be used is refused
  with a coded error; a partial panel is never returned in its place.

Governance. The feed is production market data. It is not the research dataset: it does not read, write or
modify the dataset store (``data/research``) or its manifest, it holds no reference to them, and it exposes no
function that evaluates a strategy over stored history. The research holdout of Mandate 004 (``spec.
HOLDOUT_START`` to ``spec.HOLDOUT_END``) is reserved and unopened; its bars are in the feed only because the
indicators are path-dependent and need the whole history, and ``panel_until`` refuses a decision day inside it.

Public data only: no credentials, no account, no order endpoint. Time is injected.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Protocol, Sequence, Set, Tuple

import numpy as np

from app.trading_intelligence.hashing import stable_hash
from shared_lib.persistence.cati_feed_schema import (
    FEED_BAR_TABLE,
    FEED_FUNDING_TABLE,
    FEED_METADATA_TABLE,
    FEED_REJECTION_TABLE,
    FEED_REVISION_TABLE,
    FEED_SEAL_TABLE,
    FEED_STATE_TABLE,
    FEED_SYMBOL_TABLE,
    FEED_UPDATE_TABLE,
)

from . import binance_archive as BA
from . import daily_dataset as DD

FEED_VERSION = "daily-bar-feed-1"
DAY_MS = BA.DAY_MS
HOUR_MS = 3_600_000
#: the research dataset starts here; indicators are path-dependent, so the feed keeps the same start
DEFAULT_FIRST_DAY = DD.day_index(f"{DD.FIRST_MONTH}-01")
PRICE_FIELDS: Tuple[str, ...] = ("open", "high", "low", "close", "quote_volume")
FUNDING_FIELDS: Tuple[str, ...] = ("funding_midnight", "funding_later_positive", "funding_later_negative", "funding_hours")
NO_TRADES = "NO_TRADES"
COMPLETED, PARTIAL, FAILED, RUNNING, ABANDONED = "COMPLETED", "PARTIAL", "FAILED", "RUNNING", "ABANDONED"
BAR, FUNDING = "BAR", "FUNDING"
UNAVAILABLE = "DAILY_FEED_UNAVAILABLE"

KlineRow = Tuple[int, float, float, float, float, float, float, int]
FundingRow = Tuple[int, int, float]


class FeedUnavailable(RuntimeError):
    """The day cannot be served. ``code`` says why; the driver records it and waits, it never guesses."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


@dataclass(frozen=True)
class Fetched:
    """What the source returned for one symbol. ``rows`` are raw, in source order. ``answered`` are the
    requested days the source gave a definitive answer for (a file was there, with or without a row); a day
    that is not answered may simply not be published yet."""
    rows: Tuple[Any, ...]
    answered: FrozenSet[int] = frozenset()


class DailyFetcher(Protocol):
    """The source of the feed. Public market data only. ``requests`` (optional) is the running number of
    requests the source has made, which the feed reports and bounds."""

    def symbols(self) -> Sequence[str]: ...
    def metadata(self, fetched_at: str) -> Mapping[str, Any]: ...
    def klines(self, symbol: str, days: Sequence[int]) -> Fetched: ...
    def funding(self, symbol: str, days: Sequence[int]) -> Fetched: ...


@dataclass(frozen=True)
class FeedPolicy:
    #: share of the symbols with a bar yesterday that must have a bar on the day (the driver applies its own
    #: ``MIN_BAR_COVERAGE`` to the in-scope symbols on top of this; neither replaces the other)
    min_coverage: float = 0.90
    #: until this long after the close, a day with an expected symbol still unresolved is not served
    settle_ms: int = 6 * HOUR_MS
    #: a day the source has not answered is taken as "no bar" once it is this old
    bar_absent_after_ms: int = 3 * DAY_MS
    funding_absent_after_ms: int = 45 * DAY_MS
    #: a contract without a bar for this many days is asked again only every ``dormant_probe_ms``
    dormant_after_days: int = 14
    dormant_probe_ms: int = 7 * DAY_MS
    #: the symbol list and the exchange metadata are refreshed at most this often
    listing_refresh_ms: int = 6 * HOUR_MS
    metadata_max_age_ms: int = 3 * DAY_MS
    #: bound of one update, in requests of the source
    max_requests_per_update: int = 5_000
    #: an update whose first fetches all fail stops: the source is down, the other symbols are not hammered
    circuit_breaker_failures: int = 10
    #: a contract that could never be fetched stops blocking the feed after this many failed updates
    exclude_after_failures: int = 5


def close_ms(day: int) -> int:
    return (int(day) + 1) * DAY_MS


def research_holdout_window() -> Tuple[int, int]:
    """The reserved holdout of the daily trend mandate, as day indices. Read from the frozen specification."""
    from app.trading_intelligence.families.daily_trend import spec

    return DD.day_index(spec.HOLDOUT_START), DD.day_index(spec.HOLDOUT_END)


def bar_rejection(row: KlineRow) -> Optional[str]:
    """Why a raw kline row is not a bar, or ``None``. The decision itself is ``daily_dataset.clean_klines``
    (invalid rows) and the dataset's "a zero-trade bar is not a bar"; this only names the reason."""
    ts, o, h, l, c, v, qv, n = row
    if int(ts) % DAY_MS:
        return "TIMESTAMP_NOT_A_DAY_OPEN"
    if not all(math.isfinite(x) for x in (o, h, l, c, v, qv)):
        return "NOT_FINITE"
    if min(o, h, l, c) <= 0:
        return "PRICE_NOT_POSITIVE"
    if h < max(o, c):
        return "HIGH_BELOW_OPEN_OR_CLOSE"
    if l > min(o, c):
        return "LOW_ABOVE_OPEN_OR_CLOSE"
    if v < 0 or qv < 0:
        return "NEGATIVE_VOLUME"
    if int(n) <= 0:
        return NO_TRADES
    return None


# ------------------------------------------------------------------------------------------- the real source
class _Blob(io.BytesIO):
    """A downloaded archive file held in memory, with the name the archive parser quotes in its errors."""

    def __init__(self, body: bytes, name: str) -> None:
        super().__init__(body)
        self.name = name


def _month_of(day: int) -> str:
    return DD.day_text(day)[:7]


def _month_last_day(month: str) -> int:
    year, mon = int(month[:4]), int(month[5:7])
    following = date(year + (mon == 12), mon % 12 + 1, 1)
    return (following - timedelta(days=1) - DD.EPOCH).days


class BinanceArchiveFetcher:
    """The public Binance USD-M sources the research dataset was built from: the archive's kline and funding
    files (each verified against the archive's own SHA-256 before it is parsed, by the dataset's own parser)
    and the public ``exchangeInfo``. Nothing is written to disk.

    A short request (the daily case: yesterday) asks for the daily file directly. A long one (first contact
    with a contract, a long outage) first asks the archive what it holds, takes whole months from the monthly
    files and the remaining days from the daily files, as the dataset does.

    NOT VERIFIED AGAINST THE LIVE ENDPOINTS in this build: the request paths are the dataset's, the responses
    are parsed by the dataset's parser, and the tests use canned responses of that shape.
    """

    def __init__(self, http_get: Optional[BA.Getter] = None, *, pattern: str = DD.USDT_PERPETUAL,
                 direct_days: int = 7) -> None:
        self._http_get = http_get or BA.http_get
        self._pattern = re.compile(pattern)
        self._direct_days = int(direct_days)
        self._lock = threading.Lock()
        self.requests = 0

    def _get(self, url: str) -> bytes:
        with self._lock:
            self.requests += 1
        return self._http_get(url)

    @staticmethod
    def _absent(exc: Exception) -> bool:
        return isinstance(exc, BA.ArchiveError) and "not in the archive" in str(exc)

    def _file(self, key: str) -> Optional[bytes]:
        """The verified bytes of an archive object, or ``None`` when the archive does not have it."""
        for attempt in (1, 2):
            try:
                expected = BA.parse_checksum(self._get(f"{BA.ARCHIVE_URL}/{key}.CHECKSUM").decode("utf-8", errors="replace"))
                body = self._get(f"{BA.ARCHIVE_URL}/{key}")
            except BA.ArchiveError as exc:
                if self._absent(exc):
                    return None
                raise
            if hashlib.sha256(body).hexdigest() == expected:
                return body
            if attempt == 2:
                raise BA.ChecksumMismatch(f"{key}: downloaded bytes do not match the archive checksum")
        raise AssertionError("unreachable")

    def symbols(self) -> List[str]:
        """Every USDT-margined perpetual the archive publishes, ended contracts included, plus the ones that
        only have daily files so far (a contract listed this month)."""
        found = set(BA.archive_symbols(BA.KLINES_ROOT, get=self._get)) | set(BA.archive_symbols(DD.DAILY_KLINES_ROOT, get=self._get))
        return sorted(s for s in found if self._pattern.fullmatch(s))

    def metadata(self, fetched_at: str) -> Dict[str, Any]:
        return DD.metadata_snapshot(get=self._get, fetched_at=fetched_at)

    def klines(self, symbol: str, days: Sequence[int]) -> Fetched:
        wanted = sorted(set(int(d) for d in days))
        rows: List[KlineRow] = []
        answered: Set[int] = set()

        def daily(day: int) -> None:
            key = f"{DD.DAILY_KLINES_ROOT}{symbol}/1d/{symbol}-1d-{DD.day_text(day)}.zip"
            body = self._file(key)
            if body is not None:
                rows.extend(BA.read_kline_zip(_Blob(body, key.rsplit("/", 1)[-1])))
                answered.add(day)

        if len(wanted) <= self._direct_days:
            for day in wanted:
                daily(day)
            return Fetched(tuple(rows), frozenset(answered))
        monthly = {o.month: o.key for o in BA.symbol_files(symbol, "klines", first_month=_month_of(wanted[0]),
                                                           last_month=_month_of(wanted[-1]), get=self._get)}
        _, objects = BA.list_prefix(f"{DD.DAILY_KLINES_ROOT}{symbol}/1d/", get=self._get)
        dailies = {o.key[-14:-4] for o in objects if o.key.endswith(".zip")}
        have: Set[int] = set()
        for month in sorted({_month_of(d) for d in wanted}):
            key = monthly.get(month)
            body = self._file(key) if key else None
            if body is None:
                continue
            parsed = BA.read_kline_zip(_Blob(body, key.rsplit("/", 1)[-1]))
            rows.extend(parsed)
            have |= {int(r[0]) // DAY_MS for r in parsed}
            answered |= {d for d in wanted if _month_of(d) == month}
        for day in wanted:                      # what a monthly file skips, or a month without one
            if day not in have and DD.day_text(day) in dailies:
                daily(day)
        return Fetched(tuple(rows), frozenset(answered))

    def funding(self, symbol: str, days: Sequence[int]) -> Fetched:
        """The archive publishes funding by month, after the month has ended. A month is asked for only when
        the request shows it has ended (it contains the month's last day, or a later day)."""
        wanted = sorted(set(int(d) for d in days))
        if not wanted:
            return Fetched(())
        months = sorted({_month_of(d) for d in wanted if _month_last_day(_month_of(d)) <= wanted[-1]})
        if len(months) > 2:
            listed = {o.month for o in BA.symbol_files(symbol, "funding", first_month=months[0], last_month=months[-1],
                                                       get=self._get)}
            months = [m for m in months if m in listed]
        rows: List[FundingRow] = []
        answered: Set[int] = set()
        for month in months:
            key = f"{BA.FUNDING_ROOT}{symbol}/{symbol}-fundingRate-{month}.zip"
            body = self._file(key)
            if body is None:
                continue
            rows.extend(BA.read_funding_zip(_Blob(body, key.rsplit("/", 1)[-1])))
            answered |= {d for d in wanted if _month_of(d) == month}
        return Fetched(tuple(rows), frozenset(answered))


# --------------------------------------------------------------------------------------------------- the feed
class DailyBarFeed:
    """See the module text. ``fetcher`` may be ``None`` for a feed that only serves what is stored.
    ``auto_update``: ``panel_until`` first fetches what is missing for a day that is not sealed yet.
    ``clock`` returns epoch milliseconds."""

    def __init__(self, db: Any, *, fetcher: Optional[DailyFetcher] = None, clock: Optional[Callable[[], int]] = None,
                 first_day: Optional[int] = None, policy: Optional[FeedPolicy] = None, auto_update: bool = False,
                 workers: int = 1) -> None:
        self._db = db
        self._fetcher = fetcher
        self._clock = clock or (lambda: int(time.time() * 1000))
        self._policy = policy or FeedPolicy()
        self._auto_update = bool(auto_update)
        self._workers = max(1, int(workers))
        self._reserved = research_holdout_window()
        self._first_day = self._settle_first_day(first_day)
        self._last: Optional[Tuple[Tuple[int, ...], Any, Dict[str, Any]]] = None
        self.calls = 0                      # panels handed out
        self.builds = 0                     # panels read whole from the database
        self.extensions = 0                 # panels made from the previous one and the new day's rows only

    @property
    def first_day(self) -> int:
        return self._first_day

    @property
    def policy(self) -> FeedPolicy:
        return self._policy

    def _now(self) -> int:
        return int(self._clock())

    def _scope(self) -> Any:
        """One database connection for a whole update or a whole serve (``DB.scope``). Every ``connect()`` block
        still commits or rolls back on its own; no block here is nested inside another."""
        scope = getattr(self._db, "scope", None)
        return scope() if callable(scope) else nullcontext()

    # ------------------------------------------------------------------------------------------- state
    def _settle_first_day(self, first_day: Optional[int]) -> int:
        """The first day of the history is part of every panel's identity: it is fixed by the first feed that
        uses the database, and a feed configured with another one is refused."""
        with self._db.connect() as conn:
            row = conn.execute(f"SELECT value FROM {FEED_STATE_TABLE} WHERE key='first_day'").fetchone()
            if row is None:
                value = DEFAULT_FIRST_DAY if first_day is None else int(first_day)
                conn.execute(f"INSERT OR IGNORE INTO {FEED_STATE_TABLE} (key, value, updated_at) VALUES ('first_day',?,?)",
                             (str(value), self._now()))
                row = conn.execute(f"SELECT value FROM {FEED_STATE_TABLE} WHERE key='first_day'").fetchone()
        stored = int(row[0])
        if first_day is not None and int(first_day) != stored:
            raise ValueError(f"this feed's history starts on {DD.day_text(stored)}; it cannot be opened with {DD.day_text(first_day)}")
        return stored

    @staticmethod
    def _state(conn: Any, key: str) -> Optional[str]:
        row = conn.execute(f"SELECT value FROM {FEED_STATE_TABLE} WHERE key=?", (key,)).fetchone()
        return None if row is None else row[0]

    @staticmethod
    def _set_state(conn: Any, key: str, value: Any, now: int) -> None:
        conn.execute(f"INSERT INTO {FEED_STATE_TABLE} (key, value, updated_at) VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET "
                     "value=excluded.value, updated_at=excluded.updated_at", (key, str(value), int(now)))

    def _source_requests(self, calls: int) -> int:
        counted = getattr(self._fetcher, "requests", None)
        return int(counted) if isinstance(counted, int) else calls

    # ------------------------------------------------------------------------------------------ update
    def update(self, through: Optional[int] = None) -> Dict[str, Any]:
        """Fetch what is missing up to ``through`` (default: the last closed day). Idempotent: a second call
        with nothing new to learn makes no request for a day it already resolved. Never raises for a failing
        symbol or a failing source: the outcome is in the returned summary and in ``cati_feed_updates``."""
        if self._fetcher is None:
            raise FeedUnavailable(UNAVAILABLE, "this feed has no source")
        with self._scope():
            return self._update(through)

    def _update(self, through: Optional[int]) -> Dict[str, Any]:
        now = self._now()
        last_closed = now // DAY_MS - 1
        target = last_closed if through is None else min(int(through), last_closed)
        if target < self._first_day:
            return {"status": COMPLETED, "update_id": None, "through_day": None, "source_calls": 0, "http_requests": 0,
                    "reason": "NO_CLOSED_DAY_IN_RANGE"}
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(f"UPDATE {FEED_UPDATE_TABLE} SET status=? WHERE status=?", (ABANDONED, RUNNING))
            update_id = conn.execute(f"INSERT INTO {FEED_UPDATE_TABLE} (status, started_at, through_day) VALUES (?,?,?)",
                                     (RUNNING, now, target)).lastrowid
        c = {"source_calls": 0, "symbols_requested": 0, "symbols_failed": 0, "symbol_days_requested": 0, "bars_stored": 0,
             "funding_stored": 0, "days_rejected": 0, "revisions": 0}
        detail: Dict[str, Any] = {"feed_version": FEED_VERSION, "failed": {}, "funding_failed": {}, "listing_errors": {},
                                  "deferred": 0, "dormant_skipped": 0, "duplicates": 0, "unclosed_rows_ignored": 0}
        requests_before = self._source_requests(0)
        status, tracked = COMPLETED, 0
        try:
            self._refresh_listing(now, c, detail)
            plan, tracked, detail["dormant_skipped"] = self._plan(now, target)
            # the source is taken to be down when this many requests in a row fail before any succeeds
            breaker = max(1, min(self._policy.circuit_breaker_failures, tracked - detail["dormant_skipped"]))
            attempted = succeeded = 0
            postponed: List[Tuple[str, str]] = []      # failures before the first success: no symbol is blamed yet
            for start in range(0, len(plan), self._workers):
                if self._source_requests(c["source_calls"]) - requests_before >= self._policy.max_requests_per_update:
                    detail["deferred"] = len(plan) - start
                    status = PARTIAL
                    break
                if succeeded == 0 and attempted >= breaker:
                    detail["deferred"] = len(plan) - start
                    break
                batch = plan[start:start + self._workers]
                for (symbol, bar_days, funding_days), (klines, funding) in zip(batch, self._fetch(batch, c)):
                    if bar_days:
                        attempted += 1
                        c["symbols_requested"] += 1
                        c["symbol_days_requested"] += len(bar_days)
                    if isinstance(klines, Exception):
                        c["symbols_failed"] += 1
                        detail["failed"][symbol] = _error(klines)
                        postponed.append((symbol, _error(klines)))
                        if succeeded:
                            postponed = self._record_failures(postponed, now)
                        continue
                    if bar_days:
                        succeeded += 1
                        postponed = self._record_failures(postponed, now)
                    if isinstance(funding, Exception):
                        detail["funding_failed"][symbol] = _error(funding)
                        funding = None
                    self._store(update_id, now, last_closed, target, symbol, bar_days, klines, funding_days, funding, c, detail)
            if succeeded == 0 and attempted >= breaker:
                status = FAILED                         # nothing could be fetched: the source is not usable now
                detail["circuit_breaker"] = f"the first {attempted} requests all failed"
            else:
                self._record_failures(postponed, now)
            if tracked == 0:
                status = FAILED
                detail["reason"] = "NO_SYMBOLS_KNOWN"
        except Exception as exc:  # noqa: BLE001 -- an update never takes the caller down; it is recorded as failed
            status = FAILED
            detail["error"] = _error(exc)
        http = self._source_requests(c["source_calls"]) - requests_before
        summary = {"update_id": update_id, "status": status, "through_day": target, "symbols_tracked": tracked,
                   "http_requests": http, **c,
                   "symbol_failure_rate": (c["symbols_failed"] / c["symbols_requested"]) if c["symbols_requested"] else 0.0,
                   "detail": detail}
        with self._db.connect() as conn:
            conn.execute(
                f"UPDATE {FEED_UPDATE_TABLE} SET status=?, completed_at=?, symbols_tracked=?, symbols_requested=?, "
                "symbols_failed=?, symbol_days_requested=?, source_calls=?, http_requests=?, bars_stored=?, funding_stored=?, "
                "days_rejected=?, revisions=?, detail=? WHERE update_id=?",
                (status, self._now(), tracked, c["symbols_requested"], c["symbols_failed"], c["symbol_days_requested"],
                 c["source_calls"], http, c["bars_stored"], c["funding_stored"], c["days_rejected"], c["revisions"],
                 json.dumps(detail, sort_keys=True, default=str), update_id))
        return summary

    def _refresh_listing(self, now: int, c: Dict[str, int], detail: Dict[str, Any]) -> None:
        """The symbol list and the exchange metadata, each at most once per ``listing_refresh_ms``. A failure of
        either is recorded and the update goes on with what is known."""
        with self._db.connect() as conn:
            listed = self._state(conn, "symbols_listed_at")
            checked = self._state(conn, "metadata_checked_at")
        if listed is None or now - int(listed) >= self._policy.listing_refresh_ms:
            c["source_calls"] += 1
            try:
                symbols = sorted(set(str(s) for s in self._fetcher.symbols()))
                with self._db.connect() as conn:
                    conn.executemany(
                        f"INSERT OR IGNORE INTO {FEED_SYMBOL_TABLE} (symbol, first_seen_at, bars_through, funding_through) "
                        "VALUES (?,?,?,?)", [(s, now, self._first_day - 1, self._first_day - 1) for s in symbols])
                    self._set_state(conn, "symbols_listed_at", now, now)
            except Exception as exc:  # noqa: BLE001
                detail["listing_errors"]["symbols"] = _error(exc)
        if checked is None or now - int(checked) >= self._policy.listing_refresh_ms:
            c["source_calls"] += 1
            try:
                snapshot = dict(self._fetcher.metadata(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now / 1000.0))))
                if not isinstance(snapshot.get("symbols"), Mapping):
                    raise ValueError("the exchange metadata snapshot has no symbols")
                digest = str(snapshot.get("content_hash") or stable_hash(snapshot["symbols"]))
                with self._db.connect() as conn:
                    conn.execute(
                        f"INSERT OR IGNORE INTO {FEED_METADATA_TABLE} (content_hash, fetched_at, symbols, document, stored_at) "
                        "VALUES (?,?,?,?,?)", (digest, now, len(snapshot["symbols"]), json.dumps(snapshot, sort_keys=True), now))
                    self._set_state(conn, "metadata_checked_at", now, now)
                    self._set_state(conn, "metadata_content_hash", digest, now)
            except Exception as exc:  # noqa: BLE001
                detail["listing_errors"]["metadata"] = _error(exc)

    def _plan(self, now: int, target: int) -> Tuple[List[Tuple[str, List[int], List[int]]], int, int]:
        """``(symbol, bar days, funding days)`` still to resolve, in symbol order; dormant contracts left out."""
        plan: List[Tuple[str, List[int], List[int]]] = []
        dormant = 0
        with self._db.connect() as conn:
            states = [tuple(r) for r in conn.execute(
                f"SELECT symbol, bars_through, funding_through, last_bar_day, last_success_at, last_attempt_at FROM "
                f"{FEED_SYMBOL_TABLE} ORDER BY symbol")]
            for symbol, bars_through, funding_through, last_bar, success, attempt in states:
                if bars_through >= target and funding_through >= target:
                    continue
                if (success is not None and (last_bar is None or target - int(last_bar) > self._policy.dormant_after_days)
                        and attempt is not None and now - int(attempt) < self._policy.dormant_probe_ms):
                    dormant += 1
                    continue
                resolved = {r[0] for r in conn.execute(
                    f"SELECT day FROM {FEED_BAR_TABLE} WHERE symbol=? AND day>? UNION SELECT day FROM {FEED_REJECTION_TABLE} "
                    "WHERE symbol=? AND day>?", (symbol, bars_through, symbol, bars_through))}
                bar_days = [d for d in range(int(bars_through) + 1, target + 1) if d not in resolved]
                funding_days = list(range(int(funding_through) + 1, target + 1))
                if bar_days or funding_days:
                    plan.append((symbol, bar_days, funding_days))
        return plan, len(states), dormant

    def _fetch(self, batch: Sequence[Tuple[str, List[int], List[int]]], c: Dict[str, int]) -> List[Tuple[Any, Any]]:
        """Network only, no database. Each symbol on its own: an exception is that symbol's result."""
        def one(item: Tuple[str, List[int], List[int]]) -> Tuple[Any, Any]:
            symbol, bar_days, funding_days = item
            klines: Any = Fetched(())
            funding: Any = None
            try:
                if bar_days:
                    klines = self._fetcher.klines(symbol, bar_days)
            except Exception as exc:  # noqa: BLE001 -- failed-symbol isolation
                return exc, None
            try:
                if funding_days:
                    funding = self._fetcher.funding(symbol, funding_days)
            except Exception as exc:  # noqa: BLE001
                funding = exc
            return klines, funding

        c["source_calls"] += sum((1 if b else 0) + (1 if f else 0) for _, b, f in batch)
        if self._workers == 1 or len(batch) == 1:
            return [one(item) for item in batch]
        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            return list(pool.map(one, batch))

    def _record_failures(self, failures: Sequence[Tuple[str, str]], now: int) -> List[Tuple[str, str]]:
        if failures:
            with self._db.connect() as conn:
                conn.executemany(f"UPDATE {FEED_SYMBOL_TABLE} SET failures=failures+1, last_error=?, last_attempt_at=? WHERE symbol=?",
                                 [(error, now, symbol) for symbol, error in failures])
        return []

    def _store(self, update_id: int, now: int, last_closed: int, target: int, symbol: str, bar_days: Sequence[int],
               klines: Fetched, funding_days: Sequence[int], funding: Optional[Fetched], c: Dict[str, int],
               detail: Dict[str, Any]) -> None:
        """One symbol, one transaction: its new bars, rejections, revisions, funding records and its cursors."""
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._store_klines(conn, update_id, now, last_closed, symbol, klines.rows, c, detail)
            if funding is not None:
                self._store_funding(conn, update_id, now, last_closed, symbol, funding.rows, c)
            row = conn.execute(f"SELECT bars_through, funding_through FROM {FEED_SYMBOL_TABLE} WHERE symbol=?", (symbol,)).fetchone()
            bars_through, funding_through = int(row[0]), int(row[1])
            resolved = {r[0] for r in conn.execute(
                f"SELECT day FROM {FEED_BAR_TABLE} WHERE symbol=? AND day>? UNION SELECT day FROM {FEED_REJECTION_TABLE} "
                "WHERE symbol=? AND day>?", (symbol, bars_through, symbol, bars_through))}
            asked = set(bar_days)
            while bars_through < target:
                day = bars_through + 1
                final = day in resolved or (day in asked and (day in klines.answered
                                                              or now - close_ms(day) >= self._policy.bar_absent_after_ms))
                if not final:
                    break
                bars_through = day
            if funding is not None:
                asked = set(funding_days)
                while funding_through < target:
                    day = funding_through + 1
                    if not (day in asked and (day in funding.answered or now - close_ms(day) >= self._policy.funding_absent_after_ms)):
                        break
                    funding_through = day
            last_bar = conn.execute(f"SELECT MAX(day) FROM {FEED_BAR_TABLE} WHERE symbol=?", (symbol,)).fetchone()[0]
            conn.execute(
                f"UPDATE {FEED_SYMBOL_TABLE} SET bars_through=?, funding_through=?, last_bar_day=?, last_success_at=?, "
                "last_attempt_at=?, failures=0, last_error=NULL WHERE symbol=?",
                (bars_through, funding_through, last_bar, now, now, symbol))

    def _store_klines(self, conn: Any, update_id: int, now: int, last_closed: int, symbol: str, rows: Sequence[KlineRow],
                      c: Dict[str, int], detail: Dict[str, Any]) -> None:
        if not rows:
            return
        cleaned = DD.clean_klines(list(rows))               # the dataset's rule: first occurrence, invalid quarantined
        detail["duplicates"] += int(cleaned["duplicates"])
        lo = self._first_day
        offered_days = cleaned["days"].tolist() + [int(ts) // DAY_MS for ts in cleaned["invalid"]]
        span = (min(offered_days), max(offered_days))       # only the days the source spoke about are looked up
        stored = {int(r[0]): tuple(r)[1:] for r in conn.execute(
            f"SELECT day, open, high, low, close, volume, quote_volume, trades FROM {FEED_BAR_TABLE} WHERE symbol=? AND "
            "day>=? AND day<=?", (symbol, *span))}
        rejected = {int(r[0]): r[1] for r in conn.execute(
            f"SELECT day, reason FROM {FEED_REJECTION_TABLE} WHERE symbol=? AND day>=? AND day<=?", (symbol, *span))}

        def reject(day: int, reason: str, row: Sequence[Any]) -> None:
            conn.execute(f"INSERT INTO {FEED_REJECTION_TABLE} (symbol, day, reason, detail, update_id, recorded_at) VALUES (?,?,?,?,?,?)",
                         (symbol, day, reason, json.dumps({"row": [repr(x) for x in row]}), update_id, now))
            rejected[day] = reason
            c["days_rejected"] += 1

        for day, values, trades in zip(cleaned["days"].tolist(), cleaned["values"].tolist(), cleaned["trades"].tolist()):
            if day > last_closed:
                detail["unclosed_rows_ignored"] += 1        # the day has not closed: it is not a bar yet
                continue
            if day < lo:
                continue
            offered = (*values, int(trades))
            if day in stored:
                if stored[day] != offered:
                    c["revisions"] += self._revision(conn, update_id, now, BAR, symbol, day, stored[day], offered)
                continue
            if day in rejected:
                c["revisions"] += self._revision(conn, update_id, now, BAR, symbol, day, ("REJECTED", rejected[day]), offered)
                continue
            if int(trades) <= 0:
                reject(day, NO_TRADES, offered)             # "a zero-trade bar is not a bar"
                continue
            conn.execute(
                f"INSERT INTO {FEED_BAR_TABLE} (symbol, day, open, high, low, close, volume, quote_volume, trades, update_id, "
                "stored_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)", (symbol, day, *values, int(trades), update_id, now))
            stored[day] = offered
            c["bars_stored"] += 1
        if cleaned["invalid"]:
            first: Dict[int, KlineRow] = {}
            for row in sorted(rows, key=lambda r: r[0]):
                first.setdefault(int(row[0]), row)
            for ts in cleaned["invalid"]:
                day = int(ts) // DAY_MS
                if lo <= day <= last_closed and day not in stored and day not in rejected:
                    reject(day, bar_rejection(first[int(ts)]) or "INVALID_BY_THE_DATASET_RULE", first[int(ts)])

    def _store_funding(self, conn: Any, update_id: int, now: int, last_closed: int, symbol: str, rows: Sequence[FundingRow],
                       c: Dict[str, int]) -> None:
        if not rows:
            return
        cleaned = DD.clean_funding(list(rows))
        if not len(cleaned["time_ms"]):
            return
        lo, hi = self._first_day * DAY_MS, close_ms(last_closed)
        stored = {int(r[0]): (int(r[1]), float(r[2])) for r in conn.execute(
            f"SELECT time_ms, interval_hours, rate FROM {FEED_FUNDING_TABLE} WHERE symbol=? AND time_ms>=? AND time_ms<=?",
            (symbol, int(cleaned["time_ms"][0]), int(cleaned["time_ms"][-1])))}
        for ts, hours, rate in zip(cleaned["time_ms"].tolist(), cleaned["interval_hours"].tolist(), cleaned["rate"].tolist()):
            if not lo <= ts < hi:
                continue                                    # before the history, or in a day that has not closed
            offered = (int(hours), float(rate))
            if ts in stored:
                if stored[ts] != offered:
                    c["revisions"] += self._revision(conn, update_id, now, FUNDING, symbol, ts, stored[ts], offered)
                continue
            conn.execute(f"INSERT INTO {FEED_FUNDING_TABLE} (symbol, time_ms, interval_hours, rate, update_id, stored_at) "
                         "VALUES (?,?,?,?,?,?)", (symbol, ts, int(hours), float(rate), update_id, now))
            stored[ts] = offered
            c["funding_stored"] += 1

    @staticmethod
    def _revision(conn: Any, update_id: int, now: int, kind: str, symbol: str, key: int, stored: Sequence[Any],
                  offered: Sequence[Any]) -> int:
        """A later download disagrees with what is stored. Recorded once per distinct offer; never applied."""
        cur = conn.execute(
            f"INSERT OR IGNORE INTO {FEED_REVISION_TABLE} (kind, symbol, record_key, stored, offered, offered_hash, applied, "
            "update_id, detected_at) VALUES (?,?,?,?,?,?,0,?,?)",
            (kind, symbol, int(key), json.dumps([repr(x) for x in stored]), json.dumps([repr(x) for x in offered]),
             stable_hash([repr(x) for x in offered]), update_id, now))
        return int(cur.rowcount or 0)

    def recheck(self, first_day: int, last_day: int, symbols: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        """Download days that are already stored or rejected once more and compare (for instance a month's
        bars, taken from the daily files, against the monthly file the archive publishes later). Differences
        are recorded as revisions and NOT applied; nothing stored changes and no cursor moves."""
        if self._fetcher is None:
            raise FeedUnavailable(UNAVAILABLE, "this feed has no source")
        with self._scope():
            return self._recheck(first_day, last_day, symbols)

    def _recheck(self, first_day: int, last_day: int, symbols: Optional[Sequence[str]]) -> Dict[str, Any]:
        now = self._now()
        last_closed = now // DAY_MS - 1
        first_day, last_day = max(int(first_day), self._first_day), min(int(last_day), last_closed)
        with self._db.connect() as conn:
            held = [r[0] for r in conn.execute(
                f"SELECT symbol FROM {FEED_BAR_TABLE} WHERE day>=? AND day<=? UNION SELECT symbol FROM {FEED_REJECTION_TABLE} "
                "WHERE day>=? AND day<=? ORDER BY symbol", (first_day, last_day, first_day, last_day))]
            update_id = conn.execute(f"INSERT INTO {FEED_UPDATE_TABLE} (status, started_at, through_day) VALUES (?,?,?)",
                                     (RUNNING, now, last_day)).lastrowid
        wanted = [s for s in held if symbols is None or s in set(symbols)]
        c = {"source_calls": 0, "symbols_requested": 0, "symbols_failed": 0, "symbol_days_requested": 0, "bars_stored": 0,
             "funding_stored": 0, "days_rejected": 0, "revisions": 0}
        detail: Dict[str, Any] = {"kind": "RECHECK", "failed": {}, "duplicates": 0, "unclosed_rows_ignored": 0}
        before = self._source_requests(0)
        days = list(range(first_day, last_day + 1))
        for symbol in wanted:
            c["source_calls"] += 1
            c["symbols_requested"] += 1
            c["symbol_days_requested"] += len(days)
            try:
                fetched = self._fetcher.klines(symbol, days)
                with self._db.connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    self._store_klines(conn, update_id, now, last_closed, symbol, fetched.rows, c, detail)
            except Exception as exc:  # noqa: BLE001 -- one symbol's recheck failing does not stop the others
                c["symbols_failed"] += 1
                detail["failed"][symbol] = _error(exc)
        http = self._source_requests(c["source_calls"]) - before
        with self._db.connect() as conn:
            conn.execute(
                f"UPDATE {FEED_UPDATE_TABLE} SET status=?, completed_at=?, symbols_requested=?, symbols_failed=?, "
                "symbol_days_requested=?, source_calls=?, http_requests=?, bars_stored=?, days_rejected=?, revisions=?, detail=? "
                "WHERE update_id=?",
                (COMPLETED, self._now(), c["symbols_requested"], c["symbols_failed"], c["symbol_days_requested"],
                 c["source_calls"], http, c["bars_stored"], c["days_rejected"], c["revisions"],
                 json.dumps(detail, sort_keys=True, default=str), update_id))
        return {"update_id": update_id, "status": COMPLETED, "http_requests": http, **c, "detail": detail}

    # --------------------------------------------------------------------------------------- freshness
    def freshness(self, day: int) -> Dict[str, Any]:
        """Whether ``day`` may be used for a decision, and everything that answer rests on. Reads only."""
        day, now, p = int(day), self._now(), self._policy
        with self._db.connect() as conn:
            seal = conn.execute(f"SELECT sealed_at FROM {FEED_SEAL_TABLE} WHERE day=?", (day,)).fetchone()
            expected = {r[0] for r in conn.execute(f"SELECT symbol FROM {FEED_BAR_TABLE} WHERE day=?", (day - 1,))}
            present = {r[0] for r in conn.execute(f"SELECT symbol FROM {FEED_BAR_TABLE} WHERE day=?", (day,))}
            rejected = {r[0]: r[1] for r in conn.execute(f"SELECT symbol, reason FROM {FEED_REJECTION_TABLE} WHERE day=?", (day,))}
            newest = conn.execute(f"SELECT MAX(day) FROM {FEED_BAR_TABLE}").fetchone()[0]
            states = {r[0]: (int(r[1]), int(r[2]), r[3], r[4]) for r in conn.execute(
                f"SELECT symbol, bars_through, failures, last_error, last_success_at FROM {FEED_SYMBOL_TABLE}")}
            covering = conn.execute(
                f"SELECT update_id, status, completed_at FROM {FEED_UPDATE_TABLE} WHERE through_day>=? AND status IN (?,?) "
                "ORDER BY update_id DESC LIMIT 1", (day, COMPLETED, PARTIAL)).fetchone()
            latest = conn.execute(
                f"SELECT update_id, status, through_day, completed_at, http_requests, symbols_failed, symbols_requested FROM "
                f"{FEED_UPDATE_TABLE} ORDER BY update_id DESC LIMIT 1").fetchone()
            meta = conn.execute(f"SELECT content_hash, fetched_at, symbols FROM {FEED_METADATA_TABLE} ORDER BY seq DESC LIMIT 1").fetchone()
            checked = self._state(conn, "metadata_checked_at")
        missing = sorted(expected - present - set(rejected))
        failed = [s for s in missing if s in states and states[s][1] > 0]
        pending = [s for s in missing if s not in failed and (s not in states or states[s][0] < day)]
        absent = [s for s in missing if s not in failed and s not in pending]
        never = sorted(s for s, v in states.items() if v[3] is None)
        excluded = [s for s in never if states[s][1] >= p.exclude_after_failures]
        bootstrap = [s for s in never if s not in excluded]
        coverage = (len(expected & present) / len(expected)) if expected else None
        metadata_age = None if checked is None else now - int(checked)
        closed = now >= close_ms(day)
        reason: Optional[str] = None
        if seal is None:
            if self._reserved[0] <= day <= self._reserved[1]:
                reason = "DAILY_FEED_DAY_IN_RESERVED_HOLDOUT"
            elif day < self._first_day:
                reason = "DAILY_FEED_DAY_BEFORE_HISTORY"
            elif not closed:
                reason = "DAILY_FEED_DAY_NOT_CLOSED"
            elif covering is None:
                reason = "DAILY_FEED_NOT_UPDATED"
            elif bootstrap:
                reason = "DAILY_FEED_HISTORY_INCOMPLETE"
            elif meta is None:
                reason = "DAILY_FEED_NO_EXCHANGE_METADATA"
            elif metadata_age is None or metadata_age > p.metadata_max_age_ms:
                reason = "DAILY_FEED_EXCHANGE_METADATA_STALE"
            elif not present:
                reason = "DAILY_FEED_DAY_HAS_NO_BARS"
            elif day > self._first_day and (coverage is None or coverage < p.min_coverage):
                reason = "DAILY_FEED_DAY_INCOMPLETE"
            elif (failed or pending) and now < close_ms(day) + p.settle_ms:
                reason = "DAILY_FEED_DAY_SETTLING"
        return {
            "day": DD.day_text(day), "day_index": day, "checked_at": now, "closed": closed, "sealed": seal is not None,
            "usable": reason is None, "reason": reason,
            "bars": len(present), "expected_symbols": len(expected), "present_of_expected": len(expected & present),
            "coverage": coverage, "min_coverage": p.min_coverage,
            "symbols_failed": failed, "symbols_pending": pending, "symbols_without_a_bar": absent,
            "symbols_rejected": dict(sorted(rejected.items())), "symbols_never_fetched": bootstrap,
            "symbols_excluded_after_failures": excluded,
            "failures": {s: states[s][2] for s in failed + excluded if states[s][2]},
            "decision_bar_age_ms": now - close_ms(day) if closed else None,
            "newest_bar_day": None if newest is None else DD.day_text(newest),
            "newest_bar_age_ms": None if newest is None else now - close_ms(newest),
            "exchange_metadata": None if meta is None else {"content_hash": meta[0], "fetched_at": int(meta[1]), "symbols": int(meta[2]),
                                                           "age_ms": metadata_age},
            "last_update": None if latest is None else {
                "update_id": latest[0], "status": latest[1], "through_day": DD.day_text(latest[2]), "completed_at": latest[3],
                "http_requests": latest[4], "symbols_failed": latest[5], "symbols_requested": latest[6]}}

    # ------------------------------------------------------------------------------------------ serving
    def panel_until(self, day: int) -> Tuple[Any, Mapping[str, Any]]:
        """``(panel, exchange metadata)`` with every stored bar up to ``day``. Raises ``FeedUnavailable`` (with
        ``code``) when the day may not be used; it never returns a partial or a guessed panel."""
        day = int(day)
        if self._reserved[0] <= day <= self._reserved[1]:
            raise FeedUnavailable("DAILY_FEED_DAY_IN_RESERVED_HOLDOUT",
                                  f"{DD.day_text(day)} is inside the reserved research holdout; the feed serves no decision there")
        with self._scope():
            return self._panel_until(day)

    def _panel_until(self, day: int) -> Tuple[Any, Mapping[str, Any]]:
        seal = self._seal_of(day)
        if seal is None:
            if self._auto_update and self._now() >= close_ms(day) and day >= self._first_day:
                summary = self.update(through=day)
                if summary["status"] == FAILED:
                    raise FeedUnavailable(UNAVAILABLE, json.dumps({k: summary["detail"].get(k) for k in (
                        "error", "reason", "circuit_breaker", "listing_errors") if summary["detail"].get(k)}, default=str))
            fresh = self.freshness(day)
            if not fresh["usable"]:
                raise FeedUnavailable(fresh["reason"], json.dumps(
                    {k: fresh[k] for k in ("day", "coverage", "bars", "expected_symbols", "symbols_failed", "symbols_pending",
                                           "symbols_never_fetched")}, default=str)[:600])
            seal = self._seal(day, fresh)
        self.calls += 1
        return self._serve(seal)

    def _seal_of(self, day: int) -> Optional[Tuple[int, ...]]:
        with self._db.connect() as conn:
            row = conn.execute(
                f"SELECT day, first_day, bar_seq, funding_seq, rejection_seq, metadata_seq, bars, funding_records, symbols "
                f"FROM {FEED_SEAL_TABLE} WHERE day=?", (int(day),)).fetchone()
        return None if row is None else tuple(int(x) for x in row)

    def _seal(self, day: int, fresh: Mapping[str, Any]) -> Tuple[int, ...]:
        """Fix what ``day`` is served from: the highest ``seq`` of every table, read in one snapshot. Rows are
        only ever appended, so a row committed after the snapshot has a higher ``seq`` and stays outside. The
        seal also keeps how many rows the day holds, so a later serve can tell if any went missing."""
        with self._db.connect() as conn:
            conn.execute("BEGIN")                           # one snapshot for the four marks; no write lock
            marks = tuple(int(conn.execute(f"SELECT COALESCE(MAX(seq), 0) FROM {t}").fetchone()[0])
                          for t in (FEED_BAR_TABLE, FEED_FUNDING_TABLE, FEED_REJECTION_TABLE, FEED_METADATA_TABLE))
        panel, metadata, counts = self._extended(day, self._first_day, marks) or self._built(day, self._first_day, marks)
        with self._db.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if conn.execute(f"SELECT 1 FROM {FEED_SEAL_TABLE} WHERE day=?", (day,)).fetchone() is None:
                conn.execute(
                    f"INSERT INTO {FEED_SEAL_TABLE} (day, first_day, bar_seq, funding_seq, rejection_seq, metadata_seq, bars, "
                    "funding_records, symbols, freshness, sealed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (day, self._first_day, *marks, *counts, json.dumps(dict(fresh), sort_keys=True, default=str), self._now()))
        seal = self._seal_of(day)
        assert seal is not None
        if seal == (day, self._first_day, *marks, *counts):     # otherwise another process sealed first: that seal rules
            self._last = (seal, panel, metadata)
        return seal

    def _serve(self, seal: Tuple[int, ...]) -> Tuple[Any, Mapping[str, Any]]:
        if self._last is not None and self._last[0] == seal:
            return self._last[1], self._last[2]             # the same sealed day: the same panel, not read again
        day, first_day, marks, counts = seal[0], seal[1], seal[2:6], seal[6:]
        made = self._extended(day, first_day, marks)
        if made is None or made[2] != counts:
            made = self._built(day, first_day, marks)
        if made[2] != counts:
            raise FeedUnavailable("DAILY_FEED_SEALED_DAY_CHANGED",
                                  f"{DD.day_text(day)}: the stored rows no longer match what was sealed")
        self._last = (seal, made[0], made[1])
        return made[0], made[1]

    def _built(self, day: int, first_day: int, marks: Sequence[int]) -> Tuple[Any, Dict[str, Any], Tuple[int, int, int]]:
        """The panel of a day from every row at or below ``marks``. The one large read of the feed."""
        bar_seq, funding_seq, rejection_seq, metadata_seq = marks
        with self._db.connect() as conn:
            cur = conn.cursor()
            cur.row_factory = None                          # plain tuples: about a million rows a year of history
            bars = cur.execute(
                f"SELECT symbol, day, open, high, low, close, quote_volume FROM {FEED_BAR_TABLE} WHERE day>=? AND day<=? AND seq<=?",
                (first_day, day, bar_seq)).fetchall()
            untraded = [r[0] for r in cur.execute(
                f"SELECT DISTINCT symbol FROM {FEED_REJECTION_TABLE} WHERE reason=? AND day>=? AND day<=? AND seq<=?",
                (NO_TRADES, first_day, day, rejection_seq))]
            funding = cur.execute(
                f"SELECT symbol, time_ms, interval_hours, rate FROM {FEED_FUNDING_TABLE} WHERE time_ms>=? AND time_ms<? AND seq<=? "
                "ORDER BY symbol, time_ms", (first_day * DAY_MS, close_ms(day), funding_seq)).fetchall()
            metadata = self._metadata(cur, metadata_seq)
        # a symbol is a column when the source ever gave it a valid row up to the day, traded or not: the
        # research dataset's rule for its own columns
        symbols = sorted({r[0] for r in bars} | set(untraded))
        shape = (day - first_day + 1, len(symbols))
        fields: Dict[str, np.ndarray] = {name: np.full(shape, np.nan) for name in PRICE_FIELDS}
        fields.update({name: np.zeros(shape) for name in FUNDING_FIELDS})
        self._fill(fields, {s: j for j, s in enumerate(symbols)}, first_day, bars, funding)
        self.builds += 1
        return self._panel(first_day, day, symbols, fields), metadata, (len(bars), len(funding), len(symbols))

    def _extended(self, day: int, first_day: int, marks: Sequence[int]) -> Optional[Tuple[Any, Dict[str, Any], Tuple[int, int, int]]]:
        """The panel of a day from the panel this feed served last, when that is an earlier day and no row for
        its days has arrived since: the daily case, in which only the new day is read. The result is the same
        panel ``_built`` gives, bit for bit; ``None`` when the conditions do not hold."""
        if self._last is None:
            return None
        previous, old, old_metadata = self._last
        p_day, p_first, p_bar, p_funding, p_rejection, p_metadata, p_bars, p_funding_records, _ = previous
        bar_seq, funding_seq, rejection_seq, metadata_seq = marks
        if p_first != first_day or p_day >= day or p_bar > bar_seq or p_funding > funding_seq or p_rejection > rejection_seq:
            return None
        with self._db.connect() as conn:
            cur = conn.cursor()
            cur.row_factory = None
            known = set(old.symbols)
            late = cur.execute(
                f"SELECT (SELECT COUNT(*) FROM {FEED_BAR_TABLE} WHERE seq>? AND seq<=? AND day>=? AND day<=?) + "
                f"(SELECT COUNT(*) FROM {FEED_REJECTION_TABLE} WHERE seq>? AND seq<=? AND reason=? AND day>=? AND day<=?)",
                (p_bar, bar_seq, first_day, p_day, p_rejection, rejection_seq, NO_TRADES, first_day, p_day)).fetchone()[0]
            late_funding = dict(cur.execute(
                f"SELECT symbol, COUNT(*) FROM {FEED_FUNDING_TABLE} WHERE seq>? AND seq<=? AND time_ms>=? AND time_ms<? GROUP BY symbol",
                (p_funding, funding_seq, first_day * DAY_MS, close_ms(p_day))).fetchall())
            if late or known & set(late_funding):
                return None                                 # something older arrived since: the whole panel is read again
            bars = cur.execute(
                f"SELECT symbol, day, open, high, low, close, quote_volume FROM {FEED_BAR_TABLE} WHERE day>? AND day<=? AND seq<=?",
                (p_day, day, bar_seq)).fetchall()
            untraded = [r[0] for r in cur.execute(
                f"SELECT DISTINCT symbol FROM {FEED_REJECTION_TABLE} WHERE reason=? AND day>? AND day<=? AND seq<=?",
                (NO_TRADES, p_day, day, rejection_seq))]
            funding = cur.execute(
                f"SELECT symbol, time_ms, interval_hours, rate FROM {FEED_FUNDING_TABLE} WHERE time_ms>=? AND time_ms<? AND seq<=? "
                "ORDER BY symbol, time_ms", (close_ms(p_day), close_ms(day), funding_seq)).fetchall()
            symbols = sorted(known | {r[0] for r in bars} | set(untraded))
            earlier: List[Tuple[Any, ...]] = []             # a contract that becomes a column brings its earlier funding along
            for symbol in (s for s in symbols if s not in known):
                earlier += cur.execute(
                    f"SELECT symbol, time_ms, interval_hours, rate FROM {FEED_FUNDING_TABLE} WHERE symbol=? AND time_ms>=? AND "
                    "time_ms<? AND seq<=? ORDER BY time_ms", (symbol, first_day * DAY_MS, close_ms(p_day), funding_seq)).fetchall()
            metadata = old_metadata if metadata_seq == p_metadata else self._metadata(cur, metadata_seq)
        column = {s: j for j, s in enumerate(symbols)}
        shape, kept = (day - first_day + 1, len(symbols)), len(old.days)
        place = np.fromiter((column[s] for s in old.symbols), dtype=np.int64, count=len(old.symbols))
        fields: Dict[str, np.ndarray] = {}
        for name in PRICE_FIELDS + FUNDING_FIELDS:
            fields[name] = np.full(shape, np.nan) if name in PRICE_FIELDS else np.zeros(shape)
            fields[name][:kept, place] = old.fields[name]
        self._fill(fields, column, first_day, bars, earlier)
        self._fill(fields, column, first_day, (), funding)
        self.extensions += 1
        counts = (p_bars + len(bars), p_funding_records + sum(late_funding.values()) + len(funding), len(symbols))
        return self._panel(first_day, day, symbols, fields), metadata, counts

    @staticmethod
    def _metadata(cur: Any, metadata_seq: int) -> Dict[str, Any]:
        document = cur.execute(f"SELECT document FROM {FEED_METADATA_TABLE} WHERE seq=?", (int(metadata_seq),)).fetchone()
        if document is None:
            raise FeedUnavailable("DAILY_FEED_NO_EXCHANGE_METADATA", "the sealed exchange metadata snapshot is not stored")
        return json.loads(document[0])

    @staticmethod
    def _fill(fields: Dict[str, np.ndarray], column: Mapping[str, int], first_day: int, bars: Sequence[Tuple[Any, ...]],
              funding: Sequence[Tuple[Any, ...]]) -> None:
        """Bars into the price matrices; funding records into the four funding matrices with the same sums, in
        the same order (per cell: by time), as the dataset's loader."""
        if bars:
            names, bar_days, *prices = zip(*bars)
            rows = np.asarray(bar_days, dtype=np.int64) - first_day
            cols = np.fromiter(map(column.__getitem__, names), dtype=np.int64, count=len(names))
            for name, values in zip(PRICE_FIELDS, prices):
                fields[name][rows, cols] = np.asarray(values, dtype=np.float64)
        kept = [r for r in funding if r[0] in column]
        if kept:
            names, times, intervals, rates = zip(*kept)
            ts = np.asarray(times, dtype=np.int64)
            fr, fc = ts // DAY_MS - first_day, np.fromiter(map(column.__getitem__, names), dtype=np.int64, count=len(names))
            rate, hours = np.asarray(rates, dtype=np.float64), np.asarray(intervals, dtype=np.float64)
            mid = ts % DAY_MS < DD.MIDNIGHT_WINDOW_MS
            np.add.at(fields["funding_midnight"], (fr[mid], fc[mid]), rate[mid])
            pos, neg = ~mid & (rate > 0), ~mid & (rate <= 0)
            np.add.at(fields["funding_later_positive"], (fr[pos], fc[pos]), rate[pos])
            np.add.at(fields["funding_later_negative"], (fr[neg], fc[neg]), rate[neg])
            np.add.at(fields["funding_hours"], (fr, fc), hours)

    @staticmethod
    def _panel(first_day: int, day: int, symbols: List[str], fields: Dict[str, np.ndarray]) -> Any:
        for matrix in fields.values():
            matrix.setflags(write=False)                    # a served panel is shared with later calls: nobody edits it
        return DD.DailyPanel(np.arange(first_day, day + 1, dtype=np.int64), symbols, fields)

    # ------------------------------------------------------------------------------------------- status
    def status(self) -> Dict[str, Any]:
        """For operators: what the feed holds, its last updates, what failed and what the source revised."""
        with self._db.connect() as conn:
            one = lambda sql, *a: conn.execute(sql, a).fetchone()[0]    # noqa: E731
            updates = [dict(r) for r in conn.execute(
                f"SELECT update_id, status, through_day, started_at, completed_at, symbols_requested, symbols_failed, "
                f"symbol_days_requested, source_calls, http_requests, bars_stored, funding_stored, days_rejected, revisions "
                f"FROM {FEED_UPDATE_TABLE} ORDER BY update_id DESC LIMIT 10")]
            failing = [dict(r) for r in conn.execute(
                f"SELECT symbol, failures, last_error, last_attempt_at FROM {FEED_SYMBOL_TABLE} WHERE failures>0 ORDER BY symbol")]
            newest, sealed = one(f"SELECT MAX(day) FROM {FEED_BAR_TABLE}"), one(f"SELECT MAX(day) FROM {FEED_SEAL_TABLE}")
            out = {"feed_version": FEED_VERSION, "first_day": DD.day_text(self._first_day),
                   "reserved_research_holdout": [DD.day_text(d) for d in self._reserved],
                   "symbols_tracked": one(f"SELECT COUNT(*) FROM {FEED_SYMBOL_TABLE}"),
                   "bars": one(f"SELECT COUNT(*) FROM {FEED_BAR_TABLE}"),
                   "funding_records": one(f"SELECT COUNT(*) FROM {FEED_FUNDING_TABLE}"),
                   "rejections": one(f"SELECT COUNT(*) FROM {FEED_REJECTION_TABLE}"),
                   "revisions_not_applied": one(f"SELECT COUNT(*) FROM {FEED_REVISION_TABLE}"),
                   "metadata_snapshots": one(f"SELECT COUNT(*) FROM {FEED_METADATA_TABLE}"),
                   "sealed_days": one(f"SELECT COUNT(*) FROM {FEED_SEAL_TABLE}"),
                   "newest_bar_day": None if newest is None else DD.day_text(newest),
                   "newest_sealed_day": None if sealed is None else DD.day_text(sealed),
                   "http_requests_total": one(f"SELECT COALESCE(SUM(http_requests), 0) FROM {FEED_UPDATE_TABLE}"),
                   "symbols_failing": failing, "last_updates": updates}
        return out

    def revisions(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._db.connect() as conn:
            return [dict(r) for r in conn.execute(
                f"SELECT kind, symbol, record_key, stored, offered, applied, detected_at FROM {FEED_REVISION_TABLE} "
                "ORDER BY seq DESC LIMIT ?", (int(limit),))]


def _error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {str(exc)[:200]}"


def default_daily_bars(db: Any, *, http_get: Optional[BA.Getter] = None, clock: Optional[Callable[[], int]] = None,
                       first_day: Optional[int] = None, policy: Optional[FeedPolicy] = None, workers: int = 8) -> DailyBarFeed:
    """The provider the operator command line gives the driver: the feed tables of ``db``, the public Binance
    archive as the source, missing closed days fetched on demand before a day is served. No credentials are
    read and no order can be placed. A day that cannot be served raises ``FeedUnavailable`` with a ``code``
    (``DAILY_FEED_UNAVAILABLE`` when the source could not be fetched at all)."""
    return DailyBarFeed(db, fetcher=BinanceArchiveFetcher(http_get), clock=clock, first_day=first_day, policy=policy,
                        auto_update=True, workers=workers)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.market_data.daily_feed", description="The daily bar feed (public data only).")
    sub = parser.add_subparsers(dest="command", required=True)
    upd = sub.add_parser("update")
    upd.add_argument("--max-requests", type=int, default=None, help="bound of this run, in requests of the source")
    upd.add_argument("--workers", type=int, default=8)
    sub.add_parser("status")
    args = parser.parse_args(argv)
    from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
    from shared_lib.persistence.db import DB

    db = DB()
    ensure_cati_platform_schema(db)
    if args.command == "status":
        print(json.dumps(DailyBarFeed(db).status(), indent=1, sort_keys=True, default=str))
        return 0
    policy = FeedPolicy() if args.max_requests is None else FeedPolicy(max_requests_per_update=args.max_requests)
    summary = default_daily_bars(db, policy=policy, workers=args.workers).update()
    print(json.dumps(summary, indent=1, sort_keys=True, default=str))
    return 0 if summary["status"] == COMPLETED else 1


__all__ = ["FEED_VERSION", "DEFAULT_FIRST_DAY", "FeedUnavailable", "Fetched", "DailyFetcher", "FeedPolicy",
           "BinanceArchiveFetcher", "DailyBarFeed", "default_daily_bars", "bar_rejection", "research_holdout_window",
           "close_ms", "NO_TRADES", "COMPLETED", "PARTIAL", "FAILED", "UNAVAILABLE"]

if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
