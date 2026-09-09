"""The immutable research data contract (Phase 14).

Four things have to be true of a dataset before a model is allowed anywhere
near it, and each of them is a separate failure mode:

* **It is what it says it is.** A manifest records the symbols, the venue, the
  range, the row counts and a checksum *of the data*, not of a filename.
* **It is not silently broken.** Missing bars, duplicates, out-of-order rows,
  non-positive prices and negative volume are detected and reported. Nothing is
  quietly filled in: an imputed candle is indistinguishable from a real one
  once it is in the file, and that is exactly the kind of error that makes a
  backtest confident and wrong.
* **Derived timeframes agree with the base.** 5m/15m/1h/4h are *derived* from
  1m by a deterministic rule rather than downloaded separately, because two
  independently-fetched series disagree at the edges and nobody notices until a
  result depends on it.
* **Time is not shuffled.** Partitions are chronological, and the final holdout
  is defined once and then left alone.

Provenance is fixed at ``REAL_HISTORICAL`` for exchange data. It is not a
parameter, for the same reason replay's is not: a settable field is how the
separation gets lost later.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

logger = logging.getLogger(__name__)

#: Exchange-sourced market data. Distinct from every synthetic or derived
#: provenance in shared_lib.persistence.evidence_schema.
REAL_HISTORICAL = "REAL_HISTORICAL"

FEATURE_SCHEMA_VERSION = "1.0.0"
LABEL_SCHEMA_VERSION = "1.0.0"

MINUTE_MS = 60_000

#: Derivable from 1m by whole-number aggregation. Anything not here has to be
#: downloaded, and then it is a separate series with its own provenance.
DERIVABLE = {
    "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "12h": 720, "1d": 1440,
}


class DatasetError(RuntimeError):
    """The dataset cannot be trusted for research."""


# ── Candle access ───────────────────────────────────────────────────────────


def open_time(row: Any) -> int:
    return int(row[0])


def close_time(row: Any) -> int:
    return int(row[6])


def ohlcv(row: Any) -> tuple[float, float, float, float, float]:
    return (float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]))


# ── §14.8 Data quality ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class QualityReport:
    """What is wrong with a series, stated rather than repaired."""

    symbol: str
    timeframe: str
    rows: int
    first_open_ms: int | None
    last_close_ms: int | None
    expected_rows: int
    missing_bars: int
    duplicate_opens: int
    out_of_order: int
    inconsistent_intervals: int
    non_positive_prices: int
    negative_volume: int
    ohlc_violations: int
    unrealistic_gaps: int
    missing_windows: tuple[tuple[int, int], ...] = ()

    @property
    def completeness(self) -> float:
        return 0.0 if not self.expected_rows else (
            (self.expected_rows - self.missing_bars) / self.expected_rows
        )

    @property
    def is_usable(self) -> bool:
        """Structural faults make a series unusable; gaps make it incomplete.

        A missing bar is a fact about the market data. A duplicate or
        out-of-order bar is a fact about the loader, and means the series
        cannot be reasoned about at all.
        """
        return (
            self.duplicate_opens == 0
            and self.out_of_order == 0
            and self.inconsistent_intervals == 0
            and self.non_positive_prices == 0
            and self.negative_volume == 0
            and self.ohlc_violations == 0
        )

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["completeness"] = round(self.completeness, 6)
        data["is_usable"] = self.is_usable
        data["missing_windows"] = [list(w) for w in self.missing_windows[:50]]
        return data


def assess_quality(
    rows: Sequence[Any], *, symbol: str, timeframe: str,
    gap_multiple: float = 10.0,
) -> QualityReport:
    """Inspect a series. Never modifies it, never fills anything in."""
    step = MINUTE_MS * DERIVABLE.get(timeframe, 1) if timeframe != "1m" else MINUTE_MS
    if not rows:
        return QualityReport(
            symbol=symbol, timeframe=timeframe, rows=0, first_open_ms=None,
            last_close_ms=None, expected_rows=0, missing_bars=0,
            duplicate_opens=0, out_of_order=0, inconsistent_intervals=0,
            non_positive_prices=0, negative_volume=0, ohlc_violations=0,
            unrealistic_gaps=0,
        )

    seen: set[int] = set()
    duplicates = out_of_order = inconsistent = 0
    non_positive = negative_volume = ohlc_violations = unrealistic = 0
    missing = 0
    missing_windows: list[tuple[int, int]] = []
    previous_open: int | None = None

    for row in rows:
        start, end = open_time(row), close_time(row)
        if start in seen:
            duplicates += 1
        seen.add(start)
        if previous_open is not None:
            if start < previous_open:
                out_of_order += 1
            else:
                delta = start - previous_open
                if delta != step:
                    if delta % step:
                        inconsistent += 1
                    else:
                        gap = delta // step - 1
                        missing += gap
                        missing_windows.append((previous_open + step, start - 1))
                        if delta > step * gap_multiple:
                            unrealistic += 1
        previous_open = start

        if end - start != step - 1:
            inconsistent += 1

        o, h, low, c, volume = ohlcv(row)
        if min(o, h, low, c) <= 0:
            non_positive += 1
        if volume < 0:
            negative_volume += 1
        if h < max(o, c) or low > min(o, c) or h < low:
            ohlc_violations += 1

    first_open = open_time(rows[0])
    last_close = close_time(rows[-1])
    expected = (last_close + 1 - first_open) // step

    return QualityReport(
        symbol=symbol, timeframe=timeframe, rows=len(rows),
        first_open_ms=first_open, last_close_ms=last_close,
        expected_rows=expected, missing_bars=missing,
        duplicate_opens=duplicates, out_of_order=out_of_order,
        inconsistent_intervals=inconsistent, non_positive_prices=non_positive,
        negative_volume=negative_volume, ohlc_violations=ohlc_violations,
        unrealistic_gaps=unrealistic,
        missing_windows=tuple(missing_windows),
    )


# ── §14.7 Deterministic derivation ──────────────────────────────────────────


def derive(rows: Sequence[Any], timeframe: str) -> list[list[Any]]:
    """Aggregate 1m bars into ``timeframe``. Deterministic and gap-aware.

    Only whole, aligned, gapless groups are emitted. A window missing a minute
    is dropped rather than aggregated from what happens to be there: a 15m bar
    built from 13 minutes is not a 15m bar, and nothing downstream could tell.
    """
    factor = DERIVABLE.get(timeframe)
    if factor is None:
        raise DatasetError(
            f"{timeframe} is not derivable from 1m; it would have to be "
            f"downloaded separately, which makes it a different series"
        )
    step = MINUTE_MS * factor
    by_bucket: dict[int, list[Any]] = {}
    for row in rows:
        bucket = (open_time(row) // step) * step
        by_bucket.setdefault(bucket, []).append(row)

    out: list[list[Any]] = []
    for bucket in sorted(by_bucket):
        group = by_bucket[bucket]
        if len(group) != factor:
            continue  # incomplete window: dropped, never approximated
        group.sort(key=open_time)
        if open_time(group[0]) != bucket:
            continue
        highs = [ohlcv(r)[1] for r in group]
        lows = [ohlcv(r)[2] for r in group]
        volume = sum(ohlcv(r)[4] for r in group)
        out.append([
            bucket,
            group[0][1],
            f"{max(highs):.8f}",
            f"{min(lows):.8f}",
            group[-1][4],
            f"{volume:.8f}",
            bucket + step - 1,
            "0", 0, "0", "0", "0",
        ])
    return out


# ── §14.9 / §14.10 Partitions ───────────────────────────────────────────────

TRAIN = "TRAIN"
VALIDATION = "VALIDATION"
TEST = "TEST"
FINAL_HOLDOUT = "FINAL_HOLDOUT"
PARTITIONS = (TRAIN, VALIDATION, TEST, FINAL_HOLDOUT)


@dataclass(frozen=True)
class Partition:
    name: str
    start_ms: int
    end_ms: int
    rows: int

    def contains(self, timestamp_ms: int) -> bool:
        return self.start_ms <= timestamp_ms <= self.end_ms

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self),
                "start": _iso(self.start_ms), "end": _iso(self.end_ms)}


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, timezone.utc).isoformat()


def partition(
    rows: Sequence[Any],
    *,
    train: float = 0.60,
    validation: float = 0.15,
    test: float = 0.15,
) -> tuple[Partition, ...]:
    """Split chronologically. The remainder is the final holdout.

    Deliberately not a random split, and deliberately not shuffled: with time
    series, a shuffled split lets the model see the future of its own training
    window, and every metric that follows is meaningless.
    """
    if not rows:
        raise DatasetError("cannot partition an empty series")
    fractions = (train, validation, test)
    if any(f <= 0 for f in fractions) or sum(fractions) >= 1.0:
        raise DatasetError(
            "train/validation/test must be positive and leave room for a final "
            f"holdout; got {fractions} summing to {sum(fractions)}"
        )

    ordered = sorted(rows, key=open_time)
    total = len(ordered)
    train_end = int(total * train)
    validation_end = train_end + int(total * validation)
    test_end = validation_end + int(total * test)
    bounds = [
        (TRAIN, 0, train_end),
        (VALIDATION, train_end, validation_end),
        (TEST, validation_end, test_end),
        (FINAL_HOLDOUT, test_end, total),
    ]
    out: list[Partition] = []
    for name, start, end in bounds:
        chunk = ordered[start:end]
        if not chunk:
            raise DatasetError(f"partition {name} is empty; the dataset is too small")
        out.append(Partition(
            name=name, start_ms=open_time(chunk[0]),
            end_ms=close_time(chunk[-1]), rows=len(chunk),
        ))

    # Chronological and non-overlapping, asserted rather than assumed.
    for earlier, later in zip(out, out[1:]):
        if earlier.end_ms >= later.start_ms:
            raise DatasetError(
                f"{earlier.name} overlaps {later.name}: "
                f"{earlier.end_ms} >= {later.start_ms}"
            )
    return tuple(out)


class FinalHoldoutViolation(RuntimeError):
    """Something tried to read the final holdout. §14.10 forbids it."""


def guard_final_holdout(partitions: Iterable[Partition], timestamp_ms: int,
                        *, purpose: str) -> None:
    """Raise if ``timestamp_ms`` falls in the final holdout.

    Model selection, threshold tuning, feature selection and augmentation must
    never touch it. Calling this at the point of use is cheaper than
    discovering afterwards that a number was contaminated.
    """
    for part in partitions:
        if part.name == FINAL_HOLDOUT and part.contains(timestamp_ms):
            raise FinalHoldoutViolation(
                f"{purpose} attempted to read {_iso(timestamp_ms)}, which is in "
                f"the final holdout ({_iso(part.start_ms)} -> "
                f"{_iso(part.end_ms)}). It stays untouched until final "
                f"evaluation."
            )


# ── §14.11 Manifest ─────────────────────────────────────────────────────────


def series_checksum(rows: Sequence[Any]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(repr(list(row)).encode())
    return digest.hexdigest()[:32]


@dataclass(frozen=True)
class DatasetManifest:
    """§14.11 — everything needed to identify and trust one dataset build."""

    dataset_id: str
    venue: str
    symbols: tuple[str, ...]
    base_timeframe: str
    derived_timeframes: tuple[str, ...]
    start_ms: int
    end_ms: int
    rows: Mapping[str, int]
    checksums: Mapping[str, str]
    quality: Mapping[str, Any]
    partitions: tuple[dict[str, Any], ...]
    code_revision: str | None = None
    feature_schema_version: str = FEATURE_SCHEMA_VERSION
    label_schema_version: str = LABEL_SCHEMA_VERSION
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    #: Not a parameter. Exchange data is REAL_HISTORICAL and cannot be relabelled.
    provenance: str = field(default=REAL_HISTORICAL, init=False)

    @property
    def dataset_hash(self) -> str:
        """Fingerprint of the data, not of where it was stored."""
        blob = json.dumps(
            {"checksums": dict(sorted(self.checksums.items())),
             "rows": dict(sorted(self.rows.items())),
             "symbols": list(self.symbols), "venue": self.venue,
             "base_timeframe": self.base_timeframe},
            sort_keys=True, separators=(",", ":"),
        )
        return hashlib.sha256(blob.encode()).hexdigest()[:32]

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "provenance": self.provenance,
            "dataset_hash": self.dataset_hash,
            "start": _iso(self.start_ms),
            "end": _iso(self.end_ms),
        }

    def write(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(self.to_dict(), handle, indent=2, default=str)


def build_manifest(
    series: Mapping[str, Mapping[str, Sequence[Any]]],
    *,
    dataset_id: str,
    venue: str = "binance",
    base_timeframe: str = "1m",
    partition_by: str | None = None,
    code_revision: str | None = None,
) -> DatasetManifest:
    """Assess, partition and describe a dataset in one pass."""
    if not series:
        raise DatasetError("no series supplied")

    rows: dict[str, int] = {}
    checksums: dict[str, str] = {}
    quality: dict[str, Any] = {}
    derived: set[str] = set()
    starts: list[int] = []
    ends: list[int] = []

    for symbol in sorted(series):
        for timeframe in sorted(series[symbol]):
            data = series[symbol][timeframe]
            key = f"{symbol}:{timeframe}"
            rows[key] = len(data)
            checksums[key] = series_checksum(data)
            report = assess_quality(data, symbol=symbol, timeframe=timeframe)
            quality[key] = report.to_dict()
            if timeframe != base_timeframe:
                derived.add(timeframe)
            if data:
                starts.append(open_time(data[0]))
                ends.append(close_time(data[-1]))

    anchor_symbol = partition_by or sorted(series)[0]
    anchor = series[anchor_symbol][base_timeframe]
    parts = partition(anchor)

    return DatasetManifest(
        dataset_id=dataset_id, venue=venue, symbols=tuple(sorted(series)),
        base_timeframe=base_timeframe,
        derived_timeframes=tuple(sorted(derived)),
        start_ms=min(starts), end_ms=max(ends),
        rows=rows, checksums=checksums, quality=quality,
        partitions=tuple(p.to_dict() for p in parts),
        code_revision=code_revision,
    )
