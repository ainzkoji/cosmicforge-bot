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

import math
import hashlib
import json
import logging
from bisect import bisect_right
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence

logger = logging.getLogger(__name__)

#: Exchange-sourced market data. Distinct from every synthetic or derived
#: provenance in shared_lib.persistence.evidence_schema.
REAL_HISTORICAL = "REAL_HISTORICAL"
PAPER_FORWARD = "PAPER_FORWARD"
TESTNET = "TESTNET"
BROKER_DEMO = "BROKER_DEMO"
SYNTHETIC = "SYNTHETIC"
LEGACY_BACKFILL = "LEGACY_BACKFILL"
LIVE = "LIVE"
REPLAY = "REPLAY"

RESEARCH_PROVENANCE = (
    REAL_HISTORICAL, PAPER_FORWARD, TESTNET, BROKER_DEMO,
    SYNTHETIC, LEGACY_BACKFILL, LIVE, REPLAY,
)
DEFAULT_TRAINING_PROVENANCE = frozenset({REAL_HISTORICAL, PAPER_FORWARD, TESTNET, BROKER_DEMO, LIVE})

FEATURE_SCHEMA_VERSION = "1.0.0"
LABEL_SCHEMA_VERSION = "1.0.0"
TRAINING_EXAMPLE_SCHEMA_VERSION = "1.0.0"

MINUTE_MS = 60_000

#: Derivable from 1m by whole-number aggregation. Anything not here has to be
#: downloaded, and then it is a separate series with its own provenance.
DERIVABLE = {
    "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "12h": 720, "1d": 1440,
}


class DatasetError(RuntimeError):
    """The dataset cannot be trusted for research."""


class IntrabarOutcome(str, Enum):
    TP_FIRST = "TP_FIRST"
    SL_FIRST = "SL_FIRST"
    AMBIGUOUS = "AMBIGUOUS"
    NEITHER = "NEITHER"


@dataclass(frozen=True)
class InstrumentIdentity:
    venue: str
    venue_symbol: str
    canonical_symbol: str
    instrument_type: str
    asset_class: str
    base_asset: str
    quote_asset: str
    settlement_asset: str
    contract_type: str | None = None
    contract_multiplier: float = 1.0
    tick_size: float | None = None
    step_size: float | None = None
    expiry: str | None = None
    strike: float | None = None
    right: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ResearchFeatureContext:
    """Everything the deterministic bot knew at decision time t."""

    raw_ohlcv: Mapping[str, Any]
    htf_context: Mapping[str, Any]
    funding: Mapping[str, Any] = field(default_factory=dict)
    open_interest: Mapping[str, Any] = field(default_factory=dict)
    basis: Mapping[str, Any] = field(default_factory=dict)
    regime: Mapping[str, Any] = field(default_factory=dict)
    expert_outputs: Mapping[str, Any] = field(default_factory=dict)
    ensemble: Mapping[str, Any] = field(default_factory=dict)
    threshold: Mapping[str, Any] = field(default_factory=dict)
    decision: Mapping[str, Any] = field(default_factory=dict)
    risk: Mapping[str, Any] = field(default_factory=dict)
    quality: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {k: _plain(v) for k, v in asdict(self).items()}


@dataclass(frozen=True)
class FutureLabelSet:
    """Future outcomes. These are labels, never features."""

    horizon_bars: int
    horizon_ms: int
    mfe: float
    mae: float
    gross_return: float
    net_return: float
    r_multiple: float | None
    tp_sl_outcome: str
    cost_model_hash: str
    fee_cost: float
    spread_cost: float
    slippage_cost: float
    funding_cost: float
    label_complete: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TrainingExample:
    dataset_id: str
    dataset_hash: str
    instrument: InstrumentIdentity
    timeframe: str
    decision_timestamp_ms: int
    policy_version: str
    strategy_version: str
    feature_schema_version: str
    label_schema_version: str
    features: ResearchFeatureContext
    labels: FutureLabelSet
    provenance: str
    market_data_provenance: str = REAL_HISTORICAL
    schema_version: str = TRAINING_EXAMPLE_SCHEMA_VERSION

    @property
    def example_id(self) -> str:
        payload = {
            "dataset_id": self.dataset_id,
            "dataset_hash": self.dataset_hash,
            "venue": self.instrument.venue,
            "venue_symbol": self.instrument.venue_symbol,
            "canonical_symbol": self.instrument.canonical_symbol,
            "timeframe": self.timeframe,
            "decision_timestamp_ms": self.decision_timestamp_ms,
            "policy_version": self.policy_version,
            "strategy_version": self.strategy_version,
            "schema_version": self.schema_version,
        }
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return "tex_" + hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]

    def to_dict(self) -> dict[str, Any]:
        return {
            "example_id": self.example_id,
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "dataset_hash": self.dataset_hash,
            "instrument": self.instrument.to_dict(),
            "timeframe": self.timeframe,
            "decision_timestamp_ms": self.decision_timestamp_ms,
            "policy_version": self.policy_version,
            "strategy_version": self.strategy_version,
            "feature_schema_version": self.feature_schema_version,
            "label_schema_version": self.label_schema_version,
            "features": self.features.to_dict(),
            "labels": self.labels.to_dict(),
            "provenance": self.provenance,
            "market_data_provenance": self.market_data_provenance,
        }


# ── Candle access ───────────────────────────────────────────────────────────


def open_time(row: Any) -> int:
    return int(row[0])


def close_time(row: Any) -> int:
    return int(row[6])


def ohlcv(row: Any) -> tuple[float, float, float, float, float]:
    return (float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]))


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def assert_known_provenance(provenance: str) -> str:
    if provenance not in RESEARCH_PROVENANCE:
        raise DatasetError(f"unknown research provenance: {provenance}")
    return provenance


def include_for_training(
    provenance: str,
    *,
    allowed: Iterable[str] | None = None,
) -> bool:
    """Default future production training excludes synthetic and legacy rows."""

    allowed_set = frozenset(allowed) if allowed is not None else DEFAULT_TRAINING_PROVENANCE
    return assert_known_provenance(provenance) in allowed_set


def filter_by_provenance(
    examples: Iterable[TrainingExample],
    *,
    allowed: Iterable[str] | None = None,
) -> tuple[TrainingExample, ...]:
    return tuple(e for e in examples if include_for_training(e.provenance, allowed=allowed))


def latest_at_or_before(observations: Sequence[Mapping[str, Any]], timestamp_ms: int) -> Mapping[str, Any]:
    """Return the latest observation visible at t, or an explicit missing marker."""

    visible = [
        row for row in observations
        if int(row.get("timestamp_ms", row.get("time_ms", row.get("open_time", 0)))) <= timestamp_ms
    ]
    if not visible:
        return {"available": False, "timestamp_ms": None, "value": None}
    latest = max(visible, key=lambda r: int(r.get("timestamp_ms", r.get("time_ms", r.get("open_time", 0)))))
    return {**dict(latest), "available": True}


def visible_rows(rows: Sequence[Any], timestamp_ms: int) -> tuple[Any, ...]:
    return tuple(row for row in rows if close_time(row) <= timestamp_ms)


def future_rows(rows: Sequence[Any], timestamp_ms: int, horizon_bars: int) -> tuple[Any, ...]:
    """Return the bounded future window from a chronologically sorted candle series."""
    horizon = max(0, int(horizon_bars))
    if horizon == 0 or not rows:
        return ()

    start = bisect_right(rows, int(timestamp_ms), key=open_time)
    return tuple(rows[start : start + horizon])


def build_future_labels(
    *,
    entry_price: float,
    future: Sequence[Any],
    side: str = "LONG",
    stop_price: float | None = None,
    target_price: float | None = None,
    risk_per_unit: float | None = None,
    horizon_bars: int,
    cost_model_hash: str = "zero",
    fee_cost: float = 0.0,
    spread_cost: float = 0.0,
    slippage_cost: float = 0.0,
    funding_cost: float = 0.0,
) -> FutureLabelSet:
    entry = float(entry_price)
    if entry <= 0:
        raise DatasetError("entry_price must be positive")
    side_s = str(side or "LONG").upper()
    direction = -1.0 if side_s in {"SHORT", "SELL"} else 1.0
    highs = [ohlcv(row)[1] for row in future]
    lows = [ohlcv(row)[2] for row in future]
    closes = [ohlcv(row)[3] for row in future]
    if not future:
        return FutureLabelSet(
            horizon_bars=int(horizon_bars), horizon_ms=0, mfe=0.0, mae=0.0,
            gross_return=0.0, net_return=0.0, r_multiple=None,
            tp_sl_outcome=IntrabarOutcome.NEITHER.value,
            cost_model_hash=cost_model_hash, fee_cost=fee_cost,
            spread_cost=spread_cost, slippage_cost=slippage_cost,
            funding_cost=funding_cost, label_complete=False,
        )

    if direction > 0:
        mfe = (max(highs) - entry) / entry
        mae = (min(lows) - entry) / entry
        gross_return = (closes[-1] - entry) / entry
    else:
        mfe = (entry - min(lows)) / entry
        mae = (entry - max(highs)) / entry
        gross_return = (entry - closes[-1]) / entry

    costs = float(fee_cost) + float(spread_cost) + float(slippage_cost) + float(funding_cost)
    net_return = gross_return - costs / entry
    risk = abs(float(risk_per_unit)) if risk_per_unit else None
    r_multiple = None if not risk else (gross_return * entry - costs) / risk
    outcome = _tp_sl_outcome(future, side_s, stop_price=stop_price, target_price=target_price)
    horizon_ms = close_time(future[-1]) - open_time(future[0]) + 1
    return FutureLabelSet(
        horizon_bars=int(horizon_bars), horizon_ms=int(horizon_ms),
        mfe=float(mfe), mae=float(mae), gross_return=float(gross_return),
        net_return=float(net_return), r_multiple=None if r_multiple is None else float(r_multiple),
        tp_sl_outcome=outcome, cost_model_hash=cost_model_hash,
        fee_cost=float(fee_cost), spread_cost=float(spread_cost),
        slippage_cost=float(slippage_cost), funding_cost=float(funding_cost),
        label_complete=len(future) >= int(horizon_bars),
    )


def _tp_sl_outcome(
    future: Sequence[Any],
    side: str,
    *,
    stop_price: float | None,
    target_price: float | None,
) -> str:
    if stop_price is None or target_price is None:
        return IntrabarOutcome.NEITHER.value
    stop = float(stop_price)
    target = float(target_price)
    for row in future:
        _, high, low, _, _ = ohlcv(row)
        if side in {"SHORT", "SELL"}:
            tp = low <= target
            sl = high >= stop
        else:
            tp = high >= target
            sl = low <= stop
        if tp and sl:
            return IntrabarOutcome.AMBIGUOUS.value
        if tp:
            return IntrabarOutcome.TP_FIRST.value
        if sl:
            return IntrabarOutcome.SL_FIRST.value
    return IntrabarOutcome.NEITHER.value


def build_training_example(
    *,
    dataset_id: str,
    dataset_hash: str,
    instrument: InstrumentIdentity,
    timeframe: str,
    decision_timestamp_ms: int,
    rows: Sequence[Any],
    htf_rows: Sequence[Any] = (),
    horizon_bars: int = 12,
    policy_version: str,
    strategy_version: str,
    provenance: str = REAL_HISTORICAL,
    funding_observations: Sequence[Mapping[str, Any]] = (),
    open_interest_observations: Sequence[Mapping[str, Any]] = (),
    basis_observations: Sequence[Mapping[str, Any]] = (),
    regime: Mapping[str, Any] | None = None,
    expert_outputs: Mapping[str, Any] | None = None,
    ensemble: Mapping[str, Any] | None = None,
    threshold: Mapping[str, Any] | None = None,
    decision: Mapping[str, Any] | None = None,
    risk: Mapping[str, Any] | None = None,
    quality: Mapping[str, Any] | None = None,
    stop_price: float | None = None,
    target_price: float | None = None,
    side: str = "LONG",
    cost_model_hash: str = "zero",
    fee_cost: float = 0.0,
    spread_cost: float = 0.0,
    slippage_cost: float = 0.0,
    funding_cost: float = 0.0,
) -> TrainingExample:
    assert_known_provenance(provenance)
    visible = visible_rows(rows, decision_timestamp_ms)
    if not visible:
        raise DatasetError("no decision-time OHLCV rows are visible")
    visible_htf = visible_rows(htf_rows, decision_timestamp_ms) if htf_rows else ()
    if htf_rows and visible_htf and close_time(visible_htf[-1]) > decision_timestamp_ms:
        raise DatasetError("HTF context leaks past decision timestamp")
    current = visible[-1]
    future = future_rows(rows, decision_timestamp_ms, horizon_bars)
    entry = ohlcv(current)[3]
    risk_per_unit = abs(entry - float(stop_price)) if stop_price is not None else None
    features = ResearchFeatureContext(
        raw_ohlcv={
            "open_time_ms": open_time(current),
            "close_time_ms": close_time(current),
            "open": ohlcv(current)[0],
            "high": ohlcv(current)[1],
            "low": ohlcv(current)[2],
            "close": ohlcv(current)[3],
            "volume": ohlcv(current)[4],
        },
        htf_context={
            "available": bool(visible_htf),
            "latest_close_time_ms": close_time(visible_htf[-1]) if visible_htf else None,
            "rows": len(visible_htf),
        },
        funding=latest_at_or_before(funding_observations, decision_timestamp_ms),
        open_interest=latest_at_or_before(open_interest_observations, decision_timestamp_ms),
        basis=latest_at_or_before(basis_observations, decision_timestamp_ms),
        regime=regime or {},
        expert_outputs=expert_outputs or {},
        ensemble=ensemble or {},
        threshold=threshold or {},
        decision=decision or {},
        risk=risk or {},
        quality={
            "feature_complete": True,
            "label_complete": len(future) >= int(horizon_bars),
            "htf_complete": bool(visible_htf) if htf_rows else None,
            "funding_available": bool(funding_observations),
            "open_interest_available": bool(open_interest_observations),
            "basis_available": bool(basis_observations),
            **dict(quality or {}),
        },
    )
    labels = build_future_labels(
        entry_price=entry, future=future, side=side, stop_price=stop_price,
        target_price=target_price, risk_per_unit=risk_per_unit,
        horizon_bars=horizon_bars, cost_model_hash=cost_model_hash,
        fee_cost=fee_cost, spread_cost=spread_cost,
        slippage_cost=slippage_cost, funding_cost=funding_cost,
    )
    return TrainingExample(
        dataset_id=dataset_id,
        dataset_hash=dataset_hash,
        instrument=instrument,
        timeframe=timeframe,
        decision_timestamp_ms=int(decision_timestamp_ms),
        policy_version=policy_version,
        strategy_version=strategy_version,
        feature_schema_version=FEATURE_SCHEMA_VERSION,
        label_schema_version=LABEL_SCHEMA_VERSION,
        features=features,
        labels=labels,
        provenance=provenance,
        market_data_provenance=REAL_HISTORICAL,
    )


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
                if delta > 0 and delta != step:
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
        if not all(math.isfinite(x) for x in (o, h, low, c, volume)) or min(o, h, low, c) <= 0:
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
FORMAL_EVALUATION = "FORMAL_EVALUATION"


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


@dataclass(frozen=True)
class PurgeEmbargoPolicy:
    label_horizon_ms: int
    embargo_ms: int = 0

    def __post_init__(self) -> None:
        if self.label_horizon_ms < 0 or self.embargo_ms < 0:
            raise DatasetError("purge/embargo durations must be non-negative")

    @property
    def purge_ms(self) -> int:
        return int(self.label_horizon_ms)


@dataclass(frozen=True)
class PurgeEmbargoReport:
    label_horizon_ms: int
    purge_ms: int
    embargo_ms: int
    excluded_counts: Mapping[str, int]
    purge_ranges: tuple[dict[str, Any], ...]
    embargo_ranges: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "label_horizon_ms": self.label_horizon_ms,
            "purge_ms": self.purge_ms,
            "embargo_ms": self.embargo_ms,
            "excluded_counts": dict(self.excluded_counts),
            "purge_ranges": list(self.purge_ranges),
            "embargo_ranges": list(self.embargo_ranges),
        }


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
    # tolerance: 0.7 + 0.2 + 0.1 == 0.9999999999999999 in floating point, which is still "no holdout"
    if any(f <= 0 for f in fractions) or sum(fractions) >= 1.0 - 1e-9:
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


def purge_embargo_partitions(
    examples: Iterable[TrainingExample],
    partitions: Sequence[Partition],
    policy: PurgeEmbargoPolicy,
) -> tuple[tuple[TrainingExample, ...], PurgeEmbargoReport]:
    """Remove examples whose labels or embargo window cross split boundaries.

    The purge is label-aware: an example remains in a partition only when its
    future label horizon ends inside that same partition. The embargo removes
    early examples from the following partition when the methodology requires a
    quiet gap after a boundary.
    """

    ordered_parts = tuple(sorted(partitions, key=lambda p: p.start_ms))
    if len(ordered_parts) < 2:
        raise DatasetError("purge/embargo requires at least two partitions")

    purge_ranges: list[dict[str, Any]] = []
    embargo_ranges: list[dict[str, Any]] = []
    excluded_counts: dict[str, int] = {}

    def part_for(ts: int) -> Partition | None:
        return next((p for p in ordered_parts if p.contains(ts)), None)

    for left, right in zip(ordered_parts, ordered_parts[1:]):
        purge_start = max(left.start_ms, left.end_ms - policy.purge_ms + 1)
        purge_ranges.append({
            "from_partition": left.name,
            "to_partition": right.name,
            "start_ms": purge_start,
            "end_ms": left.end_ms,
            "start": _iso(purge_start),
            "end": _iso(left.end_ms),
        })
        if policy.embargo_ms > 0:
            embargo_end = min(right.end_ms, right.start_ms + policy.embargo_ms - 1)
            embargo_ranges.append({
                "from_partition": left.name,
                "to_partition": right.name,
                "start_ms": right.start_ms,
                "end_ms": embargo_end,
                "start": _iso(right.start_ms),
                "end": _iso(embargo_end),
            })

    kept: list[TrainingExample] = []
    for example in sorted(examples, key=lambda e: (e.decision_timestamp_ms, e.example_id)):
        part = part_for(example.decision_timestamp_ms)
        if part is None:
            excluded_counts["OUTSIDE_PARTITIONS"] = excluded_counts.get("OUTSIDE_PARTITIONS", 0) + 1
            continue
        label_end = example.decision_timestamp_ms + int(example.labels.horizon_ms or policy.label_horizon_ms)
        if label_end > part.end_ms:
            key = f"{part.name}_PURGE"
            excluded_counts[key] = excluded_counts.get(key, 0) + 1
            continue
        embargoed = False
        if policy.embargo_ms > 0:
            for left, right in zip(ordered_parts, ordered_parts[1:]):
                if part.name == right.name and example.decision_timestamp_ms < right.start_ms + policy.embargo_ms:
                    key = f"{right.name}_EMBARGO"
                    excluded_counts[key] = excluded_counts.get(key, 0) + 1
                    embargoed = True
                    break
        if embargoed:
            continue
        kept.append(example)

    return tuple(kept), PurgeEmbargoReport(
        label_horizon_ms=int(policy.label_horizon_ms),
        purge_ms=policy.purge_ms,
        embargo_ms=int(policy.embargo_ms),
        excluded_counts=excluded_counts,
        purge_ranges=tuple(purge_ranges),
        embargo_ranges=tuple(embargo_ranges),
    )


class FinalHoldoutViolation(RuntimeError):
    """Something tried to read the final holdout. §14.10 forbids it."""


_HOLDOUT_ACCESS_AUDIT: list[dict[str, Any]] = []


def holdout_access_audit() -> tuple[dict[str, Any], ...]:
    return tuple(_HOLDOUT_ACCESS_AUDIT)


def guard_final_holdout(partitions: Iterable[Partition], timestamp_ms: int,
                        *, purpose: str, mode: str | None = None) -> None:
    """Raise if ``timestamp_ms`` falls in the final holdout.

    Model selection, threshold tuning, feature selection and augmentation must
    never touch it. Calling this at the point of use is cheaper than
    discovering afterwards that a number was contaminated.
    """
    for part in partitions:
        if part.name == FINAL_HOLDOUT and part.contains(timestamp_ms):
            if mode == FORMAL_EVALUATION:
                _HOLDOUT_ACCESS_AUDIT.append({
                    "timestamp_ms": int(timestamp_ms),
                    "timestamp": _iso(timestamp_ms),
                    "purpose": purpose,
                    "mode": mode,
                    "partition": part.to_dict(),
                    "accessed_at": datetime.now(timezone.utc).isoformat(),
                })
                return
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
