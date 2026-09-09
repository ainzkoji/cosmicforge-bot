"""Adaptive threshold state -- persisted, partitioned, and restart-safe.

Two requirements shape this module.

**Restart safety.** Smoothing and rate limiting are defined relative to the
previous threshold. If that value is lost on restart the engine silently jumps
to its unsmoothed proposal, which means the same candle produces a different
answer depending on whether a process happened to restart before it. So the
previous threshold and the opportunity-distribution samples are persisted and
reloaded.

**No cross-symbol leak.** State is keyed on
``(bot_instance_id, symbol, timeframe, strategy_version)``. There is no module
level singleton and no shared accumulator: BTC's history cannot move ETH's
threshold, and one bot cannot move another's.
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

logger = logging.getLogger(__name__)

StateKey = tuple[str, str, str, str]


@dataclass
class ThresholdState:
    """Everything the engine needs to carry from one candle to the next."""

    bot_instance_id: str
    symbol: str
    timeframe: str
    strategy_version: str

    previous_threshold: float | None = None
    last_candle_time: int | None = None
    #: Confidences of opportunities that actually reached the quality stage.
    #: Rejected candles contribute nothing: the distribution describes signal
    #: quality, not how often the bot traded.
    distribution_samples: list[float] = field(default_factory=list)
    updated_at: str | None = None
    engine_version: str | None = None
    policy_hash: str | None = None

    @property
    def key(self) -> StateKey:
        return (
            str(self.bot_instance_id),
            str(self.symbol).upper(),
            str(self.timeframe),
            str(self.strategy_version),
        )

    def copy(self) -> "ThresholdState":
        return ThresholdState(
            bot_instance_id=self.bot_instance_id,
            symbol=self.symbol,
            timeframe=self.timeframe,
            strategy_version=self.strategy_version,
            previous_threshold=self.previous_threshold,
            last_candle_time=self.last_candle_time,
            distribution_samples=list(self.distribution_samples),
            updated_at=self.updated_at,
            engine_version=self.engine_version,
            policy_hash=self.policy_hash,
        )

    def with_sample(self, confidence: float, *, window: int) -> "ThresholdState":
        out = self.copy()
        out.distribution_samples.append(round(float(confidence), 6))
        if window > 0 and len(out.distribution_samples) > window:
            del out.distribution_samples[: len(out.distribution_samples) - window]
        return out


class ThresholdStateStore:
    """In-memory store, partitioned by state key.

    Used directly in tests and as the cache in front of
    :class:`SqliteThresholdStateStore`.
    """

    def __init__(self) -> None:
        self._states: dict[StateKey, ThresholdState] = {}
        self._lock = threading.RLock()

    @staticmethod
    def make_key(
        bot_instance_id: str, symbol: str, timeframe: str, strategy_version: str
    ) -> StateKey:
        return (
            str(bot_instance_id),
            str(symbol).upper(),
            str(timeframe),
            str(strategy_version),
        )

    def get(self, key: StateKey) -> ThresholdState:
        with self._lock:
            existing = self._states.get(key)
            if existing is not None:
                return existing.copy()
        bot, symbol, timeframe, version = key
        return ThresholdState(
            bot_instance_id=bot,
            symbol=symbol,
            timeframe=timeframe,
            strategy_version=version,
        )

    def put(self, state: ThresholdState) -> None:
        with self._lock:
            self._states[state.key] = state.copy()

    def keys(self) -> list[StateKey]:
        with self._lock:
            return sorted(self._states)

    def clear(self) -> None:
        with self._lock:
            self._states.clear()


class SqliteThresholdStateStore(ThresholdStateStore):
    """State store backed by the canonical database.

    Writes are best-effort and never raise into the trading path: losing a
    smoothing anchor degrades adaptation, while raising here would stop the bot
    from evaluating a candle at all. Every failure is logged, and
    :mod:`app.threshold.diagnostics` reports a store that is failing.
    """

    def __init__(self, db: Any) -> None:
        super().__init__()
        self._db = db
        self._write_failures = 0
        self._read_failures = 0

    @property
    def write_failures(self) -> int:
        return self._write_failures

    @property
    def read_failures(self) -> int:
        return self._read_failures

    def get(self, key: StateKey) -> ThresholdState:
        with self._lock:
            cached = self._states.get(key)
        if cached is not None:
            return cached.copy()

        bot, symbol, timeframe, version = key
        try:
            with self._db.connect() as conn:
                row = conn.execute(
                    """
                    SELECT previous_threshold, last_candle_time, distribution_samples_json,
                           updated_at, engine_version, policy_hash
                    FROM adaptive_threshold_state
                    WHERE bot_instance_id = ? AND symbol = ? AND timeframe = ?
                      AND strategy_version = ?
                    """,
                    (bot, symbol, timeframe, version),
                ).fetchone()
        except Exception as exc:  # pragma: no cover - defensive
            self._read_failures += 1
            logger.warning("[THRESHOLD_STATE] read failed for %s: %s", key, exc)
            row = None

        state = ThresholdState(
            bot_instance_id=bot,
            symbol=symbol,
            timeframe=timeframe,
            strategy_version=version,
        )
        if row is not None:
            state.previous_threshold = row[0]
            state.last_candle_time = row[1]
            state.distribution_samples = _load_samples(row[2])
            state.updated_at = row[3]
            state.engine_version = row[4]
            state.policy_hash = row[5]
            with self._lock:
                self._states[key] = state.copy()
        return state

    def put(self, state: ThresholdState) -> None:
        super().put(state)
        try:
            with self._db.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO adaptive_threshold_state (
                        bot_instance_id, symbol, timeframe, strategy_version,
                        previous_threshold, last_candle_time,
                        distribution_samples_json, updated_at,
                        engine_version, policy_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(bot_instance_id, symbol, timeframe, strategy_version)
                    DO UPDATE SET
                        previous_threshold = excluded.previous_threshold,
                        last_candle_time = excluded.last_candle_time,
                        distribution_samples_json = excluded.distribution_samples_json,
                        updated_at = excluded.updated_at,
                        engine_version = excluded.engine_version,
                        policy_hash = excluded.policy_hash
                    """,
                    (
                        state.bot_instance_id,
                        str(state.symbol).upper(),
                        state.timeframe,
                        state.strategy_version,
                        state.previous_threshold,
                        state.last_candle_time,
                        json.dumps(state.distribution_samples),
                        state.updated_at,
                        state.engine_version,
                        state.policy_hash,
                    ),
                )
        except Exception as exc:  # pragma: no cover - defensive
            self._write_failures += 1
            logger.warning("[THRESHOLD_STATE] write failed for %s: %s", state.key, exc)


def _load_samples(raw: Any) -> list[float]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return []
    if not isinstance(parsed, Iterable):
        return []
    out: list[float] = []
    for item in parsed:
        try:
            out.append(float(item))
        except (TypeError, ValueError):
            continue
    return out


def percentile(samples: Sequence[float], q: float) -> float | None:
    """Linear-interpolated percentile. ``q`` in [0, 1].

    Written out rather than pulled from numpy so the engine has no numeric
    dependency whose version could change a persisted threshold.
    """
    values = sorted(float(v) for v in samples)
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    q = max(0.0, min(1.0, float(q)))
    position = q * (len(values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight
