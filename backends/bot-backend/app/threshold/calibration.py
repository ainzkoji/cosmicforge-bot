"""Slow calibration -- the half of the engine that must not react quickly.

Fast market context (regime, volatility, agreement, HTF, market quality) can and
should move between candles. The two inputs here must not: they describe how the
strategy has actually been performing and what recent signal quality looks like,
and both are estimated from samples that take days to accumulate.

Three properties are enforced rather than hoped for:

* **Minimum sample.** Below the configured sample count the adjustment is
  exactly ``0.0`` and the status is ``INSUFFICIENT_SAMPLE``. Two losing trades
  must never move the entry bar.
* **Bounded.** Every adjustment is clamped to its policy bound, so a single
  outlier R-multiple cannot produce an extreme shift.
* **Dead band.** Small scores round to no adjustment at all, which stops the
  threshold from drifting on noise.

And one prohibition, tested directly: trade *frequency* is not an input. The
distribution calibrator reads the confidences of opportunities that reached the
quality stage. A quiet week produces fewer samples, never lower ones, so
inactivity can never argue for a lower bar.
"""
from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass
from typing import Any, Protocol, Sequence

from app.threshold.contracts import CalibrationStatus
from app.threshold.state import percentile

logger = logging.getLogger(__name__)

#: Scores below this magnitude produce no adjustment.
PERFORMANCE_DEAD_BAND = 0.10
DISTRIBUTION_DEAD_BAND = 0.005


def _clamp(value: float, bound: float) -> float:
    bound = abs(float(bound))
    return max(-bound, min(bound, float(value)))


@dataclass(frozen=True)
class CalibrationResult:
    score: float | None
    adjustment: float
    sample_size: int
    status: str
    detail: str = ""


NEUTRAL = CalibrationResult(
    score=None, adjustment=0.0, sample_size=0, status=CalibrationStatus.INSUFFICIENT_SAMPLE
)


class PerformanceSource(Protocol):
    """Where realised trade outcomes come from.

    A protocol rather than a concrete query so the engine can be tested without
    a database and replayed against a historical source.
    """

    def recent_r_multiples(
        self, *, bot_instance_id: str, symbol: str, timeframe: str, limit: int
    ) -> Sequence[float]:
        ...


class PerformanceCalibrator:
    """Turns realised R-multiples into a small, slow threshold adjustment."""

    def __init__(self, source: PerformanceSource | None = None) -> None:
        self._source = source

    def evaluate(
        self,
        *,
        bot_instance_id: str,
        symbol: str,
        timeframe: str,
        min_samples: int,
        lookback: int,
        bound: float,
    ) -> CalibrationResult:
        if bound <= 0:
            return CalibrationResult(None, 0.0, 0, CalibrationStatus.DISABLED, "bound is 0")
        if self._source is None:
            return CalibrationResult(
                None, 0.0, 0, CalibrationStatus.UNAVAILABLE, "no performance source"
            )

        try:
            samples = list(
                self._source.recent_r_multiples(
                    bot_instance_id=bot_instance_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=max(1, int(lookback)),
                )
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("[THRESHOLD_CALIBRATION] performance source failed: %s", exc)
            return CalibrationResult(None, 0.0, 0, CalibrationStatus.UNAVAILABLE, str(exc))

        return self.score(samples, min_samples=min_samples, bound=bound)

    @staticmethod
    def score(
        r_multiples: Sequence[float], *, min_samples: int, bound: float
    ) -> CalibrationResult:
        """Score a set of R-multiples into ``[-1, 1]`` and bound the adjustment.

        Positive score = the strategy has been paying for its risk, so the bar
        may come down a little. Negative = it has not, so the bar goes up. The
        expectancy is divided by the observed dispersion, which keeps a single
        large winner from looking like evidence.
        """
        values = [float(v) for v in r_multiples if v is not None]
        n = len(values)
        if n < max(1, int(min_samples)):
            return CalibrationResult(
                None,
                0.0,
                n,
                CalibrationStatus.INSUFFICIENT_SAMPLE,
                f"{n} < {min_samples} required",
            )

        expectancy = statistics.fmean(values)
        dispersion = statistics.pstdev(values) if n > 1 else 0.0
        if dispersion <= 0:
            # Every trade returned the same R. Real, but not evidence of skill;
            # treat the magnitude conservatively.
            normalised = 1.0 if expectancy > 0 else (-1.0 if expectancy < 0 else 0.0)
        else:
            normalised = expectancy / dispersion

        score = max(-1.0, min(1.0, normalised))
        if abs(score) < PERFORMANCE_DEAD_BAND:
            return CalibrationResult(
                round(score, 6), 0.0, n, CalibrationStatus.OK, "within dead band"
            )

        # Good performance lowers the bar, poor performance raises it.
        adjustment = _clamp(-score * abs(bound), bound)
        return CalibrationResult(
            round(score, 6),
            round(adjustment, 6),
            n,
            CalibrationStatus.OK,
            f"expectancy={expectancy:.4f} dispersion={dispersion:.4f}",
        )


class DistributionCalibrator:
    """Moves the bar toward a percentile of recent *opportunity quality*.

    The samples are confidences of opportunities that reached the quality stage.
    They say what good looks like lately. They say nothing about how often the
    bot traded, and that is the point: a week with no trades yields a smaller
    sample, not a weaker one.
    """

    @staticmethod
    def evaluate(
        samples: Sequence[float],
        *,
        base_threshold: float,
        target_percentile: float,
        min_samples: int,
        bound: float,
    ) -> CalibrationResult:
        if bound <= 0:
            return CalibrationResult(None, 0.0, 0, CalibrationStatus.DISABLED, "bound is 0")

        values = [float(v) for v in samples if v is not None]
        n = len(values)
        if n < max(1, int(min_samples)):
            return CalibrationResult(
                None,
                0.0,
                n,
                CalibrationStatus.INSUFFICIENT_SAMPLE,
                f"{n} < {min_samples} required",
            )

        target = percentile(values, target_percentile)
        if target is None:
            return CalibrationResult(None, 0.0, n, CalibrationStatus.INSUFFICIENT_SAMPLE, "")

        delta = float(target) - float(base_threshold)
        if abs(delta) < DISTRIBUTION_DEAD_BAND:
            return CalibrationResult(
                round(float(target), 6), 0.0, n, CalibrationStatus.OK, "within dead band"
            )

        return CalibrationResult(
            round(float(target), 6),
            round(_clamp(delta, bound), 6),
            n,
            CalibrationStatus.OK,
            f"p{target_percentile:.2f}={target:.4f} base={base_threshold:.4f}",
        )


class SqlitePerformanceSource:
    """Reads realised R-multiples from the canonical positions table.

    Only closed positions with a usable stop distance are counted. A position
    without one has no R to report, and inventing a denominator would be exactly
    the kind of fabricated evidence the calibration must not run on.
    """

    def __init__(self, db: Any) -> None:
        self._db = db

    def recent_r_multiples(
        self, *, bot_instance_id: str, symbol: str, timeframe: str, limit: int
    ) -> Sequence[float]:
        try:
            with self._db.connect() as conn:
                rows = conn.execute(
                    """
                    SELECT realized_pnl, risk_amount
                    FROM positions
                    WHERE bot_instance_id = ?
                      AND symbol = ?
                      AND status = 'CLOSED'
                      AND realized_pnl IS NOT NULL
                      AND risk_amount IS NOT NULL
                      AND risk_amount > 0
                    ORDER BY closed_at DESC
                    LIMIT ?
                    """,
                    (str(bot_instance_id), str(symbol).upper(), int(limit)),
                ).fetchall()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("[THRESHOLD_CALIBRATION] positions query failed: %s", exc)
            return []
        out: list[float] = []
        for realized, risk in rows:
            try:
                out.append(float(realized) / float(risk))
            except (TypeError, ValueError, ZeroDivisionError):
                continue
        return out
