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
        except Exception as exc:
            # The source could not be read: a broken subsystem. That is
            # UNAVAILABLE, never INSUFFICIENT_SAMPLE -- which would claim the
            # data simply has not accumulated yet. The adjustment stays neutral.
            logger.warning("[THRESHOLD_CALIBRATION] performance source unavailable: %s", exc)
            return CalibrationResult(
                None, 0.0, 0, CalibrationStatus.UNAVAILABLE,
                " ".join(f"{type(exc).__name__}: {exc}".split())[:500],
            )

        try:
            return self.score(samples, min_samples=min_samples, bound=bound)
        except Exception as exc:
            # The data was read but could not be scored. Distinct from both a
            # shortage and an unreadable source; still neutral.
            logger.error("[THRESHOLD_CALIBRATION] performance scoring failed: %s", exc)
            return CalibrationResult(
                None, 0.0, len(samples), CalibrationStatus.ERROR,
                " ".join(f"{type(exc).__name__}: {exc}".split())[:500],
            )

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


class PerformanceSourceUnavailable(RuntimeError):
    """The realised-performance source could not be read.

    Raised rather than returned as an empty list: an empty list reads as "no
    trades yet" (INSUFFICIENT_SAMPLE), and that is how a query against a column
    that never existed passed for a data shortage on every evaluated candle.
    """


#: Provenance whose positions may calibrate a live threshold. Replay, backtest,
#: synthetic, validation and test evidence never move the production bar.
CALIBRATION_PROVENANCE = ("PAPER_FORWARD", "TESTNET", "LIVE_MAINNET")

#: A closed position's remaining quantity below this is zero.
_QTY_TOLERANCE = 1e-9

#: The canonical realised-R query. Kept at module level so a schema contract test
#: can run exactly this statement against a freshly migrated database.
#:
#: Economic definitions, resolved from the writers rather than assumed:
#:
#: * ``positions.realized_pnl`` is **gross**: the sum of each close leg's price
#:   P&L, ``(exit - entry) * qty`` (fill_bridge.project_fill). Fees are carried
#:   separately.
#: * ``positions.fees`` holds close-leg fees only -- the OPEN fill's fee is not
#:   projected onto the position. The complete fee is therefore the sum of every
#:   ``trade_fills`` leg for the position, entry included.
#: * Partial closes accumulate into ``realized_pnl`` and ``trade_fills``; only
#:   a position that is CLOSED with nothing remaining is a complete result.
#: * One position has one originating decision (``positions.decision_id``, the
#:   decision active when the OPEN fill landed). A position that was ADDED to
#:   carries more risk than its originating decision approved, so it has no
#:   well-defined R and is excluded.
#: * ``trading_decisions.risk_amount`` is the risk approved at entry for the
#:   executed quantity: |entry - stop| * quantity.
#: * A position whose entry fee is unknown (no OPEN fill with a fee) has an
#:   unknown net result and is excluded rather than assumed fee-free.
#:
#:     R = (gross realised P&L - all fees) / approved risk amount
REALIZED_R_QUERY = f"""
    SELECT
        p.position_id,
        p.realized_pnl                               AS gross_realized_pnl,
        d.risk_amount                                AS approved_risk,
        (SELECT SUM(f.fee) FROM trade_fills f
          WHERE f.position_id = p.position_id)       AS total_fees,
        (SELECT COUNT(*) FROM trade_fills f
          WHERE f.position_id = p.position_id
            AND f.action = 'OPEN' AND f.fee IS NOT NULL) AS entry_fee_legs
    FROM positions p
    JOIN trading_decisions d ON d.decision_id = p.decision_id
    WHERE p.bot_instance_id = ?
      AND p.symbol = ?
      AND p.status = 'CLOSED'
      AND p.closed_at IS NOT NULL
      AND p.remaining_qty <= {_QTY_TOLERANCE}
      AND p.realized_pnl IS NOT NULL
      AND d.risk_amount IS NOT NULL
      AND d.risk_amount > 0
      AND p.provenance IN ({", ".join("'" + p + "'" for p in CALIBRATION_PROVENANCE)})
      AND d.provenance IN ({", ".join("'" + p + "'" for p in CALIBRATION_PROVENANCE)})
      AND NOT EXISTS (
          SELECT 1 FROM position_events e
           WHERE e.position_id = p.position_id AND e.event_type = 'ADDED'
      )
    ORDER BY p.closed_at DESC
    LIMIT ?
"""


class SqlitePerformanceSource:
    """Realised R-multiples from canonical lineage: positions joined to decisions.

    ``risk_amount`` lives on ``trading_decisions``, not ``positions``; the
    previous query selected ``positions.risk_amount``, a column that never
    existed, and failed on every evaluated candle. See :data:`REALIZED_R_QUERY`
    for the economic definitions.

    Only complete results count. A position without an approved risk amount or
    a known entry fee has no R to report, and inventing a denominator or a fee
    would be exactly the fabricated evidence calibration must not run on.
    """

    def __init__(self, db: Any) -> None:
        self._db = db

    def recent_r_multiples(
        self, *, bot_instance_id: str, symbol: str, timeframe: str, limit: int
    ) -> Sequence[float]:
        try:
            with self._db.connect() as conn:
                rows = conn.execute(
                    REALIZED_R_QUERY,
                    (str(bot_instance_id), str(symbol).upper(), int(limit)),
                ).fetchall()
        except Exception as exc:
            raise PerformanceSourceUnavailable(
                f"realised-R query failed: {type(exc).__name__}: {exc}"
            ) from exc
        out: list[float] = []
        for _position_id, gross, risk, total_fees, entry_fee_legs in rows:
            if not entry_fee_legs:
                continue
            try:
                net = float(gross) - float(total_fees or 0.0)
                out.append(net / float(risk))
            except (TypeError, ValueError, ZeroDivisionError):
                continue
        return out
