"""Writes threshold decisions and expert evidence to the canonical database.

Two rules the previous evidence layer got wrong:

* A row that was never evaluated stores ``NULL``. The contract already refuses
  to build such a decision with a number, and this writer passes ``None``
  through rather than coercing it to ``0.0`` on the way to SQLite.
* Expert evidence comes from the production evaluation. Nothing here re-runs a
  strategy, so the recorded votes are the votes that were actually used.
"""
from __future__ import annotations

import logging
from typing import Any, Iterable, Sequence

from app.threshold.contracts import AdaptiveThresholdDecision, ExpertEvidence

logger = logging.getLogger(__name__)

_DECISION_COLUMNS = (
    "threshold_decision_id",
    "bot_instance_id",
    "run_id",
    "cycle_id",
    "decision_id",
    "opportunity_id",
    "market_snapshot_id",
    "symbol",
    "venue",
    "market_type",
    "timeframe",
    "closed_candle_time",
    "decided_at",
    "threshold_engine_version",
    "threshold_mode",
    "policy_hash",
    "provenance",
    "status",
    "opportunity_confidence",
    "base_threshold",
    "regime",
    "regime_adjustment",
    "volatility_score",
    "volatility_adjustment",
    "expert_agreement_score",
    "agreement_adjustment",
    "htf_alignment_score",
    "htf_adjustment",
    "market_quality_score",
    "market_quality_adjustment",
    "performance_score",
    "performance_adjustment",
    "performance_sample_size",
    "performance_status",
    "distribution_percentile",
    "distribution_adjustment",
    "distribution_sample_size",
    "distribution_status",
    "market_threshold",
    "calibration_adjustment",
    "raw_unclamped_threshold",
    "smoothed_threshold",
    "rate_limited_threshold",
    "final_threshold",
    "min_threshold",
    "max_threshold",
    "previous_threshold",
    "smoothing_applied",
    "rate_limit_applied",
    "clamp_applied",
    "passed",
    "reason",
    "detail",
    "reconciles",
)


def record_threshold_decision(
    db: Any,
    decision: AdaptiveThresholdDecision,
    *,
    decision_id: str | None = None,
    provenance: str = "PAPER_FORWARD",
) -> str:
    """Persist one threshold decision and its expert evidence."""
    values = {
        "threshold_decision_id": decision.threshold_decision_id,
        "bot_instance_id": decision.bot_instance_id,
        "run_id": decision.run_id,
        "cycle_id": decision.cycle_id,
        "decision_id": decision_id,
        "opportunity_id": decision.opportunity_id,
        "market_snapshot_id": decision.market_snapshot_id,
        "symbol": decision.symbol,
        "venue": decision.venue,
        "market_type": decision.market_type,
        "timeframe": decision.timeframe,
        "closed_candle_time": decision.closed_candle_time,
        "decided_at": decision.decided_at,
        "threshold_engine_version": decision.threshold_engine_version,
        "threshold_mode": decision.threshold_mode,
        "policy_hash": decision.policy_hash,
        "provenance": provenance,
        "status": decision.status,
        "opportunity_confidence": decision.opportunity_confidence,
        "base_threshold": decision.base_threshold,
        "regime": decision.regime,
        "regime_adjustment": decision.regime_adjustment,
        "volatility_score": decision.volatility_score,
        "volatility_adjustment": decision.volatility_adjustment,
        "expert_agreement_score": decision.expert_agreement_score,
        "agreement_adjustment": decision.agreement_adjustment,
        "htf_alignment_score": decision.htf_alignment_score,
        "htf_adjustment": decision.htf_adjustment,
        "market_quality_score": decision.market_quality_score,
        "market_quality_adjustment": decision.market_quality_adjustment,
        "performance_score": decision.performance_score,
        "performance_adjustment": decision.performance_adjustment,
        "performance_sample_size": decision.performance_sample_size,
        "performance_status": decision.performance_status,
        "distribution_percentile": decision.distribution_percentile,
        "distribution_adjustment": decision.distribution_adjustment,
        "distribution_sample_size": decision.distribution_sample_size,
        "distribution_status": decision.distribution_status,
        "market_threshold": decision.market_threshold,
        "calibration_adjustment": decision.calibration_adjustment,
        "raw_unclamped_threshold": decision.raw_unclamped_threshold,
        "smoothed_threshold": decision.smoothed_threshold,
        "rate_limited_threshold": decision.rate_limited_threshold,
        # NULL, never 0.0, when nothing was evaluated.
        "final_threshold": decision.final_threshold,
        "min_threshold": decision.min_threshold,
        "max_threshold": decision.max_threshold,
        "previous_threshold": decision.previous_threshold,
        "smoothing_applied": int(bool(decision.smoothing_applied)),
        "rate_limit_applied": int(bool(decision.rate_limit_applied)),
        "clamp_applied": int(bool(decision.clamp_applied)),
        "passed": None if decision.passed is None else int(bool(decision.passed)),
        "reason": decision.reason,
        "detail": decision.detail,
        "reconciles": int(bool(decision.reconcile())),
    }

    placeholders = ", ".join("?" for _ in _DECISION_COLUMNS)
    columns = ", ".join(_DECISION_COLUMNS)
    with db.connect() as conn:
        conn.execute(
            f"INSERT OR REPLACE INTO threshold_decisions ({columns}) VALUES ({placeholders})",
            tuple(values[c] for c in _DECISION_COLUMNS),
        )
        _write_experts(
            conn,
            decision.expert_evidence,
            threshold_decision_id=decision.threshold_decision_id,
            decision_id=decision_id,
            bot_instance_id=decision.bot_instance_id,
            symbol=decision.symbol,
            timeframe=decision.timeframe,
            closed_candle_time=decision.closed_candle_time,
            recorded_at=decision.decided_at,
        )
    return decision.threshold_decision_id


def _write_experts(
    conn: Any,
    experts: Sequence[ExpertEvidence],
    *,
    threshold_decision_id: str,
    decision_id: str | None,
    bot_instance_id: str,
    symbol: str,
    timeframe: str | None,
    closed_candle_time: int | None,
    recorded_at: str,
) -> None:
    if not experts:
        return
    # A re-record of the same threshold decision replaces its expert rows rather
    # than appending a second set.
    conn.execute(
        "DELETE FROM expert_evaluations WHERE threshold_decision_id = ?",
        (threshold_decision_id,),
    )
    conn.executemany(
        """
        INSERT INTO expert_evaluations (
            threshold_decision_id, decision_id, bot_instance_id, symbol, timeframe,
            closed_candle_time, recorded_at, strategy, eligible, executed, signal,
            confidence, raw_score, weight, weighted_contribution, reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                threshold_decision_id,
                decision_id,
                bot_instance_id,
                symbol,
                timeframe,
                closed_candle_time,
                recorded_at,
                e.strategy,
                int(bool(e.eligible)),
                int(bool(e.executed)),
                e.signal,
                e.confidence,
                e.raw_score,
                e.weight,
                e.weighted_contribution,
                e.reason,
            )
            for e in experts
        ],
    )


def load_threshold_decisions(
    db: Any,
    *,
    bot_instance_id: str,
    symbol: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Read recent threshold decisions as plain dicts, newest first."""
    query = [
        "SELECT " + ", ".join(_DECISION_COLUMNS) + " FROM threshold_decisions",
        "WHERE bot_instance_id = ?",
    ]
    params: list[Any] = [str(bot_instance_id)]
    if symbol:
        query.append("AND symbol = ?")
        params.append(str(symbol).upper())
    query.append("ORDER BY decided_at DESC LIMIT ?")
    params.append(int(limit))

    with db.connect() as conn:
        rows = conn.execute(" ".join(query), tuple(params)).fetchall()
    return [dict(zip(_DECISION_COLUMNS, row)) for row in rows]


def load_expert_evaluations(
    db: Any, *, threshold_decision_id: str
) -> list[dict[str, Any]]:
    columns = (
        "strategy",
        "eligible",
        "executed",
        "signal",
        "confidence",
        "raw_score",
        "weight",
        "weighted_contribution",
        "reason",
    )
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT " + ", ".join(columns) + " FROM expert_evaluations "
            "WHERE threshold_decision_id = ? ORDER BY strategy",
            (str(threshold_decision_id),),
        ).fetchall()
    return [dict(zip(columns, row)) for row in rows]


def safe_record(
    db: Any,
    decision: AdaptiveThresholdDecision,
    *,
    decision_id: str | None = None,
    provenance: str = "PAPER_FORWARD",
) -> str | None:
    """Record, but never raise into the trading path.

    Losing a threshold-evidence row is a reporting gap. Raising here would stop
    the runtime from evaluating a candle, which is worse.
    """
    try:
        return record_threshold_decision(
            db, decision, decision_id=decision_id, provenance=provenance
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning(
            "[THRESHOLD_EVIDENCE] failed to record %s: %s",
            decision.threshold_decision_id,
            exc,
        )
        return None


def decisions_from_rows(rows: Iterable[dict[str, Any]]) -> list[AdaptiveThresholdDecision]:
    """Rebuild decision objects from stored rows, for diagnostics."""
    out: list[AdaptiveThresholdDecision] = []
    fields = set(AdaptiveThresholdDecision.__dataclass_fields__)  # type: ignore[attr-defined]
    for row in rows:
        payload = {k: v for k, v in row.items() if k in fields}
        for flag in ("smoothing_applied", "rate_limit_applied", "clamp_applied"):
            if flag in payload:
                payload[flag] = bool(payload[flag])
        if payload.get("passed") is not None:
            payload["passed"] = bool(payload["passed"])
        try:
            out.append(AdaptiveThresholdDecision(**payload))
        except (TypeError, ValueError):
            continue
    return out
