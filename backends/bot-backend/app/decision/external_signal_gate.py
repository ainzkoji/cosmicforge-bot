"""External signals enter the same entry-quality path as internal ones.

The live audit of 2026-09-10 found that a queued TradingView candidate reached
the executor through the event filter, the PolicyEngine (whose confidence gate
was deleted) and the execution filter -- and never through
AdaptiveEntryThresholdEngine. Its confidence was capped at 0.75 and then
compared with nothing. There was one final threshold authority for internal
opportunities and none at all for external ones.

This module closes that path without creating a second threshold calculator.
An external candidate is normalised into a TradingOpportunity and goes through
exactly the canonical chain::

    external signal
      -> TradingOpportunity                (this module)
      -> AdaptiveEntryThresholdEngine      (the one threshold authority)
      -> AdaptiveThresholdDecision         (persisted before anything executes)
      -> TradingDecisionEngine             (the one comparison)
      -> risk -> execution feasibility -> executor   (the caller)

Two deliberate choices, both conservative:

* **No expert evidence.** An external source is one opinion, not an ensemble.
  Recording it as a unanimous expert would feed the agreement term and lower
  the bar on the source's own say-so, so agreement is recorded as unavailable
  and contributes no adjustment.
* **Separate adaptive state.** The threshold state is partitioned by
  ``strategy_version``; external candidates use their own
  (``external_<source>/1``), so their confidences never enter the ensemble's
  distribution calibration.

A candidate that cannot satisfy the TradingOpportunity contract -- too little
closed market data to classify the regime, an invalid side or confidence -- is
rejected explicitly. So is one whose threshold decision cannot be persisted: an
external entry may never execute without exactly one linked threshold
decision on record.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from app.decision.reasons import QualityReason

logger = logging.getLogger(__name__)

#: The regime classifier needs this many closed candles for a stable read.
MIN_REGIME_CANDLES = 100

REASON_CONTRACT = QualityReason.OPPORTUNITY_CONTRACT_UNSATISFIED
REASON_EVIDENCE_UNAVAILABLE = "THRESHOLD_EVIDENCE_UNAVAILABLE"


@dataclass(frozen=True)
class ExternalGateResult:
    """The verdict for one external candidate, and the evidence behind it."""

    passed: bool
    reason: str
    detail: str = ""
    threshold_decision_id: str | None = None
    threshold_status: str | None = None
    final_threshold: float | None = None
    confidence: float | None = None
    opportunity_id: str | None = None
    persisted: bool = False
    threshold_decision: Any = None
    entry_quality: Any = None

    def observability(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "reason": self.reason,
            "detail": self.detail,
            "threshold_decision_id": self.threshold_decision_id,
            "threshold_status": self.threshold_status,
            "final_threshold": self.final_threshold,
            "confidence": self.confidence,
            "opportunity_id": self.opportunity_id,
            "persisted": self.persisted,
            "threshold_authority": "AdaptiveEntryThresholdEngine",
        }


def _reject(reason: str, detail: str, **extra: Any) -> ExternalGateResult:
    return ExternalGateResult(passed=False, reason=reason, detail=detail, **extra)


def _default_engine() -> Any:
    from app.threshold.engine import AdaptiveEntryThresholdEngine
    from app.threshold.runtime import get_performance_calibrator, get_threshold_state_store

    return AdaptiveEntryThresholdEngine(
        state_store=get_threshold_state_store(),
        performance_calibrator=get_performance_calibrator(),
    )


def _default_persist(db: Any, decision: Any, *, provenance: str) -> str:
    from app.threshold.persistence import record_threshold_decision

    return record_threshold_decision(db, decision, provenance=provenance)


def _persisted_row(db: Any, threshold_decision_id: str) -> tuple | None:
    with db.connect() as conn:
        return conn.execute(
            "SELECT threshold_decision_id, passed FROM threshold_decisions "
            "WHERE threshold_decision_id = ?",
            (threshold_decision_id,),
        ).fetchone()


def evaluate_external_candidate(
    *,
    db: Any,
    symbol: str,
    side: str,
    confidence: Any,
    klines: Sequence[Any],
    timeframe: str,
    source: str,
    bot_instance_id: str,
    run_id: str | None = None,
    cycle_id: str | None = None,
    market_type: str | None = None,
    venue: str | None = None,
    provenance: str = "PAPER_FORWARD",
    engine: Any = None,
    policy_resolver: Callable[..., Any] | None = None,
    persist: Callable[..., str] | None = None,
    regime_classifier_factory: Callable[[], Any] | None = None,
) -> ExternalGateResult:
    """Put one external candidate through the canonical entry-quality chain."""
    from app.decision.decision_engine import TradingDecisionEngine
    from app.decision.opportunity import build_opportunity
    from app.runner.market_snapshot import MarketSnapshot
    from app.strategy.master_ensemble import MasterEnsembleStrategy
    from app.strategy.regime import RegimeClassifier
    from app.threshold.contracts import AdaptiveThresholdInput, HTFContext, RegimeContext
    from app.threshold.runtime import get_threshold_policy

    # ── 1. The TradingOpportunity contract ──────────────────────────────────
    side = str(side or "").upper()
    if side not in {"BUY", "SELL"}:
        return _reject(REASON_CONTRACT, f"side {side!r} is not BUY or SELL")
    try:
        conf = float(confidence)
    except (TypeError, ValueError):
        return _reject(REASON_CONTRACT, f"confidence {confidence!r} is not a number")
    if not 0.0 < conf <= 1.0:
        return _reject(REASON_CONTRACT, f"confidence {conf} is outside (0, 1]")

    try:
        snapshot = MarketSnapshot.build(
            symbol=symbol, timeframe=timeframe, candles=klines,
            source=f"external_{str(source).lower()}",
        )
    except (ValueError, TypeError, IndexError) as exc:
        return _reject(REASON_CONTRACT, f"no usable closed candles: {exc}", confidence=conf)
    closed = list(snapshot.candles)
    if len(closed) < MIN_REGIME_CANDLES:
        return _reject(
            REASON_CONTRACT,
            f"{len(closed)} closed candles < {MIN_REGIME_CANDLES} needed to classify the regime",
            confidence=conf,
        )

    try:
        highs = [float(k[2]) for k in closed]
        lows = [float(k[3]) for k in closed]
        closes = [float(k[4]) for k in closed]
        classifier = (regime_classifier_factory or RegimeClassifier)()
        regime_result = classifier.classify_stable(highs, lows, closes)
        regime = regime_result.regime
    except Exception as exc:
        return _reject(REASON_CONTRACT, f"regime classification failed: {exc}", confidence=conf)

    strategy_name = f"external_{str(source).lower()}"
    opportunity = build_opportunity(
        symbol=symbol,
        timeframe=timeframe,
        market_snapshot_id=snapshot.market_snapshot_id,
        side=side,
        raw_confidence=conf,
        consensus=conf,
        buy_score=conf if side == "BUY" else 0.0,
        sell_score=conf if side == "SELL" else 0.0,
        votes=[(strategy_name, side, conf)],
        regime=regime.value,
        regime_confidence=float(regime_result.regime_confidence),
        active_strategies=(strategy_name,),
        atr_pct=float(getattr(regime_result, "atr_percent", 0.0) or 0.0),
        closed_candle_time=snapshot.latest_closed_candle_time,
        bot_instance_id=bot_instance_id,
    )

    # ── 2. The one threshold authority ──────────────────────────────────────
    policy = (policy_resolver or get_threshold_policy)(
        symbol=symbol, venue=venue, market_type=market_type,
    )
    request = AdaptiveThresholdInput(
        bot_instance_id=str(bot_instance_id),
        symbol=symbol,
        timeframe=timeframe,
        strategy_version=f"{strategy_name}/1",
        venue=str(venue or "unknown"),
        market_type=str(market_type or "UNKNOWN"),
        run_id=run_id,
        cycle_id=cycle_id,
        market_snapshot_id=snapshot.market_snapshot_id,
        opportunity_id=opportunity.opportunity_id,
        closed_candle_time=snapshot.latest_closed_candle_time,
        side=side,
        opportunity_confidence=conf,
        buy_score=conf if side == "BUY" else 0.0,
        sell_score=conf if side == "SELL" else 0.0,
        consensus=conf,
        experts=(),
        regime=RegimeContext(
            regime=regime.value,
            regime_confidence=float(regime_result.regime_confidence),
        ),
        volatility=MasterEnsembleStrategy._volatility_context(regime_result, closed),
        htf=HTFContext(),
        market_quality=MasterEnsembleStrategy._market_quality_context(snapshot, closed),
    )
    threshold_decision = (engine or _default_engine()).evaluate(request, policy)

    # ── 3. The one comparison ───────────────────────────────────────────────
    entry_quality = TradingDecisionEngine().evaluate(
        opportunity, threshold_decision=threshold_decision,
    )

    evidence = dict(
        threshold_decision_id=threshold_decision.threshold_decision_id,
        threshold_status=threshold_decision.status,
        final_threshold=threshold_decision.final_threshold,
        confidence=conf,
        opportunity_id=opportunity.opportunity_id,
        threshold_decision=threshold_decision,
        entry_quality=entry_quality,
    )

    # ── 4. Evidence before execution, or no execution ───────────────────────
    try:
        (persist or _default_persist)(db, threshold_decision, provenance=provenance)
        row = _persisted_row(db, threshold_decision.threshold_decision_id)
    except Exception as exc:
        logger.error(
            "[EXTERNAL_SIGNAL] %s: threshold decision could not be persisted; "
            "candidate rejected: %s", symbol, exc,
        )
        return _reject(
            REASON_EVIDENCE_UNAVAILABLE,
            f"threshold decision not persisted: {type(exc).__name__}: {exc}",
            **evidence,
        )
    if row is None:
        return _reject(
            REASON_EVIDENCE_UNAVAILABLE,
            "threshold decision was not found after persistence",
            **evidence,
        )

    passed = bool(
        entry_quality.approved
        and threshold_decision.evaluated
        and threshold_decision.passed
    )
    return ExternalGateResult(
        passed=passed,
        reason=str(entry_quality.primary_reason),
        detail=str(getattr(threshold_decision, "detail", "") or ""),
        persisted=True,
        **evidence,
    )


__all__ = [
    "ExternalGateResult",
    "MIN_REGIME_CANDLES",
    "REASON_CONTRACT",
    "REASON_EVIDENCE_UNAVAILABLE",
    "evaluate_external_candidate",
]
