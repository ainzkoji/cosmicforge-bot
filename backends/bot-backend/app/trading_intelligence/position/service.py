"""PositionIntelligenceService -- Section 19 SHADOW / ADVISORY flow (19.19).

    PositionPathSnapshot -> current MarketState -> current Regime
    -> Section 17 remaining-cost estimate -> PositionForecast -> ExitDecision
    -> EVIDENCE ONLY

This service holds no reference to PositionManager, the executor, a broker
client or any reservation store: it CANNOT place, cancel or replace an
order, close a position, change quantity/leverage or move a stop. Its
output is evidence; routing an ExitDecision to the lifecycle authority is
Section 20's ``process_exit_decision`` and is disabled by default.

Legacy positions (no TradePlan lineage) are not eligible (19.20): the
service returns ``LEGACY_POSITION_NOT_ELIGIBLE`` and writes nothing. V2
positions keep their current management untouched.

Failure (19.16 / 21.21): a component exception becomes structured
CATI_COMPONENT_ERROR evidence and a NO_CHANGE_FALLBACK decision (existing
mechanical protection continues) -- never a silent swallow, never a V2
fallback (P9).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from app.trading_intelligence.contracts.position import (
    ExitAction, ExitDecision, ExitPolicy, PositionForecast, PositionPathSnapshot, PositionReasonCode as PR,
)
from app.trading_intelligence.contracts.trade_plan import TradePlan
from app.trading_intelligence.observability.logging import log_stage, record_stage_error
from app.trading_intelligence.observability.metrics import METRICS, reason_family
from app.trading_intelligence.position.exit_engine import decide_exit
from app.trading_intelligence.position.forecast import build_position_forecast
from app.trading_intelligence.position.path import PositionLifecycleView, build_position_path_snapshot
from app.trading_intelligence.position.thesis import ThesisContext, evaluate_thesis

logger = logging.getLogger(__name__)

ELIGIBLE = "EVALUATED"
LEGACY = "LEGACY_POSITION_NOT_ELIGIBLE"
LINEAGE_MISMATCH = "LINEAGE_MISMATCH"
COMPONENT_ERROR = "COMPONENT_ERROR"


@dataclass(frozen=True)
class PositionEvaluation:
    status: str
    path: Optional[PositionPathSnapshot] = None
    forecast: Optional[PositionForecast] = None
    decision: Optional[ExitDecision] = None
    reason_codes: tuple = ()


def _fallback_forecast_id(path: PositionPathSnapshot) -> str:
    return f"pfc_unavailable_{path.position_path_id[6:]}"


class PositionIntelligenceService:
    def __init__(self, *, library: Any = None, policy: Optional[ExitPolicy] = None, venue_policy: Any = None,
                 db: Any = None, evaluation_mode: str = "SHADOW") -> None:
        self.library = library
        self.policy = policy or ExitPolicy()
        self.venue_policy = venue_policy
        self.evaluation_mode = evaluation_mode
        self._db = db
        self._forecasts = self._decisions = None
        if db is not None:
            from app.trading_intelligence.evidence.stores import ExitDecisionStore, PositionForecastStore

            self._forecasts, self._decisions = PositionForecastStore(db), ExitDecisionStore(db)

    def evaluate(self, *, plan: Optional[TradePlan], position: PositionLifecycleView, candle_rows: Sequence[Any],
                 current_time: int, current_price: float, market_state: Any, regime: Any,
                 venue_observation: Any = None, event_context: Any = None, system_context: Any = None,
                 timeframe: Optional[str] = None, instrument_group: Optional[str] = None,
                 runtime_session_id: Optional[str] = None) -> PositionEvaluation:
        # 19.20: CATI lineage is mandatory -- a legacy V2 position is left untouched
        if plan is None or not position.trade_plan_id:
            METRICS.inc("cati_position_legacy_skipped_total")
            return PositionEvaluation(LEGACY, reason_codes=(PR.LEGACY_POSITION_NO_TRADE_PLAN.value,))
        if position.trade_plan_id != plan.trade_plan_id or position.side != plan.side:
            return PositionEvaluation(LINEAGE_MISMATCH, reason_codes=(PR.LINEAGE_MISMATCH.value,))
        ids = dict(cycle_id=plan.cycle_id, user_id=plan.user_id, broker_account_id=plan.broker_account_id,
                   bot_instance_id=plan.bot_instance_id)
        t0 = time.perf_counter()
        try:
            path = build_position_path_snapshot(plan=plan, position=position, candle_rows=candle_rows,
                                                current_time=current_time, current_price=current_price,
                                                timeframe=timeframe or getattr(market_state, "timeframe", None))
        except Exception as exc:
            record_stage_error("position.path", "POSITION_FORECAST", exc, db=self._db, **ids)
            return PositionEvaluation(COMPONENT_ERROR, reason_codes=(PR.COMPONENT_ERROR.value,))

        forecast: Optional[PositionForecast] = None
        try:
            thesis = evaluate_thesis(ThesisContext(plan, path, market_state, regime, event_context, system_context,
                                                   self.policy))
            forecast = build_position_forecast(
                plan=plan, path=path, market_state=market_state, regime=regime, library=self.library,
                venue_observation=venue_observation, thesis_results=thesis, policy=self.policy,
                venue_policy=self.venue_policy, instrument_group=instrument_group,
                evaluation_mode=self.evaluation_mode)
            METRICS.observe("cati_stage_latency_ms", (time.perf_counter() - t0) * 1000.0, stage="POSITION_FORECAST")
            t1 = time.perf_counter()
            decision = decide_exit(plan=plan, path=path, forecast=forecast, market_state=market_state,
                                   policy=self.policy, evaluation_mode=self.evaluation_mode)
            METRICS.observe("cati_stage_latency_ms", (time.perf_counter() - t1) * 1000.0, stage="EXIT_DECISION")
        except Exception as exc:
            record_stage_error("position.intelligence", "EXIT_DECISION", exc, db=self._db, **ids)
            decision = ExitDecision.build(
                position_forecast_id=forecast.position_forecast_id if forecast else _fallback_forecast_id(path),
                position_id=path.position_id, trade_plan_id=plan.trade_plan_id, position_path_id=path.position_path_id,
                user_id=path.user_id, broker_account_id=path.broker_account_id, bot_instance_id=path.bot_instance_id,
                action=ExitAction.NO_CHANGE_FALLBACK.value, requested_fraction=None, suggested_protection_price=None,
                existing_protection_price=path.current_stop_price, side=plan.side, decision_time=path.current_time,
                conservative_remaining_edge_R=0.0, thesis_status="UNKNOWN",
                reason_codes=(PR.COMPONENT_ERROR.value, PR.NO_TRUSTWORTHY_INTENT.value),
                policy_version=self.policy.schema_version, policy_hash=self.policy.policy_hash,
                engine_version="fallback", evaluation_mode=self.evaluation_mode)

        self._observe(plan, path, forecast, decision)
        self._persist(path, forecast, decision)
        log_stage(component="position.intelligence", status=decision.action,
                  duration_ms=(time.perf_counter() - t0) * 1000.0, reason_codes=decision.reason_codes,
                  runtime_session_id=runtime_session_id, bot_run_id=plan.run_id, **ids,
                  extra={"trade_plan_id": plan.trade_plan_id, "position_id": path.position_id,
                         "exit_decision_id": decision.exit_decision_id, "advisory_only": True})
        return PositionEvaluation(ELIGIBLE, path, forecast, decision, decision.reason_codes)

    # -- evidence ---------------------------------------------------------------------------------
    def _persist(self, path, forecast, decision) -> None:
        if self._forecasts is None:
            return
        try:
            if forecast is not None:
                self._forecasts.append(forecast, path)
            self._decisions.append(decision)
        except Exception as exc:
            record_stage_error("position.evidence", "EXIT_DECISION", exc, db=None,
                               broker_account_id=path.broker_account_id, bot_instance_id=path.bot_instance_id)

    @staticmethod
    def _observe(plan, path, forecast, decision) -> None:
        try:
            labels = dict(asset_class=plan.instrument_key.asset_class, setup_family=plan.setup_family)
            METRICS.inc("cati_position_forecasts_total", status=forecast.status if forecast else "UNAVAILABLE", **labels)
            METRICS.inc("cati_exit_decisions_total", action=decision.action, **labels)
            METRICS.observe("cati_exit_mfe_r_at_decision", path.mfe_R, action=decision.action)
            METRICS.observe("cati_exit_mae_r_at_decision", path.mae_R, action=decision.action)
            METRICS.observe("cati_exit_remaining_edge_r", decision.conservative_remaining_edge_R, action=decision.action)
            METRICS.observe("cati_position_time_in_trade_s", path.elapsed_seconds, action=decision.action)
            if decision.thesis_status == "INVALIDATED":
                METRICS.inc("cati_thesis_invalidations_total", **labels)
            for code in decision.reason_codes[:3]:
                METRICS.inc("cati_exit_reasons_total", action=decision.action, reason_family=reason_family(code))
        except Exception:
            pass


def resolve_cati_plan(db: Any, broker_account_id: str, position_id: str) -> Optional[TradePlan]:
    """The TradePlan a position was opened from, via the append-only CATI
    execution evidence (plan -> risk decision -> attempt -> position). None
    for a legacy/V2 position: no lineage, no CATI position intelligence."""
    from app.trading_intelligence.evidence.stores import ExecutionAttemptStore
    from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore

    rows = ExecutionAttemptStore(db).by_position(broker_account_id, position_id)
    if not rows:
        return None
    return TradePlanEvidenceStore(db).load_plan(broker_account_id, rows[-1]["trade_plan_id"])


__all__ = ["resolve_cati_plan", "PositionIntelligenceService", "PositionEvaluation", "ELIGIBLE", "LEGACY", "LINEAGE_MISMATCH",
           "COMPONENT_ERROR"]
