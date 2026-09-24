"""ShadowAccountPortfolioService (Sections 16.2, 16.23-16.30).

Turns a completed, ranked bot cycle into a PortfolioSelectionDecision and --
only when something was selected -- persists a SHADOW account reservation
under the account-scoped transaction. It does NOT: reserve production slots,
reserve margin/capital, size, call hard risk, or submit an order. Existing
slot/margin authorities remain the final hard authority downstream.

* available_slots comes from the EXISTING slot authority's read-only view
  (``position_slots.occupied_slots``) and the bot's effective
  ``max_open_positions`` -- never a hardcoded global.
* The reservation is written only after the store REVALIDATES, inside its
  account-scoped transaction, the exact exposure / reservation / capacity
  state this selection was computed on (EXPOSURE_CHANGED /
  ACCOUNT_RESERVATION_CONFLICT / CAPACITY_CHANGED on any drift).
* On reservation conflict the service reports the reason and does NOT
  substitute a lower-ranked candidate or mutate the selection.
* Only APPROVE_FOR_RANKING opportunities may be selected/reserved: a WATCH or
  REJECT opportunity reaching this service is refused (NOT_APPROVED_FOR_RANKING).
* After a downstream hard-risk rejection the caller releases the
  reservation; selection is only ever re-run deliberately.
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from app.execution.position_slots import occupied_slots
from app.trading_intelligence.contracts.portfolio import AccountPortfolioReservation
from app.trading_intelligence.contracts.portfolio_intel import (
    ACCOUNT_CORRELATION_CONFLICT, ACCOUNT_RESERVATION_CONFLICT, COMMON_FACTOR_CONCENTRATION, DUPLICATE_EXPOSURE,
    NOT_APPROVED_FOR_RANKING, PORTFOLIO_DATA_QUALITY_FAULT, RESERVATION_SCHEMA_MISSING, PortfolioMarketContext,
    PortfolioPolicy, PortfolioSelectionDecision, RejectedCandidate, ScoreBreakdown, SOLVER_NONE,
)
from app.trading_intelligence.contracts.ranking import EvaluatedOpportunity, RankedOpportunity
from app.trading_intelligence.contracts.veto import VetoDecision, VetoStage
from app.trading_intelligence.portfolio.exposure_builder import (
    PortfolioDataError, account_state_fingerprint, build_account_exposure_snapshot,
)
from app.trading_intelligence.portfolio.reservation_store import (
    CATIReservationStore, ReservationSchemaMissing, available_slots_from,
)
from app.trading_intelligence.portfolio.selector import select_portfolio
from app.trading_intelligence.veto.engine import evaluate_veto

_PORTFOLIO_VETO_REASONS = (DUPLICATE_EXPOSURE, ACCOUNT_RESERVATION_CONFLICT, ACCOUNT_CORRELATION_CONFLICT,
                           COMMON_FACTOR_CONCENTRATION)


@dataclass(frozen=True)
class PortfolioOutcome:
    decision: PortfolioSelectionDecision
    reservation: Optional[AccountPortfolioReservation] = None
    portfolio_vetoes: Tuple[VetoDecision, ...] = ()


class ShadowAccountPortfolioService:
    def __init__(self, db: Any, *, policy: Optional[PortfolioPolicy] = None,
                 store: Optional[CATIReservationStore] = None) -> None:
        self._db = db
        self._policy = policy or PortfolioPolicy()
        self._schema_error: Optional[str] = None
        try:
            self._store = store or CATIReservationStore(db)
        except ReservationSchemaMissing as exc:  # fail closed, reported per decision
            self._store, self._schema_error = None, str(exc)

    @property
    def store(self) -> Optional[CATIReservationStore]:
        return self._store

    # -- capacity (read-only view of the EXISTING slot authority) ----------------------
    def available_slots(self, bot_instance_id: str, max_open_positions: int, now_ms: Optional[int] = None,
                        broker_account_id: Optional[str] = None) -> int:
        occ = occupied_slots(self._db, bot_instance_id)
        own = []
        if self._store is not None and broker_account_id is not None and now_ms is not None:
            own = [r for r in self._store.active_reservations(broker_account_id, now_ms) if r.bot_instance_id == bot_instance_id]
        return available_slots_from(occ, own, max_open_positions)

    def select_and_reserve(
        self,
        *,
        ranked: Sequence[RankedOpportunity],
        broker_account_id: str,
        bot_instance_id: str,
        cycle_id: str,
        max_open_positions: int,
        context: PortfolioMarketContext,
        now_ms: int,
        evaluated_by_candidate_id: Optional[Mapping[str, EvaluatedOpportunity]] = None,
    ) -> PortfolioOutcome:
        """Section 21: stage latency + bounded portfolio metrics around the
        unchanged selection/reservation logic."""
        import time as _time

        t0 = _time.perf_counter()
        outcome = self._select_and_reserve(
            ranked=ranked, broker_account_id=broker_account_id, bot_instance_id=bot_instance_id, cycle_id=cycle_id,
            max_open_positions=max_open_positions, context=context, now_ms=now_ms,
            evaluated_by_candidate_id=evaluated_by_candidate_id)
        try:
            from app.trading_intelligence.observability.emitters import observe_portfolio
            from app.trading_intelligence.observability.metrics import METRICS

            METRICS.observe("cati_stage_latency_ms", (_time.perf_counter() - t0) * 1000.0, stage="PORTFOLIO")
            observe_portfolio(outcome.decision)
        except Exception:
            pass
        return outcome

    def _select_and_reserve(
        self,
        *,
        ranked: Sequence[RankedOpportunity],
        broker_account_id: str,
        bot_instance_id: str,
        cycle_id: str,
        max_open_positions: int,
        context: PortfolioMarketContext,
        now_ms: int,
        evaluated_by_candidate_id: Optional[Mapping[str, EvaluatedOpportunity]] = None,
    ) -> PortfolioOutcome:
        p = self._policy
        if self._store is None:
            return PortfolioOutcome(self._fault_decision(ranked, broker_account_id, bot_instance_id, cycle_id, context,
                                                         now_ms, self._schema_error or "", RESERVATION_SCHEMA_MISSING))
        # WATCH / REJECT can never be selected or reserved.
        not_approved: List[RejectedCandidate] = []
        if evaluated_by_candidate_id:
            keep = []
            for r in ranked:
                ev = evaluated_by_candidate_id.get(r.setup_candidate_id)
                if ev is not None and not ev.approved:
                    not_approved.append(RejectedCandidate(r.ranked_opportunity_id, r.setup_candidate_id,
                                                          NOT_APPROVED_FOR_RANKING, ev.veto.outcome))
                else:
                    keep.append(r)
            ranked = keep
        self._store.expire_stale(broker_account_id, now_ms)
        try:
            exposure = build_account_exposure_snapshot(
                self._db, broker_account_id, now_ms, reservation_store=self._store, now_ms=now_ms)
        except PortfolioDataError as exc:
            return PortfolioOutcome(self._fault_decision(ranked, broker_account_id, bot_instance_id, cycle_id,
                                                         context, now_ms, str(exc)))
        slots = self.available_slots(bot_instance_id, max_open_positions, now_ms, broker_account_id)
        decision = select_portfolio(
            ranked=ranked, exposure=exposure, context=context, policy=p, bot_instance_id=bot_instance_id,
            cycle_id=cycle_id, available_slots=slots, decision_time=now_ms,
        )
        reservation: Optional[AccountPortfolioReservation] = None
        if decision.selected_opportunity_ids:
            by_id = {r.ranked_opportunity_id: r for r in ranked}
            selected = [by_id[i] for i in decision.selected_opportunity_ids]
            outcome = self._store.reserve(
                broker_account_id=broker_account_id, bot_instance_id=bot_instance_id, cycle_id=cycle_id,
                selected=[(r.setup_candidate_id, r.instrument_key.canonical_symbol, r.instrument_key.venue,
                           r.instrument_key.venue_symbol, r.side) for r in selected],
                now_ms=now_ms, ttl_seconds=p.reservation_ttl_seconds, allow_hedge_duplicates=p.allow_hedge_mode_duplicates,
                # the exact assumptions this selection was computed on -- revalidated in-transaction
                expected_exposure_fingerprint=account_state_fingerprint(
                    broker_account_id, exposure.open_exposures + exposure.pending_exposures),
                expected_reservation_fingerprint=account_state_fingerprint(
                    broker_account_id, exposure.reservation_exposures),
                expected_available_slots=slots, max_open_positions=max_open_positions,
                policy_version=p.schema_version, policy_hash=p.policy_hash,
            )
            if outcome.reserved:
                reservation = outcome.reservation
                decision = dataclasses.replace(decision, reservation_id=reservation.reservation_id, reservation_status="RESERVED")
            else:
                # Lost the race (or state changed since the snapshot): report it, do NOT substitute.
                decision = dataclasses.replace(
                    decision, reservation_status="CONFLICT",
                    reason_codes=tuple(dict.fromkeys(decision.reason_codes + (outcome.conflict_reason or ACCOUNT_RESERVATION_CONFLICT,))))
        if not_approved:
            decision = dataclasses.replace(
                decision, rejected_candidates=tuple(not_approved) + decision.rejected_candidates,
                reason_codes=tuple(dict.fromkeys(decision.reason_codes + (NOT_APPROVED_FOR_RANKING,))))
        vetoes = self._portfolio_vetoes(decision, evaluated_by_candidate_id, broker_account_id, bot_instance_id, cycle_id)
        return PortfolioOutcome(decision, reservation, vetoes)

    def release(self, reservation_id: str, now_ms: int) -> bool:
        """Hard risk rejected the selected candidate: free the reservation.
        No automatic rank-#2 substitution exists or is implied."""
        return self._store is not None and self._store.release(reservation_id, now_ms)

    def expire(self, broker_account_id: str, now_ms: int) -> int:
        return 0 if self._store is None else self._store.expire_stale(broker_account_id, now_ms)

    def consume(self, reservation_id: str, now_ms: int) -> bool:
        return self._store is not None and self._store.consume(reservation_id, now_ms)

    # -- helpers -----------------------------------------------------------------------
    def _portfolio_vetoes(self, decision, evaluated, account, bot, cycle) -> Tuple[VetoDecision, ...]:
        if not evaluated:
            return ()
        out: List[VetoDecision] = []
        for rej in decision.rejected_candidates:
            if rej.reason_code not in _PORTFOLIO_VETO_REASONS:
                continue
            ev = evaluated.get(rej.setup_candidate_id)
            if ev is None:
                continue
            out.append(evaluate_veto(
                opportunity=ev.opportunity, candidate=ev.candidate, market_state=ev.market_state,
                regime_distribution=ev.regime, forecast=ev.forecast, cost_estimate=ev.cost_estimate,
                stage=VetoStage.PORTFOLIO_STAGE.value, portfolio_findings=(rej.reason_code,),
                broker_account_id=account, bot_instance_id=bot, cycle_id=cycle,
            ))
        return tuple(out)

    def _fault_decision(self, ranked, account, bot, cycle, context, now_ms, detail,
                        reason: str = PORTFOLIO_DATA_QUALITY_FAULT) -> PortfolioSelectionDecision:
        ids = tuple(r.ranked_opportunity_id for r in ranked)
        return PortfolioSelectionDecision(
            portfolio_selection_id=PortfolioSelectionDecision.build_id(
                broker_account_id=account, bot_instance_id=bot, cycle_id=cycle, ranked_ids=ids,
                exposure_snapshot_hash="fault", context_hash=context.context_hash, policy_hash=self._policy.policy_hash),
            broker_account_id=account, bot_instance_id=bot, cycle_id=cycle, account_exposure_snapshot_id="none",
            portfolio_market_context_id=context.portfolio_market_context_id, ranked_opportunity_ids=ids,
            selected_opportunity_ids=(), rejected_candidates=(), available_slots=0, portfolio_score=0.0,
            score_breakdown=ScoreBreakdown(0.0, 0.0, 0.0, 0.0, 0.0), solver=SOLVER_NONE,
            portfolio_policy_version=self._policy.schema_version, portfolio_policy_hash=self._policy.policy_hash,
            reservation_id=None, reservation_status="NOT_REQUIRED", reason_codes=(reason,),
            decision_time=now_ms,
        )


__all__ = ["PortfolioOutcome", "ShadowAccountPortfolioService"]
