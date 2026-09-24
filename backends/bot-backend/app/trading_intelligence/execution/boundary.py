"""The CATI execution boundary (Section 20) -- EXISTS, is fully testable and
demo-capable, and is DISABLED unless ``CATI_ACTIVE_EXECUTION_ENABLED`` is
explicitly set. The runner never calls it (no promotion; Section 25 owns
that).

    TradePlan
      -> [flag / environment / adapter-support gates]
      -> CATI idempotency (one attempt identity per plan id + hash + account)
      -> TradingOrchestrator.process_trade_plan   (EXISTING hard risk / sizing)
      -> ExecutionAdapter.submit_entry            (EXISTING executor: capital,
                                                   slots, margin, idempotency,
                                                   submit-unknown, fill
                                                   resolution, protection)
      -> portfolio-reservation lifecycle + append-only evidence

Portfolio reservation lifecycle (20.15):
    created with the plan                     RESERVED
    prevalidation / hard risk rejects          RELEASED
    executor slot / margin / sizing rejects    RELEASED
    execution fails before any position        RELEASED
    authoritative (partial) fill               CONSUMED
    submit-unknown / same-intent reuse         RESOLUTION_PENDING -- never expires,
                                               never re-submitted, still counted in
                                               account exposure/capacity -- until
                                               ``reconcile_submit_unknown`` proves
                                               from BROKER truth whether an entry
                                               exists (-> CONSUMED | RELEASED).
                                               Past the escalation deadline it is
                                               flagged SUBMIT_OUTCOME_UNRESOLVED
                                               (a system fault), still owned.
Transitions are the reservation store's compare-and-set (BEGIN IMMEDIATE),
so a repeat is a no-op. The next-ranked candidate is NEVER substituted.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field, replace
from typing import Any, Dict, Mapping, Optional, Tuple

from app.trading_intelligence.contracts.execution import (
    POSITION_EXISTS, ExecutionAttempt, ExecutionAttemptStatus as X, RiskRejectionFamily as F, RiskStage,
)
from app.trading_intelligence.contracts.position import ExitAction, ExitDecision
from app.trading_intelligence.contracts.trade_plan import TradePlan
from app.trading_intelligence.evidence.stores import ExecutionAttemptStore, RiskDecisionStore
from app.trading_intelligence.execution.adapter import (
    EntryRequest, EntryResult, ExecutionNotSupported, ProtectionWideningRefused, adapter_supports,
)
from app.trading_intelligence.execution.config import CATIExecutionConfig
from app.trading_intelligence.execution.risk_evidence import allocation_basis, build_risk_decision
from app.trading_intelligence.observability.logging import log_stage, record_stage_error
from app.trading_intelligence.observability.metrics import METRICS
from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore

logger = logging.getLogger(__name__)


class BoundaryStatus:
    DISABLED = "CATI_ACTIVE_EXECUTION_DISABLED"
    ENVIRONMENT_NOT_ALLOWED = "ENVIRONMENT_NOT_ALLOWED"
    ADAPTER_UNVALIDATED = "EXECUTION_ADAPTER_UNVALIDATED"
    VENUE_MISMATCH = "EXECUTION_VENUE_MISMATCH"
    DUPLICATE_PLAN = "DUPLICATE_PLAN_IGNORED"
    RISK_REJECTED = "RISK_REJECTED"
    EXECUTED = "EXECUTED"
    EXECUTION_REJECTED = "EXECUTION_REJECTED"
    SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN_PENDING_RECONCILIATION"
    RECONCILED = "RECONCILED"
    STILL_UNKNOWN = "STILL_UNKNOWN"
    EXIT_ROUTING_DISABLED = "EXIT_ROUTING_DISABLED"
    NO_BROKER_ACTION = "NO_BROKER_ACTION"
    EXIT_ROUTED = "EXIT_ROUTED"
    EXIT_REFUSED = "EXIT_REFUSED"


@dataclass(frozen=True)
class AccountState:
    current_equity: float
    margin_used: float
    margin_available: float
    open_positions: int


@dataclass(frozen=True)
class BoundaryResult:
    status: str
    trade_plan_id: str
    reason_codes: Tuple[str, ...] = ()
    risk_decision: Any = None
    attempt: Optional[ExecutionAttempt] = None
    reservation_status: Optional[str] = None
    detail: Mapping[str, Any] = field(default_factory=dict)


#: How long an unresolved submit may stay pending before it is escalated as a
#: SUBMIT_OUTCOME_UNRESOLVED system fault. Escalation never releases ownership.
DEFAULT_RESOLUTION_ESCALATION_MS = 15 * 60_000

SUBMIT_OUTCOME_UNRESOLVED = "SUBMIT_OUTCOME_UNRESOLVED"


def _now() -> int:
    return int(time.time() * 1000)


class CATIExecutionBoundary:
    def __init__(self, *, orchestrator: Any, adapter: Any, db: Any, config: Optional[CATIExecutionConfig] = None,
                 position_manager: Any = None, clock=None,
                 resolution_escalation_ms: int = DEFAULT_RESOLUTION_ESCALATION_MS) -> None:
        self.orchestrator = orchestrator
        self.adapter = adapter
        self.config = config if config is not None else CATIExecutionConfig.from_env()
        self.position_manager = position_manager
        self._clock = clock or _now
        self.resolution_escalation_ms = int(resolution_escalation_ms)
        self.reservations = CATIReservationStore(db)
        self.risk_store = RiskDecisionStore(db)
        self.attempts = ExecutionAttemptStore(db)
        self._db = db

    # ---------------------------------------------------------------------------------------
    def process_trade_plan(self, plan: TradePlan, *, market_reference: Any, broker_health: Any,
                           venue_capabilities: Any, account: AccountState, klines: list = (),
                           atr: Optional[float] = None, runtime_session_id: Optional[str] = None,
                           now_ms: Optional[int] = None, **risk_kwargs: Any) -> BoundaryResult:
        now = int(now_ms if now_ms is not None else self._clock())
        ids = dict(cycle_id=plan.cycle_id, user_id=plan.user_id, broker_account_id=plan.broker_account_id,
                   bot_instance_id=plan.bot_instance_id)
        # 20.18 -- the path exists but is OFF unless explicitly enabled (no orchestrator call, no mutation)
        if not self.config.active_execution_enabled:
            METRICS.inc("cati_execution_boundary_total", status=BoundaryStatus.DISABLED)
            return BoundaryResult(BoundaryStatus.DISABLED, plan.trade_plan_id, (BoundaryStatus.DISABLED,))
        if plan.environment.upper() not in self.config.allowed_environments:
            return BoundaryResult(BoundaryStatus.ENVIRONMENT_NOT_ALLOWED, plan.trade_plan_id,
                                  (f"ENVIRONMENT_NOT_ALLOWED:{plan.environment}",))

        attempt_id = ExecutionAttempt.build_id(trade_plan_id=plan.trade_plan_id, trade_plan_hash=plan.trade_plan_hash,
                                               broker_account_id=plan.broker_account_id)
        # 20.9 -- repeated processing of the same plan never re-submits
        prior = self.attempts.latest(plan.broker_account_id, attempt_id)
        if prior is not None:
            METRICS.inc("cati_execution_boundary_total", status=BoundaryStatus.DUPLICATE_PLAN)
            return BoundaryResult(BoundaryStatus.DUPLICATE_PLAN, plan.trade_plan_id,
                                  ("DUPLICATE_TRADE_PLAN_EXECUTION", f"PRIOR_STATUS:{prior['status']}"),
                                  detail={"execution_attempt_id": attempt_id, "prior_status": prior["status"]})

        if str(getattr(self.adapter, "venue", "")).upper() != plan.venue.upper():
            return self._fail_before_risk(plan, now, BoundaryStatus.VENUE_MISMATCH, "EXECUTION_VENUE_MISMATCH")
        if not adapter_supports(self.adapter, plan.environment):
            return self._fail_before_risk(plan, now, BoundaryStatus.ADAPTER_UNVALIDATED,
                                          f"EXECUTION_SUPPORT:{getattr(self.adapter, 'execution_support_status', None)}")

        reservation = self.reservations.get(plan.portfolio_reservation_id)
        t0 = time.perf_counter()
        risk_result = self.orchestrator.process_trade_plan(
            plan, now_ms=now, market_reference=market_reference, broker_health=broker_health,
            reservation_state=reservation, venue_capabilities=venue_capabilities, klines=list(klines or ()),
            current_equity=account.current_equity, margin_used=account.margin_used,
            margin_available=account.margin_available, open_positions=account.open_positions,
            client=getattr(getattr(self.adapter, "executor", None), "client", None), atr=atr,
            runtime_session_id=runtime_session_id, **risk_kwargs)
        risk = risk_result["risk_decision"]
        self._append_risk(risk)
        METRICS.observe("cati_stage_latency_ms", (time.perf_counter() - t0) * 1000.0, stage="RISK")
        if not risk.approved:
            released = self.reservations.release(plan.portfolio_reservation_id, now)
            METRICS.inc("cati_hard_risk_rejections_total", reason_family=risk.rejection_family or F.OTHER.value,
                        stage=risk.stage)
            log_stage(component="boundary.risk", status="REJECTED", reason_codes=risk.reason_codes,
                      runtime_session_id=runtime_session_id, bot_run_id=plan.run_id, **ids,
                      extra={"trade_plan_id": plan.trade_plan_id, "risk_decision_id": risk.risk_decision_id})
            return BoundaryResult(BoundaryStatus.RISK_REJECTED, plan.trade_plan_id, risk.reason_codes, risk_decision=risk,
                                  reservation_status="RELEASED" if released else self._res_status(plan))
        METRICS.inc("cati_hard_risk_approvals_total", stage=risk.stage)

        tp = risk_result["trade_params"]
        req = EntryRequest(
            trade_plan_id=plan.trade_plan_id, trade_plan_hash=plan.trade_plan_hash, risk_decision_id=risk.risk_decision_id,
            venue_symbol=plan.instrument_key.venue_symbol, side=plan.side,
            notional=float(tp["quantity"]) * float(tp["entry_price"]), stop_price=float(tp["stop_loss"]),
            target_price=float(tp["take_profit"]) if tp.get("take_profit") else None, leverage=float(tp["leverage"]),
            requested_order_type=plan.execution_preferences.preferred_order_style,
            requested_price=float(market_reference.price),
            max_slippage_bps=float(plan.execution_preferences.maximum_slippage_bps),
            current_open_count=int(account.open_positions), current_equity=float(account.current_equity),
            cycle_id=plan.cycle_id, intent_identity=f"{plan.trade_plan_id}|{plan.trade_plan_hash}",
        )
        base = self._attempt(plan, attempt_id, risk.risk_decision_id, req, status=X.PENDING_SUBMIT.value, now=now)
        self.attempts.append(base, 0)  # evidence exists BEFORE the broker is touched

        t1 = time.perf_counter()
        try:
            entry = self.adapter.submit_entry(req)
        except ExecutionNotSupported as exc:
            entry = EntryResult(status=X.ERROR_PRE_SUBMIT.value, raw_status="EXECUTION_NOT_SUPPORTED",
                                rejection_family=F.OTHER.value, reason_codes=("EXECUTION_NOT_SUPPORTED",),
                                detail=str(exc)[:200])
        except Exception as exc:  # 20.10: never read an unknown outcome as failure -- reconcile instead
            record_stage_error("boundary.submit_entry", "EXECUTION", exc, db=self._db, **ids)
            entry = EntryResult(status=X.SUBMIT_UNKNOWN.value, raw_status=f"EXCEPTION:{type(exc).__name__}",
                                reason_codes=("SUBMIT_OUTCOME_UNKNOWN_EXCEPTION",))
        METRICS.observe("cati_stage_latency_ms", (time.perf_counter() - t1) * 1000.0, stage="EXECUTION")
        return self._settle(plan, base, req, entry, now, runtime_session_id)

    # ---------------------------------------------------------------------------------------
    def _settle(self, plan, base: ExecutionAttempt, req: EntryRequest, entry: EntryResult, now: int,
                runtime_session_id) -> BoundaryResult:
        status = entry.status
        if entry.raw_status == "ENTRY_INTENT_REUSED":
            # the executor holds an identical in-flight intent: CATI cannot know if it filled
            status = X.SUBMIT_UNKNOWN.value
        keep_reserved = status == X.SUBMIT_UNKNOWN.value
        position_id = None
        if status in POSITION_EXISTS:
            position_id = f"cati_{base.execution_attempt_id}"
        attempt = self._resolved(base, entry, status, position_id, now)
        self.attempts.append(attempt, 1)
        self._observe_execution(plan, req, entry)

        risk = None
        if status == X.REJECTED.value and entry.rejection_family in (F.SLOT.value, F.MARGIN.value, F.SIZING.value,
                                                                     F.INSTRUMENT.value, F.LEVERAGE.value):
            # the executor's own atomic slot / margin / capital gate rejected: evidence of THAT authority
            risk = build_risk_decision(
                plan, approved=False, stage=RiskStage.EXECUTOR_GATES.value, reason_codes=entry.reason_codes,
                decision_time=now, runtime_session_id=runtime_session_id, rejection_family=entry.rejection_family,
                allocation=allocation_basis(None, getattr(self.adapter, "executor", None)),
                source=f"{self.adapter.adapter_id}.submit_entry")
            self._append_risk(risk)
            METRICS.inc("cati_hard_risk_rejections_total", reason_family=entry.rejection_family, stage=risk.stage)

        if status in POSITION_EXISTS:
            self.reservations.consume(plan.portfolio_reservation_id, now)
            self._register_lifecycle(plan, req, entry, position_id)
            outcome = BoundaryStatus.EXECUTED
        elif keep_reserved:
            # ownership is UNKNOWN: hold it explicitly (never expires, never re-submits)
            self.reservations.mark_resolution_pending(
                plan.portfolio_reservation_id, now, trade_plan_id=plan.trade_plan_id,
                execution_attempt_id=attempt.execution_attempt_id,
                resolution_deadline=now + self.resolution_escalation_ms, note=str(entry.raw_status or ""))
            METRICS.inc("cati_reservation_resolution_pending_total", venue=plan.venue)
            outcome = BoundaryStatus.SUBMIT_UNKNOWN
        else:
            self.reservations.release(plan.portfolio_reservation_id, now)
            outcome = BoundaryStatus.EXECUTION_REJECTED
        log_stage(component="boundary.execution", status=status, reason_codes=entry.reason_codes,
                  runtime_session_id=runtime_session_id, bot_run_id=plan.run_id, cycle_id=plan.cycle_id,
                  user_id=plan.user_id, broker_account_id=plan.broker_account_id, bot_instance_id=plan.bot_instance_id,
                  extra={"trade_plan_id": plan.trade_plan_id, "execution_attempt_id": attempt.execution_attempt_id,
                         "broker_order_id": entry.broker_order_id, "position_id": position_id})
        return BoundaryResult(outcome, plan.trade_plan_id, entry.reason_codes, risk_decision=risk, attempt=attempt,
                              reservation_status=self._res_status(plan))

    def reconcile_submit_unknown(self, plan: TradePlan, *, now_ms: Optional[int] = None) -> BoundaryResult:
        """Resolve a SUBMIT_UNKNOWN attempt from BROKER truth only: order query
        + position query. Unknown stays unknown (reservation untouched); it is
        never re-submitted."""
        now = int(now_ms if now_ms is not None else self._clock())
        attempt_id = ExecutionAttempt.build_id(trade_plan_id=plan.trade_plan_id, trade_plan_hash=plan.trade_plan_hash,
                                               broker_account_id=plan.broker_account_id)
        rows = self.attempts.history(plan.broker_account_id, attempt_id)
        if not rows or rows[-1]["status"] != X.SUBMIT_UNKNOWN.value:
            return BoundaryResult(BoundaryStatus.STILL_UNKNOWN if not rows else BoundaryStatus.RECONCILED,
                                  plan.trade_plan_id, ("NOTHING_TO_RECONCILE",))
        last = rows[-1]["payload"]
        sym = plan.instrument_key.venue_symbol
        try:
            order = self.adapter.query_order(sym, broker_order_id=last.get("broker_order_id"),
                                             client_order_id=last.get("client_order_id"))
            pos = self.adapter.reconcile_position(sym)
        except Exception as exc:  # reconciliation failure is NOT evidence of "no position": stay pending
            record_stage_error("boundary.reconcile_submit_unknown", "EXECUTION", exc, db=self._db,
                               cycle_id=plan.cycle_id, user_id=plan.user_id,
                               broker_account_id=plan.broker_account_id, bot_instance_id=plan.bot_instance_id)
            METRICS.inc("cati_execution_reconciliation_total", outcome="RECONCILIATION_ERROR")
            return BoundaryResult(BoundaryStatus.STILL_UNKNOWN, plan.trade_plan_id, ("RECONCILIATION_ERROR",),
                                  reservation_status=self._res_status(plan))
        base = self._attempt_from_payload(plan, last)
        if pos.answered and pos.side == plan.side and pos.quantity > 0 and (not order.answered or order.executed_qty > 0):
            filled = order.executed_qty if order.answered and order.executed_qty > 0 else pos.quantity
            resolved = replace(base, status=X.RECONCILED_POSITION_EXISTS.value, filled_quantity=float(filled),
                               filled_price=order.avg_price or pos.entry_price, position_id=f"cati_{attempt_id}",
                               resolved_at=now, recorded_at=now,
                               reason_codes=base.reason_codes + ("RECONCILED_FROM_BROKER",))
            self.attempts.append(resolved, len(rows))
            self._resolve(plan, "CONSUMED", now, "BROKER_CONFIRMED_ENTRY")
            METRICS.inc("cati_execution_reconciliation_total", outcome="POSITION_EXISTS")
            return BoundaryResult(BoundaryStatus.RECONCILED, plan.trade_plan_id, ("RECONCILED_POSITION_EXISTS",),
                                  attempt=resolved, reservation_status=self._res_status(plan))
        if pos.answered and pos.side == "FLAT" and order.answered and order.executed_qty == 0 \
                and str(order.status).upper() in ("CANCELED", "EXPIRED", "REJECTED"):
            resolved = replace(base, status=X.RECONCILED_NO_POSITION.value, resolved_at=now, recorded_at=now,
                               reason_codes=base.reason_codes + ("RECONCILED_FROM_BROKER",))
            self.attempts.append(resolved, len(rows))
            self._resolve(plan, "RELEASED", now, "BROKER_CONFIRMED_NO_ENTRY")
            METRICS.inc("cati_execution_reconciliation_total", outcome="NO_POSITION")
            return BoundaryResult(BoundaryStatus.RECONCILED, plan.trade_plan_id, ("RECONCILED_NO_POSITION",),
                                  attempt=resolved, reservation_status=self._res_status(plan))
        METRICS.inc("cati_execution_reconciliation_total", outcome="STILL_UNKNOWN")
        return BoundaryResult(BoundaryStatus.STILL_UNKNOWN, plan.trade_plan_id, ("BROKER_STATE_STILL_UNKNOWN",),
                              reservation_status=self._res_status(plan))

    def _resolve(self, plan: TradePlan, to_status: str, now: int, note: str) -> None:
        rid = plan.portfolio_reservation_id
        if not self.reservations.resolve_pending(rid, to_status, now, note=note):
            # a reservation that was never marked pending (still RESERVED) resolves the same way
            (self.reservations.consume if to_status == "CONSUMED" else self.reservations.release)(rid, now)

    def recover_pending(self, *, now_ms: Optional[int] = None) -> list:
        """Restart / periodic recovery over PERSISTED unresolved ownership.

        For every RESOLUTION_PENDING reservation on this adapter's venue: reload
        its immutable TradePlan (integrity-verified) and run the EXISTING
        submit-unknown reconciliation. Nothing is ever re-submitted. A pending
        row past its escalation deadline that is still unresolved is flagged
        SUBMIT_OUTCOME_UNRESOLVED (component-error evidence + metric) and stays
        owned -- no broker answer is never read as "no position"."""
        from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore

        now = int(now_ms if now_ms is not None else self._clock())
        plans = TradePlanEvidenceStore(self._db)
        out = []
        for row in self.reservations.pending_resolutions():
            plan = None
            try:
                if row.get("trade_plan_id"):
                    plan = plans.load_plan(row["broker_account_id"], row["trade_plan_id"])
            except Exception:
                plan = None
            if plan is None:
                result = BoundaryResult(BoundaryStatus.STILL_UNKNOWN, str(row.get("trade_plan_id")),
                                        ("TRADE_PLAN_EVIDENCE_UNAVAILABLE",), reservation_status=row["status"])
            elif plan.venue.upper() != str(getattr(self.adapter, "venue", "")).upper():
                continue  # another venue's adapter owns this reconciliation
            else:
                result = self.reconcile_submit_unknown(plan, now_ms=now)
            deadline = row.get("resolution_deadline")
            if result.status == BoundaryStatus.STILL_UNKNOWN and deadline is not None and int(deadline) <= now:
                self._escalate_unresolved(row, now)
                result = replace(result, reason_codes=tuple(result.reason_codes) + (SUBMIT_OUTCOME_UNRESOLVED,))
            out.append(result)
        return out

    def _escalate_unresolved(self, row: Mapping[str, Any], now: int) -> None:
        self.reservations.note_unresolved(row["reservation_id"], now, SUBMIT_OUTCOME_UNRESOLVED)
        record_stage_error(
            "boundary.recover_pending", "EXECUTION",
            RuntimeError(f"{SUBMIT_OUTCOME_UNRESOLVED}: reservation {row['reservation_id']} pending since "
                         f"{row.get('pending_since')}; broker has not resolved the entry outcome"),
            db=self._db, cycle_id=row.get("cycle_id"), broker_account_id=row.get("broker_account_id"),
            bot_instance_id=row.get("bot_instance_id"))
        METRICS.inc("cati_submit_outcome_unresolved_total", venue=str(getattr(self.adapter, "venue", "UNKNOWN")))

    # -- 20.17 exit-intent handoff (disabled by default) -----------------------------------------
    def process_exit_decision(self, decision: ExitDecision, plan: TradePlan, *, live_qty: float,
                              sl_order_id: Optional[str] = None, tp_order_id: Optional[str] = None,
                              tp_price: Optional[float] = None) -> BoundaryResult:
        if decision.action in (ExitAction.HOLD.value, ExitAction.NO_CHANGE_FALLBACK.value):
            return BoundaryResult(BoundaryStatus.NO_BROKER_ACTION, plan.trade_plan_id, (decision.action,))
        if not (self.config.active_execution_enabled and self.config.exit_intent_routing_enabled):
            METRICS.inc("cati_exit_routing_total", action=decision.action, status="DISABLED")
            return BoundaryResult(BoundaryStatus.EXIT_ROUTING_DISABLED, plan.trade_plan_id, (decision.action,))
        if decision.trade_plan_id != plan.trade_plan_id or decision.broker_account_id != plan.broker_account_id:
            return BoundaryResult(BoundaryStatus.EXIT_REFUSED, plan.trade_plan_id, ("LINEAGE_MISMATCH",))
        sym = plan.instrument_key.venue_symbol
        try:
            if decision.action == ExitAction.EXIT.value:
                out = self.adapter.submit_exit(sym, side=plan.side, quantity=live_qty)
            elif decision.action in (ExitAction.REDUCE.value, ExitAction.TAKE_PARTIAL.value):
                out = self.adapter.submit_reduce(sym, side=plan.side, fraction=decision.requested_fraction,
                                                 live_qty=live_qty, sl_price=decision.existing_protection_price or 0.0,
                                                 tp_price=tp_price or 0.0, sl_order_id=sl_order_id,
                                                 tp_order_id=tp_order_id)
            else:  # TIGHTEN_PROTECTION -- widening is refused before any broker call
                out = self.adapter.modify_protection(sym, side=plan.side, quantity=live_qty,
                                                     existing_stop=decision.existing_protection_price,
                                                     new_stop=decision.suggested_protection_price,
                                                     old_sl_order_id=sl_order_id, old_tp_order_id=tp_order_id,
                                                     target_price=tp_price)
        except ProtectionWideningRefused as exc:
            return BoundaryResult(BoundaryStatus.EXIT_REFUSED, plan.trade_plan_id, ("PROTECTION_WIDENING_REFUSED",),
                                  detail={"error": str(exc)[:200]})
        METRICS.inc("cati_exit_routing_total", action=decision.action, status="ROUTED")
        return BoundaryResult(BoundaryStatus.EXIT_ROUTED, plan.trade_plan_id, (decision.action,), detail=dict(out or {}))

    # -- helpers ----------------------------------------------------------------------------------
    def _fail_before_risk(self, plan, now, status, code) -> BoundaryResult:
        self.reservations.release(plan.portfolio_reservation_id, now)
        METRICS.inc("cati_execution_boundary_total", status=status)
        return BoundaryResult(status, plan.trade_plan_id, (code,), reservation_status=self._res_status(plan))

    def _res_status(self, plan) -> Optional[str]:
        r = self.reservations.get(plan.portfolio_reservation_id)
        return r.status if r is not None else None

    def _append_risk(self, risk) -> None:
        try:
            self.risk_store.append(risk)
        except Exception as exc:
            record_stage_error("boundary.risk_evidence", "RISK", exc, db=None,
                               broker_account_id=risk.broker_account_id, bot_instance_id=risk.bot_instance_id)

    def _attempt(self, plan, attempt_id, risk_id, req: EntryRequest, *, status, now) -> ExecutionAttempt:
        c = plan.expected_costs
        return ExecutionAttempt(
            execution_attempt_id=attempt_id, trade_plan_id=plan.trade_plan_id, risk_decision_id=risk_id,
            user_id=plan.user_id, broker_account_id=plan.broker_account_id, bot_instance_id=plan.bot_instance_id,
            venue=plan.venue, environment=plan.environment, instrument_key=plan.instrument_key,
            adapter_id=self.adapter.adapter_id, execution_support_status=self.adapter.execution_support_status,
            side=plan.side, resolved_quantity=req.notional / req.requested_price if req.requested_price else None,
            requested_quantity=None, submitted_quantity=None, filled_quantity=None,
            requested_order_type=req.requested_order_type, actual_order_type=None, requested_price=req.requested_price,
            submitted_price=None, filled_price=None, max_slippage_budget_bps=req.max_slippage_bps, status=status,
            broker_order_id=None, client_order_id=None, position_id=None, submitted_at=now, acknowledged_at=None,
            resolved_at=None, recorded_at=now,
            planned_costs={"fee_R": c.fee_R, "spread_R": c.spread_R, "slippage_R": c.slippage_R,
                           "funding_R": c.funding_R, "carry_R": c.carry_R, "total_cost_R": c.total_cost_R},
            realized_costs={}, protection={}, reason_codes=())

    def _resolved(self, base: ExecutionAttempt, e: EntryResult, status, position_id, now) -> ExecutionAttempt:
        realized: Dict[str, float] = {}
        if e.fees is not None:
            realized["fees"] = float(e.fees)
        if e.avg_fill_price and base.requested_price:
            sign = 1.0 if base.side == "LONG" else -1.0
            realized["slippage_bps"] = sign * (e.avg_fill_price - base.requested_price) / base.requested_price * 1e4
        return replace(base, status=status, requested_quantity=e.requested_qty, submitted_quantity=e.submitted_qty,
                       filled_quantity=e.filled_qty, actual_order_type=e.actual_order_type,
                       submitted_price=e.submitted_price, filled_price=e.avg_fill_price,
                       broker_order_id=e.broker_order_id, client_order_id=e.client_order_id, position_id=position_id,
                       acknowledged_at=now if e.broker_order_id else None,
                       resolved_at=now if status != X.SUBMIT_UNKNOWN.value else None, recorded_at=now,
                       realized_costs=realized, protection=dict(e.protection), reason_codes=tuple(e.reason_codes),
                       raw_executor_status=e.raw_status)

    @staticmethod
    def _attempt_from_payload(plan, p: Mapping[str, Any]) -> ExecutionAttempt:
        fields = {k: p.get(k) for k in ExecutionAttempt.__dataclass_fields__}
        fields["instrument_key"] = plan.instrument_key
        fields["reason_codes"] = tuple(p.get("reason_codes") or ())
        fields["planned_costs"] = dict(p.get("planned_costs") or {})
        fields["realized_costs"] = dict(p.get("realized_costs") or {})
        fields["protection"] = dict(p.get("protection") or {})
        return ExecutionAttempt(**fields)

    def _register_lifecycle(self, plan, req: EntryRequest, e: EntryResult, position_id: str) -> None:
        """Hand the BROKER-FILLED position to the existing lifecycle authority
        (PositionManager), exactly as the runner does after a V2 fill."""
        if self.position_manager is None:
            return
        try:
            from app.execution.position_manager import PositionSide

            entry = float(e.avg_fill_price or req.requested_price)
            tp = float(req.target_price) if req.target_price else entry + (1 if plan.side == "LONG" else -1) * 2.2 * abs(
                entry - req.stop_price)
            self.position_manager.open_position(
                symbol=req.venue_symbol, side=PositionSide.LONG if plan.side == "LONG" else PositionSide.SHORT,
                position_id=position_id, entry_price=entry, qty=float(e.filled_qty), stop_price=float(req.stop_price),
                tp1_price=tp, tp2_price=tp, strategy_name="CATI_TRADE_PLAN",
                sl_order_id=e.protection.get("sl_order_id"), tp_order_id=e.protection.get("tp_order_id"))
        except Exception as exc:
            record_stage_error("boundary.register_lifecycle", "EXECUTION", exc, db=self._db,
                               broker_account_id=plan.broker_account_id, bot_instance_id=plan.bot_instance_id)

    def _observe_execution(self, plan, req: EntryRequest, e: EntryResult) -> None:
        try:
            labels = dict(venue=plan.venue, asset_class=plan.instrument_key.asset_class)
            METRICS.inc("cati_execution_attempts_total", status=e.status, **labels)
            if e.status == X.PARTIALLY_FILLED.value:
                METRICS.inc("cati_execution_partial_fills_total", **labels)
            if e.status == X.SUBMIT_UNKNOWN.value:
                METRICS.inc("cati_execution_submit_unknown_total", **labels)
            if e.status == X.REJECTED.value:
                METRICS.inc("cati_execution_rejections_total", reason_family=e.rejection_family or "OTHER", **labels)
            if e.avg_fill_price and req.requested_price:
                sign = 1.0 if plan.side == "LONG" else -1.0
                METRICS.observe("cati_execution_realized_slippage_bps",
                                sign * (e.avg_fill_price - req.requested_price) / req.requested_price * 1e4, **labels)
                METRICS.observe("cati_execution_planned_slippage_r", plan.expected_costs.slippage_R, **labels)
            if e.status in POSITION_EXISTS:
                ok = bool(e.protection.get("sl_order_id")) or str(e.protection.get("status", "")).lower() in (
                    "success", "paper_protection_attached")
                METRICS.inc("cati_protection_placement_total", outcome="SUCCESS" if ok else "UNCONFIRMED", **labels)
        except Exception:
            pass


__all__ = ["BoundaryStatus", "AccountState", "BoundaryResult", "CATIExecutionBoundary",
           "DEFAULT_RESOLUTION_ESCALATION_MS", "SUBMIT_OUTCOME_UNRESOLVED"]
