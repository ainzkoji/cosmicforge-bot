"""TradePlanBuilder (Section 18.15) -- the ONLY way a TradePlan comes into
existence.

Eligibility, checked in this order, each an explicit typed result (never an
exception):

1. INVALID_INPUT            -- broken lineage, tenant mismatch, malformed
                               structural invalidation / targets, no venue evidence
2. VETO_NOT_APPROVED        -- Section 14 outcome is not APPROVE_FOR_RANKING
                               (WATCH and REJECT never plan), or Section 13 did
                               not find the opportunity ECONOMICALLY_ADMISSIBLE
3. RANKING_INCOMPLETE       -- the Section 15 batch is incomplete, or the
                               opportunity is not part of the completed ranking
4. PORTFOLIO_NOT_SELECTED   -- Section 16 did not select it
5. RESERVATION_INVALID      -- no RESERVED, unexpired, matching reservation
6. EXPIRED                  -- the source hypothesis / plan window has lapsed
7. ECONOMICS_STALE          -- the Section 17 venue evidence is no longer valid
8. PLAN_NOT_CREATED         -- valid inputs, but no coherent target or no
                               execution preference the venue declares

Lifecycle: the portfolio reservation stays RESERVED through plan creation.
Creating a plan neither consumes nor releases it -- the future hard-risk /
execution path (Section 20) does. The builder performs no I/O: no clock, no
broker, no order, no slot, margin or capital mutation, no hard-risk call.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from app.trading_intelligence.contracts.economics import AdmissionStatus, CostEstimate, EconomicOpportunity
from app.trading_intelligence.contracts.forecast import OutcomeForecast
from app.trading_intelligence.contracts.portfolio import AccountPortfolioReservation, ReservationStatus
from app.trading_intelligence.contracts.portfolio_intel import PortfolioSelectionDecision
from app.trading_intelligence.contracts.ranking import BotCycleEvaluationBatch, EvaluatedOpportunity, RankedOpportunity
from app.trading_intelligence.contracts.setup import SetupCandidate, timeframe_to_ms
from app.trading_intelligence.contracts.trade_plan import (
    AllowedEntryZone, ExecutionPreferences, ExpectedCosts, InvalidationCode, InvalidationCondition, OrderStyle,
    TargetPurpose, TargetZone, ThesisCode, ThesisCondition, TradePlan, TradePlanBuildStatus, TradePlanPolicy,
    TradePlanResult,
)
from app.trading_intelligence.contracts.venue_economics import ExecutionCapabilities, VenueEconomicObservation
from app.trading_intelligence.contracts.veto import VetoDecision, VetoOutcome, VetoStage
from app.trading_intelligence.versions import TRADE_PLAN_BUILDER_VERSION

S = TradePlanBuildStatus

_FAMILY_THESIS = {
    "TREND_PULLBACK_V2": (ThesisCode.TREND_CONTINUATION_REMAINS_VALID, ThesisCode.HTF_ALIGNMENT_VALID),
    "RANGE_MEAN_REVERSION_V2": (ThesisCode.RANGE_BOUNDARY_REMAINS_INTACT,),
    "MOMENTUM_CONTINUATION_V1": (ThesisCode.MOMENTUM_PARTICIPATION_PRESENT,),
}


@dataclass(frozen=True)
class TenantContext:
    broker_account_id: str
    bot_instance_id: str
    cycle_id: str
    user_id: Optional[str] = None
    run_id: Optional[str] = None


def style_supported(style: str, caps: ExecutionCapabilities) -> bool:
    """Whether a venue DECLARES support for an order-style intent."""
    if style == OrderStyle.MARKET.value:
        return caps.supports_market
    if style in (OrderStyle.LIMIT.value, OrderStyle.PASSIVE_LIMIT.value, OrderStyle.AGGRESSIVE_LIMIT.value):
        return caps.supports_limit
    if style == OrderStyle.POST_ONLY_PREFERRED.value:
        return caps.supports_limit and caps.supports_post_only
    return False


def _fail(status: TradePlanBuildStatus, *codes: str, detail: str = "") -> TradePlanResult:
    return TradePlanResult(status=status.value, reason_codes=tuple(codes), detail=detail)


def _tick_floor(price: float, tick: Optional[float]) -> float:
    return round(math.floor(round(price / tick, 9)) * tick, 12) if tick else price


def _tick_ceil(price: float, tick: Optional[float]) -> float:
    return round(math.ceil(round(price / tick, 9)) * tick, 12) if tick else price


class TradePlanBuilder:
    def __init__(self, policy: Optional[TradePlanPolicy] = None) -> None:
        self.policy = policy or TradePlanPolicy()

    def build_from_evaluated(self, *, ranked: RankedOpportunity, ranking_batch: BotCycleEvaluationBatch,
                             portfolio_decision: PortfolioSelectionDecision,
                             reservation: Optional[AccountPortfolioReservation], evaluated: EvaluatedOpportunity,
                             tenant: TenantContext, now_ms: int) -> TradePlanResult:
        import time as _time

        t0 = _time.perf_counter()
        result = self.build(
            ranked=ranked, ranking_batch=ranking_batch, portfolio_decision=portfolio_decision, reservation=reservation,
            market_state=evaluated.market_state, regime=evaluated.regime, candidate=evaluated.candidate,
            forecast=evaluated.forecast, cost_estimate=evaluated.cost_estimate, opportunity=evaluated.opportunity,
            veto=evaluated.veto, venue_observation=evaluated.venue_observation, tenant=tenant, now_ms=now_ms)
        try:  # Section 21.22 stage latency (bounded label only)
            from app.trading_intelligence.observability.metrics import METRICS

            METRICS.observe("cati_stage_latency_ms", (_time.perf_counter() - t0) * 1000.0, stage="TRADE_PLAN")
            METRICS.inc("cati_trade_plans_total", status=result.status)
        except Exception:
            pass
        return result

    def build(self, *, ranked: RankedOpportunity, ranking_batch: BotCycleEvaluationBatch,
              portfolio_decision: PortfolioSelectionDecision, reservation: Optional[AccountPortfolioReservation],
              market_state: Any, regime: Any, candidate: SetupCandidate, forecast: OutcomeForecast,
              cost_estimate: CostEstimate, opportunity: EconomicOpportunity, veto: VetoDecision,
              venue_observation: Optional[VenueEconomicObservation], tenant: TenantContext,
              now_ms: int) -> TradePlanResult:
        p = self.policy

        # 1. INVALID_INPUT: lineage, tenant, geometry --------------------------------------
        cid = candidate.setup_candidate_id
        links = (
            ("forecast.candidate", forecast.setup_candidate_id == cid),
            ("opportunity.candidate", opportunity.setup_candidate_id == cid),
            ("opportunity.forecast", opportunity.forecast_id == forecast.forecast_id),
            ("opportunity.cost", opportunity.cost_estimate_id == cost_estimate.cost_estimate_id),
            ("veto.opportunity", veto.economic_opportunity_id == opportunity.economic_opportunity_id),
            ("veto.candidate", veto.setup_candidate_id == cid),
            ("ranked.opportunity", ranked.economic_opportunity_id == opportunity.economic_opportunity_id),
            ("ranked.veto", ranked.veto_decision_id == veto.veto_decision_id),
            ("ranked.candidate", ranked.setup_candidate_id == cid),
            ("market_state.candidate", market_state.market_state_id == candidate.market_state_id),
            ("regime.market_state", regime.market_state_id == candidate.market_state_id),
            ("cost.instrument", cost_estimate.instrument_key.canonical_symbol == candidate.instrument_key.canonical_symbol),
        )
        broken = [name for name, ok in links if not ok]
        if broken:
            return _fail(S.INVALID_INPUT, "LINEAGE_MISMATCH", detail=",".join(broken))
        if venue_observation is None or cost_estimate.venue_observation_id != venue_observation.observation_id:
            return _fail(S.INVALID_INPUT, "VENUE_OBSERVATION_MISSING",
                         detail="a TradePlan requires Section 17 venue/account cost evidence")
        tenant_ids = {
            "account": {tenant.broker_account_id, portfolio_decision.broker_account_id, ranked.broker_account_id,
                        ranking_batch.broker_account_id},
            "bot": {tenant.bot_instance_id, portfolio_decision.bot_instance_id, ranked.bot_instance_id,
                    ranking_batch.bot_instance_id},
            "cycle": {tenant.cycle_id, portfolio_decision.cycle_id, ranked.cycle_id, ranking_batch.cycle_id},
        }
        mixed = [k for k, v in tenant_ids.items() if len(v) != 1]
        if mixed:
            return _fail(S.INVALID_INPUT, "TENANT_MISMATCH", detail=",".join(mixed))
        # account-scoped economics must belong to THIS account (never shared across tenants)
        for label, acct in (("cost", cost_estimate.broker_account_id), ("observation", venue_observation.broker_account_id)):
            if acct is not None and acct != tenant.broker_account_id:
                return _fail(S.INVALID_INPUT, "COST_ACCOUNT_MISMATCH", detail=label)
        if now_ms < candidate.decision_time:
            return _fail(S.INVALID_INPUT, "NON_CAUSAL_BUILD_TIME")
        if candidate.valid_until is None:
            return _fail(S.INVALID_INPUT, "CANDIDATE_VALIDITY_UNKNOWN")
        entry, inv, risk = candidate.trigger_reference, candidate.structural_invalidation, candidate.initial_structural_risk
        long_side = candidate.side == "LONG"
        coherent = (inv < entry) if long_side else (inv > entry)
        if risk <= 0 or not coherent or not math.isclose(abs(entry - inv), risk, rel_tol=1e-9, abs_tol=1e-12):
            return _fail(S.INVALID_INPUT, "MALFORMED_INVALIDATION")
        target = candidate.target_reference
        if target is not None and ((target <= entry) if long_side else (target >= entry)):
            return _fail(S.INVALID_INPUT, "TARGET_DIRECTION_INCOHERENT")

        # 2. Section 13/14 authority ---------------------------------------------------------
        if veto.outcome != VetoOutcome.APPROVE_FOR_RANKING.value or veto.stage != VetoStage.PRE_RANKING.value:
            return _fail(S.VETO_NOT_APPROVED, f"VETO_{veto.outcome}", *veto.reason_codes)
        if opportunity.admission_status != AdmissionStatus.ECONOMICALLY_ADMISSIBLE.value:
            return _fail(S.VETO_NOT_APPROVED, opportunity.admission_status, *opportunity.reason_codes)

        # 3. Section 15: whole-universe ranking completed, and this opportunity in it --------
        if not ranking_batch.batch_complete:
            return _fail(S.RANKING_INCOMPLETE, "BATCH_INCOMPLETE", *ranking_batch.reason_codes)
        if opportunity.economic_opportunity_id not in ranking_batch.approved_opportunity_ids \
                or ranked.ranked_opportunity_id not in portfolio_decision.ranked_opportunity_ids:
            return _fail(S.RANKING_INCOMPLETE, "NOT_IN_COMPLETED_BATCH")

        # 4. Section 16 selection ------------------------------------------------------------------
        if ranked.ranked_opportunity_id not in portfolio_decision.selected_opportunity_ids:
            return _fail(S.PORTFOLIO_NOT_SELECTED, "NOT_SELECTED_BY_PORTFOLIO")

        # 5. reservation ---------------------------------------------------------------------------
        r = reservation
        instrument = (candidate.instrument_key.canonical_symbol, candidate.instrument_key.venue,
                      candidate.instrument_key.venue_symbol, candidate.side)
        if p.require_portfolio_reservation:
            problems = []
            if r is None:
                problems.append("RESERVATION_MISSING")
            else:
                if r.status != ReservationStatus.RESERVED.value:
                    problems.append(f"RESERVATION_{r.status}")
                if not portfolio_decision.is_reserved or portfolio_decision.reservation_id != r.reservation_id:
                    problems.append("RESERVATION_NOT_DECISIONS")
                if r.broker_account_id != tenant.broker_account_id:
                    problems.append("RESERVATION_WRONG_ACCOUNT")
                if r.bot_instance_id != tenant.bot_instance_id or r.cycle_id != tenant.cycle_id:
                    problems.append("RESERVATION_WRONG_BOT_OR_CYCLE")
                if cid not in r.selected_candidate_ids or (r.selected_instruments and instrument not in r.selected_instruments):
                    problems.append("RESERVATION_WRONG_CANDIDATE")
                if r.expires_at <= now_ms:
                    problems.append("RESERVATION_EXPIRED")
                elif r.expires_at - now_ms < p.minimum_reservation_ttl_ms:
                    problems.append("RESERVATION_TTL_TOO_SHORT")
            if problems:
                return _fail(S.RESERVATION_INVALID, *problems)

        # 6. expiry: the plan cannot outlive its source hypothesis ---------------------------------------
        bar_ms = timeframe_to_ms(candidate.timeframe) or 0
        bounds = [candidate.valid_until, candidate.decision_time + p.max_plan_age_bars * bar_ms,
                  venue_observation.valid_until]
        if r is not None:
            bounds.append(r.expires_at)
        expiry = min(bounds)
        if expiry <= now_ms:
            return _fail(S.EXPIRED, "PLAN_WINDOW_ELAPSED")

        # 7. economics freshness ----------------------------------------------------------------------------
        if venue_observation.valid_until <= now_ms:
            return _fail(S.ECONOMICS_STALE, "VENUE_OBSERVATION_EXPIRED")
        if not venue_observation.tradable:
            return _fail(S.ECONOMICS_STALE, "VENUE_EVIDENCE_NOT_TRADABLE", *venue_observation.fatal_reason_codes)
        if cost_estimate.decision_time is not None and cost_estimate.decision_time > now_ms:
            return _fail(S.ECONOMICS_STALE, "NON_CAUSAL_COST_EVIDENCE")

        # 8. construct -----------------------------------------------------------------------------------------
        caps = venue_observation.execution_capabilities
        meta = venue_observation.instrument_metadata
        tick = meta.tick_size if meta else None
        asset = candidate.instrument_key.asset_class
        adverse = min(p.max_entry_extension_R * risk, p.default_max_slippage_bps / 1e4 * entry)
        improve = p.max_entry_improvement_R * risk
        if long_side:
            lo, hi = _tick_ceil(entry - improve, tick), _tick_floor(entry + adverse, tick)
        else:
            lo, hi = _tick_ceil(entry - adverse, tick), _tick_floor(entry + improve, tick)
        lo, hi = min(lo, entry), max(hi, entry)  # tick alignment never excludes the reference itself
        adverse_px = (hi - entry) if long_side else (entry - lo)
        zone = AllowedEntryZone(reference_price=entry, minimum_price=lo, maximum_price=hi,
                                maximum_adverse_slippage_bps=adverse_px / entry * 1e4,
                                maximum_extension_R=adverse_px / risk, valid_until=expiry)

        targets = self._targets(candidate, forecast, risk, long_side)
        if not targets:
            return _fail(S.PLAN_NOT_CREATED, "NO_VALID_TARGET")

        if caps is None:
            return _fail(S.PLAN_NOT_CREATED, "EXECUTION_CAPABILITIES_UNAVAILABLE")
        styles = [s for s in p.order_preferences_by_asset_class.get(asset, ()) if style_supported(s, caps)]
        tifs = [t for t in p.time_in_force_by_asset_class.get(asset, ()) if t in caps.supported_time_in_force]
        if not styles or not tifs:
            return _fail(S.PLAN_NOT_CREATED, "NO_SUPPORTED_EXECUTION_PREFERENCE")
        prefs = ExecutionPreferences(
            preferred_order_style=styles[0], fallback_order_styles=tuple(styles[1:]),
            maximum_slippage_bps=zone.maximum_adverse_slippage_bps, urgency=p.default_urgency,
            fill_policy=p.default_fill_policy, time_in_force=tifs[0], max_spread_bps=p.max_spread_budget_bps,
            market_reference_max_age_ms=p.max_market_reference_age_ms)

        costs = ExpectedCosts(
            cost_estimate_id=cost_estimate.cost_estimate_id, venue_observation_id=cost_estimate.venue_observation_id,
            fee_R=cost_estimate.fee_R, spread_R=cost_estimate.spread_R, slippage_R=cost_estimate.slippage_R,
            funding_R=cost_estimate.funding_R, carry_R=cost_estimate.carry_R, total_cost_R=cost_estimate.total_cost_R,
            cost_uncertainty_R=cost_estimate.cost_uncertainty_R, cost_scope=cost_estimate.cost_scope,
            adapter_status=cost_estimate.adapter_status, source_quality=cost_estimate.source_quality)

        reason_codes: List[str] = ["SHADOW_TRADE_PLAN", "NO_FINAL_QUANTITY", "RESERVATION_REMAINS_RESERVED"]
        if len(targets) > 1:
            reason_codes.append("FORECAST_QUANTILE_TARGET_ADDED")

        versions = (
            ("admission_policy_hash", opportunity.admission_policy_hash),
            ("adapter", f"{venue_observation.adapter_id}:{venue_observation.adapter_version}:{venue_observation.adapter_status}"),
            ("cost_model_version", cost_estimate.cost_model_version),
            ("cost_policy_hash", cost_estimate.cost_policy_hash),
            ("forecast", f"{forecast.forecast_version}:{forecast.library_hash}:{forecast.calibration_status}"),
            ("portfolio_policy_hash", portfolio_decision.portfolio_policy_hash),
            ("ranking_policy_hash", ranked.ranking_policy_hash),
            ("setup_policy_hash", candidate.setup_policy_hash),
            ("trade_plan_builder_version", TRADE_PLAN_BUILDER_VERSION),
            ("trade_plan_policy_hash", p.policy_hash),
            ("trade_plan_policy_version", p.schema_version),
            ("venue_observation_hash", venue_observation.observation_hash),
            ("veto_policy_hash", veto.veto_policy_hash),
        )
        plan = TradePlan.build(
            snapshot_id=candidate.snapshot_id, market_state_id=candidate.market_state_id,
            regime_distribution_id=regime.regime_distribution_id, source_candidate_id=cid,
            forecast_id=forecast.forecast_id, venue_observation_id=venue_observation.observation_id,
            cost_estimate_id=cost_estimate.cost_estimate_id, economic_opportunity_id=opportunity.economic_opportunity_id,
            veto_decision_id=veto.veto_decision_id, ranking_batch_id=ranking_batch.cycle_batch_id,
            ranked_opportunity_id=ranked.ranked_opportunity_id,
            portfolio_decision_id=portfolio_decision.portfolio_selection_id,
            portfolio_reservation_id=r.reservation_id if r is not None else "",
            user_id=tenant.user_id, broker_account_id=tenant.broker_account_id, bot_instance_id=tenant.bot_instance_id,
            run_id=tenant.run_id, cycle_id=tenant.cycle_id,
            instrument_key=candidate.instrument_key, venue=venue_observation.venue,
            environment=venue_observation.environment, side=candidate.side, setup_family=candidate.setup_family,
            setup_version=candidate.setup_version, decision_time=candidate.decision_time, entry_reference=entry,
            allowed_entry_zone=zone, structural_invalidation_price=inv, initial_risk_distance=risk,
            target_zones=targets, expected_holding_time_ms=cost_estimate.expected_holding_ms, plan_expiry_time=expiry,
            expected_gross_R=opportunity.ev_gross_r, expected_net_R=opportunity.ev_net_r,
            conservative_edge_R=opportunity.conservative_edge_r, p_net_profitable=opportunity.p_net_profitable_mean,
            credible_interval_low=opportunity.credible_interval_low,
            credible_interval_high=opportunity.credible_interval_high, raw_support=opportunity.raw_support,
            ess=opportunity.ess, backoff_level=opportunity.backoff_level, expected_costs=costs,
            economic_size_assumption=cost_estimate.reference_notional, execution_preferences=prefs,
            thesis_conditions=self._thesis(candidate, veto, opportunity),
            invalidation_conditions=self._invalidations(inv, candidate.side, zone, expiry, r, p),
            reason_codes=tuple(reason_codes), versions=versions, plan_created_at=int(now_ms),
        )
        return TradePlanResult(status=S.PLAN_CREATED.value, plan=plan, reason_codes=plan.reason_codes)

    # -- pieces ----------------------------------------------------------------------------------
    def _targets(self, candidate: SetupCandidate, forecast: OutcomeForecast, risk: float,
                 long_side: bool) -> Tuple[TargetZone, ...]:
        p = self.policy
        entry, hw = candidate.trigger_reference, p.target_zone_half_width_R * risk
        sign = 1.0 if long_side else -1.0
        out: List[TargetZone] = []

        def zone(zone_id: str, price: float, purpose: str) -> Optional[TargetZone]:
            # the zone sits on the entry side of the target, never beyond it
            low, high = (price - hw, price) if long_side else (price, price + hw)
            if (low <= entry) if long_side else (high >= entry):
                return None  # direction-incoherent or inside the entry: rejected, not "fixed"
            return TargetZone(zone_id, low, high, abs(price - entry) / risk, purpose)

        if candidate.target_reference is not None:
            z = zone("T1", candidate.target_reference, TargetPurpose.STRUCTURAL.value)
            if z is not None:
                out.append(z)
        if p.include_forecast_quantile_target and forecast is not None:
            mfe = forecast.mfe_R_quantiles.get(p.forecast_target_quantile)
            if mfe is not None and mfe >= p.min_forecast_target_R:
                price = entry + sign * mfe * risk
                if not out or abs(price - (candidate.target_reference or entry)) > 2 * hw:
                    z = zone(f"T{len(out) + 1}", price, TargetPurpose.FORECAST_QUANTILE.value)
                    if z is not None:
                        out.append(z)
        return tuple(sorted(out, key=lambda t: t.reference_R))

    @staticmethod
    def _thesis(candidate: SetupCandidate, veto: VetoDecision, opportunity: EconomicOpportunity
                ) -> Tuple[ThesisCondition, ...]:
        family = _FAMILY_THESIS.get(candidate.setup_family)
        if family is None and candidate.setup_family.startswith("BREAKOUT"):
            family = (ThesisCode.BREAKOUT_HOLDS_ABOVE_BOUNDARY if candidate.side == "LONG"
                      else ThesisCode.BREAKOUT_HOLDS_BELOW_BOUNDARY,)
        evidence = tuple(sorted((k, f"{v:.6g}") for k, v in candidate.evidence_components.items()))
        out = [ThesisCondition(code.value, evidence) for code in (family or ())]

        def fam(name: str) -> Tuple[Tuple[str, str], ...]:
            return tuple(sorted((c.check, c.status) for c in veto.checks if c.family == name))

        out += [
            ThesisCondition(ThesisCode.EVENT_CONTEXT_ACCEPTABLE.value, fam("EVENT")),
            ThesisCondition(ThesisCode.LIQUIDITY_NOT_DEGRADED.value, fam("LIQUIDITY")),
            ThesisCondition(ThesisCode.BROKER_HEALTH_ACCEPTABLE.value, fam("SYSTEM")),
            ThesisCondition(ThesisCode.ECONOMIC_EDGE_POSITIVE.value,
                            (("conservative_edge_r", f"{opportunity.conservative_edge_r:.6g}"),
                             ("ev_net_r", f"{opportunity.ev_net_r:.6g}"))),
        ]
        return tuple(out)

    @staticmethod
    def _invalidations(inv: float, side: str, zone: AllowedEntryZone, expiry: int,
                       reservation: Optional[AccountPortfolioReservation], p: TradePlanPolicy
                       ) -> Tuple[InvalidationCondition, ...]:
        I = InvalidationCode
        return (
            InvalidationCondition(I.STRUCTURAL_LEVEL_BROKEN.value, (("price", repr(inv)), ("side", side))),
            InvalidationCondition(I.ENTRY_ZONE_EXPIRED.value, (("valid_until", str(zone.valid_until)),)),
            InvalidationCondition(I.PLAN_EXPIRED.value, (("plan_expiry_time", str(expiry)),)),
            InvalidationCondition(I.REGIME_SHIFTED_TO_SHOCK.value),
            InvalidationCondition(I.EVENT_VETO_BECAME_ACTIVE.value),
            InvalidationCondition(I.BROKER_HEALTH_DEGRADED.value),
            InvalidationCondition(I.SPREAD_EXCEEDED_BUDGET.value, (("max_spread_bps", repr(p.max_spread_budget_bps)),)),
            InvalidationCondition(I.ECONOMIC_EDGE_INVALIDATED.value,
                                  (("min_conservative_edge_R", repr(p.min_conservative_edge_R)),)),
            InvalidationCondition(I.PORTFOLIO_RESERVATION_LOST.value,
                                  (("reservation_id", reservation.reservation_id if reservation else ""),)),
        )


__all__ = ["TenantContext", "TradePlanBuilder", "style_supported"]
