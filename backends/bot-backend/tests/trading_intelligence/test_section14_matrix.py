"""Closure item 9 -- the complete Section 14 veto matrix: every family, every
listed reason code, a positive (fires) and a negative (does not fire) case,
with the EXACT resolved outcome."""
from __future__ import annotations

import dataclasses

import pytest
from _helpers import clear_event_context, evaluated_from_chain, full_chain, healthy_system_context, permissive_veto_policy

from app.trading_intelligence.contracts.data_quality import DataQuality, DataQualityLevel
from app.trading_intelligence.contracts.events import EventRiskContext, MaintenanceContext, MarketEvent
from app.trading_intelligence.contracts.market_state import DerivativesState, LiquidityState, StateUncertainty
from app.trading_intelligence.contracts.ranking import EvaluatedOpportunity
from app.trading_intelligence.contracts.system_health import BrokerHealthContext, SystemHealthContext
from app.trading_intelligence.contracts.veto import VetoStage
from app.trading_intelligence.forecast.library import empty_library
from app.trading_intelligence.ranking.engine import rank_opportunities
from app.trading_intelligence.veto.engine import evaluate_veto


@pytest.fixture(scope="module")
def chain():
    return full_chain()


def run(chain, **over):
    args = dict(opportunity=chain["opportunity"], candidate=chain["candidate"], market_state=chain["market_state"],
                regime_distribution=chain["regime"], forecast=chain["forecast"], cost_estimate=chain["cost"],
                policy=permissive_veto_policy(), system_context=healthy_system_context(),
                event_context=clear_event_context())
    args.update(over)
    return evaluate_veto(**args)


# -- mutators: each returns evaluate_veto overrides that should trigger the code ---------------------
def _ms(chain, **kw):
    return {"market_state": dataclasses.replace(chain["market_state"], **kw)}


def _liq(**kw):
    base = dict(spread_bps=1.0, spread_percentile=0.2, top_book_depth=100.0, depth_imbalance=0.0,
                estimated_slippage=0.5, stale_book=False, available=True)
    base.update(kw)
    return LiquidityState(**base)


def _der(**kw):
    base = dict(funding_current=0.01, funding_predicted=None, funding_percentile=0.5, time_to_funding=None,
                open_interest=1.0, open_interest_delta=0.0, basis=None, mark_index_spread=None,
                crowding_state="NEUTRAL", available=True)
    base.update(kw)
    return DerivativesState(**base)


def _shift(chain, **kw):
    s = dataclasses.replace(chain["forecast"].distribution_shift_assessment, **kw)
    return {"forecast": dataclasses.replace(chain["forecast"], distribution_shift_assessment=s)}


def _event(chain, **kw):
    t = chain["candidate"].decision_time
    e = MarketEvent("e1", "cal", "INFLATION", t + 60_000, "HIGH", affected_currencies=("USD",), **kw)
    return {"event_context": dataclasses.replace(clear_event_context(), events=(e,))}


def _maint(chain):
    t = chain["candidate"].decision_time
    w = MarketEvent("m1", "venue", "EXCHANGE_MAINTENANCE", t + 60_000, "HIGH", affected_venues=("binance",))
    return {"event_context": dataclasses.replace(clear_event_context(), maintenance=MaintenanceContext("AVAILABLE", (w,)))}


def _broker(status, reasons=()):
    return {"system_context": SystemHealthContext(broker_health=BrokerHealthContext(
        "acct-1", "binance", "DEMO", status, 0, "test", 0, tuple(reasons)))}


def _portfolio(code):
    return {"stage": VetoStage.PORTFOLIO_STAGE.value, "portfolio_findings": (code,)}


def _insufficient(chain):
    c = full_chain(library=empty_library(label_policy_version="1.0.0", cost_model_version="1.0.0"))
    return dict(opportunity=c["opportunity"], candidate=c["candidate"], market_state=c["market_state"],
                regime_distribution=c["regime"], forecast=c["forecast"], cost_estimate=c["cost"])


MATRIX = [
    # family, reason code, mutator, expected outcome
    ("STATE", "TRANSITION_HIGH", lambda c: {"regime_distribution": dataclasses.replace(c["regime"], transition_uncertainty=0.9)}, "REJECT"),
    ("STATE", "STATE_UNCERTAINTY_HIGH", lambda c: _ms(c, state_uncertainty=StateUncertainty(value=0.7)), "REJECT"),
    ("STATE", "SHOCK_STATE", lambda c: _ms(c, volatility_state=dataclasses.replace(c["market_state"].volatility_state, shock_state=True)), "REJECT"),
    ("EVIDENCE", "INSUFFICIENT_SUPPORT", _insufficient, "WATCH"),
    ("EVIDENCE", "WIDE_CREDIBLE_INTERVAL", lambda c: {"forecast": dataclasses.replace(c["forecast"], credible_interval_low=0.05, credible_interval_high=0.95)}, "REJECT"),
    ("EVIDENCE", "UNCALIBRATED_PROBABILITY", lambda c: {"policy": permissive_veto_policy(calibration_statuses_allowed_for_approval=("CALIBRATED",))}, "WATCH"),
    ("OOD", "DISTRIBUTION_SHIFT_HIGH", lambda c: _shift(c, ood_score=0.95), "REJECT"),
    ("OOD", "UNSEEN_STATE_BUCKET", lambda c: _shift(c, unseen_categories=("regime:NEVER_SEEN",)), "WATCH"),
    ("GEOMETRY", "POOR_REWARD_GEOMETRY", lambda c: {"candidate": dataclasses.replace(c["candidate"], room_to_target_R=0.1)}, "REJECT"),
    ("GEOMETRY", "LATE_ENTRY", lambda c: _ms(c, trend_state=dataclasses.replace(c["market_state"].trend_state, extension_atr=5.0, available=True)), "REJECT"),
    ("GEOMETRY", "STRUCTURAL_INVALIDATION_TOO_WIDE", lambda c: {"candidate": dataclasses.replace(c["candidate"], initial_structural_risk=c["candidate"].trigger_reference * 0.2)}, "REJECT"),
    ("COST", "SPREAD_ANOMALY", lambda c: {"cost_estimate": dataclasses.replace(c["cost"], spread_R=0.5)}, "REJECT"),
    ("COST", "SLIPPAGE_TOO_HIGH", lambda c: {"cost_estimate": dataclasses.replace(c["cost"], slippage_R=0.5)}, "REJECT"),
    ("COST", "FUNDING_COST_EXCESSIVE", lambda c: {"cost_estimate": dataclasses.replace(c["cost"], funding_R=0.5)}, "REJECT"),
    ("LIQUIDITY", "DEPTH_INSUFFICIENT", lambda c: {**_ms(c, liquidity_state=_liq(top_book_depth=1.0)), "policy": permissive_veto_policy(min_top_book_depth=50.0)}, "REJECT"),
    ("LIQUIDITY", "STALE_BOOK", lambda c: _ms(c, liquidity_state=_liq(stale_book=True)), "WATCH"),
    ("LIQUIDITY", "LIQUIDITY_DETERIORATION", lambda c: _ms(c, liquidity_state=_liq(spread_percentile=0.99)), "REJECT"),
    ("CROWDING", "FUNDING_EXTREME", lambda c: _ms(c, derivatives_state=_der(funding_percentile=0.99)), "REJECT"),
    ("CROWDING", "OI_CROWDING", lambda c: _ms(c, derivatives_state=_der(crowding_state="CROWDED_LONG")), "WATCH"),
    ("EVENT", "EVENT_RISK_WINDOW", _event, "REJECT"),
    ("EVENT", "EXCHANGE_MAINTENANCE", _maint, "REJECT"),
    ("EVENT", "EVENT_SOURCE_UNAVAILABLE", lambda c: {"event_context": dataclasses.replace(clear_event_context(), source_state="UNAVAILABLE")}, "WATCH"),
    ("EVENT", "EVENT_SOURCE_STALE", lambda c: {"event_context": dataclasses.replace(clear_event_context(), source_state="STALE")}, "WATCH"),
    ("PORTFOLIO", "ACCOUNT_CORRELATION_CONFLICT", lambda c: _portfolio("ACCOUNT_CORRELATION_CONFLICT"), "REJECT"),
    ("PORTFOLIO", "COMMON_FACTOR_CONCENTRATION", lambda c: _portfolio("COMMON_FACTOR_CONCENTRATION"), "REJECT"),
    ("PORTFOLIO", "DUPLICATE_EXPOSURE", lambda c: _portfolio("DUPLICATE_EXPOSURE"), "REJECT"),
    ("SYSTEM", "DATA_QUALITY_FAULT", lambda c: _ms(c, data_quality=DataQuality(level=DataQualityLevel.INVALID, reason_codes=("X",))), "REJECT"),
    ("SYSTEM", "BROKER_DEGRADED", lambda c: _broker("DEGRADED", ("CIRCUIT_DEGRADED",)), "WATCH"),
    ("SYSTEM", "BROKER_UNAVAILABLE", lambda c: _broker("UNAVAILABLE"), "REJECT"),
    ("SYSTEM", "BROKER_UNKNOWN", lambda c: _broker("UNKNOWN"), "WATCH"),
    ("SYSTEM", "CATI_COMPONENT_ERROR", lambda c: {"system_context": dataclasses.replace(healthy_system_context(), component_errors=("boom",))}, "REJECT"),
]


def test_baseline_is_clean_approve(chain):
    d = run(chain)
    assert d.outcome == "APPROVE_FOR_RANKING" and d.reason_codes == (), d.reason_codes


@pytest.mark.parametrize("family,code,mutate,expected", MATRIX, ids=[m[1] for m in MATRIX])
def test_positive_case_fires_with_exact_outcome(chain, family, code, mutate, expected):
    d = run(chain, **mutate(chain))
    assert code in d.reason_codes, d.reason_codes
    assert d.outcome == expected, (d.outcome, d.reason_codes)
    fired = [c for c in d.checks if c.reason_code == code]
    assert fired and all(c.family == family for c in fired)
    assert not d.approved_for_ranking


@pytest.mark.parametrize("family,code,mutate,expected", MATRIX, ids=[m[1] for m in MATRIX])
def test_negative_case_does_not_fire(chain, family, code, mutate, expected):
    d = run(chain)
    assert code not in d.reason_codes
    fam = [c for c in d.checks if c.family == family]
    assert fam and all(c.status in ("PASS", "NOT_EVALUATED") for c in fam)


def test_all_ten_families_evaluated_every_time(chain):
    assert {c.family for c in run(chain).checks} == {
        "STATE", "EVIDENCE", "OOD", "GEOMETRY", "COST", "LIQUIDITY", "CROWDING", "EVENT", "PORTFOLIO", "SYSTEM"}


def test_event_source_unavailable_policy_variants(chain):
    unavailable = dataclasses.replace(clear_event_context(), source_state="UNAVAILABLE")
    assert run(chain, event_context=unavailable).outcome == "WATCH"
    assert run(chain, event_context=unavailable, policy=permissive_veto_policy(event_unavailable_status="REJECT")).outcome == "REJECT"
    permitted = run(chain, event_context=unavailable, policy=permissive_veto_policy(event_unavailable_status="NOT_EVALUATED"))
    assert permitted.outcome == "APPROVE_FOR_RANKING"  # research-shadow permission, still recorded:
    assert any(c.reason_code == "EVENT_SOURCE_UNAVAILABLE" and c.status == "NOT_EVALUATED" for c in permitted.checks)


def _ev(chain, **over):
    veto = run(chain, **over)
    return EvaluatedOpportunity(chain["candidate"], chain["market_state"], chain["regime"], chain["forecast"],
                                chain["cost"], chain["opportunity"], veto)


def test_only_approve_reaches_ranking_watch_and_reject_never(chain):
    approve = _ev(chain)
    watch = _ev(chain, **_broker("UNKNOWN"))
    reject = _ev(chain, **_broker("UNAVAILABLE"))
    ranked, excluded = rank_opportunities([approve, watch, reject])
    assert [r.veto_decision_id for r in ranked] == [approve.veto.veto_decision_id]
    assert len(excluded) == 2


def test_watch_cannot_reserve_slots(chain, tmp_path):
    from _pf import add_bot, make_db
    from app.trading_intelligence.portfolio.context import build_portfolio_market_context
    from app.trading_intelligence.contracts.portfolio_intel import PortfolioPolicy
    from app.trading_intelligence.portfolio.service import ShadowAccountPortfolioService

    db = make_db(tmp_path / "w.db")
    add_bot(db, "botA", "acct1")
    watch = _ev(chain, **_broker("UNKNOWN"))
    # force a WATCH opportunity into the portfolio stage (the ranker would never emit it)
    approve_ranked, _ = rank_opportunities([_ev(chain)])
    ctx = build_portfolio_market_context({}, chain["candidate"].decision_time, PortfolioPolicy())
    out = ShadowAccountPortfolioService(db).select_and_reserve(
        ranked=approve_ranked, broker_account_id="acct1", bot_instance_id="botA", cycle_id="c1", max_open_positions=3,
        context=ctx, now_ms=chain["candidate"].decision_time,
        evaluated_by_candidate_id={watch.candidate.setup_candidate_id: watch})
    assert out.reservation is None and out.decision.selected_opportunity_ids == ()
    assert "NOT_APPROVED_FOR_RANKING" in out.decision.reason_codes
