"""Section 14.17 -- veto and abstention tests."""
from __future__ import annotations

import dataclasses
import inspect

import pytest
from _helpers import build_library, clear_event_context, full_chain, healthy_system_context, permissive_veto_policy

from app.trading_intelligence.contracts.data_quality import DataQuality, DataQualityLevel
from app.trading_intelligence.contracts.economics import AdmissionStatus
from app.trading_intelligence.contracts.market_state import DerivativesState, LiquidityState
from app.trading_intelligence.contracts.events import EventRiskContext, MaintenanceContext, MarketEvent
from app.trading_intelligence.contracts.system_health import BrokerHealthContext, SystemHealthContext
from app.trading_intelligence.contracts.veto import VetoPolicy, VetoStage


def _broker(status, **kw):
    return SystemHealthContext(broker_health=BrokerHealthContext(
        broker_account_id="acct-1", venue="binance", environment="DEMO", status=status, observed_at=0,
        source="test", freshness_ms=0), **kw)
from app.trading_intelligence.veto.engine import component_error_decision, evaluate_veto


@pytest.fixture(scope="module")
def chain():
    return full_chain()


def _veto(chain, **kw):
    ms = kw.pop("market_state", chain["market_state"])
    policy = kw.pop("policy", permissive_veto_policy())
    kw.setdefault("system_context", healthy_system_context())
    kw.setdefault("event_context", clear_event_context())
    return evaluate_veto(
        opportunity=chain["opportunity"], candidate=chain["candidate"], market_state=ms,
        regime_distribution=chain["regime"], forecast=chain["forecast"], cost_estimate=chain["cost"],
        policy=policy, **kw,
    )


def _check(decision, name):
    return next(c for c in decision.checks if c.check == name)


def test_baseline_admissible_opportunity_can_approve(chain):
    assert chain["opportunity"].admission_status == AdmissionStatus.ECONOMICALLY_ADMISSIBLE.value
    d = _veto(chain)
    assert d.outcome == "APPROVE_FOR_RANKING", d.reason_codes
    assert d.approved_for_ranking


def test_positive_edge_but_shock_rejects(chain):
    ms = chain["market_state"]
    shocked = dataclasses.replace(ms, volatility_state=dataclasses.replace(ms.volatility_state, shock_state=True))
    d = _veto(chain, market_state=shocked)
    assert chain["opportunity"].ev_net_r > 0
    assert d.outcome == "REJECT" and "SHOCK_STATE" in d.reason_codes


def test_positive_edge_but_ood_high_rejects(chain):
    shift = dataclasses.replace(chain["forecast"].distribution_shift_assessment, ood_score=0.95)
    forecast = dataclasses.replace(chain["forecast"], distribution_shift_assessment=shift)
    d = evaluate_veto(opportunity=chain["opportunity"], candidate=chain["candidate"], market_state=chain["market_state"],
                      regime_distribution=chain["regime"], forecast=forecast, cost_estimate=chain["cost"],
                      policy=permissive_veto_policy(),
                      system_context=healthy_system_context(), event_context=clear_event_context())
    assert d.outcome == "REJECT" and "DISTRIBUTION_SHIFT_HIGH" in d.reason_codes


def test_uncalibrated_probability_is_watch_by_default_policy(chain):
    d = _veto(chain, policy=VetoPolicy())
    assert d.outcome == "WATCH" and "UNCALIBRATED_PROBABILITY" in d.reason_codes


def test_uncalibrated_can_be_configured_reject(chain):
    d = _veto(chain, policy=VetoPolicy(uncalibrated_outcome="REJECT"))
    assert d.outcome == "REJECT"


def test_calibrated_library_can_approve():
    c = full_chain(calibration_status="CALIBRATED")
    d = evaluate_veto(opportunity=c["opportunity"], candidate=c["candidate"], market_state=c["market_state"],
                      regime_distribution=c["regime"], forecast=c["forecast"], cost_estimate=c["cost"], policy=VetoPolicy(ood_score_watch=0.6),
                      system_context=healthy_system_context(), event_context=clear_event_context())
    assert d.outcome == "APPROVE_FOR_RANKING", d.reason_codes


def test_insufficient_support_cannot_approve():
    from app.trading_intelligence.forecast.library import empty_library

    empty = empty_library(label_policy_version="1.0.0", cost_model_version="1.0.0")
    c = full_chain(library=empty)
    assert c["opportunity"].admission_status == AdmissionStatus.INSUFFICIENT_EVIDENCE.value
    for pol in (permissive_veto_policy(), permissive_veto_policy(insufficient_evidence_outcome="NOT_EVALUATED")):
        d = evaluate_veto(opportunity=c["opportunity"], candidate=c["candidate"], market_state=c["market_state"],
                          regime_distribution=c["regime"], forecast=c["forecast"], cost_estimate=c["cost"], policy=pol,
                          system_context=healthy_system_context(), event_context=clear_event_context())
        assert d.outcome != "APPROVE_FOR_RANKING"
        assert "INSUFFICIENT_SUPPORT" in d.reason_codes


def test_wide_credible_interval_cannot_approve(chain):
    wide = dataclasses.replace(chain["forecast"], credible_interval_low=0.05, credible_interval_high=0.95)
    d = evaluate_veto(opportunity=chain["opportunity"], candidate=chain["candidate"], market_state=chain["market_state"],
                      regime_distribution=chain["regime"], forecast=wide, cost_estimate=chain["cost"],
                      policy=permissive_veto_policy(),
                      system_context=healthy_system_context(), event_context=clear_event_context())
    assert d.outcome == "REJECT" and "WIDE_CREDIBLE_INTERVAL" in d.reason_codes


def test_stale_book_watch(chain):
    ms = chain["market_state"]
    liq = LiquidityState(spread_bps=1.0, spread_percentile=0.2, top_book_depth=100.0, depth_imbalance=0.0,
                         estimated_slippage=0.5, stale_book=True, available=True)
    d = _veto(chain, market_state=dataclasses.replace(ms, liquidity_state=liq))
    assert d.outcome == "WATCH" and "STALE_BOOK" in d.reason_codes


def test_missing_depth_is_not_zero_depth(chain):
    ms = chain["market_state"]
    liq = LiquidityState(spread_bps=1.0, spread_percentile=0.2, top_book_depth=None, depth_imbalance=None,
                         estimated_slippage=0.5, stale_book=False, available=True)
    d = _veto(chain, market_state=dataclasses.replace(ms, liquidity_state=liq), policy=permissive_veto_policy(min_top_book_depth=50.0))
    assert _check(d, "depth").status == "NOT_EVALUATED"
    zero = dataclasses.replace(liq, top_book_depth=0.0)
    d2 = _veto(chain, market_state=dataclasses.replace(ms, liquidity_state=zero), policy=permissive_veto_policy(min_top_book_depth=50.0))
    assert _check(d2, "depth").status == "FAIL" and "DEPTH_INSUFFICIENT" in d2.reason_codes


def test_missing_book_is_not_evaluated_by_default_and_policy_driven(chain):
    assert _check(_veto(chain), "book_capability").status == "NOT_EVALUATED"
    d = _veto(chain, policy=permissive_veto_policy(missing_liquidity_status="REJECT"))
    assert d.outcome == "REJECT" and "LIQUIDITY_UNVERIFIED" in d.reason_codes


def _derivs(**kw):
    base = dict(funding_current=0.01, funding_predicted=None, funding_percentile=0.5, time_to_funding=None,
                open_interest=1.0, open_interest_delta=0.0, basis=None, mark_index_spread=None,
                crowding_state="NEUTRAL", available=True)
    base.update(kw)
    return DerivativesState(**base)


def test_funding_extreme_rejects(chain):
    ms = dataclasses.replace(chain["market_state"], derivatives_state=_derivs(funding_percentile=0.99))
    d = _veto(chain, market_state=ms)
    assert d.outcome == "REJECT" and "FUNDING_EXTREME" in d.reason_codes


def test_oi_crowding_watch(chain):
    ms = dataclasses.replace(chain["market_state"], derivatives_state=_derivs(crowding_state="CROWDED_LONG"))
    d = _veto(chain, market_state=ms)
    assert d.outcome == "WATCH" and "OI_CROWDING" in d.reason_codes


def test_missing_crowding_is_never_neutral(chain):
    assert _check(_veto(chain), "derivatives_capability").status == "NOT_EVALUATED"


def test_event_risk_window_rejects(chain):
    t = chain["candidate"].decision_time
    ev = MarketEvent("e1", "cal", "INFLATION", t + 3000, "HIGH", affected_currencies=("USD",),
                     pre_event_window_ms=2000, post_event_window_ms=2000)
    ctx = EventRiskContext(source_state="AVAILABLE", events=(ev,), maintenance=clear_event_context().maintenance)
    d = _veto(chain, event_context=ctx)
    assert d.outcome == "REJECT" and "EVENT_RISK_WINDOW" in d.reason_codes


def test_maintenance_rejects(chain):
    t = chain["candidate"].decision_time
    mw = MarketEvent("m1", "venue", "EXCHANGE_MAINTENANCE", t + 2500, "HIGH", affected_venues=("binance",),
                     pre_event_window_ms=2500, post_event_window_ms=2500)
    ctx = EventRiskContext(source_state="AVAILABLE", maintenance=MaintenanceContext(state="AVAILABLE", windows=(mw,)))
    d = _veto(chain, event_context=ctx)
    assert d.outcome == "REJECT" and "EXCHANGE_MAINTENANCE" in d.reason_codes


def test_event_feed_absent_never_fabricates_no_event_risk(chain):
    d = _veto(chain, event_context=EventRiskContext(source_state="UNAVAILABLE"))
    assert _check(d, "event_source").status == "WATCH" and d.outcome == "WATCH"
    assert "EVENT_SOURCE_UNAVAILABLE" in d.reason_codes
    d2 = _veto(chain, event_context=EventRiskContext(source_state="UNAVAILABLE"),
               policy=permissive_veto_policy(event_unavailable_status="NOT_EVALUATED"))
    assert _check(d2, "event_source").status == "NOT_EVALUATED"


def test_broker_unavailable_rejects(chain):
    d = _veto(chain, system_context=_broker("UNAVAILABLE"))
    assert d.outcome == "REJECT" and "BROKER_UNAVAILABLE" in d.reason_codes


def test_cati_component_error_rejects(chain):
    d = _veto(chain, system_context=_broker("HEALTHY", component_errors=("boom",)))
    assert d.outcome == "REJECT" and "CATI_COMPONENT_ERROR" in d.reason_codes
    e = component_error_decision(opportunity=chain["opportunity"], candidate=chain["candidate"], market_state=chain["market_state"], error="x")
    assert e.outcome == "REJECT"


def test_data_quality_fault_rejects(chain):
    bad = dataclasses.replace(chain["market_state"], data_quality=DataQuality(level=DataQualityLevel.INVALID, reason_codes=("PRIMARY_CANDLES_MISSING",)))
    d = _veto(chain, market_state=bad)
    assert d.outcome == "REJECT" and "DATA_QUALITY_FAULT" in d.reason_codes


def test_portfolio_family_not_evaluated_pre_ranking_and_fails_at_portfolio_stage(chain):
    pre = _veto(chain)
    assert _check(pre, "portfolio_context").status == "NOT_EVALUATED"
    assert pre.outcome == "APPROVE_FOR_RANKING"  # not rejected merely for missing portfolio context
    post = _veto(chain, stage=VetoStage.PORTFOLIO_STAGE.value, portfolio_findings=("DUPLICATE_EXPOSURE",))
    assert post.outcome == "REJECT" and "DUPLICATE_EXPOSURE" in post.reason_codes


def test_watch_and_reject_are_never_approved_for_ranking(chain):
    watch = _veto(chain, policy=VetoPolicy())
    reject = _veto(chain, system_context=_broker("UNAVAILABLE"))
    assert not watch.approved_for_ranking and not reject.approved_for_ranking


def test_deterministic_reason_order_and_id(chain):
    ms = chain["market_state"]
    hot = dataclasses.replace(ms, volatility_state=dataclasses.replace(ms.volatility_state, shock_state=True),
                              derivatives_state=_derivs(funding_percentile=0.99))
    a = _veto(chain, market_state=hot, system_context=_broker("UNAVAILABLE"))
    b = _veto(chain, market_state=hot, system_context=_broker("UNAVAILABLE"))
    assert a.reason_codes == b.reason_codes and a.veto_decision_id == b.veto_decision_id
    families = [c.family for c in a.checks]
    from app.trading_intelligence.contracts.veto import FAMILY_ORDER
    assert families == sorted(families, key=FAMILY_ORDER.index)
    assert a.reason_codes.index("SHOCK_STATE") < a.reason_codes.index("FUNDING_EXTREME") < a.reason_codes.index("BROKER_UNAVAILABLE")


def test_policy_hash_changes_with_policy(chain):
    assert VetoPolicy().policy_hash != VetoPolicy(ood_score_reject=0.6).policy_hash
    assert _veto(chain).veto_decision_id != _veto(chain, policy=permissive_veto_policy(ood_score_reject=0.9)).veto_decision_id


def test_family_override_applies(chain):
    pol = permissive_veto_policy(family_overrides={"TREND_PULLBACK_V2": {"missing_liquidity_status": "REJECT"}})
    assert _veto(chain, policy=pol).outcome == "REJECT"


def test_no_execution_capital_slot_or_v2_dependency():
    import app.trading_intelligence.veto.engine as engine
    import app.trading_intelligence.contracts.veto as contracts

    src = (inspect.getsource(engine) + inspect.getsource(contracts)).lower()
    for term in ("adaptiveentrythreshold", "master_ensemble", "position_slots", "capital_ledger",
                 "place_order", "reserve_entry_slot", "account_reservations", "app.execution", "app.exchange"):
        assert term not in src, term
    for name in inspect.signature(evaluate_veto).parameters:
        assert not any(x in name for x in ("client", "executor", "capital", "slot"))


def test_tenant_identity_only_when_supplied(chain):
    assert _veto(chain).broker_account_id is None
    d = _veto(chain, user_id="u", broker_account_id="a", bot_instance_id="b", run_id="r", cycle_id="c")
    assert (d.user_id, d.broker_account_id, d.bot_instance_id) == ("u", "a", "b")
    assert d.veto_decision_id == _veto(chain).veto_decision_id  # tenant ids never move analytical identity


def test_unknown_policy_string_fails_closed(chain):
    d = _veto(chain, policy=permissive_veto_policy(missing_liquidity_status="SOMETHING_ELSE"))
    assert d.outcome == "REJECT"
