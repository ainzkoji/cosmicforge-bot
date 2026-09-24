"""Section 18 -- TradePlan and invalidation: the full 18.21 matrix."""
from __future__ import annotations

import ast
import dataclasses
import json
import sqlite3
from pathlib import Path

import pytest
from _helpers import clear_event_context, healthy_system_context
from _plan import build_kwargs, fresh_db, replace_ev, run_pipeline, venue_evaluated
from _venue import FIXTURE_REGISTRY, FX_OPEN_MS, fut_raw, fut_request, fx_raw, fx_request

from app.trading_intelligence.contracts.system_health import BrokerHealthContext
from app.trading_intelligence.contracts.trade_plan import (
    SubmissionValidity as V, TradePlanBuildStatus as S, TradePlanPolicy,
)
from app.trading_intelligence.contracts.veto import VetoPolicy
from app.trading_intelligence.trade_plan.builder import TenantContext, TradePlanBuilder
from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore, trade_plan_payload
from app.trading_intelligence.trade_plan.validation import MarketReference, validate_trade_plan_for_submission

BACKEND = Path(__file__).resolve().parents[2]


@pytest.fixture
def db(tmp_path):
    return fresh_db(tmp_path)


@pytest.fixture
def pipe(db):
    return run_pipeline(db, [venue_evaluated("BTCUSDT", seed=999), venue_evaluated("ETHUSDT", seed=998)])


def build(pipe, builder=None, **overrides):
    kw = build_kwargs(pipe)
    kw.update(overrides)
    return (builder or TradePlanBuilder()).build_from_evaluated(**kw)


def plan_of(pipe):
    res = build(pipe)
    assert res.status == S.PLAN_CREATED.value, (res.status, res.reason_codes, res.detail)
    return res.plan


# ============================== CONTRACT ==============================
def test_plan_created_with_complete_lineage(pipe):
    plan = plan_of(pipe)
    lineage = plan.lineage
    assert list(lineage) == ["snapshot", "market_state", "regime_distribution", "setup_candidate", "outcome_forecast",
                             "venue_economic_observation", "cost_estimate", "economic_opportunity", "veto_decision",
                             "ranking_batch", "ranked_opportunity", "portfolio_decision", "portfolio_reservation",
                             "trade_plan"]
    assert all(lineage.values())
    ev = build_kwargs(pipe)["evaluated"]
    assert plan.source_candidate_id == ev.candidate.setup_candidate_id
    assert plan.venue_observation_id == ev.venue_observation.observation_id
    assert plan.portfolio_reservation_id == pipe["outcome"].reservation.reservation_id
    assert plan.portfolio_decision_id == pipe["outcome"].decision.portfolio_selection_id
    assert plan.ranking_batch_id == pipe["result"].batch.cycle_batch_id
    assert (plan.user_id, plan.broker_account_id, plan.bot_instance_id, plan.run_id, plan.cycle_id) == (
        "u1", "acct1", "botA", "r1", "c1")
    assert plan.venue == "BINANCE_USDM" and plan.environment == "DEMO" and plan.mode == "SHADOW"
    assert plan.expected_net_R == ev.opportunity.ev_net_r and plan.expected_costs.total_cost_R == ev.cost_estimate.total_cost_R


def test_immutable(pipe):
    plan = plan_of(pipe)
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.entry_reference = 1.0
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.allowed_entry_zone.maximum_price = 1.0


def test_deterministic_hash_and_id(pipe):
    a, b = plan_of(pipe), plan_of(pipe)
    assert a.trade_plan_hash == b.trade_plan_hash and a.trade_plan_id == b.trade_plan_id
    later = build(pipe, now_ms=pipe["now_ms"] + 1000).plan
    assert later.trade_plan_hash == a.trade_plan_hash  # operational created_at is not analytical content
    assert later.plan_created_at != a.plan_created_at


def test_no_secrets_and_versioned_policy(pipe):
    plan = plan_of(pipe)
    blob = json.dumps(trade_plan_payload(plan)).lower()
    for word in ("api_key", "secret", "signature", "authorization"):
        assert word not in blob
    versions = dict(plan.versions)
    assert versions["trade_plan_policy_hash"] == TradePlanPolicy().policy_hash
    assert TradePlanPolicy(max_plan_age_bars=4).policy_hash != TradePlanPolicy().policy_hash
    assert {"veto_policy_hash", "admission_policy_hash", "cost_policy_hash", "ranking_policy_hash",
            "portfolio_policy_hash", "setup_policy_hash", "adapter", "forecast"} <= set(versions)


# ============================== VETO ==============================
def test_watch_and_reject_never_plan(pipe):
    kw = build_kwargs(pipe)
    for outcome in ("WATCH", "REJECT"):
        ev = replace_ev(kw["evaluated"], veto=dataclasses.replace(kw["evaluated"].veto, outcome=outcome))
        res = TradePlanBuilder().build_from_evaluated(**dict(kw, evaluated=ev))
        assert res.status == S.VETO_NOT_APPROVED.value and res.plan is None


def test_real_watch_conditions_remain_watch_and_produce_zero_plans(db):
    """The production veto policy (calibration must be CALIBRATED) keeps an
    uncalibrated library's opportunity at WATCH -- which never ranks or plans."""
    ev = venue_evaluated("BTCUSDT", veto_policy=VetoPolicy())
    assert ev.veto.outcome == "WATCH" and "UNCALIBRATED_PROBABILITY" in ev.veto.reason_codes
    p = run_pipeline(db, [ev])
    assert p["result"].ranked == () and p["outcome"].decision.selected_opportunity_ids == ()


def test_stale_calendar_is_watch(db):
    from app.trading_intelligence.contracts.events import EventRiskContext

    stale = EventRiskContext(source_state="STALE", events=(), as_of=0, source="economic_events")
    ev = venue_evaluated("BTCUSDT", event_context=stale)
    assert ev.veto.outcome == "WATCH"


def test_inadmissible_economics_never_plan(pipe):
    kw = build_kwargs(pipe)
    opp = dataclasses.replace(kw["evaluated"].opportunity, admission_status="INSUFFICIENT_EVIDENCE")
    res = TradePlanBuilder().build_from_evaluated(**dict(kw, evaluated=replace_ev(kw["evaluated"], opportunity=opp)))
    assert res.status == S.VETO_NOT_APPROVED.value


# ============================== RANKING ==============================
def test_incomplete_batch_cannot_plan(pipe):
    kw = build_kwargs(pipe)
    batch = dataclasses.replace(kw["ranking_batch"], batch_complete=False)
    assert build(pipe, ranking_batch=batch).status == S.RANKING_INCOMPLETE.value


def test_candidate_absent_from_completed_batch_cannot_plan(pipe):
    batch = dataclasses.replace(build_kwargs(pipe)["ranking_batch"], approved_opportunity_ids=())
    res = build(pipe, ranking_batch=batch)
    assert res.status == S.RANKING_INCOMPLETE.value and "NOT_IN_COMPLETED_BATCH" in res.reason_codes


def test_incomplete_universe_never_ranks(db):
    p = run_pipeline(db, [venue_evaluated("BTCUSDT")], incomplete_symbol="SOLUSDT")
    assert not p["result"].batch.batch_complete and p["result"].ranked == ()


# ============================== PORTFOLIO ==============================
def test_unselected_candidate_cannot_plan(pipe):
    d = dataclasses.replace(pipe["outcome"].decision, selected_opportunity_ids=())
    assert build(pipe, portfolio_decision=d).status == S.PORTFOLIO_NOT_SELECTED.value


def test_only_selected_candidates_plan_when_slots_limit(db):
    p = run_pipeline(db, [venue_evaluated("BTCUSDT", seed=999), venue_evaluated("ETHUSDT", seed=998)], max_open_positions=1)
    d = p["outcome"].decision
    assert len(d.selected_opportunity_ids) == 1
    unselected = next(r for r in p["result"].ranked if r.ranked_opportunity_id not in d.selected_opportunity_ids)
    res = TradePlanBuilder().build_from_evaluated(**build_kwargs(p, rid=unselected.ranked_opportunity_id))
    assert res.status == S.PORTFOLIO_NOT_SELECTED.value
    assert TradePlanBuilder().build_from_evaluated(**build_kwargs(p)).created


@pytest.mark.parametrize("change,code", [
    (dict(broker_account_id="acct2"), "RESERVATION_WRONG_ACCOUNT"),
    (dict(selected_candidate_ids=("someone_else",)), "RESERVATION_WRONG_CANDIDATE"),
    (dict(status="RELEASED"), "RESERVATION_RELEASED"),
    (dict(status="CONSUMED"), "RESERVATION_CONSUMED"),
    (dict(bot_instance_id="botB"), "RESERVATION_WRONG_BOT_OR_CYCLE"),
])
def test_invalid_reservations_rejected(pipe, change, code):
    r = dataclasses.replace(pipe["outcome"].reservation, **change)
    res = build(pipe, reservation=r)
    assert res.status == S.RESERVATION_INVALID.value and code in res.reason_codes


def test_expired_and_missing_reservation_rejected(pipe):
    r = pipe["outcome"].reservation
    res = build(pipe, now_ms=r.expires_at)  # the reservation lapsed before the plan was built
    assert res.status == S.RESERVATION_INVALID.value and "RESERVATION_EXPIRED" in res.reason_codes
    assert build(pipe, reservation=None).status == S.RESERVATION_INVALID.value


def test_store_released_reservation_is_rejected(pipe):
    """The real store lifecycle: once released, the persisted state no longer plans."""
    svc, r = pipe["service"], pipe["outcome"].reservation
    assert svc.release(r.reservation_id, pipe["now_ms"])
    res = build(pipe, reservation=svc.store.get(r.reservation_id))
    assert res.status == S.RESERVATION_INVALID.value and "RESERVATION_RELEASED" in res.reason_codes


def test_reservation_stays_reserved_through_plan_creation(pipe):
    plan_of(pipe)
    stored = pipe["service"].store.get(pipe["outcome"].reservation.reservation_id)
    assert stored.status == "RESERVED"


def test_tenant_mismatch_rejected(pipe):
    wrong = TenantContext(broker_account_id="acct2", bot_instance_id="botA", cycle_id="c1")
    assert build(pipe, tenant=wrong).status == S.INVALID_INPUT.value


# ============================== ENTRY ==============================
def test_long_allowed_entry_zone(pipe):
    plan = plan_of(pipe)
    z = plan.allowed_entry_zone
    assert plan.side == "LONG" and z.minimum_price < z.reference_price < z.maximum_price
    assert z.minimum_price > plan.structural_invalidation_price
    assert z.maximum_extension_R <= TradePlanPolicy().max_entry_extension_R + 1e-9
    assert z.maximum_adverse_slippage_bps <= TradePlanPolicy().default_max_slippage_bps + 1e-6


def test_short_allowed_entry_zone(db):
    p = run_pipeline(db, [venue_evaluated("BTCUSDT", side="SHORT")])
    plan = TradePlanBuilder().build_from_evaluated(**build_kwargs(p)).plan
    z = plan.allowed_entry_zone
    assert plan.side == "SHORT" and z.minimum_price < z.reference_price < z.maximum_price
    assert z.maximum_price < plan.structural_invalidation_price
    assert all(t.price_high < plan.entry_reference for t in plan.target_zones)


def test_plan_expiry_and_expired_build(pipe):
    plan = plan_of(pipe)
    ev = build_kwargs(pipe)["evaluated"]
    assert plan.plan_expiry_time <= ev.candidate.valid_until
    assert plan.plan_expiry_time <= ev.venue_observation.valid_until
    assert plan.plan_expiry_time <= pipe["outcome"].reservation.expires_at
    late = ev.candidate.valid_until + 1
    res = build(pipe, now_ms=late, reservation=dataclasses.replace(pipe["outcome"].reservation, expires_at=late + 10**7))
    assert res.status == S.EXPIRED.value


def test_stale_economics_rejected(pipe):
    ev = build_kwargs(pipe)["evaluated"]
    obs = dataclasses.replace(ev.venue_observation, valid_until=pipe["now_ms"] - 1)
    small = TradePlanBuilder(TradePlanPolicy(max_plan_age_bars=100))
    res = build(pipe, builder=small, evaluated=replace_ev(ev, venue_observation=obs))
    assert res.status in (S.EXPIRED.value, S.ECONOMICS_STALE.value)
    closed = dataclasses.replace(ev.venue_observation, reason_codes=ev.venue_observation.reason_codes + ("MARKET_CLOSED",))
    res = build(pipe, evaluated=replace_ev(ev, venue_observation=closed))
    assert res.status == S.ECONOMICS_STALE.value


def test_missing_venue_evidence_is_invalid_input(pipe):
    ev = build_kwargs(pipe)["evaluated"]
    res = build(pipe, evaluated=replace_ev(ev, venue_observation=None))
    assert res.status == S.INVALID_INPUT.value and "VENUE_OBSERVATION_MISSING" in res.reason_codes


# ============================== INVALIDATION ==============================
def test_structural_invalidation_used_not_fixed_percentage(pipe):
    plan = plan_of(pipe)
    ev = build_kwargs(pipe)["evaluated"]
    assert plan.structural_invalidation_price == ev.candidate.structural_invalidation
    assert plan.initial_risk_distance == ev.candidate.initial_structural_risk > 0
    codes = {c.code for c in plan.invalidation_conditions}
    assert {"STRUCTURAL_LEVEL_BROKEN", "PLAN_EXPIRED", "ENTRY_ZONE_EXPIRED", "REGIME_SHIFTED_TO_SHOCK",
            "EVENT_VETO_BECAME_ACTIVE", "BROKER_HEALTH_DEGRADED", "SPREAD_EXCEEDED_BUDGET",
            "ECONOMIC_EDGE_INVALIDATED", "PORTFOLIO_RESERVATION_LOST"} == codes


def test_malformed_invalidation_rejected(pipe):
    ev = build_kwargs(pipe)["evaluated"]
    bad = dataclasses.replace(ev.candidate, structural_invalidation=ev.candidate.trigger_reference + 1)
    res = build(pipe, evaluated=replace_ev(ev, candidate=bad))
    assert res.status == S.INVALID_INPUT.value and "MALFORMED_INVALIDATION" in res.reason_codes


def test_plan_does_not_mutate_when_market_changes(pipe):
    plan = plan_of(pipe)
    before = trade_plan_payload(plan)
    validate_trade_plan_for_submission(plan, pipe["now_ms"], MarketReference(plan.entry_reference * 1.2, pipe["now_ms"]),
                                       healthy_system_context("acct1").broker_health, pipe["outcome"].reservation,
                                       build_kwargs(pipe)["evaluated"].venue_observation.execution_capabilities)
    assert trade_plan_payload(plan) == before


def test_thesis_conditions_are_typed(pipe):
    codes = {c.code for c in plan_of(pipe).thesis_conditions}
    assert {"TREND_CONTINUATION_REMAINS_VALID", "HTF_ALIGNMENT_VALID", "EVENT_CONTEXT_ACCEPTABLE",
            "LIQUIDITY_NOT_DEGRADED", "BROKER_HEALTH_ACCEPTABLE", "ECONOMIC_EDGE_POSITIVE"} == codes


# ============================== TARGETS ==============================
def test_one_and_multiple_targets(pipe):
    multi = plan_of(pipe)
    assert len(multi.target_zones) == 2 and {t.purpose for t in multi.target_zones} == {"STRUCTURAL", "FORECAST_QUANTILE"}
    one = build(pipe, builder=TradePlanBuilder(TradePlanPolicy(include_forecast_quantile_target=False))).plan
    assert len(one.target_zones) == 1 and one.target_zones[0].purpose == "STRUCTURAL"
    for t in multi.target_zones:  # direction-coherent for LONG
        assert multi.entry_reference < t.price_low <= t.price_high and t.reference_R > 0


def test_invalid_target_rejected(pipe):
    ev = build_kwargs(pipe)["evaluated"]
    wrong = dataclasses.replace(ev.candidate, target_reference=ev.candidate.trigger_reference - 3)
    res = build(pipe, evaluated=replace_ev(ev, candidate=wrong))
    assert res.status == S.INVALID_INPUT.value and "TARGET_DIRECTION_INCOHERENT" in res.reason_codes


# ============================== EXECUTION PREFERENCES / VALIDATOR ==============================
def _validate(pipe, plan, *, t=None, price=None, age=0, health="HEALTHY", reservation="keep", caps="keep", spread=None):
    t = t if t is not None else pipe["now_ms"]
    ref = MarketReference(plan.entry_reference if price is None else price, t - age, spread) if price != "none" else None
    h = BrokerHealthContext("acct1", "BINANCE", "DEMO", health, t, "test") if health else None
    r = pipe["outcome"].reservation if reservation == "keep" else reservation
    c = build_kwargs(pipe)["evaluated"].venue_observation.execution_capabilities if caps == "keep" else caps
    return validate_trade_plan_for_submission(plan, t, ref, h, r, c)


def test_supported_preference_validates(pipe):
    plan = plan_of(pipe)
    assert plan.execution_preferences.preferred_order_style == "AGGRESSIVE_LIMIT"
    assert plan.execution_preferences.time_in_force == "GTD"
    assert _validate(pipe, plan).status == V.VALID.value


def test_unsupported_preference_rejected_by_validator(pipe):
    plan = plan_of(pipe)
    caps = build_kwargs(pipe)["evaluated"].venue_observation.execution_capabilities
    no_limit = dataclasses.replace(caps, supports_limit=False, supported_order_types=("MARKET",))
    assert _validate(pipe, plan, caps=no_limit).status == V.UNSUPPORTED_EXECUTION_PREFERENCE.value
    no_tif = dataclasses.replace(caps, supported_time_in_force=("GTC",))
    assert _validate(pipe, plan, caps=no_tif).status == V.UNSUPPORTED_EXECUTION_PREFERENCE.value


def test_builder_only_picks_venue_declared_preferences(pipe):
    ev = build_kwargs(pipe)["evaluated"]
    caps = dataclasses.replace(ev.venue_observation.execution_capabilities, supports_limit=False, supports_market=False)
    obs = dataclasses.replace(ev.venue_observation, execution_capabilities=caps)
    res = build(pipe, evaluated=replace_ev(ev, venue_observation=obs))
    assert res.status == S.PLAN_NOT_CREATED.value and "NO_SUPPORTED_EXECUTION_PREFERENCE" in res.reason_codes


@pytest.mark.parametrize("kwargs,status", [
    (dict(price=None, age=60_000), V.STALE),
    (dict(price="none"), V.STALE),
    (dict(t="expired"), V.EXPIRED),
    (dict(health="DEGRADED"), V.BROKER_DEGRADED),
    (dict(health=None), V.BROKER_DEGRADED),
    (dict(reservation=None), V.RESERVATION_LOST),
    (dict(price="extended"), V.ENTRY_ZONE_VIOLATION),
    (dict(price="broken"), V.ENTRY_ZONE_VIOLATION),
    (dict(spread=50.0), V.ENTRY_ZONE_VIOLATION),
    (dict(caps=None), V.INVALID_INSTRUMENT_METADATA),
])
def test_validator_statuses(pipe, kwargs, status):
    plan = plan_of(pipe)
    kw = dict(kwargs)
    if kw.get("t") == "expired":
        kw["t"] = plan.plan_expiry_time
    if kw.get("price") == "extended":
        kw["price"] = plan.allowed_entry_zone.maximum_price * 1.01
    if kw.get("price") == "broken":
        kw["price"] = plan.structural_invalidation_price - 0.5
    assert _validate(pipe, plan, **kw).status == status.value


def test_validator_sees_released_reservation(pipe):
    plan = plan_of(pipe)
    released = dataclasses.replace(pipe["outcome"].reservation, status="RELEASED")
    assert _validate(pipe, plan, reservation=released).status == V.RESERVATION_LOST.value


# ============================== CAPITAL / NO EXECUTION ==============================
def test_no_final_quantity_leverage_or_margin(pipe):
    plan = plan_of(pipe)
    names = {f.name for f in dataclasses.fields(plan)}
    for forbidden in ("quantity", "qty", "leverage", "margin", "risk_amount", "order_id", "position_size"):
        assert not any(forbidden in n for n in names), forbidden
    assert plan.economic_size_assumption == build_kwargs(pipe)["evaluated"].cost_estimate.reference_notional
    assert plan.economic_size_assumption != 120  # allocation is not a broker quantity


def test_trade_plan_modules_never_touch_execution_risk_or_capital():
    forbidden = ("app.execution", "app.risk", "place_order", "submit_order", "capital_ledger", "position_slots",
                 "AccountMarginReservations", "Master", "ensemble")
    for path in (BACKEND / "app" / "trading_intelligence" / "trade_plan").glob("*.py"):
        src = path.read_text(encoding="utf-8")
        tree = ast.parse(src)
        imports = [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
        imports += [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names]
        for f in forbidden:
            assert not any(f in m for m in imports), (path.name, f)
            assert f not in {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}, (path.name, f)


def test_no_capital_slot_or_margin_mutation(db, pipe):
    with db.connect() as c:
        before = {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("positions", "pending_entries")}
    plan_of(pipe)
    with db.connect() as c:
        after = {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("positions", "pending_entries")}
    assert before == after


# ============================== VENUE (multi-asset) ==============================
def _other_asset_evaluated(kind):
    """Forex / dated-future opportunities through the SAME builder (fixture-validated adapters)."""
    from _helpers import candidate_for, market_state_and_regime, flat_rows
    from _plan import library
    from app.trading_intelligence.economics.engine import evaluate_economic_opportunity
    from app.trading_intelligence.forecast.engine import build_outcome_forecast
    from app.trading_intelligence.contracts.ranking import EvaluatedOpportunity
    from app.trading_intelligence.venue.reference_adapters import DatedFuturesEconomicAdapter, ForexEconomicAdapter
    from app.trading_intelligence.venue.cost_model import build_venue_cost_estimate
    from app.trading_intelligence.veto.engine import evaluate_veto
    from _helpers import permissive_veto_policy

    if kind == "fx":
        adapter, req = ForexEconomicAdapter(status_registry=FIXTURE_REGISTRY), fx_request()
        key = req.instrument_key
    else:
        adapter, req = DatedFuturesEconomicAdapter(status_registry=FIXTURE_REGISTRY), fut_request()
        key = req.instrument_key
    # the hypothesis is formed at the FX-open instant the venue data is observed at
    ms, regime = market_state_and_regime(flat_rows(999, start=FX_OPEN_MS - 60 * 900_000), key)
    cand = candidate_for(ms)
    t = cand.decision_time + 1_000
    raw = fx_raw(t=t, quote={"bid": 99.999, "ask": 100.001, "time": t - 100},
                 commission={"model": "SPREAD_PLUS_COMMISSION", "per_lot": 3.0, "lot_units": 100000, "currency": "USD"}) \
        if kind == "fx" else fut_raw(t=t, fut_px=100.0, spot=99.9)
    if kind == "fx":
        raw = dataclasses.replace(raw, payloads=dict(raw.payloads, instrument=dict(raw.payloads["instrument"],
                                                                                   tick_size=0.001)))
    else:
        raw = dataclasses.replace(raw, payloads=dict(raw.payloads, contract=dict(raw.payloads["contract"],
                                                                                 tick_size=0.01)))
    req = dataclasses.replace(req, decision_time=t, user_id="u1", broker_account_id="acct1", bot_instance_id="botA",
                              run_id="r1", cycle_id="c1")
    obs = adapter.observe(req, raw)
    forecast = build_outcome_forecast(cand, ms, regime, library())
    cost = build_venue_cost_estimate(cand, obs, forecast=forecast, policy=adapter.policy)
    opp = evaluate_economic_opportunity(cand, ms, forecast, cost, user_id="u1", broker_account_id="acct1",
                                        bot_instance_id="botA", run_id="r1", cycle_id="c1")
    veto = evaluate_veto(opportunity=opp, candidate=cand, market_state=ms, regime_distribution=regime, forecast=forecast,
                         cost_estimate=cost, policy=permissive_veto_policy(), event_context=clear_event_context(),
                         system_context=healthy_system_context("acct1"), user_id="u1", broker_account_id="acct1",
                         bot_instance_id="botA", run_id="r1", cycle_id="c1")
    return EvaluatedOpportunity(cand, ms, regime, forecast, cost, opp, veto, venue_observation=obs), t


@pytest.mark.parametrize("kind", ["fx", "futures"])
def test_forex_and_dated_future_plans_through_the_same_builder(db, kind):
    ev, t = _other_asset_evaluated(kind)
    assert ev.veto.outcome == "APPROVE_FOR_RANKING", (ev.veto.reason_codes, ev.cost_estimate.reason_codes)
    p = run_pipeline(db, [ev], now_ms=t + 60_000)
    res = TradePlanBuilder().build_from_evaluated(**build_kwargs(p))
    assert res.created, (res.status, res.reason_codes)
    plan = res.plan
    assert plan.instrument_key.asset_class == ("FX" if kind == "fx" else "FUTURES")
    assert "USDT" not in plan.instrument_key.canonical_symbol and plan.expected_costs.funding_R == 0.0
    assert plan.execution_preferences.time_in_force in (("GTD", "GTC") if kind == "fx" else ("DAY", "GTC"))


def test_binance_demo_metadata_accepted(pipe):
    plan = plan_of(pipe)
    assert plan.venue == "BINANCE_USDM" and plan.expected_costs.adapter_status == "DEMO_VALIDATED"


# ============================== DETERMINISM ==============================
def test_changed_inputs_make_new_plans(db, tmp_path):
    base = plan_of(run_pipeline(db, [venue_evaluated("BTCUSDT")]))
    db2 = fresh_db(tmp_path, "b.db")
    econ = plan_of(run_pipeline(db2, [venue_evaluated("BTCUSDT", raw_kwargs={
        "commission": {"makerCommissionRate": "0.0001", "takerCommissionRate": "0.0002"}})]))
    assert econ.trade_plan_hash != base.trade_plan_hash
    db3 = fresh_db(tmp_path, "c.db")
    p3 = run_pipeline(db3, [venue_evaluated("BTCUSDT")])
    zone = build(p3, builder=TradePlanBuilder(TradePlanPolicy(max_entry_extension_R=0.1))).plan
    assert zone.trade_plan_hash != base.trade_plan_hash
    db4 = fresh_db(tmp_path, "d.db")
    sel = plan_of(run_pipeline(db4, [venue_evaluated("BTCUSDT"), venue_evaluated("ETHUSDT", seed=998)]))
    assert sel.trade_plan_hash != base.trade_plan_hash  # different portfolio selection / reservation lineage
    db5 = fresh_db(tmp_path, "e.db")
    same = plan_of(run_pipeline(db5, [venue_evaluated("BTCUSDT")]))
    assert same.trade_plan_hash == base.trade_plan_hash  # identical evidence -> identical plan


# ============================== PERSISTENCE ==============================
def test_evidence_append_only_and_idempotent(db, pipe):
    store = TradePlanEvidenceStore(db)
    plan = plan_of(pipe)
    assert store.append(plan) is True and store.append(plan) is False
    row = store.get(plan.trade_plan_id)
    assert row["trade_plan_hash"] == plan.trade_plan_hash and row["reservation_id"] == plan.portfolio_reservation_id
    assert row["mode"] == "SHADOW" and "secret" not in row["payload"].lower()
    with db.connect() as c:
        with pytest.raises(sqlite3.DatabaseError):
            c.execute("UPDATE cati_trade_plans SET side='SHORT' WHERE trade_plan_id=?", (plan.trade_plan_id,))
    with db.connect() as c:
        with pytest.raises(sqlite3.DatabaseError):
            c.execute("DELETE FROM cati_trade_plans WHERE trade_plan_id=?", (plan.trade_plan_id,))
    assert store.for_account("acct1")[0]["trade_plan_id"] == plan.trade_plan_id and store.for_account("acct2") == []


def test_evidence_schema_is_canonical_and_idempotent(tmp_path):
    from shared_lib.persistence.cati_schema import ensure_cati_schema
    from app.trading_intelligence.trade_plan.evidence_store import TradePlanSchemaMissing

    db = fresh_db(tmp_path, "idem.db")
    ensure_cati_schema(db)
    ensure_cati_schema(db)  # re-running the migration is a no-op
    TradePlanEvidenceStore(db)

    class _Bare:
        def __init__(self, path):
            self.path = path

        def connect(self):
            conn = sqlite3.connect(self.path)
            conn.row_factory = sqlite3.Row
            return conn

    with pytest.raises(TradePlanSchemaMissing):
        TradePlanEvidenceStore(_Bare(str(tmp_path / "bare.db")))
    src = (BACKEND / "app" / "trading_intelligence" / "trade_plan" / "evidence_store.py").read_text()
    assert "CREATE TABLE" not in src.upper()  # no runtime DDL
