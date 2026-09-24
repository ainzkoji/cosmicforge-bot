"""Section 19 -- Position Intelligence & Adaptive Exit: the 19.21 matrix.

Every plan here is a REAL Section 18 TradePlan (full 12-18 pipeline); every
cost is the Section 17 model; decisions are advisory evidence only."""
from __future__ import annotations

import ast
import dataclasses
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
from _position import (
    BAR, bars, clean_events, dims_for, good_state, healthy, losers, plan_setup, policy, position, position_library,
    regime_with, winners,
)

from app.trading_intelligence.contracts.events import EventRiskContext, MaintenanceContext, MarketEvent
from app.trading_intelligence.contracts.market_state import HigherTimeframeState
from app.trading_intelligence.contracts.position import (
    REMAINING_R_BASIS, ExitAction as A, ExitDecision, ExitPolicy, PositionForecastStatus,
    PositionReasonCode as PR, ThesisStatus, protection_is_tighter,
)
from app.trading_intelligence.contracts.trade_plan import ThesisCode
from app.trading_intelligence.integration.errors import clear_component_errors, recent_component_errors
from app.trading_intelligence.position import service as svc_mod
from app.trading_intelligence.position.exit_engine import suggest_tighter_stop, validate_suggested_protection
from app.trading_intelligence.position.path import FillEvent, FillKind, build_position_path_snapshot
from app.trading_intelligence.position.service import LEGACY, PositionIntelligenceService
from app.trading_intelligence.position.thesis import EVALUATORS

BACKEND = Path(__file__).resolve().parents[2]
POSITION_PKG = BACKEND / "app" / "trading_intelligence" / "position"


@pytest.fixture
def env(tmp_path):
    db, pipe, kw, plan = plan_setup(tmp_path)
    ev = kw["evaluated"]
    ms = good_state(ev.market_state)
    regime = regime_with(ev.regime)
    dims = dims_for(plan, ms, regime)
    return SimpleNamespace(db=db, plan=plan, ev=ev, ms=ms, regime=regime, dims=dims, obs=ev.venue_observation,
                           lib=position_library(dims, winners(20)))


def run(env, *, closes=(101.0, 101.5, 102.0), price=None, lib="default", ms=None, regime=None, events="clean",
        system="healthy", pos=None, pol=None, db=True, obs="default", t_extra=1_000, mode="SHADOW"):
    plan = env.plan
    t = plan.decision_time + len(closes) * BAR + t_extra
    service = PositionIntelligenceService(library=env.lib if lib == "default" else lib, policy=pol or policy(),
                                          db=env.db if db else None, evaluation_mode=mode)
    return service.evaluate(
        plan=plan, position=pos or position(plan), candle_rows=bars(plan.decision_time, closes), current_time=t,
        current_price=price if price is not None else closes[-1] if closes else plan.entry_reference,
        market_state=ms or env.ms, regime=regime or env.regime,
        venue_observation=env.obs if obs == "default" else obs,
        event_context=clean_events(t) if events == "clean" else events,
        system_context=healthy(plan, t) if system == "healthy" else system)


def results(out):
    return {r.code: r.result for r in out.forecast.thesis_condition_results}


# ============================== POSITION PATH ==============================
def test_long_mfe_mae(env):
    plan = env.plan
    rows = bars(plan.decision_time, [102.0, 97.0, 104.0], spread=0.5)
    path = build_position_path_snapshot(plan=plan, position=position(plan), candle_rows=rows,
                                        current_time=plan.decision_time + 3 * BAR, current_price=103.0)
    assert path.mfe_price == 104.5 and path.mae_price == 96.5
    assert path.mfe_R == pytest.approx(4.5 / 5) and path.mae_R == pytest.approx(3.5 / 5)
    assert path.current_R == pytest.approx(0.6) and path.elapsed_bars == 3
    assert path.remaining_risk_distance == pytest.approx(103.0 - 95.0)
    assert path.original_R_reference == plan.initial_risk_distance == 5.0


def test_short_mfe_mae(env):
    plan = dataclasses.replace(env.plan, side="SHORT", structural_invalidation_price=105.0)
    pos = dataclasses.replace(position(env.plan), side="SHORT", current_stop_price=105.0)
    rows = bars(plan.decision_time, [98.0, 103.0, 96.0], spread=0.5)
    path = build_position_path_snapshot(plan=plan, position=pos, candle_rows=rows,
                                        current_time=plan.decision_time + 3 * BAR, current_price=97.0)
    assert path.mfe_price == 95.5 and path.mae_price == 103.5
    assert path.mfe_R == pytest.approx(4.5 / 5) and path.mae_R == pytest.approx(3.5 / 5)
    assert path.current_R == pytest.approx(0.6) and path.remaining_risk_distance == pytest.approx(8.0)


def test_partial_history_from_broker_fills(env):
    plan = env.plan
    t0 = plan.decision_time
    fills = (FillEvent(t0, FillKind.OPEN, 2.0, 100.0, fee=0.08), FillEvent(t0 + BAR, FillKind.REDUCE, 1.0, 104.0, fee=0.04),
             FillEvent(t0 + 2 * BAR, FillKind.FUNDING, amount=0.03))
    pos = dataclasses.replace(position(plan, qty=2.0), fills=fills)
    path = build_position_path_snapshot(plan=plan, position=pos, candle_rows=bars(t0, [103, 104, 103]),
                                        current_time=t0 + 3 * BAR, current_price=103.0)
    assert (path.original_quantity, path.realized_quantity, path.current_quantity, path.partial_exit_count) == (2.0, 1.0, 1.0, 1)
    assert path.realized_pnl == pytest.approx(4.0 - 0.04)
    assert path.entry_fees_paid == pytest.approx(0.08) and path.funding_paid_or_accrued == pytest.approx(0.03)
    assert path.last_fill_time == t0 + BAR and path.unrealized_pnl == pytest.approx(3.0)


def test_elapsed_bars_exclude_pre_entry_bar(env):
    plan = env.plan
    rows = bars(plan.decision_time - BAR, [90.0, 101.0, 102.0])  # first bar opened BEFORE entry
    path = build_position_path_snapshot(plan=plan, position=position(plan), candle_rows=rows,
                                        current_time=plan.decision_time + 2 * BAR, current_price=102.0)
    assert path.elapsed_bars == 2 and path.mae_price >= 99.0  # the pre-entry 90 low never counts


def test_no_future_data_can_change_the_snapshot(env):
    plan = env.plan
    t = plan.decision_time + 3 * BAR
    base = bars(plan.decision_time, [101, 102, 103])
    future = bars(plan.decision_time + 3 * BAR, [80, 130])
    fills = (FillEvent(plan.decision_time, FillKind.OPEN, 1.0, 100.0),)
    future_fill = FillEvent(t + BAR, FillKind.REDUCE, 0.5, 120.0)
    a = build_position_path_snapshot(plan=plan, position=dataclasses.replace(position(plan), fills=fills),
                                     candle_rows=base, current_time=t, current_price=103.0)
    b = build_position_path_snapshot(plan=plan, position=dataclasses.replace(position(plan), fills=fills + (future_fill,)),
                                     candle_rows=base + future, current_time=t, current_price=103.0)
    assert a.data_hash == b.data_hash and a.position_path_id == b.position_path_id
    assert b.mae_price > 80 and b.mfe_price < 130 and b.current_quantity == 1.0


def test_snapshot_is_deterministic_and_immutable(env):
    kw = dict(plan=env.plan, position=position(env.plan), candle_rows=bars(env.plan.decision_time, [101, 102]),
              current_time=env.plan.decision_time + 2 * BAR, current_price=102.0)
    a, b = build_position_path_snapshot(**kw), build_position_path_snapshot(**kw)
    assert a == b and a.position_path_id.startswith("ppath_")
    with pytest.raises(dataclasses.FrozenInstanceError):
        a.current_price = 1.0


# ============================== THESIS ==============================
def test_every_plan_thesis_code_is_re_evaluable():
    assert {c.value for c in ThesisCode} <= set(EVALUATORS)


def test_thesis_valid(env):
    out = run(env)
    assert out.forecast.thesis_status == ThesisStatus.VALID.value
    r = results(out)
    assert r[ThesisCode.TREND_CONTINUATION_REMAINS_VALID.value] == "PASS"
    assert r["STRUCTURAL_INVALIDATION_INTACT"] == "PASS"


def test_thesis_weakened(env):
    ms = good_state(env.ev.market_state, trend_state=dataclasses.replace(env.ms.trend_state, direction="FLAT"))
    assert run(env, ms=ms).forecast.thesis_status == ThesisStatus.WEAKENED.value


def test_thesis_invalidated(env):
    ms = good_state(env.ev.market_state, trend_state=dataclasses.replace(env.ms.trend_state, direction="DOWN", strength=0.8))
    out = run(env, ms=ms)
    assert out.forecast.thesis_status == ThesisStatus.INVALIDATED.value
    assert results(out)[ThesisCode.TREND_CONTINUATION_REMAINS_VALID.value] == "FAIL"


def test_thesis_unknown(env):
    ms = good_state(env.ev.market_state, higher_timeframe_state=HigherTimeframeState(available=False))
    out = run(env, ms=ms)
    assert out.forecast.thesis_status == ThesisStatus.UNKNOWN.value
    assert out.decision.action == A.NO_CHANGE_FALLBACK.value and PR.THESIS_UNKNOWN.value in out.decision.reason_codes


def test_stale_calendar_is_never_read_as_no_event(env):
    stale = EventRiskContext(source_state="STALE", maintenance=MaintenanceContext(state="AVAILABLE"))
    out = run(env, events=stale)
    assert results(out)[ThesisCode.EVENT_CONTEXT_ACCEPTABLE.value] == "UNKNOWN"
    assert out.forecast.thesis_status == ThesisStatus.WEAKENED.value
    assert out.decision.action != A.HOLD.value


def test_loss_with_valid_thesis_is_not_an_exit(env):
    out = run(env, closes=(99.5, 98.5, 98.0))
    assert out.path.current_R < 0 and out.forecast.thesis_status == ThesisStatus.VALID.value
    assert out.decision.action == A.HOLD.value


def test_profit_with_invalid_thesis_exits(env):
    ms = good_state(env.ev.market_state, trend_state=dataclasses.replace(env.ms.trend_state, direction="DOWN", strength=0.8))
    out = run(env, closes=(102.0, 103.0, 104.0), ms=ms)
    assert out.path.current_R > 0
    assert out.decision.action == A.EXIT.value and PR.THESIS_INVALIDATED.value in out.decision.reason_codes


# ============================== FORECAST ==============================
def test_conditioned_analog_support_uses_survival(env):
    lib = position_library(env.dims, winners(20) + losers(20, ts=1))
    early = run(env, lib=lib, closes=(), price=100.2)  # zero elapsed bars: every analog alive
    late = run(env, lib=lib)  # 3 bars elapsed: analogs that stopped at bar 1 are gone
    assert early.forecast.updated_support == 40 and late.forecast.updated_support == 20
    assert late.forecast.p_stop_from_now < early.forecast.p_stop_from_now
    assert "survived_bars>=3" in late.forecast.path_conditioning


def test_mfe_bucket_conditioning_updates_probability(env):
    lib = position_library(env.dims, winners(15) + losers(15))  # losers only ever reached mfe 0.4
    flat = run(env, lib=lib, closes=(100.2, 100.3, 100.4))
    ran = run(env, lib=lib, closes=(103.0, 106.0, 104.0))  # mfe 1.24R -> bucket >= 1.0R
    assert ran.forecast.updated_support == 15 and flat.forecast.updated_support == 30
    assert ran.forecast.p_target_from_now > flat.forecast.p_target_from_now


def test_hierarchical_backoff(env):
    dims = dict(env.dims, liquidity_bucket="DEGRADED")
    out = run(env, lib=position_library(dims, winners(20)))
    assert out.forecast.status == PositionForecastStatus.VALID.value
    assert out.forecast.updated_backoff_level > 0 and PR.HIERARCHICAL_BACKOFF.value in out.forecast.reason_codes


def test_insufficient_support_never_fabricates_probabilities(env):
    out = run(env, lib=position_library(env.dims, winners(4)))
    f = out.forecast
    assert f.status == PR.POSITION_FORECAST_SUPPORT_INSUFFICIENT.value
    assert (f.p_target_from_now, f.p_stop_from_now, f.conservative_remaining_edge_R) == (0.0, 0.0, 0.0)
    assert out.decision.action == A.NO_CHANGE_FALLBACK.value


def test_remaining_ev_is_from_now_not_sunk(env):
    near = run(env, closes=(101.0, 102.0, 103.0))
    far = run(env, closes=(101.0, 102.5, 104.0))
    # same analogs (all end at +2R): remaining gross = 2R - current_R, never "original EV - unrealized"
    assert near.forecast.remaining_gross_EV_R == pytest.approx(2.0 - near.path.current_R)
    assert far.forecast.remaining_gross_EV_R == pytest.approx(2.0 - far.path.current_R)
    assert near.forecast.remaining_R_basis == REMAINING_R_BASIS == "ORIGINAL_INITIAL_RISK"


def test_remaining_costs_are_future_only_and_paid_funding_not_double_counted(env):
    base = run(env)
    t0 = env.plan.decision_time
    paid = dataclasses.replace(position(env.plan), fills=(FillEvent(t0, FillKind.OPEN, 1.0, 100.0, fee=0.05),
                                                          FillEvent(t0 + BAR, FillKind.FUNDING, amount=0.4)))
    after = run(env, pos=paid)
    rc0, rc1 = base.forecast.remaining_costs, after.forecast.remaining_costs
    assert rc1.costs_already_realized_R > 0 and rc0.costs_already_realized_R == 0
    assert rc1.funding_paid_R == pytest.approx(0.4 / 5.0)
    assert rc1.expected_future_holding_and_exit_costs_R == pytest.approx(rc0.expected_future_holding_and_exit_costs_R)
    assert after.forecast.remaining_net_EV_R == pytest.approx(base.forecast.remaining_net_EV_R)
    # exit side only: the exit half-spread, never the round trip already paid on entry
    assert rc0.exit_spread_R < env.ev.cost_estimate.spread_R
    assert rc0.expected_remaining_holding_ms <= rc0.max_remaining_holding_ms


def test_forecast_and_decision_are_deterministic(env):
    a, b = run(env, db=False), run(env, db=False)
    assert a.forecast.position_forecast_id == b.forecast.position_forecast_id
    assert a.decision.exit_decision_id == b.decision.exit_decision_id and a.decision == b.decision


# ============================== HOLD / EXIT ==============================
def test_hold_requires_valid_thesis_and_edge(env):
    out = run(env)
    d = out.decision
    assert d.action == A.HOLD.value and d.thesis_status == "VALID"
    assert d.conservative_remaining_edge_R >= policy().hold_edge_floor
    assert d.requested_fraction is None and d.suggested_protection_price is None and not d.requires_broker_action


def test_exit_on_structural_break(env):
    out = run(env, closes=(99.0, 97.0, 95.5), price=94.9)
    assert out.decision.action == A.EXIT.value and PR.STRUCTURAL_INVALIDATION_BROKEN.value in out.decision.reason_codes


def test_exit_on_negative_remaining_edge(env):
    out = run(env, lib=position_library(env.dims, losers(20)), closes=(100.2, 100.3, 100.5))
    assert out.forecast.conservative_remaining_edge_R < policy().exit_edge_floor
    assert out.decision.action == A.EXIT.value and PR.REMAINING_EDGE_BELOW_EXIT_FLOOR.value in out.decision.reason_codes


def _event(plan, t, importance="HIGH"):
    return EventRiskContext(source_state="AVAILABLE", maintenance=MaintenanceContext(state="AVAILABLE"), events=(
        MarketEvent(event_id="evt_cpi", source="test", event_type="INFLATION", scheduled_time=t + 10 * 60_000,
                    importance=importance, affected_instruments=(plan.instrument_key.canonical_symbol,)),))


def test_event_mandatory_de_risk(env):
    t = env.plan.decision_time + 3 * BAR + 1_000
    out = run(env, events=_event(env.plan, t))
    assert out.decision.action == A.REDUCE.value and PR.EVENT_DE_RISK_REQUIRED.value in out.decision.reason_codes
    out = run(env, events=_event(env.plan, t), pol=policy(event_de_risk_behavior="EXIT"))
    assert out.decision.action == A.EXIT.value


def test_shock_and_system_mandatory_de_risk(env):
    out = run(env, regime=regime_with(env.regime, SHOCK=0.6, TREND_CONTINUATION=0.2))
    assert out.decision.action == A.EXIT.value and PR.SHOCK_DE_RISK_REQUIRED.value in out.decision.reason_codes
    from app.trading_intelligence.contracts.system_health import BrokerHealthContext, SystemHealthContext

    down = SystemHealthContext(broker_health=BrokerHealthContext(env.plan.broker_account_id, env.plan.venue, "DEMO",
                                                                 "UNAVAILABLE", 0, "test"))
    out = run(env, system=down)
    assert out.decision.action == A.EXIT.value and PR.SYSTEM_DE_RISK_REQUIRED.value in out.decision.reason_codes


# ============================== REDUCE ==============================
def test_reduce_valid_fraction_and_no_quantity(env):
    ms = good_state(env.ev.market_state, trend_state=dataclasses.replace(env.ms.trend_state, direction="FLAT"))
    out = run(env, ms=ms)
    d = out.decision
    assert d.action == A.REDUCE.value and 0 < d.requested_fraction < 1
    assert d.requested_fraction == policy().default_reduce_fraction
    assert PR.REMAINING_EDGE_MARGINAL.value in d.reason_codes
    assert not {"quantity", "qty", "leverage"} & set(ExitDecision.__dataclass_fields__)


def test_reduce_invalid_fraction_rejected(env):
    base = run(env, db=False).decision
    for bad in (0.0, 1.0, 1.5, None):
        with pytest.raises(ValueError):
            dataclasses.replace(base, action=A.REDUCE.value, requested_fraction=bad)
    with pytest.raises(ValueError):
        ExitPolicy(default_reduce_fraction=1.2)


# ============================== TAKE_PARTIAL ==============================
def test_take_partial_profitable_path_with_positive_residual(env):
    lib = position_library(env.dims, [dict(outcome="TARGET_BEFORE_STOP", gross=3.0, mfe=3.2, mae=0.3, tt=9)] * 20)
    out = run(env, lib=lib, closes=(104.0, 107.0, 109.8))  # inside the plan's target zone
    d = out.decision
    assert d.action == A.TAKE_PARTIAL.value and d.conservative_remaining_edge_R > 0
    assert PR.PROFIT_HARVEST.value in d.reason_codes and PR.TARGET_ZONE_REACHED.value in d.reason_codes
    assert 0 < d.requested_fraction <= policy().maximum_partial_fraction


def test_take_partial_needs_minimum_progress(env):
    lib = position_library(env.dims, [dict(outcome="TARGET_BEFORE_STOP", gross=3.0, mfe=3.2, mae=0.3, tt=9)] * 20)
    out = run(env, lib=lib, closes=(100.5, 100.8, 101.0))
    assert out.decision.action != A.TAKE_PARTIAL.value


# ============================== TIGHTEN ==============================
def test_tighten_long_only_tighter(env):
    ms = good_state(env.ev.market_state, swing_ref=98.0)
    out = run(env, ms=ms)
    d = out.decision
    assert d.action == A.TIGHTEN_PROTECTION.value and d.suggested_protection_price == 98.0
    assert d.existing_protection_price == 95.0 and d.suggested_protection_price > d.existing_protection_price


def test_tighten_break_even_after_mfe(env):
    out = run(env, closes=(103.0, 106.0, 105.5))
    assert out.decision.action == A.TIGHTEN_PROTECTION.value
    assert out.decision.suggested_protection_price == pytest.approx(env.plan.entry_reference)


def test_tighten_short_only_tighter():
    p = ExitPolicy()
    assert protection_is_tighter("SHORT", 105.0, 103.0) and not protection_is_tighter("SHORT", 105.0, 106.0)
    ok, _ = validate_suggested_protection(side="SHORT", existing_stop=105.0, proposed=102.0, current_price=98.0, R=5.0,
                                          policy=p)
    bad, why = validate_suggested_protection(side="SHORT", existing_stop=105.0, proposed=106.0, current_price=98.0, R=5.0,
                                             policy=p)
    assert ok and not bad and why == PR.PROTECTION_WIDENING_REJECTED.value


def test_widening_rejected_everywhere(env):
    ms = good_state(env.ev.market_state, swing_ref=93.0)  # below the existing 95 stop: would widen
    out = run(env, ms=ms)
    assert out.decision.action != A.TIGHTEN_PROTECTION.value
    base = out.decision
    with pytest.raises(ValueError, match="never widen"):
        dataclasses.replace(base, action=A.TIGHTEN_PROTECTION.value, suggested_protection_price=94.0,
                            existing_protection_price=95.0, requested_fraction=None)
    path = out.path
    assert suggest_tighter_stop(env.plan, path, ms, ExitPolicy()) == (None, None)


def test_broker_protection_never_removed(env):
    """CATI has no action that removes protection, and without a known
    existing stop no TIGHTEN can even be expressed."""
    assert "REMOVE_PROTECTION" not in {a.value for a in A}
    out = run(env, pos=position(env.plan, protection="UNPROTECTED"))
    assert out.decision.action == A.NO_CHANGE_FALLBACK.value
    assert PR.PROTECTION_STATE_INVALID.value in out.decision.reason_codes


# ============================== FALLBACK ==============================
def test_fallback_missing_evidence(env):
    out = run(env, lib=None)
    assert out.forecast.status == PositionForecastStatus.OUTCOME_LIBRARY_UNAVAILABLE.value
    assert out.decision.action == A.NO_CHANGE_FALLBACK.value
    out = run(env, obs=None)
    assert out.forecast.status == PositionForecastStatus.REMAINING_COST_UNAVAILABLE.value
    assert out.decision.action == A.NO_CHANGE_FALLBACK.value


def test_uncalibrated_library_blocks_adaptive_actions_but_not_hard_exits(env):
    lib = position_library(env.dims, winners(20), calibration="RESEARCH_ONLY")
    out = run(env, lib=lib, pol=policy(require_calibrated_library=True))
    assert out.decision.action == A.NO_CHANGE_FALLBACK.value and PR.LIBRARY_NOT_CALIBRATED.value in out.decision.reason_codes
    out = run(env, lib=lib, closes=(99.0, 97.0, 95.5), price=94.9)
    assert out.decision.action == A.EXIT.value


def test_thesis_based_policy_reduces_without_support(env):
    ms = good_state(env.ev.market_state, trend_state=dataclasses.replace(env.ms.trend_state, direction="FLAT"))
    out = run(env, ms=ms, lib=position_library(env.dims, winners(3)), pol=policy(insufficient_support_behavior="THESIS_BASED"))
    assert out.decision.action == A.REDUCE.value


def test_component_failure_falls_back_with_evidence(env, monkeypatch):
    clear_component_errors()

    def boom(**_kw):
        raise RuntimeError("apiKey=AKIAsecretVALUE123 forecast exploded")

    monkeypatch.setattr(svc_mod, "build_position_forecast", boom)
    pos = position(env.plan)
    out = run(env, pos=pos)
    assert out.decision.action == A.NO_CHANGE_FALLBACK.value and PR.COMPONENT_ERROR.value in out.decision.reason_codes
    rec = recent_component_errors()[-1]
    assert rec.reason_code == "CATI_COMPONENT_ERROR" and rec.stage == "EXIT_DECISION"
    assert "AKIAsecretVALUE123" not in rec.message
    assert pos.current_stop_price == env.plan.structural_invalidation_price  # existing protection unchanged


def test_existing_position_manager_state_untouched(env):
    from app.execution.position_manager import PositionManager, PositionSide

    pm = PositionManager()
    pm.open_position(symbol="BTCUSDT", side=PositionSide.LONG, position_id="pos_1", entry_price=100.0, qty=1.0,
                     stop_price=95.0, tp1_price=105.0, tp2_price=111.0)
    before = dataclasses.asdict(pm.get_position("BTCUSDT"))
    for closes in ((101.0, 102.0, 103.0), (99.0, 97.0, 95.5)):
        run(env, closes=closes)
    assert dataclasses.asdict(pm.get_position("BTCUSDT")) == before


# ============================== MULTI-ASSET REMAINING COSTS ==============================
def _path(key, *, price, R, qty=1.0, mult=1.0, side="LONG", t, funding_paid=0.0):
    return SimpleNamespace(position_path_id="ppath_x", original_R_reference=R, contract_multiplier=mult,
                           original_quantity=qty, current_quantity=qty, entry_fees_paid=0.0,
                           funding_paid_or_accrued=funding_paid, financing_paid_or_accrued=0.0, current_time=t,
                           instrument_key=key, current_price=price, side=side)


def test_crypto_funding_from_now(env):
    from app.trading_intelligence.venue.cost_model import build_remaining_cost_estimate

    obs = env.obs
    t = obs.decision_time + 1_000
    short = build_remaining_cost_estimate(path=_path(obs.instrument_key, price=100.0, R=5.0, t=t), observation=obs,
                                          expected_remaining_holding_ms=60_000, max_remaining_holding_ms=60_000)
    long_hold = build_remaining_cost_estimate(path=_path(obs.instrument_key, price=100.0, R=5.0, t=t), observation=obs,
                                              expected_remaining_holding_ms=24 * 3_600_000,
                                              max_remaining_holding_ms=24 * 3_600_000)
    assert short.usable and long_hold.future_funding_R > short.future_funding_R >= 0.0
    assert short.future_financing_R == 0.0 and short.future_carry_R == 0.0


def test_forex_rollover_from_now():
    from _venue import FIXTURE_REGISTRY, FX_OPEN_MS, fx_key, fx_raw, fx_request

    from app.trading_intelligence.venue.cost_model import build_remaining_cost_estimate
    from app.trading_intelligence.venue.reference_adapters import ForexEconomicAdapter

    obs = ForexEconomicAdapter(status_registry=FIXTURE_REGISTRY).observe(fx_request(), fx_raw(with_depth=True))
    p = _path(fx_key(), price=1.1, R=0.002, qty=100_000, t=FX_OPEN_MS + 1_000)
    none = build_remaining_cost_estimate(path=p, observation=obs, expected_remaining_holding_ms=3_600_000,
                                         max_remaining_holding_ms=3_600_000)
    over = build_remaining_cost_estimate(path=p, observation=obs, expected_remaining_holding_ms=2 * 86_400_000,
                                         max_remaining_holding_ms=2 * 86_400_000)
    assert over.future_financing_R > 0 and none.future_financing_R == 0.0 and over.future_funding_R == 0.0


def test_futures_carry_from_now():
    from _venue import FIXTURE_REGISTRY, FX_OPEN_MS, fut_key, fut_raw, fut_request

    from app.trading_intelligence.venue.cost_model import build_remaining_cost_estimate
    from app.trading_intelligence.venue.reference_adapters import DatedFuturesEconomicAdapter

    obs = DatedFuturesEconomicAdapter(status_registry=FIXTURE_REGISTRY).observe(fut_request(), fut_raw())
    p = _path(fut_key(), price=5025.0, R=20.0, qty=1.0, mult=50.0, t=FX_OPEN_MS + 1_000)
    rc = build_remaining_cost_estimate(path=p, observation=obs, expected_remaining_holding_ms=5 * 86_400_000,
                                       max_remaining_holding_ms=5 * 86_400_000)
    assert rc.future_carry_R > 0 and rc.future_funding_R == 0.0 and rc.exit_fee_R > 0


# ============================== SHADOW FLOW / LEGACY / EVIDENCE ==============================
def test_legacy_positions_untouched(env):
    service = PositionIntelligenceService(library=env.lib, policy=policy(), db=env.db)
    for plan, pos in ((None, position(env.plan)), (env.plan, position(env.plan, trade_plan_id=None))):
        out = service.evaluate(plan=plan, position=pos, candle_rows=[], current_time=env.plan.decision_time + BAR,
                               current_price=100.0, market_state=env.ms, regime=env.regime)
        assert out.status == LEGACY and out.decision is None
    with env.db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM cati_exit_decisions").fetchone()[0] == 0


def test_shadow_flow_persists_append_only_evidence(env):
    out = run(env)
    with env.db.connect() as conn:
        f = conn.execute("SELECT * FROM cati_position_forecasts").fetchall()
        d = conn.execute("SELECT * FROM cati_exit_decisions").fetchall()
    assert len(f) == 1 and len(d) == 1
    assert d[0]["action"] == out.decision.action and d[0]["position_forecast_id"] == out.forecast.position_forecast_id
    payload = json.loads(f[0]["payload"])
    assert payload["position_path"]["position_path_id"] == out.path.position_path_id
    run(env)  # identical re-evaluation: idempotent
    with env.db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM cati_exit_decisions").fetchone()[0] == 1
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            conn.execute("UPDATE cati_exit_decisions SET action='EXIT'")


def test_replay_and_shadow_produce_identical_analytics(env):
    live, replay = run(env, db=False, mode="SHADOW"), run(env, db=False, mode="REPLAY")
    assert live.forecast.position_forecast_id == replay.forecast.position_forecast_id
    assert live.decision.exit_decision_id == replay.decision.exit_decision_id
    assert replay.decision.evaluation_mode == "REPLAY"


# ============================== SAFETY ==============================
_FORBIDDEN_IMPORTS = ("app.execution", "app.exchange", "app.runner", "app.core.trading_orchestrator",
                      "app.trading_intelligence.execution", "app.risk")
_FORBIDDEN_CALLS = {"place_order", "cancel_order", "cancel_all_orders", "close_position", "close_position_market",
                    "update_protection", "place_protection", "open_position", "execute_signal", "submit_exit",
                    "submit_reduce", "modify_protection", "reserve", "consume", "release", "set_leverage"}


def test_position_intelligence_cannot_touch_orders_positions_or_protection():
    for path in sorted(POSITION_PKG.glob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                mod = node.module if isinstance(node, ast.ImportFrom) else node.names[0].name
                assert not any(str(mod).startswith(f) for f in _FORBIDDEN_IMPORTS), (path.name, mod)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in _FORBIDDEN_CALLS, (path.name, node.func.attr)


def test_decisions_are_intents_only(env):
    out = run(env)
    for name in ("place_order", "close", "cancel", "execute"):
        assert not hasattr(out.decision, name)
    assert not any(hasattr(svc_mod.PositionIntelligenceService, x) for x in ("executor", "position_manager", "client"))


def test_no_recovery_trading_objective():
    fields = set(ExitPolicy.__dataclass_fields__)
    assert not any(("recover" in f or "balance" in f or "equity" in f) for f in fields)
