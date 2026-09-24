"""Section 19-21 test helpers: a REAL Section 18 TradePlan (the full 12-18
pipeline from _plan), controllable current MarketState / regime variants,
and a hand-labeled position library whose analog paths are explicit."""
from __future__ import annotations

import dataclasses
from typing import Optional, Sequence

from _helpers import clear_event_context, healthy_system_context
from _plan import build_kwargs, fresh_db, run_pipeline, venue_evaluated

from app.trading_intelligence.contracts.events import EventRiskContext, MaintenanceContext
from app.trading_intelligence.contracts.forecast import SetupOutcomeLabel
from app.trading_intelligence.contracts.market_state import HigherTimeframeState
from app.trading_intelligence.contracts.position import ExitPolicy, ProtectionState
from app.trading_intelligence.contracts.system_health import BrokerHealthContext, SystemHealthContext
from app.trading_intelligence.forecast.cohorts import derive_cohort_dimensions
from app.trading_intelligence.forecast.library import HistoricalOutcomeLibrary, LibraryRow
from app.trading_intelligence.position.path import PositionLifecycleView
from app.trading_intelligence.trade_plan.builder import TradePlanBuilder

BAR = 900_000


def plan_setup(tmp_path, name="s19.db"):
    db = fresh_db(tmp_path, name)
    pipe = run_pipeline(db, [venue_evaluated("BTCUSDT", seed=999), venue_evaluated("ETHUSDT", seed=998)])
    kw = build_kwargs(pipe)
    res = TradePlanBuilder().build_from_evaluated(**kw)
    assert res.plan is not None, (res.status, res.reason_codes)
    return db, pipe, kw, res.plan


def good_state(ms, *, side="LONG", swing_ref: Optional[float] = None, **overrides):
    """A current MarketState whose structure CONFIRMS the thesis (never PnL)."""
    up = side == "LONG"
    trend = dataclasses.replace(ms.trend_state, direction="UP" if up else "DOWN", strength=0.8, maturity="MID",
                                available=True)
    structure = dataclasses.replace(ms.structure_state, swing_sequence="HH_HL" if up else "LH_LL", choch_direction="NONE",
                                    last_bos_direction="UP" if up else "DOWN", available=True,
                                    invalidation_reference=swing_ref, structure_integrity=0.9)
    htf = HigherTimeframeState(available=True, direction="UP" if up else "DOWN", structure_alignment="ALIGNED")
    liq = dataclasses.replace(ms.liquidity_state, available=True, spread_percentile=0.2, spread_bps=1.0, stale_book=False)
    fields = dict(trend_state=trend, structure_state=structure, higher_timeframe_state=htf, liquidity_state=liq)
    fields.update(overrides)
    return dataclasses.replace(ms, **fields)


def regime_with(regime, **weights):
    w = {"TREND_CONTINUATION": 0.7, "RANGE_EQUILIBRIUM": 0.1, "VOL_EXPANSION": 0.1, "EXHAUSTION_REVERSAL": 0.05,
         "SHOCK": 0.0, "TRANSITION_UNKNOWN": 0.05}
    w.update(weights)
    dom = max(w, key=w.get)
    return dataclasses.replace(regime, weights=w, dominant_regime=dom, dominant_weight=w[dom])


def clean_events(t=0):
    return EventRiskContext(source_state="AVAILABLE", maintenance=MaintenanceContext(state="AVAILABLE"), as_of=t,
                            source="test")


def healthy(plan, t=0):
    return SystemHealthContext(broker_health=BrokerHealthContext(plan.broker_account_id, plan.venue, plan.environment,
                                                                 "HEALTHY", t, "test"))


def label(i: int, *, outcome="TARGET_BEFORE_STOP", gross=2.0, mfe=2.2, mae=0.3, tt=6, ts=None, horizon=20,
          quality="VALID", family="TREND_PULLBACK_V2", instrument=None) -> SetupOutcomeLabel:
    return SetupOutcomeLabel(
        label_id=f"lbl_pos_{i:05d}", setup_candidate_id=f"cand_pos_{i:05d}", market_state_id=f"ms_{i}",
        instrument_key=instrument, decision_time=1_600_000_000_000 + i * BAR, setup_family=family,
        setup_version="2.0.0", setup_policy_hash="h", market_state_schema_version="1.0.0", regime_model_version="1.0.0",
        label_policy_version="1.0.0", cost_model_version="1.0.0", terminal_outcome=outcome,
        net_profitable=gross > 0.05, gross_R=gross, net_R=gross - 0.05, mfe_R=mfe, mae_R=mae,
        time_to_target_bars=tt if outcome == "TARGET_BEFORE_STOP" else None,
        time_to_stop_bars=ts if outcome == "STOP_BEFORE_TARGET" else None, terminal_horizon_bars=horizon,
        fee_R=0.02, spread_R=0.01, slippage_R=0.01, funding_R=0.01, carry_R=0.0, total_cost_R=0.05,
        label_quality=quality)


def position_library(dims, rows_spec: Sequence[dict], *, calibration="CALIBRATED") -> HistoricalOutcomeLibrary:
    rows = []
    for i, spec in enumerate(rows_spec):
        spec = dict(spec)
        d = dict(dims)
        d.update(spec.pop("dims", {}))
        rows.append(LibraryRow(label=label(i, **spec), cohort_dimensions=d))
    lib = HistoricalOutcomeLibrary.build(tuple(rows), dataset_source_hash="pos_synthetic",
                                         candidate_generation_versions={"TREND_PULLBACK_V2": "2.0.0"},
                                         label_policy_version="1.0.0", cost_model_version="1.0.0",
                                         source_kind="SYNTHETIC_TEST")
    return dataclasses.replace(lib, calibration_status=calibration)


def winners(n, **kw):
    return [{**dict(outcome="TARGET_BEFORE_STOP", gross=2.0, mfe=2.2, mae=0.3, tt=8), **kw} for _ in range(n)]


def losers(n, **kw):
    return [{**dict(outcome="STOP_BEFORE_TARGET", gross=-1.0, mfe=0.4, mae=1.0, ts=5), **kw} for _ in range(n)]


def dims_for(plan, ms, regime):
    return derive_cohort_dimensions(setup_family=plan.setup_family, side=plan.side, market_state=ms,
                                    regime_distribution=regime, instrument_group=None)


def policy(**kw) -> ExitPolicy:
    base = dict(minimum_support=10, minimum_ESS=8.0, maximum_forecast_uncertainty=0.95, maximum_OOD=0.95)
    base.update(kw)
    return ExitPolicy(**base)


def position(plan, *, entry_time=None, qty=1.0, stop=None, protection=ProtectionState.PROTECTED.value, **kw):
    return PositionLifecycleView(
        position_id=kw.pop("position_id", "pos_1"), trade_plan_id=kw.pop("trade_plan_id", plan.trade_plan_id),
        side=plan.side, entry_time=entry_time if entry_time is not None else plan.decision_time,
        entry_price=kw.pop("entry_price", plan.entry_reference), original_quantity=qty,
        current_quantity=kw.pop("current_quantity", qty),
        current_stop_price=plan.structural_invalidation_price if stop is None else stop,
        current_target_prices=tuple(z.price_high for z in plan.target_zones), protection_state=protection, **kw)


def bars(start, closes, *, spread=0.2):
    """Closed Binance-style bars opening at ``start`` (one per close)."""
    out = []
    for i, c in enumerate(closes):
        o = start + i * BAR
        out.append([o, str(c), str(c + spread), str(c - spread), str(c), "1000", o + BAR - 1])
    return out


__all__ = ["BAR", "plan_setup", "good_state", "regime_with", "clean_events", "healthy", "label", "position_library",
           "winners", "losers", "dims_for", "policy", "position", "bars", "clear_event_context",
           "healthy_system_context"]
