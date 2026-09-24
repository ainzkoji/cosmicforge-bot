"""Real Section 12-17 pipeline objects for Section 17/18 tests: synthetic
market + library (as _helpers), Section 17 venue costs from the real
Binance adapter over Binance-shaped payloads, then the real Section 13
engine, Section 14 veto, Section 15 coordinator and Section 16 service."""
from __future__ import annotations

import copy
import dataclasses
from typing import Optional, Sequence

from _helpers import (
    build_library, candidate_for, clear_event_context, flat_rows, healthy_system_context, instrument,
    market_state_and_regime, permissive_veto_policy, rows_from_closes,
)
from _pf import add_bot, context, gen, make_db
from _venue import FIXTURE

from app.replay.cost_model import CostModel
from app.trading_intelligence.contracts.ranking import EvaluatedOpportunity, SymbolEvalKind, SymbolEvaluation
from app.trading_intelligence.economics.engine import evaluate_economic_opportunity
from app.trading_intelligence.forecast.cohorts import derive_cohort_dimensions
from app.trading_intelligence.forecast.engine import build_outcome_forecast
from app.trading_intelligence.forecast.labels import label_candidate
from app.trading_intelligence.forecast.library import HistoricalOutcomeLibrary, LibraryRow
from app.trading_intelligence.portfolio.service import ShadowAccountPortfolioService
from app.trading_intelligence.ranking.coordinator import CATICycleCoordinator
from app.trading_intelligence.trade_plan.builder import TenantContext
from app.trading_intelligence.venue.adapter import VenueEconomicRequest, VenueRawSnapshot
from app.trading_intelligence.venue.binance import BinanceUsdmEconomicAdapter
from app.trading_intelligence.venue.cost_model import build_venue_cost_estimate
from app.trading_intelligence.veto.engine import evaluate_veto

_LIBS = {}


def short_library(n_rows=40, win_fraction=0.8, family="TREND_PULLBACK_V2"):
    rows, n_wins = [], int(n_rows * win_fraction)
    for i in range(n_rows):
        ms, regime = market_state_and_regime(flat_rows(i))
        cand = candidate_for(ms, side="SHORT", trigger=100.0, invalidation=105.0, target=90.0, family=family)
        future = rows_from_closes([99, 97, 94, 91, 89, 88] if i < n_wins else [101, 103, 106, 107],
                                  start=cand.decision_time + 900_000)
        label = label_candidate(cand, future, cost_model=CostModel.zero(), horizon_bars=10)
        dims = derive_cohort_dimensions(setup_family=family, side="SHORT", market_state=ms, regime_distribution=regime)
        rows.append(LibraryRow(label=label, cohort_dimensions=dims, continuous_features={"room_to_target_R": cand.room_to_target_R}))
    return HistoricalOutcomeLibrary.build(tuple(rows), dataset_source_hash="synthetic_short",
                                          candidate_generation_versions={family: "2.0.0"}, label_policy_version="1.0.0",
                                          cost_model_version="1.0.0")


def library(side="LONG"):
    if side not in _LIBS:
        _LIBS[side] = build_library(win_fraction=0.85) if side == "LONG" else short_library(win_fraction=0.85)
    return _LIBS[side]


def binance_like_raw(symbol: str, *, mid: float = 100.0, t: int, tick: float = 0.01, funding: float = 0.0001,
                     next_funding_in_ms: int = 3_600_000, commission: Optional[dict] = None, depth: bool = True,
                     book_age_ms: int = 500, depth_qty: float = 200.0) -> VenueRawSnapshot:
    """Binance-USD-M-shaped payloads (the recorded demo shape) around ``mid``."""
    info = copy.deepcopy(FIXTURE["symbols"]["BTCUSDT"]["exchange_info_symbol"])
    info.update(symbol=symbol, pair=symbol, baseAsset=symbol.replace("USDT", ""), quoteAsset="USDT", marginAsset="USDT")
    for f in info["filters"]:
        if f["filterType"] == "PRICE_FILTER":
            f["tickSize"] = str(tick)
        if f["filterType"] == "LOT_SIZE":
            f["stepSize"], f["minQty"] = "0.001", "0.001"
        if f["filterType"] == "MIN_NOTIONAL":
            f["notional"] = "5"
    bid, ask = mid - tick / 2, mid + tick / 2
    payloads = {
        "exchange_info_symbol": info, "exchange_info_as_of": t - 60_000, "funding_info": [],
        "book_ticker": {"symbol": symbol, "bidPrice": str(bid), "askPrice": str(ask), "bidQty": "500", "askQty": "500",
                        "time": t - book_age_ms},
        "premium_index": {"symbol": symbol, "markPrice": str(mid), "indexPrice": str(mid * 0.9999),
                          "lastFundingRate": str(funding), "nextFundingTime": t + next_funding_in_ms,
                          "time": t - 1_000},
    }
    if depth:
        payloads["depth"] = {"bids": [[str(bid - i * tick), str(depth_qty)] for i in range(20)],
                             "asks": [[str(ask + i * tick), str(depth_qty)] for i in range(20)], "T": t - 400}
    if commission is not None:
        payloads["commission_rate"] = commission
    return VenueRawSnapshot(venue_symbol=symbol, payloads=payloads, captured_at=t)


def venue_evaluated(symbol="BTCUSDT", *, seed=999, side="LONG", account="acct1", user="u1", bot="botA", cycle="c1",
                    run="r1", env="DEMO", veto_policy=None, raw_kwargs=None, adapter=None, lib=None,
                    system_context=None, event_context=None, start: Optional[int] = None,
                    **cand_kwargs) -> EvaluatedOpportunity:
    inst = instrument(symbol)
    ms, regime = market_state_and_regime(flat_rows(seed, start=start), inst)
    if side == "SHORT":
        cand_kwargs = {"trigger": 100.0, "invalidation": 105.0, "target": 90.0, **cand_kwargs}
    cand = candidate_for(ms, side=side, **cand_kwargs)
    forecast = build_outcome_forecast(cand, ms, regime, lib or library(side))
    raw = binance_like_raw(symbol, t=cand.decision_time + 2_000, **(raw_kwargs or {}))
    adapter = adapter or BinanceUsdmEconomicAdapter()
    request = VenueEconomicRequest(
        instrument_key=inst, environment=env, decision_time=adapter.causal_decision_time(raw, cand.decision_time),
        user_id=user, broker_account_id=account, bot_instance_id=bot, run_id=run, cycle_id=cycle,
        broker_health=(system_context or healthy_system_context(account)).broker_health)
    obs = adapter.observe(request, raw)
    cost = build_venue_cost_estimate(cand, obs, forecast=forecast, policy=adapter.policy)
    opp = evaluate_economic_opportunity(cand, ms, forecast, cost, user_id=user, broker_account_id=account,
                                        bot_instance_id=bot, run_id=run, cycle_id=cycle)
    veto = evaluate_veto(opportunity=opp, candidate=cand, market_state=ms, regime_distribution=regime, forecast=forecast,
                         cost_estimate=cost, policy=veto_policy or permissive_veto_policy(),
                         event_context=event_context or clear_event_context(),
                         system_context=system_context or healthy_system_context(account), user_id=user,
                         broker_account_id=account, bot_instance_id=bot, run_id=run, cycle_id=cycle)
    return EvaluatedOpportunity(cand, ms, regime, forecast, cost, opp, veto, venue_observation=obs)


def run_pipeline(db, evaluations: Sequence[EvaluatedOpportunity], *, account="acct1", bot="botA", cycle="c1",
                 max_open_positions=3, now_ms: Optional[int] = None, incomplete_symbol: Optional[str] = None):
    """Section 15 whole-universe ranking + Section 16 selection/reservation."""
    coord = CATICycleCoordinator()
    symbols = [e.candidate.instrument_key.venue_symbol for e in evaluations]
    t0 = max(e.candidate.decision_time for e in evaluations)
    key = coord.begin_bot_cycle(bot_instance_id=bot, cycle_id=cycle, cycle_time_ms=t0, user_id="u1",
                                broker_account_id=account, run_id="r1", universe_symbols=symbols)
    for e in evaluations:
        coord.mark_due(key, e.candidate.instrument_key.venue_symbol)
        coord.record_symbol_evaluation(key, SymbolEvaluation(e.candidate.instrument_key.venue_symbol,
                                                             SymbolEvalKind.EVALUATED.value, opportunities=(e,)))
    if incomplete_symbol:
        coord.mark_due(key, incomplete_symbol)  # due but never terminal
    result = coord.finalize_bot_cycle(key)
    now_ms = now_ms if now_ms is not None else t0 + 60_000
    service = ShadowAccountPortfolioService(db)
    ctx = context(**{s: gen(i + 1) for i, s in enumerate(sorted(set(symbols)))})
    outcome = service.select_and_reserve(
        ranked=result.ranked, broker_account_id=account, bot_instance_id=bot, cycle_id=cycle,
        max_open_positions=max_open_positions, context=ctx, now_ms=now_ms,
        evaluated_by_candidate_id={e.candidate.setup_candidate_id: e for e in evaluations})
    tenant = TenantContext(broker_account_id=account, bot_instance_id=bot, cycle_id=cycle, user_id="u1", run_id="r1")
    return dict(result=result, outcome=outcome, service=service, tenant=tenant, now_ms=now_ms,
                evaluated={e.candidate.setup_candidate_id: e for e in evaluations})


def fresh_db(tmp_path, name="s18.db", bots=(("botA", "acct1"), ("botB", "acct1"), ("botC", "acct2"))):
    db = make_db(tmp_path / name)
    for bot, acct in bots:
        add_bot(db, bot, acct)
    return db


def build_kwargs(p, rid=None):
    """Builder kwargs for the first selected (or given) ranked opportunity."""
    result, outcome = p["result"], p["outcome"]
    rid = rid or outcome.decision.selected_opportunity_ids[0]
    ranked = next(r for r in result.ranked if r.ranked_opportunity_id == rid)
    ev = p["evaluated"][ranked.setup_candidate_id]
    return dict(ranked=ranked, ranking_batch=result.batch, portfolio_decision=outcome.decision,
                reservation=outcome.reservation, evaluated=ev, tenant=p["tenant"], now_ms=p["now_ms"])


def replace_ev(ev: EvaluatedOpportunity, **changes) -> EvaluatedOpportunity:
    return dataclasses.replace(ev, **changes)
