"""Shared builders for Section 14-16 tests (real pipeline objects, no mocks)."""
from __future__ import annotations

import dataclasses
import random

from app.replay.cost_model import BINANCE_FUTURES_STANDARD, CostModel
from app.trading_intelligence.contracts.instrument import CRYPTO, from_symbol_fallback
from app.trading_intelligence.contracts.market_state import CandleSeries
from app.trading_intelligence.contracts.setup import SetupCandidate
from app.trading_intelligence.contracts.veto import VetoPolicy
from app.trading_intelligence.economics.costs import build_cost_estimate
from app.trading_intelligence.economics.engine import evaluate_economic_opportunity
from app.trading_intelligence.forecast.cohorts import derive_cohort_dimensions
from app.trading_intelligence.forecast.engine import build_outcome_forecast
from app.trading_intelligence.forecast.labels import label_candidate
from app.trading_intelligence.forecast.library import HistoricalOutcomeLibrary, LibraryRow
from app.trading_intelligence.integration.snapshot_adapter import build_data_manifest
from app.trading_intelligence.market_state.engine import build_market_state
from app.trading_intelligence.regime.engine import compute_regime_distribution
from app.trading_intelligence.regime.policy import default_policy

REGIME_POLICY = default_policy()


def instrument(symbol="BTCUSDT", venue="binance"):
    return from_symbol_fallback(venue=venue, venue_symbol=symbol, asset_class=CRYPTO)


def rows_from_closes(closes, start=1_700_000_000_000, interval=900_000):
    return [[start + i * interval, c, c + 0.5, c - 0.5, c, 1000, start + i * interval + interval - 1, 0, 0, 0, 0, 0]
            for i, c in enumerate(closes)]


def series_from_rows(rows):
    return CandleSeries(
        open=tuple(float(r[1]) for r in rows), high=tuple(float(r[2]) for r in rows),
        low=tuple(float(r[3]) for r in rows), close=tuple(float(r[4]) for r in rows),
        volume=tuple(float(r[5]) for r in rows), close_time=tuple(int(r[6]) for r in rows),
    )


def flat_rows(seed, n=60, start=None):
    rng = random.Random(seed)
    return rows_from_closes([100 + 0.1 * j + rng.uniform(-0.3, 0.3) for j in range(n)],
                            start=start if start is not None else 1_600_000_000_000 + seed * 10_000_000)


def market_state_and_regime(rows, inst=None):
    inst = inst or instrument()
    series = series_from_rows(rows)
    manifest = build_data_manifest(
        instrument_key=inst, source="Test", timeframe="15m",
        primary_last_closed_candle_time=series.latest_close_time, primary_data_hash="h",
    )
    ms = build_market_state(
        instrument_key=inst, timeframe="15m", decision_time=series.latest_close_time,
        primary_series=series, snapshot_id="ms_test", data_hash="h", manifest=manifest,
    )
    return ms, compute_regime_distribution(ms, REGIME_POLICY)


def candidate_for(ms, *, side="LONG", trigger=100.0, invalidation=95.0, target=110.0, family="TREND_PULLBACK_V2"):
    return SetupCandidate.build(
        market_state_id=ms.market_state_id, snapshot_id="s", data_hash="h", instrument_key=ms.instrument_key,
        timeframe="15m", decision_time=ms.decision_time, setup_family=family, setup_version="2.0.0",
        setup_policy_hash="p1", side=side, trigger_reference=trigger, structural_invalidation=invalidation,
        target_reference=target,
    )


def build_library(n_rows=40, win_fraction=0.8, family="TREND_PULLBACK_V2", calibration_status="UNCALIBRATED"):
    rows = []
    n_wins = int(n_rows * win_fraction)
    for i in range(n_rows):
        ms, regime = market_state_and_regime(flat_rows(i))
        cand = candidate_for(ms, family=family)
        future = rows_from_closes([101, 103, 106, 109, 111, 112] if i < n_wins else [99, 97, 94, 93],
                                  start=cand.decision_time + 900_000)
        label = label_candidate(cand, future, cost_model=CostModel.zero(), horizon_bars=10)
        dims = derive_cohort_dimensions(setup_family=family, side="LONG", market_state=ms, regime_distribution=regime)
        rows.append(LibraryRow(label=label, cohort_dimensions=dims,
                               continuous_features={"room_to_target_R": cand.room_to_target_R}))
    lib = HistoricalOutcomeLibrary.build(
        tuple(rows), dataset_source_hash="synthetic", candidate_generation_versions={family: "2.0.0"},
        label_policy_version="1.0.0", cost_model_version="1.0.0",
    )
    return dataclasses.replace(lib, calibration_status=calibration_status)


def permissive_veto_policy(**overrides):
    """Research policy that allows uncalibrated shadow evidence (used only to
    exercise the *other* veto families in isolation)."""
    # ood_score_watch sits above the 0.5 that a deep-backoff synthetic
    # library legitimately produces, so other families can be tested alone.
    base = dict(calibration_statuses_allowed_for_approval=("CALIBRATED", "UNCALIBRATED", "RESEARCH_ONLY"),
                ood_score_watch=0.6)
    base.update(overrides)
    return VetoPolicy(**base)


def full_chain(*, seed=999, win_fraction=0.85, symbol="BTCUSDT", library=None, calibration_status="UNCALIBRATED",
               cost_model=BINANCE_FUTURES_STANDARD, side="LONG", **cand_kwargs):
    inst = instrument(symbol)
    ms, regime = market_state_and_regime(flat_rows(seed), inst)
    cand = candidate_for(ms, side=side, **cand_kwargs)
    lib = library if library is not None else build_library(win_fraction=win_fraction, calibration_status=calibration_status)
    forecast = build_outcome_forecast(cand, ms, regime, lib)
    cost = build_cost_estimate(cand, cost_model=cost_model)
    opp = evaluate_economic_opportunity(cand, ms, forecast, cost)
    return dict(candidate=cand, market_state=ms, regime=regime, forecast=forecast, cost=cost, opportunity=opp, library=lib)


# ---------------------------------------------------------------------------
# Historical-candle DB for Section 12.15 pipeline tests. The candles are a
# deterministic wave market (test INPUT data); every candidate, label and
# library row downstream is computed by the real pipeline from it.
# ---------------------------------------------------------------------------
import math
import sqlite3

HIST_START_MS = 1_700_000_000_000
TF_MS = 900_000


def wave_closes(n, *, seed=0, base=100.0, trend=0.04, amp=6.0, period=16, noise=0.25):
    rng = random.Random(seed)
    return [base + trend * i + amp * math.sin((i + seed) / (period / (2 * math.pi))) + rng.uniform(-noise, noise)
            for i in range(n)]


def make_historical_db(path, symbols=("BTCUSDT", "ETHUSDT"), n_bars=900, timeframe="15m", start_ms=HIST_START_MS):
    conn = sqlite3.connect(str(path))
    conn.execute("""CREATE TABLE IF NOT EXISTS historical_candles (
        id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, interval TEXT, open_time INTEGER, open REAL, high REAL,
        low REAL, close REAL, volume REAL, quote_volume REAL, trades INTEGER, market_type TEXT, base_currency TEXT,
        quote_currency TEXT, data_source TEXT, data_version TEXT, fetched_at TEXT,
        UNIQUE(symbol, interval, open_time, data_source, market_type))""")
    for k, symbol in enumerate(symbols):
        closes = wave_closes(n_bars, seed=k * 7 + 1)
        for i, c in enumerate(closes):
            o = closes[i - 1] if i else c
            hi = max(o, c) + 0.3
            lo = min(o, c) - 0.3
            conn.execute(
                "INSERT INTO historical_candles (symbol, interval, open_time, open, high, low, close, volume, quote_volume,"
                " trades, market_type, base_currency, quote_currency, data_source, data_version, fetched_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (symbol, timeframe, start_ms + i * TF_MS, o, hi, lo, c, 1000.0 + (i % 7) * 30, 1e5, 100, "crypto",
                 symbol.replace("USDT", ""), "USDT", "synthetic_test_wave", "v1", "2026-01-01"),
            )
    conn.commit()
    conn.close()
    return path


# ---------------------------------------------------------------------------
# Section 15/16 helpers
# ---------------------------------------------------------------------------
def healthy_system_context(account="acct-1", venue="binance"):
    """A wired, HEALTHY broker-health context (as the runtime adapter produces)."""
    from app.trading_intelligence.contracts.system_health import BrokerHealthContext, SystemHealthContext

    return SystemHealthContext(broker_health=BrokerHealthContext(
        broker_account_id=account, venue=venue, environment="DEMO", status="HEALTHY", observed_at=0,
        source="test", freshness_ms=0))


def clear_event_context(as_of=0):
    """An AVAILABLE calendar with no events and an AVAILABLE, empty maintenance feed."""
    from app.trading_intelligence.contracts.events import EventRiskContext, MaintenanceContext

    return EventRiskContext(source_state="AVAILABLE", events=(), as_of=as_of, source="test",
                            maintenance=MaintenanceContext(state="AVAILABLE", source="test", as_of=as_of))


def evaluated_from_chain(chain, veto_policy=None, **veto_kwargs):
    from app.trading_intelligence.contracts.ranking import EvaluatedOpportunity
    from app.trading_intelligence.veto.engine import evaluate_veto

    veto_kwargs.setdefault("system_context", healthy_system_context())
    veto_kwargs.setdefault("event_context", clear_event_context())
    veto = evaluate_veto(
        opportunity=chain["opportunity"], candidate=chain["candidate"], market_state=chain["market_state"],
        regime_distribution=chain["regime"], forecast=chain["forecast"], cost_estimate=chain["cost"],
        policy=veto_policy or permissive_veto_policy(), **veto_kwargs,
    )
    return EvaluatedOpportunity(chain["candidate"], chain["market_state"], chain["regime"], chain["forecast"],
                                chain["cost"], chain["opportunity"], veto)


def make_evaluated(symbol, *, seed=999, win_fraction=0.85, library=None, side="LONG", veto_policy=None, **kw):
    chain = full_chain(seed=seed, win_fraction=win_fraction, symbol=symbol, library=library, side=side, **kw)
    return evaluated_from_chain(chain, veto_policy)
