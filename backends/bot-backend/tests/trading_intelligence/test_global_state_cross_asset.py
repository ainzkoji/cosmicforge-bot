"""Section 24 GlobalMarketState + cross-asset account risk (currency factor cap,
logical asset-class allocation, shared broker-account aggregation)."""
from __future__ import annotations

import dataclasses
import math
from types import SimpleNamespace

import pytest
from _helpers import instrument, market_state_and_regime, rows_from_closes
from _pf import context, fx, held, ranked, select

from app.trading_intelligence.contracts.global_market_state import (COMPONENTS, GlobalComponent,
                                                                    GlobalMarketState)
from app.trading_intelligence.contracts.instrument import from_fx_pair
from app.trading_intelligence.contracts.portfolio_intel import PortfolioPolicy
from app.trading_intelligence.market_state.global_state import MIN_INPUTS, build_global_market_state


def _trend(n=320, slope=0.2, base=100.0, seed=0):
    """slope / wiggle are in % of ``base`` so FX (1.1) and crypto (100) series behave alike."""
    return [base * (1 + slope / 100.0 * i + 0.003 * math.sin(i / 3 + seed)) for i in range(n)]


def _ms(symbol, closes, *, venue="binance", fx_pair=None, start=1_700_000_000_000):
    inst = (from_fx_pair(venue=venue, venue_symbol=symbol, base=fx_pair[0], quote=fx_pair[1]) if fx_pair
            else instrument(symbol, venue=venue))
    ms, _ = market_state_and_regime(rows_from_closes(closes, start=start), inst)
    return ms


@pytest.fixture(scope="module")
def states():
    up = [_ms(s, _trend(seed=i)) for i, s in enumerate(("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"))]
    down = [_ms("DOGEUSDT", _trend(slope=-0.2, base=200.0))]
    fxs = [_ms("EURUSD", _trend(base=1.1), venue="ref", fx_pair=("EUR", "USD")),
           _ms("GBPUSD", _trend(base=1.3, seed=1), venue="ref", fx_pair=("GBP", "USD")),
           _ms("USDJPY", _trend(slope=-0.2, base=150.0, seed=2), venue="ref", fx_pair=("USD", "JPY"))]
    return up + down + fxs


def test_global_state_is_deterministic_hashed_and_complete(states):
    a = build_global_market_state(states, decision_time=states[0].decision_time, timeframe="15m")
    b = build_global_market_state(list(reversed(states)), decision_time=states[0].decision_time, timeframe="15m")
    assert a.global_state_id == b.global_state_id and a.state_hash == b.state_hash  # order-independent
    assert set(a.components) == set(COMPONENTS)
    assert a.asset_classes == ("CRYPTO", "FX")
    assert a.component("crypto_context").status == "AVAILABLE"
    assert a.component("crypto_context").label.startswith("BTC_")
    b_ = a.component("breadth")
    assert b_.inputs == b_.detail["up"] + b_.detail["down"] + b_.detail["flat"] <= len(states)
    assert a.component("correlation").reason == "NOT_COMPUTED_IN_CYCLE"
    assert a.component("event_risk").reason == "EVENT_CONTEXT_NOT_SUPPLIED"
    json_blob = str(a.to_dict())
    for forbidden in ("user_id", "broker_account_id", "bot_instance_id"):
        assert forbidden not in json_blob  # tenant-neutral contract


def test_global_state_usd_factor_uses_fx_legs_not_stablecoin_quotes(states):
    g = build_global_market_state(states, decision_time=states[0].decision_time, timeframe="15m")
    usd = g.component("usd_factor")
    # EURUSD up (USD -), GBPUSD up (USD -), USDJPY down (USD -): three USD legs, all weak
    assert usd.status == "AVAILABLE" and usd.inputs == 3 and usd.value < 0 and usd.label == "USD_WEAK"
    crypto_only = build_global_market_state(states[:5], decision_time=states[0].decision_time, timeframe="15m")
    assert crypto_only.component("usd_factor").reason == "NO_FX_INSTRUMENTS"  # USDT quotes are not a USD view


def test_global_state_is_causal_and_broker_neutral(states):
    t = states[0].decision_time
    future = _ms("ADAUSDT", _trend(), start=1_700_000_000_000 + 10 * 900_000)  # closes after t
    dup = _ms("BTCUSDT", _trend(), venue="bybit")  # the same canonical instrument on another venue
    g = build_global_market_state(states + [future, dup], decision_time=t, timeframe="15m")
    dq = g.component("data_quality").detail
    assert dq["excluded_future_candle"] == 1 and dq["excluded_duplicate_venue"] == 1
    assert len(g.input_market_state_ids) == len(states)


def test_missing_inputs_are_unavailable_with_reason_never_zero():
    g = build_global_market_state([], decision_time=1, timeframe="15m")
    for name in COMPONENTS:
        c = g.component(name)
        assert c.status == "UNAVAILABLE" and c.reason and c.value is None and c.label is None
    with pytest.raises(ValueError):
        GlobalComponent("x", "UNAVAILABLE", value=0.0, reason="R")  # a value on UNAVAILABLE is refused
    with pytest.raises(ValueError):
        GlobalComponent("x", "UNAVAILABLE")                          # UNAVAILABLE needs a reason
    few = build_global_market_state([_ms("BTCUSDT", _trend())], decision_time=10 ** 13, timeframe="15m")
    assert few.component("breadth").reason == f"INSUFFICIENT_INPUTS_1_LT_{MIN_INPUTS}"
    with pytest.raises(ValueError):
        GlobalMarketState(decision_time=1, timeframe="15m", asset_classes=(), components={}, input_market_state_ids=(),
                          input_hash="h")


def test_global_state_evidence_is_append_only(tmp_path, states):
    from shared_lib.persistence.cati_schema import ensure_cati_schema
    from shared_lib.persistence.db import DB

    from app.trading_intelligence.market_state.global_state_store import GlobalMarketStateStore

    db = DB(path=str(tmp_path / "gms.db"))
    ensure_cati_schema(db)
    g = build_global_market_state(states, decision_time=states[0].decision_time, timeframe="15m")
    store = GlobalMarketStateStore(db)
    row = store.append(g, cycle_id="c1", bot_instance_id="b1", broker_account_id="a1")
    store.append(g, cycle_id="c1", bot_instance_id="b1", broker_account_id="a1")  # idempotent
    assert [r["global_state_id"] for r in store.latest(limit=5)] == [g.global_state_id]
    with pytest.raises(Exception):
        with db.connect() as c:
            c.execute("UPDATE cati_global_market_states SET risk_regime='X' WHERE evidence_id=?", (row["evidence_id"],))


def test_cycle_stage_records_global_state_and_never_raises(tmp_path, states, monkeypatch):
    from shared_lib.persistence.cati_schema import ensure_cati_schema
    from shared_lib.persistence.db import DB

    from app.trading_intelligence.integration import cycle_shadow as cs
    from app.trading_intelligence.market_state.global_state_store import GlobalMarketStateStore

    db = DB(path=str(tmp_path / "c.db"))
    ensure_cati_schema(db)
    monkeypatch.delenv("CATI_GLOBAL_MARKET_STATE_ENABLED", raising=False)
    monkeypatch.delenv("CATI_CYCLE_SHADOW_ENABLED", raising=False)
    evs = [SimpleNamespace(market_state=s, opportunities=()) for s in states]
    result = SimpleNamespace(evaluations=evs, batch=SimpleNamespace(cycle_id="cyc", bot_instance_id="bot",
                                                                    broker_account_id="acc"))
    runner = SimpleNamespace(db=db)
    g = cs._global_market_state_stage(runner, result, states[0].decision_time, "15m")
    assert g is not None and GlobalMarketStateStore(db).latest()[0]["cycle_id"] == "cyc"
    monkeypatch.setenv("CATI_GLOBAL_MARKET_STATE_ENABLED", "off")  # operator override
    assert cs._global_market_state_stage(runner, result, states[0].decision_time, "15m") is None
    monkeypatch.delenv("CATI_GLOBAL_MARKET_STATE_ENABLED")
    assert cs._global_market_state_stage(SimpleNamespace(db=None), result, 1, "15m") is None  # swallowed, recorded


# ── cross-asset account risk ──────────────────────────────────────────────

def test_currency_factor_cap_blocks_one_oversized_currency_bet():
    ctx = context()
    cands = [ranked("EURUSD", 1.0, pos=1), ranked("EURGBP", 0.9, pos=2), ranked("EURJPY", 0.8, pos=3)]
    d = select(cands, ctx, slots=3, policy=dataclasses.replace(PortfolioPolicy(), lambda_beta=0.0))
    assert len(d.selected_opportunity_ids) == 2  # EUR +3 would breach the 2-unit cap
    capped = select(cands, ctx, slots=3, policy=dataclasses.replace(PortfolioPolicy(), lambda_beta=0.0,
                                                                    max_net_currency_units=None))
    assert len(capped.selected_opportunity_ids) == 3  # the cap is the only thing that changed


def test_shared_broker_account_exposure_aggregates_across_bots():
    ctx = context()
    # bot B on the SAME broker account already holds two short-USD legs
    existing = [held("EURUSD", bot="botB", key=fx("EURUSD")), held("GBPUSD", bot="botB", key=fx("GBPUSD"))]
    d = select([ranked("AUDUSD", 1.0, pos=1)], ctx, slots=1, existing=existing)
    assert d.selected_opportunity_ids == () and d.rejected_candidates[0].detail == "CURRENCY_FACTOR_CAP:USD"
    hedge = select([ranked("USDCHF", 1.0, pos=1)], ctx, slots=1, existing=existing)
    assert hedge.selected_opportunity_ids  # reducing the account's USD short is allowed
    crypto = select([ranked("BTCUSDT", 1.0, pos=1)], ctx, slots=1, existing=existing)
    assert crypto.selected_opportunity_ids  # a USDT-quoted crypto perp is not an FX USD leg


def test_logical_asset_class_allocation_is_a_risk_budget():
    ctx = context()
    pol = dataclasses.replace(PortfolioPolicy(), asset_class_max_positions=(("FX", 1),), lambda_beta=0.0)
    existing = [held("EURUSD", key=fx("EURUSD"))]
    d = select([ranked("USDJPY", 1.0, pos=1), ranked("BTCUSDT", 0.5, pos=2)], ctx, slots=2, existing=existing,
               policy=pol)
    assert d.selected_opportunity_ids == ("rk_BTCUSDT_LONG",)
    assert any(r.reason_code == "ASSET_CLASS_ALLOCATION_CAP" for r in d.rejected_candidates)
    assert PortfolioPolicy().policy_hash != pol.policy_hash  # allocations are part of the policy identity
