"""Sections 17-18 shadow flow through the REAL runner-facing wiring:

MarketSnapshot -> MarketState -> Regime -> Setup -> Forecast -> Venue economics
-> EconomicOpportunity -> Veto -> Rank -> Portfolio -> Reservation -> TradePlan
-> STOP (no hard risk, no order, no slot/margin/capital mutation)."""
from __future__ import annotations

import math
import time

import pytest
from _pf import gen, rows_from_returns
from _plan import binance_like_raw, fresh_db, run_pipeline, venue_evaluated

from app.runner.market_snapshot import MarketSnapshot
from app.trading_intelligence.contracts.ranking import SymbolEvalKind, SymbolEvaluation
from app.trading_intelligence.controller.cati_controller import CATIController
from app.trading_intelligence.integration import cycle_shadow as cs
from app.trading_intelligence.integration.venue_context import SAFE_CONTEXT_FIELDS, venue_context_from_runner
from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore


class _Ctx:
    """Bot context: touching any credential field fails the test."""

    bot_instance_id = "botA"
    user_id = "u1"
    broker_account_id = "acct1"
    broker_type = "binance"
    broker_environment = "demo"
    execution_mode = "paper"
    market_type = "CRYPTO"
    max_open_positions = 3

    @property
    def broker_api_key(self):
        raise AssertionError("Section 17 must never read the API key")

    @property
    def broker_api_secret(self):
        raise AssertionError("Section 17 must never read the API secret")

    @property
    def broker_base_url(self):
        raise AssertionError("Section 17 must never read broker connection details")


class _Client:
    """Existing-client stand-in exposing only public market data; any order path explodes."""

    def __init__(self, mid=100.0):
        self.mid, self.calls = mid, []

    def _raw(self, symbol):
        return binance_like_raw(symbol, mid=self.mid, t=int(time.time() * 1000) - 50).payloads

    def exchange_info_cached(self):
        self.calls.append("exchangeInfo")
        return {"serverTime": int(time.time() * 1000) - 60_000,
                "symbols": [self._raw(s)["exchange_info_symbol"] for s in ("BTCUSDT", "ETHUSDT")]}

    def book_ticker(self, s):
        self.calls.append("bookTicker")
        return self._raw(s)["book_ticker"]

    def mark_price(self, s):
        self.calls.append("premiumIndex")
        return self._raw(s)["premium_index"]

    def depth(self, s, limit=20):
        self.calls.append("depth")
        return self._raw(s)["depth"]

    def funding_info(self):
        return []

    def klines(self, symbol, interval, limit):
        return rows_from_returns(gen(hash(symbol) % 97))

    def place_order(self, *a, **k):
        raise AssertionError("CATI shadow must never place an order")

    place_market_order = close_position_market = set_leverage = place_order


def _trend_rows(start):
    closes = [100 + 0.5 * i + 6 * math.sin((i + 3) / (16 / (2 * math.pi))) for i in range(140)]
    rows = []
    for i, c in enumerate(closes):
        o = closes[i - 1] if i else c
        t = start + i * 900_000
        rows.append([t, o, max(o, c) + 0.6, min(o, c) - 0.6, c, 1000, t + 899_999, 0, 0, 0, 0, 0])
    return rows


class _Runner:
    def __init__(self, db=None, client=None, symbols=("BTCUSDT", "ETHUSDT")):
        self.context, self.client, self.db = _Ctx(), client or _Client(), db
        self.interval, self.trade_symbols, self.state = "15m", list(symbols), {}
        self._universe_open_symbols, self._universe_runtime, self.run_id, self.cycle_id = set(), object(), "r1", "c1"


@pytest.fixture(autouse=True)
def _clean():
    cs.reset_for_tests()
    yield
    cs.reset_for_tests()


# -- venue context from the runner -------------------------------------------------------------
def test_venue_context_reads_only_safe_fields_and_canonical_environment():
    ctx = venue_context_from_runner(_Runner(), "BTCUSDT", now_ms=int(time.time() * 1000))
    assert SAFE_CONTEXT_FIELDS == ("broker_type", "broker_environment", "user_id", "broker_account_id", "bot_instance_id")
    assert ctx.environment == "DEMO" and ctx.broker_account_id == "acct1" and ctx.adapter.adapter_id == "binance_usdm"
    assert not hasattr(ctx, "client") and "book_ticker" in ctx.raw.payloads


def test_unsupported_broker_context_fails_closed():
    r = _Runner()
    r.context.broker_type = "oanda"
    ctx = venue_context_from_runner(r, "BTCUSDT", now_ms=1)
    assert ctx.adapter.adapter_id == "unsupported" and ctx.raw.payloads == {}


def test_bybit_context_uses_the_versioned_policy_and_stays_unvalidated():
    # Sections 13-16: Bybit gained an economics adapter -- under its own versioned policy, never the frozen default,
    # and without validation evidence it remains UNVALIDATED (costs NOT_VIABLE, nothing admitted).
    from app.trading_intelligence.venue.policy import default_venue_cost_policy
    r = _Runner()
    r.context.broker_type = "bybit"
    ctx = venue_context_from_runner(r, "BTCUSDT", now_ms=1)
    assert ctx.adapter.adapter_id == "bybit_linear" and ctx.adapter.status_for("DEMO") == "UNVALIDATED"
    assert ctx.cost_policy.strict_required_components
    assert ctx.cost_policy.policy_hash != default_venue_cost_policy().policy_hash


# -- controller: Section 17 feeds the unchanged Section 13 engine ------------------------------
def test_controller_uses_venue_costs_lazily_and_keeps_account_identity():
    start = int(time.time() * 1000) // 900_000 * 900_000 - 140 * 900_000
    snapshot = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=_trend_rows(start), source="Test")
    runner = _Runner()
    made = []

    def factory():
        made.append(1)
        return venue_context_from_runner(runner, "BTCUSDT")

    ev = CATIController().evaluate_symbol(snapshot=snapshot, venue="BinanceFuturesClient", source="Test",
                                          user_id="u1", broker_account_id="acct1", bot_instance_id="botA",
                                          run_id="r1", cycle_id="c1", venue_context=factory)
    assert ev.kind == SymbolEvalKind.EVALUATED.value and made == [1]
    for o in ev.opportunities:
        assert o.venue_observation is not None and o.cost_estimate.venue_observation_id == o.venue_observation.observation_id
        assert o.cost_estimate.cost_scope == "ACCOUNT" and o.cost_estimate.environment == "DEMO"
        # no production library wired: fail-closed evidence, never a fabricated admission
        assert o.opportunity.admission_status == "INSUFFICIENT_EVIDENCE" and o.veto.outcome != "APPROVE_FOR_RANKING"


def test_no_candidates_means_no_venue_requests():
    rows = [[1_700_000_000_000 + i * 900_000, 100, 100.1, 99.9, 100, 1000, 1_700_000_000_000 + i * 900_000 + 899_999,
             0, 0, 0, 0, 0] for i in range(140)]
    snapshot = MarketSnapshot.build(symbol="BTCUSDT", timeframe="15m", candles=rows, source="Test")
    made = []
    ev = CATIController().evaluate_symbol(snapshot=snapshot, venue="v", source="Test",
                                          venue_context=lambda: made.append(1))
    assert ev.kind == SymbolEvalKind.NO_CANDIDATES.value and made == []


def test_record_symbol_wires_venue_context(monkeypatch):
    monkeypatch.setenv(cs.ENV_FLAG, "1")
    captured = {}

    class _Ctl:
        def evaluate_symbol(self, **kw):
            captured.update(kw)
            return SymbolEvaluation("BTCUSDT", SymbolEvalKind.NO_CANDIDATES.value)

    monkeypatch.setattr(cs, "_get_controller", lambda: _Ctl())
    import app.trading_intelligence.integration.context_adapters as ca

    monkeypatch.setattr(ca, "build_event_risk_context", lambda db, now, **kw: None)
    r = _Runner(symbols=("BTCUSDT",))
    cs.on_cycle_start(r)
    cs.record_symbol(r, "snap", "BTCUSDT", venue="BinanceFuturesClient", source="s")
    vctx = captured["venue_context"]()
    assert vctx.environment == "DEMO" and vctx.broker_account_id == "acct1"
    assert vctx.broker_health is captured["system_context"].broker_health


# -- the whole cycle: rank once -> portfolio -> reservation -> TradePlan -> STOP ------------------
def _drive_cycle(monkeypatch, db, evaluations):
    monkeypatch.setenv(cs.ENV_FLAG, "1")
    by_symbol = {e.candidate.instrument_key.venue_symbol: e for e in evaluations}

    class _Ctl:
        def evaluate_symbol(self, *, snapshot, **kw):
            e = by_symbol[snapshot]
            return SymbolEvaluation(snapshot, SymbolEvalKind.EVALUATED.value, opportunities=(e,))

    monkeypatch.setattr(cs, "_get_controller", lambda: _Ctl())
    import app.trading_intelligence.integration.context_adapters as ca

    monkeypatch.setattr(ca, "build_event_risk_context", lambda db, now, **kw: None)
    runner = _Runner(db=db, symbols=tuple(by_symbol))
    cs.on_cycle_start(runner)
    for sym in by_symbol:
        cs.record_symbol(runner, sym, sym, venue="BinanceFuturesClient", source="s")
    cs.on_cycle_end(runner, {})
    return runner


def _now_evaluations(**kw):
    now = int(time.time() * 1000)
    start = now - 60 * 900_000 - 30_000
    return [venue_evaluated("BTCUSDT", seed=999, start=start, **kw),
            venue_evaluated("ETHUSDT", seed=998, start=start + 1, **kw)]


def test_full_shadow_cycle_produces_trade_plan_evidence_and_stops(monkeypatch, tmp_path):
    db = fresh_db(tmp_path)
    evs = _now_evaluations()
    assert all(e.veto.outcome == "APPROVE_FOR_RANKING" for e in evs)
    with db.connect() as c:
        before = [c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("positions", "pending_entries")]
    runner = _drive_cycle(monkeypatch, db, evs)
    rows = TradePlanEvidenceStore(db).for_account("acct1")
    assert len(rows) == 2 and {r["mode"] for r in rows} == {"SHADOW"}
    with db.connect() as c:
        resv = c.execute("SELECT status FROM cati_portfolio_reservations").fetchall()
        after = [c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("positions", "pending_entries")]
    assert [r["status"] for r in resv] == ["RESERVED"]  # plan creation does not consume the reservation
    assert before == after  # zero orders, zero production slot consumption
    assert not any(c in ("place_order", "order") for c in runner.client.calls)


def test_real_watch_conditions_create_zero_trade_plans(monkeypatch, tmp_path):
    from app.trading_intelligence.contracts.veto import VetoPolicy

    db = fresh_db(tmp_path)
    evs = _now_evaluations(veto_policy=VetoPolicy())  # production policy: uncalibrated -> WATCH
    assert all(e.veto.outcome == "WATCH" for e in evs)
    _drive_cycle(monkeypatch, db, evs)
    assert TradePlanEvidenceStore(db).for_account("acct1") == []
    with db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM cati_portfolio_reservations").fetchone()[0] == 0


def test_trade_plan_stage_without_reservation_creates_nothing(tmp_path):
    import dataclasses

    db = fresh_db(tmp_path)
    p = run_pipeline(db, [venue_evaluated("BTCUSDT")])
    lost = dataclasses.replace(p["outcome"], decision=dataclasses.replace(p["outcome"].decision,
                                                                          reservation_status="CONFLICT"))
    assert cs.trade_plan_stage(db, p["result"], lost, p["evaluated"], p["now_ms"]) == []
    created = cs.trade_plan_stage(db, p["result"], p["outcome"], p["evaluated"], p["now_ms"])
    assert [r.status for r in created] == ["PLAN_CREATED"]
