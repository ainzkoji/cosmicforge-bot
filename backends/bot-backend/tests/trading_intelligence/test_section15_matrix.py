"""Closure item 10 -- the complete Section 15 matrix (numbered 1..20)."""
from __future__ import annotations

import ast
import dataclasses
import itertools
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from _helpers import build_library, make_evaluated

from app.trading_intelligence.contracts.ranking import SymbolEvalKind, SymbolEvaluation
from app.trading_intelligence.integration import cycle_shadow as cs
from app.trading_intelligence.ranking.coordinator import CATICycleCoordinator
from app.trading_intelligence.ranking.engine import rank_opportunities

BACKEND = Path(__file__).resolve().parents[2]
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")


@pytest.fixture(scope="module")
def universe():
    libs = {w: build_library(win_fraction=w) for w in (0.55, 0.7, 0.85, 0.95)}
    return {s: make_evaluated(s, seed=200 + i, library=libs[w]) for i, (s, w) in enumerate(zip(SYMBOLS, libs))}


def _begin(c, bot="botA", account="acct1", cycle="c1", symbols=SYMBOLS):
    return c.begin_bot_cycle(bot_instance_id=bot, cycle_id=cycle, cycle_time_ms=0, broker_account_id=account,
                             user_id="u", universe_symbols=symbols)


def _sev(sym, universe):
    return SymbolEvaluation(sym, SymbolEvalKind.EVALUATED.value, opportunities=(universe[sym],))


def _full_cycle(universe, order=SYMBOLS, **kw):
    c = CATICycleCoordinator()
    key = _begin(c, **kw)
    for s in order:
        c.mark_due(key, s)
    for s in order:
        c.record_symbol_evaluation(key, _sev(s, universe))
    return c, c.finalize_bot_cycle(key)


def _clone(ev, i, edge):
    """A distinct opportunity with a controlled conservative edge (large-batch test)."""
    opp = dataclasses.replace(ev.opportunity, economic_opportunity_id=f"eco_{i:05d}", conservative_edge_r=edge)
    cand = dataclasses.replace(ev.candidate, setup_candidate_id=f"cand_{i:05d}")
    veto = dataclasses.replace(ev.veto, veto_decision_id=f"veto_{i:05d}", economic_opportunity_id=opp.economic_opportunity_id)
    return dataclasses.replace(ev, opportunity=opp, candidate=cand, veto=veto)


# 1 --------------------------------------------------------------------------------------------
def test_01_universe_refresh_happens_before_new_entry_collection():
    src = (BACKEND / "app" / "runner" / "runner.py").read_text(encoding="utf-8")
    i_refresh = src.index("self._apply_universe()\n            from app.trading_intelligence.integration import cycle_shadow")
    i_start = src.index("_cati_cycle.on_cycle_start(self)")
    i_loop = src.index("for symbol in list(self.trade_symbols):", i_start)
    i_end = src.index("_cati_cycle.on_cycle_end(self, results, tuple(_deferred))")
    assert i_refresh < i_start < i_loop < i_end


# 2 --------------------------------------------------------------------------------------------
def _runner(symbols=("AAAUSDT", "BBBUSDT"), held=()):
    ctx = SimpleNamespace(bot_instance_id="bot1", user_id="u1", broker_account_id=None, max_open_positions=2,
                          market_type="CRYPTO", execution_mode="DEMO")
    return SimpleNamespace(context=ctx, interval="15m", trade_symbols=list(symbols), state={},
                           _universe_open_symbols=set(held), _universe_runtime=object(), run_id="r1", cycle_id="c1",
                           db=None, client=None)


@pytest.fixture
def shadow(monkeypatch):
    cs.reset_for_tests()
    monkeypatch.setenv(cs.ENV_FLAG, "1")
    yield
    cs.reset_for_tests()


def test_02_existing_position_management_stays_separate(shadow, monkeypatch):
    seen = []

    class Spy:
        def evaluate_symbol(self, *, snapshot, **kw):
            seen.append(snapshot)
            return SymbolEvaluation(snapshot, SymbolEvalKind.NO_CANDIDATES.value)

    monkeypatch.setattr(cs, "_get_controller", lambda: Spy())
    r = _runner(held=("BBBUSDT",))
    cs.on_cycle_start(r)
    cs.record_symbol(r, "BBBUSDT", "BBBUSDT", venue="v", source="s")  # held symbol: never a CATI candidate
    assert seen == [] and "BBBUSDT" not in cs._epochs["bot1"]["expected"]


# 3 --------------------------------------------------------------------------------------------
def test_03_every_due_candidate_is_registered(shadow):
    r = _runner(symbols=("AAAUSDT", "BBBUSDT", "CCCUSDT"), held=("CCCUSDT",))
    cs.on_cycle_start(r)
    assert cs._epochs["bot1"]["expected"] == {"AAAUSDT", "BBBUSDT"}
    cyc = cs._coordinator._open[cs._epochs["bot1"]["key"]]
    assert cyc.due == {"AAAUSDT", "BBBUSDT"}


def test_03b_runtime_wiring_passes_broker_health_and_event_context(shadow, monkeypatch):
    got = {}

    class Spy:
        def evaluate_symbol(self, **kw):
            got.update(kw)
            return SymbolEvaluation("AAAUSDT", SymbolEvalKind.NO_CANDIDATES.value)

    monkeypatch.setattr(cs, "_get_controller", lambda: Spy())
    r = _runner(symbols=("AAAUSDT",))
    cs.on_cycle_start(r)
    cs.record_symbol(r, "snap", "AAAUSDT", venue="v", source="s")
    bh = got["system_context"].broker_health
    assert bh is not None and bh.source.startswith("circuit_breaker_registry")  # wired, not omitted
    assert got["event_context"].source_state in ("AVAILABLE", "STALE", "UNAVAILABLE")


# 4, 5 ---------------------------------------------------------------------------------------------
def test_04_incomplete_batch_does_not_rank(universe):
    c = CATICycleCoordinator()
    key = _begin(c)
    for s in SYMBOLS:
        c.mark_due(key, s)
    for s in SYMBOLS[:2]:
        c.record_symbol_evaluation(key, _sev(s, universe))
    res = c.finalize_bot_cycle(key)
    assert res.ranked == () and not res.ranking_performed and c.ranking_calls == 0


def test_05_errored_batch_does_not_rank(universe):
    c = CATICycleCoordinator()
    key = _begin(c)
    for s in SYMBOLS[:-1]:
        c.record_symbol_evaluation(key, _sev(s, universe))
    c.record_symbol_failure(key, SYMBOLS[-1], "ValueError: bad")
    res = c.finalize_bot_cycle(key)
    assert res.ranked == () and not res.ranking_performed


# 6, 7, 8 ------------------------------------------------------------------------------------------
def test_06_all_approved_collected_before_rank(universe):
    c = CATICycleCoordinator()
    key = _begin(c)
    for s in SYMBOLS:
        c.record_symbol_evaluation(key, _sev(s, universe))
        assert c.ranking_calls == 0
    res = c.finalize_bot_cycle(key)
    approved = {e.opportunity.economic_opportunity_id for e in universe.values() if e.approved}
    assert {r.economic_opportunity_id for r in res.ranked} == approved and c.ranking_calls == 1


def test_07_08_watch_and_reject_excluded(universe):
    good = next(e for e in universe.values() if e.approved)
    watch = dataclasses.replace(good, veto=dataclasses.replace(good.veto, outcome="WATCH", veto_decision_id="w"))
    reject = dataclasses.replace(good, veto=dataclasses.replace(good.veto, outcome="REJECT", veto_decision_id="r"))
    ranked, excluded = rank_opportunities([good, watch, reject])
    assert len(ranked) == 1 and ranked[0].veto_decision_id == good.veto.veto_decision_id


# 9, 10, 16 -----------------------------------------------------------------------------------------
def test_09_16_order_independent_nobody_rewarded_for_arriving_first(universe):
    outs = {tuple(r.setup_candidate_id for r in _full_cycle(universe, order)[1].ranked)
            for order in itertools.permutations(SYMBOLS)}
    assert len(outs) == 1


def test_10_deterministic_across_repeated_runs(universe):
    a = _full_cycle(universe)[1]
    b = _full_cycle(universe)[1]
    assert [(r.ranked_opportunity_id, r.rank_score, r.rank_position) for r in a.ranked] == \
           [(r.ranked_opportunity_id, r.rank_score, r.rank_position) for r in b.ranked]


# 11, 12, 13 ----------------------------------------------------------------------------------------
_FORBIDDEN = ("app.execution", "app.exchange", "app.risk.capital_ledger", "reservation_store", "position_slots",
              "app.runner", "portfolio.service")


def test_11_12_13_ranker_cannot_reserve_submit_or_touch_slots():
    for rel in ("ranking/engine.py", "ranking/coordinator.py", "contracts/ranking.py"):
        tree = ast.parse((BACKEND / "app" / "trading_intelligence" / rel).read_text(encoding="utf-8"))
        mods = [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
        mods += [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names]
        assert not [m for m in mods if any(f in m for f in _FORBIDDEN)], (rel, mods)


def test_13_ranking_leaves_slot_and_margin_state_untouched(universe, monkeypatch):
    from app.execution import position_slots
    from app.risk import capital_ledger

    def boom(*a, **k):
        raise AssertionError("slot/margin/order surface touched by ranking")

    monkeypatch.setattr(position_slots, "reserve_entry_slot", boom)
    monkeypatch.setattr(capital_ledger.ACCOUNT_RESERVATIONS, "reserve", boom)
    assert _full_cycle(universe)[1].ranking_performed


# 14 ----------------------------------------------------------------------------------------------
def test_14_ties_are_deterministic(universe):
    base = next(e for e in universe.values() if e.approved)
    twins = [_clone(base, i, 0.3) for i in (3, 1, 2)]
    r1, _ = rank_opportunities(twins)
    r2, _ = rank_opportunities(twins[::-1])
    assert [r.setup_candidate_id for r in r1] == [r.setup_candidate_id for r in r2] == ["cand_00001", "cand_00002", "cand_00003"]


# 15 ----------------------------------------------------------------------------------------------
def test_15_normalization_is_policy_based_not_batch_relative(universe):
    approved = [e for e in universe.values() if e.approved]
    alone = {r.setup_candidate_id: r.rank_score for e in approved for r in rank_opportunities([e])[0]}
    together = {r.setup_candidate_id: r.rank_score for r in rank_opportunities(approved)[0]}
    assert alone == together


# 17, 18 --------------------------------------------------------------------------------------------
def test_17_zero_approved_returns_valid_empty_batch():
    c = CATICycleCoordinator()
    key = _begin(c, symbols=("AAA", "BBB"))
    c.record_symbol_evaluation(key, SymbolEvaluation("AAA", SymbolEvalKind.NO_CANDIDATES.value))
    c.record_symbol_evaluation(key, SymbolEvaluation("BBB", SymbolEvalKind.NO_CANDIDATES.value))
    res = c.finalize_bot_cycle(key)
    assert res.batch.batch_complete and res.ranked == () and res.batch.approved_opportunity_ids == ()


def test_18_one_candidate_works(universe):
    good = next(e for e in universe.values() if e.approved)
    ranked, _ = rank_opportunities([good])
    assert len(ranked) == 1 and ranked[0].rank_position == 1


# 19 ----------------------------------------------------------------------------------------------
def test_19_very_large_batch_is_bounded(universe):
    base = next(e for e in universe.values() if e.approved)
    batch = [_clone(base, i, -0.2 + (i % 997) / 997.0) for i in range(5000)]
    t = time.perf_counter()
    ranked, _ = rank_opportunities(batch)
    elapsed = time.perf_counter() - t
    assert len(ranked) == 5000 and elapsed < 10.0, elapsed
    assert [r.rank_position for r in ranked] == list(range(1, 5001))


# 20 ----------------------------------------------------------------------------------------------
def test_20_tenant_ids_never_leak_between_bot_batches(universe):
    c = CATICycleCoordinator()
    ka = _begin(c, bot="botA", account="acctA", cycle="cA", symbols=SYMBOLS[:2])
    kb = _begin(c, bot="botB", account="acctB", cycle="cB", symbols=SYMBOLS[2:])
    for s in SYMBOLS[:2]:
        c.record_symbol_evaluation(ka, _sev(s, universe))
    for s in SYMBOLS[2:]:
        c.record_symbol_evaluation(kb, _sev(s, universe))
    ra, rb = c.finalize_bot_cycle(ka), c.finalize_bot_cycle(kb)
    assert all(r.bot_instance_id == "botA" and r.broker_account_id == "acctA" for r in ra.ranked)
    assert all(r.bot_instance_id == "botB" and r.broker_account_id == "acctB" for r in rb.ranked)
    assert {r.instrument_key.venue_symbol for r in ra.ranked}.isdisjoint({r.instrument_key.venue_symbol for r in rb.ranked})


def test_closed_fx_session_opens_no_batch(shadow, monkeypatch):
    """Session awareness comes from the runtime's ForexSessionGuard -- no
    24/7 assumption for non-crypto markets."""
    from app.symbols.market_hours import ForexSessionGuard

    r = _runner(symbols=("EURUSD", "GBPUSD"))
    r.context.market_type = "FOREX"
    monkeypatch.setattr(ForexSessionGuard, "is_market_open", classmethod(lambda cls, ac, ts: False))
    cs.on_cycle_start(r)
    assert cs._epochs == {}
    monkeypatch.setattr(ForexSessionGuard, "is_market_open", classmethod(lambda cls, ac, ts: True))
    cs.on_cycle_start(r)
    assert cs._epochs["bot1"]["expected"] == {"EURUSD", "GBPUSD"}
