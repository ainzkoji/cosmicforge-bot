"""Section 15.16 -- cross-universe ranking + cycle coordinator tests."""
from __future__ import annotations

import dataclasses
import inspect
import itertools

import pytest
from _helpers import build_library, make_evaluated, permissive_veto_policy

from app.trading_intelligence.contracts.ranking import RankingPolicy, SymbolEvalKind, SymbolEvaluation
from app.trading_intelligence.ranking import engine as ranking_engine
from app.trading_intelligence.ranking.coordinator import CATICycleCoordinator
from app.trading_intelligence.ranking.engine import normalize, rank_opportunities

SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")


@pytest.fixture(scope="module")
def libs():
    return {w: build_library(win_fraction=w) for w in (0.55, 0.7, 0.85, 0.95)}


@pytest.fixture(scope="module")
def universe(libs):
    """Four symbols, distinct economics (different historical win rates)."""
    wins = dict(zip(SYMBOLS, (0.55, 0.7, 0.85, 0.95)))
    return {s: make_evaluated(s, seed=100 + i, library=libs[w]) for i, (s, w) in enumerate(wins.items())}


def _all_approved(universe):
    return [e for e in universe.values() if e.approved]


def test_universe_fixture_is_meaningful(universe):
    approved = _all_approved(universe)
    assert len(approved) >= 2
    assert len({round(e.opportunity.conservative_edge_r, 6) for e in approved}) >= 2


def test_only_approved_are_ranked_watch_and_reject_excluded(universe):
    good = next(e for e in universe.values() if e.approved)
    strict = dataclasses.replace(permissive_veto_policy(), calibration_statuses_allowed_for_approval=("CALIBRATED",))
    watch = make_evaluated("ADAUSDT", seed=5, veto_policy=strict)
    reject = make_evaluated("DOGEUSDT", seed=6)
    reject = dataclasses.replace(reject, veto=dataclasses.replace(reject.veto, outcome="REJECT"))
    assert watch.veto.outcome == "WATCH" and not watch.approved
    ranked, excluded = rank_opportunities([good, watch, reject])
    assert [r.economic_opportunity_id for r in ranked] == [good.opportunity.economic_opportunity_id]
    assert set(excluded) == {watch.opportunity.economic_opportunity_id, reject.opportunity.economic_opportunity_id}


def test_ranking_is_deterministic_and_positions_are_dense(universe):
    ev = _all_approved(universe)
    a, _ = rank_opportunities(ev)
    b, _ = rank_opportunities(ev)
    assert [r.ranked_opportunity_id for r in a] == [r.ranked_opportunity_id for r in b]
    assert [r.rank_position for r in a] == list(range(1, len(a) + 1))
    assert all(a[i].rank_score >= a[i + 1].rank_score for i in range(len(a) - 1))


def test_symbol_iteration_order_invariance(universe):
    """The mandatory proof that first-come ordering cannot influence ranking."""
    ev = _all_approved(universe)
    reference, _ = rank_opportunities(ev)
    ref_view = [(r.setup_candidate_id, r.rank_position, r.rank_score) for r in reference]
    for perm in itertools.permutations(ev):
        ranked, _ = rank_opportunities(list(perm))
        assert [(r.setup_candidate_id, r.rank_position, r.rank_score) for r in ranked] == ref_view


def test_tie_break_is_deterministic_by_documented_sequence(universe):
    e = _all_approved(universe)[0]
    clone_b = dataclasses.replace(e, candidate=dataclasses.replace(e.candidate, setup_candidate_id="setc_zzz"))
    clone_a = dataclasses.replace(e, candidate=dataclasses.replace(e.candidate, setup_candidate_id="setc_aaa"))
    for order in ([clone_a, clone_b], [clone_b, clone_a]):
        ranked, _ = rank_opportunities(order)
        assert [r.setup_candidate_id for r in ranked] == ["setc_aaa", "setc_zzz"]
    assert RankingPolicy().tie_break_sequence[-1] == "setup_candidate_id_asc"


def _with_opp(e, **changes):
    return dataclasses.replace(e, opportunity=dataclasses.replace(e.opportunity, **changes))


def _score(e):
    return ranking_engine.score_components(e, RankingPolicy())["score"]


def test_monotonic_components(universe):
    e = _all_approved(universe)[0]
    base = _score(e)
    assert _score(_with_opp(e, conservative_edge_r=e.opportunity.conservative_edge_r + 0.2)) > base
    assert _score(_with_opp(e, lower_net_quantile_r=(e.opportunity.lower_net_quantile_r or 0.0) + 0.5)) > base
    assert _score(_with_opp(e, ess=e.opportunity.ess + 50)) >= base
    assert _score(_with_opp(e, ood_score=min(1.0, e.opportunity.ood_score + 0.3))) < base
    assert _score(_with_opp(e, forecast_uncertainty=min(1.0, e.opportunity.forecast_uncertainty + 0.3))) < base
    worse_cost = dataclasses.replace(e, cost_estimate=dataclasses.replace(e.cost_estimate, cost_uncertainty_R=0.5))
    assert _score(worse_cost) < base


def test_support_quality_increases_with_support(universe):
    e = _all_approved(universe)[0]
    p = RankingPolicy()
    lo = ranking_engine.support_quality(_with_opp(e, ess=2.0, raw_support=2), p)
    hi = ranking_engine.support_quality(_with_opp(e, ess=90.0, raw_support=190), p)
    assert hi > lo


def test_normalization_is_bounded_and_monotonic():
    xs = [-10, -1, 0, 0.3, 1, 10]
    vals = [normalize(x, -0.2, 1.0) for x in xs]
    assert vals == sorted(vals) and min(vals) == 0.0 and max(vals) == 1.0


def test_missing_liquidity_is_ranked_conservatively(universe):
    e = _all_approved(universe)[0]
    assert not e.market_state.liquidity_state.available
    p = RankingPolicy()
    assert ranking_engine.liquidity_quality(e, p) == p.missing_liquidity_quality < 0.5
    ranked, _ = rank_opportunities([e])
    assert "MISSING_LIQUIDITY_CONSERVATIVE" in ranked[0].reason_codes


def test_lower_tail_uses_net_quantile_cost_subtracted_once(universe):
    e = _all_approved(universe)[0]
    o = e.opportunity
    assert o.lower_net_quantile_r == pytest.approx(e.forecast.gross_R_lower_quantile - o.cost_r)


def _code_only(module) -> str:
    """Module source with every docstring removed (inspect code, not prose)."""
    import ast

    tree = ast.parse(inspect.getsource(module))
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.ClassDef)) and node.body                 and isinstance(node.body[0], ast.Expr) and isinstance(getattr(node.body[0], "value", None), ast.Constant)                 and isinstance(node.body[0].value.value, str):
            node.body = node.body[1:] or [ast.Pass()]
    return ast.unparse(tree).lower()


def test_ranking_never_uses_v2_confidence_or_setup_evidence_score():
    src = _code_only(ranking_engine)
    for term in ("evidence_score", "master_ensemble", "adaptiveentrythreshold", "raw_confidence", "p_net_profitable_mean"):
        assert term not in src, term
    for term in ("place_order", "reserve", "position_slots", "capital_ledger", "account_reservations", "app.execution"):
        assert term not in src, term


def test_score_ignores_setup_evidence_score(universe):
    e = _all_approved(universe)[0]
    hot = dataclasses.replace(e, candidate=dataclasses.replace(e.candidate, evidence_score=1.0))
    cold = dataclasses.replace(e, candidate=dataclasses.replace(e.candidate, evidence_score=0.0))
    assert _score(hot) == _score(cold)


# -- coordinator / batch completeness ---------------------------------------------------
def _begin(c, cycle="c1", symbols=SYMBOLS):
    return c.begin_bot_cycle(bot_instance_id="botA", cycle_id=cycle, cycle_time_ms=1, broker_account_id="acct",
                             user_id="u", universe_symbols=symbols)


def _ev(sym, universe):
    return SymbolEvaluation(sym, SymbolEvalKind.EVALUATED.value, opportunities=(universe[sym],))


def test_complete_batch_ranks_once_after_all_symbols_collected(universe):
    c = CATICycleCoordinator()
    key = _begin(c)
    for s in SYMBOLS:
        c.mark_due(key, s)
    for s in SYMBOLS:
        c.record_symbol_evaluation(key, _ev(s, universe))
        assert c.ranking_calls == 0  # nothing ranks inside the per-symbol callback
    res = c.finalize_bot_cycle(key)
    assert res.batch.batch_complete and res.ranking_performed and c.ranking_calls == 1
    assert {r.economic_opportunity_id for r in res.ranked} == set(res.batch.approved_opportunity_ids)
    assert c.finalize_bot_cycle(key) is res and c.ranking_calls == 1  # idempotent, never re-ranks


def test_incomplete_batch_refuses_ranking(universe):
    c = CATICycleCoordinator()
    key = _begin(c)
    for s in SYMBOLS:
        c.mark_due(key, s)
    for s in SYMBOLS[:-1]:  # last due symbol never reports a terminal state
        c.record_symbol_evaluation(key, _ev(s, universe))
    res = c.finalize_bot_cycle(key)
    assert not res.batch.batch_complete and not res.ranking_performed and res.ranked == ()
    assert SYMBOLS[-1] in res.batch.failed_instruments
    assert any("BATCH_INCOMPLETE" in r for r in res.batch.reason_codes) and c.ranking_calls == 0


def test_component_error_fails_closed_no_partial_ranking(universe):
    c = CATICycleCoordinator()
    key = _begin(c)
    for s in SYMBOLS[:-1]:
        c.record_symbol_evaluation(key, _ev(s, universe))
    c.record_symbol_failure(key, SYMBOLS[-1], "RuntimeError: boom")
    res = c.finalize_bot_cycle(key)
    assert not res.ranking_performed and res.ranked == () and SYMBOLS[-1] in res.batch.failed_instruments


def test_explicitly_benign_error_reasons_can_be_permitted(universe):
    c = CATICycleCoordinator(benign_error_reasons=("DELISTED",))
    key = _begin(c)
    for s in SYMBOLS[:-1]:
        c.record_symbol_evaluation(key, _ev(s, universe))
    c.record_symbol_failure(key, SYMBOLS[-1], "DELISTED")
    assert c.finalize_bot_cycle(key).ranking_performed


def test_not_due_symbols_are_excluded_from_the_due_set(universe):
    c = CATICycleCoordinator()
    key = _begin(c)
    c.mark_due(key, "BTCUSDT")
    c.record_symbol_evaluation(key, SymbolEvaluation("BTCUSDT", SymbolEvalKind.NOT_DUE.value))
    c.record_symbol_evaluation(key, _ev("ETHUSDT", universe))
    res = c.finalize_bot_cycle(key)
    assert res.batch.expected_due_instruments == ("ETHUSDT",) and res.batch.batch_complete


def test_watch_never_reaches_ranking_through_the_coordinator(universe):
    good = next(e for e in universe.values() if e.approved)
    bad = dataclasses.replace(good, veto=dataclasses.replace(good.veto, outcome="WATCH"))
    c = CATICycleCoordinator()
    key = _begin(c, symbols=("AAA", "BBB"))
    c.record_symbol_evaluation(key, SymbolEvaluation("AAA", "EVALUATED", (good,)))
    c.record_symbol_evaluation(key, SymbolEvaluation("BBB", "EVALUATED", (bad,)))
    res = c.finalize_bot_cycle(key)
    assert len(res.ranked) == 1 and bad.opportunity.economic_opportunity_id in res.batch.watch_ids


def test_coordinator_order_of_symbol_reports_does_not_change_result(universe):
    results = []
    for order in itertools.permutations(SYMBOLS):
        c = CATICycleCoordinator()
        key = _begin(c)
        for s in order:
            c.mark_due(key, s)
        for s in order:
            c.record_symbol_evaluation(key, _ev(s, universe))
        r = c.finalize_bot_cycle(key)
        results.append((tuple(x.setup_candidate_id for x in r.ranked), r.batch.cycle_batch_id))
    assert len({x[0] for x in results}) == 1 and len({x[1] for x in results}) == 1


def test_no_reservation_or_order_api_touched_during_ranking(universe, monkeypatch):
    """Spy every existing reservation surface: none may be called."""
    from app.execution import position_slots
    from app.risk import capital_ledger

    def boom(*a, **k):
        raise AssertionError("reservation surface touched by CATI ranking")

    monkeypatch.setattr(position_slots, "reserve_entry_slot", boom)
    monkeypatch.setattr(capital_ledger.ACCOUNT_RESERVATIONS, "reserve", boom)
    c = CATICycleCoordinator()
    key = _begin(c)
    for s in SYMBOLS:
        c.record_symbol_evaluation(key, _ev(s, universe))
    assert c.finalize_bot_cycle(key).ranking_performed
