"""Section 16.31 -- portfolio intelligence + account reservation tests."""
from __future__ import annotations

import dataclasses
import inspect
import math
import random
import threading

import pytest
from _helpers import instrument

from app.trading_intelligence.contracts.exposure import AccountExposureSnapshot, ExposureRecord, ExposureStatus
from app.trading_intelligence.contracts.portfolio_intel import (
    ACCOUNT_RESERVATION_CONFLICT, DUPLICATE_EXPOSURE, PortfolioPolicy,
)
from app.trading_intelligence.contracts.ranking import RankedOpportunity
from app.trading_intelligence.portfolio import returns as R
from app.trading_intelligence.portfolio.context import build_portfolio_market_context
from app.trading_intelligence.portfolio.exposure_builder import build_account_exposure_snapshot
from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore
from app.trading_intelligence.portfolio.selector import select_portfolio
from app.trading_intelligence.portfolio.service import ShadowAccountPortfolioService

T0 = 1_700_000_000_000
BAR = 900_000
N = 300
NOW = T0 + N * BAR


def rows_from_returns(rets, start=T0):
    closes, price = [100.0], 100.0
    for r in rets:
        price *= math.exp(r)
        closes.append(price)
    return [[start + i * BAR, c, c, c, c, 1000.0, start + (i + 1) * BAR - 1] for i, c in enumerate(closes)]


def gen(seed, n=N, scale=0.01):
    rng = random.Random(seed)
    return [rng.gauss(0, scale) for _ in range(n)]


FACTOR = gen(1)
CORR_A = [f + e for f, e in zip(FACTOR, gen(2, scale=0.002))]  # ~ +1 vs factor
CORR_B = [f + e for f, e in zip(FACTOR, gen(3, scale=0.002))]  # ~ +1 vs A
INDEP = gen(4)


def context(policy=None, **series):
    policy = policy or PortfolioPolicy()
    rows = {k: rows_from_returns(v) for k, v in series.items()}
    factors = {"BTCUSDT": rows_from_returns(FACTOR)}
    return build_portfolio_market_context(rows, NOW, policy, factor_rows=factors)


def ranked(symbol, score, *, side="LONG", liq=0.8, pos=1, cid=None):
    inst = instrument(symbol)
    return RankedOpportunity(
        ranked_opportunity_id=f"rk_{symbol}_{side}", economic_opportunity_id=f"eco_{symbol}", veto_decision_id=f"v_{symbol}",
        setup_candidate_id=cid or f"cand_{symbol}_{side}", bot_instance_id="botA", broker_account_id="acct", cycle_id="c1",
        instrument_key=inst, side=side, setup_family="TREND_PULLBACK_V2", rank_score=score,
        normalized_conservative_edge=0.5, normalized_lower_tail=0.5, support_quality=0.5, liquidity_quality=liq,
        uncertainty_penalty_component=0.0, ood_penalty_component=0.0, execution_uncertainty_component=0.0,
        rank_position=pos, ranking_policy_version="1.0.0", ranking_policy_hash="h",
    )


def exposure(records=(), account="acct"):
    return AccountExposureSnapshot.build(broker_account_id=account, as_of_time=NOW, open_exposures=tuple(records))


def held(symbol, side="LONG", bot="botB", status=ExposureStatus.OPEN.value):
    return ExposureRecord(bot, instrument(symbol), side, 1.0, 100.0, 100.0, status)


def select(cands, ctx, *, slots=2, existing=(), policy=None):
    return select_portfolio(ranked=cands, exposure=exposure(existing), context=ctx, policy=policy or PortfolioPolicy(),
                            bot_instance_id="botA", cycle_id="c1", available_slots=slots, decision_time=NOW)


# =============================== CORRELATION ==================================
def test_ewma_correlation_deterministic_and_high_for_common_driver():
    a, b = R.log_returns_from_closes(rows_from_returns(CORR_A), NOW), R.log_returns_from_closes(rows_from_returns(CORR_B), NOW)
    lam = PortfolioPolicy().ewma_lambda
    r1 = R.ewma_correlation(a, b, ewma_lambda=lam, max_bars=500)
    assert r1 == R.ewma_correlation(a, b, ewma_lambda=lam, max_bars=500)
    assert r1[0] > 0.9 and r1[1] > 200


def test_enough_history_uses_ewma_and_insufficient_history_falls_back_recorded():
    p = PortfolioPolicy()
    ctx = context(p, BTCUSDT=CORR_A, ETHUSDT=CORR_B)
    est = R.pair_correlation("BTCUSDT", "ETHUSDT", ctx, p)
    assert est.source == "EWMA" and not est.fallback_used and est.rho_ewma > 0.9
    short = context(p, BTCUSDT=CORR_A[:20], ETHUSDT=CORR_B[:20])
    fb = R.pair_correlation("BTCUSDT", "ETHUSDT", short, p)
    assert fb.source == "STATIC_GROUP_FALLBACK" and fb.fallback_used and fb.rho_shrunk == p.static_group_correlation
    unknown = R.pair_correlation("BTCUSDT", "XYZUSDT", short, p)
    # an unknown static group is never a silent 0: conservative, recorded fallback
    assert unknown.rho_shrunk == p.unknown_group_correlation > 0 and unknown.fallback_used
    assert "NO_STATIC_GROUP_AVAILABLE" in unknown.reason_codes


def test_shrinkage_is_exact():
    assert R.shrink_correlation(0.8, shrinkage_lambda=0.25, target=0.0) == pytest.approx(0.6)
    assert R.shrink_correlation(-0.4, shrinkage_lambda=0.5, target=0.2) == pytest.approx(-0.1)
    p = PortfolioPolicy()
    ctx = context(p, BTCUSDT=CORR_A, ETHUSDT=CORR_B)
    est = R.pair_correlation("BTCUSDT", "ETHUSDT", ctx, p)
    assert est.rho_shrunk == pytest.approx((1 - p.correlation_shrinkage_lambda) * est.rho_ewma + p.correlation_shrinkage_lambda * p.correlation_target)


def test_side_adjustment_and_long_short_semantics():
    p = PortfolioPolicy()
    est = R.pair_correlation("BTCUSDT", "ETHUSDT", context(p, BTCUSDT=CORR_A, ETHUSDT=CORR_B), p)
    assert R.effective_correlation(est, "LONG", "LONG") == pytest.approx(est.rho_shrunk)
    assert R.effective_correlation(est, "LONG", "SHORT") == pytest.approx(-est.rho_shrunk)
    assert R.effective_correlation(est, "SHORT", "SHORT") == pytest.approx(est.rho_shrunk)
    assert R.side_sign("LONG") == 1 and R.side_sign("SHORT") == -1


def test_future_returns_are_excluded_from_context():
    p = PortfolioPolicy()
    full = build_portfolio_market_context({"BTCUSDT": rows_from_returns(CORR_A)}, T0 + 150 * BAR, p)
    cut = build_portfolio_market_context({"BTCUSDT": rows_from_returns(CORR_A[:149])}, T0 + 150 * BAR, p)
    assert full.return_histories == cut.return_histories and full.context_hash == cut.context_hash
    assert max(t for t, _ in full.return_histories["BTCUSDT"]) <= T0 + 150 * BAR


# ================================== BETA ======================================
def test_beta_deterministic_and_close_to_truth():
    asset = [1.5 * f + e for f, e in zip(FACTOR, gen(9, scale=0.001))]
    p = PortfolioPolicy()
    ctx = context(p, SOLUSDT=asset)
    b1, b2 = R.factor_beta("SOLUSDT", "CRYPTO:MARKET:BTC", ctx, p), R.factor_beta("SOLUSDT", "CRYPTO:MARKET:BTC", ctx, p)
    assert b1 == b2 and b1.quality == "OK" and b1.beta == pytest.approx(1.5, abs=0.15)


def test_near_zero_factor_variance_is_safe_and_unknown():
    p = PortfolioPolicy()
    flat = build_portfolio_market_context({"SOLUSDT": rows_from_returns(gen(5))}, NOW, p,
                                          factor_rows={"BTCUSDT": rows_from_returns([0.0] * N)})
    est = R.factor_beta("SOLUSDT", "CRYPTO:MARKET:BTC", flat, p)
    assert est.beta is None and est.quality == "ZERO_VARIANCE"


def test_missing_factor_and_history_remain_missing_not_zero():
    p = PortfolioPolicy()
    ctx = context(p, SOLUSDT=gen(5))
    assert R.factor_beta("SOLUSDT", "CRYPTO:ALT_MARKET:ETH", ctx, p) == R.BetaEstimate("SOLUSDT", "CRYPTO:ALT_MARKET:ETH", None, 0, "FACTOR_MISSING")
    assert R.factor_beta("XRPUSDT", "CRYPTO:MARKET:BTC", ctx, p).beta is None
    short = context(p, SOLUSDT=gen(5)[:10])
    assert R.factor_beta("SOLUSDT", "CRYPTO:MARKET:BTC", short, p).beta is None


# ============================== SELECTION =====================================
def test_exact_enumeration_slot_counts_and_empty_set_possible():
    ctx = context(BTCUSDT=INDEP, ETHUSDT=gen(11), SOLUSDT=gen(12))
    cands = [ranked("BTCUSDT", 0.9, pos=1), ranked("ETHUSDT", 0.8, pos=2), ranked("SOLUSDT", 0.7, pos=3)]
    for slots in (1, 2, 3):
        d = select(cands, ctx, slots=slots)
        assert d.solver == "EXACT" and len(d.selected_opportunity_ids) == slots
    negative = [ranked("BTCUSDT", -0.3, pos=1), ranked("ETHUSDT", -0.1, pos=2)]
    empty = select(negative, ctx, slots=2)
    assert empty.selected_opportunity_ids == () and empty.portfolio_score == 0.0  # free slots never force a pick


def test_best_feasible_pair_is_chosen_not_first_two_ranked():
    p = PortfolioPolicy()
    ctx = context(p, BTCUSDT=CORR_A, ETHUSDT=CORR_B, SOLUSDT=INDEP)  # A~B ~ +1 correlated, SOL independent
    cands = [ranked("BTCUSDT", 0.90, pos=1), ranked("ETHUSDT", 0.88, pos=2), ranked("SOLUSDT", 0.80, pos=3)]
    d = select(cands, ctx, slots=2, policy=dataclasses.replace(p, lambda_corr=2.0))
    chosen = set(d.selected_opportunity_ids)
    assert "rk_SOLUSDT_LONG" in chosen and chosen != {"rk_BTCUSDT_LONG", "rk_ETHUSDT_LONG"}
    assert d.score_breakdown.raw_rank_sum > 0


def test_long_long_positive_correlation_penalized_but_long_short_offsets():
    p = dataclasses.replace(PortfolioPolicy(), lambda_beta=0.0)
    ctx = context(p, BTCUSDT=CORR_A, ETHUSDT=CORR_B)
    ll = select([ranked("BTCUSDT", 1.0, pos=1), ranked("ETHUSDT", 1.0, pos=2)], ctx, slots=2, policy=p)
    ls_cands = [ranked("BTCUSDT", 1.0, pos=1), ranked("ETHUSDT", 1.0, side="SHORT", pos=2)]
    ls = select(ls_cands, ctx, slots=2, policy=p)
    both_ll = select_portfolio  # noqa: F841
    assert ls.score_breakdown.correlation_penalty == 0.0
    # forcing both LONG names into one set shows a positive compounded penalty
    forced = select([ranked("BTCUSDT", 1.0, pos=1), ranked("ETHUSDT", 1.0, pos=2)], ctx, slots=2,
                    policy=dataclasses.replace(p, lambda_corr=0.0))
    assert forced.score_breakdown.correlation_penalty > 0.0
    assert len(ls.selected_opportunity_ids) == 2  # the offsetting pair is happily selected
    assert ll.score_breakdown.correlation_penalty <= forced.score_breakdown.correlation_penalty


def test_duplicate_position_rejected_and_existing_position_influences_selection():
    ctx = context(BTCUSDT=INDEP, ETHUSDT=gen(21))
    d = select([ranked("BTCUSDT", 1.0, pos=1), ranked("ETHUSDT", 0.5, pos=2)], ctx, slots=2,
               existing=[held("BTCUSDT")])
    reasons = {r.setup_candidate_id: r.reason_code for r in d.rejected_candidates}
    assert reasons["cand_BTCUSDT_LONG"] == DUPLICATE_EXPOSURE
    assert d.selected_opportunity_ids == ("rk_ETHUSDT_LONG",)
    p = PortfolioPolicy()
    cctx = context(p, BTCUSDT=CORR_A, ETHUSDT=CORR_B, SOLUSDT=INDEP)
    base = select([ranked("ETHUSDT", 0.6, pos=1), ranked("SOLUSDT", 0.55, pos=2)], cctx, slots=1,
                  policy=dataclasses.replace(p, lambda_corr=3.0))
    with_existing = select([ranked("ETHUSDT", 0.6, pos=1), ranked("SOLUSDT", 0.55, pos=2)], cctx, slots=1,
                           existing=[held("BTCUSDT")], policy=dataclasses.replace(p, lambda_corr=3.0))
    assert base.selected_opportunity_ids != with_existing.selected_opportunity_ids  # BTC already held => prefer SOL


def test_hedge_duplicate_allowed_only_when_policy_permits():
    ctx = context(BTCUSDT=INDEP)
    strict = select([ranked("BTCUSDT", 1.0, side="SHORT")], ctx, slots=1, existing=[held("BTCUSDT", "LONG")])
    assert strict.selected_opportunity_ids == ()
    hedge = select([ranked("BTCUSDT", 1.0, side="SHORT")], ctx, slots=1, existing=[held("BTCUSDT", "LONG")],
                   policy=dataclasses.replace(PortfolioPolicy(), allow_hedge_mode_duplicates=True))
    assert hedge.selected_opportunity_ids == ("rk_BTCUSDT_SHORT",)


def test_common_factor_concentration_penalty():
    p = dataclasses.replace(PortfolioPolicy(), lambda_corr=0.0, factor_tolerance_units=0.5, lambda_beta=1.0)
    hi_beta = [1.8 * f + e for f, e in zip(FACTOR, gen(31, scale=0.001))]
    hi_beta2 = [1.8 * f + e for f, e in zip(FACTOR, gen(32, scale=0.001))]
    ctx = context(p, SOLUSDT=hi_beta, XRPUSDT=hi_beta2)
    d = select([ranked("SOLUSDT", 1.0, pos=1), ranked("XRPUSDT", 1.0, pos=2)], ctx, slots=2, policy=p)
    forced = select([ranked("SOLUSDT", 1.0, pos=1), ranked("XRPUSDT", 1.0, pos=2)], ctx, slots=2,
                    policy=dataclasses.replace(p, lambda_beta=0.0))
    assert forced.score_breakdown.factor_penalty > 0.0
    assert len(d.selected_opportunity_ids) < 2 or d.score_breakdown.factor_penalty > 0.0


def test_missing_beta_uses_recorded_conservative_fallback_not_zero():
    p = dataclasses.replace(PortfolioPolicy(), factor_tolerance_units=0.5)
    ctx = context(p, XYZUSDT=INDEP[:10], ABCUSDT=INDEP[:10])  # too short => beta unknown
    d = select([ranked("XYZUSDT", 1.0, pos=1), ranked("ABCUSDT", 1.0, pos=2)], ctx, slots=2, policy=p)
    assert "BETA_UNAVAILABLE_CONSERVATIVE_FALLBACK" in d.reason_codes
    assert any(f.startswith("BETA_UNAVAILABLE_CONSERVATIVE_FALLBACK") for f in d.score_breakdown.fallbacks_used)


def test_sector_concentration_penalty_uses_known_groups_only():
    p = dataclasses.replace(PortfolioPolicy(), lambda_corr=0.0, lambda_beta=0.0, sector_tolerance_units=1.0, lambda_sector=1.0)
    ctx = context(p, BTCUSDT=gen(41), ETHUSDT=gen(42), XYZUSDT=gen(43), ABCUSDT=gen(44))
    same = select([ranked("BTCUSDT", 1.0, pos=1), ranked("ETHUSDT", 1.0, pos=2)], ctx, slots=2,
                  policy=dataclasses.replace(p, lambda_sector=0.0))
    assert same.score_breakdown.sector_penalty > 0.0  # BTC+ETH share STATIC_GROUP_0, both LONG
    unknown = select([ranked("XYZUSDT", 1.0, pos=1), ranked("ABCUSDT", 1.0, pos=2)], ctx, slots=2, policy=p)
    assert unknown.score_breakdown.sector_penalty == 0.0  # unknown stays unknown: no false precision


def test_liquidity_concentration_penalty():
    p = dataclasses.replace(PortfolioPolicy(), lambda_corr=0.0, lambda_beta=0.0, lambda_sector=0.0, lambda_liq=1.0,
                            liquidity_low_tolerance=1)
    ctx = context(p, XYZUSDT=gen(51), ABCUSDT=gen(52), QRSUSDT=gen(53))
    cands = [ranked(s, 1.0, liq=0.1, pos=i + 1) for i, s in enumerate(("XYZUSDT", "ABCUSDT", "QRSUSDT"))]
    forced = select(cands, ctx, slots=3, policy=dataclasses.replace(p, lambda_liq=0.0))
    assert forced.score_breakdown.liquidity_penalty == 2.0
    d = select(cands, ctx, slots=3, policy=p)
    assert d.score_breakdown.liquidity_penalty <= 1.0


def test_no_available_slots_yields_no_selection():
    d = select([ranked("BTCUSDT", 1.0)], context(BTCUSDT=INDEP), slots=0)
    assert d.selected_opportunity_ids == () and d.solver == "NONE" and "NO_AVAILABLE_SLOTS" in d.reason_codes


def test_beam_solver_for_large_slot_counts_is_deterministic():
    syms = ["S%dUSDT" % i for i in range(8)]
    ctx = context(**{s: gen(60 + i) for i, s in enumerate(syms)})
    cands = [ranked(s, 1.0 - i * 0.05, pos=i + 1) for i, s in enumerate(syms)]
    p = dataclasses.replace(PortfolioPolicy(), exact_enumeration_max_slots=2)
    a, b = select(cands, ctx, slots=5, policy=p), select(cands, ctx, slots=5, policy=p)
    assert a.solver == "BEAM" and a.selected_opportunity_ids == b.selected_opportunity_ids and len(a.selected_opportunity_ids) == 5


def test_selection_is_deterministic_and_input_order_invariant():
    p = PortfolioPolicy()
    ctx = context(p, BTCUSDT=CORR_A, ETHUSDT=CORR_B, SOLUSDT=INDEP)
    cands = [ranked("BTCUSDT", 0.9, pos=1), ranked("ETHUSDT", 0.8, pos=2), ranked("SOLUSDT", 0.7, pos=3)]
    ids = {select(list(c), ctx, slots=2).portfolio_selection_id for c in (cands, cands[::-1])}
    assert len(ids) == 1
    assert select(cands, ctx, slots=2).selected_opportunity_ids == select(cands[::-1], ctx, slots=2).selected_opportunity_ids


# ============================ DB-BACKED EXPOSURE ==============================
@pytest.fixture
def tdb(tmp_path):
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    db = DB(str(tmp_path / "cati.db"))
    migrate(db)
    return db


def add_bot(db, bot_id, account, broker="binance"):
    with db.connect() as c:
        c.execute("INSERT OR IGNORE INTO broker_accounts (id, user_id, broker_id, market_type, status, created_at, updated_at)"
                  " VALUES (?,?,?,?,?,?,?)", (account, "u1", broker, "crypto", "active", "2026-01-01", "2026-01-01"))
        c.execute("INSERT INTO bot_instances (id, user_id, broker_account_id, market_type, strategy_id, mode, status,"
                  " created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                  (bot_id, "u1", account, "crypto", "master_ensemble", "paper", "active", "2026-01-01", "2026-01-01"))


def add_position(db, bot, symbol, side="LONG", qty=1.0, px=100.0):
    with db.connect() as c:
        c.execute("INSERT INTO positions (position_id, bot_instance_id, symbol, side, original_qty, remaining_qty, entry_price, status, opened_at)"
                  " VALUES (?,?,?,?,?,?,?,?,?)", (f"p_{bot}_{symbol}", bot, symbol, side, qty, qty, px, "OPEN", "2026-01-01"))


def add_pending(db, bot, symbol, side="LONG"):
    with db.connect() as c:
        c.execute("INSERT INTO pending_entries (bot_id, symbol, side, client_order_id, state, intended_notional) VALUES (?,?,?,?,?,?)",
                  (bot, symbol, side, f"cid_{bot}_{symbol}", "PENDING_OPEN", 100.0))


def test_exposure_aggregates_all_bots_sharing_one_broker_account_and_isolates_others(tdb):
    for bot, acct in (("botA", "acct1"), ("botB", "acct1"), ("botC", "acct2")):
        add_bot(tdb, bot, acct)
    add_position(tdb, "botA", "BTCUSDT")
    add_position(tdb, "botB", "ETHUSDT", "SHORT")
    add_position(tdb, "botC", "SOLUSDT")
    snap = build_account_exposure_snapshot(tdb, "acct1", NOW)
    assert {(r.bot_instance_id, r.instrument_key.venue_symbol, r.side) for r in snap.open_exposures} == {
        ("botA", "BTCUSDT", "LONG"), ("botB", "ETHUSDT", "SHORT")}
    other = build_account_exposure_snapshot(tdb, "acct2", NOW)
    assert {r.instrument_key.venue_symbol for r in other.open_exposures} == {"SOLUSDT"}


def test_pending_entries_and_shadow_reservations_are_included(tdb):
    add_bot(tdb, "botA", "acct1"); add_bot(tdb, "botB", "acct1")
    add_pending(tdb, "botB", "XRPUSDT")
    store = CATIReservationStore(tdb)
    out = store.reserve(broker_account_id="acct1", bot_instance_id="botA", cycle_id="c1",
                        selected=[("cand1", "SOL/USDT:PERP", "binance", "SOLUSDT", "LONG")], now_ms=NOW, ttl_seconds=600)
    assert out.reserved
    snap = build_account_exposure_snapshot(tdb, "acct1", NOW, reservation_store=store)
    assert [r.instrument_key.venue_symbol for r in snap.pending_exposures] == ["XRPUSDT"]
    assert [r.exposure_status for r in snap.reservation_exposures] == [ExposureStatus.SHADOW_RESERVED.value]
    assert snap.reservation_exposures[0].instrument_key.canonical_symbol == "SOL/USDT:PERP"


def test_confirmed_entry_is_not_double_counted(tdb):
    add_bot(tdb, "botA", "acct1")
    add_position(tdb, "botA", "BTCUSDT")
    add_pending(tdb, "botA", "BTCUSDT")
    snap = build_account_exposure_snapshot(tdb, "acct1", NOW)
    assert len(snap.open_exposures) == 1 and snap.pending_exposures == ()


def test_bot_slots_come_from_effective_policy_not_a_hardcoded_global(tdb):
    add_bot(tdb, "botA", "acct1")
    add_position(tdb, "botA", "BTCUSDT")
    add_pending(tdb, "botA", "ETHUSDT")
    svc = ShadowAccountPortfolioService(tdb)
    assert svc.available_slots("botA", 5) == 3
    assert svc.available_slots("botA", 2) == 0
    # an unrealised own CATI reservation also consumes a slot
    svc.store.reserve(broker_account_id="acct1", bot_instance_id="botA", cycle_id="c1",
                      selected=[("cand_sol", "SOL/USDT:PERP", "binance", "SOLUSDT", "LONG")], now_ms=NOW, ttl_seconds=900)
    assert svc.available_slots("botA", 3, NOW, "acct1") == 0
    assert "max_open_positions=2" not in inspect.getsource(ShadowAccountPortfolioService)


# ============================ RESERVATION / SERVICE ===========================
SEL_BTC = [("cand_btc", "BTC/USDT:PERP", "binance", "BTCUSDT", "LONG")]


def reserve(store, bot, acct="acct1", cycle="c1", selected=SEL_BTC, now=NOW, ttl=600):
    return store.reserve(broker_account_id=acct, bot_instance_id=bot, cycle_id=cycle, selected=selected, now_ms=now, ttl_seconds=ttl)


def test_reservation_persists_versioned_shadow_contract(tdb):
    store = CATIReservationStore(tdb)
    out = reserve(store, "botA")
    r = out.reservation
    assert out.reserved and r.status == "RESERVED" and r.mode == "SHADOW" and r.reservation_version
    assert r.selected_candidate_ids == ("cand_btc",) and r.expires_at == NOW + 600_000 and r.bot_instance_id == "botA"
    assert store.get(r.reservation_id) == r
    assert reserve(store, "botA").reservation.reservation_id == r.reservation_id  # deterministic, idempotent


def test_two_bots_same_account_only_one_reservation_succeeds(tdb):
    store = CATIReservationStore(tdb)
    assert reserve(store, "botA").reserved
    second = reserve(store, "botB", cycle="c9")
    assert not second.reserved and second.conflict_reason == ACCOUNT_RESERVATION_CONFLICT


def test_different_broker_accounts_remain_isolated(tdb):
    store = CATIReservationStore(tdb)
    assert reserve(store, "botA", acct="acct1").reserved
    assert reserve(store, "botZ", acct="acct2").reserved


def test_existing_open_position_blocks_reservation_as_duplicate_exposure(tdb):
    add_bot(tdb, "botB", "acct1")
    add_position(tdb, "botB", "BTCUSDT")
    out = reserve(CATIReservationStore(tdb), "botA")
    assert not out.reserved and out.conflict_reason == DUPLICATE_EXPOSURE


def test_concurrent_reservations_race_exactly_one_wins(tdb):
    store = CATIReservationStore(tdb)
    results, barrier = [], threading.Barrier(6)

    def worker(i):
        s = CATIReservationStore(tdb)
        barrier.wait()
        results.append(reserve(s, f"bot{i}", cycle=f"c{i}").reserved)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    [t.start() for t in threads]; [t.join() for t in threads]
    assert sorted(results).count(True) == 1 and len(store.active_reservations("acct1", NOW)) == 1


def test_release_expire_consume_lifecycle_and_expired_no_longer_blocks(tdb):
    store = CATIReservationStore(tdb)
    rid = reserve(store, "botA").reservation.reservation_id
    assert store.release(rid, NOW + 1) and store.get(rid).status == "RELEASED"
    assert not store.release(rid, NOW + 2)
    assert reserve(store, "botB", cycle="c2").reserved  # released reservation frees the instrument
    rid2 = store.active_reservations("acct1", NOW)[0].reservation_id
    assert store.expire_stale("acct1", NOW + 601_000) == 1 and store.get(rid2).status == "EXPIRED"
    assert reserve(store, "botC", cycle="c3", now=NOW + 700_000).reserved
    rid3 = store.active_reservations("acct1", NOW + 700_000)[0].reservation_id
    assert store.consume(rid3, NOW + 700_001) and store.get(rid3).status == "CONSUMED"


def test_only_shadow_mode_is_permitted(tdb):
    with pytest.raises(ValueError):
        CATIReservationStore(tdb).reserve(broker_account_id="a", bot_instance_id="b", cycle_id="c", selected=SEL_BTC,
                                          now_ms=NOW, ttl_seconds=10, mode="ACTIVE")


def test_service_full_flow_second_bot_sees_conflict_and_no_substitution(tdb):
    add_bot(tdb, "botA", "acct1"); add_bot(tdb, "botB", "acct1")
    svc = ShadowAccountPortfolioService(tdb)
    ctx = context(BTCUSDT=INDEP, XYZUSDT=gen(71))
    out_a = svc.select_and_reserve(ranked=[ranked("BTCUSDT", 1.0)], broker_account_id="acct1", bot_instance_id="botA",
                                   cycle_id="c1", max_open_positions=2, context=ctx, now_ms=NOW)
    assert out_a.decision.is_reserved and out_a.reservation.status == "RESERVED"
    out_b = svc.select_and_reserve(ranked=[ranked("BTCUSDT", 1.0, cid="candB"), ranked("XYZUSDT", 0.9, pos=2)],
                                   broker_account_id="acct1", bot_instance_id="botB", cycle_id="c1", max_open_positions=2,
                                   context=ctx, now_ms=NOW)
    reasons = {r.setup_candidate_id: r.reason_code for r in out_b.decision.rejected_candidates}
    assert reasons["candB"] == ACCOUNT_RESERVATION_CONFLICT
    assert out_b.decision.selected_opportunity_ids == ("rk_XYZUSDT_LONG",)  # a genuinely different feasible choice, not a substitute after conflict


def test_conflict_at_reserve_time_reports_conflict_without_substituting(tdb, monkeypatch):
    add_bot(tdb, "botA", "acct1")
    svc = ShadowAccountPortfolioService(tdb)
    ctx = context(BTCUSDT=INDEP, ETHUSDT=gen(72))
    real_reserve = svc.store.reserve

    def racing_reserve(**kw):  # another bot wins the race between snapshot and reserve
        real_reserve(broker_account_id=kw["broker_account_id"], bot_instance_id="botX", cycle_id="rival", selected=SEL_BTC,
                     now_ms=kw["now_ms"], ttl_seconds=600)
        return real_reserve(**kw)

    monkeypatch.setattr(svc.store, "reserve", racing_reserve)
    out = svc.select_and_reserve(ranked=[ranked("BTCUSDT", 1.0), ranked("ETHUSDT", 0.9, pos=2)], broker_account_id="acct1",
                                 bot_instance_id="botA", cycle_id="c1", max_open_positions=1, context=ctx, now_ms=NOW)
    assert out.decision.reservation_status == "CONFLICT" and not out.decision.is_reserved
    assert ACCOUNT_RESERVATION_CONFLICT in out.decision.reason_codes
    assert out.decision.selected_opportunity_ids == ("rk_BTCUSDT_LONG",)  # NOT silently swapped to rank #2
    assert out.reservation is None


def test_hard_risk_rejection_releases_reservation_without_substitution(tdb):
    add_bot(tdb, "botA", "acct1")
    svc = ShadowAccountPortfolioService(tdb)
    ctx = context(BTCUSDT=INDEP, ETHUSDT=gen(73))
    out = svc.select_and_reserve(ranked=[ranked("BTCUSDT", 1.0), ranked("ETHUSDT", 0.9, pos=2)], broker_account_id="acct1",
                                 bot_instance_id="botA", cycle_id="c1", max_open_positions=1, context=ctx, now_ms=NOW)
    assert svc.release(out.reservation.reservation_id, NOW + 1)
    assert svc.store.active_reservations("acct1", NOW + 2) == []  # nothing auto-reserved for rank #2


def test_portfolio_stage_veto_emitted_for_duplicate_exposure(tdb):
    from _helpers import make_evaluated

    add_bot(tdb, "botB", "acct1"); add_bot(tdb, "botA", "acct1")
    add_position(tdb, "botB", "BTCUSDT")
    ev = make_evaluated("BTCUSDT", seed=77)
    rk = dataclasses.replace(ranked("BTCUSDT", 1.0), setup_candidate_id=ev.candidate.setup_candidate_id,
                             instrument_key=ev.candidate.instrument_key)
    svc = ShadowAccountPortfolioService(tdb)
    out = svc.select_and_reserve(ranked=[rk], broker_account_id="acct1", bot_instance_id="botA", cycle_id="c1",
                                 max_open_positions=2, context=context(BTCUSDT=INDEP), now_ms=NOW,
                                 evaluated_by_candidate_id={ev.candidate.setup_candidate_id: ev})
    assert out.portfolio_vetoes and out.portfolio_vetoes[0].stage == "PORTFOLIO_STAGE"
    assert out.portfolio_vetoes[0].outcome == "REJECT" and DUPLICATE_EXPOSURE in out.portfolio_vetoes[0].reason_codes


def test_service_touches_no_production_reservation_slot_capital_or_order_surface(tdb, monkeypatch):
    from app.execution import position_slots
    from app.risk import capital_ledger

    def boom(*a, **k):
        raise AssertionError("production authority touched by CATI portfolio service")

    monkeypatch.setattr(position_slots, "reserve_entry_slot", boom)
    monkeypatch.setattr(capital_ledger.ACCOUNT_RESERVATIONS, "reserve", boom)
    add_bot(tdb, "botA", "acct1")
    with tdb.connect() as c:
        before = (c.execute("SELECT COUNT(*) FROM positions").fetchone()[0], c.execute("SELECT COUNT(*) FROM pending_entries").fetchone()[0])
    svc = ShadowAccountPortfolioService(tdb)
    out = svc.select_and_reserve(ranked=[ranked("BTCUSDT", 1.0)], broker_account_id="acct1", bot_instance_id="botA",
                                 cycle_id="c1", max_open_positions=2, context=context(BTCUSDT=INDEP), now_ms=NOW)
    assert out.decision.is_reserved
    with tdb.connect() as c:
        after = (c.execute("SELECT COUNT(*) FROM positions").fetchone()[0], c.execute("SELECT COUNT(*) FROM pending_entries").fetchone()[0])
    assert before == after


def test_production_code_never_consults_cati_shadow_reservations():
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[2] / "app"
    offenders = []
    for sub in ("execution", "risk", "runner", "policy", "core"):
        for f in (root / sub).rglob("*.py"):
            if "cati_portfolio_reservations" in f.read_text(encoding="utf-8", errors="ignore"):
                offenders.append(str(f))
    assert offenders == []


def test_selector_is_pure_no_io_no_reservation_names():
    import ast

    import app.trading_intelligence.portfolio.selector as sel
    import app.trading_intelligence.portfolio.returns as ret

    for module in (sel, ret):
        tree = ast.parse(inspect.getsource(module))
        imported = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert not any(m and (m.startswith("app.execution") or m.startswith("app.exchange") or "reservation_store" in m) for m in imported)
