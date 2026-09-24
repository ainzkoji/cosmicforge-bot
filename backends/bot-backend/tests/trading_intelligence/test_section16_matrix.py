"""Closure item 11 -- the complete Section 16 matrix (exposure, correlation,
selector, reservations). Factor/FX items live in test_factors_multi_asset.py;
real concurrency lives in test_reservation_race.py."""
from __future__ import annotations

import ast
import dataclasses
import sqlite3
from pathlib import Path

import pytest
from _pf import (
    BAR, NOW, T0, add_bot, add_pending, add_position, context, gen, held, make_db, ranked, rows_from_returns, sel,
    select,
)

from app.trading_intelligence.contracts.portfolio_intel import PortfolioPolicy
from app.trading_intelligence.portfolio import returns as R
from app.trading_intelligence.portfolio.context import build_portfolio_market_context
from app.trading_intelligence.portfolio.exposure_builder import build_account_exposure_snapshot
from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore, ReservationSchemaMissing
from app.trading_intelligence.portfolio.service import ShadowAccountPortfolioService

BACKEND = Path(__file__).resolve().parents[2]
FACTOR = gen(1)
CORR_A = [f + e for f, e in zip(FACTOR, gen(2, scale=0.002))]
CORR_B = [f + e for f, e in zip(FACTOR, gen(3, scale=0.002))]


@pytest.fixture
def db(tmp_path):
    d = make_db(tmp_path / "s16.db")
    for bot, acct in (("botA", "acct1"), ("botB", "acct1"), ("botC", "acct2")):
        add_bot(d, bot, acct)
    return d


def _snap(db, account="acct1", store=None, now=NOW):
    return build_account_exposure_snapshot(db, account, now, reservation_store=store, now_ms=now)


# =============================== 11.1 EXPOSURE ===============================
def test_all_bots_on_same_account_included_other_accounts_excluded(db):
    add_position(db, "botA", "BTCUSDT"); add_position(db, "botB", "ETHUSDT"); add_position(db, "botC", "SOLUSDT")
    s = _snap(db)
    assert {r.bot_instance_id for r in s.open_exposures} == {"botA", "botB"}
    assert "SOLUSDT" not in {r.instrument_key.venue_symbol for r in s.all_exposures}


def test_open_pending_and_unexpired_reservations_included(db):
    add_position(db, "botA", "BTCUSDT"); add_pending(db, "botB", "XRPUSDT")
    store = CATIReservationStore(db)
    assert store.reserve(broker_account_id="acct1", bot_instance_id="botA", cycle_id="c", selected=[sel("SOLUSDT")],
                         now_ms=NOW, ttl_seconds=600).reserved
    s = _snap(db, store=store)
    assert [len(s.open_exposures), len(s.pending_exposures), len(s.reservation_exposures)] == [1, 1, 1]


def test_expired_and_released_reservations_excluded(db):
    store = CATIReservationStore(db)
    r1 = store.reserve(broker_account_id="acct1", bot_instance_id="botA", cycle_id="c1", selected=[sel("SOLUSDT")],
                       now_ms=NOW, ttl_seconds=60).reservation
    r2 = store.reserve(broker_account_id="acct1", bot_instance_id="botB", cycle_id="c2", selected=[sel("ADAUSDT")],
                       now_ms=NOW, ttl_seconds=600).reservation
    assert store.release(r2.reservation_id, NOW + 1)
    s = _snap(db, store=store, now=NOW + 61_000)  # r1 expired
    assert s.reservation_exposures == ()
    assert store.get(r1.reservation_id).status in ("RESERVED", "EXPIRED")  # expiry marks lazily, never deletes


def test_no_double_counting(db):
    add_position(db, "botA", "BTCUSDT", qty=1.0, pid="p1")
    add_position(db, "botA", "BTCUSDT", qty=2.0, pid="p2")  # a second fill row of the same economic position
    add_pending(db, "botA", "BTCUSDT")  # confirmed entry still listed as pending
    store = CATIReservationStore(db)
    # botB reserves ETH, then its entry lands as pending: the reservation is realised, not a 2nd exposure
    store.reserve(broker_account_id="acct1", bot_instance_id="botB", cycle_id="c", selected=[sel("ETHUSDT")],
                  now_ms=NOW, ttl_seconds=600)
    add_pending(db, "botB", "ETHUSDT")
    s = _snap(db, store=store)
    btc = [r for r in s.all_exposures if r.instrument_key.venue_symbol == "BTCUSDT"]
    assert len(btc) == 1 and btc[0].quantity == 3.0
    eth = [r for r in s.all_exposures if r.instrument_key.venue_symbol == "ETHUSDT"]
    assert [r.exposure_status for r in eth] == ["PENDING_ENTRY"]


def test_duplicate_instrument_identified_not_merged(db):
    add_position(db, "botA", "BTCUSDT"); add_position(db, "botB", "BTCUSDT")
    s = _snap(db)
    assert len(s.open_exposures) == 2  # separate attributable exposures
    assert s.duplicate_instruments() == ("BTC/USDT:PERP",)


def test_fx_account_exposures_keep_fx_identity(tmp_path):
    d = make_db(tmp_path / "fx.db")
    add_bot(d, "fxbot", "fxacct", broker="oanda", market_type="forex")
    add_position(d, "fxbot", "EUR_USD")
    rec = _snap(d, account="fxacct").open_exposures[0]
    assert rec.instrument_key.asset_class == "FX"
    assert (rec.instrument_key.base_asset, rec.instrument_key.quote_asset) == ("EUR", "USD")


# ============================== 11.2 CORRELATION ==============================
def test_correlation_causal_timestamps_only():
    p = PortfolioPolicy()
    rows = rows_from_returns(CORR_A)
    ctx = build_portfolio_market_context({"BTCUSDT": rows}, T0 + 100 * BAR, p)
    assert max(t for t, _ in ctx.return_histories["BTCUSDT"]) <= T0 + 100 * BAR


def test_minimum_history_boundary():
    p = dataclasses.replace(PortfolioPolicy(), minimum_correlation_observations=60)
    at = context(p, BTCUSDT=CORR_A[:61], ETHUSDT=CORR_B[:61])  # 61 closes-worth => 60 returns overlap
    below = context(p, BTCUSDT=CORR_A[:59], ETHUSDT=CORR_B[:59])
    assert R.pair_correlation("BTCUSDT", "ETHUSDT", at, p).source == "EWMA"
    assert R.pair_correlation("BTCUSDT", "ETHUSDT", below, p).fallback_used


def test_fallback_reason_recorded_and_never_silent_zero():
    p = PortfolioPolicy()
    ctx = context(p, BTCUSDT=CORR_A[:10], XYZUSDT=CORR_B[:10])
    est = R.pair_correlation("BTCUSDT", "XYZUSDT", ctx, p)
    assert est.fallback_used and est.rho_shrunk != 0.0
    assert {"STATISTICAL_HISTORY_INSUFFICIENT", "STATIC_CORRELATION_FALLBACK_USED", "NO_STATIC_GROUP_AVAILABLE"} <= set(est.reason_codes)


def test_ewma_and_shrinkage_deterministic():
    p = PortfolioPolicy()
    ctx = context(p, BTCUSDT=CORR_A, ETHUSDT=CORR_B)
    a, b = R.pair_correlation("BTCUSDT", "ETHUSDT", ctx, p), R.pair_correlation("BTCUSDT", "ETHUSDT", ctx, p)
    assert a == b and a.rho_shrunk == pytest.approx(0.8 * a.rho_ewma)


# =============================== 11.4 SELECTOR ================================
INDEP = {s: gen(10 + i) for i, s in enumerate(("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"))}


def test_zero_candidate_selection():
    d = select([], context(**INDEP))
    assert d.selected_opportunity_ids == () and d.solver == "NONE"


def test_one_candidate():
    d = select([ranked("BTCUSDT", 0.5)], context(**INDEP), slots=2)
    assert d.selected_opportunity_ids == ("rk_BTCUSDT_LONG",)


@pytest.mark.parametrize("slots", [2, 3])
def test_exact_enumeration(slots):
    cands = [ranked(s, 0.5 - 0.01 * i, pos=i + 1) for i, s in enumerate(INDEP)]
    d = select(cands, context(**INDEP), slots=slots)
    assert d.solver == "EXACT" and len(d.selected_opportunity_ids) <= slots


def test_best_combination_differs_from_naive_top_n():
    p = PortfolioPolicy()
    ctx = context(p, BTCUSDT=CORR_A, ETHUSDT=CORR_B, SOLUSDT=INDEP["SOLUSDT"])
    cands = [ranked("BTCUSDT", 0.90, pos=1), ranked("ETHUSDT", 0.88, pos=2), ranked("SOLUSDT", 0.80, pos=3)]
    d = select(cands, ctx, slots=2, policy=p)
    naive = {"rk_BTCUSDT_LONG", "rk_ETHUSDT_LONG"}
    assert set(d.selected_opportunity_ids) != naive and "rk_SOLUSDT_LONG" in d.selected_opportunity_ids


def test_each_penalty_component_is_active():
    p = PortfolioPolicy()
    corr = select([ranked("BTCUSDT", 1, pos=1), ranked("ETHUSDT", 1, pos=2)], context(p, BTCUSDT=CORR_A, ETHUSDT=CORR_B),
                  slots=2, policy=dataclasses.replace(p, lambda_corr=0.0))
    assert corr.score_breakdown.correlation_penalty > 0
    fac = select([ranked("SOLUSDT", 1, pos=1), ranked("XRPUSDT", 1, pos=2), ranked("ADAUSDT", 1, pos=3)], context(p),
                 slots=3, policy=dataclasses.replace(p, lambda_beta=0.0, factor_tolerance_units=0.5))
    assert fac.score_breakdown.factor_penalty > 0  # unknown betas => conservative unit exposure, recorded
    sec = select([ranked("BTCUSDT", 1, pos=1), ranked("ETHUSDT", 1, pos=2)], context(p, **INDEP), slots=2,
                 policy=dataclasses.replace(p, lambda_sector=0.0, sector_tolerance_units=1.0))
    assert sec.score_breakdown.sector_penalty > 0  # both in static group 0
    liq = select([ranked("SOLUSDT", 1, liq=0.1, pos=1), ranked("XRPUSDT", 1, liq=0.1, pos=2)], context(p, **INDEP),
                 slots=2, policy=dataclasses.replace(p, lambda_liq=0.0, liquidity_low_tolerance=1))
    assert liq.score_breakdown.liquidity_penalty > 0


def test_duplicate_economic_exposure_prohibited():
    d = select([ranked("BTCUSDT", 1.0, pos=1)], context(**INDEP), existing=[held("BTCUSDT")])
    assert d.selected_opportunity_ids == () and d.rejected_candidates[0].reason_code == "DUPLICATE_EXPOSURE"
    two = select([ranked("BTCUSDT", 1.0, pos=1, cid="a"), ranked("BTCUSDT", 0.9, pos=2, cid="b")], context(**INDEP))
    assert len(two.selected_opportunity_ids) <= 1


def test_bot_slot_constraint():
    cands = [ranked(s, 0.5, pos=i + 1) for i, s in enumerate(INDEP)]
    assert len(select(cands, context(**INDEP), slots=1).selected_opportunity_ids) == 1
    none = select(cands, context(**INDEP), slots=0)
    assert none.selected_opportunity_ids == () and "NO_AVAILABLE_SLOTS" in none.reason_codes


def test_account_exposure_constraint_uses_other_bots_positions():
    p = PortfolioPolicy()
    ctx = context(p, BTCUSDT=CORR_A, ETHUSDT=CORR_B)
    d = select([ranked("ETHUSDT", 0.2, pos=1)], ctx, slots=1, existing=[held("BTCUSDT", bot="botB")], policy=p)
    assert d.selected_opportunity_ids == ()
    assert d.rejected_candidates[0].reason_code == "ACCOUNT_CORRELATION_CONFLICT"


def test_deterministic_tie_breaking():
    a = [ranked("SOLUSDT", 0.5, pos=1, cid="c2"), ranked("XRPUSDT", 0.5, pos=2, cid="c1")]
    ctx = context(**INDEP)
    assert select(a, ctx, slots=1).selected_opportunity_ids == select(a[::-1], ctx, slots=1).selected_opportunity_ids


def test_empty_portfolio_wins_when_every_combination_is_poor():
    d = select([ranked("BTCUSDT", -0.4, pos=1), ranked("ETHUSDT", -0.2, pos=2)], context(**INDEP), slots=2)
    assert d.selected_opportunity_ids == () and d.portfolio_score == 0.0


# ============================== 11.5 RESERVATIONS ==============================
def test_migration_creates_table_and_indexes(db):
    with db.connect() as c:
        cols = {r[1] for r in c.execute("PRAGMA table_info(cati_portfolio_reservations)")}
        idx = {r[1] for r in c.execute("PRAGMA index_list(cati_portfolio_reservations)")}
    assert {"reservation_id", "broker_account_id", "bot_instance_id", "cycle_id", "selected_candidate_ids",
            "created_at", "expires_at", "status", "policy_version", "policy_hash", "payload_hash"} <= cols
    assert {"idx_cati_resv_account_status_expiry", "idx_cati_resv_bot_status", "idx_cati_resv_status_expiry"} <= idx


def test_repeated_migration_idempotent_and_upgrades_legacy_table(tmp_path):
    from shared_lib.persistence.cati_schema import ensure_cati_schema
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    d = DB(str(tmp_path / "legacy.db"))
    with d.connect() as c:  # the table shape the old lazy DDL created, with a row
        c.execute("CREATE TABLE cati_portfolio_reservations (reservation_id TEXT PRIMARY KEY, broker_account_id TEXT NOT NULL,"
                  " bot_instance_id TEXT NOT NULL, cycle_id TEXT NOT NULL, selected_candidate_ids TEXT NOT NULL,"
                  " selected_instruments TEXT NOT NULL, status TEXT NOT NULL, mode TEXT NOT NULL DEFAULT 'SHADOW',"
                  " created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, reservation_version TEXT NOT NULL,"
                  " updated_at INTEGER NOT NULL)")
        c.execute("INSERT INTO cati_portfolio_reservations VALUES ('r1','a','b','c','[]','[]','RELEASED','SHADOW',1,2,'1.0.0',3)")
    migrate(d); migrate(d); ensure_cati_schema(d)
    with d.connect() as c:
        rows = c.execute("SELECT reservation_id, status FROM cati_portfolio_reservations").fetchall()
        cols = {r[1] for r in c.execute("PRAGMA table_info(cati_portfolio_reservations)")}
    assert [tuple(r) for r in rows] == [("r1", "RELEASED")] and "policy_hash" in cols


def test_runtime_code_issues_no_create_table():
    for rel in ("portfolio/reservation_store.py", "portfolio/service.py", "portfolio/exposure_builder.py",
                "integration/cycle_shadow.py"):
        src = (BACKEND / "app" / "trading_intelligence" / rel).read_text(encoding="utf-8")
        tree = ast.parse(src)
        literals = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
        assert not any("CREATE TABLE" in s.upper() or "CREATE INDEX" in s.upper() for s in literals), rel


def test_store_fails_closed_without_migration(tmp_path):
    from shared_lib.persistence.db import DB
    with pytest.raises(ReservationSchemaMissing):
        CATIReservationStore(DB(str(tmp_path / "bare.db")))
    svc = ShadowAccountPortfolioService(DB(str(tmp_path / "bare2.db")))
    out = svc.select_and_reserve(ranked=[ranked("BTCUSDT", 1.0)], broker_account_id="a", bot_instance_id="b", cycle_id="c",
                                 max_open_positions=3, context=context(), now_ms=NOW)
    assert out.reservation is None and out.decision.reason_codes == ("RESERVATION_SCHEMA_MISSING",)


def test_create_consume_release_expire_lifecycle(db):
    store = CATIReservationStore(db)
    mk = lambda cyc, sym, ttl=600: store.reserve(broker_account_id="acct1", bot_instance_id="botA", cycle_id=cyc,
                                                 selected=[sel(sym)], now_ms=NOW, ttl_seconds=ttl).reservation
    a, b, c = mk("c1", "SOLUSDT"), mk("c2", "ADAUSDT"), mk("c3", "XRPUSDT", ttl=1)
    assert a.status == "RESERVED"
    assert store.consume(a.reservation_id, NOW + 10) and store.get(a.reservation_id).status == "CONSUMED"
    assert not store.consume(a.reservation_id, NOW + 11)  # idempotent: terminal stays terminal
    assert store.release(b.reservation_id, NOW + 10) and not store.release(b.reservation_id, NOW + 12)
    assert not store.consume(c.reservation_id, NOW + 5_000)  # expired can never be consumed late
    assert store.get(c.reservation_id).status == "EXPIRED"
    assert store.cleanup_expired(NOW + 10**7) == 0  # nothing left RESERVED+stale: sweep is idempotent


def test_expired_reservation_does_not_count(db):
    store = CATIReservationStore(db)
    store.reserve(broker_account_id="acct1", bot_instance_id="botA", cycle_id="c1", selected=[sel("SOLUSDT")],
                  now_ms=NOW, ttl_seconds=1)
    later = NOW + 5_000
    assert store.active_reservations("acct1", later) == []
    again = store.reserve(broker_account_id="acct1", bot_instance_id="botB", cycle_id="c2", selected=[sel("SOLUSDT")],
                          now_ms=later, ttl_seconds=600)
    assert again.reserved


def test_account_isolation(db):
    store = CATIReservationStore(db)
    assert store.reserve(broker_account_id="acct1", bot_instance_id="botA", cycle_id="c", selected=[sel("SOLUSDT")],
                         now_ms=NOW, ttl_seconds=600).reserved
    assert store.reserve(broker_account_id="acct2", bot_instance_id="botC", cycle_id="c", selected=[sel("SOLUSDT")],
                         now_ms=NOW, ttl_seconds=600).reserved


def _svc_select(db, svc, symbols=("SOLUSDT",), bot="botA"):
    return svc.select_and_reserve(ranked=[ranked(s, 0.9, pos=i + 1, bot=bot) for i, s in enumerate(symbols)],
                                  broker_account_id="acct1", bot_instance_id=bot, cycle_id=f"cyc_{bot}",
                                  max_open_positions=3, context=context(**INDEP), now_ms=NOW)


def test_stale_capacity_conflict(db, monkeypatch):
    svc = ShadowAccountPortfolioService(db)
    real = svc.store.reserve

    def race(**kw):  # the bot's effective max_open_positions shrank after selection
        kw["max_open_positions"] = 1  # selection assumed 3 free slots
        return real(**kw)

    monkeypatch.setattr(svc.store, "reserve", race)
    out = _svc_select(db, svc)
    assert out.reservation is None and "CAPACITY_CHANGED" in out.decision.reason_codes


def test_capacity_revalidated_in_transaction(db):
    store = CATIReservationStore(db)
    add_position(db, "botA", "DOTUSDT")
    stale = store.reserve(broker_account_id="acct1", bot_instance_id="botA", cycle_id="c", selected=[sel("SOLUSDT")],
                          now_ms=NOW, ttl_seconds=600, max_open_positions=3, expected_available_slots=3)
    assert stale.conflict_reason == "CAPACITY_CHANGED"  # selection assumed 3 free slots; only 2 are
    full = store.reserve(broker_account_id="acct1", bot_instance_id="botA", cycle_id="c", selected=[sel("SOLUSDT")],
                         now_ms=NOW, ttl_seconds=600, max_open_positions=1)
    assert full.conflict_reason == "CAPACITY_CHANGED"  # no free slot at all
    ok = store.reserve(broker_account_id="acct1", bot_instance_id="botA", cycle_id="c", selected=[sel("SOLUSDT")],
                       now_ms=NOW, ttl_seconds=600, max_open_positions=3, expected_available_slots=2)
    assert ok.reserved


def test_pending_entry_landing_after_selection_is_exposure_changed(db, monkeypatch):
    svc = ShadowAccountPortfolioService(db)
    real = svc.store.reserve

    def race(**kw):
        add_pending(db, "botA", "DOTUSDT")
        return real(**kw)

    monkeypatch.setattr(svc.store, "reserve", race)
    out = _svc_select(db, svc)
    assert out.reservation is None and "EXPOSURE_CHANGED" in out.decision.reason_codes


def test_changed_exposure_conflict(db, monkeypatch):
    svc = ShadowAccountPortfolioService(db)
    real = svc.store.reserve

    def race(**kw):  # ANOTHER bot on the account opens a position meanwhile
        add_position(db, "botB", "LINKUSDT")
        return real(**kw)

    monkeypatch.setattr(svc.store, "reserve", race)
    out = _svc_select(db, svc)
    assert out.reservation is None and "EXPOSURE_CHANGED" in out.decision.reason_codes


def test_no_auto_substitution_on_conflict(db, monkeypatch):
    svc = ShadowAccountPortfolioService(db)
    real = svc.store.reserve

    def race(**kw):
        CATIReservationStore(db).reserve(broker_account_id="acct1", bot_instance_id="botB", cycle_id="other",
                                         selected=[sel("SOLUSDT")], now_ms=NOW, ttl_seconds=600)
        return real(**kw)

    monkeypatch.setattr(svc.store, "reserve", race)
    out = svc.select_and_reserve(ranked=[ranked("SOLUSDT", 0.9, pos=1), ranked("XRPUSDT", 0.5, pos=2)],
                                 broker_account_id="acct1", bot_instance_id="botA", cycle_id="c1", max_open_positions=1,
                                 context=context(**INDEP), now_ms=NOW)
    assert out.reservation is None and out.decision.reservation_status == "CONFLICT"
    assert "ACCOUNT_RESERVATION_CONFLICT" in out.decision.reason_codes
    assert out.decision.selected_opportunity_ids == ("rk_SOLUSDT_LONG",)  # #2 was NOT silently substituted
    assert [r.bot_instance_id for r in CATIReservationStore(db).active_reservations("acct1", NOW)] == ["botB"]


def test_reservation_cannot_modify_production_slots_or_margin(db, monkeypatch):
    from app.execution import position_slots
    from app.risk import capital_ledger

    def boom(*a, **k):
        raise AssertionError("production slot/margin authority mutated")

    monkeypatch.setattr(position_slots, "reserve_entry_slot", boom)
    monkeypatch.setattr(capital_ledger.ACCOUNT_RESERVATIONS, "reserve", boom)
    add_position(db, "botA", "BTCUSDT")
    with db.connect() as c:
        before = (c.execute("SELECT COUNT(*) FROM positions").fetchone()[0],
                  c.execute("SELECT COUNT(*) FROM pending_entries").fetchone()[0])
    out = _svc_select(db, ShadowAccountPortfolioService(db), symbols=("SOLUSDT",))
    assert out.reservation is not None and out.reservation.mode == "SHADOW"
    with db.connect() as c:
        after = (c.execute("SELECT COUNT(*) FROM positions").fetchone()[0],
                 c.execute("SELECT COUNT(*) FROM pending_entries").fetchone()[0])
    assert before == after
