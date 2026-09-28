"""Section 17: portfolio, risk and capital as ONE account-scoped system.

Existing suites already prove (not duplicated here): the 2.5 % hard daily cap and adaptive daily risk
overriding a capital-ready plan (test_capital_readiness_boundary / test_section20_execution_boundary), executor
slot / margin / leverage / sizing rejections (test_section20_execution_boundary), the currency-factor cap and
multi-bot account aggregation of positions (test_global_state_cross_asset / test_portfolio), confirmed-transfer
gating and pending-transfer reservation retention (test_capital_readiness_boundary), submit-unknown
RESOLUTION_PENDING ownership (test_section20_execution_boundary)."""
import threading
from decimal import Decimal as D

import pytest
from _plan import fresh_db, run_pipeline, venue_evaluated

from shared_lib.broker.wallets import topology_for

from app.trading_intelligence.capital.planner import AccountCapitalState, CapitalSettings
from app.trading_intelligence.portfolio.account_capital import (
    AccountCapitalPolicy, CapitalBudget, consider_account, family_eligibility,
)
from app.trading_intelligence.portfolio.reservation_store import CATIReservationStore
from app.trading_intelligence.portfolio.service import ShadowAccountPortfolioService

from test_cati_section16_transfer_runtime import StubFacts


def unified(free="1000", account="acct1"):
    return AccountCapitalState(account, "USDT", topology_for("bybit", "UNIFIED"), {"UNIFIED": D(free), "FUND": D("0")})


@pytest.fixture
def two(tmp_path):
    db = fresh_db(tmp_path)
    start = 1_700_000_000_000  # same decision epoch for both instruments (one ranking batch)
    evs = [venue_evaluated("BTCUSDT", seed=999, start=start), venue_evaluated("ETHUSDT", seed=998, start=start + 1)]
    p = run_pipeline(db, evs, cycle="c0")  # ranking + a baseline reservation run
    with db.connect() as c:
        c.execute("DELETE FROM cati_portfolio_reservations")  # start each test from an empty account book
    return db, p


_DEFAULT = object()


def view(p, *, state=_DEFAULT, margin="50", family=None, block=None, reserved=None, reserved_family=None,
         account="acct1", policy=None):
    rk, ev = p["result"].ranked, p["evaluated"]
    now = max(e.candidate.decision_time for e in ev.values()) + 1_000
    return consider_account(rk, ev, user_id="u1", broker_account_id=account,
                            state=unified(account=account) if state is _DEFAULT else state,
                            settings=CapitalSettings(), per_trade_margin=D(margin), leverage=5.0,
                            family=family or {"CRYPTO": (True, None)}, route_facts=StubFacts(), cycle_id="c1",
                            now_ms=now, account_block=block, reserved_by_wallet=reserved or {},
                            reserved_by_family=reserved_family or {}, policy=policy)


def select(db, p, cv, *, bot="botA", account="acct1", cycle="c1"):
    from test_portfolio import context

    svc = ShadowAccountPortfolioService(db)
    syms = sorted({r.instrument_key.venue_symbol for r in p["result"].ranked})
    from _pf import gen
    now = max(e.candidate.decision_time for e in p["evaluated"].values()) + 60_000
    return svc.select_and_reserve(ranked=p["result"].ranked, broker_account_id=account, bot_instance_id=bot,
                                  cycle_id=cycle, max_open_positions=3,
                                  context=context(**{s: gen(i + 1) for i, s in enumerate(syms)}), now_ms=now,
                                  evaluated_by_candidate_id=p["evaluated"], capital_view=cv)


def test_global_ranking_is_fixed_before_and_independent_of_account_capital(two):
    db, p = two
    ranked = [(r.ranked_opportunity_id, r.rank_position) for r in p["result"].ranked]
    rich, poor = view(p, state=unified("1000")), view(p, state=unified("60"))
    assert list(rich.candidates) == list(poor.candidates) == [r for r, _ in sorted(ranked, key=lambda x: x[1])]
    assert all(rich.viable(r) for r, _ in ranked)
    # with capital for only ONE trade, the budget admits exactly one -- the ranking itself is never re-run
    out = select(db, p, poor)
    assert len(out.decision.selected_opportunity_ids) == 1
    assert [(r.ranked_opportunity_id, r.rank_position) for r in p["result"].ranked] == ranked


def test_account_rejections_are_recorded_and_never_reserved(two):
    db, p = two
    out = select(db, p, view(p, state=None))  # capital facts unavailable
    d = out.decision
    assert d.selected_opportunity_ids == () and out.reservation is None
    assert {r.reason_code for r in d.rejected_candidates} == {"ACCOUNT_CAPITAL_STATE_UNAVAILABLE"}
    with db.connect() as c:
        assert c.execute("SELECT COUNT(*) FROM cati_portfolio_reservations").fetchone()[0] == 0


def test_unified_collateral_budgets_are_constraints_over_one_basis():
    b = CapitalBudget(required={"c": 6000.0, "f": 5000.0}, wallet={"c": "UNIFIED", "f": "UNIFIED"},
                      family={"c": "CRYPTO", "f": "FX"}, physical=frozenset(), transfer_amount={},
                      available_by_wallet={"UNIFIED": 10_000.0}, family_limit={"CRYPTO": 6000.0, "FX": 5000.0},
                      family_reserved={})
    assert b.violation(["c"]) is None and b.violation(["f"]) is None
    assert b.violation(["c", "f"]) == "CAPITAL_BUDGET_EXCEEDED:UNIFIED"  # 6,000 + 5,000 != 11,000 available
    fam = CapitalBudget(required={"c1": 4000.0, "c2": 4000.0}, wallet={"c1": "UNIFIED", "c2": "UNIFIED"},
                        family={"c1": "CRYPTO", "c2": "CRYPTO"}, physical=frozenset(), transfer_amount={},
                        available_by_wallet={"UNIFIED": 10_000.0}, family_limit={"CRYPTO": 6000.0},
                        family_reserved={})
    assert fam.violation(["c1", "c2"]) == "FAMILY_BUDGET_EXCEEDED:CRYPTO"


def test_family_budget_is_enforced_atomically_in_the_reservation(two):
    db, p = two
    policy = AccountCapitalPolicy(family_max_fraction=(("CRYPTO", 0.06),))  # 6% of a 1,000 basis = 60
    out = select(db, p, view(p, policy=policy))
    assert len(out.decision.selected_opportunity_ids) == 1  # two x 50 would exceed the crypto budget
    store = CATIReservationStore(db)
    cap = store.capital(out.reservation.reservation_id)
    assert cap["by_family"] == {"CRYPTO": "50"} and cap["family_limits"] == {"CRYPTO": 0.06}


def test_capital_reserved_by_one_bot_is_seen_by_every_bot_on_the_account(two):
    db, p = two
    store = CATIReservationStore(db)
    first = select(db, p, view(p, state=unified("100")), bot="botA")
    assert first.reservation is not None
    held_wallet, held_family = store.capital_reserved("acct1", first.decision.decision_time)
    assert held_wallet == {"UNIFIED": D("100")} and held_family == {"CRYPTO": D("100")}
    # bot B on the SAME account sees the capital as already committed: nothing left for it
    v = view(p, state=unified("100"), reserved=held_wallet, reserved_family=held_family)
    assert all(not c.viable and c.plan["available_in_trading_wallet"] == "0" for c in v.candidates.values())
    # account B is isolated: nothing reserved there
    assert store.capital_reserved("acct2", first.decision.decision_time) == ({}, {})


def test_concurrent_capital_reservations_cannot_double_spend(two):
    """Both selections are computed on the SAME stale snapshot (1,000 free, nothing reserved); the in-transaction
    capital recheck lets exactly one reserve. The store keeps no capital in process memory: SQLite's
    BEGIN IMMEDIATE (a database-wide writer lock) is the serialization point across processes."""
    db, p = two
    store = CATIReservationStore(db)
    base = view(p, state=unified("80"), margin="50")
    rid_a, rid_b = list(base.candidates)
    now = max(e.candidate.decision_time for e in p["evaluated"].values()) + 60_000
    results = {}

    def reserve(rid, bot):
        r = next(x for x in p["result"].ranked if x.ranked_opportunity_id == rid)
        results[bot] = store.reserve(broker_account_id="acct1", bot_instance_id=bot, cycle_id=f"c-{bot}",
                                     selected=[(r.setup_candidate_id, r.instrument_key.canonical_symbol,
                                                r.instrument_key.venue, r.instrument_key.venue_symbol, r.side)],
                                     now_ms=now, ttl_seconds=600, capital=base.claim([rid]))

    threads = [threading.Thread(target=reserve, args=(rid_a, "botA")), threading.Thread(target=reserve, args=(rid_b, "botB"))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(r.reserved for r in results.values()) == [False, True]
    loser = next(r for r in results.values() if not r.reserved)
    assert loser.conflict_reason == "CAPITAL_ALREADY_RESERVED"


def test_capital_basis_unknown_is_never_reserved_against(two):
    db, p = two
    v = view(p)
    rid = next(iter(v.candidates))
    claim = dict(v.claim([rid]), basis={})
    r = next(x for x in p["result"].ranked if x.ranked_opportunity_id == rid)
    out = CATIReservationStore(db).reserve(
        broker_account_id="acct1", bot_instance_id="botA", cycle_id="cx", ttl_seconds=60,
        now_ms=p["evaluated"][r.setup_candidate_id].candidate.decision_time + 1,
        selected=[(r.setup_candidate_id, r.instrument_key.canonical_symbol, r.instrument_key.venue,
                   r.instrument_key.venue_symbol, r.side)], capital=claim)
    assert not out.reserved and out.conflict_reason == "CAPITAL_BASIS_UNKNOWN"


def test_expired_opportunity_is_never_approved_for_capital(two):
    db, p = two
    rk, ev = p["result"].ranked, p["evaluated"]
    late = max(e.candidate.valid_until for e in ev.values())
    v = consider_account(rk, ev, user_id="u1", broker_account_id="acct1", state=unified(), settings=CapitalSettings(),
                         per_trade_margin=D("50"), leverage=5.0, family={"CRYPTO": (True, None)},
                         route_facts=StubFacts(), cycle_id="c1", now_ms=late)
    assert all(c.reason_codes == ("OPPORTUNITY_EXPIRED",) for c in v.candidates.values())


def test_one_family_blocked_other_family_eligible():
    fam = family_eligibility("binance", "live", ["CRYPTO", "FX"])
    assert fam["CRYPTO"] == (True, None)
    assert fam["FX"][0] is False and fam["FX"][1] == "BROKER_EXECUTION_UNVALIDATED_FOR_LIVE"
    assert family_eligibility("bybit", "demo", ["CRYPTO", "FX"]) == {"CRYPTO": (True, None), "FX": (True, None)}


def test_blocked_family_blocks_only_its_candidates_and_kill_switch_blocks_the_account(two):
    db, p = two
    blocked = view(p, family={"CRYPTO": (False, "CERTIFICATION_NOT_READY")})
    assert all(c.reason_codes == ("FAMILY_BLOCKED:CERTIFICATION_NOT_READY",) for c in blocked.candidates.values())
    halted = view(p, block="CATI_NEW_ENTRY_KILL_SWITCH")
    assert all(c.reason_codes == ("CATI_NEW_ENTRY_KILL_SWITCH",) for c in halted.candidates.values())


def test_provisional_sizing_executes_nothing(two):
    db, p = two
    with db.connect() as c:
        before = [c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ("positions", "pending_entries", "broker_transfer_requests", "cati_portfolio_reservations")]
    v = view(p, margin="50")
    c0 = next(iter(v.candidates.values()))
    assert c0.required_margin == D("50") and c0.provisional_notional == pytest.approx(250.0)  # margin x leverage
    with db.connect() as c:
        after = [c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                 for t in ("positions", "pending_entries", "broker_transfer_requests", "cati_portfolio_reservations")]
    assert before == after


def test_internal_transfer_never_creates_risk_or_capital(two):
    """A physical route funds the SAME per-trade requirement; the capital basis never counts inbound money
    before the broker confirms it, and the requirement is not inflated by the transfer."""
    db, p = two
    seg = AccountCapitalState("acct1", "USDT", topology_for("binance"),
                              {"UMFUTURE": D("10"), "FUNDING": D("5000"), "MAIN": None}, transfer_capability_usable=True)
    rk, ev = p["result"].ranked, p["evaluated"]
    now = max(e.candidate.decision_time for e in ev.values()) + 1_000
    v = consider_account(rk, ev, user_id="u1", broker_account_id="acct1", state=seg, settings=CapitalSettings(),
                         per_trade_margin=D("50"), leverage=5.0, family={"CRYPTO": (True, None)},
                         route_facts=StubFacts(fee=0.1, latency_ms=1_000), cycle_id="c1", now_ms=now)
    c = next(iter(v.candidates.values()))
    assert c.required_margin == D("50") and c.physical
    assert v.capital_basis_by_wallet == {"UMFUTURE": D("10")}  # inbound transfer is NOT capital yet
    budget = v.budget()
    assert budget.available_by_wallet == {"UMFUTURE": 10.0}
    assert all(x.viable and x.physical for x in v.candidates.values())
    assert budget.violation(list(v.candidates)) == "CAPITAL_BUDGET_EXCEEDED:ONE_PHYSICAL_ROUTE_PER_SELECTION"


def test_full_cycle_reserves_capital_with_the_selection(two):
    db, p = two
    out = select(db, p, view(p, state=unified("1000")))
    cap = CATIReservationStore(db).capital(out.reservation.reservation_id)
    assert cap["user_id"] == "u1" and cap["asset"] == "USDT" and cap["wallet"] == "UNIFIED"
    assert D(cap["amount"]) == D("50") * len(out.decision.selected_opportunity_ids)
    assert cap["topology_class"] == "UNIFIED" and set(cap["plans"].values()) == {"NO_ACTION_SHARED_COLLATERAL"}
    with db.connect() as c:
        row = c.execute("SELECT user_id, capital_asset, capital_amount, capital_wallet FROM cati_portfolio_reservations"
                        ).fetchone()
    assert tuple(row) == ("u1", "USDT", cap["amount"], "UNIFIED")
