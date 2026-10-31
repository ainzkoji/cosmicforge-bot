"""Platform-wide liquidity coordination with 1,000 simulated accounts on ONE symbol (section 16, EXT-LIQ-01).

What is PROVED here, at 1,000 accounts, through the real ``PlatformSpreadGate`` / ``PlatformOrderBudget`` on a
migrated throwaway platform database (SQLite always; PostgreSQL too when ``PLATFORM_TEST_POSTGRES_URL`` is set):

1. **The production refusals hold at scale.** Under the production default policy every MARKET entry of every
   account is refused ``MARKET_FALLBACK_DISABLED_BY_POLICY`` before any budget exists. With a governed L-08 record
   (recorded in the temporary database only, with a test approval reference) exactly ONE account proceeds and the
   other 999 are refused ``ORDER_SPREADING_CALIBRATION_REQUIRED`` (the record is never calibrated); with an
   account-scoped record the other 999 are refused ``MARKET_POLICY_ACCOUNT_SCOPE_MISMATCH``. An uncalibrated
   market-permitting test policy also lets exactly one of 1,000 racing accounts through -- across threads and
   across separate OS processes.
2. **The coordination mechanism** -- exercised ONLY with ``_liquidity_kit.test_only_calibrated``: a policy with
   ``calibrated=True`` and a reference that says it is NOT an owner approval, built by this test against its own
   temporary database (no product code builds one, see ``test_spread_gate``). With it: the shared venue/symbol
   notional cap across signals, threads and processes; pending (SUBMITTED) slots counting; partial-fill accounting;
   cancellation and crash recovery (``sweep``); expiring reservations; the depth-freshness and conservative
   slippage constraints; fair, deterministic ordering and bounded scheduling; and a cross-account reconciliation
   of every slot against every decision link after each step.
3. **The calibration tooling** (``scripts/perf/liquidity_impact_report.py``) reads the recorded observations and
   produces a candidate model that is always ``UNCALIBRATED`` and, for these synthetic observations,
   ``evidence_class: SYNTHETIC`` -- and it writes nothing.

Depth comes from a fake venue client: nothing here is a statement about real venue liquidity.
"""
from __future__ import annotations

import hashlib
import importlib.util
import multiprocessing as mp
import random
import re
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path

import pytest

import _liquidity_kit as K
from _pg_server import PG_REQUIRED, PG_URL, _pg_admin, _pg_url
from app.trading_intelligence.pipeline.family_governance import OperatorAuthorization
from app.trading_intelligence.scale import market_entry_policy as M
from app.trading_intelligence.scale import spread_gate as G
from app.trading_intelligence.scale.order_spreading import (
    BUY, CONSERVATIVE_DEFAULT, DepthSnapshot, PlatformOrderBudget, rank_key, sequence,
)
from shared_lib.persistence.sql import Database, decimal_out, migrate
from shared_lib.persistence.sql.platform_schema import all_migrations

N = 1_000
THREADS = 16
ROOT = Path(__file__).resolve().parents[4]
DIALECTS = ["sqlite"] + (["postgresql"] if PG_URL or PG_REQUIRED else [])
OP = OperatorAuthorization(operator_id="operator-test", channel="test:liquidity-scale", permission="admin-emergency")


@pytest.fixture(params=DIALECTS)
def url(request, tmp_path):
    if request.param == "sqlite":
        yield f"sqlite:///{(tmp_path / 'liquidity_scale.db').as_posix()}"
        return
    if not PG_URL:
        pytest.fail("PLATFORM_TEST_POSTGRES_REQUIRED=1 but PLATFORM_TEST_POSTGRES_URL is not set")
    name = f"cfliq_{uuid.uuid4().hex[:12]}"
    _pg_admin(f"CREATE DATABASE {name}")
    try:
        yield _pg_url(name)
    finally:
        _pg_admin(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


@pytest.fixture
def pdb(url):
    db = Database(url, pool_size=THREADS + 4)
    migrate(db, all_migrations())
    yield db
    db.close()


@pytest.fixture
def fleet(pdb):
    ids = K.accounts(N)
    K.register(pdb, ids)
    return ids


def race(fn, ids, threads=THREADS):
    with ThreadPoolExecutor(max_workers=threads) as pool:
        return dict(zip(ids, pool.map(fn, ids)))


def count(db, sql, params=()):
    with db.transaction(write=False) as conn:
        return int(conn.scalar(sql, params) or 0)


def db_now(db):
    with db.transaction(write=False) as conn:
        return conn.now_ms()


def processes(url, policy, ids, *, families=("LIQ_SCALE_FAMILY",), notional="50", workers=4):
    ctx = mp.get_context("spawn")
    start, results = ctx.Event(), ctx.Queue()
    chunks = [ids[i::workers] for i in range(workers)]
    procs = [ctx.Process(target=K.process_worker, args=(url, policy, chunk, families, notional, start, results))
             for chunk in chunks]
    for p in procs:
        p.start()
    start.set()
    got = [results.get(timeout=300) for _ in procs]
    for p in procs:
        p.join(120)
    assert all(kind == "ok" for kind, _ in got), got        # no worker crashed
    merged = {}
    for _, out in got:
        merged.update(out)
    assert len(merged) == len(ids)
    return merged


# ═══════════════════════════════════════════════ 1. the production refusals, at 1,000 accounts
def test_the_production_default_refuses_every_market_entry_of_1000_accounts(pdb, fleet):
    gate = G.PlatformSpreadGate(pdb)
    assert gate.policy is CONSERVATIVE_DEFAULT and not gate.policy.allow_market_fallback and not gate.policy.calibrated
    books = {a: K.Book() for a in fleet}
    codes = race(lambda a: K.reserve_one(gate, a, client=books[a]), fleet)
    assert Counter(codes.values()) == {"MARKET_FALLBACK_DISABLED_BY_POLICY": N}
    assert all(b.reads == 1 for b in books.values())          # each account's own fresh depth, read once
    assert count(pdb, "SELECT COUNT(*) FROM platform_symbol_budgets") == 0     # refused before any budget
    assert count(pdb, "SELECT COUNT(*) FROM platform_order_slots") == 0
    assert count(pdb, "SELECT COUNT(*) FROM platform_spread_decisions WHERE outcome='REFUSED' AND "
                      "code='MARKET_FALLBACK_DISABLED_BY_POLICY'") == N
    assert count(pdb, "SELECT COUNT(DISTINCT account_id) FROM platform_spread_decisions") == N


def _governed_terms(db, **over):
    terms = dict(max_participation="0.10", max_slippage_bps="25", max_spread_bps="10", max_depth_age_ms=5000,
                 min_levels=5, absolute_cap_notional="5000", expires_at_ms=db_now(db) + 86_400_000)
    terms.update(over)
    return terms


def _governed(gate, a):
    # qty 0.001 at ~50,000 with the stop 2 % below: 1 USDT of risk on 10,000 of equity (0.01 %), inside every bound
    return K.reserve_one(gate, a, notional="50", qty="0.001", stop_price="49000", equity="10000")


def test_a_governed_record_admits_one_account_and_the_never_calibrated_budget_refuses_999(pdb, fleet):
    record = M.MarketEntryPolicyLedger(pdb).record(
        OP, venue=K.VENUE, symbol=K.SYMBOL, environment=K.ENV, terms=_governed_terms(pdb),
        approval_ref="TEST-ONLY-NOT-AN-OWNER-APPROVAL", reason="test-only record inside a disposable database")
    assert record.spread_policy().calibrated is False
    gate = G.PlatformSpreadGate(pdb)                          # the production default constructor policy
    codes = race(lambda a: _governed(gate, a), fleet)
    assert Counter(codes.values()) == {"RESERVED": 1, "ORDER_SPREADING_CALIBRATION_REQUIRED": N - 1}
    assert count(pdb, "SELECT COUNT(*) FROM platform_order_slots WHERE status='RESERVED'") == 1
    assert count(pdb, "SELECT COUNT(*) FROM platform_symbol_budgets WHERE calibrated <> 0") == 0
    with pdb.transaction(write=False) as conn:
        assert {r[0] for r in conn.all("SELECT calibrated FROM platform_market_entry_policy_events")} == {0}
    K.reconcile(pdb)


def test_an_account_scoped_record_admits_only_its_account_of_1000(pdb, fleet):
    M.MarketEntryPolicyLedger(pdb).record(
        OP, venue=K.VENUE, symbol=K.SYMBOL, environment=K.ENV, terms=_governed_terms(pdb), account_id=fleet[517],
        approval_ref="TEST-ONLY-NOT-AN-OWNER-APPROVAL", reason="test-only record inside a disposable database")
    gate = G.PlatformSpreadGate(pdb)
    codes = race(lambda a: _governed(gate, a), fleet)
    assert codes[fleet[517]] == "RESERVED"
    assert Counter(codes.values()) == {"RESERVED": 1, "MARKET_POLICY_ACCOUNT_SCOPE_MISMATCH": N - 1}


def test_an_uncalibrated_budget_admits_exactly_one_of_1000_racing_threads_over_two_signals(pdb, fleet):
    gate = G.PlatformSpreadGate(pdb, policy=K.uncalibrated_market_ok())
    families = ("FAM_A", "FAM_B")
    codes = race(lambda a: K.reserve_one(gate, a, family=families[int(a[-1]) % 2]), fleet)
    assert Counter(codes.values()) == {"RESERVED": 1, "ORDER_SPREADING_CALIBRATION_REQUIRED": N - 1}
    assert count(pdb, "SELECT COUNT(*) FROM platform_symbol_budgets") == 2      # two signals, one symbol
    assert count(pdb, "SELECT COUNT(*) FROM platform_order_slots WHERE status='RESERVED'") == 1
    K.reconcile(pdb)


def test_an_uncalibrated_budget_admits_exactly_one_of_1000_accounts_across_four_processes(url, pdb, fleet):
    codes = processes(url, "uncalibrated_market_ok", fleet, families=("FAM_A", "FAM_B"))
    assert Counter(codes.values()) == {"RESERVED": 1, "ORDER_SPREADING_CALIBRATION_REQUIRED": N - 1}
    assert count(pdb, "SELECT COUNT(*) FROM platform_order_slots WHERE status='RESERVED'") == 1
    K.reconcile(pdb)


# ═══════════════════════════════════════════════ 2. the mechanism, with a TEST-ONLY calibrated policy (temp DB)
def test_the_shared_symbol_cap_holds_for_1000_accounts_over_two_signals_and_16_threads(pdb, fleet):
    gate = G.PlatformSpreadGate(pdb, policy=K.test_only_calibrated("10000"))
    families = ("FAM_A", "FAM_B")
    codes = race(lambda a: K.reserve_one(gate, a, notional="30", family=families[int(a[-1]) % 2]), fleet)
    # 333 x 30 = 9,990 <= 10,000 < 334 x 30: never more, whichever signal or thread asked first
    assert Counter(codes.values()) == {"RESERVED": 333, "PLATFORM_SYMBOL_BUDGET_PARTIAL_GRANT": N - 333}
    totals = K.slot_totals(pdb)
    assert totals["TOTAL"] == Decimal("9990") <= Decimal("10000")
    assert K.reconcile(pdb, cap=Decimal("10000"))["active_slots"] == 333


def test_the_shared_symbol_cap_holds_for_1000_accounts_across_four_processes(url, pdb, fleet):
    codes = processes(url, "test_only_calibrated", fleet, families=("FAM_A", "FAM_B"), notional="30")
    assert Counter(codes.values()) == {"RESERVED": 333, "PLATFORM_SYMBOL_BUDGET_PARTIAL_GRANT": N - 333}
    assert K.slot_totals(pdb)["TOTAL"] == Decimal("9990")
    K.reconcile(pdb, cap=Decimal("10000"))


def test_pending_partial_fill_cancel_crash_and_expiry_accounting_across_1000_accounts(pdb, fleet):
    cap = Decimal("10000")
    gate = G.PlatformSpreadGate(pdb, policy=K.test_only_calibrated("10000"), claim_grace_ms=0)
    grants, refused = {}, Counter()
    queue = list(fleet)

    def admit(k):
        """The next ``k`` accounts in line ask; returns how many were reserved."""
        taken = 0
        while queue and k > 0:
            a = queue.pop(0)
            k -= 1
            code, grant = K.reserve_grant(gate, a, notional="30")
            if grant is not None:
                grants[a] = grant
                taken += 1
            else:
                refused[code] += 1
        return taken

    assert admit(400) == 333 and refused == {"PLATFORM_SYMBOL_BUDGET_PARTIAL_GRANT": 67}
    held = list(grants)
    # pending: 200 attempts claimed (SUBMITTED) still hold their notional -- nothing more is admitted
    for a in held[:200]:
        gate.mark_submitted(grants[a])
    t = K.slot_totals(pdb)
    assert (t["SUBMITTED"], t["RESERVED"], t["TOTAL"]) == (Decimal("6000"), Decimal("3990"), Decimal("9990"))
    assert admit(10) == 0
    K.reconcile(pdb, cap=cap)
    # partial fills: 100 orders filled HALF (15 of 30): the slot shrinks to the actual notional, freeing 1,500
    for a in held[:100]:
        assert gate.settle_attempt(account_id=a, tenant_id=K.tenant(a), attempt_id=K.attempt_id(a), outcome="FILLED",
                                   filled_notional=Decimal("15")) == 1
    assert K.slot_totals(pdb)["FILLED"] == Decimal("1500")
    assert admit(60) == 50                                    # (10,000 - 8,490) // 30 = 50
    K.reconcile(pdb, cap=cap)
    # cancellation: 50 reserved, never sent -> released, their capacity reused by the next 50 in line
    for a in held[200:250]:
        gate.release(grants[a], "ENTRY_CANCELLED")
    assert admit(60) == 50
    K.reconcile(pdb, cap=cap)
    # crash recovery: a sweep settles every open link of an account from its attempt's DURABLE state only
    states = {}
    for i, a in enumerate(held[250:290]):
        states[a] = [("REJECTED", {}), ("SUBMIT_UNKNOWN", {}), None,
                     ("FILLED", {"filled_quantity": "0.0004", "filled_price": "50000"})][i % 4]
    swept = Counter()
    for a, state in states.items():
        out = gate.sweep(account_id=a, tenant_id=K.tenant(a), attempt_state=lambda _id, s=state: s)
        swept.update({k: v for k, v in out.items() if v})
    assert swept == {"released": 20, "kept": 10, "filled": 10}   # REJECTED + never claimed / unknown / filled
    with pdb.transaction(write=False) as conn:
        filled = {decimal_out(r[0]) for r in conn.all(
            "SELECT s.granted_notional FROM platform_order_slots s JOIN platform_spread_decisions d ON d.slot_id="
            "s.slot_id WHERE d.settle_reason='ATTEMPT_FILLED'")}
    assert filled == {Decimal("20")}                          # 0.0004 x 50,000: the actual fill, not the 30 granted
    K.reconcile(pdb, cap=cap)
    assert K.slot_totals(pdb)["TOTAL"] <= cap
    # five accounts that were refused still have an unlinked REQUESTED slot on the budget (the allocation path)
    with pdb.transaction(write=False) as conn:
        (budget_id,) = conn.one("SELECT budget_id FROM platform_symbol_budgets")
    late = [a for a in fleet if a not in grants and a not in queue][:5]
    for a in late:
        PlatformOrderBudget(pdb, gate.policy).request(budget_id, a, K.tenant(a), "30")
    # expiry: the budget's TTL elapses (moved in this temporary database): nothing of it counts any more ...
    with pdb.transaction() as conn:
        conn.execute("UPDATE platform_symbol_budgets SET expires_at=?", (conn.now_ms() - 1,))
    assert K.slot_totals(pdb)["TOTAL"] == 0
    # ... unused slots no attempt is linked to become EXPIRED; linked ones wait for their attempt's outcome
    assert G.expire_unused(pdb) == 5
    open_before = count(pdb, "SELECT COUNT(*) FROM platform_spread_decisions WHERE outcome='RESERVED' AND "
                             "settled_at IS NULL")
    assert open_before > 0
    for a in list(grants):
        gate.sweep(account_id=a, tenant_id=K.tenant(a), attempt_state=lambda _id: None)
    assert count(pdb, "SELECT COUNT(*) FROM platform_spread_decisions WHERE outcome='RESERVED' AND "
                      "settled_at IS NULL") == 0
    K.reconcile(pdb)
    # ... and a new signal's budget has the whole cap again
    fresh = [a for a in fleet if a not in grants][:400]
    codes = race(lambda a: K.reserve_one(gate, a, notional="30", family="NEXT_SIGNAL"), fresh)
    assert Counter(codes.values())["RESERVED"] == 333
    K.reconcile(pdb, cap=cap)


@pytest.mark.parametrize("book,code", [
    (lambda: K.Book(age_ms=60_000), "DEPTH_STALE"),
    (lambda: K.Book(levels=3), "DEPTH_INSUFFICIENT_LEVELS"),
    (lambda: K.Book(crossed=True), "DEPTH_CROSSED"),
    # 70 units on a book 10 apart: the walk reaches 50,130 > the 25 bps worst price (50,125)
    (lambda: K.Book(step=10.0), "MARKET_WALK_EXCEEDS_WORST_PRICE"),
    # 200 units on a book that holds 100 within the band
    (lambda: K.Book(), "DEPTH_INSUFFICIENT_FOR_QUANTITY"),
])
def test_unusable_depth_or_a_walk_past_the_slippage_bound_refuses_all_1000_and_holds_nothing(pdb, fleet, book, code):
    gate = G.PlatformSpreadGate(pdb, policy=K.test_only_calibrated("1000000"))
    qty = {"MARKET_WALK_EXCEEDS_WORST_PRICE": "70", "DEPTH_INSUFFICIENT_FOR_QUANTITY": "200"}.get(code, "0.001")
    codes = race(lambda a: K.reserve_one(gate, a, notional="50", qty=qty, client=book()), fleet)
    assert Counter(codes.values()) == {code: N}
    assert count(pdb, "SELECT COUNT(*) FROM platform_order_slots") == 0
    assert count(pdb, "SELECT COUNT(*) FROM platform_symbol_budgets") == 0


def test_the_conservative_cap_is_a_tenth_of_the_book_inside_25_bps_whatever_1000_accounts_want(pdb, fleet):
    """No absolute cap: the participation limit alone (10 % of the visible notional inside the worst price)."""
    gate = G.PlatformSpreadGate(pdb, policy=K.test_only_calibrated("1", absolute_cap_notional=None))
    codes = race(lambda a: K.reserve_one(gate, a, notional="5000", qty="0.1"), fleet)
    with pdb.transaction(write=False) as conn:
        cap = decimal_out(conn.scalar("SELECT cap_notional FROM platform_symbol_budgets"))
    band = sum(Decimal(f"{K.MID + 0.1 * i:.4f}") * 5 for i in range(20))                # every ask level is inside
    assert cap == band * Decimal("0.10")
    granted = Counter(codes.values())["RESERVED"]
    assert granted == int(cap // 5000) == 100 and K.slot_totals(pdb)["TOTAL"] <= cap
    K.reconcile(pdb, cap=cap)


def _allocate(db, ids, signal, *, cap="25000", per_slot=10):
    K.register(db, ids)
    pb = PlatformOrderBudget(db, K.test_only_calibrated(cap, max_orders_per_slot=per_slot))
    depth = DepthSnapshot.of(K.VENUE, K.SYMBOL, [(K.MID - 0.1 * (i + 1), 5) for i in range(20)],
                             [(K.MID + 0.1 * i, 5) for i in range(20)], db_now(db))
    bid = pb.open_budget(venue=K.VENUE, symbol=K.SYMBOL, signal_id=signal, side=BUY, depth=depth)
    for a in ids:
        pb.request(bid, a, K.tenant(a), "250")
    return pb, bid


def test_ordering_is_fair_deterministic_and_bounded_in_time_for_1000_accounts(pdb, tmp_path):
    ids = K.accounts(N)
    pb, bid = _allocate(pdb, ids, "signal-2026-10-31")
    with ThreadPoolExecutor(max_workers=8) as pool:            # every worker allocates at once: one outcome
        allocations = list(pool.map(lambda _: pb.allocate(bid, tick=Decimal("0.1")), range(8)))
    assert len({tuple((s.account_id, s.status, s.granted) for s in a.slots) for a in allocations}) == 1
    slots = pb.slots(bid)
    granted = [s.account_id for s in sorted(slots, key=lambda s: s.sequence) if s.status == "RESERVED"]
    order = sequence("signal-2026-10-31", ids)
    assert granted == order[:100]                             # the first 100 in the published rank order, no others
    assert [s.account_id for s in sorted(slots, key=lambda s: s.sequence)] == order
    assert {s.status for s in slots} == {"RESERVED", "DEFERRED"} and len(slots) == N
    # bounded scheduling: at most 10 orders start per slot interval, in rank order
    per_interval = Counter(s.scheduled_at for s in slots if s.status == "RESERVED")
    assert max(per_interval.values()) == 10 and len(per_interval) == 10
    times = [s.scheduled_at for s in sorted(slots, key=lambda s: s.sequence) if s.status == "RESERVED"]
    assert times == sorted(times)
    # reproducible: the same signal and accounts on another database give the same grants, same sequence
    other = Database(f"sqlite:///{(tmp_path / 'replay.db').as_posix()}", pool_size=4)
    migrate(other, all_migrations())
    pb2, bid2 = _allocate(other, ids, "signal-2026-10-31")
    pb2.allocate(bid2, tick=Decimal("0.1"))
    assert [(s.account_id, s.sequence, s.status) for s in sorted(pb2.slots(bid2), key=lambda s: s.sequence)] == \
        [(s.account_id, s.sequence, s.status) for s in sorted(slots, key=lambda s: s.sequence)]
    other.close()
    # a different signal draws a different head of the line: no customer is always first
    assert set(sequence("signal-2026-11-01", ids)[:100]) != set(granted)
    K.reconcile(pdb)


def test_over_200_signals_every_account_takes_its_turn_and_none_is_favoured():
    """The rank is sha256(signal | account): deterministic, so this is a fixed property of the rule, not a sample."""
    ids = K.accounts(N)
    first = Counter()
    for day in range(200):
        first.update(sequence(f"DAILY_TREND:2026-{day:03d}", ids)[:100])     # a 10 % cap, 200 daily signals
    assert len(first) == N                                     # nobody is never served
    assert max(first.values()) <= 40 and min(first.values()) >= 5          # expected 20 each
    assert rank_key("s", "a") == hashlib.sha256(b"s|a").hexdigest()


# ═══════════════════════════════════════════════ 3. the calibration tooling
def _report_module():
    spec = importlib.util.spec_from_file_location("liquidity_impact_report_under_test",
                                                  ROOT / "scripts" / "perf" / "liquidity_impact_report.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _synthetic_observations(db, ids, *, slope_bps_per_pct=Decimal("0.8")):
    """One observation per account, as ``record_entry_observation`` writes it, with a SYNTHETIC fill price: the
    visible-book walk plus ``slope`` bps of slippage per % of participation, plus seeded noise."""
    from app.trading_intelligence.scale.order_spreading import CONSERVATIVE_DEFAULT as P, aggregate_cap
    rnd = random.Random(20261031)
    now = db_now(db)
    depth = DepthSnapshot.of(K.VENUE, K.SYMBOL, [(K.MID - 0.1 * (i + 1), 5) for i in range(20)],
                             [(K.MID + 0.1 * i, 5) for i in range(20)], now)
    cap = aggregate_cap(depth, BUY, P, now_ms=now)
    for i, a in enumerate(ids):
        qty = Decimal(rnd.choice(["0.01", "0.1", "0.5", "1", "2", "5"]))
        notional = qty * cap.reference_price
        participation_pct = Decimal(100) * notional / cap.band_notional
        slip = slope_bps_per_pct * participation_pct + Decimal(rnd.randint(-20, 20)) / 100
        fill = cap.reference_price * (1 + slip / 10_000)
        partial = i % 17 == 0
        G.record_observation(db, source="ENTRY_ATTEMPT", reference_id=f"att-{a}", depth=depth, side=BUY, quantity=qty,
                             notional=notional, cap=cap, depth_age_ms=rnd.randint(50, 900), account_id=a,
                             tenant_id=K.tenant(a), fill_price=fill.quantize(Decimal("0.01")),
                             filled_quantity=qty / 2 if partial else qty, requested_quantity=qty,
                             entry_status="PARTIALLY_FILLED" if partial else "FILLED")


def test_the_impact_report_reads_1000_synthetic_observations_and_stays_uncalibrated(url, pdb):
    R = _report_module()
    ids = K.accounts(N)
    _synthetic_observations(pdb, ids)
    first = R.report_from_database(url)
    again = R.report_from_database(url)
    assert (first["status"], first["calibrated"], first["evidence_class"]) == ("UNCALIBRATED", False, "SYNTHETIC")
    assert first["usable_for_calibration"] is False and first["candidate_for_owner_review"] is False
    assert first["observations_total"] == N
    assert (first["input_fingerprint"], first["report_fingerprint"]) == (again["input_fingerprint"],
                                                                          again["report_fingerprint"])
    (group,) = first["groups"].values()
    assert group["status"] == "UNCALIBRATED" and "SYNTHETIC_OBSERVATIONS" in group["not_usable_because"]
    assert group["impact_models_seen"] == [G.IMPACT_MODEL] and group["any_row_claims_calibrated_impact"] is False
    fit = group["candidate"]
    assert fit["n"] == N and abs(fit["slope_bps_per_participation_pct"] - 0.8) < 0.05    # it recovers the synthetic rule
    assert group["partial_fill_rate"] == round(len(range(0, N, 17)) / N, 6)
    # nothing was written: the spreading tables are exactly as before
    assert count(pdb, "SELECT COUNT(*) FROM platform_spread_observations") == N
    assert count(pdb, "SELECT COUNT(*) FROM platform_symbol_budgets") == 0
    # declared real provenance is still only a CANDIDATE for owner review, never a calibration
    recorded = R.report_from_database(url, provenance="recorded")
    assert (recorded["status"], recorded["calibrated"], recorded["usable_for_calibration"]) == (
        "UNCALIBRATED", False, False)
    assert recorded["candidate_for_owner_review"] is True and recorded["evidence_class"].startswith("RECORDED")


def test_the_impact_report_gives_no_coefficients_below_the_minimum_and_never_writes(url, pdb):
    R = _report_module()
    _synthetic_observations(pdb, K.accounts(40))
    report = R.report_from_database(url)
    (group,) = report["groups"].values()
    assert group["candidate"] is None
    assert any(r.startswith("INSUFFICIENT_FILLED_OBSERVATIONS:40<") for r in group["not_usable_because"])
    source = (ROOT / "scripts" / "perf" / "liquidity_impact_report.py").read_text(encoding="utf-8")
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE)\b", source.split('"""', 2)[2])
    assert not re.search(r"calibrated\s*=\s*True|approval_ref\s*=", source)
    assert "transaction(write=False)" in source and "transaction()" not in source
