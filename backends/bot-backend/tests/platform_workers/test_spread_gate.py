"""Step 5.5 / EXT-LIQ-01: the platform order budget as the execution boundary uses it (``scale.spread_gate``).

Every test runs on SQLite and on PostgreSQL (``platform_db`` / ``database_url``). Plans are minimal stand-ins with
the fields the gate reads; the venue client is a fake that answers an order-book read and nothing else.

One test (``test_concurrent_workers_never_exceed_the_symbol_cap_with_a_calibrated_mechanism``) builds a policy
with ``calibrated=True`` and a TEST-ONLY approval reference in a throwaway database: it exercises the shared cap
across accounts, which an uncalibrated policy never lets two accounts reach. No product code builds such a
policy (``test_no_product_code_builds_a_calibrated_policy``).
"""
from __future__ import annotations

import re
import threading
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.platform_workers.assignments import WorkerAssignments
from app.trading_intelligence.scale import spread_gate as G
from app.trading_intelligence.scale.order_spreading import CONSERVATIVE_DEFAULT, SpreadPolicy
from shared_lib.operations.spreading_status import spreading_status
from shared_lib.persistence.sql import Database, decimal_out

MARKET_OK = SpreadPolicy(policy_version="test-market-entry-uncalibrated", allow_market_fallback=True)
VENUE, SYMBOL = "BINANCE_USDM", "BTCUSDT"


class Book:
    """A fake venue client: ``get_orderbook`` only, ten levels a side, venue time = now unless told otherwise."""

    def __init__(self, mid=100.0, levels=10, qty="1000", age_ms=0, stamp=True, fail=None):
        self.mid, self.levels, self.qty, self.age_ms, self.stamp, self.fail = mid, levels, qty, age_ms, stamp, fail
        self.reads = 0

    def get_orderbook(self, symbol, limit=50):
        self.reads += 1
        if self.fail:
            raise self.fail
        out = {"bids": [[f"{self.mid - 0.01 * (i + 1):.2f}", self.qty] for i in range(self.levels)],
               "asks": [[f"{self.mid + 0.01 * i:.2f}", self.qty] for i in range(self.levels)]}
        if self.stamp:
            out["time"] = int(time.time() * 1000) - self.age_ms
        return out


def plan(account="a1", user="u1", side="LONG", family="FAM", decision_time=1_780_000_000_000, symbol=SYMBOL,
         style="MARKET"):
    return SimpleNamespace(
        trade_plan_id=f"tp-{account}-{decision_time}-{side}", broker_account_id=account, user_id=user, side=side,
        venue=VENUE, environment="DEMO", instrument_key=SimpleNamespace(venue_symbol=symbol), setup_family=family,
        setup_version="1.0.0", decision_time=decision_time, versions=(), mode="DEMO", source_candidate_id="d",
        plan_expiry_time=decision_time + 1, execution_preferences=SimpleNamespace(preferred_order_style=style,
                                                                                  fallback_order_styles=()))


def register(db, *accounts, tenant=None):
    WorkerAssignments(db).upsert_accounts([(a, tenant or f"u-{a}", VENUE, "DEMO") for a in accounts])


def reserve(gate, p, *, qty="1", notional="100", client=None, attempt=None):
    return gate.reserve(p, client=client or Book(), adapter=None, quantity=Decimal(qty), notional=Decimal(notional),
                        attempt_id=attempt or f"xatt-{p.trade_plan_id}", client_order_id=f"cid-{p.trade_plan_id}",
                        tick=Decimal("0.01"))


def refused(fn):
    with pytest.raises(G.SpreadRefused) as e:
        fn()
    return e.value.code


def slot_row(db, slot_id):
    with db.transaction(write=False) as conn:
        r = conn.one("SELECT status, granted_notional, requested_notional, reason FROM platform_order_slots "
                     "WHERE slot_id=?", (slot_id,))
    return r[0], decimal_out(r[1]), decimal_out(r[2]), r[3]


def decisions(db, outcome=None):
    with db.transaction(write=False) as conn:
        q = "SELECT outcome, code, execution_attempt_id, client_order_id, slot_id, settled_status, settled_notional " \
            "FROM platform_spread_decisions" + (" WHERE outcome=?" if outcome else "") + " ORDER BY created_at"
        return conn.all(q, (outcome,) if outcome else ())


def active_slots(db):
    with db.transaction(write=False) as conn:
        return conn.all("SELECT account_id, status FROM platform_order_slots WHERE status IN "
                        "('RESERVED','SUBMITTED','FILLED')")


# ── depth ──────────────────────────────────────────────────────────────────────────────────────────────────
def test_read_depth_refuses_anything_but_a_fresh_stamped_well_formed_book():
    assert G.read_depth(Book(), VENUE, SYMBOL).asks[0] == (Decimal("100.00"), Decimal("1000"))
    for client, code in ((None, G.DEPTH_UNAVAILABLE), (object(), G.DEPTH_UNAVAILABLE),
                         (Book(fail=TimeoutError()), G.DEPTH_UNAVAILABLE), (Book(stamp=False), G.DEPTH_TIMESTAMP_MISSING)):
        with pytest.raises(G.DepthRefused) as e:
            G.read_depth(client, VENUE, SYMBOL)
        assert e.value.code == code
    bad = SimpleNamespace(get_orderbook=lambda s, limit: {"bids": [["x", "1"]], "asks": [], "time": 1})
    with pytest.raises(G.DepthRefused):
        G.read_depth(bad, VENUE, SYMBOL)
    unordered = SimpleNamespace(get_orderbook=lambda s, limit: {"bids": [["1", "1"], ["2", "1"]], "asks": [], "time": 1})
    with pytest.raises(G.DepthRefused):
        G.read_depth(unordered, VENUE, SYMBOL)


@pytest.mark.parametrize("client,code", [(Book(age_ms=60_000), "DEPTH_STALE"), (Book(levels=3), "DEPTH_INSUFFICIENT_LEVELS"),
                                         (Book(fail=ConnectionError()), "DEPTH_UNAVAILABLE"),
                                         (Book(stamp=False), "DEPTH_TIMESTAMP_MISSING")])
def test_stale_thin_or_unread_depth_refuses_and_holds_nothing(platform_db, client, code):
    register(platform_db, "a1", tenant="u1")
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    assert refused(lambda: reserve(gate, plan(), client=client)) == code
    assert active_slots(platform_db) == []
    assert [(r[0], r[1]) for r in decisions(platform_db)] == [("REFUSED", code)]


def test_crossed_depth_is_refused(platform_db):
    register(platform_db, "a1", tenant="u1")
    crossed = SimpleNamespace(get_orderbook=lambda s, limit: {
        "bids": [[str(101 - i * 0.01), "5"] for i in range(6)], "asks": [[str(100 + i * 0.01), "5"] for i in range(6)],
        "time": int(time.time() * 1000)})
    assert refused(lambda: reserve(G.PlatformSpreadGate(platform_db, policy=MARKET_OK), plan(), client=crossed)) \
        == "DEPTH_CROSSED"


# ── market entries ─────────────────────────────────────────────────────────────────────────────────────────
def test_market_entry_is_refused_by_the_default_policy(platform_db):
    register(platform_db, "a1", tenant="u1")
    assert not CONSERVATIVE_DEFAULT.allow_market_fallback and not CONSERVATIVE_DEFAULT.calibrated
    gate = G.PlatformSpreadGate(platform_db)                      # the production default policy
    assert refused(lambda: reserve(gate, plan())) == "MARKET_FALLBACK_DISABLED_BY_POLICY"
    # every entry adapter of this repository sends MARKET: even a LIMIT preference is a market entry
    assert G.entry_order_style(None, plan(style="LIMIT")) == "MARKET"
    assert active_slots(platform_db) == []


def test_market_walk_must_stay_within_the_worst_price(platform_db):
    register(platform_db, "a1", tenant="u1")
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    # 25 bps from a best ask of 100.00 -> worst 100.25; the visible book holds 10 x 1 units up to 100.09 only
    thin = Book(qty="1")
    assert refused(lambda: reserve(gate, plan(), qty="15", notional="1500", client=thin)) == \
        "DEPTH_INSUFFICIENT_FOR_QUANTITY"
    wide = SimpleNamespace(get_orderbook=lambda s, limit: {
        "bids": [[f"{99.99 - i:.2f}", "1"] for i in range(10)], "asks": [[f"{100 + i:.2f}", "1"] for i in range(10)],
        "time": int(time.time() * 1000)})
    assert refused(lambda: reserve(gate, plan(), qty="3", notional="300", client=wide)) == \
        "MARKET_WALK_EXCEEDS_WORST_PRICE"


# ── reservation and calibration boundary ──────────────────────────────────────────────────────────────────
def test_single_account_reserves_the_full_notional_linked_to_its_attempt(platform_db):
    register(platform_db, "a1", tenant="u1")
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    p = plan()
    grant = reserve(gate, p, qty="2.5", notional="250.125")
    status, granted, requested, _ = slot_row(platform_db, grant.slot_id)
    assert (status, granted, requested) == ("RESERVED", Decimal("250.125"), Decimal("250.125"))
    assert not grant.calibrated and grant.policy_version == MARKET_OK.policy_version
    [row] = decisions(platform_db)
    assert row[:5] == ("RESERVED", None, f"xatt-{p.trade_plan_id}", f"cid-{p.trade_plan_id}", grant.slot_id)
    assert grant.signal_id == G.signal_id_of(p) == "sig:FAM|1.0.0|1780000000000"


def test_partial_grant_and_exhausted_budget_refuse_without_resizing(platform_db):
    register(platform_db, "a1", "a2", tenant="u1")
    small = SpreadPolicy(policy_version="t", allow_market_fallback=True, absolute_cap_notional=Decimal("99.99"))
    assert refused(lambda: reserve(G.PlatformSpreadGate(platform_db, policy=small), plan())) == \
        "PLATFORM_SYMBOL_BUDGET_PARTIAL_GRANT"
    none = SpreadPolicy(policy_version="t", allow_market_fallback=True, absolute_cap_notional=Decimal("0"))
    assert refused(lambda: reserve(G.PlatformSpreadGate(platform_db, policy=none), plan(family="OTHER"))) == \
        "PLATFORM_SYMBOL_BUDGET_EXHAUSTED"
    assert active_slots(platform_db) == []


def test_uncalibrated_policy_refuses_a_second_account_on_the_same_symbol_side(platform_db):
    register(platform_db, "a1", "a2", tenant="u1")
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    first = reserve(gate, plan("a1"))
    # same signal, and another signal on the same venue/symbol/side: both refused (EXT-LIQ-01)
    assert refused(lambda: reserve(gate, plan("a2"))) == "ORDER_SPREADING_CALIBRATION_REQUIRED"
    assert refused(lambda: reserve(gate, plan("a2", family="OTHER"))) == "ORDER_SPREADING_CALIBRATION_REQUIRED"
    # the other side of the book is another (venue, symbol, side): one account there is fine
    reserve(gate, plan("a2", side="SHORT"))
    # a filled slot still counts until its budget expires
    gate.mark_submitted(first)
    gate.settle_attempt(account_id="a1", tenant_id="u1", attempt_id=f"xatt-{plan('a1').trade_plan_id}",
                        outcome="FILLED", filled_notional=Decimal("100"))
    assert refused(lambda: reserve(gate, plan("a2"))) == "ORDER_SPREADING_CALIBRATION_REQUIRED"


def test_released_slot_frees_the_symbol_for_another_account(platform_db):
    register(platform_db, "a1", "a2", tenant="u1")
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    gate.release(reserve(gate, plan("a1")), "VENUE_REJECTED")
    second = reserve(gate, plan("a2"))
    assert slot_row(platform_db, second.slot_id)[0] == "RESERVED"


def test_account_must_be_registered_to_its_own_tenant(platform_db):
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    assert refused(lambda: reserve(gate, plan("ghost"))) == "PLATFORM_ACCOUNT_NOT_REGISTERED"
    register(platform_db, "a1", tenant="someone-else")
    assert refused(lambda: reserve(gate, plan("a1", user="u1"))) == "PLATFORM_ACCOUNT_TENANT_MISMATCH"
    assert refused(lambda: reserve(gate, plan("a1", user=None))) == "PLATFORM_ACCOUNT_TENANT_UNKNOWN"


def test_no_platform_database_or_no_schema_refuses(database_url, monkeypatch, tmp_path):
    monkeypatch.delenv("PLATFORM_DATABASE_URL", raising=False)
    gate = G.PlatformSpreadGate()
    assert refused(lambda: reserve(gate, plan())) == "PLATFORM_ORDER_BUDGET_UNAVAILABLE"
    bare = Database(database_url, pool_size=2)                    # reachable, but no platform migrations
    try:
        assert refused(lambda: reserve(G.PlatformSpreadGate(bare), plan())) == "PLATFORM_ORDER_BUDGET_UNAVAILABLE"
        monkeypatch.setenv("PLATFORM_DATABASE_URL", database_url)
        assert refused(lambda: reserve(G.PlatformSpreadGate(), plan())) == "PLATFORM_ORDER_BUDGET_UNAVAILABLE"
    finally:
        bare.close()
        cached = G._databases.pop(database_url, None)
        if cached is not None:
            cached.close()


# ── concurrency (threads; both dialects) ──────────────────────────────────────────────────────────────────
def _race(database_url, policy, accounts, notional, signals=("FAM",)):
    results, errors, barrier = {}, [], threading.Barrier(len(accounts))

    def worker(i, account):
        db = Database(database_url, pool_size=2)
        try:
            gate = G.PlatformSpreadGate(db, policy=policy)
            barrier.wait()
            try:
                reserve(gate, plan(account, user="u1", family=signals[i % len(signals)]), notional=notional)
                results[account] = "RESERVED"
            except G.SpreadRefused as exc:
                results[account] = exc.code
        except Exception as exc:  # pragma: no cover - surfaced by the assertion below
            errors.append(exc)
        finally:
            db.close()

    threads = [threading.Thread(target=worker, args=(i, a)) for i, a in enumerate(accounts)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors, errors
    return results


def test_two_accounts_cannot_both_pass_while_uncalibrated(database_url, platform_db):
    accounts = [f"r{i:02d}" for i in range(12)]
    register(platform_db, *accounts, tenant="u1")
    results = _race(database_url, MARKET_OK, accounts, "100", signals=("FAM", "OTHER"))
    assert sorted(results.values()).count("RESERVED") == 1
    assert {v for v in results.values() if v != "RESERVED"} == {"ORDER_SPREADING_CALIBRATION_REQUIRED"}
    assert len(active_slots(platform_db)) == 1


def test_concurrent_workers_never_exceed_the_symbol_cap_with_a_calibrated_mechanism(database_url, platform_db):
    # TEST-ONLY calibrated policy in a throwaway database: the only way more than one account reaches the cap
    mechanism = SpreadPolicy(policy_version="test-only-calibrated-mechanism", allow_market_fallback=True,
                             absolute_cap_notional=Decimal("1000"), calibrated=True,
                             approval_ref="TEST-ONLY-NOT-AN-OWNER-APPROVAL", max_orders_per_slot=100)
    accounts = [f"c{i:02d}" for i in range(16)]
    register(platform_db, *accounts, tenant="u1")
    results = _race(database_url, mechanism, accounts, "150", signals=("FAM", "OTHER"))
    granted = [a for a, v in results.items() if v == "RESERVED"]
    assert len(granted) == 6                                            # 6 x 150 <= 1,000 < 7 x 150
    assert set(results.values()) - {"RESERVED"} <= {"PLATFORM_SYMBOL_BUDGET_PARTIAL_GRANT",
                                                     "PLATFORM_SYMBOL_BUDGET_EXHAUSTED"}
    with platform_db.transaction(write=False) as conn:
        total = sum((decimal_out(r[0]) for r in conn.all(
            "SELECT granted_notional FROM platform_order_slots WHERE status='RESERVED'")), Decimal(0))
    assert total == Decimal("900") <= Decimal("1000")


def test_no_product_code_builds_a_calibrated_policy():
    app = Path(G.__file__).resolve().parents[2]
    scale = Path(G.__file__).resolve().parent
    for path in app.rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        assert not re.search(r"SpreadPolicy\([^)]*(calibrated\s*=\s*True|approval_ref\s*=)", text), path
        if path.parent == scale:
            assert not re.search(r"(?<![\w.])calibrated\s*=\s*True", text), path


# ── lifecycle ──────────────────────────────────────────────────────────────────────────────────────────────
def test_submitted_then_partial_fill_shrinks_the_slot_to_the_actual_notional(platform_db):
    register(platform_db, "a1", tenant="u1")
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    grant = reserve(gate, plan(), qty="2", notional="200")
    gate.mark_submitted(grant)
    assert slot_row(platform_db, grant.slot_id)[0] == "SUBMITTED"
    gate.settle_attempt(account_id="a1", tenant_id="u1", attempt_id=f"xatt-{plan().trade_plan_id}", outcome="FILLED",
                        filled_notional=Decimal("0.5") * Decimal("100.01"))
    assert slot_row(platform_db, grant.slot_id)[:2] == ("FILLED", Decimal("50.005"))
    row = decisions(platform_db)[0]
    assert row[5] == "FILLED" and decimal_out(row[6]) == Decimal("50.005")


def test_submit_unknown_keeps_the_slot_until_reconciliation(platform_db):
    register(platform_db, "a1", "a2", tenant="u1")
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    grant = reserve(gate, plan())
    gate.mark_submitted(grant)
    attempt = f"xatt-{plan().trade_plan_id}"
    gate.settle_attempt(account_id="a1", tenant_id="u1", attempt_id=attempt, outcome="KEEP")
    assert slot_row(platform_db, grant.slot_id)[0] == "SUBMITTED"
    assert refused(lambda: reserve(gate, plan("a2"))) == "ORDER_SPREADING_CALIBRATION_REQUIRED"
    gate.settle_attempt(account_id="a1", tenant_id="u1", attempt_id=attempt, outcome="RELEASED",
                        reason="RECONCILED_NO_POSITION")
    assert slot_row(platform_db, grant.slot_id)[0] == "RELEASED"
    assert decisions(platform_db, "RESERVED")[0][5] == "RELEASED"
    reserve(gate, plan("a2"))


def test_another_tenant_cannot_move_a_slot(platform_db):
    register(platform_db, "a1", tenant="u1")
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    grant = reserve(gate, plan())
    assert gate.settle_attempt(account_id="a1", tenant_id="intruder", attempt_id=f"xatt-{plan().trade_plan_id}",
                               outcome="RELEASED") == 0
    assert slot_row(platform_db, grant.slot_id)[0] == "RESERVED"


def test_crash_recovery_sweep_settles_every_open_slot_from_its_attempt(platform_db):
    register(platform_db, *(f"s{i}" for i in range(5)), tenant="u1")
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK, claim_grace_ms=0)
    grants = {}
    for i, account in enumerate(f"s{i}" for i in range(5)):
        # one account per symbol: the uncalibrated boundary allows exactly that
        grants[account] = reserve(gate, plan(account, user="u1", symbol=f"SYM{i}USDT"), attempt=f"att-{account}")
    states = {"att-s0": None,                                              # crashed before the claim
              "att-s1": ("PENDING_SUBMIT", {}),                            # crashed after the claim, before the mark
              "att-s2": ("FILLED", {"filled_quantity": 0.75, "filled_price": 100.02}),
              "att-s3": ("REJECTED", {}),
              "att-s4": ("SUBMIT_UNKNOWN", {})}
    counts = {}
    for account in grants:
        out = gate.sweep(account_id=account, tenant_id="u1", attempt_state=states.get)
        counts = {k: counts.get(k, 0) + v for k, v in out.items()}
    assert counts == {"filled": 1, "released": 2, "kept": 2, "expired": 0}
    status = {a: slot_row(platform_db, g.slot_id) for a, g in grants.items()}
    assert status["s0"][0] == "RELEASED" and status["s0"][3] == "ATTEMPT_NEVER_CLAIMED"
    assert status["s1"][0] == "SUBMITTED" and status["s4"][0] == "SUBMITTED"
    assert status["s2"][:2] == ("FILLED", Decimal("0.75") * Decimal("100.02"))
    assert status["s3"][0] == "RELEASED"
    # a young unclaimed reservation is left to the boundary that is about to claim it
    patient = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    register(platform_db, "s9", tenant="u1")
    young = reserve(patient, plan("s9", user="u1", symbol="YOUNGUSDT"), attempt="att-s9")
    assert patient.sweep(account_id="s9", tenant_id="u1", attempt_state=lambda a: None)["released"] == 0
    assert slot_row(platform_db, young.slot_id)[0] == "RESERVED"


def test_budget_ttl_expiry_frees_capacity_and_expires_unused_slots(platform_db):
    register(platform_db, "a1", "a2", "a3", tenant="u1")
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    linked = reserve(gate, plan("a1"))
    from app.trading_intelligence.scale.order_spreading import PlatformOrderBudget
    with platform_db.transaction() as conn:                             # an unlinked request on the same budget
        budget = conn.scalar("SELECT budget_id FROM platform_order_slots WHERE slot_id=?", (linked.slot_id,))
    PlatformOrderBudget(platform_db, MARKET_OK).request(budget, "a3", "u1", "10")
    with platform_db.transaction() as conn:                             # the TTL elapses (database clock)
        conn.execute("UPDATE platform_symbol_budgets SET expires_at=?", (conn.now_ms() - 1,))
    assert refused(lambda: reserve(gate, plan("a2"))) == "BUDGET_EXPIRED"
    reserve(gate, plan("a2", family="NEXT"))                            # an expired budget no longer counts
    assert G.expire_unused(platform_db) == 1                            # a3's request; a1 is still linked
    assert slot_row(platform_db, linked.slot_id)[0] == "RESERVED"
    gate.settle_attempt(account_id="a1", tenant_id="u1", attempt_id=f"xatt-{plan('a1').trade_plan_id}",
                        outcome="RELEASED", reason="ATTEMPT_ERROR_PRE_SUBMIT")
    assert slot_row(platform_db, linked.slot_id)[0] == "RELEASED"


# ── calibration observations ───────────────────────────────────────────────────────────────────────────────
def test_entry_observation_is_recorded_with_exact_decimals(platform_db):
    register(platform_db, "a1", tenant="u1")
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    grant = reserve(gate, plan(), qty="2", notional="200")
    entry = SimpleNamespace(avg_fill_price=100.015, filled_qty=1.5, requested_qty=2.0, status="PARTIALLY_FILLED")
    assert gate.record_entry_observation(grant, attempt_id="att-1", entry=entry)
    assert not gate.record_entry_observation(grant, attempt_id="att-1", entry=entry)    # idempotent per attempt
    with platform_db.transaction(write=False) as conn:
        r = conn.one("SELECT intended_quantity, intended_notional, best_bid, best_ask, spread_bps, band_notional, "
                     "reference_price, fill_price, filled_quantity, partial_fill, realized_slippage_bps, "
                     "impact_estimate_bps, impact_model, impact_calibrated, depth_levels, depth_age_ms "
                     "FROM platform_spread_observations WHERE reference_id='att-1'")
    assert not any(isinstance(v, float) for v in r)
    money = [decimal_out(v) for v in r[:12] if v is not None and not isinstance(v, int)]
    assert all(isinstance(v, Decimal) for v in money)
    assert decimal_out(r[0]) == Decimal("2") and decimal_out(r[1]) == Decimal("200")
    assert (decimal_out(r[2]), decimal_out(r[3])) == (Decimal("99.99"), Decimal("100.00"))
    assert decimal_out(r[7]) == Decimal("100.015") and decimal_out(r[8]) == Decimal("1.5") and r[9] == 1
    assert decimal_out(r[10]) == Decimal("1.50000000")                 # (100.015 - 100.00) / 100.00 * 1e4
    assert decimal_out(r[11]) == Decimal("0E-8") or decimal_out(r[11]) == 0  # 2 units at the best ask
    assert r[12] == G.IMPACT_MODEL == "VISIBLE_BOOK_WALK_UNCALIBRATED" and r[13] == 0
    assert r[14] == 10 and 0 <= int(r[15]) < 5_000


def test_a_failing_recorder_never_raises(platform_db):
    register(platform_db, "a1", tenant="u1")
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    grant = reserve(gate, plan())
    with platform_db.transaction() as conn:
        conn.execute("DROP TABLE platform_spread_observations")
    assert gate.record_entry_observation(grant, attempt_id="att-x", entry=SimpleNamespace(avg_fill_price=1.0)) is False


def test_shadow_measurement_api_records_without_an_order(platform_db):
    depth = G.read_depth(Book(), VENUE, SYMBOL)
    assert G.record_observation(platform_db, source="SHADOW", reference_id="shadow-1", depth=depth, side="SELL",
                                quantity=Decimal("3"), fill_price="99.98", filled_quantity="3")
    with platform_db.transaction(write=False) as conn:
        r = conn.one("SELECT side, impact_estimate_bps, realized_slippage_bps, partial_fill FROM "
                     "platform_spread_observations WHERE source='SHADOW'")
    assert r[0] == "SELL" and decimal_out(r[1]) == 0 and decimal_out(r[2]) == Decimal("1.00010001") and r[3] == 0


# ── administrative view ────────────────────────────────────────────────────────────────────────────────────
def test_status_reports_the_calibration_blocker_and_refusals(platform_db):
    register(platform_db, "a1", "a2", tenant="u1")
    gate = G.PlatformSpreadGate(platform_db, policy=MARKET_OK)
    reserve(gate, plan("a1"))
    refused(lambda: reserve(gate, plan("a2")))
    refused(lambda: reserve(G.PlatformSpreadGate(platform_db), plan("a2", side="SHORT")))
    view = spreading_status(platform_db)
    assert view["available"] and view["external_gate"] == "EXT-LIQ-01" and view["calibrated"] is False
    assert view["budgets"]["calibrated"] == 0 and view["budgets"]["uncalibrated"] == 1   # refused: none opened
    assert view["refusals_by_code"] == {"MARKET_FALLBACK_DISABLED_BY_POLICY": 1,
                                        "ORDER_SPREADING_CALIBRATION_REQUIRED": 1}
    assert view["reservations"] == {"OPEN": 1}
