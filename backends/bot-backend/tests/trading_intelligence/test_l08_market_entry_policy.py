"""Master plan L-08 / EXT-LIQ-01: the governed single-account market-entry policy -- built, tested, NOT activated.

Every test runs on a fake venue (``_exec.Harness``: a MagicMock exchange client and its order book) over a migrated,
disposable platform database of each dialect (SQLite always; PostgreSQL when ``PLATFORM_TEST_POSTGRES_URL`` is set,
``PLATFORM_TEST_POSTGRES_REQUIRED=1`` turning its absence into a failure). No record exists outside these
temporary databases: the production default refuses every MARKET entry (``MARKET_FALLBACK_DISABLED_BY_POLICY``).

Part 1 -- the ledger (validation, append-only, hash chain, LIVE refused, no calibration).
Part 2 -- the gate and the real boundary, gate injected over the platform database with the PRODUCTION default
          policy (``CONSERVATIVE_DEFAULT``): only an approved record permits the entry, each condition refusing
          with its own code.
Part 3 -- production settings (the real family gate on a SYNTHETIC certified family, the real go-live gate, the
          account fence, the adapter and capability checks): every gate before and after the order budget still
          applies with a record present.
Part 4 -- the administrative interface (operator token only).
"""
from __future__ import annotations

import json
import time
from decimal import Decimal
from types import SimpleNamespace

import pytest
from _exec import Harness, _entry, _order, order_book

from app.trading_intelligence.execution.boundary import BoundaryStatus as B
from app.trading_intelligence.pipeline.family_governance import OperatorAuthorization
from app.trading_intelligence.scale import market_entry_policy as M
from app.trading_intelligence.scale import spread_gate as G
from app.trading_intelligence.scale.order_spreading import CONSERVATIVE_DEFAULT
from shared_lib.persistence.sql import decimal_out

from test_step5_spread_boundary import links, pdb, platform_url, register, rows, slots  # noqa: F401 (fixtures)
from test_production_execution import live  # noqa: F401  (fixture)
from test_production_demo_execution import demo  # noqa: F401  (fixture: `fresh` depends on it)
from test_production_research_separation import fresh, process  # noqa: F401  (fixture)
from test_step3_execution_tail import certified  # noqa: F401  (fixture)

OP = OperatorAuthorization(operator_id="operator-7", channel="test:l08", permission="admin-emergency")
APPROVAL = "OWNER-APPROVAL-TEST-ONLY-L08"
DAY = 86_400_000


def db_now(db):
    with db.transaction(write=False) as conn:
        return conn.now_ms()


def terms(db, **overrides):
    base = dict(max_participation="0.10", max_slippage_bps="25", max_spread_bps="10", max_depth_age_ms=5000,
                min_levels=5, absolute_cap_notional="5000", expires_at_ms=db_now(db) + DAY)
    base.update(overrides)
    return base


def approve(db, plan, *, environment=None, symbol=None, account_id=None, **overrides):
    return M.MarketEntryPolicyLedger(db).record(
        OP, venue=plan.venue, symbol=symbol or plan.instrument_key.venue_symbol,
        environment=environment or plan.environment, terms=terms(db, **overrides), approval_ref=APPROVAL,
        reason="test-only approval inside a disposable database", account_id=account_id)


def default_gate(db, **kw):
    """What production constructs: the code-constant default policy. Only a record can permit a MARKET entry."""
    gate = G.PlatformSpreadGate(db, **kw)
    assert gate.policy is CONSERVATIVE_DEFAULT and gate.policy.allow_market_fallback is False
    return gate


def harness(tmp_path, pdb, **kw):  # noqa: F811
    h = Harness(tmp_path, **kw)
    register(pdb, h.plan)
    h.gate = default_gate(pdb)
    return h


#: The harness sizes a fixed 4.8 units with the stop 5 below the entry: 24 of risk. On the harness's 5 000 equity
#: that is 0.48 % -- refused at the executable price (proved below). The acceptance runs use 10 000 (0.24 %).
EQUITY = 10_000.0


def run(h, **kw):
    from app.trading_intelligence.execution.boundary import AccountState
    account = kw.pop("account", AccountState(EQUITY, 0.0, EQUITY, 0))
    b = h.boundary(spread_gate=h.gate, **kw)
    return b, h.run(b, account=account)


def refused_before_the_venue(h, res, code, pdb):  # noqa: F811
    assert (res.status, res.reason_codes) == (B.ORDER_SPREADING_REFUSED, (code,)), res
    assert h.seen["orders"] == [] and h.client.place_order.call_count == 0
    assert [s for s in slots(pdb) if s[0] == h.plan.broker_account_id and s[1] in ("RESERVED", "SUBMITTED",
                                                                                   "FILLED")] == []
    assert h.reservation_status() == "RELEASED"


def other_plan(plan, account="other-acct", user="other-user", **changes):
    """Another customer's plan on the same venue / symbol / side (a gate-level stand-in, as in the Step 5.5 tests)."""
    base = {k: getattr(plan, k) for k in ("venue", "environment", "instrument_key", "side", "setup_version",
                                          "decision_time", "versions", "execution_preferences", "plan_expiry_time")}
    base.update({"broker_account_id": account, "user_id": user, "setup_family": "OTHER", "trade_plan_id": f"tp-{account}",
                 "mode": "DEMO", "source_candidate_id": f"d-{account}"})
    base.update(changes)
    return SimpleNamespace(**base)


def reserve(h, plan, **kw):
    kw.setdefault("quantity", Decimal("1"))
    kw.setdefault("notional", Decimal("100"))
    kw.setdefault("stop_price", Decimal("98"))
    kw.setdefault("equity", Decimal("5000"))
    return h.gate.reserve(plan, client=h.client, adapter=h.adapter, attempt_id=f"att-{plan.broker_account_id}-"
                          f"{time.monotonic_ns()}", **kw)


# ═══════════════════════════════════════════════════════════════════ part 1: the ledger
def test_a_record_needs_an_operator_an_approval_reference_and_a_reason(pdb):  # noqa: F811
    ledger = M.MarketEntryPolicyLedger(pdb)
    common = dict(venue="BINANCE_USDM", symbol="BTCUSDT", environment="DEMO", terms=terms(pdb))
    for operator, ref, reason, code in [
            (None, APPROVAL, "why", M.OPERATOR_REQUIRED),
            (SimpleNamespace(operator_id="x", channel="c", permission="p"), APPROVAL, "why", M.OPERATOR_REQUIRED),
            (OP, None, "a reason", M.APPROVAL_REF_REQUIRED),
            (OP, "  ", "a reason", M.APPROVAL_REF_REQUIRED),
            (OP, APPROVAL, None, M.REASON_REQUIRED),
            (OP, APPROVAL, "", M.REASON_REQUIRED)]:
        with pytest.raises(M.MarketPolicyError) as err:
            ledger.record(operator, approval_ref=ref, reason=reason, **common)
        assert err.value.code == code
    assert ledger.events() == []
    rec = ledger.record(OP, approval_ref=APPROVAL, reason="owner approved, test only", **common)
    assert rec.approval_ref == APPROVAL and rec.actor_ref == "operator:operator-7@test:l08"
    assert rec.policy_version == "single-account-market-entry:" + rec.policy_id and rec.active(db_now(pdb))
    assert ledger.verify() == (True, "")


@pytest.mark.parametrize("override,code", [
    (dict(max_participation="0.11"), M.TERMS_INVALID),        # looser than the conservative default
    (dict(max_participation="0"), M.TERMS_INVALID),
    (dict(max_slippage_bps="25.01"), M.TERMS_INVALID),
    (dict(max_slippage_bps="-1"), M.TERMS_INVALID),
    (dict(max_spread_bps="0"), M.TERMS_INVALID),
    (dict(max_spread_bps="26"), M.TERMS_INVALID),
    (dict(max_depth_age_ms=5001), M.TERMS_INVALID),
    (dict(max_depth_age_ms=10), M.TERMS_INVALID),
    (dict(min_levels=4), M.TERMS_INVALID),
    (dict(min_levels=51), M.TERMS_INVALID),
    (dict(absolute_cap_notional="0"), M.TERMS_INVALID),
    (dict(absolute_cap_notional="100000.01"), M.TERMS_INVALID),
    (dict(absolute_cap_notional=1000.0), M.TERMS_INVALID),     # a float is never money
    (dict(absolute_cap_notional="NaN"), M.TERMS_INVALID),
    (dict(expires_at_ms=1), M.TERMS_INVALID),                  # in the past
    (dict(expires_at_ms=None), M.TERMS_INVALID),
    (dict(extra_knob=1), M.TERMS_INVALID),
    (dict(calibrated=True), M.CALIBRATION_NOT_PERMITTED),
    (dict(approval_ref="x"), M.CALIBRATION_NOT_PERMITTED),
    (dict(allow_market_fallback=True), M.CALIBRATION_NOT_PERMITTED)])
def test_nonsense_or_calibrating_terms_are_refused(pdb, override, code):  # noqa: F811
    t = terms(pdb, **override)
    with pytest.raises(M.MarketPolicyError) as err:
        M.MarketEntryPolicyLedger(pdb).record(OP, venue="V", symbol="S", environment="DEMO", terms=t,
                                              approval_ref=APPROVAL, reason="test")
    assert err.value.code == code
    assert M.MarketEntryPolicyLedger(pdb).events() == []


def test_missing_term_and_too_distant_expiry_are_refused(pdb):  # noqa: F811
    ledger = M.MarketEntryPolicyLedger(pdb)
    t = terms(pdb)
    del t["absolute_cap_notional"]
    for bad in (t, terms(pdb, expires_at_ms=db_now(pdb) + M.MAX_LIFETIME_MS + DAY)):
        with pytest.raises(M.MarketPolicyError) as err:
            ledger.record(OP, venue="V", symbol="S", environment="DEMO", terms=bad, approval_ref=APPROVAL, reason="test")
        assert err.value.code == M.TERMS_INVALID


def test_live_cannot_be_recorded_in_this_build_and_scope_is_required(pdb):  # noqa: F811
    from shared_lib.core.production import BUILD_PERMITS_LIVE
    assert BUILD_PERMITS_LIVE is False
    ledger = M.MarketEntryPolicyLedger(pdb)
    with pytest.raises(M.MarketPolicyError) as err:
        ledger.record(OP, venue="V", symbol="S", environment="LIVE", terms=terms(pdb), approval_ref=APPROVAL,
                      reason="go live")
    assert err.value.code == M.LIVE_NOT_PERMITTED
    for venue, symbol, env in (("", "S", "DEMO"), ("V", "", "DEMO"), ("V", "S", "PAPER")):
        with pytest.raises(M.MarketPolicyError) as err:
            ledger.record(OP, venue=venue, symbol=symbol, environment=env, terms=terms(pdb), approval_ref=APPROVAL,
                          reason="scope")
        assert err.value.code == M.SCOPE_INVALID
    assert ledger.events() == []


def test_one_active_record_per_scope_revocation_is_recorded_and_history_kept(pdb):  # noqa: F811
    ledger = M.MarketEntryPolicyLedger(pdb)
    scope = dict(venue="binance_usdm", symbol="btcusdt", environment="demo")
    first = ledger.record(OP, terms=terms(pdb), approval_ref=APPROVAL, reason="first", **scope)
    assert (first.venue, first.symbol, first.environment) == ("BINANCE_USDM", "BTCUSDT", "DEMO")
    with pytest.raises(M.MarketPolicyError) as err:
        ledger.record(OP, terms=terms(pdb), approval_ref=APPROVAL, reason="second", **scope)
    assert err.value.code == M.ALREADY_ACTIVE
    with pytest.raises(M.MarketPolicyError) as err:
        ledger.revoke(OP, first.policy_id, reason="")
    assert err.value.code == M.REASON_REQUIRED
    with pytest.raises(M.MarketPolicyError) as err:
        ledger.revoke(None, first.policy_id, reason="no operator")
    assert err.value.code == M.OPERATOR_REQUIRED
    revoked = ledger.revoke(OP, first.policy_id, reason="owner withdrew it")
    assert revoked.event_type == M.POLICY_REVOKED and revoked.approval_ref is None
    for pid, code in ((first.policy_id, M.ALREADY_REVOKED), ("nope", M.NOT_FOUND)):
        with pytest.raises(M.MarketPolicyError) as err:
            ledger.revoke(OP, pid, reason="again")
        assert err.value.code == code
    second = ledger.record(OP, terms=terms(pdb), approval_ref=APPROVAL, reason="re-approved", **scope)
    assert [e.event_type for e in ledger.events()] == [M.POLICY_RECORDED, M.POLICY_REVOKED, M.POLICY_RECORDED]
    states = {r.policy_id: r.revoked is not None for r in ledger.records()}
    assert states == {first.policy_id: True, second.policy_id: False}
    assert ledger.verify() == (True, "")


def test_the_ledger_is_append_only_and_the_schema_refuses_calibration_or_no_approval(pdb):  # noqa: F811
    ledger = M.MarketEntryPolicyLedger(pdb)
    rec = ledger.record(OP, venue="V", symbol="S", environment="DEMO", terms=terms(pdb), approval_ref=APPROVAL,
                        reason="test")
    for sql in (f"UPDATE {M.TABLE} SET reason='changed'", f"DELETE FROM {M.TABLE}"):
        with pytest.raises(Exception) as err:
            with pdb.transaction() as conn:
                conn.execute(sql)
        assert "APPEND_ONLY" in str(err.value).upper()
    insert = (f"INSERT INTO {M.TABLE} (event_id, event_type, policy_id, venue, symbol, environment, account_id, scope, "
              "calibrated, approval_ref, reason, actor_ref, terms, recorded_at, prev_hash, hash) "
              "VALUES (?,?,?,?,?,?,NULL,?,?,?,?,?,?,?,?,?)")
    for calibrated, ref, scope in ((1, APPROVAL, "SINGLE_ACCOUNT"), (0, None, "SINGLE_ACCOUNT"),
                                   (0, "", "SINGLE_ACCOUNT"), (0, APPROVAL, "MULTI_ACCOUNT")):
        with pytest.raises(Exception):
            with pdb.transaction() as conn:
                conn.execute(insert, ("e" + str(calibrated) + str(ref) + scope, "POLICY_RECORDED", "p2", "V", "S2",
                                      "DEMO", scope, calibrated, ref, "forged", "nobody", "{}", 1, "0" * 64,
                                      "h" + str(calibrated) + str(ref) + scope))
    assert [e.policy_id for e in ledger.events()] == [rec.policy_id]
    policy = rec.spread_policy()
    assert policy.calibrated is False and policy.allow_market_fallback is True
    assert policy.max_participation <= CONSERVATIVE_DEFAULT.max_participation
    assert policy.absolute_cap_notional == Decimal("5000")


def test_a_broken_chain_refuses_every_governed_entry(pdb):  # noqa: F811
    ledger = M.MarketEntryPolicyLedger(pdb)
    rec = ledger.record(OP, venue="V", symbol="S", environment="DEMO", terms=terms(pdb), approval_ref=APPROVAL,
                        reason="test")
    assert ledger.resolve(venue="V", symbol="S", environment="DEMO", account_id="a")[0].policy_id == rec.policy_id
    with pdb.transaction() as conn:   # someone with database access bypasses the triggers and loosens the cap
        if conn.dialect == "postgresql":
            conn.execute(f"ALTER TABLE {M.TABLE} DISABLE TRIGGER USER")
            conn.execute(f"UPDATE {M.TABLE} SET terms=REPLACE(terms, '5000', '90000')")
            conn.execute(f"ALTER TABLE {M.TABLE} ENABLE TRIGGER USER")
        else:
            conn.execute(f"DROP TRIGGER {M.TABLE}_no_update")
            conn.execute(f"UPDATE {M.TABLE} SET terms=REPLACE(terms, '5000', '90000')")
    assert ledger.verify()[0] is False
    with pytest.raises(M.MarketPolicyError) as err:
        ledger.resolve(venue="V", symbol="S", environment="DEMO", account_id="a")
    assert err.value.code == M.LEDGER_INTEGRITY_FAILED


def test_resolution_codes(pdb):  # noqa: F811
    ledger = M.MarketEntryPolicyLedger(pdb)

    def code(**kw):
        args = dict(venue="V", symbol="S", environment="DEMO", account_id="acct-1")
        args.update(kw)
        with pytest.raises(M.MarketPolicyError) as err:
            ledger.resolve(**args)
        return err.value.code

    assert code() == M.NO_POLICY                                    # nothing recorded: the default refusal
    rec = ledger.record(OP, venue="V", symbol="S", environment="DEMO", terms=terms(pdb), approval_ref=APPROVAL,
                        reason="test", account_id="acct-1")
    assert code(symbol="OTHER") == M.NO_POLICY and code(venue="W") == M.NO_POLICY
    assert code(environment="LIVE") == M.NO_POLICY
    assert code(account_id="acct-2") == M.ACCOUNT_SCOPE_MISMATCH
    assert ledger.resolve(venue="v", symbol="s", environment="demo", account_id="acct-1")[0].policy_id == rec.policy_id
    ledger.revoke(OP, rec.policy_id, reason="withdrawn")
    assert code() == M.REVOKED
    short = ledger.record(OP, venue="V", symbol="S", environment="DEMO", reason="short",
                          terms=terms(pdb, expires_at_ms=db_now(pdb) + 300), approval_ref=APPROVAL)
    assert ledger.resolve(venue="V", symbol="S", environment="DEMO", account_id="x")[0].policy_id == short.policy_id
    time.sleep(0.45)
    assert code() == M.EXPIRED
    # a LIVE record cannot be written through ``record``; written below the API (chain kept intact) it still
    # never resolves while BUILD_PERMITS_LIVE is False
    with pdb.transaction() as conn:
        ledger._append_on(conn, M.POLICY_RECORDED, "forged-live", "V", "S", "LIVE", None, APPROVAL, "below the api",
                          "operator:x@y", json.dumps(M.PolicyTerms.parse(terms(pdb)).to_plain(), sort_keys=True),
                          conn.now_ms())
    assert ledger.verify() == (True, "")
    assert code(environment="LIVE") == M.ENVIRONMENT_NOT_PERMITTED


def test_an_unmigrated_ledger_is_the_default_refusal(tmp_path):
    from shared_lib.persistence.sql import Database, migrate
    from shared_lib.persistence.sql.platform_schema import all_migrations
    db = Database(f"sqlite:///{(tmp_path / 'p60.db').as_posix()}", pool_size=2)
    migrate(db, [m for m in all_migrations() if m.version <= 60])
    try:
        with pytest.raises(M.MarketPolicyError) as err:
            M.MarketEntryPolicyLedger(db).resolve(venue="V", symbol="S", environment="DEMO", account_id="a")
        assert err.value.code == M.NO_POLICY
    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════ part 2: the gate and the boundary
def test_the_default_without_a_record_refuses_every_market_entry(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    b, res = run(h)
    refused_before_the_venue(h, res, "MARKET_FALLBACK_DISABLED_BY_POLICY", pdb)
    assert [(r[0], r[1]) for r in links(pdb)] == [("REFUSED", "MARKET_FALLBACK_DISABLED_BY_POLICY")]
    assert rows(pdb, "SELECT policy_version FROM platform_spread_decisions") == [(CONSERVATIVE_DEFAULT.policy_version,)]
    assert rows(pdb, "SELECT COUNT(*) FROM platform_symbol_budgets") == [(0,)]


def test_one_eligible_account_with_an_approved_record_is_permitted(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    rec = approve(pdb, h.plan)
    b, res = run(h)
    assert res.status == B.EXECUTED and len(h.seen["orders"]) == 1, res
    a = res.attempt
    [(outcome, code, attempt_id, cid, settled, settled_notional, _)] = links(pdb)
    assert (outcome, code, attempt_id, cid, settled) == ("RESERVED", None, a.execution_attempt_id, a.client_order_id,
                                                         "FILLED")
    assert rows(pdb, "SELECT policy_version, calibrated FROM platform_spread_decisions") == [(rec.policy_version, 0)]
    [(cap, calibrated, version, approval_ref)] = rows(
        pdb, "SELECT cap_notional, calibrated, policy_version, approval_ref FROM platform_symbol_budgets")
    assert (calibrated, version, approval_ref) == (0, rec.policy_version, None)   # approval_ref: calibrated only
    assert decimal_out(cap) <= Decimal("5000")                     # the record's absolute cap bounds the budget
    [(_, status, granted, requested, _)] = slots(pdb)
    assert status == "FILLED" and decimal_out(granted) == Decimal(str(a.filled_quantity)) * Decimal(str(a.filled_price))
    assert h.run(b).status == B.DUPLICATE_PLAN and len(h.seen["orders"]) == 1   # never twice


def test_a_record_for_another_symbol_or_environment_does_not_apply(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan, symbol="ETHUSDT" if h.plan.instrument_key.venue_symbol != "ETHUSDT" else "BTCUSDT")
    _, res = run(h)
    refused_before_the_venue(h, res, "MARKET_FALLBACK_DISABLED_BY_POLICY", pdb)


def test_a_record_scoped_to_another_account_does_not_apply(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan, account_id="someone-else")
    _, res = run(h)
    refused_before_the_venue(h, res, M.ACCOUNT_SCOPE_MISMATCH, pdb)


def test_a_revoked_policy_refuses_the_next_entry(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    rec = approve(pdb, h.plan)
    M.MarketEntryPolicyLedger(pdb).revoke(OP, rec.policy_id, reason="owner withdrew the approval")
    _, res = run(h)
    refused_before_the_venue(h, res, M.REVOKED, pdb)
    assert rows(pdb, "SELECT policy_version FROM platform_spread_decisions") == [(CONSERVATIVE_DEFAULT.policy_version,)]


def test_a_second_concurrent_account_is_refused_while_uncalibrated(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan)
    other = other_plan(h.plan)
    register(pdb, other)
    first = reserve(h, other)                         # the other customer's account holds the governed slot
    assert first.calibrated is False and first.policy_version.startswith(M.POLICY_VERSION_PREFIX)
    _, res = run(h)
    refused_before_the_venue(h, res, "ORDER_SPREADING_CALIBRATION_REQUIRED", pdb)
    assert [(s[0], s[1]) for s in slots(pdb)] == [("other-acct", "RESERVED")]


def test_two_accounts_racing_get_exactly_one_slot(tmp_path, pdb):  # noqa: F811
    from concurrent.futures import ThreadPoolExecutor
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan)
    plans = [other_plan(h.plan, account=f"acct-{i}", user=f"user-{i}") for i in range(4)]
    for p in plans:
        register(pdb, p)

    def attempt(p):
        try:
            return reserve(h, p).account_id
        except G.SpreadRefused as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=4) as pool:
        outcomes = list(pool.map(attempt, plans))
    winners = [o for o in outcomes if o.startswith("acct-")]
    assert len(winners) == 1, outcomes
    assert set(outcomes) - set(winners) <= {"ORDER_SPREADING_CALIBRATION_REQUIRED"}
    assert [s[1] for s in slots(pdb)] == ["RESERVED"]


def test_the_shared_symbol_cap_counts_the_accounts_other_signals(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan, absolute_cap_notional="1000")
    mine = other_plan(h.plan, account=h.plan.broker_account_id, user=h.plan.user_id, decision_time=1)
    earlier = reserve(h, mine, quantity=Decimal("6"), notional=Decimal("600"))   # an earlier signal, still active
    _, res = run(h)                                                          # 480 more would pass 1000
    assert (res.status, res.reason_codes) == (B.ORDER_SPREADING_REFUSED, ("PLATFORM_SYMBOL_BUDGET_PARTIAL_GRANT",))
    assert h.seen["orders"] == [] and h.reservation_status() == "RELEASED"
    assert [(s[1], decimal_out(s[2])) for s in slots(pdb)] == [("RESERVED", Decimal("600"))]   # only the earlier one
    assert rows(pdb, "SELECT COUNT(DISTINCT budget_id) FROM platform_symbol_budgets") == [(2,)] and earlier.slot_id
    [refusal] = rows(pdb, "SELECT requested_notional FROM platform_spread_decisions WHERE outcome='REFUSED'")
    assert decimal_out(refusal[0]) > Decimal("400")                         # never resized to what is left


def test_a_notional_above_the_records_cap_is_refused_never_resized(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan, absolute_cap_notional="50")
    _, res = run(h)
    refused_before_the_venue(h, res, "PLATFORM_SYMBOL_BUDGET_PARTIAL_GRANT", pdb)


@pytest.mark.parametrize("book,record,code", [
    # fresh enough for the default (5 s) but not for the record's own bound
    (lambda s, limit=50: order_book(age_ms=2_000), dict(max_depth_age_ms=1000), "DEPTH_STALE"),
    (lambda s, limit=50: order_book(age_ms=60_000), {}, "DEPTH_STALE"),
    # five levels pass the default; the record asks for eight
    (lambda s, limit=50: order_book(levels=6), dict(min_levels=8), "DEPTH_INSUFFICIENT_LEVELS"),
    # the visible book holds 4 of the 4.8 the risk-approved quantity needs
    (lambda s, limit=50: order_book(qty="0.4"), {}, "DEPTH_INSUFFICIENT_FOR_QUANTITY"),
    (lambda s, limit=50: (_ for _ in ()).throw(TimeoutError("venue")), {}, "DEPTH_UNAVAILABLE")])
def test_stale_or_insufficient_depth_is_refused(tmp_path, pdb, book, record, code):  # noqa: F811
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan, **record)
    h.client.get_orderbook.side_effect = book
    _, res = run(h)
    refused_before_the_venue(h, res, code, pdb)


def _book(bids, asks, age_ms=0):
    raw = order_book()
    return {"bids": [[p, q] for p, q in bids], "asks": [[p, q] for p, q in asks], "time": raw["time"] - age_ms}


def test_an_excessive_spread_is_refused(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan, max_spread_bps="10")
    bids = [(99.8 - 0.01 * i, 1000) for i in range(10)]                      # ~20 bps spread
    asks = [(100.0 + 0.01 * i, 1000) for i in range(10)]
    h.client.get_orderbook.side_effect = lambda s, limit=50: _book(bids, asks)
    _, res = run(h)
    refused_before_the_venue(h, res, G.MARKET_SPREAD_EXCESSIVE, pdb)


def test_a_walk_beyond_the_slippage_band_is_refused(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan, max_slippage_bps="10")
    bids = [(99.99 - 0.01 * i, 1000) for i in range(10)]
    asks = [(100.0, 1)] + [(100.2 + 0.01 * i, 1000) for i in range(9)]       # the 2nd level is 20 bps away
    h.client.get_orderbook.side_effect = lambda s, limit=50: _book(bids, asks)
    _, res = run(h)
    refused_before_the_venue(h, res, "MARKET_WALK_EXCEEDS_WORST_PRICE", pdb)


def test_predicted_slippage_beyond_the_plans_own_bound_is_refused(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan)
    register(pdb, h.plan)
    bids = [(99.99 - 0.01 * i, 1000) for i in range(10)]
    asks = [(100.0, 1)] + [(100.1 + 0.01 * i, 1000) for i in range(9)]       # VWAP of 6: ~8.3 bps
    h.client.get_orderbook.side_effect = lambda s, limit=50: _book(bids, asks)
    with pytest.raises(G.SpreadRefused) as err:
        reserve(h, h.plan, quantity=Decimal("6"), notional=Decimal("600"), max_slippage_bps=5)
    assert err.value.code == G.MARKET_PREDICTED_SLIPPAGE_EXCEEDS_BOUND
    grant = reserve(h, h.plan, quantity=Decimal("6"), notional=Decimal("600"), max_slippage_bps=10)
    assert grant.calibrated is False and grant.order_style == "MARKET"


def test_the_book_moving_between_decision_and_submit_is_refused_at_the_executable_price(tmp_path, pdb):  # noqa: F811
    """The plan and hard risk were decided at the reference; the book read just before the claim has moved away
    from the stop. At the WORST walked price the risk of the risk-approved quantity is over 0.40 % of equity:
    refused, and the quantity is never shrunk to fit."""
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan)
    h.book_mid = 106.0 if h.plan.side == "LONG" else 94.0                   # 4.8 x 11 = 52.8 > 40
    _, res = run(h)
    refused_before_the_venue(h, res, G.EXECUTABLE_PRICE_RISK_EXCEEDED, pdb)
    [refusal] = rows(pdb, "SELECT requested_notional FROM platform_spread_decisions WHERE outcome='REFUSED'")
    assert decimal_out(refusal[0]) == Decimal("480")                         # the risk-approved notional


def test_the_harness_plan_on_its_own_equity_risks_too_much_at_the_executable_price(tmp_path, pdb):  # noqa: F811
    from app.trading_intelligence.execution.boundary import AccountState
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan)
    _, res = run(h, account=AccountState(5000.0, 0.0, 5000.0, 0))           # 24 / 5 000 = 0.48 % > 0.40 %
    refused_before_the_venue(h, res, G.EXECUTABLE_PRICE_RISK_EXCEEDED, pdb)


@pytest.mark.parametrize("stop,equity,code", [
    (None, Decimal("5000"), G.EXECUTABLE_PRICE_RISK_UNVERIFIABLE),
    (Decimal("98"), None, G.EXECUTABLE_PRICE_RISK_UNVERIFIABLE),
    (Decimal("98"), Decimal("0"), G.EXECUTABLE_PRICE_RISK_UNVERIFIABLE),
    (Decimal("100.5"), Decimal("5000"), G.EXECUTABLE_STOP_GEOMETRY_INVALID),     # stop above the buy price
    (Decimal("80"), Decimal("1000000"), G.EXECUTABLE_STOP_DISTANCE_EXCEEDED),     # 20 % away
    (Decimal("90"), Decimal("5000"), G.EXECUTABLE_PRICE_RISK_EXCEEDED)])          # 6 x 10 = 60 > 20
def test_executable_price_risk_validation(tmp_path, pdb, stop, equity, code):  # noqa: F811
    h = harness(tmp_path, pdb)
    long_plan = other_plan(h.plan, account=h.plan.broker_account_id, user=h.plan.user_id, side="LONG")
    approve(pdb, h.plan)
    with pytest.raises(G.SpreadRefused) as err:
        reserve(h, long_plan, quantity=Decimal("6"), notional=Decimal("600"), stop_price=stop, equity=equity)
    assert err.value.code == code
    assert slots(pdb) == []
    ok = reserve(h, long_plan, quantity=Decimal("6"), notional=Decimal("600"), stop_price=Decimal("98"),
                 equity=Decimal("5000"))                                # 6 x ~2.01 = ~12 < 20: within 0.40 %
    assert ok.slot_id


def test_an_ineligible_account_is_refused(tmp_path, pdb):  # noqa: F811
    from app.platform_workers.assignments import WorkerAssignments
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan)
    stranger = other_plan(h.plan, account="never-registered", user="nobody")
    with pytest.raises(G.SpreadRefused) as err:
        reserve(h, stranger)
    assert err.value.code == "PLATFORM_ACCOUNT_NOT_REGISTERED"
    paused = other_plan(h.plan, account="paused-acct", user="paused-user")
    WorkerAssignments(pdb).upsert_accounts([("paused-acct", "paused-user", h.plan.venue, h.plan.environment)],
                                           state="PAUSED")
    with pytest.raises(G.SpreadRefused) as err:
        reserve(h, paused)
    assert err.value.code == G.MARKET_POLICY_ACCOUNT_NOT_ELIGIBLE
    impostor = other_plan(h.plan, account="paused-acct", user="another-tenant")
    with pytest.raises(G.SpreadRefused) as err:
        reserve(h, impostor)
    assert err.value.code == "PLATFORM_ACCOUNT_TENANT_MISMATCH"
    assert slots(pdb) == []


def test_partial_fill_shrinks_the_granted_notional(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb, entry=_entry(), orders=[_order("PARTIALLY_FILLED", "3.5", "100.0"),
                                                       _order("CANCELED", "3.5", "100.0")])
    approve(pdb, h.plan)
    _, res = run(h)
    assert res.status == B.EXECUTED and len(h.seen["orders"]) == 1
    [(_, status, granted, requested, _)] = slots(pdb)
    assert status == "FILLED" and decimal_out(granted) == Decimal("350.0") < decimal_out(requested)


@pytest.mark.parametrize("truth", ["position", "no_position"])
def test_an_unknown_submission_stays_submitted_until_broker_reconciliation(tmp_path, pdb, truth):  # noqa: F811
    h = harness(tmp_path, pdb, entry=_entry(), order_error=RuntimeError("timeout"))
    approve(pdb, h.plan)
    b, res = run(h)
    assert res.status == B.SUBMIT_UNKNOWN and len(h.seen["orders"]) == 1
    assert [s[1] for s in slots(pdb)] == ["SUBMITTED"] and links(pdb)[0][4] is None
    assert b.reconcile_submit_unknown(h.plan, now_ms=h.now + 1).status == B.STILL_UNKNOWN
    assert [s[1] for s in slots(pdb)] == ["SUBMITTED"]
    if truth == "position":
        h.client.get_order.side_effect = lambda s, o: _order("FILLED", "6.0", "100.0")
        h.client.get_position_info.return_value = {"positionAmt": "6.0", "entryPrice": "100.0"}
        assert b.reconcile_submit_unknown(h.plan, now_ms=h.now + 2).status == B.RECONCILED
        [(_, status, granted, _, _)] = slots(pdb)
        assert status == "FILLED" and decimal_out(granted) == Decimal("600.0")
    else:
        h.client.get_order.side_effect = lambda s, o: _order("CANCELED", "0")
        assert b.reconcile_submit_unknown(h.plan, now_ms=h.now + 2).status == B.RECONCILED
        assert [s[1] for s in slots(pdb)] == ["RELEASED"]
    assert len(h.seen["orders"]) == 1                                   # reconciliation never re-submits


def test_limit_to_market_fallback_follows_market_fallback_permitted(tmp_path, pdb):  # noqa: F811
    """``entry_order_style``: an adapter that declares LIMIT entries with a plan that allows a MARKET fallback is a
    MARKET entry (``market_fallback_permitted`` decides it) -- refused by default, permitted only under a record;
    a plan with no MARKET style is a LIMIT entry and never needs a record."""
    h = harness(tmp_path, pdb)
    limit_adapter = SimpleNamespace(entry_order_type="LIMIT", venue=h.plan.venue)
    fallback = SimpleNamespace(preferred_order_style="LIMIT", fallback_order_styles=("MARKET",),
                               maximum_slippage_bps=20)
    limit_only = SimpleNamespace(preferred_order_style="LIMIT", fallback_order_styles=(), maximum_slippage_bps=20)
    a = other_plan(h.plan, account=h.plan.broker_account_id, user=h.plan.user_id, execution_preferences=fallback)
    assert G.entry_order_style(limit_adapter, a) == "MARKET"
    with pytest.raises(G.SpreadRefused) as err:
        h.gate.reserve(a, client=h.client, adapter=limit_adapter, quantity=Decimal("1"), notional=Decimal("100"),
                       attempt_id="att-fallback-1", stop_price=Decimal("98"), equity=Decimal("5000"))
    assert err.value.code == "MARKET_FALLBACK_DISABLED_BY_POLICY"
    rec = approve(pdb, h.plan)
    grant = h.gate.reserve(a, client=h.client, adapter=limit_adapter, quantity=Decimal("1"), notional=Decimal("100"),
                           attempt_id="att-fallback-2", stop_price=Decimal("98"), equity=Decimal("5000"))
    assert (grant.order_style, grant.policy_version) == ("MARKET", rec.policy_version)
    b = other_plan(h.plan, account="limit-acct", user="limit-user", execution_preferences=limit_only,
                   decision_time=7)
    assert G.entry_order_style(limit_adapter, b) == "LIMIT"


def test_a_budget_opened_under_another_policy_is_never_joined(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    limit_adapter = SimpleNamespace(entry_order_type="LIMIT", venue=h.plan.venue)
    limit_only = SimpleNamespace(preferred_order_style="LIMIT", fallback_order_styles=(), maximum_slippage_bps=20)
    lim = other_plan(h.plan, account="limit-acct", user="limit-user", execution_preferences=limit_only)
    register(pdb, lim)
    g = h.gate.reserve(lim, client=h.client, adapter=limit_adapter, quantity=Decimal("1"), notional=Decimal("100"),
                       attempt_id="att-limit")
    assert (g.order_style, g.policy_version) == ("LIMIT", CONSERVATIVE_DEFAULT.policy_version)
    h.gate.release(g, "TEST_DONE")
    approve(pdb, h.plan)
    same_signal = other_plan(h.plan, account=h.plan.broker_account_id, user=h.plan.user_id)
    with pytest.raises(G.SpreadRefused) as err:
        reserve(h, same_signal)
    assert err.value.code == G.MARKET_POLICY_BUDGET_MISMATCH


def test_the_kill_switch_preempts_a_recorded_policy(tmp_path, pdb):  # noqa: F811
    from app.trading_intelligence.governance.promotion import StaticAuthority
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan)
    h.client.get_orderbook.side_effect = AssertionError("the order budget is never asked")
    _, res = run(h, authority=StaticAuthority(False, "CATI_NEW_ENTRY_KILL_SWITCH"))
    assert (res.status, res.reason_codes) == (B.GOVERNANCE_NOT_AUTHORIZED, ("CATI_NEW_ENTRY_KILL_SWITCH",))
    assert h.seen["orders"] == [] and slots(pdb) == [] and links(pdb) == []


def test_an_uncertified_family_never_reaches_a_recorded_policy(tmp_path, pdb):  # noqa: F811
    from app.trading_intelligence.pipeline.provenance import StaticFamilyGate
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan)
    h.client.get_orderbook.side_effect = AssertionError("the order budget is never asked")
    _, res = run(h, family_gate=StaticFamilyGate(False, "FAMILY_NOT_CERTIFIED"))
    assert (res.status, res.reason_codes) == (B.GOVERNANCE_NOT_AUTHORIZED, ("FAMILY_NOT_CERTIFIED",))
    assert h.seen["orders"] == [] and links(pdb) == []


def test_a_record_and_a_reserved_slot_are_not_a_submission_permit(tmp_path, pdb):  # noqa: F811
    """The final raw permit is the boundary's call-scoped permit over the durable PENDING_SUBMIT attempt: a record
    plus a RESERVED slot grant no transport authority of their own."""
    from app.trading_intelligence.execution.entry_permit import transport_entry_permitted
    h = harness(tmp_path, pdb)
    approve(pdb, h.plan)
    reserve(h, h.plan, quantity=Decimal("6"), notional=Decimal("600"))
    assert [s[1] for s in slots(pdb)] == ["RESERVED"]
    client = h.client
    client._production_db, client._production_account_id = h.db, h.plan.broker_account_id
    client._broker_account_id = h.plan.broker_account_id
    assert transport_entry_permitted(client, h.plan.instrument_key.venue_symbol,
                                     "BUY" if h.plan.side == "LONG" else "SELL",
                                     payload={"symbol": h.plan.instrument_key.venue_symbol}) is False


# ═══════════════════════════════════════════════════════════════════ part 3: production settings
def _production_gate(h):
    """Replace the harness's test policy with what production constructs (the default) on its own platform
    database, with the account registered."""
    boundary = h.boundary_for()
    pdb_ = h.platform_db()
    h.register_platform_account()
    boundary.spread_gate = default_gate(pdb_)
    return boundary, pdb_


def test_production_default_without_a_record_refuses(certified):  # noqa: F811
    _, pdb_ = _production_gate(certified)
    result = process(certified)["boundary"]
    assert (result["status"], result["reason_codes"][0]) == (B.ORDER_SPREADING_REFUSED,
                                                             "MARKET_FALLBACK_DISABLED_BY_POLICY")
    certified.client.place_order.assert_not_called()


def test_production_certified_family_with_a_record_executes_once(certified):  # noqa: F811
    _, pdb_ = _production_gate(certified)
    rec = approve(pdb_, certified.plan)
    result = process(certified)["boundary"]
    assert result["status"] == B.EXECUTED, result
    assert certified.client.place_order.call_count == 1
    assert rows(pdb_, "SELECT policy_version, calibrated, settled_status FROM platform_spread_decisions") == [
        (rec.policy_version, 0, "FILLED")]
    process(certified)
    assert certified.client.place_order.call_count == 1


def test_production_uncertified_family_is_refused_before_the_budget(fresh):  # noqa: F811
    _, pdb_ = _production_gate(fresh)
    approve(pdb_, fresh.plan)
    fresh.client.get_orderbook.side_effect = AssertionError("the order budget is never asked")
    result = process(fresh)["boundary"]
    assert result["status"] == B.GOVERNANCE_NOT_AUTHORIZED, result
    fresh.client.place_order.assert_not_called()
    assert links(pdb_) == []


def test_production_uncertified_venue_adapter_is_refused_before_the_budget(certified):  # noqa: F811
    from app.trading_intelligence.pipeline import provenance as P
    boundary, pdb_ = _production_gate(certified)
    approve(pdb_, certified.plan)
    boundary.family_gate = P.FamilyExecutionGate(certified.db, registry=certified.gate.registry,
                                                 venue_certified=lambda venue, environment, now_ms: (
                                                     False, "VENUE_ADAPTER_NOT_CERTIFIED"))
    certified.client.get_orderbook.side_effect = AssertionError("the order budget is never asked")
    result = process(certified)["boundary"]
    assert result["status"] == B.GOVERNANCE_NOT_AUTHORIZED, result
    certified.client.place_order.assert_not_called()
    assert links(pdb_) == []


def test_production_kill_switch_preempts_a_record(certified):  # noqa: F811
    boundary, pdb_ = _production_gate(certified)
    approve(pdb_, certified.plan)
    boundary.authority.gov = SimpleNamespace(kill_switch_on=lambda **kw: True)
    certified.client.get_orderbook.side_effect = AssertionError("the order budget is never asked")
    result = process(certified)
    assert (result["reason"], result.get("boundary")) == ("CATI_NEW_ENTRY_KILL_SWITCH", None), result
    certified.client.place_order.assert_not_called()
    assert links(pdb_) == []


def test_production_stale_worker_fence_is_refused_and_never_submits(certified):  # noqa: F811
    from app.platform_workers import account_fence
    from app.platform_workers.assignments import AssignmentLost, WorkerAssignments
    boundary, pdb_ = _production_gate(certified)
    approve(pdb_, certified.plan)
    account = certified.plan.broker_account_id
    assignments = WorkerAssignments(pdb_)
    assignments.register_worker("worker-A")
    [mine] = [a for a in assignments.claim("worker-A", take_all_free=True) if a.account_id == account]
    stale = account_fence.AccountFence(account, "worker-A", mine.fencing_token, assignments)
    taken = assignments.preempt(account, "worker-B", "TEST_TAKEOVER")
    assert taken.fencing_token == mine.fencing_token + 1
    certified.client.get_orderbook.side_effect = AssertionError("the order budget is never asked")
    with account_fence.active(stale):
        with pytest.raises(AssignmentLost):
            process(certified)
    certified.client.place_order.assert_not_called()
    assert slots(pdb_) == [] and links(pdb_) == []
    # the successor, holding the current fence, proceeds exactly once
    certified.client.get_orderbook.side_effect = lambda symbol, limit=50: order_book(certified.book_mid)
    current = account_fence.AccountFence(account, "worker-B", taken.fencing_token, assignments)
    with account_fence.active(current):
        result = process(certified)["boundary"]
    assert result["status"] == B.EXECUTED, result
    assert certified.client.place_order.call_count == 1


def test_production_missing_native_protective_stop_is_refused_before_the_budget(certified):  # noqa: F811
    _, pdb_ = _production_gate(certified)
    approve(pdb_, certified.plan)
    certified.client.exchange_info_cached.return_value["symbols"][0]["orderTypes"] = ["MARKET", "STOP_MARKET"]
    certified.client.get_orderbook.side_effect = AssertionError("the order budget is never asked")
    result = process(certified)
    assert result["reason"] == "DEMO_CAPABILITY_UNAVAILABLE", result
    assert (result["boundary"]["status"], result["boundary"]["reason_codes"]) == (
        B.PREFLIGHT_BLOCKED, ("DEMO_CAPABILITY_UNAVAILABLE",))
    certified.client.place_order.assert_not_called()
    assert links(pdb_) == []


def test_production_disabled_demo_submission_is_refused_before_the_budget(certified, monkeypatch):  # noqa: F811
    from app.core import config
    from app.trading_intelligence.integration import production_execution as production
    from test_production_demo_execution import demo_profile
    _, pdb_ = _production_gate(certified)
    approve(pdb_, certified.plan)
    state = demo_profile(demo=False)
    monkeypatch.setattr(config, "settings", state)
    monkeypatch.setattr(production, "settings", state)
    certified.client.get_orderbook.side_effect = AssertionError("the order budget is never asked")
    result = process(certified)
    assert result["boundary"]["status"] == "DEMO_ORDER_SUBMISSION_DISABLED", result
    assert result["boundary"]["risk_decision"]["status"] == "APPROVED"     # risk ran; the switch stopped it
    certified.client.place_order.assert_not_called()
    assert links(pdb_) == []


def test_production_live_is_refused_by_the_go_live_gate_whatever_the_ledger_says(live, monkeypatch, tmp_path):  # noqa: F811
    """LIVE cannot be recorded; a LIVE row written below the interface does not resolve; and the LIVE entry stops
    at the real go-live gate before hard risk and before the order budget."""
    from app.core import config
    from app.golive import gate as GL
    from test_production_execution import profile
    assert GL.BUILD_PERMITS_LIVE is False
    monkeypatch.setattr(config, "settings", profile(True))
    pdb_ = live.platform_db()
    live.register_platform_account()
    with pytest.raises(M.MarketPolicyError) as err:
        approve(pdb_, live.plan)
    assert err.value.code == M.LIVE_NOT_PERMITTED
    monkeypatch.setenv("PLATFORM_DATABASE_URL", f"sqlite:///{(tmp_path / 'golive_platform.db').as_posix()}")
    from shared_lib.persistence.sql import Database, migrate
    from shared_lib.persistence.sql.platform_schema import all_migrations
    gl = Database(f"sqlite:///{(tmp_path / 'golive_platform.db').as_posix()}", pool_size=2)
    migrate(gl, all_migrations())
    gl.close()
    monkeypatch.setenv("GOLIVE_REVOCATION_ANCHOR", str(tmp_path / "anchor" / "revocations.jsonl"))
    live.client.get_orderbook.side_effect = AssertionError("the order budget is never asked")
    live.register_platform_account()
    out = live.run(live.live_boundary(spread_gate=default_gate(pdb_)), atr=1.)
    # refused by governance before hard risk and the order budget (here the family gate already; the go-live gate
    # after it -- asked below directly -- refuses every LIVE entry of this build)
    assert out.status == B.GOVERNANCE_NOT_AUTHORIZED, out
    assert out.risk_decision is None and out.attempt is None
    live.client.place_order.assert_not_called()
    assert links(pdb_) == []
    from app.trading_intelligence.pipeline import contracts as PC
    p = PC.provenance_of(live.plan)
    decision = GL.production_gate(live.db).evaluate(GL.Request(
        account_id=live.plan.broker_account_id, user_id=live.plan.user_id, family_id=str(p.get("family_id")),
        family_version=str(p.get("family_version") or ""), venue=str(p.get("venue") or ""), environment="LIVE"),
        now_ms=live.now)
    assert decision.authorized is False and "BUILD_PERMITS_LIVE" in [c.id for c in decision.blocking]
    # and a LIVE record written below the interface (chain intact) never resolves in this build
    ledger = M.MarketEntryPolicyLedger(pdb_)
    with pdb_.transaction() as conn:
        ledger._append_on(conn, M.POLICY_RECORDED, "forged-live", live.plan.venue.upper(),
                          live.plan.instrument_key.venue_symbol.upper(), "LIVE", None, APPROVAL, "below the api",
                          "operator:x@y", json.dumps(M.PolicyTerms.parse(terms(pdb_)).to_plain(), sort_keys=True),
                          conn.now_ms())
    live.client.get_orderbook.side_effect = lambda symbol, limit=50: order_book()
    with pytest.raises(G.SpreadRefused) as err:
        default_gate(pdb_).reserve(live.plan, client=live.client, adapter=live.adapter, quantity=Decimal("1"),
                                   notional=Decimal("100"), attempt_id="att-live", stop_price=Decimal("98"),
                                   equity=Decimal("5000"))
    assert err.value.code == M.ENVIRONMENT_NOT_PERMITTED


# ═══════════════════════════════════════════════════════════════════ part 4: the administrative interface
BASE = "/api/v1/admin/order-spreading/market-entry-policies"


@pytest.fixture
def api_module():
    from app.api import market_entry_policy
    return market_entry_policy


def _client(api_module, pdb, admin=None):  # noqa: F811
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.core.auth import require_admin_emergency
    app = FastAPI()
    app.include_router(api_module.router)
    app.dependency_overrides[api_module.get_platform_db] = lambda: pdb
    if admin is not None:
        app.dependency_overrides[require_admin_emergency] = lambda: admin
    return TestClient(app)


def test_every_route_requires_the_operator_token(api_module, pdb):  # noqa: F811
    from app.core.auth import require_admin_emergency
    client = _client(api_module, pdb)
    assert len(api_module.router.routes) == 4
    for route in api_module.router.routes:
        method = sorted(route.methods - {"HEAD"})[0]
        path = route.path.replace("{policy_id}", "p")
        assert client.request(method, path, json={}).status_code == 401, route.path
        assert require_admin_emergency in {d.call for d in route.dependant.dependencies}, route.path
    assert M.MarketEntryPolicyLedger(pdb).events() == []


def _token(*, role="admin", act="admin-emergency", lifetime=60, sub="admin-1"):
    """Signed like the user-backend's tokens (as ``tests/test_emergency_controls``); built at run time."""
    import jwt
    from app.core import security
    from app.core.security import AUDIENCE, ISSUER
    now = int(time.time())
    claims = {"sub": sub, "type": "access", "role": role, "iss": ISSUER, "aud": AUDIENCE, "iat": now,
              "exp": now + lifetime}
    if act is not None:
        claims["act"] = act
    return jwt.encode(claims, security.settings.SECRET_KEY, algorithm=security.settings.ALGORITHM)


def test_any_token_but_the_operator_service_token_is_forbidden(api_module, pdb):  # noqa: F811
    client = _client(api_module, pdb)
    body = dict(venue="V", symbol="S", environment="DEMO", terms=terms(pdb), approval_ref=APPROVAL, reason="try")
    for t in (_token(act=None), _token(role="user"), _token(lifetime=86_400)):   # plain admin, customer, long-lived
        r = client.post(BASE, json=body, headers={"Authorization": f"Bearer {t}"})
        assert r.status_code == 403, r.text
    assert client.post(BASE, json=body, headers={"Authorization": "Bearer not-a-token"}).status_code == 401
    assert M.MarketEntryPolicyLedger(pdb).events() == []
    ok = client.post(BASE, json=body, headers={"Authorization": f"Bearer {_token()}"})
    assert ok.status_code == 200, ok.text
    assert ok.json()["policy"]["recorded_by"].startswith("operator:admin-1@")


def test_record_list_and_revoke_through_the_interface(api_module, pdb):  # noqa: F811
    client = _client(api_module, pdb, admin="admin-42")
    body = dict(venue="BINANCE_USDM", symbol="BTCUSDT", environment="DEMO", terms=terms(pdb))
    no_ref = client.post(BASE, json={**body, "reason": "owner approved"})
    assert (no_ref.status_code, no_ref.json()["reason"]) == (409, M.APPROVAL_REF_REQUIRED)
    no_reason = client.post(BASE, json={**body, "approval_ref": APPROVAL})
    assert (no_reason.status_code, no_reason.json()["reason"]) == (409, M.REASON_REQUIRED)
    calibrated = client.post(BASE, json={**body, "approval_ref": APPROVAL, "reason": "x" * 5, "calibrated": True})
    assert calibrated.status_code == 422
    smuggled = client.post(BASE, json={**body, "approval_ref": APPROVAL, "reason": "smuggle",
                                       "terms": {**body["terms"], "calibrated": True}})
    assert (smuggled.status_code, smuggled.json()["reason"]) == (409, M.CALIBRATION_NOT_PERMITTED)
    live_ = client.post(BASE, json={**body, "environment": "LIVE", "approval_ref": APPROVAL, "reason": "live"})
    assert (live_.status_code, live_.json()["reason"]) == (409, M.LIVE_NOT_PERMITTED)
    assert M.MarketEntryPolicyLedger(pdb).events() == []
    ok = client.post(BASE, json={**body, "approval_ref": APPROVAL, "reason": "owner approved, test only"})
    assert ok.status_code == 200, ok.text
    policy = ok.json()["policy"]
    assert policy["recorded_by"] == "operator:admin-42@" + api_module.CHANNEL and policy["calibrated"] is False
    assert policy["active"] is True and policy["scope"] == "SINGLE_ACCOUNT" and policy["approval_ref"] == APPROVAL
    listed = client.get(BASE).json()
    assert listed["integrity"] == {"ok": True, "reason": ""} and [p["policy_id"] for p in listed["policies"]] == [
        policy["policy_id"]]
    assert client.post(f"{BASE}/{policy['policy_id']}/revoke", json={}).json()["reason"] == M.REASON_REQUIRED
    assert client.post(f"{BASE}/nope/revoke", json={"reason": "gone"}).status_code == 404
    revoked = client.post(f"{BASE}/{policy['policy_id']}/revoke", json={"reason": "owner withdrew"})
    assert revoked.status_code == 200 and revoked.json()["event"]["event_type"] == M.POLICY_REVOKED
    assert client.get(BASE).json()["policies"][0]["active"] is False
    history = client.get(f"{BASE}/history").json()
    assert [e["event_type"] for e in history["events"]] == [M.POLICY_RECORDED, M.POLICY_REVOKED]
    assert all(e["actor_ref"].startswith("operator:admin-42@") for e in history["events"])


def test_the_interface_without_a_platform_database_is_unavailable(api_module, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.core.auth import require_admin_emergency
    monkeypatch.delenv("PLATFORM_DATABASE_URL", raising=False)
    app = FastAPI()
    app.include_router(api_module.router)
    app.dependency_overrides[require_admin_emergency] = lambda: "admin-1"
    assert TestClient(app).get(BASE).status_code == 503


def test_the_service_registers_the_interface_and_no_order_route():
    from app.main import app
    paths = {getattr(r, "path", "") for r in app.routes}
    assert {BASE, f"{BASE}/history", f"{BASE}/{{policy_id}}/revoke"} <= paths
    assert not [p for p in paths if p.startswith("/api/v1/admin/order-spreading") and "order" in p.split("/")[-1]]
