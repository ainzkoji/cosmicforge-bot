"""L-08 for a strategy that picks its symbol each cycle: a DEMO-only, single-account all-symbols record, and the
account-eligibility venue match between the platform registry ("binance") and the plan ("BINANCE_USDM").

Everything else about a governed market entry is unchanged and covered by ``test_l08_market_entry_policy``.
"""
from __future__ import annotations

import pytest

from _exec import Harness  # noqa: F401
from app.platform_workers.assignments import WorkerAssignments
from app.trading_intelligence.execution.boundary import BoundaryStatus as B
from app.trading_intelligence.scale import market_entry_policy as M
from test_l08_market_entry_policy import (OP, APPROVAL, harness, other_plan, refused_before_the_venue,  # noqa: F401
                                          reserve, run, terms)
from test_step5_spread_boundary import pdb, platform_url, slots  # noqa: F401 (fixtures)


def record_all(db, plan, *, account_id="__plan__", environment="DEMO", **overrides):
    return M.MarketEntryPolicyLedger(db).record(
        OP, venue=plan.venue, symbol=M.ALL_SYMBOLS, environment=environment, terms=terms(db, **overrides),
        approval_ref=APPROVAL, reason="experimental DEMO, one account, all symbols",
        account_id=plan.broker_account_id if account_id == "__plan__" else account_id)


def test_an_all_symbols_record_is_demo_only_and_names_its_one_account(pdb):  # noqa: F811
    from types import SimpleNamespace
    plan = SimpleNamespace(venue="BINANCE_USDM", broker_account_id="brk_demo")
    with pytest.raises(M.MarketPolicyError) as e:
        record_all(pdb, plan, environment="LIVE")
    assert e.value.code == M.ALL_SYMBOLS_REQUIRE_DEMO_ACCOUNT
    with pytest.raises(M.MarketPolicyError) as e:
        record_all(pdb, plan, account_id=None)
    assert e.value.code == M.ALL_SYMBOLS_REQUIRE_DEMO_ACCOUNT
    rec = record_all(pdb, plan)
    assert (rec.symbol, rec.environment, rec.account_id) == ("*", "DEMO", "brk_demo")
    assert rec.spread_policy().calibrated is False


def test_the_all_symbols_record_permits_one_demo_account_on_any_symbol(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    assert h.plan.environment == "DEMO"
    record_all(pdb, h.plan)
    _, res = run(h)
    assert res.status == B.EXECUTED and len(h.seen["orders"]) == 1, res


def test_it_does_not_apply_to_another_account_and_a_second_account_is_still_refused(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    record_all(pdb, h.plan, account_id="someone-else")
    _, res = run(h)
    refused_before_the_venue(h, res, M.ACCOUNT_SCOPE_MISMATCH, pdb)


def test_a_symbol_specific_record_wins_over_the_all_symbols_record(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    record_all(pdb, h.plan)
    specific = M.MarketEntryPolicyLedger(pdb).record(
        OP, venue=h.plan.venue, symbol=h.plan.instrument_key.venue_symbol, environment="DEMO",
        terms=terms(pdb), approval_ref=APPROVAL, reason="specific record", account_id="someone-else")
    _, res = run(h)
    refused_before_the_venue(h, res, M.ACCOUNT_SCOPE_MISMATCH, pdb)
    del specific


def test_a_revoked_symbol_record_is_not_silently_replaced_by_the_all_symbols_record(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    record_all(pdb, h.plan)
    specific = M.MarketEntryPolicyLedger(pdb).record(
        OP, venue=h.plan.venue, symbol=h.plan.instrument_key.venue_symbol, environment="DEMO",
        terms=terms(pdb), approval_ref=APPROVAL, reason="specific record", account_id=h.plan.broker_account_id)
    M.MarketEntryPolicyLedger(pdb).revoke(OP, specific.policy_id, reason="revoke the specific record")
    _, res = run(h)
    refused_before_the_venue(h, res, M.REVOKED, pdb)   # the revoked specific record is still the scope's record


def test_a_revoked_all_symbols_record_refuses_the_next_entry(tmp_path, pdb):  # noqa: F811
    h = harness(tmp_path, pdb)
    rec = record_all(pdb, h.plan)
    M.MarketEntryPolicyLedger(pdb).revoke(OP, rec.policy_id, reason="stop the experiment")
    _, res = run(h)
    refused_before_the_venue(h, res, M.REVOKED, pdb)


def test_an_account_registered_under_its_broker_id_is_eligible(tmp_path, pdb):  # noqa: F811
    """Production registers ``broker_id.lower()`` ("binance"); plans carry the product venue ("BINANCE_USDM")."""
    h = harness(tmp_path, pdb)
    WorkerAssignments(pdb).upsert_accounts([(h.plan.broker_account_id, h.plan.user_id, "binance",
                                             h.plan.environment)])
    record_all(pdb, h.plan)
    _, res = run(h)
    assert res.status == B.EXECUTED, res
    # a different platform venue is still not eligible, nor a different environment
    from app.trading_intelligence.scale import spread_gate as G
    WorkerAssignments(pdb).upsert_accounts([("bybit-acct", "bybit-user", "bybit", "DEMO")])
    with pytest.raises(G.SpreadRefused) as err:
        G.PlatformSpreadGate._account_eligible(pdb, "bybit-acct", "bybit-user", h.plan.venue, "DEMO")
    assert err.value.code == G.MARKET_POLICY_ACCOUNT_NOT_ELIGIBLE
    with pytest.raises(G.SpreadRefused) as err:
        G.PlatformSpreadGate._account_eligible(pdb, h.plan.broker_account_id, h.plan.user_id, h.plan.venue, "LIVE")
    assert err.value.code == G.MARKET_POLICY_ACCOUNT_NOT_ELIGIBLE
