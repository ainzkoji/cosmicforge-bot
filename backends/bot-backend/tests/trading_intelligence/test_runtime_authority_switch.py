"""Section 25 runtime authority switch: one owner of NEW entries per account scope, decided only by the durable
governance record; the V2 entry path and the CATI TradePlan dispatch both consult it; no V2 fallback."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from test_sections23_26 import EVIDENCE, _advance

from app.runner.runner import PaperRunner
from app.trading_intelligence.governance.promotion import GovernanceAuthority, PromotionGovernance
from app.trading_intelligence.governance.runtime_authority import CATI, NONE, V2, resolve_order_authority
from app.trading_intelligence.integration import cati_dispatch as D
from app.trading_intelligence.research.certification.registry import SqliteResearchStore


@pytest.fixture
def db(tmp_path):
    return SqliteResearchStore(str(tmp_path / "gov.db"))


def _owner(db, env="DEMO", acct="acctA", **kw):
    return resolve_order_authority(db, broker_account_id=acct, venue="BINANCE_USDM", environment=env, **kw)


def _runner(db, env="demo", acct="acctA"):
    return SimpleNamespace(db=db, context=SimpleNamespace(broker_account_id=acct, broker_type="binance",
                                                          broker_environment=env, user_id="alice"))


def _plan(env="DEMO", acct="acctA", pid="tp1"):
    return SimpleNamespace(plan=SimpleNamespace(trade_plan_id=pid, broker_account_id=acct, environment=env,
                                                cycle_id="c1", bot_instance_id="bot1", source_candidate_id="sc1",
                                                venue="BINANCE_USDM", ranked_opportunity_id="r1"))


class _Boundary:
    def __init__(self):
        self.calls = []

    def process_trade_plan(self, plan, **kw):
        self.calls.append(plan.trade_plan_id)
        return SimpleNamespace(status="SUBMITTED", reason_codes=())


def _dispatch(db, monkeypatch, *, env="demo", acct="acctA", plan_env="DEMO", factory=None):
    boundary = _Boundary()
    monkeypatch.setattr(D, "_inputs", lambda *a, **k: {})
    out = D.dispatch_trade_plans(_runner(db, env, acct), [_plan(plan_env, acct)], evaluated={},
                                 boundary_factory=factory or (lambda runner, venue: boundary))
    return out[0], boundary


def _v2_block(db, env="demo", acct="acctA"):
    return PaperRunner._v2_order_authority_block(_runner(db, env, acct), "BTCUSDT", "BUY")


# -- phases ------------------------------------------------------------------------------------------------
def test_m0_observes_cati_and_blocks_every_entry_authority(db, monkeypatch):
    assert _owner(db).owner == NONE and _owner(db, "LIVE").owner == NONE
    decision, boundary = _dispatch(db, monkeypatch)
    assert decision["status"] == D.NOT_DISPATCHED and boundary.calls == []
    assert decision["reason"] == "GOVERNANCE_PHASE_M0_NO_ORDER_AUTHORITY" and decision["authority"]["owner"] == NONE
    assert _v2_block(db).status == "BLOCKED"


def test_m5_certification_eligibility_still_grants_no_cati_submission(db, monkeypatch):
    _advance(PromotionGovernance(db), "M5")
    decision, boundary = _dispatch(db, monkeypatch)
    assert decision["status"] == D.NOT_DISPATCHED and boundary.calls == [] and _owner(db).owner == NONE


def test_demo_authority_is_demo_only_and_v2_cannot_also_submit_there(db, monkeypatch):
    _advance(PromotionGovernance(db), "M6")
    decision, boundary = _dispatch(db, monkeypatch)
    assert decision["status"] == D.DISPATCHED and boundary.calls == ["tp1"]
    blocked = _v2_block(db)  # the same demo account: V2 is refused BEFORE the executor
    assert blocked.status == "BLOCKED" and "ORDER_AUTHORITY_NOT_V2" in blocked.error
    live, live_boundary = _dispatch(db, monkeypatch, env="live", acct="acctLive", plan_env="LIVE")
    assert live["status"] == D.NOT_DISPATCHED and live_boundary.calls == []  # live account blocked during M6
    assert _owner(db, "LIVE", "acctLive").owner == NONE  # unpromoted live scope has no entry owner


def test_production_authority_allows_only_eligible_live_scopes(db, monkeypatch):
    gov = PromotionGovernance(db)
    _advance(gov, "M7")
    assert _owner(db, "LIVE").owner == NONE  # unpromoted live scope is blocked
    gov.grant_scope(broker_account_id="acctA", venue="BINANCE_USDM", environment="REAL", reason="limited")
    assert _owner(db, "LIVE").owner == CATI and _owner(db, "LIVE", "acctB").owner == NONE
    decision, boundary = _dispatch(db, monkeypatch, env="live", plan_env="LIVE")
    assert decision["status"] == D.DISPATCHED and _v2_block(db, "live") is not None
    _advance_to_m8(gov)
    assert _owner(db, "LIVE", "acctB").owner == CATI and _v2_block(db, "live", "acctB") is not None


def _advance_to_m8(gov):
    gov.transition("M8", reason="full promotion", actor_ref="operator:test", source_commit="abc", **EVIDENCE["M8"])


# -- user control, kill switch, health, no fallback --------------------------------------------------------
def test_user_auto_trading_off_blocks_both_engines(db, monkeypatch):
    assert _owner(db, auto_trading_enabled=False).owner == NONE
    _advance(PromotionGovernance(db), "M6")
    a = _owner(db, auto_trading_enabled=False)
    assert a.owner == NONE and a.reason == "USER_AUTO_TRADING_OFF"


def test_kill_switch_halts_cati_entries_and_never_hands_them_to_v2(db, monkeypatch):
    gov = PromotionGovernance(db)
    _advance(gov, "M6")
    gov.set_kill_switch(True, reason="incident", actor_ref="operator:test")
    a = _owner(db)
    assert a.owner == NONE and "NO_V2_FALLBACK" in a.reason
    decision, boundary = _dispatch(db, monkeypatch)
    assert decision["status"] == D.NOT_DISPATCHED and boundary.calls == []
    assert _v2_block(db) is not None  # V2 does not regain the demo scope


def test_cati_failure_does_not_invoke_v2(db, monkeypatch):
    _advance(PromotionGovernance(db), "M6")
    assert _owner(db, cati_healthy=False).owner == NONE

    def broken(runner, venue):
        raise RuntimeError("adapter unavailable")

    decision, _ = _dispatch(db, monkeypatch, factory=broken)
    assert decision["status"] == D.NOT_DISPATCHED and decision["reason"] == D.DISPATCH_INPUT_UNAVAILABLE
    assert _v2_block(db) is not None


def test_governance_dual_key_still_refuses_inside_the_boundary(db):
    """Even a dispatched plan meets GovernanceAuthority again inside the boundary (M0 -> refused)."""
    ok, why = GovernanceAuthority(db).authorize_entry(SimpleNamespace(environment="DEMO", broker_account_id="acctA",
                                                                      venue="BINANCE_USDM"))
    assert not ok and why == "GOVERNANCE_PHASE_M0_NO_CATI_AUTHORITY"


# -- persistence / lineage / wiring ------------------------------------------------------------------------
def test_restart_preserves_the_selected_authority_and_its_audit_lineage(tmp_path):
    path = str(tmp_path / "gov.db")
    _advance(PromotionGovernance(SqliteResearchStore(path)), "M6")
    restarted = SqliteResearchStore(path)
    assert _owner(restarted).owner == CATI and _owner(restarted, "LIVE").owner == NONE
    hist = PromotionGovernance(restarted).history()
    assert [h["to_phase"] for h in hist] == ["M1", "M2", "M3", "M4", "M5", "M6"]
    assert all(h["payload"]["actor_ref"] and h["source_commit"] for h in hist)


def test_unreadable_governance_grants_nothing():
    broken = SimpleNamespace(connect=lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    assert resolve_order_authority(broken, broker_account_id="a", venue="v", environment="DEMO").owner == NONE


def test_no_environment_flag_can_grant_authority(db, monkeypatch):
    for flag in ("CATI_ACTIVE_EXECUTION_ENABLED", "CATI_EXIT_INTENT_ROUTING_ENABLED", "CATI_ML_ENABLED"):
        monkeypatch.setenv(flag, "1")
    decision, boundary = _dispatch(db, monkeypatch)
    assert decision["status"] == D.NOT_DISPATCHED and boundary.calls == []


def test_the_runtime_switch_is_a_real_wiring_state_not_a_constant():
    from app.activation import cati as act

    assert not hasattr(act, "RUNTIME_AUTHORITY_SWITCH_IMPLEMENTED")
    state = act.runtime_authority_switch()
    assert state.satisfied and state.detail == "wired"
