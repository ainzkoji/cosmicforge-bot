"""Step 3.3: registry-backed decision-source verification at the final execution boundary.

Every test runs the real ``CATIExecutionBoundary`` over the Section 20 harness (a real orchestrator and
executor over a mocked exchange client). A refusal is proved three ways: the status, the reason code, and the
fact that the exchange client was never asked to place an order.
"""
from __future__ import annotations

import inspect

import pytest

import _step3 as S
from _exec import Harness
from app.trading_intelligence.execution.boundary import BoundaryStatus as B
from app.trading_intelligence.execution import boundary as boundary_module
from app.trading_intelligence.pipeline import family_registry as FR
from app.trading_intelligence.pipeline import provenance as P


VENUE_OK = lambda venue, environment, now_ms: (True, "")                      # noqa: E731


@pytest.fixture
def h(tmp_path):
    return Harness(tmp_path)


def certified(h, tmp_path, *, stage="M6", status="RESEARCH_PASS", **definition_over):
    """A synthetic family standing on a passed hypothesis, promoted through the real governance operations."""
    d = S.definition(**definition_over)
    registry = S.registry(h.db, S.research_register(tmp_path, status=status), d)
    registry.register_all(actor_ref="system:test")
    if stage != "M0":
        S.promote(registry, d.family_id, stage, prereq=S.prerequisites(d, certification="CERTIFIED"))
    return d, registry, P.FamilyExecutionGate(h.db, registry=registry, venue_certified=VENUE_OK)


def refused(h, result, code):
    assert result.status == B.GOVERNANCE_NOT_AUTHORIZED, (result.status, result.reason_codes)
    assert result.reason_codes[0].split(":")[0] == code, result.reason_codes
    h.client.place_order.assert_not_called()
    assert h.seen["orders"] == [] and h.seen["protection"] == []
    assert h.reservation_status() in ("RESERVED", "RELEASED")


# ------------------------------------------------------------------------------------- the real families
def test_no_real_family_passes_the_real_gate(h):
    """The committed research register, the code-defined families, a fresh database: nothing is executable."""
    gate = P.FamilyExecutionGate(h.db)
    refused(h, h.run(h.boundary(family_gate=gate)), P.FAMILY_NOT_REGISTERED)          # defined, never registered
    gate.registry.register_all(actor_ref="system:test")
    refused(h, h.run(h.boundary(family_gate=gate)), P.FAMILY_NOT_CERTIFIED)           # a specialist: no verdict
    for family, code in (("RESIDUAL_MOMENTUM_PORTFOLIO_TOP1", P.FAMILY_FAILED_RESEARCH),
                         ("DAILY_TREND", P.FAMILY_NOT_CERTIFIED), ("SOMETHING_ELSE", P.FAMILY_UNKNOWN),
                         ("DEMO_CERTIFICATION", P.FAMILY_UNKNOWN)):
        plan = S.rebuilt(h.plan, setup_family=family, mode="DEMO")
        boundary = h.boundary(family_gate=gate)
        assert gate.authorize(plan, now_ms=h.now) == (False, code)
        assert boundary._plan_source_reason(plan, h.now) == code
        if not family.startswith("RESIDUAL"):       # the harness itself reads the residual tables for that family
            refused(h, h.run(boundary, plan=plan), code)


def test_the_boundary_uses_the_real_gate_when_none_is_given(h):
    boundary = boundary_module.CATIExecutionBoundary(orchestrator=h.orch, adapter=h.adapter, db=h.db)
    assert type(boundary.family_gate) is P.FamilyExecutionGate
    refused(h, h.run(boundary), P.FAMILY_NOT_REGISTERED)


def test_production_accepts_only_the_real_gate(h, monkeypatch):
    from app.core.config import settings

    boundary = h.boundary(family_gate=P.StaticFamilyGate(True))
    monkeypatch.setattr(type(settings), "production", property(lambda self: True))
    assert boundary._plan_source_reason(h.plan, h.now) == "FAMILY_GATE_NOT_AUTHORITATIVE"


def test_there_is_no_switch_that_skips_the_gate():
    for module in (P, FR):
        source = inspect.getsource(module)
        assert "os.environ" not in source and "getenv" not in source and "settings" not in source
    source = inspect.getsource(boundary_module.CATIExecutionBoundary._plan_source_reason)
    assert "getenv" not in source and "environ" not in source
    process = inspect.getsource(boundary_module.CATIExecutionBoundary.process_trade_plan) \
        if hasattr(boundary_module.CATIExecutionBoundary, "_process_trade_plan") is False else \
        inspect.getsource(boundary_module.CATIExecutionBoundary._process_trade_plan)
    assert process.count("self._plan_source_reason(") == 2      # on entry, and again immediately before the submit


# ------------------------------------------------------------------------------ a certified family at M6
def test_a_certified_promoted_family_with_an_intact_decision_is_executed(h, tmp_path):
    d, _, gate = certified(h, tmp_path)
    plan, recorded = S.decided_plan(h.db, h.plan, d, now=h.now)
    assert gate.authorize(plan, now_ms=h.now) == (True, "FAMILY_AUTHORIZED")
    result = h.run(h.boundary(family_gate=gate), plan=plan)
    assert result.status == B.EXECUTED, (result.status, result.reason_codes)
    assert len(h.seen["orders"]) == 1
    # replay: the gate is asked again and the stored attempt suppresses a second order
    again = h.run(h.boundary(family_gate=gate), plan=plan)
    assert again.status == B.DUPLICATE_PLAN and len(h.seen["orders"]) == 1
    assert P.DecisionStore(h.db).get(recorded["decision_id"])["decision_hash"] == recorded["decision_hash"]


@pytest.mark.parametrize("stage", ["M0", "M3", "M5"])
def test_certified_is_not_enough_below_the_demo_stage(h, tmp_path, stage):
    d, _, gate = certified(h, tmp_path, stage=stage)
    plan, _ = S.decided_plan(h.db, h.plan, d, now=h.now)
    result = h.run(h.boundary(family_gate=gate), plan=plan)
    refused(h, result, P.FAMILY_STAGE_NO_ORDER_AUTHORITY)
    assert result.reason_codes[0].endswith(stage)


@pytest.mark.parametrize("status,code", [("FAILED", P.FAMILY_FAILED_RESEARCH), (None, P.FAMILY_NOT_CERTIFIED),
                                         ("SOMETHING_NEW", P.FAMILY_EVIDENCE_MISSING)])
def test_a_family_whose_hypothesis_did_not_pass_is_refused_whatever_its_stage(h, tmp_path, status, code):
    """The governance log says M6 (as if promoted before the verdict changed); the register decides."""
    d = S.definition()
    passed = S.registry(h.db, S.research_register(tmp_path, status="RESEARCH_PASS", name="passed.jsonl"), d)
    passed.register_all(actor_ref="system:test")
    S.promote(passed, d.family_id)
    now = S.registry(h.db, S.research_register(tmp_path, status=status, name="now.jsonl"), d)
    plan, _ = S.decided_plan(h.db, h.plan, d, now=h.now)
    gate = P.FamilyExecutionGate(h.db, registry=now, venue_certified=VENUE_OK)
    refused(h, h.run(h.boundary(family_gate=gate), plan=plan), code)


def test_revocation_kill_switch_and_disable_are_refused(h, tmp_path):
    d, registry, gate = certified(h, tmp_path)
    plan, _ = S.decided_plan(h.db, h.plan, d, now=h.now)
    gov = registry.governance
    gov.set_kill_switch(d.family_id, True, operator=S.OPERATOR, reason="incident")
    refused(h, h.run(h.boundary(family_gate=gate), plan=plan), P.FAMILY_KILL_SWITCH)
    gov.set_kill_switch(d.family_id, False, operator=S.OPERATOR, reason="resolved")
    gov.set_disabled(d.family_id, True, operator=S.OPERATOR, reason="maintenance")
    refused(h, h.run(h.boundary(family_gate=gate), plan=plan), P.FAMILY_DISABLED)
    gov.set_disabled(d.family_id, False, operator=S.OPERATOR, reason="done")
    gov.revoke_certification(d.family_id, operator=S.OPERATOR, reason="defect")
    refused(h, h.run(h.boundary(family_gate=gate), plan=plan), P.FAMILY_CERTIFICATION_REVOKED)


def test_a_family_stopped_between_the_first_check_and_the_submit_places_no_order(h, tmp_path):
    d, registry, gate = certified(h, tmp_path)
    plan, _ = S.decided_plan(h.db, h.plan, d, now=h.now)
    original = h.orch.process_trade_plan

    def stop_the_family_during_risk(*a, **k):
        registry.governance.set_kill_switch(d.family_id, True, operator=S.OPERATOR, reason="during risk evaluation")
        return original(*a, **k)

    h.orch.process_trade_plan = stop_the_family_during_risk
    refused(h, h.run(h.boundary(family_gate=gate), plan=plan), P.FAMILY_KILL_SWITCH)


def test_rule_hash_mismatches(h, tmp_path):
    d, registry, gate = certified(h, tmp_path)
    plan, _ = S.decided_plan(h.db, h.plan, d, now=h.now)
    forged = S.rebuilt(plan, versions=tuple(sorted({**dict(plan.versions), "family_rule_hash": "0" * 64}.items())))
    refused(h, h.run(h.boundary(family_gate=gate), plan=forged), P.FAMILY_RULE_HASH_MISMATCH)
    # the code changed after registration: the registered family no longer is what runs
    drifted = S.registry(h.db, S.research_register(tmp_path, status="RESEARCH_PASS", name="r2.jsonl"),
                         S.definition(rule_hash="e" * 64))
    gate = P.FamilyExecutionGate(h.db, registry=drifted, venue_certified=VENUE_OK)
    refused(h, h.run(h.boundary(family_gate=gate), plan=plan), P.FAMILY_RULE_HASH_MISMATCH)


def test_decision_evidence_refusals(h, tmp_path):
    d, _, gate = certified(h, tmp_path)
    run = lambda plan, **kw: h.run(h.boundary(family_gate=gate), plan=plan, **kw)     # noqa: E731
    # no decision record at all
    orphan = S.rebuilt(S.decided_plan(h.db, h.plan, d, now=h.now)[0], source_candidate_id="fdec_does_not_exist")
    refused(h, run(orphan), P.DECISION_EVIDENCE_MISSING)
    # the plan carries a different hash than the record
    plan, _ = S.decided_plan(h.db, h.plan, d, now=h.now)
    forged = S.rebuilt(plan, versions=tuple(sorted({**dict(plan.versions), "decision_hash": "1" * 64}.items())))
    refused(h, run(forged), P.DECISION_TAMPERED)
    # the plan asks for something the decision did not say (the stop was moved)
    moved, _ = S.decided_plan(h.db, h.plan, d, now=h.now, intent_over={"stop_price": h.plan.structural_invalidation_price * 0.9})
    refused(h, run(moved), P.DECISION_PLAN_MISMATCH)
    # stale
    stale, _ = S.decided_plan(h.db, h.plan, d, now=h.now, ttl=500)
    refused(h, run(stale), P.DECISION_STALE)
    # dated in the future
    future, _ = S.decided_plan(h.db, h.plan, d, now=h.now, decision_time=h.now + 60_000, expires_at=h.now + 120_000)
    refused(h, run(future), P.DECISION_FUTURE_DATED)
    # made for another account, then for another user
    other, _ = S.decided_plan(h.db, h.plan, d, now=h.now, account_id="another-account")
    refused(h, run(other), P.DECISION_ACCOUNT_MISMATCH)
    other_user, _ = S.decided_plan(h.db, h.plan, d, now=h.now, user_id="another-user")
    refused(h, run(other_user), P.DECISION_ACCOUNT_MISMATCH)


def test_a_stored_decision_that_was_edited_is_detected(h, tmp_path):
    d, _, gate = certified(h, tmp_path)
    plan, recorded = S.decided_plan(h.db, h.plan, d, now=h.now)
    with h.db.connect() as conn:
        with pytest.raises(Exception, match="append-only"):
            conn.execute("UPDATE cati_family_decisions SET expires_at=expires_at+1")
        conn.execute("DROP TRIGGER trg_cati_fdec_no_update")
        conn.execute("UPDATE cati_family_decisions SET expires_at=expires_at+86400000 WHERE decision_id=?",
                     (recorded["decision_id"],))
    refused(h, h.run(h.boundary(family_gate=gate), plan=plan), P.DECISION_TAMPERED)


def test_mode_and_environment_refusals(h, tmp_path):
    d, _, gate = certified(h, tmp_path)
    run = lambda plan: h.run(h.boundary(family_gate=gate), plan=plan)                 # noqa: E731
    shadow, _ = S.decided_plan(h.db, h.plan, d, now=h.now, mode="SHADOW")
    refused(h, run(shadow), P.FAMILY_MODE_NOT_EXECUTABLE)             # a shadow decision is never an order
    # a decision recorded for shadow, presented as a demo order
    as_demo = S.rebuilt(shadow, mode="DEMO")
    refused(h, run(as_demo), P.FAMILY_MODE_NOT_EXECUTABLE)
    live, _ = S.decided_plan(h.db, h.plan, d, now=h.now, mode="LIVE", environment="LIVE")
    result = h.run(h.boundary(family_gate=gate), plan=live)
    assert result.status in (B.GOVERNANCE_NOT_AUTHORIZED, B.ENVIRONMENT_NOT_ALLOWED)   # M6 is demo only
    h.client.place_order.assert_not_called()
    assert gate.authorize(live, now_ms=h.now)[1].startswith(P.FAMILY_STAGE_NO_ORDER_AUTHORITY)
    mixed = S.rebuilt(S.decided_plan(h.db, h.plan, d, now=h.now)[0], mode="LIVE")
    assert gate.authorize(mixed, now_ms=h.now) == (False, P.FAMILY_ENVIRONMENT_NOT_AUTHORIZED)
    elsewhere, _, gate2 = certified(h, tmp_path / "x" if (tmp_path / "x").mkdir() is None else tmp_path,
                                    family_id="BYBIT_ONLY", supported_markets=(("bybit", "LINEAR"),))
    plan, _ = S.decided_plan(h.db, h.plan, elsewhere, now=h.now)
    refused(h, h.run(h.boundary(family_gate=gate2), plan=plan), P.FAMILY_MARKET_NOT_SUPPORTED)


def test_a_certified_family_still_needs_a_certified_venue_adapter(h, tmp_path):
    """Step 4.2: the certification register of the database is asked, and it is empty: no adapter is certified."""
    d, registry, _ = certified(h, tmp_path)
    plan, _ = S.decided_plan(h.db, h.plan, d, now=h.now)
    real_register = P.FamilyExecutionGate(h.db, registry=registry)
    result = h.run(h.boundary(family_gate=real_register), plan=plan)
    refused(h, result, P.VENUE_ADAPTER_NOT_CERTIFIED)
    assert "NOT_CERTIFIED" in result.reason_codes[0]
    seen = []

    def register(venue, environment, now_ms):
        seen.append((venue, environment))
        return False, "ADAPTER_EXPIRED:CERTIFICATION_OLDER_THAN_90_DAYS"

    expired = P.FamilyExecutionGate(h.db, registry=registry, venue_certified=register)
    result = h.run(h.boundary(family_gate=expired), plan=plan)
    refused(h, result, P.VENUE_ADAPTER_NOT_CERTIFIED)
    assert seen[0] == ("binance", "DEMO") and result.reason_codes[0].endswith("CERTIFICATION_OLDER_THAN_90_DAYS")
    assert [P.platform_venue(v) for v in ("BINANCE_USDM", "binance", "Bybit_Linear", "bingx", "oanda", "", None)] == [
        "binance", "binance", "bybit", "bingx", None, None, None]
    no_adapter = S.rebuilt(plan, venue="OANDA")
    assert expired.venue_certification(no_adapter, h.now) == P.VENUE_NOT_SUPPORTED_BY_PLATFORM


def test_an_undecidable_authorization_is_a_refusal(h):
    class Broken(P.FamilyExecutionGate):
        def family_authorization(self, plan):
            raise RuntimeError("registry unreadable")

    result = h.run(h.boundary(family_gate=Broken(h.db)))
    refused(h, result, P.FAMILY_GATE_UNAVAILABLE)


def test_recovery_reconciles_and_never_submits(h, tmp_path):
    """Recovery of a pending submission reads the broker; it has no path to a new order, so a family stopped
    after its order went out cannot be re-entered by a restart."""
    d, registry, gate = certified(h, tmp_path)
    plan, _ = S.decided_plan(h.db, h.plan, d, now=h.now)
    boundary = h.boundary(family_gate=gate)
    assert h.run(boundary, plan=plan).status == B.EXECUTED
    registry.governance.set_kill_switch(d.family_id, True, operator=S.OPERATOR, reason="after the order")
    orders_before = len(h.seen["orders"])
    boundary.recover_pending(now_ms=h.now + 1_000)
    assert len(h.seen["orders"]) == orders_before
    refused_again = h.run(boundary, plan=plan)
    assert refused_again.status == B.GOVERNANCE_NOT_AUTHORIZED and len(h.seen["orders"]) == orders_before


def test_decision_store_is_idempotent_and_refuses_a_second_different_decision(h, tmp_path):
    store = P.DecisionStore(h.db)
    fields = dict(cycle_id="cyc_x", family_id="F", family_version="1", rule_hash="r", mandate_id=None, account_id="a",
                  user_id="u", environment="DEMO", mode="SHADOW", decision_time=1, data_cutoff="d", expires_at=2)
    first = store.record(document={"intents": []}, now_ms=5, **fields)
    again = store.record(document={"intents": []}, now_ms=9, **fields)
    assert again == {**first, "replayed": True} and first["replayed"] is False
    with pytest.raises(P.DecisionConflict):
        store.record(document={"intents": [{"symbol": "X"}]}, now_ms=9, **fields)
    with h.db.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM cati_family_decisions").fetchone()[0] == 1
