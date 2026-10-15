"""Step 3.3 / 3.11: the production demo bypass for uncertified strategies is closed.

Step 1 proved the execution tail by letting the residual family place demo orders at governance phase M0
(``test_production_final_audit.test_m0_demo_uses_actual_account_authority``). Those tests still run, with the
family-authorization half of the gate replaced inside the test process (see ``conftest``). This module is not
on that list: it drives the same production harnesses with the real gate and shows that the same plans now
stop before the exchange, whatever the account authority, the phase or the submission flags say.
"""
from __future__ import annotations

from shared_lib.persistence.cati_platform_schema import ensure_cati_platform_schema
from test_production_demo_execution import demo  # noqa: F401  (fixture)
from test_production_execution import live  # noqa: F401  (fixture)
from test_production_final_audit import process
from test_production_research_separation import fresh  # noqa: F401  (fixture)

from app.trading_intelligence.execution.boundary import BoundaryStatus
from app.trading_intelligence.governance.account_authority import AccountExecutionAuthority
from app.trading_intelligence.pipeline import provenance as P
from app.trading_intelligence.pipeline.family_registry import FAILED, FamilyRegistry


def register(db):
    ensure_cati_platform_schema(db)
    registry = FamilyRegistry(db)
    registry.register_all(actor_ref="system:test")
    return registry


def test_the_failed_residual_family_cannot_place_a_demo_order(demo):  # noqa: F811
    registry = register(demo.db)
    entry = registry.entry(demo.plan.setup_family)
    assert entry.certification_status == FAILED and entry.governance_stage == "M0"
    boundary = demo.demo_boundary()
    assert type(boundary.family_gate) is P.FamilyExecutionGate          # production settings: the real gate
    out = demo.run(boundary, atr=1.)
    assert out.status == BoundaryStatus.GOVERNANCE_NOT_AUTHORIZED and out.reason_codes == (P.FAMILY_FAILED_RESEARCH,)
    demo.client.place_order.assert_not_called()
    assert out.risk_decision is None                                    # stopped before risk, sizing and the adapter


def test_the_m0_demo_account_authority_no_longer_reaches_the_exchange(fresh):  # noqa: F811
    """The exact scenario Step 1 pinned as executing: connected demo account, consent on, phase M0."""
    register(fresh.db)
    boundary = fresh.boundary_for()
    boundary.authority = AccountExecutionAuthority(fresh.db, (fresh.account["user_id"], fresh.account["id"]))
    assert boundary.authority.gov.current_phase() == "M0"
    assert boundary.authority.authorize_entry(fresh.plan) == (True, "ACCOUNT_AUTHORIZED_DEMO_EXECUTION")
    result = process(fresh)
    assert result.get("boundary", {}).get("status") != "EXECUTED", result
    assert fresh.client.place_order.call_count == 0


def test_an_unregistered_family_and_an_unreadable_governance_log_both_fail_closed(demo):  # noqa: F811
    ensure_cati_platform_schema(demo.db)
    out = demo.run(demo.demo_boundary(), atr=1.)                        # defined in code, never registered
    assert out.status == BoundaryStatus.GOVERNANCE_NOT_AUTHORIZED and out.reason_codes == (P.FAMILY_NOT_REGISTERED,)
    with demo.db.connect() as conn:
        conn.execute("DROP TABLE cati_family_events")                   # a database that lost the governance log
    out = demo.run(demo.demo_boundary(), atr=1.)
    assert out.status == BoundaryStatus.GOVERNANCE_NOT_AUTHORIZED
    assert out.reason_codes[0].startswith(P.FAMILY_GATE_UNAVAILABLE)
    demo.client.place_order.assert_not_called()


def test_a_test_double_is_refused_in_production(demo):  # noqa: F811
    out = demo.run(demo.demo_boundary(family_gate=P.StaticFamilyGate(True)), atr=1.)
    assert out.reason_codes == ("FAMILY_GATE_NOT_AUTHORITATIVE",)
    demo.client.place_order.assert_not_called()
