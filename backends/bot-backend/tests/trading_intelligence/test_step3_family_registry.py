"""Step 3.2 and 3.11: the family registry and the operable M0 to M6 governance log."""
from __future__ import annotations

import sqlite3

import pytest

import _step3 as S
from app.trading_intelligence.governance.phases import PHASE_IDS
from app.trading_intelligence.pipeline import contracts as C
from app.trading_intelligence.pipeline import family_registry as FR
from app.trading_intelligence.pipeline.family_governance import (
    FamilyGovernance,
    FamilyGovernanceError,
    OperatorAuthorization,
)

REQUIRED_FIELDS = ("family_id", "family_version", "mandate_id", "hypothesis_id", "rule_hash", "supported_markets",
                   "timeframe", "trading_direction", "position_model", "certification_status", "governance_stage",
                   "allowed_modes", "entry_contract", "exit_contract", "data_requirements", "activation_policy",
                   "kill_switch_state", "evidence_reference")


@pytest.fixture
def db(tmp_path):
    return S.fresh_db(tmp_path)


# ------------------------------------------------------------------------------------- the real registry
def test_real_registry_lists_the_real_families_and_none_can_send_an_order(db):
    reg = FR.FamilyRegistry(db)                      # the committed research register
    reg.register_all(actor_ref="system:test")
    entries = {e.definition.family_id: e for e in reg.entries()}
    assert {"DAILY_TREND", "RESIDUAL_MOMENTUM_PORTFOLIO_TOP1"} <= set(entries)
    for entry in entries.values():
        plain = entry.to_plain()
        assert all(k in plain for k in REQUIRED_FIELDS)
        assert entry.governance_stage == "M0" and not entry.executable
        assert not set(entry.allowed_modes) & set(C.EXECUTING_MODES)
    assert reg.executable() == []
    daily, residual = entries["DAILY_TREND"], entries["RESIDUAL_MOMENTUM_PORTFOLIO_TOP1"]
    assert daily.certification_status == FR.NOT_CERTIFIED and C.SHADOW in daily.allowed_modes
    assert daily.definition.mandate_id == "MANDATE_004" and daily.definition.hypothesis_id == "H-018"
    # the failed family stays visible, with its failure, and is not executable
    assert residual.certification_status == FR.FAILED and "FAMILY_FAILED_RESEARCH" in residual.blockers
    assert residual.certification_detail["failure_reasons"]


def test_daily_trend_rule_hash_is_the_registered_mandate_hash_and_m6_is_out_of_reach(db):
    reg = FR.FamilyRegistry(db)
    reg.register_all(actor_ref="system:test")
    pre = reg.promotion_prerequisites("DAILY_TREND")
    assert pre["mandate_verified"] is True and pre["valid_run"] is True
    assert pre["certification_status"] == FR.NOT_CERTIFIED and pre["admission"] == "NOT_ELIGIBLE"
    state = reg.governance.state("DAILY_TREND")
    missing = reg.governance.missing_prerequisites(state, "M6", prerequisites=pre)
    assert "RESEARCH_CERTIFICATION_NOT_CERTIFIED" in missing and "ADMISSION_NOT_ELIGIBLE" in missing


def test_the_registry_does_not_assume_a_number_of_families(db, tmp_path):
    research = S.research_register(tmp_path)
    many = [S.definition(f"FAMILY_{n}") for n in range(7)]
    reg = S.registry(db, research, *many)
    reg.register_all(actor_ref="system:test")
    assert len(reg.entries()) == 7
    # the separate four-specialist invariant is untouched
    from app.trading_intelligence.setups.registry import SPECIALIST_REGISTRY
    assert len(SPECIALIST_REGISTRY) == 4
    assert set(SPECIALIST_REGISTRY) <= set(FR.definitions())


def test_selection_is_by_registry_content_not_by_name(db, tmp_path):
    research = S.research_register(tmp_path)
    with_adapter = S.definition("WITH_ADAPTER", pipeline_adapter="app.trading_intelligence.pipeline.daily_trend")
    without = S.definition("WITHOUT_ADAPTER")
    reg = S.registry(db, research, with_adapter, without)
    assert reg.evaluable() == []                    # nothing is registered yet
    reg.register_all(actor_ref="system:test")
    assert [e.definition.family_id for e in reg.evaluable()] == ["WITH_ADAPTER"]
    assert "NO_PLATFORM_PIPELINE_ADAPTER" in reg.entry("WITHOUT_ADAPTER").blockers
    reg.governance.set_disabled("WITH_ADAPTER", True, operator=S.OPERATOR, reason="test")
    assert reg.evaluable() == []


@pytest.mark.parametrize("status,expected", [(None, FR.NOT_CERTIFIED), ("RESEARCH_PASS", FR.CERTIFIED),
                                             ("FAILED", FR.FAILED), ("SOMETHING_NEW", FR.UNKNOWN)])
def test_certification_is_read_from_the_research_register_every_time(db, tmp_path, status, expected):
    research = S.research_register(tmp_path, status=status)
    reg = S.registry(db, research, S.definition())
    assert reg.entry(S.TEST_FAMILY).certification_status == expected


def test_an_unreadable_or_edited_register_authorizes_nothing(db, tmp_path):
    research = S.research_register(tmp_path, status="RESEARCH_PASS")
    reg = S.registry(db, research, S.definition())
    reg.register_all(actor_ref="system:test")
    S.promote(reg, S.TEST_FAMILY)
    assert reg.entry(S.TEST_FAMILY).executable
    path = tmp_path / "register.jsonl"
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace('"REJECTED_PRE_HOLDOUT"', '"RESEARCH_PASS"', 1), encoding="utf-8", newline="\n")
    entry = reg.entry(S.TEST_FAMILY)
    assert entry.certification_status == FR.UNKNOWN and not entry.executable
    assert "FAMILY_EVIDENCE_MISSING" in entry.blockers


def test_a_hypothesis_that_is_not_in_the_register_is_unknown(db, tmp_path):
    reg = S.registry(db, S.research_register(tmp_path), S.definition(hypothesis_id="H-999"))
    assert reg.entry(S.TEST_FAMILY).certification_detail["reason"] == "HYPOTHESIS_NOT_IN_REGISTER"


# -------------------------------------------------------------------------------------------- governance
def test_a_certified_family_is_promoted_stage_by_stage_and_only_then_is_executable(db, tmp_path):
    research = S.research_register(tmp_path, status="RESEARCH_PASS")
    d = S.definition()
    reg = S.registry(db, research, d)
    reg.register_all(actor_ref="system:test")
    assert reg.entry(d.family_id).governance_stage == "M0" and not reg.entry(d.family_id).executable
    S.promote(reg, d.family_id, "M5")
    assert not reg.entry(d.family_id).executable        # certified and at M5 is still not order authority
    gov = reg.governance
    for key in ("certification_run_id", "policy_freeze_hash", "replay_gates_passed"):
        gov.record_evidence(d.family_id, key, reference="test://m6", actor_ref="operator:tester")
    request = gov.request_promotion(d.family_id, "M6", actor_ref="operator:requester", reason="demo trading")
    assert reg.entry(d.family_id).governance_stage == "M5"   # a request changes nothing
    approved = gov.approve_promotion(request["request_id"], operator=S.OPERATOR, reason="reviewed",
                                     prerequisites=S.prerequisites(d))
    entry = reg.entry(d.family_id)
    assert entry.governance_stage == "M6" and entry.allowed_modes[-1] == C.DEMO and C.LIVE not in entry.allowed_modes
    assert approved["payload"]["reviewer"] == "admin-7" and approved["from_stage"] == "M5"
    history = gov.history(d.family_id)
    assert [h["event_type"] for h in history if h["request_id"] == request["request_id"]] == [
        "PROMOTION_REQUESTED", "PROMOTION_APPROVED"]
    assert all(h["recorded_at"] > 0 and h["actor_ref"] for h in history)
    assert gov.verify_chain()["ok"]


def test_promotion_refusals(db, tmp_path):
    research = S.research_register(tmp_path)             # hypothesis still open: NOT certified
    d = S.definition()
    reg = S.registry(db, research, d)
    gov = reg.governance
    with pytest.raises(FamilyGovernanceError, match="FAMILY_NOT_REGISTERED"):
        gov.request_promotion(d.family_id, "M1", actor_ref="x", reason="r")
    reg.register_all(actor_ref="system:test")
    with pytest.raises(FamilyGovernanceError, match="STAGE_SKIPPED"):
        gov.request_promotion(d.family_id, "M2", actor_ref="x", reason="r")
    with pytest.raises(FamilyGovernanceError, match="STAGE_NOT_OPERABLE_HERE"):
        gov.request_promotion(d.family_id, "M7", actor_ref="x", reason="r")
    with pytest.raises(FamilyGovernanceError, match="REASON_REQUIRED"):
        gov.request_promotion(d.family_id, "M1", actor_ref="x", reason=" ")
    request = gov.request_promotion(d.family_id, "M1", actor_ref="operator:x", reason="r")
    with pytest.raises(FamilyGovernanceError, match="PROMOTION_ALREADY_PENDING"):
        gov.request_promotion(d.family_id, "M1", actor_ref="operator:x", reason="again")
    pre = S.prerequisites(d, certification="NOT_CERTIFIED")
    # no command-line flag, string or dictionary is an operator
    for impostor in (None, "admin", {"operator_id": "admin"}, True):
        with pytest.raises(FamilyGovernanceError, match="OPERATOR_AUTHORIZATION_REQUIRED"):
            gov.approve_promotion(request["request_id"], operator=impostor, reason="r", prerequisites=pre)
    with pytest.raises(FamilyGovernanceError, match="EVIDENCE_MISSING:section9_shadow_verified"):
        gov.approve_promotion(request["request_id"], operator=S.OPERATOR, reason="r", prerequisites=pre)
    with pytest.raises(FamilyGovernanceError, match="FAMILY_RULE_HASH_MISMATCH"):
        gov.approve_promotion(request["request_id"], operator=S.OPERATOR, reason="r",
                              prerequisites=dict(pre, rule_hash="other"))
    rejected = gov.reject_promotion(request["request_id"], operator=S.OPERATOR, reason="not yet")
    assert rejected["event_type"] == "PROMOTION_REJECTED" and gov.state(d.family_id).stage == "M0"
    with pytest.raises(FamilyGovernanceError, match="REQUEST_NOT_PENDING"):
        gov.approve_promotion(request["request_id"], operator=S.OPERATOR, reason="r", prerequisites=pre)
    with pytest.raises(FamilyGovernanceError, match="OPERATOR_AUTHORIZATION_REQUIRED"):
        OperatorAuthorization("", "admin_api", "p")


def test_an_uncertified_family_stops_before_m5_and_m6(db, tmp_path):
    research = S.research_register(tmp_path)
    d = S.definition()
    reg = S.registry(db, research, d)
    reg.register_all(actor_ref="system:test")
    S.promote(reg, d.family_id, "M4", prereq=S.prerequisites(d, certification="NOT_CERTIFIED"))   # shadow stages
    gov = reg.governance
    for key in ("certification_run_id", "policy_freeze_hash", "certification_policy_frozen", "replay_gates_passed"):
        gov.record_evidence(d.family_id, key, reference="test://claimed", actor_ref="operator:tester")
    request = gov.request_promotion(d.family_id, "M5", actor_ref="operator:x", reason="r")
    with pytest.raises(FamilyGovernanceError, match="NO_VALID_EVALUATION_RUN"):   # recorded evidence cannot replace it
        gov.approve_promotion(request["request_id"], operator=S.OPERATOR, reason="r",
                              prerequisites=S.prerequisites(d, certification="NOT_CERTIFIED", valid_run=False))
    gov.approve_promotion(request["request_id"], operator=S.OPERATOR, reason="r",
                          prerequisites=S.prerequisites(d, certification="NOT_CERTIFIED"))
    request = gov.request_promotion(d.family_id, "M6", actor_ref="operator:x", reason="r")
    for status in ("NOT_CERTIFIED", "FAILED", "UNKNOWN", "REVOKED"):
        with pytest.raises(FamilyGovernanceError, match=f"RESEARCH_CERTIFICATION_{status}"):
            gov.approve_promotion(request["request_id"], operator=S.OPERATOR, reason="r",
                                  prerequisites=S.prerequisites(d, certification=status))
    with pytest.raises(FamilyGovernanceError, match="ADMISSION_BLOCKED_PENDING_APPROVAL"):
        gov.approve_promotion(request["request_id"], operator=S.OPERATOR, reason="r",
                              prerequisites=S.prerequisites(d, admission="BLOCKED_PENDING_APPROVAL"))
    assert reg.entry(d.family_id).governance_stage == "M5" and not reg.entry(d.family_id).executable


def test_revocation_kill_switch_and_disable_remove_order_authority_at_once(db, tmp_path):
    research = S.research_register(tmp_path, status="RESEARCH_PASS")
    d = S.definition()
    reg = S.registry(db, research, d)
    reg.register_all(actor_ref="system:test")
    S.promote(reg, d.family_id)
    gov = reg.governance
    assert reg.entry(d.family_id).executable
    gov.set_kill_switch(d.family_id, True, operator=S.OPERATOR, reason="incident")
    entry = reg.entry(d.family_id)
    assert entry.kill_switch_state == "ON" and not entry.executable and entry.governance_stage == "M6"
    assert entry.definition.activation_policy["kill_switch_position_policy"] == FR.MANAGE_TO_EXIT
    gov.set_kill_switch(d.family_id, False, operator=S.OPERATOR, reason="resolved")
    assert reg.entry(d.family_id).executable
    gov.revoke_promotion(d.family_id, operator=S.OPERATOR, reason="back to shadow", to_stage="M4")
    assert reg.entry(d.family_id).governance_stage == "M4" and not reg.entry(d.family_id).executable
    with pytest.raises(FamilyGovernanceError, match="NOT_A_REVOCATION"):
        gov.revoke_promotion(d.family_id, operator=S.OPERATOR, reason="up", to_stage="M6")
    gov.revoke_certification(d.family_id, operator=S.OPERATOR, reason="defect found")
    entry = reg.entry(d.family_id)
    assert entry.certification_status == FR.REVOKED and entry.governance_stage == "M0"
    assert "FAMILY_CERTIFICATION_REVOKED" in entry.blockers
    # the prior status is preserved: every step is still in the history, in order
    types = [h["event_type"] for h in gov.history(d.family_id)]
    assert types.count("PROMOTION_APPROVED") == 6 and types[-1] == "CERTIFICATION_REVOKED"
    stages = [h["to_stage"] for h in gov.history(d.family_id) if h["event_type"] == "PROMOTION_APPROVED"]
    assert stages == list(PHASE_IDS[1:7])


def test_a_changed_rule_is_a_new_registration_at_m0(db, tmp_path):
    research = S.research_register(tmp_path, status="RESEARCH_PASS")
    d = S.definition()
    reg = S.registry(db, research, d)
    reg.register_all(actor_ref="system:test")
    S.promote(reg, d.family_id)
    changed = S.registry(db, research, S.definition(rule_hash="f" * 64))
    entry = changed.entry(d.family_id)                # the code changed, the registration did not
    assert "FAMILY_RULE_HASH_MISMATCH" in entry.blockers and not entry.executable and C.SHADOW not in entry.allowed_modes
    changed.register_all(actor_ref="system:test")
    entry = changed.entry(d.family_id)
    assert entry.governance_stage == "M0" and not entry.executable
    assert reg.governance.register_family(S.definition(rule_hash="f" * 64), actor_ref="x")["unchanged"] is True


def test_the_governance_log_is_append_only_and_tamper_evident(db, tmp_path):
    reg = S.registry(db, S.research_register(tmp_path), S.definition())
    reg.register_all(actor_ref="system:test")
    gov = FamilyGovernance(db)
    gov.record_evidence(S.TEST_FAMILY, "note", reference="test://x", actor_ref="operator:a")
    with db.connect() as conn:
        for statement in ("UPDATE cati_family_events SET actor_ref='x'", "DELETE FROM cati_family_events"):
            with pytest.raises(sqlite3.DatabaseError, match="append-only"):
                conn.execute(statement)
    assert gov.verify_chain() == {"ok": True, "records": 2, "head_hash": gov.history()[-1]["event_hash"]}
    with db.connect() as conn:                        # an attacker who removes the triggers still breaks the chain
        conn.execute("DROP TRIGGER trg_cati_fev_no_update")
        conn.execute("UPDATE cati_family_events SET payload='{}' WHERE seq=1")
    assert gov.verify_chain() == {"ok": False, "records": 1, "broken_at_seq": 1}
    with pytest.raises(FamilyGovernanceError, match="ACTOR_REQUIRED"):
        gov.record_evidence(S.TEST_FAMILY, "note", reference="test://x", actor_ref=" ")
