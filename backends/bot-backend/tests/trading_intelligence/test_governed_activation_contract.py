"""Governed activation contract (M0 -> pre-holdout -> holdout -> M5 -> demo -> M6 -> production): the gaps the
Section 22-27 suites do not already pin down. Every governance store here is a throw-away temporary database."""
import sqlite3
from types import SimpleNamespace

import pytest

from app.trading_intelligence.governance.promotion import GovernanceAuthority, PromotionGovernance
from app.trading_intelligence.research.certification.registry import HoldoutRegistry, SqliteResearchStore
from test_sections23_26 import _advance


def _plan(env="DEMO", acct="acctA"):
    return SimpleNamespace(environment=env, broker_account_id=acct, venue="BINANCE_USDM")


def test_governance_state_survives_a_restart(tmp_path):
    path = str(tmp_path / "gov.db")
    gov = PromotionGovernance(SqliteResearchStore(path))
    _advance(gov, "M5")
    gov.set_kill_switch(True, reason="incident", actor_ref="operator:test")
    restarted = PromotionGovernance(SqliteResearchStore(path))  # a new process reads only the durable record
    assert restarted.current_phase() == "M5" and len(restarted.history()) == 5
    assert restarted.kill_switch_on()
    assert GovernanceAuthority(SqliteResearchStore(path)).authorize_entry(_plan()) == (
        False, "CATI_NEW_ENTRY_KILL_SWITCH")


def test_promotion_audit_lineage_is_complete_append_only_and_secret_free(tmp_path):
    db = SqliteResearchStore(str(tmp_path / "gov.db"))
    gov = PromotionGovernance(db)
    gov.transition("M1", reason="section 9 shadow verified", actor_ref="operator:alice", source_commit="abc123",
                   evidence={"v2_benchmark_tag": "v2-frozen", "section9_shadow_verified": True,
                             "api_secret": "must-not-persist"})
    row = gov.history()[0]
    p = row["payload"]
    assert (row["from_phase"], row["to_phase"], row["source_commit"]) == ("M0", "M1", "abc123")
    assert p["actor_ref"] == "operator:alice" and p["reason"] and p["evidence_hash"] and row["recorded_at"]
    assert "must-not-persist" not in str(p)
    with db.connect() as conn:
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("UPDATE cati_promotion_phase_history SET to_phase='M8'")
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("DELETE FROM cati_promotion_phase_history")
    assert gov.current_phase() == "M1"


def test_holdout_authorization_is_bound_to_one_dataset_manifest(tmp_path):
    from test_cati_section22_guards import _ready_inputs

    from app.trading_intelligence.research.certification import pipeline as P
    from app.trading_intelligence.research.certification.holdout_guard import (
        authorize_holdout, holdout_authorization, pre_holdout_readiness,
    )

    db = SqliteResearchStore(str(tmp_path / "cert.db"))
    reg = HoldoutRegistry(db)
    frozen_ds = reg.reserve(dataset_hash="a" * 64, start_ms=0, end_ms=10)
    other_ds = reg.reserve(dataset_hash="b" * 64, start_ms=0, end_ms=10)  # same window, different dataset
    assert frozen_ds != other_ds
    manifest, freeze, run = _ready_inputs()
    ready = pre_holdout_readiness(dataset_manifest=manifest, acquisition_state="COMPLETE", freeze=freeze,
                                  pre_holdout_run=run)
    authorize_holdout(db, holdout_id=frozen_ds, readiness=ready, policy_freeze_hash=freeze.freeze_hash,
                      actor_ref="ops", reason="throw-away test store")
    assert holdout_authorization(db, other_ds, freeze.freeze_hash) is None
    passed = {P.ST.FULL.value: SimpleNamespace(status=P.S.PASS.value)}
    res = P._holdout(True, reg, other_ds, freeze, passed, None, None, None, (), None, None, 0, True, {}, True)
    assert "HOLDOUT_AUTHORIZATION_REQUIRED" in res.reason_codes
    assert reg.status(other_ds)["status"] == reg.status(frozen_ds)["status"] == "RESERVED"


def test_analysis_runs_while_execution_authority_is_blocked_at_m0(tmp_path):
    from app.activation.cati import active_execution, cycle_shadow, global_market_state

    db = SqliteResearchStore(str(tmp_path / "gov.db"))
    auto = {}  # no operator overrides: the runtime default
    assert cycle_shadow(auto).active and global_market_state(auto).active
    for env in ("DEMO", "LIVE"):
        d = active_execution(db, environment=env, environ=auto)
        assert not d.active and "GOVERNANCE_PHASE_M0_NO_CATI_AUTHORITY" in d.to_dict()["reasons"]


@pytest.mark.parametrize("flags", [
    {"CATI_ACTIVE_EXECUTION_ENABLED": "1"},
    {"CATI_ACTIVE_EXECUTION_ENABLED": "true", "CATI_CAPITAL_ROUTING_SHADOW_ENABLED": "1",
     "CATI_EXIT_INTENT_ROUTING_ENABLED": "on"},
])
def test_operator_flags_and_user_toggles_cannot_grant_authority(tmp_path, flags):
    from app.activation.cati import active_execution

    db = SqliteResearchStore(str(tmp_path / "gov.db"))
    assert not active_execution(db, environment="DEMO", environ=flags).active
    # the authority reads only the durable phase: a bot's Auto Trading / Auto Capital Routing setting is not an input
    assert GovernanceAuthority(db).authorize_entry(_plan()) == (False, "GOVERNANCE_PHASE_M0_NO_CATI_AUTHORITY")
    _advance(PromotionGovernance(db), "M5")  # historical certification eligibility is still not execution
    assert GovernanceAuthority(db).authorize_entry(_plan()) == (False, "GOVERNANCE_PHASE_M5_NO_CATI_AUTHORITY")


@pytest.mark.parametrize("state,stage_status,expected,reason", [
    ("COMPLETE", "PASS", "READY", None),
    ("ACQUIRING", "PASS", "NOT_READY", "DATA_ACQUISITION_IN_PROGRESS"),
    ("COMPLETE", "FAIL", "NOT_READY", "PRE_HOLDOUT_RUN_NOT_COMPLETED"),
])
def test_readiness_cli_reads_recorded_evidence_only(tmp_path, capsys, state, stage_status, expected, reason):
    import json

    from test_cati_section22_guards import _ready_inputs

    from app.trading_intelligence.research.certification.cli import main

    manifest, freeze, _run = _ready_inputs()
    research = SqliteResearchStore(str(tmp_path / "cert.db"))
    with research.connect() as conn:
        conn.execute("INSERT INTO cati_certification_runs (certification_run_id, stage, status, scope_hash, "
                     "dataset_hash, policy_freeze_hash, certification_policy_hash, artifact_hash, recorded_at, "
                     "schema_version, table_version, payload, payload_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                     ("crun_t", "FULL", stage_status, "s", "d" * 64, freeze.freeze_hash, "p", "a" * 64, 1, "1", "1",
                      "{}", "h"))
    (tmp_path / "ds.json").write_text(json.dumps(manifest), encoding="utf-8")
    (tmp_path / "rep.json").write_text(json.dumps({"policy_freeze": freeze.to_dict(),
                                                   "dataset_manifest": {"dataset_hash": "d" * 64}}), encoding="utf-8")
    assert main(["readiness", "--dataset-manifest", str(tmp_path / "ds.json"), "--acquisition-state", state,
                 "--report", str(tmp_path / "rep.json"), "--research-db", str(tmp_path / "cert.db")]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == expected and (reason is None or reason in out["reason_codes"])
    assert HoldoutRegistry(research).status("any")["status"] == "UNRESERVED"  # readiness reserves / opens nothing
