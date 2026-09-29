"""CATI Section 22: frozen certification rules ENFORCED -- no holdout is opened by this suite except inside a
throw-away temporary research store, and only to prove the barrier ordering."""
import time
from types import SimpleNamespace

import pytest

from app.trading_intelligence.research.certification.freeze import build_policy_freeze
from app.trading_intelligence.research.certification.holdout_guard import (
    DATA_ACQUISITION_IN_PROGRESS, READY, authorize_holdout, holdout_authorization, pre_holdout_readiness,
)
from app.trading_intelligence.research.certification.policy import (
    RESEARCH_DEFAULT_V1_VALUES, canonical_certification_policy, research_default_v1,
)
from app.trading_intelligence.research.certification.registry import HoldoutRegistry, RegistryError, SqliteResearchStore

RESEARCH_DEFAULT_V1_HASH = "55631f31a30ad4a3cf09cf55e6af698bea5d60cce862a303d35c1bcdd842dfe3"
FROZEN_GATES = {
    "min_probability_expectancy_positive": 0.95, "catastrophic_floor_R_at_2x": -0.10, "max_holdout_drawdown_R": 10.0,
    "max_single_segment_positive_share": 0.50, "min_accepted_evidence_count": 100, "max_pbo": 0.25,
    "parameter_neighbor_min_positive_share": 0.75, "min_forward_demo_executed_count": 30,
}


def test_frozen_policy_hash_and_gate_values_are_pinned():
    assert research_default_v1().policy_hash == RESEARCH_DEFAULT_V1_HASH
    assert canonical_certification_policy().policy_hash == RESEARCH_DEFAULT_V1_HASH
    assert {k: v for k, (v, _why) in RESEARCH_DEFAULT_V1_VALUES.items()} == FROZEN_GATES
    assert research_default_v1().min_forward_demo_days.value == 30


def test_crypto_and_fx_certification_stay_separate():
    from app.market_data import research_status as rs
    from app.trading_intelligence.research.certification import scopes

    crypto, fx = rs.load_manifest("CRYPTO_BROAD")["manifest"], rs.load_manifest("FX_REFERENCE")["manifest"]
    assert crypto["universe_hash"] != fx["universe_hash"] and crypto["asset_class"] == "CRYPTO" and fx["asset_class"] == "FX"
    crypto_scopes = {s for s in scopes.SCOPES if s.startswith("CRYPTO/")}
    fx_scopes = {s for s in scopes.SCOPES if s.startswith("FX/")}
    assert crypto_scopes and fx_scopes and not crypto_scopes & fx_scopes
    assert len({scopes.holdout_namespace(s, "d") for s in crypto_scopes | fx_scopes}) == len(crypto_scopes | fx_scopes)


def _ready_inputs():
    from test_cati_section13_closure import frozen

    manifest = frozen()
    freeze = build_policy_freeze(certification_policy=canonical_certification_policy(), source_commit="abc",
                                 source_tree_dirty=False)
    run = {"stage": "FULL", "status": "PASS", "artifact_hash": "a" * 64}
    return manifest, freeze, run


def test_readiness_is_ready_only_with_complete_frozen_real_evidence():
    manifest, freeze, run = _ready_inputs()
    ok = pre_holdout_readiness(dataset_manifest=manifest, acquisition_state="COMPLETE", freeze=freeze, pre_holdout_run=run)
    assert ok["status"] == READY and ok["reason_codes"] == []
    again = pre_holdout_readiness(dataset_manifest=manifest, acquisition_state="COMPLETE", freeze=freeze, pre_holdout_run=run)
    assert again["readiness_hash"] == ok["readiness_hash"]  # deterministic, no wall clock


@pytest.mark.parametrize("change,reason", [
    (dict(acquisition_state="ACQUIRING"), DATA_ACQUISITION_IN_PROGRESS),
    (dict(dataset_manifest=None), "DATASET_MANIFEST_NOT_FROZEN"),
    (dict(tamper=True), "DATASET_MANIFEST_NOT_FROZEN"),
    (dict(synthetic=True), "SYNTHETIC_OR_NON_CERTIFIABLE_DATA"),
    (dict(dirty=True), "POLICY_FREEZE_NOT_CERTIFIABLE"),
    (dict(retuned=True), "POLICY_IS_NOT_THE_CANONICAL_SECTION22_POLICY"),
    (dict(pre_holdout_run={"stage": "FULL", "status": "FAIL", "artifact_hash": "x"}), "PRE_HOLDOUT_RUN_NOT_COMPLETED"),
])
def test_mutable_incomplete_or_non_canonical_inputs_are_not_ready(change, reason):
    import copy

    manifest, freeze, run = _ready_inputs()
    kw = dict(dataset_manifest=manifest, acquisition_state="COMPLETE", freeze=freeze, pre_holdout_run=run)
    if change.pop("tamper", False):
        kw["dataset_manifest"] = {**copy.deepcopy(manifest), "symbols": ["EURUSD", "GBPUSD"]}
    if change.pop("synthetic", False):
        m = copy.deepcopy(manifest)
        m["partitions"] = [{**p, "source": "synthetic-fixture"} for p in m["partitions"]]
        kw["dataset_manifest"] = m
    if change.pop("dirty", False):
        kw["freeze"] = build_policy_freeze(certification_policy=canonical_certification_policy(), source_commit="abc",
                                           source_tree_dirty=True)
    if change.pop("retuned", False):
        tuned = canonical_certification_policy().configure(max_pbo=0.20)  # any change = a new policy
        kw["freeze"] = build_policy_freeze(certification_policy=tuned, source_commit="abc", source_tree_dirty=False)
    kw.update(change)
    r = pre_holdout_readiness(**kw)
    assert r["status"] == "NOT_READY" and reason in r["reason_codes"]


def test_fx_currently_reports_not_ready_while_acquiring():
    manifest, freeze, run = _ready_inputs()
    r = pre_holdout_readiness(dataset_manifest=None, acquisition_state="ACQUIRING", freeze=None, pre_holdout_run=None)
    assert r["status"] == "NOT_READY" and r["reason_codes"][0] == DATA_ACQUISITION_IN_PROGRESS


def test_authorization_is_refused_unless_ready_and_never_opens(tmp_path):
    db = SqliteResearchStore(str(tmp_path / "cert.db"))
    reg = HoldoutRegistry(db)
    hid = reg.reserve(dataset_hash="d" * 64, start_ms=0, end_ms=10)
    manifest, freeze, run = _ready_inputs()
    not_ready = pre_holdout_readiness(dataset_manifest=manifest, acquisition_state="ACQUIRING", freeze=freeze,
                                      pre_holdout_run=run)
    with pytest.raises(RegistryError, match="NOT_READY"):
        authorize_holdout(db, holdout_id=hid, readiness=not_ready, policy_freeze_hash=freeze.freeze_hash,
                          actor_ref="ops", reason="test")
    ready = pre_holdout_readiness(dataset_manifest=manifest, acquisition_state="COMPLETE", freeze=freeze,
                                  pre_holdout_run=run)
    with pytest.raises(RegistryError, match="actor"):
        authorize_holdout(db, holdout_id=hid, readiness=ready, policy_freeze_hash=freeze.freeze_hash, actor_ref="",
                          reason="")
    with pytest.raises(RegistryError, match="another policy freeze"):
        authorize_holdout(db, holdout_id=hid, readiness=ready, policy_freeze_hash="other", actor_ref="ops", reason="r")
    assert holdout_authorization(db, hid, freeze.freeze_hash) is None
    authorize_holdout(db, holdout_id=hid, readiness=ready, policy_freeze_hash=freeze.freeze_hash, actor_ref="ops",
                      reason="deliberate operator action in a throw-away test store")
    assert holdout_authorization(db, hid, freeze.freeze_hash) is not None
    assert holdout_authorization(db, hid, "another-freeze") is None  # bound to ONE policy freeze
    assert reg.status(hid)["status"] == "RESERVED"  # authorizing opens nothing


def test_pipeline_refuses_to_open_without_recorded_authorization(tmp_path):
    from app.trading_intelligence.research.certification import pipeline as P

    db = SqliteResearchStore(str(tmp_path / "cert.db"))
    reg = HoldoutRegistry(db)
    hid = reg.reserve(dataset_hash="d" * 64, start_ms=0, end_ms=10)
    freeze = build_policy_freeze(certification_policy=canonical_certification_policy(), source_commit="abc",
                                 source_tree_dirty=False)
    passed = {P.ST.FULL.value: SimpleNamespace(status=P.S.PASS.value)}
    res = P._holdout(True, reg, hid, freeze, passed, None, None, None, (), None, None, 0, True, {}, True)
    assert res.status == P.S.NOT_RUN.value and "HOLDOUT_AUTHORIZATION_REQUIRED" in res.reason_codes
    assert reg.status(hid)["status"] == "RESERVED"


def test_no_retune_after_holdout_a_burned_holdout_is_never_reauthorized(tmp_path):
    db = SqliteResearchStore(str(tmp_path / "cert.db"))
    reg = HoldoutRegistry(db)
    hid = reg.reserve(dataset_hash="e" * 64, start_ms=0, end_ms=10)
    reg.open(hid, policy_freeze_hash="F1")          # simulated past event in a throw-away store
    reg.burn(hid, policy_freeze_hash="F1", result_hash="r")
    manifest, freeze, run = _ready_inputs()
    ready = pre_holdout_readiness(dataset_manifest=manifest, acquisition_state="COMPLETE", freeze=freeze,
                                  pre_holdout_run=run)
    with pytest.raises(RegistryError, match="never be re-authorized"):
        authorize_holdout(db, holdout_id=hid, readiness=ready, policy_freeze_hash=freeze.freeze_hash, actor_ref="ops",
                          reason="retune")


def test_nothing_in_the_codebase_authorizes_or_opens_a_holdout_automatically():
    import re
    from pathlib import Path

    app = Path(__file__).resolve().parents[2] / "app"
    callers = [p.relative_to(app).as_posix() for p in app.rglob("*.py")
               if re.search(r"authorize_holdout\(", p.read_text(encoding="utf-8", errors="ignore"))]
    assert callers == ["trading_intelligence/research/certification/holdout_guard.py"]  # definition only
    openers = [p.relative_to(app).as_posix() for p in app.rglob("*.py")
               if re.search(r"holdouts\.open\(", p.read_text(encoding="utf-8", errors="ignore"))]
    assert openers == ["trading_intelligence/research/certification/pipeline.py"]  # behind the barrier only


def test_m5_does_not_grant_execution_and_nothing_starts_above_m0(tmp_path):
    from app.trading_intelligence.governance.phases import PHASE_BY_ID
    from app.trading_intelligence.governance.promotion import GovernanceAuthority, PromotionGovernance

    db = SqliteResearchStore(str(tmp_path / "gov.db"))
    assert PromotionGovernance(db).current_phase() == "M0"  # a merge / deploy / data completion records nothing
    auth = GovernanceAuthority(db)
    auth.gov = SimpleNamespace(current_phase=lambda: "M5", kill_switch_on=lambda scope=None: False,
                               scope_granted=lambda **k: False)
    ok, why = auth.authorize_entry(SimpleNamespace(environment="DEMO", broker_account_id="a", venue="v"))
    assert ok is False and why == "GOVERNANCE_PHASE_M5_NO_CATI_AUTHORITY"
    m5 = PHASE_BY_ID["M5"]
    assert "production" not in m5.title.lower() and m5.cati_authority == "ADVISORY"  # replay eligibility, not trading


def test_m6_forward_demo_needs_30_trades_and_30_days(tmp_path):
    from test_section22_framework import DAY, _attempt, _plan_row, _store

    from app.trading_intelligence.research.certification.forward_demo import ForwardDemoCertificationTracker

    db = _store(tmp_path, "rt.db")
    now = int(time.time() * 1000)
    with db.connect() as conn:
        for i in range(30):  # 30 executed trades spread over 32 calendar days
            t = now - 33 * DAY + (i * 32 * DAY) // 29
            _plan_row(conn, f"p{i}", t=t)
            _attempt(conn, f"p{i}", t=t + 1)
    snap = ForwardDemoCertificationTracker(db, venue="BINANCE_USDM", min_executed=30).snapshot()
    assert snap["executed_count"] == 30 and snap["elapsed_days"] >= 30 and snap["status"] == "PASS"
    # 30 trades but inside 5 days, or 30 days but 29 trades: never PASS (AND, not OR -- see also
    # test_section22_framework forward-demo tests)
    assert ForwardDemoCertificationTracker(db, venue="BINANCE_USDM", min_executed=31).snapshot()["status"] == "IN_PROGRESS"
