"""Sections 23-26: CATI ML estimator architecture (+ legacy V2 ML isolation),
repository destination map, promotion governance M0-M9, success standard.

Synthetic data exercises code paths only; it can never train a promotable
model or advance a governance phase.
"""
from __future__ import annotations

import dataclasses
import json
import random
from pathlib import Path

import pytest

from app.trading_intelligence.governance.phases import PHASE_IDS, rollback_targets
from app.trading_intelligence.governance.promotion import (
    GovernanceAuthority, GovernanceError, PromotionGovernance, StaticAuthority,
)
from app.trading_intelligence.ml import boundaries as BND
from app.trading_intelligence.ml.contracts import (
    ROLE_SPECS, LegacyArtifactRejected, ModelCard, ModelRole, ModelStatus,
)
from app.trading_intelligence.ml.datasets import (
    DatasetContractViolation, execution_dataset, market_dataset, market_labels_from_export, reject_account_labels,
)
from app.trading_intelligence.ml.features import FEATURE_SCHEMAS, FeatureContractViolation, validate_schema
from app.trading_intelligence.ml.registry import ModelRegistry, ModelRegistryError
from app.trading_intelligence.research.certification.registry import SqliteResearchStore

BOT_ROOT = Path(__file__).resolve().parents[2]
DAY = 86_400_000
T0 = 1_767_225_600_000


def _store(tmp_path, name="g.db"):
    return SqliteResearchStore(str(tmp_path / name))


def _records(n=700, seed=3):
    """Replay-record-shaped synthetic rows (tests only)."""
    rng = random.Random(seed)
    fams = ("TREND_PULLBACK_V2", "BREAKOUT_VOL_EXPANSION_V2", "RANGE_MEAN_REVERSION_V2", "MOMENTUM_CONTINUATION_V1")
    out = []
    for i in range(n):
        p = rng.random()
        win = rng.random() < 0.3 + 0.4 * p
        out.append({"setup_candidate_id": f"c{i}", "symbol": "BTCUSDT" if i % 2 else "ETHUSDT",
                    "decision_time": T0 + i * 900_000, "setup_family": fams[i % 4], "side": "LONG" if i % 3 else "SHORT",
                    "regime": ("TREND", "RANGE", "VOL_EXPANSION")[i % 3], "session": "ASIA_00_08",
                    "volume_liquidity_proxy": "NORMAL", "symbol_group": "G0", "forecast_p": p, "cost_R": 0.1,
                    "net_R": 1.0 if win else -1.1, "gross_R": 1.1 if win else -1.0, "terminal_outcome": "X",
                    "veto_outcome": "REJECT" if i % 5 == 0 else "WATCH", "broker_account_id": "acct_secret_tenant"})
    return out


# ============================== LEGACY V2 ML ==============================
def test_legacy_v2_scorer_still_loads_for_v2_and_is_classified_legacy():
    from app.core.config import settings
    from app.ml.scorer import ACTION_SKIP, MLEntryScorer
    from app.trading_intelligence.ml.legacy import LEGACY_ML_COMPONENTS, LEGACY_V2_ML

    assert MLEntryScorer and ACTION_SKIP == "SKIP" and settings.ML_ENABLED is False
    assert LEGACY_V2_ML in LEGACY_ML_COMPONENTS["app/ml/scorer.py"][0]
    assert "DO_NOT_USE_FOR_CATI" in LEGACY_ML_COMPONENTS["app/ml/scorer.py"][0]


def test_legacy_artifact_cannot_masquerade_as_a_cati_model(tmp_path):
    from shared_lib.ml.contract import ML_CONTRACT_VERSION, ML_FEATURE_COLUMNS, ML_FEATURE_SCHEMA_HASH

    from app.trading_intelligence.ml.artifacts import load_artifact

    d = tmp_path / "entry_quality_v2"
    d.mkdir()
    (d / "metadata.json").write_text(json.dumps({"contract_version": ML_CONTRACT_VERSION, "schema_hash":
                                                 ML_FEATURE_SCHEMA_HASH, "feature_columns": list(ML_FEATURE_COLUMNS)}))
    with pytest.raises(LegacyArtifactRejected):
        load_artifact(d, expected_role="OUTCOME", feature_schema_hash="x", label_schema_hash="y")
    # even a card.json claiming the legacy contract is refused
    (d / "card.json").write_text(json.dumps({"contract_version": ML_CONTRACT_VERSION, "role": "OUTCOME"}))
    with pytest.raises(LegacyArtifactRejected):
        load_artifact(d, expected_role="OUTCOME", feature_schema_hash="x", label_schema_hash="y")


def test_legacy_v2_semantics_and_flags_are_not_cati_authority(monkeypatch):
    from app.trading_intelligence.ml.config import CATIMLConfig

    bad = dataclasses.replace(FEATURE_SCHEMAS["OUTCOME"], columns=("confidence_normed", "threshold_gap"))
    with pytest.raises(FeatureContractViolation):
        validate_schema(bad)
    for name in ("CATI_ML_ENABLED", "CATI_ML_SHADOW_ENABLED"):
        monkeypatch.delenv(name, raising=False)
    baseline = CATIMLConfig.from_env()  # AUTO: promotion + governance decide, not a flag
    for legacy in (("true", "false", "0.9"), ("false", "true", "0.1")):  # legacy V2 flags are never read
        monkeypatch.setenv("ML_ENABLED", legacy[0])
        monkeypatch.setenv("ML_SHADOW_MODE", legacy[1])
        monkeypatch.setenv("ML_HARD_BLOCK_FLOOR", legacy[2])
        assert CATIMLConfig.from_env() == baseline
    monkeypatch.setenv("CATI_ML_ENABLED", "off")  # only the CATI operator override can switch ML off
    assert CATIMLConfig.from_env().ml_enabled is False


# ============================== SECTION 23: CONTRACTS / DATA ==============================
def _card(**kw):
    base = dict(role="OUTCOME", model_version="1", training_dataset_hash="d", feature_schema_hash="f",
                label_schema_hash="l", training_start=1, training_end=2, code_commit="c", hyperparameters={"a": 1},
                calibration={"m": "iso"}, source_kind="REAL_MARKET", artifact_hash="h")
    return ModelCard(**{**base, **kw})


def test_all_six_roles_have_hard_boundaries_and_identity_covers_every_input():
    assert set(ROLE_SPECS) == {r.value for r in ModelRole}
    assert all(spec.hard_boundaries and spec.fail_mode for spec in ROLE_SPECS.values())
    assert ROLE_SPECS["SLIPPAGE"].label_family == "EXECUTION" and ROLE_SPECS["EXIT"].label_family == "POSITION"
    base = _card().model_id
    for change in ({"feature_schema_hash": "f2"}, {"label_schema_hash": "l2"}, {"hyperparameters": {"a": 2}},
                   {"training_dataset_hash": "d2"}, {"calibration": {"m": "none"}}, {"code_commit": "c2"},
                   {"artifact_hash": "h2"}):
        assert _card(**change).model_id != base, change


def test_market_models_are_tenant_neutral_and_outcomes_never_features():
    for role in ("OUTCOME", "RANKING", "REGIME", "OOD"):
        validate_schema(FEATURE_SCHEMAS[role])
    for bad_cols in (("broker_account_id",), ("net_R",), ("open_positions_count",)):
        with pytest.raises(FeatureContractViolation):
            validate_schema(dataclasses.replace(FEATURE_SCHEMAS["OUTCOME"], columns=bad_cols))
    ds = market_dataset("OUTCOME", _records(50), source_kind="SYNTHETIC_TEST")
    assert all("broker_account_id" not in r and "net_R" not in r for r in ds.rows)


def test_market_execution_account_outcomes_stay_separate():
    ds = market_dataset("OUTCOME", _records(50), source_kind="SYNTHETIC_TEST")
    assert ds.n == 50  # veto/risk REJECTED hypotheses remain valid market examples
    export = [{"lineage": {"trade_plan_id": "p1"}, "market_outcome": {"terminal_outcome": "TARGET_BEFORE_STOP",
                                                                     "net_R": 1.2, "market_observation_valid": True},
               "execution_outcome": {"status": "NOT_SUBMITTED_RISK_REJECTED", "hard_risk_rejected": True}},
              {"lineage": {"trade_plan_id": "p2"}, "market_outcome": {"terminal_outcome": "UNLABELED"},
               "execution_outcome": {"status": "REJECTED"}}]
    labels = market_labels_from_export(export)
    assert labels == [{"trade_plan_id": "p1", "terminal_outcome": "TARGET_BEFORE_STOP", "net_R": 1.2,
                       "risk_rejected": True}]  # an execution failure never becomes a negative market label
    with pytest.raises(DatasetContractViolation):
        reject_account_labels("ACCOUNT")
    with pytest.raises(DatasetContractViolation):
        market_dataset("SLIPPAGE", _records(5), source_kind="X")
    ex = execution_dataset([{"status": "FILLED", "requested_price": 100.0, "filled_price": 100.05, "side": "LONG",
                             "recorded_at": 1}, {"status": "REJECTED", "requested_price": 100.0, "recorded_at": 2},
                            {"status": "SUBMIT_UNKNOWN", "recorded_at": 3}], source_kind="X")
    assert ex.n == 1 and ex.labels[0] == pytest.approx(5.0)


def test_chronological_split_purges_label_windows():
    ds = market_dataset("OUTCOME", _records(200), source_kind="SYNTHETIC_TEST")
    horizon = 48 * 900_000
    train, hold = ds.chronological_split(horizon_ms=horizon, holdout_fraction=0.2)
    assert max(train.times) + horizon < min(hold.times) and list(train.times) == sorted(train.times)


def test_training_is_blocked_without_real_sufficient_evidence():
    from app.trading_intelligence.ml.training import training_gate

    small = market_dataset("OUTCOME", _records(100), source_kind="REAL_MARKET")
    g = training_gate("OUTCOME", small, certification_ready=False, holdout_reserved=True)
    assert g.status == "BLOCKED_EVIDENCE" and "SAMPLES_BELOW_500" in g.reasons
    assert "SECTION_22_EVIDENCE_INSUFFICIENT_FOR_ML" in g.reasons
    synth = market_dataset("OUTCOME", _records(700), source_kind="SYNTHETIC_TEST")
    assert "NON_REAL_SOURCE" in training_gate("OUTCOME", synth, certification_ready=True, holdout_reserved=True).reasons
    assert training_gate("SLIPPAGE", None, certification_ready=True, holdout_reserved=True).status == "BLOCKED_EVIDENCE"


@pytest.fixture(scope="module")
def trained():
    from app.trading_intelligence.ml.training import train_estimator, training_gate

    ds = market_dataset("OUTCOME", _records(900), source_kind="SYNTHETIC_TEST")
    gate = training_gate("OUTCOME", ds, certification_ready=False, holdout_reserved=False, allow_synthetic=True)
    assert gate.status == "READY_FOR_RESEARCH" and gate.reasons  # research-only fixture path
    est, card, metrics = train_estimator("OUTCOME", ds, horizon_ms=24 * 900_000, code_commit="test")
    return ds, est, card, metrics


def test_outcome_model_is_calibrated_chronological_and_deterministic(trained):
    from app.trading_intelligence.ml.training import train_estimator

    ds, est, card, metrics = trained
    assert card.calibration["method"] == "ISOTONIC_CHRONOLOGICAL" and metrics["holdout_calibration"]["status"] == "OK"
    preds = est.predict(list(ds.rows[:20]))
    assert all(0.0 <= p <= 1.0 for p in preds)
    est2, card2, _ = train_estimator("OUTCOME", ds, horizon_ms=24 * 900_000, code_commit="test")
    assert est2.predict(list(ds.rows[:20])) == preds and card2.identity() == card.identity()


def test_artifacts_are_immutable_and_integrity_checked(trained, tmp_path):
    from app.trading_intelligence.ml.artifacts import load_artifact, save_artifact

    ds, est, card, _ = trained
    d, saved = save_artifact(tmp_path, est, card)
    fs, ls = saved.feature_schema_hash, saved.label_schema_hash
    model, meta = load_artifact(d, expected_role="OUTCOME", feature_schema_hash=fs, label_schema_hash=ls)
    assert meta["model_id"] == saved.model_id and model.predict(list(ds.rows[:3])) == est.predict(list(ds.rows[:3]))
    with pytest.raises(LegacyArtifactRejected):
        load_artifact(d, expected_role="SLIPPAGE", feature_schema_hash=fs, label_schema_hash=ls)  # role isolation
    (d / "model.joblib").write_bytes((d / "model.joblib").read_bytes() + b"x")
    with pytest.raises(ValueError):
        load_artifact(d, expected_role="OUTCOME", feature_schema_hash=fs, label_schema_hash=ls)


def test_registry_is_append_only_and_promotion_needs_evidence(trained, tmp_path):
    from app.trading_intelligence.ml.artifacts import save_artifact
    from app.trading_intelligence.ml.promotion import evaluate_model_promotion

    _ds, est, card, _ = trained
    _d, saved = save_artifact(tmp_path / "art", est, card)
    reg = ModelRegistry(_store(tmp_path))
    mid = reg.register(saved)
    assert reg.status(mid) == ModelStatus.RESEARCH.value
    with pytest.raises(ModelRegistryError):
        reg.transition(mid, ModelStatus.PROMOTED.value, evidence={})       # no skipping
    reg.transition(mid, "VALIDATED", evidence={"holdout": "ok"})
    reg.transition(mid, "SHADOW", evidence={})
    reg.transition(mid, "PROMOTION_ELIGIBLE", evidence={})
    with pytest.raises(ModelRegistryError):
        reg.transition(mid, "PROMOTED", evidence={"promotion_gate_passed": True})  # governance missing
    decision = evaluate_model_promotion(saved.to_dict(), artifact_verified=True, reproducible=True,
                                        ood_behaviour_tested=True, holdout_untouched=True,
                                        shadow={"n": 500, "ml_not_worse": True}, section22_ready_for_ml=True,
                                        governance_phase="M6")
    assert not decision.eligible and "NON_REAL_TRAINING_DATA" in decision.reasons  # synthetic can never promote
    reg.transition(mid, "REJECTED", evidence={"reasons": list(decision.reasons)})
    with pytest.raises(ModelRegistryError):
        reg.transition(mid, "SHADOW", evidence={})
    import sqlite3

    with sqlite3.connect(str(tmp_path / "g.db")) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM cati_ml_model_events")


def test_shadow_never_affects_the_decision(tmp_path):
    from app.trading_intelligence.ml.shadow import ShadowRecorder, champion_challenger

    rec = ShadowRecorder(_store(tmp_path))
    out = rec.shadow_compare(model_id="m1", role="OUTCOME", deterministic=0.41, ml=0.93, decision_time=T0,
                             market_state_id="ms1", setup_candidate_id="c1", ood_score=0.1)
    assert out == 0.41
    rows = rec.rows("m1")
    assert len(rows) == 1 and rows[0]["market_state_id"] == "ms1" and rows[0]["payload"]["ml"] == 0.93
    cc = champion_challenger(rows, {"c1": 0})
    assert cc["n"] == 1 and cc["ml_not_worse"] is False


# ============================== SECTION 23: HARD BOUNDARIES ==============================
def test_regime_model_cannot_bypass_data_quality():
    class _MS:
        is_usable = False

    assert BND.regime_estimate(_MS(), {"TREND": 1.0}, {"TREND": 0.9, "RANGE": 0.1})["source"] == "UNAVAILABLE"
    _MS.is_usable = True
    assert BND.regime_estimate(_MS(), "det", {"TREND": 1.0}, ml_ood_score=0.99)["source"] == "DETERMINISTIC"
    ok = BND.regime_estimate(_MS(), "det", {"TREND": 3.0, "RANGE": 1.0})
    assert ok["source"] == "ML" and ok["distribution"]["TREND"] == 0.75 and 0 < ok["uncertainty"] < 1


def test_outcome_model_cannot_bypass_admission_or_veto():
    from _plan import venue_evaluated

    ev = venue_evaluated("BTCUSDT")
    costly = dataclasses.replace(ev, cost_estimate=dataclasses.replace(
        ev.cost_estimate, fee_R=ev.cost_estimate.fee_R + 5.0, total_cost_R=ev.cost_estimate.total_cost_R + 5.0))
    opp, veto = BND.outcome_through_admission(costly, 0.99)
    assert opp.admission_status != "ECONOMICALLY_ADMISSIBLE" and veto.outcome != "APPROVE_FOR_RANKING"


def test_slippage_ranking_exit_ood_are_clamped():
    assert BND.slippage_cost_bps(5.0, 1.0) == 5.0 and BND.slippage_cost_bps(5.0, 9.0) == 9.0
    assert BND.slippage_cost_bps(5.0, None) == 5.0
    assert BND.rerank_approved(["a", "b", "c"], {"c": 0.9, "a": 0.1, "zz": 99.0}) == ["c", "a", "b"]  # nothing added
    assert BND.exit_stop("LONG", 95.0, 90.0) == 95.0 and BND.exit_stop("LONG", 95.0, 97.0) == 97.0
    assert BND.exit_stop("SHORT", 105.0, 110.0) == 105.0
    assert BND.combine_ood(0.3, 0.1) == 0.3 and BND.combine_ood(0.3, 0.8) == 0.8 and BND.combine_ood(None, 0.0) == 1.0


def test_estimator_authority_never_falls_back_to_v2(tmp_path):
    from app.trading_intelligence.ml.config import CATIMLConfig
    from app.trading_intelligence.ml.promotion import estimator_authority, role_status_report

    reg = ModelRegistry(_store(tmp_path))
    off = estimator_authority("OUTCOME", registry=reg, governance_authorizes_cati=True, config=CATIMLConfig())
    on = estimator_authority("OUTCOME", registry=reg, governance_authorizes_cati=False,
                             config=CATIMLConfig(ml_enabled=True))
    none = estimator_authority("OUTCOME", registry=reg, governance_authorizes_cati=True,
                               config=CATIMLConfig(ml_enabled=True))
    assert {off["estimator"], on["estimator"], none["estimator"]} == {"DETERMINISTIC"}
    report = role_status_report(reg)
    assert set(report) == {r.value for r in ModelRole}
    assert all(v["training"] == "BLOCKED_EVIDENCE" and v["promotion"] == "NOT_READY" for v in report.values())


# ============================== SECTION 24 ==============================
def test_destination_map_is_semantically_compliant():
    from app.trading_intelligence.governance.destination_map import LEGACY_AFTER_PROMOTION, compliance, verify

    status, gaps = compliance(BOT_ROOT)
    # Section 24 closure: GlobalMarketState contract + engine + evidence integrated in the cycle
    assert status == "PASS" and gaps == []
    assert all(v["status"] != "MISSING" for v in verify(BOT_ROOT).values())
    assert all((BOT_ROOT / p).exists() for p in LEGACY_AFTER_PROMOTION)  # retained until M9


def test_no_duplicate_alpha_authority_inside_cati():
    ti = BOT_ROOT / "app" / "trading_intelligence"
    offenders = []
    for p in ti.rglob("*.py"):
        text = p.read_text()
        if "from app.threshold" in text or "import app.threshold" in text or "from app.strategy.master_ensemble" in text:
            offenders.append(str(p.relative_to(BOT_ROOT)))
    assert offenders == []  # no V2 confidence floor / ensemble inside CATI decision code


# ============================== SECTION 25 ==============================
EVIDENCE = {
    "M1": dict(evidence={"v2_benchmark_tag": "v2-frozen-benchmark", "section9_shadow_verified": True}),
    "M2": dict(evidence={"sections_10_12_shadow_verified": True}),
    "M3": dict(evidence={"sections_13_17_shadow_verified": True}),
    "M4": dict(evidence={"section19_shadow_verified": True}),
    "M5": dict(evidence={"certification_policy_frozen": True}, certification_run_id="crun_1", policy_freeze_hash="f"),
    "M6": dict(evidence={"replay_gates": {g: "PASS" for g in ("A_INTEGRITY", "B_NET_EXPECTANCY", "C_COST_STRESS",
                                                              "D_HOLDOUT", "E_CONCENTRATION", "F_CALIBRATION",
                                                              "G_EVIDENCE_COUNT")}},
               certification_run_id="crun_2", policy_freeze_hash="f"),
    "M7": dict(evidence={"forward_demo_gate_passed": True, "operational_gate_passed": True, "promoted_scope_hash": "s"}),
    "M8": dict(evidence={"production_evidence_passed": True, "rollback_release_tag": "cati-v1-rc"}),
    "M9": dict(evidence={"rollback_evidence_ref": "r", "migration_window_elapsed": True}),
}


def _advance(gov, to):
    for phase in PHASE_IDS[1:PHASE_IDS.index(to) + 1]:
        gov.transition(phase, reason="test", actor_ref="operator:test", source_commit="abc", **EVIDENCE[phase])


def test_fresh_state_is_m0_and_nothing_but_a_recorded_transition_moves_it(tmp_path):
    gov = PromotionGovernance(_store(tmp_path))
    assert gov.current_phase() == "M0" and gov.history() == []  # merging code to main changes nothing here


def test_phases_advance_one_at_a_time_with_evidence(tmp_path):
    gov = PromotionGovernance(_store(tmp_path))
    with pytest.raises(GovernanceError):
        gov.transition("M2", reason="skip", actor_ref="op", source_commit="abc", **EVIDENCE["M2"])
    with pytest.raises(GovernanceError):
        gov.transition("M1", reason="no evidence", actor_ref="op", source_commit="abc", evidence={})
    _advance(gov, "M5")
    bad = {g: "PASS" for g in EVIDENCE["M6"]["evidence"]["replay_gates"]} | {"D_HOLDOUT": "BLOCKED"}
    with pytest.raises(GovernanceError):
        gov.transition("M6", reason="r", actor_ref="op", source_commit="abc", evidence={"replay_gates": bad},
                       certification_run_id="c", policy_freeze_hash="f")
    _advance_from(gov, "M5", "M9")
    assert gov.current_phase() == "M9" and len(gov.history()) == 9
    row = gov.history()[5]
    assert row["from_phase"] == "M5" and row["to_phase"] == "M6" and row["certification_run_id"] == "crun_2"
    assert row["payload"]["evidence_hash"] and row["source_commit"] == "abc"


def _advance_from(gov, frm, to):
    for phase in PHASE_IDS[PHASE_IDS.index(frm) + 1:PHASE_IDS.index(to) + 1]:
        gov.transition(phase, reason="test", actor_ref="operator:test", source_commit="abc", **EVIDENCE[phase])


def test_rollback_rules(tmp_path):
    gov = PromotionGovernance(_store(tmp_path))
    _advance(gov, "M6")
    gov.transition("M0", reason="rollback to frozen V2 benchmark", actor_ref="op")
    assert gov.current_phase() == "M0"
    _advance(gov, "M8")
    assert rollback_targets("M8") == ()
    with pytest.raises(GovernanceError):
        gov.transition("M0", reason="no silent V2 after M8", actor_ref="op")


class _Plan:
    def __init__(self, env="DEMO", acct="acctA", venue="BINANCE_USDM"):
        self.environment, self.broker_account_id, self.venue = env, acct, venue


def test_authority_by_phase_scope_and_kill_switch(tmp_path):
    db = _store(tmp_path)
    auth, gov = GovernanceAuthority(db), PromotionGovernance(db)
    assert auth.authorize_entry(_Plan()) == (False, "GOVERNANCE_PHASE_M0_NO_CATI_AUTHORITY")
    assert auth.v2_may_place_orders(environment="DEMO")
    _advance(gov, "M6")
    assert auth.authorize_entry(_Plan("DEMO"))[0] and auth.authorize_entry(_Plan("REAL")) == (False, "M6_IS_DEMO_ONLY")
    assert not auth.v2_may_place_orders(environment="DEMO") and auth.v2_may_place_orders(environment="REAL")
    _advance_from(gov, "M6", "M7")
    assert auth.authorize_entry(_Plan("REAL")) == (False, "M7_SCOPE_NOT_PROMOTED")
    with pytest.raises(GovernanceError):
        gov.grant_scope(broker_account_id="", venue="BINANCE_USDM", environment="REAL", reason="global")
    gov.grant_scope(broker_account_id="acctA", venue="BINANCE_USDM", environment="REAL", reason="limited")
    assert auth.authorize_entry(_Plan("REAL"))[0] and not auth.authorize_entry(_Plan("REAL", acct="acctB"))[0]
    assert not auth.v2_may_place_orders(environment="REAL", broker_account_id="acctA", venue="BINANCE_USDM")
    gov.set_kill_switch(True, reason="incident", actor_ref="op")
    assert auth.authorize_entry(_Plan("REAL")) == (False, "CATI_NEW_ENTRY_KILL_SWITCH")
    gov.set_kill_switch(False, reason="resolved", actor_ref="op")
    _advance_from(gov, "M7", "M8")
    assert not auth.v2_fallback_allowed() and not auth.v2_may_place_orders(environment="REAL")


def test_boundary_needs_governance_even_with_the_flag_on(tmp_path):
    from _exec import Harness

    from app.trading_intelligence.execution.boundary import BoundaryStatus as B
    from app.trading_intelligence.trade_plan.evidence_store import TradePlanEvidenceStore

    h = Harness(tmp_path)
    TradePlanEvidenceStore(h.db).append(h.plan)
    res = h.run(h.boundary(authority=GovernanceAuthority(h.db)))   # real governance, fresh DB = M0
    assert res.status == B.GOVERNANCE_NOT_AUTHORIZED and not h.seen["orders"]
    assert h.reservation_status() == "RESERVED"                     # nothing consumed, nothing corrupted
    gov = PromotionGovernance(h.db)
    _advance(gov, "M6")
    gov.set_kill_switch(True, reason="stop new entries", actor_ref="op")
    assert h.run(h.boundary(authority=GovernanceAuthority(h.db))).reason_codes == ("CATI_NEW_ENTRY_KILL_SWITCH",)
    gov.set_kill_switch(False, reason="resume", actor_ref="op")
    assert h.run(h.boundary(authority=GovernanceAuthority(h.db))).status == B.EXECUTED
    assert StaticAuthority(False).authorize_entry(h.plan) == (False, "STATIC")


# ============================== SECTION 26 ==============================
def _report(**statuses):
    gates = ["A_INTEGRITY", "B_NET_EXPECTANCY", "C_COST_STRESS", "D_HOLDOUT", "E_CONCENTRATION", "F_CALIBRATION",
             "G_EVIDENCE_COUNT", "H_FORWARD_DEMO", "I_OPERATIONAL"]
    return {"gates": [{"gate": g, "status": statuses.get(g, "PASS"),
                       "reason_codes": ["NO_REAL_MARKET_DATA"] if statuses.get(g) == "BLOCKED" else []} for g in gates]}


def test_success_standard_layers_are_independent_and_cannot_be_averaged():
    from app.trading_intelligence.governance.success_standard import LAYERS, verify_success_standard

    none = verify_success_standard()
    assert set(none["layers"]) == set(LAYERS) and all(v["code"] == "PASS" for v in none["layers"].values())
    assert none["overall"] == "PENDING_EVIDENCE" and none["layers"]["SETUPS"]["status"] == "PASS"
    allpass = verify_success_standard(_report())
    assert allpass["overall"] == "PASS"
    one_fail = verify_success_standard(_report(A_INTEGRITY="FAIL"))
    assert one_fail["overall"] == "FAIL" and one_fail["layers"]["MARKET_STATE"]["status"] == "FAIL"
    assert one_fail["layers"]["SETUPS"]["status"] == "PASS"   # other layers keep their own verdict
    blocked = verify_success_standard(_report(A_INTEGRITY="BLOCKED"))
    assert blocked["overall"] == "BLOCKED_DATA"


def test_precision_tiers_are_certification_derived_and_default_flat():
    from app.trading_intelligence.governance.success_standard import PrecisionTierPolicy, precision_tier

    opp = {"credible_interval_low": 0.62, "credible_interval_high": 0.72, "ess": 200, "conservative_edge_r": 0.3,
           "ood_score": 0.1, "net_R_at_2x_cost": 0.2}
    on = PrecisionTierPolicy(enabled=True)
    assert precision_tier(opp, certification_status="CERTIFIED", calibration_status="CALIBRATED") == "NO_TIER"
    assert precision_tier(opp, certification_status="INSUFFICIENT_EVIDENCE", calibration_status="CALIBRATED",
                          policy=on) == "NO_TIER"
    assert precision_tier(opp, certification_status="CERTIFIED", calibration_status="RESEARCH_ONLY",
                          policy=on) == "NO_TIER"
    assert precision_tier(opp, certification_status="CERTIFIED", calibration_status="CALIBRATED",
                          policy=on) == "PRECISION_A"
    weak = dict(opp, ess=5)
    assert precision_tier(weak, certification_status="CERTIFIED", calibration_status="CALIBRATED",
                          policy=on) == "NO_TIER"
    assert precision_tier({}, certification_status="CERTIFIED", calibration_status="CALIBRATED",
                          policy=on) == "NO_TIER"


def test_runtime_safety_flags_remain_off():
    from app.trading_intelligence.execution.config import CATIExecutionConfig
    from app.trading_intelligence.ml.config import CATIMLConfig
    from app.trading_intelligence.research.certification.policy import canonical_certification_policy

    # AUTO_ACTIVE_IF_ELIGIBLE: the env switches are operator overrides; AUTHORITY is governance-derived.
    # In the test database no phase transition exists (M0), so nothing is authorized at runtime.
    from app.activation import cati as act

    assert CATIExecutionConfig().active_execution_enabled is False  # explicit construction stays OFF
    assert CATIMLConfig().ml_enabled is False
    for env in ("DEMO", "LIVE"):
        d = act.active_execution(None, environment=env)
        assert d.state.value == "BLOCKED" and not d.active
    assert "RUNTIME_AUTHORITY_SWITCH_NOT_IMPLEMENTED" in act.active_execution(None).reasons
    assert canonical_certification_policy().policy_name == "RESEARCH_DEFAULT_V1"
