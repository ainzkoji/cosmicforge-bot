"""Section H, Step 2.1 / 2.2 / 2.5 governance: the research register, mandate pinning, the portfolio-level
statistical gate, the rule-based admission route and holdout access.

Synthetic return series exercise the gate only; none of them is evidence about any strategy.
"""
from __future__ import annotations

import json
import random
import re
import threading
from pathlib import Path

import pytest

from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.research.governance import admission as A, holdout as H, statistics as S
from app.trading_intelligence.research.governance.history import (
    HISTORICAL_ANCHOR, HISTORICAL_SOURCE_RELATIVE_PATH, import_historical_hypotheses,
)
from app.trading_intelligence.research.governance.mandates import (
    MandateHashMismatch, amend_mandate, register_hypothesis_and_mandate, source_fingerprint, verify_mandate,
)
from app.trading_intelligence.research.governance.register import (
    GOVERNANCE_DECISION, HYPOTHESIS, HYPOTHESIS_FIELDS, HYPOTHESIS_STATUS, RUN_COMPLETED, RUN_FAILED, RUN_FIELDS,
    RUN_STARTED, UNKNOWN, RegisterError, RegisterTampered, ResearchRegister, canonical_text_sha256,
    default_register_path, repository_root,
)

REPO = repository_root()
SOURCE = REPO / HISTORICAL_SOURCE_RELATIVE_PATH
RULES = {"lookbacks": [10, 20], "version": "t"}


def _hyp(n, **over):
    body = {f: UNKNOWN for f in HYPOTHESIS_FIELDS}
    body.update(hypothesis_id=f"H-{n:03d}", hypothesis_number=n, family_id="F", mandate_id="M", status="REGISTERED",
                failure_reasons=[], evidence_locations=[], strategy_description="d")
    body.update(over)
    return body


def _reg(tmp_path, name="register.jsonl") -> ResearchRegister:
    return ResearchRegister(tmp_path / name)


def _mandate(tmp_path, reg, n=1, mandate_id="M-T", text="frozen rules\n"):
    spec = tmp_path / "SPEC.md"
    spec.write_bytes(text.encode())
    hyp = _hyp(n, mandate_id=mandate_id, specification_hash=canonical_text_sha256(spec))
    return register_hypothesis_and_mandate(
        reg, hypothesis=hyp, mandate_id=mandate_id, specification_path="SPEC.md", specification_version="v1",
        rule_artifact=RULES, research_code_commit="c0ffee", registered_by="tester", approved_by="owner",
        approved_at="2026-10-09", authorization_reference="test", repo_root=tmp_path)


def _run(reg, run_id="run_1", mandate_id="M-T", **over):
    body = {f: "x" for f in RUN_FIELDS}
    body.update(run_id=run_id, mandate_id=mandate_id, parent_run_id=None, reason_for_rerun=None, authorization=None,
                run_type="DEVELOPMENT")
    body.update(over)
    return reg.append(RUN_STARTED, body)


# ============================== REGISTER: CHAIN AND TAMPERING ==============================
def test_records_are_hash_chained_and_reload_identically(tmp_path):
    reg = _reg(tmp_path)
    for n in (1, 2, 3):
        reg.append(HYPOTHESIS, _hyp(n))
    recs = reg.records()
    assert [r["seq"] for r in recs] == [1, 2, 3] and recs[1]["prev_hash"] == recs[0]["record_hash"]
    assert reg.verify()["head_hash"] == recs[-1]["record_hash"] and reg.hypothesis_count() == 3
    assert b"\r" not in reg.path.read_bytes()                      # LF on every platform


def test_an_edited_record_is_detected(tmp_path):
    reg = _reg(tmp_path)
    for n in (1, 2, 3):
        reg.append(HYPOTHESIS, _hyp(n))
    lines = reg.path.read_text(encoding="utf-8").splitlines()
    lines[1] = lines[1].replace('"REGISTERED"', '"CERTIFIED"')
    reg.path.write_bytes(("\n".join(lines) + "\n").encode())
    with pytest.raises(RegisterTampered, match="altered"):
        reg.records()


def test_a_removed_or_reordered_record_is_detected(tmp_path):
    reg = _reg(tmp_path)
    for n in (1, 2, 3):
        reg.append(HYPOTHESIS, _hyp(n))
    lines = reg.path.read_text(encoding="utf-8").splitlines()
    reg.path.write_bytes(("\n".join([lines[0], lines[2]]) + "\n").encode())           # a failed attempt deleted
    with pytest.raises(RegisterTampered, match="chain"):
        reg.records()
    reg.path.write_bytes(("\n".join([lines[1], lines[0], lines[2]]) + "\n").encode())
    with pytest.raises(RegisterTampered):
        reg.records()


def test_truncating_the_tail_is_caught_by_the_anchor_and_a_partial_write_is_refused(tmp_path):
    reg = _reg(tmp_path)
    recs = [reg.append(HYPOTHESIS, _hyp(n)) for n in (1, 2, 3)]
    anchor = (3, recs[2]["record_hash"])
    lines = reg.path.read_text(encoding="utf-8").splitlines()
    reg.path.write_bytes(("\n".join(lines[:2]) + "\n").encode())
    assert len(reg.records()) == 2                                  # a shortened chain is still a valid chain ...
    with pytest.raises(RegisterTampered, match="anchored"):          # ... which is exactly why readers pin an anchor
        reg.records(anchor=anchor)
    reg.path.write_bytes(("\n".join(lines) + "\n" + lines[0][:40]).encode())
    with pytest.raises(RegisterTampered, match="partial"):
        reg.records()
    reg.path.unlink()
    with pytest.raises(RegisterTampered, match="missing"):
        reg.records(anchor=anchor)


def test_a_held_lock_blocks_the_writer_and_is_never_broken_automatically(tmp_path):
    reg = _reg(tmp_path)
    reg.LOCK_TIMEOUT_S = 0.2
    reg.path.with_name(reg.path.name + ".lock").write_text("stale")
    with pytest.raises(RegisterError, match="locked"):
        reg.append(HYPOTHESIS, _hyp(1))
    assert reg.path.with_name(reg.path.name + ".lock").exists() and not reg.path.exists()


# ============================== HYPOTHESIS ACCOUNTING ==============================
def test_numbers_are_sequential_never_reused_and_identities_are_unique(tmp_path):
    reg = _reg(tmp_path)
    reg.append(HYPOTHESIS, _hyp(1))
    with pytest.raises(RegisterError, match="next number is 2"):
        reg.append(HYPOTHESIS, _hyp(1, hypothesis_id="H-other"))    # reuse of a number
    with pytest.raises(RegisterError, match="next number is 2"):
        reg.append(HYPOTHESIS, _hyp(3))                             # a skipped number
    with pytest.raises(RegisterError, match="already registered"):
        reg.append(HYPOTHESIS, _hyp(2, hypothesis_id="H-001"))      # the same identity twice
    with pytest.raises(RegisterError, match="UNKNOWN"):
        reg.append(HYPOTHESIS, {k: v for k, v in _hyp(2).items() if k != "data_manifest_hash"})
    assert reg.hypothesis_count() == 1


def test_a_failed_hypothesis_stays_counted_and_its_history_is_kept(tmp_path):
    reg = _reg(tmp_path)
    reg.append(HYPOTHESIS, _hyp(1))
    reg.append(HYPOTHESIS, _hyp(2))
    reg.append(HYPOTHESIS_STATUS, {"hypothesis_id": "H-001", "status": "FAILED", "reason": "gate",
                                   "failure_reasons": ["PASS_RULE_3"]})
    with pytest.raises(RegisterError, match="not registered"):
        reg.append(HYPOTHESIS_STATUS, {"hypothesis_id": "H-404", "status": "FAILED", "reason": "x"})
    h1 = reg.hypothesis("H-001")
    assert h1["status"] == "FAILED" and h1["failure_reasons"] == ["PASS_RULE_3"]
    assert [s["status"] for s in h1["status_history"]] == ["REGISTERED", "FAILED"]
    assert reg.hypothesis_count() == 2                              # failure never shrinks the count


def test_the_historical_source_reconstructs_seventeen_with_explicit_unknowns(tmp_path):
    reg = _reg(tmp_path)
    out = import_historical_hypotheses(reg, SOURCE)
    hyps = reg.hypotheses()
    assert out["imported_now"] == 17 and [h["hypothesis_number"] for h in hyps] == list(range(1, 18))
    assert all(set(HYPOTHESIS_FIELDS) <= set(h) for h in hyps)
    assert all(h["evidence_locations"] and h["status"] in ("FAILED", "REJECTED_PRE_HOLDOUT") for h in hyps)
    families = [h["family_id"] for h in hyps]
    assert families.count("FORECAST_OUTCOME_LIBRARY") == 4 and families.count("CATI_ALPHA_CONFIRMED_STRUCTURE_V1") == 6
    assert families.count("CATI_ALPHA_CAUSAL_DIVERSIFIED_V2") == 4 and families.count("CATI_NEXT_EDGE_DISCOVERY") == 3
    assert UNKNOWN in json.dumps(hyps[0]) and UNKNOWN in json.dumps(hyps[4])       # uncertainty is stated
    imp = reg.of_type("HISTORICAL_IMPORT")[0]["body"]
    assert "needs the project owner's confirmation" in imp["counting_rule"]["status"]
    assert all((REPO / p).exists() for h in hyps for p in h["evidence_locations"])   # evidence is still there


def test_the_import_is_idempotent_resumable_and_never_replaced(tmp_path):
    reg = _reg(tmp_path)
    doc = json.loads(SOURCE.read_text(encoding="utf-8"))
    partial = tmp_path / "source.json"
    partial.write_text(json.dumps(doc), encoding="utf-8")
    import_historical_hypotheses(reg, partial)
    head = reg.head_hash()
    assert import_historical_hypotheses(reg, partial)["imported_now"] == 0 and reg.head_hash() == head
    # an interrupted import (only the first five reached the file) resumes with the same source
    reg2 = _reg(tmp_path, "interrupted.jsonl")
    lines = reg.path.read_text(encoding="utf-8").splitlines()[:6]
    reg2.path.write_bytes(("\n".join(lines) + "\n").encode())
    assert import_historical_hypotheses(reg2, partial)["imported_now"] == 12 and reg2.hypothesis_count() == 17
    doc["hypotheses"][0]["status"] = "PASSED"
    partial.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(RegisterError, match="different source"):
        import_historical_hypotheses(reg, partial)


def test_the_committed_register_holds_the_seventeen_and_cannot_lose_them():
    reg = ResearchRegister()
    assert default_register_path() == reg.path and reg.path.exists() and HISTORICAL_ANCHOR is not None
    recs = reg.records(anchor=tuple(HISTORICAL_ANCHOR))             # the seventeen can be neither edited nor dropped
    assert recs[0]["type"] == "HISTORICAL_IMPORT" and recs[0]["body"]["hypotheses_imported"] == 17
    assert recs[0]["body"]["source_sha256"] == canonical_text_sha256(SOURCE)
    hyps = reg.hypotheses()
    assert [h["hypothesis_number"] for h in hyps[:17]] == list(range(1, 18))
    assert all(h["origin"] == "HISTORICAL_IMPORT" for h in hyps[:17]) and reg.hypothesis_count() >= 17


# ============================== MANDATE PINNING ==============================
def test_registration_pins_the_specification_and_the_rule_artifact(tmp_path):
    reg = _reg(tmp_path)
    _mandate(tmp_path, reg)
    pin = verify_mandate(reg, "M-T", rule_artifact=RULES, repo_root=tmp_path)
    assert pin["specification_sha256"] == canonical_text_sha256(tmp_path / "SPEC.md") and len(pin["strategy_hash"]) == 64
    (tmp_path / "SPEC.md").write_bytes(b"frozen rules\r\n")           # a Windows checkout is the same specification
    assert verify_mandate(reg, "M-T", rule_artifact=RULES, repo_root=tmp_path)["strategy_hash"] == pin["strategy_hash"]
    (tmp_path / "SPEC.md").write_bytes(b"frozen rules, slightly better\n")
    with pytest.raises(MandateHashMismatch, match="edited"):
        verify_mandate(reg, "M-T", rule_artifact=RULES, repo_root=tmp_path)
    (tmp_path / "SPEC.md").write_bytes(b"frozen rules\n")
    with pytest.raises(MandateHashMismatch, match="rule set"):
        verify_mandate(reg, "M-T", rule_artifact={**RULES, "lookbacks": [10, 25]}, repo_root=tmp_path)
    with pytest.raises(RegisterError, match="not registered"):
        verify_mandate(reg, "M-OTHER", rule_artifact=RULES, repo_root=tmp_path)


def test_duplicate_mandates_and_missing_hashes_are_refused(tmp_path):
    reg = _reg(tmp_path)
    _mandate(tmp_path, reg)
    with pytest.raises(RegisterError):                               # same mandate id for a second hypothesis
        _mandate(tmp_path, reg, n=2, mandate_id="M-T")
    assert reg.hypothesis_count() == 2                               # the attempt itself stays counted
    body = {**{k: "x" for k in ("mandate_id", "hypothesis_id", "family_id", "specification_version",
                                "specification_path", "rule_artifact_hash", "research_code_commit",
                                "registry_version", "registered_at", "registered_by", "approved_at", "approved_by",
                                "authorization_reference")}, "mandate_id": "M-NOHASH", "hypothesis_id": "H-002"}
    with pytest.raises(RegisterError, match="specification_sha256"):
        reg.append("MANDATE_REGISTERED", body)
    with pytest.raises(RegisterError, match="SHA-256"):
        reg.append("MANDATE_REGISTERED", {**body, "specification_sha256": "abc"})
    hyp = _hyp(3, specification_hash="0" * 64)
    with pytest.raises(MandateHashMismatch):                         # the record must carry the real hash
        register_hypothesis_and_mandate(reg, hypothesis=hyp, mandate_id="M-3", specification_path="SPEC.md",
                                        specification_version="v1", rule_artifact=RULES, research_code_commit="c",
                                        registered_by="t", approved_by="o", approved_at="d",
                                        authorization_reference="r", repo_root=tmp_path)


def test_amendments_are_numbered_kept_and_cannot_smuggle_a_rule_change(tmp_path):
    reg = _reg(tmp_path)
    _mandate(tmp_path, reg)
    (tmp_path / "A1.md").write_text("how ATR is averaged\n", encoding="utf-8")
    amend_mandate(reg, mandate_id="M-T", kind="IMPLEMENTATION_INTERPRETATION", summary="s", artifact_path="A1.md",
                  changes_strategy_rules=False, recorded_by="t", repo_root=tmp_path)
    pin = verify_mandate(reg, "M-T", rule_artifact=RULES, repo_root=tmp_path)
    assert [a["number"] for a in pin["amendments"]] == [1]
    original = reg.mandate("M-T")
    assert original["specification_sha256"] == canonical_text_sha256(tmp_path / "SPEC.md")   # never replaced
    (tmp_path / "A1.md").write_text("how ATR is averaged, revised quietly\n", encoding="utf-8")
    with pytest.raises(MandateHashMismatch, match="amendment 1"):
        verify_mandate(reg, "M-T", rule_artifact=RULES, repo_root=tmp_path)
    (tmp_path / "A1.md").write_text("how ATR is averaged\n", encoding="utf-8")
    (tmp_path / "A2.md").write_text("use 25 days instead\n", encoding="utf-8")
    amend_mandate(reg, mandate_id="M-T", kind="PARAMETER_CHANGE", summary="s", artifact_path="A2.md",
                  changes_strategy_rules=True, recorded_by="t", repo_root=tmp_path)
    with pytest.raises(MandateHashMismatch, match="new hypothesis"):
        verify_mandate(reg, "M-T", rule_artifact=RULES, repo_root=tmp_path)
    assert len(reg.mandate("M-T")["amendments"]) == 2                 # both stay on record
    with pytest.raises(RegisterError, match="next is 3"):
        reg.append("MANDATE_AMENDMENT", {"mandate_id": "M-T", "amendment_number": 5, "kind": "k", "summary": "s",
                                         "artifact_path": "A1.md", "artifact_sha256": "h",
                                         "changes_strategy_rules": False, "recorded_by": "t"})


def test_the_source_fingerprint_ignores_line_endings_and_sees_every_edit(tmp_path):
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "a.py").write_bytes(b"x = 1\n")
    fp = source_fingerprint(["pkg"], repo_root=tmp_path)
    (pkg / "a.py").write_bytes(b"x = 1\r\n")
    assert source_fingerprint(["pkg"], repo_root=tmp_path) == fp
    (pkg / "a.py").write_bytes(b"x = 2\n")
    assert source_fingerprint(["pkg"], repo_root=tmp_path) != fp


# ============================== RUN LOG ==============================
def test_unregistered_evaluations_are_refused_and_failed_runs_stay_in_the_log(tmp_path):
    reg = _reg(tmp_path)
    with pytest.raises(RegisterError, match="not registered"):
        _run(reg, mandate_id="M-NOT-THERE")
    _mandate(tmp_path, reg)
    _run(reg, "run_1")
    with pytest.raises(RegisterError, match="already started"):
        _run(reg, "run_1")
    reg.append(RUN_FAILED, {"run_id": "run_1", "status": "ERROR", "completed_at": "t", "error": "funding sign"})
    with pytest.raises(RegisterError, match="already ended"):
        reg.append(RUN_COMPLETED, {"run_id": "run_1", "status": "OK", "completed_at": "t"})       # no replacement
    with pytest.raises(RegisterError, match="why"):
        _run(reg, "run_2", parent_run_id="run_1")
    with pytest.raises(RegisterError, match="exists"):
        _run(reg, "run_2", parent_run_id="run_0", reason_for_rerun="x")
    _run(reg, "run_2", parent_run_id="run_1", reason_for_rerun="funding sign defect fixed (regression test added)")
    with pytest.raises(RegisterError, match="never started"):
        reg.append(RUN_COMPLETED, {"run_id": "run_9", "status": "OK", "completed_at": "t"})
    runs = reg.runs(mandate_id="M-T")
    assert [(r["run_id"], r["state"]) for r in runs] == [("run_1", RUN_FAILED), ("run_2", "STARTED_NOT_FINISHED")]
    assert runs[0]["end"]["error"] == "funding sign" and reg.hypothesis_count() == 1   # a rerun is not a new hypothesis


# ============================== STATISTICAL GATE ==============================
def _series(n, mean, sd=0.01, seed=5, rho=0.0):
    rng, out, prev = random.Random(seed), [], 0.0
    for _ in range(n):
        prev = rho * prev + rng.gauss(0.0, sd)
        out.append(mean + prev)
    return out


def _policy(**over):
    base = dict(name="TEST", familywise_alpha=0.05, required_power=0.8, target_annual_sharpe=1.0, approved_by="owner",
                authorization_reference="test", bootstrap_repetitions=600, min_observations=250)
    base.update(over)
    return S.StatisticalGatePolicy(**base)


def test_without_approved_thresholds_the_gate_is_blocked_never_passed():
    out = S.evaluate_portfolio_gate(_series(3000, 0.004), hypotheses_in_register=18)
    assert out["status"] == S.BLOCKED_PENDING_APPROVAL
    assert set(out["unresolved_governance_requirements"]) == {"familywise_alpha", "required_power",
                                                              "target_annual_sharpe", "recorded_owner_approval"}
    assert out["annualized_sharpe"] > 3 and out["power"]["thresholds_are"].startswith("ILLUSTRATIVE")
    half = S.StatisticalGatePolicy(familywise_alpha=0.05, required_power=0.8, target_annual_sharpe=1.0)
    assert S.evaluate_portfolio_gate(_series(3000, 0.004), hypotheses_in_register=18,
                                     policy=half)["status"] == S.BLOCKED_PENDING_APPROVAL   # numbers without approval


def test_strong_long_evidence_passes_and_no_edge_fails():
    strong = S.evaluate_portfolio_gate(_series(8000, 0.002), hypotheses_in_register=18, policy=_policy())
    assert strong["status"] == S.PASS and strong["multiple_testing"]["p_value_adjusted"] <= 0.05
    flat = S.evaluate_portfolio_gate(_series(8000, 0.0), hypotheses_in_register=18, policy=_policy())
    assert flat["status"] == S.FAIL and flat["reason_codes"]
    losing = S.evaluate_portfolio_gate(_series(8000, -0.001), hypotheses_in_register=18, policy=_policy())
    assert losing["status"] == S.FAIL and "MEAN_NET_RETURN_NOT_POSITIVE" in losing["reason_codes"]


def test_multiple_testing_uses_the_whole_register():
    vals = _series(3000, 0.0006)
    one = S.evaluate_portfolio_gate(vals, hypotheses_in_register=1, policy=_policy())
    many = S.evaluate_portfolio_gate(vals, hypotheses_in_register=18, policy=_policy())
    p = one["bootstrap"]["p_value_one_sided"]
    assert many["bootstrap"]["p_value_one_sided"] == p
    assert many["multiple_testing"]["p_value_adjusted"] == pytest.approx(min(1.0, p * 18))
    assert many["multiple_testing"]["tests"] == 18 and set(many["multiple_testing"]["sensitivity_p_adjusted"]) == {"18", "19", "24"}
    assert many["power"]["per_test_alpha"] == pytest.approx(0.05 / 18)
    assert many["power"]["minimum_detectable_annual_sharpe"] > one["power"]["minimum_detectable_annual_sharpe"]
    assert S.evaluate_portfolio_gate(vals, hypotheses_in_register=0, policy=_policy())["status"] == S.INVALID_EVIDENCE


def test_an_underpowered_sample_never_passes_however_good_it_looks():
    vals = _series(640, 0.004)                                       # 1.75 years of an excellent-looking series
    out = S.evaluate_portfolio_gate(vals, hypotheses_in_register=18, policy=_policy())
    assert out["annualized_sharpe"] > 3 and out["multiple_testing"]["p_value_adjusted"] <= 0.05
    assert out["status"] == S.INSUFFICIENT_STATISTICAL_POWER
    assert out["power"]["achieved_power_at_target"] < 0.8
    assert out["power"]["minimum_detectable_annual_sharpe"] == pytest.approx(
        S.minimum_detectable_sharpe(effective_years=out["effective_years"], alpha=0.05 / 18, power=0.8))
    # manual reference: z(1 - 0.05/18) + z(0.8) = 2.773 + 0.842; over 1.7534 years -> about 2.73
    assert S.minimum_detectable_sharpe(effective_years=640 / 365, alpha=0.05 / 18, power=0.8) == pytest.approx(2.73, abs=0.01)
    assert S.power_at(0.5, effective_years=1.75, alpha=0.05) == pytest.approx(0.163, abs=0.005)


def test_zero_trades_missing_periods_short_samples_and_invalid_evidence_are_classified():
    pol = _policy()
    assert S.evaluate_portfolio_gate([0.0] * 400, hypotheses_in_register=18, policy=pol)["reason_codes"] == ["ZERO_VARIANCE"]
    zero = S.evaluate_portfolio_gate(_series(400, 0.001), hypotheses_in_register=18, policy=pol, trade_count=0)
    assert zero["status"] == S.INSUFFICIENT_DATA and zero["reason_codes"] == ["ZERO_TRADES"]
    gap = S.evaluate_portfolio_gate(_series(400, 0.001), hypotheses_in_register=18, policy=pol, expected_observations=420)
    assert gap["status"] == S.INSUFFICIENT_DATA and gap["reason_codes"] == ["MISSING_PERIODS"]
    short = S.evaluate_portfolio_gate(_series(100, 0.001), hypotheses_in_register=18, policy=pol)
    assert short["status"] == S.INSUFFICIENT_DATA and short["reason_codes"] == ["TOO_FEW_OBSERVATIONS"]
    bad = S.evaluate_portfolio_gate(_series(400, 0.001), hypotheses_in_register=18, policy=pol, evidence_valid=False,
                                    invalid_reasons=["LOOKAHEAD_DETECTED"])
    assert bad["status"] == S.INVALID_EVIDENCE and bad["reason_codes"] == ["LOOKAHEAD_DETECTED"]
    nan = S.evaluate_portfolio_gate([0.01, float("nan")] * 200, hypotheses_in_register=18, policy=pol)
    assert nan["status"] == S.INVALID_EVIDENCE


def test_serial_dependence_shrinks_the_effective_sample():
    iid = S.effective_sample_size(_series(4000, 0.0), 30)
    sticky = S.effective_sample_size(_series(4000, 0.0, rho=0.6), 30)
    assert iid["effective_n"] > 0.8 * 4000 and sticky["effective_n"] < 0.4 * 4000 and sticky["rho_1"] > 0.5
    anti = S.effective_sample_size([(-1) ** i * 0.01 for i in range(1000)], 30)
    assert anti["effective_n"] == 1000                              # negative dependence is never credited


def test_the_gate_is_repeatable_and_every_outcome_is_a_known_one():
    vals = _series(1500, 0.0008)
    a = S.evaluate_portfolio_gate(vals, hypotheses_in_register=18, policy=_policy())
    b = S.evaluate_portfolio_gate(list(vals), hypotheses_in_register=18, policy=_policy())
    assert stable_hash(a) == stable_hash(b) and a["status"] in S.OUTCOMES
    assert _policy().policy_hash != _policy(familywise_alpha=0.01).policy_hash


def test_the_section22_trial_count_can_no_longer_fall_below_the_register(tmp_path):
    from app.trading_intelligence.research.certification.pipeline import registered_trial_floor

    reg = _reg(tmp_path)
    assert registered_trial_floor(1, reg) == 1                       # no register file: the local count stands
    for n in range(1, 18):
        reg.append(HYPOTHESIS, _hyp(n))
    assert registered_trial_floor(1, reg) == 18 and registered_trial_floor(40, reg) == 40
    assert registered_trial_floor(1) >= 18                           # the committed register: at least 17 + this run
    reg.path.write_bytes(reg.path.read_bytes().replace(b"REGISTERED", b"CERTIFIED!", 1))
    with pytest.raises(RegisterTampered):                            # never a silently smaller count
        registered_trial_floor(1, reg)


# ============================== HOLDOUT ==============================
def _ready(reg, tmp_path, **over):
    facts = {k: True for k, _ in H.REQUIREMENTS}
    facts.update(over.pop("facts", {}))
    kw = dict(facts=facts, mandate_id="M-T", specification_sha256=canonical_text_sha256(tmp_path / "SPEC.md"),
              dataset_hash="d" * 64, source_fingerprint="s" * 64, development_run_id="run_dev")
    kw.update(over)
    return H.pre_holdout_readiness(**kw)


def _open_kw(tmp_path, **over):
    kw = dict(run_id="run_h", specification_sha256=canonical_text_sha256(tmp_path / "SPEC.md"), dataset_hash="d" * 64,
              source_fingerprint="s" * 64, code_commit="c0ffee")
    kw.update(over)
    return kw


def _authorized(tmp_path):
    reg = _reg(tmp_path)
    _mandate(tmp_path, reg)
    hid = H.reserve_holdout(reg, mandate_id="M-T", dataset_hash="d" * 64, start="2025-01-01", end="2026-09-30")
    H.record_owner_holdout_authorization(reg, holdout_id=hid, readiness=_ready(reg, tmp_path), authorized_by="Owner Name",
                                         authorization_reference="message of 2026-10-10", reason="final evaluation")
    return reg, hid


def test_readiness_needs_every_fact_proved_and_is_deterministic(tmp_path):
    reg = _reg(tmp_path)
    _mandate(tmp_path, reg)
    ready = _ready(reg, tmp_path)
    assert ready["status"] == H.READY and ready["readiness_hash"] == _ready(reg, tmp_path)["readiness_hash"]
    missing = _ready(reg, tmp_path, facts={"causality_audit_green": False, "development_run_valid": None})
    assert missing["status"] == H.NOT_READY
    assert missing["reason_codes"] == ["CAUSALITY_AUDIT_NOT_GREEN", "DEVELOPMENT_RUN_NOT_VALID"]
    assert H.pre_holdout_readiness(facts={}, mandate_id="M-T", specification_sha256=None, dataset_hash=None,
                                   source_fingerprint=None, development_run_id=None)["status"] == H.NOT_READY


def test_authorization_needs_readiness_a_named_approver_and_an_untouched_holdout(tmp_path):
    reg = _reg(tmp_path)
    _mandate(tmp_path, reg)
    hid = H.reserve_holdout(reg, mandate_id="M-T", dataset_hash="d" * 64, start="2025-01-01", end="2026-09-30")
    assert H.reserve_holdout(reg, mandate_id="M-T", dataset_hash="d" * 64, start="2025-01-01", end="2026-09-30") == hid
    kw = dict(holdout_id=hid, authorized_by="Owner Name", authorization_reference="ref", reason="final")
    with pytest.raises(H.HoldoutNotAuthorized, match="NOT_READY"):
        H.record_owner_holdout_authorization(reg, readiness=_ready(reg, tmp_path, facts={"dataset_frozen": False}), **kw)
    for blank in ("authorized_by", "authorization_reference", "reason"):
        with pytest.raises(H.HoldoutNotAuthorized, match="names the approver"):
            H.record_owner_holdout_authorization(reg, readiness=_ready(reg, tmp_path), **{**kw, blank: " "})
    with pytest.raises(H.HoldoutNotAuthorized, match="another mandate or dataset"):
        H.record_owner_holdout_authorization(reg, readiness=_ready(reg, tmp_path, dataset_hash="e" * 64), **kw)
    with pytest.raises(H.HoldoutNotAuthorized, match="recorded owner authorization"):
        H.open_holdout_once(reg, holdout_id=hid, **_open_kw(tmp_path))                 # reserved is not authorized
    H.record_owner_holdout_authorization(reg, readiness=_ready(reg, tmp_path), **kw)
    with pytest.raises(H.HoldoutNotAuthorized, match="untouched"):
        H.record_owner_holdout_authorization(reg, readiness=_ready(reg, tmp_path), **kw)   # once
    assert reg.holdout(hid)["authorization"]["authorized_by"] == "Owner Name"


def test_the_authorization_does_not_cover_a_changed_evaluation(tmp_path):
    reg, hid = _authorized(tmp_path)
    for field in ("specification_sha256", "dataset_hash", "source_fingerprint"):
        with pytest.raises(H.HoldoutNotAuthorized, match=field):
            H.open_holdout_once(reg, holdout_id=hid, **_open_kw(tmp_path, **{field: "x" * 64}))
    assert reg.holdout(hid)["status"] == "HOLDOUT_AUTHORIZED"        # a refusal opens nothing


def test_the_holdout_opens_once_is_idempotent_and_survives_a_crash(tmp_path):
    reg, hid = _authorized(tmp_path)
    access = H.open_holdout_once(reg, holdout_id=hid, **_open_kw(tmp_path))
    assert (access.start, access.end) == ("2025-01-01", "2026-09-30")
    assert ResearchRegister(reg.path).holdout(hid)["status"] == "HOLDOUT_OPENED"      # durable before any evaluation
    # the process "crashes" here: no result. The access is still on record and is not silently repeated.
    with pytest.raises(H.HoldoutAccessIncomplete, match="on record"):
        H.open_holdout_once(reg, holdout_id=hid, **_open_kw(tmp_path))
    with pytest.raises(H.HoldoutAccessIncomplete):
        H.open_holdout_once(reg, holdout_id=hid, **_open_kw(tmp_path, run_id="run_other"))
    reg.append(GOVERNANCE_DECISION, {"decision_id": f"{H.EXCEPTIONAL_RERUN}:{hid}", "state": "APPROVED",
                                     "decided_by": "Owner Name", "authorization_reference": "ref",
                                     "reason": "process crashed before the result was stored"})
    again = H.open_holdout_once(reg, holdout_id=hid, **_open_kw(tmp_path))
    assert again.opened_record_hash == access.opened_record_hash       # the ORIGINAL opening, not a new one
    assert len(reg.of_type("HOLDOUT_OPENED")) == 1
    H.burn_holdout(reg, again, result_hash="r" * 64, verdict="FAIL")
    with pytest.raises(H.HoldoutAlreadyBurned) as same:
        H.open_holdout_once(reg, holdout_id=hid, **_open_kw(tmp_path))
    assert same.value.result["result_hash"] == "r" * 64 and same.value.result["verdict"] == "FAIL"   # stored result
    with pytest.raises(H.HoldoutAlreadyBurned) as other:
        H.open_holdout_once(reg, holdout_id=hid, **_open_kw(tmp_path, run_id="run_retuned"))
    assert other.value.result is None
    with pytest.raises(RegisterError, match="once each"):
        H.burn_holdout(reg, again, result_hash="x" * 64, verdict="PASS")              # the verdict cannot be replaced


def test_concurrent_workers_cannot_both_open_the_holdout(tmp_path):
    reg, hid = _authorized(tmp_path)
    results, barrier = [], threading.Barrier(6)

    def worker(i):
        barrier.wait()
        try:
            H.open_holdout_once(ResearchRegister(reg.path), holdout_id=hid, **_open_kw(tmp_path, run_id=f"run_{i}"))
            results.append("OPENED")
        except RegisterError as exc:
            results.append(type(exc).__name__)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert results.count("OPENED") == 1 and len(results) == 6
    assert len(reg.of_type("HOLDOUT_OPENED")) == 1 and reg.verify()["records"] == len(reg.records())


def test_nothing_in_the_codebase_authorizes_or_opens_this_holdout_on_its_own():
    app = Path(__file__).resolve().parents[2] / "app"
    gov = "trading_intelligence/research/governance/"

    def users(pattern):
        return sorted(p.relative_to(app).as_posix() for p in app.rglob("*.py")
                      if re.search(pattern, p.read_text(encoding="utf-8", errors="ignore")))

    assert users(r"record_owner_holdout_authorization\(") == [gov + "__main__.py", gov + "holdout.py"]   # CLI + definition
    assert set(users(r"open_holdout_once\(")) <= {gov + "holdout.py", "trading_intelligence/research/evaluator/official.py"}


# ============================== RULE-BASED ADMISSION ROUTE ==============================
def _certified(tmp_path, **over):
    reg, hid = _authorized(tmp_path)
    access = H.open_holdout_once(reg, holdout_id=hid, **_open_kw(tmp_path))
    H.burn_holdout(reg, access, result_hash="r" * 64, verdict="PASS")
    cert = {"mandate_id": "M-T", "specification_sha256": canonical_text_sha256(tmp_path / "SPEC.md"),
            "holdout_id": hid, "holdout_result_hash": "r" * 64, "mandate_pass_rule": "PASS", "statistical_gate": "PASS",
            "verdict": "PASS", "integrity": {k: True for k, _ in A.EVIDENCE_REQUIREMENTS}}
    cert.update(over)
    return reg, cert


def test_the_rule_based_route_is_refused_until_an_owner_decision_is_recorded(tmp_path):
    reg, cert = _certified(tmp_path)
    out = A.evaluate_rule_based_admission(reg, mandate_id="M-T", certification=cert)
    assert out["status"] == A.BLOCKED and out["route_approved"] is False and out["promotes"] is False
    assert out["reason_codes"] == ["RULE_BASED_ROUTE_NOT_APPROVED_BY_RECORDED_OWNER_DECISION"]
    assert not A.route_approved(ResearchRegister())                   # and it is NOT approved in the real register


def test_the_approved_route_admits_only_complete_passing_evidence(tmp_path):
    reg, cert = _certified(tmp_path)
    reg.append(GOVERNANCE_DECISION, {"decision_id": A.ROUTE_ID, "state": "APPROVED", "decided_by": "Owner Name",
                                     "authorization_reference": "Section L decision", "reason": "rule-based families"})
    ok = A.evaluate_rule_based_admission(reg, mandate_id="M-T", certification=cert)
    assert ok["status"] == A.ELIGIBLE and ok["promotes"] is False and ok["reason_codes"] == []
    cases = {
        "verdict": ("FAIL", "CERTIFICATION_VERDICT_NOT_PASS"),
        "mandate_pass_rule": ("FAIL", "FROZEN_MANDATE_PASS_RULE_NOT_PASSED"),
        "statistical_gate": ("INSUFFICIENT_STATISTICAL_POWER", "STATISTICAL_GATE_INSUFFICIENT_STATISTICAL_POWER"),
        "holdout_result_hash": ("x" * 64, "CERTIFICATION_DOES_NOT_MATCH_THE_STORED_HOLDOUT_RESULT"),
        "specification_sha256": ("0" * 64, "CERTIFICATION_IS_FOR_ANOTHER_MANDATE_OR_SPECIFICATION"),
        "holdout_id": ("hold_none", "HOLDOUT_NOT_EVALUATED_UNDER_RECORDED_AUTHORIZATION"),
    }
    for field, (value, reason) in cases.items():
        bad = A.evaluate_rule_based_admission(reg, mandate_id="M-T", certification={**cert, field: value})
        assert bad["status"] == A.NOT_ELIGIBLE and reason in bad["reason_codes"], field
    for key, reason in A.EVIDENCE_REQUIREMENTS:                       # costs, causality, data, risk: none is waived
        weak = {**cert, "integrity": {**cert["integrity"], key: False}}
        assert reason in A.evaluate_rule_based_admission(reg, mandate_id="M-T", certification=weak)["reason_codes"]
    assert A.evaluate_rule_based_admission(reg, mandate_id="M-X", certification=cert)["status"] == A.NOT_ELIGIBLE
    reg.append(GOVERNANCE_DECISION, {"decision_id": A.ROUTE_ID, "state": "REVOKED", "decided_by": "Owner Name",
                                     "authorization_reference": "ref", "reason": "withdrawn"})
    assert A.evaluate_rule_based_admission(reg, mandate_id="M-T", certification=cert)["status"] == A.BLOCKED


def test_eligibility_changes_no_phase_and_the_m6_evidence_rule_is_untouched():
    import inspect

    from app.trading_intelligence.governance.phases import M5_REPLAY_GATES, PHASE_BY_ID

    assert "F_CALIBRATION" in M5_REPLAY_GATES and "replay_gates_passed" in PHASE_BY_ID["M6"].entry_requirements
    src = inspect.getsource(A)
    assert "PromotionGovernance(" not in src and ".transition(" not in src and "set_kill_switch" not in src
