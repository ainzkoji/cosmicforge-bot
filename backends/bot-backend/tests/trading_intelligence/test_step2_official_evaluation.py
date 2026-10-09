"""Section H, Step 2.4 / 2.5: the official evaluation path, end to end on a SYNTHETIC market.

Registered mandate -> frozen dataset -> evaluation -> run log -> result; the unauthorized and the authorized
holdout; crash, repeat and concurrency behaviour. The synthetic market certifies nothing: it only drives the
real entry point with controlled input.
"""
from __future__ import annotations

import json
import threading

import pytest
from _step2 import N, PERIODS, SRC, SyntheticDataset, authorize, go, make_setup, new_register

from app.market_data.daily_dataset import day_index, day_text
from app.trading_intelligence.families.daily_trend import spec
from app.trading_intelligence.families.daily_trend.registration import register_mandate_004
from app.trading_intelligence.research.evaluator import metrics as M, official as O
from app.trading_intelligence.research.governance import holdout as H
from app.trading_intelligence.research.governance.history import import_historical_hypotheses
from app.trading_intelligence.research.governance.register import (
    GOVERNANCE_DECISION, RUN_COMPLETED, RUN_FAILED, RegisterError, RegisterTampered, ResearchRegister, repository_root,
)

assert N == 26


@pytest.fixture()
def setup(tmp_path):
    return make_setup(tmp_path)


# ============================== PRECONDITIONS ==============================
def test_an_evaluation_needs_a_registered_mandate_dataset_and_cost_model(tmp_path):
    reg = ResearchRegister(tmp_path / "r.jsonl")
    import_historical_hypotheses(reg)
    ds = SyntheticDataset()
    kw = dict(register=reg, dataset=ds, artifacts_root=tmp_path / "runs", periods=PERIODS, source=SRC, bootstrap_repetitions=200)
    with pytest.raises(RegisterError, match="not registered"):                # unregistered mandate
        O.run_evaluation(O.DEVELOPMENT, **kw)
    register_mandate_004(reg, research_code_commit="c", registered_by="t", approved_by="o", approved_at="d",
                         authorization_reference="r")
    with pytest.raises(O.EvaluationRefused, match="not frozen"):               # dataset not in the register
        O.run_evaluation(O.DEVELOPMENT, **kw)
    O.freeze_dataset_in_register(reg, ds)
    with pytest.raises(O.EvaluationRefused, match="cost model"):
        O.run_evaluation(O.DEVELOPMENT, **kw)
    O.register_cost_model(reg)
    with pytest.raises(O.EvaluationRefused, match="not committed"):            # uncommitted evaluation source
        O.run_evaluation(O.DEVELOPMENT, **{**kw, "source": {**SRC, "evaluation_source_dirty": True, "dirty_paths": ["M x.py"]}})
    with pytest.raises(O.EvaluationRefused, match="not committed"):
        O.run_evaluation(O.DEVELOPMENT, **{**kw, "source": {**SRC, "evaluation_source_dirty": None}})
    assert reg.runs() == []                                                    # nothing was evaluated, nothing was logged
    with pytest.raises(O.EvaluationRefused, match="different manifest"):
        other = SyntheticDataset()
        other.manifest = {**ds.manifest, "manifest_hash": "0" * 64}
        O.freeze_dataset_in_register(reg, other)
    with pytest.raises(RegisterTampered):                                      # a register that lost its history
        O.run_evaluation(O.DEVELOPMENT, **{**kw, "anchor": (18, "0" * 64)})


def test_the_frozen_cost_model_is_the_mandates_and_shares_catis_definition():
    from app.replay.cost_model import CostModel

    cm = O.frozen_cost_model()
    assert cm["model"]["taker_fee"] == 0.0005 and cm["model"]["slippage"] == 0.0005 and cm["model"]["spread"] == 0.0
    assert cm["stress_multiple"] == 2.0 and cm["basis"].startswith("ASSUMED") and cm["missing_funding_rate_per_8h"] == 0.0003
    assert set(cm["model"]) == set(CostModel().to_dict()) and cm["cost_model_hash"] == O.frozen_cost_model()["cost_model_hash"]


# ============================== DEVELOPMENT ==============================
def test_a_development_run_is_logged_complete_and_never_loads_a_held_back_row(setup):
    reg, ds, runs = setup
    res = go(setup)
    assert ds.loads == ["2020-12-31"]                                          # the only load, capped at the development end
    assert res["run_type"] == O.DEVELOPMENT and res["verdict"] in ("PENDING_HOLDOUT", "FAILED_CERTIFICATION")
    assert res["mandate_pass_rule"] in (M.PENDING, M.FAIL) and res["holdout_opened_by_this_run"] is False
    assert res["statistical_gate"] == "BLOCKED_PENDING_APPROVAL"               # no approved thresholds
    assert res["hypotheses_in_register"] == 18 and res["statistical_gate_detail"]["multiple_testing"]["tests"] == 18
    assert res["specification_sha256"] == spec.SPECIFICATION_SHA256 and len(res["strategy_hash"]) == 64
    assert [a["number"] for a in res["amendments"]] == [1, 2]
    assert res["integrity"]["causality_verified"] and res["integrity"]["ledgers_close"]
    assert all(c["identical_up_to_cut"] for c in res["causality_audit"]["checks"]) and len(res["causality_audit"]["checks"]) == 6
    block = res["blocks"]["development"]
    assert len(block["scenarios"]) == 27 and res["primary_scenario"] in block["scenarios"]
    primary = block["scenarios"][res["primary_scenario"]]
    assert primary["trades"]["trades_closed"] > 0 and "equal_weight" in block["benchmarks"]
    (run,) = reg.runs(mandate_id=spec.MANDATE_ID)
    assert run["state"] == RUN_COMPLETED and run["end"]["result_hash"] == res["result_hash"] and run["end"]["status"] == "VALID"
    for field in ("strategy_hash", "dataset_hash", "code_commit", "parameter_hash", "cost_model_hash", "researcher", "period"):
        assert run[field], field
    out = runs / res["run_id"]
    assert json.loads((out / "result.json").read_text(encoding="utf-8"))["result_hash"] == res["result_hash"]
    import hashlib

    for name, digest in run["end"]["artifact_hashes"].items():
        assert hashlib.sha256((out / name).read_bytes()).hexdigest() == digest
    assert reg.holdout(res["holdout_id"])["status"] == "HOLDOUT_RESERVED"       # reserved, untouched
    assert reg.hypothesis(spec.HYPOTHESIS_ID)["status"] == "REGISTERED"         # a run records no verdict by itself


def test_reported_numbers_reconcile_with_the_raw_artifacts(setup):
    _reg, _ds, runs = setup
    res = go(setup)
    out = runs / res["run_id"]
    primary = res["blocks"]["development"]["scenarios"][res["primary_scenario"]]
    lines = (out / "development_equity.csv").read_text(encoding="utf-8").splitlines()
    header, last = lines[0].split(","), lines[-1].split(",")
    assert float(last[header.index(res["primary_scenario"])]) == pytest.approx(primary["final_equity"], rel=1e-12)
    assert primary["net_return"] == pytest.approx(primary["final_equity"] / 10_000 - 1, rel=1e-12)
    trades = (out / "development_primary_trades.csv").read_text(encoding="utf-8").splitlines()
    cols = trades[0].split(",")
    net = sum(float(r.split(",")[cols.index("net_pnl")]) for r in trades[1:])
    assert net == pytest.approx(primary["final_equity"] - 10_000, abs=1e-6)     # every unit of P&L is in the trade list
    c = primary["costs"]
    assert primary["final_equity"] == pytest.approx(10_000 + c["price_pnl"] - c["fees"] - c["slippage"] - c["funding"], abs=1e-6)
    daily = (out / "development_primary_daily.csv").read_text(encoding="utf-8").splitlines()
    assert len(daily) - 1 == 366 == primary["observations"]
    ex = res["blocks"]["development"]["scenarios"]["EXECUTABLE|balanced|base|brakes_on|no_overlay"]
    assert ex["risk_per_trade_effective"] == 0.004 and ex["risk_per_trade_approved"] == 0.005   # never shown as 0.50%
    assert primary["risk_per_trade_effective"] == 0.005 and primary["policy"] == "MANDATE"
    targets = (out / "development_primary_daily_targets.csv").read_text(encoding="utf-8").splitlines()
    assert targets[0].startswith("day,symbol,target_fraction_of_equity") and len(targets) > 10   # the Step 3 shadow reference


def test_the_same_inputs_give_the_same_run_and_the_stored_result(setup):
    reg, ds, _ = setup
    a = go(setup)
    records = len(reg.records())
    b = go(setup)
    assert b == a and len(reg.records()) == records and ds.loads == ["2020-12-31"]   # no second run, no second load
    assert len(reg.runs()) == 1
    fresh = (reg.path.parent / "fresh")
    fresh.mkdir()
    reg3 = new_register(fresh / "register.jsonl")
    ds3 = SyntheticDataset()
    O.freeze_dataset_in_register(reg3, ds3)
    O.register_cost_model(reg3)
    c = go((reg3, ds3, fresh / "runs"))
    assert c["run_id"] == a["run_id"] and c["result_hash"] == a["result_hash"]        # an independent reproduction


def test_a_failed_run_stays_in_the_log_and_a_rerun_names_it(setup, monkeypatch):
    reg, _ds, _ = setup

    def boom(*a, **k):
        raise RuntimeError("funding ledger defect")

    monkeypatch.setattr(O, "causality_audit", boom)
    with pytest.raises(RuntimeError, match="funding ledger"):
        go(setup)
    (failed,) = reg.runs()
    assert failed["state"] == RUN_FAILED and "funding ledger defect" in failed["end"]["error"]
    monkeypatch.undo()
    with pytest.raises(O.EvaluationRefused, match="names it as parent"):       # the same run cannot quietly start again
        go(setup)
    with pytest.raises(RegisterError, match="why"):
        go(setup, parent_run_id=failed["run_id"])
    ok = go(setup, parent_run_id=failed["run_id"], reason_for_rerun="defect fixed; regression test added")
    states = [(r["run_id"], r["state"]) for r in reg.runs()]
    assert states == [(failed["run_id"], RUN_FAILED), (ok["run_id"], RUN_COMPLETED)] and ok["parent_run_id"] == failed["run_id"]
    assert reg.hypothesis_count() == 18                                         # a bug-fix rerun is not a new hypothesis


def test_a_changed_rule_or_specification_stops_every_run(setup, monkeypatch):
    reg, _ds, _ = setup
    monkeypatch.setattr(spec, "RULE_ARTIFACT", {**dict(spec.RULE_ARTIFACT), "rules_version": "tuned"})
    with pytest.raises(RegisterError, match="rule set"):
        go(setup)
    assert reg.runs() == []


# ============================== HOLDOUT ==============================
def test_the_holdout_is_refused_without_a_recorded_authorization_and_leaves_evidence_of_the_attempt(setup):
    reg, ds, _ = setup
    dev = go(setup)
    with pytest.raises(H.HoldoutNotAuthorized, match="recorded owner authorization"):
        go(setup, O.HOLDOUT)
    assert ds.loads == ["2020-12-31"]                                           # no held-back row was read
    runs = reg.runs()
    assert [r["run_type"] for r in runs] == [O.DEVELOPMENT, O.HOLDOUT] and runs[1]["state"] == RUN_FAILED
    assert "HoldoutNotAuthorized" in runs[1]["end"]["error"]
    assert reg.holdout(dev["holdout_id"])["status"] == "HOLDOUT_RESERVED"       # still untouched
    with pytest.raises(O.HoldoutEmbargo):
        ds.panel(periods=PERIODS, last_day="2021-01-01")                        # the loader itself refuses
    with pytest.raises(O.HoldoutEmbargo):
        ds.panel(periods=PERIODS, last_day="2021-06-30", access=object())
    forged = H.HoldoutAccess("hold_x", "run_x", "2021-01-01", "2021-03-31", "h")
    with pytest.raises(O.HoldoutEmbargo):
        ds.panel(periods=PERIODS, last_day="2021-06-30", access=forged)         # a token for another window
    assert ds.loads == ["2020-12-31"]


def test_an_authorized_holdout_opens_once_gives_a_verdict_and_cannot_be_reopened(setup):
    reg, ds, runs = setup
    dev = go(setup)
    hid = authorize(setup, dev)
    res = go(setup, O.HOLDOUT)
    assert ds.loads == ["2020-12-31", "2021-06-30"] and res["holdout_opened_by_this_run"] is True
    assert set(res["blocks"]) == {"full", "holdout"} and res["blocks"]["holdout"]["first_day"] == "2021-01-01"
    assert all(c["status"] in (M.PASS, M.FAIL) for c in res["pass_rule"]["criteria"].values())   # nothing is pending now
    # this market covers 18 months: only 2020 is a complete calendar year of the rule, and that is a FAIL
    assert res["pass_rule"]["criteria"]["2_positive_calendar_years"]["observed"]["years_not_covered"] == 5
    assert res["mandate_pass_rule"] == M.FAIL and res["verdict"] == "FAILED_CERTIFICATION"
    assert res["statistical_gate_detail"]["sample"] == "HELD_BACK_PERIOD" and res["promotion"]["trading_authorized"] is False
    h = reg.holdout(hid)
    assert h["status"] == "HOLDOUT_BURNED" and h["burned"]["result_hash"] == res["result_hash"] and h["opened"]["run_id"] == res["run_id"]
    records = len(reg.records())
    again = go(setup, O.HOLDOUT)                                                 # the same request: the stored result
    assert again == res and len(reg.records()) == records and ds.loads == ["2020-12-31", "2021-06-30"]
    with pytest.raises(H.HoldoutAlreadyBurned):                                  # a "different" attempt cannot reopen it
        go(setup, O.HOLDOUT, parent_run_id=res["run_id"], reason_for_rerun="try again with another idea")
    assert ds.loads == ["2020-12-31", "2021-06-30"]
    assert (runs / res["run_id"] / "holdout_primary_trades.csv").exists()
    # a verdict is recorded on the hypothesis only as a separate, explicit act
    assert reg.hypothesis(spec.HYPOTHESIS_ID)["status"] == "REGISTERED"
    with pytest.raises(O.EvaluationRefused, match="not a final outcome"):
        O.record_verdict(reg, dev["run_id"], recorded_by="tester")               # a pending development run decides nothing
    O.record_verdict(reg, res["run_id"], recorded_by="tester")
    failed = reg.hypothesis(spec.HYPOTHESIS_ID)
    assert failed["status"] == "FAILED" and failed["status_history"][-1]["run_id"] == res["run_id"]
    assert reg.hypothesis_count() == 18                                          # it stays counted for the next candidate


def test_a_passing_mandate_is_still_not_certified_without_approved_statistical_thresholds(tmp_path):
    """Six and three quarter years of a strongly trending synthetic market, the REAL periods and the REAL rule:
    the frozen pass rule passes, and the verdict is BLOCKED (no approved thresholds), then INSUFFICIENT (approved
    thresholds, underpowered sample) -- never a certification, and never a governance or trading change."""
    reg = new_register(tmp_path / "register.jsonl")
    ds = SyntheticDataset(seed=3, drift=0.004, vol=0.012, end="2026-09-30")
    O.freeze_dataset_in_register(reg, ds)
    O.register_cost_model(reg)
    kw = dict(register=reg, dataset=ds, artifacts_root=tmp_path / "runs", researcher="tester", source=SRC, bootstrap_repetitions=200)
    dev = O.run_evaluation(O.DEVELOPMENT, **kw)
    assert dev["verdict"] == "PENDING_HOLDOUT" and ds.loads == ["2024-12-31"]
    assert dev["pass_rule"]["criteria"]["2_positive_calendar_years"]["status"] == M.PASS      # 2020-2024 already decide it
    hid = H.holdout_id_for(spec.MANDATE_ID, ds.dataset_hash, spec.HOLDOUT_START, spec.HOLDOUT_END)
    ready = H.pre_holdout_readiness(facts={k: True for k, _ in H.REQUIREMENTS}, mandate_id=spec.MANDATE_ID,
                                    specification_sha256=dev["specification_sha256"], dataset_hash=ds.dataset_hash,
                                    source_fingerprint=SRC["fingerprint"], development_run_id=dev["run_id"])
    H.record_owner_holdout_authorization(reg, holdout_id=hid, readiness=ready, authorized_by="Owner Name",
                                         authorization_reference="test", reason="final")
    res = O.run_evaluation(O.HOLDOUT, **kw)
    assert res["mandate_pass_rule"] == M.PASS and all(c["status"] == M.PASS for c in res["pass_rule"]["criteria"].values())
    assert res["statistical_gate"] == "BLOCKED_PENDING_APPROVAL" and res["verdict"] == "BLOCKED_PENDING_AUTHORIZATION"
    assert res["promotion"] == {"research_pass": False, "trading_authorized": False, "governance_phase_changed": False,
                                "live_trading": "DISABLED"}
    assert res["statistical_gate_full_period"]["sample"] == "FULL_PERIOD_INFORMATION_ONLY"
    with pytest.raises(O.EvaluationRefused, match="not a final outcome"):
        O.record_verdict(reg, res["run_id"], recorded_by="tester")
    from app.trading_intelligence.research.governance import admission as A

    cert = {**res, "holdout_result_hash": res["result_hash"]}
    assert A.evaluate_rule_based_admission(reg, mandate_id=spec.MANDATE_ID, certification=cert)["status"] == A.NOT_ELIGIBLE


def test_an_authorization_for_other_code_does_not_open_the_holdout(setup):
    reg, ds, _ = setup
    dev = go(setup)
    hid = authorize(setup, dev)
    with pytest.raises(H.HoldoutNotAuthorized, match="source_fingerprint"):
        go(setup, O.HOLDOUT, source={**SRC, "fingerprint": "e" * 64})            # the evaluator changed after approval
    assert reg.holdout(hid)["status"] == "HOLDOUT_AUTHORIZED" and ds.loads == ["2020-12-31"]


def test_a_crash_after_opening_keeps_the_access_on_record(setup, monkeypatch):
    reg, ds, _ = setup
    dev = go(setup)
    hid = authorize(setup, dev)
    monkeypatch.setattr(O, "_period_block", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("process died")))
    with pytest.raises(RuntimeError, match="process died"):
        go(setup, O.HOLDOUT)
    monkeypatch.undo()
    h = reg.holdout(hid)
    assert h["status"] == "HOLDOUT_OPENED" and h["burned"] is None              # accessed, no result: on record
    assert [r["state"] for r in reg.runs()][-1] == RUN_FAILED
    with pytest.raises(O.EvaluationRefused, match="names it as parent"):
        go(setup, O.HOLDOUT)
    with pytest.raises(H.HoldoutAccessIncomplete):                              # a rerun cannot silently reopen it
        go(setup, O.HOLDOUT, parent_run_id=reg.runs()[-1]["run_id"], reason_for_rerun="crash")
    assert len(reg.of_type("HOLDOUT_OPENED")) == 1


def test_two_workers_cannot_both_run_the_authorized_holdout(setup):
    reg, _ds, _ = setup
    dev = go(setup)
    hid = authorize(setup, dev)
    out, barrier = [], threading.Barrier(2)

    def worker(tag):
        barrier.wait()
        try:
            r = O.run_evaluation(O.HOLDOUT, register=ResearchRegister(reg.path), dataset=SyntheticDataset(),
                                 artifacts_root=reg.path.parent / "runs", periods=PERIODS, researcher=tag, source=SRC,
                                 bootstrap_repetitions=200, parent_run_id=dev["run_id"], reason_for_rerun=f"worker {tag}")
            out.append(("DONE", r["run_id"]))
        except Exception as exc:
            out.append((type(exc).__name__, None))

    threads = [threading.Thread(target=worker, args=(t,)) for t in ("a", "b")]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sorted(k for k, _ in out).count("DONE") == 1                          # exactly one evaluated the holdout
    assert len(reg.of_type("HOLDOUT_OPENED")) == 1 and len(reg.of_type("HOLDOUT_BURNED")) == 1
    assert reg.holdout(hid)["status"] == "HOLDOUT_BURNED" and reg.verify()["records"] == len(reg.records())


def test_approved_statistical_thresholds_are_read_from_the_register(setup):
    reg, _ds, _ = setup
    assert O.statistical_policy(reg).approved is False
    reg.append(GOVERNANCE_DECISION, {"decision_id": O.STATISTICAL_POLICY_DECISION, "state": "APPROVED",
                                     "decided_by": "Owner Name", "authorization_reference": "ref", "reason": "standard",
                                     "familywise_alpha": 0.05, "required_power": 0.8, "target_annual_sharpe": 1.0})
    pol = O.statistical_policy(reg)
    assert pol.approved and (pol.familywise_alpha, pol.required_power, pol.target_annual_sharpe) == (0.05, 0.8, 1.0)
    res = go(setup)
    assert res["statistical_gate"] in ("INSUFFICIENT_STATISTICAL_POWER", "FAIL", "PASS")   # no longer blocked
    assert res["statistical_gate"] == "INSUFFICIENT_STATISTICAL_POWER"                     # one year cannot carry it
    assert res["verdict"] in ("PENDING_HOLDOUT", "FAILED_CERTIFICATION")


def test_the_real_dataset_documents_are_consistent_when_present():
    root = repository_root()
    manifest = root / "docs" / "research" / "datasets" / "binance_usdm_daily_v1.dataset.json"
    if not manifest.exists():
        pytest.skip("the dataset manifest is not committed yet")
    from app.market_data.daily_dataset import verify_manifest

    m = json.loads(manifest.read_text(encoding="utf-8"))
    cov = json.loads((root / "docs" / "research" / "coverage" / "binance_usdm_daily_v1.coverage.json").read_text(encoding="utf-8"))
    assert verify_manifest(m) == m["manifest_hash"] == cov["manifest_hash"] and m["status"] == "FROZEN"
    assert m["coverage_start"] == "2020-01-01" and m["coverage_end"] == "2026-09-30" and m["symbols_with_bars"] == len(cov["symbols"])
    assert m["contracts_that_ended_before_coverage_end"]["count"] > 100          # delisted contracts are in the data
    assert m["quality_totals"]["failed_downloads"] == 0 and m["quality_totals"]["invalid_candles"] == 0
    frozen = [r["body"] for r in ResearchRegister().of_type("DATASET_FROZEN")]
    assert any(f["manifest_hash"] == m["manifest_hash"] and f["dataset_hash"] == m["dataset_hash"] for f in frozen)
    assert day_text(day_index(m["coverage_end"])) == m["coverage_end"]
