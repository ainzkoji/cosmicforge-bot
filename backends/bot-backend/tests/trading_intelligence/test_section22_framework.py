"""Section 22 framework: contracts, policy, manifest, splits, registries,
statistics, gates, scope, security, tenancy, forward-demo tracker.

Synthetic data is used ONLY to exercise the framework; it can never certify.
"""
from __future__ import annotations

import dataclasses
import json
import sqlite3
import time

import pytest
from _cert import BAR_15M, REAL_SOURCES, SYNTHETIC_SOURCES, T0, ohlcv, series

from app.trading_intelligence.research.certification import gates as G, stats
from app.trading_intelligence.research.certification.contracts import (
    CertificationRun, CertificationScope, CertificationStageResult, Gate, GateResult, GateStatus,
    OverallCertificationStatus as O,
)
from app.trading_intelligence.research.certification.dataset import build_certification_manifest
from app.trading_intelligence.research.certification.evaluation import stage_status
from app.trading_intelligence.research.certification.forward_demo import (
    ForwardDemoCertificationTracker, operational_defects,
)
from app.trading_intelligence.research.certification.freeze import build_policy_freeze
from app.trading_intelligence.research.certification.policy import (
    NOT_CONFIGURED, CertificationPolicy, PolicyValue, default_certification_policy,
)
from app.trading_intelligence.research.certification.registry import (
    CertificationRunStore, ExperimentRecord, ExperimentRegistry, HoldoutBurned, HoldoutRegistry, RegistryError,
    SqliteResearchStore,
)
from app.trading_intelligence.research.certification.splits import (
    FinalHoldoutViolation, assert_chronological, guard_holdout, plan_chronology, purge_for_window,
)

DAY = 86_400_000


def _scope(**kw):
    base = dict(asset_class="CRYPTO", venue="BINANCE_USDM", environment="REAL", symbol_universe=("BTCUSDT", "ETHUSDT"),
                timeframe="15m", setup_families=("TREND_PULLBACK_V2",), side_scope=("LONG", "SHORT"),
                data_start=T0, data_end=T0 + 10 * DAY)
    return CertificationScope(**{**base, **kw})


def _store(tmp_path, name="r.db"):
    return SqliteResearchStore(str(tmp_path / name))


def _configured(**over):
    vals = dict(min_probability_expectancy_positive=0.9, catastrophic_floor_R_at_2x=-0.05, max_holdout_drawdown_R=10.0,
                max_single_segment_positive_share=0.6, min_accepted_evidence_count=30, min_forward_demo_executed_count=20,
                max_pbo=0.5, parameter_neighbor_min_positive_share=0.5)
    vals.update(over)
    return default_certification_policy().configure(**vals)


# ============================== CONTRACTS ==============================
def test_run_identity_is_deterministic_and_excludes_timestamps():
    res = CertificationStageResult(stage="FAST", status="PASS", metrics={"a": 1})
    kw = dict(stage="FAST", scope=_scope(), dataset_manifest_id="m", dataset_hash="d", policy_freeze_hash="f",
              certification_policy_hash="p", version_hashes={"x": "1"}, result=res)
    a = CertificationRun(**kw, started_at=1, completed_at=2)
    b = CertificationRun(**kw, started_at=999, completed_at=1000)
    assert a.certification_run_id == b.certification_run_id and a.artifact_hash == b.artifact_hash
    c = CertificationRun(**{**kw, "dataset_hash": "other"})
    assert c.certification_run_id != a.certification_run_id


def test_stage_results_are_immutable():
    res = CertificationStageResult(stage="FAST", status="PASS", metrics={"counts": {"n": 1}})
    with pytest.raises(TypeError):
        res.metrics["counts"]["n"] = 2
    with pytest.raises(dataclasses.FrozenInstanceError):
        res.status = "FAIL"


def test_scope_is_explicit_and_isolated():
    s = _scope()
    assert s.covers(asset_class="CRYPTO", venue="BINANCE_USDM", timeframe="15m", symbol="BTCUSDT")
    assert not s.covers(asset_class="FOREX", venue="BINANCE_USDM", timeframe="15m")        # crypto never certifies FX
    assert not s.covers(asset_class="CRYPTO", venue="IBKR", timeframe="15m")                # venue isolation
    assert not s.covers(asset_class="CRYPTO", venue="BINANCE_USDM", timeframe="1h")         # timeframe isolation
    assert not s.covers(asset_class="CRYPTO", venue="BINANCE_USDM", timeframe="15m", symbol="SOLUSDT")
    assert not s.covers(asset_class="CRYPTO", venue="BINANCE_USDM", timeframe="15m", environment="DEMO")


# ============================== POLICY ==============================
def test_policy_hash_is_stable_and_every_threshold_has_provenance():
    p = default_certification_policy()
    assert p.policy_hash == CertificationPolicy().policy_hash
    for name, value in p.thresholds().items():
        assert value.provenance in ("SOURCE_SPEC", "EXISTING_REPO", "RESEARCH_DEFAULT", "NOT_CONFIGURED"), name
    # the source gives these numbers explicitly
    assert p.min_forward_demo_days.value == 30 and p.cost_stress_multipliers.value == (1.0, 1.5, 2.0)
    assert p.max_lookahead_violations.value == 0 and p.min_net_expectancy_R.value == 0.0


def test_unspecified_thresholds_fail_closed_never_invented():
    nc = set(default_certification_policy().not_configured())
    assert {"min_probability_expectancy_positive", "catastrophic_floor_R_at_2x", "max_holdout_drawdown_R",
            "max_single_segment_positive_share", "min_accepted_evidence_count",
            "min_forward_demo_executed_count"} <= nc
    with pytest.raises(ValueError):
        PolicyValue(0.5, NOT_CONFIGURED)


def test_thresholds_may_be_tightened_never_loosened():
    p = _configured()
    assert p.policy_hash != default_certification_policy().policy_hash
    tighter = p.configure(min_accepted_evidence_count=60, min_probability_expectancy_positive=0.95)
    assert tighter.min_accepted_evidence_count.value == 60
    with pytest.raises(ValueError):
        p.configure(min_accepted_evidence_count=10)          # lowering the support floor
    with pytest.raises(ValueError):
        p.configure(max_lookahead_violations=1)
    with pytest.raises(ValueError):
        p.configure(min_forward_demo_days=20)


# ============================== DATASET ==============================
def _manifest(data, sources=SYNTHETIC_SOURCES, created_at=None):
    return build_certification_manifest(data, data_sources=sources, source_provider="binance", dataset_identity="t",
                                        asset_class="CRYPTO", venue="BINANCE_USDM", environment="REAL",
                                        warmup_bars=250, label_horizon_bars=48, cost_model_version="1.0.0:x",
                                        created_at=created_at)


def test_manifest_is_deterministic_and_wall_clock_free():
    a, b = _manifest(series(n=400), created_at="2026-01-01"), _manifest(series(n=400), created_at="2027-05-05")
    assert a.manifest_hash == b.manifest_hash and a.dataset_hash == b.dataset_hash and a.manifest_id == b.manifest_id
    c = _manifest(series(n=401))
    assert c.manifest_hash != a.manifest_hash
    assert a.intrabar_ambiguity_policy == "SL_FIRST_CONSERVATIVE" and a.cati_versions["CERTIFICATION_SCHEMA_VERSION"]


def test_manifest_reports_missing_bars():
    data = series(n=400)
    rows = data["BTCUSDT"]["15m"]
    data["BTCUSDT"]["15m"] = rows[:100] + rows[110:]
    m = _manifest(data)
    assert m.missing_intervals["BTCUSDT:15m"]["missing_bars"] == 10
    assert m.missing_intervals["BTCUSDT:15m"]["window_count"] == 1


def test_no_synthetic_source_is_certifiable():
    assert not _manifest(series(n=300)).certifiable_source
    assert not _manifest(series(n=300), sources=("binance", "synthetic_test")).certifiable_source  # mixed
    assert not _manifest(series(n=300), sources=("",)).certifiable_source                           # unknown
    assert _manifest(series(n=300), sources=REAL_SOURCES).certifiable_source


# ============================== SPLITS ==============================
def _plan(**kw):
    base = dict(decision_start_ms=T0, decision_end_ms=T0 + 100 * DAY - 1, bar_ms=BAR_15M, label_horizon_bars=48,
                holdout_fraction=0.10, folds=5)
    return plan_chronology(**{**base, **kw})


def test_chronological_folds_and_reserved_holdout_tail():
    p = _plan()
    starts = [f.start_ms for f in p.folds]
    assert starts == sorted(starts) and all(a.end_ms < b.start_ms for a, b in zip(p.folds, p.folds[1:]))
    assert p.holdout.end_ms == T0 + 100 * DAY - 1 and p.holdout.days == pytest.approx(10, abs=0.1)
    # a purge gap of at least the label horizon sits between the research span and the holdout
    assert p.holdout.start_ms - p.folds[-1].end_ms > 48 * BAR_15M
    assert p.to_dict()["method"] == "CHRONOLOGICAL_WALK_FORWARD_NO_SHUFFLE"


def test_shuffled_inputs_are_refused():
    rows = ohlcv(1, 50)
    assert_chronological(rows, key=lambda r: r[0])
    shuffled = rows[:10] + rows[20:30] + rows[10:20]
    with pytest.raises(ValueError):
        assert_chronological(shuffled, key=lambda r: r[0])


def test_purge_removes_a_label_that_uses_bars_across_the_boundary():
    horizon = 48 * BAR_15M
    boundary = T0 + 10 * DAY
    items = [boundary - horizon - BAR_15M, boundary - horizon // 2, boundary - BAR_15M]
    kept = purge_for_window(items, decision_time=lambda t: t, horizon_ms=horizon, cutoff_ms=boundary)
    assert kept == (items[0],)  # the two whose future-label window reaches past the boundary are gone


def test_embargo_and_htf_widen_the_quiet_gap():
    base, emb = _plan(), _plan(embargo_bars=16)
    assert emb.embargo_ms == 16 * BAR_15M
    assert emb.library_cutoff_for(emb.folds[1]) == emb.folds[1].start_ms - emb.embargo_ms
    htf = _plan(label_horizon_bars=2, htf_ms=4 * 3_600_000)
    assert htf.purge_ms == 4 * 3_600_000 and base.purge_ms == 48 * BAR_15M


def test_holdout_guard_raises_before_the_holdout_is_opened():
    p = _plan()
    guard_holdout(p, p.folds[1].start_ms, purpose="tuning")
    with pytest.raises(FinalHoldoutViolation):
        guard_holdout(p, p.holdout.start_ms + BAR_15M, purpose="threshold tuning")


# ============================== EXPERIMENT REGISTRY ==============================
def _exp(status="FAILED", **kw):
    base = dict(dataset_hash="d", policy_hash="p", hypothesis="h", changed_parameters={}, reason_for_change="baseline",
                stages_run=("FAST",), results={"mean": -0.1}, status=status)
    return ExperimentRecord(**{**base, **kw})


def test_failed_experiments_are_kept_and_history_is_append_only(tmp_path):
    reg = ExperimentRegistry(_store(tmp_path))
    failed = reg.record(_exp("FAILED"))
    child = reg.record(_exp("SUCCESS", parent_experiment_id=failed, changed_parameters={"edge": 0.05},
                            reason_for_change="neighbor", results={"mean": 0.2}))
    assert child != failed
    ids = {e["experiment_id"]: e["status"] for e in reg.all()}
    assert ids == {failed: "FAILED", child: "SUCCESS"}   # the failure did not disappear
    with pytest.raises(RegistryError):
        reg.record(_exp("SUCCESS", results={"mean": 9.9}))  # rewriting the failed result
    with sqlite3.connect(str(tmp_path / "r.db")) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM cati_experiment_registry")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE cati_experiment_registry SET status='SUCCESS'")
    assert reg.trial_count(dataset_hash="d") == 2


def test_changed_parameter_is_a_new_experiment():
    with pytest.raises(ValueError):
        _exp(parent_experiment_id="exp_x")  # a child must say what changed
    a = _exp(parent_experiment_id="exp_x", changed_parameters={"k": 1})
    b = _exp(parent_experiment_id="exp_x", changed_parameters={"k": 2})
    assert a.experiment_id != b.experiment_id


# ============================== POLICY FREEZE / HOLDOUT ==============================
def test_policy_freeze_is_deterministic_and_changes_with_any_policy():
    from app.trading_intelligence.contracts.veto import VetoPolicy

    p = default_certification_policy()
    a = build_policy_freeze(certification_policy=p, source_commit="c", source_tree_dirty=False)
    b = build_policy_freeze(certification_policy=p, source_commit="c", source_tree_dirty=False)
    assert a.freeze_hash == b.freeze_hash and a.certifiable
    v = build_policy_freeze(certification_policy=p, source_commit="c", source_tree_dirty=False,
                            veto_policy=dataclasses.replace(VetoPolicy(), transition_uncertainty_watch=0.5))
    assert v.freeze_hash != a.freeze_hash and "veto_policy" in a.diff(v)
    assert not build_policy_freeze(certification_policy=p, source_commit="c", source_tree_dirty=True).certifiable
    assert build_policy_freeze(certification_policy=_configured(), source_commit="c",
                               source_tree_dirty=False).freeze_hash != a.freeze_hash


def test_holdout_opens_once_and_is_then_burned(tmp_path):
    reg = HoldoutRegistry(_store(tmp_path))
    hid = reg.reserve(dataset_hash="d", start_ms=1, end_ms=2)
    assert reg.reserve(dataset_hash="d", start_ms=1, end_ms=2) == hid and reg.status(hid)["status"] == "RESERVED"
    reg.open(hid, policy_freeze_hash="F1")
    with pytest.raises(RegistryError):
        reg.burn(hid, policy_freeze_hash="F2", result_hash="r")   # another policy may not claim it
    reg.burn(hid, policy_freeze_hash="F1", result_hash="r")
    for freeze in ("F1", "F2"):                                     # neither re-use nor a changed policy
        with pytest.raises(HoldoutBurned):
            reg.open(hid, policy_freeze_hash=freeze)
    st = reg.status(hid)
    assert st["status"] == "BURNED" and st["policy_freeze_hash"] == "F1" and st["first_opened_at"]
    with pytest.raises(RegistryError):
        reg.open(reg.reserve(dataset_hash="d", start_ms=5, end_ms=6)[::-1], policy_freeze_hash="F1")  # unreserved


# ============================== STATISTICS ==============================
def _st(vals, **kw):
    return stats.expectancy_summary(vals, reps=400, confidence=0.95, seed=7, **kw)


def test_expectancy_is_after_cost_with_a_deterministic_interval():
    gross = [1.0, -1.0, 1.5, -1.0, 2.0, -1.0] * 10
    net = [g - 0.2 for g in gross]
    a, b = _st(net), _st(net)
    assert a == b and a["mean_R"] == pytest.approx(sum(gross) / len(gross) - 0.2)
    assert a["ci_low"] <= a["mean_R"] <= a["ci_high"] and 0.0 <= a["p_expectancy_positive"] <= 1.0
    assert a["block_length"] == stats.block_length(len(net))


def test_high_win_rate_with_negative_expectancy_is_negative():
    vals = ([0.1] * 9 + [-10.0]) * 10  # losses spread through time, as outcomes are
    s = _st(vals)
    assert s["win_rate"] == 0.9 and s["mean_R"] < 0 and s["p_expectancy_positive"] < 0.05


def test_small_samples_are_insufficient_not_zero():
    s = _st([0.5])
    assert s["status"] == stats.INSUFFICIENT and s["mean_R"] == 0.5
    assert stats.calibration_summary([], bins=10)["status"] == stats.INSUFFICIENT


def test_calibration_buckets_brier_and_log_score():
    pairs = [(0.8, 1)] * 40 + [(0.8, 0)] * 10 + [(0.2, 0)] * 40 + [(0.2, 1)] * 10
    c = stats.calibration_summary(pairs, bins=10)
    assert c["brier"] == pytest.approx(0.16) and c["log_score"] > 0 and c["ece"] == pytest.approx(0.0, abs=1e-9)
    b8 = next(b for b in c["buckets"] if b["lo"] == 0.8)
    assert b8["n"] == 50 and b8["observed"] == pytest.approx(0.8)
    assert c["brier_skill"] == pytest.approx(1 - 0.16 / 0.25)


def test_drawdown_tail_and_streaks():
    d = stats.drawdown_and_tail([1, -1, -1, -1, 2, -3, 1] * 5, es_alpha=0.05, min_tail_samples=20)
    assert d["max_drawdown_R"] >= 3 and d["max_loss_streak"] == 3 and d["worst_trade_R"] == -3
    assert d["expected_shortfall_R"] == -3
    assert stats.drawdown_and_tail([1, -1], min_tail_samples=20)["expected_shortfall_status"] == stats.INSUFFICIENT


def test_stratification_marks_small_strata_insufficient():
    rows = [{"k": "A", "net_R": 0.5, "gross_R": 0.6, "cost_R": 0.1}] * 20 + [{"k": "B", "net_R": -1, "gross_R": -0.9,
                                                                             "cost_R": 0.1}] * 3
    s = stats.stratify(rows, lambda r: r["k"], value="net_R", min_n=15, reps=100, confidence=0.95, seed=1,
                       cost_key="cost_R", gross_key="gross_R")
    assert s["A"]["status"] == "OK" and s["A"]["count"] == 20 and s["A"]["cost_share"] == pytest.approx(0.1 / 0.6)
    assert s["B"]["status"] == stats.INSUFFICIENT and s["B"]["count"] == 3


def test_concentration_detects_one_segment_explaining_the_result():
    rows = [{"symbol": "BTC", "net_R": 1.0}] * 10 + [{"symbol": "ETH", "net_R": -0.1}] * 10
    c = stats.concentration(rows, lambda r: r["symbol"], value="net_R")
    assert c["largest_segment"] == "BTC" and c["largest_positive_share"] == 1.0


def test_pbo_not_applicable_without_compared_variants_and_bounded_with_them():
    assert stats.pbo_cscv({"frozen": [0.1] * 40}, partitions=8)["status"] == "NOT_APPLICABLE"
    assert stats.pbo_cscv({"a": [0.1] * 4, "b": [0.2] * 4}, partitions=8)["reason"] == "TOO_FEW_OBSERVATIONS_PER_PARTITION"
    import random

    rng = random.Random(3)
    perf = {f"v{i}": [rng.gauss(0, 1) for _ in range(80)] for i in range(4)}
    out = stats.pbo_cscv(perf, partitions=8)
    assert out["status"] == "OK" and 0.0 <= out["pbo"] <= 1.0 and out["combinations"] == 70


def test_deflated_sharpe_accounts_for_trials():
    import random

    rng = random.Random(5)
    vals = [rng.gauss(0.1, 1) for _ in range(300)]
    one = stats.deflated_sharpe(vals, n_trials=1)
    many = stats.deflated_sharpe(vals, n_trials=50, trial_sharpe_variance=0.01)
    assert one["benchmark_sharpe"] == 0.0 and many["benchmark_sharpe"] > 0
    assert many["deflated_sharpe_ratio"] < one["deflated_sharpe_ratio"]
    assert stats.deflated_sharpe([0.1] * 3, n_trials=1)["status"] == stats.INSUFFICIENT


# ============================== GATES ==============================
def _pop(n, mean, *, p=0.99, dd=2.0):
    return {"n": n, "net": {"mean_R": mean, "p_expectancy_positive": p, "ci_low": mean - 0.1, "ci_high": mean + 0.1},
            "drawdown_tail": {"max_drawdown_R": dd}}


def _primary(n=100, mean=0.2, *, p=0.99, m15=0.1, m2=0.0, share=0.3, neighbors=1.0, pbo=0.1):
    conc = {k: {"largest_segment": "x", "largest_positive_share": share} for k in ("symbol", "month", "regime",
                                                                                  "setup_family", "side")}
    return {"counts": {"approved": n, "admissible": n, "candidates": n},
            "populations": {"APPROVED": _pop(n, mean, p=p), "ADMISSIBLE": _pop(n, mean)},
            "cost_stress": {m: {"reselected": {"APPROVED": _pop(n, v), "ADMISSIBLE": _pop(n, v)}}
                            for m, v in (("1.0", mean), ("1.5", m15), ("2.0", m2))},
            "concentration": {"population": "APPROVED", "by": conc},
            "parameter_neighbors": {"approved_positive_share": neighbors},
            "overfitting": {"pbo_cscv": {"status": "OK", "pbo": pbo}}}


GOOD_INTEGRITY = {"lookahead_violations": 0, "determinism_ok": True, "manifest_valid": True, "certifiable_source": True,
                  "real_data_present": True, "causal_feature_path_verified": True}
GOOD_CAL = {"library": {"n": 400, "ece": 0.02, "brier_skill": 0.05, "by_setup_family": {"A": {"n": 40}}},
            "replay": {"n": 400, "ece": 0.03, "brier_skill": 0.04}}
GOOD_FWD = {"elapsed_days": 31, "executed_count": 25, "trade_plans": 40, "status": "PASS"}


def _all_gates(policy, **over):
    kw = dict(integrity=GOOD_INTEGRITY, primary=_primary(), holdout={"status": "PASS", "metrics": _primary()},
              calibration=GOOD_CAL, forward_demo=GOOD_FWD, operational={"total": 0}, policy=policy)
    kw.update(over)
    return G.evaluate_gates(**kw)


def _status(gates, gate):
    return next(g for g in gates if g.gate == gate).status


def test_every_gate_passes_only_with_a_configured_policy_and_evidence():
    gates = _all_gates(_configured())
    assert all(g.status == "PASS" for g in gates), [(g.gate, g.status, g.reason_codes) for g in gates]
    assert G.overall_status(gates) == (O.CERTIFIED.value, ())
    # the SAME evidence under the default policy cannot pass: unconfigured thresholds fail closed
    default = _all_gates(default_certification_policy())
    assert G.overall_status(default)[0] == O.INSUFFICIENT_EVIDENCE.value
    assert any("CERTIFICATION_POLICY_INCOMPLETE" in g.reason_codes for g in default)


@pytest.mark.parametrize("gate,over,expected", [
    ("A_INTEGRITY", {"integrity": {**GOOD_INTEGRITY, "lookahead_violations": 1}}, "FAIL"),
    ("A_INTEGRITY", {"integrity": {**GOOD_INTEGRITY, "determinism_ok": False}}, "FAIL"),
    ("A_INTEGRITY", {"integrity": {**GOOD_INTEGRITY, "certifiable_source": False}}, "BLOCKED"),
    ("B_NET_EXPECTANCY", {"primary": _primary(mean=-0.05)}, "FAIL"),
    ("B_NET_EXPECTANCY", {"primary": _primary(p=0.6)}, "FAIL"),
    ("B_NET_EXPECTANCY", {"primary": _primary(n=0)}, "INSUFFICIENT_EVIDENCE"),
    ("B_NET_EXPECTANCY", {"primary": _primary(neighbors=0.25)}, "FAIL"),
    ("B_NET_EXPECTANCY", {"primary": _primary(pbo=0.6)}, "FAIL"),
    ("B_NET_EXPECTANCY", {"primary": _primary(neighbors=None)}, "INSUFFICIENT_EVIDENCE"),
    ("C_COST_STRESS", {"primary": _primary(m15=-0.01)}, "FAIL"),
    ("C_COST_STRESS", {"primary": _primary(m2=-0.5)}, "FAIL"),
    ("D_HOLDOUT", {"holdout": None}, "BLOCKED"),
    ("D_HOLDOUT", {"holdout": {"status": "PASS", "metrics": _primary(mean=-0.1)}}, "FAIL"),
    ("E_CONCENTRATION", {"primary": _primary(share=0.9)}, "FAIL"),
    ("F_CALIBRATION", {"calibration": {**GOOD_CAL, "replay": {"n": 400, "ece": 0.2, "brier_skill": 0.05}}}, "FAIL"),
    ("F_CALIBRATION", {"calibration": {**GOOD_CAL, "library": {**GOOD_CAL["library"], "n": 50}}},
     "INSUFFICIENT_EVIDENCE"),
    ("G_EVIDENCE_COUNT", {"primary": _primary(n=10)}, "INSUFFICIENT_EVIDENCE"),
    ("H_FORWARD_DEMO", {"forward_demo": {**GOOD_FWD, "elapsed_days": 29.9}}, "INSUFFICIENT_EVIDENCE"),
    ("H_FORWARD_DEMO", {"forward_demo": {**GOOD_FWD, "executed_count": 3}}, "INSUFFICIENT_EVIDENCE"),
    ("I_OPERATIONAL", {"operational": {"total": 1}}, "FAIL"),
    ("I_OPERATIONAL", {"operational": None}, "INSUFFICIENT_EVIDENCE"),
])
def test_each_mandatory_gate_individually(gate, over, expected):
    gates = _all_gates(_configured(), **over)
    assert _status(gates, gate) == expected
    assert G.overall_status(gates)[0] != O.CERTIFIED.value


def test_a_mandatory_failure_cannot_be_averaged_away():
    great = _primary(n=1000, mean=5.0, p=1.0, m15=4.0, m2=3.0, share=0.1)
    lookahead = _all_gates(_configured(), primary=great, integrity={**GOOD_INTEGRITY, "lookahead_violations": 1})
    assert G.overall_status(lookahead)[0] == O.NOT_CERTIFIED.value
    bad_holdout = _all_gates(_configured(), primary=great, holdout={"status": "FAIL", "metrics": {}})
    assert G.overall_status(bad_holdout)[0] == O.NOT_CERTIFIED.value
    no_demo = _all_gates(_configured(), primary=great, forward_demo={**GOOD_FWD, "elapsed_days": 3})
    assert G.overall_status(no_demo)[0] == O.FORWARD_DEMO_REQUIRED.value
    assert G.ready_for_forward_demo(no_demo) and not G.ready_for_forward_demo(lookahead)


def test_missing_real_data_is_blocked_by_data():
    gates = _all_gates(_configured(), integrity={"real_data_present": False}, primary=None, holdout=None,
                       calibration=None)
    assert G.overall_status(gates)[0] == O.BLOCKED_BY_DATA.value


def test_overall_requires_every_gate():
    with pytest.raises(ValueError):
        G.overall_status((GateResult(Gate.A_INTEGRITY.value, GateStatus.PASS.value),))


def test_full_refuses_insufficient_coverage_even_for_real_data():
    status, reasons = stage_status("FULL", None, policy=default_certification_policy(), integrity={},
                                   certifiable_source=True, coverage_days=180, required_days=730, determinism_ok=True)
    assert status == "BLOCKED_DATA" and "INSUFFICIENT_DATA_COVERAGE" in reasons
    status, reasons = stage_status("MEDIUM", {"counts": {"approved": 0}}, policy=default_certification_policy(),
                                   integrity={}, certifiable_source=True, coverage_days=200, required_days=180,
                                   determinism_ok=True)
    assert status == "INSUFFICIENT_EVIDENCE" and "NO_ACCEPTED_EVIDENCE" in reasons


# ============================== FORWARD DEMO ==============================
def _plan_row(conn, pid, *, t, env="DEMO", acct="acctA", venue="BINANCE_USDM"):
    conn.execute(
        "INSERT INTO cati_trade_plans (trade_plan_id, trade_plan_hash, user_id, broker_account_id, bot_instance_id, "
        "run_id, cycle_id, candidate_id, economic_opportunity_id, portfolio_decision_id, reservation_id, "
        "canonical_symbol, venue, environment, side, created_at, expires_at, schema_version, table_version, payload, "
        "payload_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (pid, "h", "u", acct, "bot", "r", "c", "cand", "opp", "pd", "res", "BTC/USDT:PERP", venue, env, "LONG", t,
         t + 1, "1", "1", "{}", "h"))


def _attempt(conn, pid, *, t, status="FILLED", aid=None, acct="acctA"):
    conn.execute(
        "INSERT INTO cati_execution_attempts (execution_attempt_id, sequence, trade_plan_id, user_id, broker_account_id, "
        "bot_instance_id, status, recorded_at, schema_version, table_version, payload, payload_hash) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (aid or f"att_{pid}", 1, pid, "u", acct, "bot", status, t, "1", "1",
         json.dumps({"planned_costs": {"R": 0.1}, "realized_costs": {"R": 0.12}}), "h"))


def test_forward_demo_needs_30_days_and_counts_only_recorded_executions(tmp_path):
    db = _store(tmp_path, "rt.db")
    now = int(time.time() * 1000)
    t0 = now - 10 * DAY
    with db.connect() as conn:
        _plan_row(conn, "p1", t=t0)
        _attempt(conn, "p1", t=t0 + 1000)
        _plan_row(conn, "p2", t=now - DAY)
        _attempt(conn, "p2", t=now - DAY + 1000)
        _plan_row(conn, "p_real", t=t0, env="REAL")               # user capital: never forward-demo evidence
        _plan_row(conn, "p_other", t=t0 - 40 * DAY, acct="acctB")  # another tenant
    tracker = ForwardDemoCertificationTracker(db, venue="BINANCE_USDM", broker_account_id="acctA", min_executed=5)
    snap = tracker.snapshot()
    assert snap["status"] == "IN_PROGRESS" and snap["reason"] == "FORWARD_DEMO_DAYS_INSUFFICIENT"
    assert snap["elapsed_days"] == pytest.approx(9, abs=0.1) and snap["executed_count"] == 2
    assert snap["excluded_real_environment_plans"] == 1 and snap["cost_pairs"] == 2
    with pytest.raises(ValueError):
        tracker.snapshot(now_ms=now + 40 * DAY)                    # cannot fabricate elapsed days
    with pytest.raises(ValueError):
        ForwardDemoCertificationTracker(db, venue="BINANCE_USDM", environments=("REAL",))
    # without the tenant filter the other account's older plan would stretch the period -- it must not leak in
    assert ForwardDemoCertificationTracker(db, venue="BINANCE_USDM").snapshot()["elapsed_days"] > 40


def test_forward_demo_extends_rather_than_lowering_the_evidence_floor(tmp_path):
    db = _store(tmp_path, "rt.db")
    now = int(time.time() * 1000)
    with db.connect() as conn:
        _plan_row(conn, "p1", t=now - 31 * DAY)
        _attempt(conn, "p1", t=now - 31 * DAY + 1)
        _plan_row(conn, "p2", t=now - DAY)
        _attempt(conn, "p2", t=now - DAY + 1)
    snap = ForwardDemoCertificationTracker(db, venue="BINANCE_USDM", min_executed=20).snapshot()
    assert snap["elapsed_days"] >= 30 and snap["status"] == "IN_PROGRESS"
    assert snap["reason"] == "FORWARD_DEMO_EVIDENCE_INSUFFICIENT_EXTEND_PERIOD"
    unset = ForwardDemoCertificationTracker(db, venue="BINANCE_USDM").snapshot()
    assert "CERTIFICATION_POLICY_INCOMPLETE" in unset["reason"]


def test_operational_defects_detect_unresolved_and_duplicate_fills(tmp_path):
    db = _store(tmp_path, "rt.db")
    now = int(time.time() * 1000)
    with db.connect() as conn:
        _plan_row(conn, "p1", t=now)
        _attempt(conn, "p1", t=now, aid="a1")
        _attempt(conn, "p1", t=now + 1, aid="a2")           # a second filled entry for one plan
        _plan_row(conn, "p2", t=now)
        _attempt(conn, "p2", t=now, status="SUBMIT_UNKNOWN")
    d = operational_defects(db)
    assert d["idempotency_defects"] == 1 and d["reconciliation_defects"] == 1 and d["total"] >= 2


# ============================== PERSISTENCE / SECURITY ==============================
def test_certification_runs_are_append_only_and_sanitized(tmp_path):
    db = _store(tmp_path)
    leak = "apiKey=AKIASECRETVALUE123 signature=deadbeefcafe"
    res = CertificationStageResult(stage="FAST", status="PASS", diagnostics={"note": leak})
    run = CertificationRun(stage="FAST", scope=_scope(), dataset_manifest_id="m", dataset_hash="d",
                           policy_freeze_hash="f", certification_policy_hash="p", version_hashes={}, result=res)
    store = CertificationRunStore(db)
    assert store.append(run) and not store.append(run)       # same analytical run stored once
    other = dataclasses.replace(run, result=CertificationStageResult(stage="FAST", status="FAIL"))
    assert other.certification_run_id == run.certification_run_id
    with pytest.raises(RegistryError):
        store.append(other)                                    # a different artifact under the same id
    with sqlite3.connect(str(tmp_path / "r.db")) as conn:
        raw = " ".join(r[0] for r in conn.execute("SELECT payload FROM cati_certification_runs"))
        raw += " ".join(r[0] for r in conn.execute("SELECT payload FROM cati_certification_stage_results"))
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM cati_certification_runs")
    assert "AKIASECRETVALUE123" not in raw and "deadbeefcafe" not in raw
    cols = {r[1] for r in sqlite3.connect(str(tmp_path / "r.db")).execute("PRAGMA table_info(cati_certification_runs)")}
    assert "broker_account_id" not in cols  # research artifacts are not tenant account evidence


def test_flags_and_frozen_policies_are_untouched_by_the_framework():
    from app.trading_intelligence.economics.policy import default_admission_policy
    from app.trading_intelligence.execution.config import CATIExecutionConfig

    cfg = CATIExecutionConfig()
    assert not cfg.active_execution_enabled and not cfg.exit_intent_routing_enabled
    assert default_admission_policy().minimum_conservative_edge_r == -0.05  # neighbors never replace the frozen value


def test_research_default_v1_resolves_every_threshold_and_is_frozen():
    from app.trading_intelligence.research.certification.policy import (
        RESEARCH_DEFAULT_V1_VALUES, canonical_certification_policy, research_default_v1,
    )

    v1 = research_default_v1()
    assert v1.not_configured() == () and v1.policy_name == "RESEARCH_DEFAULT_V1"
    assert v1.policy_hash == canonical_certification_policy().policy_hash == research_default_v1().policy_hash
    assert v1.policy_hash != default_certification_policy().policy_hash
    for name, (value, why) in RESEARCH_DEFAULT_V1_VALUES.items():
        pv = v1.get(name)
        assert pv.value == value and pv.provenance == "RESEARCH_DEFAULT" and pv.note.startswith("RESEARCH_DEFAULT_V1")
    assert v1.min_probability_expectancy_positive.value == 0.95 and v1.min_accepted_evidence_count.value == 100
    with pytest.raises(ValueError):
        v1.configure(min_accepted_evidence_count=50)            # never loosened because CATI fails
    with pytest.raises(ValueError):
        v1.configure(max_pbo=0.4)
