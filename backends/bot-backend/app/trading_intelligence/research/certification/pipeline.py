"""Section 22 certification pipeline: plan -> reserve holdout -> replay ->
stages -> calibration -> (guarded) holdout -> gates -> registry -> report.

Order matters and is enforced here:

1. the dataset manifest and the policy freeze are built from content only;
2. the HOLDOUT window is RESERVED in the append-only registry BEFORE any
   replay runs (the pre-holdout replay never loads those candles);
3. one canonical pre-holdout replay feeds FAST / MEDIUM / STRESS / FULL
   (views, not reruns; cached by manifest+freeze+config hash);
4. determinism is re-checked on a bounded re-run;
5. HOLDOUT opens only when explicitly requested, with a certifiable
   (committed, clean-tree) freeze, after the preceding stage PASSED -- once.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

from app.trading_intelligence.hashing import stable_hash

from . import evaluation as E
from .contracts import (
    CertificationRun, CertificationScope, CertificationStage as ST, CertificationStageResult,
    CertificationStatus as S, CertReason as R, OverallCertificationStatus as O,
)
from .dataset import CertificationDatasetManifest, build_certification_manifest
from .freeze import PolicyFreezeManifest, build_policy_freeze
from .gates import evaluate_gates, overall_status, ready_for_forward_demo
from .policy import CertificationPolicy, canonical_certification_policy
from .registry import (
    CertificationRunStore, ExperimentRecord, ExperimentRegistry, ExperimentStatus, HoldoutBurned, HoldoutRegistry,
)
from .replay import ReplayConfig, cache_key, chronology_for, load_replay, run_replay, save_replay
from .replay_venue import COST_PROVENANCE, replay_venue_model_identity
from .report import CertificationReport

DAY_MS = 86_400_000
OUT_OF_SCOPE = {"FOREX": "BLOCKED_DATA: no real certified Forex history/feed", "FUTURES":
                "BLOCKED_DATA: no real certified futures history/feed", "OTHER_VENUES": "not evaluated",
                "OTHER_TIMEFRAMES": "not evaluated"}
DETERMINISM_SAMPLE_DECISIONS = 48


def _iso(ms: Optional[int]) -> Optional[str]:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d") if ms else None


def build_scope(manifest: CertificationDatasetManifest, cfg: ReplayConfig) -> CertificationScope:
    from app.trading_intelligence.setups.registry import SPECIALIST_REGISTRY

    return CertificationScope(asset_class=cfg.asset_class, venue="BINANCE_USDM", environment=manifest.environment,
                              symbol_universe=tuple(cfg.symbols), timeframe=cfg.timeframe,
                              setup_families=tuple(SPECIALIST_REGISTRY), side_scope=("LONG", "SHORT"),
                              data_start=manifest.start_ms, data_end=manifest.end_ms)


def _window_records(records, start_ms: int, end_ms: int):
    return [r for r in records if start_ms <= r["decision_time"] <= end_ms]


def _records_hash(records) -> str:
    from .replay import _identity

    return stable_hash([_identity(r) for r in records])


def certify(series: Mapping[str, Mapping[str, Sequence[Any]]], meta: Mapping[str, Mapping[str, str]], *,
            cfg: ReplayConfig, data_sources: Sequence[str], source_provider: str, dataset_identity: str,
            research_db: Any, artifact_dir: Path, policy: Optional[CertificationPolicy] = None,
            runtime_db: Any = None, open_holdout: bool = False, calendar_source_version: str = "UNAVAILABLE",
            freeze: Optional[PolicyFreezeManifest] = None, hypothesis: str = "frozen CATI deterministic baseline",
            now_ms: Optional[int] = None) -> CertificationReport:
    started = int(now_ms or time.time() * 1000)
    policy = policy or canonical_certification_policy()
    from app.replay.cost_model import BINANCE_FUTURES_STANDARD
    from app.trading_intelligence.versions import RESEARCH_COST_MODEL_VERSION

    manifest = build_certification_manifest(
        series, data_sources=data_sources, source_provider=source_provider, dataset_identity=dataset_identity,
        asset_class=cfg.asset_class, venue="BINANCE_USDM", environment="REAL", warmup_bars=cfg.warmup_bars,
        label_horizon_bars=cfg.label_horizon_bars,
        cost_model_version=f"{RESEARCH_COST_MODEL_VERSION}:{BINANCE_FUTURES_STANDARD.model_hash}",
        calendar_source_version=calendar_source_version)
    freeze = freeze or build_policy_freeze(certification_policy=policy, label_horizon_bars=cfg.label_horizon_bars)
    scope = build_scope(manifest, cfg)
    plan = chronology_for(series, cfg)
    holdouts = HoldoutRegistry(research_db)
    holdout_id = holdouts.reserve(dataset_hash=manifest.dataset_hash, start_ms=plan.holdout.start_ms,
                                  end_ms=plan.holdout.end_ms, now_ms=started)

    # ---- one canonical pre-holdout replay (cached by content hash) ----------------------
    key = cache_key(manifest.manifest_hash, freeze.freeze_hash, cfg, "PRE_HOLDOUT")
    cached = load_replay(Path(artifact_dir), key)
    library_rows = template = None
    if cached is None:
        result = run_replay(series, meta, cfg, data_sources=data_sources, phase="PRE_HOLDOUT")
        save_replay(result, Path(artifact_dir), key)
        records, integrity, counts, library_info, replay_hash = (result.records, result.integrity, result.counts,
                                                                 result.library, result.replay_hash)
        library_rows, template = result.library_rows, result.library_template
        reused = False
    else:
        records, integrity, counts, library_info, replay_hash = (cached["records"], cached["integrity"],
                                                                 cached["counts"], cached["library"],
                                                                 cached["replay_hash"])
        reused = True
    # ---- bounded determinism re-check ---------------------------------------------------
    check = run_replay(series, meta, cfg, data_sources=data_sources, phase="PRE_HOLDOUT",
                       max_decisions=DETERMINISM_SAMPLE_DECISIONS, library_rows=library_rows,
                       library_template=template)
    if library_rows is None:
        library_rows, template = check.library_rows, check.library_template
    if check.records:
        last = max(r["decision_time"] for r in check.records)
        base = [r for r in records if r["decision_time"] <= last and r["fold"] == check.records[0]["fold"]]
        determinism_ok = _records_hash(base) == _records_hash(check.records)
    else:
        determinism_ok = True if not records else None
    real = manifest.certifiable_source
    integrity_ev = {"lookahead_violations": integrity.get("lookahead_violations", 0),
                    "determinism_ok": determinism_ok, "manifest_valid": bool(manifest.row_counts),
                    "certifiable_source": real, "real_data_present": True,
                    "causal_feature_path_verified": integrity.get("lookahead_violations", 0) == 0}

    # ---- stage views ---------------------------------------------------------------------
    stage_names = tuple(st.value for st in ST)
    baseline = _experiment(manifest, freeze, hypothesis, stage_names, results={}, status=ExperimentStatus.INCONCLUSIVE.value,
                           artifact_hashes={}, reasons=(), created_at=started)
    # multiple-testing N: DISTINCT experiments on this dataset (a re-run of the same one is not a new trial)
    trials = 1 + sum(1 for e in ExperimentRegistry(research_db).all(dataset_hash=manifest.dataset_hash)
                     if e["experiment_id"] != baseline.experiment_id)
    eval_window = plan.evaluation_window
    eval_days = eval_window.days if eval_window else 0.0
    stages: Dict[str, CertificationStageResult] = {}
    common = dict(policy=policy, integrity=integrity, certifiable_source=real, determinism_ok=determinism_ok)
    refs = {"replay_cache_key": key, "replay_hash": replay_hash}

    def view(stage: str, rows, coverage: float, required: float, diag: Mapping[str, Any]):
        metrics = E.stage_metrics(rows, policy, n_trials=trials) if coverage + 1e-9 >= required or stage != ST.FULL.value else None
        status, reasons = E.stage_status(stage, metrics, coverage_days=coverage, required_days=required, **common)
        stages[stage] = E.build_stage_result(stage, metrics, status, reasons, diagnostics=diag, artifact_refs=refs)

    fast_min, fast_max = policy.fast_window_days.value
    if eval_window:
        f_end = min(eval_window.end_ms, eval_window.start_ms + int(fast_max * DAY_MS) - 1)
        view(ST.FAST.value, _window_records(records, eval_window.start_ms, f_end),
             min(eval_days, float(fast_max)), float(fast_min),
             {"window": [eval_window.start_ms, f_end], "counts": counts, "integrity": integrity})
        med = float(policy.medium_window_days.value)
        m_end = min(eval_window.end_ms, eval_window.start_ms + int(med * DAY_MS) - 1)
        view(ST.MEDIUM.value, _window_records(records, eval_window.start_ms, m_end), min(eval_days, med), med,
             {"window": [eval_window.start_ms, m_end], "holdout_excluded": True})
        windows = E.stress_windows(series_before(series, plan.holdout.start_ms), cfg.timeframe)
        stress_rows = [dict(r, stress_windows=E.in_stress(r, windows)) for r in records if E.in_stress(r, windows)]
        flagged = sum(len(v) for sym in windows["flagged_days"].values() for v in sym.values())
        view(ST.STRESS.value, stress_rows, 1.0 if flagged else 0.0, 1.0,
             {"selection": {k: windows[k] for k in ("version", "rules", "lookback_days", "unavailable")},
              "flagged_day_count": flagged, "flagged_days": windows["flagged_days"]})
        full_min = float(policy.full_min_days.value)
        view(ST.FULL.value, records, manifest.coverage_days, full_min,
             {"coverage_days": manifest.coverage_days, "required_days": full_min,
              "missing_days": max(0.0, full_min - manifest.coverage_days)})

    # ---- calibration (library walk-forward + replay out-of-sample) --------------------------
    calibration = _calibration(library_rows, plan, manifest, stages)

    # ---- holdout (guarded, once) -----------------------------------------------------------
    primary_stage = next((s for s in (ST.FULL.value, ST.MEDIUM.value, ST.FAST.value)
                          if s in stages and stages[s].metrics), None)
    primary = dict(stages[primary_stage].metrics) if primary_stage else None
    holdout_stage = _holdout(open_holdout, holdouts, holdout_id, freeze, stages, series, meta, cfg, data_sources,
                             (library_rows, template), policy, trials, real, integrity, determinism_ok)
    stages[ST.HOLDOUT.value] = holdout_stage

    # ---- forward demo + operational (runtime evidence, read-only) ----------------------------
    fwd, ops = _runtime_evidence(runtime_db, cfg, policy)
    stages[ST.FORWARD_DEMO.value] = CertificationStageResult(
        stage=ST.FORWARD_DEMO.value,
        status=S.PASS.value if fwd.get("status") == "PASS" else S.INSUFFICIENT_EVIDENCE.value,
        metrics={"tracker": fwd}, reason_codes=(fwd.get("reason") or "FORWARD_DEMO_REQUIRED",))

    holdout_ev = holdout_stage.to_dict() if holdout_stage.status not in (S.NOT_RUN.value,) and holdout_stage.metrics else None
    gates = evaluate_gates(integrity=integrity_ev, primary=primary, holdout=holdout_ev, calibration=calibration,
                           forward_demo=fwd, operational=ops, policy=policy)
    overall, blocking = overall_status(gates)

    # ---- persistence: every stage run + the experiment (failures included) -------------------
    runs = CertificationRunStore(research_db)
    version_hashes = {"freeze": freeze.freeze_hash, **{k: stable_hash(v) for k, v in freeze.policy_hashes.items()}}
    artifact_hashes = {}
    for name, res in stages.items():
        run = CertificationRun(stage=name, scope=scope, dataset_manifest_id=manifest.manifest_id,
                               dataset_hash=manifest.dataset_hash, policy_freeze_hash=freeze.freeze_hash,
                               certification_policy_hash=policy.policy_hash, version_hashes=version_hashes, result=res,
                               reason_codes=res.reason_codes, started_at=started, completed_at=int(time.time() * 1000))
        runs.append(run)
        artifact_hashes[name] = run.artifact_hash
    exp_status = {O.CERTIFIED.value: ExperimentStatus.SUCCESS.value,
                  O.NOT_CERTIFIED.value: ExperimentStatus.FAILED.value}.get(overall, ExperimentStatus.INCONCLUSIVE.value)
    ExperimentRegistry(research_db).record(_experiment(
        manifest, freeze, hypothesis, stage_names,
        results={"overall": overall, "blocking": list(blocking), "stages": {k: v.status for k, v in stages.items()}},
        status=exp_status, artifact_hashes=artifact_hashes, reasons=tuple(blocking), created_at=started))

    return CertificationReport(
        overall_status=overall, blocking_gates=tuple(blocking), ready_for_forward_demo=ready_for_forward_demo(gates),
        scope=scope.to_dict(), source_commit=freeze.source_commit, source_tree_dirty=freeze.source_tree_dirty,
        dataset_manifest=manifest.to_dict() | {"metadata": None}, policy_freeze=freeze.to_dict() | {"freeze_hash": freeze.freeze_hash},
        certification_policy={"policy_hash": policy.policy_hash, **policy.to_dict(),
                              "not_configured": list(policy.not_configured()),
                              "research_defaults": list(policy.research_defaults())},
        stages=stages, gates=gates, library_recertification=_library_recert(calibration),
        data_still_needed=tuple(data_needed(manifest, policy, cfg, stages, fwd)), out_of_scope=OUT_OF_SCOPE,
        replay={"chronology": plan.to_dict(), "replay_hash": replay_hash, "determinism_ok": determinism_ok,
                "venue_model": dict(replay_venue_model_identity()),
                "cost_provenance": COST_PROVENANCE, "library_folds": library_info.get("folds"),
                "holdout_id": holdout_id, "counts": counts, "config": cfg.to_dict()},
        generated_at=datetime.now(timezone.utc).isoformat(), operational={"reused_cached_replay": reused})


def _experiment(manifest, freeze, hypothesis, stage_names, *, results, status, artifact_hashes, reasons, created_at):
    return ExperimentRecord(dataset_hash=manifest.dataset_hash, policy_hash=freeze.freeze_hash, hypothesis=hypothesis,
                            changed_parameters={}, reason_for_change="baseline measurement of the frozen system",
                            stages_run=tuple(stage_names), results=results, status=status,
                            artifact_hashes=artifact_hashes, reason_codes=tuple(reasons), created_at=created_at)


def series_before(series, end_ms: int):
    return {s: {tf: [r for r in rows if int(r[6]) < end_ms] for tf, rows in tfs.items()} for s, tfs in series.items()}


def _calibration(library_rows, plan, manifest, stages) -> Dict[str, Any]:
    from app.trading_intelligence.forecast.calibration_report import (
        CalibrationPolicy, derive_status, evaluate_library_calibration,
    )
    from app.trading_intelligence.forecast.library import HistoricalOutcomeLibrary

    cp = CalibrationPolicy()
    lib = HistoricalOutcomeLibrary.build(tuple(library_rows or ()), dataset_source_hash=manifest.source_hash,
                                         candidate_generation_versions={}, label_policy_version="cert",
                                         cost_model_version=manifest.cost_model_version, source_kind=manifest.source_kind)
    rep = evaluate_library_calibration(lib, embargo_ms=plan.purge_ms + plan.embargo_ms, policy=cp)
    fams = {k: {"n": v.get("n")} for k, v in (rep.by_setup_family or {}).items()}
    replay_cal = {}
    for s in (ST.FULL.value, ST.MEDIUM.value, ST.FAST.value):
        if s in stages and stages[s].metrics:
            replay_cal = dict(stages[s].metrics.get("calibration") or {})
            replay_cal["stage"] = s
            break
    return {"library": {"n": rep.n_evaluated, "brier": rep.brier_score, "brier_skill": rep.brier_skill, "ece": rep.ece,
                        "multiclass_brier": rep.multiclass_brier, "by_setup_family": fams,
                        "derived_status": derive_status(rep, cp), "policy_hash": cp.policy_hash,
                        "reliability": [dict(b) for b in rep.reliability]},
            "replay": replay_cal}


def _library_recert(calibration: Mapping[str, Any]) -> Dict[str, Any]:
    lib = calibration.get("library") or {}
    eligible = lib.get("derived_status") == "CALIBRATED" and (calibration.get("replay") or {}).get("status") == "OK"
    return {"current_flag": "RESEARCH_ONLY", "flag_changed_by_certification": False,
            "evidence_supports_calibrated": bool(eligible), "library_calibration_status": lib.get("derived_status"),
            "n_evaluated": lib.get("n"), "ece": lib.get("ece"), "brier_skill": lib.get("brier_skill"),
            "note": "status changes only through the existing calibration-record governance, never here"}


def _holdout(open_holdout, holdouts, holdout_id, freeze, stages, series, meta, cfg, data_sources, library,
             policy, trials, real, integrity, determinism_ok) -> CertificationStageResult:
    reserved = {"holdout_id": holdout_id, **holdouts.status(holdout_id)}
    if not open_holdout:
        return CertificationStageResult(stage=ST.HOLDOUT.value, status=S.NOT_RUN.value, diagnostics=reserved,
                                        reason_codes=(R.HOLDOUT_NOT_RUN.value, "RESERVED_UNTOUCHED"))
    if not freeze.certifiable:
        return CertificationStageResult(stage=ST.HOLDOUT.value, status=S.NOT_RUN.value, diagnostics=reserved,
                                        reason_codes=(R.HOLDOUT_NOT_FROZEN.value, "COMMITTED_CLEAN_TREE_REQUIRED"))
    prior = next((stages[s] for s in (ST.FULL.value, ST.MEDIUM.value) if s in stages), None)
    if prior is None or prior.status != S.PASS.value:
        return CertificationStageResult(stage=ST.HOLDOUT.value, status=S.NOT_RUN.value, diagnostics=reserved,
                                        reason_codes=(R.PRECEDING_STAGES.value, "RESERVED_UNTOUCHED"))
    try:
        holdouts.open(holdout_id, policy_freeze_hash=freeze.freeze_hash)
    except HoldoutBurned as exc:
        return CertificationStageResult(stage=ST.HOLDOUT.value, status=S.NOT_RUN.value, diagnostics=reserved,
                                        reason_codes=(R.HOLDOUT_BURNED.value, str(exc)[:120]))
    result = run_replay(series, meta, cfg, data_sources=data_sources, phase="HOLDOUT", library_rows=library[0],
                        library_template=library[1])
    metrics = E.stage_metrics(result.records, policy, n_trials=trials)
    status, reasons = E.stage_status(ST.HOLDOUT.value, metrics, policy=policy, integrity=result.integrity,
                                     certifiable_source=real, coverage_days=1.0, required_days=0.0,
                                     determinism_ok=determinism_ok)
    res = CertificationStageResult(stage=ST.HOLDOUT.value, status=status, metrics=metrics,
                                   diagnostics={**reserved, "replay_hash": result.replay_hash, "no_tuning_after_view": True},
                                   reason_codes=reasons)
    holdouts.burn(holdout_id, policy_freeze_hash=freeze.freeze_hash, result_hash=res.result_hash)
    return res


def _runtime_evidence(runtime_db, cfg: ReplayConfig, policy: CertificationPolicy):
    from .forward_demo import EvidenceUnavailable, ForwardDemoCertificationTracker, operational_defects

    if runtime_db is None:
        return {"status": "REQUIRED", "reason": "FORWARD_DEMO_REQUIRED_NO_RUNTIME_EVIDENCE", "elapsed_days": 0.0,
                "executed_count": 0}, None
    tracker = ForwardDemoCertificationTracker(
        runtime_db, venue="BINANCE_USDM", symbols=cfg.symbols, min_days=policy.min_forward_demo_days.value,
        min_executed=policy.min_forward_demo_executed_count.value)
    try:
        ops = operational_defects(runtime_db)
    except EvidenceUnavailable:
        ops = None
    return tracker.snapshot(), ops


def data_needed(manifest, policy, cfg, stages, fwd) -> list:
    out = []
    full_min = policy.full_min_days.value
    if manifest.coverage_days < full_min:
        out.append(f"FULL: {full_min - manifest.coverage_days:.0f} more days of {','.join(cfg.symbols)} "
                   f"{cfg.timeframe} history (have {manifest.coverage_days:.0f}, need >= {full_min})")
    if stages.get(ST.MEDIUM.value) and stages[ST.MEDIUM.value].status == S.BLOCKED_DATA.value:
        out.append(f"MEDIUM: >= {policy.medium_window_days.value} evaluated days after the walk-forward seed fold")
    out.append("FORWARD_DEMO: >= 30 calendar days of DEMO/TESTNET TradePlan/execution evidence (no user capital)")
    out.append("EVENT CALENDAR: a current, validated economic-calendar source (currently stale/unavailable)")
    out.append("FOREX / FUTURES: real certified historical + economic feeds (none exist yet)")
    out.extend(f"POLICY: configure {n}" for n in policy.not_configured())
    return out


def blocked_report(*, reason: str, policy: Optional[CertificationPolicy] = None, symbols: Sequence[str] = (),
                   timeframe: str = "15m", backfill_commands: Sequence[str] = ()) -> CertificationReport:
    """Every stage BLOCKED_DATA when no real market data is reachable."""
    from .gates import evaluate_gates

    policy = policy or canonical_certification_policy()
    freeze = build_policy_freeze(certification_policy=policy)
    stages = {s.value: CertificationStageResult(stage=s.value, status=S.BLOCKED_DATA.value,
                                                reason_codes=(R.NO_REAL_DATA.value, reason))
              for s in ST if s != ST.FORWARD_DEMO}
    stages[ST.FORWARD_DEMO.value] = CertificationStageResult(stage=ST.FORWARD_DEMO.value,
                                                             status=S.INSUFFICIENT_EVIDENCE.value,
                                                             reason_codes=("FORWARD_DEMO_REQUIRED",))
    gates = evaluate_gates(integrity={"real_data_present": False}, primary=None, holdout=None, calibration=None,
                           forward_demo=None, operational=None, policy=policy)
    overall, blocking = overall_status(gates)
    return CertificationReport(
        overall_status=overall, blocking_gates=blocking, ready_for_forward_demo=False,
        scope={"asset_class": "CRYPTO", "venue": "BINANCE_USDM", "symbol_universe": list(symbols), "timeframe": timeframe},
        source_commit=freeze.source_commit, source_tree_dirty=freeze.source_tree_dirty, dataset_manifest={},
        policy_freeze=freeze.to_dict() | {"freeze_hash": freeze.freeze_hash},
        certification_policy={"policy_hash": policy.policy_hash, "not_configured": list(policy.not_configured()),
                              "research_defaults": list(policy.research_defaults())},
        stages=stages, gates=gates,
        library_recertification={"current_flag": "RESEARCH_ONLY", "flag_changed_by_certification": False,
                                 "evidence_supports_calibrated": False, "note": "no real data evaluated"},
        data_still_needed=tuple(list(backfill_commands) + [f"POLICY: configure {n}" for n in policy.not_configured()]),
        out_of_scope=OUT_OF_SCOPE, replay={"status": "NOT_RUN", "reason": reason},
        generated_at=datetime.now(timezone.utc).isoformat())


__all__ = ["certify", "blocked_report", "build_scope", "data_needed", "OUT_OF_SCOPE"]
