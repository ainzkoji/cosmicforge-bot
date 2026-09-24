"""Classification of the PRE-EXISTING (V2) ML system -- audited before any
CATI ML was added (Section 23 / consolidation Part B).

The V2 MLEntryScorer is an entry-quality LightGBM gate over V2 semantics
(``confidence_normed``, ``threshold_gap``, ``consensus_gap``, active strategy
count, account open positions). It is LEGACY_V2_ML: it stays supported for
V2 until Section 25 retires V2, and it is never a CATI estimator, never CATI
admission, and its ``ML_HARD_BLOCK_FLOOR`` is never CATI authority.
"""
from __future__ import annotations

from typing import Any, Mapping

REUSE_AS_INFRASTRUCTURE = "REUSE_AS_INFRASTRUCTURE"
REUSE_AFTER_ADAPTATION = "REUSE_AFTER_ADAPTATION"
LEGACY_V2_ML = "LEGACY_V2_ML"
RESEARCH_ONLY = "RESEARCH_ONLY"
OBSOLETE_AFTER_M9 = "OBSOLETE_AFTER_M9"
DO_NOT_USE_FOR_CATI = "DO_NOT_USE_FOR_CATI"

#: component -> (classes, reason). Nothing is deleted before M9.
LEGACY_ML_COMPONENTS: Mapping[str, tuple] = {
    "app/ml/scorer.py": ((LEGACY_V2_ML, DO_NOT_USE_FOR_CATI, OBSOLETE_AFTER_M9),
                         "V2 entry-quality gate over V2 confidence/threshold features; runner-wired for V2 only"),
    "app/ml/calibrated_model.py": ((REUSE_AS_INFRASTRUCTURE,),
                                   "generic isotonic post-calibration wrapper; reused by CATI training"),
    "shared_lib/ml/contract.py": ((LEGACY_V2_ML, DO_NOT_USE_FOR_CATI),
                                  "entry_quality_v2 feature contract; its schema-hash PATTERN is reused, not its columns"),
    "shared_lib/ml/readiness.py": ((REUSE_AFTER_ADAPTATION,),
                                   "dataset readiness checks (nulls, class balance) -- a pattern for CATI gates"),
    "shared_lib/ml/event_features.py": ((RESEARCH_ONLY,), "event timing features; the calendar source is stale"),
    "shared_lib/persistence/ml_runtime_status.py": ((LEGACY_V2_ML,), "V2 scorer runtime status table"),
    "scripts/ml/train_entry_model.py": ((LEGACY_V2_ML, OBSOLETE_AFTER_M9), "trains the V2 entry scorer"),
    "scripts/ml/build_dataset.py": ((LEGACY_V2_ML,), "V2 decision-trace dataset"),
    "scripts/ml/build_iofs_training_dataset.py": ((LEGACY_V2_ML,), "IOFS/V2 dataset"),
    "scripts/ml/analyze_holdout.py": ((LEGACY_V2_ML,), "V2 holdout analysis -- never CATI certification"),
    "scripts/ml/compare_entry_model_baselines.py": ((LEGACY_V2_ML, RESEARCH_ONLY), "V2 baselines"),
    "scripts/ml/analyze_strategy_expectancy.py": ((RESEARCH_ONLY,), "V2 strategy expectancy research"),
    "scripts/ml/phase_f_experiment.py": ((RESEARCH_ONLY,), "offline experiment"),
    "scripts/ml/phase_h_experiment.py": ((RESEARCH_ONLY,), "offline experiment"),
    "scripts/ml/promote_model.py": ((LEGACY_V2_ML,), "V2 promotion to models/production; concept reused, not code"),
    "scripts/ml/retrain_pipeline.py": ((LEGACY_V2_ML,), "V2 retraining workflow"),
    "scripts/ml/check_readiness.py": ((LEGACY_V2_ML,), "V2 dataset readiness"),
    "scripts/ml/watch_training_readiness.py": ((LEGACY_V2_ML,), "V2 readiness watcher"),
    "scripts/ml/check_event_ml_readiness.py": ((RESEARCH_ONLY,), "event-ML readiness"),
    "scripts/ml/validate_model.py": ((LEGACY_V2_ML,), "V2 artifact validation"),
    "scripts/ml/validate_ml_db.py": ((LEGACY_V2_ML,), "V2 ML DB validation"),
    "scripts/ml/verify_feature_completeness.py": ((LEGACY_V2_ML,), "V2 feature completeness"),
    "scripts/ml/monitor_shadow_collection.py": ((LEGACY_V2_ML,), "V2 shadow collection monitor"),
    "scripts/ml/monitor_live_gating.py": ((LEGACY_V2_ML,), "V2 live gating monitor"),
    "scripts/ml/live_gating_audit_monitor.py": ((LEGACY_V2_ML,), "V2 gating audit"),
    "scripts/ml/analyze_shadow.py": ((LEGACY_V2_ML,), "V2 shadow analysis"),
    "scripts/ml/backfill_historical_candles.py": ((REUSE_AS_INFRASTRUCTURE,), "canonical candle backfill"),
    "scripts/ml/historical_backfill.py": ((REUSE_AS_INFRASTRUCTURE,), "historical backfill tooling"),
    "scripts/ml/data_quality_check.py": ((REUSE_AS_INFRASTRUCTURE,), "candle data-quality tooling"),
    "scripts/ml/validate_backfill.py": ((REUSE_AS_INFRASTRUCTURE,), "backfill validation"),
}

LEGACY_CONTRACT_VERSIONS = frozenset({"entry_quality_v2", "entry_quality_v1", "entry_quality_v1.0",
                                      "entry_quality_v1.1"})


def is_legacy_v2_artifact(metadata: Mapping[str, Any]) -> bool:
    """True for V2 scorer metadata (its contract version or its feature columns)."""
    from shared_lib.ml.contract import ML_FEATURE_COLUMNS, ML_FEATURE_SCHEMA_HASH

    if str(metadata.get("contract_version") or "") in LEGACY_CONTRACT_VERSIONS:
        return True
    if metadata.get("schema_hash") == ML_FEATURE_SCHEMA_HASH:
        return True
    return list(metadata.get("feature_columns") or []) == list(ML_FEATURE_COLUMNS)


__all__ = ["LEGACY_ML_COMPONENTS", "is_legacy_v2_artifact", "LEGACY_CONTRACT_VERSIONS", "REUSE_AS_INFRASTRUCTURE",
           "REUSE_AFTER_ADAPTATION", "LEGACY_V2_ML", "RESEARCH_ONLY", "OBSOLETE_AFTER_M9", "DO_NOT_USE_FOR_CATI"]
