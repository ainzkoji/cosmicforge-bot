# Section 5B Promotion Guard Report

- generated_at_utc: 2026-06-15T17:47:24.147441+00:00
- promotion_allowed: false
- promoted: false
- dry_run: true
- model_version: v2.0
- candidate_model_path: models\artifacts\entry_quality_v2.0_20260613.pkl
- candidate_meta_path: models\artifacts\entry_quality_v2.0_20260613_meta.json
- candidate_validation_path: models\artifacts\entry_quality_v2.0_20260613_validation.json
- candidate_validation_status: rejected_or_missing
- blocking_reasons: ["NO_ACCEPTED_MODEL_ARTIFACT", "MODEL_PKL_MISSING", "META_JSON_MISSING", "VALIDATION_NOT_ACCEPTED", "AUC_BELOW_0_55", "LIGHTGBM_BASELINE_IMPROVEMENT_INSUFFICIENT", "RUNTIME_SCORER_LOAD_CHECK_FAILED"]
- runtime_load_check_passed: false
- ml_env_update_allowed: false
- section5b_status: BLOCKED
- t25_status: NOT_STARTED

No active `.env` update is performed by this guard.
