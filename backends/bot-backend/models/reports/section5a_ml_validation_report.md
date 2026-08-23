# Section 5A ML Validation Report

- assessment_date: 2026-06-13
- section_5a_status: Complete
- candidate_status: Rejected
- deployment_status: EXPERIMENTAL_ONLY
- eligible_for_section_5b_shadow: false
- production_eligible: false
- section_4_status: In Progress

## Dataset Readiness

- dataset: `models/datasets/training_v2_organic.parquet`
- organic_row_count: 326
- usable_row_count: 326
- minimum_300_met: true
- recommended_500_met: false
- warning: LOW_SAMPLE_WARNING
- feature_contract_version: `entry_quality_v2`
- schema_hash: `d4b19440376c7a066d38a733bc05253d4641caabc8ac922d4bbafc8adb949ae4`
- post_repair_cutoff: `2026-04-01T00:00:00+00:00`
- organic_only: true
- require_trace_id: true
- exclude_incomplete_labels: true
- leakage_check_passed: true
- readiness_result: READY_FOR_EXPERIMENTAL_TRAINING
- production_promotion_result: BLOCKED

The requested bare dataset command cannot run because `build_dataset.py` requires
`--db-path`. The strict organic command wrote the dataset and metadata and printed
a successful quality report, but the builder process did not exit after completion
and was terminated by the command timeout.

## Training And Validation

The required command without an override stopped correctly because the 70 percent
training split contained 230 rows, below the default 300-row split floor.

Training was then run with the intentionally supported override:

```text
python scripts/ml/train_entry_model.py --dataset-path models/datasets/training_v2_organic.parquet --model-version v2.0 --scale-pos-weight auto --calibrate --min-train-size 200
```

- total_rows: 326
- holdout_auc: 0.544226
- walk_forward_auc_mean: 0.544226
- walk_forward_auc_std: 0.0
- logistic_baseline_auc: 0.574939
- quartile_win_rate_gap: 0.083333
- trainer_accepted: false
- independent_validator_accepted: false
- model_artifact: none
- model_metadata: none
- validation_report: `models/artifacts/entry_quality_v2.0_20260613_validation.json`
- calibration_plot: `models/artifacts/entry_quality_v2.0_20260613_calibration.png`

Rejection reasons:

- Holdout AUC is below the desired 0.55 minimum.
- LightGBM AUC is below the logistic baseline.
- No model artifact was written, so runtime-scorer compatibility cannot pass.

## Safety And Tests

- active `EXECUTION_MODE=paper`
- active `TRADE_SYMBOLS=BTCUSDT,ETHUSDT`
- active `ML_ENABLED=False`
- active `IOFS_GATE_ENABLED=True`
- active `IOFS_GATE_MODE=shadow`
- `models/production/` contains no model artifact
- Section 4 remains In Progress
- earliest Section 4 four-week completion timestamp: `2026-07-11T18:20:04Z`
- closed Section 4 paper trades: 0
- required ML, IOFS, FIX-D, and FIX-E regression suite: 108 passed

Section 5A is complete because readiness, training, validation, rejection, and
safety gates were applied correctly. Section 5B remains blocked until a valid
candidate exists and the separate deployment approval criteria are satisfied.
