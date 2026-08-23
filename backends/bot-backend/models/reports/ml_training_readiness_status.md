# ML Training Readiness Status

- Last updated timestamp: 2026-07-18T02:30:00.149648+00:00
- Section 5A retry status: Not ready
- Section 5B status: Blocked
- Organic row count: 326
- IOFS organic/paper row count: 0
- Closed IOFS paper trades: 0
- Paper trade review entries: 0
- Last candidate result: REJECTED: Mean AUC 0.5442 < minimum 0.55 (model is not meaningfully better than random - not useful for gating). | LightGBM AUC 0.5442 < logistic baseline 0.5749 (gradient boosting adds no value over linear model on this dataset).
- Next action: continue_paper_validation

## Current Blockers

- Organic rows 326 are below retry threshold 500.
- IOFS organic/paper rows 0 are below retry threshold 300.
- Closed IOFS paper trades 0 are below retry threshold 20.
- No accepted validated runtime-compatible v2 .pkl artifact exists.
- Section 5B promotion guard does not allow deployment.

## Next Retry Condition

- Organic rows reach 500; or
- IOFS organic/paper rows reach 300; or
- Closed IOFS paper trades reach 20.

## ML Safety Status

- ML_ENABLED: False
- EXECUTION_MODE: paper
- IOFS_GATE_MODE: shadow
- models/production unchanged: true
- Auto-training enabled: false
- Auto-promotion enabled: false
