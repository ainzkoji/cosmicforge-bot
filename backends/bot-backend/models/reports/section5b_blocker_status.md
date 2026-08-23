# Section 5B Blocker Status

- Section 5B status: BLOCKED
- accepted_model_artifact_exists: false
- model_path: None
- meta_path: None
- validation_status: rejected_or_missing
- blocking_reasons: ["Mean AUC 0.5442 < minimum 0.55 (model is not meaningfully better than random \u2014 not useful for gating).", "LightGBM AUC 0.5442 < logistic baseline 0.5749 (gradient boosting adds no value over linear model on this dataset).", "No accepted validated v2 .pkl artifact exists."]
- ML_ENABLED remains false: true
- models/production unchanged: true
- next_retry_condition: dataset reaches at least 500 organic/paper rows or Section 4 produces enough closed IOFS paper trades, then a candidate must pass all gates.
