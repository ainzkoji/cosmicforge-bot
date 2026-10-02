# CATI next research candidates

## RESEARCH_CANDIDATE_V2_CAUSAL_BASELINE_CORRECTION

Purpose: repair the evaluation benchmark's use of future evaluation outcomes.
This candidate deliberately adds no predictive information. It uses exactly the
V1 library's immutable rows, labels, forecast probability arithmetic, uniform
analog weights, 15-row support floor and 10-observation family prior.

Its separate library schema is `2.1.0-research-causal-baseline`. Library identity
includes this schema. A new manifest pins the parent library hash, unchanged row
hash, dataset/universe/frozen certification policy, corrected calibration policy
hash, and a clean source commit. Row bytes are reused through a hard link, never
rewritten. The artifact loader permits this version only through explicitly
selected `DEVELOPMENT` mode; `RUNTIME` rejects it. No latest alias or runtime pin.

The opt-in evaluation policy is `1.1.0-causal-baseline`. Its baseline prediction
is the global profitable-event frequency in the same matured training slice
used by the model at each forecast. Scoring is uniform across selected events.
The existing skill formula and gates (skill >= 0.02, ECE <= 0.05, >= 300 samples,
>= 30 per family) remain unchanged. V1 evaluation stays available for exact
historical reproduction. The normal calibration CLI continues to select V1.

This correction must be measured once with the original deterministic selection,
not tuned. Passing a corrected benchmark would still not grant governance or
execution authority. A failure rejects the candidate before holdout.

## RESEARCH_CANDIDATE_V3_INFORMATION_CONDITIONING (design only)

Hypothesis: conditional target distance and risk/cost geometry carry information
lost by V1's coarse categorical cohorts. The current artifact stores these
continuous values but does not use them in probability estimation.

Use causal, broker-neutral inputs frozen at the decision timestamp:

- Explicit timeframe and horizon keys; never mix label horizons implicitly.
- Target distance in initial-risk units and initial risk as a price fraction.
- Cost burden under a pinned research cost model, with available data and modeled
  assumptions distinguished. Keep the label/cost contract fixed for this trial.
- Existing side, family, regime and volatility dimensions. Add higher-timeframe
  or cross-asset features only after proving timestamp alignment and recording
  missingness explicitly; do not fabricate liquidity/funding observations.

Compare a regularized, partially pooled probability estimator with the unchanged
V1 forecaster on identical causal folds. Fit feature transforms and shrinkage on
training data only. Any hyperparameter selection occurs inside nested development
folds; outer folds use frozen parameters. Register the complete candidate,
feature schema, estimator, tuning budget and fold boundaries before evaluation.
Use uniform event scoring as the governing measurement, with equal-instrument
and equal-time diagnostics alongside it. Do not select a production asset/family
subset from this report's exploratory group results.

Holdout remains closed. This investigation already viewed aggregate labels and
scores throughout the existing pre-holdout range. No part of that range may be
represented as a pristine, never-inspected final selection set. Before any V3
tuning, the research registry must identify and freeze an eligible independent
pre-holdout final selection layer, or explicitly govern reuse with its inspection
history disclosed. An existing certification window is not automatically an
untouched selection layer. If no eligible layer exists, V3 stays design-only.
Do not consume the reserved holdout to solve this constraint.

No V3 forecast, backoff, prior, threshold or production universe change is
implemented by this investigation.
