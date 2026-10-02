# CATI V3 information conditioning — completed development evaluation

V3 is implemented, integrated and evaluated. It improves the causal Brier skill
from V1's 0.00104569 to **0.01852752**, but does **not** satisfy the frozen 0.02
requirement. It is **REJECTED_PRE_HOLDOUT / RESEARCH_ONLY**. No runtime library
was pinned, no governance phase changed, and no holdout was opened or inspected.

The canonical application runs in paper mode with CATI authority blocked at M0.
FX remains an acquisition/freeze dependency. End-to-end user-account demo
validation has not been claimed from unit tests or unauthenticated API probes.
**ENGINEERING_READY = NO** for the full requested closure; **MODEL_READY = NO**.

## Registered candidate and method

- Candidate: `cati_v3_f97c1349086b6c1068ee49c0`.
- Full analytical hash: see [development result](cati_v3_development_result.json).
- Source revision of the converged evaluation: `a839d250` (full revision in result).
- Schema: `cati-v3-causal-geometry-additive-1`.
- Estimator: `L2_ADDITIVE_LOGISTIC_LOG_GEOMETRY`; final C = 0.01.
- Parent: immutable governed V1 library `0ead8264955eb958b20e165e…`.
- Dataset manifest: `d64e5c517fd634e57ecff9441341d67ea9009072d0b184772471cd33e5903dfb`.
- Registered budget: exactly three C variants (0.01, 0.1, 1.0), recorded in
  [research registry](cati_v3_research_registry.json). No family was excluded.

The numeric features are log(target room in R), log(initial risk / trigger),
log(1 + causal modeled round-trip cost in R), and the interaction of the first
two logs. Cost uses the pinned Binance research assumption and the explicit
48-bar 15-minute horizon. It never reads realized future costs, prices, labels,
MFE, MAE, terminal classes or exit time to construct a predictor. Modeled funding
remains a research assumption, not a reconstructed historical funding tape.

Family, side, dominant regime, volatility, timeframe and horizon enter as
regularized categorical effects sharing one intercept and numeric slopes.
Instrument effects require at least 300 training observations; smaller and
unseen instruments pool into the broader prediction. Other categorical effects
require 30 training observations. UNKNOWN/UNVERIFIED fields are absent rather
than fabricated. Instrument group is used only when present and populated.
Scaling and vocabulary are fitted exclusively on each training prefix.

This is additive partial pooling, not a full hierarchical Bayesian estimator.
Regularization shrinks effects and unsupported effects are omitted. It does not
make untested claims of monotonicity or a calibrated posterior confidence band.

## Chronology and inspection history

All pre-holdout data has previously been inspected. This run is explicitly
**development evidence**, not untouched confirmation. The reserved holdout was
never accessed. The evaluator has no database connection and reads only the
immutable pre-holdout rows file, verifying its pinned streaming hash and count.

Every twelfth canonical label-ID-ordered row is selected independently of its
outcome, then sorted by decision time with canonical tie ordering. There are
250,602 sampled development rows before fold exclusions. Six equal chronological
windows define seed 0 and outer evaluation windows 1–5. Each outer model trains
only on labels whose complete horizon ends strictly before that window starts.
Each training prefix has three inner windows: seed 0 and validations 1–2.
Lowest pooled inner Brier chooses C, with deterministic ties favoring smaller C.
All five outer folds independently selected C = 0.01. Final C uses only inner
selection scores, not outer winner shopping.

Training is deterministically thinned to at most 120,000 rows; outer and inner
evaluation are each capped at 12,000 per window. Each scored forecast's baseline
is the expanding positive rate of all sampled outcomes fully matured **before
that forecast**, including within-window maturation. Training maturity and
evaluation boundaries are recorded in the result. Labels arriving at the exact
forecast time are excluded. The final fitted artifact cannot forecast a
historical decision at or before its training label cutoff.

The first run stopped when a C=1 fit hit the 250-iteration ceiling. It produced
no final candidate. The repeated run used the same three registered variants,
raised the solver ceiling to 1,500, required convergence, and persisted every
inner attempt's complete metrics. This numerical repair did not expand the
model search or alter the gates.

## Outer development evidence

| Metric | Result |
|---|---:|
| Samples | 51,887 |
| Brier | 0.2264868529 |
| Causal baseline Brier | 0.2307623067 |
| Brier skill | 0.0185275224 |
| ECE, 10 bins | 0.0128049516 |
| ROC-AUC | 0.5789068191 |
| PR-AUC, average precision | 0.4134613875 |
| Log loss | 0.6445136291 |
| Top-decile lift | 1.1983896833 |

| Outer fold | Samples | Brier | Causal baseline | Skill | ECE |
|---|---:|---:|---:|---:|---:|
| 1 | 10,840 | 0.23011403 | 0.23491394 | 0.02043266 | 0.02929094 |
| 2 | 10,895 | 0.22811214 | 0.23350009 | 0.02307472 | 0.01645719 |
| 3 | 10,311 | 0.22663452 | 0.22999550 | 0.01461324 | 0.01586154 |
| 4 | 9,814 | 0.22389876 | 0.22783087 | 0.01725891 | 0.01062890 |
| 5 | 10,027 | 0.22318087 | 0.22695696 | 0.01663792 | 0.01250367 |

All folds have positive skill, but three individually fall below 0.02 and the
pooled skill fails. Sample count and ECE pass. Full metrics for every inner
attempt and breakdowns by family, side, regime, volatility, year and outer fold
are committed in the development result. Correlated instruments and overlapping
horizons mean these counts are not 51,887 independent statistical trials.

## Existing runtime integration and limitations

The canonical `load_library_artifact` recognizes the compact JSON model and
verifies its content identity, candidate ID, registered schema/estimator/budget,
dataset/parent identity, clean source provenance claim, holdout boundary, numeric
parameters and derived calibration verdict. Research loading requires explicit
DEVELOPMENT mode. RUNTIME refuses V3 until a separate governance checkpoint
approves this format, even if development metrics eventually pass. This artifact
fails the metrics in any event. The scope remains crypto only.

The existing `build_outcome_forecast`/`forecast_from_dimensions` contract produces
V3 p(net profitable). Ancillary terminal/R/MFE/MAE statistics reuse a bounded
512-per-family analog reference. They are explicitly **research approximations**,
not calibrated V3 conditional distributions. Forecast uncertainty is set to 1
and the binary interval to [0,1]. This preserves the research interface without
granting admission or pretending that binary calibration validates an entire
trade payoff distribution. The existing authority router, dispatcher, V2 gate,
no-fallback rule, broker path and hard-risk code are preserved.

The loader's artifact integrity is bound to a configured expected hash; it is
not a cryptographic third-party signature on model training. No runtime
governance approval, M1+ promotion or holdout authorization is inferred.

## Runtime verification

Canonical launcher start, graceful stop and restart succeeded against the
canonical paper database. Status showed one live lease, one RUNNING session,
fresh heartbeats and current bot cycles. HTTP health reports:

- runtime lease owned;
- signal scheduler running with eight jobs;
- calendar sync and event ingestion running;
- adaptive daily risk enabled, hard-loss fraction 0.025.

Calendar storage contains 584 events with a fresh ingestion timestamp. Runtime
startup discovered 741 instrument specifications. Market cycles processed
symbols and rejected a position whose stop loss exceeded the configured equity
risk cap. Strong-trend blocking remained enforced. Health, OpenAPI, broker
catalog (six entries) and event-stream health responded successfully. Protected
monitoring/risk endpoints returned 401 without a logged-in session; authorization
was not bypassed. No authenticated frontend account/portfolio reconciliation or
new demo order was performed. The unauthenticated broker account view was empty.

A read-only canonical authority probe returned phase M0 and V2 demo authority;
CATI received no entry authority. Runtime library remains unpinned. Broker keys,
withdrawal permissions and the user-connected architecture were unchanged.

## FX continuation

The existing resumable 1m supervisor was resumed only after checking for another
writer. It progressed from 6,565 remaining pair/day periods to fewer than 6,500.
The venv redirector and its Python child are one writer, not two acquisitions.
Provider pacing and backoff remain unchanged.

`scripts/fx_completion.ps1` waits read-only until acquisition is complete and the
writer exits. It then invokes `scripts/finalize_fx_reference.py` sequentially:
derive 5m/15m/4h, run full-history bid/ask and scale QA for all five resolutions,
classify gaps with the existing governed rule, and invoke the existing dataset
freeze only if every partition is complete. QA failure, any unexplained gap,
invalid/duplicate/misaligned bars or dirty provenance prevents freezing.
Status/evidence go to `data/research/fx_finalization/`. Neither acquisition nor
the final FX freeze has completed at this checkpoint. No gaps were reclassified
merely to pass the freeze.

## Validation and performance

- Full CATI suite: **1,110 passed**, eight existing LGBM feature-name warnings.
- Runtime/ownership/risk/broker/market-data/V3/FX suites: **353 passed**.
- Final scope/authority/V3/FX regression: **24 passed**.
- Peak evaluation working set: **591,540,224 bytes (564.14 MiB)**.
- Complete evaluation: **87.18 seconds**.
- Compact artifact load: **124.22 ms**.
- Binary inference: **0.654 ms**.
- Complete research forecast contract: **8.26 ms median**, **25.35 ms maximum**
  over 20 calls. This excludes broker/network/market-state computation.

Existing V1 manifests and calibration remained byte-identical; their regression
policy hash and frozen thresholds are unchanged. Holdout opened NO, inspected
NO, query count 0. Governance M0, CATI execution OFF.

Remaining closure blockers: pooled skill below 0.02; unvalidated conditional
payoff estimates; FX acquisition/QA/gaps/freeze; authenticated user-connected
account/demo validation after a genuinely eligible governance checkpoint.

## Reproduction

Use the canonical venv with a clean committed tree; output must be a new directory:

```powershell
backends\venv\Scripts\python.exe scripts/evaluate_cati_v3.py --library data/research/cati_libraries/cati_lib_0ead8264955eb958b20e165e --output data/research/calibration_diagnostics/v3_new_run
```

The generated artifact and prediction CSV are local research data. The committed
result contains the actual model coefficients/transforms, identities, complete
metrics, all attempts, performance and full artifact file hash. Reference rows
remain in the immutable source and generated artifact; they are reproducible
through the registered sampling rather than duplicated into Git.
