# V3 residual findings used to register V4

Every one of the 51,887 V3 outer predictions was matched to its original
deterministic development sample, without dropping any instrument or family.
The join asserts decision time, label and all four coarse categorical fields;
canonical tie ordering recovers the exact instrument and geometry.

**Failure mode: MIXED — family-dependent geometry, underfit nonlinear effects,
and temporal outcome/discrimination drift.** Market-context omission is a
testable hypothesis, not an established causal explanation. Instrument-specific
error and all residual groups are reported; no production subsets are selected.

| Evidence | Folds 1–2 | Folds 3–5 |
|---|---:|---:|
| Causal Brier skill | 0.02175304 | 0.01614207 |
| ROC-AUC | 0.58614089 | 0.57485469 |
| Positive rate | 0.37322291 | 0.35234810 |
| Prediction mean | 0.35086557 | 0.35507601 |
| Prediction std | 0.06965665 | 0.07603246 |
| ECE | 0.02245340 | 0.01283849 |
| Mean y − p | +0.02235734 | −0.00272791 |

Later ECE improves while skill and discrimination weaken. Calibration drift
alone therefore does not explain the decline. Recalibration was not selected as
the primary improvement. The positive base rate and regime mix change over
calendar time; these observational comparisons cannot isolate temporal drift
from changing instruments, families or regimes.

Family geometry residuals justify bounded shared-slope deviations: breakout's
room quintile mean residuals rise from −0.0195 to +0.0319, whereas range mean
reversion's fall from +0.0338 to −0.0063. A single room relationship underfits
these differences. At the lowest and highest risk quintiles pooled residuals
are approximately +0.0111 and +0.0181, versus +0.00043 in the middle quintile.
Smooth nonlinear geometry is justified without creating exact cohorts.

Training-quantile geometry PSI is small for room (0.0018–0.0072) and moderate
for risk/cost in later folds (roughly 0.038–0.065). Risk and modeled cost are
deterministically linked under the pinned cost assumption; they are not two
independent discoveries. Sparse subgroup extremes are reported with counts and
are not treated as reliable evidence for exclusion or autonomous fine cells.

The row-level residual CSV additionally records modeled cost burden, every
instrument, family, side, regime, volatility, calendar time, mean training age,
and the reconstructed fixed-window market context. Age and calendar time are
correlated, so the analysis does not claim an independent training-age effect.

Generated evidence: `data/research/calibration_diagnostics/v3_residuals_for_v4/`
contains `outer_residuals.csv` and `residual_summary.json`. All data remains
previously inspected DEVELOPMENT evidence. Context source bounds, per-symbol
prefix hashes, exact close times and cache hash are in the V4 preparation
metadata. Reserved holdout query count is zero.

The V4 registry is written before new model fitting. It contains four families,
two C values each, a fixed shared conditional payoff family, training-only
selection, frozen binary gates, explicit late-fold consistency requirements,
and separately declared payoff checks. No outer-result-driven calibration or
winner selection is permitted.
