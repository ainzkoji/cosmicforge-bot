# Section 5A Candidate Failure Diagnosis

- assessment_date: 2026-06-14
- candidate: v2.0
- candidate_status: REJECTED
- deployment_status: EXPERIMENTAL_ONLY
- section_5b_status: BLOCKED

## Dataset And Split

- organic rows: 326
- wins: 95
- losses: 231
- overall win rate: 29.14%
- mean label R: -0.242800R
- initial chronological train/validation/test split: 230 / 48 / 48
- final evaluation train/holdout split: 278 / 48
- final holdout win rate: 22.92%
- train-period win rate: approximately 31.74%
- feature contract: `entry_quality_v2`
- schema hash: `d4b19440376c7a066d38a733bc05253d4641caabc8ac922d4bbafc8adb949ae4`
- leakage check: passed

## Candidate Result

- LightGBM holdout AUC: 0.544226
- logistic baseline AUC: 0.574939
- LightGBM minus logistic AUC: -0.030713
- top/bottom quartile win-rate gap: 0.083333
- top-quartile mean R: -0.329028R
- bottom-quartile mean R: -0.495911R
- candidate artifact: none

The candidate failed because its holdout AUC was below 0.55 and it underperformed
the simpler logistic baseline. Since the candidate was rejected before final model
training, candidate feature importances/top features are unavailable. This absence
must not be replaced with invented importance values.

## Feature And Label Diagnosis

Weak or unstable features include:

- `htf_opposed`: constant across all 326 rows.
- `active_count_normed`: only two distinct values; 305 rows are 1.0.
- `planned_rr`: effectively constant at approximately 2.0.
- `regime_enc`: strongly imbalanced; 208 strong-trend rows, 97 weak-trend rows,
  19 range rows, and only 2 high-volatility rows.
- temporal label rate: the later validation/holdout periods have materially lower
  win rates than the earlier training period.

The labels are complete and leakage-safe, but they are weak/noisy for learning:
only 95 of 326 rows are wins and average realized R is negative. A single 48-row
holdout is also statistically fragile.

## IOFS Diagnosis

- available IOFS event rows: 5,953
- trace-linked IOFS organic/paper rows: 0
- closed Section 4 IOFS paper trades: 0
- IOFS organic dataset status: `IOFS_ORGANIC_DATA_INSUFFICIENT`
- IOFS replay rows: 21, separated and marked `data_source=replay`

The organic dataset has no trace-linked IOFS context. IOFS fields are therefore
missing from the production-candidate training evidence. Replay data is useful for
research only and is not mixed into organic candidate training.

## Cause Assessment

| Suspected cause | Assessment |
|---|---|
| Low sample size | Yes. Minimum 300 is met, but recommended 500 is not met. |
| Unstable features | Yes. Several features are constant, nearly constant, or regime-imbalanced. |
| Weak model | Yes for the rejected tuned candidate; it lost to logistic regression. |
| Weak/noisy labels | Yes. Win rate is 29.14% and mean R is negative. |
| Temporal split issue | No leakage found, but the 48-row holdout is high variance and later win rates are lower. |
| IOFS fields missing | Yes. There are zero trace-linked IOFS organic/paper rows. |
| Market regime instability | Yes. Regimes are heavily imbalanced, with only two high-volatility rows. |

## Baseline Follow-Up

A separate fixed-parameter LightGBM comparison scored 0.675676 AUC versus logistic
0.574939 on the final 48-row holdout. Across three expanding temporal folds,
however, fixed LightGBM averaged 0.605287 AUC versus logistic 0.607080 and was more
variable (0.062657 versus 0.036721 standard deviation). The isolated holdout win
does not establish a stable advantage and does not override the small-sample
warning or missing IOFS evidence.

Training was not retried because the dataset remains at 326 rows, no new IOFS
organic rows exist, and the approved next retry condition remains at least 500
organic/paper rows or enough closed Section 4 IOFS paper trades.
