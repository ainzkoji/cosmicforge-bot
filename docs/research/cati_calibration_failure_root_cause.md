# CATI Calibration Failure Root-Cause Report

Library: `cati_lib_0ead8264955eb958b20e165e`  
Hash: `0ead8264955eb958b20e165edfd3dfc1ecd1548731e717e87c4bb04f526bc9b4`  
Calibration samples: **1,998**. Governance M0; runtime stopped.

| Original metric | Value |
| --- | --- |
| Original Brier | 0.2276457386 |
| Baseline Brier | 0.2277545313 |
| Brier skill | 0.0004776754 |
| ECE | 0.0104027439 |


## Reproduction

**PASS.** Every legacy aggregate shown above reproduced exactly (zero numerical discrepancy). The diagnostic replay uses indexed sufficient statistics to reproduce V1's scored Beta/Dirichlet probabilities, fixed stride and embargo. It does not recompute unused R-quantile/OOD fields for millions of training rows. Tests compare it with the unmodified full forecast engine across backoff cases and with the existing complete calibration evaluator on a deterministic small walk-forward fixture. This is exact numerical reproduction of the scored calibration semantics, not a claim that the original slow CLI was invoked again. The CLI writes calibration.json and was deliberately not used on V1.

- Row count: 3,007,222; library hash and canonical rows hash independently recomputed.
- Ordering: decision time, then lexicographic label ID. IDs are unique and file-ordered.
- Selection: stride **1,504**; 2,000 selected rows; two early predictions skipped for insufficient training; 1,998 scored. No randomness and no sampling seed.
- Training at each prediction: preceding rows whose decision time + 43,200,000 ms is <= prediction time. No own/future label enters its forecast.
- Timeframe: 15m; label horizon: 48 bars (12h); all four setup families; both directions.
- Universe: 136 instruments, all represented in scored predictions; identical frozen Binance crypto universe.
- Library decision range: 2024-09-25T14:44:59.999000+00:00 to 2026-07-12T04:59:59.999000+00:00.
- Each analog has weight 1; each scored event has equal weight. No class, family, asset or time reweighting. Event-dense assets/periods therefore receive more aggregate influence.

## Engineering

```text
IMPLEMENTATION_DEFECT = NO
DATA_DEFECT = NO
LABEL_DEFECT = NO
CALIBRATION_DEFECT = YES
```

The Brier formula is correct: skill = 1 - model Brier / baseline Brier. An independent fixture yields model Brier 0.055, baseline 0.25, skill 0.78. The defect is the benchmark's information set, not this formula. The NO findings mean no material defect identified by the specified audit; source-path checks are sampled, not an independent relabeling of all three million source histories.

### Baseline correction

V1 uses the **global net-profitable frequency across the entire scored evaluation sample**, 0.3508508509. It is neither rolling nor per-family/per-instrument/per-horizon. This is a retrospective best constant (a legitimate descriptive Brier reference), but it is not a causal forecaster: it uses later evaluation outcomes and the current target. It fails the user's explicit pre-prediction-information requirement. V1 model probabilities themselves remain causally embargoed.

The opt-in V2 benchmark computes the global profitable-event frequency in the same matured training slice as each forecast. It changes neither the model nor the acceptance gates.

| Measurement | V1 retrospective baseline | V2 causal expanding baseline |
| --- | --- | --- |
| Model Brier | 0.2276457386 | 0.2276457386 |
| Baseline Brier | 0.2277545313 | 0.2278840345 |
| Skill | 0.0004776754 | 0.0010456895 |


The correction leaves skill far below 0.02. The benchmark defect does not explain away the failure.

### Calculation details

All selected probabilities are finite and within [0,1]. The forecaster's Beta posterior bounds the mean; the Brier scorer does not clip probabilities. Diagnostic log loss uses the standard finite-probability implementation. No duplicate labels or scored rows were found. Invalid forecasts and insufficient training are skipped and counted; the two skips occur before enough matured training exists. No observed missing/NaN label values occur in audited numeric fields. Scoring does not impute missing outcomes. Group tables use the same global benchmark predictions restricted to each group, never a newly fitted group hindsight baseline.

## Prediction Distribution

| Statistic | Value |
| --- | --- |
| max | 0.6661594644 |
| mean | 0.3545045975 |
| min | 0.0874482369 |
| p05 | 0.2757312831 |
| p25 | 0.3181125734 |
| p50 | 0.3549561746 |
| p75 | 0.3893141547 |
| p95 | 0.4375423907 |
| positive_rate | 0.3508508509 |
| sharpness_variance | 0.0029418745 |
| std | 0.0542390499 |


Probabilities are not literally constant (standard deviation 0.05424, range 0.08745Ã¢â‚¬â€œ0.66616), but 90% lie around 0.276Ã¢â‚¬â€œ0.438 and the mean is close to the positive rate. The hypothesis of limited discriminating information is supported; absolute collapse to one probability is rejected.

### Reliability buckets

| Bin | Samples | Mean predicted | Actual positive rate | Bucket Brier | Aggregate Brier contribution |
| --- | --- | --- | --- | --- | --- |
| 0 | 1 | 0.0874482369 | 1.0000000000 | 0.8327507203 | 0.0004167922 |
| 1 | 10 | 0.1735654610 | 0.4000000000 | 0.2878873630 | 0.0014408777 |
| 2 | 274 | 0.2789585728 | 0.2919708029 | 0.2076676852 | 0.0284789518 |
| 3 | 1351 | 0.3508586485 | 0.3464100666 | 0.2267948225 | 0.1533532559 |
| 4 | 343 | 0.4235466155 | 0.4052478134 | 0.2416734488 | 0.0414884850 |
| 5 | 15 | 0.5500193780 | 0.4666666667 | 0.2555955927 | 0.0019188858 |
| 6 | 4 | 0.6264050366 | 0.5000000000 | 0.2739708827 | 0.0005484903 |
| 7 | 0 | None | None | None | 0.0000000000 |
| 8 | 0 | None | None | None | 0.0000000000 |
| 9 | 0 | None | None | None | 0.0000000000 |


### Brier decomposition

| Component | Value |
| --- | --- |
| binned_brier | 0.2272411903 |
| binned_reliability | 0.0008516448 |
| binned_resolution | 0.0013649858 |
| uncertainty | 0.2277545313 |
| within_bin_residual | 0.0004045483 |


The listed reliability and resolution use ten probability buckets. Their within-bin residual is reported explicitly; the binned Murphy expression is not asserted to equal the raw-probability score. Low ECE is dominated by well-populated near-base-rate buckets and is not proof of discrimination.

## Brier Skill by Horizon

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| 48 | 1998 | 0.2276457386 | 0.2277545313 | 0.0004776754 | 0.0104027439 |


Only 48 bars / 15m occurs. A different horizon or timeframe cannot be blamed or evaluated from this artifact.

## Brier Skill by Family

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| BREAKOUT_VOL_EXPANSION_V2 | 1045 | 0.2323200272 | 0.2309975222 | -0.0057251912 | 0.0252129845 |
| MOMENTUM_CONTINUATION_V1 | 86 | 0.2722164665 | 0.2826512233 | 0.0369174300 | 0.1375002353 |
| RANGE_MEAN_REVERSION_V2 | 800 | 0.2165318013 | 0.2170602835 | 0.0024347253 | 0.0093380439 |
| TREND_PULLBACK_V2 | 67 | 0.2302343295 | 0.2344016547 | 0.0177785657 | 0.0520075835 |


Breakout has 1,045 samples and negative skill; its error offsets small improvements elsewhere. Momentum's apparent skill exceeds 0.02 on only 86 samples, with ECE 0.13750 (fails 0.05). Pullback has 67 samples and ECE 0.05201. These exploratory subgroups do not justify production selection or threshold changes.

## Brier Skill by Fold

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| FOLD_0 | 340 | 0.2305563365 | 0.2283780719 | -0.0095379761 | 0.0369178670 |
| FOLD_1 | 348 | 0.2273737160 | 0.2259578017 | -0.0062662776 | 0.0204432403 |
| FOLD_2 | 348 | 0.2254590353 | 0.2302436968 | 0.0207808579 | 0.0278556823 |
| FOLD_3 | 328 | 0.2344726576 | 0.2349581814 | 0.0020664263 | 0.0295485053 |
| FOLD_4 | 314 | 0.2225133192 | 0.2209457613 | -0.0070947633 | 0.0325651665 |
| FOLD_5 | 320 | 0.2252656871 | 0.2256363596 | 0.0016427871 | 0.0124080313 |


These six temporal windows use the canonical pre-holdout span and bar-aligned equal-width segmentation. The legacy calibration is per-prediction expanding walk-forward, including predictions inside the initial seed window; it is not the certification replay's fixed-training library per evaluation fold. A single fixed training window must not be invented for it. Below, the training end is the latest matured training row among predictions in each window; individual train ends are in predictions.csv.

| Window | Training starts (UTC) | Latest training end (UTC) | First evaluated (UTC) | Last evaluated (UTC) | Positive rate |
| --- | --- | --- | --- | --- | --- |
| FOLD_0 | 2024-09-25T14:44:59.999000+00:00 | 2025-01-12T03:29:59.999000+00:00 | 2024-09-26T06:14:59.999000+00:00 | 2025-01-12T15:29:59.999000+00:00 | 0.3529411765 |
| FOLD_1 | 2024-09-25T14:44:59.999000+00:00 | 2025-05-01T02:59:59.999000+00:00 | 2025-01-13T00:29:59.999000+00:00 | 2025-05-01T14:59:59.999000+00:00 | 0.3448275862 |
| FOLD_2 | 2024-09-25T14:44:59.999000+00:00 | 2025-08-18T09:14:59.999000+00:00 | 2025-05-02T00:44:59.999000+00:00 | 2025-08-18T21:14:59.999000+00:00 | 0.3591954023 |
| FOLD_3 | 2024-09-25T14:44:59.999000+00:00 | 2025-12-05T04:44:59.999000+00:00 | 2025-08-19T07:14:59.999000+00:00 | 2025-12-05T16:44:59.999000+00:00 | 0.3750000000 |
| FOLD_4 | 2024-09-25T14:44:59.999000+00:00 | 2026-03-24T06:44:59.999000+00:00 | 2025-12-06T03:14:59.999000+00:00 | 2026-03-24T18:44:59.999000+00:00 | 0.3280254777 |
| FOLD_5 | 2024-09-25T14:44:59.999000+00:00 | 2026-07-11T12:14:59.999000+00:00 | 2026-03-25T04:29:59.999000+00:00 | 2026-07-12T00:14:59.999000+00:00 | 0.3437500000 |


Only FOLD_2 narrowly exceeds 0.02. Three windows are negative; the others are barely positive. No consistent strong-early/failed-recent pattern is established. Quarterly group labels in the initial summary are supplemental calendar diagnostics; this section and walk_forward_folds.json and by_walk_forward_fold.csv contain the correct six-window breakdown.

## Backoff Analysis

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| 0 | 1989 | 0.2273170028 | 0.2279281499 | 0.0026813144 | 0.0085888938 |
| 1 | 6 | 0.2064025710 | 0.1728127026 | -0.1943715244 | 0.3350764936 |
| 5 | 1 | 0.4437684320 | 0.1230963195 | -2.6050503675 | 0.6661594644 |
| 6 | 2 | 0.5102415747 | 0.2722454687 | -0.8741967575 | 0.6729163839 |


Exact level 0: **1,989/1,998 = 99.55%**; first backoff: **6/1,998 = 0.30%**; second backoff: **0**; level 5: **1**; level 6: **2**; final family-only level 7: **0**. There is no global/default level in this engine; its broadest level retains setup family. Excessive backoff is rejected as the dominant explanation. Rare deeper-backoff predictions are unstable, but removing them still leaves exact-cohort skill only 0.0026813.

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| 15-49 | 15 | 0.2446372209 | 0.2424156389 | -0.0091643511 | 0.1638220080 |
| >=50 | 1983 | 0.2275172100 | 0.2276436303 | 0.0005553434 | 0.0096246061 |


1,983 forecasts have >=50 matched rows; 15 have 15Ã¢â‚¬â€œ49. Dense-cohort skill remains 0.0005553, so sparse support is not the primary cause.

## Label Audit

Full-population checks: **zero** violations for numeric finiteness, cost-component sum, net_R = gross_R - total_cost_R, net_profitable = (net_R > 0), valid quality, 48-bar horizons, stop gross_R = -1, or target gross_R matching target-distance R. Sampled source reconstruction: **65/65** exact candidate IDs, labels and dimensions. Every source query was opened read-only and explicitly bounded below the reserved holdout start; maximum candle closes were also checked.

Decision time is the closed candle timestamp. Entry is the frozen candidate's trigger reference, not an assertion of an executable broker fill. future_rows begins at the next candle open strictly after that decision. Target/stop touches are detected in future bars; same-bar ties resolve to stop. Timeout uses the final future close with direction applied. A zero net return is not profitable; timeout may be profitable or unprofitable. Missing future horizons are censored and excluded by the builder. Touch indices are zero-based.

Fees, modeled half-spread and slippage are charged for both legs in price units and divided by initial structural risk. Funding uses the modeled rate and the full observed label horizon; it is not actual historical funding stamps or a realized exit-time funding simulation. MFE/MAE also describe the full label horizon. These are pinned research-label assumptions, not independently validated execution costs. They must not be silently relabeled to claim an improvement.

Library building and certification replay call the same label_candidate function. Calibration directly scores its net_profitable field; runtime entry forecasting estimates the same binary label through cohort posteriors. Position-management forecasts additionally condition on path survival and current remaining costs; they are not identical to the entry-calibration target. No off-by-one disagreement appeared in the 65 exact source reconstructions.

## Feature Information

Nine cohort dimensions are family, side, dominant regime, volatility, liquidity, trend maturity, HTF alignment, funding crowding and static instrument group. There are **1,204** exact cohorts; their full-population minimum size is **1**, median **67.5**. V1 stores room_to_target_R and initial_risk_fraction but uses neither in its probability estimate (room is used for OOD only). Horizon and timeframe are fixed artifact metadata rather than cohort keys.

Liquidity is UNVERIFIED for 100%; funding and HTF alignment are UNKNOWN for 100%. These three dimensions have zero within-library information. Instrument group is UNKNOWN for 84.93%; trend maturity is UNKNOWN for 71.47%. More rows do not create information in constant/unavailable features. Marginal outcome-rate differences below are descriptive in-sample associations, not causal out-of-sample predictive edge.

## Discrimination Metrics

| Metric | Value |
| --- | --- |
| average_precision | 0.3826327677 |
| log_loss | 0.6481015081 |
| pr_auc_trapezoid | 0.3811456079 |
| roc_auc | 0.5373477915 |


| Decile | Samples | Mean predicted | Positive rate | Lift |
| --- | --- | --- | --- | --- |
| 1 | 200 | 0.2655938524 | 0.3100000000 | 0.8835663338 |
| 2 | 200 | 0.3007395320 | 0.3050000000 | 0.8693152639 |
| 3 | 200 | 0.3176059547 | 0.2850000000 | 0.8123109843 |
| 4 | 200 | 0.3320848349 | 0.3650000000 | 1.0403281027 |
| 5 | 200 | 0.3472069884 | 0.3900000000 | 1.1115834522 |
| 6 | 200 | 0.3614448324 | 0.3600000000 | 1.0260770328 |
| 7 | 200 | 0.3751363945 | 0.3500000000 | 0.9975748930 |
| 8 | 200 | 0.3898501680 | 0.3650000000 | 1.0403281027 |
| 9 | 199 | 0.4049798412 | 0.3517587940 | 1.0025878322 |
| 10 | 199 | 0.4511391259 | 0.4271356784 | 1.2174280819 |


Top-decile lift is 1.2174, with 199 samples; outcome rates are not monotonically ordered across deciles. ROC-AUC 0.53735 and average precision 0.38263 against prevalence 0.35085 show weak discrimination, not proven absence of all possible market edge. Label overlap and cross-asset dependence mean individual events should not be treated as independent evidence for significance. These metrics never replace the Brier gate.

## Diagnosis

**MIXED_CAUSES.** A benchmark-information defect exists, but the dominant observed failure is insufficient predictive information in the current representation/estimator. V1's large, mostly exact cohorts average heterogeneous risk/target geometry and historical conditions uniformly; unavailable dimensions cannot differentiate outcomes, and stored continuous geometry is ignored by probability estimation. Breakout losses and modest gains elsewhere partly cancel. Sparse backoff and one bad horizon are not dominant causes. Temporal performance is weak and mixed. The audit does not establish that markets have no predictive edge in principle.

## Existing Candidate

**CURRENT_CANDIDATE = REJECTED_PRE_HOLDOUT.** The original immutable artifact/result remains **RESEARCH_ONLY**; rejection is this research decision, not an edit to calibration.json. No production universe is selected from subgroup results.

## Recommended Next Action

The causal benchmark correction is implemented as opt-in CausalCalibrationPolicy, with regression tests and a separate research library version. A new research-only V2 artifact was evaluated once on the same fixed selection; its final identity/result is recorded below. It changes evaluation semantics, not predictive information.

See cati_next_candidate_design.md for V3's predeclared research hypothesis: causal risk/target/cost geometry with partial pooling, explicit horizon/timeframe, nested training-only selection, and no cherry-picked production scope. V3 is design-only. The present audit has inspected pre-holdout outcomes across the current span; an untouched final selection layer cannot be claimed by simply renaming some of these rows. A registry-backed eligible independent layer must be established before tuning, without consuming holdout.

## Governance

```text
Holdout opened: NO
Holdout inspected: NO
Threshold changed: NO
Runtime library pinned: NO
CATI execution: OFF
Governance: M0
```

85 targeted tests passed across legacy library building/loading, source isolation, forecast labeling/probabilities, and new calibration regressions. Source provenance commit: `4719a831e0e4c8e8bf911e6cd5e865715cf9cf3e`, branch `codex/cati-calibration-root-cause`.

## Complete cohort tables

All group baselines below restrict the same V1 global reference to that group. Corresponding causal-baseline columns and training/evaluation ranges are available in the CSV evidence. Small-group values are exploratory.

### Instrument

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| 1000BONKUSDT | 22 | 0.1991463380 | 0.2044504009 | 0.0259430299 | 0.1439679477 |
| 1000FLOKIUSDT | 11 | 0.1897770225 | 0.2044504009 | 0.0717698685 | 0.1210952770 |
| 1000LUNCUSDT | 16 | 0.2448558577 | 0.2349581814 | -0.0421252678 | 0.1277180217 |
| 1000PEPEUSDT | 15 | 0.1762247596 | 0.1827559792 | 0.0357373785 | 0.1499930273 |
| 1000SHIBUSDT | 14 | 0.2610746090 | 0.2935524900 | 0.1106373890 | 0.3257067222 |
| 1INCHUSDT | 16 | 0.2778225694 | 0.2722454687 | -0.0204855594 | 0.1725354018 |
| AAVEUSDT | 12 | 0.2179421667 | 0.1976708941 | -0.1025506192 | 0.2978061717 |
| ACEUSDT | 10 | 0.2207397710 | 0.2424156389 | 0.0894161283 | 0.0983377717 |
| ADAUSDT | 18 | 0.2534566788 | 0.2556733410 | 0.0086698997 | 0.1221984474 |
| ALGOUSDT | 12 | 0.2062048937 | 0.1976708941 | -0.0431727675 | 0.1177274404 |
| ALICEUSDT | 10 | 0.2188799451 | 0.2125858090 | -0.0296075082 | 0.1049930455 |
| ALTUSDT | 15 | 0.2567375413 | 0.2424156389 | -0.0590799444 | 0.1314025684 |
| APEUSDT | 17 | 0.2037721071 | 0.2108311132 | 0.0334818045 | 0.0800003985 |
| APTUSDT | 17 | 0.2175974520 | 0.2283780719 | 0.0472051447 | 0.1170270373 |
| ARBUSDT | 12 | 0.2044942272 | 0.2225290856 | 0.0810449490 | 0.0853211532 |
| ARKMUSDT | 16 | 0.1425871830 | 0.1417399632 | -0.0059772824 | 0.2876763984 |
| ARKUSDT | 23 | 0.1971981919 | 0.1879437757 | -0.0492403440 | 0.1824633407 |
| ARUSDT | 16 | 0.2686743344 | 0.2908891123 | 0.0763685438 | 0.3093152904 |
| ATOMUSDT | 14 | 0.2657649523 | 0.2722454687 | 0.0238039460 | 0.1431145351 |
| AVAXUSDT | 18 | 0.2123840148 | 0.2059569580 | -0.0312058251 | 0.2176690383 |
| AXSUSDT | 13 | 0.1977190430 | 0.2148804113 | 0.0798647406 | 0.1687625356 |
| BBUSDT | 9 | 0.2267829614 | 0.2225290856 | -0.0191160438 | 0.0650399996 |
| BCHUSDT | 14 | 0.2168538965 | 0.2083244048 | -0.0409433150 | 0.1292052174 |
| BEAMXUSDT | 21 | 0.2269504689 | 0.2225290856 | -0.0198687881 | 0.0718304756 |
| BICOUSDT | 14 | 0.2267364444 | 0.2509384474 | 0.0964459740 | 0.1254797360 |
| BIGTIMEUSDT | 13 | 0.2962938073 | 0.2837184802 | -0.0443232572 | 0.2571382674 |
| BNBUSDT | 9 | 0.2047392926 | 0.2225290856 | 0.0799436757 | 0.0538231665 |
| BOMEUSDT | 13 | 0.2333022871 | 0.2607724572 | 0.1053415320 | 0.1464815775 |
| BTCUSDT | 15 | 0.1538605471 | 0.1429828728 | -0.0760767642 | 0.2851559051 |
| CAKEUSDT | 21 | 0.2342252802 | 0.2367337665 | 0.0105962338 | 0.0899514177 |
| CATIUSDT | 21 | 0.2309454049 | 0.2367337665 | 0.0244509336 | 0.0767779443 |
| CFXUSDT | 13 | 0.2889068514 | 0.2607724572 | -0.1078886724 | 0.1739309788 |
| CHZUSDT | 14 | 0.2245652442 | 0.2083244048 | -0.0779593702 | 0.1695294980 |
| COMPUSDT | 9 | 0.2250788156 | 0.1893848303 | -0.1884733072 | 0.2312677882 |
| COTIUSDT | 16 | 0.2851599808 | 0.2722454687 | -0.0474370141 | 0.1627202368 |
| CRVUSDT | 5 | 0.3332925494 | 0.3020752985 | -0.1033426138 | 0.2675123083 |
| DASHUSDT | 19 | 0.2218451723 | 0.2172957822 | -0.0209363939 | 0.1057195702 |
| DODOXUSDT | 8 | 0.2097369638 | 0.2349581814 | 0.1073434323 | 0.2695401067 |
| DOGEUSDT | 13 | 0.2665541605 | 0.2837184802 | 0.0604977148 | 0.1403437555 |
| DOGSUSDT | 12 | 0.2227873974 | 0.2225290856 | -0.0011608000 | 0.1655431650 |
| DOTUSDT | 15 | 0.2291747668 | 0.2026425324 | -0.1309312221 | 0.2331073186 |
| DUSKUSDT | 16 | 0.1557200882 | 0.1603836068 | 0.0290772772 | 0.2130599539 |
| DYDXUSDT | 7 | 0.1673957311 | 0.1657103622 | -0.0101705706 | 0.2310625902 |
| EGLDUSDT | 11 | 0.2379878755 | 0.2586864551 | 0.0800141609 | 0.2811237007 |
| ENAUSDT | 22 | 0.2766935760 | 0.2722454687 | -0.0163385909 | 0.1959205480 |
| ENJUSDT | 10 | 0.2233912288 | 0.2125858090 | -0.0508285094 | 0.0857825016 |
| ENSUSDT | 16 | 0.2451881315 | 0.2536018250 | 0.0331767864 | 0.1192582034 |
| ETCUSDT | 12 | 0.2608938435 | 0.2225290856 | -0.1724033413 | 0.1301610592 |
| ETHFIUSDT | 24 | 0.2513121530 | 0.2473872772 | -0.0158653098 | 0.0963478302 |
| ETHUSDT | 9 | 0.1581536687 | 0.1562405749 | -0.0122445387 | 0.2358673282 |
| FETUSDT | 15 | 0.2161941982 | 0.2225290856 | 0.0284676828 | 0.0885722020 |
| FILUSDT | 19 | 0.2361907872 | 0.2329956926 | -0.0137131059 | 0.1698010154 |
| GALAUSDT | 12 | 0.2018590654 | 0.1976708941 | -0.0211875974 | 0.1340527515 |
| GRTUSDT | 24 | 0.2669798400 | 0.2722454687 | 0.0193414741 | 0.1528705215 |
| HBARUSDT | 15 | 0.2049175322 | 0.2026425324 | -0.0112266650 | 0.1717208207 |
| ICPUSDT | 19 | 0.2110755099 | 0.1858959613 | -0.1354496806 | 0.3330880067 |
| IDUSDT | 13 | 0.2363893950 | 0.2378264343 | 0.0060423866 | 0.1408026246 |
| IMXUSDT | 15 | 0.2298222732 | 0.2424156389 | 0.0519494770 | 0.0706405690 |
| INJUSDT | 16 | 0.2246947094 | 0.2163145378 | -0.0387406771 | 0.1483618979 |
| IOSTUSDT | 15 | 0.1871194400 | 0.1827559792 | -0.0238758852 | 0.2625857150 |
| IOTAUSDT | 14 | 0.1784947651 | 0.1870173835 | 0.0455712630 | 0.1054031860 |
| IOUSDT | 16 | 0.2351459972 | 0.2349581814 | -0.0007993585 | 0.1078551631 |
| JASMYUSDT | 15 | 0.2165950100 | 0.2026425324 | -0.0688526610 | 0.0686745140 |
| JTOUSDT | 17 | 0.2338259444 | 0.2634719893 | 0.1125206706 | 0.1609596464 |
| JUPUSDT | 20 | 0.1937533847 | 0.1976708941 | 0.0198183420 | 0.0920482529 |
| KASUSDT | 14 | 0.1783156327 | 0.1870173835 | 0.0465291011 | 0.1885684881 |
| KAVAUSDT | 14 | 0.2409997669 | 0.2296314261 | -0.0495069034 | 0.1482912464 |
| KSMUSDT | 12 | 0.2148602847 | 0.2225290856 | 0.0344620163 | 0.0310028335 |
| LDOUSDT | 9 | 0.2178613780 | 0.2225290856 | 0.0209757193 | 0.2017719946 |
| LINKUSDT | 12 | 0.2655995045 | 0.2473872772 | -0.0736182859 | 0.1331067731 |
| LPTUSDT | 14 | 0.2236915847 | 0.2083244048 | -0.0737656249 | 0.2500085554 |
| LSKUSDT | 13 | 0.2209500838 | 0.2148804113 | -0.0282467463 | 0.2881937633 |
| LTCUSDT | 16 | 0.1691108025 | 0.1603836068 | -0.0544145119 | 0.2455823417 |
| MANAUSDT | 20 | 0.2417067982 | 0.2424156389 | 0.0029240716 | 0.1145514659 |
| MEMEUSDT | 18 | 0.2552681304 | 0.2556733410 | 0.0015848761 | 0.2146025534 |
| MINAUSDT | 17 | 0.2758489287 | 0.2985659068 | 0.0760869798 | 0.3281207534 |
| MOVRUSDT | 14 | 0.2410804152 | 0.2083244048 | -0.1572355888 | 0.3179259288 |
| NEARUSDT | 15 | 0.2367338302 | 0.2424156389 | 0.0234382925 | 0.1222256189 |
| NEIROUSDT | 11 | 0.2449343448 | 0.2315684280 | -0.0577190808 | 0.3445843379 |
| NEOUSDT | 11 | 0.3038488884 | 0.2858044823 | -0.0631354904 | 0.2016711195 |
| NOTUSDT | 14 | 0.2081380978 | 0.2083244048 | 0.0008943120 | 0.0659219150 |
| ONDOUSDT | 14 | 0.2430297446 | 0.2296314261 | -0.0583470596 | 0.0883323155 |
| ONEUSDT | 16 | 0.1591208637 | 0.1417399632 | -0.1226252649 | 0.3067255986 |
| ONGUSDT | 16 | 0.2181801982 | 0.2163145378 | -0.0086247577 | 0.0547710230 |
| ONTUSDT | 17 | 0.1900123923 | 0.1932841544 | 0.0169272133 | 0.1423947439 |
| OPUSDT | 14 | 0.2573386187 | 0.2509384474 | -0.0255049451 | 0.1934245075 |
| ORDIUSDT | 14 | 0.1531176482 | 0.1657103622 | 0.0759923147 | 0.2345352004 |
| PENDLEUSDT | 17 | 0.2995731510 | 0.2810189481 | -0.0660247399 | 0.1998680998 |
| PEOPLEUSDT | 12 | 0.1974074890 | 0.1976708941 | 0.0013325439 | 0.1139157995 |
| POLUSDT | 11 | 0.2015277152 | 0.2044504009 | 0.0142953289 | 0.0821333398 |
| POPCATUSDT | 10 | 0.3232676090 | 0.3319051284 | 0.0260240612 | 0.3508503510 |
| PORTALUSDT | 10 | 0.3146508121 | 0.3020752985 | -0.0416303937 | 0.2453571267 |
| PYTHUSDT | 19 | 0.2038985147 | 0.2172957822 | 0.0616545216 | 0.0763384752 |
| QNTUSDT | 15 | 0.2122125300 | 0.2026425324 | -0.0472260066 | 0.0881384834 |
| RENDERUSDT | 13 | 0.2929301914 | 0.2837184802 | -0.0324677871 | 0.1857211419 |
| REZUSDT | 10 | 0.2011860660 | 0.2125858090 | 0.0536241959 | 0.1637148752 |
| RIFUSDT | 14 | 0.2042442159 | 0.2083244048 | 0.0195857457 | 0.0791813041 |
| ROSEUSDT | 10 | 0.3013389274 | 0.3020752985 | 0.0024377071 | 0.2518265928 |
| RSRUSDT | 10 | 0.1433465985 | 0.1529261494 | 0.0626416795 | 0.2440146726 |
| RUNEUSDT | 19 | 0.2715927660 | 0.2643955135 | -0.0272215381 | 0.1257852922 |
| RVNUSDT | 14 | 0.2068136010 | 0.2083244048 | 0.0072521690 | 0.0544956724 |
| SAGAUSDT | 16 | 0.2406615934 | 0.2349581814 | -0.0242741578 | 0.1694161365 |
| SANDUSDT | 14 | 0.2137802897 | 0.2083244048 | -0.0261893700 | 0.0814990584 |
| SEIUSDT | 13 | 0.1829592565 | 0.1689883654 | -0.0826736860 | 0.2541535333 |
| SNXUSDT | 21 | 0.2402760114 | 0.2509384474 | 0.0424902446 | 0.1585129221 |
| SOLUSDT | 18 | 0.1964362267 | 0.2059569580 | 0.0462268009 | 0.1287642710 |
| STRKUSDT | 18 | 0.2918861268 | 0.2722454687 | -0.0721431955 | 0.1481817156 |
| STXUSDT | 15 | 0.2439248771 | 0.2225290856 | -0.0961482916 | 0.2854883851 |
| SUIUSDT | 15 | 0.2013522275 | 0.2225290856 | 0.0951644504 | 0.1111813901 |
| SUPERUSDT | 12 | 0.2186526584 | 0.2225290856 | 0.0174198678 | 0.1031895908 |
| SUSHIUSDT | 12 | 0.2746044792 | 0.2722454687 | -0.0086650130 | 0.2255690794 |
| SYNUSDT | 19 | 0.2085167408 | 0.2015958717 | -0.0343304107 | 0.1285357175 |
| TAOUSDT | 14 | 0.2434083113 | 0.2509384474 | 0.0300079010 | 0.0732833661 |
| THETAUSDT | 12 | 0.2616191201 | 0.2722454687 | 0.0390322330 | 0.3258980749 |
| TIAUSDT | 14 | 0.1934554522 | 0.2296314261 | 0.1575392990 | 0.2734508429 |
| TLMUSDT | 15 | 0.1717903971 | 0.1827559792 | 0.0600012219 | 0.2298169782 |
| TRBUSDT | 17 | 0.2508993676 | 0.2459250306 | -0.0202270463 | 0.1228297212 |
| TRXUSDT | 16 | 0.2276365764 | 0.2349581814 | 0.0311613113 | 0.0414013695 |
| TURBOUSDT | 15 | 0.2022266743 | 0.2225290856 | 0.0912348661 | 0.0924471708 |
| TUSDT | 10 | 0.1955012807 | 0.2125858090 | 0.0803653285 | 0.2090482105 |
| TWTUSDT | 17 | 0.2466938573 | 0.2459250306 | -0.0031262644 | 0.1674604385 |
| UNIUSDT | 9 | 0.1995293051 | 0.2225290856 | 0.1033562892 | 0.1659217730 |
| VETUSDT | 19 | 0.2216732950 | 0.2329956926 | 0.0485948794 | 0.0683057448 |
| WIFUSDT | 12 | 0.2723648878 | 0.2722454687 | -0.0004386452 | 0.1068950038 |
| WLDUSDT | 16 | 0.1864386203 | 0.1976708941 | 0.0568231041 | 0.1059987164 |
| WUSDT | 13 | 0.2279910049 | 0.2148804113 | -0.0610134424 | 0.1809089155 |
| XLMUSDT | 23 | 0.2706025995 | 0.2657607231 | -0.0182189315 | 0.1303858110 |
| XMRUSDT | 11 | 0.1217503381 | 0.1230963195 | 0.0109343762 | 0.3414539276 |
| XRPUSDT | 7 | 0.3164574891 | 0.3361665326 | 0.0586288093 | 0.3597397369 |
| XTZUSDT | 19 | 0.2625559060 | 0.2800954239 | 0.0626197948 | 0.1665082172 |
| XVGUSDT | 12 | 0.2517900612 | 0.2722454687 | 0.0751358970 | 0.3800874375 |
| ZECUSDT | 17 | 0.2062284318 | 0.2108311132 | 0.0218311294 | 0.1128324275 |
| ZENUSDT | 14 | 0.2223979590 | 0.2083244048 | -0.0675559553 | 0.1139740667 |
| ZILUSDT | 17 | 0.2480810370 | 0.2283780719 | -0.0862734541 | 0.1171368328 |
| ZKUSDT | 21 | 0.2666920265 | 0.2793478091 | 0.0453047500 | 0.2002647876 |
| ZROUSDT | 23 | 0.1945489674 | 0.1879437757 | -0.0351445090 | 0.1396780643 |


### Market family

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| crypto | 1998 | 0.2276457386 | 0.2277545313 | 0.0004776754 | 0.0104027439 |


### Timeframe

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| 15m | 1998 | 0.2276457386 | 0.2277545313 | 0.0004776754 | 0.0104027439 |


### Direction

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| LONG | 984 | 0.2214061032 | 0.2219227883 | 0.0023282200 | 0.0245534036 |
| SHORT | 1014 | 0.2337007693 | 0.2334137376 | -0.0012297124 | 0.0237139956 |


### Regime

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| EXHAUSTION_REVERSAL | 6 | 0.1082168072 | 0.1230963195 | 0.1208769882 | 0.3268260548 |
| RANGE_EQUILIBRIUM | 1061 | 0.2219422787 | 0.2217793570 | -0.0007346117 | 0.0045725107 |
| SHOCK | 118 | 0.2295371010 | 0.2267423384 | -0.0123257201 | 0.0724045713 |
| TREND_CONTINUATION | 284 | 0.2391732504 | 0.2396847389 | 0.0021340054 | 0.0498148892 |
| VOL_EXPANSION | 529 | 0.2338290096 | 0.2347467223 | 0.0039093739 | 0.0259891609 |


### Volatility bucket

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| HIGH | 750 | 0.2365572285 | 0.2376428661 | 0.0045683575 | 0.0105762324 |
| LOW | 625 | 0.2170582573 | 0.2199836068 | 0.0132980340 | 0.0162490073 |
| MEDIUM | 623 | 0.2275390906 | 0.2236463077 | -0.0174059786 | 0.0353257000 |


### Liquidity bucket

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| UNVERIFIED | 1998 | 0.2276457386 | 0.2277545313 | 0.0004776754 | 0.0104027439 |


### Year

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| 2024 | 305 | 0.2287238182 | 0.2267672035 | -0.0086282964 | 0.0416743313 |
| 2025 | 1134 | 0.2289439859 | 0.2301575254 | 0.0052726473 | 0.0068116091 |
| 2026 | 559 | 0.2244238660 | 0.2234184664 | -0.0045000738 | 0.0219655582 |


### Month

| Group | Samples | Model Brier | Baseline Brier | Skill | ECE |
| --- | --- | --- | --- | --- | --- |
| 2024-09 | 15 | 0.3491996209 | 0.2623021921 | -0.3312874672 | 0.3780155895 |
| 2024-10 | 93 | 0.2389953944 | 0.2417741371 | 0.0114931347 | 0.0458619487 |
| 2024-11 | 97 | 0.2202586545 | 0.2245792458 | 0.0192386046 | 0.0436907843 |
| 2024-12 | 100 | 0.2093110907 | 0.2096028260 | 0.0013918483 | 0.0590092925 |
| 2025-01 | 100 | 0.2217819030 | 0.2215347580 | -0.0011156040 | 0.0345508685 |
| 2025-02 | 90 | 0.2469103353 | 0.2457300644 | -0.0048031196 | 0.0657635109 |
| 2025-03 | 98 | 0.2390080951 | 0.2387630066 | -0.0010264928 | 0.0638915415 |
| 2025-04 | 92 | 0.2138809854 | 0.2073980125 | -0.0312586064 | 0.1204518329 |
| 2025-05 | 101 | 0.2172275718 | 0.2205601200 | 0.0151094776 | 0.0436487559 |
| 2025-06 | 95 | 0.2558329891 | 0.2581155493 | 0.0088431719 | 0.0934463136 |
| 2025-07 | 98 | 0.2188786731 | 0.2265875659 | 0.0340216938 | 0.0360790534 |
| 2025-08 | 95 | 0.2206947395 | 0.2267157284 | 0.0265574383 | 0.0507099420 |
| 2025-09 | 90 | 0.2382008981 | 0.2357867878 | -0.0102385311 | 0.0396665663 |
| 2025-10 | 96 | 0.2338314915 | 0.2318509075 | -0.0085424901 | 0.0288021196 |
| 2025-11 | 89 | 0.2183257833 | 0.2202946415 | 0.0089373855 | 0.0573998237 |
| 2025-12 | 90 | 0.2238376434 | 0.2291579367 | 0.0232167099 | 0.0287934791 |
| 2026-01 | 89 | 0.2174223742 | 0.2135913089 | -0.0179364289 | 0.0524303665 |
| 2026-02 | 85 | 0.2238063302 | 0.2248686801 | 0.0047243127 | 0.0247289978 |
| 2026-03 | 87 | 0.2278567504 | 0.2225290856 | -0.0239414311 | 0.0648149862 |
| 2026-04 | 89 | 0.2550335046 | 0.2571629705 | 0.0082806084 | 0.0914635956 |
| 2026-05 | 93 | 0.2018511553 | 0.2032840341 | 0.0070486541 | 0.0920040896 |
| 2026-06 | 85 | 0.2130706478 | 0.2143405049 | 0.0059244850 | 0.1011907073 |
| 2026-07 | 31 | 0.2475525808 | 0.2385666286 | -0.0376664257 | 0.0564112066 |


## Population information by dimension

### dominant_regime

| Value | Rows | Population share | Positive rate | Min exact-cohort size | Median exact-cohort size |
| --- | --- | --- | --- | --- | --- |
| EXHAUSTION_REVERSAL | 7123 | 0.0023686312 | 0.3575740559 | 1 | 7.0000000000 |
| RANGE_EQUILIBRIUM | 1627361 | 0.5411509360 | 0.3395521952 | 1 | 203.5000000000 |
| SHOCK | 188282 | 0.0626099437 | 0.4154672247 | 1 | 41.5000000000 |
| TREND_CONTINUATION | 431427 | 0.1434636352 | 0.3616973439 | 1 | 138.0000000000 |
| VOL_EXPANSION | 753029 | 0.2504068539 | 0.3864286767 | 1 | 68.0000000000 |


### funding_crowding_bucket

| Value | Rows | Population share | Positive rate | Min exact-cohort size | Median exact-cohort size |
| --- | --- | --- | --- | --- | --- |
| UNKNOWN | 3007222 | 1.0000000000 | 0.3592631339 | 1 | 67.5000000000 |


### htf_alignment

| Value | Rows | Population share | Positive rate | Min exact-cohort size | Median exact-cohort size |
| --- | --- | --- | --- | --- | --- |
| UNKNOWN | 3007222 | 1.0000000000 | 0.3592631339 | 1 | 67.5000000000 |


### initial_risk_fraction

Unique values: 2451482. Not used in probability estimation.

| Quantile | Value |
| --- | --- |
| max | 2.4769833497 |
| min | 0.0000083326 |
| p05 | 0.0041813315 |
| p25 | 0.0090695599 |
| p50 | 0.0154900965 |
| p75 | 0.0264337260 |
| p95 | 0.0563216474 |


| Lower | Upper | Population | Positive rate |
| --- | --- | --- | --- |
| 0.0000083326 | 0.0056082830 | 300722 | 0.2622588304 |
| 0.0056082830 | 0.0079446640 | 300723 | 0.2992887142 |
| 0.0079446640 | 0.0102174353 | 300722 | 0.3199001071 |
| 0.0102174353 | 0.0126728111 | 300720 | 0.3377726789 |
| 0.0126728111 | 0.0154900965 | 300723 | 0.3547284378 |
| 0.0154900965 | 0.0189349112 | 300723 | 0.3700714611 |
| 0.0189349112 | 0.0234693878 | 300722 | 0.3866328370 |
| 0.0234693878 | 0.0301602262 | 300719 | 0.4072872017 |
| 0.0301602262 | 0.0426919559 | 300725 | 0.4254618006 |
| 0.0426919559 | 2.4769833497 | 300723 | 0.4292288917 |


### instrument_group

| Value | Rows | Population share | Positive rate | Min exact-cohort size | Median exact-cohort size |
| --- | --- | --- | --- | --- | --- |
| STATIC_GROUP_0 | 148636 | 0.0494263476 | 0.3526130951 | 1 | 48.0000000000 |
| STATIC_GROUP_1 | 152479 | 0.0507042713 | 0.3559572138 | 1 | 52.0000000000 |
| STATIC_GROUP_2 | 108475 | 0.0360714972 | 0.3622954598 | 1 | 39.5000000000 |
| STATIC_GROUP_3 | 43461 | 0.0144522087 | 0.3651319574 | 1 | 30.0000000000 |
| UNKNOWN | 2554171 | 0.8493456752 | 0.3596188352 | 1 | 373.0000000000 |


### liquidity_bucket

| Value | Rows | Population share | Positive rate | Min exact-cohort size | Median exact-cohort size |
| --- | --- | --- | --- | --- | --- |
| UNVERIFIED | 3007222 | 1.0000000000 | 0.3592631339 | 1 | 67.5000000000 |


### room_to_target_R

Unique values: 1485722. Not used in probability estimation.

| Quantile | Value |
| --- | --- |
| max | 631.8000000004 |
| min | 1.0000000000 |
| p05 | 1.2556008569 |
| p25 | 1.5467625899 |
| p50 | 2.0222222222 |
| p75 | 2.9012702079 |
| p95 | 4.9047619048 |


| Lower | Upper | Population | Positive rate |
| --- | --- | --- | --- |
| 1.0000000000 | 1.3478260870 | 300708 | 0.4346342631 |
| 1.3478260870 | 1.5000000000 | 280073 | 0.4177946464 |
| 1.5000000000 | 1.6472203157 | 321383 | 0.4118357225 |
| 1.6472203157 | 1.8664563617 | 300725 | 0.3913542273 |
| 1.8664563617 | 2.0222222222 | 300708 | 0.3636351544 |
| 2.0222222222 | 2.3097826087 | 300646 | 0.3612953440 |
| 2.3097826087 | 2.6726973684 | 300785 | 0.3429526073 |
| 2.6726973684 | 3.1800000000 | 300746 | 0.3224149282 |
| 3.1800000000 | 4.0465032671 | 300725 | 0.2959647518 |
| 4.0465032671 | 631.8000000004 | 300723 | 0.2511680184 |


### setup_family

| Value | Rows | Population share | Positive rate | Min exact-cohort size | Median exact-cohort size |
| --- | --- | --- | --- | --- | --- |
| BREAKOUT_VOL_EXPANSION_V2 | 1602315 | 0.5328223191 | 0.3815504442 | 1 | 121.0000000000 |
| MOMENTUM_CONTINUATION_V1 | 129925 | 0.0432043261 | 0.4231979988 | 1 | 28.0000000000 |
| RANGE_MEAN_REVERSION_V2 | 1163925 | 0.3870432579 | 0.3232656743 | 1 | 125.5000000000 |
| TREND_PULLBACK_V2 | 111057 | 0.0369300969 | 0.3401766660 | 1 | 32.0000000000 |


### side

| Value | Rows | Population share | Positive rate | Min exact-cohort size | Median exact-cohort size |
| --- | --- | --- | --- | --- | --- |
| LONG | 1499588 | 0.4986622205 | 0.3462391003 | 1 | 65.0000000000 |
| SHORT | 1507634 | 0.5013377795 | 0.3722176603 | 1 | 74.0000000000 |


### trend_maturity

| Value | Rows | Population share | Positive rate | Min exact-cohort size | Median exact-cohort size |
| --- | --- | --- | --- | --- | --- |
| EARLY | 792465 | 0.2635206180 | 0.3725905876 | 1 | 169.5000000000 |
| LATE | 42709 | 0.0142021440 | 0.4601606219 | 1 | 11.0000000000 |
| MID | 22852 | 0.0075990399 | 0.3652634343 | 1 | 8.0000000000 |
| UNKNOWN | 2149196 | 0.7146781980 | 0.3522801085 | 3 | 1110.0000000000 |


### volatility_bucket

| Value | Rows | Population share | Positive rate | Min exact-cohort size | Median exact-cohort size |
| --- | --- | --- | --- | --- | --- |
| HIGH | 1113946 | 0.3704236003 | 0.3815355502 | 1 | 74.5000000000 |
| LOW | 924603 | 0.3074608393 | 0.3370138319 | 1 | 68.0000000000 |
| MEDIUM | 968673 | 0.3221155605 | 0.3548875627 | 1 | 62.0000000000 |


## Evidence and reproduction files

- `scripts/cati_calibration_diagnostics.py`: indexed probability replay and all cohort tables.
- `scripts/cati_label_audit.py`: bounded source-label reconstruction.
- `scripts/build_cati_causal_candidate.py`: immutable parent reuse and clean-provenance research identity.
- `data/research/calibration_diagnostics/v1_root_cause/`: summary.json, predictions.csv, reliability.csv, by_*.csv, walk_forward_folds.json, source_label_audit.json, original_fingerprints.json, notebook and plot.

The diagnostic directory is generated local evidence and excluded from Git; source and this report are tracked.

## Completed V2 correction candidate

Library: `cati_lib_80348290bf88005f4b2c151c`  
Hash: `80348290bf88005f4b2c151ce322dbb67f8d7a36095f9f244694315e548f3e55`  
Calibration policy hash: `d6baee27c12dd48d49cd3d60fc1164cf8752c3470ed676972234e0c2043ca8ae`  
Status: **RESEARCH_ONLY**  
Research decision: **REJECTED_PRE_HOLDOUT**

Model Brier: 0.22764573856574608; causal baseline: 0.22788403450766645; skill: 0.0010456894992016963; ECE: 0.010402743914891039. The same 1,998 probabilities, labels and backoff levels match V1 exactly. The row hash is unchanged. Original manifests and calibration JSON fingerprints match after the work. The actual V2 artifact is rejected by RUNTIME with incompatible library_version.

Additional diagnostic: against a causal per-family frequency baseline, skill is -0.00296765131360055. This is supplementary and does not replace the registered global causal benchmark.

V2 outputs: `data/research/calibration_diagnostics/v2_causal_baseline/`; candidate identity: `docs/research/cati_causal_baseline_candidate.json`. No claim of full certification replay or holdout completion is made.
