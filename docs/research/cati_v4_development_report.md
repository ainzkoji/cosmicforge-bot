# CATI V4 edge and payoff development result

**V4 is REJECTED_PRE_HOLDOUT / RESEARCH_ONLY. MODEL_READY = NO.** The nested procedure improves probability skill from V3’s 0.01852752 to 0.01928768, below the unchanged 0.02 requirement. All later folds improve on V3. The separate payoff estimator passes its predeclared development checks, but supplementary diagnostics show it is not ready for CATI decision logic. Governance remains M0; CATI execution is off; no library is pinned. No reserved holdout was opened, inspected or queried.

## Identity, evidence and bounded selection

- Candidate: `cati_v4_a71dc63a483d307c3b6cfbeb`.
- Full identity: `a71dc63a483d307c3b6cfbeb1d8d9649b46ae19e044e8120fc1927dd680f6d16`.
- Feature schema: `cati-v4-closed-context-geometry-1`.
- Evaluation/fitting source commit: `705d7b1f2638d70b72a2af7374def3abecba3310`; clean source tree at evaluation.
- Main start: `81aceaa6e91ef4f9bf1473cce47147bebde14680`; all work directly on main.
- Frozen parent rows, labels, cost arithmetic and dataset manifest remain unchanged. The cache preparation streamed and verified all 3,007,222 parent rows once; outcome-independent stride-12 sampling retained 250,602 rows. Outer predictions match V3’s 51,887 chronological indices.
- All previously inspected pre-holdout history is DEVELOPMENT evidence, not pristine final confirmation. Residual analysis preceded the registered search; no instruments or setup families were selected away.
- Registry: [fixed four-family budget](cati_v4_research_registry.json). Two C values (0.01, 0.1), two inner validation windows in each of five expanding outer prefixes: 80 inner fits, 20 outer family fits, five fixed payoff fits, and the final refits. Transformations, knots, vocabularies and selection use training prefixes only; labels must mature strictly before the cutoff.
- Inner Brier chooses C per family and then the family for each outer prefix. Final family/C comes from the last prefix’s inner selection: FAMILY_AWARE_GEOMETRY, C=0.01. Pooled outer results do not select the deployed artifact. There is no fitted recalibration layer.

## Residual diagnosis

**MIXED**: temporal discrimination drift, family-dependent geometry and instrument heterogeneity, with possible underfit nonlinearity and missing market context. These are observational hypotheses, not proof that a particular missing feature causes the errors.

V3 folds 1–2 versus 3–5: skill 0.0217530 → 0.0161421, ROC-AUC 0.58614 → 0.57485, positive rate 0.37322 → 0.35235. ECE improves 0.02245 → 0.01284 while ranking weakens, so recalibration alone does not explain the loss. Room residuals increase across breakout quintiles but decrease in range mean reversion, supporting regularized family slopes. Risk and cost burden are mechanically related; their diagnostics are not independent discoveries. Full residual summaries cover geometry, modeled cost, family, side, regime, volatility, instrument, calendar time and matured-prefix training age for every outer prediction.

See [residual findings](cati_v3_residual_findings_for_v4.md) and the published `v3_residual_summary.json`. No causal attribution or production subset selection is claimed.

## Probability results

| Metric | Nested V4 |
|---|---|
| samples | 51887 |
| brier | 0.2263114362700001 |
| causal_baseline_brier | 0.23076230669411518 |
| brier_skill | 0.019287683885110707 |
| ece | 0.010190272994330486 |
| roc_auc | 0.5808956822920436 |
| pr_auc | 0.4156980615694823 |
| log_loss | 0.6440821066256414 |
| top_decile_lift | 1.2368235617469732 |

| Fold | Inner-selected model | Skill | ECE | Positive rate | Prediction mean / std |
|---|---|---:|---:|---:|---:|
| 1 | CAUSAL_MARKET_CONTEXT | 0.02049202 | 0.030267 | 0.375277 | 0.346965 / 0.070925 |
| 2 | REGULARIZED_SPLINE_LOGISTIC | 0.02354299 | 0.016656 | 0.371179 | 0.354898 / 0.073611 |
| 3 | BOUNDED_HYBRID | 0.01653700 | 0.013552 | 0.358452 | 0.360190 / 0.081041 |
| 4 | BOUNDED_HYBRID | 0.01815023 | 0.012661 | 0.350723 | 0.354707 / 0.079798 |
| 5 | FAMILY_AWARE_GEOMETRY | 0.01716711 | 0.011653 | 0.347661 | 0.353450 / 0.073880 |

| Registered family | Candidate | Pooled skill | ECE | Fold 3 / 4 / 5 skill | Verdict |
|---|---|---:|---:|---|---|
| REGULARIZED_SPLINE_LOGISTIC | `cati_v4_family_ded3b2bb22ef254b23708d50` | 0.01883723 | 0.012236 | 0.01439413 / 0.01774028 / 0.01703072 | REJECTED |
| FAMILY_AWARE_GEOMETRY | `cati_v4_family_4e2e4f11ff6ce8b459e3d237` | 0.01973321 | 0.008891 | 0.01571416 / 0.01830325 / 0.01716711 | REJECTED |
| CAUSAL_MARKET_CONTEXT | `cati_v4_family_083627f32a666a79667900ea` | 0.01909901 | 0.012277 | 0.01572130 / 0.01769568 / 0.01586235 | REJECTED |
| BOUNDED_HYBRID | `cati_v4_family_7619ccfcd77bd72cdba2ea15` | 0.02011761 | 0.009360 | 0.01653700 / 0.01815023 / 0.01620297 | REJECTED |

**The hybrid’s 0.02011761 pooled skill does not rescue V4:** its last fold is 0.01620297, below V3’s 0.01663792. The predeclared later-fold robustness condition fails. Choosing it after reading the outer results would also violate nested selection. The nested procedure preserves all later-fold improvements but misses the pooled gate.

Fixed ten-bin decomposition reports descriptive discrimination gain (resolution increase) **0.0003878941** and calibration gain (reliability reduction) **0.0000194051**; fitted calibration-layer gain is **0**. Actual Brier improvement is 0.0001754166. The within-bin residual changes, so those descriptive components are not an exact additive attribution of Brier improvement.

## Causal market context and drift

Actual native 15m candles supply return-4/24, realized volatility-24, ATR-14/price, distance from MA-24, trend slope-24, log volume normalization-24, and BTC/ETH return-4/24. Fixed trailing windows require contiguous candles. Instrument and both benchmarks must align exactly to the last closed candle at the decision timestamp. Missing/stale/future context fails closed. Liquidity, funding crowding and HTF alignment remain omitted; UNKNOWN is not replaced by a neutral value. Market breadth and unavailable historical turnover are also omitted.

Source queries are read-only and bounded at candle close ≤ 1783832399999, strictly before reserved holdout start 1783876499999. Artifact provenance records all 136 source prefix hashes, source row counts and bounds, the immutable parent/dataset identity and cache SHA-256. No holdout data enters features, labels, selection or reporting.

Training-quantile population stability: room PSI stays 0.0018–0.0072; later risk/cost PSI is 0.0379–0.0646. ATR-fraction PSI rises to 0.1699/0.1751 in folds 4/5; realized-volatility PSI reaches 0.1700 in fold 5. These changes support temporal distribution shift without establishing causation.

All 20 coefficient/effect traces and fitted encoders are in the model artifact. Comparable within-family unstandardized shared slopes are in supplemental diagnostics. For family-aware geometry, the shared log-room slope varies −0.0909 to +0.0416 and log-risk −0.0689 to −0.0138. Hybrid room slopes vary −0.0220 to +0.2125. Correlated geometry and training-dependent spline bases make individual coefficients unsuitable for causal interpretation. Full geometry/context PSI, category breakdowns and inner attempts are preserved.

## Conditional payoff validation

The fixed payoff model uses the hybrid encoder, histogram gradient boosting (60 trees, ≤15 leaves, minimum leaf 200, L2=10, no early stopping) for positive/nonpositive net-R means, timeout gross-R and log1p MFE/MAE quantiles at 0.1/0.5/0.9; a C=0.01 multinomial logistic estimates TARGET/STOP/TIMEOUT. Each outer payoff fit uses only its matured training prefix; future payoffs are supervised targets, never predictors. Expected net-R = p(profit) × conditional positive net-R + (1−p(profit)) × conditional loss net-R. Original label and cost semantics are retained.

| Quantity | MAE | RMSE | Bias |
|---|---:|---:|---:|
| expected_net_R | 1.13763390 | 1.37544848 | -0.01021195 |
| causal_baseline_net_R | 1.15634181 | 1.38979093 | 0.00450570 |
| conditional_positive_net_R | 0.51262296 | 0.73176626 | 0.03287348 |
| conditional_loss_net_R | 0.14183804 | 0.35678170 | -0.00797135 |

Terminal multiclass Brier is **0.51280544**, versus causal frequency baseline **0.59911512**. Expected-R maximum pooled bucket absolute bias is **0.09263012 R**, within the predeclared 0.15 R bound. All five expected-R RMSEs improve on their causal mean baselines.

| Quantiles | Coverage 0.1 / 0.5 / 0.9 | 10–90 interval coverage |
|---|---|---:|
| MFE | 0.105576 / 0.507815 / 0.897219 | 0.791643 |
| MAE | 0.112841 / 0.521923 / 0.901979 | 0.789138 |

No raw predicted quantile crossings were observed in the outer folds. Sorting would use predictions only. Quantile calibration is pooled, not a guarantee of calibrated conditional distributions for every instrument or regime.

| Fold | Expected-R RMSE / causal baseline | Expectancy monotonic | Largest adjacent inversion R |
|---|---|---|---:|
| 1 | 1.396088 / 1.418813 | False | 0.083442 |
| 2 | 1.379979 / 1.395901 | True | 0.000000 |
| 3 | 1.362494 / 1.374394 | True | 0.000000 |
| 4 | 1.378990 / 1.392347 | True | 0.000000 |
| 5 | 1.357711 / 1.364444 | False | 0.060686 |

The registered payoff gate passes because its bucket/quantile/monotonicity conditions are pooled, with per-fold RMSE protection. **Folds 1 and 5 are not monotonic** (0.08344 R and 0.06069 R inversions). They must not be hidden by the pooled pass.

| Pooled expectancy quintile | Predicted net-R | Realized net-R | Samples |
|---|---:|---:|---:|
| 1 | -0.41576576 | -0.33573859 | 10378 |
| 2 | -0.20746548 | -0.14523910 | 10377 |
| 3 | -0.13236345 | -0.10152504 | 10377 |
| 4 | -0.07089809 | -0.10029701 | 10377 |
| 5 | 0.01425120 | -0.07837892 | 10378 |

All four families have their own predicted/realized payoff and bucket results in `family_results`. Pooled realized expectancy increases monotonically for the nested procedure. **The highest quintile predicts +0.01425 R but realizes −0.07838 R.** Improved ranking does not demonstrate a profitable trading edge.

**Additional joint-consistency blocker:** independent binary and terminal models give p(net profitable) > 1−p(STOP) on 16.4107% of outer predictions, with maximum excess 0.46549. Since a stop has negative net-R under frozen semantics, these cannot be marginals of one coherent joint outcome distribution. Supplemental per-class terminal calibration and conditional-mean bucket diagnostics are preserved. The registered development verdict is retained honestly; `decision_payoff_ready=false` separately records this blocker. Canonical forecasts flag `V4_JOINT_PAYOFF_NOT_VALIDATED`, have RESEARCH_ONLY status and uncertainty 1. Runtime loading remains rejected. Time-to-event distributions are also unmodeled. A future predeclared model must reconcile the joint terminal/profit/payoff distribution and temporal expectancy stability before decision use; no post-hoc adjustment was fit to these outer outcomes.

## Replay, resource use and tests

- Numeric JSON tree exports match sklearn predictions at 1e-11 during every fit. A float32 threshold comparison bug discovered during the first run was repaired using exact float64 comparison semantics, with an adjacent-value regression test. The initial attempt aborted before yielding a candidate; the complete accepted evaluation uses the repaired clean commit.
- Sparse categorical effects now avoid dense N×instrument matrices, and completed optimizer objects are released. Dense-reference tests cover all four model families and unknown categories.
- Fixed replay after optimization: all 20 outer probability traces reproduce exactly (maximum difference 0); final probability and payoff refits have identical serialized parameter hashes. No variants or scores were selected by this check.
- Full registered evaluation: 445.16 seconds, peak working set 2,903,564,288 bytes (2.70 GiB), one fit worker. Optimized fixed-final-refit + replay: 44.05 seconds, peak 1,112,264,704 bytes (1.04 GiB). Different workloads: the lower replay peak is not a measured rerun peak for the entire search.
- Artifact DEVELOPMENT load: 0.111 s. Parameter replay of 998 development feature rows: 0.128 s. This is throughput evidence, not historical out-of-sample scoring of the final fit or a live decision latency claim.
- Full relevant CATI suite: 1,127 passed in 411.63 s (eight existing LightGBM feature-name warnings). Final post-change V4/published-artifact/authority/adaptive risk/threshold/risk responsibility/runtime evidence checks: **173 passed in 21.24 s**. The original V1 manifest/interim-manifest/calibration fingerprints remain identical (3/3).

## Runtime, FX and remaining work

The healthy canonical paper runtime remains the same single owner PID 19436/session `rts_a605e6301df149ccb931`; lease status records startup revision 81aceaa6. It was not restarted to activate research changes. A health fingerprint reading current Git HEAD is not proof that startup-loaded modules changed. Scheduler, calendar and event services remain healthy, adaptive risk is enabled and daily hard-loss fraction is 0.025. M0 permits its established V2 demo path; CATI denial never falls back to V2.

The existing FX supervisor/writer and queued completion continue; no new writer was launched. Latest observed completion status is WAITING_ACQUISITION with 5,990 remaining pair/day periods (2026-10-02 22:23 UTC). When existing acquisition ends, queued completion derives 5m/15m/4h then QA/classifies gaps strictly; it freezes only if all checks pass. Open-session UNKNOWN gaps remain a separate data-freeze blocker. Acquisition/freeze completion is not claimed.

Remaining blockers: binary skill below 0.02 and hybrid late-fold degradation; joint payoff incoherence, non-monotonic temporal expectancy and negative top-bucket realized return; unmodeled event time; live causal-context provisioning/integration and explicit governance before any holdout access or pin. The context adapter is implemented and tested as an explicit research input contract, not auto-enabled in the running scheduler. Runtime and holdout boundaries remain unchanged.

Published numeric models, all evaluation metrics, source provenance, attempts and effect traces: [V4 artifact](artifacts/cati_v4_a71dc63a483d307c3b6cfbeb/v4_model.json). Replay/performance, supplemental diagnostics and residual summaries are alongside it. Local prediction arrays and preparation cache remain under `data/research/calibration_diagnostics`; no library-row reload is needed to replay.
