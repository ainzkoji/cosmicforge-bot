# CATI V5 coherent joint outcome development result

**Joint coherence is fixed by construction. V5 remains REJECTED_PRE_HOLDOUT / RESEARCH_ONLY. MODEL_READY = NO; DECISION_PAYOFF_READY = NO.** The nested procedure meets the frozen pooled numerical thresholds (skill 0.02009139, ECE 0.01304056, n=51,887), but fails the separately registered temporal guard in folds 3 and 5. The payoff estimator improves RMSE and passes its registered statistical checks; every fold’s top expectancy bucket nevertheless realizes a negative return. No runtime pin, authority, threshold, label, cost or holdout boundary changed.

## Identity and research boundary

- Candidate: `cati_v5_b131674954a223230581dbbc`; hash `b131674954a223230581dbbc9d988cf9a98842055986386f29b02376cd3c7e31`.
- Source fit/evaluation commit: `dd700907cc7211bb9749eb1c1aaa038ea12c65a8`, clean main at run start.
- Final generator replay commit: `600dc7ccc780e46841560ee36b7208aede9de1e2`; fitted numbers remain from `dd700907cc7211bb9749eb1c1aaa038ea12c65a8`. Generation policy: `CAUSAL_TERMINAL_SUPPORT_V5_3`.
- Feature schema: `cati-v5-joint-closed-context-geometry-1`.
- Main start: `03b462e6a87bee4b1765ba8ebb0e39b84c9aac89`. Work and commits directly on main.
- Existing V1–V4 and all inspected pre-holdout history remain DEVELOPMENT evidence. Outer predictions are untouched relative to their own inner fit; this is not pristine final confirmation. V4’s observed hybrid was neither promoted nor retroactively selected.
- Frozen parent 3,007,222 row identities were streamed and verified once when adding event times. The identical V4 stride-12 sample contains 250,602 rows and the identical 51,887 outer prediction indices. No family, instrument or universe subset was removed.
- Source context uses actual instrument/BTC/ETH 15m candles aligned exactly to the decision’s last closed candle. Native trailing windows, geometry, categorical family/side/regime/volatility/instrument effects and training-only spline/scaler/vocabulary are retained. Timeframe 15m and horizon 48 are validated constant domain fields. Missing/stale/future context fails closed. Liquidity, funding crowding and HTF alignment remain omitted.
- Parent/dataset/cache hashes and all 136 causal candle-prefix hashes/bounds are preserved in the artifact. Last context close is 1783832399999; reserved holdout begins 1783876499999. All label horizons end before the reserved holdout, and each training prefix uses only labels fully matured before its cutoff. Evaluation and replay contain no source database query.

## One joint outcome distribution

| State | Frozen development sample count | Final conditional-fit support |
|---|---:|---:|
| TARGET_PROFIT | 52281 | 10473 |
| TARGET_LOSS | 37 | 8 |
| STOP_LOSS | 137019 | 27390 |
| TIMEOUT_PROFIT | 37863 | 7476 |
| TIMEOUT_LOSS | 23402 | 4740 |

All five states are possible, including TARGET_LOSS after costs. STOP_PROFIT is impossible under the frozen contract and is rejected if supplied as a training label. The classifier’s four latent classes are TARGET, STOP, TIMEOUT_PROFIT and TIMEOUT_LOSS. Known room minus frozen modeled cost deterministically splits TARGET into profit/loss. When room cannot cover cost, profitable timeout is outside the causal payoff support: its logit is −infinity before the one softmax. There is no post-hoc clipping of profit or terminal probabilities.

The resulting five-state vector supplies both profit and terminal marginals. State-conditioned distributions use 101 equiprobable atoms. TARGET/STOP net payoff is the exact known room−cost / −1−cost formula. TIMEOUT payoff magnitudes are causally fitted bounded histogram means with empirical residual-ratio atoms. Expected net-R is the sum of state probability × conditional state atom mean. Conditional positive and loss means derive from the same weighted atoms. Gross payoff equals net payoff plus the unchanged modeled cost.

MFE/MAE distributions use state-conditioned bounded histogram means and empirical atoms. A shared atom index couples payoff and path draws: each path contains the gross terminal payoff, TARGET MFE reaches known room and STOP MAE reaches at least 1R. Marginal quantiles are computed from the weighted joint mixture. Rare states (<400 training samples) retain empirical conditional support rather than being excluded. Their small support remains a material uncertainty; broad pooled scores do not validate every rare state.

| Engineering requirement | Observed |
|---|---:|
| max_conditional_R_identity_error | 4.440892098500626e-16 |
| max_expected_R_identity_error | 0.0 |
| max_probability_sum_error | 4.440892098500626e-16 |
| max_profit_identity_error | 0.0 |
| max_terminal_identity_error | 0.0 |
| negative_probability_count | 0 |
| over_one_probability_count | 0 |
| profit_stop_incoherence_count | 0 |
| state_payoff_sign_violation_count | 0 |

**Profit/stop incoherence is 0%, compared with V4’s 16.4107%.** Probability normalization and conditional expectation differences are at floating-point roundoff (≤4.44e−16), within the registered 1e−12 engineering tolerance. Profit, terminal and state-weighted expected-R identities have zero measured error. No negative probabilities, probabilities over 1 or conditional payoff sign violations occur.

GEN3 enforces TIMEOUT net R strictly inside touch bounds, MFE below target room and MAE below stop distance, while containing the closing payoff. TARGET requires MFE at least target room; STOP requires MAE at least one R. A TARGET atom with a subsequent stop excursion cannot target on the final bar; its event-time prior is truncated accordingly. These dependencies use the same fitted means, empirical atoms, classifier coefficients and time priors. GEN2 fixed only the TIMEOUT closing-price bound; GEN3 completes path/censoring support. No relabeling or additional fit occurred.

## Registered search and temporal selection

[Registry](cati_v5_research_registry.json): three architectures, six declared variants but five unique fits. A: multinomial logistic C=0.01/0.1. B: fixed histogram classifier, 60 iterations, ≤15 leaves, minimum leaf 400, L2=20, no early stopping. C: multinomial C=0.01 with NO_DECAY / 365-day / 180-day half-life. NO_DECAY reuses A’s identical fit/inner scores. Recency weights depend only on decision timestamp distance from the training cutoff and are mean-normalized; no outcome quality enters weights.

Same five expanding outer folds and two inner validation windows per prefix. Fifty unique inner fit records, fifteen outer architecture fits and the fixed final refits; one fit worker. Inner pooled binary Brier derived from the joint vector selects regularization/recency/architecture. Final fit uses the last prefix’s inner choice: **A_001, JOINT_MULTINOMIAL_REGULARIZED, C=0.01, NO_DECAY**. Nothing is chosen using pooled outer scores.

| Architecture | Pooled skill | ECE | F1 / F2 / F3 / F4 / F5 skill | Model ready |
|---|---:|---:|---|---|
| JOINT_MULTINOMIAL_REGULARIZED | 0.02031994 | 0.01230375 | 0.02195307 / 0.02798503 / 0.01743901 / 0.01836296 / 0.01484874 | False |
| JOINT_HISTOGRAM_BOOSTING | 0.01372871 | 0.02486462 | 0.01223204 / 0.02038938 / 0.01054854 / 0.01399795 / 0.01100703 | False |
| RECENCY_AWARE_JOINT_MODEL | 0.02009139 | 0.01304056 | 0.02195307 / 0.02798503 / 0.01628508 / 0.01836296 / 0.01484874 | False |

The recency architecture selects NO_DECAY in folds 1/2/4/5 and 180-day decay in fold 3. Fold 3’s causal inner choice scores 0.01628508 outside the prefix, versus 0.01743901 for the no-decay architecture. This is evidence that the small recency experiment does not consistently rescue temporal generalization; it is not permission to substitute the outer winner. Every architecture fails the registered temporal guard. ATR-fraction PSI reaches 0.17514 and realized-volatility PSI 0.16996 in the final fold, consistent with distribution shift but not proof of its causal mechanism.

## Probability metrics

| Metric | Nested V5 |
|---|---|
| samples | 51887 |
| brier | 0.22612597058962955 |
| causal_baseline_brier | 0.23076230669411518 |
| brier_skill | 0.020091392614788206 |
| ece | 0.013040559716324028 |
| roc_auc | 0.5843046133571816 |
| pr_auc | 0.42028311916343686 |
| log_loss | 0.6437181324517623 |
| top_decile_lift | 1.2245460727870332 |

| Fold | Inner choice | n | Brier | Causal Brier | Skill | ECE | ROC-AUC | PR-AUC | Log loss | Top-decile lift |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | A_001 | 10840 | 0.22975686 | 0.23491394 | 0.02195307 | 0.02912747 | 0.58629313 | 0.43258800 | 0.65100602 | 1.17994100 |
| 2 | A_001 | 10895 | 0.22696558 | 0.23350009 | 0.02798503 | 0.01411148 | 0.60040254 | 0.45174279 | 0.64548691 | 1.36313794 |
| 3 | C_MEDIUM | 10311 | 0.22625001 | 0.22999550 | 0.01628508 | 0.01884261 | 0.57799513 | 0.42005303 | 0.64400600 | 1.21494467 |
| 4 | A_001 | 9814 | 0.22364722 | 0.22783087 | 0.01836296 | 0.01383067 | 0.58375112 | 0.40479790 | 0.63850736 | 1.20618598 |
| 5 | A_001 | 10027 | 0.22358694 | 0.22695696 | 0.01484874 | 0.01735309 | 0.57592666 | 0.39845716 | 0.63872149 | 1.23436718 |

| Fold | Positive rate | Prediction mean | Prediction std |
|---|---:|---:|---:|
| 1 | 0.37527675 | 0.34814838 | 0.08200083 |
| 2 | 0.37117944 | 0.35711512 | 0.08193697 |
| 3 | 0.35845214 | 0.36273998 | 0.09009945 |
| 4 | 0.35072346 | 0.35515631 | 0.08592483 |
| 5 | 0.34766131 | 0.35465293 | 0.08279371 |

**Frozen pooled threshold check: PASS. Registered temporal admission: FAIL.** Fold 3 is below V4’s 0.01653700, and fold 5 is below V4’s 0.01716711. Fold 5 degrades by approximately 13.5% relative to V4. Accordingly MODEL_READY is NO even though pooled skill narrowly exceeds 0.02. No gate or selection rule is relaxed.

Fixed ten-bin descriptive resolution gain versus V4 is +0.00035543; reliability gain is −0.00011338 (calibration worsens slightly). ECE remains below 0.05. No calibration layer was fit. The within-bin residual changes, so these components are not an exact additive decomposition of the observed Brier improvement.

## Payoff and expectancy validation

The registered statistical payoff checks PASS. Decision payoff readiness FAILS: every fold and the pooled top expectancy quintile realize negative net R. Four folds have small adjacent reversals; all remain within the registered 0.05 R inversion allowance. Neither improved RMSE nor pooled ordering establishes profitable decisions.

| Quantity | MAE | RMSE | Bias |
|---|---:|---:|---:|
| expected_net_R | 1.13177561 | 1.35838222 | -0.01280052 |
| causal_baseline_net_R | 1.15634181 | 1.38979093 | 0.00450570 |
| conditional_positive_net_R | 0.48918125 | 0.71843956 | 0.03932589 |
| conditional_loss_net_R | 0.12646301 | 0.23636814 | -0.01332027 |

Terminal multiclass Brier 0.51239254 versus causal frequency baseline 0.59911512. Maximum pooled quintile bias 0.09112145 R.

| Path quantity | q10 / q50 / q90 coverage | 10�90 interval coverage |
|---|---|---:|
| MAE | 0.11659953 / 0.54079056 / 0.90822364 | 0.79162411 |
| MFE | 0.10576830 / 0.51837647 / 0.90196003 | 0.79621100 |

| Fold | Expected-R RMSE | Causal baseline RMSE | Strict monotonicity | Largest inversion R | Top predicted R | Top realized R |
|---|---:|---:|---|---:|---:|---:|
| 1 | 1.33826994 | 1.41881330 | False | 0.02092575 | 0.01155892 | -0.05016104 |
| 2 | 1.37437089 | 1.39590100 | True | 0.00000000 | 0.00374999 | -0.00118328 |
| 3 | 1.35112340 | 1.37439425 | False | 0.01033714 | 0.04931068 | -0.08269288 |
| 4 | 1.37601937 | 1.39234681 | False | 0.04984637 | 0.01297104 | -0.07264224 |
| 5 | 1.35254664 | 1.36444426 | False | 0.01146148 | 0.00461336 | -0.10185519 |

| Fold / pooled | Quintile | Predicted mean R | Realized mean R | Samples |
|---|---:|---:|---:|---:|
| 1 | 1 | -0.43888934 | -0.33183057 | 2168 |
| 1 | 2 | -0.21628669 | -0.12170480 | 2168 |
| 1 | 3 | -0.15267054 | -0.05167611 | 2168 |
| 1 | 4 | -0.09163887 | -0.02923530 | 2168 |
| 1 | 5 | 0.01155892 | -0.05016104 | 2168 |
| 2 | 1 | -0.40465413 | -0.31385302 | 2179 |
| 2 | 2 | -0.18498476 | -0.17755564 | 2179 |
| 2 | 3 | -0.11546741 | -0.09046548 | 2179 |
| 2 | 4 | -0.06674246 | -0.04442560 | 2179 |
| 2 | 5 | 0.00374999 | -0.00118328 | 2179 |
| 3 | 1 | -0.43497354 | -0.31416917 | 2062 |
| 3 | 2 | -0.21478217 | -0.14403118 | 2062 |
| 3 | 3 | -0.11318533 | -0.15436833 | 2063 |
| 3 | 4 | -0.04366460 | -0.11850660 | 2061 |
| 3 | 5 | 0.04931068 | -0.08269288 | 2063 |
| 4 | 1 | -0.44132847 | -0.39101631 | 1963 |
| 4 | 2 | -0.21666135 | -0.21324181 | 1963 |
| 4 | 3 | -0.13493254 | -0.06574212 | 1962 |
| 4 | 4 | -0.07365558 | -0.11558848 | 1963 |
| 4 | 5 | 0.01297104 | -0.07264224 | 1963 |
| 5 | 1 | -0.43250351 | -0.36544587 | 2006 |
| 5 | 2 | -0.21901394 | -0.19142835 | 2005 |
| 5 | 3 | -0.13657224 | -0.13902733 | 2005 |
| 5 | 4 | -0.07794396 | -0.15048880 | 2005 |
| 5 | 5 | 0.00461336 | -0.10185519 | 2006 |
| POOLED | 1 | -0.43079842 | -0.33967696 | 10378 |
| POOLED | 2 | -0.21075134 | -0.16353431 | 10377 |
| POOLED | 3 | -0.13126781 | -0.09674736 | 10377 |
| POOLED | 4 | -0.07040695 | -0.09515896 | 10377 |
| POOLED | 5 | 0.01804069 | -0.06606187 | 10378 |

Every architecture�s fold and pooled errors, bucket tables, quantile coverage and timing scores are retained in the numeric artifact.

## Event-time model and validation

A causal state-conditioned 48-bin empirical distribution with Dirichlet 0.5/bin smoothing estimates first-event timing. Frozen first-touch label indices are zero-based; the model converts to elapsed bars 1–48. Canonical target/stop quantile fields retain zero-based label units; the added joint event-time mass declares elapsed-bar units explicitly. TIMEOUT places its mass at administrative horizon censoring 48, not at an invented target/stop event. Joint event-time mass is state probability × state-conditional duration probability.

Validation scores time distributions conditional on the observed terminal type (evaluation only; the outcome never enters forecast inputs). Target/stop event rows: 39,142; timeout censoring rows: 12,745. The causal comparator is the matured-prefix terminal-specific empirical duration frequency. Time readiness compares MAE and CRPS within a registered 5% noninferiority margin pooled and per fold; it is separate from binary admission.

| Fold | Time MAE / baseline (bars) | CRPS / baseline | Log loss / baseline | q10 / q50 / q90 coverage |
|---|---|---|---|---|
| 1 | 10.118342 / 10.127485 | 6.622210 / 6.625407 | 3.571062 / 3.571634 | 0.151174 / 0.530408 / 0.910012 |
| 2 | 10.095978 / 10.116066 | 6.611763 / 6.616653 | 3.557688 / 3.557452 | 0.163930 / 0.544935 / 0.903442 |
| 3 | 10.343134 / 10.341005 | 6.826553 / 6.830897 | 3.587687 / 3.588263 | 0.162197 / 0.513925 / 0.896467 |
| 4 | 10.175187 / 10.173230 | 6.740921 / 6.745057 | 3.581693 / 3.581014 | 0.162819 / 0.506711 / 0.901879 |
| 5 | 10.132753 / 10.141136 | 6.685390 / 6.689327 | 3.581373 / 3.580362 | 0.155975 / 0.511478 / 0.905287 |
| POOLED | 10.171812 / 10.178761 | 6.695405 / 6.699508 | 3.575578 / 3.575435 | 0.159190 / 0.521997 / 0.903480 |

Registered relative-baseline time checks PASS. Timing is near the empirical frequency baseline, not a demonstrated feature-conditioned timing edge or a fitted survival model. Discrete first-bar mass makes nominal quantile coverage coarse. Time distributions are causally fitted and bounded, with censoring reported explicitly; decision readiness remains NO for the reasons above.

## Engineering attempts, exact replay and memory

- The original event preparation stopped before producing a cache when it detected zero-based label indices. Correcting the unit conversion preserved the labels; the completed stream verified all parent rows.
- An initial evaluation was aborted after 30 completed inner records and two outer folds to enforce terminal/path support constraints. No candidate was produced. The registry’s physical support rule was made explicit before the complete evaluation; architecture, regularization, decay, sampling and selection were unchanged. All 30 original inner probability records reproduce exactly. This technical attempt and its inspection history are retained; none of this development history is described as pristine.
- The first complete supported evaluation used source bc55c377 and produced research candidate cati_v5_a3f02633b0dabb8683fdcd08. It is the same numerical experiment, not an outer-selected alternative.
- The final repeat verifies memory changes only: shared category string references, read-only memmaps, sparse effects, released fit objects, streamed numeric model components with SHA-256 identity, and streamed manifest serialization. All 50 inner records, five fold reports, three architecture result sets and all 68 prediction arrays are bit-exact against the first complete run. No new hyperparameter or architecture was tried. There were 50 completed inner fits in each full run, in addition to the 30 completed records of the aborted technical run; the repeated computation is disclosed rather than hidden in the search budget.
- First complete V5 peak: 2,171,478,016 bytes (2.02 GiB), 609.93 s. Final complete repeat: 1,879,846,912 bytes (**1.75 GiB**), 625.09 s, one fitting worker. This improves on V4’s 2.70 GiB, but **the <1.5 GiB target was not met**. No numeric precision, sample set or correctness check was weakened to claim a lower peak.
- Compressed numeric components are plain JSON, not executable pickle/joblib. Component SHA-256 and root candidate identity are checked. Runtime mode rejects V5 regardless of statistical readiness.
- GEN2 and final GEN3 support replays each performed zero new fits. All fitted conditional numbers and classifier parameters are unchanged; all probability vectors are bit-exact. The final repair changes expected R on only 56 selected rows and yields zero net-R support violations. GEN3 repair peak 688,836,608 bytes (0.64 GiB), 63.10 s.
- Final serialized GEN3 replay reproduces all 15 outer classifiers and 51,887 selected joint/payoff/path forecasts exactly, including nested maturity and runtime rejection. Peak 345,088,000 bytes (0.32 GiB), 24.86 s.
- Archived candidates cati_v5_2130b8e979e2c5d27ea97cf8 (GEN1) and cati_v5_42975f7898e66a0ef5a12fda (GEN2) are superseded research benchmarks, never runtime eligible. Their versioned generators remain reproducible. Performance/memory-parity files beside the final artifact describe the original full-fit benchmark, not a new GEN3 fit.
- Final regression: **1,162 CATI tests passed in 389.48 s**, eight existing LightGBM feature-name warnings. Additional hard-risk and authority suites: **151 passed** (117 plus 34). Final exact serialized replay passed.

## Runtime, FX and remaining blockers

The canonical paper runtime remains owner PID 19436/session rts_a605e6301df149ccb931, startup-loaded revision 81aceaa6. It was not restarted, stopped or reconfigured for this research. Current Git HEAD in a health fingerprint is not evidence that startup-loaded modules changed. Lease/scheduler/calendar/event services remain healthy; adaptive daily hard-loss cap is 0.025. Governance is M0, CATI authority/execution OFF, no library pin. The established V2 demo path at M0 is distinct from fallback; scoped CATI denial never falls back to V2.

FX continues under existing supervisor PID 31396 and queued completion PID 35444. Its provider circuit breaker stopped one acquisition run after six consecutive failures; the same supervisor waited 600 seconds and resumed with one redirected venv writer. Latest recorded remaining periods: 5,564 (2026-10-02 23:53 UTC). No additional supervisor/writer was launched by this task. Completion remains acquisition → derive 5m/15m/4h → QA → strict gap classification → freeze only if PASS. UNKNOWN_GAP rules were not weakened; no acquisition/freeze completion is claimed.

Remaining blockers: temporal probability admission (folds 3 and 5); negative top-bucket realized expectancy and temporal reversals; limited rare-state support and empirical timing/context integration evidence; unmet 1.5 GiB memory target; untouched reserved holdout and explicit governance before any pin/authority; separate FX completion/strict freeze dependency. No holdout was opened, inspected or queried. There is no admission checkpoint to authorize because both readiness flags remain NO.

Published [root artifact](artifacts/cati_v5_b131674954a223230581dbbc/v5_model.json) contains all metrics, attempts, prefix provenance and component hashes; its sibling files contain the final and all outer numeric models plus memory parity evidence. [Structured result](cati_v5_development_result.json). Local preparation/prediction arrays remain reusable under `data/research/calibration_diagnostics`; no additional 3M-row reload is necessary.
