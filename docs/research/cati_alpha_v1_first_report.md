# CATI alpha V1 — first bounded design and evaluation

**Economic viability FAIL. Next CATI model training is blocked. CATI is not live ready.**

The numbers below describe the union of all four available hypotheses (1,469 overlapping hypothetical opportunities). This union is diagnostic, never a selected strategy or executable portfolio. Every hypothesis is assessed independently below. Costs and execution differ from the frozen baseline (next-open entry, gap-aware stop, conservative funding reserve); the samples and horizon domains also differ. This is not a matched proof of improvement over V5.

```text
CURRENT_CATI_GENERATOR = Historical baseline: BREAKOUT_VOL_EXPANSION_V2, MOMENTUM_CONTINUATION_V1, RANGE_MEAN_REVERSION_V2, TREND_PULLBACK_V2 (unchanged)
ROOT_CAUSE_CONFIRMED = YES: audit matched gross -0.0083164315 R / net -0.1522378435 R; no stable positive gross alpha established
NEW_ALPHA_GENERATOR_ID = CATI_ALPHA_CONFIRMED_STRUCTURE_V1
NEW_SETUP_FAMILIES = BREAKOUT_ACCEPTANCE_V1; TREND_RECLAIM_V1
ENTRY_CHANGES = Two-bar acceptance after contraction; pullback structure reclaim; trend, volume and exact closed BTC/ETH alignment
STOP_CHANGES = 8-bar structural extreme plus 0.25 ATR buffer, minimum 1.5 ATR; minimum risk fraction 0.003
TARGET_CHANGES = 3 ATR from entry; reject room below 1.25 R; no partials/trailing/breakeven hypothesis in this budget
COST_AWARENESS = Pre-candidate maximum estimated 0.15 R; modeled taker fee/spread/slippage/funding retained, no cost reduction
TIMEFRAME_HYPOTHESES = 5m (unavailable); 15m (native); 1h (4 complete 15m bars)
HORIZON_HYPOTHESES = 5m/72 bars (6h); 15m/32 bars (8h); 1h/16 bars (16h)
DEVELOPMENT_GROSS_R = -0.040923573
DEVELOPMENT_NET_R = -0.138137635
FOLD_1_GROSS_R = -0.083495496
FOLD_2_GROSS_R = 0.011478910
FOLD_3_GROSS_R = -0.118220673
FOLD_4_GROSS_R = 0.044999395
FOLD_5_GROSS_R = -0.059019466
FOLD_1_NET_R = -0.177384968
FOLD_2_NET_R = -0.087148637
FOLD_3_NET_R = -0.214059743
FOLD_4_NET_R = -0.052333355
FOLD_5_NET_R = -0.161507196
ECONOMIC_VIABILITY_GATE = FAIL
NEXT_CATI_MODEL_TRAINING_ALLOWED = NO
CATI_AUTHORITY = OFF
LEGACY_V2_AUTHORITY_USED = NO
HOLDOUT_OPENED = NO
HOLDOUT_QUERY_COUNT = 0
DEVELOPMENT_RANGE_ADAPTIVELY_INSPECTED = YES
FX_STATUS = STALLED: latest completion snapshot has 3103 remaining pair/day periods; provider failures; no worker visible; no writer started
RUNTIME_STATUS = STOPPED / STALE_LEASE at inspection. M0 governance would grant V2 entries on restart; runtime left stopped to respect CATI-only authority and preserve the router/runtime.
TESTS = 1176 CATI regression tests passed (8 existing LightGBM feature-name warnings); final 8 alpha tests passed separately; compileall and diff checks passed
MAIN_COMMIT = Implementation commit is identified in the delivery message and git history
REMOTE_MAIN_PUSHED = Push confirmation is recorded in the delivery message
```

## Fixed design, source timestamps and invalidity

Research generator only, implemented under CATI setups. The existing specialist registry, authority router, dispatcher, V5 joint distribution, broker/execution paths, portfolio exposure and 2.5% hard daily-loss policy are unchanged. No model is fitted and no setup is registered for runtime use.

Two mechanisms × three domains = six declared hypotheses, one completed economic evaluation, no outcome-based parameter changes. An initial attempt aborted before any labels because 5m data was absent. The second attempt records this as unavailable and derives 1h bars from complete 15m groups. Both attempts are disclosed in summary.json.

- Side: long when the 12-bar mean exceeds the 48-bar mean, otherwise short. Require signed 8-bar slow-mean slope ≥0.25 ATR, signed 12-bar return >0, prior-24-bar volume ratio ≥1.1, and distance from fast mean ≤2 ATR.
- Breakout acceptance: previous and current closes beyond the prior 24-bar extreme (excluding both trigger bars); current wick holds within 0.25 ATR of the boundary; preceding 8/24 true-range compression ≤0.85; body displacement ≥0.25 ATR.
- Trend reclaim: previous wick reaches its fast mean, then a directional candle closes beyond the previous high/low; displacement ≥0.35 ATR.
- BTC and ETH signed 12-bar returns must align at the exact instrument candle close. No stale/as-of substitution. No order-book, actual funding, market breadth, relative-strength ranking or unavailable context is fabricated.
- ATR uses 14 closed true ranges. Initial risk is max(distance to 8-bar swing plus 0.25 ATR, 1.5 ATR). Target is 3 ATR from the reference close, with at least 1.25 R room. Reject risk fraction <0.003, cost/R >0.15, or nonpositive levels.
- Invalid/nonfinite OHLCV, nonpositive prices, negative volume, unordered/unaligned timestamps and future input fail closed. A missing bar invalidates 80-bar warmup. Incomplete future paths or fold-boundary maturity are excluded and counted.
- Each setup version contains family, timeframe and horizon. Policy identity hashes generator, geometry and cost assumptions; source code SHA-256 also freezes exact entry rules. Labels have their own policy version and candidate/path identity.

Candidates use the last closed candle reference; research execution occurs at the next bar open. Gap deviations enter gross R. Stops fill at the worse of stop/open; targets never claim favorable gap improvement; simultaneous stop/target touches resolve to stop. Terminal-bar MFE/MAE are censored OHLC bounds, not exact event-path training targets.

Costs inherit the audit assumptions: fee 4 bps per side, half-spread 1 bp per side, slippage 2 bps per side, funding 1 bp per 8h. Admission reserves ceil(full horizon/8h) funding stamps; labels retain this reserve even for early exits. Fees/spread/slippage apply to entry plus exit notional. These are disclosed historical model assumptions, not current account-specific venue quotes.

The fixed feasibility panel is ADA, BNB, BTC, DOGE, ETH, LINK, SOL and XRP. Selection was not based on profitable audited cells. It is still a small surviving-instrument panel on adaptively inspected development data; no whole-universe or untouched-confirmation claim is made. Source SQL is read-only, bounded below the inherited reserved holdout cutoff 1783876499999 ms. Labels also mature below each fold end. Five test windows follow a seed window over the same calendar range used by V5.

## All hypotheses and chronological folds

| Setup version | Fold | n | Gross R | Net R | Cost R | TARGET | STOP | TIMEOUT | Profit |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| BREAKOUT_ACCEPTANCE_V1_5m_H72 | 1 | 0 | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| BREAKOUT_ACCEPTANCE_V1_5m_H72 | 2 | 0 | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| BREAKOUT_ACCEPTANCE_V1_5m_H72 | 3 | 0 | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| BREAKOUT_ACCEPTANCE_V1_5m_H72 | 4 | 0 | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| BREAKOUT_ACCEPTANCE_V1_5m_H72 | 5 | 0 | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| TREND_RECLAIM_V1_5m_H72 | 1 | 0 | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| TREND_RECLAIM_V1_5m_H72 | 2 | 0 | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| TREND_RECLAIM_V1_5m_H72 | 3 | 0 | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| TREND_RECLAIM_V1_5m_H72 | 4 | 0 | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| TREND_RECLAIM_V1_5m_H72 | 5 | 0 | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| BREAKOUT_ACCEPTANCE_V1_15m_H32 | 1 | 9 | -0.246733748 | -0.361984612 | 0.115250864 | 0.222222222 | 0.555555556 | 0.222222222 | 0.333333333 |
| BREAKOUT_ACCEPTANCE_V1_15m_H32 | 2 | 11 | 0.509407040 | 0.393111219 | 0.116295821 | 0.636363636 | 0.363636364 | 0.000000000 | 0.636363636 |
| BREAKOUT_ACCEPTANCE_V1_15m_H32 | 3 | 7 | 0.047179327 | -0.060476342 | 0.107655669 | 0.428571429 | 0.428571429 | 0.142857143 | 0.428571429 |
| BREAKOUT_ACCEPTANCE_V1_15m_H32 | 4 | 5 | 0.202978600 | 0.104283109 | 0.098695490 | 0.400000000 | 0.400000000 | 0.200000000 | 0.600000000 |
| BREAKOUT_ACCEPTANCE_V1_15m_H32 | 5 | 3 | -1.017749436 | -1.126048454 | 0.108299018 | 0.000000000 | 1.000000000 | 0.000000000 | 0.000000000 |
| TREND_RECLAIM_V1_15m_H32 | 1 | 229 | -0.047468836 | -0.154145975 | 0.106677139 | 0.305676856 | 0.510917031 | 0.183406114 | 0.393013100 |
| TREND_RECLAIM_V1_15m_H32 | 2 | 200 | -0.006927317 | -0.116686404 | 0.109759087 | 0.325000000 | 0.505000000 | 0.170000000 | 0.415000000 |
| TREND_RECLAIM_V1_15m_H32 | 3 | 184 | -0.171070309 | -0.282056349 | 0.110986041 | 0.271739130 | 0.592391304 | 0.135869565 | 0.347826087 |
| TREND_RECLAIM_V1_15m_H32 | 4 | 167 | 0.059961805 | -0.043868110 | 0.103829915 | 0.329341317 | 0.425149701 | 0.245508982 | 0.437125749 |
| TREND_RECLAIM_V1_15m_H32 | 5 | 92 | -0.033765233 | -0.151896303 | 0.118131070 | 0.326086957 | 0.532608696 | 0.141304348 | 0.380434783 |
| BREAKOUT_ACCEPTANCE_V1_1h_H16 | 1 | 2 | 0.119937210 | 0.070314959 | 0.049622251 | 0.000000000 | 0.000000000 | 1.000000000 | 1.000000000 |
| BREAKOUT_ACCEPTANCE_V1_1h_H16 | 2 | 3 | -0.401761330 | -0.474427861 | 0.072666531 | 0.000000000 | 0.666666667 | 0.333333333 | 0.333333333 |
| BREAKOUT_ACCEPTANCE_V1_1h_H16 | 3 | 3 | 0.428471681 | 0.363172559 | 0.065299122 | 0.333333333 | 0.000000000 | 0.666666667 | 0.666666667 |
| BREAKOUT_ACCEPTANCE_V1_1h_H16 | 4 | 0 | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| BREAKOUT_ACCEPTANCE_V1_1h_H16 | 5 | 2 | -0.421508981 | -0.536047687 | 0.114538705 | 0.000000000 | 0.500000000 | 0.500000000 | 0.500000000 |
| TREND_RECLAIM_V1_1h_H16 | 1 | 110 | -0.148839372 | -0.215164356 | 0.066324983 | 0.263636364 | 0.527272727 | 0.209090909 | 0.354545455 |
| TREND_RECLAIM_V1_1h_H16 | 2 | 116 | 0.006683708 | -0.071747494 | 0.078431202 | 0.232758621 | 0.456896552 | 0.310344828 | 0.448275862 |
| TREND_RECLAIM_V1_1h_H16 | 3 | 106 | -0.052876750 | -0.122506961 | 0.069630211 | 0.216981132 | 0.349056604 | 0.433962264 | 0.367924528 |
| TREND_RECLAIM_V1_1h_H16 | 4 | 113 | 0.015896576 | -0.071773870 | 0.087670446 | 0.292035398 | 0.460176991 | 0.247787611 | 0.398230088 |
| TREND_RECLAIM_V1_1h_H16 | 5 | 107 | -0.047077601 | -0.135726798 | 0.088649196 | 0.242990654 | 0.448598131 | 0.308411215 | 0.392523364 |

| Setup version | n | Pooled gross R | Pooled net R | Gate |
| --- | ---: | ---: | ---: | --- |
| BREAKOUT_ACCEPTANCE_V1_5m_H72 | 0 | N/A | N/A | FAIL |
| TREND_RECLAIM_V1_5m_H72 | 0 | N/A | N/A | FAIL |
| BREAKOUT_ACCEPTANCE_V1_15m_H32 | 35 | 0.047850677 | -0.063248637 | FAIL |
| TREND_RECLAIM_V1_15m_H32 | 872 | -0.042231128 | -0.151187513 | FAIL |
| BREAKOUT_ACCEPTANCE_V1_1h_H16 | 10 | -0.052301249 | -0.126523136 | FAIL |
| TREND_RECLAIM_V1_1h_H16 | 552 | -0.044280700 | -0.122481415 | FAIL |

## Admission and drift

Each hypothesis must independently have ≥300 candidates and ≥60 observed UTC days in every fold, positive gross expectancy in every fold, positive day-clustered 95% net lower bound in every fold, and pooled net expectancy positive at twice modeled costs. Missing data, empty folds, low support or recent deterioration fail admission. No pooled result can override a failed fold. The normal day-cluster interval is descriptive and does not remove serial dependence or multiplicity; even a future pass would still require broader validation, coherent CATI forecast/calibration, pre-holdout certification and explicit holdout authorization.

Both reclaim hypotheses are gross-negative overall and net-negative in every fold; fold 3 deteriorates substantially. The pooled diagnostic union is gross-positive only in folds 2 and 4, but every fold loses net. The 15m breakout has a positive gross average but only 35 samples, a negative net average and three stop losses in the latest fold. The 1h breakout has only 10 samples and an empty fourth fold. These samples do not support a credible regime-specific claim. Report.json includes counts, cost burden, direction mix, uncertainty and all per-instrument diagnostics. No profitable cell is promoted.

The generator hypotheses are rejected for promotion; no next forecast model is trained. A revised hypothesis needs a new identity and declared budget, never retuned V1 definitions. The reserved holdout stays closed. Demo, forward demo and explicit production approval remain separate later requirements; legacy V2 is absent from that progression.

## Evidence and reproduction

Saved evidence: [report](artifacts/cati_alpha_v1/report.json), [registry](cati_alpha_v1_registry.json), [summary and attempts](artifacts/cati_alpha_v1/summary.json), [full fresh labels](artifacts/cati_alpha_v1/labels.jsonl.gz), [operational observations](artifacts/cati_alpha_v1/operations.json). The report records source prefix hashes, source/evaluator code hashes, label hash, baseline revision and dirty-tree disclosure.

```powershell
& backends/venv/Scripts/python.exe scripts/evaluate_cati_alpha.py --database backends/shared/shared_lib/persistence/cosmicforge.db --registry docs/research/cati_alpha_v1_registry.json --output <fresh-output-directory>
& backends/venv/Scripts/python.exe scripts/render_cati_alpha_report.py --artifact docs/research/artifacts/cati_alpha_v1 --tests "<test-result>" --output docs/research/cati_alpha_v1_first_report.md
```

Reproduction runs are not additional tuning attempts; preserve their identities. No source writer, holdout registry mutation, runtime promotion or production activation occurs in either tool.
