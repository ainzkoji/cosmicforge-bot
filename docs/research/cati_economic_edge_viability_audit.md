# CATI economic edge viability audit — existing V5, no V6

**PRIMARY_DIAGNOSIS = MIXED.** Weak/slightly negative gross candidate economics, substantial cost/geometry burden, temporal deterioration and optimistic high-score payoff estimates act together. The existing V5 evidence does not establish a realistic, reliably selectable positive net trading edge. No V6 is built.

## Population, boundaries and reproducibility

The primary matched ranking/economics population is the exact **51,887 saved nested outer predictions**. Pooled tails sort saved scores globally; fold tails sort within each fold. Fixed tails take ceil(n × fraction), with stable saved order resolving ties. All seven percentages and all eight fixed thresholds are reported; empty cells remain empty. No best percentage/threshold is chosen.

Separately, raw economics covers **all 3,007,222 frozen parent candidates**, including 2,494,072 in the outer calendar windows, plus the 250,602-row stride-12 development sample. Unsampled parent rows receive no invented predictions. Final POOLED fields use the matched outer population for comparability with V5 scores; full-label differences are shown below.

Exact frozen label fee/spread/slippage/funding/carry fields are used. Parent row hashes, cache identities, saved Brier/RMSE replay, joint weighted-payoff identity, fold maturity and label-horizon boundaries are verified. Holdout cutoff remains 1783876499999; no source-price/database query, model fit, output change, threshold optimization or production subset is part of this audit.

Candidate returns overlap across setups, instruments and time. They are hypothetical per-candidate R, not executable portfolio P&L, fills, turnover or capacity. UTC-day-clustered normal intervals handle simultaneous rows but not all serial/instrument dependence; no multiplicity adjustment is claimed.

**DEVELOPMENT_RANGE_ADAPTIVELY_INSPECTED = YES.** V1–V5 and this audit have inspected this range. No portion is pristine confirmation; positive development cells are not independent validation.

```powershell
& C:/Projects/cosmicforge-bot/backends/venv/Scripts/python.exe scripts/audit_cati_economic_edge.py --full-parent-raw --output data/research/calibration_diagnostics/economic_edge_audit_reproduction
& C:/Projects/cosmicforge-bot/backends/venv/Scripts/python.exe scripts/render_cati_economic_audit.py --input data/research/calibration_diagnostics/economic_edge_audit_reproduction/audit.json --output data/research/calibration_diagnostics/economic_edge_audit_reproduction/report.md
```

Fresh output directories are required. `--cost-cache` can reuse a SHA-verified identical extraction. [Complete evidence and input/tool identities](artifacts/cati_v5_economic_edge_audit/audit.json); [assessment](artifacts/cati_v5_economic_edge_audit/assessment.json). The renderer supplies interpretation for this fixed V5 audit, not an automatic model admission decision.

## Raw economics — matched outer population

| Population | n | Gross mean | Gross median | Cost mean | Net mean | Net median | Profitable |
| --- | --- | --- | --- | --- | --- | --- | --- |
| POOLED | 51887 | -0.008316 | -1.000000 | 0.143921 | -0.152238 | -1.045983 | 0.361092 |
| FOLD_1 | 10840 | 0.007758 | -1.000000 | 0.124680 | -0.116922 | -1.035663 | 0.375277 |
| FOLD_2 | 10895 | 0.012425 | -1.000000 | 0.137922 | -0.125497 | -1.046324 | 0.371179 |
| FOLD_3 | 10311 | -0.015824 | -1.000000 | 0.146926 | -0.162749 | -1.045705 | 0.358452 |
| FOLD_4 | 9814 | -0.015748 | -1.000000 | 0.155909 | -0.171657 | -1.054184 | 0.350723 |
| FOLD_5 | 10027 | -0.033238 | -1.000000 | 0.156420 | -0.189658 | -1.053757 | 0.347661 |

| Population | TARGET freq | STOP freq | TIMEOUT freq | TARGET net R | STOP net R | TIMEOUT net R |
| --- | --- | --- | --- | --- | --- | --- |
| POOLED | 0.209667 | 0.544703 | 0.245630 | 1.941254 | -1.172237 | 0.322705 |
| FOLD_1 | 0.214945 | 0.531365 | 0.253690 | 1.926303 | -1.157436 | 0.331315 |
| FOLD_2 | 0.210831 | 0.543919 | 0.245250 | 1.965691 | -1.165958 | 0.384351 |
| FOLD_3 | 0.207448 | 0.544758 | 0.247794 | 1.911658 | -1.171378 | 0.317999 |
| FOLD_4 | 0.208376 | 0.550744 | 0.240880 | 1.978691 | -1.185130 | 0.285347 |
| FOLD_5 | 0.206243 | 0.554004 | 0.239753 | 1.924549 | -1.182608 | 0.286074 |

| Population | Net std | p05 | p25 | p50 | p75 | p95 |
| --- | --- | --- | --- | --- | --- | --- |
| POOLED | 1.389726 | -1.325300 | -1.131500 | -1.045983 | 0.963283 | 2.354179 |
| FOLD_1 | 1.418071 | -1.270183 | -1.109501 | -1.035663 | 1.060792 | 2.308669 |
| FOLD_2 | 1.395771 | -1.310721 | -1.125933 | -1.046324 | 1.007676 | 2.387865 |
| FOLD_3 | 1.374229 | -1.333258 | -1.133661 | -1.045705 | 0.933796 | 2.357818 |
| FOLD_4 | 1.392154 | -1.349382 | -1.146988 | -1.054184 | 0.904363 | 2.391278 |
| FOLD_5 | 1.363976 | -1.362945 | -1.148762 | -1.053757 | 0.864069 | 2.334818 |

Matched gross means are slightly positive in folds 1–2 and negative in 3–5; all net means are negative. Pooled descriptive day-clustered net interval is [-0.172892, -0.131583] R. A slightly negative gross mean does not prove absence of every possible causal alpha; positive gross edge is not demonstrated in this population.

## Full frozen parent population check

| Full label population | n | Gross R | Cost R | Net R |
| --- | --- | --- | --- | --- |
| FULL_PARENT | 3007222 | -0.015416 | 0.140393 | -0.155809 |
| OUTER_CALENDAR_POOLED | 2494072 | -0.006473 | 0.144242 | -0.150715 |
| FOLD_1 | 523946 | 0.006949 | 0.124327 | -0.117378 |
| FOLD_2 | 522606 | -0.000603 | 0.139319 | -0.139922 |
| FOLD_3 | 494478 | -0.018969 | 0.148972 | -0.167940 |
| FOLD_4 | 472328 | -0.006803 | 0.153549 | -0.160353 |
| FOLD_5 | 480714 | -0.014306 | 0.157289 | -0.171595 |

The stride-12 development sample (n=250,602) averages -0.015926 gross, 0.140227 cost and -0.156152 net R. Only full-parent fold 1 has positive gross mean; all full-parent folds lose net. Full/sample quantiles, terminal breakdowns and scenarios are in [raw_economics.csv](artifacts/cati_v5_economic_edge_audit/raw_economics.csv).

## Frozen cost decomposition and stress scenarios

| Population | Fee R | Spread R | Slippage R | Funding R | Carry R | 0× net R | 1× net R | 2× net R |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| POOLED | 0.076758 | 0.019190 | 0.038379 | 0.009595 | 0.000000 | -0.008316 | -0.152238 | -0.296159 |
| FOLD_1 | 0.066496 | 0.016624 | 0.033248 | 0.008312 | 0.000000 | 0.007758 | -0.116922 | -0.241601 |
| FOLD_2 | 0.073558 | 0.018390 | 0.036779 | 0.009195 | 0.000000 | 0.012425 | -0.125497 | -0.263418 |
| FOLD_3 | 0.078360 | 0.019590 | 0.039180 | 0.009795 | 0.000000 | -0.015824 | -0.162749 | -0.309675 |
| FOLD_4 | 0.083152 | 0.020788 | 0.041576 | 0.010394 | 0.000000 | -0.015748 | -0.171657 | -0.327566 |
| FOLD_5 | 0.083424 | 0.020856 | 0.041712 | 0.010428 | 0.000000 | -0.033238 | -0.189658 | -0.346078 |

Frozen labels charge round-trip taker fees/spread/slippage and funding over the **full 48-bar label horizon**, even for an earlier touch. Twelve hours completes one modeled 8-hour funding interval for every candidate. Cost burden is approximately 0.0015 / initial_risk_fraction. Funding is an average assumption, not observed funding stamps. Carry is zero; latency, market impact and borrow are not modeled. No extra components are fabricated.

Costs remove 0.143921 R per candidate, arithmetically 94.54% of the pooled net deficit. **GROSS_EDGE_POSITIVE = NO; EDGE_KILLED_BY_COSTS = NO for the pooled sign-flip definition**: gross is already negative. Costs do destroy small early and selected-tail positive gross means.

Early matched folds need more than 92.31% aggregate cost reduction to make their observed gross mean net positive. Halving fees under the frozen maker assumption alone, ignoring fill/adverse-selection effects, still gives -0.113859 R pooled net. Removing all fees leaves -0.075480 R; zero total costs still leaves -0.008316 R. Cost reduction alone cannot rescue the pooled population. These are counterfactual arithmetic, not evidence that such execution is feasible.

## Every fixed V5 expected-R ranking tail

| Population | Top | n | Predicted R | Gross R | Net R | Profitable | TARGET | STOP | TIMEOUT |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| POOLED | 0.5% | 260 | 0.294898 | 0.007456 | -0.060024 | 0.392308 | 0.076923 | 0.446154 | 0.476923 |
| POOLED | 1% | 519 | 0.222409 | 0.037872 | -0.028726 | 0.404624 | 0.088632 | 0.443160 | 0.468208 |
| POOLED | 2% | 1038 | 0.162966 | 0.036377 | -0.031734 | 0.409441 | 0.102119 | 0.442197 | 0.455684 |
| POOLED | 5% | 2595 | 0.099476 | 0.025294 | -0.043871 | 0.411561 | 0.115607 | 0.420039 | 0.464355 |
| POOLED | 10% | 5189 | 0.057817 | 0.012872 | -0.055691 | 0.410869 | 0.127385 | 0.414145 | 0.458470 |
| POOLED | 20% | 10378 | 0.018041 | 0.003248 | -0.066062 | 0.409906 | 0.141453 | 0.409809 | 0.448738 |
| POOLED | 30% | 15567 | -0.006555 | -0.003542 | -0.074645 | 0.406051 | 0.151410 | 0.421661 | 0.426929 |
| FOLD_1 | 0.5% | 55 | 0.277687 | 0.061822 | -0.011047 | 0.381818 | 0.272727 | 0.454545 | 0.272727 |
| FOLD_1 | 1% | 109 | 0.213722 | -0.035958 | -0.102890 | 0.376147 | 0.220183 | 0.513761 | 0.266055 |
| FOLD_1 | 2% | 217 | 0.160643 | -0.061051 | -0.131066 | 0.368664 | 0.184332 | 0.506912 | 0.308756 |
| FOLD_1 | 5% | 542 | 0.098861 | 0.029055 | -0.041964 | 0.400369 | 0.197417 | 0.466790 | 0.335793 |
| FOLD_1 | 10% | 1084 | 0.055545 | 0.027845 | -0.041829 | 0.415129 | 0.189114 | 0.455720 | 0.355166 |
| FOLD_1 | 20% | 2168 | 0.011559 | 0.018561 | -0.050161 | 0.412823 | 0.182196 | 0.448339 | 0.369465 |
| FOLD_1 | 30% | 3252 | -0.017428 | 0.010101 | -0.060263 | 0.410517 | 0.185117 | 0.456335 | 0.358549 |
| FOLD_2 | 0.5% | 55 | 0.175999 | 0.110241 | 0.053107 | 0.400000 | 0.127273 | 0.436364 | 0.436364 |
| FOLD_2 | 1% | 109 | 0.139152 | 0.088552 | 0.034887 | 0.403670 | 0.128440 | 0.440367 | 0.431193 |
| FOLD_2 | 2% | 218 | 0.106675 | 0.020808 | -0.039656 | 0.408257 | 0.114679 | 0.431193 | 0.454128 |
| FOLD_2 | 5% | 545 | 0.065906 | 0.130550 | 0.067052 | 0.444037 | 0.143119 | 0.387156 | 0.469725 |
| FOLD_2 | 10% | 1090 | 0.035016 | 0.126627 | 0.060769 | 0.455963 | 0.152294 | 0.370642 | 0.477064 |
| FOLD_2 | 20% | 2179 | 0.003750 | 0.066778 | -0.001183 | 0.443323 | 0.157871 | 0.392382 | 0.449748 |
| FOLD_2 | 30% | 3269 | -0.015747 | 0.049263 | -0.021084 | 0.433772 | 0.166412 | 0.410217 | 0.423371 |
| FOLD_3 | 0.5% | 52 | 0.412588 | -0.034040 | -0.104345 | 0.442308 | 0.000000 | 0.519231 | 0.480769 |
| FOLD_3 | 1% | 104 | 0.308212 | 0.013824 | -0.055888 | 0.423077 | 0.028846 | 0.500000 | 0.471154 |
| FOLD_3 | 2% | 207 | 0.223428 | 0.207934 | 0.135228 | 0.487923 | 0.077295 | 0.420290 | 0.502415 |
| FOLD_3 | 5% | 516 | 0.141948 | 0.071745 | -0.002374 | 0.426357 | 0.112403 | 0.436047 | 0.451550 |
| FOLD_3 | 10% | 1032 | 0.093089 | 0.010925 | -0.062906 | 0.409884 | 0.128876 | 0.442829 | 0.428295 |
| FOLD_3 | 20% | 2063 | 0.049311 | -0.010803 | -0.082693 | 0.404750 | 0.131847 | 0.415414 | 0.452739 |
| FOLD_3 | 30% | 3094 | 0.023305 | -0.036898 | -0.109474 | 0.395281 | 0.135747 | 0.424370 | 0.439884 |
| FOLD_4 | 0.5% | 50 | 0.283046 | -0.113019 | -0.178186 | 0.340000 | 0.020000 | 0.460000 | 0.520000 |
| FOLD_4 | 1% | 99 | 0.215723 | -0.002078 | -0.065069 | 0.383838 | 0.040404 | 0.404040 | 0.555556 |
| FOLD_4 | 2% | 197 | 0.155521 | 0.053529 | -0.008575 | 0.416244 | 0.060914 | 0.401015 | 0.538071 |
| FOLD_4 | 5% | 491 | 0.092212 | 0.009856 | -0.051123 | 0.423625 | 0.071283 | 0.366599 | 0.562118 |
| FOLD_4 | 10% | 982 | 0.051617 | -0.017799 | -0.080263 | 0.407332 | 0.094705 | 0.389002 | 0.516293 |
| FOLD_4 | 20% | 1963 | 0.012971 | -0.007280 | -0.072642 | 0.400917 | 0.121243 | 0.380540 | 0.498217 |
| FOLD_4 | 30% | 2945 | -0.011155 | 0.001086 | -0.066779 | 0.406112 | 0.140577 | 0.392530 | 0.466893 |
| FOLD_5 | 0.5% | 51 | 0.275708 | 0.025736 | -0.041309 | 0.372549 | 0.019608 | 0.352941 | 0.627451 |
| FOLD_5 | 1% | 101 | 0.210041 | -0.068778 | -0.138200 | 0.366337 | 0.009901 | 0.415842 | 0.574257 |
| FOLD_5 | 2% | 201 | 0.151231 | 0.004735 | -0.068701 | 0.388060 | 0.019900 | 0.407960 | 0.572139 |
| FOLD_5 | 5% | 502 | 0.085392 | -0.030473 | -0.099957 | 0.386454 | 0.051793 | 0.408367 | 0.539841 |
| FOLD_5 | 10% | 1003 | 0.042788 | -0.012939 | -0.082793 | 0.400798 | 0.080758 | 0.406780 | 0.512463 |
| FOLD_5 | 20% | 2006 | 0.004613 | -0.030777 | -0.101855 | 0.392323 | 0.113659 | 0.405783 | 0.480558 |
| FOLD_5 | 30% | 3009 | -0.018386 | -0.044889 | -0.118233 | 0.387504 | 0.134264 | 0.421402 | 0.444334 |

All seven pooled tails lose net. Fold 2 has isolated positive tails; fold 3 top 2% is positive; no fixed fraction is positive in every fold. Top-0.5% cells have roughly 50–55 rows/fold; its pooled descriptive interval spans [-0.221058, +0.101010] R. No fraction is selected. [Exact tail cells and intervals](artifacts/cati_v5_economic_edge_audit/fixed_tails.csv).

## Fixed expectancy thresholds — no optimization

| Population | Predicted R > | n | Folds represented / 5 | Predicted mean R | Realized net R | Profitable |
| --- | --- | --- | --- | --- | --- | --- |
| POOLED | 0.000000 | 5182 | 5 | 0.057895 | -0.055641 | 0.410845 |
| POOLED | 0.050000 | 1998 | 5 | 0.116426 | -0.044237 | 0.407908 |
| POOLED | 0.100000 | 801 | 5 | 0.183738 | 0.019395 | 0.423221 |
| POOLED | 0.200000 | 195 | 5 | 0.330029 | -0.045458 | 0.394872 |
| FOLD_1 | 0.000000 | 992 | 5 | 0.060964 | -0.040139 | 0.414315 |
| FOLD_1 | 0.050000 | 414 | 5 | 0.116886 | -0.056000 | 0.391304 |
| FOLD_1 | 0.100000 | 181 | 5 | 0.173654 | -0.075709 | 0.386740 |
| FOLD_1 | 0.200000 | 32 | 5 | 0.345811 | -0.121524 | 0.343750 |
| FOLD_2 | 0.000000 | 871 | 5 | 0.045106 | 0.059261 | 0.452354 |
| FOLD_2 | 0.050000 | 282 | 5 | 0.094884 | -0.033656 | 0.418440 |
| FOLD_2 | 0.100000 | 87 | 5 | 0.150489 | 0.096137 | 0.413793 |
| FOLD_2 | 0.200000 | 11 | 5 | 0.266059 | -0.095173 | 0.272727 |
| FOLD_3 | 0.000000 | 1674 | 5 | 0.062368 | -0.100432 | 0.396655 |
| FOLD_3 | 0.050000 | 681 | 5 | 0.121441 | -0.024158 | 0.422907 |
| FOLD_3 | 0.100000 | 270 | 5 | 0.196610 | 0.144754 | 0.477778 |
| FOLD_3 | 0.200000 | 77 | 5 | 0.351035 | 0.033379 | 0.454545 |
| FOLD_4 | 0.000000 | 904 | 5 | 0.056271 | -0.074302 | 0.407080 |
| FOLD_4 | 0.050000 | 328 | 5 | 0.118414 | -0.022047 | 0.423780 |
| FOLD_4 | 0.100000 | 134 | 5 | 0.188119 | -0.044055 | 0.417910 |
| FOLD_4 | 0.200000 | 36 | 5 | 0.320336 | -0.235897 | 0.305556 |
| FOLD_5 | 0.000000 | 741 | 5 | 0.060696 | -0.087500 | 0.394062 |
| FOLD_5 | 0.050000 | 293 | 5 | 0.122628 | -0.109310 | 0.368601 |
| FOLD_5 | 0.100000 | 129 | 5 | 0.188817 | -0.095390 | 0.372093 |
| FOLD_5 | 0.200000 | 39 | 5 | 0.302594 | 0.051114 | 0.435897 |

The >+0.10 cell averages +0.019395 R on 801 rows but loses in folds 1, 4 and 5; >+0.20 loses pooled. This is not a production rule or stable edge confirmation.

## Coherent profit-probability thresholds

| Population | p > | n | Mean p | Actual profitable | Net R |
| --- | --- | --- | --- | --- | --- |
| POOLED | 0.500000 | 1051 | 0.529392 | 0.469077 | -0.000448 |
| POOLED | 0.550000 | 187 | 0.585815 | 0.502674 | 0.045946 |
| POOLED | 0.600000 | 39 | 0.652324 | 0.384615 | -0.141594 |
| POOLED | 0.650000 | 13 | 0.716329 | 0.384615 | -0.260931 |
| FOLD_1 | 0.500000 | 143 | 0.530316 | 0.454545 | -0.102635 |
| FOLD_1 | 0.550000 | 26 | 0.585267 | 0.384615 | -0.244453 |
| FOLD_1 | 0.600000 | 7 | 0.631493 | 0.428571 | -0.196711 |
| FOLD_1 | 0.650000 | 2 | 0.666481 | 0.500000 | -0.169462 |
| FOLD_2 | 0.500000 | 149 | 0.523958 | 0.557047 | 0.180520 |
| FOLD_2 | 0.550000 | 21 | 0.571663 | 0.523810 | 0.096646 |
| FOLD_2 | 0.600000 | 2 | 0.628436 | 0.500000 | -0.438655 |
| FOLD_2 | 0.650000 | 0 | — | — | — |
| FOLD_3 | 0.500000 | 407 | 0.529339 | 0.503686 | 0.040053 |
| FOLD_3 | 0.550000 | 72 | 0.587904 | 0.625000 | 0.220666 |
| FOLD_3 | 0.600000 | 17 | 0.648103 | 0.588235 | 0.169576 |
| FOLD_3 | 0.650000 | 7 | 0.692560 | 0.428571 | -0.261515 |
| FOLD_4 | 0.500000 | 231 | 0.531692 | 0.406926 | -0.070327 |
| FOLD_4 | 0.550000 | 48 | 0.580861 | 0.437500 | -0.035259 |
| FOLD_4 | 0.600000 | 9 | 0.638190 | 0.000000 | -0.637377 |
| FOLD_4 | 0.650000 | 1 | 0.766648 | 0.000000 | -1.031882 |
| FOLD_5 | 0.500000 | 121 | 0.530775 | 0.380165 | -0.105354 |
| FOLD_5 | 0.550000 | 20 | 0.605751 | 0.350000 | -0.063867 |
| FOLD_5 | 0.600000 | 4 | 0.750460 | 0.250000 | -0.103569 |
| FOLD_5 | 0.650000 | 3 | 0.788249 | 0.333333 | -0.063564 |

p > 0.55 averages +0.045946 R on 187 rows but loses in folds 1, 4 and 5. p > 0.60 / 0.65 lose pooled, with 39 / 13 rows. Pooled probability calibration does not establish sparse-tail calibration or profitable decisions.

## Rank quality, ordering breaks and feature selectability

| Population | Expected-R Spearman | Kendall | Gross Spearman | Cost Spearman | p vs binary Spearman | p vs binary Kendall |
| --- | --- | --- | --- | --- | --- | --- |
| POOLED | 0.312094 | 0.226713 | 0.109969 | -0.667149 | 0.140272 | 0.114533 |
| FOLD_1 | 0.254002 | 0.178062 | 0.093845 | -0.487473 | 0.144739 | 0.118185 |
| FOLD_2 | 0.330729 | 0.243834 | 0.129366 | -0.697156 | 0.168031 | 0.137203 |
| FOLD_3 | 0.298805 | 0.216869 | 0.094487 | -0.674368 | 0.129565 | 0.105795 |
| FOLD_4 | 0.360752 | 0.266884 | 0.134160 | -0.779083 | 0.138445 | 0.113046 |
| FOLD_5 | 0.338517 | 0.250599 | 0.112149 | -0.749321 | 0.125256 | 0.102276 |

Rank classification **D. mixed**: ranking persists late and improves relative losses, but strong gross discrimination/stable positive expectancy is absent. Spearman with net R is +0.312 pooled, with gross R +0.110, and with cost burden -0.667. These associations do not establish causal attribution; much of the forecast’s usefulness is compatible with avoiding cost-heavy candidates.

| Population | Ranking | Quintile realized means low→high | Decile realized means low→high | Breaks after decile |
| --- | --- | --- | --- | --- |
| POOLED | expected_R | -0.3397 / -0.1636 / -0.0967 / -0.0952 / -0.0660 | -0.4732 / -0.2062 / -0.1992 / -0.1280 / -0.1256 / -0.0679 / -0.0988 / -0.0912 / -0.0767 / -0.0555 | 6 |
| POOLED | probability_net_R | -0.2924 / -0.1621 / -0.1431 / -0.1017 / -0.0618 | -0.3626 / -0.2223 / -0.1846 / -0.1395 / -0.1334 / -0.1524 / -0.1172 / -0.0868 / -0.0579 / -0.0655 | 5, 9 |
| FOLD_1 | expected_R | -0.3318 / -0.1217 / -0.0517 / -0.0292 / -0.0502 | -0.5000 / -0.1636 / -0.1290 / -0.1144 / -0.0837 / -0.0196 / 0.0220 / -0.0805 / -0.0585 / -0.0418 | 7 |
| FOLD_1 | probability_net_R | -0.3261 / -0.0721 / -0.1004 / -0.0455 / -0.0404 | -0.4330 / -0.2192 / -0.1048 / -0.0395 / -0.0769 / -0.1239 / -0.0703 / -0.0207 / -0.0218 / -0.0591 | 4, 5, 8, 9 |
| FOLD_2 | expected_R | -0.3139 / -0.1776 / -0.0905 / -0.0444 / -0.0012 | -0.3990 / -0.2294 / -0.1713 / -0.1847 / -0.0946 / -0.0835 / -0.0248 / -0.0658 / -0.0632 / 0.0618 | 3, 7 |
| FOLD_2 | probability_net_R | -0.2622 / -0.1683 / -0.1373 / -0.0693 / 0.0096 | -0.3032 / -0.2220 / -0.1977 / -0.1398 / -0.1204 / -0.1519 / -0.1124 / -0.0275 / -0.0316 / 0.0518 | 5, 8 |
| FOLD_3 | expected_R | -0.3133 / -0.1453 / -0.1539 / -0.1172 / -0.0840 | -0.4327 / -0.1939 / -0.1855 / -0.1050 / -0.2058 / -0.1020 / -0.0726 / -0.1618 / -0.1061 / -0.0619 | 4, 7 |
| FOLD_3 | probability_net_R | -0.2805 / -0.1718 / -0.1598 / -0.0996 / -0.1020 | -0.3500 / -0.2110 / -0.1426 / -0.2010 / -0.1423 / -0.1772 / -0.1171 / -0.0822 / -0.1145 / -0.0895 | 3, 5, 8 |
| FOLD_4 | expected_R | -0.3910 / -0.2132 / -0.0662 / -0.1140 / -0.0737 | -0.4974 / -0.2854 / -0.2800 / -0.1473 / -0.0700 / -0.0584 / -0.1770 / -0.0531 / -0.0667 / -0.0807 | 6, 8, 9 |
| FOLD_4 | probability_net_R | -0.3202 / -0.1812 / -0.1616 / -0.1284 / -0.0668 | -0.3560 / -0.2857 / -0.2413 / -0.1225 / -0.1885 / -0.1329 / -0.1504 / -0.1054 / -0.0463 / -0.0873 | 4, 6, 9 |
| FOLD_5 | expected_R | -0.3654 / -0.1919 / -0.1375 / -0.1520 / -0.1014 | -0.5300 / -0.2009 / -0.1849 / -0.1990 / -0.1703 / -0.1056 / -0.1516 / -0.1519 / -0.1188 / -0.0834 | 3, 6, 7 |
| FOLD_5 | probability_net_R | -0.2901 / -0.2055 / -0.1780 / -0.1593 / -0.1153 | -0.3548 / -0.2253 / -0.1989 / -0.2121 / -0.2104 / -0.1441 / -0.1784 / -0.1427 / -0.1349 / -0.0947 | 3, 6 |

Both pooled quintile orderings are monotone. Expected-R pooled deciles reverse at 6→7; probability deciles reverse at 5→6 and 9→10. Fold reversals are reported, not hidden by pooling. [All bin counts/predicted means](artifacts/cati_v5_economic_edge_audit/ranking_bins.csv).

| Population | Score | Top minus bottom decile R | Top decile minus population R | Top 5% minus population R |
| --- | --- | --- | --- | --- |
| POOLED | expected_R | 0.417715 | 0.096753 | 0.108367 |
| POOLED | probability_net_R | 0.297084 | 0.086716 | 0.105752 |
| FOLD_1 | expected_R | 0.458206 | 0.075092 | 0.074958 |
| FOLD_1 | probability_net_R | 0.373880 | 0.057818 | 0.014336 |
| FOLD_2 | expected_R | 0.460789 | 0.187286 | 0.192548 |
| FOLD_2 | probability_net_R | 0.355002 | 0.177306 | 0.205477 |
| FOLD_3 | expected_R | 0.370788 | 0.100817 | 0.160375 |
| FOLD_3 | probability_net_R | 0.260482 | 0.073265 | 0.182751 |
| FOLD_4 | expected_R | 0.416731 | 0.090987 | 0.120534 |
| FOLD_4 | probability_net_R | 0.268730 | 0.084379 | 0.098802 |
| FOLD_5 | expected_R | 0.446552 | 0.106239 | 0.089701 |
| FOLD_5 | probability_net_R | 0.260108 | 0.094920 | 0.095062 |

Deciles use np.array_split on ascending scores; fixed top fractions use ceil on descending scores. They can differ by one pooled boundary row. Both conventions are fixed and disclosed.

## Joint payoff contribution errors in the top 20%

| State | Predicted p | Observed freq | Predicted R contribution | Realized R contribution | Bias R |
| --- | --- | --- | --- | --- | --- |
| TARGET_PROFIT | 0.176181 | 0.141453 | 0.327190 | 0.254540 | 0.072649 |
| TARGET_LOSS | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| STOP_LOSS | 0.419991 | 0.409809 | -0.452861 | -0.442778 | -0.010082 |
| TIMEOUT_PROFIT | 0.260689 | 0.268452 | 0.196044 | 0.186580 | 0.009464 |
| TIMEOUT_LOSS | 0.143139 | 0.180285 | -0.052332 | -0.064404 | 0.012072 |

Forecast +0.018041 R versus -0.066062 R realized is +0.084103 R optimism. TARGET_PROFIT contributes +0.072649 R (86.38%): 17.62% predicted state probability versus 14.15% observed. STOP contributes -0.010082 R bias, so underestimating stops is not the main pooled top-quintile explanation. TIMEOUT_LOSS is underpredicted (14.31% versus 18.03%), adding +0.012072 R optimism; TIMEOUT_PROFIT adds +0.009464 R. These are exact state-weighted contribution errors, not separate causal estimates of probability versus magnitude error. [Every fold/all-candidate decomposition](artifacts/cati_v5_economic_edge_audit/joint_payoff_bias.csv).

## Setup economics — every group and fold retained

Diagnostic sufficient-evidence flag: ≥300 pooled rows and ≥50 in each fold, fixed before group inspection. This is a count flag, not a significance test or production filter. All groups, including sparse/empty cells, remain in the evidence.

| Dimension | Group | n | Gross R | Cost R | Net R | Profitable | Sufficient | Fold 1–5 net R |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| family | BREAKOUT_VOL_EXPANSION_V2 | 27428 | 0.001278 | 0.087064 | -0.085786 | 0.384607 | True | -0.0316 / -0.0227 / -0.1280 / -0.1062 / -0.1469 |
| family | MOMENTUM_CONTINUATION_V1 | 2063 | -0.002242 | 0.054423 | -0.056665 | 0.425594 | True | -0.0641 / -0.0253 / -0.1149 / 0.0813 / -0.1545 |
| family | RANGE_MEAN_REVERSION_V2 | 20590 | -0.018776 | 0.202376 | -0.221152 | 0.325401 | True | -0.1986 / -0.2579 / -0.1771 / -0.2397 / -0.2321 |
| family | TREND_PULLBACK_V2 | 1806 | -0.041719 | 0.443221 | -0.484940 | 0.337209 | True | -0.4651 / -0.3471 / -0.5568 / -0.6347 / -0.4496 |
| side | LONG | 26036 | -0.044544 | 0.141713 | -0.186257 | 0.345982 | True | -0.1581 / -0.1042 / -0.2460 / -0.2195 / -0.2048 |
| side | SHORT | 25851 | 0.028171 | 0.146146 | -0.117976 | 0.376310 | True | -0.0808 / -0.1455 / -0.0827 / -0.1172 / -0.1719 |
| regime | EXHAUSTION_REVERSAL | 129 | 0.113754 | 0.127765 | -0.014011 | 0.364341 | False | -0.1130 / 0.1951 / 0.1192 / -0.0979 / -0.1072 |
| regime | RANGE_EQUILIBRIUM | 28472 | -0.013293 | 0.185953 | -0.199246 | 0.341107 | True | -0.1805 / -0.1885 / -0.1996 / -0.2181 / -0.2115 |
| regime | SHOCK | 3380 | -0.001316 | 0.059964 | -0.061281 | 0.415385 | True | 0.0795 / 0.0150 / -0.0821 / -0.1581 / -0.1399 |
| regime | TREND_CONTINUATION | 7133 | 0.000690 | 0.115715 | -0.115025 | 0.363241 | True | -0.0913 / -0.0656 / -0.1403 / -0.1448 / -0.1592 |
| regime | VOL_EXPANSION | 12773 | -0.005338 | 0.088361 | -0.093699 | 0.390041 | True | -0.0434 / -0.0536 / -0.1163 / -0.0864 / -0.1704 |
| volatility | HIGH | 19391 | -0.006058 | 0.112777 | -0.118835 | 0.380383 | True | -0.0580 / -0.1100 / -0.1036 / -0.1641 / -0.1691 |
| volatility | LOW | 15801 | -0.020792 | 0.176528 | -0.197320 | 0.337953 | True | -0.1681 / -0.1467 / -0.2267 / -0.2137 / -0.2350 |
| volatility | MEDIUM | 16695 | 0.000868 | 0.149235 | -0.148367 | 0.360587 | True | -0.1399 / -0.1240 / -0.1712 / -0.1395 / -0.1693 |
| family_side | BREAKOUT_VOL_EXPANSION_V2 \| LONG | 13371 | -0.031652 | 0.088169 | -0.119822 | 0.367886 | True | -0.1024 / -0.0165 / -0.2061 / -0.1334 / -0.1497 |
| family_side | BREAKOUT_VOL_EXPANSION_V2 \| SHORT | 14057 | 0.032601 | 0.086013 | -0.053412 | 0.400512 | True | 0.0262 / -0.0291 / -0.0543 / -0.0799 / -0.1441 |
| family_side | MOMENTUM_CONTINUATION_V1 \| LONG | 979 | -0.020893 | 0.054903 | -0.075796 | 0.406537 | True | -0.0474 / -0.0781 / -0.0491 / -0.0077 / -0.2316 |
| family_side | MOMENTUM_CONTINUATION_V1 \| SHORT | 1084 | 0.014602 | 0.053990 | -0.039388 | 0.442804 | True | -0.0798 / 0.0261 / -0.1641 / 0.1673 / -0.0875 |
| family_side | RANGE_MEAN_REVERSION_V2 \| LONG | 10890 | -0.061611 | 0.191324 | -0.252935 | 0.314325 | True | -0.1812 / -0.2196 / -0.2834 / -0.3130 / -0.2549 |
| family_side | RANGE_MEAN_REVERSION_V2 \| SHORT | 9700 | 0.029314 | 0.214783 | -0.185469 | 0.337835 | True | -0.2155 / -0.2908 / -0.0682 / -0.1310 / -0.1995 |
| family_side | TREND_PULLBACK_V2 \| LONG | 796 | -0.056698 | 0.469147 | -0.525845 | 0.336683 | True | -0.7594 / -0.3310 / -0.5933 / -0.4236 / -0.4050 |
| family_side | TREND_PULLBACK_V2 \| SHORT | 1010 | -0.029914 | 0.422788 | -0.452702 | 0.337624 | True | -0.2397 / -0.3587 / -0.5272 / -0.7814 / -0.4959 |
| family_regime | BREAKOUT_VOL_EXPANSION_V2 \| EXHAUSTION_REVERSAL | 126 | 0.084160 | 0.127270 | -0.043110 | 0.357143 | False | -0.2181 / 0.1951 / 0.1192 / -0.0979 / -0.1665 |
| family_regime | BREAKOUT_VOL_EXPANSION_V2 \| RANGE_EQUILIBRIUM | 9454 | -0.012949 | 0.107936 | -0.120885 | 0.355617 | True | -0.0775 / -0.0493 / -0.2045 / -0.1251 / -0.1567 |
| family_regime | BREAKOUT_VOL_EXPANSION_V2 \| SHOCK | 3207 | 0.001047 | 0.060721 | -0.059674 | 0.416277 | True | 0.1049 / 0.0203 / -0.0916 / -0.1617 / -0.1397 |
| family_regime | BREAKOUT_VOL_EXPANSION_V2 \| TREND_CONTINUATION | 3050 | 0.033924 | 0.071864 | -0.037940 | 0.393443 | True | -0.0382 / 0.0763 / -0.0410 / -0.0874 / -0.1371 |
| family_regime | BREAKOUT_VOL_EXPANSION_V2 \| VOL_EXPANSION | 11591 | 0.003454 | 0.080892 | -0.077437 | 0.397464 | True | -0.0270 / -0.0386 / -0.1013 / -0.0782 / -0.1429 |
| family_regime | MOMENTUM_CONTINUATION_V1 \| RANGE_EQUILIBRIUM | 108 | 0.023517 | 0.071199 | -0.047682 | 0.444444 | False | -0.0682 / 0.0553 / -0.3001 / -0.0307 / -0.0237 |
| family_regime | MOMENTUM_CONTINUATION_V1 \| SHOCK | 67 | 0.110954 | 0.044660 | 0.066294 | 0.492537 | False | 0.0027 / 0.2536 / 0.3417 / -0.0436 / -0.1080 |
| family_regime | MOMENTUM_CONTINUATION_V1 \| TREND_CONTINUATION | 1527 | 0.005313 | 0.056244 | -0.050932 | 0.427636 | True | -0.0739 / -0.0011 / -0.1154 / 0.1256 / -0.1747 |
| family_regime | MOMENTUM_CONTINUATION_V1 \| VOL_EXPANSION | 361 | -0.062914 | 0.043513 | -0.106427 | 0.398892 | True | -0.0384 / -0.2244 / -0.1504 / -0.0201 / -0.1382 |
| family_regime | RANGE_MEAN_REVERSION_V2 \| EXHAUSTION_REVERSAL | 2 | 1.035088 | 0.164269 | 0.870819 | 0.500000 | False | — / — / — / — / 0.8708 |
| family_regime | RANGE_MEAN_REVERSION_V2 \| RANGE_EQUILIBRIUM | 17646 | -0.010158 | 0.201640 | -0.211799 | 0.334807 | True | -0.1969 / -0.2561 / -0.1586 / -0.2250 / -0.2223 |
| family_regime | RANGE_MEAN_REVERSION_V2 \| TREND_CONTINUATION | 2253 | -0.049880 | 0.195322 | -0.245202 | 0.275632 | True | -0.1824 / -0.2962 / -0.2522 / -0.3221 / -0.1747 |
| family_regime | RANGE_MEAN_REVERSION_V2 \| VOL_EXPANSION | 689 | -0.140829 | 0.244390 | -0.385220 | 0.246734 | True | -0.3072 / -0.1923 / -0.4765 / -0.3452 / -0.5987 |
| family_regime | TREND_PULLBACK_V2 \| EXHAUSTION_REVERSAL | 1 | 2.000000 | 0.117203 | 1.882797 | 1.000000 | False | 1.8828 / — / — / — / — |
| family_regime | TREND_PULLBACK_V2 \| RANGE_EQUILIBRIUM | 1264 | -0.062772 | 0.560278 | -0.623050 | 0.311709 | True | -0.6278 / -0.3749 / -0.6799 / -0.8769 / -0.6037 |
| family_regime | TREND_PULLBACK_V2 \| SHOCK | 106 | -0.143784 | 0.046740 | -0.190523 | 0.339623 | False | -0.3421 / -0.2469 / 0.0469 / -0.1054 / -0.1740 |
| family_regime | TREND_PULLBACK_V2 \| TREND_CONTINUATION | 303 | 0.018880 | 0.264902 | -0.246022 | 0.386139 | False | -0.1354 / -0.3501 / -0.4563 / -0.1646 / -0.1874 |
| family_regime | TREND_PULLBACK_V2 \| VOL_EXPANSION | 132 | 0.087268 | 0.052487 | 0.034781 | 0.462121 | False | -0.1507 / -0.1539 / 0.2663 / 0.2809 / 0.0326 |
| family_volatility | BREAKOUT_VOL_EXPANSION_V2 \| HIGH | 11822 | 0.004736 | 0.069417 | -0.064681 | 0.406107 | True | 0.0099 / -0.0215 / -0.0674 / -0.1281 / -0.1236 |
| family_volatility | BREAKOUT_VOL_EXPANSION_V2 \| LOW | 6966 | -0.007315 | 0.112873 | -0.120188 | 0.353862 | True | -0.0781 / -0.0263 / -0.2248 / -0.0945 / -0.1861 |
| family_volatility | BREAKOUT_VOL_EXPANSION_V2 \| MEDIUM | 8640 | 0.003474 | 0.090403 | -0.086929 | 0.379977 | True | -0.0504 / -0.0215 / -0.1375 / -0.0867 / -0.1475 |
| family_volatility | MOMENTUM_CONTINUATION_V1 \| HIGH | 802 | -0.021164 | 0.047545 | -0.068709 | 0.403990 | True | 0.0027 / -0.0998 / -0.0989 / -0.0377 / -0.1866 |
| family_volatility | MOMENTUM_CONTINUATION_V1 \| LOW | 606 | 0.000288 | 0.064421 | -0.064132 | 0.424092 | True | -0.1743 / -0.0889 / -0.0936 / 0.2967 / -0.1405 |
| family_volatility | MOMENTUM_CONTINUATION_V1 \| MEDIUM | 655 | 0.018584 | 0.053595 | -0.035011 | 0.453435 | True | -0.0687 / 0.1106 / -0.1487 / 0.0139 / -0.1250 |
| family_volatility | RANGE_MEAN_REVERSION_V2 \| HIGH | 5923 | -0.013560 | 0.180662 | -0.194223 | 0.332602 | True | -0.1586 / -0.2441 / -0.1444 / -0.1863 / -0.2338 |
| family_volatility | RANGE_MEAN_REVERSION_V2 \| LOW | 7820 | -0.034118 | 0.217935 | -0.252053 | 0.317391 | True | -0.2178 / -0.2538 / -0.1995 / -0.3216 / -0.2659 |
| family_volatility | RANGE_MEAN_REVERSION_V2 \| MEDIUM | 6847 | -0.005764 | 0.203389 | -0.209153 | 0.328319 | True | -0.2141 / -0.2759 / -0.1783 / -0.1872 / -0.1911 |
| family_volatility | TREND_PULLBACK_V2 \| HIGH | 844 | -0.090247 | 0.305706 | -0.395953 | 0.332938 | True | -0.2932 / -0.3399 / -0.3655 / -0.6537 / -0.4101 |
| family_volatility | TREND_PULLBACK_V2 \| LOW | 409 | -0.026781 | 0.635105 | -0.661886 | 0.332518 | True | -0.5948 / -0.3895 / -0.9256 / -0.7460 / -0.6883 |
| family_volatility | TREND_PULLBACK_V2 \| MEDIUM | 553 | 0.021297 | 0.511181 | -0.489884 | 0.347197 | True | -0.6222 / -0.3305 / -0.5541 / -0.5293 / -0.3240 |

Every sufficient pooled group loses net; none is positive in all five folds. Four positive pooled combination cells have only 1, 2, 67 and 132 rows. [All setup-family, side, regime, volatility and declared combination metrics in every fold](artifacts/cati_v5_economic_edge_audit/setup_groups.csv). No group deletion/universe filter is recommended.

## Fixed population geometry deciles

Pooled outer q10–q90 boundaries are fixed, then applied unchanged to each fold. Labels never determine boundaries. Tied boundaries can create unequal/empty cells; none is rebalanced or optimized. [Exact edges and every per-fold decile](artifacts/cati_v5_economic_edge_audit/geometry_deciles.csv), with boundaries also in the JSON.

| Feature | Decile | n | Feature mean | TARGET | STOP | TIMEOUT | Gross R | Cost R | Net R |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| room_to_target_R | 1 | 5189 | 1.229544 | 0.273270 | 0.375217 | 0.351513 | -0.035605 | 0.082254 | -0.117860 |
| room_to_target_R | 2 | 4892 | 1.428991 | 0.295789 | 0.457686 | 0.246525 | -0.016933 | 0.103779 | -0.120712 |
| room_to_target_R | 3 | 5485 | 1.551544 | 0.259617 | 0.459070 | 0.281313 | -0.020602 | 0.092318 | -0.112920 |
| room_to_target_R | 4 | 5189 | 1.754259 | 0.245519 | 0.498747 | 0.255733 | -0.016434 | 0.114097 | -0.130531 |
| room_to_target_R | 5 | 5188 | 1.963583 | 0.239784 | 0.548188 | 0.212028 | -0.018893 | 0.231143 | -0.250037 |
| room_to_target_R | 6 | 5189 | 2.161194 | 0.204278 | 0.552322 | 0.243399 | -0.019677 | 0.126754 | -0.146431 |
| room_to_target_R | 7 | 5189 | 2.477377 | 0.191752 | 0.574870 | 0.233378 | 0.014429 | 0.136264 | -0.121835 |
| room_to_target_R | 8 | 5188 | 2.912249 | 0.160177 | 0.613724 | 0.226099 | -0.001695 | 0.151468 | -0.153163 |
| room_to_target_R | 9 | 5189 | 3.572655 | 0.128734 | 0.658316 | 0.212950 | -0.015853 | 0.170667 | -0.186520 |
| room_to_target_R | 10 | 5189 | 5.421806 | 0.099827 | 0.708807 | 0.191366 | 0.048306 | 0.231133 | -0.182828 |
| initial_risk_fraction | 1 | 5189 | 0.003872 | 0.253228 | 0.729235 | 0.017537 | 0.006950 | 0.494659 | -0.487709 |
| initial_risk_fraction | 2 | 5189 | 0.006619 | 0.269223 | 0.689150 | 0.041627 | -0.014103 | 0.228843 | -0.242946 |
| initial_risk_fraction | 3 | 5188 | 0.008805 | 0.256168 | 0.665382 | 0.078450 | -0.026186 | 0.171207 | -0.197393 |
| initial_risk_fraction | 4 | 5189 | 0.011038 | 0.254192 | 0.624398 | 0.121411 | 0.009528 | 0.136400 | -0.126871 |
| initial_risk_fraction | 5 | 5188 | 0.013533 | 0.222051 | 0.596955 | 0.180995 | -0.004438 | 0.111205 | -0.115643 |
| initial_risk_fraction | 6 | 5189 | 0.016449 | 0.203122 | 0.569667 | 0.227211 | -0.038711 | 0.091481 | -0.130192 |
| initial_risk_fraction | 7 | 5189 | 0.020176 | 0.184621 | 0.514165 | 0.301214 | -0.015855 | 0.074617 | -0.090473 |
| initial_risk_fraction | 8 | 5188 | 0.025275 | 0.172899 | 0.446800 | 0.380301 | -0.003741 | 0.059652 | -0.063393 |
| initial_risk_fraction | 9 | 5189 | 0.034023 | 0.158027 | 0.374253 | 0.467720 | -0.008212 | 0.044541 | -0.052753 |
| initial_risk_fraction | 10 | 5189 | 0.063585 | 0.123145 | 0.237040 | 0.639815 | 0.011602 | 0.026593 | -0.014990 |
| cost_burden_R | 1 | 5189 | 0.026593 | 0.123145 | 0.237040 | 0.639815 | 0.011602 | 0.026593 | -0.014990 |
| cost_burden_R | 2 | 5189 | 0.044541 | 0.158027 | 0.374253 | 0.467720 | -0.008212 | 0.044541 | -0.052753 |
| cost_burden_R | 3 | 5188 | 0.059652 | 0.172899 | 0.446800 | 0.380301 | -0.003741 | 0.059652 | -0.063393 |
| cost_burden_R | 4 | 5189 | 0.074617 | 0.184621 | 0.514165 | 0.301214 | -0.015855 | 0.074617 | -0.090473 |
| cost_burden_R | 5 | 5188 | 0.091479 | 0.203161 | 0.569584 | 0.227255 | -0.038526 | 0.091479 | -0.130005 |
| cost_burden_R | 6 | 5189 | 0.111203 | 0.222008 | 0.597032 | 0.180960 | -0.004630 | 0.111203 | -0.115833 |
| cost_burden_R | 7 | 5189 | 0.136400 | 0.254192 | 0.624398 | 0.121411 | 0.009528 | 0.136400 | -0.126871 |
| cost_burden_R | 8 | 5188 | 0.171207 | 0.256168 | 0.665382 | 0.078450 | -0.026186 | 0.171207 | -0.197393 |
| cost_burden_R | 9 | 5189 | 0.228843 | 0.269223 | 0.689150 | 0.041627 | -0.014103 | 0.228843 | -0.242946 |
| cost_burden_R | 10 | 5189 | 0.494659 | 0.253228 | 0.729235 | 0.017537 | 0.006950 | 0.494659 | -0.487709 |

All 30 pooled deciles lose net. Narrowest-risk decile: mean fraction 0.003872, cost 0.494659 R, net -0.487709 R; widest: 0.063585, 0.026593 R, -0.014990 R. Highest target-room decile reaches TARGET only 9.98% and STOP 70.88%, versus 27.33% / 37.52% in the lowest. Larger stated reward is not itself alpha. Cost and risk bins largely invert because cost is deterministic in risk fraction. These are diagnostic associations, not chosen geometry thresholds.

## Event-time economics and unchanged 48-bar horizon

| Population | Terminal | n | Mean bars | Median | First ≤4 | First ≤8 | Net R | Funding R |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| POOLED | TARGET | 10879 | 17.889144 | 15.000000 | 0.137605 | 0.301039 | 1.941254 | 0.010570 |
| POOLED | STOP | 28263 | 13.394155 | 9.000000 | 0.299544 | 0.478718 | -1.172237 | 0.011482 |
| POOLED | TIMEOUT | 12745 | 48.000000 | 48.000000 | 0.000000 | 0.000000 | 0.322705 | 0.004576 |
| FOLD_1 | TARGET | 2330 | 18.130043 | 15.000000 | 0.126609 | 0.280687 | 1.926303 | 0.008338 |
| FOLD_1 | STOP | 5760 | 13.132812 | 9.000000 | 0.302431 | 0.481944 | -1.157436 | 0.010496 |
| FOLD_1 | TIMEOUT | 2750 | 48.000000 | 48.000000 | 0.000000 | 0.000000 | 0.331315 | 0.003716 |
| FOLD_2 | TARGET | 2297 | 18.015673 | 15.000000 | 0.131911 | 0.289508 | 1.965691 | 0.009808 |
| FOLD_2 | STOP | 5926 | 13.034593 | 9.000000 | 0.313027 | 0.493925 | -1.165958 | 0.011064 |
| FOLD_2 | TIMEOUT | 2672 | 48.000000 | 48.000000 | 0.000000 | 0.000000 | 0.384351 | 0.004523 |
| FOLD_3 | TARGET | 2139 | 17.457691 | 14.000000 | 0.147733 | 0.321646 | 1.911658 | 0.011748 |
| FOLD_3 | STOP | 5617 | 13.801317 | 9.000000 | 0.294463 | 0.473918 | -1.171378 | 0.011425 |
| FOLD_3 | TIMEOUT | 2555 | 48.000000 | 48.000000 | 0.000000 | 0.000000 | 0.317999 | 0.004576 |
| FOLD_4 | TARGET | 2045 | 18.117359 | 15.000000 | 0.137897 | 0.300244 | 1.978691 | 0.011454 |
| FOLD_4 | STOP | 5405 | 13.503978 | 9.000000 | 0.295467 | 0.470675 | -1.185130 | 0.012342 |
| FOLD_4 | TIMEOUT | 2364 | 48.000000 | 48.000000 | 0.000000 | 0.000000 | 0.285347 | 0.005023 |
| FOLD_5 | TARGET | 2068 | 17.697776 | 14.000000 | 0.145551 | 0.316248 | 1.924549 | 0.011837 |
| FOLD_5 | STOP | 5555 | 13.530153 | 9.000000 | 0.291269 | 0.471827 | -1.182608 | 0.012174 |
| FOLD_5 | TIMEOUT | 2404 | 48.000000 | 48.000000 | 0.000000 | 0.000000 | 0.286074 | 0.005182 |

| Terminal | Elapsed bucket | n | Gross R | Cost R | Net R | Profitable |
| --- | --- | --- | --- | --- | --- | --- |
| TARGET | 1–1 | 249 | 1.900784 | 0.534169 | 1.366615 | 0.971888 |
| TARGET | 2–2 | 339 | 1.892745 | 0.231856 | 1.660889 | 1.000000 |
| TARGET | 3–4 | 909 | 1.958342 | 0.206096 | 1.752246 | 1.000000 |
| TARGET | 5–8 | 1778 | 2.059171 | 0.186255 | 1.872915 | 1.000000 |
| TARGET | 9–16 | 2656 | 2.132629 | 0.158313 | 1.974316 | 1.000000 |
| TARGET | 17–32 | 3146 | 2.137969 | 0.123219 | 2.014750 | 1.000000 |
| TARGET | 33–48 | 1802 | 2.162663 | 0.103533 | 2.059130 | 1.000000 |
| STOP | 1–1 | 2557 | -1.000000 | 0.446991 | -1.446991 | 0.000000 |
| STOP | 2–2 | 2259 | -1.000000 | 0.225126 | -1.225126 | 0.000000 |
| STOP | 3–4 | 3650 | -1.000000 | 0.186829 | -1.186829 | 0.000000 |
| STOP | 5–8 | 5064 | -1.000000 | 0.158010 | -1.158010 | 0.000000 |
| STOP | 9–16 | 5933 | -1.000000 | 0.135807 | -1.135807 | 0.000000 |
| STOP | 17–32 | 5834 | -1.000000 | 0.112957 | -1.112957 | 0.000000 |
| STOP | 33–48 | 2966 | -1.000000 | 0.090897 | -1.090897 | 0.000000 |
| TIMEOUT | 1–1 | 0 | — | — | — | — |
| TIMEOUT | 2–2 | 0 | — | — | — | — |
| TIMEOUT | 3–4 | 0 | — | — | — | — |
| TIMEOUT | 5–8 | 0 | — | — | — | — |
| TIMEOUT | 9–16 | 0 | — | — | — | — |
| TIMEOUT | 17–32 | 0 | — | — | — | — |
| TIMEOUT | 33–48 | 12745 | 0.391352 | 0.068647 | 0.322705 | 0.617026 |

Targets average 17.89 bars (4.47 h), stops 13.39 (3.35 h). 29.95% of stops versus 13.76% of targets occur within four bars. Stops are disproportionately early and targets slower; this does not prove targets are “too slow” or identify an optimal horizon. TIMEOUT is administrative censoring at 48 bars (12 h), with +0.322705 R conditional net mean.

Full-horizon funding is charged even to early exits in the frozen labels. TIMEOUT therefore does not accumulate more duration-based funding in this dataset; its lower mean funding reflects different risk geometry. Actual event-duration carry/cost accumulation and post-censor outcomes are unavailable. Horizon mismatch is a separately versioned hypothesis, not demonstrated optimality. No horizon or label changed. [Every fold event-time bucket](artifacts/cati_v5_economic_edge_audit/event_time_buckets.csv).

## Temporal deterioration and existing feature drift

| Metric | Folds 1–2 | Folds 3–5 | Late minus early |
| --- | --- | --- | --- |
| mean_gross_R | 0.010098 | -0.021590 | -0.031688 |
| mean_cost_R | 0.131317 | 0.153007 | 0.021690 |
| mean_net_R | -0.121220 | -0.174597 | -0.053377 |
| TARGET_frequency | 0.212882 | 0.207349 | -0.005533 |
| STOP_frequency | 0.537658 | 0.549781 | 0.012123 |
| TIMEOUT_frequency | 0.249459 | 0.242869 | -0.006590 |
| predicted_mean_R | -0.165572 | -0.164654 | 0.000919 |
| roc_auc | 0.592572 | 0.579253 | -0.013319 |
| brier_skill | 0.024968 | 0.016485 | -0.008482 |

**TEMPORAL_CLASSIFICATION = MIXED.** Net deterioration is 0.053377 R: gross deterioration 0.031688 R (59.37%), increased modeled cost 0.021690 R (40.63%). This is arithmetic, not causal identification. Predicted expectancy barely changes (+0.000919 R) while realized expectancy drops. AUC declines 0.592572→0.579253; skill 0.024968→0.016485. Net-R ranking remains positive late.

The full-label calendar check also has negative net in every fold but is not perfectly monotone (fold 4 improves over fold 3). An abrupt regime break, universally gradual decay or stale history alone is not established. PSI supports distribution shift, without proving its causal source.

| Fold | Brier skill | ROC-AUC | ATR-fraction PSI | Realized-vol PSI |
| --- | --- | --- | --- | --- |
| 1 | 0.021953 | 0.586293 | 0.027079 | 0.028192 |
| 2 | 0.027985 | 0.600403 | 0.141204 | 0.124964 |
| 3 | 0.016285 | 0.577995 | 0.103686 | 0.103296 |
| 4 | 0.018363 | 0.583751 | 0.169854 | 0.142771 |
| 5 | 0.014849 | 0.575927 | 0.175138 | 0.169956 |

## NON_DEPLOYABLE_HINDSIGHT_DIAGNOSTIC

| Population | net > 0 | net > +0.25 | net > +0.50 | net > +1.00 |
| --- | --- | --- | --- | --- |
| POOLED | 0.361092 | 0.325631 | 0.293542 | 0.246285 |
| FOLD_1 | 0.375277 | 0.339483 | 0.304797 | 0.256365 |
| FOLD_2 | 0.371179 | 0.336393 | 0.303350 | 0.250574 |
| FOLD_3 | 0.358452 | 0.322762 | 0.291630 | 0.244399 |
| FOLD_4 | 0.350723 | 0.317098 | 0.282250 | 0.242307 |
| FOLD_5 | 0.347661 | 0.310262 | 0.283734 | 0.236561 |

Individual worthwhile outcomes exist (36.11% positive, 24.63% above +1 R). This is hindsight availability, not positive ex-ante conditional expectancy or predictive capability. No hindsight rule is defined.

## One primary diagnosis and next architectural decision

**PRIMARY_DIAGNOSIS = MIXED**, ranked by measured impact:

1. Cost/geometry burden: 0.143921 R cost versus -0.008316 R matched gross; narrow risks amplify ordinary modeled notional charges into large R losses.
2. Weak/deteriorating gross setup economics: full-parent gross -0.015416 R, late matched gross -0.021590 R. Stable positive gross alpha is not demonstrated.
3. Tail payoff optimism/limited gross discrimination: +0.084103 R top-quintile optimism, mainly TARGET_PROFIT contributions; relative-loss ranking does not produce stable positive net selection.

**Do not build V6 on the unchanged generator yet.** Preregister separately versioned causal setup/entry and target/stop-geometry alpha hypotheses, with economic viability gates before further ML admission. Horizon/timeframe and additional actually observable market information should be independent hypotheses, not retrospective variants optimized on this audit. Validate execution-cost and event-duration funding semantics against independent fill/funding evidence before claiming maker routing, another venue or lower turnover restores edge. Do not simply lower modeled costs.

Do not remove the bad groups or promote sparse positive cells. Do not deploy expected R >0.10 or p >0.55. A bounded direct net-R/distributional ranking experiment could follow only after a separately researched generator demonstrates stable net economics under credible costs. Adaptive recency is a future hypothesis after distinguishing setup/cost decay from stale training with predeclared temporal tests. No V6 is implemented or independently validated here.

## Engineering, runtime, FX and blockers

The first extraction rejected a raw SHA comparison against the library’s JSON-escaped-text hash; the canonical TextHasher corrected verification, without changing labels. Later numeric-only passes added diagnostics/full-parent checks, with zero new fits. A synthetic test fixture initially converted line endings on Windows; byte-exact writing corrected it. These attempts are disclosed.

198 tests passed in 17.71 s: diagnostic arithmetic and published-evidence conservation, V5 coherence, runtime authority, adaptive daily risk, execution responsibility and hard-risk controls. Exact audit-tool/input identities and all three original frozen fingerprints are unchanged. Production forecast files were not modified.

Final observation: healthy runtime PID 19436, fresh lease and all health components, paper M0, hard-loss fraction 0.025, startup-loaded revision 81aceaa6 unchanged. Existing FX supervisor 31396 and queued completion 35444 remain active; one venv writer chain (redirector 9916 → worker 19300). Latest queued status WAITING_ACQUISITION, 3,137 periods remaining at 2026-10-03T06:23:47.9146643Z. No restart, new writer or freeze claim.

Production forecast behavior is untouched. Runtime remains PAPER/M0, CATI OFF, PID 19436/session rts_a605e6301df149ccb931, startup revision 81aceaa6, hard daily loss cap 0.025. Current Git HEAD does not reload startup modules. No restart, pin, promotion or V2 fallback.

FX uses the existing supervisor PID 31396 and queued finalization PID 35444. No duplicate writer was started. Strict UNKNOWN_GAP classification and freeze only on PASS remain unchanged.

Remaining blockers: no demonstrated stable selectable positive net candidate economics; execution/cost/horizon semantics; unresolved causal origin of temporal deterioration; sparse positives and repeated development inspection; untouched holdout/governance before admission; separate FX strict-freeze completion. HOLDOUT_OPENED = NO; HOLDOUT_INSPECTED = NO; HOLDOUT_QUERY_COUNT = 0; GOVERNANCE = M0; CATI_EXECUTION = OFF.

