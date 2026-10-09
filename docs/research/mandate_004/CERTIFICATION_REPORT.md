# Mandate 004 — Daily Trend: research certification report

Generated from run `run_0b88d389f8da50dc48de5e51` (DEVELOPMENT), result hash `a48879dde9f4a9c6cdf38ea86c9e7d1ef986f3a99f56202cc83b9a7ed36f64e9`. Every figure below is read from that run's `result.json`, the research register and the dataset manifest.

## Part A — Executive verdict

| Item | Value |
|---|---|
| Mandate | MANDATE_004 (specification SHA-256 `ea02ace43ecc75ea409dd2bb3592640e0d9b77412dc4774ddf58492ccf7e4edb`) |
| Strategy family | DAILY_TREND — long-or-flat breakout ensemble on daily bars |
| Hypothesis number | 18 of 18 in the research register |
| Evaluation date | 2026-10-09T01:55:39Z |
| Research status | READY FOR HOLDOUT / BLOCKED ON AUTHORIZATION — the held-back period has not been opened |
| Certification verdict | **BLOCKED PENDING AUTHORIZATION** |
| Frozen pass rule | PENDING_HOLDOUT |
| Statistical gate | BLOCKED_PENDING_APPROVAL |
| Held-back period | RESERVED (2025-01-01 to 2026-09-30) |
| Promotion eligibility | NONE — no trading is authorized by this report; governance phase unchanged; live trading disabled |

**Major result.** Over 2020-01-01 to 2024-12-31 (development period, Balanced, base cost, as specified) the strategy returned 33.68% net (5.97% a year) with a Sharpe ratio of 0.99 and a maximum drawdown of 11.54%, from 1192 closed trades.

**Principal risks.**
- The figures above are development figures. The rules were frozen before any run, but a development result is not an independent test.
- Statistical power: with 18 hypotheses tried, this sample could only detect an annual Sharpe ratio of about 1.75 or more (illustrative 5% / 80% standard; no standard is approved).
- Deployment conflict: 86% of the entries the mandate selects have a stop wider than the engine's 15% limit and would be refused today; Balanced and Aggressive are also clamped to 0.40% risk per trade.
- The held-back period overlaps the period earlier hypotheses were developed on, and contains a window still reserved for another certification.
- Costs are the mandate's assumptions (0.05% fee, 0.05% slippage), not measured costs; capacity is unknown.
- Survivorship bias is reduced (delisted contracts are included), not proven absent.

A research pass, a governance promotion and a trading authorization are three different things. This report can only ever establish the first. It establishes none of them today.

## Part B — Strategy explanation

**What it tries to capture.** Sustained price trends in liquid crypto perpetuals: coins that keep making new highs tend, on average, to continue for a while. The strategy only buys; when nothing is trending it holds cash.

**How it finds opportunities.** Each day, among the 20 most traded USDT perpetuals with at least 120 days of history, it checks seven look-back windows (10 to 150 days). A window switches on when the close is at or above its highest close, and off when the close falls below the lowest close of half that window. Strength is the share of windows that are on.

**Entry, exit, size.** Decisions use the daily close and trade at the next open. Position size is strength × risk per trade × equity ÷ stop distance, where the stop distance is three times the 20-day average true range (at least 5%, at most 40% of price). The stop only moves up. A position is closed when its strength reaches zero, when it leaves the top 20, or when the stop is hit.

**Portfolio and risk control.** At most 4, 6 or 8 positions by risk level; total open risk capped at 1%, 2% or 3% of equity; a daily loss pauses new entries; a drawdown first halves all sizes and then closes everything.

**Why it is plausible.** Trend following has a long record across asset classes, and crypto has shown long directional moves. **Where it fails.** Sideways markets (repeated small losses), sudden reversals after a breakout, and regimes where costs and funding eat a thin edge. Because it is long-only, it earns nothing in a falling market. None of this is evidence; the evidence is in Parts D to I.

## Part C — Data integrity

| Item | Value |
|---|---|
| Dataset | `binance_usdm_daily` v1, hash `0c3392d8daf78e08244fdd21edac5a93ef0c6cfd5386e3cecbf6cb379fde284e` |
| Manifest hash | `38d940f14f98a604ffe7bd3336e51b51e07fdda571b057db19327b4019a6a450` |
| Sources | Binance public archive (daily klines, funding rates), every file checked against the archive's SHA-256 |
| Coverage | 2020-01-01 to 2026-09-30 |
| Contracts | 911 USDT-margined perpetuals; 177 of them ended before the last day |
| Raw files | 43,626 (inventory SHA-256 `d3ee73726e133679a003ea129d8d235d778dd30a45b7cdbdf3b9be09a57f3b70`) |
| Daily bars | 663,145; 56,815 have no trade and are never used as prices |
| Missing bars | 346 in 3 symbols (319 recovered from the archive's daily files) |
| Invalid / duplicate bars | 0 / 0 |
| Funding | 40,668 bar-days have no funding record, almost all after a contract stopped trading; missing funding is charged at 0.03% per 8 hours, never treated as zero |
| Funding imputed in this run | largest share over all scenarios 0.005% (limit 2%) |
| Universe method | point in time: membership decided each day from bars on or before that day |
| Universe in this run | development: 136 contracts ever members, 1665 of 1827 days with 20 members, 119 with none |
| Ended contracts that were universe members | AGIXUSDT, BAKEUSDT, BLZUSDT, EOSUSDT, FTMUSDT, HIGHUSDT, LEVERUSDT, LINAUSDT, LRCUSDT, LUNAUSDT, MATICUSDT, MKRUSDT, OCEANUSDT, OMGUSDT, PERPUSDT, REEFUSDT, RNDRUSDT, STMXUSDT, STORJUSDT, SXPUSDT, TOMOUSDT, TONUSDT, UNFIUSDT, WAVESUSDT, YFIIUSDT |
| Forced exits for missing bars (primary run) | 0 |

**Survivorship.** The symbol list comes from the archive listing, not from today's exchange list, so contracts that were delisted are in the data and can be traded and lost on by the simulation. The dataset is **not** claimed to be free of survivorship bias: listing and delisting dates are reconstructed from where the archive's bars start and stop trading, no public point-in-time listing record exists, and a contract with no archive file at all cannot be seen.

**Other limitations.** The archive starts on 2020-01-01: contracts listed in 2019 have no earlier bars here. Listing and delisting dates are reconstructed from archive coverage, not from an exchange record. After a contract is settled the archive keeps publishing bars with zero trades; they are not prices. A ticker that was settled and listed again keeps one archive symbol; its history restarts after the gap. Exchange filters (step size, minimum notional) and contract categories are today's snapshot. A contract with no archive files at all cannot be seen; survivorship bias is reduced, not proven absent. Daily bars carry no intraday order of high and low. No historical order-book, spread or depth data is part of this dataset.

## Part D — Performance

Development and held-back results are reported separately and never combined in one figure.

### Development period: 2020-01-01 to 2024-12-31

Primary scenario `MANDATE|balanced|base|brakes_on|no_overlay` (the certification basis). 1827 daily observations.

| Scenario | Net return | Annual | Volatility | Sharpe | Sortino | Max drawdown | Calmar | Trades | Hit rate | Profit factor | Days held |
|---|---|---|---|---|---|---|---|---|---|---|---|
| MANDATE / balanced / base / brakes_on / no_overlay | 33.68% | 5.97% | 6.06% | 0.99 | 1.46 | 11.54% | 0.52 | 1192 | 40.9% | 1.44 | 8.1 |
| MANDATE / balanced / cost_x2 / brakes_on / no_overlay | 30.15% | 5.41% | 5.94% | 0.92 | 1.35 | 11.89% | 0.45 | 1190 | 40.1% | 1.40 | 8.1 |
| MANDATE / balanced / base / brakes_off / no_overlay | 38.18% | 6.67% | 6.35% | 1.05 | 1.55 | 11.17% | 0.60 | 1195 | 41.1% | 1.46 | 8.1 |
| MANDATE / balanced / cost_x2 / brakes_off / no_overlay | 34.66% | 6.13% | 6.35% | 0.97 | 1.43 | 11.97% | 0.51 | 1195 | 40.1% | 1.41 | 8.1 |

- Gross return (price P&L before all costs): 40.85%; net: 33.68%.
- Fees 168.77 USDT (0.28% of equity a year), slippage 168.77 (0.28%), funding 380.02 (0.64%).
- Turnover 5.69x equity a year; in the market on 93.2% of days; average 5.3 positions; average exposure 6.3%, highest 18.8% of equity.
- Worst day -1.72%; mean of the worst 5% of days -0.75%; average win 23.18 USDT, average loss -11.13 USDT.
- Calendar years: 2020: 13.87%, 2021: 11.25%, 2022: -7.54%, 2023: 6.34%, 2024: 7.32%.
- Brake days: daily pause 0, halved 306, stopped 0 (drawdown stop fired on no day).

**Validation period.** The frozen specification defines no validation period; none is reported.

**Held-back period.** NOT EVALUATED. It has not been opened; no held-back figure exists.

**Uncertainty.** 1827 daily observations (DEVELOPMENT_PERIOD); effective sample 1,558 after serial dependence. Block-bootstrap 95% interval for the annual return: 0.57% to 11.69%.

## Part E — Per-risk-level analysis

Two policies are shown and must not be confused. **Mandate** is the frozen specification as written (risk per trade 0.25% / 0.50% / 0.75%). **Executable** is what the engine would accept today: it clamps risk per trade to 0.40% and refuses an entry whose stop is more than 15% away. Balanced and Aggressive therefore risk the same 0.40% per trade today; the 0.50% and 0.75% figures are not deployable until the owner decides RISK-01.

### Development: 2020-01-01 to 2024-12-31 (base cost, brakes on)

| Policy | Level | Approved risk | Effective risk | Net return | Annual | Sharpe | Max drawdown | Trades | Entries refused (stop > 15%) | Drawdown stop |
|---|---|---|---|---|---|---|---|---|---|---|
| Mandate | Conservative | 0.25% | 0.25% | 16.14% | 3.04% | 1.01 | 5.08% | 912 | 0 | no |
| Mandate | Balanced | 0.50% | 0.50% | 33.68% | 5.97% | 0.99 | 11.54% | 1192 | 0 | no |
| Mandate | Aggressive | 0.75% | 0.75% | 52.65% | 8.82% | 0.96 | 15.82% | 1429 | 0 | no |
| Executable | Conservative | 0.25% | 0.25% | 6.38% | 1.24% | 0.98 | 2.16% | 141 | 5857 | no |
| Executable | Balanced | 0.50% | 0.40% | 12.59% | 2.40% | 0.95 | 4.23% | 202 | 8636 | no |
| Executable | Aggressive | 0.75% | 0.40% | 20.19% | 3.74% | 1.22 | 4.40% | 249 | 11230 | no |

## Part F — Benchmarks

The two benchmarks named by the specification, over the same period, scaled after the fact to the strategy's realised volatility (a comparison device, not a tradable portfolio).

### Development: 2020-01-01 to 2024-12-31

| Series | Net return | Annual | Volatility | Sharpe | Max drawdown | Scale |
|---|---|---|---|---|---|---|
| Strategy (primary) | 33.68% | 5.97% | 6.06% | 0.99 | 11.54% | — |
| Bitcoin buy-and-hold (perpetual, with funding) — unscaled | 532.69% | 44.56% | 65.31% | 0.90 | 78.93% | 1.00 |
| Bitcoin buy-and-hold (perpetual, with funding) — scaled | 30.13% | 5.40% | 6.06% | 0.90 | 11.39% | 0.093 |
| Equal-weight universe — unscaled | 83.52% | 12.90% | 83.19% | 0.57 | 88.24% | 1.00 |
| Equal-weight universe — scaled | 17.75% | 3.32% | 6.06% | 0.57 | 11.02% | 0.073 |

## Part G — Portfolio overlay

Secondary test of the specification: halve a coin's target when its trailing 3-day funding is above 30% a year, set it to zero above 60%. It is reported separately and cannot rescue a fail. The drawdown brakes are the other overlay; their effect is the difference between the brakes-on and brakes-off rows in Part D.

### Development

| Level | Net, signal only | Net, with overlay | Sharpe | Sharpe overlay | Max DD | Max DD overlay | Funding paid | Funding paid overlay |
|---|---|---|---|---|---|---|---|---|
| Conservative | 16.14% | 14.16% | 1.01 | 1.06 | 5.08% | 5.06% | 86.23 | -85.14 |
| Balanced | 33.68% | 30.82% | 0.99 | 1.04 | 11.54% | 11.56% | 380.02 | 22.26 |
| Aggressive | 52.65% | 56.62% | 0.96 | 1.13 | 15.82% | 17.05% | 633.32 | 110.79 |

Any difference between the two columns comes from the overlay, not from the trend signal.

## Part H — Robustness

Only the robustness checks the specification registers are run. No parameter was varied: the specification authorizes no parameter search, and none was made.

### Development

- **Cost sensitivity.** Net return 33.68% at base cost, 30.15% at twice the cost.
- **Funding sensitivity.** Funding cost 0.64% of equity a year; fees and slippage together 0.57%.
- **Market regimes.** Positive complete calendar years: 2020, 2021, 2023, 2024; zero or negative: 2022.
- **Drawdown brakes.** Maximum drawdown 11.54% with the brakes, 11.17% without.
- **Instrument concentration.** 119 different contracts traded; largest single winning trade 262.76 USDT, largest loss -52.58 USDT, against a net result of 3,367.80 USDT.
- **Delisted contracts.** 25 ended contracts were universe members at some point and were tradable by the simulation.
- **Liquidity.** Largest order was 0.0046% of that day's traded volume at the simulated account size (Part J).

- **Parameter sensitivity.** Not evaluated: not authorized by the mandate.
- **Implementation assumptions.** Listed in `research/trend_v1/INTERPRETATION_001.md` and `INTERPRETATION_002.md`, both registered before the first run.

## Part I — Statistical gate

### The frozen pass rule (binding; all four at Balanced)

| Criterion | Status | Observed |
|---|---|---|
| 1 holdout net return positive | PENDING_HOLDOUT | null |
| 2 positive calendar years | PASS | {"of_known": 5, "positive": 4, "required": 4, "years": {"2020": 0.13871863815425178, "2021": 0.11247479867736265, "2022": -0.07540126879397768, "2023": 0.06344109451919566, "2024": 0.07321493639654975}, "years_not_covered": 1} |
| 3 max drawdown without brake | PENDING_HOLDOUT | {"limit": 0.15, "max_drawdown": 0.1117462805240651, "period": "DEVELOPMENT_ONLY"} |
| 4 holdout net sharpe | PENDING_HOLDOUT | null |

Pass rule: **PENDING_HOLDOUT**

### The portfolio-level statistical gate

| Item | Value |
|---|---|
| Method | one-sided circular block bootstrap of portfolio daily net returns |
| Null hypothesis | mean daily net return <= 0 |
| Sample | DEVELOPMENT_PERIOD: 1827 days, effective 1,558 |
| Effect size (annualised Sharpe) | 0.99 |
| p-value, one test | 0.01465 |
| Multiple-testing adjustment | Bonferroni over 18 hypotheses in the register |
| p-value, adjusted | 0.26370 |
| Adjusted p-value if the count were 19 / 24 | 0.27835 / 0.35160 |
| Deflated Sharpe ratio | 0.5802 |
| Smallest annual Sharpe this sample could detect | 1.75 (ILLUSTRATIVE_5PCT_80PCT_NOT_APPROVED: level 0.00278 per test, power 0.80) |
| Power to detect a Sharpe of 0.5 / 1.0 | 4.1% / 24.0% |
| Thresholds | NOT APPROVED — significance level, required power and target effect are unset |
| Result | **BLOCKED_PENDING_APPROVAL** ['STATISTICAL_GATE_THRESHOLDS_NOT_APPROVED'] |

**Why there is no verdict yet.** Two of the four criteria are defined on the held-back period, which has not been opened. Nothing on the development data already fails the rule (status: PENDING_HOLDOUT). The statistical gate is BLOCKED_PENDING_APPROVAL.

## Part J — Capacity assessment

Measured on 2003 simulated orders (development period): order size as a share of that day's traded volume, scaled linearly with account size.

| Account equity (USDT) | Median order | 95th percentile | Largest order | Orders above 1% of daily volume |
|---|---|---|---|---|
| 10,000 | 0.0000% | 0.0002% | 0.0046% | 0.0% |
| 100,000 | 0.0001% | 0.0019% | 0.0457% | 0.0% |
| 1,000,000 | 0.0011% | 0.0188% | 0.4571% | 0.0% |
| 10,000,000 | 0.0107% | 0.1881% | 4.5709% | 1.1% |
| 100,000,000 | 0.1067% | 1.8813% | 45.7092% | 9.0% |

**This is not a capacity estimate.** daily volume is not depth: no order-book, spread or impact data exists in the dataset; the frozen 0.05% slippage is not scaled with size. These figures bound participation, not capacity. A defensible capacity figure needs order-book depth and observed market impact, which do not exist for this history. The table shows where participation stops being negligible; it does not show how much capital the strategy can carry.

## Part K — Rejection and veto accounting

### Development (Balanced)

**Raw strategy decisions (mandate).** 1251 new positions were selected by the rule; 1251 were opened. Stop distance: median 25.4%, middle half 18.1% to 34.9%, range 5.0% to 40.0%; 2 at the 5% floor, 205 at the 40% cap. **1072 of 1251 (85.7%) have a stop wider than the engine's 15% limit.**

| Policy | Not selected (position limit) | Refused: stop > 15% | Refused: daily pause | Refused: exchange minimum | Refused: no bar | Trades | Net return | Sharpe | Max drawdown |
|---|---|---|---|---|---|---|---|---|---|
| Mandate | 16186 | 0 | 0 | 171 | 0 | 1192 | 33.68% | 0.99 | 11.54% |
| Executable | 16186 | 8636 | 0 | 47 | 0 | 202 | 12.59% | 0.95 | 4.23% |

After the executable-policy constraints the result changes from 33.68% to 12.59% net. Refused entries are not removed from the statistics: they are counted above, and the executable run is a separate simulation of what would have traded, not an edit of the mandate run.

**Liquidity and data-quality vetoes.** No liquidity veto exists in the mandate (membership of the top 20 by volume is its liquidity rule). Data-quality exits are counted in Part C.

**The 55-of-74 finding.** Step 1 found that 55 of 74 recorded decisions had a stop wider than the engine's 15% maximum. Those records belong to the residual momentum family (hypothesis 15, failed), not to this mandate. They are forward observation records, not executable plans: each is written in OBSERVE mode with entry authority blocked, and the count includes counterfactual candidates that were never portfolio-selected. The stop geometry is as designed (the wider of the prior-24h swing plus a quarter ATR, twice ATR, and 0.3% of price), measured from the decision close; the engine measures the same fraction from the live price at submission, so the units agree and the reference price differs slightly. Every plan beyond 15% is rejected, never clipped. In the committed historical evaluation of that family, 189 of 326 selected trades (58%) had a stop wider than 15%; mean net result 0.0728 R over all trades, 0.1141 R over the 137 within the limit and 0.0428 R over those beyond it. This is a description of a family that already failed its gate, not a new test, and it changes no verdict. For the daily trend mandate the same conflict exists by construction and is measured above.

## Part L — Complete experiment register

Research register: 27 records, head `d35c4a31825be3e9347331b8e025936786c5a12befd12fa271126f7034e3c810`. Hypothesis count used for multiple testing: 18.

### Hypotheses

| # | Identity | Family | Mandate | Status |
|---|---|---|---|---|
| 1 | cati_lib_0ead8264955eb958b20e165e (V1) + cati_lib_80348290bf88005f4b2c | FORECAST_OUTCOME_LIBRARY | NONE_RECORDED | REJECTED_PRE_HOLDOUT |
| 2 | cati_v3_f97c1349086b6c1068ee49c0 (RESEARCH_CANDIDATE_V3_INFORMATION_CO | FORECAST_OUTCOME_LIBRARY | NONE_RECORDED | REJECTED_PRE_HOLDOUT |
| 3 | cati_v4_a71dc63a483d307c3b6cfbeb (RESEARCH_CANDIDATE_V4_EDGE_AND_PAYOF | FORECAST_OUTCOME_LIBRARY | NONE_RECORDED | REJECTED_PRE_HOLDOUT |
| 4 | cati_v5_b131674954a223230581dbbc (RESEARCH_CANDIDATE_V5_COHERENT_JOINT | FORECAST_OUTCOME_LIBRARY | NONE_RECORDED | REJECTED_PRE_HOLDOUT |
| 5 | BREAKOUT_ACCEPTANCE_V1_5m_H72 | CATI_ALPHA_CONFIRMED_STRUCTURE_V1 | NONE_RECORDED (research attempt 1, inferred  | FAILED |
| 6 | TREND_RECLAIM_V1_5m_H72 | CATI_ALPHA_CONFIRMED_STRUCTURE_V1 | NONE_RECORDED (research attempt 1, inferred  | FAILED |
| 7 | BREAKOUT_ACCEPTANCE_V1_15m_H32 | CATI_ALPHA_CONFIRMED_STRUCTURE_V1 | NONE_RECORDED (research attempt 1, inferred  | FAILED |
| 8 | TREND_RECLAIM_V1_15m_H32 | CATI_ALPHA_CONFIRMED_STRUCTURE_V1 | NONE_RECORDED (research attempt 1, inferred  | FAILED |
| 9 | BREAKOUT_ACCEPTANCE_V1_1h_H16 | CATI_ALPHA_CONFIRMED_STRUCTURE_V1 | NONE_RECORDED (research attempt 1, inferred  | FAILED |
| 10 | TREND_RECLAIM_V1_1h_H16 | CATI_ALPHA_CONFIRMED_STRUCTURE_V1 | NONE_RECORDED (research attempt 1, inferred  | FAILED |
| 11 | CROSS_SECTIONAL_RELATIVE_STRENGTH | CATI_ALPHA_CAUSAL_DIVERSIFIED_V2 | NONE_RECORDED (research attempt 2, inferred  | FAILED |
| 12 | VOLATILITY_TRANSITION_CONTINUATION | CATI_ALPHA_CAUSAL_DIVERSIFIED_V2 | NONE_RECORDED (research attempt 2, inferred  | FAILED |
| 13 | MULTI_TIMEFRAME_TREND_PULLBACK | CATI_ALPHA_CAUSAL_DIVERSIFIED_V2 | NONE_RECORDED (research attempt 2, inferred  | FAILED |
| 14 | FLOW_CONFIRMED_DIRECTIONAL | CATI_ALPHA_CAUSAL_DIVERSIFIED_V2 | NONE_RECORDED (research attempt 2, inferred  | FAILED |
| 15 | RESIDUAL_MOMENTUM_PORTFOLIO_TOP1 | CATI_NEXT_EDGE_DISCOVERY | CATI_NEXT_EDGE_DISCOVERY_MANDATE_003 | FAILED |
| 16 | DISPERSION_BREAK_RESIDUAL_RELATIVE_VALUE | CATI_NEXT_EDGE_DISCOVERY | CATI_NEXT_EDGE_DISCOVERY_MANDATE_003 | FAILED |
| 17 | SETTLED_FUNDING_BASIS_RELATIVE_CARRY | CATI_NEXT_EDGE_DISCOVERY | CATI_NEXT_EDGE_DISCOVERY_MANDATE_003 | FAILED |
| 18 | H-018 | DAILY_TREND | MANDATE_004 | REGISTERED |

### Mandate 004 registration and amendments

| Record | Detail |
|---|---|
| Registered | 2026-10-09T01:14:59Z by Claude (coding agent), on the project owner's instruction; approved by Project owner (Section H Step 2 assignment of 9 October 2026, sections 4.1 and 4.2: 'Register: Mandate 00…) |
| Specification | `research/trend_v1/SPEC.md` SHA-256 `ea02ace43ecc75ea409dd2bb3592640e0d9b77412dc4774ddf58492ccf7e4edb` |
| Rule artifact hash | `7795ad79de962300351fcf4d419cd511256dce356c9ee46cdbbc21842582cbb9` |
| Amendment 1 | IMPLEMENTATION_INTERPRETATION; changes strategy rules: False; `research/trend_v1/INTERPRETATION_001.md` SHA-256 `cd252fc540a861c5da0958c6c7394d10cecb26bc6d2eb1255343d062512745ec` |
| Amendment 2 | IMPLEMENTATION_INTERPRETATION; changes strategy rules: False; `research/trend_v1/INTERPRETATION_002.md` SHA-256 `f0dc1853e4f94424c006e62b60224e5e18ef4537101d7c5fd58b018a59e97d42` |

### Runs (every run that was started, failures included)

| Run | Type | State | Verdict | Code commit | Parent | Reason for rerun / error |
|---|---|---|---|---|---|---|
| run_0b88d389f8da50dc48de5e51 | DEVELOPMENT | COMPLETED | PENDING_HOLDOUT | 9d4aadb34d | — | — |

### Holdout access

| Event | Detail |
|---|---|
| Reserved | `hold_36c4dece60da11fddee1a98e` 2025-01-01 to 2026-09-30 |
| Authorization | NONE RECORDED |
| Opened | NOT OPENED |
| Result stored | — |

## Part M — Final recommendation

**BLOCKED PENDING AUTHORIZATION**

The evaluator, the dataset and the governance are in place and the development run is valid. The strategy has not been certified and has not failed: its two decisive criteria need the held-back period, which only the project owner can authorize. Even if those criteria pass, certification additionally needs an approved statistical standard, and the report shows that the available history cannot reach a conventional one. CATI stays in shadow: no family is certified, so Step 3 may build the pipeline but may not let any family place an order.

---

Machine-readable result: `docs/research/mandate_004/certification_result.json` (schema `cati-certification-result-v1`, content hash `c3536114165b27e549e984d419f62ff34ba099a50c7351bee34966cd0d525806`). Reproduction: `docs/research/mandate_004/REPRODUCTION.md`.
