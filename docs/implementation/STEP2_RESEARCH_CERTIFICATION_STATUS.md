# Section H, Step 2 — Research governance and strategy certification: implementation status

Authority: the Step 2 assignment of 9 October 2026 (work items 2.1 to 2.7) and the frozen specification
`research/trend_v1/SPEC.md`. The CosmicForge Master Plan A to Z (8 October 2026) is **not in the repository**
and was not found on this workstation; nothing below is presented as a quotation of it, and every decision it
would have to supply is listed as open.

Deliverables D1 (this tracker) and D14 (closure, last section).

## Outcome

| | |
|---|---|
| **STEP 2 IMPLEMENTATION** | **PARTIAL** — six of seven work items meet their evidence requirement; 2.5 is ready and stops at the holdout gate |
| **MANDATE 004 CERTIFICATION** | **BLOCKED** — the held-back period has not been opened; opening it needs the project owner's recorded authorization |
| **STRATEGY VERDICT** | none yet: not certified, not failed. The development run does not fail the frozen pass rule |
| **LIVE TRADING** | disabled throughout; no order of any kind was placed; governance phase unchanged (M0) |

## Baseline

| Check | Result |
|---|---|
| Repository and branch | `ainzkoji/cosmicforge-bot`, `main` |
| Starting commit | `8dcf1c1` = `origin/main`, working tree clean, CI green on it (run 37861709781) |
| Remote changes since | none (`git fetch` before starting) |
| Step 1 | untouched; its carry-forward gates are unchanged |

## Work-item register

| Item | Status | Evidence | Commit |
|---|---|---|---|
| 2.1 Research governance | **PASS** | register with 18 hypotheses, statistical gate, admission route (disabled), 32 tests | `e897f88` |
| 2.2 Mandate 004 / hypothesis 18 | **PASS** | register records 19 to 22 (registration, two amendments), hash pins, 7 tests | `7d8cea0`, `dd455f7`, `a794792` |
| 2.3 Historical dataset | **PASS** | frozen manifest, coverage, inventory of 43,626 verified files, quality report, 16 tests | `a794792` |
| 2.4 Research evaluator | **PASS** | rules, portfolio rule, simulator, 57 synthetic tests passing before any run on market data | `9d4aadb` |
| 2.5 Development and holdout | **READY FOR HOLDOUT / BLOCKED ON AUTHORIZATION** | valid development run, independent recomputation, from-scratch reproduction, readiness READY, holdout reserved and unopened | `6f7fad0` |
| 2.6 Research report | **PASS** (development stage; verdict BLOCKED PENDING AUTHORIZATION) | `CERTIFICATION_REPORT.md` and `certification_result.json` generated from the run, 9 tests | `9d4aadb`, `6f7fad0` |
| 2.7 Measured market costs | **PASS** (measurement system); observed-cost calibration **INSUFFICIENT_DATA** | records, provenance, cost table, versioned calibration, 8 tests; 242 observations | `d7deba6` |

### 2.1 Research governance

- **Requirement.** One authoritative record of every hypothesis; the seventeen earlier ones recognised; a
  registered portfolio-level test with explicit power; no unapproved admission route.
- **Defect found.** The multiple-testing count came from `cati_experiment_registry`, a table in a gitignored
  database filtered by dataset hash. It held zero rows in every research database on the workstation, so a
  certification run would have counted between 1 and 5 trials whatever had been tried.
- **Implementation.** `research/governance/`: `register.py` (one committed, append-only, hash-chained file with
  per-type rules and an anchor on the imported history), `history.py` (one-time, resumable import),
  `statistics.py` (daily net returns, effective sample size, block bootstrap, Bonferroni over the whole
  register, power; six outcomes), `mandates.py`, `holdout.py`, `admission.py`. The Section 22 pipeline now uses
  at least the register's count.
- **The seventeen.** The number is given by the assignment. No document in the repository states it or lists
  them. They were reconstructed under a written counting rule (4 forecast-model candidates + 6 + 4 + 3); two
  other readings also give seventeen and one gives eighteen. All are recorded in the import record with
  `UNKNOWN` wherever the evidence does not support a field.
- **Statistical thresholds.** Not given by the frozen specification; the master plan is unavailable. They are
  unset, so the gate reports `BLOCKED_PENDING_APPROVAL`. It cannot pass by default.
- **Rule-based admission route.** Defined and tested; disabled until an owner decision is recorded.
  `governance/phases.py` is unchanged.
- **Documents.** `docs/research/CATI_RESEARCH_GOVERNANCE.md` (D2); `docs/research/registry/` (D3).
- **Next action.** Owner: confirm the enumeration; decide the statistical standard and the admission route.

### 2.2 Mandate 004

- **Authority.** `research/trend_v1/SPEC.md`, frozen 2026-10-07 on `origin/step1-portal-to-engine` (`f85a6cf`),
  brought to `main` unchanged (`7d8cea0`, same blob). No conflicting specification exists.
- **Pins.** Specification SHA-256 `ea02ace4…4edb`; rule artifact `7795ad79…cbb9`; research code commit `e897f88`.
- **Amendments (no rule changed).** 1: twenty points the text leaves open, fixed before any price series was
  loaded. 2: a bar with zero trades is not a bar; days the monthly files skip come from the archive's daily
  files. Decided from counts of bars only, before the first run.
- **Order.** Registration (record 20), amendments (21, 22), dataset (23), cost model (24), then the first run
  (26). Verified by test.
- **Missing mandate parameters.** The frozen text leaves 21 points open (how ATR is averaged, which price
  sizes an order, what happens to a coin that leaves the top 20, how funding is timed, what a bar without a
  trade is ...). The assignment says such a point is a missing parameter that needs approval. They were not
  left open and they were not chosen from results: each was fixed in writing by the engineering agent before
  any price series was loaded, and registered. **They have not been approved by the project owner.** The
  `authorization_reference` on the two amendment records points to the instruction to register and freeze
  the mandate; it is not an approval of the individual points. The development run was made under them. If
  the owner rejects a point, the run stays in the log as it is and the changed specification is a new
  hypothesis (the frozen text's own rule). The statistical gate's significance level, power and target effect
  were left unset rather than chosen.
- **Documents.** D4: the specification, both interpretation files, the register.

### 2.3 Dataset

- **Result.** `binance_usdm_daily` v1: 911 USDT perpetuals, 2020-01-01 to 2026-09-30, 663,145 daily bars,
  2,735,181 funding records; 43,626 files verified against the archive's SHA-256; 0 failed, 0 invalid,
  0 duplicate; 177 contracts that ended are included.
- **Gaps.** 346 missing bar-days in three symbols after recovery from daily files; 56,815 bars with no trade,
  never used as prices; funding gaps on traded bars in three tickers.
- **Limitations stated.** Listing dates reconstructed; exchange filters and categories are today's snapshot;
  not claimed free of survivorship bias; the archive starts in 2020.
- **Documents.** D5: the manifest; D6: `docs/research/datasets/binance_usdm_daily_v1.QUALITY.md`.
- **Storage.** Raw files and tables stay local (280 MB, reproducible); manifest, coverage, inventory and
  metadata are committed.

### 2.4 Evaluator

- **Where.** Rules in `app/trading_intelligence/families/daily_trend/` (shared with Step 3); accounting and the
  official run in `app/trading_intelligence/research/evaluator/`. No second strategy engine; no specialist was
  registered; no execution path was touched.
- **What is modelled.** Next-open fills; fee and slippage on the traded amount; stops at the stop price or at
  the open when the day opens below it; signed funding per event; missing funding charged; data-gap exits;
  exchange minimums; position limit, open-risk cap, leverage cap; the three brakes.
- **Risk levels.** Conservative, Balanced, Aggressive under two policies that are never merged: the mandate as
  written, and the engine's current limits (0.40% risk per trade, no entry with a stop beyond 15%). The 0.40%
  ceiling and the 15% limit were not changed.
- **Tests before real data.** 57 synthetic tests including hand-computed accounts, gap-through stops, funding
  sign, missing candles, delisting, new listings, capital limits, deterministic replay, randomized financial
  invariants and adversarial no-lookahead (D8).

### 2.5 Development and holdout

- **Development run.** `run_0b88d389f8da50dc48de5e51`, 2020-01-01 to 2024-12-31. Valid. Causality audit
  identical at three cuts under truncation and under a rewritten future. Ledgers close to 2.4e-10.
- **Defect search.** An independent script that imports none of the evaluator recomputed 60 entry decisions,
  60 trades with their funding, and all 1,827 daily ledger rows: all agree. A from-scratch reproduction gave
  the same run id and result hash. No implementation defect was found; no rerun was needed; no parameter was
  varied.
- **Run log.** The research register: one run started, one completed, none failed (D9).
- **Readiness.** `pre_holdout_readiness.json`: READY, twelve facts, hash `1b266a00…f3a4`.
- **Holdout.** Reserved (`2025-01-01` to `2026-09-30`), **not authorized, not opened**. No held-back row was
  loaded by any code path.
- **Blocker.** The project owner's recorded authorization.
- **Next action.** See "Exact next action".

### 2.6 Report

- **Generated, not written.** `docs/research/mandate_004/CERTIFICATION_REPORT.md` (D10) and
  `certification_result.json` are produced from the run's `result.json`, the register and the manifest.
- **Verdict in the report.** BLOCKED PENDING AUTHORIZATION. Development figures only; the held-back period is
  shown as not evaluated.
- **After the holdout** the same command regenerates the report from the held-back run.

### 2.7 Measured costs

- **Implemented.** Record contract, five provenances, validation, append-only store, cost table, versioned
  calibration; public-endpoint collector; read-only extraction of stored demo fills without identifiers.
- **Observed today.** 240 top-of-book snapshots of the 20 most traded contracts in one two-minute window
  (median of medians 1.2 bps); 2 demo fills from the Step 1 certification (fee 0.04% of notional; no order
  book was captured with them); the archive's funding history for the development period.
- **Calibration.** INSUFFICIENT_DATA: one short window on one day. The frozen mandate costs are retained.
- **Documents.** D11: `docs/research/mandate_004/market_costs/`.

## Strategy numbers (development period only)

Balanced, mandate as written, base cost, brakes on, 2020-01-01 to 2024-12-31, 10,000 USDT:

| | |
|---|---|
| Net return | +33.68% (5.97% a year); gross +40.85% |
| Volatility / Sharpe / Sortino | 6.06% / 0.99 / 1.46 |
| Maximum drawdown | 11.54% (11.17% without the drawdown brakes) |
| Calendar years | 2020 +13.87%, 2021 +11.25%, 2022 −7.54%, 2023 +6.34%, 2024 +7.32% |
| Trades | 1,192 closed; hit rate 40.9%; profit factor 1.44; average holding 8.1 days |
| Costs on 10,000 USDT | fees 168.77, slippage 168.77, funding 380.02 |
| At twice the cost | net +30.15% |
| Exposure | 6.3% of equity on average, 18.8% at most |
| Block-bootstrap 95% interval, annual return | +0.57% to +11.69% |
| p-value, one test / adjusted for 18 hypotheses | 0.0147 / 0.264 |
| Smallest Sharpe this sample could detect (illustrative 5% / 80%) | 1.75 |

| Level | Mandate: net / Sharpe / max DD | Executable today: net / Sharpe / max DD / trades |
|---|---|---|
| Conservative | +16.14% / 1.01 / 5.08% | +6.38% / 0.98 / 2.16% / 141 |
| Balanced | +33.68% / 0.99 / 11.54% | +12.59% / 0.95 / 4.23% / 202 |
| Aggressive | +52.65% / 0.96 / 15.82% | +20.19% / 1.22 / 4.40% / 249 |

Benchmarks over the same period, scaled to the strategy's volatility: Bitcoin buy-and-hold +30.13% (Sharpe
0.90, unscaled +532.7% with a 78.9% drawdown); equal-weight universe +17.75% (Sharpe 0.57).

These are development figures. They are not an independent test and they certify nothing.

Reading the report's refusal counts: the column "Entries refused (stop > 15%)" counts refusals per decision
day (8,636 at Balanced under the executable policy: the same contract is refused again each day it is
selected). The number of distinct new positions the mandate selected is 1,251, of which 1,072 (86%) had a
stop beyond 15%.

## The 55-of-74 finding

The 74 records are forward observation records of the residual momentum family (hypothesis 15, failed), written
in OBSERVE mode with entry authority blocked, and include counterfactual candidates. They are not executable
plans. The geometry is as designed; the engine rejects, never clips. In that family's committed historical
evaluation 189 of 326 selected trades (58%) had a stop beyond 15% (`residual_stop_distance.json`). For the
daily trend mandate the same conflict is larger: 86% of selected entries.

## Tests

Filled in under "Closure" below.

## Security and trading safety

- `LIVE_ORDER_SUBMISSION_ENABLED` unchanged; no live or demo order was placed; no execution flag, risk ceiling
  or governance phase was changed.
- No credential was read. Network access was to public endpoints only: the Binance public archive, its bucket
  listing, and public `exchangeInfo`, `ticker/bookTicker`, `premiumIndex`, `ticker/24hr`.
- The runtime database was opened read-only for one purpose: the six `cati_execution_attempts` rows of the
  Step 1 demo certification. No account, user or bot identifier was copied.
- Downloaded archives are parsed in memory as CSV; nothing is extracted to disk or executed; no pickle.
- No API route was added.

## Open decisions (project owner)

0. **Ratify or reject the 21 interpretation points** in `research/trend_v1/INTERPRETATION_001.md` and
   `INTERPRETATION_002.md` (see 2.2). This comes before the holdout: the held-back period should be opened
   only for a mandate whose every parameter the owner has accepted.
1. **Holdout authorization** for Mandate 004, knowing that its window overlaps the period earlier hypotheses
   were developed on and contains the window (2026-07-12 to 2026-09-23) still reserved for the Section 22
   certification of the forecast-driven system.
2. **Statistical standard** for the portfolio-level gate. At 5% familywise and 80% power with 18 hypotheses,
   the 1.75-year held-back period can only detect a Sharpe ratio of about 2.7. No realistic trend strategy
   can be certified at that standard on the history that exists.
3. **Rule-based admission route** (Section L): without it a rule-based family cannot reach M6.
4. **The enumeration of the seventeen earlier hypotheses.**
5. **RISK-01 and the 15% stop limit** as they apply to this family: under today's limits 86% of its entries
   are refused.
6. **The master plan**: add it to the repository so its requirements can be checked against their source.

## Exact next action

The project owner decides item 1. To authorize, after reading `CERTIFICATION_REPORT.md` and
`pre_holdout_readiness.json`:

```bash
cd backends/bot-backend && ../venv/Scripts/python.exe -m app.trading_intelligence.research.governance authorize-holdout --readiness ../../docs/research/mandate_004/pre_holdout_readiness.json --holdout-id hold_36c4dece60da11fddee1a98e --authorized-by "NAME" --reference "WHERE THE APPROVAL WAS GIVEN" --reason "final evaluation of Mandate 004" --acknowledgements "{\"interpretations_001_and_002_approved\": true, \"holdout_overlaps_earlier_development_period\": true, \"contains_window_reserved_for_section_22\": true}"
```

The holdout id is also in `pre_holdout_readiness.json` (`evidence.holdout_id`). Then the held-back evaluation runs
once (`python -m app.trading_intelligence.research.evaluator holdout --researcher "NAME"`), the report is
regenerated, and the verdict is recorded on hypothesis 18. If the owner declines, hypothesis 18 stays
registered without a verdict and the next candidate is registered as hypothesis 19.
