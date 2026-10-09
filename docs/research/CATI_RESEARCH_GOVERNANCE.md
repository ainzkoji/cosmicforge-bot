# CATI research governance specification

Section H, Step 2.1 (deliverable D2). Version 1, 9 October 2026.

Code: `backends/bot-backend/app/trading_intelligence/research/governance/`.
Tests: `backends/bot-backend/tests/trading_intelligence/test_step2_research_governance.py`.
Register: `docs/research/registry/research_register.jsonl`.

## Authority and what is missing

The CosmicForge Master Plan A to Z (8 October 2026) is **not in the repository** and was not found on this
workstation. This specification is therefore built from three sources only: the Step 2 assignment of 9 October
2026, the frozen specification `research/trend_v1/SPEC.md`, and the code and evidence already in the
repository. Where the master plan or its Section L would have to supply a number or a decision, this document
says so and the code fails closed. Nothing below is presented as a master-plan requirement.

## 1. Hypothesis accounting

### The defect that was repaired

The certification code took its multiple-testing count from `cati_experiment_registry`, a table in a
gitignored SQLite file, filtered by dataset hash. On 9 October 2026 that table held **zero rows** in every
research database on the workstation, while thirteen labelled hypotheses and four forecast-model candidates had
been evaluated by scripts that never wrote to it. A certification run would have used a count between 1 and 5
whatever had been tried before, and the count restarted for every new dataset and every new machine.

### The authoritative register

One committed, append-only file is now the single source of hypothesis accounting. Each line is a record
carrying the hash of the previous one, so editing, reordering or removing an earlier record breaks every later
hash. Because cutting off the end of a file leaves a valid chain, readers pin an anchor: the last imported
historical record (`HISTORICAL_ANCHOR` in `history.py`) must still be present with the same hash.

Rules enforced when a record is written, under an exclusive lock:

| Rule | Refusal |
|---|---|
| Hypothesis numbers run 1, 2, 3 … | a skipped, reused or reassigned number |
| One record per identity | a second registration of the same hypothesis |
| Every field of the record is present | a missing field; what cannot be supported is the literal `UNKNOWN` |
| Status never mutates | a later status record supersedes an earlier one; both remain |
| A mandate is registered once, for an existing hypothesis | a duplicate mandate id; a second mandate for a hypothesis |
| A run needs a registered mandate | an unregistered evaluation |
| A run starts once and ends once | a replaced result; a rerun without a parent and a reason |
| A holdout goes RESERVED → AUTHORIZED → OPENED → BURNED | any other order, any repetition |

The count used for multiple testing is the number of hypothesis records, whatever became of them. It never
shrinks. The Section 22 pipeline (`certification/pipeline.py`) now uses at least that count plus the run itself.

### The seventeen earlier hypotheses

The assignment states that seventeen hypotheses preceded this one. **No document in the repository states that
number or lists them.** The register therefore records the counting rule it used:

> A hypothesis is one separately identified trading or forecasting idea that was given its own frozen
> evaluation and its own recorded verdict. Sub-variants searched inside one registered candidate, and a
> re-scoring that adds no information, belong to that candidate.

| Numbers | What | Verdict |
|---|---|---|
| 1 | Forecast outcome library V1, with its V2 causal re-scoring | rejected before holdout |
| 2 – 4 | Forecast candidates V3, V4, V5 | rejected before holdout |
| 5 – 10 | Alpha V1: two mechanisms on 5m, 15m and 1h | failed (two had no data) |
| 11 – 14 | Alpha V2: four mechanisms | failed |
| 15 – 17 | Mandate 003: residual momentum, dispersion break, funding carry | failed |

Two other readings of the evidence also give seventeen and one gives eighteen; they are written into the import
record with the difference each makes. Items that are visible but not numbered (the four baseline setups, model
sub-families, superseded generations) are listed there too, with the reason. The source document is
`docs/research/registry/historical_hypotheses_v1.json`; the files it points to were not moved or changed.

**Open for the project owner:** confirm this enumeration, or supply the one the master plan uses. The count
feeding the statistics is 18 under this rule; the statistical output also reports 19 and 24 as a sensitivity.

## 2. Mandate registration

A mandate is registered before any run, as one immutable record that pins the SHA-256 of the frozen
specification (computed with line endings normalised, so it equals the hash Git stores on any platform), the
hash of the machine-readable rule set the code executes, and the commit it was registered on. Every evaluation
recomputes these and refuses to run on a difference.

A permitted clarification is a numbered amendment with its own pinned file. An amendment that changes a
strategy rule is recorded, and from then on every evaluation of that mandate is refused: a changed rule is a new
hypothesis with a new number.

## 3. The portfolio-level statistical gate

### What it replaces and why

The Section 22 gates judge the per-trade result of a forecast-driven setup. A rule-based portfolio has no
forecast to calibrate, its trades overlap and are correlated, and its risk is taken at portfolio level. The unit
of evidence is therefore the **portfolio's daily net return**: after fees, slippage and funding, all instruments
together. Correlation between instruments and between signals is inside that series.

### Method

| Concern | Treatment |
|---|---|
| Unit of independent evidence | daily net returns, reduced to an effective sample size |
| Serial dependence | lag autocorrelations; effective n = n / (1 + 2 × sum of leading positive pairs), never above n |
| Significance | CATI's canonical circular block bootstrap (block length ⌈n^⅓⌉, seed 22022), one-sided, H0: mean ≤ 0 |
| Multiple attempts | Bonferroni over every hypothesis in the register; deflated Sharpe reported beside it |
| Costs | the series is net; a gross series is not accepted as evidence |
| Too few observations | fewer than 250 daily returns, zero trades, zero variance or missing days: insufficient data |
| Uncertainty | bootstrap 95% interval for the mean and annual return |
| Power | probability of detecting a stated annual Sharpe at the adjusted level; smallest detectable Sharpe |

Bonferroni was chosen because it holds under any dependence between attempts and needs no estimate of how
variable past results were, which the historical record cannot give.

### Outcomes

`PASS`, `FAIL`, `INSUFFICIENT_STATISTICAL_POWER`, `INSUFFICIENT_DATA`, `INVALID_EVIDENCE`,
`BLOCKED_PENDING_APPROVAL`. A sample that cannot reach the required power at the target effect is never a pass,
however good its point estimate.

### Unresolved governance requirements

The frozen specification gives no significance level, required power or target effect, and the master plan is
not available. These three values, with a named approver, are **not set**. Until they are, the gate computes and
reports everything and answers `BLOCKED_PENDING_APPROVAL`.

The power arithmetic the owner needs for that decision, at the conventional 5% familywise level and 80% power
with 18 hypotheses (illustrative, not approved):

| Sample | Smallest annual Sharpe detectable | Power to detect a true Sharpe of 0.5 |
|---|---|---|
| 1.75 years (the held-back period) | about 2.7 | about 2% |
| 6.75 years (development plus held-back) | about 1.4 | about 7% |

A daily trend strategy with a realistic Sharpe ratio cannot be shown to work at that standard on the history
that exists. The owner's options are: accept a lower standard and say so; treat the frozen pass rule as the
binding economic test and require forward (shadow) evidence to supply the statistical power over time; or
require both. This is a decision, not an engineering choice.

## 4. Admission rules

Promotion from M5 to M6 requires seven replay gates, including calibration of a per-trade forecast. A rule-based
family has none, so it cannot be admitted on that route. The rule-based route (`admission.py`) is defined and
tested but **disabled**: without a recorded owner decision `RULE_BASED_PORTFOLIO_ADMISSION_V1 = APPROVED` in the
register it answers `BLOCKED_PENDING_APPROVAL`.

When approved it still requires: a verified mandate hash, a frozen dataset, acceptable data quality, verified
causality, net-of-cost results, applied risk limits, a holdout evaluated under a recorded authorization, the
frozen pass rule passed and the statistical gate passed. Its best answer is "eligible for governance review".
It does not change a phase, enable an entry or touch a flag. `governance/phases.py` is unchanged.

## 5. Holdout policy

1. The window is reserved in the register before any run.
2. A pre-holdout readiness check must be READY: twelve facts, each proved by the caller (mandate frozen,
   hypothesis registered, dataset frozen, costs registered, statistical policy recorded, simulator tests green,
   causality audit green, development run valid, no known critical defect, report ready, reproduction
   documented, evaluation source committed).
3. The project owner authorizes, by name, with a reference to where the approval was given. The authorization
   is bound to the specification hash, the dataset hash and a fingerprint of the evaluation source; a change to
   any of them voids it. No code path, test or agent records this on its own.
4. Opening writes the access record to disk before any held-back row is read. Of several workers, one wins.
5. The result hash is stored once. The same run asked again receives the stored result. An opening without a
   stored result stays on record as incomplete; completing it needs a separate recorded decision and never
   removes the first attempt.
6. The data loader returns held-back rows only against the access token produced in step 4.

Three points the owner must weigh before authorizing the Mandate 004 holdout:

- The frozen specification leaves 21 points open. They were fixed in writing before any price series was
  loaded (`research/trend_v1/INTERPRETATION_001.md`, `INTERPRETATION_002.md`) and registered as amendments,
  by the engineering agent. They are missing mandate parameters that still need the owner's approval; the
  authorization command records that approval as an acknowledgement.
- Its held-back period (2025-01-01 to 2026-09-30) overlaps the period 2024-09-25 to 2026-07-12 that the
  seventeen earlier hypotheses used for development. The daily trend rules were frozen without being run, but
  the period is not unseen by the research programme.
- It contains the window 2026-07-12 to 2026-09-23 that is still reserved, unopened, for the Section 22
  certification of the forecast-driven system. Reading daily prices of that window for this mandate reduces its
  value as untouched evidence for any later hypothesis.

## 6. Promotion boundaries

A research pass is a statement about evidence. It does not enable trading, deploy a bot, turn on live orders,
select accounts, change exchange credentials or activate the production driver. The current governance phase is
M0 and nothing in Step 2 moves it. Step 3 owns activation.

One existing inconsistency is recorded rather than changed: the residual momentum family failed the Mandate 003
economic gate (hypothesis 15) and was afterwards made the frozen prospective and demo strategy without
certification. It remains as it is; CATI-01 in the Step 1 register already lists certification as its blocker.
