# Step 3 handoff from Step 2 (research certification)

Deliverable D13. State on 9 October 2026, commit recorded in the Step 2 tracker.

## The one thing Step 3 must not assume

**No strategy family is certified.** Seventeen hypotheses failed; hypothesis 18 (daily trend, Mandate 004)
has a valid development run and no verdict. Step 3 may build the multi-position pipeline and run it in
shadow. It may not let any family place an order, on demo or live, until a family is certified and the
governance process promotes it. The current phase is M0 and Step 2 did not move it.

## Family status

| Family | Hypothesis | Status | Eligible for demo | Eligible for live |
|---|---|---|---|---|
| Daily trend (`DAILY_TREND`, Mandate 004) | 18 | registered; development run valid; held-back period not opened | no | no |
| Residual momentum (`RESIDUAL_MOMENTUM_PORTFOLIO_TOP1`, Mandate 003) | 15 | failed its economic gate; kept as the frozen prospective strategy without certification | no (see below) | no |
| Dispersion break, funding carry (Mandate 003) | 16, 17 | failed | no | no |
| Alpha V1 (6), Alpha V2 (4), forecast models (4) | 1 to 14 | failed or rejected before holdout | no | no |

Query it in code rather than reading this table:

```python
from app.trading_intelligence.research.governance.register import ResearchRegister
reg = ResearchRegister()
reg.hypothesis("H-018")["status"]            # REGISTERED until a final verdict is recorded
reg.holdout(holdout_id)["status"]            # HOLDOUT_RESERVED
```

`docs/research/mandate_004/certification_result.json` (`certification_status`) carries the same answer for
tooling: `certified: false`, `eligible_for_demo: false`, `eligible_for_live: false`, `cati_mode: SHADOW_ONLY`.

## Components that are ready to reuse

| Need in Step 3 | Component | Notes |
|---|---|---|
| Strategy-family interface | `app/trading_intelligence/families/daily_trend/` | pure functions over day × symbol matrices; no account, no I/O |
| Immutable mandate reference | `families/daily_trend/spec.py` and `governance.mandates.verify_mandate` | call it at start-up; it refuses when the specification, the rule set or an amendment changed |
| Signal rules | `rules.py`: `strength`, `stop_distance`, `universe`, `consecutive_bars` | the evaluator imports the same functions; do not re-implement them |
| Daily targets and orders | `targets.py`: `decide_targets`, `orders_from_targets`, `apply_exchange_filters` | one call per decision day; `risk_level(name, EXECUTABLE)` applies the engine's limits |
| Causal daily data | `app/market_data/daily_dataset.py` (`DailyPanel`), `binance_archive.py` | a bar with zero trades is not a bar; missing days are never filled |
| Cost accounting | `research/evaluator/simulator.py` ledgers; `research/costs/measured.py` | the frozen costs are assumptions; measured costs are not yet calibrated |
| Portfolio constraints | `targets.decide_targets` (position limit, open-risk cap, leverage cap, brakes) | the shared risk table `shared_lib.risk_levels.PROFILES` holds the same numbers (tested) |
| Research output contract | `result.json` schema `cati-mandate-evaluation-v1`; `certification_result.json` schema `cati-certification-result-v1` | versioned; every headline number carries its run id and dataset hash |
| Certification status query | the research register and `certification_result.json` | above |
| Promotion eligibility | `governance.admission.evaluate_rule_based_admission` | answers `NOT_ELIGIBLE` or `BLOCKED_PENDING_APPROVAL` today; it never changes a phase |

## Daily targets for shadow comparison

`docs/research/mandate_004/runs/run_0b88d389f8da50dc48de5e51/development_primary_daily_targets.csv` holds, for
every decision day of the development period, the target of every selected contract (Balanced, mandate
policy): fraction of equity, notional, strength, stop distance and the equity it was computed from. The Step 3
pipeline, fed the same daily bars, must reproduce these rows by calling `decide_targets`. The other files in
that folder give the fills, the rejections and the daily ledger of the same run.

After Step 3 exists, the forward comparison is: each day, the pipeline's targets against
`decide_targets` evaluated on that day's bars. That needs no held-back data.

## Constraints Step 3 inherits

1. **The mandate does not fit the engine as it is.** In the development run 1,072 of 1,251 new positions
   (86%) had a stop wider than the engine's 15% maximum (median stop distance 25%). Under today's limits the
   strategy makes 202 trades instead of 1,192 and is a different, much smaller strategy. Either the engine's
   stop limit is revisited for this family by an explicit decision, or a different specification (a new
   hypothesis) is needed. Neither was done in Step 2.
2. **RISK-01 is still open.** The engine caps risk per trade at 0.40%; Balanced (0.50%) and Aggressive (0.75%)
   are therefore identical per trade today. Nothing in Step 2 changed the ceiling.
3. **Position sizes are small.** With wide stops the mandate holds about 6% of equity in the market on
   average (19% at most). A typical position is about 2% of equity. On a 10,000 USDT account 171 orders were
   rejected for the exchange minimum; an account of a few hundred USDT, like the connected demo account,
   cannot place most of these orders at all.
4. **The execution path is wired to one family by name.** `trading_orchestrator.py` and
   `production_execution.py` special-case `RESIDUAL_MOMENTUM_PORTFOLIO_TOP1`. That family failed its gate
   (hypothesis 15). Step 3 has to replace this with a family-neutral path gated by certification status.
5. **`setups.registry.SPECIALIST_REGISTRY` must keep exactly four specialists** (asserted by the success
   standard). The daily trend family is deliberately not registered there.
6. **The family the engine runs today holds one position at a time**; this family holds up to 4, 6 or 8, which
   is the multi-position engine Step 3 is to build.
7. **Selection has no holding priority** (interpretation I7): the six strongest contracts are held each day,
   so the average holding is 8 days and turnover is 5.7 times equity a year. Step 3 must implement this as
   registered, not "improve" it; a different selection rule is a new hypothesis.

## Decisions that block certification (project owner)

1. Authorize, or decline, the opening of the Mandate 004 held-back period.
2. Approve a statistical standard for the portfolio-level gate (significance level, required power, target
   effect), or decide that forward shadow evidence supplies the power instead.
3. Decide the rule-based admission route (Section L): without it a rule-based family cannot reach M6 even
   with a research pass.
4. Confirm the enumeration of the seventeen earlier hypotheses.
5. RISK-01 (0.40% ceiling) and the 15% stop limit for this family.

## Known unresolved issues

- The master plan (and its Sections H, I and L) is not in the repository.
- Measured costs are not calibrated (one short observation window).
- The dataset is not point-in-time for listings, filters or contract categories.
- Remote CI has not run on the Step 2 commits until they are pushed.
