# The CATI platform pipeline

Updated integration status (16 October 2026): [A�N validation](STEP3_STEP4_VALIDATION.md) and [requirement register](STEP3_STEP4_IMPLEMENTATION_STATUS.md). The baseline audit sections below describe the earlier implementation; runtime handoff, shared budgets, account-close brakes and the concrete certification harness are now wired. Actual research, venue and elapsed-time gates remain separate.

Section H, Step 3 (requirements 3.1, 3.2, 3.3, 3.5, 3.6, 3.11 and 3.15). Version 1, 9 October 2026.

Code: `backends/bot-backend/app/trading_intelligence/pipeline/`. Schema:
`backends/shared/shared_lib/persistence/cati_platform_schema.py`. Administrative interface:
`backends/bot-backend/app/api/family_governance.py`. Tests: `backends/bot-backend/tests/trading_intelligence/test_step3_*.py`
and `backends/bot-backend/tests/test_step3_governance_api.py`.

Nothing in this package is part of the evaluation source that the Mandate 004 research evidence is bound to, and
nothing in `research/` or `families/` imports it.

## 1. What may send an order, and why nothing does today

An order for a strategy can only leave through `CATIExecutionBoundary`. Before anything else, and again
immediately before the submit, the boundary asks `pipeline.provenance.FamilyExecutionGate`. The gate answers yes
only when all of the following hold at that moment:

| Part | Condition | Read from |
|---|---|---|
| Family | defined in code and registered; the code's rule hash equals the registered one | `family_registry.definitions()`, governance log |
| | not disabled, family kill switch off | governance log |
| | its hypothesis PASSED and the certification was not revoked | research register file, read every time |
| | stage M6 or higher for a demo order (M7 and the global scopes for live) | governance log |
| | the plan's mode is an executing mode and equals its environment | the plan |
| Decision | an immutable decision record exists and still hashes to its stored hash and to the hash the plan carries | `cati_family_decisions` |
| | made for this account, user and environment, by this family, under this rule hash | same |
| | not stale, not dated in the future; the plan asks for what the decision said | same |
| Venue | the exchange adapter holds a complete, unexpired certification for this environment | adapter certification register (Step 4.2) |

There is no setting, environment variable or argument that skips the gate, and production settings accept only
the real gate class. A failure to decide is a refusal.

**Actual state on 9 October 2026.** Every family is at M0. No family is certified: `DAILY_TREND` is
`NOT_CERTIFIED` (hypothesis 18 has a valid development run and an unopened holdout),
`RESIDUAL_MOMENTUM_PORTFOLIO_TOP1` is `FAILED` (hypothesis 15), the four setup specialists have no verdict. No
exchange adapter is certified. `FamilyRegistry.executable()` is empty.

**The demo bypass that existed.** Before this change a connected demo account at phase M0 could receive orders
from the residual family, which had failed its research gate: the account authority authorized demo entries
without looking at the phase, and the boundary's only source check was the family's name. That path now ends at
the gate with `FAMILY_FAILED_RESEARCH` (`test_step3_no_uncertified_orders.py`).

**The operator harness is not a family.** The Step 1 infrastructure harness (`DEMO_CERTIFICATION`) keeps its own
narrow permit and is never authorized by the gate. An order it places is evidence about the adapter and says
nothing about any strategy.

**Step 1 tests.** Nine Step 1 test modules prove the execution tail by driving it with the residual family
under production settings. In those modules only, the family part and the venue part of the gate are replaced
inside the test process (`LEGACY_EXECUTION_TAIL_MODULES` in `backends/bot-backend/conftest.py`); the decision
part runs for real. The product has no such switch.

## 2. The family registry (3.2)

`FamilyRegistry.entry(family_id)` joins three independent sources into the entry the assignment lists:

- the **definition** in code (`FamilyDefinition`): identity, version, mandate, hypothesis, the hash of the rules
  the code executes, markets, timeframe, direction, position model, entry and exit contracts, data requirements,
  activation policy, evidence reference;
- the **research register**: certification status, derived each time (`RESEARCH_PASS` → `CERTIFIED`; `FAILED` and
  `REJECTED_PRE_HOLDOUT` → `FAILED`; `REGISTERED` → `NOT_CERTIFIED`; anything else, an unreadable file, a broken
  hash chain or a missing anchor → `UNKNOWN`, which authorizes nothing);
- the **governance log**: stage, kill switch, disabled, certification revoked, the registered definition hash.

`allowed_modes` is derived, never stored: `RESEARCH_SIMULATION` always; `SHADOW` when the family is registered,
enabled, unchanged since registration and has a pipeline adapter; `DEMO` when it is also certified, not killed
and at M6; `LIVE` at M7. A failed family stays visible with its failure reasons and without any executing mode.

Families are selected by registry content (`evaluable()`, `executable()`), never by name, and their number is not
fixed anywhere. Registered today: `DAILY_TREND`, `RESIDUAL_MOMENTUM_PORTFOLIO_TOP1`, and the specialists of
`setups.registry.SPECIALIST_REGISTRY` (exactly four; that separate invariant is untouched).

What still names the residual family: its own production loop from Step 1 (the hourly decision collector, the
broker synchronisation and the maintenance of positions it opened). That code is the implementation of one
family, kept as it is; it no longer decides whether that family may trade.

## 3. Governance, M0 to M6 (3.11)

One append-only, hash-chained log (`cati_family_events`): registration, evidence, promotion requests, approvals
and rejections with the reviewer, revocations, the family kill switch, disabling. The state of a family is the
fold of its events. The stages are the phases of `governance/phases.py`, unchanged, and each stage asks for that
phase's evidence keys. Code adds what recorded evidence may not replace:

- M5 and above: the family's mandate verifies against the research register (specification, rule set and
  amendments by pinned hash) and a completed, valid evaluation run exists;
- M6: the research register says the hypothesis passed, and for a rule-based family the rule-based admission
  route answers eligible.

A request changes nothing. Approval, rejection, revocation, the kill switch and disabling need an
`OperatorAuthorization`, which only the administrative interface builds, from the operator token of the
emergency router. The prerequisites of an approval are verified against the research register at the moment of
approval; the request body cannot carry one. The command line (`python -m app.trading_intelligence.pipeline`)
can show status, register definitions and run shadow cycles; it cannot decide anything. Nothing here promotes
above M6.

Five things that are kept apart: research simulation (the evaluator), shadow operation (this pipeline),
operator-scoped infrastructure certification (the harness and the Step 4.2 runner), certified family demo trading
(M6, not reached by any family), live authorization (M7 and beyond, outside this build).

Kill switches: platform (`/api/v1/admin/emergency/kill-switch`, existing), family and account
(`/api/v1/admin/governance/...`). A family kill switch stops new entries and increases of that family on every
account from the next decision; its positions follow the family's registered policy (`MANAGE_TO_EXIT` for every
family registered today: stops and exits continue, nothing is added).

## 4. Family-neutral contracts (3.5)

`pipeline/contracts.py`. One tagged type per action, validated on construction: `EntryIntent` (a stop and a risk
fraction are mandatory; a profit target is optional), `AddIntent`, `ReduceIntent` (quantity or fraction),
`ExitIntent`, `RebalanceIntent`, `StopReplaceIntent` (trail, tighten, break-even, re-size; a widening move is
refused). `IntentBatch` binds several intents of one account to one decision and its portfolio constraints and
refuses contradictions, unsupported instruments, leverage above the account's, risk above the engine ceiling, an
entry stop wider than the engine allows (refused, never clipped), inconsistent timeframes and stale or
future-dated decisions. The existing `TradePlan` carries the same provenance in its `versions`
(`provenance_of`).

## 5. The cycle driver (3.1)

`pipeline/driver.py`. It does not use the legacy runner. A cycle is `(family, cycle key, mode)`; its id is a
function of the three.

```
STARTED → MARKET_DECIDED → COMPLETED
        → WAITING_FOR_DATA   the decision bars are not there: nothing is guessed, the cycle is retried
        → PARTIAL            an account failed: the others are done, the failed one is retried
        → FAILED             the family cannot be evaluated (for example its mandate no longer verifies)
```

- The market stage runs once per cycle and serves every account. Each account is one transaction: its book, its
  fills, the day's figures, its immutable decision and its cycle row commit together or not at all.
- Replay cannot duplicate: a completed cycle is returned, not recomputed; decisions are keyed by (cycle,
  account); a different decision for the same key is a recorded conflict.
- Missed cycles are made up oldest first; a book never jumps a day.
- The driver holds no adapter, client or boundary and keeps no state in the process. A test fails if any module
  of the shadow path imports an execution module or grows a module-level container.
- Stages of the assignment's pipeline are recorded per cycle and per decision. For the daily trend family
  "market state" and "regime" are recorded as not applicable, because its frozen rule reads prices only.
- Accounts: model accounts (the platform's reference books, no user, no exchange) and connected accounts (one
  active bot, the capital the customer allocated as an amount; sized with the engine's limits). A connected
  account is still shadow: the mode recorded on every decision is `SHADOW`, with the registry's blockers.

## 6. Daily trend in the pipeline (3.6)

`pipeline/daily_trend.py` calls the frozen Step 2 code and adds no rule: features and universe from the
evaluator's `prepare` (which uses `rules.py`), selection and sizing from `targets.decide_targets`, orders from
`orders_from_targets`, exchange filters from `apply_exchange_filters`, constants from `spec.py`, and
`verify_mandate` before every cycle. Nothing prefers a held position over a stronger candidate.

The shadow book (`advance_day`) is an independent, incremental implementation of the evaluator's daily
accounting, kept in the database between days.

## 7. Reconciliation with the evaluator (3.15)

`pipeline/shadow_compare.py` compares, per account and day: universe, selected contracts, direction, strength,
stop distance, equity basis, position fractions, target notional, the scaling caps, the rejections, the brake
flags and the cash left over. Tolerance: relative 1e-9 is agreement; up to 1e-6 is a `PRECISION_MISMATCH`
(reported and alerted); above that the divergence is classified (`DATA_MISMATCH`, `RULE_MISMATCH`,
`TIMING_MISMATCH`, `RISK_POLICY_MISMATCH`, `EXPECTED_OPERATIONAL_DIFFERENCE`, `UNKNOWN_DEFECT`). Every comparison
is a durable row and every divergence a structured alert on the `cati.shadow_compare` logger.

Results are in the tracker (`STEP3_STEP4_IMPLEMENTATION_STATUS.md`) and in
`docs/implementation/evidence/step3/development_shadow_reconciliation.json`.

## 8. What this does not establish

- No family is certified and none was promoted. Shadow decisions are records of intent.
- One week and four weeks of continuous shadow operation have not been observed: there is no continuously
  running host. Synthetic cycles and a replay of history are not elapsed time.
- The comparison with the evaluator covers the development period only. No held-back row was read.
- All of this was run on a Windows workstation; Linux behaviour is what remote CI shows for the committed tests.
