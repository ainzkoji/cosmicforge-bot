# CosmicForge — Phase 16: forward paper validation plan

**Phase 16 cannot start yet.** This document says what it requires, what is
currently blocking it, and — using the Phase 15 measurements — roughly how long
it will take once unblocked, because the answer is not three weeks.

---

## Entry conditions

Phase 16 begins only when all of these hold.

| Prerequisite | State |
| --- | --- |
| Always-on runtime hardening | **PASS** |
| Phase 12 full lifecycle proof | **PASS** |
| Phase 13 production-parity replay | **PARTIAL** — §13.2 and §13.8 not built |
| Phase 14 research data contract | **NOT STARTED** |
| Phase 15 deterministic baseline | **PASS** (measured; §15.10 left open) |
| Active-bot capital configuration resolved | **BLOCKED** — see below |
| Paper database role explicit | **BLOCKED** — see below |
| Runtime restarted onto the Phase 12 fixes | **BLOCKED** — see below |

### Blocker 1 — capital over-commitment

`bot_a8117dc719fc` resolves to 2 slots × 120 = **240 worst-case exposure
against a 120 budget**, with no warning and no clamp
(`scripts/audit_bot_capital_config.py`). A readiness sample gathered under a
configuration that can deploy twice its stated capital is not a readiness
sample. Operator decision: reduce the allocation, reduce the slots, or raise
the budget.

### Blocker 2 — database role

`DATABASE_ROLE` is `development` on the canonical paper runtime, and that value
is stamped into every `runtime_sessions` row and the ownership lease. Phase 16
evidence would carry a role that contradicts what it is. `paper` is an accepted
role; the change is an environment edit plus a restart, with no database switch.

### Blocker 3 — the runtime is running pre-fix code

The live process started before the Phase 12 fixes landed. Until it is
restarted it still cannot execute a TP1 partial, still flattens paper positions
in the PositionManager on the cycle after they open, and still writes no
`execution_attempts`, `positions` or `position_events`. **Any evidence gathered
before that restart is not Phase 16 evidence.**

---

## §16.1 Fresh validation identity

A new `bot_instance_id`, a new run, a locked `EffectiveBotPolicy` and a locked
`policy_hash`, recorded at the start of the window. Not
`bot_a8117dc719fc`, and not `bot_e5fe913972a9`.

Provenance must be organic (`PAPER_FORWARD`), which means this bot must *not*
be driven by the Phase 12 harness or any controlled opportunity. The controlled
path exists to prove plumbing; it must never contribute to a readiness sample,
and `PAPER_FORWARD_VALIDATION` is outside `ORGANIC_PROVENANCE` precisely so it
cannot.

## §16.2 Minimum validation period

Both conditions, not either:

* **≥ 3 consecutive paper weeks**, and
* **≥ 60 correctly closed trades.**

### How long that actually takes

Phase 15 measured **0 approvals in 96 evaluations**. Two symbols on 15m produce
about 192 evaluations per day, so:

| assumed approval rate | evaluations for 60 trades | calendar time |
| --- | ---: | ---: |
| 2% | 3,000 | ~16 days |
| 1% | 6,000 | ~31 days |
| 0.5% | 12,000 | ~62 days |
| observed (0/96) | — | unbounded |

**The binding constraint is the opportunity rate, not the three-week minimum.**
At the rate actually observed, 60 closed trades will not arrive. This is the
single most important planning fact in this document: Phase 16 is gated on
Phase 15's §15.10 question being answered, not merely on waiting.

Do not shorten the window because results look good, and do not widen the
symbol set mid-window to manufacture trades — that changes the sample.

## §16.3 Performance requirements

Unchanged, and evaluated only over the locked window:

* positive expectancy after fees
* profit factor ≥ 1.3
* max drawdown ≤ 8%
* complete decision traces
* no unresolved sizing failures
* daily-close evidence
* scheduler evidence

Costs must be real, not assumed. Fees and slippage come from the paper
executor's recorded fills, not from a model.

## §16.4 Scheduler evidence

Expected vs actual scheduler coverage, candle-evaluation coverage, and no
unexplained gaps. `/api/v1/admin/trading/operations/coverage/{bot}` already
computes expected-vs-actual closed candles; every missing evaluation needs a
reason.

Test, replay and synthetic runs must not be counted. Provenance makes this
mechanical: `REPLAY`, `PAPER_FORWARD_VALIDATION`, `TEST_FIXTURE` and
`LEGACY_BACKFILL` are all outside `ORGANIC_PROVENANCE`.

Host suspension is a real and observed cause of gaps (a six-hour window with
zero evaluations). It is an operating-environment problem, not an application
one, and the runtime's job is to make it visible — which it now does.

## §16.5 Decision evidence

Every trade reconstructable end to end:

```
market_snapshot -> trading_opportunity -> trading_decision -> risk decision
-> execution feasibility -> execution_attempt -> fill -> position
-> position_event -> close -> realized result
```

This became achievable only with the Phase 12 evidence wiring. Before it,
`execution_attempts`, `positions` and `position_events` had zero rows in
production, so this requirement could not have been met at all.

## §16.6 Consecutive-loss counter

OLD-R1 closes only when real paper closes demonstrate correct win/loss
classification, a correct consecutive-loss count, and a correct reset after a
winner. It cannot be closed by fixture.

## §16.7 Daily close

Daily-close evidence on every eligible paper day, through the live `run_cycle`.
Phase 12 proved the mechanism; Phase 16 needs the ongoing record.

One open question from Phase 12: after a daily close the bot re-entered inside
the same window. The mark prevents a duplicate *close*, nothing prevents a new
*entry*. Decide before the window opens whether a daily close should also
suppress entries for its remainder — changing it mid-window would invalidate
the sample.

## §16.8 Policy locking

The policy hash is recorded at the start and checked continuously. A material
change invalidates the window and restarts it. `RUNNER_POLICY_CHANGE` already
invalidates readiness approval when the effective policy changes, so the
mechanism exists; Phase 16's discipline is not to trigger it.

## §16.9 Readiness evaluator

When and only when every requirement passes, state advances to
**`READY_FOR_CONTROLLED_BETA_REVIEW`** — a request for human review, not an
approval. `APPROVED_FOR_CONTROLLED_BETA` remains an operator action.

## §16.10 What Phase 16 does not authorise

Not AI strategy activation. Not online learning. Not self-modification. Not
live or user capital. Not mainnet. It establishes a trustworthy forward
baseline and nothing more.

---

## Recommended order

1. Restart the canonical runtime onto the Phase 12 fixes.
2. Resolve the capital configuration and the database role.
3. Apply the fill-provenance labelling (`scripts/classify_fill_provenance.py --apply`).
4. Answer §15.10 — the opportunity-generation question. Without this, step 6
   cannot reach 60 trades.
5. Build Phase 13 §13.2/§13.8 and Phase 14, so a strategy change can be
   evaluated on replay before it is committed to a forward window.
6. Only then open the Phase 16 window with a fresh identity and a locked policy.

Steps 4 and 5 are the substantial ones. Step 6 is mostly patience, and it is
wasted patience if step 4 is skipped.
