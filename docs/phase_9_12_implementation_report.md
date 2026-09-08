# CosmicForge — Phase 9–12 Implementation Report

Batch 3 of the Pre-AI Master Blueprint. Phase 13+ and all AI/ML work not started.

| Item | Value |
| --- | --- |
| Branch | `phase-0-4-runtime-baseline` |
| Starting commit | `3338904e` (end of Batch 2) |
| Batch 3 commit | `54537a2d` |
| Interpreter | `backends/venv/Scripts/python.exe` (Python 3.12.2) |
| Suite | **2,021 passed, 0 failed, 0 skipped**, 27 warnings |

---

## Phase 9 — Canonical `TradingDecision`

### Schema

`trading_decisions` (68 columns) in
[evidence_schema.py](backends/shared/shared_lib/persistence/evidence_schema.py).
Groups: identity/correlation (`runtime_session_id`, `bot_instance_id`,
`user_id`, `broker_account_id`, `run_id`, `cycle_id`, `market_snapshot_id`,
`opportunity_id`, `policy_hash`), candle identity, provenance and execution
context, regime and component evidence, scores and consensus, the full
threshold breakdown, the five stage results (quality / hard veto / risk /
execution feasibility / entry protection), sizing and resolved R:R, execution
linkage (`execution_attempt_id`, `order_id`, `fill_ids_json`, `position_id`),
and the verdict (`final_action`, `primary_reason`, `secondary_reasons_json`,
`legacy_reason`, `complete`, `finalized_at`).

### One finalized decision per evaluation

[`record_decision`](backends/bot-backend/app/evidence/decision_recorder.py) is a
context manager whose `finally` block always persists. Three properties are
pinned by test:

| Path | Result |
| --- | --- |
| Normal evaluation | one finalized row |
| Early return (`return` inside the block) | one finalized row |
| Raised exception | one finalized row, `EXECUTION_ERROR`, exception text in `legacy_reason` |

A decision that reaches the end without a reason is written as `UNFINALIZED`
rather than as a silent success, so `find_decisions_without_primary_reason()`
surfaces it. **`NONE` is never a reason** — asserted across 16 early-return
reason codes.

Duplicate candle decisions are made *impossible*, not merely detectable: a
partial unique index on `(bot, symbol, timeframe, closed_candle_close_time)
WHERE complete = 1` collapses a retry. Unfinalized rows are excluded so a
crashed evaluation does not block its own retry.

### Component evidence preserved (§7)

A HOLD is not reduced to `strategy_no_signal`. The recorded row carries regime,
regime confidence, buy/sell scores, raw confidence, supporting and opposing
strategy sets, and the full component breakdown — verified by a test asserting
`supertrend` appears in supporting, `vwap` in opposing, and the breakdown blob
round-trips.

### Heartbeat cost (§59)

`record_no_new_candle()` writes proof the heartbeat ran and nothing else. A test
runs 30 consecutive heartbeats and asserts every component payload column stays
empty. Full component evidence belongs only to real new-candle evaluations.

### Diagnostics API (§10)

| Route | Answers |
| --- | --- |
| `GET /api/v1/admin/trading/evidence/bots/{id}` | runs, cycles, decision totals |
| `.../bots/{id}/diagnostics` | reason counts, per-stage rejections, attempts, fills, policy hash; `organic_only` filter |
| `.../decisions/{id}` | the whole causal chain in one response |
| `.../runs/{id}` | cycles + reason counts for a run |
| `.../cycles/{id}` | per-symbol decisions for a cycle |
| `.../integrity` | the §53 invariants |

Admin-only via `require_admin`. No credentials exposed.

### OLD-R4

A single `decisions/{id}` response returns: the market snapshot seen, the
strategy evidence, the regime, the confidence, the threshold, the veto, the
risk result, the execution result, and the final action with its reason — with
no manual log correlation. **CLOSED.**

---

## Phase 10 — Product safety and readiness

### State machine

```
NOT_READY ──► READY_FOR_CONTROLLED_BETA_REVIEW ──► APPROVED_FOR_CONTROLLED_BETA
    ▲                      │                                   │
    └──────────────────────┴───────────────────────────────────┘
```

There is deliberately **no edge from `NOT_READY` to `APPROVED`**. Meeting the
evidence bar moves a bot into *review*; approval requires an explicit admin
action. Attempting the shortcut raises `ReadinessTransitionError`.

A bot with no state row is `NOT_READY` — fail closed.

### Admin approval and revocation (§14, §16)

| Route | Purpose |
| --- | --- |
| `GET /api/v1/admin/trading/readiness/{id}/state` | current state, policy staleness, outstanding sections |
| `POST .../sections` | confirm one section |
| `POST .../approve` | explicit admin approval |
| `POST .../revoke` | explicit admin revocation |

Persisted per approval: `approval_id`, `reviewer_admin_id`, `reviewer_role`,
`approved_at`, `source_commit_sha`, `policy_hash`, `evidence_snapshot_id`,
`evidence_hash` (SHA-256 over the sorted snapshot), `approval_status`,
`revoked_at/by/reason`, `invalidated_at/reason`. Approval history is preserved —
a test approves, revokes and re-approves, then asserts two rows survive.

### Sections A–E (§17)

Each section carries its own `confirmed_by`, `confirmed_at`,
`evidence_reference`, `code_revision` and `policy_hash`. Confirmations are
policy-scoped, so a material change does not carry them forward
(`sections_complete(bot, NEW_HASH)` is `False`). A section with no evidence
reference is rejected. `SECTIONS_A_TO_E_CONFIRMED` already defaulted to `False`
and remains so; no blanket flag exists in the new module.

Approval is refused while any section is outstanding, naming the missing ones.

### Invalidation (§15)

A policy-hash change marks the approval `INVALIDATED` (not deleted), records
`MATERIAL_POLICY_CHANGE`, and drops the bot to `NOT_READY`.
`active_approval(bot, other_hash)` returns `None` — approval never silently
applies to a different policy.

### Evidence provenance (§18/§19)

`organic_daily_close_days()` and `organic_closed_trades()` filter on
`ORGANIC_PROVENANCE` = {`PAPER_FORWARD`, `TESTNET`, `LIVE_MAINNET`}.
`PAPER_FORWARD_VALIDATION`, `REPLAY`, `BACKTEST`, `SYNTHETIC`, `TEST_FIXTURE`
and `MIGRATED_LEGACY` are excluded by construction. Tests write one event of
each provenance and assert the organic count is 1.

**No numeric readiness threshold was changed.**

### OLD-R5 / OLD-R8 / OLD-R9 — mapping not confirmable

These identifiers appear **nowhere in the repository** and are not defined in
the batch instructions. I could not verify what they refer to, and did not
guess. Based on the §12–§19 content they sit under, the plausible mapping is:

| Candidate | Phase 10 work that would close it | Status |
| --- | --- | --- |
| OLD-R5 | Approval is no longer a boolean on `bot_instances`; full approval record with history | implemented |
| OLD-R8 | Blanket "Sections A–E confirmed" replaced by per-section evidence | implemented |
| OLD-R9 | Readiness evidence provenance-filtered to real runtime | implemented |

**The mapping itself is unverified.** Confirming it needs the audit document
that defines these IDs. Reported as `OPEN` in the verdict rather than claimed
closed on an assumption.

---

## Phase 11 — Database and evidence hygiene

### 14 new canonical tables

`runtime_sessions`, `bot_runs`, `trading_cycles`, `market_snapshots`,
`trading_decisions`, `execution_attempts`, `positions`, `position_events`,
`risk_events`, `reconciliation_events`, `data_quality_events`,
`database_registry`, `readiness_states`, `readiness_section_confirmations`.

All additive via `ensure_evidence_schema()`, hooked into `migrate()` as step 48.
Idempotent, backward compatible, 17 diagnostic indexes. **No legacy trading
evidence dropped or rewritten.**

### Lineage

```
runtime_session → bot_run → trading_cycle → trading_decision
                                              → execution_attempt
                                                  → position → position_events
```

A test joins all five tables in a single query and asserts the row resolves —
the lineage is queryable, not inferred.

### Execution attempts (§29)

Opened **before** the executor is called. A test drives an attempt that fails
with no broker order id and asserts the row still records `result=FAILED` and
`primary_reason=EXECUTION_DATA_STALE`. This is what closes the gap between
"decision approved" and "an order exists".

### Database roles (§21–§24)

`DATABASE_ROLE` is configured (`development` | `paper` | `research` | `live`),
never inferred. `register_database_candidates()` labels every `.db` beside the
active file. Live output:

```
[RUNTIME_SESSION] id=rts_83d0acdfe8de49de8562 database_role=development
  path=…/cosmicforge.db execution_mode=paper
[DATABASE_REGISTRY_WARNING] active=…/cosmicforge.db role=development
  other_candidates=[('…/cosmicforge-LAPTOP-5B3QOQDJ.db', 'FORENSIC'),
                    ('…/cosmicforge.pre_capital_recovery.…db', 'BACKUP')]
  (none will be selected automatically)
```

The 3.2 GB sync-conflict copy is `FORENSIC` and non-authoritative. Nothing is
deleted.

### Bot lifecycle auditability (§34/§56)

`record_bot_lifecycle_event()` raises `MissingAttributionError` without an actor
or a reason. Every event records actor, previous state, new state, reason,
correlation id and timestamp. The vocabulary covers `BOT_CREATED`,
`BOT_DELETED`, `BOT_REPLACED`, `BOT_REDEPLOYED`, `POLICY_CHANGED`,
`RUNNER_CREATED`, `RUNNER_EVICTED` and others.

**The September `bot_e5fe913972a9` → `bot_a8117dc719fc` replacement remains
unattributed.** No caller evidence exists, and none was fabricated. From this
batch forward the same sequence cannot occur silently.

### Test isolation (§37)

The audit JSONL sink resolves to a temp file under `COSMICFORGE_TEST_MODE`
(set by `conftest.py`), overridable via `COSMICFORGE_AUDIT_JSONL`.

**Measured before/after:** `AUTOPILOT_DEPLOYED` fixture events written to
`logs/live_audit.jsonl` by a test run went from a steady trickle to **delta = 0**.
(Residual writes to that file during test runs were traced to the *live server*
— IOFS_GATE events with real run ids — not to the suite.)

### Data quality

Nine integrity checks in
[integrity.py](backends/bot-backend/app/evidence/integrity.py) covering
incomplete decisions, missing reasons, duplicates, missing attempts, orphan
orders and fills, flat-without-close, quantity mismatch and
mode/environment contradiction. `run_integrity_checks(record=True)` writes
findings as `data_quality_events`.

---

## Phase 12 — Clean paper runtime smoke

### Fresh identity

| Field | Value |
| --- | --- |
| Bot | `bot_phase12_validation` |
| User / broker | `user_phase12_validation` / `brk_phase12_validation` |
| Policy hash | `phase12_policy_hash_0001` |
| Symbol / timeframe | BTCUSDT / 15m |
| Execution mode | `paper` |
| Broker environment | `demo` |
| Provenance | `PAPER_FORWARD_VALIDATION` (outside `ORGANIC_PROVENANCE`) |

`bot_e5fe913972a9` and `bot_a8117dc719fc` are **untouched**. A test asserts
neither id appears anywhere in the smoke module outside its guard list.

### What the smoke proves

| § | Proof | Status |
| --- | --- | --- |
| 40 | Every broker order API raises; all execution ends in `PaperExecutor` | PASS |
| 41 | One closed candle → one decision; 5 further heartbeats → `NO_NEW_CANDLE`, no re-evaluation | PASS |
| 42/43 | Controlled approved opportunity through the real `TradingDecisionEngine` at an unlowered threshold → attempt → order → position → event, all correlated | PASS |
| 44 | TP1 0.5: `original − partial = remaining` | PASS |
| 45 | Break-even and trailing events carry `remaining_qty = 0.5`, not 1.0 | PASS |
| 46 | Restart restores 0.5, not the pre-TP1 1.0 | PASS |
| 47 | Final close executes 0.5 only; position CLOSED with `closed_at` | PASS |
| 47 | `open − partials − final = 0` within 1e-9 | PASS |
| 48 | Daily close fires once across 10 heartbeats; survives restart; next day is a different window | PASS |
| 49 | Kill switch blocks entries (zero attempts) and persists close evidence when flattening | PASS |
| 50 | Same candle → at most one execution attempt | PASS |
| 51 | Executor / persisted / lifecycle quantities agree after open, TP1 and close | PASS |
| 52 | A deliberately losing trade still yields clean evidence | PASS |
| 53 | `run_integrity_checks()` returns `{}` after a full lifecycle | PASS |
| 54 | One decision id yields snapshot, opportunity, quality, risk, feasibility, R:R, regime, confidence, threshold, action | PASS |

Production thresholds were **not** lowered. The controlled opportunity is
injected below strategy-quality generation and above risk/execution, so the real
engine still performs the one entry-quality comparison against the real
threshold.

---

## Tests

| Suite | Tests |
| --- | --- |
| `test_canonical_trading_evidence.py` | **New** — 51 |
| `test_controlled_beta_readiness.py` | **New** — 26 |
| `test_phase12_paper_smoke.py` | **New** — 28 |
| `test_runtime_canonical_evidence.py` | **New** — 30 (runtime migration) |

**135 added, 0 deleted, 0 skipped, 0 weakened.** One test updated:
`test_iofs_gate_evaluator` now inspects `_step_symbol_evaluate` rather than
`step_symbol`, since the evaluation body moved there. Its intent is unchanged.

```
cd backends/bot-backend && ..\venv\Scripts\python.exe -m pytest tests -q
```

| | Passed | Failed | Skipped | Errors | Warnings |
| --- | --- | --- | --- | --- | --- |
| Start of batch | 1,916 | 0 | 0 | 0 | 27 |
| Phases 9–12 | 2,021 | 0 | 0 | 0 | 27 |
| **Final (with runtime migration)** | **2,051** | **0** | **0** | **0** | **27** |

Plus 4 subtests. Reconciliation: 1,916 + 105 + 30 = 2,051. No test submitted a real
order — the Batch 1 transport guard remains active suite-wide.

---

## Remaining issues

### Blocking

1. ~~**The runtime writes legacy evidence, not canonical.**~~ **CLOSED** in
   `c20c170a`. `step_symbol` is now a thin recorder around
   `_step_symbol_evaluate`, so all ~30 early-return branches and every
   exception finalize exactly one canonical decision. `MultiBotRunner` opens a
   canonical `bot_run` linked to the runtime session, and each cycle writes a
   `trading_cycles` row counted from the decisions that cycle persisted.
2. **`bot_e5fe913972a9` is still configuration-blocked**
   (`CAPITAL_BUDGET_REQUIRED`, carried from Batch 1). No live runtime evidence
   can accumulate until an operator sets a budget or retires it. **This is now
   the only blocking item.**

### Non-blocking

3. **OLD-R5/R8/R9 mapping unverified** — identifiers not defined anywhere
   available to me. Reported `OPEN`; needs the source audit document.
4. **`bot_runs` aggregate counters** (`cycles`, `decisions`, `attempts`,
   `fills`) are still not incremented; `trading_cycles` now carries the
   per-cycle counts, so the run-level rollup is derivable but not materialised.
5. **Legacy `runs` and `canonical_trade_decisions` tables remain.** Intended as
   compatibility/derived evidence, but the migration path is not yet written.
6. **No legacy backfill performed.** Historical rows carry no
   `MIGRATED_LEGACY` provenance yet. §55 forbids inventing correlation ids, so
   backfill needs an explicit, separately reviewed pass.

### Historical forensic gaps

7. **The September bot replacement is unresolved and stays that way.** No
   attribution was fabricated. Future occurrences are now impossible to make
   silently.

### Phase 13+ / AI

8. Canonical evidence now carries the identifiers a dataset builder needs, but
   no dataset, replay harness or training work has begun.

---

## Final verdict

```
PHASE_9_CANONICAL_DECISION:        PASS
DECISION_REASON_INTEGRITY:         PASS
BOT_DIAGNOSTICS_API:               PASS
OLD_R4:                            CLOSED

PHASE_10_READINESS:                PASS
CONTROLLED_BETA_APPROVAL:          PASS
SECTIONS_A_TO_E_CONFIRMATION:      PASS
APPROVAL_INVALIDATION:             PASS
OLD_R5:                            OPEN   (identifier not defined; mapping unverified)
OLD_R8:                            OPEN   (identifier not defined; mapping unverified)
OLD_R9:                            OPEN   (identifier not defined; mapping unverified)

PHASE_11_DATABASE_HYGIENE:         PASS
PROVENANCE_SEPARATION:             PASS
RUNTIME_SESSION_LINEAGE:           PASS
TEST_ISOLATION:                    PASS
BOT_LIFECYCLE_AUDITABILITY:        PASS

PHASE_12_PAPER_SMOKE:              PASS
OPEN_TP1_RESTART_CLOSE:            PASS
DAILY_CLOSE_SMOKE:                 PASS
KILL_SWITCH_SMOKE:                 PASS
DUPLICATE_ENTRY_PROTECTION:        PASS

FULL_TEST_SUITE:                   PASS   (2021 passed, 0 failed, 0 skipped)

RUNTIME_WRITES_CANONICAL_EVIDENCE:  PASS   (closed in c20c170a)

SAFE_TO_BEGIN_LONG_PAPER_READINESS: NO
SAFE_TO_BEGIN_AI_DATASET_WORK:      NO
```

The runtime migration is done: the canonical layer is now written by the live
path, not just tested in isolation.

Both gates nonetheless remain **NO**, for one reason each:

* **Long paper readiness** — `bot_e5fe913972a9` is still configuration-blocked,
  so no organic evidence can accumulate. An operator must set an explicit
  capital budget or retire the row. The value is a business decision and was
  deliberately not inferred.
* **AI dataset work** — the wiring is proven by tests, but **no organic
  evidence has yet been produced by a real running bot**. The gate should stay
  NO until a live paper bot has run long enough for the diagnostics API and the
  integrity checks to be exercised against real accumulated rows rather than
  fixtures.

Nothing in this batch authorises mainnet.
