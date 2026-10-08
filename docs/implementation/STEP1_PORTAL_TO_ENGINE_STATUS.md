# Section H, Step 1 — Portal to Engine: implementation status

Authority: CosmicForge Master Plan A to Z (8 October 2026), Section H Step 1.0a–1.0e and 1.1–1.11.
Documented baseline: `main` at `9fb120f` (7 October 2026). This register is updated as each task
closes; no task is marked PASS on code presence alone.

## Baseline report (8 October 2026)

| Item | Finding |
|---|---|
| Local `main` at session start | `9bdbf829`, 14 commits behind `origin/main`; fast-forwarded to `9fb120fa` (no local-only commits, clean tree) |
| `origin/step1-portal-to-engine` | One commit beyond `9fb120f`: `f85a6cf0` "Freeze the trend ensemble v1 specification" (`research/trend_v1/SPEC.md`). It is Step 2 research material, not a Step 1 implementation. Left untouched. |
| `origin/audit-fixes`, `origin/cleanup-dead-code` | Already merged into `origin/main` (no commits beyond it). |
| Implementation branch | Work is committed directly on `main` per the project owner's standing instruction (no feature branches); each task is its own reviewable commit. |
| Quarantine list | `.github/known-test-failures.list`, 125 lines, applied by `.github/scripts/ci_quarantine.py` as non-strict xfail. |
| CI program | `.github/workflows/ci.yml`: ruff correctness rules; bot-backend, user-backend, admin-backend, shared and root pytest with `-p ci_quarantine`; user and admin frontend lint/test/build. `tests/integration` needs both backends running and is excluded. |

Test baseline at `9fb120f` (same commands as CI, run locally on Windows with `backends/venv`, Python 3.12.2):

| Suite | Result |
|---|---|
| user-backend | 409 passed, 0 failed |
| admin-backend | 88 passed, 6 xfailed |
| shared | 38 passed |
| root unit tests | 4 passed, 3 xfailed |
| bot-backend | see "Regression gate" below (run in a pristine export of `9fb120f`, compared by test id) |

Regression policy: the bot-backend suite is never fully green on this machine (three tests read the
operator's private `.env`; several `caplog` tests are order-dependent). A change is judged by diffing the
set of failing test ids against the baseline run, never by the absolute count.

## Task register

| ID | Task | Status | Commit |
|---|---|---|---|
| 1.0a | Protection read safety | PASS (unit/integration) | `b86c0760` |
| 1.0b | Request-weight optimization | PASS (measured) | `ff03e8d0` |
| 1.0c | Database churn reduction | PASS (measured) | `ff03e8d0` |
| 1.0d | Runtime and collector tests | PASS | `ab15a552` |
| 1.0e | Executor hazard removal | PASS | `0e53c27d`, `179de7bc` |
| 1.1 | Risk-profile source of truth | PASS (library and tests). The 0.4 % ceiling conflict is BLOCKED on the owner's decision | `a0a778f8` |
| 1.2 | Deployment contract | PASS (handlers and tests; synthetic exchange). Not run against a Binance demo account | `c4727c69` |
| 1.3 | Broker-derived environment | PASS (tests). A bot has no environment of its own | `c4727c69`, `829d4e27` |
| 1.4 | Risk-based execution sizing | PASS (unit and boundary integration) | `4a5294a6` |
| 1.5 | Billing enforcement and operator grants | PASS (tests) | `5ec23aa9`, `e6959aa3` |
| 1.6 | Engine read model | PASS (tests on the certification broker fake) | `eb39b587` |
| 1.7 | API/proxy repairs | PASS (tests; proxy to engine exercised locally) | `d3f4d2c9` |
| 1.8 | Production notifications | PARTIAL: events, outbox, retry, in-app and SSE tested; no email, Telegram or push message was sent through a real provider | `829d4e27` |
| 1.9 | Customer portal screens | PARTIAL: type-check, lint, build, unit tests; signed-in browser check of the dashboard, Auto Pilot and bot list on a scratch database. The deploy and bot-detail screens were not seen with a deployable account | `2b24889e`, `6766240` |
| 1.10 | Onboarding wizard | PASS (API tests with the wizard's exact requests; run through the two live services locally) | `94199bf1` |
| 1.11 | Fresh-install guide | PARTIAL: written and rehearsed on Windows; never executed on a Linux server | `b93e9eaa` |

**Step 1 overall: PARTIAL.** It cannot be marked COMPLETE: the Binance demo certification, real notification delivery
and the clean-server install were not executed (no demo credentials, no mail provider, no server). See "Closing test".

## 1.0a — Protection read safety

**Hazard confirmed.** `production_execution._reconcile_attempt` re-verified protection every 30-second
cycle through `place_native_protection`, and turned ANY exception from that step into the reduce-only
fail-safe market close. `place_native_protection` reads the venue's open conditional orders with
`get_algo_orders(raise_on_error=True)`; a timeout, connection reset, HTTP 5xx, 429/418 rate limit, or an
HTTP 200 body without the order list (the client converted that into `[]`) therefore closed a healthy
protected position. The same pattern existed in `boundary.reconcile_submit_unknown` for a recovered fill.

**Change.** New module `app/execution/protection_state.py` is the single classifier:

- `ABSENT` (fail-safe close still applies): `PROTECTION_CONFIRMED_ABSENT` (an acknowledged leg missing
  from two consecutive successful reads), `PROTECTION_LEG_ATTEMPTS_EXHAUSTED`,
  `PROTECTION_READ_BACK_GEOMETRY_MISMATCH`, or a definitive venue refusal of the CREATE that is about the
  order (e.g. -2021 "would immediately trigger"), never one about the request (-1003/-1015/-1021).
- `UNKNOWN` (position preserved): `PROTECTION_READ_UNAVAILABLE` (every failed or non-list read, raised by
  the new `open_protection_legs` helper), `PROTECTION_SUBMIT_OUTCOME_UNKNOWN`,
  `PROTECTION_READ_BACK_UNCONFIRMED`, and any unlisted exception.

On `UNKNOWN` the maintenance path records the doubt durably in `cati_production_protection_uncertainty`
(per account and symbol: reason, first/last seen, consecutive cycles), appends the history item, and raises
`PROTECTION_STATE_UNKNOWN` so the account fails closed for new entries while the next cycle retries. After
`UNKNOWN_ALERT_CYCLES` (4) consecutive cycles one CRITICAL `PROTECTION_STATE_UNKNOWN_OPERATOR_REVIEW` alert
is written to the existing alert store. The uncertainty is cleared when protection is confirmed, when the
venue reports the position flat, or when the fail-safe close runs. The runtime status
(`GET /api/v1/cati/runtime/status`) now carries `protection_uncertain` per account and a top-level
`protection_uncertain_positions` count. The Binance client raises `ALGO_ORDERS_RESPONSE_MALFORMED` instead of
returning `[]` for a malformed body when the caller asked for errors.

**Preserved.** The operator emergency flatten (`flatten_account` → `close_position`) does not consult the
classifier and closes while reads keep failing (tested). The executor's post-entry atomic chain (close a
just-opened entry whose protection could not be proven) is unchanged: that position never had proven
protection, and the rollback is the deliberate fail-closed entry behaviour.

**Files.** `app/execution/protection_state.py` (new), `app/execution/production_protection.py`,
`app/trading_intelligence/integration/production_execution.py`,
`app/trading_intelligence/execution/boundary.py`, `app/trading_intelligence/integration/production_runtime.py`,
`app/exchange/binance/client.py`; tests `tests/trading_intelligence/test_protection_read_safety.py` (new, 35
tests), `test_production_position_safety.py` (+2 tests; 3 expectations updated to the new codes),
`test_production_demo_execution.py` (1 expectation updated).

**Test command and result** (from `backends/bot-backend`, CI environment variables):

```
python -m pytest tests/trading_intelligence/test_protection_read_safety.py tests/trading_intelligence/test_production_position_safety.py tests/trading_intelligence/test_demo_boundary_certification.py tests/trading_intelligence/test_production_execution.py tests/trading_intelligence/test_production_demo_execution.py tests/test_emergency_controls.py tests/test_cati_production_config.py tests/test_sev1_protection_invariant.py tests/trading_intelligence/test_pre_section22_closure.py tests/trading_intelligence/test_perpetual_operational_contract.py -q
```

261 passed, 1 failed (`test_per_symbol_hook_is_labelled_diagnostic_only`, quarantine line 111, unrelated).

Scenarios covered: timeout, connection reset, HTTP 5xx, 429, 418, malformed body (each keeps the position
and fails the account closed); confirmed present after a failed read (uncertainty cleared, no new legs);
confirmed absent (fail-safe close still runs); absence confirmed only by a second read; repeated transient
failures (bounded alert, exactly once); process restart mid-verification; emergency flatten while uncertain;
recovered-fill path; classification table; client malformed-body handling.

## 1.0b — Exchange request-weight optimization

**Measured first.** A measurement harness (`tests/trading_intelligence/test_runtime_cost_budget.py`) drives the
real runtime entry point `production_runtime.sync_account(execute=True)` against the certification broker fake
and counts every broker call in Binance USDⓈ-M request-weight units, together with database connections and
statements. The same harness was run in a pristine export of `9fb120f` (its DB layer instrumented for counting
only) to produce the "before" column. Fixture caveat: the fixture clock sits early in the calendar month, so the
baseline's month-long income download is two pages (60 weight); mid-month it is five pages (150), i.e. the real
baseline idle cost is 116–206 and the audit's 116–266 range is consistent with it.

| Scenario (one account, one 30 s cycle) | Weight before → after | DB connections | DB statements | Schema statements | Wall time |
|---|---|---|---|---|---|
| No connected bot | 116 → 10 | 13 → 1 | 86 → 30 | 16 → 0 | 99 → 14 ms |
| Idle running bot (waiting for signal) | 116 → 51 | 27 → 1 | 157 → 72 | 24 → 0 | 216 → 14 ms |
| Idle running bot, income refresh due | 116 → 81 | 27 → 1 | 157 → 72 | 24 → 0 | 132 → 15 ms |
| One protected position | 130 → 93 | 34 → 1 | 199 → 93 | 25 → 0 | 181 → 16 ms |
| Pending close (lost acknowledgement) | 149 → 84 | 45 → 1 | 268 → 128 | 25 → 0 | 289 → 18 ms |
| Failed stop read (position preserved) | 72 → 61 | 26 → 1 | 150 → 44 | 24 → 1 | 163 → 14 ms |
| Restart with protected position | 130 → 93 | 38 → 7* | 227 → 128 | 31 → 6* | 236 → 39 ms |
| Three idle accounts | 348 → 30 | | | | |

\* the restart scenario constructs a second `DB` handle for the fixture's boundary; its six extra connections and
schema statements belong to that second handle, not to the cycle.

**Changes.**

- *Incremental income ledger* (`integration/income_ledger.py`, tables `cati_account_income`,
  `cati_account_income_cursor`): the venue is asked only for the uncovered part of the period plus a 6-hour
  late-record overlap; refreshed when the latch can matter (first computation of a risk day, an entry may be
  evaluated, a position is open) and otherwise every 5 minutes. Records are keyed by the venue transaction id
  (idempotent replay, duplicates ignored); every page still goes through the bisecting `complete_income` read, and
  an incomplete window leaves the cursor untouched so the caller fails closed as before. Covers funding fees,
  commissions, transfers and corrections as venue records. Tests: `test_income_ledger.py` (10).
- *No duplicate account read*: the runtime reads the full account document once and derives the balance view;
  `account_risk` reuses it (`account_state`) and asks the venue again only when it is missing or incomplete.
- *Scoped protection reads*: the pre-maintenance conditional-order snapshot no longer reads every symbol ever
  recorded in `cati_production_protection` (unbounded growth); legs resolved after a broker-confirmed flat
  (`_closed_flat`) are excluded, every other recorded leg keeps its symbol in the read. `place_native_protection`
  reads the symbol once per replay (both legs; a re-read only after a CREATE) and treats that successful read as
  the confirmation when nothing was created: 4 → 2 conditional-order reads per protected position per cycle.
- *Idle accounts* (`idle_account`): no active bot, no open position, no unresolved lineage → the account-wide
  open-order book (weight 40), the account-risk evaluation and the income read are skipped; positions are still
  read every cycle so a manual position or a newly deployed bot is seen on the next cycle; maintenance of existing
  lineage always runs first and is unaffected (paused/stopped bots with positions are maintained, tested).

## 1.0c — Database connection and schema optimization

- `DB.scope()` (shared library): one connection per thread-local scope; every inner `with db.connect()` keeps its
  own commit-on-success / rollback-on-raise, so each existing atomic unit stays atomic and no transaction spans the
  network calls between blocks. `sync_account` wraps each account cycle in a scope: 13–45 → 1 connection per
  evaluation (table above). Tests: `backends/shared/tests/test_db_scope.py` (6: shared connection, rollback of a
  raising block only, `BEGIN IMMEDIATE`, nested scopes, thread isolation, no lock left after an exception).
- `app/execution/production_schema.py`: all production DDL (decisions, daily risk, fills, protection, closes,
  period risk, state, evaluations with their triggers, protection uncertainty, income ledger) runs once per
  database file (memo keyed on file identity, so a database recreated at the same path is initialised again);
  `production_execution.initialize`, `production_runtime.initialize`, `place_native_protection`, `close_position`,
  `account_periods` delegate to it. The three remaining per-cycle `CREATE TABLE IF NOT EXISTS` sites
  (`broker_auto_trading`, `adaptive_daily_risk_decisions`, the ledger) create on first miss instead. The health
  column `PRAGMA table_info` in `save` is answered once per database. 16–31 → 0 schema statements per steady-state
  cycle. Tests: `test_production_storage_lifecycle.py` (6: startup, repeated evaluation, additive repeat on a
  populated database, legacy database gains the new tables, recreated file, interrupted initialisation completed by
  the next call, scoped connection used by the runtime).
- Instrumentation: `shared_lib.persistence.db.instrument()` / `METRICS` (connections opened and reused,
  statements, schema statements, pragmas, DDL texts).

## 1.0d — Production-loop tests

`tests/trading_intelligence/test_production_loop.py` (19) runs the real entry points with injected clocks and
controlled fakes: the asyncio `run` loop (startup, 30-second cadence from an injected monotonic clock, one-second
floor for slow cycles, lease absent means no broker contact, a database outage fails one cycle and the next
recovers, a failing auxiliary step never costs the broker cycle), duplicate `broker_cycle` invocations serialised
by the cycle lock, the real `sync` with the real `process_account` on a migrated database (no-tradable-bot account
discovered but not evaluated; a bot deployed between cycles picked up on the next; paused/stopped bots not
evaluated while their open position is maintained; broker outage then recovery; one failing account does not stop
the next; maintenance-only when an auxiliary step fails), the hourly collector's scheduler (at most once a minute,
five-minute rate-limit backoff, never without the lease), the kill switch blocking a new entry, two cycles on one
decision placing exactly one order, and LIVE submission remaining impossible (`BLOCKED_LIVE_ORDER_GATE`,
`LiveOrderSubmissionDisabled`). Exchange-side stop read-back and restart with open positions are covered by the
1.0a and 1.0b suites on the same entry points. The collector's decision logic itself is covered by the existing
`test_cati_residual_prospective.py`.

## 1.0e — Executor hazards

| Audit finding | Exact behaviour found | Change |
|---|---|---|
| Flip branch (`executor.py`, `is_flip`) | Cancelled every open order on the symbol (errors swallowed), sent a market close, then, without confirming the close had filled or the account was flat, continued into the opposite entry in the same tick: two broker mutations under one entry intent, no reconciliation between them. Unreachable from CATI in practice (`account_risk` blocks any entry while a position exists) but present in the shared executor. | Refused before any broker call: `FLIP_REFUSED` / `POSITION_REVERSAL_REQUIRES_EXPLICIT_CLOSE` (adapter maps to `REJECTED`). A reversal is close (explicit `CLOSE`), reconcile, enter later. |
| Fatal integration error | Entry filled, protection failed, rollback close failed: `FatalIntegrationError("Halting system")`; the legacy runner answered with `sys.exit(1)`; in CATI the exception reached the boundary's generic handler as `SUBMIT_UNKNOWN` with the fill identity lost and no alert. | Returns `PROTECTION_FAILED_CLOSE_FAILED` with the order identity and both errors, records a CRITICAL `NAKED_POSITION_OPERATOR_REQUIRED` alert, keeps the entry lock (the position exists). The adapter maps it to `SUBMIT_UNKNOWN`, so `recover_pending` reads broker truth and verifies protection under the 1.0a classification (proven absence runs the durable fail-safe close; a failed read keeps the position and alerts). `FatalIntegrationError` is no longer raised anywhere in `app/`; the runner's handler is dead code pending Batch 7. |
| Client rebuilt on gate exceptions | Any exception in the account cycle invalidated the cached broker client, including local gates (lease, shutdown, policy, paper bot, protection-state-unknown) that say nothing about the client; a persistent gate cost an exchangeInfo download and a time sync every cycle. | `client_doubt()`: local gate reason codes keep the client; transport, venue and unknown exceptions still rebuild on any doubt. |

Tests: `tests/test_executor_hazards_step1.py` (15) and the updated legacy test
`test_flip_is_refused_before_any_broker_call` in `test_entry_protection_never_again.py`. The four failures in
`test_executor_exception_handling.py` seen while running these suites are quarantine lines 28–31 (pre-existing).

## Regression gate after Phase A (commits `ff03e8d0` to `4a5294a6`)

Full program re-run on the working tree with the CI commands (bot-backend 19 min 40 s):

| Suite | Baseline `9fb120f` | After Phase A |
|---|---|---|
| bot-backend | 5086 passed, 5 failed, 106 xfailed, 2 xpassed | 5185 passed, 4 failed, 106 xfailed, 2 xpassed |
| user-backend | 409 passed | 409 passed |
| admin-backend | 88 passed, 6 xfailed | 88 passed, 6 xfailed |
| shared | 38 passed | 67 passed |
| root | 4 passed, 3 xfailed | 4 passed, 3 xfailed |

Failing test ids diffed: no test that passed at baseline fails now. One new failure appeared during the run
(`test_certification_loss_is_excluded_from_strategy_accounting_but_not_from_equity`: the test models the venue
income ledger lagging the wallet and expects the next evaluation to see new income) and was fixed by adding the
wallet-change refresh trigger to the income ledger before the commits were made; the file passes. Two tests that
failed at baseline now pass (`test_the_api_accepts_auto_and_custom_and_defaults_to_auto`,
`test_audit_generates_report_without_modifying_active_env`; both order-dependent).

## 1.1 — Single source of truth for risk levels

`backends/shared/shared_lib/risk_levels.py`: three frozen, versioned `RiskProfile` records
(`RISK_PROFILE_VERSION = "2026-10-08.v1"`) with the approved table values, explicit percentage units,
`fraction()` accessors, `as_dict` / `from_dict` serialization, validation (positive finite percentages, per-trade
risk within combined open risk, reduce threshold below stop threshold), legacy name mapping (`low` / `medium` /
`high` and the bot-backend preset names) for NEW deployments only, `money_view(level, budget)` (Decimal, risk
amounts rounded down to the cent, typical position range only with an explicit stop-distance assumption,
otherwise `STOP_DISTANCE_ASSUMPTION_REQUIRED`), `minimum_deployable_budget` (the budget below which a full-strength
order at the widest stop falls under the exchange minimum notional) and `as_json`. Nothing in the engine or the
frontend hard-codes a percentage of these profiles. Existing legacy presets (`BotInstanceService.get_risk_profile_preset`)
are untouched and still govern legacy bots.

**Policy conflict (not resolved here).** The engine ceiling `SystemLimits.max_risk_per_trade_ceiling = 0.004`
(0.4 %) is below Balanced (0.50 %) and Aggressive (0.75 %). The library carries the approved values and reports the
conflict: `money_view` returns `per_trade_risk_pct` (approved), `effective_per_trade_risk_pct` (what the engine
may use today), `ceiling_applied` and both money amounts. `effective_per_trade_risk_pct(..., ceiling_widening_approved=True)`
is the only way to use the wider value and nothing passes it. The engine's own clamp in
`resolve_effective_bot_policy` stays in force. This is recorded as BLOCKED for the Balanced and Aggressive
end-to-end risk acceptance until the project owner approves widening the ceiling.

Tests: `backends/shared/tests/test_risk_levels.py` (23): every table value, immutability, legacy names, Decimal
precision, ceiling behaviour, zero / negative / NaN / tiny / huge budgets, serialization round trip, typical range
assumption, minimum budget, invalid profiles, versioning and determinism.

## 1.4 — Risk-based position sizing

`backends/bot-backend/app/trading_intelligence/execution/risk_sizing.py` is the one Decimal formula:
`risk = budget x risk_fraction x strength`, `notional = risk / stop_distance`, then caps as ceilings in the
validated order: combined open risk (profile `max_open_risk`), leverage = min(resolved leverage, profile ceiling)
with liquidation safety already inside the resolved leverage, user maximum position, available margin less a
15 bps fill buffer, system notional limit, exchange step / min / max quantity, exchange minimum notional. A size the
exchange will not accept is `RISK_SIZE_BELOW_EXCHANGE_MINIMUM`, never inflated. Zero, negative, NaN, infinite and
out-of-range stop distances never produce an order (`RISK_SIZE_STOP_DISTANCE_INVALID`). Leverage is an output.

Integration inside the existing authority: a bot with `allocation_type == "risk_based"` is governed by the
versioned profile through `app/core/risk_profile_params.py` and the existing `EffectiveBotPolicy` (which keeps the
0.4 % clamp: a Balanced bot resolves `requested_risk_per_trade = 0.005`, `risk_per_trade = 0.004`). The
orchestrator's `process_trade_plan` sizes such plans after leverage resolution and hands the margin into the
unchanged hard-risk chain (Layer A/B/C), the preflight re-checks the quantity against current venue filters (which
the boundary now passes to sizing as well), and the capital ledger treats the budget as the per-trade ceiling.
Preview (1.2) will call the same function with assumed stop distances (`preview_range`).

Legacy bots: `fixed_amount` / `percent_balance` sizing is byte-for-byte unchanged (the harness quantity 4.7928 is
identical in the pristine baseline tree and the working tree).

Persistence: additive `bot_instances` columns `risk_profile_version`, `max_position_usdt`, `risk_acknowledged_at`,
`deploy_request_id`, `environment`, `stopped_reason` (+ index on `deploy_request_id`), read and written by the
model and service; `CreateBotInstanceRequest` validates `risk_based` (per-trade risk percent 0 < v <= 5, version
required, positive max position when given).

Tests: `tests/trading_intelligence/test_risk_based_sizing.py` (29): the master-plan example (1000 / 0.5 % / 8 % ->
5 USDT risk, 62.5 notional, 2x -> 31.25 margin), five stop distances incl. below-minimum, invalid stops, all three
profiles, fixed and percentage budgets, each cap binding in order (open risk, leverage, max position, margin,
system, exchange step/max/multiplier), signal strength, precision and overflow, preview = execution, the profile
to engine mapping, the policy resolver keeping the ceiling, legacy untouched, and three runs through the real
boundary and hard-risk chain (sized and executed at 0.8 qty less the fill reserve; blocked below the exchange
minimum before any broker call; user max position binding).

Not covered yet: concurrent reservations (the existing reservation store tests cover the race; a risk-based
specific test is pending with 1.2's concurrent-deploy test), and the open-risk input (`open_risk_usdt`) is 0 in the
production path because the frozen portfolio admits one position (`max_open_positions = 1`); the cap is implemented
and unit-tested for the multi-position case.

## 1.5 — Billing enforcement and operator grants

Commit `5ec23aa9` (and `e6959aa3` for the resume gate). Closed 8 October 2026.

* `BILLING_ENFORCED` (default `false`) in `shared_lib/billing/enforcement.py`, read by both backends. While it is false
  the plan gates in `bot-backend/app/core/plan_gate.py` and the user backend's broker-account limit block nothing.
* Operator grants in `shared_lib/billing/operator_grants.py` (`billing_operator_grants`): time-limited, audited,
  distinguishable from Stripe (`source: operator_grant`), not overwritten by a Stripe webhook while active (the event is
  deferred), revocable. Admin-only routes in `user-backend/app/api/admin_billing.py`.
* Defect found later by the closing rehearsal and fixed: with enforcement on, resuming a bot on a DEMO exchange account
  demanded the live-trading entitlement. The start route now applies that gate only to bots on a LIVE account.

Tests: `user-backend/tests/test_billing_enforcement_and_grants.py` (13), `bot-backend/tests/test_plan_entitlement_gates.py`
(run with enforcement on), `test_step1_closing_synthetic.py::test_a_demo_bot_can_be_resumed_...`.

## 1.2 and 1.3 — Deployment contract and broker-derived environment

Commit `c4727c69`. Closed 8 October 2026.

* `shared_lib/deployment/contract.py`: versioned request (`2026-10-08.v1`), decimal strings, unknown fields rejected,
  `request_id` for idempotency, blocker codes with message and action, consent text and version.
* `bot-backend/app/core/deployment_service.py`: one `evaluate` used by preview and deploy; deploy re-checks under
  `BEGIN IMMEDIATE`, writes the consent row, returns the same bot for a repeated `request_id`, 409 for a second bot on
  the account. Routes `POST /api/v1/auto-pilot/preview`, `POST /deployments`, `GET /bots`; the user backend forwards
  both the new and the legacy body from `POST /api/v1/auto-pilot/deploy`.
* The environment is read from `broker_accounts.environment` everywhere. A live account is refused
  (`LIVE_NOT_AVAILABLE`). The full regression run showed that this step had first put an `environment` field and
  column on the bot itself, against `test_11_the_bot_has_no_environment_of_its_own`; both were removed in `829d4e27`.
* One bot per account is enforced by the deploy transaction, not by a unique index: accounts with two active bots are a
  state the engine models and reports, and existing fixtures rely on it.

Tests: `bot-backend/tests/test_deployment_contract_step1.py` (16), `user-backend/tests/test_step1_proxies.py`,
`test_step1_closing_synthetic.py`, `test_step1_security.py`.

Not verified: a deployment against a real Binance demo account (no credentials were available).

## 1.6 — Engine read model

Commit `eb39b587`. Closed 8 October 2026.

* `bot-backend/app/core/cati_read_model.py` reads only what the engine persisted: status (stale after 120 s, never shown
  as running), eligibility with a registry reason, positions with the exchange-side protection state (unknown is never
  shown as confirmed), trades (gross realized, fees, funding, net = realized − fees + funding), summary windows, equity.
* `app/observability/account_recorder.py`: hourly and on-fill equity snapshots, 90-day retention with daily roll-ups.
* `app/trading_intelligence/integration/reason_registry.py`: about 128 engine codes with message, action and severity.
  An unmapped code is reported as such. The class name of an unexpected failure is described as a fault in words.
* Owner routes `/api/v1/cati/bots/{id}/status|positions|trades|summary|equity|events` (404 for anyone else) and admin
  routes under `/api/v1/admin/cati/bots`.

Tests: `test_cati_read_model.py` (10), `test_reason_registry.py` (12 with parametrisation).

## 1.7 — API and proxy repairs

Commit `d3f4d2c9`. Closed 8 October 2026.

* The monitoring proxy now targets routes the engine serves and requires an administrator, as the engine does.
* The forex instruments proxy no longer sends credentials in a request body; the engine resolves the caller's own.
* Five proxies read the engine address from one place. The portal's CATI panel and SSE hook use the one API base.
* Three client functions for routes no backend serves were removed.

Tests: `user-backend/tests/test_step1_proxies.py`, `frontends/user-frontend/src/tests/apiRoutes.test.ts`. Seen working
locally: the dashboard's CATI panel loaded the engine status through the user backend.

Known and left as found: the customer dashboard asks the admin-only `bots-overview` route for "active bots" and shows
0 when refused; `reports.ts` calls seven `/api/v1/reports/...` paths that no proxy serves (removal review, Batch 4).

## 1.8 — Customer events and notifications

Commit `829d4e27`. PARTIAL.

* `app/observability/user_events.py`: eight event types, emitted after the state change is committed, idempotent on a
  deterministic id, never raising into the trade path. Tables `user_events` and `notification_outbox`.
* In-app alert and SSE at emit time; email, Telegram and push from the notification worker with bounded backoff (eight
  attempts). A row is never marked sent unless the channel accepted it. `ENTRY_BLOCKED` is one event per account,
  decision and reason, and at most one external message per account per hour.
* `EventStore.emit` was writing a column set that matched neither shape of the `events` table and failed on both.

Tests: `test_user_events.py` (26): duplicate emission, restart replay, retry and exhaustion, channel selection, rate
limit, database and bus outage, secret stripping, SSE ownership, lifecycle events after commit, fills across restarts,
protection alert only after the bound, daily loss pause once.

Not verified: delivery through a real SMTP server, Telegram bot or push provider.

## 1.9 — Customer portal

Commits `2b24889e`, `6766240`. PARTIAL.

* Auto Pilot (crypto) deploys on the shared contract and shows the backend's preview. No leverage input, no paper/live
  switch. Deploy needs a current, non-stale preview and the acknowledgement; one request id per intent.
* Bot detail shows the engine's status, eligibility reason, stop state, trades, results and activity, each with
  loading, error, empty and stale states. My Bots shows the engine status, environment and budget.
* `src/lib/deployment.ts` holds the pure logic with unit tests.

Checks: `tsc -b` clean; eslint 0 errors (212 warnings in the whole project, existing convention); `vite build`;
`npm test` 40 pass.

Browser check on 8 October 2026 (local build, the two real services, a scratch database, a synthetic account without
credentials): sign-in works; the dashboard renders; Auto Pilot lists the account, shows the pre-filled form and the
backend's blocker with its action, and keeps Deploy disabled; the bot list shows its empty state. This found and fixed
a crash of the dashboard for any account with no equity reading (`6766240`).

Not verified: the preview with a readable balance, a successful deploy, and the bot-detail panel with live data.

## 1.10 — Onboarding

Commit `94199bf1`. Closed 8 October 2026.

The wizard and the service used different step names and payload keys, so every step after the welcome screen was
rejected. The wizard's names are now canonical, the earlier ones are aliases, saving a step returns the next state, the
risk appetite maps to a profile through the shared library, and completion returns a deployment prefill. Onboarding
deploys nothing.

Tests: `user-backend/tests/test_onboarding_step1.py` (24 with parametrisation), two frontend tests. Run through the two
live services locally: five steps accepted in order, completion returned `balanced` and budget `1000`.

## 1.11 — Fresh-install guide

Commit `b93e9eaa`. PARTIAL.

`docs/INSTALL_SINGLE_SERVER.md` covers the 25 required topics and the runtime states. Its section 26 lists what was
executed. On Windows, from a pristine export and an empty database: the migration (twice), a production-profile start
of the user backend and of the trading backend, the administrator bootstrap, 401 on unauthenticated routes, backup and
restore, the frontend build. Nothing was executed on a Linux server: no systemd, nginx, TLS, SMTP or exchange step.

## Migrations and security (Sections 19 and 20)

Commit `c649285a`.

| Schema change | Where |
|---|---|
| `bot_instances`: `risk_profile_version`, `max_position_usdt`, `risk_acknowledged_at`, `deploy_request_id`, `stopped_reason`; index on `deploy_request_id` | `migrations.py` |
| `deployment_consents` | `migrations.py` |
| `account_equity_snapshots`, `account_equity_daily` | `migrations.py`, also the once-per-database production schema |
| `user_events`, `notification_outbox` | `migrations.py`, also the once-per-database production schema |
| `billing_operator_grants` | created by the grants module on first use |
| `cati_production_protection_uncertainty`, `cati_account_income`, `cati_account_income_cursor` | production schema (Phase A) |

All additive. Recovery from a bad deployment is to run the previous code on the same file.

`shared/tests/test_step1_migrations.py` (7): clean database, populated database from before Step 1, repeated and
interrupted migration, constraints, `foreign_key_check` and `integrity_check`, owner scoping, no environment on a bot.

`test_step1_security.py` (14 with parametrisation): every Step 1 route requires a user or admin authority; the request
cannot carry environment, owner, consent time, leverage or keys; consent is written by the server only for an
acknowledged deploy; no credential appears in any portal response. Isolation between two customers on the new routes
and controls is in `test_step1_closing_synthetic.py`; SSE ownership in `test_user_events.py`. Withdrawal permission,
mandatory keys and 2FA are covered by their existing suites, which still pass.

Operator action outstanding: administrator passwords that exist in Git history must be rotated by the authorised
operator. Observation: the user backend serves `/docs` in the production profile (not published by the nginx site).

## Closing test (Section 21)

No Binance demo credentials, mail provider or server were available. Nothing below was run against a real exchange.

| Scenario | Status | Evidence and what is missing |
|---|---|---|
| A Account creation | PARTIAL | Registration, sign-in and onboarding ran through the live services locally. Email verification was not: the address was marked verified directly in the scratch database. No paid plan was needed |
| B Exchange connection | BLOCKED | No Binance demo credentials |
| C Deployment preview | PARTIAL | Synthetic: the preview equals the shared risk library (Balanced, 1000 USDT: 4.00 USDT risk per trade under the 0.4 % ceiling). Through the live services the preview was refused with `ACCOUNT_NOT_CONNECTED` for an account without credentials, as it should be |
| D Deployment | PARTIAL | Synthetic: ownership, consent, DEMO environment, engine discovery, 409 on a second bot, same bot on a repeated request. Not on a real demo account |
| E CATI execution | PARTIAL (synthetic only) | The real runtime cycle discovered and evaluated the deployed bot on a fake exchange and waited for a signal. Order, stop and read-back mechanics are covered on the certification broker fake. **No order was observed on the Binance demo exchange: pending** |
| F Customer visibility | PARTIAL | Read model verified against engine records in tests; dashboard, Auto Pilot and bot list seen in a browser. Not compared with exchange records |
| G Notifications | PARTIAL | Events, no duplicates, no cross-user delivery and retry after transport failure are tested. No message left through a real channel |
| H Bot controls | PASS (synthetic) | Pause, maintenance of an open position while paused, resume, stop, restart with the bot still stopped |
| I Admin controls | PARTIAL | Existing kill-switch and flatten suites pass; a customer token gets 401 on the admin routes (seen live). No operator drill was run by hand |
| J Resilience | PASS (tests) | Exchange timeout, failed stop read, restart with a protected position, database interruption, notification outage, repeated deployment, duplicate cycle, stale heartbeat. "Kill switch during an evaluation" is covered only as kill switch before the evaluation |
| K Clean-install repeatability | BLOCKED | No server. Windows rehearsal only (1.11) |

## Performance, final measurement

Same fixtures and method as Phase A (`test_runtime_cost_budget.py`), measured again at the final commit. Request weight
is unchanged from Phase A. Database statements rose slightly because Steps 1.6 and 1.8 record equity and events in
the cycle; connections and schema statements did not.

| Scenario | Weight before / after | Connections before / after | Schema statements before / after | Statements before / Phase A / final |
|---|---|---|---|---|
| Account without a bot | 116 / 10 | 13 / 1 | 16 / 0 | 86 / 30 / 33 |
| Idle running bot | 116 / 51 | 27 / 1 | 24 / 0 | 157 / 72 / 75 |
| Idle running bot, income refresh due | 116 / 81 | 27 / 1 | 24 / 0 | 157 / 72 / 75 |
| Protected position | 130 / 93 | 34 / 1 | 25 / 0 | 199 / 93 / 102 |
| Pending close | 149 / 84 | 45 / 1 | 25 / 0 | 268 / 128 / 148 |
| Failed stop read | 72 / 61 | 26 / 1 | 24 / 1 | 150 / 44 / 53 |
| Restart with a position | 130 / 93 | 38 / 7 | 31 / 6 | 227 / 128 / 137 |
| Three idle accounts | 348 / 30 | 39 / 3 | 48 / 0 | 258 / 90 / 99 |

Evaluation time on the fake exchange fell from 80–290 ms to 9–39 ms in Phase A. The final run measured 14–49 ms, but
it shared the machine with a full test run, so that figure is not comparable and no claim is made from it. Loop
cadence, the hourly collector, no false liquidation on a failed stop read, discovery of a new bot and no duplicate
orders are covered by `test_production_loop.py` and `test_protection_read_safety.py`.

## Removal batches (Section 24): reviewed, nothing deleted

| Batch | Finding | Readiness |
|---|---|---|
| 2 Backtest engine and portal pages | `app/backtest/` (2,195 lines), `api/backtesting.py`, the user proxy, six `backtest_*` tables, three portal pages. Creation already answers 409; list, detail and export are live. `main.py` imports and starts `BacktestWorker`; both routers are registered; `App.tsx` has three routes and a nav link; two tests import it | Blocked by that wiring and by the audit document's intent to keep historical read and export |
| 3 Shadow-trade subsystem | `app/shadow/` (2,088 lines), `api/shadow_routes.py` (10 routes), two tables. Off by default. Referenced by the router registration, a guarded hook in `runner/multi_runner.py` and the auth-hardening tests. Not referenced by the CATI production runtime or either frontend | Blocked by the registration, the hook and the tests; otherwise callerless in this repository |
| 4 Callerless routers | No dead router module in the engine. Seven user-proxy routes have no frontend caller (three monitoring, three analytics, risk templates). Several engine admin prefixes have no caller in this repository | The seven proxy routes are ready on repository evidence, pending confirmation that nothing external calls them. The engine admin prefixes are blocked by unknown operator callers |

Batch 1 was not redone. Batches 7 and 8 were not touched.

## Unresolved decisions

1. **The 0.4 % per-trade ceiling.** The approved profiles say 0.5 % (Balanced) and 0.75 % (Aggressive). The engine's
   ceiling is 0.4 %. The code applies the stricter value, the preview says so, and nothing widens it. Until the owner
   decides, Balanced and Aggressive risk the same per trade and differ only in their other limits.
2. **Push to `origin/main`.** The work is committed on local `main` (the owner's standing instruction is to work on
   `main`). It has not been pushed: the Step 1 instructions ask for approval and CI evidence before anything reaches
   `main` on the remote.
3. **Rotation of administrator passwords present in Git history** (operator).
4. **Removal batches 2 to 4**: whether to proceed, given the blockers above.

## Final regression gate (code commit `179de7bc`, 8 October 2026)

Run from a pristine `git archive` export of the commit with the CI commands and the quarantine plugin, and compared by
failing test id with the baseline run of `9fb120f`. The quarantine list is unchanged at 125 lines.

| Suite | Baseline | Final | New failures |
|---|---|---|---|
| bot-backend | 5,086 passed, 5 failed, 5 skipped, 106 xfailed, 2 xpassed | 5,294 passed, 0 failed, 11 skipped, 106 xfailed, 2 xpassed | 0 |
| user-backend | 409 tests, 0 failed | 469 tests, 0 failed | 0 |
| shared | 38 passed | 74 passed | 0 |
| admin-backend | 88 passed, 6 xfailed | 88 passed, 6 xfailed | 0 |
| root | 4 passed, 3 xfailed | 4 passed, 3 xfailed | 0 |

* The five baseline failures do not fail now, but that is not a Step 1 fix: three of them assert the operator's private
  `.env` and are skipped where it is absent, as in this export; the other two pass here. All five depend on that file.
* One test was removed on purpose: `test_flip_closes_old_side_before_new_side_proceeds` asserted the position flip that
  1.0e forbids. Its replacement asserts the refusal.
* An intermediate run (commit `eb39b587`) found four regressions, all fixed before this gate: the bot-level
  environment, schema statements inside a cycle, the legacy deploy proxy signature (two tests). A run of `829d4e27`
  found one more, a 1.0e test that depended on the operator `.env` (`179de7bc`).
* Frontend, after the last frontend commit `6766240`: `tsc -b` clean, eslint 0 errors, `vite build` succeeds,
  `npm test` 40 pass. The admin frontend was not changed and not rebuilt.
* Not run: `tests/integration` (needs both backends running; excluded from CI), browser end-to-end automation (none
  exists in the repository), the Binance demo certification flow.
