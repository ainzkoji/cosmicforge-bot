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
| 1.0a | Protection read safety | PASS (unit/integration) | see below |
| 1.0b | Request-weight optimization | NOT STARTED | |
| 1.0c | Database churn reduction | NOT STARTED | |
| 1.0d | Runtime and collector tests | NOT STARTED | |
| 1.0e | Executor hazard removal | NOT STARTED | |
| 1.1 | Risk-profile source of truth | NOT STARTED | |
| 1.2 | Deployment contract | NOT STARTED | |
| 1.3 | Broker-derived environment | NOT STARTED | |
| 1.4 | Risk-based execution sizing | NOT STARTED | |
| 1.5 | Billing enforcement and operator grants | NOT STARTED | |
| 1.6 | Engine read model | NOT STARTED | |
| 1.7 | API/proxy repairs | NOT STARTED | |
| 1.8 | Production notifications | NOT STARTED | |
| 1.9 | Customer portal screens | NOT STARTED | |
| 1.10 | Onboarding wizard | NOT STARTED | |
| 1.11 | Fresh-install guide | NOT STARTED | |

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
