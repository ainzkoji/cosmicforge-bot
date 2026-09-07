# CosmicForge — Phase 0–4 Implementation Report

Batch 1 of the Pre-AI Master Blueprint. Phases 5+ were not started.

---

## 1. Baseline

| Item | Value |
| --- | --- |
| Repository | `cosmicforge-bot` (`origin` = `github.com/ainzkoji/cosmicforge-bot`) |
| Baseline branch | `main` |
| Baseline commit | `db4580b185fd59d3a8f30adf08a30f18ccb6fb84` — *"Core implementation done and user and admin setup done, started connecting the frontend to the backend"* (2026-01-16) |
| Working branch | `phase-0-4-runtime-baseline` (not pushed) |
| Final commits | `b35c198` (Phase 0–4), `7f2bca6` (attribution guards) |
| Working tree at baseline | 1,144 changed paths: `backends/`, `frontends/`, `tests/`, `scripts/`, `docs/` **entirely untracked**; `backend/`, `frontend/` deleted on disk but still tracked; 838 tracked `venv/` files |

### Canonical roots

| Role | Path |
| --- | --- |
| `canonical_backend_root` | `backends/bot-backend` |
| Sibling backends | `backends/admin-backend`, `backends/user-backend` |
| `canonical_shared_library_root` | `backends/shared/shared_lib` (installed editable as `cosmicforge_shared`) |
| `canonical_frontend_root` | `frontends/user-frontend`, `frontends/admin-frontend` |
| `canonical_server_entrypoint` | `backends/bot-backend/app/main.py` (`uvicorn app.main:app`) |
| `canonical_test_root` | `backends/bot-backend/tests` (1,761 tests) |
| **Server / test interpreter** | **`backends/venv/Scripts/python.exe`** (Python 3.12.2, 114 packages) |
| Active database | `backends/shared/shared_lib/persistence/cosmicforge.db` (3.38 GB) |

**Interpreter correction.** Two virtual environments exist. The repository-root
`venv/` (72 packages) is *not* the server environment — running the suite there
produced 15 failures that were entirely `ModuleNotFoundError: apscheduler`,
unrelated to any code under test. `backends/venv` is the interpreter the server
actually runs on, and is now the documented canonical one. This was confirmed
directly by the operator's own diagnostic invocation.

---

## 2. Phase 0 — Repository and runtime baseline

### Source control

- Tracked the plural `backends/`, `frontends/`, `tests/`, `scripts/`, `docs/`,
  `.github/`, `manual_tests/` trees (1,179 files). A clean checkout now contains
  the code the server imports.
- `git rm -r --cached venv` — 838 files of dependency noise removed from history
  going forward.
- Staged removal of the legacy singular `backend/` and `frontend/` trees
  (284 files) already deleted on disk and already ignored via `/backend/`,
  `/frontend/`.
- Untracked file count: **1,144 → 0**.

### `.gitignore` additions

| Rule | Reason |
| --- | --- |
| `*.pyc.*`, `/tmp_shadow/`, `**/tmp_shadow/` | Shadow compile cache wrote 190 bytecode variants that `*.pyc` did not match |
| `*.bak`, `*.bak-*`, `*.orig`, `*.rej` | `runner.py.bak-get-run-id-fix` and `runner.py.bak-symbol-fix` (~700 KB of stale source copies) |
| `**/models/reports/` | Generated replay/diagnosis JSON, up to 2.4 MB each |
| `**/*.db`, `**/*.db-wal`, `**/*.db-shm` | Runtime evidence databases at any depth |
| `'@*`, `%s` | Shell-quoting accidents at the repository root |

### Artifact classification (nothing deleted)

| Artifact | Size | Class | Action |
| --- | --- | --- | --- |
| `backends/shared/.../cosmicforge.db` | 3.38 GB | **SOURCE OF TRUTH** | Active; ignored, retained |
| `backends/shared/.../cosmicforge-LAPTOP-5B3QOQDJ.db` | 3.24 GB | **FORENSIC / STALE_COPY** | OneDrive sync-conflict copy from 2026-08-25. Retained; it is the cause of the existing `[DATABASE_EVIDENCE_WARNING] multiple_candidates` line |
| `repo_cleanup_backup_20260502_134654/` | ~47 MB | FORENSIC | Retained, ignored |
| `%s`, `'@ + $dbPath + @'`, `…-journal` | 0–512 B | RUNTIME_GENERATED | Zero-byte PowerShell quoting accidents. Retained and ignored rather than deleted — classification before removal |
| `app.db`, `bot.db`, `tmp_ml_validation.db`, `tmp_signal_api_main_import.db` | 0 B | RUNTIME_GENERATED | Empty; retained, ignored |

Database resolution was already deterministic (`DATABASE_URL` first, resolved
relative to `backends/bot-backend`, with a hard startup guard that raises on a
DB split). No nearby database is ever silently selected.

### Startup diagnostics

New `app/ops/runtime_baseline.py` emits at startup:

```
[RUNTIME_BASELINE] code_revision=… branch=main working_tree_dirty=True
  process_id=42932 python_executable=…\python.exe python_version=3.12.2
  application_root=…\backends\bot-backend
  resolved_db_path=…\cosmicforge.db db_exists=True db_size=3381989376
  db_schema_version=0 execution_mode=paper broker_environment=resolved-per-bot
  environment_name=unknown timestamp=…
```

No secrets, keys or credentials are logged. Diagnostics never block startup.

### Environment standardisation

- `requirements.txt` was **UTF-16 encoded** — `pip install -r` could not parse
  it. Normalised to UTF-8 and added the missing `pytest`, `httpx`.
- New `pytest.ini` pins `testpaths`, excludes `integration`/`tmp_shadow`/`models`,
  and documents the canonical invocation:

  ```bash
  cd backends/bot-backend && ../venv/Scripts/python.exe -m pytest tests -q
  ```

- New `conftest.py` puts `bot-backend` and `shared` on `sys.path` so imports
  resolve identically from any launch directory, forces `EXECUTION_MODE=paper`,
  and installs an **autouse live-order guard** that raises
  `LiveOrderSubmissionBlocked` on any `POST`/`PUT`/`DELETE` to a broker order
  endpoint across all shipped adapters. Read-only market-data calls are
  untouched so connectivity tests still work; a transport test can opt out with
  `@pytest.mark.allow_live_orders`.

---

## 3. Phase 1 — EffectiveBotPolicy

**Location:** `backends/bot-backend/app/runner/effective_policy.py`

A frozen dataclass carrying identity, execution, capital, risk, entry, execution
feasibility, optional subsystem modes and metadata — 50 fields.

### Resolution precedence

```
SystemLimits (absolute ceiling)
      ↓
Operator/global settings (settings.MAX_TRADES_DAILY, MAX_OPEN_POSITIONS, …)
      ↓
Risk-profile request (conservative / balanced / aggressive preset)
      ↓
Safe default
      ↓
EffectiveBotPolicy   ← the only value the runtime reads
```

Nothing may exceed a `SystemLimit`; a profile or user setting may be stricter.

### Added this batch

- **Requested-vs-effective for every clamped limit.** Previously only
  `requested_risk_per_trade` was carried. Added `requested_max_leverage`,
  `max_leverage_ceiling`, `requested_max_open_positions`,
  `requested_max_daily_trades`.
- **Structured clamp records.** Each reduction now appends
  `{setting, requested_value, effective_value, hard_ceiling, clamp_reason}`
  with reasons `SYSTEM_LIMIT_CEILING`, `OPERATOR_LIMIT`,
  `ASSET_CLASS_LEVERAGE_CEILING`. No clamp is silent.
- **Fail-closed validation** for unsupported timeframes (`INVALID_TIMEFRAME`)
  and any limit resolving to a non-positive value (`NON_POSITIVE_LIMIT`).

### Policy hash

SHA-256 over `runtime_payload()` — a canonical, sorted, separator-normalised
JSON encoding. Explicitly **excluded**: `resolved_at`, `policy_hash`,
`clamp_warnings`, `clamps`. **Included**: execution mode, broker environment,
strategy, symbols, timeframes, capital, allocation, every risk value and limit,
leverage, entry configuration, and all optional subsystem modes.

Verified deterministic across repeated resolutions and sensitive to every
material field (7 parametrised cases plus broker environment).

### Diagnostics

`GET /bot-instances/{id}/effective-policy` now returns the full policy —
`policy_hash`, `clamps`, and a `requested_vs_effective` block — plus
`runner_policy_hash` and `policy_stale` read from the live `MultiBotRunner`
cache. An unresolvable policy returns **409 `CONFIGURATION_INVALID`** with its
reason code rather than an invented policy. No broker credentials are exposed
(asserted by test).

### Consumers

Migrated: `BotRunContext` (via `from_effective_policy`), `PaperRunner`,
`TradingOrchestrator` (`self.effective_policy.risk_per_trade`), `Executor`,
`MultiBotRunner`, readiness/approval gating, status endpoints.

Remaining legacy consumers (documented, not regressions): `main.py`'s singleton
`paper_runner_instance` used by the non-Auto-Pilot manual endpoints still reads
global settings. It is outside the `MultiBotRunner` → `run_cycle` active path.

---

## 4. Configuration authority — before / after

| Setting | Before | After |
| --- | --- | --- |
| Execution mode | process `EXECUTION_MODE`, DB `mode`, cached runner | `EffectiveBotPolicy.execution_mode` |
| Broker environment | inferred from mode/base URL | `EffectiveBotPolicy.broker_environment` (from broker account) |
| Symbols / timeframe | globals, DB, cached runner | `EffectiveBotPolicy` |
| Capital budget | DB, **`or 10000` fallback** | `EffectiveBotPolicy.capital_budget`, fail-closed |
| Per-position allocation | DB, context, executor defaults | `EffectiveBotPolicy.position_allocation_*` |
| Risk per trade | preset + globals | `EffectiveBotPolicy.risk_per_trade` (+ requested + ceiling) |
| Max daily trades | preset / settings / SystemLimits (3 vs 200) | `EffectiveBotPolicy.max_daily_trades` |
| Max positions | preset / settings / SystemLimits (2 vs 3 vs 20) | `EffectiveBotPolicy.max_open_positions` |
| Daily-loss limit | hardcoded % in orchestrator | `EffectiveBotPolicy.max_daily_loss` |
| Max leverage | preset, symbol map, SystemLimits | `EffectiveBotPolicy.max_leverage` (+ asset-class ceiling) |

---

## 5. Phase 2 — Runner refresh

**Before.** `MultiBotRunner` rebuilt the context each cycle but reused the cached
`PaperRunner`, refreshing only client handles. Executor mode, orchestrator
limits, capital, risk, symbols, timeframes and leverage could all remain stale.

**After.** Every cycle: load bot → resolve auth → resolve candidate policy →
compute hash → compare with `runner.effective_policy_hash`.

| Case | Behaviour |
| --- | --- |
| No cached runner | Create, store policy + hash, restore lifecycle from persisted state |
| Hash unchanged | Reuse runner; refresh runner/executor/paper-executor/strategy client handles |
| Hash changed | Log `[RUNNER_POLICY_CHANGE]` with `bot_id`, `old_hash`, `new_hash`, `changed_fields`; invalidate readiness approval; **evict safely**; rebuild; log `[PAPER_LIFECYCLE] event=RESTORE` |

**Added this batch — safe eviction** (`_evict_runner`). Previously the stale
runner was replaced in place. Now it is stopped (`_stop_requested = True`) and
any open positions it still holds in memory are flushed to the store *before*
the replacement is constructed — the replacement rehydrates them from persisted
state in `PaperRunner.__init__`. Eviction **never closes a position**: a
configuration change is not an exit signal. A failing store does not abort the
eviction. `_log_restored_lifecycle` then emits the positions the rebuilt runner
inherited.

Paper positions are restored from persisted paper state and are never
reconciled against the exchange; broker-mode positions participate in exchange
reconciliation. `_step_symbol_orchestrated` reads `get_position_info` only when
`_effective_execution_mode() == "broker"`.

---

## 6. Execution mode vs broker environment

Two orthogonal dimensions, both observable:

```
execution_mode     : paper | broker
broker_environment : demo | testnet | mainnet
```

Legacy DB values normalise deterministically — `paper`/`sim`/`simulation` →
`paper`; `live`/`broker`/`testnet`/`demo` → `broker`; anything else raises
`INVALID_EXECUTION_MODE`.

> A row with `mode="live"` against a `demo` broker account resolves to
> **execution_mode = broker, broker_environment = demo**. It is never described
> as paper. This is asserted by test.

Mainnet is not activated by this refactor; Product Safety readiness gating is
untouched, and readiness approval is invalidated on any material policy change.

---

## 7. Phase 3 — Auto Pilot capital flow

```
Frontend  { total_capital_budget: 500, trade_amount_per_position: 50,
            allocation_type: "fixed_amount" }
   ↓  AutoPilotAllocation           500 / 50 / fixed_amount   (validated)
   ↓  DeployAutoPilotRequest        legacy flat fields normalised
   ↓  service.deploy_auto_pilot()   capital_allocation=500, allocation_value=50
   ↓  CreateBotInstanceRequest      capital_allocation=500, allocation_value=50
   ↓  bot_instances table           capital_allocation=500, allocation_value=50
   ↓  BotRunContext                 capital_budget=500, trade_usdt_per_order=50
   ↓  EffectiveBotPolicy            capital_budget=500, position_allocation=50
   ↓  Risk / Executor               sized against 500, constrained to 50
```

Verified end-to-end by `test_auto_pilot_capital_flow.py` (18 tests). The
fabricated 10,000 budget is gone: a source-level guard asserts neither
`or 10000` nor `or 10_000` appears in `bot_context.py` or `effective_policy.py`,
and a missing budget raises `CAPITAL_BUDGET_REQUIRED` rather than being
repaired.

Percentage allocation is preserved as a percentage — `percent_balance` at 10%
of a 500 budget yields `trade_usdt_per_order == 50.0` while
`position_allocation_value` stays `10.0` and `get_trade_amount_settings()`
returns `("percent", 10.0)`.

### Observed in production during this work

The live bot `bot_e5fe913972a9` is a legacy NULL-capital row and is currently
**correctly blocked**:

```
capital_allocation:        None
allocation_value:          120.0
mode: live   |   broker environment: demo   →  execution_mode = broker
bot_health_status:         ERROR_CONFIGURATION
bot_health_reason_code:    CAPITAL_BUDGET_REQUIRED
```

This is Phase 3 §36 working as designed — the bot is reported, not repaired. It
requires explicit operator recovery (see §15).

---

## 8. Risk contract

| Profile | Per-trade risk | Positions | Daily loss |
| --- | --- | --- | --- |
| Conservative | 0.15 % | 1 | 3 % |
| Balanced | 0.25 % | 2 | 5 % |
| Aggressive | 0.40 % | 3 | 10 % |
| **SystemLimits ceiling** | **0.40 %** | **20** | **10 %** |

`Conservative < Balanced < Aggressive ≤ ceiling` holds, and each profile
resolves to exactly its requested risk with **no clamping** — so routine Auto
Pilot deploys never trip the clamp path. No SystemLimit was raised to
accommodate a preset. Asserted by `test_risk_profiles_are_ordered_and_within_the_system_ceiling`
and `test_routine_profiles_do_not_require_clamping`.

Other effective values for a balanced 500-budget bot: max daily trades **3**
(operator limit, from 200 system ceiling), max open positions **2**, max
leverage **10×** (major) / **7×** (alt), daily loss **25.00 USDT**.

Clamping example:

```
requested max_leverage = 125.0  →  effective 10.0
  hard_ceiling 10.0, clamp_reason ASSET_CLASS_LEVERAGE_CEILING
requested risk_per_trade = 0.5  →  effective 0.004
  hard_ceiling 0.004, clamp_reason SYSTEM_LIMIT_CEILING
```

---

## 9. Phase 4 — Paper lifecycle

```
OPEN 1.0    → original_qty 1.0, remaining_qty 1.0, realized 0.0, status OPEN
TP1  0.5    → realized 0.5, remaining 0.5, status PARTIALLY_CLOSED, fill persisted
BE/trailing → operate against the 0.5 remainder (unchanged behaviour)
CLOSE 0.5   → closes the remainder, remaining 0.0, FLAT, fill persisted
RESTART     → remaining 0.5 after TP1, never 1.0; FLAT stays FLAT
```

### Defect found and fixed

The `step_symbol` TP1 call site (`runner.py:5294`) reduced `st.entry_qty`
in memory but persisted **neither the SymbolState nor a fill row**. Consequences:

1. On restart the remaining quantity reverted to the pre-TP1 size (1.0).
2. A subsequent close would then execute the **original** quantity, not the
   remainder — the exact failure the blueprint's mandatory invariant forbids.
3. No TP1 evidence existed, so the accounting invariant was unverifiable.

Both TP1 call sites now route through one `PaperRunner._persist_tp1_outcome()`
helper that saves the authoritative remaining quantity to the store and records
the partial-close fill. Guarded by
`test_j_both_tp1_call_sites_persist_through_the_shared_helper`.

### Close contract

Explicit fail-closed reason codes, surfaced on `result.details["reason_code"]`:

| Code | Condition |
| --- | --- |
| `PAPER_CLOSE_SIDE_UNKNOWN` | Side cannot be determined — never assumes LONG |
| `PAPER_CLOSE_QUANTITY_UNKNOWN` | Quantity ≤ 0 — never infers a fake zero |
| `PAPER_CLOSE_POSITION_NOT_FOUND` | No simulated position |
| `PAPER_CLOSE_QUANTITY_MISMATCH` | Requested ≠ authoritative remainder |

A rejected close leaves the position intact. New `get_position()` and
`remaining_quantity()` accessors give callers one authoritative remainder;
`remaining_quantity()` returns `None` for "unknown" rather than a phantom `0.0`.

### Quantity synchronisation

`PaperExecutor.remaining_qty` = `PositionManager.current_qty` = persisted
`SymbolState.entry_qty` = canonical remainder. The final close reads
`pos.current_qty` (post-TP1) with `st.entry_qty` as fallback — both now reduced
and persisted.

### Accounting invariant

`open − Σ partials − final == 0` within 1e-9, verified for LONG and SHORT across
1, 2 and 3 partial closes (6 parametrised cases), plus
`realized_qty + remaining_qty == original_qty` at every intermediate step.

---

## 10. Fill attribution

`bot_instance_id`, `run_id`, `cycle_id`, `position_id` and `user_id` are five
distinct identifiers. All 8 `record_fill` call sites in `runner.py` were audited:
every one passes `bot_instance_id=self.context.bot_instance_id` and
`run_id=self.run_id`. The orchestrated full-close path carries
`position_id=_pm_close_pos_id`, `run_id=_pm_run_id`, `cycle_id=_pm_cycle_id`,
and logs `[FILL LINKAGE ERROR]` when linkage is missing rather than persisting
silently.

Three regression guards now fail the build if any call site conflates them, if
the full-close path loses its linkage, or if the final close reverts to the
original quantity.

---

## 11. Database migrations

**None required.** Every field Phases 0–4 depend on already exists:
`bot_instances.capital_allocation`, `capital_allocation_type`,
`allocation_type`, `allocation_value`, `bot_health_*`; and `trade_fills`
already carries `fill_type`, `remaining_qty`, `position_id`, `run_id`,
`cycle_id`, `bot_instance_id`, `user_id`.

No rows were deleted, no historical trading evidence removed, no database copy
auto-deleted.

---

## 12. Files changed

### Source

| File | Purpose |
| --- | --- |
| `.gitignore` | Runtime-generated artifact exclusions |
| `backends/bot-backend/requirements.txt` | UTF-16 → UTF-8; added `pytest`, `httpx` |
| `backends/bot-backend/pytest.ini` | **New** — canonical test config + invocation |
| `backends/bot-backend/conftest.py` | **New** — path bootstrap + live-order guard |
| `backends/bot-backend/app/ops/runtime_baseline.py` | **New** — `[RUNTIME_BASELINE]` |
| `backends/bot-backend/app/main.py` | Emit runtime baseline at startup |
| `backends/bot-backend/app/runner/effective_policy.py` | Requested/effective fields, structured clamps, timeframe + non-positive validation |
| `backends/bot-backend/app/api/bot_instances.py` | Full policy diagnostics, `policy_stale`, 409 on invalid |
| `backends/bot-backend/app/runner/multi_runner.py` | `_evict_runner`, `_log_restored_lifecycle` |
| `backends/bot-backend/app/runner/runner.py` | `_persist_tp1_outcome`; both TP1 sites routed through it |
| `backends/bot-backend/app/execution/paper_executor.py` | Close reason codes, `get_position`, `remaining_quantity` |

### Tests

| File | Tests |
| --- | --- |
| `tests/test_effective_bot_policy.py` | **New** — 35 |
| `tests/test_runner_policy_refresh.py` | **New** — 19 |
| `tests/test_auto_pilot_capital_flow.py` | **New** — 18 |
| `tests/test_paper_lifecycle_matrix.py` | **New** — 38 (matrix A–L + guards) |
| `tests/test_paper_executor.py` | Updated 1 assertion to the explicit reason-code contract |

**110 tests added, 1 updated, 0 deleted.**

### Changed test expectation

`test_paper_mode_close_requires_an_existing_internal_position` asserted the
ad-hoc string `"position_side_required"`. The close contract now fails with the
explicit `PAPER_CLOSE_SIDE_UNKNOWN` reason code (blueprint §48), surfaced on
both `result.error` and `result.details["reason_code"]`. The test asserts the
new contract — the behaviour it guards (fail closed, no exchange call) is
unchanged and still asserted.

---

## 13. Test results

Canonical interpreter, canonical invocation:

```
cd backends/bot-backend && ../venv/Scripts/python.exe -m pytest tests -q
```

| | Passed | Failed | Skipped | Errors | Warnings |
| --- | --- | --- | --- | --- | --- |
| Baseline (wrong venv) | 1,636 | 15 | 0 | 0 | 996 |
| **Final (canonical venv)** | **1,761** | **0** | **0** | **0** | **27** |

Plus 4 subtests passed. Reconciliation: 1,636 + 110 new + 15 recovered = 1,761.

The 15 baseline failures were `ModuleNotFoundError: No module named 'apscheduler'`
from the wrong virtual environment — **PRE-EXISTING, environmental, now
resolved** by standardising on `backends/venv`. No test was deleted or skipped
to reach green.

---

## 14. Observability

| Event | Emitted from | Contents |
| --- | --- | --- |
| `[RUNTIME_BASELINE]` | startup | revision, branch, dirty, pid, interpreter, version, app root, DB path/size/schema, execution mode, broker environment, environment, timestamp |
| `[DATABASE_EVIDENCE]` / `_WARNING` | startup | active DB identity; warns on multiple candidates |
| `[EFFECTIVE_POLICY]` | policy resolution | bot, hash, execution mode, broker environment, capital, allocation, risk, symbols, timeframe, clamps |
| `[RUNNER_POLICY_CHANGE]` | cache rebuild | bot, old/new hash, changed fields, `runner_rebuilt=true` |
| `[PAPER_LIFECYCLE]` | TP1, restore | bot, run_id, position_id, symbol, side, executed/remaining qty, fill_id, status |
| `[FILL LINKAGE ERROR]` | fill persistence | missing run/cycle linkage |

No secrets, API keys or broker credentials appear in any event.

---

## 15. Remaining known issues

1. **`bot_e5fe913972a9` is blocked pending operator recovery.** Legacy NULL
   `capital_allocation`. Deliberate fail-closed behaviour, not a bug — but the
   bot will not trade until an operator sets an explicit budget. Its original
   configuration is not reconstructable: it predates the audit log, and no
   `AUTOPILOT_DEPLOYED` event exists for it. **The capital value is the
   operator's decision and must not be inferred.** Note it is
   `execution_mode=broker` / `broker_environment=demo`, not paper.

2. **Tests write to the production audit log.** `deploy_auto_pilot` calls from
   the suite appended `AUTOPILOT_DEPLOYED` events for fixture users
   (`user_123`, `brk_abc`, `brk_oanda`) to
   `backends/bot-backend/logs/live_audit.jsonl` (80 MB). Verified **not** a
   database contamination — `bot_instances` holds 6 rows, all the real user's,
   none created today. The audit sink is file-based and not DB-scoped.
   **NON-BLOCKING**, out of scope for this batch; recommend routing the audit
   sink through the test DB path in a later batch.

3. **`cosmicforge-LAPTOP-5B3QOQDJ.db`** (3.24 GB OneDrive sync-conflict copy)
   sits beside the active database and triggers the existing
   `multiple_candidates` warning every startup. Retained as forensic evidence;
   resolution is an operator decision.

4. **Legacy `paper_runner_instance` singleton** in `main.py` still reads global
   settings rather than an `EffectiveBotPolicy`. It serves non-Auto-Pilot manual
   endpoints and is outside the `MultiBotRunner` active path. **NON-BLOCKING**,
   documented as a remaining legacy consumer.

5. **`feedparser` missing from `backends/venv`.** Listed in `requirements.txt`;
   the news ingestion worker degrades gracefully and no test depends on it.
   **NON-BLOCKING.**

6. **`repo_cleanup_backup_20260502_134654/`** (~47 MB) remains on disk, ignored.
   Retained pending operator confirmation that it is no longer needed.

No **BLOCKING** issues remain.

---

## 16. Phase completion

| Phase | Status |
| --- | --- |
| Phase 0 — Canonical repository and runtime baseline | **COMPLETE** |
| Phase 1 — EffectiveBotPolicy and runtime source of truth | **COMPLETE** |
| Phase 2 — Persistent runner refresh and mode/environment separation | **COMPLETE** |
| Phase 3 — Auto Pilot allocation and risk contract | **COMPLETE** |
| Phase 4 — Complete paper position lifecycle and fill truth | **COMPLETE** |

---

## 17. Final verdict

```
PHASE 0–4 IMPLEMENTATION VERDICT:

READY to proceed to Phase 5.

Blocking items:
- None.

Operator actions required before live paper accumulation resumes:
- Set an explicit capital budget for bot_e5fe913972a9, or retire the row.
  The value is a business decision and was deliberately not inferred.
```

Phase 5 was not started.
