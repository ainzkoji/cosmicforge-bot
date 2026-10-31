# Legacy runtime disposition (Section I Batch 7, master prompt section 10)

Assessed 31 October 2026 against `main` at `d9010c2` plus this pass's working-tree changes (E-04, E-05).
This file classifies every remaining reference to the legacy runner. It deletes nothing: the owner has not
authorized removal of the legacy routes or of Batches 2–4, 6, 9 and 10 (decision L-04).

## Classes

| Class | Meaning |
|---|---|
| **replace-with-CATI-equivalent** | A CATI-side path serves this behaviour, or now does after this pass. The legacy item can go once the owner authorizes it. |
| **retain-as-compatibility** | Kept so an existing response shape, guard or probe stays the same. Nothing supported depends on the legacy runner behind it. |
| **retain-for-isolated-simulation** | Part of the only working non-production simulation (V2 `PaperRunner` per bot, and the CATI cycle `cycle_shadow` → `cati_dispatch` that the legacy runner hosts). It must not be removed until that simulation has another home. |
| **retire-obsolete** | No reader and no behaviour: it always answers HTTP 410, or it is an ad-hoc tool. Removal needs only L-04. |
| **blocked-pending-owner** | Works outside production and has no CATI equivalent. Keeping, replacing or dropping it is an owner decision. |

## Evidence

**Callers.** The search covered:
- `frontends/` (user and admin);
- the user-backend and admin-backend proxies;
- `scripts/` and `backends/bot-backend/scripts/`;
- `deploy/` (nginx, staging shell);
- every `*.ps1`, `*.sh` and `*.bat` file;
- `.github/`;
- the runbooks (`docs/`, `README*`, `ROUTE_INVENTORY.md`, `ROUTES_USER_BACKEND.md`).

It used ripgrep plus plain `grep -r`, so gitignored files were read too. **No frontend, proxy, script, CI job or
deploy script makes an HTTP call to any route below.** Each user-backend proxy forwards only `/api/v1/...` paths,
and none of them is a catch-all.

**What does touch these routes:**
- the `PROCESS_ROLE=api` prefix guard (`app/ops/process_role.py:30`);
- nginx: `deploy/nginx/cosmicforge.conf:106-107` denies `/debug/` and `/runner/`;
- tests;
- historical reports.

**Look-alike routes.** Some frontend features have names close to these routes, but they use their own CATI-side
routers, not the legacy routes:

| Feature | Route | Served by |
|---|---|---|
| Emergency kill switch, flatten and status | `/api/v1/admin/emergency/*` | `app/api/emergency.py` |
| Family and account kill switches | `/api/v1/admin/governance/...` | `app/api/family_governance.py` |
| Circuit reset | `/api/v1/monitoring/admin/circuit-breaker/reset` | `app/api/monitoring.py` |

**Count.** The earlier documents speak of "21 legacy routes". The route families they name hold **33** routes
(table below). The 21 are the routes that call `get_runner()`, and the count did not include the routes that
always answer 410.

**Production behaviour.** `get_runner()` raises HTTP 410 in production (`settings.production`). So in production
every route that calls it answers 410 before any runner exists.

## E-05 in this pass: production ownership left the legacy runner

**Before.** `main.runner_loop` built `MultiBotRunner` in every trading process, and in production that object
only carried the lease loop. The readers listed below read `runner_service.multi_runner` and its attributes.

**Now.**
- In production, `main.runner_loop` builds the CATI-side `RuntimeOwnerHost`
  (`app/trading_intelligence/integration/runtime_owner.py`). It is the `RuntimeOwnerLoop` and nothing else.
- `runner_service.multi_runner` is `None` in production, and `app.runner.multi_runner` is imported only outside
  production.
- Outside production `MultiBotRunner` is still the host. It runs `PaperRunner` per bot and holds the same lease
  through its own `RuntimeOwnerLoop`.

**The readers now ask through `runtime_owner`:**
- `_may_run_background_jobs`;
- `runner_status` (`/runner/status`);
- `/health` (`components.runtime_owns_lease`);
- `api/bot_instances.get_effective_policy` (`runner_policy_hash`);
- `ops/runtime_supervisor.observe_runtime`;
- `ops/runtime_shutdown.quiesce`.

Each reader uses `host_of`, `ownership`, `stop_scheduler`, `release`, `simulation_runner(s)`,
`scheduler_running` and `iteration`, so none of them depends on which host is running.

**Unchanged:**
- the lease rules (`app/ops/runtime_ownership.py`);
- the response shapes;
- the shutdown order: stop entries, stop the scheduler, drain under the lease, release, close the session.

**Tests.** `tests/test_runtime_owner_readers.py` drives the real `runner_loop` against a real lease table:
- 8 characterization cases passed before the change and pass after it;
- 2 cases pin the change itself.

## Routes in `app/main.py` (all `_LEGACY_ADMIN_ONLY`)

| Route | Readers | Production today | Classification | Reason | Next step |
|---|---|---|---|---|---|
| `GET /runner/status` | No HTTP caller. Docs only: `README_ARCHITECTURE.md:130` (an old auth scheme), `backends/bot-backend/README.md:40` and old reports. 2 test files. | Serves the CATI runtime status (`engine: CATI`, `owns_runtime`). Since E-05 the lease state comes from `runtime_owner`. | **replace-with-CATI-equivalent (done)** | The production payload no longer needs the legacy runner. Outside production it still reports the cached simulation runners. | Keep the route. Optionally move it under `/api/v1/cati/...` once the owner retires the legacy prefix. |
| `POST /runner/live/start` | 2 test files | Answers `already_running` while the owner loop runs. Otherwise `get_runner()` answers 410. | **retain-as-compatibility** | Restarting the runtime is the service manager's job (supervisor exit, then a restart). `runner_loop` now builds the right host for each environment. | Retire with L-04. |
| `POST /runner/live/stop` | 1 test file | Cancels the owner task. Then `get_runner()` answers 410, but the task is already cancelled. | **retain-as-compatibility** | The supported stop is the stop file or a signal, followed by `quiesce`, which releases the lease; this route does not release it. | Retire with L-04. Runbooks must keep using `scripts/trading_runtime.ps1 stop` and the stop file. |
| `GET /runner/paper/once`, `POST /runner/live/once` | none | Always 410 | **retire-obsolete** | Hard 410 in every environment. | Remove with L-04. |
| `GET /runner/paper/state` | none | 410 | **blocked-pending-owner** | Paper-runner state; works only outside production. | Owner: drop it, or keep it for the simulation. |
| `GET /runner/audit/tail` | none (a docstring in `shared_lib/persistence/audit.py:15`) | Reads `logs/live_audit.jsonl` if present | **retire-obsolete** | V2 live-audit file; CATI evidence is in the database and the CATI read models. | Remove with L-04. |
| `POST /trade/close`, `/trade/close-record`, `/trade/close-record-usertrades` | none (1 test file calls the handlers to prove the 410) | Always 410 (each handler raises first) | **replace-with-CATI-equivalent** | Closing is served by `/api/v1/admin/emergency/flatten` (preemptive account lease) and by the CATI protection and reconciliation path. | Remove with L-04. `test_legacy_endpoint_containment.py` goes with them. |
| `GET /risk/daily`, `POST /risk/reset` | none | 410 | **blocked-pending-owner** | Daily loss state of the paper runner (simulation only). CATI daily loss is per-bot policy plus governance. | Owner: drop, or keep for the simulation. |
| `GET /risk/status` | none (1 test checks that the role guard lets GET through) | 410 | **blocked-pending-owner** | Same as `/risk/daily`. | Same. |
| `POST /risk/kill`, `/risk/unkill`, `/risk/reset_kill` | none (2 test files: role guard) | 410 | **replace-with-CATI-equivalent** | The supported kill switches are `/api/v1/admin/emergency/kill-switch` and the governance family and account switches, all called by the admin frontend. | Remove with L-04. |
| `POST /risk/circuit/reset`, `GET /risk/circuit/status` | none | Works: reads or resets the in-memory broker circuit registry | **replace-with-CATI-equivalent** | Only `PaperRunner` writes that registry, so in production it is empty. `/api/v1/monitoring/admin/circuit-breaker/reset` resets the same registry and the database state. | Remove with L-04. |
| `GET /strategy/signal` | none | 410 | **blocked-pending-owner** | V2 strategy signal of the paper runner; CATI decisions come from the CATI read models. | Owner: drop, or keep for the simulation. |
| `GET /logs/events/tail` | none (1 test file) | 410 | **blocked-pending-owner** | Tails audit events through the paper runner's `Audit`. There is no direct CATI route; the CATI read model and admin observability serve the decision history. | Owner: drop, or re-implement over `shared_lib.persistence.audit` without the runner (small; not done because nothing calls it). |
| `GET /debug/settings`, `/debug/config`, `/debug/sanity` | none (1 test file: auth of `/debug/config`) | Work: settings and sanity output with no runner | **retain-as-compatibility** | No runner dependency. nginx denies `/debug/`. | Retire with L-04. `/health` and `configuration_matrix` cover the supported checks. |
| `GET /debug/run/current` | none | Works (`run_manager`, `runner_service`) | **retain-as-compatibility** | No runner dependency. | Retire with L-04. |
| `POST /debug/crash-next-cycle` | none (2 test files: role guard) | Sets a flag that no loop reads any more | **retire-obsolete** | `runner_service.crash_next_cycle` has no reader. | Remove with L-04. |
| `POST /debug/run/cycle`, `/debug/run/force` | none (1 direct test + the role guard) | 410 | **retire-obsolete** | Each answers 410 before doing anything. | Remove with L-04. |
| `GET /debug/db/daily`, `/debug/db/symbols`, `/debug/position_amt/{symbol}`, `/debug/check-symbol`, `/debug/strategy/check`, `POST /debug/set_last_stop` | none | 410 | **blocked-pending-owner** | Paper-runner debugging, simulation only. | Owner: drop, or keep for the simulation. |

Summary of the 33 routes:
- replace-with-CATI-equivalent: 9 (one already done, `/runner/status`);
- retain-as-compatibility: 6;
- retire-obsolete: 6;
- blocked-pending-owner: 12.

None has a supported caller.

## Modules and scripts

| Item | Readers (non-test) | Classification | Reason | Next step |
|---|---|---|---|---|
| `app/runner/multi_runner.py` (`MultiBotRunner`) | `main.runner_loop` only, imported lazily and outside production. The six ownership readers no longer read it directly; they go through `runtime_owner`. 8 test files. | **retain-for-isolated-simulation** | It hosts `PaperRunner` per bot outside production, and through it the CATI cycle. The production lease no longer needs it (E-05). | Remove once the simulation has another home (see `runner.py`) and L-04 is granted. |
| `app/runner/runner.py` (`PaperRunner`, `get_runner`) | `main.py` (the type and the routes above), `multi_runner.py`, the reporting-only probe in `activation/cati.py:75-82`, `scripts/phase12/harness.py`, `debug_eval_logic.py`. 31 test files. | **retain-for-isolated-simulation** | It hosts the non-production CATI cycle (`cycle_shadow` at `runner.py:1976` and `:4427`, then `cati_dispatch`). This pass removed the dead IOFS code (E-04). | Move the cycle to a CATI-side simulation host. Then the owner decides on the routes, and the module can go. |
| `app/activation/cati.py` `_legacy_entry_refused` probe | engine-status route, admin `/cati`, market eligibility (reporting only) | **retain-as-compatibility** | It reports whether a scalar entry to the legacy runner is refused. It already reports "not missing" when `app.runner.runner` is absent (`find_spec`). | None needed: removing `runner.py` makes the probe pass on its own. |
| `app/trading_intelligence/integration/cycle_shadow.py`, `cati_dispatch.py` | `runner.py` (lazy) and the `activation/cati.py` probe. 16 and 2 test files. | **retain-for-isolated-simulation** | The only working non-production CATI simulation path. It must not be removed. | Re-host it off `PaperRunner`. `cati_dispatch.boundary_for` goes with it (Batch 8 note). |
| `app/runner/session_monitor.py` | `runner.py:492` (lazy), and `product_safety/daily_close_validation.py:11`, which backs the paper/testnet-only `POST /api/v1/monitoring/admin/bots/{id}/test/daily-close` | **retain-for-isolated-simulation** | Paper daily-close validation. Its only link to `runner.py` was a type-only import, now pointed at `app.runner.models` (this pass), so it no longer imports the legacy runner. | Keep while the paper daily-close validation is offered. |
| `scripts/phase12/harness.py` (+ `run_stage.py`) | Manual CLI only. Not in CI, no test, no runbook (historical `docs/phase_12_full_smoke_report.md`). `CATI_SOLE_RUNTIME_REFERENCE_INVENTORY.json` classes it DEAD_CODE. | **retire-obsolete** | It drives `PaperRunner` for the Phase 12 smoke, which CATI replaced. | Remove with L-04 (with `runner.py`). |
| `backends/bot-backend/debug_eval_logic.py` | none (ad-hoc, imports `PaperRunner`) | **retire-obsolete** | One-off debugging script at the backend root. | Remove with L-04. |
| Old simulation and research harnesses under `scripts/` | No script imports `PaperRunner`, `multi_runner`, `cycle_shadow` or `cati_dispatch` other than `phase12/harness.py`. `scripts/cati_simulation.py` uses `residual_simulation` (CATI). | n/a | Nothing else to classify. | none |
| `runner/market_snapshot.py`, `runner/effective_policy.py` shims | `main.py` (`normalize_execution_mode` through the shim), `master_ensemble.py`, tests | **retain-as-compatibility** | Alias shims from the earlier Batch 7 move. | Point `main.py` at `app.policy.effective_policy` in a later change; remove after M9 (`master_ensemble`). |

## Supported behaviour served only by the legacy runner

There is none in production:
- every legacy route either answers 410 there, has no runner dependency, or (`/runner/status`) is now served
  from the CATI side;
- the frontends use only the CATI-side emergency, governance and monitoring routers.

Outside production, the paper simulation, its debug routes and the paper daily-close validation remain. They are
classified retain-for-isolated-simulation or blocked-pending-owner above.
