# Runtime Activation and Process Ownership Fix

Branch `phase-0-4-runtime-baseline`, HEAD `a4d3106c`, 2026-09-10. All changes below are uncommitted in the working tree.

Scope was operational runtime and process management only. No strategy, threshold, risk, capital, entry-protection, PaperExecutor or replay code was touched.

---

## 1. Root cause

A second `python -m uvicorn app.main:app --port 9000` did not fail fast. It ran the **entire trading startup**: canonical runtime session, `run_id`, signal scheduler jobs, MultiBotRunner, bot restore. Only after all of that did it die on

```
[Errno 10048] only one usage of each socket address is normally permitted
```

That is uvicorn's own sequencing. `Server._serve()` calls `await self.startup(sockets)`, which runs `await self.lifespan.startup()` first and only afterwards reaches *"Standard case. Create a socket from a host/port pair."* Every FastAPI startup hook completes before the bind is attempted, so the port conflict can only be discovered after the damage is done.

The runtime ownership lease did stop the duplicate from *scheduling*: it failed to acquire and refused to run the loop. But by then it had already written evidence, and the duplicate's session row was never closed.

A second, independent defect made every stop leave evidence behind as well: the supervised launcher stopped the runtime with `Stop-Process -Force`, so the application's shutdown path never ran. The lease kept `released_at = NULL` against a dead PID, and the session stayed `RUNNING`.

## 2. Process topology before

Audited live before any change:

| | |
|---|---|
| Supervisor | `powershell 37204`, running `start_trading_runtime.ps1` |
| Launcher stub | `python 42184`, the venv `python.exe` (a redirector that re-execs the system interpreter) |
| Real server | `python 26780`, which held port 9000 and the lease |
| `runtime.pid` | **42184**, i.e. the stub, not the server |
| Lease | pid 26780, session `rts_67e95200586647e18bbe`, heartbeat fresh, `released_at` NULL |
| `runtime_sessions` | **48 of 48 rows marked RUNNING** against one live process. Duplicates were visible one minute apart. No session had ever been closed. |
| Identity | `database_role=development`, `environment_name=unknown`, revision `4fb218d5` |
| Bot | `bot_a8117dc719fc`, paper, WAITING_FOR_SIGNAL / NO_NEW_CANDLE, **0 open positions, 0 in-flight execution attempts** |

`environment_name=unknown` had a mundane cause: the code read `settings.ENVIRONMENT`, and no setting of that name existed.

## 3. Startup-order defect: fix

The decision is now made **at import of `app.main`**. uvicorn performs that import during `config.load()`, before `_serve` and therefore before lifespan startup. A refusal at that point costs nothing and writes nothing.

`app/ops/runtime_preflight.py` consults two independent authorities, and neither subsumes the other:

* **The lease**, scoped to the database. This is what actually guarantees a single scheduler.
* **The port**, checked with a real bind test (deliberately without `SO_REUSEADDR`) plus a psutil lookup of the listener.

| Case | Lease | Port | Result |
|---|---|---|---|
| A | none | free | `READY`: may start |
| B | live, same pid | held by owner | `ALREADY_RUNNING`: refused |
| C | dead pid or stale heartbeat | free | `STALE_LEASE`: may start; the existing lease logic takes over |
| C′ | dead or stale | held by someone | `OWNERSHIP_CONFLICT`: refused |
| D | none | held | `PORT_OCCUPIED_BY_OTHER_PROCESS`: refused (fail closed; no guessing) |
| E | live pid X | held by pid Y | `OWNERSHIP_CONFLICT`: refused |
| F | live | free (other port) | `ALREADY_RUNNING`: refused. Same database means one scheduler. |
| — | unreadable database | any | `ERROR`: refused |

The last row came from a bug found while writing the tests. `current_owner()` returns `None` on *any* error, which is correct when acquiring ("assume you are not the owner") but meant preflight read an unreadable database as "no lease held" and would have allowed a start. Preflight now probes the database first, so a broken database produces `ERROR` rather than a green light.

A refused start prints a structured message and exits with code 1:

```
[COSMICFORGE_RUNTIME]
The canonical trading runtime is already active.
  pid=16328
  port=9000
  port_pid=16328
  runtime_session_id=rts_eb1c2703dff84c629c39
  lease_status=HEALTHY
  ...
  No second scheduler was started.
  No MultiBotRunner was created.
  No runtime evidence was written.
```

Test imports (`COSMICFORGE_TEST_MODE`) and diagnostic tooling (`COSMICFORGE_SKIP_RUNTIME_PREFLIGHT`) are exempt. They are marked explicitly rather than inferred from the process name.

**The singleton protection was not weakened.** Preflight is an extra gate in front of the lease. The lease, its PID-liveness takeover and its renewal check inside the loop are unchanged.

## 4. Supervisor stop defect: fix

A graceful stop is now a request, not a kill:

1. The operator, via `trading_runtime.ps1 stop` or by hand, creates `backends/bot-backend/logs/runtime/STOP`.
2. The runtime polls for it every second, writes `STOPPED_BY_OPERATOR` (its exit reason), and runs `quiesce()`:
   stop new entries → stop the scheduler loop → let the current cycle finish (≤15 s) → release the lease → close the session.
3. It then raises SIGINT, so uvicorn runs its own shutdown and the lifespan shutdown hook. That hook also calls `quiesce()` first; it is idempotent.
4. The supervisor reads the exit reason **after the child has gone**, and does not restart.

**Positions are never flattened on stop.** A process stopping is not an exit signal, and closing on shutdown would destroy the restart-restorability that Phase 12 established.

Force termination still exists, but only after `-TimeoutSeconds`/`-GracefulStopSeconds`, and it always prints `FORCED_RUNTIME_TERMINATION`. When it happens, the next start recovers the stale lease through the existing PID-liveness takeover. The launcher also now records the real listener PID in `runtime.pid` instead of the redirector stub.

## 5. Defects found during live acceptance

These three were mine, introduced by the first version of this fix. None was caught by the unit tests; each was found by driving the real runtime. Each now has a regression test.

1. **The supervisor restarted a runtime the operator had just stopped.** The first watcher *deleted* the STOP file on sight. The runtime polls every 1 s and the supervisor every 2 s, so the supervisor usually never saw the request. It then saw an unexplained exit, treated it as a crash, and relaunched. Observed live: pid 16328 stopped cleanly, and pid 40372 appeared seconds later. **Fix:** the runtime leaves the STOP file alone and writes a durable exit reason (`STOPPED_BY_OPERATOR`) that the supervisor reads after the child exits. There is no polling race to win.

2. **Background jobs silently never registered.** `_when_owner()` called `_await_runtime_ownership()` without `await`. The coroutine was unpacked as a tuple, raised `TypeError` inside a task nobody inspected, and pid 25792 ran with no signal scheduler, no nightly dataset build, no retrain and no daily monitor. My unit test had called the helper directly, so it never exercised the caller. **Fix:** added the `await`, and added a test that runs the real startup hook end to end. I confirmed that this test fails with the `await` removed.

3. **The supervisor force-killed a healthy child.** A `STOPPED_BY_OPERATOR` marker from the previous stop was still on disk when the next supervisor launched. The supervisor took it as a shutdown in progress, waited out the 45 s grace period for an exit nobody had requested, and killed pid 25792 mid-cycle. That left an unreleased lease and session `rts_347b09a17cba4e368b7b` stuck at RUNNING. The supervisor ran in a hidden window, so this is inferred rather than read from its output. The evidence fits exactly: the marker existed before launch, the process died about 45 s in mid-cycle, and the supervisor exited without restarting, which only the forced-stop branch does. **Fix:** the supervisor and `trading_runtime.ps1 start` both clear stale STOP/marker files *before* launching a child. That session was later correctly marked ABANDONED by the reaper (§7).

Two smaller issues were found on the way:

* `trading_runtime.ps1 start` reported "up" as soon as the lease was taken, which happens before uvicorn binds. The operator then saw the lease-held-but-port-free message, which reads like a conflict. It now waits for both.
* `activate_manual_runtime.ps1` failed to parse. Windows PowerShell 5.1 decodes a BOM-less `.ps1` as cp1252, so an em dash in a comment became a smart quote, which PowerShell treats as a string delimiter. That script is now pure ASCII. `start_trading_runtime.ps1` (702 non-ASCII bytes of box-drawing characters, all in comments) now has a UTF-8 BOM, so the same thing cannot happen to it.

## 6. Background job singleton (§14)

Preflight refuses a duplicate at import, but that is not the whole story. A process can pass preflight against a stale lease and still lose the acquisition race inside the runner. Such a process may serve read-only APIs. It must not also generate signals or write monitor rows into a database whose scheduler belongs to someone else.

All four job families (signal generation and expiry, organic dataset nightly, ML monthly retrain, daily paper validation monitor) are registered by a single coroutine. It now runs only after `MultiBotRunner.owns_runtime` is true. The wait happens off the startup path, and a decided "no" returns immediately instead of waiting out the timeout. Live, from pid 39296:

```
[BACKGROUND_JOBS] BACKGROUND_JOBS_OWNER reason=RUNTIME_OWNER
[SIGNAL_SCHEDULER] SIGNAL_SCHEDULER_JOB_REGISTERED job_id=signal_gen_0700 ...   (x4)
[SIGNAL_SCHEDULER] SIGNAL_EXPIRY_JOB_REGISTERED job_id=signal_expiry interval=5min
[ORGANIC_DATASET_NIGHTLY] ORGANIC_DATASET_JOB_REGISTERED job_id=organic_dataset_nightly
[ML_MONTHLY_RETRAIN] ML_MONTHLY_RETRAIN_JOB_REGISTERED job_id=ml_monthly_retrain
[DAILY_PAPER_VALIDATION_MONITOR] JOB_REGISTERED job_id=daily_paper_validation_monitor
```

## 7. Runtime session evidence (§15)

* A refused start opens no session. Preflight runs before `_open_runtime_session_once`.
* Sessions whose process is gone are marked **ABANDONED** at the next startup, with `shutdown_reason=PROCESS_GONE_NO_CLEAN_SHUTDOWN`. `stopped_at` is **left NULL**: we know the process is gone but not when it went, and stamping "now" would invent a stop time. ABANDONED and STOPPED are different facts and are recorded as different facts. No row is deleted. If a PID has been reused by an unrelated process, the row is left RUNNING: the reaper under-reports rather than fabricates.

| | before | after |
|---|---|---|
| RUNNING | 48 | 1 (the live runtime) |
| ABANDONED | 0 | 49 (48 historical + the one from defect 3) |
| STOPPED | 0 | 7 (every clean stop during acceptance) |
| total rows | 48 | 57 |

## 8. Database identity (§16, §17)

`DATABASE_ROLE` and `environment_name` are pure labels written on evidence rows; nothing branches on them. I verified this by reading every consumer.

* `backends/bot-backend/.env` now declares `DATABASE_ROLE=paper` and `ENVIRONMENT_NAME=paper_forward_local`. **`DATABASE_URL` is unchanged.** This relabels the existing database; it does not switch to a different one. The code default stays `development`, so a checkout without `.env` is labelled conservatively. Rows written before the change keep `database_role=development`, which is their true lineage, and are left alone. `.env` is not tracked by git, so this change lives only on this machine.
* A new `ENVIRONMENT_NAME` setting replaces the never-defined `ENVIRONMENT`, which is why every earlier session recorded `unknown`.
* Database candidates beside the active file, registered and labelled but **never deleted and never selected**:

| Label | File | Size |
|---|---|---|
| ACTIVE | `cosmicforge.db` | 3.51 GB |
| FORENSIC | `cosmicforge-LAPTOP-5B3QOQDJ.db` (OneDrive sync conflict) | 3.24 GB |
| BACKUP | `cosmicforge.pre_capital_recovery.20260907T203236Z.db` | 3.38 GB |
| VALIDATION | `phase12_paper_validation.db` | 0.002 GB |

The Phase 12 database had been labelled STALE_COPY, which suggests a leftover and is the opposite of what it is. A `VALIDATION` classification was added for it.

## 9. Changes

New:

| File | Purpose |
|---|---|
| `backends/bot-backend/app/ops/runtime_preflight.py` | the import-time decision |
| `backends/bot-backend/app/ops/runtime_shutdown.py` | `quiesce()`, STOP file, exit-reason marker |
| `scripts/trading_runtime.ps1` | operator CLI: `status`, `start` (`-Mode manual/supervised`), `stop`, `restart` |
| `scripts/runtime_status.py` | read-only JSON status probe used by the scripts |
| `scripts/activate_manual_runtime.ps1` | dot-source to enter the venv and see the runtime state; starts nothing |
| `backends/bot-backend/tests/test_runtime_activation_and_ownership.py` | 59 tests |

Modified:

| File | Change |
|---|---|
| `app/main.py` | preflight at import; `quiesce()` first in the shutdown hook; stop-file watcher; ownership-gated background jobs; `ENVIRONMENT_NAME`; abandoned-session reaping |
| `app/ops/runtime_ownership.py` | public `pid_is_alive`, `lease_is_stale`, so preflight shares the lease's own policy |
| `app/runner/multi_runner.py` | read-only `owns_runtime` and `ownership_decided` properties |
| `app/evidence/writers.py` | `reap_abandoned_sessions` |
| `app/ops/database_registry.py` | `VALIDATION` label |
| `app/core/config.py` | `ENVIRONMENT_NAME` setting |
| `scripts/start_trading_runtime.ps1` | graceful-first stop, exit-reason check before the restart decision, stale-request clearing, real listener PID, UTF-8 BOM |
| `tests/test_strategy_clock_and_snapshot.py` | §19 fix, below |
| `.env` (untracked) | `DATABASE_ROLE=paper`, `ENVIRONMENT_NAME=paper_forward_local` |

**§19, `test_run_once_has_no_unique_business_logic`**, fixed without skipping. Every test in that file used `inspect.getsource(PaperRunner.<method>)`, which resolves through the *current* attribute value. Once any other test in the suite patched a method, these assertions read someone else's lambda: green alone, red in-suite. They now parse `app/runner/runner.py` with `ast` and take the method's source segment from the file. A new test patches `PaperRunner.run_once` and shows the assertion still reads the real code.

**Working tree at the start of this task.** Another session had uncommitted edits to `trading_orchestrator.py`, `runner.py`, `hold_breakdown.py`, `master_ensemble.py` and an untracked `tests/test_session_gate_consistency.py`. I did not touch them. Their owner later committed them in `8622fc73` and `a4d3106c`. I have committed nothing.

## 10. Tests

* `tests/test_runtime_activation_and_ownership.py`: **59 passed**. Covers the case matrix A–F plus stale heartbeat and unreadable database; "preflight writes nothing"; the refusal text; the skip flags; the bind primitive; quiesce (lease released, session closed, idempotent, never flattens, clean with no runtime); import-order wiring; background-job gating (owner, lost lease, no runner, pending, resolves on acquisition, fast refusal, end-to-end hook); session reaping (ABANDONED, no invented `stopped_at`, ABANDONED ≠ STOPPED, live and current never reaped, rows preserved); database labels and no-delete/no-switch; the exit-reason marker; and script ordering for the three live defects.
* `tests/test_strategy_clock_and_snapshot.py`: **35 passed**, including the new order-independence test.
* Existing scheduler tests (`test_section_c_signal_scheduler`, `test_nightly_dataset_scheduler`, `test_ml_retrain_pipeline`, `test_daily_paper_validation_monitor`): **73 passed**, unchanged by the gating.
* Full repository suite: see the acceptance block.

## 11. Live activation proof

**A. Recorded state.** See §2.

**B/C. First stop.** Pid 26780 was running pre-fix code (revision `4fb218d5`) with no stop-file watcher. The graceful request timed out after 30 s, and the CLI escalated and said so: `FORCED_RUNTIME_TERMINATION pid=26780`. The port was released. The lease was left stale but recoverable, and the next start took it over. **This is not a graceful-stop pass. It shows that escalation is visible, bounded and recoverable.**

**E. Manual `python -m uvicorn` start**, run by hand with no supervisor: pid 43100, session `rts_6892c8d5ecfd47c78837`, bound to port 9000, one lease, one session RUNNING, HEALTHY.

**D. Activation helper** (`. .\scripts\activate_manual_runtime.ps1`) against a live runtime:

```
[activate] a runtime is ALREADY ACTIVE
[activate]   pid=31892 session=rts_42417069b5794bf0b976
[activate]   starting another uvicorn here will be refused by preflight.
```

The user independently started a manual runtime from a VS Code terminal at 19:49 local (pid 46412, session `rts_5548a9cb6dee44579790`). It came up cleanly, recording `role=paper` and `env=paper_forward_local`. `trading_runtime.ps1 start` then correctly refused to start a second one. That runtime is still running.

## 12. Duplicate-start proof

**F.** The same command run in a second terminal while the canonical runtime (pid 16328) was serving:

```
sessions before duplicate start : 49
[COSMICFORGE_RUNTIME]
The canonical trading runtime is already active.
  pid=16328  port=9000  port_pid=16328  runtime_session_id=rts_eb1c2703dff84c629c39
  lease_status=HEALTHY ...
  No second scheduler was started.
  No MultiBotRunner was created.
  No runtime evidence was written.
EXIT CODE = 1
sessions after duplicate start  : 49
```

There was no Errno 10048, because the refused process never reached the bind. The only output before the refusal was the global exception handler installing itself.

**Same database, different port**, against the user's live runtime (pid 46412):

* `uvicorn --port 9001`: refused, exit 1.
* `COSMICFORGE_RUNTIME_PORT=9001 uvicorn --port 9001`: refused, exit 1, on the lease alone (port 9001 was free). This is case F.
* Sessions went 57 → 57 across both attempts; port 9001 never bound.

## 13. Graceful stop and restart proof

**G. Supervised graceful stop**, with the fixed supervisor, against pid 39296:

```
[trading_runtime] graceful stop complete (lease released, port 9000 free)
graceful stop elapsed = 5.2s
port 9000 listener after 60s: none            <- supervisor did NOT restart it
[RUNTIME_SHUTDOWN] clean=True reason=OPERATOR_STOP_FILE lease_released=True session_closed=True
```

Database afterwards: session `rts_ce8ec32bdbe049969a1a` STOPPED, `stopped_at=2026-09-10T04:45:41.827842Z`, reason `OPERATOR_STOP_FILE`. Lease `released_at=2026-09-10T04:45:41.758177Z`, `release_reason=OPERATOR_STOP_FILE`. Open positions 0, in-flight attempts 0.

**Manual graceful stop** (pid 43100, no supervisor): `clean=True lease_released=True session_closed=True`, followed by uvicorn's own `Application shutdown complete.` and `SIGNAL_SCHEDULER_STOPPED`.

**H. Restart.** `trading_runtime.ps1 restart` did a graceful stop of pid 39932 and brought up a clean start as pid 31892 in 18.3 s, reporting "The canonical trading runtime is already active." There was no stale-owner problem. The starts that followed a forced termination (pids 16328 and 39296) took over the dead lease through PID-liveness, and the reaper marked the orphaned session ABANDONED (`abandoned_sessions_marked=1`).

## 14. Known limitations

* **Preflight checks `COSMICFORGE_RUNTIME_PORT` (default 9000), not uvicorn's `--port`.** A duplicate is refused either way, because the lease is authoritative (proven in §12). The only wrong answer is a fail-closed refusal if an unrelated process holds 9000 while an operator deliberately serves on another port. Set `COSMICFORGE_RUNTIME_PORT` in that case.
* **`OPEN_POSITION_RESTART_SAFE` is proven by tests, not live.** `quiesce()` never closes positions (tested), and position restoration was proven by tests in Phase 12. The real bot was flat throughout, and forcing it to trade is prohibited.
* **The changes are uncommitted.** Running sessions report `code_revision=a4d3106c` while executing working-tree code. Commit before the long readiness run so evidence maps to a real revision.
* **Host memory is the binding constraint.** 0.7 GB of 15.8 GB was free during the full suite, which is the likely cause of the earlier suite deaths around 66%. This is an environment issue, not something the application can solve.
* The `bot_a8117dc719fc` capital configuration blocker from Brief 2 is untouched, as instructed.

---

```
MANUAL_UVICORN_START:
PASS

SUPERVISED_START:
PASS

DUPLICATE_START_REFUSED_EARLY:
PASS

DUPLICATE_START_CREATED_RUNNER:
NO

DUPLICATE_START_CREATED_SCHEDULER:
NO

DUPLICATE_START_CREATED_RUNTIME_SESSION:
NO

PORT_9000_SINGLE_OWNER:
PASS

DATABASE_RUNTIME_LEASE:
PASS

SAME_DB_DIFFERENT_PORT_REJECTED:
PASS

GRACEFUL_STOP:
PASS

LEASE_RELEASED_ON_NORMAL_STOP:
PASS

PORT_RELEASED_ON_NORMAL_STOP:
PASS

OPEN_POSITION_RESTART_SAFE:
PASS (test-proven; not exercised live, the bot was flat throughout)

FORCE_KILL_NORMAL_PATH:
NO

DATABASE_ROLE:
paper (declared in .env; same DATABASE_URL; pre-change rows keep "development")

ENVIRONMENT_NAME:
paper_forward_local

FULL_TEST_SUITE:
2474 passed, 0 failed, 0 skipped, 27 warnings, 4 subtests passed in 1005.18s (0:16:45)

LIVE_MANUAL_ACTIVATION:
PASS

LIVE_DUPLICATE_ACTIVATION_TEST:
PASS

LIVE_STOP_RESTART:
PASS

TRADING_LOGIC_CHANGED:
NO

SAFE_FOR_CONTINUED_PAPER:
YES

SAFE_FOR_MAINNET:
NO
```
