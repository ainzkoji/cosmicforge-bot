# CosmicForge — Always-On Runtime Hardening

Operational hardening of the trading runtime, driven by a real production
failure rather than a hypothetical one.

| Item | Value |
| --- | --- |
| Repository | `C:\Users\favou\OneDrive\Desktop\cosmicforge-bot` |
| Backend | `backends\bot-backend` (port 9000) |
| Interpreter | `backends\venv\Scripts\python.exe` |
| Database | `backends\shared\shared_lib\persistence\cosmicforge.db` (3.41 GB, role `development`) |
| Active bot | `bot_a8117dc719fc` — paper / demo, master_ensemble, BTCUSDT+ETHUSDT, 15m |
| Suite | **2,100 passed, 0 failed, 0 skipped** |

`bot_e5fe913972a9` was not touched. It remains deleted by operator intent.

---

## The failure this fixes

The runtime looked healthy for twelve hours while doing nothing.

```
cycle heartbeat gap: 10:21:47 -> 16:21:48 UTC   = 21600s (exactly 6.00 h)
runtime sessions in that window: 1   (no crash, no restart)
bot health throughout:  WAITING_FOR_SIGNAL / NO_NEW_CANDLE
candle slots in span:   48      evaluated: 25      coverage: 52.1%
```

The host suspended. The process did not crash — it froze, resumed, and carried
on. Nothing recorded the gap, and `status=active` plus `NO_NEW_CANDLE` is
exactly what a genuinely quiet market looks like.

**Attribution, measured precisely:**

| Window | Expected | Evaluated | Coverage |
| --- | --- | --- | --- |
| Last 24 h (includes time before canonical evidence existed) | 95 | 26 | 27.4% |
| Since evidence wiring went live (04:30 UTC) | 49 | 26 | 53.1% |
| Inside the 6 h host suspension | 24 | 1 | 4.2% |

**All 23 missed candles since 04:30 fall inside the suspension.** Outside it,
coverage was **100%** — the software never missed a candle while it was
actually running. The defect was observability, not evaluation.

---

## §1/§2 — One canonical backend, one runner

The port-8000 `--reload` development backend was terminated. Only port 9000
remains.

Port numbers protect nothing: two backends on different ports pointed at the
same runtime database would both start a `MultiBotRunner` and both drive the
same bots. The lease is therefore held against the **database**.

`app/ops/runtime_ownership.py` persists `runtime_owner_id`, `runtime_session_id`,
`pid`, `hostname`, `started_at`, `heartbeat_at`, `database_path`,
`database_role`, plus release metadata.

* One holder per `(lease_name, database_path)`.
* Renewed every scheduler tick; losing it stops the scheduler immediately.
* A stale heartbeat (>90 s) **or** a dead PID can be taken over, so a crash
  self-recovers without manual intervention.
* Fails closed: any error means "not the owner".

`MultiBotRunner` acquires the lease before its loop and refuses to schedule
without it. A process that cannot acquire it may still serve read-only APIs.

**Live proof — a duplicate on a *different* port, which no port check could catch:**

```
[start_trading_runtime] database : …\cosmicforge.db [role=development, 3.18 GB]
[start_trading_runtime] FATAL: runtime trading lease is held by a live process
  (pid=48688 host=LAPTOP-5B3QOQDJ, heartbeat=2026-09-08T17:01:21.750026+00:00).
  Refusing to start a duplicate canonical runner.
```

---

## §3 — Always-on launcher

`scripts/start_trading_runtime.ps1` resolves canonical paths, verifies the
database, checks the port and the lease, refuses duplicates, launches uvicorn
**without `--reload`**, forces UTF-8, writes timestamped logs to
`backends/bot-backend/logs/runtime/`, records the PID, and supervises with
bounded backoff **5 → 10 → 30 → 60 s** (capped; never a rapid restart loop).

Operator shutdown is distinguished from a crash: a clean exit or the `STOP`
file ends supervision, a non-zero exit restarts.

`scripts/runtime_probe.py` holds the database and lease probes. They began
inline in the script and **silently failed** — Windows PowerShell 5.1 mangles
multi-line here-strings passed to `python -c`, and the probe additionally
resolved the legacy `data/bot.db` because `find_dotenv()` walked up from
`scripts/` rather than loading the backend `.env`. Both are fixed and pinned by
tests.

---

## §4–§6 — Watchdogs and honest health

`app/ops/runtime_watchdog.py` tracks the scheduler heartbeat, per-symbol market
data, and the strategy clock, then derives a verdict.

The strategy-clock rule:

```
latest_available_closed_candle  ==  last_evaluated   -> NO_NEW_CANDLE (healthy)
latest  >  evaluated, 1-2 iterations                 -> catching up (healthy)
latest  >  evaluated, >= 3 iterations                -> STRATEGY_CLOCK_STALLED
```

New canonical reasons: `STRATEGY_CLOCK_STALLED`, `MARKET_DATA_STALE`,
`MARKET_DATA_UNAVAILABLE`, `RUNNER_HEARTBEAT_STALE`,
`RUNNER_INITIALIZATION_FAILED`, `CANONICAL_EVIDENCE_WRITE_FAILED`,
`RUNTIME_OWNERSHIP_LOST`.

**§12 semantics.** `status=active` means *configured to run*. Health separately
answers *is it actually running?* A six-hour heartbeat gap now yields
`ERROR / RUNNER_HEARTBEAT_STALE`, never `WAITING_FOR_SIGNAL`.

---

## §7 — Safe recovery

A stalled clock evicts and rebuilds that bot's `PaperRunner`. Positions, TP1
state, remaining quantity, break-even, trailing, protection state, policy hash
and the last evaluated candle all live in persisted state, so the replacement
rehydrates them. A test asserts the recovery path contains no
`close_position`, `flatten` or `activate_kill_switch` — **a rebuild is never an
exit signal.** Only policy-defined kill-switch behaviour may flatten.

### Recovery is tiered, and it stops

The first implementation rebuilt on every detected stall, with no ceiling. That
is visible in the logs of the 19:17 process:

```
$ grep -c "STRATEGY_CLOCK_STALLED - evicting runner for rebuild" \
        logs/runtime/runtime-20260908-191701.log.err
11
```

Eleven rebuilds inside one short-lived process, each reloading 735 instrument
specs. `seed_evaluated_candle()` removed *that* trigger; tiering removes the
loop itself, so a stall arriving from any other cause cannot become a rebuild
storm.

| Tier | Action | Cost |
| --- | --- | --- |
| 1 `SOFT_REFRESH` | reload the candle marker from `bot_candle_evaluations`, clear a latched market-data error, re-read instrument specs | in place, no rebuild |
| 2 `RUNNER_REBUILD` | evict; the replacement rehydrates from persisted state | one runner construction |
| 3 `RUNNER_RECOVERY_FAILED` | escalate once, with an audit event and an operator action | stops retrying |

An advancing clock clears the escalation — `candle_evaluated()` resets the tier
counter — so a bot that recovers does not stay latched in `ERROR`.

Two deliberate details:

* `clear_market_data_error()` does **not** touch `last_market_data_at`.
  `MARKET_DATA_STALE` is measured from that timestamp; refreshing it during
  recovery would let the runtime hide a feed that has stopped producing.
* `sync_evaluated_marker()` is not `seed_evaluated_candle()`. Seeding fills an
  *unknown* marker without claiming an evaluation. The resync is a deliberate
  tier-1 correction of a marker that has *drifted*, and does reset the stall
  counter so the refresh can be judged on its own.

`RUNNER_RECOVERY_FAILED` joins the §6 taxonomy and outranks the stall that
caused it: reporting `STRATEGY_CLOCK_STALLED` after recovery is spent would
suggest the runtime is still trying to fix itself.

---

## §9 — Market data vs. no new candle

A `klines` fetch failure previously fell through into the candle gate and
surfaced as `NO_NEW_CANDLE`, making a broken feed indistinguishable from a
quiet market. It now returns `MARKET_DATA_UNAVAILABLE` with the provider error
class recorded (no secrets), and the watchdog reports `ERROR`.

---

## §11 — Heartbeat storage

Before: every 10-second tick wrote a full `TradingDecision` row — **~17,000
rows/day** for two symbols.

After: a `NO_NEW_CANDLE` tick is cycle evidence, tallied per cycle in
`trading_cycles`. Only real candle evaluations become decisions.

**Measured live after restart:**

```
heartbeats tallied in cycles : 20
heartbeat decision rows      : 0
real evaluations             : 2   -> 2 decision rows
```

The earlier heartbeat-overwrite regression tests are preserved: a heartbeat
still cannot overwrite a real evaluation for the same candle.

---

## §13/§14 — Status and operational health

`/runner/status` gains `runtime_health`, `runtime_health_reason`,
`runner_initialization_status`, `scheduler_heartbeat_at` (+age), `last_cycle_at`,
`last_execution_attempt_at`, `runtime_session_id`, `run_id`, `policy_hash`, and
per-symbol clocks with `latest_closed_candle_at`,
`last_evaluated_closed_candle_at`, `evaluation_lag_seconds`, `is_behind`,
`last_market_data_at`, `last_strategy_reason`.

New endpoints:

| Route | Purpose |
| --- | --- |
| `GET /api/v1/admin/trading/operations/health` | process, database, ownership, scheduler, bots, market data, strategy clock, evidence, positions, execution → `HEALTHY`/`DEGRADED`/`ERROR` |
| `.../operations/coverage/{bot}` | expected closed candles vs actual evaluations |
| `.../operations/metrics/{bot}` | the §18 integrity metrics |

**Live:**

```
OVERALL: HEALTHY
 database  : development True 3.41 GB
 ownership : held pid 48688 this_process True hb_age 8.8
 scheduler : HEALTHY hb_age 8.3 iters 10
 marketdata: HEALTHY | strategy_clock: HEALTHY
 evidence  : {'decisions_last_hour': 402, 'cycles_last_hour': 217, 'incomplete_decisions': 0}
 bot       : bot_a8117dc719fc | stored: WAITING_FOR_SIGNAL NO_NEW_CANDLE | runtime: HEALTHY None
```

---

## §17 — Active-bot capital: two pathologies

**Nothing was changed.** `capital_allocation = 120`, `allocation_value = 120`.
Resolved under the current risk model:

```
capital_budget        : 120.0        position_allocation : fixed_amount 120.0
trade_usdt_per_order  : 120.0        max_open_positions  : 2
risk_per_trade        : 0.25%        risk per trade      : 0.30 USDT
max_daily_loss        : 6.00 USDT    minimum_notional    : 5.00 USDT
```

**1. 200% over-commit.** Two position slots × 120 USDT = **240 USDT against a
120 USDT budget**. If BTC and ETH both fill, the bot commits twice its stated
capital. Total capital equalling per-position allocation is only coherent with
`max_open_positions = 1`.

**2. Risk-based sizing cannot express itself.** 0.25% of 120 is **0.30 USDT** of
risk per trade, against a 5.00 USDT exchange minimum notional — roughly 17×
below the floor. ATR-risk sizing can never produce a tradeable position, so
every entry falls back to the fixed 120 USDT allocation: **400× the intended
risk budget.**

Both are configuration consequences, not code defects. Resolving them is an
operator decision; no value was invented.

---

## Tests

`tests/test_always_on_runtime.py` — **46 added**, covering: single owner;
duplicate initialization denied; lease keyed on database not port; stale-lease
takeover; renewal failure after takeover; heartbeat freshness; the six-hour
stale case; strategy-clock advancement; `NO_NEW_CANDLE` between boundaries;
stall detection and clearing; independent per-symbol clocks; recovery without
flattening; market-data failure ≠ `NO_NEW_CANDLE`; stale vs unavailable;
coverage and gap detection; heartbeats excluded from coverage; bounded
heartbeat storage; honest health semantics; status freshness fields; and five
launcher-contract tests.

Two earlier tests asserted the old behaviour where a heartbeat became a decision
row; they now assert the new contract. One asserted launcher text that moved
into `runtime_probe.py`; it now checks the probe call and the probe itself. No
test was deleted or skipped.

```
2100 passed, 0 failed, 0 skipped, 27 warnings
```

---

## Live paper proof

Launched with `scripts/start_trading_runtime.ps1`. Port-8000 terminated
beforehand; only canonical port 9000 ran throughout.

### Three genuine 15m boundaries, spanning a controlled restart

```
16:59 BTCUSDT  NO_OPPORTUNITY       regime=WEAK_TREND           ms=ms_d9e73844 run=4670b998
16:59 ETHUSDT  NO_OPPORTUNITY       regime=WEAK_TREND           ms=ms_31a8278f run=4670b998
17:14 BTCUSDT  NO_OPPORTUNITY       regime=WEAK_TREND           ms=ms_eae6d730 run=4670b998
17:14 ETHUSDT  NO_OPPORTUNITY       regime=WEAK_TREND           ms=ms_bf98f616 run=4670b998
        --- controlled restart (operator STOP file, supervised relaunch) ---
17:29 BTCUSDT  REGIME_LOW_VOL_CHOP  regime=LOW_VOLATILITY_CHOP  ms=ms_9558f241 run=5b49bc41
17:29 ETHUSDT  NO_OPPORTUNITY       regime=WEAK_TREND           ms=ms_1dd23159 run=5b49bc41
```

Each symbol evaluated **exactly once** per candle, each with its own market
snapshot. The 17:29 boundary was evaluated by a different `run_id` — the
restart did not cost a candle.

```
duplicate candle decisions : 0    PASS
heartbeat decision rows    : 0    PASS   (§11 bounded storage)
incomplete decisions       : 0    PASS
execution attempts         : 0           (no trade forced — correct)
cycles / heartbeats / evals: 177 / 348 / 6
coverage since launch      : BTCUSDT 2/2, ETHUSDT 2/2 = 100%
```

348 heartbeats produced **zero** decision rows; the 6 real evaluations produced
exactly 6. Under the old scheme those 348 ticks would each have been a row.

### Ownership

```
{"held": true, "pid": 34420, "hostname": "LAPTOP-5B3QOQDJ",
 "heartbeat_at": "2026-09-08T17:17:50Z",
 "runtime_session_id": "rts_cfefbae5fde64aeabed0",
 "stale": false, "pid_alive": true}
```

Exactly one lease. A duplicate on port 9100 against the same database was
refused (quoted in §1/§2 above) — the case a port check cannot catch.

### Controlled restart

```
pid before : 29396
STOP file   -> stopped by operator   (no crash-restart: correct)
pid after  : 37516
health after restart: HEALTHY  BTCUSDT/ETHUSDT behind=False
```

### A defect the restart exposed

The first supervised restart reported `ERROR / STRATEGY_CLOCK_STALLED` for a bot
that was perfectly up to date. A fresh process starts with an empty in-memory
clock while the candle marker is persisted, so `claim_candle` correctly declined
the already-evaluated 17:14 candle and the new process never reported an
evaluation — leaving `last_evaluated = None` and the clock looking permanently
behind.

`seed_evaluated_candle()` now rehydrates the marker on the not-claimed path. It
deliberately does **not** reset the stall counter: nothing was evaluated, we
merely learned what the previous process had done, so a genuinely stale marker
still surfaces as a stall. Four regression tests. Verified live: the next
restart reported `HEALTHY` with `eval=17:14:59, behind=False`.



---

## Verdict

```
SINGLE_CANONICAL_BACKEND:        PASS
RUNTIME_OWNERSHIP:               PASS
MULTIBOTRUNNER_SINGLETON:        PASS
RUNNER_HEARTBEAT:                PASS
MARKET_DATA_HEALTH:              PASS
STRATEGY_CLOCK:                  PASS
CANDLE_EVALUATION_COVERAGE:      PASS
STALL_DETECTION:                 PASS
SAFE_RUNNER_RECOVERY:            PASS
PROCESS_RESTART_RECOVERY:        PASS
POSITION_RESTORE:                PASS
CANONICAL_EVIDENCE:              PASS
HEARTBEAT_OVERWRITE_PROTECTION:  PASS
DUPLICATE_ENTRY_PROTECTION:      PASS
FULL_TEST_SUITE:                 PASS   (2100 passed, 0 failed, 0 skipped)
LIVE_TWO_CANDLE_PROOF:           PASS   (3 boundaries, across a restart)

BOT_OPERATIONALLY_ALWAYS_ON:     YES
```

**YES is claimed on live runtime evidence**, not unit tests: three genuine 15m
boundaries with exactly one evaluation per symbol per candle, 100% coverage
since launch, zero duplicates, one ownership lease, and a controlled restart
that cost no candle.

### Caveats the operator should know

1. **`POSITION_RESTORE` is proven by test, not by a live open position.** The
   bot held no position during the window (correctly — it found no opportunity).
   Restoration is covered by the Phase 12 suite and by the design (state is
   persisted, rebuild rehydrates), but has not been observed live with a real
   open paper position.
2. **The supervisor cannot prevent host suspension.** The original six-hour gap
   was the machine sleeping, which no user-space process can stop. What is now
   guaranteed is that it becomes *visible*: `RUNNER_HEARTBEAT_STALE`, a coverage
   gap, and an honest `ERROR` health state. If continuous operation matters,
   disable sleep/hibernate on this host — that is an OS setting, not a code fix.
3. **The §17 capital configuration is pathological** (200% over-commit; risk
   sizing 17× below minimum notional). Unchanged, as instructed. It has caused
   no incident only because no trade has been approved.
4. **`DATABASE_ROLE` is `development`** while this is the live paper runtime.
   Setting it to `paper` would make the role reporting honest.

