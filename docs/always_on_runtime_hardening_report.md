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
| Suite | **2,096 passed, 0 failed, 0 skipped** |

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

`tests/test_always_on_runtime.py` — **42 added**, covering: single owner;
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
2096 passed, 0 failed, 0 skipped, 27 warnings
```

---

## Live paper proof

<!-- LIVE_PROOF -->

---

## Verdict

<!-- VERDICT -->
