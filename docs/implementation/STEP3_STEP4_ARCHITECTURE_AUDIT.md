# Section H, Steps 3 and 4: architecture and dependency audit (Phase Zero)

9 October 2026. Read-only audit of the repository at `75435cd` (the pushed Step 2 closure, remote CI run
37960746421: success), made before any Step 3 or Step 4 code was written. File and line references are to that
commit; paths without a prefix are under `backends/bot-backend/app`. `TI` stands for `trading_intelligence`.

The CosmicForge Master Plan A to Z is not in the repository. This audit and everything built on it rest on the
Step 3 and 4 assignment of 9 October 2026, the Step 2 handoff (`docs/research/mandate_004/STEP3_HANDOFF.md`),
the frozen specification and the code. Where the master plan would have to supply a number or a decision, the
tracker says so.

## 1. What actually runs in production

```
main.py:920        production_runtime.run(db)                 one worker thread, 30 s loop
  ├─ residual_schedule → residual_prospective.collect         one hourly decision row for the residual family
  └─ broker_cycle(db)                                         accounts one after another
       sync → sync_account → production_execution.process_account
         latest_decision → maintain_account → account_risk → boundary_for
         → eligibility → reservations.reserve → build_plan → prepare_submission
         → CATIExecutionBoundary.process_trade_plan → TradingOrchestrator → BinanceExecutionAdapter → executor
```

- `MultiBotRunner.run_once` returns immediately in production (`runner/multi_runner.py:507-510`). The legacy
  strategy runner is dead in production and nothing new depends on it.
- `CATIController` (the forecast pipeline of the four setup specialists) never runs in production.
- The whole chain was written for one family by name: `RESIDUAL_MOMENTUM_PORTFOLIO_TOP1`.

## 2. Findings that shaped the work

### 2.1 A demo order was possible for a family that failed research (closed in Step 3.3)

The residual family failed the Mandate 003 economic gate (hypothesis 15). At governance phase M0 it could
still place demo orders: production injected `AccountExecutionAuthority`, whose `authorize_entry` answered
`ACCOUNT_AUTHORIZED_DEMO_EXECUTION` for a demo account before the phase check
(`TI/governance/account_authority.py:41-46`); `plan.mode` was never read by anything; consent defaults to
enabled for demo. The only source check in the boundary was "is this the frozen residual family"
(`TI/execution/boundary.py:692-710`). Generalising that check without a certification gate would have opened
demo to every family. The first thing built was therefore the registry-backed gate at the final boundary.

A Step 1 test pinned the old behaviour (`test_production_final_audit.test_m0_demo_uses_actual_account_authority`).

### 2.2 The decision-source check was a name comparison

`_production_plan_reason` compared `plan.setup_family` with a constant and then read the residual family's own
decision table. No registry, no certification status, no rule hash of the code, no stage, no kill switch per
family, and nothing at all outside production settings.

### 2.3 Governance could not be operated

`PromotionGovernance.transition` has no caller outside tests. There is no pending state (a request is an
approval), one global phase for all of CATI, no per-family stage, no reviewer identity, no revocation of a
certification, no family or account kill switch route (the global kill switch has one), and no administrative
page. Phases M0 to M9 and their evidence keys are defined in `TI/governance/phases.py` and were kept as they are.

### 2.4 One position, enforced in four places

`max_open_positions=1` is hard-coded (`production_execution.py:438,463,953`); any open or pending row refuses
a reservation (`TI/portfolio/reservation_store.py:238-246`); `ACCOUNT_WIDE_POSITION_OR_ORDER_ACTIVE`
(`production_execution.py:291-294`); more than one active bot on an account is `ACCOUNT_EXECUTION_OWNER_AMBIGUOUS`.
Open risk is not enforced (`open_risk_usdt` defaults to 0), there is no aggregate leverage cap, and
`drawdown_reduce_pct` / `max_drawdown_pct` are emitted by `core/risk_profile_params.py:27-28` and read by nothing.

### 2.5 Five reservation stores

| Store | Kind | Role today |
|---|---|---|
| `cati_portfolio_reservations` (`TI/portfolio/reservation_store.py`) | durable table | authoritative for the one-position path |
| `pending_entries` (`execution/entry_protection.py:66-93`) | durable table | executor-level entry guard, `UNIQUE(bot_id, symbol, side)` |
| `position_slots` (`execution/position_slots.py`) | derived view | not a source |
| `AccountMarginReservations` (`risk/capital_ledger.py:425-559`) | process memory | live, lost on restart |
| `AdaptiveDailyRiskBudgetEngine._reservations` | process memory | inert |

### 2.6 Risk brakes: one of four existed

Daily loss pause exists (`cati_production_daily_risk`, day in `ADAPTIVE_DAILY_RISK_TIMEZONE`). The drawdown
brake that halves targets, the drawdown brake that closes and stops, a persisted terminal state, an all-time
high-water mark and an explicit restart did not exist.

### 2.7 Stop modification is cancel-then-create

The only code that moves a stop is `binance_adapter.modify_protection` → `exchange/binance/client.py:1146-1280`,
which cancels the old stop and then creates the new one. Between the two calls the position has no stop.
Binance refuses a second `closePosition` trigger of the same kind (-4130), so "create the new one first" is not
available with that order type. The position-management package (`TI/position/`) decides exits and executes
nothing in production.

### 2.8 Process-global state

`production_runtime._clients`, `_cycle_lock`, `_progress`; `production_execution._boundaries`; mutable
attributes on the cached exchange client; `cycle_shadow._epochs`; `reservation_store._LOCKS`;
`risk/capital_ledger.ACCOUNT_RESERVATIONS`; `entry_protection._registry`. Permits held in `ContextVar` do not
follow a thread pool. The new pipeline keeps no module-level state at all (pinned by a test).

### 2.9 Emergency flatten waits behind the cycle

`production_runtime.flatten` sets the global kill switch, then waits up to 60 s for the same lock the broker
cycle holds, and answers 409 `BROKER_CYCLE_BUSY` when it does not get it. Binance only. No preemption.

### 2.10 Placeholder evidence in the tail

`prepare_submission` passed `BrokerHealthContext(..., 'HEALTHY', ...)` and `CapitalReadiness(True, 'LOGICAL')`
as constants (`production_execution.py:1058-1060`). `build_plan` set `mode="LIVE"` for demo accounts too.

### 2.11 Evidence grows without bound

About one `cati_execution_evaluations` row per account per 30 s cycle, in a table guarded by append-only
triggers. No retention framework, no growth measurement; `account_recorder.prune` exists and is never called.

### 2.12 Exchanges: three raw clients, four contracts, one certified path

- Raw clients exist for Binance USD-M (`exchange/binance/client.py`), Bybit V5 linear (`exchange/bybit/client.py`)
  and BingX swap V2 (`exchange/bingx/client.py`). Only Binance is wired to execution; durable close and flatten
  refuse the other two.
- Four overlapping contracts: `exchange/contract.py` (method names), `exchange/interface.py` (a protocol nobody
  implements fully), `TI/execution/adapter.py` (`ExecutionAdapter`, used by the boundary), the economic adapters
  in `TI/venue/`. Four hard-coded status vocabularies (`EXECUTOR_WRAPPER_SUPPORT`, `environment_certifications`,
  `ADAPTER_STATUS_REGISTRY`, `DECLARED_PROFILES`), none with an expiry.
- No websocket anywhere. Rate limiting exists for Binance only, in process memory, not shared between workers;
  Bybit and BingX have no 429 handling and no time synchronisation.
- Instrument rules: a catalogue and a 1-hour staleness refusal exist (`exchange/instruments.py`,
  `TI/execution/preflight.py`); silent defaults remain in the raw path (`get_symbol_filters` returns zero filters
  on error, a tick falls back to 0.01, `max_leverage=125`).
- `execution/demo_transport_smoke.run` posts a raw MARKET order outside the boundary (an operator tool).

## 3. Dependencies between the requirements

```
3.2 registry ──► 3.3 gate ──► 3.4 one tail ──► 3.8 multi-position execution ──► 3.9 stop replacement
     │                │                               ▲                 ▲
     ▼                ▼                               │                 │
3.11 governance   3.5 contracts ──► 3.6 daily trend ──┤   3.7 portfolio + reservations ──► 3.10 brakes
                       │                 │
                       ▼                 ▼
                 3.1 cycle driver ──► 3.15 shadow comparison      3.12 performance, 3.13 parallelism, 3.14 retention
4.1 adapter contract ──► 4.2 certification, 4.3 rule cache, 4.4 streams, 4.5 rate budget, 4.6 BingX findings
4.1 fake venue ──► 3.8, 3.9 (controlled fakes)
```

Order of work: the gate first (safety), then the shared contracts (family-neutral intents, the venue adapter
protocol), then everything else in parallel against those two contracts.

## 4. Constraints kept

- Engine per-trade risk ceiling 0.40 %, maximum entry stop distance 15 %: unchanged. A wider stop refuses the
  entry; it is never clipped.
- No strategy is certified; no governance promotion was made; no order authority exists for any family.
- `LIVE_ORDER_SUBMISSION_ENABLED` is false and untouched.
- `setups.registry.SPECIALIST_REGISTRY` holds exactly four specialists.
- The research evidence is untouched: no file was added or changed under the evaluation source roots
  (`TI/families`, `TI/research/governance`, `TI/research/evaluator`, `TI/research/certification/stats.py`,
  `market_data/binance_archive.py`, `market_data/daily_dataset.py`), in `research/` or in `docs/research/`. The
  pre-holdout readiness of Mandate 004 therefore still holds. The holdout was not opened.
- Retired systems were not deleted.
