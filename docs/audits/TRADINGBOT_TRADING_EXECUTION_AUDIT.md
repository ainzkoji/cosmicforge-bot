# CATI Decision Pipeline, Brokers, Risk & Execution Audit (Sections 5, 9, 11, 12, 13, 14)

**Audit:** CosmicForge Tradingbot master end-to-end audit (read-only) · **Date:** 2026-11-13
**Baseline:** `origin/main` = `649080b` · engineering branch `origin/ccr-b5c520cf-80g8x2` = `732e0e2` (7 commits not on main, inspected by diff/worktree only)
**Master report:** [TRADINGBOT_MASTER_END_TO_END_AUDIT.md](TRADINGBOT_MASTER_END_TO_END_AUDIT.md) · **Gap register:** [TRADINGBOT_MASTER_GAP_REGISTER.csv](TRADINGBOT_MASTER_GAP_REGISTER.csv)

Evidence levels used throughout: L1 observed authenticated behaviour · L2 real venue evidence · L3 integration test with real boundaries ·
L4 contract/unit test with mocks · L5 static source inspection · L6 documentation/historical claim · L7 inference.
No production database, VPS or authenticated venue session was available to this audit; nothing below is L1.
Finding IDs are stable and map to the gap register (column "Master ID" consolidates duplicates across areas).

## Lead auditor reconciliation
- Two auditors independently found that the branch fix `334f2ff` (order switch must never block protection) is **incomplete**; the lead verified it directly: on the branch, `production_execution.py` ~l.1473 still wraps post-fill `submit_protection`, the fail-safe close and the 48 h horizon close in `if order_submission_gate(plan.environment)["enabled"]`, and `app/execution/perpetual_protection.py:23` still raises when the gate is off. Consolidated as **M-01 (P0)** in the master report.
- Binance DEMO certification `cert_6852df804410b16bee8ad4d1` is genuine (L2): hash chain recomputed independently, `mocked_transport=false`, venue order ids present, expiry 2027-01-30. It covers the platform adapter path, MARKET + GTC LIMIT, leverage 1, ADAUSDT only; DEMO strategy orders (RESIDUAL) run through the legacy `BinanceExecutor` tail, which the certificate did not exercise (EXEC-05).
- **No CATI-originated strategy order exists on any venue** (CATI-13). End-to-end strategy execution is supported only by L4 tests with mocked transport.
- Part 1 below is the CATI decision-pipeline audit; Part 2 is the broker/risk/execution/certification audit.

---

# Part 1

## CATI runtime audit (sections 5 and 14) -- AREA = CATI

Repo: /home/user/cosmicforge-bot at origin/main 649080b. Branch origin/ccr-b5c520cf-80g8x2 (732e0e2) inspected by diff only.
All paths below are relative to `backends/bot-backend/` unless prefixed.

### 1. Scope and method

- Static call-graph trace from `deploy/systemd/cosmicforge-trading.service` ExecStart (`uvicorn app.main:app --workers 1`)
  through `app/main.py` startup hooks into `app/trading_intelligence/integration/*` and `pipeline/*`.
- AST import-closure from `integration.production_runtime` over `app/trading_intelligence` (script in
  scratchpad `cati_imports.py`), then grep of the actual CALL sites of the "intelligence" modules (regime, forecast,
  setups, ranking, veto, controller, market_state, position), to separate imported from called.
- Research register (`docs/research/registry/research_register.jsonl`) read to obtain the certification status the
  family registry would compute (it reads this file at runtime).
- Prior reports (`docs/implementation/TRADINGBOT_DEMO_ACTIVATION_REPORT.md`,
  `docs/master_ensemble_no_trade_forensic_report.md`) treated as claims (L6).
- Targeted tests run (Python 3.12 venv, APP_ENV=TEST ...): `tests/trading_intelligence/test_experimental_demo.py
  test_bot_sizing_config.py test_production_demo_execution.py test_step3_connected_portfolio.py
  test_step3_family_registry.py test_production_execution.py` -> **119 passed in 173 s** (L4; exchange mocked).
- No production database, no journal, no venue access was available: there is **no L1 observation** of the runtime.

### 2. Detailed analysis

#### 2.1 Production entry point and what actually runs (call graph)

```
cosmicforge-trading.service  ExecStart uvicorn app.main:app --workers 1
 app/main.py:680  _startup_run_manager -> runner_loop()  (main.py:1117)
    production: RuntimeOwnerHost (integration/runtime_owner.py) -> RuntimeOwnerLoop.run (runtime_owner_loop.py:136)
    = lease acquire/renew + watchdog only, tick does nothing (runtime_owner.py:41-68).  MultiBotRunner/PaperRunner NOT built.
 app/main.py:981  _startup_background_jobs -> _when_owner() (waits for lease, main.py:962)
    settings.production -> asyncio.create_task(production_runtime.run(DB()))      (main.py:1000-1001)
    else               -> residual_simulation.run
 integration/production_runtime.py:809 run(db)
    ShadowScheduler(db).run(interval=30)          platform_runtime.py:28-106  -> PlatformCycleDriver.run_due (SHADOW ONLY)
    VenueRuntime(...).run()                        (venue reads)
    _broker_loop(db)  production_runtime.py:884, every 30 s:
       residual_prospective.schedule (60 s throttle, 1 thread)     -> collect() (residual_prospective.py:373)
       forward_observe.schedule                                     (public BTC/ETH book observations)
       broker_cycle -> sync -> _sync_accounts -> sync_account -> _sync_with_client  (production_runtime.py:413/451/244/276)
           -> production_execution.process_account (production_execution.py:575)
                -> portfolio_runtime.dispatch_account (portfolio_runtime.py:410)  [families with DEMO/LIVE in allowed_modes]
                   else -> _process_account (production_execution.py:978)          [frozen RESIDUAL TradePlan path]
```

Two, and only two, decision sources can reach an order in production:

| Path | Family | Signal producer | Executor |
|---|---|---|---|
| A. TradePlan path | `RESIDUAL_MOMENTUM_PORTFOLIO_TOP1` (H-015) | `residual_prospective.collect` hourly, table `cati_residual_decisions` | `_process_account` -> `CATIExecutionBoundary.process_trade_plan` (`execution/boundary.py`) |
| B. Portfolio path | any registry family with a `pipeline_adapter` AND DEMO/LIVE in `allowed_modes` -> today only `DAILY_TREND` (H-018) qualifies by construction | `pipeline/daily_trend.py` `broker_account` | `portfolio_runtime.ConnectedPortfolioRuntime.process` -> `pipeline/execution.PortfolioExecutionBoundary.execute` |

#### 2.2 Market intelligence components vs production decisions (item 1)

Called from a production entry point (L5):
- Data ingestion: `residual_prospective.PublicCandles` (direct `requests` GET to fapi.binance.com 15m klines,
  residual_prospective.py:280-297 -- NOT through the http budget), `market_data.daily_feed.default_daily_bars`
  (budgeted, platform_runtime.py:14-25), `forward_observe` (BTC/ETH book).
- Validation: `hourly_window` (exact 673 h, gap/duplicate/OHLC geometry refusal, residual_prospective.py:49-66).
- Feature/score: 24 h BTC/ETH-residual momentum z-score in `decision_snapshot`; daily-trend breakout ensemble in
  `pipeline/daily_trend.py` + `families/daily_trend`.
- Opportunity/qualification: `eligibility()` (production_execution.py:124-178): SELECTED_TOP1, |score|>=2,
  >=30 eligible, frozen universe, geometry 2.5R.
- Cost: frozen rates only (`next_edge_registry.json` cost_policy: fee 4 bps, half-spread 1 bp, slippage 2 bps).
- Ranking: "top-1 by |z|" inside the residual snapshot; no ranking module used.
- Risk-adjusted selection/sizing: hard risk + sizing in `prepare_submission` (production_execution.py:1204),
  L-08 / platform order budget (`scale/spread_gate.py`) on path A only.
- Decision persistence: `cati_residual_tracker`, `cati_residual_inputs`, `cati_residual_decisions`,
  `cati_trade_plans` (trade_plan/evidence_store.py:21), `cati_execution_evaluations` (production_evidence.py:8),
  `cati_production_decisions/_fills/_protection/_closes/_state/_daily_risk`, `cati_account_period_risk`
  (execution/production_schema.py:45-63), `cati_family_decisions` + `cati_family_events` (shared cati_platform_schema),
  `cati_execution_intents/_events` (path B), `cati_experimental_demo_events`, `platform_market_entry_policy_events`.

**No production caller** (imported at most; the only call sites are `app/runner/runner.py` PaperRunner
(runner.py:1976, 4402, 4427 -- not built when `settings.production`, main.py:1125-1131) and `research/certification/*`):
- `regime/engine.compute_regime_distribution`, `market_state/engine.build_market_state` +
  `integration/snapshot_adapter.evaluate_market_state`, `setups/*` specialists (4 families in the registry, 15m,
  `pipeline_adapter=None` -> blocker `NO_PLATFORM_PIPELINE_ADAPTER`, family_registry.py:157-177, 326-327),
  `forecast/*` (posterior, library, OOD, calibration), `ranking/coordinator.CATICycleCoordinator`,
  `veto/engine.evaluate_veto`, `controller/cati_controller.CATIController`, `portfolio/service`,
  `integration/cycle_shadow.py`, `integration/shadow_hook.py`, `integration/cati_dispatch.py` (only referenced by
  cycle_shadow.PLAN_ROUTER and the static check in `app/activation/cati.py:96-119`), `position/*` (0/6 modules
  even imported), `integration/opportunity_gate.py`.
- Path A TradePlans are built with every intelligence lineage id set to the residual decision id
  (`build_plan`, production_execution.py:452-468: regime_distribution_id, veto_decision_id, ranking_batch_id,
  economic_opportunity_id ... = `did`; expected_net_R = conservative_edge_R = p_net_profitable = 0). The plan
  says FORECAST_NOT_APPLICABLE, but the regime/veto/ranking lineage fields are placeholders, not evidence of a stage.
- Import closure: 192 of 274 CATI modules (45k of 57k lines) are importable from production_runtime; the
  regime/forecast/veto/ranking/controller stack is in that closure only via imports, never executed.

Conclusion: the production "intelligence" is two narrow rule-based families (one FAILED, one uncertified). The
regime -> forecast -> veto -> ranking -> controller stack is **implemented and tested but not wired** to production.

#### 2.3 End-to-end transitions (item 2)

| # | Transition | Source (file:line) | In/out schema | DB writes | Error conditions | Tests (L4) | Production wiring | Gap |
|---|---|---|---|---|---|---|---|---|
| 1 | Market data -> validation | residual_prospective.py:373-424 `collect`, :49 `hourly_window`; daily_feed via platform_runtime.py:14 | raw kline rows -> numpy window | cati_residual_inputs, cati_residual_tracker heartbeat | RateLimited (5-min backoff), INPUT_GAP_OR_DUPLICATE, INVALID_OHLCV; missed hour -> MISSED_PROSPECTIVE_BOUNDARY | test_production_execution, test_natural_signal_replay | _broker_loop :893 -> residual_schedule | PublicCandles bypasses http budget; no per-symbol timeout budget for the 15-min window |
| 2 | Features -> regime | none in production | -- | -- | -- | regime tests (unit) | **not wired** | CATI-01 |
| 3 | Strategy evaluation -> opportunity | `decision_snapshot` / `Tracker.commit_decision` residual_prospective.py:180; daily trend `pipeline.broker_account` portfolio_runtime.py:353 | snapshot dict -> decision row; DAILY_TREND -> `IntentBatch` (pipeline/contracts) | cati_residual_decisions; cati_family_decisions (record_decision) | PROSPECTIVE_HOUR_BOUNDARY_REQUIRED, FAMILY_BROKER_DECISION_STALE | test_step3_connected_portfolio | residual: yes; daily trend broker stage: only if DEMO in allowed_modes (never today) | -- |
| 4 | Opportunity -> ranking/portfolio decision | `eligibility` production_execution.py:124; reservation `boundary.reservations.reserve` :1121 | row -> eligibility code; `TradePlan` (contracts/trade_plan) via `build_plan` :431 | cati_trade_plans, reservation store | RESIDUAL_DECISION_NOT_QUALIFIED, PROSPECTIVE_ENTRY_WINDOW_EXPIRED, CATI_DECISION_ALREADY_ATTEMPTED | test_production_final_audit, natural replay | yes | lineage placeholders (CATI-07) |
| 5 | Bot configuration | `boundary_for` production_execution.py:478; portfolio_runtime.py:69 | BotInstance -> EffectiveBotPolicy | -- | PRODUCTION_REQUIRES_CATI_BOT, PRODUCTION_REJECTS_PAPER_BOT, ACCOUNT_EXECUTION_OWNER_AMBIGUOUS | test_bot_sizing_config (15) | yes | fixed amount = margin semantics (CATI-06) |
| 6 | Sizing -> risk | `prepare_submission` :1204, `account_risk` :255, `persisted_risk_controls` :400 | plan + equity -> sizing evidence | cati_production_daily_risk, cati_account_period_risk | EXECUTABLE_PRICE_RISK_EXCEEDED, ACCOUNT_DAILY_LOSS_LIMIT_REACHED, kill switch, drawdown | test_k1_sizing_gates, test_risk_based_sizing | yes | -- |
| 7 | Execution eligibility (family + venue + L-08) | provenance.py:256-365 `FamilyExecutionGate`; boundary.py:917-958 `_reserve_spread`; experimental_demo.py:372 `plan_scope` | plan -> (allowed, reason) | cati_experimental_demo_events (ENTRY_CLAIMED) | FAMILY_FAILED_RESEARCH, FAMILY_STAGE_NO_ORDER_AUTHORITY, VENUE_ADAPTER_NOT_CERTIFIED, PLATFORM_ORDER_BUDGET_UNAVAILABLE, MARKET_FALLBACK_DISABLED_BY_POLICY, MARKET_SPREAD_EXCESSIVE, EXPERIMENTAL_DEMO_NOTIONAL_LIMIT_EXCEEDED | test_experimental_demo (20), test_step5_spread_boundary | path A only | path B has no spread/L-08 gate (CATI-04) |
| 8 | Broker order -> fill | boundary.process_trade_plan; pipeline/execution.py:1043 `place_order` | EntryRequest/OrderRequest -> attempt / intent rows | attempt store, cati_production_fills, cati_execution_intents/_events | SUBMIT_UNKNOWN_PENDING_RECONCILIATION, DEMO_ORDER_SUBMISSION_DISABLED | natural replay (mocked transport) | yes | no venue evidence of a strategy order (CATI-02) |
| 9 | Protection | reconcile_executions :1331 (protection at ~:1470), production_protection.place_native_protection | position -> closePosition STOP/TP | cati_production_protection | PROTECTION_STATE_UNKNOWN, fail-safe close on ABSENT | test_production_position_safety | yes | order switch off strands path-A protection even on branch (CATI-05) |
| 10 | Monitoring/exit | maintain_account :683, horizon close (48 h) in reconcile_executions; path B stop replacement | -- | cati_production_closes | -- | position safety tests | yes | `position/` exit engine unused (by design) |
| 11 | Reconciliation | production_runtime.py:298-314 `reconcile_position_rows`; reconcile_confirmed_intents | broker positions -> projections | position projections, cati_production_state, account_equity_snapshots | ACCOUNT_OWNER_MAPPING_REQUIRED, READ_FAILED | test_production_loop | yes | -- |

#### 2.4 Decision frequency (item 3)

- Residual: collection attempted every 60 s (`schedule`, residual_prospective.py:426-437, hard-coded), decision
  once per UTC hour at the hour close, must be recorded within Q = 15 min or is MISSED (`commit_decision` :186-188);
  missed hours are written as explicit skips, **never recovered** (by design, prospective integrity). Entry only in
  `[decision_time, decision_time+15 min)` and after the next 15m open is recorded (eligibility :139-147).
- Broker cycle: every 30 s (`_broker_loop` :917 `sleep(max(1, 30 - elapsed))`, hard-coded). Each cycle re-evaluates
  every account; the decision is re-validated immediately before submission (`latest_decision` + `eligibility`
  re-check, production_execution.py:1144-1152) -- a genuine stale-signal guard.
- Daily trend: shadow driver ticks every 30 s (platform_runtime.py:91) with daily `due_key`; catch-up up to 30
  missed cycles (driver.py MAX_CATCH_UP); broker stage TTL 24 h (`FAMILY_BROKER_DECISION_STALE`, portfolio_runtime.py:344).
  Timeframes match the families (1h residual, 1d trend). Specialist 15m families are never evaluated.
- Not configurable: 60 s / 30 s / 15 min / 30 s intervals are constants.
- Blocking: residual collection is single-threaded and sequential over BTC/ETH + ~136 symbols at >=0.5 s pacing
  with a 15 s timeout per request (residual_prospective.py:286-289): a degraded venue (timeouts) can push the
  hour's collection past the 15-min window -> MISSED (no trade). Broker cycle processes accounts sequentially in one
  thread; one slow account delays all others (bounded by HTTP timeouts), which consumes the 15-min entry window.
- Strategy evaluation (residual collect, shadow driver, portfolio broker stage) is distinct from the
  maintenance/reconciliation part of the 30-s broker cycle (`sync_account`, `maintain_account`); maintenance runs
  first and fail-closed (production_execution.py:1018-1046).

#### 2.5 Autonomous trading readiness per state (item 4)

| State | Status | Evidence |
|---|---|---|
| Engine running | Implemented + wired (lease-gated) | main.py:981-1007, production_runtime.py:809; L5. No L1 on the VPS. |
| Account synced | Implemented; operational claim only for owner laptop | production_runtime.py:276-338; report claim SYNCED (L6) |
| Data available | Implemented (residual public klines; daily bars budgeted) | L5 |
| Strategy evaluating | Residual hourly YES; DAILY_TREND shadow only; specialists never | L5 |
| Signal detected | Rare: needs |z|>=2 top-1; report shows NO_SIGNAL majority (L6) | eligibility :164 |
| Qualified | eligibility() strict | L4 |
| Strategy authorized | **NO**: RESIDUAL = FAILED (register HYPOTHESIS_STATUS FAILED, ECONOMIC_GATE FAIL); DAILY_TREND = NOT_CERTIFIED (H-018 REGISTERED, run verdict PENDING_HOLDOUT, statistical gate BLOCKED_PENDING_APPROVAL); both M0. Only waiver: experimental DEMO grant, path A only; the recorded grant (72 h from 2026-11-01) has lapsed | family_registry.py:307-339; research_register.jsonl seq 27; L5/L6 |
| Bot config valid | Owner bot's fixed 120 USDT margin cannot satisfy 0.40 % per-trade (L4 test_bot_sizing_config) | L4 |
| Market-entry policy (L-08) | Default refuses MARKET entries; recorded DEMO policy expired 2026-11-08 (claim) | spread_gate.py:1-40; L6 |
| Risk approval | Implemented | L4 |
| Order authorization | DEMO switch true in env example; LIVE false | deploy/env.production.example:76 |
| Submitted / filled / protected / managed / closed | Proven only for the venue-certification run (not a strategy order) | evidence/binance-demo-certification-20261101 (L2) |

No-trade causes, classified:

| Cause | Class | Verified? |
|---|---|---|
| RESIDUAL failed research (H-015 FAILED), DAILY_TREND uncertified (H-018 pending holdout), both M0 | failed research M0 / missing authorization | Verified L5 (register + registry code) |
| Experimental DEMO grant expired (72 h from 2026-11-01) and absent in any new production DB | expired DEMO auth | L6 + L7 |
| Grant for DAILY_TREND can be issued but cannot execute (portfolio path ignores grants) | software defect | Verified L5 (portfolio_runtime.py:237-245, 299-300) |
| L-08 policy absent/expired -> MARKET_FALLBACK_DISABLED_BY_POLICY / MARKET_POLICY_EXPIRED | missing market policy | L5 + L6 |
| Frozen plan max spread 2 bps (half_spread 1 bp x 20000) -> MARKET_SPREAD_EXCESSIVE on low-priced symbols | spread (structural) | L5 (build_plan :472) + L4 test_with_the_real_tick... |
| Fixed 120 USDT margin -> EXECUTABLE_PRICE_RISK_EXCEEDED / EXPERIMENTAL_DEMO_NOTIONAL_LIMIT_EXCEEDED | invalid bot settings / insufficient capital | L4 |
| No |z|>=2 signal most hours | no signal | L6 |
| No PLATFORM_DATABASE_URL in main's production env -> PLATFORM_ORDER_BUDGET_UNAVAILABLE | infrastructure (fixed on branch a746a58) | L5 |
| Adapter certification lives in the local DB; a fresh VPS DB has none -> VENUE_ADAPTER_NOT_CERTIFIED | missing certification | L7 |
| Hour collection > 15 min -> MISSED_PROSPECTIVE_BOUNDARY | scheduler | L5 |
| >1 active bot -> ACCOUNT_EXECUTION_OWNER_AMBIGUOUS | invalid bot settings | L5 |
| Local HTTP budget refusals starving reads (historical) | software defect, fixed def0bf4 | L6 |
| Venue-name mismatch in L-08 eligibility (historical) | software defect, fixed | L6 |
| master_ensemble zero trades (Sept 2026) | legacy strategy, irrelevant to CATI; production refuses non-`cati` bots (PRODUCTION_REQUIRES_CATI_BOT) | L5 |
| Stale depth / exchange rejection / worker failure / unsupported instrument | possible refusal codes, no evidence they occurred | L5 |

#### 2.6 Explicit answers (item 5)

**Can an appropriately authorized and correctly configured customer bot execute a genuine eligible strategy signal
end to end?** Code path: yes, for exactly one family (RESIDUAL, path A) on a Binance DEMO account, and only with an
active operator experimental-DEMO grant (because the family FAILED research), an active single-account L-08 policy,
a platform DB, an adapter-certification record in the runtime DB, a sizing that fits 0.40 % risk and the grant cap,
and a symbol whose tick fits the 2 bps spread bound. Evidence level **L4** (test_natural_signal_replay with real
production factory and mocked transport + harness spread policy that allows market entries). No certified family can
do it today: DAILY_TREND has no DEMO authority and path B cannot be unlocked by a grant. LIVE is impossible by
construction (`MAX_OPERABLE_STAGE = "M6"`, family_governance.py:41; LIVE_STAGE M7).

**Evidence of a CATI-originated strategy order on any venue?** **None.** `evidence/` contains only the Binance DEMO
certification run (`evidence/binance-demo-certification-20261101/orders/...085334Z.json`, purpose certification),
scale/drill evidence and BingX public klines (branch). The DEMO activation report states explicitly "This report does
not claim an autonomous strategy trade: none has happened" (section 6). The 1INCHUSDT signal of 2026-11-01 was refused
`FAMILY_FAILED_RESEARCH` (claim).

#### 2.7 Branch diff review (item 6, commit 334f2ff)

Files: `pipeline/execution.py`, `integration/production_execution.py`, `integration/production_runtime.py`,
`execution/production_close.py`, `execution/production_protection.py`, `shared_lib/core/production.py`.

- `risk_reduction_gate` + `is_risk_reducing_mutation`: DEMO always admits reducing mutations with switch off; LIVE only
  in a live-capable build (none). Transport (`require_broker_mutation_permission`) admits POST only with
  reduceOnly/closePosition on order paths, Bybit cancel/trading-stop paths, and every DELETE. Binance stops are
  `closePosition=true` (production_protection.py:141), closes `reduceOnly=true` (production_close.py:248) -> admitted.
- `PortfolioExecutionBoundary`: batch admission now uses `_operating_refusal(..., risk_reducing=True)` and refuses
  only increases via `_submission_refusal()`; `_increase_refusal` re-checks the switch first; `recover()` admits with
  switch off. Logic is consistent; no regression found by reading. Substituted gates (tests) still refuse whole batch.
- **Incomplete (not a regression):** path A still gates protection on `order_submission_gate`:
  branch `production_execution.py:1473` (first protection after fill AND the 48 h horizon close are inside
  `if order_submission_gate(...)["enabled"]`) and `execution/boundary.py:623-624` (protection of recovered fills);
  `execution/perpetual_protection.py:23` (Bybit/BingX protection) unchanged. So with the switch off, a residual-path
  position gets no first/recovered native stop and no horizon close; maintenance via `reconcile_executions` is the
  same gated code. Gate finding 3.2 is fixed for path B, close and flatten only.
- Minor: every DELETE is "risk reducing", including cancel of a protective stop (only reached in stop-replace flows).
- Branch tests not run by me (cannot checkout); NOT_VERIFIED here.

### 3. Gap list

| ID | Component | File:lines | Problem | Root cause (verified/unverified) | Evidence (+level) | Customer impact | Trading impact | Severity | Classification | Existing implementation | Required correction | Dependency | Owner | Validation test | State | Effort | Release phase |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| CATI-01 | Regime/forecast/setups/veto/ranking/controller/market_state/position stack | integration/cycle_shadow.py, shadow_hook.py, cati_dispatch.py; callers only app/runner/runner.py:1976,4402,4427 | "Intelligence" pipeline has no production caller; production decisions come from two rule-based families only | Verified: production builds RuntimeOwnerHost, not PaperRunner (main.py:1125-1131) | L5 grep/AST | Product claims of regime-aware/ranked intelligence not true in production | No regime/veto/ranking influence on any order | P2 | INTEGRATION_GAP | Full stack implemented + unit tested | Either wire through a certified family pipeline adapter or label as research-only in product/docs | Research certification | Engineering/Research | Production-entry call-graph test asserting which stages run | OPEN | XL | C |
| CATI-02 | Strategy authority | pipeline/family_registry.py:307-339; research_register.jsonl (H-015 FAILED, H-018 REGISTERED/PENDING_HOLDOUT) | No family has DEMO/LIVE order authority; all strategy orders blocked | Verified | L5 + register | Bots never trade autonomously without operator waiver | Zero strategy orders | P1 | RESEARCH_GAP | Governance, register, holdout reserved | Complete MANDATE_004 holdout evaluation; if pass, promote DAILY_TREND to M6 | Holdout data, owner approval | Research/Owner | Registry entry shows DEMO in allowed_modes | OPEN | L | C |
| CATI-03 | Experimental DEMO grant vs portfolio path | portfolio_runtime.py:237-245, 299-300; governance/experimental_demo.py:288-330 | Grant can be issued for DAILY_TREND but path B filters on allowed_modes and never consults grants -> grant silently ineffective; only the FAILED residual family can trade under a grant | Verified | L5 | Operator believes DAILY_TREND is authorized for DEMO; nothing trades | Only a failed strategy can ever trade on DEMO | P2 | LOGIC_DEFECT | Grant ledger + path A waiver | Make `_may_manage` honour `plan_scope` grants, or refuse grants for families without a TradePlan route | CATI-04 | Engineering | Grant for DAILY_TREND on DEMO yields an executed (mocked) batch, or grant refused | OPEN | M | C |
| CATI-04 | Portfolio execution boundary liquidity gates | pipeline/execution.py (no spread_gate/L-08/depth refs) vs execution/boundary.py:917-958 | Path B MARKET entries bypass platform order budget (EXT-LIQ-01), L-08 policy, depth freshness and go-live gate checks that path A enforces | Verified by grep (equivalence of the bounded-slippage check not verified) | L5 | Inconsistent protection once DAILY_TREND is certified | Possible excess slippage / order spreading collisions at scale | P1 | RISK_GAP | Bounded-slippage check, governance authority in path B | Route path-B entries through PlatformSpreadGate / L-08 like path A | -- | Engineering | Path-B entry refused with MARKET_FALLBACK_DISABLED_BY_POLICY without policy | OPEN | M | C |
| CATI-05 | Order switch vs path-A protection | branch production_execution.py:1473-1505; execution/boundary.py:623-624; execution/perpetual_protection.py:23 | With DEMO/LIVE switch off, path-A fills get no first/recovered native stop and no 48 h horizon close; Bybit/BingX protection still gated | Verified (branch diff) | L5 | Open DEMO position unprotected when operator turns switch off | Unprotected exposure | P1 | RISK_GAP | 334f2ff fixed path B, close, flatten | Use risk_reduction_gate at these three sites | 334f2ff merge | Engineering | Switch off + filled path-A entry -> stop placed, horizon close sent | OPEN (partially FIXED_ON_BRANCH_ONLY) | S | B |
| CATI-06 | Bot sizing semantics | production_execution.py:478-574; portfolio_runtime.py:94-110; DEMO report sec. 6 | Fixed "Trade Amount" is margin; with 0.40 % per-trade cap, small accounts' fixed bots are always refused; path B treats fixed amount as max notional (different semantics) | Verified (tests) | L4 test_bot_sizing_config | Customer bot never trades, confusing explanation | All entries refused | P2 | LOGIC_DEFECT | Refusal explanation, risk-based mode | Unify fixed-amount semantics across paths; validate settings at deploy time against risk ceiling | Owner policy | Engineering/Owner | Deploy-time validation test | OPEN | M | C |
| CATI-07 | TradePlan lineage | production_execution.py:452-468 | regime/veto/ranking/economic ids filled with the residual decision id; edge fields 0 | Verified | L5 | Audit trail suggests stages that never ran | None directly | P3 | LOGIC_DEFECT | forecast_id NOT_APPLICABLE label | Use explicit NOT_APPLICABLE sentinels for all unexecuted stages | -- | Engineering | Plan evidence test | OPEN | S | C |
| CATI-08 | Residual frozen spread bound | production_execution.py:459,472 (bps = 2, max spread = half_spread*20000 = 2 bps) | Structural MARKET_SPREAD_EXCESSIVE for symbols whose tick > 2 bps | Verified | L5 + L4 | Signals on low-priced symbols always refused | Fewer eligible trades | P3 | RESEARCH_GAP | L-08 per-record bounds | Owner/research decision (family is FAILED anyway) | CATI-02 | Research/Owner | -- | OPEN | S | C |
| CATI-09 | Residual collection scheduling | residual_prospective.py:280-297, 373-424, 426-437 | Single thread, sequential ~138 symbols, 15 s timeout, direct requests not through http budget; slow venue -> hour MISSED; not configurable | Verified (static) | L5 | Missed signals | Lost entries; unbudgeted IP weight | P2 | ARCHITECTURE_GAP | 5-min rate-limit backoff | Budgeted client, bounded total collection time, metrics on missed hours | -- | Engineering | Simulated slow venue test | OPEN | M | C |
| CATI-10 | Sequential broker cycle | production_runtime.py:451-520 | One slow account delays all accounts in the round; entry window is 15 min | Verified (static) | L5 | Late/missed entries at scale | Lost entries | P2 | ARCHITECTURE_GAP | assigned-mode workers (Step 5.2) | Per-account time budget / parallel workers | Platform DB | Engineering | Load test with slow account | OPEN | M | C |
| CATI-11 | Runtime-local authorizations | adapter certification register, experimental grant, L-08 policy (DB rows) | A fresh production DB has no venue certification, grant or L-08 record; recorded ones expired (grant ~2026-11-04, L-08 2026-11-08) | Unverified (no prod DB) | L6/L7 | Nothing trades after deploy until re-certified/re-authorized | All entries refused | P1 | OPERATIONS_GAP | Admin APIs and cert runner | Runbook: certify venue on prod DB, record grant/L-08 or promote family | CATI-02 | Operations/Owner | Health endpoint lists all four as active | OPEN | S | B |
| CATI-12 | Platform DB in production env | deploy/env.production.example (main) | No PLATFORM_DATABASE_URL -> every path-A entry PLATFORM_ORDER_BUDGET_UNAVAILABLE | Verified | L5 | No trading | All entries refused | P1 | INFRASTRUCTURE_GAP | Branch a746a58 adds it | Merge a746a58 | -- | Engineering/Operations | Env check in preflight | FIXED_ON_BRANCH_ONLY | S | B |
| CATI-13 | Strategy order evidence | evidence/ | No venue evidence of any CATI strategy order (only certification orders) | Verified | L2 absence | Autonomous trading unproven | -- | P1 | TESTING_GAP | Mocked e2e tests (L4) | One supervised DEMO strategy order end-to-end with evidence capture | CATI-02/11 | Operations | Evidence file with plan id -> order -> stop -> exit | OPEN | M | C |
| CATI-14 | Portfolio dispatch shadowing residual | portfolio_runtime.py:299-304 | Any cati_execution_intents history on an account makes dispatch return a result, so path A (residual) is never evaluated for that account | Verified (static) | L5 | Confusing "NO_EXECUTABLE_FAMILY" | Residual signals ignored on such accounts | P3 | LOGIC_DEFECT | -- | Explicit routing rule per account | -- | Engineering | Unit test | OPEN | S | C |

### 4. Verdicts

- **CATI runtime: PARTIAL.** The canonical runtime is correctly wired (single lease-owned process, 30-s broker
  cycle, hourly residual decisions, shadow daily-trend driver, fail-closed maintenance-first account step, stale-signal
  re-check before submit). It is NOT ready for autonomous trading: no family has order authority (RESIDUAL FAILED,
  DAILY_TREND pending holdout), the only waiver path trades a failed strategy, the regime/forecast/ranking intelligence
  stack has no production caller, path B lacks the liquidity/L-08 gates, and there is zero venue evidence of a strategy
  order. Platform/engine available (Phase A): READY in code (L5). Broker-connected DEMO (Phase B): PARTIAL (needs
  platform DB merge, prod-DB venue certification, CATI-05). Autonomous DEMO trading (Phase C): BLOCKED (CATI-02,
  CATI-11, CATI-13). Real money (Phase D): BLOCKED by construction (M7 > MAX_OPERABLE_STAGE M6, LIVE switch, build).
- **End-to-end genuine strategy signal on an authorized, correctly configured bot:** possible in code for RESIDUAL on
  Binance DEMO under an experimental grant -- **L4 only**; never observed (no L1/L2).
- **Branch 334f2ff:** no regression found; fix is incomplete for path A protection and Bybit/BingX (CATI-05).


# Part 2

## EXEC audit: broker connections, risk, order execution, certification (audit sections 9, 11, 12, 13)

### 1. Scope and method
- Baseline origin/main 649080b; branch origin/ccr-b5c520cf-80g8x2 (732e0e2) read with `git show`/`git diff` and a temporary `git worktree` in the scratchpad (since removed).
- Static tracing from production entry points: `deploy/systemd/cosmicforge-trading.service` -> `app.main:app` -> `_startup_background_jobs` (app/main.py:979-1006) -> `production_runtime.run` (settings.production) -> `process_account` / `recover_account` / `maintain_only` / `flatten`.
- Independent recomputation of the Binance DEMO certification hash chain, run seals and action hashes (script: (audit workspace) verify_cert.py).
- Tests run:
  - Branch worktree: `tests/test_risk_reduction_gate.py tests/test_emergency_controls.py tests/trading_intelligence/test_production_position_safety.py tests/trading_intelligence/test_step3_multi_position_execution.py`: **212 passed** (367 s).
  - Main: `tests/test_emergency_controls.py -k refuses_when_the_engine_cannot_act`: **6 passed**. The "gates" case asserts that on main a DEMO flatten is REFUSED when the DEMO order switch is off.
- No exchange was contacted. No repository file was changed.

### 2. Detailed analysis

#### 2.1 Broker connection journey (user-backend `app/api/brokers.py`, `app/core/broker_service.py`; shared `shared_lib/broker/*`)
| Step | Implementation | Evidence | Assessment |
|---|---|---|---|
| Select broker | `GET /catalog`, `create_broker_account_draft` (broker_service.py:243) | L5 | OK |
| DEMO/LIVE | Draft carries `environment`. `submit_broker_credentials_v2` (1160-1270) takes it from the `broker_accounts` row and refuses a blob env or base_url that does not match (`BROKER_ENVIRONMENT_MISMATCH`, 1196, 1229) | L5 | Good. The schema default is `"live"` (brokers.py:29, 41), so an unspecified request creates a LIVE account |
| Submit creds / encryption | Fernet `encrypt_credentials` -> `broker_credentials_v2` with status `validating` (1231-1253) | L5 | OK |
| Auth validation | `validate_and_activate_credential_v2` -> `_test_broker_connection` (1272+) | L5 | Venue HTTP calls are made **inside `BEGIN IMMEDIATE`** (1308-1352), so the shared SQLite write lock is held during network I/O. P2 |
| Permission check / withdrawal refusal | `_evaluate_key_permissions` -> `probe_permissions` -> `evaluate_trading_permissions` (permissions.py:94-103) | L5 | WITHDRAW=True -> REJECTED. **Fail-open**: a probe error, BingX (permission_probe.py:50-53, never inspectable), and Binance/Bybit DEMO (no wallet host: environment.py:41-47) all give `ACCEPTED_UNVERIFIED`, and the key is activated. Probed only at activation (no other caller of probe_permissions). |
| Identity | `broker_accounts` and `resolve_broker_auth` (shared resolver) are the single source for both user-backend and CATI. Both backends use the same DATABASE_URL (deploy/env.production.example:23) | L5 | Same authoritative account and environment: YES. No venue-side account UID is captured, so the same key can back two platform accounts undetected (P2) |
| Balance / positions / open orders | CATI broker cycle (`production_runtime._sync_with_client`). The DEMO report claims reads every cycle | L5 / L6 | Implemented. Operational on the owner's local machine only (L6) |
| Reconciliation | `recover_account`, `reconcile_executions`, PortfolioExecutionBoundary.recover | L4 / L5; L2 for Binance (RECONCILIATION, MANUAL_POSITION_CHANGE checks) | OK |
| Rotation | v2 versioning: supersede and activate atomically; health cache invalidated | L5 | OK |
| Failure diagnosis | `broker_read_diagnosis.classify` + reason registry | L5 | OK |
| Disconnect / revoke | `disconnect_broker_account` (926-964) / `delete_broker_account_permanently` (966-1002) | L5 | **Defect.** They delete only the legacy `broker_credentials` table. `broker_credentials_v2` rows (the primary source, resolver.py:187-230) are NOT deleted or revoked. Bots are soft-deleted with no check for open positions or resting orders and no flatten, and CATI then refuses the account (resolver status check, resolver.py:153-165). Open positions are left unmanaged: native stop only, no stop replacement, no platform flatten |
| Wrong creds / network / env / unsupported venue | -2015/-2014/401 classified; base_url mismatch refused; unsupported -> `_unsupported_broker_result` | L5 | OK |

**Binance AdapterError rate-budget root cause: RESOLVED in source (L5); operational claim L6.**
- `shared_lib/broker/http_budget.py:38-55` replaces the flat 100 units per request with a per-path weight catalogue.
- Unknown paths fail closed (KeyError on lookup).
- `production_runtime.client_doubt` (87-101) no longer rebuilds the client on a local budget refusal.
- No change to these files since def0bf4 on main (`git diff def0bf4 origin/main -- platform_adapter shared_lib/broker` is empty).

#### 2.2 Risk
Two entry-capable execution tails exist in production. `process_account` (production_execution.py:575-616) dispatches:
- **Portfolio tail**: `portfolio_runtime.ConnectedPortfolioRuntime` -> `pipeline/execution.PortfolioExecutionBoundary` over the certified platform adapter. Used when a registry family is executable.
- **Legacy residual tail**: `_process_account` -> `execution/boundary.CATIExecutionBoundary` -> `BinanceExecutionAdapter` -> legacy `app/execution/executor.BinanceExecutor`. Used for the residual family under the experimental-DEMO grant. This is the path the DEMO activation report's strategy orders take (FAMILY_FAILED_RESEARCH waived).

Controls present (L5, plus L4 suites):
- Per-trade risk: portfolio `RISK_ABOVE_CEILING` (pipeline/portfolio.py:73); legacy orchestrator hard risk (boundary.py:330-346) with the 0.4 % system ceiling.
- Daily loss latch (durable, `cati_production_daily_risk`, production_execution.py:255-400). Applied to BOTH tails: portfolio via `operating_controls` (portfolio_runtime.py:157-205).
- Weekly/monthly drawdown and consecutive-loss gates.
- Daily trade cap and hard runaway cap.
- Kill switch: `CATI_NEW_ENTRY_KILL_SWITCH`.
- Unresolved transfers block entries.
- Leverage cap, margin sufficiency and liquidation distance: portfolio `LEVERAGE_CAP`, `INSUFFICIENT_MARGIN`, `LIQUIDATION_TOO_CLOSE`.
- Stop width and stop-widening refusal.
- Open-risk cap.
- Reservations: `pipeline/reservations.py`, `CATIReservationStore`, `ACCOUNT_RESERVATIONS` pending margin.
- Slippage: `_slippage_refusal`, L-08 policy depth/spread.
- Partial fills: `PARTIAL_FILL_CUT` (pipeline/execution.py:1112-1122).
- Account brakes (paused/halve/halted) on the portfolio tail, evaluated at the daily close only.

Gaps:
- **Correlation**: no check in the portfolio tail (grep). The legacy app/risk/correlation_filter.py exists, but positions are capped at 1 on the legacy tail. P2.
- **Shared capital across bots**: the portfolio tail refuses accounts with more than one active bot (`ACCOUNT_EXECUTION_OWNER_AMBIGUOUS`, portfolio_runtime.py:323-327); the legacy tail uses max_open_positions=1. Cross-account aggregate exposure is not modelled (P3).
- **Duplicated risk implementations**: legacy orchestrator/safety engine vs `pipeline/portfolio.authorize`. Different arithmetic, two refusal vocabularies. P2 (contradictions possible; not proven).

**Is risk enforced at the final execution boundary?** Yes, in layers (L5):
1. CATIExecutionBoundary hard risk, then a re-check of family authorization right before submission (boundary.py:400-406).
2. Executor call-scoped entry permit (executor.py:1099-1109, entry_permit.py:163-168).
3. In production, the transport refuses any non-reduce-only order POST without a CATI permit (`CATI_ENTRY_AUTHORITY_REQUIRED`, production.py `require_broker_mutation_permission`). All raw clients (binance/bybit/bingx/oanda/mt/ibkr) call it.

Residual holes:
- (a) The transport gate is a no-op when `APP_ENV != PRODUCTION` (it returns before any check).
- (b) The transport entry check covers only the single entry path (`/fapi/v1/order`, `/v5/order/create`, `/openApi/swap/v2/trade/order`). A non-reduce-only `/fapi/v1/algoOrder` (conditional entry) is not checked. There is no current caller (P3).
- (c) `backends/bot-backend/close_btc_raw.py` signs raw HTTP to `https://fapi.binance.com` (LIVE) with the key read from `data/bot.db`, bypassing every gate. It sends reduce-only, but it skips environment, fence and audit. Other root/legacy scripts (close_btc.py, scripts/close_all_positions.py, scripts/emergency_flatten_all.py) go through gated clients. emergency_flatten_all cancels all orders (including stops) BEFORE it closes.
- (d) Legacy admin-only routes in app/main.py (`POST /binance/leverage`, `POST /binance/cancel-all`, `GET /trade/protection` which mutates) use the platform `.env` key (settings.BINANCE_API_KEY) outside CATI. They are transport-gated in production. P3.
- No order placement exists in user-backend or admin-backend (grep: order methods in user-backend/app/exchange have no callers).

#### 2.3 Order lifecycle
**Binance, platform adapter (`platform_adapter/binance.py`)**
- Client order ids are deterministic (`order_client_id`, pipeline/execution.py:322).
- Lookup before create (`_idempotent`, binance.py:303-331).
- Stops are algo orders with `clientAlgoId`. Replacement is create-confirm-retire.
- An unknown acknowledgement is resolved by lookup.
- L2 evidence: IDEMPOTENT_REPEAT shows a second intent with the same id returning the same venue order 1333417962. LOST_ACKNOWLEDGMENT: certa81da5d9f007 was UNCERTAIN and later resolved to 1333418058, then cancelled. PROTECTION_REPLACE: 0.1841 -> 0.1889, new confirmed before old cancelled. External reduce reconciled; reduce-only close; FINAL flat.
- **Limit**: only MARKET and GTC LIMIT were exercised, at leverage 1, on one symbol. Post-only was not exercised.

**Binance, legacy tail (BinanceExecutor + `production_protection.place_native_protection` + `production_close.close_position`)**
- This is a different code path from the one certified.
- Its "DEMO_VALIDATED" is a hard-coded class attribute (execution/binance_adapter.py:76-78), not register evidence.
- It is gated in production by `adapter_supports(production_scope=True)` (boundary.py:379) and by the family gate's `adapter_certified(venue, env)` (provenance.py:303-316). That call does NOT pass adapter_version, and certifies the venue, not this implementation.

**Bybit (`platform_adapter/bybit.py`)**
- Stop is the position trading-stop (Full), amended in place.
- No venue evidence (predeploy_readiness_matrix.json:17 "venue_certification": PENDING).
- The legacy wrapper is UNVALIDATED and refused in production.

**BingX (`platform_adapter/bingx.py`)**
- Stops carry no clientOrderId; they are identified by matching.
- Stop replacement is deliberately refused (`REPLACEMENT_NOT_POSSIBLE_SAFELY`, bingx.py:27-60). This is safe (no gap), but there is no trailing stop or stop tightening.
- Hedge mode is not carried.
- No venue evidence.

Emergency flatten and restart recovery: implemented (production_runtime.flatten, recover_account). L4 suites pass; Binance RESTART_WITH_PROTECTED_POSITION and FLATTEN checks are L2.

#### 2.4 Branch fix 334f2ff + 732e0e2
**MAIN is unsafe when the order switch is off** (L5 + L4). It is DEMO-only, because LIVE is incapable in this build. With `DEMO_ORDER_SUBMISSION_ENABLED=false`:
- `require_broker_mutation_permission` refuses every mutation.
- `place_native_protection` (production_protection.py:110-113), `close_position` (production_close.py:151-162), `flatten_account` (production_execution.py:890) and runtime `flatten` (production_runtime.py:700) all refuse.
- Portfolio `recover` returns REFUSED.
- Main's own test (`test_runtime_flatten_refuses_when_the_engine_cannot_act[gates]`, passed) encodes the refusal.

Resting native stops stay at the venue, but stop replacement, the protection fail-safe and emergency flatten are stranded.

**Branch**:
- `risk_reduction_gate` + `is_risk_reducing_mutation` (production.py on the branch).
- Entries are refused by `_submission_refusal` / `_increase_refusal`.
- Protection, reduce-only and closePosition orders, cancels and flatten are admitted on DEMO.
- On LIVE they are admitted only if `live_build_status().capable` (always False: `BUILD_PERMITS_LIVE = False`, production.py:19).
- Revoked authorizations cannot add exposure: `_increase_refusal` re-checks the switch, and the transport refuses non-reduce-only orders.
- Uncertain outcomes: the recovery journal is unchanged.
- Tests: 212 passed.

**The branch fix is INCOMPLETE.** Branch call sites that still gate risk-reducing actions on `order_submission_gate`:
- (1) `production_execution._reconcile_attempt` (branch line 1473): a filled legacy-tail entry gets NO native stop, NO fail-safe close and NO horizon close while the switch is off. It records the reason only.
- (2) `execution/boundary.reconcile_submit_unknown` (line 624): a recovered fill after an unknown acknowledgement is left unprotected.
- (3) `app/execution/perpetual_protection.place_protection` (line 23): Bybit/BingX protection.

A position whose entry was in flight when the switch went off is therefore unprotected on the legacy tail.

Classification nuance:
- Every DELETE counts as "risk reducing", including cancelling the only protective stop.
- Bybit `trading-stop` with `stopLoss=0` (which removes the stop) is admitted.
- Neither adds exposure, but both reduce protection. P2.

#### 2.5 Certification matrix
| Venue / env | Adapter version | Auth | Bal read | Order read | Entry | Fill rec | Native stop | Stop repl. | Reduce-only exit | Final state | Expiry | Evidence |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Binance DEMO | binance-usdm-1.2.0 (factory.py:59-60), source def0bf4 | PASS | PASS | PASS | PASS (MARKET, 1333417962) | PASS | PASS (algo:1000000268023071) | PASS (create-confirm-retire) | PASS (1333418135) | flat, PASS | 2027-01-30 (90 d after 2026-11-01T08:53:33Z) | L2 |
| Binance LIVE | n/a | - | - | - | - | - | - | - | - | - | - | NOT CERTIFIED; the register refuses LIVE CERTIFIED (certification.py `LIVE_CERTIFICATION_NOT_PERMITTED_IN_THIS_BUILD`) |
| Bybit DEMO/LIVE | bybit-linear-1.2.0 | none | | | | | | | | | | No venue evidence (synthetic wire tests only, L4) |
| BingX DEMO/LIVE | bingx-swap-1.2.0 | none | | | | | | | | | | No venue evidence. Stop replacement refused by design |

Binance DEMO 30/30 verification:
- The hash chain is intact across both files, seq 1-4: genesis -> readonly run (INCOMPLETE, source "unknown", 08:50:29Z, before the def0bf4 commit at 08:51:46Z) -> orders run (08:52:16Z-08:53:33Z, source def0bf4).
- Each run's `actions_hash` and all 30 `evidence_hashes` recompute correctly.
- `mocked_transport: false`, venue order ids present, permit bounded (ADAUSDT, leverage 1, 60 USDT, 1 h).
- The hash is self-referential (not externally anchored), so it proves integrity, not authenticity (L2 at best; venue ids not independently verifiable here).

Adapter code after certification: `git diff def0bf4 origin/main -- backends/bot-backend/app/exchange/platform_adapter backends/shared/shared_lib/broker` is empty. The branch changes `shared_lib/core/production.py` (transport gate) and `pipeline/execution.py`, neither of which is an adapter file.

Retest triggers are weak:
- `adapter_version` is a hand-maintained constant.
- `record_adapter_version` (the VERSION_CHANGED trigger) is called only by `AccountSession`, which has no production caller.
- The certification lives only in the DB where it ran (`cati_adapter_cert_events`). There is no import tool, so a fresh VPS starts NOT_CERTIFIED until a run on the VPS.
- `docs/exchanges/EXCHANGE_CERTIFICATION.md:12-14, 174` still says every venue is NOT_CERTIFIED and "Not wired" (stale).

Unverified Binance operations: post-only, leverage above 1, hedge mode, multiple symbols, partial fills of market orders, take-profit orders, and the legacy executor path.

### 3. Gap list
| ID | Component | File:lines | Problem | Root cause | Evidence | Customer impact | Trading impact | Sev | Classification | Existing implementation | Required correction | Dependency | Owner | Validation test | State | Effort | Phase |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| EXEC-01 | Order switch vs protection (main) | shared_lib/core/production.py require_broker_mutation_permission; production_protection.py:110; production_close.py:151-162; production_execution.py:890; production_runtime.py:700 | With DEMO switch off, stops, reduce-only closes and flatten are all refused | verified | L5 + L4 (main test "gates" asserts refusal) | Open DEMO positions cannot be flattened or re-protected after an operator stop | Stranded exposure (DEMO) | P1 (P0 for any LIVE use) | LOGIC_DEFECT | Branch 334f2ff risk_reduction_gate | Merge 334f2ff/732e0e2 | - | Engineering | tests/test_risk_reduction_gate.py; test_a_demo_flatten_proceeds_while_the_demo_order_switch_is_off | FIXED_ON_BRANCH_ONLY | S | B |
| EXEC-02 | Branch fix incomplete (legacy tail) | branch production_execution.py:1473-1505; execution/boundary.py:624; app/execution/perpetual_protection.py:23 | Post-fill protection, fail-safe close, horizon close and recovered-fill protection still gated on order_submission_gate | verified | L5 | A fill in flight at switch-off is left with no native stop | Unprotected position possible | P1 | LOGIC_DEFECT | risk_reduction_gate exists | Use risk_reduction_gate at these 3 sites; add tests | EXEC-01 | Engineering | New test: switch off after entry ack; expect stop placed | OPEN | S | B |
| EXEC-03 | Disconnect / delete | user-backend/app/core/broker_service.py:926-1002 | Only legacy `broker_credentials` deleted; v2 encrypted keys remain; bots deleted with open positions and no flatten | verified | L5 | "Disconnected" keys persist; positions orphaned | Unmanaged positions; no platform flatten afterwards | P1 | SECURITY_GAP | v2 lifecycle in resolver | Revoke/delete v2 rows; refuse or flatten on disconnect with exposure; audit | - | Engineering | Test: disconnect with v2 creds -> no decryptable row; open position -> refused/flattened | OPEN | M | B |
| EXEC-04 | Withdrawal-permission refusal fail-open | shared_lib/broker/permission_probe.py:34-58; permissions.py:94-109; broker_service.py:626-650 | Probe error, BingX, DEMO -> ACCEPTED_UNVERIFIED and activated; no periodic re-probe | verified | L5 | A withdrawal-capable key can be accepted (BingX LIVE, probe errors) | Custody risk on LIVE | P1 (Phase D) | SECURITY_GAP | REJECTED on explicit WITHDRAW=True | Owner policy: LIVE requires ACCEPTED (inspected); re-probe periodically | Owner decision | Owner/Engineering | Test: probe exception on LIVE -> not activated | OPEN | M | D |
| EXEC-05 | Legacy execution tail uncertified | execution/binance_adapter.py:70-78; boundary.py:379; provenance.py:303-316 | DEMO strategy orders run via BinanceExecutor, but the certification covered platform_adapter/binance; DEMO_VALIDATED is hard-coded; adapter_version not checked | verified | L5 | Certified status shown for a path not exercised | Untested order/stop path for real DEMO trades | P1 | EXECUTION_GAP | Platform adapter certified; portfolio tail checks version | Route all execution through the certified adapter, or certify the legacy path; pass adapter_version | - | Engineering | DEMO cert run through CATIExecutionBoundary (demo_boundary_certification) with evidence | OPEN | L | C |
| EXEC-06 | Certification portability / retest triggers | platform_adapter/session.py:27 (only caller); factory.py:59-60; certification register in local DB | Version change never recorded in production; no import of evidence; VPS starts NOT_CERTIFIED | verified | L5 | DEMO trading blocked on fresh VPS until re-run; code changes don't expire cert | Stale certification possible | P2 | OPERATIONS_GAP | Register, verify_chain, expiry | Record adapter_version at runtime start; derive version from code hash; re-run on VPS | - | Engineering/Operations | Change adapter file -> status EXPIRED | OPEN | M | B |
| EXEC-07 | Bybit / BingX venue evidence | predeploy_readiness_matrix.json:17-18; docs/exchanges/EXCHANGE_CERTIFICATION.md | No genuine DEMO run for Bybit or BingX | verified | L4 only | Customers on these venues cannot trade (correctly refused) | None (fail-closed) | P1 (for those venues) | EXTERNAL_DEPENDENCY | Harness supports both | Run certification on DEMO accounts | Venue DEMO accounts | Operations | Register CERTIFIED with evidence files | BLOCKED | M | B |
| EXEC-08 | BingX stop management | platform_adapter/bingx.py:27-60 | Stop replacement refused; stops have no client id | verified (doc-based) | L5/L6 | No trailing / stop tightening on BingX | Exits only via the initial stop or a close | P2 | WORKING_AS_DESIGNED | Safe refusal | Certify step 7 (BINGX_ENVIRONMENT_CERTIFICATION.md) or accept | EXEC-07 | Research/Engineering | Venue run | OPEN | M | C |
| EXEC-09 | Gate bypass scripts | backends/bot-backend/close_btc_raw.py:1-67 | Raw signed POST to LIVE fapi, bypassing all gates; legacy emergency_flatten_all cancels stops before closing | verified | L5 | Operator foot-gun | Ungated LIVE mutation (reduce-only) | P2 | SECURITY_GAP | Gated clients elsewhere | Delete the raw scripts; route emergency through production flatten | - | Engineering | grep CI rule for raw signing outside clients | OPEN | S | A |
| EXEC-10 | Transport gate scope | shared_lib/core/production.py require_broker_mutation_permission | No-op outside APP_ENV=PRODUCTION; entry check only on one path per venue (algoOrder non-reduce not checked); DELETE always "reducing" | verified | L5 | - | Defence-in-depth gap | P3 | RISK_GAP | Upper-layer permits | Apply the entry check to all order-creating paths; classify cancel of protection | - | Engineering | Unit tests | OPEN | S | C |
| EXEC-11 | Credential validation under write lock | user-backend broker_service.py:1308-1352 | Exchange HTTP inside BEGIN IMMEDIATE | verified | L5 | Slow venue blocks all DB writers | Missed cycles under load | P2 | ARCHITECTURE_GAP | - | Validate outside the transaction, then commit | - | Engineering | Concurrency test | OPEN | S | A |
| EXEC-12 | Risk duplication / correlation | pipeline/portfolio.py vs app/risk/*, trading_orchestrator | Two risk engines; no correlation check in portfolio tail; brakes evaluated at daily close only (intraday covered only by the account daily-loss latch) | verified (presence) / unverified (contradiction) | L5 | Displayed limits may differ by tail | Inconsistent sizing | P2 | ARCHITECTURE_GAP | Shared operating_controls | Converge on one risk authority; add correlation/cluster cap for multi-position | - | Engineering | Parity tests | OPEN | L | C |
| EXEC-13 | Draft env default LIVE | user-backend/app/api/brokers.py:29,41 | Default environment "live" | verified | L5 | Accidental LIVE account | None in this build (LIVE incapable) | P3 | LOGIC_DEFECT | Explicit env accepted | Require explicit environment | - | Engineering | API test | OPEN | S | A |
| EXEC-14 | Binance rate-budget AdapterError | shared_lib/broker/http_budget.py:38-55; production_runtime.py:87-101 | Flat 100-unit budget starved reads | verified | L5; L6 operational | - | - | - | LOGIC_DEFECT | Weight catalogue + no rebuild on local refusal | - | - | Engineering | tests/shared http_budget suites | VERIFIED_FIXED | - | B |
| EXEC-15 | LIVE execution | production.py:19 BUILD_PERMITS_LIVE=False; certification LIVE refusal | No LIVE path, by design | verified | L5 | No real-money trading | - | P1 (Phase D) | OWNER_POLICY_DECISION | live_build gate | LIVE release process, LIVE certification, EXEC-04 | EXEC-02..07 | Owner | - | BLOCKED | XL | D |

### 4. Verdicts
- **Broker connections: PARTIAL.**
  - Binance DEMO is connected, with reads repaired (L5 fix, L6 operational).
  - There is a single shared account identity and environment between user-backend and CATI.
  - Against that: disconnect/delete leaves v2 keys and orphans positions (EXEC-03); the withdrawal refusal is fail-open (EXEC-04); Bybit and BingX connect but are not execution-certified.
- **DEMO execution: PARTIAL.**
  - Binance DEMO platform adapter: genuine, hash-intact 30/30 evidence, and no adapter change since.
  - But the strategy path that trades under the experimental grant is the legacy executor tail, which was not certified (EXEC-05).
  - Main strands positions when the switch is off (EXEC-01, fixed on the branch only), and the branch fix misses three legacy sites (EXEC-02).
  - Certification does not carry to a fresh VPS (EXEC-06). Bybit and BingX DEMO are blocked (EXEC-07).
- **LIVE execution: BLOCKED.** By design: `BUILD_PERMITS_LIVE=False`, the register refuses LIVE certification, there is no LIVE evidence for any venue, and EXEC-04 is open.
- **Risk management: PARTIAL.**
  - Hard risk is enforced at the final boundary in three layers (boundary, executor permit, production transport permit).
  - Daily, weekly and monthly loss limits, leverage, margin, liquidation distance, slippage and the kill switch are implemented on both tails (L5/L4).
  - Gaps: two duplicated engines, no correlation control in the portfolio tail, a transport gate that is a no-op outside PRODUCTION, the raw bypass script, and risk-reducing actions stranded by the switch on main.

