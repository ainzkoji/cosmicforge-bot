# CATI sole authority product and external-entry surface audit

Audit date: 2026-10-03. Scope: API controls, user frontend, signal services,
non-runner execution callers, and preservation of historical records. The
runtime owner, loader, governor, dispatcher, and executor audit is consolidated
by the main completion report.

## Runtime surface decisions

| Surface | Before | Final classification and behavior |
| --- | --- | --- |
| Bot instance creation | Arbitrary strategy identifier entered active execution pool | RUNTIME_ACTIVE CATI only: non-`cati` creation rejected before broker/account/database side effects |
| Auto Pilot deployment | `master_ensemble` selected internally | RUNTIME_ACTIVE CATI only; request produces `strategy_id=cati`; legacy stored IDs remain historical metadata |
| Existing instance resume / pause / account repair | Stored legacy strategy selected by runner | RUNTIME_ACTIVE CATI only via sole loader; preserve historical label, lifecycle/reconciliation, and user ownership |
| Auto Pilot status / pause all / resume all | Filtered by legacy engine ID | RUNTIME_ACTIVE management of all user's CATI-bound instances, including historical labels; no authority granted by lifecycle control |
| TradingView webhook | Legacy candidate mode created execution queue rows | RUNTIME_ACTIVE advisory ingestion only: stored mode cannot enqueue; incoming BUY/SELL remain advisory observations with no execution authority |
| `evaluate_external_candidate` | Threshold-based independent external entry approval | RUNTIME_REACHABLE retired boundary: unconditional denial, no legacy classifier/model import, no persistence or risk/executor call |
| External runner processor | Risk/threshold/execution chain could accept source alpha | RUNTIME_REACHABLE retired by parent: unconditional CATI dispatch-required denial |
| Crypto signal scan scheduler entry points | Independent production signals and publish workflow | RUNTIME_REACHABLE retired boundaries: class and module generation functions return BLOCKED with zero scans, candidates, signals, or publications |
| Crypto signal scoring/geometry helpers | Historical generator mathematics | TEST_ONLY / HISTORICAL_DATA_ONLY reproducibility helpers; public production generation boundaries do not reach them |
| Manual Crypto Signal Center publish/unpublish/cancel | Advisory record management | RUNTIME_ACTIVE advisory metadata; no broker executor caller in either API package |
| News ingestion/calendar | Market context and blackout inputs | RUNTIME_ACTIVE market/risk information; no independent broker entry caller found |
| Product backtest creation | UI risk modes mapped to `sma_cross`; simulated worker loaded legacy strategies | RUNTIME_REACHABLE creation retired: HTTP 409 `CATI_BACKTEST_ARTIFACT_REQUIRED` before job/database creation; CATI-only disabled frontend choice |
| Historical backtest result/list/export | Stored legacy strategy IDs | HISTORICAL_DATA_ONLY; read/export preserved, no new trade authority |
| Historical offline replay/backtest modules | Legacy replay implementations and simulation executors | HISTORICAL_DATA_ONLY reproducibility, no authenticated external broker orders; replay broker client explicitly blocks place_order |
| Broker adapters / CATI Binance bridge | place_order / execution calls | RUNTIME_ACTIVE execution infrastructure preserved; parent executor enforces sole CATI permit for new entries |
| Manual close endpoint | `execute_signal(..., "CLOSE", ...)` | RUNTIME_ACTIVE broker reduction/position management retained; no new entry |
| Demo validation helper | Optional test order validation | RUNTIME_REACHABLE governed broker validation infrastructure, not strategy alpha; no order validation performed in this audit |
| Notification integration examples | Illustrative place/submit calls | DOCUMENTATION_ONLY example module with no mounted entry API or scheduler |
| Old generated OpenAPI snapshots, historical docs, seed/backup scripts, migrations | Legacy strategy/source labels | DOCUMENTATION_ONLY / MIGRATION_ONLY / HISTORICAL_DATA_ONLY; no active selectable engine |
| Old code after unconditional retirement returns | Previous production entry algorithms | DEAD_CODE, unreachable through the public runtime boundary |

No direct `place_order`, `submit_order`, or `execute_signal` caller was found in
`backends/bot-backend/app/api` or `backends/user-backend/app/api`. Normal account
ownership, capability checks, risk, portfolio, reconciliation, historical orders,
and fills remain available.

## Product integration

The onboarding catalog contains only CATI. The previously unmounted legacy
configuration wizard is CATI-only and uses the current onboarding catalog.
Auto Trading presents governed CATI observation/demo/live semantics and states
that Auto Capital Routing is independent. Bot rows display CATI plus the stored
historical strategy label when present.

`GET /api/v1/bot-instances/{instance_id}/engine-status`, forwarded through the
user backend, checks both bot ownership and connected broker ownership. It
exposes CATI runtime selection independently of account-scoped entry authority
(BLOCKED / DEMO / LIVE), governance phase, observe state, activation prerequisites,
library configuration, and the 2.5% daily hard loss cap. Runtime selection is an
architecture fact, not a claim of a live process heartbeat. Actual process/lease
health remains in runtime operational status. Broker-specific economics and
exact account scope are used; unavailable governance/library prerequisites block
entry status. Artifact content/certification verification remains mandatory at
the actual CATI dispatch boundary.

The audited frontend-to-user-backend Auto Pilot proxy rejected the frontend's
`allocation_type` and silently forced fixed allocation, and omitted custom
universe fields. It now validates/preserves percentage versus fixed allocation,
capital budget, symbol-universe mode, and custom symbols, without forwarding
broker secrets. Fixed minimum-notional validation no longer treats percentages
as USDT. Broker execution remains responsible for executable sizing. Product
risk presets now advertise at most the permanent 2.5% daily cap.

## Verification

- Bot-backend surface set: 86 tests passed across sole-authority controls, crypto
  signal helpers/retirement, webhook advisory behavior, external-signal denial,
  Auto Pilot capital flow, and FX Auto Pilot contracts. Added account-scoped M0
  status coverage subsequently passed with all 13 sole-authority surface tests.
- User-backend product controls plus existing broker security regression: 16
  tests passed using an explicitly separate temporary test database.
- User frontend TypeScript project build: passed after CATI status and disabled
  legacy simulation changes.
- No demo/live broker orders, holdout queries, credential publication, or
  capital-routing activation occurred in this surface audit.

Full CATI/risk/runtime regression and canonical process evidence are reported
by the parent completion run, not inferred from these surface checks.
