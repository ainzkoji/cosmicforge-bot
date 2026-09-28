# CATI Sections 16-19 audit and closure record

Baseline: `main@3680dd96` (HEAD == origin/main, clean tree; no delta).

## Background acquisition (not touched)
Observed at start: no acquisition Python process; the FX 1m supervisor (another session) in its provider-refusal
cool-down between rounds; crypto deep / features DB last written 2026-09-27 00:36 (finished). Acquisition writes only
`data/research/*.db` (`ensure_market_data_schema`). No change in this work touches `market_data_schema.py`, the
research DBs, their tables or status enums. Schema changes are in the BOT database only (`cati_schema`,
`broker_schema`), applied by `migrate()`.

## 16.A Runtime ordering

Before (the gap):

| Step | Where |
|---|---|
| candidate + VENUE economics + admission, per symbol | `integration/cycle_shadow.record_symbol` -> `controller/cati_controller.evaluate_symbol` -> `economics/canonical.canonical_economics` |
| transfer economics EXPECTED here (always None -> `TRANSFER_ECONOMICS_UNAVAILABLE` for Bybit/BingX) | `canonical_economics(..., transfer_economics=venue_context.transfer_economics)` |
| whole-universe ranking (once per epoch) | `ranking/coordinator.finalize_bot_cycle` via `cycle_shadow._finalize_epoch` |
| broker_account_id known | bot context (`runner.context.broker_account_id`), fixed per bot |
| portfolio selection + SHADOW reservation | `portfolio/service.select_and_reserve` |
| TradePlan (shadow evidence) | `cycle_shadow.trade_plan_stage` |
| capital planner (Section 9) | `capital/shadow_hook.shadow_capital_routing` -- AFTER selection, evidence only |
| sizing | only at `core/trading_orchestrator.process_trade_plan` (execution boundary) |
| physical transfers | `transfers/service.InternalTransferService.request_transfer` (not called by the cycle) |
| hard risk / order | `execution/boundary.CATIExecutionBoundary` (disabled; governance M0) |

After (Section 17 ordering closes 16.C/16.D):

```
record_symbol: venue economics, transfer component PENDING_ACCOUNT_CAPITAL_PLAN (not final)
_finalize_epoch: WHOLE-UNIVERSE RANKING (unchanged, once)
_portfolio_stage:
  build_account_capital_view   (integration/account_capital_stage)
    - broker-authoritative capital state, read ONCE per epoch (transfer adapter)
    - capital held by active reservations on the account (all bots)
    - per-family eligibility + account kill switch
    consider_account (portfolio/account_capital), in global rank order:
      provisional size (per-trade allocation) -> DRY-RUN plan_capital -> route facts
      -> assess_transfer (economics/transfer) -> FINAL economics (same admission gates)
  select_and_reserve(capital_view=...)
    - non-viable recorded with reasons (never re-ranked, never substituted)
    - selector enforces the capital budget (one basis; family budgets as constraints)
    - reservation stores the capital claim; conflict rechecked in the SAME BEGIN IMMEDIATE txn
  trade_plan_stage (shadow)
  record_capital_evidence (the same plans + topology + balances)
execution boundary (disabled): capital readiness -> account scope -> instrument preflight
  -> hard risk -> quantity preflight -> existing adapter
```

## 17 Portfolio / risk / capital map
Reused: `ranking/coordinator` (global ranking), `portfolio/selector` (+ optional capital budget), `portfolio/service`,
`portfolio/reservation_store` (+ capital columns), `portfolio/exposure_builder` (multi-bot account aggregation),
`capital/planner` (dry run), `risk/capital_ledger.per_trade_allocation_margin` (provisional size),
`shared_lib.broker.capabilities.execution_readiness` (per family), `governance/promotion` (kill switch), existing
hard risk in `core/trading_orchestrator` and executor slot/margin gates. New: `portfolio/account_capital.py`,
`capital/route_facts.py`, `integration/account_capital_stage.py`. The process-local
`risk.capital_ledger.AccountMarginReservations` is unchanged (executor-level affordability); the CATI capital claim is
DB-serialized.

Release semantics: account-rejected -> never reserved; hard-risk / preflight / definitive capital failure / executor
rejection -> RELEASED; expiry -> EXPIRED (sweep); submit unknown -> RESOLUTION_PENDING (never expires, released or
consumed only by broker reconciliation); transfer in flight or unknown -> reservation kept (`CAPITAL_NOT_READY`
pending) and the planner blocks the whole account (`ACCOUNT_RECONCILIATION_REQUIRED`) until the transfer resolves.

## 18 Execution map
TradePlan creation `trade_plan/builder`; boundary `execution/boundary`; adapter selection
`execution/binance_adapter.executor_adapter_for` (the broker-generic executor wrapper for binance / bybit / bingx;
anything else -> `UnvalidatedExecutionAdapter`); metadata / capability `exchange/instruments.InstrumentCatalog` +
`execution_eligibility` + `exchange/catalog_refresh.request_refresh` via `execution/preflight.SubmissionPreflight`;
client ids `execution/executor._build_entry_idempotency` (plan id + hash, no time bucket) -> Binance
`newClientOrderId`, Bybit `orderLinkId` (36), BingX `clientOrderID` (40); submit-unknown / reconciliation / restart
recovery `boundary.reconcile_submit_unknown` / `recover_pending`; fills `execution/fill_resolution`; positions
`adapter.reconcile_position`; protection `executor.ensure_protection` (P7 no widening); transfer recovery
`transfers/reconciliation.TransferReconciler`.

## 19 Persistence mapping

| Required semantic model | Existing table / model | Status | Change |
|---|---|---|---|
| instrument_registry | `venue_instruments` (+ `exchange/canonical_registry`) | EXISTING | none |
| venue_instrument_capabilities | `venue_instruments` (status, api_tradable, last_seen_ms, metadata_hash/changed) + declared `capabilities` profiles | EXISTING | none (venue-global, no account fields) |
| broker_account_capabilities | `broker_credentials_v2.permissions_json` (per credential version) + `broker_accounts.permission_status` / `active_credential_version`; derived via `execution_readiness` per family | EXISTING | none |
| broker_account_topology | live broker read (transfer adapter); persisted per decision in `cati_capital_plan_evidence.topology_json` | EXTENDED | additive column |
| balance_segments | broker-authoritative `adapter.transferable` per native wallet; persisted per decision in `cati_capital_plan_evidence.balances_json` (unknown = null) | EXTENDED | additive column |
| capital_transfer_intents | `broker_transfer_requests` (UNIQUE user+account+idempotency_key) | EXISTING | none |
| capital_transfer_receipts | `broker_transfer_requests` (broker_transfer_id, submitted_at, confirmed_at, failure_reason) + events | EXISTING | none |
| capital_transfer_reconciliation | `broker_transfer_events` (append-only triggers) + `broker_transfer_reconciliations` | EXISTING | none |
| dataset_manifests | `dataset_manifests` / `universe_manifests` (immutable triggers) | EXISTING | lineage-v2 row identity = verified manifest_hash |
| dataset_partitions | frozen partitions inside the immutable manifest payload; acquisition state in `market_ingest_log` / `fx_reference_ingest_log`; reproducible `quality.audit_partition` | EXISTING (derived) | none |
| data_quality_events | `data_quality_events` (runtime), `fx_reference_repairs` (append-only), ingest-log FAILED rows, `market_feature_observations` UNAVAILABLE rows, partition missing-range reasons | EXISTING | none |
| fx_reference_mapping | exposed by `market_data/fx_mapping` from the frozen FX universe + `venue_instruments` | NEW (function, no table) | none |

Status names (repository equivalents): CONFIRMED = `COMPLETED`; RESOLUTION_PENDING = `RECONCILIATION_REQUIRED`;
REJECTED = `BLOCKED` / `FAILED`; CANCELLED is not a broker-internal transfer state.

## Migrations (bot database only; additive, idempotent, WAL-safe)

| Table | Change | Reason |
|---|---|---|
| `cati_portfolio_reservations` | `user_id`, `capital_asset`, `capital_amount`, `capital_wallet`, `capital_json` (TEXT, nullable) | 17.8 capital reservation |
| `cati_capital_plan_evidence` | `topology_json`, `balances_json` (TEXT, nullable) | 19.6 / 19.7 |

No index added (the reserve transaction's account/status index already covers the capital query). No row rewritten.
`historical_candles`, `market_candles` and every research table unchanged.

## External / later
Bybit / BingX: economics adapters and the executor wrapper are UNVALIDATED -> blocked by the boundary; no DEMO
evidence collected (EXTERNAL_VALIDATION_REQUIRED). Physical internal-transfer fees are not published by any verified
venue API (`shared_lib.broker.wallets`), so a segmented account needing a physical move stays
`TRANSFER_COST_UNAVAILABLE` until a validated route fee is supplied.
