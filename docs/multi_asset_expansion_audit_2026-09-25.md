# Multi-Asset Expansion — Phase 1 Repository Audit

- Baseline: `main` @ `9316a862a7ff0f4e9ebb67e6bed85608d3d059d1` (verified: `origin/main` == HEAD)
- Scope: read-only audit. No code changed. No certification threshold, holdout, or CATI execution flag touched.
- Method: static reading of the code plus one scratch import probe of `shared_lib.broker.client_factory` (results in §B/§C). The test suite was **not** run. The container is missing project deps, and the system `cryptography` binding panics on import.

Every endpoint named below as missing or to-be-added is based on vendor API knowledge. It must be re-verified against current Binance, Bybit and BingX docs before Phase 2 code is written.

---

## A. Current architecture

### Broker / exchange layers (there are four, and they overlap)

| Layer | Path | Role | Brokers wired |
|---|---|---|---|
| Canonical auth resolver | `backends/shared/shared_lib/broker/resolver.py` (`resolve_broker_auth`, `BrokerAuth`) | Only sanctioned reader of `broker_credentials_v2`. Checks ownership (`user_id`), status lifecycle, env cross-check and version pointer | broker-agnostic |
| Canonical env/URL table | `shared_lib/broker/environment.py` (`BrokerEnvironment`, `_BASE_URLS`) | One base URL per (broker, LIVE/DEMO) | binance, bybit, bingx, oanda |
| Canonical client factory | `shared_lib/broker/client_factory.py` (`build_client_from_auth`) | Builds the runtime client. **The only path the live runtime uses** (`runner/multi_runner.py:716`) | binance, bybit, bingx (the bingx branch is broken, see §C) |
| Legacy factory | `bot-backend/app/exchange/factory.py` (`build_exchange_client`, `build_generic_exchange_client`) | Context-driven, with hard-coded URLs | binance, bybit, bingx, oanda, ibkr, mt4/5. **Nothing in the runtime calls it** |
| Unified protocol | `bot-backend/app/exchange/interface.py` (`ExchangeClient`, `BrokerCapabilities`) + `app/models/unified_trading.py` (`InstrumentSpec`, `AssetClass`, `OrderRequest`, `SymbolFilters`…) | Broker-neutral adapter protocol | Implemented by `BinanceAdapter`, `OandaAdapter`, `MetaTraderBridgeAdapter`, `IBKRTwsAdapter`. **No Bybit or BingX adapter** (`factory.py:218` TODO) |
| Instrument registry | `app/exchange/registry.py` (`InstrumentRegistry`) | Caches `list_instruments()` per broker (1h TTL) | anything with `list_instruments` |
| Universe | `app/universe/{adapters,engine,contracts,identity,runtime}.py` | Broker-derived tradable universe (`UniverseMode.BROKER`) | `_ADAPTERS = {"binance": BinanceUsdmUniverseAdapter}` only |
| CATI venue economics | `app/trading_intelligence/venue/{registry,binance,reference_adapters,adapter}.py` | Section 17 economic adapters + validation status | binance_usdm (DEMO_VALIDATED). Forex and futures reference adapters are UNVALIDATED. All other brokers get `UnsupportedVenueAdapter` |
| CATI execution adapter | `app/trading_intelligence/execution/{adapter,binance_adapter,boundary,config}.py` | Section 20 boundary, wraps the existing executor | Binance only. Disabled by `CATI_ACTIVE_EXECUTION_ENABLED` (default off) |
| User-backend clients | `backends/user-backend/app/exchange/**`, `core/broker_service.py` | Credential validation (`_test_broker_connection`), catalog, portfolio and transactions | binance, bybit, bingx, oanda, ibkr, mt4/5. coinbase/kraken/alpaca are **mock-success** |

### Execution call surface
`app/execution/executor.py` duck-types a Binance-shaped client. It calls `place_order(OrderRequest)`, `get_order`, `get_order_by_client_order_id`, `get_algo_orders`, `place_protection`, `update_protection`, `user_trades`, `position_risk_all`, `open_orders`, `exchange_info`, `_signed_get` and others. `BinanceFuturesClient` has all of them. `BybitClient` and `BingXClient` do not (§B/§C).

### Account / tenancy model
`users` → `broker_accounts` (`user_id`, `broker_id`, **single** `market_type`, `environment`, `account_type`, `capabilities` JSON, `active_credential_version`) → `broker_credentials_v2` (versioned Fernet blob) → `bot_instances` (`user_id`, `broker_account_id`). Execution evidence is keyed by `bot_instance_id` and `broker_account_id`. The CATI reservation store serialises per `broker_account_id` (`portfolio/reservation_store.py:198`).

---

## B. Existing Bybit capability

| Item | Finding |
|---|---|
| Files | `bot-backend/app/exchange/bybit/{client.py,filters.py,signing.py}`. User-backend has a separate copy at `user-backend/app/exchange/bybit/client.py` and re-exports it via `bybit_client.py` |
| Class | `BybitClient` (V5, HMAC `sign_v5`) |
| Account model | Reads `wallet-balance` with `accountType=UNIFIED` and falls back to `CONTRACT` (`client.py:121-124`) |
| Market scope | Hard-coded `category="linear"`, `settleCoin="USDT"`, `quoteCoin=="USDT"`. Only USDT linear perps. No spot, inverse, option or USDC-perp support |
| Instrument discovery | `exchange_info_cached()` → `/v5/market/instruments-info` is mapped to a Binance-shaped dict. Requests use the default page with **no `limit`/`cursor` pagination**, so the list may be truncated on a large linear universe. There is **no `list_instruments()`**, so `InstrumentRegistry` cannot use it |
| Orders | `place_market_order`, `place_stop_market`, `place_take_profit_market`, `close_position_market`, `cancel_all_orders`, `set_leverage`, `update_protection` |
| Missing vs. executor | **`place_order`, `get_order`, `get_order_by_client_order_id`, `place_protection`, `get_algo_orders`, `user_trades`, `open_orders`, `position_risk_all`, `list_instruments`, `get_prices`**, depth/book/funding/mark-price. The executor calls `self.client.place_order(req)` (`executor.py:1880`), so **a Bybit bot cannot open an entry through the current executor** |
| Balance | `account()` / `get_account_snapshot()` (USDT, UNIFIED or CONTRACT) |
| Positions | `position_risk()` via `/v5/position/list`, remapped to Binance shape |
| Transfers | `get_transfers_history()` is a stub that returns `[]`. No internal-transfer write path |
| Permissions | The user-backend `test_connection()` calls `/v5/user/query-api` and records the flattened permissions as `capabilities`. **Withdraw permission is recorded but never rejected** |
| Demo | `_BASE_URLS[bybit, DEMO] = api-testnet.bybit.com`. Bybit "Demo Trading" (`api-demo.bybit.com`) is a different environment; the choice needs a decision |
| Universe | No Bybit universe adapter, so `UniverseMode.BROKER` bots on Bybit raise `UniverseAdapterUnavailable` |
| CATI | No venue-economic adapter (fails closed) and no execution adapter |
| Factory probe | `build_client_from_auth(bybit)` constructs `BybitClient`. Missing: `place_order`, `get_order_by_client_order_id`, `list_instruments`, `get_order`, `place_protection` |
| FX/TradFi | None. Bybit TradFi is MT5-based and separate from V5. Treat it as unverified |

## C. Existing BingX capability

| Item | Finding |
|---|---|
| Files | `bot-backend/app/exchange/bingx/{client.py,signing.py}`. User-backend has a separate, smaller copy at `user-backend/app/exchange/bingx/client.py` |
| Class | `BingXClient`, USDT-M perpetual swap (`/openApi/swap/v2|v3/...`) |
| Symbol mapping | `_normalize_symbol`: `AVAXUSDT → AVAX-USDT`. It **defaults to `-USDT`** when no known quote is found |
| Instrument discovery | `exchange_info_cached()` → `/openApi/swap/v2/quote/contracts`. No `list_instruments()` |
| Orders | `place_market_order`, `place_stop_market`, `place_take_profit_market`, `close_position_market`, `cancel_all_orders`, `set_leverage` |
| Missing vs. executor | Everything missing for Bybit, **plus `get_position_info` and `update_protection`** |
| Balance / positions | `/openApi/swap/v2/user/balance`, `/openApi/swap/v2/user/positions` |
| Transfers | `get_transfers_history()` stub returns `[]`. No write path |
| Permissions | `test_connection()` hard-codes `["read","trade","futures"]` and **never inspects the key's real permissions** |
| Demo | Both clients **ignore `testnet=True`** and default to mainnet `open-api.bingx.com` when no `base_url` is passed. This affects user-backend validation (`broker_service._test_broker_connection`) and anything else that omits `base_url` |
| **Runtime blocker** | `shared_lib/broker/client_factory.py::_build_bingx` imports `app.exchange.bingx_client`, and **that module does not exist in either backend**. Every BingX bot fails at client construction with `REASON_AUTH_FAILED`. Confirmed by the import probe |
| Universe / CATI | No universe adapter, no venue-economic adapter, no execution adapter |

## D. Existing Binance capability

| Item | Finding |
|---|---|
| Files | `bot-backend/app/exchange/binance/{client.py(1135 LOC),adapter.py,filters.py,signing.py}`. User-backend has a copy plus a `binance_client.py` wrapper |
| Classes | `BinanceFuturesClient` (USDⓈ-M futures, `/fapi/*`) and `BinanceAdapter(ExchangeClient)` |
| Scope | USDⓈ-M futures only. No `/sapi` (wallet), `/api/v3` (spot) or `/dapi` (COIN-M) in the runtime client. `scripts/acquire_market_data.py` uses **spot** `api.binance.com/api/v3/klines` for research data |
| Discovery | `exchange_info_cached`, `list_instruments()` (emits `asset_class="crypto_perp"` for every symbol, `max_leverage=125` hard-coded, no `contractType` filter). `BinanceUsdmUniverseAdapter` does classify `contractType` |
| Orders | Full set: `place_order` (clientOrderId), algo-order protection (`/fapi/v1/algoOrder`), `update_protection`, get/cancel, fills (`userTrades`), `income` |
| Market data | klines, `historical_klines`, `premiumIndex`, `bookTicker`, `depth`, `fundingInfo` |
| Balance | `/fapi/v2/account`, `/fapi/v2/balance` |
| Transfers | `get_transfers_history()` stub returns `[]`. User-backend `transaction_service` reads `/fapi/v1/income?incomeType=TRANSFER` and **labels every positive row DEPOSIT and every negative row WITHDRAWAL**, although these are internal wallet↔futures transfers. No universal-transfer (`/sapi/v1/asset/transfer`) support |
| Permissions | User-backend `BinanceClient.test_connection` reads `canTrade/canDeposit/canWithdraw` from the futures **account** (an account flag, not an API-key permission). `/sapi/v1/account/apiRestrictions` is never called, so a withdraw-enabled key is accepted |
| Demo URL mismatch | Canonical DEMO URL is `demo-fapi.binance.com`, but user-backend validation uses `testnet.binancefuture.com` (`user-backend/app/exchange/binance_client.py:12`). A demo account can validate against one environment and trade against another |
| CATI | `venue/binance.py` (`BinanceUsdmEconomicAdapter`, DEMO_VALIDATED) and `execution/binance_adapter.py` (wraps the executor) |

### Other brokers already present (do not duplicate)
- **OANDA:** `app/exchange/oanda/{client,adapter,mapping}.py`, a complete `ExchangeClient` adapter. It is absent from `shared_lib/broker/client_factory.py`, so it is not reachable by the runtime.
- **MT4/MT5 bridge:** `app/exchange/mt_bridge/*`, `backends/mt-bridge/*`, and `app/exchange/forex/mt5_bridge/interface.py`. Also absent from the canonical factory.
- **IBKR:** `app/exchange/ibkr/*` (web gateway) and `app/exchange/ibkr_tws/*` (TWS), which are two parallel implementations. Absent from the canonical factory.
- Coinbase, Kraken and Alpaca are **catalog entries only**. `_test_broker_connection` returns mock `success: True`, so the account becomes `connected` with no client behind it.

---

## E. Existing CATI multi-asset capability

| Component | FX / multi-asset state |
|---|---|
| `contracts/instrument.py` `InstrumentKey` | Asset classes `CRYPTO, FUTURES, FX, EQUITY, OPTION`; contract types `PERPETUAL, FUTURE, SPOT, CFD, OPTION`. `from_fx_pair`, `instrument_key_for(FX)` (6-letter pair). **Ready** |
| asset_class resolution | `integration/cycle_shadow.py::_asset_class` and `portfolio/exposure_builder.py` map `context.market_type` to one class **per bot**. There is no per-instrument asset class, so a single bot cannot mix crypto and FX |
| Sessions | `_session_open` reuses `symbols/market_hours.py::ForexSessionGuard`. FX weekends and rollover are handled, and unknown sessions fail closed. **Present** |
| Setups (`setups/*`) | Asset-agnostic OHLCV specialists. No FX-specific calibration |
| Forecast / outcome library | Built from `historical_candles` (crypto, Binance). No FX library exists, and `RUNTIME` mode refuses an unhashed library, so FX forecasts would be `OUTCOME_LIBRARY_UNAVAILABLE` (fail closed) |
| Economics / venue economics | One cost contract. `VenueCostPolicy` has FX fallbacks for spread, slippage and reference notional, and FX financing (swap/rollover). `ForexReferenceAdapter` and `DatedFuturesReferenceAdapter` exist but are **UNVALIDATED**, so they fail the Section 13 cost-quality gate. No real FX broker feeds them |
| Veto / ranking / portfolio selection | Asset-class labels propagate. `portfolio/factors.py` supports FX currency-leg factors (`FX:CURRENCY:<CODE>`). **Structurally ready** |
| Reservation | Per `broker_account_id`, asset-agnostic |
| TradePlan | `trade_plan/builder.py:254` keys order preferences and TIF by asset class. Needs FX entries in policy |
| Risk | The existing orchestrator. Sizing (`symbols/sizing.py`) has forex branches |
| Execution | Binance-only adapter. `UnvalidatedExecutionAdapter` elsewhere. **Disabled** (`CATI_ACTIVE_EXECUTION_ENABLED` unset). The runner never calls the boundary |
| Exits (Section 19) | Asset-agnostic. Routing is gated by `CATI_EXIT_INTENT_ROUTING_ENABLED` (off) |
| Research / certification (Section 22) | Venue hard-coded to `BINANCE_USDM` (`pipeline.py:54,83,349`). `OUT_OF_SCOPE = {"FOREX": "BLOCKED_DATA…", "FUTURES": "BLOCKED_DATA…"}`. The certification CLI reads `historical_candles WHERE market_type='crypto'` |
| Governance (Section 25) | `scope_hash(... asset_class ...)` already scopes promotion per asset class. Phases M0–M9 are unchanged |
| ML | `ml/contracts.py: supported_asset_classes = ("CRYPTO",)` |

**FX verdict:** the identity, session, cost-contract, factor and governance scaffolding for FX exists. Nothing is FX-*operational*. There is no reachable FX broker in the runtime factory, no validated FX economic adapter, no FX history, no FX outcome library and no FX certification scope. Per-bot (not per-instrument) asset class blocks mixed portfolios.

---

## F. Existing dataset capability

| Item | State |
|---|---|
| `historical_candles` (`shared_lib/persistence/migrations.py:1092`) | OHLCV plus `market_type` (default `crypto`), `base/quote_currency`, `data_source`, `data_version`. `UNIQUE(symbol, interval, open_time, data_source, market_type)`. **There is no `venue`/`instrument_type` column**, so Binance USDM, Bybit linear and Binance spot rows for `BTCUSDT` are only distinguishable by the free-text `data_source` |
| Backfill | `bot-backend/scripts/ml/backfill_historical_candles.py`: Binance **fapi**, default 5 symbols, `--symbols` list, idempotent |
| Research acquisition | `scripts/acquire_market_data.py`: Binance **spot** `/api/v3/klines` at 1m, deterministic `derive()` to 5m/15m/1h/4h (`app/research/dataset.py`), with manifests and quality checks |
| Datasets on disk | `data/research/binance_1m_btcusdt[_ethusdt]_*.manifest.json` (BTC and ETH only, 2y) |
| Certification dataset | `research/certification/dataset.py` + `cli.py`: provenance from `historical_candles.data_source` |
| Funding | Live only (`premiumIndex`/`fundingInfo` via the CATI venue adapter). The replay cost model uses constants. **No historical funding table** |
| Open interest | Live snapshot fields only (`market_state/derivatives.py`, symbol scoring). **No history** |
| Liquidations | **None** |
| Order book / spread / bid-ask | Live `bookTicker`/`depth` only (universe spread filter, CATI venue quote). **No history** |
| FX / reference feeds | None. `api/forex_instruments.py` lists instruments with a fallback config. The event calendar is flagged stale by certification |

Gaps against the targets:
- **A. 100+ crypto instruments.** Candle backfill works per symbol but is serial and Binance-only. It has no venue column and no funding, OI or spread history. Default settings cap at `MAX_SYMBOLS=2`, `RUN_MAX_SYMBOLS=10`, `UNIVERSE_ACTIVE_LIMIT=100`, and `parse_symbols(max_symbols=100)`.
- **B. FX.** Nothing: no source, schema usage (`market_type='forex'` is allowed but unused), calendar or financing history.
- **C. Multi-venue.** Not representable without a venue dimension in `historical_candles`, and no Bybit/BingX kline backfill exists.

---

## G. Existing account-transfer capability

| Capability | Binance | Bybit | BingX |
|---|---|---|---|
| Internal transfer (write) | **None** | **None** | **None** |
| Transfer history (read) | `/fapi/v1/income TRANSFER` only (mislabeled deposit/withdraw) in user-backend | stub `[]` | stub `[]` |
| Wallet/account-type balance query | futures only | UNIFIED/CONTRACT wallet-balance | swap balance only |
| Transfer status / reconciliation | None | None | None |
| Storage | `broker_transfers_cache` (`shared_lib/persistence/db.py:616`, `UNIQUE(broker_account_id,type,raw_id)`) is **read by `api/equity.py:435` but written by nothing** | same | same |
| Idempotency / audit | `broker_audit_log` exists (generic). No transfer-intent table | — | — |

Vendor endpoints to integrate in Phase 2 (verify first):
- **Binance:** `POST /sapi/v1/asset/transfer` (universal transfer; types such as `MAIN_UMFUTURE`, `UMFUTURE_MAIN`, `MAIN_FUNDING`, `FUNDING_UMFUTURE`), `GET /sapi/v1/asset/transfer`, `GET /sapi/v1/account/apiRestrictions`. These need the **spot/SAPI base URL (`api.binance.com`)**, which `_BASE_URLS` lacks. Demo support for SAPI transfers is unverified.
- **Bybit:** `POST /v5/asset/transfer/inter-transfer` (`fromAccountType`/`toAccountType` FUND/UNIFIED/CONTRACT/SPOT; caller-supplied UUID `transferId` gives natural idempotency), `GET /v5/asset/transfer/query-inter-transfer-list`, `GET /v5/asset/transfer/query-account-coins-balance`, and `/v5/user/query-api` (permission `Wallet: AccountTransfer`).
- **BingX:** the asset transfer endpoint (fund ↔ USDT-M perp ↔ spot ↔ standard futures) and its history, plus an API-key permission query. The exact paths and version need confirming from current BingX docs.
- TradFi/FX wallets (Bybit TradFi/MT5, Binance TradFi perps, BingX TradFi) are **unverified**. They must be discovered from each venue's account-type list rather than assumed.

---

## H. Gaps (ranked)

**P0: correctness or security, independent of expansion**
1. **BingX runtime is broken.** `client_factory._build_bingx` imports the non-existent `app.exchange.bingx_client`.
2. **Bybit and BingX cannot execute through the current executor** (no `place_order`, `get_order*`, `place_protection`, and more). Nothing gates bot creation on this, so these bots start and then fail at entry.
3. **Unauthenticated legacy platform-key endpoints.** `POST /runner/live/start` and `/runner/live/stop` (`bot-backend/app/main.py:1554,1607`) have no auth dependency. `/binance/leverage`, `/binance/cancel-all`, `/binance/balance`, `/binance/order` accept **any** authenticated user and act on the operator's global `settings.BINANCE_API_KEY`. This is a cross-tenant control surface.
4. **Withdraw-enabled keys are accepted.** Binance only reads the account-level `canWithdraw`, Bybit records permissions without rejecting, and BingX checks nothing.
5. **Mock validation.** Coinbase, Kraken and Alpaca become `connected` without any real validation.
6. **Encryption key fallback.** `broker_security._get_fernet_key` silently falls back to `sha256(settings.SECRET_KEY)` or to the constant `b"0"*32`. Each backend loads its own `app.core.config.settings`, so the two backends can derive different keys (the bot-backend then fails to decrypt), or both can use the all-zeros key. There is no fail-closed check in production.
7. **Credential reads that bypass the resolver.** `user-backend/core/transaction_service.py`, `broker_service.get_decrypted_credentials` and `analytics/daily_snapshot_scheduler.py` read the **legacy** `broker_credentials` table, which is stale after a v2 rotation. `daily_snapshot_scheduler` also imports the non-existent `build_exchange_client_from_broker`, filters on `status='active'` (accounts are `connected`), and defaults to mainnet URLs. Daily snapshots therefore silently record nothing.
8. **Demo URL divergence** between validation and runtime (Binance testnet vs. demo-fapi, BingX mainnet).

**P1: expansion blockers**
9. Universe adapter exists for Binance only.
10. No `list_instruments()` / `InstrumentSpec` for Bybit or BingX. Bybit discovery is unpaginated.
11. The canonical factory lacks OANDA, MT and IBKR, although full adapters exist.
12. `broker_accounts.market_type` is single-valued and the bot asset class is per-bot, so there is no per-instrument asset class.
13. There is no wallet/account-type model (FUND/SPOT/UNIFIED/FUTURES/TRADFI) and no transfer service, intent table, permission model or reconciliation.
14. `historical_candles` has no venue dimension, and there is no funding, OI, spread or liquidation history.
15. CATI certification is hard-wired to `BINANCE_USDM`, and ML is `CRYPTO` only (correctly fail-closed; keep that until certified).

**P2: hygiene**
16. Duplicated client code between `user-backend/app/exchange/*` and `bot-backend/app/exchange/*`.
17. `InstrumentRegistry` writes debug prints to stderr. `BinanceFuturesClient.list_instruments` hard-codes `max_leverage=125`.
18. 81 ad-hoc scripts in the `bot-backend/` root, several of which read credentials directly (`close_btc_raw.py`, `check_api_creds*.py`, `migrate_broker.py`).
19. Exchange HTTP errors (requests `HTTPError`, BingX `RequestException`) embed full signed URLs in exception text. These contain the signature, not the key, so the risk is low, but a redaction filter is still needed.

## I. Files that need modification (Phase 2 candidates)

- `backends/shared/shared_lib/broker/client_factory.py`: fix the BingX import. Later, register OANDA, MT and IBKR.
- `backends/shared/shared_lib/broker/environment.py`: add per-broker **wallet/asset base URLs** (e.g. Binance SAPI) next to the trading URLs.
- `backends/shared/shared_lib/broker/resolver.py`: no logic change. Consumers of `BrokerAuth` gain capability flags.
- `backends/shared/shared_lib/core/security/broker_security.py`: fail closed in production without `BROKER_SECRET_KEY`.
- `backends/shared/shared_lib/persistence/migrations.py`: new `broker_wallet_transfers` (intent + status) table, a `broker_api_permissions` column or table, and additive `historical_candles.venue`/`instrument_type` columns with a new unique index.
- `backends/bot-backend/app/exchange/bybit/client.py`, `bingx/client.py`: add `list_instruments`, pagination, the Binance-parity execution surface and wallet/transfer methods.
- New `backends/bot-backend/app/exchange/bybit/adapter.py`, `bingx/adapter.py` (`ExchangeClient`), following `binance/adapter.py`.
- `backends/bot-backend/app/exchange/binance/client.py`: add a SAPI sub-client (or a separate wallet client) for transfers and `apiRestrictions`.
- `backends/bot-backend/app/universe/adapters.py`: register Bybit and BingX universe adapters.
- `backends/bot-backend/app/main.py`: add auth, or admin-only scoping, to the legacy platform-key endpoints.
- `backends/bot-backend/app/analytics/daily_snapshot_scheduler.py`: route through `resolve_broker_auth` and `build_client_from_auth`.
- `backends/user-backend/app/core/broker_service.py`: real permission inspection, withdraw-permission rejection, remove the mock successes, use canonical URLs.
- `backends/user-backend/app/core/transaction_service.py`: use the resolver and correct transfer labelling.
- `backends/user-backend/app/exchange/{binance_client.py,bingx/client.py}`: canonical demo URLs.
- New user-backend API `api/transfers.py` plus the frontend page. New bot-backend or shared `transfers/` service.

## J. Files that must not be duplicated

- Broker auth and URLs: `shared_lib/broker/{resolver,environment,client_factory}.py`. **No second resolver, URL table or factory.**
- Exchange clients: `app/exchange/{binance,bybit,bingx,oanda,mt_bridge,ibkr,ibkr_tws}/`. Extend these in place. Do not add `bybit_v2/` or similar.
- Protocol and models: `app/exchange/interface.py`, `app/models/unified_trading.py` (`AssetClass`, `InstrumentSpec`).
- Identity: `app/universe/identity.py` (`CanonicalInstrument`) and CATI `contracts/instrument.py` (`InstrumentKey`, `from_fx_pair`).
- Universe: `app/universe/adapters.py` (use `register_adapter`).
- CATI venue economics: `trading_intelligence/venue/{registry,adapter,reference_adapters,policy}.py`.
- CATI execution: `trading_intelligence/execution/{adapter,boundary,config}.py`.
- Session guard: `app/symbols/market_hours.py`.
- Research data: `scripts/acquire_market_data.py`, `app/research/dataset.py::derive`, `scripts/ml/backfill_historical_candles.py`.
- Evidence and certification: `trading_intelligence/research/certification/*` (policy, gates and holdout registry are frozen).
- Security: `shared_lib/core/security/broker_security.py` (`user-backend/app/core/broker_security.py` is already a re-export shim).
- Transfers storage: reuse and extend `broker_transfers_cache` for history. Do not create a parallel history table.

## K. Proposed minimal implementation plan

1. **Phase 2a: repair and safety (no new features).**
   - Fix the BingX factory import.
   - Gate bot start on a declared `execution_supported` capability per broker, so Bybit/BingX bots fail closed with a reason code instead of an AttributeError.
   - Close the unauthenticated or any-user legacy platform-key endpoints.
   - Enforce `BROKER_SECRET_KEY` in production.
   - Remove the mock-success validations (mark those accounts `unsupported`).
   - Unify demo URLs through `resolve_base_url`.
   - Re-route the three resolver bypasses.
2. **Phase 2b: API permission model.** Add a `BrokerPermissionProbe` per broker (Binance `apiRestrictions`, Bybit `query-api`, BingX permission endpoint). Normalise results to `{read, spot_trade, futures_trade, internal_transfer, withdraw, ip_restricted}`. Persist them on the credential version. **Reject or quarantine keys with `withdraw=true`**, and require `internal_transfer` only when the user opts into transfers.
3. **Phase 2c: internal transfers (same account only).**
   - Add a `WalletTransferService` with a `TransferIntent` (uuid, user_id, broker_account_id, from_wallet, to_wallet, asset, amount, status `PENDING→SUBMITTED→CONFIRMED|FAILED|UNKNOWN`, broker_transfer_id, idempotency key) and an append-only audit.
   - Add per-broker implementations for Binance universal transfer, Bybit inter-transfer and BingX asset transfer.
   - Reconcile against each broker's transfer-history endpoint, writing into `broker_transfers_cache`.
   - Declare wallet types per broker from vendor docs, with TradFi wallets only where the API genuinely exposes them. No withdrawals, no cross-account moves and no sub-account transfers in v1.
4. **Phase 2d: Bybit and BingX execution parity.** Add `list_instruments`/`InstrumentSpec`, pagination, `place_order` with a client order id, `get_order[_by_client_order_id]`, `place_protection`, fills and open orders. Add `ExchangeClient` adapters and universe adapters. Validate on demo only.
5. **Phase 2e+ (later, out of Phase 2).**
   - Venue column on `historical_candles` and multi-venue backfill.
   - Funding, OI and spread history.
   - Per-instrument asset class.
   - FX broker in the canonical factory (OANDA first, since its adapter exists).
   - FX data, library and certification scope as a **separate** Section 22 scope. The CRYPTO scope and its holdout are not touched.

## L. Risks

- A transfer is a money movement. A bug that sends the wrong direction, amount or asset directly affects user capital, so the design needs idempotency plus UNKNOWN-state reconciliation with no blind retry.
- Asking users for a new "internal transfer" key permission may push some to create over-permissioned keys. The withdraw-rejection rule must land first (2b before 2c).
- Vendor transfer and permission APIs differ in shape and change often. Demo/testnet environments may not support wallet endpoints at all, which limits pre-production validation.
- Moving margin out of futures while positions are open can trigger liquidation. Transfers out of a trading wallet must check free margin and open-position risk, and refuse while a bot on that account is running, or cap at free balance minus a buffer.
- Enabling Bybit/BingX execution changes live behaviour for existing (currently broken) bot rows. Require explicit re-approval.
- Tightening the encryption key can make existing blobs undecryptable if deployments relied on the fallback key. This needs a migration and re-encrypt path, and a check of the key currently used in each environment before any change.
- Section 22 is active. Any change to `historical_candles` uniqueness or `data_source` semantics can alter dataset hashes. Keep schema changes additive and keep certification inputs byte-identical.
- CATI active execution must stay off. None of the Phase 2 work may set `CATI_ACTIVE_EXECUTION_ENABLED` or register non-Binance CATI execution adapters as supported.

## M. Tests required

- **Factory:** `build_client_from_auth` builds binance, bybit and bingx (regression for the BingX import), and an unsupported broker raises `REASON_AUTH_FAILED`.
- **Capability gate:** a bot on a broker without `execution_supported` is refused at start with a reason code.
- **Auth:** `/runner/live/start|stop` and `/binance/*` legacy endpoints return 401 without a token and 403 for a non-admin user.
- **Encryption:** production mode without `BROKER_SECRET_KEY` fails closed, and both backends derive the same key from the same env.
- **Permissions (per broker, recorded fixtures):**
  - A withdraw-enabled key is rejected or quarantined.
  - A trade-only key is accepted.
  - A transfer-disabled key disables the transfer feature only.
- **Transfers:**
  - The idempotency key prevents double submission.
  - A timeout leads to UNKNOWN, then reconcile reaches CONFIRMED or FAILED.
  - Direction and wallet-type mapping per broker.
  - Refusal when the amount exceeds free balance or the margin buffer.
  - Cross-user `broker_account_id` access is denied (resolver ownership).
  - Audit rows are written.
  - Secrets never appear in logs or exceptions.
- **Isolation:** user A cannot list or trigger transfers, balances, positions or fills of user B's account, across all new endpoints.
- **Bybit/BingX parity:** contract tests on recorded payloads for `list_instruments` (with Bybit pagination), `place_order` clientOrderId round-trip, `get_order_by_client_order_id`, protection placement, and symbol normalization.
- **Regression:** the existing CATI Section 22–26 suites and `test_connected_account_execution_safety.py` are unchanged and green. The certification policy hash and freeze hash are unchanged.

---

## AUDIT VERDICT: **PARTIAL**

Bybit and BingX integrations exist, but only as USDT-linear market-order clients. BingX is **unreachable at runtime** because of a broken factory import, and neither can serve the executor's order/fill surface. CATI's FX scaffolding is real but non-operational. There is **zero** internal-transfer capability, and there are security gaps (unauthenticated platform-key endpoints, withdraw-enabled keys accepted, weak encryption-key fallback) that must be closed before any money-moving feature ships.

### Exact recommended Phase 2 scope

Phase 2 = **2a + 2b + 2c** from §K, crypto venues only. Specifically:

1. **Repair and safety.**
   - Fix `_build_bingx`.
   - Add a per-broker `execution_supported` capability and a bot-start gate (Binance true; Bybit and BingX false until 2d).
   - Put admin auth on the legacy `main.py` platform-key and runner endpoints.
   - Make `BROKER_SECRET_KEY` fail closed in production.
   - Remove the mock broker validations.
   - Use canonical demo URLs in user-backend validation.
   - Move `transaction_service`, `get_decrypted_credentials` and `daily_snapshot_scheduler` onto `resolve_broker_auth`.
2. **API-permission model.**
   - Build a normalized permission probe for Binance, Bybit and BingX, persisted per credential version.
   - Reject or quarantine withdraw-enabled keys.
   - Surface the permissions in the broker UI.
3. **Same-account internal transfers for Binance, Bybit and BingX.**
   - Wallet-type catalogue per broker: FUNDING/SPOT/FUTURES/UNIFIED, plus TradFi only where the vendor API exposes it.
   - `TransferIntent` table with idempotency and state machine.
   - Transfer service with free-margin and running-bot safety checks.
   - History reconciliation into `broker_transfers_cache`, with a corrected transfer-vs-deposit classification.
   - User-backend `api/transfers.py` endpoints scoped by `user_id` via the resolver.
   - Append-only audit via `broker_audit_log`.
   - The full §M test set for these three items.

**Explicitly excluded from Phase 2:**
- Cross-broker moves, withdrawals and sub-account transfers.
- Bybit/BingX execution parity (Phase 2d).
- Multi-venue datasets, FX data and FX certification.
- Any CATI execution enablement, certification threshold change or holdout access.
- Removal of any V2 component.
