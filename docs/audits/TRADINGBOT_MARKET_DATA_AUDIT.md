# Market Data & Multi-Asset / Multi-Broker Audit (Sections 7 and 8)

**Audit:** CosmicForge Tradingbot master end-to-end audit (read-only) · **Date:** 2026-11-13
**Baseline:** `origin/main` = `649080b` · engineering branch `origin/ccr-b5c520cf-80g8x2` = `732e0e2` (7 commits not on main, inspected by diff/worktree only)
**Master report:** [TRADINGBOT_MASTER_END_TO_END_AUDIT.md](TRADINGBOT_MASTER_END_TO_END_AUDIT.md) · **Gap register:** [TRADINGBOT_MASTER_GAP_REGISTER.csv](TRADINGBOT_MASTER_GAP_REGISTER.csv)

Evidence levels used throughout: L1 observed authenticated behaviour · L2 real venue evidence · L3 integration test with real boundaries ·
L4 contract/unit test with mocks · L5 static source inspection · L6 documentation/historical claim · L7 inference.
No production database, VPS or authenticated venue session was available to this audit; nothing below is L1.
Finding IDs are stable and map to the gap register (column "Master ID" consolidates duplicates across areas).

## Lead auditor reconciliation
- The 2026-11-01 Binance `AdapterError` rate-budget root cause is fixed on main (`def0bf4`, endpoint weight catalogue) — confirmed independently by the EXEC auditor (EXEC-14).
- BingX pagination: main still truncates to the newest 1,000 bars; the branch fix `f8401d5` is logically correct (probe: 1,000/3,000 on main vs 3,000/3,000 on branch). No production caller uses BingX history today, so impact is research-only (DATA-01, P3).
- The two shared-budget gaps (DATA-02, DATA-04) are P1 for real-money operation because a mainnet IP ban or a DEMO-host 429 can block protection/flatten traffic.

---
## DATA audit: market data and multi-asset / multi-broker architecture (audit sections 7 and 8)

Auditor area: DATA (prefix DATA). Repo at origin/main 649080b. Branch origin/ccr-b5c520cf-80g8x2 inspected without checkout.

### 1. Scope and method

- Static tracing from production entry points: `deploy/systemd/cosmicforge-trading.service` runs `uvicorn app.main:app`, then
  `app/main.py:_startup_background_jobs` (l.981) calls `production_runtime.run` (settings.production, APP_ENV=PRODUCTION
  in every non-test deploy profile), which starts `ShadowScheduler` and `VenueRuntime` and then `_broker_loop`
  (`trading_intelligence/integration/production_runtime.py:809-917`). Also `_startup_notifications` (l.899: news, events,
  reaction workers).
- Read: app/market_data/*, app/data, app/markets, app/universe, app/exchange (instruments, catalog_refresh, platform_adapter),
  shared_lib/broker/{http_budget,rate_budget,environment,client_factory}, trading_intelligence/{integration,pipeline,venue},
  scripts/acquire_*.py, data/research, docs/research/datasets, the multi-asset audit and STEP7 docs (claims only).
- Tests I ran (venv, APP_ENV=TEST): `tests/test_phase4_market_data.py tests/test_cati_forward_observe.py
  tests/test_cati_residual_prospective.py tests/trading_intelligence/test_step3_daily_feed.py tests/exchange/test_rate_budget.py`
  on main: **94 passed** (L4). The branch's `tests/test_phase4_market_data.py`, from a `git archive` export into the scratchpad:
  **14 passed** (L4). I also ran my own probe of main against branch `bingx_klines` on a simulated newest-first venue (L4, below).
- No exchange was contacted.

### 2. Analysis

#### 2.1 Inventory: data sources and production consumers

**Wired to production (started by cosmicforge-trading.service, only in the process holding the runtime lease, and not under COSMICFORGE_TEST_MODE):**

| # | Data | Venue/host | Collector (file:lines) | Transport / cadence | Budgeted? | Storage | Production consumer |
|---|---|---|---|---|---|---|---|
| P1 | 15m OHLCV, 138 symbols (BTC, ETH + 136 frozen residual universe), 28-day window | fapi.binance.com (MAINNET public) | `integration/residual_prospective.py` `PublicCandles` l.281-298, `refresh_inputs` l.304-334, `collect` l.373-423, `schedule` l.426 (every 60 s from `_broker_loop`) | REST, 0.5 s pacing, own `requests.Session` | **NO**: bypasses the shared budget | `cati_residual_inputs` (pruned to WINDOW+8 h, l.419) | Hourly decisions `cati_residual_decisions` of **RESIDUAL_MOMENTUM_PORTFOLIO_TOP1** (research FAILED, per family_markets.py note). Its ATR and cost rates feed `production_execution.prepare_submission` (l.1243-1247). Its tracker heartbeat is the only `market_data_status` in `/health` (production_runtime.py:555-575) and the only market-data input to `runtime_supervisor` (COLLECTION_STALLED, l.224-231). |
| P2 | Daily OHLCV, daily funding, exchangeInfo snapshot, about 900 USDT perpetuals since 2020 | data.binance.vision archive (+S3 listing); exchangeInfo from fapi.binance.com | `market_data/daily_feed.py` `BinanceArchiveFetcher` l.197-308, `DailyBarFeed.update` l.380-466; production wiring `platform_runtime.ShadowScheduler._initialize` (`default_daily_bars(..., http_get=budgeted_archive_get)`), `auto_update=True` | REST; each 30 s ShadowScheduler tick asks for unsealed days | exchangeInfo: yes (MARKET_DATA class). Archive CDN: no (separate host, by design) | `cati_feed_*` tables (append-only, revisions table, sealed days) | DAILY_TREND shadow cycles (`pipeline/daily_trend.py:562`, `PlatformCycleDriver`) |
| P3 | Depth top-20, premiumIndex (mark, index, funding, next funding), openInterest, aggTrades(1000), BTCUSDT+ETHUSDT | fapi.binance.com | `integration/forward_observe.py` `collect` l.38-122, every 5 min | REST via `public_get` | yes (MARKET_DATA, ceiling 50%) | `market_feature_observations` (full raw book / trade payload stored in `value_json` for every feature row) | Forward observation only (`FORWARD_OBSERVE_ONLY`). No decision consumer found. |
| P4 | Per-account: exchangeInfo (cached), 15m klines(250), last price, depth(100), account, positionRisk, open orders, algo orders, income, commissionRate | account venue host (demo-fapi / fapi / Bybit / BingX via `resolve_base_url`) | `production_execution.prepare_submission` l.1204-1260; `production_runtime.sync_account` | REST via `build_budgeted_client` | yes | decision / evidence tables | Entry sizing, slippage gate, protection, reconciliation |
| P5 | Private user-data stream (orders, fills) | fstream / demo-fstream, Bybit, BingX | `VenueRuntime` + `platform_adapter/session.py`, `user_stream.py` | WebSocket (the only websocket; no market-data websocket exists) | n/a | reconciliation | Account reconciliation |
| P6 | Instrument catalog (tick, step, minQty, minNotional, status, onboardDate) | per venue | `exchange/instruments.py` parsers l.236-330; `catalog_refresh.py` (refresh requested on instrument errors) + `activation.account_status.refresh_if_stale` | REST | via client | `venue_instruments` | Activation, preflight, dispatch |

**Present but not in the production path:**

| Source | File | Status |
|---|---|---|
| Binance/Bybit/BingX historical klines, funding, OI | `market_data/venue_history.py` | Used only by `scripts/acquire_multi_asset_dataset.py`, `build_crypto_research_universe.py`, `acquire_fx_reference_dataset.py`. Unbudgeted `requests_getter` (l.34). |
| 1m deep dataset, mark/index klines, funding | `scripts/acquire_crypto_deep_dataset.py` | Research CLI |
| Dukascopy FX bid/ask minute/hour | `market_data/fx_reference.py`, `scripts/acquire_fx_reference_dataset.py` | Research CLI. Production `_broker_loop` calls `residual_simulation.ensure_fx_watcher` (l.514-531), which **spawns `scripts/fx_finalization_watch.py` as an unsupervised subprocess** when `data/research/fx_reference_dukascopy.db` exists. |
| Multi-timeframe fetcher | `app/data/multi_timeframe_fetcher.py` | Test-only (no production import) |
| Market calendars, contract specs, roll, economics, guard | `app/markets/*` | Read-only `/markets` API and deployment blocker text only. Not wired into execution (STEP7 doc says the same). |
| Universe engine, dynamic universe, market snapshot | `app/universe/*`, `symbols/dynamic_universe.py`, `market_data/market_snapshot.py` | Called from the legacy `runner/runner.py` (PaperRunner); in production, `runner_loop` builds `RuntimeOwnerHost`, not the legacy runner. |
| News (RSS, CryptoPanic, Benzinga, Reuters), economic events, market reaction | `app/news`, `app/events`, `app/workers` | Started in `_startup_notifications`, but every flag defaults to False (`config.py` l.512, 528, 554, 590). No production decision consumer. |
| Liquidations | none | Reported as unavailable (`forward_observe` l.117-119; `venue_history.liquidations`) |
| MT bridge, OANDA, IBKR | `app/exchange/{mt_bridge,oanda,ibkr*}`, `backends/mt-bridge` | Not buildable by `shared_lib/broker/client_factory.py` (l.44: binance, bybit, bingx only). MT bridge is one account per Windows instance (README). |

#### 2.2 Data quality controls

- **Residual 15m inputs (P1):** strong per-row checks (`refresh_inputs` l.318-325): alignment, close time = open+Q-1, closed bars
  only, finite and positive values, OHLC envelope, unique and ascending. `INSERT OR IGNORE` dedups. Gaps are not filled; missed
  hourly boundaries are committed as explicit skips (l.389-393). Weakness: a per-symbol failure is recorded in
  `input_errors`, but the heartbeat still says `COLLECTING`, so `/health` reports market data fresh even if every symbol failed.
  Example: a geo-block (HTTP 451) or DNS failure on fapi.binance.com (DATA-06).
- **Daily feed (P2):** the strongest controls in the repo. Archive SHA-256 per file with one retry (`_file` l.227-241), the dataset's
  own bar rule, rejections table, append-only rows with a revision table (corrections are recorded, never applied), sealed
  served days, a coverage threshold of 90% plus `settle_ms` 6 h, a dormant-contract probe, a circuit breaker, `max_requests_per_update` 5,000, and a
  holdout refusal. The module states "NOT VERIFIED AGAINST THE LIVE ENDPOINTS in this build" (l.205-206). The demo activation
  report claims it runs (L6). Gap: a day the archive has not yet published (404, not counted as a failure) is re-requested
  for every active symbol on each 30 s tick until it appears (`_plan` l.501-523; driver `WAITING_FOR_DATA` is non-terminal),
  with no backoff (DATA-08, L5/L7).
- **Forward observation (P3):** validates book levels, crossed books, future trade timestamps, mark/index > 0 and negative OI.
  Unavailable values are recorded with a reason, never as zero. Retention is the gap (DATA-03).
- **venue_history (research):** `_dedupe`, `check_series` (base gap / duplicate / OHLC / alignment / listing bounds).
  - On **main**, `check_series` cannot detect a series cut short at either end of the requested range, because a short page leaves no internal gap.
  - `binance_open_interest` (l.87-96) makes one `limit=500` call with no pagination: 30 days of 1h (720 rows) is silently truncated (DATA-11).
  - Bybit and BingX error envelopes (HTTP 200 with non-zero code) read as "no data" on main.
- **BingX pagination fix f8401d5 (branch only):**
  - **Main bug confirmed.** `venue_history.py:151-168` sends `endTime=end_ms` with `limit=1000`. BingX answers with the newest 1,000 bars of the range, and the loop then sets `cursor = last + step`, which is past `end_ms`, and exits. Only the newest page is kept.
  - **Probe (3,000 1m bars):** main returned 1,000 rows in 1 call. The branch returned 3,000 rows in 3 calls, first and last bar correct.
  - **Loop termination:** correct. `cursor` advances by exactly `step*limit` per iteration (`cursor = window_end + 1`), so the loop always ends.
  - **Overlap and duplicates:** none. Windows `[c, c+step*limit-1]` are disjoint, and `_dedupe` plus the final range filter remove any residue.
  - **endTime handling:**
    - With an aligned start, each interior `window_end` is unaligned, so inclusive and exclusive endTime give the same result.
    - At the final window, an aligned `end_ms` with an exclusive venue loses the last bar. The probe showed 2,999 of 3,000. That loss is caught only when the caller passes `expected_end_ms` (the branch's acquire script does, 15m only).
    - An unaligned start correctly yields the first aligned bar.
  - **Empty windows** (halts, before listing) move on instead of stopping the walk, which is correct.
  - **Error envelopes** now raise `VenueDataError`.
  - **Residual weaknesses:**
    - The fetcher does not compare its rows with the bars expected per window: in the probe, a venue that caps at 500 silently drops 500 bars per window. `check_series` sees those as internal gaps, so it is detectable but not self-reported.
    - No pacing or 429 handling in the loop.
    - The live evidence (`evidence/bingx-public-market-data-20261112/bingx_live_klines.json`) is a summary of 24 series, not raw payloads (L2-, summary).
  - **Customer and production impact is nil.** `bingx_klines` has no production caller. It affects only research datasets built with the BingX fetcher, and I found no frozen BingX dataset in `docs/research`.
- **Timestamp normalization:** ms everywhere. Daily dataset: day-boundary check, other units converted or refused (QUALITY.md, L6).
- **Venue identity:** `SeriesId(venue, environment="REAL")` in forward_observe. The residual source is labelled `BINANCE_PUBLIC_RESIDUAL_PROSPECTIVE_V1` and `reference_venue_only`.
- **Licensing and provenance:** archive and file hashes are recorded. No licensing record exists anywhere for Dukascopy (redistribution and commercial terms) or for Binance/Bybit/BingX market-data reuse in a customer product (grep `licen[cs]`: only a SPEC D-row placeholder) (DATA-14).
- **Symbol mapping:**
  - Canonical symbols come from venue base/quote (`instruments.py`). `contract_multiplier` is always 1.0, so 1000-prefixed contracts (1000PEPE, 1000SHIB) are not normalized across venues.
  - BingX `qty_step` is taken from the contract `size` field (l.326). I did not verify that field's meaning (DATA-15).

#### 2.3 Continuous acquisition, rate budgets, starvation

- **Collectors started in production:** yes. P1, P2, P3 and P4/P5 all start from `cosmicforge-trading.service`. P1 and P3 are scheduled from `_broker_loop` (production_runtime.py:893-899). P2 is run by ShadowScheduler.
- **Backfill:**
  - P2 backfills from 2020 through monthly and daily archive files, then increments.
  - P1 warms 28 days per symbol on cold start: about 138 x 3 pages at 2 requests/s, which is 3-4 minutes and about 600-1,200 weight/min.
  - P3 has no backfill, by design.
- **Recovery:** P1 records missed hours as skips, not retrospectively. P2 re-fetches missing days. There is no market-data websocket; all collection is REST polling.
- **Shared budget (`shared_lib/broker/http_budget.py`, `rate_budget.py`):**
  - The budget is a persistent sliding log per (venue, egress scope). Binance usable capacity is 2400 x 0.8 = 1,920 per minute.
  - Each class may fill the total only up to its ceiling (cumulative): EMERGENCY 1.0, PROTECTION 0.95, RECONCILIATION 0.80, ENTRY 0.65, MARKET_DATA 0.50 (`rate_budget.py:47`).
  - Venue-reported `X-MBX-USED-WEIGHT-1M` above the local sum is added as OBSERVED. A 429 or 418 closes the budget for every class.
- **AdapterError incident (2026-11-01), verified fixed on main (L5; L6 for the measurements):**
  - Root cause: a flat 100 units per Binance request. One account cycle (about 8 requests, roughly 800 units) plus a client rebuild after each refusal exhausted the reconciliation share, and the collector was refused 853 times (`TRADINGBOT_DEMO_ACTIVATION_REPORT.md` §1).
  - Fix def0bf4, present on main: the endpoint-weight catalogue `BINANCE_ENDPOINT_WEIGHTS` (http_budget.py:36-48), unknown paths fail closed, and `client_doubt` no longer rebuilds the client on a local refusal (production_runtime.py:87-101).
  - I checked every Binance REST path literal in the production client, adapter, venue and executor code against the catalogue. Only `/fapi/v1/algoOrders` is missing, and it is used only on an executor fallback path (`executor.py:3188`). That path is dead when the client has `get_algo_orders`, which the production client does.
- **Starvation analysis (L5/L7):**
  - Market data can never take capacity from reconciliation (it stops at 50%). The reverse is designed in: reconciliation can starve market data.
  - At the measured about 280 units/min per account, roughly 3.5 accounts push the total past 960 (50% of 1,920). From then on, forward observation and the feed's exchangeInfo are refused on that egress scope.
  - The Binance reconciliation ceiling (1,536) supports about 5 accounts per egress IP.
  - BingX is configured at 20 requests per 10 s (UNVERIFIED). That is about 77 reconciliation requests/min, so about 3-4 BingX accounts per IP.
  - Net: account reconciliation is protected, and data starvation is possible beyond a handful of accounts per IP (DATA-07).
- **Budget leaks and couplings:**
  - (a) P1 `PublicCandles` bypasses the budget on fapi.binance.com. Its 418/429 is not written to the shared budget, so other mainnet traffic keeps sending into a ban (DATA-02).
  - (b) The budget key is (venue, egress scope), not host or environment. demo-fapi and fapi (separate venue limits) share one log, and a 429/418 on either host closes the budget for all Binance traffic, LIVE protection and emergency included (DATA-04).
  - (c) The legacy `BinanceFuturesClient` market-reaction client in main.py is unbudgeted but disabled by default.
- **Storage growth:**
  - `market_feature_observations` is classified REFERENCE_MARKET_DATA with no prune rule (`pipeline/retention.py:196-199`, `PRUNE_RULES` covers only cycle metrics and equity snapshots).
  - forward_observe stores the full 1,000-trade payload in two feature rows and the full book in seven, for 2 symbols, every 5 minutes. My estimate is about 0.4 MB per cycle, about 120 MB/day, about 40+ GB/year in the trading SQLite DB and every backup (L7 estimate from the code) (DATA-03).
  - The `cati_feed_*` tables grow slowly (about 900 rows/day). `cati_residual_inputs` is bounded.
- **Scheduling of research refresh:** no systemd unit or timer runs any acquisition script. `feed-update` exists as a CLI but no unit calls it; it is not needed because auto_update runs inside the service.

#### 2.4 Datasets for strategy development

- **Repo contents:** only manifests are in the repo. All data files are gitignored (`*.parquet`, `data/*.db`). The example manifest records an `output_path` on the owner's Windows workstation (`C:\Users\...\data\research\...`). Reproduction needs a public re-download plus hash verification, with no committed or offsite copy of the frozen research data (DATA-10).
- **Frozen datasets and coverage:**

  | Dataset | Manifest | Coverage | Quality / notes | Used by |
  |---|---|---|---|---|
  | binance_usdm_daily_v1 | `docs/research/datasets/binance_usdm_daily_v1.*` | 911 USDT perps, 2020-01-01 to 2026-09-30, 663,145 daily bars, 2.7 M funding records, 43,626 files, all checksummed | 346 missing bar-days (3 symbols). Listing dates and filters are not point-in-time; survivorship not excluded (QUALITY.md, L6) | Mandate 004 |
  | crypto_broad_binance_v1 | `docs/research/datasets/crypto_broad_binance_v1.dataset.json` | 136 symbols, 15m, about 2024-09-23 to 2026-09-23 (70,176 rows per symbol) | Partition `source_version` is `LEGACY_UNVERSIONED`; store is "canonical historical_candles" in the owner DB | Residual / forecast research |
  | deep subset | `cati_crypto_deep_universe_binance_v1.json` | 35 symbols, 1m, target 1,826 days | Acquisition outcome not in the repo | Deep research |
  | BTC/ETH 1m | `data/research/binance_1m_btcusdt_ethusdt_2y_v1.manifest.json` | 2024-09-15 to 2026-09-14, 1,051,200 1m rows per symbol, derived 5m/15m/1h/4h | completeness 1.0 | — |
  | BTC/ETH 1m, short spans | 120d and 3d manifests | 120 days; 3 days | — | — |
  | FX Dukascopy reference | `cati_fx_universe_dukascopy_v1.json` | — | No licensing record | FX research |

- **Missing per intended use:**
  - Historical depth, spread and bid/ask for crypto. Only forward observation since deployment, BTC and ETH only. This blocks slippage and cost calibration (the registry notes no planned-vs-realized calibration).
  - OI history older than 30 days, and liquidations (none from any venue).
  - Point-in-time exchange filters and listing history.
  - Fee-tier and funding-interval history.
  - Bybit and BingX history: no frozen dataset, and BingX acquisition was defective on main.
  - Mark/index price history exists only via the deep script, which stops are not using (CONTRACT_PRICE).
  - Demo-venue price history: decisions use mainnet reference data and execute on the demo book, and divergence between the two is not measured.
  - All TradFi data (calendars are illustrative examples only).

#### 2.5 Multi-asset / multi-broker

- **Broker-neutral layers that exist:**
  - `shared_lib/broker/venue_contract` and `platform_adapter/{binance,bybit,bingx}.py` (the certification harness).
  - `environment.py`: one table of base URLs, with `testnet`, `sandbox` and `paper` mapped to DEMO. Bybit DEMO maps to api-demo.bybit.com and BingX DEMO to open-api-vst.
  - `exchange/instruments.py`: canonical symbols, settlement asset, tick, step, min notional.
  - Venue economic adapters for binance, bybit, bingx (`venue/registry.py`). The status registry declares only `binance_usdm` DEMO_VALIDATED (Bybit and BingX have no status entry; the docstring is stale).
  - `app/markets` ContractSpec, calendar and roll (TradFi foundations, not wired).
- **Hard-coded Binance and crypto assumptions in core paths:**
  - **Grep counts (python files):** "USDT" in 55 files, "binance" in 68, "fapi" in 16, across trading_intelligence, execution, risk and portfolio.
  - **Decision-data hosts:** `residual_prospective.py:289`, `forward_observe.py:43` and `platform_runtime.py:20` hard-code fapi.binance.com and the 15m interval.
  - **Family declarations:** all families declare only CRYPTO_BINANCE_USDT_PERPETUAL (`family_markets.py:40-50`).
  - **Binance-only branches in `production_execution.py`:**
    - emergency flatten returns `EMERGENCY_FLATTEN_UNSUPPORTED_BROKER` for non-Binance accounts (l.886-889);
    - the rate-limit evidence block (l.1270);
    - the native protection snapshot (l.620);
    - exchangeInfo metadata (l.1221).
  - **Shapes and units:** Binance-shaped `positionAmt` and `incomeType` throughout; capital in USDT (`production_execution.py:229, 549`); `forward_observe` canonical symbol is `symbol[:-4]+"/USDT:PERP"`.
  - **Budget tuning:** `rate_budget.DEFAULT_LIMITS` for Bybit and BingX are UNVERIFIED conservative envelopes.
- **Launch blockers vs scalability.** For the initial Binance USD-M DEMO launch (phases B/C) none of these is a blocker, except operational items DATA-02, DATA-03 and DATA-06. For Bybit/BingX customer trading, DATA-12 is required: emergency flatten, the validated status entry and data-venue alignment. MT bridge, OANDA, IBKR and TradFi are phase-D-plus scalability items (DATA-13).

### 3. Gap list

| ID | Component | File:lines | Problem | Root cause (verified/unverified) | Evidence (+level) | Customer impact | Trading impact | Severity | Classification | Existing implementation | Required correction | Dependency | Owner | Validation test | State | Effort | Release phase |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| DATA-01 | BingX historical klines | backends/bot-backend/app/market_data/venue_history.py:151-168 (main) | BingX backfill keeps only the newest 1,000 bars of a range. Bybit and BingX error envelopes read as "no data". check_series cannot see end truncation | Verified: endTime=end_ms on a newest-first venue, and loop exit after page 1 | git show f8401d5; my probe: main 1000/3000, branch 3000/3000; branch tests 14 passed (L4); live summary evidence (L2-) | None in production (research only) | Research on BingX data would be biased or truncated | P3 | LOGIC_DEFECT | Fixed on the branch (windowed walk, VenueDataError, expected range in check_series) | Merge f8401d5. Optionally assert per-window row counts and add pacing/429 handling | Merge of branch | Engineering | tests/test_phase4_market_data.py::test_bingx_backfill_* (branch) | FIXED_ON_BRANCH_ONLY | S | C |
| DATA-02 | Residual 15m collector | integration/residual_prospective.py:281-298, 373-423 | Production mainnet public kline polling (138 symbols, about 600-1,200 weight/min on cold start) bypasses the shared HTTP budget. Its 429/418 is not recorded in the budget | Verified (own requests.Session; `RateLimited` only backs off locally) | Static (L5) | With LIVE accounts on the same IP, an IP ban can block reconciliation, protection and emergency closes | Possible 418 ban on fapi.binance.com affecting live accounts | P1 | INTEGRATION_GAP | Pacing 0.5 s, 5-minute local backoff | Route through `public_get` (MARKET_DATA) or record 418/429 via `budget.note_error`; charge the catalogue weight | DATA-04 | Engineering | Unit test: a PublicCandles 429 closes the shared budget; a cold-start backfill is refused at the MARKET_DATA ceiling | OPEN | S | D (P2 for B/C) |
| DATA-03 | Forward observation storage | integration/forward_observe.py:38-122; pipeline/retention.py:196-199, PRUNE_RULES | Full raw book and 1,000-trade payload duplicated into every feature row every 5 min. Table has no prune rule. Estimated about 120 MB/day, about 40 GB/yr in the trading DB and backups | Verified for the code path; growth figure unverified (estimate) | Static (L5); estimate (L7) | Disk exhaustion or slow backups, risking platform availability | Indirect (DB / disk failure halts the runtime) | P2 | OPERATIONS_GAP | Retention framework exists; table classified REFERENCE_MARKET_DATA | Store the payload once per endpoint (or a hash), add a PruneRule or archive, measure growth on the VPS | — | Engineering | Measure `table_sizes` after 24 h; retention unit test for this table | OPEN | S | B |
| DATA-04 | Shared rate budget scoping | shared_lib/broker/http_budget.py:83-149, 167-206; rate_budget.py:255-300 | Budget keyed by (venue, egress scope), not host or environment. demo-fapi and fapi (separate venue limits) share one log, and a 429/418 from either host closes ALL Binance classes, LIVE EMERGENCY included | Verified (`reserve(self.venue, self.server_address)`, venue = "binance" for both) | Static (L5) | A DEMO-host ban could block LIVE protection or flatten for the ban duration | Cannot reduce live exposure during a ban caused by demo traffic | P1 | RISK_GAP | Conservative closure semantics; per-account UID scope exists for Bybit | Key the budget by (venue, host or environment, egress); keep one physical-IP view only where the venue really shares it | — | Engineering | Test: a 418 on demo-fapi does not refuse a LIVE PROTECTION reservation | OPEN | M | D |
| DATA-05 | AdapterError / budget incident | shared_lib/broker/http_budget.py:36-55; production_runtime.py:87-101 | Previous: flat 100 units per request starved reconciliation and the collector | Verified fixed: weight catalogue + no rebuild on local refusal | Code on main (L5); report measurements (L6); test_rate_budget passed (L4) | Resolved | Resolved | P3 | WORKING_AS_DESIGNED | def0bf4 on main | Keep the catalogue versioned; add `/fapi/v1/algoOrders` to cover the fallback path | — | Engineering | tests/exchange/test_rate_budget.py | VERIFIED_FIXED | S | B |
| DATA-06 | Market-data health signal | production_runtime.py:555-575; ops/runtime_supervisor.py:224-231, 286-296 | `/health market_data_status` and the supervisor read only the heartbeat of the residual collector, a research-FAILED family. The heartbeat says COLLECTING even when every symbol failed (input_errors). DAILY_TREND feed freshness is not in /health | Verified | Static (L5) | Dashboard and ops may show "ok" while decision data is missing (e.g. fapi geo-block 451) | Stale or empty decisions are not alerted on | P2 | OPERATIONS_GAP | Feed `freshness()` exists; supervisor stall checks exist | Derive market_data_status from per-family data freshness and error rate; include the daily feed seal / freshness; alert on all-symbol failure | — | Engineering | Simulate all-symbol HTTP errors; /health must report degraded | OPEN | S | B |
| DATA-07 | Data vs reconciliation priority | shared_lib/broker/rate_budget.py:47-50, 90-110 | MARKET_DATA stops at 50% of the budget. At about 280 units/min per account, about 3-4 Binance accounts per IP starve forward observation and feed exchangeInfo. Reconciliation is capped at about 5 accounts per IP. BingX limits are UNVERIFIED (20/10 s) | Verified by arithmetic over the code constants plus the measured per-account cost (L6) | Static (L5) / inference (L7) | Data refusals, then `FeedUnavailable` (WAITING_FOR_DATA), as accounts grow | Decisions wait when the egress IP is shared | P2 | ARCHITECTURE_GAP | Priorities designed (reconciliation over data) | Capacity plan per egress IP (separate egress for public data or more IPs); verify Bybit/BingX limits; load test N accounts | Ops egress design | Engineering/Operations | Load test with 5/10 accounts measuring MARKET_DATA denials | OPEN | M | C |
| DATA-08 | Daily feed polling of unpublished days | market_data/daily_feed.py:501-523, 252-285; pipeline/driver.py:162-242 | Until the archive publishes yesterday's daily files, each 30 s tick re-requests a 404 CHECKSUM for every active symbol (several hundred requests per tick), with no backoff. Unbudgeted CDN host | Unverified (static trace; not observed live) | Static (L5/L7) | Possible CDN throttling, delaying the daily decision | DAILY_TREND waits | P2 | LOGIC_DEFECT | settle_ms, circuit breaker (does not count 404s) | Per-day "not yet published" backoff (e.g. 15-30 min), or probe one symbol first | — | Engineering | Feed unit test with a fake 404 archive counting requests across ticks | NOT_VERIFIED | S | C |
| DATA-09 | Reference vs execution venue | integration/residual_prospective.py:289; production_execution.py:1240-1260 | Decisions and ATR use MAINNET public 15m data. Execution reads demo-fapi price/depth. Demo vs mainnet divergence is unmeasured; family declarations are Binance only | Verified design | Static (L5) | DEMO results may not reflect live economics | Sizing and stop distance from mainnet ATR on a demo book | P2 | RESEARCH_GAP | Execution uses the venue's own price and depth for gates | Record a reference-vs-execution price divergence metric; gate on it | — | Research | Divergence report over N DEMO cycles | OPEN | M | C |
| DATA-10 | Research dataset custody | data/research/*.manifest.json; docs/research/datasets/* | Only manifests are in the repo. Data lives on the owner workstation (Windows path); the canonical `historical_candles` store is not on the VPS or backed up. crypto_broad partitions are LEGACY_UNVERSIONED | Verified | Repo inspection (L5) | None directly | Research reproducibility and re-certification depend on one machine | P2 | DATA_GAP | Deterministic rebuild with archive checksums for the daily dataset | Offsite immutable copy of frozen datasets with hash verification; a rebuild script for crypto_broad with versioned source | — | Research/Operations | Rebuild binance_usdm_daily_v1 and match the dataset_hash | OPEN | M | C |
| DATA-11 | Binance OI history fetcher | market_data/venue_history.py:87-96 | One `limit=500` call with no pagination silently truncates 30 days of 1h (720 rows) | Verified (code) | Static (L5) | None (research) | Biased OI features in research | P3 | LOGIC_DEFECT | FeatureUnavailable for >30 d | Paginate; check expected count | — | Engineering | Fake venue with 720 rows | OPEN | S | C |
| DATA-12 | Bybit/BingX production readiness (data side) | production_execution.py:620, 886-889, 1221-1230; venue/registry.py:22-26; rate_budget.py:96-110; family_markets.py:40-50 | Non-Binance accounts: no emergency flatten, no adapter status entry, budget limits UNVERIFIED, decisions from Binance mainnet data. Families declare Binance only | Verified | Static (L5) | Bybit/BingX customers cannot be safely offered autonomous trading | Cannot flatten on Bybit/BingX via the durable path | P1 (for Bybit/BingX only) | MISSING_FEATURE | Clients, economic adapters, certification harness, user streams | Keep Bybit/BingX disabled for trading until flatten, venue certification, verified limits and venue-specific data exist | Execution area | Engineering/Owner | Venue certification run on demo for each venue | OPEN | L | C (for those venues) |
| DATA-13 | Multi-asset / TradFi / MT bridge | app/markets/*; app/exchange/{mt_bridge,oanda,ibkr*}; shared_lib/broker/client_factory.py:44 | TradFi calendars are illustrative; contract specs, roll and guard are not wired into execution; MT/OANDA/IBKR are not buildable by the production factory; the MT bridge is one account per Windows host | Verified | Static (L5); STEP7 doc (L6) | Not offered | None now | P3 | ARCHITECTURE_GAP | Foundations and tests | Future programme: data source and licence per market, adapters, certification | Owner decisions D1-D16 | Owner/Engineering | — | OPEN | XL | D+ |
| DATA-14 | Data licensing | (none); research/trend_v2_tradfi/SPEC.md D-rows | No recorded licence or terms review for Dukascopy data or for exchange market data used in a commercial customer product | Verified absence (grep) | Static (L5) | Legal / commercial exposure | None | P2 | OWNER_POLICY_DECISION | Provenance hashes recorded | Record terms per source; restrict Dukascopy to permitted use | — | Owner | Licence register reviewed | OPEN | S | D |
| DATA-15 | Canonical symbol / multiplier | exchange/instruments.py:135, 236-330 | contract_multiplier always 1.0; 1000-prefixed contracts are not normalized across venues; BingX qty_step from `size` is unverified | Unverified | Static (L5) | None (single venue today) | Cross-venue sizing errors if aggregation is added | P3 | DATA_GAP | Canonical symbol, settlement asset | Normalize multipliers; verify BingX field semantics | DATA-12 | Engineering | Instrument parser tests with venue fixtures | NOT_VERIFIED | S | D |
| DATA-16 | Research subprocess from the trading service | integration/residual_simulation.py:514-531; production_runtime.py:896-899 | Production `_broker_loop` may spawn `scripts/fx_finalization_watch.py` (research) as an unsupervised child process if a research DB file exists | Verified | Static (L5) | None if the file is absent on the VPS | Resource contention or noise | P3 | OPERATIONS_GAP | Guarded by file existence | Remove from the production loop, or run as its own unit | — | Engineering | Production profile does not spawn it | OPEN | S | B |

### 4. Verdicts

- **Market-data collection: PARTIAL.**
  - **Implemented and wired:** three production collectors start from `cosmicforge-trading.service` (residual 15m, daily archive feed, forward microstructure), and the per-account reads go through the shared budget. The AdapterError budget defect is fixed on main (L5/L4), and the per-row validation is strong.
  - **Not operationally verified** in this audit beyond the DEMO activation report (L6). The daily-feed module itself says it was not verified against live endpoints.
  - **Phase B/C gaps:** forward-observation storage is unbounded (DATA-03); the health signal is tied to a failed family's collector and ignores errors (DATA-06); the daily feed polls unpublished files every tick (DATA-08); data starves beyond about 3-4 accounts per IP (DATA-07).
  - **Phase D (real money) blockers:** the residual collector bypasses the budget (DATA-02) and the budget couples DEMO and LIVE hosts (DATA-04).
  - **Scope:** Binance only. Bybit and BingX have no production data path of their own.
- **Historical research data: PARTIAL.**
  - Well-governed, checksum-verified frozen datasets exist (Binance USD-M daily, 911 symbols, 2020-2026; 136-symbol 15m, 2 years; BTC/ETH 1m, 2 years; FX reference), with quality reports.
  - **Gaps:**
    - Data custody is on the owner workstation only (DATA-10).
    - No historical depth, spread, liquidation or long OI data, and no point-in-time filters, so cost and slippage cannot be calibrated.
    - No Bybit or BingX datasets. The BingX acquisition bug is fixed only on the branch (DATA-01), and Binance OI history is truncated (DATA-11).
    - No licensing record (DATA-14).

