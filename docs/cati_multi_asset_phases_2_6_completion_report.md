# CATI Multi-Asset Phases 2–6: Completion Report

- Branch: `claude/cosmicforge-audit-multi-asset-ht7upz`, based on `main` @ `9316a862a7ff0f4e9ebb67e6bed85608d3d059d1`.
- Audit input: `docs/multi_asset_expansion_audit_2026-09-25.md`.
- Commits:
  - `8ea199b`: Phase 1 audit.
  - `a9f693e`: Phase 2.
  - `290f320`: Phase 3.
  - `5c5e484`: Phase 4.
  - Phase 5 and Phase 6: see the final section.

## Summary

| Phase | Verdict | One line |
|---|---|---|
| 2: broker foundation, security, transfers | **PASS (code + tests)** | Every Phase 2 exit criterion is implemented and tested. The venue endpoints for transfers and permissions have not been run against real venues (a production blocker, not a code gap). |
| 3: execution parity and discovery | **PARTIAL** | Bybit and BingX satisfy the executor contract. They are UNVALIDATED, so they are allowed on DEMO and refused on LIVE. No demo run was possible from this environment. |
| 4: data platform | **PARTIAL** | The schema, pipeline, FX ingestion, universes and manifests are built and tested. **No dataset was acquired**, because the network policy blocks the exchange and Dukascopy hosts. |
| 5: CATI multi-asset and capital allocation | **PARTIAL** | FX context, currency-exposure graph, capital planner, admission gate, account selection and the FX-perp economics overlay are built. The planner runs in SHADOW only. The exposure graph is not yet wired into the hard-risk orchestrator. |
| 6: certification and production readiness | **PARTIAL** | Separate certification scopes, transfer simulation, combined portfolio simulation, the security-certification suite, shadow evidence and metrics are built. No multi-asset replay or demo evidence exists yet. |

**Runtime authority is unchanged.** CATI active execution remains disabled (`CATI_ACTIVE_EXECUTION_ENABLED` is unset and nothing sets it). Migration phase M0–M9 did not advance. No holdout was reserved or opened. No certification threshold or policy value changed.

---

## Audit findings re-verified on `main` before changes

Every item was present on the baseline (none was `VERIFIED_ALREADY_FIXED`).

| Finding | Verified on baseline | Resolution |
|---|---|---|
| BingX factory imports non-existent `app.exchange.bingx_client` | yes (import probe) | fixed (2A) |
| Bybit/BingX lack the executor contract | yes (`place_order`, etc. missing) | implemented (3) |
| Legacy platform-key routes open (`/runner/live/start` unauthenticated) | yes | admin-only (2C) |
| Withdraw-capable keys accepted | yes | rejected (2F) |
| Unsafe `BROKER_SECRET_KEY` fallback (`SECRET_KEY` / `b"0"*32`) | yes | fail-closed in production (2D) |
| Resolver bypasses (`transaction_service`, `get_decrypted_credentials`, snapshot scheduler) | yes | on resolver (2E) |
| Daily snapshot scheduler imports a missing function | yes | fixed (2E) |
| No internal-transfer implementation | yes | implemented (2G–2J) |
| `broker_transfers_cache` read but never written | yes | written by reconciliation (2I) |
| Binance internal transfers labelled DEPOSIT/WITHDRAWAL | yes | `INTERNAL_TRANSFER` (2E/2I) |
| CATI FX foundations present but incomplete | yes | extended (5) |
| `historical_candles` lacks venue identity | yes | new venue-aware tables; `historical_candles` untouched (4) |
| No funding/OI/spread/liquidation history | yes | schema and fetchers, with UNAVAILABLE semantics (4) |
| Market discovery Binance-specific | yes | Bybit and BingX discovery and universe adapters (3) |
| One asset class per bot | yes | `CRYPTO,FX` / `MULTI_ASSET` (3F) |

Additional defects found and fixed during implementation:
- `decode_token` logged token and `SECRET_KEY` prefixes on every request.
- User-backend proxies forwarded decrypted plaintext credentials to bot-backend, which never used them.
- BingX ignored `testnet`, so demo accounts hit mainnet.
- User-backend Binance validation used a different demo host than the runtime.
- BingX filters were keyed by `BTC-USDT`, so runtime lookups silently got empty filters.
- BingX positions were returned in hyphenated form (reconciliation mismatch).
- Bybit instruments were unpaginated and USDT-only.
- Bybit conditional stops were sent with `qty "0"`.
- Bybit update-protection had a cancel-then-replace window with no stop.
- `forex_proxy` awaited a synchronous function.

---

## Phase 2: PASS (code + tests)

**Implemented**
- **2A**
  - BingX factory fixed.
  - `shared_lib/broker/capabilities.py`:
    - declared per-broker profiles, states `SUPPORTED`/`UNVALIDATED`/`UNSUPPORTED`/`ACCOUNT_RESTRICTED`/`VENUE_API_UNAVAILABLE`;
    - narrowed by key permissions;
    - `WITHDRAWALS_SUPPORTED_BY_PLATFORM = False`.
  - Bot create, bot start and every runtime cycle refuse brokers that cannot execute (`BROKER_EXECUTION_CAPABILITY_INCOMPLETE`, `BROKER_EXECUTION_UNVALIDATED_FOR_LIVE`) and enforce account ownership.
- **2B:** `app/exchange/contract.py`, one canonical contract with capability-gated extras.
- **2C:** all 49 legacy `main.py` routes that drive the global runner or the platform `BINANCE_API_KEY` now require an admin token. Only `/` and `/health` stay public.
- **2D**
  - `BROKER_SECRET_KEY` is the only key source. Production startup fails without it (both backends).
  - Legacy keys are decrypt-only: always in dev, in production only during an explicit `BROKER_LEGACY_KEY_DECRYPT` window.
  - `backends/scripts/rotate_broker_credential_encryption.py` handles re-encryption.
  - Redaction applies to logs, exceptions and mappings.
- **2E:** all runtime credential reads go through `resolve_broker_auth`. Validation-mode access (`allow_unvalidated`) never covers revoked accounts. Bridge brokers resolve.
- **2F**
  - Permission evidence per credential version (`permissions_json`): Binance `apiRestrictions`, Bybit `query-api`. BingX is `UNVERIFIED`.
  - `WITHDRAW` → `REJECTED_WITHDRAW_PERMISSION`: the account is restricted and never connected, and the execution gate refuses it.
  - Fake Coinbase/Kraken/Alpaca validation removed (`UNSUPPORTED_BROKER`).
- **2G–2J**
  - Wallet topology per broker, covering physical transfer, shared collateral, logical-only and unsupported cases.
  - Transfer ledger: requests, append-only events, reconciliations, settings.
  - Idempotency keys and the full pre-submit validation chain.
  - A dispatched submission with no answer → `UNKNOWN`, never re-submitted.
  - Reconciliation worker.
  - `INTERNAL_TRANSFER` history classification.
  - User-scoped API: `GET|PUT /api/v1/brokers/{id}/transfer-settings`, `GET /wallets`, `GET /transfer-capabilities`, `POST|GET /internal-transfers`, `GET /internal-transfers/{tid}`, `POST /internal-transfers/reconcile`, in bot-backend with a user-backend proxy.
  - No withdrawal route or method exists.

**Tests**
- `test_phase2a_broker_foundation.py`: 36.
- `test_phase2b_permissions_transfers.py`: 33.
- `user-backend/tests/test_phase2_broker_security.py`: 8.

**Known limitations**
- Transfer and permission endpoint shapes are implemented from vendor documentation and tested on recorded shapes, not against live venues.
- Binance demo has no verified SAPI host, so permissions are UNVERIFIED and transfers unavailable on DEMO.
- BingX key permissions are not inspectable, so BingX internal transfers are always blocked (`PERMISSION_EVIDENCE_REQUIRED`).
- The BingX fund-wallet balance endpoint is unverified (balance UNAVAILABLE, so the transfer is blocked).

**Production blockers**
- A per-venue transfer and permission contract run on real accounts.
- Setting `BROKER_SECRET_KEY` and re-encrypting blobs in every production deployment.

**Authority**
- Migration authority: unchanged.
- Runtime authority: unchanged.
- Next action: run the transfer and permission contract on Bybit testnet and a Binance live account with a trade-only key.

## Phase 3: PARTIAL

**Implemented**
- Bybit, extended in place:
  - `orderLinkId` idempotency;
  - realtime and history order lookup;
  - executions as fills;
  - merged open orders (`Order`, `StopOrder`, `tpslOrder`);
  - `trading-stop` SL/TP amended **in place**;
  - cursor-paginated discovery;
  - cached filters with exact venue precision;
  - funding, ticker and orderbook;
  - wallet and transfer primitives.
- BingX, repaired in place:
  - `clientOrderID`;
  - order lookup, fills and open orders;
  - protection that places the new order first and then cancels the old one;
  - runtime symbol namespace.
- Binance: canonical aliases added, behavior unchanged.
- `DiscoveredInstrument` with parsers for all three venues. Venue metadata comes first; the FX/metals heuristic is explicit and labelled.
- `venue_instruments` catalog (delistings retained; an empty discovery never records delistings).
- Account-level eligibility separates market availability from API execution.
- Bybit and BingX universe adapters.
- One bot, several asset classes (`market_type` `CRYPTO,FX` / `MULTI_ASSET`):
  - universe widened to the allowed classes;
  - CATI classifies per instrument;
  - sessions are gated per asset class.

**Tests**
- `test_phase3_execution_parity.py`: 19.
- Legacy tests updated where they pinned replaced behavior: Bybit cancel-replace, hyphenated BingX positions, status-less Bybit payload, and paper-mode BingX mainnet URL.

**Not implemented / limitations**
- No demo validation run for Bybit or BingX, so LIVE is refused by design.
- FX perpetual availability on Bybit V5 is discovery-dependent and unverified.
- BingX FX/TradFi is `VENUE_API_UNAVAILABLE`.
- The spot and COIN-M products are not implemented.

**Authority**
- Runtime authority: Bybit and BingX may run on DEMO only.
- Next action: 30+ demo trades per venue with fill-resolution and protection evidence, then promote in `capabilities.py`.

## Phase 4: PARTIAL

**Implemented**
- Venue-aware tables: candles, feature observations (a schema CHECK makes UNAVAILABLE ≠ 0), FX reference quotes, and immutable dataset and universe manifests.
- Causal deterministic resampling reusing `research.derive`.
- Extended quality checks.
- Dukascopy bi5 decoder and CSV FX import.
- Venue/reference divergence.
- Dynamic universe selection with roles and a deep subset.
- Venue history fetchers.
- `scripts/acquire_multi_asset_dataset.py`.

**Tests:** `test_phase4_market_data.py`: 10.

**Not done:** **no data acquired.** The session's network policy denies `fapi.binance.com`, `api.bybit.com`, `open-api.bingx.com` and `datafeed.dukascopy.com` (proxy CONNECT 403). The 100+ symbol universe, the deep dataset and the FX history do not exist yet.

**Next action:** allow those hosts, then run the acquisition script with `--plan-only`, then the full crypto run, then `--fx`.

## Phase 5: PARTIAL

**Implemented**
- `fx/context.py` FXMarketContext: causal, versioned, hashed; UNAVAILABLE with a reason, never 0.
- `portfolio/currency_exposure.py`: notional exposure graph plus concentration checks, including cross pairs and crypto base legs.
- `capital/planner.py` CapitalAllocationPlanner:
  - all six outcomes;
  - deterministic transfer idempotency keys;
  - `is_fundable` is true only on a broker-confirmed COMPLETED transfer;
  - MANUAL vs. authorised AUTOMATED mode.
- `integration/opportunity_gate.py`: the 5H admission gate.
- `venue/account_selection.py`: same-user after-cost account selection, with no cross-broker path.
- `economics/fx_perp.py`: FX-perp divergence and session-liquidity overlay. It is separate from the certified Section 17 cost model, whose policy hash is part of the Section 22 freeze.
- Shadow capital-routing hook (`CATI_CAPITAL_ROUTING_SHADOW_ENABLED`, default off).

**Tests:** `trading_intelligence/test_phase5_6_multi_asset.py`: 19.

**Not wired yet**
- The currency-exposure check in the hard-risk orchestrator.
- The admission gate in the Section 18 TradePlan builder.
- The FX overlay in the Section 17 cost estimate. This needs a governed cost-policy version bump.
- The planner in live execution (the Section 20 boundary stays disabled).

**Next action:** a governed decision on the cost-policy bump; then wire the exposure check and admission gate in shadow first.

## Phase 6: PARTIAL

**Implemented**
- `research/certification/scopes.py`:
  - CRYPTO on Binance, Bybit and BingX;
  - FX reference data;
  - FX on Bybit;
  - FX/TradFi on BingX (blocked);
  - scope-qualified holdout namespaces;
  - freeze completeness required.
- `transfer_sim.py`: latency, failure and unknown outcomes; entries fund only on confirmation; risk rechecked when the transfer completes.
- `portfolio_sim.py`: combined crypto+FX account simulation covering margin, currency concentration, simultaneous signals and capital routing.
- Shadow capital-plan evidence (append-only).
- Multi-asset metrics on the bounded CATI registry.
- Adversarial security suite: user A cannot resolve, trade, transfer, read wallets of, or see credentials for user B.
- Fail-closed behavior tests.
- Performance guards (resampling, 600-instrument selection, the bisect future-row lookup).

**Tests:** `test_phase6_certification_security.py`: 12.

**Not done**
- No multi-asset walk-forward replay (no data).
- No demo forward evidence.
- No scope frozen.
- The frozen Section 22 values (P(expectancy>0), cost-stress, drawdown, concentration, minimum evidence, PBO, neighbor stability, forward demo ≥30 trades and ≥30 days) are unchanged and asserted.

**Authority:** runtime authority unchanged; CATI remains non-authoritative.

---

## Current capability matrix (declared; LIVE requires SUPPORTED)

| Capability | Binance | Bybit | BingX |
|---|---|---|---|
| Crypto perpetual execution | SUPPORTED | UNVALIDATED (demo only) | UNVALIDATED (demo only) |
| Instrument discovery | SUPPORTED | UNVALIDATED (paginated) | UNVALIDATED |
| FX perpetuals | UNVALIDATED (only if discovered) | UNVALIDATED (only if discovered) | VENUE_API_UNAVAILABLE |
| TradFi | UNVALIDATED (only if discovered) | VENUE_API_UNAVAILABLE (MT5 is separate) | VENUE_API_UNAVAILABLE |
| Permission inspection | UNVALIDATED (LIVE only) | UNVALIDATED | UNSUPPORTED |
| Internal transfer | UNVALIDATED (LIVE host only) | UNVALIDATED | UNVALIDATED (blocked: no permission evidence) |
| Withdrawals by platform | **never** | **never** | **never** |

## Test results

The final numbers are in the final section.

- **Baseline** (clean worktree at `8ea199b`):
  - bot-backend: 3830 passed, 10 skipped, 0 failed.
  - root `tests/`: 67 failed / 5 passed. These tests require running servers (connection refused), and the failures predate this work.
- **After Phase 2:** 3897 passed, 1 failed. The failure was a legacy assertion that paper-mode BingX uses mainnet; it was corrected.
- **After Phase 3:** 3943 passed, 2 failed. These were pinned legacy Bybit cancel-replace tests; they were rewritten to the new contract.
- **Final** (all phases, this commit):
  - bot-backend `tests`: **3958 passed, 10 skipped, 0 failed**. The skips are the same classified baseline skips.
  - `tests/trading_intelligence` (CATI): **872 passed, 0 failed**.
  - root `tests/`: 67 failed / 5 passed. The failing set is **identical to the baseline** (diffed); the failures are server-dependent integration tests.
  - `user-backend/tests/test_phase2_broker_security.py`: **8 passed**.
  - New tests added across phases: 137 (36 + 33 + 8 + 19 + 10 + 19 + 12).

## Remaining production blockers

1. Allow outbound HTTPS to the exchange and data hosts, then acquire the datasets (Phase 4).
2. Demo validation for Bybit and BingX, and venue contract runs for transfer and permission endpoints.
3. `BROKER_SECRET_KEY` in every deployment, plus blob re-encryption.
4. Wire the currency exposure check, admission gate and FX overlay (governed cost-policy bump).
5. Multi-asset replays per scope, then freeze, then holdout. Then forward demo: ≥30 trades and ≥30 days per scope.
6. A transfers UI in the frontend (the API exists; no UI).
7. Pre-existing issues, out of scope and noted:
   - `/api/admin/tradingview/*` status routes are unauthenticated (read-only).
   - The daily snapshot scheduler is never started.
   - The legacy user-backend test suite fails at collection.
