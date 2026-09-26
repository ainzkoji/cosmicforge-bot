# CATI Sections 13–16 audit and reuse map

Baseline: edc310304007cf713077fed353568bd9b452308c. origin/main fetched and identical.
Actual checkout: C:/Projects/cosmicforge-bot (desktop project path is stale).

## Operational isolation
Read-only SQLite inspection: crypto_deep_binance.db and fx_reference_dukascopy.db both WAL.
Active acquisition commands: three crypto acquire shards, crypto features, FX minute acquisition.
Python launcher/child pairs are not counted as distinct jobs. No workers stopped.
Writers: market_candles, market_feature_observations, market_ingest_log;
fx_reference_quotes, fx_reference_ingest_log; FX repair path also fx_reference_repairs.
No production database migrations or data rewriting planned. All test DBs temporary.

## Initial requirement classification (before implementation)
DONE means verified code exists; closure additionally requires tests. This is an initial audit, not a completion claim.

| Requirements | Initial classification | Existing reuse point / gap |
|---|---|---|
|13.1|DONE|market_data/quality.py wraps research/dataset.py assess_quality; store.py, gaps.py, universe.py, fx_universe.py; scripts/dataset_coverage.py; certification/dataset.py|
|13.2–13.4|PARTIAL|Quality checks exist; store accepts invalid/out-of-order rows and reports attempted rather than inserted count; FX QA only checks close spread, not independent sides|
|13.5|PARTIAL|gaps.py taxonomy exists; FAILED/QUARANTINED incorrectly imply provider outage|
|13.6|PARTIAL|Coverage JSON exists, streamed timestamps/SQL aggregates; invalid/duplicate and missing-range detail differs by dataset|
|13.7–13.8|PARTIAL|Existing research/certification manifests and stable_hash; old identities must remain stable; complete lineage/freeze extension needed|
|13.9|PARTIAL|Frozen universe tampering checked; dataset freeze completeness gate needs audit/extension|
|13.10–13.11|PARTIAL|Source dimensions stored; optional-provider lookup may mix sources; FX repair lineage exists; crypto repairs not yet demonstrated|
|13.12|PARTIAL|Stable content hashes exist; full requirement matrix needs tests|
|13.13|DATA_DEPENDENCY_IN_PROGRESS|Deep crypto, supplemental features and FX 1m actively acquiring; final identities must not be frozen|
|13.14–13.15|PARTIAL|Existing dataset tests; closure matrix incomplete|
|14.1–14.3|PARTIAL|market_state/global_state.py canonical builder and cycle integration; calendar timestamp provenance needs extension|
|14.4|PARTIAL|Frozen dataclass contains mutable mappings|
|14.5–14.6|DONE|Global state contract has no private account fields; integration is evidence-only after selection|
|14.7|MISSING|Crypto families in MarketState; no canonical CryptoMarketContext|
|14.8–14.9|PARTIAL|fx/context.py exists; spread_state falls back to reference spread; provider/calendar freshness lineage incomplete|
|14.10–14.11|DONE|Existing summaries and append-only global_state_store.py|
|14.12–14.13|PARTIAL|Need closure tests|
|15.1–15.4|PARTIAL|portfolio/currency_exposure.py weights notionals; factors.py pre-size unit model and selector hard cap exist; missing mappings/nonfinite inputs not rejected|
|15.5|DONE|exposure_builder.py SQL joins account across bots; account-scoped snapshot/reservations|
|15.6–15.7|PARTIAL|STABLECOINS shared mapping, selector settlement proxy; notional graph lacks distinct settlement concentration|
|15.8–15.9|PARTIAL|Unknown notional tracked but malformed/missing mapping silently drops legs; persisted loader defaults missing numeric values to zero|
|15.10–15.11|DONE|Existing configured factor cap and superior hard-risk gates; no threshold changes authorized|
|15.12–15.13|PARTIAL|Need closure tests and runtime weighted exposure audit|
|16.1–16.4|DONE|venue/adapter.py, contracts/venue_economics.py, venue/cost_model.py, venue/context.py; economics/engine.py subtracts costs once; Binance adapter|
|16.5–16.6|MISSING|Existing Bybit/BingX exchange clients expose ticker/book/funding/instruments, but registry.py has only Binance economic adapter|
|16.7–16.10|PARTIAL|Base adapter has generic fallbacks; new venues require strict versioned policy, account-tier isolation and freshness|
|16.11–16.12|PARTIAL|capital/planner.py models logical versus physical funding and readiness; no fee/latency validity economics contract|
|16.13–16.14|PARTIAL|Instrument metadata and rounded cost quantity exist; metadata/fee TTL enforcement incomplete|
|16.15–16.16|DONE|Frozen default policy and fx_perp.py overlay separate; any strict path must be a new policy|
|16.17–16.20|PARTIAL|Existing cost components/lineage/gates/evidence; transfer and explicit optional unavailable cost fields need extension|
|16.21|EXTERNAL_VALIDATION_REQUIRED|No Bybit/BingX demo evidence collected; never equate adapter code with validation|
|16.22|PARTIAL|Adapter/policy/integration and tests needed|

## Frozen universe identities (not dataset hashes)
Crypto broad: a5b5d1ee053fc5d3d12441113457cd2782405c8009f9ebea83878f65d1779f26
Crypto deep membership: a1406b7aaa5ec91f0f983882b25dd70f933dfddd52786be01048f11b816475f2
FX: 7242727983a010819d1476047b11477e4ff4bcc4fd2db5443014fea5e128a579

Universe hash identifies membership/selection metadata. Dataset hash identifies data and source.
Partition checksum identifies a single series/window. Metadata hash identifies contract metadata.
Manifest hash binds data plus policies/lineage. Code revision identifies implementation.
Library/replay hashes bind their own configuration and upstream artifacts; they are not interchangeable.

## Final closure status (after implementation)
Classification after code + tests. EXTERNAL / DATA items are not code gaps.

| Req | Final | Evidence |
|---|---|---|
|13.2 chronology|DONE|store rejects FX out-of-order (`FX_OUT_OF_ORDER`); candle batches pass `check_series` (out-of-order/duplicate = unusable); FX derivation refuses unordered input; `audit_partition` streams and counts out-of-order without sorting|
|13.3 duplicates|DONE|UNIQUE identity incl. venue/provider/source; store returns rows actually inserted (`changes()`); venue/provider never collapse|
|13.4 OHLC|DONE|non-finite/non-positive/high/low/volume checks; FX BID and ASK validated independently, per-field ask>=bid; derived FX bars re-validated. Ingestion/derivation gates are STRUCTURAL ONLY (`STRUCTURAL_ONLY`): a wide spread is stored and flagged by QA, never dropped|
|13.5 gaps|DONE|FX: FAILED/QUARANTINED -> `INGEST_FAILURE`; outage only on explicit `PROVIDER_OUTAGE` evidence; EMPTY/mixed -> `UNKNOWN_GAP`. Crypto: LISTING_AGE / PROVIDER_FAILURE (= ingest failure) / MARKET_HALT / VENUE_OUTAGE need evidence. Policy advanced to `gap-classification-v2`|
|13.6 coverage|DONE (code)|`audit_partition`: requested/actual range, rows, expected (closures excluded), coverage %, missing ranges + reason, invalid/duplicate/out-of-order, source/venue/timeframe, `ACQUIRING`/`FAILED`/`PARTIAL`/`COMPLETE`|
|13.7-13.9 manifest/freeze|DONE (code)|`freeze_dataset_payload` extends the existing manifest: exact membership, source/venue, product, base interval, metadata hash, code commit, quality/resampling/gap policy versions; `created_at` is metadata only; refuses any non-COMPLETE partition; tamper -> `DATASET_MANIFEST_TAMPERED`|
|13.10 provenance|DONE|`read_candles` refuses ambiguous source identity; `fx_reference_at` requires provider when >1 exists; freeze refuses source/venue substitution|
|13.11 repairs|DONE|existing FX repair lineage reused; a store-rejected repair rolls back as one transaction and logs `FAILED` + `QUALITY_REJECTED`|
|13.13 active acquisition|DATA_DEPENDENCY_IN_PROGRESS|deep crypto 1m, crypto features and FX 1m still acquiring; no final dataset manifest frozen|
|14.1-14.5|DONE|one GlobalMarketState; nested components frozen (`FrozenDict`); schema/engine v2; causal on candle time AND MarketState decision time; calendar needs a causal timestamp; tenant-neutral payload asserted by key|
|14.6 shadow only|DONE|allowlist test of every importer; AST proof the cycle discards the stage result; stage leaves the cycle result untouched|
|14.7 CryptoMarketContext|DONE|`market_state/crypto_context.py`: projection of existing MarketState families; missing funding/basis/OI/book -> UNAVAILABLE with reason; liquidations unsupported. Persisted with GMS evidence|
|14.8-14.9 FXMarketContext|DONE|reference spread no longer backs `spread_state`; reference provider, calendar observed_at + AVAILABLE/STALE/UNAVAILABLE_WITH_REASON; calendar feed freshness remains an OPERATIONAL dependency|
|15.x|DONE|size-weighted legs; settlement/collateral concentration (`settlement_by_code`) separate from direction, any settlement asset; malformed/unknown mapping or notional -> unknown (blocks approval); loader raises `CURRENCY_EXPOSURE_NOTIONAL_UNKNOWN` instead of zero; 2.0-unit cap unchanged|
|16.1-16.4|DONE|Binance adapter + frozen default policy unchanged (hash pinned)|
|16.5-16.6|CODE COMPLETE / EXTERNAL_VALIDATION_REQUIRED|`venue/perpetual.py` Bybit linear + BingX swap adapters over the existing clients; registry status UNVALIDATED (costs NOT_VIABLE)|
|16.7|DONE|reference payloads never read by venue adapters; missing venue book -> spread/slippage UNAVAILABLE|
|16.8-16.10|DONE|depth-walk slippage from venue book; funding needs rate + interval + next time + freshness (BingX interval from `premiumIndex.fundingIntervalHours`); fee tier scoped to user/account/environment/symbol, TTL 300s|
|16.11-16.12|PARTIAL (integration)|`economics/transfer.py`: logical routes cost 0, physical routes need fee + latency, arrival after `valid_until` rejects. The production cycle does not yet supply `TransferEconomics` (capital routing runs after selection/sizing), so Bybit/BingX shadow economics carry `TRANSFER_ECONOMICS_UNAVAILABLE` -> fail closed. Sequencing routing before admission belongs to Section 17|
|16.13-16.14|DONE|executable rounded quantity incl. min qty/notional; metadata/fee/funding/depth TTLs|
|16.15-16.16|DONE|`MultiAssetVenueCostPolicy` (`multi-asset-venue-cost-policy-v1`) is new and separate; `fx_perp.py` unwired from any production path|
|16.17-16.19|DONE|`native_costs["multi_asset_economics"]`: per-component value or None, availability, reason codes, policy hash, `automatic_execution_authorized: False`|
|16.21|EXTERNAL_VALIDATION_REQUIRED|no Bybit/BingX DEMO evidence collected in this session|

Pinned hashes (baseline == final): RESEARCH_DEFAULT_V1 `55631f31a30ad4a3cf09cf55e6af698bea5d60cce862a303d35c1bcdd842dfe3`;
default venue cost policy `234bac9012b2c1686ea000e7c67e14141fe49155b370a5ba8964bfe8853d6291`;
new multi-asset policy `3b169d721e4cd52d1328c28b7be0ecc62b0853acd3245f7d583f4f751e4d3968`.

Frozen FX universe v1 records `gap_policy_version: gap-classification-v1` inside its identity. The file is unchanged and
still verifies; re-running `freeze` now refuses (policy changed) and a future freeze must write a NEW versioned file.

No migrations: no table, column or index was added or altered. `historical_candles` and `market_candles` identity unchanged.
