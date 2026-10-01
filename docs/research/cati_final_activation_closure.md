# CATI final activation closure — living report

Single source of truth for CATI activation status. Updated in place; every claim has evidence (commit, artifact,
hash, count). Last update: 2026-10-01 04:50 UTC, `main@c45a8a95`.

**Governance today:** phase **M0**. CATI execution **BLOCKED** (`GOVERNANCE_PHASE_M0_NO_CATI_AUTHORITY`).
Demo authority **FALSE**. Live authority **FALSE**. Crypto holdout **RESERVED, unopened** (`hold_8c0c0e484403395b79b63fdc`).
FX holdout **not reserved** (no frozen FX dataset). `RESEARCH_DEFAULT_V1` = `55631f31…` (unchanged).
Section 22 thresholds unchanged. No holdout row has been evaluated.

## Definition of done

| Item | Status |
|---|---|
| Outcome library | RUNNING (canonical crypto library being built by the certification run) |
| Calibration | RUNNING — runs on the library as soon as it exists |
| Runtime ranking | WAITING (needs a calibrated library) |
| 5 full epochs | WAITING (measured after the library is installed, with certification suspended) |
| Crypto pre-holdout | RUNNING |
| FX acquisition | RUNNING (single writer) |
| FX derivation / QA / freeze | WAITING (after 1m completes) |
| Bybit demo validation | WAITING_FOR_USER (connect a Bybit Demo Trading account in the app) |
| BingX demo validation | WAITING_FOR_USER (connect a BingX VST account in the app) |
| Holdout | WAITING_FOR_GOVERNANCE (needs PRE_HOLDOUT_PASS and explicit user authorization) |
| M5 / demo / M6 / production | WAITING_FOR_GOVERNANCE |
| CATI live authority | OFF |
| Legacy V2 fallback | DISABLED by design (router never hands a CATI scope to V2) |
| Hard risk / 2.5% daily hard-loss | ACTIVE (unchanged; every CATI entry passes the existing hard-risk stack) |

## DONE

| Work | Evidence |
|---|---|
| Bybit DEMO host = `api-demo.bybit.com`; demo internal transfer reported unavailable | `b3674cfb` |
| Crypto dataset identity frozen (lineage v2) | manifest `d64e5c51…`, 136 partitions, 9,543,936 rows, reconciles to coverage `259f2ef5…`; `37bb0077`; registry row `ds_d64e5c517fd634e5` |
| Dataset freeze tool + pre-holdout readiness CLI | `6d0c6eae` |
| Governed outcome library: parallel identity-neutral build, holdout-safe pins, canonical pre-holdout library persisted by the certification run | `76a6bbac`; real-data hash equality (sequential == 2 workers == `e8d7fbf3…`) |
| Library at certification scale: streaming write/hash/load, shared immutable values (−41% memory/row), identical bytes and hashes | `c45a8a95` |
| Library scope (a crypto library never forecasts stock/index/commodity perps) + explicit failure reasons | `71100682` |
| Runtime order-authority switch (router + V2 entry gate + CATI dispatch; no V2 fallback; wiring-derived capability state) | `f8c13085`; 12 switch tests |
| Authenticated demo venue-validation harness (A–P checks, DEMO-only, withdraw-capable keys refused, transfer `UNAVAILABLE_ON_DEMO`) | `3a86438c`; `scripts/validate_demo_venue.py` |
| Per-epoch performance evidence `[CATI_EPOCH_PERF]` | `08af2932` |
| Economic calendar restored (ingestion had never been enabled) | 189 events, current week through 2026-10-03; 0 "Feed stale" since restart |
| Test environment: no `PYTHONPATH` / `PYTHONUTF8` needed; user-backend 68 passed / 0 failed | `76a6bbac`, `205aeea0` |
| FX acquisition supervisor (backoff, single-writer check) | `8ee94d65` |

## RUNNING

| Job | Detail |
|---|---|
| Crypto FULL/MEDIUM pre-holdout | relaunched 2026-10-01 04:40 UTC on `c45a8a95`, 4 build workers, `--library-output data/research/cati_libraries`, no `--open-holdout`. Library expected ~8–9 h after start, then a ~2-day sequential walk-forward replay. |
| FX 1m acquisition | single writer (supervisor of session "CosmicForge multi-asset expansion"); 11,689 pair/day periods remaining at 2026-09-30 03:10 UTC; 0 FAILED |

## WAITING_FOR_USER

- Connect **Bybit Demo Trading** (not Testnet) and **BingX VST** accounts as **Demo** in Broker Connection.
  Keys need read / orders / positions / trade, **never withdrawal**. Do not paste keys in chat.

## WAITING_FOR_GOVERNANCE

- Holdout opening (explicit authorization after PRE_HOLDOUT_PASS), M5, demo activation, M6, production.

## FAILED / incidents

| When (UTC) | What | Outcome |
|---|---|---|
| 2026-09-29 21:18–22:01 | Two FX writers overlapped ~43 min (a second session's supervisor woke from cooldown) | No damage: primary keys on quotes and ingest log; 0 periods logged twice; 0 FAILED. One writer since. |
| 2026-09-30 09:10 | First crypto certification run died (`MemoryError`) writing the ~2.8M-row library | Root-caused and fixed in `c45a8a95`; relaunched. |

## Known constraints (not defects)

- The frozen veto policy approves only `CALIBRATED` forecasts and certification counts only APPROVED records.
  Ranking at runtime and accepted evidence in certification therefore depend on the library passing the existing
  out-of-sample calibration thresholds. Thresholds are not changed.
- The FX dataset freeze adapter (partition semantics for FX closures) is written against the finished dataset.
