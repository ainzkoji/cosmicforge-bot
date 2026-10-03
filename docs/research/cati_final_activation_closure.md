# CATI final activation closure — living report

Single source of truth for CATI activation status. Updated in place; every claim has evidence (commit, artifact,
hash, count). Last update: 2026-10-02, V3 implementation/runtime checkpoint.

**Governance today:** phase **M0**. CATI execution **BLOCKED** (`GOVERNANCE_PHASE_M0_NO_CATI_AUTHORITY`).
Demo authority **FALSE**. Live authority **FALSE**. Crypto holdout **RESERVED, unopened** (`hold_8c0c0e484403395b79b63fdc`).
FX holdout **not reserved** (no frozen FX dataset). `RESEARCH_DEFAULT_V1` = `55631f31…` (unchanged).
Section 22 thresholds unchanged. No holdout row has been evaluated.

## Definition of done

| Item | Status |
|---|---|
| Outcome library | COMPLETE: 3,007,222 rows; `0ead8264955e…`; clean rebuild; 8/8 verification PASS |
| Calibration | FAILED: V1 0.0004776754; causal V2 0.0010456895; information-conditioned V3 0.0185275224; all below frozen 0.02 |
| Runtime ranking | BLOCKED (no calibrated eligible library; canonical application RUNNING safely at M0; no library pin) |
| 5 full epochs | WAITING (measured after the library is installed, with certification suspended) |
| Crypto pre-holdout | NOT_RUNNING; no completed certification result established by this calibration audit |
| FX acquisition | RUNNING: existing single-writer supervisor resumed; fewer than 6,500 pair/day periods remain |
| FX derivation / QA / freeze | QUEUED sequentially after acquisition; strict gap rule retained; NOT FROZEN |
| Bybit demo validation | WAITING_FOR_USER (connect a Bybit Demo Trading account in the app) |
| BingX demo validation | WAITING_FOR_USER (connect a BingX VST account in the app) |
| Holdout | WAITING_FOR_GOVERNANCE (needs PRE_HOLDOUT_PASS and explicit user authorization) |
| M5 / demo / M6 / production | WAITING_FOR_GOVERNANCE |
| CATI live authority | OFF |
| Legacy V2 fallback | DISABLED by design (router never hands a CATI scope to V2) |
| Hard risk / 2.5% daily hard-loss | ACTIVE (unchanged; every CATI entry passes the existing hard-risk stack) |

## DONE

V3 is now actual code and a completed nested chronological evaluation, not a
design-only proposal. Candidate `cati_v3_f97c1349086b6c1068ee49c0`, 51,887 outer
predictions, skill 0.0185275224, ECE 0.0128049516, positive skill in all five
outer folds. Gate FAILED; RESEARCH_ONLY. See
[development report](cati_v3_development_report.md),
[registered variants](cati_v3_research_registry.json) and
[model/metrics/provenance result](cati_v3_development_result.json).

Canonical runtime health, live scheduler/calendar workers, market cycles,
instrument discovery and active hard-risk blocks verified. Full CATI regressions
1,110 passed; broader runtime/risk/broker/market/V3/FX checks 353 passed; final
scope/authority/V3/FX checks 24 passed. Full requested engineering closure remains
incomplete pending FX acquisition/QA/gaps/freeze and authenticated user-account
validation. MODEL_READY = NO. No holdout query, promotion or CATI authority change.

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

## Last reported acquisition/certification jobs (historical)

The job descriptions below are the previous checkpoint, not current running-state evidence.
No Python jobs were running at the readiness audit; the diagnostic jobs subsequently completed.
No acquisition, certification replay, or trading runtime was restarted.

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

## FX dataset — interim evidence (read-only coverage, 2026-10-01 04:51 UTC, content hash `3bf0ffe5…`)

| | provider 1h | 1m (acquiring) |
|---|---|---|
| rows | 634,985 | 29,207,790 |
| pairs complete | 50 / 50 | 39 / 50 |
| missing bid/ask side, negative spread | 0, 0 | 0, 0 |
| WEEKEND / HOLIDAY / SESSION closed gaps | 5,119 / 112 / 178 | 3,664 / 3,976 / 23,019 |
| **UNKNOWN_GAP** | **1,909 gaps / 30,847 bars** | **265,121 gaps / 2,903,222 minutes** |

Cross-rate QA: 32 relations, 0 failing (worst median rel. error 0.088%). EURCNH / EURZAR 2024-08 scale repairs remain
validated (rel. diff 0.0033% / 0.15% vs triangular level). Derived 5m / 4h: not started; 15m: 3 pairs (preliminary).

**FX freeze blocker (DATA + CODE, open).** Dukascopy daily 1m files carry all 1,440 minutes; minutes without ticks are
zero-volume flat bars, which ingestion drops (`fx_reference.drop_flat_closed_bars`) so a closed market is a gap and
never a fabricated price. Inside open sessions those dropped minutes are classified `UNKNOWN_GAP`
(`gaps.classify_fx_gap`: the period is FETCHED), and the lineage-v2 freeze accepts only expected closures — so the FX
dataset cannot freeze under the current rules (USDTRY: median 1,150 of 1,440 minutes on Tue–Thu, never ≥ 1,435;
USDZAR median 1,430). A legitimate fix needs per-minute provider evidence (the missing minutes are exactly the file's
zero-volume flat records), not an assumption; verification against a real file was attempted and refused by the provider
(HTTP 503 / connection reset) and will be retried when the provider recovers. No rule has been changed.

## Known constraints (not defects)

- The frozen veto policy approves only `CALIBRATED` forecasts and certification counts only APPROVED records.
  Ranking at runtime and accepted evidence in certification therefore depend on the library passing the existing
  out-of-sample calibration thresholds. Thresholds are not changed.
- The FX dataset freeze adapter (partition semantics for FX closures) is written against the finished dataset.

## Calibration failure diagnosis — 2026-10-02

The original V1 calibration was numerically reproduced exactly using an indexed
replay of its scored probabilities. V1's baseline uses evaluation-set prevalence,
so it is a retrospective constant rather than a causal baseline forecaster.
Correcting that information set does not rescue the model: skill is still only
0.0010456895 against the unchanged 0.02 requirement. Exact-cohort lookup covers
99.55% of predictions; ROC-AUC is 0.53735. The dominant finding is insufficient
predictive information, with a secondary evaluation-methodology defect.

Full checks of all library rows found no label arithmetic/validity violations;
65/65 source-bounded candidate/label/cohort reconstructions matched exactly.
Source verification is sampled, not an independent rebuild of every price path.

- V1 research decision: **REJECTED_PRE_HOLDOUT**. Its immutable artifact and
  calibration result remain **RESEARCH_ONLY**, unchanged.
- New opt-in causal-baseline research candidate: `cati_lib_80348290bf88005f4b2c151c`,
  hash `80348290bf88005f4b2c151ce322dbb67f8d7a36095f9f244694315e548f3e55`.
  Identical row bytes; separate research schema and calibration record; runtime
  loader rejects its version. Also **REJECTED_PRE_HOLDOUT / RESEARCH_ONLY**.
- Clean source provenance: `4719a831e0e4c8e8bf911e6cd5e865715cf9cf3e`;
  85 targeted tests passed.
- No thresholds, `RESEARCH_DEFAULT_V1`, governance phase, execution authority,
  runtime library pin, or holdout access changed.

Report: [calibration root cause](cati_calibration_failure_root_cause.md).
Identity: [causal correction candidate](cati_causal_baseline_candidate.json).
Next research design: [candidate plan](cati_next_candidate_design.md).
That diagnosis preceded the completed V3 and V4 development work described below.
The inspected pre-holdout span cannot be represented as pristine selection data.

## Latest development status — V4, 2026-10-03

V3 was implemented and rejected at skill 0.01852752. V4’s registered nested
four-family search is complete: `cati_v4_a71dc63a483d307c3b6cfbeb`, 51,887 outer
predictions, skill **0.01928768**, ECE **0.01019027**. Each later fold improves on
V3, but pooled skill still misses the unchanged 0.02 gate. The hybrid family’s
0.02011761 pooled score fails the predeclared last-fold robustness condition.
V4 remains **REJECTED_PRE_HOLDOUT / RESEARCH_ONLY; MODEL_READY = NO**.

The separate conditional payoff model passes its registered pooled development
checks. It is not ready for CATI decisions: folds 1 and 5 have expectancy-bucket
reversals; the highest pooled predicted-return bucket realizes −0.07838 R; and
independent profit/terminal probabilities violate joint consistency on 16.41%
of outer rows. These limits are reported separately from the registered pass.

M0, CATI execution OFF, no runtime library pin, and the 2.5% hard-loss cap remain.
No holdout was opened, inspected or queried. The existing healthy paper runtime
and single FX acquisition writer/queued completion workflow were preserved.
No FX acquisition/freeze completion is claimed. Full CATI regression passed;
numeric models and evidence are committed with the research implementation.

See [V4 development report](cati_v4_development_report.md) for all candidates,
folds, residuals, payoff diagnostics, provenance, tests and resource measurements.


## V5 development closure

V5 candidate `cati_v5_b131674954a223230581dbbc` fixes coherent joint outcome/path/event generation and meets pooled skill/ECE/sample thresholds, but fails registered late-fold robustness (folds 3 and 5). Every fold�s top expectancy bucket realizes negative net R. MODEL_READY = NO; DECISION_PAYOFF_READY = NO; REJECTED_PRE_HOLDOUT / RESEARCH_ONLY. No runtime pin or CATI authority; M0 paper runtime remains unchanged at a 2.5% hard daily loss cap. Holdout opened/inspected = NO, query count = 0. See [V5 report](cati_v5_development_report.md) and [structured result](cati_v5_development_result.json).


## Economic edge viability audit

The existing V5 audit yields primary diagnosis MIXED: weak/slightly negative gross alpha, substantial cost/geometry burden, temporal deterioration and optimistic high-score payoff estimates. All seven fixed pooled expectancy tails lose net; isolated positive threshold/group cells do not establish stable selectable edge. The full 3,007,222-candidate parent check averages -0.015416 gross and -0.155809 net R. No V6, new fit, production filter, threshold change, pin, holdout query or runtime change. M0 / CATI OFF continues. Next research: separately versioned setup/geometry alpha and execution-cost realism before further ML. See [audit report](cati_economic_edge_viability_audit.md).
