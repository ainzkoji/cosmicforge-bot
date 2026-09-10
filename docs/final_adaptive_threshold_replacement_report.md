# Final Threshold System Replacement — Removal Report

The old threshold architecture is **deleted**, not deprecated. There is one
engine, one policy, one band, one final threshold.

Companion documents:

* [`final_threshold_removal_inventory.md`](final_threshold_removal_inventory.md) — the frozen pre-deletion audit.
* [`adaptive_entry_threshold_engine_rebuild_report.md`](adaptive_entry_threshold_engine_rebuild_report.md) — how the new engine works.
* [`entry_threshold_provenance_report.md`](entry_threshold_provenance_report.md) — the original forensic trace. Historical; untouched.

---

## 1. Before

Five components resolved the entry threshold in series, none aware of the
others:

```
dynamic_threshold.py     percentile clamped to [0.40, 0.65], fallback 0.45
  -> adaptive/engine.py  min_confidence_gate = base + confidence_gate_modifier
  -> effective_policy.py confidence_absolute_floor = max(0.70, 0.55) = 0.70
  -> runner.py:3883      max(adaptive_gate, 0.70)         <-- SATURATION
  -> master_ensemble.py  max(_threshold_val, 0.55)         (no-op)
```

Plus two more that could reject an already-approved entry: `SafetyEngine`
Gate 3 (which resolved its *own* threshold from the same calculator and capped
it at its *own* floor) and `PolicyEngine`'s `LOW_CONFIDENCE` gate.

**Seven components could decide or veto entry quality. One number won, always:
0.70.**

---

## 2. Deleted

### Source files (`git rm`)

| Path | Why |
| --- | --- |
| `app/risk/dynamic_threshold.py` | `DynamicThresholdCalculator`, the rolling percentile window, `log_threshold_event`, and the module-level MIN/MAX/FALLBACK bounds. It independently calculated an entry threshold — a second authority by definition. **Not retained as RESEARCH_ONLY.** |
| `tests/test_dynamic_threshold.py` | Asserted the deleted algorithm. |
| `scripts/validation/replay_runtime_equivalent.py` | A replay-specific threshold reproducer. Superseded by `app/replay/`, which drives the real engine through the real runner. |
| `tests/test_runtime_equivalent_replay.py` | Tested the above. |

### Configuration keys

`MIN_CONFIDENCE_THRESHOLD` (migrated once, then deleted),
`ENSEMBLE_MIN_THRESHOLD_FLOOR`, `DYNAMIC_THRESHOLD_ENABLED`,
`DYNAMIC_THRESHOLD_MIN`, `DYNAMIC_THRESHOLD_MAX`, `DYNAMIC_THRESHOLD_FALLBACK`,
`DYNAMIC_THRESHOLD_MIN_SAMPLES`, `DYNAMIC_THRESHOLD_WINDOW_SIZE`,
`DYNAMIC_THRESHOLD_PERCENTILE`.

Removed from `config.py`, `.env`, `.env.example` and `.env.data_collection`.

### Code-level authorities

| Removed | Where |
| --- | --- |
| `SafetyEngine` **Gate 3** + `min_confidence_hard` / `min_confidence_soft` | `app/risk/safety_engine.py` |
| `PolicyEngine.min_confidence` + its `LOW_CONFIDENCE` gate | `app/policy/policy_engine.py` |
| `SystemLimits.min_strategy_confidence` | `app/risk/system_limits.py` |
| `BotContext.min_confidence` | `app/runner/bot_context.py` |
| `AdaptiveState.min_confidence_gate` + `get_adaptive_state(base_threshold=...)` | `app/adaptive/engine.py` |
| `AdaptiveState.threshold_adjustment` alias | `app/adaptive/engine.py` |
| `consensus_threshold` constructor arg + `params_schema` entry | `app/strategy/master_ensemble.py` |
| Four runner call sites that fetched a dynamic threshold | `app/runner/runner.py` |
| `confidence_absolute_floor` | `app/runner/effective_policy.py` (already removed in the rebuild) |

### Re-domained, not deleted

`confidence_gate_modifier` → **`caution_modifier`**, and `ConfidenceGatePolicy`
→ **`LossStreakCautionPolicy`**. Same arithmetic, same bounds `[0, 0.12]`, but
it is no longer expressed as threshold units and its only consumer is the
aggressiveness score, which governs **size and leverage**. The adaptive engine
keeps those responsibilities; it lost only its entry-threshold responsibility.

The persisted `decision_traces.confidence_gate_modifier` column and the ML
feature of the same name keep their names deliberately: one is a storage schema
holding historical rows, the other is a trained-model input. Renaming either
would rewrite history or silently break inference. Both are now *sourced* from
`caution_modifier`.

---

## 3. The one configuration contract

```
THRESHOLD_ENGINE_MODE   ADAPTIVE | STATIC | RESEARCH     (MODEL is rejected)
THRESHOLD_BASE          0.70   (migrated once from MIN_CONFIDENCE_THRESHOLD)
THRESHOLD_MIN           0.50
THRESHOLD_MAX           0.90
THRESHOLD_STATIC        required when mode is STATIC

THRESHOLD_REGIME_ADJUSTMENT_MAX          0.06
THRESHOLD_VOLATILITY_ADJUSTMENT_MAX      0.05
THRESHOLD_AGREEMENT_ADJUSTMENT_MAX       0.08
THRESHOLD_HTF_ADJUSTMENT_MAX             0.05
THRESHOLD_MARKET_QUALITY_ADJUSTMENT_MAX  0.04
THRESHOLD_PERFORMANCE_ADJUSTMENT_MAX     0.05
THRESHOLD_DISTRIBUTION_ADJUSTMENT_MAX    0.05

THRESHOLD_SMOOTHING_ALPHA / _MAX_STEP_UP / _MAX_STEP_DOWN
THRESHOLD_PERFORMANCE_MIN_SAMPLES / _LOOKBACK
THRESHOLD_DISTRIBUTION_MIN_SAMPLES / _WINDOW / _PERCENTILE
THRESHOLD_HARD_BLOCK_REGIMES
THRESHOLD_SCOPED_OVERRIDES
```

One namespace. No aliases. `policy.py::SETTING_MAP` is the whole surface, and a
test asserts every `THRESHOLD_*` setting reaches the resolved policy — a setting
an operator can see and set, which cannot affect behaviour, is the defect this
removal exists to end.

### Legacy keys now FAIL startup

`config.detect_legacy_threshold_keys()` scans both the process environment and
the `.env` file, and configuration validation fails with
**`LEGACY_THRESHOLD_CONFIGURATION_PRESENT`**, listing the offenders.

Both locations are checked on purpose: pydantic ignores unknown `.env` keys, so
a reinstated `MIN_CONFIDENCE_THRESHOLD=0.70` would otherwise sit in the file
looking authoritative and doing nothing — which is precisely the failure mode
being removed.

**The base is resolved from `THRESHOLD_BASE` and nowhere else.** There is no
second branch. If it is unset, resolution raises `THRESHOLD_BASE_UNRESOLVED`;
the engine does not invent an entry bar, and no legacy setting is consulted.

---

## 4. Inputs wired for real

**HTF (§15) — now genuinely wired.** `_htf_context` derives direction and
strength from `MarketSnapshot.higher_timeframe_candles`, which are already
filtered to closed candles. No network request, no in-progress candle.
Direction is price vs the HTF EMA200 with the same 0.05% buffer the existing
HTF bias veto uses — reusing that definition rather than inventing a second
notion of "the 4h trend". Strength is distance from the EMA normalised by the
EMA, saturating at 5%, so it is comparable across instruments.

Unavailable stays unavailable: no HTF series, fewer than 200 candles (an EMA200
from 40 candles is a number, not a trend), or a timestamp-misaligned HTF candle
all yield a context that reports nothing rather than "neutral".

**Market quality (§16) — partial, and labelled partial.** Populated:
`volume_percentile`, `price_discontinuity` (an open gapping >0.5% from the
previous close), and `data_stale`. Left `None`: `spread_percentile`,
`estimated_slippage_bps`, `liquidity_score` — the MarketSnapshot carries no
order-book data. The engine skips absent inputs; filling them with a
neutral-looking default would claim knowledge the runtime does not have.

---

## 5. Evidence and the database

**No historical trading evidence was deleted, rewritten or backfilled.**

Verified against the pre-deletion backup
(`backups/cosmicforge_20260909T225655Z_pre_legacy_threshold_removal.db`,
3,482,271,744 bytes, taken with SQLite's online backup API while the runtime
was live):

```
trade_fills  backup: 17561 rows  0ee1d88b19029046ac0d33618c76d5ca4c0d8a427538934943607f5f03aca43b
trade_fills  live  : 17561 rows  0ee1d88b19029046ac0d33618c76d5ca4c0d8a427538934943607f5f03aca43b
ECONOMIC FINGERPRINT IDENTICAL: True        (all 64 columns, every row)

historical trading_decisions backup: 4708 rows  138b68c854913561dfebd5be7714baeb...
historical trading_decisions live  : 4708 rows  138b68c854913561dfebd5be7714baeb...
HISTORICAL DECISIONS IDENTICAL: True
```

Row growth in `trading_decisions`, `trading_cycles`, `canonical_trade_decisions`
and `equity_snapshots` is the live runtime continuing to write new rows during
the work. Additive only.

### LEGACY_READ_ONLY_EVIDENCE_COLUMN

`trading_decisions.consensus_required` is retained and never written with a
value. Dropping it would destroy the record of what the old stack decided. The
recorder writes `NULL`, and a test asserts it cannot start writing a number
again. The exemption in the invariant scan is per-identifier and per-file, not
file-wide, so any *other* deleted control appearing in the recorder still fails.

---

## 6. Tests

### New: `tests/test_threshold_authority_invariants.py`

* **19 forbidden identifiers** scanned across every production module in
  `app/` and `shared_lib/`, with docstrings stripped so prose can neither
  satisfy nor break a check.
* **Single-authority test.** Parses production source and fails if any module
  other than `app/threshold/engine.py` *computes* a final threshold —
  distinguishing computation (arithmetic, a conditional, `max`/`min`/`_clamp`,
  a non-passthrough call) from consumption (a bare name or attribute read).
  A companion test asserts the authority still *is* an authority, so the check
  cannot pass vacuously.
* Downstream consumers proven unable to modify the decision: the decision engine
  contains no `max(`, `min(` or `_clamp`, and `AdaptiveThresholdDecision` is
  frozen.
* Every `.env` file scanned for deleted keys.

### Rewritten rather than deleted

Tests that asserted deleted behaviour were re-pointed at the property they were
protecting, not dropped:

| Was | Now |
| --- | --- |
| `PolicyEngine` blocks low confidence | `PolicyEngine` has no confidence gate at all |
| Safety/Policy *bypass* their gate when quality approved | Both gates are **deleted** — nothing to bypass |
| `PolicyEngine` reads `MIN_CONFIDENCE_THRESHOLD` from settings (F-2) | It takes no confidence threshold; the setting is gone |
| Inactivity still rejected at PolicyEngine's 0.70 floor (B-6) | Inactivity cannot soften the bar, tested in the distribution calibrator where the bar now lives; plus trade counts proven absent from the engine |
| `DynamicThresholdCalculator` multi-tenant isolation | `ThresholdStateStore` isolation: per bot, per symbol, per timeframe, and copy-on-read |
| `DynamicThresholdCalculator.adaptive_offset` integration | Deleted with the calculator; a comment records where the coverage went |

Deleting a failing test to reach green would have removed the guarantee. Each
one above still guards its original property.

---

## 7. Rollback

Rollback is `git revert` of these commits. There is no runtime rollback path,
no compatibility flag and no legacy environment variable that re-enables the old
stack — retaining one would mean retaining a second way to compute an entry
threshold, which is the thing being removed.

Database migrations are forward-safe and additive; historical evidence is
untouched, so a code revert needs no data migration.

---

## 8. Frontend / admin contract

No frontend or admin control writes a bot threshold setting. The only matches
for `minConfidence` / `min_confidence` in `frontends/` are the **signal-list
display filter** (`Signals.tsx`, `SignalFilters.tsx`), bound to the user
preference `minimum_confidence` and sent as a query parameter to the signals
endpoint. It filters what a human sees. It cannot influence the bot's entry
threshold, and it is unchanged.

`backends/admin-backend` contains one historical deployment-note string in an ML
monitoring record. It is a record, not a control.

Admin threshold routes expose the resolved policy only:
`/threshold/status/{bot_id}`, `/threshold/decisions/{id}`,
`/threshold/history/{bot_id}`, `/threshold/policy`.


---

## 9. Final repository search (§39)

Executable production code only -- docstrings stripped, comments absent from the
AST, tests and reports excluded:

```
MIN_CONFIDENCE_THRESHOLD          0
ENSEMBLE_MIN_THRESHOLD_FLOOR      0
DYNAMIC_THRESHOLD_MIN             0
DYNAMIC_THRESHOLD_MAX             0
DYNAMIC_THRESHOLD_FALLBACK        0
DYNAMIC_THRESHOLD_MIN_SAMPLES     0
consensus_threshold               0
consensus_required                0
confidence_absolute_floor         0
min_confidence_gate               0
DynamicThresholdCalculator        0
get_dynamic_threshold_calculator  0
min_confidence_hard               0
min_confidence_soft               0
min_strategy_confidence           0
```

Allowed occurrences, by design: `app/threshold/migration.py` (the permanent
removal record), `app/core/config.py` (the rejection list has to name the keys to
reject them), and `consensus_required` in the recorder and schema (the
LEGACY_READ_ONLY_EVIDENCE_COLUMN, written NULL). Every one is asserted by
`tests/test_threshold_authority_invariants.py`.

---

## 10. Runtime acceptance

The runtime was stopped through its own operator stop-file with the bot flat
(0 open positions, 0 in-flight execution attempts) and restarted supervised.

| Check | Result |
| --- | --- |
| Runtime on final HEAD | `code_revision = 28341638` |
| Runtime ownership | exactly one unreleased lease, pid 34468 |
| MultiBotRunner | one, initialised, cycling -- 44 cycles in 8 minutes |
| Incomplete decisions | 0 |
| BTCUSDT clock | candle 00:29:59.999Z evaluated 00:30:23Z |
| ETHUSDT clock | candle 00:29:59.999Z evaluated 00:30:25Z |
| Legacy config loaded | **none** -- 0 occurrences of the old stack in startup logs |
| `LEGACY_THRESHOLD_CONFIGURATION_PRESENT` | not raised; the live `.env` is clean |
| Startup errors | none |

Hard-gate evidence on genuine candles after the restart:

```
00:30:23  BTCUSDT  SESSION_BLOCKED  effective_entry_threshold=NULL  threshold_status=NOT_EVALUATED
00:30:25  ETHUSDT  SESSION_BLOCKED  effective_entry_threshold=NULL  threshold_status=NOT_EVALUATED
```

### The first live EVALUATED threshold decision

At **01:30:05 UTC** an ETHUSDT candle reached the quality stage and the engine
ran end to end in production. Nothing was forced: no gate weakened, no threshold
lowered, no trade manufactured.

```
threshold_decision_id     thr_b1cd0f630be34451
symbol / timeframe        ETHUSDT 15m          regime WEAK_TREND
status                    EVALUATED            mode ADAPTIVE   engine 1.0.0
policy_hash               afd2b463...1f17802   provenance PAPER_FORWARD

base_threshold                          0.700000
  regime_adjustment                    +0.018000   WEAK_TREND, confidence-weighted
  volatility_adjustment                -0.005000   percentile 0.642, healthy band
  agreement_adjustment                 +0.044596   agreement 0.221: 1 of 4 experts voted
  htf_adjustment                       +0.050000   alignment -1.0: 4h fully opposed
  market_quality_adjustment            +0.002667   quality 0.467
  performance_adjustment                0.000000   INSUFFICIENT_SAMPLE
  distribution_adjustment               0.000000   INSUFFICIENT_SAMPLE
= raw_unclamped_threshold               0.810263
-> smoothed / rate_limited / final      0.810263   no previous; band [0.50, 0.90]

opportunity_confidence                  0.346667
passed                                  false      THRESHOLD_NOT_MET
reconciles                              true       components - raw = 0.00e+00
```

Four things this proves that unit tests cannot:

1. **The arithmetic reconciles exactly in production** -- 0.00e+00, not merely
   within tolerance.
2. **HTF is genuinely wired.** `htf_alignment_score = -1.0` is a real value
   derived from closed 4h candles. Before this pass it was structurally always
   absent.
3. **All seven experts persisted, from the production evaluation**, with
   `NOT_RUN` and `DISABLED` correctly distinguished:

   | strategy | eligible | executed | signal | conf | weight | contribution |
   | --- | --- | --- | --- | --- | --- | --- |
   | supertrend | yes | yes | HOLD | 0.00 | 1.50 | 0.000 |
   | trend_pullback | yes | yes | **SELL** | 0.80 | 1.30 | 1.040 |
   | donchian_breakout | yes | yes | HOLD | 0.00 | 1.00 | 0.000 |
   | sma_cross | yes | yes | HOLD | 0.00 | 0.90 | 0.000 |
   | bollinger_reversion | no | no | DISABLED | 0.00 | 1.00 | 0.000 |
   | squeeze_breakout | no | no | DISABLED | 0.00 | 1.10 | 0.000 |
   | vwap_reversion | no | no | DISABLED | 0.00 | 1.20 | 0.000 |

4. **The engine raised the bar on weak evidence.** One expert of four voting,
   with the 4h timeframe fully opposed, produced **0.810** -- higher than the
   old constant 0.70, not lower. The `trading_decision` links to it by
   `threshold_decision_id` and records `ENTRY_CONFIDENCE_BELOW_THRESHOLD` with
   `effective_entry_threshold = 0.810263`. Adaptive state persisted for
   ETHUSDT/15m with `previous_threshold = 0.810263`, so the next candle smooths
   against it.

The other nine post-restart evaluations were hard-gated and carry
`final_threshold = NULL` / `threshold_status = NOT_EVALUATED`, as specified.

**Live acceptance remains PARTIAL** under §37: one opportunity is not a sample
from which to show the threshold varies across contexts. That needs several
organic opportunities, and the session window does not open until 06:00 UTC.
The single decision proves the engine runs, reconciles and adapts its
components; it does not yet prove a distribution.

### Open observation, not caused by this work

At 01:30 the two symbols diverged: BTCUSDT recorded `SESSION_BLOCKED` while
ETHUSDT passed the session gate and evaluated, **two seconds apart, in the same
bot, both with `market_type = CRYPTO` on record**. The session gate is a UTC
time-window check and should treat both identically, and the CRYPTO 24/7 bypass
should have applied to both or neither.

I did not establish the cause, and it is outside this mandate: both affected
code paths predate the threshold work, and nothing changed here touches the
session gate. It is flagged because it means one symbol may be reaching the
quality stage on a rule the other is not. Worth a separate look.

---

## 11. Acceptance

```
OLD_DYNAMIC_THRESHOLD_ENGINE:              DELETED (git rm, module and test)
MIN_CONFIDENCE_THRESHOLD:                  DELETED (migrated once, then removed;
                                           reappearance fails startup)
ENSEMBLE_MIN_THRESHOLD_FLOOR:              DELETED
LEGACY_CONSENSUS_GATE:                     DELETED (ctor arg and params_schema)
LEGACY_ADAPTIVE_CONFIDENCE_GATE:           DELETED (min_confidence_gate and the
                                           threshold_adjustment alias)
LEGACY_RUNNER_THRESHOLD_OVERRIDE:          DELETED
LEGACY_SAFETY_CONFIDENCE_AUTHORITY:        DELETED (Gate 3 + soft/hard config)
LEGACY_ENV_THRESHOLD_KEYS:                 DELETED (.env, .env.example,
                                           .env.data_collection)
LEGACY_API_THRESHOLD_CONTROLS:             NOT_APPLICABLE (none existed; routes
                                           expose the resolved policy only)
LEGACY_FRONTEND_THRESHOLD_CONTROLS:        NOT_APPLICABLE (the only match is a
                                           signal-list display filter)
OLD_THRESHOLD_SOURCE_FILES:                MIGRATED_THEN_DELETED

HISTORICAL_TRADING_EVIDENCE:               PRESERVED (fingerprints identical)

ACTIVE_THRESHOLD_ENGINE_COUNT:             1
ACTIVE_THRESHOLD_ENGINE:                   AdaptiveEntryThresholdEngine
ACTIVE_THRESHOLD_POLICY:                   EffectiveThresholdPolicy
THRESHOLD_CONFIGURATION_NAMESPACE:         NEW_ONLY

HTF_RUNTIME_INPUT:                         PASS (closed HTF candles, EMA200, no
                                           network call, unavailable stays NULL)
MARKET_QUALITY_RUNTIME_INPUT:              PARTIAL (volume percentile, price
                                           discontinuity, staleness; spread,
                                           slippage and depth stay NULL because
                                           the snapshot has no order-book data)
SEVEN_EXPERT_EVIDENCE:                     PASS
NOT_EVALUATED_NULL_SEMANTICS:              PASS (verified in production)
NO_CROSS_SYMBOL_LEAK:                      PASS
NO_CROSS_CANDLE_LEAK:                      PASS
RESTART_SAFE_STATE:                        PASS
REPLAY_USES_IDENTICAL_ENGINE:              PASS (the replay-specific reproducer
                                           is deleted; 18/18 parity tests pass)
LEGACY_CONFIG_REINTRODUCTION_BLOCKED:      PASS (env + .env scanned; fails startup)
ARCHITECTURAL_SINGLE_AUTHORITY_TEST:       PASS
TEST_DATABASE_ISOLATION:                   PASS

FULL_TEST_SUITE:                           2392 passed, 0 failed, 0 skipped,
                                           4 subtests passed, 27 warnings
                                           (pre-removal baseline: 2377 passed)
                                           compileall clean; git diff --check clean
                                           (ruff/mypy/flake8 not installed here)

RUNTIME_ON_FINAL_HEAD:                     PASS (28341638)
LIVE_HARD_GATE_EVIDENCE:                   PASS
LIVE_EVALUATED_THRESHOLD:                  PASS (ETHUSDT 01:30:05 UTC,
                                           thr_b1cd0f630be34451, final 0.810263,
                                           reconciles exactly, 7 experts persisted)
LIVE_ADAPTATION_PROOF:                     PARTIAL (n=1 opportunity; a
                                           distribution needs several, and the
                                           session window opens at 06:00 UTC)

DOWNSTREAM_THRESHOLD_OVERRIDE:             NONE
HIDDEN_THRESHOLD_FLOOR:                    NONE

THRESHOLD_ENGINE_VERSION:                  1.0.0
THRESHOLD_BASE:                            0.70
THRESHOLD_MIN:                             0.50
THRESHOLD_MAX:                             0.90

THRESHOLD_PARAMETERS_OPTIMIZED:            NO
TRADING_FREQUENCY_TARGETED:                NO
AI_SELF_TUNING:                            DISABLED
SAFE_FOR_CONTINUED_PAPER:                  YES
SAFE_FOR_AI_ACTIVATION:                    NO
SAFE_FOR_MAINNET:                          NO
```
