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
