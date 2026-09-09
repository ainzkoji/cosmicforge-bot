# Adaptive Entry Threshold Engine — Rebuild Report

**No threshold was tuned to produce trades.** The base was migrated from the
existing configuration rather than chosen, and which base is correct remains an
open research question (see §11).

---

## 1. What was wrong

The entry threshold was decided by five components in series, none of which knew
the others existed:

```
dynamic_threshold.py     percentile, clamped to [0.40, 0.65], fallback 0.45
  -> adaptive/engine.py  min_confidence_gate = base + confidence_gate_modifier
  -> effective_policy.py confidence_absolute_floor = max(0.70, 0.55) = 0.70
  -> runner.py:3883      max(adaptive_gate, 0.70)          <-- SATURATION
  -> master_ensemble.py  max(_threshold_val, 0.55)          (no-op)
```

Because `MIN_CONFIDENCE_THRESHOLD = 0.70` sat **above** the dynamic system's hard
cap of `0.65`, `max(<= 0.65, 0.70)` was unconditionally `0.70`. Every candle
resolved to exactly 0.70; all 17 rows with a resolved threshold in the organic
window recorded 0.70 and nothing else.

Three consequences, in increasing order of seriousness:

1. The entire dynamic threshold subsystem was dead code that still ran.
2. `ENSEMBLE_MIN_THRESHOLD_FLOOR` was **documented in `config.py` as the binding
   constraint** and could not bind. The operator tuning recorded in `.env` as
   *"T-04: Raise confidence floor to 0.55 (was 0.50 default)"* changed nothing.
3. Nothing anywhere reported any of this. A stable threshold looked like a
   healthy engine.

---

## 2. Old threshold architecture — full inventory

Every legacy control, with its disposition. This table is published at runtime by
`app.threshold.migration.legacy_inventory()` and asserted by
`test_every_legacy_setting_has_a_valid_disposition`.

| Setting | Disposition | Where it lived | What happened |
| --- | --- | --- | --- |
| `MIN_CONFIDENCE_THRESHOLD` | **MIGRATED** | `app/core/config.py` | Becomes `EffectiveThresholdPolicy.base_threshold` when `THRESHOLD_BASE` is unset. No longer applied as a floor after the fact, so it can now be adapted away from rather than saturating the band. |
| `ENSEMBLE_MIN_THRESHOLD_FLOOR` | **REMOVED** | `config.py` + `master_ensemble.py` | Removed as an authority. Was dominated by 0.70 and could never bind. Replaced by the single band `THRESHOLD_MIN`/`THRESHOLD_MAX`. Still read, only to emit a deprecation warning. |
| `DYNAMIC_THRESHOLD_MIN` | RESEARCH_ONLY | `app/risk/dynamic_threshold.py` | Calculator retained and its rolling `record()` still fed, so research keeps a continuous series. It no longer resolves the entry threshold. |
| `DYNAMIC_THRESHOLD_MAX` | RESEARCH_ONLY | same | same |
| `DYNAMIC_THRESHOLD_FALLBACK` | RESEARCH_ONLY | same | same |
| `DYNAMIC_THRESHOLD_MIN_SAMPLES` | RESEARCH_ONLY | same | same |
| `consensus_threshold` | **REMOVED** | `master_ensemble.py` + `decision_engine.py` | The constructor default of 0.40 was stored and never compared against anything, because the ensemble passed `consensus_required=0.0`. Expert agreement is now one bounded input to the threshold. Also removed from the strategy `params_schema`, where it was advertised as tunable. |
| `consensus_required` | **REMOVED** | `decision_engine.py` | Removed with it. `consensus_observed` survives as evidence; `consensus_required` is written NULL because no requirement is applied. |
| `confidence_absolute_floor` | **REMOVED** | `app/runner/effective_policy.py` | Removed from `EffectiveBotPolicy`. Replaced by `threshold_policy_hash`, `threshold_mode`, `threshold_min`, `threshold_max` — the bot policy now records *which* threshold policy governed the run instead of duplicating its number. |
| `runner.min_confidence_gate_max` | **REMOVED** | `app/runner/runner.py:3883` | The `max(adaptive_gate, context.min_confidence)` that saturated the chain. Deleted. The runner no longer computes or raises a threshold. |
| `adaptive.min_confidence_gate` | RETAINED_AS_NON_AUTHORITY | `app/adaptive/engine.py` | Still computed and logged as adaptive-engine observability; nothing consumes it as a threshold. The adaptive engine keeps its **size and leverage** authority. |
| `BotContext.min_confidence` | RETAINED_AS_NON_AUTHORITY | `app/runner/bot_context.py` | Sourced from the policy band floor, and passed only to PolicyEngine — whose confidence check the orchestrated path already bypasses via `confidence_already_approved`. |
| `SafetyEngine.min_confidence_soft/hard` | RETAINED_AS_NON_AUTHORITY | `app/risk/safety_engine.py` | Hard safety backstop for callers outside the orchestrated path. |
| `ENSEMBLE_BLOCKED_REGIMES` | LEGACY_COMPATIBILITY | `config.py` | Still a hard regime gate, deliberately outside the threshold engine. |

**One active final threshold authority: `AdaptiveEntryThresholdEngine`.**

---

## 3. New architecture

```
MarketSnapshot
  -> RegimeClassifier
  -> Master Ensemble (7 experts, weighted vote)
  -> TradingOpportunity
  -> AdaptiveEntryThresholdEngine  ──► AdaptiveThresholdDecision
  -> TradingDecisionEngine            (compares, does not compute)
  -> Risk -> ExecutionFeasibility -> EntryProtection -> Executor
```

The computation, in one shape:

```
base                                (policy)
  + regime_adjustment          \
  + volatility_adjustment       |
  + agreement_adjustment        |   fast market context
  + htf_adjustment              |
  + market_quality_adjustment  /
  = market_threshold

  + performance_adjustment     \    slow calibration
  + distribution_adjustment    /
  = raw_unclamped_threshold

  -> smoothing (EMA vs previous)
  -> rate limiting (max step up/down)
  -> clamp to the ONE policy band
  = final_threshold
```

**There is no step after the clamp.** That is the entire point: the old stack's
final act was `max(dynamic, 0.70)`, applied by a component that did not know a
threshold had already been resolved.

### New modules

| File | Role |
| --- | --- |
| `app/threshold/contracts.py` | `AdaptiveThresholdInput`, `AdaptiveThresholdDecision`, `ExpertEvidence`, statuses, modes |
| `app/threshold/policy.py` | `EffectiveThresholdPolicy`, scope resolution, `validate_policy` |
| `app/threshold/engine.py` | The engine and its five market-context components |
| `app/threshold/calibration.py` | Performance and opportunity-distribution calibration |
| `app/threshold/state.py` | Restart-safe, partitioned adaptive state |
| `app/threshold/diagnostics.py` | Health reasons and inert-engine detection |
| `app/threshold/migration.py` | Legacy inventory, config migration, startup report |
| `app/threshold/persistence.py` | Threshold and expert evidence writers |
| `app/threshold/runtime.py` | Process wiring; `research_policy()` override |
| `app/api/threshold_diagnostics.py` | Operator routes |

---

## 4. Policy precedence

```
GLOBAL -> ASSET_CLASS -> VENUE -> SYMBOL -> BOT
```

A narrower scope may change the band, the adjustment bounds, the calibration
parameters or the hard-blocked regimes. It **may not introduce a second
authority** — every scope configures the same fields of the same engine.

Two properties worth stating:

* An **unknown field in a scope override raises** rather than being ignored. A
  silently-ignored typo is exactly how a setting becomes inert.
* Two identical policies assembled through different scopes **hash the same**, so
  a scope reorganisation that changes no value does not look like a policy
  change.

### Resolved configuration at startup

```
[THRESHOLD_ENGINE] resolved configuration
  threshold_engine = AdaptiveEntryThresholdEngine
  mode             = ADAPTIVE
  policy_version   = 1.0.0
  policy_hash      = afd2b4636b9d89e9d99dabc46ea08338386e717e66d7b8cb226911c601f17802
  base             = 0.7  (from MIN_CONFIDENCE_THRESHOLD (migrated))
  band             = [0.5, 0.9]
  adjustment_bounds= {regime 0.06, volatility 0.05, agreement 0.08, htf 0.05,
                      market_quality 0.04, performance 0.05, distribution 0.05}
  calibration      = {performance_min_samples 30, performance_lookback 100,
                      distribution_min_samples 40, distribution_window 200,
                      distribution_target_percentile 0.6}
  smoothing        = {alpha 0.35, max_step_up 0.05, max_step_down 0.03}
  hard_block       = ['LOW_VOLATILITY_CHOP']
  active final threshold authorities = 1 (AdaptiveEntryThresholdEngine)
  DEPRECATED: ENSEMBLE_MIN_THRESHOLD_FLOOR=0.55 ... no longer affects the entry threshold.
  DEPRECATED: MIN_CONFIDENCE_THRESHOLD=0.7 is DEPRECATED as a floor ...
```

**On the base of 0.70.** It is the migrated legacy value, not a chosen one. The
engine refuses to invent a base: if neither `THRESHOLD_BASE` nor
`MIN_CONFIDENCE_THRESHOLD` is available, policy resolution raises. Migrating
rather than re-picking keeps the entry bar's centre where the operator last put
it, and leaves the question of the right value to the sensitivity study.

**This does change live behaviour**, and it should be stated plainly: under the
old stack the threshold was 0.70 on every candle. It can now move within
`[0.50, 0.90]`. Contexts that argue for more evidence produce a higher bar than
before; contexts that argue for less produce a lower one. That is what "adaptive"
means, and it is the requested change — but it is a behaviour change, not a
refactor.

---

## 5. Hard gates stayed outside

Operational and safety failures are **not** encoded by pushing the threshold up.
A blocked regime is recorded as `HARD_BLOCKED` with `final_threshold = NULL`, not
as `threshold = 0.99`.

`LOW_VOLATILITY_CHOP` remains a capital-preservation hard block, declared in
policy as `hard_block_regimes`. Stale market data hard-blocks. Kill switch, daily
loss limit, position limits, capital, correlation, ATR/stop validity, R:R,
duplicate entry, min-notional, broker availability and execution feasibility all
remain where they were, untouched.

Tested directly: an opportunity at confidence 0.95 in a hard-blocked regime
returns `HARD_BLOCKED` with `passed = None`. It cannot buy its way past.

The engine module is also asserted to contain no reference to `daily_loss`,
`kill_switch`, `max_open_positions`, `min_notional`, `position_size` or
`leverage`.

---

## 6. NOT_EVALUATED semantics

`final_threshold` is `NULL`, never `0.0`, whenever nothing was evaluated. This is
enforced in three places rather than by convention:

* `AdaptiveThresholdDecision.__post_init__` **raises** if a `NOT_EVALUATED`,
  `HARD_BLOCKED`, `INSUFFICIENT_DATA` or `ERROR` decision carries a number, and
  raises if an `EVALUATED` one does not.
* `EntryQualityDecision.effective_entry_threshold` is now `float | None`
  defaulting to `None` — it previously defaulted to `0.0`, which is how 51 rows
  came to record an entry threshold of zero.
* The schema column is nullable and the writer passes `None` through.

`TradingDecisionEngine` refuses to compare against a missing threshold at all: no
threshold decision means the verdict is the threshold's own reason, not
`ENTRY_CONFIDENCE_BELOW_THRESHOLD`. The `0.0 >= 0.0` diagnostic nonsense cannot
recur.

---

## 7. Evidence model

New tables (`evidence_schema.py`, additive, `CREATE TABLE IF NOT EXISTS`):

* **`threshold_decisions`** — every component of every calculation: base, all
  seven adjustments, both calibration scores and sample sizes, market threshold,
  raw/smoothed/rate-limited/final, bounds, previous, the three "was it modified"
  flags, and whether the arithmetic reconciles.
* **`expert_evaluations`** — per-expert `eligible`, `executed`, `signal`,
  `confidence`, `raw_score`, `weight`, `weighted_contribution`, `reason`.
* **`adaptive_threshold_state`** — the restart-safe state, keyed on
  `(bot, symbol, timeframe, strategy_version)`.

`trading_decisions` gains `threshold_decision_id`, `threshold_status`,
`threshold_engine_version`, `threshold_mode`.

**Reconciliation.** `decision.reconcile()` asserts that base plus every recorded
adjustment equals `raw_unclamped_threshold` to within 1e-6. It deliberately
checks the *raw* value: smoothing, rate limiting and clamping are allowed to move
the number, and each records its own flag saying it did. A decision whose
components do not produce its value is a lie, and `health_check` reports
`THRESHOLD_COMPONENTS_DO_NOT_RECONCILE`.

**Historical rows are untouched.** No old `TradingDecision` was deleted,
rewritten or backfilled. Rows written by the old stack keep exactly what it
recorded. Trustworthy threshold evidence begins at engine version `1.0.0`.

---

## 8. The stale-evidence defect (fixed)

`last_opportunity` and `last_entry_quality` were assigned in `__init__` and then
written only at Step 7. Every path that returned earlier — no new candle, blocked
regime, no valid votes, an error — left the **previous symbol's** opportunity on
the instance for the evidence layer to read and record against this candle. The
forensic audit measured 78/78 contaminated rows with zero counter-examples.

Two fixes:

1. `MasterEnsembleStrategy._reset_evaluation_state()` is now the first statement
   in `get_signal()`. Every evaluation starts from a clean slate.
2. `runner_bridge.apply_evidence` preferred whatever was already set
   (`decision.regime = decision.regime or meta.get("regime")`), so a stale regime
   beat the fresh one. It now takes the fresh regime from the per-evaluation meta
   dict unconditionally.

Regression tests cover both the source-level guarantee and the behaviour.

---

## 9. Diagnostics, health, and the check that would have caught this

Routes (admin-only, no secrets):

* `GET /api/v1/admin/trading/threshold/status/{bot_id}`
* `GET /api/v1/admin/trading/threshold/decisions/{threshold_decision_id}`
* `GET /api/v1/admin/trading/threshold/history/{bot_id}`
* `GET /api/v1/admin/trading/threshold/policy`

Health reasons: `THRESHOLD_ENGINE_INITIALIZATION_FAILED`,
`THRESHOLD_POLICY_INVALID`, `THRESHOLD_STATE_INVALID`, `THRESHOLD_OUT_OF_BOUNDS`,
`THRESHOLD_NOT_FINITE`, `THRESHOLD_COMPONENTS_DO_NOT_RECONCILE`,
`THRESHOLD_ZERO_WHERE_NOT_EVALUATED`, `ADAPTIVE_THRESHOLD_EFFECTIVELY_STATIC`.
These are surfaced in `/operations/health` under `entry_threshold`.

### `detect_inert_engine`

The test that matters is the **disagreement between the raw proposal and the
final value**. If the components produced many distinct raw thresholds and the
final threshold is a single value sitting on a bound, a configuration artifact is
eating the adaptation. That is precisely the shape of `max(dynamic <= 0.65, 0.70)`.

It deliberately does **not** flag a stable threshold on its own. Stable
components producing a stable bar is the engine working, and an alert that fires
on correct behaviour is an alert nobody reads. Both behaviours are tested:
`test_it_catches_the_0_70_shape` and
`test_a_stable_market_is_not_reported_as_a_failure`.

---

## 10. Observed behaviour

Four synthetic contexts through the real engine at the migrated policy:

| Context | final | raw | regime | vol | agree | htf | mq | reconciles |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RANGE, 3 agree, aligned HTF, calm | **0.5914** | 0.5914 | −0.0278 | 0 | −0.0328 | −0.0400 | −0.0080 | yes |
| STRONG_TREND, split vote, opposed HTF | **0.9000** | 0.9195 | +0.0555 | +0.0500 | +0.0770 | +0.0450 | −0.0080 | yes |
| WEAK_TREND, 2 agree, neutral | **0.7304** | 0.7304 | +0.0167 | 0 | +0.0217 | 0 | −0.0080 | yes |
| HIGH_VOLATILITY, 1 agree, 2 not run | **0.8255** | 0.8255 | +0.0444 | +0.0500 | +0.0391 | 0 | −0.0080 | yes |

Four distinct thresholds spanning 0.591 to 0.900, every one reconciling. The
second row is clamped (`raw` 0.9195 → `final` 0.9000) and records
`clamp_applied = True`.

**Live paper runtime observation has not been performed and is not claimed.** The
runtime has not been restarted on this HEAD, and no genuine BTC/ETH candle has
been evaluated by this engine. See the acceptance block.

---

## 11. Unresolved research questions

1. **What should the base be?** 0.70 is inherited, not justified. Settling it is
   the job of the threshold sensitivity study, which remains gated on Phase 13
   production-parity replay and on there being enough organic history for
   out-of-sample evaluation. The organic window still contains **0 trades**, so
   expectancy, profit factor, drawdown, win rate and average R are undefined.
2. **Are the band edges right?** `[0.50, 0.90]` was chosen to leave the engine
   room to move around the migrated base. The STRONG_TREND scenario above already
   clamps at the ceiling, which is worth watching: frequent clamping means the
   band, not the evidence, is deciding.
3. **Are the adjustment bounds calibrated?** They are bounded and directionally
   argued from the existing regime win-rate analysis, but their magnitudes are
   not fitted to anything. Sum of bounds is 0.38 against a band span of 0.40.
4. **Volatility percentile quality.** `_volatility_context` computes an ATR
   percentile against the last 120 candles of the same measure. That is
   normalised and broker-neutral, but it is a coarse estimator.
5. **HTF direction is not yet populated by the runner.** `_htf_context` reads
   `htf_direction`/`htf_strength` from kwargs the runner does not currently
   supply, so in production `htf_adjustment` is 0 until that is wired. The
   component is implemented and tested; the input is not yet connected. Flagged
   rather than faked.
6. **Market quality is partially populated.** Only `volume_percentile` and
   `data_stale` come from the snapshot today. Spread, slippage and depth are
   supported by the contract and left `None` — the engine skips absent inputs
   rather than substituting a neutral-looking default.

---

## 12. What was deliberately not done

* No threshold was optimised, and trade count was not an input to any decision.
* `LOW_VOLATILITY_CHOP` was not unblocked.
* No AI, self-learning, online calibration or self-modifying configuration.
  `MODEL` mode is rejected at startup rather than silently downgraded.
* Mainnet not enabled; no real or user capital involved.
* Historical decisions not rewritten.
* No broker-specific threshold logic; no second replay engine.
