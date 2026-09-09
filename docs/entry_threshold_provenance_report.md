# Entry Threshold Provenance — Why 0.70?

Read-only trace. **No threshold, weight, limit or setting was changed.**

**Answer in one line:** 0.70 is an **intentional configuration override**
(`MIN_CONFIDENCE_THRESHOLD`) applied as an absolute floor *after* the dynamic
threshold is computed — and because it sits **above** the dynamic system's hard
cap of 0.65, it saturates every possible dynamic output and renders the entire
dynamic threshold subsystem inert.

---

## The complete calculation chain

```
STAGE 1  app/risk/dynamic_threshold.py
         percentile over rolling confidence window
         clamped to [MIN_THRESHOLD 0.40 , MAX_THRESHOLD 0.65]
         cold start / disabled -> FALLBACK_THRESHOLD 0.45
              |
              v  base_threshold ∈ [0.40, 0.65]
STAGE 2  app/adaptive/engine.py:685
         min_confidence_gate = base_threshold + confidence_gate_modifier
              |
              v  adaptive gate
STAGE 3  app/runner/effective_policy.py:258
         confidence_absolute_floor = max(
             MIN_CONFIDENCE_THRESHOLD      = 0.70,
             ENSEMBLE_MIN_THRESHOLD_FLOOR  = 0.55,
         )                                 = 0.70
              |
              v
STAGE 4  app/runner/runner.py:3883          <-- THE SATURATION POINT
         min_confidence_gate = max(adaptive_gate, context.min_confidence)
                             = max(<= 0.65 , 0.70)
                             = 0.70   ALWAYS
              |
              v
STAGE 5  app/strategy/master_ensemble.py:378-390
         _threshold_val = _min_gate                     (= 0.70)
         _threshold_val = max(_threshold_val, 0.55)     (no-op)
              |
              v
         effective_entry_threshold = 0.70
```

### Arithmetic proof against live configuration

| Dynamic output | After `max(gate, 0.70)` |
| --- | --- |
| 0.40 (min) | **0.70** |
| 0.45 (fallback) | **0.70** |
| 0.65 (max) | **0.70** |

```
absolute floor (dominates)              : 0.7
dynamic max possible output             : 0.65
is dynamic ever binding?                : False
is ENSEMBLE_MIN_THRESHOLD_FLOOR binding?: False
for dynamic to matter, MIN_CONFIDENCE_THRESHOLD must be < 0.40
```

**Runtime confirmation.** Across the 24-hour organic window, every one of the 17
rows with a resolved threshold recorded **exactly 0.70**. The dynamic value
never once survived.

---

## Classification

| Hypothesis | Verdict |
| --- | --- |
| Intentional final threshold after documented adjustments | **PARTLY YES** |
| **Configuration override** | **YES — primary answer** |
| Stale legacy configuration | **NO** |
| Double application of an adjustment | **NO** (but one redundancy, below) |
| Defect | **NOT in the value — YES in the interaction** |

### Why "intentional" — the 0.70 is deliberate

* `config.py:195` — `MIN_CONFIDENCE_THRESHOLD: float = 0.70` under a
  "Trade Quality Gate" heading.
* `.env:120` — explicitly re-stated `MIN_CONFIDENCE_THRESHOLD=0.70`.
* `config.py:580` — a startup validator **warns if the value drops below 0.70**:
  `"MIN_CONFIDENCE_THRESHOLD={x} is below recommended 0.70."`

Three independent places assert 0.70. This is a considered choice, not drift.

### Why "configuration override" is the accurate label

The dynamic subsystem is fully built — rolling window, percentile logic,
cold-start fallback, `dynamic_percentile` / `dynamic_floor` / `dynamic_cap` /
`fallback_static` bound labels for observability — and **none of it can affect
the outcome**. `max(≤0.65, 0.70)` is unconditionally 0.70. The override does not
adjust the dynamic threshold; it replaces it.

### Why not "stale legacy"

The opposite: comments show the dynamic bounds were *recently curated*.
`dynamic_threshold.py:77` — *"Restored to original design cap; allows
high-conviction regimes to reach 0.65"*; `:78` — *"Stage 2A: raised cold-start
fallback from 0.40 → 0.45"*. Someone deliberately tuned a subsystem that cannot
reach the decision.

### Why not "double application"

Two floors act **in series**, not twice on the same value:

* Stage 4 applies `confidence_absolute_floor` (0.70)
* Stage 5 applies `ENSEMBLE_MIN_THRESHOLD_FLOOR` (0.55)

The second is **redundant** — `ENSEMBLE_MIN_THRESHOLD_FLOOR` is already folded
into `confidence_absolute_floor` at Stage 3 via `max()`, and 0.55 < 0.70 so it
never binds. Redundant, not erroneous. No value is added or compounded.

### Where there *is* a defect: the interaction

Two configuration parameters are documented as controlling the entry bar, and
**neither actually does**:

`config.py:212-215` documents `ENSEMBLE_MIN_THRESHOLD_FLOOR` as:

> *"dynamic threshold can never go below this. Default 0.50 — slightly tighter
> than the historic min floor of 0.40. Raising to 0.55+ further reduces trade
> count but improves quality."*

That description is written as though this parameter is the binding constraint.
It is not. `.env:140-141` records a deliberate tuning action —
*"T-04: Raise confidence floor to 0.55 (was 0.50 default)"* — which **had no
effect whatsoever**, because 0.55 < 0.70 and 0.70 already dominates.

So the defect is not a wrong number. It is that **the system presents three
tunable threshold controls (dynamic bounds, ensemble floor, absolute floor) of
which only one is live**, and the documentation points at one of the dead ones.
A future operator tuning `ENSEMBLE_MIN_THRESHOLD_FLOOR` or the dynamic bounds
would observe zero change and have no indication why.

**Nothing has been changed.** This is reported for decision, not acted on.

---

## The 0.40 disambiguation — three distinct concepts

Your recollection of `0.40 / 0.65 / 0.45` maps **exactly** onto the dynamic
threshold calculator's bounds, and **not** onto the final entry threshold:

```python
# app/risk/dynamic_threshold.py:76-78
MIN_THRESHOLD:      float = _env_float("DYNAMIC_THRESHOLD_MIN", 0.40)
MAX_THRESHOLD:      float = _env_float("DYNAMIC_THRESHOLD_MAX", 0.65)
FALLBACK_THRESHOLD: float = _env_float("DYNAMIC_THRESHOLD_FALLBACK", 0.45)
```

| Concept | Value | Where | Live? |
| --- | --- | --- | --- |
| **Dynamic entry minimum** | **0.40** | `dynamic_threshold.py:76` — clamp floor for the percentile calculation | **No** — output discarded at Stage 4 |
| Dynamic entry maximum | 0.65 | `dynamic_threshold.py:77` | No |
| Dynamic cold-start fallback | 0.45 | `dynamic_threshold.py:78` | No |
| **Consensus threshold** | **0.40** | `master_ensemble.py:187` constructor default | **No** — `consensus_required=0.0` is passed to the engine; never enforced |
| Ensemble minimum floor | 0.55 (`.env`) / 0.50 (default) | `config.py:226` | No — dominated by 0.70 |
| **Final entry threshold** | **0.70** | `MIN_CONFIDENCE_THRESHOLD` via `confidence_absolute_floor` | **YES — the only live control** |

### The trap, stated plainly

**Two different parameters both equal 0.40 and mean completely different
things:**

* `DYNAMIC_THRESHOLD_MIN = 0.40` — the lowest value the *dynamic entry
  threshold* may be clamped to. A bound on a computed threshold.
* `consensus_threshold = 0.40` — a `MasterEnsembleStrategy` constructor
  parameter intended to require a minimum level of *agreement between experts*.
  A bound on vote concordance.

**Neither has ever been the final entry threshold**, and neither is currently
active. The consensus parameter is stored on the instance but never compared
against anything: Master Ensemble passes `consensus_required=0.0` to
`TradingDecisionEngine.evaluate`, deliberately, so that turning it on would not
silently change strategy behaviour. That decision is recorded in
`test_master_ensemble_does_not_introduce_a_new_consensus_gate`.

---

## Implication for the sensitivity study

Sweeping the threshold **cannot be done by changing the dynamic bounds or
`ENSEMBLE_MIN_THRESHOLD_FLOOR`** — those are inert. The only parameter that
moves the effective entry threshold today is `MIN_CONFIDENCE_THRESHOLD`, and
values below 0.40 would additionally re-activate the dynamic subsystem, which
would make the sweep non-comparable (two variables changing at once).

For a clean study each arm must pin the effective threshold to a fixed value —
i.e. hold the dynamic path suppressed exactly as it is today — so that the only
thing varying is the entry bar itself.

---

## Threshold sensitivity study — BLOCKED, not run

The study over `0.40 / 0.45 / 0.50 / 0.55 / 0.60 / 0.65 / 0.70` has **not been
run**, because its stated precondition is not met.

**Phase 13 production-parity replay is incomplete.** `app/replay/engine.py` and
`tests/test_replay_production_parity.py` are present but **uncommitted and
in-progress in another session's working tree**. Running a sensitivity study on
an unvalidated replay engine would produce numbers that look authoritative and
are not — and selecting a live trading threshold from them would be worse than
not running it at all.

Two further blockers, independent of the replay engine:

1. **Sample size.** The organic window contains **10 opportunities and 0
   trades**. Expectancy, profit factor, max drawdown, win rate and average R are
   undefined at zero trades. Even at a 0.40 threshold only the ~0.59 and possibly
   the ~0.30-0.33 candidates would clear — a handful of trades, far too few for
   a risk-adjusted comparison.
2. **Evidence defects.** The two defects in the forensic report (stale
   regime/score contamination in 78 rows; `0.0` written where the threshold was
   never evaluated in 51 rows) would corrupt the regime breakdown and any
   threshold-derived metric in the study output.

### What must be true before the study is meaningful

* Phase 13 replay validated for production parity, by its owning session.
* The two evidence defects fixed, so regime and threshold fields are truthful.
* Enough organic history to make out-of-sample evaluation possible — with 96
  candles/day and ~7% reaching quality, a usable sample is weeks, not a day.
* A pinned harness where each arm varies **only** the effective entry
  threshold, with the dynamic path held in its current suppressed state.

I have not implemented any of this, and no threshold has been changed.

---

## Summary

```
EFFECTIVE_ENTRY_THRESHOLD:        0.70 (observed in 17/17 resolved rows)

ORIGIN:                           MIN_CONFIDENCE_THRESHOLD = 0.70
                                  -> EffectiveBotPolicy.confidence_absolute_floor
                                  -> runner.py:3883  max(adaptive_gate, floor)

CLASSIFICATION:                   CONFIGURATION OVERRIDE (intentional)
                                  + DESIGN DEFECT in the interaction

STALE_LEGACY:                     NO
DOUBLE_APPLICATION:               NO (one redundant but harmless second floor)

DYNAMIC_THRESHOLD_SUBSYSTEM:      INERT — max output 0.65 < floor 0.70
ENSEMBLE_MIN_THRESHOLD_FLOOR:     INERT — 0.55 < 0.70 (the T-04 tuning did nothing)
CONSENSUS_THRESHOLD (0.40):       INERT — consensus_required=0.0 is passed

HISTORICAL 0.40 REFERRED TO:      DYNAMIC ENTRY MINIMUM
                                  (DYNAMIC_THRESHOLD_MIN, dynamic_threshold.py:76)
                                  NOT the consensus threshold, which coincidentally
                                  also equals 0.40, and NOT the final entry threshold

SENSITIVITY_STUDY:                NOT RUN — Phase 13 replay incomplete
                                  (also blocked by 0 trades and 2 evidence defects)

THRESHOLD_CHANGED:                NO
```
