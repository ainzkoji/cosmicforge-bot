# Master Ensemble — Zero-Trade Forensic Audit

Read-only forensic analysis of `bot_a8117dc719fc`. **Nothing was tuned,
disabled or changed.** No threshold, weight, risk limit, capital value, regime
rule, strategy parameter or execution setting was modified.

| Item | Value |
| --- | --- |
| Bot | `bot_a8117dc719fc` — paper / demo, master_ensemble, BTCUSDT+ETHUSDT, 15m |
| Window | 2026-09-08 04:44 → 2026-09-09 04:29 UTC |
| Evidence | `trading_decisions` (canonical), `canonical_trade_decisions` (legacy, used as an independent control) |

**Headline:** the bot is behaving correctly and the no-trade result is genuine
strategy selectivity — but the canonical evidence describing *why* is partly
wrong. Two real defects were found, both in the evidence layer, neither
affecting trading behaviour.

---

## AUDIT 1 — Exact real evaluation set

Heartbeat rows excluded throughout.

```
total rows for this bot        : 4562
NO_NEW_CANDLE heartbeat rows   : 4416
genuine closed-candle evals    :  146
earliest genuine candle        : 2026-09-08 04:44 UTC
latest genuine candle          : 2026-09-09 04:29 UTC
candle slots in interval       :   96  (x2 symbols = 192 expected)
```

| Symbol | Rows | Distinct candles | Expected | Missing | Coverage |
| --- | --- | --- | --- | --- | --- |
| BTCUSDT | 73 | 73 | 96 | 23 | **76.0%** |
| ETHUSDT | 73 | 73 | 96 | 23 | **76.0%** |

**Duplicates: 0.** The 23 missing slots per symbol are the six-hour host
suspension already documented in the always-on hardening report — not an
evaluation failure.

Outcome mix of the 146 genuine evaluations:

| Reason | Count | Share |
| --- | --- | --- |
| `REGIME_LOW_VOL_CHOP` | 85 | 58.2% |
| `NO_OPPORTUNITY` | 51 | 34.9% |
| `ENTRY_CONFIDENCE_BELOW_THRESHOLD` | 10 | 6.8% |

(Counts differ slightly from the figures in the request — 146 vs 144 — because
the bot kept running between the snapshot and this audit.)

---

## AUDIT 2 — Reason / regime consistency → **DEFECT #1**

### The contradiction

78 rows carry `primary_reason = REGIME_LOW_VOL_CHOP` while `regime` reads
something else. Two distinct shapes exist under one reason:

| Field | Shape A (7 rows) | Shape B (78 rows) |
| --- | --- | --- |
| `regime` | `LOW_VOLATILITY_CHOP` | `WEAK_TREND` |
| `effective_entry_threshold` | `0.7` | `0.0` |
| `raw_confidence` | `NULL` | `0.0` |
| `quality_result` | `NULL` | `FAIL` |
| `quality_reason` | `NULL` | `NO_OPPORTUNITY` |
| `buy_score` / `sell_score` | `NULL` | `0.0` / `0.0` |

### Root cause: stale cross-evaluation evidence leakage

`MasterEnsembleStrategy` publishes its last result on two **instance**
attributes:

* `backends/bot-backend/app/strategy/master_ensemble.py:204-205` — initialised
  to `None` **once, in `__init__`**
* `:766-767` — written at Step 6/7

They are **never reset at the start of `get_signal()`**.

When the regime is `LOW_VOLATILITY_CHOP`, the ensemble takes an early return at
`master_ensemble.py:449-467` (`if not available_active:`) which fires **before**
Step 6/7. `last_opportunity` and `last_entry_quality` therefore still hold the
**previous evaluation's** values.

`backends/bot-backend/app/evidence/runner_bridge.py::apply_evidence` then reads
those attributes and stamps them onto the current decision. Worse, it applies
the stale opportunity *first*:

```python
decision.set_opportunity(opportunity)          # stale regime written here
...
decision.regime = decision.regime or meta.get("regime")   # fresh regime IGNORED
```

The fresh `regime` from `_imeta` is discarded because the field is already
populated with the stale value.

The ensemble instance is shared across symbols within one runner, so
contamination crosses **both candles and symbols**.

### Proof

* **78 / 78** contradictory rows carry regime + quality values identical to an
  earlier evaluated row. **0 do not.**
* The 7 uncontaminated Shape-A rows all occur immediately after a process
  restart (`run=5b49bc41` at 17:30, `run=6a25e9dd` at 04:00–04:30) — i.e. while
  `last_opportunity` was still `None` and there was nothing stale to leak.
* **Independent control.** The legacy `canonical_trade_decisions` table records
  regime at decision time and is not fed through `apply_evidence`:

```
matched pairs: 141   agree=65   DISAGREE=76
  trading_decisions=WEAK_TREND   legacy=LOW_VOLATILITY_CHOP   reason=REGIME_LOW_VOL_CHOP   n=76
```

**All 76 disagreements are the same shape. Zero disagreements of any other kind.**

### Verdict

**The reason is CORRECT; the `regime` field is STALE.** The market genuinely was
in low-volatility chop for those evaluations. True distribution from the
uncontaminated source:

```
LOW_VOLATILITY_CHOP : 88   (57.9%)
WEAK_TREND          : 64   (42.1%)
```

This defect is in code introduced during the canonical-evidence work
(`last_opportunity`/`last_entry_quality` publication, and `apply_evidence`'s
precedence). It is mine.

---

## AUDIT 3 — All seven experts → **EVIDENCE GAP, cannot be answered**

Per-expert output is **not persisted anywhere** for this window:

| Source | Per-expert data? |
| --- | --- |
| `trading_decisions.component_metadata_json` | populated only when a `TradingOpportunity` exists (10 rows); `NoOpportunity` has no `component_breakdown` field |
| `canonical_trade_decisions.decision_json → component_signals` | contains only the top-level `master_ensemble` entry: `[{"strategy":"master_ensemble","signal":"hold","confidence":0.0,"reason":"NO_OPPORTUNITY"}]` |
| `decision_traces.strategy_signals_json` | same top-level shape only |
| Runtime logs | `[REGIME-GATE]` and `[ENSEMBLE]` lines are emitted at INFO but the supervised launcher runs at `--log-level info` with only `[CYCLE]`/`[DYNAMIC_SHADOW_DEBUG]` surviving; **0 REGIME-GATE lines captured** |

A per-expert BUY/SELL/HOLD tally across the 146 evaluations **cannot be
reconstructed from organic evidence**. I will not fabricate one.

What *can* be established, from the 10 opportunity rows plus the activation
matrix (AUDIT 4/6):

* In the 88 `LOW_VOLATILITY_CHOP` evaluations, **all seven experts were
  deactivated before running** — they did not HOLD, they were never invoked.
* In the 10 rows that produced an opportunity, **9 had exactly one supporting
  expert and 1 had two**; opposing experts were **0 in every case**.
* The 51 `NO_OPPORTUNITY` rows show `buy_score == sell_score == 0.0`, i.e. the
  eligible experts ran and none produced a directional vote.

So the experts do overwhelmingly return HOLD in `WEAK_TREND` — but the
per-expert attribution needed to say *which* ones is missing.

---

## AUDIT 4 — Regime routing → **CORRECT BY DESIGN**

`backends/bot-backend/app/strategy/activation.py:27-51`:

```python
MarketRegime.WEAK_TREND: frozenset([
    "supertrend", "trend_pullback", "vwap_reversion", "squeeze_breakout",
]),
# Capital preservation mode — zero strategies, HOLD is mandatory
MarketRegime.LOW_VOLATILITY_CHOP: frozenset(),
```

| Regime | Eligible experts | Excluded | Evaluations |
| --- | --- | --- | --- |
| `LOW_VOLATILITY_CHOP` | **0 of 7** (by design) | all 7 | 88 |
| `WEAK_TREND` | 4 of 7 — supertrend, trend_pullback, vwap_reversion, squeeze_breakout | bollinger_reversion, donchian_breakout, sma_cross | 64 |

`LOW_VOLATILITY_CHOP → frozenset()` is explicit, commented capital-preservation
behaviour. The early return at `master_ensemble.py:449` and the resulting
`REGIME_LOW_VOL_CHOP` reason are **exactly what the configuration specifies**.

### The WEAK_TREND contradiction, explained

There is no routing contradiction. The rows are **not** WEAK_TREND rows ending
in a low-vol reason — they are genuine `LOW_VOLATILITY_CHOP` rows displaying a
stale `WEAK_TREND` regime string (AUDIT 2). Routing behaved correctly in all
146 evaluations.

---

## AUDIT 5 — Why no opportunity was emitted (136 rows)

| Classification | Count |
| --- | --- |
| Regime disabled all experts (`LOW_VOLATILITY_CHOP → frozenset()`) | **85** |
| No expert directional signal (`buy_score == sell_score == 0.0`) | **51** |
| Mixed BUY/SELL conflict | 0 |
| Insufficient weighted direction | 0 |
| Missing component data | 0 |
| HTF filtering | 0 |
| Session filtering | 0 |
| Volatility filtering | 0 |
| Ensemble-specific veto | 0 |
| Other | 0 |

HTF, session and volatility vetoes produced **zero** rows in this window — they
are not contributing to the no-trade outcome.

---

## AUDIT 6 — The ten real opportunities

All are `WEAK_TREND`. Threshold was `0.70` in every case.

| Time (UTC) | Symbol | Side | Buy | Sell | Raw conf | Threshold | **Shortfall** | Support | Oppose |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 05:00:09 | BTCUSDT | SELL | 0.000 | 0.280 | 0.280 | 0.70 | 0.420 | 1 | 0 |
| 06:00:04 | BTCUSDT | SELL | 0.000 | 0.292 | 0.292 | 0.70 | 0.408 | 1 | 0 |
| 06:15:07 | BTCUSDT | SELL | 0.000 | 0.285 | 0.285 | 0.70 | 0.415 | 1 | 0 |
| 06:30:09 | BTCUSDT | SELL | 0.000 | 0.267 | 0.267 | 0.70 | 0.433 | 1 | 0 |
| **06:45:03** | **ETHUSDT** | **SELL** | 0.000 | 0.586 | **0.586** | 0.70 | **0.114** | **2** | 0 |
| 07:00:08 | ETHUSDT | SELL | 0.000 | 0.302 | 0.302 | 0.70 | 0.398 | 1 | 0 |
| 10:15:12 | BTCUSDT | SELL | 0.000 | 0.325 | 0.325 | 0.70 | 0.375 | 1 | 0 |
| 16:21:58 | ETHUSDT | BUY | 0.300 | 0.000 | 0.300 | 0.70 | 0.400 | 1 | 0 |
| 16:30:02 | ETHUSDT | BUY | 0.302 | 0.000 | 0.302 | 0.70 | 0.398 | 1 | 0 |
| 16:45:12 | ETHUSDT | BUY | 0.302 | 0.000 | 0.302 | 0.70 | 0.398 | 1 | 0 |

**Mean shortfall: 0.376.** Nothing was lowered.

### Observation (reported, not acted on)

The pattern is structural, not marginal. Confidence is normalised by
`NOMINAL_CONSENSUS_WEIGHT = 3.0` (`master_ensemble.py`), so:

* one supporting expert → ≈ **0.27–0.33**
* two supporting experts → ≈ **0.59** (the single best case)
* clearing **0.70** requires roughly **three** experts agreeing at high confidence

In `WEAK_TREND` only **four** experts are eligible, and regime multipliers reduce
their effective weights further. Nine of ten candidates had a lone supporter;
the best case in 24 hours still missed by 0.114.

This is a calibration relationship between `NOMINAL_CONSENSUS_WEIGHT = 3.0`, the
`0.70` floor and a 4-expert eligible set. **It is a design observation, not a
defect, and no change is recommended from a 24-hour window.**

---

## AUDIT 7 — Threshold evidence → **DEFECT #2**

```
rows with effective_entry_threshold == 0 : 129
rows with effective_entry_threshold  > 0 :  17   (all exactly 0.7)
raw_confidence IS NULL : 7
raw_confidence == 0.0  : 129
```

The 129 zero-threshold rows split into two different causes:

| Reason | Count | Cause |
| --- | --- | --- |
| `REGIME_LOW_VOL_CHOP` | 78 | **Stale contamination** (AUDIT 2). True threshold was `0.7`. |
| `NO_OPPORTUNITY` | 51 | **Never evaluated.** `TradingDecisionEngine.evaluate()` returns early for a `NoOpportunity` *before* `resolve_threshold()` runs; `EntryQualityDecision.effective_entry_threshold` then keeps its dataclass default of `0.0`. |

### Answer to the question posed

**The zero is a not-evaluated default, not an actual decision threshold.**

This is what produces the misleading aggregate "median confidence = 0, median
threshold = 0, confidence ≥ threshold = 129, quality approvals = 0". The
comparison `0.0 >= 0.0` is arithmetically true but semantically meaningless: no
threshold was ever resolved for those rows, because there was no opportunity to
compare against.

The schema permits `NULL` on both columns (they are nullable `REAL`), and the 7
uncontaminated rows already demonstrate the honest representation
(`raw_confidence = NULL`). The intended contract is therefore **`NULL` =
not evaluated**, and `0.0` is being written where `NULL` is meant.

**No schema or evidence change has been made**, per instruction.

---

## AUDIT 8 — Organic opportunity funnel

| Stage | Combined | BTCUSDT | ETHUSDT |
| --- | --- | --- | --- |
| Eligible closed-candle evaluations | 146 (100%) | 73 (100%) | 73 (100%) |
| Regime accepted (experts eligible) | **61 (41.8%)** | 35 (47.9%) | 26 (35.6%) |
| ≥1 directional expert / candidate | 10 (6.8%) | 5 (6.8%) | 5 (6.8%) |
| **TradingOpportunity emitted** | **10 (6.8%)** | 5 (6.8%) | 5 (6.8%) |
| Consensus pass (not gated; required 0.0) | 10 (6.8%) | 5 | 5 |
| **Confidence pass → quality approved** | **0 (0.0%)** | 0 | 0 |
| Risk evaluated | 0 | 0 | 0 |
| Risk approved | 0 | 0 | 0 |
| Execution feasibility | 0 | 0 | 0 |
| Execution attempt | 0 | 0 | 0 |
| Fill | 0 | 0 | 0 |

Two attrition points dominate:

1. **58.2% lost at the regime gate** — capital-preservation mode, by design.
2. **Of the 41.8% that survive, 83.6% produce no directional candidate at all**
   (51 of 61). Only 10 of 146 evaluations (6.8%) reach the quality comparison.

Risk and execution were **never reached**. Nothing downstream of entry quality
has been exercised, so nothing downstream can be blamed.

---

## AUDIT 9 — Verdict

### **D. MULTIPLE** — specifically **A + C**

**A. EXPECTED_STRATEGY_SELECTIVITY** — the trading behaviour is correct.

* `LOW_VOLATILITY_CHOP → frozenset()` is explicit, commented capital-preservation
  configuration (`activation.py:50-51`). 88 evaluations correctly suspended.
* The 10 genuine candidates were each rejected by a real comparison against a
  real `0.70` threshold, with a mean shortfall of 0.376.
* No HTF, session, volatility or event veto contributed.
* Risk and execution were never reached, so neither is implicated.

**C. EVIDENCE_DEFECT** — two real defects, both in the evidence layer only.

| # | Defect | Rows affected | Source |
| --- | --- | --- | --- |
| 1 | Stale `regime` / scores / quality leaked across evaluations and symbols | 78 | `master_ensemble.py:204-205,766-767` (attributes never reset) + `runner_bridge.py::apply_evidence` (stale applied first, fresh `meta["regime"]` then discarded) |
| 2 | `effective_entry_threshold` / `raw_confidence` written as `0.0` when never evaluated | 51 | `decision_engine.py::TradingDecisionEngine.evaluate` early return + dataclass defaults |

**B. OPPORTUNITY_GENERATION_DEFECT — not supported.** Regime routing, the
activation matrix, opportunity construction and the single entry-quality
comparison all behaved as configured across all 146 evaluations.

Neither defect changed a single trading decision. Both corrupt the *explanation*
of decisions that were themselves correct — which is exactly the class of
problem that makes a future AI dataset untrustworthy.

---

## Final summary

```
GENUINE_EVALUATIONS:
146   (heartbeat rows excluded; 4416 NO_NEW_CANDLE ticks not counted)

EVALUATION_COVERAGE:
76.0%   (73 of 96 candle slots per symbol; the 23 missing = the 6h host suspension)

NO_DIRECTIONAL_OPPORTUNITY:
136 / 93.2%
  - 85 regime-suspended (LOW_VOLATILITY_CHOP -> zero eligible experts, by design)
  - 51 no expert directional vote (buy_score == sell_score == 0.0)

TRADING_OPPORTUNITIES:
10 / 6.8%

QUALITY_APPROVED:
0

RISK_REACHED:
0

EXECUTION_REACHED:
0

EXPERTS_MOSTLY_HOLD:
YES - but per-expert attribution is NOT recoverable from organic evidence.
     In 88 evaluations all 7 experts were deactivated before running. In the 51
     WEAK_TREND no-opportunity rows the eligible experts produced no directional
     vote. Of the 10 candidates, 9 had a single supporting expert and 0 opposing.

REGIME_ROUTING_CORRECT:
YES - activation.py matches the observed behaviour in all 146 evaluations.

REASON_REGIME_EVIDENCE_CORRECT:
NO - primary_reason is correct; the regime field is stale in 78 of 146 rows
     (76 confirmed against the independent legacy table, 0 counter-examples).

THRESHOLD_EVIDENCE_CORRECT:
NO - 129 rows record 0.0 where the threshold was either never evaluated (51) or
     lost to stale contamination (78). Zero is a default, not a decision
     threshold, and NULL is the honest representation the schema already allows.

PRIMARY_NO_TRADE_BOTTLENECK:
Opportunity generation, upstream of entry quality - and within it, the regime
gate. 58.2% of evaluations are suspended by LOW_VOLATILITY_CHOP before any
expert runs; of the survivors, 83.6% yield no directional vote. Only 6.8% of
evaluations reach the confidence comparison, and those fail it by a mean of
0.376. Risk and execution are not implicated - they were never reached.

BUG_FOUND:
YES - two evidence-layer defects (regime/score staleness; zero-vs-null
      threshold). Neither altered any trading decision.

STRATEGY_CHANGE_RECOMMENDED:
NOT_YET - 24 hours of data, 57.9% of it in a regime the configuration
          deliberately sits out, is too thin a basis. The evidence defects
          should be fixed first so that a longer sample can be trusted; the
          NOMINAL_CONSENSUS_WEIGHT=3.0 / 0.70-floor / 4-eligible-expert
          relationship is then worth reviewing on real data, as a separate
          approved change.
```

No strategy change has been implemented. No threshold, weight, limit, capital
value or execution setting was modified during this audit.
