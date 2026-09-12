# Adaptive Entry Threshold Engine — audit and recalibration (threshold policy 1.1.0)

Date: 2026-09-11 · Bot `bot_a8117dc719fc` (BTCUSDT, ETHUSDT, master_ensemble) · mode `live` ·
broker account `brk_c729454e6c98` (binance, demo) · code base `2884d9b7`

The engine architecture is unchanged: one authority, one policy, one band. What changed is the
calibration, and three defects that the audit found in the numbers feeding it.

---

## Phase 1 — Authority map

### How 0.70 reached the active bot

```
bot-backend/.env  THRESHOLD_BASE=0.70  THRESHOLD_MIN=0.50  THRESHOLD_MAX=0.90   (untracked file)
  -> app/core/config.py Settings (pydantic reads .env from the runtime cwd; env beats class default)
  -> app/threshold/policy.py policy_from_settings -> global_scope_from_settings -> resolve_threshold_policy
  -> app/threshold/runtime.py get_threshold_policy (cached per symbol/venue/market scope)
  -> MasterEnsembleStrategy.get_signal Step 4 (_threshold_policy)        -> AdaptiveEntryThresholdEngine.evaluate
  -> app/decision/external_signal_gate.py (same resolver, same engine)   -> AdaptiveEntryThresholdEngine.evaluate
  -> TradingDecisionEngine.evaluate: approved = raw_confidence >= final_threshold   (the one comparison)
```

Every live `threshold_decisions` row before this change carries base 0.70, band [0.50, 0.90],
engine 1.0.0 and policy hash `afd2b4636b9d89e9d99dabc46ea08338386e717e66d7b8cb226911c601f17802`.

### SOURCE → VALUE → PRECEDENCE → RUNTIME CONSUMER

| Source | Value (before) | Precedence | Runtime consumer |
|---|---|---|---|
| `bot-backend/.env` THRESHOLD_BASE/MIN/MAX | 0.70 / 0.50 / 0.90 | Highest: overrides the Settings defaults | The engine, via the chain above |
| `app/core/config.py` THRESHOLD_* defaults | base 0.70, band 0.50–0.90; bounds 0.06/0.05/0.08/0.05/0.04/0.05/0.05; steps 0.05/0.03 | Used for any key that `.env` does not set (bounds and steps: yes) | Same chain |
| `app/threshold/policy.py` DEFAULTS | band 0.50–0.90, same bounds and steps; base `None` | Seed of resolution; GLOBAL scope (settings) overrides it | Same chain |
| THRESHOLD_SCOPED_OVERRIDES | `''` | ASSET_CLASS → VENUE → SYMBOL → BOT would override GLOBAL | None configured; `bot_overrides` is never passed at runtime |
| `adaptive_threshold_state` (DB) | BTC prev 0.795 with 21 samples; ETH prev 0.767 with 15 samples; engine 1.0.0, hash afd2b… | Anchors smoothing and rate limiting; feeds the distribution term | Engine `_smooth`, `_rate_limit`, `DistributionCalibrator` |
| `threshold_decisions` (DB) | Evidence, per-row version and hash | Immutable record, never read back as policy | Diagnostics API, reports |
| `bot_runs.policy_hash` / effective bot policy | Records `threshold_policy_hash` | Evidence only | Evidence |
| Threshold diagnostics API (`/status`, `/decisions`, `/history`, `/policy`) | — | GET only; no mutation route anywhere in `app/api` | Operators |
| Legacy keys (MIN_CONFIDENCE_THRESHOLD, ENSEMBLE_MIN_THRESHOLD_FLOOR, DYNAMIC_THRESHOLD_*, CONSENSUS_*) | — | Rejected at startup | None |
| `.env.example` (untracked local template) | 0.70 / 0.50 / 0.90 | Only if copied to `.env` | None directly |
| `bot_instance_service` risk presets `additional_params.min_confidence_score` | 0.70 in all three profiles | — | **No consumer** (dead admin default) |
| `MasterEnsembleStrategy(min_confidence=0.15)`; params_schema default 0.20 | 0.15 / 0.20 | — | Only the opt-in `legacy_secondary_confidence_gate`, which no caller passes (**dormant second authority**) |
| Expert-internal `min_confidence` (supertrend 0.50, trend_pullback 0.75, donchian 0.75, sma_cross 0.55, bollinger 0.55, squeeze 0.60, vwap 0.60) | as listed | Inside each expert | Whether the expert votes at all. A strategy formula (unchanged); can only suppress a vote, never admit an entry |
| `runner._MAX_EXTERNAL_CONFIDENCE` | 0.75 | Clamps an external input before the engine | External candidates; the engine still decides |
| `PositionManager.reentry_min_confidence` (`can_open_after_reset`) | 0.80 | — | **No caller** (dormant) |
| `AddManager` min confidence | 0.85 / 0.75 | — | Scale-in adds to an existing position; not an entry threshold |
| `strategy/activity_targets.py` | "never below 0.20" | — | Neutralised (reduction is always 0) and mixed into no strategy |
| `metrics/calibration.py` fallback_min_confidence | 0.80 | — | **No importer** |
| News services `min_confidence_threshold` | 0.70 | — | News-narrative cluster confidence, not entry quality |
| `config.validate_for_production` warning "base < 0.50 is permissive" | 0.50 | Warning only | Startup report |
| Tests | 0.70 / 0.50 / 0.90 in fixtures | — | Test-only |

### The 24 audit items

1. **TradingOpportunity confidence** = `max(buy_pct, sell_pct)`, built in the ensemble; `build_opportunity` carries it unchanged.
2. **Ensemble confidence** = Σ(weight × regime multiplier × performance multiplier × expert confidence) / `NOMINAL_CONSENSUS_WEIGHT` (3.0), capped at 1.
3. **Expert weights**: supertrend 1.5, trend_pullback 1.3, vwap 1.2, squeeze 1.1, bollinger 1.0, donchian 1.0, sma 0.9. There is no mathematical defect, so they are unchanged.
4. **Eligible denominator**: regime-activated experts only. DISABLED experts are excluded; NOT_RUN experts stay in the denominator. This was already correct.
5. **Agreement score**: **defect**, see Phase 3.
6. **Engine**: reconstructed exactly on all 39 live EVALUATED rows, with 0 arithmetic failures.
7. **Policy hierarchy**: GLOBAL → ASSET_CLASS → VENUE → SYMBOL → BOT. Only GLOBAL is in use.
8. **Persistent defaults**: config.py and policy.py, table above.
9. **Runtime resolution**: `get_threshold_policy`, cached per scope, with no fallback.
10. **Smoothing**: `0.35·raw + 0.65·previous`, verified.
11. **Rate limiting**: +0.05 / −0.03 per evaluated candle, verified.
12. **Clamp**: the single band, and nothing after it.
13. **Performance calibration**: min 30 closed trades, bounded, dead band. Zero trades so far, so it is neutral.
14. **Distribution calibration**: 40 samples minimum, window 200, target P60. It would have *lowered* a 0.70 bar by 0.05 once warm, which is not enough to matter.
15. **Regime**: factor × classifier trust. WEAK_TREND is always slightly positive, which is intended.
16. **Volatility**: **defect**, see Phase 3.
17. **HTF**: sign correct; **saturation miscalibrated**, see Phase 3.
18. **Market quality**: volume percentile only (no order-book data). Sign correct.
19. **External path**: `external_signal_gate` → same engine, same policy resolver, persisted decision before any execution. Separate state partition (`external_tradingview/1`).
20. **Tests**: the fixtures pinned the old scale; updated in Phase 16.
21. **API/admin inputs**: none can change a threshold.
22. **DB state**: `adaptive_threshold_state` (2 rows of 0.70-era state), `threshold_decisions` (immutable).
23. **Env/config defaults**: `.env`, `.env.example`, config.py, policy.py.
24. **Legacy 0.70 / 0.50 / 0.90**: all instances are listed in the table above. None survives in the production path after this change, except as historical text in `docs/` and in `migration.py`'s legacy record.

---

## Phase 2 — The confidence scale the ensemble actually produces

**Clean live evidence** runs from session `rts_419a…` (after the sma_cross fix) through `rts_2f06…`, the running session. It excludes the broken-sma period, the false SESSION_BLOCKED window, test and replay rows, and dirty sessions.

- 30 evaluated opportunities, all WEAK_TREND.
- Confidence: min 0.195, median 0.325, mean 0.337, max 0.624.
- Final threshold: minimum 0.728.
- **0 passes.**

Thirty rows cannot bound a distribution, so a **production-parity capture** supplies the statistics:

- **Code path.** The real `MasterEnsembleStrategy` runs every closed 15m candle through a `MarketSnapshot`, exactly as the runner does. That is 250 × 15m bars plus 250 × 4h HTF bars, the 4h bars aggregated from stored 1h bars.
- **Window.** 2026-01-16 → 2026-06-10: 145 days, 13,921 candles per symbol, zero gaps.
- **Isolation.** In memory, with the canonical DB read-only.
- **Regime gating as live.**
  - STRONG_TREND is blocked before the engine, as the live broker-mode guard blocks it.
  - LOW_VOLATILITY_CHOP is hard-blocked.
  - RANGE candles (707) fail closed: `vwap_reversion` needs 5m candles and none are stored, so they contribute no confidence.
  - HIGH_VOLATILITY was never classified.

Live and replay agree: median 0.325 in both, and the same discrete clusters.

| Set | n | min | P10 | P20 | P25 | P30 | P40 | P50 | P60 | P70 | P75 | P80 | P90 | P95 | max | mean | sd |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Combined | 4644 | 0.1950 | 0.2824 | 0.3052 | 0.3090 | 0.3134 | 0.3237 | 0.3250 | 0.3303 | 0.3467 | 0.3482 | 0.3612 | 0.4000 | 0.4838 | 0.8965 | 0.3352 | 0.0777 |
| BTCUSDT | 2111 | 0.1950 | 0.1950 | 0.3031 | 0.3066 | 0.3102 | 0.3191 | 0.3250 | 0.3250 | 0.3383 | 0.3467 | 0.3493 | 0.4000 | 0.4999 | 0.8965 | 0.3304 | 0.0799 |
| ETHUSDT | 2533 | 0.1950 | 0.3002 | 0.3077 | 0.3118 | 0.3167 | 0.3250 | 0.3251 | 0.3366 | 0.3467 | 0.3577 | 0.3699 | 0.4000 | 0.4593 | 0.8339 | 0.3393 | 0.0755 |
| WEAK_TREND | 4644 | as combined | | | | | | | | | | | | | | | |
| STRONG_TREND ¹ | 3687 | 0.2535 | 0.3986 | 0.4104 | 0.4175 | 0.4225 | 0.4329 | 0.4507 | 0.4719 | 0.5047 | 0.5200 | 0.5200 | 0.5200 | 0.8355 | 1.0000 | 0.4871 | 0.1239 |
| RANGE | 0 | fails closed without 5m data | | | | | | | | | | | | | | | |
| HIGH_VOLATILITY | 0 | never classified in the window | | | | | | | | | | | | | | | |
| LOW_VOLATILITY_CHOP | 0 | hard-blocked before any expert runs | | | | | | | | | | | | | | | |

Candles classified over the window: WEAK_TREND 15,179 · LOW_VOLATILITY_CHOP 6,145 · STRONG_TREND 5,802 · RANGE 716.

### Phase 8 — What the numbers mean

The score is not a probability. It is a weighted vote count over a nominal weight of 3.0, so it comes in clusters:

| Confidence | Means | Share |
|---|---|---|
| 0.195 | sma_cross alone (0.9 × 0.65 / 3) | 9.2% |
| 0.25–0.35 | one primary expert alone (supertrend 0.50–0.70, trend_pullback 0.75, donchian) | ~74% |
| 0.35–0.45 | one expert at high conviction (supertrend 0.80 → 0.40) | ~12% |
| 0.45–0.90 | two or three experts agreeing (median 0.593) | 4.8% |

95.2% of opportunities are a single expert voting alone. On this scale:
- 0.30 means "a primary expert has a setup";
- 0.40 means "one expert, convinced";
- 0.50–0.60 means "two experts agree";
- 0.60 and above means "two strong or three".

A 0.70 base demanded near-unanimity. P90 is 0.40; P95 is 0.48.

---

## Phase 3 — Formula audit

**Arithmetic.** All 39 live EVALUATED rows reconcile. For 30 clean rows plus 9 earlier ones:
- components sum to raw;
- smoothing is 0.35/0.65;
- the rate limit is +0.05/−0.03;
- the clamp holds;
- `passed == (confidence >= final)`.

There were 0 failures. The engine computed exactly what it was configured to compute; the configuration was the problem.

**Signs and monotonicity** (clean live rows; replay agrees):

| Term | Intended | Found | Verdict |
|---|---|---|---|
| Regime | poor regime raises, RANGE lowers | WEAK_TREND +0.016 on 30/30 | correct (by design) |
| Volatility | both tails raise, middle neutral | +0.031 mean, positive 24/30 | **input defect** (below) |
| Agreement | better agreement must not raise | positive on 30/30 live and 4,643/4,644 replayed | **defect: double counting, unreachable neutral point** |
| HTF | aligned lowers, opposed raises | aligned −, opposed + | sign correct; **saturated** on 60% of candles |
| Market quality | better quality lowers | volume high → negative | correct |
| Performance | neutral below 30 trades | 0.0 | correct |
| Distribution | toward P60 of recent confidence | 0.0 (fewer than 40 samples) | correct; see Phase 9 |

### Defects

1. **Agreement double-counted confidence.**
   - *Mechanism.* The score was (Σ weight × confidence for the side − opposition) / eligible weight. The opportunity's confidence is that same weighted sum over 3.0. A lukewarm opportunity therefore paid twice: once in its confidence and again in a higher bar. The neutral point needs half the eligible weight times full confidence, which a lone expert can never supply.
   - *Effect.* The term raised the bar on 4,643 of 4,644 replayed opportunities (mean +0.047).
   - *Secondary defect.* The dispersion term also included opposing experts, contradicting its own comment ("dispersion among the agreeing experts") and counting opposition a second time.
2. **The volatility percentile compared unlike measures.**
   - *Mechanism.* `_volatility_context` ranked the 14-candle ATR% against single-candle high-low ranges. An average of true ranges (gaps included) sits above most individual ranges, and the docstring itself says "the same measure".
   - *Effect.* The recorded percentile had mean 0.64, with 36% of values ≥ 0.75 and 8% ≤ 0.35. An unbiased rank would be centred on 0.5, with 25% and 35%. Calm markets were read as volatile.
3. **HTF strength saturated at 5% distance from the 4h EMA200.** The observed median distance is 6.1%, and 58% of opportunities are beyond 5%, so the term was a binary ±bound.
4. **0.70-era state would anchor the new policy.**
   - *Mechanism.* Stored `previous_threshold` values are 0.795 (BTC) and 0.767 (ETH). With smoothing (0.65 weight on the previous value) and a −0.03 step per *evaluated* candle, of which there are about 14 per symbol per day, a 0.30 policy would have stayed above 0.70 for about a day.
   - *Contamination.* The stored distribution samples include pre-fix evidence.

### What the evidence does not support

A close-to-close forward-move proxy over the replay window (no costs, no stops) is not a P&L, but it can falsify a sign. In this window no context term had measurable support for its direction:
- HTF-opposed opportunities moved +12.6 bps at 16 candles, against +0.9 for aligned ones.
- Low-volume opportunities moved +15.2 bps, against +3.3 for high volume.
- The calm tail of volatility did best.
- Confidence itself is not monotone in forward move: the hit rate is 0.46–0.51 at every level.

No sign was flipped on in-sample evidence, because that would be curve-fitting. The magnitudes were kept proportional and small, and the performance term, fed by realised R, remains the data-driven correction path.

---

## Phases 4–12 — What changed

| Parameter | 1.0.0 | 1.1.0 | Why |
|---|---|---|---|
| Base | 0.70 | **0.30** | Production decision; ≈ P20 of the scale, "a primary expert has a setup" |
| Min | 0.50 | **0.25** | Top of the empty interval between the lone-sma cluster (0.195, 9.2%) and every other opportunity (≥ 0.25; there is zero mass in [0.20, 0.25)). The most permissive state still rejects sma_cross alone |
| Max | 0.90 | **0.60** | Median of genuine multi-expert consensus (P50 0.593, P60 0.602); above P95 (0.484), below P99 (0.643). Attainable: a typical two-expert WEAK_TREND consensus (supertrend 0.80 + trend_pullback 0.75 → 0.725) clears it |
| Regime / volatility / agreement / HTF / market quality | 0.06 / 0.05 / 0.08 / 0.05 / 0.04 | **0.021 / 0.018 / 0.029 / 0.018 / 0.014** | All scaled by k = (P90 − base)/0.28 = 0.10/0.28. Every ordinary market penalty at once lifts the bar to P90 (0.40) and no further |
| Performance / distribution | 0.05 / 0.05 | **0.018 / 0.018** | Same k. With every term at maximum the bar is 0.436, below P95 |
| Step up / down | 0.05 / 0.03 | **0.018 / 0.011** | Same k: the same fraction of the adjustment range per evaluated candle |
| Smoothing α | 0.35 | 0.35 | Dimensionless; unchanged |
| Agreement measure | weight × confidence, all directional dispersion | **breadth of eligible weight**, dispersion among agreeing experts only | Defect 1. Neutral = simple majority of eligible weight; unanimity = −bound; lone expert = modest +; opposition raises |
| Volatility input | ATR% vs single-candle ranges | **ATR% vs rolling ATR%** (`_rolling_atr_percent`, bit-identical to `regime.calculate_atr_percent`) | Defect 2 |
| HTF saturation | 5% | **13%** (upper quartile of observed distance) | Defect 3; graded instead of binary |
| State across a policy change | carried | **new calibration epoch**: state from another engine version or policy hash is discarded, and the first decision records `state_reset_reason` | Defect 4 |
| Engine / policy version | 1.0.0 / 1.0.0 | **1.1.0 / 1.1.0** | New hash `7f6b14ca8d09ed09ab78caf8c78781b1ac1ffa628ff40802de6dc291ec79d590` |

Phase 6 requirements, checked by the tests:
- No single term exceeds 10% of the base. The largest, agreement, is 0.029.
- All ordinary penalties together reach P90, not P95.
- Good conditions can lower the bar to the floor of 0.25.
- Poor conditions still raise it.
- Hard conditions stay hard gates: regime blocks, stale data, expert ERROR, event veto, risk, capital, feasibility.

**Phase 9, distribution calibration.**
- *Cold start.* Neutral: fewer than 40 samples gives 0.0.
- *Warm.* It moves the bar toward P60 of the opportunity confidences by at most ±0.018. On this scale P60 is about 0.33, so a warm engine adds about +0.018.
- *Ceiling.* It cannot rebuild a 0.70 bar: base plus every bound is 0.436.
- *Epoch.* The 0.70-era samples are discarded with the epoch reset.

**Phase 10, performance calibration.** Unchanged in design. It stays neutral until 30 closed trades with complete R, then moves by at most ±0.018 behind a dead band. No performance data is manufactured.

**Phase 11, smoothing and rate limiting.** On the first evaluated candle after deployment the old state is discarded: no anchor, no rate limit, and the bar is the new policy's own number from that candle on.

**Phase 12, versioning.** Old `threshold_decisions` rows keep engine 1.0.0 and hash `afd2b…`. Nothing rewrites them.

---

## Phase 13 — One authority

- `TradingDecisionEngine.evaluate` performs the only comparison; `AdaptiveEntryThresholdEngine` produces the only threshold.
- The external TradingView path goes through the same engine and resolver, and a decision must be persisted before execution.
- The ensemble's dormant `legacy_secondary_confidence_gate` has been **deleted**, along with its `min_confidence` parameter and schema entry.
- The dead `min_confidence_score: 0.70` admin preset has been removed.
- The ensemble emits BUY/SELL only inside `if entry_quality.approved:`. This is enforced by an AST test.
- No second threshold-engine class exists anywhere in `app/`. This is enforced by a test.

`ACTIVE_FINAL_THRESHOLD_AUTHORITY_COUNT = 1`.

---

## Phases 14–15 — Replay

### Threshold level (same 13,921 candles × 2 symbols, cold start, OLD inputs for OLD and NEW inputs for NEW)

The two captures agree on side and confidence for 27,842 of 27,842 records; only the volatility and HTF inputs differ.

| Run | Evaluated | Passed | Pass rate | Passes/day/symbol | Threshold min / P25 / median / P75 / P90 / max | Mean |
|---|---|---|---|---|---|---|
| OLD 1.0.0 (0.70, 0.50–0.90) | 4644 | 24 | 0.52% | 0.08 | 0.614 / 0.690 / 0.719 / 0.750 / 0.769 / 0.821 | 0.7187 |
| **NEW 1.1.0 (0.30, 0.25–0.60)** | 4644 | 1628 | **35.1%** | 5.61 | 0.298 / 0.332 / 0.340 / 0.347 / 0.354 / 0.378 | **0.3392** |
| base 0.25 | 4644 | 4063 | 87.5% | 14.01 | 0.250 / 0.282 / 0.290 / 0.297 / 0.304 / 0.328 | 0.2892 |
| base 0.35 | 4644 | 993 | 21.4% | 3.42 | 0.320 / 0.350 / 0.358 / 0.367 / 0.378 / 0.428 | 0.3598 |
| base 0.40 | 4644 | 451 | 9.7% | 1.56 | 0.370 / 0.396 / 0.404 / 0.412 / 0.419 / 0.442 | 0.4039 |

Pass rate by confidence bucket:

| Bucket | OLD | NEW (0.30) | 0.25 | 0.35 | 0.40 |
|---|---|---|---|---|---|
| 0.20–0.25 | – (0) | – (0) | – | – | – |
| 0.25–0.30 | 0/139 | 0/139 | 56/139 | 0/139 | 0/139 |
| 0.30–0.35 | 0/2955 | **526/2955 (17.8%)** | 97.5% | 2.7% | 0% |
| 0.35–0.40 | 0/528 | **505/528 (95.6%)** | 100% | 61.9% | 4.2% |
| 0.40–0.50 | 0/379 | 379/379 | 100% | 97.4% | 55.7% |
| 0.50–0.60 | 0/126 | 126/126 | 100% | 100% | 100% |
| 0.60+ | 24/92 | 92/92 | 100% | 100% | 100% |

The NEW engine is selective without being locked:
- a lone primary expert (0.30–0.35) passes in about 18% of contexts;
- a convinced one (0.35–0.40) passes in 96%;
- every multi-expert opportunity passes;
- nothing below 0.30 passes.

Component means under NEW:

| Term | Mean | Share positive |
|---|---|---|
| regime | +0.006 | |
| volatility | +0.007 | 60% |
| agreement | +0.012 | 98%, because 95% of opportunities are lone experts; it is negative for majorities |
| HTF | ±0.018 | balanced |
| market quality | −0.003 | |
| distribution (warm) | +0.018 | |

Distribution calibration reaches its target within the first ~80 opportunities per symbol. The rate limit bound on 110 of 4,644 evaluations, the clamp on 0.

The input fixes alone: the same 1.1.0 policy on the old volatility and HTF inputs admits 1,793 rather than 1,628 opportunities.

Forward-move proxy of admitted opportunities (close-to-close, no costs):

| Run | +16 candles |
|---|---|
| OLD | n = 24, −10.4 bps, hit 0.42 |
| NEW | n = 1,625, +4.1 bps, hit 0.46 |
| 0.25 | +7.3 bps, hit 0.50 |
| 0.35 | +2.8 bps, hit 0.46 |
| 0.40 | −2.5 bps, hit 0.45 |

None is distinguishable from a coin flip.

### Full-trade production-parity replay

The real `PaperRunner` was driven over the same 13,921 candles per symbol by `app/replay/engine.py`:
- **Code under test.** OLD ran an archive of `2884d9b7` (engine 1.0.0, hash `afd2b…`, which reproduces the live policy exactly). NEW and the sensitivity runs used the working tree, now commit `51f9f901`.
- **Isolation.** A fresh migrated database per run.
- **Configuration.** The production `.env` minus secrets. STRONG_TREND is blocked as in live broker mode.
- **Sizing.** Capital 120 USDT.
- **Costs.** Taker 4 bps, maker 2 bps, spread 1 bp, slippage 2 bps, funding 1 bp per 8 h.
- **Fills.** Next-bar open, stop-first intrabar.

All ten runs completed with 0 cycle errors, 0 broker calls (all blocked by design) and an accounting residual of 0.

**The full-runner pass counts equal the engine-level ones exactly:**

| Policy | Full runner | Engine level |
|---|---|---|
| OLD | 10 + 14 = 24 | 24 |
| NEW | 639 + 989 = 1,628 | 1,628 |
| 0.25 | 4,063 | 4,063 |
| 0.35 | 993 | 993 |
| 0.40 | 451 | 451 |

The threshold is therefore the one the production path uses.

| Run | Symbol | Regime-blocked | No opportunity | Expert ERROR | Evaluated | Threshold passed | Entries | Closed | Result of the one trade |
|---|---|---|---|---|---|---|---|---|---|
| OLD | BTC | 6,075 | 5,517 | 217 | 2,111 | 10 | 1 | 1 | −23.04 USDT (−1.07 R) |
| OLD | ETH | 6,101 | 4,796 | 490 | 2,533 | 14 | 1 | 1 | −50.06 USDT (−1.95 R) |
| **NEW** | BTC | 6,075 | 5,517 | 217 | 2,111 | **639** | 1 | 1 | −3.31 USDT (−1.45 R) |
| **NEW** | ETH | 6,101 | 4,796 | 490 | 2,533 | **989** | 1 | 1 | −11.40 USDT (−1.08 R) |
| 0.25 | BTC / ETH | | | | 2,111 / 2,533 | 1,802 / 2,261 | 1 / 1 | 1 / 1 | −3.31 / −12.08 USDT |
| 0.35 | BTC / ETH | | | | 2,111 / 2,533 | 412 / 581 | 1 / 1 | 1 / 1 | −29.74 / −4.99 USDT |
| 0.40 | BTC / ETH | | | | 2,111 / 2,533 | 191 / 260 | 1 / 1 | 1 / 1 | −29.74 / −4.99 USDT |

Final-threshold statistics in the full runner (min / P25 / median / P75 / P90 / max):

| Run | BTCUSDT | ETHUSDT |
|---|---|---|
| OLD | 0.614 / 0.690 / 0.717 / 0.746 / 0.767 / 0.821 | 0.620 / 0.690 / 0.722 / 0.753 / 0.770 / 0.818 |
| NEW | 0.306 / 0.332 / 0.339 / 0.346 / 0.353 / 0.370 | 0.298 / 0.332 / 0.340 / 0.348 / 0.355 / 0.378 |

Opportunity confidence (min / P25 / median / P75 / P90 / max):

| Symbol | Values |
|---|---|
| BTCUSDT | 0.195 / 0.307 / 0.325 / 0.347 / 0.400 / 0.897 |
| ETHUSDT | 0.195 / 0.312 / 0.325 / 0.358 / 0.400 / 0.834 |

**Trade metrics are not computable from this replay.** Win rate, loss rate, profit factor, expectancy, max drawdown, and average and median R are all measured on n=1.

Every run, whatever its policy, opened exactly one position in 145 days, because of a pre-existing defect in the replay/paper path, not the threshold:
- **Stuck symbol state.** After the first position closed on its stop (`positions.status='CLOSED'`, `close_reason='SL'`), `bot_symbol_state` still held `position='SHORT'` with the closed `position_id` and `lifecycle_phase='SEEKING_TP1'`. Every later approved decision stopped at the orchestrator's protective-order layer (`primary_reason='Protective orders validated'`) with no execution attempt. That is 637 of NEW BTC's 639 passes.
- **Wall-clock daily state.** `bot_daily_state`, `daily_trade_counts` and `daily_activity_tracking` are keyed by the wall-clock date. One replayed loss therefore keeps the daily loss limit engaged for the rest of the replay: `Daily loss limit reached: -10.92 <= -6.00`, on 382 NEW ETH decisions.

Its fix touches stop-loss handling and daily risk state, both outside this change, so it is filed as a separate task. Until it is fixed the replay cannot give PF, expectancy or drawdown for any policy, OLD included.

The only outcome evidence that exists is the forward-move proxy above, and it shows no edge at any base.

Sensitivity summary (both symbols, 145 days):

| Base | Opportunities | Passes | Entries (replay-limited) | PF / expectancy / drawdown | Avg threshold | Median threshold |
|---|---|---|---|---|---|---|
| 0.25 | 4,644 | 4,063 (87.5%) | 2 | not computable (n=1 per symbol) | 0.289 | 0.290 |
| **0.30** | 4,644 | **1,628 (35.1%)** | 2 | not computable | **0.339** | **0.340** |
| 0.35 | 4,644 | 993 (21.4%) | 2 | not computable | 0.360 | 0.358 |
| 0.40 | 4,644 | 451 (9.7%) | 2 | not computable | 0.404 | 0.404 |

What would be unsafe about 0.30 is frequency without edge, not a mechanical failure:
- it admits about 5.6 opportunities per symbol per day, where 1.0.0 admitted 0.08;
- the forward proxy of those admissions is flat.

The existing hard limits (daily loss, one position per symbol, the capital budget, duplicate-entry protection) are what bound the consequences, and they are unchanged. The requested base was not altered.

---

## Phase 16 — Tests

`tests/test_threshold_policy_1_1_recalibration.py` covers the 22 required cases, numbered in the file:
1. base 0.30;
2. no 0.70 fallback in config, policy, threshold code or admin presets;
3. min < 0.30 and above the lone-sma cluster;
4. max attainable on the observed distribution;
5–6. agreement in both directions, plus the no-double-count, eligible-denominator, regime-fairness and lock-removed cases;
7–8. HTF in both directions, plus graded strength;
9–10. market quality in both directions;
11–12. cold starts;
13. a 0.70-era state cannot drag the new policy;
14. the epoch reset is persisted, and a same-epoch state is kept;
15. the rate limit holds both ways;
16–18. hard gates, expert ERROR, LOW_VOLATILITY_CHOP;
19. the external path uses the same engine and policy;
20. one threshold-engine class and no second ensemble gate;
21. entries only on the quality verdict;
22. persisted rows reproduce the threshold.

It also covers the like-for-like volatility percentile (with the old bias as a regression case) and a single source of defaults between config and policy.

Updated fixtures:
- `test_adaptive_entry_threshold_engine.py`: smoothing seeds a same-epoch state; static, hash and precedence tests moved to the new scale.
- `test_expert_error_contract.py`: base 0.30.
- `test_external_signal_threshold_path.py`: production defaults, and a "weak" candidate is 0.20 on the new scale.

Results:

- **Focused** (the ten threshold, ensemble, authority, expert-error, external-path, decision-engine, session-gate and risk-responsibility suites): **356 passed** in 23.15 s.
- **Full suite** on the isolated test database (`TEST_DATABASE_ROLE=test`, a per-session temp file): **2656 passed, 1 skipped, 27 warnings, 4 subtests passed in 2150.48 s**.
- **Canonical database** over the whole run, including the pytest run, every research capture and every replay: 0 foreign rows. Every new row is attributable to the live bot `bot_a8117dc719fc` / session `rts_2f06fb80765546468ac7` / run `5a2ba9da…`.

---

## Phase 17 — Not changed

Strategy formulas, expert weights, regime classifier thresholds, risk per trade, the 120 USDT capital budget, leverage, stop-loss, take-profit, the capital ledger, duplicate-entry protection, the broker connection, bot mode, `broker_account_id`, KYC/readiness and AI/ML authority are all untouched.

The expert-internal `min_confidence` gates are strategy formulas and are unchanged.

---

## Phases 18–19 — Commit, deploy, forward acceptance

### Phase 18 — Commit and deploy

- **Commits.** `51f9f901` holds the code and tests. `5f297f22` holds this report. This follow-up adds the acceptance record.
- **Configuration.** `.env` and the local `.env.example` now read THRESHOLD_BASE / MIN / MAX = 0.30 / 0.25 / 0.60, and the stale "migrated from 0.70" comment was rewritten. The backup is `.env.backup_before_threshold_policy_1_1`.
  - `.env` was edited only once the committed code was 1.1.0. Before that, any restart would have refused to start, because 0.30 lies outside the 1.0.0 band.
- **Incident: a restart onto uncommitted work.**
  - At 21:38:45Z the runtime was restarted by hand from the VS Code terminal while the 1.1.0 changes were still uncommitted. Session `rts_f2ba53f66c19488c8425` ran `2884d9b7` plus the dirty working tree, with the old `.env` values 0.70 / 0.50 / 0.90.
  - The result was a policy nobody studied: engine 1.1.0, hash `dfd80f56...`, thresholds around 0.73.
  - It only blocked. It evaluated 11 opportunities and passed none, so no money was at risk.
  - Its evidence comes from a dirty tree and is excluded.
  - It did confirm the Task 4 fix live: KYC `NOT_REQUIRED`, readiness `NOT_REQUIRED_FOR_DEMO_EXECUTION`.
- **Graceful restart.** At 2026-09-12T01:26:44Z, via `scripts/trading_runtime.ps1 restart`.
  - The pre-restart gate passed first: 0 positions, 0 non-flat symbol state, 0 in-flight attempts, 0 pending entries.
  - The lease was released, port 9000 was freed, and the runtime started supervised.

### Phase 19 — Clean forward acceptance

| Field | Value |
|---|---|
| runtime_session_id | `rts_32fdec27c9344e419abd` (pid 37064), started 2026-09-12T01:26:55Z |
| run_id | `b148edd736024ff79222175508b53f1a` (broker / demo / TESTNET) |
| commit | `5f297f22`, working tree clean |
| engine / policy | 1.1.0 / 1.1.0, hash `7f6b14ca8d09ed09ab78caf8c78781b1ac1ffa628ff40802de6dc291ec79d590`. Resolved from the live `.env` and identical to the replayed policy |
| bot | `bot_a8117dc719fc`, mode `live`, broker `brk_c729454e6c98`, environment demo |
| execution safety | account_environment demo, real_capital False, KYC NOT_REQUIRED, readiness NOT_REQUIRED_FOR_DEMO_EXECUTION |
| runtime log | 0 tracebacks |

**First organic evaluated opportunity** (2026-09-12T01:30:05Z, BTCUSDT, WEAK_TREND):

| Term | Value |
|---|---|
| opportunity_confidence | 0.3250 |
| base | 0.30 |
| regime | +0.0063 |
| volatility | +0.0117 |
| agreement | +0.0130 (one eligible expert of four) |
| HTF | +0.0070 (opposed, graded) |
| market quality | +0.0114 (low volume) |
| performance | 0.0000 (no closed trades) |
| distribution | 0.0000 (cold start, new epoch) |
| raw / smoothed / rate-limited / final | 0.3494 / 0.3494 / 0.3494 / **0.3494** |
| previous_threshold | none. `CALIBRATION_EPOCH_RESET` policy `dfd80f56 -> 7f6b14ca`, discarded previous_threshold 0.735 |
| passed | no (gap 0.024) |
| reconciles | yes |

**What this shows.**
- The final threshold now sits inside the confidence scale. The same kind of candidate faced 0.735 on the previous candle's policy.
- A lone supertrend vote, against the 4h trend and on low volume, was held just below the bar. That is the adaptive terms deciding, not an anchor.
- A 0.50-0.60 candidate would pass. The epoch reset stopped the stale 0.735 anchor from dragging the bar.
- The ETHUSDT state still holds the hybrid epoch and resets the same way on its first evaluated candle.

**Canonical database.** Across the whole task (the full pytest run, every capture and all ten replays) there were 0 foreign rows. The two new legacy `runs` rows are the runtime starts at 21:38:58Z and 01:26:55Z.

First organic threshold pass and first broker order: not yet observed when this was recorded.

---

## Findings outside this change

- **A runtime module is gitignored.** `app/adaptive/audit_log.py` matches `.gitignore:72 audit_*.py`. The live runtime imports it, but a clean checkout cannot import `app.adaptive`. The OLD-code replay needed it copied in.
- **Replay cannot evaluate RANGE.** Its `vwap_reversion` expert needs 5m candles, which are not stored. RANGE candles fail closed in replay; live fetches 5m.
- **External confidence is capped at 0.75, above the new maximum of 0.60.** An external candidate therefore clears the band in almost every context. It still goes through the one engine, but the cap no longer means "must still pass the bar". This is a product decision.
- **Confidence has no monotone edge.** In the forward proxy, raising the bar does not select better entries. The recalibration makes the engine functional; it does not create an edge.

## Final verdict

```
CURRENT_OLD_BASE:                     0.70
NEW_BASE:                             0.30
OLD_MIN:                              0.50
NEW_MIN:                              0.25  (top of the empty gap above the lone-sma_cross cluster at 0.195 (9.2%); zero mass in [0.20, 0.25))
OLD_MAX:                              0.90
NEW_MAX:                              0.60  (median of multi-expert consensus P50 0.593 / P60 0.602; between P95 0.484 and P99 0.643; attainable)
EMPIRICAL_CONFIDENCE_COUNT:           4644 (production-parity capture; 30 clean live rows consistent)
CONFIDENCE_P25:                       0.3090
CONFIDENCE_P50:                       0.3250
CONFIDENCE_P75:                       0.3482
CONFIDENCE_P90:                       0.4000
CONFIDENCE_MAX:                       0.8965
OLD_AVG_THRESHOLD:                    0.7187 (replay; clean live 0.769)
NEW_REPLAY_AVG_THRESHOLD:             0.3392
OLD_THRESHOLD_PASS_RATE:              0.52% (24 / 4644)
NEW_REPLAY_THRESHOLD_PASS_RATE:       35.06% (1628 / 4644)
ADJUSTMENT_MAGNITUDES_RECALIBRATED:   YES
AGREEMENT_DENOMINATOR_CORRECT:        YES (eligible experts only; the confidence double count is removed)
DISTRIBUTION_CALIBRATION:             PASS
PERFORMANCE_CALIBRATION:              PASS
COLD_START_BASE:                      0.30
OLD_ADAPTIVE_STATE_RESET:             PASS (observed live, BTCUSDT 01:30:05Z; ETHUSDT on its first evaluated candle)
NEW_THRESHOLD_ENGINE_VERSION:         1.1.0
NEW_POLICY_HASH:                      7f6b14ca8d09ed09ab78caf8c78781b1ac1ffa628ff40802de6dc291ec79d590
ACTIVE_FINAL_THRESHOLD_AUTHORITY_COUNT: 1
OLD_THRESHOLD_AUTHORITY_FOUND:        dormant ensemble legacy_secondary_confidence_gate (deleted); dead 0.70 min_confidence_score preset (removed); none active
FOCUSED_TESTS:                        356 passed
FULL_SUITE:                           2656 passed, 1 skipped, 27 warnings, 4 subtests passed in 2150.48 s
TEST_DB_ISOLATION:                    PASS
REPLAY_TRADES:                        2 (one per symbol; the replay stuck-state defect caps every run at one trade)
REPLAY_PROFIT_FACTOR:                 NOT_COMPUTABLE (n=1 per symbol)
REPLAY_EXPECTANCY:                    NOT_COMPUTABLE (n=1: -3.31 / -11.40 USDT)
REPLAY_MAX_DRAWDOWN:                  NOT_COMPUTABLE (n=1: 3.31 / 11.40 USDT)
WORKING_TREE_CLEAN:                   YES
FINAL_COMMIT:                         this commit (code 51f9f901, report 5f297f22)
LIVE_RUNTIME_RESTARTED:               YES (rts_32fdec27c9344e419abd)
LIVE_BOT_MODE:                        live
LIVE_BROKER_ACCOUNT:                  brk_c729454e6c98
LIVE_POLICY_HASH:                     7f6b14ca8d09ed09ab78caf8c78781b1ac1ffa628ff40802de6dc291ec79d590
FIRST_ORGANIC_THRESHOLD_PASS:         NOT_YET_OBSERVED
FIRST_ORGANIC_BROKER_ORDER:           NOT_YET_OBSERVED
AI_DECISION_AUTHORITY:                DISABLED (ML_ENABLED=False)
```
