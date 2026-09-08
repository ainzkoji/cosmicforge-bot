# CosmicForge — Phase 15: Master Ensemble baseline

Measured, not tuned. Every number here comes from canonical `trading_decisions`
written by the live paper runtime; nothing was replayed, simulated or adjusted
to produce it.

    python scripts/build_master_ensemble_baseline.py

| Item | Value |
| --- | --- |
| Bot | `bot_a8117dc719fc` |
| Provenance | `PAPER_FORWARD` (single provenance, no mixing) |
| Window | 2026-09-08 04:32:36Z → 22:15:12Z (~17.7 h) |
| Symbols | BTCUSDT 48, ETHUSDT 48 |
| Decision rows | 4,512 |
| Heartbeats | 4,416 |
| **Real evaluations** | **96** |

**The sample is small.** 96 evaluations over one day is enough to identify a
dominant bottleneck and nowhere near enough to retire a strategy component.
§15.8 says so explicitly, and the generator flags the window
`TOO_SMALL_FOR_COMPONENT_DECISIONS` rather than letting a reader forget.

---

## §15.1 Decision funnel

| reason | count |
| --- | ---: |
| `NO_NEW_CANDLE` | 4,416 |
| `NO_OPPORTUNITY` | 44 |
| `REGIME_LOW_VOL_CHOP` | 42 |
| `ENTRY_CONFIDENCE_BELOW_THRESHOLD` | 10 |
| `APPROVED_FOR_EXECUTION` | **0** |

The heartbeats are cycle evidence, not evaluations. Including them would make
every ratio below meaningless — they are 98% of the rows — so the funnel
ratios are computed over the 96 real evaluations only.

## §15.2 Funnel ratios

| stage | count | of evaluations |
| --- | ---: | ---: |
| closed candles evaluated | 96 | 100% |
| produced an opportunity | 10 | 10.4% |
| quality approved | 0 | 0% |
| reached the risk layer | 0 | 0% |
| execution feasible | 0 | 0% |
| execution attempts | 0 | 0% |
| fills | 0 | 0% |

The funnel collapses at its very first stage.

## §15.3 Confidence

| metric | n | min | p50 | p90 | max | mean |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| raw confidence | 95 | 0.0000 | **0.0000** | 0.2672 | 0.5859 | 0.0341 |
| effective threshold | 96 | 0.0000 | 0.0000 | 0.7000 | 0.7000 | 0.0802 |
| confidence − threshold | 95 | −0.4328 | 0.0000 | 0.0000 | 0.0000 | −0.0396 |

The threshold reads 0.0000 at the median only because on most candles no
opportunity exists, so no threshold is resolved. Where one *is* resolved it is
0.70 on every occasion — the dynamic calculator sat at its configured bound for
the whole window.

## §15.4 Consensus

| metric | n | p50 | p90 | max | mean |
| --- | ---: | ---: | ---: | ---: | ---: |
| buy score | 96 | 0.0000 | 0.0000 | 0.3020 | 0.0094 |
| sell score | 96 | 0.0000 | 0.0000 | 0.5859 | 0.0244 |
| consensus observed | 95 | 0.0000 | 0.2672 | 0.5859 | 0.0341 |

## §15.5 Regimes

`WEAK_TREND` 95, `LOW_VOLATILITY_CHOP` 1. Effectively a single-regime window,
which is another reason not to generalise from it.

## §15.8 Component contribution

| component | active | supported a direction |
| --- | ---: | ---: |
| `donchian_breakout` | 95 | 5 |
| `supertrend` | 95 | 5 |
| `trend_pullback` | 95 | 1 |
| `sma_cross` | 95 | **0** |
| `vwap_reversion` | 0 | — |
| `squeeze_breakout` | 0 | — |
| `bollinger_reversion` | 0 | — |

Three of the seven components were **never active**: the regime gate disables
them in `WEAK_TREND`. Across 95 evaluations the four active components cast
**eleven** supporting votes between them, and no opposing votes at all.

`sma_cross` was active 95 times and supported a direction zero times. That is a
observation, not a verdict — 95 evaluations in one regime over one day is far
too small to retire a component on, and §15.8 forbids it.

---

## §15.9 No-trade diagnosis

**The dominant bottleneck is that the ensemble produces no directional
candidate, not that the threshold is too high.**

* **86 of 96 evaluations (90%) produced no opportunity at all.** On those
  candles the entry threshold was never consulted, because there was nothing to
  compare against it.
* **Median raw confidence is exactly 0.0000.** On a typical candle the ensemble
  has no directional conviction whatsoever.
* On the 10 candles that did produce an opportunity, the best confidence
  observed was 0.5859 against a 0.7000 threshold. Those ten are the only
  evaluations where the threshold mattered at all.

This matters because the intuitive fix — lower the threshold — would address
10% of the problem and nothing else, and the programme forbids it anyway.
Even a threshold of zero would have produced at most ten more candidate entries
in this window, all of them from a consensus the ensemble itself scored below
0.6.

**Everything downstream is unmeasured.** With zero approvals, the risk layer,
sizing, execution feasibility, `EntryProtection` and the executor were never
exercised by organic traffic in this window. Their only end-to-end evidence is
the Phase 12 controlled lifecycle. No statement about risk or execution
behaviour can be supported by this baseline.

---

## §15.10 Strategy decision gate — not yet answerable

The programme asks whether Master Ensemble should remain unchanged, be tuned,
remain as an expert layer, or be replaced as the primary opportunity producer.

**That question cannot be answered from this window.** 96 evaluations, one
regime, one day, one symbol pair, zero approvals. What the window *does*
support is narrowing where to look:

1. The bottleneck is **opportunity generation**, upstream of every gate. Any
   investigation that starts at the threshold is starting in the wrong place.
2. The `WEAK_TREND` regime disables three of seven components, so the ensemble
   was running on four experts for the entire window. Whether that is intended
   at this regime strength is worth checking before anything else.
3. Consensus is not merely low, it is usually *absent*. Eleven supporting votes
   in 95 evaluations is a different failure from "components disagree".

The measurement to take next is the same one, over a window with enough
evaluations and more than one regime. §15.8's threshold for component-level
decisions is not met and should not be worked around.

---

## Caveats stated plainly

* One bot, one day, two symbols, ~18 hours.
* One regime for 95 of 96 evaluations.
* Zero approvals, so the entire post-quality path is unmeasured.
* `trade_fills` is not used here at all. It is 94% legacy backfill from a
  different trading brain (see the Phase 13 report), and profitability metrics
  (§15.6, §15.7) built on it would be measuring that engine, not this one.
  **Profitability is therefore not reported.** It becomes measurable once the
  provenance labelling is applied and enough organic closed trades exist.
