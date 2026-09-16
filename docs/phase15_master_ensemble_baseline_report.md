# CosmicForge - Phase 15 Master Ensemble baseline

Measured evidence only. This report is generated from the accompanying `phase15_master_ensemble_baseline_results.json` artifact.

## Scope

- Window: `2026-09-08T04:32:36.858606+00:00` -> `2026-09-10T23:45:06.996541+00:00`
- Bots: `bot_a8117dc719fc`
- Provenance: `PAPER_FORWARD`
- Real evaluations: `396`
- Decision rows: `4812`

## Decision Funnel

| Reason | Count |
| --- | ---: |
| `NO_NEW_CANDLE` | 4416 |
| `NO_OPPORTUNITY` | 155 |
| `REGIME_LOW_VOL_CHOP` | 133 |
| `REGIME_BLOCKED` | 20 |
| `SESSION_BLOCKED` | 35 |
| `ENTRY_CONFIDENCE_BELOW_THRESHOLD` | 47 |
| `STOP_REQUESTED` | 6 |

## Transition Rates

| Transition | Rate |
| --- | ---: |
| `evaluation_to_opportunity` | 0.1389 |
| `opportunity_to_quality_approved` | 0.0000 |
| `approved_to_risk_seen` | 0.0000 |
| `risk_seen_to_execution_feasible` | 0.0000 |
| `feasible_to_attempt` | 0.0000 |
| `attempt_to_fill` | 0.0000 |

## Confidence And Thresholds

```json
{
  "confidence": {
    "confidence_minus_threshold": {
      "max": 0.0,
      "mean": -0.07554715996392802,
      "min": -0.504195571288446,
      "n": 283,
      "p10": -0.3969734191786126,
      "p50": 0.0,
      "p90": 0.0
    },
    "effective_entry_threshold": {
      "max": 0.814643,
      "mean": 0.1565098595890411,
      "min": 0.0,
      "n": 292,
      "p10": 0.0,
      "p50": 0.0,
      "p90": 0.7
    },
    "raw_confidence": {
      "max": 0.6046431056191879,
      "mean": 0.0636785608841285,
      "min": 0.0,
      "n": 283,
      "p10": 0.0,
      "p50": 0.0,
      "p90": 0.30497906681142817
    }
  },
  "consensus": {
    "buy_score": {
      "max": 0.6046,
      "mean": 0.01837948717948718,
      "min": 0.0,
      "n": 390,
      "p10": 0.0,
      "p50": 0.0,
      "p90": 0.0
    },
    "consensus_observed": {
      "max": 0.6046431056191879,
      "mean": 0.05908535321379792,
      "min": 0.0,
      "n": 305,
      "p10": 0.0,
      "p50": 0.0,
      "p90": 0.30449572291403276
    },
    "sell_score": {
      "max": 0.5859,
      "mean": 0.028629230769230768,
      "min": 0.0,
      "n": 390,
      "p10": 0.0,
      "p50": 0.0,
      "p90": 0.0
    }
  },
  "threshold_gap_buckets": {
    "at_threshold_or_unresolved": 228,
    "below_by_10_to_20pct": 2,
    "below_by_5_to_10pct": 1,
    "below_by_more_than_20pct": 52
  }
}
```

## Regime And Component Evidence

```json
{
  "components": {
    "active": {
      "donchian_breakout": 305,
      "sma_cross": 305,
      "supertrend": 305,
      "trend_pullback": 305
    },
    "never_active": [
      "bollinger_reversion",
      "squeeze_breakout",
      "vwap_reversion"
    ],
    "never_supported": [
      "sma_cross"
    ],
    "opposing": {
      "supertrend": 1
    },
    "support_count_distribution": {
      "0": 341,
      "1": 52,
      "2": 3
    },
    "supporting": {
      "donchian_breakout": 10,
      "supertrend": 37,
      "trend_pullback": 11
    }
  },
  "regimes": {
    "LOW_VOLATILITY_CHOP": 36,
    "STRONG_TREND": 14,
    "WEAK_TREND": 340
  }
}
```

## Profitability After Costs

```json
{
  "closed_trades": 0,
  "note": "no closed positions; profitability is not measurable"
}
```

## Diagnosis

- NO_DIRECTIONAL_CANDIDATE is dominant: 341/396 (86%) of evaluations produced no opportunity at all, so the entry threshold was never consulted on them.
- Median raw confidence is exactly 0.0 — on a typical candle the ensemble has no directional conviction whatsoever.
- Components active but never supporting a direction: sma_cross.
- Components never active in this window (regime-disabled): bollinger_reversion, squeeze_breakout, vwap_reversion.
- Zero approvals in 396 evaluations: nothing reached the risk layer, so risk, sizing, feasibility and execution are entirely unmeasured.

## Phase 15 Classification

`E_INCONCLUSIVE` - sample lacks enough evaluations and/or closed trades for a primary-strategy retention or replacement decision

## Historical Benchmark

| Period | Historical Issue | Current Status |
| --- | --- | --- |
| `MAY_2026` | STOP_TOO_WIDE historical unit/path defect | not part of current Master Ensemble baseline path |
| `JULY_2026_EXECUTION_DEFECT` | PAPER_ONLY without actual simulated fills | Phase 12/13 evidence path records attempts, fills and positions when approvals occur |
| `JULY_2026_NATURAL_BEHAVIOR` | strategy_no_signal/HOLD / opportunity scarcity | still the dominant observed bottleneck when opportunity rate is low |
| `SEPTEMBER_2026` | multiple confidence authorities, same-candle repeated evaluation, generic HOLD evidence, configuration conflict, lifecycle defects | threshold authority and lifecycle are tested separately; baseline still measures zero downstream exercise when approvals are absent |

## Phase 16 Implication

This artifact does not start or complete Phase 16. Phase 16 still requires fresh forward-paper identity, at least three consecutive real forward weeks, and at least 60 correctly closed campaign trades.
