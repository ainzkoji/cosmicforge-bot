# Adaptive Daily Risk Policy Replay Calibration

- Code revision: `b27d8f9af2b4af926098a0f0aef37ec5e4ed3213`
- Risk policy version: `adaptive_daily_risk_budget/1.0.0`
- Dataset hash: `c6d087522db3bd35a1c087b89885a0ca570deca1e8bf40939bf2dc3c4866f64e`
- Opportunity stream: 7 closed broker-outcome trades
- Risk observations: 100
- Canonical DB access: `sqlite-uri-mode-ro`

## Verdict

The replay harness is deterministic and policy-isolated, but the current broker-linked outcome sample is small. Use this as calibration evidence and guardrail validation, not as a statistically complete production-readiness claim.

## Old vs Candidate

| Policy | Approved | Blocked | Net PnL | Expectancy | PF | Max DD | Hard-stop days |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| OLD_6_USDT | 4 | 3 | -2.5699 | -0.6425 | 0.3299 | 0.3376% | 2 |
| R_1_5_CAP_2_5 | 5 | 2 | -0.9167 | -0.1833 | 0.7610 | 0.3376% | 2 |

## Full Grid

| Policy | Approved | Blocked | Net PnL | Expectancy | PF | Max DD | Hard-stop days |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| OLD_6_USDT | 4 | 3 | -2.5699 | -0.6425 | 0.3299 | 0.3376% | 2 |
| R_1_0_CAP_2_5 | 5 | 2 | -2.5699 | -0.5140 | 0.3299 | 0.3376% | 2 |
| R_1_25_CAP_2_5 | 5 | 2 | -0.9167 | -0.1833 | 0.7610 | 0.3376% | 2 |
| R_1_5_CAP_015 | 5 | 2 | -0.9167 | -0.1833 | 0.7610 | 0.3376% | 2 |
| R_1_5_CAP_020 | 5 | 2 | -0.9167 | -0.1833 | 0.7610 | 0.3376% | 2 |
| R_1_5_CAP_030 | 5 | 2 | -0.9167 | -0.1833 | 0.7610 | 0.3376% | 2 |
| R_1_5_CAP_035 | 5 | 2 | -0.9167 | -0.1833 | 0.7610 | 0.3376% | 2 |
| R_1_5_CAP_2_5 | 5 | 2 | -0.9167 | -0.1833 | 0.7610 | 0.3376% | 2 |
| R_1_75_CAP_2_5 | 6 | 1 | -0.9167 | -0.1528 | 0.7610 | 0.3376% | 1 |
| R_2_0_CAP_2_5 | 7 | 0 | -20.0876 | -2.8697 | 0.1269 | 2.6386% | 0 |
| R_2_5_CAP_2_5 | 7 | 0 | -20.0876 | -2.8697 | 0.1269 | 2.6386% | 0 |

## Holdout

{
  "holdout_opportunities": 2,
  "policy_results": {
    "OLD_6_USDT": {
      "approved_holdout_trades": 0,
      "blocked_holdout_trades": 2
    },
    "R_1_0_CAP_2_5": {
      "approved_holdout_trades": 1,
      "blocked_holdout_trades": 1
    },
    "R_1_25_CAP_2_5": {
      "approved_holdout_trades": 0,
      "blocked_holdout_trades": 2
    },
    "R_1_5_CAP_015": {
      "approved_holdout_trades": 0,
      "blocked_holdout_trades": 2
    },
    "R_1_5_CAP_020": {
      "approved_holdout_trades": 0,
      "blocked_holdout_trades": 2
    },
    "R_1_5_CAP_030": {
      "approved_holdout_trades": 0,
      "blocked_holdout_trades": 2
    },
    "R_1_5_CAP_035": {
      "approved_holdout_trades": 0,
      "blocked_holdout_trades": 2
    },
    "R_1_5_CAP_2_5": {
      "approved_holdout_trades": 0,
      "blocked_holdout_trades": 2
    },
    "R_1_75_CAP_2_5": {
      "approved_holdout_trades": 1,
      "blocked_holdout_trades": 1
    },
    "R_2_0_CAP_2_5": {
      "approved_holdout_trades": 2,
      "blocked_holdout_trades": 0
    },
    "R_2_5_CAP_2_5": {
      "approved_holdout_trades": 2,
      "blocked_holdout_trades": 0
    }
  },
  "split": "chronological final 20%",
  "tuning_note": "Ranking is reported after the fixed grid replay; no threshold, strategy, regime, slot, leverage, or affordability parameter is optimized on the holdout."
}

## Limitations

- The canonical broker-outcome stream currently has only the closed positions linked to originating decisions; it is suitable for deterministic policy comparison but not statistically decisive.
- Costs are whatever the broker-fill evidence recorded on positions/trade_fills; missing funding remains zero rather than fabricated.
- This harness does not place orders, does not mutate canonical storage, and does not repair the older wall-clock production replay defect documented separately.
