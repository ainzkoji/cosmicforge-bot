# Post-Restart Runtime Smoke

Successful restart: `2026-06-15T04:46:39.294507+00:00`

## Runtime Result

- Full smoke cycles recorded below: `10`
- Full post-restart cycles observed at final audit: `36`
- BTCUSDT observations in selected smoke: `10`
- ETHUSDT observations in selected smoke: `10`
- Market data loaded: `True` (nonzero changing prices and regime metrics persisted)
- Final post-restart runtime actions: `BUY=0`, `SELL=0`, `HOLD=72`
- Selected ten-cycle actions: `BUY=0`, `SELL=0`, `HOLD=20`
- Submit attempts: `0`
- Paper orders created: `0`
- Positions opened: `0`
- Trade fills after restart: `0`
- Safe diagnostic paper executor reachable: `True`

All selected runtime cycles occurred before the configured `06:00 UTC` session
open. BTCUSDT was explicitly `SESSION_BLOCKED`; ETHUSDT was either
`SESSION_BLOCKED` or `REGIME_BLOCKED_STRONG_TREND`. Component evaluation was
therefore intentionally skipped by an earlier safety gate, not silently dead.
An in-session runtime component cycle remains required to satisfy the strict
runtime-component-observability acceptance item.

## Regime Hysteresis

- BTCUSDT startup/stable regime: `WEAK_TREND`
- ETHUSDT startup regime: `STRONG_TREND`
- ETHUSDT observed stable changes:
  `STRONG_TREND -> WEAK_TREND -> STRONG_TREND`
- Candidate counter behavior: transition calls at `0.800` and `0.766`
  confidence are consistent with the repaired one-call stabilization output;
  the following matching call updated the stable state.
- Stable regime updates: observed after the configured two-call threshold.
- `REGIME_FIX_NOT_ACTIVE`: `False`

The candidate regime/count detail is emitted by `classify_stable()` during a
stabilizing call but is not persisted in `decision_traces`; candidate behavior
above is an inference from the damped-confidence transition calls and following
stable-state updates.

## Signal And Component Diagnostics

Runtime gate diagnostics were explicit:

- `SESSION_BLOCKED`
- `REGIME_BLOCKED_STRONG_TREND`

A post-restart diagnostic-only component replay generated at
`2026-06-15T04:54:57.436062+00:00` proved the path remains alive without
calling an executor:

- component BUY/SELL activity: `True`
- component examples: supertrend `BUY=207/SELL=463`, SMA cross
  `BUY=26/SELL=23`
- ensemble decisions: `BUY=5`, `SELL=11`, `HOLD=1984`
- most common exact failed condition: `fresh_fast_slow_sma_cross`
- source: `post_restart_component_diagnostic.json`

The diagnostic-only reachability function returned
`executor_would_be_called=True` for a BUY with strategy gate allowed, IOFS in
shadow, ML disabled, and risk limits available. It did not construct or call an
executor and created no order.

## Selected Ten Cycles

| Cycle | BTC timestamp / stable regime / reason | ETH timestamp / stable regime / reason |
|---:|---|---|
| 1 | `04:46:51Z` / WEAK_TREND / SESSION_BLOCKED | `04:47:28Z` / STRONG_TREND / REGIME_BLOCKED_STRONG_TREND |
| 2 | `04:47:38Z` / WEAK_TREND / SESSION_BLOCKED | `04:47:45Z` / STRONG_TREND / REGIME_BLOCKED_STRONG_TREND |
| 3 | `04:47:54Z` / WEAK_TREND / SESSION_BLOCKED | `04:48:01Z` / WEAK_TREND / SESSION_BLOCKED |
| 4 | `04:48:10Z` / WEAK_TREND / SESSION_BLOCKED | `04:48:17Z` / WEAK_TREND / SESSION_BLOCKED |
| 5 | `04:48:26Z` / WEAK_TREND / SESSION_BLOCKED | `04:48:33Z` / WEAK_TREND / SESSION_BLOCKED |
| 6 | `04:48:42Z` / WEAK_TREND / SESSION_BLOCKED | `04:48:50Z` / STRONG_TREND / REGIME_BLOCKED_STRONG_TREND |
| 7 | `04:49:00Z` / WEAK_TREND / SESSION_BLOCKED | `04:49:07Z` / STRONG_TREND / REGIME_BLOCKED_STRONG_TREND |
| 8 | `04:49:16Z` / WEAK_TREND / SESSION_BLOCKED | `04:49:23Z` / STRONG_TREND / REGIME_BLOCKED_STRONG_TREND |
| 9 | `04:49:33Z` / WEAK_TREND / SESSION_BLOCKED | `04:49:41Z` / STRONG_TREND / REGIME_BLOCKED_STRONG_TREND |
| 10 | `04:49:50Z` / WEAK_TREND / SESSION_BLOCKED | `04:49:57Z` / STRONG_TREND / REGIME_BLOCKED_STRONG_TREND |

For every row: `session_allowed=False`, raw/final action is `HOLD`,
confidence is `0.0`, IOFS was not reached after the earlier gate, executor was
not reached, and no paper order was created. Full per-symbol records are in
`post_restart_runtime_smoke.json`.

## Acceptance

Controlled restart, safety configuration, market data, repaired hysteresis,
diagnostic signal path, and paper executor reachability are proven. Strict
acceptance is **pending** only for an in-session runtime cycle that persists
component outputs; no safety setting was bypassed to manufacture that evidence.
