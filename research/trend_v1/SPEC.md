# Trend ensemble v1: specification, frozen before any result was seen

Frozen 2026-10-07. Nothing below may be changed after the first backtest run.
Any later idea is a new, separately numbered specification, and the number of
specifications tried is reported with every result.

## Question

Does a long-or-flat breakout ensemble on liquid Binance USD-M perpetuals make
money after fees, slippage and funding, within each risk level's loss limits?

## Data

- Binance USD-M perpetual daily klines and funding-rate history from the public
  archive (data.binance.vision), USDT-margined contracts only.
- Development period: 2020-01-01 to 2024-12-31.
- Held-back period: 2025-01-01 to 2026-09-30. It is evaluated once, after the
  code passes its tests on the development period. It is never used to choose
  anything.
- Symbols that were later delisted are included wherever the archive has them.
  If the symbol list cannot be built point-in-time, the report says so and
  states the survivorship risk.

## Universe (point in time)

On each day, the 20 contracts with the highest median daily quote volume over
the previous 30 days, among contracts with at least 120 days of history.
Stablecoin-against-stablecoin contracts and non-crypto contracts are excluded.

## Signal

Daily closes at 00:00 UTC. For each lookback L in {10, 20, 30, 45, 65, 100, 150}
days, a sub-signal is:

- ON when today's close is at or above the highest close of the previous L days;
- OFF when today's close is below the lowest close of the previous L/2 days
  (rounded down, minimum 5);
- otherwise unchanged.

Signal strength s is the fraction of the seven sub-signals that are ON (0 to 1).
There are no short positions.

## Sizing

- Stop distance d = 3 x ATR(20) / close, floored at 5% and capped at 40%.
  The exchange-side stop sits at entry x (1 - d) and ratchets up only.
- Target notional for a coin = s x (risk per trade x equity) / d.
- Risk levels (share of equity):

  | | Conservative | Balanced | Aggressive |
  |---|---|---|---|
  | Risk per trade | 0.25% | 0.50% | 0.75% |
  | Max total open risk | 1% | 2% | 3% |
  | Max positions | 4 | 6 | 8 |
  | Leverage ceiling (total notional / equity) | 1x | 2x | 3x |
  | Daily pause (no new entries that day) | -1% | -2% | -3% |
  | Halve all sizes at drawdown | -5% | -8% | -12% |
  | Close everything and stop at drawdown | -10% | -15% | -25% |

- When more coins qualify than the position limit, keep those with the highest
  s, ties broken by higher 30-day volume. Open risk is counted as the sum of
  notional x d; if it exceeds the level's maximum, all positions scale down
  proportionally.
- After the stop level is hit the account stays flat for the rest of the test
  (a user must restart by hand). Results are reported both with and without the
  drawdown brake so its effect is visible.

## Execution and costs

- Decisions use data up to and including the close of day t; orders fill at the
  open of day t+1.
- A position is adjusted only on entry, on exit, or when the target differs
  from the holding by more than 25% of the target.
- Base cost: 0.05% taker fee plus 0.05% slippage per side (0.10% per side).
  Stress case: twice that.
- Funding: the contract's actual funding rates applied to the notional held.
- A stop is hit when the day's low is at or below the stop; the fill is the
  stop price, or the open if the day opens below it, less slippage.

## Reported for each risk level

Net and gross annual return, volatility, Sharpe ratio, maximum drawdown, return
by calendar year, number of trades, turnover, fee and funding drag, share of
days with any position, days on which each brake fired, and the same figures
for two benchmarks: Bitcoin buy-and-hold and an equal-weight universe, both
scaled to the same volatility.

## Pass rule

The strategy passes only if all hold at Balanced:

1. Held-back net return is positive at base cost and at stress cost.
2. Net return is positive in at least 4 of the 6 calendar years 2020 to 2025.
3. Without the brake, maximum drawdown over the full period stays inside 15%.
4. Held-back net Sharpe ratio is at least 0.3.

Anything else is a fail and is reported as one.

## Secondary test (reported separately, cannot rescue a fail)

Funding overlay: halve a coin's target when its trailing 3-day mean funding
rate, annualised, is above 30%; set it to zero above 60%.
