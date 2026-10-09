# Trend ensemble v1: implementation interpretation 001

Recorded 2026-10-09, before any price series was loaded and before any backtest was run. Registered in the
research register as amendment 1 to Mandate 004 (hypothesis 18).

`SPEC.md` is the frozen authority and is not changed by this file. Where the specification states a number or a
rule, that number or rule is used as written. This file only fixes the points the specification leaves open, so
that the evaluator has exactly one behaviour and that behaviour was chosen without seeing a result. None of
these items was chosen by comparing outcomes; none may be revisited after the first run. A different choice on
any item is a new, separately numbered specification.

The same choices are held as data in
`backends/bot-backend/app/trading_intelligence/families/daily_trend/spec.py` (`INTERPRETATION`) and are part of
the registered rule artifact hash.

## I1. Contracts

- Each symbol in the public archive is one contract with its own history. A relisted ticker does not inherit
  the history of the delisted one.
- A USDT-margined perpetual is an archive symbol that ends in `USDT`, or in `USDT` followed by one or more
  `SETTLED` suffixes (the archive's name for a contract that was settled and later replaced). Dated delivery
  contracts (`_YYMMDD`) and contracts margined in another asset are not in scope.
- If two symbols with the same base name have a bar on the same day, the one with fewer `SETTLED` suffixes is
  used on that day.
- "History" means history in the archive. The archive starts on 2020-01-01; nothing earlier is added from any
  other source.

## I2. Exclusions

- Stablecoin against stablecoin: the base asset is one of AEUR, BFUSD, BUSD, DAI, EURI, FDUSD, PYUSD, RLUSD,
  TUSD, USD0, USD1, USDC, USDD, USDE, USDP, USDS, UST, XUSD.
- Non-crypto: the contract type is `TRADIFI_PERPETUAL` in the exchange metadata snapshot stored with the
  dataset (equities, commodities, currencies, pre-listing companies). Index contracts built from crypto assets
  are crypto.
- A symbol that is not in the metadata snapshot (delisted before the snapshot) is treated as crypto. The
  snapshot is today's, not point-in-time; this is stated as a limitation in the data report.

## I3. Universe

- A contract has "at least 120 days of history" on day t when it has a daily bar on each of the 120 days ending
  on t. A missing day restarts the count.
- "The previous 30 days" are the 30 days before the decision day (t-30 to t-1), the same meaning "previous"
  has in the signal rule.
- Ties in volume are broken by symbol, ascending.

## I4. Sub-signals

- A sub-signal starts OFF. It cannot turn ON until L earlier closes exist, and cannot turn OFF by rule until
  its exit window exists.
- On a day without a bar the sub-signal is reset to OFF.
- Strength is always the number ON divided by seven.

## I5. ATR

- True range uses the previous day's close. ATR(20) is the simple mean of the 20 true ranges ending on the
  decision day. It is undefined until 21 consecutive bars exist; a coin without it has no stop distance and
  cannot be entered.

## I6. Account and order size

- The account starts with 10,000 USDT. Results are in percent of equity; the amount matters only for exchange
  minimum sizes.
- Equity in the sizing formula is the account marked to market at the decision close.
- An order is sized in contracts at the decision close (target notional divided by that close) and fills at the
  next open.

## I7. Selection

- Coins qualify when they are in the day's universe, have strength above zero and have a stop distance.
- When more qualify than the position limit: highest strength first, then higher 30-day median volume, then
  symbol ascending. A coin already held has no priority.
- A held coin that is not in the day's universe, or is not selected, has a target of zero and is closed at the
  next open.

## I8. Caps

- Applied in this order: the halve brake, then total open risk, then the leverage ceiling. Each scales all
  targets by the same factor.
- Open risk uses the current stop distance d of each coin, as the specification states (notional x d).

## I9. Brakes

- Halve: while the drawdown from the highest closing equity is at or beyond the level's threshold, all targets
  are halved. It lifts when the drawdown is smaller again.
- Close everything and stop: permanent for the rest of the run.
- Daily pause: when the day's return is at or below minus the level's threshold, the decisions of that close
  open no new position. Existing positions are still adjusted and closed.
- "Without the brake" means both drawdown brakes off and the daily pause on.

## I10. Stops

- The stop of a new position is its fill price x (1 - d), with d from the decision day.
- At each close the stop becomes the larger of its previous value and close x (1 - d of that day).
- The stop is live from the entry day. A position can be stopped on the day it is opened.
- After a stop the coin can be entered again by a later decision. The specification defines no waiting period.
- If a day opens below the stop while an order to add is pending, the add fills at the open and the whole
  position is then stopped at the open, less slippage.

## I11. Exchange sizes

- Quantity step and minimum notional come from the metadata snapshot (today's values, not historical), rounded
  down to the step. An order below the minimum is rejected and reported.
- A contract without metadata is not rounded and has a minimum notional of 5 USDT.

## I12. Funding

- A long position pays when the rate is positive and receives when it is negative.
- A funding event at 00:00 applies to the quantity held before that day's opening fills. Later events of the
  day apply to the quantity held after them.
- The notional of every event of a day is quantity x that day's open.
- On a day a position is stopped out, later events that are a cost are charged and those that are income are
  not credited.

## I13. Missing funding

- A funding event that the archive does not contain is charged as a cost of 0.03% per 8 hours (in proportion
  for other intervals). It is never treated as zero.
- If charged events of this kind exceed 2% of all funding events applied in a run, the run is not certifiable
  (insufficient data).

## I14. Missing bars

- Prices are never filled forward. A held coin without a bar (a gap, or the contract ended) is closed at its
  last close with the stress cost (twice the base cost) and the event is reported.

## I15. Periods and the pass rule

- Calendar-year returns and the full-period drawdown come from one continuous account started on 2020-01-01.
  A year's return is year-end equity over the previous year-end equity.
- Held-back figures come from a fresh account started on 2025-01-01, with indicators computed from earlier
  data. Development figures are the continuous account up to 2024-12-31.
- Pass rule items 1, 2 and 4 use the strategy as specified (brakes on). Item 3 uses the drawdown brakes off.
- "Inside 15%" means at most 15%.

## I16. Statistics

- Sharpe ratio: mean of daily net returns over their sample standard deviation, times the square root of 365,
  with no risk-free rate. Annual return is geometric. Drawdown is measured on daily closing equity.

## I17. Benchmarks

- Bitcoin buy-and-hold: BTCUSDT perpetual held long at 1x, with its funding and one entry cost.
- Equal-weight universe: each day, equal weights in the previous day's universe, with funding and the base
  cost on the traded amount.
- Both are scaled after the fact to the strategy's realised volatility over the same period. This is a
  comparison device, not a tradable benchmark.

## I18. Funding overlay (secondary test)

- A coin's rate is the mean funding rate per event over the three days ending on the decision day, annualised
  by the number of events per year. Above 30% its raw target is halved, above 60% it is zero, before the caps
  of I8.

## I19. Executable-policy scenario (reported separately)

- The engine today limits risk per trade to 0.40% and refuses an entry whose stop is more than 15% away. A
  second set of runs applies those two limits: risk per trade is the smaller of the level's value and 0.40%,
  and an entry with d above 15% is rejected and counted.
- These runs describe what could be deployed today. They are not part of the pass rule and cannot rescue or
  replace it.

## I20. Statistical gate

- The pass rule of the specification is binding as written. In addition, the portfolio-level test of the
  research governance specification is computed on daily net returns. Its significance level, required power
  and target effect are not given by the specification and are not approved; until they are, that test reports
  `BLOCKED_PENDING_APPROVAL` and no certification can be recorded as passed.
