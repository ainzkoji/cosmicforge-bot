# Trend ensemble v2 (traditional markets): DRAFT — NOT FROZEN, NOT REGISTERED

Status: **DRAFT**. Written 2026-10-17. This document is not a specification in the sense of
`research/trend_v1/SPEC.md`: it is not frozen, it has no number in the research register, no data has been
loaded for it, no code evaluates it and no result exists. Anything in it may still change. It becomes a
specification only through the registration procedure at the end of this file.

**Mandate 004 certification does not transfer.** Whatever the outcome of Mandate 004 (hypothesis 18, the
Binance USD-M perpetual daily trend ensemble), it says nothing about traditional-market contracts. The
instruments, sessions, costs, carry, data source and history are all different. A traditional-market version
of the family needs its own frozen specification, its own registered hypothesis, its own development and
held-back periods and its own certification. No status, stage, approval or evidence of the crypto family is
reused for it.

Every number below that is not marked as fixed by the platform is a proposal, listed again in
"Open research-policy decisions". None of them was chosen by looking at a result.

## Question (proposed)

Does a long-or-flat breakout ensemble on liquid traditional-market contracts make money after commissions,
spread and slippage, roll costs and financing, within each risk level's loss limits?

Whether the question is long-or-flat (as in v1) or long-short is itself an open decision (D1): futures and FX
are symmetric instruments in a way crypto perpetuals on a long-only platform were not treated as.

## What differs from v1, and what the specification must settle

### Market structure

| Topic | v1 (crypto perpetuals) | v2 (traditional markets) — to be specified |
|---|---|---|
| Trading hours | Continuous, 24/7 | Venue-local sessions, weekends closed, overnight sessions spanning midnight, daily maintenance breaks |
| Holidays and early closes | None | Venue holiday calendar and early closes, per year, from a cited source |
| Daily bar boundary | 00:00 UTC | Venue settlement or session close, which moves with daylight saving in UTC terms |
| Contract lifetime | Perpetual | Dated futures expire; FX spot rolls daily; equities can be delisted, split, merged |
| Price / quantity rules | Tick size, step size, min notional from the venue | Tick size, tick value, contract multiplier, minimum quantity, quantity step per contract |
| Carry | Funding rate every 8 h, from the archive | Futures: carry is inside the price (basis) and appears at the roll. FX / CFD: overnight financing (swap) per broker. Equities: borrow cost if shorted, dividends |
| Corporate actions | None | Dividends, splits and symbol changes adjust price history (equities, equity-index CFDs) |
| Settlement | USDT | Contract settlement currency (USD, EUR, JPY ...); conversion to account currency is part of the PnL |
| Margin | Venue leverage brackets | Exchange or broker initial / maintenance margin, which changes over time and by volatility regime |
| History available | 2020-01-01 onward in the public archive | Depends on the licensed data source (D4) |

### Data (to be specified)

- The data source, its licence, and whether the licence permits research use and retention (D4).
- For futures: individual contract histories (not a vendor's continuous series whose construction is
  unknown), with first-notice and last-trade dates per contract.
- For FX: the price type (bid, ask, mid) and the venue the prices come from; OTC FX has no single
  authoritative price.
- Session times, holidays and early closes as versioned calendar data with a cited source; the platform's
  calendar engine (`backends/bot-backend/app/markets/`) refuses an UNVERIFIED calendar for order submission.
- Development period and held-back period, chosen before any data is loaded (D5). The held-back period is
  evaluated once and never used to choose anything, as in v1.
- Point-in-time universe: contracts that were later delisted or stopped trading are included.

### Universe (to be specified)

A fixed list of instruments or a point-in-time liquidity rule (D2). Proposal for discussion only: a small
fixed set of the most liquid equity-index, rate, currency and commodity futures on one venue group. The list,
the venue and the contract months are owner decisions (D2, D3).

### Contract rolls (to be specified)

- Which contract is held on each date (D6): e.g. a fixed number of business days before first notice or last
  trade, or a volume / open-interest crossover rule. The rule must be computable from data available on the
  decision date.
- **Back-adjustment method is an open decision (D7):** difference (additive) adjustment, ratio
  (multiplicative) adjustment, or no adjustment with signals computed per contract. Signals that use
  breakouts over 10 to 150 days depend on this choice. Whatever is chosen, PnL is always computed on the
  traded contract's actual prices, never on an adjusted series.
- Roll cost: the roll is two trades (close old, open new) and pays the cost model twice.
- No order is ever placed in an expired contract or a contract that is not yet listed
  (`app/markets/roll.py` enforces this in code).

### Signal and sizing (proposed: as v1, adapted)

- The v1 lookbacks {10, 20, 30, 45, 65, 100, 150} sessions, applied to session bars instead of UTC days.
  Whether "days" means sessions or calendar days is D8.
- Stop distance d = 3 x ATR(20) / close as in v1 is a proposal. **The platform caps apply regardless of
  anything chosen here: at most 0.40 % of equity at risk per trade, and an entry stop at most 15 % from the
  entry price.** The specification may choose tighter values, never wider ones.
- Quantity is an integer number of contracts (or the instrument's quantity step). A target smaller than one
  contract is zero, never rounded up.
- Gap risk: a closed market cannot be exited. A stop placed at the venue may fill far through the stop on the
  next open. The specification states how gaps are simulated (fill at the open when it is through the stop).

### Execution and costs (to be specified)

- Decision time: after the session close and settlement price; orders fill at the next session's open, or at
  a stated time inside the session (D9).
- Cost model (D10): commission per contract, exchange and clearing fees, spread and slippage in ticks, a
  stress case. FX / CFD: broker spread and overnight financing. All values from cited sources or measured;
  none are assumed.
- Financing and carry accrual: computed by `app/markets/economics.py` from the stated model (futures: none
  outside the roll; FX / CFD: daily financing on held notional, triple on the stated rollover weekday if the
  broker documents one).
- Dividends and corporate actions (equities, index CFDs): adjustment records applied to history; the
  position receives or pays the dividend adjustment the broker documents.
- PnL is in the contract's settlement currency and converted to the account currency at a stated rate.

### Reported (proposed: as v1)

Net and gross annual return, volatility, Sharpe ratio, maximum drawdown, return by calendar year, trades,
turnover, cost, roll-cost and financing drag, share of days with a position, brake activity, and the same
figures for stated benchmarks (D11).

### Pass rule (to be specified)

The v1 structure is proposed (held-back net return positive at base and stress cost, positive in most
calendar years, drawdown limit without the brake, minimum held-back Sharpe ratio). Every threshold is an
owner decision (D12). As in v1, anything else is a fail and is reported as one, and a secondary test cannot
rescue a fail.

## Open research-policy decisions (owner approval required)

Nothing in this table has a value yet. A row is settled only by the owner, in writing, before the
specification is frozen, and the decision is recorded with the specification.

| # | Decision | Notes / constraints |
|---|---|---|
| D1 | Direction: long-or-flat or long-short | Short futures / FX positions change the risk model and the brake design |
| D2 | Universe and its selection rule | Fixed list or point-in-time liquidity rule; must be point-in-time |
| D3 | Venue(s) and broker(s); which instruments (futures, FX spot, CFDs, equities) | Determines settlement currency, margin source and which execution adapter is needed (none exists today) |
| D4 | Data source and licensing | Licence must permit research use, storage and audit retention; continuous-series construction must be known |
| D5 | Development period and held-back period | Fixed before any data is loaded; held-back period evaluated once |
| D6 | Roll rule (which contract is held on each date) | Computable from data known on the decision date |
| D7 | Back-adjustment method for signals | Difference, ratio or none; PnL always on actual traded prices |
| D8 | Lookback unit (sessions or calendar days) and the daily bar boundary | Session close / settlement vs a fixed clock time |
| D9 | Decision and fill timing | Next open vs a stated intraday time; gap handling |
| D10 | Cost model: commissions, fees, spread, slippage, stress multiplier, financing | Every value cited or measured |
| D11 | Benchmarks | e.g. buy-and-hold of the universe, volatility-matched |
| D12 | Pass rule thresholds | Return, years-positive count, drawdown limit, minimum Sharpe |
| D13 | Risk levels (per trade, open risk, positions, leverage, daily pause, drawdown brakes) | Platform caps apply and cannot be exceeded: 0.40 % per trade, entry stop at most 15 % |
| D14 | Session calendars, holidays and early closes for every venue in scope | Versioned data with a cited source; UNVERIFIED calendars are refused for order submission |
| D15 | Contract specifications (multiplier, tick size, tick value, quantity step, minimum quantity, margin rule, expiry, first notice, last trade) | From the venue's published specifications, cited per contract |
| D16 | Staged activation durations (paper and demo periods) | `app/markets/activation.py` blocks any duration-dependent transition until these are set |

## Registration procedure

1. The owner settles every row of the table above, in writing.
2. The draft is rewritten as a specification with every decision filled in, the title changed from DRAFT to
   "frozen before any result was seen", and the date of freezing.
3. The frozen specification is registered through the governed research register
   (`docs/research/registry/research_register.jsonl`, via the register tooling, never by editing the file)
   as **H-019**, with its own mandate and the hash of the specification. H-019 is reserved for this purpose
   only after the owner approves; until then no hypothesis number is assigned and nothing is appended to the
   register.
4. Freeze rules, as for v1: after registration nothing in the specification changes. Points the
   specification leaves open are fixed in numbered interpretation records registered as amendments before
   any data is loaded. Any later idea is a new, separately numbered specification, and the number of
   specifications tried is reported with every result.
5. Only then is data loaded and code written to evaluate it. A pass is a research result only; trading the
   family additionally needs the staged activation of `app/markets/activation.py` (PAPER, then DEMO with the
   CERTIFIED research status, then LIVE, which this build refuses) and an execution adapter for the venue.
