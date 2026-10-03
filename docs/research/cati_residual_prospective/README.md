# Frozen residual momentum prospective evidence

This tracker observes `RESIDUAL_MOMENTUM_PORTFOLIO_TOP1` from Mandate 003. It
does not promote the failed historical family, grant execution authority, train
a forecast, consume another historical evaluation run, or access holdout.
The frozen registry remains unchanged and runtime eligibility remains false.

The canonical CATI cycle schedules one bounded asynchronous worker, gated by
the current process's runtime lease. Existing public microstructure observation
and the FX writer/completion pipeline run independently. The tracker uses only
unauthenticated GET requests to public Binance USD-M reference candles; it has
no execution client or broker-order path.

At initial enrollment, it records the next future aligned hourly close as its
first possible decision. Approximately 28 days of recent public 15m candles
warm up the trailing 672 complete hourly returns. These are predictor inputs,
never historical decisions or historical outcome labels. No research database,
historical result ledger or reserved holdout dataset is read. Closed inputs are
kept in a separate, source-tagged, bounded cache in the canonical database.

Every new hour persists the actual eligible symbols, complete registered
universe, missing-input reasons, top-ranked symbol/side/score, decision-close
entry reference, fixed structural risk/stop/target, frozen modeled costs and
OBSERVE/blocked risk state. No-signal and insufficient-data decisions persist
as explicit skips. A delayed or missed boundary is a skip, never a replayed
historical decision. Inputs require exactly four aligned closed 15m bars per
hour, 673 consecutive hourly closes, full-rank BTC/ETH OLS, and at least 30
complete tradable assets. Current-window coefficients reconstruct the 672h
residual volatility and 24h residual sum. ATR is the existing simple 14h mean;
prior 24h swings exclude the current bar. Top absolute score >=2 and lexical
tie-break, .25 ATR swing padding, max(structure,2 ATR,.003 close), 2.5R target,
48h horizon, universe and costs are unchanged.

The decision record is committed before requesting the next native 15m opening
reference or any subsequent outcome bar. Real `recorded_at`, receipt timestamps
and recording latency are persisted. Decisions must be recorded before the
first 15m outcome bar closes. The next-open reference is an observation anchor,
not a claim that a delayed real order could fill there. Its partial H/L/C are
never used. Non-executable entry gaps are explicit and never resize geometry or
select a substitute symbol. Closed later bars settle stop/target in order, with
gap-aware stop price and stop priority on an ambiguous bar; otherwise the 192nd
bar closes the 48h timeout. Missing/out-of-order/future bars cannot fabricate an
outcome. An already enrolled forward observation survives restart, retaining its
exact cursor and immutable selection; missing path segments may be reconciled
only for that previously registered observation.

One registered portfolio position may be pending/open at a time, enforced by
an atomic database transaction and unique partial index. Each hourly top1
candidate still receives a prospective counterfactual observation and eventual
outcome if otherwise executable. Overlap-rejected observations have
`portfolio_selected=0`, retain their rejection reason and are never portfolio
trades. Never pool those two populations. TARGET/STOP/TIMEOUT, gross R, actual
entry/exit-notional modeled costs, net R and 1.5x/2x cost stress are persisted.
Every outcome retains the full six-period funding reserve; book proxies or
public spreads never lower the frozen fee/spread/slippage assumptions.

Persistence tables:

- `cati_residual_tracker`: durable activation, future boundary, last processed
  boundary, collector heartbeat and source/error state.
- `cati_residual_inputs`: recent closed public native bars and receipt lineage.
- `cati_residual_decisions`: immutable prospective selection/geometry/cost/risk
  snapshot plus entry reference and incremental eventual outcome lifecycle.

Read-only operator status (no historical prices or holdout queries):

```powershell
backends\venv\Scripts\python.exe scripts\cati_residual_prospective_status.py
```

If public data is unavailable, the tracker records skips/errors rather than
changing the frozen universe. HTTP 418/429 invokes five-minute backoff. The
observer never blocks the CATI cycle. Runtime log tag:
`[CATI_RESIDUAL_PROSPECTIVE]`. Source identity:
`BINANCE_PUBLIC_RESIDUAL_PROSPECTIVE_V1`.

The public candle format is documented by [Binance](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data).

Verification: synthetic tests compare scores/ATR/swing/geometry with the frozen
evaluator and cover future/gap rejection, lexical selection, minimum support,
prospective enrollment, idempotency, persisted restart cursor, portfolio
occupancy, symmetric targets, ambiguous stop priority, entry gaps, actual
notional costs, exact timeout and lease/test-mode scheduling. No production
outcome or model is created by those tests.
