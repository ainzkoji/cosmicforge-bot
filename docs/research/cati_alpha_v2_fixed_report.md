# CATI alpha V2 — fixed four-mechanism development evaluation

All four fixed hypotheses FAIL economic admission. RESEARCH_PROMISING is false for all four; next model training is blocked. Economic admission is assessed independently for every fixed hypothesis. No aggregate union is a promoted strategy. V1 remains immutable and REJECTED. This is research on a development range repeatedly inspected before this run; no untouched confirmation claim applies.

Generator: `CATI_ALPHA_CAUSAL_DIVERSIFIED_V2`. Registered source universe: 136; available native 15m assets: 136. The universe is all symbols in the existing V4 source metadata, including BTC and ETH; no instrument was selected using profitability. This is a source-supported survivor universe, not a historical delisting-complete universe.

## Predeclared research budget

One evaluation run, four mechanisms, fixed 15m execution and 48-bar (12h) terminal horizon. No threshold search, predictor fit, mechanism replacement or automatic next alpha version. Signals and numerical thresholds were fixed in the generator before evaluation. The exact generator/evaluator hashes are recorded below.

- Cross-sectional strength: signed 96-bar excess return >1% versus BTC and ETH, >1.5% versus exact-close equal-weight basket; signed 12-bar momentum >0.2%, signed body >0.35 ATR, volume ratio >=1.2. Basket requires >=30 actually observed continuous assets at that timestamp.
- Volatility transition: previous ATR <=0.85 of shifted 48-bar ATR baseline, current ATR >1.1 baseline, 12-bar path efficiency >0.5, signed body >0.6 ATR, volume ratio >=1.5. This uses a regime crossing, not V1 breakout acceptance.
- Multi-timeframe pullback: closed 1h and 4h 12/48-bar moving average direction agree, both normalized eight-bar slopes >0.25, prior execution close across fast average followed by reclaim; pullback depth 0.25..1.5 ATR; signed body >0.25 ATR; volume ratio >=1.1.
- Flow confirmed direction: only actual candle volume evidence; volume ratio >=2, path efficiency >0.4, signed body >0.75 ATR, signed momentum >0.2%. OI, basis, signed trade flow and actual funding are unavailable and omitted. This is a volume confirmation hypothesis, not a claim of measured order flow.

All hypotheses require 97 continuous execution bars and exact-close BTC/ETH continuous context. A gap invalidates warmup. 1h/4h derivation requires exactly 4/16 aligned contiguous native 15m bars; context uses the last fully closed HTF bar and rejects stale contexts. Native 5m was unavailable in the bounded source check and is never fabricated.

## Geometry and cost audit

8bar structural invalidation+.25ATR,1.5ATR floor,.003 executable fraction floor; target prior96bar extreme; room>=1.25R; estimatedcost<=.15R. The prior 96-bar high/low is the actual structural target. Signals without measured target room are rejected before labels. Next-open entry, adverse gap-stop fill and stop-first ambiguous bars match the conservative labeling policy. Whole future horizons must be contiguous, matured inside their chronological test fold and before holdout.

OHLCV only; retain V1 conservative modeled fee .0004,halfspread .0001,slippage .0002 each leg,funding .0001 per8h ceil full12h horizon. Not observed user fee tier or historical execution.
The source candle schema contains OHLCV, quote volume and trade count, but no historical bid/ask, execution slippage, venue/user fee tier, funding timestamps or OI. Volume uses observed historical values; no neutral values are imputed. V1–V5 historical artifacts and costs were not modified. V2 has a separate cost version/hash without cheaper assumptions.

MFE/MAE in terminal candles remain censored bounds, not exact event-time observations. Full-horizon funding reserves stay charged even on early exits. Fees/spread/slippage use actual hypothetical entry+exit turnover over fixed decision-time structural R.

## Fixed economic admission

Five chronological test folds follow an initial seed window across the existing development range. Every fold needs >=300 samples, >=60 observed UTC days, positive gross R and a positive UTC-day-clustered two-sided 95% lower net-R bound. Pooled net R under 2x all costs must exceed zero. A pooled score cannot hide a failed fold. The normal interval is a development diagnostic: cross-day serial dependence, overlapping opportunities, cross-sectional dependence and multiple testing limit confidence.

RESEARCH_PROMISING separately requires >=1,500 pooled samples, >=300 days, positive pooled gross and net and >=4/5 positive net folds. It never grants admission.

| Hypothesis | N | Days | Gross R | Cost R | Net R | Net 2x cost R | Promising | Economic pass |
|---|---:|---:|---:|---:|---:|---:|---|---|
| CROSS_SECTIONAL_RELATIVE_STRENGTH | 20407 | 546 | -0.032447 | 0.082632 | -0.115079 | -0.197710 | False | False |
| VOLATILITY_TRANSITION_CONTINUATION | 12 | 11 | -0.346722 | 0.049054 | -0.395776 | -0.444830 | False | False |
| MULTI_TIMEFRAME_TREND_PULLBACK | 3523 | 460 | 0.050512 | 0.102384 | -0.051873 | -0.154257 | False | False |
| FLOW_CONFIRMED_DIRECTIONAL | 16550 | 545 | -0.009169 | 0.076996 | -0.086165 | -0.163160 | False | False |

## CROSS_SECTIONAL_RELATIVE_STRENGTH

Counts: `{"geometry_cost_rejected": 184585, "incomplete_or_boundary": 1004, "signals": 205996}`.

| Fold | N | Days | Gross R | Cost R | Net R | Net lower 95% R | 2x cost net R | TARGET | STOP | TIMEOUT | Profitable |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4969 | 110 | 0.059303 | 0.077242 | -0.017939 | -0.207961 | -0.095182 | 0.240692 | 0.533307 | 0.226001 | 0.404105 |
| 2 | 2873 | 110 | -0.044441 | 0.084691 | -0.129132 | -0.239226 | -0.213824 | 0.257919 | 0.575357 | 0.166725 | 0.371737 |
| 3 | 4381 | 110 | -0.116363 | 0.077149 | -0.193512 | -0.270504 | -0.270661 | 0.190824 | 0.538462 | 0.270714 | 0.336225 |
| 4 | 3996 | 109 | -0.083631 | 0.089376 | -0.173008 | -0.268982 | -0.262384 | 0.211461 | 0.566066 | 0.222472 | 0.354855 |
| 5 | 4188 | 109 | 0.003540 | 0.086912 | -0.083372 | -0.157419 | -0.170284 | 0.242120 | 0.540592 | 0.217287 | 0.378223 |

## VOLATILITY_TRANSITION_CONTINUATION

Counts: `{"geometry_cost_rejected": 751, "incomplete_or_boundary": 4, "signals": 767}`.

| Fold | N | Days | Gross R | Cost R | Net R | Net lower 95% R | 2x cost net R | TARGET | STOP | TIMEOUT | Profitable |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 1 | -0.994446 | 0.021458 | -1.015904 | N/A | -1.037363 | 0.000000 | 1.000000 | 0.000000 | 0.000000 |
| 2 | 4 | 4 | -0.074223 | 0.050883 | -0.125106 | -1.113826 | -0.175989 | 0.250000 | 0.250000 | 0.500000 | 0.250000 |
| 3 | 1 | 1 | -0.634429 | 0.014275 | -0.648704 | N/A | -0.662979 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| 4 | 2 | 2 | -0.324329 | 0.043651 | -0.367981 | -0.730148 | -0.411632 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| 5 | 4 | 3 | -0.396560 | 0.065519 | -0.462080 | -1.671926 | -0.527599 | 0.000000 | 0.500000 | 0.500000 | 0.250000 |

## MULTI_TIMEFRAME_TREND_PULLBACK

Counts: `{"geometry_cost_rejected": 16827, "incomplete_or_boundary": 71, "signals": 20421}`.

| Fold | N | Days | Gross R | Cost R | Net R | Net lower 95% R | 2x cost net R | TARGET | STOP | TIMEOUT | Profitable |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 700 | 94 | -0.027597 | 0.097499 | -0.125096 | -0.418588 | -0.222595 | 0.288571 | 0.637143 | 0.074286 | 0.347143 |
| 2 | 856 | 89 | 0.186190 | 0.104563 | 0.081627 | -0.282714 | -0.022937 | 0.389019 | 0.550234 | 0.060748 | 0.433411 |
| 3 | 772 | 89 | -0.159913 | 0.101266 | -0.261179 | -0.414298 | -0.362445 | 0.196891 | 0.628238 | 0.174870 | 0.321244 |
| 4 | 591 | 91 | 0.214125 | 0.107839 | 0.106286 | -0.125088 | -0.001553 | 0.306261 | 0.500846 | 0.192893 | 0.456853 |
| 5 | 604 | 97 | 0.057611 | 0.101050 | -0.043439 | -0.402679 | -0.144489 | 0.317881 | 0.600993 | 0.081126 | 0.370861 |

## FLOW_CONFIRMED_DIRECTIONAL

Counts: `{"geometry_cost_rejected": 146153, "incomplete_or_boundary": 809, "signals": 163512}`.

| Fold | N | Days | Gross R | Cost R | Net R | Net lower 95% R | 2x cost net R | TARGET | STOP | TIMEOUT | Profitable |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3514 | 110 | 0.018529 | 0.065565 | -0.047036 | -0.212535 | -0.112601 | 0.156517 | 0.455891 | 0.387592 | 0.411212 |
| 2 | 2508 | 108 | 0.085721 | 0.076317 | 0.009404 | -0.249539 | -0.066913 | 0.236842 | 0.452951 | 0.310207 | 0.438198 |
| 3 | 3315 | 109 | -0.103732 | 0.076125 | -0.179857 | -0.367488 | -0.255981 | 0.177979 | 0.507994 | 0.314027 | 0.365008 |
| 4 | 3920 | 109 | -0.034191 | 0.080142 | -0.114333 | -0.295577 | -0.194475 | 0.158673 | 0.468367 | 0.372959 | 0.398214 |
| 5 | 3293 | 109 | 0.013986 | 0.086842 | -0.072856 | -0.186445 | -0.159699 | 0.181294 | 0.460674 | 0.358032 | 0.407227 |

## Availability and provenance

Unavailable source assets: NONE.
Omitted observations: 5m, open_interest, actual_funding, basis, signed_trade_flow, orderbook_spread, actual_slippage.

Registry hash: `54b6fcc12706749f498cbc4c184506c9e1625c1ee638b3a3878683d5b21493ec`. Dataset manifest: `d64e5c517fd634e57ecff9441341d67ea9009072d0b184772471cd33e5903dfb`. Source commit: `b8724a54aea91c4ac429cf9d95fc8534751f7378`; source tree dirty at research run: `True`. Exact source prefix counts, bounds and hashes are in report.json.
Generator SHA-256: `0bb9b64e15c9da1523a9412785727adb386013fabd415457a952355b2206423d`. Evaluator SHA-256: `0db3889196c5fdc8456554fe16f4a9b95ee2ea6d21e10a9255011aa7394c451d`. Fresh deterministic label stream SHA-256: `65c3aa355b12aad635479b27afb0a6ba113e43e3fe1ab24442bd73fefa42917c`.
Peak sampled process RSS: 254.1 MB; elapsed: 270.7 seconds. Source data are processed in two bounded per-symbol passes; a changed source hash between passes aborts. Full fresh labels remain in the local evaluation directory with canonical candidate and label identities; no old candidate population is reused.

Reproduction requires the same locally prepared V4 metadata and native source database (both referenced by exact hashes). All fresh labels are published in `docs/research/artifacts/cati_alpha_v2_fixed/labels.jsonl.gz` using deterministic gzip (mtime zero, empty filename). `label_integrity.json` records both compressed and decompressed hashes, row counts, candidate identities and causal timestamp checks. Verify the published compressed evidence offline without price queries using `scripts/verify_cati_alpha_v2.py --artifact docs/research/artifacts/cati_alpha_v2_fixed --output <verification.json>`. Reproduce with a fresh output directory:
```powershell
& backends/venv/Scripts/python.exe scripts/evaluate_cati_alpha_v2.py --database backends/shared/shared_lib/persistence/cosmicforge.db --registry docs/research/cati_alpha_v2_registry.json --output <fresh-output-directory>
```

Holdout opened: NO. Holdout inspected: NO. Holdout price queries: 0. All SQL source close predicates end strictly before the immutable reserved boundary. Model fits: 0. Runtime eligibility: false. No library, certificate, runtime pin or execution authority follows from this generator. If all economic gates fail, model development/calibration/certification remains blocked; runtime and FX work can proceed independently.

Tests: 6 alpha V2 tests passed; consolidated CATI suite to be recorded by parent. Exact lower-level per-instrument metrics and full fold costs/rates are recorded in the JSON artifact; a sparse positive subset cannot be selected after inspection.
