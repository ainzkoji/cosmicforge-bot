# FX, demo validation, and pre-holdout status — 2026-09-29

This is a progress record, not a dataset freeze or a venue certification. The
holdout remains closed. No live-fund order or transfer was submitted.

## Track 1: FX reference data

The existing resumable Dukascopy 1m BID/ASK acquisition is running against
`data/research/fx_reference_dukascopy.db`. Do not start another writer against
that database. A read-only `minute --plan` snapshot on 2026-09-29 found 50
frozen-universe pairs, 652 expected market days per pair, and 13,720 remaining
pair/day periods (an estimate of 27,440 provider requests). At that snapshot,
the 1m ingest log had 37,642 FETCHED, 6,608 EMPTY, 6 NO_FILE, and no FAILED
periods. The 1m table had 21,921,849 rows across 29 pairs. These numbers are
interim and will change as acquisition continues.

The final sequence is: finish all 1m pair/day periods; retry FAILED or
quarantined periods; derive 5m, 15m, and 4h from complete 1m BID/ASK; run
full-history scale, bid/ask, gap, and cross-rate QA; regenerate
`docs/research/coverage/fx_dukascopy_v1.coverage.json`; only then freeze the
final dataset identity. Existing 15m rows for three pairs are preliminary, not
evidence that the entire derived dataset is complete.

## Track 2: external venue validation

The canonical Bybit DEMO trading host is now `https://api-demo.bybit.com`,
matching Bybit's Demo Trading keys. It was previously mapped to Testnet, a
different account environment. The user-backend's connection test now also
uses the canonical host for Bybit and BingX, rather than deriving a separate
host from a `testnet` flag or accepting a caller-provided URL. Bybit's
published demo API list does not include the internal-transfer endpoint, so
this repo now reports that feature unavailable on Bybit DEMO and refuses its
adapter call. BingX DEMO remains
`https://open-api-vst.bingx.com`; its demo wallet transfer remains unavailable
in the current verified contract.

Unauthenticated public preflight returned HTTP 200 and venue code 0 for
instrument/contract metadata, order book, and funding/ticker endpoints on both
demo hosts. This proves only public endpoint reachability. Account permissions,
fee tier, order submit, fills, positions, protection, reconciliation, and
private transfer cannot be validated without connected demo credentials.
Both venues therefore remain `EXTERNAL_VALIDATION_REQUIRED`.
Targeted broker routing, transfer, activation, and economics tests passed
(133 bot/shared tests and 10 user-backend tests); these are local contract
checks, not authenticated venue evidence.

To unblock Bybit: sign in to a Bybit account, switch to **Demo Trading**, and
create an API key from that demo account. Connect the key and secret in this
app as a Bybit **DEMO** broker account, with read/account, orders/positions,
and trade permissions; do not enable withdrawal. Ensure demo USDT is present.
Do not use a Bybit Testnet key: Bybit documents it as a separate host and
credential environment. The app must validate the key and the account's
reported mode before any demo order. Demo internal transfer remains skipped
unless Bybit publishes and an authenticated probe verifies a supported API.

To unblock BingX: enable **Perpetual Futures Demo Trading / VST**, ensure VST
is available, and connect an API key and secret through this app as a BingX
**DEMO** broker account. The account/key must be permitted to read futures
account, order, fill, and position data and submit futures orders on the VST
host. Do not enable withdrawal. BingX key permissions are not API-inspectable
through the current integration, so internal transfer stays blocked. A
successful public quote or test-order request is not fill/reconciliation
evidence; use a real VST demo order for that validation.

Use only the app's credential connection flow. Do not paste API secrets into
this report, command lines, logs, or chat.

## Track 3: certification

No final FX dataset freeze exists yet, and the local certification research
database is absent. The FULL/MEDIUM pre-holdout run has **not** started. After
the FX dataset genuinely freezes, run the canonical certification plan and
FULL/MEDIUM pre-holdout stages, generate artifacts, and assess gates. Do not
pass `-OpenHoldout` or `--open-holdout`, create a holdout authorization event,
or inspect holdout data without explicit user authorization.

Sources: [Bybit Demo Trading API](https://bybit-exchange.github.io/docs/v5/demo),
[BingX VST API hosts](https://github.com/BingX-API/api-ai-skills/blob/main/skills/references/base-urls.md),
[BingX VST trading API](https://github.com/BingX-API/api-ai-skills/blob/main/skills/swap-trade/SKILL.md).
