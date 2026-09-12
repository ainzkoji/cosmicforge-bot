# Connected-broker market universe checkpoint

Evidence date: 2026-09-12/13. Runtime commit before this change: `23d9e764260a84df3440d67af5c52c0b9cea1ecd`.

## Symbol authority audit

| Source | Precedence | Runtime consumer | Previous/current value |
|---|---:|---|---|
| `bot_instances.universe_mode` | 1 | `EffectiveBotPolicy`, `BotRunContext`, `PaperRunner` | `bot_a8117dc719fc`: `BROKER` |
| Connected `broker_accounts` row | 2 | broker client and `UniverseAdapter` | `brk_c729454e6c98`, Binance, demo, connected |
| Broker instrument metadata | 3 | `InstrumentRegistry`, `BinanceUsdmUniverseAdapter`, `UniverseEngine` | 739 instruments observed |
| Bot `symbols_json` | 1 only in `ALLOWLIST` mode | effective policy and runner | active bot: `[]`; old value retained in migration evidence |
| `.env` `TRADE_SYMBOLS` / `MAX_SYMBOLS` | legacy only | contextless development/replay runner | `BTCUSDT,ETHUSDT` / `2`; no authority over a context-backed bot |
| Legacy shadow universe settings | shadow only | `DynamicUniverseShadowRecorder` | separate diagnostics; no executor authority |

The old two-symbol restriction came from `deploy_auto_pilot`, which copied
`settings.TRADE_SYMBOLS` into every crypto Auto Pilot row. The API only allowed
`symbol_universe_mode="auto"`, so `bot_a8117dc719fc` could not have expressed a
custom list through that flow. The additive migration recorded the prior value
as `["BTCUSDT", "ETHUSDT"]` with reason
`AUTO_PILOT_CRYPTO_DEPLOY_COPIED_TRADE_SYMBOLS`, set the bot to `BROKER`, and
cleared `symbols_json`. It did not recreate the bot or alter mode, account,
capital, strategy, timeframe, or threshold policy.

The effective path is now:

`BotInstance -> EffectiveBotPolicy -> BotRunContext -> MultiBotRunner -> PaperRunner -> connected client -> InstrumentRegistry/UniverseAdapter -> UniverseEngine -> active candidates -> closed-candle snapshot -> Master Ensemble -> adaptive threshold -> risk -> capital -> executor`.

Open positions are prepended to the managed list on every cycle. Ranking only
controls new-entry candidates. Discovery failure therefore fails closed for new
entries while position reconciliation and protection continue.

## Eligibility and quality

The Binance USD-M adapter maps venue metadata into provider-neutral contracts.
It excludes non-perpetual products, non-trading contracts, incompatible quote
or margin assets, unsupported underlying types, invalid tick/step/minimum
quantity data, insufficient listing age, stale/invalid prices, and minimum
orders beyond the bot's maximum position notional. The quality stage uses only
available metrics: 24-hour quote volume, trade count, last price, and batched
bid/ask spread. Open interest, depth, volatility, and price continuity remain
explicitly unavailable at this stage.

The observed refresh produced:

- discovered: 739
- hard eligible: 509
- quality ranked: 173
- active: 100
- metadata/stats requests: one metadata call and two batched ticker calls
- reported request weight after refresh: 5 of 6000
- rate-limit errors: 0

## Performance evidence

This is based on the first live **demo/testnet** broker-universe scan for run
`3803cda29dc94291a869a88f55d3d07d`. It placed no mainnet orders. The table's
25/50/100 rows are observed prefixes of the same run. Larger rows are projections
from the observed 100-symbol completion rate and were not sent to the venue.

| Requested active size | Full-strategy markets | Completion time | Base kline calls | Status |
|---:|---:|---:|---:|---|
| 25 | 25 | 76.991 s | 75 | observed |
| 50 | 50 | 161.594 s | 150 | observed |
| 100 | 100 | 340.897 s | 300 | observed |
| 200 | 173 quality-ranked | about 590 s | 519 | projected/capped by current quality set |
| all hard eligible | 509 | about 1,735 s | 1,527 | projected; unsafe for one 15m cadence |

Each newly claimed 15m candle currently requires one primary 15m, one 4h, and
one auxiliary 5m candle request. The median end-to-end interval between first
decisions for consecutive symbols was 2.511 s and p95 was 9.539 s; this includes
network, strategy, and persistence time. Pure data-fetch latency is **UNKNOWN**
because it is not timed separately. The 100-symbol scan completed in 14 bounded
management cycles. Once candles were claimed, cycles returned to roughly the
management heartbeat with candidate candle fetches suppressed.

At observation time the backend process used about 132 MiB working set and 378
MiB private memory. Its cumulative CPU was 84.5 seconds since startup; this is
process-wide and cannot be attributed solely to the universe scan.

`UNIVERSE_ACTIVE_LIMIT=100` is retained as the largest directly observed safe
default. It completed the scan in 5.7 minutes, leaving substantial margin before
the next 15m candle. Raising it to 173 is plausible from the projection but needs
a dedicated observed run before becoming the default. Scanning all 509 hard-
eligible contracts would overrun the candle cadence and is rejected as a default.

## Runtime acceptance evidence

The active demo runtime recorded 100 unique symbol evaluations, including 98
outside BTCUSDT/ETHUSDT. Thirteen non-core opportunities passed the adaptive
threshold. Capital enforcement then rejected entry because the 120 USDT bot
budget was already committed. This proves universe breadth does not bypass the
threshold engine or capital ledger. No rate-limit error was recorded.

The first observed non-core evaluation was `BCHUSDT`. `STORJUSDT` was the first
observed non-core threshold pass; execution stopped at the capital gate. A
non-BTC/ETH broker order was therefore not observed and should not be forced.

## Frontend contract

The API contract accepts:

- `symbol_universe_mode: "auto"` with no symbols: connected-broker universe.
- `symbol_universe_mode: "custom"` with a non-empty `symbols` list: explicit allowlist.

The current Auto Pilot screen continues to submit `auto`. Its client type now
also represents the custom contract so a later focused UI change can expose the
selector without another API change.
