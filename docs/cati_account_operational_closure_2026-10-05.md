# CATI account operational closure — 2026-10-05

Starting revision: `43178689a88bf1e45573427e82f55d1b28fa2910`. Work performed directly on `main`, against the existing local production database. No external deployment or real-money order was performed.

## Production account and credential repair

The one connected execution account is Binance DEMO `brk_c729454e6c98`, owned by `b04f9242-39b4-48aa-915b-ef2096c4a00f`. Its existing bot mapping `bot_a8117dc719fc` was retained and explicitly migrated from the historical strategy label to `cati`. Historical/deleted bots and orphaned credential records were retained.

The active encrypted credential could be recovered cryptographically with a historical key. The explicit migration transaction verified primary-key encryption, rewrapped owned credential history and the legacy mirror, superseded version 1, advanced the active pointer to version 2, and wrote an audit event. Both backends now use the same dedicated primary broker key; normal runtime legacy decryption remains disabled. Secret values and the local encrypted-record/configuration backups are excluded from Git.

The migration CLI requires an existing database and a dedicated primary key. It defaults to dry-run, does not initialize/reset the database, rolls back on any failed verification/audit write, and marks unrecoverable owned active credentials `CREDENTIAL_RECONNECT_REQUIRED` without deleting account identity or history. An explicit active credential pointer can never fall back to a different version or legacy mirror.

## Real DEMO transport evidence

The canonical production lease owner executed the user-authorized, separate `TESTNET_SMOKE` lifecycle at `https://demo-fapi.binance.com`:

| Evidence | Broker read-back |
|---|---|
| Deterministic entry identity | `CFSMOKE8d25ed7107191f77841e46f3` |
| Entry | Order `1319685905`, BUY MARKET, FILLED, 20 ADA at 0.26360 USDT |
| Native SL | Algo `1000000229261065`, SELL, close-position |
| Native TP | Algo `1000000229261075`, SELL, close-position |
| Close | Order `1319685926`, SELL MARKET, reduce-only, FILLED, 20 ADA at 0.26320 USDT |
| Actual broker fills | Entry and close both confirmed |
| Final account | Zero open positions, zero open orders, zero native protective orders |
| Final virtual balance | 426.38043893 USDT |

The smoke record is durable in `demo_transport_smoke`, separate from CATI plans, forecast evidence and strategy performance. Durable CREATE ownership precedes submission; ambiguous CREATE/CLOSE outcomes never trigger a retry. Completed replay returns the existing certification and submits no second order. Risk checks include the account-wide 2.5% cap, adaptive remaining budget, weekly/monthly limits, loss cooldown and persisted/governance kill controls. Only the DEMO Binance entry gets this explicit lease-scoped transport permit; LIVE remains closed.

The actual user-backend account HTTP probe returned 200 and the broker balance using credential version 2 and the canonical DEMO URL. A signed request for an unrelated user returned 404. These probes did not modify credentials.

## Natural CATI operation and risk

Runtime settings remain PRODUCTION, CATI LIVE, sole engine, account-scoped brokerage, live market data. DEMO order submission is enabled; LIVE submission is disabled. The account is synced and waiting for a natural frozen residual decision. `RESIDUAL_PORTFOLIO_OVERLAP` is a signal waiting condition; the smoke does not manufacture a CATI opportunity.

Account-wide risk includes broker positions, pending entries, fees, funding, manual activity and other bots. Complete cash history is read in bounded windows, with pagination/time subdivision and fail-closed incomplete history. Weekly/monthly baselines now come from complete broker cash history and durable account equity peaks, adjusting capital transfers instead of depending on an inactive legacy runner. After the smoke, weekly drawdown was 0.0028645873%, monthly drawdown 2.0376268684%, below the configured 5%/10% limits. Adaptive risk remained NORMAL; both kill controls were off. A realized smoke loss/fee remains in account risk even though it is excluded from strategy performance.

Account auto-trading authorization is bound to user, account, bot and environment. Bot start records authorization; pause removes new-entry authorization. Existing active DEMO assignments are admitted for the proving phase. LIVE requires explicit matching authorization. Multiple active owners block execution; no account can silently inherit another user's mapping or switch environment.

Risk/kill/unknown-outcome reasons take precedence over signal waiting. Recovery and broker read-back run before new-entry eligibility. Public health reports fresh account risk, reconciliation and market-data status, and degrades when evidence is stale; it publishes no account identifiers. The runtime revision is captured at process startup.

## Broker capability status

| Broker | DEMO | LIVE |
|---|---|---|
| Binance | Real local lifecycle certified on the connected DEMO account | Contracts and guards tested; no connected LIVE account or real-money authorization; system gate remains off |
| Bybit | Native account/income pagination, deterministic entry lookup, native conditional close-only protection, exact geometry read-back and UNKNOWN recovery implemented and tested | Uncertified broker profiles remain fail-closed until external account certification; no connected credentials |
| BingX | Canonical VST URL, documented entry ID casing, broker fill normalization, income accounting, one-way leverage guard and native close-position SL/TP recovery implemented and tested | Uncertified broker profiles remain fail-closed until external account certification; no connected credentials |
| Other brokers | Unsupported/unvalidated execution remains blocked with explicit capability status | No crypto-perpetual fallback into FX/CFD/MT/IBKR adapters |

Bybit and BingX transport certification cannot be claimed without their account credentials. No external certification flag was promoted merely because mocks passed. BingX conditionals do not support client IDs; local durable ownership plus broker ID/geometry resolves them, and absent evidence stays UNKNOWN.

Implementation references: [Bybit create order](https://bybit-exchange.github.io/docs/v5/order/create-order), [Bybit transaction log](https://bybit-exchange.github.io/docs/v5/account/transaction-log), [Bybit transaction types](https://bybit-exchange.github.io/docs/v5/enum), [BingX official trade API reference](https://raw.githubusercontent.com/BingX-API/api-ai-skills/main/skills/swap-trade/api-reference.md), [BingX official account API reference](https://raw.githubusercontent.com/BingX-API/api-ai-skills/main/skills/swap-account/api-reference.md).

## Account connection flow

Production UI exposes LIVE and DEMO explicitly. Draft creation stores the selected canonical environment; credential submission sends it as account metadata, not encrypted authority. Changes to established account environments are refused. Resume/reconnect preserves account ID, mappings and environment, and creates/activates a new credential version through the exact frontend `/credentials` and `/validate` endpoints. Activation clears stale reconnect errors. Ownership and a mismatching credential URL/environment fail before mutation; draft environment selection and credential writes share one transaction.

## Validation evidence

Final acceptance suite: **100 passed**, including production DEMO/LIVE gates, native venue contracts, account/user isolation, unknown CREATE recovery, protection replay, risk controls, stale public health and the isolated observability check. Additional risk-reason-priority checks: 50 passed. Venue contracts: 35 passed. Health/account checks: 27 passed. Shared credential/account HTTP/migration tests: 32 passed. User-backend suite: 75 passed using an isolated test database and synthetic encryption key. Frontend build passed, and all 17 frontend tests passed.

The broad backend regression is not a green certification. The final full run reported **4537 passed, 97 failed, 11 errors, one skip and four passing subtests** in 919.13 seconds. An unchanged checkout of the starting revision reproduced 94 failures and all 11 errors from the failing-node set; the executor contract inventory failure is now fixed. Of the remaining 97 failures, 93 were reproduced at baseline. Three additional failures require local `.env` paper-mode settings that conflict with this task's production profile; the remaining observability failure passed on isolated rerun. Existing tests were retained. Updated venue fixtures assert documented margin fields and `clientOrderId` casing; no risk or authority assertion was weakened. Subsequent focused checks validate the final changes made while the broad run was in progress.

Local evidence is under ignored `logs/closure_*`, with baseline comparison under ignored `tmp_shadow/closure_baseline`. Database and encrypted/configuration backups remain local. The complete baseline failure inventory can be reproduced using the same test nodes at the starting revision; this report does not represent historical paper/legacy test contracts as current production support.
