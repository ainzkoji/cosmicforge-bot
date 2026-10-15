# Step 4.6: BingX environment certification

Status on 2026-10-09: **BLOCKED** for everything that needs BingX itself. This document records what the code
and the configuration of this repository establish, what they cannot establish, and the check an operator must
run on a connected demo account before anything about BingX may be called certified.

No call was made to BingX in this work: not to the live host, not to the demo host, not to a public endpoint.
There was no network access. For the same reason the current BingX documentation was not read; where this
document says "documentation as remembered", it means the author's prior knowledge of the published API, which a
person must check against the current documentation before relying on it. No account, live or demo, was assumed
to exist, and no order was sent. Trading stays shadow only.

## 1. Verdict by item of the assignment

| Item (Step 4.6) | Result |
|---|---|
| 1. Verify current BingX documentation | NOT DONE. No network access. To be done by a person; section 5 lists what to look up |
| 2. Does an official demo or test environment exist | The repository is configured for one (the VST host). Its existence and behaviour were not confirmed in this work |
| 3. Which perpetual order and protection features it supports | NOT ESTABLISHED. Section 3 lists what the code sends; whether the VST host accepts it is unknown |
| 4. Record the gaps | Done: sections 3 and 4 |
| 5. Complete synthetic contract coverage | Done: the BingX adapter passes the contract suite over a fake HTTP layer (section 2.4) |
| 6. Certify actual venue reads when access is available | BLOCKED: no access was available. Procedure in section 5 |
| 7. Prepare safe operator certification procedures | Done: section 5 |

BingX is not certified. A green synthetic suite is not a certification, and nothing here should be read as one.

## 2. What the code and the configuration establish

### 2.1 Two environments, two hosts, one code path

| | LIVE | DEMO |
|---|---|---|
| Host (`shared_lib/broker/environment.py`) | `https://open-api.bingx.com` | `https://open-api-vst.bingx.com` ("VST") |
| Wallet and transfer host | `https://open-api.bingx.com` | none configured: internal transfers are unavailable on demo |
| Raw client | `BingXClient(..., base_url=<host>)` | the same class, the other host |
| Declared capability profile (`shared_lib/broker/capabilities.py`) | every execution capability `UNVALIDATED` | the same |
| May a bot execute (`execution_readiness`) | no: `BROKER_EXECUTION_UNVALIDATED_FOR_LIVE` | yes, as far as this gate is concerned |
| Order submission flag (default) | `LIVE_ORDER_SUBMISSION_ENABLED = False` | `DEMO_ORDER_SUBMISSION_ENABLED = False` |

- The raw client has no branch that depends on the environment. It sends the same paths and the same fields to
  both hosts. The code therefore assumes that the VST host offers the same swap V2 API as the live host. That
  assumption has not been tested.
- The Step 4.1 adapter refuses to be built when the client's host is not the canonical host of the stated
  environment (`factory.adapter_for`). A DEMO adapter cannot wrap a client on the live host, and the reverse.
- With the production profile and the default flags, every mutation of the adapter is refused before it reaches
  the HTTP layer (`PERMISSION_DENIED`), on both environments.

### 2.2 Product coverage recorded in the repository

`shared_lib/broker/capabilities.py` records, as evidence dated 2026-09-26, that the public contract list
(`/openApi/swap/v2/quote/contracts`) of both the live host and the VST host lists crypto perpetuals and, under
the `NCFX`, `NCSK`, `NCCO` and `NCSI` symbol prefixes, FX, stock, commodity and index perpetuals, each with its
own `status` and `apiStateOpen`. That observation was made before this work and was not repeated here. No
recorded BingX answer is stored in the repository as a fixture. One existing test calls its fill row a "real
fill payload" (`tests/trading_intelligence/test_perpetual_operational_contract.py`) without naming a source; the
fake HTTP layer of the new tests follows that row, and it remains unverified.

Whether an instrument takes orders through the API is decided per instrument: `status == 1` and `apiStateOpen`
true. The adapter reports any other state as `NOT_TRADING`.

### 2.3 What is known to be different or missing on demo

- No wallet host is configured for demo, so internal transfers and wallet balances cannot be read there.
- API-key permissions cannot be inspected on BingX at all (`PERMISSION_INSPECTION` is `UNSUPPORTED`). Whether a
  key can withdraw is not observable through the API, on either environment.
- The VST account is funded with virtual funds. The adapter reports the margin asset the venue names in the
  balance answer and falls back to the raw client's label (`USDT`) when the venue names none.
- Everything else (which order types, which fields, which limits, which error codes the VST host accepts or
  returns, and whether its instrument rules equal the live ones) is not established.

### 2.4 Synthetic coverage

The BingX adapter (`app/exchange/platform_adapter/bingx.py`) wraps the real `BingXClient` and passes the contract
suite and its own wire tests with the HTTP library replaced by a fake:

```
../venv/Scripts/python.exe -m pytest tests/exchange/test_platform_adapter_contract.py tests/exchange/test_platform_adapter_venues.py -q
```

This proves that requests are built in the dialect the raw client implements and that answers of the shapes the
raw client reads are translated into the contract. Every BingX answer in those tests is rendered from the keys
the raw client's parser reads. None was compared with an answer from BingX.

## 3. Order and protection features: what the code sends, and what is not established

| Feature | What the adapter sends | Established at the venue |
|---|---|---|
| Market and limit entry, one-way | `POST /openApi/swap/v2/trade/order` with `positionSide=BOTH`, `type`, `quantity` in coin units, `price` for a limit, `clientOrderId` | no |
| Reduce-only close | the same, with `reduceOnly=true` | no |
| Client order id | lower case, at most 40 characters; any other id travels as a 32-character hash | no: the limit and the case folding come from the raw client |
| Order lookup | `GET /openApi/swap/v2/trade/order` by `orderId` or `clientOrderId` | no |
| Open orders | `GET /openApi/swap/v2/trade/openOrders` | no |
| Cancel | `DELETE /openApi/swap/v2/trade/order` by `orderId` | no |
| Stop-loss | `STOP_MARKET` with `stopPrice`, `quantity`, `reduceOnly=true`, `positionSide=BOTH`, `workingType` | no |
| Take-profit | `TAKE_PROFIT_MARKET`, the same fields | no |
| Stop bound to the whole position without a quantity | not sent (`UNSUPPORTED`) | no: the repository always sends a quantity |
| Client order id on a trigger order | not sent | no: the repository's protection code states that trigger orders take none |
| Change a resting stop | not sent (`UNSUPPORTED`); the caller cancels, then creates | no |
| Two stops resting together | refused by the adapter (`STOP_ALREADY_EXISTS`) | no: the raw client's `update_protection` assumes it is possible |
| Leverage | `POST /openApi/swap/v2/trade/leverage`, once for `LONG` and once for `SHORT` | no: whether a one-way account wants `BOTH` is unknown |
| Position mode | `GET /openApi/swap/v1/positionSide/dual` | no: the type of `dualSidePosition` (boolean or text) is unknown |
| Fills | `GET /openApi/swap/v2/trade/allFillOrders` with `tradingUnit=COIN` | no |
| Fee rates | `GET /openApi/swap/v2/user/commissionRate` | no |
| Funding | `GET /openApi/swap/v2/quote/premiumIndex` | no |
| User stream | not available (`UNSUPPORTED`) | no stream host or key call exists in the repository |

Adapter capabilities for BingX, both environments: `close_position_stop=False`, `quantity_stop=True`,
`stop_amend_in_place=False`, `multiple_stops_per_position=False`, `stop_client_order_id=False`,
`hedge_mode=False`, `user_stream=False`. Where the truth could not be established the value is the conservative
one.

## 4. Gaps

1. No BingX answer of any kind has been observed by this code base in a form that is stored. Every parser is
   untested against the venue.
2. No BingX error code is handled in the repository. The adapter's table (11 codes and 3 "server failed" codes)
   is the published error list as remembered. The codes for a duplicate client id, a rejected reduce-only order,
   an invalid quantity or price, a notional below the minimum and a trigger that is already reached are not
   mapped at all: they arrive as `UNMAPPED_VENUE_ERROR` with the venue's code.
3. The adapter looks an order up by client id before it creates one. If BingX reports "no such order" neither
   with code 80016 nor with an empty order record, that lookup fails and the adapter places nothing. This fails
   closed, and it means the adapter may be unable to place any order until the first operator check.
4. Protective stops: the form, the absence of a client id, whether a second stop may rest, and what happens to a
   stop when its position closes are all unverified.
5. A stop is identified only by its venue order id. After a restart the adapter cannot find it by the caller's
   id; the caller must have stored the venue order id.
6. Rate limits: the raw client discards response headers, so the adapter never knows a `Retry-After` or the used
   budget on BingX. The HTTP status BingX uses for a rate limit or a ban is unverified.
7. Balance: the raw client reads unrealised profit under `unrealisedPNL`; the documentation as remembered prints
   `unrealizedProfit`. The adapter accepts either and refuses an answer with neither.
8. Fills: the raw client turns an unreported fee into 0, so the adapter reports a fee of exactly 0 as unknown.
   The fill id key (`tradeId`) and the time format are unverified.
9. Key permissions are not inspectable. The rule "never trade with a key that can withdraw" cannot be enforced
   by observation on BingX; it rests on how the user created the key.
10. Whether the VST host lists the same instruments with the same rules as the live host at the time of a run
    is unknown. Rules must be read per environment, never copied from one to the other.

## 5. Operator check on a connected demo account

### 5.1 Before anything is sent

All of these must hold. If one does not, stop.

- The project owner has authorised the check in writing: the account, the environment (DEMO, the VST host), the
  symbol, and the maximum size.
- The account is a BingX demo account that its user connected through the normal broker connection flow as
  DEMO. Its credentials are resolved by `shared_lib.broker.resolver`; nobody types or prints a key.
- The account is used for nothing else, holds no position and has no open order on the chosen symbol.
- The API key was created without withdrawal permission. This cannot be read back from BingX, so the operator
  records how the key was created.
- The check runs on the demo host only. Nothing in this procedure applies to a live account. A live-size or
  live-host check needs a separate, explicit approval that names the account, the exposure and the plan.
- The current BingX documentation has been read for the endpoints of section 3, and any difference from that
  table has been written down first.

### 5.2 Step A: reads only

Run the existing read-only harness. It places nothing:

```
python scripts/validate_demo_venue.py --account-id <brk_...> --user-id <uuid> --symbol BTCUSDT --out-dir <a directory outside docs/research>
```

Pass `--out-dir`. The script's default writes under `docs/research/`, whose content is hash-pinned.

Then, with an adapter built by `factory.adapter_for(client, "bingx", "DEMO")` on the same account (through the
Step 4.2 certification runner or an operator session; this step ships no command of its own), call each read
method once and keep the answers. Record for each one:

| Read | What to record |
|---|---|
| `server_time` | the offset from the local clock |
| `instrument_rules` | the number of instruments; for the chosen symbol the tick, step, minimum quantity, minimum notional and state; the raw record's keys |
| `balance` | the keys of the balance record; the asset name; under which key unrealised profit is reported |
| `positions` | with no position: whether `data` is an empty list or null |
| `open_orders` | with no order: whether `orders` is an empty list or null |
| `order` for an id that does not exist | the venue code, or an empty order record |
| `fills` | with no fill: the shape of the empty answer |
| `fee_rates`, `funding` | the keys present; whether the funding interval is reported |
| position mode | the value and the type of `dualSidePosition` |
| any answer | the response headers that describe a rate limit |

Step A already settles gaps 3 and 7 and most of gap 1. If the lookup of an unknown order fails with
`UNMAPPED_VENUE_ERROR`, stop: no order can be placed through the adapter until that code is mapped.

### 5.3 Step B: minimum-size demo orders

Only after step A is clean and its findings are written down. Use the smallest quantity the instrument rules
allow, on one symbol. Each line is one action and what to record. Stop at the first surprise and flatten.

1. Set leverage to 1. Record the answers for both sides and what a second identical call returns.
2. Place a resting limit order far from the market. Record the create answer (does it carry a state), the
   lookup by client id, the open-order row, then cancel it and record the cancel answer and the final state.
3. Send the same client id again after the cancel. Record whether the venue refuses it and with which code.
4. Place a second resting limit order with a 40-character client id. Read it back and record the id exactly as
   the venue stores it (the raw client always sends lower case). Cancel it.
5. Place a minimum market entry. Record the create answer, the order state, the fill rows (id key, fee sign, fee
   asset, time format) and the position row (`positionSide`, the sign of `positionAmt`, margin mode).
6. Place a `STOP_MARKET` with `reduceOnly`, the position's quantity and `positionSide=BOTH`, well below the
   market. Record whether it is accepted, the answer, and its open-order row (`reduceOnly`, `closePosition`,
   `stopPrice`, `origQty`, whether a client id appears).
7. Place a second stop at another trigger. Record whether the venue accepts it. Cancel the second one.
8. Try a stop above the market for a long position. Record the refusal and its code.
9. Try a reduce-only order larger than the position, an order off the quantity step, an order below the minimum
   notional and an order on a symbol that does not exist. Record each code.
10. Close the position with a reduce-only market order. Record what happens to the resting stop: still open,
    cancelled, or expired. Cancel it if it still rests.
11. Read positions, open orders and balance. All must show the starting state: flat, no orders.

Do not test rate limits by provoking them. If a 429 or a ban arrives by itself, record the status, the body and
the headers, and stop for the time stated.

### 5.4 If something goes wrong

- Cancel every open order on the symbol and close the position with a reduce-only market order. Confirm flat by
  reading positions and open orders. The raw client's `cancel_all` and `close_position_market` do this.
- Keep every request and answer of the run, without keys or signatures.
- Do not repeat a request whose outcome is unknown. Look the order up first.

### 5.5 What the check must deliver

- A dated record of every answer above, with the commit it was run on.
- A list of every difference from sections 3 and 4.
- For each UNVERIFIED row of the BingX tables in `VENUE_ADAPTER_CONTRACT.md` (sections 8 and 9.4), one of:
  confirmed, wrong (with the observed value), or still unknown.

Only then can the adapter's BingX table, its capability flags and its parsers be corrected and the Step 4.2
certification suite be run for (`bingx`, `DEMO`). Until that run is green and current, BingX stays not
selectable for certified execution, on demo as well as on live.
