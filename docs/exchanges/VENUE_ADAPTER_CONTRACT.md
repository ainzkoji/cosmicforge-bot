# Step 4.1: one venue adapter contract for Binance, Bybit and BingX

Status on 2026-10-09: implemented and tested on Windows against a fake HTTP layer. Not run on Linux. No exchange
was called, on any host, live or demo. The venues' online documentation was not consulted in this work: there
was no network access. Trading stays shadow only; no flag, limit, governance phase or environment setting was
changed.

| Part | File |
|---|---|
| The contract (protocol, values, error codes) | `backends/bot-backend/app/exchange/platform_adapter/contract.py` |
| Factory and the shared translation | `backends/bot-backend/app/exchange/platform_adapter/factory.py` |
| Adapters | `.../platform_adapter/binance.py`, `bybit.py`, `bingx.py` |
| In-memory venue for tests | `.../platform_adapter/fake.py` |
| Contract suite (six subjects) | `backends/bot-backend/tests/exchange/test_platform_adapter_contract.py` |
| Per-venue wire tests | `backends/bot-backend/tests/exchange/test_platform_adapter_venues.py` |
| Fake venue tests | `backends/bot-backend/tests/exchange/test_fake_venue.py` |
| Fake HTTP and test rig | `backends/bot-backend/tests/exchange/_platform_adapter_wire.py` |

Run from `backends/bot-backend`:

```
../venv/Scripts/python.exe -m pytest tests/exchange/test_platform_adapter_contract.py tests/exchange/test_platform_adapter_venues.py tests/exchange/test_fake_venue.py -q
```

Result on 2026-10-09 (Windows, Python 3.12): 434 passed (274 contract, 124 per-venue, 36 fake venue).

## 1. What a green run proves, and what it does not

The three adapters are tested around their real raw client classes. The raw client's HTTP library is replaced by
a fake that renders venue answers from the keys the raw client's parser reads. Behind that fake is the same
in-memory venue that is also tested on its own.

A green run proves:

- every method of the protocol is translated into each venue's request dialect and back, through the real raw
  client code (request building, status handling, parsers);
- the same 38 contract tests (45 cases) hold for each of the three adapters and for the fake venue in each of its
  three profiles; 4 further tests check the values of the contract itself;
- failures become the stated contract errors, and no credential reaches a `repr`, an error or a returned value.
  Venue text in an error message is redacted, and any long run that mixes letters and digits is masked, because
  the adapter holds no credential it could search for.

A green run does not prove:

- that a venue answers with the shapes the fake renders. Two shapes were recorded from a venue (Binance
  `exchangeInfo` symbol record and `premiumIndex`, from the public demo host, already in the repository).
  Every other shape is derived from what the raw client reads and has not been compared with a venue answer;
- that a venue returns a given error code for a given refusal;
- any venue behaviour: fills, triggers, margin, rate limits. The behaviour behind the fake HTTP is the fake's.

## 2. The contract in brief

`VenueAdapter` is one connected customer account on one venue and one environment. There is no platform-owned
trading account.

- Symbols are canonical (`BTCUSDT`). Quantities are base units. Sides are `BUY`/`SELL` for orders and
  `LONG`/`SHORT`/`FLAT` for positions.
- Every mutation carries the caller's client order id. A repeat of the same id returns the existing order or
  raises `DUPLICATE_CLIENT_ORDER_ID`; it never creates a second order.
- Every failure is an `AdapterError` with a code from `ERROR_CODES`. `uncertain=True` (only with
  `NETWORK_UNCERTAIN`) means the venue may have acted: look the order up before doing anything else.
- A value the venue did not report is `None`. An order state the adapter cannot map is `UNKNOWN`, which is
  neither open nor final.
- An adapter never retries a mutation, never sleeps, and never sends anything again after HTTP 418 or 429.

Read methods: `capabilities`, `server_time`, `instrument_rules`, `balance`, `positions`, `open_orders`, `order`,
`fills`, `fee_rates`, `funding`, `rate_limit`, `user_stream`. Mutations: `set_leverage`, `place_order`,
`cancel_order`, `amend_stop`.

### Additions made to `contract.py` in this step

Nothing was removed or renamed.

- Error code `STOP_ALREADY_EXISTS`, appended to `ERROR_CODES`: a protective stop of this kind already rests and
  the venue holds only one; the new stop was not created and the old one still rests.
- `AdapterCapabilities.quantity_stop` (default `False`): a stop may protect part of a position.
- `AdapterCapabilities.stop_client_order_id` (default `False`): the venue stores the caller's client order id
  on a protective stop.
- `venue_client_order_id(client_order_id, *, max_length, lowercase=False)`, with the constants
  `CLIENT_ORDER_ID_ALPHABET` and `PORTABLE_CLIENT_ORDER_ID_LENGTH`.

## 3. How an adapter is built

`factory.adapter_for(client, venue, environment, *, clock=None)` takes a raw client that the connection resolver
has already built for one connected account, and that account's environment (`LIVE` or `DEMO`).

- The adapter never reads a credential. Signing stays inside the raw client. A test wraps the raw client in a
  proxy that fails on any access to a key or a secret and runs a full flow through it.
- The environment is checked: the client's host must equal `shared_lib.broker.environment.resolve_base_url` for
  that venue and environment, otherwise no adapter is built. A `DEMO` adapter cannot wrap a client on the live
  host.
- The factory never returns the fake venue.
- The platform's gates are untouched. With the production profile and the order-submission flags at their
  defaults, every adapter mutation ends in `PERMISSION_DENIED` and nothing reaches the HTTP layer (tested for
  the three venues).

## 4. What each method calls

"Transport" means the raw client's own signed request method (`_signed_get/_post/_delete`, `_request_v5`,
`_pages`, `_request`). It is used where the raw client's convenience method loses information the contract needs
(the venue's verbatim status, a failure code, the difference between "unreported" and zero).

| Method | Binance USD-M | Bybit V5 linear | BingX swap V2 |
|---|---|---|---|
| `server_time` | `server_time()` | `server_time()` | `server_time()` |
| `instrument_rules` | `exchange_info()`, parsed by `parse_binance_symbol` | `_instruments_page` across cursors, `parse_bybit_instrument` | transport `GET /quote/contracts`, `parse_bingx_contract` |
| `balance` | `account()` | transport `GET /v5/account/wallet-balance` (UNIFIED, then CONTRACT) | transport `GET /user/balance` |
| `positions` | `position_risk()` | `_pages` `GET /v5/position/list` | transport `GET /user/positions`; `GET /positionSide/dual` when the rows do not state the mode |
| `open_orders` | `open_orders()` and `get_algo_orders()` | `_pages` `GET /v5/order/realtime` (three filters) and the position list | transport `GET /trade/openOrders` |
| `order` | `get_order`, `get_order_by_client_order_id`, then the open conditional orders | transport `GET /v5/order/realtime`, then `/v5/order/history`; the position list for a position's stop | transport `GET /trade/order` |
| `fills` | `user_trades()` | `_pages` `GET /v5/execution/list` | `user_trades()` |
| `fee_rates` | transport `GET /fapi/v1/commissionRate` | `get_trading_fee_rates()` | `get_trading_fee_rates()` |
| `funding` | `get_funding()` | transport `GET /v5/market/tickers` | `get_funding()` |
| `rate_limit` | local: the used weight and the back-off the raw client kept | local: nothing is known | local: nothing is known |
| `user_stream` | `UNSUPPORTED` | `UNSUPPORTED` | `UNSUPPORTED` |
| `set_leverage` | `set_leverage()` | transport `POST /v5/position/set-leverage` | `set_leverage()` (one request per side) |
| `place_order`, entry | `place_market_order()`; LIMIT by transport `POST /fapi/v1/order` | transport `POST /v5/order/create` | transport `POST /trade/order` |
| `place_order`, stop | transport `POST /fapi/v1/algoOrder` (`closePosition`) | transport `POST /v5/position/trading-stop` (`tpslMode=Full`) | transport `POST /trade/order` (`STOP_MARKET`, quantity, `reduceOnly`) |
| `cancel_order` | transport `DELETE /fapi/v1/order` or `DELETE /fapi/v1/algoOrder` | transport `POST /v5/order/cancel`; a position's stop is cleared with `trading-stop` and `"0"` | transport `DELETE /trade/order` |
| `amend_stop` | `UNSUPPORTED` | transport `POST /v5/position/trading-stop` | `UNSUPPORTED` |

Request shapes that this repository did not already send, taken from the venues' documentation as remembered and
not exercised against a venue:

- Binance: a LIMIT order (`price`, `timeInForce=GTC`); `openAlgoOrders` without `symbol`; `workingType=MARK_PRICE`.
- Bybit: `slTriggerBy`/`tpTriggerBy` = `MarkPrice`; clearing a stop with `stopLoss="0"`.
- BingX: `workingType=MARK_PRICE`.

`OrderRequest.working_type` defaults to `MARK_PRICE`, so the default path of a stop uses one of these values on
every venue. The repository itself only ever sent the last-price basis.

## 5. Symbols, quantities, states

- Symbols. Binance and Bybit use the canonical form. BingX uses `BTC-USDT`; the adapter learns the mapping from
  the venue's contract list and otherwise uses the raw client's own rule. A canonical name claimed by two venue
  symbols is left out of `instrument_rules`.
- Quantities. All three venues trade these contracts in base units; `contract_size` is 1.0 for every instrument
  the repository's parsers produce. The adapters send the quantity they are given, as plain decimal text, and do
  not round it. A quantity off the step is the venue's to refuse.
- Order states.

| Venue state | Contract state |
|---|---|
| Binance `NEW`, `PARTIALLY_FILLED`, `FILLED`, `CANCELED`, `REJECTED`, `EXPIRED` | the same |
| Binance `EXPIRED_IN_MATCH` | `EXPIRED` |
| Bybit `New`, `Untriggered`, `Triggered`, `Active` | `NEW` |
| Bybit `PartiallyFilled`, `Filled`, `Rejected` | `PARTIALLY_FILLED`, `FILLED`, `REJECTED` |
| Bybit `Cancelled`, `PartiallyFilledCanceled`, `Deactivated` | `CANCELED` |
| BingX `NEW`, `PENDING` | `NEW` |
| BingX `PARTIALLY_FILLED`, `FILLED`, `EXPIRED` | the same |
| BingX `CANCELED`, `CANCELLED`; `FAILED` | `CANCELED`; `REJECTED` |
| anything else, or no state in the answer | `UNKNOWN` |

  The Bybit and BingX rows are the raw clients' own maps. `venue_status` carries the venue's word verbatim, or
  one of the adapter's own markers when the venue gave no state:

| `venue_status` | `status` | Meaning |
|---|---|---|
| `ACKNOWLEDGED` | `UNKNOWN` | the venue acknowledged an entry and the one read-back failed |
| `ACCEPTED` | `NEW` | the venue accepted a trigger order; the caller reads protection back |
| `CANCEL_ACCEPTED` | `CANCELED` | the venue accepted the cancel of a stop (Binance conditional order, Bybit position stop) |
| `CANCEL_ACKNOWLEDGED` | `UNKNOWN` | the venue acknowledged a cancel and the read-back failed |
| `POSITION_ATTRIBUTE` | `NEW` | a Bybit position carries this stop |
| `OPEN` | `NEW` | Binance lists this conditional order as open and gave no state field |

- `OrderInfo.updated_at_ms` is 0 when the venue did not report a time.
- `positions()` returns open positions only. An empty tuple means flat.

## 6. Client order ids and idempotency

`venue_client_order_id` decides what is sent. The id travels unchanged when it has at most the venue's maximum
length, uses only `A-Z a-z 0-9 _ -`, and (BingX) has no capital letter. Any other id is replaced by `cf` plus the
first 30 hexadecimal digits of its SHA-256. An id is never truncated and never case-folded. An id of at most 32
lower-case characters reaches every venue unchanged.

| Venue | Field | Maximum | Evidence |
|---|---|---|---|
| Binance | `newClientOrderId`, `clientAlgoId` | 36 | documentation as remembered; `clientAlgoId` assumed equal. UNVERIFIED |
| Bybit | `orderLinkId` | 36 | the raw client cuts at 36. UNVERIFIED at the venue |
| BingX | `clientOrderId` | 40, lower case | the raw client lower-cases and cuts at 40. UNVERIFIED at the venue |

Orders read back from the venue carry the id the venue stored. For an id that travelled as its hash, that is the
hash; a lookup by the caller's id returns the caller's id.

Idempotency does not rely on the venue refusing a duplicate, because the scope of that refusal is not
established for any of the three. Before an order is created the adapter asks the venue for an order with that
id. If one exists it is returned (or `DUPLICATE_CLIENT_ORDER_ID` when its symbol, side or type differ). If the
lookup fails for any other reason, nothing is sent. If the venue itself answers "duplicate", the adapter looks
again. The cost is one read before each create on Binance and BingX and up to two on Bybit.

## 7. Protective stops

| | Binance | Bybit | BingX |
|---|---|---|---|
| What rests | a conditional order with `closePosition` | the position's own stop (`trading-stop`, Full) | a trigger order with a quantity and `reduceOnly` |
| `close_position=True` | yes | yes, needs an open position | `UNSUPPORTED` |
| `reduce_only` with the whole open quantity | placed as the `closePosition` order | set as the position's stop | yes |
| `reduce_only` with part of the position | `UNSUPPORTED` | `UNSUPPORTED` | yes |
| `venue_order_id` | `algo:<algoId>` | `POSITION_SL:<symbol>` / `POSITION_TP:<symbol>` | the venue's order id |
| Caller's id stored at the venue | yes (`clientAlgoId`) | no | no |
| Second stop of the same kind | refused by the venue (`-4130`) | refused by the adapter | refused by the adapter |
| Change the trigger | cancel, then create | `amend_stop`, in place | cancel, then create |
| Stop after the position closes | not established | gone with the position (documentation) | not established |

One request form works on all three: `reduce_only=True` with the quantity of the whole open position. On Binance
and Bybit the adapter reads the position first; the stop is placed only when the quantity is exactly what is
open, and it comes back as what rests (`close_position=True`, no quantity). A partial quantity is refused there,
never widened to the whole position.

Where the venue stores no client id for a stop (Bybit, BingX):

- the adapter remembers which stop it placed for which id while it lives. After a restart a lookup by the
  caller's id is `ORDER_NOT_FOUND`; use the `venue_order_id` that was returned when the stop was placed;
- a request for a stop that already rests with the same geometry returns that stop and sends nothing. With
  another trigger it is `STOP_ALREADY_EXISTS`. A stop is never silently overwritten.

Read-back limits:

- Binance: only open conditional orders can be read. One that fired or was cancelled is `ORDER_NOT_FOUND`, and
  its client id cannot be reused (the venue refuses it as a duplicate and no open order answers for it).
- Bybit: a position's stop that fired, or whose position closed, is `ORDER_NOT_FOUND`.
- `fills(venue_order_id=...)` is `UNSUPPORTED` for a Binance conditional order and for a Bybit position stop:
  the execution is recorded under an order id the adapter cannot know. Read fills by time.

## 8. Capability matrix

`capabilities()` returns the same values for `LIVE` and `DEMO`, except `environment`. The fake venue's profiles
state the same matrix, except `user_stream`.

| Flag | Binance | Bybit | BingX | Evidence |
|---|---|---|---|---|
| `native_stop_market`, `native_take_profit` | True | True | True | the raw clients place both. UNVERIFIED at the venue for Bybit and BingX |
| `reduce_only` | True | True | True | the raw clients send it |
| `close_position_stop` | True | True | False | Binance, Bybit: repository code. BingX: False because the repository always sends a quantity. UNVERIFIED |
| `quantity_stop` | False | False | True | False where the repository has no partial stop. UNVERIFIED (conservative) |
| `stop_amend_in_place` | False | True | False | Bybit: the raw client amends in place. Others: no amend in the repository |
| `multiple_stops_per_position` | False | False | False | Binance: `-4130` in the raw client. Bybit: one stop field per position. BingX: UNVERIFIED (conservative; the raw client assumes the opposite) |
| `position_stop_endpoint` | False | True | False | repository code |
| `stop_client_order_id` | True | False | False | Binance: `clientAlgoId` in repository code. BingX: the repository states trigger orders take no client id. UNVERIFIED |
| `one_way_mode` / `hedge_mode` | True / False | True / False | True / False | the adapters send one-way orders only |
| `user_stream` | False | False | False | see section 11 |
| `demo_environment` | True | True | True | `shared_lib.broker.capabilities.DECLARED_PROFILES` |
| `client_order_id_max_length` | 36 | 36 | 40 | section 6 |

## 9. Error mapping

### 9.1 Rules that hold for every venue

| What happened | Mutation | Read |
|---|---|---|
| HTTP 418 | `BANNED` | `BANNED` |
| HTTP 429 | `RATE_LIMITED` | `RATE_LIMITED` |
| HTTP 5xx or 408 | `NETWORK_UNCERTAIN` | `VENUE_UNAVAILABLE` |
| A venue code that means "the server failed or timed out" | `NETWORK_UNCERTAIN` | `VENUE_UNAVAILABLE` |
| Connect timeout (no connection was made) | `VENUE_UNAVAILABLE` | `VENUE_UNAVAILABLE` |
| Read timeout, lost connection | `NETWORK_UNCERTAIN` | `VENUE_UNAVAILABLE` |
| HTTP 2xx with a body that is not JSON | `NETWORK_UNCERTAIN` | `MALFORMED_RESPONSE` |
| An accepted mutation whose answer carries no identity | `NETWORK_UNCERTAIN` | n/a |
| A read answer without a required field | n/a | `MALFORMED_RESPONSE` |
| A venue code in the adapter's table | the mapped code | the mapped code |
| HTTP 401 without a mapped code | `AUTH_FAILED` | `AUTH_FAILED` |
| The platform refused to send it (order submission disabled, entry authority) | `PERMISSION_DENIED` | n/a |
| Anything else the venue answered | `UNMAPPED_VENUE_ERROR` with `venue_code` and `http_status` | the same |

A mutation that meets HTTP 5xx is reported as uncertain, not as unavailable. The repository's own rule
(`app/execution/fill_resolution.py`) is that a 5xx, a 408 or an unreadable body proves nothing about whether the
venue acted. `VENUE_UNAVAILABLE` is retryable in the contract, so it is given to a mutation only when a connect
timeout proves nothing was sent.

`retry_after_s` and `banned_until_ms` are known only for Binance reads. The Bybit and BingX raw clients discard
response headers, and the Binance raw client keeps no back-off for a refused mutation. Elsewhere both are `None`.

`UNMAPPED_VENUE_ERROR` on a mutation means the venue answered with a refusal the adapter cannot name. It is not
marked uncertain. A caller that wants to send again should look the client order id up first.

### 9.2 Binance

VERIFIED means: this repository already handles the code in the file named, which is the evidence used. No row
was checked against a venue answer in this work. UNVERIFIED means: the venue's published error list as
remembered, nothing else.

| Code | Contract error | Status | Evidence |
|---|---|---|---|
| -1003, -1015 | `RATE_LIMITED` | VERIFIED | `execution/fill_resolution.py`, `tests/test_binance_rate_limits.py` |
| -1021 | `TIMESTAMP_OUT_OF_SYNC` | VERIFIED | `exchange/binance/client.py` |
| -2015 | `AUTH_FAILED` | VERIFIED | `tests/test_binance_rate_limits.py` |
| -2018, -2019 | `INSUFFICIENT_MARGIN` | VERIFIED | `execution/executor.py` |
| -4164 | `MIN_NOTIONAL` | VERIFIED | `exchange/catalog_refresh.py` |
| -4003, -4005, -4023 | `INVALID_QUANTITY` | VERIFIED | `exchange/catalog_refresh.py` |
| -4014 | `INVALID_PRICE` | VERIFIED | `exchange/catalog_refresh.py` |
| -1121 | `INSTRUMENT_UNKNOWN` | VERIFIED | `exchange/catalog_refresh.py` |
| -4140 | `INSTRUMENT_NOT_TRADING` | VERIFIED | `exchange/catalog_refresh.py` |
| -2013 | `ORDER_NOT_FOUND` | VERIFIED | `execution/fill_resolution.py` |
| -4111, -4115, -4116 | `DUPLICATE_CLIENT_ORDER_ID` | VERIFIED | `execution/fill_resolution.py` |
| -2022 | `REDUCE_ONLY_REJECTED` | VERIFIED | `tests/test_binance_rate_limits.py` |
| -2021 | `STOP_WOULD_TRIGGER_IMMEDIATELY` | VERIFIED | `execution/executor.py` |
| -4130 | `STOP_ALREADY_EXISTS` | VERIFIED | `exchange/binance/client.py` |
| -1000 to -1099 except -1002, -1003, -1013, -1014, -1015, -1020 to -1023 | uncertain / unavailable | VERIFIED | `execution/fill_resolution.py` |
| -1022, -2014 | `AUTH_FAILED` | UNVERIFIED | documentation only |
| -2011 | `ORDER_NOT_FOUND` | UNVERIFIED | documentation only |
| -4061 | `POSITION_MODE_MISMATCH` | UNVERIFIED | documentation only |
| -4028 | `LEVERAGE_NOT_ALLOWED` | UNVERIFIED | documentation only |
| -1111 (precision) and every other code | `UNMAPPED_VENUE_ERROR` | UNVERIFIED | not mapped: -1111 does not say whether the price or the quantity is wrong |
| HTTP 403 | `UNMAPPED_VENUE_ERROR` | UNVERIFIED | not mapped |

### 9.3 Bybit

Only 34040 is handled anywhere in this repository. Every other row is the venue's published error list as
remembered. Nothing was checked against a venue answer.

| Code | Contract error | Status |
|---|---|---|
| 34040 (trading-stop not modified) | treated as success | VERIFIED (`exchange/bybit/client.py`) |
| 110043 (leverage not modified) | treated as success | UNVERIFIED (the raw client matches the text "not modified") |
| 10002 | `TIMESTAMP_OUT_OF_SYNC` | UNVERIFIED |
| 10003, 10004 | `AUTH_FAILED` | UNVERIFIED |
| 10005, 10010 | `PERMISSION_DENIED` | UNVERIFIED |
| 10006, 10018 | `RATE_LIMITED` | UNVERIFIED |
| 10009 | `BANNED` | UNVERIFIED |
| 110001 | `ORDER_NOT_FOUND` | UNVERIFIED |
| 110004, 110007, 110012 | `INSUFFICIENT_MARGIN` | UNVERIFIED |
| 110017 | `REDUCE_ONLY_REJECTED` | UNVERIFIED |
| 110072 | `DUPLICATE_CLIENT_ORDER_ID` | UNVERIFIED |
| 10000, 10016 | uncertain / unavailable | UNVERIFIED |
| HTTP 403 | `BANNED` | UNVERIFIED |
| 10001 (parameter error), 110092, 110093 (trigger already reached), 110094 (notional), every other code | `UNMAPPED_VENUE_ERROR` | UNVERIFIED, not mapped |

An HTTP error from Bybit arrives without its `retCode`: the raw client keeps the status and the text only.

### 9.4 BingX

No BingX error code is handled anywhere in this repository. Every row is the venue's published error list as
remembered. Nothing was checked against a venue answer.

| Code | Contract error | Status |
|---|---|---|
| 100001, 100412, 100413 | `AUTH_FAILED` | UNVERIFIED |
| 100004, 100419 | `PERMISSION_DENIED` | UNVERIFIED |
| 100421 | `TIMESTAMP_OUT_OF_SYNC` | UNVERIFIED |
| 100410 | `RATE_LIMITED` | UNVERIFIED |
| 101204 | `INSUFFICIENT_MARGIN` | UNVERIFIED |
| 80016 | `ORDER_NOT_FOUND` | UNVERIFIED |
| 101415 | `INSTRUMENT_NOT_TRADING` | UNVERIFIED |
| 101414 | `LEVERAGE_NOT_ALLOWED` | UNVERIFIED |
| 100500, 80012, 101500 | uncertain / unavailable | UNVERIFIED |
| every other code, including the codes for a duplicate client id, a rejected reduce-only order, an invalid quantity and a trigger already reached | `UNMAPPED_VENUE_ERROR` | UNVERIFIED, not mapped |

If BingX reports "no such order" with a code other than 80016 and not with an empty order record, the lookup
before every create fails with `UNMAPPED_VENUE_ERROR` and the adapter places nothing. That fails closed and is
the first thing the operator check in `BINGX_ENVIRONMENT_CERTIFICATION.md` settles.

## 10. The fake venue

`FakeVenue(profile, ...)` is a deterministic in-memory venue that implements the protocol. Its `adapter_version`
starts with `fake-` and the factory never returns it. Its rules are stated in the module docstring. In short:

- profiles `binance`, `bybit`, `bingx` state the capability matrix of section 8 and the stop rules of section 7;
- instrument rules are enforced; positions are one-way, one per symbol, cross margin;
- `mutations` and `reads` journal every contract call with arguments, outcome and effect. A shadow run is proved
  by `fake.mutations == []`;
- `fail_next(method, error, *, after_effect=False, times=1, symbol=None)` injects a failure; `after_effect=True`
  models an accepted mutation whose answer was lost; `rate_limit_for(seconds, banned=False)` refuses every venue
  call until the clock has passed;
- `protection_gaps(symbol)` lists the intervals in which a non-zero position had no resting `STOP_MARKET` on its
  closing side covering all of it. A cancel followed by a create at the same clock instant is still a gap.

The fake is not evidence about any venue.

## 11. Known gaps

1. `user_stream()` is `UNSUPPORTED` on the three adapters. The raw clients issue no stream key, and the
   repository holds no stream host for any venue or environment. Supplying one needs a stream host table in
   `shared_lib/broker/environment.py` and a key-issuing method on the Binance and BingX raw clients. Issuing a
   key is a POST, which the platform gate refuses while order submission is disabled.
2. Order book depth is not part of the contract. The raw clients have `get_orderbook`.
3. Hedge mode is not carried. A hedge-mode position is reported by `positions()`; a stop that would need the
   position refuses with `POSITION_MODE_MISMATCH`.
4. Only `GTC` limit orders are carried.
5. `fills(since_ms)` is read in weekly windows and refuses a start older than 13 weeks. On Binance a full page
   of 1000 fills is refused rather than trusted.
6. `funding().interval_minutes` is `None` on Binance and Bybit (published elsewhere). `max_leverage` is `None`
   on Binance and BingX (not in the listing the adapters read).
7. Bybit account totals are the unified account's USD-valued totals, labelled `USDT` as the raw client does.
8. `rate_limit().scope` is `IP` for Binance and BingX and `UID` for Bybit, whose documented response headers
   describe the account's budget. The Bybit and BingX raw clients keep none of what the venue says, so their
   observation is always empty. The BingX scope is UNVERIFIED.
9. Inherited from the Binance raw client, unchanged: it repeats a rate-limited or timed-out READ a bounded
   number of times with short sleeps, and it re-sends a request once after `-1021` (the venue did not process
   the first). The adapter adds no retry of its own.
10. Linux was not run. The code uses no file time, locale, time zone or path separator.

## 12. Observations about the raw clients (not changed here)

- `exchange/binance/client.py:528`: `place_order` sends a LIMIT order without `price` or `timeInForce`.
- `exchange/binance/client.py:1396`: `cancel_order` turns every failure, a timeout included, into `False`.
- `exchange/bybit/client.py:411`: `set_leverage` does not read `retCode`; a refusal sent with HTTP 200 is
  returned as if it had succeeded.
- `exchange/bybit/client.py:691`: `user_trades` returns every execution type. If the venue lists funding
  settlements there, as its documentation says, they are returned as trades. To be confirmed against the venue.
- `exchange/bybit/client.py:137`: `account` returns a balance of zero when the venue reports no wallet.
- `exchange/bybit/client.py:635` and `exchange/bingx/client.py:591`: a client order id is cut to length, so two
  ids that share a prefix become one.
- `exchange/bybit/client.py:84` and `exchange/bingx/client.py:94`: `Retry-After` is discarded.
- `exchange/bingx/client.py:223`: `account` reports 0.0 for a missing balance, equity or available margin.
- `exchange/bingx/client.py:227` and `:309`: unrealised profit is read under `unrealisedPNL`; the documentation
  as remembered prints `unrealizedProfit`. To be confirmed against the venue.
- `exchange/bingx/client.py:348`: `mode is not False` treats the text `"false"` as hedge mode. To be confirmed.
- `exchange/bingx/client.py:598`: `place_order` reports an order state it cannot map as `NEW`.
- `exchange/bingx/client.py:106`: every transport failure, including an unreadable 200 answer, becomes
  "Network Error"; whether the request left is lost.
- `exchange/bingx/client.py:664`: a fee the venue did not report becomes 0.
- `scripts/validate_demo_venue.py:33`: the default evidence directory is `docs/research/venue_validation`,
  inside the area whose content is hash-pinned.
