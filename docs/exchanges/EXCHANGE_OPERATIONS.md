# Exchange operations: rule cache, user streams, REST reconciliation, rate budget (Section H, Steps 4.3 to 4.5)

Current integration status (16 October 2026): [validation report](../implementation/STEP3_STEP4_VALIDATION.md). Concrete TLS transport, runtime lifecycle, shared-library HTTP budgets and scoped certification harness are implemented. No real venue certification was performed.

Code: `backends/bot-backend/app/exchange/platform_adapter/` (`rules_cache.py`, `user_stream.py`,
`reconcile.py`, `rate_budget.py`)
Tables: `backends/shared/shared_lib/persistence/cati_exchange_schema.py`
Tests: `backends/bot-backend/tests/exchange/` (`test_rules_cache.py`, `test_user_stream.py`,
`test_rate_budget.py`)

Status on 2026-10-09: the four modules exist and are tested against stubs. **None of them is wired into a
running path.** No execution path reads the rule cache, no worker runs a stream manager, no adapter is
wrapped in the budget. Trading stays SHADOW ONLY. No venue was contacted to build or test any of this, and
no venue documentation could be re-read during the build (no network access): see section 5 for what that
means for every venue fact used.

Verification labels used below:

* **VERIFIED-BY-DOC**: the value or shape is the one in the venue's published API documentation as known to
  the author with certainty. It was not re-read online for this build, and no frame or response of a real
  venue was observed. The first real connection must confirm it (section 5).
* **UNVERIFIED**: not certain. The code either does not rely on it at all, or uses a deliberately low
  configuration value in its place.

---

## 1. Market-rule cache (4.3)

### What it holds

`cati_instrument_rules`, one row per (venue, environment, canonical symbol):

| Requirement | Column(s) |
|---|---|
| symbol identity | `symbol`, `venue_symbol`, `base_asset`, `quote_asset`, `margin_asset` |
| contract status | `status` (TRADING / NOT_TRADING), `venue_status` (verbatim) |
| minimum / maximum quantity, quantity step | `min_quantity`, `max_quantity`, `market_max_quantity`, `quantity_step` |
| price tick, minimum notional | `price_tick`, `min_notional` |
| leverage bounds | `max_leverage` (the contract carries no lower bound; none is stored or assumed) |
| position mode, supported order types, protection capabilities | `capabilities` (the adapter's `AdapterCapabilities` at refresh time; venue level, not per symbol) |
| precision and margin rules | `price_tick`, `quantity_step`, `contract_size`, `margin_asset` (margin tiers are not in the contract and are not stored) |
| source timestamp | `fetched_at_ms` (stamped by the adapter) |
| adapter version | `adapter_version` |
| expiry time | `expires_at_ms` = `fetched_at_ms` + maximum age |

A value the venue did not report is NULL and stays NULL. There is no numeric default anywhere.

`cati_instrument_rule_changes` (append-only) keeps every change: FIRST_SEEN, RULES_CHANGED (with the changed
fields), STATUS_CHANGED, DELISTED, RELISTED, CONFLICT, CONFLICT_CLEARED.
`cati_instrument_rule_refreshes` keeps the refresh state per (venue, environment).

### Asking for rules

| Call | For | Refuses when |
|---|---|---|
| `rules_for_planning` | sizing a new plan; returns the rules and their hash | unknown, conflict, delisted, stale, dated in the future, incomplete, not trading |
| `rules_for_entry` | submitting an entry | all of the above, and: no plan reference given, rules changed since the plan |
| `rules_for_reduce` | a protective stop, a close, a flatten | unknown instrument; quantity step never reported |

Reason codes, each distinct: `RULES_UNKNOWN_INSTRUMENT`, `RULES_STALE`, `RULES_DATED_IN_THE_FUTURE`,
`RULES_INCOMPLETE`, `RULES_INSTRUMENT_NOT_TRADING`, `RULES_DELISTED`, `RULES_CONFLICT`,
`RULES_CHANGED_SINCE_PLAN`, `RULES_PLAN_REFERENCE_MISSING`.

`rules_for_reduce` is how emergency maintenance and flatten survive a stale cache: it returns the venue's own
last known rules with the tolerated reasons in `degraded`. It still refuses when nothing was ever read, or
when the venue never reported a quantity step: no step is invented even to close.

A plan says which rules it used either by hash (`plan_rules_hash`) or by time (`plan_time_ms`). An entry
checked with neither is refused.

### Refresh

* Maximum age 24 h (`DEFAULT_MAX_AGE_MS`), refresh due every 6 h (`DEFAULT_REFRESH_INTERVAL_MS`). Both are
  configuration. `ensure_loaded(adapter, symbols, now_ms)` is the start-up call; `refresh_due` tells a
  scheduler when to call `refresh`.
* No transaction is open while the venue is asked.
* A failed refresh changes no rule row. The next attempt waits 5 s, doubling to at most 15 min
  (`refresh_backoff_ms`); a venue instruction to wait (Retry-After, ban end) outlasts a shorter backoff.
  Rows that could not be refreshed age into `RULES_STALE` by themselves.
* An empty full listing is a failure (`RULES_REFRESH_EMPTY`), never a mass delisting.
* A full listing that returns fewer than half of at least 20 known instruments is treated as partial
  (`RULES_REFRESH_SUSPECT_PARTIAL`): what came back is stored, nothing is delisted, the refresh counts as
  failed. The fraction is `app.exchange.instruments.MASS_DELIST_MAX_FRACTION`.
* A record with an older `fetched_at_ms` than the stored one never replaces it.
* A record that contradicts itself or its key (minimum above maximum, step above maximum, key and symbol
  differ, wrong venue or environment, stamped in the future) is not stored; the symbol answers
  `RULES_CONFLICT` until a consistent record arrives.
* Two workers do not ask the venue for the same listing at once (a 60 s claim in the refresh-state row).

### Quantisation

`quantize_quantity` rounds down only. `quantize_stop_price` moves a stop toward the entry (up for a LONG,
down for a SHORT). `quantize_limit_price` never pays more (BUY down) and never sells for less (SELL up).
`executable_quantity` refuses a quantity outside the venue's limits instead of moving it into them.
`minimum_order_quantity` is the venue's own floor and exists for bounded infrastructure tests; it is not a
rounding of a risk-sized quantity. `leverage_allowed` refuses when the venue reported no limit.

### Relation to the existing catalogue

`app.exchange.instruments.InstrumentCatalog` (`venue_instruments`) and
`app.trading_intelligence.execution.preflight.SubmissionPreflight` are unchanged. The catalogue is the
discovery of every product a venue publishes, keyed by the legacy venue keys, and the preflight refuses on
metadata older than one hour. The rule cache holds the contract's `InstrumentRules` for the platform
adapters. Neither reads the other.

### Not done

* Nothing calls `ensure_loaded` at start-up and no scheduler calls `refresh`.
* No execution path calls `rules_for_entry`. The acceptance "stale or invalid entry filters prevent order
  creation" is true of the cache and not yet of the order path.

---

## 2. User streams and REST reconciliation (4.4)

### Principle

The stream is a hint and REST is the truth. `reconcile.RestReconciler` reads open orders, positions and the
fills since the last pass through the `VenueAdapter`; its answer replaces the hints. A pass runs on connect,
on reconnect, after a restart, on a gap, on an order nobody announced (`expect_order` announces the
platform's own), on a frame that cannot be read, while the stream is down (every 15 s) and every 5 min while
it is live.

### Transport: DEFERRED

`user_stream.py` imports no websocket library. The connection is the `StreamTransport` protocol
(`connect`, `send`, `receive`, `ping`, `close`) and the tests script it.

A transport that speaks to a real venue is **not part of this build**. `websockets` 15.0.1 is pinned in
`deploy/constraints.txt` and present in the local virtual environment, but it is not declared in
`backends/bot-backend/requirements.txt` (which is what CI installs) and nothing in the repository imports it.
Adding it there is a dependency decision outside this workstream. Until a transport exists, the only way the
account state is kept is REST reconciliation, which needs no stream: `RestReconciler.run` works on its own.

What an operator or the next workstream must do: declare a websocket client dependency, implement
`StreamTransport` on it (returning `HEARTBEAT` for protocol pings and pongs, raising `StreamClosed` without
the URL in the message), and run a worker that calls `UserStreamManager.step(now_ms)` in a loop.

### Frame shapes

A field is read only from a shape labelled VERIFIED-BY-DOC. A shape labelled UNVERIFIED is never parsed: the
frame becomes a `RECONCILE_HINT` and the venue is asked over REST.

| Venue | Frame | Label | What is read |
|---|---|---|---|
| Binance USD-M | `ORDER_TRADE_UPDATE` | VERIFIED-BY-DOC | `E`; `o.s`, `o.c`, `o.S`, `o.i`, `o.x`, `o.X`, `o.l`, `o.z`, `o.L`, `o.ap`, `o.t`, `o.T`, `o.n`, `o.N`, `o.rp`, `o.m` |
| Binance USD-M | `ACCOUNT_UPDATE` | VERIFIED-BY-DOC | `E`; `a.B[].a`, `a.B[].wb`; `a.P[].s`, `.pa`, `.ep`, `.up`, `.ps` |
| Binance USD-M | `listenKeyExpired` | VERIFIED-BY-DOC | the event type only |
| Binance USD-M | `ALGO_UPDATE` and any other event type | UNVERIFIED | nothing (hint) |
| Binance USD-M | `TRADE_LITE`, `MARGIN_CALL`, `STRATEGY_UPDATE`, `GRID_UPDATE` | ignored | nothing (no hint) |
| Bybit V5 private | `op` answers `auth`, `subscribe`, `pong` / `ping` | VERIFIED-BY-DOC | `op`, `success` |
| Bybit V5 private | topic `order` | VERIFIED-BY-DOC | `category`, `symbol`, `orderId`, `orderLinkId`, `side`, `orderStatus`, `cumExecQty`, `avgPrice`, `updatedTime` |
| Bybit V5 private | topic `execution` | VERIFIED-BY-DOC | `category`, `symbol`, `execId`, `execType`, `execQty`, `execPrice`, `execFee`, `isMaker`, `orderId`, `orderLinkId`, `side`, `execTime` |
| Bybit V5 private | topic `position` | VERIFIED-BY-DOC | `category`, `symbol`, `side`, `size`, `entryPrice`, `unrealisedPnl`, `positionIdx`, `seq`, `updatedTime` |
| Bybit V5 private | topic `wallet` | VERIFIED-BY-DOC | `creationTime`; `coin[].coin`, `coin[].walletBalance` |
| Bybit V5 private | topic `execution.fast` and any other topic | UNVERIFIED | nothing (hint) |
| BingX swap | every frame | UNVERIFIED | nothing (hint), except that a frame naming itself `listenKeyExpired` renews the key early |

Deliberate non-readings:

* Bybit fill fee asset and realised profit are left `None` on a stream fill (the field names are not
  certain); REST supplies them.
* Bybit `orderStatus` `Triggered` and any Binance status outside NEW, PARTIALLY_FILLED, FILLED, CANCELED,
  EXPIRED, EXPIRED_IN_MATCH map to `UNKNOWN`, which triggers a REST pass. Bybit `Untriggered` maps to NEW,
  `Deactivated` and `PartiallyFilledCanceled` to CANCELED.
* Bybit execution types other than `Trade`, `AdlTrade`, `BustTrade`, `BlockTrade` are hints; `Funding` is
  ignored as not a fill.
* A hedge-mode position (Binance `ps` LONG/SHORT, Bybit `positionIdx` 1/2) is a hint: two positions on one
  symbol are not one position of the contract.
* A malformed frame of a known type is a hint, never an exception.

BingX consequences: on BingX the stream can only say "something happened". Data frames cost REST passes, at
most one per 2 s per account however many frames arrive. Before a field of a BingX frame is read, an operator must capture
frames on the venue's demo environment and compare them with the venue's documentation.

### Heartbeat and keys

| Behaviour | Label |
|---|---|
| Any venue: a protocol-level ping every 15 s when the venue documents no application ping; 45 s without any frame or pong is silence | RFC 6455, venue independent |
| Bybit: application ping frame `{"op": "ping"}` | VERIFIED-BY-DOC |
| BingX: frames are gzip; a text `Ping` is answered with `Pong` | UNVERIFIED (gzip is detected by its magic number for every venue; answering `Ping` is harmless if the venue never sends it) |
| Listen-key streams: every `keepalive_seconds` of the adapter's specification the manager calls `adapter.user_stream()` again. The same address back means the key was extended; a different address means a new key and the connection moves to it. `expires_after_seconds` without a successful renewal ends the connection | an assumption about the ADAPTER, not a venue fact: see "Outside this workstream" |
| Auth-frame streams (Bybit): LIVE only after the `auth` and the `subscribe` answers; 10 s without them reconnects; a refused authentication waits the maximum backoff | VERIFIED-BY-DOC for the answers |

### Health

`stream_status(db, venue, environment, account_ref, now_ms)` and `stream_allows_entries(...)` can be read by
any process.

| Health | Meaning | New entries |
|---|---|---|
| LIVE | connected, acknowledged, reconciled, heard from recently | allowed |
| RECONCILE_REQUIRED | connected; the venue has not confirmed the state (after connect, a gap, an unknown order, a failed pass) | blocked |
| STALE | the connection went silent; or the owning worker stopped renewing its lease | blocked |
| DISCONNECTED | no connection (never connected, lost, refused, held by another worker, closed) | blocked |

Only new entries consult this. Position maintenance, protection and flatten never do.

**Not wired.** No entry path calls `stream_allows_entries`. In this build no stream can be LIVE at all (no
transport), so wiring the gate before a transport exists would block every entry.

### Gaps, duplicates, restart

* No venue's user stream documents a gap-free sequence number that could be substantiated. A gap is found by
  a lost connection, by silence, or by an order whose cumulative filled quantity exceeds the fills received
  for it for more than 2 s. Bybit `seq` only orders position updates.
* The fill ledger (`cati_user_stream_fills`, append-only) is keyed by venue, account, symbol, trade id and
  source. The symbol is in the key because Binance numbers trades per symbol. A fill is delivered to
  `on_fill` once across stream, REST and restart; where the venue's record differs from the stream's, the
  venue's is the one read and the difference is reported (`FILL_CORRECTED_BY_VENUE`).
* `on_fill` is a notification, not a queue: a process that dies between the commit of a fill and the
  callback does not get that callback again. A consumer that must not miss a fill reads
  `FillLedger.fills(since_ms=...)` after a restart.
* Cursors are per symbol (`cati_user_stream_cursors`); every pass reads again from 60 s before its cursor.
* A failed pass moves no cursor and returns no state. It is retried after 5 s, doubling to 60 s while it
  keeps failing, and never before the venue's own instruction to wait; further hints do not bring it forward.
* The log of passes (`cati_user_stream_reconciliations`, append-only) holds every pass that failed, found a
  new fill or a difference, or was started by a connect, a restart, a gap or an unknown order. A pass started
  by the timer or by a hint that found nothing is not written, so the table does not grow with the clock;
  `cati_user_stream_state.last_reconcile_at_ms` always says when the last pass ran.
* A frame older than the venue's last answer, and a replay of the frame that closed an order, are dropped.
* One connection per account: a lease in `cati_user_stream_state` (60 s, renewed every 20 s). A second worker
  does not connect; a worker that lost its lease drops its connection and writes nothing.
* Reconnect backoff: 1 s doubling to 60 s. The transport is closed before every connect.

### Limits of the reconciliation scope

The contract reads fills per symbol. A pass covers the symbols given to the manager, every symbol with an
open order or position now or in the state held before, and every symbol a frame mentioned. A fill on a
symbol outside all of these is not seen.

### Secrets

The stream specification (address, subscribe frames) lives in memory for one connection. No address, listen
key or signature is written to a table, a log or an exception; a transport failure is recorded by the name
of its exception type. A test dumps every table and checks.

---

## 3. Rate budget (4.5)

### Model

One budget per (venue, server address) in `cati_rate_budgets`, drawn on by every process that opens the same
database. The server address is supplied by the caller; this module makes no network call to discover it.

* Exact sliding log (`cati_rate_reservations`): the weight granted in the last window is summed inside
  `BEGIN IMMEDIATE`, so no window of the venue can receive more than the capacity from this database.
* Usable capacity = venue limit x utilisation (default 0.8).
* Class ceilings, as a share of the usable capacity:

| Priority | Ceiling | Used for |
|---|---|---|
| EMERGENCY | 100% | flatten, emergency close |
| PROTECTION | 95% | stop placement and replacement, protection verification, unknown-submit read-back |
| RECONCILIATION | 80% | position and order reconciliation |
| ENTRY | 65% | new entries, leverage changes |
| MARKET_DATA | 50% | everything else |

  The gap between two neighbours is capacity the lower class can never take.
* `reserve(...)` before the call. A refusal carries `retry_at_ms`.
* `observe(...)` after the call: usage the venue reports above the local sum is added (never subtracted); a
  lower limit reported by the venue replaces the configured one, a higher one does not.
* HTTP 429 closes the budget until Retry-After; HTTP 418 or any ban closes it until the ban ends. A closed
  budget refuses every class, EMERGENCY included. A closure is only ever extended. There is no reopen call.
  Without an instruction from the venue the closure is 60 s for a rate refusal and 10 min for a ban
  (configuration).
* A second address has its own budget (failover).
* An account-scoped refusal (`scope="UID"`) closes only that account; no capacity is accounted per account.
* `entries_allowed(...)` is the circuit breaker for new entries; `snapshot(...)` is the operator's view
  (usage by class, ceilings, closure, denials, the venue's own figures).
* `RetryPolicy`: at most 3 attempts by default, exponential backoff, jitter that is only ever added, never
  earlier than the venue's instruction, and `None` (fail now) when the venue asks for longer than
  `max_wait_ms`.
* `BudgetedVenueAdapter` wraps a `VenueAdapter`: reserves by method, raises the budget's refusal as
  `AdapterError(RATE_LIMITED | BANNED)` with `venue_code` beginning `LOCAL_BUDGET` before the venue is
  called, closes the budget on a venue 429 / 418, never retries, and feeds `adapter.rate_limit()` back. An
  order that adds exposure is always ENTRY, whatever class the view was given.

### Limits used

| Venue | Default | Label | Basis |
|---|---|---|---|
| Binance USD-M | 2400 request weight per 60 s per IP | VERIFIED-BY-DOC | the REQUEST_WEIGHT limit published in `GET /fapi/v1/exchangeInfo` and reported in `X-MBX-USED-WEIGHT-1M`; the same figure as `app/exchange/binance/client.py` `REQUEST_WEIGHT_LIMIT_1M` |
| Bybit V5 | 150 requests per 5 s per IP | UNVERIFIED | placeholder: a quarter of the 600 per 5 s recalled from the V5 documentation |
| BingX swap | 20 requests per 10 s per IP | UNVERIFIED | placeholder: no figure could be substantiated |

Per-method weights (`DEFAULT_METHOD_WEIGHTS`) are UNVERIFIED as a mapping for every venue: which HTTP requests
an adapter sends for one contract method is the adapter's business. On Binance a wrong estimate is corrected
by the venue's reported usage after each response. On Bybit and BingX it stays an estimate.

An operator sets real limits by passing `limits={venue: VenueLimit(...)}` to `RateBudget`.

### Not done

* No adapter is wrapped. `BudgetedVenueAdapter` must be applied where adapters are constructed.
* The older `app/exchange/binance/client.py` keeps its own in-process rate state and does not reserve here.
  On Binance its usage still reaches this budget through the venue's reported usage, once an adapter is
  wrapped.
* The budget is shared by the workers of one host (one SQLite file). Several hosts behind one public address
  would need one shared database; that is not provided.
* Venue order-count limits (Binance 10 s / 1 min order counts per account) are not modelled; an
  account-scoped refusal closes the account when the venue signals one.

---

## 4. Tables added (all additive, `IF NOT EXISTS`)

| Table | Append-only | Purpose |
|---|---|---|
| `cati_adapter_cert_events` | yes | certification register (hash chain) |
| `cati_adapter_cert_checks` | yes | evidence of each check |
| `cati_adapter_cert_actions` | yes | every mutation of a run: intent, then outcome |
| `cati_instrument_rules` | no | current rules |
| `cati_instrument_rule_changes` | yes | change history |
| `cati_instrument_rule_refreshes` | no | refresh state and backoff |
| `cati_rate_budgets` | no | closure and observed figures per scope |
| `cati_rate_reservations` | no (pruned after 10 min) | sliding log |
| `cati_rate_denials` | no | denial counters |
| `cati_user_stream_state` | no | health and lease |
| `cati_user_stream_fills` | yes | fill ledger |
| `cati_user_stream_cursors` | no | REST cursors |
| `cati_user_stream_reconciliations` | yes | record of each pass that failed, found something, or was not routine |

Rollback: none of these tables is read by an older build, so an older build runs on an upgraded database
unchanged. Nothing existing is altered. The append-only tables cannot be emptied through the application.

---

## 5. Every unverified venue fact, and what to do about it

| Fact | Where it is used | What an operator must do |
|---|---|---|
| Binance and Bybit frame shapes listed as VERIFIED-BY-DOC were not observed on a venue and not re-read online | stream parsers | on the first DEMO connection, capture one frame of each type and compare the fields in section 2 |
| Binance `ALGO_UPDATE` shape | not read (hint) | nothing, unless field-level parsing of conditional orders is wanted |
| Bybit stream fields for fee asset and realised profit of an execution | not read | nothing; REST supplies both |
| BingX user-stream frame shapes, gzip framing, `Ping` / `Pong` | not read; `Ping` is answered | capture frames on the BingX demo environment before any field is read |
| Bybit limits | configured 150 per 5 s, below documented 600 per 5 s; separate connection and UID handling | [official rules checked 16 October 2026](https://bybit-exchange.github.io/docs/v5/rate-limit); validate endpoint-specific account throughput before tuning |
| BingX limits | conservative local 20 per 10 s; never claimed as the venue limit | [official update](https://bingx.com/en/support/articles/31103871611289) distinguishes market, account and endpoint limits; confirm authenticated behaviour before tuning |
| Per-method request weights | fallback only for adapters without a physical HTTP budget | runtime physical requests are charged individually; Binance reserves a conservative 100 units per registered endpoint, unknown paths fail closed; certify endpoint weights before throughput tuning |
| Default closure without a venue instruction (60 s, 10 min) | `RateBudget` | configuration; adjust if a venue documents otherwise |
| Whether `adapter.user_stream()` extends an existing listen key | stream keep-alive | confirm per adapter; see below |

## 6. Integration and remaining external checks

`AccountSession` wires rules refresh, adapter-version recording, concrete TLS
transport and stream health. `VenueRuntime` manages account removal, credential
rotation and shutdown. The canonical portfolio boundary checks persisted
certification and reconciled entry health.

`shared_lib/broker/http_budget.py` is installed before raw client construction in
both backends. It charges retries, stream keys and maintenance at the physical
HTTP boundary. Direct production validation clients also share the same egress
allowance. IP bans survive restart; Bybit UID closures do not close other accounts.
A separate persistent handshake window bounds private WebSocket reconnections.

`CertificationHarness` now composes the real adapters, shared budget,
`HTTPFaultInjector`, a fresh-adapter restart and a bounded one-shot external
reduce-only mutation. Actual certification remains subject to an explicit owner
permit and a current canonical runtime lease. LIVE certification remains refused.

Binance listen keys renew through POST; Bybit authenticates before subscribing;
BingX LIVE has documented listen-key setup. BingX VST private routing remains
unverified and refused. Authenticated frames, full venue certification and
production throughput remain external acceptance gates.
