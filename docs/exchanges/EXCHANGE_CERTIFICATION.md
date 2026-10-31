# Exchange adapter certification (Section H, Step 4.2)

Code: `backends/bot-backend/app/exchange/platform_adapter/certification.py`
Tables: `cati_adapter_cert_events`, `cati_adapter_cert_checks`, `cati_adapter_cert_actions`
(`backends/shared/shared_lib/persistence/cati_exchange_schema.py`)
Tests: `backends/bot-backend/tests/exchange/test_venue_certification.py`

## 1. State on 2026-10-09

| Venue | DEMO | LIVE |
|---|---|---|
| binance | NOT_CERTIFIED | NOT_CERTIFIED |
| bybit | NOT_CERTIFIED | NOT_CERTIFIED |
| bingx | NOT_CERTIFIED | NOT_CERTIFIED |

No certification run has been made against any venue. The register is empty and nothing in this build writes
to it on its own. Every run in the test suite is against a simulated venue held in the test file, in a
temporary database.

The Binance demo evidence gathered in Step 1 (`app/execution/demo_boundary_certification.py`,
`app/venue_validation/demo_validation.py`) is history. It was produced by other code, for the older
execution adapter, before this register existed. It is not a certification under this register and does not
change any cell of the table above.

Trading stays SHADOW ONLY. This module changes no flag, no risk limit and no governance phase.

## 2. What a certification is, and is not

It is infrastructure evidence about one adapter at one version against one venue environment: the adapter
did what the broker-neutral contract (`contract.py`) promises.

It is not evidence about any strategy. A certification order:

* is never produced by a family decision or a TradePlan;
* is never recorded as strategy performance (its trail lives in `cati_adapter_cert_actions`, nowhere else);
* always carries the client order id prefix `cert` (`is_certification_order`), 16 characters, lower case.

## 3. Who may run one

Nobody by default. `CertificationRunner` needs an `OperatorPermit`:

| Field | Meaning |
|---|---|
| `operator_ref` | the named operator who runs it |
| `owner_authorization_ref` | reference to the owner's authorisation of this environment, account and scope |
| `venue`, `environment` | exactly one of each |
| `account_ref` | reference to the account the operator chose (an id, never a credential) |
| `authorized_scope` | what was authorised, in words |
| `symbol` | the one test symbol |
| `max_notional` | quote units the run may hold on the symbol at any moment |
| `issued_at_ms`, `expires_at_ms` | validity of the permit |
| `allow_orders` | default `False`: read checks only |
| `leverage` | default 1 |

The runner refuses, before any venue call and without writing anything:

| Refusal | When |
|---|---|
| `OPERATOR_PERMIT_REQUIRED` | no permit object |
| `NOT_A_VENUE_ADAPTER` | the object does not satisfy the contract protocol |
| `PERMIT_VENUE_MISMATCH` / `PERMIT_ENVIRONMENT_MISMATCH` / `PERMIT_ACCOUNT_MISMATCH` | the permit is for something else |
| `PERMIT_NOT_YET_VALID` / `PERMIT_EXPIRED` | outside the permit's validity |
| `LIVE_CERTIFICATION_NOT_PERMITTED_IN_THIS_BUILD` | a LIVE adapter with a matching LIVE permit: complete authorisation still does not run LIVE |

A LIVE adapter with a DEMO permit, or the reverse, is already refused as an environment mismatch, so no
combination reaches a LIVE venue. The register independently refuses to record CERTIFIED for LIVE, so
bypassing the runner does not help.

Conditions of the owner's "critical distinction" and where each is enforced:

| Condition | Enforcement |
|---|---|
| owner authorised environment, account and scope | the permit; recorded with the run |
| account appropriate for the test | `TEST_SCOPE`: flat on the symbol, no open order on it, size fits the permit |
| no live-money exposure | LIVE refused twice (runner and register) |
| not confusable with strategy performance | `cert` order ids; separate tables; no plan, no decision |
| existing positions protected | an account that is not flat on the test symbol is never used; cleanup closes only what the run's own orders opened |
| bounded size, rollback | minimum order size; `max_notional` checked before every order that adds exposure; cleanup in `finally`; `recover` for a dead process |
| all actions auditable | an INTENT row before every mutation, an OUTCOME row after it |

## 4. The catalogue

The same 30 checks for every venue and environment (`CATALOGUE`). "Owner test" is the number in the
assignment's list of 20 required tests.

| Check | Owner test | Phase | Needs from the harness |
|---|---|---|---|
| ACCOUNT_ENVIRONMENT | 1 | read | |
| TIME_SYNC | 16 | read | |
| INSTRUMENT_RULES | 2 | read | |
| ACCOUNT_STATE | 2 | read | |
| RATE_LIMIT_OBSERVATION | 15 | read | |
| USER_STREAM_SPEC | (operation USER_STREAM) | read | |
| TEST_SCOPE | | order | |
| LEVERAGE_AND_MODE | 2 | order | |
| VENUE_REJECTION | 14 | order | |
| ENTRY_SUBMIT | 3 | order | |
| ORDER_LOOKUP | 3 | order | |
| ENTRY_FILL | 4 | order | |
| IDEMPOTENT_REPEAT | 3 | order | |
| PROTECTION_CREATE | 5 | order | |
| PROTECTION_READ_BACK | 6 | order | |
| PROTECTION_GEOMETRY | 7 | order | |
| PROTECTION_REPLACE | 5, 6 | order | |
| UNKNOWN_SUBMIT_RECONCILE | 8 | order | |
| CANCEL | 9 | order | |
| LOST_ACKNOWLEDGMENT | 10 | fault | fault injector |
| CONNECTION_INTERRUPTION | 11 | fault | fault injector |
| RATE_LIMIT | 15 | fault | fault injector |
| CLOCK_DRIFT | 16 | fault | fault injector |
| RESTART_WITH_PROTECTED_POSITION | 12 | order | adapter factory |
| MANUAL_POSITION_CHANGE | 13 | fault | fault injector (or the operator, by hand) |
| CLOSE_POSITION | 17 | order | |
| CANCEL_OBSOLETE_PROTECTION | 18 | order | |
| FLATTEN | 19 | cleanup | |
| RECONCILIATION | 20 | cleanup | |
| FINAL_ACCOUNT_STATE | 20 | cleanup | |

All are core except `USER_STREAM_SPEC`, which certifies only the operation `USER_STREAM`.

Faults a venue cannot be asked to produce (a lost answer, an interrupted request, a 429, a drifted clock) are
armed on the client side of the adapter's transport through the `FaultInjector` protocol. A run without an
injector and without an adapter factory records those six checks as NOT_RUN and is INCOMPLETE: it cannot
certify.

What a run does, in order: read checks; `TEST_SCOPE`; set leverage; a reduce-only order on the flat account
that the venue must refuse; a minimum-size market entry (two minimum units when an injector will reduce the
position from outside, otherwise one); lookup; fill agreement between order, fills and position; a repeat
of the same client order id; a reduce-only stop 5% under the entry; read-back; geometry; replacement by a
stop 2.5% under the entry (amend in place or create-then-cancel, whichever the venue's capabilities allow,
recorded as `method`; for a venue with neither, the tighter stop must be refused and the first must survive:
method `REPLACEMENT_NOT_POSSIBLE_SAFELY`, and `STOP_REPLACEMENT` is not certified, see section 10); lookup of an id never sent; a resting limit order 5% under the
mark, cancelled, cancelled again; the fault checks; a fresh adapter; an outside reduction; close; removal
of the stop; flatten; reconciliation; final state.

A failed check stops the sequence. Cleanup always runs.

Size. The unit is the venue's minimum order at a price 5% under the mark. The permit must hold twice the
entry, because a failed `IDEMPOTENT_REPEAT` is a second entry. The largest position any green run in the
tests held was two units.

Side effect that is not undone: the leverage of the test symbol is left at the permit's leverage (default 1).

## 5. Outcome and status

A run's outcome is computed by the register from the recorded checks, never from a caller's word:

| Outcome | Rule | Register status |
|---|---|---|
| PASSED | every core check PASS | CERTIFIED, expires 90 days after completion |
| FAILED | any check FAIL | FAILED (supersedes an earlier CERTIFIED) |
| INCOMPLETE | no FAIL, some core check not run | none: it neither certifies nor withdraws |

`certification_status(db, venue, environment, now_ms, adapter_version=None)`:

| Status | When |
|---|---|
| NOT_CERTIFIED | no evidence; only incomplete runs; a certificate dated in the future; register or run evidence failed its integrity check |
| CERTIFIED | latest decisive event is a green run, unexpired, versions unchanged |
| EXPIRED | `now >= completed + 90 days`; a `VERSION_CHANGED` event after the run; the contract version of the code differs from the certified one; the caller's `adapter_version` differs |
| REVOKED | `CertificationRegister.revoke` |
| FAILED | latest decisive event is a failed run |
| BLOCKED | `CertificationRegister.block` (no safe environment, no authorised account) |

Only CERTIFIED is certified. A recorded version change keeps the certificate expired even if the old version
is deployed again; only a new complete green run certifies.

`adapter_certified(db, venue, environment, now_ms, adapter_version=None, operation=None) -> (bool, reason)`
is what an execution boundary calls. With `operation` it also requires the venue capability and the checks of
that operation (`OPERATIONS`: ENTRY, PROTECTIVE_STOP, STOP_REPLACEMENT, CANCEL, CLOSE, USER_STREAM). An
operation this framework does not exercise (take-profit orders, hedge mode, close-position stops) is never
selectable on the strength of a certificate.

**Not wired.** No execution path calls `adapter_certified` yet. The existing boundary still uses
`adapter_supports` and the hard-coded `EXECUTOR_WRAPPER_SUPPORT` table
(`app/trading_intelligence/execution/adapter.py`, `binance_adapter.py`).

`record_adapter_version` must be called by whatever constructs adapters at start-up for a version change to
be recorded; until that is wired, a version change is caught only when a caller passes `adapter_version`.

## 6. Evidence

| Owner's field | Where |
|---|---|
| exchange, environment | `cati_adapter_cert_events.venue`, `.environment` |
| adapter_version, contract version | `.adapter_version`, `.contract_version` |
| certification_run_id | `.run_id` |
| authorized_scope, owner authorisation | `.authorized_scope`; `document.owner_authorization_ref` of the RUN_STARTED event |
| test_account_reference | `.account_ref` |
| started_at, completed_at, expiration_date | `.started_at`, `.completed_at`, `.expires_at` |
| test_cases, pass_fail_status, failure_reasons | `document.test_cases`, `document.outcome`, `document.failure_reasons` |
| exchange_capabilities | `document.exchange_capabilities` |
| evidence_hashes | `document.evidence_hashes` (one per check) and `document.actions_hash` |
| source_commit | `.source_commit` (supplied by the caller; the runner does not run git) |

All three tables are append-only (triggers). The event table is one hash chain; `verify_chain` and
`verify_run` are run on every status read, so an edited event or an edited check answers NOT_CERTIFIED.

No credential, stream address, listen key or signature is stored. `USER_STREAM_SPEC` records the
authentication mode and the keep-alive figures only.

## 7. Rollback

* In-process failure: cleanup runs in `finally`. It cancels every order of the run that is still open and
  closes, reduce-only, the smaller of (bought minus sold by the run's own orders, less any outside reduction)
  and the venue's position. It needs only what looking an order up needs.
* Cleanup that cannot finish: the result carries `cleanup_complete=False` and the failure reason
  `CLEANUP_INCOMPLETE_OPERATOR_ACTION_REQUIRED`. The operator closes the position named by the last
  `PLACE_ORDER` intents of the run in `cati_adapter_cert_actions`.
* Dead process: `CertificationRegister.open_runs()` lists runs that started and never completed.
  `CertificationRunner.recover(db, adapter, run_id=..., account_ref=..., permit=..., clock=...)` rebuilds the
  run's orders from its recorded intents, flattens, verifies and completes the run. It needs a valid permit
  for the same venue, environment, account and symbol, and it can never certify.

## 8. What the tests prove, and what they do not

Proven (51 tests, simulated venue): the permit rules; that nothing is sent without permission or on an
account that is not flat on the symbol; the order of the run; the audit trail; cleanup after failure,
including an adapter that created a second order for a repeated id; recovery after a dead process; the
90-day rule; version rules; revocation and blocking; tamper detection; that the outcome cannot be asserted
by a caller; that LIVE cannot be certified; that no database transaction is open during a venue call;
additive and idempotent schema, including completion after an interrupted application.

Not proven: anything about Binance, Bybit or BingX. No real adapter was run through the catalogue, and the
suite now includes local real-HTTP fault tests; no genuine venue certification is claimed.

## 9. To certify a venue for real

1. The owner authorises a DEMO account, a symbol and a notional bound, and names the operator.
2. Use `CertificationHarness`, which installs `HTTPFaultInjector` underneath the shared HTTP budget. It can lose an acknowledgement after one send, interrupt before send, inject 429 with Retry-After, and re-sign skewed timestamps. Local HTTP tests verify these behaviours; the owner-scoped venue run must still exercise the complete catalogue.
3. An adapter factory for the same account is supplied (a second adapter object built from the same
   connected account).
4. The outside reduction of `MANUAL_POSITION_CHANGE` is made by the injector or by the operator in the
   venue's own interface, for exactly one unit.
5. The operator runs the runner with `allow_orders=True` and keeps the returned `run_id`.

For BingX, whether a safe demo environment supports the required orders is the subject of Step 4.6. Until
that is established the honest record is `block(BINGX, <environment>, reason=...)`, not a run.

Integration update, 16 October 2026: the harness supplies the independent scoped external reduction and fresh-adapter factory. Its raw order permit must match the current DEMO run, owner authorization, pending action, account, symbol, order and quantity. It is serialized by the canonical broker-cycle lock, retains normal order flags, and has no automatic runtime trigger. See [carry-forward gates](../implementation/STEP3_STEP4_CARRY_FORWARD.md).

## 10. Harness readiness for an authorised DEMO run (Section 28, 2026-10-31)

**Venue certification: PENDING for binance, bybit and bingx, DEMO and LIVE.** No real run has been made. The
register holds nothing decisive. Nothing in this section is evidence about a venue.

### 10.1 The operator entry point

`scripts/run_venue_certification.py` runs the harness without any code change.

| Mode | Command | What it does |
|---|---|---|
| dry run (the default) | `python scripts/run_venue_certification.py --venue <v> --dry-run [--out-dir <dir>]` | a preflight of a real run's settings (by name only), then the WHOLE catalogue over a mocked transport in a scratch database: the real raw client, adapter, `BudgetedHTTP`, `HTTPFaultInjector`, runner, register, entry permit (production order gate on) and canonical runtime lease. No network. Records neither CERTIFIED nor FAILED |
| real run | `python scripts/run_venue_certification.py --venue <v> --execute --environment DEMO --account-id <brk_...> --user-id <uuid> --operator <name> --authorization-ref <owner reference> --scope "<words>" --symbol BTCUSDT --max-notional <quote> [--allow-orders] [--leverage 1] [--valid-minutes 60] --out-dir <dir>` | resolves the connected DEMO account, builds an `OperatorPermit` (at most 120 minutes), takes the canonical runtime lease when orders are allowed, runs, releases the lease, writes the evidence |

Refusals before anything is built: `--environment` accepts only `DEMO` (LIVE cannot be named); missing
account, user, operator, authorisation reference, scope, notional or output directory; a preflight problem
(below); an account whose stored environment is not DEMO or that belongs to another venue. The harness then
refuses again: a permit that is not DEMO, a permit for another account, venue or environment, an account
whose host is not the venue's canonical DEMO host (`ACCOUNT_IS_NOT_ON_THE_DEMO_HOST`), an expired permit, a
mocked transport with a real account or a real transport with a rehearsal account, and orders without the
canonical lease. The runner and the register refuse LIVE a third and fourth time.

### 10.2 What a real run needs, per venue (`certification_harness.VENUE_REQUIREMENTS`)

| | binance | bybit | bingx |
|---|---|---|---|
| REST host (only this one is accepted) | `https://demo-fapi.binance.com` | `https://api-demo.bybit.com` | `https://open-api-vst.bingx.com` |
| Credentials | the connected DEMO account (`broker_accounts` / `broker_credentials`), resolved by `resolve_broker_auth(account_id, user_id, db)`; decrypted with `BROKER_SECRET_KEY`; database `DATABASE_URL`. Never passed on the command line, never printed | same | same |
| Order flag | `DEMO_ORDER_SUBMISSION_ENABLED` on for `--allow-orders` (never changed by the harness) | same | same |
| Account settings | one-way (`dualSidePosition` false); flat, no open order on the symbol; key without withdrawal permission | one-way (positionIdx 0); margin mode is an account setting (UTA 2.0) | one-way (`dualSidePosition` "false"); key permissions not inspectable: the operator records how the key was made |
| Instrument | `BTCUSDT` by default; any canonical symbol the venue lists as TRADING | same | same (venue form `BTC-USDT`; `apiStateOpen` true) |
| Test size | the minimum order at a price 5% under the mark; two units with the fault injector; `max_notional` must hold twice the entry (about 2 x 2 x minimum notional) | same | same |
| Leverage | 1 (left at 1 afterwards) | 1 | 1 (`side=BOTH`) |
| Stop | conditional (Algo Service) STOP_MARKET, reduceOnly + quantity | the position's own stop (trading-stop, Full) | STOP_MARKET, reduceOnly + quantity, no client id |
| PROTECTION_REPLACE method | CREATE_THEN_CANCEL | AMEND_IN_PLACE | REPLACEMENT_NOT_POSSIBLE_SAFELY (proves the refusal; STOP_REPLACEMENT never certified) |
| Private stream | `wss://demo-fstream.binance.com/private/ws/<key>`: the `/private` route on demo is NOT documented | `wss://stream-demo.bybit.com/v5/private` (documented) | none on VST: USER_STREAM_SPEC is NOT_SUPPORTED |
| Unverified on demo | the Algo Service on the demo host; -4130; the conditional-order limit | atomicity of a Full-mode change; Partial mode (not used) | every answer and code; second stop; stop after close |

### 10.3 Scenarios (unchanged catalogue, 30 checks)

Read checks; test scope; leverage and one-way mode; a venue rejection (reduce-only on a flat account); entry;
lookup by both ids; fill agreement; idempotent repeat; stop create, read-back, geometry; stop replacement
(amend, or create-then-cancel; for a venue with neither, the refusal and the survival of the first stop);
unknown-submission reconciliation; cancel and repeated cancel; lost acknowledgement; interrupted connection;
injected 429; clock drift (re-signed skewed timestamps); a fresh adapter after a "restart" (finding the stop
by the venue order id where the venue keeps no client id); an outside reduction through a separately built
client; close; removal of the obsolete stop; flatten; reconciliation; final state.

Cleanup: in `finally` the run cancels every open order of its own and closes, reduce-only, only what its own
orders left open (the outside reduction is journalled as `EXTERNAL_REDUCE` and not counted as an own order).
A dead process is recovered with `CertificationRunner.recover`.

Pacing: the platform's own HTTP budget refuses a request before it is sent when its priority class has used
its share (the Binance policy charges 100 units per request against 2400 per minute, so a whole run cannot fit
in one minute). The harness waits such a local refusal out and asks again, at most 300 s per call. A venue's
refusal (429, 418, Bybit 10006) and a ban are never retried. A real Binance run therefore takes several minutes.

### 10.4 Evidence

The `--out-dir` file is one JSON document (`certification_harness.evidence_document`): `mocked_transport`,
`venue_contacted`, venue, environment, `run_id`, `outcome`, `recorded_status`, `operations`,
`failure_reasons`, `cleanup_complete`, `checks`, `source_commit`, the permit's references, and `register`:
every event (hash-chained), every check with its evidence and evidence hash, and every INTENT / OUTCOME
action, exactly as the register holds them. No credential, signature, listen key or stream URL is in it.
Write it outside `docs/research` (the script refuses that tree).

### 10.5 What the mocked end-to-end run proves, and what it found

`tests/exchange/test_certification_harness.py::test_the_full_harness_runs_end_to_end_over_a_mocked_transport`
runs the catalogue for all three venues with the production order gate on: every core check passes, the
account ends flat with nothing resting, and the scratch register still answers NOT_CERTIFIED. A rehearsal
account (`rehearsal-` prefix) can never record CERTIFIED or FAILED in any register.

It found five harness defects that would have failed or endangered a real run, all fixed:

1. BingX entries were refused by the entry permit (it did not read the `clientOrderId` key BingX sends);
2. the outside reduction was counted twice (as an own order and as an outside change), so RECONCILIATION failed;
3. the fresh adapter looked the stop up by a client id the venue does not store (Bybit, BingX);
4. fills were asked for by a stop id that has none (Bybit's position stop, Binance's conditional orders);
5. the local HTTP budget refused the entry on Binance and BingX mid-run (now waited out, 10.3).

And, in the runner: a venue with neither an amend nor a second stop was "replaced" by cancel-then-create,
leaving the demo position unprotected and certifying STOP_REPLACEMENT. It now proves the refusal instead.

What it does not prove: anything about Binance, Bybit or BingX. The answers behind the transport are the
fake venue's, rendered in the shapes the documentation describes.
