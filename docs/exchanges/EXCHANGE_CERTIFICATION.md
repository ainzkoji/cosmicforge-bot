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
stop 2.5% under the entry (amend in place, or create-then-cancel, or cancel-then-create, whichever the venue's
capabilities allow, recorded as `method`); lookup of an id never sent; a resting limit order 5% under the
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
suite contains no real fault injector.

## 9. To certify a venue for real

1. The owner authorises a DEMO account, a symbol and a notional bound, and names the operator.
2. A `FaultInjector` is implemented on the adapter's HTTP transport (lose an answer, drop a request, return a
   429 with Retry-After, shift the request timestamp). It does not exist yet; without it the result is
   INCOMPLETE.
3. An adapter factory for the same account is supplied (a second adapter object built from the same
   connected account).
4. The outside reduction of `MANUAL_POSITION_CHANGE` is made by the injector or by the operator in the
   venue's own interface, for exactly one unit.
5. The operator runs the runner with `allow_orders=True` and keeps the returned `run_id`.

For BingX, whether a safe demo environment supports the required orders is the subject of Step 4.6. Until
that is established the honest record is `block(BINGX, <environment>, reason=...)`, not a run.
