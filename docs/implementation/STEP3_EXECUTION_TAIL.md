# Step 3.4: the unified execution tail

Updated integration status (16 October 2026): [A–N validation](STEP3_STEP4_VALIDATION.md) and [requirement register](STEP3_STEP4_IMPLEMENTATION_STATUS.md). The baseline audit sections below describe the earlier implementation; runtime handoff, shared budgets, account-close brakes and the concrete certification harness are now wired. Actual research, venue and elapsed-time gates remain separate.

Status on 2026-10-09: implemented and tested on Windows. Not run on Linux, never run against an exchange.
Trading stays shadow only: no flag, limit, governance phase or environment setting was changed and no family was
made executable. Line numbers are those of the working tree on that day; several of the cited files were being
edited by other workstreams at the same time (`production_runtime.py`, `pipeline/provenance.py`), so a function
name is given with every number.

| Part | File |
|---|---|
| Evidence evaluators (pure) | `backends/bot-backend/app/trading_intelligence/pipeline/readiness.py` |
| The tail that uses them | `backends/bot-backend/app/trading_intelligence/integration/production_execution.py` (`build_plan`, `prepare_submission`) |
| Operator tool kept apart | `backends/bot-backend/app/execution/demo_transport_smoke.py` |
| Tests | `backends/bot-backend/tests/trading_intelligence/test_step3_execution_tail.py` |

```
cd backends/bot-backend
../venv/Scripts/python.exe -m pytest tests/trading_intelligence/test_step3_execution_tail.py -q
```

## 1. What changed

1. **Placeholder evidence is gone.** `prepare_submission` passed
   `BrokerHealthContext(..., 'HEALTHY', now, 'BROKER_SYNC')` and `CapitalReadiness(True, 'LOGICAL')` whatever
   the cycle had seen. Both are now verdicts of `pipeline/readiness.py` over evidence the cycle already holds
   (sections 4 and 5). No venue call was added.
2. **The plan mode is the account's environment.** `build_plan` set `mode="LIVE"` on every account; it now
   sets `DEMO` on a demo account and `LIVE` on a live one (section 6).
3. **The one order sender outside the boundary is fenced.** `demo_transport_smoke` refuses to act inside a
   strategy submission and on an account with unresolved strategy execution (section 3).
4. **The verdicts are recorded.** `prepare_submission` adds `execution_readiness` (both verdicts, their reason
   codes and the values they were reached on) to the evidence it already returned as `controls_evidence`; the
   cycle stores it with its evaluation (`result["risk_controls"]`, `cati_execution_evaluations`,
   `cati_production_decisions`) and the certification harness with its report. The risk path receives the
   controls unchanged.

No existing test was edited.

## 2. One authoritative implementation: the audit

Paths examined: the production tail (`production_runtime.broker_cycle` -> `production_execution.process_account`),
the legacy runner (`app/runner/runner.py`, `multi_runner.py`), the full CATI pipeline (`cycle_shadow` ->
`trade_plan/builder.py` -> `cati_dispatch`), the certification harness (`demo_boundary_certification`), the
transport smoke (`demo_transport_smoke`), and the Step 3 cycle driver (`pipeline/driver.py`).

| Item | Authoritative today | Other implementations and how they differ | Unified here |
|---|---|---|---|
| Trade-plan construction | `production_execution.build_plan` (:349), the only builder whose plans can reach an order | `trade_plan/builder.py` (`TradePlan.build`, :290-315) builds the full pipeline's plans without a mode, so they are `SHADOW` (`contracts/trade_plan.py:278`) and the gate refuses them. `demo_boundary_certification.build_plan` (:34) builds the harness plan, labelled `DEMO_CERTIFICATION`, not a family. | Mode only (section 6). The builders stay separate: they transport different decisions. |
| Plan validation | `trade_plan/validation.validate_trade_plan_for_submission` (:41) and `verify_trade_plan_integrity` (:123), called once, by `TradingOrchestrator.process_trade_plan` (`core/trading_orchestrator.py:326`, :339) | None. Every path that reaches hard risk goes through that call. | Nothing to unify. The broker-health input of the validator (:88) is now evidence instead of a constant. |
| Governance authorization | The boundary: the family gate `pipeline/provenance.FamilyExecutionGate.authorize` (asked at `boundary.py:205` and again at :342) and the account authority `authority.authorize_entry` (:218, :329) | Two account authorities exist. The production tail wires `AccountExecutionAuthority` (`governance/account_authority.py:8`; wired in `boundary_for`, `production_execution.py:483`), which admits a consenting demo account at any promotion phase. `cati_dispatch.boundary_for` (:55-69) leaves the default `GovernanceAuthority` (`governance/promotion.py:205`, phase M6 or above) and routes first through `runtime_authority.resolve_order_authority` (`cati_dispatch.py:29-39`, :119). | Not unified (outside this workstream's files). Both sit behind the family gate, which admits no real family. |
| Portfolio-risk authorization | In order: `account_risk` (`production_execution.py:191`), `production_portfolio.execution_portfolio` / `durable_occupancy` (:62, :35), `CATIReservationStore.reserve(mode='PRODUCTION', max_open_positions=1)` (`reservation_store.py:191`; called at `production_execution.py:961`), then hard risk `TradingOrchestrator._evaluate_hard_risk` (:575) and the executor's capital ledger and margin reservation (`executor._authorize_capital`, `executor.py:438`; :1585) | `pipeline/portfolio.authorize` with `pipeline/reservations.py` and `pipeline/brakes.py` (Step 3.7, 3.10) is a second, multi-position implementation. It is not called by any execution path. The full pipeline uses `portfolio/service.py` with SHADOW reservations. | Not unified. The tail still enforces one position per account. |
| Boundary creation | `production_execution.boundary_for` (:396; constructs at :483) | `cati_dispatch.boundary_for` (:55-69) builds the same class from a legacy runner: no account authority, the runner's own orchestrator and executor. Tests build it through `tests/trading_intelligence/_exec.Harness.boundary`. | Not unified; the second factory is not reachable under the production profile (see section 3). |
| Submit / reconcile coordination | `CATIExecutionBoundary._process_trade_plan` (:182; claim :355, reservation hold :358, submit :372), `_settle` (:393), `reconcile_submit_unknown` (:452), `recover_pending` (:600); position truth in `production_execution.reconcile_executions` (:1150) and `_reconcile_attempt` (:1240); the caller is `maintain_account` (:588) | None for entries: `adapter.submit_entry` has one caller (`boundary.py:372`). The transport smoke coordinates its own order (section 3). | Unchanged. This is the Step 1 safety machinery. |
| State persistence | Plans `cati_trade_plans` (`trade_plan/evidence_store.py:21`), attempts and risk decisions `cati_execution_attempts` / `cati_risk_decisions` (`evidence/stores.py`), reservations `cati_portfolio_reservations` (`reservation_store.py:72`), fills, closes and protection `cati_production_fills` / `_closes` / `_protection` (`execution/production_schema.py`), evaluations `cati_execution_evaluations` (`integration/production_evidence.py`) | The full pipeline writes plans and reservations through the same store classes, as `SHADOW` rows. The smoke writes only `demo_transport_smoke`. The Step 3 driver writes its own decision and shadow-book tables. | Unchanged. |

Two behaviours differed between the production tail and the full pipeline's dispatch and were not chosen here:

- **Broker health.** `cati_dispatch._inputs` (:72-93) takes it from the legacy runner's circuit breaker
  (`context_adapters.broker_health_from_runner`). The production tail has no runner; it now evaluates the
  evidence of section 4. Two evidence models remain, one per caller.
- **Capital.** `cati_dispatch.capital_for` (:96-103) calls `capital.planner.capital_readiness` on the capital
  plan of the account-capital stage. The production tail now calls the same planner and the same readiness
  function (section 5). The inputs still differ: the pipeline requires the bot's whole per-trade allocation
  and applies the user's transfer settings; the tail requires the smallest admissible order and applies none.

## 3. Which paths can send an order

An opening order can leave the application through `executor.execute_signal` or through a client's transport.

- **The executor** refuses every entry that does not hold the boundary's call-scoped permit, in every profile:
  `executor.py:1079-1084` returns `CATI_BOUNDARY_PERMIT_REQUIRED`. The permit is issued only at
  `boundary.py:370-372`.
- **The transport**, under the production profile only, refuses a non-closing POST to the entry endpoint of
  Binance, Bybit or BingX unless the boundary's permit matches a durable `PENDING_SUBMIT` attempt, or, on a
  Binance demo account, the smoke's own permit is set (`shared_lib/core/production.py:137-165`). Outside the
  production profile this gate does nothing (`:142`).

| Path | Through the final boundary | Under the production profile | Can it open a position today |
|---|---|---|---|
| Production tail, natural entry | Yes (`production_execution.py:998`) | Runs every cycle (`production_runtime._sync_with_client`, :293) | No: the family gate refuses the residual family (research FAILED, not registered) |
| Certification harness `demo_boundary_certification.run` (:76) | Yes (:167), with its operator permit (`permitted`, :19) | Operator request file, picked up by `broker_cycle` (`production_runtime.py:494-495`) | Only with `DEMO_ORDER_SUBMISSION_ENABLED` (false) |
| Transport smoke `demo_transport_smoke.run` (:70) | **No**: raw `MARKET BUY` at :147 | Operator request file, picked up by `broker_cycle` (`production_runtime.py:492-493`) | Only with `DEMO_ORDER_SUBMISSION_ENABLED` (false); Binance demo only |
| Full CATI pipeline `cati_dispatch.dispatch_trade_plans` (:106) | Yes (:128) | Not reachable: it is driven by `PaperRunner`, and `MultiBotRunner.run_once` returns at once (`multi_runner.py:507-510`) | No: its plans are `SHADOW`, refused at `boundary.py:205` |
| Legacy runner | Not applicable | Not cycled; `get_runner` answers 410 (`main.py:731-732`) | No: BUY/SELL is blocked before the executor (`runner.py:2156-2159`, `LEGACY_ENTRY_PATH_REMOVED`), and the executor would refuse it anyway. `process_trading_opportunity` is retired (`trading_orchestrator.py:238-255`). |
| Step 3 cycle driver `pipeline/driver.py` | Not applicable | Shadow only: `run_cycle` fixes `mode = C.SHADOW` (:191); it holds no adapter, client or boundary | No |
| `venue_validation/demo_validation.py` (:218) | **No**: `client.place_order` with a market BUY | CLI only (`scripts/validate_demo_venue.py`, `--submit-demo-orders`); demo accounts only (:81-82). Under the production profile the transport refuses its order (no permit). | Outside the production profile, yes, on a demo account |
| `exchange/platform_adapter/certification.py` (`CertificationRunner._place`, :733) | **No**: `adapter.place_order` | No caller in `app/` or `scripts/`; tests only. Needs an operator permit with `allow_orders`; refuses LIVE (:202-203). Another workstream's file. | Not from the application |

`execution/production_close.py:270` also posts to the order endpoint. It is the durable close: the request is
built with `reduceOnly: 'true'` (:246-247), so it can only shrink a position.

Not changed, reported: `executor.execute_tp1_partial_close` sends its close with `reduce_only=supports_ro`
(`executor.py:2583-2594`). For a client that declares reduce-only unsupported the order is a plain market order.
Callers: `runner.py:3147`, `execution/binance_adapter.py:248`. The Binance, Bybit and BingX clients carry no
`capabilities` attribute, so for them the default (reduce-only enforced) applies; IBKR declares it unsupported
(`exchange/ibkr/capabilities.py:8`).

### The transport smoke

It is an operator tool and stays one. It was not deleted and its order is still raw. What was done, in
`demo_transport_smoke.py`:

- The module says what it is and is not (docstring): no family, no trade plan, no reservation, no attempt; rows
  in `demo_transport_smoke`, order ids `CFSMOKE...`, intent identity `TESTNET_SMOKE:...`.
- `_boundary_submission_active` (:32): true while the boundary's entry permit or the certification harness's
  permit is set in the calling context. `permitted` (:57) is then false, so the transport gate cannot be opened
  with the smoke's permit during a strategy submission, and `run` (:81) raises
  `DEMO_SMOKE_INSIDE_BOUNDARY_SUBMISSION` before it builds a client.
- A new smoke (:111-114) raises `DEMO_SMOKE_STRATEGY_EXECUTION_UNRESOLVED` before any venue read when
  `production_execution.open_lineage` reports an unresolved attempt, a position of a trade plan, a
  `RESOLUTION_PENDING` reservation or a pending close on the account. Before, an entry whose outcome was
  unknown on a flat account did not stop it. Resuming an existing smoke is not gated: it only verifies,
  protects and closes.

Limits of that fence. `broker_cycle` still calls the smoke at the start of a cycle, before `sync`; both run
under `_cycle_lock`, one after the other, which is what keeps them from interleaving. The smoke's fills are not
labelled in `cati_production_fills`, so its realized result counts in the account-wide daily-loss arithmetic of
`account_risk` like any manual trade (that is conservative).

## 4. Broker health: the evidence

`evaluate_broker_health(BrokerEvidence) -> BrokerHealthContext`. `HEALTHY` needs every positive condition. A
failure sets the worst status found (`UNAVAILABLE` > `UNKNOWN` > `DEGRADED`) and lists every reason. The
context's `environment` is the client's observed one, so the validator's cross-check against the plan
(`validation.py:92-95`) is no longer comparing the plan with itself.

| Condition | Evidence and where it comes from | When it fails |
|---|---|---|
| Account and venue named | `account['id']`, `plan.venue` | `UNKNOWN` `BROKER_IDENTITY_MISSING` |
| Client bound to the plan's environment | `normalize_environment(client.broker_environment)` against `plan.environment` | unreadable: `UNKNOWN` `BROKER_ENVIRONMENT_UNREADABLE`; different: `UNAVAILABLE` `BROKER_ENVIRONMENT_MISMATCH` |
| Account snapshot complete | `risk['equity']`, `['wallet']`, `['free_capital']`, `['margin_used']` from `account_risk` (the cycle's account document): all finite, equity and wallet positive | `UNKNOWN` `BROKER_ACCOUNT_SNAPSHOT_INCOMPLETE` |
| An authenticated read completed recently | `risk['income_ledger']['refreshed_at']`: when the income ledger was last read to completion (`income_ledger.ensure_covered`), a signed read of this account. Bound `MAX_AUTHENTICATED_READ_AGE_MS` = 330 s | absent: `UNKNOWN` `..._UNPROVEN`; more than 5 s ahead of now: `UNKNOWN` `..._FUTURE_DATED`; older: `DEGRADED` `BROKER_AUTHENTICATED_READ_STALE` |
| The venue answered in this preparation | the price `prepare_submission` has just read (`client.last_price`) and its time: finite, positive, within 30 s | `UNKNOWN` `BROKER_VENUE_READ_UNPROVEN` |
| No pending back-off | `binance.client.rate_limit_backoff_remaining(endpoint)`: the process-wide `_RATE_STATE` record of the canonical endpoint of the account's broker and environment (`resolve_base_url`), read after the last venue read of the preparation | no record (any venue but Binance, or an unresolvable endpoint): `UNKNOWN` `BROKER_RATE_LIMIT_STATE_UNREADABLE`; remaining > 0: `UNAVAILABLE` `BROKER_RATE_LIMIT_BACKOFF` |
| Request weight (can only withdraw health) | `client.last_used_weight_1m` against the client's own pacing threshold (`REQUEST_WEIGHT_LIMIT_1M` x `REQUEST_WEIGHT_PAUSE_FRACTION`) | at or above it: `DEGRADED` `BROKER_REQUEST_WEIGHT_NEAR_LIMIT`. Not reported by the venue: ignored. |

Why the back-off check matters: the client refuses reads during a `Retry-After` but never delays a mutation
(`binance/client.py`, `_pace_signed_read`), and `prepare_submission` swallows a failed depth read. Without the
check an entry could be posted to a venue that has just told this address to wait.

Candidates evaluated and how they ended:

- *Freshness of the account snapshot itself.* Not available. The snapshot's time (`observed_at`, set in
  `production_runtime._sync_with_client`) is not passed to `prepare_submission`, and `risk` carries no read time
  for the account document. The age that is measured is that of the income-ledger refresh. For a natural entry
  that refresh is forced in the same cycle (`income_force`, `production_execution.py:894-897`), so the age is the
  time spent inside the cycle; the account document was read slightly earlier. For the certification harness
  the refresh is not forced (`demo_boundary_certification.py:128`) and may be up to 300 s old
  (`income_ledger.REFRESH_INTERVAL_MS`), which is why the bound is 330 s and not tighter.
- *Server-time offset.* Not used. `client._time_offset_ms` is 0 both when the clock agrees and when the
  synchronisation never succeeded, so it cannot prove health. A signed read that succeeded does prove the clock
  was inside the receive window at that moment, and hard risk already measures drift with a live read
  (`BrokerHealthMonitor.check_health`, `trading_orchestrator.py:613`; that call existed before).
- *Whether the last authenticated read succeeded.* Used, as above. A failed account or income read raises in
  `account_risk`, and no entry is prepared at all.

## 5. Capital readiness: the evidence

`evaluate_capital_readiness(CapitalEvidence) -> CapitalReadiness`. It builds an `AccountCapitalState` from the
evidence and asks `capital.planner.plan_capital` and `capital_readiness`, the same two functions the full
pipeline uses. The first failing condition gives the reason; the boundary reports it as
`CAPITAL_NOT_READY:<reason>` (`boundary.py:242-251`).

| Condition | Evidence and where it comes from | Reason when it fails |
|---|---|---|
| The plan names the synchronised account | `plan.broker_account_id` against `account['id']` | `CAPITAL_ACCOUNT_MISMATCH` |
| No transfer is unresolved | count of `broker_transfer_requests` rows of the account whose status is not terminal (`transfers.models.TERMINAL`): planned, validating, submitting, submitted, confirmation pending, unknown, reconciliation required. One local read. | unreadable: `TRANSFER_STATE_UNREADABLE`; one or more: `ACCOUNT_RECONCILIATION_REQUIRED` (the only pending refusal) |
| The requirement is known | `minimum_order_margin(instrument.min_notional, leverage cap)`: the venue's minimum notional from the catalog record at the highest leverage hard risk may resolve for the bot and symbol (the orchestrator's own lookup: `effective_policy.max_leverage`, else `validated_config.requested_leverage`) | `CAPITAL_REQUIREMENT_UNKNOWN` |
| In-flight margin is known | `capital_ledger.ACCOUNT_RESERVATIONS.pending_margin(account id)`: margin of this process's entries the venue does not show yet | `IN_FLIGHT_MARGIN_UNREADABLE` |
| The wallet topology is known | `shared_lib.broker.wallets.topology_for_account(broker, None)` and its wallet for the product | `ACCOUNT_TOPOLOGY_UNKNOWN` (Bybit: the account mode is never read), `NO_WALLET_COLLATERALISES_PRODUCT` |
| Free margin covers the requirement | `risk['free_capital']` (the account document's `availableBalance`) minus in-flight margin | not a number: `TRADING_WALLET_BALANCE_UNKNOWN`; short: `INSUFFICIENT_FREE_MARGIN` |
| The plan's own valid reservation | the row `boundary.reservations.get(plan.portfolio_reservation_id)` returns: same id, `RESERVED`, `expires_at` after now, this account, this bot, mode `PRODUCTION` | `LOGICAL_RESERVATION_UNKNOWN`, `LOGICAL_RESERVATION_NOT_VALID`, `RESERVATION_PLAN_MISMATCH`, `RESERVATION_EXPIRED`, `RESERVATION_ACCOUNT_MISMATCH`, `RESERVATION_BOT_MISMATCH`, `RESERVATION_NOT_EXECUTION_SCOPED` |

**Why the margin requirement is a floor.** "The margin the plan needs at its leverage" does not exist when
capital is checked: the boundary checks capital before hard risk, and hard risk is where leverage and size are
resolved. The stages that do know the size do not refuse a shortfall, they shrink the entry: risk-based sizing
caps the notional at the free margin (`execution/risk_sizing.py`, step 5), and the executor reserves
`min(requested, 95% of free margin)` and blocks only below a minimum (`capital_ledger.AccountMarginReservations.reserve`;
`executor.py:1585-1591`). A requirement equal to the configured per-trade margin would therefore refuse entries
the existing authorities accept at a reduced size, and for a risk-based bot that figure is the whole budget
(`capital_ledger.per_trade_allocation_margin`), which free margin stops covering after the first loss. The
floor refuses only an entry that no later stage could have sent, and it is still a real check.

## 6. The plan mode

`build_plan` computes the environment once and uses it for both `environment` and `mode`
(`production_execution.py:349-393`). An unrecognised environment raises in `normalize_environment` as before.

The family gate requires `mode in (DEMO, LIVE)` and `mode == environment`
(`pipeline/provenance.FamilyExecutionGate.family_authorization`). With the previous constant a demo plan would
have been refused with `FAMILY_ENVIRONMENT_NOT_AUTHORIZED` once a family is certified; a test shows both.

`mode` is hashed, so on a demo account the plan id, the plan hash, the attempt id and the client order id of a
given decision differ from what they were before this change. Consumers checked:

| Consumer | Effect |
|---|---|
| Exactly-once per decision | Does not rest on the plan id. The reservation id is derived from account, bot and decision (`reservation_store.py:212`); a decision that has an attempt is refused with `CATI_DECISION_ALREADY_ATTEMPTED` (:230-235), and an unresolved attempt blocks through `durable_occupancy`. Tested. |
| Stored plans | Plans already in `cati_trade_plans` keep their stored mode and still verify. |
| `production_portfolio.durable_occupancy` (:39-41) and `pipeline/reservations._reservation_claims` (:358-364) | Both count a reservation when it is `PRODUCTION` **or** has a plan with `mode='LIVE'`. The tail always reserves in `PRODUCTION` mode (`production_execution.py:961-964`; `demo_boundary_certification.py:139-141`), so nothing changes for it. A non-`PRODUCTION` reservation carrying a `DEMO` plan is no longer caught by the second clause. See follow-ups. |
| Fill labels | `_reconcile_attempt` stores `plan.mode` as the fill's `purpose`. Natural demo fills are now labelled `DEMO` instead of `LIVE`. `certification_pnl` only looks for `DEMO_CERTIFICATION`; the read model passes the label through (`core/cati_read_model.py:288`). |
| Tests | None pinned the old mode or a plan id. |

## 7. What is proven and what is not

Proven by `test_step3_execution_tail.py` (273 passed, 2 skipped as not applicable), on Windows, against a mocked
exchange client:

- Every field of both evidence records, set to nothing, to text, to an object, to NaN, to infinity or to a
  boolean, yields not healthy / not ready with a reason. Each condition has its status and reason. Bounds are
  inclusive; the worst status wins.
- Through the real `process_account` cycle, boundary, orchestrator and executor: a venue back-off, a request
  weight at the pacing threshold, stale, absent, future-dated or incomplete account evidence, any unresolved
  transfer status, an unreadable transfer table, free margin under the floor and an invalid reservation each
  stop the entry before an attempt row exists and before the client is asked to place an order, and the reason
  is in the cycle's record. With complete evidence the same cycle places exactly one order.
- The certification harness gets the same evidence and is stopped by the same conditions.
- The preparation makes the same four venue reads as before (`exchange_info_cached`, `klines`, `last_price`,
  `depth`). `test_runtime_cost_budget.py` passes unchanged.
- The mode follows the environment; the real gate accepts the new mode and refuses the old one.
- The smoke's permit is dead inside a boundary or harness submission, and a new smoke is refused, before any
  venue read, while strategy execution is unresolved.

The end-to-end tests use the real gate class. Because that gate refuses every real family, they register the
plan's family in the test database on a synthetic passed hypothesis, promote it through the real governance
operations and stub the venue adapter's certification register. That is test scaffolding and says nothing
about any family.

Not proven:

- Nothing ran against a venue. The back-off, weight and price evidence were exercised with a mocked client and
  the real process-wide rate-limit state.
- Linux. The module uses no file time or path semantics, but only Windows was run.
- That the 330 s bound is right for operations. It is derived from the ledger's refresh interval, not measured.
- That the account document is as fresh as the ledger refresh (section 4).
- Bybit and BingX. They have no rate-limit record and Bybit's account mode is never read, so the tail reports
  them not healthy / not ready. They were already refused later as uncertified adapters; the refusal now comes
  earlier and with another reason code.
- Customer-facing text for the new codes (follow-ups).

## 8. Follow-ups outside this workstream's files

1. `production_portfolio.py:39-41` and `pipeline/reservations.py:358-359`: `p.mode='LIVE'` should read
   `p.mode IN ('LIVE','DEMO')`, so that an executing demo plan marks its reservation as occupying whatever the
   reservation's own mode.
2. `integration/reason_registry.py`: no entry for `PREVALIDATION_BROKER_DEGRADED`, `CAPITAL_NOT_READY:<reason>`
   or the codes of `readiness.py`. A customer currently sees "Unrecognised engine code" with an operator-review
   action, which is truthful but unhelpful. `tests/trading_intelligence/test_reason_registry.py` lists
   `BROKER_SYNC`, `HEALTHY` and `LOGICAL` as internal labels of `production_execution.py`; they no longer occur
   there.
3. `_process_account` could pass the snapshot's `observed_at` to `prepare_submission`, and
   `demo_boundary_certification.run` could force the ledger refresh as a natural entry does. Either would let
   the freshness bound be tightened.
4. The two account authorities, the two broker-health models and the capital inputs of section 2.
5. Step 3.7's portfolio authorization is not part of the tail.
6. `boundary.py:3-4` still says "The runner never calls it"; `cati_dispatch.py:128` does.
7. The transport gate is a no-op outside the production profile (`shared_lib/core/production.py:142`), which is
   what lets `venue_validation/demo_validation.py` post a demo order from the CLI.
8. `executor.execute_tp1_partial_close` and `reduce_only` (section 3).
