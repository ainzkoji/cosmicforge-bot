# Step 3.7 and 3.10: per-account portfolio, reservation ledger, risk brakes

Status on 2026-10-09: implemented and tested on Windows. Not run on Linux, not wired into a cycle driver or an
execution stage, and never run against an exchange. Trading stays shadow only; no flag, limit, governance phase
or environment setting was changed.

| Part | File |
|---|---|
| Portfolio authorization (pure) | `backends/bot-backend/app/trading_intelligence/pipeline/portfolio.py` |
| Reservation ledger | `backends/bot-backend/app/trading_intelligence/pipeline/reservations.py` |
| Risk brakes | `backends/bot-backend/app/trading_intelligence/pipeline/brakes.py` |
| Tables | `backends/shared/shared_lib/persistence/cati_portfolio_schema.py` |
| Tests | `backends/bot-backend/tests/trading_intelligence/test_step3_portfolio.py`, `test_step3_reservations.py`, `test_step3_brakes.py` |

Run them from `backends/bot-backend`:

```
../venv/Scripts/python.exe -m pytest tests/trading_intelligence/test_step3_portfolio.py tests/trading_intelligence/test_step3_reservations.py tests/trading_intelligence/test_step3_brakes.py -q
```

## 1. Where the numbers come from

No threshold is defined in the new code. Limits are read from `shared_lib/risk_levels.py` (`get_profile`) and
from `app/risk/system_limits.py` (`SystemLimits`).

| Profile | Per trade (approved) | Per trade (in force) | Open-risk cap | Positions | Leverage | Daily pause | Halve | Stop |
|---|---|---|---|---|---|---|---|---|
| Conservative | 0.25% | 0.25% | 1% | 4 | 1x | 1% | 5% | 10% |
| Balanced | 0.50% | 0.40% | 2% | 6 | 2x | 2% | 8% | 15% |
| Aggressive | 0.75% | 0.40% | 3% | 8 | 3x | 3% | 12% | 25% |

"In force" is `min(profile, SYSTEM_PER_TRADE_RISK_CEILING_PCT)`, the 0.40% engine clamp. The widest entry stop
is `SystemLimits.max_stop_loss_pct`, 15%. Tests assert that these values equal the frozen daily-trend family's
own (`families/daily_trend/targets.risk_level`), so the two cannot drift apart unnoticed.

Three values are stated in the new code because no authority holds them:

- `HALVE_FACTOR = 0.5` in `portfolio.py`: what "halve" means (Step 3.10; frozen interpretation I8).
- `MARGIN_FILL_BUFFER` is imported from `execution/risk_sizing.py` (0.15%), not redefined.
- A relative tolerance of `1e-9` on comparisons against a limit, the same as `pipeline/contracts.py`.

## 2. Portfolio authorization (3.7)

`authorize(snapshot, limits, intents, rules, *, brake_state) -> PortfolioAuthorization` decides, for one account
and from one immutable `AccountSnapshot`, which intents of a decision may be executed and at what quantity. It
has no database, clock or network access, so the same inputs always give the same result (and the same
`authorization_hash`).

### What it never does

- It never selects, ranks or invents a trade. Intents are processed in the order the family gave them.
- It never reduces an existing position. Only a family may state a reduction.
- It never clips or moves a stop to make an entry fit. An entry whose stop is more than 15% away is refused
  (`ENTRY_STOP_TOO_WIDE`). The only change to a stop price is putting it on the instrument's price tick, by
  less than one tick and toward the market, so the loss at the stop cannot grow.
- It never raises an order to reach an exchange minimum. Quantities are rounded down to the step.

### Order of processing

1. **Risk-reducing intents** (exit, reduce, rebalance down, stop replace). They pass every cap and every brake.
   A full close passes every exchange filter; a partial one is rounded down to the step and must meet the
   minimum quantity and notional. A stop replacement may not widen the resting stop.
2. **Gates** on each increase (entry, add, rebalance up): brake state, equity, instrument rules, position and
   reservation state, rebalancing band, the 15% stop limit, leverage, the stated per-trade risk.
3. **Per-intent ceilings**, which only make an order smaller: the per-trade risk clamp measured on the
   snapshot's equity, then the customer's maximum position.
4. **Position slots**, first come first served in the given order.
5. **Shared budgets.** Each scales every surviving increase of the decision by one common factor, in the frozen
   rule's order: open risk, then gross leverage, then available margin.
6. **Exchange filters**: step, maximum quantity, minimum quantity, minimum notional.

An exit or reduction releases its slot, risk, notional and margin to the increases of the same decision,
because the frozen rule replaces a position inside one decision. An increase that fits only thanks to such a
release is marked `awaits_releases`; the ledger will not let it be marked submitted until an account snapshot
taken afterwards shows the release.

### Refusal reasons

Each refused intent carries exactly one reason. Those the assignment names: `POSITION_LIMIT`, `OPEN_RISK_CAP`,
`LEVERAGE_CAP`, `INSUFFICIENT_MARGIN`, `RISK_ABOVE_CEILING`, `ENTRY_STOP_TOO_WIDE`, `BELOW_MIN_NOTIONAL`,
`CUSTOMER_MAX_POSITION`, `BRAKE_ACTIVE`. The others: `BELOW_MIN_QUANTITY`, `QUANTITY_ROUNDS_TO_ZERO`,
`BRAKE_STATE_UNKNOWN`, `EQUITY_UNAVAILABLE`, `INSTRUMENT_RULES_MISSING`, `INSTRUMENT_RULES_INCOMPLETE`,
`INSTRUMENT_NOT_TRADING`, `POSITION_EXISTS`, `NO_POSITION`, `DIRECTION_MISMATCH`, `DUPLICATE_RESERVATION`,
`LEGACY_CLAIM_UNSIZED`, `INSIDE_REBALANCE_BAND`, `STOP_WIDENING_REFUSED`, `INVALID_STOP_GEOMETRY`,
`CONTRADICTORY_ACTION`.

When a cap shrinks an order below an exchange minimum, the reason is the cap, not the minimum. An accepted
intent lists what made it smaller in `limited_by`.

### Open risk

`open_risk(snapshot)` is the sum over positions of `quantity x |mark - stop|`, as a fraction of equity. A
position whose stop is unknown, or lies on the wrong side of the mark, counts with its whole notional and is
listed in `unprotected`.

### Brakes inside the authorization

- `halted`: every increase is refused; `flatten_intents(snapshot)` gives the exits the terminal stop asks for.
- `paused`: new positions are refused; adds and rebalances of held positions are not.
- `halve`: the family halves its own targets. This layer enforces the bound that follows: no position may carry
  more than half the per-trade ceiling. A position sized above it is cut to it and reports `BRAKE_ACTIVE`.
- Brake state not supplied: every increase is refused.

### Three places where this layer can differ from the frozen rule

These matter for the shadow comparison of Step 3.6.

1. **Open risk is measured on what is held.** The frozen rule caps the sum of `target x d`. This layer caps
   measured risk (`quantity x |mark - stop|`) plus the new risk. When holdings have drifted inside the 25% band
   and the cap binds, new entries are scaled below the frozen target. A test proves the two agree for a decision
   on a flat account at all three levels; agreement with drifted holdings is not proven and is not expected.
2. **The margin fill buffer.** At exactly the leverage cap with instrument leverage equal to the profile's, the
   0.15% buffer scales the increases by 0.9985.
3. **The per-trade clamp uses the snapshot's equity.** If equity fell between decision and authorization, a
   full-strength entry is trimmed to 0.40% of current equity.

## 3. Reservation ledger (3.7)

`ReservationLedger.reserve(...)` runs the same authorization again inside one `BEGIN IMMEDIATE` transaction,
against what the ledger holds at that instant, and writes the accepted reservations before it commits. SQLite
admits one writer at a time, so the second of two concurrent callers sees the first one's rows.

### Tables

- `cati_reservation_ledger`: one row per (account, book, decision, symbol, kind).
- `cati_reservation_ledger_events`: append-only; one row per insert and per state change, written by triggers.

### Lifecycle

```
RESERVED -> SUBMITTED -> FILLED
   |            |-----> RELEASED   only with evidence that the venue holds no order
   |            '-----> UNKNOWN -> FILLED | RELEASED   only by resolve_unknown, with evidence
   |-> RELEASED   never sent
   |-> EXPIRED    never sent, its decision ran out
   '-> UNKNOWN    the outcome of a send is not known
```

Enforced by the database, whatever code runs:

- `UNKNOWN` leaves only to `FILLED` or `RELEASED`, and only with non-blank evidence. It never expires.
- `SUBMITTED` never expires and is released only with evidence.
- `FILLED`, `RELEASED`, `EXPIRED` are final.
- An open row cannot be deleted. Account, book, intent key, symbol, direction, kind and source are immutable.
- One open pipeline reservation per symbol of an account and book (partial unique index).

### What counts as committed

Rows in `RESERVED`, `SUBMITTED` and `UNKNOWN`, plus `FILLED` rows whose fill is not older than the account
snapshot in use. A reservation and the position it became count once.

### Books

`SHADOW` reservations and executing ones (`DEMO`, `LIVE`, `DEMO_CERTIFICATION`) do not consume each other's
capacity. Demo and live share one book.

### The brake latch inside `reserve`

`reserve` reads the persisted terminal stop in its own transaction and lets it override the flags the caller
passed. In the execution book it checks the whole account (`ACCOUNT_SCOPE`) and the `brake_scope` the caller
names. In the shadow book it checks only the named `brake_scope`: a shadow book has its own equity and its own
brake state, so a stop of the executing account does not stop it.

### Calling order for the execution stage

1. `reserve(batch, snapshot=..., limits=..., rules=..., brake_state=..., now_ms=...)`.
2. `mark_submitted(...)` **before** the order leaves; send only if it returns `ok`. For a reservation with
   `awaits_releases`, pass a snapshot taken after the exit.
3. `mark_filled(...)` on a fill, `release(..., evidence=...)` on a definitive rejection,
   `mark_unknown(...)` when the outcome is not known.
4. `resolve_unknown(..., evidence=...)` from reconciliation that asked the venue.
5. After a restart: `unresolved()` lists every `SUBMITTED` and `UNKNOWN` row to look up first.

Two behaviours to know:

- An intent whose reservation ended `RELEASED` or `EXPIRED` cannot be reserved again under the same decision
  (`INTENT_ALREADY_FINAL`). A retry needs a new decision id.
- Every method takes the account id. A reservation id of another account is reported as not found.

## 4. The five earlier stores

| # | Store | Class | Evidence | What the ledger does |
|---|---|---|---|---|
| 1 | `cati_portfolio_reservations` | Authoritative for the legacy one-position path; historical otherwise | Durable table (`cati_schema.py:36-65`); the live path and the demo certification reserve in it (`production_execution.py:950-953`, `demo_boundary_certification.py:139`); its `PRODUCTION` rows hold the single slot (`reservation_store.py:238-246`, `production_portfolio.py:35-59`) | Mirrors open `PRODUCTION` rows and rows with a `LIVE` trade plan. `SHADOW` rows are not mirrored: the store itself says they hold no execution capacity (`reservation_store.py:6-8, 243-244`) |
| 2 | `pending_entries` | Authoritative for an in-flight legacy entry | Durable table (`entry_protection.py:66-93`); the row is the per-bot lock and is deleted when the entry fails or the position closes (`:706-764`) | Mirrors every row of the account's bots |
| 3 | `position_slots` | Derived | No table: `occupied_slots` reads `positions` and `pending_entries` (`position_slots.py:55-67`) | Nothing to mirror; covered through store 2 |
| 4 | `AccountMarginReservations` | Volatile | A dictionary in process memory, "process-local by design" (`capital_ledger.py:425-559`), used by the executor (`executor.py:1579-1621`) | Cannot be read from a database. Named in `StoreReconciliation.not_inspected` |
| 5 | `AdaptiveDailyRiskBudgetEngine._reservations` | Volatile and inert | A dictionary in process memory (`adaptive_daily_budget.py:131`); its `reserve` is called only by the offline replay (`adaptive_daily_replay.py:432`) | As store 4 |

A mirror counts as: one position slot; risk at the per-trade ceiling (the most one legacy entry may risk); the
notional its store states. A claim whose store states no notional and that is not yet a position blocks every
increase on the account (`LEGACY_CLAIM_UNSIZED`). In practice that is the short interval between a legacy
reservation and its entry lock.

A mirror follows its legacy row and cannot be moved through the ledger's own methods
(`LEGACY_ROW_FOLLOWS_ITS_STORE`). One exception protects the invariant: a mirror in `UNKNOWN` whose legacy row
has disappeared stays `UNKNOWN`, because a vanished row proves nothing about the order
(`EntryProtection.clear_all_for_bot` deletes rows unconditionally, `entry_protection.py:1024-1027`). It is
ended only by `resolve_unknown` with evidence.

### Transition procedure

1. Apply the schema. `migrate()` now does it (step 48g calls `ensure_cati_platform_schema`). The part defined
   in `cati_portfolio_schema.py` creates five tables with their indexes and triggers and touches nothing else.
2. Run `import_legacy_reservations(db, account_id, now_ms=...)` per account. It is idempotent and writes no
   legacy row. `reserve` also runs it at the start of every transaction, so the ledger is never behind.
3. Run `reconcile_stores(db, account_id, now_ms=...)`. It is read only and reports
   `LEGACY_CLAIM_NOT_MIRRORED`, `MIRROR_STATE_DIFFERS`, `MIRROR_WITHOUT_LEGACY_ROW`, `SYMBOL_CLAIMED_TWICE`,
   `UNKNOWN_PAST_DEADLINE`, `RESERVED_PAST_EXPIRY`.
4. Legacy code keeps running unchanged, with its own recovery (`pending_resolutions`, `resolve_pending`).

**Limit of the procedure.** The ledger sees legacy claims; the legacy path does not see the ledger, and could
not be taught to without editing it. One account must therefore be driven by one path at a time. Nothing in
these files enforces that.

### Rollback

The schema is additive, so an older build runs on an upgraded database unchanged. To abandon the ledger:
stop the new pipeline, resolve every row `unresolved()` returns against the venue, then leave the tables in
place. Dropping them while a row is `SUBMITTED` or `UNKNOWN` would discard the only record that an order may
exist. A stopped account's row in `cati_brake_state` must likewise be kept.

A later change to a trigger body needs an explicit step in `upgrade(conn)`: `CREATE TRIGGER IF NOT EXISTS`
does not replace an existing trigger.

## 5. Risk brakes (3.10)

`evaluate(db, *, account_id, scope, risk_day, day_definition, closing_equity, thresholds, now_ms,
initial_equity=None, net_transfers=0.0)` is called once per risk day at the decision close.

```
daily_return = closing / opening - 1            opening = the previous closing equity
highest      = max(highest, closing)
drawdown     = closing / highest - 1
paused       = daily_return <= -daily_pause
halve        = drawdown     <= -halve_drawdown
halted       = halted or drawdown <= -stop_drawdown or closing <= 0
```

This is the frozen evaluator's arithmetic (`research/evaluator/simulator.py`, step 6). There is no option that
turns a brake off.

- **State**: one row per (account, scope) in `cati_brake_state`: risk day, opening and closing equity, highest
  closing equity, daily return, daily loss, drawdown, the three flags, active brake, reason, trigger time and
  day, restart authorization. Every evaluation and every change appends to `cati_brake_events`.
- **Scope**: the bot or family whose equity the caller feeds, or `ACCOUNT_SCOPE`.
- **Idempotence**: a second evaluation of a risk day with the same equity returns the first result and writes
  nothing; with a different equity it is refused. Days only move forward.
- **Skipped days**: the daily return is measured from the last evaluated close. If a risk day is not evaluated,
  the next evaluation's return covers the gap. The caller is expected to evaluate every day.
- **Risk day**: an explicit argument. `risk_day_closed_at(close_ms, UTC_DAY)` is the daily trend's day (the UTC
  bar that just closed). `risk_day_containing(now_ms, settings.ADAPTIVE_DAILY_RISK_TIMEZONE)` is the existing
  production path's day. A state keeps the definition it was opened with.
- **Capital flows**: `net_transfers` shifts the opening equity and the highest closing equity by the same
  amount, as the existing account-period rule does (`production_execution.py:78`), so a deposit is not a gain
  and a withdrawal is not a loss.
- **Equity** is supplied by the caller: wallet balance plus unrealised result at the decision close. This
  module has no exchange access and does not derive it.

### Terminal stop and restart

The stop is a persisted column. The database refuses to clear it unless the row names a pending restart record
made for that stop, and refuses to delete a stopped row.

`authorize_restart(db, *, account_id, scope, authorized_by, role, reference, now_ms)` writes that record
(`cati_brake_restarts`, append-only) with the identity of the user or operator. It does not lift the stop. The
next decision close tests the stop threshold again:

- drawdown still at or beyond the threshold: the account stays stopped and the authorization is used up;
- otherwise: the stop is lifted, and the halve brake applies as the preserved drawdown dictates.

**The highest closing equity is kept across a restart.** The approved policy does not say what a restart does
to it, so the stricter reading is implemented. Its consequence is plain: an account that stopped at its
threshold and received no new capital stays stopped after a restart, because its drawdown is unchanged. See
owner decision 1.

### Existing brake pieces, unchanged

| Table | Role | Relation to the new state |
|---|---|---|
| `cati_production_daily_risk` | Daily loss latch of the legacy path (`production_schema.py:49-51`, `production_execution.py:251-266`) | Authoritative for that path. Not read or written here |
| `cati_account_period_risk` | Weekly and monthly peak equity of the legacy path (`production_schema.py:61-62`) | As above. Its treatment of transfers is the one `net_transfers` follows |

## 6. What the tests prove

157 tests, all passing on Windows 11, Python 3.12.

| Claim | Test (file: name contains) |
|---|---|
| Limits equal the profile authority and the frozen family | portfolio: `limits_are_the_shared_profile`, `limits_equal_the_frozen` |
| 15% stop refuses, never clips | portfolio: `wider_than_the_engine_limit` |
| Each cap, with common-factor scaling | portfolio: `open_risk_cap_scales`, `leverage_cap_limits`, `available_margin_is_account_wide`, `budgets_apply_in_the_frozen_order` |
| Reductions pass every cap and brake | portfolio: `risk_reducing_intents_pass` |
| A frozen decision on a flat account passes unchanged | portfolio: `frozen_daily_trend_decision_passes_unchanged` |
| Concurrent decisions cannot jointly exceed positions, open risk, leverage or margin | reservations: `concurrent_decisions_cannot_jointly_exceed` (threads), `separate_processes_cannot_jointly_exceed` (four OS processes) |
| No double reservation, no concurrent duplicate entry | reservations: `same_intent_reserved_concurrently`, `concurrent_entries_on_one_symbol` |
| Multi-account isolation | reservations: `accounts_reserve_concurrently`, `cannot_read_move_or_use_another_accounts` |
| An unknown outcome is never released or expired | reservations: `unknown_outcome_is_never_released_or_expired` |
| Schema: twice, on legacy rows, after an interruption | reservations: `schema_upgrades_a_database`, `interrupted_schema_run` |
| Legacy rows untouched; legacy recovery still works | reservations: `imported_once_and_the_legacy_rows_are_left_alone`, `mirror_follows_its_legacy_store` |
| Each brake at its exact threshold, one ulp either side | brakes: `fires_at_its_exact_threshold`, `crosses_each_threshold_between_two_adjacent`, `exact_equality_where_the_threshold_is_representable` |
| Same flags as the frozen evaluator, day by day | brakes: `crafted_equity_path`, `random_equity_paths` |
| A stop survives a restart of the process | brakes: `stays_stopped_after_the_process_restarts`, `different_process_finds_the_account_stopped` |
| A stop is cleared only by a restart record | brakes: `refuses_to_clear_or_delete_a_stop`, `restart_needs_a_stopped_account_an_identity` |

On exact thresholds: for a threshold that is not a power of two (all but the aggressive 25% stop), no pair of
equities gives a drawdown exactly equal to it in binary floating point. Equality is therefore tested on the
comparison itself, and end to end as the crossing between two adjacent representable equities.

### Not proven

- Nothing was run on Linux.
- Nothing calls these modules yet: no cycle driver, no execution stage, no real account equity or transfer feed.
- An upgrade from a database file produced by an older release was not tested. The upgrade tests build the
  earlier schema by running today's `migrate()` with the platform-schema step (48g) disabled, then insert
  legacy reservations through the legacy store's own code, then upgrade.
- Agreement with the frozen rule when holdings have drifted (section 2).
- The two in-memory stores cannot be reconciled from a database.
- Scale: the largest run is 18 concurrent reservations on 3 accounts. Every `reserve` holds the database's one
  write lock while it runs; throughput at the planned platform scale was not measured.

## 7. Owner decisions

1. **High-water mark at a restart.** Implemented: kept. Alternative: reset it to the equity at the restart, so
   the account trades again at full size and may lose the stop percentage once more. The change is small and
   confined to `brakes.evaluate`; it is a risk decision, not a technical one.
2. **Capital flows and the high-water mark.** Implemented: the existing additive rule. With it, a deposit
   reduces the drawdown percentage (10 000 down to 8 500, plus 3 000 deposited, reads as 11.5% rather than 15%),
   which is how a restarted account can come back inside its stop. A unitised treatment would leave the
   percentage unchanged by deposits, and then a restart could never take effect under decision 1 as implemented.
3. **Open-risk measure for the cap**: measured on holdings (implemented, as specified) or on targets as the
   frozen rule does. It decides whether live sizing can fall below the frozen targets.
4. **The margin fill buffer in the shadow book**: applied (implemented) or not.
5. **One path per account during the transition**: who enforces that the legacy one-position path and the new
   pipeline never drive the same account.
