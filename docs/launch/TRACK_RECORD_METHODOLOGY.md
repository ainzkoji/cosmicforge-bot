# Track Record Methodology

Methodology version: **TR-METHOD-1.0**. This is the public description of how CosmicForge records, computes
and publishes performance. The code is `backends/bot-backend/app/golive/track_record.py`, the tables are in
`backends/shared/shared_lib/persistence/sql/track_record_schema.py`, and the public page is `/track-record`.

## Current status

- **No live track record exists.** Live trading has never happened, and every strategy family is uncertified
  (M0). The recorder refuses every `LIVE` row until a first authorized live date is configured
  (`LIVE_NOT_AUTHORIZED_YET`). The public API and page therefore show the Live tab as **Not yet available:
  "No authorized live trading has occurred"**.
- **Results are not audited.** Nobody has independently verified any figure published under this methodology.
  Every published document says `"audited": false`.
- Backtest, research-holdout, paper and demo results are labelled **"Simulated — not achieved trading
  results"**. They are never published as live results and never appear under the Live tab.

## What is recorded

One row per account, venue, strategy family and trading date (UTC):

| Field | Meaning |
|---|---|
| trading_date, venue, family_id, family_version | what traded, where, and which strategy version |
| account_class | `OPERATOR`, `BETA_INVITED`, `CUSTOMER`, or `SIMULATION` (only and always for backtest / research holdout) |
| risk_level | the deployment's risk level |
| source_kind | `BACKTEST`, `RESEARCH_HOLDOUT`, `PAPER`, `DEMO` or `LIVE` (enforced by a database CHECK) |
| starting_equity | equity at the start of the day, **before** that day's deposits and withdrawals |
| ending_equity | equity at the end of the day, after flows, fees and funding |
| deposits, withdrawals, fees | non-negative amounts for the day |
| funding | signed; positive means funding was received |
| slippage | signed cost, or empty when it could not be measured |
| realized / unrealized PnL, drawdown, gross exposure | as measured for the account |
| reconciliation_status | `RECONCILED`, `MISMATCH` or `PENDING` (orders and fills against the venue) |
| outage_minutes, strategy_suspended | platform outage minutes and whether the strategy was suspended |
| amendment_note | required on a correction; why the earlier row was wrong |
| account_ref | pseudonym: HMAC-SHA256 of the account id keyed with a server secret (`TRACK_RECORD_PSEUDONYM_KEY`). The account id itself is never stored; without the key the recorder refuses to record |
| evidence_ref | the evidence behind the row, e.g. the reconciliation run id |
| authorization_ref | for `LIVE` rows, the go-live approval id (required) |
| prev_hash, row_hash | the provenance chain (below) |

Rules enforced by the recorder and, where possible, by the database:

- Amounts are exact decimals; binary floating-point input is refused.
- A row dated in the future is refused. A second row for the same series and date is refused; it must be
  recorded as a correction.
- `LIVE` rows need a go-live authorization reference, a configured first authorized live date, and a trading
  date on or after it. A `LIVE` row can be marked publishable only when its status is `RECONCILED`. No row with
  status `MISMATCH` is ever publishable.

## Append-only ledger and corrections

Rows are never updated or deleted. The recorder has no edit path, and the database refuses `UPDATE` and
`DELETE` on the ledger and on the publications table with a trigger (SQLite and PostgreSQL). A correction is a
new row of the same series and trading date whose `supersedes_record_id` names the row it replaces, with an
amendment note. A row can be superseded only once (the latest correction is the one in force). Exports use
only rows that no other row supersedes.

## Return calculation

### Daily return and the flow convention

For each account and day, with `D` deposits and `W` withdrawals:

    r_t = (E_end - E_start - (D - W)) / (E_start + D_weight),   D_weight = (D - W) x w,   w = 1

**Flows are treated as occurring at the start of the trading day (w = 1).** Reasons:

1. The engine sizes positions from the equity it sees at the start of its cycle, so money deposited during a
   day is in practice available to (and at risk in) that day's trading, and money withdrawn is not.
2. Venue statements do not reliably time-stamp flows; a time-weighted fraction (modified Dietz) would depend on
   timestamps that cannot be verified. A fixed, documented convention is reproducible from the record.
3. It is conservative for deposits: the deposit enlarges the denominator, so a deposit can never raise the
   measured return.

The flow is removed from the numerator, so **a deposit or a withdrawal on its own is a 0 % return**. Example:
1,010 at the start of the day, 10,000 deposited, 11,010 at the end gives r = 0, not +990 %.

If `E_start + D - W` is zero or negative the day has no capital at risk; it is excluded from the index and
counted (`excluded_zero_capital_days`).

Returns are **net of fees and funding**, because both are already reflected in ending equity.

### Time-weighted return and drawdown

Daily returns are chain-linked into an index that starts at 1:

    I_t = I_{t-1} x (1 + r_t),   TWR = I_T - 1

Maximum drawdown is measured on the index, not on equity, so deposits and withdrawals cannot create or hide a
drawdown:

    MDD = max_t (1 - I_t / max(1, I_1, ..., I_t))

### Fees, funding, slippage, exposure, outages

With `C_t = E_start + D - W`:

- fee drag = prod(1 + r_t + fees_t / C_t) - prod(1 + r_t): the period return without fees minus the actual one;
- funding drag = prod(1 + r_t - funding_t / C_t) - prod(1 + r_t); negative when funding was received;
- slippage drag = sum(slippage) / sum(C_t) over the days on which slippage was measured, with the number of
  measured days; not shown when no day was measured;
- gross exposure ratio = gross_exposure_t / C_t, average and maximum;
- outage minutes, days with an outage and days on which the strategy was suspended are counted and reported.
  Returns are not adjusted for them.

## Aggregation and suppression

A published series has exactly one `source_kind`; kinds are never mixed. Within a series, accounts are grouped
by account class and risk level, and a composite covers all accounts in the series.

- A group's daily return pools its accounts (asset-weighted): the sums of starting equity, ending equity, flows,
  fees and funding are used in the formulas above, so the group behaves like one portfolio. An outage on a pooled
  day is the longest any account saw; a pooled day counts as suspended if any account was suspended; slippage is
  measured for the pooled day only if it was measured for every account.
- A group with fewer accounts than the minimum is published only as `SUPPRESSED_MIN_COUNT`. Days on which
  fewer accounts than the minimum contributed are left out of that group's index and counted
  (`days_suppressed_min_count`).
- The minimum is configurable (default 5). Groups that contain `CUSTOMER` or `BETA_INVITED` accounts are never
  published with fewer than 5 accounts, whatever is configured.
- Published documents contain returns, ratios, counts, dates and minutes. They contain no equity or flow amounts,
  no account reference, no user id, no email and no credential; every document is scanned for such fields before
  it is returned or stored, and the export is refused if one is found.

## Provenance and how to verify it

Each series of rows (`source_kind + family + venue + account_ref`) is a hash chain:

- `prev_hash` is the previous row's `row_hash` in the same series (64 zeros for the first row);
- `row_hash` = SHA-256 of the canonical JSON (sorted keys, no whitespace; decimals written without trailing
  zeros) of every stored field of the row except `row_hash`, including `prev_hash`, the sequence number and the
  recording time.

Each published document carries `provenance.chain_head_hash` = SHA-256 of the newline-joined, sorted head
hashes (latest `row_hash`) of every chain in the series' kind, family and venue, plus `chain_count`,
`chain_row_count`, `rows_used` and data freshness (latest trading date, last recording time, generation
time).

To verify a publication, someone with read access to the ledger:

1. runs `verify_chain` (or recomputes each row hash and each `prev_hash` link independently); every chain must
   verify with no sequence gaps;
2. recomputes the SHA-256 of the sorted head hashes and compares it with `chain_head_hash`;
3. re-runs the export for the same filters and compares the figures. Rows recorded after the publication
   change the head hash; the publication time says which state was published.

Publications are stored append-only; the newest publication of a series is the one shown.

## Material limitations

- Results are not audited or independently verified.
- Past performance does not predict future results. Losses can be larger than any shown.
- The start-of-day flow convention can misstate a day's return when a large deposit or withdrawal happened late
  in that day.
- Aggregates are asset-weighted: large accounts weigh more, and an individual account's result can differ
  materially from its group.
- Suppressed groups and days are omitted, so aggregates may leave out some activity.
- Fees and funding are as the venue reports them. Slippage is shown only where it could be measured.
- Outages and suspensions are reported, not adjusted for.
- Simulated, paper and demo results do not reflect real execution, liquidity, fees or the effects of trading
  real money, and are not achieved trading results.
- A strategy version can change during an observation period; each published series lists every version with
  its first and last date.
