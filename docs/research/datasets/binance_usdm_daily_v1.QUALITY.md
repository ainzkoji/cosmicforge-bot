# Dataset `binance_usdm_daily` v1: data quality report

Deliverable D6 (Section H, Step 2.3). Built 9 October 2026. Frozen in the research register (record 23).

| | |
|---|---|
| Dataset hash | `0c3392d8daf78e08244fdd21edac5a93ef0c6cfd5386e3cecbf6cb379fde284e` |
| Manifest | `binance_usdm_daily_v1.dataset.json`, hash `38d940f14f98a604ffe7bd3336e51b51e07fdda571b057db19327b4019a6a450` |
| Per-symbol detail | `../coverage/binance_usdm_daily_v1.coverage.json` (one record per contract) |
| Per-file checksums | `binance_usdm_daily_v1.raw_inventory.csv.gz` (43,626 files) |
| Exchange metadata snapshot | `binance_usdm_daily_v1.metadata.json` (today's values, not history) |
| Code | `backends/bot-backend/app/market_data/binance_archive.py`, `daily_dataset.py` |

## Verdict

**Usable for the Mandate 004 evaluation, with stated limitations.** Every file was verified, no bar is
invalid or duplicated, missing days are few and confined to three symbols, and delisted contracts are in the
data. The dataset is **not** claimed to be free of survivorship bias, and its listing dates, exchange filters
and contract categories are not point-in-time records.

## What was collected

| | |
|---|---|
| Source | Binance public archive, `data.binance.vision`: daily klines and funding rates of USD-M futures |
| Period | 2020-01-01 to 2026-09-30 (81 months) |
| Contracts | 911 USDT-margined perpetuals, from the archive's own listing (1,056 symbols seen; 145 out of scope: other margin assets and dated delivery contracts) |
| Daily bars | 663,145 |
| Funding records | 2,735,181 |
| Raw files | 43,626, each checked against the archive's own SHA-256 before use |
| Failed downloads / unreadable files | 0 / 0 |

## Checks and what they found

| Check | Result |
|---|---|
| Checksum of every file | 43,626 of 43,626 match |
| Timestamps | all on 00:00 UTC day boundaries; milliseconds (other units are converted or refused) |
| Duplicate bars | 0 |
| Out-of-order rows | sorted when found; counted per symbol |
| Invalid bars (non-positive price, high below low, high below open or close, low above open or close) | 0 |
| Days missing from the monthly files | 665 bar-days in 54 symbols; 319 recovered from the same archive's daily files; **346 remain missing** |
| Bars with no trade | 56,815 (8.6% of bars): kept, never used as a price |
| Funding gaps on traded bars | see below |

### Missing days

Most of the 665 skipped days are 26 to 28 February 2022 and 1 to 2 April 2022, for about fifty symbols in one
alphabetical block. The archive's daily files have them, and the dataset takes them from there with the same
checksum rule. The 346 days that neither file has are:

| Symbol | Missing days | Period |
|---|---|---|
| TLMUSDTSETTLED | 188 | 2022-02-26 to 2022-04-02, September 2022 to January 2023 (a settled contract) |
| BNXUSDTSETTLED | 153 | 2022-09-01 to 2023-01-31 (a settled contract) |
| ICPUSDT | 5 | 2022-09-22 to 2022-09-26 |

Nothing is interpolated or filled forward. A missing day restarts a contract's 120-day history count and
closes a position held in it.

### Bars with no trade

After a contract is settled, the archive keeps publishing one bar a day at an unchanging price with zero
volume. Of the 56,815 bars with no trade, 53,731 follow a contract's last traded day (in 153 contracts),
3,072 lie before the first trade or between two trading periods of 12 symbols (tickers that were settled and
later listed again), and 12 are the single bars of 12 `SETTLED` symbols that never show a trade. They stay in
the table and are counted, and the loader presents them as "no bar". Without this rule a backtest could
hold, value and sell a contract that no longer trades.

`research/trend_v1/INTERPRETATION_002.md` quotes the same counts as they stood on the monthly files alone,
before the daily files were added (56,754 bars; 53,742 after the last trade in 165 contracts, which there
includes the 12 symbols that never trade; 3,012 between trading periods). The figures above are the final
dataset.

### Funding

| Year | Traded bar-days (contracts at least 120 days old) | With no funding record | Share |
|---|---|---|---|
| 2020 | 6,078 | 0 | 0.00% |
| 2021 | 33,854 | 164 | 0.48% |
| 2022 | 48,114 | 546 | 1.13% |
| 2023 | 60,187 | 42 | 0.07% |
| 2024 | 87,632 | 0 | 0.00% |
| 2025 | 132,233 | 0 | 0.00% |
| 2026 | 139,085 | 0 | 0.00% |

The gaps on traded bars are in three tickers (ICPUSDT, TLMUSDT, BNXUSDT and their settled forms). The much
larger raw count of bar-days without funding (40,668) is almost entirely the zero-trade tail of settled
contracts, which is never used. Funding is published at 8-hour, 4-hour and, for a few contracts, 1-hour
intervals; the evaluator charges each event at its own rate. A missing event is charged at 0.03% per 8 hours
and counted; it is never treated as zero. In the development run 1 of 29,606 funding events was imputed.

## Point-in-time universe and survivorship

- **How membership is decided.** Each day, from bars on or before that day only. The contract list is the
  archive's listing, not today's exchange list.
- **Delisted contracts.** 177 contracts do not trade on the last day of coverage. 165 of them have a last
  traded day (by year: 2020: 1, 2021: 3, 2022: 15, 2023: 5, 2024: 30, 2025: 47, 2026: 64); the other 12 are
  `SETTLED` symbols with a single bar and no trade. 130 of the 177 are shown as `SETTLING` in today's exchange
  metadata and 47 are no longer in it at all. In the development run 25 contracts that later ended were
  universe members and could be traded.
- **What is reconstructed, not recorded.** A contract's listing and delisting dates are inferred from where
  its archive bars start and stop trading. No public point-in-time record of listings, contract status or
  exchange filters exists.
- **What cannot be seen.** A contract that has no archive file at all. The archive also starts on
  2020-01-01, so contracts listed in 2019 have no earlier history here; with the 120-day rule the strategy
  has no eligible contract before 29 April 2020.
- **Conclusion.** Survivorship bias is reduced, and measurably so, but it is not proven absent. The dataset
  must not be described as survivorship-bias-free.

## Exchange metadata

The snapshot of `exchangeInfo` (fetched 9 October 2026) supplies the contract category used to exclude
non-crypto contracts (216 equity, commodity, currency and pre-listing perpetuals), and the quantity step and
minimum notional used for order sizes. These are today's values. For the 47 contracts that are no longer
listed there are none: they are treated as crypto, without quantity rounding and with a 5 USDT minimum.

## Reproducing it

`docs/research/mandate_004/REPRODUCTION.md`, section 2. The build is deterministic: the same raw files give
the same content hashes on any platform. `verify --raw` re-hashes every raw file against the inventory.
