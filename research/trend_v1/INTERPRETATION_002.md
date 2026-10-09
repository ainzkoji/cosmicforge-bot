# Trend ensemble v1: implementation interpretation 002

Recorded 2026-10-09, after the dataset was downloaded and its coverage was counted, and before any price
series was loaded into the evaluator and before any backtest was run. Registered in the research register as
amendment 2 to Mandate 004 (hypothesis 18).

It adds one point to interpretation 001. It changes no value of `SPEC.md` and no item of interpretation 001.
It was decided from counts of bars and trades only (how many bars have no trade, and where in a contract's
life they fall). No price, return or result was looked at.

## What was found

Of 662,826 daily bars in the archive's monthly files for USDT-margined perpetuals, 56,754 have zero trades
and zero volume. They are of two kinds:

- 53,742 follow a contract's last traded day. After a contract is settled the archive keeps publishing one bar
  per day at an unchanging price. 165 contracts have such a tail.
- 3,012 lie between two trading periods of the same symbol (11 symbols that were settled and later listed
  again under the same name).

Interpretation 001 already says that a contract that ended is closed at its last close (I14) and that a
relisted ticker does not inherit earlier history (I1). Both were written on the assumption that the archive
stops when a contract stops. It does not, so the rule has to say what a bar without a trade is.

## I21. A bar with zero trades is not a bar

- A daily bar with no trade is treated exactly like a missing day: it is not a price, it is not volume, and it
  does not count toward a contract's history.
- Consequences, all of which follow from interpretation 001 once such a day is "no bar":
  - the 120-day history count restarts after it (I3), so a relisted symbol starts again;
  - a sub-signal is reset to OFF on it (I4);
  - a held coin whose bar has no trade is closed at its last traded close with the stress cost (I14);
  - an order cannot fill on it.
- The bars stay in the dataset and are counted in the coverage report. Only the loader ignores them.

## Days missing from the monthly files

This is a statement about the data source, not a strategy choice, recorded here so that it is on file before
the first run.

- For 54 symbols the archive's monthly files skip some days (665 bar-days, most of them 26 to 28 February
  2022 and 1 to 2 April 2022 for about fifty symbols). The same archive publishes daily files for those days.
- The dataset takes such a day from the archive's daily file for the same symbol, with the same checksum
  verification. The number of bars taken this way is reported per symbol.
- A day that neither file has stays missing. Nothing is interpolated or filled forward.
- Result of the build: 319 of the 665 days were found in daily files. 346 remain missing, in three symbols:
  two settled contracts (BNXUSDTSETTLED, TLMUSDTSETTLED) and five days of ICPUSDT (22 to 26 September 2022).

`SPEC.md` names the public archive as the data source; the daily files are part of it.
