#!/usr/bin/env python3
"""
verify_feature_completeness.py — Step 5E-4 Section 1

Connects to bot.db and verifies that decision_traces produced after the
Step 5E-3B fix have complete ML feature vectors (no NULLs on indicator
columns regardless of regime).

Usage (from backends/bot-backend/):
    python scripts/ml/verify_feature_completeness.py --db-path ../../data/bot.db

    # Restrict to traces captured after the fix:
    python scripts/ml/verify_feature_completeness.py \\
        --db-path ../../data/bot.db \\
        --since 2026-03-22T00:00:00
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

# -- Columns that must be non-NULL after Step 5E-3B ---------------------------
# These come from the MasterEnsembleStrategy meta dict and are stored by
# trace_recorder.record_strategies().  Old traces (pre-fix) have all NULLs here.
ML_FEATURE_COLS = [
    "adx",
    "atr_pct",
    "ma_slope",
    "compression_ratio",
    "breakout_pressure",
    "buy_score",
    "sell_score",
    "threshold",
    "active_strategy_count",
    "htf_opposed",
]

# regime_state uses a sentinel "UNKNOWN" not NULL, check separately
REGIME_COL = "regime_state"


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _total_and_since(conn: sqlite3.Connection, since: str | None) -> tuple[int, int]:
    total = conn.execute("SELECT COUNT(*) FROM decision_traces").fetchone()[0]
    if since:
        since_count = conn.execute(
            "SELECT COUNT(*) FROM decision_traces WHERE ts >= ?", (since,)
        ).fetchone()[0]
    else:
        since_count = total
    return total, since_count


def _null_rates(conn: sqlite3.Connection, since: str | None) -> dict[str, tuple[int, int]]:
    """Return {col: (null_count, total)} for each ML feature column."""
    where = f"WHERE ts >= '{since}'" if since else ""
    total = conn.execute(f"SELECT COUNT(*) FROM decision_traces {where}").fetchone()[0]
    result: dict[str, tuple[int, int]] = {}
    for col in ML_FEATURE_COLS:
        null_count = conn.execute(
            f"SELECT COUNT(*) FROM decision_traces {where} AND {col} IS NULL"
            if since
            else f"SELECT COUNT(*) FROM decision_traces WHERE {col} IS NULL"
        ).fetchone()[0]
        result[col] = (null_count, total)
    return result


def _regime_distribution(conn: sqlite3.Connection, since: str | None) -> dict[str, int]:
    where = f"WHERE ts >= '{since}'" if since else ""
    rows = conn.execute(
        f"SELECT regime_state, COUNT(*) AS n FROM decision_traces {where} "
        f"GROUP BY regime_state ORDER BY n DESC"
    ).fetchall()
    return {r["regime_state"]: r["n"] for r in rows}


def _sample_chop_trace(conn: sqlite3.Connection, since: str | None) -> dict | None:
    where = f"WHERE ts >= '{since}' AND" if since else "WHERE"
    row = conn.execute(
        f"SELECT ts, adx, atr_pct, buy_score, sell_score, threshold, regime_state "
        f"FROM decision_traces {where} regime_state = 'LOW_VOLATILITY_CHOP' "
        f"ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def _sample_nochop_trace(conn: sqlite3.Connection, since: str | None) -> dict | None:
    where = f"WHERE ts >= '{since}' AND" if since else "WHERE"
    row = conn.execute(
        f"SELECT ts, adx, atr_pct, buy_score, sell_score, threshold, regime_state "
        f"FROM decision_traces {where} regime_state NOT IN ('LOW_VOLATILITY_CHOP','UNKNOWN','unknown') "
        f"ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def _date_range(conn: sqlite3.Connection, since: str | None) -> tuple[str | None, str | None]:
    where = f"WHERE ts >= '{since}'" if since else ""
    row = conn.execute(
        f"SELECT MIN(ts) AS first_ts, MAX(ts) AS last_ts FROM decision_traces {where}"
    ).fetchone()
    return row["first_ts"], row["last_ts"]


def analyze(db_path: Path, since: str | None, verbose: bool) -> int:
    if not db_path.exists():
        print(f"[X]  Database not found: {db_path}", file=sys.stderr)
        return 1

    conn = _connect(str(db_path))

    print("=" * 70)
    print("ML FEATURE COMPLETENESS VERIFICATION")
    print(f"DB:    {db_path}")
    print(f"Since: {since or '(all time)'}")
    print("=" * 70)

    total_all, total_since = _total_and_since(conn, since)
    first_ts, last_ts = _date_range(conn, since)

    print(f"\nTotal traces (all time):  {total_all:,}")
    print(f"Traces in analysis range: {total_since:,}")
    if first_ts:
        print(f"Date range: {first_ts[:19]} to {last_ts[:19]}")

    if total_since == 0:
        print("\n!!️  No traces found in the specified range.")
        print("   Run the bot for 2–3 cycles, then re-run this script.")
        return 1

    # -- NULL analysis ---------------------------------------------------------
    print("\n" + "-" * 70)
    print("NULL RATE PER ML FEATURE COLUMN")
    print("-" * 70)

    null_rates = _null_rates(conn, since)
    worst_null_pct = 0.0
    all_clean = True

    for col, (null_count, total) in null_rates.items():
        null_pct = (null_count / total * 100) if total else 0.0
        worst_null_pct = max(worst_null_pct, null_pct)
        if null_pct == 0.0:
            status = "[OK]"
        elif null_pct < 5.0:
            status = "!!️ "
            all_clean = False
        else:
            status = "[X] "
            all_clean = False
        bar = "#" * int(null_pct / 5) + "." * (20 - int(null_pct / 5))
        print(f"  {status} {col:<28} NULL={null_count:>6,}/{total:>6,}  ({null_pct:5.1f}%)  {bar}")

    # -- Regime distribution ---------------------------------------------------
    print("\n" + "-" * 70)
    print("REGIME DISTRIBUTION")
    print("-" * 70)

    regime_dist = _regime_distribution(conn, since)
    total_traces = sum(regime_dist.values())
    chop_count = regime_dist.get("LOW_VOLATILITY_CHOP", 0)
    unknown_count = regime_dist.get("UNKNOWN", 0) + regime_dist.get("unknown", 0)
    non_chop_active = total_traces - chop_count - unknown_count

    for regime, count in sorted(regime_dist.items(), key=lambda x: -x[1]):
        pct = count / total_traces * 100 if total_traces else 0
        bar = "#" * int(pct / 5)
        print(f"  {regime:<30} {count:>6,}  ({pct:5.1f}%)  {bar}")

    has_chop = chop_count > 0
    has_non_chop = non_chop_active > 0

    # -- Sample traces ---------------------------------------------------------
    if verbose:
        print("\n" + "-" * 70)
        print("SAMPLE TRACES (most recent)")
        print("-" * 70)

        chop_sample = _sample_chop_trace(conn, since)
        if chop_sample:
            print("\n  [CHOP trace]")
            for k, v in chop_sample.items():
                print(f"    {k}: {v}")
        else:
            print("\n  [CHOP trace] — none found in range")

        nochop_sample = _sample_nochop_trace(conn, since)
        if nochop_sample:
            print("\n  [Non-CHOP trace]")
            for k, v in nochop_sample.items():
                print(f"    {k}: {v}")
        else:
            print("\n  [Non-CHOP trace] — none found in range")

    # -- GO / NO-GO ------------------------------------------------------------
    print("\n" + "-" * 70)
    print("FEATURE COMPLETENESS VERDICT")
    print("-" * 70)

    checks = []

    def chk(name: str, passed: bool, detail: str) -> None:
        symbol = "[OK]" if passed else "[X] "
        print(f"  {symbol} {name}: {detail}")
        checks.append(passed)

    chk("Traces present",       total_since > 0,    f"{total_since:,} traces in range")
    chk("No NULL features",     all_clean,           f"worst column: {worst_null_pct:.1f}% NULL")
    chk("CHOP traces present",  has_chop,            f"{chop_count:,} CHOP traces")
    chk("Non-CHOP traces present", has_non_chop,     f"{non_chop_active:,} non-CHOP active-regime traces")
    chk("No unknown-regime traces", unknown_count == 0,
        f"{unknown_count} traces with regime='UNKNOWN' (pre-regime failures)")

    if unknown_count > 0 and unknown_count < total_since:
        print(f"\n  i️  {unknown_count} UNKNOWN-regime traces are pre-regime failures "
              f"(klines error / classify error).  Safe to exclude from training via "
              f"--exclude-regime unknown in build_dataset.py.")

    print()
    all_pass = all(checks)
    if all_pass:
        print("[GO] FEATURE COMPLETENESS: PASS — ready to proceed to data collection.")
    else:
        n_fail = sum(1 for c in checks if not c)
        print(f"[NOGO] FEATURE COMPLETENESS: {n_fail} check(s) failed.")
        if not all_clean:
            print("   NULL features detected.  Likely cause: traces predate Step 5E-3B fix.")
            print("   Use --since <iso-timestamp> to filter to only post-fix traces.")
        if not has_non_chop:
            print("   No non-CHOP traces.  Market may be in LOW_VOLATILITY_CHOP.")
            print("   Wait for regime change, or run during a trending/volatile session.")

    conn.close()
    return 0 if all_pass else 1


def main() -> int:
    p = argparse.ArgumentParser(
        description="Verify ML feature completeness in decision_traces.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--db-path", type=Path, default=Path("data/bot.db"),
        help="Path to bot SQLite DB (default: data/bot.db)",
    )
    p.add_argument(
        "--since", default=None,
        help=(
            "Only analyse traces at or after this ISO timestamp "
            "(e.g. 2026-03-22T00:00:00).  Useful to exclude old pre-fix traces."
        ),
    )
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Print sample trace rows.")
    args = p.parse_args()
    return analyze(db_path=args.db_path.resolve(), since=args.since, verbose=args.verbose)


if __name__ == "__main__":
    sys.exit(main())
