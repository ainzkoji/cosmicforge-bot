#!/usr/bin/env python3
"""
data_quality_check.py — Step 5E-4 Section 4

Validates trade_fills data quality before ML training.  Checks:
  1. Row counts (OPEN / CLOSE actions)
  2. exit_reason population rate on CLOSE fills
  3. mfe_pct / mae_pct population rate on CLOSE fills
  4. Duplicate position_id detection
  5. Timestamp ordering (OPEN timestamp < CLOSE timestamp per position)
  6. Win / loss distribution
  7. Symbol and regime breakdown
  8. Overall data readiness for build_dataset.py

Usage (from backends/bot-backend/):
    python scripts/ml/data_quality_check.py --db-path ../../data/bot.db

Exit codes:
    0 — all checks pass (READY for build_dataset.py)
    1 — one or more checks fail
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

# Minimum closed trades required before build_dataset.py makes sense.
# (build_dataset.py --min-trades default is 100; we warn earlier at 50.)
MIN_TRADES_WARN = 50
MIN_TRADES_GO   = 500  # Target from Step 5E-4 spec


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _pct(n: int, d: int) -> str:
    if d == 0:
        return "N/A"
    return f"{n / d * 100:.1f}%"


def analyze(db_path: Path, verbose: bool) -> int:
    if not db_path.exists():
        print(f"[X]  Database not found: {db_path}", file=sys.stderr)
        return 1

    conn = _connect(str(db_path))

    print("=" * 70)
    print("TRADE FILLS DATA QUALITY CHECK")
    print(f"DB: {db_path}")
    print("=" * 70)

    # -- 1. Row counts ---------------------------------------------------------
    print("\n" + "-" * 70)
    print("1. ROW COUNTS")
    print("-" * 70)

    total_fills = conn.execute("SELECT COUNT(*) FROM trade_fills").fetchone()[0]
    open_fills  = conn.execute(
        "SELECT COUNT(*) FROM trade_fills WHERE action = 'OPEN'"
    ).fetchone()[0]
    close_fills = conn.execute(
        "SELECT COUNT(*) FROM trade_fills WHERE action = 'CLOSE'"
    ).fetchone()[0]

    print(f"  Total trade_fills rows: {total_fills:,}")
    print(f"  OPEN  fills:            {open_fills:,}")
    print(f"  CLOSE fills:            {close_fills:,}")

    if total_fills == 0:
        print("\n[X]  trade_fills is EMPTY.")
        print("   The bot must execute trades to generate training data.")
        print("   Ensure the market is NOT in LOW_VOLATILITY_CHOP regime.")
        print("   Use .env.data_collection for a lower signal threshold.")
        conn.close()
        return 1

    date_row = conn.execute(
        "SELECT MIN(timestamp_utc) AS first_ts, MAX(timestamp_utc) AS last_ts "
        "FROM trade_fills"
    ).fetchone()
    print(f"  Date range: {date_row['first_ts']} -> {date_row['last_ts']}")

    # -- 2. exit_reason completeness -------------------------------------------
    print("\n" + "-" * 70)
    print("2. EXIT_REASON COMPLETENESS (CLOSE fills only)")
    print("-" * 70)

    if close_fills > 0:
        exit_null = conn.execute(
            "SELECT COUNT(*) FROM trade_fills WHERE action='CLOSE' AND exit_reason IS NULL"
        ).fetchone()[0]
        exit_dist = conn.execute(
            "SELECT exit_reason, COUNT(*) AS n FROM trade_fills "
            "WHERE action='CLOSE' GROUP BY exit_reason ORDER BY n DESC"
        ).fetchall()

        exit_populated_pct = _pct(close_fills - exit_null, close_fills)
        print(f"  exit_reason populated: {close_fills - exit_null:,}/{close_fills:,}  ({exit_populated_pct})")

        if exit_null > 0:
            print(f"  !!️  {exit_null} CLOSE fills have NULL exit_reason")

        print(f"  exit_reason breakdown:")
        for row in exit_dist:
            label = row["exit_reason"] or "NULL"
            print(f"    {label:<20} {row['n']:>6,}  ({_pct(row['n'], close_fills)})")

        exit_reason_ok = exit_null == 0
    else:
        print("  (no CLOSE fills yet)")
        exit_reason_ok = False

    # -- 3. MFE / MAE completeness ---------------------------------------------
    print("\n" + "-" * 70)
    print("3. MFE / MAE COMPLETENESS (CLOSE fills only)")
    print("-" * 70)

    if close_fills > 0:
        mfe_null = conn.execute(
            "SELECT COUNT(*) FROM trade_fills WHERE action='CLOSE' AND mfe_pct IS NULL"
        ).fetchone()[0]
        mae_null = conn.execute(
            "SELECT COUNT(*) FROM trade_fills WHERE action='CLOSE' AND mae_pct IS NULL"
        ).fetchone()[0]

        print(f"  mfe_pct populated: {close_fills - mfe_null:,}/{close_fills:,}  ({_pct(close_fills - mfe_null, close_fills)})")
        print(f"  mae_pct populated: {close_fills - mae_null:,}/{close_fills:,}  ({_pct(close_fills - mae_null, close_fills)})")

        mfe_mae_ok = (mfe_null == 0) and (mae_null == 0)
        if not mfe_mae_ok:
            print(f"  !!️  MFE/MAE NULLs detected — check position_manager.py close handling.")
    else:
        mfe_mae_ok = False

    # -- 4. Duplicate position_id detection ------------------------------------
    print("\n" + "-" * 70)
    print("4. DUPLICATE POSITION_ID CHECK")
    print("-" * 70)

    dup_rows = conn.execute(
        """
        SELECT position_id, action, COUNT(*) AS n
        FROM trade_fills
        WHERE position_id IS NOT NULL
        GROUP BY position_id, action
        HAVING COUNT(*) > 1
        """
    ).fetchall()

    if dup_rows:
        print(f"  [X]  {len(dup_rows)} duplicate (position_id, action) pairs found:")
        for row in dup_rows[:10]:
            print(f"     position_id={row['position_id']}  action={row['action']}  count={row['n']}")
        dups_ok = False
    else:
        print(f"  [OK] No duplicate (position_id, action) pairs.")
        dups_ok = True

    # -- 5. Timestamp ordering -------------------------------------------------
    print("\n" + "-" * 70)
    print("5. TIMESTAMP ORDERING (OPEN before CLOSE per position)")
    print("-" * 70)

    if close_fills > 0:
        # Join OPEN and CLOSE on position_id, check OPEN.timestamp < CLOSE.timestamp
        inverted = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM trade_fills o
            JOIN trade_fills c ON c.position_id = o.position_id
                AND c.action = 'CLOSE'
            WHERE o.action = 'OPEN'
              AND o.timestamp_utc >= c.timestamp_utc
            """
        ).fetchone()[0]

        if inverted > 0:
            print(f"  [X]  {inverted} position(s) have OPEN timestamp >= CLOSE timestamp.")
            ts_order_ok = False
        else:
            matched_positions = conn.execute(
                """
                SELECT COUNT(DISTINCT o.position_id)
                FROM trade_fills o
                JOIN trade_fills c ON c.position_id = o.position_id
                    AND c.action = 'CLOSE'
                WHERE o.action = 'OPEN'
                """
            ).fetchone()[0]
            print(f"  [OK] All {matched_positions:,} matched OPEN->CLOSE pairs have correct ordering.")
            ts_order_ok = True
    else:
        print("  (skipped — no CLOSE fills)")
        ts_order_ok = True  # Not a blocker when there are no fills

    # -- 6. Win / loss distribution --------------------------------------------
    print("\n" + "-" * 70)
    print("6. WIN / LOSS DISTRIBUTION (CLOSE fills with realized_pnl)")
    print("-" * 70)

    if close_fills > 0:
        dist = conn.execute(
            """
            SELECT
                SUM(CASE WHEN realized_pnl > 0  THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN realized_pnl < 0  THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN realized_pnl = 0  THEN 1 ELSE 0 END) AS breakeven,
                SUM(CASE WHEN realized_pnl IS NULL THEN 1 ELSE 0 END) AS no_pnl,
                AVG(realized_pnl) AS avg_pnl,
                SUM(realized_pnl) AS total_pnl
            FROM trade_fills WHERE action = 'CLOSE'
            """
        ).fetchone()

        wins     = dist["wins"] or 0
        losses   = dist["losses"] or 0
        brk      = dist["breakeven"] or 0
        no_pnl   = dist["no_pnl"] or 0
        avg_pnl  = dist["avg_pnl"] or 0.0
        total_pnl = dist["total_pnl"] or 0.0
        labelled  = wins + losses + brk

        print(f"  Wins:       {wins:>6,}  ({_pct(wins, close_fills)})")
        print(f"  Losses:     {losses:>6,}  ({_pct(losses, close_fills)})")
        print(f"  Break-even: {brk:>6,}  ({_pct(brk, close_fills)})")
        print(f"  No PnL:     {no_pnl:>6,}  ({_pct(no_pnl, close_fills)})")
        print(f"  Win rate:   {_pct(wins, labelled)} (of labelled)")
        print(f"  Avg PnL:    {avg_pnl:+.4f} USDT/trade")
        print(f"  Total PnL:  {total_pnl:+.4f} USDT")

        # Check class balance — ML needs both wins AND losses
        both_classes = wins > 0 and losses > 0
        if not both_classes:
            print(f"  !!️  Only one class present — ML cannot train until both wins AND losses exist.")
        win_loss_ok = both_classes
    else:
        win_loss_ok = False

    # -- 7. Symbol and regime distribution ------------------------------------
    print("\n" + "-" * 70)
    print("7. SYMBOL DISTRIBUTION (CLOSE fills)")
    print("-" * 70)

    if close_fills > 0:
        symbol_rows = conn.execute(
            "SELECT symbol, COUNT(*) AS n FROM trade_fills WHERE action='CLOSE' "
            "GROUP BY symbol ORDER BY n DESC"
        ).fetchall()
        symbols_seen = len(symbol_rows)
        print(f"  Symbols with closed trades: {symbols_seen}")
        for row in symbol_rows:
            print(f"    {row['symbol']:<15} {row['n']:>6,} trades")
        multi_symbol = symbols_seen > 1
        if not multi_symbol:
            print("  !!️  Only 1 symbol — richer training with multiple symbols.")
    else:
        multi_symbol = False

    # -- Summary ---------------------------------------------------------------
    print("\n" + "-" * 70)
    print("DATA QUALITY VERDICT")
    print("-" * 70)

    checks = []

    def chk(name: str, passed: bool, detail: str) -> None:
        sym = "[OK]" if passed else "[X] "
        print(f"  {sym} {name}: {detail}")
        checks.append(passed)

    has_min_warn = close_fills >= MIN_TRADES_WARN
    has_min_go   = close_fills >= MIN_TRADES_GO

    chk("trade_fills not empty",        total_fills > 0,
        f"{total_fills:,} rows total")
    chk(f">={MIN_TRADES_WARN} closed trades",  has_min_warn,
        f"{close_fills:,} CLOSE fills")
    chk("exit_reason populated",        exit_reason_ok if close_fills > 0 else False,
        "100%" if exit_reason_ok else (f"{_pct(close_fills - (conn.execute('SELECT COUNT(*) FROM trade_fills WHERE action=? AND exit_reason IS NULL', ('CLOSE',)).fetchone()[0]), close_fills)} populated" if close_fills > 0 else "no fills"))
    chk("mfe_pct / mae_pct populated",  mfe_mae_ok if close_fills > 0 else False,
        "100%" if mfe_mae_ok else "NULLs present" if close_fills > 0 else "no fills")
    chk("No duplicate position_ids",    dups_ok,
        "clean" if dups_ok else f"{len(dup_rows)} duplicates")
    chk("Timestamp ordering valid",     ts_order_ok,
        "all ordered" if ts_order_ok else "inversions found")
    chk("Both win and loss classes",    win_loss_ok if close_fills > 0 else False,
        f"wins={wins if close_fills > 0 else 0}  losses={losses if close_fills > 0 else 0}" if close_fills > 0 else "no fills")

    print()
    if has_min_go and all(checks):
        print(f"[GO] DATA QUALITY: PASS — {close_fills:,} trades collected.  Run build_dataset.py.")
    elif has_min_warn and all(checks):
        print(f"[WARN] DATA QUALITY: PARTIAL — {close_fills:,} trades.  "
              f"Consider waiting for {MIN_TRADES_GO:,} before training for best results.")
        print(f"   You can test build_dataset.py now with --min-trades {close_fills}.")
    else:
        n_fail = sum(1 for c in checks if not c)
        print(f"[NOGO] DATA QUALITY: {n_fail} check(s) failed.  Continue data collection.")

    print()
    print("Next steps:")
    if not has_min_warn:
        print(f"  1. Continue running the bot until >={MIN_TRADES_WARN} CLOSE fills exist")
    if close_fills > 0 and not has_min_go:
        print(f"  2. Target: {MIN_TRADES_GO:,} trades for robust ML training")
    if close_fills >= MIN_TRADES_WARN:
        print(f"  3. Run: python scripts/ml/build_dataset.py --db-path {db_path} "
              f"--min-trades {min(close_fills, 100)}")

    conn.close()
    return 0 if all(checks) else 1


def main() -> int:
    p = argparse.ArgumentParser(
        description="Validate trade_fills data quality for ML training.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--db-path", type=Path, default=Path("data/bot.db"),
        help="Path to bot SQLite DB (default: data/bot.db)",
    )
    p.add_argument("--verbose", "-v", action="store_true")
    args = p.parse_args()
    return analyze(db_path=args.db_path.resolve(), verbose=args.verbose)


if __name__ == "__main__":
    sys.exit(main())
