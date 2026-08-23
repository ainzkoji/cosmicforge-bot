"""
validate_backfill.py — Phase 3 Step 5E-3 Validation

Verifies that the historical_backfill.py script successfully populated
decision_traces and trade_fills with sufficient data for ML training.

USAGE:
  python scripts/ml/validate_backfill.py [--db auto]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_SHARED = Path(__file__).resolve().parents[4] / "shared"
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(_SHARED))


def validate(db_path: str) -> bool:
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    passed = True
    results = {}

    print(f"\n{'='*60}")
    print(f"BACKFILL VALIDATION: {db_path}")
    print(f"{'='*60}")

    # 1. Row counts
    try:
        traces_total = conn.execute("SELECT COUNT(*) FROM decision_traces").fetchone()[0]
        fills_open   = conn.execute("SELECT COUNT(*) FROM trade_fills WHERE action='OPEN'").fetchone()[0]
        fills_close  = conn.execute("SELECT COUNT(*) FROM trade_fills WHERE action='CLOSE'").fetchone()[0]

        results["decision_traces_total"] = traces_total
        results["trade_fills_OPEN"]      = fills_open
        results["trade_fills_CLOSE"]     = fills_close

        print(f"\n[1] Row Counts")
        print(f"  decision_traces : {traces_total:,}")
        print(f"  trade_fills OPEN: {fills_open:,}")
        print(f"  trade_fills CLOSE: {fills_close:,}")

        if traces_total < 1000:
            print(f"  [FAIL] decision_traces < 1000 (target: 1000+)")
            passed = False
        else:
            print(f"  [OK] decision_traces >= 1000")

        if fills_close < 500:
            print(f"  [FAIL] Closed trades < 500 (target: 500+)")
            passed = False
        else:
            print(f"  [OK] Closed trades >= 500")
    except Exception as e:
        print(f"  [FAIL] Row count query failed: {e}")
        passed = False

    # 2. Join validation
    print(f"\n[2] Join Validation (decision_traces JOIN trade_fills)")
    try:
        joinable = conn.execute("""
            SELECT COUNT(*) FROM decision_traces dt
            INNER JOIN trade_fills tf
                ON dt.run_id = tf.run_id
               AND dt.cycle_id = tf.cycle_id
               AND dt.symbol = tf.symbol
            WHERE tf.action = 'OPEN'
        """).fetchone()[0]
        results["joinable_rows"] = joinable

        if joinable == 0:
            print(f"  [FAIL] No joinable rows found — check run_id/cycle_id alignment")
            passed = False
        else:
            print(f"  [OK] {joinable:,} joinable traces")
    except Exception as e:
        print(f"  [FAIL] Join query failed: {e}")
        passed = False

    # 3. Feature null rates
    print(f"\n[3] ML Feature Null Rates")
    feature_cols = ["adx", "atr_pct", "ma_slope", "buy_score", "sell_score",
                    "regime_state", "confidence"]
    total_rows = conn.execute("SELECT COUNT(*) FROM decision_traces").fetchone()[0]

    for col in feature_cols:
        try:
            null_count = conn.execute(
                f"SELECT COUNT(*) FROM decision_traces WHERE {col} IS NULL"
            ).fetchone()[0]
            null_rate = null_count / max(1, total_rows)
            results[f"null_rate_{col}"] = f"{null_rate:.1%}"

            flag = "[OK]" if null_rate < 0.10 else "[!!] " if null_rate < 0.40 else "[FAIL]"
            print(f"  {flag} {col}: {null_rate:.1%} null ({null_count:,}/{total_rows:,})")

            if null_rate > 0.40:
                passed = False
        except Exception as e:
            print(f"  [FAIL] {col}: query failed — {e}")
            passed = False

    # 4. Side distribution (gate_allowed=1 rows)
    print(f"\n[4] Signal Distribution")
    try:
        buy_count  = conn.execute(
            "SELECT COUNT(*) FROM decision_traces WHERE signal='BUY' AND gate_allowed=1"
        ).fetchone()[0]
        sell_count = conn.execute(
            "SELECT COUNT(*) FROM decision_traces WHERE signal='SELL' AND gate_allowed=1"
        ).fetchone()[0]
        hold_count = conn.execute(
            "SELECT COUNT(*) FROM decision_traces WHERE signal='HOLD'"
        ).fetchone()[0]

        print(f"  BUY (gate_allowed): {buy_count:,}")
        print(f"  SELL (gate_allowed): {sell_count:,}")
        print(f"  HOLD: {hold_count:,}")
        results["signal_BUY"]  = buy_count
        results["signal_SELL"] = sell_count
        results["signal_HOLD"] = hold_count

        if buy_count == 0 or sell_count == 0:
            print(f"  [FAIL] Both BUY and SELL signals needed for balanced training")
            passed = False
        else:
            print(f"  [OK] Both directions present")
    except Exception as e:
        print(f"  [FAIL] Signal distribution query failed: {e}")

    # 5. Date range
    print(f"\n[5] Date Range")
    try:
        min_ts = conn.execute("SELECT MIN(ts) FROM decision_traces").fetchone()[0]
        max_ts = conn.execute("SELECT MAX(ts) FROM decision_traces").fetchone()[0]
        symbols = conn.execute(
            "SELECT COUNT(DISTINCT symbol) FROM decision_traces"
        ).fetchone()[0]
        print(f"  From : {min_ts}")
        print(f"  To   : {max_ts}")
        print(f"  Symbols: {symbols}")
        results["date_from"] = min_ts
        results["date_to"]   = max_ts
        results["symbols"]   = symbols
    except Exception as e:
        print(f"  [FAIL] Date range query failed: {e}")

    # 6. Schema check
    print(f"\n[6] Schema Completeness")
    required_cols = [
        "ml_score", "ml_action", "ml_model_version", "ml_threshold",
        "regime_state", "buy_score", "sell_score", "adx", "atr_pct",
        "ma_slope", "gate_allowed", "confidence",
    ]
    try:
        pragma = conn.execute("PRAGMA table_info(decision_traces)").fetchall()
        existing = {r[1] for r in pragma}
        missing = [c for c in required_cols if c not in existing]
        if missing:
            print(f"  [FAIL] Missing columns: {missing}")
            passed = False
        else:
            print(f"  [OK] All required ML columns present")
    except Exception as e:
        print(f"  [FAIL] Schema check failed: {e}")
        passed = False

    conn.close()

    # Summary
    print(f"\n{'='*60}")
    if passed:
        print("[OK] VALIDATION PASSED — Database ready for ML training")
    else:
        print("[FAIL] VALIDATION FAILED — See issues above")
    print(f"{'='*60}\n")

    return passed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="auto")
    args = parser.parse_args()

    if args.db == "auto":
        _shared = Path(__file__).resolve().parents[4] / "shared"
        db_path = str(_shared / "shared_lib" / "persistence" / "cosmicforge.db")
    else:
        db_path = args.db

    ok = validate(db_path)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
