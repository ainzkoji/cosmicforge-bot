"""
analyze_shadow.py -- ML Shadow Deployment Analysis (Phase 3 Step 5D-2)

Reads prediction JSONL logs produced by MLEntryScorer during shadow mode and
produces a GO / NO-GO recommendation for activating the ML scorer in live mode.

Usage:
    python scripts/ml/analyze_shadow.py \\
        --log-dir models/logs \\
        --db-path data/bot.db \\
        [--threshold 0.40] \\
        [--min-days 30] \\
        [--output-csv reports/shadow_analysis.csv]

GO criteria (ALL must pass):
    1. Minimum coverage: ≥ 80% of entry cycles are scored (model didn't error out)
    2. Precision lift:   Would-have-blocked trades have lower win rate than would-have-passed
    3. Block rate:       Would-have-blocked ≤ 40% of rule-engine entries (not too aggressive)
    4. False-positive rate: (low score + actual win) / total_passed ≤ 30%
    5. Concordance:     Score > 0.5 correlates with rule-engine ALLOW in ≥ 70% of cycles
    6. Minimum data:    ≥ 100 scored entry cycles with known outcomes
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _load_predictions(log_dir: Path) -> list[dict]:
    """Load all prediction records from JSONL files in log_dir."""
    records = []
    for jsonl_file in sorted(log_dir.glob("predictions_*.jsonl")):
        with open(jsonl_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records


def _load_trace_outcomes(db_path: Path) -> dict[str, dict]:
    """
    Load decision traces with known outcomes from the database.

    Returns dict: trace_id -> {gate_allowed, fill_price, realized_pnl, symbol, ts}
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                dt.trace_id,
                dt.symbol,
                dt.ts,
                dt.gate_allowed,
                dt.fill_price,
                dt.ml_score,
                dt.ml_action,
                tf.realized_pnl
            FROM decision_traces dt
            LEFT JOIN trade_fills tf
                ON tf.run_id = dt.run_id
                AND tf.cycle_id = dt.cycle_id
                AND tf.symbol = dt.symbol
                AND tf.action = 'CLOSE'
            ORDER BY dt.ts
            """
        ).fetchall()
        return {r["trace_id"]: dict(r) for r in rows}
    finally:
        conn.close()


def _pct(n: int, d: int) -> str:
    if d == 0:
        return "N/A"
    return f"{n / d * 100:.1f}%"


def _win_rate(outcomes: list[Optional[float]]) -> Optional[float]:
    closed = [p for p in outcomes if p is not None]
    if not closed:
        return None
    return sum(1 for p in closed if p > 0) / len(closed)


def analyze(
    log_dir: Path,
    db_path: Path,
    threshold: float = 0.40,
    min_days: int = 30,
    output_csv: Optional[Path] = None,
    verbose: bool = False,
) -> int:
    """
    Run the shadow analysis.  Returns 0 for GO, 1 for NO-GO.
    """
    print("=" * 70)
    print("ML SHADOW DEPLOYMENT ANALYSIS")
    print(f"Log dir : {log_dir}")
    print(f"DB path : {db_path}")
    print(f"Threshold: {threshold}")
    print(f"Min days: {min_days}")
    print("=" * 70)

    # ── Load prediction logs ───────────────────────────────────────────────────
    if not log_dir.exists():
        print(f"[FAIL] Log directory not found: {log_dir}")
        return 1

    predictions = _load_predictions(log_dir)
    if not predictions:
        print("[FAIL] No prediction log files found in log directory.")
        return 1

    print(f"\nLoaded {len(predictions)} prediction records from {log_dir}")

    # ── Compute shadow period ──────────────────────────────────────────────────
    timestamps = []
    for p in predictions:
        try:
            ts = datetime.fromisoformat(p["ts"].replace("Z", "+00:00"))
            timestamps.append(ts)
        except Exception:
            continue

    if not timestamps:
        print("[FAIL] No valid timestamps found in prediction logs.")
        return 1

    first_ts = min(timestamps)
    last_ts = max(timestamps)
    days_elapsed = (last_ts - first_ts).days
    print(f"Shadow period : {first_ts.date()} -> {last_ts.date()} ({days_elapsed} days)")

    # ── Load trace outcomes ────────────────────────────────────────────────────
    if not db_path.exists():
        print(f"[FAIL] Database not found: {db_path}")
        return 1

    trace_outcomes = _load_trace_outcomes(db_path)
    print(f"Loaded {len(trace_outcomes)} traces with outcomes from DB")

    # ── Join predictions with outcomes ─────────────────────────────────────────
    # For each prediction, find the matching trace outcome
    matched: list[dict] = []
    unmatched_count = 0

    for pred in predictions:
        tid = pred.get("trace_id")
        score = pred.get("score")
        if score is None:
            continue  # Skip SKIP records
        outcome = trace_outcomes.get(tid)
        if outcome is None:
            unmatched_count += 1
            continue
        matched.append({
            "trace_id": tid,
            "symbol": pred.get("symbol", outcome.get("symbol", "?")),
            "score": float(score),
            "action": pred.get("action", "?"),
            "model_version": pred.get("model_version"),
            "shadow_mode": pred.get("shadow_mode", True),
            "realized_pnl": outcome.get("realized_pnl"),
            "gate_allowed": outcome.get("gate_allowed"),
        })

    total_scored = len(matched)
    print(f"\nMatched predictions with outcomes: {total_scored}")
    print(f"Unmatched (no outcome yet):        {unmatched_count}")

    if total_scored == 0:
        print("\n[FAIL] No matched records -- cannot evaluate. Wait for more trades to close.")
        return 1

    # ── Compute metrics ────────────────────────────────────────────────────────
    above_thresh = [r for r in matched if r["score"] >= threshold]   # would PASS
    below_thresh = [r for r in matched if r["score"] < threshold]    # would BLOCK

    above_with_outcome = [r for r in above_thresh if r["realized_pnl"] is not None]
    below_with_outcome = [r for r in below_thresh if r["realized_pnl"] is not None]

    # True improvement (PnL, win rate)
    baseline_outcomes = [r for r in matched if r["gate_allowed"] and r["realized_pnl"] is not None]
    ml_outcomes = [r for r in matched if r["gate_allowed"] and r["score"] >= threshold and r["realized_pnl"] is not None]
    blocked_outcomes = [r for r in matched if r["gate_allowed"] and r["score"] < threshold and r["realized_pnl"] is not None]

    baseline_wr = _win_rate([r["realized_pnl"] for r in baseline_outcomes])
    pass_wr     = _win_rate([r["realized_pnl"] for r in ml_outcomes])
    block_wr    = _win_rate([r["realized_pnl"] for r in blocked_outcomes])

    # True improvement over baseline
    precision_lift = (pass_wr - baseline_wr) if (pass_wr is not None and baseline_wr is not None) else None

    # Would-have-blocked count (only out of those rule engine ALLOWED)
    n_would_block = len([r for r in matched if r["gate_allowed"] and r["score"] < threshold])
    n_total_allowed = len([r for r in matched if r["gate_allowed"]])
    block_rate = n_would_block / n_total_allowed if n_total_allowed else 0.0

    # False positive: model blocked it, but it was a winner that rule engine allowed
    false_positives = [r for r in blocked_outcomes if r["realized_pnl"] > 0]
    fp_rate = len(false_positives) / len(blocked_outcomes) if blocked_outcomes else 0.0

    # Coverage: scored / total ALL traces in period
    total_traces = len(trace_outcomes.values())
    coverage = total_scored / total_traces if total_traces else 0.0

    # Concordance: ML and Rule Engine Agree (both pass OR both block)
    agreements = [
        r for r in matched 
        if (r["score"] >= 0.5 and r["gate_allowed"]) or (r["score"] < 0.5 and not r["gate_allowed"])
    ]
    concordance = len(agreements) / total_scored if total_scored else 0.0

    # ── Print results ──────────────────────────────────────────────────────────
    print("\n" + "─" * 70)
    print("SHADOW ANALYSIS RESULTS")
    print("─" * 70)
    print(f"Total scored entries     : {total_scored}")
    print(f"  -> ML passes (of gate allowed)   : {len(ml_outcomes)}")
    print(f"  -> ML blocks (of gate allowed)   : {n_would_block}")
    print()
    print(f"Baseline Win Rate        : {baseline_wr * 100:.1f}%" if baseline_wr is not None else "Baseline Win Rate        : N/A")
    print(f"ML filtered Win Rate     : {pass_wr * 100:.1f}%" if pass_wr is not None else "ML filtered Win Rate     : N/A")
    print(f"Win rate of blocked      : {block_wr * 100:.1f}%" if block_wr is not None else "Win rate of blocked      : N/A")
    print(f"True improvement (Lift)  : {precision_lift * 100:+.1f} pp" if precision_lift is not None else "True improvement (Lift)  : N/A")
    print(f"Block rate on allowed    : {block_rate * 100:.1f}% ({n_would_block}/{n_total_allowed})")
    print(f"False positive rate      : {fp_rate * 100:.1f}% (would-block that were wins)")
    print(f"Model coverage           : {coverage * 100:.1f}% ({total_scored}/{total_traces})")
    print(f"Concordance (score>0.5)  : {concordance * 100:.1f}%")
    print(f"Shadow days elapsed      : {days_elapsed}")

    # ── Apply GO/NO-GO criteria ────────────────────────────────────────────────
    print("\n" + "─" * 70)
    print("GO / NO-GO CRITERIA")
    print("─" * 70)

    checks = []

    def check(name: str, passed: bool, detail: str) -> bool:
        symbol = "[OK]" if passed else "[FAIL]"
        print(f"  {symbol} {name}: {detail}")
        checks.append(passed)
        return passed

    check(
        "Min shadow days",
        days_elapsed >= min_days,
        f"{days_elapsed} days (need ≥ {min_days})",
    )
    check(
        "Min scored entries",
        total_scored >= 100,
        f"{total_scored} entries (need ≥ 100)",
    )
    check(
        "Model coverage",
        coverage >= 0.80,
        f"{coverage * 100:.1f}% (need ≥ 80%)",
    )
    check(
        "Precision lift",
        precision_lift is not None and precision_lift > 0.0,
        (f"{precision_lift * 100:+.1f} pp (need > 0)" if precision_lift is not None else "N/A -- need closed trades"),
    )
    check(
        "Block rate",
        block_rate <= 0.40,
        f"{block_rate * 100:.1f}% (need ≤ 40%)",
    )
    check(
        "False positive rate",
        fp_rate <= 0.30,
        f"{fp_rate * 100:.1f}% (need ≤ 30%)",
    )
    check(
        "Concordance",
        concordance >= 0.70,
        f"{concordance * 100:.1f}% (need ≥ 70%)",
    )

    all_pass = all(checks)
    print()
    if all_pass:
        print("[GO] RECOMMENDATION: GO -- activate ML scorer in live mode.")
        print("   Set ML_SHADOW_MODE=False in .env and restart the bot.")
    else:
        n_fail = sum(1 for c in checks if not c)
        print(f"[NOGO] RECOMMENDATION: NO-GO -- {n_fail} criteria not met.")
        print("   Continue shadow deployment. Revisit after more data accumulates.")

    # ── Optional CSV export ────────────────────────────────────────────────────
    if output_csv:
        try:
            import csv
            output_csv.parent.mkdir(parents=True, exist_ok=True)
            fieldnames = ["trace_id", "symbol", "score", "action", "model_version",
                          "realized_pnl", "gate_allowed"]
            with open(output_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(matched)
            print(f"\nDetailed results written to: {output_csv}")
        except Exception as exc:
            print(f"\nWarning: CSV export failed: {exc}")

    return 0 if all_pass else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyze ML shadow deployment logs and produce GO/NO-GO recommendation.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=Path("models/logs"),
        help="Directory containing predictions_YYYYMMDD.jsonl files (default: models/logs)",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/bot.db"),
        help="Path to bot SQLite database (default: data/bot.db)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.40,
        help="ML score threshold to evaluate (default: 0.40 -- must match ML_SCORE_THRESHOLD)",
    )
    parser.add_argument(
        "--min-days",
        type=int,
        default=30,
        help="Minimum shadow days required for GO (default: 30)",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Optional path to write detailed per-prediction CSV",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print additional debug output",
    )

    args = parser.parse_args()

    return analyze(
        log_dir=args.log_dir,
        db_path=args.db_path,
        threshold=args.threshold,
        min_days=args.min_days,
        output_csv=args.output_csv,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    sys.exit(main())
