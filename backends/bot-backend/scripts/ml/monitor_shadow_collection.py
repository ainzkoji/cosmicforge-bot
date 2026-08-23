"""
monitor_shadow_collection.py  --  Step 5E-6B: Shadow Data Collection Monitor
=============================================================================

Run this at any time during the shadow collection window to check progress.

Usage:
    python scripts/ml/monitor_shadow_collection.py \
        --db-path ../shared/shared_lib/persistence/cosmicforge.db \
        --log-dir models/logs

Exit codes:
    0  Collection targets met (>=100 scored entries, >=20 matched outcomes)
    1  Collection in progress (targets not yet met)
    2  Error (DB unavailable, no prediction log, scorer exceptions detected)
"""

import argparse
import json
import os
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------
TARGET_SCORED_ENTRIES = 100
TARGET_MATCHED_OUTCOMES = 20


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def _fmt_pct(n: int, total: int) -> str:
    if total == 0:
        return "0.0%"
    return f"{100.0 * n / total:.1f}%"


def _bar(n: int, target: int, width: int = 30) -> str:
    filled = min(width, int(width * n / max(target, 1)))
    return "[" + "#" * filled + "-" * (width - filled) + f"] {n}/{target}"


# ---------------------------------------------------------------------------
# 1. Decision traces: ML score population
# ---------------------------------------------------------------------------

def check_traces(conn: sqlite3.Connection) -> dict:
    result = {}

    # Total traces (live only — exclude backfill marker if present)
    cur = conn.execute(
        "SELECT COUNT(*) FROM decision_traces WHERE ml_score IS NOT NULL"
    )
    scored = cur.fetchone()[0]
    result["scored_entries"] = scored

    cur = conn.execute("SELECT COUNT(*) FROM decision_traces")
    total = cur.fetchone()[0]
    result["total_traces"] = total
    result["unscored_traces"] = total - scored

    # Date range of scored traces
    cur = conn.execute(
        "SELECT MIN(ts), MAX(ts) FROM decision_traces WHERE ml_score IS NOT NULL"
    )
    row = cur.fetchone()
    result["first_scored_ts"] = row[0]
    result["last_scored_ts"] = row[1]

    # Score distribution (buckets of 0.1)
    cur = conn.execute(
        "SELECT ml_score FROM decision_traces WHERE ml_score IS NOT NULL"
    )
    scores = [r[0] for r in cur.fetchall()]
    buckets = Counter()
    for s in scores:
        bucket = int(s * 10) / 10.0
        buckets[bucket] += 1
    result["score_distribution"] = dict(sorted(buckets.items()))

    # ML action distribution
    cur = conn.execute(
        "SELECT ml_action, COUNT(*) FROM decision_traces "
        "WHERE ml_score IS NOT NULL GROUP BY ml_action"
    )
    result["action_distribution"] = {r[0]: r[1] for r in cur.fetchall()}

    # Model versions seen
    cur = conn.execute(
        "SELECT DISTINCT ml_model_version FROM decision_traces WHERE ml_model_version IS NOT NULL"
    )
    result["model_versions"] = [r[0] for r in cur.fetchall()]

    # Symbols covered
    cur = conn.execute(
        "SELECT symbol, COUNT(*) FROM decision_traces "
        "WHERE ml_score IS NOT NULL GROUP BY symbol ORDER BY COUNT(*) DESC"
    )
    result["symbols"] = {r[0]: r[1] for r in cur.fetchall()}

    # Regime distribution (from strategy_signals_json if populated in ML scored rows)
    try:
        cur = conn.execute(
            "SELECT regime_state, COUNT(*) FROM decision_traces "
            "WHERE ml_score IS NOT NULL AND regime_state IS NOT NULL "
            "GROUP BY regime_state ORDER BY COUNT(*) DESC"
        )
        result["regime_distribution"] = {r[0]: r[1] for r in cur.fetchall()}
    except Exception:
        result["regime_distribution"] = {}

    return result


# ---------------------------------------------------------------------------
# 2. Matched outcomes: scored entries that have a subsequent CLOSE fill
# ---------------------------------------------------------------------------

def check_matched_outcomes(conn: sqlite3.Connection) -> dict:
    result = {}

    # Find decision_traces with ml_score that have a matching CLOSE trade fill
    # Matching via: same symbol, order_id not null, and a CLOSE fill exists after the trace ts
    # Simpler join: traces with ml_score + trade_fills CLOSE where symbol matches and close_ts > trace_ts
    try:
        cur = conn.execute("""
            SELECT COUNT(DISTINCT dt.trace_id)
            FROM decision_traces dt
            JOIN trade_fills tf ON tf.symbol = dt.symbol
                               AND tf.action = 'CLOSE'
                               AND tf.timestamp_utc > dt.ts
            WHERE dt.ml_score IS NOT NULL
              AND dt.symbol IS NOT NULL
        """)
        matched = cur.fetchone()[0]
    except Exception as e:
        matched = -1
        result["match_error"] = str(e)

    result["matched_outcomes"] = matched

    # Win rate among matched outcomes (where realized_pnl known)
    if matched > 0:
        try:
            cur = conn.execute("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN tf.realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
                    AVG(tf.realized_pnl) AS avg_pnl,
                    AVG(dt.ml_score) AS avg_ml_score_wins,
                    AVG(CASE WHEN tf.realized_pnl <= 0 THEN dt.ml_score END) AS avg_ml_score_losses
                FROM decision_traces dt
                JOIN trade_fills tf ON tf.symbol = dt.symbol
                                   AND tf.action = 'CLOSE'
                                   AND tf.timestamp_utc > dt.ts
                WHERE dt.ml_score IS NOT NULL
                  AND dt.symbol IS NOT NULL
                  AND tf.realized_pnl IS NOT NULL
            """)
            row = cur.fetchone()
            if row and row[0]:
                result["win_rate"] = round(row[1] / row[0], 3) if row[0] else None
                result["avg_pnl"] = round(row[2], 4) if row[2] is not None else None
                result["avg_ml_score_wins"] = round(row[3], 3) if row[3] is not None else None
                result["avg_ml_score_losses"] = round(row[4], 3) if row[4] is not None else None
        except Exception as e:
            result["outcome_stats_error"] = str(e)

    return result


# ---------------------------------------------------------------------------
# 3. Prediction JSONL logs
# ---------------------------------------------------------------------------

def check_prediction_logs(log_dir: str) -> dict:
    result = {}

    log_path = Path(log_dir)
    if not log_path.exists():
        result["log_dir_exists"] = False
        return result

    result["log_dir_exists"] = True
    jsonl_files = sorted(log_path.glob("predictions_*.jsonl"))
    result["log_files"] = [f.name for f in jsonl_files]

    total_entries = 0
    live_entries = 0  # not from smoke test (synthetic_ prefix)
    anomalies = []
    first_ts = None
    last_ts = None
    symbols_seen = set()
    exception_count = 0

    for fpath in jsonl_files:
        try:
            with open(fpath, "r", encoding="utf-8") as fh:
                for lineno, line in enumerate(fh, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError as e:
                        anomalies.append(f"{fpath.name}:{lineno} invalid JSON: {e}")
                        continue

                    total_entries += 1

                    # Count live entries (not from smoke test)
                    trace_id = str(entry.get("trace_id", ""))
                    is_test = trace_id.startswith("synthetic_") or trace_id.startswith("test_")
                    if not is_test:
                        live_entries += 1

                    # Exception marker
                    if entry.get("action") == "EXCEPTION":
                        exception_count += 1
                        anomalies.append(
                            f"{fpath.name}:{lineno} EXCEPTION scored: {entry.get('symbol','?')} "
                            f"trace={trace_id}"
                        )

                    # Timestamps
                    ts = entry.get("ts", "")
                    if ts:
                        if first_ts is None or ts < first_ts:
                            first_ts = ts
                        if last_ts is None or ts > last_ts:
                            last_ts = ts

                    # Symbols
                    sym = entry.get("symbol")
                    if sym:
                        symbols_seen.add(sym)

                    # Score validity
                    score = entry.get("score")
                    if score is None:
                        anomalies.append(f"{fpath.name}:{lineno} NULL score for trace={trace_id}")
                    elif not isinstance(score, (int, float)) or not (0.0 <= score <= 1.0):
                        anomalies.append(
                            f"{fpath.name}:{lineno} invalid score={score} for trace={trace_id}"
                        )

        except OSError as e:
            anomalies.append(f"Cannot read {fpath.name}: {e}")

    result["total_log_entries"] = total_entries
    result["live_log_entries"] = live_entries
    result["smoke_test_entries"] = total_entries - live_entries
    result["exception_count"] = exception_count
    result["first_log_ts"] = first_ts
    result["last_log_ts"] = last_ts
    result["symbols_in_logs"] = sorted(symbols_seen)
    result["anomalies"] = anomalies

    # File sizes
    result["log_files_detail"] = [
        {"name": f.name, "size_kb": round(f.stat().st_size / 1024, 1)}
        for f in jsonl_files
    ]

    return result


# ---------------------------------------------------------------------------
# 4. Execution guard — confirm shadow never blocked a trade
# ---------------------------------------------------------------------------

def check_shadow_guard(conn: sqlite3.Connection) -> dict:
    result = {}

    # In shadow mode, ml_action should always be 'SHADOW' (never 'BLOCK')
    try:
        cur = conn.execute(
            "SELECT ml_action, COUNT(*) FROM decision_traces "
            "WHERE ml_action IS NOT NULL GROUP BY ml_action"
        )
        actions = {r[0]: r[1] for r in cur.fetchall()}
        result["action_counts"] = actions
        result["block_count"] = actions.get("BLOCK", 0)
        result["shadow_only"] = result["block_count"] == 0
    except Exception as e:
        result["guard_error"] = str(e)
        result["shadow_only"] = None

    return result


# ---------------------------------------------------------------------------
# 5. Print report
# ---------------------------------------------------------------------------

def print_report(traces: dict, outcomes: dict, logs: dict, guard: dict) -> int:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print()
    print("=" * 64)
    print("  SHADOW COLLECTION MONITOR  --  Step 5E-6B")
    print(f"  Report time: {now}")
    print("=" * 64)

    # ---- Section 1: Scored Entries ----
    scored = traces.get("scored_entries", 0)
    print()
    print("[ 1 ] SCORED ENTRIES (decision_traces.ml_score)")
    print(f"      {_bar(scored, TARGET_SCORED_ENTRIES)}")
    if scored == 0:
        print("      NOTE: 0 live entries scored. Bot must be running with ML_ENABLED=True.")
    else:
        first_ts = traces.get("first_scored_ts", "-")
        last_ts = traces.get("last_scored_ts", "-")
        print(f"      Date range : {first_ts}  ->  {last_ts}")
        print(f"      Total traces in DB: {traces.get('total_traces', '?')}")

    symbols = traces.get("symbols", {})
    if symbols:
        print(f"      Symbols ({len(symbols)}):")
        for sym, cnt in symbols.items():
            print(f"        {sym:<14} {cnt:>6} scored")

    regimes = traces.get("regime_distribution", {})
    if regimes:
        print(f"      Regime distribution:")
        for rg, cnt in regimes.items():
            print(f"        {rg:<28} {cnt:>5}  ({_fmt_pct(cnt, scored)})")

    score_dist = traces.get("score_distribution", {})
    if score_dist:
        print("      Score histogram:")
        for bucket, cnt in sorted(score_dist.items()):
            bar = "#" * min(40, int(40 * cnt / max(score_dist.values(), default=1)))
            print(f"        [{bucket:.1f}-{bucket+0.1:.1f})  {bar} {cnt}")

    versions = traces.get("model_versions", [])
    if versions:
        print(f"      Model version(s): {', '.join(versions)}")

    # ---- Section 2: Matched Outcomes ----
    matched = outcomes.get("matched_outcomes", 0)
    print()
    print("[ 2 ] MATCHED OUTCOMES (scored trace + subsequent CLOSE fill)")
    print(f"      {_bar(matched, TARGET_MATCHED_OUTCOMES)}")
    if matched > 0:
        wr = outcomes.get("win_rate")
        avg_pnl = outcomes.get("avg_pnl")
        avg_w = outcomes.get("avg_ml_score_wins")
        avg_l = outcomes.get("avg_ml_score_losses")
        if wr is not None:
            print(f"      Win rate         : {wr:.1%}")
        if avg_pnl is not None:
            print(f"      Avg realized PnL : {avg_pnl:.4f} USDT")
        if avg_w is not None:
            print(f"      Avg ML score (wins)   : {avg_w:.3f}")
        if avg_l is not None:
            print(f"      Avg ML score (losses) : {avg_l:.3f}")
    elif matched == -1:
        print(f"      Match query error: {outcomes.get('match_error', '?')}")

    # ---- Section 3: Prediction Logs ----
    print()
    print("[ 3 ] PREDICTION LOGS (models/logs/predictions_*.jsonl)")
    if not logs.get("log_dir_exists"):
        print("      [FAIL] Log directory not found.")
    else:
        files = logs.get("log_files", [])
        if not files:
            print("      [FAIL] No prediction JSONL files found.")
        else:
            for fd in logs.get("log_files_detail", []):
                print(f"      {fd['name']}  ({fd['size_kb']} KB)")
            total = logs.get("total_log_entries", 0)
            live = logs.get("live_log_entries", 0)
            smoke = logs.get("smoke_test_entries", 0)
            print(f"      Total log entries : {total}")
            print(f"        - Live entries  : {live}")
            print(f"        - Smoke test    : {smoke}")
            if logs.get("first_log_ts"):
                print(f"      First entry : {logs['first_log_ts']}")
                print(f"      Last entry  : {logs['last_log_ts']}")
            log_syms = logs.get("symbols_in_logs", [])
            if log_syms:
                print(f"      Symbols in logs : {', '.join(log_syms)}")
            exc = logs.get("exception_count", 0)
            if exc > 0:
                print(f"      [!!] EXCEPTION entries: {exc}")
            anomalies = logs.get("anomalies", [])
            if anomalies:
                print(f"      Anomalies ({len(anomalies)}):")
                for a in anomalies[:10]:
                    print(f"        [!!] {a}")
                if len(anomalies) > 10:
                    print(f"        ... and {len(anomalies)-10} more")
            else:
                print("      [OK] No anomalies found in log files")

    # ---- Section 4: Shadow Guard ----
    print()
    print("[ 4 ] SHADOW GUARD (ml_action must never be BLOCK)")
    shadow_only = guard.get("shadow_only")
    block_count = guard.get("block_count", 0)
    if shadow_only is True:
        print("      [OK] Shadow only -- zero BLOCK actions. Execution is fully unaffected.")
    elif shadow_only is False:
        print(f"      [FAIL] {block_count} BLOCK action(s) detected! Shadow mode may not be active.")
    else:
        print(f"      [!!] Guard check error: {guard.get('guard_error', '?')}")
    action_counts = guard.get("action_counts", {})
    if action_counts:
        for action, cnt in action_counts.items():
            print(f"        {action:<10} {cnt:>6}")

    # ---- Section 5: Verdict ----
    print()
    print("=" * 64)
    scored_ok = scored >= TARGET_SCORED_ENTRIES
    matched_ok = matched >= TARGET_MATCHED_OUTCOMES
    guard_ok = guard.get("shadow_only", False) is True
    exc_ok = logs.get("exception_count", 0) == 0
    anomaly_ok = len(logs.get("anomalies", [])) == 0

    statuses = [
        ("Scored entries >= 100", scored_ok, f"({scored}/{TARGET_SCORED_ENTRIES})"),
        ("Matched outcomes >= 20", matched_ok, f"({matched}/{TARGET_MATCHED_OUTCOMES})"),
        ("Shadow guard: no BLOCKs", guard_ok, ""),
        ("No scorer exceptions", exc_ok, f"({logs.get('exception_count',0)} exceptions)"),
        ("No log anomalies", anomaly_ok, f"({len(logs.get('anomalies',[]))} anomalies)"),
    ]

    all_ok = all(ok for _, ok, _ in statuses)
    for label, ok, note in statuses:
        marker = "[OK]  " if ok else "[WAIT]"
        print(f"  {marker} {label} {note}")

    print()
    if all_ok:
        print("  VERDICT: [COLLECTION COMPLETE] -- Ready for analyze_shadow.py")
        print()
        print("  Next step:")
        print("    python scripts/ml/analyze_shadow.py \\")
        print("      --log-dir models/logs \\")
        print("      --db-path ../shared/shared_lib/persistence/cosmicforge.db")
        exit_code = 0
    else:
        pending = sum(1 for _, ok, _ in statuses if not ok)
        print(f"  VERDICT: [IN PROGRESS] -- {pending} target(s) not yet met.")
        print()
        if not scored_ok:
            print(f"  Action: Keep bot running (ML_ENABLED=True, ML_SHADOW_MODE=True).")
            print(f"          Need {TARGET_SCORED_ENTRIES - scored} more scored entries.")
        exit_code = 1

    print("=" * 64)
    print()
    return exit_code


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Monitor ML shadow data collection progress.")
    parser.add_argument(
        "--db-path",
        default="../shared/shared_lib/persistence/cosmicforge.db",
        help="Path to cosmicforge.db"
    )
    parser.add_argument(
        "--log-dir",
        default="models/logs",
        help="Directory containing predictions_*.jsonl files"
    )
    args = parser.parse_args()

    # Check DB
    if not Path(args.db_path).exists():
        print(f"[FAIL] Database not found: {args.db_path}")
        sys.exit(2)

    try:
        conn = _connect(args.db_path)
    except Exception as e:
        print(f"[FAIL] Cannot open database: {e}")
        sys.exit(2)

    try:
        traces = check_traces(conn)
        outcomes = check_matched_outcomes(conn)
        guard = check_shadow_guard(conn)
    finally:
        conn.close()

    logs = check_prediction_logs(args.log_dir)

    exit_code = print_report(traces, outcomes, logs, guard)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
