"""
monitor_live_gating.py  --  Post-promotion live ML gating monitor (Step 5E-9).

Reads the decision_traces DB and daily JSONL prediction logs to report on:
  - Blocked vs allowed trade counts (per symbol, per regime)
  - Volume impact (trade-count drop vs pre-ML baseline)
  - Win rate of ML-allowed trades vs baseline
  - Score distribution and bin stability
  - Runtime/scorer exceptions (SKIP actions without ML_ENABLED=False)
  - Final verdict: KEEP LIVE / ADJUST THRESHOLD / ROLLBACK

Usage:
    python scripts/ml/monitor_live_gating.py
        [--db-path PATH]          default: ../shared/shared_lib/persistence/cosmicforge.db
        [--log-dir DIR]           default: models/logs
        [--days N]                monitoring window in days (default: 14)
        [--threshold FLOAT]       expected threshold to audit (default: 0.40)
        [--baseline-win-rate F]   pre-ML baseline win rate (default: 0.33)
        [--min-trades N]          minimum trades needed for verdict (default: 50)
        [--json]                  emit machine-readable JSON summary

Exit codes:
    0  -- KEEP LIVE (all checks pass)
    1  -- ADJUST (threshold or config change recommended)
    2  -- ROLLBACK (immediate rollback recommended)
    3  -- INSUFFICIENT DATA (fewer than --min-trades scored)
    4  -- ERROR (unrecoverable script failure)
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

_DEFAULT_DB = Path(__file__).parent.parent.parent / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
_DEFAULT_LOG_DIR = Path(__file__).parent.parent.parent / "backends" / "bot-backend" / "models" / "logs"

# Resolve relative to script location
_BOT_ROOT = Path(__file__).parent.parent.parent
_DEFAULT_DB_LOCAL = _BOT_ROOT / ".." / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
_DEFAULT_LOG_DIR_LOCAL = _BOT_ROOT / "models" / "logs"


# ---------------------------------------------------------------------------
# Thresholds for verdict logic
# ---------------------------------------------------------------------------

# Win-rate uplift: ML-allowed trades must beat baseline by at least this much
_MIN_WIN_RATE_DELTA = -0.03   # allow up to 3pp below baseline before flagging

# Block rate: if >60% of potential entries are blocked, something is wrong
_MAX_BLOCK_RATE = 0.60

# Score anomaly: SKIP rate among enabled-scorer cycles should be <5%
_MAX_SKIP_RATE = 0.05

# Minimum trades in highest score bin (>= 0.7) needed to validate distribution
_MIN_HIGH_SCORE_TRADES = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _open_db(db_path: Path) -> Optional[sqlite3.Connection]:
    if not db_path.exists():
        return None
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _window_ts(days: int) -> str:
    """ISO timestamp for start of monitoring window."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return cutoff.isoformat()


def _load_jsonl_logs(log_dir: Path, days: int) -> List[dict]:
    """Load all prediction records from JSONL files within the monitoring window."""
    records = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    if not log_dir.exists():
        return records
    for fpath in sorted(log_dir.glob("predictions_*.jsonl")):
        try:
            # File name: predictions_YYYYMMDD.jsonl
            date_str = fpath.stem.replace("predictions_", "")
            file_date = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
            if file_date < cutoff - timedelta(days=1):
                continue  # skip files clearly before window
        except ValueError:
            pass  # include files with unexpected names

        try:
            with open(fpath, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        ts_str = rec.get("ts", "")
                        if ts_str:
                            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                            if ts < cutoff:
                                continue
                        records.append(rec)
                    except (json.JSONDecodeError, ValueError):
                        continue
        except OSError:
            continue
    return records


def _safe_rate(num: int, denom: int, default: float = 0.0) -> float:
    return num / denom if denom > 0 else default


def _fmt_pct(val: float) -> str:
    return f"{val*100:.1f}%"


# ---------------------------------------------------------------------------
# DB queries
# ---------------------------------------------------------------------------

def _query_ml_decisions(conn: sqlite3.Connection, cutoff_ts: str) -> List[dict]:
    """
    Load decision_traces rows that have ml_score populated within the window.
    Joins with trade_fills to get realized_pnl for win-rate computation.
    """
    sql = """
        SELECT
            dt.trace_id,
            dt.symbol,
            dt.ts,
            dt.regime_state,
            dt.ml_score,
            dt.ml_action,
            dt.ml_model_version,
            dt.ml_threshold,
            -- outcome: join to trade_fills via order_id or position_id
            tf.realized_pnl,
            tf.action AS fill_action
        FROM decision_traces dt
        LEFT JOIN trade_fills tf
            ON dt.order_id = tf.order_id
            AND tf.action = 'CLOSE'
        WHERE dt.ts >= ?
          AND dt.ml_score IS NOT NULL
        ORDER BY dt.ts ASC
    """
    try:
        rows = conn.execute(sql, (cutoff_ts,)).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        # Fallback: decision_traces without join (older schema)
        try:
            rows = conn.execute(
                """
                SELECT trace_id, symbol, ts, regime_state,
                       ml_score, ml_action, ml_model_version, ml_threshold
                FROM decision_traces
                WHERE ts >= ? AND ml_score IS NOT NULL
                ORDER BY ts ASC
                """,
                (cutoff_ts,),
            ).fetchall()
            return [dict(r) for r in rows]
        except sqlite3.OperationalError:
            return []


def _query_baseline_win_rate(conn: sqlite3.Connection, cutoff_ts: str) -> Optional[float]:
    """
    Compute pre-ML or overall win rate from trade_fills (CLOSE records with realized_pnl).
    Used as the baseline comparison for ML-allowed trades.
    """
    try:
        rows = conn.execute(
            """
            SELECT realized_pnl FROM trade_fills
            WHERE action = 'CLOSE'
              AND realized_pnl IS NOT NULL
              AND timestamp_utc >= ?
            """,
            (cutoff_ts,),
        ).fetchall()
        if not rows:
            return None
        wins = sum(1 for r in rows if r["realized_pnl"] > 0)
        return wins / len(rows)
    except sqlite3.OperationalError:
        return None


def _query_pre_ml_trade_count(conn: sqlite3.Connection, pre_ml_cutoff: str, ml_start: str) -> int:
    """Count trades in the pre-ML window for volume-impact baseline."""
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) as cnt FROM decision_traces
            WHERE ts >= ? AND ts < ?
              AND ml_score IS NULL
            """,
            (pre_ml_cutoff, ml_start),
        ).fetchone()
        return row["cnt"] if row else 0
    except sqlite3.OperationalError:
        return 0


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def _analyze(
    db_rows: List[dict],
    jsonl_records: List[dict],
    baseline_win_rate: float,
    threshold: float,
) -> dict:
    """Run all analysis on the combined DB + JSONL data. Returns structured results dict."""

    total_scored = len(db_rows)
    blocked = [r for r in db_rows if r.get("ml_action") == "BLOCK"]
    allowed = [r for r in db_rows if r.get("ml_action") in ("ALLOW", None) and r.get("ml_score") is not None]
    shadow = [r for r in db_rows if r.get("ml_action") == "SHADOW"]
    skip_db = [r for r in db_rows if r.get("ml_action") == "SKIP"]

    # From JSONL (may include entries not yet in DB)
    jsonl_blocked = [r for r in jsonl_records if r.get("action") == "BLOCK"]
    jsonl_allowed = [r for r in jsonl_records if r.get("action") == "ALLOW"]
    jsonl_shadow = [r for r in jsonl_records if r.get("action") == "SHADOW"]
    jsonl_skip = [r for r in jsonl_records if r.get("action") == "SKIP"]
    jsonl_shadow_mode_on = any(r.get("shadow_mode", True) for r in jsonl_records)

    # Use JSONL as primary count source (more complete for recent sessions)
    total_jsonl = len(jsonl_records)
    use_jsonl = total_jsonl > total_scored

    n_blocked = len(jsonl_blocked) if use_jsonl else len(blocked)
    n_allowed = len(jsonl_allowed) if use_jsonl else len(allowed)
    n_shadow = len(jsonl_shadow) if use_jsonl else len(shadow)
    n_skip = len(jsonl_skip) if use_jsonl else len(skip_db)
    n_total = total_jsonl if use_jsonl else total_scored

    block_rate = _safe_rate(n_blocked, n_blocked + n_allowed)
    skip_rate = _safe_rate(n_skip, n_total)

    # Shadow mode guard
    shadow_mode_still_on = jsonl_shadow_mode_on or (n_shadow > 0 and n_blocked == 0)

    # Win-rate analysis (requires DB join — only DB rows have realized_pnl)
    with_outcome = [r for r in db_rows if r.get("realized_pnl") is not None]
    allowed_with_outcome = [r for r in with_outcome if r.get("ml_action") in ("ALLOW", None)]
    blocked_with_outcome = [r for r in with_outcome if r.get("ml_action") == "BLOCK"]

    allowed_wins = sum(1 for r in allowed_with_outcome if r.get("realized_pnl", 0) > 0)
    blocked_wins = sum(1 for r in blocked_with_outcome if r.get("realized_pnl", 0) > 0)

    allowed_win_rate = _safe_rate(allowed_wins, len(allowed_with_outcome), default=float("nan"))
    blocked_win_rate = _safe_rate(blocked_wins, len(blocked_with_outcome), default=float("nan"))

    # Score distribution (from JSONL — has scores for all actions)
    scores_all = [r["score"] for r in jsonl_records if r.get("score") is not None]
    bins = {"0.0-0.2": 0, "0.2-0.4": 0, "0.4-0.6": 0, "0.6-0.8": 0, "0.8-1.0": 0}
    for s in scores_all:
        if s < 0.2:
            bins["0.0-0.2"] += 1
        elif s < 0.4:
            bins["0.2-0.4"] += 1
        elif s < 0.6:
            bins["0.4-0.6"] += 1
        elif s < 0.8:
            bins["0.6-0.8"] += 1
        else:
            bins["0.8-1.0"] += 1

    # Per-symbol breakdown
    by_symbol: Dict[str, dict] = defaultdict(lambda: {"blocked": 0, "allowed": 0, "wins": 0, "outcomes": 0})
    for r in jsonl_records:
        sym = r.get("symbol", "UNKNOWN")
        act = r.get("action", "")
        if act == "BLOCK":
            by_symbol[sym]["blocked"] += 1
        elif act == "ALLOW":
            by_symbol[sym]["allowed"] += 1
    for r in db_rows:
        sym = r.get("symbol", "UNKNOWN")
        if r.get("realized_pnl") is not None:
            by_symbol[sym]["outcomes"] += 1
            if r["realized_pnl"] > 0:
                by_symbol[sym]["wins"] += 1

    # Per-regime breakdown
    by_regime: Dict[str, dict] = defaultdict(lambda: {"blocked": 0, "allowed": 0, "wins": 0, "outcomes": 0})
    for r in db_rows:
        reg = r.get("regime_state", "UNKNOWN") or "UNKNOWN"
        act = r.get("ml_action", "")
        if act == "BLOCK":
            by_regime[reg]["blocked"] += 1
        elif act in ("ALLOW", None) and r.get("ml_score") is not None:
            by_regime[reg]["allowed"] += 1
        if r.get("realized_pnl") is not None:
            by_regime[reg]["outcomes"] += 1
            if r["realized_pnl"] > 0:
                by_regime[reg]["wins"] += 1

    # Model version consistency
    versions_seen = set(r.get("model_version") for r in jsonl_records if r.get("model_version"))
    threshold_seen = set(r.get("threshold") for r in jsonl_records if r.get("threshold") is not None)
    threshold_mismatch = any(abs(t - threshold) > 0.001 for t in threshold_seen if t is not None)

    return {
        "total_scored": n_total,
        "n_blocked": n_blocked,
        "n_allowed": n_allowed,
        "n_shadow": n_shadow,
        "n_skip": n_skip,
        "block_rate": block_rate,
        "skip_rate": skip_rate,
        "shadow_mode_still_on": shadow_mode_still_on,
        "allowed_with_outcome": len(allowed_with_outcome),
        "blocked_with_outcome": len(blocked_with_outcome),
        "allowed_win_rate": allowed_win_rate,
        "blocked_win_rate": blocked_win_rate,
        "baseline_win_rate": baseline_win_rate,
        "win_rate_delta": (allowed_win_rate - baseline_win_rate) if not math.isnan(allowed_win_rate) else float("nan"),
        "score_bins": bins,
        "by_symbol": {k: dict(v) for k, v in by_symbol.items()},
        "by_regime": {k: dict(v) for k, v in by_regime.items()},
        "model_versions": sorted(versions_seen),
        "threshold_mismatch": threshold_mismatch,
        "thresholds_seen": sorted(threshold_seen),
        "scores_count": len(scores_all),
    }


# ---------------------------------------------------------------------------
# Verdict logic
# ---------------------------------------------------------------------------

def _verdict(results: dict, min_trades: int) -> Tuple[str, List[str], List[str]]:
    """
    Determine KEEP LIVE / ADJUST / ROLLBACK / INSUFFICIENT DATA.
    Returns (verdict_str, issues_list, recommendations_list).
    """
    issues = []
    recommendations = []

    n_total = results["total_scored"]
    if n_total < min_trades:
        return (
            "INSUFFICIENT DATA",
            [f"Only {n_total} scored entries — need {min_trades} for a reliable verdict"],
            ["Continue running. Re-run this script after more trades accumulate."],
        )

    # Shadow mode check — promotion hasn't taken effect
    if results["shadow_mode_still_on"]:
        issues.append("Shadow mode appears still ON (no BLOCK actions seen) — check .env ML_SHADOW_MODE setting")
        recommendations.append("Verify .env ML_SHADOW_MODE=False and restart the bot")

    # Block rate check
    if results["block_rate"] > _MAX_BLOCK_RATE:
        issues.append(
            f"Block rate {_fmt_pct(results['block_rate'])} is very high (>{_fmt_pct(_MAX_BLOCK_RATE)}) "
            f"— model may be too aggressive"
        )
        recommendations.append(
            f"Lower ML_SCORE_THRESHOLD from current value or review model calibration. "
            f"If win rate delta is also negative, consider rollback to shadow mode."
        )

    # Skip rate check
    if results["skip_rate"] > _MAX_SKIP_RATE:
        issues.append(
            f"SKIP rate {_fmt_pct(results['skip_rate'])} is elevated (>{_fmt_pct(_MAX_SKIP_RATE)}) "
            f"— scorer may be failing to load"
        )
        recommendations.append("Check scorer startup logs. Verify model artifact files exist at configured paths.")

    # Win rate delta check
    wr_delta = results["win_rate_delta"]
    if not math.isnan(wr_delta) and results["allowed_with_outcome"] >= 20:
        if wr_delta < _MIN_WIN_RATE_DELTA:
            issues.append(
                f"ML-allowed win rate ({_fmt_pct(results['allowed_win_rate'])}) is "
                f"{_fmt_pct(abs(wr_delta))} BELOW baseline ({_fmt_pct(results['baseline_win_rate'])}) "
                f"— model is not filtering correctly"
            )
            recommendations.append(
                "Raise ML_SCORE_THRESHOLD or revert to shadow mode and retrain with more live data."
            )

    # Threshold mismatch
    if results["threshold_mismatch"]:
        issues.append(
            f"Threshold mismatch detected — expected {results['thresholds_seen']} in logs "
            f"but config says {results.get('config_threshold', '?')}"
        )
        recommendations.append("Restart bot to reload config from .env.")

    # Model version drift
    if len(results["model_versions"]) > 1:
        issues.append(f"Multiple model versions seen in logs: {results['model_versions']} — possible mid-session reload")
        recommendations.append("Ensure bot is restarted cleanly between model updates.")

    # Score distribution: check for bimodal collapse
    bins = results["score_bins"]
    high_pct = _safe_rate(bins.get("0.8-1.0", 0) + bins.get("0.6-0.8", 0), results["scores_count"])
    low_pct = _safe_rate(bins.get("0.0-0.2", 0) + bins.get("0.2-0.4", 0), results["scores_count"])
    if high_pct > 0.90 or low_pct > 0.90:
        issues.append(
            f"Score distribution appears collapsed — {_fmt_pct(high_pct)} high scores or "
            f"{_fmt_pct(low_pct)} low scores. Calibration may not be generalizing on live data."
        )
        recommendations.append(
            "This is expected in early live data if distribution is very different from backfill. "
            "Monitor for 30+ days before retraining."
        )

    # Determine verdict
    critical_issues = [i for i in issues if "BELOW baseline" in i or "failing to load" in i or "still ON" in i]
    rollback_triggers = [i for i in issues if "not filtering correctly" in i or "failing to load" in i]

    if rollback_triggers and results["allowed_with_outcome"] >= 30:
        return "ROLLBACK", issues, recommendations
    elif issues:
        return "ADJUST", issues, recommendations
    else:
        return "KEEP LIVE", issues, recommendations


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _print_report(results: dict, verdict: str, issues: List[str], recommendations: List[str], days: int) -> None:
    sep = "=" * 68
    print(sep)
    print("  ML LIVE GATING MONITOR  --  Step 5E-9")
    print(f"  Window: last {days} days")
    print(sep)

    print("\n[VOLUME]")
    print(f"  Total scored entries   : {results['total_scored']}")
    print(f"  ALLOW (allowed)        : {results['n_allowed']}")
    print(f"  BLOCK                  : {results['n_blocked']}")
    print(f"  SHADOW                 : {results['n_shadow']}")
    print(f"  SKIP (no score)        : {results['n_skip']}")
    print(f"  Block rate             : {_fmt_pct(results['block_rate'])}")
    print(f"  Skip rate              : {_fmt_pct(results['skip_rate'])}")

    print("\n[SHADOW MODE]")
    print(f"  Shadow mode still ON?  : {'YES — check .env!' if results['shadow_mode_still_on'] else 'No (gating active)'}")

    print("\n[WIN RATE]")
    if results["allowed_with_outcome"] > 0:
        print(f"  Baseline win rate      : {_fmt_pct(results['baseline_win_rate'])}")
        print(f"  ML-allowed win rate    : {_fmt_pct(results['allowed_win_rate'])}  (n={results['allowed_with_outcome']})")
        if not math.isnan(results["win_rate_delta"]):
            delta_sign = "+" if results["win_rate_delta"] >= 0 else ""
            print(f"  Delta vs baseline      : {delta_sign}{_fmt_pct(results['win_rate_delta'])}")
        if results["blocked_with_outcome"] > 0:
            print(f"  Blocked trade win rate : {_fmt_pct(results['blocked_win_rate'])}  (n={results['blocked_with_outcome']}, oracle check)")
    else:
        print("  Win rate analysis      : insufficient outcomes (need closed trades)")

    print("\n[SCORE DISTRIBUTION]")
    if results["scores_count"] > 0:
        for bin_name, cnt in results["score_bins"].items():
            bar = "#" * int(cnt / max(results["scores_count"], 1) * 30)
            pct = _fmt_pct(cnt / results["scores_count"])
            marker = " <-- THRESHOLD" if bin_name == "0.4-0.6" else ""
            print(f"  {bin_name} : {bar:<30} {pct:>6}  (n={cnt}){marker}")
    else:
        print("  No scores in JSONL logs")

    print("\n[MODEL]")
    print(f"  Versions seen          : {results['model_versions']}")
    print(f"  Threshold mismatch     : {'YES' if results['threshold_mismatch'] else 'No'}")

    if results["by_symbol"]:
        print("\n[PER SYMBOL]")
        print(f"  {'Symbol':<12} {'Blocked':>8} {'Allowed':>8} {'Win/Outcome':>14}")
        for sym, d in sorted(results["by_symbol"].items()):
            wr = f"{d['wins']}/{d['outcomes']}" if d["outcomes"] else "n/a"
            print(f"  {sym:<12} {d['blocked']:>8} {d['allowed']:>8} {wr:>14}")

    if results["by_regime"]:
        print("\n[PER REGIME]")
        print(f"  {'Regime':<22} {'Blocked':>8} {'Allowed':>8} {'Win/Outcome':>14}")
        for reg, d in sorted(results["by_regime"].items()):
            wr = f"{d['wins']}/{d['outcomes']}" if d["outcomes"] else "n/a"
            print(f"  {reg:<22} {d['blocked']:>8} {d['allowed']:>8} {wr:>14}")

    print("\n" + sep)
    if issues:
        print(f"[ISSUES ({len(issues)})]")
        for i, iss in enumerate(issues, 1):
            print(f"  {i}. {iss}")
    else:
        print("[ISSUES] None")

    if recommendations:
        print(f"\n[RECOMMENDATIONS ({len(recommendations)})]")
        for i, rec in enumerate(recommendations, 1):
            print(f"  {i}. {rec}")

    print(f"\n[VERDICT]  {verdict}")
    print(sep)

    if verdict == "KEEP LIVE":
        print("  System is operating normally. Continue monitoring.")
    elif verdict == "ADJUST":
        print("  Adjustments recommended. Review issues above before next trading session.")
    elif verdict == "ROLLBACK":
        print("  ROLLBACK REQUIRED. Set ML_SHADOW_MODE=True in .env and restart.")
        print("  Investigate issues before re-activating active gating.")
    elif verdict == "INSUFFICIENT DATA":
        print("  Not enough data yet. Re-run after more trades accumulate.")
    print(sep)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="ML live gating monitor (Step 5E-9)")
    parser.add_argument("--db-path", default=None, help="Path to cosmicforge.db")
    parser.add_argument("--log-dir", default=None, help="Path to ML prediction log directory")
    parser.add_argument("--days", type=int, default=14, help="Monitoring window in days")
    parser.add_argument("--threshold", type=float, default=0.40, help="Expected ML threshold (for audit)")
    parser.add_argument("--baseline-win-rate", type=float, default=0.33, help="Pre-ML baseline win rate")
    parser.add_argument("--min-trades", type=int, default=50, help="Minimum scored trades for verdict")
    parser.add_argument("--json", action="store_true", dest="emit_json", help="Emit JSON summary")
    args = parser.parse_args()

    # Resolve paths
    db_path = Path(args.db_path) if args.db_path else _DEFAULT_DB_LOCAL
    log_dir = Path(args.log_dir) if args.log_dir else _DEFAULT_LOG_DIR_LOCAL

    # Canonical fallback for db path
    if not db_path.exists():
        alt = Path(__file__).parent.parent.parent.parent / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
        if alt.exists():
            db_path = alt

    cutoff_ts = _window_ts(args.days)

    # 1. Load JSONL logs
    jsonl_records = _load_jsonl_logs(log_dir, args.days)

    # 2. Load DB data
    conn = _open_db(db_path)
    db_rows: List[dict] = []
    baseline_win_rate = args.baseline_win_rate
    if conn:
        db_rows = _query_ml_decisions(conn, cutoff_ts)
        db_baseline = _query_baseline_win_rate(conn, cutoff_ts)
        if db_baseline is not None:
            baseline_win_rate = db_baseline
        conn.close()
    else:
        print(f"[WARN] DB not found at {db_path}. Using JSONL logs only.", file=sys.stderr)

    if not jsonl_records and not db_rows:
        print("[ERROR] No ML prediction data found. Is the bot running with ML_ENABLED=True?", file=sys.stderr)
        return 4

    # 3. Analyze
    results = _analyze(db_rows, jsonl_records, baseline_win_rate, args.threshold)
    results["config_threshold"] = args.threshold

    # 4. Verdict
    verdict, issues, recommendations = _verdict(results, args.min_trades)

    # 5. Output
    if args.emit_json:
        out = {
            "verdict": verdict,
            "issues": issues,
            "recommendations": recommendations,
            "results": {
                k: v for k, v in results.items()
                if not isinstance(v, float) or not math.isnan(v)
            },
            "window_days": args.days,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        print(json.dumps(out, indent=2, default=str))
    else:
        _print_report(results, verdict, issues, recommendations, args.days)

    # Exit code
    exit_map = {
        "KEEP LIVE": 0,
        "ADJUST": 1,
        "ROLLBACK": 2,
        "INSUFFICIENT DATA": 3,
    }
    return exit_map.get(verdict, 4)


if __name__ == "__main__":
    sys.exit(main())
