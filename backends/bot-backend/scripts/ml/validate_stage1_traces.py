"""
Stage 1 Trace Validation
========================
Confirms that the Stage 1A/1B patches are producing real data in decision_traces.

Run from backends/bot-backend/:
    python scripts/ml/validate_stage1_traces.py

Exit codes:
    0 — All checks pass (traces are healthy)
    1 — One or more checks failed (traces still dark)
    2 — Insufficient data (< 20 recent traces; run again after bot has cycled)
"""
from __future__ import annotations

import os
import sys
import json

# Allow running from repo root or from backends/bot-backend/
_here = os.path.dirname(__file__)
_bot_root = os.path.normpath(os.path.join(_here, "..", ".."))
_shared_root = os.path.normpath(os.path.join(_bot_root, "..", "shared"))
for _p in (_bot_root, os.path.join(_shared_root, "shared_lib", "..", "..")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from shared_lib.persistence.db import DB
except ImportError:
    sys.exit("ERROR: Cannot import shared_lib. Run from backends/bot-backend/ with venv active.")


DB_PATH = os.environ.get("DB_PATH", os.path.join(_bot_root, "..", "..", "data", "bot.db"))
LOOKBACK_HOURS = 2      # Check traces from the last N hours
MIN_TRACES = 20         # Minimum rows required for meaningful checks
PASS_RATE = 0.85        # 85% of rows must pass each check (allows for cold-start rows)


def pct(numerator: int, denominator: int) -> float:
    return round(numerator / denominator * 100, 1) if denominator else 0.0


def run_checks(db: DB) -> int:
    """Returns exit code: 0=pass, 1=fail, 2=insufficient_data."""
    failures = []
    warnings = []

    with db.connect() as conn:

        # ── 0. Count recent traces ────────────────────────────────────────────
        total = conn.execute(
            """
            SELECT COUNT(*) AS n FROM decision_traces
            WHERE ts >= datetime('now', ? || ' hours')
            """,
            (f"-{LOOKBACK_HOURS}",)
        ).fetchone()["n"]

        print(f"\n{'='*60}")
        print(f"STAGE 1 TRACE VALIDATION  (last {LOOKBACK_HOURS}h, {total} traces)")
        print(f"{'='*60}\n")

        if total < MIN_TRACES:
            print(f"⚠  INSUFFICIENT DATA: only {total} traces in last {LOOKBACK_HOURS}h "
                  f"(need {MIN_TRACES}). Run again after the bot has cycled more symbols.")
            return 2

        # ── 1. regime_state is not UNKNOWN ────────────────────────────────────
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS n,
                SUM(CASE WHEN regime_state != 'UNKNOWN' AND regime_state IS NOT NULL THEN 1 ELSE 0 END) AS good
            FROM decision_traces
            WHERE ts >= datetime('now', ? || ' hours')
            """,
            (f"-{LOOKBACK_HOURS}",)
        ).fetchone()
        n, good = row["n"], row["good"]
        rate = pct(good, n)
        status = "✅" if rate >= PASS_RATE * 100 else "❌"
        print(f"{status} CHECK 1  regime_state populated: {good}/{n} ({rate}%)")
        if rate < PASS_RATE * 100:
            failures.append(f"regime_state still UNKNOWN for {n-good}/{n} traces — Stage 1A not active")

        # ── 2. adx is not NULL ────────────────────────────────────────────────
        row = conn.execute(
            """
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN adx IS NOT NULL THEN 1 ELSE 0 END) AS good
            FROM decision_traces
            WHERE ts >= datetime('now', ? || ' hours')
            """,
            (f"-{LOOKBACK_HOURS}",)
        ).fetchone()
        n, good = row["n"], row["good"]
        rate = pct(good, n)
        status = "✅" if rate >= PASS_RATE * 100 else "❌"
        print(f"{status} CHECK 2  adx populated:          {good}/{n} ({rate}%)")
        if rate < PASS_RATE * 100:
            failures.append(f"adx NULL for {n-good}/{n} traces — record_strategies() not firing")

        # ── 3. buy_score is not NULL ──────────────────────────────────────────
        row = conn.execute(
            """
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN buy_score IS NOT NULL THEN 1 ELSE 0 END) AS good
            FROM decision_traces
            WHERE ts >= datetime('now', ? || ' hours')
            """,
            (f"-{LOOKBACK_HOURS}",)
        ).fetchone()
        n, good = row["n"], row["good"]
        rate = pct(good, n)
        status = "✅" if rate >= PASS_RATE * 100 else "❌"
        print(f"{status} CHECK 3  buy_score populated:    {good}/{n} ({rate}%)")
        if rate < PASS_RATE * 100:
            failures.append(f"buy_score NULL for {n-good}/{n} traces — record_strategies() not firing")

        # ── 4. confidence > 0 for non-HOLD traces ────────────────────────────
        row = conn.execute(
            """
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN confidence > 0 THEN 1 ELSE 0 END) AS good
            FROM decision_traces
            WHERE ts >= datetime('now', ? || ' hours')
              AND signal != 'HOLD'
              AND signal IS NOT NULL
            """,
            (f"-{LOOKBACK_HOURS}",)
        ).fetchone()
        n, good = row["n"], row["good"]
        if n > 0:
            rate = pct(good, n)
            status = "✅" if rate >= PASS_RATE * 100 else "❌"
            print(f"{status} CHECK 4  confidence>0 on non-HOLD: {good}/{n} ({rate}%)")
            if rate < PASS_RATE * 100:
                failures.append(f"confidence=0 for {n-good}/{n} non-HOLD traces — record_strategies() not updating confidence")
        else:
            print(f"⚠  CHECK 4  (skipped — no non-HOLD signals in window yet)")
            warnings.append("No non-HOLD signals in last 2h — strategy may be all-HOLD")

        # ── 5. fill_price > 0 for executed traces ────────────────────────────
        row = conn.execute(
            """
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN fill_price > 0 THEN 1 ELSE 0 END) AS good
            FROM decision_traces
            WHERE ts >= datetime('now', ? || ' hours')
              AND execution_status IN ('filled', 'ok', 'success', 'FILLED', 'paper_fill')
            """,
            (f"-{LOOKBACK_HOURS}",)
        ).fetchone()
        n, good = row["n"], row["good"]
        if n > 0:
            rate = pct(good, n)
            status = "✅" if rate >= PASS_RATE * 100 else "❌"
            print(f"{status} CHECK 5  fill_price>0 on fills:   {good}/{n} ({rate}%)")
            if rate < PASS_RATE * 100:
                failures.append(f"fill_price=0 for {n-good}/{n} executed traces — Stage 1B not active")
        else:
            print(f"⚠  CHECK 5  (skipped — no executed traces in window)")
            warnings.append("No executed fills in last 2h — bot may not be executing")

        # ── 6. Regime distribution (should not be all one regime) ─────────────
        rows = conn.execute(
            """
            SELECT regime_state, COUNT(*) AS n
            FROM decision_traces
            WHERE ts >= datetime('now', ? || ' hours')
              AND regime_state != 'UNKNOWN'
            GROUP BY regime_state
            ORDER BY n DESC
            """,
            (f"-{LOOKBACK_HOURS}",)
        ).fetchall()
        if rows:
            dist = {r["regime_state"]: r["n"] for r in rows}
            total_regimed = sum(dist.values())
            max_pct = max(dist.values()) / total_regimed * 100 if total_regimed else 0
            regime_str = "  ".join(f"{k}={v}" for k, v in dist.items())
            status = "✅" if max_pct < 95 else "⚠ "
            print(f"{status} CHECK 6  regime distribution:    {regime_str}")
            if max_pct >= 95:
                warnings.append(f"Regime distribution very skewed ({max_pct:.0f}% single class) — regime classifier may be stuck")
        else:
            print("⚠  CHECK 6  (no rows with real regime — all UNKNOWN)")

        # ── 7. ML score distribution ──────────────────────────────────────────
        row = conn.execute(
            """
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN ml_score IS NOT NULL THEN 1 ELSE 0 END) AS scored,
                   SUM(CASE WHEN ml_score = 1.0 THEN 1 ELSE 0 END) AS perfect,
                   ROUND(AVG(ml_score), 4) AS avg_score
            FROM decision_traces
            WHERE ts >= datetime('now', ? || ' hours')
            """,
            (f"-{LOOKBACK_HOURS}",)
        ).fetchone()
        n_total = row["n"]
        scored = row["scored"] or 0
        perfect = row["perfect"] or 0
        avg_score = row["avg_score"]
        if scored > 0:
            perfect_pct = pct(perfect, scored)
            status = "✅" if perfect_pct < 30 else "⚠ "
            print(f"{status} CHECK 7  ML score health:        scored={scored}/{n_total}  avg={avg_score}  exact_1.0={perfect}({perfect_pct}%)")
            if perfect_pct >= 50:
                warnings.append(
                    f"ML scoring still degraded: {perfect_pct}% of scores are exactly 1.0. "
                    "Wait for indicator fields to accumulate before relying on ML scores."
                )
        else:
            print(f"ℹ  CHECK 7  ML scoring disabled (ML_ENABLED=False — expected until data is clean)")

        # ── Summary ───────────────────────────────────────────────────────────
        print(f"\n{'─'*60}")
        if failures:
            print(f"❌  RESULT: {len(failures)} check(s) FAILED\n")
            for f in failures:
                print(f"   • {f}")
            if warnings:
                print()
                for w in warnings:
                    print(f"   ⚠  {w}")
            return 1
        else:
            print(f"✅  RESULT: All checks passed")
            if warnings:
                for w in warnings:
                    print(f"   ⚠  {w}")
            return 0


def main() -> int:
    db = DB(db_path=DB_PATH)
    try:
        return run_checks(db)
    except Exception as e:
        print(f"\nFATAL: {e}", file=sys.stderr)
        raise


if __name__ == "__main__":
    sys.exit(main())
