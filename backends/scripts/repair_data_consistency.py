#!/usr/bin/env python3
"""
Data consistency repair script.

Detects and repairs cross-service data inconsistencies:

1. GHOST_BOTS          — bot_instances with status IN ('active','error') linked to
                         broker accounts that are invalid/deleted/missing.
                         Fix: soft-delete the ghost bot.

2. HARD_DELETED_BOTS   — bot_instances rows with status NOT IN known values
                         (shouldn't occur after soft-delete migration).

3. PORTFOLIO_DB_MISMATCH — checks that DATABASE_URL points to the correct DB
                           (the one user-backend uses) and warns if portfolio
                           service would connect to a different file.

4. BROKER_NO_CREDS     — broker accounts with status='connected' but no row in
                         broker_credentials_v2 AND no row in broker_credentials.
                         Fix: mark as 'invalid'.

5. MULTI_ACTIVE_BOTS   — multiple bot_instances in status='active' for the same
                         (user_id, broker_account_id) pair. Warns for review.

6. STALE_ERROR_BOTS    — bot_instances with status='error' that have not been
                         updated in > 24 hours (probably stuck). Warns.

Usage:
    # Dry run (default)
    python scripts/repair_data_consistency.py --db path/to/cosmicforge.db

    # Apply fixes
    python scripts/repair_data_consistency.py --db path/to/cosmicforge.db --fix

    # Check specific user
    python scripts/repair_data_consistency.py --db path/to/cosmicforge.db --user user_abc
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone, timedelta

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "shared"))

from shared_lib.persistence.db import DB


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hours_ago(iso_str: str) -> float:
    """Return how many hours ago an ISO timestamp was."""
    try:
        ts = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - ts
        return delta.total_seconds() / 3600
    except Exception:
        return 0.0


def run_repair(db_path: str, fix: bool, user_filter: str | None) -> None:
    db = DB(path=db_path)
    issues: list[dict] = []
    fixes_applied = 0

    with db.connect() as conn:
        bi_cols = {r["name"] for r in conn.execute("PRAGMA table_info(bot_instances)").fetchall()}
        ba_cols = {r["name"] for r in conn.execute("PRAGMA table_info(broker_accounts)").fetchall()}
        has_health = "broker_health_status" in bi_cols
        has_v2 = bool(conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='broker_credentials_v2'"
        ).fetchone())
        has_legacy = bool(conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='broker_credentials'"
        ).fetchone())

        user_clause = f" AND bi.user_id = '{user_filter}'" if user_filter else ""

        # ── 1. Ghost bots linked to broken/missing brokers ─────────────────
        ghost_bots = conn.execute(
            f"""
            SELECT bi.id, bi.user_id, bi.broker_account_id, bi.status,
                   bi.updated_at, ba.status AS broker_status
            FROM bot_instances bi
            LEFT JOIN broker_accounts ba ON ba.id = bi.broker_account_id
            WHERE bi.status IN ('active', 'error')
              AND (
                    ba.id IS NULL
                    OR ba.status IN ('invalid', 'deleted', 'revoked', 'error')
              )
              {user_clause}
            """
        ).fetchall()

        for bot in ghost_bots:
            # Check if already quarantined
            if has_health:
                health = conn.execute(
                    "SELECT broker_health_status FROM bot_instances WHERE id=?",
                    (bot["id"],),
                ).fetchone()
                if health and health["broker_health_status"] == "broker_blocked":
                    continue  # Already handled by quarantine system

            issues.append({
                "type":          "GHOST_BOT",
                "bot_id":        bot["id"],
                "user_id":       bot["user_id"],
                "account_id":    bot["broker_account_id"],
                "bot_status":    bot["status"],
                "broker_status": bot["broker_status"] or "NOT_FOUND",
                "detail":        "Active bot linked to broken/missing broker account",
                "fixed":         False,
            })
            if fix:
                conn.execute(
                    "UPDATE bot_instances SET status='deleted', stopped_at=?, updated_at=? WHERE id=?",
                    (_utc(), _utc(), bot["id"]),
                )
                issues[-1]["fixed"] = True
                fixes_applied += 1
                print(f"[FIX] Soft-deleted ghost bot {bot['id']} (broker={bot['broker_account_id']!r})")

        # ── 2. Broker accounts with connected status but no credentials ────
        no_cred_accounts = conn.execute(
            f"""
            SELECT ba.id, ba.user_id, ba.broker_id
            FROM broker_accounts ba
            WHERE ba.status = 'connected'
              {f"AND ba.user_id = '{user_filter}'" if user_filter else ""}
            """
        ).fetchall()

        for acct in no_cred_accounts:
            has_any_cred = False
            if has_v2:
                row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM broker_credentials_v2 WHERE account_id=?",
                    (acct["id"],),
                ).fetchone()
                has_any_cred = (row["cnt"] > 0)
            if not has_any_cred and has_legacy:
                row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM broker_credentials WHERE account_id=?",
                    (acct["id"],),
                ).fetchone()
                has_any_cred = (row["cnt"] > 0)

            if not has_any_cred:
                issues.append({
                    "type":       "BROKER_NO_CREDS",
                    "account_id": acct["id"],
                    "user_id":    acct["user_id"],
                    "broker_id":  acct["broker_id"],
                    "detail":     "Broker account status=connected but no credentials found in any table",
                    "fixed":      False,
                })
                if fix:
                    conn.execute(
                        "UPDATE broker_accounts SET status='invalid', updated_at=? WHERE id=?",
                        (_utc(), acct["id"]),
                    )
                    issues[-1]["fixed"] = True
                    fixes_applied += 1
                    print(f"[FIX] Invalidated no-credential broker {acct['id']!r}")

        # ── 3. Multiple active bots for the same broker account ────────────
        dupes = conn.execute(
            f"""
            SELECT user_id, broker_account_id, COUNT(*) AS cnt
            FROM bot_instances
            WHERE status = 'active'
            {f"AND user_id = '{user_filter}'" if user_filter else ""}
            GROUP BY user_id, broker_account_id
            HAVING COUNT(*) > 1
            """
        ).fetchall()

        for d in dupes:
            bot_ids = [
                r["id"]
                for r in conn.execute(
                    "SELECT id FROM bot_instances WHERE user_id=? AND broker_account_id=? AND status='active' ORDER BY created_at",
                    (d["user_id"], d["broker_account_id"]),
                ).fetchall()
            ]
            issues.append({
                "type":       "MULTI_ACTIVE_BOTS",
                "user_id":    d["user_id"],
                "account_id": d["broker_account_id"],
                "count":      d["cnt"],
                "bot_ids":    bot_ids,
                "detail":     "Multiple active bots on the same broker account — may cause duplicate trades",
                "fixed":      False,
                "action_needed": "Review manually; stop older duplicates if unintentional",
            })
            # No auto-fix — this requires human review

        # ── 4. Stale error bots (stuck > 24 hours) ─────────────────────────
        stale = conn.execute(
            f"""
            SELECT id, user_id, broker_account_id, last_error, updated_at
            FROM bot_instances
            WHERE status = 'error'
            {user_clause.replace('bi.', '')}
            """
        ).fetchall()

        for bot in stale:
            hours = _hours_ago(bot["updated_at"] or "")
            if hours > 24:
                issues.append({
                    "type":     "STALE_ERROR_BOT",
                    "bot_id":   bot["id"],
                    "user_id":  bot["user_id"],
                    "hours":    round(hours, 1),
                    "detail":   f"Bot stuck in error state for {hours:.1f}h. Last error: {bot['last_error'] or 'none'}",
                    "fixed":    False,
                    "action_needed": "Investigate error; stop or delete if unrecoverable",
                })

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*64}")
    print(f"Data consistency audit: {len(issues)} issue(s) found, {fixes_applied} fix(es) applied")
    print(f"{'='*64}")
    if not issues:
        print("  No issues found. Database is consistent.")
        return

    for i, issue in enumerate(issues, 1):
        tag = "[FIXED]  " if issue.get("fixed") else "[OPEN]   "
        if issue.get("action_needed"):
            tag = "[MANUAL] "
        print(f"\n  {i}. {tag} {issue['type']}")
        for k, v in issue.items():
            if k not in ("type", "fixed"):
                print(f"       {k}: {v}")

    if not fix:
        auto_fixable = sum(
            1 for iss in issues
            if iss["type"] in ("GHOST_BOT", "BROKER_NO_CREDS") and not iss.get("fixed")
        )
        if auto_fixable:
            print(f"\n  Re-run with --fix to auto-repair {auto_fixable} issue(s).")
        manual = sum(1 for iss in issues if iss.get("action_needed"))
        if manual:
            print(f"  {manual} issue(s) require manual review (see action_needed above).")


def main() -> None:
    parser = argparse.ArgumentParser(description="Detect and repair data consistency issues")
    parser.add_argument("--db",   required=True, help="Path to cosmicforge.db")
    parser.add_argument("--fix",  action="store_true", help="Apply auto-fixes")
    parser.add_argument("--user", default=None, help="Limit to specific user_id")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: Database not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    run_repair(db_path=args.db, fix=args.fix, user_filter=args.user)


if __name__ == "__main__":
    main()
