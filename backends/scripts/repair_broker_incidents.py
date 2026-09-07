#!/usr/bin/env python3
"""
Broker incident repair script — quarantine integration.

Scans the database for active bots that are linked to invalid/broken broker
accounts and either:
  (a) Auto-rebinds the bot to a healthy replacement broker of the same type
      (if one exists for the same user), or
  (b) Quarantines the bot (broker_health_status='broker_blocked') so it stops
      re-entering the execution pool every cycle.

Also invalidates broker_accounts rows that are in a non-connectable state but
still show status='connected' (stale connected status from before the
quarantine system was deployed).

Run this once after deploying the quarantine migration (section 36), and again
any time a batch of broker re-credentials is processed.

Usage:
    # Dry run (report only, no writes)
    python scripts/repair_broker_incidents.py --db path/to/cosmicforge.db

    # Apply fixes (quarantine + rebind + invalidate)
    python scripts/repair_broker_incidents.py --db path/to/cosmicforge.db --fix

    # Fix a single user
    python scripts/repair_broker_incidents.py --db path/to/cosmicforge.db --fix --user user_abc123

    # Fix a single broker account
    python scripts/repair_broker_incidents.py --db path/to/cosmicforge.db --fix --account brk_xyz
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Optional

# Allow running from project root
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "bot-backend"))
sys.path.insert(0, os.path.join(_ROOT, "shared"))

from shared_lib.persistence.db import DB
from shared_lib.broker.errors import BrokerResolverError


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Non-transient broker statuses that block trading ─────────────────────────
_BROKEN_STATUSES = {"invalid", "error", "deleted", "revoked"}

# ── Reason codes that are non-transient (structural breakage) ─────────────────
_NON_TRANSIENT_CODES = {
    BrokerResolverError.REASON_ENV_MISMATCH,
    BrokerResolverError.REASON_DECRYPT_FAILED,
    BrokerResolverError.REASON_REVOKED,
    BrokerResolverError.REASON_NO_CREDENTIALS,
    BrokerResolverError.REASON_INVALID,
    BrokerResolverError.REASON_NOT_FOUND,
}


def run_repair(
    db_path: str,
    fix: bool,
    user_filter: Optional[str],
    account_filter: Optional[str],
) -> None:
    db = DB(path=db_path)
    issues: list[dict] = []
    fixes_applied = 0

    with db.connect() as conn:

        # ── 0. Check migration 36 columns exist ──────────────────────────────
        bi_cols = {r["name"] for r in conn.execute("PRAGMA table_info(bot_instances)").fetchall()}
        ba_cols = {r["name"] for r in conn.execute("PRAGMA table_info(broker_accounts)").fetchall()}
        has_quarantine = "broker_health_status" in bi_cols
        has_validation_error = "validation_error" in ba_cols

        if not has_quarantine:
            print(
                "[WARN] bot_instances.broker_health_status column missing — "
                "run migrations first (section 36). "
                "Quarantine fixes will be skipped.",
                file=sys.stderr,
            )
        if not has_validation_error:
            print(
                "[WARN] broker_accounts.validation_error column missing — "
                "run migrations first (section 34). "
                "Broker invalidation will be skipped.",
                file=sys.stderr,
            )

        # ── 1. Find broker accounts that are broken but still 'connected' ────
        where_clauses = ["ba.status = 'connected'"]
        params: list = []

        # Also include accounts already 'invalid' — they may still have bots needing quarantine
        # We load all non-connected and check for bots
        broken_where = ["ba.status IN ('invalid', 'error', 'deleted', 'revoked', 'connected')"]
        if account_filter:
            broken_where.append("ba.id = ?")
            params.append(account_filter)
        if user_filter:
            broken_where.append("ba.user_id = ?")
            params.append(user_filter)

        # Fetch all broker accounts with their v2 credential status
        all_accounts = conn.execute(
            f"""
            SELECT ba.id, ba.user_id, ba.broker_id, ba.environment,
                   ba.status, ba.validation_error,
                   (SELECT COUNT(*) FROM broker_credentials_v2 bc2
                    WHERE bc2.account_id = ba.id
                      AND (bc2.status = 'active' OR bc2.status IS NULL)) AS active_cred_count,
                   (SELECT COUNT(*) FROM broker_credentials_v2 bc3
                    WHERE bc3.account_id = ba.id) AS total_cred_count
            FROM broker_accounts ba
            WHERE {" AND ".join(broken_where)}
            ORDER BY ba.id
            """,
            params,
        ).fetchall()

        for acct in all_accounts:
            account_id  = acct["id"]
            user_id     = acct["user_id"]
            broker_id   = acct["broker_id"]
            acct_status = acct["status"]
            active_creds = acct["active_cred_count"]
            total_creds  = acct["total_cred_count"]

            # ── 1a. Accounts with no credentials at all ───────────────────────
            if total_creds == 0 and acct_status == "connected":
                issues.append({
                    "type":       "CONNECTED_NO_CREDENTIALS",
                    "account_id": account_id,
                    "user_id":    user_id,
                    "detail":     "broker_accounts.status=connected but no broker_credentials_v2 rows",
                    "fixed":      False,
                })
                if fix and has_validation_error:
                    conn.execute(
                        "UPDATE broker_accounts SET status='invalid', validation_error=?, updated_at=? WHERE id=?",
                        ("No credentials found — re-submit credentials to reconnect.", _utc(), account_id),
                    )
                    issues[-1]["fixed"] = True
                    fixes_applied += 1
                    print(f"[FIX] Invalidated account {account_id} (no credentials)")

            # ── 1b. Connected accounts with zero active creds ─────────────────
            elif active_creds == 0 and acct_status == "connected":
                issues.append({
                    "type":       "CONNECTED_NO_ACTIVE_CRED",
                    "account_id": account_id,
                    "user_id":    user_id,
                    "detail":     f"broker_accounts.status=connected but all {total_creds} credential(s) are superseded/invalid",
                    "fixed":      False,
                })
                if fix and has_validation_error:
                    conn.execute(
                        "UPDATE broker_accounts SET status='invalid', validation_error=?, updated_at=? WHERE id=?",
                        ("All credentials superseded or invalid — re-submit credentials.", _utc(), account_id),
                    )
                    issues[-1]["fixed"] = True
                    fixes_applied += 1
                    print(f"[FIX] Invalidated account {account_id} (no active credentials)")

            # ── 2. Find active bots linked to this account ────────────────────
            bot_where = "bi.broker_account_id = ? AND bi.status NOT IN ('stopped', 'archived')"
            bot_params: list = [account_id]
            if has_quarantine:
                # Include bots not yet quarantined
                bot_where += " AND (bi.broker_health_status IS NULL OR bi.broker_health_status != 'broker_blocked')"

            active_bots = conn.execute(
                f"SELECT id, user_id, status FROM bot_instances bi WHERE {bot_where}",
                bot_params,
            ).fetchall()

            for bot in active_bots:
                bot_id = bot["id"]

                # Is the broker broken?
                if acct_status not in _BROKEN_STATUSES and active_creds > 0:
                    continue  # Broker looks OK, skip

                # Broker is broken — this bot needs action
                reason_code = (
                    BrokerResolverError.REASON_NO_CREDENTIALS if active_creds == 0
                    else BrokerResolverError.REASON_INVALID
                )
                error_msg = f"Broker account {account_id!r} status={acct_status!r}"

                # Try to find a replacement broker of same type for same user
                replacement = None
                if fix:
                    replacement = conn.execute(
                        """
                        SELECT id FROM broker_accounts
                        WHERE user_id   = ?
                          AND broker_id = ?
                          AND status    = 'connected'
                          AND id        != ?
                        ORDER BY updated_at DESC
                        LIMIT 1
                        """,
                        (user_id, broker_id, account_id),
                    ).fetchone()

                if replacement:
                    # Auto-rebind to replacement
                    new_account_id = replacement["id"]
                    issues.append({
                        "type":           "BOT_BROKER_BROKEN_REBIND",
                        "bot_id":         bot_id,
                        "account_id":     account_id,
                        "new_account_id": new_account_id,
                        "detail":         f"Bot rebound from broken {account_id!r} → {new_account_id!r}",
                        "fixed":          False,
                    })
                    if fix and has_quarantine:
                        conn.execute(
                            """
                            UPDATE bot_instances
                            SET broker_account_id    = ?,
                                broker_health_status = 'ok',
                                broker_error_code    = NULL,
                                broker_blocked_at    = NULL,
                                last_error           = NULL,
                                updated_at           = ?
                            WHERE id = ?
                            """,
                            (new_account_id, _utc(), bot_id),
                        )
                        issues[-1]["fixed"] = True
                        fixes_applied += 1
                        print(
                            f"[FIX] Rebound bot {bot_id}: {account_id!r} → {new_account_id!r}"
                        )
                else:
                    # No replacement — quarantine the bot
                    issues.append({
                        "type":       "BOT_BROKER_BROKEN_QUARANTINE",
                        "bot_id":     bot_id,
                        "account_id": account_id,
                        "reason":     reason_code,
                        "detail":     f"No replacement broker found. Bot quarantined until broker reconnected.",
                        "fixed":      False,
                    })
                    if fix and has_quarantine:
                        conn.execute(
                            """
                            UPDATE bot_instances
                            SET broker_health_status = 'broker_blocked',
                                broker_error_code    = ?,
                                broker_blocked_at    = ?,
                                last_error           = ?,
                                updated_at           = ?
                            WHERE id = ?
                            """,
                            (reason_code, _utc(), f"[{reason_code}] {error_msg}", _utc(), bot_id),
                        )
                        issues[-1]["fixed"] = True
                        fixes_applied += 1
                        print(
                            f"[FIX] Quarantined bot {bot_id} "
                            f"(broker={account_id!r} reason={reason_code})"
                        )

        # ── 3. Bots already quarantined whose broker has since been reconnected
        #       — clear their quarantine automatically ──────────────────────────
        if has_quarantine:
            reconnected_bots = conn.execute(
                """
                SELECT bi.id, bi.broker_account_id, bi.broker_error_code
                FROM bot_instances bi
                JOIN broker_accounts ba ON ba.id = bi.broker_account_id
                WHERE bi.broker_health_status = 'broker_blocked'
                  AND ba.status = 'connected'
                  AND (
                        SELECT COUNT(*) FROM broker_credentials_v2 bc
                        WHERE bc.account_id = ba.id
                          AND (bc.status = 'active' OR bc.status IS NULL)
                  ) > 0
                """
                + (f" AND bi.broker_account_id = '{account_filter}'" if account_filter else "")
                + (f" AND bi.user_id = '{user_filter}'" if user_filter else ""),
            ).fetchall()

            for bot in reconnected_bots:
                issues.append({
                    "type":   "STALE_QUARANTINE",
                    "bot_id": bot["id"],
                    "detail": f"Bot is quarantined but its broker {bot['broker_account_id']!r} is now connected — safe to unquarantine",
                    "fixed":  False,
                })
                if fix:
                    conn.execute(
                        """
                        UPDATE bot_instances
                        SET broker_health_status = 'ok',
                            broker_error_code    = NULL,
                            broker_blocked_at    = NULL,
                            last_error           = NULL,
                            updated_at           = ?
                        WHERE id = ?
                        """,
                        (_utc(), bot["id"]),
                    )
                    issues[-1]["fixed"] = True
                    fixes_applied += 1
                    print(f"[FIX] Cleared stale quarantine for bot {bot['id']}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*64}")
    print(f"Broker incident repair: {len(issues)} issue(s) found, {fixes_applied} fix(es) applied")
    print(f"{'='*64}")
    if not issues:
        print("  No issues found. All active bots are linked to healthy brokers.")
        return

    for i, issue in enumerate(issues, 1):
        tag = "[FIXED]" if issue.get("fixed") else "[OPEN] "
        print(f"\n  {i}. {tag} {issue['type']}")
        for k, v in issue.items():
            if k not in ("type", "fixed"):
                print(f"       {k}: {v}")

    if not fix and issues:
        print(f"\n  Re-run with --fix to apply fixes.")
        print(
            "  NOTE: Run migrations first if broker_health_status column is missing "
            "(see migrations section 36)."
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Repair broker incident quarantine state for bots"
    )
    parser.add_argument("--db",      required=True, help="Path to cosmicforge.db")
    parser.add_argument("--fix",     action="store_true", help="Apply fixes (default: dry run)")
    parser.add_argument("--user",    default=None, help="Limit to a specific user_id")
    parser.add_argument("--account", default=None, help="Limit to a specific broker_account_id")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: Database not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    run_repair(
        db_path=args.db,
        fix=args.fix,
        user_filter=args.user,
        account_filter=args.account,
    )


if __name__ == "__main__":
    main()
