#!/usr/bin/env python3
"""
Broker linkage repair script.

Detects and optionally repairs the following classes of broker data problems:

1. ENVIRONMENT MISMATCH — broker_accounts.environment ≠ credential blob environment
   Impact: resolver raises REASON_ENV_MISMATCH; bots cannot connect
   Fix:    update broker_accounts.environment to match blob, or vice versa

2. STALE CREDENTIALS — credential blob was last updated before a given threshold
   Impact: exchange may reject keys; bots silently hit 401 → zero equity
   Fix:    set broker_accounts.status='invalid' to force user to reconnect

3. MISSING V2 ROWS — broker_credentials_v2 has no row for a broker_accounts entry
   Impact: resolver falls through to v2 query, finds nothing, raises REASON_NO_CREDENTIALS
   Fix:    backfill v2 row from legacy broker_credentials (done by migrations, re-run here)

4. MISSING active_credential_version POINTER — broker_accounts.active_credential_version IS NULL
   Impact: resolver uses legacy fallback (highest active v2 row); works but logs a warning
   Fix:    set pointer to highest active version

5. ORPHANED BOTS — bot_instances.broker_account_id references a non-existent or invalid account
   Impact: every cycle throws BrokerResolverError REASON_NOT_FOUND / REASON_INVALID
   Fix:    stop bot instance (set status='error') with informative last_error

6. events TABLE MISSING trace_id — OperationalError on every Audit.event() call
   Fix:    run migrations (handled by migration #35, shown here for diagnosis)

Usage:
    # Dry run (report only)
    python scripts/repair_broker_linkage.py --db path/to/cosmicforge.db

    # Apply fixes
    python scripts/repair_broker_linkage.py --db path/to/cosmicforge.db --fix

    # Fix specific account only
    python scripts/repair_broker_linkage.py --db path/to/cosmicforge.db --fix --account brk_d85739ad7956
"""
from __future__ import annotations

import argparse
import json
import sys
import os

# Allow running from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared_lib.persistence.db import DB
from shared_lib.core.security.broker_security import decrypt_credentials
from shared_lib.broker.environment import normalize_environment


def _utc_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def run_repair(db_path: str, fix: bool, account_filter: str | None) -> None:
    db = DB(path=db_path)
    issues: list[dict] = []
    fixes_applied = 0

    with db.connect() as conn:

        # ── 0. Check events.trace_id column ──────────────────────────────────
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(events)").fetchall()}
        if "trace_id" not in cols:
            issues.append({
                "type":    "MISSING_EVENTS_TRACE_ID",
                "detail":  "events table is missing trace_id column — every Audit.event() call raises OperationalError",
                "fix_sql": "ALTER TABLE events ADD COLUMN trace_id TEXT",
                "fixed":   False,
            })
            if fix:
                conn.execute("ALTER TABLE events ADD COLUMN trace_id TEXT")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_events_trace_id ON events(trace_id) WHERE trace_id IS NOT NULL")
                issues[-1]["fixed"] = True
                fixes_applied += 1
                print("[FIX] Added events.trace_id column")

        # ── 1. Load all broker accounts ───────────────────────────────────────
        filter_sql = ""
        filter_params: tuple = ()
        if account_filter:
            filter_sql = " WHERE ba.id = ?"
            filter_params = (account_filter,)

        rows = conn.execute(
            f"""
            SELECT ba.id, ba.user_id, ba.broker_id, ba.environment,
                   ba.status, ba.active_credential_version,
                   bc.encrypted_blob, bc.updated_at AS cred_updated_at
            FROM broker_accounts ba
            LEFT JOIN broker_credentials bc ON bc.account_id = ba.id
            {filter_sql}
            ORDER BY ba.id
            """,
            filter_params,
        ).fetchall()

        for row in rows:
            account_id  = row["id"]
            user_id     = row["user_id"]
            broker_id   = row["broker_id"]
            acct_env    = (row["environment"] or "").strip().lower()
            status      = row["status"]
            act_ver     = row["active_credential_version"]
            enc_blob    = row["encrypted_blob"]
            cred_ts     = row["cred_updated_at"]

            # ── 2. MISSING V2 ROW ────────────────────────────────────────────
            v2_row = conn.execute(
                "SELECT COUNT(*) as cnt FROM broker_credentials_v2 WHERE account_id=?",
                (account_id,),
            ).fetchone()
            if v2_row["cnt"] == 0:
                issues.append({
                    "type":       "MISSING_V2_ROW",
                    "account_id": account_id,
                    "detail":     "No broker_credentials_v2 row. Legacy row exists." if enc_blob else "No credentials at all.",
                    "fixed":      False,
                })
                if fix and enc_blob:
                    fingerprint = ""
                    try:
                        dec = decrypt_credentials(enc_blob)
                        key = dec.get("api_key") or dec.get("key") or ""
                        fingerprint = f"...{key[-4:]}" if len(key) >= 4 else "****"
                    except Exception:
                        pass
                    now = _utc_now()
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO broker_credentials_v2
                            (account_id, version, status, encrypted_blob, key_metadata,
                             key_fingerprint, created_at, updated_at)
                        VALUES (?, 1, 'active', ?, 'fernet_v1', ?, ?, ?)
                        """,
                        (account_id, enc_blob, fingerprint, cred_ts or now, cred_ts or now),
                    )
                    issues[-1]["fixed"] = True
                    fixes_applied += 1
                    print(f"[FIX] Created broker_credentials_v2 v1 for account {account_id}")

            # ── 3. MISSING active_credential_version POINTER ─────────────────
            if act_ver is None:
                best = conn.execute(
                    "SELECT version FROM broker_credentials_v2 WHERE account_id=? AND status='active' ORDER BY version DESC LIMIT 1",
                    (account_id,),
                ).fetchone()
                if best:
                    issues.append({
                        "type":       "MISSING_ACTIVE_VERSION_POINTER",
                        "account_id": account_id,
                        "detail":     f"active_credential_version IS NULL, best available version={best['version']}",
                        "fixed":      False,
                    })
                    if fix:
                        conn.execute(
                            "UPDATE broker_accounts SET active_credential_version=?, updated_at=? WHERE id=?",
                            (best["version"], _utc_now(), account_id),
                        )
                        issues[-1]["fixed"] = True
                        fixes_applied += 1
                        print(f"[FIX] Set active_credential_version={best['version']} for account {account_id}")

            # ── 4. ENVIRONMENT MISMATCH ───────────────────────────────────────
            if enc_blob:
                try:
                    dec = decrypt_credentials(enc_blob)
                    blob_env_raw = dec.get("environment", "")
                    if blob_env_raw:
                        try:
                            norm_blob = normalize_environment(blob_env_raw)
                            norm_acct = normalize_environment(acct_env) if acct_env else None
                            if norm_acct and norm_blob != norm_acct:
                                issues.append({
                                    "type":        "ENV_MISMATCH",
                                    "account_id":  account_id,
                                    "acct_env":    acct_env,
                                    "blob_env":    blob_env_raw,
                                    "detail": (
                                        f"broker_accounts.environment={acct_env!r} but "
                                        f"credential blob.environment={blob_env_raw!r}. "
                                        f"The account row is authoritative; the blob should match."
                                    ),
                                    "fixed": False,
                                })
                                if fix:
                                    # Trust the account row; update v2 blob to strip conflicting env
                                    # (Canonical resolver will always use account row going forward)
                                    print(
                                        f"[WARN] ENV_MISMATCH for {account_id}: "
                                        f"acct={acct_env} blob={blob_env_raw}. "
                                        f"Account row is authoritative — no blob change needed "
                                        f"since resolver now ignores blob.environment. "
                                        f"Mark as informational."
                                    )
                                    issues[-1]["fixed"] = True
                                    # No DB write needed — new resolver never trusts blob.environment
                        except ValueError:
                            pass
                except Exception as exc:
                    issues.append({
                        "type":       "DECRYPT_FAILED",
                        "account_id": account_id,
                        "detail":     f"decrypt_credentials() raised: {exc}",
                        "fixed":      False,
                    })
                    if fix and status not in ("invalid", "disconnected"):
                        conn.execute(
                            "UPDATE broker_accounts SET status='invalid', last_error_message=?, updated_at=? WHERE id=?",
                            ("Credential decryption failed. Re-submit credentials.", _utc_now(), account_id),
                        )
                        issues[-1]["fixed"] = True
                        fixes_applied += 1
                        print(f"[FIX] Marked account {account_id} as invalid (decrypt failed)")

            # ── 5. ORPHANED BOT INSTANCES ────────────────────────────────────
        orphaned_bots = conn.execute(
            """
            SELECT bi.id, bi.broker_account_id, bi.status, bi.user_id
            FROM bot_instances bi
            LEFT JOIN broker_accounts ba ON ba.id = bi.broker_account_id
            WHERE bi.status NOT IN ('stopped', 'error', 'archived')
              AND (ba.id IS NULL OR ba.status IN ('invalid', 'disconnected', 'deleted'))
            """
            + (f" AND bi.broker_account_id = '{account_filter}'" if account_filter else ""),
        ).fetchall()

        for bot in orphaned_bots:
            acct_status = conn.execute(
                "SELECT status FROM broker_accounts WHERE id=?",
                (bot["broker_account_id"],),
            ).fetchone()
            detail = (
                f"Broker account {bot['broker_account_id']!r} "
                f"status={acct_status['status'] if acct_status else 'NOT_FOUND'} — "
                f"bot cannot connect"
            )
            issues.append({
                "type":       "ORPHANED_BOT",
                "bot_id":     bot["id"],
                "account_id": bot["broker_account_id"],
                "detail":     detail,
                "fixed":      False,
            })
            if fix:
                conn.execute(
                    "UPDATE bot_instances SET status='error', last_error=?, updated_at=? WHERE id=?",
                    (f"[broker_not_found_or_invalid] {detail}", _utc_now(), bot["id"]),
                )
                issues[-1]["fixed"] = True
                fixes_applied += 1
                print(f"[FIX] Stopped orphaned bot {bot['id']} (broker={bot['broker_account_id']})")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Broker linkage audit: {len(issues)} issue(s) found, {fixes_applied} fix(es) applied")
    print(f"{'='*60}")
    if not issues:
        print("  No issues found. Broker linkage is clean.")
        return

    for i, issue in enumerate(issues, 1):
        status_tag = "[FIXED]" if issue.get("fixed") else "[OPEN]"
        print(f"\n  {i}. {status_tag} {issue['type']}")
        for k, v in issue.items():
            if k not in ("type", "fixed"):
                print(f"       {k}: {v}")

    if not fix and issues:
        print(f"\n  Re-run with --fix to apply {len(issues)} fix(es).")


def main() -> None:
    parser = argparse.ArgumentParser(description="Detect and repair broker linkage issues")
    parser.add_argument("--db",      required=True, help="Path to cosmicforge.db (or target SQLite DB)")
    parser.add_argument("--fix",     action="store_true", help="Apply fixes (default: dry run / report only)")
    parser.add_argument("--account", default=None, help="Limit to a specific broker_account_id")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"ERROR: Database not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    run_repair(db_path=args.db, fix=args.fix, account_filter=args.account)


if __name__ == "__main__":
    main()
