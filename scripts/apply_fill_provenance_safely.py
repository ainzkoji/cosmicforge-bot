"""Backup, verify, apply and re-verify the fill provenance labelling.

The labelling itself is in ``shared_lib/persistence/fill_provenance.py``. This
wraps it in the safety the canonical database deserves:

1. an online backup (SQLite's own backup API, safe with a live writer);
2. a fingerprint of every economic column *before* the change;
3. a dry run, compared against the expected counts;
4. the apply;
5. the same fingerprint *after*, compared byte for byte.

Step 5 is the point. The fingerprint covers id, symbol, side, action, qty,
price, fee, realized_pnl, net_pnl, position_id, order_id and timestamp_utc --
everything that constitutes the trade. If a single one of those changed, the
hash changes and this script says so. Provenance is deliberately excluded from
the fingerprint, because provenance is the only thing allowed to move.

    python scripts/apply_fill_provenance_safely.py            # verify only
    python scripts/apply_fill_provenance_safely.py --apply
"""
from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND = os.path.join(REPO_ROOT, "backends", "bot-backend")
SHARED = os.path.join(REPO_ROOT, "backends", "shared")

#: Everything that constitutes the trade. None of it may change.
ECONOMIC_COLUMNS = (
    "id", "symbol", "side", "action", "qty", "price", "fee", "realized_pnl",
    "net_pnl", "gross_pnl", "total_fees", "position_id", "order_id",
    "timestamp_utc", "run_id", "cycle_id", "trace_id", "bot_instance_id",
    "user_id", "broker_account_id", "strategy", "account_id", "exit_reason",
    "remaining_qty", "r_multiple", "mfe_pct", "mae_pct",
)

#: The prior dry run, for the determinism comparison the brief asks for.
EXPECTED = {"LEGACY_BACKFILL": 16_494, "TEST_FIXTURE": 2, "UNCLASSIFIED": 1_065}


def economic_fingerprint(path: str) -> tuple[str, int]:
    """SHA-256 over every economic column of every fill, in id order."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        available = {r[1] for r in conn.execute("PRAGMA table_info(trade_fills)")}
        columns = [c for c in ECONOMIC_COLUMNS if c in available]
        digest = hashlib.sha256()
        digest.update(("|".join(columns)).encode())
        rows = 0
        for row in conn.execute(
            f"SELECT {', '.join(columns)} FROM trade_fills ORDER BY id"
        ):
            digest.update(repr(row).encode())
            rows += 1
        return digest.hexdigest(), rows
    finally:
        conn.close()


def online_backup(source: str, destination: str) -> int:
    """SQLite's own backup API — consistent even with a live writer."""
    src = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    dst = sqlite3.connect(destination)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return os.path.getsize(destination)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=None)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup-dir", default=None)
    args = parser.parse_args()

    for path in (BACKEND, SHARED):
        if path not in sys.path:
            sys.path.insert(0, path)
    os.chdir(BACKEND)
    env = os.path.join(BACKEND, ".env")
    if os.path.exists(env):
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env, override=True)

    from shared_lib.persistence.db import DB
    from shared_lib.persistence.fill_provenance import (
        classify_existing_fills,
        provenance_breakdown,
    )

    db = DB(args.db) if args.db else DB()
    path = db.path
    print(f"database : {path}")
    print(f"size     : {os.path.getsize(path):,} bytes")

    # ── 1. Fingerprint before ───────────────────────────────────────────────
    before_hash, before_rows = economic_fingerprint(path)
    before_counts = provenance_breakdown(db)
    print(f"\nBEFORE")
    print(f"  rows              {before_rows:,}")
    print(f"  economic hash     {before_hash}")
    print(f"  provenance        {before_counts}")

    # ── 2. Dry run and determinism check ────────────────────────────────────
    dry = classify_existing_fills(db, dry_run=True)
    print(f"\nDRY RUN")
    for key in sorted(dry, key=lambda k: -dry[k]):
        expected = EXPECTED.get(key)
        mark = "  (matches prior run)" if expected == dry[key] else (
            f"  (PRIOR RUN SAID {expected:,})" if expected is not None else "  (new)"
        )
        print(f"  {key:<20} {dry[key]:>8,}{mark}")

    deterministic = dry == EXPECTED
    print(f"\n  deterministic vs prior run: {deterministic}")
    if not deterministic:
        print("  Counts differ from the prior dry run. Not applying: the brief")
        print("  requires the classification to be deterministic and unambiguous.")
        return 1

    if not args.apply:
        print("\nVerify-only. Re-run with --apply to write the labels.")
        return 0

    # ── 3. Backup ───────────────────────────────────────────────────────────
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = args.backup_dir or os.path.join(os.path.dirname(path), "backups")
    os.makedirs(backup_dir, exist_ok=True)
    backup = os.path.join(backup_dir, f"cosmicforge_{stamp}_pre_provenance.db")
    print(f"\nBACKUP\n  -> {backup}")
    size = online_backup(path, backup)
    print(f"  {size:,} bytes")

    backup_hash, backup_rows = economic_fingerprint(backup)
    if backup_hash != before_hash:
        print("  BACKUP DOES NOT MATCH THE SOURCE. Not applying.")
        return 1
    print(f"  economic hash matches the source ({backup_rows:,} rows)")

    # ── 4. Apply ────────────────────────────────────────────────────────────
    applied = classify_existing_fills(db, dry_run=False)
    print(f"\nAPPLIED")
    for key in sorted(applied, key=lambda k: -applied[k]):
        print(f"  {key:<20} {applied[key]:>8,}")

    # ── 5. Fingerprint after ────────────────────────────────────────────────
    after_hash, after_rows = economic_fingerprint(path)
    after_counts = provenance_breakdown(db)
    print(f"\nAFTER")
    print(f"  rows              {after_rows:,}")
    print(f"  economic hash     {after_hash}")
    print(f"  provenance        {after_counts}")

    print("\nVERIFICATION")
    print(f"  row count unchanged      {before_rows == after_rows}")
    print(f"  economic hash unchanged  {before_hash == after_hash}")
    if before_hash != after_hash or before_rows != after_rows:
        print("\n  TRADE ECONOMICS CHANGED. Restore from the backup above.")
        return 1
    print("  Only provenance moved. Prices, quantities, PnL, ids and timestamps")
    print("  are byte-for-byte identical.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
