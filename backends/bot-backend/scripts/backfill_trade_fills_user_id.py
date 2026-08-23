from __future__ import annotations

import argparse
import os
import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class BackfillResult:
    would_update: int
    updated: int
    skipped_no_bot_instance: int
    skipped_no_bot_instance_user: int
    already_set: int


def _default_db_path() -> str:
    """
    Default to the shared durable DB location used by the bot backend when
    DATABASE_URL=sqlite:///../shared/shared_lib/persistence/cosmicforge.db
    """
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "shared",
            "shared_lib",
            "persistence",
            "cosmicforge.db",
        )
    )


def _resolve_db_path(db_path: str | None) -> str:
    if db_path:
        return os.path.abspath(db_path)
    env_url = os.environ.get("DATABASE_URL") or ""
    if env_url.startswith("sqlite:///"):
        return os.path.abspath(env_url.replace("sqlite:///", ""))
    return _default_db_path()


def backfill_trade_fills_user_id(db_path: str, *, dry_run: bool = True) -> BackfillResult:
    """
    Backfill trade_fills.user_id from bot_instances.user_id when deterministically
    linkable via trade_fills.bot_instance_id.

    Safety:
    - Only updates rows where user_id is NULL/empty.
    - Only updates when bot_instance_id maps to an existing bot_instances row
      with a non-empty user_id.
    - Does not touch realized_pnl, fees, or any trade outcome fields.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        already_set = conn.execute(
            "SELECT COUNT(*) AS c FROM trade_fills WHERE user_id IS NOT NULL AND user_id != ''"
        ).fetchone()["c"]

        # Rows eligible for backfill attempt (NULL/empty user_id but has bot_instance_id)
        eligible = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM trade_fills
            WHERE (user_id IS NULL OR user_id = '')
              AND bot_instance_id IS NOT NULL
              AND bot_instance_id != ''
            """
        ).fetchone()["c"]

        if eligible == 0:
            return BackfillResult(
                would_update=0,
                updated=0,
                skipped_no_bot_instance=0,
                skipped_no_bot_instance_user=0,
                already_set=int(already_set or 0),
            )

        # Determine skipped categories before update (reporting only)
        skipped_no_bot_instance = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM trade_fills f
            LEFT JOIN bot_instances bi ON bi.id = f.bot_instance_id
            WHERE (f.user_id IS NULL OR f.user_id = '')
              AND f.bot_instance_id IS NOT NULL
              AND f.bot_instance_id != ''
              AND bi.id IS NULL
            """
        ).fetchone()["c"]

        skipped_no_bot_instance_user = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM trade_fills f
            JOIN bot_instances bi ON bi.id = f.bot_instance_id
            WHERE (f.user_id IS NULL OR f.user_id = '')
              AND f.bot_instance_id IS NOT NULL
              AND f.bot_instance_id != ''
              AND (bi.user_id IS NULL OR bi.user_id = '')
            """
        ).fetchone()["c"]

        would_update = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM trade_fills f
            JOIN bot_instances bi ON bi.id = f.bot_instance_id
            WHERE (f.user_id IS NULL OR f.user_id = '')
              AND f.bot_instance_id IS NOT NULL
              AND f.bot_instance_id != ''
              AND bi.user_id IS NOT NULL
              AND bi.user_id != ''
            """
        ).fetchone()["c"]

        updated = 0
        if not dry_run:
            with conn:
                cur = conn.execute(
                    """
                    UPDATE trade_fills
                    SET user_id = (
                        SELECT bi.user_id
                        FROM bot_instances bi
                        WHERE bi.id = trade_fills.bot_instance_id
                        LIMIT 1
                    )
                    WHERE (user_id IS NULL OR user_id = '')
                      AND bot_instance_id IS NOT NULL
                      AND bot_instance_id != ''
                      AND EXISTS (
                        SELECT 1
                        FROM bot_instances bi2
                        WHERE bi2.id = trade_fills.bot_instance_id
                          AND bi2.user_id IS NOT NULL
                          AND bi2.user_id != ''
                      )
                    """
                )
                updated = cur.rowcount or 0

        return BackfillResult(
            would_update=int(would_update or 0),
            updated=int(updated),
            skipped_no_bot_instance=int(skipped_no_bot_instance or 0),
            skipped_no_bot_instance_user=int(skipped_no_bot_instance_user or 0),
            already_set=int(already_set or 0),
        )
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill trade_fills.user_id from bot_instances.user_id")
    parser.add_argument("--db", dest="db_path", default=None, help="Path to sqlite DB (defaults to DATABASE_URL or shared cosmicforge.db)")
    parser.add_argument("--apply", action="store_true", help="Apply updates (default is dry-run)")
    args = parser.parse_args()

    db_path = _resolve_db_path(args.db_path)
    res = backfill_trade_fills_user_id(db_path, dry_run=not args.apply)

    mode = "APPLY" if args.apply else "DRY_RUN"
    print(f"[{mode}] db={db_path}")
    print(f"[{mode}] already_set_user_id={res.already_set}")
    print(f"[{mode}] would_update={res.would_update}")
    print(f"[{mode}] updated={res.updated}")
    print(f"[{mode}] skipped_no_bot_instance={res.skipped_no_bot_instance}")
    print(f"[{mode}] skipped_no_bot_instance_user={res.skipped_no_bot_instance_user}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
