"""Platform schema operations for deploy and rollback scripts (Section H, Steps 5.1 / 5.6).

    python -m shared_lib.persistence.sql status     [--url URL]
    python -m shared_lib.persistence.sql migrate    [--url URL] [--target N]
    python -m shared_lib.persistence.sql check-compat --app-min N --app-max M [--url URL]
    python -m shared_lib.persistence.sql rollback   --to N [--url URL] --i-have-a-backup

``--url`` defaults to ``PLATFORM_DATABASE_URL`` (then ``DATABASE_URL``). Output is one JSON object; the exit status
is non-zero when the operation was refused, so a deploy script can stop on it. ``rollback`` refuses irreversible
versions and requires an explicit acknowledgement that a backup exists.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from . import Database, compatibility, current_version, migrate, rollback
from .migrations import IrreversibleMigration, MigrationError, applied
from .platform_schema import all_migrations

#: The schema range this build understands. A build needs every migration it ships; older additive schemas are
#: refused (run migrate first), newer additive ones are tolerated (see ``migrations.compatibility``).
APP_SCHEMA_MIN = APP_SCHEMA_MAX = max(m.version for m in all_migrations())


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m shared_lib.persistence.sql")
    ap.add_argument("command", choices=["status", "migrate", "check-compat", "rollback", "build-range"])
    ap.add_argument("--url")
    ap.add_argument("--target", type=int)
    ap.add_argument("--to", type=int)
    ap.add_argument("--app-min", type=int)
    ap.add_argument("--app-max", type=int)
    ap.add_argument("--i-have-a-backup", action="store_true")
    a = ap.parse_args(argv)
    if a.command == "build-range":
        print(json.dumps({"app_schema_min": APP_SCHEMA_MIN, "app_schema_max": APP_SCHEMA_MAX}))
        return 0
    url = a.url or os.environ.get("PLATFORM_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        print(json.dumps({"ok": False, "error": "PLATFORM_DATABASE_URL_MISSING"}))
        return 2
    db = Database(url, pool_size=2)
    try:
        if a.command == "status":
            done = applied(db)
            print(json.dumps({"ok": True, "dialect": db.dialect, "version": current_version(db),
                              "applied": sorted(done), "known": [m.version for m in all_migrations()],
                              "pending": [m.version for m in all_migrations() if m.version not in done],
                              "health": db.health()}))
            return 0
        if a.command == "migrate":
            r = migrate(db, all_migrations(), target=a.target)
            print(json.dumps({"ok": True, "applied": r.applied, "version": r.final_version,
                              "durations_ms": r.durations_ms}))
            return 0
        if a.command == "check-compat":
            if a.app_min is None or a.app_max is None:
                ap.error("check-compat needs --app-min and --app-max")
            c = compatibility(db, app_min_schema=a.app_min, app_max_schema=a.app_max)
            print(json.dumps({"ok": c.compatible, "database_version": c.database_version, "reason": c.reason}))
            return 0 if c.compatible else 3
        if a.command == "rollback":
            if a.to is None or not a.i_have_a_backup:
                print(json.dumps({"ok": False, "error": "ROLLBACK_NEEDS_--to_AND_--i-have-a-backup"}))
                return 2
            undone = rollback(db, all_migrations(), to_version=a.to)
            print(json.dumps({"ok": True, "undone": undone, "version": current_version(db)}))
            return 0
    except IrreversibleMigration as exc:
        print(json.dumps({"ok": False, "error": exc.code, "detail": str(exc)}))
        return 4
    except MigrationError as exc:
        print(json.dumps({"ok": False, "error": exc.code, "detail": str(exc)}))
        return 5
    finally:
        db.close()
    return 1


if __name__ == "__main__":
    sys.exit(main())
