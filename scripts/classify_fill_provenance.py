"""Label the legacy backfill in ``trade_fills`` — §13.11.

94% of the canonical fill table is the output of
``scripts/ml/historical_backfill.py``, a standalone indicator engine that is
not the production trading brain, written through the same ``record_fill()``
the live runner uses and carrying no provenance:

    strategy              account_id    n
    backfill_ensemble     backfill      16,494
    orchestrated          default        1,056
    external_tradingview  default            9
    paper_execution_smoke paper_smoke        2

This adds ``trade_fills.provenance`` and labels the rows whose writer is
unambiguous. It **deletes nothing and changes no value**; rows the writer did
not clearly identify are left NULL rather than guessed at, and an existing
label is never overwritten.

    python scripts/classify_fill_provenance.py                 # dry run
    python scripts/classify_fill_provenance.py --apply
    python scripts/classify_fill_provenance.py --db <path> --apply
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND = os.path.join(REPO_ROOT, "backends", "bot-backend")
SHARED = os.path.join(REPO_ROOT, "backends", "shared")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=None, help="database path (default: DATABASE_URL)")
    parser.add_argument("--apply", action="store_true", help="write the labels")
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
    print(f"database: {db.path}")
    print(f"before  : {provenance_breakdown(db)}")

    counts = classify_existing_fills(db, dry_run=not args.apply)
    verb = "would label" if not args.apply else "labelled"
    for provenance, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        if provenance == "UNCLASSIFIED":
            print(f"  left alone   {n:>7,}  (writer not unambiguous)")
        else:
            print(f"  {verb} {n:>7,}  {provenance}")

    if args.apply:
        print(f"after   : {provenance_breakdown(db)}")
    else:
        print("\ndry run — nothing written. Re-run with --apply to write the labels.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
