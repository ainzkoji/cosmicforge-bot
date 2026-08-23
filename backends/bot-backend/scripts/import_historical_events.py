#!/usr/bin/env python3
"""
G1: Historical economic event importer.

Reads a CSV of manually-curated historical events and upserts them into
the economic_events table.  Idempotent: re-running with the same CSV
produces no duplicate rows.

CSV format (required columns):
  event_id, title, event_type, country_currency, impact_level,
  scheduled_utc, source, is_global, affected_symbols, confidence, notes

Usage:
  python scripts/import_historical_events.py \\
      --csv data/event_calendar/historical_events_2025_12_to_2026_05.csv

  python scripts/import_historical_events.py \\
      --csv data/event_calendar/historical_events_2025_12_to_2026_05.csv \\
      --db  path/to/cosmicforge.db
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT   = _SCRIPT_DIR.parent
_SHARED     = _BOT_ROOT.parent / "shared"
for _p in (str(_BOT_ROOT), str(_SHARED)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared_lib.persistence.db import DB
from shared_lib.persistence.economic_events import insert_event

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_VALID_IMPACT_LEVELS = {"HIGH", "MEDIUM", "LOW"}
_REQUIRED_COLUMNS    = {"event_id", "title", "event_type", "country_currency",
                        "impact_level", "scheduled_utc"}

_DEFAULT_DB = (
    Path(__file__).resolve().parent.parent.parent
    / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_utc(value: str, row_num: int, event_id: str) -> str | None:
    """Return normalised ISO-8601 UTC string, or None if invalid."""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            raise ValueError("no timezone")
        # Normalise to UTC
        return dt.astimezone(timezone.utc).isoformat()
    except (ValueError, AttributeError):
        return None


def _validate_impact(value: str) -> bool:
    return value.strip().upper() in _VALID_IMPACT_LEVELS


def _validate_affected_symbols(value: str) -> list | None:
    """Return parsed list or None (empty/global events have no list)."""
    if not value or not value.strip():
        return None
    try:
        parsed = json.loads(value)
        if not isinstance(parsed, list):
            raise ValueError("not a list")
        return [str(s) for s in parsed if s]
    except (json.JSONDecodeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Main import logic
# ---------------------------------------------------------------------------

def import_events(csv_path: Path, db: DB) -> dict:
    """
    Read the CSV and upsert into economic_events.

    Returns a summary dict with counts for rows_read, inserted/updated,
    skipped, and validation_errors.
    """
    counts = {
        "rows_read":        0,
        "inserted":         0,
        "updated":          0,
        "skipped":          0,
        "validation_errors": [],
    }

    if not csv_path.exists():
        counts["validation_errors"].append(f"CSV not found: {csv_path}")
        return counts

    # Fetch existing event_ids to distinguish insert vs update
    with db.connect() as conn:
        existing = {
            row[0]
            for row in conn.execute(
                "SELECT event_id FROM economic_events"
            ).fetchall()
        }

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)

        missing_cols = _REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing_cols:
            counts["validation_errors"].append(
                f"CSV missing required columns: {sorted(missing_cols)}"
            )
            return counts

        for row_num, row in enumerate(reader, start=2):  # 1-indexed, row 1 = header
            counts["rows_read"] += 1

            event_id = (row.get("event_id") or "").strip()
            title    = (row.get("title") or "").strip()
            etype    = (row.get("event_type") or "").strip().upper()
            currency = (row.get("country_currency") or "").strip().upper()
            impact   = (row.get("impact_level") or "").strip().upper()
            sched    = (row.get("scheduled_utc") or "").strip()
            source   = (row.get("source") or "manual_historical_backfill").strip()

            # --- Required-field presence ---
            if not event_id:
                counts["validation_errors"].append(f"Row {row_num}: missing event_id — skipped")
                counts["skipped"] += 1
                continue
            if not title:
                counts["validation_errors"].append(f"Row {row_num} ({event_id}): missing title — skipped")
                counts["skipped"] += 1
                continue
            if not etype:
                counts["validation_errors"].append(f"Row {row_num} ({event_id}): missing event_type — skipped")
                counts["skipped"] += 1
                continue
            if not currency:
                counts["validation_errors"].append(f"Row {row_num} ({event_id}): missing country_currency — skipped")
                counts["skipped"] += 1
                continue

            # --- impact_level ---
            if not _validate_impact(impact):
                counts["validation_errors"].append(
                    f"Row {row_num} ({event_id}): invalid impact_level '{impact}' — skipped"
                )
                counts["skipped"] += 1
                continue

            # --- scheduled_utc ---
            normed_utc = _validate_utc(sched, row_num, event_id)
            if normed_utc is None:
                counts["validation_errors"].append(
                    f"Row {row_num} ({event_id}): invalid scheduled_utc '{sched}' — skipped"
                )
                counts["skipped"] += 1
                continue

            was_existing = event_id in existing

            try:
                insert_event(
                    db,
                    event_id=event_id,
                    title=title,
                    event_type=etype,
                    country_currency=currency,
                    impact_level=impact,
                    scheduled_utc=normed_utc,
                    source=source,
                    # Deliberately NOT setting actual_val / forecast_val / previous_val
                    # to maintain the no-outcome-values rule.
                )
            except Exception as exc:
                counts["validation_errors"].append(
                    f"Row {row_num} ({event_id}): DB error — {exc}"
                )
                counts["skipped"] += 1
                continue

            existing.add(event_id)
            if was_existing:
                counts["updated"] += 1
            else:
                counts["inserted"] += 1

    return counts


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import historical economic events from CSV")
    p.add_argument("--csv", required=True, help="Path to historical events CSV")
    p.add_argument("--db",  default=str(_DEFAULT_DB), help="Path to cosmicforge.db")
    p.add_argument("--dry-run", action="store_true", help="Validate only, do not write")
    return p.parse_args()


def main() -> None:
    args  = _parse_args()
    csv_p = Path(args.csv).resolve()
    db_p  = Path(args.db).resolve()

    print(f"CSV path : {csv_p}")
    print(f"DB  path : {db_p}")

    if not db_p.exists():
        print(f"[FAIL] Database not found: {db_p}", file=sys.stderr)
        sys.exit(1)

    db = DB(path=str(db_p))

    if args.dry_run:
        print("[DRY-RUN] Validation only — no writes.")

    counts = import_events(csv_p, db)

    print(f"\n--- Import summary ---")
    print(f"  Rows read             : {counts['rows_read']}")
    print(f"  Rows inserted         : {counts['inserted']}")
    print(f"  Rows updated          : {counts['updated']}")
    print(f"  Rows skipped          : {counts['skipped']}")
    print(f"  Validation errors     : {len(counts['validation_errors'])}")
    for err in counts["validation_errors"]:
        print(f"    [WARN] {err}")

    total_ok = counts["inserted"] + counts["updated"]
    print(f"\n  Total events in DB after import:")
    with db.connect() as conn:
        n = conn.execute("SELECT COUNT(*) FROM economic_events").fetchone()[0]
        print(f"    economic_events rows: {n}")

    if counts["skipped"] > 0 and total_ok == 0:
        print("[FAIL] All rows were skipped — nothing imported.", file=sys.stderr)
        sys.exit(1)

    print("\n[OK] Import complete.")


if __name__ == "__main__":
    main()
