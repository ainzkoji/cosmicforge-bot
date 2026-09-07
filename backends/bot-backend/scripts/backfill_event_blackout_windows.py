#!/usr/bin/env python3
"""
G2: Backfill blackout windows for historical economic events.

For every event in the DB within the specified date range, creates a blackout
window according to impact level:

  HIGH  (USD or global):  start = scheduled_utc - 30 min
                          end   = scheduled_utc + 15 min
                          is_global = True

  MEDIUM (USD):           start = scheduled_utc - 5 min
                          end   = scheduled_utc + 5 min
                          is_global = False  (log/display only by default)

  Crypto-specific:        symbol-specific where mapping is reliable
                          start = scheduled_utc - 30 min
                          end   = scheduled_utc + 30 min
                          is_global = False

Idempotent: re-running produces no duplicate windows.

Usage:
  python scripts/backfill_event_blackout_windows.py \\
      --from 2025-12-01 --to 2026-05-25

  python scripts/backfill_event_blackout_windows.py \\
      --from 2025-12-01 --to 2026-05-25 \\
      --db path/to/cosmicforge.db
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
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
from shared_lib.persistence.economic_events import (
    get_upcoming_events,
    upsert_blackout_window,
)

_DEFAULT_DB = (
    Path(__file__).resolve().parent.parent.parent
    / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
)

# ---------------------------------------------------------------------------
# Window rules
# ---------------------------------------------------------------------------

# Minutes before/after event for each impact level
_WINDOW_RULES = {
    "HIGH":   {"pre": 30, "post": 15},
    "MEDIUM": {"pre":  5, "post":  5},
    "LOW":    {"pre":  0, "post":  0},   # no window for LOW
}

# These currencies always trigger a global (all-symbol) block for HIGH events
_GLOBAL_BLOCK_CURRENCIES = {"USD"}

# Crypto event types that are symbol-specific
_CRYPTO_EVENT_TYPES = {
    "ETH_UPGRADE":    {"symbols": ["ETHUSDT", "ETHBTC"],        "pre": 30, "post": 30},
    "BTC_HALVING":    {"symbols": ["BTCUSDT"],                  "pre": 60, "post": 60},
    "SOL_MAINNET":    {"symbols": ["SOLUSDT"],                  "pre": 30, "post": 30},
    "XRP_SEC":        {"symbols": ["XRPUSDT"],                  "pre": 30, "post": 30},
    "BTC_ETF_DECISION": {"symbols": ["BTCUSDT"],                "pre": 60, "post": 60},
}


def _to_utc(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc)


def _fmt(dt: datetime) -> str:
    return dt.isoformat()


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def backfill_windows(db: DB, from_utc: str, to_utc: str) -> dict:
    """
    For all events in [from_utc, to_utc], generate blackout windows.
    Returns a summary dict.
    """
    events = get_upcoming_events(db, from_utc=from_utc, to_utc=to_utc)

    counts = {
        "events_processed":   0,
        "windows_created":    0,
        "windows_updated":    0,
        "windows_skipped":    0,
        "global_windows":     0,
        "symbol_specific_windows": 0,
        "errors":             [],
    }

    # Snapshot existing window count to detect created vs updated
    with db.connect() as conn:
        existing_count = conn.execute(
            "SELECT COUNT(*) FROM event_blackout_windows"
        ).fetchone()[0]

    windows_before = existing_count

    for ev in events:
        counts["events_processed"] += 1

        db_id      = ev["id"]            # integer PK in economic_events
        event_type = ev["event_type"]
        currency   = ev["country_currency"].upper()
        impact     = ev["impact_level"].upper()

        try:
            scheduled = _to_utc(ev["scheduled_utc"])
        except (ValueError, AttributeError):
            counts["errors"].append(
                f"event_id={ev['event_id']}: invalid scheduled_utc '{ev['scheduled_utc']}'"
            )
            counts["windows_skipped"] += 1
            continue

        # --- Crypto-specific window ---
        if event_type in _CRYPTO_EVENT_TYPES:
            cfg = _CRYPTO_EVENT_TYPES[event_type]
            start = scheduled - timedelta(minutes=cfg["pre"])
            end   = scheduled + timedelta(minutes=cfg["post"])
            reason = f"CRYPTO_{impact}_{currency}_{event_type}"
            try:
                upsert_blackout_window(
                    db,
                    event_db_id=db_id,
                    start_utc=_fmt(start),
                    end_utc=_fmt(end),
                    reason=reason,
                    affected_symbols=cfg["symbols"],
                    is_global=False,
                )
                counts["symbol_specific_windows"] += 1
            except Exception as exc:
                counts["errors"].append(
                    f"event_id={ev['event_id']}: window write failed — {exc}"
                )
            continue

        # --- Standard macro window ---
        rules = _WINDOW_RULES.get(impact)
        if not rules or (rules["pre"] == 0 and rules["post"] == 0):
            counts["windows_skipped"] += 1
            continue

        is_global = (impact == "HIGH" and currency in _GLOBAL_BLOCK_CURRENCIES) or (
            impact == "HIGH" and currency not in {"ETH", "BTC", "SOL", "XRP"}
        )

        start  = scheduled - timedelta(minutes=rules["pre"])
        end    = scheduled + timedelta(minutes=rules["post"])
        reason = f"{impact}_IMPACT_{currency}_{event_type}"

        try:
            upsert_blackout_window(
                db,
                event_db_id=db_id,
                start_utc=_fmt(start),
                end_utc=_fmt(end),
                reason=reason,
                affected_symbols=None if is_global else [],
                is_global=is_global,
            )
            if is_global:
                counts["global_windows"] += 1
            else:
                counts["symbol_specific_windows"] += 1
        except Exception as exc:
            counts["errors"].append(
                f"event_id={ev['event_id']}: window write failed — {exc}"
            )

    # Tally created vs updated by comparing before/after counts
    with db.connect() as conn:
        new_count = conn.execute(
            "SELECT COUNT(*) FROM event_blackout_windows"
        ).fetchone()[0]

    net_new = new_count - windows_before
    counts["windows_created"] = max(net_new, 0)
    counts["windows_updated"]  = max(
        (counts["global_windows"] + counts["symbol_specific_windows"]) - net_new, 0
    )

    return counts


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate blackout windows for historical events"
    )
    p.add_argument("--from", dest="from_date", required=True,
                   help="Start date YYYY-MM-DD (inclusive)")
    p.add_argument("--to",   dest="to_date",   required=True,
                   help="End date YYYY-MM-DD (inclusive)")
    p.add_argument("--db",   default=str(_DEFAULT_DB),
                   help="Path to cosmicforge.db")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    db_p = Path(args.db).resolve()

    if not db_p.exists():
        print(f"[FAIL] Database not found: {db_p}", file=sys.stderr)
        sys.exit(1)

    from_utc = f"{args.from_date}T00:00:00+00:00"
    to_utc   = f"{args.to_date}T23:59:59+00:00"

    print(f"DB path      : {db_p}")
    print(f"Date range   : {args.from_date} -> {args.to_date}")

    db = DB(path=str(db_p))
    counts = backfill_windows(db, from_utc, to_utc)

    print(f"\n--- Blackout window backfill summary ---")
    print(f"  Events processed          : {counts['events_processed']}")
    print(f"  Windows created           : {counts['windows_created']}")
    print(f"  Windows updated (upserted): {counts['windows_updated']}")
    print(f"  Events skipped (no window): {counts['windows_skipped']}")
    print(f"  Global windows            : {counts['global_windows']}")
    print(f"  Symbol-specific windows   : {counts['symbol_specific_windows']}")
    print(f"  Errors                    : {len(counts['errors'])}")
    for err in counts["errors"]:
        print(f"    [WARN] {err}")

    with db.connect() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM event_blackout_windows"
        ).fetchone()[0]
        glob  = conn.execute(
            "SELECT COUNT(*) FROM event_blackout_windows WHERE is_global=1"
        ).fetchone()[0]
        sym   = conn.execute(
            "SELECT COUNT(*) FROM event_blackout_windows WHERE is_global=0"
        ).fetchone()[0]
    print(f"\n  Total windows in DB : {total}  (global={glob}, symbol-specific={sym})")
    print("\n[OK] Blackout window backfill complete.")


if __name__ == "__main__":
    main()
