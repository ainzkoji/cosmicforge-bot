"""Provenance for ``trade_fills`` — §13.11 / §14.5.

``trade_fills`` is the table every dataset builder, analytics view and
performance metric reads. It had no provenance column, and it holds two very
different kinds of row mixed together:

    strategy             account_id   n        window
    backfill_ensemble    backfill     16,494   2026-03-22 15:03 -> 16:42
    orchestrated         default       1,056   2026-03-28 -> 2026-09-06
    external_tradingview default           9
    paper_execution_smoke paper_smoke      2

The 16,494 rows are ``scripts/ml/historical_backfill.py``, which replays public
klines through a **standalone indicator engine** — its own ADX/ATR/MA-slope
logic and its own BUY/SELL/HOLD rule — and writes the result through the same
``record_fill()`` the live runner uses. It is not the production trading brain,
and 94% of the fill table is its output.

The programme is explicit: do not delete it, label it, and keep it out of
organic datasets by default.

Classification here is conservative. A row is labelled only where the source is
unambiguous from how the writer identified itself; anything else is left NULL
rather than guessed at, because a wrong provenance label is worse than a
missing one.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

LEGACY_BACKFILL = "LEGACY_BACKFILL"
TEST_FIXTURE = "TEST_FIXTURE"

#: (column, value) -> provenance. Only unambiguous writer signatures.
UNAMBIGUOUS_SOURCES: tuple[tuple[str, str, str], ...] = (
    ("account_id", "backfill", LEGACY_BACKFILL),
    ("strategy", "backfill_ensemble", LEGACY_BACKFILL),
    ("account_id", "paper_smoke", TEST_FIXTURE),
    ("strategy", "paper_execution_smoke", TEST_FIXTURE),
)


def has_fill_provenance_column(db: Any) -> bool:
    with db.connect() as conn:
        return "provenance" in {r[1] for r in conn.execute("PRAGMA table_info(trade_fills)")}


def ensure_fill_provenance_column(db: Any) -> bool:
    """Add ``trade_fills.provenance`` if it is missing. Returns True if added."""
    with db.connect() as conn:
        columns = {r[1] for r in conn.execute("PRAGMA table_info(trade_fills)")}
        if "provenance" in columns:
            return False
        conn.execute("ALTER TABLE trade_fills ADD COLUMN provenance TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_fills_provenance "
            "ON trade_fills(provenance)"
        )
    logger.info("[FILL_PROVENANCE] added trade_fills.provenance")
    return True


def classify_existing_fills(db: Any, *, dry_run: bool = False) -> dict[str, int]:
    """Label rows whose writer is unambiguous. Never overwrites a label.

    Returns how many rows each provenance gains. A row matching more than one
    rule is counted once, under the first rule that claims it -- the same order
    the updates apply in, so a dry run reports exactly what a real run will do.
    """
    if not dry_run:
        ensure_fill_provenance_column(db)
    elif not has_fill_provenance_column(db):
        # A dry run must not alter the schema, so with no column there is
        # nothing labelled and everything is unclassified.
        with db.connect() as conn:
            total = int(conn.execute("SELECT COUNT(*) FROM trade_fills").fetchone()[0])
        return {"UNCLASSIFIED": total}

    counts: dict[str, int] = {}
    with db.connect() as conn:
        claimed: list[tuple[str, str]] = []
        for column, value, provenance in UNAMBIGUOUS_SOURCES:
            # Exclude rows an earlier rule has already claimed, so nothing is
            # counted twice when a writer matches on both strategy and account.
            exclusions = "".join(f" AND NOT ({c}=?)" for c, _ in claimed)
            args = [value] + [v for _, v in claimed]
            matched = conn.execute(
                f"SELECT COUNT(*) FROM trade_fills "
                f"WHERE {column}=? AND provenance IS NULL{exclusions}",
                tuple(args),
            ).fetchone()[0]
            claimed.append((column, value))
            if not matched:
                continue
            counts[provenance] = counts.get(provenance, 0) + int(matched)
            if not dry_run:
                conn.execute(
                    f"UPDATE trade_fills SET provenance=? "
                    f"WHERE {column}=? AND provenance IS NULL",
                    (provenance, value),
                )
        remaining = int(
            conn.execute(
                "SELECT COUNT(*) FROM trade_fills WHERE provenance IS NULL"
            ).fetchone()[0]
        )
    # After a real run `remaining` is already the leftover. After a dry run
    # nothing was written, so subtract what the run would have labelled.
    labelled = sum(counts.values())
    counts["UNCLASSIFIED"] = remaining - labelled if dry_run else remaining
    return counts


def provenance_breakdown(db: Any) -> dict[str, int]:
    """Read-only. Never creates the column -- callers may be inspecting."""
    if not has_fill_provenance_column(db):
        with db.connect() as conn:
            total = int(conn.execute("SELECT COUNT(*) FROM trade_fills").fetchone()[0])
        return {"UNCLASSIFIED": total} if total else {}
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT COALESCE(provenance,'UNCLASSIFIED') p, COUNT(*) n "
            "FROM trade_fills GROUP BY p ORDER BY n DESC"
        ).fetchall()
    return {str(r[0]): int(r[1]) for r in rows}


def organic_fill_filter(alias: str = "trade_fills") -> str:
    """SQL predicate selecting fills that may count toward readiness.

    Unclassified rows are *excluded*. An unlabelled row predates the labelling
    and cannot be shown to be organic; counting it would be the same mistake
    the label exists to prevent.
    """
    from shared_lib.persistence.evidence_schema import ORGANIC_PROVENANCE

    values = ", ".join(f"'{p}'" for p in sorted(ORGANIC_PROVENANCE))
    return f"{alias}.provenance IN ({values})"
