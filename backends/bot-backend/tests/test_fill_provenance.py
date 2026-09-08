"""Provenance for ``trade_fills`` — §13.11 / §14.5.

94% of the production fill table (16,494 of 17,561 rows) is the output of
``scripts/ml/historical_backfill.py``, which replays klines through a
standalone indicator engine — not the production trading brain — and wrote them
through the same ``record_fill()`` the live runner uses, with no provenance at
all. Any dataset built from that table without a label mixes the two.
"""
from __future__ import annotations

import pytest

from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import ORGANIC_PROVENANCE
from shared_lib.persistence.fill_provenance import (
    LEGACY_BACKFILL,
    TEST_FIXTURE,
    classify_existing_fills,
    ensure_fill_provenance_column,
    organic_fill_filter,
    provenance_breakdown,
)
from shared_lib.persistence.migrations import migrate
from shared_lib.persistence.trade_fills import record_fill


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    return database


def write(db, **kw):
    base = dict(
        symbol="BTCUSDT", side="LONG", action="OPEN", qty=1.0, price=100.0,
        account_id="default", strategy="orchestrated",
    )
    base.update(kw)
    return record_fill(db, **base)


def test_migrate_provides_the_column(db):
    with db.connect() as conn:
        columns = {r[1] for r in conn.execute("PRAGMA table_info(trade_fills)")}
    assert "provenance" in columns
    assert ensure_fill_provenance_column(db) is False  # idempotent


def test_a_fill_can_carry_its_provenance(db):
    write(db, provenance="PAPER_FORWARD_VALIDATION")
    assert provenance_breakdown(db) == {"PAPER_FORWARD_VALIDATION": 1}


def test_a_fill_without_provenance_is_unclassified_not_organic(db):
    write(db)
    assert provenance_breakdown(db) == {"UNCLASSIFIED": 1}


def test_the_legacy_backfill_is_labelled_by_its_writer_signature(db):
    write(db, account_id="backfill", strategy="backfill_ensemble")
    write(db, account_id="paper_smoke", strategy="paper_execution_smoke")
    write(db)  # the real runner: deliberately left alone

    counts = classify_existing_fills(db)
    assert counts[LEGACY_BACKFILL] == 1
    assert counts[TEST_FIXTURE] == 1
    assert counts["UNCLASSIFIED"] == 1

    breakdown = provenance_breakdown(db)
    assert breakdown[LEGACY_BACKFILL] == 1
    assert breakdown["UNCLASSIFIED"] == 1


def test_a_dry_run_reports_exactly_what_a_real_run_does(db):
    """A row matching two rules must be counted once, not twice."""
    for _ in range(3):
        write(db, account_id="backfill", strategy="backfill_ensemble")

    dry = classify_existing_fills(db, dry_run=True)
    assert provenance_breakdown(db) == {"UNCLASSIFIED": 3}, "dry run must not write"

    applied = classify_existing_fills(db, dry_run=False)
    assert dry == applied
    assert applied[LEGACY_BACKFILL] == 3


def test_classification_never_overwrites_an_existing_label(db):
    write(db, account_id="backfill", provenance="PAPER_FORWARD")
    classify_existing_fills(db)
    assert provenance_breakdown(db) == {"PAPER_FORWARD": 1}


def test_unclassified_rows_are_excluded_from_organic_selection(db):
    write(db, provenance="PAPER_FORWARD")
    write(db, account_id="backfill", provenance=LEGACY_BACKFILL)
    write(db)  # unclassified

    with db.connect() as conn:
        organic = conn.execute(
            f"SELECT COUNT(*) FROM trade_fills WHERE {organic_fill_filter()}"
        ).fetchone()[0]
    assert organic == 1
    assert LEGACY_BACKFILL not in ORGANIC_PROVENANCE


def test_the_backfill_script_declares_itself_legacy():
    """§13.11 — its output must never look like the live bot's."""
    from pathlib import Path

    source = Path("scripts/ml/historical_backfill.py").read_text(encoding="utf-8")
    assert "LEGACY_BACKFILL" in source
    assert "NOT the production trading brain" in source
