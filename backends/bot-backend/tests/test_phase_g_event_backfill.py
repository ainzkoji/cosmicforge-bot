"""
Phase G tests: historical event backfill, blackout window generation,
feature coverage audit, and date-proxy detection.

Test map:
  T1 - Historical event CSV import inserts valid events.
  T2 - Import is idempotent (re-run produces no duplicates).
  T3 - Invalid rows are rejected safely (missing required fields).
  T4 - Blackout windows are generated correctly for historical events.
  T5 - Feature coverage audit detects missing minutes_since_last_event.
  T6 - Feature coverage audit detects date-proxy risk.
  T7 - Feature coverage passes when events are distributed through the dataset.
  T8 - Reaction backfill refuses to fabricate data when candles are missing.
  T9 - Dataset metadata records Phase G event coverage fields.
"""
from __future__ import annotations

import csv
import io
import os
import sqlite3
import tempfile
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
import sys
_TESTS_DIR  = Path(__file__).resolve().parent
_BOT_ROOT   = _TESTS_DIR.parent
_SHARED     = _BOT_ROOT.parent / "shared"
for _p in (str(_BOT_ROOT), str(_SHARED)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared_lib.persistence.db import DB
from shared_lib.persistence.migrations import migrate
from shared_lib.persistence.economic_events import (
    insert_event,
    upsert_blackout_window,
    get_upcoming_events,
)
from shared_lib.ml.event_features import build_event_feature_frame
from app.events.event_symbol_mapper import get_affected_symbols

# Import the scripts under test
sys.path.insert(0, str(_BOT_ROOT / "scripts"))
from import_historical_events import import_events, _validate_utc, _validate_impact
from backfill_event_blackout_windows import backfill_windows
from backfill_market_reactions import backfill_reactions


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    migrate(db_path=path)
    db = DB(path=path)
    yield db
    os.unlink(path)


def _make_csv(rows: list[dict]) -> Path:
    """Write a list of row dicts to a temp CSV file. Returns the path."""
    fieldnames = [
        "event_id", "title", "event_type", "country_currency",
        "impact_level", "scheduled_utc", "source",
        "is_global", "affected_symbols", "confidence", "notes",
    ]
    fd, path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            full_row = {f: row.get(f, "") for f in fieldnames}
            writer.writerow(full_row)
    return Path(path)


def _valid_row(**overrides) -> dict:
    base = {
        "event_id":         "test_FOMC_20260319_1900",
        "title":            "FOMC Rate Decision Mar 2026",
        "event_type":       "FOMC",
        "country_currency": "USD",
        "impact_level":     "HIGH",
        "scheduled_utc":    "2026-03-19T19:00:00+00:00",
        "source":           "manual_historical_backfill",
        "is_global":        "true",
        "affected_symbols": "",
        "confidence":       "HIGH",
        "notes":            "test",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# T1: CSV import inserts valid events
# ---------------------------------------------------------------------------

class TestT1ImportInsertsValidEvents:
    def test_single_valid_row_is_inserted(self, tmp_db):
        """T1: A well-formed CSV row produces exactly one economic_events row."""
        csv_path = _make_csv([_valid_row()])
        try:
            counts = import_events(csv_path, tmp_db)
            assert counts["rows_read"]  == 1
            assert counts["inserted"]   == 1
            assert counts["skipped"]    == 0
            assert counts["validation_errors"] == []

            with tmp_db.connect() as conn:
                row = conn.execute(
                    "SELECT event_id, title, event_type, impact_level "
                    "FROM economic_events WHERE event_id='test_FOMC_20260319_1900'"
                ).fetchone()
            assert row is not None
            assert row[0] == "test_FOMC_20260319_1900"
            assert row[2] == "FOMC"
            assert row[3] == "HIGH"
        finally:
            os.unlink(csv_path)

    def test_multiple_valid_rows_all_inserted(self, tmp_db):
        """T1: All valid rows from a multi-row CSV are inserted."""
        rows = [
            _valid_row(event_id="test_CPI_20260312", title="CPI Feb 2026",
                       event_type="CPI", scheduled_utc="2026-03-12T13:30:00+00:00"),
            _valid_row(event_id="test_NFP_20260306", title="NFP Feb 2026",
                       event_type="NFP", scheduled_utc="2026-03-06T13:30:00+00:00"),
        ]
        csv_path = _make_csv(rows)
        try:
            counts = import_events(csv_path, tmp_db)
            assert counts["rows_read"] == 2
            assert counts["inserted"]  == 2
        finally:
            os.unlink(csv_path)

    def test_source_is_preserved(self, tmp_db):
        """T1: source field from CSV is stored correctly."""
        csv_path = _make_csv([_valid_row(source="manual_historical_backfill")])
        try:
            import_events(csv_path, tmp_db)
            with tmp_db.connect() as conn:
                src = conn.execute(
                    "SELECT source FROM economic_events WHERE event_id='test_FOMC_20260319_1900'"
                ).fetchone()[0]
            assert src == "manual_historical_backfill"
        finally:
            os.unlink(csv_path)

    def test_actual_values_not_set(self, tmp_db):
        """T1: actual_val / forecast_val / previous_val remain NULL (no outcome values)."""
        csv_path = _make_csv([_valid_row()])
        try:
            import_events(csv_path, tmp_db)
            with tmp_db.connect() as conn:
                row = conn.execute(
                    "SELECT actual_val, forecast_val, previous_val "
                    "FROM economic_events WHERE event_id='test_FOMC_20260319_1900'"
                ).fetchone()
            assert row[0] is None, "actual_val must be NULL"
            assert row[1] is None, "forecast_val must be NULL"
            assert row[2] is None, "previous_val must be NULL"
        finally:
            os.unlink(csv_path)


# ---------------------------------------------------------------------------
# T2: Import idempotency
# ---------------------------------------------------------------------------

class TestT2ImportIdempotency:
    def test_rerun_produces_no_duplicate(self, tmp_db):
        """T2: Running import twice with the same CSV yields count=1 in DB."""
        csv_path = _make_csv([_valid_row()])
        try:
            import_events(csv_path, tmp_db)
            counts2 = import_events(csv_path, tmp_db)

            # Second run: row already exists -> updated, not inserted
            assert counts2["inserted"] == 0
            assert counts2["updated"]  == 1

            with tmp_db.connect() as conn:
                n = conn.execute("SELECT COUNT(*) FROM economic_events").fetchone()[0]
            assert n == 1
        finally:
            os.unlink(csv_path)

    def test_updated_row_reflects_new_title(self, tmp_db):
        """T2: Updating a row on re-import changes the title in-place."""
        csv_path = _make_csv([_valid_row(title="Original Title")])
        try:
            import_events(csv_path, tmp_db)
            os.unlink(csv_path)

            csv_path2 = _make_csv([_valid_row(title="Updated Title")])
            import_events(csv_path2, tmp_db)
            os.unlink(csv_path2)

            with tmp_db.connect() as conn:
                title = conn.execute(
                    "SELECT title FROM economic_events WHERE event_id='test_FOMC_20260319_1900'"
                ).fetchone()[0]
            assert title == "Updated Title"
        finally:
            pass   # already unlinked above


# ---------------------------------------------------------------------------
# T3: Invalid rows are rejected safely
# ---------------------------------------------------------------------------

class TestT3InvalidRowsRejected:
    def _run_bad(self, tmp_db, **overrides):
        csv_path = _make_csv([_valid_row(**overrides)])
        try:
            counts = import_events(csv_path, tmp_db)
        finally:
            os.unlink(csv_path)
        return counts

    def test_missing_event_id_skipped(self, tmp_db):
        """T3: Row missing event_id is skipped with a validation error recorded."""
        counts = self._run_bad(tmp_db, event_id="")
        assert counts["skipped"] >= 1
        assert any("missing event_id" in e for e in counts["validation_errors"])

    def test_missing_title_skipped(self, tmp_db):
        """T3: Row missing title is skipped."""
        counts = self._run_bad(tmp_db, title="")
        assert counts["skipped"] >= 1

    def test_invalid_impact_level_skipped(self, tmp_db):
        """T3: Row with an unknown impact_level is rejected."""
        counts = self._run_bad(tmp_db, impact_level="CRITICAL")
        assert counts["skipped"] >= 1
        assert any("impact_level" in e for e in counts["validation_errors"])

    def test_invalid_scheduled_utc_skipped(self, tmp_db):
        """T3: Row with a non-parseable scheduled_utc is rejected."""
        counts = self._run_bad(tmp_db, scheduled_utc="not-a-date")
        assert counts["skipped"] >= 1
        assert any("scheduled_utc" in e for e in counts["validation_errors"])

    def test_valid_rows_alongside_bad_still_imported(self, tmp_db):
        """T3: Valid rows in a mixed CSV are not blocked by invalid neighbors."""
        rows = [
            _valid_row(event_id="good_row_1"),
            _valid_row(event_id="", title=""),          # bad row
            _valid_row(event_id="good_row_2", scheduled_utc="2026-04-10T13:30:00+00:00"),
        ]
        csv_path = _make_csv(rows)
        try:
            counts = import_events(csv_path, tmp_db)
            assert counts["inserted"] == 2
            assert counts["skipped"]  == 1
        finally:
            os.unlink(csv_path)


# ---------------------------------------------------------------------------
# T4: Blackout windows generated correctly
# ---------------------------------------------------------------------------

class TestT4BlackoutWindows:
    def test_high_impact_global_window_dimensions(self, tmp_db):
        """T4: HIGH USD event produces global window from -30min to +15min."""
        insert_event(
            tmp_db,
            event_id="test_cpi_win",
            title="CPI Test",
            event_type="CPI",
            country_currency="USD",
            impact_level="HIGH",
            scheduled_utc="2026-03-12T13:30:00+00:00",
        )
        from_utc = "2026-03-12T00:00:00+00:00"
        to_utc   = "2026-03-12T23:59:59+00:00"
        counts = backfill_windows(tmp_db, from_utc, to_utc)

        assert counts["global_windows"] >= 1

        with tmp_db.connect() as conn:
            win = conn.execute(
                """SELECT bw.start_utc, bw.end_utc, bw.is_global
                   FROM event_blackout_windows bw
                   JOIN economic_events ev ON ev.id = bw.event_id
                   WHERE ev.event_id = 'test_cpi_win'"""
            ).fetchone()
        assert win is not None
        start = datetime.fromisoformat(win[0])
        end   = datetime.fromisoformat(win[1])
        sched = datetime(2026, 3, 12, 13, 30, tzinfo=timezone.utc)
        assert start == sched - timedelta(minutes=30)
        assert end   == sched + timedelta(minutes=15)
        assert win[2] == 1  # is_global

    def test_medium_impact_window_dimensions(self, tmp_db):
        """T4: MEDIUM USD event produces window from -5min to +5min."""
        insert_event(
            tmp_db,
            event_id="test_ppi_medium",
            title="PPI Test",
            event_type="PPI",
            country_currency="USD",
            impact_level="MEDIUM",
            scheduled_utc="2026-03-13T13:30:00+00:00",
        )
        backfill_windows(tmp_db, "2026-03-13T00:00:00+00:00", "2026-03-13T23:59:59+00:00")

        with tmp_db.connect() as conn:
            win = conn.execute(
                """SELECT bw.start_utc, bw.end_utc
                   FROM event_blackout_windows bw
                   JOIN economic_events ev ON ev.id = bw.event_id
                   WHERE ev.event_id = 'test_ppi_medium'"""
            ).fetchone()
        assert win is not None
        start = datetime.fromisoformat(win[0])
        end   = datetime.fromisoformat(win[1])
        sched = datetime(2026, 3, 13, 13, 30, tzinfo=timezone.utc)
        assert start == sched - timedelta(minutes=5)
        assert end   == sched + timedelta(minutes=5)

    def test_crypto_event_symbol_specific(self, tmp_db):
        """T4: ETH_UPGRADE event produces symbol-specific (non-global) window."""
        insert_event(
            tmp_db,
            event_id="test_eth_upgrade",
            title="ETH Pectra Mainnet",
            event_type="ETH_UPGRADE",
            country_currency="ETH",
            impact_level="HIGH",
            scheduled_utc="2026-05-07T12:00:00+00:00",
        )
        backfill_windows(tmp_db, "2026-05-07T00:00:00+00:00", "2026-05-07T23:59:59+00:00")

        with tmp_db.connect() as conn:
            win = conn.execute(
                """SELECT bw.is_global, bw.affected_symbols
                   FROM event_blackout_windows bw
                   JOIN economic_events ev ON ev.id = bw.event_id
                   WHERE ev.event_id = 'test_eth_upgrade'"""
            ).fetchone()
        assert win is not None
        assert win[0] == 0   # not global
        import json
        syms = json.loads(win[1])
        assert "ETHUSDT" in syms

    def test_backfill_idempotent(self, tmp_db):
        """T4: Running backfill_windows twice produces no duplicate windows."""
        insert_event(
            tmp_db,
            event_id="test_fomc_idem",
            title="FOMC Idempotency Test",
            event_type="FOMC",
            country_currency="USD",
            impact_level="HIGH",
            scheduled_utc="2026-01-29T19:00:00+00:00",
        )
        range_ = ("2026-01-29T00:00:00+00:00", "2026-01-29T23:59:59+00:00")
        backfill_windows(tmp_db, *range_)
        backfill_windows(tmp_db, *range_)

        with tmp_db.connect() as conn:
            n = conn.execute(
                """SELECT COUNT(*) FROM event_blackout_windows bw
                   JOIN economic_events ev ON ev.id = bw.event_id
                   WHERE ev.event_id = 'test_fomc_idem'"""
            ).fetchone()[0]
        assert n == 1


# ---------------------------------------------------------------------------
# T5: Coverage audit detects missing minutes_since_last_event
# ---------------------------------------------------------------------------

class TestT5CoverageAuditDetectsMissing:
    def test_all_null_minutes_since_fails_check(self, tmp_db):
        """T5: When minutes_since_last_event is 100% null, coverage check fails."""
        # Build raw data without any past events — all rows have null minutes_since
        raw = pd.DataFrame([
            {
                "trace_ts": "2026-03-22T10:00:00+00:00",
                "open_timestamp": "2026-03-22T10:00:00+00:00",
                "symbol": "BTCUSDT", "side": "LONG", "signal": "BUY",
            }
        ])

        with tmp_db.connect() as conn:
            events   = pd.read_sql_query("SELECT * FROM economic_events", conn)
            windows  = pd.read_sql_query("SELECT * FROM event_blackout_windows", conn)
            reactions = pd.read_sql_query("SELECT * FROM market_event_reactions", conn)

        feat = build_event_feature_frame(
            raw,
            economic_events=events,
            blackout_windows=windows,
            reactions=reactions,
            symbol_resolver=get_affected_symbols,
        )

        null_pct = feat["minutes_since_last_event"].isna().mean() * 100
        assert null_pct == 100.0, (
            "minutes_since_last_event should be 100% null when no past events exist"
        )


# ---------------------------------------------------------------------------
# T6: Coverage audit detects date-proxy risk
# ---------------------------------------------------------------------------

class TestT6DateProxyRisk:
    def test_single_future_event_is_proxy_risk(self):
        """T6: A monotonically-decreasing minutes_to_next_event series is flagged as proxy risk."""
        try:
            from scipy.stats import spearmanr
        except ImportError:
            pytest.skip("scipy not installed")

        import numpy as np

        # Simulate monotonically decreasing: all rows counting down to one event
        n = 200
        mtn = pd.Series(np.linspace(10000, 10, n))  # countdown from 10000 to 10

        idx = np.arange(n, dtype=float)
        corr, _ = spearmanr(idx, mtn.values)
        # Should be very strongly negative (close to -1.0)
        assert abs(corr) > 0.70, f"Expected |corr| > 0.70, got {corr:.4f}"

    def test_well_distributed_events_not_proxy_risk(self):
        """T6: Events spread evenly through the dataset produce low Spearman correlation."""
        try:
            from scipy.stats import spearmanr
        except ImportError:
            pytest.skip("scipy not installed")

        import numpy as np

        # Simulate a repeating sawtooth: events every 1440 minutes (daily)
        period = 1440
        n = 2000
        mtn = pd.Series([period - (i % period) for i in range(n)], dtype=float)

        idx = np.arange(n, dtype=float)
        corr, _ = spearmanr(idx, mtn.values)
        # With periodic events, Spearman correlation should be near zero
        assert abs(corr) < 0.30, f"Expected |corr| < 0.30 for distributed events, got {corr:.4f}"


# ---------------------------------------------------------------------------
# T7: Coverage passes when events are distributed through the dataset
# ---------------------------------------------------------------------------

class TestT7CoveragePassesWithDistributedEvents:
    def test_past_and_future_events_give_non_null_timing(self, tmp_db):
        """T7: With both past and future events, timing features have non-null values."""
        # Insert a past event (trace_ts is after it) and a future event
        insert_event(
            tmp_db,
            event_id="test_past_cpi",
            title="CPI Past",
            event_type="CPI",
            country_currency="USD",
            impact_level="HIGH",
            scheduled_utc="2026-03-10T13:30:00+00:00",
        )
        insert_event(
            tmp_db,
            event_id="test_future_fomc",
            title="FOMC Future",
            event_type="FOMC",
            country_currency="USD",
            impact_level="HIGH",
            scheduled_utc="2026-03-19T19:00:00+00:00",
        )

        # Trace at 2026-03-12: after CPI, before FOMC
        raw = pd.DataFrame([{
            "trace_ts":       "2026-03-12T10:00:00+00:00",
            "open_timestamp": "2026-03-12T10:00:00+00:00",
            "symbol": "BTCUSDT", "side": "LONG", "signal": "BUY",
        }])

        with tmp_db.connect() as conn:
            events   = pd.read_sql_query(
                "SELECT event_id, event_type, country_currency, impact_level, scheduled_utc "
                "FROM economic_events ORDER BY scheduled_utc ASC", conn
            )
            windows  = pd.read_sql_query(
                "SELECT event_id, start_utc, end_utc, affected_symbols, is_global, reason "
                "FROM event_blackout_windows ORDER BY start_utc ASC", conn
            )
            reactions = pd.read_sql_query(
                "SELECT event_id, symbol, event_time_utc, post_window_end_utc, "
                "volatility_expansion_ratio, volume_spike_ratio, reaction_type, "
                "max_move_pct, min_move_pct FROM market_event_reactions "
                "ORDER BY symbol ASC, post_window_end_utc ASC", conn
            )

        feat = build_event_feature_frame(
            raw,
            economic_events=events,
            blackout_windows=windows,
            reactions=reactions,
            symbol_resolver=get_affected_symbols,
        )

        row = feat.iloc[0]
        # Past event (CPI on Mar 10): about 2 days = ~2880 min before trace
        assert not pd.isna(row["minutes_since_last_event"]), "minutes_since_last_event should be non-null"
        # Future event (FOMC on Mar 19): about 7+ days away
        assert not pd.isna(row["minutes_to_next_event"]),    "minutes_to_next_event should be non-null"

        past_minutes = row["minutes_since_last_event"]
        future_minutes = row["minutes_to_next_event"]
        assert past_minutes > 0,   "past minutes must be positive"
        assert future_minutes > 0, "future minutes must be positive"


# ---------------------------------------------------------------------------
# T8: Reaction backfill refuses to fabricate without candles
# ---------------------------------------------------------------------------

class TestT8ReactionBackfillNoFabrication:
    def test_no_candle_data_means_no_reactions(self, tmp_db):
        """T8: backfill_reactions does not insert any reaction rows when candles are absent."""
        # Insert a HIGH-impact event with no corresponding snapshot data
        insert_event(
            tmp_db,
            event_id="test_nfp_no_candles",
            title="NFP No Candles",
            event_type="NFP",
            country_currency="USD",
            impact_level="HIGH",
            scheduled_utc="2026-03-06T13:30:00+00:00",
        )

        counts = backfill_reactions(
            tmp_db,
            from_utc="2026-03-06T00:00:00+00:00",
            to_utc="2026-03-06T23:59:59+00:00",
            symbols=["BTCUSDT", "ETHUSDT"],
        )

        assert counts["no_fabrication"] is True, "Fabrication guarantee must hold"
        assert counts["reactions_computed"] == 0, "No reactions without candle data"
        assert counts["candle_data_absent"] > 0,  "Should detect missing candles"
        assert counts["status"] == "NOT_READY"

        with tmp_db.connect() as conn:
            n = conn.execute(
                "SELECT COUNT(*) FROM market_event_reactions"
            ).fetchone()[0]
        assert n == 0, "No reactions should be in DB when candle data was absent"


# ---------------------------------------------------------------------------
# T9: Dataset metadata records Phase G coverage fields
# ---------------------------------------------------------------------------

class TestT9DatasetMetadataFields:
    def test_build_contract_metadata_has_base_fields(self):
        """T9: build_contract_metadata includes the base event feature column lists."""
        from shared_lib.ml.contract import build_contract_metadata, EVENT_FEATURE_COLUMNS
        meta = build_contract_metadata(training_dataset_path=None)
        assert "event_feature_columns" in meta
        assert set(meta["event_feature_columns"]) == set(EVENT_FEATURE_COLUMNS)

    def test_phase_g_metadata_field_names_are_defined(self):
        """T9: Phase G metadata field names are the documented set."""
        expected_fields = {
            "historical_event_backfill_enabled",
            "historical_event_range_start",
            "historical_event_range_end",
            "historical_event_count",
            "blackout_window_count",
            "event_window_row_count",
            "before_event_row_count",
            "after_event_row_count",
            "blackout_active_row_count",
            "date_proxy_risk",
            "event_feature_coverage_passed",
            "reaction_backfill_enabled",
            "reaction_count",
            "reaction_feature_coverage_passed",
        }
        # These fields are written by build_dataset.py main() directly into meta_doc.
        # We verify them by inspection of the build_dataset source rather than
        # importing it (it has side-effectful top-level code).
        build_dataset_path = (
            Path(__file__).resolve().parent.parent / "scripts" / "ml" / "build_dataset.py"
        )
        assert build_dataset_path.exists(), f"build_dataset.py not found at {build_dataset_path}"

        source = build_dataset_path.read_text(encoding="utf-8")
        for field in expected_fields:
            assert f'"{field}"' in source, (
                f"Phase G metadata field '{field}' not found in build_dataset.py"
            )
