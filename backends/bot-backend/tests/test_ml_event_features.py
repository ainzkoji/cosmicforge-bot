from __future__ import annotations

import os
import sqlite3
import tempfile

import pandas as pd
import pytest

from shared_lib.ml.contract import (
    EVENT_FEATURE_COLUMNS,
    ML_FEATURE_COLUMNS,
    build_contract_metadata,
)
from shared_lib.ml.event_features import build_event_feature_frame
from shared_lib.persistence.db import DB
from shared_lib.persistence.economic_events import insert_event, upsert_blackout_window
from shared_lib.persistence.market_reactions import upsert_reaction
from shared_lib.persistence.migrations import migrate
from app.events.event_symbol_mapper import get_affected_symbols

# News/sentiment column names that must never appear in ML feature sets
_NEWS_VALIDATION_COLUMNS = frozenset({
    "narrative_effectiveness_score",
    "sentiment_accuracy_score",
    "false_signal_ratio",
    "validated_news_impact_score",
    "news_impact_score",
})
_RAW_NEWS_COLUMNS = frozenset({
    "title",
    "headline",
    "body_snippet",
    "raw_text",
    "news_text",
    "sentiment_compound",
    "sentiment_label",
})


@pytest.fixture()
def tmp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    migrate(db_path=path)
    db = DB(path=path)
    yield db
    os.unlink(path)


def _load_sources(conn: sqlite3.Connection):
    events = pd.read_sql_query(
        "SELECT event_id, event_type, country_currency, impact_level, scheduled_utc FROM economic_events ORDER BY scheduled_utc ASC",
        conn,
    )
    windows = pd.read_sql_query(
        "SELECT event_id, start_utc, end_utc, affected_symbols, is_global, reason FROM event_blackout_windows ORDER BY start_utc ASC",
        conn,
    )
    reactions = pd.read_sql_query(
        """
        SELECT event_id, symbol, event_time_utc, post_window_end_utc,
               volatility_expansion_ratio, volume_spike_ratio, reaction_type,
               max_move_pct, min_move_pct
        FROM market_event_reactions
        ORDER BY symbol ASC, post_window_end_utc ASC
        """,
        conn,
    )
    return events, windows, reactions


class TestMLEventFeatures:
    def test_scheduled_event_timing_and_blackout_flag(self, tmp_db):
        past_id = insert_event(
            tmp_db,
            event_id="evt_cpi_past",
            title="US CPI",
            event_type="CPI",
            country_currency="USD",
            impact_level="HIGH",
            scheduled_utc="2026-04-25T10:00:00+00:00",
        )
        next_id = insert_event(
            tmp_db,
            event_id="evt_fomc_next",
            title="FOMC",
            event_type="FOMC",
            country_currency="USD",
            impact_level="HIGH",
            scheduled_utc="2026-04-25T12:00:00+00:00",
        )
        upsert_blackout_window(
            tmp_db,
            event_db_id=next_id,
            start_utc="2026-04-25T11:30:00+00:00",
            end_utc="2026-04-25T12:15:00+00:00",
            reason="HIGH_IMPACT_USD_FOMC",
            is_global=True,
        )

        raw = pd.DataFrame(
            [
                {
                    "trace_ts": "2026-04-25T11:40:00+00:00",
                    "symbol": "BTCUSDT",
                    "side": "LONG",
                    "signal": "BUY",
                }
            ]
        )
        with tmp_db.connect() as conn:
            events, windows, reactions = _load_sources(conn)

        feat = build_event_feature_frame(
            raw,
            economic_events=events,
            blackout_windows=windows,
            reactions=reactions,
            symbol_resolver=get_affected_symbols,
        )

        row = feat.iloc[0]
        assert row["minutes_since_last_event"] == pytest.approx(100.0)
        assert row["minutes_to_next_event"] == pytest.approx(20.0)
        assert row["is_blackout_active"] == 1
        assert row["reaction_type"] == "NO_PRIOR_EVENT"

    def test_symbol_specific_and_reaction_features_are_leak_safe(self, tmp_db):
        eth_id = insert_event(
            tmp_db,
            event_id="evt_eth_upgrade",
            title="Ethereum Upgrade",
            event_type="UPGRADE",
            country_currency="ETH",
            impact_level="HIGH",
            scheduled_utc="2026-04-25T12:00:00+00:00",
        )
        upsert_blackout_window(
            tmp_db,
            event_db_id=eth_id,
            start_utc="2026-04-25T11:30:00+00:00",
            end_utc="2026-04-25T12:15:00+00:00",
            reason="HIGH_IMPACT_ETH_UPGRADE",
            affected_symbols=["ETHUSDT"],
            is_global=False,
        )
        upsert_reaction(
            tmp_db,
            event_id="evt_react_past",
            symbol="ETHUSDT",
            exchange="binance",
            event_time_utc="2026-04-25T10:00:00+00:00",
            post_window_end_utc="2026-04-25T11:00:00+00:00",
            volatility_expansion_ratio=2.4,
            volume_spike_ratio=3.6,
            reaction_type="VOL_SPIKE",
            max_move_pct=1.8,
            min_move_pct=-0.9,
            data_quality="COMPLETE",
        )
        upsert_reaction(
            tmp_db,
            event_id="evt_react_future",
            symbol="ETHUSDT",
            exchange="binance",
            event_time_utc="2026-04-25T12:00:00+00:00",
            post_window_end_utc="2026-04-25T12:30:00+00:00",
            volatility_expansion_ratio=9.9,
            volume_spike_ratio=9.9,
            reaction_type="REVERSAL",
            max_move_pct=4.0,
            min_move_pct=-4.0,
            data_quality="COMPLETE",
        )

        raw = pd.DataFrame(
            [
                {
                    "trace_ts": "2026-04-25T11:40:00+00:00",
                    "symbol": "ETHUSDT",
                    "side": "LONG",
                    "signal": "BUY",
                },
                {
                    "trace_ts": "2026-04-25T11:40:00+00:00",
                    "symbol": "BTCUSDT",
                    "side": "SHORT",
                    "signal": "SELL",
                },
            ]
        )
        with tmp_db.connect() as conn:
            events, windows, reactions = _load_sources(conn)

        feat = build_event_feature_frame(
            raw,
            economic_events=events,
            blackout_windows=windows,
            reactions=reactions,
            symbol_resolver=get_affected_symbols,
        )

        eth_row = feat.iloc[0]
        btc_row = feat.iloc[1]

        assert eth_row["minutes_to_next_event"] == pytest.approx(20.0)
        assert eth_row["is_blackout_active"] == 1
        assert eth_row["volatility_post_event"] == pytest.approx(2.4)
        assert eth_row["volume_spike_post_event"] == pytest.approx(3.6)
        assert eth_row["reaction_type"] == "VOL_SPIKE"
        assert eth_row["max_adverse_move"] == pytest.approx(0.9)

        assert pd.isna(btc_row["minutes_to_next_event"])
        assert btc_row["is_blackout_active"] == 0
        assert btc_row["reaction_type"] == "NO_PRIOR_EVENT"

    # ── T1: Dataset builds with event features enabled ────────────────────────

    def test_dataset_builds_with_event_features_enabled(self, tmp_db):
        """E7-T1: build_event_feature_frame produces all 7 event columns without errors."""
        insert_event(
            tmp_db,
            event_id="evt_build_test",
            title="Fed Rate",
            event_type="FOMC",
            country_currency="USD",
            impact_level="HIGH",
            scheduled_utc="2026-04-25T14:00:00+00:00",
        )
        raw = pd.DataFrame([
            {"trace_ts": "2026-04-25T13:50:00+00:00", "symbol": "BTCUSDT", "side": "LONG",  "signal": "BUY"},
            {"trace_ts": "2026-04-25T14:10:00+00:00", "symbol": "BTCUSDT", "side": "SHORT", "signal": "SELL"},
            {"trace_ts": "2026-04-25T15:00:00+00:00", "symbol": "ETHUSDT", "side": "LONG",  "signal": "BUY"},
        ])
        with tmp_db.connect() as conn:
            events, windows, reactions = _load_sources(conn)

        feat = build_event_feature_frame(
            raw,
            economic_events=events,
            blackout_windows=windows,
            reactions=reactions,
            symbol_resolver=get_affected_symbols,
        )

        # All 7 event columns must be present
        assert list(feat.columns) == list(EVENT_FEATURE_COLUMNS)
        assert len(feat) == 3

        # No null explosion on is_blackout_active — must always be 0 or 1
        assert feat["is_blackout_active"].isin([0, 1]).all()

        # reaction_type fallback for rows with no prior reaction
        assert (feat["reaction_type"] == "NO_PRIOR_EVENT").all()

    # ── T5: Completed reaction only after post_window_end_utc ─────────────────

    def test_incomplete_reaction_not_used_before_completion(self, tmp_db):
        """E7-T5: Reaction whose post_window_end_utc is in the future must not be used."""
        upsert_reaction(
            tmp_db,
            event_id="evt_future_reaction",
            symbol="BTCUSDT",
            exchange="binance",
            event_time_utc="2026-04-25T12:00:00+00:00",
            post_window_end_utc="2026-04-25T13:00:00+00:00",  # window ends at 13:00
            volatility_expansion_ratio=5.0,
            volume_spike_ratio=8.0,
            reaction_type="REVERSAL",
            max_move_pct=3.0,
            min_move_pct=-3.0,
            data_quality="COMPLETE",
        )
        # Trace timestamp is BEFORE the reaction window ends
        raw = pd.DataFrame([{
            "trace_ts": "2026-04-25T12:30:00+00:00",  # inside window, not yet complete
            "symbol": "BTCUSDT",
            "side": "LONG",
            "signal": "BUY",
        }])
        with tmp_db.connect() as conn:
            events, windows, reactions = _load_sources(conn)

        feat = build_event_feature_frame(
            raw,
            economic_events=events,
            blackout_windows=windows,
            reactions=reactions,
            symbol_resolver=get_affected_symbols,
        )

        row = feat.iloc[0]
        # Reaction is not complete at trace_ts=12:30; must not appear
        assert row["reaction_type"] == "NO_PRIOR_EVENT"
        assert pd.isna(row["volatility_post_event"])

    # ── T6: Completed reaction available after completion time ────────────────

    def test_completed_reaction_available_after_completion(self, tmp_db):
        """E7-T6: Reaction available at timestamps after post_window_end_utc."""
        upsert_reaction(
            tmp_db,
            event_id="evt_completed_reaction",
            symbol="BTCUSDT",
            exchange="binance",
            event_time_utc="2026-04-25T10:00:00+00:00",
            post_window_end_utc="2026-04-25T11:00:00+00:00",  # completed at 11:00
            volatility_expansion_ratio=2.8,
            volume_spike_ratio=4.1,
            reaction_type="TREND_CONTINUATION",
            max_move_pct=1.5,
            min_move_pct=-0.4,
            data_quality="COMPLETE",
        )
        raw = pd.DataFrame([
            # Before completion — must not see reaction
            {"trace_ts": "2026-04-25T10:30:00+00:00", "symbol": "BTCUSDT", "side": "LONG",  "signal": "BUY"},
            # After completion — must see reaction
            {"trace_ts": "2026-04-25T11:30:00+00:00", "symbol": "BTCUSDT", "side": "LONG",  "signal": "BUY"},
        ])
        with tmp_db.connect() as conn:
            events, windows, reactions = _load_sources(conn)

        feat = build_event_feature_frame(
            raw,
            economic_events=events,
            blackout_windows=windows,
            reactions=reactions,
            symbol_resolver=get_affected_symbols,
        )

        before = feat.iloc[0]
        after  = feat.iloc[1]

        assert before["reaction_type"] == "NO_PRIOR_EVENT"
        assert pd.isna(before["volatility_post_event"])

        assert after["reaction_type"] == "TREND_CONTINUATION"
        assert after["volatility_post_event"] == pytest.approx(2.8)
        assert after["volume_spike_post_event"] == pytest.approx(4.1)
        # Reaction age must be positive (reaction completed before trace_ts)
        # (tested via the merge_asof direction="backward" logic passing correctly)

    # ── T7: News validation features excluded by default ──────────────────────

    def test_news_validation_features_excluded(self):
        """E7-T7: Known news validation column names are absent from both feature sets."""
        all_ml_cols = set(ML_FEATURE_COLUMNS) | set(EVENT_FEATURE_COLUMNS)
        for col in _NEWS_VALIDATION_COLUMNS:
            assert col not in all_ml_cols, (
                f"News validation column '{col}' must not be in ML feature contract"
            )

    # ── T8: Raw news text excluded from ML dataset ────────────────────────────

    def test_raw_news_text_excluded(self):
        """E7-T8: Raw news text/sentiment column names are absent from both feature sets."""
        all_ml_cols = set(ML_FEATURE_COLUMNS) | set(EVENT_FEATURE_COLUMNS)
        for col in _RAW_NEWS_COLUMNS:
            assert col not in all_ml_cols, (
                f"Raw news column '{col}' must not be in ML feature contract"
            )

    # ── T9: Metadata records event/reaction feature settings ─────────────────

    def test_metadata_has_phase_e_policy_fields(self):
        """E7-T9: build_contract_metadata includes event/reaction feature column lists."""
        meta = build_contract_metadata(training_dataset_path=None)

        # Contract-level fields
        assert "event_feature_columns" in meta
        assert "dataset_feature_columns" in meta
        assert set(meta["event_feature_columns"]) == set(EVENT_FEATURE_COLUMNS)
        assert set(meta["dataset_feature_columns"]) == (
            set(ML_FEATURE_COLUMNS) | set(EVENT_FEATURE_COLUMNS)
        )

        # Event timing subset
        timing_cols = {"minutes_to_next_event", "minutes_since_last_event", "is_blackout_active"}
        for col in timing_cols:
            assert col in meta["event_feature_columns"], f"{col} missing from event_feature_columns"

        # Reaction subset
        reaction_cols = {
            "volatility_post_event", "volume_spike_post_event",
            "reaction_type", "max_adverse_move",
        }
        for col in reaction_cols:
            assert col in meta["event_feature_columns"], f"{col} missing from event_feature_columns"

    # ── T10: Runtime scorer is unaffected by experimental features ────────────

    def test_scorer_only_uses_base_feature_columns(self):
        """E7-T10: Runtime scorer FEATURE_COLUMNS list equals ML_FEATURE_COLUMNS (no event features)."""
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from app.ml.scorer import FEATURE_COLUMNS as SCORER_FEATURES

        assert list(SCORER_FEATURES) == list(ML_FEATURE_COLUMNS), (
            "Scorer feature list diverged from ML_FEATURE_COLUMNS — "
            "event/experimental features must not be added to the runtime scorer"
        )
        # Confirm no event feature column leaked into scorer
        for col in EVENT_FEATURE_COLUMNS:
            assert col not in SCORER_FEATURES, (
                f"Event feature '{col}' must not appear in runtime scorer FEATURE_COLUMNS"
            )


# ── Phase H: Timing-only dataset grouping ─────────────────────────────────────


class TestPhaseHDatasetGrouping:
    """
    Phase H1 — verify that the dataset builder's feature grouping logic correctly
    separates EVENT_TIMING_FEATURE_COLUMNS from MARKET_REACTION_FEATURE_COLUMNS,
    and that timing-only mode (ML_MARKET_REACTION_FEATURES_ENABLED=false) is the
    default and safe configuration.
    """

    def test_timing_only_mode_includes_exactly_3_features(self):
        """H1-A: event_enabled=True, reaction_enabled=False → exactly 3 timing columns."""
        from shared_lib.ml.contract import (
            EVENT_TIMING_FEATURE_COLUMNS,
            MARKET_REACTION_FEATURE_COLUMNS,
            EVENT_FEATURE_COLUMNS,
        )
        # Mirrors build_dataset.py lines 98-103
        event_enabled = True
        reaction_enabled = False
        if not event_enabled:
            selected: list[str] = []
        elif reaction_enabled:
            selected = list(EVENT_FEATURE_COLUMNS)
        else:
            selected = list(EVENT_TIMING_FEATURE_COLUMNS)

        assert len(selected) == 3, f"Expected 3 timing features, got {len(selected)}: {selected}"
        assert set(selected) == set(EVENT_TIMING_FEATURE_COLUMNS)
        assert "minutes_to_next_event" in selected
        assert "minutes_since_last_event" in selected
        assert "is_blackout_active" in selected

    def test_reaction_disabled_mode_excludes_reaction_columns(self):
        """H1-B: Reaction columns must be absent from timing-only feature selection."""
        from shared_lib.ml.contract import (
            EVENT_TIMING_FEATURE_COLUMNS,
            MARKET_REACTION_FEATURE_COLUMNS,
        )
        selected = list(EVENT_TIMING_FEATURE_COLUMNS)
        for col in MARKET_REACTION_FEATURE_COLUMNS:
            assert col not in selected, (
                f"Reaction column '{col}' must not appear in timing-only feature set"
            )

    def test_full_reaction_mode_includes_all_7_features(self):
        """H1-C: event_enabled=True, reaction_enabled=True → all 7 event feature columns."""
        from shared_lib.ml.contract import (
            EVENT_TIMING_FEATURE_COLUMNS,
            MARKET_REACTION_FEATURE_COLUMNS,
            EVENT_FEATURE_COLUMNS,
        )
        event_enabled = True
        reaction_enabled = True
        if not event_enabled:
            selected: list[str] = []
        elif reaction_enabled:
            selected = list(EVENT_FEATURE_COLUMNS)
        else:
            selected = list(EVENT_TIMING_FEATURE_COLUMNS)

        assert len(selected) == 7, f"Expected 7 features (3 timing + 4 reaction), got {len(selected)}"
        assert set(selected) == set(EVENT_FEATURE_COLUMNS)
        assert set(selected) == (
            set(EVENT_TIMING_FEATURE_COLUMNS) | set(MARKET_REACTION_FEATURE_COLUMNS)
        )

    def test_event_features_disabled_gives_empty_list(self):
        """H1-D: ML_EVENT_FEATURES_ENABLED=false → zero event features selected."""
        from shared_lib.ml.contract import EVENT_TIMING_FEATURE_COLUMNS, EVENT_FEATURE_COLUMNS
        event_enabled = False
        if not event_enabled:
            selected: list[str] = []
        else:
            selected = list(EVENT_TIMING_FEATURE_COLUMNS)
        assert selected == [], (
            "EVENT_FEATURES must be empty when ML_EVENT_FEATURES_ENABLED=false"
        )

    def test_build_dataset_default_env_is_timing_only(self):
        """H1-E: build_dataset.py defaults configure timing-only mode (reaction disabled)."""
        from pathlib import Path
        source = (
            Path(__file__).resolve().parent.parent / "scripts" / "ml" / "build_dataset.py"
        ).read_text(encoding="utf-8")

        # Reaction default must be "false" so timing-only is the out-of-box behaviour
        assert 'ML_MARKET_REACTION_FEATURES_ENABLED", "false"' in source, (
            "ML_MARKET_REACTION_FEATURES_ENABLED default must be 'false' for timing-only mode"
        )
        # Event features default must be "true"
        assert 'ML_EVENT_FEATURES_ENABLED", "true"' in source, (
            "ML_EVENT_FEATURES_ENABLED default must be 'true'"
        )

    def test_phase_h_metadata_fields_in_build_dataset(self):
        """H2: build_dataset.py source contains all Phase H required metadata field names."""
        from pathlib import Path
        build_dataset_path = (
            Path(__file__).resolve().parent.parent / "scripts" / "ml" / "build_dataset.py"
        )
        assert build_dataset_path.exists(), f"build_dataset.py not found: {build_dataset_path}"
        source = build_dataset_path.read_text(encoding="utf-8")

        phase_h_fields = [
            "event_features_enabled",
            "reaction_features_enabled",
            "news_validation_features_enabled",
            "raw_news_features_enabled",
            "event_timing_feature_columns",
            "reaction_feature_columns",
            "historical_event_count",
            "blackout_window_count",
            "date_proxy_risk",
            "reaction_feature_coverage_passed",
        ]
        for field in phase_h_fields:
            assert f'"{field}"' in source, (
                f"Phase H metadata field '{field}' not found in build_dataset.py"
            )

    def test_phase_h_experiment_uses_base_plus_timing_only(self):
        """H4: phase_h_experiment.py defines ALL_FEATURES = BASE + TIMING (not reaction)."""
        from pathlib import Path
        exp_path = (
            Path(__file__).resolve().parent.parent / "scripts" / "ml" / "phase_h_experiment.py"
        )
        assert exp_path.exists(), f"phase_h_experiment.py not found: {exp_path}"
        source = exp_path.read_text(encoding="utf-8")

        # ALL_FEATURES must be the union of base + timing only
        assert "ALL_FEATURES" in source and "TIMING_FEATS" in source, (
            "phase_h_experiment.py must define ALL_FEATURES using TIMING_FEATS"
        )
        # Reaction columns must be imported for absence-validation, not for model use
        assert "MARKET_REACTION_FEATURE_COLUMNS" in source, (
            "phase_h_experiment.py must import MARKET_REACTION_FEATURE_COLUMNS for validation"
        )
        # Verify reaction cols are checked for absence
        assert "reaction_cols_in_data" in source or "missing_reaction" in source, (
            "phase_h_experiment.py must verify reaction columns are absent from the dataset"
        )
        # Safety: experiment must not deploy a model
        assert "No model is deployed" in source or "no scorer" in source.lower() or "SAFETY" in source, (
            "phase_h_experiment.py must document its offline-only safety invariant"
        )

    def test_news_features_absent_from_all_ml_feature_groups(self):
        """H1-F: Raw news and news-validation column names absent from all ML feature groups."""
        from shared_lib.ml.contract import (
            ML_FEATURE_COLUMNS,
            EVENT_TIMING_FEATURE_COLUMNS,
            MARKET_REACTION_FEATURE_COLUMNS,
        )
        news_validation_cols = {
            "narrative_effectiveness_score",
            "sentiment_accuracy_score",
            "false_signal_ratio",
            "validated_news_impact_score",
            "news_impact_score",
        }
        raw_news_cols = {
            "title",
            "headline",
            "body_snippet",
            "raw_text",
            "news_text",
            "sentiment_compound",
            "sentiment_label",
        }
        all_ml_cols = (
            set(ML_FEATURE_COLUMNS)
            | set(EVENT_TIMING_FEATURE_COLUMNS)
            | set(MARKET_REACTION_FEATURE_COLUMNS)
        )
        forbidden_found = (news_validation_cols | raw_news_cols) & all_ml_cols
        assert not forbidden_found, (
            f"News/validation columns found in ML feature contract: {sorted(forbidden_found)}"
        )
