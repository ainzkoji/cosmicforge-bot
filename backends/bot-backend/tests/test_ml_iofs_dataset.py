from __future__ import annotations

import pandas as pd

from scripts.ml.build_iofs_training_dataset import (
    IOFS_FEATURE_COLUMNS,
    build_organic_iofs_dataset,
    build_replay_dataset,
    leakage_fields_in_features,
)


def test_iofs_features_are_leakage_safe():
    assert leakage_fields_in_features(IOFS_FEATURE_COLUMNS) == []


def test_replay_dataset_is_marked_and_keeps_outcomes_out_of_features():
    replay = build_replay_dataset(
        [
            {
                "signal_time": "2026-06-01T08:00:00+00:00",
                "symbol": "BTCUSDT",
                "score": 75,
                "passed": True,
                "r_multiple": 2.0,
                "outcome": "TP2",
            }
        ]
    )
    assert replay.loc[0, "data_source"] == "replay"
    assert replay.loc[0, "label_win"] == 1
    assert "label_win" not in IOFS_FEATURE_COLUMNS
    assert "label_r_multiple" not in IOFS_FEATURE_COLUMNS
    assert "label_exit_reason" not in IOFS_FEATURE_COLUMNS


def test_organic_iofs_dataset_keeps_only_trace_linked_rows_as_paper():
    organic = pd.DataFrame(
        [
            {"trace_id": "linked", "label_win": 1, "symbol": "OLD"},
            {"trace_id": "unlinked", "label_win": 0, "symbol": "UNLINKED"},
        ]
    )
    events = pd.DataFrame(
        [{"trace_id": "linked", "iofs_score": 80, "iofs_passed": True, "symbol": "BTCUSDT"}]
    )
    result = build_organic_iofs_dataset(organic, events)
    assert result["trace_id"].tolist() == ["linked"]
    assert result["data_source"].tolist() == ["paper"]
    assert result["symbol"].tolist() == ["BTCUSDT"]
