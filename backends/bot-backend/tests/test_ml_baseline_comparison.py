from __future__ import annotations

import json

import pandas as pd

from scripts.ml.compare_entry_model_baselines import compare_baselines, write_reports
from shared_lib.ml.contract import ML_FEATURE_COLUMNS


def test_baseline_comparison_report_is_generated(tmp_path):
    rows = 80
    values = {
        feature: [float((index + offset) % 7) for index in range(rows)]
        for offset, feature in enumerate(ML_FEATURE_COLUMNS)
    }
    values["label_win"] = [index % 2 for index in range(rows)]
    values["label_r_multiple"] = [1.0 if index % 2 else -1.0 for index in range(rows)]
    values["label_exit_reason"] = ["TP1" if index % 2 else "SL" for index in range(rows)]
    values["open_timestamp"] = pd.date_range("2026-01-01", periods=rows, freq="h")
    dataset = tmp_path / "dataset.parquet"
    pd.DataFrame(values).to_parquet(dataset)

    report = compare_baselines(dataset)
    output_json = tmp_path / "comparison.json"
    output_md = tmp_path / "comparison.md"
    write_reports(report, output_json, output_md)

    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert "logistic_regression" in payload["models"]
    assert "lightgbm_fixed_baseline" in payload["models"]
    assert payload["models"]["logistic_regression"]["walk_forward_fold_count"] >= 2
    assert payload["models"]["lightgbm_fixed_baseline"]["walk_forward_fold_count"] >= 2
    assert "Entry Model Baseline Comparison" in output_md.read_text(encoding="utf-8")
