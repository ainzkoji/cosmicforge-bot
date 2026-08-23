from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd

from scripts.ml.check_readiness import check_readiness
from shared_lib.ml.contract import (
    ML_CONTRACT_VERSION,
    ML_FEATURE_COLUMNS,
    ML_FEATURE_SCHEMA_HASH,
)


def _fixture(tmp_path: Path, rows: int) -> tuple[Path, Path]:
    dataset = tmp_path / "training_v2_organic.parquet"
    metadata = tmp_path / "training_v2_organic_meta.json"
    values = {feature: [1.0] * rows for feature in ML_FEATURE_COLUMNS}
    values["label_win"] = [index % 2 for index in range(rows)]
    pd.DataFrame(values).to_parquet(dataset)
    metadata.write_text(
        json.dumps(
            {
                "contract_version": ML_CONTRACT_VERSION,
                "schema_hash": ML_FEATURE_SCHEMA_HASH,
                "feature_columns": list(ML_FEATURE_COLUMNS),
                "row_count": rows,
                "leakage_check_passed": True,
                "strict_filters": {
                    "only_organic": True,
                    "require_trace_id": True,
                    "exclude_incomplete_labels": True,
                    "post_repair_only": "2026-04-01T00:00:00+00:00",
                },
            }
        ),
        encoding="utf-8",
    )
    return dataset, metadata


def _section4(tmp_path: Path, status: str = "In Progress") -> Path:
    path = tmp_path / "section4.md"
    path.write_text(f"- status: {status}\n", encoding="utf-8")
    return path


def test_readiness_allows_experimental_training_at_300_rows(tmp_path):
    dataset, metadata = _fixture(tmp_path, 300)
    result = check_readiness(
        dataset, metadata_path=metadata, section4_status_path=_section4(tmp_path),
        artifacts_dir=tmp_path / "artifacts",
    )
    assert result["ready_for_experimental_training"] is True


def test_readiness_warns_below_500_rows(tmp_path):
    dataset, metadata = _fixture(tmp_path, 499)
    result = check_readiness(
        dataset, metadata_path=metadata, section4_status_path=_section4(tmp_path),
        artifacts_dir=tmp_path / "artifacts",
    )
    assert any("LOW_SAMPLE_WARNING" in warning for warning in result["warnings"])


def test_readiness_blocks_below_300_rows(tmp_path):
    dataset, metadata = _fixture(tmp_path, 299)
    result = check_readiness(
        dataset, metadata_path=metadata, section4_status_path=_section4(tmp_path),
        artifacts_dir=tmp_path / "artifacts",
    )
    assert result["ready_for_experimental_training"] is False


def test_readiness_blocks_production_while_section4_is_in_progress(tmp_path):
    dataset, metadata = _fixture(tmp_path, 500)
    result = check_readiness(
        dataset, metadata_path=metadata, section4_status_path=_section4(tmp_path),
        artifacts_dir=tmp_path / "artifacts",
    )
    assert result["ready_for_production_promotion"] is False
    assert result["deployment_status"] == "EXPERIMENTAL_ONLY"


def test_readiness_blocks_5b_without_accepted_model_artifact(tmp_path):
    dataset, metadata = _fixture(tmp_path, 500)
    result = check_readiness(
        dataset,
        metadata_path=metadata,
        section4_status_path=_section4(tmp_path, "Passed"),
        artifacts_dir=tmp_path / "artifacts",
    )
    assert result["ready_for_5b_shadow_deployment"] is False
    assert any("SECTION_5B_BLOCKED" in warning for warning in result["warnings"])


def test_readiness_does_not_count_replay_rows_as_production_organic(tmp_path):
    dataset, metadata = _fixture(tmp_path, 500)
    frame = pd.read_parquet(dataset)
    frame["data_source"] = "replay"
    frame.to_parquet(dataset)
    result = check_readiness(
        dataset,
        metadata_path=metadata,
        section4_status_path=_section4(tmp_path, "Passed"),
        artifacts_dir=tmp_path / "artifacts",
    )
    assert result["replay_row_count"] == 500
    assert result["organic_paper_row_count"] == 0
    assert result["ready_for_production_training"] is False


def test_section5b_blocker_report_is_blocked_without_artifact(tmp_path):
    from scripts.ml.check_readiness import render_section5b_blocker_report

    dataset, metadata = _fixture(tmp_path, 500)
    result = check_readiness(
        dataset,
        metadata_path=metadata,
        section4_status_path=_section4(tmp_path, "Passed"),
        artifacts_dir=tmp_path / "artifacts",
    )
    report = render_section5b_blocker_report(result)
    assert "Section 5B status: BLOCKED" in report
    assert "accepted_model_artifact_exists: false" in report


def test_readiness_blocks_5b_for_unloadable_artifact_claimed_as_accepted(tmp_path):
    dataset, metadata = _fixture(tmp_path, 500)
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    base = artifacts / "entry_quality_v2.0_20260614"
    joblib.dump({"not": "a scorer"}, artifacts / f"{base.name}.pkl")
    (artifacts / f"{base.name}_meta.json").write_text(
        json.dumps(
            {
                "accepted": True,
                "contract_version": ML_CONTRACT_VERSION,
                "schema_hash": ML_FEATURE_SCHEMA_HASH,
                "feature_columns": list(ML_FEATURE_COLUMNS),
                "row_count": 500,
                "logistic_baseline_auc": 0.60,
                "walk_forward_metrics": {
                    "auc": {"mean": 0.65, "std": 0.05},
                    "quartile_diff_win": {"mean": 0.10},
                },
            }
        ),
        encoding="utf-8",
    )
    (artifacts / f"{base.name}_validation.json").write_text(
        json.dumps(
            {
                "accepted": True,
                "row_count": 500,
                "logistic_baseline_auc": 0.60,
                "lgbm_binary_aggregate": {
                    "auc": {"mean": 0.65, "std": 0.05},
                    "quartile_diff_win": {"mean": 0.10},
                },
            }
        ),
        encoding="utf-8",
    )
    result = check_readiness(
        dataset,
        metadata_path=metadata,
        section4_status_path=_section4(tmp_path, "Passed"),
        artifacts_dir=artifacts,
    )
    assert result["ready_for_5b_shadow_deployment"] is False
