from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression

from scripts.ml.watch_training_readiness import evaluate_training_readiness, run_watcher
from shared_lib.ml.contract import (
    ML_CONTRACT_VERSION,
    ML_FEATURE_COLUMNS,
    ML_FEATURE_SCHEMA_HASH,
)


def _organic_dataset(tmp_path: Path, rows: int) -> Path:
    path = tmp_path / "training_v2_organic.parquet"
    values = {feature: [float(index % 3) for index in range(rows)] for feature in ML_FEATURE_COLUMNS}
    values["label_win"] = [index % 2 for index in range(rows)]
    pd.DataFrame(values).to_parquet(path)
    path.with_name(f"{path.stem}_meta.json").write_text(
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
    return path


def _iofs_dataset(tmp_path: Path, rows: int) -> Path:
    path = tmp_path / "training_v2_iofs_organic.parquet"
    pd.DataFrame({"data_source": ["paper"] * rows}).to_parquet(path)
    return path


def _status_files(tmp_path: Path, closed_trades: int = 0) -> tuple[Path, Path, Path]:
    status = tmp_path / "iofs_paper_validation_status.md"
    status.write_text(
        f"- number_of_closed_paper_trades: {closed_trades}\n- status: In Progress\n",
        encoding="utf-8",
    )
    reviews = tmp_path / "iofs_paper_trade_reviews.md"
    reviews.write_text("# Reviews\n", encoding="utf-8")
    blocker = tmp_path / "section5b_blocker_status.md"
    blocker.write_text("- Section 5B status: BLOCKED\n", encoding="utf-8")
    return status, reviews, blocker


def _active_env(tmp_path: Path) -> Path:
    path = tmp_path / ".env"
    path.write_text(
        "EXECUTION_MODE=paper\nML_ENABLED=False\nIOFS_GATE_MODE=shadow\n",
        encoding="utf-8",
    )
    return path


def _evaluate(tmp_path: Path, *, organic_rows: int, iofs_rows: int, closed_trades: int = 0):
    status, reviews, blocker = _status_files(tmp_path, closed_trades)
    return evaluate_training_readiness(
        organic_dataset=_organic_dataset(tmp_path, organic_rows),
        iofs_organic_dataset=_iofs_dataset(tmp_path, iofs_rows),
        paper_status_path=status,
        paper_reviews_path=reviews,
        section5b_status_path=blocker,
        artifacts_dir=tmp_path / "artifacts",
        active_env_path=_active_env(tmp_path),
    )


def test_watcher_blocks_retry_below_all_thresholds(tmp_path):
    result = _evaluate(tmp_path, organic_rows=326, iofs_rows=0)
    assert result["ready_to_retry_5a"] is False
    assert result["ready_for_5b"] is False
    assert result["next_action"] == "continue_paper_validation"
    assert result["paper_trade_review_entries"] == 0


def test_watcher_allows_retry_at_500_organic_rows(tmp_path):
    result = _evaluate(tmp_path, organic_rows=500, iofs_rows=0)
    assert result["ready_to_retry_5a"] is True
    assert result["next_action"] == "retry_section_5a_manually"


def test_watcher_allows_retry_at_300_iofs_organic_rows(tmp_path):
    result = _evaluate(tmp_path, organic_rows=326, iofs_rows=300)
    assert result["ready_to_retry_5a"] is True


def test_watcher_allows_retry_at_closed_trade_threshold(tmp_path):
    result = _evaluate(tmp_path, organic_rows=326, iofs_rows=0, closed_trades=20)
    assert result["ready_to_retry_5a"] is True


def test_watcher_writes_json_and_markdown_reports(tmp_path):
    status, reviews, blocker = _status_files(tmp_path)
    output_json = tmp_path / "status.json"
    output_md = tmp_path / "status.md"
    result = run_watcher(
        organic_dataset=_organic_dataset(tmp_path, 326),
        iofs_organic_dataset=_iofs_dataset(tmp_path, 0),
        paper_status_path=status,
        paper_reviews_path=reviews,
        section5b_status_path=blocker,
        artifacts_dir=tmp_path / "artifacts",
        active_env_path=_active_env(tmp_path),
        output_json=output_json,
        output_md=output_md,
    )
    assert json.loads(output_json.read_text(encoding="utf-8"))["ready_to_retry_5a"] is False
    assert "Section 5A retry status: Not ready" in output_md.read_text(encoding="utf-8")
    assert result["auto_training_enabled"] is False


def test_watcher_marks_5b_ready_only_for_strong_loadable_artifact(tmp_path):
    organic = _organic_dataset(tmp_path, 500)
    iofs = _iofs_dataset(tmp_path, 0)
    status, reviews, blocker = _status_files(tmp_path)
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    base = artifacts / "entry_quality_v2.0_20260614"
    model = LogisticRegression().fit([[0.0] * 21, [1.0] * 21], [0, 1])
    joblib.dump(model, artifacts / f"{base.name}.pkl")
    meta = {
        "accepted": True,
        "contract_version": ML_CONTRACT_VERSION,
        "schema_hash": ML_FEATURE_SCHEMA_HASH,
        "feature_columns": list(ML_FEATURE_COLUMNS),
        "row_count": 500,
        "logistic_baseline_auc": 0.60,
        "minimum_baseline_auc_improvement": 0.01,
        "walk_forward_metrics": {
            "auc": {"mean": 0.65, "std": 0.05},
            "quartile_diff_win": {"mean": 0.10},
        },
    }
    validation = {
        "accepted": True,
        "row_count": 500,
        "logistic_baseline_auc": 0.60,
        "minimum_baseline_auc_improvement": 0.01,
        "lgbm_binary_aggregate": {
            "auc": {"mean": 0.65, "std": 0.05},
            "quartile_diff_win": {"mean": 0.10},
        },
    }
    (artifacts / f"{base.name}_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    (artifacts / f"{base.name}_validation.json").write_text(
        json.dumps(validation), encoding="utf-8"
    )
    result = evaluate_training_readiness(
        organic_dataset=organic,
        iofs_organic_dataset=iofs,
        paper_status_path=status,
        paper_reviews_path=reviews,
        section5b_status_path=blocker,
        artifacts_dir=artifacts,
        active_env_path=_active_env(tmp_path),
    )
    assert result["ready_for_5b"] is True
    assert result["promotion_guard_allows_5b"] is True


def test_retry_watcher_config_defaults_are_safe():
    from app.core.config import settings

    assert settings.ML_RETRY_WATCHER_ENABLED is True
    assert settings.ML_RETRY_MIN_ORGANIC_ROWS == 500
    assert settings.ML_RETRY_MIN_IOFS_ORGANIC_ROWS == 300
    assert settings.ML_RETRY_MIN_CLOSED_IOFS_TRADES == 20
    assert settings.ML_ENABLED is False
