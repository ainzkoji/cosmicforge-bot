from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np

from scripts.ml.promote_model import run_promotion
from shared_lib.ml.contract import (
    ML_CONTRACT_VERSION,
    ML_FEATURE_COLUMNS,
    ML_FEATURE_SCHEMA_HASH,
)


class FixtureProbabilityModel:
    def predict_proba(self, values):
        return np.array([[0.4, 0.6] for _ in range(len(values))])


def fixture_group(tmp_path: Path, **overrides):
    artifacts = tmp_path / "artifacts"
    production = tmp_path / "production"
    reports = tmp_path / "reports"
    artifacts.mkdir()
    production.mkdir()
    (production / "README.md").write_text("production marker", encoding="utf-8")
    active_env = tmp_path / ".env"
    active_env.write_text("EXECUTION_MODE=paper\nML_ENABLED=False\n", encoding="utf-8")
    base = "entry_quality_v2.0_20260615"
    model = artifacts / f"{base}.pkl"
    meta_path = artifacts / f"{base}_meta.json"
    validation_path = artifacts / f"{base}_validation.json"
    joblib.dump(FixtureProbabilityModel(), model)
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
    for key, value in overrides.items():
        if key.startswith("meta_"):
            meta[key.removeprefix("meta_")] = value
        elif key.startswith("validation_"):
            validation[key.removeprefix("validation_")] = value
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    validation_path.write_text(json.dumps(validation), encoding="utf-8")
    return {
        "artifacts": artifacts,
        "production": production,
        "reports": reports,
        "active_env": active_env,
        "model": model,
        "meta": meta_path,
        "validation": validation_path,
    }


def run(paths, *, dry_run=True, model_version="v2.0"):
    return run_promotion(
        model_version=model_version,
        artifacts_dir=paths["artifacts"],
        production_dir=paths["production"],
        dry_run=dry_run,
        report_md=paths["reports"] / "guard.md",
        report_json=paths["reports"] / "guard.json",
        shadow_suggestion=paths["reports"] / "shadow.env",
        active_env=paths["active_env"],
    )


def set_metric(paths, key, value):
    validation = json.loads(paths["validation"].read_text(encoding="utf-8"))
    aggregate = validation["lgbm_binary_aggregate"]
    if key == "auc":
        aggregate["auc"]["mean"] = value
    elif key == "auc_std":
        aggregate["auc"]["std"] = value
    elif key == "gap":
        aggregate["quartile_diff_win"]["mean"] = value
    paths["validation"].write_text(json.dumps(validation), encoding="utf-8")


def test_blocks_when_no_pkl_exists(tmp_path):
    paths = fixture_group(tmp_path)
    paths["model"].unlink()
    assert "MODEL_PKL_MISSING" in run(paths)["blocking_reasons"]


def test_blocks_when_meta_missing(tmp_path):
    paths = fixture_group(tmp_path)
    paths["meta"].unlink()
    assert "META_JSON_MISSING" in run(paths)["blocking_reasons"]


def test_blocks_when_validation_rejected(tmp_path):
    paths = fixture_group(tmp_path, validation_accepted=False)
    assert "VALIDATION_NOT_ACCEPTED" in run(paths)["blocking_reasons"]


def test_blocks_auc_below_floor(tmp_path):
    paths = fixture_group(tmp_path)
    set_metric(paths, "auc", 0.54)
    assert "AUC_BELOW_0_55" in run(paths)["blocking_reasons"]


def test_blocks_auc_above_hard_ceiling(tmp_path):
    paths = fixture_group(tmp_path)
    set_metric(paths, "auc", 0.91)
    assert "AUC_ABOVE_0_90" in run(paths)["blocking_reasons"]


def test_blocks_auc_above_promotion_range(tmp_path):
    paths = fixture_group(tmp_path)
    set_metric(paths, "auc", 0.73)
    assert "AUC_ABOVE_0_72_PROMOTION_RANGE" in run(paths)["blocking_reasons"]


def test_blocks_unstable_auc(tmp_path):
    paths = fixture_group(tmp_path)
    set_metric(paths, "auc_std", 0.16)
    assert "AUC_STD_ABOVE_0_15_OR_MISSING" in run(paths)["blocking_reasons"]


def test_blocks_small_quartile_gap(tmp_path):
    paths = fixture_group(tmp_path)
    set_metric(paths, "gap", 0.04)
    assert "QUARTILE_GAP_BELOW_5PP_OR_MISSING" in run(paths)["blocking_reasons"]


def test_blocks_when_lightgbm_does_not_beat_logistic(tmp_path):
    paths = fixture_group(tmp_path, validation_logistic_baseline_auc=0.645)
    assert "LIGHTGBM_BASELINE_IMPROVEMENT_INSUFFICIENT" in run(paths)["blocking_reasons"]


def test_blocks_wrong_contract(tmp_path):
    paths = fixture_group(tmp_path, meta_contract_version="legacy_entry_quality_v1")
    assert "FEATURE_CONTRACT_VERSION_MISMATCH" in run(paths)["blocking_reasons"]


def test_blocks_schema_mismatch(tmp_path):
    paths = fixture_group(tmp_path, meta_schema_hash="wrong")
    assert "SCHEMA_HASH_MISMATCH" in run(paths)["blocking_reasons"]


def test_blocks_legacy_v1_artifacts(tmp_path):
    paths = fixture_group(tmp_path)
    assert "LEGACY_V1_ARTIFACT" in run(paths, model_version="v1.0")["blocking_reasons"]


def test_dry_run_copies_nothing_and_does_not_write_shadow_suggestion(tmp_path):
    paths = fixture_group(tmp_path)
    before = sorted(item.name for item in paths["production"].iterdir())
    report = run(paths)
    assert report["promotion_allowed"] is True
    assert report["promoted"] is False
    assert sorted(item.name for item in paths["production"].iterdir()) == before
    assert not (paths["reports"] / "shadow.env").exists()


def test_normal_mode_copies_only_when_all_gates_pass(tmp_path):
    paths = fixture_group(tmp_path)
    report = run(paths, dry_run=False)
    assert report["promotion_allowed"] is True
    assert report["promoted"] is True
    assert (paths["production"] / paths["model"].name).exists()
    assert (paths["production"] / paths["meta"].name).exists()
    assert (paths["reports"] / "shadow.env").exists()


def test_active_env_remains_unchanged(tmp_path):
    paths = fixture_group(tmp_path)
    before = paths["active_env"].read_bytes()
    report = run(paths)
    assert report["active_env_changed"] is False
    assert paths["active_env"].read_bytes() == before


def test_production_unchanged_when_blocked(tmp_path):
    paths = fixture_group(tmp_path, validation_accepted=False)
    before = sorted(item.name for item in paths["production"].iterdir())
    report = run(paths, dry_run=False)
    assert report["promoted"] is False
    assert sorted(item.name for item in paths["production"].iterdir()) == before


def test_shadow_suggestion_only_written_on_success(tmp_path):
    paths = fixture_group(tmp_path, validation_accepted=False)
    run(paths, dry_run=False)
    assert not (paths["reports"] / "shadow.env").exists()
