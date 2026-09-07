from __future__ import annotations

import json
from pathlib import Path

import joblib

from app.ml.scorer import ACTION_SKIP, MLEntryScorer
from shared_lib.ml.contract import ML_FEATURE_COLUMNS, ML_FEATURE_SCHEMA_HASH


def _disable_runtime_status_write(monkeypatch) -> None:
    monkeypatch.setattr("app.ml.scorer.upsert_ml_runtime_status", lambda *args, **kwargs: None)


def test_ml_disabled_means_no_scoring_and_no_blocks(monkeypatch):
    _disable_runtime_status_write(monkeypatch)

    scorer = MLEntryScorer(enabled=False)

    assert scorer.loaded is False
    assert scorer.score({}) is None
    assert scorer.get_action(None) == ACTION_SKIP
    assert scorer.should_block(None) is False


def test_experimental_artifacts_cannot_be_loaded_by_runtime_scorer(tmp_path, monkeypatch):
    _disable_runtime_status_write(monkeypatch)
    model_path = tmp_path / "models" / "experiments" / "candidate.pkl"
    model_path.parent.mkdir(parents=True)
    model_path.write_bytes(b"not a runtime model")

    scorer = MLEntryScorer(model_path=str(model_path), enabled=True)

    assert scorer.loaded is False
    assert "not in an approved runtime directory" in (scorer.status()["load_error"] or "")


def test_legacy_33_feature_artifact_cannot_load_under_21_feature_contract(tmp_path, monkeypatch):
    _disable_runtime_status_write(monkeypatch)
    production_dir = tmp_path / "models" / "production"
    production_dir.mkdir(parents=True)
    model_path = production_dir / "legacy_33.pkl"
    metadata_path = production_dir / "legacy_33_meta.json"
    joblib.dump({"dummy": "model"}, model_path)
    metadata_path.write_text(
        json.dumps(
            {
                "contract_version": "entry_quality_v2",
                "schema_hash": ML_FEATURE_SCHEMA_HASH,
                "feature_columns": [f"legacy_feature_{i}" for i in range(33)],
            }
        ),
        encoding="utf-8",
    )

    scorer = MLEntryScorer(
        model_path=str(model_path),
        metadata_path=str(metadata_path),
        enabled=True,
    )

    assert len(ML_FEATURE_COLUMNS) == 21
    assert scorer.loaded is False
    assert "feature_columns do not match" in (scorer.status()["load_error"] or "")


# ---------------------------------------------------------------------------
# FIX-B: Legacy v1.x retirement contract tests
# ---------------------------------------------------------------------------

# Exact metadata values from the two retired artifacts on disk
_V1_0_SCHEMA_HASH = "173cf9e75d0ce3cf4ead7db00ed171995b06ab8d69f82955c7134c7f170f6b16"
_V1_1_SCHEMA_HASH = "c8ffb1285f7e65ccfa6a9926e8cde69146ceb899cc56bd5d8f8908d1f88c3d94"
_LEGACY_CONTRACT  = "legacy_entry_quality_v1"


def test_runtime_expects_entry_quality_v2_contract():
    """Runtime contract version constant must be entry_quality_v2."""
    from shared_lib.ml.contract import ML_CONTRACT_VERSION
    assert ML_CONTRACT_VERSION == "entry_quality_v2", (
        f"Runtime contract is '{ML_CONTRACT_VERSION}', expected 'entry_quality_v2'."
    )


def test_runtime_schema_hash_starts_with_d4b19440():
    """Runtime schema hash must start with d4b19440 (v2 contract)."""
    assert ML_FEATURE_SCHEMA_HASH.startswith("d4b19440"), (
        f"Runtime schema_hash is '{ML_FEATURE_SCHEMA_HASH}', expected d4b19440..."
    )


def test_ml_enabled_is_false_by_default():
    """ML_ENABLED must default to False — safe before any v2 model is in production."""
    import importlib
    config = importlib.import_module("app.core.config")
    # Inspect the field default without instantiating (avoids env-var side effects)
    field = config.Settings.model_fields.get("ML_ENABLED")
    if field is not None:
        # Pydantic v2 path
        default = field.default
    else:
        # Fallback: instantiate with no env vars present
        import os
        env_backup = os.environ.pop("ML_ENABLED", None)
        try:
            s = config.Settings()
            default = s.ML_ENABLED
        finally:
            if env_backup is not None:
                os.environ["ML_ENABLED"] = env_backup
    assert default is False, (
        f"ML_ENABLED default is {default!r}, must be False to prevent premature ML activation."
    )


def test_active_env_keeps_ml_disabled():
    active_env = Path(__file__).resolve().parents[1] / ".env"
    settings = {}
    for raw_line in active_env.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            settings[key.strip()] = value.strip()
    assert settings.get("ML_ENABLED", "").lower() == "false"


def test_active_env_keeps_paper_execution_and_iofs_shadow_mode():
    active_env = Path(__file__).resolve().parents[1] / ".env"
    settings = {}
    for raw_line in active_env.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            settings[key.strip()] = value.strip()
    assert settings.get("EXECUTION_MODE", "").lower() == "paper"
    assert settings.get("IOFS_GATE_MODE", "").lower() == "shadow"


def test_no_model_exists_in_production_directory():
    production = Path(__file__).resolve().parents[1] / "models" / "production"
    assert not list(production.glob("*.pkl"))


def test_legacy_v1_schema_hash_is_rejected_by_runtime(tmp_path, monkeypatch):
    """
    A model artifact presenting the v1.1 schema hash (c8ffb128...) must be
    rejected by the v2 runtime even if feature_columns happen to be correct.
    This is the exact scenario of the retired entry_quality_v1.1_20260322 artifact.
    """
    _disable_runtime_status_write(monkeypatch)
    production_dir = tmp_path / "models" / "production"
    production_dir.mkdir(parents=True)
    model_path    = production_dir / "legacy_v1_1.pkl"
    metadata_path = production_dir / "legacy_v1_1_meta.json"
    joblib.dump({"dummy": "model"}, model_path)
    # Simulate v1.1 metadata: right feature list but wrong schema hash + wrong contract
    metadata_path.write_text(
        json.dumps({
            "contract_version": _LEGACY_CONTRACT,
            "schema_hash":      _V1_1_SCHEMA_HASH,
            "feature_columns":  list(ML_FEATURE_COLUMNS),  # 21 correct v2 names
        }),
        encoding="utf-8",
    )

    scorer = MLEntryScorer(
        model_path=str(model_path),
        metadata_path=str(metadata_path),
        enabled=True,
    )

    assert scorer.loaded is False, (
        "Legacy v1.1 artifact (schema_hash=c8ffb128...) must not load under v2 runtime."
    )
    load_error = scorer.status()["load_error"] or ""
    assert (
        "schema_hash" in load_error or "contract_version" in load_error
    ), f"Expected schema_hash or contract_version rejection; got: {load_error!r}"


def test_legacy_v1_0_schema_hash_is_rejected_by_runtime(tmp_path, monkeypatch):
    """
    A model artifact presenting the v1.0 schema hash (173cf9e7...) must be
    rejected.  Mirrors entry_quality_v1.0_20260322 artifact.
    """
    _disable_runtime_status_write(monkeypatch)
    production_dir = tmp_path / "models" / "production"
    production_dir.mkdir(parents=True)
    model_path    = production_dir / "legacy_v1_0.pkl"
    metadata_path = production_dir / "legacy_v1_0_meta.json"
    joblib.dump({"dummy": "model"}, model_path)
    metadata_path.write_text(
        json.dumps({
            "contract_version": _LEGACY_CONTRACT,
            "schema_hash":      _V1_0_SCHEMA_HASH,
            "feature_columns":  list(ML_FEATURE_COLUMNS),
        }),
        encoding="utf-8",
    )

    scorer = MLEntryScorer(
        model_path=str(model_path),
        metadata_path=str(metadata_path),
        enabled=True,
    )

    assert scorer.loaded is False, (
        "Legacy v1.0 artifact (schema_hash=173cf9e7...) must not load under v2 runtime."
    )


def test_artifacts_readme_marks_v1x_as_retired():
    """
    models/artifacts/README.md must exist and mention v1.0 and v1.1 as retired,
    along with the legacy schema hashes and the prohibition on production use.
    """
    readme = Path(__file__).resolve().parents[1] / "models" / "artifacts" / "README.md"
    assert readme.exists(), "models/artifacts/README.md must exist."
    text = readme.read_text(encoding="utf-8")
    for marker in (
        "entry_quality_v1.0_20260322",
        "entry_quality_v1.1_20260322",
        "legacy_entry_quality_v1",
        "c8ffb128",     # v1.1 schema hash prefix
        "173cf9e7",     # v1.0 schema hash prefix
        "d4b19440",     # v2 schema hash prefix
        "models/production",
        "ML_ENABLED",
    ):
        assert marker in text, (
            f"README.md missing required marker: {marker!r}. "
            "The retirement notice is incomplete."
        )


def test_v2_training_metadata_structure():
    """
    If a trained v2 model metadata file exists in models/artifacts, it must
    declare entry_quality_v2 contract, d4b19440 schema hash, and 21 features.
    This is a structural check — it does not require the file to exist yet.
    If found, it must be valid.
    """
    artifacts_dir = Path(__file__).resolve().parents[1] / "models" / "artifacts"
    v2_metas = [
        p for p in artifacts_dir.glob("entry_quality_v2*_meta.json")
        if p.is_file()
    ]
    for meta_path in v2_metas:
        data = json.loads(meta_path.read_text(encoding="utf-8"))
        assert data.get("feature_contract_version") == "entry_quality_v2" or \
               data.get("contract_version") == "entry_quality_v2", (
            f"{meta_path.name}: feature_contract_version must be 'entry_quality_v2'"
        )
        sha = data.get("schema_hash", "")
        assert sha.startswith("d4b19440"), (
            f"{meta_path.name}: schema_hash must start with 'd4b19440', got '{sha[:16]}'"
        )
        assert data.get("feature_count") == 21, (
            f"{meta_path.name}: feature_count must be 21, got {data.get('feature_count')}"
        )


def test_experiment_metadata_is_marked_offline_only():
    root = Path(__file__).resolve().parents[1]
    experiment_files = [
        root / "models" / "experiments" / "event_features_experimental_20260426" / "phase_f_results_20260426.json",
        root / "models" / "experiments" / "phase_h_20260427" / "phase_h_results_20260427.json",
    ]

    for path in experiment_files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["deployed"] is False
        assert payload["allowed_runtime"] is False
        assert payload["feature_contract_version"]
        assert payload["schema_hash"]
        assert payload["feature_count"] > len(ML_FEATURE_COLUMNS)
