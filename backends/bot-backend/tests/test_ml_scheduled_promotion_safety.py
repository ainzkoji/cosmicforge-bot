"""The scheduled retrain can train and validate, but never promote on its own authority.

AI decision authority stays disabled. The monthly job used to call the
promotion guard with ``dry_run=False``, so a candidate that passed validation
was copied into ``models/production`` with no human in the loop. Promotion is
now a dry run unless an operator passes ``operator_approved_promotion=True``.
"""
from __future__ import annotations

import inspect
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts.ml import retrain_pipeline
from scripts.ml.retrain_pipeline import retrain_entry_model_if_ready


def _paths(tmp_path: Path) -> dict:
    production = tmp_path / "production"
    artifacts = tmp_path / "artifacts"
    reports = tmp_path / "reports"
    for folder in (production, artifacts, reports):
        folder.mkdir(parents=True)
    (production / "README.md").write_text("marker", encoding="utf-8")
    env = tmp_path / ".env"
    env.write_text("EXECUTION_MODE=paper\nML_ENABLED=False\n", encoding="utf-8")
    strong = tmp_path / ".env.paper_strong_trend_experiment"
    strong.write_text("ENSEMBLE_BLOCKED_REGIMES=\n", encoding="utf-8")
    strong_status = reports / "strong.json"
    strong_status.write_text(json.dumps({"scope": "paper_only"}), encoding="utf-8")
    return {
        "dataset_path": tmp_path / "dataset.parquet",
        "active_env": env,
        "artifacts_dir": artifacts,
        "production_dir": production,
        "strong_trend_config": strong,
        "strong_trend_status": strong_status,
        "report_json": reports / "monthly.json",
        "report_md": reports / "monthly.md",
    }


def _run_to_promotion(tmp_path: Path, promoter: MagicMock, **kwargs):
    paths = _paths(tmp_path)
    readiness = {
        "ready_to_retry_5a": True, "organic_rows": 600,
        "iofs_organic_rows": 0, "closed_iofs_paper_trades": 0,
    }
    with patch.object(retrain_pipeline, "discover_candidate",
                      return_value={"model": Path("candidate.pkl")}), \
         patch.object(retrain_pipeline, "_write_manual_activation_report") as activation:
        result = retrain_entry_model_if_ready(
            **paths,
            readiness_evaluator=MagicMock(return_value=readiness),
            dataset_builder=MagicMock(return_value={"success": True}),
            training_runner=MagicMock(return_value={"success": True}),
            section5a_validator=MagicMock(return_value={"accepted": True}),
            promotion_validator=MagicMock(return_value={
                "blocking_reasons": [], "metrics": {"auc": 0.70},
            }),
            promoter=promoter,
            **kwargs,
        )
    return result, paths, activation


def _allowing_promoter() -> MagicMock:
    # A guard that would happily promote -- the pipeline must still not.
    return MagicMock(return_value={
        "promotion_allowed": True, "promoted": True, "blocking_reasons": [],
    })


def test_a_scheduled_run_only_ever_dry_runs_the_promotion(tmp_path):
    promoter = _allowing_promoter()
    result, paths, activation = _run_to_promotion(tmp_path, promoter)

    promoter.assert_called_once()
    assert promoter.call_args.kwargs["dry_run"] is True
    assert result["promotion_mode"] == "DRY_RUN_AWAITING_OPERATOR_APPROVAL"
    assert result["promoted"] is False
    assert result["section5b_status"] == "AWAITING_OPERATOR_APPROVAL"
    activation.assert_not_called()
    assert (paths["production_dir"] / "README.md").read_text(encoding="utf-8") == "marker"
    assert paths["active_env"].read_text(encoding="utf-8") == (
        "EXECUTION_MODE=paper\nML_ENABLED=False\n"
    )


def test_promotion_requires_an_explicit_operator_approval(tmp_path):
    promoter = _allowing_promoter()
    result, _, _ = _run_to_promotion(tmp_path, promoter, operator_approved_promotion=True)

    assert promoter.call_args.kwargs["dry_run"] is False
    assert result["promotion_mode"] == "OPERATOR_APPROVED"
    assert result["promoted"] is True


def test_the_default_is_no_promotion_authority():
    parameter = inspect.signature(retrain_entry_model_if_ready).parameters[
        "operator_approved_promotion"
    ]
    assert parameter.default is False


def test_the_scheduler_never_grants_promotion_authority():
    from app import main as main_module

    source = inspect.getsource(main_module._startup_signal_scheduler)
    assert "retrain_entry_model_if_ready" in source
    assert "operator_approved_promotion" not in source


def test_the_pipeline_never_writes_env_or_enables_ml():
    source = inspect.getsource(retrain_pipeline)
    assert "ML_ENABLED=True" not in source
    assert ".write_text(" not in inspect.getsource(retrain_entry_model_if_ready)
