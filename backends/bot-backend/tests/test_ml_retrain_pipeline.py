from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts.ml.retrain_pipeline import retrain_entry_model_if_ready


def _readiness(organic=326, iofs=0, trades=0):
    return {
        "ready_to_retry_5a": organic >= 500 or iofs >= 300 or trades >= 20,
        "organic_rows": organic,
        "iofs_organic_rows": iofs,
        "closed_iofs_paper_trades": trades,
    }


def _paths(tmp_path: Path):
    production = tmp_path / "production"
    artifacts = tmp_path / "artifacts"
    reports = tmp_path / "reports"
    production.mkdir(parents=True)
    artifacts.mkdir(parents=True)
    (production / "README.md").write_text("marker", encoding="utf-8")
    env = tmp_path / ".env"
    env.write_text(
        "EXECUTION_MODE=paper\nML_ENABLED=False\nIOFS_GATE_MODE=shadow\n",
        encoding="utf-8",
    )
    strong = tmp_path / ".env.paper_strong_trend_experiment"
    strong.write_text("ENSEMBLE_BLOCKED_REGIMES=\n", encoding="utf-8")
    strong_status = reports / "strong.json"
    reports.mkdir(parents=True)
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


def _run(tmp_path: Path, readiness, **kwargs):
    paths = _paths(tmp_path)
    evaluator = MagicMock(return_value=readiness)
    result = retrain_entry_model_if_ready(
        **paths,
        readiness_evaluator=evaluator,
        dataset_builder=kwargs.pop("dataset_builder", MagicMock(return_value={"success": True})),
        training_runner=kwargs.pop("training_runner", MagicMock(return_value={"success": True})),
        section5a_validator=kwargs.pop(
            "section5a_validator",
            MagicMock(return_value={"accepted": False, "rejection_reasons": ["REJECTED"]}),
        ),
        promotion_validator=kwargs.pop("promotion_validator", MagicMock()),
        promoter=kwargs.pop("promoter", MagicMock()),
        **kwargs,
    )
    return result, paths


def test_dry_run_does_not_train_or_promote(tmp_path):
    training = MagicMock()
    promoter = MagicMock()
    result, _ = _run(
        tmp_path,
        _readiness(organic=500),
        dry_run=True,
        training_runner=training,
        promoter=promoter,
    )
    assert result["ready_to_retrain"] is True
    assert result["training_attempted"] is False
    assert result["promotion_attempted"] is False
    training.assert_not_called()
    promoter.assert_not_called()


def test_below_all_thresholds_blocks_retraining(tmp_path):
    result, _ = _run(tmp_path, _readiness())
    assert result["retrain_attempted"] is False
    assert result["blocking_reasons"] == [
        "ORGANIC_ROWS_BELOW_500",
        "IOFS_ROWS_BELOW_300",
        "CLOSED_IOFS_TRADES_BELOW_20",
    ]


def test_each_readiness_threshold_allows_retrain_attempt(tmp_path):
    for index, readiness in enumerate(
        [_readiness(organic=500), _readiness(iofs=300), _readiness(trades=20)]
    ):
        result, _ = _run(tmp_path / str(index), readiness)
        assert result["retrain_attempted"] is True
        assert result["dataset_build_attempted"] is True


def test_training_failure_blocks_promotion(tmp_path):
    promoter = MagicMock()
    result, _ = _run(
        tmp_path,
        _readiness(organic=500),
        training_runner=MagicMock(return_value={"success": False}),
        promoter=promoter,
    )
    assert "TRAINING_FAILED" in result["blocking_reasons"]
    assert result["promotion_attempted"] is False
    promoter.assert_not_called()


def test_validation_failure_blocks_promotion(tmp_path):
    promoter = MagicMock()
    result, _ = _run(tmp_path, _readiness(organic=500), promoter=promoter)
    assert "SECTION5A_VALIDATION_FAILED" in result["blocking_reasons"]
    assert result["promotion_attempted"] is False
    promoter.assert_not_called()


def test_promotion_uses_guard_and_does_not_override_block(tmp_path):
    promoter = MagicMock(
        return_value={
            "promotion_allowed": False,
            "promoted": False,
            "blocking_reasons": ["GUARD_BLOCKED"],
        }
    )
    validation = MagicMock(
        return_value={"accepted": True, "model_path": "candidate.pkl"}
    )
    preflight = MagicMock(
        return_value={
            "blocking_reasons": [],
            "metrics": {"auc": 0.65, "auc_std": 0.05, "quartile_win_rate_gap": 0.10},
        }
    )
    with patch(
        "scripts.ml.retrain_pipeline.discover_candidate",
        return_value={"model": Path("candidate.pkl")},
    ):
        result, _ = _run(
            tmp_path,
            _readiness(organic=500),
            section5a_validator=validation,
            promotion_validator=preflight,
            promoter=promoter,
        )
    promoter.assert_called_once()
    assert result["promotion_attempted"] is True
    assert result["promoted"] is False
    assert "GUARD_BLOCKED" in result["blocking_reasons"]


def test_promotion_preflight_failure_never_calls_guard(tmp_path):
    promoter = MagicMock()
    with patch(
        "scripts.ml.retrain_pipeline.discover_candidate",
        return_value={"model": Path("candidate.pkl")},
    ):
        result, _ = _run(
            tmp_path,
            _readiness(organic=500),
            section5a_validator=MagicMock(
                return_value={"accepted": True, "model_path": "candidate.pkl"}
            ),
            promotion_validator=MagicMock(
                return_value={"blocking_reasons": ["AUC_BELOW_0_55"], "metrics": {}}
            ),
            promoter=promoter,
        )
    assert "AUC_BELOW_0_55" in result["blocking_reasons"]
    promoter.assert_not_called()


def test_candidate_must_beat_current_production_auc_by_more_than_one_point(tmp_path):
    promoter = MagicMock()
    paths = _paths(tmp_path)
    current_model = paths["production_dir"] / "entry_quality_v2.0_20260601.pkl"
    current_model.write_bytes(b"fixture")
    current_model.with_name("entry_quality_v2.0_20260601_meta.json").write_text(
        json.dumps(
            {
                "accepted": True,
                "row_count": 500,
                "walk_forward_metrics": {
                    "auc": {"mean": 0.645, "std": 0.05},
                    "quartile_diff_win": {"mean": 0.10},
                },
            }
        ),
        encoding="utf-8",
    )
    with patch(
        "scripts.ml.retrain_pipeline.discover_candidate",
        return_value={"model": Path("candidate.pkl")},
    ):
        result = retrain_entry_model_if_ready(
            **paths,
            readiness_evaluator=MagicMock(return_value=_readiness(organic=500)),
            dataset_builder=MagicMock(return_value={"success": True}),
            training_runner=MagicMock(return_value={"success": True}),
            section5a_validator=MagicMock(
                return_value={"accepted": True, "model_path": "candidate.pkl"}
            ),
            promotion_validator=MagicMock(
                return_value={
                    "blocking_reasons": [],
                    "metrics": {
                        "auc": 0.65,
                        "auc_std": 0.05,
                        "quartile_win_rate_gap": 0.10,
                    },
                }
            ),
            promoter=promoter,
        )
    assert "CURRENT_PRODUCTION_AUC_IMPROVEMENT_INSUFFICIENT" in result["blocking_reasons"]
    promoter.assert_not_called()


def test_blocked_pipeline_preserves_env_production_and_strong_trend(tmp_path):
    result, paths = _run(tmp_path, _readiness())
    assert result["env_changed"] is False
    assert result["ml_enabled_changed"] is False
    assert result["production_files_before"] == result["production_files_after"]
    assert result["strong_trend_experiment_changed"] is False
    assert "ML_ENABLED=False" in paths["active_env"].read_text(encoding="utf-8")


def test_force_check_only_does_not_build_or_train(tmp_path):
    builder = MagicMock()
    training = MagicMock()
    result, _ = _run(
        tmp_path,
        _readiness(organic=500),
        force_check_only=True,
        dataset_builder=builder,
        training_runner=training,
    )
    assert result["ready_to_retrain"] is True
    assert result["retrain_attempted"] is False
    builder.assert_not_called()
    training.assert_not_called()


def test_pipeline_catches_exceptions_and_writes_report(tmp_path):
    result, paths = _run(
        tmp_path,
        _readiness(organic=500),
        dataset_builder=MagicMock(side_effect=RuntimeError("builder exploded")),
    )
    assert result["pipeline_error"] == "builder exploded"
    assert "PIPELINE_ERROR" in result["blocking_reasons"]
    assert paths["report_json"].exists()
    assert paths["report_md"].exists()


def test_monthly_scheduler_registration_is_safe_and_correct():
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    import app.main as main_mod

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    scheduler = MagicMock(spec=AsyncIOScheduler)
    jobs = []
    scheduler.add_job.side_effect = lambda fn, trigger=None, **kwargs: jobs.append(
        {"fn": fn, "trigger": trigger, "kwargs": kwargs}
    )
    original = main_mod._signal_scheduler
    main_mod._signal_scheduler = None
    try:
        with patch("apscheduler.schedulers.asyncio.AsyncIOScheduler", return_value=scheduler):
            loop.run_until_complete(main_mod._startup_signal_scheduler())
    finally:
        main_mod._signal_scheduler = original
        loop.close()
        asyncio.set_event_loop(None)
    job = next(job for job in jobs if job["kwargs"].get("id") == "ml_monthly_retrain")
    assert job["trigger"] == "cron"
    assert job["kwargs"]["day"] == 1
    assert job["kwargs"]["hour"] == 3
    assert job["kwargs"]["minute"] == 0
    assert job["kwargs"]["max_instances"] == 1
