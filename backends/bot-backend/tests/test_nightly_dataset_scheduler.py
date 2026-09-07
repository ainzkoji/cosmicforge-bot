from __future__ import annotations

import asyncio
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.jobs.nightly_dataset_builder import run_nightly_organic_dataset_build


def test_nightly_job_calls_strict_organic_builder_and_logs_row_count(tmp_path, caplog):
    import logging

    caplog.set_level(logging.INFO, logger="app.jobs.nightly_dataset_builder")
    output = tmp_path / "training_v2_organic.parquet"
    metadata = tmp_path / "training_v2_organic_meta.json"
    metadata.write_text(json.dumps({"row_count": 314}), encoding="utf-8")
    completed = SimpleNamespace(returncode=0, stdout="ok", stderr="")

    with patch("app.jobs.nightly_dataset_builder.subprocess.run", return_value=completed) as run:
        result = run_nightly_organic_dataset_build(
            db_path=tmp_path / "cosmicforge.db",
            output_path=output,
            script_path=tmp_path / "build_dataset.py",
            watcher_enabled=False,
        )

    command = run.call_args.args[0]
    assert "--only-organic" in command
    assert "--require-trace-id" in command
    assert "--exclude-incomplete-labels" in command
    assert "--post-repair-only" in command
    assert result["success"] is True
    assert result["row_count"] == 314
    assert "row_count=314" in caplog.text


def test_nightly_job_catches_builder_failure(tmp_path):
    completed = SimpleNamespace(returncode=1, stdout="", stderr="build failed")
    with patch("app.jobs.nightly_dataset_builder.subprocess.run", return_value=completed):
        result = run_nightly_organic_dataset_build(
            db_path=tmp_path / "cosmicforge.db",
            output_path=tmp_path / "training_v2_organic.parquet",
            script_path=tmp_path / "build_dataset.py",
            watcher_enabled=False,
        )
    assert result["success"] is False
    assert "build failed" in result["error"]
    assert result["finished_at"]


def test_nightly_job_accepts_fresh_output_when_builder_does_not_exit(tmp_path):
    output = tmp_path / "training_v2_organic.parquet"
    metadata = tmp_path / "training_v2_organic_meta.json"
    metadata.write_text(
        json.dumps(
            {
                "row_count": 326,
                "dataset_build_timestamp": (
                    datetime.now(timezone.utc) + timedelta(seconds=1)
                ).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    timeout = subprocess.TimeoutExpired(cmd=["python", "build_dataset.py"], timeout=1)
    with patch("app.jobs.nightly_dataset_builder.subprocess.run", side_effect=timeout):
        result = run_nightly_organic_dataset_build(
            db_path=tmp_path / "cosmicforge.db",
            output_path=output,
            script_path=tmp_path / "build_dataset.py",
            timeout_seconds=1,
            watcher_enabled=False,
        )
    assert result["success"] is True
    assert result["row_count"] == 326
    assert result["finished_at"]


def _registered_jobs(enabled: bool):
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    import app.main as main_mod

    scheduler = MagicMock(spec=AsyncIOScheduler)
    jobs = []
    scheduler.add_job.side_effect = lambda fn, trigger=None, **kwargs: jobs.append(
        {"fn": fn, "trigger": trigger, "kwargs": kwargs}
    )
    original = main_mod._signal_scheduler
    main_mod._signal_scheduler = None
    try:
        with patch("apscheduler.schedulers.asyncio.AsyncIOScheduler", return_value=scheduler), \
             patch.object(main_mod.settings, "ORGANIC_DATASET_NIGHTLY_ENABLED", enabled), \
             patch.object(main_mod.settings, "ORGANIC_DATASET_NIGHTLY_TIME_UTC", "02:00"):
            loop.run_until_complete(main_mod._startup_signal_scheduler())
    finally:
        if main_mod._signal_scheduler:
            main_mod._signal_scheduler.shutdown(wait=False)
        main_mod._signal_scheduler = original
        loop.close()
        asyncio.set_event_loop(None)
    return jobs


def test_scheduler_registers_nightly_job_at_0200_utc_when_enabled():
    jobs = _registered_jobs(True)
    job = next(job for job in jobs if job["kwargs"].get("id") == "organic_dataset_nightly")
    assert job["trigger"] == "cron"
    assert job["kwargs"]["hour"] == 2
    assert job["kwargs"]["minute"] == 0
    assert job["kwargs"]["max_instances"] == 1


def test_scheduler_does_not_register_nightly_job_when_disabled():
    jobs = _registered_jobs(False)
    assert not any(job["kwargs"].get("id") == "organic_dataset_nightly" for job in jobs)


def test_nightly_job_writes_readiness_reports_after_dataset_build(tmp_path):
    output = tmp_path / "training_v2_organic.parquet"
    output.with_name("training_v2_organic_meta.json").write_text(
        json.dumps({"row_count": 500}), encoding="utf-8"
    )
    completed = SimpleNamespace(returncode=0, stdout="ok", stderr="")
    output_json = tmp_path / "readiness.json"
    output_md = tmp_path / "readiness.md"

    def watcher_runner(**kwargs):
        payload = {
            "ready_to_retry_5a": True,
            "ready_for_5b": False,
            "next_action": "retry_section_5a_manually",
        }
        output_json.write_text(json.dumps(payload), encoding="utf-8")
        output_md.write_text("# ML Training Readiness Status\n", encoding="utf-8")
        return payload

    with patch("app.jobs.nightly_dataset_builder.subprocess.run", return_value=completed):
        result = run_nightly_organic_dataset_build(
            db_path=tmp_path / "cosmicforge.db",
            output_path=output,
            script_path=tmp_path / "build_dataset.py",
            watcher_enabled=True,
            watcher_runner=watcher_runner,
        )
    assert result["success"] is True
    assert result["readiness_watcher_success"] is True
    assert output_json.exists()
    assert output_md.exists()


def test_nightly_watcher_failure_does_not_crash_runner(tmp_path):
    output = tmp_path / "training_v2_organic.parquet"
    output.with_name("training_v2_organic_meta.json").write_text(
        json.dumps({"row_count": 326}), encoding="utf-8"
    )
    completed = SimpleNamespace(returncode=0, stdout="ok", stderr="")

    def failing_watcher(**kwargs):
        raise RuntimeError("watcher failed")

    with patch("app.jobs.nightly_dataset_builder.subprocess.run", return_value=completed):
        result = run_nightly_organic_dataset_build(
            db_path=tmp_path / "cosmicforge.db",
            output_path=output,
            script_path=tmp_path / "build_dataset.py",
            watcher_enabled=True,
            watcher_runner=failing_watcher,
        )
    assert result["success"] is True
    assert result["readiness_watcher_success"] is False
    assert "watcher failed" in result["readiness_watcher_error"]
