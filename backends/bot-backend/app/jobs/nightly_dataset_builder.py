from __future__ import annotations

import json
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from app.core.config import settings

logger = logging.getLogger(__name__)

BACKEND_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCRIPT_PATH = BACKEND_ROOT / "scripts" / "ml" / "build_dataset.py"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_backend_path(value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (BACKEND_ROOT / path).resolve()


def _database_path_from_url(database_url: str) -> Path:
    prefix = "sqlite:///"
    if not str(database_url).startswith(prefix):
        raise ValueError("nightly organic dataset build requires a sqlite DATABASE_URL")
    return _resolve_backend_path(str(database_url)[len(prefix):])


def _read_fresh_metadata(metadata_path: Path, started_at: datetime) -> dict[str, Any]:
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    build_timestamp = datetime.fromisoformat(str(metadata["dataset_build_timestamp"]))
    if build_timestamp < started_at:
        raise RuntimeError("dataset builder timed out without writing fresh metadata")
    return metadata


def run_nightly_organic_dataset_build(
    *,
    db_path: str | Path | None = None,
    output_path: str | Path | None = None,
    script_path: str | Path | None = None,
    timeout_seconds: int = 1800,
    watcher_enabled: bool | None = None,
    watcher_runner: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Refresh the strict post-repair organic dataset without raising to the scheduler."""
    started = datetime.now(timezone.utc)
    started_at = started.isoformat()
    resolved_output = _resolve_backend_path(
        output_path or settings.ORGANIC_DATASET_OUTPUT_PATH
    )
    result: dict[str, Any] = {
        "success": False,
        "output_path": str(resolved_output),
        "row_count": None,
        "started_at": started_at,
        "finished_at": None,
        "error": None,
        "readiness_watcher_success": None,
        "readiness_watcher_error": None,
        "readiness_status": None,
    }

    try:
        resolved_db = (
            _resolve_backend_path(db_path)
            if db_path is not None
            else _database_path_from_url(settings.DATABASE_URL)
        )
        resolved_script = _resolve_backend_path(script_path or DEFAULT_SCRIPT_PATH)
        command = [
            sys.executable,
            str(resolved_script),
            "--db-path",
            str(resolved_db),
            "--only-organic",
            "--require-trace-id",
            "--exclude-incomplete-labels",
            "--post-repair-only",
            "--output",
            str(resolved_output),
            "--verbose",
        ]
        try:
            completed = subprocess.run(
                command,
                cwd=str(BACKEND_ROOT),
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            metadata_path = resolved_output.with_name(f"{resolved_output.stem}_meta.json")
            metadata = _read_fresh_metadata(metadata_path, started)
            result["row_count"] = int(metadata["row_count"])
            result["success"] = True
            logger.warning(
                "[ORGANIC_DATASET_NIGHTLY] builder timed out after writing fresh output; "
                "success=true row_count=%s output_path=%s",
                result["row_count"],
                resolved_output,
            )
            return result

        if completed.returncode != 0:
            error_text = (completed.stderr or completed.stdout or "dataset builder failed").strip()
            raise RuntimeError(error_text[-2000:])

        metadata_path = resolved_output.with_name(f"{resolved_output.stem}_meta.json")
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        result["row_count"] = int(metadata["row_count"])
        result["success"] = True
        logger.info(
            "[ORGANIC_DATASET_NIGHTLY] success=true row_count=%s output_path=%s",
            result["row_count"],
            resolved_output,
        )
    except Exception as exc:
        result["error"] = str(exc)
        logger.exception(
            "[ORGANIC_DATASET_NIGHTLY] success=false output_path=%s error=%s",
            resolved_output,
            exc,
        )
    finally:
        should_run_watcher = (
            bool(getattr(settings, "ML_RETRY_WATCHER_ENABLED", True))
            if watcher_enabled is None else watcher_enabled
        )
        if should_run_watcher:
            try:
                if watcher_runner is None:
                    from scripts.ml.watch_training_readiness import run_watcher

                    watcher_runner = run_watcher
                readiness = watcher_runner(
                    organic_dataset=resolved_output,
                    min_organic_rows=int(getattr(settings, "ML_RETRY_MIN_ORGANIC_ROWS", 500)),
                    min_iofs_organic_rows=int(
                        getattr(settings, "ML_RETRY_MIN_IOFS_ORGANIC_ROWS", 300)
                    ),
                    min_closed_iofs_trades=int(
                        getattr(settings, "ML_RETRY_MIN_CLOSED_IOFS_TRADES", 20)
                    ),
                )
                result["readiness_watcher_success"] = True
                result["readiness_status"] = readiness
                logger.info(
                    "[ML_RETRY_WATCHER] success=true ready_to_retry_5a=%s "
                    "ready_for_5b=%s next_action=%s",
                    readiness["ready_to_retry_5a"],
                    readiness["ready_for_5b"],
                    readiness["next_action"],
                )
            except Exception as exc:
                result["readiness_watcher_success"] = False
                result["readiness_watcher_error"] = str(exc)
                logger.exception(
                    "[ML_RETRY_WATCHER] success=false error=%s; runner continues",
                    exc,
                )
        result["finished_at"] = _utc_now_iso()

    return result
