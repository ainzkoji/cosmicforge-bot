#!/usr/bin/env python3
"""Run the guarded monthly Section 5A retraining pipeline."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from app.jobs.nightly_dataset_builder import run_nightly_organic_dataset_build
from scripts.ml.promote_model import (
    discover_candidate,
    extract_metrics,
    list_files,
    load_json,
    run_promotion,
    validate_candidate as validate_promotion_candidate,
)
from scripts.ml.validate_model import validate_candidate as validate_section5a_candidate
from scripts.ml.watch_training_readiness import evaluate_training_readiness


logger = logging.getLogger(__name__)

DEFAULT_DATASET_PATH = _BOT_ROOT / "models/datasets/training_v2_organic.parquet"
DEFAULT_ARTIFACTS_DIR = _BOT_ROOT / "models/artifacts"
DEFAULT_PRODUCTION_DIR = _BOT_ROOT / "models/production"
DEFAULT_ACTIVE_ENV = _BOT_ROOT / ".env"
DEFAULT_STRONG_TREND_CONFIG = _BOT_ROOT / ".env.paper_strong_trend_experiment"
DEFAULT_STRONG_TREND_STATUS = _BOT_ROOT / "models/reports/strong_trend_paper_experiment_status.json"
DEFAULT_REPORT_JSON = _BOT_ROOT / "models/reports/ml_monthly_retrain_status.json"
DEFAULT_REPORT_MD = _BOT_ROOT / "models/reports/ml_monthly_retrain_status.md"
DEFAULT_MANUAL_ACTIVATION_REPORT = (
    _BOT_ROOT / "models/reports/ml_shadow_manual_activation_required.md"
)

DEFAULT_MODEL_VERSION = "v2.0"
DEFAULT_MIN_ORGANIC_ROWS = 500
DEFAULT_MIN_IOFS_ROWS = 300
DEFAULT_MIN_CLOSED_IOFS_TRADES = 20
MIN_PRODUCTION_AUC_IMPROVEMENT = 0.01


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def _read_env(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def _readiness_blockers(
    readiness: dict[str, Any],
    *,
    min_organic_rows: int,
    min_iofs_rows: int,
    min_closed_iofs_trades: int,
) -> list[str]:
    blockers: list[str] = []
    if int(readiness.get("organic_rows") or 0) < min_organic_rows:
        blockers.append(f"ORGANIC_ROWS_BELOW_{min_organic_rows}")
    if int(readiness.get("iofs_organic_rows") or 0) < min_iofs_rows:
        blockers.append(f"IOFS_ROWS_BELOW_{min_iofs_rows}")
    if int(readiness.get("closed_iofs_paper_trades") or 0) < min_closed_iofs_trades:
        blockers.append(f"CLOSED_IOFS_TRADES_BELOW_{min_closed_iofs_trades}")
    return blockers


def _run_training(
    *,
    dataset_path: Path,
    model_version: str,
    organic_rows: int,
    timeout_seconds: int = 7200,
) -> dict[str, Any]:
    command = [
        sys.executable,
        str(_SCRIPT_DIR / "train_entry_model.py"),
        "--dataset-path",
        str(dataset_path),
        "--model-version",
        model_version,
        "--scale-pos-weight",
        "auto",
        "--calibrate",
    ]
    if 300 <= organic_rows < DEFAULT_MIN_ORGANIC_ROWS:
        command.extend(["--min-train-size", "200"])
    completed = subprocess.run(
        command,
        cwd=str(_BOT_ROOT),
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    return {
        "success": completed.returncode == 0,
        "returncode": completed.returncode,
        "command": command,
        "stdout": (completed.stdout or "")[-4000:],
        "stderr": (completed.stderr or "")[-4000:],
    }


def _current_production_summary(production_dir: Path) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    if production_dir.exists():
        for meta_path in production_dir.glob("entry_quality_*_meta.json"):
            model_path = meta_path.with_name(f"{meta_path.name.removesuffix('_meta.json')}.pkl")
            if not model_path.exists():
                continue
            meta = load_json(meta_path)
            if meta.get("accepted") is not True:
                continue
            metrics = extract_metrics(meta, {})
            candidates.append(
                {
                    "model_path": str(model_path),
                    "meta_path": str(meta_path),
                    "metrics": metrics,
                    "mtime": model_path.stat().st_mtime,
                }
            )
    if not candidates:
        return {"model_path": None, "meta_path": None, "metrics": {}}
    latest = max(candidates, key=lambda item: item["mtime"])
    latest.pop("mtime", None)
    return latest


def _write_manual_activation_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "# ML Shadow Manual Activation Required",
                "",
                f"- generated_at_utc: {report['generated_at_utc']}",
                f"- promoted_model: {report['candidate_model_path']}",
                "- T-25 status: NOT_STARTED",
                "- Active `.env` was not changed.",
                "- ML remains disabled until explicit manual review and approval.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def render_markdown(report: dict[str, Any]) -> str:
    blockers = report["blocking_reasons"] or ["None"]
    warnings = report["warnings"] or ["None"]
    return "\n".join(
        [
            "# Monthly ML Retrain Status",
            "",
            f"- generated_at_utc: {report['generated_at_utc']}",
            f"- dry_run: {str(report['dry_run']).lower()}",
            f"- force_check_only: {str(report['force_check_only']).lower()}",
            f"- ready_to_retrain: {str(report['ready_to_retrain']).lower()}",
            f"- retrain_attempted: {str(report['retrain_attempted']).lower()}",
            f"- dataset_build_attempted: {str(report['dataset_build_attempted']).lower()}",
            f"- training_attempted: {str(report['training_attempted']).lower()}",
            f"- validation_attempted: {str(report['validation_attempted']).lower()}",
            f"- promotion_attempted: {str(report['promotion_attempted']).lower()}",
            f"- promotion_allowed: {str(report['promotion_allowed']).lower()}",
            f"- promoted: {str(report['promoted']).lower()}",
            f"- section5b_status: {report['section5b_status']}",
            f"- t25_status: {report['t25_status']}",
            f"- organic_rows: {report['organic_rows']}",
            f"- iofs_organic_rows: {report['iofs_organic_rows']}",
            f"- closed_iofs_paper_trades: {report['closed_iofs_paper_trades']}",
            f"- candidate_model_path: {report['candidate_model_path'] or 'None'}",
            f"- candidate_validation_status: {report['candidate_validation_status']}",
            f"- current_production_model: {report['current_production_model'] or 'None'}",
            f"- ml_enabled_changed: {str(report['ml_enabled_changed']).lower()}",
            f"- env_changed: {str(report['env_changed']).lower()}",
            f"- strong_trend_experiment_status: {report['strong_trend_experiment_status']}",
            "",
            "## Blocking Reasons",
            "",
            *[f"- {reason}" for reason in blockers],
            "",
            "## Warnings",
            "",
            *[f"- {warning}" for warning in warnings],
            "",
            "This automation never enables ML or edits the active `.env`.",
            "",
        ]
    )


def _write_reports(report: dict[str, Any], report_json: Path, report_md: Path) -> None:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    report_md.write_text(render_markdown(report), encoding="utf-8")


def retrain_entry_model_if_ready(
    *,
    dataset_path: str | Path = DEFAULT_DATASET_PATH,
    model_version: str = DEFAULT_MODEL_VERSION,
    min_organic_rows: int = DEFAULT_MIN_ORGANIC_ROWS,
    min_iofs_rows: int = DEFAULT_MIN_IOFS_ROWS,
    min_closed_iofs_trades: int = DEFAULT_MIN_CLOSED_IOFS_TRADES,
    dry_run: bool = False,
    force_check_only: bool = False,
    report_json: str | Path = DEFAULT_REPORT_JSON,
    report_md: str | Path = DEFAULT_REPORT_MD,
    active_env: str | Path = DEFAULT_ACTIVE_ENV,
    artifacts_dir: str | Path = DEFAULT_ARTIFACTS_DIR,
    production_dir: str | Path = DEFAULT_PRODUCTION_DIR,
    strong_trend_config: str | Path = DEFAULT_STRONG_TREND_CONFIG,
    strong_trend_status: str | Path = DEFAULT_STRONG_TREND_STATUS,
    readiness_evaluator: Callable[..., dict[str, Any]] = evaluate_training_readiness,
    dataset_builder: Callable[..., dict[str, Any]] = run_nightly_organic_dataset_build,
    training_runner: Callable[..., dict[str, Any]] = _run_training,
    section5a_validator: Callable[..., dict[str, Any]] = validate_section5a_candidate,
    promotion_validator: Callable[..., dict[str, Any]] = validate_promotion_candidate,
    promoter: Callable[..., dict[str, Any]] = run_promotion,
) -> dict[str, Any]:
    """Retry Section 5A only when ready, and never raise into the scheduler."""
    dataset = Path(dataset_path)
    env_path = Path(active_env)
    artifacts = Path(artifacts_dir)
    production = Path(production_dir)
    strong_config = Path(strong_trend_config)
    strong_status = Path(strong_trend_status)
    output_json = Path(report_json)
    output_md = Path(report_md)

    env_before = _sha256(env_path)
    env_values_before = _read_env(env_path)
    strong_config_before = _sha256(strong_config)
    production_before = list_files(production)
    current_production = _current_production_summary(production)
    report: dict[str, Any] = {
        "generated_at_utc": _utc_now(),
        "dry_run": bool(dry_run),
        "force_check_only": bool(force_check_only),
        "ready_to_retrain": False,
        "retrain_attempted": False,
        "dataset_build_attempted": False,
        "training_attempted": False,
        "validation_attempted": False,
        "promotion_attempted": False,
        "promotion_allowed": False,
        "promoted": False,
        "section5b_status": "BLOCKED",
        "t25_status": "NOT_STARTED",
        "organic_rows": 0,
        "iofs_organic_rows": 0,
        "closed_iofs_paper_trades": 0,
        "candidate_model_path": None,
        "candidate_validation_status": "not_attempted",
        "candidate_metrics": {},
        "current_production_model": current_production["model_path"],
        "production_comparison": {},
        "blocking_reasons": [],
        "warnings": [],
        "ml_enabled_changed": False,
        "env_changed": False,
        "production_files_before": production_before,
        "production_files_after": production_before,
        "strong_trend_experiment_status": (
            "ACTIVE" if strong_config.exists() and strong_status.exists() else "STATUS_UNAVAILABLE"
        ),
        "strong_trend_experiment_changed": False,
        "dataset_build_result": None,
        "training_result": None,
        "section5a_validation_result": None,
        "promotion_preflight_result": None,
        "promotion_result": None,
        "pipeline_error": None,
    }

    try:
        readiness_kwargs = {
            "organic_dataset": dataset,
            "active_env_path": env_path,
            "artifacts_dir": artifacts,
            "min_organic_rows": min_organic_rows,
            "min_iofs_organic_rows": min_iofs_rows,
            "min_closed_iofs_trades": min_closed_iofs_trades,
        }
        readiness = readiness_evaluator(**readiness_kwargs)
        report["organic_rows"] = int(readiness.get("organic_rows") or 0)
        report["iofs_organic_rows"] = int(readiness.get("iofs_organic_rows") or 0)
        report["closed_iofs_paper_trades"] = int(
            readiness.get("closed_iofs_paper_trades") or 0
        )
        report["ready_to_retrain"] = bool(readiness.get("ready_to_retry_5a"))
        threshold_blockers = _readiness_blockers(
            readiness,
            min_organic_rows=min_organic_rows,
            min_iofs_rows=min_iofs_rows,
            min_closed_iofs_trades=min_closed_iofs_trades,
        )
        if not report["ready_to_retrain"]:
            report["blocking_reasons"].extend(threshold_blockers)
            return report
        if dry_run:
            report["warnings"].append("DRY_RUN_CHECK_ONLY_NO_TRAINING_OR_PROMOTION")
            return report
        if force_check_only:
            report["warnings"].append("FORCE_CHECK_ONLY_NO_TRAINING_OR_PROMOTION")
            return report

        report["retrain_attempted"] = True
        report["dataset_build_attempted"] = True
        build_result = dataset_builder(output_path=dataset)
        report["dataset_build_result"] = build_result
        if not build_result.get("success"):
            report["blocking_reasons"].append("DATASET_BUILD_FAILED")
            return report

        readiness = readiness_evaluator(**readiness_kwargs)
        report["organic_rows"] = int(readiness.get("organic_rows") or 0)
        report["iofs_organic_rows"] = int(readiness.get("iofs_organic_rows") or 0)
        report["closed_iofs_paper_trades"] = int(
            readiness.get("closed_iofs_paper_trades") or 0
        )
        report["ready_to_retrain"] = bool(readiness.get("ready_to_retry_5a"))
        if not report["ready_to_retrain"]:
            report["blocking_reasons"].append("POST_BUILD_READINESS_GATE_FAILED")
            report["blocking_reasons"].extend(
                _readiness_blockers(
                    readiness,
                    min_organic_rows=min_organic_rows,
                    min_iofs_rows=min_iofs_rows,
                    min_closed_iofs_trades=min_closed_iofs_trades,
                )
            )
            return report

        report["training_attempted"] = True
        training = training_runner(
            dataset_path=dataset,
            model_version=model_version,
            organic_rows=report["organic_rows"],
        )
        report["training_result"] = training
        if not training.get("success"):
            report["blocking_reasons"].append("TRAINING_FAILED")
            return report

        report["validation_attempted"] = True
        validation = section5a_validator(model_version=model_version)
        report["section5a_validation_result"] = validation
        report["candidate_model_path"] = validation.get("model_path")
        report["candidate_validation_status"] = (
            "accepted" if validation.get("accepted") else "rejected"
        )
        if not validation.get("accepted"):
            report["blocking_reasons"].append("SECTION5A_VALIDATION_FAILED")
            report["blocking_reasons"].extend(validation.get("rejection_reasons") or [])
            return report

        candidate = discover_candidate(artifacts, model_version)
        preflight = promotion_validator(candidate, model_version)
        report["promotion_preflight_result"] = preflight
        report["candidate_model_path"] = (
            str(candidate["model"]) if candidate.get("model") else report["candidate_model_path"]
        )
        report["candidate_metrics"] = preflight.get("metrics") or {}
        if preflight.get("blocking_reasons"):
            report["candidate_validation_status"] = "rejected_by_promotion_preflight"
            report["blocking_reasons"].extend(preflight["blocking_reasons"])
            return report

        current = _current_production_summary(production)
        report["current_production_model"] = current["model_path"]
        candidate_auc = (preflight.get("metrics") or {}).get("auc")
        current_auc = (current.get("metrics") or {}).get("auc")
        report["production_comparison"] = {
            "new_auc": candidate_auc,
            "current_auc": current_auc,
            "new_quartile_gap": (preflight.get("metrics") or {}).get(
                "quartile_win_rate_gap"
            ),
            "current_quartile_gap": (current.get("metrics") or {}).get(
                "quartile_win_rate_gap"
            ),
            "new_auc_std": (preflight.get("metrics") or {}).get("auc_std"),
            "current_auc_std": (current.get("metrics") or {}).get("auc_std"),
        }
        if (
            current["model_path"] is not None
            and (
                candidate_auc is None
                or current_auc is None
                or candidate_auc <= current_auc + MIN_PRODUCTION_AUC_IMPROVEMENT
            )
        ):
            report["blocking_reasons"].append(
                "CURRENT_PRODUCTION_AUC_IMPROVEMENT_INSUFFICIENT"
            )
            return report

        report["promotion_attempted"] = True
        promotion = promoter(
            model_version=model_version,
            artifacts_dir=artifacts,
            production_dir=production,
            dry_run=False,
            active_env=env_path,
        )
        report["promotion_result"] = promotion
        report["promotion_allowed"] = bool(promotion.get("promotion_allowed"))
        report["promoted"] = bool(promotion.get("promoted"))
        report["section5b_status"] = (
            "PROMOTED_SHADOW_PREP" if report["promoted"] else "BLOCKED"
        )
        if not report["promotion_allowed"]:
            report["blocking_reasons"].extend(promotion.get("blocking_reasons") or [])
        if report["promoted"]:
            _write_manual_activation_report(DEFAULT_MANUAL_ACTIVATION_REPORT, report)
    except Exception as exc:
        report["pipeline_error"] = str(exc)
        report["blocking_reasons"].append("PIPELINE_ERROR")
        logger.exception("[ML_MONTHLY_RETRAIN] failure=%s; trading runner continues", exc)
    finally:
        env_after = _sha256(env_path)
        env_values_after = _read_env(env_path)
        report["env_changed"] = env_before != env_after
        report["ml_enabled_changed"] = (
            env_values_before.get("ML_ENABLED") != env_values_after.get("ML_ENABLED")
        )
        report["production_files_after"] = list_files(production)
        report["strong_trend_experiment_changed"] = strong_config_before != _sha256(
            strong_config
        )
        if report["env_changed"]:
            report["blocking_reasons"].append("ACTIVE_ENV_CHANGED")
        if report["ml_enabled_changed"]:
            report["blocking_reasons"].append("ML_ENABLED_CHANGED")
        if report["strong_trend_experiment_changed"]:
            report["blocking_reasons"].append("STRONG_TREND_EXPERIMENT_CHANGED")
        report["blocking_reasons"] = list(dict.fromkeys(report["blocking_reasons"]))
        report["warnings"] = list(dict.fromkeys(report["warnings"]))
        report["generated_at_utc"] = _utc_now()
        try:
            _write_reports(report, output_json, output_md)
        except Exception as exc:
            report["pipeline_error"] = report["pipeline_error"] or str(exc)
            report["blocking_reasons"] = list(
                dict.fromkeys([*report["blocking_reasons"], "REPORT_WRITE_FAILED"])
            )
            logger.exception(
                "[ML_MONTHLY_RETRAIN] report_write_failed=%s; trading runner continues",
                exc,
            )
        logger.info(
            "[ML_MONTHLY_RETRAIN] ready=%s retrain_attempted=%s promoted=%s blockers=%s",
            report["ready_to_retrain"],
            report["retrain_attempted"],
            report["promoted"],
            report["blocking_reasons"],
        )
    return report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-path", default=str(DEFAULT_DATASET_PATH))
    parser.add_argument("--model-version", default=DEFAULT_MODEL_VERSION)
    parser.add_argument("--min-organic-rows", type=int, default=DEFAULT_MIN_ORGANIC_ROWS)
    parser.add_argument("--min-iofs-rows", type=int, default=DEFAULT_MIN_IOFS_ROWS)
    parser.add_argument(
        "--min-closed-iofs-trades", type=int, default=DEFAULT_MIN_CLOSED_IOFS_TRADES
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-check-only", action="store_true")
    parser.add_argument("--report-json", default=str(DEFAULT_REPORT_JSON))
    parser.add_argument("--report-md", default=str(DEFAULT_REPORT_MD))
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    report = retrain_entry_model_if_ready(
        dataset_path=args.dataset_path,
        model_version=args.model_version,
        min_organic_rows=args.min_organic_rows,
        min_iofs_rows=args.min_iofs_rows,
        min_closed_iofs_trades=args.min_closed_iofs_trades,
        dry_run=args.dry_run,
        force_check_only=args.force_check_only,
        report_json=args.report_json,
        report_md=args.report_md,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
