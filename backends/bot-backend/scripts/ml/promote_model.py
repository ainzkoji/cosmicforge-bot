#!/usr/bin/env python3
"""Guarded Section 5B/T-24 model promotion."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from shared_lib.ml.contract import (
    ML_CONTRACT_VERSION,
    ML_FEATURE_COLUMNS,
    ML_FEATURE_SCHEMA_HASH,
)


DEFAULT_REPORT_MD = _BOT_ROOT / "models/reports/section5b_promotion_guard_report.md"
DEFAULT_REPORT_JSON = _BOT_ROOT / "models/reports/section5b_promotion_guard_report.json"
DEFAULT_SHADOW_SUGGESTION = _BOT_ROOT / "models/reports/section5b_shadow_env_suggestion.env"
MIN_BASELINE_IMPROVEMENT = 0.01
SCHEMA_PREFIX = "d4b19440"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def list_files(path: Path) -> list[str]:
    if not path.exists():
        return []
    return sorted(item.name for item in path.iterdir() if item.is_file())


def load_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def discover_candidate(artifacts_dir: Path, model_version: str) -> dict[str, Path | None]:
    prefix = f"entry_quality_{model_version}_"
    suffixes = ("_validation.json", "_meta.json", "_encoders.pkl", ".pkl")
    bases: set[str] = set()
    if artifacts_dir.exists():
        for path in artifacts_dir.iterdir():
            if not path.is_file() or not path.name.startswith(prefix):
                continue
            for suffix in suffixes:
                if path.name.endswith(suffix):
                    base = path.name[: -len(suffix)]
                    if re.fullmatch(rf"entry_quality_{re.escape(model_version)}_\d{{8}}", base):
                        bases.add(base)
                    break
    if not bases:
        return {
            "base": None,
            "model": None,
            "meta": None,
            "validation": None,
            "encoders": None,
        }
    base = sorted(bases, reverse=True)[0]
    expected = {
        "base": Path(base),
        "model": artifacts_dir / f"{base}.pkl",
        "meta": artifacts_dir / f"{base}_meta.json",
        "validation": artifacts_dir / f"{base}_validation.json",
        "encoders": artifacts_dir / f"{base}_encoders.pkl",
    }
    return expected


def extract_metrics(meta: dict[str, Any], validation: dict[str, Any]) -> dict[str, Any]:
    aggregate = validation.get("lgbm_binary_aggregate") or meta.get("walk_forward_metrics") or {}
    return {
        "training_row_count": int(validation.get("row_count") or meta.get("row_count") or 0),
        "auc": number((aggregate.get("auc") or {}).get("mean")),
        "auc_std": number((aggregate.get("auc") or {}).get("std")),
        "quartile_win_rate_gap": number((aggregate.get("quartile_diff_win") or {}).get("mean")),
        "logistic_baseline_auc": number(
            validation.get("logistic_baseline_auc") or meta.get("logistic_baseline_auc")
        ),
        "minimum_baseline_improvement": number(
            validation.get("minimum_baseline_auc_improvement")
            or meta.get("minimum_baseline_auc_improvement")
            or MIN_BASELINE_IMPROVEMENT
        ),
    }


def runtime_load_check(
    model_path: Path | None,
    meta_path: Path | None,
    encoders_path: Path | None,
) -> dict[str, Any]:
    result = {
        "attempted": False,
        "passed": False,
        "model_loaded": False,
        "contract_loaded": False,
        "schema_hash_matches": False,
        "feature_order_matches": False,
        "missing_features_encoded_as_nan": False,
        "dummy_score": None,
        "error": None,
    }
    if model_path is None or meta_path is None or not model_path.exists() or not meta_path.exists():
        result["error"] = "MODEL_OR_METADATA_MISSING"
        return result

    result["attempted"] = True
    try:
        import numpy as np
        import app.ml.scorer as scorer_module

        with tempfile.TemporaryDirectory(prefix="section5b_runtime_check_") as tmp:
            production = Path(tmp) / "models" / "production"
            production.mkdir(parents=True)
            staged_model = production / model_path.name
            staged_meta = production / meta_path.name
            shutil.copy2(model_path, staged_model)
            shutil.copy2(meta_path, staged_meta)
            staged_encoders: Path | None = None
            if encoders_path is not None and encoders_path.exists():
                staged_encoders = production / encoders_path.name
                shutil.copy2(encoders_path, staged_encoders)

            original_persist = scorer_module.upsert_ml_runtime_status
            scorer_module.upsert_ml_runtime_status = lambda *args, **kwargs: None
            scorer = None
            try:
                scorer = scorer_module.MLEntryScorer(
                    model_path=str(staged_model),
                    metadata_path=str(staged_meta),
                    encoders_path=str(staged_encoders or ""),
                    enabled=True,
                    shadow_mode=True,
                    hard_block_floor=0.0,
                    log_dir=str(Path(tmp) / "logs"),
                )
                status = scorer.status()
                result["model_loaded"] = bool(status["loaded"])
                result["contract_loaded"] = status["contract_version"] == ML_CONTRACT_VERSION
                result["schema_hash_matches"] = status["schema_hash"] == ML_FEATURE_SCHEMA_HASH
                result["feature_order_matches"] = list(status["feature_columns"]) == list(ML_FEATURE_COLUMNS)
                missing = scorer._encode_features({})  # Runtime rule: missing values become NaN.
                result["missing_features_encoded_as_nan"] = bool(
                    missing.shape == (1, len(ML_FEATURE_COLUMNS)) and np.isnan(missing).all()
                )
                dummy = {feature: 0.0 for feature in ML_FEATURE_COLUMNS}
                result["dummy_score"] = scorer.score(dummy)
                result["passed"] = bool(
                    result["model_loaded"]
                    and result["contract_loaded"]
                    and result["schema_hash_matches"]
                    and result["feature_order_matches"]
                    and result["missing_features_encoded_as_nan"]
                    and result["dummy_score"] is not None
                )
                if not result["passed"]:
                    result["error"] = status.get("load_error") or "DUMMY_SCORE_FAILED"
            finally:
                if scorer is not None:
                    scorer.close()
                scorer_module.upsert_ml_runtime_status = original_persist
    except Exception as exc:
        result["error"] = str(exc)
    return result


def validate_candidate(
    candidate: dict[str, Path | None],
    model_version: str,
    *,
    min_baseline_improvement: float = MIN_BASELINE_IMPROVEMENT,
) -> dict[str, Any]:
    model = candidate["model"]
    meta_path = candidate["meta"]
    validation_path = candidate["validation"]
    encoders = candidate["encoders"]
    meta = load_json(meta_path)
    validation = load_json(validation_path)
    metrics = extract_metrics(meta, validation)
    blockers: list[str] = []
    warnings: list[str] = []

    if model_version.lower().startswith("v1"):
        blockers.append("LEGACY_V1_ARTIFACT")
    if candidate["base"] is None:
        blockers.append("NO_ACCEPTED_MODEL_ARTIFACT")
    if model is None or not model.exists():
        blockers.append("MODEL_PKL_MISSING")
    if meta_path is None or not meta_path.exists():
        blockers.append("META_JSON_MISSING")
    if validation_path is None or not validation_path.exists():
        blockers.append("VALIDATION_JSON_MISSING")
    if validation_path is not None and validation_path.exists() and validation.get("accepted") is not True:
        blockers.append("VALIDATION_NOT_ACCEPTED")
    if meta_path is not None and meta_path.exists() and meta.get("accepted") is not True:
        blockers.append("METADATA_NOT_ACCEPTED")

    contract_version = meta.get("contract_version") or meta.get("feature_contract_version")
    schema_hash = str(meta.get("schema_hash") or "")
    if meta_path is not None and meta_path.exists():
        if contract_version != ML_CONTRACT_VERSION:
            blockers.append("FEATURE_CONTRACT_VERSION_MISMATCH")
        if schema_hash != ML_FEATURE_SCHEMA_HASH or not schema_hash.startswith(SCHEMA_PREFIX):
            blockers.append("SCHEMA_HASH_MISMATCH")
        if list(meta.get("feature_columns") or []) != list(ML_FEATURE_COLUMNS):
            blockers.append("FEATURE_ORDER_MISMATCH")

    rows = metrics["training_row_count"]
    auc = metrics["auc"]
    auc_std = metrics["auc_std"]
    gap = metrics["quartile_win_rate_gap"]
    logistic_auc = metrics["logistic_baseline_auc"]
    if rows < 300:
        blockers.append("TRAINING_ROW_COUNT_BELOW_300")
    if auc is None:
        blockers.append("AUC_MISSING")
    elif auc < 0.55:
        blockers.append("AUC_BELOW_0_55")
    elif auc > 0.90:
        blockers.append("AUC_ABOVE_0_90")
    elif auc > 0.72:
        blockers.append("AUC_ABOVE_0_72_PROMOTION_RANGE")
    if auc_std is None or auc_std > 0.15:
        blockers.append("AUC_STD_ABOVE_0_15_OR_MISSING")
    if gap is None or gap < 0.05:
        blockers.append("QUARTILE_GAP_BELOW_5PP_OR_MISSING")
    if (
        auc is None
        or logistic_auc is None
        or auc < logistic_auc + min_baseline_improvement
    ):
        blockers.append("LIGHTGBM_BASELINE_IMPROVEMENT_INSUFFICIENT")

    runtime = runtime_load_check(model, meta_path, encoders)
    if not runtime["passed"]:
        blockers.append("RUNTIME_SCORER_LOAD_CHECK_FAILED")
    if model is not None and model.exists() and (encoders is None or not encoders.exists()):
        warnings.append("Encoders artifact absent; runtime will use its documented missing-encoder behavior.")
    if blockers and "NO_ACCEPTED_MODEL_ARTIFACT" not in blockers:
        blockers.insert(0, "NO_ACCEPTED_MODEL_ARTIFACT")
    return {
        "blocking_reasons": list(dict.fromkeys(blockers)),
        "warnings": warnings,
        "metrics": metrics,
        "runtime_load_check": runtime,
        "validation_accepted": validation.get("accepted"),
        "metadata_accepted": meta.get("accepted"),
        "contract_version": contract_version,
        "schema_hash": schema_hash or None,
    }


def production_conflict_check(production_dir: Path) -> dict[str, Any]:
    model_files = sorted(production_dir.glob("*.pkl")) if production_dir.exists() else []
    invalid: list[str] = []
    valid_groups: list[dict[str, Path | None]] = []
    for model in model_files:
        if model.name.endswith("_encoders.pkl"):
            continue
        base = model.stem
        meta = production_dir / f"{base}_meta.json"
        encoders = production_dir / f"{base}_encoders.pkl"
        check = runtime_load_check(model, meta, encoders)
        if check["passed"]:
            valid_groups.append({"model": model, "meta": meta, "encoders": encoders if encoders.exists() else None})
        else:
            invalid.append(model.name)
    for artifact in production_dir.glob("entry_quality_*") if production_dir.exists() else []:
        if not artifact.is_file() or "-prev" in artifact.stem:
            continue
        if artifact.suffix == ".json" and artifact.name.endswith("_meta.json"):
            base = artifact.name.removesuffix("_meta.json")
            if not (production_dir / f"{base}.pkl").exists():
                invalid.append(artifact.name)
        if artifact.name.endswith("_encoders.pkl"):
            base = artifact.name.removesuffix("_encoders.pkl")
            if not (production_dir / f"{base}.pkl").exists():
                invalid.append(artifact.name)
    return {"invalid_models": sorted(set(invalid)), "valid_groups": valid_groups}


def archive_valid_production_groups(groups: list[dict[str, Path | None]]) -> list[str]:
    archived: list[str] = []
    for group in groups:
        for path in group.values():
            if path is None or not path.exists():
                continue
            target = path.with_name(f"{path.stem}-prev{path.suffix}")
            counter = 2
            while target.exists():
                target = path.with_name(f"{path.stem}-prev{counter}{path.suffix}")
                counter += 1
            path.rename(target)
            archived.append(target.name)
    return archived


def write_shadow_suggestion(path: Path, model_name: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "ML_ENABLED=True",
                "ML_SHADOW_MODE=True",
                f"ML_MODEL_PATH=models/production/{model_name}",
                "ML_SCORE_THRESHOLD=0.60",
                "ML_HARD_BLOCK_FLOOR=0.0",
                "",
            ]
        ),
        encoding="utf-8",
    )


def render_markdown(report: dict[str, Any]) -> str:
    blockers = report["blocking_reasons"] or ["None"]
    return "\n".join(
        [
            "# Section 5B Promotion Guard Report",
            "",
            f"- generated_at_utc: {report['generated_at_utc']}",
            f"- promotion_allowed: {str(report['promotion_allowed']).lower()}",
            f"- promoted: {str(report['promoted']).lower()}",
            f"- dry_run: {str(report['dry_run']).lower()}",
            f"- model_version: {report['model_version']}",
            f"- candidate_model_path: {report['candidate_model_path'] or 'None'}",
            f"- candidate_meta_path: {report['candidate_meta_path'] or 'None'}",
            f"- candidate_validation_path: {report['candidate_validation_path'] or 'None'}",
            f"- candidate_validation_status: {report['candidate_validation_status']}",
            f"- blocking_reasons: {json.dumps(blockers)}",
            f"- runtime_load_check_passed: {str(report['runtime_load_check']['passed']).lower()}",
            f"- ml_env_update_allowed: {str(report['ml_env_update_allowed']).lower()}",
            f"- section5b_status: {report['section5b_status']}",
            f"- t25_status: {report['t25_status']}",
            "",
            "No active `.env` update is performed by this guard.",
            "",
        ]
    )


def run_promotion(
    *,
    model_version: str,
    artifacts_dir: Path,
    production_dir: Path,
    dry_run: bool,
    report_md: Path = DEFAULT_REPORT_MD,
    report_json: Path = DEFAULT_REPORT_JSON,
    shadow_suggestion: Path = DEFAULT_SHADOW_SUGGESTION,
    active_env: Path = _BOT_ROOT / ".env",
    min_baseline_improvement: float = MIN_BASELINE_IMPROVEMENT,
) -> dict[str, Any]:
    env_hash_before = sha256(active_env)
    production_dir.mkdir(parents=True, exist_ok=True)
    before = list_files(production_dir)
    candidate = discover_candidate(artifacts_dir, model_version)
    validation = validate_candidate(
        candidate,
        model_version,
        min_baseline_improvement=min_baseline_improvement,
    )
    conflict = production_conflict_check(production_dir)
    blockers = list(validation["blocking_reasons"])
    if conflict["invalid_models"]:
        blockers.append("CONFLICTING_INVALID_PRODUCTION_MODEL")
    blockers = list(dict.fromkeys(blockers))
    allowed = not blockers
    promoted = False
    archived: list[str] = []

    if allowed and not dry_run:
        archived = archive_valid_production_groups(conflict["valid_groups"])
        for key in ("model", "meta", "encoders"):
            source = candidate[key]
            if source is not None and source.exists():
                shutil.copy2(source, production_dir / source.name)
        promoted = True
        write_shadow_suggestion(shadow_suggestion, candidate["model"].name)  # type: ignore[union-attr]

    env_hash_after = sha256(active_env)
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "promotion_allowed": allowed,
        "promoted": promoted,
        "dry_run": dry_run,
        "model_version": model_version,
        "candidate_model_path": str(candidate["model"]) if candidate["model"] else None,
        "candidate_meta_path": str(candidate["meta"]) if candidate["meta"] else None,
        "candidate_validation_path": str(candidate["validation"]) if candidate["validation"] else None,
        "candidate_validation_status": (
            "accepted" if validation["validation_accepted"] is True else "rejected_or_missing"
        ),
        "blocking_reasons": blockers,
        "warnings": validation["warnings"],
        "metrics": validation["metrics"],
        "production_files_before": before,
        "production_files_after": list_files(production_dir),
        "production_archived_files": archived,
        "runtime_load_check": validation["runtime_load_check"],
        "ml_env_update_allowed": promoted,
        "section5b_status": "PROMOTED_SHADOW_PREP" if promoted else "BLOCKED",
        "t25_status": "NOT_STARTED",
        "active_env_sha256_before": env_hash_before,
        "active_env_sha256_after": env_hash_after,
        "active_env_changed": env_hash_before != env_hash_after,
        "shadow_env_suggestion_written": bool(promoted and shadow_suggestion.exists()),
    }
    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_md.write_text(render_markdown(report), encoding="utf-8")
    report_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--artifacts-dir", type=Path, required=True)
    parser.add_argument("--production-dir", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--min-baseline-improvement",
        type=float,
        default=float(os.environ.get("ML_MIN_BASELINE_AUC_IMPROVEMENT", str(MIN_BASELINE_IMPROVEMENT))),
    )
    args = parser.parse_args()
    report = run_promotion(
        model_version=args.model_version,
        artifacts_dir=args.artifacts_dir,
        production_dir=args.production_dir,
        dry_run=args.dry_run,
        min_baseline_improvement=args.min_baseline_improvement,
    )
    print(json.dumps(report, indent=2))
    return 0 if report["promotion_allowed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
