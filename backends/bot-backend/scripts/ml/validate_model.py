#!/usr/bin/env python3
"""Validate an experimental ML candidate against strengthened Section 5A gates."""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import joblib

from shared_lib.ml.contract import ML_CONTRACT_VERSION, ML_FEATURE_COLUMNS, ML_FEATURE_SCHEMA_HASH


def evaluate_acceptance(
    metrics: dict[str, Any],
    contract_status: dict[str, Any],
    *,
    min_baseline_improvement: float = 0.01,
) -> dict[str, Any]:
    auc = _number(metrics.get("holdout_auc"))
    auc_std = _number(metrics.get("auc_std"))
    quartile_gap = _number(metrics.get("quartile_win_rate_gap"))
    logistic_auc = _number(metrics.get("logistic_baseline_auc"))
    row_count = int(metrics.get("row_count") or 0)
    reasons: list[str] = []
    warnings: list[str] = []

    if auc is None:
        reasons.append("Holdout AUC is unavailable.")
    elif auc < 0.55:
        reasons.append(f"Holdout AUC {auc:.4f} is below minimum 0.55.")
    elif auc > 0.90:
        reasons.append(f"Holdout AUC {auc:.4f} exceeds hard ceiling 0.90 (leakage risk).")
    elif auc > 0.72:
        warnings.append("Holdout AUC exceeds desired 0.72 range; inspect for overfit.")

    if quartile_gap is None:
        reasons.append("Quartile win-rate gap is unavailable.")
    elif quartile_gap < 0.05:
        reasons.append(f"Quartile win-rate gap {quartile_gap:.4f} is below minimum 0.05.")
    if auc_std is None:
        reasons.append("AUC standard deviation is unavailable.")
    elif auc_std > 0.15:
        reasons.append(f"AUC standard deviation {auc_std:.4f} exceeds 0.15.")
    if row_count < 300:
        reasons.append(f"Training row count {row_count} is below 300.")
    elif row_count < 500:
        warnings.append(f"LOW_SAMPLE_WARNING: training row count {row_count} is below 500.")

    if logistic_auc is None:
        reasons.append("Logistic baseline AUC is unavailable.")
    elif auc is not None and auc < logistic_auc + min_baseline_improvement:
        reasons.append(
            f"LightGBM AUC {auc:.4f} does not beat logistic baseline {logistic_auc:.4f} "
            f"by required {min_baseline_improvement:.4f}."
        )
    if contract_status.get("contract_version") != ML_CONTRACT_VERSION:
        reasons.append("Feature contract version does not match entry_quality_v2.")
    if contract_status.get("schema_hash") != ML_FEATURE_SCHEMA_HASH:
        reasons.append("Schema hash does not match the runtime v2 contract.")
    if list(contract_status.get("feature_columns") or []) != list(ML_FEATURE_COLUMNS):
        reasons.append("Feature columns do not match the runtime v2 contract.")
    if not contract_status.get("runtime_compatible"):
        reasons.append("Model is not runtime-scorer compatible.")

    warnings.append("EXPERIMENTAL_ONLY: Section 4 is in progress; not for production.")
    return {
        "accepted": not reasons,
        "eligible_for_section_5b_shadow_only": not reasons,
        "deployment_status": "EXPERIMENTAL_ONLY",
        "not_for_production": True,
        "rejection_reasons": reasons,
        "warnings": warnings,
        "metrics": {
            "holdout_auc": auc,
            "walk_forward_auc_mean": auc,
            "walk_forward_auc_std": auc_std,
            "quartile_win_rate_gap": quartile_gap,
            "row_count": row_count,
            "logistic_baseline_auc": logistic_auc,
            "baseline_auc_improvement": (
                round(auc - logistic_auc, 6)
                if auc is not None and logistic_auc is not None else None
            ),
            "minimum_baseline_auc_improvement": min_baseline_improvement,
        },
        "contract_status": contract_status,
    }


def validate_candidate(
    *,
    model_version: str,
    model_path: str | None = None,
    meta_path: str | None = None,
    validation_path: str | None = None,
    min_baseline_improvement: float = 0.01,
) -> dict[str, Any]:
    model, meta, validation = _resolve_paths(model_version, model_path, meta_path, validation_path)
    validation_doc = json.loads(validation.read_text(encoding="utf-8"))
    meta_doc = json.loads(meta.read_text(encoding="utf-8")) if meta else {}
    source = meta_doc or validation_doc
    aggregate = validation_doc.get("lgbm_binary_aggregate") or source.get("walk_forward_metrics") or {}
    runtime_compatible = False
    runtime_error: str | None = None
    if model is not None:
        try:
            loaded = joblib.load(model)
            runtime_compatible = callable(getattr(loaded, "predict_proba", None))
            if not runtime_compatible:
                runtime_error = "Loaded model has no predict_proba method."
        except Exception as exc:
            runtime_error = str(exc)
    else:
        runtime_error = "No model artifact was written by the trainer."

    contract_status = {
        "contract_version": source.get("contract_version") or source.get("feature_contract_version"),
        "schema_hash": source.get("schema_hash"),
        "feature_columns": source.get("feature_columns"),
        "runtime_compatible": runtime_compatible,
        "runtime_compatibility_error": runtime_error,
    }
    metrics = {
        "holdout_auc": (aggregate.get("auc") or {}).get("mean"),
        "auc_std": (aggregate.get("auc") or {}).get("std"),
        "quartile_win_rate_gap": (aggregate.get("quartile_diff_win") or {}).get("mean"),
        "row_count": source.get("row_count") or validation_doc.get("row_count"),
        "logistic_baseline_auc": validation_doc.get("logistic_baseline_auc")
        or source.get("logistic_baseline_auc"),
    }
    result = evaluate_acceptance(
        metrics, contract_status, min_baseline_improvement=min_baseline_improvement
    )
    result.update(
        {
            "model_path": str(model) if model else None,
            "meta_path": str(meta) if meta else None,
            "validation_path": str(validation),
            "trainer_accepted": validation_doc.get("accepted"),
        }
    )
    return result


def _resolve_paths(
    model_version: str,
    model_path: str | None,
    meta_path: str | None,
    validation_path: str | None,
) -> tuple[Path | None, Path | None, Path]:
    artifacts = _BOT_ROOT / "models" / "artifacts"
    validation = Path(validation_path) if validation_path else None
    if validation is None:
        matches = sorted(
            artifacts.glob(f"entry_quality_{model_version}_*_validation.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if not matches:
            raise FileNotFoundError(f"No validation report found for model version {model_version}.")
        validation = matches[0]
    base = validation.name.removesuffix("_validation.json")
    model = Path(model_path) if model_path else validation.with_name(f"{base}.pkl")
    meta = Path(meta_path) if meta_path else validation.with_name(f"{base}_meta.json")
    return model if model.exists() else None, meta if meta.exists() else None, validation


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-version", required=True)
    parser.add_argument("--model-path")
    parser.add_argument("--meta-path")
    parser.add_argument("--validation-path")
    parser.add_argument(
        "--min-baseline-improvement",
        type=float,
        default=float(os.environ.get("ML_MIN_BASELINE_AUC_IMPROVEMENT", "0.01")),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = validate_candidate(
        model_version=args.model_version,
        model_path=args.model_path,
        meta_path=args.meta_path,
        validation_path=args.validation_path,
        min_baseline_improvement=args.min_baseline_improvement,
    )
    print(json.dumps(result, indent=2))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
