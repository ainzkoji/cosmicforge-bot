#!/usr/bin/env python3
"""Check offline training readiness and automatically guard Section 5B."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import pandas as pd
import joblib

from shared_lib.ml.contract import (
    LABEL_COLUMNS,
    METADATA_COLUMNS,
    ML_CONTRACT_VERSION,
    ML_FEATURE_COLUMNS,
    ML_FEATURE_SCHEMA_HASH,
)


MINIMUM_ROWS = 300
RECOMMENDED_ROWS = 500
EXPECTED_POST_REPAIR_START = "2026-04-01T00:00:00+00:00"
TARGET_COLUMNS = ("label_win", "label")
LEAKAGE_FIELDS = frozenset(
    set(LABEL_COLUMNS)
    | set(METADATA_COLUMNS)
    | {
        "fill_price",
        "fill_qty",
        "execution_status",
        "execution_error",
        "order_id",
        "realized_pnl",
        "r_multiple",
        "exit_reason",
        "mfe_pct",
        "mae_pct",
        "final_position",
        "outcome",
        "exit_time",
        "ambiguous_candle",
    }
)


def check_readiness(
    dataset_path: str | Path,
    *,
    metadata_path: str | Path | None = None,
    section4_status_path: str | Path | None = None,
    artifacts_dir: str | Path | None = None,
) -> dict[str, Any]:
    dataset = Path(dataset_path)
    metadata = Path(metadata_path) if metadata_path else _default_metadata_path(dataset)
    section4_path = Path(section4_status_path) if section4_status_path else (
        _BOT_ROOT / "models" / "reports" / "iofs_paper_validation_status.md"
    )
    artifacts = Path(artifacts_dir) if artifacts_dir else _BOT_ROOT / "models" / "artifacts"
    blocking_reasons: list[str] = []
    warnings: list[str] = []
    meta: dict[str, Any] = {}
    frame: pd.DataFrame | None = None

    if not dataset.exists():
        blocking_reasons.append(f"Dataset file does not exist: {dataset}")
    else:
        try:
            frame = pd.read_parquet(dataset)
        except Exception as exc:
            blocking_reasons.append(f"Dataset could not be read: {exc}")

    if not metadata.exists():
        blocking_reasons.append(f"Dataset metadata does not exist: {metadata}")
    else:
        try:
            meta = json.loads(metadata.read_text(encoding="utf-8"))
        except Exception as exc:
            blocking_reasons.append(f"Dataset metadata could not be read: {exc}")

    row_count = int(len(frame)) if frame is not None else int(meta.get("row_count") or 0)
    if row_count < MINIMUM_ROWS:
        blocking_reasons.append(f"Row count {row_count} is below minimum {MINIMUM_ROWS}.")
    elif row_count < RECOMMENDED_ROWS:
        warnings.append(
            f"LOW_SAMPLE_WARNING: row count {row_count} is below recommended {RECOMMENDED_ROWS}."
        )

    contract_version = meta.get("contract_version") or meta.get("feature_contract_version")
    schema_hash = str(meta.get("schema_hash") or "")
    if contract_version != ML_CONTRACT_VERSION:
        blocking_reasons.append("Feature contract version does not match entry_quality_v2.")
    if schema_hash != ML_FEATURE_SCHEMA_HASH or not schema_hash.startswith("d4b19440"):
        blocking_reasons.append("Schema hash does not match the runtime v2 contract.")

    required_features = list(ML_FEATURE_COLUMNS)
    columns = set(frame.columns) if frame is not None else set()
    missing_features = [feature for feature in required_features if feature not in columns]
    if missing_features:
        blocking_reasons.append(f"Missing required v2 feature columns: {missing_features}")
    runtime_features = list(
        meta.get("runtime_feature_columns") or meta.get("feature_columns") or required_features
    )
    if runtime_features != required_features:
        blocking_reasons.append("Runtime feature count or ordering does not match the v2 contract.")

    declared_features = list(meta.get("feature_columns") or required_features)
    leakage_features = sorted(set(declared_features) & LEAKAGE_FIELDS)
    if leakage_features:
        blocking_reasons.append(f"Obvious leakage fields declared as features: {leakage_features}")
    if meta and meta.get("leakage_check_passed") is not True:
        blocking_reasons.append("Dataset builder leakage check did not pass.")

    target = next((column for column in TARGET_COLUMNS if column in columns), None)
    if target is None:
        blocking_reasons.append("No label/target column exists.")
    critical_columns = [column for column in required_features if column in columns]
    if target:
        critical_columns.append(target)
    usable_row_count = (
        int(len(frame.dropna(subset=critical_columns)))
        if frame is not None and critical_columns else 0
    )
    if usable_row_count == 0:
        blocking_reasons.append("Dataset is empty after dropping null-critical rows.")

    strict_filters = meta.get("strict_filters") or {}
    if strict_filters:
        if strict_filters.get("only_organic") is not True:
            blocking_reasons.append("Organic-only dataset filter is not confirmed.")
        if strict_filters.get("require_trace_id") is not True:
            blocking_reasons.append("Trace-id lineage filter is not confirmed.")
        if strict_filters.get("exclude_incomplete_labels") is not True:
            blocking_reasons.append("Incomplete-label exclusion is not confirmed.")
        if strict_filters.get("post_repair_only") != EXPECTED_POST_REPAIR_START:
            blocking_reasons.append("Post-repair cutoff does not match the approved cutoff.")

    source_counts = _source_counts(frame, meta)
    replay_rows = int(source_counts.get("replay", 0))
    organic_rows = int(source_counts.get("organic", 0) + source_counts.get("paper", 0))
    is_iofs_dataset = "iofs_score" in columns or "iofs_score" in set(declared_features)
    if replay_rows:
        warnings.append(
            f"RESEARCH_ONLY: dataset contains {replay_rows} replay rows; "
            "they cannot count as production organic rows."
        )
    if is_iofs_dataset and organic_rows < MINIMUM_ROWS:
        blocking_reasons.append(
            f"IOFS organic/paper row count {organic_rows} is below minimum {MINIMUM_ROWS}."
        )

    section4_status = _section4_status(section4_path)
    production_training_ready = (
        not blocking_reasons
        and row_count >= RECOMMENDED_ROWS
        and replay_rows == 0
        and section4_status.lower() == "passed"
    )
    if not production_training_ready:
        warnings.append(
            f"NOT_FOR_PRODUCTION: Section 4 status is {section4_status!r}; "
            "production training/promotion is blocked."
        )

    artifact_status = find_accepted_artifact(artifacts)
    latest_candidate_rejection_reasons = _latest_candidate_rejection_reasons(artifacts)
    active_env = _read_env(_BOT_ROOT / ".env")
    ml_enabled_remains_false = active_env.get("ML_ENABLED", "").lower() == "false"
    production_model_files = sorted(
        str(path) for path in (_BOT_ROOT / "models" / "production").glob("*.pkl")
    )
    ready_for_5b = artifact_status["accepted_model_artifact_exists"]
    if not ready_for_5b:
        warnings.append("SECTION_5B_BLOCKED: no accepted validated v2 .pkl artifact exists.")

    return {
        "ready_for_experimental_training": not blocking_reasons,
        "ready_for_production_training": production_training_ready,
        "ready_for_production_promotion": production_training_ready,
        "ready_for_5b_shadow_deployment": ready_for_5b,
        "deployment_status": "EXPERIMENTAL_ONLY",
        "row_count": row_count,
        "usable_row_count": usable_row_count,
        "organic_paper_row_count": organic_rows,
        "replay_row_count": replay_rows,
        "recommended_row_count_met": row_count >= RECOMMENDED_ROWS,
        "feature_contract_version": contract_version,
        "schema_hash": schema_hash,
        "leakage_check_passed": meta.get("leakage_check_passed") is True,
        "post_repair_cutoff": strict_filters.get("post_repair_only"),
        "section4_status": section4_status,
        "accepted_model_artifact": artifact_status,
        "latest_candidate_rejection_reasons": latest_candidate_rejection_reasons,
        "ml_enabled_remains_false": ml_enabled_remains_false,
        "production_model_files": production_model_files,
        "models_production_unchanged": not production_model_files,
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
    }


def find_accepted_artifact(artifacts_dir: Path) -> dict[str, Any]:
    for meta_path in sorted(
        artifacts_dir.glob("entry_quality_v2*_meta.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    ):
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        base = meta_path.name.removesuffix("_meta.json")
        model_path = meta_path.with_name(f"{base}.pkl")
        validation_path = meta_path.with_name(f"{base}_validation.json")
        try:
            validation = json.loads(validation_path.read_text(encoding="utf-8"))
        except Exception:
            validation = {}
        valid = (
            model_path.exists()
            and meta.get("accepted") is True
            and validation.get("accepted") is True
            and meta.get("contract_version") == ML_CONTRACT_VERSION
            and meta.get("schema_hash") == ML_FEATURE_SCHEMA_HASH
            and list(meta.get("feature_columns") or []) == list(ML_FEATURE_COLUMNS)
            and _artifact_passes_strong_gates(model_path, meta, validation)
        )
        if valid:
            return {
                "accepted_model_artifact_exists": True,
                "model_path": str(model_path),
                "meta_path": str(meta_path),
                "validation_path": str(validation_path),
            }
    return {
        "accepted_model_artifact_exists": False,
        "model_path": None,
        "meta_path": None,
        "validation_path": None,
    }


def _artifact_passes_strong_gates(
    model_path: Path,
    meta: dict[str, Any],
    validation: dict[str, Any],
) -> bool:
    aggregate = validation.get("lgbm_binary_aggregate") or meta.get("walk_forward_metrics") or {}
    auc = _number((aggregate.get("auc") or {}).get("mean"))
    auc_std = _number((aggregate.get("auc") or {}).get("std"))
    quartile_gap = _number((aggregate.get("quartile_diff_win") or {}).get("mean"))
    logistic_auc = _number(
        validation.get("logistic_baseline_auc") or meta.get("logistic_baseline_auc")
    )
    row_count = int(validation.get("row_count") or meta.get("row_count") or 0)
    improvement = _number(
        validation.get("minimum_baseline_auc_improvement")
        or meta.get("minimum_baseline_auc_improvement")
        or 0.01
    )
    metrics_pass = (
        auc is not None
        and 0.55 <= auc <= 0.90
        and auc_std is not None
        and auc_std <= 0.15
        and quartile_gap is not None
        and quartile_gap >= 0.05
        and logistic_auc is not None
        and improvement is not None
        and auc >= logistic_auc + improvement
        and row_count >= MINIMUM_ROWS
    )
    if not metrics_pass:
        return False
    try:
        model = joblib.load(model_path)
    except Exception:
        return False
    return callable(getattr(model, "predict_proba", None))


def render_section5b_blocker_report(result: dict[str, Any]) -> str:
    artifact = result["accepted_model_artifact"]
    blockers = list(result["blocking_reasons"])
    blockers.extend(result.get("latest_candidate_rejection_reasons") or [])
    if not artifact["accepted_model_artifact_exists"]:
        blockers.append("No accepted validated v2 .pkl artifact exists.")
    return "\n".join(
        [
            "# Section 5B Blocker Status",
            "",
            f"- Section 5B status: {'READY' if result['ready_for_5b_shadow_deployment'] else 'BLOCKED'}",
            f"- accepted_model_artifact_exists: {str(artifact['accepted_model_artifact_exists']).lower()}",
            f"- model_path: {artifact['model_path'] or 'None'}",
            f"- meta_path: {artifact['meta_path'] or 'None'}",
            f"- validation_status: {'accepted' if artifact['accepted_model_artifact_exists'] else 'rejected_or_missing'}",
            f"- blocking_reasons: {json.dumps(blockers)}",
            f"- ML_ENABLED remains false: {str(result['ml_enabled_remains_false']).lower()}",
            f"- models/production unchanged: {str(result['models_production_unchanged']).lower()}",
            "- next_retry_condition: dataset reaches at least 500 organic/paper rows or "
            "Section 4 produces enough closed IOFS paper trades, then a candidate must pass all gates.",
            "",
        ]
    )


def _source_counts(frame: pd.DataFrame | None, meta: dict[str, Any]) -> dict[str, int]:
    if frame is not None and "data_source" in frame.columns:
        return {str(key): int(value) for key, value in frame["data_source"].value_counts().items()}
    existing = meta.get("source_counts") or {}
    if existing:
        return {str(key): int(value) for key, value in existing.items()}
    if (meta.get("strict_filters") or {}).get("only_organic") is True:
        return {"organic": int(len(frame)) if frame is not None else int(meta.get("row_count") or 0)}
    return {}


def _latest_candidate_rejection_reasons(artifacts_dir: Path) -> list[str]:
    matches = sorted(
        artifacts_dir.glob("entry_quality_v2*_validation.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in matches:
        try:
            validation = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if validation.get("accepted") is not True:
            return [str(reason) for reason in validation.get("rejection_reasons") or []]
    return []


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _default_metadata_path(dataset_path: Path) -> Path:
    return dataset_path.with_name(f"{dataset_path.stem}_meta.json")


def _section4_status(path: Path | None) -> str:
    if path is None or not path.exists():
        return "Unknown"
    for line in path.read_text(encoding="utf-8").splitlines():
        normalized = line.strip().lstrip("-").strip()
        if normalized.lower().startswith("status:"):
            return normalized.split(":", 1)[1].strip()
    return "Unknown"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-path", required=True)
    parser.add_argument("--metadata-path")
    parser.add_argument("--section4-status-path")
    parser.add_argument("--artifacts-dir")
    parser.add_argument("--blocker-report")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = check_readiness(
        args.dataset_path,
        metadata_path=args.metadata_path,
        section4_status_path=args.section4_status_path,
        artifacts_dir=args.artifacts_dir,
    )
    print(json.dumps(result, indent=2))
    if args.blocker_report:
        path = Path(args.blocker_report)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_section5b_blocker_report(result), encoding="utf-8")
    return 0 if result["ready_for_experimental_training"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
