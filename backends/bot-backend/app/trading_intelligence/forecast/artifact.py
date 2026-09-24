"""Immutable HistoricalOutcomeLibrary artifact: writer + VERIFIED loader
(Section 12.15 A8-A10).

Layout (compact, deterministic, no extra dependency)::

    <output_dir>/cati_lib_<hash24>/manifest.json   # identity + provenance
    <output_dir>/cati_lib_<hash24>/rows.jsonl      # one canonical JSON row per line
    <output_dir>/cati_lib_<hash24>/calibration.json  # optional, written separately

The directory name IS the analytical identity (derived from the library
hash). There is no ``latest`` alias: a different library can never be stored
under the same identity, and any hash mismatch fails loading closed -- no
partial best-effort loading ever reaches Section 13.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple

from app.trading_intelligence.contracts.forecast import SetupOutcomeLabel
from app.trading_intelligence.contracts.instrument import InstrumentKey
from app.trading_intelligence.forecast.library import HistoricalOutcomeLibrary, LibraryRow
from app.trading_intelligence.forecast.source import (
    SOURCE_POLICY_VERSION, LibraryLoadMode, RUNTIME_TRUSTED_SOURCE_KINDS, allowed_source_kinds,
)
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.versions import (
    COHORT_SCHEMA_VERSION,
    LABEL_POLICY_VERSION,
    LIBRARY_ARTIFACT_SCHEMA_VERSION,
    MARKET_STATE_ENGINE_VERSION,
    MARKET_STATE_SCHEMA_VERSION,
    OUTCOME_LIBRARY_SCHEMA_VERSION,
    REGIME_MODEL_VERSION,
)

MANIFEST_FILE = "manifest.json"
ROWS_FILE = "rows.jsonl"
CALIBRATION_FILE = "calibration.json"

REQUIRED_MANIFEST_FIELDS: Tuple[str, ...] = (
    "artifact_schema_version", "library_id", "library_version", "library_hash", "rows_sha256",
    # source provenance (pre-Section-17 closure)
    "source_kind", "source_provider", "source_dataset_id", "source_hash", "source_policy_version",
    "venue", "start_time", "end_time", "row_count", "label_count",
    "source_data_hash", "source_range", "market_state_schema_version", "market_state_engine_version",
    "regime_model_version", "regime_policy_hash", "setup_family_versions", "setup_policy_hashes",
    "label_policy_version", "cost_model_version", "cohort_schema_version", "ood_feature_schema_version",
    "created_from_start", "created_from_end", "symbols", "timeframe", "candidate_count", "labeled_count",
    "skipped_count", "counts_by_setup_family", "counts_by_side", "counts_by_regime", "label_quality_counts",
    "manifest_hash",
)


class LibraryArtifactError(RuntimeError):
    """The artifact cannot be trusted; loading must fail closed."""


# -- row (de)serialization ----------------------------------------------------
def row_to_dict(row: LibraryRow) -> Dict[str, Any]:
    label = asdict(row.label)
    return {
        "label": label,
        "cohort_dimensions": dict(row.cohort_dimensions),
        "continuous_features": dict(row.continuous_features),
    }


def row_from_dict(d: Mapping[str, Any]) -> LibraryRow:
    label = dict(d["label"])
    label["instrument_key"] = InstrumentKey(**label["instrument_key"])
    label["reason_codes"] = tuple(label.get("reason_codes", ()))
    return LibraryRow(
        label=SetupOutcomeLabel(**label),
        cohort_dimensions=dict(d["cohort_dimensions"]),
        continuous_features=dict(d.get("continuous_features", {})),
    )


def _canonical_line(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def _ordered_rows(library: HistoricalOutcomeLibrary):
    return sorted(library.rows, key=lambda r: r.label.label_id)


def _rows_text(library: HistoricalOutcomeLibrary) -> str:
    return "".join(_canonical_line(row_to_dict(r)) + "\n" for r in _ordered_rows(library))


def library_identity(library: HistoricalOutcomeLibrary) -> str:
    return f"cati_lib_{library.library_hash[:24]}"


def manifest_hash_of(manifest: Mapping[str, Any]) -> str:
    return stable_hash({k: v for k, v in manifest.items() if k != "manifest_hash"})


# -- write ----------------------------------------------------------------------
def write_library_artifact(
    library: HistoricalOutcomeLibrary,
    output_dir: os.PathLike,
    *,
    provenance: Mapping[str, Any],
) -> Path:
    """Write the immutable artifact. ``provenance`` supplies the manifest
    fields the library object itself does not carry (source hash/range,
    policy hashes, counts, ...). Refuses to overwrite a DIFFERENT library
    stored under the same identity."""
    rows_text = _rows_text(library)
    rows_sha = stable_hash(rows_text)
    manifest: Dict[str, Any] = {
        "artifact_schema_version": LIBRARY_ARTIFACT_SCHEMA_VERSION,
        "library_id": library_identity(library),
        "library_version": library.library_version,
        "library_hash": library.library_hash,
        "rows_sha256": rows_sha,
        "source_data_hash": library.dataset_source_hash,
        "market_state_schema_version": MARKET_STATE_SCHEMA_VERSION,
        "market_state_engine_version": MARKET_STATE_ENGINE_VERSION,
        "regime_model_version": REGIME_MODEL_VERSION,
        "setup_family_versions": dict(library.candidate_generation_versions),
        "label_policy_version": library.label_policy_version,
        "cost_model_version": library.cost_model_version,
        "cohort_schema_version": library.feature_bucket_schema_version,
        "row_count": len(library.rows),
        "label_count": len(library.rows),
        "source_kind": library.source_kind,
        "source_hash": library.dataset_source_hash,
        "source_policy_version": SOURCE_POLICY_VERSION,
    }
    manifest.update(dict(provenance))
    # Identity fields always come from the library itself, never from
    # caller-supplied provenance (which could otherwise contradict them).
    manifest.update({
        "source_kind": library.source_kind, "source_hash": library.dataset_source_hash,
        "source_data_hash": library.dataset_source_hash, "library_hash": library.library_hash,
        "row_count": len(library.rows), "label_count": len(library.rows),
    })
    manifest["manifest_hash"] = manifest_hash_of(manifest)
    missing = [f for f in REQUIRED_MANIFEST_FIELDS if f not in manifest]
    if missing:
        raise LibraryArtifactError(f"manifest is missing required fields: {missing}")

    target = Path(output_dir) / manifest["library_id"]
    manifest_text = json.dumps(manifest, sort_keys=True, indent=2, default=str)
    if target.exists():
        existing = target / MANIFEST_FILE
        existing_rows = target / ROWS_FILE
        if existing.exists() and existing_rows.exists() and \
                json.loads(existing.read_text(encoding="utf-8")).get("manifest_hash") == manifest["manifest_hash"] and \
                existing_rows.read_text(encoding="utf-8") == rows_text:
            return target  # identical artifact already stored: idempotent
        raise LibraryArtifactError(f"refusing to overwrite a different library stored under identity {manifest['library_id']}")
    target.mkdir(parents=True, exist_ok=False)
    (target / ROWS_FILE).write_text(rows_text, encoding="utf-8", newline="\n")
    (target / MANIFEST_FILE).write_text(manifest_text, encoding="utf-8", newline="\n")
    return target


# -- verified load ------------------------------------------------------------------
def load_library_artifact(
    path: os.PathLike,
    *,
    expected_hash: Optional[str] = None,
    mode: str = LibraryLoadMode.RUNTIME.value,
) -> Tuple[HistoricalOutcomeLibrary, Dict[str, Any]]:
    """Load + fully verify. Raises ``LibraryArtifactError`` on ANY problem.

    ``mode`` defaults to RUNTIME, which accepts only REAL_MARKET /
    REPLAY_CAPTURE sources. SYNTHETIC_TEST, FIXTURE_TEST and UNKNOWN
    libraries load only with an explicit ``mode="TEST"``/``"DEVELOPMENT"``."""
    root = Path(path)
    mpath, rpath = root / MANIFEST_FILE, root / ROWS_FILE
    if not mpath.is_file() or not rpath.is_file():
        raise LibraryArtifactError(f"not a library artifact directory: {root}")
    try:
        manifest = json.loads(mpath.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise LibraryArtifactError(f"unreadable manifest: {exc}") from exc

    missing = [f for f in REQUIRED_MANIFEST_FIELDS if f not in manifest]
    if missing:
        raise LibraryArtifactError(f"manifest missing required fields: {missing}")
    if manifest_hash_of(manifest) != manifest["manifest_hash"]:
        raise LibraryArtifactError("manifest hash mismatch (manifest was altered)")

    source_kind = str(manifest["source_kind"])
    if source_kind not in allowed_source_kinds(mode):
        raise LibraryArtifactError(
            f"SOURCE_KIND_NOT_TRUSTED: library source_kind={source_kind} is not accepted in mode={mode} "
            f"(runtime accepts only {sorted(RUNTIME_TRUSTED_SOURCE_KINDS)})")
    if manifest["source_hash"] != manifest["source_data_hash"]:
        raise LibraryArtifactError("source hash fields disagree")

    compat = {
        "artifact_schema_version": LIBRARY_ARTIFACT_SCHEMA_VERSION,
        "label_policy_version": LABEL_POLICY_VERSION,
        "cohort_schema_version": COHORT_SCHEMA_VERSION,
        "market_state_schema_version": MARKET_STATE_SCHEMA_VERSION,
        "market_state_engine_version": MARKET_STATE_ENGINE_VERSION,
        "regime_model_version": REGIME_MODEL_VERSION,
        "library_version": OUTCOME_LIBRARY_SCHEMA_VERSION,
    }
    for key, supported in compat.items():
        if manifest[key] != supported:
            raise LibraryArtifactError(f"incompatible {key}: artifact={manifest[key]!r} supported={supported!r}")
    _check_candidate_versions(manifest)
    if not str(manifest.get("cost_model_version") or ""):
        raise LibraryArtifactError("cost_model_version is required")

    rows_text = rpath.read_text(encoding="utf-8")
    if stable_hash(rows_text) != manifest["rows_sha256"]:
        raise LibraryArtifactError("rows hash mismatch (row data was altered)")
    try:
        rows = tuple(row_from_dict(json.loads(line)) for line in rows_text.splitlines() if line.strip())
    except (KeyError, TypeError, ValueError) as exc:
        raise LibraryArtifactError(f"malformed row data: {exc}") from exc
    if len(rows) != int(manifest["row_count"]) or len(rows) != int(manifest["label_count"]):
        raise LibraryArtifactError("row/label count disagrees with manifest")
    _check_rows(rows, manifest)

    library = HistoricalOutcomeLibrary.build(
        rows, dataset_source_hash=manifest["source_data_hash"],
        candidate_generation_versions=manifest["setup_family_versions"],
        label_policy_version=manifest["label_policy_version"], cost_model_version=manifest["cost_model_version"],
        feature_bucket_schema_version=manifest["cohort_schema_version"], source_kind=source_kind,
    )
    if library.library_hash != manifest["library_hash"]:
        raise LibraryArtifactError("library hash mismatch (recomputed hash differs from manifest)")
    if expected_hash is not None and library.library_hash != expected_hash:
        raise LibraryArtifactError(f"library hash does not match the configured expected hash ({expected_hash[:12]}...)")

    calibration_status = _load_calibration_status(root, library)
    import dataclasses

    return dataclasses.replace(library, calibration_status=calibration_status), manifest


_REQUIRED_ROW_FIELDS = ("label_id", "setup_candidate_id", "setup_family", "terminal_outcome", "label_quality",
                        "cost_model_version", "label_policy_version", "decision_time")
_REQUIRED_COHORT_FIELDS = ("setup_family", "side")


def _check_candidate_versions(manifest: Mapping[str, Any]) -> None:
    """The library's setup specialists must be the ones the runtime runs:
    evidence about a different candidate definition is not evidence."""
    from app.trading_intelligence.setups.registry import SPECIALIST_REGISTRY

    current = {fam: spec.setup_version for fam, spec in SPECIALIST_REGISTRY.items()}
    built = dict(manifest.get("setup_family_versions") or {})
    stale = sorted(f for f, v in built.items() if current.get(f) != v)
    if stale:
        raise LibraryArtifactError(f"incompatible candidate policy versions for families: {stale}")


def _check_rows(rows: Tuple[LibraryRow, ...], manifest: Mapping[str, Any]) -> None:
    for row in rows:
        label = row.label
        for f in _REQUIRED_ROW_FIELDS:
            if getattr(label, f, None) in (None, ""):
                raise LibraryArtifactError(f"row {getattr(label, 'label_id', '?')} is missing required field {f}")
        for f in _REQUIRED_COHORT_FIELDS:
            if not row.cohort_dimensions.get(f):
                raise LibraryArtifactError(f"row {label.label_id} is missing cohort dimension {f}")
        if label.cost_model_version != manifest["cost_model_version"]:
            raise LibraryArtifactError(f"row {label.label_id} cost model version disagrees with manifest")
        if label.label_policy_version != manifest["label_policy_version"]:
            raise LibraryArtifactError(f"row {label.label_id} label policy version disagrees with manifest")


def default_calibration_status(source_kind: str) -> str:
    """Without a calibration report: real evidence is RESEARCH_ONLY (usable
    for research, never CALIBRATED); non-real evidence is UNCALIBRATED."""
    return "RESEARCH_ONLY" if source_kind in RUNTIME_TRUSTED_SOURCE_KINDS else "UNCALIBRATED"


def _load_calibration_status(root: Path, library: HistoricalOutcomeLibrary) -> str:
    """Status is RE-DERIVED from the stored report + policy, never trusted
    from a bare assertion in the file. A report that cannot be verified
    => UNCALIBRATED. Loading never makes a library CALIBRATED by itself."""
    cpath = root / CALIBRATION_FILE
    if not cpath.is_file():
        return default_calibration_status(library.source_kind)
    try:
        from app.trading_intelligence.forecast.calibration_report import status_from_stored_record

        return status_from_stored_record(json.loads(cpath.read_text(encoding="utf-8")), library_hash=library.library_hash)
    except Exception:
        return "UNCALIBRATED"


__all__ = [
    "MANIFEST_FILE", "ROWS_FILE", "CALIBRATION_FILE", "REQUIRED_MANIFEST_FIELDS", "LibraryArtifactError",
    "row_to_dict", "row_from_dict", "library_identity", "manifest_hash_of",
    "write_library_artifact", "load_library_artifact", "default_calibration_status",
]
