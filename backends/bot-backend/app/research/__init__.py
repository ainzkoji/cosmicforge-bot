"""Research data contract and dataset tooling (Phase 14).

Kept separate from ``app.replay``: replay is about reproducing the *system*,
this is about the *data* the system is measured on. They meet only where a
replay is handed a dataset.
"""

from app.research.dataset import (
    FINAL_HOLDOUT,
    REAL_HISTORICAL,
    DatasetError,
    DatasetManifest,
    FinalHoldoutViolation,
    Partition,
    QualityReport,
    assess_quality,
    build_manifest,
    derive,
    guard_final_holdout,
    partition,
)

__all__ = [
    "DatasetError",
    "DatasetManifest",
    "FINAL_HOLDOUT",
    "FinalHoldoutViolation",
    "Partition",
    "QualityReport",
    "REAL_HISTORICAL",
    "assess_quality",
    "build_manifest",
    "derive",
    "guard_final_holdout",
    "partition",
]
