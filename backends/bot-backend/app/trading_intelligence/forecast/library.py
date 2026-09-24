"""Frozen/versioned HistoricalOutcomeLibrary (Section 12.4).

Immutable: changing membership, label policy, cost model, or bucket schema
produces a *different* library (a new ``library_hash``), never an in-place
mutation of an existing one's semantics.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Tuple

from app.trading_intelligence.contracts.forecast import SetupOutcomeLabel
from app.trading_intelligence.hashing import stable_hash
from app.trading_intelligence.versions import COHORT_SCHEMA_VERSION, OUTCOME_LIBRARY_SCHEMA_VERSION


@dataclass(frozen=True)
class LibraryRow:
    """A labeled analog plus the cohort dimensions it was tagged with at
    build time (Section 12.5) -- kept separate from ``SetupOutcomeLabel``
    itself, which is the label contract, not a cohort-indexing concern."""

    label: SetupOutcomeLabel
    cohort_dimensions: Mapping[str, str]
    #: A few continuous SetupCandidate-time values (e.g. room_to_target_R)
    #: kept alongside the label so OOD's continuous z-score has genuine
    #: reference data (Section 12.13) instead of only bucketed categories.
    continuous_features: Mapping[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class HistoricalOutcomeLibrary:
    library_id: str
    rows: Tuple[LibraryRow, ...]
    dataset_source_hash: str
    candidate_generation_versions: Mapping[str, str]  # setup_family -> setup_version
    label_policy_version: str
    cost_model_version: str
    feature_bucket_schema_version: str = COHORT_SCHEMA_VERSION
    schema_version: str = OUTCOME_LIBRARY_SCHEMA_VERSION
    #: An assessment ABOUT the library (produced by a calibration report +
    #: versioned policy), not analytical content -- deliberately excluded
    #: from ``library_hash`` so attaching a status never changes identity.
    #: Defaults to UNCALIBRATED: existence is not calibration.
    calibration_status: str = "UNCALIBRATED"
    #: HistoricalLibrarySourceKind (forecast/source.py). Part of identity:
    #: the same rows labelled SYNTHETIC_TEST and REAL_MARKET are different
    #: libraries, so a classification cannot be swapped without a new hash.
    source_kind: str = "UNKNOWN"

    @property
    def library_hash(self) -> str:
        payload = {
            "source_kind": self.source_kind,
            "dataset_source_hash": self.dataset_source_hash,
            "candidate_generation_versions": dict(self.candidate_generation_versions),
            "label_policy_version": self.label_policy_version,
            "cost_model_version": self.cost_model_version,
            "feature_bucket_schema_version": self.feature_bucket_schema_version,
            "schema_version": self.schema_version,
            "row_label_ids": sorted(r.label.label_id for r in self.rows),
            # Labels can differ (different future data / costs) while their
            # ids coincide, so row CONTENT is part of identity too.
            "rows_content_hash": self.rows_content_hash,
        }
        return stable_hash(payload)

    @property
    def rows_content_hash(self) -> str:
        from app.trading_intelligence.forecast.artifact import row_to_dict

        return stable_hash(sorted((row_to_dict(r) for r in self.rows), key=lambda d: d["label"]["label_id"]))

    @property
    def library_version(self) -> str:
        return self.schema_version

    def __len__(self) -> int:
        return len(self.rows)

    @classmethod
    def build(
        cls,
        rows: Tuple[LibraryRow, ...],
        *,
        dataset_source_hash: str,
        candidate_generation_versions: Mapping[str, str],
        label_policy_version: str,
        cost_model_version: str,
        feature_bucket_schema_version: str = COHORT_SCHEMA_VERSION,
        source_kind: str = "UNKNOWN",
    ) -> "HistoricalOutcomeLibrary":
        identity_seed = {
            "dataset_source_hash": dataset_source_hash,
            "n_rows": len(rows),
            "label_policy_version": label_policy_version,
        }
        library_id = f"lib_{stable_hash(identity_seed)[:24]}"
        return cls(
            library_id=library_id,
            rows=tuple(rows),
            dataset_source_hash=dataset_source_hash,
            candidate_generation_versions=dict(candidate_generation_versions),
            label_policy_version=label_policy_version,
            cost_model_version=cost_model_version,
            feature_bucket_schema_version=feature_bucket_schema_version,
            source_kind=source_kind,
        )


def empty_library(*, label_policy_version: str, cost_model_version: str) -> HistoricalOutcomeLibrary:
    """An explicitly empty, valid library -- distinct from
    OUTCOME_LIBRARY_UNAVAILABLE (no library object at all). Cohort lookup
    against this naturally yields zero support at every backoff level."""
    return HistoricalOutcomeLibrary.build(
        (), dataset_source_hash="empty", candidate_generation_versions={},
        label_policy_version=label_policy_version, cost_model_version=cost_model_version,
    )


__all__ = ["LibraryRow", "HistoricalOutcomeLibrary", "empty_library"]
