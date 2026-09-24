"""Section 23 estimator contracts: roles, hard boundaries, statuses, schemas,
immutable model identity."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional, Tuple

from app.trading_intelligence.hashing import short_id, stable_hash

CATI_ML_CONTRACT_VERSION = "cati_ml_estimator_v1"


class ModelRole(str, Enum):
    REGIME = "REGIME"
    OUTCOME = "OUTCOME"
    SLIPPAGE = "SLIPPAGE"
    RANKING = "RANKING"
    EXIT = "EXIT"
    OOD = "OOD"


class ModelStatus(str, Enum):
    RESEARCH = "RESEARCH"
    VALIDATED = "VALIDATED"
    SHADOW = "SHADOW"
    PROMOTION_ELIGIBLE = "PROMOTION_ELIGIBLE"
    PROMOTED = "PROMOTED"
    REJECTED = "REJECTED"
    RETIRED = "RETIRED"


class TrainingStatus(str, Enum):
    NOT_TRAINED = "NOT_TRAINED"
    BLOCKED_EVIDENCE = "BLOCKED_EVIDENCE"
    READY_FOR_RESEARCH = "READY_FOR_RESEARCH"


class OutcomeFamily(str, Enum):
    """Section 21 semantics -- never mixed inside one label."""
    MARKET = "MARKET"        # what the market path did (valid even if risk rejected the trade)
    EXECUTION = "EXECUTION"  # what the broker did (fills, slippage)
    ACCOUNT = "ACCOUNT"      # capital / slot / margin permission -- NEVER a market label
    POSITION = "POSITION"    # the path of an open position (exit intelligence)


#: allowed forward transitions; any status may go to REJECTED, PROMOTED -> RETIRED
STATUS_TRANSITIONS: Mapping[str, Tuple[str, ...]] = {
    ModelStatus.RESEARCH.value: (ModelStatus.VALIDATED.value,),
    ModelStatus.VALIDATED.value: (ModelStatus.SHADOW.value,),
    ModelStatus.SHADOW.value: (ModelStatus.PROMOTION_ELIGIBLE.value,),
    ModelStatus.PROMOTION_ELIGIBLE.value: (ModelStatus.PROMOTED.value, ModelStatus.SHADOW.value),
    ModelStatus.PROMOTED.value: (ModelStatus.RETIRED.value,),
    ModelStatus.REJECTED.value: (),
    ModelStatus.RETIRED.value: (),
}


@dataclass(frozen=True)
class RoleSpec:
    role: str
    estimates: Tuple[str, ...]
    label_family: str
    replaces_estimator: str
    hard_boundaries: Tuple[str, ...]
    fail_mode: str
    #: account-identifying state may be a feature only when the role requires it
    account_context_allowed: bool = False


ROLE_SPECS: Mapping[str, RoleSpec] = {
    ModelRole.REGIME.value: RoleSpec(
        ModelRole.REGIME.value, ("regime_distribution", "transition_probability"), OutcomeFamily.MARKET.value,
        "regime.engine.compute_regime_distribution",
        ("cannot bypass data-quality gating", "cannot bypass hard risk", "cannot manufacture unavailable features",
         "must expose uncertainty", "not a trade authority"), "FAIL_CLOSED_TO_DETERMINISTIC_OR_UNAVAILABLE"),
    ModelRole.OUTCOME.value: RoleSpec(
        ModelRole.OUTCOME.value, ("p_net_profitable", "net_R_distribution", "mfe_R", "mae_R", "time_to_event"),
        OutcomeFamily.MARKET.value, "forecast.engine.build_outcome_forecast",
        ("calibrated before any probability claim", "after-cost semantics explicit",
         "cannot bypass economic admission", "cannot bypass veto", "shadow first"), "DETERMINISTIC_FORECAST"),
    ModelRole.SLIPPAGE.value: RoleSpec(
        ModelRole.SLIPPAGE.value, ("expected_slippage_bps",), OutcomeFamily.EXECUTION.value,
        "venue.cost_model slippage term",
        ("labels from EXECUTION outcomes only", "can only raise the deterministic cost, never lower it",
         "executor remains authoritative for validation and execution", "cannot approve an invalid order"),
        "DETERMINISTIC_VENUE_COST"),
    ModelRole.RANKING.value: RoleSpec(
        ModelRole.RANKING.value, ("relative_opportunity_score",), OutcomeFamily.MARKET.value,
        "ranking.engine rank score",
        ("whole-universe semantics", "reorders ONLY veto-approved opportunities",
         "cannot reserve capital or slots", "portfolio constraints stay deterministic",
         "cannot bypass veto / economic admission"), "DETERMINISTIC_RANKING"),
    ModelRole.EXIT.value: RoleSpec(
        ModelRole.EXIT.value, ("remaining_edge_R", "p_stop", "p_target", "p_time_exit", "future_mfe_R",
                               "future_mae_R"), OutcomeFamily.POSITION.value, "position.forecast remaining-edge",
        ("cannot widen stop or risk beyond hard limits", "mechanical protection remains",
         "hard risk superior", "exit intelligence, not broker authority", "legacy V2 positions excluded"),
        "DETERMINISTIC_EXIT_POLICY"),
    ModelRole.OOD.value: RoleSpec(
        ModelRole.OOD.value, ("ood_score",), OutcomeFamily.MARKET.value, "forecast.ood distribution shift",
        ("may only increase uncertainty / abstention", "may never force a trade or increase risk",
         "missing data is never zero"), "ABSTAIN"),
}


@dataclass(frozen=True)
class FeatureSchema:
    name: str
    role: str
    columns: Tuple[str, ...]
    categorical: Tuple[str, ...] = ()
    source: str = "point_in_time_decision_features"
    version: str = "1.0.0"

    @property
    def schema_hash(self) -> str:
        return stable_hash({"name": self.name, "role": self.role, "columns": list(self.columns),
                            "categorical": list(self.categorical), "source": self.source, "version": self.version,
                            "contract": CATI_ML_CONTRACT_VERSION})


@dataclass(frozen=True)
class LabelSchema:
    name: str
    role: str
    family: str
    target: str
    kind: str  # BINARY | REGRESSION | MULTICLASS | DENSITY
    version: str = "1.0.0"

    @property
    def schema_hash(self) -> str:
        return stable_hash({"name": self.name, "role": self.role, "family": self.family, "target": self.target,
                            "kind": self.kind, "version": self.version, "contract": CATI_ML_CONTRACT_VERSION})


@dataclass(frozen=True)
class ModelCard:
    """Everything that identifies one immutable estimator artifact. Changing
    features, labels, hyperparameters, data, calibration or code changes the
    model_id; a promoted artifact is never mutated in place."""

    role: str
    model_version: str
    training_dataset_hash: str
    feature_schema_hash: str
    label_schema_hash: str
    training_start: int
    training_end: int
    code_commit: Optional[str]
    hyperparameters: Mapping[str, Any]
    calibration: Mapping[str, Any]
    source_kind: str
    supported_asset_classes: Tuple[str, ...] = ("CRYPTO",)
    supported_venues: Tuple[str, ...] = ()
    supported_timeframes: Tuple[str, ...] = ("15m",)
    supported_setup_families: Tuple[str, ...] = ()
    ood_support: bool = True
    artifact_hash: str = ""
    contract_version: str = CATI_ML_CONTRACT_VERSION
    metrics: Mapping[str, Any] = field(default_factory=dict)

    def identity(self) -> dict:
        return {"role": self.role, "model_version": self.model_version,
                "training_dataset_hash": self.training_dataset_hash, "feature_schema_hash": self.feature_schema_hash,
                "label_schema_hash": self.label_schema_hash, "training_start": self.training_start,
                "training_end": self.training_end, "code_commit": self.code_commit,
                "hyperparameters": dict(self.hyperparameters), "calibration": dict(self.calibration),
                "source_kind": self.source_kind, "contract_version": self.contract_version,
                "artifact_hash": self.artifact_hash}

    @property
    def model_id(self) -> str:
        return short_id(f"mdl_{self.role.lower()}", self.identity())

    def to_dict(self) -> dict:
        return {"model_id": self.model_id, **self.identity(), "metrics": dict(self.metrics),
                "supported_asset_classes": list(self.supported_asset_classes),
                "supported_venues": list(self.supported_venues),
                "supported_timeframes": list(self.supported_timeframes),
                "supported_setup_families": list(self.supported_setup_families), "ood_support": self.ood_support}


class LegacyArtifactRejected(ValueError):
    """A legacy V2 (or otherwise non-CATI) artifact tried to load as a CATI estimator."""


__all__ = ["CATI_ML_CONTRACT_VERSION", "ModelRole", "ModelStatus", "TrainingStatus", "OutcomeFamily",
           "STATUS_TRANSITIONS", "RoleSpec", "ROLE_SPECS", "FeatureSchema", "LabelSchema", "ModelCard",
           "LegacyArtifactRejected"]
