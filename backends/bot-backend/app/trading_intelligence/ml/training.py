"""Per-role training gates and deterministic, chronological, calibrated trainers.

Training is NEVER automatic because code exists. ``training_gate`` decides
per role; anything short of real, sufficient, leakage-controlled evidence is
``BLOCKED_EVIDENCE``. Synthetic fixtures may exercise the trainers in tests
(``allow_synthetic=True``) but the resulting card carries the synthetic
source kind and the promotion gate refuses it.

Trainers are deterministic (fixed seeds, single thread) and chronological:
train -> calibration -> holdout, each later in time, with a label-horizon
purge; probabilities are isotonic-calibrated (legacy ``app/ml`` wrapper,
reused) before any metric calls them probabilities.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import median
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .contracts import ModelCard, ModelRole, TrainingStatus
from .datasets import LABEL_SCHEMAS, MLDataset
from .features import FEATURE_SCHEMAS, FeatureContractViolation, matrix, validate_schema

#: RESEARCH_DEFAULT minimum real samples per role (never lowered to train)
MIN_SAMPLES: Mapping[str, int] = {ModelRole.OUTCOME.value: 500, ModelRole.RANKING.value: 500,
                                  ModelRole.REGIME.value: 500, ModelRole.OOD.value: 300,
                                  ModelRole.SLIPPAGE.value: 200, ModelRole.EXIT.value: 300}
MAX_MISSING_FRACTION = 0.05
SEED = 23023


@dataclass(frozen=True)
class TrainingGateResult:
    role: str
    status: str
    checks: Mapping[str, Any]
    reasons: Tuple[str, ...] = ()


def training_gate(role: str, dataset: Optional[MLDataset], *, certification_ready: bool, holdout_reserved: bool,
                  allow_synthetic: bool = False) -> TrainingGateResult:
    reasons: List[str] = []
    checks: Dict[str, Any] = {}
    if dataset is None or dataset.n == 0:
        return TrainingGateResult(role, TrainingStatus.BLOCKED_EVIDENCE.value, {"samples": 0}, ("NO_REAL_SAMPLES",))
    real = dataset.source_kind == "REAL_MARKET"
    checks["real_source"] = real
    if not real and not allow_synthetic:
        reasons.append("NON_REAL_SOURCE")
    checks["samples"] = dataset.n
    if dataset.n < MIN_SAMPLES[role]:
        reasons.append(f"SAMPLES_BELOW_{MIN_SAMPLES[role]}")
    try:
        validate_schema(FEATURE_SCHEMAS[role])
        checks["feature_schema_stable"] = True
    except FeatureContractViolation as exc:
        checks["feature_schema_stable"] = False
        reasons.append(f"FEATURE_SCHEMA_INVALID:{exc}")
    checks["chronological"] = list(dataset.times) == sorted(dataset.times)
    if not checks["chronological"]:
        reasons.append("NOT_CHRONOLOGICAL")
    cols = FEATURE_SCHEMAS[role].columns
    missing = {c: sum(1 for r in dataset.rows if r.get(c) is None) / dataset.n for c in cols}
    checks["max_missing_fraction"] = max(missing.values()) if missing else 0.0
    if checks["max_missing_fraction"] > MAX_MISSING_FRACTION:
        reasons.append("MISSINGNESS_TOO_HIGH")
    if role in (ModelRole.OUTCOME.value, ModelRole.RANKING.value, ModelRole.REGIME.value):
        regimes = {r.get("regime") for r in dataset.rows}
        families = {r.get("setup_family") for r in dataset.rows} - {None}
        checks["regime_coverage"], checks["setup_coverage"] = len(regimes), len(families)
        if len(regimes) < 2 or (role != ModelRole.REGIME.value and len(families) < 2):
            reasons.append("INSUFFICIENT_REGIME_OR_SETUP_COVERAGE")
    checks["holdout_reserved"] = holdout_reserved
    if not holdout_reserved:
        reasons.append("HOLDOUT_NOT_RESERVED")
    checks["section22_ready_for_ml"] = certification_ready
    if not certification_ready:
        reasons.append("SECTION_22_EVIDENCE_INSUFFICIENT_FOR_ML")
    if not reasons:
        return TrainingGateResult(role, TrainingStatus.READY_FOR_RESEARCH.value, checks, ())
    # a test fixture may exercise the trainer despite these (the card stays non-real => never promotable)
    fixture_tolerable = {"NON_REAL_SOURCE", "SECTION_22_EVIDENCE_INSUFFICIENT_FOR_ML", "HOLDOUT_NOT_RESERVED",
                         f"SAMPLES_BELOW_{MIN_SAMPLES[role]}"}
    if allow_synthetic and set(reasons) <= fixture_tolerable:
        return TrainingGateResult(role, TrainingStatus.READY_FOR_RESEARCH.value, checks, tuple(reasons))
    return TrainingGateResult(role, TrainingStatus.BLOCKED_EVIDENCE.value, checks, tuple(reasons))


# ------------------------------------------------------------------ estimators
class TabularEstimator:
    """Holds the training vocabulary so inference encodes identically."""

    def __init__(self, role: str, model: Any, names: Sequence[str], vocab: Mapping[str, Sequence[str]], kind: str):
        self.role, self.model, self.names, self.vocab, self.kind = role, model, list(names), dict(vocab), kind

    def _encode(self, rows: Sequence[Mapping[str, Any]]):
        schema = FEATURE_SCHEMAS[self.role]
        out = []
        for r in rows:
            row = []
            for c in schema.columns:
                if c in self.vocab:
                    row.extend(1.0 if str(r.get(c)) == v else 0.0 for v in self.vocab[c])
                else:
                    v = r.get(c)
                    row.append(float("nan") if v is None else float(v))
            out.append(row)
        return out

    def predict(self, rows: Sequence[Mapping[str, Any]]) -> List[Any]:
        X = self._encode(rows)
        if self.kind == "BINARY":
            return [float(p) for p in self.model.predict_proba(X)[:, 1]]
        return list(self.model.predict(X))


class RobustOODModel:
    """Per-feature median/MAD distance; the score only ever ADDS uncertainty.
    A missing value is maximal distance (never zero)."""

    def __init__(self, columns: Sequence[str], centers: Mapping[str, float], scales: Mapping[str, float]):
        self.columns, self.centers, self.scales = list(columns), dict(centers), dict(scales)

    def score(self, row: Mapping[str, Any]) -> float:
        zs = []
        for c in self.columns:
            v = row.get(c)
            if v is None:
                return 1.0
            zs.append(abs(float(v) - self.centers[c]) / (self.scales[c] or 1e-9))
        z = max(zs) if zs else 0.0
        return 1.0 - math.exp(-z / 3.0)

    def predict(self, rows):
        return [self.score(r) for r in rows]


def _lgbm(kind: str, params: Mapping[str, Any]):
    import lightgbm as lgb

    base = dict(n_estimators=200, learning_rate=0.05, num_leaves=15, min_child_samples=20, random_state=SEED,
                n_jobs=1, deterministic=True, force_col_wise=True, verbose=-1)
    base.update(params)
    return lgb.LGBMClassifier(**base) if kind in ("BINARY", "MULTICLASS") else lgb.LGBMRegressor(**base)


def train_estimator(role: str, dataset: MLDataset, *, horizon_ms: int, code_commit: Optional[str],
                    hyperparameters: Optional[Mapping[str, Any]] = None, model_version: str = "1.0.0"):
    """Returns (estimator, card-without-artifact-hash, metrics). Caller must
    have passed ``training_gate`` first."""
    from app.trading_intelligence.research.certification import stats

    label_schema = LABEL_SCHEMAS[role]
    feature_schema = FEATURE_SCHEMAS[role]
    params = dict(hyperparameters or {})
    train, holdout = dataset.chronological_split(horizon_ms=horizon_ms, holdout_fraction=0.2)
    fit, calib = train.chronological_split(horizon_ms=horizon_ms, holdout_fraction=0.25)
    metrics: Dict[str, Any] = {"n_fit": fit.n, "n_calibration": calib.n, "n_holdout": holdout.n}
    calibration: Dict[str, Any] = {"method": "NONE"}
    if role == ModelRole.OOD.value:
        cols = feature_schema.columns
        centers = {c: median(float(r[c]) for r in train.rows if r.get(c) is not None) for c in cols}
        scales = {c: median(abs(float(r[c]) - centers[c]) for r in train.rows if r.get(c) is not None) * 1.4826
                  for c in cols}
        est: Any = RobustOODModel(cols, centers, scales)
        metrics["holdout_mean_ood"] = sum(est.predict(holdout.rows)) / max(1, holdout.n)
    else:
        X, names, vocab = matrix(feature_schema, fit.rows)
        model = _lgbm(label_schema.kind, params)
        model.fit(X, list(fit.labels))
        est = TabularEstimator(role, model, names, vocab, label_schema.kind)
        if label_schema.kind == "BINARY":
            from sklearn.isotonic import IsotonicRegression

            from app.ml.calibrated_model import IsotonicCalibratedModel  # REUSED legacy infrastructure

            raw = TabularEstimator(role, model, names, vocab, "BINARY").predict(calib.rows)
            iso = IsotonicRegression(out_of_bounds="clip").fit(raw, list(calib.labels))
            est = TabularEstimator(role, IsotonicCalibratedModel(model, iso), names, vocab, "BINARY")
            calibration = {"method": "ISOTONIC_CHRONOLOGICAL", "n": calib.n}
            pairs = list(zip(est.predict(holdout.rows), holdout.labels))
            metrics["holdout_calibration"] = stats.calibration_summary(pairs, bins=10)
        elif label_schema.kind == "REGRESSION":
            preds = est.predict(holdout.rows)
            metrics["holdout_mae"] = (sum(abs(float(p) - float(y)) for p, y in zip(preds, holdout.labels))
                                      / max(1, holdout.n))
        else:
            preds = est.predict(holdout.rows)
            metrics["holdout_accuracy"] = sum(1 for p, y in zip(preds, holdout.labels) if p == y) / max(1, holdout.n)
    card = ModelCard(role=role, model_version=model_version, training_dataset_hash=dataset.dataset_hash,
                     feature_schema_hash=feature_schema.schema_hash, label_schema_hash=label_schema.schema_hash,
                     training_start=min(dataset.times), training_end=max(dataset.times), code_commit=code_commit,
                     hyperparameters={**params, "seed": SEED}, calibration=calibration,
                     source_kind=dataset.source_kind, metrics=metrics)
    return est, card, metrics


__all__ = ["training_gate", "train_estimator", "TrainingGateResult", "TabularEstimator", "RobustOODModel",
           "MIN_SAMPLES", "MAX_MISSING_FRACTION"]
