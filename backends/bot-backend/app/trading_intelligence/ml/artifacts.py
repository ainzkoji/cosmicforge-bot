"""Immutable CATI estimator artifacts (reuses the joblib pattern of legacy
``app/ml``; the isotonic wrapper lives in ``app/ml/calibrated_model``).

Layout: ``<dir>/<model_id>/model.joblib`` + ``card.json``. ``artifact_hash``
is the sha256 of the model bytes and is part of the model identity, so an
artifact cannot be swapped under an existing card. Loading re-verifies the
contract version, role, schema hashes and bytes, and REJECTS any legacy V2
artifact. Binaries are never committed to git and never stored in SQLite.
"""
from __future__ import annotations

import dataclasses
import hashlib
import io
import json
from pathlib import Path
from typing import Any, Tuple

from .contracts import CATI_ML_CONTRACT_VERSION, LegacyArtifactRejected, ModelCard
from .legacy import is_legacy_v2_artifact


def _bytes(model: Any) -> bytes:
    import joblib

    buf = io.BytesIO()
    joblib.dump(model, buf)
    return buf.getvalue()


def save_artifact(root: Path, model: Any, card: ModelCard) -> Tuple[Path, ModelCard]:
    blob = _bytes(model)
    card = dataclasses.replace(card, artifact_hash=hashlib.sha256(blob).hexdigest())
    d = Path(root) / card.model_id
    if d.exists():
        existing = json.loads((d / "card.json").read_text(encoding="utf-8"))
        if existing.get("artifact_hash") != card.artifact_hash:
            raise ValueError(f"model {card.model_id} exists with different bytes (artifacts are immutable)")
        return d, card
    d.mkdir(parents=True)
    (d / "model.joblib").write_bytes(blob)
    (d / "card.json").write_text(json.dumps(card.to_dict(), sort_keys=True, indent=2, default=str), encoding="utf-8")
    return d, card


def load_artifact(directory: Path, *, expected_role: str, feature_schema_hash: str, label_schema_hash: str):
    import joblib

    d = Path(directory)
    meta_path = d / "card.json"
    if not meta_path.exists():
        # a V2 artifact directory carries metadata.json with V2 contract fields
        v2 = d / "metadata.json"
        if v2.exists() and is_legacy_v2_artifact(json.loads(v2.read_text(encoding="utf-8"))):
            raise LegacyArtifactRejected("legacy V2 entry-scorer artifact cannot load as a CATI estimator")
        raise LegacyArtifactRejected("not a CATI estimator artifact (no card.json)")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if is_legacy_v2_artifact(meta) or meta.get("contract_version") != CATI_ML_CONTRACT_VERSION:
        raise LegacyArtifactRejected(f"contract {meta.get('contract_version')!r} is not {CATI_ML_CONTRACT_VERSION}")
    if meta.get("role") != expected_role:
        raise LegacyArtifactRejected(f"artifact role {meta.get('role')} != {expected_role}")
    if meta.get("feature_schema_hash") != feature_schema_hash or meta.get("label_schema_hash") != label_schema_hash:
        raise LegacyArtifactRejected("feature/label schema hash mismatch")
    blob = (d / "model.joblib").read_bytes()
    if hashlib.sha256(blob).hexdigest() != meta.get("artifact_hash"):
        raise ValueError("artifact bytes do not match artifact_hash (integrity failure)")
    return joblib.load(io.BytesIO(blob)), meta


__all__ = ["save_artifact", "load_artifact"]
