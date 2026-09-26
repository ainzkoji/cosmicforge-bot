"""Frozen FX reference universe (Section 10.9 / 12.13).

Independent of the crypto universe (own schema, own hash). The identity is
what defines the dataset -- membership, provider, price kind, requested
window, base resolution, exclusions with reasons and the policy versions for
scale QA, gaps and resampling. Acquired ranges and row counts are NOT
identity (they grow while acquisition runs); they live in the coverage
artifact. ``generated_at`` and ``code_commit`` are operational metadata.

``holdout_opened`` is recorded false and nothing here reserves or reads a
holdout; ``execution_authorized`` is false -- reference data is never an
execution price.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Mapping, Optional, Sequence

FX_UNIVERSE_SCHEMA_VERSION = "frozen-fx-universe-manifest-v1"
FX_CANDIDATE_RULE_VERSION = "fx-candidates-g10-plus-usd-eur-crosses-v1"
FX_RESAMPLING_POLICY_VERSION = "fx-resample-from-1m-bid-ask-v1"
_IDENTITY = ("schema_version", "role", "asset_class", "provider", "price_kind", "candidate_rule_version",
             "base_resolution", "window_start_ms", "window_end_ms", "members", "excluded", "bid_ask_required",
             "scale_qa_version", "gap_policy_version", "resampling_policy_version", "source_versions")


class FrozenFxUniverseError(ValueError):
    pass


def _hash(identity: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps({k: identity[k] for k in _IDENTITY}, sort_keys=True,
                                     default=str).encode()).hexdigest()


def build_fx_universe_manifest(*, provider: str, members: Sequence[Mapping[str, Any]],
                               excluded: Mapping[str, str], window_start_ms: int, window_end_ms: int,
                               scale_qa_version: str, gap_policy_version: str, source_versions: Sequence[str],
                               generated_at: str, code_commit: Optional[str], role: str = "CERTIFICATION_UNIVERSE",
                               base_resolution: str = "1m") -> Dict[str, Any]:
    """``members``: [{pair, base, quote, scale_status, cross_rate_status}] -- a member must be scale-verified."""
    bad = [m["pair"] for m in members if str(m.get("scale_status", "")).startswith("SCALE_BREAK")]
    if bad:
        raise FrozenFxUniverseError(f"members with unresolved scale breaks cannot be frozen: {bad}")
    identity = {
        "schema_version": FX_UNIVERSE_SCHEMA_VERSION, "role": role, "asset_class": "FX", "provider": provider,
        "price_kind": "REFERENCE_MARKET_PRICE", "candidate_rule_version": FX_CANDIDATE_RULE_VERSION,
        "base_resolution": base_resolution, "window_start_ms": int(window_start_ms),
        "window_end_ms": int(window_end_ms),
        "members": sorted(({k: m[k] for k in ("pair", "base", "quote", "scale_status", "cross_rate_status")}
                           for m in members), key=lambda m: m["pair"]),
        "excluded": dict(sorted(excluded.items())), "bid_ask_required": True,
        "scale_qa_version": scale_qa_version, "gap_policy_version": gap_policy_version,
        "resampling_policy_version": FX_RESAMPLING_POLICY_VERSION, "source_versions": sorted(source_versions),
    }
    h = _hash(identity)
    return {**identity, "universe_id": f"univ_fx_{provider}_{h[:12]}", "universe_hash": h,
            "generated_at": generated_at, "code_commit": code_commit, "holdout_opened": False,
            "execution_authorized": False,
            "note": "reference-market research/certification universe; never an execution price or authority"}


def verify_fx_universe(manifest: Mapping[str, Any]) -> str:
    missing = [k for k in _IDENTITY if k not in manifest]
    if missing:
        raise FrozenFxUniverseError(f"FX universe manifest missing {missing}")
    h = _hash(manifest)
    if h != manifest.get("universe_hash"):
        raise FrozenFxUniverseError(f"FX universe hash mismatch: recorded {manifest.get('universe_hash')} computed {h}")
    if manifest.get("holdout_opened") is not False or manifest.get("execution_authorized") is not False:
        raise FrozenFxUniverseError("a frozen FX universe never records an opened holdout or execution authority")
    return h


def load_fx_universe(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        m = json.load(fh)
    verify_fx_universe(m)
    return m


__all__ = ["FX_CANDIDATE_RULE_VERSION", "FX_RESAMPLING_POLICY_VERSION", "FX_UNIVERSE_SCHEMA_VERSION",
           "FrozenFxUniverseError", "build_fx_universe_manifest", "load_fx_universe", "verify_fx_universe"]
