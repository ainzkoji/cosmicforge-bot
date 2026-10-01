"""scripts/verify_governed_library.py: the 8 checks a governed library must pass before it is pinned."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest
from _cert import SYNTHETIC_SOURCES, meta, series

from app.trading_intelligence.research.certification.freeze import build_policy_freeze
from app.trading_intelligence.research.certification.pipeline import certify
from app.trading_intelligence.research.certification.policy import canonical_certification_policy
from app.trading_intelligence.research.certification.registry import SqliteResearchStore
from app.trading_intelligence.research.certification.replay import ReplayConfig

REPO = Path(__file__).resolve().parents[4]
DATASET = REPO / "docs" / "research" / "datasets" / "crypto_broad_binance_v1.dataset.json"


def _verifier():
    spec = importlib.util.spec_from_file_location("verify_governed_library", REPO / "scripts" / "verify_governed_library.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def governed(tmp_path_factory):
    root = tmp_path_factory.mktemp("gov")
    committed = json.loads(DATASET.read_text(encoding="utf-8"))
    cfg = ReplayConfig(symbols=("BTCUSDT", "ETHUSDT"), timeframe="15m", label_horizon_bars=24, warmup_bars=250, folds=3)
    certify(series(n=620), meta(), cfg=cfg, data_sources=SYNTHETIC_SOURCES, source_provider="synthetic",
            dataset_identity="synthetic", research_db=SqliteResearchStore(str(root / "r.db")), artifact_dir=root / "art",
            freeze=build_policy_freeze(certification_policy=canonical_certification_policy(), source_commit="abc123",
                                       source_tree_dirty=False),
            library_output=root / "libs",
            library_governance={"dataset_manifest_hash": committed["manifest_hash"],
                                "universe_hash": committed["universe_hash"]})
    (lib,) = [p for p in (root / "libs").iterdir() if p.is_dir()]
    return lib


def test_all_eight_checks_pass_for_a_correctly_governed_library(governed):
    out = _verifier().verify(governed, DATASET, mode="TEST")
    assert out["verdict"] == "VERIFIED", out["checks"]
    assert len(out["checks"]) == 8 and out["governance"]["code_commit"] == "abc123"


def test_runtime_mode_refuses_synthetic_evidence(governed):
    out = _verifier().verify(governed, DATASET)  # RUNTIME default
    assert out["verdict"] == "FAILED" and out["checks"]["1_INTEGRITY"]["result"] == "FAIL"


def test_a_library_pinned_to_another_dataset_fails_identity(governed, tmp_path):
    other = json.loads(DATASET.read_text(encoding="utf-8"))
    other["partitions"] = other["partitions"][:-1]
    other["symbols"] = other["symbols"][:-1]
    from app.trading_intelligence.hashing import stable_hash

    body = {k: v for k, v in other.items() if k not in ("manifest_hash", "metadata")}
    other["manifest_hash"] = stable_hash(body)
    (tmp_path / "other.json").write_text(json.dumps(other), encoding="utf-8")
    out = _verifier().verify(governed, tmp_path / "other.json", mode="TEST")
    assert out["checks"]["4_DATASET_IDENTITY"]["result"] == "FAIL" and out["verdict"] == "FAILED"
