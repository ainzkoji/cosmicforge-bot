#!/usr/bin/env python3
"""Verify a governed outcome-library artifact before it may be pinned to the runtime (read-only).

    python scripts/verify_governed_library.py --library data/research/cati_libraries/cati_lib_<id> \\
        --dataset-manifest docs/research/datasets/crypto_broad_binance_v1.dataset.json \\
        [--write-pin docs/research/libraries/crypto_runtime_library.pin.json]

Checks (each PASS / FAIL with evidence; the verdict is VERIFIED only if all pass):

  1 INTEGRITY        RUNTIME-mode load: manifest hash, rows hash, library hash, versions, trusted source
  2 ROW_COUNT        rows loaded == manifest row_count == label_count == labeled_count
  3 LIBRARY_HASH     recomputed library hash == manifest library_hash
  4 DATASET_IDENTITY pinned dataset manifest == the committed frozen dataset manifest (verified), same universe
  5 POLICY_IDENTITY  the certification policy is the canonical RESEARCH_DEFAULT_V1 and a policy freeze is recorded
  6 CODE_IDENTITY    built from a recorded commit on a clean tree
  7 HOLDOUT_CUTOFF   every row's label window ends before the reserved holdout start (holdout never read)
  8 RELOAD_IDENTITY  a second streamed reload reproduces the same library hash and rows hash

The pin records identity only (no rows, no secrets). Pinning grants no authority.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / "backends" / "bot-backend"), str(REPO / "backends" / "shared")]


def verify(library_dir: Path, dataset_manifest: Path, mode: str = "RUNTIME") -> dict:
    from app.market_data.universe import verify_dataset_payload
    from app.trading_intelligence.contracts.setup import timeframe_to_ms
    from app.trading_intelligence.forecast.artifact import _file_text_hash, ROWS_FILE, load_library_artifact
    from app.trading_intelligence.research.certification.policy import canonical_certification_policy

    checks: dict = {}

    def put(name, ok, **evidence):
        checks[name] = {"result": "PASS" if ok else "FAIL", **evidence}

    try:
        lib, manifest = load_library_artifact(library_dir, mode=mode)  # RUNTIME by default
    except Exception as exc:
        put("1_INTEGRITY", False, error=f"{type(exc).__name__}: {exc}")
        return {"verdict": "FAILED", "checks": checks}
    put("1_INTEGRITY", True, source_kind=manifest["source_kind"], artifact_schema=manifest["artifact_schema_version"])
    n = len(lib.rows)
    put("2_ROW_COUNT", n == int(manifest["row_count"]) == int(manifest["label_count"]) == int(manifest["labeled_count"]),
        rows=n, manifest_row_count=manifest["row_count"], labeled_count=manifest["labeled_count"])
    put("3_LIBRARY_HASH", lib.library_hash == manifest["library_hash"], library_hash=lib.library_hash)
    gov = dict(manifest.get("governance") or {})
    committed = json.loads(dataset_manifest.read_text(encoding="utf-8"))
    committed_hash = verify_dataset_payload(committed)
    put("4_DATASET_IDENTITY", gov.get("dataset_manifest_hash") == committed_hash
        and gov.get("universe_hash") == committed.get("universe_hash"),
        dataset_manifest_hash=gov.get("dataset_manifest_hash"), committed=committed_hash,
        universe_hash=gov.get("universe_hash"), certification_dataset_hash=gov.get("certification_dataset_hash"))
    canon = canonical_certification_policy().policy_hash
    put("5_POLICY_IDENTITY", gov.get("certification_policy_hash") == canon and bool(gov.get("policy_freeze_hash")),
        certification_policy_hash=gov.get("certification_policy_hash"), canonical=canon,
        policy_freeze_hash=gov.get("policy_freeze_hash"))
    put("6_CODE_IDENTITY", bool(gov.get("code_commit")) and gov.get("source_tree_dirty") is False,
        code_commit=gov.get("code_commit"), source_tree_dirty=gov.get("source_tree_dirty"))
    holdout = gov.get("holdout_start_ms")
    horizon_ms = int(manifest["label_horizon_bars"]) * (timeframe_to_ms(manifest["timeframe"]) or 0)
    last_label_end = max((r.label.decision_time + horizon_ms for r in lib.rows), default=None)
    put("7_HOLDOUT_CUTOFF", holdout is not None and last_label_end is not None and last_label_end < holdout,
        holdout_id=gov.get("holdout_id"), holdout_start_ms=holdout, last_label_end_ms=last_label_end)
    first_hash, calibration_status = lib.library_hash, lib.calibration_status
    del lib  # one resident copy at a time: a certification-scale library is millions of rows
    import gc

    gc.collect()
    again, _ = load_library_artifact(library_dir, mode=mode)
    reload_hash = again.library_hash
    del again
    gc.collect()
    file_hash = _file_text_hash(library_dir / ROWS_FILE)
    put("8_RELOAD_IDENTITY", reload_hash == first_hash and file_hash == manifest["rows_sha256"],
        reload_library_hash=reload_hash, rows_sha256=file_hash)
    verdict = "VERIFIED" if all(c["result"] == "PASS" for c in checks.values()) else "FAILED"
    return {"verdict": verdict, "checks": checks, "library_id": manifest["library_id"],
            "library_hash": first_hash, "manifest_hash": manifest["manifest_hash"],
            "market_type": manifest.get("market_type"), "row_count": n, "governance": gov,
            "calibration_status": calibration_status}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--library", required=True)
    ap.add_argument("--dataset-manifest", required=True)
    ap.add_argument("--write-pin", default=None)
    ap.add_argument("--mode", default="RUNTIME", choices=["RUNTIME", "TEST"], help="TEST only for synthetic fixtures")
    args = ap.parse_args()
    os.chdir(REPO / "backends" / "bot-backend")
    out = verify(Path(args.library).resolve(), Path(args.dataset_manifest).resolve(), args.mode)
    if args.write_pin and out["verdict"] == "VERIFIED":
        pin = {k: out[k] for k in ("library_id", "library_hash", "manifest_hash", "market_type", "row_count",
                                   "governance", "calibration_status")}
        pin["checks"] = {k: v["result"] for k, v in out["checks"].items()}
        Path(args.write_pin).parent.mkdir(parents=True, exist_ok=True)
        Path(args.write_pin).write_text(json.dumps(pin, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(out, indent=2, sort_keys=True, default=str))
    return 0 if out["verdict"] == "VERIFIED" else 1


if __name__ == "__main__":
    sys.exit(main())
