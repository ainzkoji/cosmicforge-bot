#!/usr/bin/env python3
"""Freeze a research DATASET identity (lineage v2) -- distinct from the frozen UNIVERSE manifest.

The universe manifest pins WHICH instruments and window; the dataset freeze pins WHAT DATA: one partition per
member with its row count, quality counts, gap classification and a content hash over the actual rows. It is
what ``holdout_guard.pre_holdout_readiness`` verifies (``manifest_frozen`` / ``dataset_complete`` /
``quality_acceptable``) before a holdout may even be authorized.

    python scripts/freeze_research_dataset.py crypto-broad --canonical-db <cosmicforge.db> \\
        --out docs/research/datasets/crypto_broad_binance_v1.dataset.json [--persist-db <cosmicforge.db>]

Read-only against the candle store. ``app.market_data.universe.freeze_dataset_payload`` refuses (and nothing is
written) unless every member partition is COMPLETE with zero invalid / duplicate / misaligned rows and only
expected-closure gaps. Freezing opens no holdout and grants no authority. FX is frozen separately, only after
its 1m acquisition, derivation, QA and coverage are final.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(REPO, "scripts"), os.path.join(REPO, "backends", "bot-backend"),
                os.path.join(REPO, "backends", "shared")]

EXPECTED_CLOSURES = ("WEEKEND_CLOSED", "HOLIDAY_CLOSED", "SESSION_CLOSED", "LISTING_AGE")


def _commit() -> str:
    head = subprocess.run(["git", "-C", REPO, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", REPO, "status", "--porcelain"], capture_output=True, text=True).stdout.strip()
    if not head or dirty:
        raise SystemExit("REFUSED: a dataset freeze records its code commit -- commit first (clean tree required)")
    return head


def _partition_hash(conn: sqlite3.Connection, symbol: str, start_ms: int, end_ms: int) -> str:
    h = hashlib.sha256()
    for row in conn.execute(
            "SELECT open_time, open, high, low, close, volume FROM historical_candles WHERE symbol=? AND "
            "interval='15m' AND data_source='binance' AND market_type='crypto' AND open_time>=? AND open_time<? "
            "ORDER BY open_time", (symbol, start_ms, end_ms)):
        h.update(json.dumps(row, separators=(",", ":")).encode())
        h.update(b"\n")
    return h.hexdigest()


def crypto_broad(args) -> dict:
    from dataset_coverage import BROAD, crypto_broad_evidence

    from app.market_data.universe import freeze_dataset_payload, load_frozen_universe

    commit = _commit()
    universe = load_frozen_universe(BROAD)
    cov = crypto_broad_evidence(args.canonical_db)
    if not cov["aggregate"]["reconciles_to_db"]:
        raise SystemExit("REFUSED: coverage does not reconcile to the database")
    gap_policies = {(m["gap_classification"] or {}).get("policy_version") for m in cov["members"]}
    if len(gap_policies) != 1 or None in gap_policies:
        raise SystemExit(f"REFUSED: members were gap-classified under {sorted(map(str, gap_policies))}")
    s, e = int(universe["window_start_ms"]), int(universe["window_end_ms"])
    conn = sqlite3.connect(f"file:{args.canonical_db}?mode=ro", uri=True)
    try:
        partitions = []
        for mem in cov["members"]:
            by_cls = (mem["gap_classification"] or {}).get("by_classification") or {}
            partitions.append({
                "symbol": mem["symbol"], "timeframe": "15m", "source": universe["source_provider"],
                "venue": universe["source_venue"], "source_version": mem["data_version"],
                "status": mem["history_status"], "rows": mem["rows"], "expected_rows": mem["expected_rows"],
                "invalid_rows": mem["invalid_rows"], "duplicate_rows": mem["duplicate_rows"],
                # off-grid open times are the chronology defect a 15m series can have (rows are read ordered)
                "out_of_order": mem["misaligned_rows"],
                "missing_ranges": [{"reason": cls, "gaps": v.get("gaps")} for cls, v in sorted(by_cls.items())
                                   if v.get("gaps")],
                "start_ms": mem["actual_start_ms"], "last_open_ms": mem["actual_last_open_ms"],
                "partition_hash": _partition_hash(conn, mem["symbol"], s, e)})
    finally:
        conn.close()
    payload = freeze_dataset_payload(
        universe=universe, partitions=partitions, metadata_hash=universe["metadata_hash"], code_commit=commit,
        product_type="PERPETUAL", base_interval="15m", resampling_policy_version="native-provider-15m-no-resampling",
        gap_policy_version=gap_policies.pop(), created_at=datetime.now(timezone.utc).isoformat())
    payload = {**payload, "metadata": {**payload["metadata"], "dataset": "CRYPTO_BROAD",
                                       "coverage_content_hash": _content_hash(cov),
                                       "store": "canonical historical_candles (read-only)"}}
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
    if args.persist_db:
        from app.market_data.universe import persist_dataset_manifest

        class _DB:
            @contextmanager
            def connect(self):
                c = sqlite3.connect(args.persist_db, timeout=60)
                try:
                    yield c
                    c.commit()
                finally:
                    c.close()

        persist_dataset_manifest(_DB(), role="CERTIFICATION_DATASET", asset_class="CRYPTO",
                                 venue=universe["source_venue"], payload=payload, created_at=int(time.time() * 1000))
    return {"dataset": "CRYPTO_BROAD", "manifest_hash": payload["manifest_hash"], "status": payload["status"],
            "universe_hash": payload["universe_hash"], "partitions": len(partitions), "code_commit": commit,
            "out": os.path.relpath(args.out, REPO).replace(os.sep, "/"), "persisted": bool(args.persist_db)}


def _content_hash(body: dict) -> str:
    from dataset_coverage import COVERAGE_VERSION

    content = json.dumps({**body, "coverage_version": COVERAGE_VERSION}, sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).hexdigest()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("crypto-broad")
    a.add_argument("--canonical-db", required=True)
    a.add_argument("--out", required=True)
    a.add_argument("--persist-db", default=None, help="also record it in dataset_manifests (append-only, idempotent)")
    a.set_defaults(func=crypto_broad)
    args = p.parse_args()
    try:
        print(json.dumps(args.func(args), indent=2))
    except ValueError as exc:  # freeze_dataset_payload refusals are the verdict, not a crash
        print(json.dumps({"status": "NOT_FROZEN", "reason": str(exc)}, indent=2))
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
