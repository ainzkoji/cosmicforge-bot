"""Materialize immutable Phase 14 training examples from real market data.

This is a research artifact builder, not a trainer. It reads the immutable
market-data manifest plus ignored raw shards, builds causal examples through
``app.research.dataset.build_training_example``, applies purge/embargo, and
writes an examples JSONL plus a small manifest.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
BACKEND = REPO_ROOT / "backends" / "bot-backend"
SHARED = REPO_ROOT / "backends" / "shared"


def _load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=str)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_symbol_rows(raw_root: Path, symbol: str) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for shard in sorted((raw_root / symbol).glob("*.json")):
        rows.extend(_load_json(shard))
    return rows


def _instrument(symbol: str):
    from app.research.dataset import InstrumentIdentity

    base = symbol.removesuffix("USDT")
    return InstrumentIdentity(
        venue="binance",
        venue_symbol=symbol,
        canonical_symbol=f"{base}/USDT",
        instrument_type="SPOT",
        asset_class="CRYPTO",
        base_asset=base,
        quote_asset="USDT",
        settlement_asset="USDT",
        contract_type=None,
        contract_multiplier=1.0,
        tick_size=None,
        step_size=None,
    )


def _context(rows: list[list[Any]], index: int) -> dict[str, Any]:
    visible = rows[: index + 1]
    closes = [float(r[4]) for r in visible[-60:]]
    short = sum(closes[-10:]) / min(10, len(closes))
    long = sum(closes) / len(closes)
    close = closes[-1]
    buy = max(0.0, min(1.0, (short / long - 1.0) * 25.0 + 0.5))
    sell = max(0.0, min(1.0, (1.0 - short / long) * 25.0 + 0.5))
    action = "BUY" if buy >= 0.7 and buy > sell else "SELL" if sell >= 0.7 else "HOLD"
    confidence = max(buy, sell)
    return {
        "regime": {
            "source": "materialized_sma_context",
            "trend": "UP" if short > long else "DOWN" if short < long else "FLAT",
            "available": len(closes) >= 60,
        },
        "expert_outputs": {
            "sma_context": {
                "short_sma": short,
                "long_sma": long,
                "close": close,
                "buy_score": buy,
                "sell_score": sell,
            }
        },
        "ensemble": {
            "source": "deterministic_materializer",
            "buy_score": buy,
            "sell_score": sell,
            "raw_confidence": confidence,
        },
        "threshold": {
            "source": "locked_research_materializer",
            "effective_entry_threshold": 0.7,
        },
        "decision": {
            "action": action,
            "decision_outcome": "NO_OPPORTUNITY" if action == "HOLD" else "CANDIDATE",
            "decision_context": "materialized_historical_context",
        },
        "risk": {
            "source": "not_executed_research_example",
            "execution_risk": "NOT_EVALUATED",
        },
    }


def _partition_objects(manifest: dict[str, Any]):
    from app.research.dataset import Partition

    return tuple(
        Partition(
            name=p["name"],
            start_ms=int(p["start_ms"]),
            end_ms=int(p["end_ms"]),
            rows=int(p["rows"]),
        )
        for p in manifest["partitions"]
    )


def _partition_for(parts, ts: int) -> str | None:
    for part in parts:
        if part.contains(ts):
            return part.name
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--raw-root", default=str(REPO_ROOT / "data" / "research" / "raw"))
    parser.add_argument("--out", default=str(REPO_ROOT / "data" / "research" / "examples"))
    parser.add_argument("--example-dataset-id", default=None)
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--horizon-bars", type=int, default=12)
    parser.add_argument("--embargo-bars", type=int, default=4)
    parser.add_argument("--max-per-symbol", type=int, default=64)
    args = parser.parse_args()

    for path in (str(BACKEND), str(SHARED)):
        if path not in sys.path:
            sys.path.insert(0, path)

    from app.research.dataset import (
        FINAL_HOLDOUT,
        MINUTE_MS,
        PurgeEmbargoPolicy,
        build_training_example,
        derive,
        filter_by_provenance,
        purge_embargo_partitions,
        series_checksum,
    )

    manifest_path = Path(args.manifest)
    market_manifest = _load_json(manifest_path)
    dataset_id = market_manifest["dataset_id"]
    dataset_hash = market_manifest["dataset_hash"]
    example_dataset_id = args.example_dataset_id or f"{dataset_id}_examples_v1"
    raw_root = Path(args.raw_root)
    out_dir = Path(args.out)
    output_path = out_dir / f"{example_dataset_id}.jsonl"
    manifest_out = out_dir.parent / f"{example_dataset_id}.manifest.json"
    parts = _partition_objects(market_manifest)
    horizon_minutes = int(args.horizon_bars) * 15
    policy = PurgeEmbargoPolicy(
        label_horizon_ms=horizon_minutes * MINUTE_MS,
        embargo_ms=int(args.embargo_bars) * 15 * MINUTE_MS,
    )

    examples = []
    for symbol in market_manifest["symbols"]:
        base = _load_symbol_rows(raw_root, symbol)
        rows = derive(base, args.timeframe)
        htf = derive(base, "1h")
        usable = [
            (i, row) for i, row in enumerate(rows)
            if _partition_for(parts, int(row[6])) != FINAL_HOLDOUT
        ]
        if not usable:
            continue
        stride = max(1, len(usable) // max(1, int(args.max_per_symbol)))
        for i, row in usable[::stride][: int(args.max_per_symbol)]:
            ctx = _context(rows, i)
            example = build_training_example(
                dataset_id=dataset_id,
                dataset_hash=dataset_hash,
                instrument=_instrument(symbol),
                timeframe=args.timeframe,
                decision_timestamp_ms=int(row[6]),
                rows=rows,
                htf_rows=htf,
                horizon_bars=int(args.horizon_bars),
                policy_version="locked_pre_ai_policy",
                strategy_version="master_ensemble_context_materializer/1.0.0",
                regime=ctx["regime"],
                expert_outputs=ctx["expert_outputs"],
                ensemble=ctx["ensemble"],
                threshold=ctx["threshold"],
                decision=ctx["decision"],
                risk=ctx["risk"],
                side="LONG",
                cost_model_hash="phase13_cost_model_compatible_zero_funding",
                fee_cost=0.0005 * float(row[4]),
                spread_cost=0.0001 * float(row[4]),
                slippage_cost=0.0002 * float(row[4]),
                funding_cost=0.0,
            )
            examples.append(example)

    filtered, purge_report = purge_embargo_partitions(examples, parts, policy)
    filtered = filter_by_provenance(filtered)
    out_dir.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for example in sorted(filtered, key=lambda e: (e.decision_timestamp_ms, e.example_id)):
            handle.write(json.dumps(example.to_dict(), sort_keys=True, separators=(",", ":")) + "\n")

    partition_counts: dict[str, int] = {}
    provenance_counts: dict[str, int] = {}
    for example in filtered:
        part = _partition_for(parts, example.decision_timestamp_ms) or "OUTSIDE"
        partition_counts[part] = partition_counts.get(part, 0) + 1
        provenance_counts[example.provenance] = provenance_counts.get(example.provenance, 0) + 1

    payload = {
        "example_dataset_id": example_dataset_id,
        "example_schema_version": "1.0.0",
        "source_market_dataset_id": dataset_id,
        "source_market_dataset_hash": dataset_hash,
        "builder": "scripts/materialize_research_examples.py",
        "builder_revision": _git_revision(),
        "policy_version": "locked_pre_ai_policy",
        "strategy_version": "master_ensemble_context_materializer/1.0.0",
        "feature_version": "deterministic_materialized_market_context/1.0.0",
        "label_version": "1.0.0",
        "cost_model_hash": "phase13_cost_model_compatible_zero_funding",
        "instruments": market_manifest["symbols"],
        "timeframes": [args.timeframe, "1h"],
        "horizons": [{"horizon_bars": args.horizon_bars, "horizon_minutes": horizon_minutes}],
        "partition_counts": partition_counts,
        "example_count": len(filtered),
        "provenance_counts": provenance_counts,
        "purge_embargo": purge_report.to_dict(),
        "output_path": str(output_path),
        "output_sha256": _sha256_file(output_path),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "final_holdout_excluded": True,
        "synthetic_or_legacy_included": False,
    }
    _write_json(manifest_out, payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _git_revision() -> str | None:
    import subprocess

    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.stdout.strip() or None
    except Exception:
        return None


if __name__ == "__main__":
    raise SystemExit(main())
