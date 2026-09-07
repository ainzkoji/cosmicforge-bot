#!/usr/bin/env python3
"""
Phase I1 -- ML Data Readiness Audit CLI.

Loads a training parquet, runs all Phase I readiness checks, and prints a
detailed threshold table.  Exits 0 when all blocking checks pass, 1 otherwise.

Usage:
  python scripts/ml/check_event_ml_readiness.py \\
      --parquet models/datasets/training_20260427.parquet

  python scripts/ml/check_event_ml_readiness.py \\
      --parquet models/datasets/training_20260427.parquet \\
      --meta    models/datasets/training_20260427_meta.json \\
      --db-path /path/to/cosmicforge.db

Flags:
  --check-reactions   Also run reaction feature readiness checks
  --check-blackout    Treat blackout_row_count as a blocking check
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT   = _SCRIPT_DIR.parent.parent
_SHARED     = _BOT_ROOT.parent / "shared"
for _p in (str(_BOT_ROOT), str(_SHARED)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    import pandas as pd
except ImportError:
    print("[FAIL] pandas is required.  pip install pandas", file=sys.stderr)
    sys.exit(1)

from shared_lib.ml.readiness import (
    check_dataset_readiness,
    format_readiness_report,
)


def _load_meta(meta_path: Path | None, parquet_path: Path) -> dict:
    if meta_path and meta_path.exists():
        with open(meta_path, encoding="utf-8") as f:
            return json.load(f)
    # Auto-discover: training_YYYYMMDD_meta.json beside the parquet
    auto = parquet_path.parent / (parquet_path.stem + "_meta.json")
    if auto.exists():
        with open(auto, encoding="utf-8") as f:
            return json.load(f)
    return {}


def main() -> None:
    p = argparse.ArgumentParser(
        description="Check whether a training dataset meets Phase I ML readiness thresholds."
    )
    p.add_argument("--parquet",         required=True,       help="Path to training parquet file")
    p.add_argument("--meta",            default=None,        help="Path to metadata JSON (auto-discovered if omitted)")
    p.add_argument("--db-path",         default=None,        help="SQLite DB path (used for event count, optional)")
    p.add_argument("--check-reactions", action="store_true", help="Also run reaction feature checks")
    p.add_argument("--check-blackout",  action="store_true", help="Treat blackout row count as blocking")
    args = p.parse_args()

    parquet_path = Path(args.parquet).resolve()
    if not parquet_path.exists():
        print(f"[FAIL] Parquet not found: {parquet_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading: {parquet_path}")
    try:
        df = pd.read_parquet(str(parquet_path))
    except Exception as exc:
        print(f"[FAIL] Could not read parquet: {exc}", file=sys.stderr)
        sys.exit(1)
    print(f"Rows: {len(df):,}  Columns: {len(df.columns)}")

    meta_path = Path(args.meta).resolve() if args.meta else None
    meta = _load_meta(meta_path, parquet_path)
    if meta:
        print(f"Metadata loaded: {len(meta)} keys")

    # Optionally enrich meta with live DB event count
    if args.db_path and "historical_event_count" not in meta:
        try:
            import sqlite3
            conn = sqlite3.connect(f"file:{args.db_path}?mode=ro", uri=True)
            cursor = conn.execute(
                "SELECT COUNT(*) FROM economic_events WHERE impact_level IN ('HIGH','VERY_HIGH')"
            )
            row = cursor.fetchone()
            if row:
                meta["historical_event_count"] = int(row[0])
            conn.close()
            print(f"DB high-impact event count: {meta.get('historical_event_count', 'N/A')}")
        except Exception as exc:
            print(f"[WARN] Could not query DB for event count: {exc}")

    result = check_dataset_readiness(
        df,
        meta=meta or None,
        check_reactions=args.check_reactions,
        check_blackout=args.check_blackout,
    )

    print()
    print(format_readiness_report(result, title="ML Data Readiness -- Phase I Gate"))

    # Actionable recommendation
    print()
    if result.passed:
        print("RECOMMENDATION: Dataset meets readiness thresholds.")
        print("  You may run offline experiments and treat metrics as valid for research.")
    else:
        failed = [c.name for c in result.checks if not c.passed and c.blocking]
        print("RECOMMENDATION: Dataset is NOT ready for offline ML experiments.")
        print(f"  {len(failed)} blocking failure(s): {', '.join(failed)}")
        print()
        print("  Required actions:")

        organic_check = next((c for c in result.checks if c.name == "organic_row_count"), None)
        coverage_check = next((c for c in result.checks if c.name == "organic_time_coverage_days"), None)
        pre_check = next((c for c in result.checks if c.name == "pre_event_row_count"), None)
        post_check = next((c for c in result.checks if c.name == "post_event_row_count"), None)
        sl_check = next((c for c in result.checks if c.name == "sl_distance_pct_null_rate"), None)
        rr_check = next((c for c in result.checks if c.name == "planned_rr_null_rate"), None)

        if organic_check and not organic_check.passed:
            actual = organic_check.actual if isinstance(organic_check.actual, int) else 0
            need = max(1_000 - actual, 0)
            print(f"  - Accumulate ~{need:,} more organic (non-backfill) trades")
        if coverage_check and not coverage_check.passed:
            actual = coverage_check.actual if isinstance(coverage_check.actual, int) else 0
            need = max(14 - actual, 0)
            print(f"  - Continue live trading for ~{need} more day(s) to reach 14-day coverage")
        if (pre_check and not pre_check.passed) or (post_check and not post_check.passed):
            print("  - Ensure >=5 distinct high-impact economic events occur during live trading")
            print("    (pre_event_60min and post_event_60min rows need >=200 each)")
        if (sl_check and not sl_check.passed) or (rr_check and not rr_check.passed):
            print("  - Fix sl_plan/stop_loss_price backfill in build_dataset.py (I3)")
            print("    Then rebuild dataset with: python scripts/ml/build_dataset.py")

        print()
        print("  Re-run this check after accumulating more organic trade data.")

    sys.exit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
