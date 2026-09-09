"""Phase 15 over the production-parity historical dataset.

The live baseline measured 150 real evaluations from one day. This measures the
same funnel over real market data, through the same replay engine that proves
production parity, so the numbers describe the production brain rather than a
reimplementation of it.

Nothing is tuned. The ensemble runs unassisted, with production thresholds, and
whatever it does is what gets reported.

    python scripts/run_historical_baseline.py --timeframe 15m --days 30
    python scripts/run_historical_baseline.py --timeframe 1h --days 120 --json

The final holdout is excluded by default (§14.10): a baseline is a measurement
that informs decisions, which is exactly what the holdout must not do.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND = os.path.join(REPO_ROOT, "backends", "bot-backend")
SHARED = os.path.join(REPO_ROOT, "backends", "shared")

HTF_FOR = {"1m": "15m", "5m": "1h", "15m": "4h", "1h": "4h", "4h": "1d"}


def load_base(root: str, symbol: str, days: int | None) -> list[list]:
    directory = os.path.join(root, "raw", symbol)
    if not os.path.isdir(directory):
        raise SystemExit(
            f"no acquired data for {symbol} at {directory}. Run "
            f"scripts/acquire_market_data.py first."
        )
    shards = sorted(os.listdir(directory))
    if days:
        shards = shards[-days:]
    rows: list[list] = []
    for shard in shards:
        with open(os.path.join(directory, shard), encoding="utf-8") as handle:
            rows.extend(json.load(handle))
    rows.sort(key=lambda r: int(r[0]))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"])
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--data", default=os.path.join("data", "research"))
    parser.add_argument("--out", default=None, help="research database path")
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--include-final-holdout", action="store_true",
        help="§14.10 forbids this for anything that informs a decision",
    )
    args = parser.parse_args()

    for path in (BACKEND, SHARED):
        if path not in sys.path:
            sys.path.insert(0, path)
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)

    data_root = args.data if os.path.isabs(args.data) else os.path.join(REPO_ROOT, args.data)
    out = args.out or os.path.join(data_root, "baseline_replay.db")
    os.environ["DATABASE_URL"] = "sqlite:///" + out.replace("\\", "/")
    os.chdir(BACKEND)

    from app.replay.engine import ReplaySession
    from app.research.dataset import (
        FINAL_HOLDOUT,
        assess_quality,
        derive,
        partition,
    )
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate

    if os.path.exists(out):
        os.remove(out)
    db = DB(out)
    migrate(db)

    htf = HTF_FOR.get(args.timeframe, "4h")
    print(f"database   : {out}")
    print(f"timeframe  : {args.timeframe}  (higher timeframe {htf})")
    print(f"window     : last {args.days} day(s) of acquired 1m data")
    print(f"symbols    : {', '.join(args.symbols)}")

    started = time.time()
    total_evaluations = 0
    for symbol in args.symbols:
        base = load_base(data_root, symbol, args.days)
        if not base:
            print(f"\n{symbol}: no data")
            continue

        primary = derive(base, args.timeframe) if args.timeframe != "1m" else base
        higher = derive(base, htf)
        report = assess_quality(primary, symbol=symbol, timeframe=args.timeframe)
        print(f"\n{symbol}: {len(base):,} 1m bars -> {len(primary):,} {args.timeframe} "
              f"bars, {len(higher):,} {htf} bars "
              f"(completeness {report.completeness:.4%}, usable={report.is_usable})")
        if not report.is_usable:
            print(f"  refusing to baseline on a structurally broken series")
            return 1

        parts = partition(primary)
        holdout = next(p for p in parts if p.name == FINAL_HOLDOUT)
        end_ms = None if args.include_final_holdout else holdout.start_ms - 1
        if end_ms is not None:
            kept = [r for r in primary if int(r[6]) <= end_ms]
            print(f"  final holdout excluded: baselining {len(kept):,} of "
                  f"{len(primary):,} bars (up to {holdout.start_ms})")
        else:
            print("  WARNING: final holdout INCLUDED. This result must not inform "
                  "any decision.")

        session = ReplaySession(
            db, {symbol: {args.timeframe: primary, htf: higher}},
            bot_instance_id=f"bot_baseline_{symbol.lower()}",
            symbol=symbol, timeframe=args.timeframe, higher_timeframe=htf,
            capital_budget=10_000.0, position_allocation=1_000.0,
            dataset_id=f"binance_{args.timeframe}_{symbol.lower()}_{args.days}d",
        )
        result = session.run(end_ms=end_ms)
        total_evaluations += result.evaluations
        print(f"  evaluations {result.evaluations:,} | decisions "
              f"{len(result.decisions):,} | positions {len(result.positions)} "
              f"| errors {len(result.errors)}")
        if result.errors:
            print(f"  first error: {result.errors[0][:160]}")
        print(f"  manifest replay_hash={result.manifest['replay_hash']} "
              f"dataset_hash={result.manifest['dataset_hash']}")

    elapsed = time.time() - started
    print(f"\n{total_evaluations:,} evaluations in {elapsed / 60:.1f} min")

    sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))
    from build_master_ensemble_baseline import build, render

    report = build(db, bot_id=None, provenance="REPLAY")
    report["dataset"] = {
        "timeframe": args.timeframe, "days": args.days,
        "symbols": args.symbols, "final_holdout_included": args.include_final_holdout,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    print()
    print(json.dumps(report, indent=2, default=str) if args.json else render(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
