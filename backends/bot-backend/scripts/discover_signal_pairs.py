from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BOT_BACKEND = ROOT / "backends" / "bot-backend"
SHARED = ROOT / "backends" / "shared"
sys.path.insert(0, str(BOT_BACKEND))
sys.path.insert(0, str(SHARED))

from app.signals.pair_discovery import PairDiscoveryService  # noqa: E402
from app.signals.signal_scheduler_config import (  # noqa: E402
    DEFAULT_MAX_SPREAD,
    DEFAULT_MIN_VOLUME,
    LOCK_PAIR_DISCOVERY,
    PAIR_DISCOVERY_LOCK_TTL_SECONDS,
)
from shared_lib.persistence.db import DB  # noqa: E402
from shared_lib.persistence.migrations import migrate  # noqa: E402
from shared_lib.persistence.signals import (  # noqa: E402
    acquire_signal_operation_lock,
    is_pair_discovery_paused,
    release_signal_operation_lock,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Discover safe Signal Center crypto pairs.")
    parser.add_argument("--min-volume", type=float, default=DEFAULT_MIN_VOLUME)
    parser.add_argument("--max-spread", type=float, default=DEFAULT_MAX_SPREAD)
    parser.add_argument("--quote-asset", default="USDT")
    parser.add_argument("--contract-type", default="PERPETUAL")
    parser.add_argument("--skip-candle-validation", action="store_true")
    parser.add_argument("--candle-timeframe", default="1h")
    parser.add_argument("--min-candles", type=int, default=200)
    parser.add_argument("--db-path")
    parser.add_argument("--scheduled", action="store_true", help="Respect pause switch and prevent overlapping discovery.")
    parser.add_argument("--ignore-pause", action="store_true", help="Explicitly bypass pair_discovery_paused.")
    parser.add_argument("--lock-ttl-seconds", type=int, default=PAIR_DISCOVERY_LOCK_TTL_SECONDS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.db_path:
        migrate(args.db_path)
        db = DB(path=args.db_path)
    else:
        migrate()
        db = DB()
    if args.scheduled and is_pair_discovery_paused(db=db) and not args.ignore_pause:
        summary = {"paused": True, "errors": []}
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    lock_acquired = False
    if args.scheduled:
        lock_acquired = acquire_signal_operation_lock(
            LOCK_PAIR_DISCOVERY,
            args.lock_ttl_seconds,
            metadata={"script": "discover_signal_pairs"},
            db=db,
        )
        if not lock_acquired:
            summary = {"lock_not_acquired": True, "errors": []}
            print(json.dumps(summary, indent=2, sort_keys=True))
            return 0
    try:
        service = PairDiscoveryService(db=db)
        summary = service.discover_binance_futures_pairs(
            min_quote_volume_24h=args.min_volume,
            max_spread_percent=args.max_spread,
            quote_asset=args.quote_asset,
            contract_type=args.contract_type,
            validate_candles=not args.skip_candle_validation,
            candle_timeframe=args.candle_timeframe,
            min_candles=args.min_candles,
        )
    finally:
        if lock_acquired:
            release_signal_operation_lock(LOCK_PAIR_DISCOVERY, db=db)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 1 if summary.get("errors") else 0


if __name__ == "__main__":
    raise SystemExit(main())
