#!/usr/bin/env python3
"""
G4: Historical market reaction backfill.

For each HIGH-impact economic event in the specified date range, checks
whether historical OHLCV candle data exists for BTCUSDT / ETHUSDT / SOLUSDT
in event_market_snapshots.  If candle data is present, computes reaction
metrics and upserts into market_event_reactions.  If candle data is absent,
reports that and exits without fabricating any data.

SAFETY:
  - Will NOT fabricate reaction data without real candles.
  - Will NOT guess, interpolate, or generate synthetic prices.
  - If no candle source is available, reports REACTION_BACKFILL_STATUS=NOT_READY
    and recommends the next steps.

Usage:
  python scripts/backfill_market_reactions.py \\
      --from 2025-12-01 --to 2026-05-25 \\
      --symbols BTCUSDT,ETHUSDT,SOLUSDT

  python scripts/backfill_market_reactions.py \\
      --from 2025-12-01 --to 2026-05-25 \\
      --db path/to/cosmicforge.db
"""
from __future__ import annotations

import argparse
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT   = _SCRIPT_DIR.parent
_SHARED     = _BOT_ROOT.parent / "shared"
for _p in (str(_BOT_ROOT), str(_SHARED)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from shared_lib.persistence.db import DB
from shared_lib.persistence.economic_events import get_upcoming_events

_DEFAULT_DB = (
    Path(__file__).resolve().parent.parent.parent
    / "shared" / "shared_lib" / "persistence" / "cosmicforge.db"
)

_DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
_REQUIRED_WINDOWS = ["-60m", "-30m", "event", "+5m", "+15m", "+30m", "+60m"]


# ---------------------------------------------------------------------------
# Candle data check
# ---------------------------------------------------------------------------

def _has_snapshot_data(conn, event_id: str, symbol: str) -> bool:
    """Return True if event_market_snapshots has any row for this event+symbol."""
    row = conn.execute(
        "SELECT 1 FROM event_market_snapshots WHERE event_id=? AND symbol=? LIMIT 1",
        (event_id, symbol),
    ).fetchone()
    return row is not None


def _load_snapshots(conn, event_id: str, symbol: str) -> dict:
    """Load available snapshots for an event+symbol, keyed by window_label."""
    rows = conn.execute(
        """SELECT window_label, price, volume, candle_open, candle_high,
                  candle_low, candle_close, atr
           FROM event_market_snapshots
           WHERE event_id=? AND symbol=?""",
        (event_id, symbol),
    ).fetchall()
    return {
        row[0]: {
            "price": row[1], "volume": row[2],
            "open": row[3], "high": row[4], "low": row[5], "close": row[6],
            "atr": row[7],
        }
        for row in rows
    }


# ---------------------------------------------------------------------------
# Reaction computation (only from real candle data)
# ---------------------------------------------------------------------------

def _compute_reaction(
    snapshots: dict,
    event_id: str,
    symbol: str,
    exchange: str,
    event_time_utc: str,
) -> Optional[dict]:
    """
    Compute reaction metrics from snapshot windows.
    Returns None if data is insufficient.
    """
    ev   = snapshots.get("event")
    pre  = snapshots.get("-30m") or snapshots.get("-60m")
    p5   = snapshots.get("+5m")
    p15  = snapshots.get("+15m")
    p30  = snapshots.get("+30m")
    p60  = snapshots.get("+60m")

    if ev is None or pre is None:
        return None

    price_before = pre["close"] or pre["price"]
    price_at_ev  = ev["close"]  or ev["price"]

    if not price_before or not price_at_ev:
        return None

    # Net price change at event
    net_move_pct = (price_at_ev - price_before) / price_before * 100.0

    # Collect all post-event prices for range computation
    post_prices = [
        snap["close"] or snap["price"]
        for key, snap in snapshots.items()
        if key.startswith("+") and snap.get("close")
    ]

    max_move_pct = None
    min_move_pct = None
    if post_prices:
        max_price = max(post_prices)
        min_price = min(post_prices)
        max_move_pct = (max_price - price_before) / price_before * 100.0
        min_move_pct = (min_price - price_before) / price_before * 100.0

    # Volatility expansion ratio (ATR before vs after)
    atr_before = pre.get("atr")
    atr_after  = (p30 or p60 or {}).get("atr")
    vol_expansion = None
    if atr_before and atr_after and atr_before > 0:
        vol_expansion = atr_after / atr_before

    # Volume spike ratio
    vol_before = pre.get("volume")
    vol_event  = ev.get("volume")
    vol_spike  = None
    if vol_before and vol_event and vol_before > 0:
        vol_spike = vol_event / vol_before

    # Classify reaction type
    reaction_type = _classify_simple(
        net_move_pct=net_move_pct,
        max_move_pct=max_move_pct,
        min_move_pct=min_move_pct,
        vol_expansion=vol_expansion,
        vol_spike=vol_spike,
    )

    post_end_label = "+60m" if "+60m" in snapshots else ("+30m" if "+30m" in snapshots else None)
    post_window_end_utc = None
    if post_end_label:
        pass  # timestamp not in snapshot; use event_time + offset in caller

    return {
        "event_id":                  event_id,
        "symbol":                    symbol,
        "exchange":                  exchange,
        "event_time_utc":            event_time_utc,
        "net_move_pct":              round(net_move_pct, 4),
        "max_move_pct":              round(max_move_pct, 4) if max_move_pct is not None else None,
        "min_move_pct":              round(min_move_pct, 4) if min_move_pct is not None else None,
        "volatility_expansion_ratio": round(vol_expansion, 4) if vol_expansion is not None else None,
        "volume_spike_ratio":         round(vol_spike, 4)    if vol_spike is not None else None,
        "reaction_type":              reaction_type,
        "data_quality":               "COMPLETE",
    }


def _classify_simple(
    net_move_pct: float,
    max_move_pct: Optional[float],
    min_move_pct: Optional[float],
    vol_expansion: Optional[float],
    vol_spike: Optional[float],
) -> str:
    """Minimal reaction classifier based on Phase D taxonomy."""
    high_vol = (vol_expansion or 1.0) > 1.5 or (vol_spike or 1.0) > 2.0
    if not high_vol and abs(net_move_pct) < 0.2:
        return "NO_REACTION"
    if max_move_pct is not None and min_move_pct is not None:
        range_pct = abs(max_move_pct - min_move_pct)
        if range_pct > 1.0 and abs(net_move_pct) < 0.3:
            return "WHIPSAW"
    if abs(net_move_pct) >= 0.5:
        return "TREND_CONTINUATION"
    if high_vol:
        return "VOL_SPIKE"
    return "NO_REACTION"


# ---------------------------------------------------------------------------
# Main backfill logic
# ---------------------------------------------------------------------------

def backfill_reactions(
    db: DB,
    from_utc: str,
    to_utc: str,
    symbols: list[str],
) -> dict:
    events = get_upcoming_events(db, from_utc=from_utc, to_utc=to_utc, impact_levels=["HIGH"])

    counts = {
        "events_checked":        0,
        "candle_data_present":   0,
        "candle_data_absent":    0,
        "reactions_computed":    0,
        "reactions_skipped":     0,
        "no_fabrication":        True,
        "symbols_checked":       symbols,
        "status":                "NOT_READY",
        "missing_event_ids":     [],
        "errors":                [],
    }

    with db.connect() as conn:
        for ev in events:
            counts["events_checked"] += 1
            event_id    = ev["event_id"]
            event_time  = ev["scheduled_utc"]

            for symbol in symbols:
                if not _has_snapshot_data(conn, event_id, symbol):
                    counts["candle_data_absent"] += 1
                    counts["missing_event_ids"].append(f"{event_id}/{symbol}")
                    continue

                counts["candle_data_present"] += 1
                snapshots = _load_snapshots(conn, event_id, symbol)

                reaction = _compute_reaction(
                    snapshots,
                    event_id=event_id,
                    symbol=symbol,
                    exchange="binance",
                    event_time_utc=event_time,
                )
                if reaction is None:
                    counts["reactions_skipped"] += 1
                    continue

                try:
                    from shared_lib.persistence.market_reactions import upsert_reaction
                    upsert_reaction(db, **reaction)
                    counts["reactions_computed"] += 1
                except Exception as exc:
                    counts["errors"].append(f"{event_id}/{symbol}: {exc}")

    if counts["candle_data_absent"] > 0 and counts["candle_data_present"] == 0:
        counts["status"] = "NOT_READY"
    elif counts["reactions_computed"] > 0:
        counts["status"] = "PARTIAL" if counts["candle_data_absent"] > 0 else "READY"
    else:
        counts["status"] = "NOT_READY"

    return counts


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill market reactions from candle data")
    p.add_argument("--from",    dest="from_date", required=True, help="YYYY-MM-DD start")
    p.add_argument("--to",      dest="to_date",   required=True, help="YYYY-MM-DD end")
    p.add_argument("--symbols", default=",".join(_DEFAULT_SYMBOLS),
                   help="Comma-separated symbols")
    p.add_argument("--db", default=str(_DEFAULT_DB), help="Path to cosmicforge.db")
    return p.parse_args()


def main() -> None:
    args    = _parse_args()
    db_p    = Path(args.db).resolve()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    if not db_p.exists():
        print(f"[FAIL] Database not found: {db_p}", file=sys.stderr)
        sys.exit(1)

    from_utc = f"{args.from_date}T00:00:00+00:00"
    to_utc   = f"{args.to_date}T23:59:59+00:00"

    print(f"DB path    : {db_p}")
    print(f"Range      : {args.from_date} -> {args.to_date}")
    print(f"Symbols    : {symbols}")
    print(f"Note       : Will NOT fabricate reactions without real candle data.")

    db = DB(path=str(db_p))
    counts = backfill_reactions(db, from_utc, to_utc, symbols)

    print(f"\n--- Market reaction backfill summary ---")
    print(f"  HIGH-impact events checked        : {counts['events_checked']}")
    print(f"  Symbol-events with candle data    : {counts['candle_data_present']}")
    print(f"  Symbol-events WITHOUT candle data : {counts['candle_data_absent']}")
    print(f"  Reactions computed and stored     : {counts['reactions_computed']}")
    print(f"  Reactions skipped (bad data)      : {counts['reactions_skipped']}")
    print(f"  No fabrication guarantee          : {counts['no_fabrication']}")
    print(f"  Status                            : {counts['status']}")

    if counts["candle_data_absent"] > 0:
        absent_sample = counts["missing_event_ids"][:10]
        print(f"\n  Missing candle data (first 10):")
        for eid in absent_sample:
            print(f"    - {eid}")
        if len(counts["missing_event_ids"]) > 10:
            print(f"    ... and {len(counts['missing_event_ids'])-10} more")
        print(
            "\n  [REACTION_BACKFILL_STATUS=NOT_READY]"
            "\n  Next steps:"
            "\n    1. Ingest historical OHLCV candles via exchange API (Binance /klines)."
            "\n    2. Store them in event_market_snapshots via insert_snapshot()."
            "\n    3. Re-run this script."
            "\n    4. Until candles are available, reaction features should be"
            "\n       EXCLUDED from the Phase F re-run (use timing features only)."
        )
    else:
        print(f"\n  [REACTION_BACKFILL_STATUS={counts['status']}]")

    for err in counts["errors"]:
        print(f"  [ERROR] {err}")


if __name__ == "__main__":
    main()
