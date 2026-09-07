#!/usr/bin/env python3
"""Paper execution smoke test.

This script exercises the internal paper simulator only. It never constructs a
live exchange order request and never calls place_order/update_protection.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_BOT_ROOT = _SCRIPT_DIR.parent.parent
_SHARED_ROOT = _BOT_ROOT.parent / "shared"
for _path in (str(_BOT_ROOT), str(_SHARED_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from app.execution.paper_executor import PaperExecutor
from scripts.validation.run_paper_cycle_diagnostic import resolve_db_path


class StaticPaperMarketClient:
    def __init__(self, price: float) -> None:
        self.price = float(price)
        self.real_exchange_orders_sent = False

    def get_prices(self, symbols: list[str]) -> dict[str, float]:
        return {symbol: self.price for symbol in symbols}

    def get_ticker(self, symbol: str) -> dict[str, str]:
        return {
            "symbol": symbol,
            "lastPrice": str(self.price),
            "bidPrice": str(self.price * 0.9999),
            "askPrice": str(self.price * 1.0001),
        }

    def place_order(self, *_args: Any, **_kwargs: Any) -> None:
        self.real_exchange_orders_sent = True
        raise AssertionError("paper smoke must not call place_order")

    def update_protection(self, *_args: Any, **_kwargs: Any) -> None:
        self.real_exchange_orders_sent = True
        raise AssertionError("paper smoke must not call update_protection")

    def close_position_market(self, *_args: Any, **_kwargs: Any) -> None:
        self.real_exchange_orders_sent = True
        raise AssertionError("paper smoke must not call close_position_market")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except sqlite3.Error:
        return set()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
    )


def _insert_dynamic(conn: sqlite3.Connection, table: str, values: dict[str, Any]) -> int:
    if not _table_exists(conn, table):
        return 0
    columns = _table_columns(conn, table)
    payload = {key: value for key, value in values.items() if key in columns}
    if not payload:
        return 0
    names = list(payload)
    placeholders = ",".join("?" for _ in names)
    quoted = ",".join(names)
    insert_verb = "INSERT OR REPLACE" if table == "position_lifecycle_state" else "INSERT"
    conn.execute(
        f"{insert_verb} INTO {table} ({quoted}) VALUES ({placeholders})",
        tuple(payload[name] for name in names),
    )
    return 1


def write_paper_records(db_path: Path, result: Any, *, symbol: str, side: str) -> dict[str, int]:
    if not db_path.exists():
        raise FileNotFoundError(f"Database does not exist; refusing to create it: {db_path}")

    now = utc_now_iso()
    trace_id = f"paper_smoke_trace_{now.replace(':', '').replace('-', '').replace('.', '')}"
    run_id = "paper_execution_smoke"
    cycle_id = f"paper_smoke_cycle_{now}"
    details = result.details or {}
    position_id = str(details.get("position_id") or f"paper_smoke_position_{symbol}")
    fill_side = "LONG" if side.upper() == "BUY" else "SHORT"
    qty = float(result.filled_qty or details.get("qty") or 0.0)
    price = float(result.avg_price or details.get("avg_price") or 0.0)
    fee = float(result.fee or details.get("fee") or 0.0)

    writes = {"trade_fills": 0, "decision_traces": 0, "position_lifecycle_state": 0}
    conn = sqlite3.connect(db_path)
    try:
        writes["trade_fills"] = _insert_dynamic(
            conn,
            "trade_fills",
            {
                "trace_id": trace_id,
                "run_id": run_id,
                "cycle_id": cycle_id,
                "bot_instance_id": "paper_smoke",
                "symbol": symbol,
                "side": fill_side,
                "action": "OPEN",
                "qty": qty,
                "price": price,
                "fee": fee,
                "strategy": "paper_execution_smoke",
                "strategy_version": "0",
                "broker_id": "paper_simulator",
                "account_id": "paper_smoke",
                "asset_class": "CRYPTO",
                "timeframe": "15m",
                "timestamp_utc": now,
                "ts": now,
                "created_at": now,
                "slippage_pct": (
                    ((price - float(details.get("reference_price") or price)) / float(details.get("reference_price") or price)) * 100.0
                    if price > 0
                    else None
                ),
                "entry_price_expected": details.get("reference_price"),
                "stop_loss_price": details.get("sl_price"),
                "position_id": position_id,
                "order_id": result.order_id,
                "position_phase": "SEEKING_TP1",
                "broker_response": json.dumps(details, sort_keys=True),
            },
        )
        writes["decision_traces"] = _insert_dynamic(
            conn,
            "decision_traces",
            {
                "trace_id": trace_id,
                "run_id": run_id,
                "cycle_id": cycle_id,
                "bot_instance_id": "paper_smoke",
                "symbol": symbol,
                "timeframe": "15m",
                "ts": now,
                "last_price": details.get("reference_price") or price,
                "regime_state": "SMOKE_TEST",
                "signal": side.upper(),
                "confidence": 1.0,
                "reason_codes": "PAPER_EXECUTION_SMOKE",
                "gate_allowed": 1,
                "gate_reason": "PAPER_EXECUTION_SMOKE",
                "intended_action": side.upper(),
                "execution_status": result.status,
                "execution_error": result.error,
                "submit_attempted": 1,
                "fill_recorded": 1 if result.success else 0,
                "position_opened": 1 if result.success else 0,
                "order_id": result.order_id,
                "created_at": now,
            },
        )
        writes["position_lifecycle_state"] = _insert_dynamic(
            conn,
            "position_lifecycle_state",
            {
                "bot_instance_id": "paper_smoke",
                "symbol": symbol,
                "position_id": position_id,
                "phase": "SEEKING_TP1",
                "original_stop": details.get("sl_price"),
                "current_stop": details.get("sl_price"),
                "original_tp1": details.get("tp_price"),
                "original_tp2": details.get("tp_price"),
                "is_break_even": 0,
                "tp1_hit": 0,
                "trailing_active": 0,
                "highest_since_entry": price,
                "lowest_since_entry": price,
                "entry_qty_remaining": qty,
                "sl_order_id": (details.get("protection") or {}).get("sl_order_id"),
                "tp_order_id": (details.get("protection") or {}).get("tp_order_id"),
                "exchange_position_active": 1,
                "reconciliation_status": "PAPER_SMOKE",
                "reconciliation_reason": "paper execution smoke",
                "last_reconciled_at": now,
                "updated_at": now,
            },
        )
        conn.commit()
    finally:
        conn.close()
    return writes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a paper execution simulator smoke test.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--side", choices=["BUY", "SELL"], default="BUY")
    parser.add_argument("--notional", type=float, default=25.0)
    parser.add_argument("--price", type=float, default=100000.0)
    parser.add_argument("--db-path")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", help="Simulate only; write no records.")
    group.add_argument("--write-paper", action="store_true", help="Write isolated paper_smoke records.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = StaticPaperMarketClient(args.price)
    executor = PaperExecutor(client=client)
    result = executor.open_position(
        symbol=args.symbol.upper(),
        side=args.side.upper(),
        notional_usdt=float(args.notional),
    )

    writes = {"trade_fills": 0, "decision_traces": 0, "position_lifecycle_state": 0}
    db_path = resolve_db_path(args.db_path)
    if args.write_paper:
        writes = write_paper_records(db_path, result, symbol=args.symbol.upper(), side=args.side.upper())

    payload = {
        "success": bool(result.success),
        "status": result.status,
        "order_id": result.order_id,
        "avg_price": result.avg_price,
        "filled_qty": result.filled_qty,
        "fee": result.fee,
        "dry_run": not args.write_paper,
        "orders_written": sum(writes.values()),
        "records_written": writes,
        "db_path": str(db_path),
        "real_exchange_orders_sent": bool(client.real_exchange_orders_sent),
        "details": result.details,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if result.success and not client.real_exchange_orders_sent else 1


if __name__ == "__main__":
    raise SystemExit(main())
