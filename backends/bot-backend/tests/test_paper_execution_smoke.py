from __future__ import annotations

import sqlite3

from app.execution.paper_executor import PaperExecutor
from scripts.validation.run_paper_execution_smoke import (
    StaticPaperMarketClient,
    write_paper_records,
)


def _create_smoke_db(path):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE trade_fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trace_id TEXT, run_id TEXT, cycle_id TEXT, bot_instance_id TEXT,
            symbol TEXT, side TEXT, action TEXT, qty REAL, price REAL, fee REAL,
            strategy TEXT, strategy_version TEXT, broker_id TEXT, account_id TEXT,
            asset_class TEXT, timeframe TEXT, timestamp_utc TEXT, ts TEXT,
            created_at TEXT, slippage_pct REAL, entry_price_expected REAL,
            stop_loss_price REAL, position_id TEXT, order_id TEXT,
            position_phase TEXT, broker_response TEXT
        );
        CREATE TABLE decision_traces (
            trace_id TEXT, run_id TEXT, cycle_id TEXT, bot_instance_id TEXT,
            symbol TEXT, timeframe TEXT, ts TEXT, last_price REAL,
            regime_state TEXT, signal TEXT, confidence REAL, reason_codes TEXT,
            gate_allowed INTEGER, gate_reason TEXT, intended_action TEXT,
            execution_status TEXT, execution_error TEXT, submit_attempted INTEGER,
            fill_recorded INTEGER, position_opened INTEGER, order_id TEXT,
            created_at TEXT
        );
        CREATE TABLE position_lifecycle_state (
            bot_instance_id TEXT, symbol TEXT, position_id TEXT, phase TEXT,
            original_stop REAL, current_stop REAL, original_tp1 REAL,
            original_tp2 REAL, is_break_even INTEGER, tp1_hit INTEGER,
            trailing_active INTEGER, highest_since_entry REAL,
            lowest_since_entry REAL, entry_qty_remaining REAL,
            sl_order_id TEXT, tp_order_id TEXT, exchange_position_active INTEGER,
            reconciliation_status TEXT, reconciliation_reason TEXT,
            last_reconciled_at TEXT, updated_at TEXT
        );
        """
    )
    conn.commit()
    conn.close()


def test_smoke_write_persists_only_isolated_paper_records(tmp_path):
    db_path = tmp_path / "smoke.db"
    _create_smoke_db(db_path)
    client = StaticPaperMarketClient(price=100.0)
    result = PaperExecutor(client=client).open_position(
        symbol="BTCUSDT",
        side="BUY",
        notional_usdt=25.0,
        sl_price=95.0,
        tp_price=110.0,
    )

    writes = write_paper_records(db_path, result, symbol="BTCUSDT", side="BUY")

    assert writes == {
        "trade_fills": 1,
        "decision_traces": 1,
        "position_lifecycle_state": 1,
    }
    assert client.real_exchange_orders_sent is False
    conn = sqlite3.connect(db_path)
    try:
        fill = conn.execute("SELECT bot_instance_id, action, order_id FROM trade_fills").fetchone()
        trace = conn.execute("SELECT execution_status, fill_recorded, position_opened FROM decision_traces").fetchone()
        lifecycle = conn.execute("SELECT bot_instance_id, phase FROM position_lifecycle_state").fetchone()
    finally:
        conn.close()

    assert fill == ("paper_smoke", "OPEN", result.order_id)
    assert trace == ("PAPER_POSITION_OPENED", 1, 1)
    assert lifecycle == ("paper_smoke", "SEEKING_TP1")
