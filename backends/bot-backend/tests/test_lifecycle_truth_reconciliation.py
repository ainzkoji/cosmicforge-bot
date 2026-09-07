from __future__ import annotations

import sqlite3

from app.execution.position_manager import PositionManager, PositionSide
from app.runner.runner import PaperRunner
from app.symbols.symbol_promotion import SymbolPromotionEvaluator
from shared_lib.persistence.db import DB
from shared_lib.persistence.state_store import StateStore


def _create_lifecycle_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS position_lifecycle_state (
            bot_instance_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            position_id TEXT,
            phase TEXT NOT NULL DEFAULT 'SEEKING_TP1',
            original_stop REAL,
            current_stop REAL,
            original_tp1 REAL,
            original_tp2 REAL,
            is_break_even INTEGER NOT NULL DEFAULT 0,
            tp1_hit INTEGER NOT NULL DEFAULT 0,
            trailing_active INTEGER NOT NULL DEFAULT 0,
            highest_since_entry REAL,
            lowest_since_entry REAL,
            entry_qty_remaining REAL,
            sl_order_id TEXT,
            tp_order_id TEXT,
            exchange_position_active INTEGER,
            reconciliation_status TEXT,
            reconciliation_reason TEXT,
            last_reconciled_at TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (bot_instance_id, symbol)
        )
        """
    )
    existing_trade_cols = {
        row["name"] if hasattr(row, "keys") else row[1]
        for row in conn.execute("PRAGMA table_info(trade_fills)").fetchall()
    }
    for col, col_type in {
        "symbol": "TEXT",
        "action": "TEXT",
        "position_id": "TEXT",
        "exit_reason": "TEXT",
        "broker_response": "TEXT",
        "timestamp_utc": "TEXT",
        "ts": "TEXT",
        "created_at": "TEXT",
    }.items():
        if col not in existing_trade_cols:
            conn.execute(f"ALTER TABLE trade_fills ADD COLUMN {col} {col_type}")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS symbol_universe_rankings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ranking_run_id TEXT,
            created_at TEXT NOT NULL,
            bot_instance_id TEXT,
            mode TEXT NOT NULL,
            symbol TEXT NOT NULL,
            rank INTEGER,
            score REAL,
            recommended_action TEXT NOT NULL,
            selected_for_trading INTEGER NOT NULL DEFAULT 0,
            preserved_for_management INTEGER NOT NULL DEFAULT 0,
            diagnostics_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            action TEXT,
            position_id TEXT,
            exit_reason TEXT,
            broker_response TEXT,
            timestamp_utc TEXT,
            ts TEXT,
            created_at TEXT
        )
        """
    )


def _db(tmp_path, name: str = "lifecycle.db") -> DB:
    db = DB(path=str(tmp_path / name))
    with db.connect() as conn:
        _create_lifecycle_schema(conn)
    return db


def test_close_fill_marks_active_lifecycle_flat(tmp_path):
    db = _db(tmp_path)
    store = StateStore(db, bot_instance_id="bot_life")
    store.save_lifecycle_state(
        "BTCUSDT",
        {
            "position_id": "pos-1",
            "phase": "SEEKING_TP1",
            "sl_order_id": None,
            "tp_order_id": None,
            "exchange_position_active": 1,
        },
    )
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO trade_fills(symbol, side, action, qty, price, position_id, exit_reason, timestamp_utc, ts, created_at)
            VALUES ('BTCUSDT', 'LONG', 'CLOSE', 1.0, 100.0, 'pos-1', 'SL',
                    '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z')
            """
        )

    updated = store.reconcile_lifecycle_from_fills()
    row = store.load_lifecycle_state("BTCUSDT")

    assert updated == [{"symbol": "BTCUSDT", "reason": "DB_CLOSE_FILL:1"}]
    assert row["phase"] == "FLAT"
    assert row["exchange_position_active"] == 0
    assert row["reconciliation_status"] == "FLAT"


def test_already_flat_close_marks_active_lifecycle_flat(tmp_path):
    db = _db(tmp_path)
    store = StateStore(db, bot_instance_id="bot_life")
    store.save_lifecycle_state(
        "ETHUSDT",
        {"phase": "SEEKING_TP1", "exchange_position_active": 1},
    )
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO trade_fills(symbol, side, action, qty, price, position_id, exit_reason, broker_response, timestamp_utc, ts, created_at)
            VALUES ('ETHUSDT', 'LONG', 'CLOSE', 1.0, 100.0, NULL, 'TIME_EXIT', '{"reason":"ALREADY_FLAT"}',
                    '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z', '2026-04-26T00:00:00Z')
            """
        )

    updated = store.reconcile_lifecycle_from_fills()
    row = store.load_lifecycle_state("ETHUSDT")

    assert updated == [{"symbol": "ETHUSDT", "reason": "DB_ALREADY_FLAT:1"}]
    assert row["phase"] == "FLAT"
    assert row["exchange_position_active"] == 0


def test_pm_restore_persists_returned_protection_order_ids(tmp_path):
    db = _db(tmp_path)
    store = StateStore(db, bot_instance_id="bot_life")
    pm = PositionManager(store=store, bot_instance_id="bot_life")
    pm.open_position(
        symbol="SOLUSDT",
        side=PositionSide.LONG,
        position_id="pos-sol",
        entry_price=100.0,
        qty=1.0,
        stop_price=95.0,
        tp1_price=105.0,
        tp2_price=110.0,
    )
    runner = PaperRunner.__new__(PaperRunner)
    runner.position_manager = pm
    runner.store = store

    runner._persist_protection_result(
        "SOLUSDT",
        {"status": "repaired", "sl_order_id": "sl-123", "tp_order_id": "tp-456"},
        "PM_RESTORE",
    )

    row = store.load_lifecycle_state("SOLUSDT")
    assert row["sl_order_id"] == "sl-123"
    assert row["tp_order_id"] == "tp-456"
    assert row["reconciliation_status"] == "PROTECTED"


def test_startup_reconcile_backfills_existing_protection_ids(tmp_path):
    db = _db(tmp_path)
    store = StateStore(db, bot_instance_id="bot_life")
    store.save_lifecycle_state(
        "BNBUSDT",
        {"phase": "SEEKING_TP1", "exchange_position_active": 1},
    )
    runner = PaperRunner.__new__(PaperRunner)
    runner.position_manager = PositionManager(store=store, bot_instance_id="bot_life")
    runner.store = store

    runner._persist_protection_result(
        "BNBUSDT",
        {"status": "ok", "has_sl": True, "has_tp": True, "sl_order_id": "sl-open", "tp_order_id": "tp-open"},
        "STARTUP_RECONCILE",
    )

    row = store.load_lifecycle_state("BNBUSDT")
    assert row["sl_order_id"] == "sl-open"
    assert row["tp_order_id"] == "tp-open"
    assert row["reconciliation_status"] == "PROTECTED"


def test_startup_reconcile_repairs_missing_protection_and_persists_ids(tmp_path):
    db = _db(tmp_path)
    store = StateStore(db, bot_instance_id="bot_life")
    store.save_lifecycle_state(
        "ADAUSDT",
        {"phase": "SEEKING_TP1", "exchange_position_active": 1},
    )
    runner = PaperRunner.__new__(PaperRunner)
    runner.position_manager = PositionManager(store=store, bot_instance_id="bot_life")
    runner.store = store

    runner._persist_protection_result(
        "ADAUSDT",
        {"status": "repaired", "sl_order_id": "sl-new", "tp_order_id": "tp-new"},
        "STARTUP_RECONCILE",
    )

    row = store.load_lifecycle_state("ADAUSDT")
    assert row["sl_order_id"] == "sl-new"
    assert row["tp_order_id"] == "tp-new"
    assert row["reconciliation_status"] == "PROTECTED"


def test_promotion_gate_blocks_active_lifecycle_missing_protection_ids(tmp_path, monkeypatch):
    db = _db(tmp_path, "promotion_lifecycle.db")
    store = StateStore(db, bot_instance_id="bot_life")
    store.save_lifecycle_state(
        "XRPUSDT",
        {
            "phase": "SEEKING_TP1",
            "sl_order_id": None,
            "tp_order_id": None,
            "exchange_position_active": 1,
        },
    )
    monkeypatch.setattr("app.symbols.symbol_promotion.settings.SYMBOL_UNIVERSE_MODE", "dynamic_shadow")

    evaluator = SymbolPromotionEvaluator(db)
    evaluation = evaluator.evaluate(bot_instance_id="bot_life")
    decision = evaluator.ledger.record(evaluation, bot_instance_id="bot_life", from_mode="dynamic_shadow")

    assert decision["executed"] is False
    assert "open_naked_positions" in decision["failure_reasons"]
    assert evaluation.evidence_summary["live_safety"]["checks"]["open_naked_lifecycle_positions"] == 1
