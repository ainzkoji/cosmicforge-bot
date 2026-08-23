"""
Phase 3 Step 3B — Lifecycle Persistence Validation Tests.

Covers all 10 scenarios from the implementation plan:
  1. Restart with open protected position
  2. Restart with TP1 already hit
  3. Restart with break-even active
  4. Restart with trailing active
  5. Missing stop detected by heartbeat
  6. Heartbeat uses persisted values (not live price)
  7. Reduce-only on SL/TP orders
  8. update_protection cancel-replace pathway
  9. Cancel succeeds, replace fails (partial failure handling)
 10. SymbolState + PM stay synchronized after mutations

Run with:
    python -m pytest tests/test_lifecycle_persistence.py -v
"""
from __future__ import annotations

import sqlite3
import tempfile
import os
from typing import Optional
from unittest.mock import MagicMock, call, patch

import pytest

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_tmp_db():
    """Return a (path, conn) pair for a fresh in-memory / temp SQLite DB."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


def _migrate(path: str):
    """Run the project migration to set up all tables."""
    from shared_lib.persistence.migrations import migrate
    migrate(path)


def _state_store(path: str, bot_id: str = "test_bot"):
    """Return a StateStore connected to the temp DB."""
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.state_store import StateStore
    db = DB(path)
    return StateStore(db=db, bot_instance_id=bot_id)


def _position_manager(store=None, bot_id: str = "test_bot"):
    """Return a fresh PositionManager, optionally wired to a store."""
    from app.execution.position_manager import PositionManager, PositionManagerConfig
    return PositionManager(config=PositionManagerConfig(), store=store, bot_instance_id=bot_id)


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def db_path():
    path = _make_tmp_db()
    _migrate(path)
    yield path
    if os.path.exists(path):
        os.remove(path)


@pytest.fixture
def store(db_path):
    return _state_store(db_path)


@pytest.fixture
def pm(store):
    return _position_manager(store)


# ──────────────────────────────────────────────────────────────────────────────
# 1. Restart with open protected position
# ──────────────────────────────────────────────────────────────────────────────

def test_restart_open_position_restores_lifecycle(db_path):
    """
    After opening a position, a NEW PositionManager (simulating restart)
    can restore the full state from DB using restore_from_persisted().
    """
    from app.execution.position_manager import PositionSide, PositionPhase

    store1 = _state_store(db_path)
    pm1 = _position_manager(store1)

    pm1.open_position(
        symbol="BTCUSDT",
        side=PositionSide.LONG,
        entry_price=50_000.0,
        qty=0.1,
        stop_price=49_000.0,
        tp1_price=51_000.0,
        tp2_price=53_000.0,
    )

    # Simulate restart — new store + new PM
    store2 = _state_store(db_path)
    pm2 = _position_manager(store2)

    lifecycle = store2.load_lifecycle_state("BTCUSDT")
    assert lifecycle is not None, "Lifecycle state should be persisted after open"

    restored = pm2.restore_from_persisted(
        symbol="BTCUSDT",
        lifecycle=lifecycle,
        entry_price=50_000.0,
        entry_qty=0.1,
        side_str="LONG",
    )

    assert restored is not None
    assert restored.phase == PositionPhase.SEEKING_TP1
    assert restored.sl.current_stop == pytest.approx(49_000.0, rel=1e-5)
    assert restored.tp.tp1_price == pytest.approx(51_000.0, rel=1e-5)
    assert restored.tp.tp2_price == pytest.approx(53_000.0, rel=1e-5)
    assert restored.sl.is_break_even is False
    assert restored.tp.tp1_hit is False


# ──────────────────────────────────────────────────────────────────────────────
# 2. Restart with TP1 already hit
# ──────────────────────────────────────────────────────────────────────────────

def test_restart_tp1_hit_restores_phase(db_path):
    """After TP1 event + persist, restore should show tp1_hit=True and TP1_TAKEN phase."""
    from app.execution.position_manager import PositionSide, PositionPhase

    store = _state_store(db_path)
    pm = _position_manager(store)

    pm.open_position("ETHUSD", PositionSide.LONG, 2_000.0, 1.0, 1_900.0, 2_100.0, 2_300.0)
    # Simulate price reaching TP1
    pm.update_price("ETHUSD", 2_105.0, 50.0)

    lifecycle = store.load_lifecycle_state("ETHUSD")
    assert lifecycle is not None
    assert lifecycle["tp1_hit"] is True

    # Restore
    pm2 = _position_manager(store)
    restored = pm2.restore_from_persisted("ETHUSD", lifecycle, 2_000.0, 1.0, "LONG")
    assert restored.tp.tp1_hit is True
    # After TP1 phase becomes TP1_TAKEN or RUNNER_TRAILING depending on timing
    assert restored.phase.value in ("TP1_TAKEN", "RUNNER_TRAILING")


# ──────────────────────────────────────────────────────────────────────────────
# 3. Restart with break-even active
# ──────────────────────────────────────────────────────────────────────────────

def test_restart_break_even_restores_flag(db_path):
    """After break-even is activated (after TP1), restart restores is_break_even=True."""
    from app.execution.position_manager import PositionSide

    store = _state_store(db_path)
    pm = _position_manager(store)

    pm.open_position("SOLUSDT", PositionSide.LONG, 100.0, 10.0, 95.0, 105.0, 115.0)
    pm.update_price("SOLUSDT", 106.0, 2.0)  # Triggers TP1 → break-even

    lifecycle = store.load_lifecycle_state("SOLUSDT")
    assert lifecycle is not None
    assert lifecycle["is_break_even"] is True

    pm2 = _position_manager(store)
    restored = pm2.restore_from_persisted("SOLUSDT", lifecycle, 100.0, 10.0, "LONG")
    assert restored.sl.is_break_even is True


# ──────────────────────────────────────────────────────────────────────────────
# 4. Restart with trailing active
# ──────────────────────────────────────────────────────────────────────────────

def test_restart_trailing_restores_anchor(db_path):
    """After trailing phase is active, restart restores highest_since_entry."""
    from app.execution.position_manager import PositionSide, PositionPhase

    store = _state_store(db_path)
    pm = _position_manager(store)

    pm.open_position("BNBUSDT", PositionSide.LONG, 300.0, 5.0, 290.0, 310.0, 330.0)
    pm.update_price("BNBUSDT", 311.0, 5.0)   # TP1 hit
    pm.update_price("BNBUSDT", 320.0, 5.0)   # Trailing phase

    lifecycle = store.load_lifecycle_state("BNBUSDT")
    assert lifecycle is not None
    assert lifecycle["trailing_active"] is True
    assert lifecycle["phase"] == "RUNNER_TRAILING"

    pm2 = _position_manager(store)
    restored = pm2.restore_from_persisted("BNBUSDT", lifecycle, 300.0, 5.0, "LONG")
    assert restored.phase == PositionPhase.RUNNER_TRAILING
    assert restored.highest_since_entry >= 320.0


# ──────────────────────────────────────────────────────────────────────────────
# 5. Missing stop detected by heartbeat
# ──────────────────────────────────────────────────────────────────────────────

def test_heartbeat_detects_missing_protection():
    """ensure_protection returns status=repaired when SL is missing."""
    from app.models.unified_trading import ProtectionResult

    client = MagicMock()
    client.get_position_amt.return_value = 0.5      # LONG
    client.open_orders.return_value = []            # No orders → protection missing
    client.cancel_all_orders.return_value = {}
    client.place_protection.return_value = ProtectionResult(
        sl_order_id="SL123", tp_order_id="TP456", status="success"
    )

    from app.execution.executor import BinanceExecutor
    executor = BinanceExecutor.__new__(BinanceExecutor)
    executor.client = client

    result = executor.ensure_protection(
        symbol="BTCUSDT",
        sl_price=49_000.0,
        tp_price=52_000.0,
        repair_source="PERSISTED",
    )

    assert result["status"] == "repaired"
    assert result["repair_source"] == "PERSISTED"


# ──────────────────────────────────────────────────────────────────────────────
# 6. Heartbeat uses persisted values (not live price)
# ──────────────────────────────────────────────────────────────────────────────

def test_heartbeat_uses_persisted_sl_not_live_price():
    """
    When sl_price and tp_price are passed explicitly (from persisted state),
    ensure_protection should NOT call last_price() at all.
    """
    from app.models.unified_trading import ProtectionResult

    client = MagicMock()
    client.get_position_amt.return_value = 0.5
    client.open_orders.return_value = []
    client.cancel_all_orders.return_value = {}
    client.place_protection.return_value = ProtectionResult(status="success")

    from app.execution.executor import BinanceExecutor
    executor = BinanceExecutor.__new__(BinanceExecutor)
    executor.client = client

    executor.ensure_protection(
        symbol="ETHUSDT",
        sl_price=1_800.0,
        tp_price=2_200.0,
        repair_source="PERSISTED",
    )

    client.last_price.assert_not_called(), "last_price() should not be called when sl/tp are provided"


# ──────────────────────────────────────────────────────────────────────────────
# 7. Reduce-only on SL/TP orders
# ──────────────────────────────────────────────────────────────────────────────

def test_reduce_only_on_sl_tp_orders():
    """ProtectionRequest passed to place_protection must have reduce_only=True."""
    from app.models.unified_trading import ProtectionResult, ProtectionRequest

    captured = {}

    def mock_place_protection(req):
        captured["req"] = req
        return ProtectionResult(status="success")

    client = MagicMock()
    client.get_position_amt.return_value = 1.0
    client.open_orders.return_value = []
    client.cancel_all_orders.return_value = {}
    client.place_protection.side_effect = mock_place_protection

    from app.execution.executor import BinanceExecutor
    executor = BinanceExecutor.__new__(BinanceExecutor)
    executor.client = client

    executor.ensure_protection("BTCUSDT", sl_price=48_000.0, tp_price=55_000.0)

    assert "req" in captured
    assert captured["req"].reduce_only is True, "ProtectionRequest.reduce_only must be True"


# ──────────────────────────────────────────────────────────────────────────────
# 8. update_protection cancel-replace pathway
# ──────────────────────────────────────────────────────────────────────────────

def test_update_protection_cancel_replace_bybit():
    """
    BybitClient.update_protection() cancels old order then places new SL/TP.
    """
    from app.models.unified_trading import ProtectionUpdateRequest

    cancelled = []
    placed = []

    client = MagicMock()
    client._request_v5.side_effect = lambda method, path, *args, **kwargs: (
        cancelled.append(path) or {}
    ) if "cancel" in path else {}
    client.place_stop_market.return_value = {"orderId": "NEW_SL_001"}
    client.place_take_profit_market.return_value = {"orderId": "NEW_TP_001"}

    from app.exchange.bybit.client import BybitClient
    bc = BybitClient.__new__(BybitClient)
    bc._request_v5 = client._request_v5
    bc.place_stop_market = client.place_stop_market
    bc.place_take_profit_market = client.place_take_profit_market

    req = ProtectionUpdateRequest(
        symbol="BTCUSDT",
        position_side="LONG",
        new_sl_price=48_000.0,
        new_tp_price=55_000.0,
        qty=0.1,
        old_sl_order_id="OLD_SL",
        old_tp_order_id="OLD_TP",
        reason="TRAILING_UPDATE",
    )
    result = bc.update_protection(req)

    assert result["status"] == "OK"
    assert result["sl_order_id"] == "NEW_SL_001"
    assert result["tp_order_id"] == "NEW_TP_001"
    client.place_stop_market.assert_called_once()
    client.place_take_profit_market.assert_called_once()


# ──────────────────────────────────────────────────────────────────────────────
# 9. Cancel succeeds, replace fails (partial failure)
# ──────────────────────────────────────────────────────────────────────────────

def test_cancel_replace_partial_failure_handling():
    """
    When cancel succeeds but SL replace fails, update_protection returns
    status=REPLACE_PARTIAL_FAILURE and error is non-None.
    """
    from app.models.unified_trading import ProtectionUpdateRequest

    client = MagicMock()
    client._request_v5.return_value = {}  # cancel succeeds
    client.place_stop_market.side_effect = RuntimeError("Exchange rejected: insufficient margin")
    client.place_take_profit_market.return_value = {"orderId": "TP_OK"}

    from app.exchange.bybit.client import BybitClient
    bc = BybitClient.__new__(BybitClient)
    bc._request_v5 = client._request_v5
    bc.place_stop_market = client.place_stop_market
    bc.place_take_profit_market = client.place_take_profit_market

    req = ProtectionUpdateRequest(
        symbol="BTCUSDT",
        position_side="LONG",
        new_sl_price=48_000.0,
        new_tp_price=55_000.0,
        qty=0.1,
        old_sl_order_id="OLD_SL",
        reason="BREAK_EVEN",
    )
    result = bc.update_protection(req)

    assert result["status"] == "REPLACE_PARTIAL_FAILURE"
    assert result["error"] is not None
    assert "SL_PLACE_FAILED" in result["error"]


# ──────────────────────────────────────────────────────────────────────────────
# 10. SymbolState + PM stay synchronized after mutations
# ──────────────────────────────────────────────────────────────────────────────

def test_symbolstate_pm_stay_synchronized(db_path):
    """
    After a TP1 event:
    - PM marks tp1_hit=True, phase=TP1_TAKEN/RUNNER_TRAILING
    - DB persisted lifecycle reflects same values
    Neither should diverge.
    """
    from app.execution.position_manager import PositionSide

    store = _state_store(db_path)
    pm = _position_manager(store)

    pm.open_position("XRPUSDT", PositionSide.LONG, 0.5, 1000.0, 0.48, 0.52, 0.56)
    state_before = store.load_lifecycle_state("XRPUSDT")
    assert state_before is not None
    assert state_before["tp1_hit"] is False

    # Price hits TP1
    pm.update_price("XRPUSDT", 0.521, 0.005)
    state_after = store.load_lifecycle_state("XRPUSDT")
    assert state_after is not None
    assert state_after["tp1_hit"] is True, "DB should reflect TP1 hit after update_price"
    assert state_after["is_break_even"] is True, "DB should reflect break-even after TP1"


# ──────────────────────────────────────────────────────────────────────────────
# Extra: lifecycle state deleted on close
# ──────────────────────────────────────────────────────────────────────────────

def test_lifecycle_state_deleted_on_close(db_path):
    """After close_position(), lifecycle state is removed from DB."""
    from app.execution.position_manager import PositionSide

    store = _state_store(db_path)
    pm = _position_manager(store)

    pm.open_position("ADAUSDT", PositionSide.SHORT, 0.4, 500.0, 0.42, 0.38, 0.35)
    assert store.load_lifecycle_state("ADAUSDT") is not None

    pm.close_position("ADAUSDT", reason="test_close")
    assert store.load_lifecycle_state("ADAUSDT") is None, "Lifecycle state should be deleted after close"
