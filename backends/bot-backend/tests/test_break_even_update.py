"""
Phase 3 Step 3D — Break-Even Stop Update Validation Tests.

Covers all 10 spec scenarios (Sections 9 & 10):
  1.  Long runner activates break-even successfully
  2.  Short runner activates break-even successfully
  3.  Break-even applies correct fee/slippage buffer (long & short)
  4.  Current stop already tighter → skipped safely
  5.  Break-even update succeeds and order IDs rotate correctly
  6.  Cancel succeeds but replace fails (REPLACE_PARTIAL_FAILURE)
  7.  Restart during BREAK_EVEN_PENDING heals correctly
  8.  Duplicate trigger is ignored (be_exchange_confirmed=True)
  9.  Broker quantity differs from internal runner qty → broker wins
 10.  Break-even does not activate before TP1 confirmation

Run with:
    python -m pytest tests/test_break_even_update.py -v
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_filters(tick_size="0.10"):
    f = MagicMock()
    f.tick_size = float(tick_size)
    return f


def _make_client(
    broker_qty: float = 0.5,
    update_protection_rv=None,
    update_protection_raises=None,
):
    """Build a mock ExchangeClient with common BE behaviours."""
    client = MagicMock()
    client.get_position_amt.return_value = broker_qty
    client.get_symbol_filters.return_value = _make_filters()
    if update_protection_raises is not None:
        client.update_protection.side_effect = update_protection_raises
    else:
        client.update_protection.return_value = (
            update_protection_rv
            or {"status": "OK", "sl_order_id": "SL_NEW", "tp_order_id": "TP_NEW"}
        )
    return client


def _make_executor(client):
    from app.execution.executor import BinanceExecutor
    ex = BinanceExecutor.__new__(BinanceExecutor)
    ex.client = client
    ex.circuit = MagicMock(is_tripped=lambda: False)
    ex.audit = None
    ex.run_id = None
    ex.execution_mode = "live"
    ex._live_symbols_override = {"BTCUSDT"}
    return ex


def _make_pm_in_runner_trailing(
    symbol="BTCUSDT",
    side="LONG",
    entry_price: float = 50_000.0,
    current_stop: float = 49_000.0,
    runner_qty: float = 0.5,
    tp1_hit: bool = True,
    be_confirmed: bool = False,
    phase_override=None,
):
    """Return a PositionManager with a post-TP1 runner position."""
    from app.execution.position_manager import (
        PositionManager, PositionPhase, PositionSide,
    )
    pm = PositionManager()
    ps = PositionSide.LONG if side == "LONG" else PositionSide.SHORT
    tp1_price = entry_price * 1.02 if ps == PositionSide.LONG else entry_price * 0.98
    tp2_price = entry_price * 1.05 if ps == PositionSide.LONG else entry_price * 0.95
    sl_price  = current_stop
    pm.open_position(symbol, ps, entry_price, runner_qty * 2, sl_price, tp1_price, tp2_price)

    pos = pm.get_position(symbol)
    pos.current_qty = runner_qty
    pos.tp.tp1_hit = tp1_hit
    pos.sl.be_exchange_confirmed = be_confirmed
    pos.sl.sl_order_id = "SL_ORIG"
    pos.sl.tp_order_id = "TP_ORIG"
    pos.sl.current_stop = current_stop
    pos.phase = phase_override or PositionPhase.RUNNER_TRAILING
    return pm


# ──────────────────────────────────────────────────────────────────────────────
# 1. Long runner activates break-even successfully
# ──────────────────────────────────────────────────────────────────────────────

def test_be_long_success():
    """
    LONG runner: execute_break_even_update() calls update_protection(), 
    sets be_exchange_confirmed=True, transitions to RUNNER_TRAILING.
    """
    from app.execution.position_manager import PositionPhase

    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm_in_runner_trailing(side="LONG", entry_price=50_000.0, current_stop=49_000.0)

    result = ex.execute_break_even_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=49_000.0,
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=52_500.0,
        position_manager=pm,
    )

    assert result["break_even_applied"] is True, f"BE should be applied: {result}"
    assert result["lifecycle_state_after"] == "RUNNER_TRAILING"
    assert result["protection_update_status"] == "OK"

    pos = pm.get_position("BTCUSDT")
    assert pos.sl.be_exchange_confirmed is True
    assert pos.sl.is_break_even is True
    assert pos.sl.sl_order_id == "SL_NEW"
    assert pos.sl.tp_order_id == "TP_NEW"
    assert pos.phase == PositionPhase.RUNNER_TRAILING

    # update_protection must be called once with BREAK_EVEN reason
    client.update_protection.assert_called_once()
    req = client.update_protection.call_args[0][0]
    assert req.reason == "BREAK_EVEN"


# ──────────────────────────────────────────────────────────────────────────────
# 2. Short runner activates break-even successfully
# ──────────────────────────────────────────────────────────────────────────────

def test_be_short_success():
    """
    SHORT runner: BE stop is BELOW entry (inverted direction), still applied.
    """
    from app.models.unified_trading import ProtectionUpdateRequest

    entry = 50_000.0
    captured_req = {}

    client = _make_client(broker_qty=0.5)
    def capture_prot(req):
        captured_req["req"] = req
        return {"status": "OK", "sl_order_id": "SL_SHORT_NEW", "tp_order_id": "TP_SHORT_NEW"}
    client.update_protection.side_effect = capture_prot

    ex = _make_executor(client)
    pm = _make_pm_in_runner_trailing(
        side="SHORT", entry_price=entry, current_stop=entry * 1.02
    )

    result = ex.execute_break_even_update(
        symbol="BTCUSDT",
        position_side="SHORT",
        runner_qty=0.5,
        entry_price=entry,
        current_stop=entry * 1.02,  # stop is above entry for SHORT
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=47_500.0,
        position_manager=pm,
    )

    assert result["break_even_applied"] is True, f"SHORT BE should be applied: {result}"
    assert result["lifecycle_state_after"] == "RUNNER_TRAILING"

    # BE stop must be BELOW entry for SHORT (entry - buffer)
    norm_be = result["normalized_break_even_price"]
    assert norm_be < entry, f"SHORT BE stop {norm_be} must be below entry {entry}"


# ──────────────────────────────────────────────────────────────────────────────
# 3. Break-even applies correct fee/slippage buffer
# ──────────────────────────────────────────────────────────────────────────────

def test_be_fee_buffer_applied_long():
    """
    For LONG: buffered_be = entry + (entry * taker_fee * 2 * buffer_mult)
    Raw BE = entry_price, buffered BE > entry_price.
    """
    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm_in_runner_trailing(entry_price=40_000.0, current_stop=38_000.0)

    result = ex.execute_break_even_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=40_000.0,
        current_stop=38_000.0,
        fee_buffer_mult=1.2,
        taker_fee_rate=0.0005,
        tp2_price=42_000.0,
        position_manager=pm,
    )

    assert result["raw_break_even_price"] == pytest.approx(40_000.0)
    expected_buffer = 40_000.0 * 0.0005 * 2.0 * 1.2
    expected_buffered = 40_000.0 + expected_buffer
    assert result["buffered_break_even_price"] == pytest.approx(expected_buffered, rel=1e-4)
    assert result["normalized_break_even_price"] >= result["buffered_break_even_price"] or \
           result["normalized_break_even_price"] == pytest.approx(result["buffered_break_even_price"], rel=0.01)


def test_be_fee_buffer_applied_short():
    """
    For SHORT: buffered_be = entry - buffer, so buffered BE < entry_price.
    """
    entry = 40_000.0
    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm_in_runner_trailing(
        side="SHORT", entry_price=entry, current_stop=entry * 1.03
    )

    result = ex.execute_break_even_update(
        symbol="BTCUSDT",
        position_side="SHORT",
        runner_qty=0.5,
        entry_price=entry,
        current_stop=entry * 1.03,
        fee_buffer_mult=1.2,
        taker_fee_rate=0.0005,
        tp2_price=38_000.0,
        position_manager=pm,
    )

    assert result["buffered_break_even_price"] < entry, \
        "For SHORT, buffered BE must be below entry"


# ──────────────────────────────────────────────────────────────────────────────
# 4. Current stop already tighter → skipped safely
# ──────────────────────────────────────────────────────────────────────────────

def test_be_skipped_when_stop_already_tighter():
    """
    If current_stop > proposed BE stop for LONG → skip (BE_WOULD_LOOSEN_STOP).
    The executor must never widen risk.
    """
    # For LONG, current_stop must be GREATER than norm_be to trigger the skip.
    # entry=50k, buffer~60, norm_be~50060. Set current_stop=50_500 (already tighter).
    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm_in_runner_trailing(entry_price=50_000.0, current_stop=50_500.0)

    result = ex.execute_break_even_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=50_500.0,   # already tighter than BE
        position_manager=pm,
    )

    assert result["break_even_applied"] is False
    assert result["skip_reason"] == "BE_WOULD_LOOSEN_STOP"
    client.update_protection.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# 5. Order IDs rotate correctly on success
# ──────────────────────────────────────────────────────────────────────────────

def test_be_order_ids_rotate_on_success():
    """
    After successful update_protection, PM state must hold NEW order IDs,
    not the original ones.
    """
    client = _make_client(
        broker_qty=0.5,
        update_protection_rv={"status": "OK", "sl_order_id": "SL_V2", "tp_order_id": "TP_V2"},
    )
    ex = _make_executor(client)
    pm = _make_pm_in_runner_trailing(entry_price=50_000.0, current_stop=49_000.0)

    ex.execute_break_even_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=49_000.0,
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=52_000.0,
        position_manager=pm,
    )

    pos = pm.get_position("BTCUSDT")
    assert pos.sl.sl_order_id == "SL_V2", "sl_order_id must update to new value"
    assert pos.sl.tp_order_id == "TP_V2", "tp_order_id must update to new value"


# ──────────────────────────────────────────────────────────────────────────────
# 6. Cancel succeeds but replace fails (REPLACE_PARTIAL_FAILURE)
# ──────────────────────────────────────────────────────────────────────────────

def test_be_protection_update_failure_rolls_back_state():
    """
    When update_protection() raises, the phase must be reverted to the
    pre-update phase and be_exchange_confirmed must stay False.
    """
    from app.execution.position_manager import PositionPhase

    client = _make_client(
        broker_qty=0.5,
        update_protection_raises=RuntimeError("REPLACE_PARTIAL_FAILURE"),
    )
    ex = _make_executor(client)
    pm = _make_pm_in_runner_trailing(
        entry_price=50_000.0, current_stop=49_000.0,
        phase_override=PositionPhase.RUNNER_TRAILING,
    )

    result = ex.execute_break_even_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=49_000.0,
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        position_manager=pm,
    )

    assert result["break_even_applied"] is False
    assert "UPDATE_PROTECTION_FAILED" in (result.get("failure_reason") or "")

    pos = pm.get_position("BTCUSDT")
    assert pos.sl.be_exchange_confirmed is False, "Must NOT mark confirmed on failure"
    assert pos.phase == PositionPhase.RUNNER_TRAILING, "Phase must revert on failure"
    # Order IDs must not be replaced with new (failed) values
    assert pos.sl.sl_order_id == "SL_ORIG"


# ──────────────────────────────────────────────────────────────────────────────
# 7. Restart during BREAK_EVEN_PENDING heals correctly
# ──────────────────────────────────────────────────────────────────────────────

def test_reconcile_break_even_pending_retries_when_not_confirmed():
    """
    _reconcile_break_even_pending() with be_exchange_confirmed=False should
    call execute_break_even_update() and advance the phase.
    """
    from app.execution.position_manager import (
        PositionManager, PositionPhase, PositionSide,
    )

    pm = PositionManager()
    pm.open_position("BTCUSDT", PositionSide.LONG, 50_000.0, 1.0, 49_000.0, 51_000.0, 53_000.0)
    pos = pm.get_position("BTCUSDT")
    pos.phase = PositionPhase.BREAK_EVEN_PENDING
    pos.tp.tp1_hit = True
    pos.sl.be_exchange_confirmed = False
    pos.current_qty = 0.5

    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)

    # Build runner stub
    from app.runner.runner import PaperRunner
    runner = PaperRunner.__new__(PaperRunner)
    runner.client = client
    runner.position_manager = pm
    runner.executor = ex

    runner._reconcile_break_even_pending("BTCUSDT", pos)

    # update_protection must have been called (BE retry on restart)
    client.update_protection.assert_called_once()
    req = client.update_protection.call_args[0][0]
    assert req.reason == "BREAK_EVEN"


def test_reconcile_break_even_pending_advances_when_already_confirmed():
    """
    _reconcile_break_even_pending() with be_exchange_confirmed=True should
    immediately advance to RUNNER_TRAILING without calling update_protection.
    """
    from app.execution.position_manager import (
        PositionManager, PositionPhase, PositionSide,
    )

    pm = PositionManager()
    pm.open_position("BTCUSDT", PositionSide.LONG, 50_000.0, 1.0, 49_000.0, 51_000.0, 53_000.0)
    pos = pm.get_position("BTCUSDT")
    pos.phase = PositionPhase.BREAK_EVEN_PENDING
    pos.sl.be_exchange_confirmed = True   # exchange already knows
    pos.current_qty = 0.5

    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)

    from app.runner.runner import PaperRunner
    runner = PaperRunner.__new__(PaperRunner)
    runner.client = client
    runner.position_manager = pm
    runner.executor = ex

    runner._reconcile_break_even_pending("BTCUSDT", pos)

    # Phase must advance without touching the exchange
    assert pos.phase == PositionPhase.RUNNER_TRAILING
    client.update_protection.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# 8. Duplicate trigger is ignored (be_exchange_confirmed=True)
# ──────────────────────────────────────────────────────────────────────────────

def test_be_duplicate_trigger_ignored():
    """
    Calling execute_break_even_update() when be_exchange_confirmed=True
    must return immediately with skip_reason=BE_ALREADY_CONFIRMED.
    """
    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm_in_runner_trailing(
        entry_price=50_000.0, current_stop=49_000.0, be_confirmed=True
    )

    result = ex.execute_break_even_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=49_000.0,
        position_manager=pm,
    )

    assert result["break_even_applied"] is False
    assert result["skip_reason"] == "BE_ALREADY_CONFIRMED"
    client.update_protection.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# 9. Broker quantity differs from internal runner qty → broker wins
# ──────────────────────────────────────────────────────────────────────────────

def test_be_broker_qty_divergence_broker_wins():
    """
    If internal runner_qty=0.5 but broker says 0.4, the update_protection
    call must use 0.4 (the broker truth).
    """
    client = _make_client(broker_qty=0.4)  # broker has less than internal 0.5
    ex = _make_executor(client)
    pm = _make_pm_in_runner_trailing(
        entry_price=50_000.0, current_stop=49_000.0, runner_qty=0.5
    )

    result = ex.execute_break_even_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,          # internal qty
        entry_price=50_000.0,
        current_stop=49_000.0,
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=52_000.0,
        position_manager=pm,
    )

    assert result["break_even_applied"] is True
    # live_qty in result should reflect broker truth (0.4), not internal (0.5)
    assert result["live_qty"] == pytest.approx(0.4, rel=0.01)
    # The update_protection call should use qty=0.4
    req = client.update_protection.call_args[0][0]
    assert float(req.qty) == pytest.approx(0.4, rel=0.01)


# ──────────────────────────────────────────────────────────────────────────────
# 10. Break-even does not activate before TP1 confirmation
# ──────────────────────────────────────────────────────────────────────────────

def test_be_not_activated_before_tp1_confirmed():
    """
    When tp1_hit=False, execute_break_even_update() must return
    skip_reason=TP1_NOT_CONFIRMED and NOT call update_protection.
    """
    client = _make_client(broker_qty=1.0)
    ex = _make_executor(client)
    pm = _make_pm_in_runner_trailing(
        tp1_hit=False,           # TP1 not confirmed
        current_stop=49_000.0,
    )

    result = ex.execute_break_even_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=1.0,
        entry_price=50_000.0,
        current_stop=49_000.0,
        position_manager=pm,
    )

    assert result["break_even_applied"] is False
    assert result["skip_reason"] == "TP1_NOT_CONFIRMED"
    client.update_protection.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# 11. Zero-quantity guard
# ──────────────────────────────────────────────────────────────────────────────

def test_be_skipped_when_qty_zero():
    """
    If runner_qty=0, BE must skip with QTY_ZERO (no orders to protect).
    """
    client = _make_client(broker_qty=0.0)
    ex = _make_executor(client)
    pm = _make_pm_in_runner_trailing(runner_qty=0.5)

    result = ex.execute_break_even_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.0,          # zero qty
        entry_price=50_000.0,
        current_stop=49_000.0,
        position_manager=pm,
    )

    assert result["break_even_applied"] is False
    assert result["skip_reason"] in ("QTY_ZERO", "BROKER_QTY_ZERO")
    client.update_protection.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# 12. Normalisation: BE stop rounds correctly for long vs short
# ──────────────────────────────────────────────────────────────────────────────

def test_be_normalize_stop_price_long():
    """
    For LONG, _normalize_be_stop_price rounds UP so BE stop is never
    placed below the raw buffered price.
    """
    from app.execution.executor import BinanceExecutor
    ex = BinanceExecutor.__new__(BinanceExecutor)
    client = MagicMock()
    f = MagicMock()
    f.tick_size = 0.1
    client.get_symbol_filters.return_value = f
    ex.client = client

    raw = 50_000.12   # not aligned to 0.1 tick
    norm, status = ex._normalize_be_stop_price("BTCUSDT", raw, "LONG")
    assert status == "OK"
    assert norm >= raw, f"LONG normalization must round UP: {norm} < {raw}"
    # Check alignment
    from decimal import Decimal
    tick = Decimal("0.1")
    assert Decimal(str(norm)) % tick == 0, f"{norm} not aligned to tick {tick}"


def test_be_normalize_stop_price_short():
    """
    For SHORT, _normalize_be_stop_price rounds DOWN so BE stop is never
    placed above the raw buffered price.
    """
    from app.execution.executor import BinanceExecutor
    ex = BinanceExecutor.__new__(BinanceExecutor)
    client = MagicMock()
    f = MagicMock()
    f.tick_size = 0.1
    client.get_symbol_filters.return_value = f
    ex.client = client

    raw = 49_999.88
    norm, status = ex._normalize_be_stop_price("BTCUSDT", raw, "SHORT")
    assert status == "OK"
    assert norm <= raw, f"SHORT normalization must round DOWN: {norm} > {raw}"
