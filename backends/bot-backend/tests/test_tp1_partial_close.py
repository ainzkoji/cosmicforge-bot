"""
Phase 3 Step 3C — TP1 Partial Close Validation Tests.

Covers all 10 spec scenarios (Sections 8 & 9):
  1.  Long position hits TP1 normally
  2.  Short position hits TP1 normally
  3.  TP1 quantity normalizes correctly (step_size rounding)
  4.  TP1 quantity too small after normalization → promoted to full close
  5.  TP1 close partially fills (broker reports less than requested)
  6.  TP1 close fills and protection is resized correctly
  7.  Protection resize fails after TP1 → emergency ensure_protection called
  8.  Restart during TP1_EXECUTING → self-healed by _reconcile_tp1_executing
  9.  Duplicate TP1 trigger is ignored safely
  10. Broker live quantity differs from internal state → broker qty wins

Run with:
    python -m pytest tests/test_tp1_partial_close.py -v
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch, call

import pytest

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_filters(step_size="0.001", min_qty="0.001", min_notional="5.0"):
    f = MagicMock()
    f.step_size = float(step_size)
    f.min_qty = float(min_qty)
    f.min_notional = float(min_notional)
    return f


def _make_capabilities(supports_reduce_only=True):
    c = MagicMock()
    c.supports_reduce_only = supports_reduce_only
    return c


def _make_client(
    pos_amt=1.0,
    post_pos_amt=0.5,
    filters=None,
    price=50_000.0,
    place_order_rv=None,
    update_protection_rv=None,
):
    """Build a mock ExchangeClient with common behaviours."""
    client = MagicMock()
    client.get_position_amt.side_effect = [pos_amt, post_pos_amt]  # before, then after place_order
    client.get_prices.return_value = {"BTCUSDT": price}
    client.get_symbol_filters.return_value = filters or _make_filters()
    client.capabilities = _make_capabilities()
    client.place_order.return_value = place_order_rv or MagicMock(
        model_dump=lambda: {"orderId": "ORD001", "status": "FILLED"}
    )
    client.update_protection.return_value = {
        "status": "OK", "sl_order_id": "SL_NEW", "tp_order_id": "TP_NEW"
    }
    client.cancel_all_orders.return_value = {}
    client.close_position_market.return_value = {"orderId": "FULL_CLOSE_001"}
    if update_protection_rv is not None:
        client.update_protection.return_value = update_protection_rv
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


def _make_pm_in_seeking_tp1(symbol="BTCUSDT"):
    """Return a PositionManager with a SEEKING_TP1 position open."""
    from app.execution.position_manager import (
        PositionManager, PositionPhase, PositionSide
    )
    pm = PositionManager()
    pm.open_position(symbol, PositionSide.LONG, 50_000.0, 1.0, 49_000.0, 51_000.0, 53_000.0)
    pos = pm.get_position(symbol)
    # Hardcode order IDs as if protection was placed at entry
    pos.sl.sl_order_id = "SL_ORIG"
    pos.sl.tp_order_id = "TP_ORIG"
    return pm


# ──────────────────────────────────────────────────────────────────────────────
# 1. Long position hits TP1 normally
# ──────────────────────────────────────────────────────────────────────────────

def test_tp1_long_partial_close_success():
    """
    Normal LONG TP1: places a reduce-only SELL market order,
    reconciles fill, resizes protection, transitions to RUNNER_TRAILING.
    """
    from app.execution.position_manager import PositionPhase

    client = _make_client(pos_amt=1.0, post_pos_amt=0.5)
    ex = _make_executor(client)
    pm = _make_pm_in_seeking_tp1()

    result = ex.execute_tp1_partial_close(
        symbol="BTCUSDT",
        live_qty=1.0,
        position_side="LONG",
        sl_price=49_000.0,
        tp_price=53_000.0,
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp1_fraction=0.5,
        position_manager=pm,
    )

    assert not result["skipped"], f"Should not skip: {result}"
    assert not result["promoted"], f"Should not promote: {result}"
    assert result["fill_qty"] == pytest.approx(0.5, rel=0.1)
    assert result["runner_qty"] == pytest.approx(0.5, rel=0.1)
    assert result["failure_reason"] is None
    assert result["reduce_only_status"] == "ENFORCED"

    # PM transitions to RUNNER_TRAILING
    pos = pm.get_position("BTCUSDT")
    assert pos.phase == PositionPhase.RUNNER_TRAILING
    assert pos.tp.tp1_hit is True
    assert pos.current_qty == pytest.approx(0.5, rel=0.1)

    # update_protection was called for the runner
    client.update_protection.assert_called_once()


# ──────────────────────────────────────────────────────────────────────────────
# 2. Short position hits TP1 normally
# ──────────────────────────────────────────────────────────────────────────────

def test_tp1_short_partial_close_success():
    """SHORT TP1: uses BUY exit side (opposite of SHORT)."""
    from app.execution.position_manager import (
        PositionManager, PositionSide, PositionPhase
    )
    from app.models.unified_trading import Side

    client = _make_client(pos_amt=-1.0, post_pos_amt=-0.5)
    client.get_position_amt.side_effect = [-1.0, -0.5]
    ex = _make_executor(client)

    pm = PositionManager()
    pm.open_position("BTCUSDT", PositionSide.SHORT, 50_000.0, 1.0, 51_000.0, 49_000.0, 47_000.0)
    pos = pm.get_position("BTCUSDT")
    pos.sl.sl_order_id = "SL_SHORT"
    pos.sl.tp_order_id = "TP_SHORT"

    result = ex.execute_tp1_partial_close(
        symbol="BTCUSDT",
        live_qty=1.0,
        position_side="SHORT",
        sl_price=51_000.0,
        tp_price=47_000.0,
        position_manager=pm,
    )

    assert not result["skipped"]
    assert not result["promoted"]

    # Verify a BUY order was placed (exit side for SHORT)
    placed_req = client.place_order.call_args[0][0]
    assert placed_req.side == Side.BUY


# ──────────────────────────────────────────────────────────────────────────────
# 3. TP1 quantity normalizes correctly
# ──────────────────────────────────────────────────────────────────────────────

def test_tp1_qty_normalization():
    """Raw qty 0.5050 should round DOWN to 0.505 with step_size=0.001."""
    from app.execution.executor import BinanceExecutor
    ex = BinanceExecutor.__new__(BinanceExecutor)
    
    client = MagicMock()
    client.get_symbol_filters.return_value = _make_filters(step_size="0.001", min_qty="0.001", min_notional="5")
    client.get_prices.return_value = {"BTCUSDT": 50_000.0}
    ex.client = client

    norm_qty, status = ex._normalize_partial_close_qty("BTCUSDT", 0.5059)
    assert status == "OK"
    # 0.5059 / 0.001 = 505.9 → floor → 505 → 0.505
    assert norm_qty == pytest.approx(0.505, rel=1e-6)


# ──────────────────────────────────────────────────────────────────────────────
# 4. TP1 quantity too small → promoted to full close
# ──────────────────────────────────────────────────────────────────────────────

def test_tp1_promoted_to_full_close_when_too_small():
    """
    When normalized TP1 qty < min_qty, must promote to full market close.
    """
    filters = _make_filters(step_size="1.0", min_qty="0.5", min_notional="5.0")
    client = _make_client(filters=filters)
    client.get_position_amt.return_value = 0.01  # Very small position
    ex = _make_executor(client)

    result = ex.execute_tp1_partial_close(
        symbol="BTCUSDT",
        live_qty=0.01,         # 50% = 0.005 → normalized=0 < min_qty=0.5 → TOO_SMALL
        position_side="LONG",
        sl_price=49_000.0,
        tp_price=53_000.0,
    )

    assert result["promoted"] is True
    assert "PROMOTED_TO_FULL_CLOSE" in (result.get("failure_reason") or "")
    client.close_position_market.assert_called_once()


# ──────────────────────────────────────────────────────────────────────────────
# 5. TP1 close partially fills
# ──────────────────────────────────────────────────────────────────────────────

def test_tp1_partial_fill_uses_broker_qty():
    """
    If broker reports post-close qty only dropped by 0.3 (not 0.5),
    fill_qty = 0.3 and runner_qty = 0.7.
    """
    client = _make_client(pos_amt=1.0, post_pos_amt=0.7)  # filled only 0.3
    ex = _make_executor(client)
    pm = _make_pm_in_seeking_tp1()

    result = ex.execute_tp1_partial_close(
        symbol="BTCUSDT",
        live_qty=1.0,
        position_side="LONG",
        sl_price=49_000.0,
        tp_price=53_000.0,
        tp1_fraction=0.5,
        position_manager=pm,
    )

    assert result["fill_qty"] == pytest.approx(0.3, rel=1e-5)
    assert result["runner_qty"] == pytest.approx(0.7, rel=1e-5)


# ──────────────────────────────────────────────────────────────────────────────
# 6. TP1 fills and protection is resized correctly
# ──────────────────────────────────────────────────────────────────────────────

def test_tp1_protection_resized_after_fill():
    """update_protection called with runner qty and TP1_PARTIAL_CLOSE reason."""
    client = _make_client()
    ex = _make_executor(client)
    pm = _make_pm_in_seeking_tp1()

    ex.execute_tp1_partial_close(
        symbol="BTCUSDT",
        live_qty=1.0,
        position_side="LONG",
        sl_price=49_000.0,
        tp_price=53_000.0,
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp1_fraction=0.5,
        position_manager=pm,
    )

    call_kwargs = client.update_protection.call_args[0][0]
    assert call_kwargs.reason == "TP1_PARTIAL_CLOSE"
    assert call_kwargs.old_sl_order_id == "SL_ORIG"
    assert call_kwargs.old_tp_order_id == "TP_ORIG"
    # runner qty should be 0.5
    assert call_kwargs.qty == pytest.approx(0.5, rel=0.1)


# ──────────────────────────────────────────────────────────────────────────────
# 7. Protection resize fails after TP1
# ──────────────────────────────────────────────────────────────────────────────

def test_tp1_prot_update_failure_triggers_emergency_ensure():
    """When update_protection raises, ensure_protection is called as fallback."""
    client = _make_client()
    client.update_protection.side_effect = RuntimeError("Exchange error")
    # ensure_protection will call open_orders, cancel_all_orders, etc.
    client.open_orders.return_value = []
    client.place_protection.return_value = MagicMock(status="success")

    ex = _make_executor(client)

    # patch ensure_protection to verify it's called
    called = {}
    original_ep = ex.ensure_protection

    def mock_ep(*a, **kw):
        called["invoked"] = True
        called["source"] = kw.get("repair_source")

    ex.ensure_protection = mock_ep

    result = ex.execute_tp1_partial_close(
        symbol="BTCUSDT",
        live_qty=1.0,
        position_side="LONG",
        sl_price=49_000.0,
        tp_price=53_000.0,
    )

    assert called.get("invoked") is True, "ensure_protection should be called as emergency fallback"
    assert called.get("source") == "TP1_EMERGENCY_REPAIR"
    assert "FAILED" in result["protection_update_status"]


# ──────────────────────────────────────────────────────────────────────────────
# 8. Restart during TP1_EXECUTING → _reconcile_tp1_executing heals
# ──────────────────────────────────────────────────────────────────────────────

def test_reconcile_tp1_executing_heals_to_runner_trailing():
    """
    If phase=TP1_EXECUTING and broker qty reduced → advance to RUNNER_TRAILING.
    """
    from app.execution.position_manager import (
        PositionManager, PositionSide, PositionPhase
    )

    pm = PositionManager()
    pm.open_position("BTCUSDT", PositionSide.LONG, 50_000.0, 1.0, 49_000.0, 51_000.0, 53_000.0)
    pos = pm.get_position("BTCUSDT")
    pos.phase = PositionPhase.TP1_EXECUTING
    pos.tp1_exec_qty = 0.5
    pos.entry_qty = 1.0
    pos.current_qty = 1.0

    client = MagicMock()
    client.get_position_amt.return_value = 0.5   # qty reduced by 0.5 — TP1 filled
    client.open_orders.return_value = []
    client.place_protection.return_value = MagicMock(status="success")

    # Build a minimal runner stub
    from app.runner.runner import PaperRunner
    runner = PaperRunner.__new__(PaperRunner)
    runner.client = client
    runner.position_manager = pm
    runner.executor = _make_executor(client)
    runner.executor.ensure_protection = MagicMock(return_value={"status": "ok"})

    runner._reconcile_tp1_executing("BTCUSDT", pos)

    pos_after = pm.get_position("BTCUSDT")
    assert pos_after.phase == PositionPhase.RUNNER_TRAILING
    assert pos_after.tp.tp1_hit is True
    assert pos_after.current_qty == pytest.approx(0.5, rel=0.1)


def test_reconcile_tp1_executing_reverts_if_no_fill():
    """
    If phase=TP1_EXECUTING and broker qty unchanged → revert to SEEKING_TP1.
    """
    from app.execution.position_manager import (
        PositionManager, PositionSide, PositionPhase
    )

    pm = PositionManager()
    pm.open_position("BTCUSDT", PositionSide.LONG, 50_000.0, 1.0, 49_000.0, 51_000.0, 53_000.0)
    pos = pm.get_position("BTCUSDT")
    pos.phase = PositionPhase.TP1_EXECUTING
    pos.tp1_exec_qty = 0.5
    pos.entry_qty = 1.0
    pos.current_qty = 1.0

    client = MagicMock()
    client.get_position_amt.return_value = 1.0   # qty unchanged — TP1 not filled

    from app.runner.runner import PaperRunner
    runner = PaperRunner.__new__(PaperRunner)
    runner.client = client
    runner.position_manager = pm
    runner.executor = _make_executor(client)

    runner._reconcile_tp1_executing("BTCUSDT", pos)

    pos_after = pm.get_position("BTCUSDT")
    assert pos_after.phase == PositionPhase.SEEKING_TP1


# ──────────────────────────────────────────────────────────────────────────────
# 9. Duplicate TP1 trigger is ignored safely
# ──────────────────────────────────────────────────────────────────────────────

def test_tp1_duplicate_trigger_ignored():
    """Calling execute_tp1_partial_close when already RUNNER_TRAILING → skipped."""
    from app.execution.position_manager import (
        PositionManager, PositionSide, PositionPhase
    )

    client = _make_client()
    ex = _make_executor(client)
    pm = _make_pm_in_seeking_tp1()

    # Force phase to already-executed
    pos = pm.get_position("BTCUSDT")
    pos.phase = PositionPhase.RUNNER_TRAILING

    result = ex.execute_tp1_partial_close(
        symbol="BTCUSDT",
        live_qty=1.0,
        position_side="LONG",
        sl_price=49_000.0,
        tp_price=53_000.0,
        position_manager=pm,
    )

    assert result["skipped"] is True
    assert result["failure_reason"] == "TP1_DUPLICATE_IGNORED"
    client.place_order.assert_not_called()
    client.update_protection.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# 10. Broker qty differs from internal state → broker wins
# ──────────────────────────────────────────────────────────────────────────────

def test_tp1_broker_qty_divergence_broker_wins():
    """
    If broker reports 0.8 but internal was 1.0 → should use 0.8 for calculation.
    """
    client = _make_client(pos_amt=0.8, post_pos_amt=0.4)  # broker says 0.8, not 1.0
    ex = _make_executor(client)
    pm = _make_pm_in_seeking_tp1()

    result = ex.execute_tp1_partial_close(
        symbol="BTCUSDT",
        live_qty=1.0,          # internal says 1.0
        position_side="LONG",
        sl_price=49_000.0,
        tp_price=53_000.0,
        tp1_fraction=0.5,
        position_manager=pm,
    )

    # After broker reconcile: live_qty = 0.8, tp1_qty = 0.4, runner = 0.4
    assert result["live_qty_before"] == pytest.approx(0.8, rel=0.05)
    assert not result["skipped"]
    # Ensure no order was placed for more than the broker qty
    placed_req = client.place_order.call_args[0][0]
    assert float(placed_req.qty) <= 0.8
