"""
FIX-D: TP1/TP2 Exit Structure — Break-Even Buffer Tests

Root cause fixed: _move_to_break_even and execute_break_even_update previously
set the post-TP1 stop to entry_price + fee_buffer (only ~$80 for BTC at $67k),
which is smaller than typical post-TP1 oscillation.  The fix uses:

    buffer = max(0.20 * |entry - original_sl|, fee_buffer)

This gives the runner room to breathe toward TP2 without removing downside protection.

Test plan
---------
Break-even buffer (8 tests):
  1. LONG after TP1 moves SL to entry_price + 0.2 * original_sl_distance
  2. SHORT after TP1 moves SL to entry_price - 0.2 * original_sl_distance
  3. Stop is NOT moved exactly to entry price
  4. Stop is NOT moved if proposed stop would weaken protection
  5. Missing original SL falls back gracefully + logs MISSING_ORIGINAL_SL
  6. Buffer applied only once (idempotent via is_break_even flag)
  7. TP2 remains active after TP1 / break-even applied
  8. Remaining position survives normal oscillation < buffer

Exit reason (2 tests):
  9. ExitReason.BREAK_EVEN_BUFFER exists in trade_fills.ExitReason
 10. ExitReason.BREAK_EVEN_BUFFER is in ExitReason.ALL

Daily close audit (4 tests):
 11. EXIT_DAILY_CLOSE constant defined and in ALL
 12. SessionMonitor._scan_and_close records eligible positions
 13. SessionMonitor does not close non-profitable positions
 14. Daily close interaction: tp1_hit flag carried on pos state

Regression (4 tests):
 15. Pre-TP1 stop-loss still works correctly
 16. TP1 detection still triggers break-even move
 17. TP2 check still active after transition to RUNNER_TRAILING
 18. execute_break_even_update falls back to fee buffer when no PM provided

Run with:
    python -m pytest tests/test_fix_d_tp1_tp2_break_even.py -v --tb=short
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pm_config(**kwargs):
    from app.execution.position_manager import PositionManagerConfig
    return PositionManagerConfig(**kwargs)


def _open_long(
    entry: float = 50_000.0,
    initial_stop: float = 49_000.0,  # SL_distance = 1000
    tp1: float = 51_000.0,
    tp2: float = 52_200.0,
    config=None,
) -> tuple:
    """Return (PositionManager, symbol)."""
    from app.execution.position_manager import PositionManager, PositionSide
    pm = PositionManager(config=config or _make_pm_config())
    sym = "BTCUSDT"
    pm.open_position(
        symbol=sym,
        side=PositionSide.LONG,
        position_id="pos-001",
        entry_price=entry,
        qty=1.0,
        stop_price=initial_stop,
        tp1_price=tp1,
        tp2_price=tp2,
    )
    return pm, sym


def _open_short(
    entry: float = 50_000.0,
    initial_stop: float = 51_000.0,  # SL_distance = 1000
    tp1: float = 49_000.0,
    tp2: float = 47_800.0,
    config=None,
) -> tuple:
    from app.execution.position_manager import PositionManager, PositionSide
    pm = PositionManager(config=config or _make_pm_config())
    sym = "ETHUSDT"
    pm.open_position(
        symbol=sym,
        side=PositionSide.LONG,  # open as LONG then override side for direct test
        position_id="pos-002",
        entry_price=entry,
        qty=1.0,
        stop_price=initial_stop,
        tp1_price=tp1,
        tp2_price=tp2,
    )
    # Override side to SHORT for direct internal testing
    pos = pm.get_position(sym)
    from app.execution.position_manager import PositionSide as PS
    pos.side = PS.SHORT
    return pm, sym


# ---------------------------------------------------------------------------
# 1. LONG: buffer = max(20% of SL_distance, fee_buffer)
# ---------------------------------------------------------------------------

def test_long_be_buffer_uses_sl_distance():
    """
    LONG trade: after _move_to_break_even, stop is at entry + max(0.20*SL_dist, fee).
    With entry=$50k and SL=$49k (dist=$1000): buffer = max(0.20*1000, ~$60) = $200.
    Stop should be $50,200 — not $50,060 (fee-only).
    """
    config = _make_pm_config(taker_fee_rate=0.0005, be_fee_buffer_mult=1.2,
                              be_sl_distance_buffer_pct=0.20)
    pm, sym = _open_long(entry=50_000.0, initial_stop=49_000.0, config=config)
    pos = pm.get_position(sym)

    pm._move_to_break_even(pos)

    sl_distance = 1_000.0  # entry - initial_stop
    fee_buffer = 50_000.0 * 0.0005 * 2 * 1.2
    expected_buffer = max(sl_distance * 0.20, fee_buffer)
    expected_stop = 50_000.0 + expected_buffer

    assert pos.sl.is_break_even is True
    assert abs(pos.sl.current_stop - expected_stop) < 0.01, (
        f"Expected stop={expected_stop:.4f}, got {pos.sl.current_stop:.4f}"
    )
    assert pos.sl.be_buffer_amount is not None
    assert abs(pos.sl.be_buffer_amount - expected_buffer) < 0.01


# ---------------------------------------------------------------------------
# 2. SHORT: buffer = max(20% of SL_distance, fee_buffer)
# ---------------------------------------------------------------------------

def test_short_be_buffer_uses_sl_distance():
    """
    SHORT trade: after _move_to_break_even, stop is at entry - max(0.20*SL_dist, fee).
    entry=$50k, initial_stop=$51k (dist=$1000): expected stop = $49,800.
    """
    from app.execution.position_manager import PositionManager, PositionSide, PositionManagerConfig
    config = PositionManagerConfig(taker_fee_rate=0.0005, be_fee_buffer_mult=1.2,
                                   be_sl_distance_buffer_pct=0.20)
    pm = PositionManager(config=config)
    sym = "ETHUSDT"
    pm.open_position(
        symbol=sym,
        side=PositionSide.SHORT,
        position_id="pos-short",
        entry_price=50_000.0,
        qty=1.0,
        stop_price=51_000.0,
        tp1_price=49_000.0,
        tp2_price=47_800.0,
    )
    pos = pm.get_position(sym)

    pm._move_to_break_even(pos)

    sl_distance = 1_000.0
    fee_buffer = 50_000.0 * 0.0005 * 2 * 1.2
    expected_buffer = max(sl_distance * 0.20, fee_buffer)
    expected_stop = 50_000.0 - expected_buffer

    assert pos.sl.is_break_even is True
    assert abs(pos.sl.current_stop - expected_stop) < 0.01, (
        f"Expected stop={expected_stop:.4f}, got {pos.sl.current_stop:.4f}"
    )


# ---------------------------------------------------------------------------
# 3. Stop is NOT placed exactly at entry price
# ---------------------------------------------------------------------------

def test_be_stop_is_not_exactly_entry_price():
    """The new stop must be strictly above entry for LONG (not at exactly entry)."""
    config = _make_pm_config(taker_fee_rate=0.0005, be_fee_buffer_mult=1.2,
                              be_sl_distance_buffer_pct=0.20)
    pm, sym = _open_long(entry=50_000.0, initial_stop=49_000.0, config=config)
    pos = pm.get_position(sym)

    pm._move_to_break_even(pos)

    assert pos.sl.current_stop > pos.entry_price, (
        f"Stop {pos.sl.current_stop} must be ABOVE entry {pos.entry_price} for LONG. "
        "A stop AT entry can be triggered by spread/fees alone."
    )
    # And it should be meaningfully above entry — more than fee-only
    fee_only = pos.entry_price * 0.0005 * 2 * 1.2
    expected_min = pos.entry_price + max(1_000.0 * 0.20, fee_only) - 0.01
    assert pos.sl.current_stop >= expected_min, (
        f"Stop {pos.sl.current_stop:.4f} should be >= {expected_min:.4f} "
        f"(entry + max(20% SL dist, fee))"
    )


# ---------------------------------------------------------------------------
# 4. Stop is NOT moved if it would weaken protection (never-loosen guard)
# ---------------------------------------------------------------------------

def test_be_not_applied_if_would_weaken_stop():
    """
    If current_stop is already higher than the proposed BE stop (e.g. trailing already
    moved it up), _move_to_break_even must NOT set is_break_even or change the stop.
    """
    config = _make_pm_config(taker_fee_rate=0.0005, be_fee_buffer_mult=1.2,
                              be_sl_distance_buffer_pct=0.20)
    pm, sym = _open_long(entry=50_000.0, initial_stop=49_000.0, config=config)
    pos = pm.get_position(sym)

    # Manually set current_stop to a high value (e.g. trailing already moved it to $50,500)
    pos.sl.current_stop = 50_500.0

    pm._move_to_break_even(pos)

    # proposed BE = 50_000 + max(200, ~60) = 50_200, which is < 50_500
    # so the guard should skip and leave stop at 50_500
    assert pos.sl.current_stop == 50_500.0, (
        f"Stop should remain at 50_500 (not weakened), got {pos.sl.current_stop}"
    )
    assert pos.sl.is_break_even is False, (
        "is_break_even should not be set when the guard rejects the move."
    )


# ---------------------------------------------------------------------------
# 5. Missing original SL — fallback + no crash
# ---------------------------------------------------------------------------

def test_missing_original_sl_falls_back_gracefully():
    """
    If initial_stop is 0 (no original SL stored), the system must NOT crash.
    It should fall back to the fee-only buffer and log BREAK_EVEN_BUFFER_MISSING_ORIGINAL_SL.
    """
    config = _make_pm_config(taker_fee_rate=0.0005, be_fee_buffer_mult=1.2,
                              be_sl_distance_buffer_pct=0.20)
    pm, sym = _open_long(config=config)
    pos = pm.get_position(sym)

    # Force initial_stop to 0 to simulate missing SL
    pos.sl.initial_stop = 0.0
    pos.sl.current_stop = 0.0  # also reset so never-loosen guard won't block it

    import logging
    with patch.object(logging.getLogger("app.execution.position_manager"), "warning") as mock_warn:
        pm._move_to_break_even(pos)

    # Should still apply a stop using fee-only buffer
    assert pos.sl.is_break_even is True, "Should still set is_break_even even with missing SL."
    fee_buffer = pos.entry_price * 0.0005 * 2 * 1.2
    assert pos.sl.current_stop > pos.entry_price, "Stop should still be above entry."
    assert abs(pos.sl.current_stop - (pos.entry_price + fee_buffer)) < 0.01

    # Check that warning was logged (MISSING_ORIGINAL_SL)
    warned = any(
        "BREAK_EVEN_BUFFER_MISSING_ORIGINAL_SL" in str(args)
        for args, _ in mock_warn.call_args_list
    )
    assert warned, "Expected BREAK_EVEN_BUFFER_MISSING_ORIGINAL_SL warning log."


# ---------------------------------------------------------------------------
# 6. Buffer applied only once (idempotent)
# ---------------------------------------------------------------------------

def test_be_buffer_applied_only_once():
    """
    Calling _move_to_break_even twice must not move the stop a second time.
    The never-loosen guard prevents the second call from doing anything harmful.
    """
    config = _make_pm_config(taker_fee_rate=0.0005, be_fee_buffer_mult=1.2,
                              be_sl_distance_buffer_pct=0.20)
    pm, sym = _open_long(entry=50_000.0, initial_stop=49_000.0, config=config)
    pos = pm.get_position(sym)

    pm._move_to_break_even(pos)
    stop_after_first = pos.sl.current_stop

    pm._move_to_break_even(pos)
    stop_after_second = pos.sl.current_stop

    assert stop_after_first == stop_after_second, (
        f"Second call should not change stop. "
        f"First={stop_after_first:.4f} Second={stop_after_second:.4f}"
    )


# ---------------------------------------------------------------------------
# 7. TP2 remains active after TP1 / break-even applied
# ---------------------------------------------------------------------------

def test_tp2_remains_active_after_tp1():
    """
    After _move_to_break_even, the TP2 price must remain unchanged.
    TP2 is the profit target — it must never be cleared by the BE move.
    """
    pm, sym = _open_long(entry=50_000.0, initial_stop=49_000.0,
                          tp1=51_000.0, tp2=52_200.0)
    pos = pm.get_position(sym)
    tp2_before = pos.tp.tp2_price

    pm._move_to_break_even(pos)

    assert pos.tp.tp2_price == tp2_before, (
        f"TP2 must not change after BE move: was {tp2_before}, now {pos.tp.tp2_price}"
    )
    assert pos.tp.tp2_price > 0


# ---------------------------------------------------------------------------
# 8. Position survives oscillation smaller than buffer
# ---------------------------------------------------------------------------

def test_position_not_stopped_by_sub_buffer_oscillation():
    """
    After TP1, a price above the buffered BE stop must NOT trigger HIT_STOP.
    With entry=50_000, initial_stop=49_000 (dist=1000): buffer = max(200, ~60) = 200.
    Buffered stop = 50_200.  A price of 50_300 (above the stop) must survive.

    Separately, verify the buffered stop is strictly LARGER than the old fee-only stop
    — this is the core guarantee that the fix gives extra protection.
    """
    config = _make_pm_config(taker_fee_rate=0.0005, be_fee_buffer_mult=1.2,
                              be_sl_distance_buffer_pct=0.20)
    pm, sym = _open_long(entry=50_000.0, initial_stop=49_000.0,
                          tp1=51_000.0, tp2=52_200.0, config=config)
    pos = pm.get_position(sym)

    pm._move_to_break_even(pos)
    buffered_stop = pos.sl.current_stop   # ~50_200

    # The buffered stop must be above entry AND above the fee-only buffer
    fee_only = 50_000.0 * 0.0005 * 2 * 1.2  # ~$60
    assert buffered_stop > 50_000.0 + fee_only, (
        f"New buffered stop {buffered_stop:.2f} should exceed fee-only stop "
        f"{50_000.0 + fee_only:.2f} — confirms the 20% SL distance buffer is applied."
    )

    # A price ABOVE the buffered stop must not trigger HIT_STOP
    from app.execution.position_manager import PositionPhase
    pos.phase = PositionPhase.RUNNER_TRAILING
    price_above_stop = buffered_stop + 100.0  # comfortably above the buffered stop
    action = pm.update_price(sym, price_above_stop, current_atr=300.0)

    assert action != "HIT_STOP", (
        f"Price {price_above_stop:.2f} (above buffered_stop={buffered_stop:.2f}) "
        "must NOT trigger HIT_STOP."
    )


# ---------------------------------------------------------------------------
# 9. ExitReason.BREAK_EVEN_BUFFER exists and has correct value
# ---------------------------------------------------------------------------

def test_exit_reason_break_even_buffer_defined():
    """ExitReason.BREAK_EVEN_BUFFER must be defined in trade_fills.ExitReason."""
    from shared_lib.persistence.trade_fills import ExitReason
    assert hasattr(ExitReason, "BREAK_EVEN_BUFFER"), (
        "ExitReason.BREAK_EVEN_BUFFER must be defined for FIX-D."
    )
    assert ExitReason.BREAK_EVEN_BUFFER == "BREAK_EVEN_BUFFER"
    assert hasattr(ExitReason, "EXIT_BREAK_EVEN_BUFFER")
    assert ExitReason.EXIT_BREAK_EVEN_BUFFER == "BREAK_EVEN_BUFFER"


# ---------------------------------------------------------------------------
# 10. ExitReason.BREAK_EVEN_BUFFER is in ExitReason.ALL
# ---------------------------------------------------------------------------

def test_exit_reason_break_even_buffer_in_all():
    """BREAK_EVEN_BUFFER must appear in ExitReason.ALL for validation sweeps."""
    from shared_lib.persistence.trade_fills import ExitReason
    assert "BREAK_EVEN_BUFFER" in ExitReason.ALL, (
        "'BREAK_EVEN_BUFFER' must be in ExitReason.ALL."
    )


# ---------------------------------------------------------------------------
# 11. EXIT_DAILY_CLOSE is defined and in ALL
# ---------------------------------------------------------------------------

def test_exit_daily_close_defined_and_in_all():
    """EXIT_DAILY_CLOSE must be defined and in ALL."""
    from shared_lib.persistence.trade_fills import ExitReason
    assert hasattr(ExitReason, "EXIT_DAILY_CLOSE")
    assert ExitReason.EXIT_DAILY_CLOSE == "DAILY_CLOSE"
    assert "DAILY_CLOSE" in ExitReason.ALL


# ---------------------------------------------------------------------------
# 12. SessionMonitor closes profitable positions
# ---------------------------------------------------------------------------

def test_session_monitor_closes_eligible_position():
    """
    SessionMonitor._scan_and_close must close a profitable position
    and call _force_market_close exactly once.
    """
    from app.runner.session_monitor import SessionMonitor

    mock_executor = MagicMock()
    mock_executor.client.get_position.return_value = {
        "positionAmt": "1.0",
        "unRealizedProfit": "50.0",
        "initialMargin": "500.0",
    }
    mock_executor.cancel_open_orders = MagicMock()
    mock_executor.client.order_market = MagicMock(return_value={})

    mock_audit = MagicMock()
    monitor = SessionMonitor(executor=mock_executor, audit=mock_audit, bot_instance_id="test-bot")

    from app.runner.models import SymbolState
    state = {
        "BTCUSDT": SymbolState(
            position="LONG",
            entry_price=50_000.0,
            entry_qty=1.0,
        )
    }
    now_local = datetime.now(timezone.utc)
    result = monitor._scan_and_close(state, now_local)

    assert result is True
    mock_executor.client.order_market.assert_called_once_with(
        symbol="BTCUSDT",
        side="SELL",
        quantity=1.0,
        reduce_only=True,
    )


# ---------------------------------------------------------------------------
# 13. SessionMonitor does not close non-profitable position
# ---------------------------------------------------------------------------

def test_session_monitor_skips_losing_position():
    """SessionMonitor must NOT close a losing position (upnl < 0)."""
    from app.runner.session_monitor import SessionMonitor

    mock_executor = MagicMock()
    mock_executor.client.get_position.return_value = {
        "positionAmt": "1.0",
        "unRealizedProfit": "-25.0",
        "initialMargin": "500.0",
    }
    mock_audit = MagicMock()
    monitor = SessionMonitor(executor=mock_executor, audit=mock_audit)

    from app.runner.models import SymbolState
    state = {
        "BTCUSDT": SymbolState(
            position="LONG",
            entry_price=50_000.0,
            entry_qty=1.0,
        )
    }
    now_local = datetime.now(timezone.utc)
    result = monitor._scan_and_close(state, now_local)

    assert result is False
    mock_executor.client.order_market.assert_not_called()


# ---------------------------------------------------------------------------
# 14. TP1 hit flag is carried on position state through daily close window
# ---------------------------------------------------------------------------

def test_tp1_hit_flag_carried_on_position_state():
    """
    After TP1 is hit, pos.tp.tp1_hit must be True — giving daily close audit
    code the ability to identify trades that were TP1-active before closing.
    """
    pm, sym = _open_long(entry=50_000.0, initial_stop=49_000.0,
                          tp1=51_000.0, tp2=52_200.0)
    pos = pm.get_position(sym)
    assert pos.tp.tp1_hit is False, "tp1_hit should be False before TP1"

    # Simulate TP1 hit via update_price
    action = pm.update_price(sym, 51_000.0, current_atr=300.0)

    assert action == "HIT_TP1"
    pos = pm.get_position(sym)
    assert pos.tp.tp1_hit is True, "tp1_hit must be True after TP1 for daily close audit"


# ---------------------------------------------------------------------------
# 15. Pre-TP1 stop-loss still works
# ---------------------------------------------------------------------------

def test_pre_tp1_stop_loss_works():
    """
    Before TP1, a stop-loss hit must still return HIT_STOP.
    The fix must not affect the pre-TP1 stop mechanism.
    """
    pm, sym = _open_long(entry=50_000.0, initial_stop=49_000.0,
                          tp1=51_000.0, tp2=52_200.0)
    action = pm.update_price(sym, 48_900.0, current_atr=300.0)
    assert action == "HIT_STOP", f"Expected HIT_STOP at price below initial_stop, got {action}"


# ---------------------------------------------------------------------------
# 16. TP1 detection still triggers break-even move
# ---------------------------------------------------------------------------

def test_tp1_detection_triggers_break_even():
    """
    When price hits TP1, update_price must return HIT_TP1, and the
    break-even stop must be applied immediately.
    """
    config = _make_pm_config(taker_fee_rate=0.0005, be_fee_buffer_mult=1.2,
                              be_sl_distance_buffer_pct=0.20)
    pm, sym = _open_long(entry=50_000.0, initial_stop=49_000.0,
                          tp1=51_000.0, tp2=52_200.0, config=config)

    action = pm.update_price(sym, 51_000.0, current_atr=300.0)

    assert action == "HIT_TP1"
    pos = pm.get_position(sym)
    assert pos.sl.is_break_even is True, "is_break_even must be True after TP1"
    # New stop must be above entry
    assert pos.sl.current_stop > 50_000.0, (
        f"Stop {pos.sl.current_stop} should be above entry 50_000 after TP1"
    )
    # And it must be the buffered version (≈ 50_200)
    expected_buffer = max(1_000.0 * 0.20, 50_000.0 * 0.0005 * 2 * 1.2)
    expected_stop = 50_000.0 + expected_buffer
    assert abs(pos.sl.current_stop - expected_stop) < 0.01


# ---------------------------------------------------------------------------
# 17. TP2 check still active after RUNNER_TRAILING transition
# ---------------------------------------------------------------------------

def test_tp2_check_active_after_runner_trailing():
    """
    After TP1, when phase transitions to RUNNER_TRAILING, hitting TP2 must
    eventually return HIT_TP2.  The fix must not remove TP2 detection.

    Implementation note: _update_trailing_stop is evaluated BEFORE _check_tp2_hit
    in the same update_price cycle.  When price first reaches TP2 the trailing
    anchor moves (TRAIL_UPDATED).  On the immediately following tick at the same
    TP2 price the anchor no longer moves (no new high) so _check_tp2_hit fires.
    We verify this two-step behaviour explicitly.
    """
    from app.execution.position_manager import PositionPhase
    config = _make_pm_config(taker_fee_rate=0.0005, be_fee_buffer_mult=1.2,
                              be_sl_distance_buffer_pct=0.20)
    pm, sym = _open_long(entry=50_000.0, initial_stop=49_000.0,
                          tp1=51_000.0, tp2=52_200.0, config=config)

    # Hit TP1
    pm.update_price(sym, 51_000.0, current_atr=300.0)

    # Advance to RUNNER_TRAILING (next tick at a price below TP2)
    pm.update_price(sym, 51_100.0, current_atr=300.0)
    pos = pm.get_position(sym)
    assert pos.phase == PositionPhase.RUNNER_TRAILING, (
        f"Expected RUNNER_TRAILING, got {pos.phase}"
    )

    # First tick at TP2: trailing anchor moves → TRAIL_UPDATED (expected, not HIT_TP2 yet)
    action_first = pm.update_price(sym, 52_200.0, current_atr=300.0)
    # Acceptable outcomes: TRAIL_UPDATED (anchor moved) or already HIT_TP2
    assert action_first in ("TRAIL_UPDATED", "HIT_TP2"), (
        f"First tick at TP2 price: expected TRAIL_UPDATED or HIT_TP2, got {action_first!r}"
    )

    if action_first != "HIT_TP2":
        # Second tick at the same TP2 price: anchor already at 52_200, no new high.
        # _update_trailing_stop returns False -> falls through to _check_tp2_hit -> HIT_TP2.
        action_second = pm.update_price(sym, 52_200.0, current_atr=300.0)
        assert action_second == "HIT_TP2", (
            f"Second tick at TP2 price: expected HIT_TP2, got {action_second!r}. "
            "TP2 detection must be reachable after RUNNER_TRAILING."
        )


# ---------------------------------------------------------------------------
# 18. execute_break_even_update falls back to fee-only buffer if no PM provided
# ---------------------------------------------------------------------------

def test_executor_be_update_fallback_no_pm():
    """
    When execute_break_even_update is called without a position_manager,
    it must still apply the fee-only buffer (graceful degradation).
    No crash, break_even_applied=True.
    """
    from app.execution.executor import BinanceExecutor

    mock_client = MagicMock()
    mock_client.get_position_amt.return_value = 0.5
    mock_client.get_symbol_filters.return_value = MagicMock(tick_size=0.1)
    mock_client.update_protection.return_value = {
        "status": "OK", "sl_order_id": "SL_NEW", "tp_order_id": "TP_NEW"
    }

    ex = BinanceExecutor.__new__(BinanceExecutor)
    ex.client = mock_client
    ex.circuit = MagicMock(is_tripped=lambda: False)
    ex.audit = None
    ex.run_id = None
    ex.execution_mode = "live"
    ex._live_symbols_override = {"BTCUSDT"}

    result = ex.execute_break_even_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=49_000.0,
        position_manager=None,  # No PM: simulates direct call
        taker_fee_rate=0.0005,
        fee_buffer_mult=1.2,
        be_sl_distance_buffer_pct=0.20,
    )

    # With no PM (initial_stop unknown → 0), sl_distance=0 → fee-only fallback
    assert result["break_even_applied"] is True, (
        f"Expected break_even_applied=True; got skip_reason={result.get('skip_reason')}"
    )
    assert result["sl_distance"] == 0.0, "sl_distance must be 0 when no PM provided"
    assert result["buffer_amount"] is not None
    fee_expected = 50_000.0 * 0.0005 * 2.0 * 1.2
    assert abs(result["buffer_amount"] - fee_expected) < 0.01, (
        f"Expected fee-only buffer {fee_expected:.4f}, got {result['buffer_amount']:.4f}"
    )
