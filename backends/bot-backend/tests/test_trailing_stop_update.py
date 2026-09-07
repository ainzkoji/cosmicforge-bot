"""
Phase 3 Step 3E — Trailing Stop Update Validation Tests.

Covers all 12 spec scenarios (Section 11):
  1.  Long runner trailing update succeeds
  2.  Short runner trailing update succeeds
  3.  Price normalization correctness (LONG ceil / SHORT floor)
  4.  Never-loosen rule — stop not tighter → skip
  5.  Break-even floor enforcement (LONG: trail never below BE)
  6.  Anti-spam: tiny move below min_delta_pct → skip
  7.  Anti-spam: update within throttle window → skip
  8.  Material move triggers a real update
  9.  Protection mutation failure → rollback, phase stays RUNNER_TRAILING
 10.  Restart recovery during TRAILING_UPDATE_PENDING → heals to RUNNER_TRAILING
 11.  ATR missing/invalid → skip safely
 12.  Broker/internal quantity divergence → broker wins

Run with:
    python -m pytest tests/test_trailing_stop_update.py -v
"""
from __future__ import annotations

from unittest.mock import MagicMock
from datetime import datetime, timezone, timedelta

import pytest


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_client(
    broker_qty: float = 0.5,
    update_protection_rv=None,
    update_protection_raises=None,
    tick_size: float = 0.1,
):
    client = MagicMock()
    client.get_position_amt.return_value = broker_qty
    f = MagicMock()
    f.tick_size = tick_size
    client.get_symbol_filters.return_value = f
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


def _make_pm(
    symbol="BTCUSDT",
    side="LONG",
    entry_price: float = 50_000.0,
    current_stop: float = 50_060.0,   # post-BE stop
    runner_qty: float = 0.5,
    be_confirmed: bool = True,
    tp1_hit: bool = True,
    highest_since_entry: float = 51_000.0,
    lowest_since_entry: float = 49_000.0,
    be_price: float = 50_060.0,
    trail_last_update_ts=None,
    phase_override=None,
):
    from app.execution.position_manager import (
        PositionManager, PositionPhase, PositionSide,
    )
    pm = PositionManager()
    ps = PositionSide.LONG if side == "LONG" else PositionSide.SHORT
    tp1_price = entry_price * 1.02 if ps == PositionSide.LONG else entry_price * 0.98
    tp2_price = entry_price * 1.05 if ps == PositionSide.LONG else entry_price * 0.95
    pm.open_position(symbol, ps, entry_price, runner_qty * 2, current_stop, tp1_price, tp2_price)
    pos = pm.get_position(symbol)
    pos.current_qty = runner_qty
    pos.tp.tp1_hit = tp1_hit
    pos.sl.be_exchange_confirmed = be_confirmed
    pos.sl.current_stop = current_stop
    pos.sl.break_even_price = be_price
    pos.sl.sl_order_id = "SL_ORIG"
    pos.sl.tp_order_id = "TP_ORIG"
    pos.sl.trailing_last_update_ts = trail_last_update_ts
    pos.highest_since_entry = highest_since_entry
    pos.lowest_since_entry = lowest_since_entry
    pos.phase = phase_override or PositionPhase.RUNNER_TRAILING
    return pm


# ──────────────────────────────────────────────────────────────────────────────
# 1. Long runner trailing update succeeds
# ──────────────────────────────────────────────────────────────────────────────

def test_trail_long_success():
    """
    LONG with favorable anchor: trailing update applied.
    norm_trail > current_stop, update_protection called once.
    """
    from app.execution.position_manager import PositionPhase

    # entry=50k, highest=51k, atr=200, trail_mult=1.2
    # raw = 51000 - (1.2*200) = 50760, well above current_stop=50060
    entry = 50_000.0
    highest = 51_000.0
    atr = 200.0
    current_stop = 50_060.0

    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm(entry_price=entry, current_stop=current_stop, highest_since_entry=highest)

    result = ex.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=entry,
        current_stop=current_stop,
        highest_since_entry=highest,
        lowest_since_entry=entry - 500,
        atr=atr,
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=52_500.0,
        position_manager=pm,
        trail_atr_mult=1.2,
        min_delta_pct=0.0001,   # very small for test
        min_update_interval_s=0,  # no throttle for test
    )

    assert result["trailing_applied"] is True, f"Expected trailing applied: {result}"
    assert result["lifecycle_state_after"] == "RUNNER_TRAILING"
    assert result["protection_update_status"] == "OK"

    pos = pm.get_position("BTCUSDT")
    assert pos.sl.current_stop > current_stop, "Stop must have tightened"
    assert pos.sl.trailing_last_stop_price is not None
    assert pos.sl.trailing_last_update_ts is not None
    assert pos.sl.sl_order_id == "SL_NEW"
    assert pos.phase == PositionPhase.RUNNER_TRAILING
    client.update_protection.assert_called_once()
    req = client.update_protection.call_args[0][0]
    assert req.reason == "TRAILING"


# ──────────────────────────────────────────────────────────────────────────────
# 2. Short runner trailing update succeeds
# ──────────────────────────────────────────────────────────────────────────────

def test_trail_short_success():
    """
    SHORT with favorable anchor: trailing update applied.
    norm_trail < current_stop (stop moves downward toward entry).
    """
    entry = 50_000.0
    lowest = 49_000.0    # Short: price dropped — favorable direction
    be_stop = 49_940.0   # Just below entry (SHORT BE is below entry)
    current_stop = be_stop
    atr = 200.0

    # raw = 49000 + (1.2*200) = 49240, which is < current_stop (49940) ✓ tighter
    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm(
        side="SHORT",
        entry_price=entry,
        current_stop=current_stop,
        lowest_since_entry=lowest,
        be_confirmed=True,
        be_price=be_stop,
    )

    result = ex.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="SHORT",
        runner_qty=0.5,
        entry_price=entry,
        current_stop=current_stop,
        highest_since_entry=entry + 1000,
        lowest_since_entry=lowest,
        atr=atr,
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=47_500.0,
        position_manager=pm,
        trail_atr_mult=1.2,
        min_delta_pct=0.0001,
        min_update_interval_s=0,
        be_floor_price=be_stop,
    )

    assert result["trailing_applied"] is True, f"SHORT trail should apply: {result}"
    pos = pm.get_position("BTCUSDT")
    assert pos.sl.current_stop < current_stop, "SHORT stop must have moved downward"


# ──────────────────────────────────────────────────────────────────────────────
# 3. Price normalization correctness
# ──────────────────────────────────────────────────────────────────────────────

def test_normalize_trailing_long_rounds_up():
    """LONG normalization must always round up to tick boundary."""
    from app.execution.executor import BinanceExecutor
    ex = BinanceExecutor.__new__(BinanceExecutor)
    client = MagicMock()
    f = MagicMock()
    f.tick_size = 0.5
    client.get_symbol_filters.return_value = f
    ex.client = client

    raw = 50_761.13
    norm, status = ex._normalize_trailing_stop_price("BTCUSDT", raw, "LONG")
    assert status == "OK"
    assert norm >= raw, f"LONG must round UP: {norm} < {raw}"
    from decimal import Decimal
    assert Decimal(str(norm)) % Decimal("0.5") == 0, f"{norm} not aligned to 0.5 tick"


def test_normalize_trailing_short_rounds_down():
    """SHORT normalization must always round down to tick boundary."""
    from app.execution.executor import BinanceExecutor
    ex = BinanceExecutor.__new__(BinanceExecutor)
    client = MagicMock()
    f = MagicMock()
    f.tick_size = 0.5
    client.get_symbol_filters.return_value = f
    ex.client = client

    raw = 49_238.87
    norm, status = ex._normalize_trailing_stop_price("BTCUSDT", raw, "SHORT")
    assert status == "OK"
    assert norm <= raw, f"SHORT must round DOWN: {norm} > {raw}"
    from decimal import Decimal
    assert Decimal(str(norm)) % Decimal("0.5") == 0


# ──────────────────────────────────────────────────────────────────────────────
# 4. Never-loosen rule
# ──────────────────────────────────────────────────────────────────────────────

def test_trail_skipped_when_would_loosen_long():
    """
    LONG: if normalized trail <= current_stop → TRAILING_WOULD_LOOSEN, no call.
    """
    # highest=50500, atr=1000, trail_mult=1.2 → raw=50500-1200=49300 < current_stop=50260
    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm(current_stop=50_260.0, highest_since_entry=50_500.0)

    result = ex.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=50_260.0,
        highest_since_entry=50_500.0,
        lowest_since_entry=49_000.0,
        atr=1000.0,   # large ATR → trail too wide → would loosen
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=53_000.0,
        position_manager=pm,
        trail_atr_mult=1.2,
        min_delta_pct=0.0001,
        min_update_interval_s=0,
    )

    assert result["trailing_applied"] is False
    assert result["skip_reason"] == "TRAILING_WOULD_LOOSEN"
    client.update_protection.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# 5. Break-even floor enforcement (LONG)
# ──────────────────────────────────────────────────────────────────────────────

def test_trail_be_floor_prevents_stop_below_be_long():
    """
    For LONG: trailing stop must never be placed below break-even price.
    If raw trail < BE floor, trail is clamped to BE floor (then never-loosen guard
    catches it if BE floor == current_stop).
    """
    entry = 50_000.0
    be_floor = 50_060.0
    current_stop = be_floor
    # raw trail would be 50050 (below be_floor) → clamp to 50060 → same as current_stop → skip
    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm(entry_price=entry, current_stop=current_stop, be_price=be_floor,
                  highest_since_entry=50_200.0)

    result = ex.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=entry,
        current_stop=current_stop,
        highest_since_entry=50_200.0,
        lowest_since_entry=49_000.0,
        atr=150.0,     # raw = 50200 - 180 = 50020 < be_floor → clamped to 50060
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=52_500.0,
        position_manager=pm,
        trail_atr_mult=1.2,
        min_delta_pct=0.0001,
        min_update_interval_s=0,
        be_floor_price=be_floor,
    )

    # buffered trail should equal be_floor → never-loosen triggers since be_floor == current_stop
    assert result["trailing_applied"] is False
    assert result["buffered_trailing_stop"] == pytest.approx(be_floor, rel=0.001)
    assert result["skip_reason"] in ("TRAILING_WOULD_LOOSEN",)
    client.update_protection.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# 6. Anti-spam: tiny delta below threshold
# ──────────────────────────────────────────────────────────────────────────────

def test_trail_skipped_delta_below_threshold():
    """
    Improvement is 0.0001 (0.01%) but min_delta_pct=0.005 → skip DELTA_BELOW_THRESHOLD.
    """
    current_stop = 50_200.0
    # raw trail = 51000 - (1.2*100) = 50880 — but we make it only barely above current_stop
    # by using current_stop + 0.05 as the target
    entry = 50_000.0
    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm(current_stop=current_stop, highest_since_entry=50_210.0)

    result = ex.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=entry,
        current_stop=current_stop,
        highest_since_entry=50_210.0,
        lowest_since_entry=49_000.0,
        atr=1.0,   # tiny ATR → raw trail = 50210 - 1.2 = 50208.8
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=52_500.0,
        position_manager=pm,
        trail_atr_mult=1.2,
        min_delta_pct=0.005,  # 0.5% minimum — will be exceeded by 50208.8 vs 50200, which is 0.017%
        min_update_interval_s=0,
    )

    # delta = (50208.8 - 50200) / 50200 ≈ 0.000175 < 0.005 → skip
    assert result["trailing_applied"] is False
    assert result["skip_reason"] is not None
    assert "DELTA_BELOW_THRESHOLD" in result["skip_reason"] or result["skip_reason"] == "TRAILING_WOULD_LOOSEN"
    client.update_protection.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# 7. Anti-spam: update within throttle window
# ──────────────────────────────────────────────────────────────────────────────

def test_trail_throttled_within_min_interval():
    """
    last_update_ts was 10 seconds ago, min_update_interval_s=60 → THROTTLE_TOO_SOON.
    """
    now = datetime.now(timezone.utc)
    last_ts = (now - timedelta(seconds=10)).isoformat()

    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm(current_stop=50_060.0, highest_since_entry=51_000.0,
                  trail_last_update_ts=last_ts)

    result = ex.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=50_060.0,
        highest_since_entry=51_000.0,
        lowest_since_entry=49_000.0,
        atr=200.0,
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=52_500.0,
        position_manager=pm,
        min_delta_pct=0.0001,
        min_update_interval_s=60,
        last_update_ts=last_ts,
    )

    assert result["trailing_applied"] is False
    assert result["skip_reason"] is not None
    assert "THROTTLE_TOO_SOON" in result["skip_reason"]
    client.update_protection.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# 8. Material move triggers a real update
# ──────────────────────────────────────────────────────────────────────────────

def test_trail_material_move_triggers_update():
    """
    A large favorable move (entry=50k, highest=52k, atr=200) creates a material
    improvement → update_protection is called and trailing_applied=True.
    """
    now = datetime.now(timezone.utc)
    last_ts = (now - timedelta(seconds=120)).isoformat()  # 2 min ago — past throttle

    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm(current_stop=50_060.0, highest_since_entry=52_000.0,
                  trail_last_update_ts=last_ts)

    result = ex.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=50_060.0,
        highest_since_entry=52_000.0,
        lowest_since_entry=49_000.0,
        atr=200.0,      # raw = 52000 - 240 = 51760, well above current_stop=50060
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=52_500.0,
        position_manager=pm,
        trail_atr_mult=1.2,
        min_delta_pct=0.001,
        min_update_interval_s=60,
        last_update_ts=last_ts,
    )

    assert result["trailing_applied"] is True
    assert result["normalized_trailing_stop"] > 50_060.0
    client.update_protection.assert_called_once()


# ──────────────────────────────────────────────────────────────────────────────
# 9. Protection mutation failure → rollback
# ──────────────────────────────────────────────────────────────────────────────

def test_trail_failure_rolls_back_to_runner_trailing():
    """
    update_protection() raises → phase stays RUNNER_TRAILING, current_stop unchanged.
    """
    from app.execution.position_manager import PositionPhase

    client = _make_client(
        broker_qty=0.5,
        update_protection_raises=RuntimeError("exchange_timeout"),
    )
    ex = _make_executor(client)
    pm = _make_pm(current_stop=50_060.0, highest_since_entry=51_000.0)

    result = ex.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=50_060.0,
        highest_since_entry=51_000.0,
        lowest_since_entry=49_000.0,
        atr=200.0,
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=52_500.0,
        position_manager=pm,
        min_delta_pct=0.0001,
        min_update_interval_s=0,
    )

    assert result["trailing_applied"] is False
    assert "UPDATE_PROTECTION_FAILED" in (result.get("failure_reason") or "")

    pos = pm.get_position("BTCUSDT")
    assert pos.phase == PositionPhase.RUNNER_TRAILING, "Phase must revert on failure"
    assert pos.sl.current_stop == pytest.approx(50_060.0), "Stop must not move on failure"
    assert pos.sl.sl_order_id == "SL_ORIG", "Order IDs must not change on failure"


# ──────────────────────────────────────────────────────────────────────────────
# 10. Restart recovery during TRAILING_UPDATE_PENDING
# ──────────────────────────────────────────────────────────────────────────────

def test_reconcile_trailing_update_pending_resets_to_runner_trailing():
    """
    _reconcile_trailing_update_pending() always resets TRAILING_UPDATE_PENDING
    to RUNNER_TRAILING on restart without re-sending any exchange call.
    """
    from app.execution.position_manager import (
        PositionManager, PositionPhase, PositionSide,
    )

    pm = PositionManager()
    pm.open_position("BTCUSDT", PositionSide.LONG, 50_000.0, 1.0,
                     50_060.0, 51_000.0, 53_000.0)
    pos = pm.get_position("BTCUSDT")
    pos.phase = PositionPhase.TRAILING_UPDATE_PENDING
    pos.current_qty = 0.5
    pos.sl.be_exchange_confirmed = True

    client = MagicMock()
    from app.execution.executor import BinanceExecutor
    ex = BinanceExecutor.__new__(BinanceExecutor)
    ex.client = client

    from app.runner.runner import PaperRunner
    runner = PaperRunner.__new__(PaperRunner)
    runner.position_manager = pm
    runner.executor = ex

    runner._reconcile_trailing_update_pending("BTCUSDT", pos)

    assert pos.phase == PositionPhase.RUNNER_TRAILING
    client.update_protection.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# 11. ATR missing/invalid → skip safely
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad_atr", [None, 0.0, float("nan"), -5.0])
def test_trail_skipped_atr_invalid(bad_atr):
    """ATR of None, 0, nan, or negative must produce ATR_INVALID skip."""
    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm()

    result = ex.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=50_060.0,
        highest_since_entry=51_000.0,
        lowest_since_entry=49_000.0,
        atr=bad_atr,
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=52_500.0,
        position_manager=pm,
        min_delta_pct=0.0001,
        min_update_interval_s=0,
    )

    assert result["trailing_applied"] is False
    assert result["skip_reason"] == "ATR_INVALID"
    client.update_protection.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# 12. Broker/internal quantity divergence → broker wins
# ──────────────────────────────────────────────────────────────────────────────

def test_trail_broker_qty_wins():
    """
    Internal runner_qty=0.5 but broker reports 0.3 → update_protection
    is called with qty=0.3 (broker truth).
    """
    client = _make_client(broker_qty=0.3)
    ex = _make_executor(client)
    pm = _make_pm(runner_qty=0.5, current_stop=50_060.0, highest_since_entry=51_000.0)

    result = ex.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=50_060.0,
        highest_since_entry=51_000.0,
        lowest_since_entry=49_000.0,
        atr=200.0,
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=52_500.0,
        position_manager=pm,
        min_delta_pct=0.0001,
        min_update_interval_s=0,
    )

    assert result["trailing_applied"] is True
    assert result["live_qty"] == pytest.approx(0.3)
    req = client.update_protection.call_args[0][0]
    assert float(req.qty) == pytest.approx(0.3)


# ──────────────────────────────────────────────────────────────────────────────
# Refinement 1 — trailing_last_stop_price used as never-loosen / delta baseline
# ──────────────────────────────────────────────────────────────────────────────

def test_refinement3_confirmed_stop_used_as_baseline():
    """
    Refinement 3: When trailing_last_stop_price is set, it is used as the
    comparison baseline (not current_stop).

    Setup:
      current_stop = 50_060 (PM value, stale)
      trailing_last_stop_price = 50_500 (last confirmed stop at exchange)
      norm_trail = 50_760

    Without refinement 3: delta = (50760-50060)/50060 = 1.4% → APPLY
    With refinement 3:    delta = (50760-50500)/50500 = 0.51% → still > 0.1% → APPLY
    But if we set norm_trail slightly above confirmed_stop (0.05% above 50500 = 50525):
      delta = 25/50500 = 0.049% < 0.1% → DELTA_BELOW_THRESHOLD
    """
    # With trailing_last_stop_price=50500 and tiny improvement:
    # raw = highest(50526) - 1.2*atr(1) ≈ 50524.8 → norm ≈ 50525
    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm(current_stop=50_060.0, highest_since_entry=50_526.0)
    pos = pm.get_position("BTCUSDT")
    pos.sl.trailing_last_stop_price = 50_500.0  # last confirmed at exchange

    result = ex.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=50_060.0,
        highest_since_entry=50_526.0,
        lowest_since_entry=49_000.0,
        atr=1.0,
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=52_500.0,
        position_manager=pm,
        trail_atr_mult=1.2,
        min_delta_pct=0.001,   # 0.1%
        min_update_interval_s=0,
    )

    # Baseline is 50500 (confirmed), norm_trail ≈ 50525 → delta ≈ 0.049% < 0.1% →
    # DELTA_BELOW_THRESHOLD (not APPLY, even though it's > current_stop=50060).
    assert result["trailing_applied"] is False
    assert result["skip_reason"] is not None and (
        "DELTA_BELOW_THRESHOLD" in result["skip_reason"]
        or result["skip_reason"] == "TRAILING_WOULD_LOOSEN"
    )
    client.update_protection.assert_not_called()


def test_refinement3_never_loosen_uses_confirmed_stop():
    """
    Refinement 3: If trailing_last_stop_price > current_stop (e.g. desync),
    never-loosen check uses trailing_last_stop_price — trail that would pass
    (trail > current_stop) still skips if trail <= trailing_last_stop_price.
    """
    # trailing_last_stop_price = 50800 (higher than current_stop=50060 — desync)
    # norm_trail = 50760 → 50760 <= 50800 → TRAILING_WOULD_LOOSEN
    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm(current_stop=50_060.0, highest_since_entry=51_000.0)
    pos = pm.get_position("BTCUSDT")
    pos.sl.trailing_last_stop_price = 50_800.0  # confirmed stop is already higher

    result = ex.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=50_060.0,
        highest_since_entry=51_000.0,
        lowest_since_entry=49_000.0,
        atr=200.0,   # raw = 51000 - 240 = 50760 < confirmed_stop(50800) → loosen
        sl_order_id="SL_ORIG",
        tp_order_id="TP_ORIG",
        tp2_price=52_500.0,
        position_manager=pm,
        trail_atr_mult=1.2,
        min_delta_pct=0.0001,
        min_update_interval_s=0,
    )

    assert result["trailing_applied"] is False
    assert result["skip_reason"] == "TRAILING_WOULD_LOOSEN"
    client.update_protection.assert_not_called()


def test_refinement4_tp_order_id_always_passed():
    """
    Refinement 4: Even when tp2_price is None, old_tp_order_id is always
    passed to ProtectionUpdateRequest so the adapter can preserve TP2.
    """
    client = _make_client(broker_qty=0.5)
    ex = _make_executor(client)
    pm = _make_pm(current_stop=50_060.0, highest_since_entry=51_000.0)
    pos = pm.get_position("BTCUSDT")
    pos.sl.tp_order_id = "TP_EXISTING"

    result = ex.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=0.5,
        entry_price=50_000.0,
        current_stop=50_060.0,
        highest_since_entry=51_000.0,
        lowest_since_entry=49_000.0,
        atr=200.0,
        sl_order_id="SL_ORIG",
        tp_order_id=None,   # not passed by caller
        tp2_price=None,     # tp2 price also unknown
        position_manager=pm,
        min_delta_pct=0.0001,
        min_update_interval_s=0,
    )

    assert result["trailing_applied"] is True
    req = client.update_protection.call_args[0][0]
    # Must carry existing TP order ID so adapter can preserve TP2
    assert req.old_tp_order_id == "TP_EXISTING"
    # new_tp_price may be None — adapter preserves existing TP2 in that case
    assert req.new_tp_price is None


def test_reconcile_trailing_update_pending_confirms_when_broker_matches():
    """
    Refinement 2: If broker stop matches trailing_last_stop_price,
    reconcile confirms the update and advances to RUNNER_TRAILING.
    """
    from app.execution.position_manager import (
        PositionManager, PositionPhase, PositionSide,
    )

    pm = PositionManager()
    pm.open_position("BTCUSDT", PositionSide.LONG, 50_000.0, 1.0,
                     50_060.0, 51_000.0, 53_000.0)
    pos = pm.get_position("BTCUSDT")
    pos.phase = PositionPhase.TRAILING_UPDATE_PENDING
    pos.sl.trailing_last_stop_price = 50_760.0  # what we tried to set

    client = MagicMock()
    # Broker confirms stop = 50760 → update landed
    client.get_position_stop.return_value = 50_760.0

    from app.execution.executor import BinanceExecutor
    ex = BinanceExecutor.__new__(BinanceExecutor)
    ex.client = client

    from app.runner.runner import PaperRunner
    runner = PaperRunner.__new__(PaperRunner)
    runner.position_manager = pm
    runner.executor = ex

    runner._reconcile_trailing_update_pending("BTCUSDT", pos)

    assert pos.phase == PositionPhase.RUNNER_TRAILING
    # current_stop updated to confirmed value
    assert pos.sl.current_stop == pytest.approx(50_760.0)


def test_reconcile_trailing_update_pending_reverts_when_broker_differs():
    """
    Refinement 2: If broker stop does NOT match trailing_last_stop_price,
    reconcile reverts to RUNNER_TRAILING so next heartbeat can retry.
    """
    from app.execution.position_manager import (
        PositionManager, PositionPhase, PositionSide,
    )

    pm = PositionManager()
    pm.open_position("BTCUSDT", PositionSide.LONG, 50_000.0, 1.0,
                     50_060.0, 51_000.0, 53_000.0)
    pos = pm.get_position("BTCUSDT")
    pos.phase = PositionPhase.TRAILING_UPDATE_PENDING
    pos.sl.current_stop = 50_060.0          # PM value before update
    pos.sl.trailing_last_stop_price = 50_760.0  # what we intended to set

    client = MagicMock()
    # Broker still shows old stop — update did NOT land
    client.get_position_stop.return_value = 50_060.0

    from app.execution.executor import BinanceExecutor
    ex = BinanceExecutor.__new__(BinanceExecutor)
    ex.client = client

    from app.runner.runner import PaperRunner
    runner = PaperRunner.__new__(PaperRunner)
    runner.position_manager = pm
    runner.executor = ex

    runner._reconcile_trailing_update_pending("BTCUSDT", pos)

    assert pos.phase == PositionPhase.RUNNER_TRAILING
    # current_stop stays at old value since update didn't land
    assert pos.sl.current_stop == pytest.approx(50_060.0)
    client.update_protection.assert_not_called()

