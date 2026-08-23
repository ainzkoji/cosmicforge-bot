from __future__ import annotations

from app.execution.executor import BinanceExecutor
from app.execution.position_manager import PositionManager, PositionPhase, PositionSide


class LifecyclePaperClient:
    def __init__(self, price: float = 100.0) -> None:
        self.price = price
        self.live_calls: list[str] = []

    def get_prices(self, symbols: list[str]) -> dict[str, float]:
        return {symbol: self.price for symbol in symbols}

    def get_ticker(self, symbol: str) -> dict[str, str]:
        return {"symbol": symbol, "lastPrice": str(self.price)}

    def place_order(self, *_args, **_kwargs):
        self.live_calls.append("place_order")
        raise AssertionError("paper lifecycle must not call place_order")

    def update_protection(self, *_args, **_kwargs):
        self.live_calls.append("update_protection")
        raise AssertionError("paper lifecycle must not call update_protection")

    def close_position_market(self, *_args, **_kwargs):
        self.live_calls.append("close_position_market")
        raise AssertionError("paper lifecycle must not call close_position_market")

    def cancel_all_orders(self, *_args, **_kwargs):
        self.live_calls.append("cancel_all_orders")
        raise AssertionError("paper lifecycle must not call cancel_all_orders")

    def get_position_amt(self, *_args, **_kwargs):
        self.live_calls.append("get_position_amt")
        raise AssertionError("paper lifecycle must not reconcile broker quantity")


def _open_long(pm: PositionManager):
    return pm.open_position(
        symbol="BTCUSDT",
        side=PositionSide.LONG,
        position_id="paper-position-1",
        entry_price=100.0,
        qty=1.0,
        stop_price=90.0,
        tp1_price=110.0,
        tp2_price=122.0,
        sl_order_id="paper_sl_initial",
        tp_order_id="paper_tp_initial",
    )


def test_paper_stop_hit_can_close_without_live_order_calls():
    client = LifecyclePaperClient(price=89.0)
    executor = BinanceExecutor(client=client, execution_mode="paper")
    pm = PositionManager()
    _open_long(pm)

    action = pm.update_price("BTCUSDT", 89.0, current_atr=2.0)
    close = executor.execute_signal("BTCUSDT", "CLOSE", 0.0)

    assert action == "HIT_STOP"
    assert close.success is True
    assert close.status == "CLOSED_POSITION"
    assert close.action == "PAPER_POSITION_CLOSED"
    assert client.live_calls == []


def test_paper_tp1_runner_can_reach_tp2_without_live_order_calls():
    client = LifecyclePaperClient(price=123.0)
    executor = BinanceExecutor(client=client, execution_mode="paper")
    pm = PositionManager()
    pos = _open_long(pm)

    tp1 = executor.execute_tp1_partial_close(
        symbol="BTCUSDT",
        live_qty=pos.current_qty,
        position_side="LONG",
        sl_price=pos.sl.current_stop,
        tp_price=pos.tp.tp2_price,
        sl_order_id=pos.sl.sl_order_id,
        tp_order_id=pos.sl.tp_order_id,
        tp1_fraction=0.5,
        position_manager=pm,
    )
    pos = pm.get_position("BTCUSDT")
    action = pm.update_price("BTCUSDT", 123.0, current_atr=100.0)
    close = executor.execute_signal("BTCUSDT", "CLOSE", 0.0)

    assert tp1["reduce_only_status"] == "PAPER_REDUCE_ONLY"
    assert tp1["protection_update_status"] == "PAPER_PROTECTION_RESIZED"
    assert tp1["fill_qty"] == 0.5
    assert pos is not None
    assert pos.current_qty == 0.5
    assert pos.phase == PositionPhase.EXITING
    assert action == "HIT_TP2"
    assert close.success is True
    assert client.live_calls == []


def test_paper_break_even_update_can_stop_runner_without_live_order_calls():
    client = LifecyclePaperClient(price=102.0)
    executor = BinanceExecutor(client=client, execution_mode="paper")
    pm = PositionManager()
    pos = _open_long(pm)

    tp1 = executor.execute_tp1_partial_close(
        symbol="BTCUSDT",
        live_qty=pos.current_qty,
        position_side="LONG",
        sl_price=pos.sl.current_stop,
        tp_price=pos.tp.tp2_price,
        sl_order_id=pos.sl.sl_order_id,
        tp_order_id=pos.sl.tp_order_id,
        tp1_fraction=0.5,
        position_manager=pm,
    )
    pos = pm.get_position("BTCUSDT")
    assert pos is not None
    be = executor.execute_break_even_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=pos.current_qty,
        entry_price=pos.entry_price,
        current_stop=pos.sl.current_stop,
        sl_order_id=pos.sl.sl_order_id,
        tp_order_id=pos.sl.tp_order_id,
        tp2_price=pos.tp.tp2_price,
        position_manager=pm,
    )
    pos = pm.get_position("BTCUSDT")
    assert pos is not None
    action = pm.update_price("BTCUSDT", float(pos.sl.current_stop), current_atr=2.0)
    close = executor.execute_signal("BTCUSDT", "CLOSE", 0.0)

    assert tp1["protection_update_status"] == "PAPER_PROTECTION_RESIZED"
    assert be["break_even_applied"] is True
    assert be["protection_update_status"] == "PAPER_PROTECTION_UPDATED"
    assert pos.sl.be_exchange_confirmed is True
    assert action == "HIT_STOP"
    assert close.success is True
    assert client.live_calls == []


def test_paper_trailing_update_never_calls_exchange_protection():
    client = LifecyclePaperClient(price=120.0)
    executor = BinanceExecutor(client=client, execution_mode="paper")
    pm = PositionManager()
    pos = _open_long(pm)
    pos.tp.tp1_hit = True
    pos.sl.be_exchange_confirmed = True
    pos.phase = PositionPhase.RUNNER_TRAILING
    pos.current_qty = 0.5

    result = executor.execute_trailing_stop_update(
        symbol="BTCUSDT",
        position_side="LONG",
        runner_qty=pos.current_qty,
        entry_price=pos.entry_price,
        current_stop=pos.sl.current_stop,
        highest_since_entry=120.0,
        lowest_since_entry=100.0,
        atr=5.0,
        sl_order_id=pos.sl.sl_order_id,
        tp_order_id=pos.sl.tp_order_id,
        tp2_price=pos.tp.tp2_price,
        position_manager=pm,
        min_update_interval_s=0.0,
    )

    assert result["trailing_applied"] is True
    assert result["protection_update_status"] == "PAPER_TRAILING_UPDATED"
    assert pm.get_position("BTCUSDT").phase == PositionPhase.RUNNER_TRAILING
    assert client.live_calls == []
