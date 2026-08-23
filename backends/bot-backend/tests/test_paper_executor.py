from __future__ import annotations

import pytest

from app.execution.executor import BinanceExecutor
from app.execution.paper_executor import PaperExecutor


class PriceOnlyClient:
    def __init__(self, price: float | None = 100.0) -> None:
        self.price = price
        self.live_order_calls: list[str] = []

    def get_prices(self, symbols: list[str]) -> dict[str, float]:
        if self.price is None:
            return {}
        return {symbol: float(self.price) for symbol in symbols}

    def get_ticker(self, symbol: str) -> dict[str, str]:
        if self.price is None:
            raise RuntimeError("no ticker")
        return {
            "symbol": symbol,
            "lastPrice": str(self.price),
            "bidPrice": str(float(self.price) * 0.9999),
            "askPrice": str(float(self.price) * 1.0001),
        }

    def klines(self, **_kwargs):
        return []

    def place_order(self, *_args, **_kwargs):
        self.live_order_calls.append("place_order")
        raise AssertionError("paper execution must not call place_order")

    def update_protection(self, *_args, **_kwargs):
        self.live_order_calls.append("update_protection")
        raise AssertionError("paper execution must not call update_protection")

    def close_position_market(self, *_args, **_kwargs):
        self.live_order_calls.append("close_position_market")
        raise AssertionError("paper execution must not call close_position_market")

    def get_position_amt(self, *_args, **_kwargs):
        self.live_order_calls.append("get_position_amt")
        raise AssertionError("paper execution must not call get_position_amt")


def test_paper_mode_open_returns_internal_fill_without_live_order_calls():
    client = PriceOnlyClient(price=100.0)
    executor = BinanceExecutor(client=client, execution_mode="paper")

    result = executor.execute_signal(
        "BTCUSDT",
        "BUY",
        25.0,
        sl_price=95.0,
        tp_price=110.0,
    )

    assert result.success is True
    assert result.status == "PAPER_POSITION_OPENED"
    assert result.action == "ORDER_PLACED"
    assert result.order_id and result.order_id.startswith("paper_order_")
    assert result.details["status"] == "PAPER_FILLED"
    assert result.details["execution_status"] == "PAPER_POSITION_OPENED"
    assert result.details["protection"]["sl_order_id"].startswith("paper_sl_")
    assert result.details["protection"]["tp_order_id"].startswith("paper_tp_")
    assert result.avg_price == pytest.approx(100.02)
    assert result.details["filled_qty"] == pytest.approx(25.0 / 100.02)
    assert result.details["fee"] == pytest.approx(result.details["filled_qty"] * 100.02 * 0.0004)
    assert client.live_order_calls == []


def test_paper_mode_close_returns_internal_close_without_live_order_calls():
    client = PriceOnlyClient(price=100.0)
    executor = BinanceExecutor(client=client, execution_mode="paper")

    result = executor.execute_signal("BTCUSDT", "CLOSE", 0.0)

    assert result.success is True
    assert result.status == "CLOSED_POSITION"
    assert result.action == "PAPER_POSITION_CLOSED"
    assert result.order_id and result.order_id.startswith("paper_order_")
    assert result.details["execution_status"] == "PAPER_POSITION_CLOSED"
    assert client.live_order_calls == []


def test_paper_mode_reports_error_when_reference_price_is_missing():
    client = PriceOnlyClient(price=None)
    executor = BinanceExecutor(client=client, execution_mode="paper")

    result = executor.execute_signal("BTCUSDT", "BUY", 25.0)

    assert result.success is False
    assert result.status == "PAPER_ERROR"
    assert "No reference price available" in str(result.error)
    assert client.live_order_calls == []


def test_paper_executor_sell_slippage_and_fee_are_explicit():
    client = PriceOnlyClient(price=200.0)
    executor = PaperExecutor(client=client, slippage_bps=5.0, fee_bps=10.0)

    result = executor.open_position(symbol="ETHUSDT", side="SELL", notional_usdt=100.0)

    assert result.success is True
    assert result.avg_price == pytest.approx(199.9)
    assert result.filled_qty == pytest.approx(100.0 / 199.9)
    assert result.fee == pytest.approx(result.filled_qty * 199.9 * 0.001)
    assert client.live_order_calls == []
