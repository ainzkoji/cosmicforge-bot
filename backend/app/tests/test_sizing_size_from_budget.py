import pytest

from app.symbols.sizing import size_from_budget


class DummyFilters:
    def __init__(self, step_size="0.001", min_qty="0.0", min_notional="0.0"):
        self.step_size = step_size
        self.min_qty = min_qty
        self.min_notional = min_notional


def test_size_ok_rounding_and_notional():
    filters = DummyFilters(step_size="0.001", min_qty="0.001", min_notional="100")
    # margin 50, leverage 5 => notional target 250
    # price 50000 => raw qty 0.005
    res = size_from_budget(
        symbol="BTCUSDT",
        price=50000,
        usdt_margin=50,
        leverage=5,
        filters=filters,
    )
    assert res.reason == "ok"
    assert res.qty > 0
    # qty must be step-size aligned (0.001)
    assert abs((res.qty / 0.001) % 1) < 1e-9
    # notional must meet min_notional
    assert res.notional >= res.min_notional_required


def test_invalid_price_returns_reason():
    filters = DummyFilters(step_size="0.001", min_qty="0.001", min_notional="100")
    res = size_from_budget(
        symbol="BTCUSDT",
        price=0,
        usdt_margin=50,
        leverage=5,
        filters=filters,
    )
    assert res.reason == "invalid_price"
    assert res.qty == 0.0


def test_qty_below_min_qty():
    filters = DummyFilters(step_size="0.001", min_qty="0.01", min_notional="0")
    # target notional 5*1=5, price 50000 => raw qty 0.0001 -> below min_qty 0.01
    res = size_from_budget(
        symbol="BTCUSDT",
        price=50000,
        usdt_margin=1,
        leverage=5,
        filters=filters,
    )
    assert res.reason == "qty_below_min_qty"
    assert res.qty == 0.0
    assert "min_qty" in res.details


def test_below_min_notional():
    filters = DummyFilters(step_size="0.001", min_qty="0.001", min_notional="200")
    # margin 10, leverage 5 => notional 50 < 200
    res = size_from_budget(
        symbol="ETHUSDT",
        price=1000,
        usdt_margin=10,
        leverage=5,
        filters=filters,
    )
    assert res.reason == "below_min_notional"
    assert res.qty == 0.0
    assert res.min_margin_required > 0


def test_conflicting_budget_args_is_strict():
    filters = DummyFilters(step_size="0.001", min_qty="0.001", min_notional="100")
    res = size_from_budget(
        symbol="BTCUSDT",
        price=50000,
        usdt_margin=50,
        budget_usdt=60,  # conflict
        leverage=5,
        filters=filters,
    )
    assert res.reason == "conflicting_budget_args"
    assert res.qty == 0.0


def test_budget_alias_budget_usdt_works():
    filters = DummyFilters(step_size="0.001", min_qty="0.001", min_notional="0")
    res1 = size_from_budget(
        symbol="BTCUSDT",
        price=50000,
        usdt_margin=50,
        leverage=5,
        filters=filters,
    )
    res2 = size_from_budget(
        symbol="BTCUSDT",
        price=50000,
        budget_usdt=50,  # alias
        leverage=5,
        filters=filters,
    )
    assert res1.reason == res2.reason
    assert res1.qty == res2.qty
