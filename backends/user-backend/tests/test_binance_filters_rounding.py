from decimal import Decimal
import pytest

from app.exchange.binance.filters import (
    round_qty_to_step,
    round_price_to_tick,
)


def _is_multiple(value: float, step: float) -> bool:
    """
    Check that value is an exact multiple of step using Decimal arithmetic.
    Float math is NOT reliable for this (e.g. 1.9 / 0.1 issues).
    """
    v = Decimal(str(value))
    s = Decimal(str(step))
    return (v / s) % 1 == 0


@pytest.mark.parametrize(
    "qty,step,expected",
    [
        (0.01234, 0.001, 0.012),
        (0.01299, 0.001, 0.012),
        (1.999, 0.1, 1.9),
        (10.0, 0.01, 10.0),
    ],
)
def test_round_qty_to_step_floor(qty, step, expected):
    out = round_qty_to_step(qty, step)
    assert out == expected
    assert _is_multiple(out, step)


@pytest.mark.parametrize(
    "price,tick,expected",
    [
        (43210.12, 0.1, 43210.1),
        (43210.19, 0.1, 43210.1),
        (123.4567, 0.01, 123.45),
        (0.123456, 0.0001, 0.1234),
    ],
)
def test_round_price_to_tick_floor(price, tick, expected):
    out = round_price_to_tick(price, tick)
    assert out == expected
    assert _is_multiple(out, tick)
