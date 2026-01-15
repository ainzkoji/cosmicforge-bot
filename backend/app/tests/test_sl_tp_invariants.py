import pytest

from app.execution.exit_rules import _validate_sl_tp


def test_long_sl_tp_ok():
    _validate_sl_tp(
        side="LONG",
        entry_price=100,
        stop_loss=95,
        take_profit=110,
    )


def test_long_invalid_sl():
    with pytest.raises(ValueError):
        _validate_sl_tp(
            side="LONG",
            entry_price=100,
            stop_loss=101,
            take_profit=110,
        )


def test_short_sl_tp_ok():
    _validate_sl_tp(
        side="SHORT",
        entry_price=100,
        stop_loss=105,
        take_profit=90,
    )


def test_short_invalid_tp():
    with pytest.raises(ValueError):
        _validate_sl_tp(
            side="SHORT",
            entry_price=100,
            stop_loss=105,
            take_profit=101,
        )
