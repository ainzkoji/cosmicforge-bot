from app.execution.executor import BinanceExecutor


class _FakeClient:
    def __init__(self, pos_amt: float, entry_price: float):
        self._pos_amt = pos_amt
        self._entry_price = entry_price
        self.cancel_calls = 0
        self.placed_calls = 0

    def get_position_amt(self, symbol: str) -> float:
        return self._pos_amt

    def get_position_info(self, symbol: str) -> dict:
        return {"positionAmt": str(self._pos_amt), "entryPrice": str(self._entry_price)}

    def cancel_all_orders(self, symbol: str) -> dict:
        self.cancel_calls += 1
        return {"status": "canceled"}

    def open_orders(self, symbol: str):
        # Pretend SL+TP exist already
        return [
            {"type": "STOP_MARKET"},
            {"type": "TAKE_PROFIT_MARKET"},
        ]


def test_protection_sanity_long_ok():
    ex = BinanceExecutor(_FakeClient(pos_amt=1.0, entry_price=100.0))
    assert ex._protection_is_sane("LONG", 100.0, sl=95.0, tp=110.0) is True


def test_protection_sanity_long_wrong_side():
    ex = BinanceExecutor(_FakeClient(pos_amt=1.0, entry_price=100.0))
    assert ex._protection_is_sane("LONG", 100.0, sl=105.0, tp=110.0) is False


def test_protection_sanity_short_ok():
    ex = BinanceExecutor(_FakeClient(pos_amt=-1.0, entry_price=100.0))
    assert ex._protection_is_sane("SHORT", 100.0, sl=110.0, tp=95.0) is True


def test_protection_sanity_short_wrong_side():
    ex = BinanceExecutor(_FakeClient(pos_amt=-1.0, entry_price=100.0))
    assert ex._protection_is_sane("SHORT", 100.0, sl=90.0, tp=95.0) is False
