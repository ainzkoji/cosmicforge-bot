from app.core.config import settings
from app.execution.executor import BinanceExecutor


def _force_live_mode() -> None:
    """Force settings to run the live execution branch for these unit tests."""
    # Settings is created at import time; override in-memory values.
    settings.EXECUTION_MODE = "live"
    settings.LIVE_SYMBOLS = ["BTCUSDT"]


class _FakeClient:
    """Minimal fake Binance client for position-management tests."""

    def __init__(self, *, pos_amt_sequence=None):
        # Sequence returned by get_position_info(). If None, always flat.
        self._pos_amt_sequence = list(pos_amt_sequence or [0.0])
        self.cancel_calls = 0
        self.close_calls = 0
        self.set_leverage_calls = 0

    # --- exchange position ---
    def get_position_amt(self, symbol: str) -> float:
        # Use the *current* sequence head as the position amount.
        if not self._pos_amt_sequence:
            return 0.0
        return float(self._pos_amt_sequence[0])

    def get_position_info(self, symbol: str) -> dict:
        if not self._pos_amt_sequence:
            amt = 0.0
        else:
            amt = float(self._pos_amt_sequence[0])
            # advance toward flat over time
            if len(self._pos_amt_sequence) > 1:
                self._pos_amt_sequence.pop(0)
        return {"positionAmt": str(amt)}

    # --- order ops ---
    def cancel_all_orders(self, symbol: str) -> dict:
        self.cancel_calls += 1
        return {"status": "canceled", "symbol": symbol}

    def close_position_market(self, symbol: str) -> dict:
        self.close_calls += 1
        return {"status": "close_sent", "symbol": symbol}

    # --- misc required by executor ---
    def set_leverage(self, symbol: str, leverage: int) -> dict:
        self.set_leverage_calls += 1
        return {"symbol": symbol, "leverage": leverage}


def test_close_when_already_flat_returns_no_trade():
    _force_live_mode()
    client = _FakeClient(pos_amt_sequence=[0.0])
    ex = BinanceExecutor(client)

    r = ex.execute_signal("BTCUSDT", "CLOSE", usdt=10)

    assert r.action == "NO_TRADE"
    assert r.details["reason"] == "ALREADY_FLAT"
    assert client.cancel_calls == 0
    assert client.close_calls == 0


def test_close_long_cancels_then_closes_and_confirms_flat():
    _force_live_mode()
    # Simulate: position exists, then becomes flat on next poll.
    client = _FakeClient(pos_amt_sequence=[0.01, 0.0])
    ex = BinanceExecutor(client)

    r = ex.execute_signal("BTCUSDT", "CLOSE", usdt=10)

    assert r.action == "CLOSED_POSITION"
    assert client.cancel_calls == 1
    assert client.close_calls == 1
    assert r.details["position_before"] == "LONG"
    assert r.details["confirmed_flat"] is True


def test_flip_sell_while_long_closes_first_and_confirms_flat():
    _force_live_mode()
    # SELL while LONG should *not* open short here; it must close first.
    client = _FakeClient(pos_amt_sequence=[0.02, 0.0])
    ex = BinanceExecutor(client)

    r = ex.execute_signal("BTCUSDT", "SELL", usdt=10)

    assert r.action == "CLOSED_LONG"
    assert client.set_leverage_calls == 1  # leverage ensured before action
    assert client.cancel_calls == 1
    assert client.close_calls == 1
    assert r.details["confirmed_flat"] is True


def test_flip_buy_while_short_closes_first_and_confirms_flat():
    _force_live_mode()
    client = _FakeClient(pos_amt_sequence=[-0.02, 0.0])
    ex = BinanceExecutor(client)

    r = ex.execute_signal("BTCUSDT", "BUY", usdt=10)

    assert r.action == "CLOSED_SHORT"
    assert client.set_leverage_calls == 1
    assert client.cancel_calls == 1
    assert client.close_calls == 1
    assert r.details["confirmed_flat"] is True
