import inspect
from app.execution.executor import BinanceExecutor


class _FakeClient:
    """
    Fake client that supports both styles of ensure_protection implementations:
    - ensure_protection(symbol, signal, qty, sl_price, tp_price)
    - ensure_protection(symbol) which derives / calculates sl/tp internally
    """

    def __init__(self, *, pos_amt: float, entry_price: float, sl: float, tp: float):
        self._pos_amt = pos_amt
        self._entry_price = entry_price
        self._sl = sl
        self._tp = tp

        self.cancel_calls = 0
        self.sl_calls = 0
        self.tp_calls = 0

    # --- position ---
    def get_position_amt(self, symbol: str) -> float:
        return float(self._pos_amt)

    def get_position_info(self, symbol: str) -> dict:
        return {"positionAmt": str(self._pos_amt), "entryPrice": str(self._entry_price)}

    # --- orders ---
    def open_orders(self, symbol: str):
        # Existing protection orders with trigger prices (realistic)
        return [
            {"type": "STOP_MARKET", "stopPrice": str(self._sl)},
            {"type": "TAKE_PROFIT_MARKET", "stopPrice": str(self._tp)},
        ]

    def cancel_all_orders(self, symbol: str):
        self.cancel_calls += 1
        return {"status": "canceled", "symbol": symbol}

    # --- used by some executor implementations during recreation ---
    def exchange_info_cached(self):
        return {}

    def last_price(self, symbol: str):
        return 100.0

    # Methods that some executors use to place protection
    def place_stop_market(self, symbol: str, side: str, stop_price: float):
        self.sl_calls += 1
        return {"ok": True, "type": "STOP_MARKET", "stopPrice": stop_price}

    def place_take_profit_market(self, symbol: str, side: str, stop_price: float):
        self.tp_calls += 1
        return {"ok": True, "type": "TAKE_PROFIT_MARKET", "stopPrice": stop_price}


def _call_ensure_protection(
    ex: BinanceExecutor, symbol: str, *, signal="BUY", qty=1.0, sl=95.0, tp=110.0
):
    """
    Calls ensure_protection() regardless of whether it takes 1 arg or 5 args.
    """
    fn = ex.ensure_protection
    sig = inspect.signature(fn)
    params = list(sig.parameters.keys())

    # params include 'self' in inspect? No—bound method, so not included.
    if len(params) == 1:
        # ensure_protection(symbol)
        return fn(symbol)
    # ensure_protection(symbol, signal, qty, sl_price, tp_price) OR similar
    return fn(symbol, signal, qty=qty, sl_price=sl, tp_price=tp)


def test_auto_repair_wrong_side_long(monkeypatch):
    # LONG with wrong-side SL (above entry) should trigger cancel + re-place (repair)
    client = _FakeClient(pos_amt=1.0, entry_price=100.0, sl=105.0, tp=110.0)
    ex = BinanceExecutor(client)

    # Some executors use extract_filters for rounding; patch it to provide tick_size.
    import app.execution.executor as ex_mod

    if hasattr(ex_mod, "extract_filters"):

        class _F:
            tick_size = ex_mod.Decimal("0.01")
            step_size = ex_mod.Decimal("0.01")

        monkeypatch.setattr(ex_mod, "extract_filters", lambda exch, sym: _F())

    # Some executors call place_protection_orders; patch it so we can assert it was invoked.
    placed = {"called": 0}

    if hasattr(ex, "place_protection_orders"):

        def _fake_place(symbol, signal, qty, sl_price, tp_price):
            placed["called"] += 1
            # Simulate actual placement
            client.place_stop_market(symbol, "SELL", float(sl_price))
            client.place_take_profit_market(symbol, "SELL", float(tp_price))
            return {"sl_price": float(sl_price), "tp_price": float(tp_price)}

        monkeypatch.setattr(ex, "place_protection_orders", _fake_place)

    out = _call_ensure_protection(
        ex, "BTCUSDT", signal="BUY", qty=1.0, sl=105.0, tp=110.0
    )

    # We accept either "repaired" or a dict with repaired=True depending on your implementation style
    assert (out.get("status") == "repaired") or (out.get("repaired") is True)

    assert client.cancel_calls >= 1
    # Either direct placement functions were called, or place_protection_orders was called
    assert (client.sl_calls >= 1 and client.tp_calls >= 1) or (placed["called"] >= 1)


def test_no_repair_when_sane_long(monkeypatch):
    client = _FakeClient(pos_amt=1.0, entry_price=100.0, sl=95.0, tp=110.0)
    ex = BinanceExecutor(client)

    out = _call_ensure_protection(
        ex, "BTCUSDT", signal="BUY", qty=1.0, sl=95.0, tp=110.0
    )

    # If protection exists and is sane, should not cancel
    assert client.cancel_calls == 0
    # Many implementations return {"status":"ok"} or {"repaired":False} or similar
    assert out is not None
