import threading
from app.runner.runner import PaperRunner
from app.runner.models import SymbolState
from app.core.config import settings


class _FakeClient:
    def __init__(self, position_amt: float, entry_price: float):
        self._position_amt = position_amt
        self._entry_price = entry_price

    def klines(self, symbol: str, interval: str, limit: int = 120):
        # minimal shape; runner only needs it to not crash
        return [[0, "0", "0", "0", "0", "0", 0, "0", 0, "0", "0", "0"]] * limit

    def last_price(self, symbol: str):
        return 100.0

    def get_position_info(self, symbol: str) -> dict:
        return {
            "positionAmt": str(self._position_amt),
            "entryPrice": str(self._entry_price),
        }

    # Used by some paths
    def get_position_amt(self, symbol: str) -> float:
        return float(self._position_amt)


class _Sig:
    def __init__(self, v: str):
        self.value = v


class _StrategyRes:
    def __init__(self, signal: str = "HOLD"):
        self.signal = _Sig(signal)
        self.confidence = 0.0
        self.reason = "test"
        self.meta = {}


def _make_runner(monkeypatch, client: _FakeClient, symbol: str):
    r = PaperRunner(client)

    # Ensure symbol exists in runner state & locks
    if symbol not in r.state:
        r.state[symbol] = SymbolState()
    if symbol not in r._symbol_locks:
        r._symbol_locks[symbol] = threading.Lock()

    # Avoid DB/audit writes
    monkeypatch.setattr(r.audit, "event", lambda *a, **k: None)
    monkeypatch.setattr(r.store, "save_symbol", lambda *a, **k: None)
    monkeypatch.setattr(r.store, "save_daily", lambda *a, **k: None)

    # Force strategy to HOLD so runner doesn't trade
    monkeypatch.setattr(r.strategy, "get_signal", lambda sym: _StrategyRes("HOLD"))

    # Ensure exit rules do nothing in this test
    import app.runner.runner as runner_mod

    monkeypatch.setattr(runner_mod, "should_exit", lambda **kwargs: (False, None))

    return r


def test_state_sync_flat_resets_fields(monkeypatch):
    settings.EXECUTION_MODE = "live"
    sym = "BTCUSDT"
    client = _FakeClient(position_amt=0.0, entry_price=0.0)
    r = _make_runner(monkeypatch, client, sym)

    st = r.state[sym]
    st.position = "LONG"
    st.entry_price = 100.0
    st.entry_qty = 1.0
    st.adds = 2
    st.pending_open = "BUY"

    r.step_symbol(sym)

    st2 = r.state[sym]
    assert st2.position == "NONE"
    assert st2.entry_price is None
    assert st2.entry_qty == 0.0
    assert st2.adds == 0
    assert st2.pending_open == "NONE"


def test_state_sync_broker_overrides_local(monkeypatch):
    settings.EXECUTION_MODE = "live"
    sym = "BTCUSDT"
    client = _FakeClient(position_amt=-2.0, entry_price=123.4)
    r = _make_runner(monkeypatch, client, sym)

    st = r.state[sym]
    st.position = "LONG"  # wrong locally

    r.step_symbol(sym)

    st2 = r.state[sym]
    assert st2.position == "SHORT"
    assert st2.entry_price == 123.4
    assert st2.entry_qty == 2.0
