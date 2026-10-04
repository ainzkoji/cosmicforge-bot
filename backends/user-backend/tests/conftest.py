import os
import pytest
os.environ["APP_ENV"] = "TEST"


@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    """
    Ensure tests never hit live trading accidentally.
    """
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setenv("BINANCE_ENV", "testnet")
    monkeypatch.setenv("TRADE_SYMBOLS", "BTCUSDT")
    monkeypatch.setenv("LIVE_SYMBOLS", "BTCUSDT")
    monkeypatch.setenv("DEFAULT_INTERVAL", "1m")
    monkeypatch.setenv("MAX_SYMBOLS", "1")
