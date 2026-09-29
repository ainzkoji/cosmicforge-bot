import pytest

from app.core.config import Settings


def test_live_requires_valid_binance_env():
    s = Settings(
        EXECUTION_MODE="live",
        BINANCE_ENV="banana",  # invalid
        TRADE_SYMBOLS="BTCUSDT",
        LIVE_SYMBOLS="BTCUSDT",
        MAX_SYMBOLS=1,
    )
    with pytest.raises(ValueError):
        s.validate_runtime()


def test_live_mainnet_warning_not_error():
    s = Settings(
        EXECUTION_MODE="live",
        BINANCE_ENV="mainnet",
        # inert placeholders: the separate key-presence rule must not mask what this test checks
        BINANCE_API_KEY="placeholder-not-a-key",
        BINANCE_API_SECRET="placeholder-not-a-secret",
        TRADE_SYMBOLS="BTCUSDT",
        LIVE_SYMBOLS="BTCUSDT",
        MAX_SYMBOLS=1,
    )
    warnings = s.validate_runtime()
    assert any("REAL money" in w or "REAL MONEY" in w for w in warnings)
