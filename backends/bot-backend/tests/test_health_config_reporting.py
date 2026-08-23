from __future__ import annotations

import asyncio

from app.core.config import settings
from app.main import health
from app.symbols.universe import parse_symbols


def test_health_trade_symbols_count_matches_actual_trade_symbols_list():
    payload = asyncio.run(health())
    expected = parse_symbols(settings.TRADE_SYMBOLS, settings.MAX_SYMBOLS)

    assert payload["trade_symbols_count"] == len(expected)
    assert payload["trade_symbols"] == ",".join(expected)


def test_active_runtime_symbol_config_is_btc_and_eth_only():
    assert parse_symbols(settings.TRADE_SYMBOLS, settings.MAX_SYMBOLS) == [
        "BTCUSDT",
        "ETHUSDT",
    ]
