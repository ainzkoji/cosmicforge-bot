"""
Maps economic event country/currency codes to trading symbols.

USD macro events → global block (all crypto symbols).
EUR/GBP/JPY events → log only (no hard block, crypto pairs rarely move on these).
Crypto-specific events → symbol-specific block if mapping is reliable.

Extend CRYPTO_EVENT_SYMBOL_MAP as needed when adding crypto-native events.
"""
from __future__ import annotations

import re
from typing import List, Optional

# Currencies whose high-impact events trigger a GLOBAL block on all crypto
GLOBAL_BLOCK_CURRENCIES = {"USD"}

# Currencies that log only (medium or low impact path regardless of configured level)
LOG_ONLY_CURRENCIES = {"EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD", "CNY"}

# Crypto-specific symbol mappings (event_type → affected symbols)
CRYPTO_EVENT_SYMBOL_MAP: dict[str, List[str]] = {
    "ETH_UPGRADE": ["ETHUSDT", "ETHBTC"],
    "BTC_HALVING": ["BTCUSDT"],
    "XRP_SEC": ["XRPUSDT"],
    "SOL_MAINNET": ["SOLUSDT"],
}

_CRYPTO_EVENT_TYPES = {
    "TOKEN_UNLOCK",
    "UPGRADE",
    "LISTING",
    "TOKEN_BURN",
    "LAUNCH",
    "AIRDROP",
    "CRYPTO_EVENT",
}


def is_global_block_currency(country_currency: str) -> bool:
    """Return True if events for this currency should trigger a global trade block."""
    return country_currency.upper() in GLOBAL_BLOCK_CURRENCIES


def get_affected_symbols(
    event_type: str,
    country_currency: str,
    all_active_symbols: Optional[List[str]] = None,
) -> Optional[List[str]]:
    """
    Return the list of symbols affected by an event, or None for a global block.

    None means ALL symbols (global block).
    An empty list means no symbols (log/display only).
    A non-empty list means only those specific symbols are blocked.
    """
    cc = country_currency.upper()

    if cc in GLOBAL_BLOCK_CURRENCIES:
        return None  # global — caller should treat as block on all

    if event_type in CRYPTO_EVENT_SYMBOL_MAP:
        return CRYPTO_EVENT_SYMBOL_MAP[event_type]

    if event_type in _CRYPTO_EVENT_TYPES:
        coin = cc.upper()
        if re.fullmatch(r"[A-Z0-9]{2,12}", coin):
            candidate = f"{coin}USDT"
            if all_active_symbols:
                return [sym for sym in all_active_symbols if sym.upper() == candidate]
            return [candidate]

    # EUR, GBP, etc. — log only
    return []
