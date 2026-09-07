"""
Symbol mapping utilities for MetaTrader bridge.

Handles conversion between internal canonical symbols (e.g., "EUR_USD") 
and MT4/MT5 bridge symbols (e.g., "EURUSD").
"""

from typing import Dict

def to_mt_symbol(canonical: str) -> str:
    """
    Convert internal canonical symbol to MT symbol.
    
    Examples:
        EUR_USD -> EURUSD
        GBP_JPY -> GBPJPY
        BTC_USD -> BTCUSD
    
    Args:
        canonical: Internal symbol format (with underscore)
        
    Returns:
        MT symbol format (no separator)
    """
    return canonical.replace("_", "").replace("-", "")


def from_mt_symbol(mt_symbol: str) -> str:
    """
    Convert MT symbol to internal canonical format.
    
    For forex pairs, inserts underscore after 3 characters.
    For other symbols, returns as-is (or applies custom rules).
    
    Examples:
        EURUSD -> EUR_USD
        GBPJPY -> GBP_JPY
        BTCUSD -> BTC_USD
    
    Args:
        mt_symbol: MT symbol format
        
    Returns:
        Internal canonical format
    """
    # For standard forex pairs (6 characters), split 3-3
    if len(mt_symbol) == 6 and mt_symbol.isalpha():
        return f"{mt_symbol[:3]}_{mt_symbol[3:]}"
    
    # For other formats, check for common patterns
    # BTC, ETH, XAU, XAG typically 6 chars: BTCUSD, XAUUSD
    if mt_symbol.startswith(("BTC", "ETH", "XAU", "XAG", "XPT", "XPD")):
        # Crypto or metals - assume 3-letter prefix
        if len(mt_symbol) >= 6:
            return f"{mt_symbol[:3]}_{mt_symbol[3:]}"
    
    # Fallback: return as-is (indices, CFDs, stocks may not need conversion)
    return mt_symbol


def build_mapping_dict(mt_symbols: list[str]) -> Dict[str, str]:
    """
    Build bidirectional mapping from a list of MT symbols.
    
    Returns:
        Dictionary with both MT->canonical and canonical->MT mappings
    """
    mapping = {}
    
    for mt_sym in mt_symbols:
        canonical = from_mt_symbol(mt_sym)
        mapping[mt_sym] = canonical  # MT -> canonical
        mapping[canonical] = mt_sym  # canonical -> MT
    
    return mapping
