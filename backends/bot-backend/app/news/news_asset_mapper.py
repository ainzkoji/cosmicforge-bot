"""
Maps news items to trading symbols via keyword matching.
"""
from __future__ import annotations

import re
from typing import Dict, List, Tuple

from shared_lib.persistence.db import DB
from shared_lib.persistence.news_intelligence import upsert_asset_mapping


# symbol -> list of keywords (case-insensitive)
_SYMBOL_KEYWORDS: Dict[str, List[str]] = {
    "BTCUSDT": ["bitcoin", "btc", "satoshi", "lightning network"],
    "ETHUSDT": ["ethereum", "eth", "ether", "solidity", "evm", "vitalik"],
    "XRPUSDT": ["xrp", "ripple", "ripplenet"],
    "SOLUSDT": ["solana", "sol "],
    "BNBUSDT": ["binance coin", "bnb", "bsc", "binance smart chain"],
    "ADAUSDT": ["cardano", "ada", "hoskinson"],
    "DOGEUSDT": ["dogecoin", "doge"],
    "MATICUSDT": ["polygon", "matic"],
    "AVAXUSDT": ["avalanche", "avax"],
    "LINKUSDT": ["chainlink", "link oracle"],
    "DOTUSDT": ["polkadot", "dot "],
    "LTCUSDT": ["litecoin", "ltc"],
    "UNIUSDT": ["uniswap", "uni "],
    "ATOMUSDT": ["cosmos", "atom "],
    "NEARUSDT": ["near protocol", "near "],
}

# Broad market terms map to multiple symbols
_BROAD_TERMS: Dict[str, List[str]] = {
    "crypto": list(_SYMBOL_KEYWORDS.keys()),
    "cryptocurrency": list(_SYMBOL_KEYWORDS.keys()),
    "defi": ["ETHUSDT", "UNIUSDT", "LINKUSDT", "AVAXUSDT"],
    "nft": ["ETHUSDT", "SOLUSDT", "MATICUSDT"],
    "blockchain": list(_SYMBOL_KEYWORDS.keys()),
    "altcoin": [k for k in _SYMBOL_KEYWORDS if k != "BTCUSDT"],
}


class NewsAssetMapper:
    def __init__(self, db: DB, extra_symbols: List[str] | None = None) -> None:
        self._db = db
        self._symbols = set(_SYMBOL_KEYWORDS.keys())
        if extra_symbols:
            self._symbols.update(extra_symbols)

    def map_and_store(self, cluster_id: int, title: str, body: str = "") -> List[str]:
        """
        Returns list of matched symbols and persists mappings to DB.
        """
        text = f"{title} {body}".lower()
        matched: Dict[str, float] = {}

        for symbol, keywords in _SYMBOL_KEYWORDS.items():
            for kw in keywords:
                if re.search(r"\b" + re.escape(kw.lower()) + r"\b", text):
                    matched[symbol] = max(matched.get(symbol, 0.0), 0.90)
                    break

        for term, symbols in _BROAD_TERMS.items():
            if re.search(r"\b" + re.escape(term) + r"\b", text):
                for symbol in symbols:
                    if symbol not in matched:
                        matched[symbol] = 0.60

        for symbol, confidence in matched.items():
            upsert_asset_mapping(
                self._db,
                cluster_id=cluster_id,
                symbol=symbol,
                asset="crypto",
                mapping_confidence=confidence,
                mapping_reason="keyword",
            )

        return list(matched.keys())
