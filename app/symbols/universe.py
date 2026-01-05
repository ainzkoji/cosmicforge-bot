from __future__ import annotations
from dataclasses import dataclass
from typing import List, Set
from typing import Union, List


@dataclass(frozen=True)
class SymbolUniverse:
    requested: List[str]
    valid: List[str]
    invalid: List[str]


def parse_symbols(raw: Union[str, List[str]], max_symbols: int = 100) -> List[str]:
    # Accept both CSV string and list[str]
    if isinstance(raw, list):
        symbols = [str(s).strip().upper() for s in raw if str(s).strip()]
    else:
        symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]

    # remove duplicates but keep order
    seen: Set[str] = set()
    unique = []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            unique.append(s)

    return unique[:max_symbols]


def build_universe(requested: List[str], exchange_info: dict) -> SymbolUniverse:
    # Binance exchangeInfo structure: {"symbols": [{"symbol": "...", "status":"TRADING", ...}, ...]}
    info_symbols = exchange_info.get("symbols", [])
    tradable = {s["symbol"] for s in info_symbols if s.get("status") == "TRADING"}

    valid = [s for s in requested if s in tradable]
    invalid = [s for s in requested if s not in tradable]
    return SymbolUniverse(requested=requested, valid=valid, invalid=invalid)
