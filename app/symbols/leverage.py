# app/symbols/leverage.py
from __future__ import annotations

from typing import Any, Dict, Mapping


def parse_leverage_map(raw: Any) -> Dict[str, int]:
    """
    Accepts:
      - dict: {"BTCUSDT": 5, "ETHUSDT": 10}
      - str:  "BTCUSDT:5,ETHUSDT:10"
      - list: ["BTCUSDT:5", "ETHUSDT:10"]
    Returns normalized dict with UPPERCASE symbols.
    """
    if raw is None or raw == "":
        return {}

    # already a dict-like object
    if isinstance(raw, Mapping):
        out: Dict[str, int] = {}
        for k, v in raw.items():
            key = str(k).strip().upper()
            if not key:
                continue
            out[key] = int(v)
        return out

    # list/tuple/set -> treat as comma joined
    if isinstance(raw, (list, tuple, set)):
        raw = ",".join(str(x) for x in raw)

    s = str(raw).strip()
    if not s:
        return {}

    out: Dict[str, int] = {}
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        out[k.strip().upper()] = int(v.strip())
    return out


def leverage_for(symbol: str, lev_map: Dict[str, int], default_lev: int, min_lev: int) -> int:
    sym = symbol.strip().upper()
    lev = int(lev_map.get(sym, default_lev))
    if lev < int(min_lev):
        lev = int(min_lev)
    return lev
