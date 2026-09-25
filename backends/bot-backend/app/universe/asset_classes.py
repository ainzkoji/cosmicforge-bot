"""A bot's allowed asset classes (Phase 3F: one bot, several asset classes).

``bot_instances.market_type`` keeps its historical values (``CRYPTO``,
``FOREX``) and additionally accepts a comma-separated set or ``MULTI_ASSET``:

    CRYPTO            -> (CRYPTO,)
    FOREX / FX        -> (FX,)
    CRYPTO,FX         -> (CRYPTO, FX)
    MULTI_ASSET       -> (CRYPTO, FX)

A single-class bot behaves exactly as before. Hard risk stays account-wide:
the classes a bot may trade never partition the account's capital.
"""
from __future__ import annotations

from typing import Optional, Tuple

_ALIASES = {"CRYPTO": "CRYPTO", "FOREX": "FX", "FX": "FX"}
MULTI_ASSET = "MULTI_ASSET"
MULTI_ASSET_DEFAULT = ("CRYPTO", "FX")

#: universe ``underlying_type`` (Binance vocabulary) -> CATI InstrumentKey asset class
UNDERLYING_TO_CATI = {"COIN": "CRYPTO", "FX": "FX", "FOREX": "FX", "COMMODITY": "FUTURES", "INDEX": "FUTURES",
                      "EQUITY": "EQUITY"}


def parse_allowed_asset_classes(market_type: Optional[str]) -> Tuple[str, ...]:
    raw = str(market_type or "").strip().upper()
    if not raw:
        return ()
    if raw == MULTI_ASSET:
        return MULTI_ASSET_DEFAULT
    out = []
    for token in raw.replace("+", ",").split(","):
        cls = _ALIASES.get(token.strip())
        if cls is None:
            raise ValueError(f"unknown asset class {token.strip()!r} in market_type {market_type!r}")
        if cls not in out:
            out.append(cls)
    return tuple(out)


def is_valid_market_type(market_type: Optional[str]) -> bool:
    try:
        return bool(parse_allowed_asset_classes(market_type))
    except ValueError:
        return False


__all__ = ["MULTI_ASSET", "MULTI_ASSET_DEFAULT", "UNDERLYING_TO_CATI", "is_valid_market_type",
           "parse_allowed_asset_classes"]
