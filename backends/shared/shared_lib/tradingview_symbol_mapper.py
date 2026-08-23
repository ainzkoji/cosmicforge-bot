from __future__ import annotations

import json
import os
import re
from typing import Any

from shared_lib.persistence.db import DB


_SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,30}USDT$")


def normalize_tradingview_symbol(symbol: str) -> str:
    raw = (symbol or "").strip().upper()
    if not raw:
        return ""
    if ":" in raw:
        raw = raw.split(":", 1)[1]
    raw = raw.replace(".P", "")
    raw = raw.replace("/", "")
    raw = raw.replace("-", "")
    raw = raw.replace("_", "")
    return raw


def _env_symbols() -> set[str]:
    values: set[str] = set()
    for key in ("TRADE_SYMBOLS", "LIVE_SYMBOLS"):
        raw = os.environ.get(key, "")
        for part in raw.replace(";", ",").split(","):
            sym = normalize_tradingview_symbol(part)
            if sym:
                values.add(sym)
    return values


def _bot_symbols(db: DB, bot_id: str) -> set[str]:
    try:
        with db.connect() as conn:
            row = conn.execute(
                "SELECT symbols FROM bot_instances WHERE id = ?",
                (bot_id,),
            ).fetchone()
    except Exception:
        return set()
    if not row or not row["symbols"]:
        return set()
    try:
        parsed = json.loads(row["symbols"])
    except Exception:
        parsed = row["symbols"]
    if isinstance(parsed, str):
        items = parsed.replace(";", ",").split(",")
    elif isinstance(parsed, list):
        items = parsed
    else:
        items = []
    return {normalize_tradingview_symbol(str(item)) for item in items if str(item).strip()}


def supported_symbols_for_bot(db: DB, bot_id: str | None) -> set[str]:
    symbols = _bot_symbols(db, bot_id) if bot_id else set()
    if not symbols:
        symbols = _env_symbols()
    return {s for s in symbols if _SYMBOL_RE.match(s)}


def validate_tradingview_symbol(
    *,
    db: DB,
    bot_id: str | None,
    symbol_raw: str,
    exchange_raw: str | None = None,
    allowed_symbols: list[str] | None = None,
    expected_exchange: str = "BINANCE",
) -> tuple[bool, str | None, str | None]:
    normalized = normalize_tradingview_symbol(symbol_raw)
    if not normalized or not _SYMBOL_RE.match(normalized):
        return False, normalized or None, "UNSUPPORTED_SYMBOL"

    exchange = (exchange_raw or "").strip().upper()
    if ":" in (symbol_raw or ""):
        exchange = symbol_raw.split(":", 1)[0].strip().upper()
    if exchange and expected_exchange and exchange != expected_exchange.upper():
        return False, normalized, "EXCHANGE_MISMATCH"

    allowed = {normalize_tradingview_symbol(s) for s in (allowed_symbols or [])}
    if allowed and normalized not in allowed:
        return False, normalized, "SYMBOL_NOT_ALLOWED_BY_WEBHOOK"

    supported = supported_symbols_for_bot(db, bot_id)
    if supported and normalized not in supported:
        return False, normalized, "SYMBOL_NOT_SUPPORTED_BY_BOT"

    return True, normalized, None
