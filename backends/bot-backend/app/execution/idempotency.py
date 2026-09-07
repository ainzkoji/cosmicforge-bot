from __future__ import annotations

from decimal import Decimal
from hashlib import blake2s


def _normalize_fragment(value: str, length: int) -> str:
    cleaned = "".join(ch for ch in str(value or "").upper() if ch.isalnum())
    if not cleaned:
        cleaned = "X"
    return cleaned[:length]


def _normalize_decimal(value: float | int | str | None) -> str:
    if value in (None, ""):
        return "0"
    return format(Decimal(str(value)).normalize(), "f")


def build_entry_intent_key(
    bot_instance_id: str,
    symbol: str,
    side: str,
    intent_bucket: int,
    strategy_intent: str,
    intended_notional: float,
    sl_price: float | None = None,
    tp_price: float | None = None,
) -> str:
    payload = "|".join(
        [
            _normalize_fragment(bot_instance_id, 24),
            _normalize_fragment(symbol, 24),
            _normalize_fragment(side, 8),
            str(int(intent_bucket)),
            strategy_intent or "default",
            _normalize_decimal(intended_notional),
            _normalize_decimal(sl_price),
            _normalize_decimal(tp_price),
        ]
    )
    return blake2s(payload.encode("ascii", "ignore"), digest_size=12).hexdigest()


def generate_client_order_id(
    bot_instance_id: str,
    symbol: str | None = None,
    side: str | None = None,
    intent_key: str | None = None,
    run_id: str | None = None,
    cycle_sequence: int | None = None,
    retry_count: int = 0,
) -> str:
    if intent_key is None:
        fallback_payload = "|".join(
            [
                str(run_id or "0"),
                str(symbol or "UNKNOWN"),
                str(side or "SIDE"),
                str(cycle_sequence or 0),
                str(retry_count),
            ]
        )
        intent_key = blake2s(
            fallback_payload.encode("ascii", "ignore"),
            digest_size=12,
        ).hexdigest()

    bot_fragment = _normalize_fragment(bot_instance_id, 4)
    symbol_fragment = _normalize_fragment(symbol or "SYM", 6)
    side_fragment = _normalize_fragment(side or "S", 1)
    cid = f"CF{bot_fragment}{side_fragment}{symbol_fragment}{intent_key[:16]}"
    return cid[:36]
