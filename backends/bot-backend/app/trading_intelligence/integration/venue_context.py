"""Runner -> VenueEconomicContext (Section 17.22 shadow integration).

Reads ONLY normalized, non-secret account metadata from the bot context --
``broker_type``, ``broker_environment`` (the canonical
``broker_accounts.environment``), ``user_id``, ``broker_account_id``,
``bot_instance_id`` -- and asks the resolved adapter's collector to capture
public market data through the runner's EXISTING exchange client. The
context's API key/secret/base-url fields are never read; the client object
is never stored in CATI evidence.

Never raises: any failure yields an explicit fail-closed context (the cost
model then marks the estimate COST_NOT_VIABLE), never a zero-cost default.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

from app.trading_intelligence.contracts.venue_economics import VenueReasonCode
from app.trading_intelligence.venue.adapter import VenueRawSnapshot, normalize_environment
from app.trading_intelligence.venue.context import VenueEconomicContext
from app.trading_intelligence.venue.policy import VenueCostPolicy, default_venue_cost_policy
from app.trading_intelligence.venue.registry import resolve_adapter

logger = logging.getLogger(__name__)

#: The only context attributes Section 17 may read.
SAFE_CONTEXT_FIELDS = ("broker_type", "broker_environment", "user_id", "broker_account_id", "bot_instance_id")


def venue_context_from_runner(runner: Any, symbol: str, *, now_ms: Optional[int] = None,
                              policy: Optional[VenueCostPolicy] = None,
                              broker_health: Any = None) -> VenueEconomicContext:
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    policy = policy or default_venue_cost_policy()
    ctx = getattr(runner, "context", None)
    safe = {name: getattr(ctx, name, None) for name in SAFE_CONTEXT_FIELDS}
    adapter, collector = resolve_adapter(safe["broker_type"], policy)
    sym = str(symbol).upper()
    raw = VenueRawSnapshot(venue_symbol=sym, payloads={}, captured_at=now_ms)
    client = getattr(runner, "client", None)
    if collector is not None and client is not None:
        try:
            raw = collector(client, sym)
        except Exception as exc:  # recorded; the observation then fails closed
            logger.info("[CATI_VENUE] %s: collection failed (%s)", sym, type(exc).__name__)
            raw = VenueRawSnapshot(venue_symbol=sym, payloads={}, captured_at=now_ms,
                                   reason_codes=(VenueReasonCode.COLLECTION_ERROR.value,))
    return VenueEconomicContext(
        adapter=adapter, raw=raw, environment=normalize_environment(safe["broker_environment"]),
        decision_time=now_ms, user_id=safe["user_id"], broker_account_id=safe["broker_account_id"],
        bot_instance_id=safe["bot_instance_id"], run_id=str(getattr(runner, "run_id", "") or "") or None,
        cycle_id=getattr(runner, "cycle_id", None), broker_health=broker_health, policy=policy,
    )


__all__ = ["SAFE_CONTEXT_FIELDS", "venue_context_from_runner"]
