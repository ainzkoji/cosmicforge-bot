"""
EventReactionRiskGate — optional risk integration for the Market Reaction Layer.

Design invariants:
  - When REACTION_ALLOW_RISK_INFLUENCE=False (default), ALWAYS returns
    BlockDecision(is_blocked=False). Zero impact on live trading.
  - When enabled, checks the most recent reaction for the symbol and blocks
    new entries if volatility is still elevated after an event.
  - NEVER closes existing positions.
  - NEVER generates trade signals.

This gate is called AFTER the existing EventBlackoutFilter in the runner.
It can extend the effective blackout window based on live market reaction data.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from shared_lib.persistence.db import DB
from shared_lib.persistence.market_reactions import get_latest_reaction_for_symbol

logger = logging.getLogger(__name__)

# Reaction types that warrant a dynamic blackout extension.
# Keep legacy aliases for historical rows; new Phase 2 rows should use the
# reduced observational taxonomy (VOL_SPIKE / WHIPSAW / REVERSAL).
_ELEVATED_REACTION_TYPES = {
    "VOL_SPIKE",
    "WHIPSAW",
    "REVERSAL",
    "VOLATILITY_SPIKE",
    "SHARP_REVERSAL",
}

# How long after the event time to keep the extension window active (minutes)
_DEFAULT_EXTENSION_MINUTES = 30


@dataclass
class BlockDecision:
    is_blocked: bool
    reason: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


class EventReactionRiskGate:
    """
    Checks whether a post-event volatility state should block new entries.

    Instantiated once at startup; thread-safe for read-only DB access.
    """

    def __init__(
        self,
        db: DB,
        *,
        enabled: bool = False,                    # REACTION_ALLOW_RISK_INFLUENCE
        extend_on_vol_spike: bool = True,          # REACTION_EXTEND_BLACKOUT_ON_VOL_SPIKE
        vol_spike_threshold: float = 2.5,          # REACTION_VOL_SPIKE_THRESHOLD
        extension_minutes: int = _DEFAULT_EXTENSION_MINUTES,
    ):
        self._db = db
        self._enabled = enabled
        self._extend = extend_on_vol_spike
        self._vol_thresh = vol_spike_threshold
        self._ext_minutes = extension_minutes

    def check(self, symbol: str) -> BlockDecision:
        """
        Return a BlockDecision for the given symbol.

        Fast path when disabled: returns (False) immediately.
        """
        if not self._enabled:
            return BlockDecision(is_blocked=False)

        if not self._extend:
            return BlockDecision(is_blocked=False)

        try:
            reaction = get_latest_reaction_for_symbol(self._db, symbol)
        except Exception as exc:
            logger.warning(
                "[ReactionRiskGate] DB read error for symbol=%s: %s", symbol, exc
            )
            return BlockDecision(is_blocked=False)

        if reaction is None:
            return BlockDecision(is_blocked=False)

        return self._evaluate(symbol, reaction)

    def _evaluate(self, symbol: str, reaction: Dict[str, Any]) -> BlockDecision:
        reaction_type = reaction.get("reaction_type", "NO_REACTION")
        if reaction_type not in _ELEVATED_REACTION_TYPES:
            return BlockDecision(is_blocked=False)

        vol_expansion = reaction.get("volatility_expansion_ratio") or 0.0
        if vol_expansion < self._vol_thresh:
            return BlockDecision(is_blocked=False)

        # Only extend within the configured window after event time
        event_time_iso = reaction.get("event_time_utc")
        if event_time_iso:
            try:
                event_dt = datetime.fromisoformat(event_time_iso)
                if event_dt.tzinfo is None:
                    event_dt = event_dt.replace(tzinfo=timezone.utc)
                cutoff = event_dt + timedelta(minutes=self._ext_minutes)
                if datetime.now(timezone.utc) > cutoff:
                    return BlockDecision(is_blocked=False)
            except ValueError:
                pass

        reason = f"POST_EVENT_{reaction_type}_VOLATILITY_EXTENSION"
        details = {
            "reaction_type": reaction_type,
            "volatility_expansion_ratio": vol_expansion,
            "event_time_utc": event_time_iso,
            "symbol_checked": symbol,
        }
        logger.info(
            "[ReactionRiskGate] BLOCKED symbol=%s reason=%s vol_expansion=%.2fx",
            symbol, reason, vol_expansion,
        )
        return BlockDecision(is_blocked=True, reason=reason, details=details)


def build_event_reaction_risk_gate(db: DB) -> EventReactionRiskGate:
    """Factory that wires gate to current settings."""
    from app.core.config import settings

    return EventReactionRiskGate(
        db=db,
        enabled=settings.REACTION_ALLOW_RISK_INFLUENCE,
        extend_on_vol_spike=settings.REACTION_EXTEND_BLACKOUT_ON_VOL_SPIKE,
        vol_spike_threshold=settings.REACTION_VOL_SPIKE_THRESHOLD,
    )
