"""FX-perpetual economic overlay (Phase 5G).

An exchange-listed FX perpetual has costs the OTC-based Section 17 FX model
does not see: the venue's price can sit away from the reference FX market
(divergence), and venue liquidity collapses outside the main FX sessions.

This overlay is ADDITIVE and separately versioned: it does not modify the
Section 17 cost model or ``VenueCostPolicy`` (whose hash is part of the
frozen Section 22 certification freeze). Folding it into the certified cost
model is a governed policy-version change, not something a code merge does.

All values are in R (multiples of the candidate's structural risk). A
missing input is a conservative fallback flagged in ``reasons`` -- never 0.

    total_extra_R = divergence_R + session_liquidity_R
    divergence_R  = |divergence_bps| / 1e4 * entry / risk   (entry pays it,
                    the exit is assumed to converge -- charged once)
    session_R     = (multiplier(session) - 1) * spread_R    (thin sessions
                    widen the effective spread)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Optional, Tuple

FX_PERP_OVERLAY_VERSION = "fx-perp-overlay-v1"
SESSION_SPREAD_MULTIPLIER: Mapping[str, float] = {
    "LONDON_NY_OVERLAP": 1.0, "LONDON": 1.1, "NEW_YORK": 1.2, "ASIA": 1.6, "CLOSED": float("inf"),
}
FALLBACK_DIVERGENCE_BPS = 15.0


@dataclass(frozen=True)
class FxPerpOverlay:
    divergence_R: float
    session_liquidity_R: float
    total_extra_R: float
    tradable: bool
    reasons: Tuple[str, ...] = ()
    version: str = FX_PERP_OVERLAY_VERSION


def fx_perp_overlay(*, fx_context, entry: float, risk: float, spread_R: float,
                    max_divergence_bps: float = 50.0) -> FxPerpOverlay:
    """``fx_context``: an ``FXMarketContext`` at the candidate's decision time."""
    reasons = []
    if risk <= 0 or entry <= 0:
        return FxPerpOverlay(0.0, 0.0, 0.0, False, ("INVALID_ENTRY_OR_RISK",))
    if not fx_context.market_open:
        return FxPerpOverlay(0.0, 0.0, 0.0, False, ("FX_MARKET_CLOSED",))
    div = fx_context.divergence_bps
    if div is None:
        div = FALLBACK_DIVERGENCE_BPS
        reasons.append("DIVERGENCE_FALLBACK_USED:" + fx_context.unavailable.get("divergence", "UNKNOWN"))
    if abs(div) > max_divergence_bps:
        return FxPerpOverlay(0.0, 0.0, 0.0, False, (f"VENUE_REFERENCE_DIVERGENCE_{abs(div):.1f}BPS",))
    div_r = abs(div) / 1e4 * entry / risk
    mult = SESSION_SPREAD_MULTIPLIER.get(fx_context.session, 2.0)
    if fx_context.session not in SESSION_SPREAD_MULTIPLIER:
        reasons.append("SESSION_UNKNOWN_FALLBACK")
    session_r = max(0.0, mult - 1.0) * max(spread_R, 0.0)
    return FxPerpOverlay(div_r, session_r, div_r + session_r, True, tuple(reasons))


def after_cost_expectancy_R(gross_expectancy_R: float, base_total_cost_R: float, overlay: FxPerpOverlay) -> Optional[float]:
    """Ranking input: expectancy AFTER the Section 17 costs AND the overlay;
    None when the overlay says the instrument is not tradable now."""
    if not overlay.tradable:
        return None
    return gross_expectancy_R - base_total_cost_R - overlay.total_extra_R


__all__ = ["FALLBACK_DIVERGENCE_BPS", "FX_PERP_OVERLAY_VERSION", "FxPerpOverlay", "SESSION_SPREAD_MULTIPLIER",
           "after_cost_expectancy_R", "fx_perp_overlay"]
