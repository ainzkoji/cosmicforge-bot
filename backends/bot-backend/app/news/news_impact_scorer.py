"""
News Impact Scorer.

Given a market_event_reactions row (Phase 2), compute:
  - impact_score         : 0.0 → 1.0
  - reaction_latency_category : IMMEDIATE / DELAYED / NO_REACTION

Impact formula (weighted):
  impact_score = clip(
      0.40 * |net_move_pct|   / 3.0   [3% move = full weight]
    + 0.30 * (vol_expansion - 1) / 3.0 [3x expansion = full weight]
    + 0.30 * (vol_spike - 1)  / 4.0   [4x spike = full weight]
  , 0.0, 1.0)

If any component is missing/None, its weight is redistributed equally
to the remaining components.

Latency categories:
  IMMEDIATE : latency_minutes ≤ 5
  DELAYED   : 5 < latency_minutes ≤ 60
  NO_REACTION: reaction_type == NO_REACTION or latency_minutes > 60
"""
from __future__ import annotations

from typing import Dict, Optional, Tuple


# Thresholds for full-weight normalisation
_MOVE_NORM    = 3.0   # 3% net move = 1.0 in that component
_VOL_EXP_NORM = 3.0   # 3× volatility expansion = 1.0
_VOL_SPK_NORM = 4.0   # 4× volume spike = 1.0

# Latency buckets (minutes)
_IMMEDIATE_MAX = 5.0
_DELAYED_MAX   = 60.0

# Minimum impact to avoid pure-noise classification
MIN_IMPACT_THRESHOLD = 0.02


def compute_impact_score(
    reaction_row: Dict,
    latency_minutes: float = 0.0,
) -> Tuple[float, str]:
    """
    Compute impact_score and reaction_latency_category from a Phase 2 row.

    Parameters
    ----------
    reaction_row      : dict from market_event_reactions
    latency_minutes   : float from news_market_linker (news→event delta)

    Returns
    -------
    (impact_score, reaction_latency_category)
    """
    reaction_type = reaction_row.get("reaction_type", "NO_REACTION") or "NO_REACTION"

    # ── 1. Latency category ──────────────────────────────────────────────────
    if reaction_type == "NO_REACTION":
        lat_cat = "NO_REACTION"
    elif abs(latency_minutes) <= _IMMEDIATE_MAX:
        lat_cat = "IMMEDIATE"
    elif abs(latency_minutes) <= _DELAYED_MAX:
        lat_cat = "DELAYED"
    else:
        lat_cat = "NO_REACTION"

    # ── 2. Raw components ────────────────────────────────────────────────────
    net_move   = abs(reaction_row.get("net_move_pct")            or 0.0)
    vol_exp    = reaction_row.get("volatility_expansion_ratio")  or None
    vol_spike  = reaction_row.get("volume_spike_ratio")          or None

    # Compute each normalised component (None = missing)
    components: list[Optional[float]] = [
        min(1.0, net_move / _MOVE_NORM),
        min(1.0, max(0.0, (vol_exp - 1.0) / _VOL_EXP_NORM))  if vol_exp  is not None else None,
        min(1.0, max(0.0, (vol_spike - 1.0) / _VOL_SPK_NORM)) if vol_spike is not None else None,
    ]

    base_weights = [0.40, 0.30, 0.30]

    # Redistribute weights for missing components
    present_mask    = [c is not None for c in components]
    missing_weight  = sum(w for w, p in zip(base_weights, present_mask) if not p)
    present_count   = sum(present_mask)
    bonus           = missing_weight / present_count if present_count else 0.0

    impact_score = 0.0
    for comp, weight, present in zip(components, base_weights, present_mask):
        if present:
            impact_score += comp * (weight + bonus)  # type: ignore[operator]

    impact_score = round(min(1.0, max(0.0, impact_score)), 4)

    # Downgrade latency if impact is too low
    if impact_score < MIN_IMPACT_THRESHOLD:
        lat_cat = "NO_REACTION"

    return impact_score, lat_cat
