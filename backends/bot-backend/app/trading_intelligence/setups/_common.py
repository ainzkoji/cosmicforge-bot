"""Shared, non-canonical helpers used by more than one Section 11 specialist.

Not part of the public SetupSpecialist interface -- purely to avoid
duplicating the same snapshot-field access and HTF-conflict check four times.
"""
from __future__ import annotations

from typing import Any, Tuple

from app.trading_intelligence.contracts.setup import SetupSide


def htf_conflicts(market_state: Any, side: str) -> bool:
    """True only when HTF is actually available and structurally opposed --
    an unavailable HTF is never treated as a conflict (P7 fail closed means
    'unknown', not 'assume the worst' here, since HTF is optional evidence)."""
    htf = market_state.higher_timeframe_state
    if not htf.available or htf.direction is None:
        return False
    if htf.structure_alignment != "CONFLICT":
        return False
    expected = "UP" if side == SetupSide.LONG.value else "DOWN"
    return htf.direction != expected


def regime_weight(regime_distribution: Any, regime_name: str) -> float:
    return float(regime_distribution.weights.get(regime_name, 0.0))


def liquidity_reason_codes(market_state: Any) -> Tuple[str, ...]:
    from app.trading_intelligence.contracts.setup import SetupReasonCode

    if not market_state.liquidity_state.available:
        return (SetupReasonCode.LIQUIDITY_UNVERIFIED.value,)
    return ()


def snapshot_identity(snapshot: Any) -> dict:
    return {
        "snapshot_id": snapshot.market_snapshot_id,
        "data_hash": snapshot.data_hash,
        "reference_price": float(snapshot.reference_price),
        "timeframe": snapshot.timeframe,
    }


__all__ = ["htf_conflicts", "regime_weight", "liquidity_reason_codes", "snapshot_identity"]
