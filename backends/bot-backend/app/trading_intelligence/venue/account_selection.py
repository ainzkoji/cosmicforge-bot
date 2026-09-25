"""Single-user, multi-broker venue/account selection (Phase 5I).

CATI observes an opportunity once (global intelligence); a user may hold the
same canonical instrument on several connected accounts (Binance, Bybit,
BingX). Selection picks ONE of the user's accounts on AFTER-COST expected
value, using only that account's own capital and wallet topology.

Hard rules:
* only accounts of the SAME user are candidates;
* an account whose execution capability, instrument, credentials or
  permissions are not established is excluded with a reason;
* capital must already be (or become, via a same-account internal transfer)
  available in THAT account -- there is no cross-broker funding path;
* an account needing a physical internal transfer is penalised by the
  transfer's latency/uncertainty and only chosen if still best.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Sequence, Tuple

TRANSFER_PENALTY_BPS = 2.0


@dataclass(frozen=True)
class AccountCandidate:
    user_id: str
    broker_account_id: str
    broker: str
    execution_permitted: bool
    instrument_active: bool
    credential_valid: bool
    permissions_valid: bool
    gross_expectancy_bps: Optional[float]
    fee_bps: Optional[float]
    spread_bps: Optional[float]
    slippage_bps: Optional[float]
    funding_bps: Optional[float]          # expected funding cost over the holding horizon
    capital_outcome: str                  # CapitalPlan.outcome
    exclusion_notes: Tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class AccountSelection:
    selected: Optional[str]
    scores: Dict[str, float]
    excluded: Dict[str, Tuple[str, ...]]


def net_expectancy_bps(c: AccountCandidate) -> Optional[float]:
    parts = (c.gross_expectancy_bps, c.fee_bps, c.spread_bps, c.slippage_bps, c.funding_bps)
    if any(p is None for p in parts):
        return None  # unknown cost is never a zero cost
    net = c.gross_expectancy_bps - c.fee_bps - c.spread_bps / 2.0 - c.slippage_bps - c.funding_bps
    if c.capital_outcome == "PHYSICAL_INTERNAL_TRANSFER_REQUIRED":
        net -= TRANSFER_PENALTY_BPS
    return net


def select_account(user_id: str, candidates: Sequence[AccountCandidate]) -> AccountSelection:
    scores: Dict[str, float] = {}
    excluded: Dict[str, Tuple[str, ...]] = {}
    for c in candidates:
        reasons = []
        if c.user_id != user_id:
            reasons.append("DIFFERENT_USER")
        if not c.execution_permitted:
            reasons.append("EXECUTION_NOT_PERMITTED")
        if not c.instrument_active:
            reasons.append("INSTRUMENT_INACTIVE")
        if not c.credential_valid:
            reasons.append("CREDENTIAL_INVALID")
        if not c.permissions_valid:
            reasons.append("PERMISSIONS_INVALID")
        if c.capital_outcome not in ("NO_ACTION_SHARED_COLLATERAL", "LOGICAL_REALLOCATION",
                                     "PHYSICAL_INTERNAL_TRANSFER_REQUIRED"):
            reasons.append(f"CAPITAL:{c.capital_outcome}")
        net = net_expectancy_bps(c)
        if net is None:
            reasons.append("COSTS_UNKNOWN")
        elif net <= 0:
            reasons.append("NON_POSITIVE_AFTER_COST")
        if reasons:
            excluded[c.broker_account_id] = tuple(reasons)
            continue
        scores[c.broker_account_id] = round(net, 6)
    selected = min(scores, key=lambda k: (-scores[k], k)) if scores else None  # deterministic tie-break
    return AccountSelection(selected=selected, scores=scores, excluded=excluded)


__all__ = ["AccountCandidate", "AccountSelection", "TRANSFER_PENALTY_BPS", "net_expectancy_bps", "select_account"]
