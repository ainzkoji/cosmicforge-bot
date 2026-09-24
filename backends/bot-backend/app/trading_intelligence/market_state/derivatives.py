"""Derivatives state (Section 9.9).

All inputs are causal by construction: ``funding_predicted`` must be the
venue's currently-published predicted rate as of ``decision_time``, never a
value the venue only reveals after decision_time. When no derivatives feed
is supplied -- true today, since the shadow integration has none wired --
every field is ``None`` with ``FUNDING_UNAVAILABLE`` / ``OPEN_INTEREST_UNAVAILABLE``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np

from app.trading_intelligence.contracts.data_quality import ReasonCode
from app.trading_intelligence.contracts.market_state import DerivativesState, safe_float
from app.trading_intelligence.market_state.indicators import percentile_rank_of_last


@dataclass(frozen=True)
class DerivativesSnapshotInput:
    """Causal derivatives input. ``as_of`` must be <= decision_time."""

    as_of: int
    funding_current: Optional[float] = None
    funding_predicted: Optional[float] = None
    funding_predicted_as_of: Optional[int] = None
    next_funding_time: Optional[int] = None
    funding_history: tuple = ()
    open_interest: Optional[float] = None
    open_interest_prior: Optional[float] = None
    mark_price: Optional[float] = None
    index_price: Optional[float] = None


def compute_derivatives_state(
    derivatives: Optional[DerivativesSnapshotInput],
    *,
    decision_time: int,
) -> DerivativesState:
    if derivatives is None:
        return DerivativesState(
            funding_current=None,
            funding_predicted=None,
            funding_percentile=None,
            time_to_funding=None,
            open_interest=None,
            open_interest_delta=None,
            basis=None,
            mark_index_spread=None,
            crowding_state=None,
            available=False,
            reason_codes=(ReasonCode.FUNDING_UNAVAILABLE.value, ReasonCode.OPEN_INTEREST_UNAVAILABLE.value),
        )

    if derivatives.as_of > decision_time:
        return DerivativesState(
            funding_current=None,
            funding_predicted=None,
            funding_percentile=None,
            time_to_funding=None,
            open_interest=None,
            open_interest_delta=None,
            basis=None,
            mark_index_spread=None,
            crowding_state=None,
            available=False,
            reason_codes=(ReasonCode.OUT_OF_CAUSAL_BOUNDARY.value,),
        )

    reason_codes = []

    funding_predicted = derivatives.funding_predicted
    if funding_predicted is not None and derivatives.funding_predicted_as_of is not None:
        if derivatives.funding_predicted_as_of > decision_time:
            # The predicted rate was only published after decision_time --
            # using it would be look-ahead. Drop it, keep everything else.
            funding_predicted = None
            reason_codes.append(ReasonCode.OUT_OF_CAUSAL_BOUNDARY.value)

    funding_percentile = None
    if derivatives.funding_current is not None and derivatives.funding_history:
        history = np.array(list(derivatives.funding_history) + [derivatives.funding_current], dtype=float)
        if np.all(np.isfinite(history)):
            funding_percentile = percentile_rank_of_last(history)
    else:
        reason_codes.append(ReasonCode.INSUFFICIENT_HISTORY.value)

    time_to_funding = None
    if derivatives.next_funding_time is not None:
        delta = derivatives.next_funding_time - decision_time
        time_to_funding = int(delta) if delta >= 0 else None

    open_interest_delta = None
    if derivatives.open_interest is not None and derivatives.open_interest_prior is not None:
        open_interest_delta = safe_float(derivatives.open_interest - derivatives.open_interest_prior)

    basis = None
    mark_index_spread = None
    if derivatives.mark_price is not None and derivatives.index_price is not None and derivatives.index_price:
        basis = safe_float((derivatives.mark_price - derivatives.index_price) / derivatives.index_price)
        mark_index_spread = safe_float(derivatives.mark_price - derivatives.index_price)

    crowding_state = None
    if derivatives.funding_current is not None:
        if funding_percentile is not None and funding_percentile >= 0.95:
            crowding_state = "CROWDED_LONG"
        elif funding_percentile is not None and funding_percentile <= 0.05:
            crowding_state = "CROWDED_SHORT"
        else:
            crowding_state = "NEUTRAL"

    return DerivativesState(
        funding_current=safe_float(derivatives.funding_current),
        funding_predicted=safe_float(funding_predicted),
        funding_percentile=funding_percentile,
        time_to_funding=time_to_funding,
        open_interest=safe_float(derivatives.open_interest),
        open_interest_delta=open_interest_delta,
        basis=basis,
        mark_index_spread=mark_index_spread,
        crowding_state=crowding_state,
        available=True,
        reason_codes=tuple(dict.fromkeys(reason_codes)),
    )
