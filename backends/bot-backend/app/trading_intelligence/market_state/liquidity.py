"""Liquidity state (Section 9.8).

Order-book data is optional causal input, never fabricated from OHLCV. When
no book snapshot is supplied -- true today for the shadow integration, which
currently has no book-data feed wired to it -- every field is ``None`` and
``available`` is False with ``TOP_BOOK_UNAVAILABLE``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np

from app.trading_intelligence.contracts.data_quality import ReasonCode
from app.trading_intelligence.contracts.market_state import LiquidityState, safe_float
from app.trading_intelligence.market_state.indicators import percentile_rank_of_last

#: A book snapshot is considered stale if older than this relative to decision_time.
MAX_BOOK_AGE_MS = 5_000


@dataclass(frozen=True)
class BookSnapshotInput:
    """Causal top-of-book / depth input. ``as_of`` must be <= decision_time;
    the caller (integration adapter) is responsible for that causal check --
    this module only consumes what it is given."""

    as_of: int
    best_bid: float
    best_ask: float
    bid_depth: Optional[float] = None
    ask_depth: Optional[float] = None
    reference_price: Optional[float] = None
    spread_history_bps: Tuple[float, ...] = ()


def compute_liquidity_state(
    book: Optional[BookSnapshotInput],
    *,
    decision_time: int,
) -> LiquidityState:
    if book is None:
        return LiquidityState(
            spread_bps=None,
            spread_percentile=None,
            top_book_depth=None,
            depth_imbalance=None,
            estimated_slippage=None,
            stale_book=False,
            available=False,
            reason_codes=(ReasonCode.TOP_BOOK_UNAVAILABLE.value,),
        )

    if book.as_of > decision_time:
        return LiquidityState(
            spread_bps=None,
            spread_percentile=None,
            top_book_depth=None,
            depth_imbalance=None,
            estimated_slippage=None,
            stale_book=False,
            available=False,
            reason_codes=(ReasonCode.OUT_OF_CAUSAL_BOUNDARY.value,),
        )

    stale = (decision_time - book.as_of) > MAX_BOOK_AGE_MS
    reason_codes = []
    if stale:
        # Non-critical: a stale optional book feed degrades liquidity
        # evidence but must never invalidate the whole MarketState the way
        # stale *primary* candles would.
        reason_codes.append(ReasonCode.TOP_BOOK_STALE.value)

    mid = (book.best_bid + book.best_ask) / 2.0 if (book.best_bid and book.best_ask) else None
    spread_bps = None
    if mid and mid > 0 and book.best_ask >= book.best_bid:
        spread_bps = safe_float((book.best_ask - book.best_bid) / mid * 10_000)

    spread_percentile = None
    if spread_bps is not None and book.spread_history_bps:
        history = np.array(list(book.spread_history_bps) + [spread_bps], dtype=float)
        spread_percentile = percentile_rank_of_last(history)

    top_book_depth = None
    depth_imbalance = None
    if book.bid_depth is not None and book.ask_depth is not None:
        total = book.bid_depth + book.ask_depth
        top_book_depth = safe_float(total)
        if total > 0:
            depth_imbalance = safe_float((book.bid_depth - book.ask_depth) / total)

    estimated_slippage = None
    if spread_bps is not None:
        # V1 proxy: half-spread is the minimum expected slippage for a
        # size-agnostic marketable order. A size-adjusted model belongs to a
        # later economic-admission section, not this shared market layer.
        estimated_slippage = safe_float(spread_bps / 2.0)

    return LiquidityState(
        spread_bps=spread_bps,
        spread_percentile=spread_percentile,
        top_book_depth=top_book_depth,
        depth_imbalance=depth_imbalance,
        estimated_slippage=estimated_slippage,
        stale_book=stale,
        available=True,
        reason_codes=tuple(dict.fromkeys(reason_codes)),
    )
