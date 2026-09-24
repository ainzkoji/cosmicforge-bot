"""Causal PositionPathSnapshot construction (Section 19.2).

P1 CAUSALITY: only data timestamped at or before ``current_time`` is used --
candles whose close is after ``current_time`` and fills after it are dropped
before anything is computed, so appending future data can never change a
snapshot (tested). Excursions are measured from bars that OPENED at or after
entry (the entry bar's pre-entry range is never counted) plus the current
price.

The PositionManager adapter (``view_from_position_state``) only READS the
lifecycle state; it never calls a PositionManager method that mutates.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from typing import Any, Iterable, Optional, Sequence, Tuple

from app.trading_intelligence.contracts.position import PositionPathSnapshot, ProtectionState
from app.trading_intelligence.contracts.setup import timeframe_to_ms
from app.trading_intelligence.contracts.trade_plan import TradePlan


class FillKind:
    OPEN = "OPEN"
    REDUCE = "REDUCE"
    CLOSE = "CLOSE"
    FUNDING = "FUNDING"
    FINANCING = "FINANCING"


@dataclass(frozen=True)
class FillEvent:
    """Broker-authoritative fill/charge record. ``amount`` is a cash charge
    (positive = paid) for FUNDING / FINANCING rows."""

    time: int
    kind: str
    qty: float = 0.0
    price: float = 0.0
    fee: float = 0.0
    amount: float = 0.0


@dataclass(frozen=True)
class PositionLifecycleView:
    """Read-only view of the canonical position lifecycle (``positions`` row +
    PositionManager state + broker fills). CATI never writes any of it."""

    position_id: str
    trade_plan_id: Optional[str]
    side: str
    entry_time: int
    entry_price: float
    original_quantity: float
    current_quantity: float
    realized_pnl: float = 0.0
    partial_exit_count: int = 0
    current_stop_price: Optional[float] = None
    current_target_prices: Tuple[float, ...] = ()
    protection_state: str = ProtectionState.UNKNOWN.value
    funding_paid_or_accrued: float = 0.0
    financing_paid_or_accrued: float = 0.0
    entry_fees_paid: float = 0.0
    last_fill_time: Optional[int] = None
    fills: Tuple[FillEvent, ...] = ()


def _row(r: Any) -> Tuple[int, float, float, int]:
    """(open_time, high, low, close_time) from a Binance list row or a dict row."""
    if isinstance(r, dict):
        g = lambda *k: next(r[x] for x in k if x in r)  # noqa: E731
        return int(g("openTime", "open_time")), float(g("high")), float(g("low")), int(g("closeTime", "close_time"))
    return int(r[0]), float(r[2]), float(r[3]), int(r[6])


def causal_bars(rows: Iterable[Any], entry_time: int, current_time: int) -> list:
    """Closed bars that opened at/after entry and closed at/before now."""
    out = []
    for r in rows or ():
        open_t, high, low, close_t = _row(r)
        if open_t >= entry_time and close_t <= current_time:
            out.append((open_t, high, low, close_t))
    return sorted(out)


def _fold_fills(view: PositionLifecycleView, current_time: int, multiplier: float):
    """Quantities / PnL / charges from causal fills (None when no fills given)."""
    fills = [f for f in view.fills if f.time <= current_time]
    if not fills:
        return None
    sign = 1.0 if view.side == "LONG" else -1.0
    opens = [f for f in fills if f.kind == FillKind.OPEN]
    original = sum(f.qty for f in opens) or view.original_quantity
    entry = (sum(f.qty * f.price for f in opens) / sum(f.qty for f in opens)) if opens and sum(
        f.qty for f in opens) > 0 else view.entry_price
    exits = [f for f in fills if f.kind in (FillKind.REDUCE, FillKind.CLOSE)]
    realized_qty = sum(f.qty for f in exits)
    realized = sum(sign * (f.price - entry) * f.qty * multiplier for f in exits) - sum(f.fee for f in exits)
    return dict(
        entry_price=entry, original_quantity=original, realized_quantity=realized_qty,
        current_quantity=max(0.0, original - realized_qty),
        partial_exit_count=sum(1 for f in fills if f.kind == FillKind.REDUCE),
        realized_pnl=realized,
        entry_fees_paid=sum(f.fee for f in opens),
        funding_paid_or_accrued=sum(f.amount for f in fills if f.kind == FillKind.FUNDING),
        financing_paid_or_accrued=sum(f.amount for f in fills if f.kind == FillKind.FINANCING),
        last_fill_time=max(f.time for f in fills if f.kind in (FillKind.OPEN, FillKind.REDUCE, FillKind.CLOSE))
        if any(f.kind in (FillKind.OPEN, FillKind.REDUCE, FillKind.CLOSE) for f in fills) else None,
    )


def build_position_path_snapshot(
    *,
    plan: TradePlan,
    position: PositionLifecycleView,
    candle_rows: Sequence[Any],
    current_time: int,
    current_price: float,
    timeframe: Optional[str] = None,
) -> PositionPathSnapshot:
    if position.trade_plan_id != plan.trade_plan_id:
        raise ValueError("position is not linked to this TradePlan (no CATI lineage)")
    if position.side != plan.side:
        raise ValueError("position side does not match its TradePlan")
    if current_time < position.entry_time:
        raise ValueError("current_time precedes entry (non-causal)")
    mult = float(getattr(plan.instrument_key, "contract_multiplier", 1.0) or 1.0)
    folded = _fold_fills(position, current_time, mult)
    q = dict(
        entry_price=position.entry_price, original_quantity=position.original_quantity,
        current_quantity=position.current_quantity,
        realized_quantity=max(0.0, position.original_quantity - position.current_quantity),
        partial_exit_count=position.partial_exit_count, realized_pnl=position.realized_pnl,
        entry_fees_paid=position.entry_fees_paid, funding_paid_or_accrued=position.funding_paid_or_accrued,
        financing_paid_or_accrued=position.financing_paid_or_accrued, last_fill_time=position.last_fill_time,
    )
    if folded is not None:
        q.update(folded)
    if q["last_fill_time"] is not None and q["last_fill_time"] > current_time:
        q["last_fill_time"] = None

    entry, price = float(q["entry_price"]), float(current_price)
    R = float(plan.initial_risk_distance)
    long_side = plan.side == "LONG"
    sign = 1.0 if long_side else -1.0
    bars = causal_bars(candle_rows, position.entry_time, current_time)
    highs = [b[1] for b in bars] + [entry, price]
    lows = [b[2] for b in bars] + [entry, price]
    mfe_price = max(highs) if long_side else min(lows)
    mae_price = min(lows) if long_side else max(highs)
    mfe_R = max(0.0, sign * (mfe_price - entry) / R)
    mae_R = max(0.0, sign * (entry - mae_price) / R)
    current_R = sign * (price - entry) / R

    stop = position.current_stop_price
    remaining = None
    if stop is not None:
        d = sign * (price - float(stop))
        remaining = d if d > 0 else 0.0
    tf = timeframe or getattr(plan, "timeframe", None) or "15m"
    return PositionPathSnapshot.build(
        user_id=plan.user_id, broker_account_id=plan.broker_account_id, bot_instance_id=plan.bot_instance_id,
        position_id=position.position_id, trade_plan_id=plan.trade_plan_id,
        instrument_key=plan.instrument_key, venue=plan.venue, environment=plan.environment, side=plan.side,
        entry_time=int(position.entry_time), entry_price=entry, current_time=int(current_time), current_price=price,
        original_quantity=float(q["original_quantity"]), current_quantity=float(q["current_quantity"]),
        realized_quantity=float(q["realized_quantity"]), partial_exit_count=int(q["partial_exit_count"]),
        realized_pnl=float(q["realized_pnl"]),
        unrealized_pnl=sign * (price - entry) * float(q["current_quantity"]) * mult,
        mfe_price=float(mfe_price), mae_price=float(mae_price), mfe_R=mfe_R, mae_R=mae_R, current_R=current_R,
        elapsed_bars=len(bars), elapsed_seconds=int((current_time - position.entry_time) // 1000),
        current_stop_price=None if stop is None else float(stop),
        current_target_prices=tuple(float(t) for t in position.current_target_prices),
        protection_state=position.protection_state,
        funding_paid_or_accrued=float(q["funding_paid_or_accrued"]),
        financing_paid_or_accrued=float(q["financing_paid_or_accrued"]),
        entry_fees_paid=float(q["entry_fees_paid"]), last_fill_time=q["last_fill_time"],
        original_R_reference=R, remaining_risk_distance=remaining, timeframe=str(tf), contract_multiplier=mult,
    )


def bar_ms(timeframe: str) -> int:
    return timeframe_to_ms(timeframe) or 900_000


def view_from_position_state(state: Any, *, trade_plan_id: Optional[str], entry_time_ms: Optional[int] = None,
                             position_row: Optional[dict] = None) -> PositionLifecycleView:
    """Read-only adapter from ``app.execution.position_manager.PositionState``
    (and optionally the canonical ``positions`` row). Mutates nothing."""
    side = getattr(getattr(state, "side", None), "value", str(getattr(state, "side", "")))
    if entry_time_ms is None:
        et = getattr(state, "entry_time", None)
        if et is not None and et.tzinfo is None:  # PositionManager stores naive UTC
            et = et.replace(tzinfo=timezone.utc)
        entry_time_ms = int(et.timestamp() * 1000) if et is not None else 0
    sl = getattr(state, "sl", None)
    tp = getattr(state, "tp", None)
    if sl is not None and getattr(sl, "sl_order_id", None):
        protection = ProtectionState.PROTECTED.value
    elif getattr(state, "reconciliation_status", "") in ("UNPROTECTED",):
        protection = ProtectionState.UNPROTECTED.value
    else:
        protection = ProtectionState.UNKNOWN.value
    targets = tuple(t for t in ((getattr(tp, "tp1_price", None) if tp and not getattr(tp, "tp1_hit", False) else None),
                                getattr(tp, "tp2_price", None) if tp else None) if t)
    original = float((position_row or {}).get("original_qty") or getattr(state, "entry_qty", 0.0))
    current = float((position_row or {}).get("remaining_qty") or getattr(state, "current_qty", 0.0))
    return PositionLifecycleView(
        position_id=str(getattr(state, "position_id", None) or (position_row or {}).get("position_id") or ""),
        trade_plan_id=trade_plan_id, side=side, entry_time=int(entry_time_ms),
        entry_price=float((position_row or {}).get("entry_price") or getattr(state, "entry_price", 0.0)),
        original_quantity=original, current_quantity=current,
        realized_pnl=float((position_row or {}).get("realized_pnl") or 0.0),
        partial_exit_count=1 if (tp is not None and getattr(tp, "tp1_hit", False)) else 0,
        current_stop_price=float(sl.current_stop) if sl is not None and getattr(sl, "current_stop", None) else None,
        current_target_prices=targets, protection_state=protection,
        entry_fees_paid=float((position_row or {}).get("fees") or 0.0),
    )


__all__ = ["FillKind", "FillEvent", "PositionLifecycleView", "causal_bars", "build_position_path_snapshot", "bar_ms",
           "view_from_position_state"]
