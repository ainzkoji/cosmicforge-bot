"""Fill semantics for replay (§13.5) and the intrabar ambiguity rule (§13.7).

A backtest that assumes a perfect same-bar fill is not modelling execution, it
is assuming the answer. So every fill in a replay goes through an explicit,
named model, and the model that produced each fill is recorded with it.

The dangerous case is §13.7: a bar whose high reaches the target *and* whose
low reaches the stop. OHLC alone cannot say which came first. Silently choosing
the profitable one is how a losing strategy backtests well, so the policy is
explicit, defaults to the pessimistic answer, and is persisted with the result.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping


class FillModel(str, Enum):
    """How an order becomes a fill.

    ``NEXT_BAR_OPEN`` is the honest default for a signal generated on a closed
    bar: the earliest price actually reachable after the decision.
    """

    NEXT_BAR_OPEN = "next_bar_open"
    NEXT_BAR_MARKET = "next_bar_market"
    BAR_TOUCH_LIMIT = "bar_touch_limit"
    BAR_TOUCH_STOP = "bar_touch_stop"


class IntrabarPolicy(str, Enum):
    """Which side wins when one bar touches both the stop and the target.

    ``CONSERVATIVE_STOP_FIRST`` is the default because it cannot flatter a
    strategy. ``LOWER_TIMEFRAME_RECONSTRUCTION`` resolves the order from finer
    data and is the only one that is actually *correct*; it requires that data
    to be present, and says so rather than guessing when it is not.
    """

    CONSERVATIVE_STOP_FIRST = "conservative_stop_first"
    OPTIMISTIC_TARGET_FIRST = "optimistic_target_first"
    LOWER_TIMEFRAME_RECONSTRUCTION = "lower_timeframe_reconstruction"


class IntrabarUnresolved(RuntimeError):
    """Lower-timeframe reconstruction was asked for and the data is missing."""


def _f(row: Any, index: int, *keys: str) -> float:
    if isinstance(row, Mapping):
        for key in keys:
            if key in row:
                return float(row[key])
        raise KeyError(f"candle mapping has none of {keys}")
    return float(row[index])


def bar_open(row: Any) -> float:
    return _f(row, 1, "open", "o")


def bar_high(row: Any) -> float:
    return _f(row, 2, "high", "h")


def bar_low(row: Any) -> float:
    return _f(row, 3, "low", "l")


def bar_close(row: Any) -> float:
    return _f(row, 4, "close", "c")


def bar_open_time(row: Any) -> int:
    if isinstance(row, Mapping):
        for key in ("openTime", "open_time"):
            if key in row:
                return int(row[key])
        raise KeyError("candle mapping has no open time")
    return int(row[0])


def bar_close_time(row: Any) -> int:
    if isinstance(row, Mapping):
        for key in ("closeTime", "close_time"):
            if key in row:
                return int(row[key])
        raise KeyError("candle mapping has no close time")
    return int(row[6])


@dataclass(frozen=True)
class Fill:
    """One simulated fill and the reason it happened at that price."""

    filled: bool
    price: float | None
    at_ms: int | None
    model: FillModel | None
    reason: str
    intrabar_policy: IntrabarPolicy | None = None

    @classmethod
    def none(cls, reason: str) -> "Fill":
        return cls(filled=False, price=None, at_ms=None, model=None, reason=reason)


# ── Entry models ────────────────────────────────────────────────────────────


def fill_entry(bar: Any, model: FillModel) -> Fill:
    """Fill an entry decided on the *previous* closed bar.

    ``bar`` is the bar after the decision. Both entry models resolve to a price
    on that bar, never on the bar the decision was made from -- filling at the
    close of the bar you decided on is look-ahead.
    """
    if model is FillModel.NEXT_BAR_OPEN:
        return Fill(
            filled=True, price=bar_open(bar), at_ms=bar_open_time(bar),
            model=model, reason="filled at the open of the bar after the signal",
        )
    if model is FillModel.NEXT_BAR_MARKET:
        # A market order sent at the open, filling somewhere in the bar. The
        # midpoint of open..close is a modelling choice, and it is named as one.
        price = (bar_open(bar) + bar_close(bar)) / 2.0
        return Fill(
            filled=True, price=price, at_ms=bar_open_time(bar), model=model,
            reason="market order modelled at the open/close midpoint",
        )
    raise ValueError(f"{model} is not an entry model")


def fill_limit(bar: Any, limit_price: float, side: str) -> Fill:
    """A resting limit order fills only if the bar trades through it."""
    side = side.upper()
    if side in {"BUY", "LONG"}:
        touched = bar_low(bar) <= limit_price
    else:
        touched = bar_high(bar) >= limit_price
    if not touched:
        return Fill.none("limit price not touched by this bar")
    return Fill(
        filled=True, price=limit_price, at_ms=bar_close_time(bar),
        model=FillModel.BAR_TOUCH_LIMIT, reason="bar traded through the limit",
    )


def fill_stop(bar: Any, stop_price: float, side: str) -> Fill:
    """A stop fills at the stop price once touched.

    No slippage is added here: slippage is the cost model's job, and applying
    it in two places is how a replay quietly double-charges.
    """
    side = side.upper()
    if side in {"BUY", "LONG"}:
        touched = bar_low(bar) <= stop_price
    else:
        touched = bar_high(bar) >= stop_price
    if not touched:
        return Fill.none("stop not touched by this bar")
    return Fill(
        filled=True, price=stop_price, at_ms=bar_close_time(bar),
        model=FillModel.BAR_TOUCH_STOP, reason="bar traded through the stop",
    )


# ── §13.7 the ambiguous bar ─────────────────────────────────────────────────


def resolve_exit(
    bar: Any,
    *,
    side: str,
    stop_price: float | None,
    target_price: float | None,
    policy: IntrabarPolicy = IntrabarPolicy.CONSERVATIVE_STOP_FIRST,
    lower_timeframe_bars: list[Any] | None = None,
) -> Fill:
    """Decide which of stop and target a single bar hit, and record how.

    When only one is touched there is no ambiguity and the policy is irrelevant
    but still recorded, so every fill in the run carries the rule that produced
    it.
    """
    side = side.upper()
    hit_stop = stop_price is not None and fill_stop(bar, stop_price, side).filled
    hit_target = target_price is not None and _target_touched(bar, target_price, side)

    if not hit_stop and not hit_target:
        return Fill.none("neither stop nor target touched")

    if hit_stop and not hit_target:
        return _stop_fill(bar, stop_price, policy, "only the stop was touched")
    if hit_target and not hit_stop:
        return _target_fill(bar, target_price, policy, "only the target was touched")

    # Both. OHLC cannot say which came first.
    if policy is IntrabarPolicy.LOWER_TIMEFRAME_RECONSTRUCTION:
        return _reconstruct(
            bar, side=side, stop_price=stop_price, target_price=target_price,
            lower_timeframe_bars=lower_timeframe_bars,
        )
    if policy is IntrabarPolicy.OPTIMISTIC_TARGET_FIRST:
        return _target_fill(
            bar, target_price, policy,
            "both touched; policy resolves in favour of the target",
        )
    return _stop_fill(
        bar, stop_price, policy,
        "both touched; resolved conservatively to the stop",
    )


def _target_touched(bar: Any, target_price: float, side: str) -> bool:
    if side in {"BUY", "LONG"}:
        return bar_high(bar) >= target_price
    return bar_low(bar) <= target_price


def _stop_fill(bar, stop_price, policy, reason) -> Fill:
    return Fill(
        filled=True, price=float(stop_price), at_ms=bar_close_time(bar),
        model=FillModel.BAR_TOUCH_STOP, reason=reason, intrabar_policy=policy,
    )


def _target_fill(bar, target_price, policy, reason) -> Fill:
    return Fill(
        filled=True, price=float(target_price), at_ms=bar_close_time(bar),
        model=FillModel.BAR_TOUCH_LIMIT, reason=reason, intrabar_policy=policy,
    )


def _reconstruct(bar, *, side, stop_price, target_price, lower_timeframe_bars) -> Fill:
    """Walk finer bars in order and take whichever level is reached first."""
    if not lower_timeframe_bars:
        raise IntrabarUnresolved(
            "LOWER_TIMEFRAME_RECONSTRUCTION was requested for an ambiguous bar "
            "but no lower-timeframe data was supplied. Supply it, or choose an "
            "explicit policy -- guessing the order is what this rule exists to "
            "prevent."
        )
    window_open, window_close = bar_open_time(bar), bar_close_time(bar)
    for fine in lower_timeframe_bars:
        if not (window_open <= bar_open_time(fine) <= window_close):
            continue
        fine_stop = stop_price is not None and fill_stop(fine, stop_price, side).filled
        fine_target = target_price is not None and _target_touched(fine, target_price, side)
        if fine_stop and fine_target:
            # Still ambiguous at this resolution: stay conservative and say so.
            return _stop_fill(
                fine, stop_price, IntrabarPolicy.LOWER_TIMEFRAME_RECONSTRUCTION,
                "still ambiguous at the lower timeframe; resolved to the stop",
            )
        if fine_stop:
            return _stop_fill(
                fine, stop_price, IntrabarPolicy.LOWER_TIMEFRAME_RECONSTRUCTION,
                "lower-timeframe reconstruction: the stop was reached first",
            )
        if fine_target:
            return _target_fill(
                fine, target_price, IntrabarPolicy.LOWER_TIMEFRAME_RECONSTRUCTION,
                "lower-timeframe reconstruction: the target was reached first",
            )
    raise IntrabarUnresolved(
        "the lower-timeframe bars supplied do not cover this bar's window, so "
        "the order of the stop and the target is still unknown"
    )
