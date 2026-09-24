"""Chronological certification splits, purge/embargo and holdout protection
(Sections 22.5-22.6).

Never random, never shuffled. The layout of one certification dataset::

    [warmup][fold 0 | fold 1 | ... | fold K-1][purge][ HOLDOUT ][label tail]
             seed    <---- walk-forward evaluation ---->  reserved

* The HOLDOUT is the chronological tail, reserved BEFORE any inspection
  (``holdout_fraction`` of the decision span, the existing research-contract
  remainder). Nothing before it may read it (``guard_holdout``).
* Walk-forward: fold ``k`` is evaluated with a forecast library built ONLY
  from folds ``< k``. A library row is admitted only if its FUTURE-LABEL
  window (decision_time + label horizon) ends before the fold starts minus
  the embargo -- so a label that uses future bars can never leak across a
  boundary (purge), and the embargo adds a quiet gap after it.
* The purge width derives from the label horizon (and the higher timeframe:
  a closed HTF bar spans ``htf_ms``, so the purge is at least one HTF bar).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, List, Optional, Sequence, Tuple, TypeVar

from app.research.dataset import FinalHoldoutViolation

T = TypeVar("T")

#: RESEARCH_DEFAULT walk-forward fold count (versioned via the chronology hash)
DEFAULT_WALK_FORWARD_FOLDS = 6


@dataclass(frozen=True)
class Window:
    name: str
    start_ms: int
    end_ms: int

    def contains(self, ts: int) -> bool:
        return self.start_ms <= int(ts) <= self.end_ms

    @property
    def days(self) -> float:
        return max(0.0, (self.end_ms - self.start_ms + 1) / 86_400_000)

    def to_dict(self) -> dict:
        return {"name": self.name, "start_ms": self.start_ms, "end_ms": self.end_ms, "days": round(self.days, 4)}


@dataclass(frozen=True)
class ChronologyPlan:
    decision_start_ms: int
    decision_end_ms: int
    bar_ms: int
    label_horizon_ms: int
    purge_ms: int
    embargo_ms: int
    folds: Tuple[Window, ...]
    holdout: Window

    @property
    def evaluation_folds(self) -> Tuple[Window, ...]:
        return self.folds[1:]

    @property
    def evaluation_window(self) -> Optional[Window]:
        ev = self.evaluation_folds
        return Window("EVALUATION", ev[0].start_ms, ev[-1].end_ms) if ev else None

    def fold_for(self, ts: int) -> Optional[Window]:
        return next((f for f in self.folds if f.contains(ts)), None)

    def library_cutoff_for(self, fold: Window) -> int:
        """A training row is usable for ``fold`` only if its label window
        ends strictly before this instant."""
        return fold.start_ms - self.embargo_ms

    def to_dict(self) -> dict:
        return {"decision_start_ms": self.decision_start_ms, "decision_end_ms": self.decision_end_ms,
                "bar_ms": self.bar_ms, "label_horizon_ms": self.label_horizon_ms, "purge_ms": self.purge_ms,
                "embargo_ms": self.embargo_ms, "folds": [f.to_dict() for f in self.folds],
                "holdout": self.holdout.to_dict(), "method": "CHRONOLOGICAL_WALK_FORWARD_NO_SHUFFLE"}


def plan_chronology(*, decision_start_ms: int, decision_end_ms: int, bar_ms: int, label_horizon_bars: int,
                    holdout_fraction: float, embargo_bars: int = 0, htf_ms: Optional[int] = None,
                    folds: int = DEFAULT_WALK_FORWARD_FOLDS) -> ChronologyPlan:
    if decision_end_ms <= decision_start_ms:
        raise ValueError("empty decision span")
    if not 0.0 < holdout_fraction < 1.0:
        raise ValueError("holdout fraction must be in (0, 1)")
    if folds < 2:
        raise ValueError("walk-forward needs a seed fold and at least one evaluated fold")
    label_ms = int(label_horizon_bars) * int(bar_ms)
    purge_ms = max(label_ms, int(htf_ms or 0))
    embargo_ms = int(embargo_bars) * int(bar_ms)
    span = decision_end_ms - decision_start_ms + 1
    holdout_start = decision_start_ms + int(span * (1.0 - holdout_fraction))
    holdout_start -= (holdout_start - decision_start_ms) % bar_ms  # bar-aligned
    holdout = Window("HOLDOUT", holdout_start, decision_end_ms)
    research_end = holdout_start - purge_ms - embargo_ms - 1  # label windows end before the holdout
    if research_end <= decision_start_ms:
        raise ValueError("no research span before the holdout")
    width = (research_end - decision_start_ms + 1) // folds
    width -= width % bar_ms
    if width < bar_ms:
        raise ValueError("research span too short for the walk-forward folds")
    windows: List[Window] = []
    for k in range(folds):
        start = decision_start_ms + k * width
        end = research_end if k == folds - 1 else start + width - 1
        windows.append(Window(f"FOLD_{k}", start, end))
    return ChronologyPlan(decision_start_ms, decision_end_ms, int(bar_ms), label_ms, purge_ms, embargo_ms,
                          tuple(windows), holdout)


def purge_for_window(items: Iterable[T], *, decision_time: Callable[[T], int], horizon_ms: int,
                     cutoff_ms: int) -> Tuple[T, ...]:
    """Keep only items whose FUTURE-LABEL window ends before ``cutoff_ms``.
    An item decided before the boundary whose label reads bars after it is
    removed -- that is exactly the leak the purge exists to prevent."""
    return tuple(i for i in items if int(decision_time(i)) + int(horizon_ms) < int(cutoff_ms))


def guard_holdout(plan: ChronologyPlan, ts: int, *, purpose: str, allowed: bool = False) -> None:
    """Raise if a pre-holdout process reads the reserved holdout."""
    if plan.holdout.contains(ts) and not allowed:
        raise FinalHoldoutViolation(
            f"{purpose} attempted to read {ts}, inside the reserved HOLDOUT "
            f"({plan.holdout.start_ms} -> {plan.holdout.end_ms})")


def assert_chronological(items: Sequence[Any], key: Callable[[Any], int]) -> None:
    """Certification refuses shuffled inputs outright."""
    last = None
    for item in items:
        t = int(key(item))
        if last is not None and t < last:
            raise ValueError("certification inputs must be in chronological order (no shuffle)")
        last = t


__all__ = ["ChronologyPlan", "Window", "plan_chronology", "purge_for_window", "guard_holdout",
           "assert_chronological", "DEFAULT_WALK_FORWARD_FOLDS", "FinalHoldoutViolation"]
