"""Position management as family-neutral intents (Section H, Step 3.9).

The position-management package (``trading_intelligence.position``) answers "is this position still worth
holding?" with an ``ExitDecision``: EXIT, REDUCE, TAKE_PARTIAL, TIGHTEN_PROTECTION or HOLD. It is advisory and
executes nothing; its only route to a broker was ``CATIExecutionBoundary.process_exit_decision``, which replaces a
stop by cancel-then-create. This module gives it (and the daily-trend book's stop ratchet) the other route: it
TRANSLATES what was decided into the intents of ``pipeline.contracts``, which the execution boundary
(``pipeline.execution``) authorizes and executes like any other intent of a decision.

==========================  =====================================================================================
what was decided            the intent
==========================  =====================================================================================
exit signal                 ``ExitIntent`` (the whole position, reduce-only)
reduction                   ``ReduceIntent`` by fraction
partial exit (take profit)  ``ReduceIntent`` by fraction, reason ``TAKE_PARTIAL``
break-even transition       ``StopReplaceIntent`` reason ``BREAK_EVEN`` (never below the entry of a long)
trailing stop               ``StopReplaceIntent`` reason ``TRAIL`` (the daily-trend ratchet: ``max(stop, new)``)
stop tightening             ``StopReplaceIntent`` reason ``TIGHTEN``
position scaling            ``RebalanceIntent`` to a target quantity (up or down), ``ExitIntent`` for target zero
post-fill recalculation     done by the boundary after every fill; ``resize_intent`` states it explicitly
==========================  =====================================================================================

Nothing here widens a stop: the contracts refuse it when the intent is built, the portfolio layer refuses it
against the resting stop, and the protection layer refuses it against what the venue reports. Nothing here sends
anything: every function is pure except ``record_decision``, which stores the immutable decision record the
boundary's family gate verifies a batch against. An intent that is not part of a recorded decision of a
registered, certified, promoted family is refused by that gate like any other.

The package ``trading_intelligence.position`` is not modified, and the operator emergency flatten
(``integration.production_execution.flatten_account``) is a separate path that this module does not touch.
"""
from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from app.trading_intelligence.contracts.position import ExitAction, ExitDecision, PositionReasonCode
from app.trading_intelligence.pipeline.contracts import (
    BREAK_EVEN, LONG, RESIZE, TIGHTEN, TRAIL, AccountRef, DecisionProvenance, ExitIntent, Intent, IntentBatch,
    PortfolioConstraints, RebalanceIntent, ReduceIntent, StopReplaceIntent, to_plain,
)
from app.trading_intelligence.pipeline.family_registry import FamilyDefinition
from app.trading_intelligence.pipeline.portfolio import AccountSnapshot, PortfolioPosition, rebalance_required
from app.trading_intelligence.pipeline.provenance import DecisionStore

#: reason an exit carries when the decision names none
EXIT_SIGNAL = "EXIT_SIGNAL"
TAKE_PARTIAL = "TAKE_PARTIAL"
POSITION_SCALED_TO_ZERO = "POSITION_SCALED_TO_ZERO"


class TranslationError(ValueError):
    """The decision cannot be expressed as an intent for this position. ``code`` is stable."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(f"{code}: {message}" if message else code)
        self.code = code


def _wider(direction: str, new_stop: float, current_stop: float) -> bool:
    return new_stop < current_stop if direction == LONG else new_stop > current_stop


# ----------------------------------------------------------------------------------------------- ExitDecision
def intents_from_exit_decision(decision: ExitDecision, *, symbol: str, position: PortfolioPosition) -> Tuple[Intent, ...]:
    """The intent an ``ExitDecision`` asks for, for the position it was made about. ``HOLD`` and
    ``NO_CHANGE_FALLBACK`` ask for nothing. A decision about another side than the position holds is refused:
    it was made about a position that no longer exists."""
    action = decision.action
    if action in (ExitAction.HOLD.value, ExitAction.NO_CHANGE_FALLBACK.value):
        return ()
    if position.symbol != symbol or position.direction != decision.side:
        raise TranslationError("LINEAGE_MISMATCH", f"the decision is about a {decision.side} position on {symbol}")
    reason = decision.reason_codes[0] if decision.reason_codes else EXIT_SIGNAL
    if action == ExitAction.EXIT.value:
        return (ExitIntent(symbol=symbol, direction=position.direction, reason=reason, reference_price=position.mark_price,
                           tag=decision.exit_decision_id),)
    if action == ExitAction.REDUCE.value:
        return (ReduceIntent(symbol=symbol, direction=position.direction, reason=reason, fraction=decision.requested_fraction,
                             reference_price=position.mark_price, tag=decision.exit_decision_id),)
    if action == ExitAction.TAKE_PARTIAL.value:
        return (ReduceIntent(symbol=symbol, direction=position.direction, reason=f"{TAKE_PARTIAL}:{reason}",
                             fraction=decision.requested_fraction, reference_price=position.mark_price,
                             tag=decision.exit_decision_id),)
    if action == ExitAction.TIGHTEN_PROTECTION.value:
        break_even = PositionReasonCode.PROTECTION_TIGHTENED_BREAK_EVEN.value in decision.reason_codes
        return (StopReplaceIntent(symbol=symbol, direction=position.direction, reason=BREAK_EVEN if break_even else TIGHTEN,
                                  new_stop_price=float(decision.suggested_protection_price),
                                  current_stop_price=position.stop_price,
                                  entry_price=position.entry_price if break_even else None,
                                  tag=decision.exit_decision_id),)
    raise TranslationError("UNKNOWN_EXIT_ACTION", str(action))


# ------------------------------------------------------------------------------------------------- stop moves
def trailing_stop_intents(targets: Mapping[str, float], snapshot: AccountSnapshot, *,
                          skip: Sequence[str] = ()) -> Tuple[StopReplaceIntent, ...]:
    """A stop ratchet: for every held position with a target stop that is TIGHTER than the stop resting on the
    exchange, the intent that moves it. A target at or behind the resting stop asks for nothing (a ratchet only
    turns one way), and a symbol in ``skip`` already has another intent in the same decision."""
    out = []
    for position in snapshot.positions:
        target = targets.get(position.symbol)
        if target is None or position.symbol in skip:
            continue
        resting = position.stop_price
        if resting is not None and not _wider(position.direction, float(resting), float(target)):
            continue                                                    # equal or wider than what rests: no move
        out.append(StopReplaceIntent(symbol=position.symbol, direction=position.direction, reason=TRAIL,
                                     new_stop_price=float(target), current_stop_price=resting))
    return tuple(out)


def book_stop_targets(book: Any) -> Dict[str, float]:
    """The ratcheted stops of a daily-trend book (``pipeline.daily_trend.Book``): ``advance_day`` raises each
    position's stop to ``max(stop, close x (1 - d))``. These are the targets of ``trailing_stop_intents``."""
    return {symbol: float(position.stop) for symbol, position in book.positions.items()}


def break_even_intent(position: PortfolioPosition, *, offset: float = 0.0) -> Optional[StopReplaceIntent]:
    """Move the stop to the entry price (``offset`` further into profit). None when the resting stop is already
    there or beyond, or when the market has not passed the level yet (a stop there would not be protective)."""
    level = position.entry_price + offset if position.direction == LONG else position.entry_price - offset
    resting = position.stop_price
    if resting is not None and not _wider(position.direction, float(resting), level):
        return None
    if (position.mark_price <= level) if position.direction == LONG else (position.mark_price >= level):
        return None
    return StopReplaceIntent(symbol=position.symbol, direction=position.direction, reason=BREAK_EVEN, new_stop_price=level,
                             current_stop_price=resting, entry_price=position.entry_price)


def resize_intent(position: PortfolioPosition) -> Optional[StopReplaceIntent]:
    """State explicitly that the resting stop is to cover the position as it is now. The boundary reads the
    coverage back after every fill by itself; this is for a caller that found a difference on its own."""
    if position.stop_price is None:
        return None
    return StopReplaceIntent(symbol=position.symbol, direction=position.direction, reason=RESIZE,
                             new_stop_price=float(position.stop_price), current_stop_price=float(position.stop_price),
                             quantity=position.quantity)


# ---------------------------------------------------------------------------------------------------- scaling
def scaling_intent(position: PortfolioPosition, target_quantity: float, *, stop_price: Optional[float] = None,
                   band: float = 0.0, reason: str = POSITION_SCALED_TO_ZERO) -> Optional[Intent]:
    """Scale a held position to ``target_quantity``: up or down by a ``RebalanceIntent``, to nothing by an
    ``ExitIntent``. None inside the band. The stop that covers the scaled position is the resting one unless a
    tighter one is given; a position without a known stop cannot be scaled up."""
    if target_quantity <= 0:
        return ExitIntent(symbol=position.symbol, direction=position.direction, reason=reason,
                          reference_price=position.mark_price)
    if not rebalance_required(position.quantity, target_quantity, band):
        return None
    stop = stop_price if stop_price is not None else position.stop_price
    if stop is None:
        raise TranslationError("MISSING_RISK_INFORMATION", f"{position.symbol}: scaling needs the stop that covers the position")
    return RebalanceIntent(symbol=position.symbol, direction=position.direction, current_quantity=position.quantity,
                           target_quantity=float(target_quantity), reference_price=position.mark_price, stop_price=float(stop),
                           band=float(band))


# --------------------------------------------------------------------------------------------------- decision
def record_decision(db: Any, *, definition: FamilyDefinition, account: AccountRef, intents: Sequence[Intent],
                    constraints: PortfolioConstraints, instruments: Sequence[str], cycle_id: str, decision_time_ms: int,
                    expires_at_ms: int, data_snapshot_id: str, mode: str, now_ms: int,
                    extra: Optional[Mapping[str, Any]] = None) -> IntentBatch:
    """Store the immutable decision record of these intents (``cati_family_decisions``, once per cycle and
    account) and return the batch that carries its provenance. Recording gives no authority: the family gate
    still decides whether the family may send anything."""
    document = dict(extra or {})
    document["intents"] = [to_plain(i) for i in intents]
    recorded = DecisionStore(db).record(
        cycle_id=cycle_id, family_id=definition.family_id, family_version=definition.family_version,
        rule_hash=definition.rule_hash, mandate_id=definition.mandate_id, account_id=account.broker_account_id,
        user_id=account.user_id, environment=account.environment, mode=mode, decision_time=int(decision_time_ms),
        data_cutoff=str(data_snapshot_id), expires_at=int(expires_at_ms), document=document, now_ms=int(now_ms))
    provenance = DecisionProvenance(
        family_id=definition.family_id, family_version=definition.family_version, mandate_id=definition.mandate_id or "NONE",
        hypothesis_id=definition.hypothesis_id or "NONE", rule_hash=definition.rule_hash,
        decision_id=recorded["decision_id"], decision_hash=recorded["decision_hash"], decision_time_ms=int(decision_time_ms),
        data_snapshot_id=str(data_snapshot_id), timeframe=definition.timeframe, mode=mode)
    return IntentBatch(provenance=provenance, account=account, intents=tuple(intents), constraints=constraints,
                       instruments=frozenset(instruments), created_at_ms=int(now_ms), expires_at_ms=int(expires_at_ms))


__all__ = ["EXIT_SIGNAL", "TAKE_PARTIAL", "POSITION_SCALED_TO_ZERO", "TranslationError", "intents_from_exit_decision",
           "trailing_stop_intents", "book_stop_targets", "break_even_intent", "resize_intent", "scaling_intent",
           "record_decision"]
