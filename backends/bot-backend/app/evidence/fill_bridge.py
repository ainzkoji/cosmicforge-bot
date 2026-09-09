"""Project execution truth onto the canonical evidence tables.

The gap this closes was found by driving a real ``PaperRunner`` through a full
paper lifecycle and then looking at what the database actually held:

    trading_decisions    4478      <- written
    execution_attempts      0      <- never written
    positions               0      <- never written
    position_events         0      <- never written
    risk_events             0      <- never written
    trade_fills         17561      <- written

``app/evidence/writers.py`` defines all of those writers correctly, and the
Phase 12 smoke exercised them — by calling them directly. Nothing in the
runtime ever called them. So the canonical lineage stopped at the decision, and
§16.5 ("every trade must be reconstructable: decision -> execution attempt ->
fill -> position -> lifecycle -> close -> realized result") was not achievable
from canonical evidence, whatever the tests said.

This module is the missing edge. Three entry points, each with one job:

* :func:`record_fill_with_evidence` wraps ``record_fill`` so a fill and its
  canonical position row are written together, never one without the other.
* :func:`execution_attempt` brackets an executor call, so an attempt that never
  produces an order still leaves a row saying we tried and why it failed.
* :func:`sync_lifecycle_events` watches the PositionManager for break-even,
  trailing and stop moves — transitions that produce no fill and would
  otherwise leave no canonical trace.

Evidence failure never blocks trading. A broken write is reported through
``CANONICAL_EVIDENCE_WRITE_FAILED`` rather than by refusing to manage an open
position, because refusing is the more dangerous failure.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any

from app.risk.capital_ledger import margin_for

logger = logging.getLogger(__name__)

#: Quantity comparisons are float arithmetic on exchange step sizes.
QTY_TOLERANCE = 1e-9

#: Close reasons that get their own canonical event type, because §48/§49 ask
#: for daily close and kill-switch closes to be distinguishable from a normal
#: exit without reading free text.
_CLOSE_EVENT_BY_REASON = (
    ("DAILY_CLOSE", "DAILY_CLOSE"),
    ("END_OF_DAY", "DAILY_CLOSE"),
    ("KILL_SWITCH", "KILL_SWITCH_CLOSE"),
    ("KILLSWITCH", "KILL_SWITCH_CLOSE"),
)


def _evidence_failed(runner: Any, message: str) -> None:
    """Report a canonical-evidence write failure without stopping the runtime."""
    logger.error("[CANONICAL_EVIDENCE_WRITE_FAILED] %s", message)
    try:
        from app.ops.runtime_watchdog import get_watchdog

        context = getattr(runner, "context", None)
        bot_id = getattr(context, "bot_instance_id", None)
        if bot_id:
            get_watchdog().record_error(f"CANONICAL_EVIDENCE_WRITE_FAILED bot={bot_id} {message}")
    except Exception:
        pass


def _identity(runner: Any) -> dict[str, Any]:
    context = getattr(runner, "context", None)
    if context is None:
        return {}
    from app.evidence.runner_bridge import resolve_provenance, run_provenance

    execution_mode = runner._effective_execution_mode()
    run_id = getattr(runner, "run_id", None)
    return {
        "bot_instance_id": context.bot_instance_id,
        "user_id": getattr(context, "user_id", None),
        "broker_account_id": getattr(context, "broker_account_id", None),
        "run_id": run_id,
        "cycle_id": getattr(runner, "cycle_id", None),
        "execution_mode": execution_mode,
        "broker_environment": getattr(context, "broker_environment", None),
        "provenance": run_provenance(
            getattr(runner, "db", None),
            run_id,
            resolve_provenance(execution_mode, getattr(context, "broker_environment", None)),
        ),
    }


def active_decision_id(runner: Any, symbol: str) -> str | None:
    """The canonical decision this symbol is currently being evaluated under.

    Pre-allocated by the evidence bridge before the evaluation runs, because
    execution happens inside the evaluation and therefore *before* the decision
    row is finalized.
    """
    return (getattr(runner, "_active_decision_ids", None) or {}).get(str(symbol).upper())


# ── Execution attempts ──────────────────────────────────────────────────────


@contextmanager
def execution_attempt(runner: Any, symbol: str, requested_action: str):
    """Bracket an executor call with a canonical attempt row.

    Yields a small recorder. The attempt is completed on the way out with
    whatever the caller reported, or with the exception if one escaped — an
    executor that dies mid-order still leaves evidence that an order was
    attempted.
    """
    db = getattr(runner, "db", None)
    identity = _identity(runner)
    attempt_id = None
    if db is not None and identity:
        try:
            from app.evidence.writers import open_execution_attempt

            attempt_id = open_execution_attempt(
                db,
                decision_id=active_decision_id(runner, symbol),
                bot_instance_id=identity["bot_instance_id"],
                symbol=symbol,
                requested_action=str(requested_action).upper(),
                execution_mode=identity["execution_mode"],
                broker_environment=identity["broker_environment"],
                provenance=identity["provenance"],
                run_id=identity["run_id"],
                cycle_id=identity["cycle_id"],
            )
        except Exception as exc:
            _evidence_failed(runner, f"open_execution_attempt failed: {exc}")

    recorder = _AttemptRecorder(attempt_id)
    try:
        yield recorder
    except Exception as exc:
        _complete(runner, attempt_id, result="ERROR", error_class=type(exc).__name__,
                  error_detail=str(exc)[:500])
        raise
    else:
        _complete(
            runner, attempt_id,
            result=recorder.result or "UNKNOWN",
            broker_order_id=recorder.broker_order_id,
            position_id=recorder.position_id,
            primary_reason=recorder.primary_reason,
        )


class _AttemptRecorder:
    __slots__ = ("attempt_id", "result", "broker_order_id", "position_id", "primary_reason")

    def __init__(self, attempt_id: str | None) -> None:
        self.attempt_id = attempt_id
        self.result: str | None = None
        self.broker_order_id: str | None = None
        self.position_id: str | None = None
        self.primary_reason: str | None = None

    def completed(self, result: str, *, broker_order_id: Any = None,
                  position_id: str | None = None, primary_reason: str | None = None) -> None:
        self.result = str(result)
        if broker_order_id:
            self.broker_order_id = str(broker_order_id)
        if position_id:
            self.position_id = position_id
        if primary_reason:
            self.primary_reason = str(primary_reason)


def _complete(runner: Any, attempt_id: str | None, **kw: Any) -> None:
    if not attempt_id:
        return
    try:
        from app.evidence.writers import complete_execution_attempt

        complete_execution_attempt(runner.db, attempt_id, **kw)
    except Exception as exc:
        _evidence_failed(runner, f"complete_execution_attempt failed: {exc}")


# ── Fills -> positions and their event stream ───────────────────────────────


def record_fill_with_evidence(runner: Any, db: Any, **kw: Any) -> Any:
    """``record_fill``, plus the canonical position evidence for that fill.

    The fill is written first and unconditionally: execution truth must never
    depend on the evidence projection succeeding.
    """
    from shared_lib.persistence.trade_fills import record_fill

    # The fill row carries the same provenance as the canonical evidence it
    # produces, so trade_fills can be partitioned by provenance instead of by
    # guessing from strategy/account_id.
    if "provenance" not in kw:
        identity = _identity(runner)
        if identity:
            kw["provenance"] = identity["provenance"]

    result = record_fill(db, **kw)
    try:
        project_fill(runner, db, kw)
    except Exception as exc:
        _evidence_failed(runner, f"canonical projection failed for {kw.get('symbol')}: {exc}")
    return result


def _decision_leverage(db: Any, decision_id: str | None) -> float:
    """Leverage this entry was sized at, from its canonical decision.

    Defaults to 1.0 when unknown, which over-reserves capital rather than
    under-reserving it -- the safe direction for a budget check.
    """
    if not decision_id:
        return 1.0
    try:
        with db.connect() as conn:
            row = conn.execute(
                "SELECT leverage FROM trading_decisions WHERE decision_id=?",
                (decision_id,),
            ).fetchone()
        value = float((row[0] if row else None) or 0.0)
        return value if value >= 1.0 else 1.0
    except Exception:
        return 1.0


def project_fill(runner: Any, db: Any, kw: dict[str, Any]) -> None:
    """Derive the canonical position row and lifecycle event from one fill."""
    from app.evidence.writers import (
        record_position_event,
        record_position_opened,
        update_position_quantities,
    )

    identity = _identity(runner)
    position_id = kw.get("position_id")
    if not identity or not position_id:
        # No bot context, or a fill the runtime could not attribute to a
        # position. Recording a position row with an invented id would be
        # worse than recording nothing.
        return

    symbol = str(kw.get("symbol") or "").upper()
    action = str(kw.get("action") or "").upper()
    qty = float(kw.get("qty") or 0.0)
    price = float(kw.get("price") or 0.0)
    fee = kw.get("total_fees")
    if fee is None:
        fee = kw.get("fee")
    realized_pnl = kw.get("realized_pnl")
    if realized_pnl is None:
        realized_pnl = kw.get("pnl")
    decision_id = active_decision_id(runner, symbol)

    with db.connect() as conn:
        row = conn.execute(
            "SELECT original_qty, remaining_qty, realized_qty, realized_pnl, fees, "
            "status, leverage, committed_margin "
            "FROM positions WHERE position_id=?", (position_id,),
        ).fetchone()

    if action == "OPEN":
        if row is None:
            # Capital accounting lives on the position row so it survives a
            # restart: the ledger sums committed_margin over OPEN positions
            # rather than trusting anything held in memory.
            leverage = _decision_leverage(db, decision_id)
            record_position_opened(
                db,
                position_id=position_id,
                bot_instance_id=identity["bot_instance_id"],
                symbol=symbol,
                side=str(kw.get("side") or "").upper(),
                original_qty=qty,
                entry_price=price,
                provenance=identity["provenance"],
                user_id=identity["user_id"],
                broker_account_id=identity["broker_account_id"],
                run_id=identity["run_id"],
                decision_id=decision_id,
                execution_mode=identity["execution_mode"],
                broker_environment=identity["broker_environment"],
                leverage=leverage,
                committed_margin=margin_for(qty, price, leverage),
            )
        else:
            # A second OPEN on a live position is an ADD, not a new position.
            original = float(row["original_qty"]) + qty
            remaining = float(row["remaining_qty"]) + qty
            update_position_quantities(
                db, position_id,
                remaining_qty=remaining, realized_qty=float(row["realized_qty"]),
            )
            leverage = float(row["leverage"] or 1.0)
            with db.connect() as conn:
                conn.execute(
                    "UPDATE positions SET original_qty=?, committed_margin=? "
                    "WHERE position_id=?",
                    (original,
                     float(row["committed_margin"] or 0.0) + margin_for(qty, price, leverage),
                     position_id),
                )
            record_position_event(
                db, position_id=position_id, bot_instance_id=identity["bot_instance_id"],
                symbol=symbol, event_type="ADDED", quantity=qty, remaining_qty=remaining,
                price=price, provenance=identity["provenance"],
                run_id=identity["run_id"], cycle_id=identity["cycle_id"],
                decision_id=decision_id,
            )
        return

    if row is None:
        # A close for a position that was never opened canonically. This is a
        # real integrity problem; say so rather than inventing a row.
        _evidence_failed(
            runner,
            f"close fill for unknown position {position_id} ({symbol} {action} {qty})",
        )
        return

    remaining = max(0.0, float(row["remaining_qty"]) - qty)
    realized = float(row["realized_qty"]) + qty
    is_final = remaining <= QTY_TOLERANCE
    # The runner labels a TP1 partial through several fields depending on which
    # site recorded it; any of them identifies the leg.
    exit_reason = " ".join(
        str(kw.get(key) or "")
        for key in ("exit_reason", "fill_type", "trigger_source", "position_phase")
    ).upper()

    if is_final:
        event_type = "FINAL_CLOSE"
        for needle, mapped in _CLOSE_EVENT_BY_REASON:
            if needle in exit_reason:
                event_type = mapped
                break
    elif action == "PARTIAL_CLOSE" and "TP1" in exit_reason:
        event_type = "TP1"
    else:
        event_type = "PARTIAL_CLOSE"

    total_pnl = float(row["realized_pnl"] or 0.0) + float(realized_pnl or 0.0)
    total_fees = float(row["fees"] or 0.0) + float(fee or 0.0)

    update_position_quantities(
        db, position_id,
        remaining_qty=remaining,
        realized_qty=realized,
        realized_pnl=total_pnl,
        fees=total_fees,
        status="CLOSED" if is_final else None,
        close_reason=(kw.get("exit_reason") or None) if is_final else None,
    )

    # Release capital in proportion to what was closed. A final close leaves
    # the row OPEN-less, so the ledger stops counting it entirely; a partial
    # close has to hand back exactly its share and no more.
    original_qty = float(row["original_qty"] or 0.0)
    opening_margin = float(row["committed_margin"] or 0.0)
    released_margin = (
        0.0 if is_final
        else opening_margin * (remaining / original_qty) if original_qty > 0
        else 0.0
    )
    with db.connect() as conn:
        conn.execute(
            "UPDATE positions SET committed_margin=? WHERE position_id=?",
            (released_margin, position_id),
        )
    record_position_event(
        db, position_id=position_id, bot_instance_id=identity["bot_instance_id"],
        symbol=symbol, event_type=event_type, quantity=qty, remaining_qty=remaining,
        price=price, fee=fee, realized_pnl=realized_pnl,
        reason=kw.get("exit_reason"), provenance=identity["provenance"],
        run_id=identity["run_id"], cycle_id=identity["cycle_id"], decision_id=decision_id,
    )


# ── Lifecycle transitions that produce no fill ──────────────────────────────


def sync_lifecycle_events(runner: Any, symbol: str) -> None:
    """Emit canonical events for break-even, trailing and stop moves.

    These transitions change a position without producing a fill, so nothing in
    the fill path can see them. Called once per symbol per cycle, after
    management has run, and driven purely by observed PositionManager state --
    so it is indifferent to which of the several code paths caused the change.

    Idempotence is checked against the *database*, not against in-process
    memory. A diff against memory would miss every transition that happened in
    a cycle the process did not witness -- including the first cycle after a
    restart, which is exactly when break-even and trailing tend to engage.
    """
    db = getattr(runner, "db", None)
    identity = _identity(runner)
    manager = getattr(runner, "position_manager", None)
    if db is None or not identity or manager is None:
        return

    try:
        position = manager.get_position(symbol)
    except Exception:
        return
    if position is None:
        return

    position_id = getattr(position, "position_id", None)
    if not position_id:
        return

    try:
        _emit_transitions(runner, db, identity, symbol, position, position_id)
    except Exception as exc:
        _evidence_failed(runner, f"lifecycle event projection failed for {symbol}: {exc}")


def _recorded(db: Any, position_id: str) -> tuple[set[str], float | None]:
    """Event types already recorded for this position, and the last stop seen."""
    with db.connect() as conn:
        rows = conn.execute(
            "SELECT event_type, stop_price FROM position_events "
            "WHERE position_id=? ORDER BY occurred_at",
            (position_id,),
        ).fetchall()
    seen = {str(r[0]) for r in rows}
    last_stop = None
    for row in rows:
        if row[1] is not None:
            last_stop = float(row[1])
    return seen, last_stop


def _emit_transitions(runner, db, identity, symbol, position, position_id) -> None:
    from app.evidence.writers import record_position_event

    sl = getattr(position, "sl", None)
    stop_price = float(getattr(sl, "current_stop", 0.0) or 0.0)
    break_even = bool(getattr(sl, "is_break_even", False))
    trailing = str(getattr(position, "phase", "")).endswith("RUNNER_TRAILING")

    seen, last_stop = _recorded(db, position_id)

    common = dict(
        position_id=position_id,
        bot_instance_id=identity["bot_instance_id"],
        symbol=symbol,
        provenance=identity["provenance"],
        run_id=identity["run_id"],
        cycle_id=identity["cycle_id"],
        decision_id=active_decision_id(runner, symbol),
        remaining_qty=float(getattr(position, "current_qty", 0.0) or 0.0),
    )

    if break_even and "BREAK_EVEN_ACTIVATED" not in seen:
        record_position_event(
            db, event_type="BREAK_EVEN_ACTIVATED", stop_price=stop_price,
            reason="stop moved to break-even", **common,
        )
        seen.add("BREAK_EVEN_ACTIVATED")
        last_stop = stop_price

    if trailing and "TRAILING_ACTIVATED" not in seen:
        record_position_event(
            db, event_type="TRAILING_ACTIVATED", stop_price=stop_price,
            reason="runner entered trailing phase", **common,
        )
        seen.add("TRAILING_ACTIVATED")
        last_stop = stop_price

    if stop_price > 0 and last_stop is not None and abs(stop_price - last_stop) > 1e-9:
        record_position_event(
            db, event_type="STOP_UPDATED", stop_price=stop_price,
            reason=f"stop moved from {last_stop}", **common,
        )
