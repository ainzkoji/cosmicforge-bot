"""Exchange-authoritative position reconciliation.

Broker positions use venue quantity and side as economic truth.  Local rows are
an auditable projection: mismatches are closed or corrected in place while an
append-only reconciliation event records every material transition.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Callable, Iterable

from app.risk.capital_ledger import margin_for

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class BrokerPosition:
    symbol: str
    side: str
    quantity: Decimal
    entry_price: Decimal
    leverage: Decimal
    margin_type: str
    position_mode: str


def quantity_close_reason(
    quantity: Any,
    spec: Any | None,
    *,
    broker_quantity: Any | None = None,
) -> str | None:
    """Return why a remaining quantity is economically closed, if it is.

    Decimal arithmetic and the connected venue's InstrumentSpec are used; no
    symbol-specific epsilon is allowed.
    """
    qty = abs(Decimal(str(quantity or 0)))
    if broker_quantity is not None and abs(Decimal(str(broker_quantity or 0))) == 0:
        return "BROKER_FLAT"
    if qty == 0:
        return "FILLED_TO_ZERO"
    if spec is None:
        return None
    step = abs(Decimal(str(getattr(spec, "step_size", 0) or 0)))
    minimum = abs(Decimal(str(getattr(spec, "min_qty", 0) or 0)))
    executable = (qty / step).to_integral_value(rounding=ROUND_DOWN) * step if step else qty
    if (minimum and qty < minimum) or (step and qty < step) or executable == 0:
        return "UNTRADEABLE_DUST"
    return None


def parse_broker_positions(rows: Iterable[dict[str, Any]], *, hedge_mode: bool) -> list[BrokerPosition]:
    positions: list[BrokerPosition] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        qty_signed = Decimal(str(row.get("positionAmt") or 0))
        if not symbol or qty_signed == 0:
            continue
        raw_side = str(row.get("positionSide") or "BOTH").upper()
        if hedge_mode and raw_side in {"LONG", "SHORT"}:
            side = raw_side
        else:
            side = "LONG" if qty_signed > 0 else "SHORT"
        positions.append(BrokerPosition(
            symbol=symbol,
            side=side,
            quantity=abs(qty_signed),
            entry_price=Decimal(str(row.get("entryPrice") or 0)),
            leverage=Decimal(str(row.get("leverage") or 1)),
            margin_type=str(row.get("marginType") or "").lower(),
            position_mode="HEDGE" if hedge_mode else "ONE_WAY",
        ))
    return positions


def _position_spec(client: Any, symbol: str) -> Any | None:
    from app.exchange.registry import get_instrument_registry

    registry = get_instrument_registry()
    spec = registry.get_spec("binance", symbol)
    if spec is None:
        try:
            registry.refresh(broker_id="binance", client=client, force=True)
            spec = registry.get_spec("binance", symbol)
        except Exception:
            logger.exception("[POSITION_RECONCILIATION] instrument lookup failed for %s", symbol)
    return spec


def _record_reconciliation(
    db: Any,
    *,
    bot_instance_id: str,
    run_id: str | None,
    cycle_id: str | None,
    execution_mode: str,
    broker_environment: str,
    position_id: str,
    symbol: str,
    expected: Any,
    observed: Any,
    action: str,
    reason: str,
    detail: dict[str, Any],
) -> None:
    from app.evidence.writers import record_position_event, record_reconciliation_event

    provenance = "TESTNET" if broker_environment in {"demo", "testnet"} else "LIVE_MAINNET"
    record_reconciliation_event(
        db,
        bot_instance_id=bot_instance_id,
        run_id=run_id,
        cycle_id=cycle_id,
        symbol=symbol,
        position_id=position_id,
        execution_mode=execution_mode,
        broker_environment=broker_environment,
        scope="BROKER_POSITION",
        expected=expected,
        observed=observed,
        action=action,
        reason=reason,
        result="APPLIED",
        provenance=provenance,
        detail=detail,
    )
    record_position_event(
        db,
        position_id=position_id,
        bot_instance_id=bot_instance_id,
        run_id=run_id,
        cycle_id=cycle_id,
        symbol=symbol,
        event_type="RECONCILIATION",
        remaining_qty=float(detail.get("broker_quantity", 0) or 0),
        reason=reason,
        provenance=provenance,
        detail=detail,
    )


def reconcile_position_rows(
    db: Any,
    *,
    bot_instance_id: str,
    broker_account_id: str,
    broker_positions: Iterable[BrokerPosition],
    position_mode: str,
    spec_resolver: Callable[[str], Any | None],
    run_id: str | None = None,
    cycle_id: str | None = None,
    execution_mode: str = "broker",
    broker_environment: str = "demo",
) -> dict[str, Any]:
    """Project a broker snapshot onto canonical local positions, idempotently."""
    mode = str(position_mode).upper()
    broker = list(broker_positions)
    broker_by_key = {
        (p.symbol, p.side if mode == "HEDGE" else "NET"): p for p in broker
    }
    with db.connect() as conn:
        local = conn.execute(
            """SELECT rowid AS _rowid, * FROM positions
               WHERE bot_instance_id=? AND broker_account_id=? AND status='OPEN'
               ORDER BY opened_at, rowid""",
            (bot_instance_id, broker_account_id),
        ).fetchall()

    by_symbol: dict[str, list[Any]] = {}
    for row in local:
        by_symbol.setdefault(str(row["symbol"]).upper(), []).append(row)
    for p in broker:
        by_symbol.setdefault(p.symbol, [])

    changes: list[dict[str, Any]] = []
    for symbol, rows in by_symbol.items():
        spec = spec_resolver(symbol)
        if mode == "HEDGE":
            sides = {"LONG", "SHORT"}
        else:
            sides = {"NET"}
        for mode_side in sides:
            bp = broker_by_key.get((symbol, mode_side))
            candidates = rows if mode_side == "NET" else [r for r in rows if str(r["side"]).upper() == mode_side]
            if bp is None:
                for row in candidates:
                    local_qty = Decimal(str(row["remaining_qty"] or 0))
                    dust_reason = quantity_close_reason(local_qty, spec, broker_quantity=0)
                    reason = "ROUNDING_RESIDUAL_RECONCILED" if quantity_close_reason(local_qty, spec) == "UNTRADEABLE_DUST" else (dust_reason or "BROKER_FLAT")
                    now = _now()
                    with db.connect() as conn:
                        conn.execute(
                            """UPDATE positions SET status='CLOSED', remaining_qty=0,
                               committed_margin=0, closed_at=?, updated_at=?,
                               close_reason=?, reconciliation_reason=?, last_reconciled_at=?,
                               broker_position_mode=? WHERE position_id=? AND status='OPEN'""",
                            (now, now, reason, reason, now, mode, row["position_id"]),
                        )
                    detail = {"local_side": row["side"], "local_quantity": float(local_qty), "broker_quantity": 0.0, "position_mode": mode}
                    _record_reconciliation(db, bot_instance_id=bot_instance_id, run_id=run_id,
                        cycle_id=cycle_id, execution_mode=execution_mode,
                        broker_environment=broker_environment, position_id=row["position_id"],
                        symbol=symbol, expected=f"{row['side']} {local_qty}", observed="FLAT",
                        action="CLOSE_LOCAL_POSITION", reason=reason, detail=detail)
                    changes.append({"position_id": row["position_id"], "action": "CLOSED", "reason": reason})
                continue

            matching = [r for r in candidates if str(r["side"]).upper() == bp.side]
            keeper = matching[-1] if matching else None
            stale = [r for r in candidates if keeper is None or r["position_id"] != keeper["position_id"]]
            for row in stale:
                local_qty = Decimal(str(row["remaining_qty"] or 0))
                if quantity_close_reason(local_qty, spec) == "UNTRADEABLE_DUST":
                    reason = "ROUNDING_RESIDUAL_RECONCILED"
                elif str(row["side"]).upper() != bp.side:
                    reason = "BROKER_NET_POSITION_OPPOSITE_SIDE_RECONCILED"
                else:
                    reason = "DUPLICATE_LOCAL_POSITION_RECONCILED"
                now = _now()
                with db.connect() as conn:
                    conn.execute(
                        """UPDATE positions SET status='CLOSED', remaining_qty=0,
                           committed_margin=0, closed_at=?, updated_at=?, close_reason=?,
                           reconciliation_reason=?, last_reconciled_at=?,
                           broker_position_mode=? WHERE position_id=? AND status='OPEN'""",
                        (now, now, reason, reason, now, mode, row["position_id"]),
                    )
                detail = {"local_side": row["side"], "local_quantity": row["remaining_qty"], "broker_side": bp.side, "broker_quantity": float(bp.quantity), "position_mode": mode}
                _record_reconciliation(db, bot_instance_id=bot_instance_id, run_id=run_id,
                    cycle_id=cycle_id, execution_mode=execution_mode,
                    broker_environment=broker_environment, position_id=row["position_id"],
                    symbol=symbol, expected=f"{row['side']} {row['remaining_qty']}",
                    observed=f"{bp.side} {bp.quantity}", action="CLOSE_STALE_LOCAL_POSITION",
                    reason=reason, detail=detail)
                changes.append({"position_id": row["position_id"], "action": "CLOSED", "reason": reason})

            margin = margin_for(float(bp.quantity), float(bp.entry_price), float(bp.leverage or 1))
            now = _now()
            if keeper is None:
                pid = f"rec_{uuid.uuid4().hex}"
                with db.connect() as conn:
                    conn.execute(
                        """INSERT INTO positions
                           (position_id,bot_instance_id,broker_account_id,run_id,symbol,side,
                            execution_mode,broker_environment,provenance,original_qty,
                            remaining_qty,realized_qty,entry_price,status,opened_at,updated_at,
                            leverage,committed_margin,requested_qty,broker_executed_qty,
                            broker_position_mode,reconciliation_reason,last_reconciled_at)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (pid,bot_instance_id,broker_account_id,run_id,symbol,bp.side,
                         execution_mode,broker_environment,"TESTNET" if broker_environment in {"demo","testnet"} else "LIVE_MAINNET",
                         float(bp.quantity),float(bp.quantity),0,float(bp.entry_price),"OPEN",now,now,
                         float(bp.leverage),margin,None,float(bp.quantity),mode,"BROKER_POSITION_DISCOVERED",now),
                    )
                detail = {"broker_side": bp.side, "broker_quantity": float(bp.quantity), "broker_entry_price": float(bp.entry_price), "position_mode": mode}
                _record_reconciliation(db, bot_instance_id=bot_instance_id, run_id=run_id,
                    cycle_id=cycle_id, execution_mode=execution_mode,
                    broker_environment=broker_environment, position_id=pid, symbol=symbol,
                    expected="NO_LOCAL_POSITION", observed=f"{bp.side} {bp.quantity}",
                    action="CREATE_RECONCILED_POSITION", reason="BROKER_POSITION_DISCOVERED", detail=detail)
                changes.append({"position_id": pid, "action": "CREATED", "reason": "BROKER_POSITION_DISCOVERED"})
            else:
                requested = keeper["requested_qty"] if "requested_qty" in keeper.keys() and keeper["requested_qty"] is not None else keeper["original_qty"]
                material = (
                    str(keeper["side"]).upper() != bp.side
                    or Decimal(str(keeper["remaining_qty"] or 0)) != bp.quantity
                    or Decimal(str(keeper["entry_price"] or 0)) != bp.entry_price
                    or Decimal(str(keeper["leverage"] or 1)) != bp.leverage
                    or abs(float(keeper["committed_margin"] or 0) - margin) > 1e-9
                )
                if material:
                    with db.connect() as conn:
                        conn.execute(
                            """UPDATE positions SET side=?, original_qty=?, remaining_qty=?,
                               entry_price=?, leverage=?, committed_margin=?, requested_qty=?,
                               broker_executed_qty=?, broker_position_mode=?,
                               reconciliation_reason='BROKER_QUANTITY_AUTHORITATIVE',
                               last_reconciled_at=?, updated_at=? WHERE position_id=?""",
                            (bp.side,float(bp.quantity),float(bp.quantity),float(bp.entry_price),
                             float(bp.leverage),margin,float(requested) if requested is not None else None,
                             float(bp.quantity),mode,now,now,keeper["position_id"]),
                        )
                    detail = {"local_side": keeper["side"], "local_quantity": keeper["remaining_qty"], "broker_side": bp.side, "broker_quantity": float(bp.quantity), "broker_entry_price": float(bp.entry_price), "position_mode": mode}
                    _record_reconciliation(db, bot_instance_id=bot_instance_id, run_id=run_id,
                        cycle_id=cycle_id, execution_mode=execution_mode,
                        broker_environment=broker_environment, position_id=keeper["position_id"],
                        symbol=symbol, expected=f"{keeper['side']} {keeper['remaining_qty']}",
                        observed=f"{bp.side} {bp.quantity}", action="CORRECT_LOCAL_POSITION",
                        reason="BROKER_QUANTITY_AUTHORITATIVE", detail=detail)
                    changes.append({"position_id": keeper["position_id"], "action": "CORRECTED", "reason": "BROKER_QUANTITY_AUTHORITATIVE"})
    return {"position_mode": mode, "changes": changes, "changed": len(changes)}


def reconcile_runner_positions(runner: Any, rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Runtime adapter using the runner's resolved broker identity."""
    context = getattr(runner, "context", None)
    if context is None or str(runner._effective_execution_mode()).lower() != "broker":
        return {"position_mode": "PAPER", "changes": [], "changed": 0}
    try:
        mode_payload = runner.client._signed_get("/fapi/v1/positionSide/dual", {})
        hedge = bool(mode_payload.get("dualSidePosition"))
    except Exception:
        hedge = any(str(r.get("positionSide") or "BOTH").upper() in {"LONG", "SHORT"} for r in rows)
    parsed = parse_broker_positions(rows, hedge_mode=hedge)
    return reconcile_position_rows(
        runner.db,
        bot_instance_id=context.bot_instance_id,
        broker_account_id=context.broker_account_id,
        broker_positions=parsed,
        position_mode="HEDGE" if hedge else "ONE_WAY",
        spec_resolver=lambda symbol: _position_spec(runner.client, symbol),
        run_id=getattr(runner, "run_id", None),
        cycle_id=getattr(runner, "cycle_id", None),
        execution_mode="broker",
        broker_environment=str(getattr(context, "broker_environment", "")),
    )
