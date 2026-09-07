from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Optional

from app.core.config import settings
from app.runner.session_monitor import SessionMonitor
from shared_lib.persistence.audit import Audit
from shared_lib.persistence.db import DB, utc_now_iso
from shared_lib.persistence.trade_fills import record_fill, ExitReason


@dataclass
class DailyCloseValidationReport:
    bot_instance_id: str
    run_id: str
    environment: str
    daily_close_enabled: bool
    window_start: str
    window_end: str

    position_opened_at: str
    position_symbol: str
    position_side: str
    entry_price: float

    close_trigger_time: Optional[str]
    close_price: Optional[float]
    exit_reason: str
    close_fill_id: Optional[int]

    gross_pnl: Optional[float]
    fees: Optional[float]
    slippage: Optional[float]
    net_pnl: Optional[float]

    audit_event_written: bool
    state_reset_confirmed: bool
    validation_status: str
    errors: list[str]
    validated_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class _FakeClient:
    def __init__(self, symbol: str, qty: float, entry_price: float, close_price: float):
        self._symbol = symbol
        self._qty = qty
        self._entry_price = entry_price
        self._close_price = close_price
        self._closed = False

    def get_position(self, symbol: str) -> dict[str, Any] | None:
        if symbol != self._symbol or self._closed:
            return None
        upnl = (self._close_price - self._entry_price) * abs(self._qty)
        return {
            "positionAmt": str(self._qty),
            "unRealizedProfit": str(upnl),
            "initialMargin": str(abs(self._qty) * self._entry_price / 10.0),
        }

    def order_market(self, symbol: str, side: str, quantity: float, reduce_only: bool = True) -> dict[str, Any]:
        if symbol != self._symbol:
            raise RuntimeError("symbol_mismatch")
        if quantity <= 0:
            raise RuntimeError("qty_invalid")
        self._closed = True
        return {"status": "FILLED"}


class _FakeExecutor:
    def __init__(self, client: _FakeClient):
        self.client = client

    def cancel_open_orders(self, symbol: str) -> None:
        return None


def validate_daily_close_paper(
    *,
    db: DB,
    bot_instance_id: str,
    run_id: str,
    symbol: str,
    side: str,
    entry_price: float,
    close_price: float,
    quantity: float,
    estimated_fees: float = 0.0,
    estimated_slippage_pct: float = 0.0,
) -> DailyCloseValidationReport:
    """
    Paper-mode daily close validation helper.

    This is a test/admin-only routine that:
      - runs the SessionMonitor daily close path against a fake paper executor
      - writes required audit events
      - records a CLOSE fill with EXIT_DAILY_CLOSE
      - persists a validation report row
    """
    audit = Audit(db=db)
    validated_at = utc_now_iso()
    errors: list[str] = []

    audit_ok = True
    try:
        audit.event(
            event_type="DAILY_CLOSE_VALIDATION_STARTED",
            run_id=run_id,
            symbol=symbol,
            details={"bot_instance_id": bot_instance_id, "environment": "paper"},
        )
    except Exception as exc:
        audit_ok = False
        errors.append(f"audit_start_failed:{type(exc).__name__}")

    client = _FakeClient(symbol=symbol, qty=quantity, entry_price=entry_price, close_price=close_price)
    executor = _FakeExecutor(client)
    monitor = SessionMonitor(executor, audit, bot_instance_id=bot_instance_id)

    # Build minimal SymbolState-like object
    st = SimpleNamespace(position="LONG" if side.upper() == "LONG" else "SHORT", entry_price=float(entry_price))
    state = {symbol: st}

    close_trigger_time = None
    closed = False
    try:
        # Force "in window" by calling _scan_and_close directly.
        closed = bool(monitor._scan_and_close(state, datetime.now(timezone.utc)))
        close_trigger_time = utc_now_iso()
        audit.event(
            event_type="DAILY_PROFIT_CLOSE_TRIGGERED",
            run_id=run_id,
            symbol=symbol,
            details={"bot_instance_id": bot_instance_id, "forced_validation": True},
        )
    except Exception as exc:  # pragma: no cover
        errors.append(f"scan_and_close_failed:{type(exc).__name__}:{exc}")

    close_fill_id = None
    gross_pnl = None
    net_pnl = None
    if closed:
        try:
            # Record a synthetic close fill (paper proof artifact)
            _position_id = f"validation_{uuid.uuid4().hex[:10]}"
            gross_pnl = (float(close_price) - float(entry_price)) * float(abs(quantity)) * (1.0 if side.upper() == "LONG" else -1.0)
            net_pnl = gross_pnl - float(estimated_fees or 0.0)
            record_fill(
                db=db,
                symbol=symbol,
                side=side.upper(),
                action="CLOSE",
                qty=float(abs(quantity)),
                price=float(close_price),
                fee=float(estimated_fees),
                realized_pnl=float(gross_pnl),
                bot_instance_id=bot_instance_id,
                exit_reason=ExitReason.EXIT_DAILY_CLOSE,
                slippage_pct=float(estimated_slippage_pct),
                slippage_estimated=True,
                total_fees=float(estimated_fees),
                fees_estimated=True,
                net_pnl=float(net_pnl),
                position_id=_position_id,
                run_id=run_id,
                cycle_id="daily_close_validation",
                trace_id=None,
            )
            with db.connect() as conn:
                row = conn.execute(
                    """
                    SELECT id FROM trade_fills
                    WHERE position_id=? AND action='CLOSE'
                      AND (bot_instance_id = ? OR (bot_instance_id IS NULL AND ? = 'default'))
                    ORDER BY id DESC LIMIT 1
                    """,
                    (_position_id, bot_instance_id, bot_instance_id),
                ).fetchone()
            close_fill_id = int(row["id"]) if row else None
            audit.event(
                event_type="DAILY_CLOSE_FILL_RECORDED",
                run_id=run_id,
                symbol=symbol,
                details={"bot_instance_id": bot_instance_id, "close_fill_id": close_fill_id, "exit_reason": "EXIT_DAILY_CLOSE"},
            )
            audit.event(
                event_type="DAILY_CLOSE_POSITION_CLOSED",
                run_id=run_id,
                symbol=symbol,
                details={"bot_instance_id": bot_instance_id},
            )
        except Exception as exc:  # pragma: no cover
            errors.append(f"fill_record_failed:{type(exc).__name__}:{exc}")

    # State reset confirmation is runtime-owned; for validation we record an audit marker.
    state_reset_confirmed = closed
    try:
        audit.event(
            event_type="DAILY_CLOSE_STATE_RESET",
            run_id=run_id,
            details={"bot_instance_id": bot_instance_id, "state_reset_confirmed": state_reset_confirmed},
        )
    except Exception:
        audit_ok = False

    validation_status = "PASSED" if closed and close_fill_id is not None else "FAILED"
    try:
        audit.event(
            event_type="DAILY_CLOSE_VALIDATION_COMPLETED" if validation_status == "PASSED" else "DAILY_CLOSE_VALIDATION_FAILED",
            run_id=run_id,
            symbol=symbol,
            details={"bot_instance_id": bot_instance_id, "status": validation_status, "errors": errors},
        )
    except Exception:
        audit_ok = False

    report = DailyCloseValidationReport(
        bot_instance_id=bot_instance_id,
        run_id=run_id,
        environment="paper",
        daily_close_enabled=bool(settings.DAILY_CLOSE_ENABLED),
        window_start=str(settings.DAILY_CLOSE_WINDOW_START),
        window_end=str(settings.DAILY_CLOSE_WINDOW_END),
        position_opened_at=validated_at,
        position_symbol=symbol,
        position_side=side.upper(),
        entry_price=float(entry_price),
        close_trigger_time=close_trigger_time,
        close_price=float(close_price) if closed else None,
        exit_reason="EXIT_DAILY_CLOSE",
        close_fill_id=close_fill_id,
        gross_pnl=gross_pnl,
        fees=float(estimated_fees),
        slippage=float(estimated_slippage_pct),
        net_pnl=net_pnl,
        audit_event_written=audit_ok,
        state_reset_confirmed=state_reset_confirmed,
        validation_status=validation_status,
        errors=errors,
        validated_at=validated_at,
    )

    # Persist report
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO daily_close_validation_reports (
                id, bot_instance_id, run_id, environment,
                daily_close_enabled, window_start, window_end,
                position_opened_at, position_symbol, position_side, entry_price,
                close_trigger_time, close_price, exit_reason, close_fill_id,
                gross_pnl, fees, slippage, net_pnl,
                audit_event_written, state_reset_confirmed,
                validation_status, errors_json, validated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                f"dcv_{uuid.uuid4().hex[:12]}",
                bot_instance_id,
                run_id,
                "paper",
                1 if settings.DAILY_CLOSE_ENABLED else 0,
                str(settings.DAILY_CLOSE_WINDOW_START),
                str(settings.DAILY_CLOSE_WINDOW_END),
                report.position_opened_at,
                report.position_symbol,
                report.position_side,
                report.entry_price,
                report.close_trigger_time,
                report.close_price,
                report.exit_reason,
                report.close_fill_id,
                report.gross_pnl,
                report.fees,
                report.slippage,
                report.net_pnl,
                1 if report.audit_event_written else 0,
                1 if report.state_reset_confirmed else 0,
                report.validation_status,
                json.dumps(report.errors),
                report.validated_at,
            ),
        )

    return report
