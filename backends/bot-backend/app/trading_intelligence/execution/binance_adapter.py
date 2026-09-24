"""Binance implementation of the broker-neutral ExecutionAdapter -- a thin
WRAPPER around the existing ``BinanceExecutor`` (Section 20.12). Nothing
about working Binance execution is rewritten:

* ``submit_entry`` -> ``BinanceExecutor.execute_signal``, which keeps ALL of
  its existing authority: the per-trade capital gate (fixed_amount = up to
  that allocation PER approved trade), the atomic position-slot reservation
  (``reserve_entry_slot``), the account margin reservation
  (``ACCOUNT_RESERVATIONS``), entry-protection idempotency (now keyed by the
  CATI plan identity), submit-unknown handling, broker fill resolution and
  mandatory protection placement at the broker-FILLED quantity (with
  rollback when protection fails).
* the other protocol methods delegate to the existing executor / exchange
  client paths (ensure_protection, update_protection, TP1 partial close,
  CLOSE, resolve_order_fill, get_position_info).

This module only TRANSLATES the executor's result vocabulary into the
broker-neutral ``EntryResult``. Binance semantics (BUY/SELL signals,
clientOrderId, reduce-only) never leak into CATI contracts.
"""
from __future__ import annotations

import time
from typing import Any, Dict, Optional

from app.trading_intelligence.contracts.execution import (
    ExecutionAttemptStatus as X, ExecutionSupportStatus, RiskRejectionFamily as F,
)
from app.trading_intelligence.execution.adapter import (
    BrokerPositionState, EntryRequest, EntryResult, OrderState, assert_not_widening,
)
from app.trading_intelligence.versions import EXECUTION_ADAPTER_PROTOCOL_VERSION

#: executor status -> (broker-neutral status, rejection family)
_STATUS_MAP = {
    "SUBMIT_UNCERTAIN": (X.SUBMIT_UNKNOWN.value, None),
    "ORDER_NOT_FILLED": (X.NOT_FILLED.value, None),
    "MAX_OPEN_POSITIONS": (X.REJECTED.value, F.SLOT.value),
    "INSUFFICIENT_MARGIN": (X.REJECTED.value, F.MARGIN.value),
    "CAPITAL_LEDGER_UNAVAILABLE": (X.REJECTED.value, F.MARGIN.value),
    "EXPOSURE_LIMIT_EXCEEDED": (X.REJECTED.value, F.SIZING.value),
    "NO_TRADE_INVALID_QTY": (X.REJECTED.value, F.SIZING.value),
    "SKIPPED_NOT_LIVE_SYMBOL": (X.REJECTED.value, F.INSTRUMENT.value),
    "STALE_DATA_DETECTED": (X.REJECTED.value, F.OTHER.value),
    "CIRCUIT_BREAKER_TRIPPED": (X.REJECTED.value, F.OTHER.value),
    "NO_TRADE": (X.REJECTED.value, F.OTHER.value),
    "PAPER_ERROR": (X.REJECTED.value, F.OTHER.value),
    "ENTRY_INTENT_REUSED": (X.DUPLICATE_SUPPRESSED.value, None),
    "ENTRY_LOCK_HELD": (X.DUPLICATE_SUPPRESSED.value, None),
    "ALREADY_OPEN": (X.DUPLICATE_SUPPRESSED.value, None),
    "PROTECTION_FAILED_ENTRY_CLOSED": (X.PROTECTION_FAILED_ROLLED_BACK.value, None),
}
_FILLED_STATUSES = ("ORDER_PLACED", "PAPER_POSITION_OPENED", "PAPER_FILLED")


def _f(v) -> Optional[float]:
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


class BinanceExecutionAdapter:
    #: The wrapper passes the broker contract suite (tests/trading_intelligence/
    #: test_section20_execution_boundary.py). It is not DEMO_VALIDATED until
    #: this adapter itself has demo-run evidence; the wrapped executor keeps
    #: its own operational history.
    execution_support_status = ExecutionSupportStatus.CONTRACT_VALIDATED.value
    protocol_version = EXECUTION_ADAPTER_PROTOCOL_VERSION

    def __init__(self, executor: Any, *, venue: str = "BINANCE_USDM", position_manager: Any = None,
                 clock=None) -> None:
        self.executor = executor
        self.venue = venue
        self.adapter_id = f"binance_executor_wrapper:{venue}"
        self.position_manager = position_manager
        self._clock = clock or (lambda: int(time.time() * 1000))

    # -- entry --------------------------------------------------------------------------------------
    def submit_entry(self, request: EntryRequest) -> EntryResult:
        from app.execution.executor import ExchangeError

        signal = "BUY" if request.side == "LONG" else "SELL"
        try:
            res = self._submit(request, signal)
        except ExchangeError as exc:
            # The executor classifies this as PRE-SUBMIT (order never dispatched) and has
            # already released its entry lock: honour that verdict. Any OTHER exception
            # propagates and the boundary treats the outcome as UNKNOWN (never re-submits).
            return EntryResult(status=X.REJECTED.value, raw_status="EXCHANGE_ERROR_PRE_SUBMIT",
                               rejection_family=F.OTHER.value, reason_codes=("EXCHANGE_ERROR_PRE_SUBMIT",),
                               detail=str(exc)[:200])
        return self.translate(res)

    def _submit(self, request: EntryRequest, signal: str):
        return self.executor.execute_signal(
            request.venue_symbol, signal, float(request.notional), sl_price=float(request.stop_price),
            tp_price=float(request.target_price or 0.0), current_open_count=int(request.current_open_count),
            current_equity=float(request.current_equity), leverage_override=int(round(request.leverage)),
            cycle_id=request.cycle_id, intent_identity=request.intent_identity,
        )

    @staticmethod
    def translate(res: Any) -> EntryResult:
        raw = str(getattr(res, "status", "") or "UNKNOWN")
        d = res.details if isinstance(getattr(res, "details", None), dict) else {}
        normalized = d.get("normalized") if isinstance(d.get("normalized"), dict) else {}
        resolution = d.get("fill_resolution") if isinstance(d.get("fill_resolution"), dict) else {}
        requested = _f(d.get("requested_qty") if d.get("requested_qty") is not None else d.get("qty"))
        filled = _f(d.get("filled_qty") if d.get("filled_qty") is not None else
                    (normalized.get("executed_qty") if normalized else resolution.get("executed_qty")))
        protection = d.get("protection") if isinstance(d.get("protection"), dict) else {}
        common = dict(
            raw_status=raw, broker_order_id=getattr(res, "order_id", None) or resolution.get("broker_order_id"),
            client_order_id=d.get("client_order_id") or resolution.get("client_order_id"),
            requested_qty=requested, submitted_qty=requested, filled_qty=filled,
            avg_fill_price=_f(getattr(res, "avg_price", None)), submitted_price=_f(d.get("price")),
            actual_order_type="MARKET", fees=_f(d.get("fee")), leverage=_f(d.get("leverage")),
            protection={k: v for k, v in protection.items() if k in ("sl_order_id", "tp_order_id", "status")},
            capital=dict(d.get("capital") or {}), detail=str(getattr(res, "error", "") or "")[:300],
        )
        if raw in _FILLED_STATUSES and bool(getattr(res, "success", False)):
            if filled is None or filled <= 0:
                # broker-authoritative quantity unknown: never assume the planned quantity filled
                return EntryResult(status=X.SUBMIT_UNKNOWN.value, reason_codes=("FILL_QUANTITY_UNRESOLVED",), **common)
            partial = requested is not None and filled < requested * (1 - 1e-9)
            return EntryResult(status=X.PARTIALLY_FILLED.value if partial else X.FILLED.value,
                               reason_codes=("PARTIAL_FILL",) if partial else (), **common)
        status, family = _STATUS_MAP.get(raw, (X.REJECTED.value, F.OTHER.value))
        code = d.get("reason_code") or raw
        return EntryResult(status=status, rejection_family=family, reason_codes=(str(getattr(code, "value", code)),),
                           **common)

    # -- orders / fills -------------------------------------------------------------------------------
    def query_order(self, venue_symbol: str, *, broker_order_id: Optional[str] = None,
                    client_order_id: Optional[str] = None) -> OrderState:
        from app.execution.fill_resolution import _query_order

        view = _query_order(self.executor.client, venue_symbol, broker_order_id, client_order_id)
        if view is None:
            return OrderState(broker_order_id, client_order_id, None, 0.0, 0.0, False)
        return OrderState(view.order_id, view.client_order_id, view.status, float(view.executed_qty),
                          float(view.avg_price), True)

    def cancel_order(self, venue_symbol: str, broker_order_id: str) -> bool:
        return bool(self.executor.client.cancel_order(venue_symbol, broker_order_id))

    def resolve_fill(self, venue_symbol: str, *, order_response: Any, client_order_id: Optional[str] = None) -> Any:
        from app.execution.fill_resolution import resolve_order_fill

        return resolve_order_fill(self.executor.client, symbol=venue_symbol, order_response=order_response,
                                  client_order_id=client_order_id,
                                  sleep=getattr(self.executor, "_fill_resolution_sleep", time.sleep))

    def reconcile_position(self, venue_symbol: str) -> BrokerPositionState:
        try:
            info = self.executor.client.get_position_info(venue_symbol)
        except Exception:
            return BrokerPositionState(venue_symbol, "FLAT", 0.0, None, False)
        if isinstance(info, dict):
            amt, entry = float(info.get("positionAmt", 0.0) or 0.0), _f(info.get("entryPrice"))
        else:
            amt = float(getattr(info, "quantity", 0.0) or 0.0)
            if str(getattr(info, "side", "")).upper() == "SELL":
                amt = -amt
            entry = _f(getattr(info, "entry_price", None))
        side = "LONG" if amt > 0 else "SHORT" if amt < 0 else "FLAT"
        return BrokerPositionState(venue_symbol, side, abs(amt), entry, True)

    # -- protection / exits ----------------------------------------------------------------------------
    def submit_protection(self, venue_symbol: str, *, side: str, quantity: float, stop_price: float,
                          target_price: Optional[float]) -> Dict[str, Any]:
        return self.executor.ensure_protection(venue_symbol, signal="BUY" if side == "LONG" else "SELL",
                                               qty=float(quantity), sl_price=float(stop_price),
                                               tp_price=float(target_price or 0.0), repair_source="CALLER")

    def modify_protection(self, venue_symbol: str, *, side: str, quantity: float, existing_stop: float,
                          new_stop: float, old_sl_order_id: Optional[str] = None,
                          old_tp_order_id: Optional[str] = None, target_price: Optional[float] = None,
                          **_kwargs: Any) -> Dict[str, Any]:
        assert_not_widening(side, existing_stop, new_stop)  # P7: refused BEFORE any broker call
        from app.models.unified_trading import ProtectionUpdateRequest

        req = ProtectionUpdateRequest(symbol=venue_symbol, position_side=side, new_sl_price=float(new_stop),
                                      new_tp_price=target_price, qty=float(quantity), old_sl_order_id=old_sl_order_id,
                                      old_tp_order_id=old_tp_order_id, reduce_only=True, reason="CATI_TIGHTEN")
        out = self.executor.client.update_protection(req)
        return out if isinstance(out, dict) else getattr(out, "model_dump", lambda: {"status": str(out)})()

    def submit_reduce(self, venue_symbol: str, *, side: str, fraction: float, live_qty: float = 0.0,
                      sl_price: float = 0.0, tp_price: float = 0.0, sl_order_id: Optional[str] = None,
                      tp_order_id: Optional[str] = None, **_kwargs: Any) -> Dict[str, Any]:
        if not 0.0 < float(fraction) < 1.0:
            raise ValueError("reduce fraction must satisfy 0 < fraction < 1")
        return self.executor.execute_tp1_partial_close(
            venue_symbol, float(live_qty), side, float(sl_price), float(tp_price), sl_order_id=sl_order_id,
            tp_order_id=tp_order_id, tp1_fraction=float(fraction), position_manager=self.position_manager)

    def submit_exit(self, venue_symbol: str, *, side: str, quantity: Optional[float] = None) -> Dict[str, Any]:
        res = self.executor.execute_signal(venue_symbol, "CLOSE", 0.0, position_side=side,
                                           remaining_quantity=quantity)
        return {"status": getattr(res, "status", None), "success": bool(getattr(res, "success", False)),
                "avg_price": getattr(res, "avg_price", None)}


__all__ = ["BinanceExecutionAdapter"]
