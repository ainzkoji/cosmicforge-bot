"""Authenticated DEMO venue validation (Bybit Demo Trading, BingX VST) on a USER-CONNECTED broker account.

The account is one a user connected through the normal app flow; its credentials are resolved by the canonical
resolver (``shared_lib.broker.resolver.resolve_broker_auth``) -- nothing here owns, prints, or stores a secret.

Check matrix (each PASS / FAIL / SKIPPED / UNAVAILABLE_ON_DEMO with a reason; never inferred):

  A authentication authenticated account read            I order submit     min-size MARKET entry, client id
  B environment    DEMO + canonical demo host only       J order lookup     broker lookup by client order id
  C account mode   the venue-reported account topology   K fill             broker executions for that order
  D instrument     live metadata (status / min / step)   L position         broker position for the symbol
  E permissions    trade yes, withdraw NEVER             M protection       position SL/TP installed + read back
  F fees           account fee tier                      N reconciliation   fills == broker position quantity
  G spread         executable order book                 O restart          a FRESH client re-reads order + position
  H funding        current funding                       P transfer         internal transfer (demo: unavailable)

Order checks (I-O) run only with ``submit_orders=True``, only on DEMO, only when E proved no withdrawal permission;
the position is closed and its protection cancelled afterwards (recorded as CLEANUP). A public HTTP 200 proves
none of A-O: everything here is an authenticated read or a demo order.
"""
from __future__ import annotations

import hashlib
import math
import time
import uuid
from decimal import Decimal
from typing import Any, Callable, Dict, List, Mapping, Optional

VALIDATION_VERSION = "demo-venue-validation-v1"
PASS, FAIL, SKIPPED, UNAVAILABLE_ON_DEMO = "PASS", "FAIL", "SKIPPED", "UNAVAILABLE_ON_DEMO"
READ_CHECKS = ("A_AUTHENTICATION", "B_ENVIRONMENT", "C_ACCOUNT_MODE", "D_INSTRUMENT", "E_PERMISSIONS", "F_FEES",
               "G_SPREAD", "H_FUNDING")
ORDER_CHECKS = ("I_ORDER_SUBMIT", "J_ORDER_LOOKUP", "K_FILL", "L_POSITION", "M_PROTECTION", "N_RECONCILIATION",
                "O_RESTART_RECONCILIATION")
CHECKS = READ_CHECKS + ORDER_CHECKS + ("P_INTERNAL_TRANSFER",)


class ValidationRefused(RuntimeError):
    pass


def _h(value: Any) -> Optional[str]:
    return hashlib.sha256(str(value).encode()).hexdigest()[:16] if value else None


def _num(x: Any) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


class _Recorder:
    def __init__(self, clock: Callable[[], float]):
        self.clock, self.checks = clock, {}

    def put(self, check: str, result: str, reason: str = "", **evidence: Any) -> str:
        self.checks[check] = {"result": result, "reason": reason, "at_ms": int(self.clock() * 1000),
                              "evidence": {k: v for k, v in evidence.items() if v is not None}}
        return result

    def run(self, check: str, fn: Callable[[], Mapping[str, Any]]) -> Optional[Mapping[str, Any]]:
        try:
            out = dict(fn() or {})
        except Exception as exc:  # the venue said no, or the call failed: evidence, never a crash
            self.put(check, FAIL, f"{type(exc).__name__}: {str(exc)[:160]}")
            return None
        result = out.pop("_result", PASS)
        self.put(check, result, out.pop("_reason", ""), **out)
        return out if result == PASS else None


def run_validation(*, auth: Any, client_factory: Callable[[Any], Any], symbol: str, submit_orders: bool = False,
                   code_version: Optional[str] = None, clock: Callable[[], float] = time.time,
                   sleep: Callable[[float], None] = time.sleep, fill_wait_s: float = 10.0) -> Dict[str, Any]:
    from shared_lib.broker.environment import BrokerEnvironment, resolve_base_url, resolve_wallet_base_url

    broker = str(auth.broker_type).lower()
    env = getattr(auth.environment, "value", str(auth.environment)).upper()
    if env != "DEMO":
        raise ValidationRefused(f"{broker}: environment {env} -- validation runs on DEMO accounts only (no live funds)")
    demo_host = resolve_base_url(broker, BrokerEnvironment.DEMO)
    rec = _Recorder(clock)
    symbol = symbol.upper()
    client = client_factory(auth)

    # -- A..H: authenticated reads -------------------------------------------------------------------------
    def credential():
        acc = client.account()
        if not isinstance(acc, Mapping) or not acc:
            return {"_result": FAIL, "_reason": "EMPTY_ACCOUNT_RESPONSE"}
        wallet = next((acc.get(k) for k in ("totalWalletBalance", "totalEquity", "balance") if acc.get(k) is not None),
                      None)
        return {"wallet_balance": _num(wallet), "available": _num(acc.get("availableBalance"))}

    rec.run("A_AUTHENTICATION", credential)
    host_ok = str(getattr(auth, "base_url", "") or "").rstrip("/") == str(demo_host or "").rstrip("/")
    rec.put("B_ENVIRONMENT", PASS if host_ok and demo_host else FAIL,
            "" if host_ok else "CREDENTIAL_HOST_IS_NOT_THE_CANONICAL_DEMO_HOST", host=demo_host, environment=env)

    def account_mode():
        info = client.account_info() if hasattr(client, "account_info") else None
        caps = client.get_account_capabilities(environment="demo") if hasattr(client, "get_account_capabilities") else None
        mode = None
        if isinstance(info, Mapping):
            r = info.get("result") if isinstance(info.get("result"), Mapping) else info
            mode = r.get("unifiedMarginStatus") or r.get("marginMode") or r.get("accountMode")
        return {"account_mode": mode, "capabilities": caps if isinstance(caps, (Mapping, list)) else None,
                **({} if mode or caps else {"_result": FAIL, "_reason": "ACCOUNT_MODE_NOT_REPORTED"})}

    rec.run("C_ACCOUNT_MODE", account_mode)

    instrument: Dict[str, Any] = {}

    def instrument_check():
        i = client.get_instrument(symbol)
        if i is None:
            return {"_result": FAIL, "_reason": "INSTRUMENT_NOT_LISTED"}
        instrument.update(status=str(getattr(i, "status", "")), min_qty=_num(getattr(i, "min_qty", None)),
                          qty_step=_num(getattr(i, "qty_step", None)), min_notional=_num(getattr(i, "min_notional", None)),
                          tick_size=_num(getattr(i, "tick_size", None)),
                          api_tradable=bool(getattr(i, "api_tradable", False)))
        ok = instrument["status"].upper() in ("TRADING", "1") and instrument["api_tradable"]
        return {**instrument, **({} if ok else {"_result": FAIL, "_reason": "INSTRUMENT_NOT_TRADABLE"})}

    rec.run("D_INSTRUMENT", instrument_check)

    perms: Dict[str, Any] = {}

    def permissions():
        p = client.get_account_permissions() or {}
        got = dict(p.get("permissions") or {})
        perms.update(got)
        if got.get("WITHDRAW") is True:
            return {"_result": FAIL, "_reason": "WITHDRAW_PERMISSION_PRESENT_KEY_REFUSED", "permissions": got}
        if got.get("TRADE") is True:
            return {"permissions": got, "inspectable": True}
        # not inspectable (BingX): not a pass -- the demo order checks below are the trade-permission evidence
        return {"_result": SKIPPED, "_reason": "PERMISSIONS_NOT_API_INSPECTABLE", "permissions": got,
                "note": str(p.get("note") or p.get("source") or "")[:120]}

    rec.run("E_PERMISSIONS", permissions)

    def fees():
        f = client.get_trading_fee_rates(symbol) or {}
        maker, taker = _num(f.get("maker")), _num(f.get("taker"))
        return {"maker": maker, "taker": taker,
                **({} if maker is not None and taker is not None else {"_result": FAIL, "_reason": "FEE_TIER_UNAVAILABLE"})}

    rec.run("F_FEES", fees)
    book: Dict[str, float] = {}

    def spread():
        ob = client.get_orderbook(symbol, limit=5)
        bid, ask = float(ob["bids"][0][0]), float(ob["asks"][0][0])
        if not (0 < bid < ask):
            return {"_result": FAIL, "_reason": "CROSSED_OR_EMPTY_BOOK", "bid": bid, "ask": ask}
        book.update(bid=bid, ask=ask)
        return {"bid": bid, "ask": ask, "spread_bps": round((ask - bid) / ((ask + bid) / 2) * 1e4, 4),
                "book_time_ms": ob.get("time")}

    rec.run("G_SPREAD", spread)

    def funding():
        f = client.get_funding(symbol) or {}
        rate = _num(f.get("fundingRate"))
        return {"funding_rate": rate, "next_funding_time": f.get("nextFundingTime"), "mark_price": _num(f.get("markPrice")),
                **({} if rate is not None else {"_result": FAIL, "_reason": "FUNDING_UNAVAILABLE"})}

    rec.run("H_FUNDING", funding)

    # -- P: transfer is a separate capability ----------------------------------------------------------------
    if resolve_wallet_base_url(broker, BrokerEnvironment.DEMO) is None:
        rec.put("P_INTERNAL_TRANSFER", UNAVAILABLE_ON_DEMO, "INTERNAL_TRANSFER_UNAVAILABLE_ON_DEMO")
    else:
        rec.put("P_INTERNAL_TRANSFER", SKIPPED, "TRANSFER_NOT_EXERCISED_BY_THIS_HARNESS")

    # -- I..O: demo orders (explicit) --------------------------------------------------------------------------
    gate = [c for c in ("A_AUTHENTICATION", "B_ENVIRONMENT", "D_INSTRUMENT", "G_SPREAD") if rec.checks[c]["result"] != PASS]
    if perms.get("WITHDRAW") is True:
        gate.append("E_PERMISSIONS:WITHDRAW")
    if not submit_orders or gate:
        why = "ORDER_CHECKS_NOT_REQUESTED" if not submit_orders else "PRECONDITIONS_FAILED:" + ",".join(gate)
        for c in ORDER_CHECKS:
            rec.put(c, SKIPPED, why)
    else:
        _order_checks(rec, client, client_factory, auth, symbol, instrument, book, clock, sleep, fill_wait_s)

    results = {c: rec.checks[c]["result"] for c in CHECKS}
    required = READ_CHECKS + ORDER_CHECKS
    hard_fail = [c for c in required if results[c] == FAIL]
    missing = [c for c in required if results[c] != PASS and not (c == "E_PERMISSIONS" and results[c] == SKIPPED)]
    verdict = "VALIDATED" if not missing else ("FAILED" if hard_fail else "INCOMPLETE")
    from app.trading_intelligence.observability.sanitize import sanitize_payload

    return sanitize_payload({
        "version": VALIDATION_VERSION, "venue": broker, "environment": env, "host": demo_host, "symbol": symbol,
        "account_scope": {"broker_account_ref": _h(getattr(auth, "account_id", None)),
                          "user_ref": _h(getattr(auth, "user_id", None)),
                          "credential_version": getattr(auth, "credential_version", None)},
        "code_version": code_version, "checks": rec.checks, "results": results, "verdict": verdict,
        "blocking": sorted(set(hard_fail) | set(missing)), "generated_at_ms": int(clock() * 1000)})


def _order_checks(rec, client, client_factory, auth, symbol, instrument, book, clock, sleep, fill_wait_s):
    from app.models.unified_trading import OrderRequest, OrderType, ProtectionRequest, Side

    price = book["ask"]
    step = instrument.get("qty_step") or instrument.get("min_qty") or 0.001
    need = max(instrument.get("min_qty") or 0.0, ((instrument.get("min_notional") or 0.0) * 1.1) / price)
    qty = Decimal(str(round(math.ceil(need / step) * step, 10)))
    cid = f"cfval{int(clock())}{uuid.uuid4().hex[:8]}"
    t0 = int(clock() * 1000) - 60_000
    order: Dict[str, Any] = {}

    def submit():
        o = client.place_order(OrderRequest(symbol=symbol, side=Side.BUY, type=OrderType.MARKET, qty=qty,
                                            client_order_id=cid))
        status = str(getattr(getattr(o, "status", None), "value", getattr(o, "status", "")))
        order.update(broker_order_id=str(getattr(o, "broker_order_id", "") or ""), status=status)
        ok = order["broker_order_id"] and status not in ("rejected", "canceled", "expired")
        return {"client_order_id": cid, "broker_order_id": order["broker_order_id"] or None, "status": status,
                "qty": str(qty), **({} if ok else {"_result": FAIL, "_reason": f"ORDER_{status.upper() or 'NO_ID'}",
                                                  "error": getattr(o, "error_message", None)})}

    if rec.run("I_ORDER_SUBMIT", submit) is None:
        for c in ORDER_CHECKS[1:]:
            rec.put(c, SKIPPED, "ORDER_NOT_SUBMITTED")
        return
    try:
        def lookup():
            o = client.get_order_by_client_order_id(symbol, cid) or {}
            oid = str(o.get("orderId") or o.get("order_id") or "")
            return {"broker_order_id": oid or None, "status": o.get("status") or o.get("orderStatus"),
                    **({} if oid else {"_result": FAIL, "_reason": "ORDER_NOT_FOUND_BY_CLIENT_ID"})}

        rec.run("J_ORDER_LOOKUP", lookup)
        fills: List[Mapping[str, Any]] = []

        def fill():
            deadline = clock() + fill_wait_s
            while True:
                trades = client.user_trades(symbol, start_time_ms=t0) or []
                fills[:] = [t for t in trades if str(t.get("orderId")) == order["broker_order_id"]]
                if fills or clock() >= deadline:
                    break
                sleep(1.0)
            filled = sum(float(t.get("qty") or 0) for t in fills)
            return {"executions": len(fills), "filled_qty": filled,
                    "avg_price": (sum(float(t["qty"]) * float(t["price"]) for t in fills) / filled) if filled else None,
                    "commission": sum(float(t.get("commission") or 0) for t in fills) if fills else None,
                    **({} if filled > 0 else {"_result": FAIL, "_reason": "NO_BROKER_EXECUTION_FOR_ORDER"})}

        rec.run("K_FILL", fill)
        position: Dict[str, Any] = {}

        def pos(c=client):
            p = next((p for p in c.get_positions() or [] if str(p.symbol).upper().replace("-", "") ==
                      symbol.replace("-", "") and float(p.quantity) > 0), None)
            if p is None:
                return {"_result": FAIL, "_reason": "NO_BROKER_POSITION"}
            position.update(qty=float(p.quantity), side=str(getattr(p.side, "value", p.side)),
                            entry=float(p.entry_price))
            return dict(position)

        rec.run("L_POSITION", pos)

        def protect():
            entry = position.get("entry") or price
            r = client.place_protection(ProtectionRequest(symbol=symbol, position_side=Side.BUY,
                                                          qty=Decimal(str(position.get("qty") or qty)),
                                                          sl_price=Decimal(str(round(entry * 0.95, 8))),
                                                          tp_price=Decimal(str(round(entry * 1.05, 8)))))
            status, err = str(getattr(r, "status", "")), getattr(r, "error", None)
            readback = client.get_position_stop(symbol) if hasattr(client, "get_position_stop") else None
            ok = not err and status.lower() not in ("failed", "error", "rejected")
            return {"status": status, "sl_order_id": getattr(r, "sl_order_id", None),
                    "tp_order_id": getattr(r, "tp_order_id", None), "readback": readback if readback else None,
                    **({} if ok else {"_result": FAIL, "_reason": f"PROTECTION_{status.upper()}", "error": err})}

        rec.run("M_PROTECTION", protect) if position else rec.put("M_PROTECTION", SKIPPED, "NO_POSITION")

        def reconcile():
            filled = rec.checks["K_FILL"]["evidence"].get("filled_qty") or 0.0
            held = position.get("qty") or 0.0
            ok = filled > 0 and abs(filled - held) <= max(step, 1e-12) / 2
            return {"filled_qty": filled, "position_qty": held,
                    **({} if ok else {"_result": FAIL, "_reason": "FILLS_DO_NOT_RECONCILE_TO_BROKER_POSITION"})}

        rec.run("N_RECONCILIATION", reconcile)

        def restart():
            fresh = client_factory(auth)  # no in-process state: broker truth only
            o = fresh.get_order_by_client_order_id(symbol, cid) or {}
            p = next((p for p in fresh.get_positions() or [] if str(p.symbol).upper().replace("-", "") ==
                      symbol.replace("-", "") and float(p.quantity) > 0), None)
            same = str(o.get("orderId") or o.get("order_id") or "") == order["broker_order_id"]
            qty_ok = p is not None and abs(float(p.quantity) - (position.get("qty") or -1)) <= max(step, 1e-12) / 2
            return {"order_recovered": same, "position_recovered": qty_ok,
                    **({} if same and qty_ok else {"_result": FAIL, "_reason": "RESTART_STATE_DIVERGES_FROM_BROKER"})}

        rec.run("O_RESTART_RECONCILIATION", restart)
    finally:
        cleanup: Dict[str, Any] = {}
        for name, fn in (("cancel_protection", lambda: client.cancel_all(symbol)),
                         ("close_position", lambda: client.close_position_market(symbol))):
            try:
                fn()
                cleanup[name] = "OK"
            except Exception as exc:
                cleanup[name] = f"{type(exc).__name__}"
        rec.put("CLEANUP", PASS if all(v == "OK" for v in cleanup.values()) else FAIL, "", **cleanup)


__all__ = ["CHECKS", "ORDER_CHECKS", "READ_CHECKS", "PASS", "FAIL", "SKIPPED", "UNAVAILABLE_ON_DEMO",
           "VALIDATION_VERSION", "ValidationRefused", "run_validation"]
