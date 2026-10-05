"""Explicit local-operator DEMO certification, outside strategy performance.

Executed by the canonical lease owner. Durable CREATE ownership precedes the
network call. Restart reads the deterministic ID; uncertain results never retry.
"""
from contextvars import ContextVar
from decimal import Decimal, ROUND_UP
import hashlib
import json
import time

_permit = ContextVar("demo_transport_smoke", default=None)


def lookup(client, symbol, cid):
    try:
        return client.get_order_by_client_order_id(symbol, cid)
    except RuntimeError as exc:
        # The venue's exact not-found code proves only this lookup was absent;
        # a durable pending CREATE still stays unknown and is never retried.
        message = str(exc)
        if message.startswith("Binance HTTP 400: "):
            try:
                if json.loads(message.split(": ", 1)[1]).get("code") == -2013:
                    return None
            except (ValueError, AttributeError):
                pass
        raise


def permitted(client, payload):
    scope = _permit.get()
    if scope is None or client is not scope[0]:
        return False
    from shared_lib.broker.environment import normalize_environment, resolve_base_url
    from app.trading_intelligence.integration.residual_prospective import owner_current
    return (normalize_environment(client.broker_environment).value == "demo"
        and client.base_url.rstrip("/") == resolve_base_url("binance", normalize_environment("demo"))
        and owner_current(scope[1]) and payload.get("newClientOrderId") == scope[2]
        and payload.get("symbol") == scope[3] and payload.get("side") == "BUY"
        and payload.get("type") == "MARKET")


def run(db, account_id, *, run_id="closure-43178689", symbol="ADAUSDT"):
    from shared_lib.broker.resolver import resolve_broker_auth
    from shared_lib.broker.client_factory import build_client_from_auth
    from shared_lib.broker.environment import normalize_environment
    from shared_lib.broker.auto_trading import authorization
    from shared_lib.core.production import order_submission_gate
    from app.trading_intelligence.integration.residual_prospective import owner_current
    from app.trading_intelligence.integration.production_execution import account_risk, persisted_risk_controls
    from app.models.unified_trading import ProtectionRequest, Side
    if not owner_current(db):
        raise ValueError("CANONICAL_RUNTIME_LEASE_REQUIRED")
    with db.connect() as c:
        account = dict(c.execute("SELECT * FROM broker_accounts WHERE id=?", (account_id,)).fetchone())
        bots = [dict(r) for r in c.execute("SELECT * FROM bot_instances WHERE broker_account_id=? AND status='active'", (account_id,))]
        auto = authorization(c, account, bots)
        c.execute("CREATE TABLE IF NOT EXISTS demo_transport_smoke(id TEXT PRIMARY KEY,account_id TEXT NOT NULL,environment TEXT NOT NULL,status TEXT NOT NULL,document TEXT NOT NULL)")
    if account["broker_id"] != "binance" or normalize_environment(account["environment"]).value != "demo":
        raise ValueError("DEMO_SMOKE_REQUIRES_BINANCE_DEMO")
    if not auto["enabled"] or not order_submission_gate("demo")["enabled"]:
        raise ValueError("DEMO_SMOKE_NOT_AUTHORIZED")
    cid = "CFSMOKE" + hashlib.sha256((account_id+run_id+symbol).encode()).hexdigest()[:24]
    auth = resolve_broker_auth(account_id, account["user_id"], db)
    client = build_client_from_auth(auth)
    client._production_db, client._production_account_id = db, account_id
    client._production_intent_identity = "TESTNET_SMOKE:" + cid
    with db.connect() as c:
        prior = c.execute("SELECT status,document FROM demo_transport_smoke WHERE id=?", (cid,)).fetchone()
    if prior and prior["status"] == "COMPLETED":
        return json.loads(prior["document"])
    now = int(time.time()*1000)
    if prior:
        report = json.loads(prior["document"])
        order = lookup(client, symbol, cid)
        if not order or str(order.get("status")) != "FILLED":
            raise ValueError("DEMO_SMOKE_CREATE_OUTCOME_UNKNOWN")
    else:
        positions, orders = client.position_risk(), client.open_orders()
        if client.get_algo_orders(symbol, raise_on_error=True):
            raise ValueError("DEMO_SMOKE_EXISTING_PROTECTION")
        risk = account_risk(db, account, client, positions, orders, bots, now)
        if risk["reason"] or risk["loss_latched"]:
            raise ValueError("DEMO_SMOKE_RISK_BLOCKED")
        controls = persisted_risk_controls(db, bots[0]["id"], risk, now)
        if (controls["kill_switch"] or controls["consec_loss_day_paused"]
                or controls["consec_loss_cooldown_until_ms"] > now
                or any(controls[p+"_drawdown_pct"] >= controls["max_"+p+"_drawdown_pct"]
                       for p in ("weekly", "monthly") if controls["max_"+p+"_drawdown_pct"] > 0)):
            raise ValueError("DEMO_SMOKE_PERSISTED_RISK_BLOCKED")
        # Governance kill controls apply to smoke too.
        from app.trading_intelligence.governance.promotion import PromotionGovernance
        gov = PromotionGovernance(db)
        if gov.kill_switch_on(scope=account_id):
            raise ValueError("DEMO_SMOKE_KILL_SWITCH")
        filters = client.get_symbol_filters(symbol)
        step = Decimal(str(filters.step_size))
        price = Decimal(str(client.last_price(symbol)))
        minimum = max(Decimal(str(filters.min_notional or 0)), Decimal("5")) * Decimal("1.02")
        qty = max(Decimal(str(filters.min_qty)), (minimum/price/step).to_integral_value(rounding=ROUND_UP)*step)
        if qty*price > Decimal(str(risk["free_capital"])) or float(qty*price*Decimal(".01")) > risk["remaining_daily_risk"]:
            raise ValueError("DEMO_SMOKE_MARGIN_OR_DAILY_RISK")
        report = {"classification": "TESTNET_SMOKE", "account_id": account_id, "environment": "DEMO",
                  "client_order_id": cid, "symbol": symbol, "quantity": str(qty), "started_at": now}
        with db.connect() as c:
            inserted = c.execute("INSERT OR IGNORE INTO demo_transport_smoke VALUES(?,?,'DEMO','CREATE_PENDING',?)", (cid, account_id, json.dumps(report))).rowcount
        if not inserted:
            raise ValueError("DEMO_SMOKE_CREATE_OUTCOME_UNKNOWN")
        token = _permit.set((client, db, cid, symbol))
        try:
            client._signed_post("/fapi/v1/order", params={"symbol": symbol, "side": "BUY", "type": "MARKET", "quantity": str(qty), "newClientOrderId": cid})
        finally:
            _permit.reset(token)
        order = lookup(client, symbol, cid)
    if not order or order.get("status") != "FILLED" or float(order.get("executedQty", 0)) <= 0:
        raise ValueError("DEMO_SMOKE_FILL_UNCONFIRMED")
    report["order"] = {k: order.get(k) for k in ("orderId", "clientOrderId", "status", "executedQty", "avgPrice")}
    fills = [f for f in client.user_trades(symbol, start_time_ms=report["started_at"], end_time_ms=int(time.time()*1000), limit=1000) if str(f["orderId"]) == str(order["orderId"])]
    if not fills:
        raise ValueError("DEMO_SMOKE_FILLS_UNCONFIRMED")
    report["fill_count"] = len(fills)
    close_id = cid + "C"
    close_order = lookup(client, symbol, close_id)
    if prior and prior["status"] == "CLOSE_PENDING" and close_order and close_order.get("status") == "FILLED":
        if float(client.get_position_amt(symbol)) != 0:
            raise ValueError("DEMO_SMOKE_CLOSE_UNCONFIRMED")
        return finish(db, client, symbol, cid, report)
    quantity = Decimal(str(order["executedQty"]))
    px = Decimal(str(order["avgPrice"]))
    position = client.get_position_info(symbol)
    if not position or Decimal(str(position["positionAmt"])) != quantity:
        raise ValueError("DEMO_SMOKE_POSITION_UNCONFIRMED")
    protection = client.place_protection(ProtectionRequest(symbol=symbol, position_side=Side.BUY, qty=quantity,
        sl_price=str(px*Decimal(".99")), tp_price=str(px*Decimal("1.02"))))
    report["protection"] = protection.model_dump()
    if protection.status != "success":
        raise ValueError("DEMO_SMOKE_PROTECTION_UNCONFIRMED")
    protective = client.get_algo_orders(symbol, raise_on_error=True)
    ids = {str(protection.sl_order_id), str(protection.tp_order_id)}
    verified = [o for o in protective if str(o.get("algoId")) in ids and o.get("side") == "SELL" and str(o.get("closePosition")).lower() == "true"]
    if len(verified) != 2:
        raise ValueError("DEMO_SMOKE_PROTECTION_READ_BACK_UNCONFIRMED")
    report["position_verified"] = True
    # Durable close identity; absence of a close acknowledgement never retries.
    if not close_order:
        with db.connect() as c:
            state = c.execute("SELECT status FROM demo_transport_smoke WHERE id=?", (cid,)).fetchone()[0]
            if state == "CLOSE_PENDING":
                raise ValueError("DEMO_SMOKE_CLOSE_OUTCOME_UNKNOWN")
            c.execute("UPDATE demo_transport_smoke SET status='CLOSE_PENDING',document=? WHERE id=?", (json.dumps(report), cid))
        client._signed_post("/fapi/v1/order", params={"symbol": symbol, "side": "SELL", "type": "MARKET", "quantity": str(quantity), "reduceOnly": "true", "newClientOrderId": close_id})
    if float(client.get_position_amt(symbol)) != 0:
        raise ValueError("DEMO_SMOKE_CLOSE_UNCONFIRMED")
    return finish(db, client, symbol, cid, report)


def finish(db, client, symbol, cid, report):
    close_order = lookup(client, symbol, cid+"C")
    if not close_order or close_order.get("status") != "FILLED":
        raise ValueError("DEMO_SMOKE_CLOSE_ACK_UNCONFIRMED")
    report["close_order"] = {k: close_order.get(k) for k in ("orderId", "clientOrderId", "status", "executedQty", "avgPrice")}
    close_fills = [f for f in client.user_trades(symbol, start_time_ms=report["started_at"], end_time_ms=int(time.time()*1000), limit=1000)
                   if str(f["orderId"]) == str(close_order["orderId"])]
    if not close_fills:
        raise ValueError("DEMO_SMOKE_CLOSE_FILLS_UNCONFIRMED")
    report["close_fill_count"] = len(close_fills)
    ids = {str(report["protection"]["sl_order_id"]), str(report["protection"]["tp_order_id"])}
    for oid in sorted(ids):
        # Cancellation cannot increase exposure; final book read certifies it.
        if any(str(o.get("algoId")) == oid for o in client.get_algo_orders(symbol, raise_on_error=True)):
            client._signed_delete("/fapi/v1/algoOrder", params={"symbol": symbol, "algoId": oid})
    report.update(final_position=0, final_open_orders=len(client.open_orders(symbol)),
                  final_protection_orders=len(client.get_algo_orders(symbol, raise_on_error=True)), status="COMPLETED")
    if report["final_open_orders"] or report["final_protection_orders"]:
        raise ValueError("DEMO_SMOKE_CLEANUP_UNCONFIRMED")
    with db.connect() as c:
        c.execute("UPDATE demo_transport_smoke SET status='COMPLETED',document=? WHERE id=?", (json.dumps(report), cid))
    return report


def process_local_request(db):
    from pathlib import Path
    request_path = Path("logs/runtime/DEMO_TRANSPORT_SMOKE.json")
    response_path = Path("logs/runtime/DEMO_TRANSPORT_SMOKE_RESULT.json")
    if not request_path.exists():
        return
    try:
        request = json.loads(request_path.read_text())
        result = run(db, request["account_id"], run_id=request.get("run_id", "closure-43178689"))
    except Exception as exc:
        code = str(exc) if isinstance(exc, ValueError) and str(exc).replace("_", "").isalnum() else type(exc).__name__
        result = {"status": "BLOCKED", "reason": code}
    response_path.write_text(json.dumps(result, default=str), encoding="utf-8")
    request_path.unlink()
