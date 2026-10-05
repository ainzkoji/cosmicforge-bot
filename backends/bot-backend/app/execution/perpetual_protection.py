"""Durable broker-native close-only protection for Bybit and BingX.

BingX conditional orders do not accept client IDs. A local intent owns each
leg; broker ID and exact geometry read-back resolve it. Missing evidence after
an uncertain create remains UNKNOWN and can never trigger another create.
"""
import hashlib
import json
from decimal import Decimal


def place_protection(client, request, *, broker):
    from app.models.unified_trading import ProtectionResult, Side
    from app.exchange.binance.filters import normalize_protection_price
    from shared_lib.core.production import require_execution_account, order_submission_gate
    from app.trading_intelligence.integration.residual_prospective import owner_current
    db = getattr(client, "_production_db", None)
    account = getattr(client, "_production_account_id", None)
    identity = getattr(client, "_production_intent_identity", None)
    if db is None or not account or not identity or getattr(client, "_broker_account_id", None) != account:
        raise ValueError("PRODUCTION_PROTECTION_ACCOUNT_SCOPE_REQUIRED")
    require_execution_account(client.broker_environment, broker, client.base_url)
    if not order_submission_gate(client.broker_environment)["enabled"]:
        raise ValueError(order_submission_gate(client.broker_environment)["reason"])
    if not owner_current(db):
        raise ValueError("CANONICAL_RUNTIME_LEASE_REQUIRED")
    side = "LONG" if request.position_side == Side.BUY else "SHORT"
    exit_side = "SELL" if side == "LONG" else "BUY"
    positions = [p for p in client.position_risk(request.symbol) if Decimal(str(p["positionAmt"])) != 0]
    if len(positions) != 1 or (Decimal(str(positions[0]["positionAmt"])) > 0) != (side == "LONG"):
        raise ValueError("PROTECTION_POSITION_MISMATCH")
    if Decimal(str(request.qty)) <= 0 or Decimal(str(request.qty)) > abs(Decimal(str(positions[0]["positionAmt"]))):
        raise ValueError("PROTECTION_QUANTITY_MISMATCH")
    instrument = client.get_instrument(request.symbol)
    if instrument is None or not instrument.tick_size:
        raise ValueError("PROTECTION_PRECISION_UNKNOWN")
    result = ProtectionResult(status="initiated")
    with db.connect() as c:
        c.execute("CREATE TABLE IF NOT EXISTS cati_production_protection(account_id TEXT,client_id TEXT,document TEXT NOT NULL,response TEXT,PRIMARY KEY(account_id,client_id))")
    for leg, price, kind in (("SL", request.sl_price, "STOP_MARKET"), ("TP", request.tp_price, "TAKE_PROFIT_MARKET")):
        if not price or Decimal(str(price)) <= 0:
            raise ValueError("FROZEN_PROTECTION_GEOMETRY_REQUIRED")
        px = normalize_protection_price(price, float(instrument.tick_size), side, leg)
        document = {"broker": broker, "environment": str(getattr(client.broker_environment, "value", client.broker_environment)),
                    "symbol": request.symbol, "side": exit_side, "type": kind, "stopPrice": px,
                    "quantity": client._fmt_qty(request.symbol, request.qty)}
        cid = "cfp" + hashlib.sha256((account+identity+json.dumps(document, sort_keys=True)).encode()).hexdigest()[:28]
        orders = client.open_orders(request.symbol)
        if not isinstance(orders, list):
            raise ValueError("PROTECTION_READ_BACK_UNAVAILABLE")
        def matches(o):
            return (str(o.get("symbol", "")).upper().replace("-", "") == request.symbol.upper().replace("-", "")
                and o.get("side") == exit_side and o.get("type") == kind
                and Decimal(str(o.get("stopPrice") or 0)) == Decimal(px)
                and (o.get("reduceOnly") is True or o.get("closePosition") is True))
        with db.connect() as c:
            prior = c.execute("SELECT document,response FROM cati_production_protection WHERE account_id=? AND client_id=?", (account, cid)).fetchone()
        oid = json.loads(prior["response"]).get("orderId") if prior and prior["response"] else None
        candidates = [o for o in orders if (o.get("clientOrderId") == cid if broker == "bybit" else matches(o))]
        if len(candidates) > 1:
            raise ValueError("PROTECTION_READ_BACK_AMBIGUOUS")
        found = candidates[0] if candidates else None
        if found and (not matches(found) or (oid and str(found.get("orderId")) != str(oid))):
            raise ValueError("PROTECTION_READ_BACK_GEOMETRY_MISMATCH")
        if found is None:
            if prior:
                raise ValueError("PROTECTION_SUBMIT_OUTCOME_UNKNOWN")
            with db.connect() as c:
                inserted = c.execute("INSERT OR IGNORE INTO cati_production_protection VALUES(?,?,?,NULL)", (account, cid, json.dumps(document, sort_keys=True))).rowcount
            if not inserted:
                raise ValueError("PROTECTION_SUBMIT_OUTCOME_UNKNOWN")
            if broker == "bybit":
                payload = {"category": "linear", "symbol": request.symbol.upper(), "side": exit_side.capitalize(),
                    "orderType": "Market", "qty": "0", "triggerPrice": px,
                    "triggerDirection": (2 if side == "LONG" else 1) if leg == "SL" else (1 if side == "LONG" else 2),
                    "triggerBy": "LastPrice", "reduceOnly": True, "closeOnTrigger": True, "positionIdx": 0, "orderLinkId": cid}
                response = client._ok(client._request_v5("POST", "/v5/order/create", payload), "protection")
            else:
                payload = {"symbol": client._normalize_symbol(request.symbol), "side": exit_side, "positionSide": "BOTH",
                    "type": kind, "quantity": document["quantity"], "stopPrice": px, "closePosition": "true", "workingType": "CONTRACT_PRICE"}
                data = client._request("POST", "/openApi/swap/v2/trade/order", payload)["data"]
                response = data.get("order", data)
            if not isinstance(response, dict) or not response.get("orderId"):
                raise ValueError("PROTECTION_SUBMIT_OUTCOME_UNKNOWN")
            with db.connect() as c:
                c.execute("UPDATE cati_production_protection SET response=? WHERE account_id=? AND client_id=?", (json.dumps({"orderId": str(response["orderId"])}), account, cid))
            # An acknowledgement alone does not certify protective geometry.
            found = client.get_order(request.symbol, response["orderId"])
            if not found or not matches(found):
                raise ValueError("PROTECTION_READ_BACK_GEOMETRY_MISMATCH")
        if not found.get("orderId"):
            raise ValueError("PROTECTION_READ_BACK_UNAVAILABLE")
        setattr(result, "sl_order_id" if leg == "SL" else "tp_order_id", str(found["orderId"]))
    result.status = "success"
    return result
