"""Durable submission envelope for the existing native close-position legs.

An absent open order is not proof that an ambiguous CREATE failed. Such a leg
stays unknown and cannot be recreated or amended automatically.
"""
from hashlib import sha256
import json
import time
from decimal import Decimal


def place_native_protection(client, request):
    from app.models.unified_trading import ProtectionResult, Side
    from app.exchange.binance.filters import normalize_protection_price
    # Transport remains the final gate as well; do not create an intent when off.
    from shared_lib.core.production import order_submission_gate
    gate = order_submission_gate(client.broker_environment)
    if not gate["enabled"]:
        raise ValueError(gate["reason"])
    db = getattr(client, "_production_db", None)
    account = getattr(client, "_production_account_id", None)
    if db is None or not account:
        raise ValueError("PRODUCTION_PROTECTION_ACCOUNT_SCOPE_REQUIRED")
    from app.trading_intelligence.integration.residual_prospective import owner_current
    if not owner_current(db):
        raise ValueError("CANONICAL_RUNTIME_LEASE_REQUIRED")
    instrument = client.get_instrument(request.symbol)
    if instrument is None or not instrument.tick_size:
        raise ValueError("PROTECTION_PRECISION_UNKNOWN")
    tick = float(instrument.tick_size)
    side = "LONG" if request.position_side == Side.BUY else "SHORT"
    exit_side = "SELL" if side == "LONG" else "BUY"
    with db.connect() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS cati_production_protection (
            account_id TEXT, client_id TEXT, document TEXT NOT NULL, response TEXT,
            PRIMARY KEY(account_id,client_id))""")
    result = ProtectionResult(status="initiated")
    expected = []
    for leg, price, kind in (("SL", request.sl_price, "STOP_MARKET"), ("TP", request.tp_price, "TAKE_PROFIT_MARKET")):
        if not price or Decimal(str(price)) <= 0:
            raise ValueError("FROZEN_PROTECTION_GEOMETRY_REQUIRED")
        normalized = normalize_protection_price(price, tick, side, leg)
        params = {"algoType": "CONDITIONAL", "symbol": request.symbol, "side": exit_side, "type": kind,
                  "triggerPrice": normalized, "closePosition": "true", "workingType": "CONTRACT_PRICE"}
        identity = getattr(client, "_production_intent_identity", None)
        if not identity:
            raise ValueError("PROTECTION_ENTRY_LINEAGE_REQUIRED")
        cid = "CFP" + sha256((account + identity + json.dumps(params, sort_keys=True)).encode()).hexdigest()[:28]
        params["clientAlgoId"] = cid
        expected.append((leg, cid, kind, normalized))
        # Every replay reads broker truth before considering any CREATE.
        orders = client.get_algo_orders(request.symbol, raise_on_error=True)
        if not isinstance(orders, list):
            raise ValueError("PROTECTION_READ_BACK_UNAVAILABLE")
        found = next((o for o in orders if o.get("clientAlgoId") == cid), None)
        if found is not None and (str(found.get("symbol")) != request.symbol or found.get("side") != exit_side
                or found.get("type", found.get("orderType")) != kind
                or Decimal(str(found.get("triggerPrice", found.get("stopPrice", 0)))) != Decimal(normalized)
                or str(found.get("closePosition", "")).lower() != "true"):
            raise ValueError("PROTECTION_READ_BACK_GEOMETRY_MISMATCH")
        if found is None:
            with db.connect() as c:
                inserted = c.execute("INSERT OR IGNORE INTO cati_production_protection VALUES(?,?,?,NULL)",
                    (account, cid, json.dumps(params, sort_keys=True))).rowcount
            if not inserted:
                raise ValueError("PROTECTION_SUBMIT_OUTCOME_UNKNOWN")
            # Any exception or malformed response leaves durable unknown ownership.
            found = client._signed_post("/fapi/v1/algoOrder", params=params)
            if not isinstance(found, dict) or not found.get("algoId"):
                raise ValueError("PROTECTION_SUBMIT_OUTCOME_UNKNOWN")
        # Also resolve a previously ambiguous CREATE when read-back found it.
        with db.connect() as c:
            c.execute("UPDATE cati_production_protection SET response=? WHERE account_id=? AND client_id=?",
                      (json.dumps(found), account, cid))
        oid = found.get("algoId")
        if not oid:
            raise ValueError("PROTECTION_READ_BACK_UNAVAILABLE")
        setattr(result, "sl_order_id" if leg == "SL" else "tp_order_id", str(oid))
    # An acknowledgement alone is not a healthy protected position.
    # Binance may acknowledge before the open-algo view propagates. Retry only
    # the read for a bounded interval; never repeat either protection CREATE.
    confirmed = []
    for delay in (0, 1, 1, 2, 2):
        if delay:
            getattr(client,'_production_protection_readback_sleep',time.sleep)(delay)
        confirmed = client.get_algo_orders(request.symbol, raise_on_error=True)
        if not isinstance(confirmed, list):
            raise ValueError("PROTECTION_READ_BACK_UNAVAILABLE")
        if all(any(o.get('clientAlgoId') == cid for o in confirmed) for _,cid,_,_ in expected):
            break
    for leg, cid, kind, price in expected:
        found = next((o for o in confirmed if o.get('clientAlgoId') == cid), None)
        if not found or found.get('symbol') != request.symbol or found.get('side') != exit_side \
                or found.get('type', found.get('orderType')) != kind \
                or str(found.get('closePosition')).lower() != 'true' \
                or Decimal(str(found.get('triggerPrice', found.get('stopPrice',0)))) != Decimal(price):
            raise ValueError("PROTECTION_READ_BACK_UNCONFIRMED")
    result.status = "success"
    return result


def cancel_flat_protection(client, identity):
    """Cancel only this entry's native legs after broker-confirmed flatness."""
    db, account = client._production_db, client._production_account_id
    with db.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='cati_production_protection'").fetchone():
            return []
        rows = [dict(r) for r in c.execute('SELECT * FROM cati_production_protection WHERE account_id=?',(account,))]
    cancelled = []
    for row in rows:
        params = json.loads(row['document'])
        cid = params.pop('clientAlgoId',None)
        expected = 'CFP'+sha256((account+identity+json.dumps(params,sort_keys=True)).encode()).hexdigest()[:28]
        if cid != expected:
            continue
        symbol = params['symbol']
        if float(client.get_position_amt(symbol)) != 0:
            raise ValueError('PROTECTION_CANCEL_REQUIRES_FLAT_POSITION')
        orders = client.get_algo_orders(symbol,raise_on_error=True)
        found = next((o for o in orders if o.get('clientAlgoId') == cid),None)
        response = json.loads(row['response']) if row['response'] else None
        if found:
            if response and response.get('_cancel_requested'):
                raise ValueError('PROTECTION_CANCEL_OUTCOME_UNKNOWN')
            response = {**found,'_cancel_requested':True}
            with db.connect() as c:
                c.execute('UPDATE cati_production_protection SET response=? WHERE account_id=? AND client_id=?',
                          (json.dumps(response),account,cid))
            client._signed_delete('/fapi/v1/algoOrder',params={'symbol':symbol,'algoId':found['algoId']})
            if any(o.get('clientAlgoId')==cid for o in client.get_algo_orders(symbol,raise_on_error=True)):
                raise ValueError('PROTECTION_CANCEL_READ_BACK_UNCONFIRMED')
        elif response is None:
            # An unacknowledged CREATE may still appear later. No false release.
            raise ValueError('PROTECTION_SUBMIT_OUTCOME_UNKNOWN')
        with db.connect() as c:
            c.execute('UPDATE cati_production_protection SET response=? WHERE account_id=? AND client_id=?',
                      (json.dumps({**response,'_closed_flat':True}),account,cid))
        cancelled.append(cid)
    return cancelled
