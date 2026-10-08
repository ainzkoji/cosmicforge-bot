"""Durable submission envelope for the existing native close-position legs.

An absent open order is not proof that an ambiguous CREATE failed. Such a leg
stays unknown and cannot be recreated or amended automatically.

Bounded retry (audit: one failed leg CREATE used to block the leg forever):
every leg is a ``closePosition`` conditional order -- it can only close the
position it protects, never open or increase one, and the venue refuses a
second closePosition leg of the same kind and direction (-4130). A new attempt
(``<base>-r<n>``, at most ``MAX_PROTECTION_ATTEMPTS`` per leg) is made only
when the previous one is PROVEN absent:

* the venue refused the CREATE (HTTP 4xx + definitive error code), or
* the CREATE was never acknowledged, the venue's open-order list does not hold
  it, and ``ABSENT_RESOLUTION_MS`` has passed since it was sent.

A leg the venue ACKNOWLEDGED and that is no longer open is still never
recreated here: it was triggered or cancelled, and the caller's fail-safe
close decides.
"""
from hashlib import sha256
import json
import time
from decimal import Decimal

from .production_close import ABSENT_RESOLUTION_MS, attempt_client_id, operator_alert

#: Total CREATE attempts that may ever be sent for one protection leg.
MAX_PROTECTION_ATTEMPTS = 3


class _AcknowledgedLegMissing(Exception):
    """Internal signal: a leg the venue acknowledged is not in ONE successful
    read of the open list. ``place_native_protection`` confirms with a second
    read before calling the protection absent."""

    def __init__(self, client_id):
        super().__init__(client_id)
        self.client_id = client_id


def open_protection_legs(client, symbol):
    """The venue's open conditional orders for ``symbol``, or
    ``PROTECTION_READ_UNAVAILABLE`` (state UNKNOWN) when the venue did not
    answer with a list: a transport failure, 5xx, rate limit or malformed body
    is never read as "no protection"."""
    try:
        orders = client.get_algo_orders(symbol, raise_on_error=True)
    except Exception as exc:
        raise ValueError("PROTECTION_READ_UNAVAILABLE") from exc
    if not isinstance(orders, list):
        raise ValueError("PROTECTION_READ_UNAVAILABLE")
    return orders


def _leg_ids(base):
    return [attempt_client_id(base, n) for n in range(MAX_PROTECTION_ATTEMPTS)]


def _set_response(db, account, cid, response, only_unanswered=False):
    with db.connect() as c:
        c.execute("UPDATE cati_production_protection SET response=? WHERE account_id=? AND client_id=?"
                  + (" AND response IS NULL" if only_unanswered else ""), (json.dumps(response), account, cid))


def _next_leg_attempt(db, account, symbol, candidates, now):
    """The client id for the next CREATE of a leg that is not open at the
    broker, or PROTECTION_SUBMIT_OUTCOME_UNKNOWN while that cannot be decided."""
    with db.connect() as c:
        rows = {r["client_id"]: r for r in c.execute(
            "SELECT client_id,document,response FROM cati_production_protection WHERE account_id=? AND client_id IN (%s)"
            % ",".join("?" for _ in candidates), (account, *candidates))}
    for cid in candidates:
        row = rows.get(cid)
        if row is None:
            return cid
        response = json.loads(row["response"]) if row["response"] else None
        if response is None:
            # Sent, never acknowledged, not in the venue's open list.
            document = json.loads(row["document"])
            sent = document.get("_requested_at")
            if sent is None:
                # A row from before attempts were timed: start its clock now.
                document["_requested_at"] = now
                with db.connect() as c:
                    c.execute("UPDATE cati_production_protection SET document=? WHERE account_id=? AND client_id=? "
                              "AND response IS NULL", (json.dumps(document, sort_keys=True), account, cid))
                raise ValueError("PROTECTION_SUBMIT_OUTCOME_UNKNOWN")
            if now - int(sent) < ABSENT_RESOLUTION_MS:
                raise ValueError("PROTECTION_SUBMIT_OUTCOME_UNKNOWN")
            # Past the venue's receive window it can no longer be created.
            _set_response(db, account, cid, {"_absent": True, "resolved_at": now}, only_unanswered=True)
            continue
        if response.get("_rejected") or response.get("_absent"):
            continue
        # Acknowledged by the venue and no longer open: never blindly recreated.
        # One read is not proof; the caller confirms with a second read.
        raise _AcknowledgedLegMissing(cid)
    # Every attempt the venue could have received is proven absent by its own
    # successful reads: protection is CONFIRMED absent and cannot be created.
    operator_alert(db, account, symbol, candidates[0], {"leg_client_ids": list(candidates),
                   "reason": "PROTECTION_LEG_ATTEMPTS_EXHAUSTED"})
    raise ValueError("PROTECTION_LEG_ATTEMPTS_EXHAUSTED")


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
        candidates = _leg_ids("CFP" + sha256((account + identity + json.dumps(params, sort_keys=True)).encode()).hexdigest()[:28])
        # Every replay reads broker truth before considering any CREATE. A read
        # the venue did not answer is PROTECTION_READ_UNAVAILABLE (unknown),
        # never an empty list.
        orders = open_protection_legs(client, request.symbol)
        found = next((o for o in orders if o.get("clientAlgoId") in candidates), None)
        if found is not None and (str(found.get("symbol")) != request.symbol or found.get("side") != exit_side
                or found.get("type", found.get("orderType")) != kind
                or Decimal(str(found.get("triggerPrice", found.get("stopPrice", 0)))) != Decimal(normalized)
                or str(found.get("closePosition", "")).lower() != "true"):
            raise ValueError("PROTECTION_READ_BACK_GEOMETRY_MISMATCH")
        now = int(time.time() * 1000)
        # No attempt of this leg is open: the first id never used, or -- only
        # when every earlier attempt is proven absent -- the next retry id.
        if found is not None:
            cid = found["clientAlgoId"]
        else:
            try:
                cid = _next_leg_attempt(db, account, request.symbol, candidates, now)
            except _AcknowledgedLegMissing as missing:
                # The venue acknowledged this leg and one read no longer lists
                # it. A second successful read is the confirmation: still
                # missing means triggered or cancelled at the venue -- the
                # protection is CONFIRMED absent and is never recreated here;
                # the caller's fail-safe close decides. Present after all
                # (propagation) means it is the live leg.
                getattr(client, '_production_protection_readback_sleep', time.sleep)(1)
                again = open_protection_legs(client, request.symbol)
                found = next((o for o in again if o.get("clientAlgoId") == missing.client_id), None)
                if found is None:
                    raise ValueError("PROTECTION_CONFIRMED_ABSENT") from None
                cid = found["clientAlgoId"]
        params["clientAlgoId"] = cid
        expected.append((leg, cid, kind, normalized))
        if found is None:
            with db.connect() as c:
                inserted = c.execute("INSERT OR IGNORE INTO cati_production_protection VALUES(?,?,?,NULL)",
                    (account, cid, json.dumps({**params, "_requested_at": now}, sort_keys=True))).rowcount
            if not inserted:
                raise ValueError("PROTECTION_SUBMIT_OUTCOME_UNKNOWN")
            # Any exception or malformed response leaves durable unknown ownership,
            # except a definitive venue refusal: that CREATE does not exist, and
            # recording it is what lets a later call try the next attempt id.
            try:
                found = client._signed_post("/fapi/v1/algoOrder", params=params)
            except Exception as exc:
                from .fill_resolution import definitive_rejection
                code = definitive_rejection(exc)
                if code is not None:
                    _set_response(db, account, cid, {"_rejected": True, "venue_code": code, "resolved_at": now},
                                  only_unanswered=True)
                raise
            if isinstance(found, dict) and found.get("orderId") == "DUPLICATE_4130":
                # The transport's marker for venue code -4130: another closePosition
                # leg of this kind already exists, so THIS CREATE was refused.
                _set_response(db, account, cid, {"_rejected": True, "venue_code": -4130, "resolved_at": now},
                              only_unanswered=True)
                raise ValueError("PROTECTION_SUBMIT_OUTCOME_UNKNOWN")
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
        confirmed = open_protection_legs(client, request.symbol)
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
    now = int(time.time()*1000)
    for row in rows:
        params = json.loads(row['document'])
        cid = params.pop('clientAlgoId',None)
        # Bookkeeping stored beside the venue parameters is not part of the id.
        sent = params.pop('_requested_at',None)
        expected = 'CFP'+sha256((account+identity+json.dumps(params,sort_keys=True)).encode()).hexdigest()[:28]
        if cid not in _leg_ids(expected):
            continue
        symbol = params['symbol']
        if float(client.get_position_amt(symbol)) != 0:
            raise ValueError('PROTECTION_CANCEL_REQUIRES_FLAT_POSITION')
        orders = open_protection_legs(client, symbol)
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
            if any(o.get('clientAlgoId')==cid for o in open_protection_legs(client, symbol)):
                raise ValueError('PROTECTION_CANCEL_READ_BACK_UNCONFIRMED')
        elif response is None:
            # An unacknowledged CREATE may still appear later. No false release
            # until it provably cannot: the position is flat (checked above), the
            # venue's open list does not hold the leg, and the venue's receive
            # window for the request has long passed. Nothing is submitted here.
            if sent is None:
                # A row from before attempts were timed: start its clock now.
                with db.connect() as c:
                    c.execute('UPDATE cati_production_protection SET document=? WHERE account_id=? AND client_id=? '
                              'AND response IS NULL',(json.dumps({**params,'clientAlgoId':cid,'_requested_at':now},
                              sort_keys=True),account,cid))
                raise ValueError('PROTECTION_SUBMIT_OUTCOME_UNKNOWN')
            if now - int(sent) < ABSENT_RESOLUTION_MS:
                raise ValueError('PROTECTION_SUBMIT_OUTCOME_UNKNOWN')
            response = {'_absent':True,'resolved_at':now}
        with db.connect() as c:
            c.execute('UPDATE cati_production_protection SET response=? WHERE account_id=? AND client_id=?',
                      (json.dumps({**response,'_closed_flat':True}),account,cid))
        cancelled.append(cid)
    return cancelled
