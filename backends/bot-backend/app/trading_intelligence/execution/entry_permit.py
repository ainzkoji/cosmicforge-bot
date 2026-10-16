"""Internal call-scoped entry permit, issued only after CATI governance/risk.

An id string or environment flag cannot authorize the legacy executor. The
permit exists only during the boundary's synchronous adapter submission and
is reset even on an unknown result. Exit/protection/reconciliation need none.
"""
from contextlib import contextmanager
from contextvars import ContextVar

_PERMIT = ContextVar("cati_entry_permit", default=None)
_PORTFOLIO_PERMIT = ContextVar("cati_portfolio_entry_permit", default=None)
_CERTIFICATION_PERMIT = ContextVar("cati_certification_order_permit", default=None)


def _order_shape_matches(request, payload):
    import math
    if str(payload.get("type", payload.get("orderType", ""))).upper() != request.order_type:
        return False
    if request.price is not None:
        try:
            return math.isclose(float(payload.get("price")), float(request.price), rel_tol=1e-9)
        except (TypeError, ValueError):
            return False
    return True


@contextmanager
def certification_order_permit(db, run_id, permit, request):
    """Infrastructure-only authority for the runner's exact journalled order."""
    token = _CERTIFICATION_PERMIT.set((db, run_id, permit, request))
    try:
        yield
    finally:
        _CERTIFICATION_PERMIT.reset(token)


def _certification_transport_permitted(client, symbol, side, payload):
    import json
    import math
    import time
    from app.exchange.platform_adapter.certification import OperatorPermit
    from shared_lib.broker.environment import normalize_environment
    from shared_lib.persistence.cati_exchange_schema import CERT_ACTION_TABLE, CERT_EVENT_TABLE
    claim = _CERTIFICATION_PERMIT.get()
    if claim is None or client is None or not isinstance(payload, dict):
        return False
    db, run_id, permit, request = claim
    if not _order_shape_matches(request, payload):
        return False
    if not isinstance(permit, OperatorPermit) or not permit.allow_orders or permit.environment != "DEMO" \
            or not permit.issued_at_ms <= int(time.time() * 1000) < permit.expires_at_ms:
        return False
    if getattr(client, "_production_db", None) is not db or getattr(client, "_broker_account_id", None) != permit.account_ref:
        return False
    if normalize_environment(client.broker_environment).value.upper() != permit.environment:
        return False
    cid = payload.get("newClientOrderId") or payload.get("orderLinkId") or payload.get("clientOrderID")
    try:
        if not (cid == request.client_order_id and str(cid).startswith("cert")
                and request.symbol == permit.symbol == str(symbol).upper().replace("-", "")
                and request.side == str(side).upper()
                and math.isclose(float(payload.get("quantity", payload.get("qty"))), float(request.quantity), rel_tol=1e-9)):
            return False
    except (TypeError, ValueError):
        return False
    with db.connect() as conn:
        start = conn.execute(f"SELECT * FROM {CERT_EVENT_TABLE} WHERE run_id=? AND event_type='RUN_STARTED'", (run_id,)).fetchone()
        done = conn.execute(f"SELECT 1 FROM {CERT_EVENT_TABLE} WHERE run_id=? AND event_type='RUN_COMPLETED'", (run_id,)).fetchone()
        action = conn.execute(f"SELECT phase,document FROM {CERT_ACTION_TABLE} WHERE run_id=? AND client_order_id=? "
                              "AND action='PLACE_ORDER' ORDER BY seq DESC LIMIT 1", (run_id, cid)).fetchone()
    if start is None or done or action is None or action["phase"] != "INTENT":
        return False
    record, intended = json.loads(start["document"]), json.loads(action["document"])
    if (start["account_ref"], start["venue"], start["environment"], start["operator_ref"]) != (
            permit.account_ref, permit.venue, permit.environment, permit.operator_ref):
        return False
    if record["owner_authorization_ref"] != permit.owner_authorization_ref or record["max_notional"] != permit.max_notional \
            or record["permit_expires_at_ms"] != permit.expires_at_ms or not record["allow_orders"]:
        return False
    if intended["side"] != request.side or intended["quantity"] != request.quantity:
        return False
    from app.trading_intelligence.integration.residual_prospective import owner_current
    return bool(owner_current(db))


@contextmanager
def portfolio_entry_permit(db, intent_id, request):
    """Scope a raw CREATE to the already persisted portfolio authorization.

    The transport re-reads these records immediately before sending; a caller
    cannot turn an id, a flag or an unsubmitted reservation into permission.
    """
    token = _PORTFOLIO_PERMIT.set((db, intent_id, request))
    try:
        yield
    finally:
        _PORTFOLIO_PERMIT.reset(token)


def _portfolio_transport_permitted(client, symbol, side, payload):
    import math
    claim = _PORTFOLIO_PERMIT.get()
    if claim is None or client is None or not isinstance(payload, dict):
        return False
    db, intent_id, request = claim
    if not _order_shape_matches(request, payload):
        return False
    if getattr(client, "_production_db", None) is not db:
        return False
    account = getattr(client, "_broker_account_id", None)
    user = getattr(client, "_broker_user_id", None)
    if account != getattr(client, "_production_account_id", None):
        return False
    with db.connect() as conn:
        row = conn.execute(
            "SELECT i.*,r.state AS reservation_state,r.authorization_hash,r.quantity AS reserved_quantity "
            "FROM cati_execution_intents i JOIN cati_reservation_ledger r ON r.reservation_id=i.reservation_id "
            "AND r.account_id=i.account_id AND r.decision_id=i.decision_id AND r.family_id=i.family_id "
            "AND r.symbol=i.symbol AND r.user_id=i.user_id AND r.mode=i.environment "
            "WHERE i.intent_id=? AND i.account_id=? AND i.user_id=?",
            (intent_id, account, user)).fetchone()
    if row is None or row["state"] != "SUBMITTING" or not row["increases_risk"] or row["reduce_only"] \
            or row["reservation_state"] not in ("SUBMITTED", "FILLED") or not row["authorization_hash"]:
        return False
    from shared_lib.broker.environment import normalize_environment
    try:
        if normalize_environment(client.broker_environment).value.upper() != row["environment"]:
            return False
    except (AttributeError, ValueError):
        return False
    cid = payload.get("newClientOrderId") or payload.get("orderLinkId") or payload.get("clientOrderID")
    quantity = payload.get("quantity", payload.get("qty"))
    try:
        size_ok = (math.isclose(float(quantity), float(row["quantity"]), rel_tol=1e-9, abs_tol=1e-12)
                   and float(quantity) <= float(row["reserved_quantity"]) * (1 + 1e-9))
    except (TypeError, ValueError):
        return False
    from app.trading_intelligence.integration.residual_prospective import owner_current
    return bool(owner_current(db) and size_ok and cid == row["client_order_id"] == request.client_order_id
                and row["symbol"] == request.symbol == str(symbol or "").upper().replace("-", "")
                and row["side"] == request.side == str(side or "").upper())


@contextmanager
def boundary_entry_permit(request):
    if not request.risk_decision_id or not request.trade_plan_id or not request.trade_plan_hash:
        raise ValueError("CATI governance and hard-risk evidence required")
    token = _PERMIT.set(request)
    try:
        yield
    finally:
        _PERMIT.reset(token)


def entry_permitted(symbol, signal, notional, intent_identity):
    request = _PERMIT.get()
    return bool(request is not None and request.venue_symbol == symbol
                and ("BUY" if request.side == "LONG" else "SELL") == str(signal).upper()
                and request.intent_identity == intent_identity
                and abs(float(request.notional)-float(notional)) <= 1e-8*max(1, float(request.notional)))


def transport_entry_permitted(client, symbol, side, *, payload=None):
    """Production broker CREATE must still be inside the risk-approved boundary."""
    if _PORTFOLIO_PERMIT.get() is not None:
        return _portfolio_transport_permitted(client, symbol, side, payload)
    if _CERTIFICATION_PERMIT.get() is not None:
        return _certification_transport_permitted(client, symbol, side, payload)
    request = _PERMIT.get()
    if client is None or request is None:
        return False
    db = getattr(client, "_production_db", None)
    account = getattr(client, "_production_account_id", None)
    if db is None or not account or getattr(client, "_broker_account_id", None) != account:
        return False
    with db.connect() as c:
        intent = c.execute("SELECT risk_decision_id,user_id,status FROM cati_execution_attempts "
            "WHERE broker_account_id=? AND trade_plan_id=? ORDER BY sequence DESC LIMIT 1",
            (account, request.trade_plan_id)).fetchone()
    if intent is None or intent["status"] != "PENDING_SUBMIT" or intent["risk_decision_id"] != request.risk_decision_id \
            or intent["user_id"] != getattr(client, "_broker_user_id", None):
        return False
    from app.trading_intelligence.integration.residual_prospective import owner_current
    return bool(owner_current(db) and request.intent_identity == getattr(client, "_production_intent_identity", None)
                and request.venue_symbol == str(symbol or "").upper().replace("-", "")
                and ("BUY" if request.side == "LONG" else "SELL") == str(side or "").upper())
