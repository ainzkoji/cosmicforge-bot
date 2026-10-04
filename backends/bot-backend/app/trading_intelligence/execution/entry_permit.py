"""Internal call-scoped entry permit, issued only after CATI governance/risk.

An id string or environment flag cannot authorize the legacy executor. The
permit exists only during the boundary's synchronous adapter submission and
is reset even on an unknown result. Exit/protection/reconciliation need none.
"""
from contextlib import contextmanager
from contextvars import ContextVar

_PERMIT = ContextVar("cati_entry_permit", default=None)


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


def transport_entry_permitted(client, symbol, side):
    """Production broker CREATE must still be inside the risk-approved boundary."""
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
