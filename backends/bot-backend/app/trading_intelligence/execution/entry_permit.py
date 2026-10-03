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
