"""The call-scoped permit the CATI boundary issues after governance and hard risk, for executor unit tests.

Since the CATI boundary became the sole entry authority, ``BinanceExecutor.execute_signal`` refuses every entry
that is not made inside ``entry_permit.boundary_entry_permit`` (``CATI_BOUNDARY_PERMIT_REQUIRED``). In production
the permit is issued only by ``CATIExecutionBoundary`` around ``adapter.submit_entry``, which reaches the
executor through ``binance_adapter``. Tests of what the executor does ONCE an entry is permitted (sizing,
account affordability, the capital ledger, fill resolution, slots, error mapping) supply that permit here, as
``tests/test_paper_executor.py`` and ``tests/trading_intelligence/test_step3_execution_tail.py`` do. Nothing here
changes the executor or the gate: a call without this helper is still refused, and
``tests/trading_intelligence/test_cati_sole_runtime.py`` proves it.
"""
from __future__ import annotations

import uuid
from types import SimpleNamespace

from app.trading_intelligence.execution.entry_permit import boundary_entry_permit


def permitted_entry(executor, symbol, signal, usdt, **kwargs):
    """``executor.execute_signal(symbol, signal, usdt, **kwargs)`` as the boundary makes it: inside a permit for
    exactly this symbol, direction, notional and intent (a fresh intent per call, as each plan has its own)."""
    identity = kwargs.pop("intent_identity", None) or f"test-intent-{uuid.uuid4().hex}"
    request = SimpleNamespace(risk_decision_id="test-risk-decision", trade_plan_id="test-trade-plan",
                              trade_plan_hash="test-plan-hash", venue_symbol=symbol,
                              side="LONG" if str(signal).upper() == "BUY" else "SHORT", notional=usdt,
                              intent_identity=identity)
    with boundary_entry_permit(request):
        return executor.execute_signal(symbol, signal, usdt, intent_identity=identity, **kwargs)
