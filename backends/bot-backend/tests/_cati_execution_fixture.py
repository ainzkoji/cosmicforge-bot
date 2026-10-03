"""Approved CATI submission fixture for executor safety unit tests.

Production permits are issued by the CATI boundary only. These tests start
AFTER governance/hard risk to exercise protection, capital and unknown results;
separate boundary and sole-runtime tests prove unpermitted submissions fail.
"""
from types import SimpleNamespace
from app.trading_intelligence.execution.entry_permit import boundary_entry_permit


def approved_cati_executor(executor):
    original = executor.execute_signal
    def submit(symbol, signal, notional, *args, **kwargs):
        if signal == "CLOSE":
            return original(symbol, signal, notional, *args, **kwargs)
        identity = kwargs.setdefault("intent_identity", "fixture-plan|fixture-hash")
        request = SimpleNamespace(risk_decision_id="fixture-approved-risk", trade_plan_id="fixture-plan",
                                  trade_plan_hash="fixture-hash", venue_symbol=symbol,
                                  side="LONG" if signal == "BUY" else "SHORT",
                                  notional=notional, intent_identity=identity)
        with boundary_entry_permit(request):
            return original(symbol, signal, notional, *args, **kwargs)
    executor.execute_signal = submit
    return executor
