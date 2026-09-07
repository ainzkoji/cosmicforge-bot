"""A finalized decision must never claim it had no reason.

Observed inconsistency in ``decision_traces``:

    gate_reason  = ERROR_STRATEGY_UNAVAILABLE
    reason_codes = NONE

and in ``canonical_trade_decisions``:

    primary_reason_code        = ERROR_STRATEGY_UNAVAILABLE
    secondary_reason_codes_json = ["NONE"]

``record_gate`` set only ``gate_reason``; ``reason_codes`` kept its column
default sentinel, which then leaked into the secondary-reason list.
"""
from __future__ import annotations

import pytest

from shared_lib.persistence.trace_recorder import DecisionTrace, TraceRecorder


@pytest.fixture
def recorder(tmp_path):
    """In-memory-lifecycle recorder: these tests assert on the in-flight trace,
    before finalize() touches the database."""
    return TraceRecorder(db_path=str(tmp_path / "trace.db"))


def _start(recorder: TraceRecorder) -> str:
    return recorder.start_trace(
        run_id="run-1", cycle_id="cycle-1", symbol="BTCUSDT",
        account_id="acct-1", environment="broker", timeframe="15m",
        bot_instance_id="bot-1", user_id="user-1", effective_policy_hash="hash-1",
    )


def test_gate_reason_populates_reason_codes(recorder):
    trace_id = _start(recorder)
    recorder.record_gate(
        trace_id, allowed=False,
        reason_code="ERROR_STRATEGY_UNAVAILABLE",
        reason="Auto Pilot requires the orchestrated strategy path",
        details={},
    )
    trace = recorder._traces[trace_id]
    assert trace.gate_reason == "ERROR_STRATEGY_UNAVAILABLE"
    assert trace.reason_codes == "ERROR_STRATEGY_UNAVAILABLE", (
        "reason_codes must not stay at the NONE sentinel when a gate reason exists"
    )


def test_gate_does_not_overwrite_an_existing_reason_code(recorder):
    trace_id = _start(recorder)
    recorder._traces[trace_id].reason_codes = "LOW_CONFIDENCE"
    recorder.record_gate(trace_id, allowed=False, reason_code="RISK_BLOCKED", reason="r")
    assert recorder._traces[trace_id].reason_codes == "LOW_CONFIDENCE"
    assert recorder._traces[trace_id].gate_reason == "RISK_BLOCKED"


@pytest.mark.parametrize(
    "reason_codes,primary,expected_secondary",
    [
        ("NONE", "ERROR_STRATEGY_UNAVAILABLE", []),
        ("", "ERROR_STRATEGY_UNAVAILABLE", []),
        ("ERROR_STRATEGY_UNAVAILABLE", "ERROR_STRATEGY_UNAVAILABLE", []),
        ("LOW_CONFIDENCE,NONE,HTF_OPPOSED", "RISK_BLOCKED", ["LOW_CONFIDENCE", "HTF_OPPOSED"]),
    ],
)
def test_none_sentinel_never_becomes_a_secondary_reason(reason_codes, primary, expected_secondary):
    """Mirrors the derivation inside finalize()."""
    secondary = [
        x for x in (part.strip() for part in str(reason_codes or "").split(","))
        if x and x.upper() != "NONE" and x != primary
    ]
    assert secondary == expected_secondary


def test_default_trace_still_starts_from_the_none_sentinel():
    """The column default is unchanged; only its leakage into reasons is fixed."""
    trace = DecisionTrace(trace_id="t", run_id="r", cycle_id="c", symbol="BTCUSDT", ts="now")
    assert trace.reason_codes == "NONE"


# ── The canonical ledger holds decisions, not heartbeats ─────────────────────


def test_heartbeat_reasons_are_declared_and_exclude_real_decisions():
    from shared_lib.persistence.trace_recorder import _HEARTBEAT_ONLY_REASONS

    assert "NO_NEW_CANDLE" in _HEARTBEAT_ONLY_REASONS
    # Anything that represents an actual evaluation outcome must still be logged.
    for real_reason in (
        "ERROR_STRATEGY_UNAVAILABLE", "LOW_CONFIDENCE", "SIGNAL_HOLD",
        "RISK_BLOCKED", "EVENT_BLOCKED", "NO_OPPORTUNITY",
    ):
        assert real_reason not in _HEARTBEAT_ONLY_REASONS


def test_same_candle_heartbeat_is_traced_but_not_recorded_as_a_decision(tmp_path):
    """The 10s heartbeat must not manufacture one 'decision' per tick.

    Regression guard: with a 15m timeframe and a 10s loop this wrote ~17k rows a
    day for two symbols and pushed every real evaluation out of the diagnostics
    window that reads the newest 5,000 canonical decisions.
    """
    from shared_lib.persistence.db import DB
    from shared_lib.persistence.migrations import migrate
    from shared_lib.persistence.trace_recorder import TraceRecorder

    db_path = tmp_path / "trace.db"
    db = DB(path=str(db_path))
    # decision_traces gains several columns via ALTER migrations, so a bare
    # CREATE-only database cannot accept a real trace insert.
    migrate(str(db_path))
    rec = TraceRecorder(db_path=str(db_path))

    def _run(reason_code: str, state_change: str) -> str:
        trace_id = rec.start_trace(
            run_id="run-1", cycle_id="cycle-1", symbol="BTCUSDT",
            account_id="acct-1", environment="broker", timeframe="15m",
            bot_instance_id="bot-1", user_id="user-1", effective_policy_hash="h",
        )
        rec.record_gate(trace_id, allowed=False, reason_code=reason_code, reason=reason_code)
        rec.finalize(trace_id, state_change=state_change, final_position="NONE")
        return trace_id

    heartbeat_id = _run("NO_NEW_CANDLE", "NO_NEW_CANDLE")
    decision_id = _run("LOW_CONFIDENCE", "HOLD")

    with db.connect() as conn:
        traced = {r[0] for r in conn.execute("SELECT trace_id FROM decision_traces")}
        canonical = {r[0] for r in conn.execute("SELECT decision_id FROM canonical_trade_decisions")}

    # Full detail is retained for both.
    assert {heartbeat_id, decision_id} <= traced
    # Only the real evaluation becomes a canonical decision.
    assert decision_id in canonical
    assert heartbeat_id not in canonical
