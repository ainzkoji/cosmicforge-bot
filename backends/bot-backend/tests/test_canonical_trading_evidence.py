"""Phases 9 & 11 — canonical trading evidence.

The claim under test: after this batch, *every* symbol-level evaluation leaves
exactly one finalized record, that record carries enough structured evidence to
reconstruct the decision without reading logs, and the lineage from runtime
session down to individual fills is queryable rather than inferred.
"""
from __future__ import annotations

import inspect

import pytest

from app.evidence.decision_recorder import (
    UNFINALIZED_REASON,
    TradingDecision,
    record_decision,
    record_no_new_candle,
)
from app.evidence.integrity import (
    find_approved_decisions_without_attempt,
    find_decisions_without_primary_reason,
    find_duplicate_candle_decisions,
    find_flat_positions_without_close_evidence,
    find_incomplete_decisions,
    find_mode_environment_contradictions,
    find_non_organic_in_readiness_scope,
    find_position_quantity_mismatches,
    organic_decision_count,
    run_integrity_checks,
)
from app.evidence.writers import (
    MissingAttributionError,
    complete_execution_attempt,
    open_bot_run,
    open_execution_attempt,
    open_runtime_session,
    record_bot_lifecycle_event,
    record_data_quality_event,
    record_market_snapshot,
    record_position_event,
    record_position_opened,
    record_reconciliation_event,
    record_risk_event,
    record_trading_cycle,
    update_position_quantities,
)
from app.runner.market_snapshot import MarketSnapshot
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import (
    BOT_LIFECYCLE_EVENT_TYPES,
    NON_ORGANIC_PROVENANCE,
    ORGANIC_PROVENANCE,
    PAPER_FORWARD,
    PAPER_FORWARD_VALIDATION,
    REPLAY,
    TEST_FIXTURE,
)
from shared_lib.persistence.migrations import migrate

BOT = "bot-evidence-1"
TF_MS = 15 * 60 * 1000


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    return database


def candles(n=20):
    return [
        [i * TF_MS, 100.0, 101.0, 99.0, 100.0, 10.0, i * TF_MS + TF_MS - 1]
        for i in range(1, n + 1)
    ]


def count(db, table, where="", args=()):
    with db.connect() as conn:
        sql = f"SELECT COUNT(*) FROM {table}" + (f" WHERE {where}" if where else "")
        return int(conn.execute(sql, args).fetchone()[0])


# ══════════════════════════════════════════════════════════════════════════
# Phase 9 §4/§5 — one finalized decision per evaluation, no abandoned traces
# ══════════════════════════════════════════════════════════════════════════


def test_a_normal_evaluation_finalizes_exactly_one_decision(db):
    with record_decision(db, bot_instance_id=BOT, symbol="BTCUSDT") as decision:
        decision.hold("NO_OPPORTUNITY")

    assert count(db, "trading_decisions") == 1
    assert find_incomplete_decisions(db, grace_seconds=0) == []


def test_an_early_return_still_finalizes(db):
    """The recorder finalizes on block exit, however the block was left."""
    def evaluate():
        with record_decision(db, bot_instance_id=BOT, symbol="BTCUSDT") as decision:
            decision.hold("SYMBOL_LOCK_BUSY")
            return "early"

    assert evaluate() == "early"
    with db.connect() as conn:
        row = conn.execute("SELECT primary_reason, complete FROM trading_decisions").fetchone()
    assert row["primary_reason"] == "SYMBOL_LOCK_BUSY"
    assert row["complete"] == 1


def test_an_exception_records_the_evaluation_rather_than_losing_it(db):
    with pytest.raises(RuntimeError):
        with record_decision(db, bot_instance_id=BOT, symbol="BTCUSDT") as decision:
            decision.set_snapshot(None)
            raise RuntimeError("executor blew up")

    with db.connect() as conn:
        row = conn.execute("SELECT primary_reason, final_action, complete, legacy_reason "
                           "FROM trading_decisions").fetchone()
    assert row["primary_reason"] == "EXECUTION_ERROR"
    assert row["final_action"] == "ERROR"
    assert row["complete"] == 1
    assert "executor blew up" in row["legacy_reason"]


def test_a_decision_that_forgot_its_reason_is_flagged_not_silently_accepted(db):
    with record_decision(db, bot_instance_id=BOT, symbol="BTCUSDT"):
        pass  # no reason set — a bug we want to be able to find

    with db.connect() as conn:
        row = conn.execute("SELECT primary_reason FROM trading_decisions").fetchone()
    assert row["primary_reason"] == UNFINALIZED_REASON
    assert find_decisions_without_primary_reason(db)


def test_no_decision_ever_reports_none_as_its_reason(db):
    for reason in ("NO_NEW_CANDLE", "REGIME_BLOCKED", "RISK_DAILY_LOSS_LIMIT"):
        with record_decision(db, bot_instance_id=BOT, symbol="BTCUSDT") as decision:
            decision.hold(reason)

    assert find_decisions_without_primary_reason(db) == []


@pytest.mark.parametrize(
    "reason",
    [
        "NO_NEW_CANDLE", "NO_OPPORTUNITY", "REGIME_BLOCKED", "SESSION_BLOCKED",
        "EVENT_BLACKOUT", "HTF_NOT_ALIGNED", "CONSENSUS_INSUFFICIENT",
        "ENTRY_CONFIDENCE_BELOW_THRESHOLD", "SYMBOL_LOCK_BUSY",
        "RISK_DAILY_LOSS_LIMIT", "RISK_MAX_OPEN_POSITIONS", "RISK_RR_BELOW_MINIMUM",
        "EXECUTION_DATA_STALE", "EXECUTION_MIN_NOTIONAL", "ENTRY_DUPLICATE",
        "EXECUTION_ERROR",
    ],
)
def test_every_early_return_reason_finalizes_a_record(db, reason):
    with record_decision(db, bot_instance_id=BOT, symbol="BTCUSDT") as decision:
        decision.reject(reason)

    assert count(db, "trading_decisions", "primary_reason=?", (reason,)) == 1
    assert find_incomplete_decisions(db, grace_seconds=0) == []


def test_duplicate_candle_decisions_are_detected(db):
    for _ in range(2):
        with record_decision(
            db, bot_instance_id=BOT, symbol="BTCUSDT",
            timeframe="15m", closed_candle_close_time=999,
        ) as decision:
            decision.hold("NO_OPPORTUNITY")

    # The unique index collapses the retry rather than storing two rows.
    assert count(db, "trading_decisions") == 1
    assert find_duplicate_candle_decisions(db) == []


# ══════════════════════════════════════════════════════════════════════════
# Phase 9 §7 — component evidence is preserved, not flattened to a label
# ══════════════════════════════════════════════════════════════════════════


def test_full_strategy_evidence_survives_into_the_decision(db):
    from app.decision.opportunity import build_opportunity

    opportunity = build_opportunity(
        symbol="BTCUSDT", timeframe="15m", market_snapshot_id="ms1", side="BUY",
        raw_confidence=0.62, consensus=0.62, buy_score=0.62, sell_score=0.10,
        votes=[("supertrend", "BUY", 0.62), ("donchian", "BUY", 0.58),
               ("sma", "HOLD", 0.0), ("vwap", "SELL", 0.2)],
        regime="WEAK_TREND", regime_confidence=0.71,
        component_breakdown=[{"name": "supertrend", "signal": "BUY", "confidence": 0.62}],
    )

    with record_decision(db, bot_instance_id=BOT, symbol="BTCUSDT") as decision:
        decision.set_opportunity(opportunity).hold("ENTRY_CONFIDENCE_BELOW_THRESHOLD")

    with db.connect() as conn:
        row = dict(conn.execute("SELECT * FROM trading_decisions").fetchone())

    assert row["regime"] == "WEAK_TREND"
    assert row["regime_confidence"] == pytest.approx(0.71)
    assert row["buy_score"] == pytest.approx(0.62)
    assert row["sell_score"] == pytest.approx(0.10)
    assert row["raw_confidence"] == pytest.approx(0.62)
    assert "supertrend" in row["supporting_strategies_json"]
    assert "vwap" in row["opposing_strategies_json"]
    assert "supertrend" in row["component_metadata_json"]
    assert row["opportunity_id"] == opportunity.opportunity_id


def _threshold_at(value: float):
    """An EVALUATED threshold decision fixed at ``value``.

    The quality engine no longer resolves a threshold, so evidence tests supply
    the one the threshold engine would have produced.
    """
    from app.threshold.contracts import (
        AdaptiveThresholdDecision,
        ThresholdMode,
        ThresholdStatus,
    )

    return AdaptiveThresholdDecision(
        threshold_decision_id="thr_fixture",
        bot_instance_id=BOT,
        symbol="BTCUSDT",
        timeframe="15m",
        status=ThresholdStatus.EVALUATED,
        threshold_engine_version="1.0.0",
        threshold_mode=ThresholdMode.ADAPTIVE,
        base_threshold=value,
        raw_unclamped_threshold=value,
        final_threshold=value,
        min_threshold=0.0,
        max_threshold=1.0,
    )


def test_entry_quality_evidence_survives_into_the_decision(db):
    from app.decision import TradingDecisionEngine
    from app.decision.opportunity import build_opportunity

    engine = TradingDecisionEngine()
    opportunity = build_opportunity(
        symbol="BTCUSDT", timeframe="15m", market_snapshot_id="ms1", side="BUY",
        raw_confidence=0.40, consensus=0.40, buy_score=0.40, sell_score=0.0,
    )
    quality = engine.evaluate(opportunity, threshold_decision=_threshold_at(0.55))

    with record_decision(db, bot_instance_id=BOT, symbol="BTCUSDT") as decision:
        decision.set_opportunity(opportunity).set_entry_quality(quality)
        decision.hold(quality.primary_reason)

    with db.connect() as conn:
        row = dict(conn.execute("SELECT * FROM trading_decisions").fetchone())

    assert row["quality_result"] == "FAIL"
    assert row["quality_reason"] == "ENTRY_CONFIDENCE_BELOW_THRESHOLD"
    assert row["effective_entry_threshold"] == pytest.approx(0.55)
    assert row["raw_confidence"] == pytest.approx(0.40)


def test_snapshot_lineage_binds_the_decision_to_the_market_it_saw(db):
    snapshot = MarketSnapshot.build(
        symbol="BTCUSDT", timeframe="15m", candles=candles(), source="test",
    )
    with record_decision(db, bot_instance_id=BOT, symbol="BTCUSDT") as decision:
        decision.set_snapshot(snapshot).hold("NO_OPPORTUNITY")

    with db.connect() as conn:
        row = dict(conn.execute("SELECT * FROM trading_decisions").fetchone())
    assert row["market_snapshot_id"] == snapshot.market_snapshot_id
    assert row["closed_candle_close_time"] == snapshot.latest_closed_candle_time


# ══════════════════════════════════════════════════════════════════════════
# §59 — NO_NEW_CANDLE must be cheap
# ══════════════════════════════════════════════════════════════════════════


def test_no_new_candle_records_evidence_without_a_strategy_payload(db):
    record_no_new_candle(db, bot_instance_id=BOT, symbol="BTCUSDT", timeframe="15m")

    with db.connect() as conn:
        row = dict(conn.execute("SELECT * FROM trading_decisions").fetchone())

    assert row["primary_reason"] == "NO_NEW_CANDLE"
    assert row["complete"] == 1
    # Proof the heartbeat ran — and nothing more. No component blob every 10s.
    for heavy in ("component_signals_json", "component_metadata_json",
                  "supporting_strategies_json", "opposing_strategies_json"):
        assert row[heavy] in (None, "[]", "null")
    assert row["raw_confidence"] is None


def test_repeated_heartbeats_stay_cheap(db):
    for _ in range(30):
        record_no_new_candle(db, bot_instance_id=BOT, symbol="BTCUSDT", timeframe="15m")

    with db.connect() as conn:
        sizes = [
            len(r["component_metadata_json"] or "")
            for r in conn.execute("SELECT component_metadata_json FROM trading_decisions")
        ]
    assert count(db, "trading_decisions") == 30
    assert max(sizes) == 0, "heartbeat rows must not carry component payloads"


# ══════════════════════════════════════════════════════════════════════════
# Phase 11 §25-31 — runtime lineage and execution evidence
# ══════════════════════════════════════════════════════════════════════════


def test_runtime_session_records_process_identity(db):
    session_id = open_runtime_session(
        db, database_role="development", database_path=":memory:",
        process_execution_mode="paper", environment_name="test",
    )
    with db.connect() as conn:
        row = dict(conn.execute(
            "SELECT * FROM runtime_sessions WHERE runtime_session_id=?", (session_id,)
        ).fetchone())

    assert row["pid"] > 0
    assert row["python_executable"]
    assert row["database_role"] == "development"
    assert row["status"] == "RUNNING"


def test_full_lineage_from_session_to_position_event(db):
    session_id = open_runtime_session(db, database_role="paper", database_path=":memory:")
    open_bot_run(db, run_id="run-1", bot_instance_id=BOT, runtime_session_id=session_id,
                 policy_hash="hash-1")
    record_trading_cycle(db, {"cycle_id": "cyc-1", "run_id": "run-1", "bot_instance_id": BOT})

    with record_decision(
        db, bot_instance_id=BOT, symbol="BTCUSDT", run_id="run-1", cycle_id="cyc-1",
        runtime_session_id=session_id,
    ) as decision:
        decision.approve("APPROVED_FOR_EXECUTION")
        decision_id = decision.decision_id
        attempt_id = open_execution_attempt(
            db, decision_id=decision_id, bot_instance_id=BOT, symbol="BTCUSDT",
            requested_action="BUY", run_id="run-1", cycle_id="cyc-1",
        )
        decision.execution_attempt_id = attempt_id
        decision.position_id = "pos-1"

    complete_execution_attempt(db, attempt_id, result="SUCCESS",
                               broker_order_id="ord-1", position_id="pos-1")
    record_position_opened(db, position_id="pos-1", bot_instance_id=BOT, symbol="BTCUSDT",
                           side="LONG", original_qty=1.0, entry_price=100.0,
                           run_id="run-1", decision_id=decision_id)

    with db.connect() as conn:
        joined = conn.execute(
            """SELECT d.decision_id, a.execution_attempt_id, p.position_id, r.run_id, s.runtime_session_id
               FROM trading_decisions d
               JOIN execution_attempts a ON a.decision_id = d.decision_id
               JOIN positions p ON p.position_id = a.position_id
               JOIN bot_runs r ON r.run_id = d.run_id
               JOIN runtime_sessions s ON s.runtime_session_id = r.runtime_session_id""",
        ).fetchone()

    assert joined is not None, "the full lineage must be joinable in one query"
    assert joined["runtime_session_id"] == session_id


def test_an_attempt_exists_even_when_the_broker_returns_no_order_id(db):
    """This is the gap between 'approved' and 'an order exists'."""
    attempt_id = open_execution_attempt(
        db, decision_id="dec-x", bot_instance_id=BOT, symbol="BTCUSDT", requested_action="BUY",
    )
    complete_execution_attempt(
        db, attempt_id, result="FAILED", primary_reason="EXECUTION_DATA_STALE",
        error_class="StaleData",
    )
    with db.connect() as conn:
        row = dict(conn.execute("SELECT * FROM execution_attempts").fetchone())

    assert row["result"] == "FAILED"
    assert row["broker_order_id"] is None
    assert row["primary_reason"] == "EXECUTION_DATA_STALE"


def test_approved_decision_without_an_attempt_is_an_integrity_violation(db):
    with record_decision(db, bot_instance_id=BOT, symbol="BTCUSDT") as decision:
        decision.approve("APPROVED_FOR_EXECUTION")

    violations = find_approved_decisions_without_attempt(db)
    assert len(violations) == 1


def test_position_events_are_append_only_and_ordered(db):
    record_position_opened(db, position_id="pos-1", bot_instance_id=BOT, symbol="BTCUSDT",
                           side="LONG", original_qty=1.0, entry_price=100.0)
    for event_type, qty, remaining in (
        ("TP1", 0.5, 0.5), ("BREAK_EVEN_ACTIVATED", None, 0.5),
        ("TRAILING_ACTIVATED", None, 0.5), ("FINAL_CLOSE", 0.5, 0.0),
    ):
        record_position_event(db, position_id="pos-1", bot_instance_id=BOT, symbol="BTCUSDT",
                              event_type=event_type, quantity=qty, remaining_qty=remaining)

    with db.connect() as conn:
        events = [r["event_type"] for r in conn.execute(
            "SELECT event_type FROM position_events WHERE position_id='pos-1' ORDER BY occurred_at, rowid"
        )]
    assert events == ["OPENED", "TP1", "BREAK_EVEN_ACTIVATED", "TRAILING_ACTIVATED", "FINAL_CLOSE"]


def test_position_quantity_invariant_is_enforced(db):
    record_position_opened(db, position_id="pos-1", bot_instance_id=BOT, symbol="BTCUSDT",
                           side="LONG", original_qty=1.0, entry_price=100.0)
    update_position_quantities(db, "pos-1", remaining_qty=0.5, realized_qty=0.5)
    assert find_position_quantity_mismatches(db) == []

    update_position_quantities(db, "pos-1", remaining_qty=0.7, realized_qty=0.5)
    assert len(find_position_quantity_mismatches(db)) == 1


def test_flat_position_without_close_evidence_is_flagged(db):
    record_position_opened(db, position_id="pos-1", bot_instance_id=BOT, symbol="BTCUSDT",
                           side="LONG", original_qty=1.0, entry_price=100.0)
    update_position_quantities(db, "pos-1", remaining_qty=0.0, realized_qty=1.0, status="CLOSED")

    assert len(find_flat_positions_without_close_evidence(db)) == 1

    record_position_event(db, position_id="pos-1", bot_instance_id=BOT, symbol="BTCUSDT",
                          event_type="FINAL_CLOSE", quantity=1.0, remaining_qty=0.0)
    assert find_flat_positions_without_close_evidence(db) == []


def test_reconciliation_records_the_mismatch_before_any_repair(db):
    record_reconciliation_event(
        db, bot_instance_id=BOT, symbol="BTCUSDT", expected="1.0", observed="0.5",
        action="ADOPT_BROKER", reason="QTY_DIVERGENCE", result="REPAIRED",
    )
    with db.connect() as conn:
        row = dict(conn.execute("SELECT * FROM reconciliation_events").fetchone())
    assert row["expected"] == "1.0"
    assert row["observed"] == "0.5"
    assert row["action"] == "ADOPT_BROKER"
    assert row["result"] == "REPAIRED"


def test_risk_events_are_recorded_with_the_observed_and_limit_values(db):
    record_risk_event(db, bot_instance_id=BOT, event_type="DAILY_LOSS_LIMIT",
                      observed_value=-30.0, limit_value=-25.0, action="BLOCK_ENTRIES")
    with db.connect() as conn:
        row = dict(conn.execute("SELECT * FROM risk_events").fetchone())
    assert row["event_type"] == "DAILY_LOSS_LIMIT"
    assert row["observed_value"] == pytest.approx(-30.0)


# ══════════════════════════════════════════════════════════════════════════
# Phase 11 §34/§56 — bot lifecycle must be attributable
# ══════════════════════════════════════════════════════════════════════════


def test_bot_lifecycle_event_requires_an_actor(db):
    with pytest.raises(MissingAttributionError, match="actor"):
        record_bot_lifecycle_event(
            db, bot_instance_id=BOT, event_type="BOT_DELETED",
            actor="", reason="cleanup",
        )


def test_bot_lifecycle_event_requires_a_reason(db):
    with pytest.raises(MissingAttributionError, match="reason"):
        record_bot_lifecycle_event(
            db, bot_instance_id=BOT, event_type="BOT_DELETED",
            actor="admin:u1", reason="",
        )


def test_an_attributable_deletion_records_actor_states_and_correlation(db):
    """The September active->deleted replacement must not be repeatable silently."""
    record_bot_lifecycle_event(
        db, bot_instance_id=BOT, event_type="BOT_DELETED", actor="admin:u1",
        reason="superseded by redeploy", previous_state="active", new_state="deleted",
        correlation_id="req-123",
    )
    with db.connect() as conn:
        row = dict(conn.execute("SELECT * FROM bot_system_events").fetchone())

    assert row["event_type"] == "BOT_DELETED"
    assert "admin:u1" in row["details_json"]
    assert "active" in row["details_json"] and "deleted" in row["details_json"]
    assert row["run_id"] == "req-123"


def test_the_lifecycle_vocabulary_covers_the_replacement_scenario():
    for required in ("BOT_CREATED", "BOT_DELETED", "BOT_REPLACED", "BOT_REDEPLOYED",
                     "POLICY_CHANGED", "RUNNER_CREATED", "RUNNER_EVICTED"):
        assert required in BOT_LIFECYCLE_EVENT_TYPES


# ══════════════════════════════════════════════════════════════════════════
# Phase 11 §36 — provenance separation
# ══════════════════════════════════════════════════════════════════════════


def test_validation_and_test_provenance_are_not_organic():
    assert PAPER_FORWARD in ORGANIC_PROVENANCE
    for excluded in (REPLAY, TEST_FIXTURE, PAPER_FORWARD_VALIDATION, "BACKTEST", "SYNTHETIC"):
        assert excluded in NON_ORGANIC_PROVENANCE
        assert excluded not in ORGANIC_PROVENANCE


def test_phase_12_validation_evidence_never_counts_toward_readiness(db):
    for provenance in (PAPER_FORWARD, PAPER_FORWARD_VALIDATION, REPLAY, TEST_FIXTURE):
        with record_decision(
            db, bot_instance_id=BOT, symbol="BTCUSDT", provenance=provenance,
        ) as decision:
            decision.hold("NO_OPPORTUNITY")

    assert organic_decision_count(db, BOT) == 1, "only PAPER_FORWARD is organic"
    assert len(find_non_organic_in_readiness_scope(db, BOT)) == 3


def test_paper_mode_against_a_mainnet_environment_is_a_contradiction(db):
    with record_decision(
        db, bot_instance_id=BOT, symbol="BTCUSDT",
        execution_mode="paper", broker_environment="mainnet",
    ) as decision:
        decision.hold("NO_OPPORTUNITY")

    assert len(find_mode_environment_contradictions(db)) == 1


# ══════════════════════════════════════════════════════════════════════════
# §53 — the aggregate integrity suite
# ══════════════════════════════════════════════════════════════════════════


def test_a_clean_database_reports_no_integrity_violations(db):
    with record_decision(db, bot_instance_id=BOT, symbol="BTCUSDT") as decision:
        decision.hold("NO_OPPORTUNITY")

    assert run_integrity_checks(db) == {}


def test_integrity_findings_can_be_recorded_as_data_quality_events(db):
    with record_decision(db, bot_instance_id=BOT, symbol="BTCUSDT") as decision:
        decision.approve("APPROVED_FOR_EXECUTION")  # no attempt -> violation

    findings = run_integrity_checks(db, record=True)

    assert "MISSING_EXECUTION_ATTEMPT" in findings
    assert count(db, "data_quality_events", "event_type=?", ("MISSING_EXECUTION_ATTEMPT",)) == 1


def test_market_snapshot_lineage_is_stored_by_hash_not_by_copying_candles(db):
    snapshot = MarketSnapshot.build(
        symbol="BTCUSDT", timeframe="15m", candles=candles(500), source="binance",
        source_environment="demo",
    )
    record_market_snapshot(db, snapshot, bot_instance_id=BOT)

    with db.connect() as conn:
        row = dict(conn.execute("SELECT * FROM market_snapshots").fetchone())

    assert row["data_hash"] == snapshot.data_hash
    assert row["candle_count"] == len(snapshot.candles)
    assert row["source_environment"] == "demo"
    # The row identifies the data; it does not duplicate it.
    assert "candles" not in row


# ══════════════════════════════════════════════════════════════════════════
# Phase 11 §37 — test isolation
# ══════════════════════════════════════════════════════════════════════════


def test_audit_sink_is_isolated_under_test_mode(monkeypatch):
    """Fixture events must never append to the organic runtime audit log."""
    from shared_lib.persistence.audit import Audit

    monkeypatch.setenv("COSMICFORGE_TEST_MODE", "1")
    monkeypatch.delenv("COSMICFORGE_AUDIT_JSONL", raising=False)
    resolved = Audit._resolve_jsonl_path()

    assert "live_audit.jsonl" not in resolved


def test_audit_sink_can_be_pointed_anywhere_explicitly(tmp_path, monkeypatch):
    from shared_lib.persistence.audit import Audit

    target = tmp_path / "isolated.jsonl"
    monkeypatch.setenv("COSMICFORGE_AUDIT_JSONL", str(target))
    assert Audit._resolve_jsonl_path() == str(target)


def test_conftest_enables_test_mode_for_the_whole_suite():
    import os

    assert os.environ.get("COSMICFORGE_TEST_MODE") == "1"


# ══════════════════════════════════════════════════════════════════════════
# §55 — do not backfill false certainty
# ══════════════════════════════════════════════════════════════════════════


def test_unknown_correlation_ids_stay_null_rather_than_invented(db):
    with record_decision(
        db, bot_instance_id=BOT, symbol="BTCUSDT",
        provenance="MIGRATED_LEGACY", evidence_quality="PARTIAL",
    ) as decision:
        decision.hold("NO_OPPORTUNITY")

    with db.connect() as conn:
        row = dict(conn.execute("SELECT * FROM trading_decisions").fetchone())

    assert row["provenance"] == "MIGRATED_LEGACY"
    assert row["evidence_quality"] == "PARTIAL"
    for unknown in ("market_snapshot_id", "opportunity_id", "run_id", "cycle_id"):
        assert row[unknown] is None, f"{unknown} was invented rather than left unknown"


def test_the_recorder_never_fabricates_a_correlation_id():
    source = inspect.getsource(TradingDecision)
    for fabricator in ("uuid4()", "or 'unknown'", 'or "unknown"'):
        assert fabricator not in source
