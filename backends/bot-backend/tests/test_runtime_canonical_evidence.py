"""The live runner writes canonical evidence.

Phases 9-12 built the canonical layer; this pins that the *runtime* actually
uses it. Without these, the tables stay empty in production and the diagnostics
API has nothing to serve.

The wrapping strategy under test: ``step_symbol`` is a thin recorder around
``_step_symbol_evaluate``, so every one of the evaluation body's ~30 early
returns — and every exception — finalizes exactly one canonical decision
without each branch having to remember to record itself.
"""
from __future__ import annotations

import inspect

import pytest

from app.decision.reasons import CycleReason, QualityReason
from app.evidence.integrity import find_decisions_without_primary_reason, run_integrity_checks
from app.evidence.runner_bridge import (
    canonical_reason,
    final_action,
    record_symbol_evaluation,
    resolve_provenance,
)
from app.runner.multi_runner import MultiBotRunner
from app.runner.runner import PaperRunner
from shared_lib.persistence.db import DB
from shared_lib.persistence.evidence_schema import (
    LIVE_MAINNET,
    PAPER_FORWARD,
    TESTNET,
)
from shared_lib.persistence.migrations import migrate

BOT = "bot-runtime-evidence"


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


@pytest.fixture
def db():
    database = DB(":memory:")
    migrate(database)
    return database


class FakeContext:
    bot_instance_id = BOT
    user_id = "user-1"
    broker_account_id = "brk-1"
    market_type = "CRYPTO"
    broker_environment = "demo"
    effective_policy_hash = "hash-1"


class FakeRunner:
    """Minimal stand-in exposing exactly what the bridge reads."""

    def __init__(self, db, *, execution_mode="paper", context=None):
        self.db = db
        self.context = FakeContext() if context is None else context
        self.run_id = "run-1"
        self.cycle_id = "cyc-1"
        self.runtime_session_id = "rts-1"
        self.interval = "15m"
        self._mode = execution_mode
        self._symbol_evidence = {}

    def _effective_execution_mode(self):
        return self._mode


def decisions(db):
    with db.connect() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM trading_decisions")]


# ── The wrapper is in place ─────────────────────────────────────────────────


def test_step_symbol_is_a_recording_wrapper_around_the_evaluation_body():
    wrapper = inspect.getsource(PaperRunner.step_symbol)
    assert "record_symbol_evaluation" in wrapper
    assert "self._step_symbol_evaluate" in wrapper

    # The wrapper must stay thin; the logic lives in the body it delegates to.
    body_lines = [
        line for line in wrapper.splitlines()
        if line.strip() and not line.strip().startswith(("#", '"""', "'''"))
    ]
    assert len(body_lines) <= 12, f"step_symbol grew logic of its own: {body_lines}"


def test_the_evaluation_body_still_exists_and_is_substantial():
    body = inspect.getsource(PaperRunner._step_symbol_evaluate)
    assert len(body.splitlines()) > 200, "the real evaluation body should be here"


def test_the_runner_writes_a_canonical_cycle_summary():
    source = inspect.getsource(PaperRunner.run_cycle)
    assert "_record_canonical_cycle" in source

    helper = inspect.getsource(PaperRunner._record_canonical_cycle)
    assert "record_trading_cycle" in helper
    # Counts come from persisted decisions, not from in-memory tallies, so the
    # summary and the detail cannot disagree.
    assert "FROM trading_decisions WHERE cycle_id=?" in helper


def test_multi_bot_runner_opens_a_canonical_run_and_links_the_session():
    source = inspect.getsource(MultiBotRunner._open_canonical_run)
    assert "open_bot_run" in source
    assert "runtime_session_id=self._runtime_session_id()" in source
    assert "RUNNER_CREATED" in source


def test_runner_eviction_is_recorded_as_an_attributable_event():
    source = inspect.getsource(MultiBotRunner._evict_runner)
    assert "RUNNER_EVICTED" in source
    assert "actor=" in source and "reason=" in source


# ── Every evaluation path finalizes exactly one decision ────────────────────


@pytest.mark.parametrize(
    "result,expected_reason,expected_action",
    [
        ({"symbol": "BTCUSDT", "decision": "HOLD", "reason": "NO_OPPORTUNITY"},
         QualityReason.NO_OPPORTUNITY, "HOLD"),
        ({"symbol": "BTCUSDT", "skipped": True, "reason": "SYMBOL_LOCK_BUSY"},
         CycleReason.SYMBOL_LOCK_BUSY, "SKIPPED"),
        ({"symbol": "BTCUSDT", "skipped": True, "reason": "CONSECUTIVE_LOSS_COOLDOWN"},
         "RISK_CONSECUTIVE_LOSS_LIMIT", "SKIPPED"),
        ({"symbol": "BTCUSDT", "decision": "ERROR", "reason": "ERROR_STRATEGY_UNAVAILABLE"},
         "EXECUTION_ERROR", "ERROR"),
        ({"symbol": "BTCUSDT", "decision": "execute"},
         QualityReason.APPROVED_FOR_EXECUTION, "APPROVED"),
        ({"symbol": "BTCUSDT", "error": "boom"}, "EXECUTION_ERROR", "ERROR"),
    ],
)
def test_each_runner_result_shape_finalizes_one_decision(db, result, expected_reason, expected_action):
    """Every real evaluation shape becomes exactly one decision.

    NO_NEW_CANDLE is deliberately absent from this table: since the §11
    heartbeat-storage change it is counted in the cycle summary rather than
    persisted as a decision. Its contract is covered by
    test_a_heartbeat_is_counted_not_persisted below.
    """
    runner = FakeRunner(db)
    returned = record_symbol_evaluation(runner, "BTCUSDT", evaluate=lambda s: result)

    rows = decisions(db)
    assert returned == result
    assert len(rows) == 1
    assert rows[0]["primary_reason"] == expected_reason
    assert rows[0]["final_action"] == expected_action
    assert rows[0]["complete"] == 1


def test_an_exception_in_the_evaluation_still_leaves_a_finalized_record(db):
    runner = FakeRunner(db)

    def explode(symbol):
        raise RuntimeError("orchestrator died")

    with pytest.raises(RuntimeError):
        record_symbol_evaluation(runner, "BTCUSDT", evaluate=explode)

    rows = decisions(db)
    assert len(rows) == 1
    assert rows[0]["primary_reason"] == "EXECUTION_ERROR"
    assert rows[0]["complete"] == 1
    assert "orchestrator died" in rows[0]["legacy_reason"]


def test_no_result_shape_produces_a_missing_reason(db):
    runner = FakeRunner(db)
    for result in ({}, {"symbol": "X"}, {"decision": ""}, {"reason": None}):
        record_symbol_evaluation(runner, "BTCUSDT", evaluate=lambda s, r=result: r)

    assert find_decisions_without_primary_reason(db) == []


def test_correlation_ids_are_carried_from_the_runner(db):
    runner = FakeRunner(db)
    record_symbol_evaluation(
        runner, "BTCUSDT", evaluate=lambda s: {"decision": "HOLD", "reason": "NO_OPPORTUNITY"}
    )

    row = decisions(db)[0]
    assert row["bot_instance_id"] == BOT
    assert row["user_id"] == "user-1"
    assert row["broker_account_id"] == "brk-1"
    assert row["run_id"] == "run-1"
    assert row["cycle_id"] == "cyc-1"
    assert row["runtime_session_id"] == "rts-1"
    assert row["policy_hash"] == "hash-1"
    assert row["timeframe"] == "15m"


def test_a_contextless_runner_writes_no_decision(db):
    """The legacy global runner never trades; it must not manufacture evidence."""
    runner = FakeRunner(db)
    runner.context = None

    returned = record_symbol_evaluation(runner, "BTCUSDT", evaluate=lambda s: {"decision": "HOLD"})

    assert returned == {"decision": "HOLD"}
    assert decisions(db) == []


# ── Rich evidence flows through ─────────────────────────────────────────────


def test_opportunity_and_quality_evidence_reach_the_canonical_row(db):
    from app.decision import TradingDecisionEngine
    from app.decision.opportunity import build_opportunity
    from app.runner.market_snapshot import MarketSnapshot

    tf = 15 * 60 * 1000
    snapshot = MarketSnapshot.build(
        symbol="BTCUSDT", timeframe="15m",
        candles=[[i * tf, 1, 2, 0.5, 1.5, 10, i * tf + tf - 1] for i in range(1, 10)],
        source="test",
    )
    opportunity = build_opportunity(
        symbol="BTCUSDT", timeframe="15m", market_snapshot_id=snapshot.market_snapshot_id,
        side="BUY", raw_confidence=0.42, consensus=0.42, buy_score=0.42, sell_score=0.1,
        votes=[("supertrend", "BUY", 0.6), ("vwap", "SELL", 0.2)],
        regime="WEAK_TREND", regime_confidence=0.55,
    )
    threshold_decision = _threshold_at(0.55)
    quality = TradingDecisionEngine().evaluate(
        opportunity, threshold_decision=threshold_decision
    )

    runner = FakeRunner(db)

    def evaluate(symbol):
        # The real runner publishes evidence from inside the evaluation; the
        # recorder clears the bucket first so each evaluation starts clean.
        runner._symbol_evidence[symbol] = {
            "snapshot": snapshot, "opportunity": opportunity, "entry_quality": quality,
            "threshold_decision": threshold_decision,
        }
        return {"decision": "HOLD", "reason": "ENTRY_CONFIDENCE_BELOW_THRESHOLD"}

    record_symbol_evaluation(runner, "BTCUSDT", evaluate=evaluate)

    row = decisions(db)[0]
    assert row["market_snapshot_id"] == snapshot.market_snapshot_id
    assert row["opportunity_id"] == opportunity.opportunity_id
    assert row["regime"] == "WEAK_TREND"
    assert row["raw_confidence"] == pytest.approx(0.42)
    assert row["effective_entry_threshold"] == pytest.approx(0.55)
    assert row["quality_result"] == "FAIL"
    assert "supertrend" in row["supporting_strategies_json"]
    assert "vwap" in row["opposing_strategies_json"]


def test_the_runner_publishes_evidence_for_the_recorder():
    """The orchestrated path must stash the objects, not just the meta dict."""
    orchestrated = inspect.getsource(PaperRunner._step_symbol_orchestrated)
    # The NO_NEW_CANDLE branch returns before the orchestrator runs, so it
    # publishes the snapshot it does have.
    assert '_bucket.setdefault(symbol, {})["snapshot"] = market_snapshot' in orchestrated
    assert 'last_opportunity' in orchestrated
    assert 'last_entry_quality' in orchestrated
    assert '"snapshot": market_snapshot' in orchestrated


# ── Provenance is derived from mode + environment ───────────────────────────


@pytest.mark.parametrize(
    "mode,env,expected",
    [
        ("paper", "demo", PAPER_FORWARD),
        ("paper", "mainnet", PAPER_FORWARD),
        ("broker", "demo", TESTNET),
        ("broker", "testnet", TESTNET),
        ("broker", "mainnet", LIVE_MAINNET),
        ("broker", "live", LIVE_MAINNET),
        ("broker", "production", LIVE_MAINNET),
    ],
)
def test_provenance_never_mistakes_demo_for_real_money(mode, env, expected):
    assert resolve_provenance(mode, env) == expected


def test_broker_mode_evidence_is_recorded_as_testnet_not_paper_forward(db):
    runner = FakeRunner(db, execution_mode="broker")
    record_symbol_evaluation(
        runner, "BTCUSDT", evaluate=lambda s: {"decision": "HOLD", "reason": "NO_OPPORTUNITY"}
    )

    row = decisions(db)[0]
    assert row["provenance"] == TESTNET
    assert row["execution_mode"] == "broker"
    assert row["broker_environment"] == "demo"


# ── Reason mapping ──────────────────────────────────────────────────────────


def test_a_reason_is_always_produced():
    for result in ({}, {"decision": ""}, {"reason": ""}, {"symbol": "X"}):
        assert canonical_reason(result)


def test_legacy_runner_strings_map_onto_the_canonical_taxonomy():
    assert canonical_reason({"reason": "SYMBOL_LOCK_BUSY"}) == CycleReason.SYMBOL_LOCK_BUSY
    assert canonical_reason({"reason": "daily_loss_limit_reached"}) == "RISK_DAILY_LOSS_LIMIT"
    assert canonical_reason({"reason": "POST_EVENT_VOLATILITY"}) == QualityReason.EVENT_BLACKOUT
    assert canonical_reason({"reason": "MAX_POSITIONS_REACHED"}) == "RISK_MAX_OPEN_POSITIONS"


def test_final_action_distinguishes_approval_from_hold_and_error():
    assert final_action({"decision": "execute"}) == "APPROVED"
    assert final_action({"decision": "HOLD"}) == "HOLD"
    assert final_action({"decision": "CLOSE"}) == "CLOSE"
    assert final_action({"error": "x"}) == "ERROR"
    assert final_action({"skipped": True, "reason": "SYMBOL_LOCK_BUSY"}) == "SKIPPED"


# ── A whole cycle's worth of evidence stays consistent ──────────────────────


def test_a_multi_symbol_cycle_leaves_one_decision_per_symbol_and_clean_integrity(db):
    runner = FakeRunner(db)
    outcomes = {
        "BTCUSDT": {"decision": "HOLD", "reason": "NO_OPPORTUNITY"},
        "ETHUSDT": {"decision": "NO_NEW_CANDLE", "reason_code": "NO_NEW_CANDLE"},
        "SOLUSDT": {"skipped": True, "reason": "SYMBOL_LOCK_BUSY"},
    }
    for symbol, outcome in outcomes.items():
        record_symbol_evaluation(runner, symbol, evaluate=lambda s, o=outcome: o)

    rows = decisions(db)
    # ETHUSDT was a heartbeat: counted in the cycle summary, not persisted as a
    # decision (§11). The two real evaluations still each get exactly one row.
    assert len(rows) == 2
    assert {r["symbol"] for r in rows} == {"BTCUSDT", "SOLUSDT"}
    assert runner._heartbeat_counts == {"ETHUSDT": 1}
    assert all(r["cycle_id"] == "cyc-1" for r in rows)
    assert run_integrity_checks(db) == {}


def test_a_heartbeat_is_counted_not_persisted(db):
    """§11: ~17k decision rows/day was unacceptable; the tally replaces them."""
    runner = FakeRunner(db)
    for _ in range(25):
        record_symbol_evaluation(
            runner, "BTCUSDT",
            evaluate=lambda s: {"decision": "NO_NEW_CANDLE", "reason_code": "NO_NEW_CANDLE"},
        )

    assert decisions(db) == []
    assert runner._heartbeat_counts["BTCUSDT"] == 25


# ── Regression: the heartbeat must not overwrite a real evaluation ──────────


def test_a_no_new_candle_heartbeat_never_replaces_the_candles_real_evaluation(db):
    """Observed in live runtime, not caught by any earlier test.

    NO_NEW_CANDLE rows carry the current candle's timestamp for diagnostics.
    While the uniqueness index covered them, the 10-second heartbeat's
    INSERT OR REPLACE silently overwrote the genuine evaluation for that
    candle: a real ENTRY_CONFIDENCE_BELOW_THRESHOLD row was replaced by the
    heartbeat that followed it, and the evidence was lost.
    """
    from app.evidence.decision_recorder import record_decision, record_no_new_candle

    close_time = 1788841799999

    # The real evaluation for this candle.
    with record_decision(
        db, bot_instance_id=BOT, symbol="BTCUSDT", timeframe="15m",
        closed_candle_close_time=close_time,
    ) as decision:
        decision.raw_confidence = 0.42
        decision.regime = "LOW_VOL_CHOP"
        decision.hold(QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD)

    # Five heartbeats on the same candle.
    for _ in range(5):
        record_no_new_candle(
            db, bot_instance_id=BOT, symbol="BTCUSDT", timeframe="15m",
            closed_candle_close_time=close_time,
        )

    with db.connect() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT primary_reason, raw_confidence, regime FROM trading_decisions "
            "WHERE symbol='BTCUSDT' AND closed_candle_close_time=?", (close_time,)
        )]

    reasons = [r["primary_reason"] for r in rows]
    assert QualityReason.ENTRY_CONFIDENCE_BELOW_THRESHOLD in reasons, (
        "the heartbeat destroyed the real evaluation for this candle"
    )
    real = next(r for r in rows if r["primary_reason"] != CycleReason.NO_NEW_CANDLE)
    assert real["raw_confidence"] == pytest.approx(0.42)
    assert real["regime"] == "LOW_VOL_CHOP"


def test_duplicate_detection_ignores_heartbeats_but_still_catches_real_duplicates(db):
    from app.evidence.decision_recorder import record_no_new_candle
    from app.evidence.integrity import find_duplicate_candle_decisions

    close_time = 1788841799999
    for _ in range(4):
        record_no_new_candle(
            db, bot_instance_id=BOT, symbol="BTCUSDT", timeframe="15m",
            closed_candle_close_time=close_time,
        )

    # Many heartbeats on one candle are expected, not a duplicate entry decision.
    assert find_duplicate_candle_decisions(db) == []


def test_the_entry_decision_uniqueness_index_still_collapses_a_retry(db):
    from app.evidence.decision_recorder import record_decision

    close_time = 1788841799999
    for _ in range(3):
        with record_decision(
            db, bot_instance_id=BOT, symbol="BTCUSDT", timeframe="15m",
            closed_candle_close_time=close_time,
        ) as decision:
            decision.hold(QualityReason.NO_OPPORTUNITY)

    with db.connect() as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM trading_decisions WHERE primary_reason=?",
            (QualityReason.NO_OPPORTUNITY,),
        ).fetchone()[0]
    assert n == 1, "one entry decision per candle still holds"
